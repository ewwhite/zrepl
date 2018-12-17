package heartbeatconn

import (
	"fmt"
	"net"
	"os"
	"sync/atomic"
	"time"

	"github.com/zrepl/zrepl/rpc/dataconn/frameconn2"
	"github.com/zrepl/zrepl/rpc/dataconn/timeoutconn"
)

type Conn struct {
	state state
	// if not nil, opErr is returned for ReadFrame and WriteFrame (not for Close, though)
	opErr                 atomic.Value // error
	fc                    *frameconn.Conn
	sendInterval, timeout time.Duration
	stopSend              chan struct{}
	stopWatch             chan struct{}
	lastFrameSent         atomic.Value // time.Time
	lastPeerAlive         atomic.Value // time.Time
}

type HeartbeatTimeout struct{}

func (e HeartbeatTimeout) Error() string {
	return "heartbeat timeout"
}

func (e HeartbeatTimeout) Temporary() bool { return true }

func (e HeartbeatTimeout) Timeout() bool { return true }

var _ net.Error = HeartbeatTimeout{}

type state = int32

const (
	stateInitial  state = 0
	statePeerDead state = 1
	stateClosed   state = 2
)

const (
	heartbeat uint32 = 1 << 24
)

// The 4 MSBs of ft are reserved for frameconn, we reserve the next 4 MSB for us.
func IsPublicFrameType(ft uint32) bool {
	return frameconn.IsPublicFrameType(ft) && (0xf<<24)&ft == 0
}

func assertPublicFrameType(frameType uint32) {
	if !IsPublicFrameType(frameType) {
		panic(fmt.Sprintf("heartbeatconn: frame type %v cannot be used by consumers of this package", frameType))
	}
}

func Wrap(nc net.Conn, sendInterval, timeout time.Duration) *Conn {
	c := &Conn{
		fc:           frameconn.Wrap(timeoutconn.Wrap(nc, timeout)), // timeoutconn necessary?
		stopSend:     make(chan struct{}),
		stopWatch:    make(chan struct{}),
		sendInterval: sendInterval,
		timeout:      timeout,
	}
	c.lastFrameSent.Store(time.Now())
	c.lastPeerAlive.Store(time.Now())
	go c.sendHeartbeats()
	go c.watchHeartbeats()
	return c
}

func (c *Conn) Close() error {
	normalClose := atomic.CompareAndSwapInt32(&c.state, stateInitial, stateClosed)
	peerDead := atomic.CompareAndSwapInt32(&c.state, statePeerDead, stateClosed)
	if normalClose || peerDead {
		if peerDead {
		}
		close(c.stopSend)
		close(c.stopWatch)
		return c.fc.Close()
	}
	return nil
}

// started as a goroutine in constructor
func (c *Conn) sendHeartbeats() {
	sleepTime := func(now time.Time) time.Duration {
		lastSend := c.lastFrameSent.Load().(time.Time)
		return lastSend.Add(c.sendInterval).Sub(now)
	}
	timer := time.NewTimer(sleepTime(time.Now()))
	defer timer.Stop()
	for {
		select {
		case <-c.stopSend:
			return
		case now := <-timer.C:
			func() {
				defer func() {
					timer.Reset(sleepTime(time.Now()))
				}()
				if sleepTime(now) > 0 {
					return
				}
				fmt.Fprintf(os.Stderr, "send heartbeat\n")
				// if the connection is in zombie mode (aka iptables DROP inbetween peers)
				// this call or one of its successors will block after filling up the kernel tx buffer
				c.fc.WriteFrame([]byte{}, heartbeat)
				// ignore errors from WriteFrame to rate-limit SendHeartbeat retries
				c.lastFrameSent.Store(time.Now())
			}()
		}
	}
}

func (c *Conn) watchHeartbeats() {
	sleepTime := func(now time.Time) time.Duration {
		lastAlive := c.lastPeerAlive.Load().(time.Time)
		return lastAlive.Add(c.timeout).Sub(now)
	}
	timer := time.NewTimer(c.timeout)
	defer timer.Stop() // FIXME
	for {
		select {
		case <-c.stopWatch:
			return
		case now := <-timer.C:
			peerDeadEdge := func() (peerDead bool) {
				defer func() {
					timer.Reset(sleepTime(time.Now()))
				}()
				if sleepTime(now) > 0 {
					return false
				}
				return atomic.CompareAndSwapInt32(&c.state, stateInitial, statePeerDead)
			}()
			if peerDeadEdge {
				fmt.Fprintf(os.Stderr, "peer dead\n")
				err := c.Close()
				if err != nil {
					fmt.Fprintf(os.Stderr, "clsoe err: %T %s\n", err, err)
				}
				fmt.Fprintf(os.Stderr, "close done\n")
				return
			}
		}
	}
}

func (c *Conn) getOpErr() error {
	if i := c.opErr.Load(); i != nil {
		return i.(error)
	}
	return nil
}

func (c *Conn) ReadFrame() (frameconn.Frame, error) {
	if err := c.getOpErr(); err != nil {
		return frameconn.Frame{}, err
	}
	f, err := c.readFrameFiltered()
	opErr := c.getOpErr() // load again, might have been changed by watchHeartbeats
	if err != nil && opErr != nil {
		err = opErr
	}
	return f, err
}

func (c *Conn) readFrameFiltered() (frameconn.Frame, error) {
	for {
		f, err := c.fc.ReadFrame()
		if err == nil {
			c.lastPeerAlive.Store(time.Now())
		}
		if err != nil {
			return frameconn.Frame{}, err
		}
		if IsPublicFrameType(f.Header.Type) {
			return f, nil
		}
		if f.Header.Type != heartbeat {
			return frameconn.Frame{}, fmt.Errorf("unknown frame type %x", f.Header.Type)
		}
		// drop heartbeat frame
		fmt.Fprintf(os.Stderr, "received heartbeat\n")
		continue
	}
}

// Callers must ensure that ReadFrame is being called simultaneously to
// a call to WriteFrame (e.g. from a separate goroutine). Otherwise,
// the connection will return heartbeat errors after the heartbeat timeout,
// even though the peer behaves correctly and connectivity works.
//
// The above is necessary because the underlying frameconn.Conn is a simple
// TCP connection which means that there could be non-heartbeat frames
// head of line or mixed with heartbeat frames, but this packages doesn't
// have a chance to only remove the non-heartbeat frames from the underlying
// frameconn.Conn.
// Hence, heartbeats are simple frames that are filtered out during ReadFrame,
// which is the reason why simultaneous ReadFrame is necessary.
func (c *Conn) WriteFrame(payload []byte, frameType uint32) error {
	if err := c.getOpErr(); err != nil {
		return err
	}
	err := c.writeFrame(payload, frameType)
	opErr := c.getOpErr() // load again, might have been changed by watchHeartbeats
	if err != nil && opErr != nil {
		err = opErr
	}
	return err
}

func (c *Conn) writeFrame(payload []byte, frameType uint32) error {
	assertPublicFrameType(frameType)
	err := c.fc.WriteFrame(payload, frameType)
	if err == nil {
		c.lastFrameSent.Store(time.Now())
	}
	return err
}
