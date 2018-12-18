package dataconn

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"time"

	"github.com/go-playground/validator"
	"github.com/zrepl/zrepl/rpc/dataconn/heartbeatconn"
	"github.com/zrepl/zrepl/rpc/dataconn/stream"
	"github.com/zrepl/zrepl/zfs"
)

type SharedConfig struct {
	MaxProtoLen      uint32        `validate:"gt=0"`
	MaxHeaderLen     uint32        `validate:"gt=0"`
	SendChunkSize    uint32        `validate:"gt=0"`
	MaxRecvChunkSize uint32        `validate:"gt=0"`
	IdleConnTimeout  time.Duration `validate:"gte=0"`
}

var validate = validator.New()

const (
	EndpointSend string = "/v1/send"
	EndpointRecv string = "/v1/recv"
)

const (
	ReqHeader uint32 = 1 + iota
	ReqStructured
	ResHeader
	ResStructured
	Stream
	Bye
)

const (
	HeartbeatInterval    = 5 * time.Second
	HeartbeatPeerTimeout = 10 * time.Second
)

type Conn struct {
	hc                 *heartbeatconn.Conn
	readMtx            sync.Mutex
	readClean          bool
	allowWriteStreamTo bool

	writeMtx   sync.Mutex
	writeClean bool

	// must hold both readMtx and writeMtx for closed
	closed bool
}

func wrap(nc net.Conn, sendHeartbeatInterval, peerTimeout time.Duration) *Conn {
	hc := heartbeatconn.Wrap(nc, sendHeartbeatInterval, peerTimeout)
	return &Conn{hc: hc, readClean: true, writeClean: true}
}

var readMessageSentinel = fmt.Errorf("read stream complete")

func isConnCleanAfterRead(res *stream.ReadStreamError) bool {
	return res == nil || res.Kind == stream.ReadStreamErrorKindSource || res.Kind == stream.ReadStreamErrorKindSourceErrEncoding
}

func isConnCleanAfterWrite(err error) bool {
	return err == nil
}

func (c *Conn) ReadStreamedMessage(ctx context.Context, maxSize uint32, frameType uint32) ([]byte, error) {
	c.readMtx.Lock()
	defer c.readMtx.Unlock()
	if !c.readClean {
		return nil, fmt.Errorf("dataconn read message: connection is in unknown state")
	}

	r, w := io.Pipe()
	var buf bytes.Buffer
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		lr := io.LimitReader(r, int64(maxSize))
		if _, err := io.Copy(&buf, lr); err != nil && err != readMessageSentinel {
			panic(err)
		}
	}()
	err := stream.ReadStream(c.hc, w, frameType)
	c.readClean = isConnCleanAfterRead(err)
	w.CloseWithError(readMessageSentinel)
	wg.Wait()
	if err != nil {
		return nil, err
	} else {
		return buf.Bytes(), nil
	}
}

func (c *Conn) setAllowWriteStreamTo() {
	c.readMtx.Lock()
	defer c.readMtx.Unlock()
	if !c.readClean {
		panic("setAllowWriteStreamTo must check for clean read-side")
	}
	c.allowWriteStreamTo = true
}

type writeStreamToErrorUnknownState struct{}

func (e writeStreamToErrorUnknownState) Error() string {
	return "dataconn read stream: connection is in unknown state"
}

func (e writeStreamToErrorUnknownState) IsReadError() bool { return true }

func (e writeStreamToErrorUnknownState) IsWriteError() bool { return false }

// WriteStreamTo reads a stream from Conn and writes it to w.
func (c *Conn) WriteStreamTo(w io.Writer) zfs.StreamCopierError {
	c.readMtx.Lock()
	defer c.readMtx.Unlock()
	if !c.allowWriteStreamTo {
		panic("Conn not set to allow WriteStreamTo, this is a safeguard to prevent multiple uses of WriteStreamTo by a handler")
	}
	c.allowWriteStreamTo = false // reset it
	err := stream.ReadStream(c.hc, w, Stream)
	if !c.readClean {
		return writeStreamToErrorUnknownState{}
	}
	c.readClean = isConnCleanAfterRead(err)
	return err
}

func (c *Conn) WriteStreamedMessage(ctx context.Context, buf io.Reader, frameType uint32) error {
	c.writeMtx.Lock()
	defer c.writeMtx.Unlock()
	if !c.writeClean {
		return fmt.Errorf("dataconn write message: connection is in unknown state")
	}
	err := stream.WriteStream(ctx, c.hc, buf, frameType)
	c.writeClean = isConnCleanAfterWrite(err)
	return err
}

func (c *Conn) SendStream(ctx context.Context, src zfs.StreamCopier) error {
	c.writeMtx.Lock()
	defer c.writeMtx.Unlock()
	if !c.writeClean {
		return fmt.Errorf("dataconn send stream: connection is in unknown state")
	}

	r, w := io.Pipe()
	streamCopierErrChan := make(chan zfs.StreamCopierError)
	go func() {
		streamCopierErrChan <- src.WriteStreamTo(w)
	}()
	writeStreamErrChan := make(chan error)
	go func() {
		writeStreamErr := stream.WriteStream(ctx, c.hc, r, Stream)
		if writeStreamErr != nil {
			w.CloseWithError(writeStreamErr)
		}
		writeStreamErrChan <- writeStreamErr
	}()

	writeStreamErr := <-writeStreamErrChan
	streamCopierErr := <-streamCopierErrChan
	c.writeClean = isConnCleanAfterWrite(writeStreamErr)
	if streamCopierErr != nil && streamCopierErr.IsReadError() {
		return streamCopierErr // something on our side is bad
	} else {
		return writeStreamErr // most likely, something wrt connection is bad
	}
}

func (c *Conn) Close() error {
	c.writeMtx.Lock()
	defer c.writeMtx.Unlock()
	c.readMtx.Lock()
	defer c.readMtx.Unlock()

	// we know that both calls below will run into a timeout,
	// thus we ignore the error
	if c.writeClean {
		fmt.Fprintf(os.Stderr, "send bye\n")
		c.hc.WriteFrame([]byte{}, Bye)
	}
	if c.readClean {
		c.hc.ReadFrame()
	}

	c.writeClean = false
	c.readClean = false
	defer func() {
		c.closed = true
	}()
	return c.hc.Close()
}
