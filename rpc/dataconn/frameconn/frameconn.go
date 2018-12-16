package frameconn

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"sync"
	"sync/atomic"
)

type Conn struct {
	readMtx, writeMtx sync.Mutex
	nc                net.Conn
	ncBuf             *bufio.ReadWriter
	readBuf           []byte
	state             int32
}

type Frame struct {
	Type      uint32
	BufferLen int
	Buffer    []byte
}

func (f *Frame) FitBufferLenToBuffer() {
	f.BufferLen = len(f.Buffer)
}

func (f *Frame) Bytes() []byte {
	return f.Buffer[0:f.BufferLen]
}

func (f *Frame) Validate() error {
	if f.BufferLen < 0 {
		return errors.New("BufferLen must be positive")
	}
	if f.BufferLen > len(f.Buffer) {
		return errors.New("BufferLen exceeds len(Buffer)")
	}
	if f.Buffer == nil {
		return errors.New("Buffer must not be nil")
	}
	return nil
}

func NewDataConn(nc net.Conn) *Conn {
	return &Conn{
		nc: nc,
		//		ncBuf: bufio.NewReadWriter(bufio.NewReaderSize(nc, 1<<23), bufio.NewWriterSize(nc, 1<<23)),
		state:   stateInitial,
		readBuf: make([]byte, 8+1<<19), // parameterize
	}
}

var ErrReadFrameLengthShort = errors.New("read frame length too short")
var ErrFixedFrameLengthMismatch = errors.New("read frame length mismatch")

// f is an out-parameter
// if err == nil is returned, f is valid until the next call to ReadFrame
func (c *Conn) ReadFrame(f *Frame) error {
	c.readMtx.Lock()
	defer c.readMtx.Unlock()

	if _, err := io.ReadFull(c.nc, c.readBuf[:]); err != nil {
		return err
	}
	f.Type = binary.BigEndian.Uint32(c.readBuf[0:4])
	bufferLen := binary.BigEndian.Uint32(c.readBuf[4:8])

	if uint32(len(c.readBuf)-8) < bufferLen {
		c.renderUnusable()
		return fmt.Errorf("read frame length mismatch: %v %v", len(c.readBuf), bufferLen)
	}
	f.Buffer = c.readBuf[8:8+bufferLen]
	f.BufferLen = int(bufferLen) // unsafe
	return nil
}

func (c *Conn) WriteFrame(f *Frame) error {
	c.writeMtx.Lock()
	defer c.writeMtx.Unlock()

	if f.BufferLen < 0 {
		panic("invalid buffer length")
	}
	if f.BufferLen > math.MaxUint32 {
		panic("invalid buffer length")
	}
	bufferLen := uint32(f.BufferLen)

	var hdrBuf [8]byte
	binary.BigEndian.PutUint32(hdrBuf[0:4], f.Type)
	binary.BigEndian.PutUint32(hdrBuf[4:8], bufferLen)

	bufs := net.Buffers([][]byte{hdrBuf[:], f.Buffer[:]})
	if _, err := io.Copy(c.nc, &bufs); err != nil {
		return err
	}
	return nil
}

const (
	stateInitial   int32 = 0
	stateUnusuable int32 = 1
	stateClosed    int32 = 2
)

func (c *Conn) renderUnusable() {
	atomic.CompareAndSwapInt32(&c.state, stateInitial, stateUnusuable)
}

func (c *Conn) Close() error {
	edgeOne := atomic.CompareAndSwapInt32(&c.state, stateInitial, stateClosed)
	edgeTwo := atomic.CompareAndSwapInt32(&c.state, stateUnusuable, stateClosed)
	if edgeOne || edgeTwo {
		return c.nc.Close()
	}
	return nil
}
