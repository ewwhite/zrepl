package frameconn

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/zrepl/zrepl/rpc/dataconn/base2bufpool"
	"github.com/zrepl/zrepl/rpc/dataconn/timeoutconn"
)

type FrameHeader struct {
	Type       uint32
	PayloadLen uint32
}

// The 4 MSBs of ft are reserved for frameconn.
func IsPublicFrameType(ft uint32) bool {
	return (0xf<<28)&ft == 0
}

func assertPublicFrameType(frameType uint32) {
	if !IsPublicFrameType(frameType) {
		panic(fmt.Sprintf("frameconn: frame type %v cannot be used by consumers of this package", frameType))
	}
}

func (f *FrameHeader) Unmarshal(buf []byte) {
	if len(buf) != 8 {
		panic(fmt.Sprintf("frame header is 8 bytes long"))
	}
	f.Type = binary.BigEndian.Uint32(buf[0:4])
	f.PayloadLen = binary.BigEndian.Uint32(buf[4:8])
}

type Conn struct {
	readMtx, writeMtx sync.Mutex
	nc                timeoutconn.Conn
	ncBuf             *bufio.ReadWriter
	readNextValid     bool
	readNext          FrameHeader
	bufPool           *base2bufpool.Pool // no need for sync around it
}

func Wrap(nc timeoutconn.Conn) *Conn {
	return &Conn{
		nc: nc,
		//		ncBuf: bufio.NewReadWriter(bufio.NewReaderSize(nc, 1<<23), bufio.NewWriterSize(nc, 1<<23)),
		bufPool:       base2bufpool.New(15, 22, base2bufpool.Allocate), // FIXME switch to Panic, but need to enforce the limits in recv for that. => need frameconn config
		readNext:      FrameHeader{},
		readNextValid: false,
	}
}

var ErrReadFrameLengthShort = errors.New("read frame length too short")
var ErrFixedFrameLengthMismatch = errors.New("read frame length mismatch")

type Buffer struct {
	bufpoolBuffer base2bufpool.Buffer
	payloadLen    uint32
}

func (b *Buffer) Free() {
	b.bufpoolBuffer.Free()
}

func (b *Buffer) Bytes() []byte {
	return b.bufpoolBuffer.Bytes()[0:b.payloadLen]
}

type Frame struct {
	Header FrameHeader
	Buffer Buffer
}

// f is an out-parameter
// if err == nil is returned, f is valid until the next call to ReadFrame
func (c *Conn) ReadFrame() (Frame, error) {
	c.readMtx.Lock()
	defer c.readMtx.Unlock()

	if !c.readNextValid {
		var buf [8]byte
		if _, err := io.ReadFull(c.nc, buf[:]); err != nil {
			return Frame{}, err
		}
		c.readNext.Unmarshal(buf[:])
		c.readNextValid = true
	}

	// read payload + next header
	var nextHdrBuf [8]byte
	buffer := c.bufPool.Get(uint(c.readNext.PayloadLen))
	bufferBytes := buffer.Bytes()

	endOfConnection := false
	if n, err := c.nc.ReadvFull([][]byte{bufferBytes, nextHdrBuf[:]}); err != nil {
		endOfConnection = (n == 0 && err == io.EOF) ||
			(err == io.ErrUnexpectedEOF && uint32(n) == c.readNext.PayloadLen)
		if !endOfConnection {
			return Frame{}, err
		}
	}

	frame := Frame{
		Header: c.readNext,
		Buffer: Buffer{
			bufpoolBuffer: buffer,
			payloadLen:    c.readNext.PayloadLen,
		},
	}

	if !endOfConnection {
		c.readNext.Unmarshal(nextHdrBuf[:])
		c.readNextValid = true
	} else {
		c.readNextValid = false
	}

	return frame, nil
}

func (c *Conn) WriteFrame(payload []byte, frameType uint32) error {
	assertPublicFrameType(frameType)
	return c.writeFrame(payload, frameType)
}

func (c *Conn) writeFrame(payload []byte, frameType uint32) error {
	c.writeMtx.Lock()
	defer c.writeMtx.Unlock()

	var hdrBuf [8]byte
	binary.BigEndian.PutUint32(hdrBuf[0:4], frameType)
	binary.BigEndian.PutUint32(hdrBuf[4:8], uint32(len(payload)))
	bufs := net.Buffers([][]byte{hdrBuf[:], payload})
	if _, err := c.nc.WritevFull(bufs); err != nil {
		return err
	}
	return nil
}

func (c *Conn) Close() error {
	return c.nc.Close()
}
