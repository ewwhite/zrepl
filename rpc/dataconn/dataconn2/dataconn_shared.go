package dataconn

import (
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/go-playground/validator"
	"github.com/zrepl/zrepl/rpc/dataconn/frameconn2"
)

type SharedConfig struct {
	MaxProtoLen      uint32        `validate:"gt=0"`
	MaxHeaderLen     uint32        `validate:"gt=0"`
	SendChunkSize    uint32        `validate:"gt=0"`
	MaxRecvChunkSize uint32        `validate:"gt=0"`
	IdleConnTimeout  time.Duration `validate:"gte=0"`
}

var validate = validator.New()

type streamer struct {
	log Logger
}

// if sendStream returns an error, that error will be sent as a trailer to the client
// ok will return true, though.
func (s *streamer) writeSendStream(c *frameconn.Conn, sendStream io.Reader) (ok bool) {

	f := frameconn.Frame{
		Type:   frameconn.Stream,
		Buffer: make([]byte, 1<<21),
	}

	var err error
	for {
		f.BufferLen, err = sendStream.Read(f.Buffer[:])
		if err != nil && err != io.EOF {
			s.log.WithError(err).Error("error reading from send stream")
			f.Type = frameconn.StreamError
			errReader := strings.NewReader(err.Error())
			f.BufferLen, _ = errReader.Read(f.Buffer[:])
			if err := c.WriteFrame(&f); err != nil {
				s.log.WithError(err).Error("cannot send stream error to client")
				return false
			}
			return true
		}
		// next line is the hot path...
		if err := c.WriteFrame(&f); err != nil {
			s.log.WithError(err).Error("cannot send stream frame")
			return false
		}
		if err == io.EOF {
			s.log.Debug("send stream finished")
			f := frameconn.Frame{
				Type:      frameconn.StreamDone,
				Buffer:    []byte{},
				BufferLen: 0,
			}
			if err := c.WriteFrame(&f); err != nil {
				s.log.WithError(err).Error("cannot send stream done frame")
				return false
			}
			break
		}
	}

	return true
}

func (s *streamer) readSendStream(c *frameconn.Conn, receiver io.Writer) error {

	f := frameconn.Frame{
		Buffer: make([]byte, 1<<21),
	}

	for {
		if err := c.ReadFrame(&f); err != nil {
			return err
		}
		if err := f.Validate(); err != nil {
			return fmt.Errorf("invalid frame")
		}
		if f.Type != frameconn.Stream {
			break
		}

		n, err := receiver.Write(f.Bytes())
		if err != nil {
			return err // FIXME wrap as writer error
		}
		if n != f.BufferLen {
			return io.ErrShortWrite
		}
	}

	if f.Type == frameconn.StreamDone {
		return nil
	}

	if f.Type == frameconn.StreamError {
		return fmt.Errorf("stream error: %q", string(f.Bytes())) // FIXME
	}

	return fmt.Errorf("received unexpected frame type: %v", f.Type)
}
