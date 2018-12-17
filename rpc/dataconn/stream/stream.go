package stream

import (
	"context"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"

	"github.com/zrepl/zrepl/logger"
	"github.com/zrepl/zrepl/rpc/dataconn/base2bufpool"
	frameconn "github.com/zrepl/zrepl/rpc/dataconn/frameconn2"
	"github.com/zrepl/zrepl/rpc/dataconn/heartbeatconn"
)

type Logger = logger.Logger

type contextKey int

const (
	contextKeyLogger contextKey = 1 + iota
)

func WithLogger(ctx context.Context, log Logger) context.Context {
	return context.WithValue(ctx, contextKeyLogger, log)
}

func getLog(ctx context.Context) Logger {
	log, ok := ctx.Value(contextKeyLogger).(Logger)
	if !ok {
		log = logger.NewNullLogger()
	}
	return log
}

// The following frameconn.Frame.Type are reserved for Streamer.
const (
	SourceEOF uint32 = 1 << 16
	SourceErr
	// max 16
)

// NOTE: make sure to add a tests for each frame type that checks
//       whether it is frameconn.IsPublicFrameType()

// Check whether the given frame type is allowed to be used by
// consumers of this package. Intended for use in unit tests.
func IsPublicFrameType(ft uint32) bool {
	// 4 MSBs are reserved for frameconn, next 4 MSB for heartbeatconn, next 4 MSB for us.
	return frameconn.IsPublicFrameType(ft) && ((0xf<<16)&ft == 0)
}

func assertPublicFrameType(frameType uint32) {
	if !IsPublicFrameType(frameType) {
		panic(fmt.Sprintf("stream: frame type %v cannot be used by consumers of this package", frameType))
	}
}

// if sendStream returns an error, that error will be sent as a trailer to the client
// ok will return nil, though.
func WriteStream(ctx context.Context, c *heartbeatconn.Conn, stream io.Reader, stype uint32) error {

	if stype == 0 {
		panic("stype must be non-zero")
	}
	assertPublicFrameType(stype)

	bufpool := base2bufpool.New(19, 19)
	type read struct {
		buf base2bufpool.Buffer
		err error
	}
	reads := make(chan read, 1)
	go func() {
		for {
			buffer := bufpool.Get(1 << 19)
			bufferBytes := buffer.Bytes()
			n, err := io.ReadFull(stream, bufferBytes)
			buffer.Shrink(uint(n))
			if err == io.ErrUnexpectedEOF {
				err = io.EOF
			}
			reads <- read{buffer, err}
			if err != nil {
				close(reads)
				return
			}
		}
	}()

	for read := range reads {
		buf := read.buf
		if read.err != nil && read.err != io.EOF {
			buf.Free()
			errReader := strings.NewReader(read.err.Error())
			err := WriteStream(ctx, c, errReader, SourceErr)
			if err != nil {
				return err
			}
			return nil
		}
		// next line is the hot path...
		writeErr := c.WriteFrame(buf.Bytes(), stype)
		buf.Free()
		if writeErr != nil {
			return writeErr
		}
		if read.err == io.EOF {
			if err := c.WriteFrame([]byte{}, SourceEOF); err != nil {
				return err
			}
			break
		}
	}

	return nil
}

type ReadStreamErrorKind int

const (
	ReadStreamErrorKindConn ReadStreamErrorKind = 1 + iota
	ReadStreamErrorKindWrite
	ReadStreamErrorKindSource
	ReadStreamErrorKindSourceErrEncoding
	ReadStreamErrorKindUnexpectedFrameType
)

type ReadStreamError struct {
	Kind ReadStreamErrorKind
	Err  error
}

// ReadStream will close c if an error reading  from c or writing to receiver occurs
func ReadStream(ctx context.Context, c *heartbeatconn.Conn, receiver io.Writer, stype uint32) *ReadStreamError {

	type read struct {
		f   frameconn.Frame
		err error
	}
	reads := make(chan read, 1)
	go func() {
		for {
			var r read
			r.f, r.err = c.ReadFrame()
			reads <- r
			if r.err != nil || r.f.Header.Type == SourceEOF || r.f.Header.Type == SourceErr {
				close(reads)
				return
			}
		}
	}()

	var f frameconn.Frame
	for read := range reads {
		f = read.f
		if read.err != nil {
			return &ReadStreamError{ReadStreamErrorKindConn, read.err}
		}
		if f.Header.Type != stype {
			break
		}

		n, err := receiver.Write(f.Buffer.Bytes())
		if err != nil {
			f.Buffer.Free()
			return &ReadStreamError{ReadStreamErrorKindWrite, err} // FIXME wrap as writer error
		}
		if n != len(f.Buffer.Bytes()) {
			f.Buffer.Free()
			return &ReadStreamError{ReadStreamErrorKindWrite, io.ErrShortWrite}
		}
		f.Buffer.Free()
	}

	if f.Header.Type == SourceEOF {
		return nil
	}

	if f.Header.Type == SourceErr {
		if !utf8.Valid(f.Buffer.Bytes()) {
			return &ReadStreamError{ReadStreamErrorKindSourceErrEncoding, fmt.Errorf("source error, but not encoded as UTF-8")}
		}
		return &ReadStreamError{ReadStreamErrorKindSource, fmt.Errorf("%s", string(f.Buffer.Bytes()))}
	}

	return &ReadStreamError{ReadStreamErrorKindUnexpectedFrameType, fmt.Errorf("unexpected frame type")}
}
