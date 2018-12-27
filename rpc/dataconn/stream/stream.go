package stream

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"unicode/utf8"

	"github.com/zrepl/zrepl/logger"
	"github.com/zrepl/zrepl/rpc/dataconn/base2bufpool"
	frameconn "github.com/zrepl/zrepl/rpc/dataconn/frameconn2"
	"github.com/zrepl/zrepl/rpc/dataconn/heartbeatconn"
	"github.com/zrepl/zrepl/zfs"
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

// Frame types used by this package.
// 4 MSBs are reserved for frameconn, next 4 MSB for heartbeatconn, next 4 MSB for us.
const (
	StreamErrTrailer uint32 = 1 << (16 + iota)
	End
	// max 16
)

// NOTE: make sure to add a tests for each frame type that checks
//       whether it is heartbeatconn.IsPublicFrameType()

// Check whether the given frame type is allowed to be used by
// consumers of this package. Intended for use in unit tests.
func IsPublicFrameType(ft uint32) bool {
	return frameconn.IsPublicFrameType(ft) && heartbeatconn.IsPublicFrameType(ft) && ((0xf<<16)&ft == 0)
}

const FramePayloadShift = 19

var bufpool = base2bufpool.New(FramePayloadShift, FramePayloadShift, base2bufpool.Panic)

// if sendStream returns an error, that error will be sent as a trailer to the client
// ok will return nil, though.
func writeStream(ctx context.Context, c *heartbeatconn.Conn, stream io.Reader, stype uint32) (errStream, errConn error) {
	fmt.Fprintf(os.Stderr, "writeStream: enter %v\n", stype)
	defer fmt.Fprintf(os.Stderr, "writeStream: return\n")
	if stype == 0 {
		panic("stype must be non-zero")
	}
	if !IsPublicFrameType(stype) {
		panic(fmt.Sprintf("stype %v is not public", stype))
	}
	return doWriteStream(ctx, c, stream, stype)
}

func doWriteStream(ctx context.Context, c *heartbeatconn.Conn, stream io.Reader, stype uint32) (errStream, errConn error) {
	type read struct {
		buf base2bufpool.Buffer
		err error
	}
	reads := make(chan read, 5)
	go func() {
		for {
			buffer := bufpool.Get(1 << FramePayloadShift)
			bufferBytes := buffer.Bytes()
			//			fmt.Fprintf(os.Stderr, "writeStream: read full begin\n");
			n, err := io.ReadFull(stream, bufferBytes)
			//			fmt.Fprintf(os.Stderr, "writeStream: read full end %v %v\n", err, n);
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
			errReadErrReader, errConnWrite := doWriteStream(ctx, c, errReader, StreamErrTrailer)
			if errReadErrReader != nil {
				panic(errReadErrReader) // in-memory, cannot happen
			}
			return read.err, errConnWrite
		}
		// next line is the hot path...
		writeErr := c.WriteFrame(buf.Bytes(), stype)
		buf.Free()
		if writeErr != nil {
			return nil, writeErr
		}
		if read.err == io.EOF {
			if err := c.WriteFrame([]byte{}, End); err != nil {
				return nil, err
			}
			break
		}
	}

	return nil, nil
}

type ReadStreamErrorKind int

const (
	ReadStreamErrorKindConn ReadStreamErrorKind = 1 + iota
	ReadStreamErrorKindWrite
	ReadStreamErrorKindSource
	ReadStreamErrorKindStreamErrTrailerEncoding
	ReadStreamErrorKindUnexpectedFrameType
)

type ReadStreamError struct {
	Kind ReadStreamErrorKind
	Err  error
}

func (e *ReadStreamError) Error() string {
	kindStr := ""
	switch e.Kind {
	case ReadStreamErrorKindConn:
		kindStr = " read error: "
	case ReadStreamErrorKindWrite:
		kindStr = " write error: "
	case ReadStreamErrorKindSource:
		kindStr = " source error: "
	case ReadStreamErrorKindStreamErrTrailerEncoding:
		kindStr = " source implementation error: "
	case ReadStreamErrorKindUnexpectedFrameType:
		kindStr = " protocol error: "
	}
	return fmt.Sprintf("stream:%s%s", kindStr, e.Err)
}

var _ net.Error = &ReadStreamError{}

func (e ReadStreamError) netErr() net.Error {
	if netErr, ok := e.Err.(net.Error); ok {
		return netErr
	}
	return nil
}

func (e ReadStreamError) Timeout() bool {
	if netErr := e.netErr(); netErr != nil {
		return netErr.Timeout()
	}
	return false
}

func (e ReadStreamError) Temporary() bool {
	if netErr := e.netErr(); netErr != nil {
		return netErr.Temporary()
	}
	return false
}

var _ zfs.StreamCopierError = &ReadStreamError{}

func (e ReadStreamError) IsReadError() bool {
	return e.Kind != ReadStreamErrorKindWrite
}

func (e ReadStreamError) IsWriteError() bool {
	return e.Kind == ReadStreamErrorKindWrite
}

type readStreamResult struct {
	f   frameconn.Frame
	err error
}

// ReadStream will close c if an error reading  from c or writing to receiver occurs
func readStream(reads chan readStreamResult, c *heartbeatconn.Conn, receiver io.Writer, stype uint32) *ReadStreamError {
	go func() { // FIXME only one per conn, move this out of here
		for {
			var r readStreamResult
			r.f, r.err = c.ReadFrame()
			reads <- r
			if r.err != nil || r.f.Header.Type == End {
				return
			}
		}
	}()

	var f frameconn.Frame
	for read := range reads {
		fmt.Fprintf(os.Stderr, "readStream.didRead %v %v\n", read.err, read.f)
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

	if f.Header.Type == End {
		fmt.Fprintf(os.Stderr, "End reached\n")
		return nil
	}

	if f.Header.Type == StreamErrTrailer {
		fmt.Fprintf(os.Stderr, "begin of StreamErrTrailer\n")
		var errBuf bytes.Buffer
		if n, err := errBuf.Write(f.Buffer.Bytes()); n != len(f.Buffer.Bytes()) || err != nil {
			panic(fmt.Sprintf("unexpected bytes.Buffer write error: %v %v", n, err))
		}
		// recursion ftw! we won't enter this if stmt because stype == StreamErrTrailer in the following call
		rserr := readStream(reads, c, &errBuf, StreamErrTrailer)
		if rserr != nil && rserr.Kind == ReadStreamErrorKindWrite {
			panic(fmt.Sprintf("unexpected bytes.Buffer write error: %s", rserr))
		} else if rserr != nil {
			fmt.Fprintf(os.Stderr, "rserr != nil && != ReadStreamErrorKindWrite: %v %v\n", rserr.Kind, rserr.Err)
			return rserr
		}
		if !utf8.Valid(errBuf.Bytes()) {
			return &ReadStreamError{ReadStreamErrorKindStreamErrTrailerEncoding, fmt.Errorf("source error, but not encoded as UTF-8")}
		}
		return &ReadStreamError{ReadStreamErrorKindSource, fmt.Errorf("%s", errBuf.String())}
	}

	return &ReadStreamError{ReadStreamErrorKindUnexpectedFrameType, fmt.Errorf("unexpected frame type %v (expected %v)", f.Header.Type, stype)}
}
