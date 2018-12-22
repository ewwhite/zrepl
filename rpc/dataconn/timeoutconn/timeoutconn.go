package timeoutconn

import (
	"fmt"
	"io"
	"net"
	"os"
	"syscall"
	"time"
	"unsafe"
)

type Conn struct {
	net.Conn
	idleTimeout time.Duration
}

func Wrap(conn net.Conn, idleTimeout time.Duration) Conn {
	return Conn{Conn: conn, idleTimeout: idleTimeout}
}

func (c Conn) Read(p []byte) (n int, err error) {
	n = 0
	err = nil
restart:
	if err := c.SetDeadline(time.Now().Add(c.idleTimeout)); err != nil {
		return n, err
	}
	var nCurRead int
	nCurRead, err = c.Conn.Read(p[n:len(p)])
	n += nCurRead
	if netErr, ok := err.(net.Error); ok && netErr.Timeout() && nCurRead > 0 {
		err = nil
		goto restart
	}
	return n, err
}

func (c Conn) Write(p []byte) (n int, err error) {
	n = 0
restart:
	if err := c.SetDeadline(time.Now().Add(c.idleTimeout)); err != nil {
		return n, err
	}
	var nCurWrite int
	nCurWrite, err = c.Conn.Write(p[n:len(p)])
	n += nCurWrite
	if netErr, ok := err.(net.Error); ok && netErr.Timeout() && nCurWrite > 0 {
		err = nil
		goto restart
	}
	return n, err
}

func (c Conn) WritevFull(bufs net.Buffers) (n int64, err error) {
	n = 0
restart:
	if err := c.SetDeadline(time.Now().Add(c.idleTimeout)); err != nil {
		return n, err
	}
	var nCurWrite int64
	nCurWrite, err = io.Copy(c.Conn, &bufs)
	n += nCurWrite
	if netErr, ok := err.(net.Error); ok && netErr.Timeout() && nCurWrite > 0 {
		err = nil
		goto restart
	}
	return n, err
}

type SyscallConner interface {
	SyscallConn() (syscall.RawConn, error)
}

// Reads the given buffers full, like io.ReadvFull but using the readv syscall.
func (c Conn) ReadvFull(buffers [][]byte) (n int64, err error) {
	totalLen, _ := buildIovecs(buffers)
	defer func() {
		if err != nil {
			fmt.Fprintf(os.Stderr, "ReadvFull error %v\n", err)
		}
		if totalLen != n {
			fmt.Fprintf(os.Stderr, "ReadvFull short%v %v\n", totalLen, n)
			panic("ReadvFull short")
		}
	}()
	scc, ok := c.Conn.(SyscallConner)
	if !ok {
		return c.readvFallback(buffers)
	}
	raw, err := scc.SyscallConn()
	if err != nil {
		return 0, err
	}
	n, err = c.readv(raw, buffers)
	return
}

func (c Conn) readvFallback(buffers [][]byte) (n int64, err error) {
	panic("implement me")
}

func buildIovecs(buffers [][]byte) (totalLen int64, vecs []syscall.Iovec) {
	vecs = make([]syscall.Iovec, 0, len(buffers))
	for i := range buffers {
		totalLen += int64(len(buffers[i]))
		if len(buffers[i]) == 0 {
			continue
		}
		vecs = append(vecs, syscall.Iovec{
			Base: &buffers[i][0],
			Len:  uint64(len(buffers[i])),
		})
	}
	return totalLen, vecs
}

func (c Conn) readv(rawConn syscall.RawConn, buffers [][]byte) (n int64, err error) {
	fmt.Fprintf(os.Stderr, "EXT readv\n")
	defer fmt.Fprintf(os.Stderr, "RET readv\n")
	_, iovecs := buildIovecs(buffers)

	for len(iovecs) > 0 {
		if err := c.SetDeadline(time.Now().Add(c.idleTimeout)); err != nil {
			return n, err
		}
		oneN, oneErr := c.doOneReadv(rawConn, &iovecs)
		n += oneN
		if netErr, ok := oneErr.(net.Error); ok && netErr.Timeout() && oneN > 0 { // TODO likely not working
			continue
		} else if oneErr == nil && oneN > 0 {
			continue
		} else {
			return n, oneErr
		}
	}
	return n, nil
}

func (c Conn) doOneReadv(rawConn syscall.RawConn, iovecs *[]syscall.Iovec) (n int64, err error) {
	fmt.Fprintf(os.Stderr, "doOneReadv\n")

	rawReadErr := rawConn.Read(func(fd uintptr) (done bool) {
		// iovecs, n and err must not be shadowed!
		fmt.Fprintf(os.Stderr, "enter readv\n")
		thisReadN, x, errno := syscall.Syscall(
			syscall.SYS_READV,
			fd,
			uintptr(unsafe.Pointer(&(*iovecs)[0])),
			uintptr(len(*iovecs)),
		)
		fmt.Fprintf(os.Stderr, "done readv %v %v\n", thisReadN, x)
		if thisReadN == ^uintptr(0) {
			if errno == syscall.EAGAIN {
				fmt.Fprintf(os.Stderr, "EAGAIN\n")
				return false
			}
			err = syscall.Errno(errno)
			return true
		}
		if int(thisReadN) < 0 {
			panic("unexpected return value")
		}
		n += int64(thisReadN)
		// shift iovecs forward
		for left := int64(thisReadN); left > 0; {
			curVecNewLength := int64((*iovecs)[0].Len) - left // TODO assert conversion
			if curVecNewLength <= 0 {
				left -= int64((*iovecs)[0].Len)
				*iovecs = (*iovecs)[1:]
			} else {
				(*iovecs)[0].Base = (*byte)(unsafe.Pointer(uintptr(unsafe.Pointer((*iovecs)[0].Base)) + uintptr(left)))
				(*iovecs)[0].Len = uint64(curVecNewLength)
				break // inner
			}
		}
		if thisReadN == 0 {
			err = io.EOF
			return true
		}
		return true
	})

	if rawReadErr != nil {
		err = rawReadErr
	}

	return n, err
}
