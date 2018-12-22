// package timeoutconn wraps a net.Conn to provide idle timeouts
// based on Set{Read,Write}Deadline.
// Additionally, it exports abstractions for vectored I/O.
package timeoutconn

// NOTE
// Readv and Writev are not split-off into a separate package
// because we use raw syscalls, bypassing Conn's Read / Write methods.

import (
	"io"
	"net"
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
	if err := c.SetReadDeadline(time.Now().Add(c.idleTimeout)); err != nil {
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
	if err := c.SetWriteDeadline(time.Now().Add(c.idleTimeout)); err != nil {
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

// Writes the given buffers to Conn, following the sematincs of io.Copy,
// but is guaranteed to use the writev system call if the wrapped net.Conn
// support it.
// Note the Conn does not support writev through io.Copy(aConn, aNetBuffers).
func (c Conn) WritevFull(bufs net.Buffers) (n int64, err error) {
	n = 0
restart:
	if err := c.SetWriteDeadline(time.Now().Add(c.idleTimeout)); err != nil {
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

// The interface that must be implemented for vectored I/O support.
// If the wrapped net.Conn does not implement it, a less efficient
// fallback implementation is used.
// Rest assured that Go's *net.TCPConn implements this interface.
type SyscallConner interface {
	SyscallConn() (syscall.RawConn, error)
}

var _ SyscallConner = (*net.TCPConn)(nil)

func buildIovecs(buffers net.Buffers) (totalLen int64, vecs []syscall.Iovec) {
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

// Reads the given buffers full:
// Think of io.ReadvFull, but for net.Buffers + using the readv syscall.
//
// If the underlying net.Conn is not a SyscallConner, a fallback
// ipmlementation based on repeated Conn.Read invocations is used.
func (c Conn) ReadvFull(buffers net.Buffers) (n int64, err error) {
	totalLen, iovecs := buildIovecs(buffers)
	if debugReadvNoShortReadsAssertEnable {
		defer debugReadvNoShortReadsAssert(totalLen, n, err)
	}
	scc, ok := c.Conn.(SyscallConner)
	if !ok {
		return c.readvFallback(buffers)
	}
	raw, err := scc.SyscallConn()
	if err != nil {
		return 0, err
	}
	n, err = c.readv(raw, iovecs)
	return
}

func (c Conn) readvFallback(buffers net.Buffers) (n int64, err error) {
	for i := range buffers {
		thisN, err := io.ReadFull(c, buffers[i])
		n += int64(thisN)
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

func (c Conn) readv(rawConn syscall.RawConn, iovecs []syscall.Iovec) (n int64, err error) {
	for len(iovecs) > 0 {
		if err := c.SetReadDeadline(time.Now().Add(c.idleTimeout)); err != nil {
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
	rawReadErr := rawConn.Read(func(fd uintptr) (done bool) {
		// iovecs, n and err must not be shadowed!
		thisReadN, _, errno := syscall.Syscall(
			syscall.SYS_READV,
			fd,
			uintptr(unsafe.Pointer(&(*iovecs)[0])),
			uintptr(len(*iovecs)),
		)
		if thisReadN == ^uintptr(0) {
			if errno == syscall.EAGAIN {
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
