package timeoutconn

import (
	"net"
	"time"
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
