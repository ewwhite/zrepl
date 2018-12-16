package timeoutconn

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zrepl/zrepl/util/socketpair"
)

func TestReadTimeout(t *testing.T) {

	a, b, err := socketpair.SocketPair()
	require.NoError(t, err)
	defer a.Close()
	defer b.Close()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		var buf bytes.Buffer
		buf.WriteString("tooktoolong")
		time.Sleep(500 * time.Millisecond)
		_, err := io.Copy(a, &buf)
		require.NoError(t, err)
	}()

	go func() {
		defer wg.Done()
		conn := Wrap(b, 100*time.Millisecond)
		buf := [4]byte{} // shorter than message put on wire
		n, err := conn.Read(buf[:])
		assert.Equal(t, 0, n)
		assert.Error(t, err)
		netErr, ok := err.(net.Error)
		require.True(t, ok)
		assert.True(t, netErr.Timeout())
	}()

	wg.Wait()
}

type writeBlockConn struct {
	net.Conn
	blockTime time.Duration
}

func (c writeBlockConn) Write(p []byte) (int, error) {
	time.Sleep(c.blockTime)
	return c.Conn.Write(p)
}

func TestWriteTimeout(t *testing.T) {
	a, b, err := socketpair.SocketPair()
	require.NoError(t, err)
	defer a.Close()
	defer b.Close()
	var buf bytes.Buffer
	buf.WriteString("message")
	blockConn := writeBlockConn{a, 500 * time.Millisecond}
	conn := Wrap(blockConn, 100*time.Millisecond)
	n, err := conn.Write(buf.Bytes())
	assert.Equal(t, 0, n)
	assert.Error(t, err)
	netErr, ok := err.(net.Error)
	require.True(t, ok)
	assert.True(t, netErr.Timeout())
}

func TestNoPartialReadsDueToDeadline(t *testing.T) {
	a, b, err := socketpair.SocketPair()
	require.NoError(t, err)
	defer a.Close()
	defer b.Close()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		a.Write([]byte{1, 2, 3, 4, 5})
		time.Sleep(150 * time.Millisecond)
		a.Write([]byte{6, 7, 8, 9, 10})
	}()

	go func() {
		defer wg.Done()
		bc := Wrap(b, 100*time.Millisecond)
		beginRead := time.Now()
		var fillBuf [10]byte
		bc.Read(fillBuf[:])
		readDuration := time.Now().Sub(beginRead)
		fmt.Fprintf(os.Stderr, "fillBuf: %v\n", fillBuf)
		assert.Equal(t, []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, fillBuf[:])
		assert.True(t, readDuration > 100*time.Millisecond)
		assert.True(t, readDuration < 200*time.Millisecond)
	}()

	wg.Wait()
}

type partialWriteMockConn struct {
	net.Conn                // to satisfy interface
	buf                     bytes.Buffer
	writeDuration           time.Duration
	returnAfterBytesWritten int
}

func newPartialWriteMockConn(writeDuration time.Duration, returnAfterBytesWritten int) *partialWriteMockConn {
	return &partialWriteMockConn{
		writeDuration:           writeDuration,
		returnAfterBytesWritten: returnAfterBytesWritten,
	}
}

func (c *partialWriteMockConn) Write(p []byte) (int, error) {
	time.Sleep(c.writeDuration)
	consumeBytes := len(p)
	if consumeBytes > c.returnAfterBytesWritten {
		consumeBytes = c.returnAfterBytesWritten
	}
	n, err := c.buf.Write(p[0:consumeBytes])
	if err != nil || n != consumeBytes {
		panic("bytes.Buffer behaves unexpectedly")
	}
	return n, nil
}

func TestPartialWriteMockConn(t *testing.T) {
	mc := newPartialWriteMockConn(100*time.Millisecond, 5)
	buf := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	begin := time.Now()
	n, err := mc.Write(buf[:])
	duration := time.Now().Sub(begin)
	assert.NoError(t, err)
	assert.Equal(t, 5, n)
	assert.True(t, duration > 100*time.Millisecond)
	assert.True(t, duration < 150*time.Millisecond)
}

func TestNoPartialWritesDueToDeadline(t *testing.T) {
	a, b, err := socketpair.SocketPair()
	require.NoError(t, err)
	defer a.Close()
	defer b.Close()
	var buf bytes.Buffer
	buf.WriteString("message")
	blockConn := writeBlockConn{a, 150 * time.Millisecond}
	conn := Wrap(blockConn, 100*time.Millisecond)
	n, err := conn.Write(buf.Bytes())
	assert.Equal(t, 0, n)
	assert.Error(t, err)
	netErr, ok := err.(net.Error)
	require.True(t, ok)
	assert.True(t, netErr.Timeout())
}
