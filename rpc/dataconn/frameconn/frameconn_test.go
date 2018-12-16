package frameconn

import (
	"net"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zrepl/zrepl/util/socketpair"
)

func TestDataConnSendRecv(t *testing.T) {

	anc, bnc, err := socketpair.SocketPair()
	require.NoError(t, err)
	defer anc.Close()
	defer bnc.Close()

	ac, bc := NewDataConn(anc), NewDataConn(bnc)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		var f Frame
		f.Type = 0x23
		f.Buffer = []byte{23, 42, 0, 0, 0}
		f.BufferLen = 2
		err := ac.WriteFrame(&f)
		assert.Equal(t, 5, len(f.Buffer))
		require.NoError(t, err)
	}()

	go func() {
		defer wg.Done()
		var f Frame
		f.Buffer = make([]byte, 1024)
		err := bc.ReadFrame(&f)
		assert.NoError(t, err)
		assert.Equal(t, 2, f.BufferLen)
		assert.Equal(t, uint32(0x23), f.Type)
		assert.Equal(t, []byte{23, 42}, f.Buffer[0:f.BufferLen])
		assert.Equal(t, 1024, len(f.Buffer))
	}()

	wg.Wait()
}

func TestDataConnShortRead(t *testing.T) {
	anc, bnc, err := socketpair.SocketPair()
	require.NoError(t, err)
	defer anc.Close()
	defer bnc.Close()

	ac, bc := NewDataConn(anc), NewDataConn(bnc)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		var f Frame
		f.Type = 0x42 
		f.Buffer = []byte{23, 42}
		f.BufferLen = 2
		err := ac.WriteFrame(&f)
		require.NoError(t, err)
	}()

	go func() {
		defer wg.Done()
		var f Frame
		f.Buffer = make([]byte, 1)
		err := bc.ReadFrame(&f)
		assert.Equal(t, ErrReadFrameLengthShort, err)
	}()

	wg.Wait()
}

type mockConn struct {
	net.Conn
	closeCount int
}

func (m *mockConn) Close() error {
	m.closeCount++
	return nil
}

func TestDataConnCloseOnce(t *testing.T) {
	mc := &mockConn{}
	c := NewDataConn(mc)
	c.Close()
	assert.Equal(t, 1, mc.closeCount)
	c.Close() // no effect on underlying conn
	assert.Equal(t, 1, mc.closeCount)
}
