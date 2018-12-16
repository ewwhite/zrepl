package dataconn

import (
	"bytes"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zrepl/zrepl/logger"
	"github.com/zrepl/zrepl/rpc/dataconn/frameconn2"
	"github.com/zrepl/zrepl/util/socketpair"
)

func TestStreamer(t *testing.T) {

	anc, bnc, err := socketpair.SocketPair()
	require.NoError(t, err)

	a := frameconn.NewDataConn(anc)
	b := frameconn.NewDataConn(bnc)
	defer a.Close()
	defer b.Close()

	log := logger.NewTestLogger(t)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		var buf bytes.Buffer
		buf.Write(
			bytes.Repeat([]byte{1, 2}, 1<<25),
		)
		sender := streamer{log}
		sender.writeSendStream(a, &buf)
	}()

	go func() {
		defer wg.Done()
		var buf bytes.Buffer
		receiver := streamer{log}
		receiver.readSendStream(b, &buf)
		expected := bytes.Repeat([]byte{1, 2}, 1<<25)
		assert.True(t, bytes.Equal(expected, buf.Bytes()))
	}()

	wg.Wait()

}
