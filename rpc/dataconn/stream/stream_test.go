package stream

import (
	"bytes"
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zrepl/zrepl/logger"
	"github.com/zrepl/zrepl/rpc/dataconn/frameconn2"
	"github.com/zrepl/zrepl/util/socketpair"
)

func TestFrameTypesOk(t *testing.T) {
	t.Logf("%v", SourceEOF)
	assert.False(t, IsPublicFrameType(SourceEOF))
	assert.False(t, IsPublicFrameType(SourceErr))
	assert.True(t, IsPublicFrameType(0))
	assert.True(t, IsPublicFrameType(1))
	assert.True(t, IsPublicFrameType(255))
}

func TestStreamer(t *testing.T) {

	anc, bnc, err := socketpair.SocketPair()
	require.NoError(t, err)

	a := frameconn.Wrap(anc)
	b := frameconn.Wrap(bnc)

	log := logger.NewStderrDebugLogger()
	ctx := WithLogger(context.Background(), log)
	stype := uint32(0x23)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		var buf bytes.Buffer
		buf.Write(
			bytes.Repeat([]byte{1, 2}, 1<<25),
		)
		WriteStream(ctx, a, &buf, stype)
		log.Debug("WriteStream returned")
		a.Close()
	}()

	go func() {
		defer wg.Done()
		var buf bytes.Buffer
		err := ReadStream(ctx, b, &buf, stype)
		log.Debug("ReadStream returned")
		assert.NoError(t, err)
		expected := bytes.Repeat([]byte{1, 2}, 1<<25)
		assert.True(t, bytes.Equal(expected, buf.Bytes()))
		b.Close()
	}()

	wg.Wait()

}
