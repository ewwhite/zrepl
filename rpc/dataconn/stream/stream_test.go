package stream

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zrepl/zrepl/logger"
	"github.com/zrepl/zrepl/rpc/dataconn/frameconn2"
	"github.com/zrepl/zrepl/rpc/dataconn/heartbeatconn"
	"github.com/zrepl/zrepl/util/socketpair"
)

func TestFrameTypesOk(t *testing.T) {
	t.Logf("%v", SourceEOF)
	assert.True(t, frameconn.IsPublicFrameType(SourceEOF))
	assert.True(t, frameconn.IsPublicFrameType(SourceErr))

	assert.True(t, IsPublicFrameType(0))
	assert.True(t, IsPublicFrameType(1))
	assert.True(t, IsPublicFrameType(255))
}

func TestStreamer(t *testing.T) {

	anc, bnc, err := socketpair.SocketPair()
	require.NoError(t, err)

	hto := 1 * time.Hour
	a := heartbeatconn.Wrap(anc, hto, hto)
	b := heartbeatconn.Wrap(bnc, hto, hto)

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
		err := ReadStream(b, &buf, stype)
		log.WithField("errType", fmt.Sprintf("%T %v", err, err)).Debug("ReadStream returned")
		assert.Nil(t, err)
		expected := bytes.Repeat([]byte{1, 2}, 1<<25)
		assert.True(t, bytes.Equal(expected, buf.Bytes()))
		b.Close()
	}()

	wg.Wait()

}
