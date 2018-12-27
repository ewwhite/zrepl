package stream

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"strings"
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

type errReader struct {
	t       *testing.T
	readErr error
}

func (er errReader) Read(p []byte) (n int, err error) {
	er.t.Logf("errReader.Read called")
	return 0, er.readErr
}

func TestMultiFrameSourceError(t *testing.T) {
	anc, bnc, err := socketpair.SocketPair()
	require.NoError(t, err)

	hto := 1 * time.Hour
	a := heartbeatconn.Wrap(anc, hto, hto)
	b := heartbeatconn.Wrap(bnc, hto, hto)

	log := logger.NewStderrDebugLogger()
	ctx := WithLogger(context.Background(), log)
	stype := uint32(0x23)

	longErr := fmt.Errorf("an error that definitley spans more than one frame:\n%s", strings.Repeat("a\n", 1<<4))

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		defer log.Printf("WriteStream done")
		r := errReader{t, longErr}
		WriteStream(ctx, a, &r, stype)
		time.Sleep(4 * time.Second)
		a.Close()
	}()

	go func() {
		defer wg.Done()
		defer log.Printf("ReadStream done")
		defer b.Close()
		defer log.Printf("reader pre-close")
		var buf bytes.Buffer
		err := ReadStream(b, &buf, stype)
		require.NotNil(t, err)
		assert.True(t, buf.Len() == 0)
		assert.Equal(t, err.Kind, ReadStreamErrorKindSource)
		receivedErr := err.Err.Error()
		expectedErr := longErr.Error()
		assert.True(t, receivedErr == expectedErr) // builtin Equals is too slow
		if receivedErr != expectedErr {
			log.Printf("lengths: %v %v", len(receivedErr), len(expectedErr))
			ioutil.WriteFile("/tmp/received.txt", []byte(receivedErr), 0600)
			ioutil.WriteFile("/tmp/expected.txt", []byte(expectedErr), 0600)
		}

	}()

	wg.Wait()
}
