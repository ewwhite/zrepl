package dataconn

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/go-playground/validator"
	"github.com/zrepl/zrepl/rpc/dataconn/frameconn2"
	"github.com/zrepl/zrepl/rpc/dataconn/stream"
)

type SharedConfig struct {
	MaxProtoLen      uint32        `validate:"gt=0"`
	MaxHeaderLen     uint32        `validate:"gt=0"`
	SendChunkSize    uint32        `validate:"gt=0"`
	MaxRecvChunkSize uint32        `validate:"gt=0"`
	IdleConnTimeout  time.Duration `validate:"gte=0"`
}

var validate = validator.New()

const (
	EndpointSend string = "/v1/send"
	EndpointRecv string = "/v1/recv"
)

const (
	ReqHeader uint32 = 1 + iota
	ReqStructured
	ResHeader
	ResStructured
	Stream
)

var readMessageSentinel = fmt.Errorf("read stream complete")

func readMessage(ctx context.Context, conn *frameconn.Conn, maxSize uint32, frameType uint32) (b []byte, err error) {
	r, w := io.Pipe()
	var buf bytes.Buffer
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		lr := io.LimitReader(r, int64(maxSize))
		if _, err := io.Copy(&buf, lr); err != nil && err != readMessageSentinel {
			panic(err)
		}
	}()
	err = stream.ReadStream(ctx, conn, w, frameType)
	w.CloseWithError(readMessageSentinel)
	wg.Wait()
	return buf.Bytes(), err
}
