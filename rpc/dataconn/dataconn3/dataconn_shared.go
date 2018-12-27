package dataconn

import (
	"io"
	"sync"
	"time"

	"github.com/go-playground/validator"
	"github.com/zrepl/zrepl/rpc/dataconn/stream"
	"github.com/zrepl/zrepl/zfs"
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
	ZFSStream
)

const (
	HeartbeatInterval    = 5 * time.Second
	HeartbeatPeerTimeout = 10 * time.Second
)

type streamCopier struct {
	mtx  sync.Mutex
	used bool
	*stream.Conn
}

// WriteStreamTo reads a stream from Conn and writes it to w.
func (s *streamCopier) WriteStreamTo(w io.Writer) zfs.StreamCopierError {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	if s.used {
		panic("streamCopier used mulitple times")
	}
	s.used = true
	return s.ReadStreamInto(w, ZFSStream)
}
