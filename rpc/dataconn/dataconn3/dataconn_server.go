package dataconn

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/zrepl/zrepl/logger"
	"github.com/zrepl/zrepl/replication/pdu"
	"github.com/zrepl/zrepl/rpc/dataconn/heartbeatconn"
	"github.com/zrepl/zrepl/rpc/dataconn/stream"
)

// WireInterceptor has a chance to exchange the context and connection on each client connection.
type WireInterceptor func(ctx context.Context, conn net.Conn) (context.Context, net.Conn)

type StreamCopier func(io.Writer) error

// Handler implements the functionality that is exposed by Server to the Client.
type Handler interface {
	// Send handles a SendRequest.
	// The returned io.ReadCloser is allowed to be nil, for example if the requested Send is a dry-run.
	Send(ctx context.Context, r *pdu.SendReq) (*pdu.SendRes, io.ReadCloser, error)
	// Receive handles a ReceiveRequest.
	// It is guaranteed that Server calls Receive with a stream that holds the IdleConnTimeout
	// configured in ServerConfig.Shared.IdleConnTimeout.
	Receive(ctx context.Context, r *pdu.ReceiveReq, receive StreamCopier) (*pdu.ReceiveRes, error)
}

type Logger = logger.Logger

type ServerConfig struct {
	Shared        SharedConfig
	HeaderTimeout time.Duration `validate:"gte=0"`
}

func (c ServerConfig) Validate() error {
	return validate.Struct(c)
}

type Server struct {
	h   Handler
	wi  WireInterceptor
	log Logger
}

func NewServer(wi WireInterceptor, logger Logger, handler Handler) *Server {
	return &Server{
		h:   handler,
		wi:  wi,
		log: logger,
	}
}

// Serve consumes the listener, closes it as soon as ctx is closed.
// No accept errors are returned: they are logged to the Logger passed
// to the constructor.
func (s *Server) Serve(ctx context.Context, l net.Listener) {

	go func() {
		<-ctx.Done()
		s.log.Debug("context done")
		if err := l.Close(); err != nil {
			s.log.WithError(err).Error("cannot close listener")
		}
	}()
	conns := make(chan net.Conn)
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				if ctx.Done() != nil {
					s.log.Debug("stop accepting after context is done")
					return
				}
				s.log.WithError(err).Error("accept error")
				continue
			}
			conns <- conn
		}
	}()
	for conn := range conns {
		go s.serveConn(conn)
	}
}

func (s *Server) serveConn(nc net.Conn) {

	ctx := context.Background()
	if s.wi != nil {
		ctx, nc = s.wi(ctx, nc)
	}

	c := heartbeatconn.Wrap(nc, HeartbeatInterval, HeartbeatPeerTimeout)
	defer func() {
		if err := c.Close(); err != nil {
			s.log.WithError(err).Error("cannot close client connection")
		}
	}()

	header, err := readMessage(ctx, c, 1<<15, ReqHeader)
	if err != nil {
		s.log.WithError(err).Error("error reading structured part")
		return
	}
	endpoint := string(header)

	reqStructured, err := readMessage(ctx, c, 1<<22, ReqStructured)
	if err != nil {
		s.log.WithError(err).Error("error reading structured part")
		return
	}

	var res proto.Message
	var sendStream io.ReadCloser
	var handlerErr error
	switch endpoint {
	case EndpointSend:
		var req pdu.SendReq
		if err := proto.Unmarshal(reqStructured, &req); err != nil {
			s.log.WithError(err).Error("cannot unmarshal send request")
			return
		}
		res, sendStream, handlerErr = s.h.Send(ctx, &req) // SHADOWING
	case EndpointRecv:
		var req pdu.ReceiveReq
		if err := proto.Unmarshal(reqStructured, &req); err != nil {
			s.log.WithError(err).Error("cannot unmarshal receive request")
			return
		}
		receive := func(w io.Writer) error {
			return stream.ReadStream(ctx, c, w, Stream)
		}
		res, handlerErr = s.h.Receive(ctx, &req, receive) // SHADOWING
	default:
		s.log.WithField("endpoint", endpoint).Error("unknown endpoint")
		handlerErr = fmt.Errorf("requested endpoint does not exist")
		return
	}

	var resHeaderBuf bytes.Buffer
	if handlerErr == nil {
		resHeaderBuf.WriteString("HANDLER OK\n")
	} else {
		resHeaderBuf.WriteString("HANDLER ERROR:\n")
		resHeaderBuf.WriteString(handlerErr.Error())
	}
	if err := stream.WriteStream(ctx, c, &resHeaderBuf, ResHeader); err != nil {
		s.log.WithError(err).Error("cannot write response header")
		return
	}

	if handlerErr != nil {
		s.log.Debug("stop serving connection after handler error")
		return
	}

	protobufBytes, err := proto.Marshal(res)
	if err != nil {
		s.log.WithError(err).Error("cannot marshal handler protobuf")
		return
	}
	protobuf := bytes.NewBuffer(protobufBytes)
	if err := stream.WriteStream(ctx, c, protobuf, ResStructured); err != nil {
		s.log.WithError(err).Error("cannot write structured part of response")
		return
	}

	if sendStream != nil {
		err := stream.WriteStream(ctx, c, sendStream, Stream)
		if err != nil {
			s.log.WithError(err).Error("cannot write send stream")
		}
	}

	return
}
