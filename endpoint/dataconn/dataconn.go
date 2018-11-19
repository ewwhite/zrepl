package dataconn

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/zrepl/zrepl/daemon/transport/connecter"
	"github.com/zrepl/zrepl/daemon/transport/serve"
	"github.com/zrepl/zrepl/logger"
	"github.com/zrepl/zrepl/replication/pdu"
)

type contextKey int

const (
	contextKeyLog = 1 + iota
)

type Logger = logger.Logger

func WithLogger(ctx context.Context, logger Logger) context.Context {
	return context.WithValue(ctx, contextKeyLog, logger)
}

func getLog(ctx context.Context) Logger {
	if l, ok := ctx.Value(contextKeyLog).(Logger); ok {
		return l
	}
	return logger.NewNullLogger()
}

type Client struct {
	config    ClientConfig
	connecter connecter.Connecter
}

type ClientConfig struct {
	IdleConnTimeout time.Duration
	MaxIdleConns    int // unused
}

func (c *ClientConfig) Validate() error {
	panic("not implemented")
}

// NewClient panics if config is not valid
func NewClient(connecter connecter.Connecter, config ClientConfig) *Client {
	if err := config.Validate(); err != nil {
		panic(err)
	}
	return &Client{config, connecter}
}

func (c *Client) ReqSendStream(ctx context.Context, r *pdu.SendTokenReq) (*pdu.SendTokenRes, io.ReadCloser, error) {
	panic("not implemented")
}

func (c *Client) ReqRecv(ctx context.Context, r *pdu.ReceiveTokenReq, sendStream io.ReadCloser) error {
	panic("not implemented")

}

type Handler interface {
	HandleSend(ctx context.Context, r *pdu.SendTokenReq, streamWriter io.Writer) error
	HandleReceive(ctx context.Context, r *pdu.ReceiveTokenReq, stream io.Reader) error
}

type Server struct {
	l      serve.AuthenticatedListener
	h      Handler
	config ServerConfig
}

type ServerConfig struct {
	MaxProtoLen     uint32
	MaxHeaderLen    uint32
	HeaderTimeout   time.Duration
	IdleConnTimeout time.Duration
}

func (c ServerConfig) Validate() error {
	panic("not implemented")
}

func timeoutToDeadline(duration time.Duration) time.Time {
	if duration == 0 {
		return time.Time{}
	}
	return time.Now().Add(duration)
}

// NewServe panics if config is not valid.
func NewServer(authListener serve.AuthenticatedListener, handler Handler, config ServerConfig) *Server {
	if err := config.Validate(); err != nil {
		panic(err)
	}
	return &Server{authListener, handler, config}
}

func (s *Server) Serve(ctx context.Context) {
outer:
	for ctx.Err() == nil {
		wire, err := s.l.Accept(ctx)
		if err != nil {
			getLog(ctx).WithError(err).Error("accept error")
			continue
		}
		clientError := func(msgToClient string) {
			var res reqHeader
			res.HandlerErr = msgToClient
			if err := res.marshalToWire(wire); err != nil {
				getLog(ctx).WithError(err). // not an error, could be malicious
								Debug("response after failed header unmarshal failed")
			}
			getLog(ctx).Debug("closing client connection")
			if err := wire.Close(); err != nil {
				getLog(ctx).WithError(err).
					Error("error closing conection after previous error")
			}
		}
		headerDL := timeoutToDeadline(s.config.HeaderTimeout)
		if err := wire.SetDeadline(headerDL); err != nil {
			getLog(ctx).WithError(err).
				Error("cannot set connection deadline")
			clientError("internal server error")
			continue
		}

		var hdr reqHeader
		if err := hdr.unmarshalFromWire(wire, s.config.MaxHeaderLen); err != nil {
			getLog(ctx).WithError(err).Debug("unmarshaling header failed") // not an error, could be malicious
			clientError(fmt.Sprintf("unmarshalling header failed: %s", err))
			continue
		}

		if hdr.ProtoLen > s.config.MaxProtoLen {
			getLog(ctx).WithError(err).
				WithField("max_proto_length", s.config.MaxProtoLen).
				Debug("request proto exceeds max length")
			clientError(fmt.Sprintf("protolen exceeds configured maximum"))
			continue
		}

		protoBuf := make([]byte, hdr.ProtoLen)
		if _, err := io.ReadFull(wire, protoBuf); err != nil {
			clientError(fmt.Sprintf("reading protobuf failed: %s", err))
			continue
		}

		switch hdr.Endpoint {
		case EndpointSend:
			var req pdu.SendTokenReq
			if err := proto.Unmarshal(protoBuf, &req); err != nil {
				clientError(fmt.Sprintf("cannot unmarshal send request"))
				continue outer
			}
			err := s.h.HandleSend(ctx, &req, wire)
			if err != nil {
				clientError(err.Error())
				continue outer
			}
		case EndpointRecv:
			var req pdu.ReceiveTokenReq
			if err := proto.Unmarshal(protoBuf, &req); err != nil {
				clientError(fmt.Sprintf("cannot unmarshal receive request"))
				continue outer
			}
			err := s.h.HandleReceive(ctx, &req, wire)
			if err != nil {
				clientError(err.Error())
				continue outer
			}
		default:
			clientError(fmt.Sprintf("unknown endpoint %q", hdr.Endpoint))
			continue outer
		}
	}
}

const (
	EndpointSend string = "/v1/send"
	EndpointRecv string = "/v1/recv"
)

// Wire representation: UTF-8
//
// First line:
//
// 	ZREPL_DATA_CONN HEADER_LEN=4294967295\n
// 	^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
// 	38 bytes, incl. newline
//	length string is base 10 fixed length
//  length string denotes length of json-encoded reqHeader, including newline afte that
//
// Second line:
//
//	{"Endpoint":"someendpoint", "ProtoLen":23}\n
//
// 	ProtoLen denotes length of following protobuf-encoded data
type reqHeader struct {
	// Following two fields are valid in requests
	Endpoint string
	ProtoLen uint32

	// Following field is valid in server responses
	HandlerErr string
}

type HeaderMarshalError struct {
	msg string
}

func (e *HeaderMarshalError) Error() string {
	return fmt.Sprintf("protocol error: %s", e.msg)
}

func (r reqHeader) marshalToWire(w io.Writer) error {
	rJSON, err := json.Marshal(r)
	if err != nil {
		panic(err)
	}
	if _, err := fmt.Fprintf(w, "ZREPL_DATA_CONN HEADER_LEN=%0*d\n%s\n", 10, len(rJSON)+1, rJSON); err != nil {
		return err
	}
	return nil
}

func (r *reqHeader) unmarshalFromWire(w io.Reader, maxLen uint32) error {

	var firstline [38]byte
	if _, err := io.ReadFull(w, firstline[:]); err != nil {
		return err
	}

	var rJSONLen uint32
	if _, err := fmt.Sscanf(string(firstline[:]), "ZREPL_DATA_CONN HEADER_LEN=%d\n", &rJSONLen); err != nil {
		return &HeaderMarshalError{fmt.Sprintf("cannot parse header length from first line %q", firstline)}
	}

	if rJSONLen > maxLen {
		return &HeaderMarshalError{fmt.Sprintf("header exceeds max length %d bytes", maxLen)}
	}

	jsonBuf := make([]byte, rJSONLen)
	if _, err := io.ReadFull(w, jsonBuf); err != nil {
		return err
	}
	if err := json.Unmarshal(jsonBuf, r); err != nil {
		return &HeaderMarshalError{fmt.Sprintf("cannot unmarshal header: %s", err)}
	}

	return nil
}
