package dataconn

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/go-playground/validator"

	"github.com/golang/protobuf/proto"
	"github.com/zrepl/zrepl/daemon/transport/connecter"
	"github.com/zrepl/zrepl/logger"
	"github.com/zrepl/zrepl/replication/pdu"
	"github.com/zrepl/zrepl/endpoint/dataconn/chunker"
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
	MaxProtoLen     uint32        `validate:"gt=0"`
	MaxHeaderLen    uint32        `validate:"gt=0"`
	IdleConnTimeout time.Duration `validate:"gte=0"`
}

var validate = validator.New()

func (c *ClientConfig) Validate() error {
	return validate.Struct(c)
}

// NewClient panics if config is not valid
func NewClient(connecter connecter.Connecter, config ClientConfig) *Client {
	if err := config.Validate(); err != nil {
		panic(err)
	}
	return &Client{config, connecter}
}

type connCloser func(closeReason error)

func (c *Client) getWire(ctx context.Context) (conn net.Conn, closeConn connCloser, err error) {
	conn, err = c.connecter.Connect(ctx)
	closeConn = func(closeReason error) {
		getLog(ctx).WithError(closeReason).Debug("closing connection after error")
		if err := conn.Close(); err != nil {
			getLog(ctx).WithError(err).Error("error closing connection")
		}
	}
	return conn, closeConn, err
}

func (c *Client) putWire(ctx context.Context, wire net.Conn) {
	getLog(ctx).Debug("closing conn because connection is full")
	if err := wire.Close(); err != nil {
		getLog(ctx).WithError(err).Error("cannot close connection")
	}
}

func (c *Client) sendStructured(ctx context.Context, wire net.Conn, endpoint string, req proto.Message) error {
	protobuf, err := proto.Marshal(req)
	if err != nil {
		return err
	}
	var hdr reqHeader
	hdr.Endpoint = endpoint
	hdr.ProtoLen = uint32(len(protobuf))

	// send out data
	if err := hdr.marshalToWire(wire); err != nil {
		return err
	}

	n, err := wire.Write(protobuf)
	if err != nil {
		return err
	}
	if n != len(protobuf) {
		return io.ErrShortWrite
	}
	return nil
}

type RemoteError struct {
	msg string
}

func (r *RemoteError) Error() string {
	return r.msg
}

func (c *Client) receiveStructured(ctx context.Context, wire net.Conn, res proto.Message) error {

	var hdr reqHeader
	if err := hdr.unmarshalFromWire(wire, c.config.MaxHeaderLen); err != nil {
		return err
	}

	if hdr.HandlerErr != "" {
		return &RemoteError{hdr.HandlerErr}
	}

	if hdr.ProtoLen >= c.config.MaxProtoLen {
		return fmt.Errorf("response protobuf len exceeds limit")
	}

	protoBuf := make([]byte, hdr.ProtoLen)
	if _, err := io.ReadFull(wire, protoBuf); err != nil {
		return err
	}

	if err := proto.Unmarshal(protoBuf, res); err != nil {
		return fmt.Errorf("cannot unmarshal protobuf: %s", err)
	}

	return nil
}

type unchunkingReadCloser struct {
	wire      net.Conn              // only for close
	unchunker *chunker.StreamReader // wraps wire
}

func (r *unchunkingReadCloser) Read(p []byte) (n int, err error) {
	return r.unchunker.Read(p)
}

func (r *unchunkingReadCloser) Close() error {
	if r.unchunker.Consumed() {
		// TODO could put conn back into some kind of pool
		return nil
	} else {
		return r.wire.Close()
	}
}

func (c *Client) ReqSendStream(ctx context.Context, req *pdu.SendTokenReq) (*pdu.SendTokenRes, io.ReadCloser, error) {
	// connect
	wire, closeWire, err := c.getWire(ctx)
	if err != nil {
		return nil, nil, err
	}
	// FIXME wrap wire into idle conn

	if err := c.sendStructured(ctx, wire, EndpointSend, req); err != nil {
		closeWire(err)
		return nil, nil, err
	}

	var res pdu.SendTokenRes
	if err := c.receiveStructured(ctx, wire, &res); err != nil {
		closeWire(err)
		return nil, nil, err
	}

	if req.DryRun {
		c.putWire(ctx, wire)
		return &res, nil, nil
	}

	reader := chunker.NewStreamReader(wire, 1<<20) // FIXME constant
	rc := &unchunkingReadCloser{wire: wire, unchunker: reader}
	return &res, rc, nil // TODO: return something that enables caller to put wire back..?
}

func (c *Client) ReqRecv(ctx context.Context, req *pdu.ReceiveTokenReq, sendStream io.ReadCloser) (*pdu.ReceiveTokenRes, error) {

	wire, closeWire, err := c.getWire(ctx)
	if err != nil {
		return nil, err
	}
	// FIXME wrap wire into idle conn

	if err := c.sendStructured(ctx, wire, EndpointRecv, req); err != nil {
		closeWire(err)
		return nil, err
	}

	var wg sync.WaitGroup

	var structuredResult pdu.ReceiveTokenRes
	var recvErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		recvErr = c.receiveStructured(ctx, wire, &structuredResult)
		getLog(ctx).Debug("receive complete")
	}()

	var sendErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		// only a write error possible, see API
		writeErr := chunker.WriteStream(ctx, wire, sendStream, 1<<20) // FIXME constant
		if err != nil {
			getLog(ctx).WithError(writeErr).Error("network or receiver error while writing send stream")
		} else {
			getLog(ctx).Debug("send complete")
		}
	}()

	// FIXME simplify this construct by using serve.AuthenticatedListener
	getLog(ctx).Debug("waiting for send to finish and recv to return")
	allDone := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			closeWire(ctx.Err())
		case <-allDone:
		}
	}()
	wg.Wait()
	close(allDone)

	getLog(ctx).Debug("closing send stream")
	if err := sendStream.Close(); err != nil {
		getLog(ctx).WithError(err).Error("error closing send stream")
	}

	if ctx.Err() != nil {
		return nil, ctx.Err()
	} else if recvErr != nil {
		return nil, recvErr
	} else if sendErr != nil {
		return nil, sendErr
	}

	return &structuredResult, nil
}

type Handler interface {
	HandleSend(ctx context.Context, r *pdu.SendTokenReq) (*pdu.SendTokenRes, io.ReadCloser, error)
	HandleReceive(ctx context.Context, r *pdu.ReceiveTokenReq, stream io.Reader) (*pdu.ReceiveTokenRes, error)
}

type Server struct {
	l      net.Listener
	h      Handler
	config ServerConfig
}

type ServerConfig struct {
	MaxProtoLen     uint32        `validate:"gt=0"`
	MaxHeaderLen    uint32        `validate:"gt=0"`
	HeaderTimeout   time.Duration `validate:"gte=0"`
	IdleConnTimeout time.Duration `validate:"gte=0"`
}

func (c ServerConfig) Validate() error {
	return validate.Struct(c)
}

func timeoutToDeadline(duration time.Duration) time.Time {
	if duration == 0 {
		return time.Time{}
	}
	return time.Now().Add(duration)
}

// NewServe panics if config is not valid.
func NewServer(l net.Listener, handler Handler, config ServerConfig) *Server {
	if err := config.Validate(); err != nil {
		panic(err)
	}
	return &Server{l, handler, config}
}

func (s *Server) Serve(ctx context.Context) {
outer:
	for ctx.Err() == nil {
		// remove goroutine construct once we use serve.AuthentiactedListener
		accepted := make(chan struct{})
		go func() {
			select {
			case <-ctx.Done():
				if err := s.l.Close(); err != nil {
					getLog(ctx).WithError(err).Error("error closing listener after context error")
				}
			case <-accepted:
			}
		}()
		wire, err := s.l.Accept()
		close(accepted)
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

		wire.SetDeadline(time.Time{})
		// FIXME wrap wire into idleconn and give it to handlers

		getLog(ctx).Debug("pre proto decode")

		var res proto.Message
		var sendStream io.ReadCloser
		switch hdr.Endpoint {
		case EndpointSend:
			var req pdu.SendTokenReq
			if err := proto.Unmarshal(protoBuf, &req); err != nil {
				clientError(fmt.Sprintf("cannot unmarshal send request"))
				continue outer
			}
			getLog(ctx).Debug("pre send handler")
			res, sendStream, err = s.h.HandleSend(ctx, &req) // SHADOWING
		case EndpointRecv:
			var req pdu.ReceiveTokenReq
			if err := proto.Unmarshal(protoBuf, &req); err != nil {
				clientError(fmt.Sprintf("cannot unmarshal receive request"))
				continue outer
			}
			getLog(ctx).Debug("pre receive handler")
			unchunker := chunker.NewStreamReader(wire, 1 << 20) // FIXME constant
			res, err = s.h.HandleReceive(ctx, &req, unchunker) // SHADOWING
		default:
			clientError(fmt.Sprintf("unknown endpoint %q", hdr.Endpoint))
			continue outer
		}

		// FIXME enable timeouts again
		getLog(ctx).Debug("returned from handler")

		if err != nil {
			clientError(err.Error())
			continue outer
		}

		protobuf, err := proto.Marshal(res)
		if err != nil {
			clientError(fmt.Sprintf("server error: cannot marshal handler protobuf: %s", err))
			continue outer
		}

		var respHdr reqHeader
		respHdr.ProtoLen = uint32(len(protobuf))
		if err := respHdr.marshalToWire(wire); err != nil {
			clientError(err.Error()) // TODO improve this
			continue outer
		}

		n, err := wire.Write(protobuf)
		if err != nil {
			clientError(err.Error()) // TODO more context
			continue outer
		}
		if n != len(protobuf) {
			clientError(io.ErrShortWrite.Error()) // TODO more context
			continue outer
		}

		getLog(ctx).Debug("wrote response")

		if sendStream != nil {
			// TODO move this logic (and the above) to a ResonseWriter similar to Go's http package
				
			getLog(ctx).Debug("begin writing stream")
			writeErr := chunker.WriteStream(ctx, wire, sendStream, 1<<20) // FIXME constant
			// API guarantees this is due to wire or receiver, not sender
			if writeErr != nil {
				getLog(ctx).WithError(writeErr).Error("network or receiver error while writing send stream")
			} else {
				getLog(ctx).Debug("finished writing stream")
			}
			// no matter what, close sendStream, we owe that to the handler
			if err := sendStream.Close(); err != nil {
				getLog(ctx).WithError(err).Error("error closing send stream returned from handler")
			}

			if err != nil {
				clientError(err.Error()) // TODO more context
				continue outer
			}
		}

		getLog(ctx).Debug("closing client connection after successful transfer")
		if err := wire.Close(); err != nil {
			getLog(ctx).WithError(err).Error("could not close client connection after successful transfer")
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
	// Following field is valid in requests
	Endpoint string

	// Following field is valid in server responses
	HandlerErr string

	// Following field is valid in both directions
	ProtoLen uint32
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
