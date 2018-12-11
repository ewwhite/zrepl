// Package dataconn implements a client and server for ZFS send and receive requests over a net.Conn.
// The wire format consists of requests and responses with similar outlines but differences in field values:
//
//   reqHeader length
//   reqHeader JSON encoded (see reqHeader)
//   encoded protobuf
// 	    - requests: pdu.SendReq or pdu.ReceiveReq
//	    - responses: pdu.SendRes or pdu.ReceiveRes
//   chunked stream (see package ./chunker)
//
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
	"github.com/zrepl/zrepl/rpc/dataconn/chunker"
	"github.com/zrepl/zrepl/logger"
	"github.com/zrepl/zrepl/replication/pdu"
	"github.com/zrepl/zrepl/transport"
)

type contextKey int

const (
	contextKeyLog = 1 + iota
	contextKeyReset
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
	connecter transport.Connecter
}

type ClientConfig struct {
	Shared SharedConfig
}

var validate = validator.New()

func (c *ClientConfig) Validate() error {
	return validate.Struct(c)
}

// NewClient panics if config is not valid
func NewClient(connecter transport.Connecter, config ClientConfig) *Client {
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

// putWire (and getWirte) are a preparation for connection pooling at some point in the future
//
// Connection Pooling Design Draft
//
// - encapsulate pooled connections in a struct poolConn { net.Conn, parent *Client }
// - poolConn implements WriterTo for io.Copy performance
// - poolConns are what is returned to the users of Client
// - closed poolConns return to the connection pool using parent.putWire()
//   - this is necessary because we return the connections to the users of Client
//     in ReqSendStream
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
	if err := hdr.unmarshalFromWire(wire, c.config.Shared.MaxHeaderLen); err != nil {
		return err
	}

	if hdr.HandlerErr != "" {
		return &RemoteError{hdr.HandlerErr}
	}

	if hdr.ProtoLen >= c.config.Shared.MaxProtoLen {
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

func (c *Client) ReqSendStream(ctx context.Context, req *pdu.SendReq) (*pdu.SendRes, io.ReadCloser, error) {
	wire, closeWire, err := c.getWire(ctx)
	if err != nil {
		return nil, nil, err
	}

	if err := c.sendStructured(ctx, wire, EndpointSend, req); err != nil {
		closeWire(err)
		return nil, nil, err
	}

	var res pdu.SendRes
	if err := c.receiveStructured(ctx, wire, &res); err != nil {
		closeWire(err)
		return nil, nil, err
	}

	if req.DryRun {
		c.putWire(ctx, wire)
		return &res, nil, nil
	}

	reader := chunker.NewStreamReader(wire, c.config.Shared.MaxRecvChunkSize)
	rc := &unchunkingReadCloser{wire: wire, unchunker: reader}
	return &res, rc, nil
}

func (c *Client) ReqRecv(ctx context.Context, req *pdu.ReceiveReq, sendStream io.Reader) (*pdu.ReceiveRes, error) {

	wire, closeWire, err := c.getWire(ctx)
	if err != nil {
		return nil, err
	}

	if err := c.sendStructured(ctx, wire, EndpointRecv, req); err != nil {
		closeWire(err)
		return nil, err
	}

	var wg sync.WaitGroup

	var structuredResult pdu.ReceiveRes
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
		writeErr := chunker.WriteStream(ctx, wire, sendStream, c.config.Shared.SendChunkSize)
		if err != nil {
			getLog(ctx).WithError(writeErr).Error("network or receiver error while writing send stream")
		} else {
			getLog(ctx).Debug("send complete")
		}
	}()

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

	if ctx.Err() != nil {
		return nil, ctx.Err()
	} else if recvErr != nil {
		return nil, recvErr
	} else if sendErr != nil {
		return nil, sendErr
	}

	return &structuredResult, nil
}

// Handler implements the functionality that is exposed by Server to the Client.
type Handler interface {
	// Send handles a SendRequest.
	// The returned io.ReadCloser is allowed to be nil, for example if the requested Send is a dry-run.
	Send(ctx context.Context, r *pdu.SendReq) (*pdu.SendRes, io.ReadCloser, error)
	// Receive handles a ReceiveRequest.
	// It is guaranteed that Server calls Receive with a stream that holds the IdleConnTimeout
	// configured in ServerConfig.Shared.IdleConnTimeout.
	Receive(ctx context.Context, r *pdu.ReceiveReq, stream io.Reader) (*pdu.ReceiveRes, error)
}

// WireInterceptor has a chance to exchange the context and connection on each client connection.
type WireInterceptor func(ctx context.Context, conn net.Conn) (context.Context, net.Conn)

type Server struct {
	h      Handler
	config ServerConfig
	wireInterceptor WireInterceptor
}

type SharedConfig struct {
	MaxProtoLen      uint32        `validate:"gt=0"`
	MaxHeaderLen     uint32        `validate:"gt=0"`
	SendChunkSize    uint32        `validate:"gt=0"`
	MaxRecvChunkSize uint32        `validate:"gt=0"`
	IdleConnTimeout  time.Duration `validate:"gte=0"`
}

type ServerConfig struct {
	Shared				SharedConfig
	HeaderTimeout    time.Duration `validate:"gte=0"`
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
func NewServer(handler Handler, config ServerConfig, wireInterceptor WireInterceptor) *Server {
	if err := config.Validate(); err != nil {
		panic(err)
	}
	return &Server{handler, config, wireInterceptor}
}

func (s *Server) Serve(ctx context.Context, l net.Listener) {
outer:
	for ctx.Err() == nil {

		// fork off the initial Serve-call context for each connection
		ctx := context.WithValue(ctx, contextKeyReset, nil)

		accepted := make(chan struct{})
		go func() {
			select {
			case <-ctx.Done():
				if err := l.Close(); err != nil {
					getLog(ctx).WithError(err).Error("error closing listener after context error")
				}
			case <-accepted:
			}
		}()
		wire, err := l.Accept()
		close(accepted)
		if err != nil {
			getLog(ctx).WithError(err).Error("accept error")
			continue
		}
		if s.wireInterceptor != nil {
			ctx, wire = s.wireInterceptor(ctx, wire) // SHADOWING
		}
		clientError := func(msgToClient string) {
			// the client may be malicious, use a hard deadline through timed Close
			// (we don't use SetDeadline because that may be the cause for the call)
			dl := time.NewTimer(s.config.HeaderTimeout)	
			messageSent := make(chan struct{})
			go func() {
				defer close(messageSent)
				var res reqHeader
				res.HandlerErr = msgToClient
				if err := res.marshalToWire(wire); err != nil {
					getLog(ctx).WithError(err). // not an error, could be malicious
									Debug("response after failed header unmarshal failed")
				}
			}()
			go func() {
				select {
				case <-messageSent:
					dl.Stop()
				case <-dl.C:
					getLog(ctx).Debug("sending client error timed out")
				}
				getLog(ctx).Debug("closing client connection")
				if err := wire.Close(); err != nil {
					getLog(ctx).WithError(err).
						Error("error closing client conection after previous error")
				}
			}()
		}

		// header timeout, reset before call to handler
		headerDL := timeoutToDeadline(s.config.HeaderTimeout)
		if err := wire.SetDeadline(headerDL); err != nil {
			getLog(ctx).WithError(err).
				Error("cannot set connection deadline")
			clientError("internal server error")
			continue
		}

		var hdr reqHeader
		if err := hdr.unmarshalFromWire(wire, s.config.Shared.MaxHeaderLen); err != nil {
			getLog(ctx).WithError(err).Debug("unmarshaling header failed") // not an error, could be malicious
			clientError(fmt.Sprintf("unmarshalling header failed: %s", err))
			continue
		}

		if hdr.ProtoLen > s.config.Shared.MaxProtoLen {
			getLog(ctx).WithError(err).
				WithField("max_proto_length", s.config.Shared.MaxProtoLen).
				Debug("request proto exceeds max length")
			clientError(fmt.Sprintf("protolen exceeds configured maximum"))
			continue
		}

		protoBuf := make([]byte, hdr.ProtoLen)
		if _, err := io.ReadFull(wire, protoBuf); err != nil {
			clientError(fmt.Sprintf("reading protobuf failed: %s", err))
			continue
		}

		// disable header timeouts (absolute deadlines) while in handler
		if err := wire.SetDeadline(time.Time{}); err != nil {
			getLog(ctx).WithError(err).Error("cannot unset connection deadline")
		}

		getLog(ctx).Debug("pre handler")

		// FIXME wrap conn into an idle-timeout conn
		// - use SetDeadline(time.Now().Add(shared.IdleConnTimeout)) on each Read / Write / Close call
		// - if a Read / Write fails, check whether it's a timeout error
		//   AND and whether it made _any_ progress in the interval (i.e. n > 0)
		// - if both are true, then mask the error and return a short read

		var res proto.Message
		var sendStream io.ReadCloser
		switch hdr.Endpoint {
		case EndpointSend:
			var req pdu.SendReq
			if err := proto.Unmarshal(protoBuf, &req); err != nil {
				clientError(fmt.Sprintf("cannot unmarshal send request"))
				continue outer
			}
			getLog(ctx).Debug("pre send handler")
			res, sendStream, err = s.h.Send(ctx, &req) // SHADOWING
		case EndpointRecv:
			var req pdu.ReceiveReq
			if err := proto.Unmarshal(protoBuf, &req); err != nil {
				clientError(fmt.Sprintf("cannot unmarshal receive request"))
				continue outer
			}
			getLog(ctx).Debug("pre receive handler")
			unchunker := chunker.NewStreamReader(wire, s.config.Shared.MaxRecvChunkSize)
			res, err = s.h.Receive(ctx, &req, unchunker) // SHADOWING
		default:
			clientError(fmt.Sprintf("unknown endpoint %q", hdr.Endpoint))
			continue outer
		}

		getLog(ctx).Debug("returned from handler")

		if err != nil {
			clientError(err.Error())
			continue outer
		}

		// re-enable timeouts for header response
		headerDL = timeoutToDeadline(s.config.HeaderTimeout)
		if err := wire.SetDeadline(headerDL); err != nil {
			getLog(ctx).WithError(err).
				Error("cannot set connection deadline")
			clientError("internal server error")
			continue
		}

		protobuf, err := proto.Marshal(res)
		if err != nil {
			clientError(fmt.Sprintf("server error: cannot marshal handler protobuf: %s", err))
			continue outer
		}

		var respHdr reqHeader
		respHdr.ProtoLen = uint32(len(protobuf))
		if err := respHdr.marshalToWire(wire); err != nil {
			getLog(ctx).WithError(err).Error("cannot send response header")
			clientError(err.Error())
			continue outer
		}

		n, err := wire.Write(protobuf)
		if err != nil {
			getLog(ctx).WithError(err).Error("cannot send handler protobuf response")
			clientError(err.Error())
			continue outer
		}
		if n != len(protobuf) {
			getLog(ctx).Error("short write writing protobuf response")
			clientError(io.ErrShortWrite.Error())
			continue outer
		}

		getLog(ctx).Debug("wrote response")

		if sendStream != nil {
			// TODO move this logic (and the above) to a ResonseWriter similar to Go's http package

			// disable header deadline for sending response
			if err := wire.SetDeadline(time.Time{}); err != nil {
				getLog(ctx).WithError(err).Error("cannot unset connection deadline")
			}

			getLog(ctx).Debug("begin writing stream")
			writeErr := chunker.WriteStream(ctx, wire, sendStream, s.config.Shared.SendChunkSize)
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

			// re-enable deadline for closing
			headerDL = timeoutToDeadline(s.config.HeaderTimeout)
			if err := wire.SetDeadline(headerDL); err != nil {
				getLog(ctx).WithError(err).
					Error("cannot set connection deadline")
				clientError("internal server error")
				continue
			}

		}

		getLog(ctx).Debug("closing client connection")
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
