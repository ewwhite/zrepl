package dataconn

import (
	"context"
	"fmt"
	"io"

	"github.com/golang/protobuf/proto"
	"github.com/zrepl/zrepl/replication/pdu"
	"github.com/zrepl/zrepl/rpc/dataconn/frameconn2"
	"github.com/zrepl/zrepl/transport"
)

type ClientConfig struct {
	Shared SharedConfig
}

func (c *ClientConfig) Validate() error {
	return validate.Struct(c)
}

type Client struct {
	log Logger
	cn  transport.Connecter
}

func NewClient(connecter transport.Connecter, log Logger) *Client {
	return &Client{
		log: log,
		cn: connecter,
	}
}

func (c *Client) send(conn *frameconn.Conn, endpoint string, req proto.Message, stream io.Reader) (ok bool) {

	reqHeaderBuf := frameconn.Frame{
		Type:   frameconn.ReqHeader,
		Buffer: []byte(endpoint),
	}
	reqHeaderBuf.FitBufferLenToBuffer()
	if err := conn.WriteFrame(&reqHeaderBuf); err != nil {
		c.log.WithError(err).Error("cannot write header")
		return false
	}

	protoBuf, err := proto.Marshal(req)
	if err != nil {
		c.log.WithError(err).Error("cannot marshal proto buf request")
		return false
	}
	reqStructured := frameconn.Frame{
		Type:   frameconn.Structured,
		Buffer: protoBuf,
	}
	reqStructured.FitBufferLenToBuffer()
	if err := conn.WriteFrame(&reqStructured); err != nil {
		c.log.WithError(err).Error("cannot write request")
		return false
	}

	if stream != nil {
		streamer := streamer{c.log}
		ok := streamer.writeSendStream(conn, stream)
		if !ok {
			return false
		}
	}

	return true
}

func (c *Client) recv(conn *frameconn.Conn, res proto.Message) error {

	resHeader := frameconn.Frame{
		Buffer: make([]byte, 1<<15),
	}
	resHeader.FitBufferLenToBuffer()
	if err := conn.ReadFrame(&resHeader); err != nil {
		c.log.WithError(err).Error("cannot read response header")
		return err
	}
	if err := resHeader.Validate(); err != nil {
		c.log.WithError(err).Error("invalid response header")
		return err
	}
	if resHeader.Type == frameconn.ResErrHeader {
		// TODO better wrapping
		return fmt.Errorf("server error: %q", string(resHeader.Bytes()))
	}
	if resHeader.Type != frameconn.ResOkHeader {
		return fmt.Errorf("unexpected header frame type %v", resHeader.Type)
	}

	protoBuf := frameconn.Frame{
		Buffer: make([]byte, 1<<22),
	}
	protoBuf.FitBufferLenToBuffer()
	if err := conn.ReadFrame(&protoBuf); err != nil {
		return err // TODO
	}
	if err := protoBuf.Validate(); err != nil {
		return err // TODO
	}
	if protoBuf.Type != frameconn.Structured {
		return fmt.Errorf("expecting structured frame type, got %v", protoBuf.Type)
	}
	if err := proto.Unmarshal(protoBuf.Bytes(), res); err != nil {
		return fmt.Errorf("cannot unmarshal response: %s", err) // TODO classify
	}
	return nil
}

func (c *Client) getWire(ctx context.Context) (*frameconn.Conn, error) {
	nc, err := c.cn.Connect(ctx)
	if err != nil {
		return nil, err
	}
	conn := frameconn.NewDataConn(nc)
	return conn, nil
}

func (c *Client) putWire(conn *frameconn.Conn) {
	if err := conn.Close(); err != nil {
		c.log.WithError(err).Error("error closing connection")
	}
}

func (c *Client) ReqSend(ctx context.Context, req *pdu.SendReq) (*pdu.SendRes, StreamCopier, error) {
	conn, err := c.getWire(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer c.putWire(conn)

	if ok := c.send(conn, EndpointSend, req, nil); !ok {
		// TODO proper error propagation
		return nil, nil, fmt.Errorf("some kind of error occurred")
	}

	var res pdu.SendRes
	if err := c.recv(conn, &res); err != nil {
		return nil, nil, err
	}

	var copier StreamCopier = nil
	if !req.DryRun {
		copier = func(w io.Writer) error {
			streamer := streamer{c.log}
			return streamer.readSendStream(conn, w)
		}
	}

	return &res, copier, nil
}

func (c *Client) ReqRecv(ctx context.Context, req *pdu.ReceiveReq, sendStream io.Reader) (*pdu.ReceiveRes, error) {

	conn, err := c.getWire(ctx)
	if err != nil {
		return nil, err
	}
	defer c.putWire(conn)

	if ok := c.send(conn, EndpointRecv, req, sendStream); !ok {
		// TODO proper error propagation
		return nil, fmt.Errorf("some kind of error occurred")
	}

	var res pdu.ReceiveRes
	if err := c.recv(conn, &res); err != nil {
		return nil, err
	}

	return &res, nil
}
