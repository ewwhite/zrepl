package dataconn

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/golang/protobuf/proto"
	"github.com/zrepl/zrepl/replication/pdu"
	"github.com/zrepl/zrepl/rpc/dataconn/heartbeatconn"
	"github.com/zrepl/zrepl/rpc/dataconn/stream"
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
		cn:  connecter,
	}
}

func (c *Client) send(ctx context.Context, conn *heartbeatconn.Conn, endpoint string, req proto.Message, sendStream io.Reader) error {

	var buf bytes.Buffer
	_, memErr := buf.WriteString(endpoint)
	if memErr != nil {
		panic(memErr)
	}
	if err := stream.WriteStream(ctx, conn, &buf, ReqHeader); err != nil {
		return err
	}

	protobufBytes, err := proto.Marshal(req)
	if err != nil {
		return err
	}
	protobuf := bytes.NewBuffer(protobufBytes)
	if err := stream.WriteStream(ctx, conn, protobuf, ReqStructured); err != nil {
		return err
	}

	if sendStream != nil {
		return stream.WriteStream(ctx, conn, sendStream, Stream)
	} else {
		return nil
	}
}

func (c *Client) recv(ctx context.Context, conn *heartbeatconn.Conn, res proto.Message) error {

	headerBuf, err := readMessage(ctx, conn, 1<<15, ResHeader)
	if err != nil {
		return err
	}
	header := string(headerBuf)
	if strings.HasPrefix(header, "HANDLER ERROR:\n") {
		// FIXME distinguishable error type
		return fmt.Errorf("server error: %q", strings.TrimPrefix(header, "HANDLER ERROR:\n"))
	}
	if !strings.HasPrefix(header, "HANDLER OK") {
		return fmt.Errorf("protocol error: invalid header: %q", header)
	}

	protobuf, err := readMessage(ctx, conn, 1<<22, ResStructured)
	if err := proto.Unmarshal(protobuf, res); err != nil {
		return err
	}
	return nil
}

func (c *Client) getWire(ctx context.Context) (*heartbeatconn.Conn, error) {
	nc, err := c.cn.Connect(ctx)
	if err != nil {
		return nil, err
	}
	conn := heartbeatconn.Wrap(nc, HeartbeatInterval, HeartbeatPeerTimeout)
	return conn, nil
}

func (c *Client) putWire(conn *heartbeatconn.Conn) {
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

	if err := c.send(ctx, conn, EndpointSend, req, nil); err != nil {
		return nil, nil, err
	}

	var res pdu.SendRes
	if err := c.recv(ctx, conn, &res); err != nil {
		return nil, nil, err
	}

	var copier StreamCopier = nil
	if !req.DryRun {
		copier = func(w io.Writer) error {
			return stream.ReadStream(ctx, conn, w, Stream)
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

	if err := c.send(ctx, conn, EndpointRecv, req, sendStream); err != nil {
		return nil, err
	}

	var res pdu.ReceiveRes
	if err := c.recv(ctx, conn, &res); err != nil {
		return nil, err
	}

	return &res, nil
}
