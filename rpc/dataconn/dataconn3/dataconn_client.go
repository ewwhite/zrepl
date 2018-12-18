package dataconn

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/golang/protobuf/proto"
	"github.com/zrepl/zrepl/replication/pdu"
	"github.com/zrepl/zrepl/transport"
	"github.com/zrepl/zrepl/zfs"
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

func (c *Client) send(ctx context.Context, conn *Conn, endpoint string, req proto.Message, streamCopier zfs.StreamCopier) error {

	var buf bytes.Buffer
	_, memErr := buf.WriteString(endpoint)
	if memErr != nil {
		panic(memErr)
	}
	if err := conn.WriteStreamedMessage(ctx, &buf, ReqHeader); err != nil {
		return err
	}

	protobufBytes, err := proto.Marshal(req)
	if err != nil {
		return err
	}
	protobuf := bytes.NewBuffer(protobufBytes)
	if err := conn.WriteStreamedMessage(ctx, protobuf, ReqStructured); err != nil {
		return err
	}

	if streamCopier != nil {
		return conn.SendStream(ctx, streamCopier)
	} else {
		return nil
	}
}

func (c *Client) recv(ctx context.Context, conn *Conn, res proto.Message) error {

	headerBuf, err := conn.ReadStreamedMessage(ctx, 1<<15, ResHeader)
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

	protobuf, err := conn.ReadStreamedMessage(ctx, 1<<22, ResStructured)
	if err := proto.Unmarshal(protobuf, res); err != nil {
		return err
	}
	return nil
}

func (c *Client) getWire(ctx context.Context) (*Conn, error) {
	nc, err := c.cn.Connect(ctx)
	if err != nil {
		return nil, err
	}
	conn := wrap(nc, HeartbeatInterval, HeartbeatPeerTimeout)
	return conn, nil
}

func (c *Client) putWire(conn *Conn) {
	if err := conn.Close(); err != nil {
		c.log.WithError(err).Error("error closing connection")
	}
}

func (c *Client) ReqSend(ctx context.Context, req *pdu.SendReq) (*pdu.SendRes, zfs.StreamCopier, error) {
	conn, err := c.getWire(ctx)
	if err != nil {
		return nil, nil, err
	}
	putWireOnReturn := true
	defer func() {
		if putWireOnReturn {
			c.putWire(conn)
		}
	}()

	if err := c.send(ctx, conn, EndpointSend, req, nil); err != nil {
		return nil, nil, err
	}

	var res pdu.SendRes
	if err := c.recv(ctx, conn, &res); err != nil {
		return nil, nil, err
	}

	var copier zfs.StreamCopier = nil
	if !req.DryRun {
		putWireOnReturn = false
		copier = conn
	}

	return &res, copier, nil
}

func (c *Client) ReqRecv(ctx context.Context, req *pdu.ReceiveReq, streamCopier zfs.StreamCopier) (*pdu.ReceiveRes, error) {

	defer c.log.Info("ReqRecv returns")
	conn, err := c.getWire(ctx)
	if err != nil {
		return nil, err
	}

	// send and recv response concurrently to catch early exists of remote handler
	// (e.g. disk full, permission error, etc)

	type recvRes struct {
		res *pdu.ReceiveRes
		err error
	}
	recvErrChan := make(chan recvRes)
	go func() {
		res := &pdu.ReceiveRes{}
		if err := c.recv(ctx, conn, res); err != nil {
			recvErrChan <- recvRes{res, err}
		} else {
			recvErrChan <- recvRes{res, nil}
		}
	}()

	sendErrChan := make(chan error)
	go func() {
		if err := c.send(ctx, conn, EndpointRecv, req, streamCopier); err != nil {
			sendErrChan <- err
		} else {
			sendErrChan <- nil
		}
	}()

	var res recvRes
	var sendErr error
	didTryClose := false
	for i := 0; i < 2; i++ {
		select {
		case res = <-recvErrChan:
				c.log.WithField("errType", fmt.Sprintf("%T", res.err)).WithError(res.err).Debug("recv goroutine returned")
		case sendErr = <- sendErrChan:
				c.log.WithField("errType", fmt.Sprintf("%T", sendErr)).WithError(sendErr).Debug("send goroutine returned")
		}
		if !didTryClose && (res.err != nil || sendErr != nil) {
			didTryClose = true
			if err := conn.Close(); err != nil {
				c.log.WithError(err).Error("ReqRecv: cannot close connection, will likely block indefinitely")
			}
			c.log.WithError(err).Debug("ReqRecv: closed connection, should trigger other goroutine error")
		}
	}

	if !didTryClose {
		// didn't close it in above loop, so we can give it back
		c.putWire(conn)	
	}

	// determine which error is more relevant (i.e. not caused by Close)
	relevantErr := res.err
	if relevantErr == nil {
		relevantErr = sendErr
	}

	return res.res, relevantErr
}
