package rpc

import (
	"context"
	"fmt"
	"github.com/zrepl/zrepl/transport"
	"github.com/zrepl/zrepl/rpc/versionhandshake"
	"github.com/zrepl/zrepl/util/envconst"
	"io"
	"net"
	"time"

	"github.com/zrepl/zrepl/config"
	"github.com/zrepl/zrepl/rpc/dataconn"
	"github.com/zrepl/zrepl/rpc/grpcclientidentity/grpchelper"
	"github.com/zrepl/zrepl/replication"
	"github.com/zrepl/zrepl/replication/pdu"
)

// Client implements the active side of a replication setup.
// It satisfies the Endpoint, Sender and Receiver interface defined by package replication.
type Client struct {
	config        *ClientConfig
	dataClient    *dataconn.Client
	controlClient pdu.ReplicationClient // this the grpc client instance, see constructor
}

type ClientConfig struct {
	MaxIdleConns        int
	IdleConnTimeout     time.Duration
	RPCCallTimeout      time.Duration
	SendCallIdleTimeout time.Duration
	RecvCallIdleTimeout time.Duration

	DataConn			dataconn.ClientConfig
}

func dataConnSharedFromConfig(c *config.DataConnShared) dataconn.SharedConfig {
	return dataconn.SharedConfig{
		IdleConnTimeout:  c.IdleConnTimeout,
		MaxProtoLen:      c.MaxProtoLen,
		MaxHeaderLen:     c.MaxHeaderLen,
		SendChunkSize:    c.SendChunkSize,
		MaxRecvChunkSize: c.MaxRecvChunkSize,
	}
}

func (c ClientConfig) Validate() error {
	if c.MaxIdleConns < 0 {
		return fmt.Errorf("MaxIdleConns must be 0 or positive")
	}
	if c.IdleConnTimeout < 0 {
		return fmt.Errorf("IdleConnTimeout must be 0 or positive")
	}
	if c.RPCCallTimeout < 0 {
		return fmt.Errorf("RPCCallTimeout must be 0 or positive")
	}
	if c.SendCallIdleTimeout < 0 {
		return fmt.Errorf("SendCallIdleTimeout must be 0 or positive")
	}
	if c.RecvCallIdleTimeout < 0 {
		return fmt.Errorf("RecvCallIdleTimeout must be 0 or positive")
	}
	if err := c.DataConn.Validate(); err != nil {
		return fmt.Errorf("DataConn is invalid: %s", err)
	}
	return nil
}

func (c *ClientConfig) FromConfig(global *config.Global, clientConfig *config.RPCClientConfig) error {
	c.MaxIdleConns = clientConfig.MaxIdleConns
	c.RPCCallTimeout = clientConfig.RPCCallTimeout
	c.SendCallIdleTimeout = clientConfig.SendCallIdleTimeout
	c.RecvCallIdleTimeout = clientConfig.RecvCallIdleTimeout
	c.DataConn = dataconn.ClientConfig{
		Shared: dataConnSharedFromConfig(clientConfig.DataConn.Shared),
	}
	return c.Validate()
}

var _ replication.Endpoint = &Client{}
var _ replication.Sender = &Client{}
var _ replication.Receiver = &Client{}

type DialContextFunc = func(ctx context.Context, network string, addr string) (net.Conn, error)

// config must be validated, NewClient will panic if it is not valid
func NewClient(cn transport.Connecter, config ClientConfig) *Client {
	if err := config.Validate(); err != nil {
		panic(fmt.Errorf("client config invalid: %s", err))
	}

	cn = versionhandshake.Connecter(cn, envconst.Duration("ZREPL_RPC_CLIENT_VERSIONHANDSHAKE_TIMEOUT", 10*time.Second))

	muxedConnecter := mux(cn)

	c := &Client{}
	grpcConn, err := grpchelper.Dial(muxedConnecter.control)
	if err != nil {
		panic(err) // TODO
	}
	c.controlClient = pdu.NewReplicationClient(grpcConn)

	c.dataClient = dataconn.NewClient(muxedConnecter.data, config.DataConn)
	return c
}

// callers must ensure that the returned io.ReadCloser is closed
func (c *Client) Send(ctx context.Context, r *pdu.SendReq) (*pdu.SendRes, io.ReadCloser, error) {
	// TODO the returned sendStream may return a read error created by the remote side
	return c.dataClient.ReqSendStream(ctx, r)
}

func (c *Client) Receive(ctx context.Context, r *pdu.ReceiveReq, sendStream io.Reader) (*pdu.ReceiveRes, error) {
	return c.dataClient.ReqRecv(ctx, r, sendStream) // TODO process token?
}

func (c *Client) ListFilesystems(ctx context.Context, in *pdu.ListFilesystemReq) (*pdu.ListFilesystemRes, error) {
	return c.controlClient.ListFilesystems(ctx, in)
}

func (c *Client) ListFilesystemVersions(ctx context.Context, in *pdu.ListFilesystemVersionsReq) (*pdu.ListFilesystemVersionsRes, error) {
	return c.controlClient.ListFilesystemVersions(ctx, in)
}

func (c *Client) DestroySnapshots(ctx context.Context, in *pdu.DestroySnapshotsReq) (*pdu.DestroySnapshotsRes, error) {
	return c.controlClient.DestroySnapshots(ctx, in)
}

func (c *Client) ReplicationCursor(ctx context.Context, in *pdu.ReplicationCursorReq) (*pdu.ReplicationCursorRes, error) {
	return c.controlClient.ReplicationCursor(ctx, in)
}

