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
	"github.com/zrepl/zrepl/daemon/logging"
	"github.com/zrepl/zrepl/rpc/dataconn"
	"github.com/zrepl/zrepl/rpc/grpcclientidentity"
	"github.com/zrepl/zrepl/rpc/netadaptor"
	"github.com/zrepl/zrepl/endpoint"
	"github.com/zrepl/zrepl/replication/pdu"
	"google.golang.org/grpc"
)

type Handler interface {
	pdu.ReplicationServer
	Send(ctx context.Context, r *pdu.SendReq) (*pdu.SendRes, io.ReadCloser, error)
	Receive(ctx context.Context, r *pdu.ReceiveReq, stream io.Reader) (*pdu.ReceiveRes, error) // TODO ReadCloser?
}

type ServerConfig struct {
	ZFSReceiveIdleTimeout time.Duration
	ZFSSendIdleTimeout    time.Duration
	DataConn              dataconn.ServerConfig
}

func (c ServerConfig) Validate() error {
	if c.ZFSReceiveIdleTimeout < 0 {
		return fmt.Errorf("ZFSReceiveIdleTimeout must be 0 or positive")
	}
	if c.ZFSSendIdleTimeout < 0 {
		return fmt.Errorf("ZFSSendIdleTimeout must be 0 or positive")
	}
	if err := c.DataConn.Validate(); err != nil {
		return fmt.Errorf("DataConn invalid: %s", err)
	}
	return nil
}

func (c *ServerConfig) FromConfig(global *config.Global, serverConfig *config.RPCServerConfig) error {
	c.ZFSSendIdleTimeout = serverConfig.ZFSSendIdleTimeout
	c.ZFSReceiveIdleTimeout = serverConfig.ZFSReceiveIdleTimeout
	c.DataConn.HeaderTimeout = serverConfig.DataConn.HeaderTimeout
	c.DataConn.Shared = dataConnSharedFromConfig(serverConfig.DataConn.Shared)
	return c.Validate()
}

type serveFunc func(ctx context.Context, demuxedListener transport.AuthenticatedListener, errOut chan<- error)

// Server abstracts the accept and request routing infrastructure for the
// passive side of a replication setup.
type Server struct {
	logger             Logger
	handler            Handler
	config             ServerConfig
	controlServer      *grpc.Server
	controlServerServe serveFunc
	dataServer         *dataconn.Server
	dataServerServe    serveFunc
}

type serverContextKey int

// config must be valid (use its Validate function).
func NewServer(config ServerConfig, handler Handler, log Logger) *Server {

	if err := config.Validate(); err != nil {
		panic(err)
	}

	// setup control server
	controlLog := logging.LogSubsystem(log, logging.SubsysControlServer)
	tcs := grpcclientidentity.NewTransportCredentials(controlLog) // TODO different subsystem for log
	unary, stream := grpcclientidentity.NewInterceptors(controlLog, endpoint.ClientIdentityKey)
	controlServer := grpc.NewServer(grpc.Creds(tcs), grpc.UnaryInterceptor(unary), grpc.StreamInterceptor(stream))
	pdu.RegisterReplicationServer(controlServer, handler)
	controlServerServe := func(ctx context.Context, controlListener transport.AuthenticatedListener, errOut chan<- error) {
		// give time for graceful stop until deadline expires, then hard stop
		go func() {
			<-ctx.Done()
			if dl, ok := ctx.Deadline(); ok {
				go time.AfterFunc(dl.Sub(dl), controlServer.Stop)
			}
			controlLog.Debug("shutting down control server")
			controlServer.GracefulStop()
		}()

		errOut <- controlServer.Serve(netadaptor.New(controlListener, controlLog))
	}

	// setup data server
	dataLog := logging.LogSubsystem(log, logging.SubsysDataServer)
	dataServerClientIdentitySetter := func(ctx context.Context, wire net.Conn) (context.Context, net.Conn) {
		authConn := wire.(*transport.AuthConn) // we set the listener cascase up above, we know this is true
		ci := authConn.ClientIdentity()
		return context.WithValue(ctx, endpoint.ClientIdentityKey, ci), wire
	}
	dataServer := dataconn.NewServer(handler, config.DataConn, dataServerClientIdentitySetter)
	dataServerServe := func(ctx context.Context, dataListener transport.AuthenticatedListener, errOut chan<- error) {
		dataNetListener := netadaptor.New(dataListener, dataLog)
		dataServer.Serve(ctx, dataNetListener)
		errOut <- nil // TODO bad design of dataServer?
	}

	server := &Server{
		logger:             log,
		handler:            handler,
		config:             config,
		controlServer:      controlServer,
		controlServerServe: controlServerServe,
		dataServer:         dataServer,
		dataServerServe:    dataServerServe,
	}

	return server
}

// The context is used for cancellation only.
// Serve never returns an error, it logs them to the Server's logger.
func (s *Server) Serve(ctx context.Context, l transport.AuthenticatedListener) {
	ctx, cancel := context.WithCancel(ctx)

	l = versionhandshake.Listener(l, envconst.Duration("ZREPL_RPC_SERVER_VERSIONHANDSHAKE_TIMEOUT", 10*time.Second))

	// it is important that demux's context is cancelled,
	// it has background goroutines attached
	demuxListener := demux(ctx, l)

	serveErrors := make(chan error, 2)
	go s.controlServerServe(ctx, demuxListener.control, serveErrors)
	go s.dataServerServe(ctx, demuxListener.data, serveErrors)
	select {
	case serveErr := <-serveErrors:
		s.logger.WithError(serveErr).Error("serve error")
		s.logger.Debug("wait for other server to shut down")
		cancel()
		secondServeErr := <-serveErrors
		s.logger.WithError(secondServeErr).Error("serve error")
	case <-ctx.Done():
		s.logger.Debug("context cancelled, wait for control and data servers")
		cancel()
		for i := 0; i < 2; i++ {
			<-serveErrors
		}
		s.logger.Debug("control and data server shut down, returning from Serve")
	}
}
