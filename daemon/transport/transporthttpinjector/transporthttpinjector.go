package transporthttpinjector

import (
	"context"
	"fmt"
	"github.com/zrepl/zrepl/logger"
	"github.com/zrepl/zrepl/daemon/transport/connecter"
	"github.com/zrepl/zrepl/daemon/transport/serve"
	"net"
	"net/http"
)

type contextKey int

const (
	ContextKeyClientIdentity contextKey = 1 + iota
	contextKeyLog
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

func ClientIdentity(ctx context.Context) string {
	s, _ := ctx.Value(ContextKeyClientIdentity).(string)
	return s
}

// FIXME ciruclare dependency package endpoint
type DialContextFunc = func(ctx context.Context, network string, addr string) (net.Conn, error)

func ClientTransport(connecter connecter.Connecter)  DialContextFunc {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
			return connecter.Connect(ctx)
	}
}

type authconnIdentityInjector struct {
	handler http.Handler
}

func (i authconnIdentityInjector) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	var a serve.AuthRemoteAddr
	if err := a.FromString(req.RemoteAddr); err != nil {
		panic(fmt.Sprintf("implementation error: %s %s", req.RemoteAddr, err))
	}
	req = req.WithContext(context.WithValue(req.Context(), ContextKeyClientIdentity, a.ClientIdentity))
	i.handler.ServeHTTP(w, req)
}

var _ http.Handler = authconnIdentityInjector{}

type Server struct {
	l authListenerNetListenerAdaptor
	s http.Server
}

func (srv *Server) Serve(ctx context.Context) error {
	go func() {
		go srv.l.handleAccept(ctx)
		<-ctx.Done()
		srv.l.Close()
	}()
	return srv.s.Serve(srv.l)
}

type acceptReq struct {
	callback chan net.Conn
}

// Wraps a serve.AuthenticatedListener into a net.Listener.
// The net.Listener is used for net/http.Server.Serve(),
// which expects the listener to only fail with net.Error,
// and fails immediately if !net.Error.Temporary().
// If the error is temporary, it logs it to its ErrorLog,
// which we can't point to our logging infrastructure.
//
// Since the entire transport infrastructure was written with the
// idea that Accept() may return any kind of error, and the consumer
// would just log the error and continue calling Accept(),
// we have to adapt these listeners' behavior to the expectations
// of net/http.Server.
//
// Hence, authListenerNetListenerAdaptor does not return an error
// at all but blocks the caller of Accept() until we get a connection
// without errors from the transport.
type authListenerNetListenerAdaptor struct {
	al serve.AuthenticatedListener
	accepts chan acceptReq
}

// the returned net.Conn is guaranteed to be *AuthConn
func (a authListenerNetListenerAdaptor) Accept() (net.Conn, error) {
	req := acceptReq{ make(chan net.Conn, 1) }
	a.accepts <- req
	return <-req.callback, nil
}

func ( a authListenerNetListenerAdaptor) handleAccept(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case req := <-a.accepts:
			for {
				authConn, err := a.al.Accept(context.Background())
				if err != nil {
					getLog(ctx).WithError(err).Error("accept error")
					continue
				}
				req.callback <- authConn
				break
			}
		}
	}
}

func (a authListenerNetListenerAdaptor) Addr() net.Addr {
	return a.al.Addr()
}

func (a authListenerNetListenerAdaptor) Close() error {
	return a.al.Close()
}

func NewServer(listener serve.AuthenticatedListener, handler http.Handler) Server {
	return Server{
		l: authListenerNetListenerAdaptor{
			al: listener,
			accepts: make(chan acceptReq),
		},
		s: http.Server{
			Handler:           authconnIdentityInjector{handler},
			// TODO FIXME ErrorLog
		},
	}
}