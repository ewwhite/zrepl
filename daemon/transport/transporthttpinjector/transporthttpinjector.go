package transporthttpinjector

import (
	"context"
	"fmt"
	"github.com/zrepl/zrepl/daemon/transport/connecter"
	"github.com/zrepl/zrepl/daemon/transport/serve"
	"net"
	"net/http"
)

type contextKey int

const (
	ContextKeyClientIdentity contextKey = 1 + iota
)

func ClientIdentity(ctx context.Context) string {
	s, _ := ctx.Value(ContextKeyClientIdentity).(string)
	return s
}

func ClientTransport(connecter connecter.Connecter) *http.Transport {
	return &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return connecter.Connect(ctx)
		},
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

func (srv Server) Serve(ctx context.Context) error {
	go func() {
		<-ctx.Done()
		srv.l.Close()
	}()
	return srv.s.Serve(srv.l)
}

// makes a serve.AuthenticatedListener a net.Listener
type authListenerNetListenerAdaptor struct {
	al serve.AuthenticatedListener
}

// the returned net.Conn is guaranteed to be *AuthConn
func (a authListenerNetListenerAdaptor) Accept() (net.Conn, error) {
	var (
		authConn *serve.AuthConn
		err error
	)
	authConn, err = a.al.Accept(context.Background())
	if err != nil {
		return nil, err
	}
	return authConn, err
}

func (a authListenerNetListenerAdaptor) Addr() net.Addr {
	return a.al.Addr()
}

func (a authListenerNetListenerAdaptor) Close() error {
	return a.al.Close()
}

func NewServer(listener serve.AuthenticatedListener, handler http.Handler) Server {
	return Server{
		l: authListenerNetListenerAdaptor{listener},
		s: http.Server{
			Handler:           authconnIdentityInjector{handler},
			// TODO FIXME ErrorLog
		},
	}
}