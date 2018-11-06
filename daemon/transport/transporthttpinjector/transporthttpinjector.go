package transporthttpinjector

import (
	"context"
	"fmt"
	"github.com/zrepl/zrepl/daemon/transport/connecter"
	"github.com/zrepl/zrepl/daemon/transport/serve"
	"github.com/zrepl/zrepl/replication/pdu"
	"net"
	"net/http"
)

type clientIdentityInjector struct {
	identity string
	cl *http.Client
}

const HEADER = "X-ZREPL-CLIENT-IDENTITY"

func (i clientIdentityInjector) Do(req *http.Request) (*http.Response, error) {
	req.Header.Set(HEADER, i.identity)
	return i.cl.Do(req)
}

var _ pdu.HTTPClient = &clientIdentityInjector{}

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
	req.Header.Set(HEADER, a.ClientIdentity)
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
			Handler: authconnIdentityInjector{handler},
		},
	}
}