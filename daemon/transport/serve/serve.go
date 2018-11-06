package serve

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/zrepl/zrepl/config"
	"github.com/zrepl/zrepl/daemon/transport"
	"net"
	"context"
	"github.com/zrepl/zrepl/logger"
	"github.com/zrepl/zrepl/zfs"
	"time"
)

type contextKey int

const contextKeyLog contextKey = 0

type Logger = logger.Logger

func WithLogger(ctx context.Context, log Logger) context.Context {
	return context.WithValue(ctx, contextKeyLog, log)
}

func getLogger(ctx context.Context) Logger {
	if log, ok := ctx.Value(contextKeyLog).(Logger); ok {
		return log
	}
	return logger.NewNullLogger()
}

// A client identity must be a single component in a ZFS filesystem path
func ValidateClientIdentity(in string) (err error) {
	path, err := zfs.NewDatasetPath(in)
	if err != nil {
		return err
	}
	if path.Length() != 1 {
		return errors.New("client identity must be a single path comonent (not empty, no '/')")
	}
	return nil
}

type AuthConn struct {
	net.Conn
	clientIdentity string
}

func (c *AuthConn) ClientIdentity() string {
	if err := ValidateClientIdentity(c.clientIdentity); err != nil {
		panic(err)
	}
	return c.clientIdentity
}

type AuthRemoteAddr struct {
	ClientIdentity string
}

func (AuthRemoteAddr) Network() string {
	return "AuthConn"
}

func (a AuthRemoteAddr) String() string {
	return fmt.Sprintf("AuthRemoteAddr{ClientIdentity:%q}", a.ClientIdentity)
}

func (a *AuthRemoteAddr) FromString(s string) error {
	_, err := fmt.Sscanf(s, "AuthRemoteAddr{ClientIdentity:%q}", &a.ClientIdentity)
	if err != nil {
		return fmt.Errorf("could not decode string-encoded AuthRemoteAddr: %s", err)
	}
	return nil
}

// override remote addr
func (c *AuthConn) RemoteAddr() net.Addr {
	return AuthRemoteAddr{c.clientIdentity}
}

// like net.Listener, but with an AuthenticatedConn instead of net.Conn
type AuthenticatedListener interface {
	Addr() (net.Addr)
	Accept(ctx context.Context) (*AuthConn, error)
	Close() error
}

type AuthenticatedListenerFactory func() (AuthenticatedListener,error)

// wrapper type that performs a a protocol version handshake before returning the connection
type HandshakeListener struct {
	l AuthenticatedListener
}

func (l HandshakeListener) Addr() (net.Addr) { return l.l.Addr() }

func (l HandshakeListener) Close() error { return l.l.Close() }

func (l HandshakeListener) Accept(ctx context.Context) (*AuthConn, error) {
	conn, err := l.l.Accept(ctx)
	if err != nil {
		return nil, err
	}
	dl, ok := ctx.Deadline()
	if !ok {
		dl = time.Now().Add(10*time.Second) // FIXME constant
	}
	if err := transport.DoHandshakeCurrentVersion(conn, dl); err != nil {
		conn.Close()
		return nil, err
	}
	return conn, nil
}

func FromConfig(g *config.Global, in config.ServeEnum) (AuthenticatedListenerFactory,error) {

	var (
		l AuthenticatedListenerFactory
		err error
	)
	switch v := in.Ret.(type) {
	case *config.TCPServe:
		l, err = TCPListenerFactoryFromConfig(g, v)
	case *config.TLSServe:
		l, err = TLSListenerFactoryFromConfig(g, v)
	case *config.StdinserverServer:
		l, err = MultiStdinserverListenerFactoryFromConfig(g, v)
	case *config.LocalServe:
		l, err = LocalListenerFactoryFromConfig(g, v)
	default:
		return nil, errors.Errorf("internal error: unknown serve type %T", v)
	}

	if err != nil {
		return nil, err
	}

	handshakeLF := func() (AuthenticatedListener,error) {
		underlyingListener, err := l()
		if err != nil {
			return nil, err
		}
		return HandshakeListener{underlyingListener}, nil
	}
	return handshakeLF, nil
}

