package connecter

import (
	"context"
	"fmt"
	"github.com/problame/go-streamrpc"
	"github.com/zrepl/zrepl/config"
	"github.com/zrepl/zrepl/daemon/transport"
	"net"
	"time"
)


type HandshakeConnecter struct {
	connecter streamrpc.Connecter
}

func (c HandshakeConnecter) Connect(ctx context.Context) (net.Conn, error) {
	conn, err := c.connecter.Connect(ctx)
	if err != nil {
		return nil, err
	}
	dl, ok := ctx.Deadline()
	if !ok {
		dl = time.Now().Add(10 * time.Second) // FIXME constant
	}
	if err := transport.DoHandshakeCurrentVersion(conn, dl); err != nil {
		conn.Close()
		return nil, err
	}
	return conn, nil
}

type Connecter interface {
	Connect(ctx context.Context) (net.Conn, error)
}

func FromConfig(g *config.Global, in config.ConnectEnum) (Connecter, error) {
	var (
		connecter    Connecter
		errConnecter error
	)
	switch v := in.Ret.(type) {
	case *config.SSHStdinserverConnect:
		connecter, errConnecter = SSHStdinserverConnecterFromConfig(v)
	case *config.TCPConnect:
		connecter, errConnecter = TCPConnecterFromConfig(v)
	case *config.TLSConnect:
		connecter, errConnecter = TLSConnecterFromConfig(v)
	case *config.LocalConnect:
		connecter, errConnecter = LocalConnecterFromConfig(v)
	default:
		panic(fmt.Sprintf("implementation error: unknown connecter type %T", v))
	}

	if errConnecter != nil {
		return nil, errConnecter
	}

	handshakeConnecter := HandshakeConnecter{
		connecter: connecter,
	}

	return handshakeConnecter, nil
}
