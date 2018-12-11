// Package grpchelper wraps the adaptors implemented by package grpcclientidentity into a less flexible API
// which, however, ensures that the individual adaptor primitive's expectations are met and hence do not panic.
package grpchelper

import (
	"github.com/zrepl/zrepl/rpc/grpcclientidentity"
	"github.com/zrepl/zrepl/rpc/netadaptor"
	"github.com/zrepl/zrepl/transport"
	"google.golang.org/grpc"
)

// Dial is an easy-to-use wrapper around the Dialer and TransportCredentials interface
// to produce a grpc.ClientConn
func Dial(cn transport.Connecter) (*grpc.ClientConn, error) {
	dialerOption := grpc.WithDialer(grpcclientidentity.NewDialer(cn))
	cred := grpc.WithTransportCredentials(grpcclientidentity.NewTransportCredentials(nil))
	return grpc.Dial("doesn't matter done by dialer", dialerOption, cred)
}

// NewServer is a convenience interface around the TransportCredentials and Interceptors interface.
func NewServer(authListenerFactory transport.AuthenticatedListenerFactory, clientIdentityKey interface{}, logger grpcclientidentity.Logger) (srv *grpc.Server, serve func() error, err error) {
	tcs := grpcclientidentity.NewTransportCredentials(logger)
	unary, stream := grpcclientidentity.NewInterceptors(logger, clientIdentityKey)
	srv = grpc.NewServer(grpc.Creds(tcs), grpc.UnaryInterceptor(unary), grpc.StreamInterceptor(stream))

	serve = func() error {
		l, err := authListenerFactory()
		if err != nil {
			return err
		}
		if err := srv.Serve(netadaptor.New(l, logger)); err != nil {
			return err
		}
		return nil
	}

	return srv, serve, nil
}
