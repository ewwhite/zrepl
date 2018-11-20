package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"net"
	"os"

	"github.com/pkg/profile"
	"github.com/zrepl/zrepl/endpoint/dataconn"
	"github.com/zrepl/zrepl/logger"
	"github.com/zrepl/zrepl/replication/pdu"
)

func orDie(err error) {
	if err != nil {
		panic(err)
	}
}

type devNullHandler struct{}

func (devNullHandler) HandleSend(ctx context.Context, r *pdu.SendTokenReq) (*pdu.SendTokenRes, io.ReadCloser, error) {
	var res pdu.SendTokenRes
	return &res, os.Stdin, nil
}

func (devNullHandler) HandleReceive(ctx context.Context, r *pdu.ReceiveTokenReq, stream io.Reader) (*pdu.ReceiveTokenRes, error) {
	var buf [1<<15]byte
	_, err := io.CopyBuffer(os.Stdout, stream, buf[:])
	var res pdu.ReceiveTokenRes
	return &res, err
}

type tcpConnecter struct {
	net, addr string
}

func (c tcpConnecter) Connect(ctx context.Context) (net.Conn, error) {
	return net.Dial(c.net, c.addr)
}

var args struct {
	addr      string
	appmode   string
	direction string
	profile   bool
}

func server() {

	log := logger.NewStderrDebugLogger()
	log.Debug("starting server")
	l, err := net.Listen("tcp", args.addr)
	orDie(err)

	srvConfig := dataconn.ServerConfig{
		MaxProtoLen:      4096,
		MaxHeaderLen:     4096,
		SendChunkSize:    1 << 17,
		MaxRecvChunkSize: 1 << 17,
	}
	srv := dataconn.NewServer(l, devNullHandler{}, srvConfig)

	ctx := context.Background()
	ctx = dataconn.WithLogger(ctx, log)
	srv.Serve(ctx)

}

func main() {

	flag.BoolVar(&args.profile, "profile", false, "")
	flag.StringVar(&args.addr, "address", ":8888", "")
	flag.StringVar(&args.appmode, "appmode", "client|server", "")
	flag.StringVar(&args.direction, "direction", "", "send|recv")
	flag.Parse()

	if args.profile {
		defer profile.Start(profile.CPUProfile).Stop()
	}

	switch args.appmode {
	case "client":
		client()
	case "server":
		server()
	default:
		orDie(fmt.Errorf("unknown appmode %q", args.appmode))
	}
}

func client() {

	logger := logger.NewStderrDebugLogger()
	ctx := context.Background()
	ctx = dataconn.WithLogger(ctx, logger)

	clientConfig := dataconn.ClientConfig{
		MaxProtoLen:      4096,
		MaxHeaderLen:     4096,
		SendChunkSize:    1 << 17,
		MaxRecvChunkSize: 1 << 17,
	}
	orDie(clientConfig.Validate())

	connecter := tcpConnecter{"tcp", args.addr}
	client := dataconn.NewClient(connecter, clientConfig)

	switch args.direction {
	case "send":
		req := pdu.SendTokenReq{}
		_, stream, err := client.ReqSendStream(ctx, &req)
		orDie(err)
		var buf [1<<15]byte
		_, err = io.CopyBuffer(os.Stdout, stream, buf[:])
		orDie(err)
	case "recv":
		var buf bytes.Buffer
		buf.WriteString("teststreamtobereceived")
		req := pdu.ReceiveTokenReq{}
		_, err := client.ReqRecv(ctx, &req, os.Stdin)
		orDie(err)
	default:
		orDie(fmt.Errorf("unknown direction%q", args.direction))
	}

}
