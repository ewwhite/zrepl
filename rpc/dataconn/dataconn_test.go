package dataconn

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zrepl/zrepl/logger"
	"github.com/zrepl/zrepl/replication/pdu"
)

func TestReqHeaderMarshalling(t *testing.T) {
	var r reqHeader
	secondline := `{"endpoint":"someendpoint","protolen":23}`
	input := fmt.Sprintf("ZREPL_DATA_CONN HEADER_LEN=%0*d\n%s\n", 10, len(secondline)+1, secondline)
	var buf bytes.Buffer
	buf.WriteString(input)
	err := r.unmarshalFromWire(&buf, 4096)
	assert.NoError(t, err)
	assert.Equal(t, "someendpoint", r.Endpoint)
	assert.Equal(t, uint32(23), r.ProtoLen)
}

func TestReqHeaderRoundtripMarshal(t *testing.T) {
	r := reqHeader{
		Endpoint:   "someendpointwith\ninit",
		ProtoLen:   23,
		HandlerErr: "some error message\n{}{}{}{}",
	}

	var buf bytes.Buffer
	assert.NoError(t, r.marshalToWire(&buf))
	buf.WriteString("teststream")

	var ur reqHeader
	assert.NoError(t, ur.unmarshalFromWire(&buf, 4096))
	assert.Equal(t, uint32(23), ur.ProtoLen)
	assert.Equal(t, "someendpointwith\ninit", ur.Endpoint)
	assert.Equal(t, "some error message\n{}{}{}{}", ur.HandlerErr)

	assert.Equal(t, string(buf.Bytes()), "teststream")
}

type mockHandler struct {
	handleSendErr   error
	handleSendRes   *pdu.SendRes
	handleSendBytes *mockReadCloser

	handleRecvErr   error
	handleRecvRes   *pdu.ReceiveRes
	recvStreamBytes *bytes.Buffer
}

type mockReadCloser struct {
	*bytes.Buffer
	closeError error
	closeCount int
}

func (c *mockReadCloser) Close() error {
	c.closeCount++
	if c.closeError != nil {
		return c.closeError
	}
	return nil
}

func (h *mockHandler) Send(ctx context.Context, r *pdu.SendReq) (*pdu.SendRes, io.ReadCloser, error) {
	if h.handleSendErr != nil {
		return nil, nil, h.handleSendErr
	}
	return h.handleSendRes, h.handleSendBytes, nil
}

func (h *mockHandler) Receive(ctx context.Context, r *pdu.ReceiveReq, stream io.Reader) (*pdu.ReceiveRes, error) {
	if h.handleRecvErr != nil {
		return nil, h.handleRecvErr
	}
	getLog(ctx).Printf("handler started: %T", stream)
	_, err := io.Copy(h.recvStreamBytes, stream)
	if err != nil {
		return nil, err
	}
	getLog(ctx).Printf("handler done")
	return h.handleRecvRes, nil
}

type mockConnecter struct {
	addr net.Addr
}

func (c mockConnecter) Connect(ctx context.Context) (net.Conn, error) {
	return net.Dial(c.addr.Network(), c.addr.String())
}

func TestClientServerBasics(t *testing.T) {

	l, err := net.Listen("tcp", "127.0.0.1:8888")
	require.NoError(t, err)

	mh := &mockHandler{
		// for send test
		handleSendRes:   &pdu.SendRes{},
		handleSendBytes: &mockReadCloser{Buffer: bytes.NewBuffer([]byte("teststream"))},

		// for recv test
		handleRecvRes:   &pdu.ReceiveRes{},
		recvStreamBytes: bytes.NewBuffer([]byte{}),
	}

	sharedConfig := SharedConfig{
		MaxProtoLen:      4096,
		MaxHeaderLen:     4096,
		SendChunkSize:    1 << 15,
		MaxRecvChunkSize: 1 << 15,
		IdleConnTimeout:  10 * time.Second,
	}

	srvConfig := ServerConfig{
		HeaderTimeout: 10 * time.Second,
		Shared:        sharedConfig,
	}
	require.NoError(t, srvConfig.Validate())
	srv := NewServer(mh, srvConfig, nil)

	ctx, shutdownServer := context.WithCancel(context.Background())
	ctx = WithLogger(ctx, logger.NewTestLogger(t))
	var serverDone sync.WaitGroup
	serverDone.Add(1)
	go func() {
		defer serverDone.Done()
		srv.Serve(ctx, l)
	}()

	mc := &mockConnecter{l.Addr()}
	clientConfig := ClientConfig{
		sharedConfig,
	}
	require.NoError(t, clientConfig.Validate())
	client := NewClient(mc, clientConfig)

	// actual test

	{
		req := pdu.SendReq{}
		res, stream, err := client.ReqSendStream(ctx, &req)
		assert.NoError(t, err)
		assert.NotNil(t, stream)
		assert.NotNil(t, res)

		var buf bytes.Buffer
		_, err = io.Copy(&buf, stream)
		assert.NoError(t, err)
		assert.Equal(t, "teststream", buf.String())
	}

	{
		var buf bytes.Buffer
		buf.WriteString("teststreamtobereceived")
		req := pdu.ReceiveReq{}
		res, err := client.ReqRecv(ctx, &req, &mockReadCloser{Buffer: &buf})
		assert.NoError(t, err)
		assert.NotNil(t, res)
		assert.Equal(t, 0, buf.Len())
		assert.Equal(t, "teststreamtobereceived", mh.recvStreamBytes.String())
	}

	shutdownServer()
	serverDone.Wait()
}

func TestMockReadCloser(t *testing.T) {
	var buf bytes.Buffer
	buf.WriteString("testcontentstobread")
	mrc := &mockReadCloser{Buffer: &buf}
	var target bytes.Buffer
	_, err := io.Copy(&target, mrc)
	assert.NoError(t, err)
	assert.Equal(t, target.String(), "testcontentstobread")

}
