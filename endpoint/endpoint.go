// Package endpoint implements replication endpoints for use with package replication.
package endpoint

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/zrepl/zrepl/daemon/transport/transporthttpinjector"
	"github.com/zrepl/zrepl/replication"
	"github.com/zrepl/zrepl/replication/pdu"
	"github.com/zrepl/zrepl/util/envconst"
	"github.com/zrepl/zrepl/zfs"
	"io"
	"net/http"
	"path"
	"strings"
	"time"
)

type TokenStore interface {
	Add(data interface{}, expirationTime time.Time) (token string, err error)
	Take(token string) (data interface{}, err error)
}

// Sender implements replication.ReplicationEndpoint for a sending side
type Sender struct {
	FSFilter   zfs.DatasetFilter
	tokenStore TokenStore
}

var _ ReplicationServer = &Sender{}

func NewSender(fsf zfs.DatasetFilter, tokenStore TokenStore) *Sender {
	return &Sender{FSFilter: fsf, tokenStore: tokenStore}
}

func (s *Sender) filterCheckFS(fs string) (*zfs.DatasetPath, error) {
	dp, err := zfs.NewDatasetPath(fs)
	if err != nil {
		return nil, err
	}
	if dp.Length() == 0 {
		return nil, errors.New("empty filesystem not allowed")
	}
	pass, err := s.FSFilter.Filter(dp)
	if err != nil {
		return nil, err
	}
	if !pass {
		return nil, replication.NewFilteredError(fs)
	}
	return dp, nil
}


func (s *Sender) ListFilesystems(ctx context.Context, r *pdu.ListFilesystemReq) (*pdu.ListFilesystemRes, error) {
	fss, err := zfs.ZFSListMapping(s.FSFilter)
	if err != nil {
		return nil, err
	}
	rfss := make([]*pdu.Filesystem, len(fss))
	for i := range fss {
		rfss[i] = &pdu.Filesystem{
			Path: fss[i].ToString(),
			// FIXME: not supporting ResumeToken yet
		}
	}
	res := &pdu.ListFilesystemRes{Filesystems: rfss}
	return res, nil
}

func (s *Sender) ListFilesystemVersions(ctx context.Context, r *pdu.ListFilesystemVersionsReq) (*pdu.ListFilesystemVersionsRes, error) {
	lp, err := s.filterCheckFS(r.GetFilesystem())
	if err != nil {
		return nil, err
	}
	fsvs, err := zfs.ZFSListFilesystemVersions(lp, nil)
	if err != nil {
		return nil, err
	}
	rfsvs := make([]*pdu.FilesystemVersion, len(fsvs))
	for i := range fsvs {
		rfsvs[i] = pdu.FilesystemVersionFromZFS(&fsvs[i])
	}
	res := &pdu.ListFilesystemVersionsRes{Versions: rfsvs}
	return res, nil

}

func (s *Sender) GetSendToken(ctx context.Context, r *pdu.SendTokenReq) (*pdu.SendTokenRes, error) {
	_, err := s.filterCheckFS(r.Filesystem)
	if err != nil {
		return nil, err
	}

	if r.DryRun {
		si, err := zfs.ZFSSendDry(r.Filesystem, r.From, r.To, "")
		if err != nil {
			return nil, err
		}
		var expSize int64 = 0 // protocol says 0 means no estimate
		if si.SizeEstimate != -1 { // but si returns -1 for no size estimate
			expSize = si.SizeEstimate
		}
		return &pdu.SendTokenRes{ExpectedSize: expSize}, nil
	} else {
		expTime := time.Now().Add(envconst.Duration("ENDPOINT_SENDER_TOKEN_EXPIRATION", 10*time.Second))
		tok, err := s.tokenStore.Add(r, expTime)
		if err != nil {
			return nil, err
		}
		return &pdu.SendTokenRes{SendToken: tok}, nil
	}
}

func (s *Sender) DoSend(ctx context.Context, token string) (io.ReadCloser, error) {
	rI, err := s.tokenStore.Take(token)
	if err != nil {
		return nil, err
	}
	r := rI.(*pdu.SendTokenReq)

	stream, err := zfs.ZFSSend(ctx, r.Filesystem, r.From, r.To, "")
	if err != nil {
		return nil, err
	}
	return stream, nil
}

func (s *Sender) GetReceiveToken(context.Context, *pdu.ReceiveTokenReq) (*pdu.ReceiveTokenRes, error) {
	return nil, fmt.Errorf("this is a sender endpoint")
}

func (s *Sender) DoReceive(ctx context.Context, token string, zfsStream io.ReadCloser) error {
	return fmt.Errorf("this is a sender endpoint")
}

func (p *Sender) DestroySnapshots(ctx context.Context, req *pdu.DestroySnapshotsReq) (*pdu.DestroySnapshotsRes, error) {
	dp, err := p.filterCheckFS(req.Filesystem)
	if err != nil {
		return nil, err
	}
	return doDestroySnapshots(ctx, dp, req.Snapshots)
}

func (p *Sender) ReplicationCursor(ctx context.Context, req *pdu.ReplicationCursorReq) (*pdu.ReplicationCursorRes, error) {
	dp, err := p.filterCheckFS(req.Filesystem)
	if err != nil {
		return nil, err
	}

	switch op := req.Op.(type) {
	case *pdu.ReplicationCursorReq_Get:
		cursor, err := zfs.ZFSGetReplicationCursor(dp)
		if err != nil {
			return nil, err
		}
		if cursor == nil {
			return &pdu.ReplicationCursorRes{Result: &pdu.ReplicationCursorRes_Notexist{Notexist: true}}, nil
		}
		return &pdu.ReplicationCursorRes{Result: &pdu.ReplicationCursorRes_Guid{Guid: cursor.Guid}}, nil
	case *pdu.ReplicationCursorReq_Set:
		guid, err := zfs.ZFSSetReplicationCursor(dp, op.Set.Snapshot)
		if err != nil {
			return nil, err
		}
		return &pdu.ReplicationCursorRes{Result: &pdu.ReplicationCursorRes_Guid{Guid: guid}}, nil
	default:
		return nil, errors.Errorf("unknown op %T", op)
	}
}

type FSFilter interface { // FIXME unused
	Filter(path *zfs.DatasetPath) (pass bool, err error)
}

// FIXME: can we get away without error types here?
type FSMap interface { // FIXME unused
	FSFilter
	Map(path *zfs.DatasetPath) (*zfs.DatasetPath, error)
	Invert() (FSMap, error)
	AsFilter() FSFilter
}

// Receiver implements replication.ReplicationEndpoint for a receiving side
type Receiver struct {
	rootWithoutClientComponent *zfs.DatasetPath
	tokenStore TokenStore
}

var _ ReplicationServer = &Receiver{}

func NewReceiver(rootDataset *zfs.DatasetPath, tokenStore TokenStore) (*Receiver) {
	if rootDataset.Length() <= 0 {
		panic(fmt.Sprintf("root dataset must not be an empty path: %v", rootDataset))
	}
	return &Receiver{rootWithoutClientComponent: rootDataset.Copy(), tokenStore: tokenStore}
}

func TestClientIdentity(rootFS *zfs.DatasetPath, clientIdentity string) error {
	_, err := clientRoot(rootFS, clientIdentity)
	return err
}

func clientRoot(rootFS *zfs.DatasetPath, clientIdentity string) (*zfs.DatasetPath, error) {
	rootFSLen := rootFS.Length()
	clientRootStr := path.Join(rootFS.ToString(), clientIdentity)
	clientRoot, err := zfs.NewDatasetPath(clientRootStr)
	if err != nil {
		return nil, err
	}
	if rootFSLen + 1 != clientRoot.Length() {
		return nil, fmt.Errorf("client identity must be a single ZFS filesystem path component")
	}
	return clientRoot, nil
}

func (s *Receiver) clientRootFromCtx(ctx context.Context) (*zfs.DatasetPath) {
	clientIdentity := transporthttpinjector.ClientIdentity(ctx)
	if clientIdentity == "" {
		panic(fmt.Sprintf("transporthttpinjector.ClientIdentity must be set"))
	}

	clientRoot, err := clientRoot(s.rootWithoutClientComponent, clientIdentity)
	if err != nil {
		panic(fmt.Sprintf("ClientIdentityContextKey must have been validated before invoking Receiver: %s", err))
	}
	return clientRoot
}

type subroot struct {
	localRoot *zfs.DatasetPath
}

var _ zfs.DatasetFilter = subroot{}

// Filters local p
func (f subroot) Filter(p *zfs.DatasetPath) (pass bool, err error) {
	return p.HasPrefix(f.localRoot) && !p.Equal(f.localRoot), nil
}

func (f subroot) MapToLocal(fs string) (*zfs.DatasetPath, error) {
	p, err := zfs.NewDatasetPath(fs)
	if err != nil {
		return nil, err
	}
	if p.Length() == 0 {
		return nil, errors.Errorf("cannot map empty filesystem")
	}
	c := f.localRoot.Copy()
	c.Extend(p)
	return c, nil
}


func (s *Receiver) ListFilesystems(ctx context.Context, req *pdu.ListFilesystemReq) (*pdu.ListFilesystemRes, error) {
	root := s.clientRootFromCtx(ctx)
	filtered, err := zfs.ZFSListMapping(subroot{root})
	if err != nil {
		return nil, err
	}
	// present without prefix, and only those that are not placeholders
	fss := make([]*pdu.Filesystem, 0, len(filtered))
	for _, a := range filtered {
		ph, err := zfs.ZFSIsPlaceholderFilesystem(a)
		if err != nil {
			getLogger(ctx).
				WithError(err).
				WithField("fs", a).
				Error("inconsistent placeholder property")
			return nil, errors.New("server error, see logs") // don't leak path
		}
		if ph {
			continue
		}
		a.TrimPrefix(root)
		fss = append(fss, &pdu.Filesystem{Path: a.ToString()})
	}
	return &pdu.ListFilesystemRes{Filesystems: fss}, nil
}

func (s *Receiver) ListFilesystemVersions(ctx context.Context, req *pdu.ListFilesystemVersionsReq) (*pdu.ListFilesystemVersionsRes, error) {
	root := s.clientRootFromCtx(ctx)
	lp, err := subroot{root}.MapToLocal(req.GetFilesystem())
	if err != nil {
		return nil, err
	}

	fsvs, err := zfs.ZFSListFilesystemVersions(lp, nil)
	if err != nil {
		return nil, err
	}

	rfsvs := make([]*pdu.FilesystemVersion, len(fsvs))
	for i := range fsvs {
		rfsvs[i] = pdu.FilesystemVersionFromZFS(&fsvs[i])
	}

	return &pdu.ListFilesystemVersionsRes{Versions: rfsvs}, nil
}

func (s *Receiver) ReplicationCursor(context.Context, *pdu.ReplicationCursorReq) (*pdu.ReplicationCursorRes, error) {
	return nil, fmt.Errorf("ReplicationCursor not implemented for Receiver")
}

func (s *Receiver) GetSendToken(context.Context, *pdu.SendTokenReq) (*pdu.SendTokenRes, error) {
	return nil, fmt.Errorf("Send not implemented for Receiver")
}
func (s *Receiver) DoSend(ctx context.Context, token string) (io.ReadCloser, error) {
	return nil, fmt.Errorf("DoSend not implemented for Receiver")
}

type receiveToken struct {
	clientRoot *zfs.DatasetPath
	req *pdu.ReceiveTokenReq
}

func (s *Receiver) GetReceiveToken(ctx context.Context, req *pdu.ReceiveTokenReq) (*pdu.ReceiveTokenRes, error) {
	root := s.clientRootFromCtx(ctx)
	_, err := subroot{root}.MapToLocal(req.Filesystem)
	if err != nil {
		return nil, err
	}

	expTime := time.Now().Add(envconst.Duration("ENDPOINT_RECEIVER_TOKEN_EXPIRATION", 10*time.Second))
	token, err := s.tokenStore.Add(receiveToken{clientRoot: root, req: req}, expTime)
	if err != nil {
		return nil, err
	}
	return &pdu.ReceiveTokenRes{ReceiveToken: token}, nil
}

func (s *Receiver) DoReceive(ctx context.Context, token string, zfsStream io.ReadCloser) error {
	defer zfsStream.Close()

	rI, err := s.tokenStore.Take(token)
	if err != nil {
		return err
	}
	r := rI.(receiveToken)

	lp, err := subroot{r.clientRoot}.MapToLocal(r.req.Filesystem) // FIXME this work has already been done
	if err != nil {
		return err
	}

	getLogger(ctx).Debug("incoming Receive")

	// create placeholder parent filesystems as appropriate
	var visitErr error
	f := zfs.NewDatasetPathForest()
	f.Add(lp)
	getLogger(ctx).Debug("begin tree-walk")
	f.WalkTopDown(func(v zfs.DatasetPathVisit) (visitChildTree bool) {
		if v.Path.Equal(lp) {
			return false
		}
		_, err := zfs.ZFSGet(v.Path, []string{zfs.ZREPL_PLACEHOLDER_PROPERTY_NAME})
		if err != nil {
			// interpret this as an early exit of the zfs binary due to the fs not existing
			if err := zfs.ZFSCreatePlaceholderFilesystem(v.Path); err != nil {
				getLogger(ctx).
					WithError(err).
					WithField("placeholder_fs", v.Path).
					Error("cannot create placeholder filesystem")
				visitErr = err
				return false
			}
		}
		getLogger(ctx).WithField("filesystem", v.Path.ToString()).Debug("exists")
		return true // leave this fs as is
	})
	getLogger(ctx).WithField("visitErr", visitErr).Debug("complete tree-walk")

	if visitErr != nil {
		return err
	}

	needForceRecv := false
	props, err := zfs.ZFSGet(lp, []string{zfs.ZREPL_PLACEHOLDER_PROPERTY_NAME})
	if err == nil {
		if isPlaceholder, _ := zfs.IsPlaceholder(lp, props.Get(zfs.ZREPL_PLACEHOLDER_PROPERTY_NAME)); isPlaceholder {
			needForceRecv = true
		}
	}

	args := make([]string, 0, 1)
	if needForceRecv {
		args = append(args, "-F")
	}

	getLogger(ctx).Debug("start receive command")

	if err := zfs.ZFSRecv(ctx, lp.ToString(), zfsStream, args...); err != nil {
		getLogger(ctx).
			WithError(err).
			WithField("args", args).
			Error("zfs receive failed")
		return err
	}
	return nil
}

func (s *Receiver) DestroySnapshots(ctx context.Context, req *pdu.DestroySnapshotsReq) (*pdu.DestroySnapshotsRes, error) {
	root := s.clientRootFromCtx(ctx)
	lp, err := subroot{root}.MapToLocal(req.Filesystem)
	if err != nil {
		return nil, err
	}
	return doDestroySnapshots(ctx, lp, req.Snapshots)
}

func doDestroySnapshots(ctx context.Context, lp *zfs.DatasetPath, snaps []*pdu.FilesystemVersion) (*pdu.DestroySnapshotsRes, error) {
	fsvs := make([]*zfs.FilesystemVersion, len(snaps))
	for i, fsv := range snaps {
		if fsv.Type != pdu.FilesystemVersion_Snapshot {
			return nil, fmt.Errorf("version %q is not a snapshot", fsv.Name)
		}
		var err error
		fsvs[i], err = fsv.ZFSFilesystemVersion()
		if err != nil {
			return nil, err
		}
	}
	res := &pdu.DestroySnapshotsRes{
		Results: make([]*pdu.DestroySnapshotRes, len(fsvs)),
	}
	for i, fsv := range fsvs {
		err := zfs.ZFSDestroyFilesystemVersion(lp, fsv)
		errMsg := ""
		if err != nil {
			errMsg = err.Error()
		}
		res.Results[i] = &pdu.DestroySnapshotRes{
			Snapshot: pdu.FilesystemVersionFromZFS(fsv),
			Error:    errMsg,
		}
	}
	return res, nil
}

// FIXME name
type ReplicationServer interface {
	pdu.ReplicationServer
	DoSend(ctx context.Context, token string) (io.ReadCloser, error)
	DoReceive(ctx context.Context, token string, zfsStream io.ReadCloser) error
}

// HttpHandler implements http.Handler for a Receiver or Sender
type HttpHandler struct {
	mux *http.ServeMux
	srv ReplicationServer
}

var _ http.Handler

const DoSendPathPrefix = "/zrepl/DoSend/" // trailing slash is important for http.ServeMux patterns
const DoReceivePathPrefix = "/zrepl/DoReceive/" // trailing slash is important for http.ServeMux patterns

func init() {
	if strings.HasPrefix(pdu.ReplicationServerPathPrefix, DoSendPathPrefix) {
		panic(fmt.Sprintf("ReplicationServerPathPrefix must not have DoSendPathPrefix"))
	}
	if strings.HasPrefix(pdu.ReplicationServerPathPrefix, DoReceivePathPrefix) {
		panic(fmt.Sprintf("ReplicationServerPathPrefix must not have DoReceivePathPrefix"))
	}
}

func ToHandler(srv ReplicationServer) *HttpHandler {
	h := &HttpHandler{
		mux: http.NewServeMux(),
		srv: srv,
	}
	twirpHandler := pdu.NewReplicationServerServer(h.srv, nil)
	h.mux.Handle(pdu.ReplicationServerPathPrefix, twirpHandler)
	h.mux.HandleFunc(DoSendPathPrefix, func(w http.ResponseWriter, r *http.Request) {
		h.handleSendRecv(0, w, r)
	})
	h.mux.HandleFunc(DoReceivePathPrefix, func(w http.ResponseWriter, r *http.Request) {
		h.handleSendRecv(1, w, r)
	})
	return h
}

func (s *HttpHandler) handleSendRecv(mode int, w http.ResponseWriter, r *http.Request) {

	// decode token from URL
	token := path.Base(r.URL.Path)

	switch mode {
	default:
		panic(fmt.Sprintf("implementation error: unknown mode %d", mode))
	case 0:
		stream, err := s.srv.DoSend(r.Context(), token)
		if err != nil {
			// TODO classify error as server or client side
			w.WriteHeader(500)
			fmt.Fprintf(w, "%s", err)
		} else {
			w.WriteHeader(200)
			_, err := io.Copy(w, stream)
			// this error could be on the receiving side, network related, or our side
			// thus, we should log it
			if err != nil {
				// TODO log it
			}
		}
	case 1:
		err := s.srv.DoReceive(r.Context(), token, r.Body)
		if err != nil {
			// TODO classify error as server or client side
			w.WriteHeader(500)
			fmt.Fprintf(w , "%s", err)
		} else {
			w.WriteHeader(200)
			fmt.Fprintf(w, "transfer successful")
		}
	}
}


func (s *HttpHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	client_identity := transporthttpinjector.ClientIdentity(r.Context())
	if client_identity == "" {
		panic(fmt.Sprintf("implementation error: endpoint.HttpHandler used outside of transporthttpinjector: %s", r.RemoteAddr))
	}
	s.mux.ServeHTTP(w, r)
}

// HttpClient implements the interfaces required by package replication
// for a remote instance of ReplicationServer
type HttpClient struct {
	transport *http.Transport
	twirpHttpClient    http.Client
	bulkTransferClient http.Client

	pdu.ReplicationServer // this the twirp client instance, see constructor
}

var _ replication.Endpoint = &HttpClient{}
var _ replication.Sender = &HttpClient{}
var _ replication.Receiver = &HttpClient{}

func NewClient(transport *http.Transport) *HttpClient {
	twirpHttpClient := http.Client{Transport: transport}
	bulkTransferClient := http.Client{Transport: transport}
	c := &HttpClient{
		transport: transport,
		twirpHttpClient: twirpHttpClient,
		bulkTransferClient: bulkTransferClient,
	}
	c.ReplicationServer = pdu.NewReplicationServerProtobufClient("http://daemon", &c.twirpHttpClient) // '/' could be anything
	return c
}

func (c *HttpClient) Send(ctx context.Context, r *pdu.SendTokenReq) (*pdu.SendTokenRes, io.ReadCloser, error) {
	res, err := c.ReplicationServer.GetSendToken(ctx, r)
	if err != nil {
		return nil, nil, err
	}
	if r.DryRun {
		return res, nil, nil
	}

	url := fmt.Sprintf("http://daemon%s%s", DoSendPathPrefix, res.GetSendToken())
	sendRes, err := c.bulkTransferClient.Get(url)
	if err != nil {
		return nil, nil, err
	}
	if sendRes.StatusCode != 200 { // TODO 200 too restrictive?
		var errorMsg strings.Builder
		io.Copy(&errorMsg, io.LimitReader(sendRes.Body, 1 << 15)) // TODO error handling?
		return nil, nil, fmt.Errorf("remote send error: %s", strings.TrimSpace(errorMsg.String()))
	}
	return res, sendRes.Body, nil
}

func (c *HttpClient) Receive(ctx context.Context, r *pdu.ReceiveTokenReq, sendStream io.ReadCloser) error {
	defer sendStream.Close()

	res, err := c.ReplicationServer.GetReceiveToken(ctx, r)
	if err != nil {
		return err
	}

	url := fmt.Sprintf("http://daemon%s%s", DoReceivePathPrefix, res.GetReceiveToken())
	receiveRes, err := c.bulkTransferClient.Post(url, "application/octet-stream", sendStream)
	if err != nil {
		return err
	}
	if receiveRes.StatusCode != 200 { // TODO 200 too restrictive?
		var errorMsg strings.Builder
		io.Copy(&errorMsg, io.LimitReader(receiveRes.Body, 1 << 15)) // TODO error handling?
		return fmt.Errorf("remote receive error: %s", strings.TrimSpace(errorMsg.String()))
	}
	return nil
}

type LocalClient struct {
	ReplicationServer // instance of Sender or Receiver
}

var _ replication.Endpoint = &LocalClient{}
var _ replication.Sender = &LocalClient{}
var _ replication.Receiver = &LocalClient{}

func NewLocal(server ReplicationServer) *LocalClient {
	return &LocalClient{ReplicationServer: server}
}

func (c LocalClient) Send(ctx context.Context, r *pdu.SendTokenReq) (*pdu.SendTokenRes, io.ReadCloser, error) {
	res, err := c.ReplicationServer.GetSendToken(ctx, r)
	if err != nil {
		return nil, nil, err
	}
	if r.DryRun {
		return res, nil, nil
	}

	stream, err := c.ReplicationServer.DoSend(ctx, res.GetSendToken())
	if err != nil {
		return nil, nil, err
	}
	return res, stream, nil
}

func (c LocalClient) Receive(ctx context.Context, r *pdu.ReceiveTokenReq, sendStream io.ReadCloser) error {
	defer sendStream.Close()
	res, err := c.ReplicationServer.GetReceiveToken(ctx, r)
	if err != nil {
		return err
	}
	err = c.ReplicationServer.DoReceive(ctx, res.GetReceiveToken(), sendStream)
	if err != nil {
		return err
	}
	return nil
}