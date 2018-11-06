// Package endpoint implements replication endpoints for use with package replication.
package endpoint

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
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

func (s *Sender) DoSend(token string) (io.ReadCloser, error) {
	rI, err := s.tokenStore.Take(token)
	if err != nil {
		return nil, err
	}
	r := rI.(*pdu.SendTokenReq)

	stream, err := zfs.ZFSSend(context.Background(), r.Filesystem, r.From, r.To, "")
	if err != nil {
		return nil, err
	}
	return stream, nil
}

func (s *Sender) GetReceiveToken(context.Context, *pdu.ReceiveTokenReq) (*pdu.ReceiveTokenRes, error) {
	return nil, fmt.Errorf("this is a sender endpoint")
}

func (s *Sender) DoReceive(token string, zfsStream io.ReadCloser) error {
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
			return &pdu.ReplicationCursorRes{Result: &pdu.ReplicationCursorRes_Error{Error: "cursor does not exist"}}, nil
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
	root *zfs.DatasetPath
	tokenStore TokenStore
}

var _ ReplicationServer = &Receiver{}

func NewReceiver(rootDataset *zfs.DatasetPath, tokenStore TokenStore) (*Receiver, error) {
	if rootDataset.Length() <= 0 {
		return nil, errors.New("root dataset must not be an empty path")
	}
	return &Receiver{root: rootDataset.Copy(), tokenStore: tokenStore}, nil
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
	filtered, err := zfs.ZFSListMapping(subroot{s.root})
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
		a.TrimPrefix(s.root)
		fss = append(fss, &pdu.Filesystem{Path: a.ToString()})
	}
	return &pdu.ListFilesystemRes{Filesystems: fss}, nil
}

func (s *Receiver) ListFilesystemVersions(ctx context.Context, req *pdu.ListFilesystemVersionsReq) (*pdu.ListFilesystemVersionsRes, error) {
	lp, err := subroot{s.root}.MapToLocal(req.GetFilesystem())
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
func (s *Receiver) DoSend(token string) (io.ReadCloser, error) {
	return nil, fmt.Errorf("DoSend not implemented for Receiver")
}

func (s *Receiver) GetReceiveToken(ctx context.Context, req *pdu.ReceiveTokenReq) (*pdu.ReceiveTokenRes, error) {

	_, err := subroot{s.root}.MapToLocal(req.Filesystem)
	if err != nil {
		return nil, err
	}

	expTime := time.Now().Add(envconst.Duration("ENDPOINT_RECEIVER_TOKEN_EXPIRATION", 10*time.Second))
	token, err := s.tokenStore.Add(req, expTime)
	if err != nil {
		return nil, err
	}
	return &pdu.ReceiveTokenRes{ReceiveToken: token}, nil
}

func (s *Receiver) DoReceive(token string, zfsStream io.ReadCloser) error {
	defer zfsStream.Close()

	rI, err := s.tokenStore.Take(token)
	if err != nil {
		return err
	}
	r := rI.(*pdu.ReceiveTokenReq)

	lp, err := subroot{s.root}.MapToLocal(r.Filesystem) // FIXME this work has already been done
	if err != nil {
		return err
	}

	ctx := context.Background() // FIXME

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
	lp, err := subroot{s.root}.MapToLocal(req.Filesystem)
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
	DoSend(token string) (io.ReadCloser, error)
	DoReceive(token string, zfsStream io.ReadCloser) error
}

// HttpServer implements http.Handler for a Receiver or Sender
type HttpServer struct {
	mux *http.ServeMux
	srv ReplicationServer
}

var _ http.Handler

const DoSendPathPrefix = "/zrepl/DoSend/"
const DoReceivePathPrefix = "/zrepl/DoRecieve"

func init() {
	if strings.HasPrefix(pdu.ReplicationServerPathPrefix, DoSendPathPrefix) {
		panic(fmt.Sprintf("ReplicationServerPathPrefix must not have DoSendPathPrefix"))
	}
	if strings.HasPrefix(pdu.ReplicationServerPathPrefix, DoReceivePathPrefix) {
		panic(fmt.Sprintf("ReplicationServerPathPrefix must not have DoReceivePathPrefix"))
	}
}

func NewServer(srv ReplicationServer) *HttpServer {
	s := &HttpServer{
		mux: http.NewServeMux(),
		srv: srv,
	}
	twirpHandler := pdu.NewReplicationServerServer(s.srv, nil)
	s.mux.Handle(pdu.ReplicationServerPathPrefix, twirpHandler)
	s.mux.HandleFunc(DoSendPathPrefix, func(w http.ResponseWriter, r *http.Request) {
		s.handleSendRecv(0, w, r)
	})
	s.mux.HandleFunc(DoReceivePathPrefix, func(w http.ResponseWriter, r *http.Request) {
		s.handleSendRecv(1, w, r)
	})
	return s
}

func (s *HttpServer) handleSendRecv(mode int, w http.ResponseWriter, r *http.Request) {

	// decode token from URL
	token := path.Base(r.URL.Path)

	switch mode {
	default:
		panic(fmt.Sprintf("implementation error: unknown mode %d", mode))
	case 0:
		stream, err := s.srv.DoSend(token)
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
		err := s.srv.DoReceive(token, r.Body)
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

func (s *HttpServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.ServeHTTP(w, r)
}

// Client implements the interfaces required by package replication
type Client struct {
	baseURL string
	client http.Client // TODO ideally, the same client as pdu.ReplicationServer ?
	pdu.ReplicationServer // the client instance
}

var _ replication.Endpoint = &Client{}
var _ replication.Sender = &Client{}
var _ replication.Receiver = &Client{}

func (c *Client) Send(ctx context.Context, r *pdu.SendTokenReq) (*pdu.SendTokenRes, io.ReadCloser, error) {
	res, err := c.ReplicationServer.GetSendToken(ctx, r)
	if err != nil {
		return nil, nil, err
	}
	if r.DryRun {
		return res, nil, nil
	}

	url := fmt.Sprintf("%s%s", c.baseURL, DoSendPathPrefix)
	sendRes, err := c.client.Get(url)
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

func (c *Client) Receive(ctx context.Context, r *pdu.ReceiveTokenReq, sendStream io.ReadCloser) error {
	defer sendStream.Close()

	res, err := c.ReplicationServer.GetReceiveToken(ctx, r)
	if err != nil {
		return err
	}

	url := fmt.Sprintf("%s%s/%s", c.baseURL, DoReceivePathPrefix, res.GetReceiveToken())
	receiveRes, err := c.client.Post(url, "application/octet-stream", sendStream)
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
