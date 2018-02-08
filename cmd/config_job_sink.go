package cmd

import (
	"context"
	"io"

	"fmt"
	"github.com/pkg/errors"
	"github.com/zrepl/zrepl/rpc"
	"github.com/zrepl/zrepl/util"
	"github.com/zrepl/zrepl/zfs"

	"bytes"
	"encoding/binary"
	"encoding/json"
	"github.com/mitchellh/mapstructure"
)

type SinkJob struct {
	Name           string
	Serve          AuthenticatedChannelListenerFactory
	Mapping        *DatasetMapFilter
	pruneFilter    zfs.DatasetFilter
	SnapshotPrefix string
	Prune          PrunePolicy
	Debug          JobDebugSettings
	serveTask      *Task
}

func parseSinkJob(c JobParsingContext, name string, i map[string]interface{}) (j *SinkJob, err error) {

	var asMap struct {
		Serve          map[string]interface{}
		Mapping        map[string]string
		Prune          map[string]interface{}
		SnapshotPrefix string `mapstructure:"snapshot_prefix"`
		Debug          map[string]interface{}
	}

	if err = mapstructure.Decode(i, &asMap); err != nil {
		err = errors.Wrap(err, "mapstructure error")
		return nil, err
	}

	j = &SinkJob{Name: name}

	if j.Serve, err = parseAuthenticatedChannelListenerFactory(c, asMap.Serve); err != nil {
		return
	}

	j.Mapping, err = parseDatasetMapFilter(asMap.Mapping, false)
	if err != nil {
		err = errors.Wrap(err, "cannot parse 'mapping'")
		return nil, err
	}

	if j.pruneFilter, err = j.Mapping.InvertedFilter(); err != nil {
		err = errors.Wrap(err, "cannot automatically invert 'mapping' for prune job")
		return nil, err
	}

	if j.SnapshotPrefix, err = parseSnapshotPrefix(asMap.SnapshotPrefix); err != nil {
		return
	}

	if j.Prune, err = parsePrunePolicy(asMap.Prune); err != nil {
		err = errors.Wrap(err, "cannot parse prune policy")
		return
	}

	if err = mapstructure.Decode(asMap.Debug, &j.Debug); err != nil {
		err = errors.Wrap(err, "cannot parse 'debug'")
		return
	}

	return
}

func (j *SinkJob) JobName() string {
	return j.Name
}

func (j *SinkJob) JobStart(ctx context.Context) {

	log := ctx.Value(contextKeyLog).(Logger)
	defer log.Info("exiting")

	j.serveTask = NewTask("serve", log)

	var err error
	if err != nil {
		log.WithError(err).Error("error creating pruner")
		return
	}

	// TODO pruning
	j.serve(ctx, j.serveTask)

}

func (j *SinkJob) JobStatus(ctxt context.Context) (*JobStatus, error) {
	return &JobStatus{
		Tasks: []*TaskStatus{
			j.serveTask.Status(),
		}}, nil
}

func (j *SinkJob) serve(ctx context.Context, task *Task) {

	log := ctx.Value(contextKeyLog).(Logger)

	listener, err := j.Serve.Listen()
	if err != nil {
		log.WithError(err).Error("error listening")
		return
	}

	type rwcChanMsg struct {
		rwc io.ReadWriteCloser
		err error
	}
	rwcChan := make(chan rwcChanMsg)

	// Serve connections until interrupted or error
outer:
	for {

		go func() {
			rwc, err := listener.Accept()
			if err != nil {
				rwcChan <- rwcChanMsg{rwc, err}
				close(rwcChan)
				return
			}
			rwcChan <- rwcChanMsg{rwc, err}
		}()

		select {

		case rwcMsg := <-rwcChan:

			if rwcMsg.err != nil {
				log.WithError(err).Error("error accepting connection")
				break outer
			}

			j.handleConnection(rwcMsg.rwc, task)

		case <-ctx.Done():
			log.WithError(ctx.Err()).Info("context")
			break outer

		}

	}

	task.Enter("close_listener")
	defer task.Finish()
	err = listener.Close()
	if err != nil {
		log.WithError(err).Error("error closing listener")
	}

	return

}

func (j *SinkJob) handleConnection(rwc io.ReadWriteCloser, task *Task) {

	task.Enter("handle_connection")
	defer task.Finish()

	rwc, err := util.NewReadWriteCloserLogger(rwc, j.Debug.Conn.ReadDump, j.Debug.Conn.WriteDump)
	if err != nil {
		panic(err)
	}

	// construct connection handler
	handler := SinkHandler{task, j.Mapping, NewPrefixFilter(j.SnapshotPrefix)}

	// handle connection
	rpcServer := rpc.NewServer(rwc)
	if j.Debug.RPC.Log {
		rpclog := task.Log().WithField("subsystem", "rpc")
		rpcServer.SetLogger(rpclog, true)
	}
	handler.MustRegisterEndpoints(rpcServer)
	if err = rpcServer.Serve(); err != nil {
		task.Log().WithError(err).Error("error serving connection")
	}
	rwc.Close()
}

type ReceiveRequestHeader struct {
	Filesystem *zfs.DatasetPath
	Rollback   bool
}
type ReceiveRequest struct {
	Header ReceiveRequestHeader
	Stream io.Reader
}

func (r *ReceiveRequest) ToReader() (io.Reader, error) {
	var header bytes.Buffer
	if err := json.NewEncoder(&header).Encode(r.Header); err != nil {
		return nil, err
	}
	var headerLen [4]byte
	binary.LittleEndian.PutUint32(headerLen[:], uint32(header.Len()))
	reader := util.NewChainedReader(
		bytes.NewBuffer(headerLen[:]), &header, r.Stream,
	)
	return reader, nil
}

func ReceiveRequestFromReader(r io.Reader) (*ReceiveRequest, error) {
	var headerLenBytes [4]byte
	if _, err := io.ReadFull(r, headerLenBytes[:]); err != nil {
		return nil, err
	}
	headerLen := binary.LittleEndian.Uint32(headerLenBytes[:])
	req := &ReceiveRequest{}
	headerReader := io.LimitReader(r, int64(headerLen))
	if err := json.NewDecoder(headerReader).Decode(&req.Header); err != nil {
		return nil, err
	}
	// drain headerReader
	var scratch [1]byte // terminating newline, more shouldn't be allowed
	n, err := headerReader.Read(scratch[:])
	if err != nil && err != io.EOF {
		return nil, err
	}
	if err != io.EOF {
		return nil, fmt.Errorf("header shorter than indicated in headerLen, got %d bytes", n)
	}
	req.Stream = r
	return req, nil
}

type SinkHandler struct {
	task          *Task
	fsMapping     *DatasetMapFilter
	versionFilter zfs.FilesystemVersionFilter
}

func (h *SinkHandler) MustRegisterEndpoints(server rpc.RPCServer) {
	if err := server.RegisterEndpoint("FilesystemVersionsRequest", h.FilesystemVersionsRequest); err != nil {
		panic(err)
	}
	if err := server.RegisterEndpoint("ReceiveRequest", h.ReceiveRequest); err != nil {
		panic(err)
	}
}

func (h *SinkHandler) FilesystemVersionsRequest(r *FilesystemVersionsRequest, versions *[]zfs.FilesystemVersion) (err error) {

	log := h.task.Log().WithField("endpoint", "FilesystemVersionsRequest")

	log.WithField("req_filesystem", r.Filesystem.ToString()).Debug("request")

	localFS, err := h.fsMapping.Map(r.Filesystem)
	if err != nil {
		log.WithError(err).Error("error mapping requested filesystem")
		return
	}

	exists, err := zfs.ZFSDatasetExists(localFS.ToString())
	if err != nil {
		log.WithError(err).Error("error checking if filesystem exists")
	}
	if !exists {
		log.Debug("local filesystem does not exist, returning empty list")
		*versions = make([]zfs.FilesystemVersion, 0)
		return
	}

	// find our versions
	vs, err := zfs.ZFSListFilesystemVersions(localFS, h.versionFilter)
	if err != nil {
		log.WithError(err).Error("cannot list filesystem versions")
		return
	}

	log.WithField("response", vs).Debug("response")

	*versions = vs
	return

}

func (h *SinkHandler) ReceiveRequest(reqEncoded io.Reader, confirm *struct{}) error {
	log := h.task.Log().WithField("endpoint", "ReceiveRequest")

	// FIXME: rpc should support 'multipart' params, i.e. array of JSON and io.Reader
	req, err := ReceiveRequestFromReader(reqEncoded)
	if err != nil {
		log.WithError(err).Error("cannot decode receive request")
		return err
	}

	log.WithField("request", req.Header).Debug("request")

	// allowed to push there?
	localFS, err := h.fsMapping.Map(req.Header.Filesystem)
	if err != nil {
		log.WithError(err).Error("error mapping requested filesystem")
		return nil
	}

	log.WithField("local_fs", localFS.ToString()).Info("receiving into local filesystem")

	// create parent fs placeholders if necessary
	// FIXME cache ZFSDatasetExists between queries
	log.Debug("creating parent placeholder filesystems as necessary")
	localTraversal := zfs.NewDatasetPathForest()
	localTraversal.Add(localFS)
	localTraversal.WalkTopDown(func(v zfs.DatasetPathVisit) bool {
		if !v.FilledIn {
			return true
		}
		exists, err := zfs.ZFSDatasetExists(v.Path.ToString())
		if err != nil {
			log.WithError(err).
				WithField("intermed_fs", v.Path).
				Error("error cheching if intermediate filesystem exists")
			goto errout
		}
		if !exists {
			err := zfs.ZFSCreatePlaceholderFilesystem(v.Path)
			if err != nil {
				log.WithField("intermed_fs", v.Path).Error("error creating placeholder filesystem")
				goto errout
			}
		}
		return true
	errout:
		log.Warn("aborting receive because we cannot create parent placeholder")
		return false
	})

	// FIXME unify with receive path in replication.go
	// always receive without mounting, since this could corrupt the receiving mountpoint hierechy
	recvArgs := []string{"-u"}
	if req.Header.Rollback {
		log.Info("receive with forced rollback")
		recvArgs = append(recvArgs, "-F")
	}

	progressStream := h.task.ProgressUpdater(req.Stream)
	if err := zfs.ZFSRecv(localFS, progressStream, recvArgs...); err != nil {
		log.WithError(err).Error("cannot receive stream")
		return err
	}
	return nil
}

func (h *SinkHandler) pullACLCheck(p *zfs.DatasetPath, v *zfs.FilesystemVersion) (err error) {
	var fsAllowed, vAllowed bool
	fsAllowed, err = h.fsMapping.Filter(p)
	if err != nil {
		err = fmt.Errorf("error evaluating ACL: %s", err)
		return
	}
	if !fsAllowed {
		err = fmt.Errorf("ACL prohibits access to %s", p.ToString())
		return
	}
	if v == nil {
		return
	}

	vAllowed, err = h.versionFilter.Filter(*v)
	if err != nil {
		err = errors.Wrap(err, "error evaluating version filter")
		return
	}
	if !vAllowed {
		err = fmt.Errorf("ACL prohibits access to %s", v.ToAbsPath(p))
		return
	}
	return
}
