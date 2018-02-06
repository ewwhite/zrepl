package cmd

import (
	"fmt"
	"io"

	"bytes"
	"encoding/json"
	"github.com/pkg/errors"
	"github.com/zrepl/zrepl/rpc"
	"github.com/zrepl/zrepl/zfs"
	"strings"
)

type localPullACL struct{}

func (a localPullACL) Filter(p *zfs.DatasetPath) (pass bool, err error) {
	return true, nil
}

const DEFAULT_INITIAL_REPL_POLICY = InitialReplPolicyMostRecent

type InitialReplPolicy string

const (
	InitialReplPolicyMostRecent InitialReplPolicy = "most_recent"
	InitialReplPolicyAll        InitialReplPolicy = "all"
)

type Puller struct {
	task              *Task
	Remote            pullerRemote
	Mapping           DatasetMapping
	InitialReplPolicy InitialReplPolicy
}

func (p *Puller) Receive(sourcePath *zfs.DatasetPath, sendStream io.Reader, rollback bool) error {

	recvFS, err := p.Mapping.Map(sourcePath)
	if err != nil {
		p.task.Log().WithField("source", sourcePath.ToString()).WithError(err).Error("cannot map source path")
		return err
	}

	progressStream := p.task.ProgressUpdater(sendStream)

	// always receive without mounting, since this could corrupt the receiving mountpoint hierechy
	recvArgs := []string{"-u"}
	if rollback {
		p.task.Log().Info("receive with forced rollback to replace placeholder filesystem")
		recvArgs = append(recvArgs, "-F")
	}

	if err := zfs.ZFSRecv(recvFS, progressStream, recvArgs...); err != nil {
		p.task.Log().WithError(err).Error("cannot receive stream")
		return err
	}
	return nil
}

type pullerRemote struct {
	rpc.RPCClient
}

func (r pullerRemote) Send(sourcePath *zfs.DatasetPath, from, to *zfs.FilesystemVersion) (io.Reader, error) {
	var stream io.Reader
	if to == nil {
		req := InitialTransferRequest{
			Filesystem:        sourcePath,
			FilesystemVersion: *from,
		}
		if err := r.Call("InitialTransferRequest", &req, &stream); err != nil {
			return nil, err
		}
	} else {
		req := IncrementalTransferRequest{
			Filesystem: sourcePath,
			From:       *from,
			To:         *to,
		}
		if err := r.Call("IncrementalTransferRequest", &req, &stream); err != nil {
			return nil, err
		}
	}
	return stream, nil
}

type remoteLocalMapping struct {
	Remote *zfs.DatasetPath
	Local  *zfs.DatasetPath
}

func (p *Puller) getRemoteFilesystems() (rfs []*zfs.DatasetPath, ok bool) {
	p.task.Enter("fetch_remote_fs_list")
	defer p.task.Finish()

	fsr := FilesystemRequest{}
	if err := p.Remote.Call("FilesystemRequest", &fsr, &rfs); err != nil {
		p.task.Log().WithError(err).Error("cannot fetch remote filesystem list")
		return nil, false
	}
	return rfs, true
}

func (p *Puller) buildReplMapping(remoteFilesystems []*zfs.DatasetPath) (replMapping map[string]remoteLocalMapping, ok bool) {
	p.task.Enter("build_repl_mapping")
	defer p.task.Finish()

	replMapping = make(map[string]remoteLocalMapping, len(remoteFilesystems))
	for fs := range remoteFilesystems {
		var err error
		var localFs *zfs.DatasetPath
		localFs, err = p.Mapping.Map(remoteFilesystems[fs])
		if err != nil {
			err := fmt.Errorf("error mapping %s: %s", remoteFilesystems[fs], err)
			p.task.Log().WithError(err).WithField(logMapFromField, remoteFilesystems[fs]).Error("cannot map")
			return nil, false
		}
		if localFs == nil {
			continue
		}
		p.task.Log().WithField(logMapFromField, remoteFilesystems[fs].ToString()).
			WithField(logMapToField, localFs.ToString()).Debug("mapping")
		m := remoteLocalMapping{remoteFilesystems[fs], localFs}
		replMapping[m.Local.ToString()] = m
	}
	return replMapping, true
}

func ResolveDiff(initialReplPolicy InitialReplPolicy, diff zfs.FilesystemDiff) *DiffResolution {

	switch diff.Conflict {
	case zfs.ConflictAllRight:

		if initialReplPolicy != InitialReplPolicyMostRecent {
			return &DiffResolution{
				NeedsAction: true,
				Error: &DiffResoltionError{
					Problem: fmt.Sprintf("policy '%s' not implemented", initialReplPolicy),
				},
			}
		}

		snapsOnly := make([]zfs.FilesystemVersion, 0, len(diff.MRCAPathRight))
		for s := range diff.MRCAPathRight {
			if diff.MRCAPathRight[s].Type == zfs.Snapshot {
				snapsOnly = append(snapsOnly, diff.MRCAPathRight[s])
			}
		}

		if len(snapsOnly) < 1 {
			return &DiffResolution{
				NeedsAction: true,
				Error: &DiffResoltionError{
					Problem: "source has no remote snapshots",
				},
			}
		}

		version := snapsOnly[len(snapsOnly)-1]
		return &DiffResolution{
			NeedsAction:                  true,
			FirstElementNeedsReplication: true,
			PullList:                     []zfs.FilesystemVersion{version},
		}

	case zfs.ConflictIncremental:

		if len(diff.IncrementalPath) < 2 {
			return &DiffResolution{
				NeedsAction: false,
			}
		}

		return &DiffResolution{
			NeedsAction:                  true,
			FirstElementNeedsReplication: false,
			PullList:                     diff.IncrementalPath,
		}

	case zfs.ConflictNoCommonAncestor:
		fallthrough
	case zfs.ConflictDiverged:

		var error *DiffResoltionError

		switch diff.Conflict {
		case zfs.ConflictNoCommonAncestor:
			error = &DiffResoltionError{
				Problem:    "remote and local filesystem have snapshots, but no common one",
				Resolution: "perform manual establish a common snapshot history",
			}
		case zfs.ConflictDiverged:
			error = &DiffResoltionError{
				Problem: "remote and local filesystem share a history but have diverged",
				Resolution: "perform manual replication or delete snapshots on the receiving" +
					"side  to establish an incremental replication parse",
			}
		default:
			// must cover all cases that match in the outer switch
			panic("implementation error")
		}

		return &DiffResolution{
			NeedsAction: true,
			Error:       error,
		}

	}

	return &DiffResolution{
		NeedsAction: true,
		Error: &DiffResoltionError{
			Problem:    "unknown conflict type",
			Resolution: fmt.Sprintf("implement support for config type %s", diff.Conflict),
		},
	}

}

func (p *Puller) diffFilesystem(m remoteLocalMapping, localFilesystemState map[string]zfs.FilesystemState) (diff zfs.FilesystemDiff, err error) {

	log := p.task.Log().
		WithField(logMapFromField, m.Remote.ToString()).
		WithField(logMapToField, m.Local.ToString())

	log.Debug("examining local filesystem state")
	localState, localExists := localFilesystemState[m.Local.ToString()]
	var versions []zfs.FilesystemVersion
	switch {
	case !localExists:
		log.Info("local filesystem does not exist")
	case localState.Placeholder:
		log.Info("local filesystem is marked as placeholder")
	default:
		log.Debug("local filesystem exists")
		log.Debug("requesting local filesystem versions")
		if versions, err = zfs.ZFSListFilesystemVersions(m.Local, nil); err != nil {
			return zfs.FilesystemDiff{}, errors.Wrap(err, "cannot get local filesytem versions")
		}
	}

	log.Info("requesting remote filesystem versions")
	r := FilesystemVersionsRequest{
		Filesystem: m.Remote,
	}
	var theirVersions []zfs.FilesystemVersion
	if err = p.Remote.Call("FilesystemVersionsRequest", &r, &theirVersions); err != nil {
		return zfs.FilesystemDiff{}, errors.Wrap(err, "cannot get remote filesystem versions")
	}

	log.Debug("computing diff between remote and local filesystem versions")
	diff = zfs.MakeFilesystemDiff(versions, theirVersions)

	if localState.Placeholder && diff.Conflict != zfs.ConflictAllRight {
		panic("internal inconsistency: local placeholder implies ConflictAllRight")
	}

	return diff, nil

}

// returns true if the receiving filesystem (local side) exists and can have child filesystems
func (p *Puller) replFilesystem(m remoteLocalMapping, localFilesystemState map[string]zfs.FilesystemState) (localExists bool) {

	p.task.Enter("repl_fs")
	defer p.task.Finish()
	var err error

	diff, err := p.diffFilesystem(m, localFilesystemState)
	if err != nil {
		p.task.Log().WithError(err).Error("error diffing filesystems")
		return false
	}
	var jsonDiff bytes.Buffer
	if err := json.NewEncoder(&jsonDiff).Encode(diff); err != nil {
		p.task.Log().WithError(err).Error("cannot JSON-encode diff")
		return false
	}
	p.task.Log().WithField("diff", jsonDiff.String()).Debug("diff between local and remote filesystem")

	resolution := ResolveDiff(p.InitialReplPolicy, diff)
	if resolution.Error != nil {
		p.task.Log().WithError(resolution.Error).Error("error resolving filesystem diff")
		return false
	}
	var localState zfs.FilesystemState
	localState, localExists = localFilesystemState[m.Local.ToString()]
	resolution.RollbackReceiveFS = localState.Placeholder

	p.task.Log().Debug("pulling from remote")
	if hadError := resolution.Resolve(p.task, p, p.Remote, m.Remote); hadError {
		exists, err := zfs.ZFSDatasetExists(m.Local.ToString())
		if err != nil {
			p.task.Log().WithError(err).Error("cannot determine if pulled filesystem exists")
			p.task.Log().Warn("not setting properties, make sure they are ok")
			return false
		}
		localExists = exists
	} else {
		localExists = true
	}

	if localExists {
		p.task.Log().Debug("configuring properties of received filesystem")
		props := zfs.NewZFSProperties()
		props.Set("readonly", "on")
		props.Set("mountpoint", "none") // TODO provide configurable mounpoint mapping
		if err := zfs.ZFSSet(m.Local, props); err != nil {
			p.task.Log().WithError(err).Error("cannot set zfs properties")
		}
	}

	return localExists

}

func (p *Puller) Pull() {
	p.task.Enter("run")
	defer p.task.Finish()

	p.task.Log().Info("request remote filesystem list")
	remoteFilesystems, ok := p.getRemoteFilesystems()
	if !ok {
		return
	}

	p.task.Log().Debug("map remote filesystems to local paths and determine order for per-filesystem sync")
	replMapping, ok := p.buildReplMapping(remoteFilesystems)
	if !ok {

	}

	p.task.Log().Debug("build cache for already present local filesystem state")
	p.task.Enter("cache_local_fs_state")
	localFilesystemState, err := zfs.ZFSListFilesystemState()
	p.task.Finish()
	if err != nil {
		p.task.Log().WithError(err).Error("cannot request local filesystem state")
		return
	}

	localTraversal := zfs.NewDatasetPathForest()
	for _, m := range replMapping {
		localTraversal.Add(m.Local)
	}

	p.task.Log().Info("start per-filesystem sync")
	localTraversal.WalkTopDown(func(v zfs.DatasetPathVisit) bool {

		p.task.Enter("tree_walk")
		defer p.task.Finish()

		log := p.task.Log().WithField(logFSField, v.Path.ToString())

		if v.FilledIn {
			if _, exists := localFilesystemState[v.Path.ToString()]; exists {
				// No need to verify if this is a placeholder or not. It is sufficient
				// to know we can add child filesystems to it
				return true
			}
			log.Debug("create placeholder filesystem")
			p.task.Enter("create_placeholder")
			err = zfs.ZFSCreatePlaceholderFilesystem(v.Path)
			p.task.Finish()
			if err != nil {
				log.Error("cannot create placeholder filesystem")
				return false
			}
			return true
		}

		m, ok := replMapping[v.Path.ToString()]
		if !ok {
			panic("internal inconsistency: replMapping should contain mapping for any path that was not filled in by WalkTopDown()")
		}

		localExists := p.replFilesystem(m, localFilesystemState)
		if !localExists {
			log.Warn("stopping replication for all filesystems mapped as children of receiving filesystem")
		} else {
			log.Debug("continuing replication with child filesystems")
		}
		return localExists
	})

	return

}

type DiffResoltionError struct {
	Problem    string
	Resolution string
}

func (e *DiffResoltionError) Error() string {
	return strings.Join([]string{e.Problem, e.Resolution}, ". ")
}

type DiffResolution struct {
	NeedsAction                  bool
	Error                        *DiffResoltionError
	FirstElementNeedsReplication bool
	PullList                     []zfs.FilesystemVersion
	RollbackReceiveFS            bool
}

func (r *DiffResolution) consistencyCheck(task *Task) (hadError bool) {
	if r.PullList == nil || len(r.PullList) < 1 {
		task.Log().Error("resolution is inconsistent: PullList nil or len(PullList) == 0 but no Error")
		return true
	}
	if len(r.PullList) == 1 && !r.FirstElementNeedsReplication {
		task.Log().Error("resolution is inconsistent: len(PullList) == 1 but FirstElementNeedsReplication=false")
		return true
	}
	if len(r.PullList) > 1 && r.FirstElementNeedsReplication && len(r.PullList)%2 != 0 {
		task.Log().Error("resolution is inconsistent: len(PullList) % 2 must be 0 if pull list contains multiple entries")
		return true
	}
	return false
}

type Receiver interface {
	Receive(sourcePath *zfs.DatasetPath, sendStream io.Reader, rollback bool) error
}
type Sender interface {
	Send(sourcePath *zfs.DatasetPath, from, to *zfs.FilesystemVersion) (io.Reader, error)
}

func (r *DiffResolution) Resolve(task *Task, receiver Receiver, sender Sender, sourceFS *zfs.DatasetPath) (hadError bool) {

	if !r.NeedsAction {
		return false
	}

	if r.consistencyCheck(task) {
		return true
	}

	if r.FirstElementNeedsReplication {

		task.Log().Debug("first element needs replication")
		version := r.PullList[0]

		sendStream, err := sender.Send(sourceFS, &version, nil)
		if err != nil {
			task.Log().WithError(err).Error("cannot zfs send")
			return true
		}

		task.Log().WithField("version", version).Debug("requesting recv")

		err = receiver.Receive(sourceFS, sendStream, r.RollbackReceiveFS)
		if err != nil {
			task.Log().WithError(err).Error("error requesting recv")
			return true
		}

	}

	task.Log().Debug("follow PullList")

	for i := 0; i < len(r.PullList)-1; i++ {

		// safe because we asserted len(r.PullList) % 2 == 0
		from, to := r.PullList[i], r.PullList[i+1]

		sendStream, err := sender.Send(sourceFS, &from, &to)
		if err != nil {
			task.Log().WithError(err).Error("cannot zfs send incremental")
			return true
		}
		err = receiver.Receive(sourceFS, sendStream, r.RollbackReceiveFS)
		if err != nil {
			task.Log().WithError(err).Error("error requesting recv")
			return true
		}

	}

	return false
}
