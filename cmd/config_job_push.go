package cmd

import (
	"context"
	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"
	"github.com/zrepl/zrepl/rpc"
	"github.com/zrepl/zrepl/util"
	"time"
)

type PushJob struct {
	Name              string
	Connect           RWCConnecter
	Interval          time.Duration
	Filesystems       *DatasetMapFilter
	InitialReplPolicy InitialReplPolicy
	SnapshotPrefix    string
	Debug             JobDebugSettings
	Prune             PrunePolicy
	task              *Task
}

func parsePushJob(c JobParsingContext, name string, i map[string]interface{}) (j *PushJob, err error) {

	var asMap struct {
		Connect           map[string]interface{}
		Interval          string
		Filesystems       map[string]string
		InitialReplPolicy string `mapstructure:"initial_repl_policy"`
		SnapshotPrefix    string `mapstructure:"snapshot_prefix"`
		Prune             map[string]interface{}
		Debug             map[string]interface{}
	}

	if err = mapstructure.Decode(i, &asMap); err != nil {
		err = errors.Wrap(err, "mapstructure error")
		return nil, err
	}

	j = &PushJob{Name: name}

	j.Connect, err = parseSSHStdinserverConnecter(asMap.Connect)
	if err != nil {
		err = errors.Wrap(err, "cannot parse 'connect'")
		return nil, err
	}

	if j.Interval, err = parsePostitiveDuration(asMap.Interval); err != nil {
		err = errors.Wrap(err, "cannot parse 'interval'")
		return nil, err
	}

	j.Filesystems, err = parseDatasetMapFilter(asMap.Filesystems, true)
	if err != nil {
		err = errors.Wrap(err, "cannot parse 'filesystems'")
		return nil, err
	}

	j.InitialReplPolicy, err = parseInitialReplPolicy(asMap.InitialReplPolicy, DEFAULT_INITIAL_REPL_POLICY)
	if err != nil {
		err = errors.Wrap(err, "cannot parse 'initial_repl_policy'")
		return
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

func (j *PushJob) JobName() string {
	return j.Name
}

func (j *PushJob) JobStart(ctx context.Context) {
	log := ctx.Value(contextKeyLog).(Logger)
	defer log.Info("exiting")

	j.task = NewTask("push", log)

	// TODO autosnap, prune

	ticker := time.NewTicker(j.Interval)
	for {
		j.doRun(ctx)
		select {
		case <-ctx.Done():
			j.task.Log().WithError(ctx.Err()).Info("context")
			return
		case <-ticker.C:
		}
	}

}

func (j *PushJob) doRun(ctx context.Context) {
	j.task.Enter("run")
	defer j.task.Finish()

	j.task.Log().Info("connecting")
	rwc, err := j.Connect.Connect()
	if err != nil {
		j.task.Log().WithError(err).Error("error connecting")
		return
	}

	rwc, err = util.NewReadWriteCloserLogger(rwc, j.Debug.Conn.ReadDump, j.Debug.Conn.WriteDump)
	if err != nil {
		return
	}

	client := rpc.NewClient(rwc)
	if j.Debug.RPC.Log {
		client.SetLogger(j.task.Log(), true)
	}

	j.task.Log().Debug("start pusher")
	pusher := Pusher{
		pusherRemote{commonRemote{client}},
		j.Filesystems,
		NewPrefixFilter(j.SnapshotPrefix),
		j.InitialReplPolicy,
	}
	pusher.Push(j.task)

	closeRPCWithTimeout(j.task, client, time.Second*10, "")

}

func (j *PushJob) JobStatus(ctxt context.Context) (*JobStatus, error) {
	return &JobStatus{
		Tasks: []*TaskStatus{j.task.Status()},
	}, nil
}
