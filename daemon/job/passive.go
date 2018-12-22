package job

import (
	"context"
	"fmt"
	"github.com/zrepl/zrepl/transport"
	"github.com/zrepl/zrepl/transport/fromconfig"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/zrepl/zrepl/config"
	"github.com/zrepl/zrepl/daemon/filters"
	"github.com/zrepl/zrepl/daemon/logging"
	"github.com/zrepl/zrepl/daemon/snapper"
	"github.com/zrepl/zrepl/rpc"
	"github.com/zrepl/zrepl/endpoint"
	"github.com/zrepl/zrepl/zfs"
)

type PassiveSide struct {
	mode   passiveMode
	name   string
	listen transport.AuthenticatedListenerFactory

	validatedServerConfig rpc.ServerConfig
}

type passiveMode interface {
	Handler() rpc.Handler
	RunPeriodic(ctx context.Context)
	Type() Type
}

type modeSink struct {
	rootDataset *zfs.DatasetPath
}

func (m *modeSink) Type() Type { return TypeSink }

func (m *modeSink) Handler() rpc.Handler {
	return endpoint.NewReceiver(m.rootDataset, true)
}

func (m *modeSink) RunPeriodic(_ context.Context) {}

func modeSinkFromConfig(g *config.Global, in *config.SinkJob) (m *modeSink, err error) {
	m = &modeSink{}
	m.rootDataset, err = zfs.NewDatasetPath(in.RootFS)
	if err != nil {
		return nil, errors.New("root dataset is not a valid zfs filesystem path")
	}
	if m.rootDataset.Length() <= 0 {
		return nil, errors.New("root dataset must not be empty") // duplicates error check of receiver
	}
	return m, nil
}

type modeSource struct {
	fsfilter zfs.DatasetFilter
	snapper *snapper.PeriodicOrManual
}

func modeSourceFromConfig(g *config.Global, in *config.SourceJob) (m *modeSource, err error) {
	// FIXME exact dedup of modePush
	m = &modeSource{}
	fsf, err := filters.DatasetMapFilterFromConfig(in.Filesystems)
	if err != nil {
		return nil, errors.Wrap(err, "cannnot build filesystem filter")
	}
	m.fsfilter = fsf

	if m.snapper, err = snapper.FromConfig(g, fsf, in.Snapshotting); err != nil {
		return nil, errors.Wrap(err, "cannot build snapper")
	}

	return m, nil
}

func (m *modeSource) Type() Type { return TypeSource }

func (m *modeSource) Handler() rpc.Handler {
	return endpoint.NewSender(m.fsfilter)
}

func (m *modeSource) RunPeriodic(ctx context.Context) {
	m.snapper.Run(ctx, nil)
}

func passiveSideFromConfig(g *config.Global, in *config.PassiveJob, mode passiveMode) (s *PassiveSide, err error) {

	s = &PassiveSide{mode: mode, name: in.Name}
	if s.listen, err = fromconfig.ListenerFactoryFromConfig(g, in.Serve); err != nil {
		return nil, errors.Wrap(err, "cannot build listener factory")
	}

	if err := s.validatedServerConfig.FromConfig(g, in.RPC); err != nil {
		return nil, errors.Wrap(err, "invalid rpc config")
	}

	return s, nil
}

func (j *PassiveSide) Name() string { return j.name }

type PassiveStatus struct{}

func (s *PassiveSide) Status() *Status {
	return &Status{Type: s.mode.Type()} // FIXME PassiveStatus
}

func (*PassiveSide) RegisterMetrics(registerer prometheus.Registerer) {}

func (j *PassiveSide) Run(ctx context.Context) {

	log := GetLogger(ctx)
	defer log.Info("job exiting")

	{
		ctx := logging.WithSubsystemLoggers(ctx, log) // shadowing
		ctx, cancel := context.WithCancel(ctx)        // shadowing
		defer cancel()
		go j.mode.RunPeriodic(ctx)
	}

	handler := j.mode.Handler()
	if handler == nil {
		panic(fmt.Sprintf("implementation error: j.mode.Handler() returned nil: %#v", j))
	}

	rpcLog := logging.LogSubsystem(log, logging.SubsysRPC)
	ctxInterceptor := func(handlerCtx context.Context) context.Context {
		return logging.WithSubsystemLoggers(handlerCtx, log)
	}
	server := rpc.NewServer(j.validatedServerConfig, handler, rpcLog, ctxInterceptor)

	listener, err := j.listen()
	if err != nil {
		log.WithError(err).Error("cannot listen")
		return
	}

	server.Serve(ctx, listener)
}
