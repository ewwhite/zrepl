package job

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/zrepl/zrepl/config"
	"github.com/zrepl/zrepl/daemon/filters"
	"github.com/zrepl/zrepl/daemon/logging"
	"github.com/zrepl/zrepl/daemon/snapper"
	"github.com/zrepl/zrepl/daemon/transport/serve"
	"github.com/zrepl/zrepl/daemon/transport/transporthttpinjector"
	"github.com/zrepl/zrepl/endpoint"
	"github.com/zrepl/zrepl/endpoint/tokenstore"
	"github.com/zrepl/zrepl/zfs"
	"net/http"
	"sync/atomic"
)

type PassiveSide struct {
	mode passiveMode
	name     string
	listen   serve.AuthenticatedListenerFactory

	tokenStore *tokenstore.Store
	tokenStoreStop tokenstore.StopExpirationFunc

	validatedHandlerConfig endpoint.HttpHandlerConfig
}

type passiveMode interface {
	Handler(handlerConfig endpoint.HttpHandlerConfig, tokenStore endpoint.TokenStore) http.Handler
	RunPeriodic(ctx context.Context)
	Type() Type
}

type modeSink struct {
	tokenStore endpoint.TokenStore
	rootDataset *zfs.DatasetPath
}

func (m *modeSink) Type() Type { return TypeSink }

func (m *modeSink) Handler(handlerConfig endpoint.HttpHandlerConfig, tokenStore endpoint.TokenStore) http.Handler {
	local :=  endpoint.NewReceiver(m.rootDataset, tokenStore)
	return endpoint.ToHandler(local, handlerConfig)
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

func (m *modeSource) Handler(handlerConfig endpoint.HttpHandlerConfig, tokenStore endpoint.TokenStore) http.Handler {
	sender := endpoint.NewSender(m.fsfilter, tokenStore)
	return endpoint.ToHandler(sender, handlerConfig)
}

func (m *modeSource) RunPeriodic(ctx context.Context) {
	m.snapper.Run(ctx, nil)
}

func passiveSideFromConfig(g *config.Global, in *config.PassiveJob, mode passiveMode) (s *PassiveSide, err error) {

	s = &PassiveSide{mode: mode, name: in.Name}
	if s.listen, err = serve.FromConfig(g, in.Serve); err != nil {
		return nil, errors.Wrap(err, "cannot build listener factory")
	}

	if err := s.validatedHandlerConfig.FromConfig(g, in.RPC); err != nil {
		return nil, errors.Wrap(err, "invalid rpc config")
	}

	s.tokenStore, s.tokenStoreStop, err = tokenstore.NewWithRandomKey()
	if err != nil {
		return nil, errors.Wrap(err, "cannot build token store")
	}

	return s, nil
}

func (j *PassiveSide) Name() string { return j.name }

type PassiveStatus struct {}

func (s *PassiveSide) Status() *Status {
	return &Status{Type: s.mode.Type()} // FIXME PassiveStatus
}

func (*PassiveSide) RegisterMetrics(registerer prometheus.Registerer) {}

func (j *PassiveSide) Run(ctx context.Context) {

	log := GetLogger(ctx)
	defer log.Info("job exiting")

	defer j.tokenStoreStop()

	{
		ctx := logging.WithSubsystemLoggers(ctx, log) // shadowing
		ctx, cancel := context.WithCancel(ctx) // shadowing
		defer cancel()
		go j.mode.RunPeriodic(ctx)
	}

	handler := j.mode.Handler(j.validatedHandlerConfig, j.tokenStore)
	if handler == nil {
		panic(fmt.Sprintf("implementation error: j.mode.Handler() returned nil: %#v", j))
	}

	listener, err := j.listen()
	if err != nil {
		log.WithError(err).Error("cannot listen")
		return
	}
	defer listener.Close()

	// FIXME hacky
	var connId uint64
	requestLogger := http.NewServeMux()
	requestLogger.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {
		connId := atomic.AddUint64(&connId, 1)
		log := log.WithField("connID", connId) // shadow
		log.WithField("uri", request.RequestURI).Debug("handling request")
		request = request.WithContext(logging.WithSubsystemLoggers(request.Context(), log))
		handler.ServeHTTP(writer, request)
		log.WithField("uri", request.RequestURI).Debug("finished request")
	})

	server := transporthttpinjector.NewServer(listener, requestLogger)
	serveCtx := logging.WithSubsystemLoggers(ctx, log)
	if err := server.Serve(serveCtx); err != nil {
		if err != context.Canceled {
			log.WithError(err).WithField("errType", fmt.Sprintf("%T", err)).Error("error serving")
		}
	}
}
