package daemon

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/pprof" //nolint:gosec
	"os"
	"os/signal"
	runtimePprof "runtime/pprof"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/stellar/go/clients/stellarcore"
	"github.com/stellar/go/historyarchive"
	"github.com/stellar/go/ingest/ledgerbackend"
	supporthttp "github.com/stellar/go/support/http"
	supportlog "github.com/stellar/go/support/log"
	"github.com/stellar/go/support/ordered"
	"github.com/stellar/go/support/storage"
	"github.com/stellar/go/xdr"

	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal"
	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/config"
	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/db"
	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/events"
	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/feewindow"
	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/ingest"
	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/ledgerbucketwindow"
	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/preflight"
	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/util"
)

const (
	prometheusNamespace                   = "soroban_rpc"
	maxLedgerEntryWriteBatchSize          = 150
	defaultReadTimeout                    = 5 * time.Second
	defaultShutdownGracePeriod            = 10 * time.Second
	inMemoryInitializationLedgerLogPeriod = 1_000_000
)

type Daemon struct {
	core                *ledgerbackend.CaptiveStellarCore
	coreClient          *CoreClientWithMetrics
	ingestService       *ingest.Service
	db                  *db.DB
	jsonRPCHandler      *internal.Handler
	logger              *supportlog.Entry
	preflightWorkerPool *preflight.PreflightWorkerPool
	listener            net.Listener
	server              *http.Server
	adminListener       net.Listener
	adminServer         *http.Server
	closeOnce           sync.Once
	closeError          error
	done                chan struct{}
	metricsRegistry     *prometheus.Registry
}

func (d *Daemon) GetDB() *db.DB {
	return d.db
}

func (d *Daemon) GetEndpointAddrs() (net.TCPAddr, *net.TCPAddr) {
	var addr = d.listener.Addr().(*net.TCPAddr)
	var adminAddr *net.TCPAddr
	if d.adminListener != nil {
		adminAddr = d.adminListener.Addr().(*net.TCPAddr)
	}
	return *addr, adminAddr
}

func (d *Daemon) close() {
	shutdownCtx, shutdownRelease := context.WithTimeout(context.Background(), defaultShutdownGracePeriod)
	defer shutdownRelease()
	var closeErrors []error

	if err := d.server.Shutdown(shutdownCtx); err != nil {
		d.logger.WithError(err).Error("error during Soroban JSON RPC server Shutdown")
		closeErrors = append(closeErrors, err)
	}
	if d.adminServer != nil {
		if err := d.adminServer.Shutdown(shutdownCtx); err != nil {
			d.logger.WithError(err).Error("error during Soroban JSON admin server Shutdown")
			closeErrors = append(closeErrors, err)
		}
	}

	if err := d.ingestService.Close(); err != nil {
		d.logger.WithError(err).Error("error closing ingestion service")
		closeErrors = append(closeErrors, err)
	}
	if err := d.core.Close(); err != nil {
		d.logger.WithError(err).Error("error closing captive core")
		closeErrors = append(closeErrors, err)
	}
	d.jsonRPCHandler.Close()
	if err := d.db.Close(); err != nil {
		d.logger.WithError(err).Error("Error closing db")
		closeErrors = append(closeErrors, err)
	}
	d.preflightWorkerPool.Close()
	d.closeError = errors.Join(closeErrors...)
	close(d.done)
}

func (d *Daemon) Close() error {
	d.closeOnce.Do(d.close)
	return d.closeError
}

// newCaptiveCore creates a new captive core backend instance and returns it.
func newCaptiveCore(cfg *config.Config, logger *supportlog.Entry) (*ledgerbackend.CaptiveStellarCore, error) {
	captiveCoreTomlParams := ledgerbackend.CaptiveCoreTomlParams{
		HTTPPort:                           &cfg.CaptiveCoreHTTPPort,
		HistoryArchiveURLs:                 cfg.HistoryArchiveURLs,
		NetworkPassphrase:                  cfg.NetworkPassphrase,
		Strict:                             true,
		UseDB:                              true,
		EnforceSorobanDiagnosticEvents:     true,
		EnforceSorobanTransactionMetaExtV1: true,
		CoreBinaryPath:                     cfg.StellarCoreBinaryPath,
	}
	captiveCoreToml, err := ledgerbackend.NewCaptiveCoreTomlFromFile(cfg.CaptiveCoreConfigPath, captiveCoreTomlParams)
	if err != nil {
		logger.WithError(err).Fatal("Invalid captive core toml")
	}

	captiveConfig := ledgerbackend.CaptiveCoreConfig{
		BinaryPath:          cfg.StellarCoreBinaryPath,
		StoragePath:         cfg.CaptiveCoreStoragePath,
		NetworkPassphrase:   cfg.NetworkPassphrase,
		HistoryArchiveURLs:  cfg.HistoryArchiveURLs,
		CheckpointFrequency: cfg.CheckpointFrequency,
		Log:                 logger.WithField("subservice", "stellar-core"),
		Toml:                captiveCoreToml,
		UserAgent:           cfg.ExtendedUserAgent("captivecore"),
		UseDB:               true,
	}
	return ledgerbackend.NewCaptive(captiveConfig)

}

func MustNew(cfg *config.Config, logger *supportlog.Entry) *Daemon {
	logger.SetLevel(cfg.LogLevel)
	if cfg.LogFormat == config.LogFormatJSON {
		logger.UseJSONFormatter()
	}

	logger.WithFields(supportlog.F{
		"version": config.Version,
		"commit":  config.CommitHash,
	}).Info("starting Soroban RPC")

	core, err := newCaptiveCore(cfg, logger)
	if err != nil {
		logger.WithError(err).Fatal("could not create captive core")
	}

	if len(cfg.HistoryArchiveURLs) == 0 {
		logger.Fatal("no history archives URLs were provided")
	}

	historyArchive, err := historyarchive.NewArchivePool(
		cfg.HistoryArchiveURLs,
		historyarchive.ArchiveOptions{
			Logger:              logger,
			NetworkPassphrase:   cfg.NetworkPassphrase,
			CheckpointFrequency: cfg.CheckpointFrequency,
			ConnectOptions: storage.ConnectOptions{
				Context:   context.Background(),
				UserAgent: cfg.HistoryArchiveUserAgent},
		},
	)

	if err != nil {
		logger.WithError(err).Fatal("could not connect to history archive")
	}

	metricsRegistry := prometheus.NewRegistry()
	dbConn, err := db.OpenSQLiteDBWithPrometheusMetrics(cfg.SQLiteDBPath, prometheusNamespace, "db", metricsRegistry)
	if err != nil {
		logger.WithError(err).Fatal("could not open database")
	}

	daemon := &Daemon{
		logger:          logger,
		core:            core,
		db:              dbConn,
		done:            make(chan struct{}),
		metricsRegistry: metricsRegistry,
		coreClient: newCoreClientWithMetrics(stellarcore.Client{
			URL:  cfg.StellarCoreURL,
			HTTP: &http.Client{Timeout: cfg.CoreRequestTimeout},
		}, metricsRegistry),
	}

	feewindows, eventStore := daemon.mustInitializeStorage(cfg)

	onIngestionRetry := func(err error, dur time.Duration) {
		logger.WithError(err).Error("could not run ingestion. Retrying")
	}

	// Take the larger of (event retention, tx retention) and then the smaller
	// of (tx retention, default event retention) if event retention wasn't
	// specified, for some reason...?
	maxRetentionWindow := ordered.Max(cfg.EventLedgerRetentionWindow, cfg.TransactionLedgerRetentionWindow)
	if cfg.EventLedgerRetentionWindow <= 0 {
		maxRetentionWindow = ordered.Min(
			maxRetentionWindow,
			ledgerbucketwindow.DefaultEventLedgerRetentionWindow)
	}
	ingestService := ingest.NewService(ingest.Config{
		Logger: logger,
		DB: db.NewReadWriter(
			logger,
			dbConn,
			daemon,
			maxLedgerEntryWriteBatchSize,
			maxRetentionWindow,
			cfg.NetworkPassphrase,
		),
		EventStore:        eventStore,
		NetworkPassPhrase: cfg.NetworkPassphrase,
		Archive:           historyArchive,
		LedgerBackend:     core,
		Timeout:           cfg.IngestionTimeout,
		OnIngestionRetry:  onIngestionRetry,
		Daemon:            daemon,
		FeeWindows:        feewindows,
	})

	ledgerEntryReader := db.NewLedgerEntryReader(dbConn)
	preflightWorkerPool := preflight.NewPreflightWorkerPool(
		daemon,
		cfg.PreflightWorkerCount,
		cfg.PreflightWorkerQueueSize,
		cfg.PreflightEnableDebug,
		ledgerEntryReader,
		cfg.NetworkPassphrase,
		logger,
	)

	jsonRPCHandler := internal.NewJSONRPCHandler(cfg, internal.HandlerParams{
		Daemon:            daemon,
		EventStore:        eventStore,
		FeeStatWindows:    feewindows,
		Logger:            logger,
		LedgerReader:      db.NewLedgerReader(dbConn),
		LedgerEntryReader: db.NewLedgerEntryReader(dbConn),
		TransactionReader: db.NewTransactionReader(logger, dbConn, cfg.NetworkPassphrase),
		PreflightGetter:   preflightWorkerPool,
	})

	httpHandler := supporthttp.NewAPIMux(logger)
	httpHandler.Handle("/", jsonRPCHandler)

	daemon.preflightWorkerPool = preflightWorkerPool
	daemon.ingestService = ingestService
	daemon.jsonRPCHandler = &jsonRPCHandler

	// Use a separate listener in order to obtain the actual TCP port
	// when using dynamic ports during testing (e.g. endpoint="localhost:0")
	daemon.listener, err = net.Listen("tcp", cfg.Endpoint)
	if err != nil {
		daemon.logger.WithError(err).WithField("endpoint", cfg.Endpoint).Fatal("cannot listen on endpoint")
	}
	daemon.server = &http.Server{
		Handler:     httpHandler,
		ReadTimeout: defaultReadTimeout,
	}
	if cfg.AdminEndpoint != "" {
		adminMux := supporthttp.NewMux(logger)
		adminMux.HandleFunc("/debug/pprof/", pprof.Index)
		adminMux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		adminMux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		adminMux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		adminMux.HandleFunc("/debug/pprof/trace", pprof.Trace)
		// add the entry points for:
		// goroutine, threadcreate, heap, allocs, block, mutex
		for _, profile := range runtimePprof.Profiles() {
			adminMux.Handle("/debug/pprof/"+profile.Name(), pprof.Handler(profile.Name()))
		}
		adminMux.Handle("/metrics", promhttp.HandlerFor(metricsRegistry, promhttp.HandlerOpts{}))
		daemon.adminListener, err = net.Listen("tcp", cfg.AdminEndpoint)
		if err != nil {
			daemon.logger.WithError(err).WithField("endpoint", cfg.Endpoint).Fatal("cannot listen on admin endpoint")
		}
		daemon.adminServer = &http.Server{Handler: adminMux}
	}
	daemon.registerMetrics()
	return daemon
}

// mustInitializeStorage initializes the storage using what was on the DB
func (d *Daemon) mustInitializeStorage(cfg *config.Config) (*feewindow.FeeWindows, *events.MemoryStore) {
	eventStore := events.NewMemoryStore(
		d,
		cfg.NetworkPassphrase,
		cfg.EventLedgerRetentionWindow,
	)
	feewindows := feewindow.NewFeeWindows(cfg.ClassicFeeStatsLedgerRetentionWindow, cfg.SorobanFeeStatsLedgerRetentionWindow, cfg.NetworkPassphrase)

	readTxMetaCtx, cancelReadTxMeta := context.WithTimeout(context.Background(), cfg.IngestionTimeout)
	defer cancelReadTxMeta()
	var initialSeq uint32
	var currentSeq uint32
	dataMigrations, err := db.BuildMigrations(readTxMetaCtx, d.logger, d.db, cfg)
	if err != nil {
		d.logger.WithError(err).Fatal("could not build migrations")
	}
	// NOTE: We could optimize this to avoid unnecessary ingestion calls
	//       (the range of txmetas can be larger than the individual store retention windows)
	//       but it's probably not worth the pain.
	err = db.NewLedgerReader(d.db).StreamAllLedgers(readTxMetaCtx, func(txmeta xdr.LedgerCloseMeta) error {
		currentSeq = txmeta.LedgerSequence()
		if initialSeq == 0 {
			initialSeq = currentSeq
			d.logger.WithFields(supportlog.F{
				"seq": currentSeq,
			}).Info("initializing in-memory store")
		} else if (currentSeq-initialSeq)%inMemoryInitializationLedgerLogPeriod == 0 {
			d.logger.WithFields(supportlog.F{
				"seq": currentSeq,
			}).Debug("still initializing in-memory store")
		}
		if err := eventStore.IngestEvents(txmeta); err != nil {
			d.logger.WithError(err).Fatal("could not initialize event memory store")
		}
		if err := feewindows.IngestFees(txmeta); err != nil {
			d.logger.WithError(err).Fatal("could not initialize fee stats")
		}
		// TODO: clean up once we remove the in-memory storage.
		//       (we should only stream over the required range)
		if r := dataMigrations.ApplicableRange(); r.IsLedgerIncluded(currentSeq) {
			if err := dataMigrations.Apply(readTxMetaCtx, txmeta); err != nil {
				d.logger.WithError(err).Fatal("could not run migrations")
			}
		}
		return nil
	})
	if err != nil {
		d.logger.WithError(err).Fatal("could not obtain txmeta cache from the database")
	}
	if err := dataMigrations.Commit(readTxMetaCtx); err != nil {
		d.logger.WithError(err).Fatal("could not commit data migrations")
	}

	if currentSeq != 0 {
		d.logger.WithFields(supportlog.F{
			"seq": currentSeq,
		}).Info("finished initializing in-memory store")
	}

	return feewindows, eventStore
}

func (d *Daemon) Run() {
	d.logger.WithFields(supportlog.F{
		"addr": d.listener.Addr().String(),
	}).Info("starting HTTP server")

	panicGroup := util.UnrecoverablePanicGroup.Log(d.logger)
	panicGroup.Go(func() {
		if err := d.server.Serve(d.listener); !errors.Is(err, http.ErrServerClosed) {
			d.logger.WithError(err).Fatal("soroban JSON RPC server encountered fatal error")
		}
	})

	if d.adminServer != nil {
		d.logger.WithFields(supportlog.F{
			"addr": d.adminListener.Addr().String(),
		}).Info("starting Admin HTTP server")
		panicGroup.Go(func() {
			if err := d.adminServer.Serve(d.adminListener); !errors.Is(err, http.ErrServerClosed) {
				d.logger.WithError(err).Error("soroban admin server encountered fatal error")
			}
		})
	}

	// Shutdown gracefully when we receive an interrupt signal.
	// First server.Shutdown closes all open listeners, then closes all idle connections.
	// Finally, it waits a grace period (10s here) for connections to return to idle and then shut down.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-signals:
		d.Close()
	case <-d.done:
		return
	}
}
