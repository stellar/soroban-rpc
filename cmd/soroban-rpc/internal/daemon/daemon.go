package daemon

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/pprof"
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
	"github.com/stellar/go/support/storage"
	"github.com/stellar/go/xdr"

	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal"
	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/config"
	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/db"
	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/feewindow"
	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/ingest"
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
	preflightWorkerPool *preflight.WorkerPool
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
	//nolint:forcetypeassert
	addr := d.listener.Addr().(*net.TCPAddr)
	var adminAddr *net.TCPAddr
	if d.adminListener != nil {
		//nolint:forcetypeassert
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
				UserAgent: cfg.HistoryArchiveUserAgent,
			},
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

	feewindows := daemon.mustInitializeStorage(cfg)

	onIngestionRetry := func(err error, dur time.Duration) {
		logger.WithError(err).Error("could not run ingestion. Retrying")
	}

	ingestService := ingest.NewService(ingest.Config{
		Logger: logger,
		DB: db.NewReadWriter(
			logger,
			dbConn,
			daemon,
			maxLedgerEntryWriteBatchSize,
			cfg.HistoryRetentionWindow,
			cfg.NetworkPassphrase,
		),
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
		preflight.WorkerPoolConfig{
			Daemon:            daemon,
			WorkerCount:       cfg.PreflightWorkerCount,
			JobQueueCapacity:  cfg.PreflightWorkerQueueSize,
			EnableDebug:       cfg.PreflightEnableDebug,
			LedgerEntryReader: ledgerEntryReader,
			NetworkPassphrase: cfg.NetworkPassphrase,
			Logger:            logger,
		},
	)

	jsonRPCHandler := internal.NewJSONRPCHandler(cfg, internal.HandlerParams{
		Daemon:            daemon,
		FeeStatWindows:    feewindows,
		Logger:            logger,
		LedgerReader:      db.NewLedgerReader(dbConn),
		LedgerEntryReader: db.NewLedgerEntryReader(dbConn),
		TransactionReader: db.NewTransactionReader(logger, dbConn, cfg.NetworkPassphrase),
		EventReader:       db.NewEventReader(logger, dbConn, cfg.NetworkPassphrase),
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
func (d *Daemon) mustInitializeStorage(cfg *config.Config) *feewindow.FeeWindows {
	feeWindows := feewindow.NewFeeWindows(
		cfg.ClassicFeeStatsLedgerRetentionWindow,
		cfg.SorobanFeeStatsLedgerRetentionWindow,
		cfg.NetworkPassphrase,
		d.db,
	)

	readTxMetaCtx, cancelReadTxMeta := context.WithTimeout(context.Background(), cfg.IngestionTimeout)
	defer cancelReadTxMeta()

	var initialSeq, currentSeq uint32
	applicableRange, err := db.GetMigrationLedgerRange(readTxMetaCtx, d.db, cfg.HistoryRetentionWindow)
	if err != nil {
		d.logger.WithError(err).Fatal("could not get ledger range for migration")
	}

	maxFeeRetentionWindow := max(cfg.ClassicFeeStatsLedgerRetentionWindow, cfg.SorobanFeeStatsLedgerRetentionWindow)
	feeStatsRange, err := db.GetMigrationLedgerRange(readTxMetaCtx, d.db, maxFeeRetentionWindow)
	if err != nil {
		d.logger.WithError(err).Fatal("could not get ledger range for fee stats")
	}

	// Combine the ledger range for fees, events and transactions
	ledgerSeqRange := feeStatsRange.Merge(applicableRange)

	dataMigrations, err := db.BuildMigrations(readTxMetaCtx, d.logger, d.db, cfg.NetworkPassphrase, ledgerSeqRange)
	if err != nil {
		d.logger.WithError(err).Fatal("could not build migrations")
	}

	// Apply migration for events, transactions and fee stats
	err = db.NewLedgerReader(d.db).StreamLedgerRange(
		readTxMetaCtx,
		ledgerSeqRange.First,
		ledgerSeqRange.Last,
		func(txMeta xdr.LedgerCloseMeta) error {
			currentSeq = txMeta.LedgerSequence()
			if initialSeq == 0 {
				initialSeq = currentSeq
				d.logger.WithField("seq", currentSeq).
					Info("initializing in-memory store")
			} else if (currentSeq-initialSeq)%inMemoryInitializationLedgerLogPeriod == 0 {
				d.logger.WithField("seq", currentSeq).
					Debug("still initializing in-memory store")
			}

			if err = feeWindows.IngestFees(txMeta); err != nil {
				d.logger.WithError(err).Fatal("could not initialize fee stats")
			}

			if err := dataMigrations.Apply(readTxMetaCtx, txMeta); err != nil {
				d.logger.WithError(err).Fatal("could not apply migration for ledger ", currentSeq)
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
		d.logger.WithField("seq", currentSeq).
			Info("finished initializing in-memory store and applying DB data migrations")
	}

	return feeWindows
}

func (d *Daemon) Run() {
	d.logger.WithField("addr", d.listener.Addr().String()).Info("starting HTTP server")

	panicGroup := util.UnrecoverablePanicGroup.Log(d.logger)
	panicGroup.Go(func() {
		if err := d.server.Serve(d.listener); !errors.Is(err, http.ErrServerClosed) {
			d.logger.WithError(err).Fatal("soroban JSON RPC server encountered fatal error")
		}
	})

	if d.adminServer != nil {
		d.logger.
			WithField("addr", d.adminListener.Addr().String()).
			Info("starting Admin HTTP server")
		panicGroup.Go(func() {
			if err := d.adminServer.Serve(d.adminListener); !errors.Is(err, http.ErrServerClosed) {
				d.logger.WithError(err).Error("soroban admin server encountered fatal error")
			}
		})
	}

	// Shutdown gracefully when we receive an interrupt signal. First
	// server.Shutdown closes all open listeners, then closes all idle
	// connections. Finally, it waits a grace period (10s here) for connections
	// to return to idle and then shut down.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-signals:
		d.Close()
	case <-d.done:
		return
	}
}
