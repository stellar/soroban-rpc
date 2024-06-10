package daemon

import (
	"context"
	"errors"
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
	transactionsTableMigrationDoneMetaKey = "TransactionsTableMigrationDone"
)

type Daemon struct {
	core                *ledgerbackend.CaptiveStellarCore
	coreClient          *CoreClientWithMetrics
	ingestService       *ingest.Service
	db                  *db.DB
	jsonRPCHandler      *internal.Handler
	logger              *supportlog.Entry
	preflightWorkerPool *preflight.PreflightWorkerPool
	server              *http.Server
	adminServer         *http.Server
	closeOnce           sync.Once
	closeError          error
	done                chan struct{}
	metricsRegistry     *prometheus.Registry
}

func (d *Daemon) GetDB() *db.DB {
	return d.db
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

func MustNew(cfg *config.Config) *Daemon {
	logger := supportlog.New()
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

	eventStore := events.NewMemoryStore(
		daemon,
		cfg.NetworkPassphrase,
		cfg.EventLedgerRetentionWindow,
	)
	feewindows := feewindow.NewFeeWindows(cfg.ClassicFeeStatsLedgerRetentionWindow, cfg.SorobanFeeStatsLedgerRetentionWindow, cfg.NetworkPassphrase)

	// initialize the stores using what was on the DB
	readTxMetaCtx, cancelReadTxMeta := context.WithTimeout(context.Background(), cfg.IngestionTimeout)
	defer cancelReadTxMeta()
	// NOTE: We could optimize this to avoid unnecessary ingestion calls
	//       (the range of txmetas can be larger than the individual store retention windows)
	//       but it's probably not worth the pain.
	var initialSeq uint32
	var currentSeq uint32
	// We should do the migration somewhere else
	migrationSession := dbConn.Clone()
	if err = migrationSession.Begin(readTxMetaCtx); err != nil {
		logger.WithError(err).Fatal("could not start migration session")
	}
	migrationFunc := func(txmeta xdr.LedgerCloseMeta) error { return nil }
	migrationDoneFunc := func() {}
	val, err := db.GetMetaBool(readTxMetaCtx, migrationSession, transactionsTableMigrationDoneMetaKey)
	if err == db.ErrEmptyDB || val == false {
		logger.Info("migrating transaction to new backend")
		writer := db.NewTransactionWriter(logger, migrationSession, cfg.NetworkPassphrase)
		migrationFunc = func(txmeta xdr.LedgerCloseMeta) error {
			return writer.InsertTransactions(txmeta)
		}
		migrationDoneFunc = func() {
			err := db.SetMetaBool(readTxMetaCtx, migrationSession, transactionsTableMigrationDoneMetaKey)
			if err != nil {
				logger.WithError(err).WithField("key", transactionsTableMigrationDoneMetaKey).Fatal("could not set metadata")
				migrationSession.Rollback()
				return
			}
			// TODO: rollback wherever necessary
			if err = migrationSession.Commit(); err != nil {
				logger.WithError(err).Error("could not commit migration session")
			}
		}
	} else if err != nil {
		logger.WithError(err).WithField("key", transactionsTableMigrationDoneMetaKey).Fatal("could not get metadata")
	}

	err = db.NewLedgerReader(dbConn).StreamAllLedgers(readTxMetaCtx, func(txmeta xdr.LedgerCloseMeta) error {
		currentSeq = txmeta.LedgerSequence()
		if initialSeq == 0 {
			initialSeq = currentSeq
			logger.WithFields(supportlog.F{
				"seq": currentSeq,
			}).Info("initializing in-memory store")
		} else if (currentSeq-initialSeq)%inMemoryInitializationLedgerLogPeriod == 0 {
			logger.WithFields(supportlog.F{
				"seq": currentSeq,
			}).Debug("still initializing in-memory store")
		}
		if err := eventStore.IngestEvents(txmeta); err != nil {
			logger.WithError(err).Fatal("could not initialize event memory store")
		}
		if err := feewindows.IngestFees(txmeta); err != nil {
			logger.WithError(err).Fatal("could not initialize fee stats")
		}
		if err := migrationFunc(txmeta); err != nil {
			// TODO: we should only migrate the transaction range
			logger.WithError(err).Fatal("could not run migration")
		}
		return nil
	})
	if err != nil {
		logger.WithError(err).Fatal("could not obtain txmeta cache from the database")
	}
	migrationDoneFunc()

	if currentSeq != 0 {
		logger.WithFields(supportlog.F{
			"seq": currentSeq,
		}).Info("finished initializing in-memory store")
	}

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

	daemon.server = &http.Server{
		Addr:        cfg.Endpoint,
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
		daemon.adminServer = &http.Server{Addr: cfg.AdminEndpoint, Handler: adminMux}
	}
	daemon.registerMetrics()
	return daemon
}

func (d *Daemon) Run() {
	d.logger.WithFields(supportlog.F{
		"addr": d.server.Addr,
	}).Info("starting HTTP server")

	panicGroup := util.UnrecoverablePanicGroup.Log(d.logger)
	panicGroup.Go(func() {
		if err := d.server.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			// Error starting or closing listener:
			d.logger.WithError(err).Fatal("soroban JSON RPC server encountered fatal error")
		}
	})

	if d.adminServer != nil {
		panicGroup.Go(func() {
			if err := d.adminServer.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
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
