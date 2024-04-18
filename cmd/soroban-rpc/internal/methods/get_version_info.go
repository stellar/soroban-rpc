package methods

import (
	"context"
	"github.com/creachadair/jrpc2"
	"github.com/creachadair/jrpc2/handler"
	"github.com/stellar/go/support/log"
	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/config"
	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/daemon/interfaces"
	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/db"
)

type GetVersionInfoRequest struct {
}

type GetVersionInfoResponse struct {
	Version            string `json:"version"`
	CommitHash         string `json:"commit_hash"`
	BuildTimestamp     string `json:"build_time_stamp"`
	CaptiveCoreVersion string `json:"captive_core_version"`
	ProtocolVersion    uint32 `json:"protocol_version"`
}

func NewGetVersionInfoHandler(logger *log.Entry, ledgerEntryReader db.LedgerEntryReader, ledgerReader db.LedgerReader, daemon interfaces.Daemon) jrpc2.Handler {
	//coreClient := daemon.CoreClient()
	return handler.New(func(ctx context.Context, request GetVersionInfoRequest) (GetVersionInfoResponse, error) {

		// Fetch Protocol version
		var protocolVersion uint32
		readTx, err := ledgerEntryReader.NewCachedTx(ctx)
		if err != nil {
			logger.WithError(err).WithField("request", request).
				Info("Cannot create read transaction")
		}
		defer func() {
			_ = readTx.Done()
		}()

		latestLedger, err := readTx.GetLatestLedgerSequence()
		if err != nil {
			logger.WithError(err).WithField("request", request).
				Info("error occurred while getting latest ledger")
		}

		_, protocolVersion, err = getBucketListSizeAndProtocolVersion(ctx, ledgerReader, latestLedger)
		if err != nil {
			logger.WithError(err).WithField("request", request).
				Info("error occurred while fetching protocol version")
		}

		return GetVersionInfoResponse{
			Version:            config.Version,
			CommitHash:         config.CommitHash,
			BuildTimestamp:     config.BuildTimestamp,
			CaptiveCoreVersion: config.CaptiveCoreVersion,
			ProtocolVersion:    protocolVersion,
		}, nil
	})
}
