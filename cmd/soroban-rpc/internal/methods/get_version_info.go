package methods

import (
	"context"
	"fmt"

	"github.com/creachadair/jrpc2"
	"github.com/creachadair/jrpc2/handler"

	"github.com/stellar/go/support/log"

	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/config"
	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/daemon/interfaces"
	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/db"
)

type GetVersionInfoResponse struct {
	Version string `json:"version"`
	// TODO: to be fixed by https://github.com/stellar/soroban-rpc/pull/164
	CommitHash         string `json:"commit_hash"`          //nolint:tagliatelle
	BuildTimestamp     string `json:"build_time_stamp"`     //nolint:tagliatelle
	CaptiveCoreVersion string `json:"captive_core_version"` //nolint:tagliatelle
	ProtocolVersion    uint32 `json:"protocol_version"`     //nolint:tagliatelle
}

func NewGetVersionInfoHandler(logger *log.Entry, ledgerReader db.LedgerReader, daemon interfaces.Daemon) jrpc2.Handler {
	coreClient := daemon.CoreClient()
	return handler.New(func(ctx context.Context) (GetVersionInfoResponse, error) {
		var captiveCoreVersion string
		info, err := coreClient.Info(ctx)
		if err != nil {
			logger.WithError(err).Info("error occurred while calling Info endpoint of core")
		} else {
			captiveCoreVersion = info.Info.Build
		}

		// Fetch Protocol version
		var protocolVersion uint32
		ledgerRange, err := ledgerReader.GetLedgerRange(ctx)
		if err != nil {
			return GetVersionInfoResponse{}, &jrpc2.Error{
				Code:    jrpc2.InternalError,
				Message: fmt.Errorf("could not get ledger range: %w", err).Error(),
			}
		}

		_, protocolVersion, err = getBucketListSizeAndProtocolVersion(ctx, ledgerReader, ledgerRange.LastLedger.Sequence)
		if err != nil {
			logger.WithError(err).Info("error occurred while fetching protocol version")
		}

		return GetVersionInfoResponse{
			Version:            config.Version,
			CommitHash:         config.CommitHash,
			BuildTimestamp:     config.BuildTimestamp,
			CaptiveCoreVersion: captiveCoreVersion,
			ProtocolVersion:    protocolVersion,
		}, nil
	})
}
