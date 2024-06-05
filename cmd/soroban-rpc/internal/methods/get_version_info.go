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

type GetVersionInfoResponse struct {
	Version            string `json:"version"`
	CommitHash         string `json:"commit_hash"`
	BuildTimestamp     string `json:"build_time_stamp"`
	CaptiveCoreVersion string `json:"captive_core_version"`
	ProtocolVersion    uint32 `json:"protocol_version"`
}

func NewGetVersionInfoHandler(
	logger *log.Entry,
	ledgerEntryReader db.LedgerEntryReader,
	ledgerReader db.LedgerReader,
	daemon interfaces.Daemon,
) jrpc2.Handler {

	core := daemon.GetCore()

	return handler.New(func(ctx context.Context) (GetVersionInfoResponse, error) {

		captiveCoreVersion := core.GetCaptiveCoreVersion()
		protocolVersion, err := getProtocolVersion(ctx, ledgerEntryReader, ledgerReader)

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
