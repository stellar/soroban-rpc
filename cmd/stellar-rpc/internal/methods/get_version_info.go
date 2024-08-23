package methods

import (
	"context"

	"github.com/creachadair/jrpc2"
	"github.com/creachadair/jrpc2/handler"

	"github.com/stellar/go/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/config"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/daemon/interfaces"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
)

type GetVersionInfoResponse struct {
	Version string `json:"version"`
	// TODO: casing to be fixed by https://github.com/stellar/soroban-rpc/pull/164
	CommitHash         string `json:"commit_hash"`          //nolint:tagliatelle
	BuildTimestamp     string `json:"build_time_stamp"`     //nolint:tagliatelle
	CaptiveCoreVersion string `json:"captive_core_version"` //nolint:tagliatelle
	ProtocolVersion    uint32 `json:"protocol_version"`     //nolint:tagliatelle
}

func NewGetVersionInfoHandler(
	logger *log.Entry,
	ledgerEntryReader db.LedgerEntryReader,
	ledgerReader db.LedgerReader,
	daemon interfaces.Daemon,
) jrpc2.Handler {
	core := daemon.GetCore()

	return handler.New(func(ctx context.Context) (GetVersionInfoResponse, error) {
		captiveCoreVersion := core.GetCoreVersion()
		protocolVersion, err := getProtocolVersion(ctx, ledgerEntryReader, ledgerReader)
		if err != nil {
			logger.WithError(err).Error("failed to fetch protocol version")
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
