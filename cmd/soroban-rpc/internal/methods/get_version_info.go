package methods

import (
	"context"
	"github.com/creachadair/jrpc2"
	"github.com/creachadair/jrpc2/handler"
	"github.com/stellar/go/support/log"
	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/config"
	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/daemon/interfaces"
)

type GetVersionInfoRequest struct {
}

type GetVersionInfoResponse struct {
	Version            string `json:"version"`
	CommitHash         string `json:"commit_hash"`
	BuildTimestamp     string `json:"build_time_stamp"`
	CaptiveCoreVersion string `json:"captive_core_version"`
}

func NewGetVersionInfoHandler(logger *log.Entry, daemon interfaces.Daemon) jrpc2.Handler {
	coreClient := daemon.CoreClient()
	return handler.New(func(ctx context.Context, request GetVersionInfoRequest) (GetVersionInfoResponse, error) {
		info, err := coreClient.Info(ctx)
		var captiveCoreVersion string
		if err != nil {
			logger.WithError(err).WithField("request", request).
				Infof("error occurred while calling Info endpoint of core")
		} else {
			captiveCoreVersion = info.Info.Build
		}

		return GetVersionInfoResponse{
			Version:            config.Version,
			CommitHash:         config.CommitHash,
			BuildTimestamp:     config.BuildTimestamp,
			CaptiveCoreVersion: captiveCoreVersion,
		}, nil
	})
}
