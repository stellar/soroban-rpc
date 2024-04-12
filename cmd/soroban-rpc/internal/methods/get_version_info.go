package methods

import (
	"context"
	"github.com/creachadair/jrpc2"
	"github.com/creachadair/jrpc2/handler"
	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/config"
	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/daemon/interfaces"
)

type GetVersionInfoRequest struct {
}

type GetVersionInfoResponse struct {
	Version                string `json:"version"`
	CommitHash             string `json:"commit_hash"`
	BuildTimestamp         string `json:"build_time_stamp"`
	ProtocolVersion        int    `json:"protocol_version"`
	CaptiveCoreVersionInfo string `json:"captive_core_version"`
}

func NewGetVersionInfoHandler(daemon interfaces.Daemon) jrpc2.Handler {
	coreClient := daemon.CoreClient()
	return handler.New(func(ctx context.Context, request GetVersionInfoRequest) (GetVersionInfoResponse, error) {
		info, err := coreClient.Info(ctx)
		if err != nil {
			return GetVersionInfoResponse{}, &jrpc2.Error{
				Code:    jrpc2.InternalError,
				Message: err.Error(),
			}
		}

		return GetVersionInfoResponse{
			Version:                config.Version,
			CommitHash:             config.CommitHash,
			BuildTimestamp:         config.BuildTimestamp,
			CaptiveCoreVersionInfo: config.CaptiveCoreVersionInfo,
			ProtocolVersion:        info.Info.ProtocolVersion,
		}, nil
	})
}
