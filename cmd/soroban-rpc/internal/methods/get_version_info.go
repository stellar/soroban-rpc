package methods

import (
	"context"
	"github.com/creachadair/jrpc2"
	"github.com/creachadair/jrpc2/handler"
	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/config"
)

type GetVersionInfoRequest struct {
}

type GetVersionInfoResponse struct {
	Version                string `json:"version"`
	CommitHash             string `json:"commit_hash"`
	BuildTimestamp         string `json:"build_time_stamp"`
	CaptiveCoreVersionInfo string `json:"captive_core_version"`
}

func NewGetVersionInfoHandler() jrpc2.Handler {
	return handler.New(func(ctx context.Context, request GetVersionInfoRequest) (GetVersionInfoResponse, error) {

		return GetVersionInfoResponse{
			Version:                config.Version,
			CommitHash:             config.CommitHash,
			BuildTimestamp:         config.BuildTimestamp,
			CaptiveCoreVersionInfo: config.CaptiveCoreVersionInfo,
		}, nil
	})
}
