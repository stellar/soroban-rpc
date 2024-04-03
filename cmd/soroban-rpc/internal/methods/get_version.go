package methods

import (
	"context"
	"github.com/creachadair/jrpc2"
	"github.com/creachadair/jrpc2/handler"
	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/daemon/interfaces"
)

type GetVersionRequest struct {
}

type GetVersionResponse struct {
	Version         string `json:"version"`
	XDRVersion      string `json:"xdr_version"`
	ContractVersion string `json:"contract_version"`
}

func NewGetVersionHandler(daemon interfaces.Daemon) jrpc2.Handler {
	coreClient := daemon.CoreClient()
	return handler.New(func(ctx context.Context, request GetVersionRequest) (GetVersionResponse, error) {
		_, err := coreClient.Info(ctx)
		if err != nil {
			return GetVersionResponse{}, &jrpc2.Error{
				Code:    jrpc2.InternalError,
				Message: err.Error(),
			}
		}
		return GetVersionResponse{}, nil
	})
}
