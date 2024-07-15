package methods

import (
	"context"

	"github.com/creachadair/jrpc2"

	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/db"
)

type GetNetworkRequest struct{}

type GetNetworkResponse struct {
	FriendbotURL    string `json:"friendbotUrl,omitempty"`
	Passphrase      string `json:"passphrase"`
	ProtocolVersion int    `json:"protocolVersion"`
}

// NewGetNetworkHandler returns a json rpc handler to for the getNetwork method
func NewGetNetworkHandler(
	networkPassphrase string,
	friendbotURL string,
	ledgerEntryReader db.LedgerEntryReader,
	ledgerReader db.LedgerReader,
) jrpc2.Handler {
	return NewHandler(func(ctx context.Context, request GetNetworkRequest) (GetNetworkResponse, error) {
		protocolVersion, err := getProtocolVersion(ctx, ledgerEntryReader, ledgerReader)
		if err != nil {
			return GetNetworkResponse{}, &jrpc2.Error{
				Code:    jrpc2.InternalError,
				Message: err.Error(),
			}
		}

		return GetNetworkResponse{
			FriendbotURL:    friendbotURL,
			Passphrase:      networkPassphrase,
			ProtocolVersion: int(protocolVersion),
		}, nil
	})
}
