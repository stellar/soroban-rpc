package methods

import (
	"context"

	"github.com/creachadair/jrpc2"

	"github.com/stellar/go/support/log"

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
	logger *log.Entry,
	networkPassphrase string,
	friendbotURL string,
	ledgerEntryReader db.LedgerEntryReader,
	ledgerReader db.LedgerReader,
) jrpc2.Handler {
	return NewHandler(func(ctx context.Context, request GetNetworkRequest) (GetNetworkResponse, error) {
		protocolVersion, err := getProtocolVersion(ctx, ledgerEntryReader, ledgerReader)
		if err != nil {
			logger.WithError(err).Error("failed to fetch protocol version")
		}

		return GetNetworkResponse{
			FriendbotURL:    friendbotURL,
			Passphrase:      networkPassphrase,
			ProtocolVersion: int(protocolVersion),
		}, nil
	})
}
