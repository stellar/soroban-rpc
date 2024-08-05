package interfaces

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/stellar/go/ingest/ledgerbackend"
	proto "github.com/stellar/go/protocols/stellarcore"
)

// Daemon defines the interface that the Daemon would be implementing.
// this would be useful for decoupling purposes, allowing to test components without
// the actual daemon.
type Daemon interface {
	MetricsRegistry() *prometheus.Registry
	MetricsNamespace() string
	CoreClient() CoreClient
	GetCore() *ledgerbackend.CaptiveStellarCore
	GetMajorCoreVersion() string
}

type CoreClient interface {
	Info(ctx context.Context) (*proto.InfoResponse, error)
	SubmitTransaction(ctx context.Context, txBase64 string) (*proto.TXResponse, error)
}
