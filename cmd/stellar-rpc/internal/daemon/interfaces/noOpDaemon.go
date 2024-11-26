package interfaces

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/stellar/go/ingest/ledgerbackend"
	proto "github.com/stellar/go/protocols/stellarcore"
)

// NoOpDaemon The noOpDeamon is a dummy daemon implementation, supporting the Daemon interface.
// Used only in testing.
type NoOpDaemon struct {
	metricsRegistry  *prometheus.Registry
	metricsNamespace string
	coreClient       noOpCoreClient
	core             *ledgerbackend.CaptiveStellarCore
}

func MakeNoOpDeamon() *NoOpDaemon {
	return &NoOpDaemon{
		metricsRegistry:  prometheus.NewRegistry(),
		metricsNamespace: "stellar_rpc",
		coreClient:       noOpCoreClient{},
	}
}

func (d *NoOpDaemon) MetricsRegistry() *prometheus.Registry {
	return prometheus.NewRegistry() // so that you can register metrics many times
}

func (d *NoOpDaemon) MetricsNamespace() string {
	return d.metricsNamespace
}

func (d *NoOpDaemon) CoreClient() CoreClient {
	return d.coreClient
}

func (d *NoOpDaemon) GetCore() *ledgerbackend.CaptiveStellarCore {
	return d.core
}

type noOpCoreClient struct{}

func (s noOpCoreClient) Info(context.Context) (*proto.InfoResponse, error) {
	return &proto.InfoResponse{}, nil
}

func (s noOpCoreClient) SubmitTransaction(context.Context, string) (*proto.TXResponse, error) {
	return &proto.TXResponse{Status: proto.PreflightStatusOk}, nil
}
