package test

import (
	"fmt"
	io_prometheus_client "github.com/prometheus/client_model/go"
	"github.com/stellar/go/support/errors"
	"github.com/stretchr/testify/assert"
	"io"
	"net/http"
	"net/url"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/soroban-tools/cmd/soroban-rpc/internal/config"
)

func TestMetrics(t *testing.T) {
	test := NewTest(t, nil)
	metrics := getMetrics(test)
	buildMetric := fmt.Sprintf(
		"soroban_rpc_build_info{branch=\"%s\",build_timestamp=\"%s\",commit=\"%s\",goversion=\"%s\",version=\"%s\"} 1",
		config.Branch,
		config.BuildTimestamp,
		config.CommitHash,
		runtime.Version(),
		config.Version,
	)
	require.Contains(t, metrics, buildMetric)

	logger := test.daemon.Logger()
	err := errors.Errorf("test-error")
	logger.WithError(err).Error("test error 1")
	logger.WithError(err).Error("test error 2")

	metricFamilies, err := test.daemon.MetricsRegistry().Gather()
	assert.NoError(t, err)
	var metric *io_prometheus_client.MetricFamily
	for _, mf := range metricFamilies {
		if *mf.Name == "soroban_rpc_log_error_total" {
			metric = mf
			break
		}
	}
	val := metric.Metric[0].Counter.GetValue()
	assert.Equal(t, val, 2.0)
}

//func TestLogMetrics(t *testing.T) {
//	logMetrics := logmetrics.New("log_metrics_test")
//	logger := supportlog.New()
//	logger.AddHook(logMetrics)
//
//	err := errors.Errorf("test-error")
//	logger.WithError(err).Error("test error 1")
//	logger.WithError(err).Error("test error 2")
//
//	val := testutil.ToFloat64(logMetrics[logrus.ErrorLevel])
//	assert.Equal(t, val, 2.0)
//}

func getMetrics(test *Test) string {
	metricsURL, err := url.JoinPath(test.adminURL(), "/metrics")
	require.NoError(test.t, err)
	response, err := http.Get(metricsURL)
	require.NoError(test.t, err)
	responseBytes, err := io.ReadAll(response.Body)
	require.NoError(test.t, err)
	require.NoError(test.t, response.Body.Close())
	return string(responseBytes)
}
