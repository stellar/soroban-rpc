package test

import (
	"fmt"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sirupsen/logrus"
	"github.com/stellar/go/support/errors"
	supportlog "github.com/stellar/go/support/log"
	"github.com/stellar/go/support/logmetrics"
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
}

func TestLogMetrics(t *testing.T) {
	logMetrics := logmetrics.New("log_metrics_test")
	logger := supportlog.New()
	logger.AddHook(logMetrics)

	err := errors.Errorf("test-error")
	logger.WithError(err).Error("test error 1")
	logger.WithError(err).Error("test error 2")

	val := testutil.ToFloat64(logMetrics[logrus.ErrorLevel])
	assert.Equal(t, val, 2.0)
}

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
