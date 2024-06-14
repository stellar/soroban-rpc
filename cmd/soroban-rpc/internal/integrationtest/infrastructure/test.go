package infrastructure

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stellar/go/clients/stellarcore"
	"github.com/stellar/go/keypair"
	proto "github.com/stellar/go/protocols/stellarcore"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/ledgerbucketwindow"
	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/methods"

	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/config"
	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/daemon"
)

const (
	StandaloneNetworkPassphrase = "Standalone Network ; February 2017"
	MaxSupportedProtocolVersion = 21
	FriendbotURL                = "http://localhost:8000/friendbot"
	// Needed when Core is run with ARTIFICIALLY_ACCELERATE_TIME_FOR_TESTING=true
	checkpointFrequency               = 8
	captiveCoreConfigFilename         = "captive-core-integration-tests.cfg"
	captiveCoreConfigTemplateFilename = captiveCoreConfigFilename + ".tmpl"

	inContainerCoreHostname    = "core"
	inContainerCorePort        = 11625
	inContainerCoreHTTPPort    = 11626
	inContainerCoreArchivePort = 1570
	// any unused port would do
	inContainerCaptiveCorePort = 11725

	inContainerRPCPort      = 8000
	inContainerRPCAdminPort = 8080
)

// Only run RPC, telling how to connect to Core
// and whether we should wait for it
type TestOnlyRPCConfig struct {
	CorePorts TestCorePorts
	DontWait  bool
}

type TestConfig struct {
	ProtocolVersion uint32
	// Run a previously released version of RPC (in a container) instead of the current version
	UseReleasedRPCVersion string
	// Use/Reuse a SQLite file instead of creating it from scratch
	UseSQLitePath string
	OnlyRPC       *TestOnlyRPCConfig
	// Do not mark the test as running in parallel
	NoParallel bool
}

type TestCorePorts struct {
	CorePort        uint16
	CoreHTTPPort    uint16
	CoreArchivePort uint16
	// This only needs to be an unconflicting port
	captiveCorePort uint16
}

type TestPorts struct {
	RPCPort      uint16
	RPCAdminPort uint16
	TestCorePorts
}

type Test struct {
	t *testing.T

	testPorts TestPorts

	protocolVersion uint32

	rpcConfigFilesDir string

	rpcContainerVersion        string
	rpcContainerSQLiteMountDir string
	rpcContainerLogsCommand    *exec.Cmd

	rpcClient  *Client
	coreClient *stellarcore.Client

	daemon *daemon.Daemon

	masterAccount txnbuild.Account
	shutdownOnce  sync.Once
	shutdown      func()
	onlyRPC       bool
}

func NewTest(t *testing.T, cfg *TestConfig) *Test {
	if os.Getenv("SOROBAN_RPC_INTEGRATION_TESTS_ENABLED") == "" {
		t.Skip("skipping integration test: SOROBAN_RPC_INTEGRATION_TESTS_ENABLED not set")
	}
	i := &Test{t: t}

	i.masterAccount = &txnbuild.SimpleAccount{
		AccountID: i.MasterKey().Address(),
		Sequence:  0,
	}

	parallel := true
	sqlitePath := ""
	shouldWaitForRPC := true
	if cfg != nil {
		i.rpcContainerVersion = cfg.UseReleasedRPCVersion
		i.protocolVersion = cfg.ProtocolVersion
		sqlitePath = cfg.UseSQLitePath
		if cfg.OnlyRPC != nil {
			i.onlyRPC = true
			i.testPorts.TestCorePorts = cfg.OnlyRPC.CorePorts
			shouldWaitForRPC = !cfg.OnlyRPC.DontWait
		}
		parallel = !cfg.NoParallel
	}
	if sqlitePath == "" {
		sqlitePath = path.Join(i.t.TempDir(), "soroban_rpc.sqlite")
	}

	if parallel {
		t.Parallel()
	}

	// TODO: this function is pretty unreadable

	if i.protocolVersion == 0 {
		// Default to the maximum supported protocol version
		i.protocolVersion = GetCoreMaxSupportedProtocol()
	}

	i.rpcConfigFilesDir = i.t.TempDir()

	if i.runRPCInContainer() {
		// The container needs to use the sqlite mount point
		i.rpcContainerSQLiteMountDir = filepath.Dir(sqlitePath)
		i.generateCaptiveCoreCfgForContainer()
		rpcCfg := i.getRPConfigForContainer(sqlitePath)
		i.generateRPCConfigFile(rpcCfg)
	}

	i.prepareShutdownHandlers()
	if i.runRPCInContainer() || !i.onlyRPC {
		// There are containerized workloads
		upCmd := []string{"up"}
		if i.runRPCInContainer() && i.onlyRPC {
			upCmd = append(upCmd, "rpc")
		}
		upCmd = append(upCmd, "--detach", "--quiet-pull", "--no-color")
		i.runSuccessfulComposeCommand(upCmd...)
		if i.runRPCInContainer() {
			i.rpcContainerLogsCommand = i.getComposeCommand("logs", "--no-log-prefix", "-f", "rpc")
			i.rpcContainerLogsCommand.Stdout = os.Stdout
			i.rpcContainerLogsCommand.Stderr = os.Stderr
			require.NoError(t, i.rpcContainerLogsCommand.Start())
		}
		i.fillContainerPorts()
	}
	if !i.onlyRPC {
		i.coreClient = &stellarcore.Client{URL: "http://localhost:" + strconv.Itoa(int(i.testPorts.CoreHTTPPort))}
		i.waitForCore()
		i.waitForCheckpoint()
	}
	if !i.runRPCInContainer() {
		// We need to get a free port. Unfortunately this isn't completely clash-Free
		// but there is no way to tell core to allocate the port dynamically
		i.testPorts.captiveCorePort = getFreeTCPPort(i.t)
		i.generateCaptiveCoreCfgForDaemon()
		rpcCfg := i.getRPConfigForDaemon(sqlitePath)
		i.daemon = i.createDaemon(rpcCfg)
		i.fillDaemonPorts()
		go i.daemon.Run()
	}

	i.rpcClient = NewClient(i.GetSorobanRPCURL(), nil)
	if shouldWaitForRPC {
		i.waitForRPC()
	}

	return i
}

func (i *Test) GetPorts() TestPorts {
	return i.testPorts
}

func (i *Test) runRPCInContainer() bool {
	return i.rpcContainerVersion != ""
}

func (i *Test) GetRPCLient() *Client {
	return i.rpcClient
}
func (i *Test) MasterKey() *keypair.Full {
	return keypair.Root(StandaloneNetworkPassphrase)
}

func (i *Test) MasterAccount() txnbuild.Account {
	return i.masterAccount
}

func (i *Test) GetSorobanRPCURL() string {
	return fmt.Sprintf("http://localhost:%d", i.testPorts.RPCPort)
}

func (i *Test) GetAdminURL() string {
	return fmt.Sprintf("http://localhost:%d", i.testPorts.RPCAdminPort)
}

func (i *Test) getCoreInfo() (*proto.InfoResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return i.coreClient.Info(ctx)
}

func (i *Test) waitForCheckpoint() {
	i.t.Log("Waiting for checkpoint...")
	require.Eventually(i.t,
		func() bool {
			info, err := i.getCoreInfo()
			return err == nil && info.Info.Ledger.Num > checkpointFrequency
		},
		30*time.Second,
		time.Second,
	)
}

func (i *Test) getRPConfigForContainer(sqlitePath string) rpcConfig {
	return rpcConfig{
		// Container's default path to captive core
		coreBinaryPath: "/usr/bin/stellar-core",
		archiveURL:     fmt.Sprintf("http://%s:%d", inContainerCoreHostname, inContainerCoreArchivePort),
		// The file will be inside the container
		captiveCoreConfigPath: "/stellar-core.cfg",
		// The container needs to listen on all interfaces, not just localhost
		// (otherwise it can't be accessible from the outside)
		endPoint:               fmt.Sprintf("0.0.0.0:%d", inContainerRPCPort),
		adminEndpoint:          fmt.Sprintf("0.0.0.0:%d", inContainerRPCAdminPort),
		sqlitePath:             "/db/" + filepath.Base(sqlitePath),
		stellarCoreURL:         fmt.Sprintf("http://%s:%d", inContainerCoreHostname, inContainerCoreHTTPPort),
		captiveCoreStoragePath: "/tmp/captive-core",
	}
}

func (i *Test) getRPConfigForDaemon(sqlitePath string) rpcConfig {
	coreBinaryPath := os.Getenv("SOROBAN_RPC_INTEGRATION_TESTS_CAPTIVE_CORE_BIN")
	if coreBinaryPath == "" {
		i.t.Fatal("missing SOROBAN_RPC_INTEGRATION_TESTS_CAPTIVE_CORE_BIN")
	}
	return rpcConfig{
		coreBinaryPath:        coreBinaryPath,
		archiveURL:            fmt.Sprintf("http://localhost:%d", i.testPorts.CoreArchivePort),
		captiveCoreConfigPath: path.Join(i.rpcConfigFilesDir, captiveCoreConfigFilename),
		stellarCoreURL:        fmt.Sprintf("http://localhost:%d", i.testPorts.CoreHTTPPort),
		// Allocate port dynamically and then figure out what the port is
		endPoint:               "localhost:0",
		adminEndpoint:          "localhost:0",
		captiveCoreStoragePath: i.t.TempDir(),
		sqlitePath:             sqlitePath,
	}
}

type rpcConfig struct {
	endPoint               string
	adminEndpoint          string
	stellarCoreURL         string
	coreBinaryPath         string
	captiveCoreConfigPath  string
	captiveCoreStoragePath string
	archiveURL             string
	sqlitePath             string
}

func (vars rpcConfig) toMap() map[string]string {
	return map[string]string{
		"ENDPOINT":                       vars.endPoint,
		"ADMIN_ENDPOINT":                 vars.adminEndpoint,
		"STELLAR_CORE_URL":               vars.stellarCoreURL,
		"CORE_REQUEST_TIMEOUT":           "2s",
		"STELLAR_CORE_BINARY_PATH":       vars.coreBinaryPath,
		"CAPTIVE_CORE_CONFIG_PATH":       vars.captiveCoreConfigPath,
		"CAPTIVE_CORE_STORAGE_PATH":      vars.captiveCoreStoragePath,
		"STELLAR_CAPTIVE_CORE_HTTP_PORT": "0",
		"FRIENDBOT_URL":                  FriendbotURL,
		"NETWORK_PASSPHRASE":             StandaloneNetworkPassphrase,
		"HISTORY_ARCHIVE_URLS":           vars.archiveURL,
		"LOG_LEVEL":                      "debug",
		"DB_PATH":                        vars.sqlitePath,
		"INGESTION_TIMEOUT":              "10m",
		"EVENT_LEDGER_RETENTION_WINDOW":  strconv.Itoa(ledgerbucketwindow.OneDayOfLedgers),
		"TRANSACTION_RETENTION_WINDOW":   strconv.Itoa(ledgerbucketwindow.OneDayOfLedgers),
		"CHECKPOINT_FREQUENCY":           strconv.Itoa(checkpointFrequency),
		"MAX_HEALTHY_LEDGER_LATENCY":     "10s",
		"PREFLIGHT_ENABLE_DEBUG":         "true",
	}
}

func (i *Test) waitForRPC() {
	i.t.Log("Waiting for RPC to be healthy...")

	require.Eventually(i.t,
		func() bool {
			result, err := i.GetRPCHealth()
			return err == nil && result.Status == "healthy"
		},
		30*time.Second,
		time.Second,
	)
}

func (i *Test) generateCaptiveCoreCfgForContainer() {
	getOldVersionCaptiveCoreConfigVersion := func(dir string, filename string) ([]byte, error) {
		cmd := exec.Command("git", "show", fmt.Sprintf("v%s:./%s/%s", i.rpcContainerVersion, dir, filename))
		cmd.Dir = GetCurrentDirectory()
		return cmd.Output()
	}

	// Get old version of captive-core-integration-tests.cfg.tmpl
	out, err := getOldVersionCaptiveCoreConfigVersion("docker", captiveCoreConfigTemplateFilename)
	if err != nil {
		// Try the directory before the integration test refactoring
		// TODO: remove this hack after protocol 22 is released
		out, err = getOldVersionCaptiveCoreConfigVersion("../../test", captiveCoreConfigFilename)
		outStr := strings.Replace(string(out), `ADDRESS="localhost"`, `ADDRESS="${CORE_HOST_PORT}"`, -1)
		out = []byte(outStr)
	}
	require.NoError(i.t, err)
	i.generateCaptiveCoreCfg(out, inContainerCaptiveCorePort, inContainerCoreHostname)
}

func (i *Test) generateCaptiveCoreCfg(tmplContents []byte, captiveCorePort uint16, coreHostPort string) {
	// Apply expansion
	mapping := func(in string) string {
		switch in {
		case "CAPTIVE_CORE_PORT":
			// any non-conflicting port would do
			return strconv.Itoa(int(captiveCorePort))
		case "CORE_HOST_PORT":
			return coreHostPort
		default:
			// Try to leave it as it was
			return "$" + in
		}
	}

	captiveCoreCfgContents := os.Expand(string(tmplContents), mapping)
	err := os.WriteFile(filepath.Join(i.rpcConfigFilesDir, captiveCoreConfigFilename), []byte(captiveCoreCfgContents), 0666)
	require.NoError(i.t, err)
}

func (i *Test) generateCaptiveCoreCfgForDaemon() {
	out, err := os.ReadFile(filepath.Join(GetCurrentDirectory(), "docker", captiveCoreConfigTemplateFilename))
	require.NoError(i.t, err)
	i.generateCaptiveCoreCfg(out, i.testPorts.captiveCorePort, "localhost:"+strconv.Itoa(int(i.testPorts.CorePort)))
}

func (i *Test) generateRPCConfigFile(rpcConfig rpcConfig) {
	cfgFileContents := ""
	for k, v := range rpcConfig.toMap() {
		cfgFileContents += fmt.Sprintf("%s=%q\n", k, v)
	}
	err := os.WriteFile(filepath.Join(i.rpcConfigFilesDir, "soroban-rpc.config"), []byte(cfgFileContents), 0666)
	require.NoError(i.t, err)
}

func (i *Test) createDaemon(c rpcConfig) *daemon.Daemon {
	var cfg config.Config
	m := c.toMap()
	lookup := func(s string) (string, bool) {
		ret, ok := m[s]
		return ret, ok
	}
	require.NoError(i.t, cfg.SetValues(lookup))
	require.NoError(i.t, cfg.Validate())
	cfg.HistoryArchiveUserAgent = fmt.Sprintf("soroban-rpc/%s", config.Version)
	return daemon.MustNew(&cfg)
}

var nonAlphanumericRegex = regexp.MustCompile("[^a-zA-Z0-9]+")

func (i *Test) getComposeProjectName() string {
	alphanumeric := nonAlphanumericRegex.ReplaceAllString(i.t.Name(), "")
	return strings.ToLower(alphanumeric)
}

func (i *Test) getComposeCommand(args ...string) *exec.Cmd {
	composeFile := "docker-compose.yml"
	if i.runRPCInContainer() {
		composeFile = "docker-compose.rpc.yml"
	}
	fullComposeFilePath := filepath.Join(GetCurrentDirectory(), "docker", composeFile)
	cmdline := []string{"-f", fullComposeFilePath}
	// Use separate projects to run them in parallel
	projectName := i.getComposeProjectName()
	cmdline = append([]string{"-p", projectName}, cmdline...)
	cmdline = append(cmdline, args...)
	cmd := exec.Command("docker-compose", cmdline...)

	if img := os.Getenv("SOROBAN_RPC_INTEGRATION_TESTS_DOCKER_IMG"); img != "" {
		cmd.Env = append(
			cmd.Env,
			"CORE_IMAGE="+img,
		)
	}

	if i.runRPCInContainer() {
		cmd.Env = append(
			cmd.Env,
			"RPC_IMAGE_TAG="+i.rpcContainerVersion,
			"RPC_CONFIG_MOUNT_DIR="+i.rpcConfigFilesDir,
			"RPC_SQLITE_MOUNT_DIR="+i.rpcContainerSQLiteMountDir,
			"RPC_UID="+strconv.Itoa(os.Getuid()),
			"RPC_GID="+strconv.Itoa(os.Getgid()),
		)
	}
	if cmd.Env != nil {
		cmd.Env = append(os.Environ(), cmd.Env...)
	}

	return cmd
}

func (i *Test) runComposeCommand(args ...string) ([]byte, error) {
	cmd := i.getComposeCommand(args...)
	return cmd.Output()
}

func (i *Test) runSuccessfulComposeCommand(args ...string) []byte {
	out, err := i.runComposeCommand(args...)
	if err != nil {
		i.t.Log("Compose command failed, args:", args)
	}
	if exitErr, ok := err.(*exec.ExitError); ok {
		i.t.Log("stdout:\n", string(out))
		i.t.Log("stderr:\n", string(exitErr.Stderr))
	}
	require.NoError(i.t, err)
	return out
}

func (i *Test) prepareShutdownHandlers() {
	done := make(chan struct{})
	i.shutdown = func() {
		close(done)
		if i.daemon != nil {
			i.daemon.Close()
			i.daemon = nil
		}
		if i.rpcClient != nil {
			i.rpcClient.Close()
		}
		if i.runRPCInContainer() || !i.onlyRPC {
			// There were containerized workloads we should bring down
			downCmd := []string{"down"}
			if i.runRPCInContainer() && i.onlyRPC {
				downCmd = append(downCmd, "rpc")
			}
			downCmd = append(downCmd, "-v")
			i.runSuccessfulComposeCommand(downCmd...)
		}
		if i.rpcContainerLogsCommand != nil {
			i.rpcContainerLogsCommand.Wait()
		}
	}

	// Register shutdown handlers (on panic and ctrl+c) so the containers are
	// stopped even if ingestion or testing fails.
	i.t.Cleanup(i.Shutdown)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		select {
		case <-c:
			i.Shutdown()
			os.Exit(int(syscall.SIGTERM))
		case <-done:
		}
	}()
}

// Shutdown stops the integration tests and destroys all its associated
// resources. It will be implicitly called when the calling test (i.e. the
// `testing.Test` passed to `New()`) is finished if it hasn't been explicitly
// called before.
func (i *Test) Shutdown() {
	i.shutdownOnce.Do(func() {
		i.shutdown()
	})
}

// Wait for core to be up and manually close the first ledger
func (i *Test) waitForCore() {
	i.t.Log("Waiting for core to be up...")
	require.Eventually(i.t,
		func() bool {
			_, err := i.getCoreInfo()
			return err == nil
		},
		30*time.Second,
		time.Second,
	)

	i.UpgradeProtocol(i.protocolVersion)

	require.Eventually(i.t,
		func() bool {
			info, err := i.getCoreInfo()
			return err == nil && info.IsSynced()
		},
		30*time.Second,
		time.Second,
	)
}

// UpgradeProtocol arms Core with upgrade and blocks until protocol is upgraded.
func (i *Test) UpgradeProtocol(version uint32) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	err := i.coreClient.Upgrade(ctx, int(version))
	cancel()
	require.NoError(i.t, err)

	require.Eventually(i.t,
		func() bool {
			info, err := i.getCoreInfo()
			return err == nil && info.Info.Ledger.Version == int(version)
		},
		10*time.Second,
		time.Second,
	)
}

func (i *Test) StopRPC() {
	if i.daemon != nil {
		i.daemon.Close()
		i.daemon = nil
	}
	if i.runRPCInContainer() {
		i.runSuccessfulComposeCommand("down", "rpc", "-v")
	}
}

func (i *Test) GetProtocolVersion() uint32 {
	return i.protocolVersion
}

func (i *Test) GetDaemon() *daemon.Daemon {
	return i.daemon
}

func (i *Test) SendMasterOperation(op txnbuild.Operation) methods.GetTransactionResponse {
	params := CreateTransactionParams(i.MasterAccount(), op)
	tx, err := txnbuild.NewTransaction(params)
	assert.NoError(i.t, err)
	return i.SendMasterTransaction(tx)
}

func (i *Test) SendMasterTransaction(tx *txnbuild.Transaction) methods.GetTransactionResponse {
	kp := keypair.Root(StandaloneNetworkPassphrase)
	return SendSuccessfulTransaction(i.t, i.rpcClient, kp, tx)
}

func (i *Test) GetTransaction(hash string) methods.GetTransactionResponse {
	return getTransaction(i.t, i.rpcClient, hash)
}

func (i *Test) PreflightAndSendMasterOperation(op txnbuild.Operation) methods.GetTransactionResponse {
	params := CreateTransactionParams(
		i.MasterAccount(),
		op,
	)
	params = PreflightTransactionParams(i.t, i.rpcClient, params)
	tx, err := txnbuild.NewTransaction(params)
	assert.NoError(i.t, err)
	return i.SendMasterTransaction(tx)
}

func (i *Test) UploadHelloWorldContract() (methods.GetTransactionResponse, xdr.Hash) {
	contractBinary := GetHelloWorldContract()
	return i.uploadContract(contractBinary)
}

func (i *Test) uploadContract(contractBinary []byte) (methods.GetTransactionResponse, xdr.Hash) {
	contractHash := xdr.Hash(sha256.Sum256(contractBinary))
	op := CreateUploadWasmOperation(i.MasterAccount().GetAccountID(), contractBinary)
	return i.PreflightAndSendMasterOperation(op), contractHash
}

func (i *Test) CreateHelloWorldContract() (methods.GetTransactionResponse, [32]byte, xdr.Hash) {
	contractBinary := GetHelloWorldContract()
	_, contractHash := i.uploadContract(contractBinary)
	salt := xdr.Uint256(testSalt)
	account := i.MasterAccount().GetAccountID()
	op := createCreateContractOperation(account, salt, contractHash)
	contractID := getContractID(i.t, account, salt, StandaloneNetworkPassphrase)
	return i.PreflightAndSendMasterOperation(op), contractID, contractHash
}

func (i *Test) InvokeHostFunc(contractID xdr.Hash, method string, args ...xdr.ScVal) methods.GetTransactionResponse {
	op := CreateInvokeHostOperation(i.MasterAccount().GetAccountID(), contractID, method, args...)
	return i.PreflightAndSendMasterOperation(op)
}

func (i *Test) GetRPCHealth() (methods.HealthCheckResult, error) {
	var result methods.HealthCheckResult
	err := i.rpcClient.CallResult(context.Background(), "getHealth", nil, &result)
	return result, err
}

func (i *Test) fillContainerPorts() {
	getPublicPort := func(service string, privatePort int) uint16 {
		var port uint16
		// We need to try several times because we detached from `docker-compose up`
		// and the container may not be ready
		require.Eventually(i.t,
			func() bool {
				out, err := i.runComposeCommand("port", service, strconv.Itoa(privatePort))
				if err != nil {
					return false
				}
				_, strPort, err := net.SplitHostPort(strings.TrimSpace(string(out)))
				require.NoError(i.t, err)
				intPort, err := strconv.Atoi(strPort)
				require.NoError(i.t, err)
				port = uint16(intPort)
				return true
			},
			2*time.Second,
			100*time.Millisecond,
		)
		return port
	}
	i.testPorts.CorePort = getPublicPort("core", inContainerCorePort)
	i.testPorts.CoreHTTPPort = getPublicPort("core", inContainerCoreHTTPPort)
	i.testPorts.CoreArchivePort = getPublicPort("core", inContainerCoreArchivePort)
	if i.runRPCInContainer() {
		i.testPorts.RPCPort = getPublicPort("rpc", inContainerRPCPort)
		i.testPorts.RPCAdminPort = getPublicPort("rpc", inContainerRPCAdminPort)
	}
}

func (i *Test) fillDaemonPorts() {
	endpointAddr, adminEndpointAddr := i.daemon.GetEndpointAddrs()
	i.testPorts.RPCPort = uint16(endpointAddr.Port)
	if adminEndpointAddr != nil {
		i.testPorts.RPCAdminPort = uint16(adminEndpointAddr.Port)
	}
}

func GetCoreMaxSupportedProtocol() uint32 {
	str := os.Getenv("SOROBAN_RPC_INTEGRATION_TESTS_CORE_MAX_SUPPORTED_PROTOCOL")
	if str == "" {
		return MaxSupportedProtocolVersion
	}
	version, err := strconv.ParseUint(str, 10, 32)
	if err != nil {
		return MaxSupportedProtocolVersion
	}

	return uint32(version)
}
