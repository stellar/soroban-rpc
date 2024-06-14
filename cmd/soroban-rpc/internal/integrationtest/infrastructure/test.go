package infrastructure

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/creachadair/jrpc2"
	"github.com/creachadair/jrpc2/jhttp"
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
	StellarCoreArchivePort      = 1570
	stellarCorePort             = 11626
	FriendbotURL                = "http://localhost:8000/friendbot"
	// Needed when Core is run with ARTIFICIALLY_ACCELERATE_TIME_FOR_TESTING=true
	checkpointFrequency    = 8
	sorobanRPCPort         = 8000
	adminPort              = 8080
	helloWorldContractPath = "../../../../../wasms/test_hello_world.wasm"
)

type TestConfig struct {
	ProtocolVersion uint32
	// Run a previously released version of RPC (in a container) instead of the current version
	UseReleasedRPCVersion string
	UseSQLitePath         string
	HistoryArchiveURL     string
}

type Test struct {
	t *testing.T

	protocolVersion uint32

	historyArchiveURL string

	rpcContainerVersion        string
	rpcContainerConfigMountDir string
	rpcContainerSQLiteMountDir string
	rpcContainerLogsCommand    *exec.Cmd

	rpcClient  *jrpc2.Client
	coreClient *stellarcore.Client

	daemon *daemon.Daemon

	masterAccount txnbuild.Account
	shutdownOnce  sync.Once
	shutdown      func()
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

	sqlLitePath := ""
	if cfg != nil {
		i.historyArchiveURL = cfg.HistoryArchiveURL
		i.rpcContainerVersion = cfg.UseReleasedRPCVersion
		i.protocolVersion = cfg.ProtocolVersion
		sqlLitePath = cfg.UseSQLitePath
	}

	if i.protocolVersion == 0 {
		// Default to the maximum supported protocol version
		i.protocolVersion = GetCoreMaxSupportedProtocol()
	}

	rpcCfg := i.getRPConfig(sqlLitePath)
	if i.runRPCInContainer() {
		i.rpcContainerConfigMountDir = i.createRPCContainerMountDir(rpcCfg)
	}
	i.runComposeCommand("up", "--detach", "--quiet-pull", "--no-color")
	if i.runRPCInContainer() {
		cmd := i.getComposeCommand("logs", "--no-log-prefix", "-f", "rpc")
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		require.NoError(t, cmd.Start())
	}
	i.prepareShutdownHandlers()
	i.coreClient = &stellarcore.Client{URL: "http://localhost:" + strconv.Itoa(stellarCorePort)}
	i.waitForCore()
	i.waitForCheckpoint()
	if !i.runRPCInContainer() {
		i.daemon = i.createDaemon(rpcCfg)
		go i.daemon.Run()
	}
	i.waitForRPC()

	return i
}

func (i *Test) runRPCInContainer() bool {
	return i.rpcContainerVersion != ""
}

func (i *Test) GetRPCLient() *jrpc2.Client {
	return i.rpcClient
}
func (i *Test) MasterKey() *keypair.Full {
	return keypair.Root(StandaloneNetworkPassphrase)
}

func (i *Test) MasterAccount() txnbuild.Account {
	return i.masterAccount
}

func (i *Test) GetSorobanRPCURL() string {
	return fmt.Sprintf("http://localhost:%d", sorobanRPCPort)
}

func (i *Test) GetAdminURL() string {
	return fmt.Sprintf("http://localhost:%d", adminPort)
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

func (i *Test) getRPConfig(sqlitePath string) map[string]string {
	if sqlitePath == "" {
		sqlitePath = path.Join(i.t.TempDir(), "soroban_rpc.sqlite")
	}

	// Container's default path to captive core
	coreBinaryPath := "/usr/bin/stellar-core"
	if !i.runRPCInContainer() {
		coreBinaryPath = os.Getenv("SOROBAN_RPC_INTEGRATION_TESTS_CAPTIVE_CORE_BIN")
		if coreBinaryPath == "" {
			i.t.Fatal("missing SOROBAN_RPC_INTEGRATION_TESTS_CAPTIVE_CORE_BIN")
		}
	}

	archiveURL := fmt.Sprintf("http://localhost:%d", StellarCoreArchivePort)
	if i.runRPCInContainer() {
		// the archive needs to be accessed from the container
		// where core is Core's hostname
		archiveURL = fmt.Sprintf("http://core:%d", StellarCoreArchivePort)
	}
	if i.historyArchiveURL != "" {
		// an archive URL was supplied explicitly
		archiveURL = i.historyArchiveURL
	}

	captiveCoreConfigPath := path.Join(GetCurrentDirectory(), "docker", "captive-core-integration-tests.cfg")
	bindHost := "localhost"
	stellarCoreURL := fmt.Sprintf("http://localhost:%d", stellarCorePort)
	if i.runRPCInContainer() {
		// The file will be inside the container
		captiveCoreConfigPath = "/stellar-core.cfg"
		// The container needs to listen on all interfaces, not just localhost
		bindHost = "0.0.0.0"
		// The container needs to use the sqlite mount point
		i.rpcContainerSQLiteMountDir = filepath.Dir(sqlitePath)
		sqlitePath = "/db/" + filepath.Base(sqlitePath)
		stellarCoreURL = fmt.Sprintf("http://core:%d", stellarCorePort)
	}

	// in the container
	captiveCoreStoragePath := "/tmp/captive-core"
	if !i.runRPCInContainer() {
		captiveCoreStoragePath = i.t.TempDir()
	}

	return map[string]string{
		"ENDPOINT":                       fmt.Sprintf("%s:%d", bindHost, sorobanRPCPort),
		"ADMIN_ENDPOINT":                 fmt.Sprintf("%s:%d", bindHost, adminPort),
		"STELLAR_CORE_URL":               stellarCoreURL,
		"CORE_REQUEST_TIMEOUT":           "2s",
		"STELLAR_CORE_BINARY_PATH":       coreBinaryPath,
		"CAPTIVE_CORE_CONFIG_PATH":       captiveCoreConfigPath,
		"CAPTIVE_CORE_STORAGE_PATH":      captiveCoreStoragePath,
		"STELLAR_CAPTIVE_CORE_HTTP_PORT": "0",
		"FRIENDBOT_URL":                  FriendbotURL,
		"NETWORK_PASSPHRASE":             StandaloneNetworkPassphrase,
		"HISTORY_ARCHIVE_URLS":           archiveURL,
		"LOG_LEVEL":                      "debug",
		"DB_PATH":                        sqlitePath,
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
	// This is needed because of https://github.com/creachadair/jrpc2/issues/118
	refreshClient := func() {
		if i.rpcClient != nil {
			i.rpcClient.Close()
		}
		ch := jhttp.NewChannel(i.GetSorobanRPCURL(), nil)
		i.rpcClient = jrpc2.NewClient(ch, nil)
	}
	require.Eventually(i.t,
		func() bool {
			refreshClient()
			result, err := i.GetRPCHealth()
			return err == nil && result.Status == "healthy"
		},
		30*time.Second,
		time.Second,
	)
}

func (i *Test) createRPCContainerMountDir(rpcConfig map[string]string) string {
	mountDir := i.t.TempDir()

	getOldVersionCaptiveCoreConfigVersion := func(dir string) ([]byte, error) {
		cmd := exec.Command("git", "show", fmt.Sprintf("v%s:./%s/captive-core-integration-tests.cfg", i.rpcContainerVersion, dir))
		cmd.Dir = GetCurrentDirectory()
		return cmd.Output()
	}

	// Get old version of captive-core-integration-tests.cfg
	out, err := getOldVersionCaptiveCoreConfigVersion("docker")
	if err != nil {
		out, err = getOldVersionCaptiveCoreConfigVersion("../../test")
	}
	require.NoError(i.t, err)

	// replace ADDRESS="localhost" by ADDRESS="core", so that the container can find core
	captiveCoreCfgContents := strings.Replace(string(out), `ADDRESS="localhost"`, `ADDRESS="core"`, -1)
	err = os.WriteFile(filepath.Join(mountDir, "stellar-core-integration-tests.cfg"), []byte(captiveCoreCfgContents), 0666)
	require.NoError(i.t, err)

	// Generate config file
	cfgFileContents := ""
	for k, v := range rpcConfig {
		cfgFileContents += fmt.Sprintf("%s=%q\n", k, v)
	}
	err = os.WriteFile(filepath.Join(mountDir, "soroban-rpc.config"), []byte(cfgFileContents), 0666)
	require.NoError(i.t, err)

	return mountDir
}

func (i *Test) createDaemon(env map[string]string) *daemon.Daemon {
	var cfg config.Config
	lookup := func(s string) (string, bool) {
		ret, ok := env[s]
		return ret, ok
	}
	require.NoError(i.t, cfg.SetValues(lookup))
	require.NoError(i.t, cfg.Validate())
	cfg.HistoryArchiveUserAgent = fmt.Sprintf("soroban-rpc/%s", config.Version)
	return daemon.MustNew(&cfg)
}

func (i *Test) getComposeCommand(args ...string) *exec.Cmd {
	integrationYaml := filepath.Join(GetCurrentDirectory(), "docker", "docker-compose.yml")
	configFiles := []string{"-f", integrationYaml}
	if i.runRPCInContainer() {
		rpcYaml := filepath.Join(GetCurrentDirectory(), "docker", "docker-compose.rpc.yml")
		configFiles = append(configFiles, "-f", rpcYaml)
	}
	cmdline := append(configFiles, args...)
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
			"RPC_CONFIG_MOUNT_DIR="+i.rpcContainerConfigMountDir,
			"RPC_SQLITE_MOUNT_DIR="+i.rpcContainerSQLiteMountDir,
			"RPC_UID="+strconv.Itoa(os.Getuid()),
			"RPC_GID="+strconv.Itoa(os.Getgid()),
		)
	}
	if len(cmd.Env) > 0 {
		cmd.Env = append(cmd.Env, os.Environ()...)
	}
	return cmd
}

// Runs a docker-compose command applied to the above configs
func (i *Test) runComposeCommand(args ...string) {
	cmd := i.getComposeCommand(args...)
	i.t.Log("Running", cmd.Args)
	out, innerErr := cmd.Output()
	if exitErr, ok := innerErr.(*exec.ExitError); ok {
		i.t.Log("stdout\n:", string(out))
		i.t.Log("stderr:\n", string(exitErr.Stderr))
	}

	if innerErr != nil {
		i.t.Fatalf("Compose command failed: %v", innerErr)
	}
}

func (i *Test) prepareShutdownHandlers() {
	done := make(chan struct{})
	i.shutdown = func() {
		close(done)
		i.StopRPC()
		if i.rpcClient != nil {
			i.rpcClient.Close()
		}
		i.runComposeCommand("down", "-v")
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
		i.runComposeCommand("down", "rpc", "-v")
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
