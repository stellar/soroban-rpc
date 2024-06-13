package test

import (
	"bytes"
	"context"
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
	"github.com/stellar/go/txnbuild"
	"github.com/stretchr/testify/require"

	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/ledgerbucketwindow"
	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/methods"

	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/config"
	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/daemon"
)

const (
	StandaloneNetworkPassphrase = "Standalone Network ; February 2017"
	MaxSupportedProtocolVersion = 21
	stellarCorePort             = 11626
	StellarCoreArchivePort      = 1570
	goModFile                   = "go.mod"

	friendbotURL = "http://localhost:8000/friendbot"
	// Needed when Core is run with ARTIFICIALLY_ACCELERATE_TIME_FOR_TESTING=true
	checkpointFrequency    = 8
	sorobanRPCPort         = 8000
	adminPort              = 8080
	helloWorldContractPath = "../../../../wasms/test_hello_world.wasm"
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

	composePath string // docker compose yml file

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
	shutdownCalls []func()
}

func NewTest(t *testing.T, cfg *TestConfig) *Test {
	if os.Getenv("SOROBAN_RPC_INTEGRATION_TESTS_ENABLED") == "" {
		t.Skip("skipping integration test: SOROBAN_RPC_INTEGRATION_TESTS_ENABLED not set")
	}
	i := &Test{
		t:           t,
		composePath: findDockerComposePath(),
	}

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
		cmd := i.getComposeCommand("logs", "-f", "rpc")
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

func (i *Test) sorobanRPCURL() string {
	return fmt.Sprintf("http://localhost:%d", sorobanRPCPort)
}

func (i *Test) adminURL() string {
	return fmt.Sprintf("http://localhost:%d", adminPort)
}

func (i *Test) waitForCheckpoint() {
	i.t.Log("Waiting for core to be up...")
	for t := 30 * time.Second; t >= 0; t -= time.Second {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		info, err := i.coreClient.Info(ctx)
		cancel()
		if err != nil {
			i.t.Logf("could not obtain info response: %v", err)
			time.Sleep(time.Second)
			continue
		}
		if info.Info.Ledger.Num <= checkpointFrequency {
			i.t.Logf("checkpoint not reached yet: %v", info)
			time.Sleep(time.Second)
			continue
		}
		return
	}
	i.t.Fatal("Core could not reach checkpoint ledger after 30s")
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

	captiveCoreConfigPath := path.Join(i.composePath, "captive-core-integration-tests.cfg")
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
		"FRIENDBOT_URL":                  friendbotURL,
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

	// This is needed because if https://github.com/creachadair/jrpc2/issues/118
	refreshClient := func() {
		if i.rpcClient != nil {
			i.rpcClient.Close()
		}
		ch := jhttp.NewChannel(i.sorobanRPCURL(), nil)
		i.rpcClient = jrpc2.NewClient(ch, nil)
	}

	var result methods.HealthCheckResult
	for t := 30; t >= 0; t-- {
		refreshClient()
		err := i.rpcClient.CallResult(context.Background(), "getHealth", nil, &result)
		if err == nil {
			if result.Status == "healthy" {
				i.t.Log("RPC is healthy")
				return
			}
		}
		i.t.Log("RPC still unhealthy", err, result.Status)
		time.Sleep(time.Second)
	}

	i.t.Fatal("RPC failed to get healthy in 30 seconds")
}

func (i *Test) createRPCContainerMountDir(rpcConfig map[string]string) string {
	mountDir := i.t.TempDir()
	// Get old version of captive-core-integration-tests.cfg
	var out bytes.Buffer
	cmd := exec.Command("git", "show", fmt.Sprintf("v%s:./captive-core-integration-tests.cfg", i.rpcContainerVersion))
	cmd.Stdout = &out
	cmd.Stderr = os.Stderr
	cmd.Dir = i.composePath
	require.NoError(i.t, cmd.Run())

	// replace ADDRESS="localhost" by ADDRESS="core", so that the container can find core
	captiveCoreCfgContents := strings.Replace(out.String(), `ADDRESS="localhost"`, `ADDRESS="core"`, -1)
	err := os.WriteFile(filepath.Join(mountDir, "stellar-core-integration-tests.cfg"), []byte(captiveCoreCfgContents), 0666)
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
	integrationYaml := filepath.Join(i.composePath, "docker-compose.yml")
	configFiles := []string{"-f", integrationYaml}
	if i.runRPCInContainer() {
		rpcYaml := filepath.Join(i.composePath, "docker-compose.rpc.yml")
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
	i.t.Log("Running", cmd.Env, cmd.Args)
	out, innerErr := cmd.Output()
	if exitErr, ok := innerErr.(*exec.ExitError); ok {
		fmt.Printf("stdout:\n%s\n", string(out))
		fmt.Printf("stderr:\n%s\n", string(exitErr.Stderr))
	}

	if innerErr != nil {
		i.t.Fatalf("Compose command failed: %v", innerErr)
	}
}

func (i *Test) prepareShutdownHandlers() {
	done := make(chan struct{})
	i.shutdownCalls = append(i.shutdownCalls,
		func() {
			close(done)
			i.StopRPC()
			if i.rpcClient != nil {
				i.rpcClient.Close()
			}
			i.runComposeCommand("down", "-v")
			if i.rpcContainerLogsCommand != nil {
				i.rpcContainerLogsCommand.Wait()
			}
		},
	)

	// Register cleanup handlers (on panic and ctrl+c) so the containers are
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
		// run them in the opposite order in which they where added
		for callI := len(i.shutdownCalls) - 1; callI >= 0; callI-- {
			i.shutdownCalls[callI]()
		}
	})
}

// Wait for core to be up and manually close the first ledger
func (i *Test) waitForCore() {
	i.t.Log("Waiting for core to be up...")
	for t := 30 * time.Second; t >= 0; t -= time.Second {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, err := i.coreClient.Info(ctx)
		cancel()
		if err != nil {
			i.t.Logf("could not obtain info response: %v", err)
			time.Sleep(time.Second)
			continue
		}
		break
	}

	i.UpgradeProtocol(i.protocolVersion)

	for t := 0; t < 5; t++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		info, err := i.coreClient.Info(ctx)
		cancel()
		if err != nil || !info.IsSynced() {
			i.t.Logf("Core is still not synced: %v %v", err, info)
			time.Sleep(time.Second)
			continue
		}
		i.t.Log("Core is up.")
		return
	}
	i.t.Fatal("Core could not sync after 30s")
}

// UpgradeProtocol arms Core with upgrade and blocks until protocol is upgraded.
func (i *Test) UpgradeProtocol(version uint32) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	err := i.coreClient.Upgrade(ctx, int(version))
	cancel()
	if err != nil {
		i.t.Fatalf("could not upgrade protocol: %v", err)
	}

	for t := 0; t < 10; t++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		info, err := i.coreClient.Info(ctx)
		cancel()
		if err != nil {
			i.t.Logf("could not obtain info response: %v", err)
			time.Sleep(time.Second)
			continue
		}

		if info.Info.Ledger.Version == int(version) {
			i.t.Logf("Protocol upgraded to: %d", info.Info.Ledger.Version)
			return
		}
		time.Sleep(time.Second)
	}

	i.t.Fatalf("could not upgrade protocol in 10s")
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

// Cluttering code with if err != nil is absolute nonsense.
func panicIf(err error) {
	if err != nil {
		panic(err)
	}
}

// findProjectRoot iterates upward on the directory until go.mod file is found.
func findProjectRoot(current string) string {
	// Lets you check if a particular directory contains a file.
	directoryContainsFilename := func(dir string, filename string) bool {
		files, innerErr := os.ReadDir(dir)
		panicIf(innerErr)

		for _, file := range files {
			if file.Name() == filename {
				return true
			}
		}
		return false
	}
	var err error

	// In either case, we try to walk up the tree until we find "go.mod",
	// which we hope is the root directory of the project.
	for !directoryContainsFilename(current, goModFile) {
		current, err = filepath.Abs(filepath.Join(current, ".."))

		// FIXME: This only works on *nix-like systems.
		if err != nil || filepath.Base(current)[0] == filepath.Separator {
			fmt.Println("Failed to establish project root directory.")
			panic(err)
		}
	}
	return current
}

// findDockerComposePath performs a best-effort attempt to find the project's
// Docker Compose files.
func findDockerComposePath() string {
	current, err := os.Getwd()
	panicIf(err)

	//
	// We have a primary and backup attempt for finding the necessary docker
	// files: via $GOPATH and via local directory traversal.
	//

	if gopath := os.Getenv("GOPATH"); gopath != "" {
		monorepo := filepath.Join(gopath, "src", "github.com", "stellar", "soroban-rpc")
		if _, err = os.Stat(monorepo); !os.IsNotExist(err) {
			current = monorepo
		}
	}

	current = findProjectRoot(current)

	// Directly jump down to the folder that should contain the configs
	return filepath.Join(current, "cmd", "soroban-rpc", "internal", "test")
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
