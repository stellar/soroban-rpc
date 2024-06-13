package test

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
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
	stellarCoreArchiveHost      = "localhost:1570"
	goModFile                   = "go.mod"

	friendbotURL = "http://localhost:8000/friendbot"
	// Needed when Core is run with ARTIFICIALLY_ACCELERATE_TIME_FOR_TESTING=true
	checkpointFrequency    = 8
	sorobanRPCPort         = 8000
	adminPort              = 8080
	helloWorldContractPath = "../../../../wasms/test_hello_world.wasm"
)

type TestConfig struct {
	historyArchiveProxyCallback func(*http.Request)
	ProtocolVersion             uint32
	UseRealRPCVersion           string
	UseSQLitePath               string
}

type Test struct {
	t *testing.T

	composePath string // docker compose yml file

	protocolVersion uint32

	rpcContainerVersion string
	rpcConfigMountDir   string
	rpcSQLiteMountDir   string

	daemon *daemon.Daemon

	historyArchiveProxy         *httptest.Server
	historyArchiveProxyCallback func(*http.Request)

	coreClient *stellarcore.Client

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
		i.rpcContainerVersion = cfg.UseRealRPCVersion
		i.historyArchiveProxyCallback = cfg.historyArchiveProxyCallback
		i.protocolVersion = cfg.ProtocolVersion
		sqlLitePath = cfg.UseSQLitePath
	}

	if i.protocolVersion == 0 {
		// Default to the maximum supported protocol version
		i.protocolVersion = GetCoreMaxSupportedProtocol()
	}

	proxy := httputil.NewSingleHostReverseProxy(&url.URL{Scheme: "http", Host: stellarCoreArchiveHost})

	i.historyArchiveProxy = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if i.historyArchiveProxyCallback != nil {
			i.historyArchiveProxyCallback(r)
		}
		proxy.ServeHTTP(w, r)
	}))

	rpcCfg := i.getRPConfig(sqlLitePath)
	if i.rpcContainerVersion != "" {
		i.rpcConfigMountDir = i.createRPCContainerMountDir(rpcCfg)
	}
	i.runComposeCommand("up", "--detach", "--quiet-pull", "--no-color")
	i.prepareShutdownHandlers()
	i.coreClient = &stellarcore.Client{URL: "http://localhost:" + strconv.Itoa(stellarCorePort)}
	i.waitForCore()
	i.waitForCheckpoint()
	if i.rpcContainerVersion == "" {
		i.daemon = i.createDaemon(rpcCfg)
		go i.daemon.Run()
	}
	i.waitForRPC()

	return i
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

	// Container default path
	coreBinaryPath := "/usr/bin/stellar-core"
	if i.rpcContainerVersion == "" {
		coreBinaryPath = os.Getenv("SOROBAN_RPC_INTEGRATION_TESTS_CAPTIVE_CORE_BIN")
		if coreBinaryPath == "" {
			i.t.Fatal("missing SOROBAN_RPC_INTEGRATION_TESTS_CAPTIVE_CORE_BIN")
		}
	}

	// Out of the container default file
	captiveCoreConfigPath := path.Join(i.composePath, "captive-core-integration-tests.cfg")
	archiveProxyURL := i.historyArchiveProxy.URL
	stellarCoreURL := fmt.Sprintf("http://localhost:%d", stellarCorePort)
	bindHost := "localhost"
	if i.rpcContainerVersion != "" {
		// The file will be inside the container
		captiveCoreConfigPath = "/stellar-core.cfg"
		// the archive needs to be accessed from the container
		url, err := url.Parse(i.historyArchiveProxy.URL)
		require.NoError(i.t, err)
		_, port, err := net.SplitHostPort(url.Host)
		require.NoError(i.t, err)
		url.Host = net.JoinHostPort("host.docker.internal", port)
		archiveProxyURL = url.String()
		// The container needs to listen on all interfaces, not just localhost
		bindHost = "0.0.0.0"
		// The container needs to use the sqlite mount point
		i.rpcSQLiteMountDir = filepath.Dir(sqlitePath)
		sqlitePath = "/db/" + filepath.Base(sqlitePath)
		stellarCoreURL = fmt.Sprintf("http://core:%d", stellarCorePort)
	}

	return map[string]string{
		"ENDPOINT":                       fmt.Sprintf("%s:%d", bindHost, sorobanRPCPort),
		"ADMIN_ENDPOINT":                 fmt.Sprintf("%s:%d", bindHost, adminPort),
		"STELLAR_CORE_URL":               stellarCoreURL,
		"CORE_REQUEST_TIMEOUT":           "2s",
		"STELLAR_CORE_BINARY_PATH":       coreBinaryPath,
		"CAPTIVE_CORE_CONFIG_PATH":       captiveCoreConfigPath,
		"CAPTIVE_CORE_STORAGE_PATH":      i.t.TempDir(),
		"STELLAR_CAPTIVE_CORE_HTTP_PORT": "0",
		"FRIENDBOT_URL":                  friendbotURL,
		"NETWORK_PASSPHRASE":             StandaloneNetworkPassphrase,
		"HISTORY_ARCHIVE_URLS":           archiveProxyURL,
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
	i.t.Log("Waiting for RPC to be up...")

	ch := jhttp.NewChannel(i.sorobanRPCURL(), nil)
	client := jrpc2.NewClient(ch, nil)
	defer client.Close()

	var result methods.HealthCheckResult
	success := false
	for t := 30; t >= 0; t-- {
		err := client.CallResult(context.Background(), "getHealth", nil, &result)
		if err == nil {
			if result.Status == "healthy" {
				success = true
				break
			}
		}
		i.t.Log("RPC still unhealthy")
		time.Sleep(time.Second)
	}
	if !success {
		i.t.Fatal("RPC failed to get healthy in 30 seconds")
	}
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

// Runs a docker-compose command applied to the above configs
func (i *Test) runComposeCommand(args ...string) {
	integrationYaml := filepath.Join(i.composePath, "docker-compose.yml")
	configFiles := []string{"-f", integrationYaml}
	if i.rpcContainerVersion != "" {
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
	if i.rpcContainerVersion != "" {
		cmd.Env = append(
			cmd.Env,
			"RPC_IMAGE_TAG="+i.rpcContainerVersion,
			"RPC_CONFIG_MOUNT_DIR="+i.rpcConfigMountDir,
			"RPC_SQLITE_MOUNT_DIR="+i.rpcSQLiteMountDir,
		)
	}
	if len(cmd.Env) > 0 {
		cmd.Env = append(cmd.Env, os.Environ()...)
	}

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
	i.shutdownCalls = append(i.shutdownCalls,
		func() {
			if i.daemon != nil {
				i.daemon.Close()
			}
			if i.historyArchiveProxy != nil {
				i.historyArchiveProxy.Close()
			}
			i.runComposeCommand("down", "-v")
		},
	)

	// Register cleanup handlers (on panic and ctrl+c) so the containers are
	// stopped even if ingestion or testing fails.
	i.t.Cleanup(i.Shutdown)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		i.Shutdown()
		os.Exit(int(syscall.SIGTERM))
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
