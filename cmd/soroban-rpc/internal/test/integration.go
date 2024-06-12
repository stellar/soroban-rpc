package test

import (
	"context"
	"fmt"
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
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stellar/go/clients/stellarcore"
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/txnbuild"
	"github.com/stretchr/testify/require"

	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/ledgerbucketwindow"

	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/config"
	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/daemon"
	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/db"
)

const (
	StandaloneNetworkPassphrase = "Standalone Network ; February 2017"
	maxSupportedProtocolVersion = 21
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

type daemonOrCommand struct {
	daemon  *daemon.Daemon
	command *exec.Cmd
}

type Test struct {
	t *testing.T

	composePath string // docker compose yml file

	protocolVersion uint32

	rpcInstance daemonOrCommand

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
	coreBinaryPath := os.Getenv("SOROBAN_RPC_INTEGRATION_TESTS_CAPTIVE_CORE_BIN")
	if coreBinaryPath == "" {
		t.Fatal("missing SOROBAN_RPC_INTEGRATION_TESTS_CAPTIVE_CORE_BIN")
	}

	i := &Test{
		t:           t,
		composePath: findDockerComposePath(),
	}

	i.masterAccount = &txnbuild.SimpleAccount{
		AccountID: i.MasterKey().Address(),
		Sequence:  0,
	}
	realRPCVersion := ""
	sqlLitePath := ""
	if cfg != nil {
		i.historyArchiveProxyCallback = cfg.historyArchiveProxyCallback
		i.protocolVersion = cfg.ProtocolVersion
		realRPCVersion = cfg.UseRealRPCVersion
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

	i.runComposeCommand("up", "--detach", "--quiet-pull", "--no-color")
	i.prepareShutdownHandlers()
	i.coreClient = &stellarcore.Client{URL: "http://localhost:" + strconv.Itoa(stellarCorePort)}
	i.waitForCore()
	i.waitForCheckpoint()
	i.launchRPC(coreBinaryPath, realRPCVersion, sqlLitePath)

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

func (i *Test) launchRPC(coreBinaryPath string, realRPCVersion string, sqlitePath string) {
	var config config.Config
	cmd := &cobra.Command{}
	if err := config.AddFlags(cmd); err != nil {
		i.t.FailNow()
	}
	if err := config.SetValues(func(string) (string, bool) { return "", false }); err != nil {
		i.t.FailNow()
	}
	if sqlitePath == "" {
		sqlitePath = path.Join(i.t.TempDir(), "soroban_rpc.sqlite")
	}
	env := map[string]string{
		"ENDPOINT":                       fmt.Sprintf("localhost:%d", sorobanRPCPort),
		"ADMIN_ENDPOINT":                 fmt.Sprintf("localhost:%d", adminPort),
		"STELLAR_CORE_URL":               "http://localhost:" + strconv.Itoa(stellarCorePort),
		"CORE_REQUEST_TIMEOUT":           "2s",
		"STELLAR_CORE_BINARY_PATH":       coreBinaryPath,
		"CAPTIVE_CORE_CONFIG_PATH":       path.Join(i.composePath, "captive-core-integration-tests.cfg"),
		"CAPTIVE_CORE_STORAGE_PATH":      i.t.TempDir(),
		"STELLAR_CAPTIVE_CORE_HTTP_PORT": "0",
		"FRIENDBOT_URL":                  friendbotURL,
		"NETWORK_PASSPHRASE":             StandaloneNetworkPassphrase,
		"HISTORY_ARCHIVE_URLS":           i.historyArchiveProxy.URL,
		"LOG_LEVEL":                      "debug",
		"DB_PATH":                        sqlitePath,
		"INGESTION_TIMEOUT":              "10m",
		"EVENT_LEDGER_RETENTION_WINDOW":  strconv.Itoa(ledgerbucketwindow.OneDayOfLedgers),
		"TRANSACTION_RETENTION_WINDOW":   strconv.Itoa(ledgerbucketwindow.OneDayOfLedgers),
		"CHECKPOINT_FREQUENCY":           strconv.Itoa(checkpointFrequency),
		"MAX_HEALTHY_LEDGER_LATENCY":     "10s",
		"PREFLIGHT_ENABLE_DEBUG":         "true",
	}

	if realRPCVersion != "" {
		i.rpcInstance.command = i.compileAndStartRPC(env, realRPCVersion)
	} else {
		i.rpcInstance.daemon = i.createDaemon(env)
		go i.rpcInstance.daemon.Run()
	}

	// wait for the storage to catch up for 1 minute
	info, err := i.coreClient.Info(context.Background())
	if err != nil {
		i.t.Fatalf("cannot obtain latest ledger from core: %v", err)
	}
	targetLedgerSequence := uint32(info.Info.Ledger.Num)

	dbConn, err := db.OpenSQLiteDB(sqlitePath)
	require.NoError(i.t, err)
	reader := db.NewLedgerEntryReader(dbConn)
	success := false
	for t := 30; t >= 0; t -= 1 {
		sequence, err := reader.GetLatestLedgerSequence(context.Background())
		if err != nil {
			if err != db.ErrEmptyDB {
				i.t.Fatalf("cannot access ledger entry storage: %v", err)
			}
		} else {
			if sequence >= targetLedgerSequence {
				success = true
				break
			}
		}
		time.Sleep(time.Second)
	}
	if !success {
		i.t.Fatal("LedgerEntryStorage failed to sync in 1 minute")
	}
}

func (i *Test) compileAndStartRPC(env map[string]string, version string) *exec.Cmd {
	newRPCDir := i.t.TempDir()

	Command := func(name string, arg ...string) *exec.Cmd {
		cmd := exec.Command(name, arg...)
		cmd.Dir = newRPCDir
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		cmd.Stdin = os.Stdin
		return cmd
	}

	// Clone
	rootDir := path.Join(i.composePath, "..", "..", "..", "..")
	err := Command("git", "clone", "--depth", "1", "--branch", version, "file://"+rootDir, newRPCDir).Run()
	require.NoError(i.t, err)

	// Compile
	cmd := Command("make", "build-libpreflight")
	require.NoError(i.t, cmd.Run())
	cmd = Command("go", "build", "./cmd/soroban-rpc")
	require.NoError(i.t, cmd.Run())

	// Run
	cmd = Command(path.Join(newRPCDir, "soroban-rpc"))
	for k, v := range env {
		cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", k, v))
	}
	require.NoError(i.t, cmd.Start())

	return cmd
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

	cmdline := append([]string{"-f", integrationYaml}, args...)
	cmd := exec.Command("docker-compose", cmdline...)

	if img := os.Getenv("SOROBAN_RPC_INTEGRATION_TESTS_DOCKER_IMG"); img != "" {
		cmd.Env = os.Environ()
		cmd.Env = append(
			cmd.Environ(),
			fmt.Sprintf("CORE_IMAGE=%s", img),
		)
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
			if i.rpcInstance.daemon != nil {
				i.rpcInstance.daemon.Close()
			}
			if i.rpcInstance.command != nil {
				require.NoError(i.t, i.rpcInstance.command.Process.Kill())
				i.rpcInstance.command.Wait()
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
		return maxSupportedProtocolVersion
	}
	version, err := strconv.ParseUint(str, 10, 32)
	if err != nil {
		return maxSupportedProtocolVersion
	}

	return uint32(version)
}
