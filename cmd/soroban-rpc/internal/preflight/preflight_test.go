package preflight

import (
	"context"
	"crypto/sha256"
	"os"
	"path"
	"runtime"
	"testing"

	"github.com/stellar/go/support/log"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/require"

	"github.com/stellar/soroban-tools/cmd/soroban-rpc/internal/db"
)

var mockContractID = xdr.Hash{0xa, 0xb, 0xc}
var mockContractHash = xdr.Hash{0xd, 0xe, 0xf}

var contractCostParams = func() *xdr.ContractCostParams {
	var result xdr.ContractCostParams

	for i := 0; i < 23; i++ {
		result = append(result, xdr.ContractCostParamEntry{
			Ext:        xdr.ExtensionPoint{},
			ConstTerm:  0,
			LinearTerm: 0,
		})
	}

	return &result
}()

var mockLedgerEntriesWithoutTTLs = []xdr.LedgerEntry{
	{
		LastModifiedLedgerSeq: 1,
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeContractData,
			ContractData: &xdr.ContractDataEntry{
				Contract: xdr.ScAddress{
					Type:       xdr.ScAddressTypeScAddressTypeContract,
					ContractId: &mockContractID,
				},
				Key: xdr.ScVal{
					Type: xdr.ScValTypeScvLedgerKeyContractInstance,
				},
				Durability: xdr.ContractDataDurabilityPersistent,
				Val: xdr.ScVal{
					Type: xdr.ScValTypeScvContractInstance,
					Instance: &xdr.ScContractInstance{
						Executable: xdr.ContractExecutable{
							Type:     xdr.ContractExecutableTypeContractExecutableWasm,
							WasmHash: &mockContractHash,
						},
						Storage: nil,
					},
				},
			},
		},
	},
	{
		LastModifiedLedgerSeq: 2,
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeContractCode,
			ContractCode: &xdr.ContractCodeEntry{
				Hash: mockContractHash,
				Code: helloWorldContract,
			},
		},
	},
	{
		LastModifiedLedgerSeq: 2,
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeConfigSetting,
			ConfigSetting: &xdr.ConfigSettingEntry{
				ConfigSettingId: xdr.ConfigSettingIdConfigSettingContractComputeV0,
				ContractCompute: &xdr.ConfigSettingContractComputeV0{
					LedgerMaxInstructions:           100000000,
					TxMaxInstructions:               100000000,
					FeeRatePerInstructionsIncrement: 1,
					TxMemoryLimit:                   100000000,
				},
			},
		},
	},
	{
		LastModifiedLedgerSeq: 2,
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeConfigSetting,
			ConfigSetting: &xdr.ConfigSettingEntry{
				ConfigSettingId: xdr.ConfigSettingIdConfigSettingContractLedgerCostV0,
				ContractLedgerCost: &xdr.ConfigSettingContractLedgerCostV0{
					LedgerMaxReadLedgerEntries:     100,
					LedgerMaxReadBytes:             100,
					LedgerMaxWriteLedgerEntries:    100,
					LedgerMaxWriteBytes:            100,
					TxMaxReadLedgerEntries:         100,
					TxMaxReadBytes:                 100,
					TxMaxWriteLedgerEntries:        100,
					TxMaxWriteBytes:                100,
					FeeReadLedgerEntry:             100,
					FeeWriteLedgerEntry:            100,
					FeeRead1Kb:                     100,
					BucketListTargetSizeBytes:      100,
					WriteFee1KbBucketListLow:       1,
					WriteFee1KbBucketListHigh:      1,
					BucketListWriteFeeGrowthFactor: 1,
				},
			},
		},
	},
	{
		LastModifiedLedgerSeq: 2,
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeConfigSetting,
			ConfigSetting: &xdr.ConfigSettingEntry{
				ConfigSettingId: xdr.ConfigSettingIdConfigSettingContractHistoricalDataV0,
				ContractHistoricalData: &xdr.ConfigSettingContractHistoricalDataV0{
					FeeHistorical1Kb: 100,
				},
			},
		},
	},
	{
		LastModifiedLedgerSeq: 2,
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeConfigSetting,
			ConfigSetting: &xdr.ConfigSettingEntry{
				ConfigSettingId: xdr.ConfigSettingIdConfigSettingContractEventsV0,
				ContractEvents: &xdr.ConfigSettingContractEventsV0{
					TxMaxContractEventsSizeBytes: 10000,
					FeeContractEvents1Kb:         1,
				},
			},
		},
	},
	{
		LastModifiedLedgerSeq: 2,
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeConfigSetting,
			ConfigSetting: &xdr.ConfigSettingEntry{
				ConfigSettingId: xdr.ConfigSettingIdConfigSettingContractBandwidthV0,
				ContractBandwidth: &xdr.ConfigSettingContractBandwidthV0{
					LedgerMaxTxsSizeBytes: 100000,
					TxMaxSizeBytes:        1000,
					FeeTxSize1Kb:          1,
				},
			},
		},
	},
	{
		LastModifiedLedgerSeq: 2,
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeConfigSetting,
			ConfigSetting: &xdr.ConfigSettingEntry{
				ConfigSettingId: xdr.ConfigSettingIdConfigSettingStateArchival,
				StateArchivalSettings: &xdr.StateArchivalSettings{
					MaxEntryTtl:                    100,
					MinTemporaryTtl:                100,
					MinPersistentTtl:               100,
					PersistentRentRateDenominator:  100,
					TempRentRateDenominator:        100,
					MaxEntriesToArchive:            100,
					BucketListSizeWindowSampleSize: 100,
					EvictionScanSize:               100,
				},
			},
		},
	},
	{
		LastModifiedLedgerSeq: 2,
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeConfigSetting,
			ConfigSetting: &xdr.ConfigSettingEntry{
				ConfigSettingId: xdr.ConfigSettingIdConfigSettingContractCostParamsCpuInstructions,
				// Obtained with TestGetLedgerEntryConfigSettings
				ContractCostParamsCpuInsns: contractCostParams,
			},
		},
	},
	{
		LastModifiedLedgerSeq: 2,
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeConfigSetting,
			ConfigSetting: &xdr.ConfigSettingEntry{
				ConfigSettingId: xdr.ConfigSettingIdConfigSettingContractCostParamsMemoryBytes,
				// Obtained with TestGetLedgerEntryConfigSettings
				ContractCostParamsMemBytes: contractCostParams,
			},
		},
	},
}

// Adds ttl entries to mockLedgerEntriesWithoutTTLs
var mockLedgerEntries = func() []xdr.LedgerEntry {
	result := make([]xdr.LedgerEntry, 0, len(mockLedgerEntriesWithoutTTLs))
	for _, entry := range mockLedgerEntriesWithoutTTLs {
		result = append(result, entry)

		if entry.Data.Type == xdr.LedgerEntryTypeContractData || entry.Data.Type == xdr.LedgerEntryTypeContractCode {
			key, err := entry.LedgerKey()
			if err != nil {
				panic(err)
			}
			bin, err := key.MarshalBinary()
			if err != nil {
				panic(err)
			}
			ttlEntry := xdr.LedgerEntry{
				LastModifiedLedgerSeq: entry.LastModifiedLedgerSeq,
				Data: xdr.LedgerEntryData{
					Type: xdr.LedgerEntryTypeTtl,
					Ttl: &xdr.TtlEntry{
						KeyHash: sha256.Sum256(bin),
						// Make sure it doesn't ttl
						LiveUntilLedgerSeq: 1000,
					},
				},
			}
			result = append(result, ttlEntry)
		}
	}
	return result
}()

var helloWorldContract = func() []byte {
	_, filename, _, _ := runtime.Caller(0)
	testDirName := path.Dir(filename)
	contractFile := path.Join(testDirName, "../../../../target/wasm32-unknown-unknown/test-wasms/test_hello_world.wasm")
	ret, err := os.ReadFile(contractFile)
	if err != nil {
		log.Fatalf("unable to read test_hello_world.wasm (%v) please run `make build-test-wasms` at the project root directory", err)
	}
	return ret
}()

type inMemoryLedgerEntryReadTx map[string]xdr.LedgerEntry

func (m inMemoryLedgerEntryReadTx) GetLedgerEntries(keys ...xdr.LedgerKey) ([]db.LedgerKeyAndEntry, error) {
	result := make([]db.LedgerKeyAndEntry, 0, len(keys))
	for _, key := range keys {
		serializedKey, err := key.MarshalBinaryBase64()
		if err != nil {
			return nil, err
		}
		entry, ok := m[serializedKey]
		if !ok {
			continue
		}
		// We don't check the TTL but that's ok for the test
		result = append(result, db.LedgerKeyAndEntry{
			Key:   key,
			Entry: entry,
		})
	}
	return result, nil
}

func newInMemoryLedgerEntryReadTx(entries []xdr.LedgerEntry) (inMemoryLedgerEntryReadTx, error) {
	result := make(map[string]xdr.LedgerEntry, len(entries))
	for _, entry := range entries {
		key, err := entry.LedgerKey()
		if err != nil {
			return inMemoryLedgerEntryReadTx{}, err
		}
		serialized, err := key.MarshalBinaryBase64()
		if err != nil {
			return inMemoryLedgerEntryReadTx{}, err
		}
		result[serialized] = entry
	}
	return result, nil
}

func (m inMemoryLedgerEntryReadTx) GetLatestLedgerSequence() (uint32, error) {
	return 2, nil
}

func (m inMemoryLedgerEntryReadTx) Done() error {
	return nil
}

func getDB(t testing.TB, restartDB bool) *db.DB {
	dbPath := path.Join(t.TempDir(), "soroban_rpc.sqlite")
	dbInstance, err := db.OpenSQLiteDB(dbPath)
	require.NoError(t, err)
	readWriter := db.NewReadWriter(dbInstance, 100, 10000)
	tx, err := readWriter.NewTx(context.Background())
	require.NoError(t, err)
	for _, e := range mockLedgerEntries {
		err := tx.LedgerEntryWriter().UpsertLedgerEntry(e)
		require.NoError(t, err)
	}
	err = tx.Commit(2)
	require.NoError(t, err)
	if restartDB {
		// Restarting the DB resets the ledger entries write-through cache
		require.NoError(t, dbInstance.Close())
		dbInstance, err = db.OpenSQLiteDB(dbPath)
		require.NoError(t, err)
	}
	return dbInstance
}

type preflightParametersDBConfig struct {
	dbInstance   *db.DB
	disableCache bool
}

func getPreflightParameters(t testing.TB, dbConfig *preflightParametersDBConfig) PreflightParameters {
	var ledgerEntryReadTx db.LedgerEntryReadTx
	if dbConfig != nil {
		entryReader := db.NewLedgerEntryReader(dbConfig.dbInstance)
		var err error
		if dbConfig.disableCache {
			ledgerEntryReadTx, err = entryReader.NewTx(context.Background())
		} else {
			ledgerEntryReadTx, err = entryReader.NewCachedTx(context.Background())
		}
		require.NoError(t, err)
	} else {
		var err error
		ledgerEntryReadTx, err = newInMemoryLedgerEntryReadTx(mockLedgerEntries)
		require.NoError(t, err)
	}
	argSymbol := xdr.ScSymbol("world")
	params := PreflightParameters{
		EnableDebug:   true,
		Logger:        log.New(),
		SourceAccount: xdr.MustAddress("GBRPYHIL2CI3FNQ4BXLFMNDLFJUNPU2HY3ZMFSHONUCEOASW7QC7OX2H"),
		OpBody: xdr.OperationBody{Type: xdr.OperationTypeInvokeHostFunction,
			InvokeHostFunctionOp: &xdr.InvokeHostFunctionOp{
				HostFunction: xdr.HostFunction{
					Type: xdr.HostFunctionTypeHostFunctionTypeInvokeContract,
					InvokeContract: &xdr.InvokeContractArgs{
						ContractAddress: xdr.ScAddress{
							Type:       xdr.ScAddressTypeScAddressTypeContract,
							ContractId: &mockContractID,
						},
						FunctionName: "hello",
						Args: []xdr.ScVal{
							{
								Type: xdr.ScValTypeScvSymbol,
								Sym:  &argSymbol,
							},
						},
					},
				},
			}},
		NetworkPassphrase: "foo",
		LedgerEntryReadTx: ledgerEntryReadTx,
		BucketListSize:    200,
	}
	return params
}

func TestGetPreflight(t *testing.T) {
	// in-memory
	params := getPreflightParameters(t, nil)
	result, err := GetPreflight(context.Background(), params)
	require.NoError(t, err)
	require.Empty(t, result.Error)
	require.NoError(t, params.LedgerEntryReadTx.Done())

	// using a restarted db with caching and
	getDB(t, true)
	dbConfig := &preflightParametersDBConfig{
		dbInstance:   getDB(t, true),
		disableCache: false,
	}
	params = getPreflightParameters(t, dbConfig)
	result, err = GetPreflight(context.Background(), params)
	require.NoError(t, err)
	require.Empty(t, result.Error)
	require.NoError(t, params.LedgerEntryReadTx.Done())
	require.NoError(t, dbConfig.dbInstance.Close())
}

func TestGetPreflightDebug(t *testing.T) {
	params := getPreflightParameters(t, nil)
	// Cause an error
	params.OpBody.InvokeHostFunctionOp.HostFunction.InvokeContract.FunctionName = "bar"

	resultWithDebug, err := GetPreflight(context.Background(), params)
	require.NoError(t, err)
	require.NotZero(t, resultWithDebug.Error)
	require.Contains(t, resultWithDebug.Error, "Backtrace")
	require.Contains(t, resultWithDebug.Error, "Event log")
	require.NotContains(t, resultWithDebug.Error, "DebugInfo not available")

	// Disable debug
	params.EnableDebug = false
	resultWithoutDebug, err := GetPreflight(context.Background(), params)
	require.NoError(t, err)
	require.NotZero(t, resultWithoutDebug.Error)
	require.NotContains(t, resultWithoutDebug.Error, "Backtrace")
	require.NotContains(t, resultWithoutDebug.Error, "Event log")
	require.Contains(t, resultWithoutDebug.Error, "DebugInfo not available")
}

type benchmarkDBConfig struct {
	restart      bool
	disableCache bool
}

type benchmarkConfig struct {
	useDB *benchmarkDBConfig
}

func benchmark(b *testing.B, config benchmarkConfig) {
	var dbConfig *preflightParametersDBConfig
	if config.useDB != nil {
		dbConfig = &preflightParametersDBConfig{
			dbInstance:   getDB(b, config.useDB.restart),
			disableCache: config.useDB.disableCache,
		}
	}

	b.ResetTimer()
	b.StopTimer()
	for i := 0; i < b.N; i++ {
		params := getPreflightParameters(b, dbConfig)
		b.StartTimer()
		result, err := GetPreflight(context.Background(), params)
		b.StopTimer()
		require.NoError(b, err)
		require.Empty(b, result.Error)
		require.NoError(b, params.LedgerEntryReadTx.Done())
	}
	if dbConfig != nil {
		require.NoError(b, dbConfig.dbInstance.Close())
	}
}

func BenchmarkGetPreflight(b *testing.B) {
	b.Run("In-memory storage", func(b *testing.B) { benchmark(b, benchmarkConfig{}) })
	b.Run("DB storage", func(b *testing.B) { benchmark(b, benchmarkConfig{useDB: &benchmarkDBConfig{}}) })
	b.Run("DB storage, restarting", func(b *testing.B) { benchmark(b, benchmarkConfig{useDB: &benchmarkDBConfig{restart: true}}) })
	b.Run("DB storage, no cache", func(b *testing.B) { benchmark(b, benchmarkConfig{useDB: &benchmarkDBConfig{disableCache: true}}) })
}
