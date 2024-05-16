# Changelog

## Unreleased

### Added
* Transactions will now be stored in a database rather than in memory ([#174](https://github.com/stellar/soroban-rpc/pull/174)).

You can opt-in to longer transaction retention by setting `--transaction-retention-window` / `TRANSACTION_RETENTION_WINDOW` to a higher number of ledgers. This will also retain corresponding number of ledgers in the database. Keep in mind, of course, that this will cause an increase in disk usage for the growing database.

* There is a new `getTransactions` endpoint with the following API ([#136](https://github.com/stellar/soroban-rpc/pull/136)):

```typescript
interface Request {
  startLedger: number; // uint32
  pagination?: {
    cursor?: string;
    limit?:  number; // uint
  }
}

interface Response {
  transactions: Transaction[];
  latestLedger: number;               // uint32
  latestLedgerCloseTimestamp: number; // int64
  oldestLedger: number;               // uint32
  oldestLedgerCloseTimestamp: number; // int64
  cursor: string;
}

interface Transaction {
  status: boolean;          // whether or not the transaction succeeded
	applicationOrder: number; // int32, index of the transaction in the ledger
	feeBump: boolean;         // if it's a fee-bump transaction
	envelopeXdr: string;      // TransactionEnvelope XDR
	resultXdr: string;        // TransactionResult XDR
	resultMetaXdr: string;    // TransactionMeta XDR
	ledger: number;           // uint32, ledger sequence with this transaction
	createdAt: int64;         // int64, UNIX timestamp the transaction's inclusion
	diagnosticEventsXdr?: string[]; // if failed, DiagnosticEvent XDRs
}
```


## [v21.1.0](https://github.com/stellar/soroban-rpc/compare/v21.0.1...v21.1.0)

### Added
* A new `getVersionInfo` RPC endpoint providing versioning info ([#132](https://github.com/stellar/soroban-rpc/pull/132)):

```typescript
interface getVersionInfo {
  version: string;
  commit_hash: string;
  build_time_stamp: string;
  captive_core_version: string;
  protocol_version: number; // uint32
}
```

### Fixed
* Deadlock on events ingestion error ([#167](https://github.com/stellar/soroban-rpc/pull/167)).
* Correctly report row iteration errors in `StreamAllLedgers` ([#168](https://github.com/stellar/soroban-rpc/pull/168)).
* Increase default ingestion timeout ([#169](https://github.com/stellar/soroban-rpc/pull/169)).
* Surface an ignored error in `getRawLedgerEntries()` ([#170](https://github.com/stellar/soroban-rpc/pull/170)).


# Formatting Guidelines

This outlines the formatting expectations for the CHANGELOG.md file.

## [vMajor.Minor.Patch](GitHub compare diff to last version)
If necessary, drop a summary here (e.g. "This release supports Protocol 420.")

### Breaking Changes come first
* This is a pull request description and it should be quite detailed if it's a breaking change. Ideally, you would even include a bit of helpful notes on how to migrate/upgrade if that's necessary. For example, if an API changes, you should provide deep detail on the delta.

### Added stuff next
* Anything added should have a details on its schema or command line arguments ([#NNNN](link to github pr)).

### Fixed bugs last
* Be sure you describe who is affected and how.
