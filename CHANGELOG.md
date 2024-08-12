# Changelog

## Unreleased

### Added
* Add support for unpacked JSON responses of base64-encoded XDR fields via a new, optional parameter. When omitted, the behavior does not change and we encode fields as base64.
```typescript
xdrFormat?: "" | "base64" | "json"
```
  - `getTransaction`
  - `getTransactions`
  - `getLedgerEntry`
  - `getLedgerEntries`
  - `getEvents`
  - `sendTransaction`
  - `simulateTransaction`

There are new field names for the JSONified versions of XDR structures. Any field with an `Xdr` suffix (e.g., `resultXdr` in `getTransaction()`) will be replaced with one that has a `Json` suffix (e.g., `resultJson`) that is a JSON object verbosely and completely describing the XDR structure.

Certain XDR-encoded fields do not have an `Xdr` suffix, but those also have a `*Json` equivalent and are listed below:
  * _getEvents_: `topic` -> `topicJson`, `value` -> `valueJson`
  * _getLedgerEntries_: `key` -> `keyJson`, `xdr` -> `dataJson`
  * _getLedgerEntry_: `xdr` -> `entryJson`
  * _simulateTransaction_: `transactionData`, `events`, `results.auth`,
    `restorePreamble.transactionData`, `stateChanges.key|before|after` all have a
    `Json` suffix, and `results.xdr` is now `results.returnValueJson`

### Fixed
* Improve performance of `getVersionInfo` and `getNetwork` ([#198](https://github.com/stellar/soroban-rpc/pull/198)).


## [v21.4.1](https://github.com/stellar/soroban-rpc/compare/v21.4.0...v21.4.1)

### Fixed
* Fix parsing of the `--log-format` parameter ([#252](https://github.com/stellar/soroban-rpc/pull/252))


## [v21.4.0](https://github.com/stellar/soroban-rpc/compare/v21.2.0...v21.4.0)

### Added
* Transactions will now be stored in a database rather than in memory ([#174](https://github.com/stellar/soroban-rpc/pull/174)).

You can opt-in to longer transaction retention by setting `--transaction-retention-window` / `TRANSACTION_RETENTION_WINDOW` to a higher number of ledgers. This will also retain corresponding number of ledgers in the database. Keep in mind, of course, that this will cause an increase in disk usage for the growing database.

* Unify transaction and event retention windows ([#234](https://github.com/stellar/soroban-rpc/pull/234)).
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
  transactions: Transaction[];        // see below
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

### Fixed
* Logging and typo fixes in ([#238](https://github.com/stellar/soroban-rpc/pull/238)).
* Fix calculation of ledger ranges across endpoints ([#217](https://github.com/stellar/soroban-rpc/pull/217)).


## [v21.2.0](https://github.com/stellar/soroban-rpc/compare/v21.1.0...v21.2.0)

### Added
* Dependencies have been updated (`stellar/go`) to enable `ENABLE_DIAGNOSTICS_FOR_TX_SUBMISSION` by default ([#179](https://github.com/stellar/soroban-rpc/pull/179)).

### Fixed
* The Captive Core path is supplied correctly for TOML generation ([#178](https://github.com/stellar/soroban-rpc/pull/178)).


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
