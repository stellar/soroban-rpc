# Changelog

## Unreleased
n/a


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
