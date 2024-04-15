extern crate anyhow;
extern crate base64;
extern crate libc;
extern crate sha2;
extern crate soroban_env_host;
extern crate soroban_simulation;

use anyhow::{anyhow, bail, Result};
use sha2::{Digest, Sha256};
use soroban_env_host::storage::EntryWithLiveUntil;
use soroban_env_host::xdr::{
    AccountId, ExtendFootprintTtlOp, Hash, InvokeHostFunctionOp, LedgerEntry, LedgerEntryData,
    LedgerFootprint, LedgerKey, LedgerKeyTtl, OperationBody, ReadXdr, ScErrorCode, ScErrorType,
    SorobanTransactionData, TtlEntry, WriteXdr,
};
use soroban_env_host::{HostError, LedgerInfo, DEFAULT_XDR_RW_LIMITS};
use soroban_simulation::simulation::{
    simulate_extend_ttl_op, simulate_invoke_host_function_op, simulate_restore_op,
    InvokeHostFunctionSimulationResult, LedgerEntryDiff, RestoreOpSimulationResult,
    SimulationAdjustmentConfig,
};
use soroban_simulation::{AutoRestoringSnapshotSource, NetworkConfig, SnapshotSourceWithArchive};
use std::cell::RefCell;
use std::ffi::{CStr, CString};
use std::panic;
use std::ptr::null_mut;
use std::rc::Rc;
use std::{mem, slice};

#[repr(C)]
#[derive(Copy, Clone)]
pub struct CLedgerInfo {
    pub protocol_version: u32,
    pub sequence_number: u32,
    pub timestamp: u64,
    pub network_passphrase: *const libc::c_char,
    pub base_reserve: u32,
}

fn fill_ledger_info(c_ledger_info: CLedgerInfo, network_config: &NetworkConfig) -> LedgerInfo {
    let network_passphrase = from_c_string(c_ledger_info.network_passphrase);
    let mut ledger_info = LedgerInfo {
        protocol_version: c_ledger_info.protocol_version,
        sequence_number: c_ledger_info.sequence_number,
        timestamp: c_ledger_info.timestamp,
        network_id: Sha256::digest(network_passphrase).into(),
        base_reserve: c_ledger_info.base_reserve,
        ..Default::default()
    };
    network_config.fill_config_fields_in_ledger_info(&mut ledger_info);
    ledger_info
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct CXDR {
    pub xdr: *mut libc::c_uchar,
    pub len: libc::size_t,
}

// It would be nicer to derive Default, but we can't. It errors with:
// The trait bound `*mut u8: std::default::Default` is not satisfied
impl Default for CXDR {
    fn default() -> Self {
        CXDR {
            xdr: null_mut(),
            len: 0,
        }
    }
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct CXDRVector {
    pub array: *mut CXDR,
    pub len: libc::size_t,
}

impl Default for CXDRVector {
    fn default() -> Self {
        CXDRVector {
            array: null_mut(),
            len: 0,
        }
    }
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct CXDRDiff {
    pub before: CXDR,
    pub after: CXDR,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct CXDRDiffVector {
    pub array: *mut CXDRDiff,
    pub len: libc::size_t,
}

impl Default for CXDRDiffVector {
    fn default() -> Self {
        CXDRDiffVector {
            array: null_mut(),
            len: 0,
        }
    }
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct CResourceConfig {
    pub instruction_leeway: u64,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct CPreflightResult {
    // Error string in case of error, otherwise null
    pub error: *mut libc::c_char,
    // Error string in case of error, otherwise null
    pub auth: CXDRVector,
    // XDR SCVal
    pub result: CXDR,
    // SorobanTransactionData XDR
    pub transaction_data: CXDR,
    // Minimum recommended resource fee
    pub min_fee: i64,
    // array of XDR ContractEvents
    pub events: CXDRVector,
    pub cpu_instructions: u64,
    pub memory_bytes: u64,
    // SorobanTransactionData XDR for a prerequired RestoreFootprint operation
    pub pre_restore_transaction_data: CXDR,
    // Minimum recommended resource fee for a prerequired RestoreFootprint operation
    pub pre_restore_min_fee: i64,
    // Contains the ledger entry changes which would be caused by the transaction execution
    pub ledger_entry_diff: CXDRDiffVector,
}

impl Default for CPreflightResult {
    fn default() -> Self {
        Self {
            error: CString::new(String::new()).unwrap().into_raw(),
            auth: Default::default(),
            result: Default::default(),
            transaction_data: Default::default(),
            min_fee: 0,
            events: Default::default(),
            cpu_instructions: 0,
            memory_bytes: 0,
            pre_restore_transaction_data: Default::default(),
            pre_restore_min_fee: 0,
            ledger_entry_diff: Default::default(),
        }
    }
}

impl CPreflightResult {
    fn new_from_invoke_host_function(
        invoke_hf_result: InvokeHostFunctionSimulationResult,
        restore_preamble: Option<RestoreOpSimulationResult>,
        error: String,
    ) -> Self {
        let mut result = Self {
            error: string_to_c(error),
            auth: xdr_vec_to_c(invoke_hf_result.auth),
            result: option_xdr_to_c(
                invoke_hf_result
                    .invoke_result
                    .map_or_else(|_| None, |v| Some(v)),
            ),
            min_fee: invoke_hf_result
                .transaction_data
                .as_ref()
                .map_or_else(|| 0, |r| r.resource_fee),
            transaction_data: option_xdr_to_c(invoke_hf_result.transaction_data),
            // TODO: Diagnostic and contract events should be separated in the response
            events: xdr_vec_to_c(invoke_hf_result.diagnostic_events),
            cpu_instructions: invoke_hf_result.simulated_instructions as u64,
            memory_bytes: invoke_hf_result.simulated_memory as u64,
            ledger_entry_diff: ledger_entry_diff_vec_to_c(invoke_hf_result.modified_entries),
            ..Default::default()
        };
        if let Some(p) = restore_preamble {
            result.pre_restore_min_fee = p.transaction_data.resource_fee;
            result.pre_restore_transaction_data = xdr_to_c(p.transaction_data);
        };
        result
    }

    fn new_from_transaction_data(
        transaction_data: Option<SorobanTransactionData>,
        restore_preamble: Option<RestoreOpSimulationResult>,
        error: String,
    ) -> Self {
        let min_fee = transaction_data.as_ref().map_or(0, |d| d.resource_fee);
        let mut result = Self {
            error: string_to_c(error),
            transaction_data: option_xdr_to_c(transaction_data),
            min_fee,
            ..Default::default()
        };
        if let Some(p) = restore_preamble {
            result.pre_restore_min_fee = p.transaction_data.resource_fee;
            result.pre_restore_transaction_data = xdr_to_c(p.transaction_data);
        };
        result
    }
}

#[no_mangle]
pub extern "C" fn preflight_invoke_hf_op(
    handle: libc::uintptr_t, // Go Handle to forward to SnapshotSourceGet and SnapshotSourceHas
    invoke_hf_op: CXDR,      // InvokeHostFunctionOp XDR in base64
    source_account: CXDR,    // AccountId XDR in base64
    ledger_info: CLedgerInfo,
    resource_config: CResourceConfig,
    enable_debug: bool,
) -> *mut CPreflightResult {
    catch_preflight_panic(Box::new(move || {
        preflight_invoke_hf_op_or_maybe_panic(
            handle,
            invoke_hf_op,
            source_account,
            ledger_info,
            resource_config,
            enable_debug,
        )
    }))
}

fn preflight_invoke_hf_op_or_maybe_panic(
    handle: libc::uintptr_t,
    invoke_hf_op: CXDR,   // InvokeHostFunctionOp XDR in base64
    source_account: CXDR, // AccountId XDR in base64
    c_ledger_info: CLedgerInfo,
    resource_config: CResourceConfig,
    enable_debug: bool,
) -> Result<CPreflightResult> {
    let invoke_hf_op =
        InvokeHostFunctionOp::from_xdr(from_c_xdr(invoke_hf_op), DEFAULT_XDR_RW_LIMITS).unwrap();
    let source_account =
        AccountId::from_xdr(from_c_xdr(source_account), DEFAULT_XDR_RW_LIMITS).unwrap();
    let go_storage = Rc::new(GoLedgerStorage::new(handle));
    let network_config = NetworkConfig::load_from_snapshot(go_storage.as_ref())?;
    let ledger_info = fill_ledger_info(c_ledger_info, &network_config);
    let auto_restore_snapshot = Rc::new(AutoRestoringSnapshotSource::new(
        go_storage.clone(),
        &ledger_info,
    )?);

    let mut adjustment_config = SimulationAdjustmentConfig::default_adjustment();
    // It would be reasonable to extend `resource_config` to be compatible with `adjustment_config`
    // in order to let the users customize the resource/fee adjustments in a more granular fashion.
    adjustment_config.instructions.additive_factor = adjustment_config
        .instructions
        .additive_factor
        .max(resource_config.instruction_leeway.min(u32::MAX as u64) as u32);
    // Here we assume that no input auth means that the user requests the recording auth.
    let auth_entries = if invoke_hf_op.auth.is_empty() {
        None
    } else {
        Some(invoke_hf_op.auth.to_vec())
    };
    // Invoke the host function. The user errors should normally be captured in `invoke_hf_result.invoke_result` and
    // this should return Err result for misconfigured ledger.
    let invoke_hf_result = simulate_invoke_host_function_op(
        auto_restore_snapshot.clone(),
        &network_config,
        &adjustment_config,
        &ledger_info,
        invoke_hf_op.host_function,
        auth_entries,
        &source_account,
        rand::Rng::gen(&mut rand::thread_rng()),
        enable_debug,
    )?;
    let maybe_restore_result = match &invoke_hf_result.invoke_result {
        Ok(_) => auto_restore_snapshot.simulate_restore_keys_op(
            &network_config,
            &SimulationAdjustmentConfig::default_adjustment(),
            &ledger_info,
        ),
        Err(e) => Err(e.clone().into()),
    };
    let error_str = extract_error_string(&maybe_restore_result, go_storage.as_ref());
    Ok(CPreflightResult::new_from_invoke_host_function(
        invoke_hf_result,
        maybe_restore_result.unwrap_or(None),
        error_str,
    ))
}

#[no_mangle]
pub extern "C" fn preflight_footprint_ttl_op(
    handle: libc::uintptr_t, // Go Handle to forward to SnapshotSourceGet and SnapshotSourceHas
    op_body: CXDR,           // OperationBody XDR
    footprint: CXDR,         // LedgerFootprint XDR
    ledger_info: CLedgerInfo,
) -> *mut CPreflightResult {
    catch_preflight_panic(Box::new(move || {
        preflight_footprint_ttl_op_or_maybe_panic(handle, op_body, footprint, ledger_info)
    }))
}

fn preflight_footprint_ttl_op_or_maybe_panic(
    handle: libc::uintptr_t,
    op_body: CXDR,
    footprint: CXDR,
    c_ledger_info: CLedgerInfo,
) -> Result<CPreflightResult> {
    let op_body = OperationBody::from_xdr(from_c_xdr(op_body), DEFAULT_XDR_RW_LIMITS)?;
    let footprint = LedgerFootprint::from_xdr(from_c_xdr(footprint), DEFAULT_XDR_RW_LIMITS)?;
    let go_storage = Rc::new(GoLedgerStorage::new(handle));
    let network_config = NetworkConfig::load_from_snapshot(go_storage.as_ref())?;
    let ledger_info = fill_ledger_info(c_ledger_info, &network_config);
    // TODO: It would make for a better UX if the user passed only the neccesary fields for every operation.
    // That would remove a possibility of providing bad operation body, or a possibility of filling wrong footprint
    // field.
    match op_body {
        OperationBody::ExtendFootprintTtl(extend_op) => {
            preflight_extend_ttl_op(extend_op, footprint.read_only.as_slice(), go_storage, &network_config, &ledger_info)
        }
        OperationBody::RestoreFootprint(_) => {
            preflight_restore_op(footprint.read_write.as_slice(), go_storage, &network_config, &ledger_info)
        }
        _ => Err(anyhow!("encountered unsupported operation type: '{:?}', instead of 'ExtendFootprintTtl' or 'RestoreFootprint' operations.",
            op_body.discriminant()).into())
    }
}

fn preflight_extend_ttl_op(
    extend_op: ExtendFootprintTtlOp,
    keys_to_extend: &[LedgerKey],
    go_storage: Rc<GoLedgerStorage>,
    network_config: &NetworkConfig,
    ledger_info: &LedgerInfo,
) -> Result<CPreflightResult> {
    let auto_restore_snapshot = AutoRestoringSnapshotSource::new(go_storage.clone(), ledger_info)?;
    let simulation_result = simulate_extend_ttl_op(
        &auto_restore_snapshot,
        &network_config,
        &SimulationAdjustmentConfig::default_adjustment(),
        &ledger_info,
        keys_to_extend,
        extend_op.extend_to,
    );
    let (maybe_transaction_data, maybe_restore_result) = match simulation_result {
        Ok(r) => (
            Some(r.transaction_data),
            auto_restore_snapshot.simulate_restore_keys_op(
                &network_config,
                &SimulationAdjustmentConfig::default_adjustment(),
                &ledger_info,
            ),
        ),
        Err(e) => (None, Err(e)),
    };

    let error_str = extract_error_string(&maybe_restore_result, go_storage.as_ref());
    Ok(CPreflightResult::new_from_transaction_data(
        maybe_transaction_data,
        maybe_restore_result.unwrap_or(None),
        error_str,
    ))
}

fn preflight_restore_op(
    keys_to_restore: &[LedgerKey],
    go_storage: Rc<GoLedgerStorage>,
    network_config: &NetworkConfig,
    ledger_info: &LedgerInfo,
) -> Result<CPreflightResult> {
    let simulation_result = simulate_restore_op(
        go_storage.as_ref(),
        &network_config,
        &SimulationAdjustmentConfig::default_adjustment(),
        &ledger_info,
        keys_to_restore,
    );
    let error_str = extract_error_string(&simulation_result, go_storage.as_ref());
    Ok(CPreflightResult::new_from_transaction_data(
        simulation_result
            .map(|r| Some(r.transaction_data))
            .unwrap_or(None),
        None,
        error_str,
    ))
}

fn preflight_error(str: String) -> CPreflightResult {
    let c_str = CString::new(str).unwrap();
    CPreflightResult {
        error: c_str.into_raw(),
        ..Default::default()
    }
}

fn catch_preflight_panic(op: Box<dyn Fn() -> Result<CPreflightResult>>) -> *mut CPreflightResult {
    // catch panics before they reach foreign callers (which otherwise would result in
    // undefined behavior)
    let res: std::thread::Result<Result<CPreflightResult>> =
        panic::catch_unwind(panic::AssertUnwindSafe(op));
    let c_preflight_result = match res {
        Err(panic) => match panic.downcast::<String>() {
            Ok(panic_msg) => preflight_error(format!("panic during preflight() call: {panic_msg}")),
            Err(_) => preflight_error("panic during preflight() call: unknown cause".to_string()),
        },
        // See https://docs.rs/anyhow/latest/anyhow/struct.Error.html#display-representations
        Ok(r) => r.unwrap_or_else(|e| preflight_error(format!("{e:?}"))),
    };
    // transfer ownership to caller
    // caller needs to invoke free_preflight_result(result) when done
    Box::into_raw(Box::new(c_preflight_result))
}

// TODO: We could use something like https://github.com/sonos/ffi-convert-rs
//       to replace all the free_* , *_to_c and from_c_* functions by implementations of CDrop,
//       CReprOf and AsRust

fn xdr_to_c(v: impl WriteXdr) -> CXDR {
    let (xdr, len) = vec_to_c_array(v.to_xdr(DEFAULT_XDR_RW_LIMITS).unwrap());
    CXDR { xdr, len }
}

fn option_xdr_to_c(v: Option<impl WriteXdr>) -> CXDR {
    v.map_or(
        CXDR {
            xdr: null_mut(),
            len: 0,
        },
        xdr_to_c,
    )
}

fn ledger_entry_diff_to_c(v: LedgerEntryDiff) -> CXDRDiff {
    CXDRDiff {
        before: option_xdr_to_c(v.state_before),
        after: option_xdr_to_c(v.state_after),
    }
}

fn xdr_vec_to_c(v: Vec<impl WriteXdr>) -> CXDRVector {
    let c_v = v.into_iter().map(xdr_to_c).collect();
    let (array, len) = vec_to_c_array(c_v);
    CXDRVector { array, len }
}

fn string_to_c(str: String) -> *mut libc::c_char {
    CString::new(str).unwrap().into_raw()
}

fn vec_to_c_array<T>(mut v: Vec<T>) -> (*mut T, libc::size_t) {
    // Make sure length and capacity are the same
    // (this allows using the length as the capacity when deallocating the vector)
    v.shrink_to_fit();
    let len = v.len();
    assert_eq!(len, v.capacity());

    // Get the pointer to our vector, we will deallocate it in free_c_null_terminated_char_array()
    // TODO: replace by `out_vec.into_raw_parts()` once the API stabilizes
    let ptr = v.as_mut_ptr();
    mem::forget(v);

    (ptr, len)
}

fn ledger_entry_diff_vec_to_c(modified_entries: Vec<LedgerEntryDiff>) -> CXDRDiffVector {
    let c_diffs = modified_entries
        .into_iter()
        .map(ledger_entry_diff_to_c)
        .collect();
    let (array, len) = vec_to_c_array(c_diffs);
    CXDRDiffVector { array, len }
}

/// .
///
/// # Safety
///
/// .
#[no_mangle]
pub unsafe extern "C" fn free_preflight_result(result: *mut CPreflightResult) {
    if result.is_null() {
        return;
    }
    let boxed = Box::from_raw(result);
    free_c_string(boxed.error);
    free_c_xdr_array(boxed.auth);
    free_c_xdr(boxed.result);
    free_c_xdr(boxed.transaction_data);
    free_c_xdr_array(boxed.events);
    free_c_xdr(boxed.pre_restore_transaction_data);
    free_c_xdr_diff_array(boxed.ledger_entry_diff);
}

fn free_c_string(str: *mut libc::c_char) {
    if str.is_null() {
        return;
    }
    unsafe {
        _ = CString::from_raw(str);
    }
}

fn free_c_xdr(xdr: CXDR) {
    if xdr.xdr.is_null() {
        return;
    }
    unsafe {
        let _ = Vec::from_raw_parts(xdr.xdr, xdr.len, xdr.len);
    }
}

fn free_c_xdr_array(xdr_array: CXDRVector) {
    if xdr_array.array.is_null() {
        return;
    }
    unsafe {
        let v = Vec::from_raw_parts(xdr_array.array, xdr_array.len, xdr_array.len);
        for xdr in v {
            free_c_xdr(xdr);
        }
    }
}

fn free_c_xdr_diff_array(xdr_array: CXDRDiffVector) {
    if xdr_array.array.is_null() {
        return;
    }
    unsafe {
        let v = Vec::from_raw_parts(xdr_array.array, xdr_array.len, xdr_array.len);
        for diff in v {
            free_c_xdr(diff.before);
            free_c_xdr(diff.after);
        }
    }
}

fn from_c_string(str: *const libc::c_char) -> String {
    let c_str = unsafe { CStr::from_ptr(str) };
    c_str.to_str().unwrap().to_string()
}

fn from_c_xdr(xdr: CXDR) -> Vec<u8> {
    let s = unsafe { slice::from_raw_parts(xdr.xdr, xdr.len) };
    s.to_vec()
}

// Functions imported from Golang
extern "C" {
    // Free Strings returned from Go functions
    fn FreeGoXDR(xdr: CXDR);
    // LedgerKey XDR in base64 string to LedgerEntry XDR in base64 string
    fn SnapshotSourceGet(handle: libc::uintptr_t, ledger_key: CXDR) -> CXDR;
}

struct GoLedgerStorage {
    golang_handle: libc::uintptr_t,
    internal_error: RefCell<Option<anyhow::Error>>,
}

impl GoLedgerStorage {
    fn new(golang_handle: libc::uintptr_t) -> Self {
        Self {
            golang_handle,
            internal_error: RefCell::new(None),
        }
    }

    // Get the XDR, regardless of ttl
    fn get_xdr_internal(&self, key_xdr: &mut Vec<u8>) -> Option<Vec<u8>> {
        let key_c_xdr = CXDR {
            xdr: key_xdr.as_mut_ptr(),
            len: key_xdr.len(),
        };
        let res = unsafe { SnapshotSourceGet(self.golang_handle, key_c_xdr) };
        if res.xdr.is_null() {
            return None;
        }
        let v = from_c_xdr(res);
        unsafe { FreeGoXDR(res) };
        Some(v)
    }

    // Gets a ledger entry by key, including the archived/removed entries.
    // The failures of this function are not recoverable and should only happen when
    // the underlying storage is somehow corrupted.
    fn get_fallible(&self, key: &LedgerKey) -> anyhow::Result<Option<EntryWithLiveUntil>> {
        let mut key_xdr = key.to_xdr(DEFAULT_XDR_RW_LIMITS)?;
        let Some(xdr) = self.get_xdr_internal(&mut key_xdr) else {
            return Ok(None);
        };

        let live_until_ledger_seq = match key {
            // TODO: it would probably be more efficient to do all of this in the Go side
            //       (e.g. it would allow us to query multiple entries at once)
            LedgerKey::ContractData(_) | LedgerKey::ContractCode(_) => {
                let key_hash: [u8; 32] = Sha256::digest(key_xdr).into();
                let ttl_key = LedgerKey::Ttl(LedgerKeyTtl {
                    key_hash: Hash(key_hash),
                });
                let mut ttl_key_xdr = ttl_key.to_xdr(DEFAULT_XDR_RW_LIMITS)?;
                let ttl_entry_xdr = self.get_xdr_internal(&mut ttl_key_xdr).ok_or_else(|| {
                    anyhow!(
                        "TTL entry is missing for an entry that should have TTL with key: '{key:?}'"
                    )
                })?;
                let ttl_entry = LedgerEntry::from_xdr(ttl_entry_xdr, DEFAULT_XDR_RW_LIMITS)?;
                let LedgerEntryData::Ttl(TtlEntry {
                    live_until_ledger_seq,
                    ..
                }) = ttl_entry.data
                else {
                    bail!(
                        "unexpected non-TTL entry '{:?}' has been fetched for TTL key '{:?}'",
                        ttl_entry,
                        ttl_key
                    );
                };
                Some(live_until_ledger_seq)
            }
            _ => None,
        };

        let entry = LedgerEntry::from_xdr(xdr, DEFAULT_XDR_RW_LIMITS)?;
        Ok(Some((Rc::new(entry), live_until_ledger_seq)))
    }
}

impl SnapshotSourceWithArchive for GoLedgerStorage {
    fn get_including_archived(
        &self,
        key: &Rc<LedgerKey>,
    ) -> std::result::Result<Option<EntryWithLiveUntil>, HostError> {
        let res = self.get_fallible(key.as_ref());
        match res {
            Ok(res) => Ok(res),
            Err(e) => {
                // Store the internal error in the storage as the info won't be propagated from simulation.
                if let Ok(mut err) = self.internal_error.try_borrow_mut() {
                    *err = Some(e.into());
                }
                // Errors that occur in storage are not recoverable, so we force host to halt by passing
                // it an internal error.
                return Err((ScErrorType::Storage, ScErrorCode::InternalError).into());
            }
        }
    }
}

fn extract_error_string<T>(simulation_result: &Result<T>, go_storage: &GoLedgerStorage) -> String {
    match simulation_result {
        Ok(_) => String::new(),
        Err(e) => {
            // Override any simulation result with a storage error (if any). Simulation does not propagate the storage
            // errors, but these provide more exact information on the root cause.
            match go_storage.internal_error.borrow().as_ref() {
                Some(e) => format!("{e:?}"),
                None => format!("{e:?}"),
            }
        }
    }
}
