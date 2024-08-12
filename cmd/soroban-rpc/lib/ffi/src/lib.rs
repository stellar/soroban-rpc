extern crate libc;

use std::ffi::{CStr, CString};
use std::ptr::null_mut;
use std::slice;

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

/// Converts a Rust string to a C byte array.
///
/// The memory allocated to the C string must be freed when you're done with it
/// by calling `free_c_string`.
///
/// # Panics
///
/// If `str` is valid, this never panics; just be cool.
#[must_use]
pub fn string_to_c(str: String) -> *mut libc::c_char {
    CString::new(str).unwrap().into_raw()
}

/// Frees the memory previously allocated by Rust in `string_to_c`.
///
/// # Safety
///
/// You should take care to only free the same string once, and don't free
/// pointers to strings allocated from across the FFI boundary.
pub unsafe fn free_c_string(str: *mut libc::c_char) {
    if str.is_null() {
        return;
    }
    unsafe {
        _ = CString::from_raw(str);
    }
}

/// Frees the memory allocated to a generic XDR structure.
///
/// # Panics
///
/// If `str` is a valid null-terminated C string, this won't panic.
///
/// # Safety
///
/// You should take care to only free the same struct once, and don't free
/// pointers to structs allocated from across the FFI boundary.
#[must_use]
pub unsafe fn from_c_string(str: *const libc::c_char) -> String {
    let c_str = unsafe { CStr::from_ptr(str) };
    c_str.to_str().unwrap().to_string()
}

/// Transforms an FFI-compatible raw XDR structure into a Rust-managed chunk of
/// memory.
///
/// # Safety
///
/// Unless the structure itself is whack (e.g., you've free'd it before or the
/// buffer is mangled), this is safe to use.
#[must_use]
pub unsafe fn from_c_xdr(xdr: CXDR) -> Vec<u8> {
    let s = unsafe { slice::from_raw_parts(xdr.xdr, xdr.len) };
    s.to_vec()
}
