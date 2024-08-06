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

pub fn string_to_c(str: String) -> *mut libc::c_char {
    CString::new(str).unwrap().into_raw()
}

pub fn free_c_string(str: *mut libc::c_char) {
    if str.is_null() {
        return;
    }
    unsafe {
        _ = CString::from_raw(str);
    }
}

pub fn free_c_xdr(xdr: CXDR) {
    if xdr.xdr.is_null() {
        return;
    }
    unsafe {
        let _ = Vec::from_raw_parts(xdr.xdr, xdr.len, xdr.len);
    }
}

pub fn from_c_string(str: *const libc::c_char) -> String {
    let c_str = unsafe { CStr::from_ptr(str) };
    c_str.to_str().unwrap().to_string()
}

pub fn from_c_xdr(xdr: CXDR) -> Vec<u8> {
    let s = unsafe { slice::from_raw_parts(xdr.xdr, xdr.len) };
    s.to_vec()
}
