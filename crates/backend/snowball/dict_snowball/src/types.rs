
use core::ffi::{c_int, c_uchar};

#[allow(non_camel_case_types)]
pub type symbol = c_uchar;

#[allow(non_camel_case_types)]
#[derive(Copy, Clone)]
#[repr(C)]
pub struct SN_env {
    pub p: *mut symbol,
    pub c: c_int,
    pub l: c_int,
    pub lb: c_int,
    pub bra: c_int,
    pub ket: c_int,
    pub S: *mut *mut symbol,
    pub I: *mut c_int,
}

#[allow(non_camel_case_types)]
#[derive(Copy, Clone)]
#[repr(C)]
pub struct among {
    pub s_size: c_int,
    pub s: *const symbol,
    pub substring_i: c_int,
    pub result: c_int,
    pub function: Option<unsafe fn(*mut SN_env) -> c_int>,
}
