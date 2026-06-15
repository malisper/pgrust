use core::ffi::{c_char, c_int};

pub const STRINGINFO_DEFAULT_SIZE: c_int = 1024;

#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub struct StringInfoData {
    pub data: *mut c_char,
    pub len: c_int,
    pub maxlen: c_int,
    pub cursor: c_int,
}

pub type StringInfo = *mut StringInfoData;
