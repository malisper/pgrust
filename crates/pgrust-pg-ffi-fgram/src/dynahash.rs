use core::ffi::{c_int, c_long, c_void};

use crate::{uint32, MemoryContext, Size};

pub type HashValueFunc = Option<unsafe extern "C" fn(*const c_void, Size) -> uint32>;
pub type HashCompareFunc =
    Option<unsafe extern "C" fn(*const c_void, *const c_void, Size) -> c_int>;
pub type HashCopyFunc =
    Option<unsafe extern "C" fn(*mut c_void, *const c_void, Size) -> *mut c_void>;
pub type HashAllocFunc = Option<unsafe extern "C" fn(Size) -> *mut c_void>;

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct HASHELEMENT {
    pub link: *mut HASHELEMENT,
    pub hashvalue: uint32,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct HASHHDR {
    _private: [u8; 0],
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct HTAB {
    _private: [u8; 0],
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct HASHCTL {
    pub num_partitions: c_long,
    pub ssize: c_long,
    pub dsize: c_long,
    pub max_dsize: c_long,
    pub keysize: Size,
    pub entrysize: Size,
    pub hash: HashValueFunc,
    pub match_: HashCompareFunc,
    pub keycopy: HashCopyFunc,
    pub alloc: HashAllocFunc,
    pub hcxt: MemoryContext,
    pub hctl: *mut HASHHDR,
}

#[repr(u32)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HASHACTION {
    HASH_FIND = 0,
    HASH_ENTER = 1,
    HASH_REMOVE = 2,
    HASH_ENTER_NULL = 3,
}

pub const HASH_FIND: HASHACTION = HASHACTION::HASH_FIND;
pub const HASH_ENTER: HASHACTION = HASHACTION::HASH_ENTER;
pub const HASH_REMOVE: HASHACTION = HASHACTION::HASH_REMOVE;
pub const HASH_ENTER_NULL: HASHACTION = HASHACTION::HASH_ENTER_NULL;

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct HASH_SEQ_STATUS {
    pub hashp: *mut HTAB,
    pub curBucket: uint32,
    pub curEntry: *mut HASHELEMENT,
    pub hasHashvalue: bool,
    pub hashvalue: uint32,
}

pub const HASH_PARTITION: c_int = 0x0001;
pub const HASH_SEGMENT: c_int = 0x0002;
pub const HASH_DIRSIZE: c_int = 0x0004;
pub const HASH_ELEM: c_int = 0x0008;
pub const HASH_STRINGS: c_int = 0x0010;
pub const HASH_BLOBS: c_int = 0x0020;
pub const HASH_FUNCTION: c_int = 0x0040;
pub const HASH_COMPARE: c_int = 0x0080;
pub const HASH_KEYCOPY: c_int = 0x0100;
pub const HASH_ALLOC: c_int = 0x0200;
pub const HASH_CONTEXT: c_int = 0x0400;
pub const HASH_SHARED_MEM: c_int = 0x0800;
pub const HASH_ATTACH: c_int = 0x1000;
pub const HASH_FIXED_SIZE: c_int = 0x2000;
pub const NO_MAX_DSIZE: c_long = -1;

pub const DEF_SEGSIZE: c_long = 256;
pub const DEF_SEGSIZE_SHIFT: c_int = 8;
pub const DEF_DIRSIZE: c_long = 256;
pub const NUM_FREELISTS: usize = 32;

#[cfg(test)]
mod tests {
    use super::*;
    use core::mem::{align_of, offset_of, size_of};

    #[test]
    fn hashctl_layout_matches_postgres_abi_on_64_bit() {
        assert_eq!(size_of::<HASHELEMENT>(), 16);
        assert_eq!(offset_of!(HASHELEMENT, link), 0);
        assert_eq!(offset_of!(HASHELEMENT, hashvalue), 8);
        assert_eq!(size_of::<HASHCTL>(), 96);
        assert_eq!(align_of::<HASHCTL>(), 8);
        assert_eq!(offset_of!(HASHCTL, num_partitions), 0);
        assert_eq!(offset_of!(HASHCTL, keysize), 32);
        assert_eq!(offset_of!(HASHCTL, hash), 48);
        assert_eq!(offset_of!(HASHCTL, hcxt), 80);
        assert_eq!(size_of::<HASH_SEQ_STATUS>(), 32);
        assert_eq!(offset_of!(HASH_SEQ_STATUS, hashp), 0);
        assert_eq!(offset_of!(HASH_SEQ_STATUS, curBucket), 8);
        assert_eq!(offset_of!(HASH_SEQ_STATUS, curEntry), 16);
        assert_eq!(offset_of!(HASH_SEQ_STATUS, hasHashvalue), 24);
        assert_eq!(offset_of!(HASH_SEQ_STATUS, hashvalue), 28);
    }
}
