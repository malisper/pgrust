use core::ptr;
use core::sync::atomic::AtomicI32;

use ::types_core::{uint32, Size};

#[repr(C)]
#[derive(Debug)]
pub struct HASHELEMENT {
    pub link: *mut HASHELEMENT,
    pub hashvalue: uint32,
}

pub type HASHBUCKET = *mut HASHELEMENT;
pub type HASHSEGMENT = *mut HASHBUCKET;

// Safe-slice renderings of C's `const void *key` signatures.
pub type HashValueFunc = fn(&[u8], Size) -> uint32;
pub type HashCompareFunc = fn(&[u8], &[u8], Size) -> i32;
pub type HashCopyFunc = fn(&mut [u8], &[u8], Size);
// Returns null when the allocation cannot be satisfied (the ShmemAllocNoError
// contract).
pub type HashAllocFunc = fn(usize) -> *mut u8;

pub const NUM_FREELISTS: usize = 32;

// `mutex` is C's slock_t widened to an in-place AtomicI32; it is used only in
// a partitioned table (unpartitioned tables touch freeList[0] with no lock).
#[repr(C)]
pub struct FreeListData {
    pub mutex: AtomicI32,
    pub nentries: i64,
    pub freeList: *mut HASHELEMENT,
}

// C keeps HASHHDR/HTAB private to dynahash.c; the dynahash unit owns the
// bodies, so their repr(C) layouts live here. A shared table's HASHHDR,
// directory, segments and elements sit in one allocation reachable from every
// backend thread; each backend keeps its own HTAB.
#[repr(C)]
pub struct HASHHDR {
    pub freeList: [FreeListData; NUM_FREELISTS],

    /* changeable, but not in a partitioned table */
    pub dsize: i64,
    pub nsegs: i64,
    pub max_bucket: uint32,
    pub high_mask: uint32,
    pub low_mask: uint32,

    /* fixed at hashtable creation */
    pub keysize: Size,
    pub entrysize: Size,
    pub num_partitions: i64,
    pub max_dsize: i64,
    pub ssize: i64,
    pub sshift: i32,
    pub nelem_alloc: i32,
}

// hcxt/tabname are opaque tokens: the dynahash crate parks the real context
// handle and the owned table name in its per-table registry (C stores a
// MemoryContext and a char* inside the table's own allocation).
#[repr(C)]
#[derive(Debug)]
pub struct HTAB {
    pub hctl: *mut HASHHDR,
    pub dir: *mut HASHSEGMENT,
    pub hash: Option<HashValueFunc>,
    pub match_: Option<HashCompareFunc>,
    pub keycopy: Option<HashCopyFunc>,
    pub alloc: Option<HashAllocFunc>,
    pub hcxt: *mut u8,
    pub tabname: *mut u8,
    pub isshared: bool,
    pub isfixed: bool,
    pub frozen: bool,
    pub keysize: Size,
    pub ssize: i64,
    pub sshift: i32,
}

// Forgetting a live HTAB strands its dynahash registry entry — planner use
// (join_rel_hash) must stay never-created or explicitly destroyed.
mcx::forget_safe_nodrop!(HTAB);

#[derive(Debug)]
pub struct HASHCTL {
    pub num_partitions: i64,
    pub ssize: i64,
    pub dsize: i64,
    pub max_dsize: i64,
    pub keysize: Size,
    pub entrysize: Size,
    pub hash: Option<HashValueFunc>,
    pub match_: Option<HashCompareFunc>,
    pub keycopy: Option<HashCopyFunc>,
    pub alloc: Option<HashAllocFunc>,
    pub hcxt: *mut u8,
    pub hctl: *mut HASHHDR,
}

impl HASHCTL {
    pub const fn new() -> Self {
        Self {
            num_partitions: 0,
            ssize: 0,
            dsize: 0,
            max_dsize: 0,
            keysize: 0,
            entrysize: 0,
            hash: None,
            match_: None,
            keycopy: None,
            alloc: None,
            hcxt: ptr::null_mut(),
            hctl: ptr::null_mut(),
        }
    }
}

impl Default for HASHCTL {
    fn default() -> Self {
        Self::new()
    }
}

pub const HASH_PARTITION: i32 = 0x0001;
pub const HASH_SEGMENT: i32 = 0x0002;
pub const HASH_DIRSIZE: i32 = 0x0004;
pub const HASH_ELEM: i32 = 0x0008;
pub const HASH_STRINGS: i32 = 0x0010;
pub const HASH_BLOBS: i32 = 0x0020;
pub const HASH_FUNCTION: i32 = 0x0040;
pub const HASH_COMPARE: i32 = 0x0080;
pub const HASH_KEYCOPY: i32 = 0x0100;
pub const HASH_ALLOC: i32 = 0x0200;
pub const HASH_CONTEXT: i32 = 0x0400;
pub const HASH_SHARED_MEM: i32 = 0x0800;
pub const HASH_ATTACH: i32 = 0x1000;
pub const HASH_FIXED_SIZE: i32 = 0x2000;

pub const NO_MAX_DSIZE: i64 = -1;

#[repr(i32)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HASHACTION {
    HASH_FIND = 0,
    HASH_ENTER = 1,
    HASH_REMOVE = 2,
    HASH_ENTER_NULL = 3,
}

pub use HASHACTION::{HASH_ENTER, HASH_ENTER_NULL, HASH_FIND, HASH_REMOVE};

#[repr(C)]
#[derive(Debug)]
pub struct HASH_SEQ_STATUS {
    pub hashp: *mut HTAB,
    pub curBucket: uint32,
    pub curEntry: *mut HASHELEMENT,
    pub hasHashvalue: bool,
    pub hashvalue: uint32,
}

impl HASH_SEQ_STATUS {
    pub const fn new() -> Self {
        Self {
            hashp: ptr::null_mut(),
            curBucket: 0,
            curEntry: ptr::null_mut(),
            hasHashvalue: false,
            hashvalue: 0,
        }
    }
}

impl Default for HASH_SEQ_STATUS {
    fn default() -> Self {
        Self::new()
    }
}

pub const DEF_SEGSIZE: i64 = 256;
pub const DEF_SEGSIZE_SHIFT: i32 = 8;
pub const DEF_DIRSIZE: i64 = 256;

// 64-bit layout pins (embedded pointers). wasm32 (ILP32) shrinks both; all
// dynahash arithmetic derives from size_of per target, so they stay
// internally consistent.
#[cfg(not(target_family = "wasm"))]
const _: () = assert!(core::mem::size_of::<HASHELEMENT>() == 16);
#[cfg(not(target_family = "wasm"))]
const _: () = assert!(core::mem::size_of::<FreeListData>() == 24);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn segsize_shift_matches() {
        assert_eq!(1i64 << DEF_SEGSIZE_SHIFT, DEF_SEGSIZE);
    }

    #[test]
    fn hashctl_defaults_zeroed() {
        let ctl = HASHCTL::new();
        assert_eq!(ctl.num_partitions, 0);
        assert!(ctl.hash.is_none());
        assert!(ctl.hctl.is_null());
    }

    #[test]
    fn hashaction_discriminants() {
        assert_eq!(HASH_FIND as i32, 0);
        assert_eq!(HASH_ENTER as i32, 1);
        assert_eq!(HASH_REMOVE as i32, 2);
        assert_eq!(HASH_ENTER_NULL as i32, 3);
    }
}
