//! Dynahash consumer vocabulary (`utils/hsearch.h`), trimmed to the items
//! current ports consume. The hash-table internals (`HTAB`/`HASHHDR` bodies)
//! are opaque in the C header too — "known only within dynahash.c" — so the
//! opacity here is inherited, not introduced; the dynahash port widens these
//! in place when it lands.

/// `HASHELEMENT` (hsearch.h) — header for a hash-table entry as stored in a
/// bucket chain; the user's entry data follows.
#[repr(C)]
#[derive(Debug)]
pub struct HASHELEMENT {
    /// `struct HASHELEMENT *link` — next entry in the same bucket.
    pub link: *mut HASHELEMENT,
    /// `uint32 hashvalue` — hash function result for this entry.
    pub hashvalue: u32,
}

/// `HASHHDR` (hsearch.h) — "Hash table header struct is an opaque type known
/// only within dynahash.c". Consumers hold only `*mut HASHHDR` (it lives in
/// shared memory for shared tables); the dynahash port defines the body.
#[repr(C)]
pub struct HASHHDR {
    _opaque: [u8; 0],
}

/// `HTAB` (hsearch.h) — "Hash table control struct is an opaque type known
/// only within dynahash.c". Consumers hold only `*mut HTAB` (a per-backend
/// handle); the dynahash port defines the body.
#[repr(C)]
pub struct HTAB {
    _opaque: [u8; 0],
}

/// `HashAllocFunc` (hsearch.h) — `void *(*)(Size request)`; returns null when
/// the allocation cannot be satisfied (the `ShmemAllocNoError` contract).
pub type HashAllocFunc = fn(usize) -> *mut u8;

/// `HASHCTL` (hsearch.h) — parameter struct for `hash_create`, trimmed
/// (docs/types.md rule 3) to the fields current ports set. The dynahash port
/// widens it (`num_partitions`, `ssize`, `hash`, `match`, `keycopy`, `hcxt`).
#[derive(Debug, Default)]
pub struct HASHCTL {
    /// `long dsize` — (initial) directory size; used if `HASH_DIRSIZE`.
    pub dsize: i64,
    /// `long max_dsize` — limit to dsize if dir size is limited.
    pub max_dsize: i64,
    /// `Size keysize` — hash key length in bytes; used if `HASH_ELEM`.
    pub keysize: usize,
    /// `Size entrysize` — total user element size in bytes.
    pub entrysize: usize,
    /// `HashAllocFunc alloc` — memory allocator; used if `HASH_ALLOC`.
    pub alloc: Option<HashAllocFunc>,
    /// `HASHHDR *hctl` — location of header in shared mem; used if
    /// `HASH_SHARED_MEM`.
    pub hctl: *mut HASHHDR,
}

impl HASHCTL {
    pub const fn new() -> Self {
        Self {
            dsize: 0,
            max_dsize: 0,
            keysize: 0,
            entrysize: 0,
            alloc: None,
            hctl: core::ptr::null_mut(),
        }
    }
}

// Flag bits for hash_create (hsearch.h).
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

/// `NO_MAX_DSIZE` (hsearch.h) — max_dsize value indicating an expansible
/// directory.
pub const NO_MAX_DSIZE: i64 = -1;

/// `HASHACTION` (hsearch.h) — hash_search operations.
#[repr(i32)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HASHACTION {
    HASH_FIND = 0,
    HASH_ENTER = 1,
    HASH_REMOVE = 2,
    HASH_ENTER_NULL = 3,
}

/// `HASH_SEQ_STATUS` (hsearch.h) — sequential-scan state ("should be
/// considered an opaque type by callers"; the C spells the fields out in the
/// header, so they are public here too).
#[repr(C)]
#[derive(Debug)]
pub struct HASH_SEQ_STATUS {
    pub hashp: *mut HTAB,
    /// `uint32 curBucket` — index of current bucket.
    pub curBucket: u32,
    /// `HASHELEMENT *curEntry` — current entry in bucket.
    pub curEntry: *mut HASHELEMENT,
    /// `bool hasHashvalue` — true if hashvalue was provided.
    pub hasHashvalue: bool,
    /// `uint32 hashvalue` — hashvalue to start seqscan over hash.
    pub hashvalue: u32,
}

impl HASH_SEQ_STATUS {
    pub const fn new() -> Self {
        Self {
            hashp: core::ptr::null_mut(),
            curBucket: 0,
            curEntry: core::ptr::null_mut(),
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
