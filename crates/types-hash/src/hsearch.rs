//! Dynahash vocabulary (`utils/hsearch.h` + the `dynahash.c` runtime structs).
//!
//! The C header keeps `HTAB`/`HASHHDR` "known only within dynahash.c"; the
//! dynahash port (`backend-utils-hash-dynahash`) owns the bodies, so they are
//! defined here in full with their real `#[repr(C)]` layout. A shared-memory
//! hash table's `HASHHDR`, directory, segments and every `HASHELEMENT` live in
//! the real `MAP_SHARED` segment, mapped at the same fixed address in every
//! forked backend, so a `HASHELEMENT *`/`HASHBUCKET *` written by one backend
//! dereferences correctly in another — byte-for-byte with PostgreSQL. The
//! per-backend `HTAB` holds raw pointers (`hctl`/`dir`) into that segment (or,
//! for a local table, into `palloc`ed memory tracked by the dynahash crate).

use core::ptr;
use core::sync::atomic::AtomicI32;

use types_core::{uint32, Size};

/// `HASHELEMENT` (hsearch.h) — header for a hash-table entry as stored in a
/// bucket chain; the user's key+entry data follows at
/// `MAXALIGN(sizeof(HASHELEMENT))`.
#[repr(C)]
#[derive(Debug)]
pub struct HASHELEMENT {
    /// `struct HASHELEMENT *link` — next entry in the same bucket / freelist.
    pub link: *mut HASHELEMENT,
    /// `uint32 hashvalue` — hash function result for this entry.
    pub hashvalue: u32,
}

/// A hash bucket is a linked list of `HASHELEMENT`s (`HASHELEMENT *HASHBUCKET`).
pub type HASHBUCKET = *mut HASHELEMENT;

/// A hash segment is an array of bucket headers (`HASHBUCKET *HASHSEGMENT`).
pub type HASHSEGMENT = *mut HASHBUCKET;

/// `HashValueFunc` (hsearch.h) — `uint32 (*)(const void *key, Size keysize)`.
pub type HashValueFunc = fn(&[u8], Size) -> uint32;
/// `HashCompareFunc` (hsearch.h) — `int (*)(const void *k1, const void *k2,
/// Size keysize)`.
pub type HashCompareFunc = fn(&[u8], &[u8], Size) -> i32;
/// `HashCopyFunc` (hsearch.h) — `void *(*)(void *dst, const void *src,
/// Size keysize)`.
pub type HashCopyFunc = fn(&mut [u8], &[u8], Size);

/// `HashAllocFunc` (hsearch.h) — `void *(*)(Size request)`; returns null when
/// the allocation cannot be satisfied (the `ShmemAllocNoError` contract).
pub type HashAllocFunc = fn(usize) -> *mut u8;

/// Number of freelists for a partitioned hash table (`dynahash.c`).
pub const NUM_FREELISTS: usize = 32;

/// Per-freelist data (`dynahash.c`). In a partitioned table each freelist owns
/// its own spinlock and `nentries` count to reduce cache-line sharing; only
/// `freeList[0]` is used (and its mutex left idle) when unpartitioned. The
/// `mutex` is a genuine in-segment spinlock (`slock_t`, an `int`-width
/// `AtomicI32`), so a partitioned shared table locks across forked backends.
#[repr(C)]
pub struct FreeListData {
    /// `slock_t mutex` — spinlock for this freelist.
    pub mutex: AtomicI32,
    /// `long nentries` — number of live entries in associated buckets.
    pub nentries: i64,
    /// `HASHELEMENT *freeList` — chain of free elements.
    pub freeList: *mut HASHELEMENT,
}

/// `HASHHDR` (`dynahash.c`) — the changeable header for a hash table. In a
/// shared table this lives in the segment; each backend keeps its own
/// [`HTAB`]. `#[repr(C)]` with field order matching the C struct so the
/// segment layout is byte-faithful across backends.
#[repr(C)]
pub struct HASHHDR {
    /// `FreeListData freeList[NUM_FREELISTS]`.
    pub freeList: [FreeListData; NUM_FREELISTS],

    /* These fields can change, but not in a partitioned table */
    /// `long dsize` — directory size.
    pub dsize: i64,
    /// `long nsegs` — number of allocated segments (<= dsize).
    pub nsegs: i64,
    /// `uint32 max_bucket` — ID of maximum bucket in use.
    pub max_bucket: uint32,
    /// `uint32 high_mask` — mask to modulo into entire table.
    pub high_mask: uint32,
    /// `uint32 low_mask` — mask to modulo into lower half of table.
    pub low_mask: uint32,

    /* These fields are fixed at hashtable creation */
    /// `Size keysize` — hash key length in bytes.
    pub keysize: Size,
    /// `Size entrysize` — total user element size in bytes.
    pub entrysize: Size,
    /// `long num_partitions` — # partitions (must be power of 2), or 0.
    pub num_partitions: i64,
    /// `long max_dsize` — 'dsize' limit if directory is fixed size.
    pub max_dsize: i64,
    /// `long ssize` — segment size --- must be power of 2.
    pub ssize: i64,
    /// `int sshift` — segment shift = log2(ssize).
    pub sshift: i32,
    /// `int nelem_alloc` — number of entries to allocate at once.
    pub nelem_alloc: i32,
}

/// `HTAB` (`dynahash.c`) — the per-backend top control struct. In a shared
/// table each backend has its own copy pointing at the same shared `hctl`/`dir`.
/// `hash`/`match`/`keycopy`/`alloc` are the C function-pointer slots.
#[repr(C)]
pub struct HTAB {
    /// `HASHHDR *hctl` — control information (shared or local).
    pub hctl: *mut HASHHDR,
    /// `HASHSEGMENT *dir` — directory of segment starts.
    pub dir: *mut HASHSEGMENT,
    /// `HashValueFunc hash` — hash function.
    pub hash: Option<HashValueFunc>,
    /// `HashCompareFunc match` — key comparison function.
    pub match_: Option<HashCompareFunc>,
    /// `HashCopyFunc keycopy` — key copying function.
    pub keycopy: Option<HashCopyFunc>,
    /// `HashAllocFunc alloc` — memory allocator.
    pub alloc: Option<HashAllocFunc>,
    /// `MemoryContext hcxt` — memory context if the default allocator is used.
    /// Modelled by a token the dynahash crate associates with its per-table
    /// slab registry (null for shared tables, which never use the default
    /// allocator); the value carries no C ABI meaning here.
    pub hcxt: *mut u8,
    /// `char *tabname` — table name (for error messages); owned by the table's
    /// allocation in C. Here the dynahash crate parks an owned name in its
    /// per-table registry and leaves this null (error paths read the registry).
    pub tabname: *mut u8,
    /// `bool isshared` — true if table is in shared memory.
    pub isshared: bool,
    /// `bool isfixed` — if true, don't enlarge.
    pub isfixed: bool,
    /// `bool frozen` — true = no more inserts allowed.
    pub frozen: bool,
    /// `Size keysize` — local copy of hash key length.
    pub keysize: Size,
    /// `long ssize` — local copy of segment size (power of 2).
    pub ssize: i64,
    /// `int sshift` — local copy of segment shift = log2(ssize).
    pub sshift: i32,
}

/// `HASHCTL` (hsearch.h) — parameter struct for `hash_create`. Field order
/// follows the C header; flag bits select which fields are honored.
#[derive(Debug)]
pub struct HASHCTL {
    /// `long num_partitions` — # partitions (must be power of 2); `HASH_PARTITION`.
    pub num_partitions: i64,
    /// `long ssize` — segment size; `HASH_SEGMENT`.
    pub ssize: i64,
    /// `long dsize` — (initial) directory size; `HASH_DIRSIZE`.
    pub dsize: i64,
    /// `long max_dsize` — limit to dsize if dir size is limited; `HASH_DIRSIZE`.
    pub max_dsize: i64,
    /// `Size keysize` — hash key length in bytes; `HASH_ELEM`.
    pub keysize: usize,
    /// `Size entrysize` — total user element size in bytes; `HASH_ELEM`.
    pub entrysize: usize,
    /// `HashValueFunc hash` — custom hash function; `HASH_FUNCTION`.
    pub hash: Option<HashValueFunc>,
    /// `HashCompareFunc match` — custom comparison; `HASH_COMPARE`.
    pub match_: Option<HashCompareFunc>,
    /// `HashCopyFunc keycopy` — custom key copy; `HASH_KEYCOPY`.
    pub keycopy: Option<HashCopyFunc>,
    /// `HashAllocFunc alloc` — memory allocator; `HASH_ALLOC`.
    pub alloc: Option<HashAllocFunc>,
    /// `MemoryContext hcxt` — context for a local table; `HASH_CONTEXT`.
    pub hcxt: *mut u8,
    /// `HASHHDR *hctl` — location of header in shared mem; `HASH_SHARED_MEM`.
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

pub use HASHACTION::{HASH_ENTER, HASH_ENTER_NULL, HASH_FIND, HASH_REMOVE};

/// `HASH_SEQ_STATUS` (hsearch.h) — sequential-scan state.
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

/// `DEF_SEGSIZE` (dynahash.c) — default segment size.
pub const DEF_SEGSIZE: i64 = 256;
/// `DEF_SEGSIZE_SHIFT` (dynahash.c) — log2(DEF_SEGSIZE).
pub const DEF_SEGSIZE_SHIFT: i32 = 8;
/// `DEF_DIRSIZE` (dynahash.c) — default directory size.
pub const DEF_DIRSIZE: i64 = 256;
