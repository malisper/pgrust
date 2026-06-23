#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::missing_safety_doc)]

//! Faithful port of `src/backend/utils/hash/dynahash.c` — the dynamic chained
//! hash tables behind `utils/hash/hsearch.h`.
//!
//! # The segmented layout (one implementation, two modes)
//!
//! PostgreSQL has ONE `HTAB` with two modes:
//!
//! * LOCAL (`palloc`/`MemoryContext`, growable): the directory can be doubled
//!   (`dir_realloc`) and buckets split on the fly (`expand_table`).
//! * SHMEM (`HASH_SHARED_MEM`, usually `HASH_PARTITION`, FIXED size): the
//!   `HASHHDR` + directory + every segment + every element live in the real
//!   `MAP_SHARED` segment, never split, lockable per-partition.
//!
//! Both share the segment/directory/bucket-chain machinery, reproduced here
//! byte-for-byte over raw pointers ([`HASHELEMENT`]/`HASHBUCKET`/`HASHSEGMENT`/
//! `HASHHDR`/`HTAB`, owned by [`hash::hsearch`]).
//!
//! ## Memory
//!
//! C draws a shared table's `hctl`/`dir`/segments/elements from the segment via
//! the caller-supplied `info->alloc` (`ShmemAllocNoError`: `NULL` on failure),
//! and a local table's from a private `MemoryContext` via the default allocator
//! `DynaHashAlloc` (`MCXT_ALLOC_NO_OOM`: `NULL` on failure). Because the segment
//! is mapped at the identical fixed virtual address in every forked backend, a
//! `HASHELEMENT *`/`HASHBUCKET *` written by one backend dereferences correctly
//! in another — exactly as in C.
//!
//! Here the shared path calls the real `info->alloc` function pointer. The local
//! default allocator hands out pinned, zeroed `Box<[u8]>` slabs charged to a
//! per-table arena tracked in a backend-local (thread_local) registry keyed by
//! the `HTAB *`; the arena (and the boxed `HTAB`/name) are dropped at
//! `hash_destroy` (== `MemoryContextDelete`), keeping every raw element/segment/
//! dir pointer valid for the table's life. `dir_realloc` retains the old dir slab
//! (C `pfree`s only the old dir); bounded and never reused.
//!
//! The partitioned freelists use real in-segment `slock_t` spinlocks driven by
//! `s_lock`, never `std::sync`.

extern crate alloc;

use alloc::boxed::Box;
use alloc::ffi::CString;
use alloc::format;
use alloc::vec::Vec;

use core::cell::RefCell;
use core::ptr;
use core::sync::atomic::AtomicI32;

use utils_error::{elog, PgError, PgResult};
use types_core::Size;
use types_error::{ERROR, FATAL, PANIC, WARNING};
use hash::hsearch::{
    HashCompareFunc, HashCopyFunc, HashValueFunc, HASHACTION, HASHBUCKET, HASHCTL,
    HASHELEMENT, HASHHDR, HASHSEGMENT, HASH_ALLOC, HASH_ATTACH, HASH_BLOBS, HASH_COMPARE,
    HASH_DIRSIZE, HASH_ELEM, HASH_FIXED_SIZE, HASH_FUNCTION, HASH_KEYCOPY,
    HASH_PARTITION, HASH_SEGMENT, HASH_SHARED_MEM, HASH_STRINGS, HASH_SEQ_STATUS, HTAB,
    NO_MAX_DSIZE, NUM_FREELISTS,
};
use hash::hsearch::HASHACTION::{HASH_ENTER, HASH_ENTER_NULL, HASH_FIND, HASH_REMOVE};
use types_storage::storage::Spinlock;

mod seam;
pub use seam::init_seams;

// ===========================================================================
// Constants (dynahash.c)
// ===========================================================================

const DEF_SEGSIZE: i64 = 256;
const DEF_SEGSIZE_SHIFT: i32 = 8;
const DEF_DIRSIZE: i64 = 256;

const MAX_SEQ_SCANS: usize = 100;

/// `MAXIMUM_ALIGNOF` (8 on all current 64-bit / int-width targets).
const MAXALIGN_SIZE: usize = 8;

#[inline]
fn MAXALIGN(value: usize) -> usize {
    (value + (MAXALIGN_SIZE - 1)) & !(MAXALIGN_SIZE - 1)
}

/// `IS_PARTITIONED(hctl)`.
#[inline]
unsafe fn IS_PARTITIONED(hctl: *const HASHHDR) -> bool {
    (*hctl).num_partitions != 0
}

/// `FREELIST_IDX(hctl, hashcode)`.
#[inline]
unsafe fn FREELIST_IDX(hctl: *const HASHHDR, hashcode: u32) -> usize {
    if IS_PARTITIONED(hctl) {
        (hashcode as usize) % NUM_FREELISTS
    } else {
        0
    }
}

/// `ELEMENTKEY(helem)` — `(char *)helem + MAXALIGN(sizeof(HASHELEMENT))`.
#[inline]
unsafe fn ELEMENTKEY(helem: *mut HASHELEMENT) -> *mut u8 {
    (helem as *mut u8).add(MAXALIGN(core::mem::size_of::<HASHELEMENT>()))
}

/// `ELEMENT_FROM_KEY(key)` — inverse of `ELEMENTKEY`.
#[inline]
unsafe fn ELEMENT_FROM_KEY(key: *mut u8) -> *mut HASHELEMENT {
    key.sub(MAXALIGN(core::mem::size_of::<HASHELEMENT>())) as *mut HASHELEMENT
}

/// `MOD(x, y)` — `x & (y-1)` (y is a power of 2).
#[inline]
fn MOD(x: i64, y: i64) -> i64 {
    x & (y - 1)
}

// ---------------------------------------------------------------------------
// Spinlock helpers. The freelist mutex is an in-segment `slock_t` (AtomicI32);
// `Spinlock` is `#[repr(transparent)]` over `AtomicI32`, so a `&AtomicI32` is
// ABI-identical and can be driven by the real s_lock primitives.
// ---------------------------------------------------------------------------

#[inline]
unsafe fn as_spinlock(m: *mut AtomicI32) -> &'static Spinlock {
    &*(m as *const Spinlock)
}

#[inline]
unsafe fn SpinLockInit(m: *mut AtomicI32) {
    s_lock::s_init_lock(as_spinlock(m));
}

#[inline]
unsafe fn SpinLockAcquire(m: *mut AtomicI32) {
    // s_lock cannot fail on a free lock; it returns the delay count (void in C).
    let _ = s_lock::s_lock(as_spinlock(m), Some(file!()), line!() as i32, Some("dynahash"));
}

#[inline]
unsafe fn SpinLockRelease(m: *mut AtomicI32) {
    s_lock::s_unlock(as_spinlock(m));
}

// ===========================================================================
// Backend-local table registry (the default allocator's backing store)
// ===========================================================================

/// The private memory of a LOCAL table created with the default allocator,
/// keyed by the `HTAB *` we hand back. Mirrors the C private `MemoryContext`:
/// `hash_destroy` drops it (== `MemoryContextDelete`); until then it pins every
/// `HTAB`/`HASHHDR`/dir/segment/element address.
struct TableArena {
    /// Pinned, zeroed slabs handed out by `DynaHashAlloc`.
    slabs: Vec<Box<[u8]>>,
    /// The boxed `HTAB` whose raw pointer is the table handle / registry key.
    htab: Box<HTAB>,
    /// The owned table name; `htab.tabname` points into this NUL-terminated buf.
    _tabname: CString,
}

thread_local! {
    /// `HTAB *` (as usize) -> the table's private arena. Only LOCAL tables (and
    /// the local `HTAB` shell of a shared table, which C also keeps backend-local)
    /// appear here.
    static TABLES: RefCell<Vec<(usize, TableArena)>> =
        const { RefCell::new(Vec::new()) };
}

/// `DynaHashAlloc(size)` for a LOCAL table: a pinned, zeroed slab charged to the
/// table's arena, OOM-safe (`MCXT_ALLOC_NO_OOM` NULL-on-failure). `into_boxed_slice`
/// pins the address for the table's life.
fn dyna_hash_alloc(htab: usize, size: Size) -> *mut u8 {
    TABLES.with(|cell| {
        let mut tables = cell.borrow_mut();
        let arena = tables
            .iter_mut()
            .find(|(k, _)| *k == htab)
            .map(|(_, a)| a)
            .expect("DynaHashAlloc on an unregistered local table");
        let mut v: Vec<u8> = Vec::new();
        if v.try_reserve_exact(size).is_err() {
            return ptr::null_mut();
        }
        v.resize(size, 0u8);
        let mut boxed = v.into_boxed_slice();
        let p = boxed.as_mut_ptr();
        arena.slabs.push(boxed);
        p
    })
}

/// Allocate `size` bytes for `hashp` via its allocator: the caller's
/// `info->alloc` if `HASH_ALLOC`/shared, else the local default allocator.
/// Returns `NULL` on OOM (the C `hashp->alloc(size)` contract). Zeroed only by
/// the local allocator and `seg_alloc`/`hdefault`/`dir_realloc` MemSet, matching C.
unsafe fn hash_alloc(hashp: *mut HTAB, size: Size) -> *mut u8 {
    match (*hashp).alloc {
        Some(alloc) => alloc(size),
        None => dyna_hash_alloc(hashp as usize, size),
    }
}

// ===========================================================================
// Built-in key hash / compare / copy
// ===========================================================================

/// `string_hash` — `string_hash(key, keysize)` via common/hashfn.
fn string_hash(key: &[u8], keysize: Size) -> u32 {
    hashfn_seams::string_hash::call(key, keysize)
}

/// `tag_hash` — `tag_hash(key, keysize)` via common/hashfn.
fn tag_hash(key: &[u8], keysize: Size) -> u32 {
    hashfn_seams::tag_hash::call(key, keysize)
}

/// `uint32_hash` — `hash_uint32(*(uint32 *)key)` via common/hashfn.
fn uint32_hash(key: &[u8], _keysize: Size) -> u32 {
    let v = u32::from_ne_bytes([key[0], key[1], key[2], key[3]]);
    hashfn_seams::hash_bytes_uint32::call(v)
}

/// `string_compare(key1, key2, keysize)` — `strncmp(key1, key2, keysize - 1)`.
fn string_compare(key1: &[u8], key2: &[u8], keysize: Size) -> i32 {
    let n = keysize.saturating_sub(1);
    for i in 0..n {
        let a = key1[i];
        let b = key2[i];
        if a != b {
            return (a as i32) - (b as i32);
        }
        if a == 0 {
            return 0;
        }
    }
    0
}

/// `memcmp(key1, key2, keysize)`.
fn blob_compare(key1: &[u8], key2: &[u8], keysize: Size) -> i32 {
    for i in 0..keysize {
        let a = key1[i];
        let b = key2[i];
        if a != b {
            return (a as i32) - (b as i32);
        }
    }
    0
}

/// `strlcpy(dst, src, keysize)`: copy up to keysize-1 bytes, stop at NUL, always
/// NUL-terminate within keysize.
fn strlcpy(dst: &mut [u8], src: &[u8], keysize: Size) {
    if keysize == 0 {
        return;
    }
    let limit = keysize - 1;
    let mut i = 0usize;
    while i < limit {
        let c = src[i];
        dst[i] = c;
        if c == 0 {
            return;
        }
        i += 1;
    }
    dst[i] = 0;
}

/// `memcpy(dst, src, keysize)`.
fn mem_copy(dst: &mut [u8], src: &[u8], keysize: Size) {
    dst[..keysize].copy_from_slice(&src[..keysize]);
}

// ===========================================================================
// CREATE ROUTINES
// ===========================================================================

/// `hash_create(tabname, nelem, info, flags)` — create (or, with `HASH_ATTACH`,
/// attach to) a dynamic hash table. `Err` carries the C `ereport(ERROR)`s.
pub fn hash_create(tabname: &str, nelem: i64, info: &HASHCTL, flags: i32) -> PgResult<*mut HTAB> {
    // Assert(flags & HASH_ELEM); Assert(info->keysize > 0);
    // Assert(info->entrysize >= info->keysize);
    debug_assert!(flags & HASH_ELEM != 0);
    debug_assert!(info.keysize > 0);
    debug_assert!(info.entrysize >= info.keysize);

    // Allocate the local HTAB shell + a copy of the table name in the table's
    // private context. For a shared table the HTAB is backend-local too (C puts
    // it in TopMemoryContext); we keep it in the per-table arena.
    let tabname_c = CString::new(tabname).unwrap_or_default();
    let mut htab_box = Box::new(htab_zeroed());
    let hashp: *mut HTAB = &mut *htab_box;

    unsafe {
        // hashp->tabname points into the owned CString.
        (*hashp).tabname = tabname_c.as_ptr() as *mut u8;

        // Select the appropriate hash function (see comments at head of file).
        // `is_string_hash` records whether the default string_hash was chosen,
        // since the match/keycopy defaults key off `hashp->hash == string_hash`
        // (a function-pointer comparison in C, unreliable in Rust).
        let mut is_string_hash = false;
        if flags & HASH_FUNCTION != 0 {
            debug_assert!(flags & (HASH_BLOBS | HASH_STRINGS) == 0);
            (*hashp).hash = info.hash;
        } else if flags & HASH_BLOBS != 0 {
            debug_assert!(flags & HASH_STRINGS == 0);
            // Optimize hashing for common key sizes.
            if info.keysize == core::mem::size_of::<u32>() {
                (*hashp).hash = Some(uint32_hash as HashValueFunc);
            } else {
                (*hashp).hash = Some(tag_hash as HashValueFunc);
            }
        } else {
            // string_hash. C asserts HASH_STRINGS and keysize > 8.
            debug_assert!(flags & HASH_STRINGS != 0);
            debug_assert!(info.keysize > 8);
            (*hashp).hash = Some(string_hash as HashValueFunc);
            is_string_hash = true;
        }

        // match defaults to string_compare for string_hash, else memcmp.
        if flags & HASH_COMPARE != 0 {
            (*hashp).match_ = info.match_;
        } else if is_string_hash {
            (*hashp).match_ = Some(string_compare as HashCompareFunc);
        } else {
            (*hashp).match_ = Some(blob_compare as HashCompareFunc);
        }

        // keycopy defaults to strlcpy for string_hash, else memcpy.
        if flags & HASH_KEYCOPY != 0 {
            (*hashp).keycopy = info.keycopy;
        } else if is_string_hash {
            (*hashp).keycopy = Some(strlcpy as HashCopyFunc);
        } else {
            (*hashp).keycopy = Some(mem_copy as HashCopyFunc);
        }

        // Select the entry allocation function. The default (None) allocator is
        // the local DynaHashAlloc, dispatched in hash_alloc.
        if flags & HASH_ALLOC != 0 {
            (*hashp).alloc = info.alloc;
        } else {
            (*hashp).alloc = None;
        }

        // Register the local arena now so DynaHashAlloc can charge to it.
        TABLES.with(|cell| {
            cell.borrow_mut().push((
                hashp as usize,
                TableArena {
                    slabs: Vec::new(),
                    htab: Box::new(htab_zeroed()), // placeholder; replaced below
                    _tabname: tabname_c,
                },
            ));
        });

        let mut attaching = false;

        if flags & HASH_SHARED_MEM != 0 {
            // ctl structure and directory are preallocated for shared tables.
            // HASH_DIRSIZE and HASH_ALLOC had better be set as well.
            (*hashp).hctl = info.hctl;
            (*hashp).dir =
                (info.hctl as *mut u8).add(core::mem::size_of::<HASHHDR>()) as *mut HASHSEGMENT;
            (*hashp).hcxt = ptr::null_mut();
            (*hashp).isshared = true;

            if flags & HASH_ATTACH != 0 {
                // Attaching to an existing table: copy heavily-used values.
                let hctl = (*hashp).hctl;
                (*hashp).keysize = (*hctl).keysize;
                (*hashp).ssize = (*hctl).ssize;
                (*hashp).sshift = (*hctl).sshift;
                attaching = true;
            }
        } else {
            (*hashp).hctl = ptr::null_mut();
            (*hashp).dir = ptr::null_mut();
            (*hashp).hcxt = hashp as *mut u8; // a private "context" token
            (*hashp).isshared = false;
        }

        if !attaching {
            if (*hashp).hctl.is_null() {
                let hdr = hash_alloc(hashp, core::mem::size_of::<HASHHDR>());
                if hdr.is_null() {
                    finalize_register(hashp, htab_box);
                    return Err(PgError::error("out of memory"));
                }
                (*hashp).hctl = hdr as *mut HASHHDR;
            }

            (*hashp).frozen = false;

            hdefault(hashp);

            let hctl = (*hashp).hctl;

            if flags & HASH_PARTITION != 0 {
                // Doesn't make sense to partition a local hash table.
                debug_assert!(flags & HASH_SHARED_MEM != 0);
                (*hctl).num_partitions = info.num_partitions;
            }

            if flags & HASH_SEGMENT != 0 {
                (*hctl).ssize = info.ssize;
                (*hctl).sshift = my_log2(info.ssize);
                debug_assert!(info.ssize == 1i64 << (*hctl).sshift);
            }

            // SHM hash tables have a fixed directory size passed by the caller.
            if flags & HASH_DIRSIZE != 0 {
                (*hctl).max_dsize = info.max_dsize;
                (*hctl).dsize = info.dsize;
            }

            // remember the entry sizes, too.
            (*hctl).keysize = info.keysize;
            (*hctl).entrysize = info.entrysize;

            // local copies of heavily-used constant fields.
            (*hashp).keysize = (*hctl).keysize;
            (*hashp).ssize = (*hctl).ssize;
            (*hashp).sshift = (*hctl).sshift;

            // Build the hash directory structure.
            if !init_htab(hashp, nelem) {
                finalize_register(hashp, htab_box);
                return elog(
                    ERROR,
                    format!("failed to initialize hash table \"{tabname}\""),
                )
                .map(|()| hashp);
            }

            // Preallocate elements (shared: always; local: if nelem < nelem_alloc).
            if (flags & HASH_SHARED_MEM != 0) || (nelem as i32) < (*hctl).nelem_alloc {
                let freelist_partitions = if IS_PARTITIONED(hctl) {
                    NUM_FREELISTS as i32
                } else {
                    1
                };
                let nelem_i = nelem as i32;
                let mut nelem_alloc = nelem_i / freelist_partitions;
                if nelem_alloc <= 0 {
                    nelem_alloc = 1;
                }
                let nelem_alloc_first = if nelem_alloc * freelist_partitions < nelem_i {
                    nelem_i - nelem_alloc * (freelist_partitions - 1)
                } else {
                    nelem_alloc
                };

                for i in 0..freelist_partitions {
                    let temp = if i == 0 { nelem_alloc_first } else { nelem_alloc };
                    if !element_alloc(hashp, temp, i as usize) {
                        finalize_register(hashp, htab_box);
                        return elog(ERROR, "out of memory").map(|()| hashp);
                    }
                }
            }

            if flags & HASH_FIXED_SIZE != 0 {
                (*hashp).isfixed = true;
            }
        }

        // Re-home the boxed HTAB into its arena and return its stable address.
        finalize_register(hashp, htab_box);
        Ok(hashp)
    }
}

/// Move the boxed `HTAB` into its arena slot so the returned `*mut HTAB` stays
/// valid until `hash_destroy`. The address does not change (the `Box` already
/// holds it); we only transfer ownership into the registry.
fn finalize_register(hashp: *mut HTAB, htab_box: Box<HTAB>) {
    TABLES.with(|cell| {
        let mut tables = cell.borrow_mut();
        if let Some((_, arena)) = tables.iter_mut().find(|(k, _)| *k == hashp as usize) {
            arena.htab = htab_box;
        } else {
            // Unreachable in practice (we registered above); avoid leaking by
            // re-inserting.
            core::mem::forget(htab_box);
        }
    });
}

/// A zeroed `HTAB` shell (`MemSet(hashp, 0, sizeof(HTAB))`).
fn htab_zeroed() -> HTAB {
    HTAB {
        hctl: ptr::null_mut(),
        dir: ptr::null_mut(),
        hash: None,
        match_: None,
        keycopy: None,
        alloc: None,
        hcxt: ptr::null_mut(),
        tabname: ptr::null_mut(),
        isshared: false,
        isfixed: false,
        frozen: false,
        keysize: 0,
        ssize: 0,
        sshift: 0,
    }
}

/// `hdefault` — set default `HASHHDR` parameters (`MemSet 0` then defaults).
unsafe fn hdefault(hashp: *mut HTAB) {
    let hctl = (*hashp).hctl;
    ptr::write_bytes(hctl as *mut u8, 0, core::mem::size_of::<HASHHDR>());

    (*hctl).dsize = DEF_DIRSIZE;
    (*hctl).nsegs = 0;
    (*hctl).num_partitions = 0;
    (*hctl).max_dsize = NO_MAX_DSIZE;
    (*hctl).ssize = DEF_SEGSIZE;
    (*hctl).sshift = DEF_SEGSIZE_SHIFT;
}

/// `choose_nelem_alloc` — how many elements to add to the table at once.
fn choose_nelem_alloc(entrysize: Size) -> i32 {
    // NB: must match element_alloc().
    let element_size = MAXALIGN(core::mem::size_of::<HASHELEMENT>()) + MAXALIGN(entrysize);
    let mut alloc_size: usize = 32 * 4; // assume elementSize at least 8
    let mut nelem_alloc: i32;
    loop {
        alloc_size <<= 1;
        nelem_alloc = (alloc_size / element_size) as i32;
        if nelem_alloc >= 32 {
            break;
        }
    }
    nelem_alloc
}

/// `init_htab` — compute derived fields and build the initial directory/segments.
unsafe fn init_htab(hashp: *mut HTAB, nelem: i64) -> bool {
    let hctl = (*hashp).hctl;

    // initialize mutexes if it's a partitioned table.
    if IS_PARTITIONED(hctl) {
        for i in 0..NUM_FREELISTS {
            SpinLockInit(&mut (*hctl).freeList[i].mutex);
        }
    }

    let mut nbuckets = next_pow2_int(nelem);

    // nbuckets must be at least num_partitions.
    while (nbuckets as i64) < (*hctl).num_partitions {
        nbuckets <<= 1;
    }

    (*hctl).max_bucket = (nbuckets - 1) as u32;
    (*hctl).low_mask = (nbuckets - 1) as u32;
    (*hctl).high_mask = ((nbuckets << 1) - 1) as u32;

    // Figure number of directory segments needed, round up to a power of 2.
    let mut nsegs = (nbuckets - 1) as i64 / (*hctl).ssize + 1;
    nsegs = next_pow2_int(nsegs) as i64;

    // Make sure directory is big enough.
    if nsegs > (*hctl).dsize {
        if (*hashp).dir.is_null() {
            (*hctl).dsize = nsegs;
        } else {
            return false;
        }
    }

    // Allocate a directory.
    if (*hashp).dir.is_null() {
        let bytes = (*hctl).dsize as usize * core::mem::size_of::<HASHSEGMENT>();
        let dir = hash_alloc(hashp, bytes);
        if dir.is_null() {
            return false;
        }
        (*hashp).dir = dir as *mut HASHSEGMENT;
    }

    // Allocate initial segments.
    let mut segp = (*hashp).dir;
    while (*hctl).nsegs < nsegs {
        let seg = seg_alloc(hashp);
        if seg.is_null() {
            return false;
        }
        *segp = seg;
        (*hctl).nsegs += 1;
        segp = segp.add(1);
    }

    // Choose number of entries to allocate at a time.
    (*hctl).nelem_alloc = choose_nelem_alloc((*hctl).entrysize);

    true
}

/// `hash_estimate_size(num_entries, entrysize)` — bytes a default-parameter
/// shared table would occupy (HASHHDR + directory + segments + elements).
pub fn hash_estimate_size(num_entries: i64, entrysize: Size) -> Size {
    let n_buckets = next_pow2_long(num_entries);
    let n_segments = next_pow2_long((n_buckets - 1) / DEF_SEGSIZE + 1);
    let mut n_dir_entries = DEF_DIRSIZE;
    while n_dir_entries < n_segments {
        n_dir_entries <<= 1;
    }

    // fixed control info (but not HTAB).
    let mut size = MAXALIGN(core::mem::size_of::<HASHHDR>());
    // directory.
    size += n_dir_entries as usize * core::mem::size_of::<HASHSEGMENT>();
    // segments.
    size += n_segments as usize
        * MAXALIGN(DEF_SEGSIZE as usize * core::mem::size_of::<HASHBUCKET>());
    // elements (allocated in groups of choose_nelem_alloc).
    let element_alloc_cnt = choose_nelem_alloc(entrysize) as i64;
    let n_element_allocs = (num_entries - 1) / element_alloc_cnt + 1;
    let element_size = MAXALIGN(core::mem::size_of::<HASHELEMENT>()) + MAXALIGN(entrysize);
    size += (n_element_allocs * element_alloc_cnt) as usize * element_size;

    size
}

/// `hash_select_dirsize(num_entries)` — power-of-two directory size for a shared
/// table of the given max size (default parameters).
pub fn hash_select_dirsize(num_entries: i64) -> i64 {
    let n_buckets = next_pow2_long(num_entries);
    let n_segments = next_pow2_long((n_buckets - 1) / DEF_SEGSIZE + 1);
    let mut n_dir_entries = DEF_DIRSIZE;
    while n_dir_entries < n_segments {
        n_dir_entries <<= 1;
    }
    n_dir_entries
}

/// `hash_get_shared_size(info, flags)` — initial shared-memory bytes for the
/// HASHHDR + the (non-expansible) directory.
pub fn hash_get_shared_size(info: &HASHCTL, _flags: i32) -> Size {
    // Assert(flags & HASH_DIRSIZE); Assert(info->dsize == info->max_dsize).
    core::mem::size_of::<HASHHDR>()
        + info.dsize as usize * core::mem::size_of::<HASHSEGMENT>()
}

// ===========================================================================
// DESTROY ROUTINES
// ===========================================================================

/// `hash_destroy` — free a local table by destroying its private memory context
/// (drops its arena). For a shared table the segment memory is never freed (C
/// asserts a destroyable table has its own context); we drop the backend-local
/// arena/HTAB shell, which is the most we own.
pub fn hash_destroy(hashp: *mut HTAB) {
    if hashp.is_null() {
        return;
    }
    TABLES.with(|cell| {
        let mut tables = cell.borrow_mut();
        if let Some(pos) = tables.iter().position(|(k, _)| *k == hashp as usize) {
            tables.swap_remove(pos); // drops the arena (== MemoryContextDelete)
        }
    });
}

// ===========================================================================
// SEARCH ROUTINES
// ===========================================================================

/// `get_hash_value` — exported routine to calculate a key's hash value.
pub fn get_hash_value(hashp: *mut HTAB, key_ptr: *const u8) -> u32 {
    unsafe { do_hash(hashp, key_ptr) }
}

/// Dispatch the table's hash function (`hashp->hash(key, keysize)`).
#[inline]
unsafe fn do_hash(hashp: *const HTAB, key_ptr: *const u8) -> u32 {
    let keysize = (*hashp).keysize;
    let bytes = core::slice::from_raw_parts(key_ptr, keysize);
    ((*hashp).hash.expect("hash fn set"))(bytes, keysize)
}

/// Dispatch the table's key comparison, returning 0 on equal.
#[inline]
unsafe fn do_match(hashp: *const HTAB, key1: *const u8, key2: *const u8, keysize: Size) -> i32 {
    let k1 = core::slice::from_raw_parts(key1, keysize);
    let k2 = core::slice::from_raw_parts(key2, keysize);
    ((*hashp).match_.expect("match fn set"))(k1, k2, keysize)
}

/// Dispatch the table's key copy.
#[inline]
unsafe fn do_keycopy(hashp: *const HTAB, dst: *mut u8, src: *const u8, keysize: Size) {
    let d = core::slice::from_raw_parts_mut(dst, keysize);
    let s = core::slice::from_raw_parts(src, keysize);
    ((*hashp).keycopy.expect("keycopy fn set"))(d, s, keysize);
}

/// `calc_bucket(hctl, hash_val)` — hash value -> bucket number.
#[inline]
unsafe fn calc_bucket(hctl: *const HASHHDR, hash_val: u32) -> u32 {
    let mut bucket = hash_val & (*hctl).high_mask;
    if bucket > (*hctl).max_bucket {
        bucket &= (*hctl).low_mask;
    }
    bucket
}

/// `hash_search(hashp, keyPtr, action, foundPtr)` — compute the key's hash and
/// look up/enter/remove it. Returns `(entry_ptr, found)` (entry null on
/// not-found / `HASH_ENTER_NULL` OOM).
pub fn hash_search(
    hashp: *mut HTAB,
    key_ptr: *const u8,
    action: HASHACTION,
) -> PgResult<(*mut u8, bool)> {
    let hashvalue = unsafe { do_hash(hashp, key_ptr) };
    hash_search_with_hash_value(hashp, key_ptr, hashvalue, action)
}

/// `hash_search_with_hash_value` — `hash_search` with a precomputed hash code.
pub fn hash_search_with_hash_value(
    hashp: *mut HTAB,
    key_ptr: *const u8,
    hashvalue: u32,
    action: HASHACTION,
) -> PgResult<(*mut u8, bool)> {
    unsafe {
        let hctl = (*hashp).hctl;
        let freelist_idx = FREELIST_IDX(hctl, hashvalue);
        let keysize = (*hashp).keysize;

        // If inserting, check if it is time to split a bucket.
        if action == HASH_ENTER || action == HASH_ENTER_NULL {
            if (*hctl).freeList[0].nentries > (*hctl).max_bucket as i64
                && !IS_PARTITIONED(hctl)
                && !(*hashp).frozen
                && !has_seq_scans(hashp)
            {
                let _ = expand_table(hashp);
            }
        }

        // Initial lookup.
        let (mut prev_bucket_ptr, _) = hash_initial_lookup(hashp, hashvalue);
        let mut curr_bucket = *prev_bucket_ptr;

        // Follow collision chain looking for a matching key.
        while !curr_bucket.is_null() {
            if (*curr_bucket).hashvalue == hashvalue
                && do_match(hashp, ELEMENTKEY(curr_bucket), key_ptr, keysize) == 0
            {
                break;
            }
            prev_bucket_ptr = &mut (*curr_bucket).link;
            curr_bucket = *prev_bucket_ptr;
        }

        let found = !curr_bucket.is_null();

        match action {
            HASH_FIND => {
                if found {
                    Ok((ELEMENTKEY(curr_bucket), true))
                } else {
                    Ok((ptr::null_mut(), false))
                }
            }
            HASH_REMOVE => {
                if found {
                    if IS_PARTITIONED(hctl) {
                        SpinLockAcquire(&mut (*hctl).freeList[freelist_idx].mutex);
                    }
                    debug_assert!((*hctl).freeList[freelist_idx].nentries > 0);
                    (*hctl).freeList[freelist_idx].nentries -= 1;
                    // remove from hash bucket's chain.
                    *prev_bucket_ptr = (*curr_bucket).link;
                    // add to the appropriate freelist.
                    (*curr_bucket).link = (*hctl).freeList[freelist_idx].freeList;
                    (*hctl).freeList[freelist_idx].freeList = curr_bucket;
                    if IS_PARTITIONED(hctl) {
                        SpinLockRelease(&mut (*hctl).freeList[freelist_idx].mutex);
                    }
                    // NB: dangling-but-usable pointer (element is on freelist).
                    Ok((ELEMENTKEY(curr_bucket), true))
                } else {
                    Ok((ptr::null_mut(), false))
                }
            }
            HASH_ENTER | HASH_ENTER_NULL => {
                if found {
                    return Ok((ELEMENTKEY(curr_bucket), true));
                }
                // disallow inserts if frozen.
                if (*hashp).frozen {
                    elog(
                        ERROR,
                        format!("cannot insert into frozen hashtable \"{}\"", tabname(hashp)),
                    )?;
                }

                let new_bucket = get_hash_entry(hashp, freelist_idx);
                if new_bucket.is_null() {
                    if action == HASH_ENTER_NULL {
                        return Ok((ptr::null_mut(), false));
                    }
                    if (*hashp).isshared {
                        elog(ERROR, "out of shared memory")?;
                    } else {
                        elog(ERROR, "out of memory")?;
                    }
                }

                // link into hashbucket chain.
                *prev_bucket_ptr = new_bucket;
                (*new_bucket).link = ptr::null_mut();
                // copy key into record.
                (*new_bucket).hashvalue = hashvalue;
                do_keycopy(hashp, ELEMENTKEY(new_bucket), key_ptr, keysize);

                Ok((ELEMENTKEY(new_bucket), false))
            }
        }
    }
}

/// `hash_update_hash_key` — change the hash key of an existing table entry.
/// Returns true on success, false if the new key already exists.
pub fn hash_update_hash_key(
    hashp: *mut HTAB,
    existing_entry: *mut u8,
    new_key_ptr: *const u8,
) -> PgResult<bool> {
    unsafe {
        let existing_element = ELEMENT_FROM_KEY(existing_entry);

        // disallow updates if frozen.
        if (*hashp).frozen {
            elog(
                ERROR,
                format!("cannot update in frozen hashtable \"{}\"", tabname(hashp)),
            )?;
        }

        // Lookup the existing element using its saved hash value.
        let (mut prev_bucket_ptr, bucket) =
            hash_initial_lookup(hashp, (*existing_element).hashvalue);
        let mut curr_bucket = *prev_bucket_ptr;
        while !curr_bucket.is_null() {
            if curr_bucket == existing_element {
                break;
            }
            prev_bucket_ptr = &mut (*curr_bucket).link;
            curr_bucket = *prev_bucket_ptr;
        }
        if curr_bucket.is_null() {
            elog(
                ERROR,
                format!(
                    "hash_update_hash_key argument is not in hashtable \"{}\"",
                    tabname(hashp)
                ),
            )?;
        }
        let old_prev_ptr = prev_bucket_ptr;

        // Equivalent of a HASH_ENTER to locate the new chain.
        let keysize = (*hashp).keysize;
        let newhashvalue = do_hash(hashp, new_key_ptr);
        let (mut prev_bucket_ptr2, newbucket) = hash_initial_lookup(hashp, newhashvalue);
        let mut curr_bucket2 = *prev_bucket_ptr2;
        while !curr_bucket2.is_null() {
            if (*curr_bucket2).hashvalue == newhashvalue
                && do_match(hashp, ELEMENTKEY(curr_bucket2), new_key_ptr, keysize) == 0
            {
                break;
            }
            prev_bucket_ptr2 = &mut (*curr_bucket2).link;
            curr_bucket2 = *prev_bucket_ptr2;
        }

        if !curr_bucket2.is_null() {
            return Ok(false); // collision with an existing entry.
        }

        let curr = existing_element;

        // Same bucket: no chain-link changes.
        if bucket != newbucket {
            *old_prev_ptr = (*curr).link;
            *prev_bucket_ptr2 = curr;
            (*curr).link = ptr::null_mut();
        }

        // copy new key into record.
        (*curr).hashvalue = newhashvalue;
        do_keycopy(hashp, ELEMENTKEY(curr), new_key_ptr, keysize);

        Ok(true)
    }
}

/// `get_hash_entry` — allocate a new entry if possible (NULL on OOM).
unsafe fn get_hash_entry(hashp: *mut HTAB, freelist_idx: usize) -> *mut HASHELEMENT {
    let hctl = (*hashp).hctl;
    let mut new_element: *mut HASHELEMENT;

    loop {
        if IS_PARTITIONED(hctl) {
            SpinLockAcquire(&mut (*hctl).freeList[freelist_idx].mutex);
        }

        new_element = (*hctl).freeList[freelist_idx].freeList;

        if !new_element.is_null() {
            break;
        }

        if IS_PARTITIONED(hctl) {
            SpinLockRelease(&mut (*hctl).freeList[freelist_idx].mutex);
        }

        // No free elements in this freelist: try another chunk from the
        // allocator; if that fails (and partitioned) root through other lists.
        if !element_alloc(hashp, (*hctl).nelem_alloc, freelist_idx) {
            if !IS_PARTITIONED(hctl) {
                return ptr::null_mut(); // out of memory
            }

            // try to borrow an element from another freelist.
            let mut borrow_from_idx = freelist_idx;
            loop {
                borrow_from_idx = (borrow_from_idx + 1) % NUM_FREELISTS;
                if borrow_from_idx == freelist_idx {
                    break; // examined all freelists, fail
                }

                SpinLockAcquire(&mut (*hctl).freeList[borrow_from_idx].mutex);
                new_element = (*hctl).freeList[borrow_from_idx].freeList;

                if !new_element.is_null() {
                    (*hctl).freeList[borrow_from_idx].freeList = (*new_element).link;
                    SpinLockRelease(&mut (*hctl).freeList[borrow_from_idx].mutex);

                    // careful: count the new element in its proper freelist.
                    SpinLockAcquire(&mut (*hctl).freeList[freelist_idx].mutex);
                    (*hctl).freeList[freelist_idx].nentries += 1;
                    SpinLockRelease(&mut (*hctl).freeList[freelist_idx].mutex);

                    return new_element;
                }

                SpinLockRelease(&mut (*hctl).freeList[borrow_from_idx].mutex);
            }

            return ptr::null_mut(); // nothing to borrow: out of memory
        }
    }

    // remove entry from freelist, bump nentries.
    (*hctl).freeList[freelist_idx].freeList = (*new_element).link;
    (*hctl).freeList[freelist_idx].nentries += 1;

    if IS_PARTITIONED(hctl) {
        SpinLockRelease(&mut (*hctl).freeList[freelist_idx].mutex);
    }

    new_element
}

/// `hash_get_num_entries` — number of live entries (sum of freelist nentries).
pub fn hash_get_num_entries(hashp: *mut HTAB) -> i64 {
    unsafe {
        let hctl = (*hashp).hctl;
        let mut sum = (*hctl).freeList[0].nentries;
        if IS_PARTITIONED(hctl) {
            for i in 1..NUM_FREELISTS {
                sum += (*hctl).freeList[i].nentries;
            }
        }
        sum
    }
}

// ===========================================================================
// SEQ SCAN
// ===========================================================================

/// `hash_seq_init` — begin a sequential scan over all entries.
pub fn hash_seq_init(status: &mut HASH_SEQ_STATUS, hashp: *mut HTAB) {
    status.hashp = hashp;
    status.curBucket = 0;
    status.curEntry = ptr::null_mut();
    status.hasHashvalue = false;
    unsafe {
        if !(*hashp).frozen {
            register_seq_scan(hashp);
        }
    }
}

/// `hash_seq_init_with_hash_value` — scan only entries with the given hash value.
pub fn hash_seq_init_with_hash_value(
    status: &mut HASH_SEQ_STATUS,
    hashp: *mut HTAB,
    hashvalue: u32,
) {
    hash_seq_init(status, hashp);
    status.hasHashvalue = true;
    status.hashvalue = hashvalue;
    unsafe {
        let (bucket_ptr, bucket) = hash_initial_lookup(hashp, hashvalue);
        status.curBucket = bucket;
        status.curEntry = *bucket_ptr;
    }
}

/// `hash_seq_search` — advance a scan, returning the next entry key pointer or
/// null (which terminates and deregisters the scan).
pub fn hash_seq_search(status: &mut HASH_SEQ_STATUS) -> PgResult<*mut u8> {
    unsafe {
        if status.hasHashvalue {
            // Scan entries only in the current bucket.
            loop {
                let cur_elem = status.curEntry;
                if cur_elem.is_null() {
                    break;
                }
                status.curEntry = (*cur_elem).link;
                if status.hashvalue != (*cur_elem).hashvalue {
                    continue;
                }
                return Ok(ELEMENTKEY(cur_elem));
            }
            hash_seq_term_inner(status.hashp)?;
            return Ok(ptr::null_mut());
        }

        let cur_elem0 = status.curEntry;
        if !cur_elem0.is_null() {
            // Continuing scan of curBucket.
            status.curEntry = (*cur_elem0).link;
            if status.curEntry.is_null() {
                status.curBucket += 1;
            }
            return Ok(ELEMENTKEY(cur_elem0));
        }

        // Search for the next nonempty bucket starting at curBucket.
        let mut cur_bucket = status.curBucket;
        let hashp = status.hashp;
        let hctl = (*hashp).hctl;
        let ssize = (*hashp).ssize;
        let sshift = (*hashp).sshift;
        let max_bucket = (*hctl).max_bucket;

        if cur_bucket > max_bucket {
            hash_seq_term_inner(hashp)?;
            return Ok(ptr::null_mut());
        }

        let mut segment_num = (cur_bucket >> sshift) as i64;
        let mut segment_ndx = MOD(cur_bucket as i64, ssize);

        let mut segp = *(*hashp).dir.offset(segment_num as isize);

        // Pick up the first item in this bucket's chain.
        let mut cur_elem;
        loop {
            cur_elem = *segp.offset(segment_ndx as isize);
            if !cur_elem.is_null() {
                break;
            }
            // empty bucket, advance to next.
            cur_bucket += 1;
            if cur_bucket > max_bucket {
                status.curBucket = cur_bucket;
                hash_seq_term_inner(hashp)?;
                return Ok(ptr::null_mut());
            }
            segment_ndx += 1;
            if segment_ndx >= ssize {
                segment_num += 1;
                segment_ndx = 0;
                segp = *(*hashp).dir.offset(segment_num as isize);
            }
        }

        // Begin scan of curBucket.
        status.curEntry = (*cur_elem).link;
        if status.curEntry.is_null() {
            cur_bucket += 1;
        }
        status.curBucket = cur_bucket;
        Ok(ELEMENTKEY(cur_elem))
    }
}

/// `hash_seq_term` — abandon a scan before exhaustion, deregistering it.
pub fn hash_seq_term(status: &mut HASH_SEQ_STATUS) -> PgResult<()> {
    unsafe { hash_seq_term_inner(status.hashp) }
}

unsafe fn hash_seq_term_inner(hashp: *mut HTAB) -> PgResult<()> {
    if !(*hashp).frozen {
        deregister_seq_scan(hashp)?;
    }
    Ok(())
}

/// `hash_freeze` — forbid future insertions (deletions still allowed).
pub fn hash_freeze(hashp: *mut HTAB) -> PgResult<()> {
    unsafe {
        if (*hashp).isshared {
            return elog(
                ERROR,
                format!("cannot freeze shared hashtable \"{}\"", tabname(hashp)),
            );
        }
        if !(*hashp).frozen && has_seq_scans(hashp) {
            return elog(
                ERROR,
                format!(
                    "cannot freeze hashtable \"{}\" because it has active scans",
                    tabname(hashp)
                ),
            );
        }
        (*hashp).frozen = true;
    }
    Ok(())
}

// ===========================================================================
// UTILITIES — expand / dir_realloc / seg_alloc / element_alloc
// ===========================================================================

/// `expand_table` — add one more hash bucket (never partitioned).
unsafe fn expand_table(hashp: *mut HTAB) -> bool {
    let hctl = (*hashp).hctl;
    debug_assert!(!IS_PARTITIONED(hctl));

    let new_bucket = (*hctl).max_bucket as i64 + 1;
    let new_segnum = new_bucket >> (*hashp).sshift;
    let new_segndx = MOD(new_bucket, (*hashp).ssize);

    if new_segnum >= (*hctl).nsegs {
        // Allocate a new segment if necessary -- could fail if dir full.
        if new_segnum >= (*hctl).dsize {
            if !dir_realloc(hashp) {
                return false;
            }
        }
        let seg = seg_alloc(hashp);
        if seg.is_null() {
            return false;
        }
        *(*hashp).dir.offset(new_segnum as isize) = seg;
        (*hctl).nsegs += 1;
    }

    // OK, we created a new bucket.
    (*hctl).max_bucket += 1;

    // Before changing masks, find old bucket.
    let old_bucket = new_bucket & (*hctl).low_mask as i64;

    // Readjust masks if we crossed a power of 2.
    if new_bucket as u32 > (*hctl).high_mask {
        (*hctl).low_mask = (*hctl).high_mask;
        (*hctl).high_mask = new_bucket as u32 | (*hctl).low_mask;
    }

    // Relocate records to the new bucket.
    let old_segnum = old_bucket >> (*hashp).sshift;
    let old_segndx = MOD(old_bucket, (*hashp).ssize);

    let old_seg = *(*hashp).dir.offset(old_segnum as isize);
    let new_seg = *(*hashp).dir.offset(new_segnum as isize);

    let mut oldlink: *mut *mut HASHELEMENT = old_seg.offset(old_segndx as isize);
    let mut newlink: *mut *mut HASHELEMENT = new_seg.offset(new_segndx as isize);

    let mut curr_element = *oldlink;
    while !curr_element.is_null() {
        let next_element = (*curr_element).link;
        if calc_bucket(hctl, (*curr_element).hashvalue) as i64 == old_bucket {
            *oldlink = curr_element;
            oldlink = &mut (*curr_element).link;
        } else {
            *newlink = curr_element;
            newlink = &mut (*curr_element).link;
        }
        curr_element = next_element;
    }
    // Terminate the rebuilt chains.
    *oldlink = ptr::null_mut();
    *newlink = ptr::null_mut();

    true
}

/// `dir_realloc` — double the directory (copy old, zero tail). Fixed dir -> false.
unsafe fn dir_realloc(hashp: *mut HTAB) -> bool {
    let hctl = (*hashp).hctl;
    if (*hctl).max_dsize != NO_MAX_DSIZE {
        return false;
    }

    let new_dsize = (*hctl).dsize << 1;
    let old_dirsize = (*hctl).dsize as usize * core::mem::size_of::<HASHSEGMENT>();
    let new_dirsize = new_dsize as usize * core::mem::size_of::<HASHSEGMENT>();

    let old_p = (*hashp).dir;
    let p = hash_alloc(hashp, new_dirsize);
    if p.is_null() {
        return false;
    }
    let new_p = p as *mut HASHSEGMENT;
    ptr::copy_nonoverlapping(old_p as *const u8, new_p as *mut u8, old_dirsize);
    ptr::write_bytes((new_p as *mut u8).add(old_dirsize), 0, new_dirsize - old_dirsize);
    (*hashp).dir = new_p;
    (*hctl).dsize = new_dsize;
    // C pfrees old_p; the old dir slab stays in the table's arena (bounded,
    // never reused) and is freed with the table.
    true
}

/// `seg_alloc` — allocate `ssize` bucket heads (zeroed).
unsafe fn seg_alloc(hashp: *mut HTAB) -> HASHSEGMENT {
    let bytes = core::mem::size_of::<HASHBUCKET>() * (*hashp).ssize as usize;
    let segp = hash_alloc(hashp, bytes);
    if segp.is_null() {
        return ptr::null_mut();
    }
    ptr::write_bytes(segp, 0, bytes);
    segp as HASHSEGMENT
}

/// `element_alloc` — allocate some new elements and link them into the
/// indicated freelist.
unsafe fn element_alloc(hashp: *mut HTAB, nelem: i32, freelist_idx: usize) -> bool {
    let hctl = (*hashp).hctl;

    if (*hashp).isfixed {
        return false;
    }

    let element_size =
        MAXALIGN(core::mem::size_of::<HASHELEMENT>()) + MAXALIGN((*hctl).entrysize);

    let total = nelem as usize * element_size;
    let first_element = hash_alloc(hashp, total);
    if first_element.is_null() {
        return false;
    }

    // prepare to link all the new entries into the freelist.
    let mut prev_element: *mut HASHELEMENT = ptr::null_mut();
    let mut tmp = first_element;
    for _ in 0..nelem {
        let el = tmp as *mut HASHELEMENT;
        (*el).link = prev_element;
        prev_element = el;
        tmp = tmp.add(element_size);
    }
    let first = first_element as *mut HASHELEMENT;

    if IS_PARTITIONED(hctl) {
        SpinLockAcquire(&mut (*hctl).freeList[freelist_idx].mutex);
    }

    // freelist could be nonempty if two backends did this concurrently.
    (*first).link = (*hctl).freeList[freelist_idx].freeList;
    (*hctl).freeList[freelist_idx].freeList = prev_element;

    if IS_PARTITIONED(hctl) {
        SpinLockRelease(&mut (*hctl).freeList[freelist_idx].mutex);
    }

    true
}

/// `hash_initial_lookup` — initial bucket lookup, returning `(&segp[ndx], bucket)`.
unsafe fn hash_initial_lookup(hashp: *mut HTAB, hashvalue: u32) -> (*mut *mut HASHELEMENT, u32) {
    let hctl = (*hashp).hctl;
    let bucket = calc_bucket(hctl, hashvalue);

    let segment_num = (bucket >> (*hashp).sshift) as i64;
    let segment_ndx = MOD(bucket as i64, (*hashp).ssize);

    let segp = *(*hashp).dir.offset(segment_num as isize);
    if segp.is_null() {
        hash_corrupted(hashp);
    }
    (segp.offset(segment_ndx as isize), bucket)
}

/// `hash_corrupted` — PANIC for shared, FATAL otherwise.
unsafe fn hash_corrupted(hashp: *mut HTAB) -> ! {
    let level = if (*hashp).isshared { PANIC } else { FATAL };
    let _ = elog(level, format!("hash table \"{}\" corrupted", tabname(hashp)));
    // elog(PANIC/FATAL) does not return in C; the PgResult-based elog returns,
    // so we still diverge. A corrupted shared hashtable is unrecoverable.
    panic!("hash table corrupted");
}

/// Read the table name (`hashp->tabname`) for an error message.
unsafe fn tabname(hashp: *const HTAB) -> alloc::borrow::Cow<'static, str> {
    let p = (*hashp).tabname;
    if p.is_null() {
        return alloc::borrow::Cow::Borrowed("");
    }
    let cstr = core::ffi::CStr::from_ptr(p as *const core::ffi::c_char);
    alloc::borrow::Cow::Owned(cstr.to_string_lossy().into_owned())
}

/// `my_log2` — ceil(log2(num)), clamped for too-large input.
pub fn my_log2(num: i64) -> i32 {
    let mut num = num;
    if num > i64::MAX / 2 {
        num = i64::MAX / 2;
    }
    pg_ceil_log2_64(num)
}

/// `pg_ceil_log2_64(num)` — ceil(log2(num)).
fn pg_ceil_log2_64(num: i64) -> i32 {
    if num <= 1 {
        return 0;
    }
    // ceil(log2(num)) = floor(log2(num - 1)) + 1.
    let v = (num - 1) as u64;
    (64 - v.leading_zeros()) as i32
}

/// `next_pow2_long` — first power of 2 >= num.
fn next_pow2_long(num: i64) -> i64 {
    1i64 << my_log2(num)
}

/// `next_pow2_int` — first power of 2 >= num, bounded to int.
fn next_pow2_int(num: i64) -> i32 {
    let mut num = num;
    if num > i32::MAX as i64 / 2 {
        num = i32::MAX as i64 / 2;
    }
    1i32 << my_log2(num)
}

// ===========================================================================
// SEQ SCAN TRACKING (process-global; modeled as a thread_local backend-global)
// ===========================================================================

thread_local! {
    /// (`HTAB *` as usize, subtransaction nest level) of each active scan.
    static SEQ_SCAN_TABLES: RefCell<Vec<(usize, i32)>> = const { RefCell::new(Vec::new()) };
}

/// Register a table as having an active `hash_seq_search` scan.
unsafe fn register_seq_scan(hashp: *mut HTAB) {
    SEQ_SCAN_TABLES.with(|cell| {
        let mut v = cell.borrow_mut();
        if v.len() >= MAX_SEQ_SCANS {
            let _ = elog(
                ERROR,
                format!(
                    "too many active hash_seq_search scans, cannot start one on \"{}\"",
                    tabname(hashp)
                ),
            );
            return;
        }
        let level = transam_xact_seams::get_current_transaction_nest_level::call();
        v.push((hashp as usize, level));
    });
}

/// Deregister an active scan (search backward, swap-remove like C).
unsafe fn deregister_seq_scan(hashp: *mut HTAB) -> PgResult<()> {
    SEQ_SCAN_TABLES.with(|cell| {
        let mut v = cell.borrow_mut();
        let target = hashp as usize;
        for i in (0..v.len()).rev() {
            if v[i].0 == target {
                let last = v.len() - 1;
                v.swap(i, last);
                v.pop();
                return Ok(());
            }
        }
        elog(
            ERROR,
            format!("no hash_seq_search scan for hash table \"{}\"", tabname(hashp)),
        )
    })
}

/// Check if a table has any active scan.
unsafe fn has_seq_scans(hashp: *mut HTAB) -> bool {
    let target = hashp as usize;
    SEQ_SCAN_TABLES.with(|cell| cell.borrow().iter().any(|(p, _)| *p == target))
}

/// `AtEOXact_HashTables` — clean up open scans at end of transaction.
pub fn AtEOXact_HashTables(is_commit: bool) {
    SEQ_SCAN_TABLES.with(|cell| {
        let mut v = cell.borrow_mut();
        if is_commit {
            for (p, _) in v.iter() {
                let _ = elog(
                    WARNING,
                    format!("leaked hash_seq_search scan for hash table {p:#x}"),
                );
            }
        }
        v.clear();
    });
}

/// `AtEOSubXact_HashTables` — clean up open scans at end of subtransaction.
pub fn AtEOSubXact_HashTables(is_commit: bool, nest_depth: i32) {
    SEQ_SCAN_TABLES.with(|cell| {
        let mut v = cell.borrow_mut();
        let mut i = v.len();
        while i > 0 {
            i -= 1;
            if v[i].1 >= nest_depth {
                if is_commit {
                    let _ = elog(
                        WARNING,
                        format!("leaked hash_seq_search scan for hash table {:#x}", v[i].0),
                    );
                }
                let last = v.len() - 1;
                v.swap(i, last);
                v.pop();
            }
        }
    });
}

#[cfg(test)]
mod tests;
