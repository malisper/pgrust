//! `backend-storage-ipc-shmem` — port of `src/backend/storage/ipc/shmem.c`:
//! the main shared-memory segment's bump allocator, the ShmemIndex
//! (named-structure lookup), overflow-checked size arithmetic, and the
//! shmem-introspection SQL functions.
//!
//! The segment itself is created elsewhere (`sysv_shmem.c` /
//! `CreateSharedMemoryAndSemaphores`); this crate only carves it up. The C
//! globals (`ShmemSegHdr`, `ShmemBase`, `ShmemEnd`, `ShmemLock`,
//! `ShmemIndex`) are per-process copies of pointers into the shared mapping,
//! inherited at fork — ported as `thread_local!` per AGENTS.md
//! "Backend-global state" (each backend attaches via `InitShmemAccess`, as a
//! forked child does in C). The pointed-to state — the segment header's
//! `freeoffset`, the `ShmemLock` spinlock, the index hash — is the genuinely
//! shared, cross-backend state, synchronized exactly as C does it
//! (`ShmemLock` for the allocator, `ShmemIndexLock` for the index).

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use std::cell::Cell;
use std::ptr::NonNull;

use backend_storage_lmgr_lwlock::LWLockAcquireMain;
use backend_storage_lmgr_s_lock::{s_init_lock, s_lock_macro, s_unlock, Spinlock};
use backend_utils_error::{elog, ereport};
use backend_utils_fmgr_funcapi_seams::{materialized_srf_putvalues, InitMaterializedSRF};
use backend_utils_hash_dynahash_seams as dynahash;
use mcx::Mcx;
use types_core::Size;
// The SQL functions here return the canonical unified `types_tuple::Datum`
// (ByVal/ByRef). They only ever return the by-value null (`Datum::null()`) or a
// by-value bool (`Datum::from_bool`), neither of which borrows from `mcx`, so
// the PGFunction return type is `types_tuple::Datum<'static>`. The `values[]`
// row arrays handed to the `materialized_srf_putvalues` seam are likewise
// canonical: `ByVal` ints (the by-value arm carries a plain `usize` word) and
// `text` columns built via the by-reference `cstring_to_text_v` seam (a
// `Datum::ByRef`). No bare-word `types_datum::Datum` survives here.
use types_error::{
    ErrorLocation, PgResult, DEBUG1, ERRCODE_OUT_OF_MEMORY, ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERROR,
};
use types_hash::hsearch::{
    HASHCTL, HASHHDR, HASH_ALLOC, HASH_DIRSIZE, HASH_ELEM, HASH_SEQ_STATUS, HASH_SHARED_MEM,
    HASH_STRINGS, HTAB,
};
use types_nodes::fmgr::FunctionCallInfoBaseData;
use types_storage::{
    HugePagesStatus, PGShmemHeader, LW_EXCLUSIVE, LW_SHARED, SHMEM_INDEX_LOCK,
};

pub mod fmgr_builtins;

const SRCFILE: &str = "shmem.c";

fn loc(funcname: &str) -> ErrorLocation {
    ErrorLocation::new(SRCFILE, 0, funcname)
}

/// `MAXIMUM_ALIGNOF` (pg_config.h) on the 64-bit migration profile.
const MAXIMUM_ALIGNOF: usize = 8;
/// `PG_CACHE_LINE_SIZE` (pg_config_manual.h).
const PG_CACHE_LINE_SIZE: usize = 128;

/// `MAXALIGN(LEN)` (c.h).
const fn MAXALIGN(len: usize) -> usize {
    (len + MAXIMUM_ALIGNOF - 1) & !(MAXIMUM_ALIGNOF - 1)
}

/// `CACHELINEALIGN(LEN)` (c.h).
const fn CACHELINEALIGN(len: usize) -> usize {
    (len + PG_CACHE_LINE_SIZE - 1) & !(PG_CACHE_LINE_SIZE - 1)
}

/// `TYPEALIGN(ALIGNVAL, LEN)` for a runtime (power-of-two) alignment.
const fn TYPEALIGN(alignval: usize, len: usize) -> usize {
    (len + alignval - 1) & !(alignval - 1)
}

/// `TYPEALIGN_DOWN(ALIGNVAL, LEN)`.
const fn TYPEALIGN_DOWN(alignval: usize, len: usize) -> usize {
    len & !(alignval - 1)
}

/// `SHMEM_INDEX_KEYSIZE` (`storage/shmem.h`) — max size of a data-structure
/// name in the shmem index, including the trailing NUL.
pub const SHMEM_INDEX_KEYSIZE: usize = 48;
/// `SHMEM_INDEX_SIZE` (`storage/shmem.h`) — estimated number of index entries.
pub const SHMEM_INDEX_SIZE: i64 = 64;

/// `ShmemIndexEnt` (`storage/shmem.h`) — one shmem-index hash entry.
/// `repr(C)` because it lives inside the shared dynahash table.
#[repr(C)]
#[derive(Debug)]
pub struct ShmemIndexEnt {
    /// `char key[SHMEM_INDEX_KEYSIZE]` — string name of the structure.
    pub key: [u8; SHMEM_INDEX_KEYSIZE],
    /// `void *location` — location in shared memory.
    pub location: *mut u8,
    /// `Size size` — numbytes requested for the structure.
    pub size: Size,
    /// `Size allocated_size` — actual number of bytes allocated.
    pub allocated_size: Size,
}

thread_local! {
    /// `static PGShmemHeader *ShmemSegHdr` — shared mem segment header.
    static SHMEM_SEG_HDR: Cell<*mut PGShmemHeader> = const { Cell::new(std::ptr::null_mut()) };
    /// `static void *ShmemBase` — start address of shared memory.
    static SHMEM_BASE: Cell<*mut u8> = const { Cell::new(std::ptr::null_mut()) };
    /// `static void *ShmemEnd` — end+1 address of shared memory.
    static SHMEM_END: Cell<*mut u8> = const { Cell::new(std::ptr::null_mut()) };
    /// `slock_t *ShmemLock` — spinlock for shared memory and LWLock
    /// allocation; lives inside the segment.
    static SHMEM_LOCK: Cell<*mut Spinlock> = const { Cell::new(std::ptr::null_mut()) };
    /// `static HTAB *ShmemIndex` — primary index hashtable for shmem (the
    /// per-backend dynahash handle onto the shared table).
    static SHMEM_INDEX: Cell<*mut HTAB> = const { Cell::new(std::ptr::null_mut()) };
    /// `static bool firstNumaTouch = true` — pages must be touched once for
    /// reliable NUMA readouts.
    static FIRST_NUMA_TOUCH: Cell<bool> = const { Cell::new(true) };
}

/// `InitShmemAccess(PGShmemHeader *seghdr)` — set up basic pointers to shared
/// memory.
///
/// # Safety
///
/// `seghdr` must point to a live, writable PostgreSQL shared-memory segment
/// header whose `totalsize` covers the mapping, and the mapping must remain
/// valid for the life of this backend.
pub unsafe fn InitShmemAccess(seghdr: *mut PGShmemHeader) {
    SHMEM_SEG_HDR.set(seghdr);
    SHMEM_BASE.set(seghdr.cast());
    // ShmemEnd = (char *) ShmemBase + seghdr->totalsize;
    // SAFETY: caller guarantees seghdr points at a live header.
    SHMEM_END.set(unsafe { seghdr.cast::<u8>().add((*seghdr).totalsize) });
}

/// `InitShmemAllocation()` — set up shared-memory space allocation.
///
/// This should be called only in the postmaster or a standalone backend.
pub fn InitShmemAllocation() -> PgResult<()> {
    let shmhdr = SHMEM_SEG_HDR.get();
    // Assert(shmhdr != NULL) — dereferenced just below, so check loudly.
    assert!(!shmhdr.is_null(), "InitShmemAccess has not run");

    // Initialize the spinlock used by ShmemAlloc. We must use
    // ShmemAllocUnlocked, since obviously ShmemAlloc can't be called yet.
    let lock = ShmemAllocUnlocked(core::mem::size_of::<Spinlock>())?
        .as_ptr()
        .cast::<Spinlock>();
    // SAFETY: `lock` is a fresh, MAXALIGNed in-segment region of slock_t
    // size; Spinlock is repr(transparent) over AtomicI32.
    s_init_lock(unsafe { &*lock });
    SHMEM_LOCK.set(lock);

    // Allocations after this point should go through ShmemAlloc, which
    // expects to allocate everything on cache line boundaries. Make sure the
    // first allocation begins on a cache line boundary (the C aligns the
    // absolute address, not the offset).
    // SAFETY: shmhdr is a live segment header (asserted above).
    unsafe {
        let aligned = CACHELINEALIGN(shmhdr as usize + (*shmhdr).freeoffset);
        (*shmhdr).freeoffset = aligned - shmhdr as usize;

        // ShmemIndex can't be set up yet (need LWLocks first).
        (*shmhdr).index = std::ptr::null_mut();
    }
    SHMEM_INDEX.set(std::ptr::null_mut());
    Ok(())
}

/// `ShmemAlloc(Size size)` — allocate a cache-line-aligned chunk from shared
/// memory; throws error if the request cannot be satisfied.
pub fn ShmemAlloc(size: Size) -> PgResult<NonNull<u8>> {
    let mut allocated_size = 0;
    let new_space = ShmemAllocRaw(size, &mut allocated_size);
    match NonNull::new(new_space) {
        Some(p) => Ok(p),
        None => {
            ereport(ERROR)
                .errcode(ERRCODE_OUT_OF_MEMORY)
                .errmsg(format!("out of shared memory ({size} bytes requested)"))
                .finish(loc("ShmemAlloc"))?;
            unreachable!("ereport(ERROR) returned");
        }
    }
}

/// `ShmemAllocNoError(Size size)` — as `ShmemAlloc`, but returns NULL if out
/// of space, rather than erroring. (This exact shape is dynahash's
/// `HashAllocFunc`; `ShmemInitHash` installs it as `infoP->alloc`.)
pub fn ShmemAllocNoError(size: Size) -> *mut u8 {
    let mut allocated_size = 0;
    ShmemAllocRaw(size, &mut allocated_size)
}

/// RAII bracket for `SpinLockAcquire(ShmemLock)` / `SpinLockRelease`.
struct ShmemSpinLockGuard<'a>(&'a Spinlock);

impl<'a> ShmemSpinLockGuard<'a> {
    fn acquire(lock: &'a Spinlock, func: &'static str) -> Self {
        s_lock_macro(lock, Some(SRCFILE), 0, Some(func));
        Self(lock)
    }
}

impl Drop for ShmemSpinLockGuard<'_> {
    fn drop(&mut self) {
        s_unlock(self.0);
    }
}

/// `ShmemAllocRaw(Size size, Size *allocated_size)` — allocate a
/// cache-line-aligned chunk under `ShmemLock`, returning NULL when out of
/// space and reporting the padded size actually consumed.
fn ShmemAllocRaw(size: Size, allocated_size: &mut Size) -> *mut u8 {
    // Ensure all space is adequately aligned. Many parts of the system are
    // very sensitive to critical data structures getting split across cache
    // line boundaries, so align the beginning of every allocation to a cache
    // line boundary (not just MAXALIGN).
    let size = CACHELINEALIGN(size);
    *allocated_size = size;

    let shmhdr = SHMEM_SEG_HDR.get();
    // Assert(ShmemSegHdr != NULL) — dereferenced below.
    assert!(!shmhdr.is_null(), "InitShmemAccess has not run");
    let lock = SHMEM_LOCK.get();
    assert!(
        !lock.is_null(),
        "ShmemLock is not initialized (InitShmemAllocation has not run)"
    );

    // SAFETY: shmhdr/lock point into the live segment; freeoffset mutation
    // is serialized by the in-segment ShmemLock spinlock, exactly as in C.
    unsafe {
        let _guard = ShmemSpinLockGuard::acquire(&*lock, "ShmemAllocRaw");

        let new_start = (*shmhdr).freeoffset;
        // C computes newStart + size with unchecked Size arithmetic; a
        // checked add that treats overflow as out-of-space is the same
        // observable behavior without the wraparound.
        match new_start.checked_add(size) {
            Some(new_free) if new_free <= (*shmhdr).totalsize => {
                (*shmhdr).freeoffset = new_free;
                SHMEM_BASE.get().add(new_start)
            }
            _ => std::ptr::null_mut(),
        }
    }
}

/// `ShmemAllocUnlocked(Size size)` — allocate a max-aligned chunk without
/// taking `ShmemLock`. This should be used for, and only for, allocations
/// that must happen before `ShmemLock` is ready.
pub fn ShmemAllocUnlocked(size: Size) -> PgResult<NonNull<u8>> {
    // We consider maxalign, rather than cachealign, sufficient here.
    let size = MAXALIGN(size);

    let shmhdr = SHMEM_SEG_HDR.get();
    // Assert(ShmemSegHdr != NULL) — dereferenced below.
    assert!(!shmhdr.is_null(), "InitShmemAccess has not run");

    // SAFETY: shmhdr points into the live segment; this path runs only in
    // the postmaster/standalone backend before ShmemLock exists, so the
    // unlocked freeoffset access is single-threaded (the C comment's
    // contract).
    unsafe {
        let new_start = (*shmhdr).freeoffset;
        let new_free = new_start.checked_add(size);
        match new_free {
            Some(new_free) if new_free <= (*shmhdr).totalsize => {
                (*shmhdr).freeoffset = new_free;
                Ok(NonNull::new_unchecked(SHMEM_BASE.get().add(new_start)))
            }
            _ => {
                ereport(ERROR)
                    .errcode(ERRCODE_OUT_OF_MEMORY)
                    .errmsg(format!("out of shared memory ({size} bytes requested)"))
                    .finish(loc("ShmemAllocUnlocked"))?;
                unreachable!("ereport(ERROR) returned");
            }
        }
    }
}

/// `ShmemAddrIsValid(const void *addr)` — true if the pointer points within
/// the shared memory segment.
pub fn ShmemAddrIsValid(addr: *const u8) -> bool {
    let addr = addr as usize;
    addr >= SHMEM_BASE.get() as usize && addr < SHMEM_END.get() as usize
}

/// `InitShmemIndex()` — set up or attach to the shmem index table.
pub fn InitShmemIndex() -> PgResult<()> {
    // Since ShmemInitHash calls ShmemInitStruct, which expects the ShmemIndex
    // hashtable to exist already, we have a bit of a circularity problem: the
    // special "ShmemIndex" name tells ShmemInitStruct to fake it.
    let mut info = HASHCTL::new();
    info.keysize = SHMEM_INDEX_KEYSIZE;
    info.entrysize = core::mem::size_of::<ShmemIndexEnt>();

    let index = ShmemInitHash(
        "ShmemIndex",
        SHMEM_INDEX_SIZE,
        SHMEM_INDEX_SIZE,
        &mut info,
        HASH_ELEM | HASH_STRINGS,
    )?;
    SHMEM_INDEX.set(index);
    Ok(())
}

/// `ShmemInitHash(name, init_size, max_size, infoP, hash_flags)` — create and
/// initialize, or attach to, a shared-memory hash table.
///
/// `*infoP` and `hash_flags` must specify at least the entry sizes and key
/// comparison semantics; flag bits specific to shared-memory hash tables are
/// added here. Always throws error rather than returning NULL.
pub fn ShmemInitHash(
    name: &str,
    init_size: i64,
    max_size: i64,
    infoP: &mut HASHCTL,
    hash_flags: i32,
) -> PgResult<*mut HTAB> {
    // Hash tables allocated in shared memory have a fixed directory; make
    // sure it is big enough to start with. The shared memory allocator must
    // be specified too.
    infoP.dsize = dynahash::hash_select_dirsize::call(max_size);
    infoP.max_dsize = infoP.dsize;
    infoP.alloc = Some(ShmemAllocNoError);
    let mut hash_flags = hash_flags | HASH_SHARED_MEM | HASH_ALLOC | HASH_DIRSIZE;

    // look it up in the shmem index
    let (location, found) =
        ShmemInitStruct(name, dynahash::hash_get_shared_size::call(infoP, hash_flags))?;

    // if it already exists, attach to it rather than allocate and initialize
    // new space
    if found {
        hash_flags |= types_hash::hsearch::HASH_ATTACH;
    }

    // Pass location of hashtable header to hash_create
    infoP.hctl = location.as_ptr().cast::<HASHHDR>();

    dynahash::hash_create::call(name, init_size, infoP, hash_flags)
}

/// Build the fixed-size key buffer dynahash's `HASH_STRINGS` support reads
/// (it copies at most `SHMEM_INDEX_KEYSIZE` bytes; C passes the raw `char *`
/// name and asserts `strlen(name) < keysize`).
fn shmem_index_key(name: &str) -> [u8; SHMEM_INDEX_KEYSIZE] {
    let mut key = [0u8; SHMEM_INDEX_KEYSIZE];
    let bytes = name.as_bytes();
    debug_assert!(bytes.len() < SHMEM_INDEX_KEYSIZE, "shmem index key too long");
    let n = bytes.len().min(SHMEM_INDEX_KEYSIZE - 1);
    key[..n].copy_from_slice(&bytes[..n]);
    key
}

/// `ShmemInitStruct(name, size, foundPtr)` — create or attach to a structure
/// in shared memory. The C `*foundPtr` (true when the object was already in
/// the shmem index, hence already initialized) is the second tuple element.
/// Always throws error rather than returning NULL.
pub fn ShmemInitStruct(name: &str, size: Size) -> PgResult<(NonNull<u8>, bool)> {
    let guard = LWLockAcquireMain(
        SHMEM_INDEX_LOCK,
        LW_EXCLUSIVE,
        backend_utils_init_small_seams::my_proc_number::call(),
    )?;

    let shmem_index = SHMEM_INDEX.get();
    if shmem_index.is_null() {
        let shmemseghdr = SHMEM_SEG_HDR.get();

        // Must be trying to create/attach to ShmemIndex itself.
        debug_assert_eq!(name, "ShmemIndex");

        let (struct_ptr, found);
        if backend_utils_init_small_seams::is_under_postmaster::call() {
            // Must be initializing a (non-standalone) backend.
            // Assert(shmemseghdr->index != NULL).
            // SAFETY: shmemseghdr points at the live segment header.
            struct_ptr = unsafe { (*shmemseghdr).index.cast::<u8>() };
            debug_assert!(!struct_ptr.is_null());
            found = true;
        } else {
            // The shmem index doesn't exist: we are bootstrapping it.
            // ShmemIndexLock is released before the index is initialized,
            // which is OK because no other process can be accessing shared
            // memory yet. An ereport from ShmemAlloc releases via the
            // guard's Drop (C error recovery's LWLockReleaseAll).
            struct_ptr = ShmemAlloc(size)?.as_ptr();
            // SAFETY: as above.
            unsafe {
                (*shmemseghdr).index = struct_ptr.cast();
            }
            found = false;
        }
        guard.release()?;
        let p = NonNull::new(struct_ptr)
            .expect("shmem index pointer is null despite bootstrap/attach");
        return Ok((p, found));
    }

    // look it up in the shmem index
    let key = shmem_index_key(name);
    let (result, mut found) = dynahash::hash_search::call(
        shmem_index,
        key.as_ptr(),
        types_hash::hsearch::HASHACTION::HASH_ENTER_NULL,
    )?;

    if result.is_null() {
        guard.release()?;
        ereport(ERROR)
            .errcode(ERRCODE_OUT_OF_MEMORY)
            .errmsg(format!(
                "could not create ShmemIndex entry for data structure \"{name}\""
            ))
            .finish(loc("ShmemInitStruct"))?;
        unreachable!("ereport(ERROR) returned");
    }
    let result = result.cast::<ShmemIndexEnt>();

    let struct_ptr;
    if found {
        // Structure is in the shmem index so someone else has allocated it
        // already. The size better be the same as the size we are trying to
        // initialize to, or there is a name conflict (or worse).
        // SAFETY: `result` is a live entry in the shared index table.
        let entry_size = unsafe { (*result).size };
        if entry_size != size {
            guard.release()?;
            ereport(ERROR)
                .errmsg(format!(
                    "ShmemIndex entry size is wrong for data structure \
                     \"{name}\": expected {size}, actual {entry_size}"
                ))
                .finish(loc("ShmemInitStruct"))?;
            unreachable!("ereport(ERROR) returned");
        }
        // SAFETY: as above.
        struct_ptr = unsafe { (*result).location };
    } else {
        // It isn't in the table yet: allocate and initialize it.
        let mut allocated_size = 0;
        struct_ptr = ShmemAllocRaw(size, &mut allocated_size);
        if struct_ptr.is_null() {
            // out of memory; remove the failed ShmemIndex entry
            dynahash::hash_search::call(
                shmem_index,
                key.as_ptr(),
                types_hash::hsearch::HASHACTION::HASH_REMOVE,
            )?;
            guard.release()?;
            ereport(ERROR)
                .errcode(ERRCODE_OUT_OF_MEMORY)
                .errmsg(format!(
                    "not enough shared memory for data structure \
                     \"{name}\" ({size} bytes requested)"
                ))
                .finish(loc("ShmemInitStruct"))?;
            unreachable!("ereport(ERROR) returned");
        }
        // SAFETY: `result` is a live entry in the shared index table.
        unsafe {
            (*result).size = size;
            (*result).allocated_size = allocated_size;
            (*result).location = struct_ptr;
        }
        found = false;
    }

    guard.release()?;

    debug_assert!(ShmemAddrIsValid(struct_ptr));
    debug_assert_eq!(struct_ptr as usize, CACHELINEALIGN(struct_ptr as usize));

    let p = NonNull::new(struct_ptr).expect("ShmemInitStruct produced a null pointer");
    Ok((p, found))
}

/// `add_size(Size s1, Size s2)` — add two Size values, checking for overflow.
pub fn add_size(s1: Size, s2: Size) -> PgResult<Size> {
    match s1.checked_add(s2) {
        Some(result) => Ok(result),
        None => {
            ereport(ERROR)
                .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
                .errmsg("requested shared memory size overflows size_t")
                .finish(loc("add_size"))?;
            unreachable!("ereport(ERROR) returned");
        }
    }
}

/// `mul_size(Size s1, Size s2)` — multiply two Size values, checking for
/// overflow.
pub fn mul_size(s1: Size, s2: Size) -> PgResult<Size> {
    if s1 == 0 || s2 == 0 {
        return Ok(0);
    }
    match s1.checked_mul(s2) {
        Some(result) => Ok(result),
        None => {
            ereport(ERROR)
                .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
                .errmsg("requested shared memory size overflows size_t")
                .finish(loc("mul_size"))?;
            unreachable!("ereport(ERROR) returned");
        }
    }
}

/// Borrow an index entry's NUL-terminated `key` as `&str` (names are written
/// by `ShmemInitStruct` from `&str`, so they are valid UTF-8; lossy only for
/// a corrupted segment).
fn key_str(key: &[u8; SHMEM_INDEX_KEYSIZE]) -> std::borrow::Cow<'_, str> {
    let end = key.iter().position(|&c| c == 0).unwrap_or(key.len());
    String::from_utf8_lossy(&key[..end])
}

/// `pg_get_shmem_allocations(PG_FUNCTION_ARGS)` — SQL SRF showing allocated
/// shared memory. `mcx` is the per-query context the C `CStringGetTextDatum`
/// pallocs in.
pub fn pg_get_shmem_allocations<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<types_tuple::Datum<'static>> {
    const PG_GET_SHMEM_SIZES_COLS: usize = 4;

    InitMaterializedSRF::call(fcinfo, 0)?;
    let rsinfo = fcinfo
        .resultinfo
        .as_mut()
        .expect("InitMaterializedSRF establishes fcinfo->resultinfo");

    let guard = LWLockAcquireMain(
        SHMEM_INDEX_LOCK,
        LW_SHARED,
        backend_utils_init_small_seams::my_proc_number::call(),
    )?;

    let mut hstat = HASH_SEQ_STATUS::new();
    dynahash::hash_seq_init::call(&mut hstat, SHMEM_INDEX.get());

    let shmem_seg_hdr = SHMEM_SEG_HDR.get();
    let mut named_allocated: Size = 0;
    let mut values: [types_tuple::Datum; PG_GET_SHMEM_SIZES_COLS] =
        core::array::from_fn(|_| types_tuple::Datum::null());
    let mut nulls = [false; PG_GET_SHMEM_SIZES_COLS];

    // output all allocated entries
    loop {
        let ent = dynahash::hash_seq_search::call(&mut hstat)?.cast::<ShmemIndexEnt>();
        if ent.is_null() {
            break;
        }
        // SAFETY: `ent` is a live entry in the shared index, held stable by
        // ShmemIndexLock (entries are never freed; shmem is never returned).
        let ent = unsafe { &*ent };
        values[0] =
            backend_utils_adt_varlena_seams::cstring_to_text_v::call(mcx, &key_str(&ent.key))?;
        values[1] = types_tuple::Datum::from_i64(ent.location as i64 - shmem_seg_hdr as i64);
        values[2] = types_tuple::Datum::from_i64(ent.size as i64);
        values[3] = types_tuple::Datum::from_i64(ent.allocated_size as i64);
        named_allocated += ent.allocated_size;

        materialized_srf_putvalues::call(rsinfo, &values, &nulls)?;
    }

    // SAFETY: live segment header; freeoffset/totalsize reads mirror the C
    // (done under the shared ShmemIndexLock, like the C).
    let (freeoffset, totalsize) = unsafe { ((*shmem_seg_hdr).freeoffset, (*shmem_seg_hdr).totalsize) };

    // output shared memory allocated but not counted via the shmem index
    values[0] = backend_utils_adt_varlena_seams::cstring_to_text_v::call(mcx, "<anonymous>")?;
    nulls[1] = true;
    values[2] = types_tuple::Datum::from_i64((freeoffset - named_allocated) as i64);
    values[3] = values[2].clone();
    materialized_srf_putvalues::call(rsinfo, &values, &nulls)?;

    // output as-of-yet unused shared memory
    nulls[0] = true;
    values[1] = types_tuple::Datum::from_i64(freeoffset as i64);
    nulls[1] = false;
    values[2] = types_tuple::Datum::from_i64((totalsize - freeoffset) as i64);
    values[3] = values[2].clone();
    materialized_srf_putvalues::call(rsinfo, &values, &nulls)?;

    guard.release()?;

    Ok(types_tuple::Datum::null())
}

/// `pg_numa_touch_mem_if_required(ptr)` (`port/pg_numa.h` static inline) —
/// page-fault a shared-memory page so the NUMA inquiry returns a real node.
///
/// # Safety
///
/// `ptr` must point at least 8 readable bytes of the live mapping.
unsafe fn pg_numa_touch_mem_if_required(ptr: *const u8) {
    // volatile uint64 touch = *(volatile uint64 *) ptr;
    // SAFETY: caller guarantees readability; the pointer is page-aligned.
    let _touch = unsafe { core::ptr::read_volatile(ptr.cast::<u64>()) };
}

/// `pg_get_shmem_allocations_numa(PG_FUNCTION_ARGS)` — SQL SRF showing NUMA
/// memory nodes for allocated shared memory.
///
/// `huge_pages_status` is the C global of the same name (sysv_shmem.c GUC
/// status), passed explicitly per the no-ambient-global rule; `mcx` is the
/// per-query context the C `palloc`s in.
pub fn pg_get_shmem_allocations_numa<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
    huge_pages_status: HugePagesStatus,
) -> PgResult<types_tuple::Datum<'static>> {
    const PG_GET_SHMEM_NUMA_SIZES_COLS: usize = 3;

    if port_pg_numa_seams::pg_numa_init::call() == -1 {
        elog(
            ERROR,
            "libnuma initialization failed or NUMA is not supported on this platform",
        )?;
        unreachable!("elog(ERROR) returned");
    }

    InitMaterializedSRF::call(fcinfo, 0)?;
    let rsinfo = fcinfo
        .resultinfo
        .as_mut()
        .expect("InitMaterializedSRF establishes fcinfo->resultinfo");

    let max_nodes = port_pg_numa_seams::pg_numa_get_max_node::call() as u64;
    // nodes = palloc(sizeof(Size) * (max_nodes + 2));
    let mut nodes: mcx::PgVec<'_, Size> = mcx::vec_with_capacity_in(mcx, (max_nodes + 2) as usize)?;
    nodes.resize((max_nodes + 2) as usize, 0);

    // To map each allocation to NUMA nodes we align each allocation's
    // start/end addresses to OS page boundaries and query NUMA node
    // information for all pages spanning the allocation.
    let os_page_size = pg_get_shmem_pagesize(huge_pages_status);

    let shmem_seg_hdr = SHMEM_SEG_HDR.get();
    assert!(!shmem_seg_hdr.is_null(), "InitShmemAccess has not run");
    // Allocate page pointers and status based on total shared memory size;
    // add 1 because the segments may not align to OS pages.
    // SAFETY: live segment header.
    let shm_total_page_count = (unsafe { (*shmem_seg_hdr).totalsize } / os_page_size) + 1;
    // page_ptrs = palloc0(...); pages_status = palloc(...);
    let mut page_ptrs: mcx::PgVec<'_, *mut u8> = mcx::vec_with_capacity_in(mcx, shm_total_page_count)?;
    page_ptrs.resize(shm_total_page_count, std::ptr::null_mut());
    let mut pages_status: mcx::PgVec<'_, i32> = mcx::vec_with_capacity_in(mcx, shm_total_page_count)?;
    pages_status.resize(shm_total_page_count, 0);

    if FIRST_NUMA_TOUCH.get() {
        elog(
            DEBUG1,
            "NUMA: page-faulting shared memory segments for proper NUMA readouts",
        )?;
    }

    let guard = LWLockAcquireMain(
        SHMEM_INDEX_LOCK,
        LW_SHARED,
        backend_utils_init_small_seams::my_proc_number::call(),
    )?;

    let mut hstat = HASH_SEQ_STATUS::new();
    dynahash::hash_seq_init::call(&mut hstat, SHMEM_INDEX.get());

    let mut values: [types_tuple::Datum; PG_GET_SHMEM_NUMA_SIZES_COLS] =
        core::array::from_fn(|_| types_tuple::Datum::null());

    // output all allocated entries
    loop {
        let ent = dynahash::hash_seq_search::call(&mut hstat)?.cast::<ShmemIndexEnt>();
        if ent.is_null() {
            break;
        }
        // SAFETY: live index entry, held stable by ShmemIndexLock.
        let ent = unsafe { &*ent };

        // Calculate the range of OS pages used by this segment: the segment
        // may start/end half-way through a page, so align start down and end
        // up and count the pages between.
        let startptr = TYPEALIGN_DOWN(os_page_size, ent.location as usize);
        let endptr = TYPEALIGN(
            os_page_size,
            ent.location as usize + ent.allocated_size,
        );
        let total_len = endptr - startptr;
        let shm_ent_page_count = total_len / os_page_size;

        // If we ever get 0xff (-1) back from kernel inquiry, then we
        // probably have a bug in mapping buffers to OS pages.
        pages_status[..shm_ent_page_count].fill(-1);

        // Set up page_ptrs[] with pointers to all OS pages for this segment;
        // touch the pages so the inquiry doesn't return -2 (ENOENT,
        // unmapped/unallocated pages).
        for i in 0..shm_ent_page_count {
            page_ptrs[i] = (startptr + i * os_page_size) as *mut u8;

            if FIRST_NUMA_TOUCH.get() {
                // SAFETY: the pointer covers a page of the live mapping
                // spanned by this index entry's allocation.
                unsafe { pg_numa_touch_mem_if_required(page_ptrs[i]) };
            }

            backend_tcop_postgres_seams::check_for_interrupts::call()?;
        }

        if port_pg_numa_seams::pg_numa_query_pages::call(
            0,
            &mut page_ptrs[..shm_ent_page_count],
            &mut pages_status[..shm_ent_page_count],
        ) == -1
        {
            // elog(ERROR, "failed NUMA pages inquiry status: %m")
            let en = backend_utils_error::errno::current_errno();
            ereport(ERROR)
                .with_saved_errno(en)
                .errmsg("failed NUMA pages inquiry status: %m")
                .finish(loc("pg_get_shmem_allocations_numa"))?;
            unreachable!("ereport(ERROR) returned");
        }

        // Count the number of NUMA nodes used for this shared memory entry.
        nodes.fill(0);

        for i in 0..shm_ent_page_count {
            let s = pages_status[i];

            // Ensure we are adding only a valid index to the array.
            if s >= 0 && (s as u64) <= max_nodes {
                // valid NUMA node
                nodes[s as usize] += 1;
                continue;
            } else if s == -2 {
                // -2 means ENOENT (e.g. page was moved to swap)
                nodes[(max_nodes + 1) as usize] += 1;
                continue;
            }

            elog(
                ERROR,
                format!("invalid NUMA node id outside of allowed range [0, {max_nodes}]: {s}"),
            )?;
            unreachable!("elog(ERROR) returned");
        }

        // no NULLs for regular nodes
        let mut nulls = [false; PG_GET_SHMEM_NUMA_SIZES_COLS];

        // Add one entry for each NUMA node, including those without
        // allocated memory for this segment.
        for i in 0..=max_nodes {
            values[0] =
                backend_utils_adt_varlena_seams::cstring_to_text_v::call(mcx, &key_str(&ent.key))?;
            // C: values[1] = i (a raw int assigned into the Datum word).
            values[1] = types_tuple::Datum::from_i64(i as i64);
            values[2] = types_tuple::Datum::from_i64((nodes[i as usize] * os_page_size) as i64);

            materialized_srf_putvalues::call(rsinfo, &values, &nulls)?;
        }

        // The last entry is used for pages without a NUMA node.
        nulls[1] = true;
        values[0] =
            backend_utils_adt_varlena_seams::cstring_to_text_v::call(mcx, &key_str(&ent.key))?;
        values[2] = types_tuple::Datum::from_i64((nodes[(max_nodes + 1) as usize] * os_page_size) as i64);

        materialized_srf_putvalues::call(rsinfo, &values, &nulls)?;
    }

    guard.release()?;
    FIRST_NUMA_TOUCH.set(false);

    Ok(types_tuple::Datum::null())
}

/// `pg_get_shmem_pagesize()` — the memory page size used for the shared
/// memory segment: the huge page size if the segment was allocated with huge
/// pages, the regular page size otherwise. Should be used only after the
/// server is started.
///
/// `huge_pages_status` is the C global (sysv_shmem.c), passed explicitly per
/// the no-ambient-global rule.
pub fn pg_get_shmem_pagesize(huge_pages_status: HugePagesStatus) -> Size {
    // WIN32 GetSystemInfo branch not ported.
    // SAFETY: sysconf is always safe to call.
    let mut os_page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as Size;

    // Assert(IsUnderPostmaster) not reproduced (debug-only C invariant about
    // the caller, not a data precondition).
    debug_assert!(huge_pages_status != HugePagesStatus::HUGE_PAGES_UNKNOWN);

    if huge_pages_status == HugePagesStatus::HUGE_PAGES_ON {
        let (hugepagesize, _mmap_flags) =
            backend_port_sysv_shmem_seams::get_huge_page_size::call();
        os_page_size = hugepagesize;
    }

    os_page_size
}

/// `pg_numa_available(PG_FUNCTION_ARGS)` — SQL function: whether NUMA
/// inquiry is supported.
pub fn pg_numa_available() -> types_tuple::Datum<'static> {
    types_tuple::Datum::from_bool(port_pg_numa_seams::pg_numa_init::call() != -1)
}

// ---------------------------------------------------------------------------
// Seam installation.
// ---------------------------------------------------------------------------

fn shmem_lock_acquire_impl() {
    let lock = SHMEM_LOCK.get();
    assert!(
        !lock.is_null(),
        "ShmemLock is not initialized (InitShmemAllocation has not run)"
    );
    // SAFETY: the lock lives in the segment for the backend's lifetime.
    s_lock_macro(unsafe { &*lock }, Some(SRCFILE), 0, Some("SpinLockAcquire"));
}

fn shmem_lock_release_impl() {
    let lock = SHMEM_LOCK.get();
    assert!(
        !lock.is_null(),
        "ShmemLock is not initialized (InitShmemAllocation has not run)"
    );
    // SAFETY: as in `shmem_lock_acquire_impl`.
    s_unlock(unsafe { &*lock });
}

/// Install every seam in `backend-storage-ipc-shmem-seams`.
pub fn init_seams() {
    backend_storage_ipc_shmem_seams::shmem_init_struct::set(|name, size| {
        ShmemInitStruct(name, size).map(|(p, found)| (p.as_ptr(), found))
    });
    backend_storage_ipc_shmem_seams::add_size::set(add_size);
    backend_storage_ipc_shmem_seams::mul_size::set(mul_size);
    backend_storage_ipc_shmem_seams::shmem_lock_acquire::set(shmem_lock_acquire_impl);
    backend_storage_ipc_shmem_seams::shmem_lock_release::set(shmem_lock_release_impl);
    backend_storage_ipc_shmem_seams::init_shmem_access::set(|seghdr| unsafe {
        InitShmemAccess(seghdr)
    });
    backend_storage_ipc_shmem_seams::init_shmem_allocation::set(|| {
        InitShmemAllocation().expect("InitShmemAllocation failed")
    });
    backend_storage_ipc_shmem_seams::init_shmem_index::set(InitShmemIndex);
    backend_storage_ipc_shmem_seams::shmem_alloc_unlocked::set(|size| {
        ShmemAllocUnlocked(size).map(|p| p.as_ptr())
    });

    fmgr_builtins::register_backend_storage_ipc_shmem_builtins();
}

#[cfg(test)]
mod tests;
