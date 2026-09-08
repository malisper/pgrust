// C model mapping (shmem.c): "shared memory" is process memory shared across
// backend threads. The segment bump allocator (ShmemAllocRaw/freeoffset) is a
// leaked, zeroed, cache-line-aligned heap allocation per request (C never
// frees shmem either); the ShmemIndex dynahash is a Mutex'd registry (the
// Mutex is C's ShmemIndexLock); ShmemLock stays a spinlock (lwlock.c brackets
// its shared counters with it via the seams). ShmemInitHash registers every
// shared dynahash table under its C name and hands dynahash the
// preallocated header + directory, as C does. Segment mechanics have no
// thread-model counterpart and no port: InitShmemAccess, the InitShmemIndex
// bootstrap (its "ShmemIndex" table never enters the index, so
// pg_shmem_allocations lists no such row in C either), ShmemAllocUnlocked,
// ShmemAddrIsValid. pg_get_shmem_allocations
// 5052 maps ShmemSegHdr->freeoffset to a bump counter over all ShmemAllocRaw
// calls, so `off` reproduces C's within-segment offsets; InitShmemAllocation
// seeds the counter and totalsize the way PGSharedMemoryCreate +
// InitShmemAllocation do, so the trailing free row is C's
// totalsize - freeoffset. The bump counter has no ceiling: C's
// out-of-shared-memory arm (shmem.c:213) needs a CalculateShmemSize that
// sums every subsystem, which ipci does not yet (audit-18.6 b020 b1-b32a6a2c).
// The NUMA builtins (4099/4100) are ported below as C's no-libnuma build.
#![allow(non_snake_case)]

use std::alloc::Layout;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

use elog::ereport;
use types_error::{ErrorLocation, PgResult, ERRCODE_OUT_OF_MEMORY, ERROR};
use types_hash::hsearch::{
    HASHCTL, HASHHDR, HASH_ALLOC, HASH_ATTACH, HASH_DIRSIZE, HASH_SHARED_MEM, HTAB,
};

#[cfg(test)]
mod tests;

// PG_CACHE_LINE_SIZE (pg_config_manual.h).
const PG_CACHE_LINE_SIZE: usize = 128;
// SHMEM_INDEX_KEYSIZE (storage/shmem.h); C truncates longer names into
// colliding keys, so overlength is asserted here instead.
pub const SHMEM_INDEX_KEYSIZE: usize = 48;

#[track_caller]
fn loc(funcname: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, funcname)
}

struct ShmemIndexEnt {
    name: &'static str,
    location: usize,
    size: usize,
    allocated_size: usize,
    // C: (char *) ent->location - (char *) ShmemSegHdr; here the bump
    // counter's value when this entry was carved.
    off: usize,
}

static SHMEM_INDEX: Mutex<Vec<ShmemIndexEnt>> = Mutex::new(Vec::new());
static SHMEM_LOCK: AtomicBool = AtomicBool::new(false);
// ShmemSegHdr->freeoffset counterpart: total bytes bump-allocated so far.
static SHMEM_FREEOFFSET: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
// ShmemSegHdr->totalsize counterpart: CalculateShmemSize's segment size, set
// by InitShmemAllocation (0 until the postmaster's bring-up runs it).
static SHMEM_TOTALSIZE: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

fn CACHELINEALIGN(len: usize) -> Option<usize> {
    len.checked_add(PG_CACHE_LINE_SIZE - 1)
        .map(|n| n & !(PG_CACHE_LINE_SIZE - 1))
}

// MAXALIGN (c.h): 8-byte alignment on every LP64 target.
const fn MAXALIGN(len: usize) -> usize {
    (len + 7) & !7
}

/// InitShmemAllocation (shmem.c:115-142) over the header PGSharedMemoryCreate
/// just filled in (sysv_shmem.c:855-856: totalsize = size, freeoffset =
/// MAXALIGN(sizeof(PGShmemHeader))): the ShmemLock spinlock is carved with
/// ShmemAllocUnlocked (shmem.c:129; :238-262 rounds start and size to
/// MAXALIGN) and the first ShmemAlloc is then pushed to a cache-line boundary
/// (shmem.c:137-138). The Rust ShmemLock is a static, so only the bump
/// accounting is reproduced: the header bytes are added to the counter, and
/// `off` of the first block indexed after this call is C's 128 whenever the
/// bring-up order is C's. (pgrust's postmaster registers "Background Worker
/// Data" and the launcher block before CreateSharedMemoryAndSemaphores —
/// C does both inside CreateOrAttachShmemStructs — so those two carry
/// pre-header offsets until that ordering is C's; the bytes stay accounted.)
pub fn InitShmemAllocation(totalsize: usize) {
    SHMEM_TOTALSIZE.store(totalsize, Ordering::Relaxed);
    SHMEM_FREEOFFSET.fetch_add(initial_freeoffset(), Ordering::Relaxed);
}

// freeoffset after InitShmemAllocation: sysv_shmem.c:856 then shmem.c:129-138.
fn initial_freeoffset() -> usize {
    let freeoffset = MAXALIGN(core::mem::size_of::<types_storage::PGShmemHeader>());
    // slock_t is 1 byte (x86_64) or an int (aarch64): MAXALIGN(1) = MAXALIGN(4).
    let freeoffset = MAXALIGN(freeoffset) + MAXALIGN(1);
    CACHELINEALIGN(freeoffset).expect("segment header fits the counter")
}

// Returns (space, off): `off` is the bump counter's value this request
// consumed (C: newStart = ShmemSegHdr->freeoffset under ShmemLock,
// shmem.c:210-215), i.e. the entry's (char *) location - ShmemSegHdr.
fn ShmemAllocRaw(size: usize, allocated_size: &mut usize) -> (*mut u8, usize) {
    let Some(padded) = CACHELINEALIGN(size) else {
        return (std::ptr::null_mut(), 0);
    };
    *allocated_size = padded;
    // C: freeoffset + padded > totalsize -> NULL; the heap-backed segment's
    // ceiling is the allocator's (isize::MAX), and the caller raises C's
    // out-of-shared-memory ERROR (shmem.c:159), never a panic.
    let Ok(layout) = Layout::from_size_align(padded.max(PG_CACHE_LINE_SIZE), PG_CACHE_LINE_SIZE)
    else {
        return (std::ptr::null_mut(), 0);
    };
    let off = SHMEM_FREEOFFSET.fetch_add(padded, Ordering::Relaxed);
    // SAFETY: layout has non-zero size. Zeroed to match a fresh C segment;
    // leaked for the cluster lifetime, as C shmem is never freed.
    (unsafe { std::alloc::alloc_zeroed(layout) }, off)
}

pub fn ShmemAlloc(size: usize) -> PgResult<*mut u8> {
    let mut allocated_size = 0;
    let (new_space, _) = ShmemAllocRaw(size, &mut allocated_size);
    if new_space.is_null() {
        out_of_shmem(size, "ShmemAlloc")?;
        unreachable!();
    }
    Ok(new_space)
}

pub fn ShmemAllocNoError(size: usize) -> *mut u8 {
    let mut allocated_size = 0;
    ShmemAllocRaw(size, &mut allocated_size).0
}

// shmem.c:428-436: hash_search(ShmemIndex, name, HASH_ENTER_NULL) returning
// NULL (no room for the index entry) is ERRCODE_OUT_OF_MEMORY "could not
// create ShmemIndex entry for data structure \"%s\"", raised after the index
// lock is released -- never an allocator abort.
fn shmem_index_reserve(
    index: &mut Vec<ShmemIndexEnt>,
    additional: usize,
    name: &str,
) -> PgResult<()> {
    if index.try_reserve(additional).is_err() {
        could_not_create_index_entry(name)?;
        unreachable!();
    }
    Ok(())
}

pub fn ShmemInitStruct(name: &str, size: usize) -> PgResult<(*mut u8, bool)> {
    debug_assert!(name.len() < SHMEM_INDEX_KEYSIZE, "shmem index key too long");
    let mut index = SHMEM_INDEX.lock().expect("ShmemIndex poisoned");

    if let Some(ent) = index.iter().find(|e| e.name == name) {
        if ent.size != size {
            let actual = ent.size;
            drop(index);
            size_mismatch(name, size, actual)?;
            unreachable!();
        }
        return Ok((std::ptr::with_exposed_provenance_mut(ent.location), true));
    }

    // C enters the index entry (shmem.c:428) before carving the space
    // (:453); the entry slot is secured first here too.
    if let Err(e) = shmem_index_reserve(&mut index, 1, name) {
        drop(index);
        return Err(e);
    }

    let mut allocated_size = 0;
    let (struct_ptr, off) = ShmemAllocRaw(size, &mut allocated_size);
    if struct_ptr.is_null() {
        drop(index);
        not_enough_shmem(name, size)?;
        unreachable!();
    }
    index.push(ShmemIndexEnt {
        name: String::leak(name.to_owned()),
        location: struct_ptr.expose_provenance(),
        size,
        allocated_size,
        off,
    });
    Ok((struct_ptr, false))
}

/// ShmemInitHash (shmem.c:332-380): create a shared-memory dynahash table.
/// The directory is fixed at hash_select_dirsize(max_size) slots, the
/// allocator is ShmemAllocNoError, and HASH_SHARED_MEM | HASH_ALLOC |
/// HASH_DIRSIZE are added to the caller's flags; the header + directory block
/// (hash_get_shared_size) is registered in the ShmemIndex under `name`, so
/// pg_shmem_allocations lists the table with C's size, and its location is
/// handed to hash_create as info->hctl. A table already in the index is C's
/// HASH_ATTACH arm, dynahash's typed refusal in one address space.
pub fn ShmemInitHash(
    name: &str,
    init_size: i64,
    max_size: i64,
    info: &mut HASHCTL,
    hash_flags: i32,
) -> PgResult<*mut HTAB> {
    info.dsize = dynahash::hash_select_dirsize(max_size);
    info.max_dsize = info.dsize;
    info.alloc = Some(ShmemAllocNoError);
    let mut hash_flags = hash_flags | HASH_SHARED_MEM | HASH_ALLOC | HASH_DIRSIZE;

    let (location, found) =
        ShmemInitStruct(name, dynahash::hash_get_shared_size(info, hash_flags))?;

    if found {
        hash_flags |= HASH_ATTACH;
    }

    info.hctl = location.cast::<HASHHDR>();

    dynahash::hash_create(name, init_size, info, hash_flags)
}

// add_size/mul_size live in mcxt.c at 18.6 (mcxt.c:1684/:1703); the
// shmem-facing names delegate so every caller raises the one C text.
pub fn add_size(s1: usize, s2: usize) -> PgResult<usize> {
    mcx::add_size(s1, s2)
}

pub fn mul_size(s1: usize, s2: usize) -> PgResult<usize> {
    mcx::mul_size(s1, s2)
}

pub fn ShmemLockAcquire() {
    if SHMEM_LOCK.swap(true, Ordering::Acquire) {
        shmem_lock_contended();
    }
}

#[cold]
#[inline(never)]
fn shmem_lock_contended() {
    let mut delay = s_lock_seams::SpinDelayStatus::new(file!(), line!() as i32, "ShmemLock");
    loop {
        if !SHMEM_LOCK.load(Ordering::Relaxed) && !SHMEM_LOCK.swap(true, Ordering::Acquire) {
            break;
        }
        s_lock_seams::perform_spin_delay::call(&mut delay);
    }
    s_lock_seams::finish_spin_delay::call(&delay);
}

pub fn ShmemLockRelease() {
    SHMEM_LOCK.store(false, Ordering::Release);
}

#[cold]
#[inline(never)]
fn out_of_shmem(size: usize, func: &'static str) -> PgResult<()> {
    ereport(ERROR)
        .errcode(ERRCODE_OUT_OF_MEMORY)
        .errmsg(format!("out of shared memory ({size} bytes requested)"))
        .finish(loc(func))
}

#[cold]
#[inline(never)]
fn not_enough_shmem(name: &str, size: usize) -> PgResult<()> {
    ereport(ERROR)
        .errcode(ERRCODE_OUT_OF_MEMORY)
        .errmsg(format!(
            "not enough shared memory for data structure \"{name}\" ({size} bytes requested)"
        ))
        .finish(loc("ShmemInitStruct"))
}

#[cold]
#[inline(never)]
fn could_not_create_index_entry(name: &str) -> PgResult<()> {
    ereport(ERROR)
        .errcode(ERRCODE_OUT_OF_MEMORY)
        .errmsg(format!(
            "could not create ShmemIndex entry for data structure \"{name}\""
        ))
        .finish(loc("ShmemInitStruct"))
}

#[cold]
#[inline(never)]
fn size_mismatch(name: &str, expected: usize, actual: usize) -> PgResult<()> {
    ereport(ERROR)
        .errmsg(format!(
            "ShmemIndex entry size is wrong for data structure \"{name}\": \
             expected {expected}, actual {actual}"
        ))
        .finish(loc("ShmemInitStruct"))
}

// pg_numa_available (shmem.c): pg_numa_init() != -1. This build has no
// libnuma, matching C's src/port/pg_numa.c non-NUMA stub (always -1).
pub fn fc_pg_numa_available(
    _flinfo: Option<&mut types_fmgr::FmgrInfo>,
    _fcinfo: &mut types_fmgr::FunctionCallInfoBaseData,
) -> PgResult<datum::Datum> {
    Ok(datum::Datum::from_bool(false))
}

// pg_get_shmem_allocations_numa (shmem.c): the pg_numa_init() == -1 arm.
pub fn fc_pg_get_shmem_allocations_numa(
    _flinfo: Option<&mut types_fmgr::FmgrInfo>,
    _fcinfo: &mut types_fmgr::FunctionCallInfoBaseData,
) -> PgResult<datum::Datum> {
    ereport(ERROR)
        .errmsg("libnuma initialization failed or NUMA is not supported on this platform")
        .finish(loc("pg_get_shmem_allocations_numa"))?;
    unreachable!()
}

// pg_get_shmem_allocations (shmem.c:491-546): named ShmemIndex rows, then
// <anonymous> (bump usage outside the index), then the free row
// (totalsize - freeoffset, shmem.c:532-536; totalsize is what
// InitShmemAllocation was handed, so the row is C's whenever
// CalculateShmemSize is).
pub fn fc_pg_get_shmem_allocations(
    flinfo: Option<&mut types_fmgr::FmgrInfo>,
    fcinfo: &mut types_fmgr::FunctionCallInfoBaseData,
) -> PgResult<datum::Datum> {
    const COLS: usize = 4;
    let flinfo = flinfo.expect("pg_get_shmem_allocations: resolved FmgrInfo required");
    // SAFETY: executor arms es_query_cxt pre-call; it outlives this frame.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let mut srf = funcapi::InitMaterializedSRF(mcx, flinfo, fcinfo, 0)?;
    debug_assert_eq!(srf.tupdesc.natts as usize, COLS);

    let mut named_allocated: usize = 0;
    {
        let index = SHMEM_INDEX.lock().expect("ShmemIndex poisoned");
        for ent in index.iter() {
            let name = varlena::cstring_to_text(mcx, ent.name.as_bytes())?;
            let values = [
                datum::Datum::from_usize(name.as_bytes().as_ptr() as usize),
                datum::Datum::from_i64(ent.off as i64),
                datum::Datum::from_i64(ent.size as i64),
                datum::Datum::from_i64(ent.allocated_size as i64),
            ];
            named_allocated += ent.allocated_size;
            srf.putvalues(&values, &[false; COLS])?;
        }
    }

    let freeoffset = SHMEM_FREEOFFSET.load(Ordering::Relaxed);
    let anon = varlena::cstring_to_text(mcx, b"<anonymous>")?;
    let anon_size = (freeoffset - named_allocated) as i64;
    srf.putvalues(
        &[
            datum::Datum::from_usize(anon.as_bytes().as_ptr() as usize),
            datum::Datum::null(),
            datum::Datum::from_i64(anon_size),
            datum::Datum::from_i64(anon_size),
        ],
        &[false, true, false, false],
    )?;
    let totalsize = SHMEM_TOTALSIZE.load(Ordering::Relaxed);
    let free = totalsize as i64 - freeoffset as i64;
    srf.putvalues(
        &[
            datum::Datum::null(),
            datum::Datum::from_i64(freeoffset as i64),
            datum::Datum::from_i64(free),
            datum::Datum::from_i64(free),
        ],
        &[true, false, false, false],
    )?;

    Ok(srf.finish(fcinfo))
}

pub const SHMEM_BUILTINS: &[types_fmgr::FmgrBuiltin] = &[
    types_fmgr::FmgrBuiltin {
        foid: 4099,
        name: "pg_numa_available",
        nargs: 0,
        strict: true,
        retset: false,
        func: fc_pg_numa_available,
    },
    types_fmgr::FmgrBuiltin {
        foid: 4100,
        name: "pg_get_shmem_allocations_numa",
        nargs: 0,
        strict: true,
        retset: true,
        func: fc_pg_get_shmem_allocations_numa,
    },
    types_fmgr::FmgrBuiltin {
        foid: 5052,
        name: "pg_get_shmem_allocations",
        nargs: 0,
        strict: true,
        retset: true,
        func: fc_pg_get_shmem_allocations,
    },
];

pub fn init_seams() {
    shmem_seams::shmem_init_struct::set(ShmemInitStruct);
    shmem_seams::shmem_alloc::set(ShmemAlloc);
    shmem_seams::add_size::set(add_size);
    shmem_seams::mul_size::set(mul_size);
    shmem_seams::shmem_lock_acquire::set(ShmemLockAcquire);
    shmem_seams::shmem_lock_release::set(ShmemLockRelease);
}
