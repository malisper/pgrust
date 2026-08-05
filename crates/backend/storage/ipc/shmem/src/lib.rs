// C model mapping (shmem.c): "shared memory" is process memory shared across
// backend threads. The segment bump allocator (ShmemAllocRaw/freeoffset) is a
// leaked, zeroed, cache-line-aligned heap allocation per request (C never
// frees shmem either); the ShmemIndex dynahash is a Mutex'd registry (the
// Mutex is C's ShmemIndexLock); ShmemLock stays a spinlock (lwlock.c brackets
// its shared counters with it via the seams). Segment mechanics have no
// thread-model counterpart and no port: InitShmemAccess/Allocation/Index
// bootstrap, ShmemAllocUnlocked, ShmemAddrIsValid. pg_get_shmem_allocations
// 5052 maps ShmemSegHdr->freeoffset to a bump counter over all ShmemAllocRaw
// calls, so `off` reproduces C's within-segment offsets; the trailing free
// row is size 0 (the malloc-backed segment reserves nothing ahead).
// The NUMA builtins (4099/4100) are ported below as C's no-libnuma build.
#![allow(non_snake_case)]

use std::alloc::Layout;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

use elog::ereport;
use types_error::{
    ErrorLocation, PgResult, ERRCODE_OUT_OF_MEMORY, ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERROR,
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

fn CACHELINEALIGN(len: usize) -> Option<usize> {
    len.checked_add(PG_CACHE_LINE_SIZE - 1)
        .map(|n| n & !(PG_CACHE_LINE_SIZE - 1))
}

fn ShmemAllocRaw(size: usize, allocated_size: &mut usize) -> *mut u8 {
    let Some(padded) = CACHELINEALIGN(size) else {
        return std::ptr::null_mut();
    };
    *allocated_size = padded;
    let layout = Layout::from_size_align(padded.max(PG_CACHE_LINE_SIZE), PG_CACHE_LINE_SIZE)
        .expect("shmem layout");
    SHMEM_FREEOFFSET.fetch_add(padded, Ordering::Relaxed);
    // SAFETY: layout has non-zero size. Zeroed to match a fresh C segment;
    // leaked for the cluster lifetime, as C shmem is never freed.
    unsafe { std::alloc::alloc_zeroed(layout) }
}

pub fn ShmemAlloc(size: usize) -> PgResult<*mut u8> {
    let mut allocated_size = 0;
    let new_space = ShmemAllocRaw(size, &mut allocated_size);
    if new_space.is_null() {
        out_of_shmem(size, "ShmemAlloc")?;
        unreachable!();
    }
    Ok(new_space)
}

pub fn ShmemAllocNoError(size: usize) -> *mut u8 {
    let mut allocated_size = 0;
    ShmemAllocRaw(size, &mut allocated_size)
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

    let mut allocated_size = 0;
    let struct_ptr = ShmemAllocRaw(size, &mut allocated_size);
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
        off: SHMEM_FREEOFFSET.load(Ordering::Relaxed) - allocated_size,
    });
    Ok((struct_ptr, false))
}

pub fn add_size(s1: usize, s2: usize) -> PgResult<usize> {
    match s1.checked_add(s2) {
        Some(result) => Ok(result),
        None => {
            size_overflow("add_size")?;
            unreachable!();
        }
    }
}

pub fn mul_size(s1: usize, s2: usize) -> PgResult<usize> {
    if s1 == 0 || s2 == 0 {
        return Ok(0);
    }
    match s1.checked_mul(s2) {
        Some(result) => Ok(result),
        None => {
            size_overflow("mul_size")?;
            unreachable!();
        }
    }
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
fn size_mismatch(name: &str, expected: usize, actual: usize) -> PgResult<()> {
    ereport(ERROR)
        .errmsg(format!(
            "ShmemIndex entry size is wrong for data structure \"{name}\": \
             expected {expected}, actual {actual}"
        ))
        .finish(loc("ShmemInitStruct"))
}

#[cold]
#[inline(never)]
fn size_overflow(func: &'static str) -> PgResult<()> {
    ereport(ERROR)
        .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
        .errmsg("requested shared memory size overflows size_t")
        .finish(loc(func))
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

// pg_get_shmem_allocations (shmem.c): named ShmemIndex rows, then
// <anonymous> (bump usage outside the index), then the free row (size 0
// here — the malloc-backed segment reserves nothing ahead).
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
    srf.putvalues(
        &[
            datum::Datum::null(),
            datum::Datum::from_i64(freeoffset as i64),
            datum::Datum::from_i64(0),
            datum::Datum::from_i64(0),
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
