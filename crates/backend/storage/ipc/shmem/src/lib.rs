// C model mapping (shmem.c): "shared memory" is process memory shared across
// backend threads. The segment bump allocator (ShmemAllocRaw/freeoffset) is a
// leaked, zeroed, cache-line-aligned heap allocation per request (C never
// frees shmem either); the ShmemIndex dynahash is a Mutex'd registry (the
// Mutex is C's ShmemIndexLock); ShmemLock stays a spinlock (lwlock.c brackets
// its shared counters with it via the seams). Segment mechanics have no
// thread-model counterpart and no port: InitShmemAccess/Allocation/Index
// bootstrap, ShmemAllocUnlocked, ShmemAddrIsValid, pagesize/NUMA SRFs.
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

fn loc(funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("shmem.c", 0, funcname)
}

struct ShmemIndexEnt {
    name: &'static str,
    location: usize,
    size: usize,
    #[allow(dead_code)]
    allocated_size: usize,
}

static SHMEM_INDEX: Mutex<Vec<ShmemIndexEnt>> = Mutex::new(Vec::new());
static SHMEM_LOCK: AtomicBool = AtomicBool::new(false);

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
    while SHMEM_LOCK.swap(true, Ordering::Acquire) {
        std::hint::spin_loop();
    }
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

pub fn init_seams() {
    shmem_seams::shmem_init_struct::set(ShmemInitStruct);
    shmem_seams::shmem_alloc::set(ShmemAlloc);
    shmem_seams::add_size::set(add_size);
    shmem_seams::mul_size::set(mul_size);
    shmem_seams::shmem_lock_acquire::set(ShmemLockAcquire);
    shmem_seams::shmem_lock_release::set(ShmemLockRelease);
}
