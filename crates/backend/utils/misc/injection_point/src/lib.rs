#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

//! `src/backend/utils/misc/injection_point.c` — the injection-points control
//! and run machinery.
//!
//! Injection points run arbitrary callbacks at named code spots so tests can
//! deterministically force races/edge-cases. In C this is gated behind
//! `--enable-injection-points` (`USE_INJECTION_POINTS`); pgrust compiles it in
//! unconditionally — the registry check is cheap (a single shared-memory atomic
//! read of `max_inuse`) when nothing is attached, exactly like C with the flag
//! on.
//!
//! Two cooperating pieces, mirroring C:
//!   * the **shared-memory registry** (`InjectionPointsCtl`): a small fixed
//!     array of attached points (name → library+function+private_data),
//!     lock-free-readable via a per-entry generation counter (see the long C
//!     comment on `InjectionPointEntry`). Mutated under `InjectionPointLock`.
//!   * the **backend-local cache**: a per-process map of already-resolved
//!     callbacks, validated against the shmem generation on each lookup.
//!
//! C resolves a point's `library`+`function` to a C function pointer with
//! `load_external_function` (dlopen). pgrust has no C ABI, so callbacks are
//! resolved through a **builtin callback registry** ([`register_callback`]) that
//! modules (the `injection_points` test extension) populate from their
//! `init_seams()` — the moral equivalent of the symbols `injection_points.so`
//! would export.

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::atomic::{fence, AtomicPtr, AtomicU32, AtomicU64, Ordering};
use std::sync::RwLock;

use ::condvar::ConditionVariable;
use ::types_core::Size;
use ::types_error::{PgResult, ERROR};
use ::types_storage::storage::{Spinlock, LW_EXCLUSIVE};
use ::utils_error::elog;

use ::lwlock::LWLockAcquireMain;

/// Maximum number of concurrent waits (the `INJ_MAX_WAIT` of the test
/// extension's `InjectionPointSharedState`). Co-located in the core shmem
/// region so the test extension needs no extra shmem wiring.
const INJ_MAX_WAIT: usize = 8;

/// `InjectionPointLock` — offset 51 in `MainLWLockArray` (`lwlocklist.h`:
/// `PG_LWLOCK(51, InjectionPoint)`).
const INJECTION_POINT_LOCK: usize = 51;

// Field sizes (injection_point.c).
const INJ_NAME_MAXLEN: usize = 64;
const INJ_LIB_MAXLEN: usize = 128;
const INJ_FUNC_MAXLEN: usize = 128;
const INJ_PRIVATE_MAXLEN: usize = 1024;

const MAX_INJECTION_POINTS: usize = 128;

/// The callback launched by an injection point — the Rust analogue of the C
/// `InjectionPointCallback` typedef `void (*)(const char *name, const void
/// *private_data, void *arg)`. `private_data` is the opaque blob stored when the
/// point was attached; `arg` is the optional string passed at run time. Errors
/// propagate as `PgError` (the C `elog(ERROR)` longjmp).
pub type InjectionPointCallback = fn(name: &str, private_data: &[u8], arg: Option<&str>) -> PgResult<()>;

// ===========================================================================
// Builtin callback registry (replaces dlopen / load_external_function)
// ===========================================================================

/// Registry of `(library, function)` → Rust callback. The C loader resolves a
/// point's `library`/`function` to a function pointer in the named `.so`; here
/// modules register their callbacks under the same (library, function) keys.
static CALLBACKS: RwLock<Option<HashMap<(String, String), InjectionPointCallback>>> =
    RwLock::new(None);

/// Register a callback under `(library, function)`. Called by a module's
/// `init_seams()` — the equivalent of `injection_points.so` exporting
/// `injection_error`/`injection_notice`/`injection_wait`.
pub fn register_callback(library: &str, function: &str, callback: InjectionPointCallback) {
    let mut guard = CALLBACKS.write().expect("injection point callback registry poisoned");
    guard
        .get_or_insert_with(HashMap::new)
        .insert((library.to_string(), function.to_string()), callback);
}

/// Resolve `(library, function)` to its registered callback — the moral
/// equivalent of `load_external_function`.
fn lookup_callback(library: &str, function: &str) -> Option<InjectionPointCallback> {
    let guard = CALLBACKS.read().expect("injection point callback registry poisoned");
    guard
        .as_ref()
        .and_then(|m| m.get(&(library.to_string(), function.to_string())).copied())
}

// ===========================================================================
// Shared-memory registry (InjectionPointsCtl)
// ===========================================================================

/// `InjectionPointEntry` (injection_point.c) — a single attached point, stored
/// in cross-process shared memory. `#[repr(C)]` + atomics/byte-arrays only, so
/// it is valid to share across `fork(2)` in a MAP_SHARED segment.
///
/// `generation` is the lock-free-read protocol from the C comment: even = slot
/// free, odd = in use; a reader re-reads it after copying the other fields to
/// detect a concurrent recycle. Writers additionally hold `InjectionPointLock`.
#[repr(C)]
struct InjectionPointEntry {
    generation: AtomicU64,
    name: [u8; INJ_NAME_MAXLEN],
    library: [u8; INJ_LIB_MAXLEN],
    function: [u8; INJ_FUNC_MAXLEN],
    private_data: [u8; INJ_PRIVATE_MAXLEN],
}

/// `InjectionPointsCtl` (injection_point.c) — the whole shared array plus
/// `max_inuse` (highest index in use + 1, an optimization so the common
/// nothing-attached case reads a single atomic).
#[repr(C)]
struct InjectionPointsCtl {
    max_inuse: AtomicU32,
    entries: [InjectionPointEntry; MAX_INJECTION_POINTS],

    /// The test extension's `InjectionPointSharedState`, co-located here so the
    /// `injection_points` module needs no separate shmem allocation (C uses a
    /// DSM or `shmem_startup_hook`; pgrust does not run extension shmem hooks,
    /// so we reserve the slot in the core region already wired into ipci.c).
    /// `wait_lock` protects `wait_counts`/`wait_names`.
    wait_lock: Spinlock,
    wait_counts: [AtomicU32; INJ_MAX_WAIT],
    wait_names: [[u8; INJ_NAME_MAXLEN]; INJ_MAX_WAIT],
    /// One condition variable broadcast on every `injection_points_wakeup`.
    wait_point: ConditionVariable,
    /// First-time-init guard for `wait_point`/`wait_lock` (the test extension
    /// lazily inits its DSM state; here the postmaster `!found` path does it).
    wait_inited: AtomicU32,
}

// SAFETY: every field is an atomic, a `Spinlock` (atomic word), a `#[repr(C)]`
// `ConditionVariable` (atomic spinlock + Copy proclist indices), or a fixed
// byte array mutated only through a raw pointer while the relevant lock
// (`InjectionPointLock` for entries via the generation protocol, `wait_lock`
// for the wait state) is held. The struct is only ever reached through a
// shared raw pointer into the cross-process segment; the lock discipline
// (mirroring the C originals) makes concurrent access safe.
unsafe impl Sync for InjectionPointsCtl {}

/// `NON_EXEC_STATIC InjectionPointsCtl *ActiveInjectionPoints` — base of the
/// shared registry in the MAP_SHARED segment, recorded by
/// [`InjectionPointShmemInit`]. NULL until then.
static ACTIVE_INJECTION_POINTS: AtomicPtr<InjectionPointsCtl> =
    AtomicPtr::new(core::ptr::null_mut());

/// `ActiveInjectionPoints` as a raw `*mut` — panics if shmem init has not run.
/// All mutation of the (non-atomic) byte arrays derives its `*mut` provenance
/// from this pointer (never from `&T as *mut T`, which is UB), serialized by
/// the relevant lock.
fn ctl_ptr() -> *mut InjectionPointsCtl {
    let p = ACTIVE_INJECTION_POINTS.load(Ordering::Relaxed);
    assert!(!p.is_null(), "ActiveInjectionPoints not initialized (InjectionPointShmemInit has not run)");
    p
}

/// `&*ActiveInjectionPoints` — shared view for atomic reads/writes.
fn ctl() -> &'static InjectionPointsCtl {
    // SAFETY: once published by InjectionPointShmemInit, the pointer addresses a
    // live `InjectionPointsCtl` in the shared segment for the process lifetime.
    unsafe { &*ctl_ptr() }
}

/// `strlcpy(dst, src, sizeof(dst))` into a fixed byte array, NUL-terminated.
fn store_cstr(dst: &mut [u8], src: &str) {
    let bytes = src.as_bytes();
    let n = bytes.len().min(dst.len() - 1);
    dst[..n].copy_from_slice(&bytes[..n]);
    dst[n] = 0;
    for b in &mut dst[n + 1..] {
        *b = 0;
    }
}

/// Read a fixed NUL-terminated byte array back to a `&str` view.
fn read_cstr(buf: &[u8]) -> &str {
    let end = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    core::str::from_utf8(&buf[..end]).unwrap_or("")
}

// ===========================================================================
// Shmem size / init (the injection_point_seams pair)
// ===========================================================================

/// `InjectionPointShmemSize()` — bytes for the shared registry.
fn InjectionPointShmemSize() -> PgResult<Size> {
    Ok(core::mem::size_of::<InjectionPointsCtl>())
}

/// `InjectionPointShmemInit()` — allocate-or-attach the shared registry. The
/// first caller (postmaster, `!found`) zero-initializes every generation word;
/// later callers just record the base.
fn InjectionPointShmemInit() -> PgResult<()> {
    let (ptr, found) = ipc_shmem_seams::shmem_init_struct::call(
        "InjectionPoint hash",
        core::mem::size_of::<InjectionPointsCtl>(),
    )?;
    let ptr = ptr as *mut InjectionPointsCtl;

    if !found {
        // SAFETY: `ptr` addresses a writable, properly-aligned
        // `InjectionPointsCtl` in shmem; we initialize each field once. All
        // fields are atomics or byte arrays, so a zeroed image is already a
        // valid value; we write the header explicitly to mirror C's
        // pg_atomic_init.
        unsafe {
            (*ptr).max_inuse.store(0, Ordering::Relaxed);
            for i in 0..MAX_INJECTION_POINTS {
                (*ptr).entries[i].generation.store(0, Ordering::Relaxed);
            }
            // Wait-state init (the test extension's injection_point_init_state).
            core::ptr::write(&mut (*ptr).wait_lock, Spinlock::new());
            for i in 0..INJ_MAX_WAIT {
                (*ptr).wait_counts[i].store(0, Ordering::Relaxed);
                (*ptr).wait_names[i] = [0; INJ_NAME_MAXLEN];
            }
            core::ptr::write(&mut (*ptr).wait_point, ConditionVariable::new());
            (*ptr).wait_inited.store(1, Ordering::Relaxed);
        }
    }
    ACTIVE_INJECTION_POINTS.store(ptr, Ordering::Relaxed);
    Ok(())
}

// ===========================================================================
// Attach / Detach
// ===========================================================================

/// `InjectionPointAttach(name, library, function, private_data, size)`.
pub fn InjectionPointAttach(
    name: &str,
    library: &str,
    function: &str,
    private_data: &[u8],
) -> PgResult<()> {
    if name.len() >= INJ_NAME_MAXLEN {
        return elog(ERROR, format!("injection point name {name} too long (maximum of {} characters)", INJ_NAME_MAXLEN - 1));
    }
    if library.len() >= INJ_LIB_MAXLEN {
        return elog(ERROR, format!("injection point library {library} too long (maximum of {} characters)", INJ_LIB_MAXLEN - 1));
    }
    if function.len() >= INJ_FUNC_MAXLEN {
        return elog(ERROR, format!("injection point function {function} too long (maximum of {} characters)", INJ_FUNC_MAXLEN - 1));
    }
    if private_data.len() > INJ_PRIVATE_MAXLEN {
        return elog(ERROR, format!("injection point data too long (maximum of {INJ_PRIVATE_MAXLEN} bytes)"));
    }

    let ctl = ctl();
    let _guard = LWLockAcquireMain(INJECTION_POINT_LOCK, LW_EXCLUSIVE, init_small_seams::my_proc_number::call())?;

    let max_inuse = ctl.max_inuse.load(Ordering::Relaxed);
    let mut free_idx: i32 = -1;

    for idx in 0..max_inuse as usize {
        let entry = &ctl.entries[idx];
        let generation = entry.generation.load(Ordering::Relaxed);
        if generation % 2 == 0 {
            if free_idx == -1 {
                free_idx = idx as i32;
            }
        } else if read_cstr(&entry.name) == name {
            return elog(ERROR, format!("injection point \"{name}\" already defined"));
        }
    }

    if free_idx == -1 {
        if max_inuse as usize == MAX_INJECTION_POINTS {
            return elog(ERROR, "too many injection points");
        }
        free_idx = max_inuse as i32;
    }

    let idx = free_idx as usize;
    let entry = &ctl.entries[idx];
    let generation = entry.generation.load(Ordering::Relaxed);
    debug_assert!(generation % 2 == 0);

    // Save the entry. SAFETY: we hold InjectionPointLock; the entry's
    // generation is even (slot free), so no reader trusts these fields until we
    // bump the generation below. The byte arrays are written through a raw
    // *mut whose provenance is the original shmem `*mut` (ctl_ptr), not a `&T`.
    unsafe {
        let entry_mut = &raw mut (*ctl_ptr()).entries[idx];
        store_cstr(&mut (*entry_mut).name, name);
        store_cstr(&mut (*entry_mut).library, library);
        store_cstr(&mut (*entry_mut).function, function);
        let pd = &mut *(&raw mut (*entry_mut).private_data);
        let n = private_data.len();
        pd[..n].copy_from_slice(private_data);
    }

    // pg_write_barrier(); pg_atomic_write_u64(&entry->generation, generation+1)
    fence(Ordering::Release);
    entry.generation.store(generation + 1, Ordering::Relaxed);

    if free_idx + 1 > max_inuse as i32 {
        ctl.max_inuse.store(free_idx as u32 + 1, Ordering::Relaxed);
    }

    Ok(())
}

/// `InjectionPointDetach(name)` — returns true if it was detached.
pub fn InjectionPointDetach(name: &str) -> PgResult<bool> {
    let ctl = ctl();
    let _guard = LWLockAcquireMain(INJECTION_POINT_LOCK, LW_EXCLUSIVE, init_small_seams::my_proc_number::call())?;

    let max_inuse = ctl.max_inuse.load(Ordering::Relaxed) as i32;
    let mut found = false;
    let mut idx = max_inuse - 1;
    while idx >= 0 {
        let entry = &ctl.entries[idx as usize];
        let generation = entry.generation.load(Ordering::Relaxed);
        if generation % 2 == 0 {
            idx -= 1;
            continue;
        }
        if read_cstr(&entry.name) == name {
            found = true;
            entry.generation.store(generation + 1, Ordering::Relaxed);
            break;
        }
        idx -= 1;
    }

    // If we removed the highest-numbered entry, update max_inuse.
    if found && idx == max_inuse - 1 {
        while idx >= 0 {
            let entry = &ctl.entries[idx as usize];
            if entry.generation.load(Ordering::Relaxed) % 2 != 0 {
                break;
            }
            idx -= 1;
        }
        ctl.max_inuse.store((idx + 1) as u32, Ordering::Relaxed);
    }

    Ok(found)
}

// ===========================================================================
// Backend-local cache + refresh
// ===========================================================================

#[derive(Clone)]
struct CacheEntry {
    private_data: Vec<u8>,
    callback: InjectionPointCallback,
    slot_idx: usize,
    generation: u64,
}

thread_local! {
    /// `static HTAB *InjectionPointCache` — the backend-local cache of resolved
    /// callbacks, keyed by point name.
    static CACHE: RefCell<Option<HashMap<String, CacheEntry>>> = const { RefCell::new(None) };
}

/// `InjectionPointCacheRefresh(name)` — check shmem for `name`, update the
/// local cache, and return the resolved cache entry (if any).
fn cache_refresh(name: &str) -> PgResult<Option<CacheEntry>> {
    let ctl = ctl();
    let max_inuse = ctl.max_inuse.load(Ordering::Relaxed);

    if max_inuse == 0 {
        // Destroy the local cache (C hash_destroy).
        CACHE.with(|c| *c.borrow_mut() = None);
        return Ok(None);
    }

    // If we have this entry cached, validate it against the shmem generation.
    let cached = CACHE.with(|c| c.borrow().as_ref().and_then(|m| m.get(name).cloned()));
    if let Some(cached) = cached {
        let entry = &ctl.entries[cached.slot_idx];
        if entry.generation.load(Ordering::Relaxed) == cached.generation {
            return Ok(Some(cached));
        }
        // Stale — drop it and fall through to a fresh scan.
        CACHE.with(|c| {
            if let Some(m) = c.borrow_mut().as_mut() {
                m.remove(name);
            }
        });
    }

    // Scan the shared array.
    for idx in 0..max_inuse as usize {
        let entry = &ctl.entries[idx];
        let generation = entry.generation.load(Ordering::Relaxed);
        if generation % 2 == 0 {
            continue; // empty slot
        }
        fence(Ordering::Acquire); // pg_read_barrier

        if read_cstr(&entry.name) != name {
            continue;
        }

        // Copy to local memory, then re-check generation for coherence.
        let library = read_cstr(&entry.library).to_string();
        let function = read_cstr(&entry.function).to_string();
        let private_data = entry.private_data.to_vec();

        fence(Ordering::Acquire); // pg_read_barrier
        if entry.generation.load(Ordering::Relaxed) != generation {
            // Concurrently detached — name match can't be trusted; keep going.
            continue;
        }

        // Resolve the callback (load_external_function analogue).
        let callback = match lookup_callback(&library, &function) {
            Some(cb) => cb,
            None => {
                elog(ERROR, format!("could not find function \"{function}\" in library \"{library}\" for injection point \"{name}\""))?;
                unreachable!("elog(ERROR) does not return");
            }
        };

        let cache_entry = CacheEntry {
            private_data,
            callback,
            slot_idx: idx,
            generation,
        };
        CACHE.with(|c| {
            c.borrow_mut()
                .get_or_insert_with(HashMap::new)
                .insert(name.to_string(), cache_entry.clone());
        });
        return Ok(Some(cache_entry));
    }

    Ok(None)
}

/// `injection_point_cache_get(name)` — local cache lookup only.
fn cache_get(name: &str) -> Option<CacheEntry> {
    CACHE.with(|c| c.borrow().as_ref().and_then(|m| m.get(name).cloned()))
}

// ===========================================================================
// Load / Run / Cached / IsAttached
// ===========================================================================

/// `InjectionPointLoad(name)` — pre-load into the backend-local cache.
pub fn InjectionPointLoad(name: &str) -> PgResult<()> {
    cache_refresh(name)?;
    Ok(())
}

/// `InjectionPointRun(name, arg)` — execute the point if attached.
pub fn InjectionPointRun(name: &str, arg: Option<&str>) -> PgResult<()> {
    if let Some(entry) = cache_refresh(name)? {
        (entry.callback)(name, &entry.private_data, arg)?;
    }
    Ok(())
}

/// `InjectionPointCached(name, arg)` — execute straight from the local cache.
pub fn InjectionPointCached(name: &str, arg: Option<&str>) -> PgResult<()> {
    if let Some(entry) = cache_get(name) {
        (entry.callback)(name, &entry.private_data, arg)?;
    }
    Ok(())
}

/// `IsInjectionPointAttached(name)`.
pub fn IsInjectionPointAttached(name: &str) -> PgResult<bool> {
    Ok(cache_refresh(name)?.is_some())
}

// ===========================================================================
// Wait / wakeup support (the test extension's injection_wait machinery,
// co-located in the core shmem region)
// ===========================================================================

/// `SpinLockAcquire(&inj_state->lock); f(); SpinLockRelease(...)`.
fn with_wait_lock<R>(ctl: &InjectionPointsCtl, f: impl FnOnce() -> R) -> R {
    if ctl.wait_lock.tas_spin() != 0 {
        s_lock::s_lock(&ctl.wait_lock, Some(file!()), line!() as i32, None);
    }
    let r = f();
    ctl.wait_lock.unlock();
    r
}

/// `injection_wait(name, ...)` — register a wait slot for `name` and sleep on
/// the shared condition variable until `injection_points_wakeup(name)` bumps
/// our counter. Returns an `ERROR` if no free wait slot is available.
pub fn injection_wait(name: &str) -> PgResult<()> {
    let ctl = ctl();

    // Custom wait event named for the injection point.
    let wait_event = waitevent::WaitEventInjectionPointNew(name)?;

    // Find a free slot and record this point's name.
    let mut index: i32 = -1;
    let mut old_wait_counts: u32 = 0;
    with_wait_lock(ctl, || {
        for i in 0..INJ_MAX_WAIT {
            if ctl.wait_names[i][0] == 0 {
                index = i as i32;
                // SAFETY: under wait_lock; exclusive access to wait_names[i].
                // `*mut` provenance derives from the shmem `ctl_ptr`.
                unsafe {
                    let names = &raw mut (*ctl_ptr()).wait_names[i];
                    store_cstr(&mut *names, name);
                }
                old_wait_counts = ctl.wait_counts[i].load(Ordering::Relaxed);
                break;
            }
        }
    });

    if index < 0 {
        return elog(ERROR, format!("could not find free slot for wait of injection point {name} "));
    }
    let index = index as usize;

    // Sleep until the counter advances.
    condition_variable::ConditionVariablePrepareToSleep(&ctl.wait_point);
    loop {
        let new_wait_counts = ctl.wait_counts[index].load(Ordering::Relaxed);
        if old_wait_counts != new_wait_counts {
            break;
        }
        condition_variable::ConditionVariableSleep(&ctl.wait_point, wait_event)?;
    }
    condition_variable::ConditionVariableCancelSleep();

    // Remove from the waiters.
    with_wait_lock(ctl, || {
        // SAFETY: under wait_lock; `*mut` provenance is the shmem ctl_ptr.
        unsafe { (*ctl_ptr()).wait_names[index][0] = 0 };
    });

    Ok(())
}

/// `injection_points_wakeup(name)` — bump the wait counter for `name` and
/// broadcast the shared condition variable. `ERROR` if `name` is not waiting.
pub fn injection_wakeup(name: &str) -> PgResult<()> {
    let ctl = ctl();
    let mut index: i32 = -1;
    let found = with_wait_lock(ctl, || {
        for i in 0..INJ_MAX_WAIT {
            if read_cstr(&ctl.wait_names[i]) == name {
                index = i as i32;
                break;
            }
        }
        if index < 0 {
            return false;
        }
        ctl.wait_counts[index as usize].fetch_add(1, Ordering::Relaxed);
        true
    });

    if !found {
        return elog(ERROR, format!("could not find injection point {name} to wake up"));
    }

    condition_variable::ConditionVariableBroadcast(&ctl.wait_point);
    Ok(())
}

// ===========================================================================
// Seam installation
// ===========================================================================

/// Install this unit's seams: the shmem size/init pair (consumed by ipci.c) and
/// the `INJECTION_POINT(...)` call-site entrypoints (consumed by ported backend
/// code).
pub fn init_seams() {
    injection_point_seams::injection_point_shmem_size::set(InjectionPointShmemSize);
    injection_point_seams::injection_point_shmem_init::set(InjectionPointShmemInit);
    injection_point_seams::injection_point_run::set(InjectionPointRun);
    injection_point_seams::injection_point_cached::set(InjectionPointCached);
    injection_point_seams::injection_point_load::set(InjectionPointLoad);
    injection_point_seams::is_injection_point_attached::set(IsInjectionPointAttached);
}
