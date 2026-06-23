//! Shared-memory setup: the `shmem_request` / `shmem_startup` hooks, the
//! `pgssSharedState` + `pgssEntry` HTAB allocation, `pgss_memsize`, and the
//! global pointers + the per-entry / shared-state spinlock helper.

use core::cell::Cell;
use core::sync::atomic::{AtomicU32, Ordering};

use types_error::PgResult;
use hash::hsearch::{HASHCTL, HASH_BLOBS, HASH_ELEM, HTAB};

use crate::{
    PgssEntry, PgssGlobalStats, PgssHashKey, PgssSharedState, ASSUMED_LENGTH_INIT,
    ASSUMED_MEDIAN_INIT, PGSS_SHMEM_NAME,
};

thread_local! {
    /// `pgssSharedState *pgss` — this process's pointer into the shared state.
    static PGSS: Cell<*mut PgssSharedState> = const { Cell::new(core::ptr::null_mut()) };
    /// `HTAB *pgss_hash` — this process's handle to the shared hashtable.
    static PGSS_HASH: Cell<*mut HTAB> = const { Cell::new(core::ptr::null_mut()) };
}

/// `pgss` global (null when not initialized / module not preloaded).
pub(crate) fn pgss() -> *mut PgssSharedState {
    PGSS.with(Cell::get)
}
/// `pgss_hash` global.
pub(crate) fn pgss_hash() -> *mut HTAB {
    PGSS_HASH.with(Cell::get)
}
/// Both globals are live (the C `if (!pgss || !pgss_hash)` guard).
pub(crate) fn is_initialized() -> bool {
    !pgss().is_null() && !pgss_hash().is_null()
}

// ---------------------------------------------------------------------------
// Spinlock over an AtomicU32 word (the C `slock_t mutex` per entry / shared
// state). A simple test-and-set; uncontended in practice (held only across a
// handful of counter updates).
// ---------------------------------------------------------------------------

/// `SpinLockAcquire(&word)` — busy-acquire the spinlock word.
pub(crate) fn spin_lock_acquire(word: &AtomicU32) {
    while word
        .compare_exchange_weak(0, 1, Ordering::Acquire, Ordering::Relaxed)
        .is_err()
    {
        core::hint::spin_loop();
    }
}

/// `SpinLockRelease(&word)`.
pub(crate) fn spin_lock_release(word: &AtomicU32) {
    word.store(0, Ordering::Release);
}

/// `SpinLockInit(&word)`.
pub(crate) fn spin_lock_init(word: &AtomicU32) {
    word.store(0, Ordering::Relaxed);
}

// ---------------------------------------------------------------------------
// Sizing.
// ---------------------------------------------------------------------------

#[inline]
const fn maxalign(len: usize) -> usize {
    (len + 7) & !7
}

/// `pgss_memsize()` (pg_stat_statements.c:2049).
pub(crate) fn pgss_memsize() -> PgResult<usize> {
    let mut size = maxalign(core::mem::size_of::<PgssSharedState>());
    let hash =
        dynahash::hash_estimate_size(crate::pgss_max() as i64, core::mem::size_of::<PgssEntry>());
    size = ipc_shmem::add_size(size, hash)?;
    Ok(size)
}

// ---------------------------------------------------------------------------
// Hooks.
// ---------------------------------------------------------------------------

/// Install the `shmem_request` / `shmem_startup` hooks (called from `_PG_init`).
pub(crate) fn install_hooks() {
    miscinit::set_shmem_request_hook(pgss_shmem_request);
    ipc::ipci_core::set_shmem_startup_hook(Some(pgss_shmem_startup));
}

/// `pgss_shmem_request()` (pg_stat_statements.c:494).
fn pgss_shmem_request() -> PgResult<()> {
    // C chains prev_shmem_request_hook; pgrust's single settable hook means we
    // are the only requester in this chain (the registry has one hook slot).
    let in_progress = miscinit::process_shmem_requests_in_progress();
    ipc::ipci_core::request_addin_shmem_space(pgss_memsize()?, in_progress)?;

    let scratch = mcx::MemoryContext::new("pgss_shmem_request");
    lwlock::RequestNamedLWLockTranche(
        scratch.mcx(),
        PGSS_SHMEM_NAME,
        1,
        in_progress,
    )?;
    Ok(())
}

/// `pgss_shmem_startup()` (pg_stat_statements.c:510). Allocate or attach to the
/// shared state + hashtable, then load any pre-existing statistics.
fn pgss_shmem_startup() -> PgResult<()> {
    // reset in case this is a restart within the postmaster
    PGSS.with(|c| c.set(core::ptr::null_mut()));
    PGSS_HASH.with(|c| c.set(core::ptr::null_mut()));

    // Create or attach to the shared memory state, including hash table.
    // ShmemInitStruct/ShmemInitHash take their own AddinShmemInitLock-equivalent
    // (the ShmemIndex is internally locked).
    let (loc, found) = ipc_shmem::ShmemInitStruct(
        PGSS_SHMEM_NAME,
        core::mem::size_of::<PgssSharedState>(),
    )?;
    let pgss = loc.as_ptr().cast::<PgssSharedState>();

    if !found {
        // First time through.
        // SAFETY: `pgss` is the live ShmemInitStruct allocation for our state.
        unsafe {
            let lock = lwlock::named_tranche_first_lock(PGSS_SHMEM_NAME)?
                as *const types_storage::storage::LWLock;
            core::ptr::write(
                pgss,
                PgssSharedState {
                    lock,
                    cur_median_usage: ASSUMED_MEDIAN_INIT,
                    mean_query_len: ASSUMED_LENGTH_INIT,
                    mutex: AtomicU32::new(0),
                    extent: 0,
                    n_writers: 0,
                    gc_count: 0,
                    stats: PgssGlobalStats {
                        dealloc: 0,
                        stats_reset: adt_datetime::timestamp::GetCurrentTimestamp(),
                    },
                },
            );
        }
    }

    let mut info = HASHCTL {
        keysize: core::mem::size_of::<PgssHashKey>(),
        entrysize: core::mem::size_of::<PgssEntry>(),
        ..hashctl_default()
    };
    let pgss_hash = ipc_shmem::ShmemInitHash(
        "pg_stat_statements hash",
        crate::pgss_max() as i64,
        crate::pgss_max() as i64,
        &mut info,
        HASH_ELEM | HASH_BLOBS,
    )?;

    PGSS.with(|c| c.set(pgss));
    PGSS_HASH.with(|c| c.set(pgss_hash));

    // If we're in the postmaster (or a standalone backend), set up a shmem exit
    // hook to dump the statistics to disk. (Deferred: the on_shmem_exit dump is
    // wired in qtext::register_shutdown_dump.)
    if !init_small_seams::is_under_postmaster::call() {
        crate::qtext::register_shutdown_dump();
    }

    // Done if some other process already completed our initialization.
    if found {
        return Ok(());
    }

    // First-time init: prepare / load the external query text file.
    crate::qtext::startup_load()?;
    Ok(())
}

/// A zero-initialized `HASHCTL` (the C `HASHCTL info;` left mostly zero before
/// the keysize/entrysize/flag fields are set).
fn hashctl_default() -> HASHCTL {
    // SAFETY: HASHCTL is plain-old-data (sizes, counts, raw pointers, an
    // Option<fn>). All-zero is a valid bit pattern (None for the Option), and is
    // exactly the uninitialized-stack state the C relies on for the fields it
    // does not set.
    unsafe { core::mem::zeroed() }
}

// ---------------------------------------------------------------------------
// Typed views over the shared state / hash entries.
// ---------------------------------------------------------------------------

/// `&mut *pgss` (caller guarantees `is_initialized`).
///
/// # Safety
/// `pgss` must be the live shared-state pointer (true after shmem_startup).
pub(crate) unsafe fn pgss_ref<'a>() -> &'a mut PgssSharedState {
    &mut *pgss()
}

/// Helper to read/modify a `PgssEntry` from a raw key-pointer (HTAB entries are
/// keyed on the leading `PgssHashKey`; the whole `PgssEntry` starts at the key).
///
/// # Safety
/// `ptr` must be a live `PgssEntry` in the shared hashtable.
pub(crate) unsafe fn entry_ref<'a>(ptr: *mut u8) -> &'a mut PgssEntry {
    &mut *ptr.cast::<PgssEntry>()
}
