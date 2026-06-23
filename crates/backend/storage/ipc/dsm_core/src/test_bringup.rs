//! Reusable in-process DSM control-segment bring-up for unit tests.
//!
//! Real `dsm_create` / `dsm_attach` need two things to exist before they run:
//!
//!   1. A mapped DSM **control segment** (`dsm_control` / `dsm_control_header`),
//!      established by [`crate::dsm::dsm_postmaster_startup`] over a real
//!      `PGShmemHeader` (`ipci.c` / `dsm_postmaster_startup` in C).
//!   2. The `DynamicSharedMemoryControlLock` published in the main LWLock array
//!      (`CreateLWLocks`), since every control-segment mutation takes it.
//!
//! Nothing in the unit-test surface set that up before this module: the merged
//! `dsm-core` / `dsm-registry` crates ship layout/validation tests only and the
//! real `dsm_create` runtime path was never exercised. [`dsm_test_bringup`]
//! mirrors the postmaster's startup sequence in-process so a single backend can
//! create, attach to, address, and detach a *real* DSM segment under
//! `cargo test`.
//!
//! ## What is real vs. mirrored
//!
//! Owners that are **merged** are wired with their genuine `init_seams()`
//! bodies (no fakes): the LWLock manager (`lwlock.c`), the FreePageManager
//! (`freepage.c`), and `dsm-core` itself (`dsm.c` / `dsm_impl.c` / `ipc.c`).
//!
//! Owners that are **not yet ported** have their seams installed here with
//! their faithful C bodies (per `mirror-pg-and-panic` we name them, but a
//! `panic!()` stub would defeat the runtime test, so the bring-up supplies the
//! real C semantics — these are not side-table stand-ins, they are exactly what
//! the C does):
//!
//!   * `MyProcNumber` (`globals.c`) — the single-user backend is proc `0`.
//!   * `HOLD_INTERRUPTS` / `RESUME_INTERRUPTS` (`miscadmin.h`) —
//!     `InterruptHoldoffCount++` / `--`, a per-backend counter.
//!   * `pgstat_report_wait_start` / `_end` (`wait_event.c`) —
//!     `*my_wait_event_info = info` / `= 0`, a per-backend `uint32`.
//!   * `ReserveExternalFD` / `ReleaseExternalFD` (`fd.c`) — the `numExternalFDs`
//!     reservation counter (the POSIX `dsm_impl` brackets each `shm_open` with
//!     it).
//!
//! `dynamic_shared_memory_type` defaults to `DSM_IMPL_POSIX`, so the control
//! segment is a real `shm_open`'d, `mmap`'d POSIX segment — genuine shared
//! memory, not a `Vec<u8>`. `min_dynamic_shared_memory` is left at its `0`
//! default, so created segments are their own POSIX segments rather than carved
//! from a preallocated main region; the control segment bookkeeping
//! (slot search/refcount under the control lock) is identical either way and is
//! what this bring-up exists to make runnable.

use std::cell::Cell;
use std::sync::Once;

use mcx::{Mcx, MemoryContext};
use types_core::{uint32, ProcNumber};
use types_storage::{PGShmemHeader, PGShmemMagic};

use crate::dsm::dsm_postmaster_startup;

/// The single-user backend's `MaxBackends`. Sized for a control segment large
/// enough for the handful of segments a unit test creates; the formula matches
/// `dsm_postmaster_startup` (`PG_DYNSHMEM_FIXED_SLOTS + 5 * MaxBackends`).
const TEST_MAX_BACKENDS: i32 = 8;

thread_local! {
    /// `InterruptHoldoffCount` (`globals.c`) — the per-backend nesting depth of
    /// `HOLD_INTERRUPTS()`.
    static INTERRUPT_HOLDOFF_COUNT: Cell<u32> = const { Cell::new(0) };
    /// `MyProc->wait_event_info` (`PGPROC`) — the wait event currently posted
    /// by this backend.
    static MY_WAIT_EVENT_INFO: Cell<uint32> = const { Cell::new(0) };
    /// `numExternalFDs` (`fd.c`) — count of FDs reserved outside the VFD pool.
    static NUM_EXTERNAL_FDS: Cell<i32> = const { Cell::new(0) };
    /// Whether this thread has already mapped the control segment into its
    /// thread-local `dsm.c` globals. The control segment's POSIX object is
    /// process-global, but `dsm_control` is a per-backend pointer (inherited at
    /// fork in C); each test thread must run the startup once to populate it.
    static THREAD_BROUGHT_UP: Cell<bool> = const { Cell::new(false) };
}

thread_local! {
    /// A leaked per-thread `TopMemoryContext`-equivalent. `dsm.c` descriptors
    /// live for the backend's whole life and `MemoryContext` is not `Sync`
    /// (its bump allocator is `!Send`), so the context is thread-local — which
    /// matches the per-backend lifetime exactly.
    static TOP_MCX: &'static MemoryContext =
        Box::leak(Box::new(MemoryContext::new("dsm-test-top")));
}

/// The calling thread's `TopMemoryContext` stand-in.
fn top_mcx() -> Mcx<'static> {
    TOP_MCX.with(|ctx| ctx.mcx())
}

/// Install the merged owners' real seams and the faithful bodies for the
/// not-yet-ported owners. Runs exactly once per process (the seam slots and the
/// `MainLWLockArray` `OnceLock` are process-global).
fn install_substrate_once() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        // --- merged owners: their genuine init_seams() ---
        lwlock::init_seams();
        mmgr_freepage::init_seams();
        crate::init_seams();

        // --- not-yet-ported owners: faithful C bodies (named above) ---
        // globals.c: MyProcNumber for the single-user backend.
        init_small_seams::my_proc_number::set(|| 0 as ProcNumber);
        // miscadmin.h: HOLD_INTERRUPTS()/RESUME_INTERRUPTS().
        init_small_seams::hold_interrupts::set(|| {
            INTERRUPT_HOLDOFF_COUNT.with(|c| c.set(c.get() + 1));
        });
        init_small_seams::resume_interrupts::set(|| {
            INTERRUPT_HOLDOFF_COUNT.with(|c| {
                let n = c.get();
                debug_assert!(n > 0, "InterruptHoldoffCount underflow");
                c.set(n - 1);
            });
        });
        // wait_event.c: pgstat_report_wait_start/end => *my_wait_event_info.
        waitevent_seams::pgstat_report_wait_start::set(|info| {
            MY_WAIT_EVENT_INFO.with(|c| c.set(info));
        });
        waitevent_seams::pgstat_report_wait_end::set(|| {
            MY_WAIT_EVENT_INFO.with(|c| c.set(0));
        });
        // shmem.c: add_size/mul_size are the overflow-checked size arithmetic
        // (LWLockShmemSize uses them). The C bodies ereport on overflow.
        ipc_shmem_seams::add_size::set(|a, b| {
            a.checked_add(b).ok_or_else(|| {
                utils_error::PgError::error(
                    "requested shared memory size overflows size_t",
                )
            })
        });
        ipc_shmem_seams::mul_size::set(|a, b| {
            a.checked_mul(b).ok_or_else(|| {
                utils_error::PgError::error(
                    "requested shared memory size overflows size_t",
                )
            })
        });

        // fd.c: ReserveExternalFD/ReleaseExternalFD => numExternalFDs.
        file_seams::reserve_external_fd::set(|| {
            NUM_EXTERNAL_FDS.with(|c| c.set(c.get() + 1));
        });
        file_seams::release_external_fd::set(|| {
            NUM_EXTERNAL_FDS.with(|c| {
                let n = c.get();
                debug_assert!(n > 0, "numExternalFDs underflow");
                c.set(n - 1);
            });
        });

        // globals.c: the per-backend interrupt/cancel flags. dsm_postmaster_
        // startup registers dsm_postmaster_shutdown with on_shmem_exit, which
        // installs ipc.c's libc::atexit handler; at process exit that handler
        // runs proc_exit_prepare, which clears these flags. Installing their
        // real (thread-local) bodies keeps the atexit path from hitting an
        // uninstalled seam. (On a thread that never brought DSM up — e.g. the
        // main thread libtest exits on — the on_shmem_exit list is empty, so
        // the shutdown callback itself does not run there.)
        init_small_seams::set_interrupt_pending::set(|_v| {});
        init_small_seams::set_proc_die_pending::set(|_v| {});
        init_small_seams::set_query_cancel_pending::set(|_v| {});
        init_small_seams::set_interrupt_holdoff_count::set(|v| {
            INTERRUPT_HOLDOFF_COUNT.with(|c| c.set(v));
        });
        // tcop: reset debug_query_string (no statement string under test).
        postgres_seams::reset_debug_query_string::set(|| {});

        // globals.c: IsPostmasterEnvironment / IsUnderPostmaster. The test
        // backend is a standalone (`--single`-equivalent) DSM owner: it is not
        // the postmaster and not under a postmaster, so `shmem_exit` runs
        // `dsm_backend_shutdown()` normally. Installing these keeps the atexit
        // cleanup path (proc_exit -> shmem_exit) from hitting an uninstalled
        // seam at process teardown.
        init_small_seams::is_postmaster_environment::set(|| false);
        init_small_seams::is_under_postmaster::set(|| false);

        // shmem.c: ShmemAlloc. `CreateLWLocks` now allocates the LWLock array
        // through `shmem_alloc::call` (the postmaster carves the lock array out
        // of the main shared segment). Under test there is no main segment, so
        // hand back a fresh zeroed, `LWLOCK_PADDED_SIZE`(=128)-aligned leaked
        // buffer of the requested size (leaked for the process lifetime like
        // genuine shmem). Mirrors the lwlock crate's own unit-test mock.
        ipc_shmem_seams::shmem_alloc::set(|size| {
            use std::alloc::{alloc_zeroed, Layout};
            let layout = Layout::from_size_align(size.max(1), 128).unwrap();
            // SAFETY: nonzero size.
            let ptr = unsafe { alloc_zeroed(layout) };
            if ptr.is_null() {
                return Err(utils_error::PgError::error(
                    "ShmemAlloc OOM during DSM test bring-up",
                ));
            }
            Ok(ptr)
        });

        // Publish MainLWLockArray (postmaster path): allocates + initializes the
        // fixed locks, including DynamicSharedMemoryControlLock. Process-global
        // OnceLock, so this is the only allocation site.
        lwlock::CreateLWLocks(top_mcx(), false)
            .expect("CreateLWLocks failed during DSM test bring-up");
    });
}

/// Map a DSM control segment for the *calling thread* so real `dsm_create` /
/// `dsm_attach` run in-process.
///
/// Mirrors the postmaster's `dsm_postmaster_startup` over a real
/// `PGShmemHeader` (leaked; lives for the process). Idempotent per thread:
/// `dsm.c`'s control-segment globals are thread-local (they stand in for the
/// per-backend pointers C inherits at fork), so each thread that wants to touch
/// DSM runs the startup once.
///
/// Returns the `Mcx<'static>` (the `TopMemoryContext` stand-in) that callers
/// pass into `dsm_create` / `dsm_attach` / `on_dsm_detach`.
pub fn dsm_test_bringup() -> Mcx<'static> {
    install_substrate_once();

    if !THREAD_BROUGHT_UP.with(Cell::get) {
        // globals.c / InitProcessGlobals: seed this backend's global PRNG (it
        // is per-backend / thread-local, inherited at fork in C). dsm.c draws
        // segment handles from it; an unseeded all-zero xoroshiro state yields
        // 0 forever, which would collide every handle and spin dsm_create's
        // "find an unused identifier" loop. Seed from pid+time, as C seeds
        // pg_global_prng_state at process start.
        let seed = (unsafe { libc::getpid() } as u64)
            ^ std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0x9E37_79B9_7F4A_7C15);
        prng::global_prng(|prng| prng.seed(seed));

        // A real PGShmemHeader, as the postmaster places at the start of the
        // main shared segment. dsm_postmaster_startup writes shim->dsm_control.
        let shim: &'static mut PGShmemHeader = Box::leak(Box::new(PGShmemHeader {
            magic: PGShmemMagic,
            creatorPID: unsafe { libc::getpid() },
            totalsize: 0,
            freeoffset: 0,
            dsm_control: 0,
            index: std::ptr::null_mut(),
            device: 0,
            inode: 0,
        }));
        dsm_postmaster_startup(shim, TEST_MAX_BACKENDS)
            .expect("dsm_postmaster_startup failed during DSM test bring-up");
        THREAD_BROUGHT_UP.with(|c| c.set(true));
    }

    top_mcx()
}
