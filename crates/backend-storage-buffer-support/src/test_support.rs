//! Test-only seam wiring.
//!
//! This crate CONSUMES seams owned by the (unported) bufmgr, smgr, shmem,
//! latch, init-small, and relpath units. The unit tests install real,
//! exercising implementations through each owner's `-seams` crate `::set`,
//! backed by process-global state guarded by a single crate-wide mutex so the
//! parallel test runner serializes every test that touches a seam.
//!
//! `TestHeaders` stands in for the bufmgr-owned shmem buffer-descriptor array
//! (the `lock_buf_hdr` / `unlock_buf_hdr` / `buf_free_next` seam targets) plus
//! the strategy GUCs and the bgwriter latch; `TestSmgr` stands in for the
//! temp-rel `smgr` I/O entry points. The `buffer_strategy_lock` is a real
//! `Spinlock` and needs no test stand-in.

use std::sync::{Mutex, MutexGuard, OnceLock};

use types_storage::buf::{BM_LOCKED, BUF_REFCOUNT_MASK, BUF_USAGECOUNT_MASK, BUF_USAGECOUNT_ONE};
use types_core::{BlockNumber, ForkNumber, Oid, ProcNumber, RelFileNumber};
use types_storage::RelFileLocator;

/// The crate-wide test guard. Locking it serializes any test that installs the
/// process-global seams / mutates the process-global backing state.
fn test_guard() -> &'static Mutex<()> {
    static GUARD: Mutex<()> = Mutex::new(());
    &GUARD
}

/// Process-global backing state for the buffer-header + strategy seams.
struct HeaderState {
    /// Per-buffer packed `state` words (the bufmgr `BufferDesc.state`).
    states: Vec<u32>,
    /// Per-buffer `freeNext` links.
    free_next: Vec<i32>,
    /// Net count of held header locks (lock_hdr +1, unlock_hdr -1).
    locked: i32,
    /// Recorded bgwriter wakeups.
    latches: Vec<i32>,
    /// `GetPinLimit()`.
    pin_limit: i32,
    /// `io_combine_limit` GUC.
    io_combine_limit: i32,
    /// `effective_io_concurrency` GUC.
    effective_io_concurrency: i32,
}

impl HeaderState {
    fn fresh(n: usize) -> Self {
        let free_next = (0..n)
            .map(|i| if i + 1 < n { (i + 1) as i32 } else { -1 })
            .collect();
        Self {
            states: vec![0u32; n],
            free_next,
            locked: 0,
            latches: Vec::new(),
            pin_limit: i32::MAX,
            io_combine_limit: 16,
            effective_io_concurrency: 16,
        }
    }
}

fn header_state() -> &'static Mutex<HeaderState> {
    static STATE: OnceLock<Mutex<HeaderState>> = OnceLock::new();
    STATE.get_or_init(|| Mutex::new(HeaderState::fresh(0)))
}

/// Process-global backing state for the local-buffer `smgr` I/O seams.
struct SmgrState {
    /// Current number of blocks in the (single) mock temp fork.
    nblocks: BlockNumber,
    /// Recorded write block numbers (and the first byte written).
    writes: Vec<(BlockNumber, u8)>,
    /// Recorded `(blocknum, nblocks)` zero-extend requests.
    zeroextends: Vec<(BlockNumber, u32)>,
}

impl SmgrState {
    fn fresh(nblocks: BlockNumber) -> Self {
        Self {
            nblocks,
            writes: Vec::new(),
            zeroextends: Vec::new(),
        }
    }
}

fn smgr_state() -> &'static Mutex<SmgrState> {
    static STATE: OnceLock<Mutex<SmgrState>> = OnceLock::new();
    STATE.get_or_init(|| Mutex::new(SmgrState::fresh(0)))
}

/// Test handle onto the buffer-header backing state (the bufmgr descriptor
/// array stand-in) and the strategy GUCs / bgwriter latch.
pub(crate) struct TestHeaders;

impl TestHeaders {
    pub(crate) fn reset(n: usize) {
        *header_state().lock().unwrap() = HeaderState::fresh(n);
    }

    pub(crate) fn set(id: usize, refcount: u32, usagecount: u32) {
        let mut s = header_state().lock().unwrap();
        s.states[id] = (refcount & BUF_REFCOUNT_MASK) | (usagecount * BUF_USAGECOUNT_ONE);
    }

    pub(crate) fn usagecount(id: usize) -> u32 {
        let s = header_state().lock().unwrap();
        (s.states[id] & BUF_USAGECOUNT_MASK) / BUF_USAGECOUNT_ONE
    }

    pub(crate) fn locked_count() -> i32 {
        header_state().lock().unwrap().locked
    }

    pub(crate) fn clear_latches() {
        header_state().lock().unwrap().latches.clear();
    }

    pub(crate) fn latches() -> Vec<i32> {
        header_state().lock().unwrap().latches.clone()
    }

    pub(crate) fn set_pin_limit(v: i32) {
        header_state().lock().unwrap().pin_limit = v;
    }

    pub(crate) fn set_io_combine_limit(v: i32) {
        header_state().lock().unwrap().io_combine_limit = v;
    }

    pub(crate) fn set_effective_io_concurrency(v: i32) {
        header_state().lock().unwrap().effective_io_concurrency = v;
    }
}

/// Test handle onto the local-buffer `smgr` I/O backing state.
pub(crate) struct TestSmgr;

impl TestSmgr {
    pub(crate) fn reset(nblocks: BlockNumber) {
        *smgr_state().lock().unwrap() = SmgrState::fresh(nblocks);
    }

    pub(crate) fn write_count() -> usize {
        smgr_state().lock().unwrap().writes.len()
    }

    pub(crate) fn first_write_block() -> BlockNumber {
        smgr_state().lock().unwrap().writes[0].0
    }

    pub(crate) fn zeroextends() -> Vec<(BlockNumber, u32)> {
        smgr_state().lock().unwrap().zeroextends.clone()
    }
}

/// Install the test seam implementations and lock the crate-wide guard. The
/// returned guard must outlive the test body. Seams are install-once (a seam
/// `set` twice panics), so the actual installation runs under a `Once`; the
/// per-test mutex serializes mutation of the process-global backing state.
pub(crate) fn install_test_seams() -> MutexGuard<'static, ()> {
    let guard = test_guard().lock().unwrap_or_else(|e| e.into_inner());
    static INSTALLED: std::sync::Once = std::sync::Once::new();
    INSTALLED.call_once(install_seams_once);
    guard
}

fn install_seams_once() {
    // --- shmem allocator: always "not found" (first creation) in tests. ---
    backend_storage_ipc_shmem_seams::shmem_init_struct::set(|_name, _size| {
        Ok((core::ptr::null_mut(), false))
    });

    // --- buffer-header array (bufmgr descriptor array stand-in). ---
    backend_storage_buffer_bufmgr_seams::lock_buf_hdr::set(|buf_id| {
        let mut s = header_state().lock().unwrap();
        s.locked += 1;
        let st = s.states[buf_id as usize] | BM_LOCKED;
        s.states[buf_id as usize] = st;
        st
    });
    backend_storage_buffer_bufmgr_seams::unlock_buf_hdr::set(|buf_id, buf_state| {
        let mut s = header_state().lock().unwrap();
        s.locked -= 1;
        s.states[buf_id as usize] = buf_state & !BM_LOCKED;
    });
    backend_storage_buffer_bufmgr_seams::buf_free_next::set(|buf_id| {
        header_state().lock().unwrap().free_next[buf_id as usize]
    });
    backend_storage_buffer_bufmgr_seams::set_buf_free_next::set(|buf_id, value| {
        header_state().lock().unwrap().free_next[buf_id as usize] = value;
    });

    // --- strategy GUCs. ---
    backend_storage_buffer_bufmgr_seams::get_pin_limit::set(|| {
        header_state().lock().unwrap().pin_limit
    });
    backend_storage_buffer_bufmgr_seams::io_combine_limit::set(|| {
        header_state().lock().unwrap().io_combine_limit
    });
    backend_storage_buffer_bufmgr_seams::effective_io_concurrency::set(|| {
        header_state().lock().unwrap().effective_io_concurrency
    });
    backend_storage_buffer_bufmgr_seams::io_direct_data::set(|| false);

    // --- resource-owner buffer bookkeeping (PinLocalBuffer / UnpinLocalBuffer
    // remember/forget the local pin; no-op stubs for the local-only tests). ---
    backend_storage_buffer_bufmgr_seams::resowner_enlarge::set(|| Ok(()));
    backend_storage_buffer_bufmgr_seams::remember_buffer::set(|_b| {});
    backend_storage_buffer_bufmgr_seams::forget_buffer::set(|_b| {});

    // --- bgwriter wakeup latch. ---
    backend_storage_ipc_latch_seams::set_latch_for_procno::set(|procno| {
        header_state().lock().unwrap().latches.push(procno);
    });

    // --- local-buffer smgr I/O. ---
    backend_storage_smgr_seams::smgr_read::set(
        |_r: RelFileLocator, _bk: ProcNumber, _f: ForkNumber, _b: BlockNumber, dst: &mut [u8]| {
            dst.fill(0);
            Ok(())
        },
    );
    backend_storage_smgr_seams::smgr_write::set(
        |_r: RelFileLocator, _bk: ProcNumber, _f: ForkNumber, b: BlockNumber, src: &[u8]| {
            smgr_state().lock().unwrap().writes.push((b, src[0]));
            Ok(())
        },
    );
    backend_storage_smgr_seams::smgrnblocks::set(
        |_r: RelFileLocator, _bk: ProcNumber, _f: ForkNumber| {
            Ok(smgr_state().lock().unwrap().nblocks)
        },
    );
    backend_storage_smgr_seams::smgr_zeroextend::set(
        |_r: RelFileLocator, _bk: ProcNumber, _f: ForkNumber, b: BlockNumber, n: u32, _skip: bool| {
            let mut s = smgr_state().lock().unwrap();
            s.zeroextends.push((b, n));
            s.nblocks += n;
            Ok(())
        },
    );
    backend_storage_smgr_seams::smgr_prefetch::set(
        |_r: RelFileLocator, _bk: ProcNumber, _f: ForkNumber, _b: BlockNumber| Ok(false),
    );

    // --- localbuf diagnostics. ---
    backend_utils_init_small_seams::my_proc_number::set(|| 3 as ProcNumber);
    backend_utils_init_small_seams::nbuffers::set(|| 1024);
    common_relpath_seams::relpathbackend::set(
        |r: RelFileLocator, proc: ProcNumber, fork: ForkNumber| {
            let (db, spc, rel): (Oid, Oid, RelFileNumber) = (r.dbOid, r.spcOid, r.relNumber);
            std::format!("base/{db}/t{proc}_{rel}.{spc}.{}", fork as i32)
        },
    );

    // --- page checksum: a direct dep on backend-storage-page; install the xlog
    // seam it consults (checksums off in tests). ---
    backend_access_transam_xlog_seams::data_checksums_enabled::set(|| false);
}
