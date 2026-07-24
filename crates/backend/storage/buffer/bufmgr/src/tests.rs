use core::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, Ordering};
use std::sync::Once;

use init_small::globals;
use types_core::{ForkNumber, BLCKSZ, INVALID_PROC_NUMBER, RELPERSISTENCE_PERMANENT};
use types_error::PgError;
use types_storage::buf::{
    BufferAccessStrategyType, BM_DIRTY, BM_LOCKED, BM_VALID, BUF_REFCOUNT_MASK,
};
use types_storage::storage::NUM_SPECIAL_WORKER_PROCS;
use types_storage::{ReadBufferMode, RelFileLocator};

use super::*;

static SMGR_READS: AtomicU64 = AtomicU64::new(0);
static REL_READS: std::sync::Mutex<Vec<u32>> = std::sync::Mutex::new(Vec::new());
static READV_SIZES: std::sync::Mutex<Vec<usize>> = std::sync::Mutex::new(Vec::new());
// Widens the BM_IO_IN_PROGRESS window so a second reader lands in WaitIO.
const SLOW_READ_REL: u32 = 9400;

// Large enough that the batched-read pin cap (GetAdditionalPinLimit ~
// NBuffers/(MaxBackends+aux) - REFCOUNT_ARRAY_ENTRIES) still allows the full
// io_combine_limit run: 2048/79 - 8 = 17 extra pins >= 16.
const TEST_NBUFFERS: i32 = 2048;
const TEST_MAX_CONNECTIONS: i32 = 32;

fn test_max_backends() -> i32 {
    TEST_MAX_CONNECTIONS + 3 + 2 + 2 + NUM_SPECIAL_WORKER_PROCS
}

fn valid_page_into(buffer: &mut [u8], blkno: u32) {
    buffer.fill(0);
    let set_u16 = |b: &mut [u8], off: usize, v: u16| b[off..off + 2].copy_from_slice(&v.to_ne_bytes());
    set_u16(buffer, 12, 24);
    set_u16(buffer, 14, BLCKSZ as u16);
    set_u16(buffer, 16, BLCKSZ as u16);
    set_u16(buffer, 18, (BLCKSZ as u16) | 4);
    buffer[24..28].copy_from_slice(&blkno.to_ne_bytes());
    // The harness declares data_checksums_enabled, so every synthesized page
    // must carry a valid checksum: reads verify it (PageIsVerified).
    // SAFETY: BLCKSZ image, 4-aligned (page fixtures are buffer-pool blocks).
    let sum = unsafe { crate::write::checksum::page_checksum_raw(buffer.as_ptr(), blkno) };
    set_u16(buffer, 8, sum);
}

static TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn setup() -> std::sync::MutexGuard<'static, ()> {
    let guard = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    setup_once();
    become_backend();
    // Pins register with CurrentResourceOwner (thread-local), as in C.
    if resowner::CurrentResourceOwner().is_null() {
        let owner =
            resowner::ResourceOwnerCreate(types_resowner::ResourceOwner::NULL, "bufmgr-tests")
                .unwrap();
        resowner::SetCurrentResourceOwner(owner);
    }
    guard
}

fn become_backend() {
    if globals::MyProcNumber() != INVALID_PROC_NUMBER {
        return;
    }
    static NEXT_PROCNO: AtomicI32 = AtomicI32::new(0);
    let procno = NEXT_PROCNO.fetch_add(1, Ordering::Relaxed);
    assert!(procno < test_max_backends(), "proc slots exhausted");
    globals::SetMyProcNumber(procno);
    globals::SetMyProcPid(7000 + procno);
    waiteventset::InitializeWaitEventSupport().unwrap();
    let h = types_storage::latch::LatchHandle::proc(procno);
    latch::OwnLatch(h).unwrap();
    globals::SetMyLatch(Some(h));
    latch::InitializeLatchWaitSet().unwrap();
    // The read pipeline issues IO through pgaio: attach this thread's aio
    // backend slot (MyProc is the bind_task_proc TLS in this harness).
    lmgr_proc::bind_task_proc(procno);
    aio_core::pgaio_init_backend();
}

// Per-relation backing file for the smgr_startreadv fake; grown on demand
// with valid pages so the real preadv returns real bytes.
fn fake_rel_fd(rel: u32, blocknum: u32, nblocks: u32) -> i32 {
    use std::io::{Seek, SeekFrom, Write};
    use std::os::fd::AsRawFd;
    static FILES: std::sync::Mutex<Vec<(u32, std::fs::File)>> = std::sync::Mutex::new(Vec::new());
    let mut files = FILES.lock().unwrap();
    if !files.iter().any(|(r, _)| *r == rel) {
        let path = std::env::temp_dir().join(format!(
            "bufmgr-aio-test-{}-{}.rel",
            std::process::id(),
            rel
        ));
        let f = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        files.push((rel, f));
    }
    let f = &mut files.iter_mut().find(|(r, _)| *r == rel).unwrap().1;
    let needed_end = (blocknum + nblocks) as u64 * BLCKSZ as u64;
    let cur = f.metadata().unwrap().len();
    if cur < needed_end {
        let first = (cur / BLCKSZ as u64) as u32;
        let last = blocknum + nblocks;
        f.seek(SeekFrom::Start(first as u64 * BLCKSZ as u64)).unwrap();
        let mut page = vec![0u8; BLCKSZ];
        for b in first..last {
            valid_page_into(&mut page, b);
            f.write_all(&page).unwrap();
        }
        f.flush().unwrap();
    }
    f.as_raw_fd()
}

fn setup_once() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        shmem_seams::shmem_alloc::set(|size| {
            let layout = std::alloc::Layout::from_size_align(size, 128).unwrap();
            // Cluster-lifetime allocation, deliberately leaked (C: shmem segment).
            let p = unsafe { std::alloc::alloc_zeroed(layout) };
            assert!(!p.is_null());
            Ok(p)
        });
        shmem_seams::add_size::set(|a, b| {
            a.checked_add(b)
                .ok_or_else(|| Box::new(PgError::error("shmem size overflow")))
        });
        shmem_seams::mul_size::set(|a, b| {
            a.checked_mul(b)
                .ok_or_else(|| Box::new(PgError::error("shmem size overflow")))
        });
        static SHMEM_LOCK: AtomicBool = AtomicBool::new(false);
        shmem_seams::shmem_lock_acquire::set(|| {
            while SHMEM_LOCK.swap(true, Ordering::Acquire) {
                std::hint::spin_loop();
            }
        });
        shmem_seams::shmem_lock_release::set(|| SHMEM_LOCK.store(false, Ordering::Release));

        smgr_seams::smgr_read::set(|rlb, _, blocknum, buffer| {
            SMGR_READS.fetch_add(1, Ordering::Relaxed);
            REL_READS.lock().unwrap().push(rlb.locator.relNumber);
            if rlb.locator.relNumber == SLOW_READ_REL {
                std::thread::sleep(std::time::Duration::from_millis(200));
            }
            valid_page_into(buffer, blocknum);
            Ok(())
        });

        smgr_seams::smgr_readv::set(|rlb, _, blocknum, buffers| {
            SMGR_READS.fetch_add(1, Ordering::Relaxed);
            REL_READS.lock().unwrap().push(rlb.locator.relNumber);
            READV_SIZES.lock().unwrap().push(buffers.len());
            for (i, b) in buffers.iter_mut().enumerate() {
                valid_page_into(b, blocknum + i as u32);
            }
            Ok(())
        });

        // mdstartreadv stand-in: serve the readv from a real per-relation temp
        // file through the FULL pgaio pipeline (set iovec -> start_readv ->
        // preadv -> completion callbacks), so these suites exercise the same
        // machinery every io_method uses.
        smgr_seams::smgr_startreadv::set(|rlb, _, blocknum, pages| {
            SMGR_READS.fetch_add(1, Ordering::Relaxed);
            REL_READS.lock().unwrap().push(rlb.locator.relNumber);
            READV_SIZES.lock().unwrap().push(pages.len());
            if rlb.locator.relNumber == SLOW_READ_REL {
                std::thread::sleep(std::time::Duration::from_millis(200));
            }
            let fd = fake_rel_fd(rlb.locator.relNumber, blocknum, pages.len() as u32);
            // The real smgrstartreadv holds interrupts across the fd resolve
            // + start (see smgr's seam impl); mirror it here.
            globals::HoldInterrupts();
            let iovcnt = aio_core::pgaio_io_set_iovec_pages(pages, BLCKSZ);
            let ioh = aio_core::pgaio_io_current();
            aio_core::pgaio_io_set_target_smgr(
                ioh,
                rlb.locator,
                ForkNumber::MAIN_FORKNUM,
                blocknum,
                pages.len() as u32,
                false,
                false,
            );
            aio_core::pgaio_io_register_callbacks(
                ioh,
                types_storage::aio::PGAIO_HCB_MD_READV,
                0,
            );
            let r =
                aio_core::pgaio_io_start_readv_current(fd, iovcnt, blocknum as i64 * BLCKSZ as i64);
            if r.is_ok() {
                globals::ResumeInterrupts();
            }
            r
        });
        // md_readv_complete stand-in, C-faithful: bytes -> blocks, zero
        // blocks = ERROR, short = PARTIAL (ProcessReadBuffersResult's
        // progress assert relies on the smgr completion contract).
        smgr_seams::aio_md_readv_complete::set(|ioh, prior, _| {
            let mut r = prior;
            if prior.result < 0 {
                r.status = types_storage::aio::PgAioResultStatus::Error;
                r.id = types_storage::aio::PGAIO_HCB_MD_READV;
                r.error_data = (-prior.result) as u32;
                r.result = 0;
                return r;
            }
            r.result /= BLCKSZ as i32;
            let nblocks = aio_core::pgaio_io_get_target_data(ioh).smgr.nblocks as i32;
            if r.result == 0 {
                r.status = types_storage::aio::PgAioResultStatus::Error;
                r.id = types_storage::aio::PGAIO_HCB_MD_READV;
                r.error_data = 0;
            } else if r.status != types_storage::aio::PgAioResultStatus::Error
                && r.result < nblocks
            {
                r.status = types_storage::aio::PgAioResultStatus::Partial;
                r.id = types_storage::aio::PGAIO_HCB_MD_READV;
            }
            r
        });
        smgr_seams::aio_md_readv_report::set(|result, _td, elevel| {
            elog::ereport(elevel)
                .errmsg(format!("fake md readv failed: {:?}", result.status))
                .finish(types_error::ErrorLocation::new("tests", 0, "md_readv_report"))
        });

        setup_write_seams();

        s_lock_seams::perform_spin_delay::set(|_| std::thread::yield_now());
        s_lock_seams::finish_spin_delay::set(|_| {});
        ipc_seams::on_shmem_exit::set(|_, _| {});
        ipc_seams::before_shmem_exit::set(|_, _| Ok(()));
        waitevent_seams::pgstat_report_wait_start::set(|_| {});
        waitevent_seams::pgstat_report_wait_end::set(|| {});
        postgres_seams::check_for_interrupts::set(|| Ok(()));
        xact_seams::get_current_transaction_nest_level::set(|| 1);
        pg_sema::init_seams();

        globals::SetIsUnderPostmaster(false);
        globals::SetMaxConnections(TEST_MAX_CONNECTIONS);
        globals::set_max_worker_processes(2);
        globals::SetNBuffers(TEST_NBUFFERS);
        globals::SetMaxBackends(test_max_backends());
        lmgr_proc::init_seams();
        lmgr_proc::InitProcGlobal(&lmgr_proc::ProcGlobalConfig {
            autovacuum_worker_slots: 3,
            max_wal_senders: 2,
            max_prepared_xacts: 2,
            fastpath_lock_groups_per_backend: 1,
        });
        waiteventset::init_seams();
        latch::init_seams();
        lwlock::CreateLWLocks(false).unwrap();
        BufferManagerShmemInit().unwrap();
        init_seams();
        aio_core::init_seams();
        guc_tables::vars::io_max_combine_limit.install_if_absent(
            guc_tables::GucVarAccessors { get: || 16, set: |_| {} },
        );
        aio_core::AioShmemSize().unwrap();
        aio_core::AioShmemInit().unwrap();
    });
    globals::SetNBuffers(TEST_NBUFFERS);
    globals::SetMaxBackends(test_max_backends());
}

fn rel_reads(rel: u32) -> usize {
    REL_READS.lock().unwrap().iter().filter(|&&r| r == rel).count()
}

fn rloc(rel: u32) -> RelFileLocator {
    RelFileLocator {
        spcOid: 1663,
        dbOid: 5,
        relNumber: rel,
    }
}

fn read_blk(rel: u32, blkno: u32) -> Buffer {
    ReadBufferWithoutRelcache(
        rloc(rel),
        ForkNumber::MAIN_FORKNUM,
        blkno,
        ReadBufferMode::Normal,
        None,
        true,
    )
    .unwrap()
}

#[test]
fn header_kernel() {
    let _g = setup();
    let desc = GetBufferDescriptor(0);
    let s = LockBufHdr(desc);
    assert!(s & BM_LOCKED != 0);
    assert!(desc.state.load(Ordering::Relaxed) & BM_LOCKED != 0);
    UnlockBufHdr(desc, s);
    assert!(desc.state.load(Ordering::Relaxed) & BM_LOCKED == 0);
    assert_eq!(BUFFERDESC_PAD_TO_SIZE, 64);
    assert!(core::mem::size_of::<BufferDesc>() <= 64);
}

#[test]
fn batched_read_lands_run_and_stops_at_resident() {
    let _g = setup();
    let smgr = RelFileLocatorBackend {
        locator: rloc(9450),
        backend: INVALID_PROC_NUMBER,
    };
    // Make block 6 resident first: the batch run from 0 must stop before it.
    let pre = read_blk(9450, 6);
    ReleaseBuffer(pre).unwrap();

    let before = SMGR_READS.load(Ordering::Relaxed);
    let (b0, _) = read::ReadBuffer_batched(smgr, RELPERSISTENCE_PERMANENT, 0, 32, None).unwrap();
    assert!(b0 > 0);
    assert_eq!(GetPrivateRefCount(b0), 1);
    assert_eq!(
        SMGR_READS.load(Ordering::Relaxed) - before,
        1,
        "one vectored read for the whole run"
    );
    assert_eq!(*READV_SIZES.lock().unwrap().last().unwrap(), 6, "run 0..=5 stops at resident 6");

    // Extras are valid, resident, and unpinned; re-reading them is a pure hit.
    for blk in 1..6u32 {
        let b = read_blk(9450, blk);
        assert_eq!(
            SMGR_READS.load(Ordering::Relaxed) - before,
            1,
            "block {blk} must hit"
        );
        assert_eq!(GetPrivateRefCount(b), 1, "our fresh pin is the only local ref");
        let page = buffer_page_ref(b);
        assert!(!page.is_new());
        ReleaseBuffer(b).unwrap();
    }
    ReleaseBuffer(b0).unwrap();
}

#[test]
fn batched_read_caps_by_hint_and_combine_limit() {
    let _g = setup();
    let smgr = RelFileLocatorBackend {
        locator: rloc(9451),
        backend: INVALID_PROC_NUMBER,
    };
    // hint 3 caps the run.
    let (b, _) = read::ReadBuffer_batched(smgr, RELPERSISTENCE_PERMANENT, 10, 3, None).unwrap();
    assert_eq!(*READV_SIZES.lock().unwrap().last().unwrap(), 3);
    ReleaseBuffer(b).unwrap();
    // io_combine_limit (default 16) caps a large hint.
    let (b, _) = read::ReadBuffer_batched(smgr, RELPERSISTENCE_PERMANENT, 100, 10_000, None).unwrap();
    assert_eq!(*READV_SIZES.lock().unwrap().last().unwrap(), 16);
    ReleaseBuffer(b).unwrap();
    // hint 1 degrades to a single-block vectored read.
    let (b, _) = read::ReadBuffer_batched(smgr, RELPERSISTENCE_PERMANENT, 200, 1, None).unwrap();
    assert_eq!(*READV_SIZES.lock().unwrap().last().unwrap(), 1);
    ReleaseBuffer(b).unwrap();
}

#[test]
fn read_miss_then_warm_hit() {
    let _g = setup();
    let before = SMGR_READS.load(Ordering::Relaxed);
    let b1 = read_blk(9001, 0);
    assert!(b1 > 0);
    assert_eq!(GetPrivateRefCount(b1), 1);
    let desc = GetBufferDescriptor(b1 - 1);
    let state = desc.state.load(Ordering::Relaxed);
    assert!(state & BM_VALID != 0);
    assert_eq!(state & BUF_REFCOUNT_MASK, 1);
    let page = buffer_page_ref(b1);
    assert!(!page.is_new());

    let b2 = read_blk(9001, 0);
    assert_eq!(b2, b1);
    assert_eq!(GetPrivateRefCount(b1), 2);
    // second read is a mapping-table hit: no extra smgr read
    assert_eq!(SMGR_READS.load(Ordering::Relaxed), before + 1);

    ReleaseBuffer(b1).unwrap();
    ReleaseBuffer(b1).unwrap();
    assert_eq!(GetPrivateRefCount(b1), 0);
    assert_eq!(
        GetBufferDescriptor(b1 - 1).state.load(Ordering::Relaxed) & BUF_REFCOUNT_MASK,
        0
    );
    AtEOXact_Buffers(true);
}

#[test]
fn privref_new_pin_entry_is_independently_droppable() {
    // GL-ASSERTMASK-1 A1 — the born-RED, at the accounting layer.
    //
    // Deliberately NOT routed through PinBuffer_Locked: that function guards
    // this very state with `debug_assert!(GetPrivateRefCount(b) == 0)`, so a
    // test that drives it is inert (worse, red) in the dev tier — which is the
    // same profile-blindness this lane exists to fix. Nothing below trips a
    // debug assertion, so this bar is live in BOTH the dev and the shipped
    // profiles.
    const B: Buffer = 31337;
    assert_eq!(GetPrivateRefCount(B), 0, "stale private entry from another test");

    // Pin #1 — PinBuffer's path; its caller added one shared refcount.
    privref::ReservePrivateRefCountEntry();
    assert_eq!(privref::track_pin(B), 0);

    // Pin #2 — PinBuffer_Locked's path. Its caller has ALREADY added a second
    // shared refcount unconditionally (the bump is fused into the header
    // unlock), so the private entry it takes must be droppable on its own.
    privref::ReservePrivateRefCountEntry();
    privref::new_pin_entry(B);

    // track_unpin returns true exactly when the caller must release one shared
    // refcount. Two shared bumps therefore have to produce two of them; the
    // merged-counter shape produced only one and leaked the other forever.
    let drops =
        i32::from(privref::track_unpin(B)) + i32::from(privref::track_unpin(B));
    assert_eq!(
        drops, 2,
        "two shared bumps produced {drops} shared drop(s): the buffer keeps a \
         shared pin forever, never becomes replaceable, and InvalidateBuffer \
         spins on it without bound"
    );
    assert_eq!(GetPrivateRefCount(B), 0, "no private entry should remain");
}

// End-to-end companion on the real shared header word. Gated to the shipped
// profiles because it DOES drive PinBuffer_Locked on an already-pinned buffer,
// which is the state its own `debug_assert!` forbids — i.e. the defect exists
// only where the assertion does not, which is precisely the thesis.
#[cfg(not(debug_assertions))]
#[test]
fn pin_buffer_locked_pairs_each_shared_bump_with_its_own_drop() {
    // GL-ASSERTMASK-1 A1 — born-RED for the assertion-masked shared-refcount
    // leak. `PinBuffer_Locked` adds a shared refcount UNCONDITIONALLY (the
    // bump is fused into the header unlock), and its `debug_assert!` that no
    // local pin preexists is stripped from every shipped profile. With the
    // assertion gone, the private entry it takes must still be independently
    // droppable, or the two shared bumps pair with a single shared drop.
    //
    // Bars are real assert_eq!s on the SHARED refcount, so this is
    // release-effective. Arms differ: pre-fix the final shared refcount reads
    // 1 (leaked), post-fix 0.
    let _g = setup();
    let b = read_blk(9405, 0);
    let desc = GetBufferDescriptor(b - 1);
    let shared = |d: &BufferDesc| d.state.load(Ordering::Relaxed) & BUF_REFCOUNT_MASK;

    assert_eq!(shared(desc), 1, "one pin, one shared refcount");
    assert_eq!(GetPrivateRefCount(b), 1);

    // Drive PinBuffer_Locked's own contract: a reserved private entry, resowner
    // room, and the header lock held. This is the state its assertion forbids
    // and that a prefetch-pinned buffer reaching SyncOneBuffer produces.
    privref::ReservePrivateRefCountEntry();
    pin::resowner_enlarge_for_pin().unwrap();
    LockBufHdr(desc);
    pin::PinBuffer_Locked(desc);
    assert_eq!(shared(desc), 2, "PinBuffer_Locked bumps the shared refcount");

    // Both pins released => the shared refcount must be back to zero. Pre-fix
    // the merged private entry only reaches zero once, so only one of the two
    // shared bumps is ever dropped.
    ReleaseBuffer(b).unwrap();
    ReleaseBuffer(b).unwrap();
    assert_eq!(GetPrivateRefCount(b), 0, "no local pin should remain");
    assert_eq!(
        shared(desc),
        0,
        "shared refcount leaked: the buffer can never be replaced again and \
         InvalidateBuffer would spin on it without bound"
    );
    AtEOXact_Buffers(true);
}

#[test]
fn privref_array_overflow() {
    let _g = setup();
    let mut pinned = Vec::new();
    for blk in 0..12u32 {
        pinned.push(read_blk(9002, blk));
    }
    for (i, &b) in pinned.iter().enumerate() {
        assert_eq!(GetPrivateRefCount(b), 1, "block {i}");
    }
    for &b in &pinned {
        ReleaseBuffer(b).unwrap();
        assert_eq!(GetPrivateRefCount(b), 0);
    }
    AtEOXact_Buffers(true);
}

#[test]
fn lock_buffer_modes() {
    let _g = setup();
    let b = read_blk(9003, 0);
    LockBuffer(b, BUFFER_LOCK_SHARE).unwrap();
    LockBuffer(b, BUFFER_LOCK_UNLOCK).unwrap();
    LockBuffer(b, BUFFER_LOCK_EXCLUSIVE).unwrap();
    LockBuffer(b, BUFFER_LOCK_UNLOCK).unwrap();
    assert!(ConditionalLockBuffer(b).unwrap());
    LockBuffer(b, BUFFER_LOCK_UNLOCK).unwrap();
    assert!(LockBuffer(b, 42).is_err());
    ReleaseBuffer(b).unwrap();
}

#[test]
fn mark_dirty_sets_flags() {
    let _g = setup();
    let b = read_blk(9004, 0);
    LockBuffer(b, BUFFER_LOCK_EXCLUSIVE).unwrap();
    MarkBufferDirty(b).unwrap();
    let state = GetBufferDescriptor(b - 1).state.load(Ordering::Relaxed);
    assert!(state & BM_DIRTY != 0);
    LockBuffer(b, BUFFER_LOCK_UNLOCK).unwrap();
    ReleaseBuffer(b).unwrap();
}

#[test]
fn dirty_victim_flushed_on_eviction() {
    let _g = setup();
    setup_write_seams();
    init_small::globals::set_enableFsync(true);
    let rel = 9300u32;
    let b = read_blk(rel, 0);
    LockBuffer(b, BUFFER_LOCK_EXCLUSIVE).unwrap();
    MarkBufferDirty(b).unwrap();
    LockBuffer(b, BUFFER_LOCK_UNLOCK).unwrap();
    let tag = BufferGetTag(b);
    ReleaseBuffer(b).unwrap();
    for blk in 0..(TEST_NBUFFERS as u32 * 3) {
        let v = read_blk(9301, blk);
        ReleaseBuffer(v).unwrap();
    }
    let evicted = WRITES
        .lock()
        .unwrap()
        .iter()
        .any(|w| w.2 == rel && w.4 == tag.blockNum);
    assert!(evicted, "dirty victim written back through FlushBuffer");
    AtEOXact_Buffers(true);
}

#[test]
fn eviction_via_clock_sweep() {
    let _g = setup();
    let first = read_blk(9005, 0);
    let first_tag = BufferGetTag(first);
    ReleaseBuffer(first).unwrap();
    // Exhaust the freelist and force sweeps: > NBuffers distinct blocks.
    for blk in 0..(TEST_NBUFFERS as u32 * 3) {
        let b = read_blk(9006, blk);
        ReleaseBuffer(b).unwrap();
    }
    let before = SMGR_READS.load(Ordering::Relaxed);
    let again = read_blk(9005, 0);
    assert_eq!(BufferGetTag(again), first_tag);
    // First block was evicted, so this is a real re-read.
    assert_eq!(SMGR_READS.load(Ordering::Relaxed), before + 1);
    ReleaseBuffer(again).unwrap();
    AtEOXact_Buffers(true);
}

#[test]
fn recent_buffer_fastpath() {
    let _g = setup();
    let b = read_blk(9007, 3);
    ReleaseBuffer(b).unwrap();
    assert!(ReadRecentBuffer(rloc(9007), ForkNumber::MAIN_FORKNUM, 3, b).unwrap());
    assert_eq!(GetPrivateRefCount(b), 1);
    // pinned re-entry arm
    assert!(ReadRecentBuffer(rloc(9007), ForkNumber::MAIN_FORKNUM, 3, b).unwrap());
    assert_eq!(GetPrivateRefCount(b), 2);
    ReleaseBuffer(b).unwrap();
    ReleaseBuffer(b).unwrap();
    assert!(!ReadRecentBuffer(rloc(9007), ForkNumber::MAIN_FORKNUM, 99, b).unwrap());
    AtEOXact_Buffers(true);
}

#[test]
fn zero_and_lock() {
    let _g = setup();
    let b = ReadBufferWithoutRelcache(
        rloc(9008),
        ForkNumber::MAIN_FORKNUM,
        7,
        ReadBufferMode::ZeroAndLock,
        None,
        true,
    )
    .unwrap();
    assert!(buffer_page_is_new(b));
    let state = GetBufferDescriptor(b - 1).state.load(Ordering::Relaxed);
    assert!(state & BM_VALID != 0);
    UnlockReleaseBuffer(b).unwrap();
    AtEOXact_Buffers(true);
}

#[test]
fn access_strategies() {
    let _g = setup();
    assert!(GetAccessStrategy(BufferAccessStrategyType::BasNormal).is_none());
    let vac = GetAccessStrategy(BufferAccessStrategyType::BasVacuum).unwrap();
    let n = vac.borrow().nbuffers;
    assert!(n > 0 && n <= TEST_NBUFFERS / 8);
    let ring = GetAccessStrategyWithSize(BufferAccessStrategyType::BasBulkwrite, 0);
    assert!(ring.is_none());
    let strat = GetAccessStrategyWithSize(BufferAccessStrategyType::BasBulkread, 64);
    let b = ReadBufferWithoutRelcache(
        rloc(9009),
        ForkNumber::MAIN_FORKNUM,
        0,
        ReadBufferMode::Normal,
        strat.clone(),
        true,
    )
    .unwrap();
    ReleaseBuffer(b).unwrap();
    FreeAccessStrategy(strat);
    AtEOXact_Buffers(true);
}

#[test]
fn page_lsn_kernel() {
    let _g = setup();
    let b = read_blk(9010, 1);
    LockBuffer(b, BUFFER_LOCK_EXCLUSIVE).unwrap();
    buffer_page_set_lsn(b, 0x1234_5678_9ABC_DEF0);
    assert_eq!(buffer_page_get_lsn(b), 0x1234_5678_9ABC_DEF0);
    LockBuffer(b, BUFFER_LOCK_UNLOCK).unwrap();
    ReleaseBuffer(b).unwrap();
}

#[test]
fn concurrent_warm_hit_pins() {
    let _g = setup();
    let b = read_blk(9011, 0);
    let handles: Vec<_> = (0..4)
        .map(|_| {
            std::thread::spawn(|| {
                let owner = resowner::ResourceOwnerCreate(
                    types_resowner::ResourceOwner::NULL,
                    "bufmgr-tests",
                )
                .unwrap();
                resowner::SetCurrentResourceOwner(owner);
                for _ in 0..20_000 {
                    let b = read_blk(9011, 0);
                    ReleaseBuffer(b).unwrap();
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    assert_eq!(GetPrivateRefCount(b), 1);
    ReleaseBuffer(b).unwrap();
    let state = GetBufferDescriptor(b - 1).state.load(Ordering::Relaxed);
    assert_eq!(state & BUF_REFCOUNT_MASK, 0);
}

#[test]
fn buf_table_roundtrip() {
    let _g = setup();
    let b = read_blk(9012, 5);
    let tag = BufferGetTag(b);
    let hash = BufTableHashCode(&tag);
    let lock = BufMappingPartitionLock(hash);
    lwlock::LWLockAcquire(lock, lwlock::LW_SHARED, globals::MyProcNumber()).unwrap();
    let id = BufTableLookup(&tag, hash).unwrap();
    lwlock::LWLockRelease(lock).unwrap();
    assert_eq!(id, b - 1);
    ReleaseBuffer(b).unwrap();
}

static WRITES: std::sync::Mutex<Vec<(u32, u32, u32, i32, u32, u16)>> =
    std::sync::Mutex::new(Vec::new());
static WRITEBACKS: std::sync::Mutex<Vec<(u32, u32, u32)>> = std::sync::Mutex::new(Vec::new());

fn setup_write_seams() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        smgr_seams::smgr_write::set(|rlocator, forknum, blocknum, buffer, _skip_fsync| {
            assert_eq!(buffer.len(), BLCKSZ);
            // SAFETY: single-threaded unit test — no other backend exists to
            // write the image (the excluding mechanism WriteChunk asks for).
            let buffer = unsafe { buffer.as_slice_unchecked() };
            let checksum = u16::from_ne_bytes([buffer[8], buffer[9]]);
            WRITES.lock().unwrap().push((
                rlocator.locator.spcOid,
                rlocator.locator.dbOid,
                rlocator.locator.relNumber,
                forknum as i32,
                blocknum,
                checksum,
            ));
            Ok(())
        });
        smgr_seams::smgr_writeback::set(|rlocator, _forknum, blocknum, nblocks| {
            WRITEBACKS
                .lock()
                .unwrap()
                .push((rlocator.locator.relNumber, blocknum, nblocks));
            Ok(())
        });
        transam_xlog_seams::xlog_flush::set(|_| Ok(()));
        transam_xlog_seams::data_checksums_enabled::set(|| true);
    });
}

fn dirty_block(rel: u32, blkno: u32) -> Buffer {
    let b = read_blk(rel, blkno);
    LockBuffer(b, BUFFER_LOCK_EXCLUSIVE).unwrap();
    MarkBufferDirty(b).unwrap();
    LockBuffer(b, BUFFER_LOCK_UNLOCK).unwrap();
    ReleaseBuffer(b).unwrap();
    b
}

#[test]
fn checkpoint_writes_dirty_buffers_sorted() {
    let _g = setup();
    setup_write_seams();
    init_small::globals::set_enableFsync(true);
    crate::gucs::set_checkpoint_flush_after(32);

    let rel = 9100u32;
    let mut bufs = Vec::new();
    for blk in [2u32, 0, 1] {
        bufs.push(dirty_block(rel, blk));
    }

    CheckPointBuffers(0x0001).unwrap();

    let writes: Vec<_> = WRITES
        .lock()
        .unwrap()
        .iter()
        .filter(|w| w.2 == rel)
        .copied()
        .collect();
    let blocks: Vec<u32> = writes.iter().map(|w| w.4).collect();
    assert_eq!(blocks, vec![0, 1, 2], "ckpt_buforder sort by block");
    for w in &writes {
        assert_ne!(w.5, 0, "checksummed image written");
    }

    for &b in &bufs {
        let state = GetBufferDescriptor(b - 1).state.load(Ordering::Relaxed);
        assert_eq!(state & BM_DIRTY, 0);
        assert_eq!(state & types_storage::buf::BM_CHECKPOINT_NEEDED, 0);
    }

    // Sorted, consecutive blocks of one fork coalesce into one writeback.
    let wbs: Vec<_> = WRITEBACKS
        .lock()
        .unwrap()
        .iter()
        .filter(|w| w.0 == rel)
        .copied()
        .collect();
    assert_eq!(wbs, vec![(rel, 0, 3)]);

    // A clean pool re-checkpoint writes nothing for this rel.
    let before = WRITES.lock().unwrap().len();
    CheckPointBuffers(0x0001).unwrap();
    let after: Vec<_> = WRITES.lock().unwrap()[before..]
        .iter()
        .filter(|w| w.2 == rel)
        .copied()
        .collect();
    assert!(after.is_empty());
    AtEOXact_Buffers(true);
}

#[test]
fn checkpoint_balances_across_tablespaces() {
    let _g = setup();
    setup_write_seams();
    init_small::globals::set_enableFsync(true);

    let rel_a = 9200u32;
    let rel_b = 9201u32;
    for blk in 0..4u32 {
        dirty_block(rel_a, blk);
    }
    let b = ReadBufferWithoutRelcache(
        RelFileLocator { spcOid: 1664, dbOid: 0, relNumber: rel_b },
        ForkNumber::MAIN_FORKNUM,
        0,
        ReadBufferMode::Normal,
        None,
        true,
    )
    .unwrap();
    LockBuffer(b, BUFFER_LOCK_EXCLUSIVE).unwrap();
    MarkBufferDirty(b).unwrap();
    LockBuffer(b, BUFFER_LOCK_UNLOCK).unwrap();
    ReleaseBuffer(b).unwrap();

    let before = WRITES.lock().unwrap().len();
    CheckPointBuffers(0x0001).unwrap();
    let writes: Vec<_> = WRITES.lock().unwrap()[before..]
        .iter()
        .filter(|w| w.2 == rel_a || w.2 == rel_b)
        .copied()
        .collect();
    assert_eq!(writes.len(), 5);
    // Balancing interleaves tablespaces: the single-buffer 1664 space
    // finishes before the 4-buffer 1663 space does.
    let pos_b = writes.iter().position(|w| w.2 == rel_b).unwrap();
    assert!(pos_b < writes.len() - 1, "small tablespace not starved to the end");
    let a_blocks: Vec<u32> = writes.iter().filter(|w| w.2 == rel_a).map(|w| w.4).collect();
    assert_eq!(a_blocks, vec![0, 1, 2, 3]);
    AtEOXact_Buffers(true);
}

#[test]
fn checksum_copy_leaves_shared_page_untouched() {
    setup_write_seams();
    let mut page = Box::new([0u8; BLCKSZ]);
    for (i, b) in page.iter_mut().enumerate() {
        *b = (i & 0xff) as u8;
    }
    page[14..16].copy_from_slice(&100u16.to_ne_bytes()); // not PageIsNew
    let orig = *page;
    crate::write::with_checksummed_page(page.as_ptr(), 7, |out| {
        assert_ne!(out.as_ptr(), page.as_ptr(), "checksummed image must be a private copy");
        assert_eq!(out.len(), BLCKSZ);
        // SAFETY: single-threaded unit test; the image is this test's Box.
        let out = unsafe { out.as_slice_unchecked() };
        let mut want = orig;
        want[8..10].fill(0);
        let sum = crate::write::page_checksum_for_tests(&want, 7);
        assert_eq!(u16::from_ne_bytes([out[8], out[9]]), sum);
        assert_eq!(out[10..], want[10..]);
    });
    assert_eq!(page[..], orig[..], "shared page must not be mutated");

    // C PageSetChecksumCopy returns the input page itself when PageIsNew.
    let newpage = Box::new([0u8; BLCKSZ]);
    crate::write::with_checksummed_page(newpage.as_ptr(), 7, |out| {
        assert_eq!(out.as_ptr(), newpage.as_ptr());
    });
}

// The aliasing witness for the write path's no-copy arm. FlushBuffer runs with
// only a SHARE content lock, and SetHintBits -> MarkBufferDirtyHint mutates
// t_infomask in the same shared image under that same SHARE lock, so the image
// handed to smgr_write must travel in a type that admits a concurrent writer.
//
// This test is the gate: under `cargo miri test` the pre-fix shape (an &[u8]
// minted over the page) is reported as "Data race detected between (1) retag
// read ... and (2) non-atomic write" — a retag counts as an access to the race
// detector precisely because it licenses speculative reads — even though
// nothing in Rust ever loads the bytes (the kernel does, via pwritev). With
// WriteChunk no reference is minted and Miri is clean. Natively the test is a
// cheap functional assertion that the arm stays copy-free.
#[test]
fn shared_page_write_admits_a_concurrent_hint_bit_writer() {
    struct SharedBase(*mut u8);
    // The page is a real shared buffer-pool image in production; here the
    // pointer is just carried to the writer thread.
    unsafe impl Send for SharedBase {}

    // pd_upper == 0, so PageIsNew selects the no-copy arm without needing the
    // data_checksums seam (`||` short-circuits before the seam call).
    let mut page = Box::new([0u8; BLCKSZ]);
    let base = page.as_mut_ptr();
    let writing = std::sync::atomic::AtomicBool::new(false);

    std::thread::scope(|s| {
        let handoff = SharedBase(base);
        s.spawn(|| {
            let handoff = handoff;
            writing.store(true, Ordering::Relaxed);
            // SetHintBits' `t_infomask |= infomask`, at tuple-ish strides.
            for i in 0..64usize {
                // SAFETY: within the BLCKSZ image, which outlives the scope.
                unsafe { *handoff.0.add(24 + i * 8) |= 0x40 };
            }
        });
        while !writing.load(Ordering::Relaxed) {
            std::hint::spin_loop();
        }
        let (ptr, len) =
            crate::write::with_checksummed_page(base, 0, |chunk| (chunk.as_ptr(), chunk.len()));
        assert_eq!(ptr, base as *const u8, "the no-copy arm must write the live image");
        assert_eq!(len, BLCKSZ);
    });
}

#[test]
fn checksum_matches_c_reference() {
    // clang -O2 of storage/checksum_impl.h on this machine (pd_checksum
    // zeroed, patterned page byte = (i*37+11) & 0xff).
    let mut page = [0u8; BLCKSZ];
    for (i, b) in page.iter_mut().enumerate() {
        *b = (i.wrapping_mul(37).wrapping_add(11) & 0xff) as u8;
    }
    page[8..10].copy_from_slice(&0u16.to_ne_bytes());
    let expected: [(u32, u16); 5] =
        [(0, 24367), (1, 24366), (2, 24369), (3, 24368), (4, 24363)];
    for (blkno, want) in expected {
        assert_eq!(crate::write::page_checksum_for_tests(&page, blkno), want);
    }
    let zero = [0u8; BLCKSZ];
    assert_eq!(crate::write::page_checksum_for_tests(&zero, 42), 50816);
}

// The VACUUM error-path contract: a pin the error path never released is
// dropped by ResourceOwnerRelease(BEFORE_LOCKS) at abort, C's only mechanism.
#[test]
fn abort_resowner_release_drops_leaked_pin() {
    let _g = setup();
    use types_resowner::{ResourceOwner, RESOURCE_RELEASE_BEFORE_LOCKS};

    let save = resowner::CurrentResourceOwner();
    let owner = resowner::ResourceOwnerCreate(ResourceOwner::NULL, "xact-like").unwrap();
    resowner::SetCurrentResourceOwner(owner);

    let b1 = read_blk(9021, 0);
    let b2 = read_blk(9021, 1);
    IncrBufferRefCount(b1);
    ReleaseBuffer(b2).unwrap();
    assert_eq!(GetPrivateRefCount(b1), 2);
    assert_eq!(GetPrivateRefCount(b2), 0);

    resowner::ResourceOwnerRelease(owner, RESOURCE_RELEASE_BEFORE_LOCKS, false, true).unwrap();
    assert_eq!(GetPrivateRefCount(b1), 0);
    assert_eq!(
        GetBufferDescriptor(b1 - 1).state.load(Ordering::Relaxed) & BUF_REFCOUNT_MASK,
        0
    );
    AtEOXact_Buffers(false);

    resowner::SetCurrentResourceOwner(ResourceOwner::NULL);
    resowner::ResourceOwnerDelete(owner);
    resowner::SetCurrentResourceOwner(save);
}

#[test]
fn crash_reset_restores_boot_image() {
    let _g = setup();
    setup_write_seams();

    let b = dirty_block(9031, 5);
    let desc = GetBufferDescriptor(b - 1);
    assert!(desc.state.load(Ordering::Relaxed) & BM_DIRTY != 0);
    let tag = desc.tag();
    let hashcode = BufTableHashCode(&tag);

    BufferManagerShmemResetAfterCrash();

    assert_eq!(desc.state.load(Ordering::Relaxed), 0);
    assert_eq!(desc.tag().blockNum, types_core::InvalidBlockNumber);
    assert_eq!(BufTableLookup(&tag, hashcode).unwrap(), -1);
    assert_eq!(
        desc.content_lock.state.load(Ordering::Relaxed),
        lwlock::LW_FLAG_RELEASE_OK
    );
    assert!(have_free_buffer());
    assert_eq!(StrategySyncStart(), (0, 0, 0));

    let before = SMGR_READS.load(Ordering::Relaxed);
    let b2 = read_blk(9032, 0);
    assert_eq!(b2, 1, "freelist must hand out buffer 0 first after reset");
    assert_eq!(SMGR_READS.load(Ordering::Relaxed), before + 1);
    ReleaseBuffer(b2).unwrap();
}

fn spawn_reader(
    rel: u32,
    delay: std::time::Duration,
) -> std::thread::JoinHandle<(Buffer, types_storage::buf::buftag)> {
    std::thread::spawn(move || {
        become_backend();
        let owner =
            resowner::ResourceOwnerCreate(types_resowner::ResourceOwner::NULL, "bufmgr-tests")
                .unwrap();
        resowner::SetCurrentResourceOwner(owner);
        std::thread::sleep(delay);
        let b = read_blk(rel, 0);
        let state = GetBufferDescriptor(b - 1).state.load(Ordering::Acquire);
        assert!(state & BM_VALID != 0, "reader got an invalid buffer");
        let tag = BufferGetTag(b);
        ReleaseBuffer(b).unwrap();
        (b, tag)
    })
}

// Two backends race ReadBuffer on one uncached block: loser sleeps in WaitIO.
#[test]
fn concurrent_cold_read_second_backend_waits_for_io() {
    let _g = setup();
    assert_eq!(rel_reads(SLOW_READ_REL), 0);
    let t1 = spawn_reader(SLOW_READ_REL, std::time::Duration::ZERO);
    // 200ms slow read: +40ms lands inside the BM_IO_IN_PROGRESS window.
    let t2 = spawn_reader(SLOW_READ_REL, std::time::Duration::from_millis(40));
    let (b1, tag1) = t1.join().unwrap();
    let (b2, tag2) = t2.join().unwrap();
    assert_eq!(b1, b2, "both readers must resolve to the same buffer");
    assert_eq!(tag1, tag2);
    assert_eq!(
        rel_reads(SLOW_READ_REL),
        1,
        "loser must WaitIO on the winner's read, not issue its own"
    );
    AtEOXact_Buffers(true);
}

// ResourceOwnerRelease(BEFORE_LOCKS) runs AbortBufferIO before pin release
// (prio 100 < 200) and must wake CV waiters — C's only mid-IO error mechanism.
#[test]
fn abort_resowner_release_aborts_leaked_io_and_wakes_waiter() {
    let _g = setup();
    use types_resowner::{ResourceOwner, RESOURCE_RELEASE_BEFORE_LOCKS};
    let rel = 9501u32;

    let save = resowner::CurrentResourceOwner();
    let owner = resowner::ResourceOwnerCreate(ResourceOwner::NULL, "io-leak").unwrap();
    resowner::SetCurrentResourceOwner(owner);

    let b = read_blk(rel, 0);
    let desc = GetBufferDescriptor(b - 1);
    // extend.rs beyond-EOF shape: invalidate, take input IO, "error out".
    let s = LockBufHdr(desc);
    UnlockBufHdr(desc, s & !BM_VALID);
    assert!(crate::read::StartBufferIO(desc, true, false, true).unwrap());

    let waiter = spawn_reader(rel, std::time::Duration::ZERO);
    std::thread::sleep(std::time::Duration::from_millis(150));

    resowner::ResourceOwnerRelease(owner, RESOURCE_RELEASE_BEFORE_LOCKS, false, true).unwrap();

    let (b2, _) = waiter.join().unwrap();
    assert_eq!(b2, b, "waiter must land on the aborted buffer");
    let state = desc.state.load(Ordering::Acquire);
    assert!(state & BM_VALID != 0, "waiter must redo the IO after abort");
    assert!(state & types_storage::buf::BM_IO_ERROR == 0);
    assert_eq!(rel_reads(rel), 2, "abort forces the waiter to reissue the read");
    assert_eq!(GetPrivateRefCount(b), 0);

    AtEOXact_Buffers(false);
    resowner::SetCurrentResourceOwner(ResourceOwner::NULL);
    resowner::ResourceOwnerDelete(owner);
    resowner::SetCurrentResourceOwner(save);
}

// ---- local (temp) buffers ----

fn temp_smgr(rel: u32) -> types_storage::RelFileLocatorBackend {
    types_storage::RelFileLocatorBackend {
        locator: rloc(rel),
        backend: globals::MyProcNumber(),
    }
}

fn read_local_blk(rel: u32, blkno: u32) -> Buffer {
    ReadBuffer_common(
        temp_smgr(rel),
        types_core::RELPERSISTENCE_TEMP,
        ForkNumber::MAIN_FORKNUM,
        blkno,
        ReadBufferMode::Normal,
        None,
    )
    .unwrap()
    .0
}

fn setup_extend_seams() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        smgr_seams::smgr_nblocks::set(|rlb, _| {
            Ok(*NBLOCKS.lock().unwrap().entry(rlb.locator.relNumber).or_insert(0))
        });
        smgr_seams::smgr_zeroextend::set(|rlb, _, blocknum, nblocks, _| {
            let mut map = NBLOCKS.lock().unwrap();
            let n = map.entry(rlb.locator.relNumber).or_insert(0);
            assert_eq!(*n, blocknum);
            *n += nblocks as u32;
            Ok(())
        });
    });
}

static NBLOCKS: std::sync::Mutex<std::collections::BTreeMap<u32, u32>> =
    std::sync::Mutex::new(std::collections::BTreeMap::new());

#[test]
fn local_negative_encoding_and_roundtrip() {
    let _g = setup();
    let rel = 9500;
    let before = rel_reads(rel);
    let b = read_local_blk(rel, 3);
    assert!(b < 0, "temp relations get negative buffer ids");
    assert_eq!(rel_reads(rel), before + 1);
    assert_eq!(crate::localbuf::local_ref_count(b), 1);
    assert!(BufferIsPinned(b));
    assert_eq!(BufferGetBlockNumber(b), 3);
    let page = buffer_page_ref(b);
    assert!(!page.is_new());

    let b2 = read_local_blk(rel, 3);
    assert_eq!(b2, b, "warm hit returns the same local buffer");
    assert_eq!(rel_reads(rel), before + 1, "hit does not re-read");
    assert_eq!(crate::localbuf::local_ref_count(b), 2);

    IncrBufferRefCount(b);
    assert_eq!(crate::localbuf::local_ref_count(b), 3);
    ReleaseBuffer(b).unwrap();
    ReleaseBuffer(b).unwrap();
    ReleaseBuffer(b).unwrap();
    assert_eq!(crate::localbuf::local_ref_count(b), 0);
    AtEOXact_Buffers(true);
}

#[test]
fn local_mark_dirty_and_flush_on_drop_path() {
    let _g = setup();
    let rel = 9501;
    let b = read_local_blk(rel, 0);
    LockBuffer(b, BUFFER_LOCK_EXCLUSIVE).unwrap();
    MarkBufferDirty(b).unwrap();
    LockBuffer(b, BUFFER_LOCK_UNLOCK).unwrap();
    let desc = crate::localbuf::local_desc(b);
    assert!(desc.state.load(Ordering::Relaxed) & BM_DIRTY != 0);
    MarkBufferDirtyHint(b, true).unwrap();
    ReleaseBuffer(b).unwrap();

    crate::localbuf::FlushRelationLocalBuffers(rloc(rel)).unwrap();
    assert!(
        WRITES
            .lock()
            .unwrap()
            .iter()
            .any(|w| w.2 == rel && w.4 == 0),
        "dirty local page reaches smgrwrite"
    );
    assert!(desc.state.load(Ordering::Relaxed) & BM_DIRTY == 0);

    DropRelationAllLocalBuffers(rloc(rel)).unwrap();
    let before = rel_reads(rel);
    let b2 = read_local_blk(rel, 0);
    assert_eq!(rel_reads(rel), before + 1, "dropped block re-reads from smgr");
    ReleaseBuffer(b2).unwrap();
}

#[test]
fn local_eviction_writes_dirty_page() {
    let _g = setup();
    let rel = 9502;
    let b = read_local_blk(rel, 0);
    LockBuffer(b, BUFFER_LOCK_EXCLUSIVE).unwrap();
    MarkBufferDirty(b).unwrap();
    LockBuffer(b, BUFFER_LOCK_UNLOCK).unwrap();
    ReleaseBuffer(b).unwrap();

    let n = crate::localbuf::n_loc_buffer();
    for blkno in 1..(n as u32 + 1) {
        let b = read_local_blk(rel, blkno);
        ReleaseBuffer(b).unwrap();
    }
    assert!(
        WRITES.lock().unwrap().iter().any(|w| w.2 == rel && w.4 == 0),
        "clock sweep flushed the dirty victim"
    );
    DropRelationAllLocalBuffers(rloc(rel)).unwrap();
    AtEOXact_Buffers(true);
}

#[test]
fn local_extend_returns_pinned_zeroed_pages() {
    let _g = setup();
    setup_extend_seams();
    let rel = 9503;
    let mut buffers = [types_core::InvalidBuffer; 8];
    let (first_block, extended_by) = crate::localbuf::ExtendBufferedRelLocal(
        temp_smgr(rel),
        ForkNumber::MAIN_FORKNUM,
        4,
        types_core::InvalidBlockNumber,
        &mut buffers,
    )
    .unwrap();
    assert_eq!(first_block, 0);
    assert_eq!(extended_by, 4);
    assert_eq!(*NBLOCKS.lock().unwrap().get(&rel).unwrap(), 4);
    for (i, b) in buffers.iter().take(4).enumerate() {
        assert!(*b < 0);
        assert_eq!(crate::localbuf::local_ref_count(*b), 1);
        assert_eq!(BufferGetBlockNumber(*b), i as u32);
        assert!(buffer_page_is_new(*b), "extended pages are zero-filled");
        ReleaseBuffer(*b).unwrap();
    }
    let (first_block, extended_by) = crate::localbuf::ExtendBufferedRelLocal(
        temp_smgr(rel),
        ForkNumber::MAIN_FORKNUM,
        2,
        types_core::InvalidBlockNumber,
        &mut buffers,
    )
    .unwrap();
    assert_eq!(first_block, 4);
    assert_eq!(extended_by, 2);
    for b in buffers.iter().take(2) {
        ReleaseBuffer(*b).unwrap();
    }
    DropRelationAllLocalBuffers(rloc(rel)).unwrap();
}

#[test]
fn local_release_and_read_buffer_fastpath() {
    let _g = setup();
    let rel = 9504;
    let b = read_local_blk(rel, 7);
    assert!(crate::localbuf::StartLocalBufferIO(b, false) == false, "clean page: no write IO");
    assert_eq!(crate::localbuf::local_ref_count(b), 1);
    assert!(ConditionalLockBuffer(b).unwrap());
    assert!(crate::ops::ConditionalLockBufferForCleanup(b).unwrap());
    CheckBufferIsPinnedOnce(b).unwrap();
    LockBufferForCleanup(b).unwrap();
    UnlockReleaseBuffer(b).unwrap();
    assert_eq!(crate::localbuf::local_ref_count(b), 0);
}

fn synth_tag(rel: u32, blkno: u32) -> types_storage::buf::buftag {
    types_storage::buf::buftag {
        spcOid: 1663,
        dbOid: 5,
        relNumber: rel,
        forkNum: ForkNumber::MAIN_FORKNUM,
        blockNum: blkno,
    }
}

fn bt_insert(tag: &types_storage::buf::buftag, id: i32) -> i32 {
    let hash = BufTableHashCode(tag);
    let lock = BufMappingPartitionLock(hash);
    lwlock::LWLockAcquire(lock, lwlock::LW_EXCLUSIVE, globals::MyProcNumber()).unwrap();
    let r = crate::buf_table::BufTableInsert(tag, hash, id).unwrap();
    lwlock::LWLockRelease(lock).unwrap();
    r
}

fn bt_lookup(tag: &types_storage::buf::buftag) -> i32 {
    let hash = BufTableHashCode(tag);
    let lock = BufMappingPartitionLock(hash);
    lwlock::LWLockAcquire(lock, lwlock::LW_SHARED, globals::MyProcNumber()).unwrap();
    let r = BufTableLookup(tag, hash).unwrap();
    lwlock::LWLockRelease(lock).unwrap();
    r
}

fn bt_delete(tag: &types_storage::buf::buftag) -> types_error::PgResult<()> {
    let hash = BufTableHashCode(tag);
    let lock = BufMappingPartitionLock(hash);
    lwlock::LWLockAcquire(lock, lwlock::LW_EXCLUSIVE, globals::MyProcNumber()).unwrap();
    let r = crate::buf_table::BufTableDelete(tag, hash);
    lwlock::LWLockRelease(lock).unwrap();
    r
}

// Dense-table torture: grow under load, backward-shift deletion keeping every
// probe chain intact, and the relfilenode-swap/truncate invalidation class
// (same tag re-mapped to a new buffer id after delete — the targblock
// incident shape).
#[test]
fn buf_table_dense_grow_delete_reinsert() {
    let _g = setup();
    let rel = 9700;
    let n: u32 = 600;
    for i in 0..n {
        assert_eq!(bt_insert(&synth_tag(rel, i), 1000 + i as i32), -1, "insert {i}");
    }
    for i in 0..n {
        assert_eq!(bt_lookup(&synth_tag(rel, i)), 1000 + i as i32, "lookup {i}");
    }
    assert_eq!(bt_insert(&synth_tag(rel, 7), 4242), 1007, "duplicate insert returns existing id");
    for i in (0..n).step_by(3) {
        bt_delete(&synth_tag(rel, i)).unwrap();
    }
    for i in 0..n {
        let expect = if i % 3 == 0 { -1 } else { 1000 + i as i32 };
        assert_eq!(bt_lookup(&synth_tag(rel, i)), expect, "post-delete lookup {i}");
    }
    for i in (0..n).step_by(3) {
        assert_eq!(bt_insert(&synth_tag(rel, i), 2000 + i as i32), -1, "reinsert {i}");
        assert_eq!(bt_lookup(&synth_tag(rel, i)), 2000 + i as i32, "swap remap {i}");
    }
    for i in 0..n {
        bt_delete(&synth_tag(rel, i)).unwrap();
    }
    for i in 0..n {
        assert_eq!(bt_lookup(&synth_tag(rel, i)), -1, "post-drop lookup {i}");
    }
    let err = bt_delete(&synth_tag(rel, 0)).unwrap_err();
    assert!(format!("{err:?}").contains("shared buffer hash table corrupted"));
}


// ---- cleanup lock / pin-count waiter ----

// The foreign_data-sweep abort shape: a cleanup-lock waiter (VACUUM) parks in
// ProcWaitForSignal under BM_PIN_COUNT_WAITER while another backend holds a
// pin; that backend's last unpin wakes it via WakePinCountWaiter/ProcSendSignal.
#[test]
fn cleanup_lock_waits_for_concurrent_pin_and_is_woken_by_unpin() {
    let _g = setup();
    let rel = 9800u32;
    let b = read_blk(rel, 0);

    let (tx, rx) = std::sync::mpsc::channel();
    let holder = std::thread::spawn(move || {
        become_backend();
        let owner =
            resowner::ResourceOwnerCreate(types_resowner::ResourceOwner::NULL, "pin-holder")
                .unwrap();
        resowner::SetCurrentResourceOwner(owner);
        let b2 = read_blk(rel, 0);
        tx.send(b2).unwrap();
        // Unpin only once the cleanup waiter has registered; ProcSendSignal
        // before the waiter parks is safe (latch stays set), so no lost wakeup.
        let desc = GetBufferDescriptor(b2 - 1);
        while desc.state.load(Ordering::Acquire) & types_storage::buf::BM_PIN_COUNT_WAITER == 0 {
            std::thread::yield_now();
        }
        ReleaseBuffer(b2).unwrap();
    });
    let b2 = rx.recv().unwrap();
    assert_eq!(b2, b, "both backends must resolve to the same buffer");
    LockBufferForCleanup(b).unwrap();
    holder.join().unwrap();

    let state = GetBufferDescriptor(b - 1).state.load(Ordering::Acquire);
    assert_eq!(state & BUF_REFCOUNT_MASK, 1, "cleanup lock implies pincount 1");
    assert_eq!(state & types_storage::buf::BM_PIN_COUNT_WAITER, 0);
    assert_eq!(crate::pin::pin_count_wait_buf(), -1);
    assert!(crate::ops::IsBufferCleanupOK(b));
    UnlockReleaseBuffer(b).unwrap();
}

// UnlockBuffers (abort path) clears an abandoned BM_PIN_COUNT_WAITER so the
// next unpin does not try to wake a waiter that already errored out.
#[test]
fn unlock_buffers_clears_abandoned_waiter_flag() {
    let _g = setup();
    let rel = 9802u32;
    let b = read_blk(rel, 0);
    let desc = GetBufferDescriptor(b - 1);

    let mut state = LockBufHdr(desc);
    // SAFETY: header lock held.
    unsafe { desc.set_wait_backend_pgprocno(globals::MyProcNumber()) };
    crate::pin::set_pin_count_wait_buf(desc.buf_id);
    state |= types_storage::buf::BM_PIN_COUNT_WAITER;
    UnlockBufHdr(desc, state);

    UnlockBuffers();
    assert_eq!(crate::pin::pin_count_wait_buf(), -1);
    let state = desc.state.load(Ordering::Acquire);
    assert_eq!(state & types_storage::buf::BM_PIN_COUNT_WAITER, 0);
    ReleaseBuffer(b).unwrap();
}

// A failed ResourceOwnerForget on unpin degrades to WARNING; a panic here
// during unwind aborts the whole process (the sweep's crash amplification).
#[test]
fn unpin_with_mismatched_owner_warns_instead_of_panicking() {
    let _g = setup();
    let rel = 9803u32;
    let save = resowner::CurrentResourceOwner();
    let b = read_blk(rel, 0);

    let other =
        resowner::ResourceOwnerCreate(types_resowner::ResourceOwner::NULL, "wrong-owner").unwrap();
    resowner::SetCurrentResourceOwner(other);
    // Pin is remembered on `save`; the forget on `other` fails but the shared
    // refcount must still be released without panicking.
    ReleaseBuffer(b).unwrap();
    assert_eq!(GetPrivateRefCount(b), 0);

    resowner::SetCurrentResourceOwner(save);
    resowner::ResourceOwnerForget(save, datum::Datum::from_i32(b), crate::pin::buffer_pin_desc())
        .unwrap();
    resowner::ResourceOwnerDelete(other);
    let state = GetBufferDescriptor(b - 1).state.load(Ordering::Acquire);
    assert_eq!(state & BUF_REFCOUNT_MASK, 0);
}
