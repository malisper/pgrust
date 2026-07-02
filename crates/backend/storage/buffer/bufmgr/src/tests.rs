use core::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Once;

use init_small::globals;
use types_core::{ForkNumber, BLCKSZ};
use types_error::PgError;
use types_storage::buf::{
    BufferAccessStrategyType, BM_DIRTY, BM_LOCKED, BM_VALID, BUF_REFCOUNT_MASK,
};
use types_storage::{ReadBufferMode, RelFileLocator};

use super::*;

static SMGR_READS: AtomicU64 = AtomicU64::new(0);

const TEST_NBUFFERS: i32 = 64;

fn valid_page_into(buffer: &mut [u8], blkno: u32) {
    buffer.fill(0);
    let set_u16 = |b: &mut [u8], off: usize, v: u16| b[off..off + 2].copy_from_slice(&v.to_ne_bytes());
    set_u16(buffer, 12, 24);
    set_u16(buffer, 14, BLCKSZ as u16);
    set_u16(buffer, 16, BLCKSZ as u16);
    set_u16(buffer, 18, (BLCKSZ as u16) | 4);
    buffer[24..28].copy_from_slice(&blkno.to_ne_bytes());
}

// LWLock's contended wait path needs PGPROC (unported): serialize tests so
// exclusive partition-lock acquisitions never actually wait.
static TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn setup() -> std::sync::MutexGuard<'static, ()> {
    let guard = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    setup_once();
    guard
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

        smgr_seams::smgr_read::set(|_, _, blocknum, buffer| {
            SMGR_READS.fetch_add(1, Ordering::Relaxed);
            valid_page_into(buffer, blocknum);
            Ok(())
        });

        setup_write_seams();

        globals::SetNBuffers(TEST_NBUFFERS);
        globals::SetMaxBackends(4);
        lwlock::CreateLWLocks(false).unwrap();
        BufferManagerShmemInit().unwrap();
        init_seams();
    });
    globals::SetNBuffers(TEST_NBUFFERS);
    globals::SetMaxBackends(4);
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
