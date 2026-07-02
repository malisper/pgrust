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
    // Pin retained for the process lifetime: a dirty victim would (correctly)
    // panic at the FlushBuffer phase-2 boundary.
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
