//! Integration tests for the WAL-insertion path: allocate a real (malloc-
//! backed) `XLogCtl` shmem region, then reserve + copy a record through the
//! genuine WAL-buffer ring and verify the bytes landed with a correct header.

extern crate std;

use super::*;
use alloc::vec;
use alloc::vec::Vec;
use std::sync::{Mutex, Once};

use ipc_shmem_seams as shmem_seam;
use lwlock as lwlock;
use lwlock_seams as lwlock_seam;
use lmgr_proc_seams as proc_s;
use waitevent_seams as waitevent;
use init_small::globals;
use init_small_seams as init_globals;

use insert::SizeOfXLogRecord;
use types_storage::storage::{proclist_node, LWLockWaitState, LW_WS_NOT_WAITING};

static TEST_LOCK: Mutex<()> = Mutex::new(());
static INSTALL: Once = Once::new();

std::thread_local! {
    static PROC_LW: std::cell::RefCell<[(LWLockWaitState, proclist_node); 64]> =
        std::cell::RefCell::new([(LW_WS_NOT_WAITING, proclist_node { next: 0, prev: 0 }); 64]);
}

fn install_seams() {
    // malloc-backed ShmemInitStruct: a fresh zeroed region each time, never
    // "found" (single-process bootstrap path).
    shmem_seam::shmem_init_struct::set(|_name, size| {
        let layout = std::alloc::Layout::from_size_align(size.max(1), 128).unwrap();
        // SAFETY: nonzero size; freed at process exit (test lifetime).
        let p = unsafe { std::alloc::alloc_zeroed(layout) };
        Ok((p, false))
    });
    shmem_seam::add_size::set(|a, b| Ok(a + b));
    shmem_seam::mul_size::set(|a, b| Ok(a * b));

    // The real LWLock initializer for the WALInsertLock array.
    lwlock_seam::lwlock_initialize::set(lwlock::LWLockInitialize);

    // Interrupt holdoff / wait-event reporting are no-ops for the test.
    init_globals::hold_interrupts::set(|| {});
    init_globals::resume_interrupts::set(|| {});
    waitevent::pgstat_report_wait_start::set(|_| {});
    waitevent::pgstat_report_wait_end::set(|| {});

    // PGPROC LWLock wait-list fields backed by a thread-local fake array.
    proc_s::proc_lw_waiting::set(|p| PROC_LW.with(|a| a.borrow()[p as usize].0));
    proc_s::set_proc_lw_waiting::set(|p, s| PROC_LW.with(|a| a.borrow_mut()[p as usize].0 = s));
    proc_s::proc_lw_wait_link::set(|p| PROC_LW.with(|a| a.borrow()[p as usize].1));
    proc_s::set_proc_lw_wait_link::set(|p, n| PROC_LW.with(|a| a.borrow_mut()[p as usize].1 = n));
    proc_s::proc_lw_wait_mode::set(|_| types_storage::storage::LW_EXCLUSIVE);
    proc_s::set_proc_lw_wait_mode::set(|_, _| {});
    proc_s::pg_semaphore_lock::set(|_| {});
    proc_s::pg_semaphore_unlock::set(|_| {});
    proc_s::set_proc_latch::set(|_| {});
}

static LWLOCKS_CREATED: Once = Once::new();

fn setup() -> std::sync::MutexGuard<'static, ()> {
    let g = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    INSTALL.call_once(install_seams);
    // Publish the main LWLock array once so the built-in WALBufMappingLock that
    // AdvanceXLInsertBuffer takes is real (CreateLWLocks(is_under_postmaster=false)).
    LWLOCKS_CREATED.call_once(|| {
        let ctx = mcx::MemoryContext::new("xlog-insert-test-lwlocks");
        lwlock::CreateLWLocks(ctx.mcx(), false).expect("CreateLWLocks");
        // CreateLWLocks leaks the context's arena for the process lifetime.
        core::mem::forget(ctx);
    });
    globals::SetMyProcNumber(0);
    g
}

/// Build an `XLogRecord` header in bytes (LP64 layout), with `xl_crc` holding
/// the caller's running CRC over the record data (here: over `data`).
fn build_record(rmid: u8, info: u8, data: &[u8]) -> Vec<u8> {
    let total = SizeOfXLogRecord + data.len();
    let mut hdr = vec![0u8; SizeOfXLogRecord];
    hdr[0..4].copy_from_slice(&(total as u32).to_ne_bytes()); // xl_tot_len
    // xl_xid @4 = 0 (InvalidTransactionId)
    // xl_prev @8 = 0 (filled by XLogInsertRecord)
    hdr[16] = info; // xl_info
    hdr[17] = rmid; // xl_rmid
    // partial CRC over the record data (xloginsert.c side): start at
    // INIT_CRC32C (0xFFFFFFFF), comp over data, but DO NOT finalize.
    let partial = crc32c::pg_comp_crc32c_sb8(0xFFFF_FFFF, data);
    hdr[20..24].copy_from_slice(&partial.to_ne_bytes()); // xl_crc (running)

    let mut rec = hdr;
    rec.extend_from_slice(data);
    rec
}

#[test]
fn insert_single_record_lands_in_wal_buffer() {
    let _g = setup();

    // Resolve a small WAL buffer count and allocate the shmem region.
    shmem::set_xlog_buffers(8);
    shmem::XLOGShmemInit(8).expect("XLOGShmemInit");

    // Bring the system out of recovery so insertion is allowed, set up the
    // insert timeline + the initial InitializedUpTo (the StartupXLOG job in the
    // real server; here we set the minimum needed for the buffer ring).
    let ctl = shmem::xlog_ctl();
    assert!(!ctl.is_null());
    // SAFETY: live region; single-threaded test.
    // Start inserting one page's worth of usable bytes in, so the first record
    // lands on WAL page 1 (page index != 0). The real server never inserts on
    // page 0 of the cache before StartupXLOG has positioned things; starting at
    // a non-zero page also exercises GetXLogBuffer's cache without aliasing the
    // zero-initialised `cachedPage`.
    let usable_per_page =
        (wal::xlog_consts::XLOG_BLCKSZ - wal::xlog_consts::SIZE_OF_XLOG_SHORT_PHD) as u64;
    let seg = shmem::wal_segment_size();
    let start_bytepos = usable_per_page;
    // SAFETY: live region; single-threaded test.
    unsafe {
        (*ctl).SharedRecoveryState = wal::xlog_consts::RecoveryState::Done;
        (*ctl).InsertTimeLineID = 1;
        (*ctl).InitializedUpTo = 0;
        (*ctl).Insert.CurrBytePos = start_bytepos;
        (*ctl).Insert.PrevBytePos = start_bytepos;
        (*ctl).Insert.fullPageWrites = false;
        (*ctl).Insert.runningBackups = 0;
    }

    // A heap rmgr record (rmid 10, arbitrary), 16 data bytes.
    let data: Vec<u8> = (0..16u8).collect();
    let rec = build_record(10, 0, &data);

    // The reservation maps usable-bytepos `start_bytepos` to a physical LSN.
    let start = XLogBytePosToRecPtr(start_bytepos, seg);
    let total = (SizeOfXLogRecord + data.len()) as u64;
    // The record fits on a single page here; EndPos = MAXALIGN64(start+total).
    let expected_end = (start + total + 7) & !7;

    let end = XLogInsertRecord(&[&rec[..]], InvalidXLogRecPtr, 0, 0, false)
        .expect("XLogInsertRecord");
    assert_eq!(end, expected_end, "end LSN");

    // ProcLastRecPtr / XactLastRecEnd updated.
    assert_eq!(insert::proc_last_rec_ptr(), start);
    assert_eq!(insert::xact_last_rec_end(), end);

    // Verify the record bytes landed in the ring at the right page + offset.
    let blcksz = wal::xlog_consts::XLOG_BLCKSZ as u64;
    let page_idx = ((start / blcksz) % 8) as usize;
    let page_off = (start % blcksz) as usize;
    // SAFETY: live region.
    unsafe {
        let page = (*ctl).pages.add(page_idx * wal::xlog_consts::XLOG_BLCKSZ);
        // Page header magic.
        let magic = u16::from_ne_bytes([*page, *page.add(1)]);
        assert_eq!(magic, 0xD118, "xlp_magic");

        let recpos = page.add(page_off);
        // xl_tot_len matches.
        let tot = u32::from_ne_bytes([
            *recpos, *recpos.add(1), *recpos.add(2), *recpos.add(3),
        ]);
        assert_eq!(tot as u64, total);
        // xl_rmid landed.
        assert_eq!(*recpos.add(17), 10);
        // The data bytes follow the 24-byte header.
        for (i, b) in data.iter().enumerate() {
            assert_eq!(*recpos.add(SizeOfXLogRecord + i), *b, "data byte {i}");
        }

        // The finalized record CRC must verify: recompute over [header(0..20)]
        // seeded by the running CRC over the data, then FIN, and compare to the
        // stored xl_crc @ recpos+20.
        let stored_crc = u32::from_ne_bytes([
            *recpos.add(20), *recpos.add(21), *recpos.add(22), *recpos.add(23),
        ]);
        // Rebuild expected: seed = comp(INIT, data); then comp over header
        // bytes [0..20] AS WRITTEN (with xl_prev filled); FIN.
        let mut hdr20 = [0u8; 20];
        for i in 0..20 {
            hdr20[i] = *recpos.add(i);
        }
        let seed = crc32c::pg_comp_crc32c_sb8(0xFFFF_FFFF, &data);
        let expected_crc = crc32c::pg_comp_crc32c_sb8(seed, &hdr20) ^ 0xFFFF_FFFF;
        assert_eq!(stored_crc, expected_crc, "xl_crc");
    }

    // Second insert advances CurrBytePos / PrevBytePos correctly.
    let data2: Vec<u8> = (100..120u8).collect();
    let rec2 = build_record(10, 0, &data2);
    let end2 = XLogInsertRecord(&[&rec2[..]], InvalidXLogRecPtr, 0, 0, false)
        .expect("second XLogInsertRecord");
    assert!(end2 > end, "second record ends after the first");
    assert_eq!(insert::proc_last_rec_ptr(), end); // new record starts at prior end
}
