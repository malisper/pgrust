//! Cross-process attach of a *grown* (on-demand, index >= 1) DSA segment.
//!
//! This is the regression guard for the parallel keystone: a forked worker must
//! be able to read bytes the leader wrote into a DSA segment that the leader
//! created (via `dsa_allocate` -> `make_new_segment` -> `dsm_create`) AFTER the
//! worker forked.
//!
//! The mechanism under test:
//!
//!   * The leader publishes each new segment's `dsm_handle` into the DSA
//!     control region's shared `segment_handles[]` array (in the initial DSM
//!     segment, which is real `MAP_SHARED` POSIX shm).
//!   * The worker `dsa_attach`es the area (attaching the initial segment by
//!     handle), then `dsa_get_address` on a pointer into segment index >= 1
//!     drives `get_segment_by_index`, which reads the published handle from the
//!     shared control region and `dsm_attach`es that on-demand segment.
//!
//! We use a real `fork(2)` so the attach genuinely crosses processes.

use std::sync::Mutex;

use ::dsm_core::dsm::{
    dsm_attach, dsm_create, dsm_pin_segment, dsm_reset_backend_local_segment_list_for_fork,
    dsm_segment_address, dsm_segment_handle,
};
use ::dsm_core::test_bringup::dsm_test_bringup;
use ::lwlock::LWLockRegisterTranche;
use ::mmgr_dsa::runtime::{
    dsa_allocate_extended, dsa_attach, dsa_attach_in_place, dsa_create_in_place_ext,
    dsa_get_address, dsa_get_handle,
};
use ::mmgr_dsa::{
    DsaPointer, DSA_DEFAULT_INIT_SEGMENT_SIZE, DSA_MAX_SEGMENT_SIZE, DSA_MIN_SEGMENT_SIZE,
    DSA_OFFSET_WIDTH,
};

static TEST_LOCK: Mutex<()> = Mutex::new(());

/// The byte pattern the leader writes into the grown segment and the worker
/// reads back. Includes a non-ASCII tail so a partial / wrong mapping is caught.
const PATTERN: &[u8] = b"DSA grown-segment cross-process \xDE\xAD\xBE\xEF\x00\x7F";

/// Tranche id for the area's control + pool LWLocks. Any registered tranche
/// works; the locks are uncontended in this test.
const TEST_TRANCHE_ID: i32 = 4242;

fn segment_index_of(dp: DsaPointer) -> u64 {
    dp >> DSA_OFFSET_WIDTH
}

#[test]
fn worker_reads_leader_bytes_from_on_demand_segment() {
    let _g = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let mcx = dsm_test_bringup();
    LWLockRegisterTranche(mcx, TEST_TRANCHE_ID, "dsa-cross-process-test")
        .expect("register tranche");

    // Small initial segment so a couple of large allocations force a new
    // segment (index >= 1). Allow growth up to a few segments.
    let area = ::mmgr_dsa::runtime::dsa_create_ext(
        TEST_TRANCHE_ID,
        DSA_MIN_SEGMENT_SIZE,
        DSA_MIN_SEGMENT_SIZE,
        mcx,
    )
    .expect("dsa_create_ext");

    let handle = dsa_get_handle(area);

    // Allocate large objects until one lands in segment index >= 1. Each large
    // alloc consumes most of a 256 kB segment, so the second/third spills.
    let alloc_size: usize = 96 * 1024;
    let mut grown_dp: DsaPointer = 0;
    for _ in 0..8 {
        let dp = dsa_allocate_extended(area, alloc_size, 0, mcx).expect("dsa_allocate_extended");
        if segment_index_of(dp) >= 1 {
            grown_dp = dp;
            break;
        }
    }
    assert!(
        grown_dp != 0,
        "no allocation landed in an on-demand segment (index >= 1)"
    );
    assert!(
        segment_index_of(grown_dp) >= 1,
        "expected grown segment index >= 1, got {}",
        segment_index_of(grown_dp)
    );

    // Write the pattern into the grown segment through the leader's mapping.
    let leader_addr = dsa_get_address(area, grown_dp, mcx).expect("leader dsa_get_address");
    unsafe {
        std::ptr::copy_nonoverlapping(PATTERN.as_ptr(), leader_addr as *mut u8, PATTERN.len());
    }

    // Make sure the writes hit the shared backing store before the child reads.
    std::sync::atomic::fence(std::sync::atomic::Ordering::SeqCst);

    // Fork a child that plays the role of a fresh parallel worker.
    let pid = unsafe { libc::fork() };
    assert!(pid >= 0, "fork failed");

    if pid == 0 {
        // ---- child (worker) ----
        // A fresh forked backend has an empty backend-local segment list.
        dsm_reset_backend_local_segment_list_for_fork();

        let code = (|| -> i32 {
            // Attach the area by handle: dsm_attach the initial segment, read
            // its shared control region.
            let child_area = match dsa_attach(handle, mcx) {
                Ok(a) => a,
                Err(_) => return 11,
            };
            // Resolve a pointer into the grown (index >= 1) segment: drives
            // get_segment_by_index -> reads published handle -> dsm_attach.
            let child_addr = match dsa_get_address(child_area, grown_dp, mcx) {
                Ok(a) => a,
                Err(_) => return 12,
            };
            let seen =
                unsafe { std::slice::from_raw_parts(child_addr as *const u8, PATTERN.len()) };
            if seen == PATTERN {
                0
            } else {
                13
            }
        })();

        // Skip Rust/atexit teardown in the child; it would tear down the
        // shared control segment the parent still owns.
        unsafe { libc::_exit(code) };
    }

    // ---- parent (leader) ----
    let mut status: libc::c_int = 0;
    let waited = unsafe { libc::waitpid(pid, &mut status, 0) };
    assert_eq!(waited, pid, "waitpid failed");
    assert!(
        libc::WIFEXITED(status),
        "child did not exit normally (status {status:#x})"
    );
    let code = libc::WEXITSTATUS(status);
    assert_eq!(
        code, 0,
        "child failed: code {code} (11=attach, 12=get_address/segment-attach, 13=bytes mismatch)"
    );
}

/// The same cross-process grown-segment read-back, but through the EXACT
/// production parallel-query path: `dsa_create_in_place_ext` (the area's
/// segment-0 control region lives in a chunk of an already-shared DSM segment,
/// like the parallel DSM segment) on the leader side, and `dsa_attach_in_place`
/// on the worker side. This is the path that backs `es_query_dsa`, which gates
/// shared Parallel Hash Join and parallel Bitmap Heap Scan.
#[test]
fn worker_in_place_reads_leader_bytes_from_on_demand_segment() {
    let _g = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let mcx = dsm_test_bringup();
    LWLockRegisterTranche(mcx, TEST_TRANCHE_ID, "dsa-cross-process-test")
        .expect("register tranche");

    // The leader places the DSA area's control region inside a real, shared DSM
    // segment (standing in for the parallel-query DSM segment). The worker will
    // attach this segment by handle and then `dsa_attach_in_place` on its base.
    const PLACE_SEG_SIZE: usize = 1024 * 1024;
    let place_seg = dsm_create(PLACE_SEG_SIZE, 0, mcx)
        .expect("dsm_create place")
        .expect("dsm_create place None");
    let place_seg_id = place_seg.into_id();
    dsm_pin_segment(place_seg_id).expect("pin place seg");
    let place_handle = dsm_segment_handle(place_seg_id);
    let place_addr = dsm_segment_address(place_seg_id) as u64;

    let area = dsa_create_in_place_ext(
        place_addr,
        ::mmgr_dsa::runtime::dsa_minimum_size(),
        TEST_TRANCHE_ID,
        Some(place_seg_id),
        DSA_DEFAULT_INIT_SEGMENT_SIZE,
        DSA_MAX_SEGMENT_SIZE,
        mcx,
    )
    .expect("dsa_create_in_place_ext");

    // Force growth into an on-demand segment (index >= 1). Init segment is 1 MB.
    let alloc_size: usize = 256 * 1024;
    let mut grown_dp: DsaPointer = 0;
    for _ in 0..16 {
        let dp = dsa_allocate_extended(area, alloc_size, 0, mcx).expect("dsa_allocate_extended");
        if segment_index_of(dp) >= 1 {
            grown_dp = dp;
            break;
        }
    }
    assert!(grown_dp != 0, "no allocation landed in an on-demand segment");
    assert!(segment_index_of(grown_dp) >= 1);

    let leader_addr = dsa_get_address(area, grown_dp, mcx).expect("leader dsa_get_address");
    unsafe {
        std::ptr::copy_nonoverlapping(PATTERN.as_ptr(), leader_addr as *mut u8, PATTERN.len());
    }
    std::sync::atomic::fence(std::sync::atomic::Ordering::SeqCst);

    let pid = unsafe { libc::fork() };
    assert!(pid >= 0, "fork failed");

    if pid == 0 {
        std::panic::set_hook(Box::new(|info| {
            let msg = format!("CHILD PANIC: {info}\n");
            unsafe {
                libc::write(2, msg.as_ptr() as *const _, msg.len());
            }
        }));
        dsm_reset_backend_local_segment_list_for_fork();
        let code = (|| -> i32 {
            // Worker attaches the shared place-segment, then attaches the DSA
            // area in place on its base — the production `dsa_attach_in_place`.
            let seg = match dsm_attach(place_handle, mcx) {
                Ok(Some(s)) => s,
                _ => return 21,
            };
            let base = dsm_segment_address(seg.id()) as u64;
            let child_area = match dsa_attach_in_place(base, Some(seg.into_id()), mcx) {
                Ok(a) => a,
                Err(_) => return 22,
            };
            // Resolve the grown-segment pointer: get_segment_by_index reads the
            // leader-published handle from the shared control region and
            // dsm_attaches the on-demand segment.
            let child_addr = match dsa_get_address(child_area, grown_dp, mcx) {
                Ok(a) => a,
                Err(_) => return 23,
            };
            let seen =
                unsafe { std::slice::from_raw_parts(child_addr as *const u8, PATTERN.len()) };
            if seen == PATTERN {
                0
            } else {
                24
            }
        })();
        unsafe { libc::_exit(code) };
    }

    let mut status: libc::c_int = 0;
    let waited = unsafe { libc::waitpid(pid, &mut status, 0) };
    assert_eq!(waited, pid, "waitpid failed");
    assert!(libc::WIFEXITED(status), "child crashed (status {status:#x})");
    let code = libc::WEXITSTATUS(status);
    assert_eq!(
        code, 0,
        "in-place worker failed: code {code} \
         (21=dsm_attach place, 22=dsa_attach_in_place, 23=grown-segment attach, 24=bytes mismatch)"
    );
}
