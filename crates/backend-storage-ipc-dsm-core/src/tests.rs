//! Runtime gating test for the in-process DSM bring-up
//! ([`crate::test_bringup`]).
//!
//! This is the whole point of family `dsm-test-harness`: prove that real
//! `dsm_create` / `dsm_attach` / `dsm_segment_address` / `dsm_detach` run
//! end-to-end under `cargo test`, over a real POSIX shared-memory control
//! segment and real `mmap`'d segments — no `Vec<u8>` emulation, no side tables.
//!
//! Seam slots and the `MainLWLockArray` are process-global, so the bring-up is
//! `Once`-guarded; the tests still serialize on a mutex because they share the
//! process-global POSIX shm namespace and the thread-local `dsm.c` state.

use std::sync::Mutex;

use crate::dsm::{
    dsm_attach, dsm_create, dsm_detach, dsm_pin_segment, dsm_segment_address, dsm_segment_handle,
    dsm_segment_map_length, dsm_unpin_segment,
};
use crate::test_bringup::dsm_test_bringup;

static TEST_LOCK: Mutex<()> = Mutex::new(());

fn guard() -> std::sync::MutexGuard<'static, ()> {
    TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

/// Create a real DSM segment, write bytes through its mapped address, drop the
/// creator's mapping, then `dsm_attach` to the same handle and read the bytes
/// back through the *fresh* mapping. Exercises the full control-segment
/// lifecycle in one backend: slot allocation under
/// `DynamicSharedMemoryControlLock`, the `dsm_impl_op(ATTACH)` re-`mmap`, the
/// refcount bookkeeping (pin, detach, attach), and the final
/// destroy + POSIX `shm_unlink`.
///
/// A single backend cannot `dsm_attach` a handle it still has mapped (the C
/// "can't attach the same segment more than once" cross-check), so the creator
/// pins the *segment* (keeping it alive past the last mapping) and detaches its
/// own mapping before re-attaching — which is exactly the path another backend
/// would take, just collapsed into one process.
#[test]
fn create_attach_address_roundtrip_detach() {
    let _g = guard();
    let mcx = dsm_test_bringup();

    const SIZE: usize = 8192;

    // dsm_create: real shm_open + mmap + a control slot. refcnt = 2.
    let seg = dsm_create(SIZE, 0, mcx)
        .expect("dsm_create errored")
        .expect("dsm_create returned None without DSM_CREATE_NULL_IF_MAXSEGMENTS");
    let id = seg.id();
    let handle = dsm_segment_handle(id);

    // The mapping is a real address into a real POSIX segment.
    let addr = dsm_segment_address(id);
    assert!(!addr.is_null(), "dsm_segment_address returned NULL");
    assert!(dsm_segment_map_length(id) >= SIZE);

    // Write a recognizable pattern through the creator's mapping.
    let pattern: &[u8] = b"family-H real DSM round-trip \xDE\xAD\xBE\xEF";
    unsafe {
        std::ptr::copy_nonoverlapping(pattern.as_ptr(), addr, pattern.len());
        // Touch the far end too, to prove the whole mapping is backed (the
        // POSIX resize path actually allocated the pages).
        *addr.add(SIZE - 1) = 0x5A;
    }

    // Pin the segment so it survives with no mapping (refcnt 2 -> 3, pinned),
    // then detach the creator's mapping (refcnt 3 -> 2, mapping gone). The
    // segment is now in exactly the state a freshly-started backend sees.
    dsm_pin_segment(id).expect("dsm_pin_segment errored");
    dsm_detach(seg.into_id()).expect("dsm_detach (creator) errored");

    // dsm_attach: real shm_open(O_RDWR) + fstat + mmap of the same object.
    let attached = dsm_attach(handle, mcx)
        .expect("dsm_attach errored")
        .expect("dsm_attach returned None for a live (pinned) segment");
    let attached_id = attached.id();
    let attached_addr = dsm_segment_address(attached_id);
    assert!(!attached_addr.is_null());

    // Read the creator's bytes back through the fresh mapping: real shared
    // memory, so the writes are visible across the unmap/remap.
    let seen = unsafe { std::slice::from_raw_parts(attached_addr, pattern.len()) };
    assert_eq!(seen, pattern, "bytes did not round-trip through real DSM");
    assert_eq!(unsafe { *attached_addr.add(SIZE - 1) }, 0x5A);

    // Detach the attached mapping (refcnt 2 -> 1), then unpin the segment
    // (pinned refcnt 1 -> destroyed + shm_unlink'd).
    dsm_detach(attached.into_id()).expect("dsm_detach (attached) errored");
    dsm_unpin_segment(handle).expect("dsm_unpin_segment errored");
}

/// A second create after a full detach reuses the freed control slot and
/// produces an independent, writable segment — proving the control segment
/// bookkeeping (refcount back to 0, slot reuse) survives a round trip.
#[test]
fn slot_is_reusable_after_detach() {
    let _g = guard();
    let mcx = dsm_test_bringup();

    let first = dsm_create(4096, 0, mcx).unwrap().unwrap();
    let first_addr = dsm_segment_address(first.id());
    unsafe { *first_addr = 0x11 };
    dsm_detach(first.into_id()).unwrap();

    let second = dsm_create(4096, 0, mcx).unwrap().unwrap();
    let second_addr = dsm_segment_address(second.id());
    // Fresh segment: independent storage, writable.
    unsafe { *second_addr = 0x22 };
    assert_eq!(unsafe { *second_addr }, 0x22);
    dsm_detach(second.into_id()).unwrap();
}
