use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard, Once};

use types_error::WARNING;
use types_storage::PGShmemHeader;

use crate::dsm::*;
use crate::dsm_impl::*;

static TEST_LOCK: Mutex<()> = Mutex::new(());
static REGISTERED_EXITS: AtomicUsize = AtomicUsize::new(0);
static EXIT_CALLBACKS: Mutex<Vec<(fn(i32, usize), usize)>> = Mutex::new(Vec::new());

fn bringup() -> MutexGuard<'static, ()> {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        shmem_seams::shmem_alloc::set(|size| {
            let layout = std::alloc::Layout::from_size_align(size, 128).unwrap();
            let p = unsafe { std::alloc::alloc_zeroed(layout) };
            assert!(!p.is_null());
            Ok(p)
        });
        shmem_seams::add_size::set(|a, b| Ok(a.checked_add(b).unwrap()));
        shmem_seams::mul_size::set(|a, b| Ok(a.checked_mul(b).unwrap()));
        ipc_seams::on_shmem_exit::set(|cb, arg| {
            REGISTERED_EXITS.fetch_add(1, Ordering::Relaxed);
            EXIT_CALLBACKS.lock().unwrap().push((cb, arg));
        });
        // splitmix64 stand-in until port/pg_prng lands.
        pg_prng_seams::global_prng_uint32::set(|| {
            static STATE: AtomicUsize = AtomicUsize::new(0x5851_f42d);
            let mut x = STATE.fetch_add(0x9e37_79b9, Ordering::Relaxed) as u64;
            x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            (x ^ (x >> 31)) as u32
        });
        lwlock::CreateLWLocks(false).unwrap();
        init_small::globals::SetMaxBackends(1);
        let shim = Box::leak(Box::new(PGShmemHeader {
            magic: 0,
            creatorPID: 0,
            totalsize: 0,
            freeoffset: 0,
            dsm_control: 0,
            index: std::ptr::null_mut(),
            device: 0,
            inode: 0,
        }));
        dsm_postmaster_startup(shim).unwrap();
        assert_eq!(REGISTERED_EXITS.load(Ordering::Relaxed), 1);
        assert_ne!(shim.dsm_control, 0);
        assert_eq!(shim.dsm_control & 1, 0);
    });
    TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

#[test]
fn create_maps_zeroed_and_destroys_on_last_detach() {
    let _g = bringup();
    let seg = dsm_create(1024, 0).unwrap().unwrap();
    let id = seg.id();
    let handle = dsm_segment_handle(id);
    assert_ne!(handle, 0);
    assert_eq!(handle & 1, 0);
    assert_eq!(dsm_segment_map_length(id), 1024);
    let p = dsm_segment_address(id);
    assert!(!p.is_null());
    let bytes = unsafe { std::slice::from_raw_parts(p, 1024) };
    assert!(bytes.iter().all(|&b| b == 0));
    assert_eq!(dsm_find_mapping(handle), Some(id));

    dsm_detach(seg.into_id()).unwrap();
    assert_eq!(dsm_find_mapping(handle), None);
    assert!(dsm_attach(handle).unwrap().is_none());
}

#[test]
fn attach_shares_memory_across_backends() {
    let _g = bringup();
    let seg = dsm_create(64, 0).unwrap().unwrap();
    let id = seg.id();
    let handle = dsm_segment_handle(id);
    unsafe { *dsm_segment_address(id) = 0xAB };

    dsm_pin_segment(id).unwrap();
    let observed = std::thread::spawn(move || {
        let seg = dsm_attach(handle).unwrap().unwrap();
        let p = dsm_segment_address(seg.id());
        let seen = unsafe { *p };
        unsafe { *p.add(1) = 0xCD };
        assert_eq!(dsm_segment_map_length(seg.id()), 64);
        dsm_detach(seg.into_id()).unwrap();
        seen
    })
    .join()
    .unwrap();
    assert_eq!(observed, 0xAB);
    assert_eq!(unsafe { *dsm_segment_address(id).add(1) }, 0xCD);

    drop(seg);
    assert!(dsm_attach(handle).unwrap().is_some_and(|s| {
        dsm_detach(s.into_id()).unwrap();
        true
    }));
    dsm_unpin_segment(handle).unwrap();
    assert!(dsm_attach(handle).unwrap().is_none());
}

#[test]
fn cannot_attach_same_segment_twice() {
    let _g = bringup();
    let seg = dsm_create(32, 0).unwrap().unwrap();
    let handle = dsm_segment_handle(seg.id());
    let err = dsm_attach(handle).unwrap_err();
    assert_eq!(err.message, "can't attach the same segment more than once");
}

#[test]
fn guard_drop_detaches() {
    let _g = bringup();
    let seg = dsm_create(32, 0).unwrap().unwrap();
    let handle = dsm_segment_handle(seg.id());
    drop(seg);
    assert_eq!(dsm_find_mapping(handle), None);
    assert!(dsm_attach(handle).unwrap().is_none());
}

#[test]
fn pin_mapping_outlives_guard_and_unpin_mapping_restores_it() {
    let _g = bringup();
    let seg = dsm_create(32, 0).unwrap().unwrap();
    let handle = dsm_segment_handle(seg.id());
    let id = dsm_pin_mapping(seg);
    assert_eq!(dsm_find_mapping(handle), Some(id));

    let seg = dsm_unpin_mapping(id);
    drop(seg);
    assert_eq!(dsm_find_mapping(handle), None);
    assert!(dsm_attach(handle).unwrap().is_none());
}

#[test]
fn pin_segment_lifecycle_and_errors() {
    let _g = bringup();
    let seg = dsm_create(32, 0).unwrap().unwrap();
    let id = seg.id();
    let handle = dsm_segment_handle(id);

    dsm_pin_segment(id).unwrap();
    let err = dsm_pin_segment(id).unwrap_err();
    assert_eq!(err.message, "cannot pin a segment that is already pinned");

    drop(seg);
    let seg2 = dsm_attach(handle).unwrap().unwrap();
    drop(seg2);

    dsm_unpin_segment(handle).unwrap();
    assert!(dsm_attach(handle).unwrap().is_none());

    let err = dsm_unpin_segment(handle).unwrap_err();
    assert_eq!(err.message, "cannot unpin unknown segment handle");

    let seg3 = dsm_create(32, 0).unwrap().unwrap();
    let err = dsm_unpin_segment(dsm_segment_handle(seg3.id())).unwrap_err();
    assert_eq!(err.message, "cannot unpin a segment that is not pinned");
}

static CB_TRACE: Mutex<Vec<usize>> = Mutex::new(Vec::new());

fn trace_cb(_seg: DsmSegmentId, arg: usize) -> types_error::PgResult<()> {
    CB_TRACE.lock().unwrap().push(arg);
    Ok(())
}

fn err_cb(_seg: DsmSegmentId, arg: usize) -> types_error::PgResult<()> {
    CB_TRACE.lock().unwrap().push(arg);
    Err(Box::new(types_error::PgError::error("detach callback failed")))
}

#[test]
fn detach_callbacks_run_lifo_and_cancel_removes() {
    let _g = bringup();
    CB_TRACE.lock().unwrap().clear();
    let seg = dsm_create(32, 0).unwrap().unwrap();
    let id = seg.id();
    on_dsm_detach(id, trace_cb, 1).unwrap();
    on_dsm_detach(id, trace_cb, 2).unwrap();
    on_dsm_detach(id, trace_cb, 3).unwrap();
    cancel_on_dsm_detach(id, trace_cb, 2);
    dsm_detach(seg.into_id()).unwrap();
    assert_eq!(*CB_TRACE.lock().unwrap(), vec![3, 1]);
}

#[test]
fn erroring_callback_leaves_rest_for_retry() {
    let _g = bringup();
    CB_TRACE.lock().unwrap().clear();
    let seg = dsm_create(32, 0).unwrap().unwrap();
    let id = dsm_pin_mapping(seg);
    on_dsm_detach(id, trace_cb, 1).unwrap();
    on_dsm_detach(id, err_cb, 2).unwrap();
    assert!(dsm_detach(id).is_err());
    assert_eq!(*CB_TRACE.lock().unwrap(), vec![2]);
    dsm_detach(id).unwrap();
    assert_eq!(*CB_TRACE.lock().unwrap(), vec![2, 1]);
    init_small::globals::ResumeInterrupts();
}

#[test]
fn reset_on_dsm_detach_forgets_callbacks_and_slots() {
    let _g = bringup();
    CB_TRACE.lock().unwrap().clear();
    let seg = dsm_create(32, 0).unwrap().unwrap();
    let id = seg.id();
    let handle = dsm_segment_handle(id);
    on_dsm_detach(id, trace_cb, 7).unwrap();
    reset_on_dsm_detach();
    dsm_detach(seg.into_id()).unwrap();
    assert!(CB_TRACE.lock().unwrap().is_empty());
    // Refcount was not decremented, so the segment is still attachable.
    let seg2 = dsm_attach(handle).unwrap().unwrap();
    dsm_detach(seg2.into_id()).unwrap();
}

#[test]
fn backend_shutdown_detaches_everything() {
    let _g = bringup();
    let a = dsm_pin_mapping(dsm_create(32, 0).unwrap().unwrap());
    let b = dsm_pin_mapping(dsm_create(32, 0).unwrap().unwrap());
    let (ha, hb) = (dsm_segment_handle(a), dsm_segment_handle(b));
    dsm_backend_shutdown().unwrap();
    assert_eq!(dsm_find_mapping(ha), None);
    assert_eq!(dsm_find_mapping(hb), None);
    assert!(dsm_attach(ha).unwrap().is_none());
    assert!(dsm_attach(hb).unwrap().is_none());
}

#[test]
fn create_reports_max_segments() {
    let _g = bringup();
    let mut guards = Vec::new();
    let mut hit_none = false;
    for _ in 0..200 {
        match dsm_create(16, DSM_CREATE_NULL_IF_MAXSEGMENTS).unwrap() {
            Some(seg) => guards.push(seg),
            None => {
                hit_none = true;
                break;
            }
        }
    }
    assert!(hit_none, "control segment never filled");
    let err = dsm_create(16, 0).unwrap_err();
    assert_eq!(err.message, "too many dynamic shared memory segments");
    drop(guards);
    let seg = dsm_create(16, 0).unwrap().unwrap();
    drop(seg);
}

#[test]
fn crash_cycle_recreates_control_segment() {
    let _g = bringup();
    // shmem_exit(1)'s dsm arm: the exit callback registered at boot startup.
    let (shutdown, arg) = EXIT_CALLBACKS.lock().unwrap()[0];
    let exits_before = REGISTERED_EXITS.load(Ordering::Relaxed);
    shutdown(1, arg);

    dsm_postmaster_startup_after_crash().unwrap();
    assert_eq!(REGISTERED_EXITS.load(Ordering::Relaxed), exits_before + 1);
    let shim = unsafe { &*(arg as *const PGShmemHeader) };
    assert_ne!(shim.dsm_control, 0);

    let seg = dsm_create(48, 0).unwrap().unwrap();
    let handle = dsm_segment_handle(seg.id());
    assert_ne!(handle, 0);
    dsm_detach(seg.into_id()).unwrap();
}

#[test]
fn cleanup_using_control_segment_is_quiet_on_missing() {
    let _g = bringup();
    dsm_cleanup_using_control_segment(0x7fff_fffe).unwrap();
}

#[test]
fn estimate_size_and_shmem_init_zero() {
    let _g = bringup();
    assert_eq!(dsm_estimate_size(), 0);
    dsm_shmem_init().unwrap();
    set_min_dynamic_shared_memory(3);
    assert_eq!(dsm_estimate_size(), 3 * 1024 * 1024);
    set_min_dynamic_shared_memory(0);
}

#[test]
fn impl_op_collision_and_missing_semantics() {
    let _g = bringup();
    let handle = 0x6000_0000;
    let mut ma = std::ptr::null_mut();
    let mut ms = 0usize;
    assert!(dsm_impl_op(DsmOp::Create, handle, 128, &mut ma, &mut ms, WARNING).unwrap());
    let mut ma2 = std::ptr::null_mut();
    let mut ms2 = 0usize;
    assert!(!dsm_impl_op(DsmOp::Create, handle, 128, &mut ma2, &mut ms2, WARNING).unwrap());
    assert!(dsm_impl_op(DsmOp::Destroy, handle, 0, &mut ma, &mut ms, WARNING).unwrap());
    assert!(!dsm_impl_op(DsmOp::Destroy, handle, 0, &mut ma, &mut ms, WARNING).unwrap());
    assert!(!dsm_impl_op(DsmOp::Attach, handle, 0, &mut ma, &mut ms, WARNING).unwrap());
}

#[test]
fn guc_defaults() {
    assert_eq!(dynamic_shared_memory_type(), DSM_IMPL_POSIX);
    assert_eq!(min_dynamic_shared_memory(), 0);
    assert_eq!(DYNAMIC_SHARED_MEMORY_OPTIONS.len(), 3);
}
