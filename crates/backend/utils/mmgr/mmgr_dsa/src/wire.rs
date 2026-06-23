//! Wiring: back the `backend-utils-mmgr-dsa-seams` declarations (the parallel
//! executor's DSA boundary) onto the real [`crate::runtime`] allocator.
//!
//! The seam vocabulary (`execparallel`) is the frozen shared boundary the
//! already-merged `backend-executor-execParallel` consumes; the adapters here
//! bridge it to the runtime's [`runtime::DsaAreaHandle`] + `u64` `DsaPointer` +
//! [`::dsm_core::DsmSegmentId`].
//!
//! The seams are infallible and carry no `Mcx`; the allocator's true surface is
//! fallible and allocates DSM segments. The adapters therefore source the DSA
//! subsystem's process-lifetime top context (the "long-lived root owned by the
//! process entry point" of `docs/query-lifecycle-raii.md`) and convert a
//! runtime `Err` (C `ereport(ERROR)` on these parallel-query paths) into a
//! panic at the boundary, since the seam cannot return it.

use std::cell::Cell;

use ::dsm_core::dsm::DsmSegmentId;
use dsa_seams as seam;
use ::mcx::{Mcx, MemoryContext};
use ::types_dsa::{DsaHandle, DSA_DEFAULT_INIT_SEGMENT_SIZE, DSA_MAX_SEGMENT_SIZE};
use ::execparallel::{
    DsaAreaHandle as XDsaAreaHandle, DsaPointer, DsmSegmentHandle, SerializeCursor, Size,
};
use ::types_storage::{dsa_handle, DsaArea};

use crate::runtime::{self, DsaAreaHandle};

/// The DSA subsystem's process-lifetime top context, used to back the DSM
/// segment allocations the in-place parallel-query paths drive. C allocates the
/// `dsa_area` and its segments in `TopMemoryContext`; this leaked root is the
/// equivalent long-lived owner.
fn dsa_top_mcx() -> Mcx<'static> {
    // Per-backend (the DSA top context is backend-local state, not shared);
    // leaked for process lifetime so the `'static` mappings outlive any call.
    thread_local! {
        static TOP: Cell<Option<&'static MemoryContext>> = const { Cell::new(None) };
    }
    TOP.with(|t| {
        if let Some(cx) = t.get() {
            return cx.mcx();
        }
        let cx: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("dsa top")));
        t.set(Some(cx));
        cx.mcx()
    })
}

/// Convert a runtime result into a value at the infallible seam boundary,
/// panicking on the C `ereport(ERROR)` the seam cannot surface.
#[inline]
fn expect<T>(r: types_error::PgResult<T>) -> T {
    r.unwrap_or_else(|e| panic!("dsa: {}", e.message()))
}

#[inline]
fn to_runtime_handle(area: XDsaAreaHandle) -> DsaAreaHandle {
    DsaAreaHandle::from_u64(area.0 as u64)
}

#[inline]
fn from_runtime_handle(area: DsaAreaHandle) -> XDsaAreaHandle {
    XDsaAreaHandle(area.as_u64() as usize)
}

/// The DSM-registry / dshash seam group crosses the boundary with the opaque
/// `*mut DsaArea` the C code holds (`dsa_area *`). It is never dereferenced by
/// the consumers — the registry stores it and passes it back, dshash holds the
/// one the registry handed it — so we carry the runtime [`DsaAreaHandle`]'s
/// `u64` inside the pointer value (the same opacity-preserving token the
/// parallel group carries as a `usize`). Decode reverses it.
#[inline]
fn ptr_to_runtime_handle(area: *mut DsaArea) -> DsaAreaHandle {
    DsaAreaHandle::from_u64(area as usize as u64)
}

#[inline]
fn runtime_handle_to_ptr(area: DsaAreaHandle) -> *mut DsaArea {
    area.as_u64() as usize as *mut DsaArea
}

/// Install every seam in `backend-utils-mmgr-dsa-seams`.
pub fn install_dsa_seams() {
    // `dsa_minimum_size()`.
    seam::dsa_minimum_size::set(runtime::dsa_minimum_size);

    // `dsa_create_in_place(place, size, tranche_id, segment)` — the C macro that
    // expands to `dsa_create_in_place_ext` with default init/max segment sizes.
    seam::dsa_create_in_place::set(
        |place: SerializeCursor, size: Size, tranche_id: i32, segment: DsmSegmentHandle| {
            let seg = dsm_segment_handle_to_id(segment);
            let area = expect(runtime::dsa_create_in_place_ext(
                place.0 as u64,
                size,
                tranche_id,
                seg,
                DSA_DEFAULT_INIT_SEGMENT_SIZE,
                DSA_MAX_SEGMENT_SIZE,
                dsa_top_mcx(),
            ));
            from_runtime_handle(area)
        },
    );

    // `dsa_attach_in_place(place, segment)`.
    seam::dsa_attach_in_place::set(|place: SerializeCursor, segment: DsmSegmentHandle| {
        let seg = dsm_segment_handle_to_id(segment);
        let area = expect(runtime::dsa_attach_in_place(place.0 as u64, seg, dsa_top_mcx()));
        from_runtime_handle(area)
    });

    // `dsa_detach(area)`.
    seam::dsa_detach::set(|area: XDsaAreaHandle| {
        expect(runtime::dsa_detach(to_runtime_handle(area)));
    });

    // `dsa_allocate(area, size)` == `dsa_allocate_extended(area, size, 0)`.
    seam::dsa_allocate::set(|area: XDsaAreaHandle, size: Size| {
        expect(runtime::dsa_allocate_extended(
            to_runtime_handle(area),
            size,
            0,
            dsa_top_mcx(),
        ))
    });

    // `dsa_free(area, dp)`.
    seam::dsa_free::set(|area: XDsaAreaHandle, dp: DsaPointer| {
        expect(runtime::dsa_free(to_runtime_handle(area), dp, dsa_top_mcx()));
    });

    // `dsa_get_address(area, dp)` — a cursor over the addressed bytes (the C
    // `void *` resolved address, carried as a `usize` cursor).
    seam::dsa_get_address::set(|area: XDsaAreaHandle, dp: DsaPointer| {
        let addr = expect(runtime::dsa_get_address(to_runtime_handle(area), dp, dsa_top_mcx()));
        SerializeCursor(addr as usize)
    });

    // --- DSM-registry DSA seams (consumer: backend-storage-ipc-dsm-registry) ---
    //
    // These cross the boundary with the opaque `*mut DsaArea`; the runtime
    // surface is fallible and carries the `ereport(ERROR)` channel, so the
    // adapters return the `PgResult` straight through (no boundary panic).

    // `dsa_create(tranche_id)` — the C macro for `dsa_create_ext` with the
    // default init/max segment sizes.
    seam::dsa_create::set(|tranche_id: i32| {
        let area = runtime::dsa_create_ext(
            tranche_id,
            DSA_DEFAULT_INIT_SEGMENT_SIZE,
            DSA_MAX_SEGMENT_SIZE,
            dsa_top_mcx(),
        )?;
        Ok(runtime_handle_to_ptr(area))
    });

    // `dsa_attach(handle)`.
    seam::dsa_attach::set(|handle: dsa_handle| {
        let area = runtime::dsa_attach(handle as DsaHandle, dsa_top_mcx())?;
        Ok(runtime_handle_to_ptr(area))
    });

    // `dsa_create_ext(tranche_id, init_segment_size, max_segment_size)`.
    seam::dsa_create_ext::set(
        |tranche_id: i32, init_segment_size: Size, max_segment_size: Size| {
            let area = runtime::dsa_create_ext(
                tranche_id,
                init_segment_size,
                max_segment_size,
                dsa_top_mcx(),
            )?;
            Ok(runtime_handle_to_ptr(area))
        },
    );

    // `dsa_pin(area)`.
    seam::dsa_pin::set(|area: *mut DsaArea| runtime::dsa_pin(ptr_to_runtime_handle(area)));

    // `dsa_pin_mapping(area)` — infallible in the runtime; the seam carries a
    // `PgResult<()>` for the C `ereport(ERROR)` on the remember-mapping alloc.
    seam::dsa_pin_mapping::set(|area: *mut DsaArea| {
        runtime::dsa_pin_mapping(ptr_to_runtime_handle(area));
        Ok(())
    });

    // `dsa_get_handle(area)`.
    seam::dsa_get_handle::set(|area: *mut DsaArea| {
        runtime::dsa_get_handle(ptr_to_runtime_handle(area)) as dsa_handle
    });

    // --- `dsa_area *`-keyed allocation/addressing (consumer: backend-lib-dshash) ---

    // `dsa_allocate_extended(area, size, flags)`.
    seam::dsa_allocate_extended::set(|area: *mut DsaArea, size: Size, flags: i32| {
        runtime::dsa_allocate_extended(ptr_to_runtime_handle(area), size, flags, dsa_top_mcx())
    });

    // `dsa_free(area, dp)`.
    seam::dsa_free_ptr::set(|area: *mut DsaArea, dp: DsaPointer| {
        runtime::dsa_free(ptr_to_runtime_handle(area), dp, dsa_top_mcx())
    });

    // `dsa_get_address(area, dp)` — the backend-local address as the `u64` it
    // resolves to.
    seam::dsa_get_address_ptr::set(|area: *mut DsaArea, dp: DsaPointer| {
        runtime::dsa_get_address(ptr_to_runtime_handle(area), dp, dsa_top_mcx())
    });

    // `dsa_detach(area)` (pointer-keyed).
    seam::dsa_detach_ptr::set(|area: *mut DsaArea| {
        runtime::dsa_detach(ptr_to_runtime_handle(area))
    });

    // `dsa_get_total_size(area)` (pointer-keyed).
    seam::dsa_get_total_size_ptr::set(|area: *mut DsaArea| {
        runtime::dsa_get_total_size(ptr_to_runtime_handle(area))
    });
}

/// Map the execParallel `DsmSegmentHandle` token onto a dsm-core
/// [`DsmSegmentId`]. The handle carries the segment's `u64` id; `0` is C's
/// `NULL` segment (no containing DSM segment).
#[inline]
fn dsm_segment_handle_to_id(segment: DsmSegmentHandle) -> Option<DsmSegmentId> {
    if segment.0 == 0 {
        None
    } else {
        Some(DsmSegmentId::from_u64(segment.0 as u64))
    }
}
