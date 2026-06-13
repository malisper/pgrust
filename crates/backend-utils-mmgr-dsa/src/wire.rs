//! Wiring: back the `backend-utils-mmgr-dsa-seams` declarations (the parallel
//! executor's DSA boundary) onto the real [`crate::runtime`] allocator.
//!
//! The seam vocabulary (`types_execparallel`) is the frozen shared boundary the
//! already-merged `backend-executor-execParallel` consumes; the adapters here
//! bridge it to the runtime's [`runtime::DsaAreaHandle`] + `u64` `DsaPointer` +
//! [`backend_storage_ipc_dsm_core::DsmSegmentId`].
//!
//! The seams are infallible and carry no `Mcx`; the allocator's true surface is
//! fallible and allocates DSM segments. The adapters therefore source the DSA
//! subsystem's process-lifetime top context (the "long-lived root owned by the
//! process entry point" of `docs/query-lifecycle-raii.md`) and convert a
//! runtime `Err` (C `ereport(ERROR)` on these parallel-query paths) into a
//! panic at the boundary, since the seam cannot return it.

use std::cell::Cell;

use backend_storage_ipc_dsm_core::dsm::DsmSegmentId;
use backend_utils_mmgr_dsa_seams as seam;
use mcx::{Mcx, MemoryContext};
use types_dsa::{DSA_DEFAULT_INIT_SEGMENT_SIZE, DSA_MAX_SEGMENT_SIZE};
use types_execparallel::{
    DsaAreaHandle as XDsaAreaHandle, DsaPointer, DsmSegmentHandle, SerializeCursor, Size,
};

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
