//! The DSA allocator core — a faithful port of the runtime half of
//! `backend/utils/mmgr/dsa.c`, resident **in place** in the real `MAP_SHARED`
//! DSM segment.
//!
//! Every bookkeeping object — [`DsaAreaControl`], [`DsaSegmentHeader`],
//! [`DsaAreaSpan`], [`DsaAreaPool`], the per-segment `FreePageManager`, and the
//! page map — lives at a computed byte offset inside a DSM segment, reached
//! through the segment's resolved backend-local base. A [`DsaPointer`] is a
//! `(segment_index, offset)` pair; [`dsa_get_address`] resolves it.
//!
//! The embedded `FreePageManager` and `LWLock` are operated through the real
//! freepage / lwlock seams (`*mut FreePageManager` / `&LWLock`).

#![allow(non_snake_case)]

use core::cell::RefCell;
use core::mem::size_of;

use dsm_core::dsm::{self, DsmSegmentId};
use lwlock_seams as lwlock_seams;
use init_small_seams::my_proc_number;
use freepage_seams as fpm_seams;
use types_core::Size;
// `Datum` here is the transitional bare-word shim type (`datum::Datum`),
// retained ONLY at the audited DSM-cursor ABI edge this crate touches: the
// `on_dsm_detach` / `on_shmem_exit` callback registry in the still-unmigrated
// `backend-storage-ipc-dsm-core` whose `OnDsmDetachCallback` / `PgOnExitCallback`
// type aliases fix the callback arg as `datum::Datum`. The arg carries a
// raw control-base machine word (C `PointerGetDatum(place)` / `DatumGetPointer`),
// not a typed SQL value, and a process-lifetime callback fn-pointer cannot carry
// the canonical `Datum<'mcx>`'s borrow. So these two hooks and their marshaling
// helpers stay fully-qualified `datum::Datum` at the edge, rather than
// constructing a canonical `Datum<'mcx>`.
use types_dsa::{
    DsaHandle, DsaPointer, DsaSegmentIndex, DSA_ALLOC_NO_OOM, DSA_ALLOC_ZERO, DSA_FULLNESS_CLASSES,
    DSA_HANDLE_INVALID, DSA_MAX_SEGMENTS, DSA_MAX_SEGMENT_SIZE, DSA_MIN_SEGMENT_SIZE,
    DSA_NUM_SEGMENTS_AT_EACH_SIZE, DSA_NUM_SEGMENT_BINS, DSA_OFFSET_BITMASK, DSA_OFFSET_WIDTH,
    DSA_PAGES_PER_SUPERBLOCK, DSA_SCLASS_BLOCK_OF_SPANS, DSA_SCLASS_SPAN_LARGE,
    DSA_SEGMENT_HEADER_MAGIC, DSA_SEGMENT_INDEX_NONE, DSA_SIZE_CLASSES, DSA_SPAN_NOTHING_FREE,
    INVALID_DSA_POINTER,
};
use types_error::{PgError, PgResult};
use types_freepage::FreePageManager;
use types_storage::{dsm_handle, LWLock, LWLockMode, DSM_HANDLE_INVALID};

use crate::{
    contiguous_pages_to_segment_bin, dsa_size_class_index, fpm_size_to_pages, make_pointer,
    validate_alloc_request,
};

/// `FPM_PAGE_SIZE` (freepage.h) — 4 kB pages.
pub const FPM_PAGE_SIZE: Size = types_freepage::FPM_PAGE_SIZE;

/// `DSA_NUM_SIZE_CLASSES = lengthof(dsa_size_classes)`.
const DSA_NUM_SIZE_CLASSES: usize = DSA_SIZE_CLASSES.len();

/// `DSA_SUPERBLOCK_SIZE` (`DSA_PAGES_PER_SUPERBLOCK * FPM_PAGE_SIZE`).
const DSA_SUPERBLOCK_SIZE: Size = DSA_PAGES_PER_SUPERBLOCK * FPM_PAGE_SIZE;

/// `MAXIMUM_ALIGNOF` (c.h) — 8 on supported 64-bit platforms.
const MAXIMUM_ALIGNOF: Size = 8;

/// `MAXALIGN(x)` — round `x` up to the next `MAXIMUM_ALIGNOF` boundary.
#[inline]
const fn maxalign(x: Size) -> Size {
    (x + (MAXIMUM_ALIGNOF - 1)) & !(MAXIMUM_ALIGNOF - 1)
}

/// `LW_EXCLUSIVE` — the only mode dsa.c acquires its locks in.
const LW_EXCLUSIVE: LWLockMode = LWLockMode::LW_EXCLUSIVE;

/// The `elog(FATAL, ...)` / backend-bug `elog(ERROR, ...)` paths in the
/// allocator core fire only on internal inconsistency (a free-page manager that
/// promised but failed to deliver pages, or a double-free / use-after-free
/// reaching `get_segment_by_index`). FATAL kills the process in C; the faithful
/// loud analogue here is a panic.
#[cold]
#[inline(never)]
fn dsa_fatal(msg: &str) -> ! {
    panic!("dsa: {msg}");
}

// ---------------------------------------------------------------------------
// In-segment `repr(C)` structs — crate-local mirrors of dsa.c's file-private
// structs, placed at computed byte offsets inside the segment. The embedded
// `FreePageManager` and `LWLock` are the real shared-memory types, operated
// through the freepage / lwlock seams.
// ---------------------------------------------------------------------------

/// The per-segment header (`dsa_segment_header`). When this is the first segment
/// it appears as the first field of [`DsaAreaControl`].
#[repr(C)]
pub struct DsaSegmentHeader {
    pub magic: u32,
    pub usable_pages: Size,
    pub size: Size,
    pub prev: DsaSegmentIndex,
    pub next: DsaSegmentIndex,
    pub bin: Size,
    pub freed: bool,
}

/// One superblock's metadata (`dsa_area_span`).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct DsaAreaSpan {
    pub pool: DsaPointer,
    pub prevspan: DsaPointer,
    pub nextspan: DsaPointer,
    pub start: DsaPointer,
    pub npages: Size,
    pub size_class: u16,
    pub ninitialized: u16,
    pub nallocatable: u16,
    pub firstfree: u16,
    pub nmax: u16,
    pub fclass: u16,
}

// `dsa_size_classes[0]` is the literal `sizeof(dsa_area_span)` (56 on LP64); a
// span must fit in the block-of-spans object that size class carves.
const _: () = assert!(size_of::<DsaAreaSpan>() <= DSA_SIZE_CLASSES[0] as usize);

/// The per-size-class allocator pool (`dsa_area_pool`).
#[repr(C)]
pub struct DsaAreaPool {
    /// A lock protecting access to this pool.
    pub lock: LWLock,
    /// A set of linked lists of spans, arranged by fullness.
    pub spans: [DsaPointer; DSA_FULLNESS_CLASSES],
}

/// The control block for an area (`dsa_area_control`). Lives at the start of the
/// first DSM segment controlled by this area.
#[repr(C)]
pub struct DsaAreaControl {
    pub segment_header: DsaSegmentHeader,
    pub handle: DsaHandle,
    pub segment_handles: [dsm_handle; DSA_MAX_SEGMENTS],
    pub segment_bins: [DsaSegmentIndex; DSA_NUM_SEGMENT_BINS],
    pub pools: [DsaAreaPool; DSA_NUM_SIZE_CLASSES],
    pub init_segment_size: Size,
    pub max_segment_size: Size,
    pub total_segment_size: Size,
    pub max_total_segment_size: Size,
    pub high_segment_index: DsaSegmentIndex,
    pub refcnt: i32,
    pub pinned: bool,
    pub freed_segment_counter: Size,
    pub lwlock_tranche_id: i32,
    /// The general lock (protects everything except object pools).
    pub lock: LWLock,
}

// ---------------------------------------------------------------------------
// Metadata-offset helpers (the `MAXALIGN(...)` offsets dsa.c computes). dsa.c
// uses `sizeof(dsa_area_control)` for the FIRST segment and
// `sizeof(dsa_segment_header)` for later segments to place the FPM, but the
// pagemap then follows `MAXALIGN(sizeof(FreePageManager))`. The first-segment
// layout differs because the control struct embeds the segment header.
// ---------------------------------------------------------------------------

#[inline]
fn first_segment_fpm_offset() -> Size {
    maxalign(size_of::<DsaAreaControl>())
}
#[inline]
fn first_segment_pagemap_offset() -> Size {
    first_segment_fpm_offset() + maxalign(size_of::<FreePageManager>())
}
#[inline]
fn segment_fpm_offset() -> Size {
    maxalign(size_of::<DsaSegmentHeader>())
}
#[inline]
fn segment_pagemap_offset() -> Size {
    segment_fpm_offset() + maxalign(size_of::<FreePageManager>())
}

// ---------------------------------------------------------------------------
// `dsa_area` — per-backend state, held in a thread-local registry.
// ---------------------------------------------------------------------------

/// `dsa_segment_map` — backend-local mapping of one segment.
#[derive(Clone, Copy)]
struct DsaSegmentMap {
    /// The DSM segment id, or `None` (the C `NULL`) for an unmapped / in-place
    /// control segment.
    segment: Option<DsmSegmentId>,
    /// Mapped base address (`*mut u8 as u64`), or `0` when unmapped.
    mapped_address: u64,
    fpm_offset: Size,
    pagemap_offset: Size,
}

impl DsaSegmentMap {
    const fn zero() -> Self {
        Self {
            segment: None,
            mapped_address: 0,
            fpm_offset: 0,
            pagemap_offset: 0,
        }
    }
}

/// `struct dsa_area` — per-backend state for a storage area.
struct DsaArea {
    /// Backend-local address of `dsa_area_control` (`area->control`).
    control_base: u64,
    /// Whether this attachment is owned by the current resource owner (`true`,
    /// the C `resowner != NULL`) or has session lifespan after
    /// `dsa_pin_mapping`.
    resowner_set: bool,
    segment_maps: Box<[DsaSegmentMap; DSA_MAX_SEGMENTS]>,
    high_segment_index: DsaSegmentIndex,
    freed_segment_counter: Size,
}

/// An opaque backend-local handle for a [`DsaArea`] (the analogue of `dsa_area
/// *`). Resolves to the area in the thread-local registry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DsaAreaHandle(u64);

impl DsaAreaHandle {
    #[inline]
    pub fn as_u64(self) -> u64 {
        self.0
    }
    #[inline]
    pub fn from_u64(v: u64) -> Self {
        DsaAreaHandle(v)
    }
}

struct DsaState {
    areas: Vec<Option<DsaArea>>,
}

std::thread_local! {
    static DSA_STATE: RefCell<DsaState> = const { RefCell::new(DsaState { areas: Vec::new() }) };
}

fn register_area(area: DsaArea) -> DsaAreaHandle {
    DSA_STATE.with(|s| {
        let mut st = s.borrow_mut();
        // A 1-based slot index is the handle token; detach leaves `None`.
        st.areas.push(Some(area));
        DsaAreaHandle(st.areas.len() as u64)
    })
}

fn with_area<R>(h: DsaAreaHandle, f: impl FnOnce(&mut DsaArea) -> R) -> R {
    DSA_STATE.with(|s| {
        let mut st = s.borrow_mut();
        let idx = (h.0 as usize)
            .checked_sub(1)
            .expect("invalid dsa_area handle");
        let area = st.areas[idx].as_mut().expect("dsa_area has been detached");
        f(area)
    })
}

// ---------------------------------------------------------------------------
// Raw in-segment field access (the substrate exception).
// ---------------------------------------------------------------------------

#[inline]
unsafe fn control_at<'a>(base: u64) -> &'a mut DsaAreaControl {
    &mut *(base as usize as *mut DsaAreaControl)
}

#[inline]
unsafe fn header_at<'a>(base: u64) -> &'a mut DsaSegmentHeader {
    &mut *(base as usize as *mut DsaSegmentHeader)
}

#[inline]
unsafe fn span_at<'a>(addr: u64) -> &'a mut DsaAreaSpan {
    &mut *(addr as usize as *mut DsaAreaSpan)
}

/// `&control->lock`.
#[inline]
unsafe fn control_lock<'a>(base: u64) -> &'a LWLock {
    &control_at(base).lock
}
/// `&control->pools[sclass].lock`.
#[inline]
unsafe fn pool_lock<'a>(base: u64, sclass: usize) -> &'a LWLock {
    &control_at(base).pools[sclass].lock
}

/// Read a `dsa_pointer` from the page map.
#[inline]
unsafe fn pagemap_get(base: u64, pagemap_offset: Size, pageno: Size) -> DsaPointer {
    let p = (base as usize as *const u8).add(pagemap_offset) as *const DsaPointer;
    core::ptr::read(p.add(pageno))
}

#[inline]
unsafe fn pagemap_set(base: u64, pagemap_offset: Size, pageno: Size, v: DsaPointer) {
    let p = (base as usize as *mut u8).add(pagemap_offset) as *mut DsaPointer;
    core::ptr::write(p.add(pageno), v);
}

/// `NextFreeObjectIndex(object)` — the first 2 bytes of a free object hold the
/// index of the next free object.
#[inline]
unsafe fn next_free_object_index_get(base: u64, offset: Size) -> u16 {
    core::ptr::read((base as usize as *const u8).add(offset) as *const u16)
}
#[inline]
unsafe fn next_free_object_index_set(base: u64, offset: Size, v: u16) {
    core::ptr::write((base as usize as *mut u8).add(offset) as *mut u16, v);
}

/// `*mut FreePageManager` at `seg_base + fpm_offset`.
#[inline]
fn fpm_ptr(seg_base: u64, fpm_offset: Size) -> *mut FreePageManager {
    (seg_base as usize + fpm_offset) as *mut FreePageManager
}

// ---------------------------------------------------------------------------
// In-segment FreePageManager + LWLock — routed through the real seams.
// ---------------------------------------------------------------------------

#[inline]
fn fpm_initialize(seg_base: u64, fpm_offset: Size) {
    fpm_seams::free_page_manager_initialize::call(
        fpm_ptr(seg_base, fpm_offset),
        seg_base as usize as *mut u8,
    );
}
#[inline]
fn fpm_get(seg_base: u64, fpm_offset: Size, npages: Size) -> Option<Size> {
    fpm_seams::free_page_manager_get::call(fpm_ptr(seg_base, fpm_offset), npages)
}
#[inline]
fn fpm_put(seg_base: u64, fpm_offset: Size, first_page: Size, npages: Size) -> PgResult<()> {
    fpm_seams::free_page_manager_put::call(fpm_ptr(seg_base, fpm_offset), first_page, npages)
}
/// `fpm_largest(fpm)` is the macro `fpm->contiguous_pages` — a direct field read
/// off the shared-memory `FreePageManager` the freepage owner maintains.
#[inline]
fn fpm_largest(seg_base: u64, fpm_offset: Size) -> Size {
    unsafe { (*fpm_ptr(seg_base, fpm_offset)).contiguous_pages }
}

/// `LWLockAcquire(lock, LW_EXCLUSIVE)` — returns the held guard. A guard's
/// `Drop` is the error/abort release; the C `LWLockRelease` call sites consume
/// it via [`lwlock_seams::LWLockGuard::release`].
#[inline]
fn lwlock_acquire(lock: &LWLock) -> PgResult<lwlock_seams::LWLockGuard<'_>> {
    // C's LWLockAcquire reads MyProc/MyProcNumber ambiently; here the no-ambient
    // rule has the lwlock seam take the backend's proc number explicitly.
    lwlock_seams::lwlock_acquire::call(lock, LW_EXCLUSIVE, my_proc_number::call())
}

// ---------------------------------------------------------------------------
// dsa_get_address.
// ---------------------------------------------------------------------------

/// `dsa_get_address(area, dp)` — backend-local address for a `dsa_pointer`.
/// Returns `0` for [`INVALID_DSA_POINTER`] (the C `NULL`).
pub fn dsa_get_address(area: DsaAreaHandle, dp: DsaPointer, mcx: mcx::Mcx<'static>) -> PgResult<u64> {
    if dp == INVALID_DSA_POINTER {
        return Ok(0);
    }

    check_for_freed_segments(area, mcx)?;

    let index = (dp >> DSA_OFFSET_WIDTH) as DsaSegmentIndex;
    let offset = (dp & DSA_OFFSET_BITMASK) as Size;
    debug_assert!(index < DSA_MAX_SEGMENTS);

    let mapped = with_area(area, |a| a.segment_maps[index].mapped_address);
    if mapped == 0 {
        // Call for effect: cause the segment to be mapped in.
        get_segment_by_index(area, index, mcx)?;
    }

    Ok(with_area(area, |a| a.segment_maps[index].mapped_address) + offset as u64)
}

// ---------------------------------------------------------------------------
// Lifecycle: create / attach.
// ---------------------------------------------------------------------------

/// `dsa_create_ext(tranche_id, init_segment_size, max_segment_size)`.
pub fn dsa_create_ext(
    tranche_id: i32,
    init_segment_size: Size,
    max_segment_size: Size,
    mcx: mcx::Mcx<'static>,
) -> PgResult<DsaAreaHandle> {
    let segment = dsm::dsm_create(init_segment_size, 0, mcx)?.ok_or_else(|| {
        PgError::error("dsa_create_ext: dsm_create returned NULL for a non-NO_OOM request")
    })?;
    let seg_id = segment.into_id();

    // All segments backing this area are pinned.
    dsm::dsm_pin_segment(seg_id)?;

    let place = dsm::dsm_segment_address(seg_id) as u64;
    let control_handle = dsm::dsm_segment_handle(seg_id);

    let area = create_internal(
        place,
        init_segment_size,
        tranche_id,
        control_handle,
        Some(seg_id),
        init_segment_size,
        max_segment_size,
    )?;

    dsm::on_dsm_detach(seg_id, dsa_on_dsm_detach_release_in_place, pointer_get_datum(place), mcx)?;

    Ok(area)
}

/// `dsa_create_in_place_ext(place, size, tranche_id, segment, init, max)`.
pub fn dsa_create_in_place_ext(
    place: u64,
    size: Size,
    tranche_id: i32,
    segment: Option<DsmSegmentId>,
    init_segment_size: Size,
    max_segment_size: Size,
    mcx: mcx::Mcx<'static>,
) -> PgResult<DsaAreaHandle> {
    let area = create_internal(
        place,
        size,
        tranche_id,
        DSM_HANDLE_INVALID,
        None,
        init_segment_size,
        max_segment_size,
    )?;

    if let Some(seg) = segment {
        dsm::on_dsm_detach(seg, dsa_on_dsm_detach_release_in_place, pointer_get_datum(place), mcx)?;
    }

    Ok(area)
}

/// `dsa_get_handle(area)`.
pub fn dsa_get_handle(area: DsaAreaHandle) -> DsaHandle {
    with_area(area, |a| {
        let h = unsafe { control_at(a.control_base).handle };
        debug_assert_ne!(h, DSA_HANDLE_INVALID);
        h
    })
}

/// `dsa_attach(handle)`.
pub fn dsa_attach(handle: DsaHandle, mcx: mcx::Mcx<'static>) -> PgResult<DsaAreaHandle> {
    let segment = match dsm::dsm_attach(handle, mcx)? {
        Some(s) => s.into_id(),
        None => return Err(PgError::error("could not attach to dynamic shared area")),
    };

    let place = dsm::dsm_segment_address(segment) as u64;
    let area = attach_internal(place, Some(segment), handle)?;

    dsm::on_dsm_detach(segment, dsa_on_dsm_detach_release_in_place, pointer_get_datum(place), mcx)?;

    Ok(area)
}

/// `dsa_attach_in_place(place, segment)`.
pub fn dsa_attach_in_place(
    place: u64,
    segment: Option<DsmSegmentId>,
    mcx: mcx::Mcx<'static>,
) -> PgResult<DsaAreaHandle> {
    let area = attach_internal(place, None, DSA_HANDLE_INVALID)?;

    if let Some(seg) = segment {
        dsm::on_dsm_detach(seg, dsa_on_dsm_detach_release_in_place, pointer_get_datum(place), mcx)?;
    }

    Ok(area)
}

/// `dsa_on_dsm_detach_release_in_place(segment, place)` — the detach hook.
pub fn dsa_on_dsm_detach_release_in_place(
    _segment: DsmSegmentId,
    place: datum::Datum,
) -> PgResult<()> {
    dsa_release_in_place(datum_get_pointer(place))
}

/// `dsa_on_shmem_exit_release_in_place(code, place)` — the on_shmem_exit hook.
/// `code` is ignored; `place` carries the control base as a `Datum`.
pub fn dsa_on_shmem_exit_release_in_place(_code: i32, place: datum::Datum) -> PgResult<()> {
    dsa_release_in_place(datum_get_pointer(place))
}

/// `dsa_release_in_place(place)` — drop a reference; if the last, unpin every
/// segment. `place` is the control base.
pub fn dsa_release_in_place(place: u64) -> PgResult<()> {
    let guard = lwlock_acquire(unsafe { control_lock(place) })?;
    let control = unsafe { control_at(place) };
    debug_assert_eq!(
        control.segment_header.magic,
        DSA_SEGMENT_HEADER_MAGIC ^ control.handle ^ 0
    );
    debug_assert!(control.refcnt > 0);
    control.refcnt -= 1;
    if control.refcnt == 0 {
        let hi = control.high_segment_index;
        let mut i: DsaSegmentIndex = 0;
        while i <= hi {
            let handle = control.segment_handles[i];
            if handle != DSM_HANDLE_INVALID {
                dsm::dsm_unpin_segment(handle)?;
            }
            i += 1;
        }
    }
    guard.release()
}

/// `dsa_pin_mapping(area)` — keep this area attached until end of session.
///
/// C clears `area->resowner` (so the area is no longer detached at owner
/// release) and calls `dsm_pin_mapping(segment)` on every mapped segment, which
/// hands the DSM mapping off from the resource owner to session lifetime. The
/// dsa registry here stores segment *ids* (`into_id()`), which already detach
/// the dsm mapping from any resource-owner guard, so the mappings are already
/// session-lifetime; clearing `resowner_set` records that the area itself is
/// likewise no longer owner-scoped. Nothing further to pin per segment.
pub fn dsa_pin_mapping(area: DsaAreaHandle) {
    with_area(area, |a| {
        if a.resowner_set {
            a.resowner_set = false;
        }
    });
}

/// `dsa_pin(area)`.
pub fn dsa_pin(area: DsaAreaHandle) -> PgResult<()> {
    let base = with_area(area, |a| a.control_base);
    let guard = lwlock_acquire(unsafe { control_lock(base) })?;
    let control = unsafe { control_at(base) };
    if control.pinned {
        guard.release()?;
        return Err(PgError::error("dsa_area already pinned"));
    }
    control.pinned = true;
    control.refcnt += 1;
    guard.release()
}

/// `dsa_unpin(area)`.
pub fn dsa_unpin(area: DsaAreaHandle) -> PgResult<()> {
    let base = with_area(area, |a| a.control_base);
    let guard = lwlock_acquire(unsafe { control_lock(base) })?;
    let control = unsafe { control_at(base) };
    debug_assert!(control.refcnt > 1);
    if !control.pinned {
        guard.release()?;
        return Err(PgError::error("dsa_area not pinned"));
    }
    control.pinned = false;
    control.refcnt -= 1;
    guard.release()
}

/// `dsa_set_size_limit(area, limit)`.
pub fn dsa_set_size_limit(area: DsaAreaHandle, limit: Size) -> PgResult<()> {
    let base = with_area(area, |a| a.control_base);
    let guard = lwlock_acquire(unsafe { control_lock(base) })?;
    unsafe { control_at(base).max_total_segment_size = limit };
    guard.release()
}

/// `dsa_get_total_size(area)`.
pub fn dsa_get_total_size(area: DsaAreaHandle) -> PgResult<Size> {
    let base = with_area(area, |a| a.control_base);
    let guard = lwlock_acquire(unsafe { control_lock(base) })?;
    let size = unsafe { control_at(base).total_segment_size };
    guard.release()?;
    Ok(size)
}

/// `dsa_detach(area)` — detach this backend from all the area's segments and
/// free the backend-local state.
pub fn dsa_detach(area: DsaAreaHandle) -> PgResult<()> {
    let segs: Vec<Option<DsmSegmentId>> = with_area(area, |a| {
        let hi = a.high_segment_index;
        (0..=hi).map(|i| a.segment_maps[i].segment).collect()
    });
    for seg in segs.into_iter().flatten() {
        dsm::dsm_detach(seg)?;
    }

    // Free the backend-local area object (pfree(area)).
    DSA_STATE.with(|s| {
        let mut st = s.borrow_mut();
        let idx = (area.0 as usize) - 1;
        st.areas[idx] = None;
    });
    Ok(())
}

/// `dsa_minimum_size()` — smallest size acceptable to `dsa_create_in_place`.
pub fn dsa_minimum_size() -> Size {
    let mut size = maxalign(size_of::<DsaAreaControl>()) + maxalign(size_of::<FreePageManager>());
    let mut pages: Size = 0;
    while (size + FPM_PAGE_SIZE - 1) / FPM_PAGE_SIZE > pages {
        pages += 1;
        size += size_of::<DsaPointer>();
    }
    pages * FPM_PAGE_SIZE
}

// ---------------------------------------------------------------------------
// create_internal / attach_internal.
// ---------------------------------------------------------------------------

fn new_area(control_base: u64) -> DsaArea {
    DsaArea {
        control_base,
        resowner_set: true, // CurrentResourceOwner
        segment_maps: Box::new([DsaSegmentMap::zero(); DSA_MAX_SEGMENTS]),
        high_segment_index: 0,
        freed_segment_counter: 0,
    }
}

fn create_internal(
    place: u64,
    size: Size,
    tranche_id: i32,
    control_handle: dsm_handle,
    control_segment: Option<DsmSegmentId>,
    init_segment_size: Size,
    max_segment_size: Size,
) -> PgResult<DsaAreaHandle> {
    debug_assert!(init_segment_size >= DSA_MIN_SEGMENT_SIZE);
    debug_assert!(max_segment_size >= init_segment_size);
    debug_assert!(max_segment_size <= DSA_MAX_SEGMENT_SIZE);

    if size < dsa_minimum_size() {
        return Err(PgError::error(format!(
            "dsa_area space must be at least {}, but {} provided",
            dsa_minimum_size(),
            size
        )));
    }

    let total_pages = size / FPM_PAGE_SIZE;
    let mut metadata_bytes = maxalign(size_of::<DsaAreaControl>())
        + maxalign(size_of::<FreePageManager>())
        + total_pages * size_of::<DsaPointer>();
    if metadata_bytes % FPM_PAGE_SIZE != 0 {
        metadata_bytes += FPM_PAGE_SIZE - (metadata_bytes % FPM_PAGE_SIZE);
    }
    debug_assert!(metadata_bytes <= size);
    let usable_pages = (size - metadata_bytes) / FPM_PAGE_SIZE;

    // Initialize the dsa_area_control object at the start of the space.
    unsafe {
        core::ptr::write_bytes(place as usize as *mut u8, 0, size_of::<DsaAreaControl>());
        let control = control_at(place);
        control.segment_header.magic = DSA_SEGMENT_HEADER_MAGIC ^ control_handle ^ 0;
        control.segment_header.next = DSA_SEGMENT_INDEX_NONE;
        control.segment_header.prev = DSA_SEGMENT_INDEX_NONE;
        control.segment_header.usable_pages = usable_pages;
        control.segment_header.freed = false;
        control.segment_header.size = size;
        control.handle = control_handle;
        control.init_segment_size = init_segment_size;
        control.max_segment_size = max_segment_size;
        control.max_total_segment_size = Size::MAX; // (size_t) -1
        control.total_segment_size = size;
        control.segment_handles[0] = control_handle;
        for i in 0..DSA_NUM_SEGMENT_BINS {
            control.segment_bins[i] = DSA_SEGMENT_INDEX_NONE;
        }
        control.refcnt = 1;
        control.lwlock_tranche_id = tranche_id;
    }

    let mut area = new_area(place);

    // LWLockInitialize(&control->lock, tranche) + each pool lock.
    unsafe {
        let control = control_at(place);
        lwlock_seams::lwlock_initialize::call(&mut control.lock, tranche_id);
        for i in 0..DSA_NUM_SIZE_CLASSES {
            lwlock_seams::lwlock_initialize::call(&mut control.pools[i].lock, tranche_id);
        }
    }

    // Set up the segment map for this process's mapping (segment 0).
    let fpm_offset = first_segment_fpm_offset();
    let pagemap_offset = first_segment_pagemap_offset();
    area.segment_maps[0] = DsaSegmentMap {
        segment: control_segment,
        mapped_address: place,
        fpm_offset,
        pagemap_offset,
    };

    // Set up the free page map.
    fpm_initialize(place, fpm_offset);
    if usable_pages > 0 {
        fpm_put(place, fpm_offset, metadata_bytes / FPM_PAGE_SIZE, usable_pages)?;
    }

    // Put this segment into the appropriate bin.
    let bin = contiguous_pages_to_segment_bin(usable_pages);
    unsafe {
        let control = control_at(place);
        control.segment_bins[bin] = 0;
        control.segment_header.bin = bin;
    }

    Ok(register_area(area))
}

fn attach_internal(
    place: u64,
    segment: Option<DsmSegmentId>,
    handle: DsaHandle,
) -> PgResult<DsaAreaHandle> {
    unsafe {
        let control = control_at(place);
        debug_assert_eq!(control.handle, handle);
        debug_assert_eq!(control.segment_handles[0], handle);
        debug_assert_eq!(
            control.segment_header.magic,
            DSA_SEGMENT_HEADER_MAGIC ^ handle ^ 0
        );
    }

    let mut area = new_area(place);

    let fpm_offset = first_segment_fpm_offset();
    let pagemap_offset = first_segment_pagemap_offset();
    area.segment_maps[0] = DsaSegmentMap {
        segment,
        mapped_address: place,
        fpm_offset,
        pagemap_offset,
    };

    // Bump the reference count.
    let guard = lwlock_acquire(unsafe { control_lock(place) })?;
    if unsafe { control_at(place).refcnt } == 0 {
        guard.release()?;
        return Err(PgError::error("could not attach to dynamic shared area"));
    }
    unsafe {
        control_at(place).refcnt += 1;
    }
    area.freed_segment_counter = unsafe { control_at(place).freed_segment_counter };
    guard.release()?;

    Ok(register_area(area))
}

// ---------------------------------------------------------------------------
// dsa_allocate_extended.
// ---------------------------------------------------------------------------

/// `dsa_allocate_extended(area, size, flags)`. Returns [`INVALID_DSA_POINTER`]
/// on a `DSA_ALLOC_NO_OOM` out-of-memory; otherwise `Err` on OOM.
pub fn dsa_allocate_extended(
    area: DsaAreaHandle,
    size: Size,
    flags: i32,
    mcx: mcx::Mcx<'static>,
) -> PgResult<DsaPointer> {
    debug_assert!(size > 0);

    validate_alloc_request(size, flags)?;

    let base = with_area(area, |a| a.control_base);

    // Large object path.
    if size > DSA_SIZE_CLASSES[DSA_SIZE_CLASSES.len() - 1] as Size {
        let npages = fpm_size_to_pages(size);

        // Obtain a span object.
        let span_pointer = alloc_object(area, DSA_SCLASS_BLOCK_OF_SPANS, mcx)?;
        if span_pointer == INVALID_DSA_POINTER {
            return oom(flags, size);
        }

        let area_guard = lwlock_acquire(unsafe { control_lock(base) })?;

        // Find a segment from which to allocate.
        let mut segment_index = get_best_segment(area, npages, mcx)?;
        if segment_index.is_none() {
            segment_index = make_new_segment(area, npages, mcx)?;
        }
        let segment_index = match segment_index {
            Some(s) => s,
            None => {
                area_guard.release()?;
                dsa_free(area, span_pointer, mcx)?;
                return oom(flags, size);
            }
        };

        let (seg_base, fpm_offset, pagemap_offset) = segment_map_parts(area, segment_index);
        let first_page = fpm_get(seg_base, fpm_offset, npages)
            .unwrap_or_else(|| dsa_fatal(&format!("dsa_allocate could not find {npages} free pages")));
        area_guard.release()?;

        let start_pointer = make_pointer(segment_index, first_page * FPM_PAGE_SIZE);

        // Initialize span and pagemap.
        let span_guard = lwlock_acquire(unsafe { pool_lock(base, DSA_SCLASS_SPAN_LARGE) })?;
        init_span(area, span_pointer, DSA_SCLASS_SPAN_LARGE, start_pointer, npages, DSA_SCLASS_SPAN_LARGE as u16, mcx)?;
        unsafe { pagemap_set(seg_base, pagemap_offset, first_page, span_pointer) };
        span_guard.release()?;

        if flags & DSA_ALLOC_ZERO != 0 {
            let addr = dsa_get_address(area, start_pointer, mcx)?;
            unsafe { core::ptr::write_bytes(addr as usize as *mut u8, 0, size) };
        }

        return Ok(start_pointer);
    }

    // Map allocation to a size class.
    let size_class = dsa_size_class_index(size);
    debug_assert!(size <= DSA_SIZE_CLASSES[size_class] as Size);
    debug_assert!(size_class == 0 || size > DSA_SIZE_CLASSES[size_class - 1] as Size);

    let result = alloc_object(area, size_class, mcx)?;
    if result == INVALID_DSA_POINTER {
        return oom(flags, size);
    }

    if flags & DSA_ALLOC_ZERO != 0 {
        let addr = dsa_get_address(area, result, mcx)?;
        unsafe { core::ptr::write_bytes(addr as usize as *mut u8, 0, size) };
    }

    Ok(result)
}

fn oom(flags: i32, size: Size) -> PgResult<DsaPointer> {
    if flags & DSA_ALLOC_NO_OOM == 0 {
        Err(PgError::error("out of memory")
            .with_sqlstate(types_error::ERRCODE_OUT_OF_MEMORY)
            .with_detail(format!("Failed on DSA request of size {size}.")))
    } else {
        Ok(INVALID_DSA_POINTER)
    }
}

// ---------------------------------------------------------------------------
// dsa_free.
// ---------------------------------------------------------------------------

/// `dsa_free(area, dp)`.
pub fn dsa_free(area: DsaAreaHandle, dp: DsaPointer, mcx: mcx::Mcx<'static>) -> PgResult<()> {
    check_for_freed_segments(area, mcx)?;

    let base = with_area(area, |a| a.control_base);

    let index = (dp >> DSA_OFFSET_WIDTH) as DsaSegmentIndex;
    let segment_index = get_segment_by_index(area, index, mcx)?;
    let (seg_base, _fpm_offset, pagemap_offset) = segment_map_parts(area, segment_index);
    let pageno = ((dp & DSA_OFFSET_BITMASK) as Size) / FPM_PAGE_SIZE;
    let span_pointer = unsafe { pagemap_get(seg_base, pagemap_offset, pageno) };

    let span_addr = dsa_get_address(area, span_pointer, mcx)?;
    let span = unsafe { span_at(span_addr) };
    let superblock = dsa_get_address(area, span.start, mcx)?;
    let object = dsa_get_address(area, dp, mcx)?;
    let size_class = span.size_class as usize;
    let size = DSA_SIZE_CLASSES[size_class] as Size;

    // Large object special case.
    if span.size_class as usize == DSA_SCLASS_SPAN_LARGE {
        let span_start_page = ((span.start & DSA_OFFSET_BITMASK) as Size) / FPM_PAGE_SIZE;
        let npages = span.npages;
        let (lseg_base, lfpm_offset, _) = segment_map_parts(area, segment_index);

        let area_guard = lwlock_acquire(unsafe { control_lock(base) })?;
        fpm_put(lseg_base, lfpm_offset, span_start_page, npages)?;
        rebin_segment(area, segment_index, mcx)?;
        area_guard.release()?;

        let span_guard = lwlock_acquire(unsafe { pool_lock(base, DSA_SCLASS_SPAN_LARGE) })?;
        unlink_span(area, span_pointer, mcx)?;
        span_guard.release()?;

        dsa_free(area, span_pointer, mcx)?;
        return Ok(());
    }

    let class_guard = lwlock_acquire(unsafe { pool_lock(base, size_class) })?;

    // Put the object on the span's freelist.
    debug_assert!(object >= superblock);
    debug_assert!(object < superblock + DSA_SUPERBLOCK_SIZE as u64);
    debug_assert_eq!((object - superblock) % size as u64, 0);
    let object_local_offset = (dp & DSA_OFFSET_BITMASK) as Size;
    unsafe { next_free_object_index_set(seg_base, object_local_offset, span.firstfree) };
    span.firstfree = ((object - superblock) / size as u64) as u16;
    span.nallocatable += 1;

    if span.nallocatable == 1 && span.fclass as usize == DSA_FULLNESS_CLASSES - 1 {
        unlink_span(area, span_pointer, mcx)?;
        add_span_to_fullness_class(area, span_pointer, DSA_FULLNESS_CLASSES - 2, mcx)?;
    } else if span.nallocatable == span.nmax
        && (span.fclass != 1 || span.prevspan != INVALID_DSA_POINTER)
    {
        destroy_superblock(area, span_pointer, mcx)?;
    }

    class_guard.release()
}

/// `dsa_trim(area)` — aggressively free all spare memory in the hope of
/// returning DSM segments to the operating system.
pub fn dsa_trim(area: DsaAreaHandle, mcx: mcx::Mcx<'static>) -> PgResult<()> {
    let base = with_area(area, |a| a.control_base);

    // Trim in reverse pool order so we get to the spans-of-spans last, just in
    // case any become entirely free while processing the other pools.
    let mut sc = (DSA_NUM_SIZE_CLASSES - 1) as isize;
    while sc >= 0 {
        let size_class = sc as usize;

        if size_class != DSA_SCLASS_SPAN_LARGE {
            // Large object frees give back segments aggressively already, so we
            // skip DSA_SCLASS_SPAN_LARGE. Search fullness class 1 only — that is
            // where an entirely empty superblock is expected (empty superblocks
            // in other fullness classes are returned to the free page map by
            // dsa_free).
            let class_guard = lwlock_acquire(unsafe { pool_lock(base, size_class) })?;
            let mut span_pointer = pool_spans_get(base, size_class, 1);
            while span_pointer != INVALID_DSA_POINTER {
                let span_addr = dsa_get_address(area, span_pointer, mcx)?;
                let (next, nallocatable, nmax) = {
                    let span = unsafe { span_at(span_addr) };
                    (span.nextspan, span.nallocatable, span.nmax)
                };
                if nallocatable == nmax {
                    destroy_superblock(area, span_pointer, mcx)?;
                }
                span_pointer = next;
            }
            class_guard.release()?;
        }

        sc -= 1;
    }
    Ok(())
}

/// `dsa_dump(area)` — print debugging information about the internal state of
/// the shared memory area. C writes to `stderr`; we mirror that with
/// `eprintln!`. Acquires and releases individual locks as it goes (an
/// inconsistent snapshot, exactly as the C does).
pub fn dsa_dump(area: DsaAreaHandle, mcx: mcx::Mcx<'static>) -> PgResult<()> {
    let base = with_area(area, |a| a.control_base);

    let area_guard = lwlock_acquire(unsafe { control_lock(base) })?;
    check_for_freed_segments_locked(area, mcx)?;
    let (handle, max_total, total, refcnt, pinned) = unsafe {
        let c = control_at(base);
        (c.handle, c.max_total_segment_size, c.total_segment_size, c.refcnt, c.pinned)
    };
    eprintln!("dsa_area handle {handle:x}:");
    eprintln!("  max_total_segment_size: {max_total}");
    eprintln!("  total_segment_size: {total}");
    eprintln!("  refcnt: {refcnt}");
    eprintln!("  pinned: {}", if pinned { 't' } else { 'f' });
    eprintln!("  segment bins:");
    for i in 0..DSA_NUM_SEGMENT_BINS {
        if unsafe { control_at(base).segment_bins[i] } != DSA_SEGMENT_INDEX_NONE {
            if i == 0 {
                eprintln!("    segment bin {i} (no contiguous free pages):");
            } else {
                eprintln!("    segment bin {i} (at least {} contiguous pages free):", 1 << (i - 1));
            }
            let mut segment_index = unsafe { control_at(base).segment_bins[i] };
            while segment_index != DSA_SEGMENT_INDEX_NONE {
                let sidx = get_segment_by_index(area, segment_index, mcx)?;
                let (sbase, sfpm, _) = segment_map_parts(area, sidx);
                let usable_pages = unsafe { header_at(sbase).usable_pages };
                let contiguous = fpm_largest(sbase, sfpm);
                eprintln!(
                    "      segment index {segment_index}, usable_pages = {usable_pages}, contiguous_pages = {contiguous}, mapped at {sbase:p}",
                    sbase = sbase as usize as *const u8
                );
                segment_index = unsafe { header_at(sbase).next };
            }
        }
    }
    area_guard.release()?;

    eprintln!("  pools:");
    for i in 0..DSA_NUM_SIZE_CLASSES {
        let mut found = false;
        let class_guard = lwlock_acquire(unsafe { pool_lock(base, i) })?;
        for j in 0..DSA_FULLNESS_CLASSES {
            if pool_spans_get(base, i, j) != INVALID_DSA_POINTER {
                found = true;
            }
        }
        if found {
            if i == DSA_SCLASS_BLOCK_OF_SPANS {
                eprintln!("    pool for blocks of span objects:");
            } else if i == DSA_SCLASS_SPAN_LARGE {
                eprintln!("    pool for large object spans:");
            } else {
                eprintln!("    pool for size class {i} (object size {} bytes):", DSA_SIZE_CLASSES[i]);
            }
            for j in 0..DSA_FULLNESS_CLASSES {
                if pool_spans_get(base, i, j) == INVALID_DSA_POINTER {
                    eprintln!("      fullness class {j} is empty");
                } else {
                    eprintln!("      fullness class {j}:");
                    let mut span_pointer = pool_spans_get(base, i, j);
                    while span_pointer != INVALID_DSA_POINTER {
                        let span_addr = dsa_get_address(area, span_pointer, mcx)?;
                        let span = unsafe { span_at(span_addr) };
                        eprintln!(
                            "        span descriptor at {span_pointer:016x}, superblock at {:016x}, pages = {}, objects free = {}/{}",
                            span.start, span.npages, span.nallocatable, span.nmax
                        );
                        span_pointer = span.nextspan;
                    }
                }
            }
        }
        class_guard.release()?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// init_span / transfer_first_span / alloc_object / ensure_active_superblock.
// ---------------------------------------------------------------------------

fn init_span(
    area: DsaAreaHandle,
    span_pointer: DsaPointer,
    pool_class: usize,
    start: DsaPointer,
    npages: Size,
    size_class: u16,
    mcx: mcx::Mcx<'static>,
) -> PgResult<()> {
    let base = with_area(area, |a| a.control_base);
    let span_addr = dsa_get_address(area, span_pointer, mcx)?;
    let span = unsafe { span_at(span_addr) };
    let obsize = DSA_SIZE_CLASSES[size_class as usize] as Size;

    // Push this span onto the front of the span list for fullness class 1.
    let pool_dp = dsa_area_pool_to_dsa_pointer(pool_class);
    let head_dp = pool_spans_get(base, pool_class, 1);
    if head_dp != INVALID_DSA_POINTER {
        let head_addr = dsa_get_address(area, head_dp, mcx)?;
        unsafe { span_at(head_addr).prevspan = span_pointer };
    }
    span.pool = pool_dp;
    span.nextspan = head_dp;
    span.prevspan = INVALID_DSA_POINTER;
    pool_spans_set(base, pool_class, 1, span_pointer);

    span.start = start;
    span.npages = npages;
    span.size_class = size_class;
    span.ninitialized = 0;
    if size_class as usize == DSA_SCLASS_BLOCK_OF_SPANS {
        span.ninitialized = 1;
        span.nallocatable = (FPM_PAGE_SIZE / obsize - 1) as u16;
    } else if size_class as usize != DSA_SCLASS_SPAN_LARGE {
        span.nallocatable = (DSA_SUPERBLOCK_SIZE / obsize) as u16;
    }
    span.firstfree = DSA_SPAN_NOTHING_FREE;
    span.nmax = span.nallocatable;
    span.fclass = 1;
    Ok(())
}

fn transfer_first_span(
    area: DsaAreaHandle,
    pool_class: usize,
    fromclass: usize,
    toclass: usize,
    mcx: mcx::Mcx<'static>,
) -> PgResult<bool> {
    let base = with_area(area, |a| a.control_base);
    let span_pointer = pool_spans_get(base, pool_class, fromclass);
    if span_pointer == INVALID_DSA_POINTER {
        return Ok(false);
    }

    let span_addr = dsa_get_address(area, span_pointer, mcx)?;
    let span = unsafe { span_at(span_addr) };
    pool_spans_set(base, pool_class, fromclass, span.nextspan);
    if span.nextspan != INVALID_DSA_POINTER {
        let na = dsa_get_address(area, span.nextspan, mcx)?;
        unsafe { span_at(na).prevspan = INVALID_DSA_POINTER };
    }

    let to_head = pool_spans_get(base, pool_class, toclass);
    span.nextspan = to_head;
    pool_spans_set(base, pool_class, toclass, span_pointer);
    if span.nextspan != INVALID_DSA_POINTER {
        let na = dsa_get_address(area, span.nextspan, mcx)?;
        unsafe { span_at(na).prevspan = span_pointer };
    }
    span.fclass = toclass as u16;

    Ok(true)
}

fn alloc_object(area: DsaAreaHandle, size_class: usize, mcx: mcx::Mcx<'static>) -> PgResult<DsaPointer> {
    let base = with_area(area, |a| a.control_base);

    let class_guard = lwlock_acquire(unsafe { pool_lock(base, size_class) })?;

    let result;
    if pool_spans_get(base, size_class, 1) == INVALID_DSA_POINTER
        && !ensure_active_superblock(area, size_class, mcx)?
    {
        result = INVALID_DSA_POINTER;
    } else {
        let active_dp = pool_spans_get(base, size_class, 1);
        debug_assert_ne!(active_dp, INVALID_DSA_POINTER);
        let span_addr = dsa_get_address(area, active_dp, mcx)?;
        let span = unsafe { span_at(span_addr) };
        debug_assert!(span.nallocatable > 0);
        let block = span.start;
        debug_assert!(size_class < DSA_NUM_SIZE_CLASSES);
        let size = DSA_SIZE_CLASSES[size_class] as DsaPointer;
        let r;
        if span.firstfree != DSA_SPAN_NOTHING_FREE {
            r = block + span.firstfree as DsaPointer * size;
            // object = dsa_get_address(area, r): read NextFreeObjectIndex.
            let r_index = (r >> DSA_OFFSET_WIDTH) as DsaSegmentIndex;
            let r_seg = get_segment_by_index(area, r_index, mcx)?;
            let (rseg_base, _, _) = segment_map_parts(area, r_seg);
            let r_local = (r & DSA_OFFSET_BITMASK) as Size;
            span.firstfree = unsafe { next_free_object_index_get(rseg_base, r_local) };
        } else {
            r = block + span.ninitialized as DsaPointer * size;
            span.ninitialized += 1;
        }
        span.nallocatable -= 1;

        if span.nallocatable == 0 {
            transfer_first_span(area, size_class, 1, DSA_FULLNESS_CLASSES - 1, mcx)?;
        }
        result = r;
    }

    class_guard.release()?;
    Ok(result)
}

fn ensure_active_superblock(area: DsaAreaHandle, size_class: usize, mcx: mcx::Mcx<'static>) -> PgResult<bool> {
    let base = with_area(area, |a| a.control_base);
    let obsize = DSA_SIZE_CLASSES[size_class] as Size;
    let mut npages: Size = 1;

    let nmax = if size_class == DSA_SCLASS_BLOCK_OF_SPANS {
        FPM_PAGE_SIZE / obsize - 1
    } else {
        DSA_SUPERBLOCK_SIZE / obsize
    };

    // Rebalance higher fullness classes into class 1 where appropriate.
    let mut fclass = 2;
    while fclass < DSA_FULLNESS_CLASSES - 1 {
        let mut span_pointer = pool_spans_get(base, size_class, fclass);

        while span_pointer != INVALID_DSA_POINTER {
            let span_addr = dsa_get_address(area, span_pointer, mcx)?;
            let span = unsafe { span_at(span_addr) };
            let next_span_pointer = span.nextspan;

            let tfclass = (nmax - span.nallocatable as Size) * (DSA_FULLNESS_CLASSES - 1) / nmax;

            if tfclass < fclass {
                // Remove from the current fullness class list.
                if pool_spans_get(base, size_class, fclass) == span_pointer {
                    debug_assert_eq!(span.prevspan, INVALID_DSA_POINTER);
                    pool_spans_set(base, size_class, fclass, span.nextspan);
                    if span.nextspan != INVALID_DSA_POINTER {
                        let na = dsa_get_address(area, span.nextspan, mcx)?;
                        unsafe { span_at(na).prevspan = INVALID_DSA_POINTER };
                    }
                } else {
                    debug_assert_ne!(span.prevspan, INVALID_DSA_POINTER);
                    let pa = dsa_get_address(area, span.prevspan, mcx)?;
                    unsafe { span_at(pa).nextspan = span.nextspan };
                }
                if span.nextspan != INVALID_DSA_POINTER {
                    let na = dsa_get_address(area, span.nextspan, mcx)?;
                    unsafe { span_at(na).prevspan = span.prevspan };
                }

                // Push onto the head of the new fullness class list.
                let new_head = pool_spans_get(base, size_class, tfclass);
                span.nextspan = new_head;
                pool_spans_set(base, size_class, tfclass, span_pointer);
                span.prevspan = INVALID_DSA_POINTER;
                if span.nextspan != INVALID_DSA_POINTER {
                    let na = dsa_get_address(area, span.nextspan, mcx)?;
                    unsafe { span_at(na).prevspan = span_pointer };
                }
                span.fclass = tfclass as u16;
            }

            span_pointer = next_span_pointer;
        }

        if pool_spans_get(base, size_class, 1) != INVALID_DSA_POINTER {
            return Ok(true);
        }
        fclass += 1;
    }

    debug_assert_eq!(pool_spans_get(base, size_class, 1), INVALID_DSA_POINTER);
    for fclass in 2..DSA_FULLNESS_CLASSES - 1 {
        if transfer_first_span(area, size_class, fclass, 1, mcx)? {
            return Ok(true);
        }
    }
    if pool_spans_get(base, size_class, 1) == INVALID_DSA_POINTER
        && transfer_first_span(area, size_class, 0, 1, mcx)?
    {
        return Ok(true);
    }

    // Allocate a new superblock.
    let mut span_pointer = INVALID_DSA_POINTER;
    if size_class != DSA_SCLASS_BLOCK_OF_SPANS {
        span_pointer = alloc_object(area, DSA_SCLASS_BLOCK_OF_SPANS, mcx)?;
        if span_pointer == INVALID_DSA_POINTER {
            return Ok(false);
        }
        npages = DSA_PAGES_PER_SUPERBLOCK;
    }

    let area_guard = lwlock_acquire(unsafe { control_lock(base) })?;
    let mut segment_index = get_best_segment(area, npages, mcx)?;
    if segment_index.is_none() {
        segment_index = make_new_segment(area, npages, mcx)?;
        if segment_index.is_none() {
            area_guard.release()?;
            return Ok(false);
        }
    }
    let segment_index = segment_index.unwrap();

    let (seg_base, fpm_offset, pagemap_offset) = segment_map_parts(area, segment_index);
    let first_page = fpm_get(seg_base, fpm_offset, npages).unwrap_or_else(|| {
        dsa_fatal(&format!("dsa_allocate could not find {npages} free pages for superblock"))
    });
    area_guard.release()?;

    let start_pointer = make_pointer(segment_index, first_page * FPM_PAGE_SIZE);

    if size_class == DSA_SCLASS_BLOCK_OF_SPANS {
        span_pointer = start_pointer;
    }

    init_span(area, span_pointer, size_class, start_pointer, npages, size_class as u16, mcx)?;
    for i in 0..npages {
        unsafe { pagemap_set(seg_base, pagemap_offset, first_page + i, span_pointer) };
    }

    Ok(true)
}

// ---------------------------------------------------------------------------
// Segment helpers.
// ---------------------------------------------------------------------------

/// `get_segment_by_index(area, index)` — map the segment in if necessary and
/// return its index (the C returns a `dsa_segment_map *`; we return the index,
/// since callers read the map through [`segment_map_parts`]).
fn get_segment_by_index(area: DsaAreaHandle, index: DsaSegmentIndex, mcx: mcx::Mcx<'static>) -> PgResult<DsaSegmentIndex> {
    let mapped = with_area(area, |a| a.segment_maps[index].mapped_address);
    if mapped == 0 {
        let handle =
            with_area(area, |a| unsafe { control_at(a.control_base).segment_handles[index] });
        if handle == DSM_HANDLE_INVALID {
            return Err(PgError::error(
                "dsa_area could not attach to a segment that has been freed",
            ));
        }
        // C swaps CurrentResourceOwner to area->resowner across the attach so the
        // mapping is owned by the area's owner. Here the dsm registry owns the
        // mapping; we keep the id and detach explicitly in dsa_detach.
        let segment = match dsm::dsm_attach(handle, mcx)? {
            Some(s) => s.into_id(),
            None => return Err(PgError::error("dsa_area could not attach to segment")),
        };
        let mapped_address = dsm::dsm_segment_address(segment) as u64;
        with_area(area, |a| {
            a.segment_maps[index] = DsaSegmentMap {
                segment: Some(segment),
                mapped_address,
                fpm_offset: segment_fpm_offset(),
                pagemap_offset: segment_pagemap_offset(),
            };
            if a.high_segment_index < index {
                a.high_segment_index = index;
            }
        });
        // C (dsa.c:1817): the magic check reads `area->control->handle`
        // *directly*, NOT via `dsa_get_handle()`. For an area created with
        // `dsa_create_in_place` (e.g. `es_query_dsa`), `control->handle ==
        // DSA_HANDLE_INVALID` is the normal, valid state; `dsa_get_handle`
        // additionally asserts the handle is *not* invalid, so calling it here
        // panicked a worker the moment it attached an on-demand (grown) segment
        // of an in-place area — the parallel-hash / parallel-bitmap keystone.
        let area_handle_val = with_area(area, |a| unsafe { control_at(a.control_base).handle });
        debug_assert_eq!(
            unsafe { header_at(mapped_address).magic },
            DSA_SEGMENT_HEADER_MAGIC ^ area_handle_val ^ index as u32
        );
    }
    debug_assert!(!unsafe { header_at(with_area(area, |a| a.segment_maps[index].mapped_address)).freed });
    Ok(index)
}

/// Read `(mapped_address, fpm_offset, pagemap_offset)` for a mapped segment.
fn segment_map_parts(area: DsaAreaHandle, index: DsaSegmentIndex) -> (u64, Size, Size) {
    with_area(area, |a| {
        let m = &a.segment_maps[index];
        (m.mapped_address, m.fpm_offset, m.pagemap_offset)
    })
}

fn destroy_superblock(area: DsaAreaHandle, span_pointer: DsaPointer, mcx: mcx::Mcx<'static>) -> PgResult<()> {
    let base = with_area(area, |a| a.control_base);
    let span_addr = dsa_get_address(area, span_pointer, mcx)?;
    let size_class = unsafe { span_at(span_addr).size_class } as usize;

    unlink_span(area, span_pointer, mcx)?;

    let area_guard = lwlock_acquire(unsafe { control_lock(base) })?;
    check_for_freed_segments_locked(area, mcx)?;

    let (span_start, span_npages) = {
        let span = unsafe { span_at(span_addr) };
        (span.start, span.npages)
    };
    let seg_idx = (span_start >> DSA_OFFSET_WIDTH) as DsaSegmentIndex;
    let segment_index = get_segment_by_index(area, seg_idx, mcx)?;
    let (seg_base, fpm_offset, _) = segment_map_parts(area, segment_index);

    let first_page = ((span_start & DSA_OFFSET_BITMASK) as Size) / FPM_PAGE_SIZE;
    fpm_put(seg_base, fpm_offset, first_page, span_npages)?;

    let mut header_still_present = true;
    if fpm_largest(seg_base, fpm_offset) == unsafe { header_at(seg_base).usable_pages }
        && segment_index != 0
    {
        unlink_segment(area, segment_index, mcx)?;
        unsafe {
            let hdr = header_at(seg_base);
            hdr.freed = true;
            let control = control_at(base);
            debug_assert!(control.total_segment_size >= hdr.size);
            control.total_segment_size -= hdr.size;
        }
        let seg_handle =
            with_area(area, |a| unsafe { control_at(a.control_base).segment_handles[segment_index] });
        let seg_id = with_area(area, |a| a.segment_maps[segment_index].segment);
        dsm::dsm_unpin_segment(seg_handle)?;
        if let Some(sid) = seg_id {
            dsm::dsm_detach(sid)?;
        }
        with_area(area, |a| {
            unsafe { control_at(a.control_base).segment_handles[segment_index] = DSM_HANDLE_INVALID };
            unsafe { control_at(a.control_base).freed_segment_counter += 1 };
            a.segment_maps[segment_index] = DsaSegmentMap::zero();
        });
        header_still_present = false;
    }

    if header_still_present {
        rebin_segment(area, segment_index, mcx)?;
    }

    area_guard.release()?;

    if size_class != DSA_SCLASS_BLOCK_OF_SPANS {
        dsa_free(area, span_pointer, mcx)?;
    }
    Ok(())
}

fn unlink_span(area: DsaAreaHandle, span_pointer: DsaPointer, mcx: mcx::Mcx<'static>) -> PgResult<()> {
    let base = with_area(area, |a| a.control_base);
    let span_addr = dsa_get_address(area, span_pointer, mcx)?;
    let (nextspan, prevspan, pool, fclass) = {
        let span = unsafe { span_at(span_addr) };
        (span.nextspan, span.prevspan, span.pool, span.fclass as usize)
    };

    if nextspan != INVALID_DSA_POINTER {
        let na = dsa_get_address(area, nextspan, mcx)?;
        unsafe { span_at(na).prevspan = prevspan };
    }
    if prevspan != INVALID_DSA_POINTER {
        let pa = dsa_get_address(area, prevspan, mcx)?;
        unsafe { span_at(pa).nextspan = nextspan };
    } else {
        // pool->spans[span->fclass] = span->nextspan.
        pool_spans_set_via_pointer(area, base, pool, fclass, nextspan, mcx)?;
    }
    Ok(())
}

fn add_span_to_fullness_class(
    area: DsaAreaHandle,
    span_pointer: DsaPointer,
    fclass: usize,
    mcx: mcx::Mcx<'static>,
) -> PgResult<()> {
    let base = with_area(area, |a| a.control_base);
    let span_addr = dsa_get_address(area, span_pointer, mcx)?;
    let pool = unsafe { span_at(span_addr).pool };

    let head = pool_spans_get_via_pointer(area, base, pool, fclass, mcx)?;
    if head != INVALID_DSA_POINTER {
        let ha = dsa_get_address(area, head, mcx)?;
        unsafe { span_at(ha).prevspan = span_pointer };
    }
    let span = unsafe { span_at(span_addr) };
    span.prevspan = INVALID_DSA_POINTER;
    span.nextspan = head;
    pool_spans_set_via_pointer(area, base, pool, fclass, span_pointer, mcx)?;
    span.fclass = fclass as u16;
    Ok(())
}

fn unlink_segment(area: DsaAreaHandle, segment_index: DsaSegmentIndex, mcx: mcx::Mcx<'static>) -> PgResult<()> {
    let base = with_area(area, |a| a.control_base);
    let (seg_base, _, _) = segment_map_parts(area, segment_index);
    let (prev, next, bin) = unsafe {
        let h = header_at(seg_base);
        (h.prev, h.next, h.bin)
    };

    if prev != DSA_SEGMENT_INDEX_NONE {
        let pidx = get_segment_by_index(area, prev, mcx)?;
        let (pb, _, _) = segment_map_parts(area, pidx);
        unsafe { header_at(pb).next = next };
    } else {
        debug_assert_eq!(unsafe { control_at(base).segment_bins[bin] }, segment_index);
        unsafe { control_at(base).segment_bins[bin] = next };
    }
    if next != DSA_SEGMENT_INDEX_NONE {
        let nidx = get_segment_by_index(area, next, mcx)?;
        let (nb, _, _) = segment_map_parts(area, nidx);
        unsafe { header_at(nb).prev = prev };
    }
    Ok(())
}

fn get_best_segment(area: DsaAreaHandle, npages: Size, mcx: mcx::Mcx<'static>) -> PgResult<Option<DsaSegmentIndex>> {
    let base = with_area(area, |a| a.control_base);
    check_for_freed_segments_locked(area, mcx)?;

    let mut bin = contiguous_pages_to_segment_bin(npages);
    while bin < DSA_NUM_SEGMENT_BINS {
        let threshold: Size = 1usize << (bin - 1);
        let mut segment_index = unsafe { control_at(base).segment_bins[bin] };
        while segment_index != DSA_SEGMENT_INDEX_NONE {
            let sidx = get_segment_by_index(area, segment_index, mcx)?;
            let (sbase, sfpm, _) = segment_map_parts(area, sidx);
            let next_segment_index = unsafe { header_at(sbase).next };
            let contiguous_pages = fpm_largest(sbase, sfpm);

            if contiguous_pages >= threshold && contiguous_pages < npages {
                segment_index = next_segment_index;
                continue;
            }
            if contiguous_pages < threshold {
                rebin_segment(area, sidx, mcx)?;
            }
            if contiguous_pages >= npages {
                return Ok(Some(sidx));
            }
            segment_index = next_segment_index;
        }
        bin += 1;
    }
    Ok(None)
}

fn make_new_segment(
    area: DsaAreaHandle,
    requested_pages: Size,
    mcx: mcx::Mcx<'static>,
) -> PgResult<Option<DsaSegmentIndex>> {
    let base = with_area(area, |a| a.control_base);

    // Find a free segment slot.
    let mut new_index: DsaSegmentIndex = 1;
    while new_index < DSA_MAX_SEGMENTS {
        if unsafe { control_at(base).segment_handles[new_index] } == DSM_HANDLE_INVALID {
            break;
        }
        new_index += 1;
    }
    if new_index == DSA_MAX_SEGMENTS {
        return Ok(None);
    }

    let (init_size, max_size, total, max_total) = unsafe {
        let c = control_at(base);
        (c.init_segment_size, c.max_segment_size, c.total_segment_size, c.max_total_segment_size)
    };
    if total >= max_total {
        return Ok(None);
    }

    let mut total_size = init_size * (1usize << (new_index / DSA_NUM_SEGMENTS_AT_EACH_SIZE));
    total_size = total_size.min(max_size);
    total_size = total_size.min(max_total - total);

    let total_pages = total_size / FPM_PAGE_SIZE;
    let mut metadata_bytes = maxalign(size_of::<DsaSegmentHeader>())
        + maxalign(size_of::<FreePageManager>())
        + size_of::<DsaPointer>() * total_pages;
    if metadata_bytes % FPM_PAGE_SIZE != 0 {
        metadata_bytes += FPM_PAGE_SIZE - (metadata_bytes % FPM_PAGE_SIZE);
    }
    if total_size <= metadata_bytes {
        return Ok(None);
    }
    let mut usable_pages = (total_size - metadata_bytes) / FPM_PAGE_SIZE;
    debug_assert!(metadata_bytes + usable_pages * FPM_PAGE_SIZE <= total_size);

    if requested_pages > usable_pages {
        usable_pages = requested_pages;
        metadata_bytes = maxalign(size_of::<DsaSegmentHeader>())
            + maxalign(size_of::<FreePageManager>())
            + usable_pages * size_of::<DsaPointer>();
        if metadata_bytes % FPM_PAGE_SIZE != 0 {
            metadata_bytes += FPM_PAGE_SIZE - (metadata_bytes % FPM_PAGE_SIZE);
        }
        total_size = metadata_bytes + usable_pages * FPM_PAGE_SIZE;
        if total_size > DSA_MAX_SEGMENT_SIZE {
            return Ok(None);
        }
        if total_size > max_total - total {
            return Ok(None);
        }
    }

    // Create the segment.
    let segment = match dsm::dsm_create(total_size, 0, mcx)? {
        Some(s) => s.into_id(),
        None => return Ok(None),
    };
    dsm::dsm_pin_segment(segment)?;

    let new_handle = dsm::dsm_segment_handle(segment);
    let mapped_address = dsm::dsm_segment_address(segment) as u64;

    unsafe {
        let c = control_at(base);
        c.segment_handles[new_index] = new_handle;
        if c.high_segment_index < new_index {
            c.high_segment_index = new_index;
        }
        c.total_segment_size += total_size;
        debug_assert!(c.total_segment_size <= c.max_total_segment_size);
    }
    with_area(area, |a| {
        if a.high_segment_index < new_index {
            a.high_segment_index = new_index;
        }
        a.segment_maps[new_index] = DsaSegmentMap {
            segment: Some(segment),
            mapped_address,
            fpm_offset: segment_fpm_offset(),
            pagemap_offset: segment_pagemap_offset(),
        };
    });

    let fpm_offset = segment_fpm_offset();

    // Set up the free page map.
    fpm_initialize(mapped_address, fpm_offset);
    fpm_put(mapped_address, fpm_offset, metadata_bytes / FPM_PAGE_SIZE, usable_pages)?;

    // Set up the segment header and put it in the appropriate bin.
    let bin = contiguous_pages_to_segment_bin(usable_pages);
    let area_handle_val = unsafe { control_at(base).handle };
    let bin_head = unsafe { control_at(base).segment_bins[bin] };
    unsafe {
        let hdr = header_at(mapped_address);
        hdr.magic = DSA_SEGMENT_HEADER_MAGIC ^ area_handle_val ^ new_index as u32;
        hdr.usable_pages = usable_pages;
        hdr.size = total_size;
        hdr.bin = bin;
        hdr.prev = DSA_SEGMENT_INDEX_NONE;
        hdr.next = bin_head;
        hdr.freed = false;
    }
    unsafe { control_at(base).segment_bins[bin] = new_index };
    let next_after = unsafe { header_at(mapped_address).next };
    if next_after != DSA_SEGMENT_INDEX_NONE {
        let nidx = get_segment_by_index(area, next_after, mcx)?;
        let (nb, _, _) = segment_map_parts(area, nidx);
        unsafe {
            debug_assert_eq!(header_at(nb).bin, bin);
            header_at(nb).prev = new_index;
        }
    }

    Ok(Some(new_index))
}

fn check_for_freed_segments(area: DsaAreaHandle, mcx: mcx::Mcx<'static>) -> PgResult<()> {
    let base = with_area(area, |a| a.control_base);
    let counter = unsafe { control_at(base).freed_segment_counter };
    let mine = with_area(area, |a| a.freed_segment_counter);
    if mine != counter {
        let guard = lwlock_acquire(unsafe { control_lock(base) })?;
        check_for_freed_segments_locked(area, mcx)?;
        guard.release()?;
    }
    Ok(())
}

fn check_for_freed_segments_locked(area: DsaAreaHandle, _mcx: mcx::Mcx<'static>) -> PgResult<()> {
    let base = with_area(area, |a| a.control_base);
    let counter = unsafe { control_at(base).freed_segment_counter };
    let mine = with_area(area, |a| a.freed_segment_counter);
    if mine != counter {
        let hi = with_area(area, |a| a.high_segment_index);
        let mut i: DsaSegmentIndex = 0;
        while i <= hi {
            let (mapped, seg) = with_area(area, |a| {
                (a.segment_maps[i].mapped_address, a.segment_maps[i].segment)
            });
            if mapped != 0 && unsafe { header_at(mapped).freed } {
                if let Some(sid) = seg {
                    dsm::dsm_detach(sid)?;
                }
                with_area(area, |a| a.segment_maps[i] = DsaSegmentMap::zero());
            }
            i += 1;
        }
        with_area(area, |a| a.freed_segment_counter = counter);
    }
    Ok(())
}

fn rebin_segment(area: DsaAreaHandle, segment_index: DsaSegmentIndex, mcx: mcx::Mcx<'static>) -> PgResult<()> {
    let base = with_area(area, |a| a.control_base);
    let (seg_base, fpm_offset, _) = segment_map_parts(area, segment_index);
    let new_bin = contiguous_pages_to_segment_bin(fpm_largest(seg_base, fpm_offset));
    if unsafe { header_at(seg_base).bin } == new_bin {
        return Ok(());
    }

    unlink_segment(area, segment_index, mcx)?;

    let bin_head = unsafe { control_at(base).segment_bins[new_bin] };
    unsafe {
        let hdr = header_at(seg_base);
        hdr.prev = DSA_SEGMENT_INDEX_NONE;
        hdr.next = bin_head;
        hdr.bin = new_bin;
    }
    unsafe { control_at(base).segment_bins[new_bin] = segment_index };
    let next_after = unsafe { header_at(seg_base).next };
    if next_after != DSA_SEGMENT_INDEX_NONE {
        let nidx = get_segment_by_index(area, next_after, mcx)?;
        let (nb, _, _) = segment_map_parts(area, nidx);
        unsafe {
            debug_assert_eq!(header_at(nb).bin, new_bin);
            header_at(nb).prev = segment_index;
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Pool span-list accessors (control->pools[class].spans[fclass]) + the
// DsaAreaPoolToDsaPointer macro.
// ---------------------------------------------------------------------------

/// `DsaAreaPoolToDsaPointer(area, &pools[class])` — segment 0, offset of the
/// pool within the control block.
fn dsa_area_pool_to_dsa_pointer(pool_class: usize) -> DsaPointer {
    make_pointer(0, pool_offset(pool_class))
}

fn pool_offset(pool_class: usize) -> Size {
    let nullc = core::ptr::null::<DsaAreaControl>();
    let pools = unsafe { core::ptr::addr_of!((*nullc).pools) as usize };
    pools + pool_class * size_of::<DsaAreaPool>()
}

#[inline]
fn pool_spans_get(base: u64, pool_class: usize, fclass: usize) -> DsaPointer {
    unsafe { control_at(base).pools[pool_class].spans[fclass] }
}

#[inline]
fn pool_spans_set(base: u64, pool_class: usize, fclass: usize, v: DsaPointer) {
    unsafe { control_at(base).pools[pool_class].spans[fclass] = v };
}

/// `pool->spans[fclass]` reached via a `dsa_pointer` to the pool.
#[inline]
fn pool_spans_get_via_pointer(
    area: DsaAreaHandle,
    _base: u64,
    pool: DsaPointer,
    fclass: usize,
    mcx: mcx::Mcx<'static>,
) -> PgResult<DsaPointer> {
    let pa = dsa_get_address(area, pool, mcx)?;
    Ok(unsafe { (*(pa as usize as *const DsaAreaPool)).spans[fclass] })
}

#[inline]
fn pool_spans_set_via_pointer(
    area: DsaAreaHandle,
    _base: u64,
    pool: DsaPointer,
    fclass: usize,
    v: DsaPointer,
    mcx: mcx::Mcx<'static>,
) -> PgResult<()> {
    let pa = dsa_get_address(area, pool, mcx)?;
    unsafe { (*(pa as usize as *mut DsaAreaPool)).spans[fclass] = v };
    Ok(())
}

// ---------------------------------------------------------------------------
// Datum <-> pointer marshaling for the detach hook.
// ---------------------------------------------------------------------------

fn pointer_get_datum(p: u64) -> datum::Datum {
    datum::Datum::from_usize(p as usize)
}

fn datum_get_pointer(d: datum::Datum) -> u64 {
    d.as_usize() as u64
}
