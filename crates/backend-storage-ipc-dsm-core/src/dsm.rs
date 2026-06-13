//! `storage/ipc/dsm.c` — the reference-counted convenience layer over the
//! low-level DSM backing.
//!
//! Shared state — the control segment (`dsm_control_header` + its
//! `dsm_control_item[]`) and the preallocated main-region pages — lives in
//! real shared memory and is touched through raw pointers while holding
//! `DynamicSharedMemoryControlLock` (always via a [`MainLWLockGuard`], so an
//! `ereport(ERROR)` under the lock releases it on unwind).
//!
//! Backend-local state — the attached-segment list and the control-segment
//! globals each process inherits at fork — is `thread_local`. The descriptor
//! and on-detach callback records that C `MemoryContextAlloc`s in
//! `TopMemoryContext` are allocated through a caller-provided `Mcx<'static>`
//! (the TopMemoryContext-equivalent handle: descriptors live for the
//! backend's life in thread-local state, so the context must be
//! backend-lifetime too).
//!
//! The C `dsm_segment *` maps to two things: a [`DsmSegmentId`] (stable
//! identity; ids are never reused, so a stale id traps loudly where the C
//! would dereference freed memory) and the [`DsmSegment`] RAII guard that
//! replaces the `ResourceOwner` bookkeeping (`docs/query-lifecycle-raii.md`):
//! `ResourceOwnerRememberDSM` is guard construction, `ResourceOwnerForgetDSM`
//! is consuming it, and `ResOwnerReleaseDSM` is its `Drop`.

use std::cell::{Cell, RefCell};

use backend_storage_lmgr_lwlock::{LWLockAcquireMain, MainLWLockGuard};
use backend_utils_error::{elog, ereport};
use mcx::{Mcx, PgVec};
use types_core::Size;
use types_datum::Datum;
use types_error::{
    ErrorLocation, PgResult, DEBUG1, DEBUG2, ERRCODE_INSUFFICIENT_RESOURCES, ERROR, LOG, WARNING,
};
use types_freepage::{FreePageManager, FPM_PAGE_SIZE};
use types_storage::{
    dsm_handle, PGShmemHeader, DSM_HANDLE_INVALID, DYNAMIC_SHARED_MEMORY_CONTROL_LOCK,
    LW_EXCLUSIVE,
};

use crate::dsm_impl::{
    dsm_impl_op, dsm_impl_pin_segment, dsm_impl_unpin_segment, dynamic_shared_memory_type,
    min_dynamic_shared_memory, DsmImplPrivate, DsmOp, DSM_IMPL_MMAP, PG_DYNSHMEM_DIR,
    PG_DYNSHMEM_MMAP_FILE_PREFIX,
};
use crate::ipc::on_shmem_exit;

use backend_utils_mmgr_freepage_seams::{
    free_page_manager_get, free_page_manager_initialize, free_page_manager_put,
};

pub const PG_DYNSHMEM_CONTROL_MAGIC: u32 = 0x9a50_3d32;

pub const PG_DYNSHMEM_FIXED_SLOTS: i32 = 64;
pub const PG_DYNSHMEM_SLOTS_PER_BACKEND: i32 = 5;

/// `INVALID_CONTROL_SLOT` (`(uint32) -1`).
pub const INVALID_CONTROL_SLOT: u32 = u32::MAX;

/// `DSM_CREATE_NULL_IF_MAXSEGMENTS` (`storage/dsm.h`).
pub const DSM_CREATE_NULL_IF_MAXSEGMENTS: i32 = 0x0001;

fn loc(funcname: &str) -> ErrorLocation {
    ErrorLocation::new("dsm.c", 0, funcname)
}

// ---------------------------------------------------------------------------
// Shared-memory layout (dsm.c private structs; repr(C) because they live in
// the control segment shared across processes).
// ---------------------------------------------------------------------------

/// `dsm_control_item` — shared-memory state for one segment.
#[repr(C)]
struct dsm_control_item {
    handle: dsm_handle,
    /// 2+ = active, 1 = moribund, 0 = gone.
    refcnt: u32,
    first_page: usize,
    npages: usize,
    /// `void *impl_private_pm_handle` — only needed on (unported) Windows;
    /// kept for layout parity.
    impl_private_pm_handle: usize,
    pinned: bool,
}

/// `dsm_control_header` — layout of the control segment.
#[repr(C)]
struct dsm_control_header {
    magic: u32,
    nitems: u32,
    maxitems: u32,
    item: [dsm_control_item; 0],
}

/// `&control->item[i]` — pointer into the flexible array member.
unsafe fn control_item(control: *mut dsm_control_header, i: u32) -> *mut dsm_control_item {
    let base = (control as *mut u8).add(std::mem::offset_of!(dsm_control_header, item));
    (base as *mut dsm_control_item).add(i as usize)
}

// ---------------------------------------------------------------------------
// Backend-local state (the dsm.c file statics; per-backend, inherited at
// fork in C, hence thread_local here).
// ---------------------------------------------------------------------------

/// `on_dsm_detach_callback` (`storage/dsm.h`). The C callback receives the
/// detaching `dsm_segment *`; here its id. Callbacks may `ereport(ERROR)`.
pub type OnDsmDetachCallback = fn(DsmSegmentId, Datum) -> PgResult<()>;

/// `dsm_segment_detach_callback`.
#[derive(Clone, Copy)]
struct DetachCallback {
    function: OnDsmDetachCallback,
    arg: Datum,
}

/// Backend-local state for one segment — the C `struct dsm_segment` minus
/// the resowner field (replaced by the [`DsmSegment`] guard) and the dlist
/// node (the descriptors live in a Vec).
struct DsmSegmentDesc {
    id: DsmSegmentId,
    /// `dsm_handle handle` — segment name.
    handle: dsm_handle,
    /// `uint32 control_slot` — slot in control segment.
    control_slot: u32,
    /// `void *impl_private`.
    impl_private: DsmImplPrivate,
    /// `void *mapped_address` — or NULL if unmapped.
    mapped_address: *mut u8,
    /// `Size mapped_size`.
    mapped_size: usize,
    /// `slist_head on_detach` — LIFO: newest at the back of the Vec.
    on_detach: PgVec<'static, DetachCallback>,
}

thread_local! {
    /// `dsm_init_done`.
    static DSM_INIT_DONE: Cell<bool> = const { Cell::new(false) };
    /// `dsm_main_space_begin` — preallocated DSM space in the main region
    /// (doubles as the `FreePageManager *`, as in C).
    static DSM_MAIN_SPACE_BEGIN: Cell<*mut u8> = const { Cell::new(std::ptr::null_mut()) };
    /// `dsm_segment_list` — segments attached by this backend; the C dlist
    /// head is the *back* of this Vec (`dlist_push_head` == `push`). `None`
    /// until the first descriptor is created (the storage is allocated in
    /// the TopMemoryContext-equivalent context passed to
    /// [`dsm_create_descriptor`], as C `MemoryContextAlloc`s descriptors in
    /// `TopMemoryContext`).
    static DSM_SEGMENT_LIST: RefCell<Option<PgVec<'static, DsmSegmentDesc>>> =
        const { RefCell::new(None) };
    /// Monotonic id mint for [`DsmSegmentId`]; starts at 1 so 0 never names a
    /// live segment.
    static DSM_NEXT_ID: Cell<u64> = const { Cell::new(1) };

    // Control segment information: not reference counted; lasts the
    // postmaster's whole life cycle and has no dsm_segment object.
    /// `dsm_control_handle`.
    static DSM_CONTROL_HANDLE: Cell<dsm_handle> = const { Cell::new(0) };
    /// `dsm_control`.
    static DSM_CONTROL: Cell<*mut dsm_control_header> =
        const { Cell::new(std::ptr::null_mut()) };
    /// `dsm_control_mapped_size`.
    static DSM_CONTROL_MAPPED_SIZE: Cell<usize> = const { Cell::new(0) };
    /// `dsm_control_impl_private`.
    static DSM_CONTROL_IMPL_PRIVATE: Cell<DsmImplPrivate> =
        const { Cell::new(DsmImplPrivate::None) };
}

/// Stable identity of a live segment — the idiomatic stand-in for the C
/// `dsm_segment *`. Never reused after detach.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct DsmSegmentId(u64);

/// RAII guard standing in for the C `seg->resowner` bookkeeping: dropping it
/// detaches the segment (`ResOwnerReleaseDSM`). [`dsm_pin_mapping`] consumes
/// the guard, making the mapping session-lifetime (the C `resowner = NULL`);
/// [`dsm_unpin_mapping`] re-creates it.
pub struct DsmSegment {
    id: DsmSegmentId,
}

impl DsmSegment {
    pub fn id(&self) -> DsmSegmentId {
        self.id
    }

    /// Consume the guard without detaching (`ResourceOwnerForgetDSM`).
    pub fn into_id(self) -> DsmSegmentId {
        let id = self.id;
        std::mem::forget(self);
        id
    }
}

impl Drop for DsmSegment {
    /// `ResOwnerReleaseDSM` — detach at owner release. A no-op if the segment
    /// was already detached through another path. An `ereport(ERROR)` from a
    /// detach callback cannot propagate out of `Drop` (C re-enters error
    /// recovery here); it is demoted to a WARNING rather than discarded,
    /// pending elog's error-during-error machinery.
    fn drop(&mut self) {
        let live = DSM_SEGMENT_LIST.with(|list| {
            list.borrow()
                .as_ref()
                .is_some_and(|l| l.iter().any(|desc| desc.id == self.id))
        });
        if live {
            if let Err(e) = dsm_detach(self.id) {
                let _ = elog(
                    WARNING,
                    format!(
                        "error ignored while detaching dynamic shared memory segment: {}",
                        e.message()
                    ),
                );
            }
        }
    }
}

impl std::fmt::Debug for DsmSegment {
    /// `ResOwnerPrintDSM`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let handle = dsm_segment_handle(self.id);
        write!(f, "dynamic shared memory segment {handle}")
    }
}

/// Run `f` on the descriptor named `id`. Panics on a stale id — the C
/// equivalent would dereference freed memory.
fn with_desc<R>(id: DsmSegmentId, f: impl FnOnce(&mut DsmSegmentDesc) -> R) -> R {
    DSM_SEGMENT_LIST.with(|list| {
        let mut list = list.borrow_mut();
        let desc = list
            .as_mut()
            .and_then(|l| l.iter_mut().find(|desc| desc.id == id))
            .expect("dsm: use of unknown or detached segment id");
        f(desc)
    })
}

fn control() -> *mut dsm_control_header {
    DSM_CONTROL.with(|c| c.get())
}

/// `LWLockAcquire(DynamicSharedMemoryControlLock, LW_EXCLUSIVE)` — guard
/// scope, so the lock can never leak across an error return.
fn acquire_control_lock() -> PgResult<MainLWLockGuard> {
    LWLockAcquireMain(DYNAMIC_SHARED_MEMORY_CONTROL_LOCK, LW_EXCLUSIVE)
}

/// `dsm_main_space_begin` viewed as the region's `FreePageManager *`, as in C.
fn main_space_fpm() -> *mut FreePageManager {
    DSM_MAIN_SPACE_BEGIN.with(|c| c.get()) as *mut FreePageManager
}

// ---------------------------------------------------------------------------
// Inline helpers.
// ---------------------------------------------------------------------------

/// `pg_leftmost_one_pos32` (`port/pg_bitutils.h`) — undefined for 0, as in C.
#[inline]
fn pg_leftmost_one_pos32(word: u32) -> i32 {
    debug_assert!(word != 0);
    31 - word.leading_zeros() as i32
}

/// `is_main_region_dsm_handle` — main-region pseudo-segment handles are odd.
#[inline]
fn is_main_region_dsm_handle(handle: dsm_handle) -> bool {
    handle & 1 != 0
}

/// `make_main_region_dsm_handle` — odd handle encoding the slot in the low
/// bits, with the remaining bits randomized to keep newly created and
/// recently destroyed handles from being confused.
fn make_main_region_dsm_handle(slot: i32) -> dsm_handle {
    let mut handle: dsm_handle = 1;
    handle |= (slot << 1) as dsm_handle;
    let maxitems = unsafe { (*control()).maxitems };
    handle |= pg_prng::global_prng(|prng| prng.next_u32())
        << (pg_leftmost_one_pos32(maxitems) + 1);
    handle
}

/// `dsm_control_bytes_needed(nitems)`.
fn dsm_control_bytes_needed(nitems: u32) -> u64 {
    std::mem::offset_of!(dsm_control_header, item) as u64
        + std::mem::size_of::<dsm_control_item>() as u64 * nitems as u64
}

/// `dsm_control_segment_sane` — enough sanity to iterate the items without
/// overrunning the mapping, plus the magic number.
fn dsm_control_segment_sane(control: *mut dsm_control_header, mapped_size: usize) -> bool {
    if mapped_size < std::mem::offset_of!(dsm_control_header, item) {
        return false; // Mapped size too short to read header.
    }
    let (magic, nitems, maxitems) =
        unsafe { ((*control).magic, (*control).nitems, (*control).maxitems) };
    if magic != PG_DYNSHMEM_CONTROL_MAGIC {
        return false; // Magic number doesn't match.
    }
    if dsm_control_bytes_needed(maxitems) > mapped_size as u64 {
        return false; // Max item count won't fit in map.
    }
    if nitems > maxitems {
        return false; // Overfull.
    }
    true
}

// ---------------------------------------------------------------------------
// Startup / shutdown.
// ---------------------------------------------------------------------------

/// `dsm_postmaster_startup(PGShmemHeader *shim)` — start the dynamic shared
/// memory system: called once per cluster lifetime at postmaster startup,
/// creating and initializing the control segment. `max_backends` is the
/// caller's `MaxBackends` (globals.c), passed explicitly per the
/// no-ambient-global rule.
pub fn dsm_postmaster_startup(shim: *mut PGShmemHeader, max_backends: i32) -> PgResult<()> {
    // Assert(!IsUnderPostmaster).

    // If we're using the mmap implementation, clean up any leftovers; for
    // POSIX and System V this happens earlier in startup via a direct call
    // to dsm_cleanup_using_control_segment.
    if dynamic_shared_memory_type() == DSM_IMPL_MMAP {
        dsm_cleanup_for_mmap()?;
    }

    // Determine size for new control segment.
    let maxitems =
        (PG_DYNSHMEM_FIXED_SLOTS + PG_DYNSHMEM_SLOTS_PER_BACKEND * max_backends) as u32;
    elog(
        DEBUG2,
        format!("dynamic shared memory system will support {maxitems} segments"),
    )?;
    let segsize = dsm_control_bytes_needed(maxitems) as usize;

    // Loop until we find an unused identifier for the new control segment;
    // DSM_HANDLE_INVALID is a sentinel ("no control segment"), so never
    // generate it for a real handle. Even numbers only.
    let mut control_handle: dsm_handle;
    let mut impl_private = DsmImplPrivate::None;
    let mut control_address: *mut u8 = std::ptr::null_mut();
    let mut control_mapped_size: usize = 0;
    loop {
        debug_assert!(control_address.is_null());
        debug_assert!(control_mapped_size == 0);
        control_handle = pg_prng::global_prng(|prng| prng.next_u32()) << 1;
        if control_handle == DSM_HANDLE_INVALID {
            continue;
        }
        if dsm_impl_op(
            DsmOp::Create,
            control_handle,
            segsize,
            &mut impl_private,
            &mut control_address,
            &mut control_mapped_size,
            ERROR,
        )? {
            break;
        }
    }
    DSM_CONTROL_HANDLE.with(|c| c.set(control_handle));
    DSM_CONTROL_IMPL_PRIVATE.with(|c| c.set(impl_private));
    DSM_CONTROL_MAPPED_SIZE.with(|c| c.set(control_mapped_size));
    let control = control_address as *mut dsm_control_header;
    DSM_CONTROL.with(|c| c.set(control));

    on_shmem_exit(dsm_postmaster_shutdown, Datum::from_usize(shim as usize))?;
    elog(
        DEBUG2,
        format!(
            "created dynamic shared memory control segment {control_handle} ({segsize} bytes)"
        ),
    )?;
    unsafe {
        (*shim).dsm_control = control_handle;
    }

    // Initialize control segment.
    unsafe {
        (*control).magic = PG_DYNSHMEM_CONTROL_MAGIC;
        (*control).nitems = 0;
        (*control).maxitems = maxitems;
    }
    Ok(())
}

/// `dsm_cleanup_using_control_segment(dsm_handle old_control_handle)` — if
/// the previous postmaster invocation's control segment still exists, remove
/// the segments it refers to and then the control segment itself.
pub fn dsm_cleanup_using_control_segment(old_control_handle: dsm_handle) -> PgResult<()> {
    let mut mapped_address: *mut u8 = std::ptr::null_mut();
    let mut junk_mapped_address: *mut u8 = std::ptr::null_mut();
    let mut impl_private = DsmImplPrivate::None;
    let mut junk_impl_private = DsmImplPrivate::None;
    let mut mapped_size: usize = 0;
    let mut junk_mapped_size: usize = 0;

    // Try to attach the segment. Failure probably just means the OS rebooted
    // or an unrelated process reused the shm ID, so fall out quietly.
    if !dsm_impl_op(
        DsmOp::Attach,
        old_control_handle,
        0,
        &mut impl_private,
        &mut mapped_address,
        &mut mapped_size,
        DEBUG1,
    )? {
        return Ok(());
    }

    // Reattached, but the contents might not be sane; if so disregard it.
    let old_control = mapped_address as *mut dsm_control_header;
    if !dsm_control_segment_sane(old_control, mapped_size) {
        let _ = dsm_impl_op(
            DsmOp::Detach,
            old_control_handle,
            0,
            &mut impl_private,
            &mut mapped_address,
            &mut mapped_size,
            LOG,
        )?;
        return Ok(());
    }

    // Use it to get a list of segments that need to be removed.
    let nitems = unsafe { (*old_control).nitems };
    for i in 0..nitems {
        // If the reference count is 0, the slot is actually unused.
        let refcnt = unsafe { (*control_item(old_control, i)).refcnt };
        if refcnt == 0 {
            continue;
        }

        // If it was using the main shmem area, there is nothing to do.
        let handle = unsafe { (*control_item(old_control, i)).handle };
        if is_main_region_dsm_handle(handle) {
            continue;
        }

        elog(
            DEBUG2,
            format!(
                "cleaning up orphaned dynamic shared memory with ID {handle} (reference count {refcnt})"
            ),
        )?;

        // Destroy the referenced segment.
        let _ = dsm_impl_op(
            DsmOp::Destroy,
            handle,
            0,
            &mut junk_impl_private,
            &mut junk_mapped_address,
            &mut junk_mapped_size,
            LOG,
        )?;
    }

    // Destroy the old control segment, too.
    elog(
        DEBUG2,
        format!(
            "cleaning up dynamic shared memory control segment with ID {old_control_handle}"
        ),
    )?;
    let _ = dsm_impl_op(
        DsmOp::Destroy,
        old_control_handle,
        0,
        &mut impl_private,
        &mut mapped_address,
        &mut mapped_size,
        LOG,
    )?;
    Ok(())
}

/// `dsm_cleanup_for_mmap` — with the mmap implementation, segments can even
/// survive an OS reboot, and the control segment can't be trusted to be
/// current; instead scan `pg_dynshmem` and blow away everything that
/// shouldn't be there.
fn dsm_cleanup_for_mmap() -> PgResult<()> {
    // Scan the directory for something with a name of the correct format.
    // The seam owns the AllocateDir/ReadDir/FreeDir bracket: the directory
    // is closed on every path, including the ereport(ERROR) below.
    backend_storage_file_seams::with_allocated_dir::call(PG_DYNSHMEM_DIR, &mut |d_name| {
        if d_name.starts_with(PG_DYNSHMEM_MMAP_FILE_PREFIX) {
            let buf = format!("{PG_DYNSHMEM_DIR}/{d_name}");

            elog(DEBUG2, format!("removing file \"{buf}\""))?;

            // We found a matching file; so remove it.
            let cbuf = std::ffi::CString::new(buf.clone())
                .expect("interior NUL in dsm file name");
            if unsafe { libc::unlink(cbuf.as_ptr()) } != 0 {
                let en = backend_utils_error::errno::current_errno();
                ereport(ERROR)
                    .with_saved_errno(en)
                    .errcode_for_file_access()
                    .errmsg(format!("could not remove file \"{buf}\": %m"))
                    .finish(loc("dsm_cleanup_for_mmap"))?;
            }
        }
        Ok(false)
    })?;

    // Cleanup complete.
    Ok(())
}

/// `dsm_postmaster_shutdown(int code, Datum arg)` — at shutdown, iterate the
/// control segment and remove all remaining segments, avoiding errors
/// (non-critical cleanup; the postmaster is exiting either way). Registered
/// with `on_shmem_exit` by [`dsm_postmaster_startup`].
fn dsm_postmaster_shutdown(_code: i32, arg: Datum) -> PgResult<()> {
    let mut junk_mapped_address: *mut u8 = std::ptr::null_mut();
    let mut junk_impl_private = DsmImplPrivate::None;
    let mut junk_mapped_size: usize = 0;
    let shim = arg.as_usize() as *mut PGShmemHeader;

    // If some other backend exited uncleanly it might have corrupted the
    // control segment while dying; warn and ignore the contents in that case.
    let control = control();
    let nitems = unsafe { (*control).nitems };
    if !dsm_control_segment_sane(control, DSM_CONTROL_MAPPED_SIZE.with(|c| c.get())) {
        ereport(LOG)
            .errmsg("dynamic shared memory control segment is corrupt")
            .finish(loc("dsm_postmaster_shutdown"))?;
        return Ok(());
    }

    // Remove any remaining segments.
    for i in 0..nitems {
        // If the reference count is 0, the slot is actually unused.
        if unsafe { (*control_item(control, i)).refcnt } == 0 {
            continue;
        }

        let handle = unsafe { (*control_item(control, i)).handle };
        if is_main_region_dsm_handle(handle) {
            continue;
        }

        elog(
            DEBUG2,
            format!("cleaning up orphaned dynamic shared memory with ID {handle}"),
        )?;

        // Destroy the segment.
        let _ = dsm_impl_op(
            DsmOp::Destroy,
            handle,
            0,
            &mut junk_impl_private,
            &mut junk_mapped_address,
            &mut junk_mapped_size,
            LOG,
        )?;
    }

    // Remove the control segment itself.
    let control_handle = DSM_CONTROL_HANDLE.with(|c| c.get());
    elog(
        DEBUG2,
        format!("cleaning up dynamic shared memory control segment with ID {control_handle}"),
    )?;
    let mut control_address = control as *mut u8;
    let mut impl_private = DSM_CONTROL_IMPL_PRIVATE.with(|c| c.get());
    let mut mapped_size = DSM_CONTROL_MAPPED_SIZE.with(|c| c.get());
    let _ = dsm_impl_op(
        DsmOp::Destroy,
        control_handle,
        0,
        &mut impl_private,
        &mut control_address,
        &mut mapped_size,
        LOG,
    )?;
    DSM_CONTROL_IMPL_PRIVATE.with(|c| c.set(impl_private));
    DSM_CONTROL_MAPPED_SIZE.with(|c| c.set(mapped_size));
    DSM_CONTROL.with(|c| c.set(control_address as *mut dsm_control_header));
    unsafe {
        (*shim).dsm_control = 0;
    }
    Ok(())
}

/// `dsm_backend_startup` — prepare this backend for DSM use. The
/// `EXEC_BACKEND` re-mapping branch is not ported (inherited-fork model);
/// only the `dsm_init_done` flip remains.
fn dsm_backend_startup() {
    DSM_INIT_DONE.with(|c| c.set(true));
}

// `dsm_set_control_handle` is EXEC_BACKEND-only and not ported.

/// `dsm_estimate_size` — bytes to reserve in the main shared memory segment
/// for DSM segments.
pub fn dsm_estimate_size() -> usize {
    1024 * 1024 * min_dynamic_shared_memory() as usize
}

/// `dsm_shmem_init` — initialize space in the main shared memory segment for
/// DSM segments (the preallocated region's `FreePageManager`).
pub fn dsm_shmem_init() -> PgResult<()> {
    let size = dsm_estimate_size();

    if size == 0 {
        return Ok(());
    }

    let (begin, found) =
        backend_storage_ipc_shmem_seams::shmem_init_struct::call("Preallocated DSM", size)?;
    DSM_MAIN_SPACE_BEGIN.with(|c| c.set(begin));
    if !found {
        let fpm = begin as *mut FreePageManager;
        let mut first_page: usize = 0;

        // Reserve space for the FreePageManager.
        while first_page * FPM_PAGE_SIZE < std::mem::size_of::<FreePageManager>() {
            first_page += 1;
        }

        // Initialize it and give it all the rest of the space.
        free_page_manager_initialize::call(fpm, begin);
        let pages = (size / FPM_PAGE_SIZE) - first_page;
        free_page_manager_put::call(fpm, first_page, pages);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Create / attach.
// ---------------------------------------------------------------------------

/// `dsm_create(Size size, int flags)` — create a new dynamic shared memory
/// segment. `mcx` is the TopMemoryContext-equivalent handle the descriptor
/// is allocated in (C: `dsm_create_descriptor`'s `MemoryContextAlloc`).
///
/// Returns `Ok(None)` only when `flags & DSM_CREATE_NULL_IF_MAXSEGMENTS` and
/// the control segment is full. The returned guard is the resource-owner
/// association: drop it (or let `?` drop it) and the segment detaches; call
/// [`dsm_pin_mapping`] for session lifetime (the C NULL-CurrentResourceOwner
/// behavior).
pub fn dsm_create(size: Size, flags: i32, mcx: Mcx<'static>) -> PgResult<Option<DsmSegment>> {
    // Assert(IsUnderPostmaster || !IsPostmasterEnvironment) — unsafe in
    // postmaster, allowed in single-user mode.

    if !DSM_INIT_DONE.with(|c| c.get()) {
        dsm_backend_startup();
    }

    // Create a new segment descriptor.
    let seg = dsm_create_descriptor(mcx)?;
    let id = seg.id();

    let dsm_main_space_fpm = main_space_fpm();
    let mut npages: usize = 0;
    let mut first_page: usize = 0;
    let mut using_main_dsm_region = false;

    // Lock the control segment while we try to allocate from the main shared
    // memory area, if configured.
    let mut control_lock: Option<MainLWLockGuard> = None;
    if !dsm_main_space_fpm.is_null() {
        npages = size / FPM_PAGE_SIZE;
        if size % FPM_PAGE_SIZE > 0 {
            npages += 1;
        }

        control_lock = Some(acquire_control_lock()?);
        if let Some(fp) = free_page_manager_get::call(dsm_main_space_fpm, npages) {
            // We can carve out a piece of the main shared memory segment.
            first_page = fp;
            let address = unsafe {
                (dsm_main_space_fpm as *mut u8).add(first_page * FPM_PAGE_SIZE)
            };
            with_desc(id, |desc| {
                desc.mapped_address = address;
                desc.mapped_size = npages * FPM_PAGE_SIZE;
            });
            using_main_dsm_region = true;
            // We'll choose a handle below.
        }
    }

    if !using_main_dsm_region {
        // We need to create a new memory segment; loop until we find an
        // unused segment identifier.
        if let Some(guard) = control_lock.take() {
            guard.release()?;
        }
        loop {
            // Use even numbers only.
            let handle: dsm_handle = pg_prng::global_prng(|prng| prng.next_u32()) << 1;
            if handle == DSM_HANDLE_INVALID {
                // Reserve sentinel.
                continue;
            }
            with_desc(id, |desc| desc.handle = handle);
            let (mut ip, mut ma, mut ms) =
                with_desc(id, |d| (d.impl_private, d.mapped_address, d.mapped_size));
            let created = dsm_impl_op(
                DsmOp::Create,
                handle,
                size,
                &mut ip,
                &mut ma,
                &mut ms,
                ERROR,
            );
            with_desc(id, |d| {
                d.impl_private = ip;
                d.mapped_address = ma;
                d.mapped_size = ms;
            });
            if created? {
                break;
            }
        }
        control_lock = Some(acquire_control_lock()?);
    }
    let control_lock = control_lock.expect("DynamicSharedMemoryControlLock must be held here");

    // Search the control segment for an unused slot.
    let control = control();
    let nitems = unsafe { (*control).nitems };
    for i in 0..nitems {
        if unsafe { (*control_item(control, i)).refcnt } == 0 {
            let handle = if using_main_dsm_region {
                let h = make_main_region_dsm_handle(i as i32);
                with_desc(id, |d| d.handle = h);
                unsafe {
                    (*control_item(control, i)).first_page = first_page;
                    (*control_item(control, i)).npages = npages;
                }
                h
            } else {
                // Assert(!is_main_region_dsm_handle(seg->handle)).
                with_desc(id, |d| d.handle)
            };
            unsafe {
                let item = control_item(control, i);
                (*item).handle = handle;
                // refcnt of 1 triggers destruction, so start at 2.
                (*item).refcnt = 2;
                (*item).impl_private_pm_handle = 0;
                (*item).pinned = false;
            }
            with_desc(id, |d| d.control_slot = i);
            control_lock.release()?;
            return Ok(Some(seg));
        }
    }

    // Verify that we can support an additional mapping.
    let maxitems = unsafe { (*control).maxitems };
    if nitems >= maxitems {
        if using_main_dsm_region {
            free_page_manager_put::call(dsm_main_space_fpm, first_page, npages);
        }
        control_lock.release()?;
        if !using_main_dsm_region {
            let (handle, mut ip, mut ma, mut ms) = with_desc(id, |d| {
                (d.handle, d.impl_private, d.mapped_address, d.mapped_size)
            });
            let _ = dsm_impl_op(
                DsmOp::Destroy,
                handle,
                0,
                &mut ip,
                &mut ma,
                &mut ms,
                WARNING,
            );
            with_desc(id, |d| {
                d.impl_private = ip;
                d.mapped_address = ma;
                d.mapped_size = ms;
            });
        }
        // ResourceOwnerForgetDSM + dlist_delete + pfree(seg).
        destroy_descriptor(seg);

        if flags & DSM_CREATE_NULL_IF_MAXSEGMENTS != 0 {
            return Ok(None);
        }
        ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_RESOURCES)
            .errmsg("too many dynamic shared memory segments")
            .finish(loc("dsm_create"))?;
        return Ok(None); // unreachable
    }

    // Enter the handle into a new array slot.
    let handle = if using_main_dsm_region {
        let h = make_main_region_dsm_handle(nitems as i32);
        with_desc(id, |d| d.handle = h);
        unsafe {
            (*control_item(control, nitems)).first_page = first_page;
            (*control_item(control, nitems)).npages = npages;
        }
        h
    } else {
        with_desc(id, |d| d.handle)
    };
    unsafe {
        let item = control_item(control, nitems);
        (*item).handle = handle;
        // refcnt of 1 triggers destruction, so start at 2.
        (*item).refcnt = 2;
        (*item).impl_private_pm_handle = 0;
        (*item).pinned = false;
        (*control).nitems += 1;
    }
    with_desc(id, |d| d.control_slot = nitems);
    control_lock.release()?;

    Ok(Some(seg))
}

/// `dsm_attach(dsm_handle h)` — attach a dynamic shared memory segment.
/// `mcx` is the TopMemoryContext-equivalent handle the descriptor is
/// allocated in.
///
/// Returns `Ok(None)` if the segment isn't known to the system (everyone
/// else, including the creator, detached it before we got here).
pub fn dsm_attach(h: dsm_handle, mcx: Mcx<'static>) -> PgResult<Option<DsmSegment>> {
    // Assert(IsUnderPostmaster).

    if !DSM_INIT_DONE.with(|c| c.get()) {
        dsm_backend_startup();
    }

    // Debugging cross-check: the same segment must not be attached twice;
    // use dsm_find_mapping first if unsure.
    let already = DSM_SEGMENT_LIST.with(|list| {
        list.borrow()
            .as_ref()
            .is_some_and(|l| l.iter().any(|desc| desc.handle == h))
    });
    if already {
        elog(ERROR, "can't attach the same segment more than once")?;
    }

    // Create a new segment descriptor.
    let seg = dsm_create_descriptor(mcx)?;
    let id = seg.id();
    with_desc(id, |d| d.handle = h);

    // Bump reference count for this segment in shared memory.
    let control_lock = acquire_control_lock()?;
    let control = control();
    let nitems = unsafe { (*control).nitems };
    for i in 0..nitems {
        let item = unsafe { control_item(control, i) };

        // refcnt == 0: unused slot. refcnt == 1: still in use but going
        // away — even on a handle match, another slot may already have
        // started using the same handle value by coincidence, so keep
        // searching.
        if unsafe { (*item).refcnt } <= 1 {
            continue;
        }

        // If the handle doesn't match, it's not the slot we want.
        if unsafe { (*item).handle } != h {
            continue;
        }

        // Otherwise we've found a match.
        unsafe {
            (*item).refcnt += 1;
        }
        with_desc(id, |d| d.control_slot = i);
        if is_main_region_dsm_handle(h) {
            let (first_page, npages) = unsafe { ((*item).first_page, (*item).npages) };
            let begin = DSM_MAIN_SPACE_BEGIN.with(|c| c.get());
            with_desc(id, |d| {
                d.mapped_address = unsafe { begin.add(first_page * FPM_PAGE_SIZE) };
                d.mapped_size = npages * FPM_PAGE_SIZE;
            });
        }
        break;
    }
    control_lock.release()?;

    // If we didn't find the handle, it probably means that everyone else who
    // had it mapped died before we got here; up to the caller what to do.
    if with_desc(id, |d| d.control_slot) == INVALID_CONTROL_SLOT {
        dsm_detach(seg.into_id())?;
        return Ok(None);
    }

    // Here's where we actually try to map the segment.
    if !is_main_region_dsm_handle(h) {
        let (mut ip, mut ma, mut ms) =
            with_desc(id, |d| (d.impl_private, d.mapped_address, d.mapped_size));
        let attached = dsm_impl_op(DsmOp::Attach, h, 0, &mut ip, &mut ma, &mut ms, ERROR);
        with_desc(id, |d| {
            d.impl_private = ip;
            d.mapped_address = ma;
            d.mapped_size = ms;
        });
        attached?;
    }

    Ok(Some(seg))
}

// ---------------------------------------------------------------------------
// Shutdown / detach.
// ---------------------------------------------------------------------------

/// `dsm_backend_shutdown` — at backend shutdown, detach any segments that
/// are still attached. (Like [`dsm_detach_all`], minus unmapping the control
/// segment, which there's no reason to do before exiting.)
pub fn dsm_backend_shutdown() -> PgResult<()> {
    loop {
        // dlist head = newest = back of the Vec.
        let head = DSM_SEGMENT_LIST
            .with(|list| list.borrow().as_ref().and_then(|l| l.last().map(|d| d.id)));
        match head {
            Some(id) => dsm_detach(id)?,
            None => break,
        }
    }
    Ok(())
}

/// `dsm_detach_all` — detach all segments *including* the control segment;
/// for processes that might inherit mappings but are not intended to be
/// connected to dynamic shared memory.
pub fn dsm_detach_all() -> PgResult<()> {
    let control_address = control() as *mut u8;

    loop {
        let head = DSM_SEGMENT_LIST
            .with(|list| list.borrow().as_ref().and_then(|l| l.last().map(|d| d.id)));
        match head {
            Some(id) => dsm_detach(id)?,
            None => break,
        }
    }

    if !control_address.is_null() {
        let mut impl_private = DSM_CONTROL_IMPL_PRIVATE.with(|c| c.get());
        // C passes a *local* copy of dsm_control here, so the dsm_control
        // global itself is deliberately left unchanged (stale) — only the
        // impl_private and mapped_size globals are updated in place.
        let mut address = control_address;
        let mut mapped_size = DSM_CONTROL_MAPPED_SIZE.with(|c| c.get());
        let detached = dsm_impl_op(
            DsmOp::Detach,
            DSM_CONTROL_HANDLE.with(|c| c.get()),
            0,
            &mut impl_private,
            &mut address,
            &mut mapped_size,
            ERROR,
        );
        DSM_CONTROL_IMPL_PRIVATE.with(|c| c.set(impl_private));
        DSM_CONTROL_MAPPED_SIZE.with(|c| c.set(mapped_size));
        detached?;
    }
    Ok(())
}

/// `dsm_detach(dsm_segment *seg)` — detach from a segment, destroying it if
/// we remove the last reference.
///
/// This function should never fail; it is often invoked while aborting a
/// transaction. The `Err` surface is exclusively the registered on-detach
/// callbacks (which may `ereport(ERROR)`, exactly as in C, where that
/// longjmps out with the remaining detach work left for error recovery).
pub fn dsm_detach(seg: DsmSegmentId) -> PgResult<()> {
    // Invoke registered callbacks, popping each before invoking it so that a
    // callback error that brings us back here cannot recurse infinitely.
    // Interrupts are held while running callbacks in non-error paths so
    // statement timeout etc. can't leave cleanup unfinished.
    backend_utils_init_small_seams::hold_interrupts::call();
    loop {
        let cb = with_desc(seg, |d| d.on_detach.pop());
        match cb {
            Some(cb) => (cb.function)(seg, cb.arg)?,
            None => break,
        }
    }
    backend_utils_init_small_seams::resume_interrupts::call();

    // Try to remove the mapping, if one exists (it might not, if we failed
    // partway through create/attach). Remove the mapping before decrementing
    // the reference count, so whoever sees a zero count knows no mappings
    // remain; even if unmapping fails, pretend it worked — retrying is
    // likely to fail the same way.
    let (handle, mapped_address) = with_desc(seg, |d| (d.handle, d.mapped_address));
    if !mapped_address.is_null() {
        if !is_main_region_dsm_handle(handle) {
            let (mut ip, mut ma, mut ms) =
                with_desc(seg, |d| (d.impl_private, d.mapped_address, d.mapped_size));
            let _ = dsm_impl_op(
                DsmOp::Detach,
                handle,
                0,
                &mut ip,
                &mut ma,
                &mut ms,
                WARNING,
            );
        }
        with_desc(seg, |d| {
            d.impl_private = DsmImplPrivate::None;
            d.mapped_address = std::ptr::null_mut();
            d.mapped_size = 0;
        });
    }

    // Reduce reference count, if we previously increased it.
    let control_slot = with_desc(seg, |d| d.control_slot);
    if control_slot != INVALID_CONTROL_SLOT {
        let control_lock = acquire_control_lock()?;
        let control = control();
        // Assert(item.handle == seg->handle); Assert(item.refcnt > 1).
        let refcnt = unsafe {
            let item = control_item(control, control_slot);
            (*item).refcnt -= 1;
            (*item).refcnt
        };
        with_desc(seg, |d| d.control_slot = INVALID_CONTROL_SLOT);
        control_lock.release()?;

        // If new reference count is 1, try to destroy the segment. If we
        // fail (or are killed first) the count stays 1 and nobody else can
        // attach; postmaster shutdown or the next postmaster start makes
        // another removal attempt.
        if refcnt == 1 {
            // A pinned segment should never reach 1.

            let destroyed = if is_main_region_dsm_handle(handle) {
                true
            } else {
                let (mut ip, mut ma, mut ms) =
                    with_desc(seg, |d| (d.impl_private, d.mapped_address, d.mapped_size));
                let destroyed = dsm_impl_op(
                    DsmOp::Destroy,
                    handle,
                    0,
                    &mut ip,
                    &mut ma,
                    &mut ms,
                    WARNING,
                );
                with_desc(seg, |d| {
                    d.impl_private = ip;
                    d.mapped_address = ma;
                    d.mapped_size = ms;
                });
                destroyed.unwrap_or(false)
            };
            if destroyed {
                let control_lock = acquire_control_lock()?;
                unsafe {
                    let item = control_item(control, control_slot);
                    if is_main_region_dsm_handle(handle) {
                        free_page_manager_put::call(
                            main_space_fpm(),
                            (*item).first_page,
                            (*item).npages,
                        );
                    }
                    // Assert(item.handle == seg->handle && item.refcnt == 1).
                    (*item).refcnt = 0;
                }
                control_lock.release()?;
            }
        }
    }

    // Clean up our remaining backend-private data structures.
    remove_descriptor(seg);
    Ok(())
}

// ---------------------------------------------------------------------------
// Pin / unpin.
// ---------------------------------------------------------------------------

/// `dsm_pin_mapping(dsm_segment *seg)` — keep the mapping until end of
/// session. Consumes the guard (the C `resowner = NULL` +
/// `ResourceOwnerForgetDSM`); the segment stays attached until an explicit
/// [`dsm_detach`] or session end.
pub fn dsm_pin_mapping(seg: DsmSegment) -> DsmSegmentId {
    seg.into_id()
}

/// `dsm_unpin_mapping(dsm_segment *seg)` — reverse [`dsm_pin_mapping`]: make
/// the mapping owned by the current scope again (the returned guard detaches
/// on drop). Useful just before an operation that will invalidate the
/// segment for this backend.
pub fn dsm_unpin_mapping(seg: DsmSegmentId) -> DsmSegment {
    // Assert(seg->resowner == NULL); ResourceOwnerEnlarge + RememberDSM.
    with_desc(seg, |_| ()); // trap stale ids, as the C deref would
    DsmSegment { id: seg }
}

/// `dsm_pin_segment(dsm_segment *seg)` — keep the *segment* (not this
/// mapping) until postmaster shutdown or [`dsm_unpin_segment`]. Must not be
/// called more than once per segment without an intervening unpin.
pub fn dsm_pin_segment(seg: DsmSegmentId) -> PgResult<()> {
    let (handle, control_slot, impl_private) =
        with_desc(seg, |d| (d.handle, d.control_slot, d.impl_private));

    // Bump the reference count in shared memory, so the segment survives
    // even with no attached session. (On the ERROR path the guard's drop
    // releases the control lock — where C leaves it for error recovery's
    // LWLockReleaseAll.)
    let control_lock = acquire_control_lock()?;
    let control = control();
    let item = unsafe { control_item(control, control_slot) };
    if unsafe { (*item).pinned } {
        elog(ERROR, "cannot pin a segment that is already pinned")?;
    }
    let pm_handle = if !is_main_region_dsm_handle(handle) {
        dsm_impl_pin_segment(handle, &impl_private)
    } else {
        0
    };
    unsafe {
        (*item).pinned = true;
        (*item).refcnt += 1;
        (*item).impl_private_pm_handle = pm_handle;
    }
    control_lock.release()?;
    Ok(())
}

/// `dsm_unpin_segment(dsm_handle handle)` — unpin a segment previously
/// pinned with [`dsm_pin_segment`]. Takes a handle rather than a segment so
/// a segment can be unpinned without being attached.
pub fn dsm_unpin_segment(handle: dsm_handle) -> PgResult<()> {
    let mut control_slot = INVALID_CONTROL_SLOT;
    let mut destroy = false;

    // Find the control slot for the given handle.
    let control_lock = acquire_control_lock()?;
    let control = control();
    let nitems = unsafe { (*control).nitems };
    for i in 0..nitems {
        let item = unsafe { control_item(control, i) };

        // Skip unused slots and segments that are concurrently going away.
        if unsafe { (*item).refcnt } <= 1 {
            continue;
        }

        // If we've found our handle, we can stop searching.
        if unsafe { (*item).handle } == handle {
            control_slot = i;
            break;
        }
    }

    // We should have found the slot, and it should not already be going
    // away, because this is only called on a pinned segment. (On these ERROR
    // paths the guard's drop releases the control lock — where C leaves it
    // for error recovery.)
    if control_slot == INVALID_CONTROL_SLOT {
        elog(ERROR, "cannot unpin unknown segment handle")?;
    }
    let item = unsafe { control_item(control, control_slot) };
    if !unsafe { (*item).pinned } {
        elog(ERROR, "cannot unpin a segment that is not pinned")?;
    }
    // Assert(item.refcnt > 1).

    // Allow implementation-specific code to run before releasing the lock,
    // because it may modify impl_private_pm_handle.
    if !is_main_region_dsm_handle(handle) {
        unsafe {
            dsm_impl_unpin_segment(handle, &mut (*item).impl_private_pm_handle);
        }
    }

    // Note that 1 means no references (0 means unused slot).
    unsafe {
        (*item).refcnt -= 1;
        if (*item).refcnt == 1 {
            destroy = true;
        }
        (*item).pinned = false;
    }

    // Now we can release the lock.
    control_lock.release()?;

    // Clean up resources if that was the last reference. The current
    // process certainly has no mapping (the count would still exceed 1
    // otherwise), so passing NULL mapping state is OK.
    if destroy {
        let mut junk_impl_private = DsmImplPrivate::None;
        let mut junk_mapped_address: *mut u8 = std::ptr::null_mut();
        let mut junk_mapped_size: usize = 0;

        let destroyed = if is_main_region_dsm_handle(handle) {
            true
        } else {
            dsm_impl_op(
                DsmOp::Destroy,
                handle,
                0,
                &mut junk_impl_private,
                &mut junk_mapped_address,
                &mut junk_mapped_size,
                WARNING,
            )
            .unwrap_or(false)
        };
        if destroyed {
            let control_lock = acquire_control_lock()?;
            unsafe {
                let item = control_item(control, control_slot);
                if is_main_region_dsm_handle(handle) {
                    free_page_manager_put::call(
                        main_space_fpm(),
                        (*item).first_page,
                        (*item).npages,
                    );
                }
                // Assert(item.handle == handle && item.refcnt == 1).
                (*item).refcnt = 0;
            }
            control_lock.release()?;
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Lookups / accessors.
// ---------------------------------------------------------------------------

/// `dsm_find_mapping(dsm_handle handle)` — find this backend's existing
/// mapping for a segment, if any.
pub fn dsm_find_mapping(handle: dsm_handle) -> Option<DsmSegmentId> {
    DSM_SEGMENT_LIST.with(|list| {
        list.borrow()
            .as_ref()
            .and_then(|l| l.iter().find(|desc| desc.handle == handle).map(|desc| desc.id))
    })
}

/// `dsm_segment_address(dsm_segment *seg)` — the address at which the
/// segment is mapped.
pub fn dsm_segment_address(seg: DsmSegmentId) -> *mut u8 {
    with_desc(seg, |d| {
        debug_assert!(!d.mapped_address.is_null());
        d.mapped_address
    })
}

/// `dsm_segment_map_length(dsm_segment *seg)` — the size of the mapping.
pub fn dsm_segment_map_length(seg: DsmSegmentId) -> Size {
    with_desc(seg, |d| {
        debug_assert!(!d.mapped_address.is_null());
        d.mapped_size
    })
}

/// `dsm_segment_handle(dsm_segment *seg)` — the handle, for passing to
/// another backend that will [`dsm_attach`].
pub fn dsm_segment_handle(seg: DsmSegmentId) -> dsm_handle {
    with_desc(seg, |d| d.handle)
}

// ---------------------------------------------------------------------------
// On-detach callbacks.
// ---------------------------------------------------------------------------

/// `on_dsm_detach(seg, function, arg)` — register an on-detach callback.
/// The C callback record is `MemoryContextAlloc`'d in `TopMemoryContext`;
/// `mcx` is that handle, and the `Err` is the allocation's OOM surface.
pub fn on_dsm_detach(
    seg: DsmSegmentId,
    function: OnDsmDetachCallback,
    arg: Datum,
    mcx: Mcx<'static>,
) -> PgResult<()> {
    with_desc(seg, |d| {
        d.on_detach
            .try_reserve(1)
            .map_err(|_| mcx.oom(std::mem::size_of::<DetachCallback>()))?;
        // slist_push_head — newest at the back of the Vec.
        d.on_detach.push(DetachCallback { function, arg });
        Ok(())
    })
}

/// `cancel_on_dsm_detach(seg, function, arg)` — unregister an on-detach
/// callback (first match walking newest-first, as the C slist walk from the
/// head).
pub fn cancel_on_dsm_detach(seg: DsmSegmentId, function: OnDsmDetachCallback, arg: Datum) {
    with_desc(seg, |d| {
        if let Some(pos) = d
            .on_detach
            .iter()
            .rposition(|cb| cb.function as usize == function as usize && cb.arg == arg)
        {
            d.on_detach.remove(pos);
        }
    });
}

/// `reset_on_dsm_detach` — discard all registered on-detach callbacks
/// without executing them, and forget the control slots so a later detach
/// won't decrement the shared reference counts (the implicit on-detach
/// action). Called after fork via `on_exit_reset`.
pub fn reset_on_dsm_detach() {
    DSM_SEGMENT_LIST.with(|list| {
        if let Some(l) = list.borrow_mut().as_mut() {
            for desc in l.iter_mut() {
                // Throw away explicit on-detach actions.
                desc.on_detach.clear();
                // Decrementing the reference count is a sort of implicit
                // on-detach action; don't do that either.
                desc.control_slot = INVALID_CONTROL_SLOT;
            }
        }
    });
}

// ---------------------------------------------------------------------------
// Descriptor lifecycle.
// ---------------------------------------------------------------------------

/// `dsm_create_descriptor` — allocate and link a new backend-local segment
/// descriptor in `mcx` (C: `MemoryContextAlloc(TopMemoryContext,
/// sizeof(dsm_segment))`). The returned guard is the resource-owner
/// association (`ResourceOwnerRememberDSM`); the `Err` is that allocation's
/// OOM surface, produced by `mcx.oom`.
fn dsm_create_descriptor(mcx: Mcx<'static>) -> PgResult<DsmSegment> {
    DSM_SEGMENT_LIST.with(|list| {
        let mut list = list.borrow_mut();
        let list = list.get_or_insert_with(|| PgVec::new_in(mcx));
        list.try_reserve(1)
            .map_err(|_| mcx.oom(std::mem::size_of::<DsmSegmentDesc>()))?;

        let id = DSM_NEXT_ID.with(|c| {
            let id = c.get();
            c.set(id + 1);
            DsmSegmentId(id)
        });

        // dlist_push_head — newest at the back of the Vec.
        list.push(DsmSegmentDesc {
            id,
            // seg->handle must be initialized by the caller.
            handle: DSM_HANDLE_INVALID,
            control_slot: INVALID_CONTROL_SLOT,
            impl_private: DsmImplPrivate::None,
            mapped_address: std::ptr::null_mut(),
            mapped_size: 0,
            on_detach: PgVec::new_in(mcx),
        });

        Ok(DsmSegment { id })
    })
}

/// The shared tail of `dsm_detach` (`dlist_delete` + `pfree`).
fn remove_descriptor(seg: DsmSegmentId) {
    DSM_SEGMENT_LIST.with(|list| {
        let mut list = list.borrow_mut();
        if let Some(l) = list.as_mut() {
            if let Some(pos) = l.iter().position(|desc| desc.id == seg) {
                l.remove(pos);
            }
        }
    });
}

/// The `dsm_create` too-many-segments error path: `ResourceOwnerForgetDSM` +
/// `dlist_delete` + `pfree`, with no detach work.
fn destroy_descriptor(seg: DsmSegment) {
    let id = seg.into_id();
    remove_descriptor(id);
}
