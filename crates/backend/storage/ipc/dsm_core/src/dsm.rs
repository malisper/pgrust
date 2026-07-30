use std::cell::{Cell, RefCell};
use std::mem::ManuallyDrop;
use std::sync::atomic::{AtomicPtr, AtomicU32, AtomicUsize, Ordering};

use elog::{elog, ereport};
use init_small::globals;
use lwlock::{main_lock, LWLock, LWLockAcquire, LWLockRelease, LW_EXCLUSIVE};
use mcx::{bind, Mcx, McxOwned, MemoryContext, PgVec};
use types_core::Size;
use types_error::{
    ErrorLocation, PgResult, DEBUG1, DEBUG2, ERRCODE_INSUFFICIENT_RESOURCES, ERROR, LOG, WARNING,
};
use types_storage::{
    dsm_handle, PGShmemHeader, DSM_HANDLE_INVALID, DYNAMIC_SHARED_MEMORY_CONTROL_LOCK,
};

use crate::dsm_impl::{dsm_impl_op, dsm_impl_pin_segment, dsm_impl_unpin_segment, DsmOp};

pub const PG_DYNSHMEM_CONTROL_MAGIC: u32 = 0x9a50_3d32;
pub const PG_DYNSHMEM_FIXED_SLOTS: i32 = 64;
pub const PG_DYNSHMEM_SLOTS_PER_BACKEND: i32 = 5;
pub const INVALID_CONTROL_SLOT: u32 = u32::MAX;
pub const DSM_CREATE_NULL_IF_MAXSEGMENTS: i32 = 0x0001;

fn loc(funcname: &str) -> ErrorLocation {
    ErrorLocation::new(file!(), line!() as i32, funcname)
}

#[repr(C)]
struct DsmControlItem {
    handle: dsm_handle,
    /// 2+ = active, 1 = moribund, 0 = gone.
    refcnt: u32,
    first_page: usize,
    npages: usize,
    pinned: bool,
}

#[repr(C)]
struct DsmControlHeader {
    magic: u32,
    nitems: u32,
    maxitems: u32,
}

const ITEM_OFFSET: usize = {
    let a = std::mem::align_of::<DsmControlItem>();
    (std::mem::size_of::<DsmControlHeader>() + a - 1) / a * a
};

unsafe fn control_item(control: *mut DsmControlHeader, i: u32) -> *mut DsmControlItem {
    ((control as *mut u8).add(ITEM_OFFSET) as *mut DsmControlItem).add(i as usize)
}

pub type OnDsmDetachCallback = fn(DsmSegmentId, usize) -> PgResult<()>;

#[derive(Clone, Copy)]
struct DetachCallback {
    function: OnDsmDetachCallback,
    arg: usize,
}

struct DsmSegmentDesc<'mcx> {
    id: u64,
    handle: dsm_handle,
    control_slot: u32,
    mapped_address: *mut u8,
    mapped_size: usize,
    /// slist LIFO: newest at the back.
    on_detach: PgVec<'mcx, DetachCallback>,
}

struct DsmState<'mcx> {
    mcx: Mcx<'mcx>,
    next_id: u64,
    /// dlist head = back of the Vec (newest first from the back).
    segs: PgVec<'mcx, DsmSegmentDesc<'mcx>>,
}

bind!(DsmStateTy => DsmState<'mcx>);

thread_local! {
    static DSM_INIT_DONE: Cell<bool> = const { Cell::new(false) };
    // ManuallyDrop keeps the TLS payload !needs_drop; C's descriptors live in
    // TopMemoryContext for the backend's whole life anyway.
    static STATE: RefCell<Option<ManuallyDrop<McxOwned<DsmStateTy>>>> =
        const { RefCell::new(None) };
}

fn with_state<R>(f: impl for<'mcx> FnOnce(&mut DsmState<'mcx>) -> R) -> R {
    STATE.with(|cell| {
        let mut slot = cell.borrow_mut();
        if slot.is_none() {
            let owned = McxOwned::<DsmStateTy>::try_new(MemoryContext::new("DsmSegments"), |mcx| {
                Ok(DsmState {
                    mcx,
                    next_id: 1,
                    segs: PgVec::new_in(mcx),
                })
            })
            .expect("DsmSegments context allocation");
            *slot = Some(ManuallyDrop::new(owned));
            // Session-memory teardown (FPBUDGET-1): freed at clean task end
            // (segments themselves detach via the exit-callback stack, which
            // runs before teardown).
            ::mcx::register_session_cleanup(Box::new(|| {
                STATE.with(|cell| {
                    if let Some(owned) = cell.borrow_mut().take() {
                        drop(ManuallyDrop::into_inner(owned));
                    }
                });
            }));
        }
        slot.as_mut().unwrap().with_mut(f)
    })
}

/// Panics on a stale id, where the C would dereference freed memory.
fn with_desc<R>(id: DsmSegmentId, f: impl for<'mcx> FnOnce(&mut DsmSegmentDesc<'mcx>) -> R) -> R {
    with_state(|st| {
        let desc = st
            .segs
            .iter_mut()
            .find(|d| d.id == id.0)
            .expect("dsm: use of unknown or detached segment id");
        f(desc)
    })
}

// C's per-process control globals are fork-inherited copies of one value;
// with a single process they are process statics shared by all backends.
static DSM_CONTROL: AtomicPtr<DsmControlHeader> = AtomicPtr::new(std::ptr::null_mut());
static DSM_CONTROL_HANDLE: AtomicU32 = AtomicU32::new(0);
static DSM_CONTROL_MAPPED_SIZE: AtomicUsize = AtomicUsize::new(0);
static DSM_MAIN_SPACE_BEGIN: AtomicPtr<u8> = AtomicPtr::new(std::ptr::null_mut());
// Boot shim retained so the crash-cycle re-create reuses it (ipci leaks it).
static DSM_STARTUP_SHIM: AtomicPtr<PGShmemHeader> = AtomicPtr::new(std::ptr::null_mut());

fn control() -> *mut DsmControlHeader {
    DSM_CONTROL.load(Ordering::Acquire)
}

/// The C `dsm_segment *`: a stable identity, never reused after detach.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct DsmSegmentId(u64);

impl DsmSegmentId {
    #[inline]
    pub fn as_u64(self) -> u64 {
        self.0
    }

    #[inline]
    pub fn from_u64(v: u64) -> Self {
        DsmSegmentId(v)
    }
}

/// `seg->resowner` as an RAII guard (docs/no-drop.md): RememberDSM =
/// construction, ForgetDSM = [`Self::into_id`], ResOwnerReleaseDSM = `Drop`.
pub struct DsmSegment {
    id: DsmSegmentId,
}

impl DsmSegment {
    pub fn id(&self) -> DsmSegmentId {
        self.id
    }

    pub fn into_id(self) -> DsmSegmentId {
        let id = self.id;
        std::mem::forget(self);
        id
    }
}

impl Drop for DsmSegment {
    fn drop(&mut self) {
        let live = with_state(|st| st.segs.iter().any(|d| d.id == self.id.0));
        if live {
            // A detach-callback ERROR cannot propagate out of Drop; demote.
            if let Err(e) = dsm_detach(self.id) {
                let _ = elog(
                    WARNING,
                    format!(
                        "error ignored while detaching dynamic shared memory segment: {}",
                        e.message
                    ),
                );
            }
        }
    }
}

impl std::fmt::Debug for DsmSegment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "dynamic shared memory segment {}", dsm_segment_handle(self.id))
    }
}

struct ControlLockGuard {
    lock: &'static LWLock,
    released: bool,
}

fn acquire_control_lock() -> PgResult<ControlLockGuard> {
    let lock = main_lock(DYNAMIC_SHARED_MEMORY_CONTROL_LOCK);
    LWLockAcquire(lock, LW_EXCLUSIVE, globals::MyProcNumber())?;
    Ok(ControlLockGuard {
        lock,
        released: false,
    })
}

impl ControlLockGuard {
    fn release(mut self) -> PgResult<()> {
        self.released = true;
        LWLockRelease(self.lock)
    }
}

impl Drop for ControlLockGuard {
    // Abort path: C error recovery's LWLockReleaseAll.
    fn drop(&mut self) {
        if !self.released {
            let _ = LWLockRelease(self.lock);
        }
    }
}

fn prng_u32() -> u32 {
    pg_prng_seams::global_prng_uint32::call()
}

#[inline]
fn pg_leftmost_one_pos32(word: u32) -> i32 {
    debug_assert!(word != 0);
    31 - word.leading_zeros() as i32
}

/// Main-region pseudo-segment handles are odd.
#[inline]
fn is_main_region_dsm_handle(handle: dsm_handle) -> bool {
    handle & 1 != 0
}

#[allow(dead_code)]
fn make_main_region_dsm_handle(slot: i32) -> dsm_handle {
    let maxitems = unsafe { (*control()).maxitems };
    let mut handle: dsm_handle = 1;
    handle |= (slot << 1) as dsm_handle;
    handle |= prng_u32() << (pg_leftmost_one_pos32(maxitems) + 1);
    handle
}

#[cold]
fn main_region_unported() -> ! {
    panic!("dsm: main-region segments (min_dynamic_shared_memory) unported: utils/mmgr/freepage.c")
}

fn dsm_control_bytes_needed(nitems: u32) -> u64 {
    ITEM_OFFSET as u64 + std::mem::size_of::<DsmControlItem>() as u64 * nitems as u64
}

fn dsm_control_segment_sane(control: *mut DsmControlHeader, mapped_size: usize) -> bool {
    if mapped_size < ITEM_OFFSET {
        return false;
    }
    let (magic, nitems, maxitems) =
        unsafe { ((*control).magic, (*control).nitems, (*control).maxitems) };
    if magic != PG_DYNSHMEM_CONTROL_MAGIC {
        return false;
    }
    if dsm_control_bytes_needed(maxitems) > mapped_size as u64 {
        return false;
    }
    if nitems > maxitems {
        return false;
    }
    true
}

/// The C mmap-arm leftover scan (dsm_cleanup_for_mmap) has no counterpart
/// here: the in-process backing writes no files.
pub fn dsm_postmaster_startup(shim: *mut PGShmemHeader) -> PgResult<()> {
    let maxitems =
        (PG_DYNSHMEM_FIXED_SLOTS + PG_DYNSHMEM_SLOTS_PER_BACKEND * globals::MaxBackends()) as u32;
    elog(
        DEBUG2,
        format!("dynamic shared memory system will support {maxitems} segments"),
    )?;
    let segsize = dsm_control_bytes_needed(maxitems) as usize;

    let mut control_handle: dsm_handle;
    let mut control_address: *mut u8 = std::ptr::null_mut();
    let mut control_mapped_size: usize = 0;
    loop {
        // Even numbers only; DSM_HANDLE_INVALID is reserved.
        control_handle = prng_u32() << 1;
        if control_handle == DSM_HANDLE_INVALID {
            continue;
        }
        if dsm_impl_op(
            DsmOp::Create,
            control_handle,
            segsize,
            &mut control_address,
            &mut control_mapped_size,
            ERROR,
        )? {
            break;
        }
    }
    DSM_CONTROL_HANDLE.store(control_handle, Ordering::Relaxed);
    DSM_CONTROL_MAPPED_SIZE.store(control_mapped_size, Ordering::Relaxed);
    let control = control_address as *mut DsmControlHeader;
    DSM_CONTROL.store(control, Ordering::Release);

    ipc_seams::on_shmem_exit::call(dsm_postmaster_shutdown, shim as usize);
    elog(
        DEBUG2,
        format!("created dynamic shared memory control segment {control_handle} ({segsize} bytes)"),
    )?;
    unsafe {
        (*shim).dsm_control = control_handle;
        (*control).magic = PG_DYNSHMEM_CONTROL_MAGIC;
        (*control).nitems = 0;
        (*control).maxitems = maxitems;
    }
    DSM_STARTUP_SHIM.store(shim, Ordering::Release);
    Ok(())
}

/// Crash-cycle re-create of the control segment
/// (notes/crash-restart-design.md): shmem_exit(1) already ran
/// dsm_postmaster_shutdown (destroying the old segment and popping its exit
/// callback), so this re-runs the boot startup against the retained shim.
pub fn dsm_postmaster_startup_after_crash() -> PgResult<()> {
    let shim = DSM_STARTUP_SHIM.load(Ordering::Acquire);
    assert!(
        !shim.is_null(),
        "dsm_postmaster_startup_after_crash before dsm_postmaster_startup"
    );
    assert!(
        control().is_null(),
        "dsm control segment still mapped; shmem_exit(1) must run first"
    );
    dsm_postmaster_startup(shim)
}

pub fn dsm_cleanup_using_control_segment(old_control_handle: dsm_handle) -> PgResult<()> {
    let mut mapped_address: *mut u8 = std::ptr::null_mut();
    let mut mapped_size: usize = 0;
    let mut junk_mapped_address: *mut u8 = std::ptr::null_mut();
    let mut junk_mapped_size: usize = 0;

    if !dsm_impl_op(
        DsmOp::Attach,
        old_control_handle,
        0,
        &mut mapped_address,
        &mut mapped_size,
        DEBUG1,
    )? {
        return Ok(());
    }

    let old_control = mapped_address as *mut DsmControlHeader;
    if !dsm_control_segment_sane(old_control, mapped_size) {
        let _ = dsm_impl_op(
            DsmOp::Detach,
            old_control_handle,
            0,
            &mut mapped_address,
            &mut mapped_size,
            LOG,
        )?;
        return Ok(());
    }

    let nitems = unsafe { (*old_control).nitems };
    for i in 0..nitems {
        let (refcnt, handle) = unsafe {
            let item = control_item(old_control, i);
            ((*item).refcnt, (*item).handle)
        };
        if refcnt == 0 {
            continue;
        }
        if is_main_region_dsm_handle(handle) {
            continue;
        }
        elog(
            DEBUG2,
            format!(
                "cleaning up orphaned dynamic shared memory with ID {handle} (reference count {refcnt})"
            ),
        )?;
        let _ = dsm_impl_op(
            DsmOp::Destroy,
            handle,
            0,
            &mut junk_mapped_address,
            &mut junk_mapped_size,
            LOG,
        )?;
    }

    elog(
        DEBUG2,
        format!("cleaning up dynamic shared memory control segment with ID {old_control_handle}"),
    )?;
    let _ = dsm_impl_op(
        DsmOp::Destroy,
        old_control_handle,
        0,
        &mut mapped_address,
        &mut mapped_size,
        LOG,
    )?;
    Ok(())
}

fn dsm_postmaster_shutdown(_code: i32, arg: usize) {
    let mut junk_mapped_address: *mut u8 = std::ptr::null_mut();
    let mut junk_mapped_size: usize = 0;
    let shim = arg as *mut PGShmemHeader;

    let control = control();
    if !dsm_control_segment_sane(control, DSM_CONTROL_MAPPED_SIZE.load(Ordering::Relaxed)) {
        let _ = ereport(LOG)
            .errmsg("dynamic shared memory control segment is corrupt")
            .finish(loc("dsm_postmaster_shutdown"));
        return;
    }

    let nitems = unsafe { (*control).nitems };
    for i in 0..nitems {
        let (refcnt, handle) = unsafe {
            let item = control_item(control, i);
            ((*item).refcnt, (*item).handle)
        };
        if refcnt == 0 {
            continue;
        }
        if is_main_region_dsm_handle(handle) {
            continue;
        }
        let _ = elog(
            DEBUG2,
            format!("cleaning up orphaned dynamic shared memory with ID {handle}"),
        );
        let _ = dsm_impl_op(
            DsmOp::Destroy,
            handle,
            0,
            &mut junk_mapped_address,
            &mut junk_mapped_size,
            LOG,
        );
    }

    let control_handle = DSM_CONTROL_HANDLE.load(Ordering::Relaxed);
    let _ = elog(
        DEBUG2,
        format!("cleaning up dynamic shared memory control segment with ID {control_handle}"),
    );
    let mut control_address = control as *mut u8;
    let mut mapped_size = DSM_CONTROL_MAPPED_SIZE.load(Ordering::Relaxed);
    let _ = dsm_impl_op(
        DsmOp::Destroy,
        control_handle,
        0,
        &mut control_address,
        &mut mapped_size,
        LOG,
    );
    DSM_CONTROL_MAPPED_SIZE.store(mapped_size, Ordering::Relaxed);
    DSM_CONTROL.store(control_address as *mut DsmControlHeader, Ordering::Release);
    unsafe {
        (*shim).dsm_control = 0;
    }
}

// The EXEC_BACKEND re-mapping branch of dsm_backend_startup and
// dsm_set_control_handle are not applicable (no exec, no fork).
fn dsm_backend_startup() {
    DSM_INIT_DONE.with(|c| c.set(true));
}

pub fn dsm_estimate_size() -> usize {
    1024 * 1024 * crate::dsm_impl::min_dynamic_shared_memory() as usize
}

pub fn dsm_shmem_init() -> PgResult<()> {
    if dsm_estimate_size() == 0 {
        return Ok(());
    }
    main_region_unported()
}

/// `Ok(None)` only under `DSM_CREATE_NULL_IF_MAXSEGMENTS` when full. The
/// guard is the C CurrentResourceOwner association; [`dsm_pin_mapping`] is
/// the NULL-resowner (session-lifetime) behavior.
pub fn dsm_create(size: Size, flags: i32) -> PgResult<Option<DsmSegment>> {
    if !DSM_INIT_DONE.with(|c| c.get()) {
        dsm_backend_startup();
    }

    let seg = dsm_create_descriptor()?;
    let id = seg.id();

    if !DSM_MAIN_SPACE_BEGIN.load(Ordering::Relaxed).is_null() {
        main_region_unported();
    }

    loop {
        // Even numbers only; DSM_HANDLE_INVALID is reserved.
        let handle: dsm_handle = prng_u32() << 1;
        if handle == DSM_HANDLE_INVALID {
            continue;
        }
        with_desc(id, |d| d.handle = handle);
        let mut ma: *mut u8 = std::ptr::null_mut();
        let mut ms: usize = 0;
        let created = dsm_impl_op(DsmOp::Create, handle, size, &mut ma, &mut ms, ERROR);
        with_desc(id, |d| {
            d.mapped_address = ma;
            d.mapped_size = ms;
        });
        if created? {
            break;
        }
    }

    let control_lock = acquire_control_lock()?;
    let control = control();
    let nitems = unsafe { (*control).nitems };
    for i in 0..nitems {
        let item = unsafe { control_item(control, i) };
        if unsafe { (*item).refcnt } == 0 {
            let handle = with_desc(id, |d| d.handle);
            debug_assert!(!is_main_region_dsm_handle(handle));
            unsafe {
                (*item).handle = handle;
                // refcnt of 1 triggers destruction, so start at 2.
                (*item).refcnt = 2;
                (*item).pinned = false;
            }
            with_desc(id, |d| d.control_slot = i);
            control_lock.release()?;
            return Ok(Some(seg));
        }
    }

    let maxitems = unsafe { (*control).maxitems };
    if nitems >= maxitems {
        control_lock.release()?;
        let handle = with_desc(id, |d| d.handle);
        let (mut ma, mut ms) = with_desc(id, |d| (d.mapped_address, d.mapped_size));
        let _ = dsm_impl_op(DsmOp::Destroy, handle, 0, &mut ma, &mut ms, WARNING);
        with_desc(id, |d| {
            d.mapped_address = ma;
            d.mapped_size = ms;
        });
        destroy_descriptor(seg);

        if flags & DSM_CREATE_NULL_IF_MAXSEGMENTS != 0 {
            return Ok(None);
        }
        ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_RESOURCES)
            .errmsg("too many dynamic shared memory segments")
            .finish(loc("dsm_create"))?;
        unreachable!();
    }

    unsafe {
        let item = control_item(control, nitems);
        (*item).handle = with_desc(id, |d| d.handle);
        // refcnt of 1 triggers destruction, so start at 2.
        (*item).refcnt = 2;
        (*item).pinned = false;
        (*control).nitems += 1;
    }
    with_desc(id, |d| d.control_slot = nitems);
    control_lock.release()?;
    Ok(Some(seg))
}

/// `Ok(None)` if everyone (including the creator) already detached.
pub fn dsm_attach(h: dsm_handle) -> PgResult<Option<DsmSegment>> {
    if !DSM_INIT_DONE.with(|c| c.get()) {
        dsm_backend_startup();
    }

    // Debugging cross-check, kept always-on as in C.
    if with_state(|st| st.segs.iter().any(|d| d.handle == h)) {
        elog(ERROR, "can't attach the same segment more than once")?;
    }

    let seg = dsm_create_descriptor()?;
    let id = seg.id();
    with_desc(id, |d| d.handle = h);

    let control_lock = acquire_control_lock()?;
    let control = control();
    let nitems = unsafe { (*control).nitems };
    for i in 0..nitems {
        let item = unsafe { control_item(control, i) };
        // refcnt 1 = going away; the same handle value may already be reused
        // by another slot, so keep searching.
        if unsafe { (*item).refcnt } <= 1 {
            continue;
        }
        if unsafe { (*item).handle } != h {
            continue;
        }
        unsafe {
            (*item).refcnt += 1;
        }
        with_desc(id, |d| d.control_slot = i);
        if is_main_region_dsm_handle(h) {
            main_region_unported();
        }
        break;
    }
    control_lock.release()?;

    if with_desc(id, |d| d.control_slot) == INVALID_CONTROL_SLOT {
        dsm_detach(seg.into_id())?;
        return Ok(None);
    }

    let mut ma: *mut u8 = std::ptr::null_mut();
    let mut ms: usize = 0;
    let attached = dsm_impl_op(DsmOp::Attach, h, 0, &mut ma, &mut ms, ERROR);
    with_desc(id, |d| {
        d.mapped_address = ma;
        d.mapped_size = ms;
    });
    attached?;

    Ok(Some(seg))
}

pub fn dsm_backend_shutdown() -> PgResult<()> {
    loop {
        let head = with_state(|st| st.segs.last().map(|d| DsmSegmentId(d.id)));
        match head {
            Some(id) => dsm_detach(id)?,
            None => break,
        }
    }
    Ok(())
}

/// C also unmaps its inherited control-segment mapping here; with one
/// process that mapping is the shared one, so it stays.
pub fn dsm_detach_all() -> PgResult<()> {
    loop {
        let head = with_state(|st| st.segs.last().map(|d| DsmSegmentId(d.id)));
        match head {
            Some(id) => dsm_detach(id)?,
            None => break,
        }
    }
    Ok(())
}

/// The `Err` surface is exclusively the on-detach callbacks (an ERROR there
/// leaves the remaining detach work for error recovery, as in C).
pub fn dsm_detach(seg: DsmSegmentId) -> PgResult<()> {
    // Pop each callback before invoking it so a callback error that re-enters
    // here cannot recurse infinitely; interrupts held as in C.
    globals::HoldInterrupts();
    loop {
        let cb = with_desc(seg, |d| d.on_detach.pop());
        match cb {
            Some(cb) => (cb.function)(seg, cb.arg)?,
            None => break,
        }
    }
    globals::ResumeInterrupts();

    // Remove the mapping before decrementing the refcount, so whoever sees a
    // zero count knows no mappings remain.
    let (handle, mapped_address) = with_desc(seg, |d| (d.handle, d.mapped_address));
    if !mapped_address.is_null() {
        if !is_main_region_dsm_handle(handle) {
            let (mut ma, mut ms) = with_desc(seg, |d| (d.mapped_address, d.mapped_size));
            let _ = dsm_impl_op(DsmOp::Detach, handle, 0, &mut ma, &mut ms, WARNING);
        }
        with_desc(seg, |d| {
            d.mapped_address = std::ptr::null_mut();
            d.mapped_size = 0;
        });
    }

    let control_slot = with_desc(seg, |d| d.control_slot);
    if control_slot != INVALID_CONTROL_SLOT {
        let control_lock = acquire_control_lock()?;
        let control = control();
        let refcnt = unsafe {
            let item = control_item(control, control_slot);
            debug_assert!((*item).handle == handle && (*item).refcnt > 1);
            (*item).refcnt -= 1;
            (*item).refcnt
        };
        with_desc(seg, |d| d.control_slot = INVALID_CONTROL_SLOT);
        control_lock.release()?;

        // If the count is now 1, destroy; on failure the count stays 1 and
        // nobody else can attach (postmaster shutdown retries the removal).
        if refcnt == 1 {
            let destroyed = if is_main_region_dsm_handle(handle) {
                main_region_unported();
            } else {
                let (mut ma, mut ms) = with_desc(seg, |d| (d.mapped_address, d.mapped_size));
                let destroyed = dsm_impl_op(DsmOp::Destroy, handle, 0, &mut ma, &mut ms, WARNING);
                with_desc(seg, |d| {
                    d.mapped_address = ma;
                    d.mapped_size = ms;
                });
                destroyed.unwrap_or(false)
            };
            if destroyed {
                let control_lock = acquire_control_lock()?;
                unsafe {
                    let item = control_item(control, control_slot);
                    debug_assert!((*item).handle == handle && (*item).refcnt == 1);
                    (*item).refcnt = 0;
                }
                control_lock.release()?;
            }
        }
    }

    remove_descriptor(seg);
    Ok(())
}

/// Consumes the resowner guard: the C `seg->resowner = NULL`.
pub fn dsm_pin_mapping(seg: DsmSegment) -> DsmSegmentId {
    seg.into_id()
}

/// Reverse of [`dsm_pin_mapping`]: the returned guard detaches on drop.
pub fn dsm_unpin_mapping(seg: DsmSegmentId) -> DsmSegment {
    with_desc(seg, |_| ());
    DsmSegment { id: seg }
}

pub fn dsm_pin_segment(seg: DsmSegmentId) -> PgResult<()> {
    let (handle, control_slot) = with_desc(seg, |d| (d.handle, d.control_slot));

    let control_lock = acquire_control_lock()?;
    let control = control();
    let item = unsafe { control_item(control, control_slot) };
    if unsafe { (*item).pinned } {
        elog(ERROR, "cannot pin a segment that is already pinned")?;
    }
    if !is_main_region_dsm_handle(handle) {
        dsm_impl_pin_segment(handle);
    }
    unsafe {
        (*item).pinned = true;
        (*item).refcnt += 1;
    }
    control_lock.release()?;
    Ok(())
}

/// Unpin by handle (a segment can be unpinned without being attached).
pub fn dsm_unpin_segment(handle: dsm_handle) -> PgResult<()> {
    let mut control_slot = INVALID_CONTROL_SLOT;
    let mut destroy = false;

    let control_lock = acquire_control_lock()?;
    let control = control();
    let nitems = unsafe { (*control).nitems };
    for i in 0..nitems {
        let item = unsafe { control_item(control, i) };
        if unsafe { (*item).refcnt } <= 1 {
            continue;
        }
        if unsafe { (*item).handle } == handle {
            control_slot = i;
            break;
        }
    }

    if control_slot == INVALID_CONTROL_SLOT {
        elog(ERROR, "cannot unpin unknown segment handle")?;
    }
    let item = unsafe { control_item(control, control_slot) };
    if !unsafe { (*item).pinned } {
        elog(ERROR, "cannot unpin a segment that is not pinned")?;
    }
    debug_assert!(unsafe { (*item).refcnt } > 1);

    if !is_main_region_dsm_handle(handle) {
        dsm_impl_unpin_segment(handle);
    }

    // 1 means no references (0 means unused slot).
    unsafe {
        (*item).refcnt -= 1;
        if (*item).refcnt == 1 {
            destroy = true;
        }
        (*item).pinned = false;
    }
    control_lock.release()?;

    if destroy {
        let mut junk_mapped_address: *mut u8 = std::ptr::null_mut();
        let mut junk_mapped_size: usize = 0;
        let destroyed = if is_main_region_dsm_handle(handle) {
            main_region_unported();
        } else {
            dsm_impl_op(
                DsmOp::Destroy,
                handle,
                0,
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
                debug_assert!((*item).handle == handle && (*item).refcnt == 1);
                (*item).refcnt = 0;
            }
            control_lock.release()?;
        }
    }
    Ok(())
}

pub fn dsm_find_mapping(handle: dsm_handle) -> Option<DsmSegmentId> {
    with_state(|st| {
        st.segs
            .iter()
            .find(|d| d.handle == handle)
            .map(|d| DsmSegmentId(d.id))
    })
}

pub fn dsm_segment_address(seg: DsmSegmentId) -> *mut u8 {
    with_desc(seg, |d| {
        debug_assert!(!d.mapped_address.is_null());
        d.mapped_address
    })
}

pub fn dsm_segment_map_length(seg: DsmSegmentId) -> Size {
    with_desc(seg, |d| {
        debug_assert!(!d.mapped_address.is_null());
        d.mapped_size
    })
}

pub fn dsm_segment_handle(seg: DsmSegmentId) -> dsm_handle {
    with_desc(seg, |d| d.handle)
}

pub fn on_dsm_detach(seg: DsmSegmentId, function: OnDsmDetachCallback, arg: usize) -> PgResult<()> {
    with_state(|st| {
        let desc = st
            .segs
            .iter_mut()
            .find(|d| d.id == seg.0)
            .expect("dsm: use of unknown or detached segment id");
        desc.on_detach
            .try_reserve(1)
            .map_err(|_| Box::new(st.mcx.oom(std::mem::size_of::<DetachCallback>())))?;
        // slist_push_head: newest at the back.
        desc.on_detach.push(DetachCallback { function, arg });
        Ok(())
    })
}

/// First match walking newest-first, as the C slist walk from the head.
pub fn cancel_on_dsm_detach(seg: DsmSegmentId, function: OnDsmDetachCallback, arg: usize) {
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

/// Drop callbacks unrun and forget control slots, so a later detach won't
/// decrement the shared refcounts.
pub fn reset_on_dsm_detach() {
    with_state(|st| {
        for desc in st.segs.iter_mut() {
            desc.on_detach.clear();
            desc.control_slot = INVALID_CONTROL_SLOT;
        }
    });
}

fn dsm_create_descriptor() -> PgResult<DsmSegment> {
    with_state(|st| {
        st.segs
            .try_reserve(1)
            .map_err(|_| Box::new(st.mcx.oom(std::mem::size_of::<DsmSegmentDesc<'static>>())))?;
        let id = st.next_id;
        st.next_id += 1;
        let on_detach = PgVec::new_in(st.mcx);
        st.segs.push(DsmSegmentDesc {
            id,
            handle: DSM_HANDLE_INVALID,
            control_slot: INVALID_CONTROL_SLOT,
            mapped_address: std::ptr::null_mut(),
            mapped_size: 0,
            on_detach,
        });
        Ok(DsmSegment {
            id: DsmSegmentId(id),
        })
    })
}

fn remove_descriptor(seg: DsmSegmentId) {
    with_state(|st| {
        if let Some(pos) = st.segs.iter().position(|d| d.id == seg.0) {
            st.segs.remove(pos);
        }
    });
}

/// The dsm_create too-many-segments error path: forget + delete, no detach.
fn destroy_descriptor(seg: DsmSegment) {
    remove_descriptor(seg.into_id());
}
