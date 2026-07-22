#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use std::cell::{Cell, UnsafeCell};
use std::mem::ManuallyDrop;
use std::rc::Rc;
use std::sync::atomic::Ordering::Relaxed;

use elog::{elog, ereport};
use mcx::{Mcx, MemoryContext, PgVec};
use types_core::{
    CommandId, GlobalVisStateHandle, InvalidTransactionId, Oid, ProcNumber, TransactionId,
    TransactionIdIsNormal, TransactionIdPrecedes,
};
use types_error::{ErrorLocation, PgResult, ERROR, WARNING};
use types_resowner::ResourceOwner;
use types_snapshot::{SnapshotData, SnapshotType};

#[cfg(test)]
mod tests;

// Rc because snapmgr itself refcounts (regd/active counts), rule 2.3.
pub type Snapshot = Rc<SnapshotData<'static>>;

pub use procarray::{RecentXmin, TransactionXmin};

fn loc(funcname: &'static str) -> ErrorLocation {
    ErrorLocation {
        filename: Some("snapmgr.c".into()),
        lineno: 0,
        funcname: Some(funcname.into()),
    }
}

fn TransactionIdFollowsOrEquals(id1: TransactionId, id2: TransactionId) -> bool {
    if !TransactionIdIsNormal(id1) || !TransactionIdIsNormal(id2) {
        return id1 >= id2;
    }
    (id1.wrapping_sub(id2) as i32) >= 0
}

#[cold]
#[inline(never)]
#[allow(dead_code)]
fn unported(what: &str) -> ! {
    panic!("unported callee reached from snapmgr.c: {what}")
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Which {
    Current,
    Secondary,
    Catalog,
}

// Current/SecondarySnapshot alias a static or a registered copy.
#[derive(Clone)]
enum SnapRef {
    Static(Which),
    Copied(Snapshot),
}

struct ActiveSnapshotElt {
    as_snap: Snapshot,
    as_level: i32,
}

struct SnapMgrState {
    mcx: Mcx<'static>,
    current_data: Snapshot,
    secondary_data: Snapshot,
    catalog_data: Snapshot,
    current: Option<SnapRef>,
    secondary: Option<SnapRef>,
    catalog_valid: bool,
    historic: Option<Snapshot>,
    historic_tuplecids: Option<HistoricTupleCids>,
    first_snapshot_set: bool,
    first_xact_snapshot: Option<Snapshot>,
    // Resource-handle owners (Rc payloads): plain-heap Vecs per docs/no-drop.md.
    active: Vec<ActiveSnapshotElt>,
    registered: Vec<Snapshot>,
    // Dead unique copies, xip capacity retained — C's CopySnapshot freelist
    // palloc in TopTransactionContext (rule 7 retained scratch).
    copy_freelist: Vec<Snapshot>,
    exported: Vec<ExportedSnapshot>,
}

struct ExportedSnapshot {
    snapfile: String,
    snapshot: Snapshot,
}

impl SnapMgrState {
    fn static_rc(&self, which: Which) -> &Snapshot {
        match which {
            Which::Current => &self.current_data,
            Which::Secondary => &self.secondary_data,
            Which::Catalog => &self.catalog_data,
        }
    }

    fn resolve_ref<'a>(&'a self, r: &'a SnapRef) -> &'a Snapshot {
        match r {
            SnapRef::Static(w) => self.static_rc(*w),
            SnapRef::Copied(rc) => rc,
        }
    }

    fn resolve(&self, r: &SnapRef) -> Snapshot {
        self.resolve_ref(r).clone()
    }

    fn is_ref_to(&self, r: Option<&SnapRef>, snap: &Snapshot) -> bool {
        r.is_some_and(|r| Rc::ptr_eq(self.resolve_ref(r), snap))
    }
}

thread_local! {
    // ManuallyDrop keeps the TLS payload !needs_drop (fabled-lessons §8).
    // UnsafeCell, not RefCell (rule 10): every with_state closure is a leaf
    // (procarray/xact/resowner callees never re-enter this module);
    // STATE_BUSY enforces in debug.
    static STATE: UnsafeCell<Option<ManuallyDrop<SnapMgrState>>> = const { UnsafeCell::new(None) };
}

#[cfg(debug_assertions)]
thread_local! {
    static STATE_BUSY: Cell<bool> = const { Cell::new(false) };
    static STATIC_REPLACED: Cell<u64> = const { Cell::new(0) };
}

// Nonzero: a held static handle defeated array reuse + the reuse fastpath.
#[cfg(debug_assertions)]
pub fn static_snapshot_replacements() -> u64 {
    STATIC_REPLACED.get()
}

fn new_static_snapshot(mcx: Mcx<'static>) -> Snapshot {
    Rc::new(SnapshotData::sentinel(mcx, SnapshotType::SNAPSHOT_MVCC))
}

#[cold]
#[inline(never)]
fn init_state(slot: &mut Option<ManuallyDrop<SnapMgrState>>) {
    let cx: &'static MemoryContext = ::mcx::session_root("SnapMgr");
    let mcx = cx.mcx();
    *slot = Some(ManuallyDrop::new(SnapMgrState {
        mcx,
        current_data: new_static_snapshot(mcx),
        secondary_data: new_static_snapshot(mcx),
        catalog_data: new_static_snapshot(mcx),
        current: None,
        secondary: None,
        catalog_valid: false,
        historic: None,
        historic_tuplecids: None,
        first_snapshot_set: false,
        first_xact_snapshot: None,
        active: Vec::new(),
        registered: Vec::new(),
        copy_freelist: Vec::new(),
        exported: Vec::new(),
    }));
}

fn with_state<R>(f: impl FnOnce(&mut SnapMgrState) -> R) -> R {
    // Guard module Drop (no-drop.md): a named-loud panic unwinding out of the
    // closure (caught at the main-loop) must clear BUSY, or every later
    // snapmgr call — including abort cleanup — re-panics and the backend
    // spins forever.
    #[cfg(debug_assertions)]
    struct BusyReset;
    #[cfg(debug_assertions)]
    impl Drop for BusyReset {
        fn drop(&mut self) {
            STATE_BUSY.set(false);
        }
    }
    #[cfg(debug_assertions)]
    let _busy = {
        assert!(!STATE_BUSY.replace(true), "with_state re-entered");
        BusyReset
    };
    // SAFETY: closures are leaves (no re-entry into this module); guarded in
    // debug builds by STATE_BUSY.
    STATE.with(|cell| unsafe {
        let slot = &mut *cell.get();
        if slot.is_none() {
            init_state(slot);
        }
        f(slot.as_mut().unwrap())
    })
}

fn my_proc_xmin() -> TransactionId {
    lmgr_proc::GetPGProcByNumber(lmgr_proc::MyProc().expect("snapmgr requires MyProc"))
        .xmin
        .read()
}

fn set_my_proc_xmin(xmin: TransactionId) {
    lmgr_proc::GetPGProcByNumber(lmgr_proc::MyProc().expect("snapmgr requires MyProc"))
        .xmin
        .value
        .store(xmin, Relaxed);
}

pub fn FirstSnapshotSet() -> bool {
    with_state(|s| s.first_snapshot_set)
}

// Always refills the SAME persistent struct so snapXactCompletionCount and
// the once-sized xip arrays survive — the reuse fastpath depends on it.
// Seams reached under the borrow never re-enter snapmgr.
fn refill_static_locked(
    s: &mut SnapMgrState,
    which: Which,
    fill: impl FnOnce(&mut SnapshotData<'static>, Mcx<'static>) -> PgResult<()>,
) -> PgResult<Snapshot> {
    let mcx = s.mcx;
    let slot = match which {
        Which::Current => &mut s.current_data,
        Which::Secondary => &mut s.secondary_data,
        Which::Catalog => &mut s.catalog_data,
    };
    let target = match Rc::get_mut(slot) {
        Some(target) => target,
        None => {
            // An outstanding handle aliases the static (C clobbers it);
            // leave the holder a stale copy, refill a fresh struct.
            #[cfg(debug_assertions)]
            STATIC_REPLACED.set(STATIC_REPLACED.get() + 1);
            *slot = new_static_snapshot(mcx);
            Rc::get_mut(slot).expect("fresh Rc is unique")
        }
    };
    fill(target, mcx)?;
    let snap = slot.clone();
    match which {
        Which::Current => s.current = Some(SnapRef::Static(Which::Current)),
        Which::Secondary => s.secondary = Some(SnapRef::Static(Which::Secondary)),
        Which::Catalog => s.catalog_valid = true,
    }
    Ok(snap)
}

fn get_snapshot_data_static_locked(s: &mut SnapMgrState, which: Which) -> PgResult<Snapshot> {
    refill_static_locked(s, which, |target, mcx| {
        procarray::GetSnapshotData(target, mcx)
    })
}

fn get_snapshot_data_static(which: Which) -> PgResult<Snapshot> {
    with_state(|s| get_snapshot_data_static_locked(s, which))
}

pub fn GetTransactionSnapshot() -> PgResult<Snapshot> {
    with_state(|s| {
        if let Some(historic) = &s.historic {
            debug_assert!(!s.first_snapshot_set);
            return Ok(historic.clone());
        }

        if !s.first_snapshot_set {
            invalidate_catalog_snapshot_locked(s);

            debug_assert!(s.registered.is_empty());
            debug_assert!(s.first_xact_snapshot.is_none());

            if xact_seams::is_in_parallel_mode::call() {
                return Err(elog(
                    ERROR,
                    "cannot take query snapshot during a parallel operation",
                )
                .expect_err("elog(ERROR)"));
            }

            // Xact-snapshot mode: the first snapshot must live to end of xact.
            if xact_seams::isolation_uses_xact_snapshot::call() {
                let current = if xact_seams::isolation_is_serializable::call() {
                    refill_static_locked(s, Which::Current, |target, mcx| {
                        predicate_seams::get_serializable_transaction_snapshot::call(target, mcx)
                    })?
                } else {
                    get_snapshot_data_static_locked(s, Which::Current)?
                };
                let copy = copy_snapshot_locked(s, &current);
                copy.regd_count.set(copy.regd_count.get() + 1);
                s.current = Some(SnapRef::Copied(copy.clone()));
                s.first_xact_snapshot = Some(copy.clone());
                s.registered.push(copy.clone());
                s.first_snapshot_set = true;
                return Ok(copy);
            }
            let current = get_snapshot_data_static_locked(s, Which::Current)?;
            s.first_snapshot_set = true;
            return Ok(current);
        }

        if xact_seams::isolation_uses_xact_snapshot::call() {
            let r = s.current.as_ref().expect("CurrentSnapshot != NULL");
            return Ok(s.resolve_ref(r).clone());
        }

        // Don't allow catalog snapshot to be older than xact snapshot.
        invalidate_catalog_snapshot_locked(s);

        get_snapshot_data_static_locked(s, Which::Current)
    })
}

pub fn GetLatestSnapshot() -> PgResult<Snapshot> {
    if xact_seams::is_in_parallel_mode::call() {
        return Err(elog(
            ERROR,
            "cannot update SecondarySnapshot during a parallel operation",
        )
        .expect_err("elog(ERROR)"));
    }

    debug_assert!(!HistoricSnapshotActive());

    if !FirstSnapshotSet() {
        return GetTransactionSnapshot();
    }

    get_snapshot_data_static(Which::Secondary)
}

pub fn GetCatalogSnapshot(relid: Oid) -> PgResult<Snapshot> {
    if let Some(historic) = with_state(|s| s.historic.clone()) {
        return Ok(historic);
    }
    GetNonHistoricCatalogSnapshot(relid)
}

pub fn GetNonHistoricCatalogSnapshot(relid: Oid) -> PgResult<Snapshot> {
    if with_state(|s| s.catalog_valid)
        && !syscache_seams::relation_invalidates_snapshots_only::call(relid)
        && !syscache_seams::relation_has_sys_cache::call(relid)
    {
        InvalidateCatalogSnapshot();
    }

    if !with_state(|s| s.catalog_valid) {
        let catalog = get_snapshot_data_static(Which::Catalog)?;
        // Registered directly (no copy) so it counts for PGPROC->xmin.
        with_state(|s| s.registered.push(catalog));
    }

    Ok(with_state(|s| s.catalog_data.clone()))
}

pub fn InvalidateCatalogSnapshot() {
    with_state(invalidate_catalog_snapshot_locked);
}

fn invalidate_catalog_snapshot_locked(s: &mut SnapMgrState) {
    if s.catalog_valid {
        s.catalog_valid = false;
        let catalog = s.catalog_data.clone();
        registered_remove(s, &catalog);
        snapshot_reset_xmin_locked(s);
    }
}

pub fn InvalidateCatalogSnapshotConditionally() {
    let should = with_state(|s| s.catalog_valid && s.active.is_empty() && s.registered.len() == 1);
    if should {
        InvalidateCatalogSnapshot();
    }
}

pub fn SnapshotSetCommandId(curcid: CommandId) {
    with_state(|s| {
        if !s.first_snapshot_set {
            return;
        }
        if let Some(current) = &s.current {
            s.resolve(current).curcid.set(curcid);
        }
        if let Some(secondary) = &s.secondary {
            s.resolve(secondary).curcid.set(curcid);
        }
        // Should we do the same with CatalogSnapshot? (C leaves this open.)
    });
}

// Single-reserve memcpy append (fabled-lessons §9), retained capacity.
fn copy_xids_into(dst: &mut PgVec<'static, TransactionId>, src: &[TransactionId]) {
    dst.clear();
    dst.reserve(src.len());
    // SAFETY: capacity reserved above; src/dst don't overlap.
    unsafe {
        core::ptr::copy_nonoverlapping(src.as_ptr(), dst.as_mut_ptr(), src.len());
        dst.set_len(src.len());
    }
}

fn copy_snapshot_locked(s: &mut SnapMgrState, src: &SnapshotData<'static>) -> Snapshot {
    let mut rc = match s.copy_freelist.pop() {
        Some(rc) => rc,
        None => Rc::new(SnapshotData::sentinel(s.mcx, src.snapshot_type)),
    };
    let snap = Rc::get_mut(&mut rc).expect("freelist copies are unique");
    snap.snapshot_type = src.snapshot_type;
    snap.xmin = src.xmin;
    snap.xmax = src.xmax;
    copy_xids_into(&mut snap.xip, &src.xip[..src.xcnt as usize]);
    snap.xcnt = src.xcnt;
    // Overflowed subxip is skipped — except in recovery (top xids live there).
    if src.subxcnt > 0 && (!src.suboverflowed || src.takenDuringRecovery) {
        copy_xids_into(&mut snap.subxip, &src.subxip[..src.subxcnt as usize]);
        snap.subxcnt = src.subxcnt;
    } else {
        snap.subxip.clear();
        snap.subxcnt = 0;
    }
    snap.suboverflowed = src.suboverflowed;
    snap.takenDuringRecovery = src.takenDuringRecovery;
    snap.copied = true;
    snap.curcid.set(src.curcid.get());
    snap.speculativeToken = src.speculativeToken;
    snap.vistest = src.vistest;
    snap.active_count.set(0);
    snap.regd_count.set(0);
    snap.snapXactCompletionCount = 0;
    rc
}

// C FreeSnapshot: a dead copy goes back to the freelist, capacity retained.
fn recycle_copy_locked(s: &mut SnapMgrState, snap: Snapshot) {
    debug_assert!(snap.copied);
    debug_assert_eq!(snap.active_count.get(), 0);
    debug_assert_eq!(snap.regd_count.get(), 0);
    if Rc::strong_count(&snap) == 1 {
        s.copy_freelist.push(snap);
    }
}

pub fn CopySnapshot(snapshot: &Snapshot) -> Snapshot {
    with_state(|s| copy_snapshot_locked(s, snapshot))
}

// C's SerializedSnapshotData byte image rendered thread-native: a plain Send
// struct (Vec lengths carry xcnt/subxcnt). vistest crosses too — the handle
// indexes process-global GlobalVis state, valid on any thread.
#[derive(Clone, Debug)]
pub struct SerializedSnapshot {
    pub xmin: TransactionId,
    pub xmax: TransactionId,
    pub xip: Vec<TransactionId>,
    pub subxip: Vec<TransactionId>,
    pub suboverflowed: bool,
    pub takenDuringRecovery: bool,
    pub curcid: CommandId,
    pub vistest: GlobalVisStateHandle,
}

pub fn SerializeSnapshot(snapshot: &Snapshot) -> SerializedSnapshot {
    debug_assert!(snapshot.snapshot_type == SnapshotType::SNAPSHOT_MVCC);
    debug_assert!(snapshot.subxcnt >= 0);
    // Overflowed subxip is skipped — except in recovery (top xids live there).
    let subxip = if snapshot.suboverflowed && !snapshot.takenDuringRecovery {
        Vec::new()
    } else {
        snapshot.subxip[..snapshot.subxcnt.max(0) as usize].to_vec()
    };
    SerializedSnapshot {
        xmin: snapshot.xmin,
        xmax: snapshot.xmax,
        xip: snapshot.xip[..snapshot.xcnt as usize].to_vec(),
        subxip,
        suboverflowed: snapshot.suboverflowed,
        takenDuringRecovery: snapshot.takenDuringRecovery,
        curcid: snapshot.curcid.get(),
        vistest: snapshot.vistest,
    }
}

pub fn RestoreSnapshot(serialized: &SerializedSnapshot) -> Snapshot {
    with_state(|s| {
        let mut rc = match s.copy_freelist.pop() {
            Some(rc) => rc,
            None => Rc::new(SnapshotData::sentinel(s.mcx, SnapshotType::SNAPSHOT_MVCC)),
        };
        let snap = Rc::get_mut(&mut rc).expect("freelist copies are unique");
        snap.snapshot_type = SnapshotType::SNAPSHOT_MVCC;
        snap.xmin = serialized.xmin;
        snap.xmax = serialized.xmax;
        copy_xids_into(&mut snap.xip, &serialized.xip);
        snap.xcnt = serialized.xip.len() as u32;
        copy_xids_into(&mut snap.subxip, &serialized.subxip);
        snap.subxcnt = serialized.subxip.len() as i32;
        snap.suboverflowed = serialized.suboverflowed;
        snap.takenDuringRecovery = serialized.takenDuringRecovery;
        snap.copied = true;
        snap.curcid.set(serialized.curcid);
        snap.speculativeToken = 0;
        snap.vistest = serialized.vistest;
        snap.active_count.set(0);
        snap.regd_count.set(0);
        snap.snapXactCompletionCount = 0;
        rc
    })
}

pub fn RestoreTransactionSnapshot(
    snapshot: &SerializedSnapshot,
    source_proc: ProcNumber,
) -> PgResult<()> {
    SetTransactionSnapshot(snapshot, source_proc)
}

fn SetTransactionSnapshot(sourcesnap: &SerializedSnapshot, source_proc: ProcNumber) -> PgResult<()> {
    with_state(|s| {
        debug_assert!(!s.first_snapshot_set);
        invalidate_catalog_snapshot_locked(s);
        debug_assert!(s.registered.is_empty());
        debug_assert!(s.first_xact_snapshot.is_none());
        debug_assert!(s.historic.is_none());

        // GetSnapshotData still runs: it sizes the xip arrays and updates the
        // GlobalVis state, exactly why C calls it before overwriting fields.
        let current = refill_static_locked(s, Which::Current, |target, mcx| {
            procarray::GetSnapshotData(target, mcx)?;
            debug_assert!(sourcesnap.xip.len() <= procarray::GetMaxSnapshotXidCount());
            debug_assert!(sourcesnap.subxip.len() <= procarray::GetMaxSnapshotSubxidCount());
            target.xmin = sourcesnap.xmin;
            target.xmax = sourcesnap.xmax;
            copy_xids_into(&mut target.xip, &sourcesnap.xip);
            target.xcnt = sourcesnap.xip.len() as u32;
            copy_xids_into(&mut target.subxip, &sourcesnap.subxip);
            target.subxcnt = sourcesnap.subxip.len() as i32;
            target.suboverflowed = sourcesnap.suboverflowed;
            target.takenDuringRecovery = sourcesnap.takenDuringRecovery;
            // curcid NOT copied: it's a local matter.
            target.snapXactCompletionCount = 0;
            Ok(())
        })?;

        if !procarray::ProcArrayInstallRestoredXmin(current.xmin, source_proc)? {
            return Err(ereport(ERROR)
                .errcode(types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .errmsg("could not import the requested snapshot")
                .errdetail("The source transaction is not running anymore.")
                .into_error()
                .with_error_location(loc("SetTransactionSnapshot"))
                .into());
        }

        if xact_seams::isolation_uses_xact_snapshot::call() {
            if xact_seams::isolation_is_serializable::call() {
                predicate_seams::set_serializable_transaction_snapshot::call()?;
            }
            let copy = copy_snapshot_locked(s, &current);
            copy.regd_count.set(copy.regd_count.get() + 1);
            s.current = Some(SnapRef::Copied(copy.clone()));
            s.first_xact_snapshot = Some(copy.clone());
            s.registered.push(copy);
        }
        s.first_snapshot_set = true;
        Ok(())
    })
}

pub fn PushActiveSnapshot(snapshot: &Snapshot) -> PgResult<()> {
    PushActiveSnapshotWithLevel(snapshot, xact_seams::get_current_transaction_nest_level::call())
}

pub fn PushActiveSnapshotWithLevel(snapshot: &Snapshot, snap_level: i32) -> PgResult<()> {
    with_state(|s| {
        debug_assert!(s
            .active
            .last()
            .map(|top| snap_level >= top.as_level)
            .unwrap_or(true));
        // Checking SecondarySnapshot is probably useless here, but be sure.
        let needs_copy = s.is_ref_to(s.current.as_ref(), snapshot)
            || s.is_ref_to(s.secondary.as_ref(), snapshot)
            || !snapshot.copied;

        let as_snap = if needs_copy {
            copy_snapshot_locked(s, snapshot)
        } else {
            snapshot.clone()
        };
        as_snap.active_count.set(as_snap.active_count.get() + 1);
        s.active.push(ActiveSnapshotElt { as_snap, as_level: snap_level });
    });
    Ok(())
}

pub fn PushCopiedSnapshot(snapshot: &Snapshot) -> PgResult<()> {
    PushActiveSnapshot(&CopySnapshot(snapshot))
}

pub fn UpdateActiveSnapshotCommandId() -> PgResult<()> {
    let top = with_state(|s| {
        s.active
            .last()
            .expect("ActiveSnapshot != NULL")
            .as_snap
            .clone()
    });
    debug_assert_eq!(top.active_count.get(), 1);
    debug_assert_eq!(top.regd_count.get(), 0);

    let save_curcid = top.curcid.get();
    let curcid = xact_seams::get_current_command_id::call(false)?;
    if xact_seams::is_in_parallel_mode::call() && save_curcid != curcid {
        return Err(elog(
            ERROR,
            "cannot modify commandid in active snapshot during a parallel operation",
        )
        .expect_err("elog(ERROR)"));
    }
    top.curcid.set(curcid);
    Ok(())
}

pub fn PopActiveSnapshot() -> PgResult<()> {
    with_state(|s| {
        let Some(popped) = s.active.pop() else {
            return elog(ERROR, "ActiveSnapshot stack is empty").map(|_| ());
        };
        let snap = popped.as_snap;
        debug_assert!(snap.active_count.get() > 0);
        snap.active_count.set(snap.active_count.get() - 1);
        if snap.active_count.get() == 0 && snap.regd_count.get() == 0 {
            recycle_copy_locked(s, snap);
        }
        snapshot_reset_xmin_locked(s);
        Ok(())
    })
}

pub fn GetActiveSnapshot() -> Snapshot {
    with_state(|s| {
        s.active
            .last()
            .expect("ActiveSnapshot != NULL")
            .as_snap
            .clone()
    })
}

pub fn ActiveSnapshotSet() -> bool {
    with_state(|s| !s.active.is_empty())
}

pub fn SnapshotStateClean() -> bool {
    with_state(|s| {
        s.current.is_none()
            && s.secondary.is_none()
            && !s.catalog_valid
            && s.historic.is_none()
            && s.historic_tuplecids.is_none()
            && !s.first_snapshot_set
            && s.first_xact_snapshot.is_none()
            && s.active.is_empty()
            && s.registered.is_empty()
            && s.exported.is_empty()
    })
}

pub fn RegisterSnapshot(snapshot: Option<&Snapshot>) -> PgResult<Option<Snapshot>> {
    let Some(snapshot) = snapshot else {
        return Ok(None);
    };
    Ok(Some(RegisterSnapshotOnOwner(
        snapshot,
        resowner_seams::current_resource_owner::call(),
    )?))
}

pub fn RegisterSnapshotOnOwner(snapshot: &Snapshot, owner: ResourceOwner) -> PgResult<Snapshot> {
    let snap = if snapshot.copied {
        snapshot.clone()
    } else {
        CopySnapshot(snapshot)
    };

    resowner_seams::resource_owner_enlarge::call(owner)?;
    snap.regd_count.set(snap.regd_count.get() + 1);
    resowner_seams::resource_owner_remember_snapshot::call(owner, snap.clone());

    if snap.regd_count.get() == 1 {
        with_state(|s| s.registered.push(snap.clone()));
    }
    Ok(snap)
}

pub fn UnregisterSnapshot(snapshot: Option<&Snapshot>) {
    if let Some(snapshot) = snapshot {
        UnregisterSnapshotFromOwner(snapshot, resowner_seams::current_resource_owner::call());
    }
}

pub fn UnregisterSnapshotFromOwner(snapshot: &Snapshot, owner: ResourceOwner) {
    resowner_seams::resource_owner_forget_snapshot::call(owner, snapshot.clone());
    UnregisterSnapshotNoOwner(snapshot);
}

// Also the ResOwnerReleaseSnapshot target: must not touch the resource owner.
pub fn UnregisterSnapshotNoOwner(snapshot: &Snapshot) {
    debug_assert!(snapshot.regd_count.get() > 0);
    debug_assert!(with_state(|s| !s.registered.is_empty()));

    snapshot.regd_count.set(snapshot.regd_count.get() - 1);
    if snapshot.regd_count.get() == 0 {
        with_state(|s| registered_remove(s, snapshot));
    }
    if snapshot.regd_count.get() == 0 && snapshot.active_count.get() == 0 {
        with_state(snapshot_reset_xmin_locked);
    }
}

fn registered_remove(s: &mut SnapMgrState, snap: &Snapshot) {
    if let Some(pos) = s.registered.iter().position(|h| Rc::ptr_eq(h, snap)) {
        s.registered.swap_remove(pos);
    }
}

pub fn SnapshotResetXmin() {
    with_state(snapshot_reset_xmin_locked);
}

// Runs under the state borrow; the proc-xmin write can't re-enter snapmgr.
fn snapshot_reset_xmin_locked(s: &mut SnapMgrState) {
    if !s.active.is_empty() {
        return;
    }

    if s.registered.is_empty() {
        set_my_proc_xmin(InvalidTransactionId);
        procarray::set_transaction_xmin(InvalidTransactionId);
        return;
    }

    let mut min_xmin = s.registered[0].xmin;
    for h in &s.registered[1..] {
        if TransactionIdPrecedes(h.xmin, min_xmin) {
            min_xmin = h.xmin;
        }
    }

    if TransactionIdPrecedes(my_proc_xmin(), min_xmin) {
        set_my_proc_xmin(min_xmin);
        procarray::set_transaction_xmin(min_xmin);
    }
}

pub fn AtSubCommit_Snapshot(level: i32) {
    with_state(|s| {
        for elt in s.active.iter_mut().rev() {
            if elt.as_level < level {
                break;
            }
            elt.as_level = level - 1;
        }
    });
}

pub fn AtSubAbort_Snapshot(level: i32) -> PgResult<()> {
    with_state(|s| {
        while s.active.last().is_some_and(|top| top.as_level >= level) {
            let snap = s.active.pop().expect("checked non-empty").as_snap;
            debug_assert!(snap.active_count.get() >= 1);
            snap.active_count.set(snap.active_count.get() - 1);
            if snap.active_count.get() == 0 && snap.regd_count.get() == 0 {
                recycle_copy_locked(s, snap);
            }
        }
        snapshot_reset_xmin_locked(s);
    });
    Ok(())
}

pub fn AtEOXact_Snapshot(is_commit: bool, reset_xmin: bool) -> PgResult<()> {
    let (leftover_registered, leftover_active) = with_state(|s| {
        if let Some(first_xact) = s.first_xact_snapshot.take() {
            debug_assert!(first_xact.regd_count.get() > 0);
            debug_assert!(!s.registered.is_empty());
            registered_remove(s, &first_xact);
        }
        for esnap in core::mem::take(&mut s.exported) {
            // fd-routed (the P4 inc-4/inc-5 dataplane reroute): the export
            // file is DATADIR-RELATIVE and must unlink in the server's world
            // (boot cwd under --cfg pgrust_sim), not the ambient process cwd.
            if fd::pg_unlink(&esnap.snapfile) < 0 {
                let _ = ereport(types_error::WARNING)
                    .with_saved_errno(fd::get_errno())
                    .errmsg(format!("could not unlink file \"{}\": %m", esnap.snapfile))
                    .finish(loc("AtEOXact_Snapshot"));
            }
            registered_remove(s, &esnap.snapshot);
        }

        if s.catalog_valid {
            s.catalog_valid = false;
            let catalog = s.catalog_data.clone();
            registered_remove(s, &catalog);
        }

        let leftover_registered = is_commit && !s.registered.is_empty();
        let leftover_active = if is_commit { s.active.len() } else { 0 };

        s.active.clear();
        s.registered.clear();
        s.current = None;
        s.secondary = None;
        s.first_snapshot_set = false;

        (leftover_registered, leftover_active)
    });

    if leftover_registered {
        ereport(WARNING)
            .errmsg_internal("registered snapshots seem to remain after cleanup")
            .finish(loc("AtEOXact_Snapshot"))?;
    }
    for _ in 0..leftover_active {
        ereport(WARNING)
            .errmsg_internal("snapshot still active")
            .finish(loc("AtEOXact_Snapshot"))?;
    }

    // On commit ProcArrayEndTransaction already reset MyProc->xmin.
    if reset_xmin {
        SnapshotResetXmin();
    }
    debug_assert!(reset_xmin || my_proc_xmin() == 0);
    Ok(())
}

pub fn XactHasExportedSnapshots() -> bool {
    with_state(|s| !s.exported.is_empty())
}

#[cold]
#[inline(never)]
fn export_file_err(errno: i32, msg: String) -> Box<types_error::PgError> {
    ereport(ERROR)
        .with_saved_errno(errno)
        .errcode_for_file_access()
        .errmsg(msg)
        .into_error()
        .with_error_location(loc("ExportSnapshot"))
        .into()
}

// ExportSnapshot (snapmgr.c): serialize into pg_snapshots/ and pin a copy
// for the rest of the transaction; format! rides a per-statement admin path.
pub fn ExportSnapshot(snapshot: &Snapshot) -> PgResult<String> {
    let top_xid = xact_seams::get_top_transaction_id_if_any::call();

    if xact_seams::is_sub_transaction::call() {
        return Err(ereport(ERROR)
            .errcode(types_error::ERRCODE_ACTIVE_SQL_TRANSACTION)
            .errmsg("cannot export a snapshot from a subtransaction")
            .into_error()
            .with_error_location(loc("ExportSnapshot"))
            .into());
    }

    let children = xact_seams::xact_get_committed_children::call()?;

    let my_proc = lmgr_proc::GetPGProcByNumber(lmgr_proc::MyProc().expect("ExportSnapshot requires MyProc"));
    let proc_number = my_proc.vxid.procNumber.load(Relaxed);
    let lxid = my_proc.vxid.lxid.load(Relaxed);

    let (path, snapshot) = with_state(|s| {
        let path = format!(
            "{SNAPSHOT_EXPORT_DIR}/{proc_number:08X}-{lxid:08X}-{}",
            s.exported.len() + 1
        );
        let copy = copy_snapshot_locked(s, snapshot);
        s.exported.push(ExportedSnapshot { snapfile: path.clone(), snapshot: copy.clone() });
        copy.regd_count.set(copy.regd_count.get() + 1);
        s.registered.push(copy.clone());
        (path, copy)
    });

    let mut buf = String::new();
    use core::fmt::Write;
    let w = &mut buf;
    let _ = writeln!(w, "vxid:{proc_number}/{lxid}");
    let _ = writeln!(w, "pid:{}", init_small::globals::MyProcPid());
    let _ = writeln!(w, "dbid:{}", init_small::globals::MyDatabaseId());
    let _ = writeln!(w, "iso:{}", xact_seams::get_xact_iso_level::call());
    let _ = writeln!(w, "ro:{}", xact_seams::xact_read_only::call() as i32);
    let _ = writeln!(w, "xmin:{}", snapshot.xmin);
    let _ = writeln!(w, "xmax:{}", snapshot.xmax);

    // Own top XID joins xip unless past xmax (GetSnapshotData omits it).
    let add_top_xid =
        top_xid != InvalidTransactionId && TransactionIdPrecedes(top_xid, snapshot.xmax);
    let _ = writeln!(w, "xcnt:{}", snapshot.xcnt + add_top_xid as u32);
    for xid in &snapshot.xip[..snapshot.xcnt as usize] {
        let _ = writeln!(w, "xip:{xid}");
    }
    if add_top_xid {
        let _ = writeln!(w, "xip:{top_xid}");
    }

    if snapshot.suboverflowed
        || snapshot.subxcnt as usize + children.len() > procarray::GetMaxSnapshotSubxidCount()
    {
        let _ = writeln!(w, "sof:1");
    } else {
        let _ = writeln!(w, "sof:0");
        let _ = writeln!(w, "sxcnt:{}", snapshot.subxcnt as usize + children.len());
        for xid in &snapshot.subxip[..snapshot.subxcnt as usize] {
            let _ = writeln!(w, "sxp:{xid}");
        }
        for xid in &children {
            let _ = writeln!(w, "sxp:{xid}");
        }
    }
    let _ = writeln!(w, "rec:{}", snapshot.takenDuringRecovery as u32);

    // fd-routed (the P4 inc-4/inc-5 dataplane reroute): pg_snapshots/ is
    // DATADIR-RELATIVE — the write must land in the server's world (boot cwd
    // under --cfg pgrust_sim), not the ambient process cwd. C parity is
    // snapmgr.c ExportSnapshot's AllocateFile+fwrite+FreeFile+rename tail
    // (same messages, same "no fsync — the file need not survive a crash");
    // the choke here is the OpenTransientFile family, the relcache-initfile
    // reroute precedent.
    let pathtmp = format!("{path}.tmp");
    let tmpfd = fd::OpenTransientFile(&pathtmp, libc::O_CREAT | libc::O_TRUNC | libc::O_WRONLY)?;
    if tmpfd < 0 {
        return Err(export_file_err(
            fd::get_errno(),
            format!("could not create file \"{pathtmp}\": %m"),
        ));
    }
    let bytes = buf.as_bytes();
    let mut off: usize = 0;
    while off < bytes.len() {
        let n = fd::pg_pwrite(tmpfd, &bytes[off..], off as i64);
        if n <= 0 {
            let errno = fd::get_errno();
            fd::CloseTransientFile(tmpfd);
            return Err(export_file_err(
                errno,
                format!("could not write to file \"{pathtmp}\": %m"),
            ));
        }
        off += n as usize;
    }
    if fd::CloseTransientFile(tmpfd) != 0 {
        return Err(export_file_err(
            fd::get_errno(),
            format!("could not write to file \"{pathtmp}\": %m"),
        ));
    }
    if fd::pg_rename(&pathtmp, &path) < 0 {
        return Err(export_file_err(
            fd::get_errno(),
            format!("could not rename file \"{pathtmp}\" to \"{path}\": %m"),
        ));
    }

    Ok(path[SNAPSHOT_EXPORT_DIR.len() + 1..].to_string())
}

// Startup-time cleanup of crashed backends' export files; failures stay LOG.
//
// fd-routed over the vfs data plane (the provider-seam/relmapper
// dataplane-class fix, found by the P4 inc-4 real-initdb cut sweep): the
// export dir is DATADIR-RELATIVE and must resolve in the server's world
// (the boot cwd under --cfg pgrust_sim), not the ambient process cwd — the
// raw std::fs walk made this crash-boot cleanup silently target the wrong
// world under sim. C parity: AllocateDir + ReadDirExtended(LOG) + unlink,
// all failures LOG-level (snapmgr.c DeleteAllExportedSnapshotFiles).
pub fn DeleteAllExportedSnapshotFiles() {
    let s_dir = match fd::AllocateDir(SNAPSHOT_EXPORT_DIR) {
        Ok(d) => d,
        Err(_) => return, // descriptor-pressure path has already reported
    };
    loop {
        match fd::ReadDirExtended(s_dir, SNAPSHOT_EXPORT_DIR, types_error::LOG) {
            Ok(Some(de)) => {
                let buf = format!("{SNAPSHOT_EXPORT_DIR}/{}", de.d_name);
                if fd::pg_unlink(&buf) < 0 {
                    let _ = ereport(types_error::LOG)
                        .with_saved_errno(fd::get_errno())
                        .errmsg(format!("could not unlink file \"{buf}\": %m"))
                        .finish(loc("DeleteAllExportedSnapshotFiles"));
                }
            }
            Ok(None) | Err(_) => break,
        }
    }
    let _ = fd::FreeDir(s_dir);
}

// SNAPSHOT_EXPORT_DIR (snapmgr.c), relative to the data directory (the
// backend's cwd, as in C).
const SNAPSHOT_EXPORT_DIR: &str = "pg_snapshots";

// ImportSnapshot (snapmgr.c:1385). ExportSnapshot is unported (phase 2), so
// no export file can exist and every reachable outcome is one of C's
// precondition/identifier/missing-file errors; an existing file means the
// otherwise-unreachable parse+install tail, which stays loud.
pub fn ImportSnapshot(idstr: &str) -> PgResult<()> {
    if FirstSnapshotSet()
        || xact_seams::get_top_transaction_id_if_any::call() != InvalidTransactionId
        || xact_seams::is_sub_transaction::call()
    {
        return Err(ereport(ERROR)
            .errcode(types_error::ERRCODE_ACTIVE_SQL_TRANSACTION)
            .errmsg("SET TRANSACTION SNAPSHOT must be called before any query")
            .into_error()
            .with_error_location(loc("ImportSnapshot"))
            .into());
    }

    if !xact_seams::isolation_uses_xact_snapshot::call() {
        return Err(ereport(ERROR)
            .errcode(types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(
                "a snapshot-importing transaction must have isolation level SERIALIZABLE or REPEATABLE READ",
            )
            .into_error()
            .with_error_location(loc("ImportSnapshot"))
            .into());
    }

    // Only 0-9, A-F and hyphens: prevents reading arbitrary files.
    if !idstr.bytes().all(|b| b.is_ascii_digit() || (b'A'..=b'F').contains(&b) || b == b'-') {
        return Err(ereport(ERROR)
            .errcode(types_error::ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!("invalid snapshot identifier: \"{idstr}\""))
            .into_error()
            .with_error_location(loc("ImportSnapshot"))
            .into());
    }

    // fd-routed (the P4 inc-5 dataplane reroute, closing the inc-4 ledger's
    // "ImportSnapshot read surface" rows): pg_snapshots/ is DATADIR-RELATIVE
    // and must resolve in the server's world (boot cwd under
    // --cfg pgrust_sim), not the ambient process cwd. C parity: snapmgr.c
    // ImportSnapshot's AllocateFile probe — errno != ENOENT complains about
    // the open, ENOENT means the snapshot identifier names nothing.
    let path = format!("{SNAPSHOT_EXPORT_DIR}/{idstr}");
    let snapfd = fd::OpenTransientFile(&path, libc::O_RDONLY)?;
    if snapfd < 0 {
        let errno = fd::get_errno();
        if errno != libc::ENOENT {
            return Err(ereport(ERROR)
                .with_saved_errno(errno)
                .errcode_for_file_access()
                .errmsg(format!("could not open file \"{path}\" for reading: %m"))
                .into_error()
                .with_error_location(loc("ImportSnapshot"))
                .into());
        }
        return Err(ereport(ERROR)
            .errcode(types_error::ERRCODE_UNDEFINED_OBJECT)
            .errmsg(format!("snapshot \"{idstr}\" does not exist"))
            .into_error()
            .with_error_location(loc("ImportSnapshot"))
            .into());
    }
    // Read the whole file, then parse ExportSnapshot's line format.
    let mut filebuf = Vec::new();
    let mut off: i64 = 0;
    loop {
        let mut chunk = [0u8; 4096];
        let n = fd::pg_pread(snapfd, &mut chunk, off);
        if n < 0 {
            fd::CloseTransientFile(snapfd);
            return Err(ereport(ERROR)
                .with_saved_errno(fd::get_errno())
                .errcode_for_file_access()
                .errmsg(format!("could not read file \"{path}\": %m"))
                .into_error()
                .with_error_location(loc("ImportSnapshot"))
                .into());
        }
        if n == 0 {
            break;
        }
        filebuf.extend_from_slice(&chunk[..n as usize]);
        off += n as i64;
    }
    fd::CloseTransientFile(snapfd);

    let invalid = || -> Box<types_error::PgError> {
        ereport(ERROR)
            .errcode(types_error::ERRCODE_INVALID_TEXT_REPRESENTATION)
            .errmsg(format!("invalid snapshot data in file \"{path}\""))
            .into_error()
            .with_error_location(loc("ImportSnapshot"))
            .into()
    };
    let text = core::str::from_utf8(&filebuf).map_err(|_| invalid())?;
    let mut lines = text.lines();
    // parse{Int,Xid,Vxid}FromText: each field is mandatory and ordered.
    let mut field = |prefix: &str| -> PgResult<&str> {
        let line = lines.next().ok_or_else(invalid)?;
        line.strip_prefix(prefix).ok_or_else(|| invalid().into())
    };

    let vxid_raw = field("vxid:")?;
    let (src_procno_s, src_lxid_s) = vxid_raw.split_once('/').ok_or_else(invalid)?;
    let src_procno: i32 = src_procno_s.parse().map_err(|_| invalid())?;
    let src_lxid: u64 = src_lxid_s.parse().map_err(|_| invalid())?;
    let _src_pid: i64 = field("pid:")?.parse().map_err(|_| invalid())?;
    let src_dbid: types_core::Oid = field("dbid:")?.parse().map_err(|_| invalid())?;
    let src_isolevel: i32 = field("iso:")?.parse().map_err(|_| invalid())?;
    let src_readonly: i32 = field("ro:")?.parse().map_err(|_| invalid())?;

    let xmin: TransactionId = field("xmin:")?.parse().map_err(|_| invalid())?;
    let xmax: TransactionId = field("xmax:")?.parse().map_err(|_| invalid())?;
    let xcnt: usize = field("xcnt:")?.parse().map_err(|_| invalid())?;
    if xcnt > procarray::GetMaxSnapshotXidCount() {
        return Err(invalid());
    }
    let mut xip = Vec::with_capacity(xcnt);
    for _ in 0..xcnt {
        xip.push(field("xip:")?.parse().map_err(|_| invalid())?);
    }
    let suboverflowed = field("sof:")?.parse::<i32>().map_err(|_| invalid())? != 0;
    let mut subxip = Vec::new();
    if !suboverflowed {
        let sxcnt: usize = field("sxcnt:")?.parse().map_err(|_| invalid())?;
        if sxcnt > procarray::GetMaxSnapshotSubxidCount() {
            return Err(invalid());
        }
        subxip.reserve(sxcnt);
        for _ in 0..sxcnt {
            subxip.push(field("sxp:")?.parse().map_err(|_| invalid())?);
        }
    }
    let taken_during_recovery = field("rec:")?.parse::<i32>().map_err(|_| invalid())? != 0;

    // Sanity checks on the critical fields (snapmgr.c:1487).
    if src_lxid == 0
        || src_procno < 0
        || src_dbid == types_core::InvalidOid
        || !types_core::TransactionIdIsNormal(xmin)
        || !types_core::TransactionIdIsNormal(xmax)
    {
        return Err(invalid());
    }

    // A serializable importer needs a serializable read-compatible source
    // (predicate.c constraints, snapmgr.c:1497).
    if xact_seams::isolation_is_serializable::call() {
        if src_isolevel != types_core::xact::XACT_SERIALIZABLE {
            return Err(ereport(ERROR)
                .errcode(types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg("a serializable transaction cannot import a snapshot from a non-serializable transaction")
                .into_error()
                .with_error_location(loc("ImportSnapshot"))
                .into());
        }
        if src_readonly != 0 && !xact_seams::xact_read_only::call() {
            return Err(ereport(ERROR)
                .errcode(types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg("a non-read-only serializable transaction cannot import a snapshot from a read-only transaction")
                .into_error()
                .with_error_location(loc("ImportSnapshot"))
                .into());
        }
    }

    // Cross-database imports would not be protected by the source's xmin
    // (snapmgr.c:1512).
    if src_dbid != init_small::globals::MyDatabaseId() {
        return Err(ereport(ERROR)
            .errcode(types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("cannot import a snapshot from a different database")
            .into_error()
            .with_error_location(loc("ImportSnapshot"))
            .into());
    }

    let snapshot = SerializedSnapshot {
        xmin,
        xmax,
        xip,
        subxip,
        suboverflowed,
        takenDuringRecovery: taken_during_recovery,
        curcid: types_core::FirstCommandId,
        vistest: GlobalVisStateHandle::new(0),
    };
    SetTransactionSnapshot(&snapshot, src_procno)
}

pub fn ThereAreNoPriorRegisteredSnapshots() -> bool {
    with_state(|s| s.registered.len() <= 1)
}

pub fn HaveRegisteredOrActiveSnapshot() -> bool {
    with_state(|s| {
        if !s.active.is_empty() {
            return true;
        }
        // The catalog snapshot doesn't count as "registered" for this check.
        if s.catalog_valid && s.registered.len() == 1 {
            return false;
        }
        !s.registered.is_empty()
    })
}

// The tuplecid hash is reorderbuffer's (relfilelocator, ctid) -> (cmin, cmax)
// map; snapmgr stores it opaquely, exactly like C's HTAB *tuplecid_data.
pub type HistoricTupleCids = std::rc::Rc<dyn std::any::Any>;

pub fn SetupHistoricSnapshot(historic_snapshot: Snapshot, tuplecids: Option<HistoricTupleCids>) {
    with_state(|s| {
        s.historic = Some(historic_snapshot);
        s.historic_tuplecids = tuplecids;
    });
}

pub fn TeardownHistoricSnapshot(_is_error: bool) {
    with_state(|s| {
        s.historic = None;
        s.historic_tuplecids = None;
    });
}

pub fn HistoricSnapshotActive() -> bool {
    with_state(|s| s.historic.is_some())
}

pub fn HistoricSnapshotGetTupleCids() -> Option<HistoricTupleCids> {
    with_state(|s| s.historic_tuplecids.clone())
}

// Callers check TransactionIdIsCurrentTransactionId first, as in C.
// inline(always): per-tuple hot inside HeapTupleSatisfiesMVCC; the xmin/xmax
// range exits must fold into the visibility gate (plain #[inline] is refused;
// the call + by-memory PgResult return cost ~8 insns/tuple — see
// docs/benchmarks/heapam_visibility.md).
#[inline(always)]
pub fn XidInMVCCSnapshot(xid: TransactionId, snapshot: &SnapshotData<'_>) -> PgResult<bool> {
    let mut xid = xid;

    if TransactionIdPrecedes(xid, snapshot.xmin) {
        return Ok(false);
    }
    if TransactionIdFollowsOrEquals(xid, snapshot.xmax) {
        return Ok(true);
    }

    if !snapshot.takenDuringRecovery {
        if !snapshot.suboverflowed {
            if snapshot.subxip[..snapshot.subxcnt.max(0) as usize].contains(&xid) {
                return Ok(true);
            }
        } else {
            xid = subtrans_seams::sub_trans_get_topmost_transaction::call(xid)?;
            if TransactionIdPrecedes(xid, snapshot.xmin) {
                return Ok(false);
            }
        }
        if snapshot.xip[..snapshot.xcnt as usize].contains(&xid) {
            return Ok(true);
        }
    } else {
        if snapshot.suboverflowed {
            xid = subtrans_seams::sub_trans_get_topmost_transaction::call(xid)?;
            if TransactionIdPrecedes(xid, snapshot.xmin) {
                return Ok(false);
            }
        }
        if snapshot.subxip[..snapshot.subxcnt.max(0) as usize].contains(&xid) {
            return Ok(true);
        }
    }

    Ok(false)
}

pub fn init_seams() {
    snapmgr_seams::invalidate_catalog_snapshot::set(InvalidateCatalogSnapshot);
    snapmgr_seams::snapshot_set_command_id::set(SnapshotSetCommandId);
    snapmgr_seams::at_eoxact_snapshot::set(AtEOXact_Snapshot);
    snapmgr_seams::at_subcommit_snapshot::set(AtSubCommit_Snapshot);
    snapmgr_seams::at_subabort_snapshot::set(AtSubAbort_Snapshot);
    snapmgr_seams::xact_has_exported_snapshots::set(XactHasExportedSnapshots);
    snapmgr_seams::delete_all_exported_snapshot_files::set(DeleteAllExportedSnapshotFiles);
    snapmgr_seams::transaction_xmin::set(TransactionXmin);
    snapmgr_seams::active_snapshot_xmin::set(|| GetActiveSnapshot().xmin);
    snapmgr_seams::get_catalog_snapshot::set(GetCatalogSnapshot);
    snapmgr_seams::register_snapshot::set(|snapshot| {
        RegisterSnapshotOnOwner(&snapshot, resowner_seams::current_resource_owner::call())
    });
    snapmgr_seams::unregister_snapshot::set(|snapshot| UnregisterSnapshot(Some(&snapshot)));
    snapmgr_seams::unregister_snapshot_no_owner::set(|snapshot| {
        UnregisterSnapshotNoOwner(&snapshot)
    });
    snapmgr_portal_seams::unregister_snapshot_from_owner::set(|snapshot, owner| {
        UnregisterSnapshotFromOwner(&snapshot, owner)
    });
    snapmgr_portal_seams::active_snapshot_set::set(ActiveSnapshotSet);
    snapmgr_portal_seams::pop_active_snapshot::set(PopActiveSnapshot);
    snapmgr_seams::get_latest_snapshot::set(GetLatestSnapshot);
    snapmgr_seams::import_snapshot::set(ImportSnapshot);
}
