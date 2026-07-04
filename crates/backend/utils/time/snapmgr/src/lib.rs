#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use std::cell::{Cell, RefCell};
use std::mem::ManuallyDrop;
use std::rc::Rc;
use std::sync::atomic::Ordering::Relaxed;

use elog::{elog, ereport};
use mcx::{Mcx, MemoryContext, PgVec};
use types_core::{
    CommandId, InvalidTransactionId, Oid, TransactionId, TransactionIdIsNormal,
    TransactionIdPrecedes,
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
    first_snapshot_set: bool,
    first_xact_snapshot: Option<Snapshot>,
    // Resource-handle owners (Rc payloads): plain-heap Vecs per docs/no-drop.md.
    active: Vec<ActiveSnapshotElt>,
    registered: Vec<Snapshot>,
    // Dead unique copies, xip capacity retained — C's CopySnapshot freelist
    // palloc in TopTransactionContext (rule 7 retained scratch).
    copy_freelist: Vec<Snapshot>,
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
    static STATE: RefCell<Option<ManuallyDrop<SnapMgrState>>> = const { RefCell::new(None) };
}

#[cfg(debug_assertions)]
thread_local! {
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

fn with_state<R>(f: impl FnOnce(&mut SnapMgrState) -> R) -> R {
    STATE.with(|cell| {
        let mut slot = cell.borrow_mut();
        if slot.is_none() {
            let cx: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("SnapMgr")));
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
                first_snapshot_set: false,
                first_xact_snapshot: None,
                active: Vec::new(),
                registered: Vec::new(),
                copy_freelist: Vec::new(),
            }));
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
        // exportedSnapshots cleanup lives with ExportSnapshot (phase 2).

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
    // ExportSnapshot is unported (phase 2), so none can exist.
    false
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

pub fn SetupHistoricSnapshot(historic_snapshot: Snapshot) {
    with_state(|s| s.historic = Some(historic_snapshot));
    // The (cmin,cmax) tuplecid hash rides with logical decoding (phase 2).
}

pub fn TeardownHistoricSnapshot(_is_error: bool) {
    with_state(|s| s.historic = None);
}

pub fn HistoricSnapshotActive() -> bool {
    with_state(|s| s.historic.is_some())
}

pub fn HistoricSnapshotGetTupleCids() -> ! {
    unported("HistoricSnapshotGetTupleCids (logical decoding tuplecid hash)")
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
    snapmgr_seams::transaction_xmin::set(TransactionXmin);
    snapmgr_seams::get_catalog_snapshot::set(GetCatalogSnapshot);
    snapmgr_seams::register_snapshot::set(|snapshot| {
        RegisterSnapshotOnOwner(&snapshot, resowner_seams::current_resource_owner::call())
    });
    snapmgr_seams::unregister_snapshot::set(|snapshot| UnregisterSnapshot(Some(&snapshot)));
    snapmgr_portal_seams::unregister_snapshot_from_owner::set(|snapshot, owner| {
        UnregisterSnapshotFromOwner(&snapshot, owner)
    });
    snapmgr_portal_seams::active_snapshot_set::set(ActiveSnapshotSet);
    snapmgr_portal_seams::pop_active_snapshot::set(PopActiveSnapshot);
}
