use core::cell::{Cell, RefCell};
use std::rc::Rc;

use ::executils::EStateData;
use ::mcx::McxOwned;
use ::snapmgr::Snapshot;
use ::types_dest::CommandDest;
use ::types_error::PgResult;
use ::types_nodes::nodes_enums::CmdType;
use ::types_nodes::plannodes::PlannedStmt;
use ::types_portal::{ParamListHandle, QueryDescHandle, QueryEnvHandle};
use ::types_tuple::TupleDescData;

use crate::procnode::PlanStateNode;

pub struct ExecData<'mcx> {
    pub estate: EStateData<'mcx>,
    pub planstate: Option<PlanStateNode<'mcx>>,
}

::mcx::bind!(pub ExecTy => ExecData<'mcx>);

::mcx::forget_safe_struct!(ExecData<'_> { estate, planstate });

/// The C `EState*` + root PlanState: the "ExecutorState" context bundle.
pub type ExecutorHandle = McxOwned<ExecTy>;

pub struct QueryDescData {
    pub operation: CmdType,
    pstmt: *const PlannedStmt<'static>,
    src_ptr: *const u8,
    src_len: usize,
    pub snapshot: Option<Snapshot>,
    pub crosscheck_snapshot: Option<Snapshot>,
    pub dest: CommandDest,
    pub params: ParamListHandle,
    pub query_env: QueryEnvHandle,
    pub instrument_options: i32,
    pub tup_desc: Option<Rc<TupleDescData<'static>>>,
    // Boxed: inline ExecData is ~1.7KB, and QueryDescData moves through the
    // registry by value — unboxed it cost ~10k memcpy instr per SELECT 1
    // (select1-gate attribution, 2026-07-03).
    pub exec: Option<Box<ExecutorHandle>>,
    pub already_executed: bool,
}

impl QueryDescData {
    #[inline]
    pub fn plannedstmt(&self) -> &'static PlannedStmt<'static> {
        // SAFETY: create_query_desc's retention contract — the caller keeps
        // the PlannedStmt alive and unmoved until free_query_desc.
        unsafe { &*self.pstmt }
    }

    #[inline]
    pub fn source_text(&self) -> &'static str {
        // SAFETY: same retention contract as plannedstmt; bytes came from &str.
        unsafe {
            core::str::from_utf8_unchecked(core::slice::from_raw_parts(self.src_ptr, self.src_len))
        }
    }
}

/// # Safety
/// The pointee must outlive `'a`; the borrow is read-only (the plan tree is
/// sealed). Invariance of `PlannedStmt<'mcx>` is a list-GAT artifact, not
/// interior mutability, so shortening is sound.
pub(crate) unsafe fn shorten_pstmt<'a>(p: &PlannedStmt<'_>) -> &'a PlannedStmt<'a> {
    unsafe { core::mem::transmute::<&PlannedStmt<'_>, &'a PlannedStmt<'a>>(p) }
}

struct Entry {
    generation: u32,
    qd: QueryDescData,
}

// ManuallyDrop: entries reach guard drops (BufferPin, Relation closers) that
// call seams, and a panic in a TLS destructor is a process abort (rust
// runtime aborts the whole postmaster). Entries surviving a FATAL exit leak,
// exactly as C's backend-process death leaves them.
thread_local! {
    static ENTRIES: RefCell<core::mem::ManuallyDrop<Vec<Option<Entry>>>> =
        const { RefCell::new(core::mem::ManuallyDrop::new(Vec::new())) };
    static FREE: RefCell<core::mem::ManuallyDrop<Vec<u32>>> =
        const { RefCell::new(core::mem::ManuallyDrop::new(Vec::new())) };
    static GENERATION: Cell<u32> = const { Cell::new(0) };
}

fn encode(idx: u32, generation: u32) -> QueryDescHandle {
    QueryDescHandle((u64::from(generation) << 32) | u64::from(idx + 1))
}

fn decode(h: QueryDescHandle) -> (u32, u32) {
    ((h.0 as u32) - 1, (h.0 >> 32) as u32)
}

fn register(qd: QueryDescData) -> QueryDescHandle {
    let generation = GENERATION.with(|g| {
        let v = g.get().wrapping_add(1);
        g.set(v);
        v
    });
    let entry = Entry { generation, qd };
    let idx = match FREE.with(|f| f.borrow_mut().pop()) {
        Some(i) => {
            ENTRIES.with(|e| e.borrow_mut()[i as usize] = Some(entry));
            i
        }
        None => ENTRIES.with(|e| {
            let mut e = e.borrow_mut();
            e.push(Some(entry));
            (e.len() - 1) as u32
        }),
    };
    encode(idx, generation)
}

// Borrow held across `f`: nothing under the executor re-enters this registry
// today (SPI unported); re-entry is a loud RefCell panic, never corruption.
pub(crate) fn with_qd<R>(h: QueryDescHandle, f: impl FnOnce(&mut QueryDescData) -> R) -> R {
    assert!(!h.is_null(), "execmain: NULL QueryDescHandle dereferenced");
    let (idx, generation) = decode(h);
    ENTRIES.with(|e| {
        let mut v = e.borrow_mut();
        match v.get_mut(idx as usize).and_then(|s| s.as_mut()) {
            Some(en) if en.generation == generation => f(&mut en.qd),
            _ => panic!("execmain: stale QueryDescHandle {h:?} (already freed)"),
        }
    })
}

pub fn registry_len() -> usize {
    ENTRIES.with(|e| e.borrow().iter().filter(|s| s.is_some()).count())
}

fn remove(h: QueryDescHandle) -> QueryDescData {
    assert!(!h.is_null(), "execmain: FreeQueryDesc of NULL handle");
    let (idx, generation) = decode(h);
    let entry = ENTRIES.with(|e| {
        let mut v = e.borrow_mut();
        match v.get_mut(idx as usize) {
            Some(slot) if slot.as_ref().map(|en| en.generation) == Some(generation) => {
                slot.take().unwrap()
            }
            _ => panic!("execmain: stale QueryDescHandle {h:?} (already freed)"),
        }
    });
    FREE.with(|f| f.borrow_mut().push(idx));
    entry.qd
}

/// `CreateQueryDesc` (pquery.c).
pub(crate) fn create_query_desc_seam(
    plannedstmt: &PlannedStmt<'_>,
    source_text: &str,
    snapshot: Option<Snapshot>,
    crosscheck_snapshot: Option<Snapshot>,
    dest: CommandDest,
    params: ParamListHandle,
    query_env: QueryEnvHandle,
    instrument_options: i32,
) -> PgResult<QueryDescHandle> {
    let snapshot = snapmgr::RegisterSnapshot(snapshot.as_ref())?;
    let crosscheck_snapshot = snapmgr::RegisterSnapshot(crosscheck_snapshot.as_ref())?;
    // SAFETY: lifetime erasure under the seam's retention contract (the
    // caller outlives free_query_desc); re-borrowed only via plannedstmt().
    let pstmt = unsafe {
        core::mem::transmute::<&PlannedStmt<'_>, &'static PlannedStmt<'static>>(plannedstmt)
    };
    Ok(register(QueryDescData {
        operation: pstmt.commandType,
        pstmt,
        src_ptr: source_text.as_ptr(),
        src_len: source_text.len(),
        snapshot,
        crosscheck_snapshot,
        dest,
        params,
        query_env,
        instrument_options,
        tup_desc: None,
        exec: None,
        already_executed: false,
    }))
}

// C frees an aborted QueryDesc with the portal context, ExecutorEnd never
// runs, and snapshot registrations are released by the resource owner.
pub(crate) fn release_query_desc_seam(h: QueryDescHandle) {
    drop(remove(h));
}

/// `FreeQueryDesc` (pquery.c).
pub(crate) fn free_query_desc_seam(h: QueryDescHandle) {
    let mut qd = remove(h);
    assert!(qd.exec.is_none(), "FreeQueryDesc of a live query");
    snapmgr::UnregisterSnapshot(qd.snapshot.take().as_ref());
    snapmgr::UnregisterSnapshot(qd.crosscheck_snapshot.take().as_ref());
}

pub(crate) fn query_desc_es_processed_seam(h: QueryDescHandle) -> u64 {
    with_qd(h, |qd| {
        qd.exec
            .as_ref()
            .expect("query_desc_es_processed before ExecutorStart")
            .with(|d| d.estate.es_processed)
    })
}

pub(crate) fn query_desc_snapshot_seam(h: QueryDescHandle) -> Option<Snapshot> {
    with_qd(h, |qd| qd.snapshot.clone())
}

pub(crate) fn query_desc_result_tupdesc_seam(
    h: QueryDescHandle,
) -> Option<Rc<TupleDescData<'static>>> {
    with_qd(h, |qd| qd.tup_desc.clone())
}

pub(crate) fn query_desc_operation_seam(h: QueryDescHandle) -> CmdType {
    with_qd(h, |qd| qd.operation)
}

pub(crate) fn query_desc_instrument_seam(
    h: QueryDescHandle,
    plan_node_id: i32,
) -> Option<types_core::instrument::Instrumentation> {
    with_qd(h, |qd| {
        let exec = qd.exec.as_mut()?;
        exec.with_mut(|d| {
            let i = d
                .estate
                .es_instrumentation
                .get_mut(usize::try_from(plan_node_id).ok()?)?;
            // C's ExplainNode forcibly InstrEndLoops before reading.
            ::instrument::instr_end_loop(i);
            Some(*i)
        })
    })
}

pub(crate) fn query_desc_agg_instrument_seam(
    h: QueryDescHandle,
    plan_node_id: i32,
) -> Option<types_core::instrument::AggregateInstrumentation> {
    with_qd(h, |qd| {
        let exec = qd.exec.as_ref()?;
        exec.with(|d| {
            d.estate
                .es_agg_instrumentation
                .iter()
                .find_map(|(id, ai)| (*id == plan_node_id).then_some(*ai))
        })
    })
}

/// nsearches for an IndexScan/IndexOnlyScan node (EXPLAIN's Index Searches).
pub(crate) fn query_desc_index_instrument_seam(
    h: QueryDescHandle,
    plan_node_id: i32,
) -> Option<u64> {
    with_qd(h, |qd| {
        let exec = qd.exec.as_ref()?;
        exec.with(|d| {
            d.estate
                .es_index_instrumentation
                .iter()
                .find_map(|(id, n)| (*id == plan_node_id).then_some(*n))
        })
    })
}

pub(crate) fn query_desc_sort_instrument_seam(
    h: QueryDescHandle,
    plan_node_id: i32,
) -> Option<types_core::instrument::TuplesortInstrumentation> {
    with_qd(h, |qd| {
        let exec = qd.exec.as_ref()?;
        exec.with(|d| {
            d.estate
                .es_sort_instrumentation
                .iter()
                .find_map(|(id, si)| (*id == plan_node_id).then_some(*si))
        })
    })
}

pub(crate) fn query_desc_incsort_instrument_seam(
    h: QueryDescHandle,
    plan_node_id: i32,
) -> Option<types_core::instrument::IncrementalSortInfo> {
    with_qd(h, |qd| {
        let exec = qd.exec.as_ref()?;
        exec.with(|d| {
            d.estate
                .es_incsort_instrumentation
                .iter()
                .find_map(|(id, si)| (*id == plan_node_id).then_some(*si))
        })
    })
}

// Main planstate + initplan/CTE subplan trees (C reads PlanState directly).
fn query_desc_instr_extra(
    h: QueryDescHandle,
    plan_node_id: i32,
) -> Option<crate::procnode::InstrExtra> {
    let id = u32::try_from(plan_node_id).ok()?;
    with_qd(h, |qd| {
        let exec = qd.exec.as_mut()?;
        exec.with_mut(|d| {
            let estate = &mut d.estate;
            if let Some(ps) = d.planstate.as_mut() {
                if let Some(x) = crate::procnode::planstate_instr_extra(ps, estate, id) {
                    return Some(x);
                }
            }
            for i in 0..estate.es_subplanstates.len() {
                let cell = estate.es_subplanstates[i];
                // SAFETY: cells are arena-live *mut Option<PlanStateNode>
                // installed by InitPlan; disjoint from everything the walk
                // reaches through estate.
                let sub = unsafe { &mut *cell.0.cast::<Option<PlanStateNode>>().as_ptr() };
                if let Some(ps) = sub.as_mut() {
                    if let Some(x) = crate::procnode::planstate_instr_extra(ps, estate, id) {
                        return Some(x);
                    }
                }
            }
            None
        })
    })
}

pub(crate) fn query_desc_tuplestore_instrument_seam(
    h: QueryDescHandle,
    plan_node_id: i32,
) -> Option<types_core::instrument::TuplestoreInstrumentation> {
    match query_desc_instr_extra(h, plan_node_id)? {
        crate::procnode::InstrExtra::Storage(s) => Some(s),
        _ => None,
    }
}

pub(crate) fn query_desc_bitmap_instrument_seam(
    h: QueryDescHandle,
    plan_node_id: i32,
) -> Option<types_core::instrument::BitmapHeapScanInstrumentation> {
    match query_desc_instr_extra(h, plan_node_id)? {
        crate::procnode::InstrExtra::Bitmap(b) => Some(b),
        _ => None,
    }
}

pub(crate) fn query_desc_index_searches_seam(
    h: QueryDescHandle,
    plan_node_id: i32,
) -> Option<u64> {
    match query_desc_instr_extra(h, plan_node_id)? {
        crate::procnode::InstrExtra::IndexSearches(n) => Some(n),
        _ => None,
    }
}

pub(crate) fn query_desc_hash_instrument_seam(
    h: QueryDescHandle,
    plan_node_id: i32,
) -> Option<types_core::instrument::HashInstrumentation> {
    with_qd(h, |qd| {
        let exec = qd.exec.as_ref()?;
        exec.with(|d| {
            d.estate
                .es_hash_instrumentation
                .iter()
                .find_map(|(id, hi)| (*id == plan_node_id).then_some(*hi))
        })
    })
}
