use core::cell::{Cell, RefCell};
use std::rc::Rc;

use ::executils::EStateData;
use ::mcx::McxOwned;
use ::snapmgr::Snapshot;
use ::types_dest::CommandDest;
use ::types_error::PgResult;
use ::types_nodes::nodes_enums::CmdType;
use ::types_nodes::plannodes::PlannedStmt;
use ::types_portal::{CachedPlanHandle, ParamListHandle, QueryDescHandle, QueryEnvHandle};
use ::types_tuple::TupleDescData;

use crate::procnode::PlanStateNode;

pub struct ExecData<'mcx> {
    pub estate: EStateData<'mcx>,
    pub planstate: Option<PlanStateNode<'mcx>>,
}

::mcx::bind!(pub ExecTy => ExecData<'mcx>);

::mcx::forget_safe_struct!(ExecData<'_> { estate, planstate });

// Abort-path release of the mem::forgotten arena registries. This Drop runs
// ONLY on the error/panic unwind (standard_executor_end drains both registries
// explicitly and then free_recycle-forgets the bundle): field drop glue never
// reaches the subplan PlanStateNodes behind SubplanStateCell raw pointers or
// the SubPlan ExprState dropper pairs, so without this walk their droppy
// owners (relation refs, tuplesorts, Rc'd descriptors) leak past the query.
impl Drop for ExecData<'_> {
    fn drop(&mut self) {
        for i in 0..self.estate.es_subplanstates.len() {
            let cell = self.estate.es_subplanstates[i];
            // SAFETY: init_plan created this arena cell; the cell (and the
            // whole arena) outlives this drop — McxOwned frees the context
            // only after the state's drop_in_place returns. The take-out
            // protocol guarantees no aliasing &mut: an unwound consumer's
            // taken node was dropped with its stack frame, leaving None.
            let slot = unsafe {
                &mut *cell.0.cast::<Option<PlanStateNode<'_>>>().as_ptr()
            };
            drop(slot.take());
        }
        while let Some((p, dropper)) = self.estate.es_subplan_expr_states.pop() {
            // SAFETY: registered by exec_init_sub_plan_expr; dropped once here.
            unsafe { dropper(p) };
        }
    }
}

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
    // Cached plan backing pstmt when the portal runs one (skeleton-cache key).
    pub cplan: CachedPlanHandle,
    // C: queryDesc->totaltime — whole-query instrumentation a tap consumer
    // (pg_stat_statements) arms at ExecutorStart; standard run/finish
    // accumulate into it exactly like execMain.c.
    pub totaltime: Option<Box<types_core::instrument::Instrumentation>>,
}

thread_local! {
    // Handed by pquery (note_cplan_for_query_desc) just before CreateQueryDesc.
    static PENDING_CPLAN: Cell<CachedPlanHandle> = const { Cell::new(CachedPlanHandle::NULL) };
}

pub(crate) fn note_cplan_for_query_desc_seam(cplan: CachedPlanHandle) {
    PENDING_CPLAN.set(cplan);
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
    // with_qd sets it around `f`; remove() refuses an in-use entry, so the
    // &mut handed to `f` cannot dangle.
    in_use: Cell<bool>,
    qd: QueryDescData,
}

// ManuallyDrop: entries reach guard drops that call seams, and a panic in a
// TLS destructor aborts the postmaster; FATAL-exit leaks match C. Boxed for
// address stability: with_qd hands out a pointer into the entry while nested
// executors register/free (Vec realloc). The box is C's palloc'd QueryDesc.
thread_local! {
    static ENTRIES: RefCell<core::mem::ManuallyDrop<Vec<Option<Box<Entry>>>>> =
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
    let entry = Box::new(Entry { generation, in_use: Cell::new(false), qd });
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

// The entry stays in place; nested executor runs (SQL functions) register and
// free their own boxed entries freely. Re-entry on the SAME handle panics.
//
// De-monomorphization byte-shell (cf. the nodes_core walker/mutator de-monos):
// this generic fn is now a thin ~5-line shell that type-erases the caller's
// closure to a single `&mut dyn FnMut` and delegates to the one monomorphic
// engine `with_qd_dyn`. The acquire/guard/release body (TLS lookup, stale +
// re-entry asserts, the `Unuse` in-use guard) is codegen'd exactly once instead
// of being re-stamped per distinct closure type at each of the ~60 callsites.
// The public signature (`<R>`, `impl FnOnce(&mut QueryDescData) -> R`, `-> R`)
// is unchanged, so every caller compiles untouched. Cold path: once per
// ExecutorStart/Run/Finish/End and per QueryDesc accessor, never per row — the
// extra indirect call and `Option` move are query setup/teardown, not the row
// loop.
pub fn with_qd<R>(h: QueryDescHandle, f: impl FnOnce(&mut QueryDescData) -> R) -> R {
    let mut f = Some(f);
    let mut ret: Option<R> = None;
    // `with_qd_dyn` invokes the closure exactly once on the success path (and
    // never on the panic paths), so `f.take()` and the final `unwrap()` cannot
    // fail: `ret` is `Some` iff control reaches here without unwinding.
    with_qd_dyn(h, &mut |qd| {
        ret = Some((f.take().unwrap())(qd));
    });
    ret.unwrap()
}

// Monomorphic engine: the caller's closure is type-erased to `&mut dyn FnMut`,
// so this body is emitted a single time. Statement-for-statement identical to
// the former `with_qd` body (same asserts, same messages, same TLS access, same
// `Unuse` guard, `f` called exactly once at the same point); only the closure's
// type-erasure and the result-return move up into the shell above.
fn with_qd_dyn(h: QueryDescHandle, f: &mut dyn FnMut(&mut QueryDescData)) {
    assert!(!h.is_null(), "execmain: NULL QueryDescHandle dereferenced");
    let (idx, generation) = decode(h);
    let ptr: *mut Entry = ENTRIES.with(|e| {
        let mut v = e.borrow_mut();
        match v.get_mut(idx as usize) {
            Some(Some(en)) if en.generation == generation => {
                assert!(
                    !en.in_use.replace(true),
                    "execmain: re-entered QueryDescHandle {h:?}"
                );
                &mut **en as *mut Entry
            }
            _ => panic!("execmain: stale QueryDescHandle {h:?}"),
        }
    });
    struct Unuse(*mut Entry);
    impl Drop for Unuse {
        fn drop(&mut self) {
            // SAFETY: entry stays boxed and un-removable (in_use) until here.
            unsafe { (*self.0).in_use.set(false) }
        }
    }
    let _guard = Unuse(ptr);
    // SAFETY: boxed entry is address-stable across nested register/free; the
    // in_use flag excludes re-entry and remove(), so this is the only &mut.
    f(unsafe { &mut (*ptr).qd });
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
                let en = slot.take().unwrap();
                assert!(!en.in_use.get(), "execmain: remove of in-use QueryDescHandle {h:?}");
                en
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
    // Taken before anything fallible: a failed create must not leave a stale
    // pending handle for an unrelated QueryDesc.
    let cplan = PENDING_CPLAN.replace(CachedPlanHandle::NULL);
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
        cplan,
        totaltime: None,
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

pub(crate) fn query_desc_jit_instr_seam(h: QueryDescHandle) -> (i32, i32, u64) {
    with_qd(h, |qd| {
        qd.exec
            .as_ref()
            .expect("query_desc_jit_instr before ExecutorStart")
            .with(|d| {
                (
                    d.estate.es_jit_flags,
                    d.estate.es_jit_instr.created_functions,
                    d.estate.es_jit_instr.generation_nanos,
                )
            })
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

/// EA-on-morsels refusal records for one node (ea-morsels.md §6). None =
/// nothing recorded — the armed+instrumented emission gate in data form.
pub(crate) fn query_desc_runtime_ea_refusals_seam(
    h: QueryDescHandle,
    plan_node_id: i32,
) -> Option<Vec<(&'static str, &'static str)>> {
    with_qd(h, |qd| {
        let exec = qd.exec.as_mut()?;
        exec.with_mut(|d| {
            let v: Vec<(&'static str, &'static str)> = d
                .estate
                .es_runtime_ea_refusals
                .iter()
                .filter(|r| r.plan_node_id == plan_node_id)
                .map(|r| (r.arm, r.reason))
                .collect();
            if v.is_empty() {
                None
            } else {
                Some(v)
            }
        })
    })
}

/// EA-on-morsels pipeline reports touching one node (ea-morsels.md §4):
/// the full block prints on the root, member nodes get the marker line —
/// the caller distinguishes by root_node_id. None = none — the
/// armed+instrumented emission gate in data form.
pub(crate) fn query_desc_runtime_ea_pipeline_seam(
    h: QueryDescHandle,
    plan_node_id: i32,
) -> Option<Vec<types_core::instrument::RuntimeEaPipeline>> {
    with_qd(h, |qd| {
        let exec = qd.exec.as_mut()?;
        exec.with_mut(|d| {
            let v: Vec<types_core::instrument::RuntimeEaPipeline> = d
                .estate
                .es_runtime_ea_pipelines
                .iter()
                .filter(|p| {
                    p.root_node_id == plan_node_id
                        || p.member_node_id == plan_node_id
                        || p.member2_node_id == plan_node_id
                })
                .copied()
                .collect();
            if v.is_empty() {
                None
            } else {
                Some(v)
            }
        })
    })
}

/// EXPLAIN (ENGINE) attribution records for one node (single-executor Phase
/// 0.2). None = nothing recorded — the EXEC_FLAG_ENGINE_REPORT emission gate
/// in data form. The executils EngineKind is converted to its types_core
/// wire twin here (the seam crate cannot depend on executils).
pub(crate) fn query_desc_engine_events_seam(
    h: QueryDescHandle,
    plan_node_id: i32,
) -> Option<Vec<(types_core::instrument::EngineKindWire, &'static str, &'static str)>> {
    use types_core::instrument::EngineKindWire;
    with_qd(h, |qd| {
        let exec = qd.exec.as_mut()?;
        exec.with_mut(|d| {
            let v: Vec<(EngineKindWire, &'static str, &'static str)> = d
                .estate
                .es_engine_events
                .iter()
                .filter(|e| e.plan_node_id == plan_node_id)
                .map(|e| {
                    let kind = match e.engine {
                        ::executils::EngineKind::Lane => EngineKindWire::Lane,
                        ::executils::EngineKind::Spine => EngineKindWire::Spine,
                        ::executils::EngineKind::FusedArm => EngineKindWire::FusedArm,
                        ::executils::EngineKind::Runtime => EngineKindWire::Runtime,
                    };
                    (kind, e.class, e.detail)
                })
                .collect();
            if v.is_empty() {
                None
            } else {
                Some(v)
            }
        })
    })
}

pub(crate) fn query_desc_prune_result_seam(
    h: QueryDescHandle,
    part_prune_index: i32,
) -> Option<Vec<i32>> {
    with_qd(h, |qd| {
        let exec = qd.exec.as_mut()?;
        exec.with_mut(|d| {
            let entry = d
                .estate
                .es_part_prune_results
                .get(usize::try_from(part_prune_index).ok()?)?
                .as_ref()?;
            let mut v = Vec::with_capacity(entry.num_members() as usize);
            let mut i = entry.next_member(-1);
            while i >= 0 {
                v.push(i);
                i = entry.next_member(i);
            }
            Some(v)
        })
    })
}

pub(crate) fn query_desc_rti_unpruned_seam(h: QueryDescHandle, rti: i32) -> Option<bool> {
    with_qd(h, |qd| {
        let exec = qd.exec.as_ref()?;
        exec.with(|d| Some(d.estate.es_unpruned_relids.is_member(rti)))
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

pub(crate) fn query_desc_foreign_explain_seam(
    h: QueryDescHandle,
    plan_node_id: i32,
    flags: types_nodes::FdwExplainFlags,
    emit: &mut dyn FnMut(&str, types_nodes::FdwExplainProp<'_>) -> types_error::PgResult<()>,
) -> types_error::PgResult<()> {
    with_qd(h, |qd| {
        let Some(exec) = qd.exec.as_mut() else { return Ok(()) };
        exec.with_mut(|d| {
            let estate = &mut d.estate;
            if let Some(ps) = d.planstate.as_mut() {
                if let Some(x) = crate::procnode::planstate_foreign_explain(
                    ps,
                    estate,
                    plan_node_id,
                    flags,
                    emit,
                ) {
                    return x;
                }
            }
            // InitPlans/SubPlans hang off es_subplanstates, not the main tree.
            for i in 0..estate.es_subplanstates.len() {
                let cell = estate.es_subplanstates[i];
                // SAFETY: cells are arena-live *mut Option<PlanStateNode>
                // installed by InitPlan; disjoint from everything the walk
                // reaches through estate.
                let sub = unsafe { &mut *cell.0.cast::<Option<PlanStateNode>>().as_ptr() };
                if let Some(ps) = sub.as_mut() {
                    if let Some(x) = crate::procnode::planstate_foreign_explain(
                        ps,
                        estate,
                        plan_node_id,
                        flags,
                        emit,
                    ) {
                        return x;
                    }
                }
            }
            // EXPLAIN found this ForeignScan in the plan tree, so a miss here
            // is a walker coverage gap; C reaches the state directly.
            panic!(
                "planstate_foreign_explain: ForeignScan plan_node_id \
                 {plan_node_id} not found in the planstate tree"
            );
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

pub(crate) fn query_desc_memoize_instrument_seam(
    h: QueryDescHandle,
    plan_node_id: i32,
) -> Option<types_core::instrument::MemoizeInstrumentation> {
    match query_desc_instr_extra(h, plan_node_id)? {
        crate::procnode::InstrExtra::Memoize(m) => Some(m),
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

/// Gather/GatherMerge nworkers_launched (EXPLAIN's Workers Launched).
pub(crate) fn query_desc_workers_launched_seam(
    h: QueryDescHandle,
    plan_node_id: i32,
) -> Option<i32> {
    match query_desc_instr_extra(h, plan_node_id)? {
        crate::procnode::InstrExtra::WorkersLaunched(n) => Some(n),
        _ => None,
    }
}

/// MERGE (inserted, updated, deleted) counters (EXPLAIN's Tuples: line).
pub(crate) fn query_desc_merge_instrument_seam(
    h: QueryDescHandle,
    plan_node_id: i32,
) -> Option<(f64, f64, f64)> {
    match query_desc_instr_extra(h, plan_node_id)? {
        crate::procnode::InstrExtra::MergeCounts([i, u, d]) => Some((i, u, d)),
        _ => None,
    }
}

/// Per-worker Instrumentation for one plan node (C planstate->worker_instrument),
/// indexed by worker number; entries with nloops <= 0 are the caller's to skip.
pub(crate) fn query_desc_worker_instrument_seam(
    h: QueryDescHandle,
    plan_node_id: i32,
) -> Option<Vec<types_core::instrument::Instrumentation>> {
    let idx = usize::try_from(plan_node_id).ok()?;
    with_qd(h, |qd| {
        let exec = qd.exec.as_ref()?;
        exec.with(|d| {
            // C attaches worker_instrument only within the Gather subtree
            // (WorkerInstr::node_ids); a node outside every subtree gets no
            // Workers group.
            let out: Vec<_> = d
                .estate
                .es_worker_instrument
                .iter()
                .filter(|w| w.node_ids.contains(&plan_node_id))
                .map(|w| w.instrument.get(idx).copied().unwrap_or_default())
                .collect();
            (!out.is_empty()).then_some(out)
        })
    })
}

/// (worker number, sort instrumentation) pairs for one plan node (C
/// SortState.shared_info).
pub(crate) fn query_desc_worker_sort_instrument_seam(
    h: QueryDescHandle,
    plan_node_id: i32,
) -> Option<Vec<(i32, types_core::instrument::TuplesortInstrumentation)>> {
    with_qd(h, |qd| {
        let exec = qd.exec.as_ref()?;
        exec.with(|d| {
            let out: Vec<_> = d
                .estate
                .es_worker_instrument
                .iter()
                .enumerate()
                .flat_map(|(n, w)| {
                    w.sort
                        .iter()
                        .filter(|(id, _)| *id == plan_node_id)
                        .map(move |(_, si)| (n as i32, *si))
                })
                .collect();
            (!out.is_empty()).then_some(out)
        })
    })
}

/// (worker number, bitmap heap scan stats) pairs for one plan node (C
/// BitmapHeapScanState.sinstrument).
pub(crate) fn query_desc_worker_bitmap_instrument_seam(
    h: QueryDescHandle,
    plan_node_id: i32,
) -> Option<Vec<(i32, types_core::instrument::BitmapHeapScanInstrumentation)>> {
    with_qd(h, |qd| {
        let exec = qd.exec.as_ref()?;
        exec.with(|d| {
            let out: Vec<_> = d
                .estate
                .es_worker_instrument
                .iter()
                .enumerate()
                .flat_map(|(n, w)| {
                    w.bitmap
                        .iter()
                        .filter(|(id, _)| *id == plan_node_id)
                        .map(move |(_, bi)| (n as i32, *bi))
                })
                .collect();
            (!out.is_empty()).then_some(out)
        })
    })
}

pub(crate) fn query_desc_worker_incsort_instrument_seam(
    h: QueryDescHandle,
    plan_node_id: i32,
) -> Option<Vec<(i32, types_core::instrument::IncrementalSortInfo)>> {
    with_qd(h, |qd| {
        let exec = qd.exec.as_ref()?;
        exec.with(|d| {
            let out: Vec<_> = d
                .estate
                .es_worker_instrument
                .iter()
                .enumerate()
                .flat_map(|(n, w)| {
                    w.incsort
                        .iter()
                        .filter(|(id, _)| *id == plan_node_id)
                        .map(move |(_, si)| (n as i32, *si))
                })
                .collect();
            (!out.is_empty()).then_some(out)
        })
    })
}

pub(crate) fn query_desc_hash_instrument_seam(
    h: QueryDescHandle,
    plan_node_id: i32,
) -> Option<types_core::instrument::HashInstrumentation> {
    with_qd(h, |qd| {
        let exec = qd.exec.as_ref()?;
        exec.with(|d| {
            // show_hash_info merges leader + every worker by maxima (each
            // parallel-hash participant saw a different subset of batches).
            let mut merged: Option<types_core::instrument::HashInstrumentation> = None;
            let mut fold = |hi: &types_core::instrument::HashInstrumentation| match &mut merged {
                Some(m) => m.accum(hi),
                None => merged = Some(*hi),
            };
            if let Some((_, hi)) = d
                .estate
                .es_hash_instrumentation
                .iter()
                .find(|(id, _)| *id == plan_node_id)
            {
                fold(hi);
            }
            for w in d.estate.es_worker_instrument.iter() {
                if let Some((_, hi)) = w.hash.iter().find(|(id, _)| *id == plan_node_id) {
                    fold(hi);
                }
            }
            merged
        })
    })
}
