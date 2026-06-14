//! The canonical owned `QueryDesc` (`executor/execdesc.h`).
//!
//! `QueryDesc` is the executor's per-invocation handle: it owns the running
//! query's working storage (the `EState`), the initialized plan-state tree, the
//! destination receiver, and the read-only inputs (`PlannedStmt`, the source
//! text, the snapshots, the parameter list). `CreateQueryDesc` (execMain.c)
//! builds it before `ExecutorStart`; `ExecutorStart` fills the `EState`/plan
//! state / result tupdesc; `ExecutorRun` drives it; `ExecutorEnd` /
//! `FreeQueryDesc` tear it down.
//!
//! ## The owned model (why a bundle)
//!
//! In this repo an `EState` is an [`mcx::McxOwned`] bundle: the `EStateData`
//! node lives *inside* its own per-query "ExecutorState" context (the C
//! `es_query_cxt`), and everything `ExecutorStart` builds — the plan-state
//! tree, the slot/exprcontext/result-rel pools, the result `TupleDesc` — is
//! allocated *in that same inner context*. None of it can be a sibling
//! `PgBox<'mcx, _>` field on a `QueryDesc<'mcx>` at an *outer* lifetime, because
//! the inner context's lifetime is private to the bundle (it is heap-pinned and
//! moves with the bundle). So the executor working state — the `EState`, the
//! plan-state tree, and the read-only `PlannedStmt`/source-text copies the
//! query reads — is held as **one** `McxOwned` bundle ([`QueryWorkState`]); the
//! `QueryDesc` is the lifetime-free handle that owns it together with the small
//! `Copy`/`Rc`/handle inputs.
//!
//! ## The four historical views this collapses (and the bridges that remain)
//!
//! This is the single owned model the executor-ownership keystone (#169)
//! collapses the trimmed views onto. Three older views still exist as bridges
//! and are retired in the executor-de-handle follow-on (F1b):
//!
//! - [`crate::copy_query::QueryDesc`] — copyto's `{tupDesc, exec_token}` value
//!   (copyto threads an opaque executor handle; re-pointing it onto this owned
//!   value is a copyto consumer re-point = F1b);
//! - the portal embeds `types_portal::QueryDesc{snapshot,dest}` *by value,
//!   non-`'mcx`* in `PortalData` (so it cannot hold this bundle without
//!   infecting `PortalData` / the merged portalmem+portalcmds consumers — F1b);
//! - the opaque `QueryDescHandle` newtypes in `types-matview` /
//!   `types-execparallel`.

use mcx::{Mcx, McxOwned, MemoryContext, PgBox, PgString, PgVec};

use crate::execnodes::EStateData;
use crate::nodeindexscan::PlannedStmt;
use crate::nodes::CmdType;
use crate::parsestmt::{DestReceiverHandle, ParamListInfoHandle};
use crate::planstate::PlanStateNode;
use types_error::PgResult;

/// The executor working state a started query owns, all living in the one
/// per-query "ExecutorState" context.
///
/// `ExecutorStart` fills `planstate` (`ExecInitNode`) and `result_tupdesc`
/// (`ExecGetResultType`); they stay `None` between `CreateQueryDesc` and
/// `ExecutorStart` (the C `NULL`). The `EState` owns the whole plan-state tree,
/// so the tree's `EStateLink` back-pointers (`PlanState.state`) stay valid as
/// long as the bundle keeps both alive together — which it does, because both
/// live in the same context that the bundle owns.
pub struct QueryWorkState<'mcx> {
    /// `EState *estate` — the per-Executor-invocation working storage, made by
    /// `CreateExecutorState` inside this bundle's context.
    pub estate: EStateData<'mcx>,
    /// `PlannedStmt *plannedstmt` — the plan to execute. C aliases the
    /// planner-owned tree; the owned model holds a `copyObject`-shape copy in
    /// the per-query context so the bundle is self-contained.
    pub plannedstmt: PgBox<'mcx, PlannedStmt<'mcx>>,
    /// `const char *sourceText` — source text of the query, copied into the
    /// per-query context.
    pub source_text: PgString<'mcx>,
    /// `PlanState *planstate` — the initialized top-level plan-state tree, built
    /// by `ExecutorStart` (`ExecInitNode`). `None` before `ExecutorStart`.
    pub planstate: Option<PgBox<'mcx, PlanStateNode<'mcx>>>,
}

mcx::bind!(pub QueryWorkStateTy => QueryWorkState<'mcx>);

/// `QueryDesc` (`executor/execdesc.h`) — the owned executor invocation handle.
///
/// Lifetime-free: the executor working state lives in [`QueryDesc::work`] (an
/// [`McxOwned`] bundle whose inner context the bundle owns); the rest are
/// `Copy`/`Rc`/handle inputs copied in by `CreateQueryDesc`.
pub struct QueryDesc {
    /// `CmdType operation` — `CMD_SELECT`/`INSERT`/`UPDATE`/`DELETE`/`MERGE`/
    /// `UTILITY`, copied from `plannedstmt.commandType` by `CreateQueryDesc`.
    pub operation: CmdType,
    /// `Snapshot snapshot` — snapshot to use for the query, or `None`
    /// (C `InvalidSnapshot`).
    pub snapshot: Option<alloc::rc::Rc<types_snapshot::SnapshotData>>,
    /// `Snapshot crosscheck_snapshot` — crosscheck snapshot for RI updates/
    /// deletes, or `None` (C `InvalidSnapshot`).
    pub crosscheck_snapshot: Option<alloc::rc::Rc<types_snapshot::SnapshotData>>,
    /// `DestReceiver *dest` — destination for tuple output. Carried as the
    /// receiver-handle bridge until the DestReceiver receiver-value router
    /// (F0b / tcop-dest keystone) lands; `NULL` is no output.
    pub dest: DestReceiverHandle,
    /// `ParamListInfo params` — external parameter values, or `NULL`.
    pub params: ParamListInfoHandle,
    /// `int instrument_options` — OR of `InstrumentOption` flags.
    pub instrument_options: i32,
    /// `bool already_executed` — `ExecutorRun` has already been called once.
    pub already_executed: bool,
    /// The executor working state (the `EState` + plan-state tree + the
    /// read-only `PlannedStmt`/source-text copies). `ExecutorStart` populates
    /// `planstate`; `ExecutorEnd`/`FreeQueryDesc` drop the bundle.
    pub work: McxOwned<QueryWorkStateTy>,
}

impl QueryDesc {
    /// `CreateQueryDesc(plannedstmt, sourceText, snapshot, crosscheck_snapshot,
    /// dest, params, queryEnv, instrument_options)` (execMain.c): create the
    /// per-query "ExecutorState" context, the `EState` in it
    /// (`CreateExecutorState`), and copy the read-only inputs in. `planstate` /
    /// the result tupdesc stay `None` until `ExecutorStart`.
    ///
    /// `parent` is the `CurrentMemoryContext` the per-query context is made an
    /// (accounting) child of.
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        parent: &MemoryContext,
        plannedstmt: &PlannedStmt<'_>,
        source_text: &str,
        snapshot: Option<alloc::rc::Rc<types_snapshot::SnapshotData>>,
        crosscheck_snapshot: Option<alloc::rc::Rc<types_snapshot::SnapshotData>>,
        dest: DestReceiverHandle,
        params: ParamListInfoHandle,
        instrument_options: i32,
    ) -> PgResult<Self> {
        // qd->operation = plannedstmt->commandType;
        let operation = plannedstmt.commandType;
        // qcontext = AllocSetContextCreate(...,"ExecutorState",...);
        // estate = CreateExecutorState() within it; copy plannedstmt + sourceText.
        let work = McxOwned::<QueryWorkStateTy>::try_new(
            parent.new_child("ExecutorState"),
            |mcx: Mcx<'_>| {
                let plannedstmt = mcx::alloc_in(mcx, plannedstmt.clone_in(mcx)?)?;
                let source_text = PgString::from_str_in(source_text, mcx)?;
                Ok(QueryWorkState {
                    estate: EStateData::new_in(mcx),
                    plannedstmt,
                    source_text,
                    planstate: None,
                })
            },
        )?;
        Ok(QueryDesc {
            operation,
            snapshot,
            crosscheck_snapshot,
            dest,
            params,
            instrument_options,
            already_executed: false,
            work,
        })
    }
}

// Touch `PgVec` so a later builder allocating bundle-internal vectors keeps the
// import live without churn.
#[allow(dead_code)]
fn _allocator_witness<'a>(mcx: Mcx<'a>) -> PgVec<'a, u8> {
    PgVec::new_in(mcx)
}
