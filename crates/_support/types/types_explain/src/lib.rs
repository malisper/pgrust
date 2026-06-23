//! EXPLAIN output-state vocabulary (`commands/explain_state.h`).
//!
//! The owner of this state is `explain_state.c` (`NewExplainState`,
//! `ParseExplainOptionList`, the extension registry); this crate holds the
//! type. The node-tree fields of the C `ExplainState` (`pstmt`, `rtable`,
//! `rtable_names`, `deparse_cxt`, `printed_subplans`, `workers_state`) are
//! filled by `ExplainPrintPlan` (explain.c) when the `backend-commands-explain`
//! unit walks a plan tree. The `extension_state`/`extension_state_allocated`
//! slots are owned by `explain_state.c` (`GetExplainExtensionState` /
//! `SetExplainExtensionState`).

#![no_std]
#![forbid(unsafe_code)]
#![allow(non_camel_case_types)]

extern crate alloc;

use ::mcx::{Mcx, PgBox, PgString, PgVec};
use ::nodes::bitmapset::Bitmapset;
use ::nodes::nodeindexscan::PlannedStmt;
use ::nodes::nodes::Node;
use ::nodes::parsenodes::RangeTblEntry;

/// `typedef enum ExplainFormat` (commands/explain_state.h) — output format.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ExplainFormat {
    #[default]
    EXPLAIN_FORMAT_TEXT,
    EXPLAIN_FORMAT_XML,
    EXPLAIN_FORMAT_JSON,
    EXPLAIN_FORMAT_YAML,
}

/// `typedef enum ExplainSerializeOption` (commands/explain_state.h) — serialize
/// the query's output?
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ExplainSerializeOption {
    #[default]
    EXPLAIN_SERIALIZE_NONE,
    EXPLAIN_SERIALIZE_TEXT,
    EXPLAIN_SERIALIZE_BINARY,
}

/// One slot of the C `void **extension_state` array — an extension's opaque
/// private state pointer (`SetExplainExtensionState` stores it,
/// `GetExplainExtensionState` returns it). `void *` is genuinely opaque (an
/// extension's own struct), so this is an opaque handle, not an invented type;
/// `None` is the C `NULL` slot.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash)]
pub struct ExtensionStateHandle(pub u64);

/// `typedef struct ExplainWorkersState` (commands/explain_state.h) — the
/// per-worker output redirection state, allocated by `ExplainCreateWorkersState`
/// when an ANALYZE'd parallel plan node has per-worker instrumentation. Only the
/// EXPLAIN ANALYZE path populates it; the structural slice never allocates one.
#[derive(Debug)]
pub struct ExplainWorkersState<'mcx> {
    /// `int num_workers` — # of worker processes the plan used.
    pub num_workers: i32,
    /// `bool *worker_inited` — per-worker state-initialized flags.
    pub worker_inited: PgVec<'mcx, bool>,
    /// `StringInfoData *worker_str` — per-worker transient output buffers.
    pub worker_str: PgVec<'mcx, PgString<'mcx>>,
    /// `int *worker_state_save` — per-worker grouping state save areas.
    pub worker_state_save: PgVec<'mcx, i32>,
    /// `StringInfo prev_str` — saved output buffer while redirecting. `None`
    /// when not currently redirecting to a worker buffer.
    pub prev_str: Option<PgString<'mcx>>,
}

/// `typedef struct ExplainState` (commands/explain_state.h) — the EXPLAIN output
/// state: the output buffer, option flags, format, grouping state, and the
/// plan-tree fields `ExplainPrintPlan` fills.
///
/// `str` (C `StringInfo`, always non-NULL during formatting) is the
/// context-allocated [`PgString`]; `grouping_stack` (C integer `List`) is the
/// context-allocated [`PgVec`] of `i32`.
#[derive(Debug)]
pub struct ExplainState<'mcx> {
    /// `StringInfo str` — output buffer.
    pub str: PgString<'mcx>,
    /* options */
    /// `bool verbose` — be verbose.
    pub verbose: bool,
    /// `bool analyze` — print actual times.
    pub analyze: bool,
    /// `bool costs` — print estimated costs.
    pub costs: bool,
    /// `bool buffers` — print buffer usage.
    pub buffers: bool,
    /// `bool wal` — print WAL usage.
    pub wal: bool,
    /// `bool timing` — print detailed node timing.
    pub timing: bool,
    /// `bool summary` — print total planning and execution timing.
    pub summary: bool,
    /// `bool memory` — print planner's memory usage information.
    pub memory: bool,
    /// `bool settings` — print modified settings.
    pub settings: bool,
    /// `bool generic` — generate a generic plan.
    pub generic: bool,
    /// `ExplainSerializeOption serialize` — serialize the query's output?
    pub serialize: ExplainSerializeOption,
    /// `ExplainFormat format` — output format.
    pub format: ExplainFormat,
    /* state for output formatting --- not reset for each new plan tree */
    /// `int indent` — current indentation level.
    pub indent: i32,
    /// `List *grouping_stack` — format-specific grouping state (integer list).
    pub grouping_stack: PgVec<'mcx, i32>,
    /* state related to the current plan tree (filled by ExplainPrintPlan) */
    /// `PlannedStmt *pstmt` — top of plan. `None` until `ExplainPrintPlan` sets
    /// it (C `NULL`). C aliases the running query's plan; the owned model holds
    /// a copy in the formatting context.
    pub pstmt: Option<PgBox<'mcx, PlannedStmt<'mcx>>>,
    /// `List *rtable` — range table (aliases `pstmt->rtable`). `None` is the C
    /// `NIL`.
    pub rtable: Option<PgVec<'mcx, RangeTblEntry<'mcx>>>,
    /// `List *rtable_names` — alias names for RTEs, produced by
    /// `select_rtable_names_for_explain`. Each element is the C
    /// `char *` (a `None` slot is the C `NULL`, meaning "use the RTE's eref").
    pub rtable_names: PgVec<'mcx, Option<PgString<'mcx>>>,
    /// `List *deparse_cxt` — context list for deparsing expressions, produced
    /// by `deparse_context_for_plan_tree` (ruleutils). Carried as the generic
    /// C `List *` of `deparse_namespace` nodes; `None` is the C `NIL`. Empty in
    /// the structural slice (ruleutils unported).
    pub deparse_cxt: Option<PgVec<'mcx, PgBox<'mcx, Node<'mcx>>>>,
    /// `Bitmapset *printed_subplans` — ids of SubPlans we've printed. `None` is
    /// the C `NULL`.
    pub printed_subplans: Option<PgBox<'mcx, Bitmapset<'mcx>>>,
    /// `bool hide_workers` — set if we find an invisible Gather.
    pub hide_workers: bool,
    /// `int rtable_size` — length of rtable excluding the RTE_GROUP entry.
    pub rtable_size: i32,
    /* state related to the current plan node */
    /// `ExplainWorkersState *workers_state` — needed if parallel plan. `None`
    /// unless an ANALYZE'd parallel node allocated one.
    pub workers_state: Option<PgBox<'mcx, ExplainWorkersState<'mcx>>>,
    /* extensions */
    /// `void **extension_state` — per-extension opaque state slots, indexed by
    /// the id `GetExplainExtensionId` hands out. A `None` slot is the C `NULL`.
    pub extension_state: PgVec<'mcx, Option<ExtensionStateHandle>>,
    /// `int extension_state_allocated` — allocated length of `extension_state`.
    pub extension_state_allocated: i32,

    /// Owned-model back-pointers into the running `EState`'s subplan-state
    /// tables, so `ExplainSubPlans` can reach the InitPlan/SubPlan child
    /// plan-state trees. In C `sps->planstate` directly aliases the child
    /// `PlanState` (and the InitPlan `SubPlanState` lives on
    /// `planstate->initPlan`); the owned model single-owns those child states in
    /// `EState.es_subplanstates` (keyed by the 1-based `plan_id`) and the
    /// InitPlan `SubPlanState`s in `EState.es_initplan`, so EXPLAIN resolves a
    /// subplan's child by `plan_id - 1` index through these non-owning slices.
    /// Set by `ExplainPrintPlan` from the started `QueryDesc`'s EState; valid for
    /// the duration of the synchronous plan-tree walk (the EState outlives the
    /// walk in `ExplainPrintPlan`). `(null, 0)` when unset (a no-subplan plan).
    pub es_subplanstates_ptr: *const (),
    pub es_subplanstates_len: usize,
    pub es_initplan_ptr: *const (),
    pub es_initplan_len: usize,

    /// Owned-model back-pointers into the running `EState`'s result-rel pool and
    /// the unpruned-relids bitmapset, so `show_modifytable_info` can implement
    /// the `labeltargets` per-target-relation labeling (explain.c:4565). In C the
    /// labeling reads `mtstate->resultRelInfo[j].ri_RangeTableIndex` /
    /// `ri_FdwRoutine` and `estate->es_unpruned_relids`; the owned model keeps the
    /// `ResultRelInfo`s in `EState.es_result_rel_pool` (addressed by `RriId`) and
    /// the bitmapset on the EState, so EXPLAIN reaches them through these
    /// non-owning back-pointers. Set by `ExplainPrintPlan`; valid for the
    /// synchronous walk. `(null, 0)` / `null` when unset.
    pub es_result_rel_pool_ptr: *const (),
    pub es_result_rel_pool_len: usize,
    /// Non-owning pointer to `EState.es_unpruned_relids` (`*const Option<PgBox<Bitmapset>>`).
    pub es_unpruned_relids_ptr: *const (),
}

impl<'mcx> ExplainState<'mcx> {
    /// A fresh formatting state charged to `mcx`, mirroring the zeroed
    /// `NewExplainState` (empty buffer, `EXPLAIN_FORMAT_TEXT`, indent 0, empty
    /// grouping stack).
    pub fn new_in(mcx: Mcx<'mcx>) -> Self {
        ExplainState {
            str: PgString::new_in(mcx),
            verbose: false,
            analyze: false,
            costs: false,
            buffers: false,
            wal: false,
            timing: false,
            summary: false,
            memory: false,
            settings: false,
            generic: false,
            serialize: ExplainSerializeOption::EXPLAIN_SERIALIZE_NONE,
            format: ExplainFormat::EXPLAIN_FORMAT_TEXT,
            indent: 0,
            grouping_stack: PgVec::new_in(mcx),
            pstmt: None,
            rtable: None,
            rtable_names: PgVec::new_in(mcx),
            deparse_cxt: None,
            printed_subplans: None,
            hide_workers: false,
            rtable_size: 0,
            workers_state: None,
            extension_state: PgVec::new_in(mcx),
            extension_state_allocated: 0,
            es_subplanstates_ptr: core::ptr::null(),
            es_subplanstates_len: 0,
            es_initplan_ptr: core::ptr::null(),
            es_initplan_len: 0,
            es_result_rel_pool_ptr: core::ptr::null(),
            es_result_rel_pool_len: 0,
            es_unpruned_relids_ptr: core::ptr::null(),
        }
    }
}
