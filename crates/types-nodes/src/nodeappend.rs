//! Append node vocabulary (`nodes/plannodes.h` `Append`, `executor/execnodes.h`
//! `AppendState`/`AsyncRequest`, the file-local `ParallelAppendState` from
//! `executor/nodeAppend.c`, and the trimmed `PartitionPruneState` from
//! `executor/execPartition.h` that the Append node reads).
//!
//! The embedded `PlanState` head reuses [`PlanStateData`]; the leading base
//! `Plan` reuses [`crate::nodeindexscan::Plan`]; executor-pool aliases follow
//! the owned model ([`SlotId`] for `TupleTableSlot *`). C pointer arrays
//! (`PlanState **`, `AsyncRequest **`, `TupleTableSlot **`) become owned
//! `Vec`s of the children; the parallel-coordination `ParallelAppendState` is
//! a DSM-resident struct, modeled here as an owned value (the leader/worker
//! split is threaded as a handle until access/parallel.c lands).

use alloc::vec::Vec;

use mcx::{Mcx, PgBox, PgVec};
use types_error::PgResult;

use crate::bitmapset::Bitmapset;
use crate::execnodes::{PlanStateData, SlotId};
use crate::nodeindexscan::Plan;
use crate::planstate::PlanStateNode;

pub use crate::execstate_tags::T_AppendState;
pub use crate::nodes::T_Append;

/// `Append` plan node (plannodes.h):
///
/// ```c
/// typedef struct Append
/// {
///     Plan        plan;
///     Bitmapset  *apprelids;
///     List       *appendplans;
///     int         nasyncplans;
///     int         first_partial_plan;
///     int         part_prune_index;
/// } Append;
/// ```
#[derive(Debug, Default)]
pub struct Append<'mcx> {
    /// `Plan plan` — its first field starts with the `NodeTag`.
    pub plan: Plan<'mcx>,
    /// `Bitmapset *apprelids` — RTIs of the appendrel(s) formed by this node.
    pub apprelids: Option<PgBox<'mcx, Bitmapset<'mcx>>>,
    /// `List *appendplans` — the child plan nodes.
    pub appendplans: Vec<crate::nodes::Node<'mcx>>,
    /// `int nasyncplans` — number of asynchronous plans.
    pub nasyncplans: i32,
    /// `int first_partial_plan` — index into `appendplans`: all preceding it
    /// are non-partial plans, all from it onward are partial plans.
    pub first_partial_plan: i32,
    /// `int part_prune_index` — index into `PlannedStmt.partPruneInfos` and the
    /// EState parallel lists; `-1` if no run-time pruning is used.
    pub part_prune_index: i32,
}

/// `AsyncRequest` (execnodes.h):
///
/// ```c
/// typedef struct AsyncRequest
/// {
///     struct PlanState *requestor;
///     struct PlanState *requestee;
///     int     request_index;
///     bool    callback_pending;
///     bool    request_complete;
///     TupleTableSlot *result;
/// } AsyncRequest;
/// ```
///
/// The C `requestor`/`requestee` aliasing back-pointers are not carried: the
/// owned tree reaches the requestor (the `AppendState`) and requestee
/// (`appendplans[request_index]`) through `request_index`, the load-bearing
/// field. `result` is the delivered tuple's arena [`SlotId`] (C's
/// `TupleTableSlot *`), `None` when no more tuples.
#[derive(Clone, Debug)]
pub struct AsyncRequestData {
    /// `int request_index` — scratch space for the requestor.
    pub request_index: i32,
    /// `bool callback_pending` — a callback is needed.
    pub callback_pending: bool,
    /// `bool request_complete` — request complete, `result` valid.
    pub request_complete: bool,
    /// `TupleTableSlot *result` — result (`None` if no more tuples).
    pub result: Option<SlotId>,
}

/// `ParallelAppendState` (nodeAppend.c, file-private). Shared-memory
/// coordination state for parallel-aware Append:
///
/// ```c
/// struct ParallelAppendState
/// {
///     LWLock      pa_lock;
///     int         pa_next_plan;
///     bool        pa_finished[FLEXIBLE_ARRAY_MEMBER];
/// };
/// ```
#[derive(Debug)]
pub struct ParallelAppendState {
    /// `LWLock pa_lock` — mutual exclusion to choose the next subplan.
    pub pa_lock: types_storage::LWLock,
    /// `int pa_next_plan` — next plan to choose by any worker.
    pub pa_next_plan: i32,
    /// `bool pa_finished[FLEXIBLE_ARRAY_MEMBER]` — per-subplan "finished"
    /// flags.
    pub pa_finished: Vec<bool>,
}

/// `PartitionPruneState` (execPartition.h), trimmed to the fields the Append
/// node reads (`do_exec_prune` at init, `execparamids` at rescan). It is the
/// same trimmed type the MergeAppend node consults, so it is defined once in
/// [`crate::nodemergeappend`] and re-used here.
pub use crate::nodemergeappend::PartitionPruneState;

/// `AppendState` (execnodes.h):
///
/// ```c
/// struct AppendState
/// {
///     PlanState   ps;
///     PlanState **appendplans;
///     int         as_nplans;
///     int         as_whichplan;
///     bool        as_begun;
///     Bitmapset  *as_asyncplans;
///     int         as_nasyncplans;
///     AsyncRequest **as_asyncrequests;
///     TupleTableSlot **as_asyncresults;
///     int         as_nasyncresults;
///     bool        as_syncdone;
///     int         as_nasyncremain;
///     Bitmapset  *as_needrequest;
///     struct WaitEventSet *as_eventset;
///     int         as_first_partial_plan;
///     ParallelAppendState *as_pstate;
///     Size        pstate_len;
///     struct PartitionPruneState *as_prune_state;
///     bool        as_valid_subplans_identified;
///     Bitmapset  *as_valid_subplans;
///     Bitmapset  *as_valid_asyncplans;
///     bool        (*choose_next_subplan) (AppendState *);
/// };
/// ```
///
/// `as_eventset` is not carried: the C `WaitEventSet *as_eventset` only ever
/// holds a transient set, created and freed within a single
/// `ExecAppendAsyncEventWait` call (it is `NULL` everywhere else). The owned
/// port holds that set as a stack-local guard during the wait, so the node
/// field is unnecessary. The `choose_next_subplan` C function pointer becomes
/// the [`AppendChooseStrategy`] sentinel selected by the node.
#[derive(Debug)]
pub struct AppendStateData<'mcx> {
    /// `PlanState ps`.
    pub ps: PlanStateData<'mcx>,
    /// `PlanState **appendplans` — array of child `PlanState`s (the C
    /// `palloc`ed pointer array; an entry is `None` only transiently during
    /// init).
    pub appendplans: PgVec<'mcx, Option<PgBox<'mcx, PlanStateNode<'mcx>>>>,
    /// `int as_nplans`.
    pub as_nplans: i32,
    /// `int as_whichplan` — index of the currently-active sync subplan.
    pub as_whichplan: i32,
    /// `bool as_begun` — false means the node still needs initialization.
    pub as_begun: bool,
    /// `Bitmapset *as_asyncplans` — indexes of asynchronous plans.
    pub as_asyncplans: Option<PgBox<'mcx, Bitmapset<'mcx>>>,
    /// `int as_nasyncplans` — number of asynchronous plans.
    pub as_nasyncplans: i32,
    /// `AsyncRequest **as_asyncrequests` — array of `AsyncRequest`s
    /// (`palloc0`ed pointer array; an entry is `None` for a non-async index).
    pub as_asyncrequests: PgVec<'mcx, Option<PgBox<'mcx, AsyncRequestData>>>,
    /// `TupleTableSlot **as_asyncresults` — unreturned results of async plans
    /// (arena ids; C's slot-pointer array).
    pub as_asyncresults: PgVec<'mcx, Option<SlotId>>,
    /// `int as_nasyncresults` — number of valid entries in `as_asyncresults`.
    pub as_nasyncresults: i32,
    /// `bool as_syncdone` — true if all sync plans done in async mode.
    pub as_syncdone: bool,
    /// `int as_nasyncremain` — number of remaining asynchronous plans.
    pub as_nasyncremain: i32,
    /// `Bitmapset *as_needrequest` — async plans needing a new request.
    pub as_needrequest: Option<PgBox<'mcx, Bitmapset<'mcx>>>,
    /// `int as_first_partial_plan` — index of `appendplans` containing the
    /// first partial plan.
    pub as_first_partial_plan: i32,
    /// `ParallelAppendState *as_pstate` — parallel coordination info.
    pub as_pstate: Option<alloc::boxed::Box<ParallelAppendState>>,
    /// `Size pstate_len` — size of the parallel coordination info.
    pub pstate_len: usize,
    /// `struct PartitionPruneState *as_prune_state`.
    pub as_prune_state: Option<PgBox<'mcx, PartitionPruneState<'mcx>>>,
    /// `bool as_valid_subplans_identified` — is `as_valid_subplans` valid?
    pub as_valid_subplans_identified: bool,
    /// `Bitmapset *as_valid_subplans`.
    pub as_valid_subplans: Option<PgBox<'mcx, Bitmapset<'mcx>>>,
    /// `Bitmapset *as_valid_asyncplans` — valid asynchronous plan indexes.
    pub as_valid_asyncplans: Option<PgBox<'mcx, Bitmapset<'mcx>>>,
    /// `bool (*choose_next_subplan)(AppendState *)` — the local/leader/worker
    /// selection strategy (the C three-way function pointer).
    pub choose_next_subplan: AppendChooseStrategy,
}

/// The `node->choose_next_subplan` C function pointer modeled as a sentinel.
/// `ExecInitAppend` installs [`Locally`](AppendChooseStrategy::Locally);
/// `ExecAppendInitializeDSM`/`InitializeWorker` override it with
/// [`Leader`](AppendChooseStrategy::Leader)/[`Worker`](AppendChooseStrategy::Worker).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum AppendChooseStrategy {
    /// `choose_next_subplan_locally` (the `ExecInitAppend` default).
    #[default]
    Locally,
    /// `choose_next_subplan_for_leader`.
    Leader,
    /// `choose_next_subplan_for_worker`.
    Worker,
}

impl<'mcx> AppendStateData<'mcx> {
    /// `makeNode(AppendState)` — a zeroed `AppendState` with its `NodeTag`
    /// stamped on the embedded head (C: `makeNode` zeroes the struct).
    pub fn make(mcx: Mcx<'mcx>) -> Self {
        AppendStateData {
            ps: PlanStateData::default(),
            appendplans: PgVec::new_in(mcx),
            as_nplans: 0,
            as_whichplan: 0,
            as_begun: false,
            as_asyncplans: None,
            as_nasyncplans: 0,
            as_asyncrequests: PgVec::new_in(mcx),
            as_asyncresults: PgVec::new_in(mcx),
            as_nasyncresults: 0,
            as_syncdone: false,
            as_nasyncremain: 0,
            as_needrequest: None,
            as_first_partial_plan: 0,
            as_pstate: None,
            pstate_len: 0,
            as_prune_state: None,
            as_valid_subplans_identified: false,
            as_valid_subplans: None,
            as_valid_asyncplans: None,
            choose_next_subplan: AppendChooseStrategy::Locally,
        }
    }
}

impl Append<'_> {
    /// Deep copy of the plan node (and its subplan list) into `mcx`
    /// (C: `copyObject` shape). Fallible: copying allocates.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<Append<'b>> {
        let mut appendplans = mcx::vec_with_capacity_in(mcx, self.appendplans.len())?;
        for child in self.appendplans.iter() {
            appendplans.push(child.clone_in(mcx)?);
        }
        Ok(Append {
            plan: self.plan.clone_in(mcx)?,
            apprelids: match &self.apprelids {
                Some(b) => Some(mcx::alloc_in(mcx, b.clone_in(mcx)?)?),
                None => None,
            },
            appendplans: appendplans.into_iter().collect(),
            nasyncplans: self.nasyncplans,
            first_partial_plan: self.first_partial_plan,
            part_prune_index: self.part_prune_index,
        })
    }
}
