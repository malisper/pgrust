//! Node-state vocabulary for `backend-executor-nodeSamplescan`.
//!
//! These types appear in the signatures of the node's seams, so they live in a
//! types crate that both the owning node crate and its `-seams` crate can name.
//!
//! `SampleScanState` / `TsmRoutine` mirror `executor/nodeSamplescan.c` and
//! `access/tsmapi.h`. The C node keeps `ss_currentRelation` /
//! `ss_currentScanDesc` in its embedded `ScanState`; the shared
//! [`ScanStateData`] does not carry a table-AM scan descriptor (that would force
//! a `types-nodes -> types-tableam` cycle), so this crate keeps the faithful
//! node shape by holding those two fields on the node-state struct directly
//! (the same arrangement `types-tidrange` uses).

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use mcx::{PgBox, PgVec};
use types_core::primitive::{uint16, uint32, BlockNumber, OffsetNumber, Oid};
use types_datum::datum::Datum;
use types_nodes::execexpr::ExprState;
use types_nodes::execnodes::Opaque;
use types_nodes::nodes::NodeTag;
use types_pathnodes::{PlannerInfo, RelOptInfo};
use types_rel::Relation;
use types_tableam::relscan::TableScanDesc;

pub use types_nodes::execnodes::ScanStateData;
pub use types_nodes::nodesamplescan::{SampleScan, TableSampleClause};

// ===========================================================================
// access/tsmapi.h — tablesample method callback signatures.
//
// These function pointers are owned/installed by the tablesample-method
// registry (`GetTsmRoutine`) and its handler functions, which live above this
// node's layer. The node never invokes them directly (it reaches them through
// the node's seams); the typed signatures are kept faithful to tsmapi.h so the
// owner that lands the registry fills them with real method functions.
// ===========================================================================

/// `SampleScanGetSampleSize_function` (access/tsmapi.h). The C callback pointer
/// is mcx-free (it takes no MemoryContext), so it is modelled as a higher-ranked
/// `for<'mcx> fn(...)` — it works for a node of any context lifetime, mirroring
/// the table-AM / index-AM vtable convention.
pub type SampleScanGetSampleSizeFunction = Option<
    for<'mcx> fn(
        root: Option<Box<PlannerInfo>>,
        baserel: Option<Box<RelOptInfo>>,
        paramexprs: Vec<types_nodes::nodes::Node<'static>>,
        pages: &mut BlockNumber,
        tuples: &mut f64,
    ),
>;

/// `InitSampleScan_function` (access/tsmapi.h). Can be `None`.
pub type InitSampleScanFunction =
    Option<for<'mcx> fn(node: &mut SampleScanState<'mcx>, eflags: i32)>;

/// `BeginSampleScan_function` (access/tsmapi.h).
pub type BeginSampleScanFunction = Option<
    for<'mcx> fn(node: &mut SampleScanState<'mcx>, params: &[Datum], nparams: i32, seed: uint32),
>;

/// `NextSampleBlock_function` (access/tsmapi.h). Can be `None`.
pub type NextSampleBlockFunction =
    Option<for<'mcx> fn(node: &mut SampleScanState<'mcx>, nblocks: BlockNumber) -> BlockNumber>;

/// `NextSampleTuple_function` (access/tsmapi.h).
pub type NextSampleTupleFunction = Option<
    for<'mcx> fn(
        node: &mut SampleScanState<'mcx>,
        blockno: BlockNumber,
        maxoffset: OffsetNumber,
    ) -> uint16,
>;

/// `EndSampleScan_function` (access/tsmapi.h). Can be `None`.
pub type EndSampleScanFunction = Option<for<'mcx> fn(node: &mut SampleScanState<'mcx>)>;

/// `TsmRoutine` (access/tsmapi.h): the struct returned by a tablesample
/// method's handler function.
#[derive(Debug)]
pub struct TsmRoutine {
    /// `NodeTag type`.
    pub type_: NodeTag,
    /// `List *parameterTypes` — datatype OIDs for the TABLESAMPLE clause args.
    pub parameterTypes: Vec<Oid>,
    /// `bool repeatable_across_queries`.
    pub repeatable_across_queries: bool,
    /// `bool repeatable_across_scans`.
    pub repeatable_across_scans: bool,
    /// `SampleScanGetSampleSize_function SampleScanGetSampleSize`.
    pub SampleScanGetSampleSize: SampleScanGetSampleSizeFunction,
    /// `InitSampleScan_function InitSampleScan` (can be `None`).
    pub InitSampleScan: InitSampleScanFunction,
    /// `BeginSampleScan_function BeginSampleScan`.
    pub BeginSampleScan: BeginSampleScanFunction,
    /// `NextSampleBlock_function NextSampleBlock` (can be `None`).
    pub NextSampleBlock: NextSampleBlockFunction,
    /// `NextSampleTuple_function NextSampleTuple`.
    pub NextSampleTuple: NextSampleTupleFunction,
    /// `EndSampleScan_function EndSampleScan` (can be `None`).
    pub EndSampleScan: EndSampleScanFunction,
}

/// `SampleScanState` (execnodes.h) — the sample-scan executor node state. The
/// leading `ss` field's first member is a `NodeTag`.
pub struct SampleScanState<'mcx> {
    /// `ScanState ss` — its first field is `NodeTag`.
    pub ss: ScanStateData<'mcx>,
    /// `Relation ss.ss_currentRelation` — the relation being scanned (held on
    /// the node-state struct; see the module note on why it is not on the
    /// shared `ScanStateData`).
    pub ss_currentRelation: Option<Relation<'mcx>>,
    /// `TableScanDesc ss.ss_currentScanDesc` — the active table-AM scan
    /// descriptor, `None` until `table_beginscan_sampling`.
    pub ss_currentScanDesc: Option<TableScanDesc<'mcx>>,
    /// `List *args` — expr states for TABLESAMPLE params (`ExprState`s).
    pub args: PgVec<'mcx, PgBox<'mcx, ExprState<'mcx>>>,
    /// `ExprState *repeatable` — expr state for the REPEATABLE expr.
    pub repeatable: Option<PgBox<'mcx, ExprState<'mcx>>>,
    /// `struct TsmRoutine *tsmroutine` — descriptor for the tablesample method.
    pub tsmroutine: Option<PgBox<'mcx, TsmRoutine>>,
    /// `void *tsm_state` — the tablesample method's own opaque scratch state.
    pub tsm_state: Option<Opaque>,
    /// `bool use_bulkread` — use bulkread buffer access strategy?
    pub use_bulkread: bool,
    /// `bool use_pagemode` — use page-at-a-time visibility checking?
    pub use_pagemode: bool,
    /// `bool begun` — false means we need to call BeginSampleScan.
    pub begun: bool,
    /// `uint32 seed` — random seed.
    pub seed: uint32,
    /// `int64 donetuples` — number of tuples already returned.
    pub donetuples: i64,
    /// `bool haveblock` — has a block for sampling been determined?
    pub haveblock: bool,
    /// `bool done` — exhausted all tuples?
    pub done: bool,
}

impl core::fmt::Debug for SampleScanState<'_> {
    // `TableScanDesc` / `Relation` / `TsmRoutine` fn-pointers are not `Debug`;
    // print only the scalar scan-state fields (enough for the carrier's
    // panic/debug messages, which is the only consumer).
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("SampleScanState")
            .field("use_bulkread", &self.use_bulkread)
            .field("use_pagemode", &self.use_pagemode)
            .field("begun", &self.begun)
            .field("seed", &self.seed)
            .field("donetuples", &self.donetuples)
            .field("haveblock", &self.haveblock)
            .field("done", &self.done)
            .finish_non_exhaustive()
    }
}

// ===========================================================================
// SampleScanStateLive carrier impl — `SampleScanState *` rides the central
// `types_nodes::PlanStateNode::SampleScan` variant as a tag-checked erased
// trait object (this crate sits ABOVE `types-nodes`, so the dispatch crate
// cannot name `SampleScanState` directly). The impl lives here to satisfy the
// orphan rule (the concrete type is local to this crate). Mirrors the
// `AggStateData` -> `AggStateLive` carrier in `backend-executor-nodeAgg`.
// ===========================================================================

/// `T_SampleScanState = 404` (nodes/nodetags.h).
const T_SampleScanState: NodeTag = NodeTag(404);

impl<'mcx> types_nodes::samplescanstate_carrier::SampleScanStateLive<'mcx>
    for SampleScanState<'mcx>
{
    fn sample_scan_state_tag(&self) -> u64 {
        types_nodes::samplescanstate_carrier::SAMPLE_SCAN_STATE_TAG
    }

    fn live_type_name(&self) -> &'static str {
        types_nodes::samplescanstate_carrier::live_type_name_of::<SampleScanState<'mcx>>()
    }

    fn tag(&self) -> NodeTag {
        T_SampleScanState
    }

    fn ps(&self) -> &types_nodes::execnodes::PlanStateData<'mcx> {
        &self.ss.ps
    }

    fn ps_mut(&mut self) -> &mut types_nodes::execnodes::PlanStateData<'mcx> {
        &mut self.ss.ps
    }

    fn ss(&self) -> &ScanStateData<'mcx> {
        &self.ss
    }
}

impl<'mcx> types_nodes::samplescanstate_carrier::SampleScanStateTagged<'mcx>
    for SampleScanState<'mcx>
{
    const TAG: u64 = types_nodes::samplescanstate_carrier::SAMPLE_SCAN_STATE_TAG;
}

// ===========================================================================
// SampleScanDriver impl — the `scanstate` the heap-AM `scan_sample_next_block`
// / `scan_sample_next_tuple` handlers pass through to the tablesample method's
// `tsm->NextSampleBlock` / `tsm->NextSampleTuple` callbacks. The heap-AM crate
// sits BELOW this one and cannot name `SampleScanState`, so it receives the
// node as `&mut dyn SampleScanDriver`; this impl forwards each method to the
// node's `tsmroutine` callback (passing the node itself as the C `scanstate`).
// ===========================================================================

impl<'mcx> types_tableam::tableam::SampleScanDriver for SampleScanState<'mcx> {
    fn has_next_sample_block(&self) -> bool {
        self.tsmroutine
            .as_ref()
            .expect("SampleScanDriver: node->tsmroutine is NULL")
            .NextSampleBlock
            .is_some()
    }

    fn next_sample_block(&mut self, nblocks: BlockNumber) -> BlockNumber {
        let f = self
            .tsmroutine
            .as_ref()
            .expect("SampleScanDriver: node->tsmroutine is NULL")
            .NextSampleBlock
            .expect("next_sample_block called but tsm->NextSampleBlock is NULL");
        f(self, nblocks)
    }

    fn next_sample_tuple(
        &mut self,
        blockno: BlockNumber,
        maxoffset: OffsetNumber,
    ) -> OffsetNumber {
        let f = self
            .tsmroutine
            .as_ref()
            .expect("SampleScanDriver: node->tsmroutine is NULL")
            .NextSampleTuple
            .expect("next_sample_tuple called but tsm->NextSampleTuple is NULL");
        f(self, blockno, maxoffset)
    }
}
