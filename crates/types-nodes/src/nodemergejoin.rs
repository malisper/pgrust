//! Merge-join node vocabulary (`nodes/plannodes.h` `MergeJoin`,
//! `executor/execnodes.h` `MergeJoinState`, and the file-local
//! `MergeJoinClauseData` from `executor/nodeMergejoin.c`).
//!
//! The embedded `JoinState`/`PlanState` head reuses [`crate::jointype::JoinStateData`]
//! / [`PlanStateData`], the leading `Join` plan base reuses
//! [`crate::jointype::Join`], the comparison support reuses
//! [`types_sortsupport::SortSupportData`], and the executor-pool aliases follow
//! the owned model ([`SlotId`] for `TupleTableSlot *`, [`EcxtId`] for
//! `ExprContext *`).

use alloc::vec::Vec;

use mcx::{Mcx, PgBox, PgVec};
use types_error::PgResult;
use types_core::primitive::Oid;
use types_datum::Datum;
use types_sortsupport::SortSupportData;

use crate::execexpr::ExprState;
use crate::execnodes::{EcxtId, PlanStateData, SlotId};
use crate::jointype::{Join, JoinStateData};
use crate::nodes::NodeTag;
use crate::primnodes::Expr;

/// `T_MergeJoin` (nodes/nodetags.h) — the plan-node tag for a MergeJoin.
pub const T_MergeJoin: NodeTag = NodeTag(358);
/// `T_MergeJoinState` (nodes/nodetags.h) — the executor-state node tag.
pub const T_MergeJoinState: NodeTag = NodeTag(422);

// States of the ExecMergeJoin state machine (nodeMergejoin.c #defines).
pub const EXEC_MJ_INITIALIZE_OUTER: i32 = 1;
pub const EXEC_MJ_INITIALIZE_INNER: i32 = 2;
pub const EXEC_MJ_JOINTUPLES: i32 = 3;
pub const EXEC_MJ_NEXTOUTER: i32 = 4;
pub const EXEC_MJ_TESTOUTER: i32 = 5;
pub const EXEC_MJ_NEXTINNER: i32 = 6;
pub const EXEC_MJ_SKIP_TEST: i32 = 7;
pub const EXEC_MJ_SKIPOUTER_ADVANCE: i32 = 8;
pub const EXEC_MJ_SKIPINNER_ADVANCE: i32 = 9;
pub const EXEC_MJ_ENDOUTER: i32 = 10;
pub const EXEC_MJ_ENDINNER: i32 = 11;

/// `MergeJoin` plan node (plannodes.h):
///
/// ```c
/// typedef struct MergeJoin
/// {
///     Join        join;
///     bool        skip_mark_restore;
///     List       *mergeclauses;
///     Oid        *mergeFamilies;
///     Oid        *mergeCollations;
///     bool       *mergeReversals;
///     bool       *mergeNullsFirst;
/// } MergeJoin;
/// ```
#[derive(Debug, Default)]
pub struct MergeJoin<'mcx> {
    /// `Join join` — its first field (`plan`) starts with the `NodeTag`.
    pub join: Join<'mcx>,
    /// `bool skip_mark_restore` — can we skip mark/restore calls?
    pub skip_mark_restore: bool,
    /// `List *mergeclauses` — mergeclauses as `OpExpr` expression trees.
    pub mergeclauses: Vec<Expr>,
    /// `Oid *mergeFamilies` — per-clause OIDs of btree opfamilies.
    pub mergeFamilies: Vec<Oid>,
    /// `Oid *mergeCollations` — per-clause OIDs of collations.
    pub mergeCollations: Vec<Oid>,
    /// `bool *mergeReversals` — per-clause ordering (ASC or DESC).
    pub mergeReversals: Vec<bool>,
    /// `bool *mergeNullsFirst` — per-clause nulls ordering.
    pub mergeNullsFirst: Vec<bool>,
}

impl MergeJoin<'_> {
    /// Deep copy into `mcx` (C: `copyObject` shape). Fallible: copying the
    /// embedded join/plan subtree allocates.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<MergeJoin<'b>> {
        Ok(MergeJoin {
            join: self.join.clone_in(mcx)?,
            skip_mark_restore: self.skip_mark_restore,
            mergeclauses: self.mergeclauses.clone(),
            mergeFamilies: self.mergeFamilies.clone(),
            mergeCollations: self.mergeCollations.clone(),
            mergeReversals: self.mergeReversals.clone(),
            mergeNullsFirst: self.mergeNullsFirst.clone(),
        })
    }
}

/// `MergeJoinClauseData` (file-local to nodeMergejoin.c) — runtime data for one
/// mergejoin clause:
///
/// ```c
/// typedef struct MergeJoinClauseData
/// {
///     ExprState  *lexpr;
///     ExprState  *rexpr;
///     Datum       ldatum;
///     Datum       rdatum;
///     bool        lisnull;
///     bool        risnull;
///     SortSupportData ssup;
/// } MergeJoinClauseData;
/// ```
#[derive(Debug)]
pub struct MergeJoinClauseData<'mcx> {
    /// `ExprState *lexpr` — compiled left-hand (outer) input expression.
    pub lexpr: Option<PgBox<'mcx, ExprState>>,
    /// `ExprState *rexpr` — compiled right-hand (inner) input expression.
    pub rexpr: Option<PgBox<'mcx, ExprState>>,
    /// `Datum ldatum` — current left-hand value.
    pub ldatum: Datum,
    /// `Datum rdatum` — current right-hand value.
    pub rdatum: Datum,
    /// `bool lisnull` — left value is null.
    pub lisnull: bool,
    /// `bool risnull` — right value is null.
    pub risnull: bool,
    /// `SortSupportData ssup` — everything needed to compare the two values.
    pub ssup: SortSupportData<'mcx>,
}

impl<'mcx> MergeJoinClauseData<'mcx> {
    /// A zeroed clause (the C `palloc0(... sizeof(MergeJoinClauseData))`
    /// element), with its sort-support context set to `mcx` (matching
    /// `MJExamineQuals`'s `ssup.ssup_cxt = CurrentMemoryContext`).
    pub fn zeroed(mcx: Mcx<'mcx>) -> Self {
        MergeJoinClauseData {
            lexpr: None,
            rexpr: None,
            ldatum: Datum::null(),
            rdatum: Datum::null(),
            lisnull: false,
            risnull: false,
            ssup: SortSupportData::new(mcx),
        }
    }
}

/// `MergeJoinState` (execnodes.h) — the per-node execution state of a merge join.
#[derive(Debug)]
pub struct MergeJoinStateData<'mcx> {
    /// `JoinState js` — its first field is `NodeTag`.
    pub js: JoinStateData<'mcx>,
    /// `int mj_NumClauses`.
    pub mj_NumClauses: i32,
    /// `MergeJoinClause mj_Clauses` — array of length `mj_NumClauses`.
    pub mj_Clauses: PgVec<'mcx, MergeJoinClauseData<'mcx>>,
    /// `int mj_JoinState` — the `EXEC_MJ_*` state-machine state.
    pub mj_JoinState: i32,
    /// `bool mj_SkipMarkRestore`.
    pub mj_SkipMarkRestore: bool,
    /// `bool mj_ExtraMarks`.
    pub mj_ExtraMarks: bool,
    /// `bool mj_ConstFalseJoin`.
    pub mj_ConstFalseJoin: bool,
    /// `bool mj_FillOuter`.
    pub mj_FillOuter: bool,
    /// `bool mj_FillInner`.
    pub mj_FillInner: bool,
    /// `bool mj_MatchedOuter`.
    pub mj_MatchedOuter: bool,
    /// `bool mj_MatchedInner`.
    pub mj_MatchedInner: bool,
    /// `TupleTableSlot *mj_OuterTupleSlot` — id into `es_tupleTable`.
    pub mj_OuterTupleSlot: Option<SlotId>,
    /// `TupleTableSlot *mj_InnerTupleSlot` — id into `es_tupleTable`.
    pub mj_InnerTupleSlot: Option<SlotId>,
    /// `TupleTableSlot *mj_MarkedTupleSlot` — id into `es_tupleTable`.
    pub mj_MarkedTupleSlot: Option<SlotId>,
    /// `TupleTableSlot *mj_NullOuterTupleSlot` — id into `es_tupleTable`.
    pub mj_NullOuterTupleSlot: Option<SlotId>,
    /// `TupleTableSlot *mj_NullInnerTupleSlot` — id into `es_tupleTable`.
    pub mj_NullInnerTupleSlot: Option<SlotId>,
    /// `ExprContext *mj_OuterEContext` — id into `es_exprcontexts`.
    pub mj_OuterEContext: Option<EcxtId>,
    /// `ExprContext *mj_InnerEContext` — id into `es_exprcontexts`.
    pub mj_InnerEContext: Option<EcxtId>,
}

impl<'mcx> MergeJoinStateData<'mcx> {
    /// `makeNode(MergeJoinState)` — a zeroed merge-join state with its clause
    /// array empty (`PgVec::new_in(mcx)`) and all `EXEC_MJ_*`/slot/econtext
    /// fields cleared, matching the C `palloc0`. The clause `PgVec` is the one
    /// field that cannot derive `Default` (it carries the allocator handle), so
    /// node construction goes through this constructor.
    pub fn new(mcx: Mcx<'mcx>) -> Self {
        MergeJoinStateData {
            js: JoinStateData::default(),
            mj_NumClauses: 0,
            mj_Clauses: PgVec::new_in(mcx),
            mj_JoinState: 0,
            mj_SkipMarkRestore: false,
            mj_ExtraMarks: false,
            mj_ConstFalseJoin: false,
            mj_FillOuter: false,
            mj_FillInner: false,
            mj_MatchedOuter: false,
            mj_MatchedInner: false,
            mj_OuterTupleSlot: None,
            mj_InnerTupleSlot: None,
            mj_MarkedTupleSlot: None,
            mj_NullOuterTupleSlot: None,
            mj_NullInnerTupleSlot: None,
            mj_OuterEContext: None,
            mj_InnerEContext: None,
        }
    }

    /// `&node->js.ps` — the embedded `PlanState` head.
    #[inline]
    pub fn ps(&self) -> &PlanStateData<'mcx> {
        &self.js.ps
    }

    /// `&mut node->js.ps`.
    #[inline]
    pub fn ps_mut(&mut self) -> &mut PlanStateData<'mcx> {
        &mut self.js.ps
    }
}
