//! `#[repr(C)]` ABI for `nodeMergejoin.c` (the merge-join executor node).
//!
//! The merge-join node is ported in-crate (`backend-executor-nodeMergejoin`), so
//! its state node is a complete, address-stable `#[repr(C)]` struct laid out
//! exactly like the C `MergeJoinState` (execnodes.h). The `MergeJoin` plan node
//! and the per-clause runtime struct `MergeJoinClauseData` (a file-local type in
//! `nodeMergejoin.c`) it navigates are spelled out here too.
//!
//! The embedded `JoinState`/`PlanState` head reuses the shared
//! [`crate::JoinStateData`] / [`crate::PlanStateData`] layouts, the leading
//! `Join` plan base reuses [`crate::Join`], and the comparison support reuses the
//! shared [`crate::SortSupportData`].

use crate::{
    Datum, ExprContext, ExprState, Join, JoinStateData, List, NodeTag, Oid, SortSupportData,
    TupleTableSlot,
};

/// NodeTag for `Const` (primnodes.h / nodetags.h order). The merge-join init code
/// uses this to recognise constant join quals in `check_constant_qual`.
pub const T_Const: NodeTag = 7;

/// `MergeJoinClauseData` (file-local to `nodeMergejoin.c`) — runtime data for one
/// mergejoin clause:
///
/// ```c
/// typedef struct MergeJoinClauseData
/// {
///     ExprState  *lexpr;          /* left-hand (outer) input expression */
///     ExprState  *rexpr;          /* right-hand (inner) input expression */
///     Datum       ldatum;         /* current left-hand value */
///     Datum       rdatum;         /* current right-hand value */
///     bool        lisnull;        /* and their isnull flags */
///     bool        risnull;
///     SortSupportData ssup;
/// } MergeJoinClauseData;
/// ```
///
/// The node addresses an array of these (`MergeJoinClause` = `*mut
/// MergeJoinClauseData`) of length `mj_NumClauses`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct MergeJoinClauseData {
    /// `ExprState *lexpr` — left-hand (outer) input expression.
    pub lexpr: *mut ExprState,
    /// `ExprState *rexpr` — right-hand (inner) input expression.
    pub rexpr: *mut ExprState,
    /// `Datum ldatum` — current left-hand value.
    pub ldatum: Datum,
    /// `Datum rdatum` — current right-hand value.
    pub rdatum: Datum,
    /// `bool lisnull` — left value is null.
    pub lisnull: bool,
    /// `bool risnull` — right value is null.
    pub risnull: bool,
    /// `SortSupportData ssup` — everything needed to compare the two values.
    pub ssup: SortSupportData,
}

/// `MergeJoin` plan node (plannodes.h):
///
/// ```c
/// typedef struct MergeJoin
/// {
///     Join        join;
///     bool        skip_mark_restore;  /* Can we skip mark/restore calls? */
///     List       *mergeclauses;       /* mergeclauses as expression trees */
///     /* these are arrays, same length as the mergeclauses list: */
///     Oid        *mergeFamilies;      /* per-clause OIDs of btree opfamilies */
///     Oid        *mergeCollations;    /* per-clause OIDs of collations */
///     bool       *mergeReversals;     /* per-clause ordering (ASC or DESC) */
///     bool       *mergeNullsFirst;    /* per-clause nulls ordering */
/// } MergeJoin;
/// ```
///
/// The leading `join.plan` is the abstract [`crate::PlanNode`] base (its first
/// field is the `NodeTag`), so a `*mut MergeJoin` is also a valid `Node *` /
/// `Plan *`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct MergeJoin {
    /// `Join join` — its first field (`plan`) starts with the `NodeTag`.
    pub join: Join,
    /// `bool skip_mark_restore` — can we skip mark/restore calls?
    pub skip_mark_restore: bool,
    /// `List *mergeclauses` — mergeclauses as expression trees.
    pub mergeclauses: *mut List,
    /// `Oid *mergeFamilies` — per-clause OIDs of btree opfamilies.
    pub mergeFamilies: *mut Oid,
    /// `Oid *mergeCollations` — per-clause OIDs of collations.
    pub mergeCollations: *mut Oid,
    /// `bool *mergeReversals` — per-clause ordering (ASC or DESC).
    pub mergeReversals: *mut bool,
    /// `bool *mergeNullsFirst` — per-clause nulls ordering.
    pub mergeNullsFirst: *mut bool,
}

/// `MergeJoinState` (execnodes.h) — the per-node execution state of a merge join:
///
/// ```c
/// typedef struct MergeJoinState
/// {
///     JoinState   js;             /* its first field is NodeTag */
///     int         mj_NumClauses;
///     MergeJoinClause mj_Clauses; /* array of length mj_NumClauses */
///     int         mj_JoinState;
///     bool        mj_SkipMarkRestore;
///     bool        mj_ExtraMarks;
///     bool        mj_ConstFalseJoin;
///     bool        mj_FillOuter;
///     bool        mj_FillInner;
///     bool        mj_MatchedOuter;
///     bool        mj_MatchedInner;
///     TupleTableSlot *mj_OuterTupleSlot;
///     TupleTableSlot *mj_InnerTupleSlot;
///     TupleTableSlot *mj_MarkedTupleSlot;
///     TupleTableSlot *mj_NullOuterTupleSlot;
///     TupleTableSlot *mj_NullInnerTupleSlot;
///     ExprContext *mj_OuterEContext;
///     ExprContext *mj_InnerEContext;
/// } MergeJoinState;
/// ```
///
/// The leading [`JoinStateData`] head's first member is a `NodeTag`, so a
/// `*mut MergeJoinStateData` is also a valid `Node *` / `PlanState *`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct MergeJoinStateData {
    /// `JoinState js` — its first field is `NodeTag`.
    pub js: JoinStateData,
    /// `int mj_NumClauses`.
    pub mj_NumClauses: core::ffi::c_int,
    /// `MergeJoinClause mj_Clauses` — array of length `mj_NumClauses`.
    pub mj_Clauses: *mut MergeJoinClauseData,
    /// `int mj_JoinState` — the `EXEC_MJ_*` state-machine state.
    pub mj_JoinState: core::ffi::c_int,
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
    /// `TupleTableSlot *mj_OuterTupleSlot`.
    pub mj_OuterTupleSlot: *mut TupleTableSlot,
    /// `TupleTableSlot *mj_InnerTupleSlot`.
    pub mj_InnerTupleSlot: *mut TupleTableSlot,
    /// `TupleTableSlot *mj_MarkedTupleSlot`.
    pub mj_MarkedTupleSlot: *mut TupleTableSlot,
    /// `TupleTableSlot *mj_NullOuterTupleSlot`.
    pub mj_NullOuterTupleSlot: *mut TupleTableSlot,
    /// `TupleTableSlot *mj_NullInnerTupleSlot`.
    pub mj_NullInnerTupleSlot: *mut TupleTableSlot,
    /// `ExprContext *mj_OuterEContext`.
    pub mj_OuterEContext: *mut ExprContext,
    /// `ExprContext *mj_InnerEContext`.
    pub mj_InnerEContext: *mut ExprContext,
}

// Layout asserts: the embedded heads must keep their C offsets so a
// `*mut MergeJoinStateData` can be navigated as the C `MergeJoinState *`, and a
// `*mut MergeJoin` as the C `MergeJoin *`.
const _: () = {
    use core::mem::{offset_of, size_of};

    // MergeJoinState: JoinState at offset 0 (so `&self` is a valid Node*/PlanState*).
    assert!(offset_of!(MergeJoinStateData, js) == 0);
    assert!(offset_of!(JoinStateData, ps) == 0);
    // mj_NumClauses follows the JoinState head.
    assert!(offset_of!(MergeJoinStateData, mj_NumClauses) == size_of::<JoinStateData>());

    // MergeJoin: Join at offset 0; Join.plan at offset 0 (PlanNode base with NodeTag).
    assert!(offset_of!(MergeJoin, join) == 0);
    assert!(offset_of!(Join, plan) == 0);
    // skip_mark_restore follows the Join head.
    assert!(offset_of!(MergeJoin, skip_mark_restore) == size_of::<Join>());

    // MergeJoinClauseData: lexpr at offset 0.
    assert!(offset_of!(MergeJoinClauseData, lexpr) == 0);
};
