//! `#[repr(C)]` ABI for `nodeNestloop.c` (the nest-loop join executor node).
//!
//! The nest-loop node is ported in-crate (`backend-executor-nodeNestloop`), so
//! its state node is a complete, address-stable `#[repr(C)]` struct laid out
//! exactly like the C `NestLoopState` (execnodes.h). The `NestLoop`/`Join` plan
//! nodes and the `NestLoopParam` it navigates are spelled out here too.
//!
//! The embedded `PlanState` head reuses the shared [`crate::PlanStateData`]
//! layout defined in `execnodes`, and the leading `Plan` of the `Join`/`NestLoop`
//! plan nodes is the abstract [`crate::PlanNode`] base. The outer-relation `Var`
//! referenced from `NestLoopParam.paramval` reuses the shared
//! [`crate::nodetidscan_abi::Var`] layout.

use core::ffi::{c_int, c_uint};

use crate::nodetidscan_abi::Var;
use crate::{ExprState, List, NodeTag, PlanNode, PlanStateData, TupleTableSlot};

/// `JoinType` (nodes/nodes.h) — discriminant of the join semantics. The
/// nest-loop node compares `jointype` against `JOIN_LEFT`/`JOIN_ANTI`/`JOIN_SEMI`
/// and errors on anything outside the recognised set. Canonical definition (with
/// the `JOIN_*` constants) lives in [`crate::pathnodes`]; re-exported here so the
/// executor and planner share one type. `c_uint` and `u32` are ABI-identical.
pub use crate::pathnodes::{
    JoinType, JOIN_ANTI, JOIN_FULL, JOIN_INNER, JOIN_LEFT, JOIN_RIGHT, JOIN_RIGHT_ANTI,
    JOIN_RIGHT_SEMI, JOIN_SEMI, JOIN_UNIQUE_INNER, JOIN_UNIQUE_OUTER,
};

/// `Join` plan node (plannodes.h), the abstract base of all join plan nodes:
///
/// ```c
/// typedef struct Join
/// {
///     pg_node_attr(abstract)
///     Plan        plan;
///     JoinType    jointype;
///     bool        inner_unique;
///     List       *joinqual;       /* JOIN quals (in addition to plan.qual) */
/// } Join;
/// ```
///
/// The leading `plan` is the abstract [`PlanNode`] base (its first field is the
/// `NodeTag`), so a `*mut Join` is also a valid `Node *` / `Plan *`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct Join {
    /// `Plan plan` — abstract plan-node base.
    pub plan: PlanNode,
    /// `JoinType jointype`.
    pub jointype: JoinType,
    /// `bool inner_unique`.
    pub inner_unique: bool,
    /// `List *joinqual` — JOIN quals (in addition to `plan.qual`).
    pub joinqual: *mut List,
}

/// `NestLoop` plan node (plannodes.h):
///
/// ```c
/// typedef struct NestLoop
/// {
///     Join        join;
///     List       *nestParams;     /* list of NestLoopParam nodes */
/// } NestLoop;
/// ```
///
/// The leading `join.plan` is the abstract [`PlanNode`] base, so a
/// `*mut NestLoop` is also a valid `Node *` / `Plan *`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct NestLoop {
    /// `Join join` — its first field (`plan`) starts with the `NodeTag`.
    pub join: Join,
    /// `List *nestParams` — list of `NestLoopParam` nodes.
    pub nestParams: *mut List,
}

/// `NestLoopParam` (plannodes.h) — describes one outer-relation `Var` that must
/// be passed down into the inner scan via a `PARAM_EXEC` slot:
///
/// ```c
/// typedef struct NestLoopParam
/// {
///     pg_node_attr(no_equal, no_query_jumble)
///     NodeTag     type;
///     int         paramno;        /* number of the PARAM_EXEC Param to set */
///     Var        *paramval;       /* outer-relation Var to assign to Param */
/// } NestLoopParam;
/// ```
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct NestLoopParam {
    /// `NodeTag type`.
    pub type_: NodeTag,
    /// `int paramno` — number of the `PARAM_EXEC` Param to set.
    pub paramno: c_int,
    /// `Var *paramval` — outer-relation `Var` to assign to the Param.
    pub paramval: *mut Var,
}

/// `JoinState` (execnodes.h) — the common base of the join state nodes:
///
/// ```c
/// typedef struct JoinState
/// {
///     PlanState   ps;
///     JoinType    jointype;
///     bool        single_match;   /* True if we should skip to next outer tuple
///                                  * after finding one inner match */
///     ExprState  *joinqual;       /* JOIN quals (in addition to ps.qual) */
/// } JoinState;
/// ```
///
/// `ps`'s first field is a `NodeTag`, so a `*mut JoinStateData` is also a valid
/// `Node *` / `PlanState *`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct JoinStateData {
    /// `PlanState ps` — its first field is `NodeTag`.
    pub ps: PlanStateData,
    /// `JoinType jointype`.
    pub jointype: JoinType,
    /// `bool single_match` — skip to next outer tuple after one inner match.
    pub single_match: bool,
    /// `ExprState *joinqual` — JOIN quals (in addition to `ps.qual`).
    pub joinqual: *mut ExprState,
}

/// `NestLoopState` (execnodes.h) — the per-node execution state of a nest-loop
/// join:
///
/// ```c
/// typedef struct NestLoopState
/// {
///     JoinState   js;             /* its first field is NodeTag */
///     bool        nl_NeedNewOuter;
///     bool        nl_MatchedOuter;
///     TupleTableSlot *nl_NullInnerTupleSlot;
/// } NestLoopState;
/// ```
///
/// The leading [`JoinStateData`] head's first member is a `NodeTag`, so a
/// `*mut NestLoopStateData` is also a valid `Node *` / `PlanState *`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct NestLoopStateData {
    /// `JoinState js` — its first field is `NodeTag`.
    pub js: JoinStateData,
    /// `bool nl_NeedNewOuter` — need to fetch a fresh outer tuple?
    pub nl_NeedNewOuter: bool,
    /// `bool nl_MatchedOuter` — has the current outer tuple matched any inner?
    pub nl_MatchedOuter: bool,
    /// `TupleTableSlot *nl_NullInnerTupleSlot` — all-NULL inner slot for outer
    /// joins.
    pub nl_NullInnerTupleSlot: *mut TupleTableSlot,
}

// Layout asserts: the embedded heads must keep their C offsets so a
// `*mut NestLoopStateData` can be navigated as the C `NestLoopState *`, and a
// `*mut NestLoop` as the C `NestLoop *`.
const _: () = {
    use core::mem::{offset_of, size_of};

    // NestLoopState: JoinState at offset 0 (so `&self` is a valid Node*/PlanState*).
    assert!(offset_of!(NestLoopStateData, js) == 0);
    // js.ps at offset 0; its first field is the NodeTag.
    assert!(offset_of!(JoinStateData, ps) == 0);
    assert!(offset_of!(PlanStateData, type_) == 0);
    // nl_NeedNewOuter follows the JoinState head.
    assert!(offset_of!(NestLoopStateData, nl_NeedNewOuter) == size_of::<JoinStateData>());

    // NestLoop: Join at offset 0; Join.plan at offset 0 (PlanNode base with NodeTag).
    assert!(offset_of!(NestLoop, join) == 0);
    assert!(offset_of!(Join, plan) == 0);
    assert!(offset_of!(PlanNode, type_) == 0);
    // nestParams follows the Join head.
    assert!(offset_of!(NestLoop, nestParams) == size_of::<Join>());

    // NestLoopParam: NodeTag at offset 0.
    assert!(offset_of!(NestLoopParam, type_) == 0);
};
