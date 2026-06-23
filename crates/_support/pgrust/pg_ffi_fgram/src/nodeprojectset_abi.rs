//! `#[repr(C)]` ABI for `nodeProjectSet.c` (the ProjectSet executor node).
//!
//! The ProjectSet node is ported in-crate
//! (`backend-executor-nodeProjectSet`), so its state node is a complete,
//! address-stable `#[repr(C)]` struct laid out exactly like the C
//! `ProjectSetState` (execnodes.h). The `ProjectSet` plan node (plannodes.h) and
//! the two target-list expression nodes its init walker inspects (`FuncExpr`,
//! `OpExpr`) are spelled out so `funcretset`/`opretset` resolve to the same
//! offsets the C compiler produces.
//!
//! The embedded `PlanState` head reuses the shared [`crate::PlanStateData`]
//! layout defined in `execnodes`; the `Plan` head reuses [`crate::PlanNode`].

use core::ffi::c_int;

use crate::{
    ExprDoneCond, List, MemoryContext, Node, NodeTag, Oid, ParseLoc, PlanNode, PlanStateData,
};

/// NodeTag for `FuncExpr` (primnodes.h / nodetags.h order).
pub const T_FuncExpr: NodeTag = 15;
/// NodeTag for `SetExprState` (execnodes.h / nodetags.h order). A node-tagged
/// expression-state struct (built by `ExecInitFunctionResultSet`); the
/// ProjectSet init walker recognises these to evaluate them via
/// `ExecMakeFunctionResultSet`.
pub const T_SetExprState: NodeTag = 391;

/// `Expr` — the abstract superclass header carried by every primitive
/// expression node (`primnodes.h`). Its only member is the `NodeTag`. Named
/// `ProjectSetExpr` here to avoid clashing with other modules' `Expr`.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ProjectSetExpr {
    /// `NodeTag type`
    pub type_: NodeTag,
}

/// `CoercionForm` — `FuncExpr.funcformat` display discriminant (primnodes.h).
pub type CoercionForm = c_int;

/// `FuncExpr` (primnodes.h) — a function-application expression. The ProjectSet
/// init walker reads `xpr.type_` (to recognise a `FuncExpr`) and `funcretset`
/// (to decide whether it is a set-returning targetlist entry); the whole struct
/// is spelled out for layout fidelity.
///
/// ```c
/// typedef struct FuncExpr
/// {
///     Expr        xpr;
///     Oid         funcid;
///     Oid         funcresulttype;
///     bool        funcretset;
///     bool        funcvariadic;
///     CoercionForm funcformat;
///     Oid         funccollid;
///     Oid         inputcollid;
///     List       *args;
///     ParseLoc    location;
/// } FuncExpr;
/// ```
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct FuncExpr {
    /// `Expr xpr`
    pub xpr: ProjectSetExpr,
    /// `Oid funcid` — PG_PROC OID of the function.
    pub funcid: Oid,
    /// `Oid funcresulttype` — PG_TYPE OID of result value.
    pub funcresulttype: Oid,
    /// `bool funcretset` — true if function returns set.
    pub funcretset: bool,
    /// `bool funcvariadic` — true if variadic args combined into an array.
    pub funcvariadic: bool,
    /// `CoercionForm funcformat` — how to display this function call.
    pub funcformat: CoercionForm,
    /// `Oid funccollid` — OID of collation of result.
    pub funccollid: Oid,
    /// `Oid inputcollid` — OID of collation that function should use.
    pub inputcollid: Oid,
    /// `List *args` — arguments to the function.
    pub args: *mut List,
    /// `ParseLoc location` — token location, or -1 if unknown.
    pub location: ParseLoc,
}

/// `ProjectSet` plan node (plannodes.h):
///
/// ```c
/// typedef struct ProjectSet
/// {
///     Plan        plan;
/// } ProjectSet;
/// ```
///
/// ProjectSet adds no fields of its own; the node layer reads the embedded
/// `plan.targetlist`/`plan.qual`/`plan.lefttree`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct ProjectSet {
    /// `Plan plan` — the abstract plan-node base (its first field is `NodeTag`).
    pub plan: PlanNode,
}

/// `ProjectSetState` (execnodes.h):
///
/// ```c
/// typedef struct ProjectSetState
/// {
///     PlanState   ps;                 /* its first field is NodeTag */
///     Node      **elems;              /* array of expression states */
///     ExprDoneCond *elemdone;         /* array of per-SRF is-done states */
///     int         nelems;             /* length of elemdone[] array */
///     bool        pending_srf_tuples; /* still evaluating srfs in tlist? */
///     MemoryContext argcontext;       /* context for SRF arguments */
/// } ProjectSetState;
/// ```
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct ProjectSetStateData {
    /// `PlanState ps` — its first field is `NodeTag`, so `&self` is a valid
    /// `Node *` / `PlanState *`.
    pub ps: PlanStateData,
    /// `Node **elems` — array of compiled per-tlist-entry expression states.
    pub elems: *mut *mut Node,
    /// `ExprDoneCond *elemdone` — array of per-SRF is-done states.
    pub elemdone: *mut ExprDoneCond,
    /// `int nelems` — length of the `elems`/`elemdone` arrays.
    pub nelems: c_int,
    /// `bool pending_srf_tuples` — still evaluating SRFs in the tlist?
    pub pending_srf_tuples: bool,
    /// `MemoryContext argcontext` — context for SRF arguments.
    pub argcontext: MemoryContext,
}

// Layout asserts: the embedded heads must keep their C offsets so a
// `*mut ProjectSetStateData` can be navigated as the C `ProjectSetState *`, and
// the trailing node-specific fields follow the embedded `PlanState`.
const _: () = {
    assert!(core::mem::offset_of!(ProjectSetStateData, ps) == 0);
    assert!(core::mem::offset_of!(PlanStateData, type_) == 0);
    // `elems` immediately follows the embedded `PlanState`.
    assert!(
        core::mem::offset_of!(ProjectSetStateData, elems) == core::mem::size_of::<PlanStateData>()
    );
    assert!(core::mem::offset_of!(ProjectSet, plan) == 0);
    assert!(core::mem::offset_of!(PlanNode, type_) == 0);
};
