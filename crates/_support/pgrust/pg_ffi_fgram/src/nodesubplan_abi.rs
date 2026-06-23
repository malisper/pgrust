//! `SubPlan` plan node + `SubPlanState` ABI for `nodeSubplan.c`.
//!
//! `nodeSubplan.c` allocates a `SubPlanState` (`makeNode`) and populates its
//! fields directly, and reads many fields of the `SubPlan` expression-plan
//! node. Crossing the crate boundary these are address-stable `#[repr(C)]`
//! structs laid out field-for-field against PostgreSQL 18.3; only the address
//! travels through the public entry points, and the node code navigates the
//! layout for the fields it touches (mirroring `node->subplan`,
//! `node->planstate`, `subplan->subLinkType`, ...).

use core::ffi::{c_char, c_int, c_void};

use crate::execnodes::PlanStateData;
use crate::funcapi::ExprContext;
use crate::heaptuple::TupleDesc;
use crate::nodeagg_abi::TupleHashTable;
use crate::{
    int32, AttrNumber, Datum, ExprState, FmgrInfo, HeapTuple, List, MemoryContext, Node, NodeTag,
    Oid,
};

/// `SubLinkType` (primnodes.h) — the kind of sub-select the `SubPlan`
/// represents. The discriminants match the C enum order exactly.
///
/// ```c
/// typedef enum SubLinkType {
///     EXISTS_SUBLINK, ALL_SUBLINK, ANY_SUBLINK, ROWCOMPARE_SUBLINK,
///     EXPR_SUBLINK, MULTIEXPR_SUBLINK, ARRAY_SUBLINK,
///     CTE_SUBLINK,        /* for SubPlans only */
/// } SubLinkType;
/// ```
pub type SubLinkType = c_int;
/// `EXISTS_SUBLINK`.
pub const EXISTS_SUBLINK: SubLinkType = 0;
/// `ALL_SUBLINK`.
pub const ALL_SUBLINK: SubLinkType = 1;
/// `ANY_SUBLINK`.
pub const ANY_SUBLINK: SubLinkType = 2;
/// `ROWCOMPARE_SUBLINK`.
pub const ROWCOMPARE_SUBLINK: SubLinkType = 3;
/// `EXPR_SUBLINK`.
pub const EXPR_SUBLINK: SubLinkType = 4;
/// `MULTIEXPR_SUBLINK`.
pub const MULTIEXPR_SUBLINK: SubLinkType = 5;
/// `ARRAY_SUBLINK`.
pub const ARRAY_SUBLINK: SubLinkType = 6;
/// `CTE_SUBLINK` (for `SubPlan`s only).
pub const CTE_SUBLINK: SubLinkType = 7;

/// `SubPlan` expression-plan node (primnodes.h):
///
/// ```c
/// typedef struct SubPlan {
///     Expr        xpr;            /* { NodeTag type; } */
///     SubLinkType subLinkType;
///     Node       *testexpr;       /* OpExpr or RowCompareExpr */
///     List       *paramIds;
///     int         plan_id;        /* Index (from 1) in PlannedStmt.subplans */
///     char       *plan_name;
///     Oid         firstColType;
///     int32       firstColTypmod;
///     Oid         firstColCollation;
///     bool        useHashTable;
///     bool        unknownEqFalse;
///     bool        parallel_safe;
///     List       *setParam;
///     List       *parParam;
///     List       *args;
///     Cost        startup_cost;
///     Cost        per_call_cost;
/// } SubPlan;
/// ```
///
/// The leading `xpr` (an `Expr`, whose first field is `NodeTag`) makes a
/// `*mut SubPlan` a valid `Node *`. The node layer reads its fields directly.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct SubPlan {
    /// `Expr xpr` — abstract expression base (its first field is `NodeTag`).
    pub xpr: Node,
    /// `SubLinkType subLinkType` — see [`SubLinkType`].
    pub subLinkType: SubLinkType,
    /// `Node *testexpr` — `OpExpr`/`RowCompareExpr` combining-expression tree.
    pub testexpr: *mut Node,
    /// `List *paramIds` — IDs of `Param`s embedded in `testexpr`.
    pub paramIds: *mut List,
    /// `int plan_id` — index (from 1) in `PlannedStmt.subplans`.
    pub plan_id: c_int,
    /// `char *plan_name` — name assigned during planning.
    pub plan_name: *mut c_char,
    /// `Oid firstColType` — type of the first column of the subplan result.
    pub firstColType: Oid,
    /// `int32 firstColTypmod` — typmod of the first result column.
    pub firstColTypmod: int32,
    /// `Oid firstColCollation` — collation of the first result column.
    pub firstColCollation: Oid,
    /// `bool useHashTable` — store subselect output in a hash table ("IN").
    pub useHashTable: bool,
    /// `bool unknownEqFalse` — okay to return FALSE for an UNKNOWN spec result.
    pub unknownEqFalse: bool,
    /// `bool parallel_safe` — is the subplan parallel-safe?
    pub parallel_safe: bool,
    /// `List *setParam` — params the initplan/MULTIEXPR subquery must set.
    pub setParam: *mut List,
    /// `List *parParam` — indices of input `Param`s from the parent plan.
    pub parParam: *mut List,
    /// `List *args` — exprs to pass as `parParam` values.
    pub args: *mut List,
    /// `Cost startup_cost` — one-time setup cost.
    pub startup_cost: f64,
    /// `Cost per_call_cost` — cost for each subplan evaluation.
    pub per_call_cost: f64,
}

/// `SubPlanState` (execnodes.h):
///
/// ```c
/// typedef struct SubPlanState {
///     NodeTag     type;
///     SubPlan    *subplan;
///     struct PlanState *planstate;
///     struct PlanState *parent;
///     ExprState  *testexpr;
///     HeapTuple   curTuple;
///     Datum       curArray;
///     TupleDesc   descRight;
///     ProjectionInfo *projLeft;
///     ProjectionInfo *projRight;
///     TupleHashTable hashtable;
///     TupleHashTable hashnulls;
///     bool        havehashrows;
///     bool        havenullrows;
///     MemoryContext hashtablecxt;
///     MemoryContext hashtempcxt;
///     ExprContext *innerecontext;
///     int         numCols;
///     AttrNumber *keyColIdx;
///     Oid        *tab_eq_funcoids;
///     Oid        *tab_collations;
///     FmgrInfo   *tab_hash_funcs;
///     ExprState  *lhs_hash_expr;
///     FmgrInfo   *cur_eq_funcs;
///     ExprState  *cur_eq_comp;
/// } SubPlanState;
/// ```
///
/// The first field is a `NodeTag`, so a `*mut SubPlanStateData` is a valid
/// `Node *`. The node code keeps its logic idiomatic but navigates this
/// address-stable layout for the fields it reads/writes.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct SubPlanStateData {
    /// `NodeTag type`.
    pub type_: NodeTag,
    /// `SubPlan *subplan` — the expression plan node.
    pub subplan: *mut SubPlan,
    /// `PlanState *planstate` — subselect plan's state tree.
    pub planstate: *mut PlanStateData,
    /// `PlanState *parent` — parent plan node's state tree.
    pub parent: *mut PlanStateData,
    /// `ExprState *testexpr` — state of the combining expression.
    pub testexpr: *mut ExprState,
    /// `HeapTuple curTuple` — copy of most recent tuple from the subplan.
    pub curTuple: HeapTuple,
    /// `Datum curArray` — most recent array from an `ARRAY()` subplan.
    pub curArray: Datum,
    /// `TupleDesc descRight` — subselect descriptor after projection.
    pub descRight: TupleDesc,
    /// `ProjectionInfo *projLeft` — for projecting lefthand exprs.
    pub projLeft: *mut c_void,
    /// `ProjectionInfo *projRight` — for projecting subselect output.
    pub projRight: *mut c_void,
    /// `TupleHashTable hashtable` — hash table for no-nulls subselect rows.
    pub hashtable: TupleHashTable,
    /// `TupleHashTable hashnulls` — hash table for rows with null(s).
    pub hashnulls: TupleHashTable,
    /// `bool havehashrows` — true if `hashtable` is not empty.
    pub havehashrows: bool,
    /// `bool havenullrows` — true if `hashnulls` is not empty.
    pub havenullrows: bool,
    /// `MemoryContext hashtablecxt` — memory context containing the hash tables.
    pub hashtablecxt: MemoryContext,
    /// `MemoryContext hashtempcxt` — temp memory context for the hash tables.
    pub hashtempcxt: MemoryContext,
    /// `ExprContext *innerecontext` — econtext for computing inner tuples.
    pub innerecontext: *mut ExprContext,
    /// `int numCols` — number of columns being hashed.
    pub numCols: c_int,
    /// `AttrNumber *keyColIdx` — control data for the hash tables.
    pub keyColIdx: *mut AttrNumber,
    /// `Oid *tab_eq_funcoids` — equality func oids for the table datatype(s).
    pub tab_eq_funcoids: *mut Oid,
    /// `Oid *tab_collations` — collations for hashing and comparison.
    pub tab_collations: *mut Oid,
    /// `FmgrInfo *tab_hash_funcs` — hash functions for the table datatype(s).
    pub tab_hash_funcs: *mut FmgrInfo,
    /// `ExprState *lhs_hash_expr` — hash expr for the lefthand datatype(s).
    pub lhs_hash_expr: *mut ExprState,
    /// `FmgrInfo *cur_eq_funcs` — equality functions for LHS vs. table.
    pub cur_eq_funcs: *mut FmgrInfo,
    /// `ExprState *cur_eq_comp` — equality comparator for LHS vs. table.
    pub cur_eq_comp: *mut ExprState,
}

// Layout asserts: the `SubPlan` `Expr`/`Node` head and the `SubPlanState`
// `NodeTag` head must keep their C offsets so the addresses can be navigated as
// the C `SubPlan *` / `SubPlanState *`.
const _: () = {
    assert!(core::mem::offset_of!(SubPlan, xpr) == 0);
    assert!(core::mem::offset_of!(SubPlan, subLinkType) == 4);
    assert!(core::mem::offset_of!(SubPlan, testexpr) == 8);
    assert!(core::mem::offset_of!(SubPlan, plan_id) == 24);
    assert!(core::mem::align_of::<SubPlan>() == 8);
    assert!(core::mem::offset_of!(SubPlanStateData, type_) == 0);
    assert!(core::mem::offset_of!(SubPlanStateData, subplan) == 8);
    assert!(core::mem::offset_of!(SubPlanStateData, planstate) == 16);
    assert!(core::mem::align_of::<SubPlanStateData>() == 8);
};

/// NodeTag for `BoolExpr` (primnodes.h / nodetags.h order). `nodeSubplan.c`'s
/// hashable-`SubPlan` init reads `testexpr` and, when it is an AND clause, pulls
/// its combining-operator list out of a `BoolExpr`.
pub const T_BoolExpr: NodeTag = 21;

/// `BoolExprType` (primnodes.h) — the boolean operator of a [`BoolExpr`].
///
/// ```c
/// typedef enum BoolExprType { AND_EXPR, OR_EXPR, NOT_EXPR } BoolExprType;
/// ```
pub type BoolExprType = c_int;
/// `AND_EXPR`.
pub const AND_EXPR: BoolExprType = 0;
/// `OR_EXPR`.
pub const OR_EXPR: BoolExprType = 1;
/// `NOT_EXPR`.
pub const NOT_EXPR: BoolExprType = 2;

/// `BoolExpr` (primnodes.h) — an `AND`/`OR`/`NOT` boolean expression node:
///
/// ```c
/// typedef struct BoolExpr {
///     Expr        xpr;        /* { NodeTag type; } */
///     BoolExprType boolop;
///     List       *args;       /* arguments to this expression */
///     ParseLoc    location;   /* token location, or -1 if unknown */
/// } BoolExpr;
/// ```
///
/// The leading `xpr` (an `Expr`, whose first field is `NodeTag`) makes a
/// `*mut BoolExpr` a valid `Node *`. `nodeSubplan.c` reads `boolop` (to confirm
/// an AND clause) and `args` (the list of combining `OpExpr`s).
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct BoolExpr {
    /// `Expr xpr` — abstract expression base (its first field is `NodeTag`).
    pub xpr: Node,
    /// `BoolExprType boolop` — the boolean operator (see [`BoolExprType`]).
    pub boolop: BoolExprType,
    /// `List *args` — arguments to this expression.
    pub args: *mut List,
    /// `ParseLoc location` — token location, or -1 if unknown.
    pub location: crate::ParseLoc,
}

const _: () = {
    assert!(core::mem::offset_of!(BoolExpr, xpr) == 0);
    assert!(core::mem::offset_of!(BoolExpr, boolop) == 4);
    assert!(core::mem::offset_of!(BoolExpr, args) == 8);
    assert!(core::mem::align_of::<BoolExpr>() == 8);
};
