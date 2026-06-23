//! ABI vocabulary for the `optimizer/prep/*` port crates
//! (`preptlist.c`, `prepqual.c`, `prepagg.c`, `prepunion.c`, `prepjointree.c`).
//!
//! These are the `#[repr(C)]` parse-/plan-node structs, enums, and type aliases
//! the prep crates manipulate that were not already present in the other
//! `pgrust-pg-ffi` modules.  Layout mirrors PostgreSQL 18.3 verbatim
//! (`nodes/parsenodes.h`, `nodes/primnodes.h`, `nodes/pathnodes.h`).
//!
//! `PlannerInfo`, `RelOptInfo`, `Relids`, `AggSplit`, `AppendRelInfo`,
//! `SpecialJoinInfo`, `QualCost`, `JoinType`, and the `*Ptr` opaque aliases live
//! in [`crate::pathnodes`]; `Node`, `List`, `Bitmapset`, `Oid`, `NodeTag`,
//! `Size`, `Cost`, `AttrNumber` come from their respective base modules and are
//! reused here.

use core::ffi::c_void;

use crate::list::List;
use crate::pathnodes::{JoinType, QualCost};
use crate::types::{NodeTag, Size};

// ---------------------------------------------------------------------------
// nodes/parsenodes.h — set operations
// ---------------------------------------------------------------------------

/// `SetOperation` (`nodes/parsenodes.h`) — the kind of a set operation.
///
/// ```c
/// typedef enum SetOperation
/// {
///     SETOP_NONE = 0,
///     SETOP_UNION,
///     SETOP_INTERSECT,
///     SETOP_EXCEPT,
/// } SetOperation;
/// ```
pub type SetOperation = u32;

pub const SETOP_NONE: SetOperation = 0;
pub const SETOP_UNION: SetOperation = 1;
pub const SETOP_INTERSECT: SetOperation = 2;
pub const SETOP_EXCEPT: SetOperation = 3;

/// `SetOperationStmt` (`nodes/parsenodes.h`) — a node in the set-operation tree
/// of a `Query` (`UNION`/`INTERSECT`/`EXCEPT`).
///
/// ```c
/// typedef struct SetOperationStmt
/// {
///     NodeTag     type;
///     SetOperation op;            /* type of set op */
///     bool        all;            /* ALL specified? */
///     Node       *larg;           /* left child */
///     Node       *rarg;           /* right child */
///     List       *colTypes;       /* OID list of output column type OIDs */
///     List       *colTypmods;     /* integer list of output column typmods */
///     List       *colCollations;  /* OID list of output column collation OIDs */
///     List       *groupClauses;   /* a list of SortGroupClause's */
/// } SetOperationStmt;
/// ```
#[repr(C)]
#[derive(Clone, Copy)]
pub struct SetOperationStmt {
    pub type_: NodeTag,
    pub op: SetOperation,
    pub all: bool,
    pub larg: *mut crate::fmgr::Node,
    pub rarg: *mut crate::fmgr::Node,
    pub colTypes: *mut List,
    pub colTypmods: *mut List,
    pub colCollations: *mut List,
    pub groupClauses: *mut List,
}

// ---------------------------------------------------------------------------
// nodes/primnodes.h — jointree primitives
// ---------------------------------------------------------------------------

/// `RangeTblRef` (`nodes/primnodes.h`) — a reference to a range-table entry by
/// index, used as a leaf of the jointree.
///
/// ```c
/// typedef struct RangeTblRef
/// {
///     NodeTag     type;
///     int         rtindex;
/// } RangeTblRef;
/// ```
#[repr(C)]
#[derive(Clone, Copy)]
pub struct RangeTblRef {
    pub type_: NodeTag,
    pub rtindex: i32,
}

/// `FromExpr` (`nodes/primnodes.h`) — the top of a jointree: a list of join
/// subtrees plus an optional WHERE/ON qualification.
///
/// ```c
/// typedef struct FromExpr
/// {
///     NodeTag     type;
///     List       *fromlist;       /* List of join subtrees */
///     Node       *quals;          /* qualifiers on join, if any */
/// } FromExpr;
/// ```
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FromExpr {
    pub type_: NodeTag,
    pub fromlist: *mut List,
    pub quals: *mut crate::fmgr::Node,
}

/// `JoinExpr` (`nodes/primnodes.h`) — an explicit `JOIN` node in the jointree.
///
/// The `usingClause`/`join_using_alias`/`alias` fields are `Alias *`/`List *`
/// pointers; modelled here as opaque `*mut c_void` (pointer-sized, ABI-identical)
/// since the prep crates only thread them through.
///
/// ```c
/// typedef struct JoinExpr
/// {
///     NodeTag     type;
///     JoinType    jointype;       /* type of join */
///     bool        isNatural;      /* Natural join? */
///     Node       *larg;           /* left subtree */
///     Node       *rarg;           /* right subtree */
///     List       *usingClause;    /* USING clause, if any (list of String) */
///     Alias      *join_using_alias;
///     Node       *quals;          /* qualifiers on join, if any */
///     Alias      *alias;          /* user-written alias clause, if any */
///     int         rtindex;        /* RT index assigned for join, or 0 */
/// } JoinExpr;
/// ```
#[repr(C)]
#[derive(Clone, Copy)]
pub struct JoinExpr {
    pub type_: NodeTag,
    pub jointype: JoinType,
    pub isNatural: bool,
    pub larg: *mut crate::fmgr::Node,
    pub rarg: *mut crate::fmgr::Node,
    pub usingClause: *mut List,
    pub join_using_alias: *mut c_void,
    pub quals: *mut crate::fmgr::Node,
    pub alias: *mut c_void,
    pub rtindex: i32,
}

// ---------------------------------------------------------------------------
// nodes/parsenodes.h — Query (opaque-leading skeleton view)
// ---------------------------------------------------------------------------

/// `Query` (`nodes/parsenodes.h`) — a parsed/analyzed query tree.
///
/// `Query` is a large struct; for the prep skeleton it is referenced only by
/// pointer (`Query *`).  The leading `NodeTag` is the only field the prep crates
/// touch structurally at the ABI boundary, so the body is intentionally
/// represented by its first member here.  The opaque [`crate::pathnodes::QueryPtr`]
/// (`*mut c_void`) remains the preferred carrier; this struct exists so callers
/// that want the named type can refer to `Query` directly.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct Query {
    /// `NodeTag type` — always `T_Query`.
    pub type_: NodeTag,
}

/// `Query *` carried by name.
pub type QueryRef = *mut Query;

// ---------------------------------------------------------------------------
// nodes/plannodes.h / nodes/lockoptions.h — row marks
// ---------------------------------------------------------------------------

/// `PlanRowMark *` — opaque pointer alias for the `PlanRowMark` plan-tree
/// rowmark node (defined as a full `#[repr(C)]` struct in
/// [`crate::nodelockrows`]).  `get_plan_rowmark` returns one of these.
pub type PlanRowMarkPtr = *mut c_void;

// ---------------------------------------------------------------------------
// nodes/pathnodes.h — aggregate clause costs
// ---------------------------------------------------------------------------

/// `AggClauseCosts` (`nodes/pathnodes.h`) — accumulated execution-cost estimates
/// for the `Aggref`/`GroupingFunc` nodes appearing in a query level, produced by
/// `get_agg_clause_costs` (prepagg.c).
///
/// ```c
/// typedef struct AggClauseCosts
/// {
///     QualCost    transCost;          /* total per-input-row execution costs */
///     QualCost    finalCost;          /* total per-aggregated-row costs */
///     Size        transitionSpace;    /* space for pass-by-ref transition data */
/// } AggClauseCosts;
/// ```
#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct AggClauseCosts {
    pub transCost: QualCost,
    pub finalCost: QualCost,
    pub transitionSpace: Size,
}
