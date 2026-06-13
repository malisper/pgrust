//! Expression-evaluation vocabulary (executor/execExpr.h), trimmed.

use mcx::{MemoryContext, PgBox, PgVec};
use types_core::fmgr::FmgrInfo;
use types_core::primitive::{AttrNumber, Oid};
use types_datum::datum::Datum;
use types_tuple::heaptuple::HeapTuple;
use types_tuple::heaptuple::TupleDescData;

use crate::execnodes::{EcxtId, Opaque};
use crate::nodes::NodeTag;
use crate::planstate::PlanStateNode;
use crate::primnodes::SubPlan;

/// `EEO_FLAG_IS_QUAL` (execnodes.h) — this expression is a qualification.
pub const EEO_FLAG_IS_QUAL: u8 = 1 << 0;
/// `EEO_FLAG_HAS_OLD` (execnodes.h) — the expression references OLD columns.
pub const EEO_FLAG_HAS_OLD: u8 = 1 << 1;
/// `EEO_FLAG_HAS_NEW` (execnodes.h) — the expression references NEW columns.
pub const EEO_FLAG_HAS_NEW: u8 = 1 << 2;
/// `EEO_FLAG_OLD_IS_NULL` (execnodes.h) — the OLD row is not present (NULL).
pub const EEO_FLAG_OLD_IS_NULL: u8 = 1 << 3;
/// `EEO_FLAG_NEW_IS_NULL` (execnodes.h) — the NEW row is not present (NULL).
pub const EEO_FLAG_NEW_IS_NULL: u8 = 1 << 4;

/// `ExprState` (execnodes.h) — the compiled, executable form of an expression
/// tree (`ExecInitExpr` / `ExecInitQual` output). Trimmed: ports so far only
/// store/hand an `ExprState *` to the expression interpreter and read/set the
/// `flags` bitmask (the `EEO_FLAG_*` bits that ExecProcessReturning toggles for
/// OLD/NEW visibility). The `expr` back-link and the step/resvalue/resnull
/// machinery arrive with the execExpr interpreter when it lands.
#[derive(Clone, Debug, Default)]
pub struct ExprState {
    /// `uint8 flags` — bitmask of `EEO_FLAG_*` bits.
    pub flags: u8,
}

/// `ProjectionInfo` (execnodes.h) — node for caching needed info for
/// projection. Trimmed to the fields readers consume: the `pi_exprContext`
/// (the [`EcxtId`] of the projection's expression context) and the compiled
/// `pi_state` (an [`ExprState`] whose `flags` carry the `EEO_FLAG_*` OLD/NEW
/// bits). The remaining expression machinery (`pi_state` steps) stays with the
/// execExpr owner when it lands.
#[derive(Clone, Debug, Default)]
pub struct ProjectionInfo {
    /// `ExprContext *pi_exprContext` — context holding the evaluation slots
    /// (`ecxt_scantuple` / `ecxt_outertuple` / `ecxt_oldtuple` /
    /// `ecxt_newtuple`). `None` until the projection is built by execExpr.
    pub pi_exprContext: Option<EcxtId>,
    /// `ExprState pi_state` — the compiled projection state; its `flags`
    /// carries the `EEO_FLAG_*` bits ExecProcessReturning manipulates.
    pub pi_state: ExprState,
}

/// `SubPlanState` (execnodes.h) — executor state for a subplan.
///
/// The `planstate` field is consumed by the `ExecReScan` walk; the remaining
/// fields are consumed by `nodeSubplan.c` (the owning unit). The compiled
/// expression states (`testexpr`, `lhs_hash_expr`, `cur_eq_comp`), the two
/// projection nodes (`projLeft`/`projRight`), and the two `TupleHashTable`s
/// (`hashtable`/`hashnulls`) belong to the still-unported execExpr /
/// execGrouping units; here they are heterogeneous owned slots ([`Opaque`])
/// that nodeSubplan only builds and probes through those units' seams. The C
/// `parent` back-pointer is not carried: callers thread the parent state
/// explicitly.
#[derive(Debug, Default)]
pub struct SubPlanState<'mcx> {
    /// `SubPlan *subplan` — the expression plan node.
    pub subplan: Option<PgBox<'mcx, SubPlan<'mcx>>>,
    /// `PlanState *planstate` — the subselect plan's state tree.
    pub planstate: Option<PgBox<'mcx, PlanStateNode<'mcx>>>,
    /// `ExprState *testexpr` — state of combining expression (execExpr-owned).
    pub testexpr: Opaque,
    /// `HeapTuple curTuple` — copy of most recent tuple from subplan.
    pub curTuple: HeapTuple<'mcx>,
    /// `Datum curArray` — most recent array from `ARRAY()` subplan.
    pub curArray: Datum,
    /// `TupleDesc descRight` — subselect desc after projection.
    pub descRight: Option<PgBox<'mcx, TupleDescData<'mcx>>>,
    /// `ProjectionInfo *projLeft` — for projecting lefthand exprs
    /// (execExpr-owned).
    pub projLeft: Opaque,
    /// `ProjectionInfo *projRight` — for projecting subselect output
    /// (execExpr-owned).
    pub projRight: Opaque,
    /// `TupleHashTable hashtable` — hash table for no-nulls subselect rows.
    /// The real owned execGrouping table (`TupleHashTable` in C is
    /// `TupleHashTableData *`; carried by box here).
    pub hashtable: Option<alloc::boxed::Box<crate::nodeagg::TupleHashTable<'mcx>>>,
    /// `TupleHashTable hashnulls` — hash table for rows with null(s).
    pub hashnulls: Option<alloc::boxed::Box<crate::nodeagg::TupleHashTable<'mcx>>>,
    /// `bool havehashrows` — true if `hashtable` is not empty.
    pub havehashrows: bool,
    /// `bool havenullrows` — true if `hashnulls` is not empty.
    pub havenullrows: bool,
    /// `MemoryContext hashtablecxt` — memory context containing hash tables.
    pub hashtablecxt: Option<MemoryContext>,
    /// `MemoryContext hashtempcxt` — temp memory context for hash tables.
    pub hashtempcxt: Option<MemoryContext>,
    /// `TupleHashIterator` cursor used by `findPartialMatch`'s full-table scan
    /// (the C `findPartialMatch` keeps a stack-local `hashiter`; the owned
    /// model carries it on the node so the canonical iterator seams can
    /// advance over the real table). One scan is active at a time.
    pub hashiter: crate::nodeagg::TupleHashIterator,
    /// `ExprContext *innerecontext` — econtext for computing inner tuples (id
    /// into the EState's `es_exprcontexts`).
    pub innerecontext: Option<EcxtId>,
    /// `int numCols` — number of columns being hashed.
    pub numCols: i32,
    /// `AttrNumber *keyColIdx` — control data for hash tables (length
    /// `numCols`).
    pub keyColIdx: Option<PgVec<'mcx, AttrNumber>>,
    /// `Oid *tab_eq_funcoids` — equality func oids for table datatype(s).
    pub tab_eq_funcoids: Option<PgVec<'mcx, Oid>>,
    /// `Oid *tab_collations` — collations for hash and comparison.
    pub tab_collations: Option<PgVec<'mcx, Oid>>,
    /// `FmgrInfo *tab_hash_funcs` — hash functions for table datatype(s).
    pub tab_hash_funcs: Option<PgVec<'mcx, FmgrInfo>>,
    /// `ExprState *lhs_hash_expr` — hash expr for lefthand datatype(s)
    /// (execExpr-owned).
    pub lhs_hash_expr: Opaque,
    /// `FmgrInfo *cur_eq_funcs` — equality functions for LHS vs. table.
    pub cur_eq_funcs: Option<PgVec<'mcx, FmgrInfo>>,
    /// `ExprState *cur_eq_comp` — equality comparator for LHS vs. table
    /// (execExpr-owned).
    pub cur_eq_comp: Opaque,
}

/// `T_SubPlanState` (nodes/nodetags.h) — PostgreSQL 18.3 generated value.
pub const T_SubPlanState: NodeTag = NodeTag(392);

/// `T_SubPlan` (nodes/nodetags.h) — PostgreSQL 18.3 generated value.
pub const T_SubPlan: NodeTag = NodeTag(23);
