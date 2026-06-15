//! Concrete `#[repr(C)]` plan-node structs (`nodes/plannodes.h`) used by the
//! optimizer's `createplan`/`setrefs` providers to read and write plan-node
//! fields.
//!
//! These mirror the PostgreSQL 18.3 layout exactly, in field order, embedding
//! the concrete [`Plan`]/[`Scan`] bases defined in [`crate::nodeindexscan`]
//! (the crate root re-exports `execnodes::Plan = c_void`, an opaque alias, so
//! we deliberately import the concrete bases by module path here).
//!
//! This module is intentionally NOT glob re-exported at the crate root, to
//! avoid colliding with the pre-existing opaque `Agg`/`Sort` aliases in
//! [`crate::nodeagg_abi`] and [`crate::nodesort_abi`]. Reach these structs by
//! module path (e.g. `pgrust_pg_ffi::plannodes_gen::Agg`).

use core::ffi::{c_char, c_void};

use crate::nodeindexscan::{Plan, Scan};
use crate::{AttrNumber, Bitmapset, List, Oid};

/// `SeqScan` (`nodes/plannodes.h`) — `typedef Scan SeqScan;`. Modelled as a
/// thin wrapper around [`Scan`] so providers field-access `scan.scanrelid`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct SeqScan {
    /// `Scan scan` — the abstract scan base (`scan.scanrelid` is the RT index).
    pub scan: Scan,
}

/// `TidRangeScan` (`nodes/plannodes.h`) — tid range scan node.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TidRangeScan {
    /// `Scan scan`.
    pub scan: Scan,
    /// `List *tidrangequals` — qual(s) involving CTID op something.
    pub tidrangequals: *mut List,
}

/// `ValuesScan` (`nodes/plannodes.h`) — VALUES scan node.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ValuesScan {
    /// `Scan scan`.
    pub scan: Scan,
    /// `List *values_lists` — list of expression lists.
    pub values_lists: *mut List,
}

/// `BitmapAnd` (`nodes/plannodes.h`) — intersection of sub-plan bitmaps.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct BitmapAnd {
    /// `Plan plan`.
    pub plan: Plan,
    /// `List *bitmapplans`.
    pub bitmapplans: *mut List,
}

/// `BitmapOr` (`nodes/plannodes.h`) — union of sub-plan bitmaps.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct BitmapOr {
    /// `Plan plan`.
    pub plan: Plan,
    /// `bool isshared`.
    pub isshared: bool,
    /// `List *bitmapplans`.
    pub bitmapplans: *mut List,
}

/// `Memoize` (`nodes/plannodes.h`) — memoize (result-caching) node.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct Memoize {
    /// `Plan plan`.
    pub plan: Plan,
    /// `int numKeys` — size of the two arrays below.
    pub numKeys: i32,
    /// `Oid *hashOperators` — hash operators for each key (array_size numKeys).
    pub hashOperators: *mut Oid,
    /// `Oid *collations` — collations for each key (array_size numKeys).
    pub collations: *mut Oid,
    /// `List *param_exprs` — cache keys as exprs containing parameters.
    pub param_exprs: *mut List,
    /// `bool singlerow` — mark cache entry complete after first tuple?
    pub singlerow: bool,
    /// `bool binary_mode` — compare keys bit-by-bit vs hash equality ops?
    pub binary_mode: bool,
    /// `uint32 est_entries` — estimated entries that fit in cache, or 0.
    pub est_entries: crate::uint32,
    /// `Bitmapset *keyparamids` — paramids from param_exprs.
    pub keyparamids: *mut Bitmapset,
}

/// `Agg` (`nodes/plannodes.h`) — plain or grouped aggregation node.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct Agg {
    /// `Plan plan`.
    pub plan: Plan,
    /// `AggStrategy aggstrategy` — basic strategy, see nodes.h.
    pub aggstrategy: crate::AggStrategy,
    /// `AggSplit aggsplit` — agg-splitting mode, see nodes.h.
    pub aggsplit: crate::AggSplit,
    /// `int numCols` — number of grouping columns.
    pub numCols: i32,
    /// `AttrNumber *grpColIdx` — their indexes in the target list.
    pub grpColIdx: *mut AttrNumber,
    /// `Oid *grpOperators` — equality operators to compare with.
    pub grpOperators: *mut Oid,
    /// `Oid *grpCollations`.
    pub grpCollations: *mut Oid,
    /// `long numGroups` — estimated number of groups in input.
    pub numGroups: i64,
    /// `uint64 transitionSpace` — for pass-by-ref transition data.
    pub transitionSpace: u64,
    /// `Bitmapset *aggParams` — IDs of Params used in Aggref inputs.
    pub aggParams: *mut Bitmapset,
    /// `List *groupingSets` — grouping sets to use.
    pub groupingSets: *mut List,
    /// `List *chain` — chained Agg/Sort nodes.
    pub chain: *mut List,
}

/// `Group` (`nodes/plannodes.h`) — GROUP BY without aggregates; presorted input.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct Group {
    /// `Plan plan`.
    pub plan: Plan,
    /// `int numCols` — number of grouping columns.
    pub numCols: i32,
    /// `AttrNumber *grpColIdx` — their indexes in the target list.
    pub grpColIdx: *mut AttrNumber,
    /// `Oid *grpOperators` — equality operators to compare with.
    pub grpOperators: *mut Oid,
    /// `Oid *grpCollations`.
    pub grpCollations: *mut Oid,
}

/// `Unique` (`nodes/plannodes.h`) — unique node.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct Unique {
    /// `Plan plan`.
    pub plan: Plan,
    /// `int numCols` — number of columns to check for uniqueness.
    pub numCols: i32,
    /// `AttrNumber *uniqColIdx` — their indexes in the target list.
    pub uniqColIdx: *mut AttrNumber,
    /// `Oid *uniqOperators` — equality operators to compare with.
    pub uniqOperators: *mut Oid,
    /// `Oid *uniqCollations` — collations for equality comparisons.
    pub uniqCollations: *mut Oid,
}

/// `Gather` (`nodes/plannodes.h`) — gather node (parallel query).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct Gather {
    /// `Plan plan`.
    pub plan: Plan,
    /// `int num_workers` — planned number of worker processes.
    pub num_workers: i32,
    /// `int rescan_param` — ID of Param that signals a rescan, or -1.
    pub rescan_param: i32,
    /// `bool single_copy` — don't execute plan more than once.
    pub single_copy: bool,
    /// `bool invisible` — suppress EXPLAIN display (for testing)?
    pub invisible: bool,
    /// `Bitmapset *initParam` — param ids of initplans referenced at/below.
    pub initParam: *mut Bitmapset,
}

/// `GatherMerge` (`nodes/plannodes.h`) — gather merge node (parallel query).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct GatherMerge {
    /// `Plan plan`.
    pub plan: Plan,
    /// `int num_workers` — planned number of worker processes.
    pub num_workers: i32,
    /// `int rescan_param` — ID of Param that signals a rescan, or -1.
    pub rescan_param: i32,
    /// `int numCols` — number of sort-key columns.
    pub numCols: i32,
    /// `AttrNumber *sortColIdx` — their indexes in the target list.
    pub sortColIdx: *mut AttrNumber,
    /// `Oid *sortOperators` — OIDs of operators to sort them by.
    pub sortOperators: *mut Oid,
    /// `Oid *collations` — OIDs of collations.
    pub collations: *mut Oid,
    /// `bool *nullsFirst` — NULLS FIRST/LAST directions.
    pub nullsFirst: *mut bool,
    /// `Bitmapset *initParam` — param ids of initplans referenced at/below.
    pub initParam: *mut Bitmapset,
}

/// `Result` (`nodes/plannodes.h`) — Result node (variable-free targetlist or
/// one-time qual over an outer plan's tuples).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct Result {
    /// `Plan plan`.
    pub plan: Plan,
    /// `Node *resconstantqual` — one-time qualification test, or NULL.
    pub resconstantqual: *mut c_void,
}

/// `Append` (`nodes/plannodes.h`) — concatenation of sub-plan results.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct Append {
    /// `Plan plan`.
    pub plan: Plan,
    /// `Bitmapset *apprelids` — RTIs of appendrel(s) formed by this node.
    pub apprelids: *mut Bitmapset,
    /// `List *appendplans`.
    pub appendplans: *mut List,
    /// `int nasyncplans` — # of asynchronous plans.
    pub nasyncplans: i32,
    /// `int first_partial_plan` — index of first partial plan in appendplans.
    pub first_partial_plan: i32,
    /// `int part_prune_index` — index into PlannedStmt.partPruneInfos, or -1.
    pub part_prune_index: i32,
}

/// `MergeAppend` (`nodes/plannodes.h`) — merge of pre-sorted sub-plans.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct MergeAppend {
    /// `Plan plan`.
    pub plan: Plan,
    /// `Bitmapset *apprelids` — RTIs of appendrel(s) formed by this node.
    pub apprelids: *mut Bitmapset,
    /// `List *mergeplans`.
    pub mergeplans: *mut List,
    /// `int numCols` — number of sort-key columns.
    pub numCols: i32,
    /// `AttrNumber *sortColIdx` — their indexes in the target list.
    pub sortColIdx: *mut AttrNumber,
    /// `Oid *sortOperators` — OIDs of operators to sort them by.
    pub sortOperators: *mut Oid,
    /// `Oid *collations` — OIDs of collations.
    pub collations: *mut Oid,
    /// `bool *nullsFirst` — NULLS FIRST/LAST directions.
    pub nullsFirst: *mut bool,
    /// `int part_prune_index` — index into PlannedStmt.partPruneInfos, or -1.
    pub part_prune_index: i32,
}

/* ----------------------------------------------------------------
 * parse/prim node structs (`nodes/parsenodes.h`, `nodes/primnodes.h`) that
 * the `createplan`/`setrefs` providers field-access. These are NOT plan
 * nodes, but they live here so they share the same module path and avoid
 * crate-root name collisions. Transcribed field-for-field in C-declaration
 * order from PostgreSQL 18.3.
 * ---------------------------------------------------------------- */

/// `CTEMaterialize` (`nodes/parsenodes.h`) — the materialization mode of a
/// `CommonTableExpr` (`CTEMaterializeDefault`/`Always`/`Never`). pg-ffi has no
/// existing alias for this enum, so it is modelled as the underlying C enum
/// width (`i32`).
///
/// ```c
/// typedef enum CTEMaterialize
/// {
///     CTEMaterializeDefault,  /* no option specified */
///     CTEMaterializeAlways,   /* MATERIALIZED */
///     CTEMaterializeNever,    /* NOT MATERIALIZED */
/// } CTEMaterialize;
/// ```
pub type CTEMaterialize = i32;

/// `SortGroupClause` (`nodes/parsenodes.h`) — represents one entry of an
/// ORDER BY / GROUP BY / DISTINCT / DISTINCT ON clause.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct SortGroupClause {
    /// `NodeTag type`.
    pub type_: crate::NodeTag,
    /// `Index tleSortGroupRef` — reference into targetlist.
    pub tleSortGroupRef: crate::Index,
    /// `Oid eqop` — the equality operator ('=' op).
    pub eqop: Oid,
    /// `Oid sortop` — the ordering operator ('<' op), or 0.
    pub sortop: Oid,
    /// `bool reverse_sort` — is sortop a "greater than" operator?
    pub reverse_sort: bool,
    /// `bool nulls_first` — do NULLs come before normal values?
    pub nulls_first: bool,
    /// `bool hashable` — can eqop be implemented by hashing?
    pub hashable: bool,
}

/// `RowCompareExpr` (`nodes/primnodes.h`) — a row-wise comparison such as
/// `(a, b) <= (1, 2)`. In PostgreSQL 18.3 the discriminator field is
/// `CompareType cmptype` (it was renamed from `RowCompareType rctype` in
/// earlier releases); modelled here with the existing [`crate::CompareType`]
/// alias (a 4-byte C enum).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct RowCompareExpr {
    /// `Expr xpr` — the abstract expression base; its only member is `NodeTag`.
    pub xpr: crate::NodeTag,
    /// `CompareType cmptype` — LT LE GE or GT, never EQ or NE.
    pub cmptype: crate::CompareType,
    /// `List *opnos` — OID list of pairwise comparison ops.
    pub opnos: *mut List,
    /// `List *opfamilies` — OID list of containing operator families.
    pub opfamilies: *mut List,
    /// `List *inputcollids` — OID list of collations for comparisons.
    pub inputcollids: *mut List,
    /// `List *largs` — the left-hand input arguments.
    pub largs: *mut List,
    /// `List *rargs` — the right-hand input arguments.
    pub rargs: *mut List,
}

/// `OnConflictExpr` (`nodes/primnodes.h`) — represents an `ON CONFLICT DO ...`
/// expression. Reuses the existing [`crate::nodemodifytable_abi::OnConflictAction`]
/// enum (a 4-byte C enum) for `action`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct OnConflictExpr {
    /// `NodeTag type`.
    pub type_: crate::NodeTag,
    /// `OnConflictAction action` — DO NOTHING or UPDATE?
    pub action: crate::nodemodifytable_abi::OnConflictAction,
    /// `List *arbiterElems` — unique index arbiter list (of InferenceElem's).
    pub arbiterElems: *mut List,
    /// `Node *arbiterWhere` — unique index arbiter WHERE clause.
    pub arbiterWhere: *mut c_void,
    /// `Oid constraint` — pg_constraint OID for arbiter.
    pub constraint: Oid,
    /// `List *onConflictSet` — List of ON CONFLICT SET TargetEntrys.
    pub onConflictSet: *mut List,
    /// `Node *onConflictWhere` — qualifiers to restrict UPDATE to.
    pub onConflictWhere: *mut c_void,
    /// `int exclRelIndex` — RT index of 'excluded' relation.
    pub exclRelIndex: i32,
    /// `List *exclRelTlist` — tlist of the EXCLUDED pseudo relation.
    pub exclRelTlist: *mut List,
}

/// `CommonTableExpr` (`nodes/parsenodes.h`) — a WITH-clause CTE. The
/// `search_clause`/`cycle_clause` members point to `CTESearchClause`/
/// `CTECycleClause` (not yet modelled in pg-ffi); they are kept opaque
/// (`*mut c_void`). `ctequery` is a `Node *`, also opaque here.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CommonTableExpr {
    /// `NodeTag type`.
    pub type_: crate::NodeTag,
    /// `char *ctename` — query name (never qualified).
    pub ctename: *mut c_char,
    /// `List *aliascolnames` — optional list of column names.
    pub aliascolnames: *mut List,
    /// `CTEMaterialize ctematerialized` — is this an optimization fence?
    pub ctematerialized: CTEMaterialize,
    /// `Node *ctequery` — the CTE's subquery.
    pub ctequery: *mut c_void,
    /// `CTESearchClause *search_clause` — SEARCH clause, opaque here.
    pub search_clause: *mut c_void,
    /// `CTECycleClause *cycle_clause` — CYCLE clause, opaque here.
    pub cycle_clause: *mut c_void,
    /// `ParseLoc location` — token location, or -1 if unknown.
    pub location: crate::ParseLoc,
    /// `bool cterecursive` — is this CTE actually recursive?
    pub cterecursive: bool,
    /// `int cterefcount` — number of RTEs referencing this CTE.
    pub cterefcount: i32,
    /// `List *ctecolnames` — list of output column names.
    pub ctecolnames: *mut List,
    /// `List *ctecoltypes` — OID list of output column type OIDs.
    pub ctecoltypes: *mut List,
    /// `List *ctecoltypmods` — integer list of output column typmods.
    pub ctecoltypmods: *mut List,
    /// `List *ctecolcollations` — OID list of column collation OIDs.
    pub ctecolcollations: *mut List,
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::mem::{offset_of, size_of};

    #[test]
    fn seqscan_is_scan_sized() {
        // typedef Scan SeqScan; — same size/leading layout as Scan.
        assert_eq!(size_of::<SeqScan>(), size_of::<Scan>());
        assert_eq!(offset_of!(SeqScan, scan), 0);
    }

    #[test]
    fn scan_derived_offsets() {
        // Every Scan-derived node has its type-specific field right after Scan.
        assert_eq!(offset_of!(TidRangeScan, tidrangequals), size_of::<Scan>());
        assert_eq!(offset_of!(ValuesScan, values_lists), size_of::<Scan>());
    }

    #[test]
    fn plan_derived_offsets() {
        // Every Plan-derived node has its first type-specific field after Plan.
        assert_eq!(offset_of!(BitmapAnd, bitmapplans), size_of::<Plan>());
        assert_eq!(offset_of!(BitmapOr, isshared), size_of::<Plan>());
        assert_eq!(offset_of!(Memoize, numKeys), size_of::<Plan>());
        assert_eq!(offset_of!(Agg, aggstrategy), size_of::<Plan>());
        assert_eq!(offset_of!(Group, numCols), size_of::<Plan>());
        assert_eq!(offset_of!(Unique, numCols), size_of::<Plan>());
        assert_eq!(offset_of!(Gather, num_workers), size_of::<Plan>());
        assert_eq!(offset_of!(GatherMerge, num_workers), size_of::<Plan>());
        assert_eq!(offset_of!(Result, resconstantqual), size_of::<Plan>());
        assert_eq!(offset_of!(Append, apprelids), size_of::<Plan>());
        assert_eq!(offset_of!(MergeAppend, apprelids), size_of::<Plan>());
    }

    #[test]
    fn agg_field_order() {
        // Spot-check the enum + array + scalar ordering of Agg.
        assert!(offset_of!(Agg, aggsplit) > offset_of!(Agg, aggstrategy));
        assert!(offset_of!(Agg, numCols) > offset_of!(Agg, aggsplit));
        assert!(offset_of!(Agg, grpColIdx) > offset_of!(Agg, numCols));
        assert!(offset_of!(Agg, numGroups) > offset_of!(Agg, grpCollations));
        assert!(offset_of!(Agg, chain) > offset_of!(Agg, groupingSets));
    }

    #[test]
    fn sort_group_clause_layout() {
        // NodeTag(4) + Index(4) + 2*Oid(8) + 3*bool(3) -> padded to 20 bytes.
        assert_eq!(offset_of!(SortGroupClause, type_), 0);
        assert_eq!(offset_of!(SortGroupClause, tleSortGroupRef), 4);
        assert_eq!(offset_of!(SortGroupClause, eqop), 8);
        assert_eq!(offset_of!(SortGroupClause, sortop), 12);
        assert_eq!(offset_of!(SortGroupClause, reverse_sort), 16);
        assert_eq!(offset_of!(SortGroupClause, nulls_first), 17);
        assert_eq!(offset_of!(SortGroupClause, hashable), 18);
        assert_eq!(size_of::<SortGroupClause>(), 20);
    }

    #[test]
    fn row_compare_expr_layout() {
        // Expr xpr (NodeTag, 4) + CompareType (4) + 5 List ptrs (8 each).
        assert_eq!(offset_of!(RowCompareExpr, xpr), 0);
        assert_eq!(offset_of!(RowCompareExpr, cmptype), 4);
        assert_eq!(offset_of!(RowCompareExpr, opnos), 8);
        assert_eq!(offset_of!(RowCompareExpr, opfamilies), 16);
        assert_eq!(offset_of!(RowCompareExpr, inputcollids), 24);
        assert_eq!(offset_of!(RowCompareExpr, largs), 32);
        assert_eq!(offset_of!(RowCompareExpr, rargs), 40);
        assert_eq!(size_of::<RowCompareExpr>(), 48);
    }

    #[test]
    fn on_conflict_expr_layout() {
        // NodeTag(4) + OnConflictAction(4) + List(8) + Node(8) + Oid(4)+pad(4)
        // + List(8) + Node(8) + int(4)+pad(4) + List(8).
        assert_eq!(offset_of!(OnConflictExpr, type_), 0);
        assert_eq!(offset_of!(OnConflictExpr, action), 4);
        assert_eq!(offset_of!(OnConflictExpr, arbiterElems), 8);
        assert_eq!(offset_of!(OnConflictExpr, arbiterWhere), 16);
        assert_eq!(offset_of!(OnConflictExpr, constraint), 24);
        assert_eq!(offset_of!(OnConflictExpr, onConflictSet), 32);
        assert_eq!(offset_of!(OnConflictExpr, onConflictWhere), 40);
        assert_eq!(offset_of!(OnConflictExpr, exclRelIndex), 48);
        assert_eq!(offset_of!(OnConflictExpr, exclRelTlist), 56);
        assert_eq!(size_of::<OnConflictExpr>(), 64);
    }

    #[test]
    fn common_table_expr_layout() {
        assert_eq!(offset_of!(CommonTableExpr, type_), 0);
        assert_eq!(offset_of!(CommonTableExpr, ctename), 8);
        assert_eq!(offset_of!(CommonTableExpr, aliascolnames), 16);
        assert_eq!(offset_of!(CommonTableExpr, ctematerialized), 24);
        assert_eq!(offset_of!(CommonTableExpr, ctequery), 32);
        assert_eq!(offset_of!(CommonTableExpr, search_clause), 40);
        assert_eq!(offset_of!(CommonTableExpr, cycle_clause), 48);
        assert_eq!(offset_of!(CommonTableExpr, location), 56);
        assert_eq!(offset_of!(CommonTableExpr, cterecursive), 60);
        assert_eq!(offset_of!(CommonTableExpr, cterefcount), 64);
        assert_eq!(offset_of!(CommonTableExpr, ctecolnames), 72);
        assert_eq!(offset_of!(CommonTableExpr, ctecoltypes), 80);
        assert_eq!(offset_of!(CommonTableExpr, ctecoltypmods), 88);
        assert_eq!(offset_of!(CommonTableExpr, ctecolcollations), 96);
        assert_eq!(size_of::<CommonTableExpr>(), 104);
    }
}
