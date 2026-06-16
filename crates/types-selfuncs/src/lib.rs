//! Selectivity-estimation vocabulary (`utils/selfuncs.h`): the
//! default-selectivity constants, the planner's `VariableStatData` (filled by
//! `examine_variable` / `get_restriction_variable`, released by
//! `ReleaseVariableStats`), and the `AttStatsSlot` the `get_attstatsslot`
//! lookups return.
//!
//! These cross the per-neighbor selectivity / lsyscache seams; the selectivity
//! crates orchestrate over them. `VariableStatData` is modeled
//! field-for-field against the C struct (`utils/selfuncs.h`): the examined
//! expression `var` is the planner node handle [`NodeId`] (`Node *`), `rel` is
//! the [`RelId`] index into `PlannerInfo.simple_rel_array` (`RelOptInfo *`, or
//! `None` when not identifiable), and `statsTuple` is the syscache-pinned
//! `pg_statistic` tuple [`StatsTuple`] (a C `HeapTuple` pointer the syscache
//! owns; the caller must run [`VariableStatData::freefunc`] when done — the
//! faithful model of the C `void (*freefunc)(HeapTuple)` member). The
//! `AttStatsSlot` value/number arrays are detoasted copies allocated in the
//! caller's `mcx`.

#![no_std]
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]

use mcx::PgVec;
use types_core::primitive::{InvalidOid, Oid};
use types_datum::datum::Datum;
use types_pathnodes::{NodeId, RelId};

/// `DEFAULT_INEQ_SEL` (selfuncs.h) — `0.3333333333333333`.
pub const DEFAULT_INEQ_SEL: f64 = 0.3333333333333333;
/// `DEFAULT_RANGE_INEQ_SEL` (selfuncs.h) — `0.005`.
pub const DEFAULT_RANGE_INEQ_SEL: f64 = 0.005;
/// `DEFAULT_MULTIRANGE_INEQ_SEL` (selfuncs.h) — `0.005`.
pub const DEFAULT_MULTIRANGE_INEQ_SEL: f64 = 0.005;

/// `SELFLAG_USED_DEFAULT` (selfuncs.h) — set in [`EstimationInfo::flags`] when a
/// selectivity estimation fell back on one of the `DEFAULT_*` constants.
pub const SELFLAG_USED_DEFAULT: u32 = 1 << 0;

/// `EstimationInfo` (selfuncs.h) — a set of flags some selectivity-estimation
/// functions pass back to callers to describe assumptions made during the
/// estimation (e.g. [`SELFLAG_USED_DEFAULT`]). Mirrors the C struct
/// field-for-field.
#[derive(Copy, Clone, Debug, Default)]
pub struct EstimationInfo {
    /// `uint32 flags` — flags marking special properties of the estimation.
    pub flags: u32,
}

/// `ATTSTATSSLOT_VALUES` (lsyscache.h) — request the slot's `stavalues` array.
pub const ATTSTATSSLOT_VALUES: i32 = 0x01;
/// `ATTSTATSSLOT_NUMBERS` (lsyscache.h) — request the slot's `stanumbers` array.
pub const ATTSTATSSLOT_NUMBERS: i32 = 0x02;

/// `STATISTIC_KIND_RANGE_LENGTH_HISTOGRAM` (pg_statistic.h) — the range-length
/// histogram slot kind; its single `stanumbers` entry is the empty fraction.
pub const STATISTIC_KIND_RANGE_LENGTH_HISTOGRAM: i32 = 6;
/// `STATISTIC_KIND_BOUNDS_HISTOGRAM` (pg_statistic.h) — the bounds-histogram
/// slot kind.
pub const STATISTIC_KIND_BOUNDS_HISTOGRAM: i32 = 7;

/// `STATISTIC_KIND_MCELEM` (pg_statistic.h) — the most-common-elements slot
/// kind, used by the array selectivity estimators.
pub const STATISTIC_KIND_MCELEM: i32 = 4;
/// `STATISTIC_KIND_DECHIST` (pg_statistic.h) — the distinct-element-count
/// histogram slot kind, used by the `array <@ const` estimator.
pub const STATISTIC_KIND_DECHIST: i32 = 5;

/// Decoded `Const` node fields the array selectivity estimators read after an
/// `IsA(node, Const)` test (`scalararraysel_containment`'s `leftop`): the
/// `(constisnull, constvalue, consttype)` triple. The `const_node_info` seam
/// returns `None` when the node is not a `Const` (C: the `!IsA` punt).
#[derive(Copy, Clone, Debug)]
pub struct ConstNodeInfo {
    /// `((Const *) node)->constisnull`.
    pub constisnull: bool,
    /// `((Const *) node)->constvalue` (the raw Datum word).
    pub constvalue: Datum,
    /// `((Const *) node)->consttype`.
    pub consttype: Oid,
}

/// A `pg_statistic` `HeapTuple` (`VariableStatData.statsTuple`,
/// `get_attstatsslot`'s `statstuple`). This mirrors the C `HeapTuple` type
/// exactly: a pointer to a tuple the syscache (or `statext_expressions_load`)
/// owns. It is C-faithful syscache-pinned memory — not an invented handle —
/// and must be released by the matching [`VariableStatData::freefunc`]
/// (`ReleaseSysCache` for a pinned syscache tuple, `pfree` for a copied one).
#[derive(Copy, Clone, Debug)]
pub struct StatsTuple {
    /// The `HeapTuple` address (syscache-owned, as in C).
    pub ptr: *mut core::ffi::c_void,
}

/// How a [`VariableStatData::stats_tuple`] is freed by `ReleaseVariableStats` —
/// the faithful model of the C `void (*freefunc)(HeapTuple tuple)` member of
/// `VariableStatData`. selfuncs.c only ever assigns one of two functions to it
/// (`ReleaseSysCache` for a pinned syscache tuple, `ReleaseDummy` = `pfree` for
/// a `statext_expressions_load` copy), so this closed enum replaces the C
/// function pointer without any opacity.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum StatsTupleFreeFunc {
    /// C `freefunc == ReleaseSysCache`: drop the syscache pin.
    ReleaseSysCache,
    /// C `freefunc == ReleaseDummy`: `pfree(tuple)` (a copied tuple).
    ReleaseDummy,
}

/// `VariableStatData` (selfuncs.h), modeled field-for-field against the C
/// struct. Filled by `examine_variable` / `get_restriction_variable`; released
/// by `ReleaseVariableStats` (which runs `freefunc(statsTuple)` — modeled by
/// the `release_variable_stats` seam, called from the consumer's RAII guard).
#[derive(Copy, Clone, Debug)]
pub struct VariableStatData {
    /// `Node *var` — the Var or expression tree the stats describe, as the
    /// planner node handle into `PlannerInfo`'s node arena.
    pub var: NodeId,
    /// `RelOptInfo *rel` — the relation the variable belongs to as the index
    /// into `PlannerInfo.simple_rel_array`, or `None` (`NULL`) when not
    /// identifiable. `scalararraysel_containment` punts when this is `None`.
    pub rel: Option<RelId>,
    /// `HeapTuple statsTuple` — the `pg_statistic` row, or `None`
    /// (`!HeapTupleIsValid`). Freed per [`Self::freefunc`].
    pub stats_tuple: Option<StatsTuple>,
    /// `void (*freefunc)(HeapTuple)` — how to free [`Self::stats_tuple`], or
    /// `None` when there is no tuple to free (C `freefunc == NULL`).
    pub freefunc: Option<StatsTupleFreeFunc>,
    /// `Oid vartype` — exposed type of the expression.
    pub vartype: Oid,
    /// `Oid atttype` — actual type (after stripping relabel).
    pub atttype: Oid,
    /// `int32 atttypmod` — actual typmod (after stripping relabel).
    pub atttypmod: i32,
    /// `bool isunique` — matches a unique index, DISTINCT or GROUP-BY clause.
    pub isunique: bool,
    /// `bool acl_ok` — true if the user has SELECT privilege on all rows from
    /// the table or column.
    pub acl_ok: bool,
}

impl VariableStatData {
    /// `MemSet(&vardata, 0, sizeof(vardata))` (selfuncs.c `examine_variable` /
    /// `examine_simple_variable` entry): a freshly zeroed `VariableStatData`
    /// the examine layer then fills in. `var` is the node the stats will
    /// describe; everything else starts cleared (no relation, no stats tuple,
    /// no freefunc, `acl_ok = false`).
    pub fn zeroed(var: NodeId) -> Self {
        VariableStatData {
            var,
            rel: None,
            stats_tuple: None,
            freefunc: None,
            vartype: InvalidOid,
            atttype: InvalidOid,
            atttypmod: 0,
            isunique: false,
            acl_ok: false,
        }
    }
}

/// `AttStatsSlot` (lsyscache.h), trimmed. `values` / `numbers` are detoasted
/// copies that `get_attstatsslot` deconstructs into the caller's `mcx`; the C
/// `free_attstatsslot` frees them, here they are freed when the `PgVec`s (or
/// the context) drop.
#[derive(Debug)]
pub struct AttStatsSlot<'mcx> {
    /// `Oid staop` — the slot's operator.
    pub staop: Oid,
    /// `Oid stacoll` — the slot's collation.
    pub stacoll: Oid,
    /// `Oid valuetype` — the element type of `values`.
    pub valuetype: Oid,
    /// `Datum *values` (length `nvalues`) — the deconstructed value array.
    pub values: PgVec<'mcx, Datum>,
    /// `float4 *numbers` (length `nnumbers`) — the `stanumbers` array.
    pub numbers: PgVec<'mcx, f32>,
}
