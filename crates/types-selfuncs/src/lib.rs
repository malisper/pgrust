//! Selectivity-estimation vocabulary (`utils/selfuncs.h`), trimmed to what the
//! range/multirange selectivity ports consume: the default-selectivity
//! constants, the planner's `VariableStatData` (filled by
//! `get_restriction_variable`, released by `ReleaseVariableStats`), and the
//! `AttStatsSlot` the `get_attstatsslot` lookups return.
//!
//! These cross the per-neighbor selectivity / lsyscache seams; the in-crate
//! `rangesel` / `multirangesel` orchestrate over them. The `statsTuple` and the
//! `var` node are planner/syscache-owned memory the selectivity crate only
//! threads back through the seams, so they stay opaque handles here
//! ([`StatsTuple`] / [`StatsVarNode`]); the `AttStatsSlot` value/number arrays
//! are detoasted copies allocated in the caller's `mcx`.

#![no_std]
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]

use mcx::PgVec;
use types_core::primitive::Oid;
use types_datum::datum::Datum;

/// `DEFAULT_INEQ_SEL` (selfuncs.h) — `0.3333333333333333`.
pub const DEFAULT_INEQ_SEL: f64 = 0.3333333333333333;
/// `DEFAULT_RANGE_INEQ_SEL` (selfuncs.h) — `0.005`.
pub const DEFAULT_RANGE_INEQ_SEL: f64 = 0.005;
/// `DEFAULT_MULTIRANGE_INEQ_SEL` (selfuncs.h) — `0.005`.
pub const DEFAULT_MULTIRANGE_INEQ_SEL: f64 = 0.005;

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

/// A `HeapTuple` from `pg_statistic` (`VariableStatData.statsTuple`). It is
/// syscache-owned memory the selectivity code only passes back to the
/// `get_attstatsslot` / `statistic_proc_security_check` seams, so it stays an
/// opaque handle here.
#[derive(Copy, Clone, Debug)]
pub struct StatsTuple {
    /// The `HeapTuple` address (syscache-owned).
    pub ptr: *mut core::ffi::c_void,
}

/// The `Node *var` of a `VariableStatData` — the planner expression the stats
/// describe. Opaque (planner-owned); threaded through `ReleaseVariableStats`.
#[derive(Copy, Clone, Debug)]
pub struct StatsVarNode {
    /// The `Node *` address (planner-owned).
    pub ptr: *mut core::ffi::c_void,
}

/// The `RelOptInfo *rel` of a `VariableStatData` — the relation the variable
/// belongs to (`NULL` when `examine_variable` could not identify one). Opaque
/// (planner-owned); the selectivity code only tests it for presence
/// (C: `if (!vardata.rel)`).
#[derive(Copy, Clone, Debug)]
pub struct StatsRelNode {
    /// The `RelOptInfo *` address (planner-owned).
    pub ptr: *mut core::ffi::c_void,
}

/// `VariableStatData` (selfuncs.h), trimmed to the fields the range selectivity
/// estimators consume. Filled by `get_restriction_variable`; released by
/// `ReleaseVariableStats` (which runs `freefunc(statsTuple)` — modeled by the
/// `release_variable_stats` seam, called from the consumer's RAII guard).
#[derive(Copy, Clone, Debug)]
pub struct VariableStatData {
    /// `Node *var` — the examined expression (opaque, planner-owned).
    pub var: StatsVarNode,
    /// `RelOptInfo *rel` — the relation the variable belongs to, or `None`
    /// (`NULL`). `scalararraysel_containment` punts when this is `None`.
    pub rel: Option<StatsRelNode>,
    /// `HeapTuple statsTuple` — the `pg_statistic` row, or `None`
    /// (`!HeapTupleIsValid`).
    pub stats_tuple: Option<StatsTuple>,
    /// `Oid vartype` — the variable's type OID.
    pub vartype: Oid,
    /// `Oid atttype` — the attribute's type OID.
    pub atttype: Oid,
    /// `int32 atttypmod` — the attribute's typmod.
    pub atttypmod: i32,
    /// Opaque cookie the owner needs to run `freefunc` in
    /// `ReleaseVariableStats` (e.g. the `freefunc` pointer / acl flags). The
    /// selectivity crate only threads it back through the release seam.
    pub release_cookie: usize,
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
