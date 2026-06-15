//! Seam declarations for the `backend-optimizer-prep-prepagg` unit
//! (`optimizer/prep/prepagg.c`).
//!
//! prepagg.c preprocesses aggregate function calls: it detects identical
//! aggregates and aggregates that can share a transition state, builds the
//! `root->agginfos` / `root->aggtransinfos` lists, and accumulates the aggregate
//! execution-cost estimate. Its two public entry points (declared in
//! `optimizer/prep.h`) are called from the planner (planner.c / planmain, still
//! unported), so this crate declares them as inward seams the planner driver can
//! be wired to:
//!
//! * `preprocess_aggrefs(root, clause)` — walk an expression and set up the
//!   aggregate de-dup / transition-state-sharing bookkeeping.
//! * `get_agg_clause_costs(root, aggsplit, costs)` — accumulate the planned
//!   aggregates' cost estimate.
//!
//! It also declares the two *outward* boundary seams prepagg needs that have no
//! owner reachable below it without a cycle: the `pg_aggregate` catalog read
//! (`SearchSysCache1(AGGFNOID)` + `SysCacheGetAttr(agginitval)` +
//! `resolve_aggregate_transtype`, bundled) and `GetAggInitVal`
//! (`getTypeInputInfo` + `OidInputFunctionCall`). The owner is the syscache /
//! aggregate-IO layer (catalog reads + fmgr), which is not callable from prep
//! without a dependency cycle; until that owner installs them, a call panics.
//!
//! ## Model
//!
//! `PlannerInfo` is lifetime-free here. `root->agginfos` / `root->aggtransinfos`
//! are `Vec<NodeId>` of handles into `PlannerInfo.node_arena`
//! ([`ArenaNode::AggInfo`](types_pathnodes::ArenaNode::AggInfo) /
//! [`ArenaNode::AggTransInfo`](types_pathnodes::ArenaNode::AggTransInfo)). The C
//! `AggInfo.aggrefs` is a `List *` of pointers to live in-tree `Aggref`s; here
//! the producer (`preprocess_aggref`) interns the canonical `Aggref` into the
//! arena ([`ArenaNode::Expr`](types_pathnodes::ArenaNode::Expr)`(Expr::Aggref)`)
//! and `AggInfo.aggrefs` holds the resulting `NodeId`s — reading
//! `PlannerInfo::node`/`node_mut` then yields the one shared, mutable `Aggref`,
//! mirroring the C "list of pointers to shared nodes" exactly.

#![allow(non_snake_case)]

/// The subset of `Form_pg_aggregate` (plus the resolved transition type and the
/// `agginitval` text attribute) that `preprocess_aggref` reads after pinning the
/// `AGGFNOID` syscache tuple. Bundled into one value because the C holds the
/// tuple pinned across all of these reads, and `resolve_aggregate_transtype`
/// (parse_agg.c) is folded in so the polymorphic transition type is resolved by
/// the catalog owner from the already-extracted argument types.
#[derive(Clone, Copy, Debug, Default)]
#[allow(non_snake_case)]
pub struct AggCatalogInfo {
    /// `aggform->aggtransfn`.
    pub aggtransfn: types_core::primitive::Oid,
    /// `aggform->aggfinalfn`.
    pub aggfinalfn: types_core::primitive::Oid,
    /// `aggform->aggcombinefn`.
    pub aggcombinefn: types_core::primitive::Oid,
    /// `aggform->aggserialfn`.
    pub aggserialfn: types_core::primitive::Oid,
    /// `aggform->aggdeserialfn`.
    pub aggdeserialfn: types_core::primitive::Oid,
    /// The polymorphism-resolved `aggtranstype`
    /// (`resolve_aggregate_transtype(aggfnoid, aggform->aggtranstype,
    /// inputTypes, numArguments)`).
    pub aggtranstype: types_core::primitive::Oid,
    /// `aggform->aggtransspace`.
    pub aggtransspace: i32,
    /// `aggform->aggfinalmodify` — used for the `shareable` test
    /// (`!= AGGMODIFY_READ_WRITE`).
    pub aggfinalmodify: i8,
    /// `SysCacheGetAttr(AGGFNOID, tuple, Anum_pg_aggregate_agginitval,
    /// &isNull)` — the raw `text` initial-value datum (only valid when
    /// `agginitval_isnull` is false).
    pub agginitval: types_datum::datum::Datum,
    /// Whether `agginitval` was SQL NULL.
    pub agginitval_isnull: bool,
}

seam_core::seam!(
    /// `preprocess_aggrefs(root, clause)` (prepagg.c:109) — walk `clause` and run
    /// the per-`Aggref` de-dup / transition-state-sharing bookkeeping
    /// (`preprocess_aggref`) on every `Aggref` found, filling `root->agginfos` /
    /// `root->aggtransinfos` (arena handles) and the `Aggref`s' `aggno` /
    /// `aggtransno` / `aggtranstype`. Mutates `root` (the arena + the agg lists +
    /// the `numOrderedAggs` / `hasNonPartialAggs` / `hasNonSerialAggs` flags).
    /// `Err` carries the catalog/aggregate-IO `ereport(ERROR)` surface.
    pub fn preprocess_aggrefs<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        root: &mut types_pathnodes::PlannerInfo,
        clause: &types_nodes::primnodes::Expr,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `get_agg_clause_costs(root, aggsplit, costs)` (prepagg.c:558) — accumulate
    /// the planned aggregates' execution-cost estimate into `*costs` for the
    /// given split mode. NOTE that costs are *added*; the caller zeroes the
    /// struct. `Err` carries the cost-eval `ereport(ERROR)` surface.
    pub fn get_agg_clause_costs(
        root: &types_pathnodes::PlannerInfo,
        aggsplit: types_nodes::nodeagg::AggSplit,
        costs: &mut types_pathnodes::AggClauseCosts,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `SearchSysCache1(AGGFNOID, aggfnoid)` + `GETSTRUCT` +
    /// `SysCacheGetAttr(agginitval)` + `resolve_aggregate_transtype(...)`
    /// (prepagg.c:149-216), bundled — the `pg_aggregate` reads `preprocess_aggref`
    /// performs while the tuple is pinned. `input_types` are the already-extracted
    /// argument type OIDs (`get_aggregate_argtypes`), used to resolve a
    /// polymorphic transition type. `Err` on `elog(ERROR, "cache lookup
    /// failed")`. Owned by the syscache / aggregate-catalog layer.
    pub fn get_agg_catalog_info(
        aggfnoid: types_core::primitive::Oid,
        input_types: &[types_core::primitive::Oid],
    ) -> types_error::PgResult<AggCatalogInfo>
);

seam_core::seam!(
    /// `GetAggInitVal(textInitVal, transtype)` (prepagg.c:520) —
    /// `getTypeInputInfo(transtype)` + `TextDatumGetCString` +
    /// `OidInputFunctionCall` to deserialize the aggregate's initial transition
    /// value text into a `Datum` of `transtype`. Owned by the type-IO / fmgr
    /// layer. `Err` carries the input-function `ereport(ERROR)`.
    pub fn get_agg_init_val(
        text_init_val: types_datum::datum::Datum,
        transtype: types_core::primitive::Oid,
    ) -> types_error::PgResult<types_datum::datum::Datum>
);

seam_core::seam!(
    /// `datumIsEqual(value1, value2, typByVal, typLen)` (utils/adt/datum.c) over
    /// the canonical `Datum` word `AggTransInfo.initValue` carries — used by
    /// `find_compatible_trans` to compare two aggregates' initial transition
    /// values. The real `datumIsEqual` (scalar-datum-core) operates on the
    /// byte-model `Datum` enum, not the canonical word the planner carries, so
    /// prepagg crosses this focused seam (owned by the datum layer) rather than
    /// inventing a word↔byte conversion at the prep boundary.
    pub fn datum_is_equal(
        value1: types_datum::datum::Datum,
        value2: types_datum::datum::Datum,
        typ_byval: bool,
        typ_len: i32,
    ) -> types_error::PgResult<bool>
);
