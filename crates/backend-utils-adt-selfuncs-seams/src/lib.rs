//! Seam declarations for the `backend-utils-adt-selfuncs` unit
//! (`utils/adt/selfuncs.c`), trimmed to the planner-side primitives the
//! range/multirange/array selectivity estimators and the cost model call
//! across the dependency cycle: variable recognition, the support-function
//! security check, the `pg_statistic` `stanullfrac` read, the variable-stats
//! release, and the distinct-group estimate.
//!
//! The planner inputs are modeled with the planner's own value types, matching
//! the C signatures: `root` is `&PlannerInfo`, `args` is the operator-argument
//! `List *` as a `&[NodeId]`, and an examined `Node *` is a [`NodeId`] handle
//! into the planner node arena. (The fmgr selectivity dispatch decodes the
//! `PG_GETARG_POINTER(...)` words into these typed planner references before
//! it reaches these entry points.)
//!
//! The owning unit (`backend-utils-adt-selfuncs`, still unported — the
//! examine/estimate machinery is the deferred F1-F7 families) installs these
//! from its `init_seams()` when it lands; until then a call panics loudly
//! (mirror-pg-and-panic).

use mcx::Mcx;
use types_core::primitive::Oid;
use types_datum::datum::Datum;
use types_error::PgResult;
use types_tuple::backend_access_common_heaptuple::Datum as DatumV;
use types_nodes::primnodes::Expr;
use types_pathnodes::planner_run::PlannerRun;
use types_pathnodes::{NodeId, PlannerInfo, SpecialJoinInfo};
use types_selfuncs::{ConstNodeInfo, EstimationInfo, StatsTuple, VariableStatData};

seam_core::seam!(
    /// `estimate_num_groups(root, groupExprs, input_rows, NULL, estinfo)`
    /// (selfuncs.c) — estimate the number of distinct groups the given grouping
    /// expressions take over `input_rows` rows. The expression list crosses as a
    /// borrowed slice of arena node handles (`SpecialJoinInfo.semi_rhs_exprs`,
    /// the planner's grouping target list, ...).
    ///
    /// `run` threads the planner-run RTE/Query store: the owner body examines
    /// each grouping expression via `examine_variable(run.mcx(), run, root,
    /// ...)`, which resolves `simple_rte_array` through the [`PlannerRun`] and
    /// pins `pg_statistic` tuples. The allocation context is recovered from
    /// [`PlannerRun::mcx`] (matching how C reaches `CurrentMemoryContext`), so no
    /// separate `mcx` parameter is threaded — `run` is already plumbed through
    /// the cost-model call sites. `root` is `&mut` because `examine_variable`
    /// re-interns the stripped (PHV/RelabelType-free) expression into the planner
    /// node arena.
    ///
    /// `estinfo` mirrors the C `EstimationInfo *estinfo` out-parameter: callers
    /// that pass `Some(&mut info)` receive estimation flags back (the owner ORs
    /// in [`types_selfuncs::SELFLAG_USED_DEFAULT`] when it falls back on a
    /// default), exactly as C does. `None` mirrors C `NULL` (the `pgset`
    /// argument is always `NULL` in the repo's callers, so it is omitted).
    ///
    /// `Err` carries the examine path's `ereport(ERROR)`s and OOM.
    pub fn estimate_num_groups<'mcx>(
        run: &PlannerRun<'mcx>,
        root: &mut PlannerInfo,
        group_exprs: &[NodeId],
        input_rows: f64,
        estinfo: Option<&mut EstimationInfo>,
    ) -> PgResult<f64>
);

seam_core::seam!(
    /// `get_restriction_variable(root, args, varRelid, &vardata, &other,
    /// &varonleft)` (selfuncs.c): recognize a `(var op const)` /
    /// `(const op var)` restriction clause. Returns `None` when the expression
    /// is not of that form (C: `false`). On `Some`, `vardata` is the examined
    /// variable's stats (the caller releases it via [`release_variable_stats`]),
    /// the [`Expr`] is the "other" operand node, and the bool is `varonleft`.
    /// `args` is the operator's two-element argument `List *` as a borrowed
    /// slice of node handles. Outputs that allocate (the detoasted stats) live
    /// in `mcx`. `Err` carries the recognition path's `ereport(ERROR)`s and OOM.
    pub fn get_restriction_variable<'mcx, 'run>(
        mcx: Mcx<'mcx>,
        run: &PlannerRun<'run>,
        root: &mut PlannerInfo,
        args: &[NodeId],
        var_relid: i32,
    ) -> PgResult<Option<(VariableStatData, Expr<'mcx>, bool)>>
);

seam_core::seam!(
    /// `IsA(node, Const)` decode (nodes/primnodes.h), as
    /// `scalararraysel_containment` applies it to `leftop`: returns `None` when
    /// `node` is not a `Const` (C: the `!IsA` punt), else its
    /// `(constisnull, constvalue, consttype)`. `node` is the planner node
    /// handle for `leftop`. `Err` carries any node-walk `ereport(ERROR)`.
    pub fn const_node_info(node: NodeId) -> PgResult<Option<ConstNodeInfo>>
);

seam_core::seam!(
    /// `examine_variable(root, node, varRelid, &vardata)` (selfuncs.c): locate
    /// the statistical data for an arbitrary expression `node` (used by
    /// `scalararraysel_containment` on its right operand). Fills the
    /// [`VariableStatData`]; its `rel` is `None` (C: `vardata->rel == NULL`)
    /// when the expression could not be identified to a relation. `root` is the
    /// planner state; `node` is the planner node handle of the examined
    /// expression. Outputs that allocate (the detoasted stats) live in `mcx`.
    /// The caller releases the result via [`release_variable_stats`]. `Err`
    /// carries the recognition path's `ereport(ERROR)`s and OOM.
    pub fn examine_variable<'mcx, 'run>(
        mcx: Mcx<'mcx>,
        run: &PlannerRun<'run>,
        root: &mut PlannerInfo,
        node: NodeId,
        var_relid: i32,
    ) -> PgResult<VariableStatData>
);

seam_core::seam!(
    /// `ReleaseVariableStats(vardata)` (selfuncs.h): release the stats tuple a
    /// prior [`get_restriction_variable`] acquired (runs `vardata.freefunc`).
    /// Infallible cleanup.
    pub fn release_variable_stats(vardata: VariableStatData)
);

seam_core::seam!(
    /// `statistic_proc_security_check(vardata, func_oid)` (selfuncs.c): whether
    /// it is safe to apply the support function `func_oid` to this variable's
    /// statistics (leakproof / ACL check). `Err` carries the check's
    /// `ereport(ERROR)`s.
    pub fn statistic_proc_security_check(
        vardata: &VariableStatData,
        func_oid: types_core::primitive::Oid,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `((Form_pg_statistic) GETSTRUCT(statsTuple))->stanullfrac`
    /// (pg_statistic.h): the fraction of NULLs in the column. Reads the
    /// (syscache-owned) `pg_statistic` row the selectivity code only holds as an
    /// opaque [`StatsTuple`].
    pub fn stats_tuple_stanullfrac(stats_tuple: StatsTuple) -> f32
);

seam_core::seam!(
    /// `get_join_variables(root, args, sjinfo, &vardata1, &vardata2,
    /// &join_is_reversed)` (selfuncs.c): examine the two operands of a join
    /// clause, filling the [`VariableStatData`] for each side and reporting
    /// whether the join was syntactically reversed relative to `args`.
    /// `args` is the operator's two-element argument `List *` as a borrowed
    /// slice of node handles. The caller releases each result via
    /// [`release_variable_stats`]. Outputs that allocate (the detoasted stats)
    /// live in `mcx`. `Err` carries the examine path's `ereport(ERROR)`s and
    /// OOM. Used by the join-selectivity estimators (`eqjoinsel`,
    /// `networkjoinsel`, ...).
    pub fn get_join_variables<'mcx, 'run>(
        mcx: Mcx<'mcx>,
        run: &PlannerRun<'run>,
        root: &mut PlannerInfo,
        args: &[NodeId],
        sjinfo: &SpecialJoinInfo,
    ) -> PgResult<(VariableStatData, VariableStatData, bool)>
);

seam_core::seam!(
    /// `mcv_selectivity(vardata, opproc, collation, constval, varOnLeft,
    /// &sumcommon)` (selfuncs.c): for a variable with a most-common-values
    /// list, add up the fractions of the MCV entries that satisfy
    /// `MCV OP CONST` (or `CONST OP MCV`, per `var_on_left`), and separately
    /// the total fraction the MCV list represents (`sumcommon`). Returns
    /// `(mcv_selec, sumcommon)`; both are `0.0` when there is no MCV slot
    /// (C: the `false` from `get_attstatsslot`). The C `FmgrInfo *opproc`
    /// crosses as the operator's underlying function OID (`get_opcode` result),
    /// which the owner re-resolves; `collation` is the input collation. The MCV
    /// values are matched against the canonical [`DatumV`] `constval` through the
    /// by-reference-capable fmgr lane, so a by-reference constant
    /// (`text`/`name`/`bytea`/`numeric`) is compared correctly. `Err` carries the
    /// syscache / fmgr `ereport(ERROR)`s and OOM.
    pub fn mcv_selectivity<'mcx>(
        mcx: Mcx<'mcx>,
        vardata: &VariableStatData,
        opproc_oid: Oid,
        collation: Oid,
        constval: &DatumV<'mcx>,
        var_on_left: bool,
    ) -> PgResult<(f64, f64)>
);

seam_core::seam!(
    /// The non-leakproof permission tail of `statext_is_compatible_clause`
    /// (extended_stats.c:1626): when an extended-statistics clause references a
    /// non-leakproof operator, C requires the user to be able to read every
    /// referenced column of relation `relid` (so the MCV list cannot reveal
    /// values the user may not see). It builds the offset-style column set from
    /// the individual-Var attnums plus `pull_varattnos()` over the matched
    /// sub-expressions, then defers to `all_rows_selectable(root, relid,
    /// clause_attnums)`.
    ///
    /// The clause-walk in `backend-statistics-extended-stats` accumulates the
    /// individual-Var attnums as a planner `Relids` and the sub-expressions as a
    /// `List`; here they cross as `attnums` (the raw, *non*-offset attribute
    /// numbers, exactly the values stored in that `Relids`) and `exprs` (a
    /// borrowed slice of the matched expression nodes). The owner offsets the
    /// attnums by `FirstLowInvalidHeapAttributeNumber`, unions in
    /// `pull_varattnos(expr, relid)` for each expression, and calls
    /// `all_rows_selectable`. Returns `true` iff the user may read all rows.
    /// `Err` carries the ACL/syscache `ereport(ERROR)`s and OOM.
    pub fn statext_clause_attnums_selectable<'mcx>(
        mcx: Mcx<'mcx>,
        run: &PlannerRun<'mcx>,
        root: &PlannerInfo,
        relid: u32,
        attnums: &[i32],
        exprs: &[Expr<'mcx>],
    ) -> PgResult<bool>
);
