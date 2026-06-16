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
use types_nodes::primnodes::Expr;
use types_pathnodes::{NodeId, PlannerInfo, SpecialJoinInfo};
use types_selfuncs::{ConstNodeInfo, EstimationInfo, StatsTuple, VariableStatData};

seam_core::seam!(
    /// `estimate_num_groups(root, groupExprs, input_rows, NULL, estinfo)`
    /// (selfuncs.c) — estimate the number of distinct groups the given grouping
    /// expressions take over `input_rows` rows. The expression list crosses as a
    /// borrowed slice of arena node handles (`SpecialJoinInfo.semi_rhs_exprs`).
    ///
    /// `estinfo` mirrors the C `EstimationInfo *estinfo` out-parameter: callers
    /// that pass `Some(&mut info)` receive estimation flags back (the owner ORs
    /// in [`types_selfuncs::SELFLAG_USED_DEFAULT`] when it falls back on a
    /// default), exactly as C does. `None` mirrors C `NULL` (the `pgset`
    /// argument is always `NULL` in the repo's callers, so it is omitted).
    pub fn estimate_num_groups(
        root: &PlannerInfo,
        group_exprs: &[NodeId],
        input_rows: f64,
        estinfo: Option<&mut EstimationInfo>,
    ) -> f64
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
    pub fn get_restriction_variable<'mcx>(
        mcx: Mcx<'mcx>,
        root: &PlannerInfo,
        args: &[NodeId],
        var_relid: i32,
    ) -> PgResult<Option<(VariableStatData, Expr, bool)>>
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
    pub fn examine_variable<'mcx>(
        mcx: Mcx<'mcx>,
        root: &PlannerInfo,
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
    pub fn get_join_variables<'mcx>(
        mcx: Mcx<'mcx>,
        root: &PlannerInfo,
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
    /// values are matched against the bare-word [`Datum`] `constval`. `Err`
    /// carries the syscache / fmgr `ereport(ERROR)`s and OOM.
    pub fn mcv_selectivity<'mcx>(
        mcx: Mcx<'mcx>,
        vardata: &VariableStatData,
        opproc_oid: Oid,
        collation: Oid,
        constval: Datum,
        var_on_left: bool,
    ) -> PgResult<(f64, f64)>
);
