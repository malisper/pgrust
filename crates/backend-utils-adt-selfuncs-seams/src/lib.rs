//! Seam declarations for the `backend-utils-adt-selfuncs` unit
//! (`utils/adt/selfuncs.c`), trimmed to the planner-side primitives the
//! range/multirange selectivity estimators call across the dependency cycle:
//! variable recognition, the support-function security check, the
//! `pg_statistic` `stanullfrac` read, and the variable-stats release.
//!
//! `root` / `args` are the raw fmgr argument words (`PG_GETARG_POINTER(0)` /
//! `PG_GETARG_POINTER(2)`): the planner `PlannerInfo *` and operator argument
//! `List *`. They are passed as `Datum` machine words because the planner-node
//! model they point at is owned by the (not-yet-ported) planner; the provider
//! retypes them.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use mcx::Mcx;
use types_datum::datum::Datum;
use types_error::PgResult;
use types_nodes::primnodes::Expr;
use types_selfuncs::{StatsTuple, VariableStatData};

seam_core::seam!(
    /// `get_restriction_variable(root, args, varRelid, &vardata, &other,
    /// &varonleft)` (selfuncs.c): recognize a `(var op const)` /
    /// `(const op var)` restriction clause. Returns `None` when the expression
    /// is not of that form (C: `false`). On `Some`, `vardata` is the examined
    /// variable's stats (the caller releases it via [`release_variable_stats`]),
    /// the [`Expr`] is the "other" operand node, and the bool is `varonleft`.
    /// Outputs that allocate (the detoasted stats) live in `mcx`. `Err` carries
    /// the recognition path's `ereport(ERROR)`s and OOM.
    pub fn get_restriction_variable<'mcx>(
        mcx: Mcx<'mcx>,
        root: Datum,
        args: Datum,
        var_relid: i32,
    ) -> PgResult<Option<(VariableStatData, Expr, bool)>>
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
