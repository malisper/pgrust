//! plancache's slice of the planner (`optimizer/util/clauses.c`'s
//! `expression_planner_with_deps`, `optimizer/path/costsize.c`'s
//! `cpu_operator_cost`).
//!
//! The `pg_plan_queries` driver seam and the `PlannedStmt`-node field-read
//! seams that used to live here were retired by the #159 STEP C plancache
//! de-handle: plancache now owns `PlannedStmt<'static>` values, plans via the
//! value seam `pg_plan_queries_value`, and reads
//! transientPlan/dependsOnRole/invalItems/commandType/utilityStmt/rtable +
//! planTree.total_cost directly off the owned stmts. Only the value seams
//! plancache (and the typcache domain loader) still cross for cost / standalone
//! expression planning remain.

extern crate alloc;
use alloc::vec::Vec;

use ::types_core::primitive::Oid;
use ::types_error::PgResult;
use ::types_plancache::InvalItemKey;

seam_core::seam!(
    /// `expression_planner_with_deps(expr, &relationOids, &invalItems)`
    /// (clauses.c:5479) over the OWNED value [`nodes::primnodes::Expr`] —
    /// prepare a standalone expression for execution (const-fold + `fix_opfuncids`
    /// via `expression_planner`) and extract the relation-OID / function-inval-item
    /// dependencies the const-folded result carries, exactly as the planner's
    /// dependency machinery would. Returns the planned expression (allocated in
    /// `mcx`) plus its `(relationOids, invalItems)`. `GetCachedExpression`
    /// (`plancache.c`) is the sole caller.
    pub fn expression_planner_with_deps_value<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        expr: nodes::primnodes::Expr<'static>,
    ) -> PgResult<(nodes::primnodes::Expr<'static>, Vec<Oid>, Vec<InvalItemKey>)>
);

seam_core::seam!(
    /// `expression_planner(expr)` (planner.c:6779) over the OWNED value
    /// [`nodes::primnodes::Expr`] — prepare a standalone expression for
    /// execution (`eval_const_expressions(NULL, expr)` + `fix_opfuncids`),
    /// returning the planned expression allocated in `mcx`. This is the deps-less
    /// VALUE counterpart of [`expression_planner_with_deps_value`]; the typcache's
    /// domain-constraint loader (`load_domaintype_info`, via the
    /// `backend-utils-adt-domains-seams::plan_check_expr` seam) is the caller —
    /// it discards the dependency lists C's typcache does not track. The owning
    /// planner unit installs this.
    pub fn expression_planner_value<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        expr: nodes::primnodes::Expr<'static>,
    ) -> PgResult<nodes::primnodes::Expr<'static>>
);

seam_core::seam!(
    /// `cpu_operator_cost` (the planner GUC).
    pub fn cpu_operator_cost() -> PgResult<f64>
);
