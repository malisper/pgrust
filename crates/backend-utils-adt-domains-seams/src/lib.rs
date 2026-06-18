//! Seam declarations for the genuinely-external callees that
//! `load_domaintype_info` / `InitDomainConstraintRef` /
//! `UpdateDomainConstraintRef` (typcache.c) invoke: the catalog scan, the
//! planner, the executor, and the "Domain constraints" memory-context
//! lifecycle. The *orchestration* (domain-stack crawl, name sort, parent-first
//! `lcons` ordering, NOT NULL handling, lazy context creation, refcounting)
//! lives in the typcache crate; only these per-callee externals cross here.
//!
//! The owning units install these from their `init_seams()` when they land;
//! until then a call panics loudly.

use types_cache::typcache::{DomainCheckConstraintRow, DomainCtxHandle, DomainLevelScan, ExprStateHandle};
use types_core::Oid;
use types_error::PgResult;
use types_nodes::primnodes::Expr;

extern crate alloc;
use alloc::vec::Vec;

seam_core::seam!(
    /// `AllocSetContextCreate(CurrentMemoryContext, "Domain constraints",
    /// ALLOCSET_SMALL_SIZES)` — create the lazy domain-constraint context that
    /// `load_domaintype_info` builds the planned constraints in (and later
    /// reparents to CacheMemoryContext). Returns the new context's handle.
    pub fn create_domain_ctx() -> PgResult<DomainCtxHandle>
);

seam_core::seam!(
    /// One level of `load_domaintype_info`'s `SearchSysCache1(TYPEOID, typeOid)`
    /// (`elog(ERROR)` if missing) + reading `typtype`/`typnotnull`/`typbasetype`.
    /// `is_domain == false` stops the in-crate crawl.
    pub fn lookup_domain_type_level(type_id: Oid) -> PgResult<DomainLevelScan>
);

seam_core::seam!(
    /// The per-level `pg_constraint` CHECK scan: `table_open` /
    /// `systable_beginscan(ConstraintTypidIndexId)` over `contypid == type_id`,
    /// skipping non-CHECK constraints, raising on NULL `conbin`, and returning
    /// each CHECK's `conname` + `conbin` node-string (`TextDatumGetCString`) in
    /// scan (index) order. The typcache plans, sorts by name, and orders
    /// parent-first.
    pub fn scan_domain_check_constraints(type_id: Oid) -> PgResult<Vec<DomainCheckConstraintRow>>
);

seam_core::seam!(
    /// `stringToNode(conbin)` + `expression_planner()` for one CHECK
    /// constraint. Returns the planned expression as the real owned [`Expr`]
    /// value (the typcache stores it at cache lifetime; `Expr` is lifetime-free).
    /// `ctx` is the "Domain constraints" memory context C plans into; the owner
    /// uses it only as scratch (the returned `Expr` is owned, not arena-bound).
    pub fn plan_check_expr(conbin: &str, ctx: DomainCtxHandle) -> PgResult<Expr>
);

seam_core::seam!(
    /// `MemoryContextSetParent(ctx, CacheMemoryContext)` — move the domain
    /// constraint context to the typcache's long-lived cache context.
    pub fn set_parent_to_cache_context(ctx: DomainCtxHandle) -> PgResult<()>
);

seam_core::seam!(
    /// `MemoryContextDelete(ctx)` — free a domain constraint context once its
    /// refcount drops to zero.
    pub fn delete_domain_ctx(ctx: DomainCtxHandle) -> PgResult<()>
);

seam_core::seam!(
    /// `MemoryContextRegisterResetCallback(refctx, dccref_deletion_callback)`:
    /// arrange for the typcache's `release_domain_constraint_ref(ref_token)`
    /// to run when `refctx` is reset/deleted.
    pub fn register_ref_reset_callback(
        refctx: DomainCtxHandle,
        ref_token: u64,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `ExecInitExpr(check_expr, NULL)` in `execctx` — compile one CHECK
    /// expression into an `ExprState`. (`prep_domain_constraints` calls this
    /// per constraint; the list copy + `MemoryContextSwitchTo` ordering lives
    /// in the typcache.)
    ///
    /// GENUINELY UNINSTALLED / blocked: the real `ExecInitExpr`
    /// (`backend-executor-execExpr`) compiles into an `EStateData<'mcx>`
    /// per-query arena, but a `DomainConstraintRef`'s `refctx` is a plain
    /// memory context with no associated `EState`. Until the executor exposes an
    /// `EState`-less `ExecInitExpr(expr, NULL)` over a bare context, this seam
    /// has no owner and a call panics. The planned `check_expr` itself is the
    /// real owned [`Expr`] value, ready for that compile when the substrate
    /// lands.
    pub fn exec_init_expr(
        check_expr: &Expr,
        execctx: DomainCtxHandle,
    ) -> PgResult<ExprStateHandle>
);

seam_core::seam!(
    /// `INJECTION_POINT(name, NULL)` — a no-op unless an injection point is
    /// attached (test harness observation hook).
    pub fn injection_point(name: &str)
);
