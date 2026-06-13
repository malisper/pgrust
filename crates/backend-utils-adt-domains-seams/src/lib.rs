//! Seam declarations for domain-constraint compilation
//! (`utils/adt/domains.c` + the planner/executor it drives), as consumed by
//! `load_domaintype_info` / `InitDomainConstraintRef` /
//! `UpdateDomainConstraintRef` in the typcache.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.
//!
//! The planned-constraint `List*` and the "Domain constraints" / ref memory
//! contexts are opaque to the typcache; they ride as the
//! [`types_cache::typcache::ConstraintListHandle`] /
//! [`types_cache::typcache::DomainCtxHandle`] tokens the owner mints.

use types_cache::typcache::{ConstraintListHandle, DomainCtxHandle};
use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    /// `load_domaintype_info`'s catalog work: for the domain stack rooted at
    /// `type_id`, scan `pg_constraint` for CHECK constraints, plan each, and
    /// store the planned `DomainConstraintState` list in a freshly-created
    /// "Domain constraints" context (parent-first ordering, NOT NULL check
    /// prepended). Returns `None` when the domain has no constraints (no
    /// context is created — matching the C lazy allocation), else
    /// `Some((ctx, list))` where `ctx` is the new context (the caller
    /// reparents it to the cache context). `Err` carries the scan/plan
    /// `ereport(ERROR)` surface.
    pub fn scan_and_plan_domain_constraints(
        type_id: Oid,
    ) -> PgResult<Option<(DomainCtxHandle, ConstraintListHandle)>>
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
    /// `prep_domain_constraints(constraints, execctx)`: flat-copy each
    /// `DomainConstraintState` and run `ExecInitExpr` in `execctx`, returning
    /// a new executable constraint list. `Err` carries the init `ereport`s.
    pub fn prep_domain_constraints(
        constraints: ConstraintListHandle,
        execctx: DomainCtxHandle,
    ) -> PgResult<ConstraintListHandle>
);

seam_core::seam!(
    /// `INJECTION_POINT(name, NULL)` — a no-op unless an injection point is
    /// attached (test harness observation hook).
    pub fn injection_point(name: &str)
);
