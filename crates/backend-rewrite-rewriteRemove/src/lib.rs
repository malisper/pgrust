//! Port of `src/backend/rewrite/rewriteRemove.c` — the guts of rewrite-rule
//! deletion (`RemoveRewriteRuleById`), the per-class `OCLASS_REWRITE` drop
//! handler `dependency.c`'s `doDeletion` invokes for a `pg_rewrite` object.

#![allow(non_snake_case)]
#![no_std]

extern crate alloc;
use alloc::format;

use mcx::{Mcx, MemoryContext};
use types_catalog::pg_rewrite::RewriteRelationId;
use types_core::Oid;
use types_error::{PgError, PgResult, ERRCODE_INSUFFICIENT_PRIVILEGE, ERROR};
use types_storage::lock::{AccessExclusiveLock, NoLock, RowExclusiveLock};

/// `RemoveRewriteRuleById(ruleOid)` (rewriteRemove.c): delete the `pg_rewrite`
/// tuple for the rule, locking its event relation `AccessExclusiveLock` first
/// (so no query depending on the rule is in progress), then broadcasting an SI
/// relcache inval so all backends rebuild with the new rule set.
pub fn RemoveRewriteRuleById<'mcx>(mcx: Mcx<'mcx>, ruleOid: Oid) -> PgResult<()> {
    /*
     * Open the pg_rewrite relation.
     */
    let rewrite_relation =
        backend_access_table_table::table_open(mcx, RewriteRelationId, RowExclusiveLock)?;

    /*
     * Find the tuple for the target rule.  The C does a writable
     * `systable_beginscan(RewriteRelation, RewriteOidIndexId, oid = ruleOid)`;
     * the owned form resolves the rule's `(rulename, ev_class)` by oid
     * (`get_catalog_object_by_oid`), then re-fetches the writable copy by
     * `(ev_class, rulename)` to obtain its heap `t_self` for the delete.
     */
    let name_evclass =
        backend_utils_cache_syscache_seams::rewrite_name_evclass::call(mcx, ruleOid)?;
    let Some((rulename, event_relation_oid)) = name_evclass else {
        // elog(ERROR, "could not find tuple for rule %u", ruleOid);
        return Err(PgError::new(
            ERROR,
            format!("could not find tuple for rule {ruleOid}"),
        ));
    };

    let ruletup = backend_utils_cache_syscache_seams::rule_tuple_by_relname::call(
        mcx,
        event_relation_oid,
        rulename.as_str(),
    )?;
    let Some((ruletup, _ruleform)) = ruletup else {
        return Err(PgError::new(
            ERROR,
            format!("could not find tuple for rule {ruleOid}"),
        ));
    };

    /*
     * We had better grab AccessExclusiveLock to ensure that no queries are
     * going on that might depend on this rule.
     */
    let event_relation =
        backend_access_table_table::table_open(mcx, event_relation_oid, AccessExclusiveLock)?;

    // if (!allowSystemTableMods && IsSystemRelation(event_relation)) ereport(ERROR, ...);
    if !backend_utils_init_small::globals::allowSystemTableMods()
        && backend_catalog_catalog::IsSystemRelation(&event_relation)
    {
        return Err(PgError::new(
            ERROR,
            format!(
                "permission denied: \"{}\" is a system catalog",
                event_relation.name()
            ),
        )
        .with_sqlstate(ERRCODE_INSUFFICIENT_PRIVILEGE));
    }

    /*
     * Now delete the pg_rewrite tuple for the rule.
     */
    backend_catalog_indexing_seams::catalog_tuple_delete::call(
        &rewrite_relation,
        ruletup.tuple.t_self,
    )?;

    rewrite_relation.close(RowExclusiveLock)?;

    /*
     * Issue shared-inval notice to force all backends (including me!) to
     * update relcache entries with the new rule set.
     */
    backend_utils_cache_inval::cache_invalidate::CacheInvalidateRelcache(&event_relation)?;

    /* Close rel, but keep lock till commit... */
    event_relation.close(NoLock)?;

    Ok(())
}

/// Install the rewriteRemove.c-owned seam.
pub fn init_seams() {
    // The inward seam carries no `mcx` (the C `RemoveRewriteRuleById(Oid)`
    // allocates in `CurrentMemoryContext`); wrap it in a scratch context.
    backend_rewrite_rewriteRemove_seams::RemoveRewriteRuleById::set(|rule_oid| {
        let ctx = MemoryContext::new("RemoveRewriteRuleById");
        RemoveRewriteRuleById(ctx.mcx(), rule_oid)
    });
}
