//! `rewrite/rewriteSupport.c`: rule-name existence/OID resolution and the
//! `pg_class.relhasrules` flag write.

use alloc::borrow::ToOwned;
use alloc::format;

use mcx::{Mcx, MemoryContext};
use ::cache::SysCacheKey;
use types_core::{Oid, RELATION_RELATION_ID};
use ::datum::Datum as KeyDatum;
use types_error::{PgError, PgResult, ERRCODE_UNDEFINED_OBJECT, ERROR};

use table as table;
use lsyscache as lsyscache;
use cache_syscache as syscache;
use syscache_seams as syscache_seams;
use indexing_seams as indexing_seams;

/// `RowExclusiveLock` (`lockdefs.h`).
const ROW_EXCLUSIVE_LOCK: types_storage::lock::LOCKMODE = types_storage::lock::RowExclusiveLock;

/// `IsDefinedRewriteRule(owningRel, ruleName)` (rewriteSupport.c): is there a
/// rule by the given name on `owningRel`?
///
/// C is infallible (`SearchSysCacheExists2` returns `bool`), but the repo's
/// catcache search can `ereport` (e.g. OOM building the cache), so the failure
/// surface is `PgResult`.
pub fn IsDefinedRewriteRule(mcx: Mcx<'_>, owning_rel: Oid, rule_name: &str) -> PgResult<bool> {
    syscache::SearchSysCacheExists(
        mcx,
        syscache::RULERELNAME,
        SysCacheKey::Value(KeyDatum::from_oid(owning_rel)),
        SysCacheKey::Str(rule_name),
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
    )
}

/// `SetRelationRuleStatus(relationId, relHasRules)` (rewriteSupport.c): set the
/// relation's `pg_class.relhasrules` field.
///
/// NOTE: caller must be holding an appropriate lock on the relation.
///
/// NOTE: an important side-effect is that an SI invalidation message is sent
/// out to all backends causing relcache entries to be flushed/updated with the
/// new set of rules for the table. This must happen even if no change is needed
/// in the pg_class row.
pub fn SetRelationRuleStatus(relation_id: Oid, rel_has_rules: bool) -> PgResult<()> {
    let ctx = MemoryContext::new("SetRelationRuleStatus");
    let mcx = ctx.mcx();

    // relationRelation = table_open(RelationRelationId, RowExclusiveLock);
    let relation_relation = table::table_open(mcx, RELATION_RELATION_ID, ROW_EXCLUSIVE_LOCK)?;

    // tuple = SearchSysCacheCopy1(RELOID, relationId);  GETSTRUCT field compare;
    // CatalogTupleUpdate (on change) or CacheInvalidateRelcacheByTuple (no
    // change); heap_freetuple. The whole compare/update-or-invalidate runs in
    // the owner against the full syscache copy (the trimmed Form_pg_class
    // projection cannot reform the on-disk tuple); the returned bool is
    // HeapTupleIsValid(tuple).
    let valid = indexing_seams::set_relation_rule_status::call(
        &relation_relation,
        relation_id,
        rel_has_rules,
    )?;
    if !valid {
        // elog(ERROR, "cache lookup failed for relation %u", relationId);
        // table_close runs on the error path too (the relation handle is
        // dropped); mirror the C, which never reaches table_close after elog.
        return Err(PgError::error(format!(
            "cache lookup failed for relation {relation_id}"
        )));
    }

    // table_close(relationRelation, RowExclusiveLock);
    table::table_close(relation_relation, ROW_EXCLUSIVE_LOCK)?;
    Ok(())
}

/// `get_rewrite_oid(relid, rulename, missing_ok)` (rewriteSupport.c): the OID
/// of the named rewrite rule on relation `relid`. With `missing_ok = false` a
/// miss raises `ERRCODE_UNDEFINED_OBJECT`; with `missing_ok = true` it returns
/// `InvalidOid`.
pub fn get_rewrite_oid(relid: Oid, rulename: &str, missing_ok: bool) -> PgResult<Oid> {
    // tuple = SearchSysCache2(RULERELNAME, relid, rulename); GETSTRUCT ->
    // (oid, ev_class); ReleaseSysCache. Returns None on !HeapTupleIsValid.
    let found = syscache_seams::search_rewrite_oid::call(relid, rulename)?;
    let Some((ruleoid, ev_class)) = found else {
        if missing_ok {
            return Ok(::types_core::InvalidOid);
        }
        let ctx = MemoryContext::new("get_rewrite_oid get_rel_name");
        let relname = lsyscache::relation::get_rel_name(ctx.mcx(), relid)?
            .map(|s| s.as_str().to_owned())
            .unwrap_or_default();
        return Err(PgError::new(
            ERROR,
            format!("rule \"{rulename}\" for relation \"{relname}\" does not exist"),
        )
        .with_sqlstate(ERRCODE_UNDEFINED_OBJECT));
    };
    // Assert(relid == ruleform->ev_class);
    debug_assert_eq!(relid, ev_class);
    Ok(ruleoid)
}
