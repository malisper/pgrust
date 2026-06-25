# Audit: backend-rewrite-rewriteSupport (rewrite/rewriteSupport.c)

Ported into the existing `backend-rewrite-core` crate (which owns rewriteSupport.c
per its CATALOG `c_sources`) as a new module `src/support.rs`. Audited
function-by-function against
`../pgrust/postgres-18.3/src/backend/rewrite/rewriteSupport.c`.

## Function inventory (all 3 C functions)

### `IsDefinedRewriteRule(owningRel, ruleName) -> bool`
C: `return SearchSysCacheExists2(RULERELNAME, ObjectIdGetDatum(owningRel),
PointerGetDatum(ruleName));`

Port: `support::IsDefinedRewriteRule(mcx, owning_rel, rule_name)` →
`syscache::SearchSysCacheExists(mcx, RULERELNAME, Value(from_oid(owning_rel)),
Str(rule_name), UNUSED, UNUSED)`. `RULERELNAME` (=60) added to
`types-syscache/src/syscache_ids.rs`; already wired in syscache `cacheinfo.rs`
(reloid 2618, keys [ev_class, rulename]). PointerGetDatum(name) crosses as the
repo's `SysCacheKey::Str` (name column). Failure surface widened to `PgResult`
because the repo catcache search can `ereport` (OOM); C's wrapper is infallible
but the underlying SearchCatCache can elog. MATCH.

### `SetRelationRuleStatus(relationId, relHasRules) -> void`
C: `table_open(RelationRelationId, RowExclusiveLock)`;
`SearchSysCacheCopy1(RELOID, relationId)`; `if (!HeapTupleIsValid) elog(ERROR,
"cache lookup failed for relation %u")`; `classForm = GETSTRUCT`;
`if (classForm->relhasrules != relHasRules) { set field; CatalogTupleUpdate } else
{ CacheInvalidateRelcacheByTuple } ; heap_freetuple; table_close`.

Port: `support::SetRelationRuleStatus(relation_id, rel_has_rules)` does the
`table_open` / `table_close` and the `cache lookup failed` `elog(ERROR)`; the
syscache-copy + `relhasrules` GETSTRUCT compare + CatalogTupleUpdate-or-
CacheInvalidateRelcacheByTuple + heap_freetuple are encapsulated in the new
owner seam `backend-catalog-indexing-seams::set_relation_rule_status(class_rel,
relation_id, rel_has_rules) -> PgResult<bool>` (returning HeapTupleIsValid).
Rationale identical to the established `set_pg_class_reltoastrelid` precedent:
pg_class's `Form_pg_class` is a trimmed projection (`types_cluster::PgClassForm`
has no `relhasrules` and cannot losslessly reform the on-disk tuple), so the
field compare/write and the relcache invalidation must run on the owner's full
syscache copy — that whole compare/update-or-invalidate IS the body of
SetRelationRuleStatus, so it crosses as one owner seam. The seam is DECLARED in
indexing-seams and is NOT installed because backend-catalog-indexing is `todo`
in CATALOG.tsv — sanctioned mirror-pg-and-panic until indexing.c lands (the
recurrence guard exempts a non-`complete` owner). Drop of the open relation on
the error path matches the repo's RAII abort-close model. The "force SI
invalidation even when no change is needed" side effect is preserved (else-arm
in the seam). MATCH (with the seam delegation noted).

### `get_rewrite_oid(relid, rulename, missing_ok) -> Oid`
C: `tuple = SearchSysCache2(RULERELNAME, relid, rulename)`; `if
(!HeapTupleIsValid) { if (missing_ok) return InvalidOid; ereport(ERROR,
ERRCODE_UNDEFINED_OBJECT, "rule \"%s\" for relation \"%s\" does not exist",
rulename, get_rel_name(relid)); }`; `ruleform = GETSTRUCT; Assert(relid ==
ruleform->ev_class); ruleoid = ruleform->oid; ReleaseSysCache; return ruleoid`.

Port: `support::get_rewrite_oid(relid, rulename, missing_ok)` delegates the
SearchSysCache2 + GETSTRUCT `(oid, ev_class)` read + ReleaseSysCache to the new
syscache-owner projection seam
`backend-utils-cache-syscache-seams::search_rewrite_oid(relid, rulename) ->
Option<(Oid /*ruleoid*/, Oid /*ev_class*/)>` (mirrors the existing
`search_relation_relam`/`cast_by_source_target` GETSTRUCT-projection seams;
pg_rewrite anums oid=1, ev_class=3; installed by syscache `init_seams()`). The
control flow stays here: `None` → missing_ok→InvalidOid else
`ERRCODE_UNDEFINED_OBJECT` error built with `get_rel_name` (called directly on
the lsyscache owner crate). The `Assert(relid == ev_class)` is preserved as a
`debug_assert_eq!`. `oid` field crosses as the returned ruleoid. MATCH.

## Seams
- INSTALLED by this unit (`backend-rewrite-core::init_seams`):
  `backend-rewrite-rewritesupport-seams::get_rewrite_oid` (pre-existing
  declaration consumed by objectaddress `OBJECT_RULE`; previously uninstalled,
  now installed — retires that orphan).
- INSTALLED by the syscache owner: `search_rewrite_oid` (new, in
  `backend-utils-cache-syscache::init_seams`).
- DECLARED (not installed; owner indexing.c `todo`):
  `backend-catalog-indexing-seams::set_relation_rule_status` (new) —
  mirror-pg-and-panic until backend-catalog-indexing lands.

## Constants verified
- `RULERELNAME` = 60 (syscache_ids.h); cacheinfo reloid 2618 (RewriteRelationId),
  keys [3 ev_class, 2 rulename]. MATCH.
- `RELATION_RELATION_ID` = 1259, `RowExclusiveLock` = 3. MATCH.
- pg_rewrite anums: oid=1, ev_class=3. MATCH.
- `ERRCODE_UNDEFINED_OBJECT` = 42704. MATCH.

## Divergences
None of logic. The two seam delegations (set_relation_rule_status,
search_rewrite_oid) are GETSTRUCT-projection / trimmed-form encapsulations
following established repo precedent; failure surface widened to PgResult where
the repo's SearchSysCache path can ereport.
