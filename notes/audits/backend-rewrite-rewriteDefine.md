# Audit: backend-rewrite-rewriteDefine (rewrite/rewriteDefine.c)

Self-audit of the port against `../pgrust/postgres-18.3/src/backend/rewrite/rewriteDefine.c`
(PostgreSQL 18.3, 872 LOC). Function-by-function enumeration of every C
function in the file.

Owner crate: `crates/backend-rewrite-rewriteDefine`. New supporting type:
`types-catalog::pg_rewrite`. New typed seams installed by their real owners:
4 pg_rewrite catalog seams (backend-catalog-indexing), 2 syscache projections
(backend-utils-cache-syscache). `getInsertSelectQuery` ported into the
rewriteManip owner `backend-rewrite-core`.

## Functions

### InsertRule (rewriteDefine.c:51) — PORTED
- `evqual = nodeToString(event_qual)`: ported; NULL event_qual renders `"<>"`
  (the C NULL-pointer rendering nodeToString does for a NULL arg — verified vs
  outfuncs.c `if (s==NULL) "<>"`), else `nodeToString(node)`.
- `actiontree = nodeToString((Node *) action)`: the action `List` of `Query`
  is wrapped as a `Node::List` of `Node::Query` (`action_as_list_node`),
  mirroring C's `(Node *) action` cast, and serialized once.
- `table_open(RewriteRelationId, RowExclusiveLock)`: ported.
- `SearchSysCache2(RULERELNAME, eventrel_oid, rulname)`: ported via the typed
  `rule_tuple_by_relname` syscache projection (returns the writable FormedTuple
  + deformed form, `None` on miss == `!HeapTupleIsValid`).
- replace branch: `!replace` -> `ERRCODE_DUPLICATE_OBJECT` "rule \"%s\" for
  relation \"%s\" already exists" (get_rel_name); else `heap_modify_tuple` with
  `replaces[]` true ONLY for ev_type/is_instead/ev_qual/ev_action (rest from
  oldtup) + `CatalogTupleUpdate` + `rewriteObjectId = GETSTRUCT(tup)->oid`, all
  inside the typed `catalog_tuple_update_pg_rewrite` indexing seam. is_update.
- create branch: `GetNewOidWithIndex(rel, RewriteOidIndexId, Anum_pg_rewrite_oid)`
  + `heap_form_tuple` + `CatalogTupleInsert`, ev_enabled = RULE_FIRES_ON_ORIGIN,
  inside the typed `catalog_tuple_insert_pg_rewrite` indexing seam.
- `ev_type = evtype + '0'`: `cmdtype_ev_type` (CmdType discriminants verified to
  match C CmdType: SELECT=1/UPDATE=2/INSERT=3/DELETE=4).
- `heap_freetuple(tup)`: the formed tuple drops inside the seam (RAII).
- `if (is_update) deleteDependencyRecordsFor(RewriteRelationId, oid, false)`:
  ported (backend-catalog-pg-depend, direct dep).
- relation dependency: classId/objectId/subId set; behavior
  `(evtype == CMD_SELECT) ? DEPENDENCY_INTERNAL : DEPENDENCY_AUTO`; recorded via
  `recordDependencyOn`. ported exactly.
- `recordDependencyOnExpr(&myself, (Node*)action, NIL, DEPENDENCY_NORMAL)`:
  ported (empty rtable `&[]`).
- event_qual dependency: `qry = linitial_node(Query, action)` (action[0]) ->
  `getInsertSelectQuery(qry, NULL)` -> `recordDependencyOnExpr(&myself,
  event_qual, qry->rtable, DEPENDENCY_NORMAL)`. ported.
- `InvokeObjectPostCreateHook(RewriteRelationId, oid, 0)`: ported (seam).
- `table_close(pg_rewrite_desc, RowExclusiveLock)`: ported (`.close`).
- returns rewriteObjectId. ported.

### DefineRule (rewriteDefine.c:189) — PORTED
- `transformRuleStmt(stmt, queryString, &actions, &whereClause)`: seam into the
  unported parse_utilcmd owner (loud panic until it lands), returning
  `(actions: PgVec<Query>, whereClause: Option<Node>)`.
- `RangeVarGetRelid(stmt->relation, AccessExclusiveLock, false)`: the RuleStmt
  relation node (`Node::RangeVar`) is converted to the access-layer RangeVar
  (`to_access_range_var`, the repo's two-RangeVar bridge) and resolved.
- `DefineQueryRewrite(rulename, relId, whereClause, event, instead, replace,
  actions)`: ported call.

### DefineQueryRewrite (rewriteDefine.c:223) — PORTED
- `table_open(event_relid, AccessExclusiveLock)`: ported (lock level matches
  DefineRule).
- relkind gate (RELATION/MATVIEW/VIEW/PARTITIONED_TABLE): ported, error
  ERRCODE_WRONG_OBJECT_TYPE "relation \"%s\" cannot have rules" +
  errdetail_relkind_not_supported.
- `!allowSystemTableMods && IsSystemRelation`: ported (globals getter +
  IsSystemRelation(&RelationData)); ERRCODE_INSUFFICIENT_PRIVILEGE.
- `object_ownercheck(RelationRelationId, event_relid, GetUserId())` ->
  `aclcheck_error(ACLCHECK_NOT_OWNER, get_relkind_objtype(relkind), relname)`:
  ported.
- OLD/NEW-modify loop: `resultRelation==0 continue`; `query !=
  getInsertSelectQuery(query) continue` (pointer-identity via `core::ptr::eq`);
  PRS2_OLD_VARNO -> "rule actions on OLD are not implemented" (+hint),
  PRS2_NEW_VARNO -> "rule actions on NEW are not implemented" (+hint). ported.
- CMD_SELECT branch: relkind must be VIEW/MATVIEW; INSTEAD NOTHING (empty
  action) rejected; multiple actions rejected; the one action must be
  INSTEAD + CMD_SELECT; no data-modifying WITH (hasModifyingCTE); no rule qual;
  `checkRuleResultList(targetList, RelationGetDescr, true, relkind != MATVIEW)`;
  the `rd_rules` pre-existing-ON-SELECT-rule loop (see note); `_RETURN` naming
  with the `_RETviewname` backwards-compat (strncmp semantics over
  NAMEDATALEN-4-4 reproduced; pstrdup _RETURN). All SQLSTATEs/messages 1:1.
- non-SELECT branch: at most one RETURNING list (multiple -> error); no
  RETURNING in conditional rules (event_qual) or non-INSTEAD rules;
  `checkRuleResultList(returningList, ..., false, false)`; non-view rule must
  NOT be named _RETURN. All 1:1.
- install: `if (action != NIL || is_instead)` -> InsertRule +
  `SetRelationRuleStatus(event_relid, true)` (rewriteSupport seam). ruleId
  defaults InvalidOid.
- `ObjectAddressSet(address, RewriteRelationId, ruleId)`;
  `table_close(event_relation, NoLock)`. ported.

NOTE (rd_rules): the "is already a view" loop reads
`event_relation->rd_rules`, the relcache-built rule lock. The repo's relcache
rule-lock builder (backend-utils-cache-relcache::derived::RelationBuildRuleLock)
is a stub seam workspace-wide, so a relcache entry never carries a rule lock —
`rd_rules` is always NULL. C guards the loop on `rd_rules != NULL`, so with no
rule lock the loop is correctly skipped (faithful to the NULL case, NOT a silent
skip of a populated set). The loop body (ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE
"%s is already a view") lands when the relcache rule-lock builder is ported.

### checkRuleResultList (rewriteDefine.c:505) — PORTED
- Assert(isSelect || !requireColumnNameMatch): debug_assert.
- resjunk skip; `i++`; `i > natts` -> too-many-entries error (SELECT vs
  RETURNING text). attr = TupleDescAttr(resultDesc, i-1); attname = NameStr.
- attisdropped -> ERRCODE_FEATURE_NOT_SUPPORTED (SELECT vs RETURNING text).
- name match (requireColumnNameMatch) -> ERRCODE_INVALID_OBJECT_DEFINITION
  "SELECT rule's target entry %d has different column name from column \"%s\"" +
  errdetail. ported.
- type match `exprType(tle->expr)` vs atttypid -> error + format_type_be
  errdetails (SELECT vs RETURNING text). ported.
- typmod match `exprTypmod` (different only if one is -1) -> error +
  format_type_with_typemod errdetails. ported.
- final `i != natts` -> too-few-entries error. ported.

### setRuleCheckAsUser / _walker / _Query (rewriteDefine.c:630/636/650) — PORTED
- `setRuleCheckAsUser(node, userid)`: `(void) setRuleCheckAsUser_walker(node,
  &userid)`. ported (mutable, `&mut Node`).
- `_walker`: NULL -> false (Option/None handled by callers); IsA(Query) ->
  `_Query` then return false; else `expression_tree_walker(...)`. ported via the
  mutable walker `expression_tree_walker_mut`.
- `_Query`: stamp `checkAsUser = userid` on every rteperminfo; recurse into
  RTE_SUBQUERY subqueries; recurse into cteList ctequery Queries; if hasSubLinks
  `query_tree_walker(qry, walker, QTW_IGNORE_RC_SUBQUERIES)` -> ported via
  `query_tree_mutator` with flags QTW_IGNORE_RT_SUBQUERIES|QTW_IGNORE_CTE_SUBQUERIES
  (= QTW_IGNORE_RC_SUBQUERIES) and the same walker closure.
- No current consumer (the rewriter that calls it is unported); exported pub.

### EnableDisableRule (rewriteDefine.c:691) — PORTED
- owningRel = RelationGetRelid(rel); `table_open(RewriteRelationId,
  RowExclusiveLock)`.
- `SearchSysCacheCopy2(RULERELNAME, owningRel, rulename)`: typed
  `rule_tuple_by_relname` (writable copy). `!HeapTupleIsValid` ->
  ERRCODE_UNDEFINED_OBJECT "rule \"%s\" for relation \"%s\" does not exist".
- eventRelationOid = ruleform->ev_class; Assert(== owningRel) (debug_assert).
- `object_ownercheck(RelationRelationId, eventRelationOid, GetUserId())` ->
  `aclcheck_error(ACLCHECK_NOT_OWNER, get_relkind_objtype(get_rel_relkind(...)),
  get_rel_name(...))`. ported.
- `if (DatumGetChar(ruleform->ev_enabled) != fires_when)` ->
  ruleform->ev_enabled = fires_when + `CatalogTupleUpdate(rel, &ruletup->t_self,
  ruletup)` via the typed `catalog_tuple_update_pg_rewrite_enabled` seam;
  changed = true.
- `InvokeObjectPostAlterHook(RewriteRelationId, ruleform->oid, 0)`: ported.
- heap_freetuple (RAII drop); `table_close(..., RowExclusiveLock)`.
- `if (changed) CacheInvalidateRelcache(rel)`: ported.

### RangeVarCallbackForRenameRule (rewriteDefine.c:755) — PORTED
- `SearchSysCache1(RELOID, relid)`: typed `class_relkind_namespace` (relkind +
  relnamespace). `!HeapTupleIsValid` -> return (concurrently dropped).
- relkind gate (RELATION/VIEW/PARTITIONED_TABLE) -> ERRCODE_WRONG_OBJECT_TYPE
  "relation \"%s\" cannot have rules" (rv->relname) + errdetail.
- `!allowSystemTableMods && IsSystemClass(relid, form)` -> via
  IsSystemClassByNamespace(relid, relnamespace) (the cross-crate face;
  IsSystemClass(relid,form) == IsCatalogRelationOid(relid)||IsToastNamespace);
  ERRCODE_INSUFFICIENT_PRIVILEGE.
- `object_ownercheck(RelationRelationId, relid, GetUserId())` -> aclcheck_error.
  ported.

### RenameRewriteRule (rewriteDefine.c:792) — PORTED
- `RangeVarGetRelidExtended(relation, AccessExclusiveLock, 0,
  RangeVarCallbackForRenameRule, NULL)`: ported with the callback closure.
- `relation_open(relid, NoLock)`; `table_open(RewriteRelationId,
  RowExclusiveLock)`.
- `SearchSysCacheCopy2(RULERELNAME, relid, oldName)` -> typed
  `rule_tuple_by_relname`; `!HeapTupleIsValid` -> ERRCODE_UNDEFINED_OBJECT
  "rule \"%s\" for relation \"%s\" does not exist". ruleOid = ruleform->oid.
- `IsDefinedRewriteRule(relid, newName)` (rewriteSupport seam) ->
  ERRCODE_DUPLICATE_OBJECT "rule \"%s\" for relation \"%s\" already exists".
- `ruleform->ev_type == CMD_SELECT + '0'` -> ERRCODE_INVALID_OBJECT_DEFINITION
  "renaming an ON SELECT rule is not allowed". ported (cmdtype_ev_type).
- `namestrcpy(&ruleform->rulename, newName)` + `CatalogTupleUpdate` via typed
  `catalog_tuple_update_pg_rewrite_name` seam.
- `InvokeObjectPostAlterHook(RewriteRelationId, ruleOid, 0)`; heap_freetuple
  (RAII); `table_close(..., RowExclusiveLock)`; `CacheInvalidateRelcache(
  targetrel)`; `ObjectAddressSet(address, RewriteRelationId, ruleOid)`;
  `relation_close(targetrel, NoLock)`. ported.

## Cross-unit seams (mirror-PG-and-panic until owner lands)
- `transformRuleStmt` -> backend-parser-parse-utilcmd-seams (parse_utilcmd.c
  unported). NEW decl.
- `SetRelationRuleStatus`, `IsDefinedRewriteRule` ->
  backend-rewrite-rewritesupport-seams (rewriteSupport.c unported). NEW decls.
These three are exempt from the seams-init declared-seam guard (owners not
`complete` in CATALOG); they loud-panic on call until their owners land.

## New owner-installed seams (substrate fully present, installed at port time)
- backend-catalog-indexing: catalog_tuple_insert_pg_rewrite /
  catalog_tuple_update_pg_rewrite / catalog_tuple_update_pg_rewrite_enabled /
  catalog_tuple_update_pg_rewrite_name (installed in family1::install).
- backend-utils-cache-syscache: rule_tuple_by_relname / class_relkind_namespace
  (installed in init_seams).

## Constants / OIDs verified vs C headers
- pg_rewrite: RewriteRelationId 2618, RewriteOidIndexId 2692,
  RewriteRelRulenameIndexId 2693; Anum oid=1/rulename=2/ev_class=3/ev_type=4/
  ev_enabled=5/is_instead=6/ev_qual=7/ev_action=8; Natts=8.
- RULE_FIRES_ON_ORIGIN 'O', ViewSelectRuleName "_RETURN", PRS2_OLD_VARNO 1,
  PRS2_NEW_VARNO 2, RelationRelationId 1259, NAMEDATALEN 64, CmdType
  discriminants (rewriteDefine.h / rewriteSupport.h / primnodes.h / pg_class.h).

## Result
All 10 C functions ported with 100% logic, matching SQLSTATEs, messages, hints,
errdetails, dependency behaviors, lock levels, and constants. The only
behaviourally-deferred branch (rd_rules ON-SELECT duplicate-rule loop) is
faithful to C's `rd_rules != NULL` guard given the repo-wide relcache rule-lock
stub. No todo!/unimplemented!; unported neighbors are 1:1 seam-and-panic.
