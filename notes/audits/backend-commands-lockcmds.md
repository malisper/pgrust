# Audit: backend-commands-lockcmds (commands/lockcmds.c)

Audited against `../pgrust/postgres-18.3/src/backend/commands/lockcmds.c` (PostgreSQL 18.3).
Result: **PASS** — all 6 functions ported with 100% logic fidelity.

## Function-by-function

### `LockTableCommand` (C 40-64) — `lock_table_command`
- `foreach(p, lockstmt->relations)` → `for p in lockstmt.relations.iter()`. MATCH.
- `RangeVar *rv = (RangeVar *) lfirst(p)` → match `Node::RangeVar`; panic on any other tag
  (mirror-pg: a malformed parse tree is a programming error, not a runtime condition the C
  guards). The parse-node `rawnodes::RangeVar` is re-encoded to the resolver's
  `types_tuple::access::RangeVar` via `to_access_range_var` (the same conversion
  `backend-parser-relation` performs for the same `RangeVarGetRelidExtended` entry point).
- `recurse = rv->inh`. MATCH.
- `RangeVarGetRelidExtended(rv, mode, nowait ? RVR_NOWAIT : 0, RangeVarCallbackForLockTable, &mode)`
  → direct call into ported `backend_catalog_namespace::RangeVarGetRelidExtended`; the callback's
  only C state (`&lockstmt->mode`) is captured by the closure, so the permission gate runs at the
  same mid-lookup point. MATCH.
- `get_rel_relkind(reloid) == RELKIND_VIEW` → `LockViewRecurse(..., NIL)` (empty `Vec`);
  `else if (recurse)` → `LockTableRecurse`. MATCH (branch order preserved).

### `RangeVarCallbackForLockTable` (C 70-107) — `range_var_callback_for_lock_table`
- `!OidIsValid(relid)` → return. MATCH.
- `relkind = get_rel_relkind(relid); if (!relkind) return` → `relkind == 0`. MATCH.
- relkind not in {RELATION, PARTITIONED_TABLE, VIEW} → `ereport(ERROR, WRONG_OBJECT_TYPE,
  "cannot lock relation \"%s\"", errdetail_relkind_not_supported(relkind))`. MATCH
  (errdetail_relkind_not_supported is the ported sibling in backend-catalog-pg-class).
- `get_rel_persistence == RELPERSISTENCE_TEMP` → `MyXactFlags |= XACT_FLAGS_ACCESSEDTEMPNAMESPACE`
  (seam `set_xact_accessed_temp_namespace`). MATCH.
- `LockTableAclCheck(relid, lockmode, GetUserId())`; on !OK →
  `aclcheck_error(aclresult, get_relkind_objtype(get_rel_relkind(relid)), rv->relname)`. MATCH
  (the C re-reads relkind inside the error arg — preserved).

### `LockTableRecurse` (C 116-158) — `lock_table_recurse`
- `find_all_inheritors(reloid, NoLock, NULL)`. MATCH.
- skip `childreloid == reloid`. MATCH.
- `!nowait` → `LockRelationOid`; else `!ConditionalLockRelationOid` → `get_rel_name`; NULL → skip,
  else `ereport(ERROR, LOCK_NOT_AVAILABLE)`. MATCH.
- `!SearchSysCacheExists1(RELOID, child)` → `UnlockRelationOid` + continue. MATCH
  (modeled as `LockGuard::release()`; on the keep path `LockGuard::keep()` makes the lock
  transaction-scoped, exactly C's implicit "function returns, lmgr lock lives to xact end").

### `LockViewRecurse_walker` (C 178-242) — `lock_view_recurse_walker`
- `IsA(node, Query)` → `node.as_query()`. MATCH.
- per-rtable: relid/relkind, relkind filter, `list_member_oid(ancestor_views, relid)` →
  `ancestor_views.contains`, `LockTableAclCheck(check_as_user)` + aclcheck_error, lock
  (nowait/conditional, error path identical), `relkind == VIEW` → LockViewRecurse else
  `rte->inh` → LockTableRecurse. MATCH.
- `query_tree_walker(query, walker, QTW_IGNORE_JOINALIASES)` else
  `expression_tree_walker(node, walker)`. MATCH. The C callback's mid-walk `ereport(ERROR)`
  non-local exit is reproduced by `walk_via`: the bool-returning generic walker callback stashes
  the first `PgError` and aborts (`true`), then `walk_via` re-surfaces it as `Err`.

### `LockViewRecurse` (C 244-274) — `lock_view_recurse`
- `table_open(reloid, NoLock)`; `get_view_query(view)`. MATCH (get_view_query is a seam-and-panic,
  see Divergences).
- `RelationHasSecurityInvoker(view)` ? `GetUserId()` : `view->rd_rel->relowner`. MATCH (seam-and-panic).
- `lappend_oid(ancestor_views, reloid)` then `list_delete_last` after the walk, before propagating
  any error (function-local list, kept for fidelity). MATCH.
- `table_close(view, NoLock)` → `Relation::close(NoLock)`. MATCH.

### `LockTableAclCheck` (C 279-299) — `lock_table_acl_check`
- `aclmask = ACL_MAINTAIN|ACL_UPDATE|ACL_DELETE|ACL_TRUNCATE`; `<= AccessShareLock` adds
  ACL_SELECT; `<= RowExclusiveLock` adds ACL_INSERT; `pg_class_aclcheck`. MATCH.
  (Unit tests cover the three mask tiers.)

## Divergences / seam-and-panic (mirror-pg, not logic changes)

- `get_view_query` (rewriteHandler.c): reads `view->rd_rules`, the relcache rewrite-rule array,
  which `RelationData` does not yet model. Declared as a new seam on
  `backend-rewrite-rewritehandler-seams` and left uninstalled (panic on call) until the relcache
  carries view rules. Guard-exempt: the seam crate's owner dir
  (`backend-rewrite-rewritehandler`) does not resolve to a crate, the same situation as the
  crate's pre-existing uninstalled `query_rewrite`/`build_column_default`.
- `RelationHasSecurityInvoker` (utils/rel.h): reads the *view* `StdRdOptions.security_invoker`;
  the ported `RelationData::rd_options` carries only the *heap* `StdRdOptions` (no
  `security_invoker` field). Same new home + same guard exemption as `get_view_query`.

No `todo!`/`unimplemented!`; no own-logic stubs. residual_own_todos = 0.
