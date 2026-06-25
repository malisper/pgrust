# Self-audit: backend-commands-publicationcmds (publicationcmds.c, PG 18.3)

Repo per-owner-seam model (NOT the src-idiomatic `rt::` facade). `cargo check
--workspace` is 0 errors; `grep -E 'todo!|unimplemented!'` over `src/` is empty.

## Function-by-function parity

- `parse_publication_options` — full WITH(...) loop: publish (split_identifier_string +
  insert/update/delete/truncate), publish_via_partition_root, publish_generated_columns;
  conflicting-defelem + syntax errors mirrored.
- `defGetGeneratedColsOption` — none/stored via pg_strcasecmp; the 42601 error + detail.
- `ObjectsInPublicationToOids` — PUBLICATIONOBJ_TABLE/_TABLES_IN_SCHEMA/_TABLES_IN_CUR_SCHEMA
  (get_namespace_oid / fetch_search_path first); dedup via list_append_unique_oid.
- `contain_invalid_rfcolumn_walker` / `pub_rf_contains_invalid_column` (pub) — REPLICA IDENTITY
  FULL skip, pubviaroot topmost-ancestor, prqual via SearchSysCache2 + string_to_node, replica-
  ident bitmap membership; walker stashes the first PgError and aborts (matching ereport longjmp).
- `pub_contains_invalid_column` (pub) — column-list + generated-column validation; REPLICA
  IDENTITY FULL no-collist; idattrs loop with pubviaroot attnum translation.
- `InvalidatePubRelSyncCache` (inward seam) — CacheInvalidateRelSyncAll / per-relid
  CacheInvalidateRelSync over GetPublicationRelations + GetAllSchemaPublicationRelations.
- `contain_mutable_or_user_functions_checker` — func_volatile != IMMUTABLE || >= FirstNormalObjectId.
- `check_simple_rowfilter_expr(_walker)` — the C switch(nodeTag) rendered as a match on the `Expr`
  enum (OpExpr|DistinctExpr|NullIfExpr share `opno`; ScalarArrayOpExpr.opno; RowCompareExpr.opnos;
  Var.varattno<0 system col; supported set + default error); user-defined type/function/collation
  checks via expr_type_info / check_functions_in_node / exprCollation / expr_input_collation_node;
  the 0A000 errors with errposition.
- `TransformPubWhereClauses` — partitioned-WHERE guard, fresh make_parsestate +
  addRangeTableEntryForRelation + addNSItemToQuery, transformWhereClause (EXPR_KIND_WHERE,
  "PUBLICATION WHERE"), assign_expr_collations, expand_generated_columns_in_expr (rt_index 1),
  check_simple_rowfilter_expr.
- `CheckPubRelationColumnList` — schema-present + partitioned-no-via-root column-list errors.
- `CreatePublication` (pub) — db ACL, FOR ALL TABLES superuser, name-dup, inline pg_publication
  tuple form/insert (namein validation + name_datum, GetNewOidWithIndex, all 10 attrs,
  CatalogTupleInsert, recordDependencyOnOwner), CommandCounterIncrement, FOR ALL TABLES relcache-all
  inval / else OpenTableList+Transform+Check+AddTables / LockSchemaList+AddSchemas, post-create hook,
  wal_level<logical WARNING.
- `AlterPublicationOptions` — via-root-false guard scan (per-root prqual/prattrs), heap_modify_tuple
  of *_given columns, CatalogTupleUpdate, relcache inval (root-relids GetPubPartitionOptionRelations
  vs GetPublicationRelations + schema rels), event-trigger collect + post-alter hook.
- `AlterPublicationTables` — AP_AddObjects/AP_DropObjects/AP_SetObjects (old-vs-new match via
  nodes_equal over decoded prqual + bms_equal_opt over prattrs int2vector; drop the rest).
- `AlterPublicationSchemas` — Add (disallow if any collist) / Drop / Set (list_difference).
- `CheckAlterPublication` — superuser-for-schemas + FOR ALL TABLES guards.
- `AlterPublication` (pub) — owner check, options branch vs objects branch (CheckAlterPublication,
  lock, re-fetch, AlterPublicationTables + AlterPublicationSchemas).
- `RemovePublicationRelById` / `RemovePublicationSchemaById` (inward seams) — genam systable index
  scan by oid (PublicationRel/NamespaceObjectIndexId) since by-oid syscaches are absent; deform
  prrelid/pnnspid; InvalidatePublicationRels; CatalogTupleDelete.
- `RemovePublicationById` (inward seam) — SearchSysCache1(PUBLICATIONOID); puballtables relcache-all
  inval; CatalogTupleDelete.
- `OpenTableList`/`CloseTableList`/`LockSchemaList` — dup filtering (relids/relids_with_rf/_collist),
  recurse via find_all_inheritors, schema lock + existence recheck.
- `PublicationAddTables/DropTables/AddSchemas/DropSchemas` — owner check, publication_add_relation/
  _schema, event-trigger + post-create hook (ALTER path), publication_rel/namespace_map_oid +
  performDeletion(DROP_CASCADE).
- `AlterPublicationOwner_internal` / `AlterPublicationOwner` (inward seam) / `AlterPublicationOwner_oid`
  (inward seam) — owner-equal short-circuit, superuser bypass, check_can_set_role, new-owner db ACL,
  FOR ALL TABLES / FOR TABLES IN SCHEMA superuser-owner guards, heap_modify_tuple(pubowner) +
  CatalogTupleUpdate + changeDependencyOnOwner + post-alter hook.

## 6 inward seams installed (init_seams, wired into seams-init)

alter_publication_owner_oid, RemovePublicationById, RemovePublicationRelById,
RemovePublicationSchemaById, AlterPublicationOwner, InvalidatePubRelSyncCache. Each installer
wrapper spins a fresh `mcx::MemoryContext` (the Mcx-free seam contract; mcx has no ambient context).

## Owner-seam decls ADDED to other -seams crates (seam-and-panic)

- `backend-rewrite-rewritehandler-seams::expand_generated_columns_in_expr`
  `(mcx, Option<Expr>, rel_oid, rt_index) -> PgResult<Option<Expr>>` — expression-level
  rewriteHandler.c:4494 (the only existing one was the planner PlannerInfo/NodeId form). Installed
  by the rewriteHandler.c owner when ported; until then a call panics. Reached by
  TransformPubWhereClauses.
- `backend-commands-event-trigger-seams::event_trigger_collect_simple_command_publication`
  `(ObjectAddress, ObjectAddress, AlterPublicationStmt) -> PgResult<()>` — the existing
  collect-simple-command seams are stmt-typed (CreateOpFamilyStmt / create_schema), so a
  publication-typed variant was added. Installed by the event_trigger.c owner; until then a call
  panics. Reached by PublicationAddTables/AddSchemas + AlterPublicationOptions.

## Not-fully-ported

None. Every function in publicationcmds.c is ported with full logic. Cross-subsystem callees that
are not this file's responsibility are reached through their owners' existing `-seams` crates
(publication catalog, aclchk, objectaccess, inherits, lmgr, xact, inval, genam, define, varlena,
lsyscache, miscinit, dbcommands, tablespace-globals, postgres interrupts, nodeFuncs, read,
analyze/clause/collate/relation parser).
