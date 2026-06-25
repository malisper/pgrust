# Audit: backend-commands-tablecmds (FAMILY F0)

Branch `port/tablecmds-f0`, commit `c62078bac`. Independent re-derivation from
`postgres-18.3/src/backend/commands/tablecmds.c`, the c2rust translation, and the
Rust port. Scope: F0 (relation create/drop/truncate + on_commit) only; F1-F6
absence is expected and not a finding.

## Gate

- `cargo check -p backend-commands-tablecmds -p backend-commands-tablecmds-seams -p seams-init`
  → Finished (warnings only; no errors).
- `grep -rE 'todo!|unimplemented!' crates/backend-commands-tablecmds/src/` → no matches.
- `grep -rE 'FIXME|TODO|for now|simplified|hack'` over src → no matches.

## Per-F0-function table

| Function | C loc | Port loc | Verdict | Notes |
|---|---|---|---|---|
| DefineRelation | 764 | create.rs:75 | MATCH | All branch order/error codes/CCI bumps faithful. reloptions: C does `transformRelOptions` then `switch(relkind)`→view/partitioned/heap_reloptions; port collapses into one `transform_and_check_reloptions` seam (unported reloptions owner) — legitimate SEAM, not lost logic. Partbound block (transformPartitionBound/check_new_partition_bound/StorePartitionBound) → `define_relation_partbound` seam (F5). Partspec → `define_relation_partspec`. Clone-partition-objects → seam. MergeAttributes/AddRelation*/heap_create_with_catalog all SEAMED to unported owners. cooked-default CookedConstraint is carried as bare Node (heap seam takes trimmed args) — behavior-preserving since the cooked_constraints list is consumed by the (unported) heap owner. |
| BuildDescForRelation | 1380 | create.rs:481 | MATCH | natts, per-col attnum, typenameTypeIdAndMod, ACL_USAGE check, GetColumnDefCollation, attdim>PG_INT16_MAX (32767) ProgramLimitExceeded, setof InvalidTableDefinition, TupleDescInitEntry+Collation, all att.* overrides (notnull/islocal/inhcount/identity/generated/compression/storage), populate_compact_attribute — field-for-field. atttypid read back from att before compression call matches C. |
| StoreCatalogInheritance (no-super path) | 3521 | create.rs:561 | MATCH | `Assert(OidIsValid)` → debug_assert; `supers==NIL` early return faithful; non-empty path → `store_catalog_inheritance_supers` seam (pg_inherits write owner). In-scope empty path is exact. |
| findAttrByName | 3609 | create.rs:580 | MATCH | 1-based index, 0 if none; strcmp→`==`. |
| storage_name | 2460 | create.rs:594 | MATCH | PLAIN/EXTERNAL/EXTENDED/MAIN + "???" default. |
| RemoveRelations | 1538 | drop.rs:61 | MATCH | concurrent lock downgrade + 2 feature-not-supported errors; relkind switch (6 arms + elog default); per-object AcceptInvalidationMessages, RangeVarGetRelidExtended w/ callback, DropErrorMsgNonExistent, concurrent-flag logic, partitioned-index concurrent error, find_all_inheritors lock acquisition, add_exact_object_address; performMultipleDeletions+free. ObjectAddresses via dependency seam (owned, not handle). |
| DropErrorMsgNonExistent | 1462 | helpers.rs:192 | MATCH | schema-not-found (ERROR vs NOTICE on missing_ok) then dropmsgstringarray scan; %s substitution via `.replace`. Loop break-on-match equivalent to C sentinel loop. |
| DropErrorMsgWrongType | 1511 | helpers.rs:233 | MATCH | rightkind `.expect` mirrors Assert; wrongkind optional → errhint only if found, mirroring C `(wentry->kind!='\0')?errhint:0`. dropmsgstringarray verified verbatim (9 entries, codes UNDEFINED_TABLE/OBJECT exactly per C). |
| RangeVarCallbackForDropRelation | 1702 | drop.rs:200 | MATCH | heap/partition stale-lock release, !OidIsValid early return, get_pg_class_drop_info (=SearchSysCache RELOID; None→concurrently dropped→return Ok), expected_relkind remap (PART_TABLE→RELATION, PART_INDEX→INDEX), DropErrorMsgWrongType, dual ownercheck (rel-owner OR schema-owner), system-index-invalid bypass (get_index_isvalid None = INDEXRELID concurrently dropped → return Ok, matches C ReleaseSysCache+return), system-catalog perm error, DROP-INDEX parent lock, partition parent lock. |
| ExecuteTruncate | 1861 | truncate.rs:41 | MATCH | per-rel RangeVarGetRelidExtended+RangeVarCallbackForTruncate, dedup, table_open NoLock, truncate_check_activity, RelationIsLogicallyLogged seam, recurse via find_all_inheritors (other-temp silent skip, truncate_check_rel+activity, no perms on children), partitioned-ONLY error w/ hint. Guts then close explicit rels. |
| ExecuteTruncateGuts | 1985 | truncate.rs:163 | MATCH | CASCADE fixpoint loop (heap_truncate_find_FKs, NOTICE cascade, check_rel/perms/activity); FK check `cfg!(debug_assertions) || behavior==DROP_RESTRICT` mirrors `#ifdef USE_ASSERT_CHECKING/#else`; restart_seqs getOwnedSequences+ownercheck(SEQUENCE); BEFORE-trigger seam; per-rel skip partitioned, foreign-table grouping (Vec linear-scan keyed by serverid ≡ HTAB), create/relfilenode-subid in-place vs serializable-conflict + RelationSetNewRelfilenumber + toast + reindex_relation; pgstat_count_truncate; foreign truncate; reset_sequence; logical WAL record seam; AFTER-trigger seam; list_difference_ptr close via split_off. EState/ResultRelInfo/AfterTriggerBegin/End machinery collapsed into the 2 trigger seams (unported trigger/EState owner) — legitimate SEAM. |
| truncate_check_rel | 2372 | truncate.rs:365 | MATCH | foreign (GetForeignServerIdByRelId + fdw ExecForeignTruncate presence), non-table WrongObjectType, system-catalog perm (allowSystemTableMods, IsSystemClass, IsBinaryUpgrade/LargeObject exception OID 2613), InvokeObjectTruncateHook. Owned model passes (relid,relkind,relnamespace,relname) not Form_pg_class — value carrier, not opaque. |
| truncate_check_perms | 2420 | truncate.rs:409 | MATCH | pg_class_aclcheck ACL_TRUNCATE + aclcheck_error w/ get_relkind_objtype. |
| truncate_check_activity | 2438 | truncate.rs:426 | MATCH | RELATION_IS_OTHER_TEMP error + CheckTableNotInUse(rel,"TRUNCATE"). |
| RangeVarCallbackForTruncate | 19530 | truncate.rs:441 | MATCH | !OidIsValid return; cache-lookup-failed → elog ERROR (`.ok_or_else` w/ "cache lookup failed for relation N"); truncate_check_rel + truncate_check_perms. |
| CheckTableNotInUse | 4416 | smallfns.rs:23 | MATCH | expected_refcnt = nailed?2:1, refcount mismatch → ObjectInUse "being used by active queries"; non-index + AfterTriggerPendingOnRel → ObjectInUse "pending trigger events". |
| SetRelationHasSubclass | 3647 | smallfns.rs:55 | MATCH (SEAMED body) | inward seam installed; modifiable-copy + GETSTRUCT + CatalogTupleUpdate/CacheInvalidate body → `set_relation_has_subclass_catalog` (catalog-write owner). The top-of-fn lock Assert (CheckRelationOidLockedByMe) is omitted — debug-only, acceptable. |
| CheckRelationTableSpaceMove | 3693 | smallfns.rs:66 | MATCH | no-change/MyDatabaseTableSpace+0 → false; RelationIsMapped → FeatureNotSupported "cannot move system relation"; pg_global → InvalidParameterValue; other-temp → FeatureNotSupported; else true. |
| SetRelationTableSpace | 3750 | smallfns.rs:120 | MATCH (SEAMED body) | `debug_assert!(CheckRelationTableSpaceMove)` mirrors Assert; modifiable-copy + MyDatabaseTableSpace→InvalidOid coercion + relfilenode + CatalogTupleUpdate + UnlockTuple + RELKIND_HAS_STORAGE dependency-record body → `set_relation_tablespace_catalog` (passes relkind so the storage check lives with the catalog owner). |
| register_on_commit_action | 19261 | oncommit.rs:49 | MATCH | NOOP/PRESERVE_ROWS early return; lcons→`insert(0)` reverse-order. CacheMemoryContext → thread_local Vec (correct backend lifetime). |
| remove_on_commit_action | 19297 | oncommit.rs:74 | MATCH | first-match deleting_subid + break. |
| PreCommit_on_commit_actions | 19320 | oncommit.rs:88 | MATCH | skip already-deleted; DELETE_ROWS gated on XACT_FLAGS_ACCESSEDTEMPNAMESPACE (hoisted constant — invariant during loop, equivalent); DROP collected; heap_truncate before drop; object_addresses + object_address_present debug_assert + PushActiveSnapshot/perform PERFORM_DELETION_INTERNAL|QUIETLY DROP_CASCADE/PopActiveSnapshot/free. Post-loop USE_ASSERT_CHECKING block omitted — debug-only, acceptable. |
| AtEOXact_on_commit_actions | 19427 | oncommit.rs:167 | MATCH | isCommit?deleting:creating → remove; else reset both subids. retain_mut ≡ foreach_delete_current. |
| AtEOSubXact_on_commit_actions | 19459 | oncommit.rs:191 | MATCH | subabort+creating==mySubid → remove; else relabel creating→parent, deleting→(commit?parent:Invalid). |

## Seam audit

`backend-commands-tablecmds-seams` declares all 13 F0-owned inward seams via
`seam_core::seam!` (pure fn decls, no bodies). `init_seams()` (lib.rs:42-60)
installs exactly those 13 via `.set()` only — no branching/computation in any
install path:
define_relation, build_desc_for_relation, remove_relations, execute_truncate,
check_table_not_in_use, set_relation_has_subclass, check_relation_tablespace_move,
set_relation_tablespace, register_on_commit_action, remove_on_commit_action,
pre_commit_on_commit_actions, at_eoxact_on_commit_actions,
at_eosubxact_on_commit_actions.

`seams-init::init_all()` calls `backend_commands_tablecmds::init_seams()`
(seams-init/src/lib.rs:128). Verified.

F1-F6 inward decls present-but-uninstalled (range_var_callback_owns_relation,
at_exec_change_owner, define_relation_partbound/partspec/clone_partition_objects,
etc.) are EXPECTED — those families are unported, and their owners (or this crate's
later families) install them when they land. Not findings.

Outward seams consumed by F0 (heap_create_with_catalog, merge_attributes,
transform_and_check_reloptions, typename_type_id*, get_column_def_collation,
get_attribute_compression/storage, store_catalog_inheritance_supers, the trigger/
WAL/FDW/sequence/snapmgr truncate seams, set_relation_*_catalog) are calls into
UNPORTED callee owners — legitimately SEAMED (panic until owner lands).

## Design-conformance findings

None.

- No `type X = Oid/usize` stand-ins for typed C things; carriers are real
  (ObjectAddress, TupleDescData, ColumnDef, Relation, CreateStmt/DropStmt/TruncateStmt).
- No `&[u8]` blobs for typed data.
- on_commits backend-global is a `thread_local! RefCell<Vec<OnCommitItem>>`
  (oncommit.rs:43) — correct per-backend semantics, NOT static/Atomic/Mutex/OnceCell.
- Allocs on palloc paths in F0-owned bodies use the fallible Mcx arena
  (alloc_in/vec_with_capacity_in, `clone_in(mcx)?`). The `Vec`/`format!`/
  `to_string` uses are on transient working lists (inherit_oids, relids, connames,
  error-message formatting, RangeVar name bridging) — these mirror C transient
  list/string handling and do not back palloc'd catalog payloads; acceptable.
- `unreachable!`/`expect` are used only for castNode invariants (Node::RangeVar/
  ColumnDef/TypeName/String/List) and NOT-NULL parser guarantees — these mirror C
  `castNode`/`Assert`, not C ereport error paths. Every C `ereport(ERROR)`/`elog`
  maps to an `Err(...)`/`ereport(ERROR)...finish/into_error` with matching SQLSTATE
  and message text.
- Two debug-only C `Assert`s are dropped (SetRelationHasSubclass lock check;
  PreCommit post-loop deleting_subid check). Behavior-preserving in release builds;
  noted but not a finding.

## Spot-checks performed

1. dropmsgstringarray — all 9 entries verified verbatim against C lines 255-310
   (kinds, ERRCODE_UNDEFINED_TABLE vs _OBJECT per entry, all message + hint strings).
2. DropErrorMsgNonExistent/WrongType control flow vs C 1462-1531 (schema branch,
   missing_ok ERROR/NOTICE split, sentinel-loop ↔ `.find`/break, optional errhint).
3. on_commit ordering + accessed-temp gating vs C 19261-19483 (lcons reverse order,
   per-action collection, truncate-before-drop, AtEO(Sub)Xact subid relabel).

## VERDICT: PASS

Every F0 function is MATCH or legitimately SEAMED (delegated to an unported callee
owner). All 13 F0-owned inward seams installed by `init_seams()`, wired into
`seams-init::init_all()`, with no logic in install paths. Zero design-conformance
findings. Gate compiles; no todo!/unimplemented!.
