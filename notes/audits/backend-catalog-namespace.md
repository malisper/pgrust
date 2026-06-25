# Audit: backend-catalog-namespace

- **C source**: `src/backend/catalog/namespace.c` (PostgreSQL 18.3, 5090 lines)
- **c2rust reference**: `../pgrust/c2rust-runs/backend-catalog-namespace/src/namespace.rs`
- **Port**: `crates/backend-catalog-namespace/src/lib.rs` (single module)
- **Types**: `crates/types-namespace`, `crates/types-acl`, `crates/types-syscache`
- **Inward seams**: `crates/backend-catalog-namespace-seams`
- **Audit date**: 2026-06-12, independent re-derivation from C + c2rust

## Method

Function inventory built by scanning `namespace.c` for every definition
(110 functions including statics/inlines), cross-checked against the c2rust
rendering's `fn` list. Every function read in all three forms; constants
verified against the PG 18.3 headers (`dependency.h`, `namespace.h`,
`parsenodes.h`, `acl.h`, `pg_config_manual.h`, `pg_namespace.dat`,
`pg_authid.dat`) and, for build-generated syscache IDs, against the c2rust
rendering.

## Per-function table

| C function (line) | Port location | Verdict | Notes |
|---|---|---|---|
| spcachekey_hash (254) | — (derived `Hash` on `(String, Oid)`) | MATCH | fasthash replaced by std HashMap hashing; hash value is internal to the map, equality semantics (below) are what matter |
| spcachekey_equal (274) | — (derived `Eq` on `(String, Oid)`) | MATCH | roleid == && string == — identical predicate |
| spcache_init (306) | `spcache_init` | MATCH | reset condition `cache && valid && len < SPCACHE_RESET_THRESHOLD(256)` identical; context create/reset modeled by dropping/recreating the map; `search_path_cache_context_created` mirrors `SearchPathCacheContext != NULL` |
| spcache_lookup (344) | `spcache_lookup` | MATCH | LastSearchPathCacheEntry memo modeled by `last_cache_key`; returns presence (only caller tests presence) |
| spcache_insert (374) | `spcache_insert` | MATCH | entry().or_default() gives the C zero-init of non-key fields; memo updated |
| RangeVarGetRelid (namespace.h macro) | `RangeVarGetRelid` | MATCH | flags = missing_ok ? RVR_MISSING_OK : 0, no callback |
| RangeVarGetRelidExtended (441) | `RangeVarGetRelidExtended` | MATCH | retry loop, inval-counter checks, temp-persistence branch (incl. "temporary tables cannot specify a schema name" 42P16), NoLock break, retry unlock, NOWAIT/SKIP_LOCKED conditional-lock path (DEBUG1 vs ERROR; returns InvalidOid), final 42P01 errors — all identical |
| RangeVarGetCreationNamespace (654) | same name | MATCH | pg_temp alias, RELPERSISTENCE_TEMP, activeTempCreationPending, "no schema has been selected" 3F000 |
| RangeVarGetAndCheckCreationNamespace (739) | same name | MATCH | bootstrap break, ACL_CREATE check, retry lock/unlock choreography (namespace AccessShareLock, relation lockmode + ownercheck), persistence adjust + existing_relation_id out-param |
| RangeVarAdjustRelationPersistence (846) | same name | MATCH | three-way switch with identical 42P16 messages; PERMANENT→TEMP rewrite in temp namespace |
| RelnameGetRelid (885) | same name | MATCH | |
| RelationIsVisible (913) | same name | MATCH | |
| RelationIsVisibleExt (925) | same name | MATCH | is_missing out-param as Option<&mut bool>; quick PG_CATALOG check; slow conflict scan (no temp-namespace skip — matches C) |
| TypenameGetTypid (995) | same name | MATCH | |
| TypenameGetTypidExtended (1008) | same name | MATCH | temp_ok skip |
| TypeIsVisible (1040) / TypeIsVisibleExt (1052) | same names | MATCH | |
| FuncnameGetCandidates (1192) | same name | MATCH | pathpos loop, proallargtypes substitution under include_out_arguments (array-validity elog is the seam installer's marshaling duty, documented on `ProcRow::proallargtypes`), named/positional branches, variadic expansion, defaults, duplicate resolution (ordered fast path vs full scan, preference >0/<0/==0 with InvalidOid ambiguity marker), prepend-newest list order preserved via `insert(0, ..)` |
| MatchNamedCall (1585) | same name | MATCH | proargnames-null → seam returns None → false; positional prefix; named scan with argmode filter (FUNC_PARAM_IN/INOUT/VARIADIC = 'i'/'b'/'v' verified vs pg_proc.h), pp counted only for considered params; arggiven collision fail; defaults fill with first_arg_with_default cutoff |
| FunctionIsVisible (1696) / FunctionIsVisibleExt (1708) | same names | MATCH | recursive FuncnameGetCandidates check with memcmp over pronargs |
| OpernameGetOprid (1785) | same name | MATCH | exact-schema syscache probe; catlist3 walk per search-path entry skipping temp |
| OpernameGetCandidates (1888) | same name | MATCH | oprkind filter, pathpos, duplicate replace-in-place (pathpos/oid rewrite) — port keeps Vec node and rewrites fields, same as C; resultSpace bulk-alloc optimization dropped (allocation strategy only) |
| OperatorIsVisible (2049) / OperatorIsVisibleExt (2061) | same names | MATCH | |
| OpclassnameGetOpcid (2121) | same name | MATCH | temp skip |
| OpclassIsVisible (2154) / OpclassIsVisibleExt (2166) | same names | MATCH | |
| OpfamilynameGetOpfid (2223) | same name | MATCH | |
| OpfamilyIsVisible (2256) / OpfamilyIsVisibleExt (2268) | same names | MATCH | |
| lookup_collation (2322) | `lookup_collation` | MATCH | exact-encoding probe then any-encoding (-1) row; COLLPROVIDER_ICU='i' gate via is_encoding_supported_by_icu |
| CollationGetCollid (2373) | same name | MATCH | |
| CollationIsVisible (2407) / CollationIsVisibleExt (2419) | same names | MATCH | |
| ConversionGetConid (2477) | same name | MATCH | |
| ConversionIsVisible (2509) / ConversionIsVisibleExt (2521) | same names | MATCH | |
| get_statistics_object_oid (2575) | same name | MATCH | 42704 "statistics object ... does not exist" |
| StatisticsObjIsVisible (2632) / Ext (2644) | same names | MATCH | slow scan skips temp namespace (matches C; unlike relation/type scans) |
| get_ts_parser_oid (2719) / TSParserIsVisible (2777) / Ext (2789) | same names | MATCH | |
| get_ts_dict_oid (2864) / TSDictionaryIsVisible (2922) / Ext (2934) | same names | MATCH | |
| get_ts_template_oid (3010) / TSTemplateIsVisible (3068) / Ext (3080) | same names | MATCH | |
| get_ts_config_oid (3155) / TSConfigIsVisible (3213) / Ext (3225) | same names | MATCH | |
| DeconstructQualifiedName (3304) | same name | MATCH | 1/2/3-part switch; cross-database 0A000; too-many-dotted 42601; out-params → tuple return |
| LookupNamespaceNoError (3358) | same name | MATCH | pg_temp alias invokes namespace search hook |
| LookupExplicitNamespace (3388) | same name | MATCH | pg_temp fallthrough when no temp ns; ACL_USAGE + aclcheck_error + search hook |
| LookupCreationNamespace (3431) | same name | MATCH | pg_temp → AccessTempTableNamespace(false); ACL_CREATE |
| CheckSetNamespace (3462) | same name | MATCH | both 0A000 messages |
| QualifiedNameGetCreationNamespace (3490) | same name | MATCH | objname out-param → tuple |
| get_namespace_oid (3538) | same name | MATCH | 3F000 "schema ... does not exist" |
| makeRangeVarFromNameList (3557) | same name | MATCH | 42601 "improper relation name" |
| NameListToString (3597) | same name | MATCH | A_Star → '*' (`None` in owned NameList); unexpected-node elog unrepresentable by type |
| NameListToQuotedString (3631) | same name | MATCH | quote_identifier via ruleutils seam |
| isTempNamespace (3652) | same name | MATCH | |
| isTempToastNamespace (3664) | same name | MATCH | |
| isTempOrTempToastNamespace (3676) | same name | MATCH | |
| isAnyTempNamespace (3690) | same name | MATCH | prefix tests "pg_temp_"/"pg_toast_temp_" |
| isOtherTempNamespace (3713) | same name | MATCH | |
| checkTempNamespaceStatus (3732) | same name | MATCH | INVALID_PROC_NUMBER(-1) / proc dead / other-db / not-owner → IDLE; else IN_USE |
| GetTempNamespaceProcNumber (3769) | same name | MATCH | atoi helper reproduces C atoi (whitespace, sign, digit prefix) |
| GetTempToastNamespace (3794) | same name | MATCH | Assert → debug_assert |
| GetTempNamespaceState (3808) | same name | MATCH | out-params → tuple |
| SetTempNamespaceState (3824) | same name | MATCH | asserts + invalidation flags |
| GetSearchPathMatcher (3855) | same name | MATCH | leading implicit-entry consumption with temp/catalog classification + assert; memory-context arg dropped (owned return) |
| CopySearchPathMatcher (3892) | same name | MATCH | Clone |
| SearchPathMatchesCurrentEnvironment (3914) | same name | MATCH | generation fast path; addTemp/addCatalog/creation/schemas/extra-tail checks; generation update on success |
| get_collation_oid (3974) | same name | MATCH | 42704 with GetDatabaseEncodingName |
| get_conversion_oid (4028) | same name | MATCH | |
| FindDefaultConversionProc (4083) | same name | MATCH | FindDefaultConversion via pg_conversion seam |
| preprocessNamespacePath (4110) | same name | MATCH | SplitIdentifierString failure → elog "invalid list syntax"; $user (AUTHOID rolname + ACL_USAGE), pg_temp (temp_missing only if oidlist empty), normal refs; temp_missing out-param → tuple |
| finalNamespacePath (4201) | same name | MATCH | dedupe + InvokeNamespaceSearchHook(false); firstNS; implicit pg_catalog then temp prepended |
| cachedNamespacePath (4246) | same name | MATCH | NIL-oidlist recompute, finalPath recompute when empty/hook-present/forceRecompute, forceRecompute latched to hook presence; returns snapshot copy instead of pointer-into-cache |
| recomputeNamespacePath (4302) | same name | MATCH | validity fast path (baseSearchPathValid && namespaceUser == roleid); pathChanged comparison incl. `equal(finalPath, baseSearchPath)`; active = base snapshot; generation bump only on change |
| AccessTempTableNamespace (4365) | same name | MATCH | MyXactFlags \|= ACCESSEDTEMPNAMESPACE via xact seam; force semantics |
| InitTempTableNamespace (4393) | same name | MATCH | ACL_CREATE_TEMP check (42501 nonstandard message), RecoveryInProgress (25006), IsParallelWorker (25006), pg_temp_N / pg_toast_temp_N create-or-clean, CCI after creates, state + MyProc->tempNamespaceId publication (no failure point between local-state and MyProc writes, so the slight reordering vs C is unobservable) |
| AtEOXact_Namespace (4515) | same name | MATCH | subID-set && !parallel gate; commit → before_shmem_exit(RemoveTempRelationsCallback); abort → forget ns + invalidate + MyProc reset; subID cleared either way |
| AtEOSubXact_Namespace (4561) | same name | MATCH | commit → parent subid; abort → full reset |
| RemoveTempRelations (4601) | same name | MATCH (after fix) | initially DIVERGES: port omitted `PERFORM_DELETION_SKIP_EXTENSIONS` (0x0010) from the performDeletion flags — fixed in this audit; flags now INTERNAL\|QUIETLY\|SKIP_ORIGINAL\|SKIP_EXTENSIONS = 0x1D, verified against dependency.h |
| RemoveTempRelationsCallback (4627) | same name | MATCH | AbortOutOfAnyTransaction / StartTransactionCommand / PushActiveSnapshot(GetTransactionSnapshot) / remove / Pop / Commit via xact+snapmgr seams |
| ResetTempTableNamespace (4647) | same name | MATCH | |
| check_search_path (4660) | same name | MATCH | use_cache = context-created; spcache lookup fast path; SplitIdentifierString failure → GUC_check_errdetail("List syntax is invalid.") + false; insert on success; GUC extra/source params dropped (unused for this hook's logic) |
| assign_search_path (4716) | same name | MATCH | baseSearchPathValid = false only; bootstrap assert |
| InitializeSearchPath (4739) | same name | MATCH | bootstrap branch (pg_catalog-only base path, generation bump); normal branch registers 4 syscache callbacks (NAMESPACEOID=38, AUTHOID=11, AUTHMEMROLEMEM=9, DATABASEOID=21 — verified vs c2rust/generated IDs) + invalidation flags |
| InvalidationCallback (4799) | same name | MATCH | |
| fetch_search_path (4822) | same name | MATCH | activeTempCreationPending → AccessTempTableNamespace(true) + recompute; implicit-prefix strip |
| fetch_search_path_array (4862) | same name | MATCH | temp skip; count beyond array length |
| pg_table_is_visible (4897) | same name | MATCH | shared `pg_is_visible_body`: missing → None (SQL NULL) |
| pg_type_is_visible (4911) | same name | MATCH | |
| pg_function_is_visible (4925) | same name | MATCH | |
| pg_operator_is_visible (4939) | same name | MATCH | |
| pg_opclass_is_visible (4953) | same name | MATCH | |
| pg_opfamily_is_visible (4967) | same name | MATCH | |
| pg_collation_is_visible (4981) | same name | MATCH | |
| pg_conversion_is_visible (4995) | same name | MATCH | |
| pg_statistics_obj_is_visible (5009) | same name | MATCH | |
| pg_ts_parser_is_visible (5023) | same name | MATCH | |
| pg_ts_dict_is_visible (5037) | same name | MATCH | |
| pg_ts_template_is_visible (5051) | same name | MATCH | |
| pg_ts_config_is_visible (5065) | same name | MATCH | |
| pg_my_temp_schema (5079) | same name | MATCH | |
| pg_is_other_temp_schema (5085) | same name | MATCH | |

110/110 C functions accounted for; no MISSING/PARTIAL; the single DIVERGES
(RemoveTempRelations flags) was fixed and re-audited from scratch against
dependency.h.

## Constants verified against headers

- `PERFORM_DELETION_*` = 0x0001/0x0002/0x0004/0x0008/0x0010/0x0020 (dependency.h) ✓
- `RVR_MISSING_OK/NOWAIT/SKIP_LOCKED` = 1<<0 / 1<<1 / 1<<2 (namespace.h) ✓
- `TEMP_NAMESPACE_NOT_TEMP/IDLE/IN_USE` = 0/1/2 (enum order, namespace.h) ✓
- `ACL_USAGE/CREATE/CREATE_TEMP` = 1<<8 / 1<<9 / 1<<10, full AclMode table ✓ (parsenodes.h)
- `ACLCHECK_OK/NO_PRIV/NOT_OWNER` = 0/1/2 (acl.h) ✓
- `FUNC_MAX_ARGS` = 100 (pg_config_manual.h) ✓
- `PG_CATALOG_NAMESPACE` = 11, `PG_TOAST_NAMESPACE` = 99 (pg_namespace.dat) ✓
- `BOOTSTRAP_SUPERUSERID` = 10 (pg_authid.dat) ✓
- `NAMESPACE_RELATION_ID` = 2615, `RELATION_RELATION_ID` = 1259, `DATABASE_RELATION_ID` = 1262 ✓
- `NAMESPACEOID`=38, `AUTHOID`=11, `AUTHMEMROLEMEM`=9, `DATABASEOID`=21 (vs c2rust generated syscache IDs) ✓
- `FUNC_PARAM_IN/INOUT/VARIADIC` = 'i'/'b'/'v', `COLLPROVIDER_ICU` = 'i' ✓
- `OBJECT_SCHEMA` = 36 (vs c2rust), `DROP_CASCADE` = 1 (enum order) ✓
- `INVALID_PROC_NUMBER` = -1, `SPCACHE_RESET_THRESHOLD` = 256 ✓
- Error codes: 0A000, 42P16, 42P01, 55P03 (LOCK_NOT_AVAILABLE), 3F000, 42601,
  42704, 42501, 25006 — each mapped to the matching `ERRCODE_*` constant ✓

## Seam audit

Outward seams (all justified: their owner units are above or beside this one
in the dependency graph and are not yet ported / would cycle):
syscache, lsyscache, aclchk, objectaccess, objectaddress, dbcommands,
pg-namespace (NamespaceCreate), pg-conversion (FindDefaultConversion),
dependency (performDeletion), lmgr, proc (MyProc->tempNamespaceId), sinval
(SharedInvalidMessageCounter), inval, procarray (ProcNumberGetProc fields),
xact, parallel, xlog (RecoveryInProgress), miscinit, guc
(GUC_check_errdetail), varlena (SplitIdentifierString), ruleutils
(quote_identifier), mbutils, funcapi (get_func_arg_info), snapmgr, ipc
(before_shmem_exit), globals (MyDatabaseId/MyProcNumber).

- Every seam crate inspected: declaration-only (`seam_core::seam!` items plus
  doc-comments and, in dependency-seams, the `PERFORM_DELETION_*` constants
  which belong to the owner's header vocabulary). No branching, node
  construction, or computation in any seam.
- Marshaling contracts that carry C call-site checks are documented on the
  type (`ProcRow::proallargtypes` 1-D/no-null/OIDOID validation;
  `FuncArgInfo` None = proargnames null) — pure tuple-decoding duties of the
  installing owner, not displaced logic.
- Inward seams (`backend-catalog-namespace-seams`): `get_namespace_oid`,
  `range_var_get_relid`, `lookup_explicit_namespace`. All three installed by
  `init_seams()`, which contains only `set()` calls.
  `seams-init::init_all()` calls `backend_catalog_namespace::init_seams()`. ✓
- No `set()` of this unit's seams anywhere outside the owning crate. ✓
- No function body replaced by a seam call: all namespace.c logic lives in
  this crate.

## Notable owned-model adaptations (verified behavior-preserving)

- Per-backend statics in a `thread_local!` `NamespaceState`;
  `activeSearchPath` (a pointer alias of `baseSearchPath` in C) is a clone
  snapshot refreshed on every `recomputeNamespacePath()` — observationally
  identical since C only mutates base under the same recompute.
- Search-path cache: simplehash → `HashMap<(String, Oid), Entry>` with the
  `LastSearchPathCacheEntry` memo as `last_cache_key`; `NIL` double-meaning
  (never-computed vs legitimately-empty) preserved by `Vec::is_empty`.
- `FuncCandidateList` linked list → `Vec` with `insert(0, ..)` preserving the
  newest-first C order, so `resultList[0]` corresponds to the C head in the
  ordered-catlist duplicate fast path.
- `atoi` reimplemented with C semantics for the temp-namespace suffix parse.

## Verdict

**PASS** (after 1 fix round). All 110 functions MATCH (seam-delegated calls
conform to the thin-marshal rule); seam wiring clean. Fixed during audit:
`RemoveTempRelations` missing `PERFORM_DELETION_SKIP_EXTENSIONS` flag.
