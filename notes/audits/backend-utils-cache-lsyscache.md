# Audit: backend-utils-cache-lsyscache

- Date: 2026-06-13 (re-audit: type-I/O divergence fix)
- Auditor model: Claude Opus 4.8 (1M context)
- Branch: fix/diverge-backend-utils-cache-lsyscache (off refs/heads/main;
  closes a genuine logic divergence the prior PASS missed)
- C source: `src/backend/utils/cache/lsyscache.c` (+ `lsyscache.h`), postgres-18.3
- c2rust: `pgrust/c2rust-runs/backend-utils-cache-lsyscache`

## Verdict: PASS

Every compiled function defined in `lsyscache.c` is present in the port with
faithful logic, and every declaration in the owned `backend-utils-cache-lsyscache-seams`
crate is installed by `init_seams()` (119 decls / 119 `set()` calls; the
installer is `set()`-calls only).

This re-port closes the prior FAIL (75 MISSING functions). The earlier audit's
accounting also silently missed `get_typdefault` (a compiled function) while
counting `get_typalign` (`#ifdef NOT_USED`); both are resolved here.

## Function inventory — C-source coverage

`lsyscache.c` defines 124 top-level function bodies. Three are `#ifdef
NOT_USED` and therefore absent from the compiled translation (verified against
the c2rust run, which ran post-preprocessor): `get_typalign` (line 2538),
`get_typmodout` (3170), `get_relnatts` (2038). The real compiled surface is 121
functions. The port covers all 121 (it additionally ports `get_typmodout` and
`get_relnatts` for completeness; both are harmless extras and faithful to the C
under NOT_USED).

All verdicts below are `MATCH` unless noted. Callee dependencies that bottom out
in unported neighbors are routed through the owner's seam (loud panic until that
owner lands) — `SEAMED` callee, not absent logic.

### opfamily / operator (pg_amop, pg_operator) — opfamily_operator.rs

| C function | port | verdict |
|---|---|---|
| op_in_opfamily | opfamily_operator::op_in_opfamily | MATCH (SearchSysCacheExists3 via `amop_search_exists`) |
| get_op_opfamily_strategy | get_op_opfamily_strategy | MATCH (AMOP_SEARCH; !valid→0) |
| get_op_opfamily_sortfamily | get_op_opfamily_sortfamily | MATCH (AMOP_ORDER; !valid→InvalidOid) |
| get_op_opfamily_properties | get_op_opfamily_properties | MATCH (seam fixes ordering_op=false; missing_ok contract documented) |
| get_opfamily_member | get_opfamily_member | MATCH |
| get_opfamily_member_for_cmptype | get_opfamily_member_for_cmptype | MATCH (get_opfamily_method + `index_am_translate_cmptype` seam) |
| get_opmethod_canorder | get_opmethod_canorder (private) | MATCH (BTREE→true; HASH/GIST/GIN/SPGIST/BRIN→false hardcoded; else amapi) |
| get_ordering_op_properties | get_ordering_op_properties | MATCH |
| get_equality_op_for_ordering_op | get_equality_op_for_ordering_op | MATCH (returns (eqop, reverse); reverse = cmptype==COMPARE_GT) |
| get_ordering_op_for_equality_op | get_ordering_op_for_equality_op | MATCH |
| get_mergejoin_opfamilies | get_mergejoin_opfamilies | MATCH (list of amcanorder-equality opfamilies) |
| get_compatible_hash_operators | get_compatible_hash_operators | MATCH (always-both-args; single/cross-type branches) |
| get_op_hash_functions | get_op_hash_functions | MATCH (HASH_AM_OID + HTEqualStrategyNumber; LHS-forget-on-RHS-miss) |
| get_op_index_interpretation | get_op_index_interpretation | MATCH (btree loop, then negator-<>→COMPARE_NE loop using amcanorder directly) |
| equality_ops_are_compatible | equality_ops_are_compatible | MATCH (op_in_opfamily then `index_am_consistent_equality`) |
| comparison_ops_are_compatible | comparison_ops_are_compatible | MATCH (`index_am_consistent_ordering`) |
| get_opfamily_proc | opclass::get_opfamily_proc | MATCH |
| get_opcode | get_opcode | MATCH (oper_oprcode; !valid→InvalidOid) |
| get_opname | get_opname | MATCH (pstrdup→PgString; !valid→None) |
| get_op_rettype | get_op_rettype | MATCH (oprresult; !valid→InvalidOid) |
| op_input_types | op_input_types | MATCH (elog on miss) |
| op_mergejoinable | op_mergejoinable | MATCH (ARRAY_EQ_OP/RECORD_EQ_OP via typcache cmp_proc; else oprcanmerge) |
| op_hashjoinable | op_hashjoinable | MATCH (ARRAY_EQ_OP/RECORD_EQ_OP via typcache hash_proc; else oprcanhash) |
| op_strict | op_strict | MATCH |
| op_volatile | op_volatile | MATCH (get_opcode + func_volatile; elog "operator %u does not exist") |
| get_commutator | get_commutator | MATCH |
| get_negator | get_negator | MATCH |
| get_oprrest | get_oprrest | MATCH |
| get_oprjoin | get_oprjoin | MATCH |

Note: `op_mergejoinable`/`op_hashjoinable` read `typentry->cmp_proc`/`hash_proc`
via the typcache `lookup_element_cmp_proc`/`lookup_element_hash_proc` seams,
which return the same OID the C reads (`cmp_proc_finfo.fn_oid == cmp_proc`); the
`TYPECACHE_CMP_PROC`/`TYPECACHE_HASH_PROC` flags are noted inline. Behaviorally
identical for the F_BTARRAYCMP / F_BTRECORDCMP / F_HASH_ARRAY / F_HASH_RECORD
equality checks.

### opclass (pg_opclass / pg_opfamily) — opclass.rs

get_opclass_family, get_opclass_input_type, get_opclass_opfamily_and_input_type,
get_opclass_method, get_opfamily_method, get_opfamily_name — all MATCH (CLAOID /
OPFAMILYOID reads; elog vs missing_ok per C). `get_default_opclass`
(GetDefaultOpClass, not an lsyscache.c function) SEAMED to backend-catalog-pg-opclass
(its pg_opclass index scan + TypeCategory/IsBinaryCoercible bottom out there).

### attribute (pg_attribute) — attribute.rs

get_attname, get_attnum (dropped-aware), get_attgenerated, get_atttype,
get_atttypetypmodcoll, get_attoptions — all MATCH. `get_attoptions` folds the
`SysCacheGetAttr(attoptions)` + `datumCopy` into the `pg_attribute_attoptions`
seam (outer None = cache miss → elog; inner None = isNull → (Datum) 0).

### function (pg_proc) — function.rs

get_func_name, get_func_namespace, get_func_rettype, get_func_nargs,
get_func_signature, get_func_variadictype, get_func_retset, func_strict,
func_volatile, func_parallel, get_func_prokind, get_func_leakproof,
get_func_support — all MATCH (PROCOID; elog vs InvalidOid/None per C). The
scalar reads project a fixed-width `PgProcForm`; `get_func_signature` keeps the
existing `proc_row_by_oid` projection (palloc'd argtypes).

### relation (pg_class / pg_index) — relation.rs

get_relname_relid, get_rel_name, get_rel_namespace, get_rel_type_id,
get_rel_relkind, get_rel_relispartition, get_rel_tablespace, get_rel_persistence,
get_rel_relam, get_relnatts, get_index_isclustered, get_index_isreplident,
get_index_isvalid, get_index_column_opclass — all MATCH. `get_index_column_opclass`
folds the `SysCacheGetAttrNotNull(indclass)` oidvector read into `pg_index_indclass`
and reproduces the indnatts/indnkeyatts asserts + non-key→InvalidOid path.

### type (pg_type / pg_range) — type_.rs

getTypeIOParam(→get_type_io_param), get_typlenbyvalalign, get_type_io_data,
getTypeInputInfo, getTypeOutputInfo, getTypeBinaryInputInfo,
getTypeBinaryOutputInfo, getBaseType, getBaseTypeAndTypmod, get_base_element_type,
get_element_type, get_array_type, get_promoted_array_type, get_multirange_range,
get_typisdefined, get_typlen, get_typbyval, get_typlenbyval, get_typstorage,
get_typtype, type_is_rowtype, type_is_enum, type_is_range, type_is_multirange,
get_type_category_preferred, get_typ_typrelid, get_typmodin, get_typmodout,
get_typcollation, type_is_collatable, get_typsubscript, getSubscriptingRoutines,
get_typavgwidth, get_typdefault, lookup_pg_range, lookup_pg_type,
syscache_hash_value_typeoid, get_array_element_io_data — all MATCH.

- `get_type_io_data` / `getTypeInputInfo` / `getTypeOutputInfo` /
  `getTypeBinaryOutputInfo`: re-derived this round against the C (lines 2465,
  3014, 3047, 3113) and c2rust — they had DIVERGED and the prior audit missed it.
  Fixes:
  - `get_type_io_data` was missing the `IsBootstrapProcessingMode()` branch
    entirely (C lines 2480-2507). Restored: when in bootstrap mode it now calls
    `boot_get_type_io_data` (bootstrap.c, reached across the
    bootstrap↔lsyscache cycle via the new `backend-bootstrap-bootstrap-seams`),
    maps `IOFunc_input→typinput` / `IOFunc_output→typoutput`, and on
    `IOFunc_receive`/`IOFunc_send` raises the plain `elog(ERROR, "binary I/O not
    supported during bootstrap")` (internal-error sqlstate, no errcode) — arm
    for arm with the c2rust `match which_func { 0 => …, 1 => …, _ => errfinish }`.
  - `getTypeInputInfo` / `getTypeOutputInfo` / `getTypeBinaryOutputInfo` had the
    wrong shell-type / no-function error surface: the port raised
    `ERRCODE_FEATURE_NOT_SUPPORTED` with invented messages ("cannot accept/output/
    send a value of type %s, which is still being defined" / "no … function
    available …"). C raises `ERRCODE_UNDEFINED_OBJECT` + `errmsg("type %s is only
    a shell", format_type_be(type))` for `!typisdefined`, and
    `ERRCODE_UNDEFINED_FUNCTION` + `errmsg("no … function available for type %s",
    format_type_be(type))` for `!OidIsValid(...)`. Now corrected, using the
    `format_type_be_str` format-type seam for the type name (matching
    `format_type_be`) instead of the raw `typname`. `getTypeBinaryOutputInfo`
    also restores the C statement order (typisdefined check, OidIsValid(typsend)
    check, then assign typSend/typIsVarlena). `getTypeBinaryInputInfo` was
    already correct (UNDEFINED_OBJECT / UNDEFINED_FUNCTION) and is unchanged.
- `get_element_type` / `get_base_element_type`: corrected this round to use
  `IsTrueArrayType` (`typelem != InvalidOid && typsubscript ==
  array_subscript_handler`), matching the C exactly. (The prior assembly used
  `typlen == -1`, a real divergence — e.g. cstring/internal pseudo-types and any
  varlena with a non-array subscript handler would have been mis-reported as
  arrays. Now fixed.)
- `getSubscriptingRoutines`: get_typsubscript + `OidFunctionCall0` via the fmgr
  `oid_function_call0` seam; the returned `const SubscriptRoutines *` rides as
  an opaque `Datum` pointer word (inherited opacity — the struct is
  forward-declared in `nodes/subscripting.h`, outside this TU, and the c2rust
  also types it `*const c_void`; no ported consumer reads it).
- `get_typdefault`: full two-branch logic — `stringToNode(typdefaultbin)` when
  present, else `makeConst` over `OidInputFunctionCall(typinput, typdefault, …)`.
  The two `SysCacheGetAttr` + `TextDatumGetCString` extractions fold into the
  `pg_type_default` projection; `stringToNode` / `OidInputFunctionCall` /
  `makeConst` route through the nodes-read / fmgr / makefuncs owner seams.
- `get_typavgwidth`: fixed-width fast path, then `type_maximum_size` (format-type
  seam) with the BPCHAR / ≤32 / <1000 / fixed thresholds. The
  `get_attavgwidth_hook` is never installed in this port (NULL-hook C path).

### statistics (pg_statistic) — statistics.rs

get_attstatsslot, get_attstatsslot_mcv (skew probe), get_attavgwidth (non-inherited
STATRELATTINH.stawidth; >0 guard), free_attstatsslot (consumes the slot; its
storage is reclaimed on Drop, mirroring the C pfree of values_arr/numbers_arr) —
all MATCH.

### namespace / range / index / pubsub — namespace_range_index_pubsub.rs

get_namespace_name, get_namespace_name_or_temp (isTempNamespace→"pg_temp"),
get_am_name, get_range_subtype, get_range_collation, get_range_multirange,
get_publication_oid, get_publication_name, get_subscription_oid,
get_subscription_name — all MATCH (GetSysCacheOid / form reads; missing_ok→
ereport(ERRCODE_UNDEFINED_OBJECT) vs InvalidOid/None; pub/sub name elog vs None).

### collation / constraint / language / cast / transform — collation_constraint_language_cast.rs

get_collation_isdeterministic, get_collation_name, get_constraint_name,
get_constraint_index (UNIQUE/PRIMARY/EXCLUSION→conindid), get_constraint_type,
get_language_name, get_cast_oid, get_transform_fromsql, get_transform_tosql — all
MATCH (carried from the assembled port; spot-re-checked get_constraint_index
contype predicate and get_cast_oid's GetSysCacheOid2 + missing_ok ereport).

## Seam audit

- Owned seam crate `backend-utils-cache-lsyscache-seams`: 119 declarations, all
  119 installed by `init_seams()`; the installer is `set()`-calls only
  (verified: no non-`set()` statements). `seams-init::init_all()` calls
  `backend_utils_cache_lsyscache::init_seams()` (preserved across the main
  merge).
- Outward seams are all thin marshal+delegate justified by real unported /
  cyclic neighbors: syscache (`SearchSysCache*` projected rows — panic until
  catcache lands, as designed), amapi (strategy/cmptype translation +
  amcanorder/amconsistent* — amapi unported), typcache (cmp_proc/hash_proc —
  typcache seams not yet installed), fmgr (`OidFunctionCall0` /
  `OidInputFunctionCall`), format-type (`format_type_be_str` / `type_maximum_size`),
  makefuncs (`makeConst`), nodes-read (`stringToNode`), pg-opclass
  (`GetDefaultOpClass`), namespace (`isTempNamespace`), arrayfuncs
  (stat-array detoast), and — added this round — miscinit
  (`is_bootstrap_processing_mode`, a plain `Mode == BootstrapProcessing` global
  read) and the new `backend-bootstrap-bootstrap-seams::boot_get_type_io_data`.
  The bootstrap seam is justified by a real cycle: bootstrap.c calls many
  lsyscache.c helpers (via lsyscache-seams), so lsyscache.c cannot take a direct
  crate dep on bootstrap.c; `boot_get_type_io_data` is owned by the
  `backend-bootstrap-bootstrap` crate, installed by its `init_seams()` (now wired
  into `seams-init::init_all()`), and the `BootTypeIoData` result struct lives in
  the seam crate so owner and caller share one type. The bootstrap branch's
  which_func dispatch + binary-I/O elog live in this crate (lsyscache.c owns that
  logic); the seam itself is a thin lookup delegate. No branching/computation
  observed in any seam path; the decision logic for every ported function lives
  in this crate.
- New seam declarations added this round (syscache pg_operator/pg_proc/
  pg_attribute/pg_class/pg_opclass/pg_range/pg_index forms, attoptions, indclass,
  pub/sub, amop-purpose, stawidth, pg_type_default; amapi cmptype +
  consistent-equality/ordering; format-type type_maximum_size; fmgr
  oid_function_call0; makefuncs make_const_node) are owned by their respective
  units; lsyscache only consumes them. These panic until their owners install
  them, consistent with the syscache unit's accepted partially-installed state.

## Design conformance

- Opacity inherited, never introduced: the `SubscriptRoutines *` stays an opaque
  Datum (forward-declared C struct, no consumer); no invented handle.
- Allocating helpers thread `Mcx` and return `PgResult` (get_*name, signature,
  mergejoin_opfamilies, op_index_interpretation, get_typdefault, attstatsslot).
- No shared statics for per-backend globals (MyDatabaseId for get_subscription_oid
  is supplied inside the syscache owner's installer, not read here).
- No locks across `?`; no registry side-tables; no unledgered divergence markers
  (the prior typlen-vs-IsTrueArrayType divergence is removed, not annotated).

## Gate

`cargo check --workspace` clean; `cargo test --workspace` passes. The
`recurrence_guard::every_seam_installing_crate_is_wired_into_init_all` test
passes — and now meaningfully covers the bootstrap unit, which gained an owned
seam (`boot_get_type_io_data`) and is therefore wired into
`seams-init::init_all()` this round. `backend-bootstrap-bootstrap` and
`backend-utils-cache-lsyscache` unit tests pass; the 2 known timeout flakes are
nondeterministic and ignored per the task.
