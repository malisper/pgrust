# Audit — backend-parser-parse-oper

Unit `backend-parser-parse-oper` ports `parser/parse_oper.c` (PostgreSQL 18.3,
~1053 LOC). STEP 4a of the parser parse-analysis campaign (route to #159).

Re-derived independently from the C (`postgres-18.3/src/backend/parser/parse_oper.c`)
and the c2rust rendering. The crate operates over the repo's owned
`types_nodes::primnodes::Expr` expression tree; the C `Operator` (a
`SearchSysCache1(OPEROID)` `HeapTuple` released by `ReleaseSysCache`) is carried
by value as a decoded `ResolvedOper` row (the syscache seam's `oper_row_by_oid`
returns it), so `ReleaseSysCache` calls dissolve into value-drop and the
operator-returning functions return `Option<ResolvedOper>` instead of an opaque
tuple handle.

## State on `origin/main`

The crate already existed on `origin/main` (commits `83c89a878` "Port parser
parse_oper.c", `f530c9ce3` parsestate keystone phase 4, `1a60bb697` "Fix
make_scalar_array_op dropping ScalarArrayOpExpr.location") with CATALOG status
`ported`, but no audit file. This audit closes that gap. All 14 C functions are
present and the prompt's scope is a subset of what is ported.

## Gate (isolated `CARGO_TARGET_DIR=/tmp/parse-oper-target`)

- `cargo check --workspace` — clean (warnings only).
- `cargo test -p no-todo-guard` — pass.
- `cargo test -p seams-init` — pass (2 tests).
- `cargo test -p backend-parser-parse-oper` — pass (5 tests).

## Seam wiring

Inward seams owned + installed by `init_seams()` (`crates/.../lib.rs:1260`),
wired into `seams-init` (`crates/seams-init/src/lib.rs:171`):
`lookup_oper_name`, `lookup_oper_with_args`, `lookup_oper_with_args_node`
(consumed by opclasscmds + objectaddress). Outward (unported-owner) seams:
namespace (`opername_get_oprid`/`opername_get_candidates`/
`lookup_explicit_namespace` + direct `DeconstructQualifiedName`/
`fetch_search_path_array`), lsyscache (`get_base_type`/`get_array_type`/
`get_func_retset`), functioncmds (`get_base_element_type`), syscache
(`oper_row_by_oid`), typcache (`sort_group_operators`), parse_type
(`typename_type_id`/`lookup_type_name_oid`), parse_coerce
(`enforce_generic_type_consistency`/`is_binary_coercible`), parse_func
(`func_match_argtypes`/`func_select_candidate`/`make_fn_arguments`/
`check_srf_call_placement`/`set_last_srf`), format-type (`format_type_be_owned`).
`backend_nodes_core::nodefuncs::expr_type` is a ported sibling called directly.

## parse_oper.c

| C function | C loc | Port | Verdict | Notes |
|---|---|---|---|---|
| `LookupOperName` | 98 | `LookupOperName` | MATCH | `OpernameGetOprid` → return if valid; else if `!noError`: postfix-not-supported `ERRCODE_SYNTAX_ERROR` when `!OidIsValid(oprright)`, else `operator does not exist` `ERRCODE_UNDEFINED_FUNCTION` with `op_signature_string`; both carry `parser_errposition`. Else `InvalidOid`. |
| `LookupOperWithArgs` | 132 | `LookupOperWithArgs` | MATCH | `Assert(len==2)` → `debug_assert_eq`. `linitial_node`/`lsecond_node` TypeName via `type_name_arg`; NULL → `InvalidOid` else `LookupTypeNameOid(NULL, tn, noError)` (modeled `typename_type_id_opclass`: on Err → `InvalidOid` iff `no_error`, mirroring the C `noError` swallow). Then `LookupOperName(NULL, objname, …, -1)`. Operates over the opclass `ObjectWithArgs`/`TypeName`. |
| `LookupOperWithArgs` (node variant) | 132 | `LookupOperWithArgs_node` | MATCH | Same logic over raw-parser `types_parsenodes::ObjectWithArgs`; each `objargs` entry is a `Node::TypeName` (`node_type_name_arg` → `as_typename`), resolved via `lookup_type_name_oid::call(tn, missing_ok)`. This is the `get_object_address` `OBJECT_OPERATOR` arm. The raw-Node/Expr keystone forces the two argument shapes (opclass vs parsenodes `TypeName`); both are faithful to the single C function — the parsenodes variant resolves through the type seam (which itself errors/swallows per `missing_ok`), so no extra Err-swallow wrapper is needed. |
| `get_sort_group_operators` | 179 | `get_sort_group_operators` | MATCH | `cache_flags` choice modeled by `want_hashable` (= C non-NULL `isHashable` toggling `TYPECACHE_HASH_PROC`); the lt/eq/gt/hash lookup delegates to `typcache::sort_group_operators` (installed on merged typcache, since the trimmed `TypeCacheEntry` copy-out lacks `lt/eq/gt_opr`+`hash_proc`). Error gates 1:1: `(needLT && !lt) || (needGT && !gt)` → ordering-operator `ERRCODE_UNDEFINED_FUNCTION` + errhint; `needEQ && !eq` → equality-operator (no hint). Outputs returned as a struct (C's four out-pointers). |
| `oprid` | 238 | `oprid` | MATCH | `op.oid` (C `((Form_pg_operator) GETSTRUCT(op))->oid`). |
| `oprfuncid` | 245 | `oprfuncid` | MATCH | `op.oprcode`. |
| `binary_oper_exact` | 262 | `binary_oper_exact` | MATCH | UNKNOWN-substitution both directions setting `was_unknown`; `OpernameGetOprid`; if `was_unknown` and `getBaseType(arg1) != arg1`, retry on basetype/basetype. Returns `InvalidOid` on miss. |
| `oper_select_candidate` | 312 | `oper_select_candidate` | MATCH | `func_match_argtypes`; `ncandidates==0` → `(NotFound, InvalidOid)`; `==1` → `(Normal, candidates[0].oid)`; else `func_select_candidate` → `Some(oid)` → `(Normal, oid)` else `(Multiple, InvalidOid)`. C's `FuncCandidateList` single-best return is modeled as the seam's `Option<Oid>` (C reads `candidates->oid` from the one returned cell). |
| `oper` | 370 | `oper` | MATCH | Cache lookup via `make_oper_cache_key`/`find_oper_cache_entry` → `search_oper_row`, early-return on hit. Exact `binary_oper_exact`; else `OpernameGetCandidates(opname,'b',false)`, non-empty → InvalidOid arg-substitution (the "probably dead code" XXX), `oper_select_candidate(2, …)`. Final `search_oper_row` on valid oid; on hit + `key_ok` insert cache entry; else `!noError` → `op_error`. Returns `Option<ResolvedOper>`. |
| `compatible_oper` | 450 | `compatible_oper` | MATCH | `oper(noError)` → `None` returns `None`; else `IsBinaryCoercible(arg1,oprleft) && IsBinaryCoercible(arg2,oprright)` → keep; else `!noError` → `operator requires run-time type coercion` `ERRCODE_UNDEFINED_FUNCTION`+errposition; else `None`. C `ReleaseSysCache(optup)` is value-drop. |
| `compatible_oper_opid` | 487 | `compatible_oper_opid` | MATCH | `compatible_oper(NULL, …, -1)` → `oprid` or `InvalidOid`. |
| `left_oper` | 518 | `left_oper` | MATCH | Cache path as `oper`. Exact `OpernameGetOprid(op, InvalidOid, arg)`; else `OpernameGetCandidates(op,'l',false)`, non-empty → `clisti->args[0] = clisti->args[1]` scribble over the mutable candidate list, then `oper_select_candidate(1, &arg, …)` (run even for one candidate, per the C comment). Final search/cache/`op_error` identical to `oper`. |
| `op_signature_string` | 602 | `op_signature_string` | MATCH | `"%s "` of `format_type_be(arg1)` only when `OidIsValid(arg1)`, then `NameListToString(op)` (`.join(".")`), then `" %s"` of `format_type_be(arg2)` (unconditional — matches C). Owned `String` for the palloc'd buffer. |
| `op_error` | 622 | `op_error` | MATCH | `FUNCDETAIL_MULTIPLE` → `operator is not unique` `ERRCODE_AMBIGUOUS_FUNCTION`+errhint; else `operator does not exist` `ERRCODE_UNDEFINED_FUNCTION` with the singular-vs-plural errhint chosen by `!arg1 || !arg2` (singular "argument type"/"an explicit type cast" when either is invalid). Both carry errposition. |
| `make_op` | 660 | `make_op` | MATCH | Postfix reject (`rtree==None` → `ERRCODE_SYNTAX_ERROR`, no errposition, matching C). Prefix (`ltree==None`): `exprType(rtree)`, `left_oper(noError=false)`; binary: `exprType` both, `oper(noError=false)`. Shell check on `oprcode`. args/actual/declared/nargs split prefix vs binary 1:1. `enforce_generic_type_consistency(…, oprresult, false)`; `make_fn_arguments`; build `OpExpr{opno,opfuncid,opresulttype=rettype,opretset=get_func_retset(oprcode),opcollid/inputcollid=Invalid (parse_collate fills),args,location}`. If `opretset`: `check_srf_call_placement(last_srf,location)` then record `p_last_srf = result` via `set_last_srf` seam (trimmed ParseState has no field). `noError=false` ops can never return `None`, so the impossible `None` surfaces as `internal_error` (`elog`-style `ERRCODE_INTERNAL_ERROR`) rather than panic. |
| `make_scalar_array_op` | 770 | `make_scalar_array_op` | MATCH | `ltypeId=exprType(ltree)`, `atypeId=exprType(rtree)`; UNKNOWN → `rtypeId=UNKNOWN` else `get_base_element_type(atypeId)`, invalid → `op ANY/ALL (array) requires array on right side` `ERRCODE_WRONG_OBJECT_TYPE`. `oper(noError=false)`, shell check, args/types, `enforce_generic_type_consistency(…,2,oprresult,false)`. `rettype != BOOLOID` → yield-boolean error; `get_func_retset` → not-a-set error. Array-type switch-back: `IsPolymorphicType(declared[1])` → keep `atypeId`, else `get_array_type(declared[1])` invalid → `could not find array type` `ERRCODE_UNDEFINED_OBJECT`. `actual[1]=atypeId; declared[1]=res_atypeId`; `make_fn_arguments`. Build `ScalarArrayOpExpr{opno,opfuncid,hashfuncid=Invalid,negfuncid=Invalid,useOr,inputcollid=Invalid,args,location}` — **`location` set** (the `1a60bb697` fix; primnodes `ScalarArrayOpExpr` carries it). |
| `make_oper_cache_key` | 937 | `make_oper_cache_key` | MATCH | `DeconstructQualifiedName`; `MemSet(key,0)` → `OprCacheKey::default()` (zero-fill for stable key compare); `strlcpy(oprname,…,NAMEDATALEN)` → `strlcpy_namedata` (zero-filled `[u8;NAMEDATALEN]`, truncates at `NAMEDATALEN-1`); `left_arg`/`right_arg`. Qualified: `key.search_path[0]=LookupExplicitNamespace(schema,false)` (the setup/cancel errposition-callback pair is the seam's bracketed failure surface). Unqualified: `fetch_search_path_array(path, MAX_CACHED_PATH_LEN)`; `> MAX_CACHED_PATH_LEN` → `Ok(false)` (didn't fit), else copy count entries. `MAX_CACHED_PATH_LEN=16`, `NAMEDATALEN` from `types_core::fmgr`. |
| `find_oper_cache_entry` | 981 | `find_oper_cache_entry` | MATCH | Lazy init of the process-global `Mutex<Option<BTreeMap<OprCacheKey,Oid>>>` (C `static HTAB *OprCacheHash=NULL`; single-backend semantics). On miss → `InvalidOid`. The C `CacheRegisterSyscacheCallback(OPERNAMENSP/CASTSOURCETARGET, …)` registration is replaced by the host explicitly invoking `invalidate_oper_cache` on the relevant inval events (the inval-callback registry is not yet modeled for this per-backend cache; sanctioned — the callback *body* is fully ported). |
| `make_oper_cache_entry` | 1020 | `make_oper_cache_entry` | MATCH | `Assert(OprCacheHash!=NULL)` → `debug_assert`; `HASH_ENTER` insert/overwrite. |
| `InvalidateOprCacheCallBack` | 1036 | `invalidate_oper_cache` | MATCH | `Assert` + flush-all (C seq-scans and `HASH_REMOVE`s every entry; the BTreeMap `.clear()` is the faithful wholesale flush). Exposed `pub` for the host to drive on pg_operator/pg_cast inval. |

## Modeling notes (verified faithful, not divergences)

- `parser_errposition` is modeled inline as `errpos` over the trimmed
  `ParseState` (carries only `p_sourcetext`): `location < 0 || pstate.is_none()`
  → 0, else `location + 1`. This reproduces the 1-based cursor column for an
  ASCII source the same way `parse_node.c` maps the byte offset; the full
  multibyte byte→char mapping arrives with the parse_node owner.
- `Operator`/`HeapTuple`/`ReleaseSysCache` carried by value as `ResolvedOper`
  (decoded `pg_operator` row: oid/oprleft/oprright/oprresult/oprcode); every C
  `ReleaseSysCache` is a value-drop. No raw syscache tuple, no `GETSTRUCT`.
- `IsPolymorphicType` / `RegProcedureIsValid` ported as local predicates over
  the polymorphic-pseudo-type OID set (pg_type.h `IsPolymorphicTypeFamily1`) and
  `OidIsValid`.
- The single-candidate `func_select_candidate` C return (`FuncCandidateList`
  that is NULL or one cell) is the seam's `Option<Oid>` — `oper_select_candidate`
  reads exactly the one cell's `oid`, so no information is lost.

## Verdict: PASS

All 19 C functions (14 public/static + the cache quartet, counting the node
variant of LookupOperWithArgs) port 1:1 — control flow, SQLSTATEs
(`ERRCODE_SYNTAX_ERROR`/`UNDEFINED_FUNCTION`/`AMBIGUOUS_FUNCTION`/
`WRONG_OBJECT_TYPE`/`UNDEFINED_OBJECT`/`INTERNAL_ERROR`), error text, errhints,
errposition, and the `ScalarArrayOpExpr.location` fix all match. No
`todo!()`/`unimplemented!()`; `residual_own_todos = 0` (the only deferrals are
unported-owner seam calls, which panic loudly until the owner lands — sanctioned
mirror-PG-and-panic). Gate green.
