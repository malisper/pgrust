# Audit — backend-parser-small1

Unit `backend-parser-small1` bundles `parser/parse_enr.c`, `parser/scansup.c`,
`parser/parse_node.c`, `parser/parse_param.c`, `parser/parse_merge.c`.

Scope: F1 (parse_enr + scansup + parse_node core), F2 (parse_param read-only
legs), and F3 (variable-parameter mutation — keystone now LANDED, see the F3
note). `parse_merge` remains sibling-blocked (explicit mirror-PG-and-panic).

Re-derived independently from the C (PostgreSQL 18.3) and the c2rust rendering.

## parse_enr.c

| C function | C loc | Port | Verdict | Notes |
|---|---|---|---|---|
| `name_matches_visible_ENR` | parse_enr.c:19 | `name_matches_visible_ENR` | MATCH | `get_visible_ENR_metadata(p_queryEnv, refname) != NULL` → `.is_some()`. |
| `get_visible_ENR` | parse_enr.c:25 | `get_visible_ENR` | MATCH | Returns the borrowed metadata tied to the parse state (C returns the aliasing pointer); shared helper reads `pstate.p_queryEnv` and delegates to the merged queryenvironment owner. |

## scansup.c

| C function | C loc | Port | Verdict | Notes |
|---|---|---|---|---|
| `downcase_truncate_identifier` | scansup.c:36 | `downcase_truncate_identifier` | MATCH | `downcase_identifier(ident, len, warn, true)`. |
| `downcase_identifier` | scansup.c:45 | `downcase_identifier` | MATCH | `palloc(len+1)` → fallible `PgVec` reserve; ASCII `A-Z` fold unconditional; high-bit fold gated on `enc_is_single_byte && IS_HIGHBIT_SET && isupper`, using `libc::isupper`/`libc::tolower` (the repo's `port-pgstrcasecmp::fold_to_lower` direct-libc convention, exactly the C's direct `isupper`/`tolower`). Truncation when `i >= NAMEDATALEN && truncate`. Returns the `char *` bytes (no trailing NUL; carrier supplies it). |
| `truncate_identifier` | scansup.c:92 | `truncate_identifier` | MATCH | `len >= NAMEDATALEN` → `pg_mbcliplen(ident, len, NAMEDATALEN-1)` (mbutils seam), optional `NOTICE`/`ERRCODE_NAME_TOO_LONG`, then clip. The seam contract (lexer consumer) and the in-crate entry share `truncate_to_clip`. |
| `scanner_isspace` | scansup.c:116 | `scanner_isspace` | MATCH | `' '`/`\t`/`\n`/`\r`/`\v`(0x0b)/`\f`(0x0c). |

`NAMEDATALEN = 64` verified vs `pg_config_manual.h`.

## parse_node.c

| C function | C loc | Port | Verdict | Notes |
|---|---|---|---|---|
| `make_parsestate` | parse_node.c:38 | `make_parsestate` | MATCH | `palloc0` image via `ParseState::new` (`p_next_resno=1`, `p_resolve_unknowns=true`); parent leg copies `p_sourcetext` (clone) + 5 hook fn-pointers (Copy). C aliases `p_queryEnv`/`p_ref_hook_state` pointers into the child — the owned model (unique `PgBox`) cannot share a pointer, and no in-repo caller passes a parent (all parent-passing callers live in unported analyze.c), so a parent carrying those two PgBox fields mirror-PG-and-panics (named follow-on) rather than silently dropping or deep-copying. Seam re-signed `(mcx, source_text)` → `(mcx, parent)` to be 1:1 with C (no `::call` consumers; EXPLAIN driver passes `None`). |
| `free_parsestate` | parse_node.c:71 | `free_parsestate` | MATCH | `p_next_resno - 1 > MaxTupleAttributeNumber` (=1664, verified) → `ERRCODE_TOO_MANY_COLUMNS`; `table_close(p_target_relation, NoLock)` via direct dep; `pfree` = value drop. |
| `parser_errposition` | parse_node.c:105 | `parser_errposition` (+ `parser_errposition_seam`) | MATCH | `location < 0` → 0; `pstate==NULL`/no source text → 0; else `pg_mbstrlen_with_len(p_sourcetext, location) + 1`. C returns through `errposition()`; owned model returns the cursor for `.errposition()`. Installed seam wraps it `Ok(...)` (the C is infallible; `PgResult` contract preserved for consumers define/cluster/explain-state). |
| `setup_parser_errposition_callback` | parse_node.c:139 | `setup_parser_errposition_callback` | MATCH (retired model) | `error_context_stack` is retired repo-wide (docs/query-lifecycle-raii.md): context attaches on propagation, no ambient callback chain to push. No-op image; location tagging happens at the fallible callee's seam. |
| `cancel_parser_errposition_callback` | parse_node.c:155 | `cancel_parser_errposition_callback` | MATCH (retired model) | No chain to pop. |
| `pcb_error_callback` | parse_node.c:169 | (folded into the above) | MATCH (retired model) | The `geterrcode() != ERRCODE_QUERY_CANCELED` callback has no counterpart — no callback fires in the propagation model. |
| `transformContainerType` | parse_node.c:188 | `transformContainerType` | MATCH | `getBaseTypeAndTypmod` (lsyscache seam) smashes domain; `INT2VECTOROID→INT2ARRAYOID`, `OIDVECTOROID→OIDARRAYOID` (OIDs 22→1005, 30→1028 verified vs `pg_type`). |
| `transformContainerSubscripts` | parse_node.c:242 | `transformContainerSubscripts` | MATCH | `!isAssignment` → smash; `getSubscriptingRoutines` (lsyscache seam, returns `(routines, typelem)`) NULL → `ERRCODE_DATATYPE_MISMATCH` "cannot subscript type %s …" with `parser_errposition(exprLocation(containerBase))`; `is_slice` scan over `A_Indices.is_slice`; build `SubscriptingRef` (refrestype=Invalid, refexpr=containerBase, refassgnexpr=None); `sbsroutines->transform(...)` reached through the `subscripting_transform` outward seam (per-type subscript handler unported → mirror-PG-and-panic); final `!OidIsValid(refrestype)` → second `ERRCODE_DATATYPE_MISMATCH`. The opaque routines Datum is discarded in favor of the separate transform seam (both legs unported and panic together; when the per-type handler lands it fetches routines + transforms in one owner). |
| `make_const` | parse_node.c:346 | `make_const` | MATCH | `isnull` → `makeConst(UNKNOWNOID,-1,Invalid,-2,0,true,false)`; `T_Integer`→INT4/4/byval `Int32GetDatum`; `T_Float`→`pg_strtoint64_safe` then int4-fits / int8 (`FLOAT8PASSBYVAL`) / numeric; `T_Boolean`→BOOL/1/byval; `T_String`→UNKNOWN/-2; `T_BitString`→BIT/-1; default→`elog(ERROR) "unrecognized node type"`. The numeric (oversize-float), string and bitstring arms call `DirectFunctionCall`(numeric_in/bit_in)/`CStringGetDatum` which yield a BY-REFERENCE Datum — that bridge is unported workspace-wide (canonical `Datum` has no pointer lane; same blocker as `makeConst`'s own by-ref panic), so those three arms mirror-PG-and-panic with the exact C line cited. The pure int/bool/null/int-fits arms are real. OIDs (INT4=23/INT8=20/BOOL=16/UNKNOWN=705/NUMERIC=1700/BIT=1560) verified. `con->location` not modeled (repo `Const` omits the field, deliberately). Takes `mcx` explicitly (C uses ambient context; `ParseState` carries no arena). |

## parse_param.c

| C function | C loc | Port | Verdict | Notes |
|---|---|---|---|---|
| `setup_parse_fixed_parameters` | parse_param.c:67 | `setup_parse_fixed_parameters` | MATCH | Returns the `FixedParamState{param_types}` carrier (C stores it in `p_ref_hook_state` + installs the hook; the owned hook fields take a bare `fn`, so the carrier is handed back for the caller to drive the hook). |
| `fixed_paramref_hook` | parse_param.c:99 | `fixed_paramref_hook` | MATCH | `paramno <= 0 || > numParams || !OidIsValid(paramTypes[paramno-1])` → `ERRCODE_UNDEFINED_PARAMETER` "there is no parameter $%d" with `parser_errposition(pref->location)`; builds `Param{PARAM_EXTERN, paramid, paramtype, -1, get_typcollation(paramtype)}`. `location` not modeled (repo `Param` omits it). |
| `setup_parse_variable_parameters` | parse_param.c:83 | `setup_parse_variable_parameters` | MATCH | Stores the `VarParamState` carrier in `pstate.p_ref_hook_state` (`ParseRefHookState::VarParams`); installs the var hooks. C's `Oid **paramTypes`/`int *numParams` two-level caller alias is modeled as one shared `Rc<RefCell<Vec<Oid>>>` (the `Vec`'s length is `*numParams`); the caller keeps a clone to read resolved types back. |
| `variable_paramref_hook` | parse_param.c:131 | `variable_paramref_hook` | MATCH | Range check `paramno <= 0 || > MaxAllocSize/sizeof(Oid)` → `ERRCODE_UNDEFINED_PARAMETER`; grows the shared array (`resize` zero-fills new slots = `palloc0`/`repalloc0_array`), sets `*numParams = paramno`; `InvalidOid` slot → `UNKNOWNOID`; the `VOIDOID && EXPR_KIND_CALL_ARGUMENT` → `UNKNOWNOID` JDBC hack; builds the `Param`. |
| `variable_coerce_param_hook` | parse_param.c:186 | `variable_coerce_param_hook` | MATCH | Only acts when `PARAM_EXTERN && paramtype==UNKNOWNOID`; range check → `UNDEFINED_PARAMETER`; resolves `UNKNOWNOID`→target, matches no-op, conflict → `ERRCODE_AMBIGUOUS_PARAMETER` "inconsistent types deduced" + `format_type_be` errdetail; mutates `param` in place (`paramtype`, `paramtypmod=-1`, `paramcollid`, leftmost `location`); returns the coerced `Param` (C `Node*`) or `None` (proceed normally). |
| `check_variable_parameters` | parse_param.c:268 | `check_variable_parameters` | MATCH | Reads the installed `VarParamState`; `numParams==0` (empty Vec) → no work; else `query_tree_walker(query, check_parameter_resolution_walker, pstate, 0)`. Walker errors captured into a `PgResult` slot (cte's fallible-walker idiom), returned. |
| `check_parameter_resolution_walker` | parse_param.c:286 | `check_parameter_resolution_walker` | MATCH | `IsA(Param) && PARAM_EXTERN`: range check → `UNDEFINED_PARAMETER`; `paramtype != paramTypes[paramno-1]` → `ERRCODE_AMBIGUOUS_PARAMETER` "could not determine data type of parameter $%d"; `IsA(Query)` → recurse `query_tree_walker`; else `expression_tree_walker`. |
| `query_contains_extern_params` | parse_param.c:330 | `query_contains_extern_params` | MATCH | `query_tree_walker(query, walker, NULL, 0)`. |
| `query_contains_extern_params_walker` | parse_param.c:338 | `query_contains_extern_params_walker` | MATCH | `node==NULL`→false (Option); `IsA(Param)` → `paramkind==PARAM_EXTERN`; `IsA(Query)` → recurse `query_tree_walker`; else `expression_tree_walker`. Walkers reused from `backend-nodes-core::node_walker` (the C `query_tree_walker`/`expression_tree_walker` over the `Node` universe). |

### F3 note (keystone LANDED)

The variable-parameter surface is now fully ported. C's `VarParamState { Oid
**paramTypes; int *numParams; }` — a two-level alias of the caller's mutable type
array + count, re-`palloc`'d in place so the caller (`PrepareQuery`) reads the
resolved types back after analysis — is modeled by
[`types_nodes::parsestmt::VarParamState`]: a single `Rc<RefCell<Vec<Oid>>>` the
caller constructs, hands to `setup_parse_variable_parameters` (which stores it in
`pstate.p_ref_hook_state` as `ParseRefHookState::VarParams`), and reads back
afterward. The `Vec`'s length is C's `*numParams` (the second-level `int *`
pointer collapses to the shared vector's own length); cloning the carrier clones
the `Rc` (same shared array), matching C's pointer aliasing exactly. `resize`
zero-fills new slots, matching `palloc0_array`/`repalloc0_array`. No invented
opacity, no handle — a real shared-ownership carrier. The walker's
`ereport(ERROR)` long-jump is modeled by capturing the error into a `PgResult`
slot and aborting the walk (the repo's established fallible-walker idiom, cf.
`backend-parser-cte`).

The `p_ref_hook_state` field changed additively from `Option<PgBox<Node>>` (an
unused opaque slot — nothing ever stored a `Node` in it) to the
`ParseRefHookState { None, VarParams(VarParamState) }` enum; `is_some()` and
`as_var_params()` accessors preserve existing call sites.

Wiring into `coerce_type`'s `p_coerce_param_hook` invocation (parse_coerce) and
the parser's `transformParamRef` dispatch remains a separate, already-flagged
keystone (the raw-`Node`↔`Expr` coercion-hook bridge); those call sites still
mirror-PG-and-panic in their own units. The parse_param.c functions themselves
are complete and tested here.

## parse_merge.c

| C function | C loc | Port | Verdict | Notes |
|---|---|---|---|---|
| `transformMergeStmt` | parse_merge.c:107 | `transformMergeStmt` | SEAMED→panic | Orchestrates `transformFromClause`/`setTargetTable`/`transformWhereClause`/`transformTargetList`/`transformExpr` + CTE machinery — every owner (parse_clause/parse_relation/parse_target/parse_expr full path/analyze) is unported. Explicit mirror-PG-and-panic; no consumers yet. |
| `setNamespaceForMergeWhen` | parse_merge.c:51 | (static helper of the above) | SEAMED→panic | Reached only from `transformMergeStmt`. |
| `setNamespaceVisibilityForRTE` | parse_merge.c:415 | (static helper of the above) | SEAMED→panic | Reached only from `transformMergeStmt`. |

## Seam audit

Owned seam crates (by C-source coverage):

- `backend-parser-small1-seams` (parse_node.c / parse_merge.c). Declares:
  - `parser_errposition` — INSTALLED by `init_seams()`
    (`parser_errposition_seam`). Consumers: define / cluster / explain-state.
  - `subscripting_transform` — OUTWARD seam (real owner = per-type subscript
    handlers, utils/adt, unported). Declared here because no owner crate exists;
    the guard's outward-exclusion (this crate `::call`s it) keeps it correct.
    Mirror-PG-and-panic until a subscript-support owner lands.
- `backend-parser-scansup-seams` (scansup.c). Declares `truncate_identifier` —
  INSTALLED by `init_seams()` (ownership reconciled here from the parser-driver,
  which only consumes it).
- `make_parsestate` lives in `backend-parser-analyze-seams` (a sibling unit's
  seam crate, but it is a parse_node.c function) — INSTALLED by this unit's
  `init_seams()`, which is correct: ownership is by C-source coverage, and
  parse_node.c is this unit's.

Outward dependency seams (real owners elsewhere, justified by cycle / unported):
`get_base_type_and_typmod`, `get_subscripting_routines`, `get_typcollation`
(lsyscache); `pg_database_encoding_max_length`, `pg_mbcliplen`,
`pg_mbstrlen_with_len` (mbutils). `format_type_be_owned` and `table_close` and
`pg_strtoint64_safe` and `make_const`(makefuncs) and the walkers are DIRECT calls
into merged owners (no cycle). All seam paths are thin marshal+delegate; no logic
in any seam closure.

`init_seams()` is `set()`-only; wired into `seams-init::init_all()`. Both
recurrence guards pass (`no-todo-guard`; `seams-init` `every_*` — confirming the
newly-installed `parser_errposition` + `make_parsestate` + `truncate_identifier`
and the outward-excluded `subscripting_transform`).

## Design conformance

- No invented opacity: ENR metadata returns the real borrowed type; the opaque
  `SubscriptRoutines *` stays the lsyscache bare-word Datum (inherited opacity).
- Allocating functions take `Mcx` and return `PgResult` (downcase/truncate/
  make_parsestate/make_const). OOM via `mcx.oom`.
- No shared statics / ambient-global seams. `error_context_stack` honored as the
  retired propagation model (not a getter).
- No locks held across `?`; `table_close` consumes the relation guard.
- No `todo!()`/`unimplemented!()`; unported callees are seam-and-panic / explicit
  mirror-PG-and-panic with the C line cited.

## Verdict: PASS

Every F1 + F2 + F3 function is `MATCH`. The variable-parameter (F3) keystone is
LANDED: the `VarParamState` shared-mutable type-array carrier is built in
`types-nodes` and the four var-param functions are real implementations (no
panic), tested. `parse_merge` is `SEAMED`→explicit mirror-PG-and-panic (unported
sibling parser layer) — sanctioned, with the C line cited. Zero seam findings.

## Parity-fix sweep (location + F3 stubs)

Following the F0 #219 `Const.location` field landing and the parity audit
(wf_02a4ccb2):

- `make_const` (parse_node.c:366, :477): both returned `Const`s now set
  `con.location = aconst.location` — the isnull branch and the final
  `makeConst(...)` result. `makeConst` (nodes-core) still hardwires
  `location = -1` exactly as the C `makeConst`; the parser sets the real
  location afterward, matching C.
- The four variable-parameter (F3) hooks are now REAL implementations (the
  `ParamHookState`/`VarParamState` carrier keystone was built — see the F3
  note): `setup_parse_variable_parameters` (parse_param.c:84),
  `variable_paramref_hook` (:131), `variable_coerce_param_hook` (:186),
  `check_parameter_resolution_walker` (:286), plus `check_variable_parameters`.
  Covered by `variable_paramref_grows_and_resolves` (grow + resolve +
  write-back + later-ref-sees-resolved-type), `variable_coerce_conflict_errors`
  (resolve, matched-re-resolve no-op, already-typed fall-through), and
  `check_variable_parameters_empty_is_ok`.
