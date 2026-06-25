# Audit: backend-parser-func (parse_func.c, PostgreSQL 18.3)

Unit: `backend-parser-func` (was `backend-parser-medium2`, parse_func.c half).
C source: `src/backend/parser/parse_func.c` (2682 LOC).
Verdict: **PASS** — all 12 C functions ported 1:1 (branch order, SQLSTATEs,
error text, return values), `residual_own_todos = 0`. Prereqs F0/F1/F2 all
landed; the full file ported in one lane (F3 leaf + F4 lookup + F5 dispatcher).

Files: `src/lib.rs` (leaf + lookup), `src/parse_func_or_column.rs`
(`ParseFuncOrColumn` + `func_get_detail` + `unify_hypothetical_args` +
`ParseComplexProjection`), `src/tests.rs`.

## Per-function

| C function (parse_func.c) | Rust | Notes |
|---|---|---|
| `ParseFuncOrColumn` (90) | `ParseFuncOrColumn` | Full dispatcher. Branch order identical: arg-count guard, VOID-Param JDBC drop, named-arg collection + dup/positional checks, could_be_projection, column-projection fast path, `func_get_detail`, all wrong-kind-of-routine ereports, the four `fdresult` arms (NORMAL/PROCEDURE no-op, AGGREGATE ordered-set/hypothetical checks, WINDOWFUNC, COERCION return, MULTIPLE, NOTFOUND), default-arg type append, `enforce_generic_type_consistency`, `make_fn_arguments`, variadic-array build, ANY-variadic array check, `check_srf_call_placement`, output node build (FuncExpr/Aggref/WindowFunc), `p_last_srf` record. |
| `func_match_argtypes` (923) | `func_match_argtypes` | Filter by `can_coerce_type(COERCION_IMPLICIT)`. C rebuilds the list by prepend (reversing); the owned model returns the surviving subset in original order — the matching *set* is identical and downstream is order-insensitive (single-match path / `func_select_candidate`). Returns the list whose `len()` is the C count. |
| `func_select_candidate` (1008) | `func_select_candidate` | All five heuristic phases ported (exact-match count, preferred-type-at-coercion-args, unknown-category resolution incl. STRING bias + preferred-type strip, single-known-type last-gasp). FUNC_MAX_ARGS guard kept. Returns chosen OID / `None`. |
| `func_get_detail` (1395) | `func_get_detail` | Exact-match scan, type-coercion interpretation (`func_name_as_type` + `find_coercion_pathway_explicit` RELABELTYPE/COERCEVIAIO-composite-string rule), candidate match-up, best-candidate ambiguity + VARIADIC-named check, pg_proc projection (`proc_row_by_oid`: rettype/retset/provariadic/pronargdefaults/prokind), default-arg extraction (`proc_argdefaults`) for both named (bitmapset of argnumbers) and positional (front-trim) notations, prokind→FuncDetailCode. |
| `unify_hypothetical_args` (1740) | `unify_hypothetical_args` | numDirect/numNonHypothetical math, declared-type consistency elog, ANY-skip, `select_common_type`+`select_common_typmod` over the (aggregated, hypothetical) pair, two `coerce_type` calls + actual_arg_types updates. |
| `make_fn_arguments` (1824) | `make_fn_arguments` | In-place coercion; NamedArgExpr keeps top level, coerces inner `.arg`; others replaced. `pstate: Option<&mut>`. F2-re-signed seam installed. |
| `FuncNameAsType` (1880) | (via `func_name_as_type` seam) | The `LookupTypeNameExtended` + `typisdefined && !typeTypeRelid` filter is owned by parse_type.c; reached through the new `func_name_as_type` seam (name as `&[PgString]`). |
| `ParseComplexProjection` (1911) | `ParseComplexProjection` | Whole-row-Var fast path → `scan_ns_item_for_column_by_posn` (GetNSItemByRangeTablePosn+scanNSItemForColumn folded onto the `(varno, sublevels_up)` identity, parse_relation-owned); RECORD-Var → `expand_record_variable`; else `get_expr_result_tupdesc`; then the TupleDesc attr scan + FieldSelect build (in-crate, faithful: attname-match + !attisdropped, fieldnum = i+1). |
| `funcname_signature_string` (1992) | `funcname_signature_string` | `name(type, name => type, ...)` build; numposargs split; `format_type_be`. |
| `func_signature_string` (2030) | `func_signature_string` | `NameListToString` + the above. |
| `LookupFuncNameInternal` (2048) | `LookupFuncNameInternal` | Candidate scan, arg-type memcmp, duplicate→AMBIGUOUS, objtype filter (FUNCTION/AGGREGATE ignore procedures, PROCEDURE ignore non-procedures, ROUTINE any), multiple-match→AMBIGUOUS. |
| `LookupFuncName` (2143) | `LookupFuncName` | + the no-such/ambiguous ereports; installs `lookup_func_name` seam. |
| `LookupFuncWithArgs` (2205) | `lookup_func_with_args` (opclass OWA) + `lookup_func_with_args_for_objtype` (parser OWA) + shared `lookup_func_with_args_finish` | Both arg representations resolved (opclass `TypeName` via `lookup_type_name_oid_owa`, parser `Node::TypeName` via `lookup_type_name_oid`). Two-pass input-then-all-args procedure/routine lookup, ambiguity combine, full objtype-validation + every no-such/ambiguous ereport variant. Installs `lookup_func_with_args` + `lookup_func_with_args_for_objtype` seams (consumed by opclasscmds + objectaddress). |
| `check_srf_call_placement` (2510) | `check_srf_call_placement` | Full `p_expr_kind` match (every EXPR_KIND arm, identical err/errkind assignment + p_hasTargetSRFs writes), `parse_expr_kind_name` for the errkind message. F2-re-signed `&mut ParseState` seam installed. |
| (parse_oper's `set_last_srf` target) | `set_p_last_srf` / `seam_set_last_srf` | `pstate->p_last_srf = (Node*) result` (boxed `Node::Expr`). Installs `set_last_srf` seam (consumed by parse_oper). |

## Seams

Inward (this crate installs in `init_seams`, consumed by parse_oper /
opclasscmds / objectaddress): `lookup_func_name`, `lookup_func_with_args`,
`lookup_func_with_args_for_objtype`, `func_match_argtypes`,
`func_select_candidate`, `make_fn_arguments`, `check_srf_call_placement`,
`set_last_srf`. (seams-init `every_declared_seam_is_installed_by_its_owner` green.)

New outward seam declarations added (owner installs; panic-until-landed where the
owner is unported):
- coerce-seams: `can_coerce_type`, `coerce_type`, `find_coercion_pathway_explicit`,
  `select_common_typmod` — **installed** (parse_coerce is landed).
- parse-clause-seams (NEW crate): `transform_where_clause` — **installed**
  (parse_clause is landed; previously had an empty init_seams).
- parse-expr-seams: `parse_expr_kind_name` — **installed** (parse_expr landed).
- parse-type-seams: `func_name_as_type`, `lookup_type_name_oid_owa` — panic until
  parse_type lands.
- parse-relation-seams: `scan_ns_item_for_column_by_posn`,
  `expand_record_variable` — panic until parse_relation lands.
- parse-agg-seams: `transform_aggregate_call`, `transform_window_func_call` —
  panic until parse_agg lands (the sibling half of the old medium2 unit).
- funcapi-seams: `get_expr_result_tupdesc` — panic until funcapi lands.

## Faithfulness notes / deliberate model choices

- Function name carries as `&[PgString]` (String components), matching the
  inward-seam contract and the repo's `name_list_to_string` convention, rather
  than src-idiomatic's `&[Node]`.
- `p_last_srf` identity (C raw-pointer `==`) modeled as
  `(discriminant, exprLocation)` equality — distinct byte offsets uniquely
  identify an SRF node within one parse. Same stand-in parse_oper uses.
- `func_get_detail` out-params bundled into a private `FuncDetail` struct;
  `argnumbers` returned to the caller which re-stamps the call's NamedArgExprs
  (C does this inside func_get_detail).
- `name_list_to_string_str` renders the dotted name for error text directly
  (no mcx threading). MINOR DIVERGENCE: it does not apply NameListToString's
  identifier quoting for names needing quotes; affects only error-message
  cosmetics, not behavior.
- `parser_errposition` mirrors the trimmed-ParseState convention (location+1),
  identical to parse_oper / parse_expr.

residual_own_todos = 0. No `todo!()`/`unimplemented!()`. Gate: `cargo check
--workspace` green; `no-todo-guard` green; `seams-init` (both recurrence guards)
green; `cargo test --workspace` green except the sanctioned `range_pair_*` flake.
