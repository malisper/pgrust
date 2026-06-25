# Audit — backend-utils-fmgr-funcapi

- Unit: `backend-utils-fmgr-funcapi` (`src/backend/utils/fmgr/funcapi.c`)
- Branch: `port/backend-utils-fmgr-funcapi-assemble` (assembled from
  `decomp/backend-utils-fmgr-funcapi-scaffold` + the five family bodies:
  srf_support, result_type, polymorphic, proc_info, tupledesc)
- Date: 2026-06-13
- Model: Claude Fable 5
- Verdict: **PASS**

Audit re-derived from the C (`postgres-18.3/src/backend/utils/fmgr/funcapi.c`)
and the c2rust rendering (`c2rust-runs/backend-utils-fmgr-funcapi/src/funcapi.rs`),
independent of the port's own comments.

## Function inventory (every definition in funcapi.c)

| # | C function | C line | Port location | Verdict | Notes |
|---|------------|-------|---------------|---------|-------|
| 1 | `InitMaterializedSRF` | 76 | srf_support.rs:26 | SEAMED* | body blocked on trimmed shapes (see SRF note) |
| 2 | `init_MultiFuncCall` | 133 | srf_support.rs:131 | SEAMED* | blocked on `fcinfo->flinfo` (fn_extra/fn_mcxt) + `rsi->econtext` |
| 3 | `per_MultiFuncCall` | 208 | srf_support.rs:182 | SEAMED* | blocked on `fcinfo->flinfo->fn_extra` |
| 4 | `end_MultiFuncCall` | 220 | srf_support.rs:202 | SEAMED* | blocked on `flinfo` + `rsi->econtext` |
| 5 | `shutdown_MultiFuncCall` | 238 | srf_support.rs:230 | SEAMED* | blocked on `flinfo->fn_extra` + `funcctx->multi_call_memory_ctx` |
| 6 | `get_call_result_type` | 276 | result_type.rs:80 | MATCH | `fn_oid`/`fn_expr` read via fmgr seam (`fn_oid_and_expr`); funnels to `internal_get_result_type` |
| 7 | `get_expr_result_type` | 299 | result_type.rs:102 | SEAMED | expr-node IsA dispatch (FuncExpr/OpExpr/RowExpr/Const/generic) owned by nodeFuncs; routed via `get_expr_result_type_node` which calls back into `internal_get_result_type` |
| 8 | `get_func_result_type` | 410 | result_type.rs:127 | MATCH | `internal_get_result_type(fid, NULL, NULL)` |
| 9 | `internal_get_result_type` | 430 | result_type.rs:140 | MATCH | OUT-param path, polymorphic rettype, `get_type_func_class` switch, RECORD-from-rsinfo all present; OIDs verified |
| 10 | `get_expr_result_tupdesc` | 551 | result_type.rs:272 | MATCH | composite/composite-domain return; non-composite error pair (RECORDOID vs not) |
| 11 | `resolve_anyelement_from_others` | 589 | polymorphic.rs:65 | MATCH | anyarray/anyrange/anymultirange branches + error msgs |
| 12 | `resolve_anyarray_from_others` | 655 | polymorphic.rs:135 | MATCH | resolve anyelement first, then get_array_type |
| 13 | `resolve_anyrange_from_others` | 681 | polymorphic.rs:166 | MATCH | from anymultirange only |
| 14 | `resolve_anymultirange_from_others` | 710 | polymorphic.rs:194 | MATCH | from anyrange only |
| 15 | `resolve_polymorphic_tupdesc` | 744 | polymorphic.rs:219 | MATCH | quick-out scan, per-arg actuals, deduce-from-others, collation block, per-att replace — 1:1 incl. range/multirange no-collation arms |
| 16 | `resolve_polymorphic_argtypes` | 1064 | polymorphic.rs:555 | MATCH | two-pass; inargno advance only for non-OUT/TABLE modes |
| 17 | `get_type_func_class` | 1328 | polymorphic.rs:762 | MATCH | get_typtype switch; domain base recursion; RECORD/VOID/CSTRING pseudo arms; TYPTYPE chars verified vs pg_type.h |
| 18 | `get_func_arg_info` | 1379 | proc_info.rs:72 | MATCH | proallargtypes vs proargtypes, names, modes; 1-D/elemtype/null shape checks |
| 19 | `get_func_trftypes` | 1475 | proc_info.rs:179 | MATCH | protrftypes 1-D Oid array |
| 20 | `get_func_input_arg_names` | 1522 | proc_info.rs:212 | MATCH | NULL proargnames guard, mode filter, empty-name -> None |
| 21 | `get_func_result_name` | 1607 | proc_info.rs:291 | MATCH | null-attr guards, scan for single named OUT arg, >1 -> NULL |
| 22 | `build_function_result_tupdesc_t` | 1705 | proc_info.rs:366 | MATCH | RECORDOID + OUT-arg gates; delegates to `_d` |
| 23 | `build_function_result_tupdesc_d` | 1751 | proc_info.rs:409 | MATCH | shape checks, OUT-arg gather, `column%d` naming, `numoutargs < 2 && !PROCEDURE` gate, CreateTemplateTupleDesc/InitEntry |
| 24 | `RelationNameGetTupleDesc` | 1870 | tupledesc.rs:37 | MATCH | name-list -> RangeVar -> relation_openrv -> CreateTupleDescCopy -> close |
| 25 | `TypeGetTupleDesc` | 1903 | tupledesc.rs:78 | MATCH | composite (alias rename, anon RECORD), scalar (1-col, alias required), RECORD/CompositeDomain/Other error arms |
| 26 | `extract_variadic_args` | 2005 | tupledesc.rs:195 | MATCH | variadic-array deconstruct vs trailing-scalar gather; unknown->text convert; NULL-array -> None (C `return -1`) |

\* SEAMED for items 1–5: see the SRF note below.

## SRF-plumbing functions (items 1–5) — blocked on upstream trimmed shapes

The five SRF functions panic loudly with documented messages. They are **not**
delegating their own logic to another crate (which would be MISSING); they are
blocked because the data structures they operate on are trimmed by their owning
units (`types-nodes`, per docs/types.md rule 3) and do not yet carry the fields
the C bodies read:

- `FunctionCallInfoBaseData` (types-nodes/src/fmgr.rs) carries only `resultinfo`;
  `flinfo`, `args`, `isnull`, `nargs` are documented as "the fmgr port widens it."
- `ReturnSetInfo` carries no `econtext`.
- `FuncCallContext` carries no `multi_call_memory_ctx`.

`InitMaterializedSRF` needs `rsinfo->econtext->ecxt_per_query_memory` and
`fcinfo->flinfo->fn_expr`; the `*_MultiFuncCall` family needs
`flinfo->fn_extra`/`fn_mcxt`, `rsi->econtext`, and
`funcctx->multi_call_memory_ctx`. None are reachable. Mirroring PG and panicking
at this boundary (rather than restructuring around the missing fields or
inventing opacity) is the sanctioned pattern; the bodies land when the fmgr /
ExprContext owners widen those shapes. Logic that *is* expressible — the
`ReturnSetInfo` NULL/allowed-modes sanity checks at the top of
`InitMaterializedSRF` and `init_MultiFuncCall` — is present and matches the C.

Constants verified against C headers / c2rust: RECORDOID=2249, ANYELEMENT=2283,
ANYARRAY=2277, ANYNONARRAY=2776, ANYENUM=3500, ANYRANGE=3831, ANYMULTIRANGE=4537,
ANYCOMPATIBLE=5077, ANYCOMPATIBLEARRAY=5078, ANYCOMPATIBLENONARRAY=5079,
ANYCOMPATIBLERANGE=5080, ANYCOMPATIBLEMULTIRANGE=4538, OIDOID=26, TEXTOID=25,
UNKNOWNOID=705, CSTRINGOID=2275, VOIDOID=2278, CHAROID=18; TYPTYPE_* = b/c/d/e/m/p/r;
PROARGMODE_* = i/o/b/v/t; PROKIND_PROCEDURE=p. All MATCH.

## Seams and wiring

Owned inward seam crate: `backend-utils-fmgr-funcapi-seams` — 5 declarations
(`InitMaterializedSRF`, `materialized_srf_putvalues`, `get_func_arg_info`,
`srf_arg0_oid`, `cstring_get_text_datum`). All 5 are installed by
`init_seams()` (lib.rs:51), which contains only `set()` calls. No uninstalled
owned seams.

Outward neighbor seams (all justified by real unported deps; thin marshal +
delegate, no business logic in the seam paths):
- fmgr frame reads (`backend-utils-fmgr-fmgr-seams`): `fn_oid_and_expr`,
  `get_fn_expr_variadic/_argtype/_arg_stable`, `pg_nargs/argisnull/getarg_*`.
- nodeFuncs (`backend-nodes-nodeFuncs-seams`): `get_expr_result_type_node`,
  `expr_type`, `get_call_expr_argtype_node`, `expr_input_collation_node`.
- syscache (`backend-utils-cache-syscache-seams`): `lookup_proc_result_info`,
  `proc_arg_attrs`.
- typcache (`backend-utils-cache-typcache-seams`): `assign_record_type_typmod`,
  `lookup_rowtype_tupdesc(_copy)`.
- lsyscache (`backend-utils-cache-lsyscache-seams`): `get_base_type`,
  `get_element_type`, `get_array_type`, `get_range_subtype`,
  `get_multirange_range`, `get_range_multirange`, `get_typcollation`,
  `get_typtype`, `get_typlenbyvalalign`.
- format-type / functioncmds: `format_type_be(_owned)`.
- arrayfuncs / toastdesc / relation / tupdesc: array projection,
  CreateTemplateTupleDesc/InitEntry/Copy, relation_openrv.

`backend-access-common-tupdesc` (`CreateTemplateTupleDesc` / `TupleDescInitEntry`
/ `TupleDescInitEntryCollation`) is a real ported neighbor and is called
directly — correct, no seam.

## Gate

- `cargo check --workspace`: clean (only pre-existing warnings in unrelated
  `backend-access-common-printtup`).
- `cargo test --workspace`: no failures.
- No `todo!()`/`unimplemented!()` in own logic.

## Verdict

**PASS.** All funcapi business logic (result-type resolution, polymorphic
resolution, pg_proc projection, descriptor builders, VARIADIC unpacking) is
complete and faithful. The five SRF-plumbing functions panic at the boundary of
upstream-owned trimmed shapes (Mirror-PG-and-panic), the sanctioned pattern for
unported data-shape dependencies; their expressible sanity-check logic is
present. All owned seams installed; outward seams are thin and justified.
