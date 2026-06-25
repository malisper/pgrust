# Audit: backend-statistics-extended-stats — stxexprs expression-statistics build leg

Scope: the CREATE STATISTICS expression-statistics build leg added to
`backend-statistics-extended-stats` (`src/expr_stats.rs` + the build-loop /
`lookup_var_attr_stats` / `make_build_data` / `statext_store` /
`fetch_statentries_for_relation` changes in `src/lib.rs`), plus the
`examine_expression` seam owned/installed by `backend-commands-analyze`.

C source: `src/backend/statistics/extended_stats.c` (PG 18.3).

## Per-function table

| C function (extended_stats.c) | Port location | Verdict | Notes |
|---|---|---|---|
| `fetch_statentries_for_relation` (419) — stxexprs decode block (483-507) | lib.rs `fetch_statentries_for_relation` | MATCH | `TextDatumGetCString` (new `text_datum_get_cstring`, detoast + short/4B varlena) → `string_to_node` → `eval_const_expressions` (per-Expr seam; C `root=NULL` = boundParams only, not modeled) → `fix_opfuncids`. Decoded into the long-lived `mcx`, not the dropped scratch scan context. NULL stxexprs → empty Vec (C `exprs = NIL`). |
| `examine_attribute(Node*expr)` (525) — local | analyze `examine_expression` via the seam, called with `stattarget = -1` | MATCH | The local extended_stats.c `examine_attribute` is `examine_expression` with `attstattarget = -1` and `anl_context = NULL`. The seam sets attstattarget=-1 (passed) and anl_context=mcx (harmless for the build kernels, which read type fields only). attrtypid/typmod/collid from `exprType/Typmod/Collation` (`expr_type_info`). typanalyze/std_typanalyze dispatch + the `!ok || compute_stats==NULL || minrows<=0 → NULL` gate are shared with `examine_attribute`. |
| `examine_expression(Node*expr,int stattarget)` (604) | analyze `examine_expression` (crate body) + `expr_stats::examine_expression` thin wrapper | MATCH | Sets attstattarget=stattarget, attrtypid/typmod/collid from the expr tree, tupattnum=InvalidAttrNumber(0), default statyp* slots, std/custom typanalyze, same NULL gate. Owned by analyze.c (shares examine_attribute internals: new_vac_attr_stats, run_custom_typanalyze, std_typanalyze) — reached through a new inward seam. |
| `lookup_var_attr_stats` (690) | lib.rs `lookup_var_attr_stats` | MATCH | Columns matched by tupattnum (None on any miss → cannot build); then one `examine_attribute(expr)` (target -1) appended per expr. C asserts `stats[i] != NULL`; the port returns "cannot build" instead of UB on the should-not-happen NULL. tupDesc comes from new_vac_attr_stats (relation desc) == C `stats[i]->tupDesc = vacatts[0]->tupDesc`. |
| `statext_compute_stattarget` (343) | lib.rs `statext_compute_stattarget` (+ `_for` adaptor) | MATCH | `_for` scans only `stats[0..ncolumns]` (C passes `bms_num_members(columns)` as nattrs, NOT including exprs). |
| `make_build_data` (2448) | lib.rs `make_build_data` + `expr_stats::eval_exprs_into_build_data` | MATCH | attnums = column attnums (ascending) then -1,-2,… for exprs (C `k=-1; k--`). Columns via heap_getattr; exprs via ExecPrepareExprList + per-row ResetExprContext/ExecStoreHeapTuple/ExecEvalExpr, isnull→(0,true). Mirrors compute_index_stats' executor idiom (slot owned by estate). |
| `compute_expr_stats` (2087) | `expr_stats::compute_expr_stats` | MATCH | Per-expr EState/econtext/slot, ExecPrepareExpr, per-row reset+store+ExecEvalExprSwitchContext, isnull→(0,true) else datumCopy(typbyval,typlen). `tcnt>0`: get_attribute_options(rd_id, tupattnum=0)→None (C: non-existent attr ⇒ NULL), set exprvals/exprnulls/rowstride=1, call compute_stats, n_distinct override. C's per-expression expr_context reset between exprs is modeled by a fresh EState per expr (free_executor_state per iter). |
| `expr_fetch_func` (2228) | `expr_stats::expr_fetch_func` | MATCH | `i = rownum*rowstride; *isNull = exprnulls[i]; return exprvals[i]`. |
| `build_expr_data` (2246) | inlined in the build loop's EXPRESSIONS arm | MATCH | `examine_expression(expr, stattarget)` per expr → `exprvacstats`; the "should not happen, no exprs" guard maps to the `stat.exprs.is_empty()` elog. |
| `serialize_expr_stats` (2271) | `expr_stats::serialize_expr_stats` | MATCH | get_rel_type_id(StatisticRelationId) (Invalid→ERRCODE_WRONG_OBJECT_TYPE message); per valid stats build a pg_statistic row (starelid=InvalidOid, staattnum=InvalidAttrNumber, stainherit=false, nullfrac/width/distinct, 5×stakind/staop/stacoll, stanumbers float4[], stavalues typed[]); heap_form_tuple + heap_copy_tuple_as_datum → composite; invalid stats → NULL element (C accumArrayResult(0,true)). Final 1-D pg_statistic[] via construct_md_array_values (composite element: typlen -1, byval false, align 'd'). C's accumArrayResult/makeArrayResult build the identical flat array; up-front construct is behavior-identical (all elements known, only !stats_valid is NULL). |
| `statext_store` (759) | lib.rs `statext_store` | MATCH | Added the `exprs` (stxdexpr) leg: `exprs != (Datum)0` → store the prebuilt composite array, else NULL. ndistinct/dependencies/mcv legs unchanged; RemoveStatisticsDataById + heap_form_tuple + CatalogTupleInsert unchanged. |
| `BuildRelationExtStatistics` build loop (110-246) | lib.rs `build_relation_ext_statistics` | MATCH | `has_exprs` early-ERROR guard removed; EXPRESSIONS arm added (build_expr_data + compute_expr_stats + serialize_expr_stats); make_build_data now passed mcx+rel for the expr eval; stattarget uses columns.len(); statext_store gets expr_bytes. Progress-reporting (pgstat_progress_update_*) and the per-object cxt reset are model-omitted (no behavioral effect on the stored catalog). |
| `statext_expressions_load` (2398) | NOT this leg | OUT OF SCOPE | The estimation read-back (selfuncs.c consumer); a SEPARATE leg (`stats-ext-estimate`). Still panics with its own message; reached only at plan time after an equal() match against an EXPRESSIONS stat. Not part of the build leg. |

## Seam audit

New inward seam `examine_expression` declared in
`backend-commands-analyze-seams`, body in `backend-commands-analyze`
(`examine_expression`), installed in `backend-commands-analyze::seams_install::init_seams()`
alongside the existing `std_typanalyze`/`std_compute_stats`. The seam exists to
break the dependency cycle (extended-stats provides the analyze-rt build seams
analyze installs; it therefore cannot depend on analyze for the typanalyze
dispatch) — justified. The body is real logic in its owning crate (analyze.c
owns examine_attribute/examine_expression), not marshal-and-delegate-elsewhere.

Outward seam calls in expr_stats.rs (string_to_node, eval_const_expressions_expr,
fix_opfuncids, create_executor_state/get_per_tuple_expr_context/reset_expr_context/
exec_prepare_expr_list/exec_eval_expr_switch_context/free_executor_state,
exec_force_store_formed_heap_tuple, table_slot_create, get_rel_type_id,
get_attribute_options, construct_*_values, heap_*) are each thin: convert args,
one call, convert result — no branching/computation in the seam path. The
`decode_stxexprs` List-walk and the per-row eval loop are crate logic, not seam
logic.

## Design conformance

- All allocating helpers thread `Mcx` and return `PgResult` (decode_stxexprs,
  eval_exprs_into_build_data, compute_expr_stats, serialize_expr_stats, the
  examine_expression seam). No new shared statics, no ambient-global seams.
- No locks held across `?`. No registry-shaped side tables.
- No invented opacity; `Expr` is the existing flat enum (no lifetime), reused
  as in relcache-nodexform's `decode_node_text_to_exprs`.
- No `todo!`/`unimplemented!`/stub in the build leg; the only panic
  (`statext_expressions_load`) is a pre-existing unported *callee* in a
  different crate/leg, which the rules permit.

## Verdict: PASS (build leg)

Every build-leg function MATCH; one OUT-OF-SCOPE read-back (separate leg). Seam
audit clean. Verified at runtime: CREATE STATISTICS (ndistinct, dependencies,
mcv) ON (a+b),(a*2),b + ANALYZE stores all four columns including a 2-element
stxdexpr pg_statistic[] with correct stadistinct/stanullfrac/stawidth.
