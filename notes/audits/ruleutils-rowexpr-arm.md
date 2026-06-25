# Audit: backend-utils-adt-ruleutils — get_rule_expr T_RowExpr arm

Scope: the RowExpr arm of get_rule_expr_e (expr_deparse.rs), newly ported
against ruleutils.c T_RowExpr (lines 9883-9942). FieldSelect (1017) and
FieldStore (942) arms re-derived and confirmed already-faithful (not changed).

| C location | Port location | Verdict | Notes |
|---|---|---|---|
| named-type probe `if row_typeid != RECORDOID: lookup_rowtype_tupdesc(...,-1)` | expr_deparse.rs RowExpr arm | MATCH | RECORDOID=2249 verified pg_type_d.h:223; typmod -1 both; Assert→debug_assert |
| `appendStringInfoString("ROW(")` always | str_(ctx,"ROW(") | MATCH | SQL99 omit-ROW not taken, same as C |
| arg loop w/ attisdropped skip, sep, get_rule_expr_toplevel | for e in args, is_none_or(!attisdropped) | MATCH | None(tupdesc)→include col (C tupdesc==NULL); i++ unconditional |
| whole-row Var special | get_rule_expr_toplevel_expr (Var→get_variable toplevel) | MATCH | |
| NULL-pad loop `while i<natts` + ReleaseTupleDesc | while i<td.natts; drop of PgBox | MATCH | ReleaseTupleDesc→Drop per seam copy semantics |
| `)` + `::%s` when row_format==COERCE_EXPLICIT_CAST | ch_(')'); format_type_with_typemod | MATCH | COERCE_EXPLICIT_CAST verified primnodes.h:754 |

Seam: lookup_rowtype_tupdesc — real dep cycle (typcache); thin marshal+delegate;
allocating+fallible (Mcx + PgResult) per rule; installed by typcache init_seams
(pre-existing). No new seam declared. No locks across ?, no shared statics, no
ambient-global seam.

VERDICT: PASS.
