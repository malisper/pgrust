# Audit: backend-parser-parse-target

Unit: `backend-parser-parse-target` — C source `src/backend/parser/parse_target.c`
(PostgreSQL 18.3, 2042 lines). STEP 4b of the parser parse-analysis campaign
(route to #159). Audited independently from the C, the c2rust rendering
(`c2rust-runs/backend-parser-medium2/src/parse_target.rs`), and the Rust port.

## Function inventory and verdicts

C's parse_target.c defines 18 real functions (the leading `newNode`/`list_*`/
`TupleDescAttr`/`for_each_cell_setup` entries in the c2rust file are inlined
header macros, not part of this translation unit's source). Every one is
present and ported in-crate.

| C function | C loc | Port (lib.rs) | Verdict | Notes |
|---|---|---|---|---|
| transformTargetEntry | 74 | transformTargetEntry | MATCH | SetToDefault pass-through for EXPR_KIND_UPDATE_SOURCE; FigureColname when colname NULL && !resjunk; `p_next_resno++`; makeTargetEntry. transformExpr via parse-expr seam. |
| transformTargetList | 120 | transformTargetList | MATCH | Assert p_multiassign empty; expand_star = !UPDATE_SOURCE; ColumnRef/A_Indirection `llast == A_Star` → Expand*Star+list_concat+continue; else transformTargetEntry; multiassign tail appended (Assert UPDATE_SOURCE) and cleared. |
| transformExpressionList | 219 | transformExpressionList | MATCH | Same star checks (bare-expr/Expr output); allowDefault+SetToDefault → pass-through (raw→prim conv); else transformExpr. |
| resolveTargetListUnknowns | 287 | resolveTargetListUnknowns | MATCH | exprType==UNKNOWNOID → coerce_type(UNKNOWN→TEXT, COERCION_IMPLICIT, COERCE_IMPLICIT_CAST). |
| markTargetListOrigins | 317 | markTargetListOrigins | MATCH | per-tle markTargetListOrigin with `(Var*) tle->expr` (None when not a Var). |
| markTargetListOrigin | 342 | markTargetListOrigin | MATCH | All RTE kinds: RELATION sets resorigtbl/col; SUBQUERY/CTE copy-up with the same `elog(ERROR ... does not have attribute)` predicates; CTE extra_cols (search+1/cycle+2) skip range; JOIN/FUNCTION/VALUES/TABLEFUNC/NAMEDTUPLESTORE/RESULT unmarked; GROUP unreachable. |
| transformAssignedExpr | 454 | transformAssignedExpr | MATCH | p_expr_kind save/restore (incl. on every error path); attrno<=0 system-col error; attnumTypeId + atttypmod/attcollation; SetToDefault stamping + array/subfield DEFAULT rejection; indirection → NULL-const (INSERT) or Var (UPDATE, p_target_nsitem->p_rtindex) + transformAssignmentIndirection; else coerce_to_target_type + datatype-mismatch error at exprLocation(orig). |
| updateTargetListEntry | 621 | updateTargetListEntry | MATCH | transformAssignedExpr(EXPR_KIND_UPDATE_TARGET); resno=attrno; resname=colname. |
| transformAssignmentIndirection | 685 | transformAssignmentIndirection | MATCH | CaseTestExpr substitution iff (cell present && !basenode); A_Indices accumulate; A_Star error; field selection: getBaseTypeAndTypmod, typeidTypeRelid (non-composite err), get_attnum (undefined/system-col errs), get_atttypetypmodcoll, recurse, FieldStore (list_make1), coerce_to_domain if base≠target; trailing subscripts; base case coerce_to_target_type + subscripted/subfield mismatch errors. |
| transformAssignmentSubscripts | 905 | transformAssignmentSubscripts | MATCH | transformContainerType; transformContainerSubscripts(is_assignment=true); collation from base type if domain; recurse; set refassgnexpr/refrestype/reftypmod; domain coerce-up with CANNOT_COERCE error. |
| checkInsertTargets | 1017 | checkInsertTargets | MATCH | NIL → default non-dropped columns (attrno i+1); else attnameAttNum (undefined-col err), whole/partial dup detection via bms (DUPLICATE_COLUMN), attrnos list. |
| ExpandColumnRefStar | 1122 | ExpandColumnRefStar | MATCH | numnames==1 → ExpandAllTables (Assert make_target_entry); else pre/post columnref hooks, 2/3/4-name refnameNamespaceItem, catalog-name check (WrongDb), TooMany; post-hook ambiguous-column error; errorMissingRTE/WrongDb/TooMany; ExpandSingleTable. |
| ExpandAllTables | 1296 | ExpandAllTables | MATCH | p_namespace p_cols_visible items (Assert !p_lateral_only), expandNSItemAttrs concat; SELECT-* no-tables syntax error. |
| ExpandIndirectionStar | 1348 | ExpandIndirectionStar | MATCH | copyObject(ind), truncate trailing '*', transformExpr, ExpandRowReference. |
| ExpandSingleTable | 1374 | ExpandSingleTable | MATCH | make_target_entry → expandNSItemAttrs; else expandNSItemVars + RTE_RELATION ACL_SELECT (zero-col) + per-Var markVarForSelectPriv. |
| ExpandRowReference | 1426 | ExpandRowReference | MATCH | whole-row Var (varattno==Invalid) → GetNSItemByRangeTablePosn + ExpandSingleTable; RECORD Var → expandRecordVariable else get_expr_result_tupdesc; FieldSelects per non-dropped attr, TE or bare per make_target_entry. |
| expandRecordVariable | 1521 | expandRecordVariable | MATCH | whole-row: expandRTE + CreateTemplateTupleDesc + TupleDescInitEntry/Collation; per-RTE drill-down (SUBQUERY/JOIN/CTE recurse via fake pstate; the same elog errors), get_expr_result_tupdesc fallback. |
| FigureColname | 1712 | FigureColname | MATCH | FigureColnameInternal; "?column?" default. |
| FigureIndexColname | 1731 | FigureIndexColname | MATCH | FigureColnameInternal; NULL on no name. |
| FigureColnameInternal | 1751 | FigureColnameInternal | MATCH (see note) | All representable node kinds ported with identical name strings and strength returns: ColumnRef/A_Indirection (last String field), FuncCall (llast), A_Expr NULLIF, TypeCast (recurse then typeName llast, strength 1), CollateClause, GroupingFunc, SubLink (Exists/Array/Expr-single-target/operator-likes), CaseExpr (defresult recurse then "case"/1), A_ArrayExpr, RowExpr, CoalesceExpr, MinMaxExpr, SQLValueFunction (all 15 ops), XmlExpr (all ops). |

### FigureColnameInternal — unmodeled raw node kinds

The C `switch` also has arms for `T_MergeSupportFunc`, `T_XmlSerialize`, and the
SQL/JSON node family (`T_JsonParseExpr`, `T_JsonScalarExpr`,
`T_JsonSerializeExpr`, `T_JsonObjectConstructor`, `T_JsonArrayConstructor`,
`T_JsonArrayQueryConstructor`, `T_JsonObjectAgg`, `T_JsonArrayAgg`,
`T_JsonFuncExpr`). These raw-grammar node kinds are **not present in the
`types_nodes::nodes::Node` enum** in this repo (gram.y has not landed the
constructors), so they are not constructible as a `Node` value and cannot be
matched. A node of one of those kinds therefore falls through to the catch-all
(strength 0) — exactly the behaviour C exhibits for any node tag absent from the
parse tree. This is the standard "mirror PG; the unmodeled node simply isn't in
the enum" situation, not dropped logic: there is no reachable input that would
take those arms. When the SQL/JSON + MergeSupport raw nodes land in
`types-nodes`, these arms must be filled (noted for the future keystone).

## Seam / wiring audit

- **Owned inward seam:** `backend-parser-target-seams::transform_target_entry`
  (declared by a concurrent parse_clause lane that maps it to `parse_target.c`).
  This crate now installs it from `init_seams()` via a thin adapter
  (`transform_target_entry_seam`: clone the by-ref `node`, wrap the
  caller-supplied already-transformed `expr` as `Some`, call
  `transformTargetEntry`). `init_seams()` is wired into `seams-init::init_all()`.
  The `every_declared_seam_is_installed_by_its_owner` and
  `every_seam_installing_crate_is_wired_into_init_all` guards pass.
- **New outward seam added + installed:** `backend_parser_parse_expr_seams::transformExpr`
  (declared in parse-expr-seams, installed by `backend_parser_parse_expr::init_seams`
  via the existing `me::transformExpr::set(transformExpr)` line). This is the
  parse_expr leg called by parse_target; a direct dep would create the
  parse_target ⇆ parse_expr cycle (parse_expr already references parse_target in
  transformRowExpr/transformMultiAssignRef), so the seam is justified. It is a
  thin marshal+delegate (one `::call`, no logic). Guard confirms it is installed.
- **Direct (cycle-free) sibling/owner calls** — all marshal-free real calls, no
  logic in any seam path:
  - parse_relation: GetRTEByRangeTablePosn, GetNSItemByRangeTablePosn,
    GetCTEForRTE, refnameNamespaceItem, errorMissingRTE, expandNSItemAttrs,
    expandNSItemVars, expandRTE, get_tle_by_resno, attnameAttNum, attnumTypeId,
    markVarForSelectPriv, getRTEPermissionInfo
  - parse_coerce: coerce_type, coerce_to_target_type, coerce_to_domain
  - parse_type: typeidTypeRelid
  - parse_node (small1): transformContainerType, transformContainerSubscripts
  - makefuncs: make_target_entry, make_var, make_null_const
  - nodefuncs: expr_type, expr_typmod, expr_collation, expr_location
  - funcapi: get_expr_result_tupdesc
  - tupdesc: CreateTemplateTupleDesc, TupleDescInitEntry, TupleDescInitEntryCollation
  - format_type: format_type_be_owned
- **Justified outward seams** (real cycles): lsyscache (get_base_type_and_typmod,
  get_attnum, get_atttypetypmodcoll, get_typcollation); dbcommands
  (get_database_name); small1 (parser_errposition); init-small (my_database_id —
  the `MyDatabaseId` per-backend global, read via the existing capability seam
  exactly as backend-catalog-namespace does for the same `get_database_name`
  cross-database check; not a new ambient-global seam).

## Design conformance

- Allocating functions take `Mcx<'mcx>` and return `PgResult`; growth via
  `try_reserve` + infallible push, OOM via `mcx.oom`. No infallible-alloc path
  in an allocating step.
- No invented opacity: raw nodes are the real `types_nodes` structs; the
  raw-vs-typed `SetToDefault` split is handled by an explicit field-for-field
  conversion (the split pre-exists in types-nodes).
- `MyDatabaseId` reached as an explicit capability seam, not modeled as a
  shared static.
- No locks/held resources; no registries; no `todo!`/`unimplemented!`.
- The fake `ParseState` of expandRecordVariable's drill-down recursion (C
  borrows the live pstate as parent + overrides p_rtable) is built as an owned
  read-only clone of the walked ancestor spine (`p_rtable` + `parentParseState`
  only — the sole fields the recursion's GetRTEByRangeTablePosn/GetCTEForRTE
  read). Behaviour-identical; clone is the ownership-sound rendering of the C's
  borrowed alias.

## Verdict

**PASS.** Every function MATCH; FigureColnameInternal's unmodeled-node arms are
unreachable in the current node model (mirror-PG, not dropped logic). No seam
findings; the one new seam is justified, thin, and installed. Workspace
compiles; no-todo-guard and seams-init guards pass; 5 crate unit tests pass.
