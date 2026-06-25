# Audit — backend-parser-clause F3a (window-defs + on-conflict)

Scope: the F3a subset of `src/backend/parser/parse_clause.c`
(PostgreSQL 18.3), ported in `crates/backend-parser-clause/src/window_conflict.rs`.
Compared function-by-function against the C source at
`/Users/malisper/workspace/work/pgrust/postgres-18.3/src/backend/parser/parse_clause.c`
(src-idiomatic had these stubbed as `-> !`, so no idiomatic reference exists).

Result: **PASS**.

## Functions in scope

### transformWindowDefinitions (C:2764) — PORTED, faithful
- `winref++` per windowdef; duplicate-name check via `findWindowClause(result, name)`
  → ERRCODE_WINDOWING_ERROR "window %s is already defined" with windef->location. ✓
- refname lookup → ERRCODE_UNDEFINED_OBJECT "window %s does not exist" if absent. ✓
- PARTITION/ORDER transform: `transformSortClause(... EXPR_KIND_WINDOW_ORDER, true)`
  then `transformGroupClause(... groupingSets=NULL, orderClause, EXPR_KIND_WINDOW_PARTITION, true)`.
  The repo's `transformGroupClause` returns `(result, gsets)`; gsets discarded (C passes
  `groupingSets == NULL`). Order matches C (sort before group). ✓
- WindowClause construction: name/refname copied (copyObject string fields). ✓
- refwc rules (SQL:2008 7.11):
  - PARTITION override → error if partitionClause non-empty when refwc; else copyObject
    refwc->partitionClause. ✓
  - ORDER: error if both orderClause and refwc->orderClause; if orderClause use it
    (copiedOrder=false); else copyObject refwc->orderClause (copiedOrder=true). ✓
  - non-refwc path: orderClause used directly, copiedOrder=false; partitionClause used. ✓
  - frame-clause copy ban: refwc->frameOptions != FRAMEOPTION_DEFAULTS → the two-message
    branch (name||orderClause||windef->frameOptions!=DEFAULTS gets the plain message;
    else the "Omit the parentheses" hint message). Both messages identical text, second
    adds errhint — matches C exactly. ✓
- `wc->frameOptions = windef->frameOptions`. ✓
- RANGE-offset single-ORDER-BY block: gated on
  `(FRAMEOPTION_RANGE) && (START_OFFSET|END_OFFSET)`; list_length(orderClause)!=1 → error;
  `sortcl = linitial`, `sortkey = get_sortgroupclause_expr(sortcl, *targetlist)`,
  `get_ordering_op_properties(sortcl->sortop)` → rangeopfamily/rangeopcintype (None = the
  C `elog(ERROR, "operator %u is not a valid ordering operator")`). inRangeColl =
  exprCollation(sortkey); inRangeAsc = !reverse_sort; inRangeNullsFirst = nulls_first. ✓
  Note: C uses `get_ordering_op_properties(sortop, &opfamily, &opcintype, &cmptype)`; the
  cmptype output is unused by this caller (only opfamily/opcintype recorded), matching C.
- GROUPS mode requires ORDER BY → error if empty. ✓
- start/end offsets via transformFrameOffset; winref set; appended to result. ✓

### findWindowClause (C:3661) — PORTED, faithful
Returns the **index** of the first WindowClause whose `name` matches (the repo stores an
owned `Vec<WindowClause>`, so an index is the borrow-safe stand-in for the C `WindowClause *`).
`wc->name && strcmp == 0`. ✓

### transformFrameOffset (C:3688) — PORTED, faithful
- `*inRangeFunc = InvalidOid` default; NULL clause → (NULL, InvalidOid) quick exit. ✓
- ROWS: transformExpr EXPR_KIND_WINDOW_FRAME_ROWS, coerce_to_specific_type INT8OID "ROWS". ✓
- RANGE: transformExpr EXPR_KIND_WINDOW_FRAME_RANGE; nodeType=exprType; preferredType =
  (nodeType!=UNKNOWNOID)?nodeType:rangeopcintype. in_range fn search over
  `search_amproc_list2(rangeopfamily, rangeopcintype)` (= C SearchSysCacheList2(AMPROCNUM,
  opfamily, opcintype)); skip amprocnum != BTINRANGE_PROC (3); nfuncs++ per inrange row;
  can_coerce_type(1, &nodeType, &amprocrighttype, IMPLICIT) gate; nmatches++; preferred-match
  selection (`if selectedType != preferredType { selectedType=righttype; selectedFunc=amproc }`)
  — identical to C. Error trio: nfuncs==0 / nmatches==0 / (nmatches!=1 && selectedType!=preferredType),
  each with format_type_be(rangeopcintype[/nodeType]) and the exact errhints/SQLSTATE
  (ERRCODE_FEATURE_NOT_SUPPORTED). coerce_to_specific_type selectedType "RANGE";
  *inRangeFunc=selectedFunc. ✓
- GROUPS: transformExpr EXPR_KIND_WINDOW_FRAME_GROUPS, coerce_to_specific_type INT8 "GROUPS". ✓
- else: C `Assert(false); node=NULL` → here an internal elog_error (the grammar guarantees
  exactly one of ROWS/RANGE/GROUPS; an unreachable assert maps to a loud error). ✓
- checkExprIsVarFree(pstate, node, constructName) on the result. ✓
- Returns (Some(Node::Expr(node)), inRangeFunc); the C `Node *` is the typed expr wrapped.

### resolve_unique_index_expr (C:3200) — PORTED, faithful
- per IndexElem: ordering != SORTBY_DEFAULT → "ASC/DESC is not allowed in ON CONFLICT clause";
  nulls_ordering != SORTBY_NULLS_DEFAULT → "NULLS FIRST/LAST is not allowed ...", both
  ERRCODE_INVALID_COLUMN_REFERENCE at exprLocation((Node*)infer) (= infer->location, the
  raw-Node arm of exprLocation for an InferClause is its location). ✓
- !ielem->expr → synthesize ColumnRef{fields=list_make1(makeString(name)), location=infer->location};
  else use ielem->expr. ✓
- pInfer->expr = transformExpr(parse, EXPR_KIND_INDEX_EXPRESSION). ✓
- infercollid: !collation → InvalidOid; else LookupCollation(pstate, collation, exprLocation(expr)).
  inferopclass: !opclass → InvalidOid; else get_opclass_oid(BTREE_AM_OID, opclass, false). ✓
- result = lappend(InferenceElem). ✓
  (collation/opclass name lists bridged from raw `types_nodes::Node::String` to the
  parse_type/opclasscmds vocabularies `types_parsenodes::Node` / `types_opclass::StringNode`,
  the same bridge parse_expr.c's transformCollateClause uses.)

### transformOnConflictArbiter (C:3296) — PORTED, faithful
- out-params modeled as the returned tuple (arbiterExpr=NIL, arbiterWhere=NULL,
  constraint=InvalidOid defaults). ✓
- action==ONCONFLICT_UPDATE && !infer → ERRCODE_SYNTAX_ERROR "ON CONFLICT DO UPDATE requires
  inference specification or constraint name" + errhint + exprLocation(onConflictClause). ✓
- IsCatalogRelation(p_target_relation) → ERRCODE_FEATURE_NOT_SUPPORTED system-catalog ban. ✓
- RelationIsUsedAsCatalogTable(p_target_relation) → ban with RelationGetRelationName. ✓
- if infer: indexElems → resolve_unique_index_expr; whereClause → transformExpr
  EXPR_KIND_INDEX_PREDICATE; conname → get_relation_constraint_attnos(relid, conname, false,
  &constraint), perminfo->requiredPerms |= ACL_SELECT, perminfo->selectedCols =
  bms_add_members(selectedCols, conattnos). ✓
  - relid = RelationGetRelid(p_target_relation) = rd_id; perminfo =
    p_target_nsitem->p_perminfo. ✓

## Helpers (no C analog beyond the marshaling they perform)
- findWindowClause returns index (borrow-safe WindowClause* substitute).
- copy_node_ptr_vec = copyObject over a List*; sortgroupclauses_to_node_vec wraps
  transformed SortGroupClause as T_SortGroupClause list cells (WindowClause stores List* of
  SortGroupClause); node_vec_as_sortby/_to_collnames/_to_opclass_names vocabulary bridges;
  RelationIsUsedAsCatalogTable = rd_options.user_catalog_table (utils/rel.h macro).

## Parity notes
- SQLSTATEs verified: WINDOWING_ERROR, UNDEFINED_OBJECT, FEATURE_NOT_SUPPORTED,
  SYNTAX_ERROR, INVALID_COLUMN_REFERENCE — all match the C ereport calls.
- Constants: BTREE_AM_OID=403, BTINRANGE_PROC=3, FRAMEOPTION_DEFAULTS =
  RANGE|START_UNBOUNDED_PRECEDING|END_CURRENT_ROW — verified vs headers.
- `search_amproc_list2` faithfully mirrors `SearchSysCacheList2(AMPROCNUM, opfamily,
  opcintype)` (2-key partial list), not the 1-key list + post-filter; projects amproc/
  amprocrighttype/amprocnum (the fields the C `procform` reads).

## Out of scope / not ported (correctly)
- transformRangeTableFunc (XMLTABLE) / transformJsonTable (F3b) — blocked: no
  RangeTableFunc/JsonTable raw-Node variant in types-nodes; the FROM-item arms remain the
  pre-existing loud panic.
- common_prefix_cmp — not present in parse_clause.c (it is in optimizer/path/indxpath.c);
  the prompt's mention is an error. Nothing to port here.

## Gates
- `cargo check --workspace` clean.
- `cargo test -p no-todo-guard` pass (0 own-logic stubs in F3a).
- `cargo test -p seams-init` pass (search_amproc_list2 installed by syscache).
- `cargo test --workspace` pass except the sanctioned `range_pair_*` flake.
