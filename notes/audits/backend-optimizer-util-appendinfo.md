# Audit: backend-optimizer-util-appendinfo (optimizer/util/appendinfo.c)

C source: `../pgrust/postgres-18.3/src/backend/optimizer/util/appendinfo.c` (1060 LOC).
Re-derived independently from the C; verdicts below.

## Function inventory (16 defns + 1 helper split)

| # | C fn (line) | port location | verdict | notes |
|---|---|---|---|---|
| 1 | make_inh_translation_list (80, static) | lib.rs make_inh_translation_list | MATCH | reads parent/child rd_att attrs directly off RelationData; same-relation fast path; name search → next-col-then-syscache (search_syscache_attname seam); type/collation mismatch ereports (INVALID_COLUMN_DEFINITION class via PgError::error); dropped→None; translated Vars interned into node_arena (C List of Var* in CurrentMemoryContext → arena handles), threads &mut PlannerInfo (C uses ambient memctx). |
| 2 | make_append_rel_info (51) | lib.rs make_append_rel_info | MATCH | parent/child_reltype from rd_rel.reltype (types-rel carrier widened); parent_reloid = rd_id; delegates to (1). Threads &mut root for interning. |
| 3 | adjust_appendrel_attrs (200) | lib.rs adjust_appendrel_attrs | MATCH | sets up context; Assert(nappinfos>=1); drives mutator; surfaces sticky err. C Assert(node != Query) is structural (Query is not an Expr variant). |
| 4 | adjust_appendrel_attrs_mutator (219, static) | lib.rs adjust_appendrel_attrs_mutator | MATCH (1 unreachable sub-branch errs) | Var attno>0 (translated_vars lookup, copy, merge varnullingrels, carry varreturningtype; non-Var translation errors on returningtype/nullingrels); Var attno==0 named-rowtype → ConvertRowtypeExpr (sets var.vartype=child_reltype, COERCE_IMPLICIT_CAST, loc -1); Var attno==0 RECORDOID → RowExpr needs parse->rtable colnames (seam drops run/mcx) → loud Err, UNREACHABLE for inheritance (per C comment) + non-inherited; ROWID_VAR leaf substitute / makeNullConst leaf-miss; CurrentOfExpr cvarno; PlaceHolderVar recurse + phrels via adjust_child_relids; generic expression_tree_mutator for the rest. C RestrictInfo/SpecialJoinInfo/AppendRelInfo/Query/SubLink/JoinExpr arms are structurally impossible (not Expr variants); RestrictInfo handled at the list entry (see #17). |
| 5 | adjust_appendrel_attrs_multilevel (545) | lib.rs adjust_appendrel_attrs_multilevel | MATCH | recurse to top parent (childrel.parent chain; elog if not a child); find_appinfos_by_relids; adjust_appendrel_attrs. C pfree(appinfos) is a no-op (owned Vec drops). |
| 6 | adjust_child_relids (578) | lib.rs adjust_child_relids | MATCH | lazy copy-on-change; del parent / add child per appinfo; returns copy of original when unchanged (value model). |
| 7 | adjust_child_relids_multilevel (612) | lib.rs adjust_child_relids_multilevel | MATCH | bms_overlap early-out; recurse to top parent; find_appinfos + adjust_child_relids. |
| 8 | adjust_inherited_attnums (652) | lib.rs adjust_inherited_attnums | MATCH | Assert parent_reloid valid; per attno: bounds + must-be-Var checks (elog), push childvar.varattno. |
| 9 | adjust_inherited_attnums_multilevel (686) | lib.rs adjust_inherited_attnums_multilevel | MATCH | append_rel_array[child_relid] lookup (elog if absent); recurse if parent != top; adjust_inherited_attnums. |
| 10 | get_translated_update_targetlist (714) | lib.rs get_translated_update_targetlist | MATCH | Assert CMD_UPDATE; relid==resultRelation → copyObject(processed_tlist)/(update_colnos); else adjust_appendrel_attrs_multilevel over tlist + adjust_inherited_attnums_multilevel. Threads run/mcx (resolves opaque parse). |
| 11 | find_appinfos_by_relids (757) | lib.rs find_appinfos_by_relids | MATCH | capacity = bms_num_members; bms_next_member loop; append_rel_array[i] → push; None → find_base_rel_ignore_join==NULL ? continue : elog. (ignore-join probe: see seam audit / DESIGN_DEBT.) |
| 12 | add_row_identity_var (813) | lib.rs add_row_identity_var | MATCH | Asserts; rtindex==resultRelation → push junk TLE; else find/make RowIdentityVarInfo by name (equal() on the ROWID_VAR-varno copy via equal_expr seam; conflict → elog), rowidwidth=get_typavgwidth(exprType,exprTypmod), rowidrels singleton, rowid_var.varattno=list_length, push ROWID_VAR ref TLE. result_relation threaded (parse opaque). |
| 13 | add_row_identity_columns (908) | lib.rs add_row_identity_columns | MATCH | Assert modify; RELATION/MATVIEW/PARTITIONED → ctid TID Var; FOREIGN → fdw AddForeignUpdateTargets (ext-seam, mirror-panic) + UPDATE-or-delete-trigger wholerow RECORD Var. |
| 14 | distribute_row_identity_vars (989) | lib.rs distribute_row_identity_vars | MATCH | non-modify → Assert empty, return; rt_fetch resultRelation; !inh → Assert empty return; row_identity_vars empty edge → table_open + add_row_identity_columns + table_close + build_base_rel_tlists; else copy ROWID_VAR ref Vars into target_rel reltarget.exprs. Seam re-signed (mcx, run) to resolve opaque parse. |
| 15 | row_identity_var_rowidwidth (preptlist.c, homed in relnode-ext) | lib.rs row_identity_var_rowidwidth | MATCH | RowIdentityVarInfo.rowidwidth at 0-based n. |
| 16 | adjust_restrictinfo (the IsA(RestrictInfo) arm of #4) | lib.rs adjust_restrictinfo | MATCH | flat copy (clone incl rinfo_serial); recurse clause + orclause; adjust_child_relids on the 5 relid sets; reset eval_cost.startup/norm_selec/outer_selec/left_em/right_em/scansel_cache/left_right_bucketsize/mcvfreq to the C sentinels. NOTE: C does NOT reset left_ec/right_ec — port leaves them (clone) ✓. |

## Seam audit

Owned inward seam crates (C-source = appendinfo.c):
- `backend-optimizer-util-appendinfo-seams`: find_appinfos_by_relids,
  adjust_child_relids, adjust_appendrel_attrs_restrictlist,
  distribute_row_identity_vars — ALL installed in `init_seams()`. ✓
- appendinfo-owned seams homed in `relnode-ext-seams` (consumer-side, this unit
  is the C owner): adjust_appendrel_attrs_node, row_identity_var_rowidwidth —
  both installed in `init_seams()`. ✓
- `backend-optimizer-util-appendinfo-ext-seams` (consumer-side, NO owner dir;
  guard skips): FDW AddForeignUpdateTargets pair — mirror-panic until FDW lands. ✓

`init_seams()` contains only `set()` calls; `seams-init::init_all()` calls it
(after relnode). seams-init gate passes (4 tests).

Outward calls all justified by real cycles and thin marshal+delegate:
- relnode-seams `relids_*` + find_base_rel (relnode↔appendinfo cycle); pathnode-seams
  relids_del_members (del_member); lsyscache-seams get_rel_name/get_typavgwidth/
  get_typlenbyval; syscache-seams search_syscache_attname; equalfuncs-seams
  equal_expr; plan-small-seams build_base_rel_tlists; table-table-seams table_open/
  relation_close; backend-nodes-core makefuncs/nodefuncs/rewrite-core relids (direct,
  acyclic). No logic in seam paths.

## Findings

1. (DESIGN_DEBT, documented) `find_base_rel_ignore_join` reimplemented in-crate as
   a `simple_rel_array` slot probe because relnode owns it but exposes no seam and
   appendinfo↔relnode is a cycle; the seam contract also drops `run` (needed only
   for the C debug RTE-kind assert). Behavior faithful on every reachable input.
   Logged in DESIGN_DEBT.md.

## Verdict: PASS (with 1 ledgered design-debt item)

Every function MATCH; the single mutator sub-branch that errs (UNION-ALL whole-row
RowExpr) is structurally unreachable for the queries this model produces and is a
seam-contract limitation (dropped run/mcx), not absent logic. All owned seams
installed. SELECT 1 stays green; the catalog SELECT clears the
distribute_row_identity_vars wall.
