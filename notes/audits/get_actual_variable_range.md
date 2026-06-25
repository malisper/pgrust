# Audit: get_actual_variable_range + get_actual_variable_endpoint (selfuncs.c)

Scope: the two functions added for the partition_join cost-model keystone. Both
re-derived from PG 18.3 selfuncs.c independently of the port comments.

## Function table

| C function (selfuncs.c) | Port location | Verdict | Notes |
|---|---|---|---|
| `get_actual_variable_endpoint` (6770-6925) | backend-access-index-indexam/src/lib.rs `get_actual_variable_endpoint` | MATCH | Bare-scandesc probe. InitNonVacuumableSnapshot (sentinel+vistest), index_beginscan(.,.,snap,NULL,1,0), xs_want_itup=true, index_rescan(scankeys,1,NULL,0), loop index_getnext_tid, VM_ALL_VISIBLE, index_fetch_heap on !all_visible, VISITED_PAGES_LIMIT=100 + block!=last_heap_block page-count cap, xs_itup null -> elog(ERROR) XX000, xs_recheck break, index_deform_tuple, isnull[0]->elog(ERROR), datumCopy=clone_in(mcx), ReleaseBuffer(vmbuffer)+index_endscan. Intermediate ExecClearTuple(tableslot) omitted but equivalent: success path breaks immediately and exec_drop_single_tuple_table_slot clears+releases the pin; the loop never continues after a successful fetch. scankey/slot/typlen built per-call (C builds once in the range driver and shares) — behaviorally identical. Transient AllocSetContext modeled by the per-call mcx arena (repo convention). |
| `get_actual_variable_range` (6581-6751) | backend-utils-adt-selfuncs/src/ineq.rs `get_actual_variable_range` | MATCH | Index-selection driver. rel==NULL/indexlist==NIL guard, Assert(rtekind==RTE_RELATION), RELKIND_PARTITIONED_TABLE skip, per-index filters (sortopfamily empty / indpred / hypothetical / !canreturn[0] / collation!=indexcollations[0] / !match_index_to_operand), get_op_opfamily_strategy + IndexAmTranslateStrategy(.,relam,.,true) -> COMPARE_LT/GT with reverse_sort -> scan direction, table_open/index_open NoLock, get_typlenbyval, min then (max && have_data) with -indexscandir, unconditional break after first suitable index. Returns ActualVariableRange{have_data,min,max} (C bool + *min/*max). NoLock close = owned Relation drop. |
| caller `ineq_histogram_selectivity` (1099-1142 endpoint refinement) | ineq.rs (endpoint_override) | MATCH | C overwrites sslot.values[0/1/probe] and reads them in both FunctionCall2Coll AND convert_to_scalar. Port keeps a parallel endpoint_override: Vec<Option<DatumV>>; hist_value_at returns the override for the comparison, and the convert_to_scalar low/high words read the override word for by-value types. By-ref types short-circuit binfrac=0.5 (repo convert.rs convention, unchanged). have_end gates clamp_probability exactly as C. |

## Seams

New seam `get_actual_variable_endpoint` declared in
backend-access-index-indexam-seams, installed by
backend-access-index-indexam::init_seams() (called from seams-init). The driver
reaches it plus the already-installed match_index_to_operand (indxpath),
get_op_opfamily_strategy/get_typlenbyval (lsyscache), index_am_translate_strategy
(amapi), relation_open/index_open (relation/indexam), global_vis_test_for
(vacuumlazy), visibilitymap_get_status (visibilitymap), index_deform_tuple
(indextuple), release_buffer (bufmgr), exec_drop_single_tuple_table_slot
(execTuples) — all installed. No logic in any seam path (thin marshal+delegate).
No outward-seam cycle violations: the endpoint probe lives in indexam (owner of
the scan-descriptor primitives), the driver in selfuncs (owner of the optimizer
index-selection logic).

## Verdict: PASS

Every function MATCH; zero seam findings. Controlled before/after on the same
tree (dd7ebc2): partition_join 2210 -> 2161 difflines; MergeJoin 4->2 (PG=2),
MergeAppend 6->5 (PG=5) — the predicted shape shift (index-ordered merge joins
no longer under-costed).
