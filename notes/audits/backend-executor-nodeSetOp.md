# Audit: backend-executor-nodeSetOp

Unit: `backend-executor-nodeSetOp` (+ `types-nodes::nodesetop`,
new `backend-executor-execGrouping-seams`, additions to
`backend-executor-execUtils-seams`/`backend-executor-execTuples-seams`/
`backend-utils-sort-sortsupport-seams`)
C source: `src/backend/executor/nodeSetOp.c`
c2rust: `c2rust-runs/backend-executor-nodeSetOp/src/nodeSetOp.rs`
Port: `crates/backend-executor-nodeSetOp/src/lib.rs`
Date: 2026-06-13
Model: Claude Opus 4.8 (1M)

## Function inventory and verdicts

Every function definition in `nodeSetOp.c` (statics included), cross-checked
against the c2rust rendering:

| C function / construct | C loc | Port loc | Verdict | Notes |
|---|---|---|---|---|
| `build_hash_table` (static) | 83 | `build_hash_table` | MATCH | Asserts strategy==HASHED & numGroups>0 (debug_assert); ExecGetResultType(outerPlanState) → desc; ExecGetCommonChildSlotOps → input ops; passes numCols/cmpColIdx/eqfuncoids/hashfunctions/cmpCollations/numGroups, additionalsize=size_of::<SetOpStatePerGroupData>()=16, metacxt=es_query_cxt, tablecxt=tableContext, tempcxt=econtext.ecxt_per_tuple_memory, use_variable_hash_iv=false. BuildTupleHashTable SEAMED to execGrouping (unported → panics). |
| `set_output_count` (static) | 119 | `set_output_count` | MATCH | All four SETOPCMD arms identical (INTERSECT/INTERSECT_ALL min/EXCEPT/EXCEPT_ALL diff-or-0); `default` → elog(ERROR,"unrecognized set op: %d") preserved with ERRCODE_INTERNAL_ERROR. |
| `ExecSetOp` (static, ExecProcNode body) | 159 | `ExecSetOp` | MATCH | CHECK_FOR_INTERRUPTS; numOutput>0 → dec+return result slot; setop_done → NULL; HASHED → fill (if !table_filled) then retrieve_hash; else retrieve_sorted. |
| `setop_retrieve_sorted` (static) | 196 | `setop_retrieve_sorted` | MATCH | need_init: read both first tuples, empty-outer short-circuit (setop_done+NULL), set both needGroup; merge loop: load_group on needGroup, numTuples==0 outer→done, cmpresult=-1 when right empty else compare; <0/==0/>0 (continue) branches set numTuples and needGroup exactly; set_output_count then emit. End: ExecClearTuple+NULL. |
| `setop_load_group` (static) | 330 | `setop_load_group` | MATCH | needGroup=false; TupIsNull(next) → ExecClearTuple(first)+numTuples=0+return; else ExecStoreMinimalTuple(ExecCopySlotMinimalTuple(next), first, true)+numTuples=1; loop ExecProcNode→next, break on null, compare(first,next), Assert(<=0) (debug_assert), break on !=0, else numTuples++. |
| `setop_compare_slots` (static) | 377 | `setop_compare_slots` | MATCH | slot_getallattrs(s1)+(s2); per-key loop over numCols reading tts_values/tts_isnull[attno-1]; ApplySortComparator; return on !=0; else 0. |
| `setop_fill_hash_table` (static) | 406 | `setop_fill_hash_table` | MATCH | outer loop: ExecProcNode→break on TupIsNull, have_tuples=true, LookupTupleHashEntry(create), if isnew zero counts, numLeft++, ResetExprContext. if have_tuples inner loop: ExecProcNode→break on null, LookupTupleHashEntry(no-create), if entry numRight++, ResetExprContext. table_filled=true; ResetTupleHashIterator. isnew-zeroing done in the lookup callback (C caller location), matching the C `if (isnew)`. |
| `setop_retrieve_hash_table` (static) | 503 | `setop_retrieve_hash_table` | MATCH | loop while !setop_done: CHECK_FOR_INTERRUPTS; ScanTupleHashTable→none sets setop_done+NULL; set_output_count; if numOutput>0 dec + ExecStoreMinimalTuple(TupleHashEntryGetTuple(entry), result, false)+return. End: ExecClearTuple+NULL. |
| `ExecInitSetOp` | 563 | `ExecInitSetOp` | MATCH | Assert no BACKWARD/MARK (debug_assert); makeNode; ps.plan back-link set; ExecProcNode=ExecSetOp; flags/numOutput/numCols/need_init; ExecAssignExprContext; HASHED→AllocSetContextCreate tableContext (mcx child); HASHED clears EXEC_FLAG_REWIND; ExecInitNode outer+inner; ExecInitResultTupleSlotTL(MinimalTuple); sorted→left.first=resultSlot alias + right.first=ExecInitExtraTupleSlot; ps_ProjInfo=NULL; HASHED→execTuplesHashPrepare; sorted→sortKeys per-key (ssup_cxt=CurrentMemoryContext, collation/nulls_first/attno, abbreviate=false, PrepareSortSupportFromOrderingOp); HASHED→build_hash_table + table_filled=false. |
| `ExecEndSetOp` | 680 | `ExecEndSetOp` | MATCH | tableContext drop = MemoryContextDelete (mcx owns domain, frees on drop; hashtable lives in it and is dropped); ExecEndNode(outer); ExecEndNode(inner). |
| `ExecReScanSetOp` | 692 | `ExecReScanSetOp` | MATCH | ExecClearTuple(result); setop_done=false; numOutput=0; HASHED: !table_filled→return; both chgParam NULL→ResetTupleHashIterator+return; MemoryContextReset(tableContext); ResetTupleHashTable; table_filled=false. sorted: need_init=true. Then chgParam-NULL guarded ExecReScan(outer)/(inner). |

Constructs verified against C headers (not memory):
- `SETOPCMD_INTERSECT/INTERSECT_ALL/EXCEPT/EXCEPT_ALL` = 0/1/2/3, `SETOP_SORTED/HASHED` = 0/1 (nodes.h enum order).
- `T_SetOp`=371, `T_SetOpState`=435 (nodetags.h, verified).
- `sizeof(SetOpStatePerGroupData)` = two int64 = 16 bytes (additionalsize).
- `EXEC_FLAG_REWIND`=0x0004, `_BACKWARD`=0x0008, `_MARK`=0x0010 (executor.h).
- `TupleHashTableData`/`TupleHashEntryData`/`TupleHashIterator` fields mirror
  execnodes.h field-for-field; `hashtab` (simplehash) kept `Opaque`
  (execGrouping-internal).

## Seam audit

Owned seam crates by C-source coverage: nodeSetOp.c is not covered by any
`crates/X-seams` that this unit owns (the node is reached via the executor
dispatch, which can depend on this crate directly — same as nodeMergejoin).
`init_seams()` is therefore empty, which is correct (no owned inbound seams);
it is registered in `seams-init::init_all()`.

Outward seam calls (all justified by the real executor cycle node↔execProcnode,
or by a genuinely unported owner; each is thin marshal+delegate):

- execProcnode (`exec_init_node`/`exec_proc_node`/`exec_end_node`) — real cycle.
- execAmi (`exec_re_scan`) — real cycle.
- tcop/postgres (`check_for_interrupts`).
- execUtils (`exec_assign_expr_context`; new `exec_get_common_child_slot_ops`,
  installed by the execUtils owner — `declared==set` upheld there).
- execTuples (`exec_init_result_tuple_slot_tl`, `exec_init_extra_tuple_slot`,
  `exec_clear_tuple`, `exec_get_result_type`, `slot_getallattrs`; new
  `exec_copy_slot_minimal_tuple`/`exec_store_minimal_tuple`) — execTuples owner
  unported; calls panic until it lands.
- execGrouping (NEW `backend-executor-execGrouping-seams`:
  `exec_tuples_hash_prepare`/`build_tuple_hash_table`/`lookup_tuple_hash_entry`/
  `reset_tuple_hash_table`/`reset_tuple_hash_iterator`/`scan_tuple_hash_table`)
  — execGrouping unported; calls panic until it lands. Crossing types are the
  real `TupleHashTable`/`TupleHashEntryData` (execnodes.h public), not invented
  handles.
- sortsupport (`apply_sort_comparator`; new
  `prepare_sort_support_from_ordering_op`) — sortsupport owner unported; panics.

No branching/computation lives in any seam shim. `set_output_count`,
`setop_compare_slots`'s key loop, the merge state machine, the SQL92 output
arithmetic, and the per-group count bookkeeping are all owned in this crate.

## Design conformance

- Mcx + PgResult on every allocating/ereport-capable seam and owned fn.
- Opacity inherited, not introduced: TupleHash family is the real public C
  struct; `hashtab` stays `Opaque` (execGrouping-internal), the only opaque field.
- No shared statics / ambient-global seams; per-query state is on the
  `EStateData`/state node; `tableContext` is an owned `MemoryContext` (drop =
  delete, `reset` = MemoryContextReset).
- No locks held across `?`.
- Owned-logic error paths return `Err(PgError)` (set_output_count's
  unrecognized-cmd elog); panics are only on unported callees / castNode-failed
  invariants (mirror-pg-and-panic), never standing in for this unit's logic.

### Minor note (not a finding)
`setop_retrieve_hash_table` copies the scanned entry's `firstTuple` inside the
`ScanTupleHashTable` callback before the `set_output_count` emit decision, where
the C only materializes on emit. This is an allocation-locality difference, not
behavioral; it is on a path gated entirely behind the (panicking) execGrouping
seam, so it never runs until execGrouping lands and can be tightened then.

## Verdict: PASS

All 11 functions MATCH (hash-table / slot / sortsupport callees SEAMED to their
unported owners per the rules). Zero seam-shim logic findings; design rules
upheld. `cargo check --workspace` clean; `cargo test --workspace` green (723
test-result-ok lines, no failures); node-crate unit tests pass.
