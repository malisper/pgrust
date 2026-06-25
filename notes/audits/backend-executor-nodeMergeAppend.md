# Audit: backend-executor-nodeMergeAppend

- **Verdict: PASS** (after fix)
- Date: 2026-06-12
- Model: claude-opus-4-8[1m]
- Branch: port/backend-executor-nodeMergeAppend (audited at commit 8b83702)
- C source: `src/backend/executor/nodeMergeAppend.c` (+ in-crate
  `src/common/binaryheap.c`, specialized to slot-index entries)
- c2rust: `../pgrust/c2rust-runs/backend-executor-nodeMergeAppend/src/nodeMergeAppend.rs`
- Port: `crates/backend-executor-nodeMergeAppend/src/lib.rs`,
  `crates/types-nodes/src/nodemergeappend.rs`

## 1. Function inventory and per-function verdicts

### nodeMergeAppend.c

| C function (location) | Port location | Verdict | Notes |
|---|---|---|---|
| `ExecInitMergeAppend` (nodeMergeAppend.c:64) | lib.rs:95 | MATCH | Both pruning/non-pruning branches; `bms_add_range(NULL,0,nplans-1)`; `palloc0` of `ms_slots`; per-key sortkey setup loop with `abbreviate=false`; `ms_initialized=false`. `ExecProcNode` callback installed (`exec_merge_append_node`). |
| `ExecMergeAppend` (nodeMergeAppend.c:214, static) | lib.rs:313 (+ callback lib.rs:78) | MATCH | `CHECK_FOR_INTERRUPTS`; `ms_nplans==0` early clear-return; lazy `ExecFindMatchingSubPlans`; first-pass pull+`add_unordered`+`build`; steady-state `binaryheap_first`/`ExecProcNode`/`replace_first`-or-`remove_first`; final empty→clear, else return head slot. Exhaustion returns the cleared `ps_ResultTupleSlot` (the C non-NULL empty slot). |
| `heap_compare_slots` (nodeMergeAppend.c:287, static) | lib.rs:436 | MATCH | nkey loop, `slot_getattr` per key, `ApplySortComparator`, `INVERT_COMPARE_RESULT` on non-zero then return; else 0. |
| `ExecEndMergeAppend` (nodeMergeAppend.c:334) | lib.rs:483 | MATCH | `ExecEndNode` over all `ms_nplans` children. |
| `ExecReScanMergeAppend` (nodeMergeAppend.c:354) | lib.rs:506 | MATCH | prune-state+`bms_overlap`→unset valid subplans; per-child `UpdateChangedParamSet` (when chgParam set) + `ExecReScan` (when subnode chgParam null); `binaryheap_reset`; `ms_initialized=false`. chgParam cloned once pre-loop (read-only in loop) — behaviorally identical. |

### In-crate binary heap (src/common/binaryheap.c, specialized; not seamed — leaf algorithm, no cycle)

| C function | Port location | Verdict | Notes |
|---|---|---|---|
| `binaryheap_allocate` (binaryheap.c:38) | nodemergeappend.rs:167 `BinaryHeap::allocate` | MATCH | capacity→`bh_space`, size 0, has_heap_property true, reserved backing store. |
| `binaryheap_reset` (binaryheap.c:62) | lib.rs:581 | MATCH | size 0, property true (clears the vec too — equivalent, content is dead). |
| `binaryheap_add_unordered` (binaryheap.c:115) | lib.rs:596 | MATCH | capacity guard → `elog(ERROR,"out of binary heap slots")`; property false; append; size++. |
| `binaryheap_build` (binaryheap.c:137) | lib.rs:659 | MATCH | `for i=parent_offset(size-1); i>=0; i--` sift_down; property true. Guarded `bh_size>=1`. |
| `binaryheap_first` (binaryheap.c:176) | lib.rs:608 | MATCH | returns `bh_nodes[0]` (empty→error vs C Assert; Assert is debug-only). |
| `binaryheap_remove_first` (binaryheap.c:191) | lib.rs:618 | MATCH | one-element fast path; else last→root, sift_down(0). |
| `binaryheap_replace_first` (binaryheap.c:254) | lib.rs:687 | MATCH | `bh_nodes[0]=d`; sift_down(0) when size>1. |
| `left_offset`/`right_offset`/`parent_offset` (binaryheap.c:89/95/101) | lib.rs:716/721/711 | MATCH | `2i+1`/`2i+2`/`(i-1)/2`. |
| `sift_down` (binaryheap.c:312) | lib.rs:728 | MATCH | swap_off=left; right<size && cmp(left,right)<0 → swap_off=right; break if left>=size **or** cmp(node,swap)>=0; the `left>=size` short-circuit is preserved (port breaks before reading `bh_nodes[swap_off]`); hole-fill. |
| `binaryheap_add` (binaryheap.c:153) | — | N/A | Not reachable from nodeMergeAppend (heap is built via add_unordered+build); correctly omitted from the specialization. |
| `binaryheap_remove_node` (binaryheap.c:225) | — | N/A | Unreachable from this node. |
| `binaryheap_free` (binaryheap.c:74) | — | N/A | Owned-tree drop replaces `pfree`. |
| `sift_up` (binaryheap.c:269, static) | — | N/A | Only used by `binaryheap_add`/`remove_node`, both unreachable here. |

### Inlined sortsupport/list helpers

| C | Port location | Verdict | Notes |
|---|---|---|---|
| `ApplySortComparator` (sortsupport.h, inline) | lib.rs:863 | MATCH | NULL ordering branches + reverse inversion; the unsigned comparator goes through the sortsupport seam. |
| `INVERT_COMPARE_RESULT` (sortsupport.h, macro) | lib.rs:824 | MATCH | `var<0 ? 1 : -var`, `-INT_MIN` corner avoided via `wrapping_neg`. |
| `list_length` (pg_list.h) | lib.rs:835 | MATCH | slice length. |

## 2. Constants verified against headers

| Constant | Authoritative value | Port value (after fix) | Source |
|---|---|---|---|
| `T_MergeAppend` | 335 | 335 | nodetags.h:352 |
| `T_MergeAppendState` | **398** | **398** (was 401 — FIXED) | nodetags.h:415 |
| `EXEC_FLAG_BACKWARD`/`EXEC_FLAG_MARK` | (from types-nodes::executor) | reused, not re-transcribed | — |

## 3. Seam / wiring audit

- **Owned seam crates:** none. No `crates/X-seams` maps to `nodeMergeAppend.c`,
  and `binaryheap.c` is inlined (leaf, no cycle). `init_seams()` is therefore
  legitimately empty, and is still wired (`seams-init/src/lib.rs:28` calls
  `backend_executor_nodeMergeAppend::init_seams()`). No owned declarations are
  left uninstalled → not a FAIL.
- **Outward seam calls** all thin marshal+delegate, each justified by a real
  executor dispatch cycle: execProcnode (`exec_init_node`/`exec_proc_node`/
  `exec_end_node`), execAmi (`exec_re_scan`), execPartition
  (`exec_init_partition_exec_pruning`/`exec_find_matching_subplans`), execTuples
  (`exec_init_result_tuple_slot_tl`/`exec_clear_tuple`/`slot_getattr`),
  nodes-core (`bms_*`), sortsupport (`prepare_sort_support_from_ordering_op`/
  `apply_sort_comparator`), tcop/postgres (`check_for_interrupts`). No branching
  or node construction in a seam path.
- `ExecGetCommonSlotOps` / `UpdateChangedParamSet` reached via a **direct
  dependency** on execUtils (no cycle) — appropriate, not seamed.
- No function body was replaced by a seam to "elsewhere"; the merge/heap logic
  lives in this crate.

## 4. Design conformance

- Allocating functions take `Mcx` + return `PgResult` (`ExecInitMergeAppend`,
  `BinaryHeap::allocate`, `clone_bitmapset`, the heap mutators). No shared
  statics for per-backend state; no ambient-global seams; no locks across `?`.
- `PartitionPruneState` is a trimmed *real* struct mirroring the C (the unread
  fields land with the execPartition owner) — inherited shaping, not invented
  opacity. `SlotId`/`PlanStateNode` follow the established owned model.
- No registry-shaped side tables; no unledgered divergence markers.

## Findings and resolution

1. **`T_MergeAppendState` transcribed as 401, correct value 398**
   (`types-nodes/src/nodemergeappend.rs:29`). The tag is live: it is returned by
   `PlanStateNode::tag()` (`planstate.rs:34`) as the runtime `NodeTag`, so the
   wrong value (which collides with the real `T_BitmapAndState`) would corrupt
   any tag comparison. This is exactly the transcribed-constant corruption class
   the audit guards against. **Fixed** → `NodeTag(398)`, verified against
   `nodetags.h:415`. Crate rebuilds clean; 5 unit tests pass.

No other findings. **PASS.**
