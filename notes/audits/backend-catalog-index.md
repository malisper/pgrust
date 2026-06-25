# Audit: backend-catalog-index (catalog/index.c) — PARTIAL

Source: `postgres-18.3/src/backend/catalog/index.c` (4290 LOC, ~38 fns).
Status: **partial** — two fully-grounded slices landed; the catalog-write/build/drop
core is STOPPED on two genuinely-unported keystones (see below). Self-audited
function-by-function against the C for the landed slices.

## Landed (faithful, function-by-function)

### IndexGetRelation(indexId, missing_ok)
C lines 3582-3602. The C reads `indexrelid` (only for the `Assert`) and `indrelid`
(the result) off the `INDEXRELID` syscache tuple and allocates nothing. Mapped to a
NEW mcx-free syscache seam `index_get_relid(index_oid) -> PgResult<Option<Oid>>`
(syscache-seams) that projects exactly `Form_pg_index.indrelid` by value — same
shape as the existing `index_isclustered` projection. `Ok(None)` == `!HeapTupleIsValid`
→ `missing_ok ? InvalidOid : elog(ERROR, "cache lookup failed for index %u")`. The
`Assert(index->indexrelid == indexId)` holds by construction (the lookup keys on
indexId). The seam is uninstalled (owned by unported syscache.c) so a real call
panics at that boundary — the faithful SearchSysCache mirror.

### Reindexing-support backend-local state machine
C lines 4108-4289. The four C file-statics
(`currentlyReindexedHeap`/`currentlyReindexedIndex`/`pendingReindexedIndexes`/
`reindexingNestLevel`) are modeled as a `thread_local! RefCell<ReindexState>`,
matching the namespace/objectaccess backend-local idiom.

- `ReindexIsProcessingHeap` (4118) — `heapOid == currentlyReindexedHeap`.
- `ReindexIsCurrentlyProcessingIndex` (4128, file-static) — `== currentlyReindexedIndex`.
- `ReindexIsProcessingIndex` (4139) — `== currentlyReindexedIndex || list_member_oid(pending,...)`.
- `SetReindexProcessing` (4150, file-static) — `Assert` both valid; "cannot reindex
  while reindexing" if a heap is already set; set heap/index; `RemoveReindexPending`;
  set nest level from `GetCurrentTransactionNestLevel`.
- `ResetReindexProcessing` (4169, file-static) — clear heap/index; nest level kept.
- `SetReindexPending` (4183, file-static) — "cannot reindex while reindexing" if
  pending nonempty; "cannot modify reindex state during a parallel operation" if
  `IsInParallelMode`; `list_copy(indexes)`; set nest level.
- `RemoveReindexPending` (4199, file-static) — parallel-mode guard; `list_delete_oid`
  (`Vec::retain` removing all occurrences, matching list_delete_oid semantics).
- `ResetReindexState` (4212) — if `reindexingNestLevel >= nestLevel`: clear all.
- `EstimateReindexStateSpace` (4241) — `offsetof(...,pendingReindexedIndexes) +
  mul_size(sizeof(Oid), list_length(pending))`. Header offset = 2*sizeof(Oid) +
  sizeof(int) = 12 (Oid/int both 4-byte 4-aligned; flex array at 12, no padding).
  `mul_size` mirrors the overflow-checked C helper.
- `SerializeReindexState` (4252) / `RestoreReindexState` (4270) — write/read the
  `SerializedReindexState` layout into/out of the DSM chunk at the C field offsets
  via the audited raw-`usize` DSM primitive (cf. backend-utils-misc-guc
  `serialize_guc_state`). Restore appends each pending OID (`lappend_oid`) and
  re-reads the worker's own nest level.

The file-static mutators (`SetReindexProcessing`/`ResetReindexProcessing`/
`SetReindexPending`/`RemoveReindexPending`/`ReindexIsCurrentlyProcessingIndex`) are
`#![allow(dead_code)]` with rationale: they are only reached from `reindex_index`/
`reindex_relation`, the not-yet-landed drivers that live in this same crate. Kept so
the state machine lands whole rather than stubbed.

### Seams installed (init_seams, wired into seams-init)
- `backend-catalog-index-seams`: `index_get_relation`, `reindex_is_processing_index`,
  `reset_reindex_state`.
- `backend-access-transam-parallel-rt-seams`: `estimate_reindex_state_space`,
  `serialize_reindex_state`, `restore_reindex_state`.

Real consumers unblocked: brin scan/insert-vacuum + cluster (`index_get_relation`),
indexam + relcache (`reindex_is_processing_index`), xact abort (`reset_reindex_state`),
parallel.c (the three reindex-state transfer seams).

## STOPPED — precise keystone boundaries (task premise incorrect vs this base)

The owner inward seams `index_create`/`build_index_info`/`index_build`/
`reindex_relation`/`index_drop` stay UNINSTALLED (mirror-PG-and-panic), blocked on:

1. **pg_index INSERT carrier keystone.** `index_create` core (`UpdateIndexRelation`)
   and `BuildIndexInfo` need a full `FormData_pg_index`/`PgIndexInsertRow` carrier —
   all 23 cols incl. `int2vector indkey`, `oidvector indcollation/indclass`,
   `int2vector indoption`, `pg_node_tree indexprs/indpred` — plus a
   `catalog_tuple_insert_pg_index` producer. The repo has only the 7-field
   `types_rel::FormData_pg_index` relcache projection + a `catalog_tuple_update_pg_index`
   (no INSERT producer, no INSERT row type). Ripples into relcache's rd_index build.
   `BuildIndexInfo` also needs `RelationGetIndexExpressions`/`RelationGetIndexPredicate`/
   `RelationGetExclusionInfo` relcache seams (none exist; pg_node_tree decode unported).

2. **ambuild vtable keystone.** `index_build` dispatches `amroutine->ambuild`, but
   `types_tableam::amapi::IndexAmRoutine` carries only scan/insert/vacuum callbacks —
   no `ambuild`/`ambuildempty`/`amoptions`/`amgettuple`/… slot. Adding `ambuild` and
   populating it in every AM handler (nbtree/hash/gist/gin/spgist/brin) is a
   cross-cutting vtable keystone.

`index_set_state_flags` additionally needs `PgIndexForm` widened
(indislive/indisready/indisreplident) + `table_open(pg_index)`. The
`index_concurrently_*` family + `index_drop` further need WaitForLockers /
snapshot push-pop / relcache rebuild substrate.

**K4 (ii_OpclassOptions) is MOOT:** PG 18.3 `IndexInfo` (execnodes.h) has no
`ii_OpclassOptions` field; the repo `types_nodes::execnodes::IndexInfo` already
matches the C struct field-for-field (opclass options are a separate
`Datum *opclassOptions` param of index_create, not a struct member).

## Gate
- `cargo check -p backend-catalog-index -p backend-utils-cache-syscache-seams -p seams-init`: clean.
- No `unwrap`/`expect`/`panic!`/`unreachable!`/`todo!`/`unimplemented!` in owned logic.
- `indexcmds` F2 / REINDEX runtime remain blocked on the two keystones above (not
  re-fireable yet); when those land, `index_create`/`index_build`/`reindex_relation`
  fill into this crate.
