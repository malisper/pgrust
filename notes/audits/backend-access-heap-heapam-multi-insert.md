# Audit: backend-access-heap-heapam — `heap_multi_insert` (task #316)

C source: `src/backend/access/heap/heapam.c` (PG 18.3), function
`heap_multi_insert` (lines 2351-2703) and the page-count helper
`heap_multi_insert_pages` (already ported). Cross-checked against the existing
`heap_insert` port (`crates/backend-access-heap-heapam/src/insert.rs`), which
shares `heap_prepare_insert`, the buffer/WAL substrate seams, and the
header/flag vocabulary.

This audit covers only the net-new `insert::heap_multi_insert` (the rest of the
crate is covered by the merged-state CATALOG row). The seam was previously
declared in `backend-access-heap-heapam-seams` and consumed by
`catalog/indexing.c`'s `CatalogTuplesMultiInsertWithInfo`
(`backend-catalog-indexing/src/keystone.rs`) but had no installed body — it was
on the `CONTRACT_RECONCILE_PENDING` allowlist as mirror-pg-and-panic. This task
installs the real body and removes the allowlist entry.

## Carrier model

C takes `TupleTableSlot **slots` and fetches each slot's heap tuple via
`ExecFetchSlotHeapTuple`, stamps `slots[i]->tts_tableOid`, and at the end writes
the stored TID back into `slots[i]->tts_tid`. The repo carries heap tuples as
owned `FormedTuple` values (header + user-data area), so:

- the batch crosses **by value** (`tuples: PgVec<FormedTuple>`) — the slot
  fetch is the caller's responsibility, as it already is in the keystone caller;
- the inserted tuples are **returned** (`PgVec<FormedTuple>`), each with
  `t_self`/`t_tableOid` stamped, in input order. This is exactly C's
  `heaptuples[]` array — the tuples it `CacheInvalidateHeapTuple`s and whose
  `t_self` it copies back to the slots — which is the information the caller
  feeds to `CatalogIndexInsert`.

This is the identical model the seam declaration already documented and the
keystone caller was already written against (it iterates `inserted` directly).

## Function parity

| C step (heapam.c) | Port | Verdict |
|---|---|---|
| `xid = GetCurrentTransactionId()` | `xact_seam::get_current_transaction_id` | MATCH |
| `need_tuple_data = RelationIsLogicallyLogged(relation)` | `relation_is_logically_logged` (shared helper) | MATCH |
| `need_cids = RelationIsAccessibleInLogicalDecoding(relation)` | `relation_is_accessible_in_logical_decoding` (shared helper) | MATCH |
| `Assert(!(options & HEAP_INSERT_NO_LOGICAL))` | `debug_assert_eq!(options & HEAP_INSERT_NO_LOGICAL, 0)` | MATCH |
| `AssertHasSnapshotForToast(relation)` | debug-only snapshot assertion; no state to mirror (cf. delete.rs/update.rs) | RE-MODELED (no-op, as elsewhere in crate) |
| `needwal = RelationNeedsWAL(relation)` | `relcache_seam::relation_needs_wal` | MATCH |
| `saveFreeSpace = RelationGetTargetPageFreeSpace(relation, HEAP_DEFAULT_FILLFACTOR)` | `hio_seams::relation_get_target_page_free_space` | MATCH |
| per-tuple: `ExecFetchSlotHeapTuple` + `tts_tableOid`/`t_tableOid` stamp + `heaptuples[i] = heap_prepare_insert(...)` | owns each input tuple, sets `t_tableOid`, runs `heap_prepare_insert`, stores toasted copy if returned else the tuple | MATCH (carrier-adapted) |
| pre-loop `CheckForSerializableConflictIn(relation, NULL, InvalidBlockNumber)` | `predicate_seam::check_for_serializable_conflict_in` | MATCH |
| `while (ndone < ntuples)` page loop | `while ndone < ntuples` | MATCH |
| `CHECK_FOR_INTERRUPTS()` | `page_seam::check_for_interrupts` | MATCH |
| npages recompute (`ndone==0 \|\| !starting_with_empty_page` else `npages_used++`) | identical | MATCH |
| `RelationGetBufferForTuple(relation, heaptuples[ndone]->t_len, InvalidBuffer, options, bistate, &vmbuffer, NULL, npages - npages_used)` | same; `NULL` vmbuffer_other → an unused `&mut vmbuffer_other` local (RelationGetBufferForTuple takes `&mut Buffer`, only writes it for a non-Invalid other_buffer) | MATCH |
| `starting_with_empty_page = PageGetMaxOffsetNumber(page) == 0` | `page_seam::page_get_max_offset_number == 0` | MATCH |
| `all_frozen_set = starting_with_empty_page && (options & HEAP_INSERT_FROZEN)` | identical | MATCH |
| `RelationPutHeapTuple(relation, buffer, heaptuples[ndone], false)` + `if (needwal && need_cids) log_heap_new_cid` | identical | MATCH |
| `for (nthispage=1; ...)` fill loop w/ `PageGetHeapFreeSpace(page) < MAXALIGN(t_len) + saveFreeSpace` break, `RelationPutHeapTuple`, `log_heap_new_cid` | identical (`maxalign(t_len) + save_free_space`) | MATCH |
| `PageIsAllVisible && !(FROZEN)` ⇒ clear + `visibilitymap_clear(..., VISIBILITYMAP_VALID_BITS)`; `else if all_frozen_set` ⇒ `PageSetAllVisible` | identical | MATCH |
| `MarkBufferDirty(buffer)` | `page_seam::mark_buffer_dirty` | MATCH |
| WAL: scratch build of `xl_heap_multi_insert` header + `offsets[]` (when `!init`) + per-tuple `SHORTALIGN`'d `xl_multi_insert_tuple` + tuple data; flags (`ALL_VISIBLE_CLEARED`/`ALL_FROZEN_SET`/`CONTAINS_NEW_TUPLE`/`LAST_IN_MULTI`); `init`⇒`XLOG_HEAP_INIT_PAGE`+`REGBUF_WILL_INIT`; `need_tuple_data`⇒`REGBUF_KEEP_DATA`; `XLogRegisterData(scratch..tupledata)`, `XLogRegisterBuffer(0,...,REGBUF_STANDARD\|bufflags)`, `XLogRegisterBufData(0, tupledata, totaldatalen)`, `XLogSetRecordFlags(XLOG_INCLUDE_ORIGIN)`, `XLogInsert(RM_HEAP2_ID, info)`, `PageSetLSN` | byte-faithful: single `Vec<u8>` scratch laid out as C lays `scratch.data`, split at `tupledata_off`; `xl_heap_multi_insert`/`xl_multi_insert_tuple` `to_bytes`; tuple data = `heap_tuple_to_disk_image(heaptup)[SizeofHeapTupleHeader..]` (== `t_len - SizeofHeapTupleHeader`, asserted) | MATCH |
| `if (all_frozen_set) visibilitymap_set(..., InvalidXLogRecPtr, vmbuffer, InvalidTransactionId, ALL_VISIBLE\|ALL_FROZEN)` | `page_seam::visibilitymap_set(VmSetArgs{...})` | MATCH |
| `UnlockReleaseBuffer(buffer)`; `ndone += nthispage` | identical | MATCH |
| post-loop `if (vmbuffer != InvalidBuffer) ReleaseBuffer(vmbuffer)` | identical | MATCH |
| post-loop `CheckForSerializableConflictIn(...)` | identical | MATCH |
| `if (IsCatalogRelation(relation)) for i: CacheInvalidateHeapTuple(relation, heaptuples[i], NULL)` | `catalog_seam::is_catalog_relation` guard + `cache_invalidate_heap_tuple` per tuple | MATCH |
| `for i: slots[i]->tts_tid = heaptuples[i]->t_self` | return the stamped heaptuples (carry `t_self`); caller reads them directly | MATCH (carrier-adapted) |
| `pgstat_count_heap_insert(relation, ntuples)` | `pgstat_seam::pgstat_count_heap_insert(rd_id, pgstat_enabled, ntuples)` | MATCH |

## Alignment / WAL layout notes

- C builds the record in `scratch.data` (a `PGAlignedBlock`) and uses absolute
  pointer arithmetic: `tuphdr = (xl_multi_insert_tuple *) SHORTALIGN(scratchptr)`
  aligns relative to the scratch base. The port mirrors this by `SHORTALIGN`ing
  `scratch.len()` (the absolute offset within the same buffer) before each
  per-tuple header, so the byte layout — and thus `XLogRegisterBufData`'s
  payload — is identical.
- `XLogRegisterData` registers `scratch.data .. tupledata` (header + offsets),
  `XLogRegisterBufData` registers `tupledata .. scratchptr` (per-tuple headers +
  tuple data). The port slices the single scratch `Vec` at `tupledata_off`
  accordingly.
- `xl_heap_multi_insert.flags`/`ntuples` are patched into the leading 4 bytes
  after the per-tuple loop (C writes them via the `xlrec` pointer that aliases
  the scratch head); `nthispage as u16` == C's `xlrec->ntuples = nthispage`.

## Mirror-pg-and-panic boundaries

None new. Every dependency (`heap_prepare_insert`, `RelationGetBufferForTuple`,
`RelationPutHeapTuple`, `log_heap_new_cid`, `heap_tuple_to_disk_image`,
`cache_invalidate_heap_tuple`, the page/WAL/pgstat/predicate/relcache/catalog/hio
seams) is the same already-wired substrate `heap_insert` uses.

## Gate

`cargo check --workspace`, `cargo test -p no-todo-guard`,
`cargo test -p seams-init` (both recurrence guards, including
`every_declared_seam_is_installed_by_its_owner` — which now passes *because*
`heap_multi_insert` is installed), and `cargo test -p backend-access-heap-heapam`
(32 tests) all green. Allowlist entry
`("backend_access_heap_heapam", "heap_multi_insert")` removed from
`crates/seams-init/src/lib.rs`.
