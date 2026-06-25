# Audit: backend-access-gist-build

Crate: `crates/backend-access-gist-build`
C sources: `src/backend/access/gist/gistbuild.c` (1581 LOC),
`src/backend/access/gist/gistbuildbuffers.c` (763 LOC).
Reference c2rust: `../pgrust/c2rust-runs` gist build units.

Function-by-function comparison against the PostgreSQL 18.3 C. F1 of the GiST
build tower (#299); F0 carriers landed in `types-gist` (db0ec6423).

## gistbuildbuffers.rs (node buffer engine)

| C function | Rust | Notes |
|---|---|---|
| `gistInitBuildBuffers` | `gistInitBuildBuffers` | Creates the temp `BufFile` (BufFileCreateTemp(false)). `nodeBuffersTab`/`bufferEmptyingQueue`/`buffersOnLevels`/`loadedBuffers` are the owned collections; `*Len`/`*Count` are their lengths. `buffersOnLevels` starts with one empty level (C `buffersOnLevelsLen = 1`). freeBlocks reserves 32 (C `freeBlocksLen = 32`). |
| `gistGetNodeBuffer` | `gistGetNodeBuffer` | HASH_ENTER over `nodeBuffersTab`; on miss builds an empty `SharedNodeBuffer`, enlarges `buffersOnLevels` (resize_with → empty lists, C repalloc + NIL-init), prepends to the level list (lcons → insert(0)). Returns the shared handle (C returns the `GISTNodeBuffer *`). |
| `gistAllocateNewPageBuffer` | `gistAllocateNewPageBuffer` | prev = InvalidBlockNumber, freespace = DATA_SIZE (BLCKSZ - BUFFER_PAGE_DATA_OFFSET), data area zeroed (MemoryContextAllocZero). |
| `gistAddLoadedBuffer` | `gistAddLoadedBuffer` | Skips isTemp buffers; pushes the handle (Vec grows; C repalloc *2). |
| `gistLoadNodeBuffer` | `gistLoadNodeBuffer` | Loads only when `pageBuffer==NULL && blocksCount>0`. Reads the temp block, deserializes, releases the on-disk block, adds to loaded, clears pageBlocknum. |
| `gistUnloadNodeBuffer` | `gistUnloadNodeBuffer` | When a page is loaded: get a free block, serialize+write, free the page (None), save pageBlocknum. |
| `gistUnloadNodeBuffers` | `gistUnloadNodeBuffers` | Unloads every loaded buffer, then clears the loaded array. (Snapshots the handle list first to satisfy the borrow checker; semantics identical.) |
| `gistPlaceItupToPage` | `gistPlaceItupToPage` | freespace -= MAXALIGN(itupsz); copy itup to `tupledata[freespace..freespace+itupsz]` (C `data + freespace`). |
| `gistGetItupFromPage` | `gistGetItupFromPage` | Read tuple at `tupledata[freespace..]`, copy out (palloc), freespace += MAXALIGN. |
| `gistPushItupToNodeBuffer` | `gistPushItupToNodeBuffer` | First-page creation; load if needed; spill-and-relink on PAGE_NO_SPACE (freespace reset to BLCKSZ-MAXALIGN(8), prev=blkno, blocksCount++); place; queue for emptying on BUFFER_HALF_FILLED (lcons → insert(0)). |
| `gistPopItupFromNodeBuffer` | `gistPopItupFromNodeBuffer` | Returns `Option<bytes>` (C bool + out-param). Empty→None. Load if needed; get tuple; on now-empty page decrement blocksCount and fetch the prev page (ReadTempFileBlock + release) or free the page. |
| `gistBuffersGetFreeBlock` | `gistBuffersGetFreeBlock` | Pop freelist last element, else extend (nFileBlocks++). |
| `gistBuffersReleaseBlock` | `gistBuffersReleaseBlock` | Push to freelist (Vec grows; C repalloc *2). |
| `gistFreeBuildBuffers` | `gistFreeBuildBuffers` | BufFileClose; rest freed on context release / Drop. |
| `gistRelocateBuildBuffersOnSplit` | `gistRelocateBuildBuffersOnSplit` | Full port incl. the `RelocationBufferInfo` file-local struct, the temp-copy-and-reset-original (isTemp) on the split buffer, the per-page node-buffer creation, the penalty-minimizing target selection loop (best_penalty[-1] sentinel, column tie-break, zero-penalty early-out), the push, and the downlink adjust via gistgetadjusted + gistDeCompressAtt. |
| `ReadTempFileBlock` / `WriteTempFileBlock` | same | BufFileSeekBlock (error on non-zero) + ReadExact/Write of a BLCKSZ block. |

Page byte model: `serialize_page`/`deserialize_page` lay out `prev` (4) +
`freespace` (4) at offset 0 then the DATA_SIZE data area, matching the C struct
written verbatim to a BLCKSZ temp-file block. BUFFER_PAGE_DATA_OFFSET = 8 =
MAXALIGN(offsetof(tupledata)). Macros PAGE_IS_EMPTY / PAGE_NO_SPACE /
LEVEL_HAS_BUFFERS / BUFFER_HALF_FILLED / BUFFER_OVERFLOWED ported as inline fns.

## gistbuild.rs (drivers + callbacks)

| C function | Rust | Notes |
|---|---|---|
| `gistbuild` | `gistbuild` | Empty-index check; initGISTstate; build-mode choice; sortsupport probe (index_getprocid per key, OidIsValid); fillfactor→freespace; sorted vs insert branch; buffering flush + WAL emit. Returns IndexBuildResult{heap_tuples, index_tuples}. **Divergence (documented):** `buffering_mode` is not carried on the repo relcache entry (`StdRdOptions` has only fillfactor), so the explicit on/off buffering reloption is treated as the C `GIST_BUFFERING_AUTO` default; this is the one behaviour a GiST relcache-options carrier (a relcache keystone) would restore. fillfactor read via RelationGetFillFactor. |
| `gistSortedBuildCallback` | `gistSortedBuildCallback` | gistCompressValues + tuplesort_putindextuplevalues + indtuples++. Per-tuple tempCxt rides mcx. |
| `gist_indexsortbuild` | `gist_indexsortbuild` | pages_allocated=1; smgr_bulk_start_rel; first leaf levelstate; getindextuple loop → levelstate_add; flush partial pages up the parent chain; write the root (PageSetLSN GistBuildLSN, bulk_get_buf + memcpy + bulk_write block 0); bulk_finish. |
| `gist_indexsortbuild_levelstate_add` | same | PageGetFreeSpace check vs IndexTupleSize+sizeof(ItemIdData); flush or advance current_page; gistinitpage(new) with old page flags; gistfillbuffer. |
| `gist_indexsortbuild_levelstate_flush` | same | gistextractpage page 0; join pages 1..current_page; gistSplit (multi) or single-page gistunion+gistfillitupvec layout; per-partition page build (gistinitpage, PageAddItem loop over the concatenated list, rightlink to last_blkno, assign block, PageSetLSN, bulk_write, set union t_tid block, parent downlink insert, root creation). |
| `gistInitBuffering` | `gistInitBuffering` | pageFreeSpace; itupAvgSize; itupMinSize (per-attr, MAXALIGN(IndexTupleData), VARHDRSZ for varlena, compact_attr().attlen); levelStep loop with pow()/effective_cache_size/maintenance_work_mem; fallback to DISABLED if levelStep<=0; calculatePagesPerBuffer; gistInitBuildBuffers; gistInitParentMap; ACTIVE. |
| `calculatePagesPerBuffer` | same | 2 * pow(avgIndexTuplesPerPage, levelStep); rint→i32. |
| `gistBuildCallback` | `gistBuildCallback` | gistFormTuple, set t_tid; indtuples/Size; buffering insert or gistdoinsert; periodic pagesPerBuffer readjust (% 4096); auto/stats buffering-switch check (% 256, effective_cache_size < smgrnblocks). |
| `gistBufferingBuildInsert` | same | gistProcessItup(root) + gistProcessEmptyingQueue. |
| `gistProcessItup` | `gistProcessItup` | Descent loop (LEVEL_HAS_BUFFERS/leaf breaks; ReadBuffer+LockBuffer EXCLUSIVE; gistchoose; childblkno; gistMemorizeParent when level>1; gistgetadjusted → gistbufferinginserttuples or unlock-release); leaf/buffer placement; returns the overflow stop flag. |
| `gistbufferinginserttuples` | same | gistplacetopage; root-split rootlevel++ + memorize grandchildren; splitinfo path: FindCorrectParent, RelocateBuildBuffersOnSplit, build downlink array + parent-map updates + release lower buffers, recurse to parent at level+1; else unlock-release. |
| `gistBufferingFindCorrectParent` | same | parent via gistGetParent (level>0) or supplied parentblkno (leaf, error if invalid); ReadBuffer+LockBuffer EXCLUSIVE + gistcheckpage; fast-path offset check; full scan; error on miss. |
| `gistProcessEmptyingQueue` | same | Pop queue (linitial+delete_first); queuedForEmptying=false; gistUnloadNodeBuffers; pop-and-process inner loop with the lower-buffer-full early stop. |
| `gistEmptyAllBuffers` | same | Top-to-bottom level walk; per-level loop pulling buffersOnLevels[i].first(); queue+process non-empty, delete empty. |
| `gistGetMaxLevel` | same | Root-down traversal, LockBuffer SHARE pro-forma, first downlink follow, depth count. (Takes `mcx` as the page-snapshot context; C reads the live page pointer.) |
| `gistInitParentMap` / `gistMemorizeParent` / `gistMemorizeAllDownlinks` / `gistGetParent` | same | parentMap = `HashMap<BlockNumber,BlockNumber>` (C HTAB keyed by childblkno). gistGetParent errors when not found. |
| `gistbuildempty` (gist.c:139) | `gistbuildempty` | ExtendBufferedRel(INIT_FORKNUM, EB_SKIP_EXTENSION_LOCK|EB_LOCK_FIRST) via the seam; crit section; GISTInitBuffer F_LEAF; MarkBufferDirty; log_newpage_buffer; UnlockReleaseBuffer. |

## Memory model

Per-tuple `giststate->tempCxt` scratch rides the threaded `mcx` (sibling
spgbuild/brinbuild convention); `createTempGistContext` / `freeGISTstate` =
the mcx lifetime + Drop. Durable data goes to shared buffer pages / the bulk
writer, so eliding the per-tuple reset is behavior-preserving.

`GISTNodeBuffer` aliasing across nodeBuffersTab / bufferEmptyingQueue /
buffersOnLevels / loadedBuffers carried via `SharedNodeBuffer =
Rc<RefCell<GISTNodeBuffer>>` (sanctioned shared carrier, F0).

## Seam-and-panic (sanctioned, 1:1)

- `tuplesort_begin_index_gist` / `tuplesort_putindextuplevalues` /
  `tuplesort_performsort` / `tuplesort_getindextuple` / `tuplesort_end`
  (backend-utils-sort-tuplesort-seams): DECLARED, UNINSTALLED until the
  tuplesort tower (#310) lands. The sorted-build leg loud-panics on call. NOT
  stubbed. The insert/buffering build path (the common case) is fully live.

All other neighbors (bufmgr, bulk_write, buffile, xloginsert, smgr, indexam,
tableam, relcache, vacuumlazy maintenance_work_mem, guc effective_cache_size,
miscinit crit section, parallel-rt CHECK_FOR_INTERRUPTS) call real installed
seams or real gist-core helpers.

## Wiring

Leaf consumer: owns no inward seams → no `-seams` crate, no `init_seams()`.
`gistbuild`/`gistbuildempty` are plain `pub fn`s matching the #307 build-scan
contract; the index.c owner that would dispatch them via the IndexAmRoutine
`ambuild`/`ambuildempty` slots is unported, so they are exported but not yet
vtable-wired — exactly like hashbuild/spgbuild/brinbuild.

residual_own_todos = 0. No todo!/unimplemented!. 2 unit tests (page-byte
offsets + IndexTupleSize bit extraction).
