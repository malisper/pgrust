# Audit: contrib-amcheck-verify-heapam

C source: `contrib/amcheck/verify_heapam.c` (2178 LOC).
Port: `crates/contrib-amcheck-verify-heapam/src/lib.rs`.

## Function inventory

| C function | C loc | Port loc | Verdict | Notes |
|---|---|---|---|---|
| `verify_heapam` (SQL SRF) | 249 | `verify_heapam` | MATCH | Arg parse (skip option), safe_xmin = GetTransactionSnapshot()->xmin, InitMaterializedSRF, relation_open + relkind/AM checks, unlogged-in-recovery early exit (DEBUG1), empty-relation exit, GetAccessStrategy(BAS_BULKREAD), block-range validation, toast open, cache xid/mxid ranges + relfrozen/relminmxid, read_stream begin (NONE → block_range cb + SEQUENTIAL\|FULL\|USE_BATCHING; else unskippable cb + DEFAULT), per-page lock+snapshot+three loops, toast check post-unlock, on_error_stop break, cleanup. Args 0-3 typed (C errors on NULL → dispatch enforces); 4-5 `Option<i64>`. |
| `heapcheck_read_stream_next_unskippable` (static) | 880 | `heapcheck_read_stream_next_unskippable` | MATCH | Loops `[current,last_exclusive)`, visibilitymap_get_status, skip on ALL_FROZEN/ALL_VISIBLE bit, else return i; InvalidBlockNumber at end. Built as the repo's `Box<dyn FnMut>` callback (folds the C `void *callback_private_data`); the VM-read error surfaces as a panic because the callback signature is infallible (mirrors C's unrecoverable smgr error). |
| `report_corruption_internal` (static) | 914 | `report_corruption_internal` | MATCH | values[0]=Int64(blkno),[1]=Int32(offnum),[2]=Int32(attnum) nulls[2]=(attnum<0),[3]=text(msg); putvalues. (C pfree(msg) = Rust drop.) |
| `report_corruption` (static) | 950 | `report_corruption` / `report` | MATCH | report_internal + is_corrupt=true. |
| `report_toast_corruption` (static) | 966 | `report_toast` | MATCH | report_internal at ta->{blkno,offnum,attnum} + is_corrupt=true. |
| `check_tuple_header` (static) | 998 | `check_tuple_header` | MATCH | t_hoff>lp_len; XMAX_COMMITTED&&XMAX_IS_MULTI; !valid(curr_xmax)&&IsHotUpdated; IsHeapOnly&&!UPDATED; expected_hoff (HASNULL → MAXALIGN(hdr+BITMAPLEN(natts)) else MAXALIGN(hdr)) with the 4 message variants; result flips on hoff/expected_hoff mismatch. |
| `check_tuple_visibility` (static) | 1111 | `check_tuple_visibility` | MATCH | xmin bounds + status switch; XminCommitted path: XminInvalid→false, MOVED_OFF/MOVED_IN xvac state machines, xmin_status!=COMMITTED→false; XMAX_IS_MULTI range check (returns checkable); XMAX_INVALID/LOCKED_ONLY → live, not prunable; multi update-xid status; plain-xid xmax status; tuple_could_be_pruned = TransactionIdPrecedes(xmax, safe_xmin) on committed delete. Returns (checkable, xmin_status_ok, xmin_status). |
| `check_toast_tuple` (static) | 1553 | `check_toast_tuple` | MATCH | last_chunk_seq=(extsize-1)/TOAST_MAX_CHUNK_SIZE; null seq; seq!=expected; null data; chunksize from VARSIZE/VARSIZE_SHORT/invalid-header; chunk_seq>last; expected_size (mid=TOAST_MAX_CHUNK_SIZE else remainder); size mismatch. fastgetattr(2/3) modeled via heap_deform_tuple columns 1/2. |
| `check_tuple_attribute` (static) | 1659 | `check_tuple_attribute` | MATCH | thisatt=CompactAttr; hoff+offset>lp_len start check; HASNULL+att_isnull skip; fixed-len align+addlength+end check; varlena pointer-align; VARATT_IS_EXTERNAL va_tag!=ONDISK guard; addlength+end check; not-external skip; rawsize>limit; compressed → method validity (PGLZ/LZ4 ok); !HASEXTERNAL; no toast rel; toast_rel None skip; push ToastedAttribute when !could_be_pruned. |
| `check_toasted_attribute` (static) | 1859 | `check_toasted_attribute` | MATCH | extsize=GET_EXTSIZE; last_chunk_seq; ScanKeyInit(1,BTEqual,F_OIDEQ,valueid); get_toast_snapshot; systable_beginscan_ordered; loop check_toast_tuple; not-found / ended-early reports. |
| `check_tuple` (static) | 1917 | `check_tuple` | MATCH | header→return; visibility→return; RelationGetDescr->natts<natts; per-attribute loop break-on-false; attnum reset -1. |
| `FullTransactionIdFromXidAndCtx` (static) | 1976 | `full_xid_from_xid_and_ctx` | MATCH | !normal→epoch0; diff = next_xid - xid (i32); corruption clamp to FirstNormalFullTransactionId; else FullTransactionIdFromU64(next - diff). |
| `update_cached_xid_range` (static) | 2018 | `update_cached_xid_range` | MATCH | next_fxid/oldest_xid (XidGenLock inside varsup getters); next_xid; oldest_fxid. |
| `update_cached_mxid_range` (static) | 2035 | `update_cached_mxid_range` | MATCH | ReadMultiXactIdRange seam → (oldest, next). |
| `fxid_in_cached_range` (static inline) | 2045 | `fxid_in_cached_range` | MATCH | oldest<=fxid<next. |
| `check_mxid_in_range` (static) | 2056 | `check_mxid_in_range` | MATCH | invalid/precedes-relmin/precedes-clustermin/in-future/ok. |
| `check_mxid_valid_in_rel` (static) | 2078 | `check_mxid_valid_in_rel` | MATCH | range check; on miss refresh + recheck. |
| `get_xid_status` (static) | 2110 | `get_xid_status` | MATCH | invalid; bootstrap/frozen→committed; bounds with refresh+reconvert; in-future/clustermin/relmin; want_status gate; cache hit; clog_horizon (XactTruncationLock inside getter) gate → IsCurrent/InProgress/DidCommit/Aborted; cache store. did_commit passes ctx.safe_xmin as TransactionXmin. |

All `static inline` macro helpers it relies on (`PageGetItemId`/`PageGetItem`/
`ItemId*`/`Heap*` accessors / varlena+att decode / FullTransactionId ops) are
ported in-crate as byte/word helpers or pulled from types-core / heapam-visibility
`htup`, matching the C inline definitions.

## Seam audit

This unit owns **no** inward seam crate: `verify_heapam` is a SQL-registered
function (`PG_FUNCTION_INFO_V1`), not a cross-crate callee, so there is nothing
for a `verify-heapam-seams` crate to declare. `init_seams()` is correctly empty.

Outward calls are thin delegates across genuine cycles, via owner seam crates
(bufmgr, visibilitymap, hio, table, genam, toast-internals, varsup, transam,
xact, procarray, multixact, snapmgr, funcapi, varlena, elog, tcop) plus direct
acyclic library deps (read-stream, buffer-support, heapam-visibility htup,
scankey, common-heaptuple deform). No branching/computation lives in a seam path.

New seam added + installed by its owner: `read_multi_xact_id_range`
(multixact-seams, installed in `backend-access-transam-multixact::init_seams`).

## Findings fixed during audit

1. **TOAST_MAX_CHUNK_SIZE off by 8 (DIVERGES → fixed).** The initial local const
   omitted the `MAXALIGN(SizeOfPageHeaderData + EXTERN_TUPLES_PER_PAGE *
   sizeof(ItemIdData))` term and `MAXALIGN_DOWN`, yielding 2004 instead of the
   correct 1996. This would mis-size every toast chunk-size / last-chunk
   validation. Re-derived the full `MaximumBytesPerTuple(4)` formula; verified
   the const now evaluates to 1996.

## Design conformance

- No invented opacity (varatt_external/ItemId/page fields decoded from real
  on-page bytes; `Datum` is the canonical unified value).
- The page is the relation's real bytes via `BufferGetPage` (an `mcx` copy);
  scratch per-page arrays are bounded stack-equivalents (C uses
  `[MaxOffsetNumber]`).
- `format!` strings are error-message construction at report sites (C psprintf +
  pfree); `.to_vec()` page/item copies mirror the C page snapshot.
- `.expect()` uses are control-flow invariants (status present on XID_BOUNDS_OK;
  resultinfo set by InitMaterializedSRF — same pattern as rmgr) plus the one
  documented infallible-callback VM-read case.
- No shared statics, no ambient-global getter seams.
- Buffer-lock window: the page content lock (`LockBuffer(BUFFER_LOCK_SHARE)`) is
  held across the per-tuple/per-chain checks, which include `?`-fallible seam
  calls (clog/procarray/multixact xid-status lookups), exactly as the C holds
  the lock across the same `ereport`-capable checks. On an error the lock is
  released by transaction abort (resowner), as in C — this is a faithful mirror,
  not a leak introduced by the port. The lock is explicitly released via
  `UnlockReleaseBuffer` on the normal path before the toast scan (the toast scan
  must run without the page lock), matching verify_heapam.c. There is no
  buffer-content-lock guard primitive in the repo to convert this to RAII without
  diverging from the C control flow.

## Verdict: PASS
