# Audit: backend-access-gin-ginutil (ginutil.c)

C source: `src/backend/access/gin/ginutil.c` (PostgreSQL 18.3, 731 LOC).
Port: `crates/backend-access-gin-ginutil/src/lib.rs`.
Carriers: `types_gin::*` (GinState with real `FmgrInfo` Vecs + owned `TupleDesc`,
GinMetaPageData/GinPageOpaqueData/GinStatsData/GinOptions/GinNullCategory),
canonical `types_tuple` `Datum`, real `types_rel::Relation`.

## Function inventory (every definition in ginutil.c)

| # | C fn (line) | port location | verdict | notes |
|---|---|---|---|---|
| 1 | `ginhandler` (37) | `ginhandler` | MATCH | All 24 property flags verified vs C 42-66: amstrategies=0, amsupport=GINNProcs(7), amoptsprocnum=GIN_OPTIONS_PROC(7), amcanmulticol=true, amoptionalkey=true, amsearchnulls=false, amstorage=true, amclusterable=false, ampredlocks=true, amcanparallel=false, amcanbuildparallel=true, amcaninclude=false, amusemaintenanceworkmem=true, amsummarizing=false, amparallelvacuumoptions=BULKDEL\|CLEANUP(0x06), amkeytype=InvalidOid. Callbacks: aminsertcleanup=None (C:71 NULL), amgettuple=None (C:84 NULL, bitmap-only), amgetbitmap=Some, amcanreturn=None (C:74), ammarkpos/amrestrpos=None (C:87/88), parallel trio=None (C:89-91). ginvalidate/ginadjustmembers/ginbuildphasename/ambuild/ambuildempty/amcostestimate/amoptions are reached by name (not fields on the unified IndexAmRoutine), matching the landed brin/bt handlers. amvalidate=None (soft-error result, reached by name) — same convention as brinhandler. |
| 2 | `initGinState` (102) | `initGinState` | MATCH | Control flow 1:1: state zeroed (`GinState::new`), index/oneCol(`natts==1`)/origTupdesc set, per-attr loop. oneCol→share descr (owned: deep-copy via CreateTupleDescCopy); else CreateTemplateTupleDesc(2) + TupleDescInitEntry(1,NULL,INT2OID(21),-1,0) + TupleDescInitEntry(2,NULL,atttypid,atttypmod,attndims) + TupleDescInitEntryCollation(2,attcollation). compareFn: `index_getprocid!=Invalid` → index_getprocinfo else lookup cmp_proc_finfo fallback with `ERRCODE_UNDEFINED_FUNCTION` "could not identify a comparison function for type %s". extract{Value,Query}Fn always. tri/consistent OidIsValid checks; missing→`elog(ERROR) "missing GIN support function (%d or %d) for attribute %d of index \"%s\""` with GIN_CONSISTENT_PROC(4)/GIN_TRICONSISTENT_PROC(6) and i+1. comparePartial→canPartialMatch. supportCollation = rd_indcollation[i] or DEFAULT_COLLATION_OID(100). Catalog/relcache/typcache substrate SEAMED (see §seams). |
| 3 | `gintuple_get_attrnum` (231) | `gintuple_get_attrnum` | MATCH | oneCol→FirstOffsetNumber(1); else index_getattr(tuple,First,tupdesc[0]), assert !isnull, DatumGetUInt16, assert First<=colN<=natts (natts read off origTupdesc, as in C which reads ginstate->origTupdesc->natts). index_getattr deform SEAMED. |
| 4 | `gintuple_get_key` (264) | `gintuple_get_key` | MATCH | oneCol→index_getattr(First,origTupdesc); else colN=gintuple_get_attrnum, index_getattr(OffsetNumberNext(First)=2, tupdesc[colN-1]). isnull→GinGetNullCategory(seam: tuple+oneCol — the C macro reads only itup->t_info + oneCol) else GIN_CAT_NORM_KEY(0). |
| 5 | `GinNewBuffer` (305) | `GinNewBuffer` | SEAMED | The FSM-recycle/extend loop (`GetFreeIndexPage`+`ConditionalLockBuffer`+`GinPageIsRecyclable`[reads pd_prune_xid vs GlobalVis — transam]+`ExtendBufferedRel` EB_LOCK_FIRST) is buffer-cache substrate across a real cycle; routed whole through `gin_new_buffer`, preserving pin/lock order. Returns pinned+xlocked buffer. |
| 6 | `GinInitPage` (343) | `GinInitPage` | MATCH | PageInit(page,size,sizeof(GinPageOpaqueData)); opaque.flags=f, rightlink=InvalidBlockNumber, maxoff=0. Pure page bytes (nbtsort pattern; special-area write at pd_special). |
| 7 | `GinInitBuffer` (355) | `GinInitBuffer` | MATCH | GinInitPage over the buffer's page bytes at BufferGetPageSize (BLCKSZ fallback for size==0). |
| 8 | `GinInitMetabuffer` (361) | `GinInitMetabuffer` | MATCH | GinInitPage(GIN_META); all-empty GinMetaPageData (head/tail=Invalid, counters 0, ginVersion=GIN_CURRENT_VERSION=2) written field-by-field at C byte offsets; pd_lower set to PageGetContents + sizeof(GinMetaPageData)=24+56=80. sizeof verified=56 (test). |
| 9 | `ginCompareEntries` (393) | `ginCompareEntries` | MATCH | categorya!=categoryb → ±1 by ordering; same non-NORM category → 0; both norm → compareFn[attnum-1] via fmgr seam. |
| 10 | `ginCompareAttEntries` (415) | `ginCompareAttEntries` | MATCH | attnuma!=attnumb → ±1; else ginCompareEntries. |
| 11 | `cmpEntries` (448, static) | `cmp_entries` | MATCH | NULL"="NULL=0, NULL">"notNULL=1, notNULL"<"NULL=-1, else compareFn. haveDups (res==0) recorded by the caller's sort closure (mirrors `arg.haveDups` out-flag); the dedup pass re-invokes cmp_entries exactly as C re-calls cmpEntries in the dup loop. |
| 12 | `ginExtractEntries` (488) | `ginExtractEntries` | MATCH | isNull→1 placeholder GIN_CAT_NULL_ITEM(3). Else extractValueFn (seam) → None/empty entries → 1 placeholder GIN_CAT_EMPTY_ITEM(2). nullFlags absent→all-false. nentries>1 → build keydata, qsort_arg(cmpEntries)+haveDups, dedup or straight copy, truncate. categories from nullFlags: GIN_CAT_NULL_KEY(1)/GIN_CAT_NORM_KEY(0). |
| 13 | `ginoptions` (607) | `ginoptions` | MATCH | tab=[{fastupdate,BOOL,offsetof(useFastUpdate)},{gin_pending_list_limit,INT,offsetof(pendingListCleanupSize)}], build_reloptions(RELOPT_KIND_GIN, sizeof(GinOptions)). GinOptions is repr(C) with vl_len_ first so offsets match C. |
| 14 | `ginGetStats` (628) | `ginGetStats` | SEAMED+MATCH | Metapage ReadBuffer/GIN_SHARE/UnlockRelease SEAMED (gin_get_stats returns GinMetaPageData); the 6-field copy into GinStatsData (incl nPendingPages, ginVersion) is in-crate, 1:1. |
| 15 | `ginUpdateStats` (655) | `ginUpdateStats` | SEAMED | Metapage GIN_EXCLUSIVE + crit section + 4-field copy + pd_lower reset + MarkBufferDirty + WAL(XLOG_GIN_UPDATE_META_PAGE before unlock when RelationNeedsWAL && !is_build) + UnlockRelease is buffer/WAL substrate routed whole through gin_update_stats. Only the 4 planner fields (nTotalPages/nEntryPages/nDataPages/nEntries) cross; nPendingPages/ginVersion NOT passed (matches C "are *not* copied over"). |
| 16 | `ginbuildphasename` (712) | `ginbuildphasename` | MATCH | 6 phase strings (1..6) + default None. Strings byte-identical to C. |

Struct/typedef inventory: `keyEntryData` → `KeyEntryData` (datum+isnull) MATCH; `cmpEntriesArg`
(cmpDatumFunc/collation/haveDups) modeled as fn params + the sort closure's `have_dups` MATCH.

## Constants verified against C headers

GINNProcs=7, GIN_{COMPARE,EXTRACTVALUE,EXTRACTQUERY,CONSISTENT,COMPARE_PARTIAL,TRICONSISTENT,OPTIONS}_PROC
=1..7, GIN_META=1<<3, GIN_METAPAGE_BLKNO=0, GIN_CURRENT_VERSION=2, GIN_CAT_{NORM_KEY=0,NULL_KEY=1,EMPTY_ITEM=2,NULL_ITEM=3},
INT2OID=21, DEFAULT_COLLATION_OID=100, F_GINHANDLER=333, GIN_AM_OID=2742,
VACUUM_OPTION_PARALLEL_{BULKDEL=1<<1,CLEANUP=1<<2}, sizeof(GinMetaPageData)=56 (verified via field
offsets + test), PageGetContents offset=MAXALIGN(24)=24, progress sub-phases 1..6.

## Seam audit

ginutil.c maps to `backend-access-gin-ginutil-seams`. ginutil's `init_seams()` is **empty** — it
owns **no inward seams**. Every declaration in `backend-access-gin-ginutil-seams` is an **outward**
call whose body lives in another owner; ginutil is merely the first cyclic caller (it assembles the
AM dispatch vector and performs the catalog/buffer/fmgr-dependent utility routines). This is the
established, audited GIN-family pattern: `backend-access-gin-core-probe-seams` holds the probe's
outward fmgr consistent-call seams and `gin-core-probe::init_seams()` is likewise empty ("owns no
inward seams"). The repo seam-wiring guard (`seams-init` recurrence_guard) passes: both
`every_seam_installing_crate_is_wired_into_init_all` and `every_declared_seam_is_installed_by_its_owner`
are green, because these outward seams' owners are unported (genuinely-blocked, mirror-and-panic).

Outward seams (all justified by a real dependency cycle: ginutil cannot depend on these owners
without a cycle, and each owner is unported):

- catalog/relcache/typcache: `gin_relation_get_descr`, `gin_relation_get_relation_name`,
  `gin_lookup_cmp_proc_finfo` (the `cmp_proc_finfo` fallback — not on the trimmed relcache
  `TypeCacheEntry`). The opclass-proc OID resolution itself uses the *already-installed* relcache
  seams `index_getprocid`/`index_getprocinfo`/`rd_indcollation` directly (no new seam) — matching
  the landed hash AM.
- fmgr GIN dispatch: `gin_extract_value` (FunctionCall3Coll extractValueFn), `gin_compare_entries`
  (FunctionCall2Coll compareFn). Marshal+delegate only.
- index-tuple deform: `gin_index_getattr`, `gin_get_null_category` (GinGetNullCategory; args = tuple
  bytes + oneCol, the only inputs the C macro reads).
- buffer/WAL substrate: `gin_new_buffer`, `gin_get_stats`, `gin_update_stats`.
- AM vtable callbacks (reached by name through the vtable only; owners ginscan/ginget/gininsert/
  ginvacuum unported): `gininsert`, `ginbulkdelete`, `ginvacuumcleanup`, `ginbeginscan`,
  `ginrescan`, `ginendscan`, `gingetbitmap`. The handler adapters are thin marshal+delegate.

No branching/node-construction/computation in any seam path. No seam-installed logic that belongs
in-crate (the page-byte init, all comparison/extraction control flow, the OidIsValid branching of
initGinState, the ginoptions table, ginbuildphasename are all in-crate).

## Design conformance

- Carriers are the real `types_gin` types (no invented opacity): real `FmgrInfo` arrays, owned
  `TupleDesc`, canonical `Datum`, real `Relation`. `GinOptions` widened to `#[repr(C)]` + `vl_len_`
  so `offset_of!` matches the C reloption layout (nothing else consumed the prior shape).
- All allocating fns take `Mcx<'mcx>` and return `PgResult` (initGinState, gintuple_get_*,
  ginExtractEntries, ginoptions, clone_tupdesc).
- No shared statics, no ambient-global seams, no locks held across `?` (GinNewBuffer's pinned buffer
  is returned by the seam; ginGetStats/ginUpdateStats's lock lifetime is owned by the substrate seam).
- New `AmOpaque` tag `GIN_SCAN` added for the eventual GinScanOpaqueData carrier (consumed by L1b
  ginscan); `F_GINHANDLER` added to amapi `GetIndexAmRoutine` dispatch.

## Verdict: PASS

All 16 functions MATCH or SEAMED per §3 rules. Zero seam findings (every seam is a justified,
thin, cyclic outward call; ginutil owns no inward seams, matching the audited gin-core-probe
precedent). Constants verified against headers. Build green; 5 in-crate unit tests pass; the audited
gin-core-probe lane (21 tests) is not regressed.
