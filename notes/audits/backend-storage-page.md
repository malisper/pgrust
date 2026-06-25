# Audit: backend-storage-page

- **Verdict: PASS**
- Date: 2026-06-13
- Model: Opus 4.8 (1M context) (claude-opus-4-8[1m])
- Branch: `port/backend-storage-page`
- C sources: `src/backend/storage/page/bufpage.c`, `src/backend/storage/page/itemptr.c`
- c2rust: `c2rust-runs/backend-storage-page/src/{bufpage,itemptr}.rs`
- Port: `crates/backend-storage-page/src/lib.rs`

This is an **independent** function-by-function re-derivation from the Postgres C
and the c2rust translation. Every function definition in the two `.c` files is
enumerated; inline accessors from the relied-on headers (`bufpage.h`,
`itemptr.h`, `itemid.h`, `block.h`) are also ported and checked.

## 1. Function inventory & verdicts

### itemptr.c

| C function | C loc | Port loc | Verdict | Notes |
|---|---|---|---|---|
| `StaticAssertDecl(sizeof(ItemPointerData)==6)` | itemptr.c:23 | — | N/A | compile-time assert; `ItemPointerData` layout enforced in `types-tuple` |
| `ItemPointerEquals` | itemptr.c:35 | lib.rs:141 | MATCH | block && offset equality, identical |
| `ItemPointerCompare` | itemptr.c:51 | lib.rs:147 | MATCH | uses `…NoCheck` getters (no posid!=0 assert); -1/0/1 ladder identical |
| `ItemPointerInc` | itemptr.c:84 | lib.rs:168 | MATCH | `off==PG_UINT16_MAX (0xffff)` -> if `blk!=InvalidBlockNumber(0xffffffff)` wrap; else `off++`. Identical. |
| `ItemPointerDec` | itemptr.c:114 | lib.rs:183 | MATCH | `off==0` -> if `blk!=0` borrow to `0xffff`/`blk--`; else `off--`. Relies on FirstOffsetNumber==1; identical. |

### bufpage.c

| C function | C loc | Port loc | Verdict | Notes |
|---|---|---|---|---|
| `PageInit` | bufpage.c:42 | lib.rs:706 | MATCH | MAXALIGN(specialSize); pageSize==BLCKSZ + `pageSize > specialSize+SizeOfPageHeaderData` asserts -> `PgResult` errors; MemSet 0; sets flags/lower/upper/special/pagesize-version. pd_prune_xid zeroed by fill. |
| `PageIsVerified` | bufpage.c:93 | lib.rs:735 | MATCH | checksum-failure out-param -> tuple return; header-sane predicate bit-identical (`pd_flags&~PD_VALID_FLAG_BITS==0 && lower<=upper<=special<=BLCKSZ && special==MAXALIGN`); all-zeroes early-out; PIV_LOG_WARNING/LOG ereport w/ ERRCODE_DATA_CORRUPTED and same format; PIV_IGNORE_CHECKSUM_FAILURE path. |
| `PageAddItemExtended` | bufpage.c:192 | lib.rs:804 | MATCH | corrupted-pointer check uses no-MAXALIGN/PANIC variant (matches C); limit=maxoff+1; OVERWRITE / shuffle / free-slot-scan / no-hint branches; `> limit` and heap-bound WARNINGs; signed lower/upper fit test; memmove shuffle; ItemIdSetNormal; memcpy; header adjust. EREPORT(ERROR)-disallowed honored (only WARNING/PANIC). |
| `PageGetTempPage` | bufpage.c:363 | lib.rs:912 | MATCH | palloc(pageSize) -> owned `PageTemp::new`, uninitialized. |
| `PageGetTempPageCopy` | bufpage.c:380 | lib.rs:918 | MATCH | palloc + memcpy whole page -> PageTemp copy. |
| `PageGetTempPageCopySpecial` | bufpage.c:400 | lib.rs:927 | MATCH | PageInit w/ same special size + copy special area (`pd_special..pageSize`). |
| `PageRestoreTempPage` | bufpage.c:422 | lib.rs:938 | MATCH | memcpy temp->old of pageSize; pfree(temp) modeled by consuming `PageTemp`. |
| `compactify_tuples` (static) | bufpage.c:472 | lib.rs:957 | MATCH | C presorted/non-presorted paths are pure optimizations with identical output: pack each kept tuple from pd_special downward in `itemidbase` order, set `lp->lp_off=upper`, set `pd_upper=upper`. Port stages bytes in captured copies (the C scratch-buffer invariant), packs in order, rewrites line pointers via `offsetindex+1`. Behaviorally identical on every input. |
| `PageRepairFragmentation` | bufpage.c:697 | lib.rs:980 | MATCH | full-MAXALIGN/ERROR paranoia check; collect used+storage lps, corrupted-lp ERROR (`itemoff<pd_upper \|\| >=pd_special`), aligned totallen; unused -> ItemIdSetUnused + nunused++; empty -> pd_upper=pd_special; else totallen-overflow ERROR + compactify; trailing-unused lower truncation; hint set/clear. `presorted` tracking dropped (selects an identical-output C path only). Working buffer charged to a per-call `MemoryContext` reserved to `nline` (C fixed `itemidbase[MaxHeapTuplesPerPage]`). |
| `PageTruncateLinePointerArray` | bufpage.c:833 | lib.rs:1074 | MATCH | back-to-front scan; countdone/sethint logic; never truncates last item (`i>FirstOffsetNumber` guard); pd_lower -= 4*nunusedend; CLOBBER_FREED_MEMORY 0x7F poison behind `clobber_freed_memory` feature (off by default, matching `#ifdef`); hint set/clear. |
| `PageGetFreeSpace` | bufpage.c:906 | lib.rs:1120 | MATCH | signed upper-lower; `<sizeof(ItemIdData)` -> 0 else minus one lp. |
| `PageGetFreeSpaceForMultipleTuples` | bufpage.c:933 | lib.rs:1130 | MATCH | signed; `< ntups*4` -> 0 else minus ntups lps. |
| `PageGetExactFreeSpace` | bufpage.c:957 | lib.rs:1141 | MATCH | signed; `<0` -> 0. |
| `PageGetHeapFreeSpace` | bufpage.c:990 | lib.rs:1152 | MATCH | PageGetFreeSpace>0 -> if nline>=MaxHeapTuplesPerPage: hint-set confirm-free-lp scan (space=0 if none) else space=0. |
| `PageIndexTupleDelete` | bufpage.c:1050 | lib.rs:1183 | MATCH | full-MAXALIGN/ERROR check; offnum bounds elog(ERROR); corrupted-lp check; MAXALIGN size; linp shift back one slot (nbytes from pd_lower); tuple-data memmove forward; pd_upper+=size, pd_lower-=4; remaining-lp offset adjust (`lp_off<=offset` -> +=size). |
| `PageIndexMultiDelete` | bufpage.c:1159 | lib.rs:1253 | MATCH | nitems<=2 -> reverse retail PageIndexTupleDelete (no ctx); else full check; build keep-list w/ corrupted-lp ERROR, offsetindex=nused; out-of-order `nextitm!=nitems` elog(ERROR); totallen-overflow ERROR; overwrite linps; pd_lower=hdr+nused*4; compactify or pd_upper=pd_special. Two working buffers (`itemidbase`,`newitemids`) charged to per-call `MemoryContext` reserved to `nline` (C fixed `[MaxIndexTuplesPerPage]` arrays). `presorted` tracking dropped (identical-output). |
| `PageIndexTupleDeleteNoCompact` | bufpage.c:1293 | lib.rs:1361 | MATCH | full check; bounds; corrupted-lp; MAXALIGN size; `offnum<nline` -> ItemIdSetUnused else pd_lower-=4 + nline--; tuple-data forward memmove; pd_upper+=size; remaining-lp adjust gated on `has_storage && lp_off<=offset`. |
| `PageIndexTupleOverwrite` | bufpage.c:1403 | lib.rs:1423 | MATCH | full check; bounds; corrupted-lp; `alignednewsize > oldsize+(upper-lower)` -> false; size_diff=oldsize-alignednewsize; relocate-before-target memmove + pd_upper+=diff + line-pointer adjust (`has_storage && lp_off<=offset`, BRIN allowed); set lp_off=offset+diff, lp_len=newsize; copy newtup. Returns Ok(true)/Ok(false). |
| `PageSetChecksumCopy` | bufpage.c:1508 | lib.rs:1502 | MATCH | PageIsNew or !DataChecksumsEnabled -> unchanged copy; else copy + pd_checksum=pg_checksum_page. C process-static scratch -> owned `PageTemp` (idiomatic; same observable bytes). |
| `PageSetChecksumInplace` | bufpage.c:1541 | lib.rs:1514 | MATCH | PageIsNew or !DataChecksumsEnabled -> return; else pd_checksum=pg_checksum_page in place. |

### Ported inline header accessors (all MATCH)

`block.h`: BlockIdSet/Get. `itemptr.h`: ItemPointerIsValid, Get/SetBlockNumber(/NoCheck),
Get/SetOffsetNumber(/NoCheck), ItemPointerSet, Copy, SetInvalid,
IndicatesMovedPartitions, SetMovedPartitions. `itemid.h`: ItemIdGet{Length,Offset,
Flags,Redirect}, ItemIdIs{Used,Normal,Redirected,Dead}, ItemIdHasStorage,
ItemIdSet{Unused,Normal,Redirect,Dead}, ItemIdMarkDead. `bufpage.h`:
PageXLogRecPtrGet/Set, PageIs{Empty,New,Full,AllVisible}, PageGetPageSize,
PageGetPageLayoutVersion, PageSetPageSizeAndVersion, PageGetSpecialSize,
PageGetMaxOffsetNumber, PageGet/SetItemId, PageGetContents, PageGetSpecialPointer
(promotes the C asserts to real range checks), PageGetItem (rejects no-storage
lp), PageGet/SetLSN, PageHas/Set/ClearHasFreeLinePointers, PageIs/Set/ClearFull,
PageIs/Set/ClearAllVisible, PageClear/SetPrunable. Helper `TransactionIdPrecedes`/
`TransactionIdIsNormal` (FirstNormalTransactionId=3) used by PageSetPrunable —
modulo-2^32 signed-diff comparison faithful to transam.

## 2. Constant verification (against PG 18.3 headers, not memory)

Confirmed in `types-storage::bufpage` / sources:
PD_HAS_FREE_LINES=0x0001, PD_PAGE_FULL=0x0002, PD_ALL_VISIBLE=0x0004,
PD_VALID_FLAG_BITS=0x0007, PG_PAGE_LAYOUT_VERSION=4, PAI_OVERWRITE=1<<0,
PAI_IS_HEAP=1<<1, PIV_LOG_WARNING=1<<0, PIV_LOG_LOG=1<<1,
PIV_IGNORE_CHECKSUM_FAILURE=1<<2, MaxOffsetNumber=BLCKSZ/sizeof(ItemIdData),
MovedPartitionsOffsetNumber=0xfffd, MovedPartitionsBlockNumber=InvalidBlockNumber
(0xFFFFFFFF), PG_UINT16_MAX=0xffff, FirstOffsetNumber=1, InvalidOffsetNumber=0,
sizeof(ItemIdData)=4. All match.

## 3. Seam audit

**Owned inward seam crates: none.** Ownership is by C-source coverage; no
`crates/X-seams` maps to `bufpage.c` or `itemptr.c` (no `backend-storage-page-seams`,
`bufpage-seams`, or `itemptr-seams` exists). The page code is self-contained
arithmetic with no consumer calling back into it across a cycle on the current
frontier. Therefore `init_seams()` is correctly a **no-op** — not an empty
installer with outstanding declarations.

**Outward seam:** `DataChecksumsEnabled()` is called via
`backend_access_transam_xlog_seams::data_checksums_enabled::call()` at lib.rs:743,
1504, 1515. This GUC/control-file read is owned by xlog (far up the spine);
this low-level storage leaf cannot take xlog as a direct dep (cycle), so the
outward seam is justified. Each call is a thin delegate (one call, bool result,
no branching/computation in the seam path) — no finding. Installation of that
seam is xlog's responsibility, not this unit's.

## 3b. Design conformance

- Allocating functions (`PageRepairFragmentation`, `PageIndexMultiDelete`,
  `PageGetTempPage*`, `PageSetChecksumCopy`) own a per-call `MemoryContext` and
  return `PgResult` — conforms to the Mcx+PgResult rule. Working buffers are
  reserved against the page-validated `nline` bound (faithful to C's fixed
  `[MaxHeapTuplesPerPage]`/`[MaxIndexTuplesPerPage]` stack arrays); no unbounded
  growth.
- No invented opacity: `Page`/`PageHeader` (a `char*` overlay) becomes a real
  byte-slice view (`PageRef`/`PageMut`) at fixed native-endian offsets; `ItemId`
  lvalue stores are modeled as explicit read/modify/write-back (`PageGetItemId`
  copy + `PageSetItemId`), documented at the call sites.
- No shared statics for per-backend globals; no ambient-global seams; no locks
  across `?`; no registry-shaped side tables.
- Divergence from C control flow: only the dropped `presorted` bookkeeping in
  `PageRepairFragmentation`/`PageIndexMultiDelete` + the unified `compactify_tuples`
  core, which collapse C's two equal-output code paths into the slow-but-faithful
  one. Output is provably identical on every input; ledgered here.
- CLOBBER_FREED_MEMORY modeled by the off-by-default `clobber_freed_memory`
  Cargo feature, mirroring the C `#ifdef`.

## 4. Verdict

Every function MATCH (or correctly SEAMED). Zero seam findings. Zero design
findings. `cargo test -p backend-storage-page`: 21 passed.

**PASS.**
