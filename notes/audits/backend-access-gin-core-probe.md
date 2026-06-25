# Audit: backend-access-gin-core-probe

C sources: `ginarrayproc.c`, `ginlogic.c`, `ginpostinglist.c`, `ginvalidate.c`
(PostgreSQL 18.3). Independent re-derivation from the C; build was green.

## Function inventory & verdicts

### ginarrayproc.c

| C function | C loc | Port | Verdict | Notes |
|---|---|---|---|---|
| `ginarrayextract` | :33 | ginarrayproc.rs `ginarrayextract` | MATCH | get_typlenbyvalalign + deconstruct_array SEAMED (lsyscache/arrayfuncs, real externals); split into elems/nulls; nkeys=nelems. |
| `ginarrayextract_2args` | :68 | `ginarrayextract_2args` | MATCH | `nargs < 3` → elog(ERROR) internal; else delegate. |
| `ginqueryarrayextract` | :79 | `ginqueryarrayextract` | MATCH | strategy→searchMode switch (overlap/contains/contained/equal) identical incl. nelems>0 branches; default → elog(ERROR). |
| `ginarrayconsistent` | :142 | `ginarrayconsistent` | MATCH | all four strategy arms, recheck assignments, loop break conditions identical; default elog. |
| `ginarraytriconsistent` | :226 | `ginarraytriconsistent` | MATCH | ternary arms identical incl. MAYBE-only-sets-r-when-FALSE in overlap; default elog. |

### ginlogic.c

| C function | C loc | Port | Verdict | Notes |
|---|---|---|---|---|
| `trueConsistentFn` | :49 | ginlogic.rs `trueConsistentFn` | MATCH | clears recheck, returns true. |
| `trueTriConsistentFn` | :55 | `trueTriConsistentFn` | MATCH | returns GIN_TRUE. |
| `directBoolConsistentFn` | :64 | `directBoolConsistentFn` | MATCH | presets recheck=true then FunctionCall8Coll SEAMED (gin_consistent_call_bool, fmgr external). |
| `directTriConsistentFn` | :88 | `directTriConsistentFn` | MATCH | FunctionCall7Coll SEAMED (gin_consistent_call_tri). |
| `shimBoolConsistentFn` | :107 | `shimBoolConsistentFn` | MATCH | MAYBE→true+recheck; else recheck=false and `!= 0`. |
| `shimTriConsistentFn` | :147 | `shimTriConsistentFn` | MATCH | MAYBE-index collection, >MAX bail, all-FALSE seed, twiddle loop, `i==nmaybe` break, recheck accumulation, TRUE+recheck→MAYBE, entryRes restore — all identical. bool→GinTernaryValue implicit conversion made explicit via bool_to_tri (provably identical). |
| `ginInitConsistentFunction` | :226 | `ginInitConsistentFunction` | MATCH | EVERYTHING→true fns; else OID/collation wiring at attnum-1 and OidIsValid → Direct/Shim selection. |

C function pointers `boolConsistentFn`/`triConsistentFn` modeled as dispatch
tags; `callBoolConsistentFn`/`callTriConsistentFn` reproduce the indirect call
(extra dispatch helpers, behavior-identical).

### ginpostinglist.c

| C function | C loc | Port | Verdict | Notes |
|---|---|---|---|---|
| `itemptr_to_uint64` | :86 | ginpostinglist.rs | MATCH | blkno<<11\|offnum. |
| `uint64_to_itemptr` | :101 | | MATCH | inverse, 11-bit mask. |
| `encode_varbyte` | :114 | | MATCH | cursor advance → Vec push. |
| `decode_varbyte` | :132 | | MATCH | 7-level continuation-bit chain identical. |
| `ginCompressPostingList` | :196 | | MATCH | SHORTALIGN_DOWN(maxsize), maxbytes, the `endptr-ptr >= MaxBytesPerInteger` fast path vs buf-fits check, totalpacked start=1, nbytes/padding/nwritten. palloc(maxsize) modeled as exact on-disk image (size == SizeOfGinPostingList ≤ maxsize; behavior-identical for the segment walk). |
| `ginPostingListDecode` | :283 | | MATCH | len = header + SHORTALIGN(nbytes). |
| `ginPostingListDecodeAllSegments` | :296 | | MATCH | nallocated guess, segment walk via GinNextPostingListSegment, first item + delta loop; repalloc → Vec growth. |
| `ginPostingListDecodeAllSegmentsToTbm` | :357 | | MATCH | tbm_add_tuples(items, ndecoded, false) SEAMED (tidbitmap external, bulk seam). |
| `ginMergeItemPointers` | :377 | | MATCH | disjoint fast paths both directions, merge with dedup on cmp==0. |
| `ginCompareItemPointers` (gin_private.h:495) | header | | MATCH | (blkno<<32)\|offnum, pg_cmp_u64. |

### ginvalidate.c

| C function | C loc | Port | Verdict | Notes |
|---|---|---|---|---|
| `ginvalidate` | :30 | ginvalidate.rs `ginvalidate` | MATCH | opckeytype fallback to opcintype; per-proc signature switch (all 7 GIN procs + default) with exact arg-type vectors; left==right check; amproclefttype!=opcintype continue; operator strategy 1..63, ORDER-BY, signature checks; group loop; 1..=GINNProcs completeness with optional/consistent continues; missing-consistent `(1<<...)` int-shift check. ereport(INFO) via error seam, result=false accumulation identical. cache-miss → Err. Catalog substrate SEAMED. |
| `ginadjustmembers` | :268 | `ginadjustmembers` | MATCH | operators all soft-family; functions: EXTRACTVALUE/EXTRACTQUERY hard, others soft-family, default ereport(ERROR) ERRCODE_INVALID_OBJECT_DEFINITION. |

## Seam audit

This unit is on the live frontier (`ported`, not yet merged), so its outward
calls into not-yet-ported owners legitimately panic (mirror-pg-and-panic).

- **Outward calls (all justified externals, thin marshal+delegate):**
  `lsyscache_seams::get_typlenbyvalalign`, `arrayfuncs_seams::deconstruct_array`
  (array/catalog runtime); `nodes_core_seams::tbm_add_tuples` (tidbitmap, bulk
  variant added to the tidbitmap owner crate); `syscache/lsyscache/regproc/
  amvalidate/error` validator substrate (identical homing to sibling
  hashvalidate). No logic in any seam path.
- **Owned inward seams:** the `backend-access-gin-core-probe-seams` crate
  declares `gin_consistent_call_bool` / `gin_consistent_call_tri`. These are
  the fmgr `FunctionCall8Coll`/`FunctionCall7Coll` into opclass consistent
  support functions — OWNED by the (unported) fmgr GIN-call dispatcher, not by
  this unit. Per the seams-init `every_declared_seam_is_installed_by_its_owner`
  guard, a non-COMPLETE owner is exempt; the guard passes. `init_seams()` is
  correctly empty (this unit installs no seam itself).
- `seams-init::init_all()` calls `backend_access_gin_core_probe::init_seams()`.
  Both recurrence-guard tests pass.

## Design conformance

- No invented opacity: `StrategyNumber` reuses `types_scan`; `ItemPointerData`/
  `Datum`/`Oid` are canonical; row types reuse `types_hash` validator shapes;
  `GinScanKey` is a runtime model (not ABI) in `types-tsearch`.
- `OpclassForm` extended with the real `opckeytype` column (was missing; GIN
  compare-proc signature checks require it) — syscache projection + hashvalidate
  test updated; additive, no divergence.
- No `Mcx`-less allocating seams: the allocating externals (`deconstruct_array`,
  `format_procedure`, `get_opfamily_name`, `identify_opfamily_groups`) all take
  `Mcx` + return `PgResult`. The pure byte/Vec codec in ginpostinglist mirrors
  C palloc with infallible `Vec` (the C path likewise aborts on OOM; bounded by
  the input list which is itself in memory).
- `GinState` is a fixed POD array (no shared static, no ambient global), matching
  C's in-`GinState` fixed arrays.
- No `todo!`/`unimplemented!`/divergence markers (greps clean).

## Verdict: PASS

All functions MATCH or SEAMED (per the rules). Zero seam findings. 21 unit
tests pass; `cargo check --workspace` and both seams-init recurrence guards
pass.

## Independent re-audit (2026-06-13, Claude Opus 4.8 [1m])

Re-derived the full inventory from the c2rust oracle
(`../pgrust/c2rust-runs/backend-access-gin-core-probe/src/*.rs`) and compared
every function against the PG 18.3 C, not the prior report. All 22 functions
across the four files (ginarrayproc: 5, ginlogic: 7 + 2 dispatch helpers,
ginpostinglist: 10, ginvalidate: 2) confirmed MATCH or legitimately SEAMED.
Spot-verified every transcribed constant against the C headers:
GIN_FALSE/TRUE/MAYBE = 0/1/2 and GIN_SEARCH_MODE_DEFAULT/INCLUDE_EMPTY/ALL =
0/1/2 (`access/gin.h`); GIN_SEARCH_MODE_EVERYTHING = 3; GIN proc numbers 1..7,
GINNProcs = 7; AMOP_SEARCH = 's'; INDEX_MAX_KEYS = 32; type OIDs
bool/char/int2/int4/internal = 16/18/21/23/2281 (`pg_type.dat`). The
`((uint64)1) << i` vs `(1 << GIN_CONSISTENT_PROC)` shift-width distinction in
ginvalidate's completeness checks is reproduced faithfully. No
`todo!`/`unimplemented!`; no own-logic stubs.

Seam note (not a finding): `gin_consistent_call_bool`/`gin_consistent_call_tri`
are genuinely OUTWARD into the not-yet-ported fmgr GIN consistent-call
dispatcher (`FunctionCall8Coll`/`FunctionCall7Coll`); no crate installs them in
production, so seam-and-panic is correct (mirror-PG-and-panic). The fmgr
dispatch logic does not belong to ginlogic.c, so `directBool`/`Tri` etc. are
SEAMED, not MISSING. With this unit now `audited`, the
`every_declared_seam_is_installed_by_its_owner` guard's condition (b) becomes
true for the `backend-access-gin-core-probe` owner-dir; it stays green only
because the guard's install-detection scans the owner's whole src incl.
`tests.rs` (which `::set()`s both seams in test setup) without stripping
`#[cfg(test)]`. That is a pre-existing guard-mechanism weakness (the seam's
*true* owner is the unported fmgr dispatcher, which the prefix-keyed exemption
does not model) — it does not affect this crate's logic correctness and the
seam is correctly left for the fmgr dispatcher to install on landing.

Pre-sync: merged current `refs/heads/main` into the branch (clean, no
conflicts; init_all wiring + CATALOG row verified intact post-merge) and
re-gated: `cargo check --workspace` green, both recurrence guards green, all
21 crate tests green, `cargo test --workspace` re-run green.
