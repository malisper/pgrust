# Audit: backend-access-common-tidstore

Independent re-audit: 2026-06-13, model Claude Fable 5 (Opus 4.8 1M).
Verdict: PASS. Re-derived the full 17-function inventory from the C and the
c2rust oracle (`c2rust-runs/backend-access-common-tidstore/src/tidstore.rs`),
re-verified the bit-math (from_offsets threshold walk / contains / offsets_into),
the constants against the headers (NUM_FULL_OFFSETS=3, MAX_OFFSET_IN_BITMAP=
Min(64*127-1, MaxOffsetNumber), bitmapword=u64, BITS_PER_BITMAPWORD=64,
InvalidOffsetNumber=0, MaxOffsetNumber=BLCKSZ/sizeof(ItemIdData)), the
encode/decode header packing (one u64: flags|nwords|3×16-bit offsets), and the
seam wiring. All 7 inward tidstore_* seams installed by init_seams() and wired
into seams-init::init_all(); radixtree.h container is correctly seam-and-panic
via backend-lib-radixtree-seams. No todo!/unimplemented!/own-logic stubs.
Gates: cargo check --workspace clean; 8 tidstore unit tests pass; both
recurrence_guard tests pass.


C source: `src/backend/access/common/tidstore.c` (609 lines).
Port: `crates/backend-access-common-tidstore/src/lib.rs` (+ `tests.rs`).
Owned seam crate: `backend-access-heap-vacuumlazy-seams` declares the
`access/tidstore.h` surface (tidstore is its owner/provider). The radix
container template (`lib/radixtree.h`, instantiated `local_ts_*`/`shared_ts_*`)
is a *separate* unit, seamed via the new `backend-lib-radixtree-seams`.

Independent re-derivation from the C and the c2rust rendering
(`c2rust-runs/backend-access-common-tidstore/src/tidstore.rs`). The c2rust file
confirms tidstore.c emits 17 public functions plus the two radixtree.h template
instantiations (`local_ts_*`, `shared_ts_*`, ~3000 lines) — the latter are NOT
tidstore.c's own logic and are correctly out of scope.

## Function inventory

| C function | C loc | port | verdict | notes |
|---|---|---|---|---|
| WORDNUM / BITNUM / WORDS_PER_PAGE (macros) | 31-35 | `wordnum`/`bitnum`/`words_per_page` | MATCH | pure arithmetic, 1:1 |
| NUM_FULL_OFFSETS (macro) | 38 | `NUM_FULL_OFFSETS` const | MATCH | `(8-1-1)/2 == 3` via size_of |
| MAX_OFFSET_IN_BITMAP (macro) | 84 | `MAX_OFFSET_IN_BITMAP` const | MATCH | `Min(64*127-1, MaxOffsetNumber)`; verified BITS_PER_BITMAPWORD=64, PG_INT8_MAX=127 |
| struct BlocktableEntry | 44-77 | `BlocktableEntry` | MATCH | flags/nwords/full_offsets[3]/words; wire image is byte-faithful to the C header slot + words[] |
| TidStoreCreateLocal | 162 | `TidStoreCreateLocal` | MATCH (sizing) + SEAMED (ctx/tree) | maxBlockSize 1/16-halving loop + ALLOCSET clamp ported in-crate; context+`local_ts_create` is mmgr/radix substrate → `radixtree_create_local` |
| TidStoreCreateShared | 208 | `TidStoreCreateShared` | MATCH (sizing) + SEAMED | dsa_max_size 1/8-halving + DSA_MIN clamp + init<=max ported in-crate; `dsa_create_ext`+`shared_ts_create` → `radixtree_create_shared` |
| TidStoreAttach | 244 | `TidStoreAttach` | SEAMED | asserts (area!=INVALID, DsaPointerIsValid) ported as debug_assert; dsa_attach+shared_ts_attach substrate |
| TidStoreDetach | 269 | `TidStoreDetach` | SEAMED | shared_ts_detach+dsa_detach+pfree substrate |
| TidStoreLockExclusive | 287 | `TidStoreLockExclusive` | SEAMED | `radixtree_lock(Some(true))`; owner no-ops for local (C `if IsShared`) |
| TidStoreLockShare | 294 | `TidStoreLockShare` | SEAMED | `radixtree_lock(Some(false))` |
| TidStoreUnlock | 301 | `TidStoreUnlock` | SEAMED | `radixtree_lock(None)` |
| TidStoreDestroy | 317 | `TidStoreDestroy` | SEAMED | free tree + dsa_detach/ctx delete + pfree substrate → `radixtree_free` |
| TidStoreSetBlockOffsets | 345 | `TidStoreSetBlockOffsets` + `from_offsets`+`encode` | MATCH | both encodings, ascending assert, out-of-range elog, nwords discriminator, per-word threshold walk, `nwords == WORDS_PER_PAGE` assert — all 1:1; radix set → `radixtree_set` |
| TidStoreIsMember | 421 | `TidStoreIsMember` + `decode`+`contains` | MATCH | header-form scan vs bitmap wn>=nwords short-circuit, both 1:1; radix find → `radixtree_find` |
| TidStoreBeginIterate | 471 | `TidStoreBeginIterate` | SEAMED | `radixtree_begin_iterate` |
| TidStoreIterateNext | 493 | `TidStoreIterateNext` | MATCH (decode) + SEAMED (radix next) | None-at-end; decodes entry into result |
| TidStoreEndIterate | 518 | `TidStoreEndIterate` | SEAMED | `radixtree_end_iterate` |
| TidStoreMemoryUsage | 532 | `TidStoreMemoryUsage` | SEAMED | `radixtree_memory_usage` |
| TidStoreGetDSA | 544 | `TidStoreGetDSA` | SEAMED | `radixtree_get_dsa` (returns dsa_handle) |
| TidStoreGetHandle | 552 | `TidStoreGetHandle` | SEAMED | `radixtree_get_handle` (dsa_pointer) |
| TidStoreGetBlockOffsets | 566 | `TidStoreGetBlockOffsets` + `offsets_into` | MATCH | header-form non-Invalid scan + bitmap per-word bit walk; returns total count even when buffer too small — 1:1 |
| ItemPointerGetOffsetNumber (itemptr.h helper) | — | `ItemPointerGetOffsetNumber` | MATCH | returns ip_posid |

Internal helpers added (not C functions): `encode`/`decode` (the across-seam
wire image of the C in-memory BlocktableEntry); `iter_result_to_reap` (the
in-crate measure-then-fill unpack that drives the `tidstore_iterate_next ->
ReapBlockInfo` seam the consumer declares). Both are marshalling for this
repo's seam shapes, not divergent logic.

## Seam audit

Ownership by C-source coverage: tidstore.c is the `access/tidstore.h` provider.
Those seams are declared in `backend-access-heap-vacuumlazy-seams` (the consumer
crate that names them). This crate installs all 7 in `init_seams()` (nothing but
`set()` calls): `tidstore_create_local`, `tidstore_destroy`,
`tidstore_set_block_offsets`, `tidstore_memory_usage`, `tidstore_begin_iterate`,
`tidstore_iterate_next`, `tidstore_end_iterate`. Wired into
`seams-init::init_all()`. No double-install (vacuumlazy installs none of them).

Outward seams (`backend-lib-radixtree-seams`, new): every call is the radix
container substrate (`local_ts_*`/`shared_ts_*` template + DSA + LWLock) which
has no crate yet → a direct dep is impossible, the seam is justified, and each
call site is thin marshal + delegate. Bodies panic "seam not installed" until
`backend-lib-radixtree` lands (mirror-PG-and-panic) — no silent fallback,
nothing fabricates a tree/entry/result.

The src-idiomatic `provider.rs` (an 1888-line adapted radix tree) was
deliberately NOT folded into this crate: it is a foreign unit (radixtree.h),
not tidstore.c's logic.

## Design conformance

- No invented opacity: reuses the canonical `types_vacuum::vacuumlazy::TidStore`
  / `TidStoreIterHandle` / `ReapBlockInfo`; DSA handle/pointer are
  `types_dsa::DsaHandle`/`DsaPointer`; `bitmapword`/`BITS_PER_BITMAPWORD` are
  `types_nodes::bitmapset` (BITS_PER_BITMAPWORD newly added there, its
  nodes/bitmapset.h home, value 64 verified against the C header).
- Allocation: every data-derived `Vec` grows via `try_reserve_exact` +
  `PgError::error("out of memory")` (the C path pallocs / can OOM). Error
  messages use `format!` only at `Err`-return sites.
- No `todo!`/`unimplemented!`; `no-todo-guard` passes.
- No shared statics / ambient globals; no locks held across `?` (lock ops
  delegate whole to the radix owner).
- C `elog(ERROR, "tuple offset out of range")` → `PgError::error(...)`, same
  predicate.

## Verdict: PASS

Every function MATCH or SEAMED per the rules; zero seam findings; design-clean.
`cargo check -p backend-access-common-tidstore -p backend-lib-radixtree-seams
-p seams-init` and `--workspace` are clean; 8 bit-math unit tests pass.
