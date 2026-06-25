# Audit: backend-access-common-indextuple

Unit: `backend-access-common-indextuple`
C source: `src/backend/access/common/indextuple.c` (PG 18.3)
c2rust: `../pgrust/c2rust-runs/backend-access-common-indextuple/src/indextuple.rs`
Port: `crates/backend-access-common-indextuple/src/lib.rs`
Owned seam crate: `crates/backend-access-common-indextuple-seams`

Verdict: **PASS**

This audit was re-derived independently from the C and c2rust; the port's
comments and self-review were not trusted.

## 1. Function inventory (every definition in indextuple.c)

`indextuple.c` defines exactly 7 top-level functions (no statics / file-local
helpers; all macro helpers — `IndexInfoFindDataOffset`, `IndexTupleSize`,
`IndexTupleHasNulls`/`HasVarwidths`, `att_isnull`, `att_nominal_alignby`,
`att_pointer_alignby`, `att_addlength_pointer`, `fetch_att`, the `VARATT_*` /
`VARSIZE_*` family — come from `access/itup.h`, `access/tupmacs.h`,
`varatt.h`, inlined by c2rust). The c2rust rendering confirms exactly these 7
`#[no_mangle]` functions.

| # | C fn (C loc) | Port loc | Verdict | Notes |
|---|---|---|---|---|
| 1 | `index_form_tuple` (43) | lib.rs:230 | MATCH | Delegates to `index_form_tuple_context(.., CurrentMemoryContext)`; owned model has one `mcx`. |
| 2 | `index_form_tuple_context` (64) | lib.rs:242 | MATCH | See detail below. |
| 3 | `nocache_index_getattr` (240) | lib.rs:386 | MATCH | Fast/slow blocks collapsed into one offset-walk; provably identical (detail below). |
| 4 | `index_deform_tuple` (455) | lib.rs:459 | MATCH | Thin wrapper: data offset + bitmap + hasnulls → `_internal`. |
| 5 | `index_deform_tuple_internal` (478) | lib.rs:476 | MATCH | 1:1 offset walk; null/varlena/fixed branches preserved. |
| 6 | `CopyIndexTuple` (546) | lib.rs:537 | MATCH | C `memcpy(IndexTupleSize)` → deep clone of header+bits+data. |
| 7 | `index_truncate_tuple` (575) | lib.rs:552 | MATCH | Easy-case copy; else `CreateTupleDescTruncatedCopy` (ported direct dep) → deform → form → stamp `t_tid`. |

## 2. Per-function detail (load-bearing checks)

### index_form_tuple_context
- `numberOfAttributes > INDEX_MAX_KEYS` (32) → `ereport(ERROR, ERRCODE_TOO_MANY_COLUMNS)` with the exact message. ERRCODE = 54011 verified in `types-error`. MATCH.
- TOAST_INDEX_HACK loop (always-on in this build): per attr, skip if `isnull || attlen != -1`; if `VARATT_IS_EXTERNAL` → `detoast_external_attr` (outward seam, detoast owner); then if `!VARATT_IS_EXTENDED && VARSIZE > TOAST_INDEX_TARGET && attstorage in {EXTENDED 'x', MAIN 'm'}` → `toast_compress_datum` (outward seam, toast-internals owner), keep result only when non-NULL. `TYPSTORAGE_EXTENDED='x'`, `TYPSTORAGE_MAIN='m'` verified. `TOAST_INDEX_TARGET = MaxHeapTupleSize/16`, `MaxHeapTupleSize = BLCKSZ(8192) - MAXALIGN(SizeOfPageHeaderData(24)+sizeof(ItemIdData)(4))` as a const — matches the c2rust constant derivation. MATCH.
- `hasnull` scan, `INDEX_NULL_MASK` (0x8000), `hoff = IndexInfoFindDataOffset(infomask)` (MAXALIGN(8) / MAXALIGN(8+4)), `data_size = heap_compute_data_size(desc, untoasted_values, isnull)` (reused from heaptuple), `size = MAXALIGN(hoff+data_size)`. MATCH.
- `heap_fill_tuple` (reused) returns owned data area, null bitmap, `infomask`; `HEAP_HASVARWIDTH` (0x0002) → `INDEX_VAR_MASK` (0x4000). All mask constants verified vs `types-tuple`. MATCH.
- C `Assert((tupmask & HEAP_HASEXTERNAL)==0)` rendered as fail-fast `Err` (HEAP_HASEXTERNAL=0x0004). Behavior-preserving (predicate cannot be true post-detoast). MATCH.
- `(size & INDEX_SIZE_MASK) != size` → `ereport(ERROR, ERRCODE_PROGRAM_LIMIT_EXCEEDED)` (54000 verified), exact message; `INDEX_SIZE_MASK=0x1FFF`. `infomask |= size`. MATCH.

### nocache_index_getattr — restructure, behavior identical
C has three blocks: (1) preceding-nulls scan setting `slow`; (2) `!slow` fast
path using `attcacheoff` + a fixed-width cache-fill loop; (3) `else` CAREFUL
walk. The port collapses (1)–(3) into a single `0..=index` offset walk. Sound:
- `attcacheoff` in C is a pure cross-call cache; the port borrows the descriptor
  immutably and recomputes from scratch every call — the *computed* offset for
  the target is identical arithmetic (`att_nominal_alignby` fixed/aligned,
  `att_pointer_alignby` unaligned varlena, `att_addlength_pointer` advance). No
  persisted state ⇒ no divergence.
- Per-iteration `att_isnull(cur)` is only true with preceding nulls (where C is
  already in the slow walk and also `continue`s without advancing `off`); with
  no nulls it is always false → matches the C fast path that never reads the
  bitmap. MATCH.
- The defensive null-target early return preserves C's caller contract (C never
  passes a null target — the `index_getattr` macro checks first). MATCH.
`fetch_att` (by-val widths 1/2/4/8 else "unsupported byval length") and the
by-ref field-span copy match `tupmacs.h`. MATCH.

### index_deform_tuple_internal
`Assert(natts <= INDEX_MAX_KEYS)` → `debug_assert`. Per-attr null/cache/varlena/
fixed branches and the `att_addlength_pointer` advance with full `VARSIZE_ANY`
(1b_e external via `VARTAG_SIZE`, 1b short, 4b) match the c2rust offset-advance
expansion byte-for-byte. `slow` set on null and on `attlen <= 0`. MATCH.

### Helper macros (varatt.h / tupmacs.h), ported 1:1
`varatt_is_external`/`_extended`/`_compressed`/`_1b`/`_4b`, `varsize_4b_len`/
`_1b`/`_external`/`_any`, `vartag_size` (INDIRECT=8, EXPANDED_RO masked=8,
ONDISK=16), `att_nominal_alignby`, `att_pointer_alignby` (`VARATT_NOT_PAD_BYTE`
= first byte != 0), `att_addlength_pointer`, `att_isnull` — each re-derived and
matched vs the little-endian macro definitions and c2rust inlines.
`VARATT_IS_EXTENDED` truth table cross-checked vs `VARATT_IS_4B_U`. MATCH.

## 3. Seam audit

Owned seam crate (by C-source coverage): `backend-access-common-indextuple-seams`
declares exactly two seams, both installed by this crate's `init_seams()`:
- `index_form_tuple(mcx, rel, values, isnull, ht_ctid) -> PgVec<u8>` — body
  `index_form_tuple_seam`; thin marshal (RelationGetDescr + Datum→TupleValue
  bridge) → `index_form_tuple` → stamp `t_tid` → `on_disk_image`. Allocating →
  `Mcx` + `PgResult`. The bare-`Datum` by-reference bridge panics loudly on the
  unresolved execTuples slot-payload frontier (not a silent stub).
- `index_deform_tuple(estate, slot, itup, itupdesc) -> ()` — header-only
  nodeIndexonlyscan adapter; panics loudly on the same frontier
  (mirror-PG-and-panic; full data-area model lives in `index_deform_tuple`/
  `_internal`, reachable directly — logic is present, not absent).

`init_seams()` contains only the two `set()` calls and is wired into
`seams-init::init_all()`. Recurrence guard passes both directions.

Outward seam `::call`s — `detoast_external_attr`
(`backend-access-common-detoast-seams`) and `toast_compress_datum`
(`backend-access-common-toast-internals-seams`) — are genuine cross-subsystem
externals to unported/sibling owners, thin arg-convert + one call + result-
convert, no branching/computation. No seam findings.

## 4. Design conformance
- Allocating fns take `Mcx`, return `PgResult`.
- No invented opacity: `FormedIndexTuple{header,bits,data}` is the real owned
  shape over `IndexTupleData` + `PgVec`, mirroring the heaptuple model.
- No shared statics, no ambient-global seams, no locks-across-`?`, no registry
  side tables, no unledgered divergence markers.
- No `todo!()`/`unimplemented!()`; no own-logic stubs. The two panics are
  mirror-PG-and-panic on the unported execTuples slot-payload frontier.

## 5. Gates
- `cargo check --workspace` — clean (only pre-existing unrelated warnings in
  `backend-access-common-printtup`).
- `cargo test -p backend-access-common-indextuple` — 6/6 pass.
- `cargo test -p seams-init` — 2/2 pass (recurrence guard both directions).

All 7 functions MATCH; zero seam findings; gates green. **PASS.**
