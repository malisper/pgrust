# Audit: backend-access-common-heaptuple

Unit: `backend-access-common-heaptuple` (`src/backend/access/common/heaptuple.c`,
1613 lines, PostgreSQL 18.3).
Crates audited: `crates/backend-access-common-heaptuple`,
`crates/backend-utils-adt-misc2-seams`, `crates/backend-access-heap-heaptoast-seams`.
Cross-checked against `../pgrust/c2rust-runs/backend-access-common-heaptuple/src/heaptuple.rs`.
Auditor: independent re-derivation from the C sources and headers
(`varatt.h`, `tupmacs.h`, `htup_details.h`, `sysattr.h`, `tupdesc.h`, `itemptr.h`).

## Function inventory (every definition in heaptuple.c)

| # | C function (heaptuple.c) | Port location | Verdict | Notes |
|---|---|---|---|---|
| 1 | `missing_hash` (:103) | — (subsumed) | MATCH (subsumed) | Pure allocation-lifetime machinery for the by-ref missing-value cache. In the owned model a returned value owns its bytes, so the cache has no observable behavior. The by-ref case it serves cannot be represented by the idiomatic `AttrMissing` (a bare `Datum` word) and panics loudly in `missing_value` — an unported-substrate panic, not absent logic (see #4). |
| 2 | `missing_match` (:111) | — (subsumed) | MATCH (subsumed) | Same cache machinery; see #1. |
| 3 | `init_missing_cache` (:125) | — (subsumed) | MATCH (subsumed) | Same cache machinery; see #1. |
| 4 | `getmissingattr` (:150) | `lib.rs::getmissingattr` | MATCH | 1-based attnum; `atthasmissing` → `am_present` → `(value, false)`, else `(NULL Datum, true)`, exactly C's predicates. C `Assert(constr && missing)` mirrored as `debug_assert` (added in fix round 1). By-value missing values returned directly (C's `attbyval` fast path); by-reference missing values panic loudly (`missing_value`) because `AttrMissing.am_value` is a word and the missing-value-as-bytes catalog substrate is unported — same contract as `expand_tuple`. The cache path (#1–#3) is identity-preserving lifetime management and is behaviorally a no-op in the owned model. |
| 5 | `heap_compute_data_size` (:218) | `lib.rs::heap_compute_data_size` | MATCH | Loop, null skip, three arms in C order: `COMPACT_ATTR_IS_PACKABLE && VARATT_CAN_MAKE_SHORT` → converted-short size, `attlen == -1 && VARATT_IS_EXTERNAL_EXPANDED` → align + `EOH_get_flat_size` (seam), else `att_datum_alignby` + `att_addlength_datum`. `COMPACT_ATTR_IS_PACKABLE` fixed in round 1 to include the `attlen == -1` half (heaptuple.c:87). Helper macros re-derived against tupmacs.h/varatt.h: `att_datum_alignby`, `att_addlength_datum`, `VARATT_CAN_MAKE_SHORT` (`<= 0x7F`), `VARATT_CONVERTED_SHORT_SIZE` all exact. EOH call is SEAMED (thin: bytes in, size out). |
| 6 | `fill_val` (:274) | `lib.rs::fill_val` | MATCH | Bitmask walk identical (`*bitmask != HIGHBIT` shift / advance-byte reset; HIGHBIT == 0x80 verified); null early-return sets `HEAP_HASNULL`; byval arm (`att_nominal_alignby` + `store_att_byval`); varlena arms in C order: external-expanded (align + `EOH_get_flat_size`/`EOH_flatten_into`, SEAMED), external (`HEAP_HASEXTERNAL`, `VARSIZE_EXTERNAL` copy, no align), short (`VARSIZE_SHORT` copy, no align), packable-can-make-short (`SET_VARSIZE_SHORT` + `VARDATA` copy of `len-1`), full 4-byte (align + `VARSIZE` copy); cstring arm (`strlen+1`, `HEAP_HASVARWIDTH`, alignby==1 asserted); fixed by-ref arm (align + attlen copy). Cursor advance `data += data_length` exact. `VARSIZE_EXTERNAL = 2 + VARTAG_SIZE(tag)` — `vartag_size` fixed in round 1 (see findings). |
| 7 | `heap_fill_tuple` (:400) | `lib.rs::heap_fill_tuple` | MATCH | `bitP = &bit[-1]` modeled with `byte = usize::MAX` + wrapping add; `bitmask = HIGHBIT`. C clears HASNULL/HASVARWIDTH/HASEXTERNAL of the caller's infomask; port starts from 0 — equivalent at both in-crate call sites (C callers pass a palloc0'd infomask). `values ? values[i] : NULL` / `isnull ? isnull[i] : true` modeled per-element. End-of-fill `Assert((data - start) == data_size)` → `debug_assert_eq`. |
| 8 | `heap_attisnull` (:455) | `lib.rs::heap_attisnull` | MATCH | NULL-tupledesc allowed (`Option`); `attnum > HeapTupleHeaderGetNatts` → `atthasmissing ? false : true`; `attnum > 0` → `HeapTupleNoNulls` / `att_isnull(attnum-1, t_bits)`; system attnums −1..−6 never null (constants verified against sysattr.h); other attnum (incl. 0) → panic == `elog(ERROR, "invalid attnum")`. |
| 9 | `nocachegetattr` (:520) | `lib.rs::nocachegetattr` | MATCH (restructured) | C computes the target offset via the slow/fast walk with opportunistic `attcacheoff` writes; port deforms up to the attribute and returns column `attnum-1`. The deform walk applies the identical alignment/length stepping (`att_pointer_alignby` / `att_addlength_pointer` / `fetchatt`), so the returned value is provably identical for every input within the function's contract (`fastgetattr`: 1 ≤ attnum ≤ header natts, non-null). The `attcacheoff` writes are a pure perf cache (C only ever writes the same deterministic offsets) and are omitted consistently crate-wide. |
| 10 | `heap_getsysattr` (:724) | `lib.rs::heap_getsysattr` | MATCH | isnull always false; ctid → `ByRef` of the 6 `ItemPointerData` bytes (stand-in for `PointerGetDatum(&t_self)`); xmin/xmax raw reads; cmin/cmax both → raw command id; tableoid → `t_tableOid`; default → panic == `elog(ERROR)`. Raw xmin/xmax/cid on a `TDatum` header read 0 (C reads union bytes; defined-value stand-in for the same memory). |
| 11 | `heap_copytuple` (:777) | `lib.rs::heap_copytuple` | MATCH | invalid/`t_data == NULL` → `None`; else deep clone of header + bitmap + data == the single-chunk memcpy. |
| 12 | `heap_copytuple_with_tuple` (:803) | `lib.rs::heap_copytuple_with_tuple` | MATCH | invalid source → result with `t_data: None` (C: `dest->t_data = NULL`); else len/self/tableOid/data copied. |
| 13 | `expand_tuple` (:829) | `lib.rs::expand_tuple` | MATCH | Re-derived in full: `hasNulls`/`sourceNullLen` ordering (computed before the missing walk mutates `hasNulls`), `firstmissingnum` scan, per-missing `att_datum_alignby`+`att_addlength` sizing, `len = nulllen + header_base; hoff = MAXALIGN; len += dataLen`, heap vs minimal header bases (23 vs 15) and `t_hoff + MINIMAL_TUPLE_OFFSET` bias, bitmap copy / 0xff-fill with inverted partial-byte mask, cursor seed `nullBits += sourceNullLen-1` + `bitMask = 1 << ((sourceNatts-1) & 7)` (wrapping for sourceNatts==0, fixed round 1), verbatim source-data copy, trailing fill_val loop with `attrmiss && am_present` predicate and `(Datum) 0, true` null case. infomask seeded from source `t_infomask` and OR-accumulated, as C. By-ref missing values panic via `missing_value` (unported substrate, loud). |
| 14 | `minimal_expand_tuple` (:1053) | `lib.rs::minimal_expand_tuple` | MATCH | expand_tuple(Minimal) + `t_infomask2 = 0` then SetNatts (== palloc0 + `HeapTupleHeaderSetNatts`). |
| 15 | `heap_expand_tuple` (:1065) | `lib.rs::heap_expand_tuple` | MATCH | expand_tuple(Heap) + Datum fields (len/typeid/typmod), invalid ctid, `t_self`/`t_tableOid` from source, SetNatts. |
| 16 | `heap_copy_tuple_as_datum` (:1080) | `lib.rs::heap_copy_tuple_as_datum` | MATCH | `HeapTupleHasExternal` → `toast_flatten_tuple_to_datum` (SEAMED, thin delegate); else clone + SetDatumLength/SetTypeId/SetTypMod. |
| 17 | `heap_form_tuple` (:1116) | `lib.rs::heap_form_tuple` | MATCH | `MaxTupleAttributeNumber` (1664, verified htup_details.h) → `TooManyColumns` (ERRCODE_TOO_MANY_COLUMNS); hasnull scan with break; `len = offsetof(t_bits)[23] (+ BITMAPLEN) → MAXALIGN → hoff → + data_len`; Datum fields, invalid `t_ctid`/`t_self` (`{0xffff,0xffff},0` per itemptr.h), `InvalidOid`, SetNatts, `t_hoff`, fill. |
| 18 | `heap_modify_tuple` (:1209) | `lib.rs::heap_modify_tuple` | MATCH | deform → overlay where `doReplace` → form → copy identity (ctid/self/tableOid). |
| 19 | `heap_modify_tuple_by_cols` (:1277) | `lib.rs::heap_modify_tuple_by_cols` | MATCH | 1-based `replCols`; `attnum <= 0 || > natts` → `InvalidColumnNumber` (== `elog(ERROR, "invalid column number %d")`); overlay → form → identity copy. |
| 20 | `heap_deform_tuple` (:1345) | `lib.rs::heap_deform_tuple` | MATCH | natts clamp `Min(header natts, tdesc natts)`; per-attr: null → `(0, true)` + slow; cached-offset read honored when `!slow && attcacheoff >= 0`; varlena align-or-cache branch (`att_pointer_alignby` with `VARATT_NOT_PAD_BYTE`), nominal align otherwise; `fetchatt` + `att_addlength_pointer`; `attlen <= 0` → slow; trailing attrs via `getmissingattr(attnum+1)`. Cache writes omitted (pure perf; see #9). |
| 21 | `heap_freetuple` (:1434) | `lib.rs::heap_freetuple` | MATCH | `pfree` == consume-and-drop. |
| 22 | `heap_form_minimal_tuple` (:1452) | `lib.rs::heap_form_minimal_tuple` | MATCH | `Assert(extra == MAXALIGN(extra))` → debug_assert; same column-limit/hasnull/len math with `SizeofMinimalTupleHeader` (15, compile-time asserted) and `t_hoff = hoff + MINIMAL_TUPLE_OFFSET` (8, verified htup_details.h); `extra` only governs C's leading zero pad (no tuple content). SetNatts via the shared `HEAP_NATTS_MASK` write. |
| 23 | `heap_free_minimal_tuple` (:1529) | `lib.rs::heap_free_minimal_tuple` | MATCH | drop. |
| 24 | `heap_copy_minimal_tuple` (:1541) | `lib.rs::heap_copy_minimal_tuple` | MATCH | extra asserted; deep clone == memcpy of `t_len` bytes. |
| 25 | `heap_tuple_from_minimal_tuple` (:1564) | `lib.rs::heap_tuple_from_minimal_tuple` | MATCH | `len = t_len + MINIMAL_TUPLE_OFFSET`; invalid `t_self`; `InvalidOid`; `memset(t_data, 0, offsetof(t_infomask2))[18]` == zeroed `t_choice` + zero `t_ctid`; shared `t_infomask2/t_infomask/t_hoff/t_bits` tail + data carried over. |
| 26 | `minimal_tuple_from_heap_tuple` (:1586) | `lib.rs::minimal_tuple_from_heap_tuple` | MATCH | asserts; `len = t_len - MINIMAL_TUPLE_OFFSET`, shared tail copied, `t_len` rewritten. C leaves copied ctid garbage in `mt_padding`; port zeroes it — `mt_padding` has no defined reader, behaviorally indistinguishable. |
| 27 | `varsize_any` (:1609) | `lib.rs::varsize_any` | MATCH | exact `VARSIZE_ANY` ladder (1B_E → external, 1B → short, else 4B); the C export is a bare wrapper kept for JIT inlining. |

Constants verified against headers (not from memory): `HEAP_HASNULL/HASVARWIDTH/HASEXTERNAL`
(0x1/0x2/0x4), `HEAP_NATTS_MASK` (0x07FF), `HIGHBIT` (0x80), `MaxTupleAttributeNumber` (1664),
`SizeofHeapTupleHeader` (23 == offsetof t_bits), `SizeofMinimalTupleHeader` (15),
`MINIMAL_TUPLE_OFFSET` (8), `MINIMAL_TUPLE_DATA_OFFSET` (10), sysattr numbers (−1..−7),
varlena header bits (`VARATT_IS_1B/1B_E/4B_U`, `VARSIZE_1B/4B`, `VARATT_SHORT_MAX` 0x7F,
`VARHDRSZ_EXTERNAL` 2), `VARTAG_*` tags (1/2/3/18) and `VARTAG_SIZE` (8/8/16),
`BITMAPLEN`, `TYPEALIGN`/`MAXALIGN`, `ItemPointerSetInvalid` ({0xffff,0xffff},0).

Port-only adapters (no C counterpart; audited for layout fidelity only):
`flat.rs` (`minimal_tuple_to_flat` / `minimal_tuple_from_flat` / `*_flat` wrappers) and
`lib.rs::minimal_tuple_to_flat_bytes` / `minimal_tuple_from_flat_bytes` /
`heap_tuple_to_disk_image` / `heap_form_tuple_heaptuple` serialize the exact C
`MinimalTupleData`/`HeapTupleHeaderData` chunk layouts (offsets re-derived from
htup_details.h and compile-time asserted); they contain no heaptuple.c logic of
their own beyond composing the audited functions.

## Findings (fix round 1 — all fixed, re-audited from scratch)

1. **`vartag_size` — DIVERGES (fixed).** Returned 16 for all three TOAST-pointer
   tags; `VARTAG_SIZE` is `sizeof(varatt_indirect)` == 8 for `VARTAG_INDIRECT`
   and `sizeof(varatt_expanded)` == 8 for the expanded tags (both single
   pointers), 16 only for `VARTAG_ONDISK` (confirmed in varatt.h:96 and the
   c2rust rendering). Mis-sized `VARSIZE_EXTERNAL`/`VARSIZE_ANY` for indirect
   pointers (18 vs 10), corrupting `fill_val`'s external copy length and deform
   stepping. Fixed to 8/8/16 with C's check order; re-audited: MATCH.
2. **`compact_attr_is_packable` — DIVERGES (fixed).** Omitted the
   `attlen == -1` conjunct of `COMPACT_ATTR_IS_PACKABLE` (heaptuple.c:87); a
   descriptor with `attispackable && attlen != -1` would take the wrong arm
   (and could panic on a by-value Datum). Fixed; re-audited: MATCH.
3. **`expand_tuple` bit-cursor seed — DIVERGES (fixed).** `source_null_len - 1`
   underflow-panics (debug) when `sourceNatts == 0` with no source bitmap; C's
   `nullBits += sourceNullLen - 1` legally parks the cursor one byte before the
   bitmap (bitMask == HIGHBIT advances onto byte 0 on the first `fill_val`).
   Fixed with `wrapping_sub(1)` (the same `&bit[-1]` model `heap_fill_tuple`
   uses); re-audited: MATCH.
4. **`getmissingattr` missing-constr Assert (hardening, fixed).** C
   `Assert(tupleDesc->constr && constr->missing)`; the port silently fell
   through to NULL on an inconsistent descriptor. Added the matching
   `debug_assert`.

## Seam audit

- `backend-utils-adt-misc2-seams` (`eoh_get_flat_size`, `eoh_flatten_into`):
  reached only from `heap_compute_data_size` / `fill_val` for
  `VARATT_IS_EXTERNAL_EXPANDED` datums, exactly C's two `EOH_*` call sites
  (`utils/adt/expandeddatum.c`, unported owner — a direct dep does not exist).
  Both call sites are thin: byte-slice in, size/flatten out; no branching or
  computation inside the seam path. Declarations are pure `seam!` slots; no
  `set()` anywhere (owner not landed → loud panic), none expected.
- `backend-access-heap-heaptoast-seams` (`toast_flatten_tuple_to_datum`):
  reached only from `heap_copy_tuple_as_datum` under `HeapTupleHasExternal`,
  exactly C's single call site (`access/heap/heaptoast.c`, unported owner).
  Thin delegate (tuple + desc in, tuple out). No stray `set()`.
- `backend-access-common-heaptuple::init_seams()` is empty (the crate declares
  no inward seams) and is invoked by `seams-init::init_all()`, which contains
  nothing but the one init call. No `set()` calls outside owners. No function
  body in this unit was replaced by a seam — all 27 bodies live in this crate.

## Verdict

**PASS** (after fix round 1). All 27 heaptuple.c functions are MATCH (or the
three cache statics subsumed with the by-ref missing-value case panicking
loudly on its unported substrate, and the two EOH/heaptoast calls SEAMED per
the rules above). Workspace builds clean; all 42 tests pass.
