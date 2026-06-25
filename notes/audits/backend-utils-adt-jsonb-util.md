# Audit: backend-utils-adt-jsonb-util

- **Verdict: PASS**
- **Date:** 2026-06-13
- **Model:** Opus 4.8 (1M context) (claude-opus-4-8[1m])
- **Branch:** port/backend-utils-adt-jsonb-util
- **Unit:** `jsonb_util.c` sub-unit of catalog row `backend-utils-adt-jsonb-core`
  (`*/jsonb.c,*/jsonb_gin.c,*/jsonb_util.c`).  This branch ports only
  `src/backend/utils/adt/jsonb_util.c`.
- **C source:** `postgres-18.3/src/backend/utils/adt/jsonb_util.c` (2006 lines)
- **c2rust:** `c2rust-runs/backend-utils-adt-jsonb-core/src/jsonb_util.rs`
- **Port:** `crates/backend-utils-adt-jsonb-util/src/lib.rs` (2016 lines)
- **In-memory/ABI types:** `crates/types-jsonb/{jsonb.rs,backend_utils_adt_jsonb_util.rs}`

## 1. Function inventory & verdicts

Enumerated all 40 function definitions (statics + helpers + exported) from the C
`jsonb_util.c`; cross-checked the count against the c2rust rendering (44 fn lines,
includes the inlined macro helpers).  Every C function has a port counterpart.

| # | C function | C line | Port location | Verdict |
|---|---|---|---|---|
| 1 | JsonbToJsonbValue | 72 | lib.rs:161 | MATCH |
| 2 | JsonbValueToJsonb | 92 | lib.rs:177 | MATCH |
| 3 | getJsonbOffset | 134 | lib.rs:225 | MATCH |
| 4 | getJsonbLength | 159 | lib.rs:240 | MATCH |
| 5 | compareJsonbContainers | 191 | lib.rs:1241 | MATCH |
| 6 | findJsonbValueFromContainer | 348 | lib.rs:1319 | MATCH |
| 7 | getKeyJsonValueFromContainer | 402 | lib.rs:1368 | MATCH |
| 8 | getIthJsonbValueFromContainer | 472 | lib.rs:1423 | MATCH |
| 9 | fillJsonbValue | 509 | lib.rs:266 | MATCH |
| 10 | pushJsonbValue | 570 | lib.rs:329 | MATCH |
| 11 | pushJsonbValueScalar | 644 | lib.rs:414 | MATCH |
| 12 | pushState | 735 | lib.rs:526 | MATCH |
| 13 | appendKey | 750 | lib.rs:538 | MATCH |
| 14 | appendValue | 779 | lib.rs:562 | MATCH |
| 15 | appendElement | 792 | lib.rs:572 | MATCH |
| 16 | JsonbIteratorInit | 821 | lib.rs:603 | MATCH |
| 17 | JsonbIteratorNext | 859 | lib.rs:624 | MATCH |
| 18 | iteratorFromContainer | 1012 | lib.rs:865 | MATCH |
| 19 | freeAndGetParent | 1054 | lib.rs:912 | MATCH |
| 20 | JsonbDeepContains | 1075 | lib.rs:1454 | MATCH |
| 21 | JsonbHashScalarValue | 1329 | lib.rs:1819 | MATCH |
| 22 | JsonbHashScalarValueExtended | 1372 | lib.rs:1842 | MATCH |
| 23 | equalsJsonbScalarValue | 1414 | lib.rs:1641 | SEAMED (numeric_eq) |
| 24 | compareJsonbScalarValue | 1446 | lib.rs:1671 | SEAMED (varstr_cmp, numeric_cmp) |
| 25 | reserveFromBuffer | 1491 | lib.rs:947 (ConvertBuffer::reserve) | MATCH |
| 26 | copyToBuffer | 1517 | lib.rs:966 (ConvertBuffer::copy_to) | MATCH |
| 27 | appendToBuffer | 1526 | lib.rs:971 (ConvertBuffer::append) | MATCH |
| 28 | padBufferToInt | 1540 | lib.rs:978 (ConvertBuffer::pad_to_int) | MATCH |
| 29 | convertToJsonb | 1561 | lib.rs:990 | MATCH |
| 30 | convertJsonbValue | 1603 | lib.rs:1010 | SEAMED (check_stack_depth) |
| 31 | convertJsonbArray | 1628 | lib.rs:1036 | MATCH |
| 32 | convertJsonbObject | 1712 | lib.rs:1090 | MATCH |
| 33 | convertJsonbScalar | 1828 | lib.rs:1155 | SEAMED (json_encode_datetime) |
| 34 | lengthCompareJsonbStringValue | 1893 | lib.rs:1892 | MATCH |
| 35 | lengthCompareJsonbString | 1912 | lib.rs:1877 | MATCH |
| 36 | lengthCompareJsonbPair | 1932 | lib.rs:1906 | MATCH |
| 37 | uniqueifyJsonbObject | 1956 | lib.rs:1926 | MATCH |
| — | hash_numeric (numeric.c digit walk, ported in-crate) | numeric.c | lib.rs:1726 | MATCH (in-crate, over hashfn seams) |
| — | hash_numeric_extended (in-crate) | numeric.c | lib.rs:1775 | MATCH (in-crate) |
| — | JsonbIteratorInitAt (port-only `doc_offset` entry) | n/a | lib.rs:617 | port-only, additive |

### Spot-check detail (re-derived MATCH verdicts)

- **Constants vs. headers** (jsonb.h): `JENTRY_OFFLENMASK=0x0FFFFFFF`,
  `JENTRY_TYPEMASK=0x70000000`, `JENTRY_HAS_OFF=0x80000000`,
  `JENTRY_ISSTRING=0`, `JENTRY_ISNUMERIC=0x10000000`,
  `JENTRY_ISBOOL_FALSE=0x20000000`, `JENTRY_ISBOOL_TRUE=0x30000000`,
  `JENTRY_ISNULL=0x40000000`, `JENTRY_ISCONTAINER=0x50000000`,
  `JB_CMASK=0x0FFFFFFF`, `JB_FSCALAR=0x10000000`, `JB_FOBJECT=0x20000000`,
  `JB_FARRAY=0x40000000`, `JB_OFFSET_STRIDE=32` — all verified identical in
  `types-jsonb/src/jsonb.rs`. `DEFAULT_COLLATION_OID=100`, `MAXDATELEN`,
  `MaxAllocSize=0x3fffffff` confirmed.
- **JSONB_MAX_ELEMS / JSONB_MAX_PAIRS**: C `Min(MaxAllocSize/sizeof(...), JB_CMASK)`
  collapses to the MaxAllocSize term: 33554431 / 14913080 respectively, both below
  `JB_CMASK` (268435455). Port hardcodes these with the derivation documented and
  uses them in the matching `ereport(ERRCODE_PROGRAM_LIMIT_EXCEEDED)` messages.
- **fillJsonbValue numeric reconstruction**: C reads `(Numeric)(base_addr +
  INTALIGN(offset))` as a self-describing varlena; port computes
  `numlen = total_len - (INTALIGN(offset) - offset)`. The stored node length is
  exactly `padlen + numlen` where `padlen == INTALIGN(node_start) - node_start ==
  INTALIGN(offset) - offset`, so the reconstructed length is bit-identical.
  Confirmed by passing `numeric_scalar_roundtrip` test.
- **convertJsonbObject** stride: keys use `i % JB_OFFSET_STRIDE`, values use
  `(i + nPairs) % JB_OFFSET_STRIDE` (lib.rs:1138) — matches C lines 1771/1806.
- **uniqueifyJsonbObject** dedup index math: C advances the `pairs` base pointer
  for leading-null skip and recomputes `nPairs = res + 1 - pairs`. Port mirrors
  with `drain(0..start)` then `truncate(new_len - start)`; the relative offsets
  are equal. The `lengthCompareJsonbPair` `order` tiebreak makes the comparator a
  strict total order, so `sort_by` is behaviorally identical to C's `qsort_arg`
  (stability irrelevant). `hasNonUniq`/`unique_keys`/`skip_nulls` predicates and the
  `ERRCODE_DUPLICATE_JSON_OBJECT_KEY_VALUE` raise match.
- **compareJsonbContainers** bool/array/object/raw-scalar ordering, the empty-array
  sorts-less-than-null quirk (no `else` after rawScalar branch), and the
  type-defined ordering on heterogeneous tokens all match C exactly. The `do/while
  (res==0)` becomes a `loop` with `if res != 0 break`.
- **compareJsonbScalarValue** bool branch `ba & !bb` correctly yields 1 only for
  (true,false), matching C `a > b`.
- **JsonbDeepContains** object-pair and array-element containment, the `nPairs`
  shortcut, raw-scalar-can't-contain-array guard, O(N^2) nested-array temp-array
  build, and all early `false`/`true` returns match C.
- **JsonbIteratorNext** state machine (ARRAY_START/ELEM, OBJECT_START/KEY/VALUE),
  `JBE_ADVANCE_OFFSET` placement, `curValueOffset` init via `getJsonbOffset(.,nElems)`,
  recurse-on-non-scalar-unless-skipNested, and the `freeAndGetParent` END tokens all
  match. The C `nElems` exposed on BEGIN tokens (with `elems`/`pairs` left unset) is
  modeled by counted placeholder vectors that are never read — equivalent.

## 2. Seam audit

**Owned seam crates:** none. `jsonb_util.c` declares no function that another
crate must call back into across a dependency cycle, so it owns no `*-seams`
crate. The crate's `init_seams()` (lib.rs:87) is intentionally empty and is
correct per SKILL §3 — there are no owned-but-uninstalled declarations.

**Outward (consumer-side) seam calls** — each is a real cross-subsystem boundary,
declared in the *neighbor*-owned seam crate, and is thin marshal+delegate:

- `backend_utils_adt_json_seams::json_encode_datetime` — json.c owns; reached only
  from `convertJsonbScalar` jbvDatetime arm; C always passes non-NULL `&tz` ⇒
  `Some(tz)`. Marshal-one-call-marshal.
- `backend_utils_adt_numeric_seams::{numeric_eq, numeric_cmp}` — numeric.c owns;
  reached from `equalsJsonbScalarValue` / `compareJsonbScalarValue`.
- `backend_utils_adt_varlena_seams::varstr_cmp` — varlena.c owns; collation-aware
  `jbvString` B-tree compare with `DEFAULT_COLLATION_OID`.
- `backend_utils_misc_stack_depth_seams::check_stack_depth` — reached from
  `convertJsonbValue` and `JsonbDeepContains` (C `check_stack_depth()`).
- `common_hashfn_seams::{hash_bytes, hash_bytes_extended, hash_bytes_uint32_extended}`
  — hashfn.c owns; the byte-hash primitives.

No branching/node-construction/computation lives in any seam path. The
`hash_numeric` / `hash_numeric_extended` digit-walk logic is **ported in-crate**
(over the hashfn seams + types-numeric byte accessors), not delegated whole — so
those are MATCH, not MISSING. The declarations are installed by their owners when
those units land; until then a call panics loudly (acceptable: panic on unported
*callee*, never absent logic). No `set()` is performed outside an owner in
production code (test-only `set()` calls in `tests.rs` are fine).

`rotate_high_and_low_32bits` and `pg_rotate_left32` mask/rotate constants match
hashfn.h / pg_bitutils.h.

## 3b. Design conformance

- **Opacity inherited, not introduced**: the C `JsonbContainer *` /`char *` cursors
  become owned `Vec<u8>` + byte offsets; the `jbvBinary` `offset` field and the
  port-only `JsonbIteratorInitAt` / `doc_offset` thread the document-relative
  position that C derives from the raw pointer. This is the real underlying
  position made explicit — not an invented handle. `doc_offset` defaults to 0 on
  the C-equivalent `JsonbIteratorInit` path and is read only by child-iterator
  propagation (`binary_offset`); it never affects convert/compare/serialize output.
  Conforms to types.md rules 6-7.
- **Mcx + PgResult on allocators**: `JsonbValueToJsonb` / `convertToJsonb` take
  `Mcx` and return `PgResult<PgVec>`; every data-derived growth uses `try_reserve`
  against a validated bound (`JENTRY_OFFLENMASK` / `MaxAllocSize` / already-bounded
  count fields) and surfaces OOM as recoverable `PgError`. No ambient-global state,
  no shared statics, no registry side tables, no locks across `?`.
- **No unledgered divergence markers**; the port-only entry point and the in-crate
  hash_numeric are documented inline.

## Verdict

**PASS.** Every C function is MATCH or properly SEAMED; zero seam findings; empty
`init_seams()` is correct (no owned seam crate); all constants verified against the
headers; design rules satisfied. 18/18 crate tests pass.
