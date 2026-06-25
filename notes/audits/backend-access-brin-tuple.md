# Audit: backend-access-brin-tuple

- **Unit:** `backend-access-brin-tuple` (C: `src/backend/access/brin/brin_tuple.c`, PostgreSQL 18.3)
- **Branch:** `port/backend-access-brin-tuple`
- **Date:** 2026-06-12
- **Model:** Opus 4.8 (1M context) — `claude-opus-4-8[1m]`
- **Verdict:** **PASS**

Independent function-by-function audit per `.claude/skills/audit-crate/SKILL.md`.
Re-derived from the C source, the c2rust rendering
(`../pgrust/c2rust-runs/backend-access-brin-tuple/src/brin_tuple.rs`), and the
Rust port; the port's own committed self-review was ignored and overwritten.

## 1. Function inventory

`brin_tuple.c` defines exactly **10** functions (one static inline helper +
nine exported/static). Cross-checked against the c2rust output, which renders
all 10 — no `#if`-gated functions outside the build config. Every function gets
a row.

| # | C function (loc) | Rust port (loc) | Verdict |
|---|---|---|---|
| 1 | `brtuple_disk_tupdesc` static (brin_tuple.c:60) | `internal::brtuple_disk_tupdesc` (internal.rs:250) | MATCH |
| 2 | `brin_form_tuple` (brin_tuple.c:98) | `tuple::brin_form_tuple` (tuple.rs:39) | MATCH (3 SEAMED callees) |
| 3 | `brin_form_placeholder_tuple` (brin_tuple.c:387) | `tuple::brin_form_placeholder_tuple` (tuple.rs:267) | MATCH |
| 4 | `brin_free_tuple` (brin_tuple.c:432) | `tuple::brin_free_tuple` (tuple.rs:309) | MATCH |
| 5 | `brin_copy_tuple` (brin_tuple.c:445) | `tuple::brin_copy_tuple` (tuple.rs:319) | MATCH |
| 6 | `brin_tuples_equal` (brin_tuple.c:464) | `tuple::brin_tuples_equal` (tuple.rs:351) | MATCH |
| 7 | `brin_new_memtuple` (brin_tuple.c:481) | `tuple::brin_new_memtuple` (tuple.rs:365) | MATCH |
| 8 | `brin_memtuple_initialize` (brin_tuple.c:510) | `tuple::brin_memtuple_initialize` (tuple.rs:385) | MATCH |
| 9 | `brin_deform_tuple` (brin_tuple.c:552) | `tuple::brin_deform_tuple` (tuple.rs:426) | MATCH (1 SEAMED callee) |
| 10 | `brin_deconstruct_tuple` static inline (brin_tuple.c:644) | `tuple::brin_deconstruct_tuple` (tuple.rs:527) | MATCH |

## 2. Per-function notes

### 1. `brtuple_disk_tupdesc` — MATCH
C lazily builds and caches `bd_disktdesc` from each stored column's
`oi_typcache[j]->type_id` via `TupleDescInitEntry(.., type_id, -1, 0)`. The port
recomputes a fresh `TupleDescData` on each call from the same `TypeCacheEntry`
parameters (`compact_attr_from_typcache` + `disk_form_attr`). Verified
behaviorally identical:
- `CompactAttribute` field-fill matches `populate_compact_attribute_internal`
  (tupdesc.c:64): `attcacheoff=-1`, `attlen=typlen`, `attbyval=typbyval`,
  `attispackable = typstorage != TYPSTORAGE_PLAIN` (exact, tupdesc.c:74),
  `attalignby` mapping (INT→4, CHAR→1, DOUBLE→8, SHORT→2) matches the C switch.
- `TupleDescInitEntry` with `typmod=-1` copies the type's own `typlen/typbyval/
  typalign/typstorage`; these equal the `TypeCacheEntry` fields the port reads,
  so the descriptor is identical. Skipping the catalog round-trip cleanly avoids
  an unported `TupleDescInitEntry` dependency.
- The cache is purely an optimization: `brin_deconstruct_tuple` itself explicitly
  refuses to cache offsets (brin_tuple.c:681), and the descriptor is recomputed
  every time it is needed within a call in C too. Recompute-on-demand is an
  optimization-only divergence (ledgered in CATALOG/module docs), not a logic
  difference. No ambient context introduced.

### 2. `brin_form_tuple` — MATCH
Walked line-by-line vs c2rust:
- `values`/`nulls` scratch arrays (`bd_totalstored` long), `idxattno` cursor,
  per-keyno `bv_allnulls` short-circuit setting all `oi_nstored` null bits and
  `anynulls=true`; `bv_hasnulls → anynulls=true`. Match.
- TOAST_INDEX_HACK path: non-varlena (`typlen != -1`) copies value directly;
  varlena path: external → detoast; `!VARATT_IS_EXTENDED && VARSIZE > TOAST_INDEX_TARGET
  && (typstorage EXTENDED|MAIN)` → compress with `attcompression` (same type) or
  `InvalidCompressionMethod`. The port's `varatt_is_external` (`b[0]==0x01`),
  `varatt_is_extended` (`b[0]&0x03 != 0`), `varsize` (`(hdr>>2)&0x3FFFFFFF`) match
  the varatt.h macros. `TOAST_INDEX_TARGET = 8160/4 = 2040` verified (MaxHeapTupleSize
  is 8160 via TOAST_TUPLE_TARGET == MaximumBytesPerTuple(4); /16 in C =
  MaxHeapTupleSize/16; the port computes TOAST_TUPLE_TARGET/4 = 8160/4 — both
  equal 2040). `INVALID_COMPRESSION_METHOD = -1` matches `'\0'`-cast/`InvalidCompressionMethod`.
- Length accounting: `len = SizeOfBrinTuple (5)`; `+ BITMAPLEN(natts*2)` when
  anynulls; `MAXALIGN` → `hoff`; `+ heap_compute_data_size`; `MAXALIGN`. Matches.
- `bt_blkno`/`bt_info=hoff`; data area filled by real `heap_fill_tuple`
  (direct dep), copying only the data bytes (the phony infomask/bitmap discarded,
  as in C). The two null-bitmap loops (allnulls then hasnulls) reproduce the
  `bitP/bitmask/HIGHBIT` bit-walk exactly, reversing the null sense (1==null).
  `BRIN_NULLS_MASK`/`PLACEHOLDER`/`EMPTY_RANGE` set under the same predicates.
- C `pfree(untoasted_values[i])` loop maps to scope drop of the owned vecs.
- SEAMED callees (see §3): `brin_serialize`, `detoast_external_attr`,
  `toast_compress_datum`. Compression returning `None` == C `PointerGetDatum(NULL)`;
  the detoasted/owned bytes survive and are stored, matching C.

### 3. `brin_form_placeholder_tuple` — MATCH
`len = SizeOfBrinTuple + BITMAPLEN(natts*2)`, `MAXALIGN→hoff`, `bt_info =
hoff | NULLS|PLACEHOLDER|EMPTY_RANGE`, single allnulls bit-walk setting every
attribute's bit. Identical to C; hasnulls intentionally left unset.

### 4. `brin_free_tuple` — MATCH
C `pfree(tuple)` → owned-image drop.

### 5. `brin_copy_tuple` — MATCH
`!destsz || *destsz==0 → palloc(len)` (destsz unchanged); `len > *destsz →
repalloc + *destsz=len`; else reuse. The port's match arms reproduce all three
branches incl. the not-updating-destsz-when-zero case; final `memcpy` is the
`extend_from_slice(&tuple[..len])`.

### 6. `brin_tuples_equal` — MATCH
`alen != blen → false`; else `memcmp == 0`. Port: `a[..alen] == b[..blen]`.

### 7. `brin_new_memtuple` — MATCH
C single-palloc block + per-tuple `bt_context` AllocSet → owned `bt_columns`
vec in `mcx`; `bt_empty_range=true`; delegates to `brin_memtuple_initialize`.
The C trailing `Datum` area / `bt_values`/`bt_allnulls`/`bt_hasnulls` scratch
buffers become owned vecs allocated where used (deform). Behavior identical.

### 8. `brin_memtuple_initialize` — MATCH
C `MemoryContextReset` + per-column init (`bv_attno=i+1`, `bv_allnulls=true`,
`bv_hasnulls=false`, `bv_values` slice of `oi_nstored`, `bv_mem_value=NULL`,
`bv_serialize=NULL`) → clear+rebuild `bt_columns` with the same fields
(`bv_mem_value=None`, `bv_has_serialize=false`); `bt_empty_range=true`. Match.

### 9. `brin_deform_tuple` — MATCH
Reuse-or-allocate memtuple; `placeholder`/`!empty_range`/`bt_blkno` from
`bt_info`; `tp = tuple + BrinTupleDataOffset`; `nullbits = HasNulls ? tuple +
SizeOfBrinTuple : NULL`; calls `brin_deconstruct_tuple`; per-column loop skips
allnulls (`valueno += oi_nstored`), else `datumCopy`s each stored value (SEAMED,
§3) keyed by `oi_typcache[i]->typbyval/typlen`, sets `bv_hasnulls`,
`bv_allnulls=false`, `bv_mem_value=None`, `bv_serialize=NULL`. The C
`MemoryContextSwitchTo(bt_context)` for the copies maps to allocation in `mcx`.
Match.

### 10. `brin_deconstruct_tuple` — MATCH
First loop sets `allnulls[attnum] = nulls && !att_isnull(attnum, nullbits)` and
`hasnulls[attnum] = nulls && !att_isnull(natts+attnum, nullbits)` — the
double-width bitmap with reversed sense. `att_isnull` (`bits[att>>3] &
(1<<(att&7)) == 0`) matches tupmacs.h. Second loop walks the disk descriptor:
allnulls → skip `oi_nstored`; else per stored attr: `attlen==-1` uses
`att_pointer_alignby` (no pad if `tp[off]!=0`, i.e. short header —
`VARATT_NOT_PAD_BYTE`), else `att_nominal_alignby`; `fetchatt` reads a by-value
word (1/2/4/8) or copies a by-ref span (`varsize_any` for varlena, `attlen` for
fixed, `strlen+1` for cstring); then `att_addlength_pointer` advances `off`.
`varsize_any` dispatch (external `VARHDRSZ_EXTERNAL(2)+VARTAG_SIZE`, short
`(hdr>>1)&0x7F`, plain `VARSIZE`) and `VARTAG_SIZE` (INDIRECT 8, EXPANDED 8,
ONDISK 16) match varatt.h. The `fetchatt` unsupported-byval-length and the
`alignby_for` invalid-attalign arms panic on unreachable inputs — mirrors C's
`elog(ERROR)` on the same unreachable arms (common-heaptuple convention). Match.

## 3. Seam audit

**Ownership by C-source coverage.** This unit's only `c_sources` file is
`brin_tuple.c`. No `crates/backend-access-brin-tuple-seams` exists, and none is
required: this unit is a leaf consumed by revmap/pageops/entry, declaring no
inward seams. Therefore there is **no `init_seams()` obligation** for this crate
(verified: lib.rs has no installer, and none is needed). Not a finding.

All seam *calls* are outward to crates owned by other (unported) units; each is
a justified real-dependency break and a thin marshal+delegate (no branching /
node construction / computation on the seam path):

| Seam call | Owner unit (declares it) | Cycle justification | Marshal check |
|---|---|---|---|
| `backend_access_brin_entry_seams::brin_serialize` | `backend-access-brin-entry` (`brin.c`/opclasses) — `brin_serialize_callback_type` | opclass callback; circular w/ brin.c | thin: alloc dst, one call, fill |
| `backend_access_common_detoast_seams::detoast_external_attr` | `backend-access-common-detoast` (`detoast.c`) | detoast unported | thin: bytes in/out |
| `backend_access_common_toast_internals_seams::toast_compress_datum` | `backend-access-common-toast-internals` (`toast_internals.c`) | unported | thin: bytes+cmethod → Option<bytes> (None == NULL) |
| `backend_utils_adt_scalar_seams::datum_copy` | `backend-utils-adt-scalar` (`datum.c`) | unported | thin: value+typbyval+typlen → value |

Each seam declaration was confirmed present in its **owner** crate's `seam!`
block (not declared here), and signatures carry `Mcx`+`PgResult` where they
allocate / can `ereport(ERROR)`. No `set()` calls live in this crate (it owns
none). No seam call replaces in-crate logic with a "call to somewhere else" —
every seam is a genuinely external operation (opclass dispatch, detoast,
compress, datum deep-copy). All BRIN-specific bit-twiddling, length accounting,
null-bitmap walks, and alignment field-walk are in-crate, matching C.

(The CATALOG note also lists a `lookup_type_cache` extension to
`backend-utils-cache-typcache-seams`; that is used by a future BrinDesc builder,
not by `brin_tuple.c`, and is not on any path in this unit — out of scope here.)

## 3b. Design conformance

- **Opacity (types.md rules 6-7):** `BrinOpcInfo::oi_opaque` is a real C `void *`
  extension slot rendered as `Option<PgBox<OpaqueOpcInfo=[u8]>>` — inherited
  opacity (the opclass owns the real shape), not invented. `BrinDesc`,
  `BrinMemTuple`, `BrinValues`, `BrinOpcInfo` are real structs trimmed to
  consumed fields, verified against `brin_internal.h`/`brin_tuple.h`. The on-disk
  `BrinTuple` is a faithful `PgVec<u8>` byte image with header accessors — not an
  invented handle. PASS.
- **Mcx + PgResult on allocating fns/seams:** every allocating function and seam
  takes `Mcx` and returns `PgResult`. PASS.
- **No shared statics / ambient globals:** the C per-`BrinDesc` `bd_disktdesc`
  cache is dropped in favor of recompute-on-demand (no shared static, no ambient
  ctx). `bt_context` AllocSet → owned vecs. PASS.
- **No locks across `?`, no registry side tables, no unledgered divergence
  markers:** none present; the recompute and panic-on-unreachable choices are
  ledgered in the module docs and CATALOG note. PASS.
- **Constants verified against headers (not memory):** all four `BRIN_*` masks
  (0x1F/0x20/0x40/0x80), `SizeOfBrinTuple=5`, `BT_INFO_OFFSET=4`,
  `VARTAG_SIZE`, `VARHDRSZ_EXTERNAL=2`, `attispackable` predicate, `attalignby`
  map, `TOAST_INDEX_TARGET=2040` checked against `brin_tuple.h`, `varatt.h`,
  `tupdesc.c`, `heaptoast.h`. PASS.

## 4. Verdict

**PASS.** All 10 C functions are MATCH (with four callees properly SEAMED to
their owner crates, each a thin marshal+delegate over a real dependency break).
Zero seam findings; no `init_seams()` obligation (leaf, owns no seam crate).
Design conformance clean. The crate builds (`cargo build -p
backend-access-brin-tuple` succeeds).

CATALOG.tsv row may be advanced to `audited`.
