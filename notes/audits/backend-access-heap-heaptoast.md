# Audit: backend-access-heap-heaptoast

- **Unit**: `backend-access-heap-heaptoast` (`src/backend/access/heap/heaptoast.c`, PostgreSQL 18.3)
- **Crates**: `crates/backend-access-heap-heaptoast` (+ owned seam crate
  `crates/backend-access-heap-heaptoast-seams`; consumed seam crates
  `backend-access-common-detoast-seams`, `backend-access-common-toast-internals-seams`,
  `backend-access-index-genam-seams`, `backend-access-table-toast-helper-seams`;
  vocabulary crate `types-scan`)
- **C source**: `../pgrust/postgres-18.3/src/backend/access/heap/heaptoast.c` (791 lines)
- **c2rust**: `../pgrust/c2rust-runs/backend-access-heap-heaptoast/src/heaptoast.rs`
- **Auditor**: independent re-derivation from C + c2rust; constants verified against headers
  (`heaptoast.h`, `htup_details.h`, `toast_helper.h`, `stratnum.h`, `pg_proc.dat`,
  `pg_collation.dat`, `heapam.h`, `varatt.h` via c2rust expansion).

## Function inventory and verdicts

`heaptoast.c` contains exactly 6 function definitions (no statics, no `#if` branches — verified
by grep). The port additionally implements two header/other-unit inline helpers used by
`heap_fetch_toast_slice` (`fastgetattr` from `htup_details.h`, `ScanKeyInit` from
`access/common/scankey.c`) and one local `att_isnull` (`tupmacs.h`); these get rows too.

| C function (heaptoast.c) | Port location | Verdict | Notes |
|---|---|---|---|
| `heap_toast_delete` (l.42) | `lib.rs::heap_toast_delete` | MATCH | Deform via `heap_deform_tuple` (direct dep), then `toast_delete_external` across the toast-internals seam. relkind Assert is a no-op in NDEBUG C (c2rust confirms absent); documented as a comment. |
| `heap_toast_insert_or_update` (l.95) | `lib.rs::heap_toast_insert_or_update` | MATCH | `options &= ~HEAP_INSERT_SPECULATIVE` (0x0010, heapam.h ✓). hoff = MAXALIGN(23 + BITMAPLEN if TOAST_HAS_NULLS); `maxDataLen = RelationGetToastTupleTarget(rel, 2032) - hoff` via relcache seam, wrapping unsigned subtraction as in C `Size` arithmetic. All four pass loops byte-for-byte: round-1 compresses EXTENDED / marks EXTERNAL `TOASTCOL_INCOMPRESSIBLE` (0x0020 ✓) and externalizes when `tai_size > maxDataLen && reltoastrelid != InvalidOid`; round-2 externalizes EXTENDED/EXTERNAL while a toast table exists; round-3 compresses MAIN; round-4 externalizes MAIN at `TOAST_TUPLE_TARGET_MAIN - hoff` (8160 ✓). `reltoastrelid` read once (cannot change mid-call; C re-reads the same field). Rebuild path: C's palloc0 + header memcpy + SetNatts + t_hoff + heap_fill_tuple is realized as `heap_form_tuple` (provably identical size/hoff/fill: heap_form_tuple's `hasnull` ⇔ `TOAST_HAS_NULLS`, since toast_tuple_init sets that flag iff some `ttc_isnull[i]` and no pass mutates isnull) plus copying `t_self`/`t_tableOid`/`t_choice`/`t_ctid`, infomask composed as `(old & ~(HASNULL\|HASVARWIDTH\|HASEXTERNAL)) \| (filled & those)` — exactly what memcpy+heap_fill_tuple produces — and `t_infomask2 = old` then `HeapTupleHeaderSetNatts` (mask 0x07FF ✓). Returns `None` for C's "return newtup unchanged". `toast_tuple_cleanup` runs in both branches, after the rebuild, as in C. |
| `toast_flatten_tuple` (l.349) | `lib.rs::toast_flatten_tuple` | MATCH | Non-null, `attlen == -1`, `VARATT_IS_EXTERNAL` (header byte == 0x01 ✓ c2rust) → `detoast_external_attr` seam. After `heap_form_tuple`: copies `t_self`, `t_tableOid`, `t_choice`, `t_ctid`; XACT-mask splice `HEAP_XACT_MASK` 0xFFF0 / `HEAP2_XACT_MASK` 0xE000 (htup_details.h ✓). C's `toast_free[]`/pfree is Rust drop. No SetNatts in C — none in port (heap_form_tuple already stamped natts). |
| `toast_flatten_tuple_to_datum` (l.448) | `lib.rs::toast_flatten_tuple_to_datum` | MATCH | tmptup control struct subsumed by `FormedTuple`. Per-attr: null → `has_nulls`; else varlena and `VARATT_IS_EXTERNAL \|\| VARATT_IS_COMPRESSED` (0x01 / `&3==2` ✓) → `detoast_attr` seam. Rebuild via `heap_form_tuple` (asserted `has_nulls == any(isnull)`, the exact C identity) which also sets the composite-Datum fields `SetDatumLength/SetTypeId/SetTypMod` — same final t_choice as C's memcpy-then-overwrite (the three datum fields cover the whole 12-byte union). `t_ctid` + non-fill infomask bits + `t_infomask2`+SetNatts copied from source header, matching memcpy semantics. Returns the formed tuple as the owned stand-in for `PointerGetDatum(new_data)`. |
| `toast_build_flattened_tuple` (l.562) | `lib.rs::toast_build_flattened_tuple` | MATCH | values memcpy → `clone_in` copies; per-attr external expansion via `detoast_external_attr`; `heap_form_tuple` with caller's isnull; `freeable_values`/pfree is drop. |
| `heap_fetch_toast_slice` (l.625) | `lib.rs::heap_fetch_toast_slice` | MATCH | `totalchunks/startchunk/endchunk` computed with wrapping unsigned division exactly as C's `uintptr_t` arithmetic (c2rust l.2607–2623 confirms). Scan keys: attno 1 `BTEqual`/`F_OIDEQ`(184) on valueid; 1 key if full range, attno 2 `BTEqual`/`F_INT4EQ`(65) if single chunk, else `BTGreaterEqual`/`F_INT4GE`(150) + `BTLessEqual`/`F_INT4LE`(149) — all OIDs verified in `pg_proc.dat`, strategies 3/4/2 in `stratnum.h` ✓. `toast_open_indexes`/`get_toast_snapshot`/`systable_beginscan_ordered`(Forward)/`getnext`/`endscan`/`toast_close_indexes` across their seams in C order. Chunk classification: `!VARATT_IS_EXTENDED` → VARSIZE−4/VARDATA; `VARATT_IS_SHORT` → VARSIZE_1B−1/VARDATA_SHORT; else `elog(ERROR, "found toasted toast chunk …")` (default XX000, matching elog). Corruption ereports: chunk-number, range, size, and trailing missing-chunk checks all `ERRCODE_DATA_CORRUPTED` (XX001 ✓ c2rust `('X'…'1')` expansion), `errmsg_internal`, identical format text and argument order, names via the relcache relname seam. `expected_size` last-chunk arithmetic in wrapping unsigned, as C. Copy window `chcpystrt/chcpyend` and destination offset `curchunk*CHUNK − sliceoffset + chcpystrt` reproduced over the caller's `VARDATA(result)` slice. Errors return before endscan/close — same as C's longjmp. Extra defensive `ERRCODE_DATA_CORRUPTED` errors for a by-val/by-ref model mismatch on columns 2/3 sit where C would be silently reinterpreting bits; unreachable for a well-formed toast table. |
| `fastgetattr` (htup_details.h l.866, inline) | `lib.rs::fastgetattr` | MATCH | Null-bitmap check (`HEAP_HASNULL` + `att_isnull`, bit-clear-means-null ✓) then `nocachegetattr`; C's `attcacheoff >= 0` branch is a pure cache fast path computing the identical value. |
| `att_isnull` (tupmacs.h, inline) | `lib.rs::att_isnull` | MATCH | `(bits[att>>3] >> (att&7)) & 1 == 0`. |
| `ScanKeyInit` (scankey.c l.76) | `lib.rs::ScanKeyInit` | MATCH (after fix) | flags=0, attno, strategy, subtype=InvalidOid, **collation=C_COLLATION_OID (950, `pg_collation.dat` ✓ — fixed this audit, was InvalidOid)**, argument; `fmgr_info` is modeled as recording `fn_oid`, with the real lookup deferred to the scan code behind the genam seam (documented vocabulary trim). |

### Constants verified against headers
- `TOAST_TUPLE_TARGET` 2032, `TOAST_TUPLE_TARGET_MAIN` 8160, `EXTERN_TUPLE_MAX_SIZE` 2032,
  `TOAST_MAX_CHUNK_SIZE` 1996 — derived by the same `MaximumBytesPerTuple` formula
  (BLCKSZ 8192, page header 24, ItemId 4, MAXALIGN 8, SizeofHeapTupleHeader 23) and unit-tested;
  c2rust constant expansion agrees.
- `HEAP_INSERT_SPECULATIVE` 0x0010; `TOAST_HAS_NULLS` 0x04 / `TOAST_NEEDS_CHANGE` 0x08 /
  `TOASTCOL_INCOMPRESSIBLE` 0x20; `HEAP_HASNULL\|HASVARWIDTH\|HASEXTERNAL` 0x1/0x2/0x4;
  `HEAP_XACT_MASK` 0xFFF0; `HEAP2_XACT_MASK` 0xE000; `HEAP_NATTS_MASK` 0x07FF;
  `MaxHeapAttributeNumber` 1600; `MaxTupleAttributeNumber` 1664; `TYPSTORAGE_EXTENDED` 'x';
  `AccessShareLock` 1; varatt header predicates and VARSIZE shifts — all match headers/c2rust.

## Seam audit

Owned seams (`backend-access-heap-heaptoast-seams`): `toast_flatten_tuple_to_datum`,
`toast_flatten_tuple`. Both installed by `backend_access_heap_heaptoast::init_seams()`
(nothing but `set()` calls); `seams-init::init_all()` calls it. No `set()` anywhere else
(grep-verified). The only consumer is `backend-access-common-heaptuple::heap_copy_tuple_as_datum`
(`HEAP_HASEXTERNAL` branch), a thin one-call delegate; the cycle is real
(heaptoast → heaptuple `heap_form/deform/fill`; heaptuple → heaptoast flatten).

Outward seams, all justified (owner units unported) and all thin marshal + one call:
- `backend-access-common-detoast-seams`: `detoast_external_attr`, `detoast_attr` (detoast.c).
- `backend-access-common-toast-internals-seams`: `toast_delete_external`,
  `toast_open_indexes`, `toast_close_indexes`, `get_toast_snapshot` (toast_internals.c).
- `backend-access-index-genam-seams`: `systable_beginscan_ordered`,
  `systable_getnext_ordered`, `systable_endscan_ordered` (genam.c), scan state crossing as
  `SysScanHandle`.
- `backend-access-table-toast-helper-seams`: `toast_tuple_init`,
  `toast_tuple_find_biggest_attribute`, `toast_tuple_try_compression`,
  `toast_tuple_externalize`, `toast_tuple_cleanup` (toast_helper.c), context crossing as the
  transparent owned `ToastTupleContext` threaded `&mut`, exactly as C threads `&ttc`.
- `backend-utils-cache-relcache-seams` (pre-existing): `rd_att`, `reltoastrelid`,
  `toast_tuple_target`, `relname` field reads by OID.

`types-scan` vocabulary: `StrategyNumber` values 0–5 match `stratnum.h`; `ScanDirection`
−1/0/1 matches `sdir.h`; `ScanKeyData` fields mirror `skey.h` with the documented `FmgrInfo`
trim; `SysScanHandle`/`SnapshotHandle` are opaque tickets. No findings.

No branching, node construction, or computation found in any seam path.

## Findings and fixes

1. **DIVERGES (fixed)** — `ScanKeyInit` stamped `sk_collation = InvalidOid`; C stamps
   `C_COLLATION_OID` (950). Fixed in `crates/backend-access-heap-heaptoast/src/lib.rs`
   (new `C_COLLATION_OID` constant) and the unit test updated to pin 950. Re-audited the
   fixed function from scratch against `scankey.c`: all seven field assignments now match.

## Verdict

**PASS** (after 1 fix round). All 6 `heaptoast.c` functions MATCH; helper inlines MATCH;
seams wired per the rules; `cargo build` and the crate's tests green.
