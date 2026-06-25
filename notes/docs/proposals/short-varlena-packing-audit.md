# Short-varlena-header packing — reader-surface audit (Phase 0)

`SHORT_VARLENA_PACKING` is hardcoded `false`
(`crates/backend-access-common-heaptuple/src/lib.rs`). C packs short (<127 B),
non-compressed, packable varlenas (`text`/`bytea`/`varchar`/`numeric`/`array`/
`jsonb`/...) into a 1-byte on-disk header. pgrust keeps the full 4-byte header on
every stored varlena so that the many open-coded readers that strip a FIXED
4-byte `VARHDRSZ` header read the payload, not 3 bytes into it. Flipping the flag
makes pgrust tuple sizes match C byte-for-byte (the copy2 physical-row-order
payoff) — but only once every reader of a STORED varlena is header-form-agnostic
(`VARSIZE_ANY` / `VARDATA_ANY` semantics: handle 1-byte short, 4-byte long, AND
compressed/external TOAST forms).

## What is already correct (no change needed)

- **Heap deform** (`heap_deform_tuple` → `fetchatt` → `att_addlength_pointer`,
  heaptuple `lib.rs`): uses `varsize_any` + `att_pointer_alignby`
  (`VARATT_NOT_PAD_BYTE`). Returns the raw on-disk varlena image verbatim,
  whatever its header form. **Phase-1 deform-side is already header-agnostic.**
- **Index deform** (`index_deform_tuple` / `nocache_index_getattr`,
  indextuple `lib.rs`): same `varsize_any` + `att_addlength_pointer`. NOTE:
  `index_form_tuple` reuses `heap_compute_data_size` + `heap_fill_tuple`, so the
  SAME flag governs index-tuple storage — index readers must be agnostic too.
- **detoast** (`backend-access-common-detoast`): `varsize_any`, `vardata_short`,
  `detoast_attr` (short arm at the `VARHDRSZ_SHORT` slice), `pg_varsize_any`
  exported via seam — fully header-agnostic.
- **`vardata_any_slice`** (`backend-utils-adt-varlena/src/lib.rs`): handles
  short (1B) + 4B-U. Does NOT detoast compressed/external (falls back to a 4B
  strip) — callers must have detoasted those first.

## The break class (Phase 2): open-coded fixed-4-byte strips on stored values

The fmgr argument boundary does **NOT** detoast/unpack: `EEOP_FUNCEXPR` gathers
`fcinfo->args[i].value` straight from the cell, and a `Var` cell is the raw slot
deform image. So with packing ON, a stored short-header varlena flows VERBATIM
into an fmgr arg. C tolerates this because cores use `VARDATA_ANY` /
`PG_GETARG_*_PP` / `PG_DETOAST_DATUM`. pgrust's per-crate `arg_text` / `arg_varlena`
helpers instead do a fixed `&image[VARHDRSZ..]` (or `&image[4..]`), which lands 3
bytes into a short value's payload — silent front-truncation = data corruption.

This pattern is **duplicated per crate** (~78 `arg_*`/payload helper definitions;
no single choke point). Each broken helper needs `VARDATA_ANY` semantics, and the
fully-faithful form must also `pg_detoast_datum_packed` a compressed/external arg
(allocating + fallible → signature change).

### BREAKS (fmgr-arg / stored-value readers that strip a fixed 4-byte header)

HARDENED so far (all behavior-preserving while the flag is OFF; each now uses a
short-aware `vardata_any`: short 1-byte low-bit-set header → skip 1, else
`VARHDRSZ`):

- batch 1 (6809eeab1 / f068d9938): adt-{char,cryptohashfuncs,quote,regexp,
  ts-small,tsvector-core,varchar}, varlena/string_agg, hash-pg-crc, mb-mbutils,
  access-hashfunc; adt-like was already header-agnostic.
- batch 2a (140f7c74a): adt-{datetime,json,jsonpath,jsonpath-exec,network,
  tsgistidx}, misc-{guc-funcs,more}, transam-xlogfuncs.
- batch 2b (76e543bf7): gin-core-probe dispatch (all 7 sites: jsonb/jsonb_path
  extractValue, jsonb extractQuery Contains/Exists/ExistsArray, gin_cmp_prefix
  comparePartial); execExprInterp eval_json_xml `xml_arg_payload`.
  (index-genam/decode.rs tgargs reader ALREADY had a short-aware vardata_any.)
- batch 2c (25ae3d6e4): adt-jsonfuncs — shared `common::vardata_any`, applied to
  every ARG-sourced internal `jb[VARHDRSZ..]`/`json_image[VARHDRSZ..]` strip in
  fmgr_builtins, getfield, setops, keys, elements, recordset, iterate, each,
  strip, length, populate (arg paths). Binary-len fields now use the post-strip
  slice len. populate.rs:986 (JsonbValueToJsonb result) kept fixed — fresh.
- batch 2d (7eae65e5f): adt-formatting fmgr_boundary `varlena_payload`.
- batch 2e (20a2e0807): execSRF — regexp_matches, regexp_split, string_to_table,
  json_srf, ts_parse, pg_input_error_info, pg_ls_dir, pg_walfile,
  pg_stat_get_progress_info (all read a stored text/json arg verbatim).
- batch 2f (cb88ee86b): adt-jsonb fmgr_builtins `arg_jsonb_root`+`arg_text_payload`,
  commands-async, replication-logical-origin, pgstatfuncs/composite,
  statistics-mcv (pg_mcv_list). network / network-spgist already short-aware.
- batch 2g (a50db33fb): adt-jsonb lib.rs arg-sourced jsonb readers — jsonb_out,
  jsonb_send, jsonb_typeof, JsonbUnquote, cast_extract, the JSONTYPE_JSONB
  datum_to_jsonb branch. Freshly-built sites (splice_jsonb_tokens parse result,
  jsonb_agg/object_agg JsonbValueToJsonb outputs at lib.rs 1454/1683/1846/1881)
  KEEP the fixed VARHDRSZ strip.

batch 3 (this lane):
- adt-jsonpath cores (lib.rs `jsonpath_header`/`jsonpath_data` via new
  `varlena_data_off`): the image consumers (`jspInit`/`jsonpath_is_lax`/
  `jsonPathToCstring`/`jspIsMutable` + `fc_jsonpath_out`'s `arg_jsonpath_image`)
  now compute the version-word + node-data offset from the actual header form
  (1B short / 4B long), not fixed bytes 4 and 8. The flatten/write path keeps a
  4B header (fresh value). This is the "principal blocker" from batch 2 — DONE.
- final triage sweep (6 sites): adt-ascii `arg_text`, adt-formatting
  `arg_text_body`, commands-sequence `text_to_str` (pg_getarg_text_pp =
  packed-detoast, preserves short), commands-publicationcmds `text_datum_str`
  (stored catalog prqual, never detoasted on this path),
  statistics-{dependencies,mvdistinct} `arg_bytea_body`, and mvdistinct's
  `statext_ndistinct_deserialize` (varsize_any now decodes short + exhdr/cursor
  use varhdrsz_any).

PHASE-3 FLIP ATTEMPTED + REVERTED (numeric-hdr lane): with every reader above
hardened, flipping `SHORT_VARLENA_PACKING = true` and running varlena_gate.sh
showed text/varchar/bytea/arrays/jsonb/composite/indexed-probe all byte-exact,
but **numeric short and numeric long returned `Infinity`/`NaN`** — a reader
gap. Reverted the flip; kept the reader hardening. (numeric deserializer then
hardened in `8de2c6fb0`.)

CAPSTONE FLIP ATTEMPTED + REVERTED (lane-varlena-flip, this lane): with numeric
landed + two more readers hardened (`0c62c5c6e`: brin minmax-multi inet distance
`parse_inet`, numeric agg `numeric_with_header`), the per-type gate
(`varlena_gate.sh`) was ALL-PASS under the flip — text/varchar/bytea short+long
+TOAST, **numeric short+long**, int[]/text[], jsonb, point/composite, AND the
indexed-text probe (index-tuple round-trip) all byte-exact. BUT a representative
regress sweep (baseline flag-OFF vs candidate flag-ON, same targeted schedule,
fresh C-initdb cluster, best-of .out/_1/_2 diff) showed **8 files REGRESS + copy2
got WORSE**, so the flip was REVERTED. The per-type gate is necessary but NOT
sufficient — it does not exercise the readers below. Empirical regression list
(baseline diff -> candidate diff; PASS=0):
- **rangetypes** 0 -> 559: `type with OID 256 does not exist` — a range struct
  deserializer reads `rangetypid` at a FIXED `[VARHDRSZ+..]` on a short-packed
  range (a path NOT covered by `datum_get_range_type_p`/`range_p_from_varlena_bytes`
  — likely the range hash/btree opclass, a range inside a composite, or a
  serialize-side reader).
- **multirangetypes** 0 -> 122: `type with OID 768 does not exist` (same range
  struct break, multirange element path) + `array is not a valid oidvector` (a
  catalog oidvector reader, see create_index).
- **inet** 0 -> 156: `range end index 23 out of range for slice of length 20` —
  an inet reader stripping a FIXED 4-byte header on a short (~20B IPv4) inet,
  overrunning the slice. (DISTINCT from `arg_inet` and the brin distance path
  hardened here — likely the GiST/SP-GiST inet index opclass or a network-fn
  reader.)
- **jsonb** 0 -> 608: `invalid jsonb container type: 0x00000420` — a jsonb
  container reader at a FIXED post-VARHDRSZ offset on a short-packed stored jsonb
  (an index/recheck path the canonical detoasting arg path does not cover).
- **jsonpath** 0 -> 12: `range start index 4 out of range for slice of length 3`
  — a jsonpath value < 4 bytes read at a fixed `[4..]` (a residual the
  `arg_jsonpath_image` rework missed).
- **tsearch** 0 -> 446: `index out of bounds: the len is 5 but the index is 5` —
  a tsvector stored-key reader (a path NOT covered by the `arg_tsvector` un-pack;
  likely the GiST/GIN tsvector index opclass or a rank/headline reader).
- **strings** 0 -> 20 / **char** 0 -> 0 (char passes but the CAST does not):
  `CAST(f1 AS text) FROM CHAR_TBL` front-truncates `a`/`ab`/`abcd` -> ``/``/`d` —
  the **bpchar (CHAR(n)) -> text cast** strips a FIXED 4-byte header on a
  short-packed bpchar (3-byte front-truncation = classic short-header miss).
- **create_index** 0 -> 31: `conexclop is not a 1-D array` /
  `stxkind is not a 1-D char array` — **catalog array/vector readers**
  (`pg_constraint.conexclop` oidvector, `pg_statistic_ext.stxkind` char[]) read
  `ARR_NDIM` at a fixed 4-byte offset on a short-packed catalog array. Catalog
  anyarray/oidvector/int2vector columns ALSO get short-packed; their open-coded
  decoders are in the break set (broad surface).
- **copy2** 126 -> 141: did NOT improve — the varlena physical-row-order payoff
  did NOT materialize (the 126 baseline diff is trigger-firing ORDER + a `widget`
  type-missing error-msg gap, NEITHER governed by tuple varlena size); the flip
  added 15 NEW difflines (corruption).

NET: char/varchar/text(col)/numeric/arrays/btree_index/brin/gin/gist/json/rowtypes
stayed byte-exact (0->0) under the flip — those reader classes ARE done. The flip
remains BLOCKED on the 8 reader classes above. The per-type `varlena_gate.sh`
gate is INSUFFICIENT as a flip gate (it passed while 8 regress files broke); the
representative regress sweep (or full suite) is the real gate.

PRECISE FIX TARGETS (index-AM stored-key readers, verified file:line by a
dedicated read-only audit this lane — mechanism re-confirmed: `index_form_tuple`
-> `heap_fill_tuple` short-packs by attstorage; `fetchatt`
(`backend-access-common-indextuple/src/lib.rs:850-862`) returns the raw stored
image unchanged; the fmgr by-ref dispatch `detoast_ref_arg_if_toasted`
(`backend-utils-fmgr-core/src/lib.rs:3511-3566`) normalizes only EXTERNAL /
COMPRESSED, NOT SHORT — so a fixed 4-byte strip on a stored short key mis-reads
by 3 bytes):
1. **GiST inet/cidr opclass** -> the `inet` 0->156 regression. Root:
   `types-network/src/lib.rs:179-188` `GistInetKey::from_datum_bytes` strips a
   FIXED 4-byte header (reads `b[4]`/`b[5]`/`b[6]`/`b[7..23]`). A ~23B inet is
   short-packable. 7 call sites in `backend-access-gist-proc/src/lib.rs`: 286
   (`inet_keys_from_vec` -> union/picksplit), 1671 (`inet_gist_consistent` — the
   smoking gun: it detoasts the QUERY at 1675 but NOT the stored key), 1919/1920
   (penalty), 2051/2052 (same), 2110 (fetch). FIXED (`867e8d9a5`,
   lane-varlena-index): `GistInetKey::from_datum_bytes` now reads the payload at
   VARDATA_ANY (skip ONE byte for a short header, else VARHDRSZ), which covers all
   7 call sites at once (they all funnel through this one decode). The query arg
   was already `datum_get_inet_pp` (un-packed).
2. **GiST range/multirange opclass** -> the rangetypes 0->559 /
   multirangetypes 0->122 regressions. Root: `gist-proc` `range_key_from_entry`
   (lib.rs:1510) + `multirange_from_datum` (1522) call `materialize_varlena`
   (1493) which copies the stored key VERBATIM (header + all, no detoast), then
   `range_deserialize` (`backend-utils-adt-rangetypes/src/range_repr_serialize.rs:561`)
   reads `varsize` as `varsize_4b` (~132-133) and the bound offset as fixed
   `size_of::<RangeType>()` (8). A ~17B int4range is short-packable. Call sites:
   1559/1561/1566/1568/1680 (consistent), 1807 (multirange compress leaf),
   1924/2056 (penalty/same). FIXED (`d0e58c667`, lane-varlena-index):
   `materialize_varlena` now un-packs short->4B in place (SET_VARSIZE + copy the
   short payload past the new 4-byte header) while keeping its MAXALIGN(8)
   guarantee, mirroring C's `DatumGetRangeTypeP`/`DatumGetMultirangeTypeP` =
   `PG_DETOAST_DATUM`. Covers all range/multirange sites (all route through this
   one materializer). (This was ALSO the non-arg range path the heap-side
   `datum_get_range_type_p`/`range_p_from_varlena_bytes` hardening did not cover.)
3. **BRIN minmax-multi inet distance** — `parse_inet` — FIXED earlier lane
   (`0c62c5c6e`/`84b55b6d5`, `vardata_any_off`).
4. **BRIN minmax-multi summary codec** (latent; not surfaced by the brin sweep
   but verified by audit): `brin-minmax-multi/src/codec.rs:101` (serialize
   data-length), `:146` (serialize copy), `:205` (deserialize unpack) size a
   per-boundary stored value with `varsize_4b` (4-byte-only) where C uses
   `VARSIZE_ANY`. FIXED (`b52ace96c`, lane-varlena-index): added a `varsize_any`
   helper (VARSIZE_1B / VARSIZE_EXTERNAL / VARSIZE_4B dispatch) and routed the
   three `typlen == -1` boundary sites through it; the outer summary blob stays
   `varsize_4b` (always SET_VARSIZE'd).
5. **tsvector/tsquery GIN opclass** (`tsvector_ops`) -> the tsearch 0->446
   regression. Root: `backend-access-gin-core-probe/src/dispatch.rs` passed the
   raw `value`/`query` image to `gin_extract_tsvector` (237) /
   `gin_extract_tsquery` (367) / `gin_tsquery_consistent` (584) /
   `gin_tsquery_triconsistent` (689). Those bodies read `size` at the fixed
   offset 4 and walk WordEntry/QueryItem arrays at DATAHDRSIZE/HDRSIZETQ-relative
   offsets, requiring a 4-byte header; `ginExtractEntries` does not detoast (C
   detoasts inside the extractValueFn). A short-packed stored tsvector / query
   tsquery lands every read 3 bytes off. FIXED (`0265d8140`, lane-varlena-index):
   added `unpack_short_to_4b` (mirroring C's `PG_DETOAST_DATUM` short->4B) and
   routed the four header-ful-image consumers through it. The gtsvector GiST key
   path was already safe (`signtsvector_from_key` detoasts at lib.rs:1412 BEFORE
   the [4..] strip). The jsonb/text GIN sites already use `vardata_any` (payload,
   not header-ful image) and are unchanged.
VERIFIED CLEAN by the same audit: all GIN (the `vardata_any` dispatch +
type-compare-proc deferral), all nbtree (`bt_compare` defers to the type compare
proc), all SP-GiST (`longValuesOK=false`, never toasted, never routes through
`index_form_tuple`), GiST fixed-length geo (point/box/circle, typlen>0), GiST
gtsvector (`signtsvector_from_key` detoasts at lib.rs:1412 BEFORE the [4..]
strip), BRIN minmax/inclusion/bloom cores (forward whole value to fmgr procs),
BRIN minmax-multi `fc_numeric` (forwards to numeric's short-aware accessors).
NOT covered by the index-AM-scoped audit (caught only by the regress sweep):
the bpchar(CHAR(n))->text CAST (strings), the jsonb container index/recheck path
(jsonb), the jsonpath <4B reader (jsonpath), and the catalog oidvector/char[]
`ARR_NDIM` readers (create_index: `pg_constraint.conexclop`,
`pg_statistic_ext.stxkind`). Those are additional distinct break clusters beyond
the index AMs. (The tsvector GiST/GIN index opclass — tsearch 0->446 — is now
FIXED in target #5 above: the GIN extractValue/extractQuery/consistent path was
the un-hardened reader; the GiST gtsvector key already detoasted.)

JSONB + JSONPATH stored-image readers — HARDENED (`eb07dd020`, lane-varlena-jsonb;
behavior-preserving while OFF, verified by a LOCAL flip then reverted). The
capstone's "jsonb container index path" + "jsonpath <4B reader" clusters:
- jsonb-util `JsonbToJsonbValue` (the central `&jb->root` + `VARSIZE - VARHDRSZ`
  extractor reached by `JsonbInitBinary` in jsonpath_exec and by jsonbsubs via
  `pg_detoast_datum_packed`, which KEEPS a short header short) now uses a
  short-aware data offset for both the container slice AND the binary len.
- jsonpath_exec `jsonb_root` / the local `jsonpath_header` (`[4..8]`) + the
  PASSING-variable `JsonItemFromDatum` text/json `VARDATA_ANY` arms; the
  `arg_jsonpath_image` outer-frame strip already used short-aware `vardata_any`.
- jsonpath `normalize_jsonpath_for_out` reads the version word at the actual data
  offset (recognises a short single-header image; re-probes the inner image of a
  double-wrapped JSON_TABLE column-path Const header-agnostically).
- execSRF `jsonb_srf` `arg_jsonpath_image` short-aware strip (parity).
- misc `arg_text_bytes` (`pg_input_is_valid` / `pg_input_error_info` text args):
  a fixed 4-byte strip OVER-READ a value shorter than 4 bytes (e.g. an unnested
  `'1a'` array element) -> `range start index 4 out of range for slice of length
  3` in jsonpath.sql. THIS was the jsonpath 0->12 regression (a text reader, not
  a jsonpath one). FIXED -> jsonpath 12->0 under the flip.
RESULT under a LOCAL flip: jsonb 608->48 (the container/version readers below) +
the misc text arg (jsonpath 12->0); the residual 48 was the jsonb_ops GIN compare
proc (resolved below) -> jsonb 48->0. With both, json / jsonb_jsonpath / jsonpath
/ jsonb are ALL 0-diff under the flip, and the OFF baseline stays 0-diff on all
four. The cores' fmgr-arg `vardata_any` / `arg_jsonb_root` and the GIN dispatch
`vardata_any` (batch 2*) were already short-aware; the gap was the deeper
container/version extractors above, the misc text arg, and the jsonb_ops GIN
compare proc.

jsonb_ops GIN compare proc — RESOLVED (`0539d3ad1`, lane-varlena-jsonb). The
residual 48-diff was `gin_compare_jsonb` (OID 3480, the jsonb_ops
`GIN_COMPARE_PROC`) comparing the two GIN text keys as RAW varlena images
(`a.cmp(b)` over `as_varlena()`, header included) instead of their `VARDATA_ANY`
payloads (C routes through `bttextcmp`). With the flag on, the stored entry key
(`make_text_key` -> small text, short-packed by `index_form_tuple` as the GIN
entry tuple) is short-headed while the fresh query key is 4-byte, so the raw
compare compared differing headers -> the entry-tree binary search never matched
-> `USING gin (j)` returned 0 rows (BOTH short and long jsonb; `jsonb_path_ops`
by-value uint32 keys + seqscan were unaffected). Fix: strip the header
header-form-agnostically in the `arg_text` fmgr boundary (mirroring
`fc_bttextcmp`'s `arg_bytes`/`vardata_any_slice`). The GIN-AM entry-key machinery
(`gintuple_get_key`/`nocache_index_getattr`/`fetchatt`, `ginCompareEntries`,
`GinFormTuple` posting offset) was already short-aware; the gap was only the
opclass compare proc. RESULT: under a LOCAL flip jsonb / json / jsonb_jsonpath /
jsonpath are ALL 0-diff (the entire jsonb+jsonpath reader class is now
header-agnostic); OFF baseline stays 0-diff.

REMAINING BLOCKER (numeric — the new principal blocker for the flip):
- backend-utils-adt-numeric/src/convert.rs `numeric_data_from_bytes` reads the
  numeric struct header at fixed `num[VARHDRSZ]`/`[VARHDRSZ+1]` and the long-form
  weight at `[VARHDRSZ+2..]`, AND validates `num[0..4]` as a 4-byte varlena
  length word (`varsize_header(num.len())`). A short-headed `numeric` fails the
  length check / mis-reads the struct header by 3 bytes -> garbage -> Inf/NaN.
  NOTE: numeric's OWN short/long header (`numeric_header_is_short`) is
  orthogonal to the varlena short header — both must be handled. Sibling helpers
  `numeric_is_special`/`numeric_digits`/`numeric_ndigits` also index from
  VARHDRSZ. Hardening numeric is NOT a one-line `vardata_any` swap: the
  deserializer + its length-validation invariant are structurally coupled to a
  4-byte varlena header (closest analog to the jsonpath-cores rework). Until
  numeric (and any other type whose deserializer reads a struct field at a fixed
  post-VARHDRSZ offset — re-audit timestamp/range/etc. the same way) is
  header-agnostic, the flip stays OFF.

## Phase-3 — fixed-post-VARHDRSZ struct deserializers (the OTHER flip-blocker class)

A second break class (distinct from the value-payload `vardata_any` readers of
Phase 2): types whose deserializer reads a TYPED struct field at a FIXED offset
after the varlena header (e.g. `npts`/`size`/`bit_len`/`rangetypid` at `[4..8]`,
items at `DATAHDRSIZE`/`HDRSIZETQ`/`sizeof(RangeType)`), and/or interprets
`[0..4]` as a 4-byte length word. A short (1-byte) header shifts every field by
`VARHDRSZ - VARHDRSZ_SHORT == 3` bytes -> garbage. C tolerates this because the
`DatumGet*P` arg macros are `PG_DETOAST_DATUM`, which un-packs short->4B; pgrust's
open-coded decoders did not. Fix = un-pack the short header at the struct decode
entry (mirroring `detoast_attr`'s short arm: `SET_VARSIZE(new, data+VARHDRSZ);
copy VARDATA_SHORT`). All behavior-preserving with the flag OFF (the un-pack
branch is dead — no stored value is short).

HARDENED (this lane, flip-safe, flag OFF):
- **range** (rangetypes/range_repr_serialize.rs): `datum_get_range_type_p` got a
  short arm (route short through `detoast_attr`); `range_p_from_varlena_bytes`
  (the HOT by-ref arg path via `getarg_range_p` — NOT `datum_get_range_type_p`)
  un-packs short->4B before the `palloc0+copy`. VERIFIED under a LOCAL flip:
  int4range/int8range/tsrange stored+read+lower/upper/`@>`/empty/inf all correct.
- **composite/record**: `DatumGetHeapTupleHeader` (heaptuple/lib.rs) +
  `FormedTuple::from_datum_image` (types-tuple) un-pack short before the fixed
  HeapTupleHeader decode (datum_len_/typmod/typeid at 0/4/8, t_hoff at 22).
  These crates sit below detoast, so the short->4B rewrite is open-coded inline.
  VERIFIED under flip: `ROW(7,'hi')` stored+read+field-extract correct.
- **geo path/polygon** (geo-ops/lib.rs `from_datum_image`, shared
  `unpack_short_geo`). VERIFIED under flip: path/polygon stored, npoints + @-@.
- **tsvector/tsquery** (tsvector-core/tsquery-core/ts-small arg readers): un-pack
  short in `arg_tsvector`/`arg_tsquery` (struct size at [4..8], items at 8). The
  fcinfo-tied `&[u8]` borrow is kept; the (never-while-OFF) short path leaks one
  small `'static` un-packed buffer. VERIFIED under flip: `cat:1 dog:2` @@ query.
- **bit/varbit** (varbit/fmgr_builtins `arg_varbit_bytes`): un-pack short before
  `decode_varbit` (bit_len at [4..8], data at VARBIT_PREFIX 8). Same leak note.
  VERIFIED under flip: `bit(11)` + `bit varying` stored, length, bitwise-and.
- **pg_snapshot/txid_snapshot** (xid8funcs `from_varlena_bytes`): un-pack short
  before the nxip[4..8]+xmin/xmax/xip decode. VERIFIED under flip: stored
  `100:200:`, xmin/xmax.

VERIFIED ALREADY-SAFE (no change needed):
- **multirange**: `datum_get_multirange_type_p` sizes via `varsize_any` + un-packs
  via `pg_detoast_datum` (-> detoast_attr short arm). VERIFIED under flip.
- **arrays**: every entry (`DatumGetArrayTypeP`/`deconstruct_array`/arraysubs/
  fmgr `arg_array_detoast`) routes through `detoast_attr` (un-packs short) before
  reading the ArrayType header; `element_slice` takes an already-flat buffer and
  its element reader handles 1B/4B. VERIFIED under flip.
- **jsonb**: the canonical arg path detoasts (`seam_jsonb_datum_bytes` ->
  detoast_attr); `arg_jsonb_root`/`vardata_any` handle short; the remaining
  `[VARHDRSZ..]` strips (lib.rs 1470/1699/1862/1897) are on freshly-built
  (always-4B) `JsonbValueToJsonb`/parse results.
- **inet/cidr**: `arg_inet` strips a short-aware header then `from_datum_bytes`
  on the body (offsets relative to the stripped payload).
- **tsgistidx** (gtsvector): strips a short-aware header before parsing the body;
  `gtsvector_compress` works over pre-deconstructed lexemes.
- **uuid (typlen 16) / macaddr (6) / macaddr8 (8) / circle / point / box / lseg /
  line / aclitem**: fixed-length (`typlen > 0`) — NOT varlena, never short-packed.
- **timestamp/timestamptz/interval/date/time**: pass-by-value (8 bytes) — not
  varlena. Safe (skip per the task's premise; confirmed by tsrange working).

REMAINING Phase-3 blocker (excluding numeric, owned by lane-numeric-hdr):
- **numeric** — see the dedicated section below. The ONLY remaining type-struct
  blocker found. Confirmed under a LOCAL flip: a `numrange`/`nummultirange`
  numeric BOUND (numeric short-packed inside the range) hits
  `assertion failed: num.len() >= VARHDRSZ + 2` in numeric_data_from_bytes — i.e.
  the range struct decode is correct and the residual is purely numeric's own
  deserializer. Once numeric lands, numrange/nummultirange round-trip too.

How close is the flip? With the above + numeric (lane-numeric-hdr), the
fixed-post-VARHDRSZ struct-deserializer class is closed. The Phase-2 value-reader
sweep residuals (jsonpath `arg_jsonpath_image`, any unvisited `arg_*`/`[VARHDRSZ..]`)
still need a final triage, and the full regress suite (esp. index-opclass / GIN /
GiST / BRIN key extractors over the shared index-tuple form) must be re-run green
under a candidate flip before committing it.

### SAFE (no change)

- All `set_varsize_4b` / `buf[VARHDRSZ..]` WRITES into freshly-built varlenas
  (arrayfuncs construct, jsonb/json build, brin codec, catalog-indexing family*,
  pg_publication, partition) — fresh values are always 4B.
- Readers that already dispatch on the header bit before slicing: commands-policy
  `varlena_body`, statistics-core / extended-stats / mvdistinct `varlena_body`,
  the varlena/lib.rs `text_payload_*` family, execTuples slot deform (varsize_any).
- detoast / toast-internals / toast-compression: operate on freshly
  decompressed (guaranteed 4B) images.
- execSRF `[4..]`/`VARHDRSZ` reads: these are genuinely SAFE only where the SRF
  detoasts first or the value is freshly built — re-verify each before the flip.
- Non-varlena `[4..]` (gindatapage ItemPointer, brin-xlog record parse): not
  varlena at all.

## Phase-3 risk assessment (why the flag is NOT flipped in this lane)

1. **Surface size**: 30-50 files, helpers duplicated, no choke point. Mechanical
   but each needs per-site review (does the core also compute length from a fixed
   header? does it forward to a sub-core that re-strips? is a compressed/external
   arg reachable, forcing a detoast + signature change?).
2. **Failure mode is silent data corruption** (front-truncated short strings),
   the hardest class to fully gate — a single missed reader returns wrong bytes
   with no crash. A correctness gate over the major types is necessary but not
   sufficient to prove ALL readers safe; the full regress suite must be green.
3. **Index storage also flips** (shared form path) — index opclass readers and
   the GIN/GiST/BRIN key extractors are in the break set.

Recommendation: complete the Phase-2 sweep crate-by-crate (each landing is a
behavior-preserving no-op while the flag is OFF), re-run the per-type correctness
gate + full regress suite after each batch, and only flip
`SHORT_VARLENA_PACKING` once the entire BREAK list above is header-agnostic and
the gate + suite are clean. The copy2 payoff is also smaller than assumed: the
current copy2 126-diff is dominated by trigger-firing ORDER (~48 lines of
"before/after trigger fired"), a separate bug, not physical varlena row size.

## LANDED (lane-varlena-catalog): catalog oidvector/char[] readers + bpchar→text cast

Two of the regress-sweep break clusters above are now header-agnostic (flag stays
OFF; behavior-preserving — every change is a no-op while every stored image is
4-byte). Verified by a LOCAL FLIP (uncommitted `SHORT_VARLENA_PACKING = true`,
fresh C-initdb cluster, best-of .out/_1/_2 diff): create_index **31→7**, strings
**20→0**, char 0, varchar 0 — and IDENTICAL with the flag OFF (create_index 7,
strings/char/varchar 0). The residual create_index 7 is the rangetypes
`type with OID 256 does not exist` range-deserialize keystone (blocker #2, the
GiST range / `range_deserialize` lane), NOT a catalog-array or bpchar miss.

1. **bpchar(CHAR(n))→text CAST** (strings 20→0). Root: the oracle_compat fmgr
   arg reader `arg_bytes` (`backend-utils-adt-oracle-compat/src/fmgr_builtins.rs`)
   stripped a FIXED `VARHDRSZ` (4) off the stored bpchar image. `text(bpchar)`
   (OID 401, prosrc `rtrim1`) front-truncated `a`/`ab`/`abcd`. FIX: routed
   `arg_bytes` through a `VARDATA_ANY` helper (skip 1 byte for a short
   low-bit-set non-external header, else 4). This also hardens every other
   oracle_compat text/bytea reader (lower/upper/initcap/casefold/lpad/rpad/
   ltrim/rtrim/btrim/translate/...).
2. **catalog oidvector / int2vector / char[] ARR_NDIM readers** (create_index
   31→7). Roots — open-coded `ArrayType`/`int2vector` header reads at a FIXED
   4-byte offset on a raw stored image (`as_ref_bytes()`, no detoast; the C path
   detoasts via `DatumGetArrayTypeP`). All routed through an `arr_content_off`
   helper (struct content starts 1 byte in for a short header, else 4):
   - `backend-access-index-genam/src/decode.rs`: `oid_array_elems` (conexclop
     oidvector — the `conexclop is not a 1-D array` blocker), `int16_array_elems`
     (conkey/confkey), `int2vector_elems` (pg_index.indkey / tgattr),
     `extract_not_null_column` (conkey).
   - `backend-statistics-extended-stats/src/lib.rs`: `decode_char_array` (stxkind
     — the `stxkind is not a 1-D char array` blocker), `decode_int2vector`
     (stxkeys).
   - `backend-catalog-pg-publication/src/lib.rs` + `backend-commands-
     publicationcmds/src/lib.rs`: `int2vector_elems` (pg_publication_rel.prattrs).

VERIFIED-SAFE (already detoast then read at fixed 4B, no change): the syscache
projection family (`getattr_oid_array`/`getattr_char_array`/`read_conkey_array`/
`read_oid_array`/`detoast_array_header`, `int2vector_to_i16s_bytes`,
`text_array_to_strings_bytes`, `deconstruct_array_values_bytes` → the aclitem[]
owner-change path family2.rs); all the catalog-indexing `buildint2vector`/
`buildoidvector`/`cstring_to_text_datum` are WRITE-side builders (always emit a
4-byte header — correct, the storage layer short-packs on the way to disk).

REMAINING adjacent (NOT in this lane's scope): `backend-catalog-indexing/src/
family2.rs` `decode_text_array` reads `pg_db_role_setting.setconfig` (a text[])
via `foundation::arr_ndim`/`arr_dims`/`arr_data_ptr_off` on the RAW image (no
detoast) — another fixed-4B array-header read that would flip-break, but it is a
generic `text[]` GUC array, distinct from the oidvector/char[]/bpchar classes.
