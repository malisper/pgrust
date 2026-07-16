# cbstore -> pgrcolumnar rename map (2026-07-15, rides t24)

Branch `rename-pgrcolumnar`, base t23 main @ a1fbbbd76fd5. Pure code-identity
rename; behavior-identical by construction (see "Frozen surface" — every
runtime string literal is byte-identical to base; verified mechanically per
file, zero literal drift).

## Mechanical rule

Applied to Rust identifiers, module paths, comments, and living docs — never
to string literals:

- `cbstore` -> `pgrcolumnar`
- `Cbstore` -> `Pgrcolumnar`
- `CBSTORE` -> `PGRCOLUMNAR`

with protections (NOT renamed anywhere): `cbstore` followed by a digit
(format-lineage: `cbstore8-v6`, `cbstore9-v8`, `cbstore9-v8x`, ...),
`PGRUST_CBSTORE_*`, `pgrust.cbstore*`, `pgrust-cbstore*` (bank keys /
scratch dirs), and the e2e script basenames (`cbstore-lane-e2e`,
`cbstore-lane-bench`, `cbstore-window-sweep`, `cbstore-abortsafe-e2e`,
`cbstore-writer2-e2e`).

## Crate

| old | new |
|---|---|
| `crates/backend/access/cbstore/` | `crates/backend/access/pgrcolumnar/` |
| package `cbstore` (lib target `cbstore`) | package `pgrcolumnar` (lib target `pgrcolumnar`) |
| dep entries in `tableam`, `tablecmds`, `copy` Cargo.tomls | `pgrcolumnar = { path = ... }` |
| `use cbstore::...` / `cbstore::` paths (all dependents) | `use pgrcolumnar::...` / `pgrcolumnar::` |

## Renamed public symbols (old = new with the reverse substitution)

- `AMFLAG_CBSTORE` -> `AMFLAG_PGRCOLUMNAR` (types/pathnodes)
- `AMFLAG_CBSTORE_ZEROCNT` -> `AMFLAG_PGRCOLUMNAR_ZEROCNT` (types/pathnodes)
- `DEFAULT_CBSTORE_GATHER_SORT_TUPLE_COST` -> `DEFAULT_PGRCOLUMNAR_GATHER_SORT_TUPLE_COST` (costsize/gucs)
- `DEFAULT_CBSTORE_GROUP_NDISTINCT_RATIO` -> `DEFAULT_PGRCOLUMNAR_GROUP_NDISTINCT_RATIO` (costsize/gucs)
- `DEFAULT_CBSTORE_PARALLEL_SETUP_COST` -> `DEFAULT_PGRCOLUMNAR_PARALLEL_SETUP_COST` (costsize/gucs)
- `DEFAULT_CBSTORE_PARALLEL_TUPLE_COST` -> `DEFAULT_PGRCOLUMNAR_PARALLEL_TUPLE_COST` (costsize/gucs)
- `CBSTORE_CLUSTER_KEY_MAX` -> `PGRCOLUMNAR_CLUSTER_KEY_MAX` (types/rel reloptions)
- `CBSTORE_CODEC_COLS_MAX` -> `PGRCOLUMNAR_CODEC_COLS_MAX` (types/rel reloptions)
- `CBSTORE_RG_ROWS` -> `PGRCOLUMNAR_RG_ROWS` (allpaths)
- `CBSTORE_RGS_PER_WORKER` -> `PGRCOLUMNAR_RGS_PER_WORKER` (allpaths)
- `CbstoreCodec` -> `PgrcolumnarCodec` (types/rel reloptions)
- `CbstoreOptions` -> `PgrcolumnarOptions` (types/rel reloptions)
- `TableAm::Cbstore` -> `TableAm::Pgrcolumnar` (tableam_vocab)
- `CbstoreGranuleSource` -> `PgrcolumnarGranuleSource` (lanev2 runtime_scan, pub(super))
- `CbstoreSource` -> `PgrcolumnarSource` (crate-internal, pgrcolumnar/src)
- `compute_cbstore_parallel_worker` -> `compute_pgrcolumnar_parallel_worker` (allpaths)
- `is_cbstore_am_oid` -> `is_pgrcolumnar_am_oid` (tableam_vocab)
- `register_cbstore_table_am` -> `register_pgrcolumnar_table_am` (tableam_vocab)
- `relam_is_cbstore` -> `relam_is_pgrcolumnar` (access/common/reloptions)
- `seq_scan_is_cbstore` -> `seq_scan_is_pgrcolumnar` (nodeseqscan)
- `cbstore()` -> `pgrcolumnar()` (types/rel reloptions accessor)
- `cbstore_analyze_fetch_row` / `_analyze_visible_rgs` / `_footer_col_bytes` / `_footer_ndv` / `_footer_sorted` / `_footer_zerocnt_all` -> `pgrcolumnar_*` (tableam)
- `cbstore_colfrac_cost` / `_feeds_plan` / `_footer_ndv_est` / `_gather_sort_tuple_cost` / `_group_ndistinct_ratio` / `_parallel_setup_cost` / `_parallel_tuple_cost` / `_scan_pathkeys` -> `pgrcolumnar_*` (costsize)
- `cbstore_ingest_sort` -> `pgrcolumnar_ingest_sort` (tuplesort_seams)
- `cbstore_reloptions` -> `pgrcolumnar_reloptions` (access/common/reloptions)
- plus all crate-internal (non-pub-workspace) identifiers, fields, locals, and
  test/mod names by the same rule (93 files, mechanical).

## Docs

| old | new |
|---|---|
| `docs/design/cbstore-impl.md` | `docs/design/pgrcolumnar-impl.md` (stub left at old path) |
| `docs/design/cbstore-v2-beat-clickhouse-plan.md` | `docs/design/pgrcolumnar-v2-beat-clickhouse-plan.md` (stub) |
| `docs/design/cbstore-zone-adaptive.md` | `docs/design/pgrcolumnar-zone-adaptive.md` (stub) |
| `docs/design/cbstore-part-cache.md` | `docs/design/pgrcolumnar-part-cache.md` (stub) |

Prose renamed in living docs (design docs, `docs/optimizations/cb-slow-queries.md`,
`docs/conformance/tie-ordering.md`). Historical records untouched: `notes/`,
`docs/postmortems/`, `docs/wedge-bisect-ledger.md`, `docs/design/m5-ratifications/`,
`crates/backend/executor/execmain/src/lanev2/m5-coverage.tsv`, `docs/design/regex-engine-ab-verdict.md`.

## Kept as-is (frozen surface — deliberate)

1. **SQL access-method name `cbstore`** (`CREATE ACCESS METHOD cbstore`,
   `CREATE TABLE ... USING cbstore`, the `pg_am.amname` probe literals in
   relcache/build.rs, tableam_vocab, reloptions, tablecmds). Banked datadirs,
   dumps, every e2e script, and regress byte-identity depend on it. Renaming
   the SQL name is a separate, deliberate compat project (would need an alias
   AM name), not part of this pass.
2. **Bank keys / format lineage**: `cbstore8-v6`, `cbstore9-v8`, `cbstore9-v8x`,
   `clickbench-pgrust-cbstore*` S3 keys, all CB_FMT values. Frozen-bank law.
3. **Env vars**: `PGRUST_CBSTORE_*` (19 names: READAHEAD{,_SERIAL,_CLAIMS},
   INTCODEC{,_COLS,_READ}, DICT_FRAMES, DICT_FRAME_KB, DICT_LAZY, FASTHASH,
   STITCH, FOOTER_{DEBUG,EAGER,NDV_EST}, PART_CACHE, SHARED_{MAP,PART},
   SCAN_PATHKEYS, COLFRAC_COST) plus unprefixed `CBSTORE_DISABLE_BLOCK_ZM`,
   `CBSTORE_DISABLE_BLOOM`, `CBSTORE_LZ4_FUZZ_ITERS`. The fleet harness and
   every runbook uses them; aliasing 22 names at scattered call sites is not
   the small safe diff — recorded as a follow-up in the knobs inventory.
4. **GUCs**: no GUC name contains cbstore (checked); GUC short_desc strings
   mentioning "cbstore" kept byte-identical (pg_settings output surface).
5. **All runtime string literals**: error/panic messages ("cbstore does not
   support NULL values", "cbstore: corrupt footer", ...), trace/marker/refusal
   strings ("cbstore prewhere armed", "scan not fusible/cbstore", CBSCAN-class
   markers) — regress expected output and in-repo script greps
   (scripts/cbstore-lane-e2e.sh etc.) match on them.
6. **scripts/** — zero changes. Scripts contain only frozen surface (SQL AM
   name, markers, bank keys, their own filenames); no script references the
   crate path or cargo package name. Script basenames (`cbstore-lane-e2e.sh`,
   ...) are fleet-harness gate identity and stay.
7. **`Cb*` short type prefixes** (`CbScanDescData`, `CBSCAN`/`CBF`/`CBE`
   marker identifiers, `cb_*` file names like `examples/cb_intcodec_survey.rs`):
   kept. They are not the literal token "cbstore", the marker *strings* they
   emit are frozen grep surface, and renaming identifiers away from the marker
   strings they produce would be a coherence loss. Follow-up candidate only.

## Follow-ups (recorded, not done here)

- Env-var aliases `PGRUST_PGRCOLUMNAR_*` -> same backing (knobs inventory).
- Optional SQL-surface alias AM name `pgrcolumnar` (compat project; touches
  banks/harness).
- `Cb*`/`CBSCAN` identifier prefixes if Michael wants them too.
