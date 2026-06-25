# Audit: types-statistics VacAttrStats / StatsBuildData carrier (analyze keystone K2)

C source: `src/include/commands/vacuum.h` (PostgreSQL 18.3, `VacAttrStats`
struct ~line 116, `VacAttrStatsP`, `AnalyzeAttrFetchFunc`,
`AnalyzeAttrComputeStatsFunc`) and
`src/include/statistics/extended_stats_internal.h:61` (`StatsBuildData`).

Crate: `crates/types-statistics` (carrier types only — no logic).

This is the **K2 keystone**: build the real `VacAttrStats` working-state carrier
and de-opaque `StatsBuildData` so the extended-stats build-side seams
(`statext_mcv_build` / `dependency_degree` / `ndistinct_for_combination`) carry a
real per-column `VacAttrStats` matrix + value/null matrices instead of an
identity `StatsBuildDataHandle(u64)`. The build sides are NOT filled here — they
stay seam-and-panic over the now-real carrier, to be filled by the future
ANALYZE owner (`analyze.c`). K1 (the table-AM analyze-scan keystone that lets
`analyze.c` acquire sample rows) still gates the ANALYZE owner.

## VacAttrStats field audit (field-for-field vs vacuum.h)

26 C fields; all 26 mirrored. Two model adaptations follow repo convention:
`Datum *`/`float4 *` heap arrays become owned `Vec`; the C `MemoryContext` /
`Form_pg_type` / `TupleDesc` / `HeapTuple *` become the repo's safe equivalents.

| C field | C type | Carrier field | Carrier type | Note |
|---|---|---|---|---|
| `attstattarget` | `int` | `attstattarget` | `i32` | |
| `attrtypid` | `Oid` | `attrtypid` | `Oid` | |
| `attrtypmod` | `int32` | `attrtypmod` | `i32` | |
| `attrtype` | `Form_pg_type` | `attrtype` | `Option<FormData_pg_type>` | NULL before setup |
| `attrcollid` | `Oid` | `attrcollid` | `Oid` | |
| `anl_context` | `MemoryContext` | `anl_context` | `Option<Mcx<'mcx>>` | |
| `compute_stats` | `AnalyzeAttrComputeStatsFunc` | `compute_stats` | `Option<AnalyzeAttrComputeStatsFunc>` | fn-ptr alias |
| `minrows` | `int` | `minrows` | `i32` | |
| `extra_data` | `void *` | `extra_data` | `u64` | owner-resolved tag |
| `stats_valid` | `bool` | `stats_valid` | `bool` | |
| `stanullfrac` | `float4` | `stanullfrac` | `f32` | |
| `stawidth` | `int32` | `stawidth` | `i32` | |
| `stadistinct` | `float4` | `stadistinct` | `f32` | |
| `stakind[STATISTIC_NUM_SLOTS]` | `int16[5]` | `stakind` | `[i16; 5]` | |
| `staop[STATISTIC_NUM_SLOTS]` | `Oid[5]` | `staop` | `[Oid; 5]` | |
| `stacoll[STATISTIC_NUM_SLOTS]` | `Oid[5]` | `stacoll` | `[Oid; 5]` | |
| `numnumbers[STATISTIC_NUM_SLOTS]` | `int[5]` | `numnumbers` | `[i32; 5]` | = `stanumbers[n].len()` |
| `stanumbers[STATISTIC_NUM_SLOTS]` | `float4 *[5]` | `stanumbers` | `[Vec<f32>; 5]` | safe value lane |
| `numvalues[STATISTIC_NUM_SLOTS]` | `int[5]` | `numvalues` | `[i32; 5]` | = `stavalues[n].len()` |
| `stavalues[STATISTIC_NUM_SLOTS]` | `Datum *[5]` | `stavalues` | `[Vec<Datum>; 5]` | safe value lane |
| `statypid[STATISTIC_NUM_SLOTS]` | `Oid[5]` | `statypid` | `[Oid; 5]` | |
| `statyplen[STATISTIC_NUM_SLOTS]` | `int16[5]` | `statyplen` | `[i16; 5]` | |
| `statypbyval[STATISTIC_NUM_SLOTS]` | `bool[5]` | `statypbyval` | `[bool; 5]` | |
| `statypalign[STATISTIC_NUM_SLOTS]` | `char[5]` | `statypalign` | `[i8; 5]` | |
| `tupattnum` | `int` | `tupattnum` | `i32` | |
| `rows` | `HeapTuple *` | `rows` | `Vec<HeapTuple<'mcx>>` | std-fetch sample rows |
| `tupDesc` | `TupleDesc` | `tup_desc` | `TupleDesc<'mcx>` | |
| `exprvals` | `Datum *` | `exprvals` | `Vec<Datum>` | index-fetch flat buffer |
| `exprnulls` | `bool *` | `exprnulls` | `Vec<bool>` | companion nulls |
| `rowstride` | `int` | `rowstride` | `i32` | |

`STATISTIC_NUM_SLOTS = 5` audited vs `catalog/pg_statistic.h:127`. The `[5]`
arrays are all fixed `[T; STATISTIC_NUM_SLOTS]` exactly as the C struct.

`AnalyzeAttrFetchFunc` / `AnalyzeAttrComputeStatsFunc` modeled as
`for<'mcx> fn` pointer aliases over the owned `VacAttrStats` (the producers /
consumers live in the unported ANALYZE driver). `Datum`-returning fetch func
keeps the repo `Datum`.

## StatsBuildData de-opaque audit (vs extended_stats_internal.h:61)

| C field | C type | Carrier field | Carrier type |
|---|---|---|---|
| `numrows` | `int` | `numrows` | `i32` |
| `nattnums` | `int` | `nattnums` | `i32` |
| `attnums` | `AttrNumber *` | `attnums` | `Vec<AttrNumber>` (len `nattnums`) |
| `stats` | `VacAttrStats **` | `stats` | `Vec<VacAttrStats<'mcx>>` (len `nattnums`) |
| `values` | `Datum **` | `values` | `Vec<Vec<Datum>>` (`nattnums` × `numrows`) |
| `nulls` | `bool **` | `nulls` | `Vec<Vec<bool>>` (`nattnums` × `numrows`) |

The opaque `StatsBuildDataHandle(u64)` is deleted; the three build-side seams now
take `&StatsBuildData<'mcx>`.

## Build-side seams (re-typed, NOT filled)

`backend-statistics-core-seams` (owner = unported `backend-statistics-core`):

* `dependency_degree<'mcx>(&StatsBuildData<'mcx>, k, dependency)` — was
  `(StatsBuildDataHandle, ...)`.
* `statext_mcv_build<'mcx>(&StatsBuildData<'mcx>, totalrows, stattarget)`.
* `ndistinct_for_combination<'mcx>(totalrows, &StatsBuildData<'mcx>, k, combination)`.

All three stay seam-and-panic (uninstalled — owner `todo` in CATALOG, exempt from
the every-seam-installed guard). Their bodies (`multi_sort_*`,
`build_sorted_items`, `lookup_type_cache(...)->lt_opr`) are owner-side.

The three landed consumer crates re-typed their public `*_build` wrappers from
`StatsBuildDataHandle` to `&StatsBuildData<'mcx>` and pass it straight through;
their in-crate serialize/deserialize logic is untouched (no regression).

## Item 3: per-type typanalyze fmgr dispatch

Deferred to the `analyze.c` lane (not trivial as a standalone carrier addition):
`std_typanalyze` needs the fmgr dispatch surface and the full ANALYZE driver,
neither of which has an owner crate yet. Noted in DESIGN_DEBT.

## Gates

* `cargo check --workspace` — exit 0 (pre-existing warnings only).
* `cargo test -p no-todo-guard` — pass; no `todo!`/`unimplemented!` introduced.
* `cargo test -p seams-init` — pass (no new/changed seam installs).
* CONTRACT_RECONCILE_PENDING count unchanged (18 files, matches origin/main).
