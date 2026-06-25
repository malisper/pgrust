# Audit — backend-statistics-mcv (mcv.c)

PARTIAL port of `src/backend/statistics/mcv.c` (PostgreSQL 18.3), the
most-common-value slice of the combined `backend-statistics-core` unit and the
sibling of the already-landed `backend-statistics-dependencies`. Same model:
the byte-layout (de)serialize + the pure arithmetic + the selectivity-summation
drivers are ported 100% in-crate; the build side and every node/fmgr/syscache/
Datum-value boundary cross seams owned by the unported owner
(`backend-statistics-core-seams`, panics until `extended_stats.c` + the
multi-sort support + vacuum's `VacAttrStats` land), mirroring how `dependency_degree`
was seamed in the dependencies sibling.

## Constant / macro verification (vs `statistics/statistics.h`, `mcv.c`)

| C macro / const | C value | port |
| --- | --- | --- |
| `STATS_MCV_MAGIC` | `0xE1A651C2` | `types_statistics::STATS_MCV_MAGIC` = `0xE1A6_51C2` ✓ |
| `STATS_MCV_TYPE_BASIC` | `1` | `= 1` ✓ |
| `STATS_MAX_DIMENSIONS` | `8` | `= 8` ✓ (pre-existing) |
| `MAX_STATISTICS_TARGET` | `10000` | `= 10000` ✓ |
| `STATS_MCVLIST_MAX_ITEMS` | `MAX_STATISTICS_TARGET` | `= 10000` ✓ |
| `ITEM_SIZE(ndims)` (mcv.c:53) | `ndims*(2+1)+2*8` | `item_size` ✓ |
| `MinSizeOfMCVList` (mcv.c:59) | `VARHDRSZ+4*3+2` = 18 | `min_size_of_mcvlist` = 18 ✓ |
| `SizeOfMCVList(ndims,nitems)` (mcv.c:68) | `(Min+4*ndims)+20*ndims+nitems*ITEM_SIZE` | `size_of_mcvlist` ✓ |
| `sizeof(DimensionInfo)` | 4 ints (16) + bool, pad to 20 | `SIZEOF_DIMENSION_INFO` = 20 ✓ |
| `RESULT_MERGE` / `RESULT_IS_FINAL` (mcv.c:88/100) | — | `result_merge` / `result_is_final` ✓ (branch-for-branch) |
| `CLAMP_PROBABILITY` | `<0->0; >1->1` | `clamp_probability` ✓ (branch order, NaN-faithful, not `f64::clamp`) |

All verified by the `constants_match_c` / `result_macros` tests.

## Per-function parity

| C function (mcv.c) | disposition | notes |
| --- | --- | --- |
| `get_mincount_for_mcv_list` (147) | IN-CRATE | f64 arithmetic + the `denom == 0.0` div-by-zero guard, line-for-line. Tested. |
| `statext_mcv_build` (179) | SEAMED | `core_seam::statext_mcv_build`. Needs `build_mss`/`build_sorted_items`/`build_distinct_groups`/`build_column_frequencies` over the opaque `StatsBuildData` (VacAttrStats matrix + Datum/bool value matrices) + multi-sort support. Mirrors dependencies' `dependency_degree`. |
| `build_mss` (346) | SEAMED (part of build) | folded into the seamed build (typcache LT_OPR + `multi_sort_add_dimension`). |
| `count_distinct_groups` (378) | SEAMED (part of build) | internal helper of the seamed build. |
| `compare_sort_item_count` (402) | SEAMED (part of build) | internal helper of the seamed build. |
| `build_distinct_groups` (423) | SEAMED (part of build) | uses `multi_sort_compare` + `qsort_interruptible`; internal to seamed build. |
| `sort_item_compare` (464) | SEAMED (part of build) | `ApplySortComparator`; internal to seamed build. |
| `build_column_frequencies` (489) | SEAMED (part of build) | internal to seamed build. |
| `statext_mcv_load` (557) | SPLIT | syscache read = `core_seam::mcv_load_bytea` (STATEXTDATASTXOID + Anum_pg_statistic_ext_data_stxdmcv, the missing-object / un-built `elog`s carried on Err); the `statext_mcv_deserialize(DatumGetByteaP(...))` half is in-crate. |
| `statext_mcv_serialize` (620) | IN-CRATE byte layout | full per-dimension dedup + DimensionInfo population + the exact `total_length` formula + the header/dimension-info/values/items emission, branch-for-branch. Crosses 3 Datum-value seams only: `mcv_lookup_lt_opr` (typcache LT_OPR), `mcv_compare_scalars_simple` (sort+bsearch comparator), `mcv_value_to_serialized_bytes` (the 4 per-category `store_att_byval`/by-ref/varlena/cstring payload bodies — project-wide-deferred Datum codec). The uint16 index `bsearch_arg` is in-crate (`bsearch_index`). All Asserts mirrored as `debug_assert!`. |
| `statext_mcv_deserialize` (995) | IN-CRATE byte layout | full size checks (MinSizeOfMCVList, SizeOfMCVList, final exact-size), header sanity (magic/type/zero-and-overlong dimension + item arrays), the by-val/by-ref/varlena/cstring map-building (via `mcv_serialized_bytes_to_value`) and the item index translation, branch-for-branch. Adds an out-of-range index guard (C trusts the catalog; here it is an Err not UB) + the negative-DimensionInfo guard. |
| `pg_stats_ext_mcvlist_items` (1337) | SEAMED | `core_seam::pg_stats_ext_mcvlist_items` — pure SRF / fmgr / tupdesc / array-builder / type-output dispatch over the deferred Datum fmgr surface. |
| `pg_mcv_list_in` (1471) | IN-CRATE | `Err(ERRCODE_FEATURE_NOT_SUPPORTED, "cannot accept a value of type pg_mcv_list")`. Tested. |
| `pg_mcv_list_out` (1497) | SEAMED | `return byteaout(fcinfo)` -> `core_seam::pg_mcv_list_out`. |
| `pg_mcv_list_recv` (1506) | IN-CRATE | same FEATURE_NOT_SUPPORTED error. Tested. |
| `pg_mcv_list_send` (1522) | IN-CRATE | `return byteasend(fcinfo)` -> ported `backend_utils_adt_varlena::bytea::byteasend`. Tested. |
| `mcv_match_expression` (1534) | SEAMED (part of bitmap) | `IsA(Var)`/`equal`/`exprCollation`/`bms_member_index`; internal to the seamed `mcv_get_match_bitmap`. |
| `mcv_get_match_bitmap` (1598) | SEAMED | `core_seam::mcv_get_match_bitmap` — walks planner `Node` clauses (OpExpr/ScalarArrayOpExpr/NullTest/AND/OR/NOT/boolean Var/bare bool expr) over the planner arena + FunctionCall2Coll/DatumGetBool. `clauses`/`keys`/`exprs` are opaque planner-arena ids. |
| `mcv_combine_selectivities` (2005) | IN-CRATE | the `other_sel` clamp, the `1 - mcv_totalsel` cap, the `mcv_sel + other_sel` clamp, line-for-line. Tested (incl. cap + both clamps). |
| `mcv_clauselist_selectivity` (2047) | IN-CRATE driver | the `rte->inh` read (`mcv_rte_inh_for_rel` seam) + the MCV load + the match bitmap (seam) + the in-crate frequency/base/total summation over matching items. NULL-load surfaced as Err (C would deref-crash). `varRelid`/`jointype`/`sjinfo` unused as in C. |
| `mcv_clause_selectivity_or` (2125) | IN-CRATE driver | the `or_matches` lazy-alloc, the `list_make1(clause)` match bitmap (seam), and the in-crate frequency/base/overlap/total summation + the running OR-bitmap update, branch-for-branch. |

## Carriers added (types-statistics, mirror MVDependencies precedent)

`MCVList` (FAM `items` -> `Vec<MCVItem>`; `types[STATS_MAX_DIMENSIONS]`),
`MCVItem` (`Datum*`/`bool*` -> owned `Vec`), `DimensionInfo` (raw 20-byte
serialized struct), `SortItem`; constants `STATS_MCV_MAGIC`,
`STATS_MCV_TYPE_BASIC`, `MAX_STATISTICS_TARGET`, `STATS_MCVLIST_MAX_ITEMS`.

## Seams installed / consumed

This crate installs NO inward seams (its public fns are called by the unported
`backend-statistics-core` dispatcher / fmgr catalog). It CONSUMES the 10 MCV
seams declared in `backend-statistics-core-seams` (owner `todo` in CATALOG.tsv,
so the every-declared-seam-is-installed guard exempts them; they are
mirror-pg-and-panic until the owner lands). No CONTRACT_RECONCILE entry.

## Divergences from C (all behaviour-preserving)

* Out-of-range uint16 index in deserialize and a NULL serialize bsearch miss are
  `Err`s, not C UB / Asserts. C trusts the catalog bytes; the owned model cannot.
* Negative `DimensionInfo.nvalues`/`nbytes` -> `Err` (C only `Assert`s).
* `mcv_clauselist_selectivity` NULL load -> `Err` (C dereferences unconditionally).
* C `palloc`-out -> owned `Vec` returns; transient working buffers use
  `try_reserve` (OOM -> `mcx.oom`) per the allocation-safety rule.
* The serialized varlena uses the long (4-byte) header (`SET_VARSIZE_4B`); the
  reader (`varsize_any`/`vardata_any`) handles both short and long tags.

## Gate

* `cargo check -p backend-statistics-mcv` — clean.
* `cargo test -p backend-statistics-mcv` — 14 passed.
* No `todo!`/`unimplemented!`.
