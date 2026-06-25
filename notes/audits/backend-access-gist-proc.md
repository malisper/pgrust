# Audit: backend-access-gist-proc

C source: `src/backend/access/gist/gistproc.c` (PostgreSQL 18.3, 1761 lines).
Port: `crates/backend-access-gist-proc/src/lib.rs`.
Dispatch seam crate: `crates/backend-access-gist-dispatch-seams`.
geo predicate seams: `crates/backend-utils-adt-geo-ops-seams` (owner
`backend-utils-adt-geo-ops`, now landed — task #285 unblock trigger).
Vocabulary types (`Point`, `BOX`, `CIRCLE`): `crates/types-core/src/geo.rs`.
`Polygon` (owned, out-of-line `Vec<Point>`): `crates/backend-utils-adt-geo-ops`.

No c2rust run exists for this unit; audited against the C directly.

This re-audit covers the polygon/circle/distance completion (task #285) on top of
the previously-merged box/point port; the box/point functions were unchanged and
are re-verified for regression below.

## OID / strategy constant verification (MANDATORY)

All support-proc OIDs verified against `src/include/catalog/pg_proc.dat`:

| proname | OID | const | verdict |
|---|---|---|---|
| gist_box_consistent | 2578 | F_GIST_BOX_CONSISTENT | MATCH |
| gist_box_penalty | 2581 | F_GIST_BOX_PENALTY | MATCH |
| gist_box_picksplit | 2582 | F_GIST_BOX_PICKSPLIT | MATCH |
| gist_box_union | 2583 | F_GIST_BOX_UNION | MATCH |
| gist_box_same | 2584 | F_GIST_BOX_SAME | MATCH |
| gist_box_distance | 3998 | F_GIST_BOX_DISTANCE | MATCH |
| gist_poly_consistent | 2585 | F_GIST_POLY_CONSISTENT | MATCH (new) |
| gist_poly_compress | 2586 | F_GIST_POLY_COMPRESS | MATCH (new) |
| gist_poly_distance | 3288 | F_GIST_POLY_DISTANCE | MATCH (new) |
| gist_circle_consistent | 2591 | F_GIST_CIRCLE_CONSISTENT | MATCH (new) |
| gist_circle_compress | 2592 | F_GIST_CIRCLE_COMPRESS | MATCH (new) |
| gist_circle_distance | 3280 | F_GIST_CIRCLE_DISTANCE | MATCH (new) |
| gist_point_compress | 1030 | F_GIST_POINT_COMPRESS | MATCH |
| gist_point_fetch | 3282 | F_GIST_POINT_FETCH | MATCH |
| gist_point_consistent | 2179 | F_GIST_POINT_CONSISTENT | MATCH |
| gist_point_distance | 3064 | F_GIST_POINT_DISTANCE | MATCH |
| gist_point_sortsupport | 3435 | F_GIST_POINT_SORTSUPPORT | MATCH |

R-tree strategy numbers verified against `src/include/access/stratnum.h`:
RTLeft=1, RTOverLeft=2, RTOverlap=3, RTOverRight=4, RTRight=5, RTSame=6,
RTContains=7, RTContainedBy=8, RTOverBelow=9, RTBelow=10, RTAbove=11,
RTOverAbove=12, RTOldBelow=29, RTOldAbove=30 — all MATCH.

`gist_point_consistent` group classification constants verified vs gistproc.c:
`GeoStrategyNumberOffset=20`, Point/Box/Polygon/Circle group = 0/1/2/3 — MATCH.

## Function inventory and verdicts

| C function (line) | Port | Verdict | Notes |
|---|---|---|---|
| `rt_box_union` (54) | `rt_box_union` | MATCH | float8_max/min into high/low. |
| `size_box` (67) | `size_box` | MATCH | zero-width → 0; NaN.high → +Inf; else mul of diffs. |
| `box_penalty` (96) | `box_penalty` | MATCH | size(union) − size(original). |
| `gist_box_consistent` (112) | `gist_box_consistent` | MATCH | recheck=false; leaf→leaf_consistent, inner→internal. |
| `adjustBox` (145) | `adjustBox` | MATCH | grow b to include addon. |
| `gist_box_union` (163) | `gist_box_union` | MATCH | seed from vector[0], adjust 1..n. |
| `gist_box_penalty` (198) | `gist_box_penalty` | MATCH | box_penalty cast to f32. |
| `fallbackSplit` (215) | `fallbackSplit` | MATCH | first half left, rest right; per-side union. |
| `interval_cmp_lower/upper` (314/326) | sort closures | MATCH | float8_cmp_internal ordering. |
| `non_negative` (338) | `non_negative` | MATCH | NaN/neg → 0. |
| `g_box_consider_split` (350) | `g_box_consider_split` | MATCH | ratio/overlap/range selection branch-for-branch. |
| `common_entry_cmp` (459) | sort closure | MATCH | by delta. |
| `gist_box_picksplit` (494) | `gist_box_picksplit` | MATCH | double-sorting split; PLACE_LEFT/RIGHT macros inlined; m=ceil(LIMIT_RATIO*nentries); common-entry delta + distribution. |
| `gist_box_same` (851) | `gist_box_same` | MATCH | exact float8_eq on all four corners. |
| `gist_box_leaf_consistent` (871) | `gist_box_leaf_consistent` | MATCH | 12-strategy box predicate dispatch via geo seams; default → unrecognized strategy. |
| `rtree_internal_consistent` (956) | `rtree_internal_consistent` | MATCH | negated/overlap strategy mapping; Same/Contains share box_contain; default → error. |
| `gist_poly_compress` (1034) | `gist_poly_compress` | MATCH (new) | leaf: bbox of in->boundbox via `poly_query_boundbox` seam; inner passthrough. |
| `gist_poly_consistent` (1061) | `gist_poly_consistent` | MATCH (new) | recheck=true; uses query->boundbox + rtree_internal_consistent at all levels (entries are boxes). PG_FREE_IF_COPY is the deferred fmgr-detoast boundary. |
| `gist_circle_compress` (1099) | `gist_circle_compress` | MATCH (new) | leaf: bbox = center ± radius (float8_pl/mi = unchecked +/−) via `circle_bbox`; inner passthrough. |
| `gist_circle_consistent` (1129) | `gist_circle_consistent` | MATCH (new) | recheck=true; bbox = center ± radius; rtree_internal_consistent. |
| `gist_point_compress` (1167) | `gist_point_compress` | MATCH | leaf point → degenerate box high=low=point; inner passthrough. |
| `gist_point_fetch` (1195) | `gist_point_fetch` | MATCH | point reconstructed from box->high. |
| `computeDistance` (1220) | `computeDistance` | MATCH | leaf→point_point_distance(low); inside→0; over/below; left/right; vertex (4 corners min). `elog(ERROR,"inconsistent point values")` → PgError. |
| `gist_point_consistent_internal` (1286) | `gist_point_consistent_internal` | MATCH | Left/Right/Above/Below via FPlt/FPgt seams; Same leaf=FPeq(low,query), inner=fuzzy box containment via FPle/FPge. |
| `gist_point_consistent` (1336) | `gist_point_consistent` | MATCH (poly/circle groups new) | RTOld→new remap; group = strat/20; Point/Box groups as before (recheck=false). Polygon group: calls gist_poly_consistent w/ RTOverlap, then on leaf+match `poly_contain_pt(query,&box->high)` w/ recheck=false. Circle group: gist_circle_consistent + `circle_contain_pt`. Asserts box.high==box.low at leaf (debug_assert). |
| `gist_point_distance` (1454) | `gist_point_distance` | MATCH | Point group → computeDistance(GIST_LEAF,...); else error. |
| `gist_bbox_distance` (1478) | `gist_bbox_distance` | MATCH | Point group → computeDistance(false, box, point); else error. |
| `gist_box_distance` (1499) | `gist_box_distance` | MATCH | gist_bbox_distance; recheck not set (exact). |
| `gist_circle_distance` (1525) | `gist_circle_distance` | MATCH (new) | gist_bbox_distance; *recheck=true (lossy MBR distance). |
| `gist_poly_distance` (1542) | `gist_poly_distance` | MATCH (new) | gist_bbox_distance; *recheck=true. |
| `point_zorder_internal` (1574) | `point_zorder_internal` | MATCH | interleave part_bits32_by2. |
| `part_bits32_by2` (1585) | `part_bits32_by2` | MATCH | 5-step bit-spreading masks. |
| `ieee_float32_to_uint32` (1602) | `ieee_float32_to_uint32` | MATCH | NaN→0xFFFFFFFF; sign-flip / set-high-bit. |
| `gist_bbox_zorder_cmp` (1680) | `gist_bbox_zorder_cmp` | MATCH | quick eq; z-order compare of low points. |
| `gist_bbox_zorder_abbrev_convert/abort` (1713/1735) | folded into sortsupport | see below | logic present (z-order); install carrier-blocked. |
| `gist_point_sortsupport` (1744) | `dispatch_sortsupport` | SEAM-AND-PANIC | `SortSupportData` carrier lacks comparator/abbrev fn-ptr fields; sorted GiST build gated on `table_index_build_scan`. Pre-existing; z-order comparison logic fully ported. |

## Datum<->struct marshaling

- BOX: `BOX::from_datum_bytes`/`to_datum_bytes` (32 bytes, high then low) —
  matches `struct BOX`.
- POINT: `Point::from_datum_bytes`/`to_datum_bytes` (16 bytes).
- CIRCLE: `CIRCLE::from_datum_bytes`/`to_datum_bytes` (24 bytes: center 16 +
  radius 8) — added to types-core for this task; matches `struct CIRCLE`.
- POLYGON: `Polygon::from_datum_image`/`to_datum_image` (added to geo-ops, where
  the owned `Polygon` lives). In-memory varlena image: 4-byte `vl_len_`, `int32
  npts`, `BOX boundbox` (32), then `Point p[npts]` — matches `struct POLYGON`
  with `POLYGON_HEADER_SIZE = offsetof(POLYGON,p) = 40`. The GiST dispatch carries
  the polygon query as this raw image and decodes inside the geo-ops owner via
  the `poly_query_boundbox` / `poly_contain_pt_image` seams (the owned `Polygon`
  type is not in types-core, so the seam takes `&[u8]`).

## Seams installed by this crate (dispatch)

All 11 `backend-access-gist-dispatch-seams` (consistent/union/compress/
decompress/penalty/picksplit/same/distance/fetch/options/sortsupport) installed
in `init_seams()`. box/point/poly/circle + inet OID arms wired. decompress/
options error for box/point/poly/circle (no such proc). recurrence guard passes.

## Seams added + installed by geo-ops (for this task)

`backend-utils-adt-geo-ops-seams`: `poly_query_boundbox(&[u8]) -> BOX`,
`poly_contain_pt_image(&[u8], &Point) -> PgResult<bool>`,
`circle_contain_pt(&CIRCLE, &Point) -> PgResult<bool>` — all installed by
`backend-utils-adt-geo-ops::init_seams()`; recurrence guard
`every_declared_seam_is_installed_by_its_owner` passes.

## Residual

`gist_point_sortsupport` SortSupportData install only (carrier-blocked, sorted
build gated on table_index_build_scan; z-order math is ported). No own-logic
todo!/unimplemented!/stubs. No CONTRACT_RECONCILE entries.

## Gate

cargo check --workspace clean (pre-existing warnings only). no-todo-guard ok.
seams-init recurrence guards (both) pass. crate tests pass (geo-ops 5, gist-proc
16, types-core). Self-audit: all 12 new/changed arms MATCH; OID + strategy +
group constants verified vs headers.
