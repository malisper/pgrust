# Audit: backend-utils-adt-geo-spgist-only

Independent function-by-function audit of `src/backend/utils/adt/geo_spgist.c`
(PostgreSQL 18.3) against the c2rust rendering
(`c2rust-runs/backend-utils-adt-geo-spgist-only/src/geo_spgist.rs`) and the Rust
port (`crates/backend-utils-adt-geo-spgist-only/src/lib.rs`).

Unit `c_sources` = `*/geo_spgist.c` (single file). Re-derived from the C and the
relevant headers; the port's comments/self-review were not trusted.

## Function inventory (every C definition)

| # | C function (line) | Port location | Verdict | Notes |
|---|---|---|---|---|
| 1 | `compareDoubles` (92) | `compare_doubles` (119) | MATCH | `==`→Equal, `>`→Greater, else Less; plain (non-fuzzy) per C comment. NaN behavior identical to C qsort comparator. |
| 2 | `getQuadrant` (129) | `getQuadrant` (131) | MATCH | 4 bit-set comparisons `> centroid`, bits 0x8/0x4/0x2/0x1 in the same order. |
| 3 | `getRangeBox` (156) | `getRangeBox` (155) | MATCH | left={low.x,high.x}, right={low.y,high.y}. |
| 4 | `initRectBox` (176) | `initRectBox` (170) | MATCH | All 8 bounds set to ±infinity; `get_float8_infinity()` inlined as `f64::INFINITY` (utils/float.h returns `(float8) INFINITY`). |
| 5 | `nextRectBox` (204) | `nextRectBox` (199) | MATCH | memcpy→`*rect_box` copy; 4 quadrant-bit branches assign centroid bounds in identical order. |
| 6 | `overlap2D` (235) | `overlap2D` (235) | MATCH | `FPge(right.high,low) && FPle(left.low,high)` (seamed FP). |
| 7 | `overlap4D` (243) | `overlap4D` (240) | MATCH | x.left && y.right. |
| 8 | `contain2D` (251) | `contain2D` (245) | MATCH | `FPge(right.high,high) && FPle(left.low,low)`. |
| 9 | `contain4D` (259) | `contain4D` (250) | MATCH | |
| 10 | `contained2D` (267) | `contained2D` (255) | MATCH | 4-clause conjunction in same order. |
| 11 | `contained4D` (277) | `contained4D` (263) | MATCH | |
| 12 | `lower2D` (285) | `lower2D` (269) | MATCH | `FPlt(left.low,low) && FPlt(right.low,low)`. |
| 13 | `overLower2D` (293) | `overLower2D` (274) | MATCH | |
| 14 | `higher2D` (301) | `higher2D` (279) | MATCH | |
| 15 | `overHigher2D` (309) | `overHigher2D` (284) | MATCH | |
| 16 | `left4D` (317) | `left4D` (289) | MATCH | `lower2D(range_box_x, query.left)`. |
| 17 | `overLeft4D` (324) | `overLeft4D` (294) | MATCH | |
| 18 | `right4D` (331) | `right4D` (299) | MATCH | |
| 19 | `overRight4D` (338) | `overRight4D` (304) | MATCH | |
| 20 | `below4D` (345) | `below4D` (309) | MATCH | uses range_box_y / query.right. |
| 21 | `overBelow4D` (352) | `overBelow4D` (314) | MATCH | |
| 22 | `above4D` (359) | `above4D` (319) | MATCH | |
| 23 | `overAbove4D` (366) | `overAbove4D` (324) | MATCH | |
| 24 | `pointToRectBoxDistance` (373) | `pointToRectBoxDistance` (333) | MATCH | dx/dy 3-way branches identical; returns `HYPOT(dx,dy)` as `PgResult` (pg_hypot can ereport). |
| 25 | `spg_box_quad_config` (400) | `spg_box_quad_config` (510) | MATCH | prefixType=BOXOID, labelType=VOIDOID, canReturnData=true, longValuesOK=false. |
| 26 | `spg_box_quad_choose` (416) | `spg_box_quad_choose` (518) | MATCH | resultType=MatchNode, restDatum=box; nodeN=getQuadrant only when `!allTheSame`. |
| 27 | `spg_box_quad_picksplit` (440) | `spg_box_quad_picksplit` (535) | MATCH | 4 coord arrays filled, sorted via compare_doubles, median=n/2, centroid built, nNodes=16, nodeLabels=NULL, per-tuple quadrant assignment. |
| 28 | `is_bounding_box_test_exact` (507) | `is_bounding_box_test_exact` (594) | MATCH | exact for Left/OverLeft/OverRight/Right/OverBelow/Below/Above/OverAbove; else false. |
| 29 | `spg_box_quad_get_scankey_bbox` (530) | `spg_box_quad_get_scankey_bbox` (613) | MATCH | BOXOID→bbox; POLYGONOID→bbox with optional `*recheck` set when test inexact; default→ERROR "unrecognized scankey subtype". `recheck` carried as `Option<&mut bool>` (NULL vs &out->recheck). |
| 30 | `spg_box_quad_inner_consistent` (552) | `spg_box_quad_inner_consistent` (630) | MATCH | traversalValue or initRectBox; allTheSame branch (all nodeNumbers + per-node distance copies); else centroid+queries, per-quadrant nextRectBox, 11-way strategy switch + default ERROR (line 691), flag-break, push traversal/distances. MemoryContextSwitchTo subsumed by owned `Vec`. pfree-on-reject = drop. |
| 31 | `spg_box_quad_leaf_consistent` (740) | `spg_box_quad_leaf_consistent` (752) | MATCH | recheck=false; leafValue only if returnData; 12-way DirectFunctionCall2 box predicate switch + default ERROR (line 831), flag-break; if flag&&norderbys>0: distances=spg_key_orderbys_distances(leaf, isLeaf=false), recheckDistances = distfnoid==F_DIST_POLYP. |
| 32 | `spg_bbox_quad_config` (858) | `spg_bbox_quad_config` (818) | MATCH | adds leafType=BOXOID, canReturnData=false. |
| 33 | `spg_poly_quad_compress` (875) | `spg_poly_quad_compress` (831) | MATCH | returns polygon->boundbox (palloc'd copy = owned value). |

Total: 33 functions, all present, all MATCH.

## Constants verified against headers (not from memory)

- `BOXOID = 603`, `POLYGONOID = 604`, `VOIDOID = 2278` — `catalog/pg_type.dat`. ✓
- `F_DIST_POLYP = 3292` — `pg_proc.dat` `dist_polyp` oid 3292. ✓
- RT strategy numbers 1–12 — `access/stratnum.h`: Left=1, OverLeft=2, Overlap=3,
  OverRight=4, Right=5, Same=6, Contains=7, ContainedBy=8, OverBelow=9, Below=10,
  Above=11, OverAbove=12. All match. ✓
- `getQuadrant` quadrant bits 0x8/0x4/0x2/0x1 verified against C. ✓
- `BOX { high: Point, low: Point }` (`types-core::geo`) matches `geo_decls.h`. ✓

## Seam audit

This unit's only `c_source` is `geo_spgist.c`, so it owns **no inward seam
crate** and has **no `init_seams()`** — correct, and consistent with the
fmgr-dispatched opclass procedures (no inward callers). The recurrence guard
(`seams-init`) passes both `every_declared_seam_is_installed_by_its_owner` and
`every_seam_installing_crate_is_wired_into_init_all`.

Two **outward** seam crates are consumed (real dependency: both owners unported,
CATALOG status `todo`):

- `backend-utils-adt-geo-ops-seams` — `FPlt/FPle/FPgt/FPge`, `HYPOT`, and the 12
  `box_*` boolean operators; owner `geo_ops.c` (+ `geo_decls.h` fuzzy macros),
  unported. Each call site is thin marshal+delegate (no branching/computation in
  the seam path). HYPOT/FP returns `PgResult`/`bool` matching the C surface.
- `backend-access-spg-proc-seams` — `spg_key_orderbys_distances`; owner
  `spgproc.c`, unported. The C `(Datum key, bool isLeaf)` pair is carried by
  `SpgKey`; the box leaf opclass passes `isLeaf=false` → `SpgKey::InnerBox`
  (verified against `spgproc.c`: the `point_box_distance` path). Returns
  `PgResult` (pg_hypot can ereport). Thin delegate.

No function body was replaced by a seam to "somewhere else"; every owned piece of
logic (quadrant math, predicates, picksplit, consistent control flow, error
paths) lives in this crate. The seamed callees are genuinely-unported neighbors —
the sanctioned mirror-PG-and-panic pattern, not absent logic.

## Design conformance

- No invented opacity: `Range/RangeBox/RectBox` are faithful plain `Copy`
  structs; `BOX/Point/SpgKey` are the real `types-core::geo` types.
- No own-logic `todo!()`/`unimplemented!()` (grep clean).
- Error sites map to `PgError::error(...).with_sqlstate(ERRCODE_INTERNAL_ERROR)`
  with the exact C message text and `elog(ERROR, ...)` line locations (544, 691,
  831). `elog(ERROR)` = XX000/internal-error — matches.
- Allocates only owned `Vec`s/values (C `palloc`s were call-local or output
  arrays); no shared statics, no locks, no registry side tables, no
  ambient-global seams.

## Gates

- `cargo check --workspace` — pass (only pre-existing unrelated warnings in
  `backend-access-common-printtup`).
- `cargo test -p backend-utils-adt-geo-spgist-only` — pass (0 tests; crate is
  fmgr-dispatched logic).
- `cargo test -p seams-init` — pass (2 recurrence-guard tests).

## Verdict: PASS

Every function MATCHes or is correctly SEAMED to a named unported owner; zero
seam findings; constants verified against headers; gates green.
