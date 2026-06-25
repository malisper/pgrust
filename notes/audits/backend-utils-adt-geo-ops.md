# Audit: backend-utils-adt-geo-ops

Function-by-function audit of `src/backend/utils/adt/geo_ops.c` (PostgreSQL
18.3) and the fuzzy-comparison macros / `pg_hypot` from
`src/include/utils/geo_decls.h` against the Rust port
(`crates/backend-utils-adt-geo-ops/src/*.rs`), reconciled from the
`src-idiomatic` reference and verified against the C source. Unit `c_sources` =
`*/geo_ops.c`.

Scope note: the `Datum NAME(PG_FUNCTION_ARGS)` fmgr shims (PG_GETARG / PG_RETURN
/ PG_FREE_IF_COPY / escontext soft-error plumbing / varlena palloc+SET_VARSIZE)
are the project-wide deferred Datum/fmgr boundary; each computational core is
ported under its C name behind a typed signature over `types_core::geo` value
structs plus the owned `Path`/`Polygon` stand-ins (points held as `Vec<Point>`;
the toast varlena serialization is the deferred fmgr layer's job). The existing
predicate subset used by the GiST/SP-GiST `box`/`point` opclasses is preserved
unchanged.

## Constant / macro verification (geo_decls.h, float.h)

| Item | C value | Port | Verdict |
|---|---|---|---|
| `EPSILON` | `1.0e-06` (geo_decls.h:41) | `EPSILON = 1.0e-6` (lib.rs) | MATCH |
| `FPzero(A)` | `fabs(A) <= EPSILON` | `a.abs() <= EPSILON` | MATCH |
| `FPeq(A,B)` | `A==B || fabs(A-B) <= EPSILON` | same | MATCH (not NaN-aware: NaN→false) |
| `FPne(A,B)` | `A!=B && fabs(A-B) > EPSILON` | same | MATCH |
| `FPlt(A,B)` | `A + EPSILON < B` | same | MATCH |
| `FPle(A,B)` | `A <= B + EPSILON` | same | MATCH |
| `FPgt(A,B)` | `A > B + EPSILON` | same | MATCH |
| `FPge(A,B)` | `A + EPSILON >= B` | same | MATCH |
| `HYPOT(A,B)` | `pg_hypot(A,B)` | `pg_hypot` | MATCH |
| `M_PI` | `3.14159265358979323846` | `core::f64::consts::PI` | MATCH |
| `POINT_ON_POLYGON` | `INT_MAX` (geo_ops.c:5337) | `i32::MAX` | MATCH |
| `LDELIM/RDELIM/DELIM` | `'(' ')' ','` | same | MATCH |
| `LDELIM_EP/RDELIM_EP` | `'[' ']'` | same | MATCH |
| `LDELIM_C/RDELIM_C` | `'<' '>'` | same | MATCH |
| `LDELIM_L/RDELIM_L` | `'{' '}'` | same | MATCH |
| `offsetof(PATH,p)` | 16 (vl_len_,npts,closed,dummy) | `PATH_HEADER_SIZE=16` | MATCH |
| `offsetof(POLYGON,p)` | 40 (vl_len_,npts,BOX boundbox) | `POLYGON_HEADER_SIZE=40` | MATCH |
| Overflow SQLSTATE | `ERRCODE_PROGRAM_LIMIT_EXCEEDED` (54000) | same | MATCH |
| Input syntax SQLSTATE | `ERRCODE_INVALID_TEXT_REPRESENTATION` (22P02) | same | MATCH |
| Binary SQLSTATE | `ERRCODE_INVALID_BINARY_REPRESENTATION` (22P03) | same | MATCH |
| `circle_poly` radius-zero | `ERRCODE_FEATURE_NOT_SUPPORTED` (0A000) | same | MATCH |
| `circle_poly` npts<2, `line/path` distinct-pt/open | `ERRCODE_INVALID_PARAMETER_VALUE` (22023) | same | MATCH |
| float over/underflow | `ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE` (22003), "value out of range: overflow/underflow" | routed via float seams + local pg_hypot reporters | MATCH |

No strategy numbers exist in geo_ops.c (those live in the opclass procs, in the
already-merged gist/spgist crates). The fuzzy comparator EPSILON is the only
numeric tolerance and is bit-identical to C.

## Cross-subsystem float8 routing

All `float8_pl/mi/mul/div/eq/lt/gt/min/max`, `get_float8_infinity/nan`, and the
text I/O (`float8in_internal`→`float8in_internal_endptr`,
`float8out_internal`) cross into `utils/adt/float.c`. They are reached through
`backend-utils-adt-float-seams` (declarations added here, installed by
`backend-utils-adt-float::init_seams`). Overflow/underflow/divide-by-zero
therefore propagate with the exact float.c message text and 22003/22012
SQLSTATE. `check_stack_depth` (polygon recursion guard, geo_ops.c:3875) routes
to `backend-utils-misc-stack-depth-seams`; `CHECK_FOR_INTERRUPTS`
(geo_ops.c:3885) routes to `backend-tcop-postgres-seams` (the canonical owner,
installed by miscinit).

## Function inventory (by module; every geo_ops.c definition)

Verdict MATCH = branch-for-branch identical control flow, same fuzzy-vs-exact
comparison choice, same seam (overflow-checked) arithmetic, same error
text/SQLSTATE.

### point.rs (geo_ops.c 1817-2046, 4089-4207)
`point_construct`, `construct_point`, `point_left/right/above/below/vert/horiz`,
`point_eq/ne`, `point_eq_point` (NaN→exact `float8_eq`, else FPeq — MATCH),
`point_distance/point_dt` (HYPOT of float8_mi deltas), `point_slope/point_sl`
(Inf on equal-x, 0 on equal-y), `point_invsl`, `point_add(_point)`,
`point_sub(_point)`, `point_mul(_point)` (complex mult), `point_div(_point)`
(complex div by |b|²). All MATCH.

### line.rs (geo_ops.c 943-1354, 3086, 2723)
`line_construct` (vertical/-horizontal/general, -0 normalization),
`line_construct_pp` (22023 on equal pts), `line_sl/line_invsl`,
`line_intersect/parallel/perp/vertical/horizontal`, `line_eq` (NaN→exact, else
proportional ratio), `line_distance`, `line_interpt`, `line_interpt_line`
(both-B branches, -0 normalization, identical-lines→parallel),
`line_contain_point`, `line_closept_point` (NaN-distance fallback sets result to
point). All MATCH.

### lseg.rs (geo_ops.c 2049-2372, 2674-2711, 3108)
`lseg_construct/statlseg_construct`, `lseg_sl/invsl/length`,
`lseg_intersect/parallel/perp/vertical/horizontal`, `lseg_eq/ne`,
`lseg_lt/le/gt/ge` (by length), `lseg_center`, `lseg_interpt_lseg/interpt`,
`lseg_interpt_line` (endpoint snap to defeat LSB residue),
`lseg_contain_point` (triangle-inequality). All MATCH.

### boxes.rs (geo_ops.c 405-941, 4210-4335, 4534-4560, 5186-5221)
`box_construct/points_box`, `box_same/overlap/box_ov`,
`box_left/overleft/right/overright/below/overbelow/above/overabove`,
`box_contained/contain/contain_box`, `box_below_eq/above_eq` (obsolete),
`box_lt/gt/eq/le/ge` (by area), `box_area/box_ar`, `box_width/box_wd`,
`box_height/box_ht`, `box_distance`, `box_center/box_cn`, `box_intersect`,
`box_diagonal`, `box_contain_point` (exact >=/<=), `box_contain_lseg`,
`box_add/sub/mul/div`, `point_box`, `boxes_bound_box`, `circle_box`,
`box_circle`. All MATCH. (The 12 box predicates + box_contain_box + box_ov +
box_same here are the exact bodies the GiST/SP-GiST seam subset installs.)

### circle.rs (geo_ops.c 4594-5317)
`cr_circle`, `circle_same` (NaN radii equal), `circle_overlap/overleft/left/
right/overright/contained/contain/below/above/overbelow/overabove`,
`circle_eq/ne/lt/gt/le/ge` (by area), `circle_add/sub/mul/div_pt`,
`circle_area/circle_ar` (πr²), `circle_diameter/radius/center/distance`,
`circle_contain_pt/pt_contained_circle`, `circle_poly` (0A000 radius-zero,
22023 npts<2, 54000 overflow via check_points_overflow, anglestep loop). All
MATCH.

### path.rs (geo_ops.c 1379-1815, 4344-4484)
`path_n_lt/gt/eq/le/ge`, `path_isclosed/isopen/npoints`, `path_close/open`,
`path_area` (shoelace, None if open), `path_length` (closure when closed),
`path_inter` (bbox quick-reject then pairwise lseg), `path_distance`,
`path_add` (None if either closed, 54000 overflow guard), `path_add/sub/mul/
div_pt`, `path_poly` (22023 if open), `poly_path`. All MATCH.

### poly.rs (geo_ops.c 3375-4086, 4493-4591, 5285-5317)
`make_bound_box`, `poly_left/overleft/right/overright/below/overbelow/above/
overabove` (exact bbox compares per C), `poly_same` (npts + plist_same),
`poly_overlap(_internal)`, `touched_lseg_inside_poly`/`lseg_inside_poly`
(recursive, check_stack_depth + CHECK_FOR_INTERRUPTS guards),
`poly_contain(_poly)/contained`, `poly_contain_pt/pt_contained_poly`,
`poly_distance`, `poly_npoints/center/box`, `poly_to_circle/poly_circle`. All
MATCH.

### proximity.rs (geo_ops.c 2381-3362, 5105-5138)
dist_*: `dist_pl/lp/ps/sp/ppath/pathp/pb/bp/sl/ls/sb/bs/cpoly/polyc/ppoly/polyp/
pc/cpoint`, `lseg_distance`. close_*: `lseg_closept_point/line/lseg`,
`box_closept_point/lseg`, `close_pl/ps/lseg/pb/ls/sb`. on_*: `on_pl/ps/pb/
ppath/sl/sb`, `box_contain_pt`. inter_*: `inter_sl/sb/lb`, `box_interpt_lseg`.
The `Option<&mut Point>` "result-or-NULL" out-parameter is modeled with
`reborrow`/`as_deref_mut`, preserving C's pass-through-result semantics. All
MATCH.

### io.rs (geo_ops.c 193-402 + each type's in/out/recv/send)
Decoders `single_decode/pair_decode/path_decode/line_decode` driven by a
NUL-terminated-string `Cursor` (cur()=`*str`/'\0', next()=`*str++`, skip_ws,
strrchr-equivalent `last_occurrence_is_here`), reporting the `endptr_p` stopping
point via the float8 seam. `pair_count` ((ndelim+1)/2 odd else -1). Encoders
`single/pair/path_encode`. Per-type: `point/box/line/lseg/path/polygon/circle`
`*_in/_out/_recv/_send`, plus `box_poly`. Binary recv/send use big-endian
`pq_getmsg*/pq_send*` semantics with "insufficient data left in message" (22P03)
short-buffer error and the npts validity checks (22P03). `check_points_overflow`
reproduces C's 32-bit `int` base_size/size truncation + signed division guard so
the "too many points requested" (54000) threshold matches C bit-for-bit on a
64-bit host. All MATCH.

### lib.rs (geo_decls.h pg_hypot + geo_ops.c 5337-5505)
`pg_hypot` (INF/NaN handling, swap, y==0 fast path, overflow/underflow
reporters), `point_inside`/`lseg_crossing` (ray-cast crossing number, overflow
on the `z` determinant raises 22003), `plist_same` (rotation- and
direction-independent match). All MATCH.

## Deliberate model deviations (behavior-preserving)

1. `Path`/`Polygon` points are an owned `Vec<Point>` rather than a varlena
   `FLEXIBLE_ARRAY_MEMBER`; the palloc/SET_VARSIZE serialization is the deferred
   fmgr layer. The observable `npts`/`closed`/`boundbox`/point data and the
   32-bit overflow guards are faithful.
2. `*_out`/`*_send` return owned `String`/`Vec<u8>` instead of a palloc'd
   `StringInfo`; the rendered bytes are identical (the C StringInfo never
   surfaces an OOM to its caller).
3. `Point`/`BOX`/`LSEG`/`LINE`/`CIRCLE` derive `Copy` in this repo, so the
   reference's `.clone()` is `*`-copy; semantics identical.

## Gate results

- `cargo check --workspace`: clean.
- `cargo test -p backend-utils-adt-geo-ops`: 15 parity tests pass (FP epsilon,
  pg_hypot specials, point/box/line/lseg/path/poly/circle I/O round-trips,
  binary recv/send + short-buffer error, box_poly, lseg/box/poly/circle
  distance + closest-point, parallel close_lseg→None, point arithmetic +
  point_inside in/on/out, circle_poly overflow + success).
- `cargo test -p backend-utils-adt-float`: 31 pass (seam adapters intact).
- seams-init recurrence guards (`every_seam_installing_crate_is_wired_into_
  init_all`, `every_declared_seam_is_installed_by_its_owner`): pass.
- no-todo-guard: no `todo!`/`unimplemented!`/`panic!` in this crate.
- GiST/SP-GiST consumers (geo-spgist-only, spg-quadtree, spg-kdtree, gist-proc,
  spg-proc): compile unchanged against the preserved predicate seam subset.

## GiST #285 prerequisite

Task #285's stated prereq is "geo_ops.c box predicates". The full box
predicate/area/distance/closest-point surface (`box_contain_box`, `box_ov`,
`box_same`, the 12 directional predicates, `box_ar`, `box_closept_*`,
`box_interpt_lseg`, `dist_*b*`) is now ported and tested. Prerequisite
**satisfied**.

## Verdict

PASS. Full geo_ops.c ported branch-for-branch; constants and SQLSTATEs verified
against the C; no stubs.
