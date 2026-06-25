# Audit addendum: `array_map` + ArrayCoerce element coercion

Scope: the `array_map` port (previously SEAMED / mirror-and-panic in
`audits/backend-utils-adt-arrayfuncs.md`) and the `ExecEvalArrayCoerce`
runtime path it backs. Re-derived against `arrayfuncs.c:3201` (`array_map`)
and `execExprInterp.c` (`ExecEvalArrayCoerce`).

## Structural decision

`array_map(Datum, ExprState*, ExprContext*, Oid retType, ArrayMapState*)` is
split at the only line that crosses the executor boundary — the per-element
`ExecEvalExpr(exprstate, econtext, &nulls[i])`. `ExecEvalExpr` / `ExprState`
/ `ExprContext` live in `backend-executor-execExprInterp`; a direct dep
`arrayfuncs -> executor` cycles. So:

- front half (`DatumGetAnyArrayP` detoast, read `AARR_NDIM/DIMS/LBOUND`,
  `get_typlenbyvalalign(inpType)`, deconstruct whole array) =
  `construct::array_map_deconstruct` (owner) → `array_map_deconstruct` seam;
- back half (`get_typlenbyvalalign(retType)`, the `att_addlength`/`att_align`/
  overflow/`CopyArrayEls` result assembly reusing source dims/lbound) =
  `construct::array_map_build` (owner) → `array_map_build` seam, delegating to
  the already-audited `construct_md_array_values`;
- the per-element loop (`*transform_source = elem`; `ExecEvalExpr`) =
  `eval_array::ExecEvalArrayCoerce` in the executor, between the two seams.

The seams are thin marshal+delegate; the loop logic lives in the executor
crate (its rightful owner), not in a seam closure.

## Per-function verdicts

| C function | port | verdict | notes |
|---|---|---|---|
| `array_map` (arrayfuncs.c:3201) | `array_map_deconstruct` + executor loop + `array_map_build` | MATCH | see line-by-line below |
| `ExecEvalArrayCoerce` (execExprInterp.c) | `eval_array::ExecEvalArrayCoerce` | MATCH | both branches now real |
| `array_coerce_relabel` (the binary-compat ExecEvalArrayCoerce branch) | `construct::array_coerce_relabel` | MATCH | `DatumGetArrayTypePCopy` + `ARR_ELEMTYPE=` rewrite |

### `array_map` line-by-line

- `v = DatumGetAnyArrayP(arrayd)` → `detoast_attr` on the source bytes. MATCH.
- `inpType = AARR_ELEMTYPE(v); ndim = AARR_NDIM; dim = AARR_DIMS; nitems =
  ArrayGetNItems(...)` → `arr_elemtype`/`arr_ndim`/`arr_dims`. MATCH.
- `if (nitems <= 0) return construct_empty_array(retType)`. In the split, an
  empty source yields an empty `elems` list; the executor loops zero times and
  `array_map_build` calls `construct_md_array_values`, which returns
  `construct_empty_array(retType)` when `ArrayGetNItems(ndim,dims) <= 0`.
  Behaviorally MATCH. Only difference: C skips the two
  `get_typlenbyvalalign` lookups on the empty path; the port performs them.
  No observable difference — those lookups cannot fail for a type that
  produced a valid array Datum.
- `inp_extra/ret_extra` element-type attr caching in `ArrayMapState`:
  the port recomputes `get_typlenbyvalalign(inpType)` and `(retType)` each
  call. The cache is a pure per-call optimization (re-derives identical
  values); recomputing is behaviorally identical. The step's `amstate` sentinel
  is now genuinely unused (the caching it represented is subsumed). MATCH.
- per element: `*transform_source = array_iter_next(...)` then
  `values[i] = ExecEvalExpr(...)`. The executor writes the source element into
  `elemstate.innermost_caseval` / `innermost_casenull` (the C
  `exprstate->innermost_caseval`) and runs `ExecInterpExprStillValid`
  (this repo's `ExecEvalExpr`). Source elements come from
  `deconstruct_array_values` (the same per-element `(Datum,isnull)` an
  `array_iter` walk yields, in order). MATCH.
- detoast-if-`typlen==-1`, `att_addlength_datum`, `att_align_nominal`,
  `AllocSizeIsValid` overflow `ereport(ERRCODE_PROGRAM_LIMIT_EXCEEDED)`,
  `ARR_OVERHEAD_*`, header write, `memcpy(ARR_DIMS/ARR_LBOUND from source)`,
  `CopyArrayEls`: all inside `construct_md_array_values` (already audited),
  driven with the source `ndim/dims/lbs`. MATCH.

### `array_coerce_relabel`

C: `array = DatumGetArrayTypePCopy(arraydatum); ARR_ELEMTYPE(array) =
resultelemtype;`. Port: `detoast_attr` then a private copy
(`slice_to_pgvec`), then overwrite the 4-byte `elemtype` field at offset 12
(`ARR_ELEMTYPE`, verified offset against `array.h` `ArrayType`). Size/ndim/
dataoffset/dims/data untouched. MATCH.

## Seam / wiring audit

`array_map_deconstruct`, `array_map_build`, `array_coerce_relabel` declared
in `backend-utils-adt-arrayfuncs-seams` and all three installed in
`backend_utils_adt_arrayfuncs::init_seams()` (already aggregated by
`seams-init`). `ArrayMapSource` carrier defined in the seams crate (real
fields: ndim/dims/lbs/elems — no invented opacity). All allocating seams take
`Mcx<'mcx>` and return `PgResult`. No shared statics, no locks across `?`.

## Verdict: PASS

Live-verified (`postgres --single -c io_method=sync`):
`SELECT ARRAY[1,2,3]::text[]` → `{1,2,3}` (text[]); `SELECT ARRAY[1,2]::numeric[]`
→ `{1,2}` (numeric[], via `int4_numeric`); `SELECT ARRAY[1,2,3]` → `{1,2,3}`
unregressed.
