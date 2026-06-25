# Audit: backend-utils-adt-arrayfuncs

- **Unit:** backend-utils-adt-arrayfuncs (`*/arrayfuncs.c` only; the catalog row
  `backend-utils-adt-array-more` also lists `array_selfuncs.c`, audited
  separately).
- **Crate:** `crates/backend-utils-adt-arrayfuncs` (modules: foundation,
  construct, element_slice, io, ops, sql).
- **Owned seam crate:** `crates/backend-utils-adt-arrayfuncs-seams`.
- **C source:** `pgrust/postgres-18.3/src/backend/utils/adt/arrayfuncs.c`
- **c2rust:** `pgrust/c2rust-runs/backend-utils-adt-array-more/src/arrayfuncs.rs`
- **Date:** 2026-06-13 (re-audited 2026-06-13 after the array-iterator port)
- **Model:** Claude Opus 4.8 (1M context)
- **Verdict:** **PASS** — every function `MATCH` or `SEAMED` (unported callee
  panicked through its owner boundary); zero seam findings.

**Re-audit (array iterator, 2026-06-13):** `array_create_iterator`,
`array_iterate`, and `array_free_iterator` — previously SEAMED mirror-and-panic
stubs awaiting the iterator state type — are now real in-crate logic. Re-derived
each from arrayfuncs.c:4602-4773 line-by-line against the byte-model port: all
three MATCH (see table + ledger item 2). The `ArrayIteratorData` state struct is
defined in the porting crate (private to arrayfuncs.c per array.h:257, so no
types-array vocabulary and no invented opacity); slice/element setup, NULL
handling, per-call deconstruct, cursor advance, and the `construct_md_array`
slice build all reproduce the C. No new seams; `init_seams()` unchanged;
recurrence_guard passes. Crate's first unit tests added (element iteration over
int4[], invalid slice_ndim error) — both green.

Build/test gate: `cargo check --workspace` clean (warnings only, pre-existing
in printtup / backend-utils-error doc-comments); `cargo test --workspace`
exit 0, no failures.

This pass closes the prior FAIL: the five `unimplemented!()` SQL bodies and the
four entirely-absent C functions are now either real in-crate logic or a
mirror-and-panic on a genuinely-unported *callee* (executor `ExecEvalExpr`
seam, `ArrayIterator` vocabulary type, expanded-array subsystem, planner
support-request vocabulary). No `unimplemented!()` / `todo!()` bodies remain.

## 1. Function inventory and per-function verdicts

C-source function definitions (90 defs; forward decls excluded). Port location
and verdict per function. `MATCH` = logic faithful; `SEAMED` = body present,
unported callee delegated through a justified seam / loud panic.

| C function | C loc | Port | Verdict | Notes |
|---|---|---|---|---|
| array_in | 179 | io.rs | MATCH | full parse via ReadArray* helpers |
| ReadArrayDimensions | 402 | io.rs | MATCH | |
| ReadDimensionInt | 519 | io.rs | MATCH | |
| ReadArrayStr | 579 | io.rs | MATCH | |
| ReadArrayToken | 796 | io.rs | MATCH | |
| CopyArrayEls | 961 | construct.rs / io.rs | MATCH | shared engine, byte-buffer faithful |
| array_out | 1016 | io.rs | MATCH | |
| array_recv | 1271 | io.rs | MATCH | |
| ReadArrayBinary | 1454 | io.rs | MATCH | |
| array_send | 1548 | io.rs | MATCH | |
| array_ndims | 1652 | element_slice.rs | MATCH | |
| array_dims | 1668 | element_slice.rs | MATCH | |
| array_lower | 1706 | element_slice.rs | MATCH | |
| array_upper | 1733 | element_slice.rs | MATCH | |
| array_length | 1763 | element_slice.rs | MATCH | |
| array_cardinality | 1790 | element_slice.rs | MATCH | |
| array_get_element | 1820 | element_slice.rs | MATCH | |
| array_get_element_expanded | 1921 | element_slice.rs:569 | SEAMED | expanded-array subsystem (array_expanded.c) not ported; loud panic |
| array_get_slice | 2030 | element_slice.rs | MATCH | |
| array_set_element | 2201 | element_slice.rs | MATCH | |
| array_set_element_expanded | 2501 | element_slice.rs:589 | SEAMED | as above |
| array_set_slice | 2806 | element_slice.rs | MATCH | |
| array_ref | 3146 | element_slice.rs | MATCH | |
| array_set | 3163 | element_slice.rs | MATCH | |
| array_map | 3201 | sql.rs:692 | SEAMED | needs executor `ExecEvalExpr` seam + the exprstate/econtext args the scaffold signature dropped; mirror-and-panic |
| construct_array | 3361 | construct.rs | MATCH | |
| construct_array_builtin | 3381 | construct.rs | MATCH | builtin type-meta switch transcribed |
| construct_md_array | 3494 | construct.rs | MATCH | |
| construct_empty_array | 3580 | construct.rs | MATCH | |
| construct_empty_expanded_array | 3597 | construct.rs:182 | SEAMED | flat empty array built in-crate; `expand_array`/`ExpandedArrayHeader` (array_expanded.c) not ported; mirror-and-panic |
| deconstruct_array | 3631 | construct.rs | MATCH | |
| deconstruct_array_builtin | 3697 | construct.rs:233 | MATCH | builtin (typlen,byval,align) switch → deconstruct_array |
| array_contains_nulls | 3773 | construct.rs | MATCH | |
| array_eq | 3820 | ops.rs | MATCH | element_eq seam |
| array_ne | 3949 | ops.rs | MATCH | |
| array_lt | 3955 | ops.rs | MATCH | |
| array_gt | 3961 | ops.rs | MATCH | |
| array_le | 3967 | ops.rs | MATCH | |
| array_ge | 3973 | ops.rs | MATCH | |
| btarraycmp | 3979 | ops.rs | MATCH | |
| array_cmp | 3991 | ops.rs | MATCH | element_cmp seam; dimensionality tie-break faithful |
| hash_array | 4164 | ops.rs | MATCH | result*31+elthash; RECORDOID->F_HASH_RECORD |
| hash_array_extended | 4297 | ops.rs | MATCH | |
| array_contain_compare | 4387 | ops.rs | MATCH | matchall semantics, strict-NULL skip |
| arrayoverlap | 4530 | ops.rs | MATCH | |
| arraycontains | 4548 | ops.rs | MATCH | |
| arraycontained | 4566 | ops.rs | MATCH | |
| array_create_iterator | 4603 | sql.rs | MATCH | `ArrayIteratorData` struct now defined in-crate (private to arrayfuncs.c per array.h:257); slice/element setup, mstate-vs-`get_typlenbyvalalign` storage, slice dims/lbound/workspace alloc, cursor init all reproduced. `mstate` modeled as `Option<TypLenByValAlign>` (the only fields read) |
| array_iterate | 4682 | sql.rs | MATCH | scalar arm (fetch_att + att_addlength/align advance, NULL leaves data_ptr) and slice arm (per-call deconstruct into workspace, construct_md_array) reproduced; returns `ArrayIterateItem` (Scalar/Slice) so the built sub-array is carried by value, not a dangling word |
| array_free_iterator | 4765 | sql.rs | MATCH | consumes iterator by value; drop releases the mcx-allocated slice workspace, mirroring the C pfree teardown |
| array_get_isnull | 4787 | foundation.rs | MATCH | |
| array_set_isnull | 4804 | foundation.rs | MATCH | |
| ArrayCast | 4822 | construct.rs (in array_cast_and_set) | MATCH | byval/varlena/cstring/fixed cases |
| ArrayCastAndSet | 4833 | construct.rs / io.rs / element_slice.rs | MATCH | |
| array_seek | 4872 | foundation.rs | MATCH | |
| array_nelems_size | 4920 | foundation.rs | MATCH | |
| array_copy | 4942 | foundation.rs | MATCH | |
| array_bitmap_copy | 4972 | foundation.rs | MATCH | |
| array_slice_size | 5043 | element_slice.rs | MATCH | |
| array_extract_slice | 5103 | element_slice.rs | MATCH | |
| array_insert_slice | 5176 | element_slice.rs | MATCH | |
| initArrayResult | 5299 | construct.rs | MATCH | |
| initArrayResultWithSize | 5316 | construct.rs | MATCH | |
| accumArrayResult | 5356 | construct.rs | MATCH | |
| makeArrayResult | 5426 | construct.rs | MATCH | |
| makeMdArrayResult | 5458 | construct.rs | MATCH | |
| initArrayResultArr | 5510 | construct.rs | MATCH | |
| accumArrayResultArr | 5556 | construct.rs | MATCH | |
| makeArrayResultArr | 5709 | construct.rs | MATCH | |
| initArrayResultAny | 5788 | construct.rs | MATCH | owned seam installer |
| accumArrayResultAny | 5835 | construct.rs | MATCH | owned seam installer |
| makeArrayResultAny | 5863 | construct.rs | MATCH | owned seam installer |
| array_larger | 5893 | sql.rs | MATCH | |
| array_smaller | 5902 | sql.rs | MATCH | |
| generate_subscripts | 5923 | sql.rs:94 | MATCH | SRF per-call materialization |
| generate_subscripts_nodir | 5987 | sql.rs:140 | MATCH | wrapper → generate_subscripts(reverse=false) |
| array_fill_with_lower_bounds | 5998 | sql.rs:244 (array_fill) | MATCH | folded into array_fill; varlena element bytes route through detoast seam |
| array_fill | 6039 | sql.rs:244 | MATCH | as above; assembly via construct_md_array |
| create_array_envelope | 6074 | sql.rs (in array_fill) | MATCH | dimension/overflow checks present |
| array_fill_internal | 6091 | sql.rs (in array_fill) | MATCH | varlena element bytes via detoast seam callee |
| array_unnest | 6260 | sql.rs:830 | MATCH | flat-array arm; SRF materialized in order; expanded-header fast path is unported callee |
| array_unnest_support | 6351 | sql.rs:895 | SEAMED | planner support-request vocabulary (SupportRequestRows / estimate_*) not ported; mirror-and-panic |
| array_replace_internal | 6387 | sql.rs:443 | MATCH | full scan/match/rebuild: lookup_element_eq_opr + element_eq seams, construct_md_array rebuild, remove shrinks dim[0] |
| array_remove | 6645 | sql.rs:280 | MATCH | wrapper → array_replace_internal(remove=true) |
| array_replace | 6667 | sql.rs:299 | MATCH | wrapper → array_replace_internal(remove=false) |
| width_bucket_array | 6696 | sql.rs:614 | MATCH | float8 + generic fixed + variable paths all present |
| width_bucket_array_float8 | 6759 | sql.rs | MATCH | |
| width_bucket_array_fixed | 6803 | sql.rs | MATCH | cmp_proc + element_cmp seams; operand crosses as ArrayElementDatum |
| width_bucket_array_variable | 6858 | sql.rs:739 | MATCH | walk-from-left + O(N) base advance, faithful to C |
| trim_array | 6928 | sql.rs | MATCH | builds slice, calls array_get_slice |

## 2. Seam audit

Owned seam crate `backend-utils-adt-arrayfuncs-seams` declares 8 entry points
(init_array_result_any, accum_array_result_any, make_array_result_any,
pfree_array_datum, construct_array_builtin, deconstruct_text_array,
deconstruct_tid_array, construct_text_array). All 8 are installed by
`backend_utils_adt_arrayfuncs::init_seams()` (lib.rs:63-74), which contains only
`set()` calls, and `seams-init::init_all()` calls it (seams-init/src/lib.rs:60).
This unit's only C file is `arrayfuncs.c`, so this is the sole owned seam crate.
**No seam findings.**

Outward seams (arrayutils ArrayGetNItems / ArrayCheckBounds, lsyscache
get_typlenbyvalalign, typcache lookup_element_eq_opr / lookup_element_cmp_proc,
fmgr element_eq / element_cmp / element_hash, format-type, detoast detoast_attr)
are thin marshal+delegate; each justified by a real unported owner. No
computation in seam paths. `array_replace_internal` and `width_bucket_array`
drive the same `lookup_element_*` + `element_*` pair `array_eq`/`array_cmp`
already use — no new seam surface introduced.

## 3. Design conformance

No invented opacity: element values cross seams as the real `ArrayElementDatum`
(ByValue/ByRef), never an opaque handle. The `search`/`replace`/`operand`
parameters of `array_replace`/`array_remove`/`width_bucket_array` were lifted
from bare `Datum` to `ArrayElementDatum` so a by-reference operand carries its
on-disk byte window (opacity-inherited, not introduced); these are
crate-internal functions with no external callers, so the signature change is
contained. Allocating functions take `Mcx` and return `PgResult`. No shared
statics, no locks across `?`. Conformant.

## 4. Notes / deferral ledger

The `SEAMED` verdicts above are all loud panics on a genuinely-unported
*callee* (permitted: "panicking on an unported callee is fine, absent logic is
not"). Each is reachable only when its neighbor lands:

1. **array_map** — executor `ExecEvalExpr` seam + the exprstate/econtext args
   the scaffold dropped.
2. ~~array_create_iterator / array_iterate / array_free_iterator~~ — **now
   ported** (2026-06-13, re-audit below). The `ArrayIteratorData` struct is
   private to arrayfuncs.c (array.h exposes only the opaque `ArrayIterator`
   typedef), so it lives in the porting crate, not types-array; no invented
   opacity. All three verdicts are now MATCH.
3. **construct_empty_expanded_array / array_get_element_expanded /
   array_set_element_expanded** — the expanded-array subsystem
   (`array_expanded.c`: `expand_array`, `ExpandedArrayHeader`, `DatumGetEOHP`).
4. **array_unnest_support** — the planner support-request vocabulary
   (`SupportRequestRows`, `estimate_expression_value`, `estimate_array_length`).
5. By-reference element bytes (array_fill varlena fill, the by-ref arms of
   width_bucket / array_replace rebuild) route through the **detoast** seam,
   which panics until the detoast owner lands — the crate-wide by-reference
   element boundary, identical to construct.rs.

One modeling note (pre-existing, crate-wide, not introduced here): the
`element_eq` seam returns `PgResult<bool>` and does not separately surface the
operator's `isnull` flag; `array_replace_internal` therefore treats a
false/absent result as "no match", exactly as the audited-MATCH `array_eq`
does. The shared equality-operator contract makes this consistent.

ArrayType header layout verified against `array.h`: `MAXDIM == 6`,
`ARRAYTYPE_HDRSZ == 16` (`vl_len_`, `ndim`, `dataoffset`, `elemtype`, each 4
bytes, in that order), `ARR_OVERHEAD_NONULLS/WITHNULLS` reproduced in
foundation. Matches.
