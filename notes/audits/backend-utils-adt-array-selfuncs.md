# Audit: backend-utils-adt-array-selfuncs

**C source:** `src/backend/utils/adt/array_selfuncs.c` (PostgreSQL 18.3, 1192 lines)
**c2rust:** `c2rust-runs/backend-utils-adt-array-more/src/array_selfuncs.rs`
**Port:** `crates/backend-utils-adt-array-selfuncs/src/lib.rs`
**Date:** 2026-06-13
**Verdict:** PASS

## Constants verified against C headers

| Constant | Port value | C source | OK |
|---|---|---|---|
| `OID_ARRAY_OVERLAP_OP` | 2750 | pg_operator.dat:2750 | ✓ |
| `OID_ARRAY_CONTAINS_OP` | 2751 | pg_operator.dat:2755 | ✓ |
| `OID_ARRAY_CONTAINED_OP` | 2752 | pg_operator.dat:2760 | ✓ |
| `STATISTIC_KIND_MCELEM` | 4 | pg_statistic.h:247 | ✓ |
| `STATISTIC_KIND_DECHIST` | 5 | pg_statistic.h:261 | ✓ |
| `ATTSTATSSLOT_VALUES` | 0x01 | lsyscache.h:43 | ✓ |
| `ATTSTATSSLOT_NUMBERS` | 0x02 | lsyscache.h:44 | ✓ |
| `DEFAULT_CONTAIN_SEL` | 0.005 | array_selfuncs.c:30 | ✓ |
| `DEFAULT_OVERLAP_SEL` | 0.01 | array_selfuncs.c:33 | ✓ |
| `EFFORT` | 100 | array_selfuncs.c:853 | ✓ |
| `CLAMP_PROBABILITY` branch order | `<0` then `>1` | selfuncs.h:63 | ✓ |

## Per-function inventory (13 functions)

| C function | C loc | Port loc | Verdict | Notes |
|---|---|---|---|---|
| `scalararraysel_containment` | :80 | `scalararraysel_containment` | MATCH | examine_variable→rel-None punt; const_node_info IsA(Const) punt; null-left→0; cmp_proc validity→-1; `<>` swaps use_or; MCELEM/DECHIST slot fetch (DECHIST only when !use_or); stanullfrac multiply only on stats path; final `<>` invert + clamp. All branches mirrored. |
| `arraycontsel` | :240 | `arraycontsel` | MATCH | get_restriction_variable→default punt; non-Const→default; null-const→0; var-on-right operator commute (CONTAINS↔CONTAINED only); base-element-type equality gate→calc_arraycontsel else default; clamp. |
| `arraycontjoinsel` | :320 | `arraycontjoinsel` | MATCH | stub returning `DEFAULT_SEL(operator)`. |
| `calc_arraycontsel` | :336 | `calc_arraycontsel` | MATCH | cmp_proc validity→default; security check gate; MCELEM slot, DECHIST only when CONTAINED; stanullfrac multiply; no-stats path. DatumGetArrayTypeP + toast pfree → SEAMED into deconstruct_array seam (owner arrayfuncs.c, where C's deconstruct_array lives). |
| `mcelem_array_selec` | :427 | `mcelem_array_selec` | MATCH | deconstruct_array seam; null-collapse in-place compaction; `@> '{...,null}'`→0; qsort_arg by cmp; operator dispatch (CONTAINS/OVERLAP vs CONTAINED); unrecognized-operator→elog(ERROR) string identical. |
| `mcelem_array_contain_overlap_selec` | :520 | `mcelem_array_contain_overlap_selec` | MATCH | nnumbers≠nmcelem+3→numbers=NULL; minfreq=numbers[nmcelem] or 2*DEFAULT_CONTAIN_SEL; use_bsearch heuristic (i64 widening, identical result for stats-bounded counts); CONTAINS init 1.0 / OVERLAP init 0.0; dup-skip via element_compare; bsearch vs linear merge; match→numbers[idx], else Min(DEFAULT_CONTAIN_SEL,minfreq/2); CONTAINS `*=`, OVERLAP inclusion-exclusion; per-iter clamp. |
| `mcelem_array_contained_selec` | :695 | `mcelem_array_contained_selec` | MATCH | punt DEFAULT_CONTAIN_SEL when numbers NULL/wrong-len or hist NULL/nhist<3; minfreq/nullelem_freq/avg_count reads; rest=avg_count, mult=1; elem_selec palloc; parallel scan updating rest/mult; trailing mcelem walk; `mult *= exp(-rest)`; EFFORT reduction (quadratic-formula N, qsort desc); calc_distr×2 + calc_hist; sum `hist_part*mult*dist/mcelem_dist` guarded by `mcelem_dist[i]>0`; `*= (1-nullelem_freq)`; clamp. |
| `calc_hist` | :920 | `calc_hist` | MATCH | hist_part palloc n+1; frac=1/(nhist-1); per-k boundary count loop (`<=` float compare); exact-bound val=(count-1)+0.5/next+0.5/prev; not-a-bound frac/prev or 0. |
| `calc_distr` | :1009 | `calc_distr` | MATCH | two-row alloc; M[0,0]=1; row swap + recurrence `prev[j]*(1-t)+prev[j-1]*t` with `j<i`/`j>0` guards; Poisson convolution when rest>DEFAULT_CONTAIN_SEL (reset row, t=exp(-rest), convolve, t*=rest/(i+1)); returns row, frees prev_row. |
| `floor_log2` | :1088 | `floor_log2` | MATCH | n==0→-1; cascading 16/8/4/2/1 shifts. Unit-tested across boundaries. |
| `find_next_mcelem` | :1129 | `find_next_mcelem` | MATCH | binary search from *index; mid=(l+r)/2; res==0 exact, <0 right, else left; writes *index=l, returns false on miss. |
| `element_compare` | :1164 | `element_compare` | MATCH | `FunctionCall2Coll(cmp_proc_finfo, typcollation, d1, d2)` + DatumGetInt32 → SEAMED to fmgr `function_call2_coll` (owner fmgr.c). The cmp_proc OID / typcollation come from the ElemCmpInfo comparator context. |
| `float_compare_desc` | :1180 | `float_compare_desc` | MATCH | `>`→-1, `<`→1, else 0; descending qsort helper. |

## Comparator-context modeling (design note)

C's `element_compare` reads `typentry->cmp_proc_finfo` and `typentry->typcollation`
off the `TypeCacheEntry *` that `lookup_type_cache(elemtype,
TYPECACHE_CMP_PROC_FINFO)` returns. The repo's `lookup_type_cache` seam view
(`types-typcache::TypeCacheEntry`) surfaces only `type_id/typlen/typbyval/typalign`,
not the comparison finfo or collation. The port reproduces the exact bundle of
fields the cache entry holds as the in-crate `ElemCmpInfo`
(`type_id/typlen/typbyval/typalign/cmp_proc/typcollation`), populated through
the established per-field seams: typcache `lookup_element_cmp_proc` (the cached
`cmp_proc_finfo.fn_oid`, same seam `array_cmp`/`btarraycmp` use),
lsyscache `get_typlenbyvalalign`, lsyscache `get_typcollation`. No invented
opacity — every field is a real `pg_type`/typcache datum resolved by a real seam.
The `OidIsValid(cmp_proc_finfo.fn_oid)` validity check becomes
`OidIsValid(cmp_proc)`, identical predicate.

## Seam audit

This crate owns **no inward seam crate** (no `backend-utils-adt-array-selfuncs-seams`):
array_selfuncs.c's functions are `scalararraysel_containment` (called directly by
selfuncs.c's `scalararraysel`), `arraycontsel`/`arraycontjoinsel` (fmgr entry
points reached via fmgr dispatch). None is a cross-cycle seam INTO this crate,
so `init_seams()` is correctly empty — same shape as
`backend-utils-adt-range-selfuncs`. The seams-init recurrence guard
(`every_seam_installing_crate_is_wired_into_init_all` /
`every_declared_seam_is_installed_by_its_owner`) passes with this crate wired in.

Outward seams (all thin marshal+delegate; each crosses a real unported-owner
boundary, mirror-and-panic until the owner lands):

| Seam | Owner C file | Justification |
|---|---|---|
| `examine_variable` (NEW decl) | selfuncs.c | planner stats access; selfuncs.c unported |
| `const_node_info` (NEW decl) | selfuncs.c / nodes | `IsA(node,Const)` decode of an opaque planner Node* |
| `get_restriction_variable` | selfuncs.c | restriction-clause var recognition |
| `statistic_proc_security_check` | selfuncs.c | leakproof/ACL stats check |
| `stats_tuple_stanullfrac` | selfuncs.c / pg_statistic | GETSTRUCT stanullfrac read |
| `release_variable_stats` | selfuncs.c | ReleaseVariableStats (RAII guard) |
| `get_base_element_type` | lsyscache.c | array element type |
| `get_attstatsslot` | lsyscache.c | MCELEM/DECHIST slot extract |
| `get_typlenbyvalalign` | lsyscache.c | element storage attrs |
| `get_typcollation` | lsyscache.c | element typcollation |
| `lookup_element_cmp_proc` | typcache.c | cached cmp_proc_finfo.fn_oid |
| `deconstruct_array` | arrayfuncs.c | DatumGetArrayTypeP + element split + toast-copy mgmt |
| `function_call2_coll` | fmgr.c | FunctionCall2Coll for element_compare |

The two new declarations (`examine_variable`, `const_node_info`) were added to
the **selfuncs.c-owned** seam crate `backend-utils-adt-selfuncs-seams` (the
owner of `get_restriction_variable` etc.), not to this crate. They are
consumed-outward here; their installer is the future selfuncs.c port. No seam
path contains branching or computation beyond argument/result marshalling.

## Design conformance

- All allocation (`alloc_f32_zeroed`, deconstruct buffers) goes through `Mcx` +
  `PgResult` (fallible). No raw global allocator.
- No invented opacity: `StatsRelNode` (added to `VariableStatData.rel`) and
  `ConstNodeInfo` mirror real planner/Node structures, opacity inherited from
  the unported planner — same pattern as the existing `StatsVarNode`.
- `VariableStatData` released via the `VarStatsGuard` RAII on every `?`/return
  path (C `ReleaseVariableStats`).
- No shared statics, no ambient-global seams, no locks across `?`, no registry
  side tables.
- Zero `todo!()` / `unimplemented!()` / `unreachable!()`. The unrecognized-operator
  arm is a real `elog(ERROR, ...)` with the C's exact message.

## Verdict: PASS

Every function MATCH (or SEAMED per the rules). Zero seam findings, zero design
findings. 12 unit tests cover the pure kernels (floor_log2, float_compare_desc,
clamp, default_sel, calc_hist, calc_distr, both mcelem estimators, the
arraycontjoinsel stub) and pass.
