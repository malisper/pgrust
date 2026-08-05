# contrib/intarray — coverage record, lane p1-intarray

All numbers below are **LOCAL** (`fuzz/cov-export.sh intarray_diff`, full
replay of the 838 committed seeds, macOS/aarch64, rig sha 15bef10216ef6ab4adbb53f46b2347488a673bfd).
The 10M fleet floor and the two a0 exhaustive sweeps are QUEUED, not run —
see `FLEET-QUEUE.md` at the worktree root.

## Equation

| file | measured | in-scope v2-SLOC |
|------|----------|------------------|
| src/lib.rs      | 375 | 470 |
| src/tool.rs     | 274 | 274 |
| src/boolquery.rs| 299 | 314 |
| **in-scope total** | **948** | **1058** (89.6%) |
| src/gist.rs     | — | 311 (CARVED) |
| src/gistbig.rs  | — | 283 (CARVED) |

1058 = 948 fuzz-measured + 110 residual, every residual line carried by a
row in `residual.tsv` with a class, a C counterpart and a justification.
uncov-not-listed = 0.

## Sufficiency route per function group

| group | route | evidence |
|-------|-------|----------|
| intset / int_to_intset | a0 EXHAUSTIVE-DIFF (full 2^32, whole-image compare) | `intarray_diff::exhaustive::exhaustive_intset_full_i32` — QUEUED |
| hashval / getbit | a0 EXHAUSTIVE-DIFF (full 2^32 x siglen {1,8,252,512,8191}) | `intarray_diff::exhaustive::exhaustive_hashval_full_i32` — QUEUED |
| bqarr_in / bqarr_out / querytree | differential fuzz, 4 planes incl. the escontext soft face | `intarray_diff` arm 0 |
| boolop / rboolop | differential fuzz | arm 1 |
| the 7 set operators + push_array + intset_subtract | differential fuzz | arm 2 |
| icount/sort_asc/sort_desc/uniq/idx/push_elem/del_elem/union_elem/subarray | differential fuzz (full i32 scalars) | arm 3 |
| sort(dir) | differential fuzz | arm 4 |
| signconsistent/execconsistent/gin_bool_consistent/query_has_required_values/gensign/hash_into | differential fuzz over exported C symbols | arm 6 |
| internal_size | differential fuzz | arm 7 |
| gin seam cores (int4_extract_query, int4_consistent) | CARVED (`_int_gin.c` not compiled) — re-open candidate | residual.tsv |
| gist.rs / gistbig.rs | CARVED (excluded state) | residual.tsv |

## Plane liveness

`fuzz/corpus/INJECTION-intarray.md`: 10 planted C-side defects, 10/10
flagged, control clean. Includes two soft-plane injections (witness counter
removed; `ereturn` forced to always hard-raise), so the second-side plane is
demonstrated to compare C against Rust rather than Rust against Rust.

## Harness defect found and fixed in this lane

**DEAD DEPTH GUARD (twice).** `stack_depth::check_stack_depth()` is
C-faithfully inert while `STACK_BASE_PTR == 0`, and that base is a
THREAD-LOCAL. The rig initially never called `set_stack_base()`, so a
3000-deep nested `query_int` overflowed the real thread stack instead of
raising 54001; the first fix armed it behind the global `OnceLock`, which
armed only the first thread to get there. Now armed per thread with a 1 MiB
budget matched on both sides. Both were found by a DIRECTED deep-nesting
seed — no exec-count floor would have surfaced either (the
exec-floors-never-witness-boundaries law, in its guard-vacuity form).
