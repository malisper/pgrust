# FINDING: ts_rewrite sort-tie order diverges from PostgreSQL (pgrust-bug)

**STATUS: FIXED** (rewritten by task #135, final/laneaf-indexproxy; the
lane's step-E fix was a per-crate sort_template port, which the pg_qsort
canon forbids). Shipped fix = INDEX PROXY in `qtn_sort`
(crates/backend/utils/adt/tsquery_core/src/util.rs) over the canonical
`crates/_support/pg_qsort::pg_qsort_arg`: C permutes an array of QTNode
pointers, the proxy permutes u32 handles under the same comparator
decisions and applies the permutation; a comparator error (stack-depth
guard) aborts the sort and propagates as Err. `tie_order_probe` is a live
test asserting C==Rust over the full 116-shape grid; both witnesses live
in the corpus `fuzz/corpus/tsqrw_diff/` (replay green). Crate-side
witnesses: tests.rs pins both docker-adjudicated outputs byte-exact plus
sorted-multiset and proxy-exactness gates across the sort_template
regimes. `collect_values`' stable sort stays: its ties are byte-identical
strings, non-surface (argued at io.rs, verified against
tsquery_op.c:322-339).

Lane: p1-laneaf (adt/tsquery_core + adt/tsquery_rewrite), step-D audit
prompted by p1-laneae's qsort tie-order findings in the tsvector family.
Date: 2026-07-31.

## Statement

Shipped `adt_tsquery_core::util::qtn_sort` (util.rs:106-118) sorts QTNode
children with Rust's **stable** `slice::sort_by`. Real PostgreSQL's QTNSort
(tsquery_util.c:176) sorts with `qsort`, which port.h:478 maps to
**pg_qsort** — unstable for arrays of >= 7 elements (below that,
sort_template.h's insertion-sort path is stable, and its presorted
pre-check exits early on already-ordered input, so small/ordered inputs
mask the class).

QTNodeCompare (== the Rust `qtnode_compare`) does NOT compare operand
weight/prefix — only type/oper/nchild/children/distance/valcrc/word bytes —
so two nodes with the SAME lexeme but DIFFERENT weights (`a:A` vs `a:B`)
are tie-equal while being image- and semantics-distinct. When an AND/OR
node has >= 7 children (after QTNTernary flattening) containing such a
pair, `ts_rewrite` (oid 3684) emits the pair in different positions on the
two implementations.

## Witness (docker postgres:18.3 adjudicated, 2026-07-31)

```
SELECT ts_rewrite('b | c | d | a:A | e | f | a:B'::tsquery,
                  'q'::tsquery, 'r'::tsquery);
-- PostgreSQL 18.3:  'a':B | 'e' | 'c' | 'b' | 'f' | 'a':A | 'd'
-- pgrust:           'a':A | 'e' | 'c' | 'b' | 'f' | 'a':B | 'd'
```

The vendored oracle (csrc/tsq/qsort.c = verbatim sort_template.h
instantiation, wired via the shim qsort macro) reproduces PostgreSQL's
output byte-for-byte; swapping the input pair swaps both PG's and the
oracle's output accordingly (second witness file). 116 of the step-D probe
grid's shapes diverge (`tie_order_probe`, #[ignore] in
fuzz/core/src/tsqrw_diff.rs).

## Why the earlier 300k/1M-exec campaigns missed it

Until step D the oracle ran the build host's libc qsort (glibc qsort is a
stable mergesort; macOS came out tie-compatible on the shapes exercised),
so the C side accidentally agreed with Rust's stable sort — the wrong
oracle masked the class exactly as p1-laneae predicted. The pg_qsort vendor
closes that hole from this commit onward.

## Scope of the surface

- Reached through `ts_rewrite(tsquery, tsquery, tsquery)` (oid 3684) and
  the 2-arg SPI form (3685, out of phase-1 scope) — the only in-tree
  QTNSort callers. Plain tsqueryin/out/cmp/mcontains do not sort children.
- Requires an AND/OR node with >= 7 children carrying tie-equal but
  payload-distinct operands (same lexeme; different weight bitmap or
  prefix flag), in a non-presorted child order.
- tsq_mcontains' value sort (tsquery_op.c:322,325 vs io.rs collect_values)
  ties only on fully identical strings — the kept representative is
  byte-identical either way; not scalar-visible. cleanup.rs / parse.rs
  contain no sorts.

## Disposition (CLOSED by task #135, final/laneaf-indexproxy)

The C-parity fix makes `qtn_sort` reproduce pg_qsort's tie decisions
through the CANONICAL crate (crates/_support/pg_qsort::pg_qsort_arg, index
proxy — the lane's per-crate sort_template port was dropped per the
pg_qsort canon: one shared max-speed crate, never new per-crate copies).

- The two witness inputs live in fuzz/corpus/tsqrw_diff/ (replay green).
- `tie_order_probe` is a live test (passes iff parity holds);
  `tie_case_detail` stays #[ignore] as a manual decode aid.
- Crate-side gates (tsquery_core/src/tests.rs): both witnesses pinned
  byte-exact to the docker-adjudicated PG 18.3 outputs; sorted-multiset +
  proxy-exactness fixtures across the n<7 / 7..40 / >40 regimes; 54001
  error-propagation contract.
