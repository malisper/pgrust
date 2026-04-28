Goal:
Fix EXPLAIN-only differences in the `partition_prune` regression after rebasing onto `foreign-key-regression-2`.

Key decisions:
Kept the fixes limited to EXPLAIN rendering/formatting and did not mask real pruning, parser, DML, PREPARE/EXECUTE, or operator-support gaps. Hidden single-child and nested Append display noise is flattened only for EXPLAIN output. Filter deparse now follows PostgreSQL-style ordering for null checks, simple range predicates, equality/scalar-array predicates, and function-wrapped range predicates. Type/collation rendering now covers bool predicates, bpchar/text scalar-array output, non-default POSIX collation, and bigint/numeric typed literals.

Files touched:
src/backend/commands/explain.rs
src/backend/commands/tablecmds.rs
src/backend/executor/nodes.rs

Tests run:
cargo fmt
scripts/cargo_isolated.sh check
scripts/cargo_isolated.sh test --lib --quiet partition_prune
scripts/run_regression.sh --test partition_prune --jobs 1 --results-dir /tmp/amsterdam_partition_prune_after_explain_final

Remaining:
Latest regression run: 563/750 queries matched, 3009 diff lines. The remaining failures are mostly real plan/pruning differences, unsupported PREPARE/EXECUTE and EXPLAIN EXECUTE, unsupported regex scalar-array operators, DML/MERGE/update gaps, enum/record/hash opclass setup cascades, runtime pruning/Subplans Removed gaps, and a few residual expression-rendering cases tied to those plan-shape gaps.
