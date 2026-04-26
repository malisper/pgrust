Goal:
Fix the early actionable failures in pasted `subselect.out` regression diff.

Key decisions:
Implemented parser support for extra-parenthesized set-operation inputs,
row-valued `IN`/`NOT IN`, row-valued scalar subquery comparison, set-op
unknown-literal coercion, grouped type-name casts like `float8(count(*))`, and
mixed int/float executor comparisons. Left full-file `subselect.out` completion
out of scope.

Files touched:
Parser/analyzer/executor/optimizer/rewrite files for subquery and set-op
handling, plus focused parser/executor tests.

Tests run:
`cargo fmt`
`scripts/cargo_isolated.sh check`
Focused tests for parenthesized set ops, row-valued subqueries, set-op literal
coercion, grouped casts, and mixed numeric comparison.
Attempted `scripts/run_regression.sh --test subselect`; blocked while building
the `post_create_index` base dependency at `create_index`.

Remaining:
`subselect` regression still needs a successful dependency setup before the
full diff can be inspected. Later failures in the pasted diff include planner
performance/EXPLAIN shape gaps, view support, `DISTINCT ON`, `ALTER FUNCTION`,
and `LIMIT null`.
