## Context

The catalog work is in good shape, but full `cargo test --lib --quiet` is still
blocked by unrelated executor stack overflows in
[src/backend/executor/tests.rs](src/backend/executor/tests.rs:2311):

- `float_math_builtins_cover_common_operations`
- `float_math_domain_errors_are_explicit`
- `abs_builtin_supports_smallint_filters`

These failures are outside the catalog/parity slices and appear to be executor
behavior bugs rather than metadata issues.

## Goal

Fix the recursive or runaway executor paths behind those stack overflows so the
full library test suite can run cleanly again.

## Likely Approaches

- trace the builtin dispatch path in `exec_expr.rs` and the math helpers in
  `expr_math.rs`
- check whether the float/math failures and the `abs(...)` failure share the
  same recursion bug or only the same symptom
- add focused regression tests once the underlying recursion is fixed

## Why Deferred

These are real bugs, but they are not part of the core catalog milestone. They
should be handled as a separate executor-stability pass.
