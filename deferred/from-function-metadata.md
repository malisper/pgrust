## Generic `FROM`-Function Metadata

### Problem

`pgrust` supports a few hardcoded functions in `FROM`, such as `generate_series`, `unnest`, and several JSON table functions, but it does not have a general abstraction for a function that behaves like a relation.

That means there is no shared metadata path for:

- output column count
- output column names
- output column types
- nullability
- planner lowering for relation-shaped functions
- executor startup/runtime for arbitrary relation-shaped functions

As a result, new `FROM` functions currently need bespoke binder/planner support even when their runtime behavior is simple.

### Concrete Example

`pg_input_error_info(text, text)` is naturally used in `FROM`, but the current implementation work for `int2.sql` does not justify inventing a full generic SRF framework. The lowest-friction implementation is to lower it to a one-row `Projection` over `Result`, because the engine already knows how to plan and execute that shape.

That works for the regression, but it is a symptom of missing infrastructure: the system cannot yet represent “this builtin function in `FROM` returns these named columns of these types” as first-class metadata.

### Deferred Work

Add a general relation-function layer that can describe and execute `FROM` functions without hardcoding each one in `scope.rs`.

At minimum that should include:

- a metadata descriptor for relation-returning builtins
- binder support that resolves a `FROM` function via metadata instead of name-specific match arms
- a plan representation for generic relation functions, or a principled lowering rule
- executor support for relation-function evaluation with predictable row production
- alias/column-alias handling shared with other `FROM` sources

### Why Deferred

This is broader than the current `int2` regression work. The regression only needs `pg_input_error_info` to behave like a fixed one-row relation with four columns, so a targeted lowering is much cheaper and lower risk than introducing a new planner/executor abstraction mid-fix.
