Goal:
Fix generated_virtual/generated_stored regression gaps called out by the user:
self tableoid regclass validation, virtual UDT/UDF restrictions, view DEFAULT
rewrite behavior, stored generated function privilege/dependency behavior, and
mixed relation/column GRANT support.

Key decisions:
Bind CREATE TABLE generated expressions with relation-aware scope and a
validation-only self-regclass rewrite for '<relation>'::regclass.
Reject virtual generated columns that expose user-defined result types or raw
user-defined function calls.
Keep SQL function calls visible instead of inlining them so EXECUTE privilege
checks still happen.
Materialize stored generated columns with tableoid metadata, including ALTER
TABLE ADD COLUMN rewrites.
Track direct SELECT target columns so privileges on a virtual generated column do
not require privileges on its base-column expression.
Support mixed relation and column GRANT by splitting relation-wide specs from
column specs during execution.

Files touched:
crates/pgrust_analyze/src/create_table.rs
crates/pgrust_analyze/src/expr/func.rs
crates/pgrust_analyze/src/expr/targets.rs
crates/pgrust_analyze/src/generated.rs
crates/pgrust_analyze/src/lib.rs
crates/pgrust_analyze/src/modify.rs
crates/pgrust_parser/src/gram.rs
crates/pgrust_rewrite/src/mod.rs
src/backend/commands/tablecmds.rs
src/backend/commands/upsert.rs
src/pgrust/database/commands/drop.rs
src/pgrust/database/commands/maintenance.rs
src/pgrust/database/commands/privilege.rs

Tests run:
cargo fmt
scripts/cargo_isolated.sh check
scripts/run_regression.sh --test generated_virtual --results-dir /tmp/pgrust-generated-virtual-fix-4 --port 55523 --timeout 60
scripts/run_regression.sh --test generated_stored --results-dir /tmp/pgrust-generated-stored-fix-5 --port 55543 --timeout 60

Remaining:
Both generated regressions still fail 117/131 overall due pre-existing broader
generated-column gaps such as exact diagnostic location text, constraint rewrite
error wording, FK action validation on generated columns, and some virtual ALTER
TABLE behavior. The user-requested markers are no longer present in the latest
diffs except the virtual UDT diagnostics now intentionally emit the pgrust
"not yet supported" wording for additional UDT shapes.
