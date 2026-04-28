Goal:
Implement PostgreSQL-style ALTER TABLE inheritance propagation for the regression-covered ALTER paths.

Key decisions:
Added a command-layer work queue modeled after PostgreSQL ATPrepCmd/ATSimpleRecursion instead of adding a separate planner abstraction. Recursive DROP COLUMN now tracks which queued parents actually dropped the column so descendants are not desynchronized when an intermediate child keeps a localized definition.

Files touched:
src/pgrust/database/commands/alter_table_work_queue.rs
src/pgrust/database/commands/mod.rs
src/pgrust/database/commands/maintenance.rs
src/pgrust/database/commands/rename.rs
src/pgrust/database/commands/constraint.rs
src/pgrust/database/commands/drop_column.rs
src/backend/catalog/store/heap.rs
src/pgrust/database.rs
src/pgrust/database_tests.rs

Tests run:
scripts/cargo_isolated.sh check
scripts/cargo_isolated.sh test --lib --quiet inheritance
scripts/cargo_isolated.sh test --lib --quiet alter_table_add_column
scripts/cargo_isolated.sh test --lib --quiet alter_table_rename_constraint
scripts/cargo_isolated.sh test --lib --quiet alter_table_drop_column
scripts/cargo_isolated.sh test --lib --quiet alter_table_alter_column_type
scripts/run_regression.sh --test alter_table --results-dir /tmp/diffs --port 55543

Remaining:
The alter_table regression still times out overall: 1065/1683 queries matched, 618 mismatched, output in /tmp/diffs. The checked inheritance rename/add/drop sections now match; remaining nearby diff is relation-qualified missing-column error wording.
