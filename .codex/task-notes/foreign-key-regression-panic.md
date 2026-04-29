Goal:
Diagnose why `scripts/run_regression.sh --test foreign_key` errors.

Key decisions:
Ran focused regression only. Re-ran with `RUST_BACKTRACE=1` after the first panic.
The error is a server panic, not just an expected-output mismatch.

Files touched:
None for product code.

Tests run:
`scripts/run_regression.sh --test foreign_key --timeout 120 --results-dir /tmp/pgrust-foreign-key-regression`
`RUST_BACKTRACE=1 scripts/run_regression.sh --test foreign_key --timeout 120 --skip-build --results-dir /tmp/pgrust-foreign-key-regression-bt`

Remaining:
Panic occurs at `src/backend/executor/nodes.rs:7819` while evaluating a partition key for:
`UPDATE fk_partitioned_fk SET a = a + 1 WHERE a = 2501;`

Backtrace path:
`TupleSlot::slot_getsomeattrs` -> `eval_expr` -> `src/backend/commands/partition.rs::key_values` ->
`get_partition_for_tuple` -> `find_partition_child` -> `exec_find_partition` ->
`route_updated_partition_row` -> `write_updated_row`.

Likely cause:
The partition-routing row for the update has only 2 visible values, but the
partition key expression expects the root partition descriptor width including
a dropped column (`fk_partitioned_fk` has `b`, dropped `fdrop1`, `a`). A virtual
slot built from the compact row is asked for 3 attributes and panics.

Concrete repro:
`.context/foreign_key_partition_panic_repro.sql` reproduces the same panic
without any foreign keys:

```sql
CREATE TABLE repro_root (b int, dropped_col int, a int)
  PARTITION BY RANGE (a, b);
ALTER TABLE repro_root DROP COLUMN dropped_col;
CREATE TABLE repro_low (a int, b int);
ALTER TABLE repro_root ATTACH PARTITION repro_low
  FOR VALUES FROM (0, 0) TO (10, 10);
CREATE TABLE repro_high (a int, b int);
ALTER TABLE repro_root ATTACH PARTITION repro_high
  FOR VALUES FROM (10, 10) TO (20, 20);
INSERT INTO repro_root (a, b) VALUES (1, 1);
UPDATE repro_root SET a = 11 WHERE a = 1;
```

This proves the foreign key regression is only the place that reaches the bug.
The root issue is partition update routing across dropped-column layouts.
