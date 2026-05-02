Goal:
Finish PostgreSQL cluster regression support and clustered-index metadata parity.

Key decisions:
The current checkout already had CLUSTER syntax, physical rewrite, partitioned
CLUSTER, toast CLUSTER, and pg_index.indisclustered support. The remaining
cluster regression mismatch was DROP USER treating session temp schemas as
role-owned dependencies. PostgreSQL does not block role drop on pg_temp_* or
pg_toast_temp_* schemas in this scenario, so role dependency collection now
ignores those namespace rows.

Files touched:
src/pgrust/database/commands/role.rs

Tests run:
cargo fmt
CARGO_TARGET_DIR=/tmp/pgrust-target-port-louis-cluster cargo test --lib --quiet drop_role_ignores_owned_temp_schemas
CARGO_TARGET_DIR=/tmp/pgrust-target-port-louis-cluster scripts/run_regression.sh --test cluster --jobs 1 --timeout 180 --port 55451 --results-dir /tmp/pgrust-regress-port-louis-cluster-final

Remaining:
cluster regression passes locally: 204/204 query blocks matched.
