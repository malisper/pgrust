Goal:
Remove eager database-open relcache/relfile and sequence loading.

Key decisions:
Normal open no longer calls open_relfiles_for_store. SequenceRuntime starts empty for durable databases and loads persistent sequence JSON by relation OID on first use.

Files touched:
src/pgrust/cluster.rs
src/pgrust/database/sequences.rs
sequence call sites in executor and DDL/ALTER/TRUNCATE paths

Tests run:
cargo fmt --all -- --check
scripts/cargo_isolated.sh check --message-format short
cluster_bootstraps_multiple_databases_and_connection_rules
create_database_clones_template1_and_persists_across_reopen
durable_prepared_transaction_survives_reopen_then_finishes
sequence
identity_sequence_options_drive_owned_sequence
plpgsql

Remaining:
Broad identity filter still has unrelated auto-updatable view rewrite service failure.
