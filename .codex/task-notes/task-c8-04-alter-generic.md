Goal:
Triage `alter_generic` out of C8 protocol/session state and fix it if the failure is narrowly generic-object DDL/catalog behavior.

Key decisions:
The focused regression diff showed only three SQL-visible error text mismatches for `ALTER AGGREGATE` applied to a normal function. PostgreSQL prefixes this wrong-object-type message with `function`; pgrust did not. This is routine/generic ALTER DDL error behavior, not protocol/session routing.

Files touched:
`src/pgrust/database/commands/routine.rs`

Tests run:
`RUSTC_WRAPPER= CARGO_TARGET_DIR='/Volumes/OSCOO PSSD/rust/cargo-target-gwangju-v4-alter-generic' scripts/run_regression.sh --test alter_generic --port 56432 --jobs 1 --results-dir /tmp/pgrust-task-c8-04-alter-generic`
`RUSTC_WRAPPER=/usr/bin/env CARGO_TARGET_DIR='/Volumes/OSCOO PSSD/rust/cargo-target-gwangju-v4-alter-generic' scripts/cargo_isolated.sh check`

Remaining:
Nothing for `alter_generic`. This should be owned by the generic ALTER routine/DDL cluster, not C8 protocol lifecycle work.
