Goal:
Fix TASK-C15-01 for the `dependency` and `object_address` regression files by
building the bounded object identity/dependency naming foundation needed there.

Key decisions:
- Preserve PostgreSQL-style role-drop dependency DETAIL grouping: privilege
  dependencies first, then owned objects in the catalog object order already
  collected by `owned_objects_for_roles`, then remaining shared dependencies.
- Keep the default-ACL object-address shim narrow. Schema-scoped same-owner
  `ALTER DEFAULT PRIVILEGES ... GRANT` statements still create identity rows,
  matching PostgreSQL's `pg_default_acl` visibility in `object_address`.
- Did not broaden into foreign key, replica identity, identity, or sequence
  semantics. Later work should reuse the role-drop detail assembly helper when
  those features add more dependency names.

Files touched:
- `src/pgrust/database/commands/role.rs`
- `src/pgrust/database/commands/execute.rs`
- `src/pgrust/session.rs`

Tests run:
- `cargo fmt`
- `CARGO_TARGET_DIR=/tmp/pgrust-c15-target scripts/run_regression.sh --test dependency --port <free-port> --results-dir /tmp/pgrust-task-c15-01-dependency`
- `CARGO_TARGET_DIR=/tmp/pgrust-c15-target scripts/run_regression.sh --test object_address --port <free-port> --results-dir /tmp/pgrust-task-c15-01-object-address`
- `CARGO_TARGET_DIR=/tmp/pgrust-c15-target scripts/cargo_isolated.sh check`

Remaining:
- The requested landscape note `.codex/task-notes/regression-failure-landscape-v2.md`
  was not present in this checkout, so the supplied GitHub artifact diffs were
  used as the source of truth.
- Full PostgreSQL `pg_shdepend` storage is still not implemented; this task only
  fixes the supported role-drop detail assembly and default-ACL identity rows.
