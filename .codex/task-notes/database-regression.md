Goal:
Fix the `database` regression diff around CREATE/ALTER DATABASE options,
database ownership reassignment, and `makeaclitem`.

Key decisions:
- Parse and execute the subset of CREATE DATABASE options used by the
  regression file.
- Add ALTER DATABASE support for rename, tablespace changes, connection limit,
  and owner changes.
- Reassign `pg_database.datdba` during REASSIGN OWNED.
- Add `makeaclitem(oid, oid, text, bool)` as a builtin scalar function.
- Add a narrow :HACK: for oversized `pg_database.datacl` updates until pgrust
  bootstraps shared-catalog toast storage.

Files touched:
- `src/backend/parser/gram.pest`
- `src/backend/parser/gram.rs`
- `src/include/nodes/parsenodes.rs`
- `src/pgrust/database/commands/database_cmds.rs`
- `src/pgrust/database/commands/role.rs`
- catalog, executor, session, and focused parser test support files

Tests run:
- `CARGO_TARGET_DIR=/tmp/pgrust-cape-town-regress-target2 cargo check --quiet`
- `CARGO_TARGET_DIR=/tmp/pgrust-cape-town-regress-target2 cargo test --lib --quiet parse_create_database`
- `CARGO_TARGET_DIR=/tmp/pgrust-cape-town-regress-target2 cargo test --lib --quiet parse_alter_database_statement`
- `scripts/run_regression.sh database` equivalent via the local harness output
  directory `/tmp/pgrust_regress_database_cape_town_4`; result: `database PASS`
  with 16/16 matched queries.

Remaining:
- Replace the shared-catalog toast compatibility shim once shared catalog toast
  tables are bootstrapped and wired into tuple update storage.
