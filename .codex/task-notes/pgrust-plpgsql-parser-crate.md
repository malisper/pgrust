Goal:
Extract root-free PL/pgSQL AST, parser lowering, and validation into a separate pgrust_plpgsql crate.

Key decisions:
- Added crates/pgrust_plpgsql depending on pgrust_plpgsql_grammar, pgrust_parser, pgrust_nodes, and pgrust_catalog_data.
- Moved AST, gram lowering, parse_block, and create-function validation into pgrust_plpgsql.
- Kept root compile/exec in src/pl/plpgsql because they still depend on executor, optimizer, catalog, session, commands, and portal runtime.
- Root ast.rs and gram.rs are :HACK: compatibility shims.

Files touched:
- Cargo.toml
- Cargo.lock
- crates/pgrust_plpgsql/*
- src/pl/plpgsql/{ast.rs,gram.rs,mod.rs}

Tests run:
- cargo fmt --all -- --check
- scripts/cargo_isolated.sh check -p pgrust_plpgsql --message-format short
- scripts/cargo_isolated.sh check --message-format short
- scripts/cargo_isolated.sh test -p pgrust_plpgsql --quiet
- scripts/cargo_isolated.sh test --lib --quiet plpgsql

Remaining:
- Compile and execution still live in root. Moving them needs service traits for executor, catalog lookup, planning, command execution, portals, notices, and GUC handling.
