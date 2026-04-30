Goal:
- Implement PostgreSQL-compatible DROP behavior needed by the drop_if_exists regression, including missing DROP syntax, IF EXISTS notices, missing schema/type tolerance, and text-search/access-method catalog drops.

Key decisions:
- Added explicit AST/parser variants for DROP TEXT SEARCH, DROP EXTENSION, DROP ACCESS METHOD, and DROP DATABASE FORCE forms.
- Kept existing richer DROP handlers in place and filled missing behavior around their IF EXISTS/error paths.
- Implemented text-search catalog deletion and dependency traversal in the text_search command module.
- Added access-method catalog deletion and dependency checks; extension DROP currently handles PostgreSQL-compatible missing-object behavior but not full CREATE/DROP extension catalog lifecycle.
- Parsed DROP DATABASE FORCE and allowed forced drops past local active-count checks, but did not implement full PostgreSQL session termination semantics.

Files touched:
- Parser/AST/routing: parsenodes.rs, gram.rs, parser tests, session.rs, query_repl.rs, executor driver, command routing.
- Catalog/storage: pg_depend.rs, heap.rs, pg_proc.rs.
- Commands: drop.rs, text_search.rs, tsearch.rs, catalog_drop.rs, and targeted existing DROP command modules.

Tests run:
- cargo fmt
- CARGO_TARGET_DIR=/tmp/pgrust-target-pool/athens-v4/3 scripts/cargo_isolated.sh check
- CARGO_TARGET_DIR=/tmp/pgrust-target-pool/athens-v4/3 scripts/cargo_isolated.sh test --lib --quiet parse_text_search_generic_statements
- CARGO_TARGET_DIR=/tmp/pgrust-target-pool/athens-v4/3 scripts/cargo_isolated.sh test --lib --quiet parse_drop_database_statement
- CARGO_TARGET_DIR=/tmp/pgrust-target-pool/athens-v4/3 scripts/run_regression.sh --port 15450 --test drop_if_exists

Remaining:
- Full PgExtensionRow support and real extension catalog lifecycle.
- Full DROP DATABASE FORCE session tracking/termination semantics.
- Access-method CASCADE deletion of dependent opclasses/opfamilies/relations beyond dependency rejection.
