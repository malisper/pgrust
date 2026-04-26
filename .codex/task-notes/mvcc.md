Goal:
Fix and explain the mvcc regression diff around pg_relation_size, PL/pgSQL RAISE, and aborted subtransaction index growth.

Key decisions:
Use PostgreSQL-compatible behavior in three narrow places: resolve text relation names in pg_relation_size, execute transaction-local DO blocks with the active catalog/context, and add a temporary PL/pgSQL exception-block subtransaction shim for anonymous DO blocks.
Prune btree leaf items whose heap tuple xmin is aborted before inserting into the leaf, so repeated aborted inserts do not force index growth.

Files touched:
src/backend/access/nbtree/nbtree.rs
src/backend/executor/exec_expr.rs
src/pgrust/session.rs
src/pgrust/toast_tests.rs
src/pl/plpgsql/ast.rs
src/pl/plpgsql/compile.rs
src/pl/plpgsql/exec.rs
src/pl/plpgsql/gram.pest
src/pl/plpgsql/gram.rs
src/pl/plpgsql/mod.rs

Tests run:
cargo test --lib --quiet condition_raise
cargo test --lib --quiet pg_relation_size_reports_empty_and_nonempty_toast_relations
scripts/run_regression.sh --test mvcc

Remaining:
The PL/pgSQL exception-block subtransaction support is intentionally marked :HACK: because pgrust still lacks full nested transaction ownership semantics.
