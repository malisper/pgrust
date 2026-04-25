Goal:
Fix remaining create_procedure diffs for routine error cursors, pg_get_functiondef procedure body formatting, and SQL procedure body validation.
Key decisions:
Added routine-name position lookup for procedure/function kind errors. Deparse uses $procedure$ for procedures and emits SQL-standard bodies without AS. CREATE PROCEDURE LANGUAGE sql now rejects unquoted CREATE TABLE bodies and nested CALLs to procedures with output args.
Files touched:
src/backend/tcop/postgres.rs, src/backend/executor/exec_expr.rs, src/pgrust/database/commands/create.rs, src/pgrust/session.rs, plus conflict resolution in src/backend/executor/sqlfunc.rs and src/backend/parser/gram.pest.
Tests run:
cargo fmt; cargo check; cargo test --lib --quiet create_procedure; scripts/run_regression.sh --test create_procedure --results-dir /tmp/diffs/create_procedure-procedure-body-fixes-2 --timeout 60 --port 5643.
Remaining:
Focused regression still fails because the workspace includes a larger in-progress procedure merge; current run is 77/125 query matches. The requested buckets improved: caret lines now appear for CALL errors; CREATE TABLE and output-arg CALL body validations now match; SQL-standard procedure bodies execute again. Remaining mismatches include procedure arg mode display, SELECT-on-procedure error path, defaults/named CALL resolution, DROP multi-procedure, and other preexisting procedure merge gaps.
