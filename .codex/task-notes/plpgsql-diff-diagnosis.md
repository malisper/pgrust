Goal:
Diagnose why /tmp/diffs/plpgsql.diff differs from the expected access fixture.

Key decisions:
The diff is a set of PL/pgSQL compatibility gaps, not a single output-format issue.
The cursor/current-of section cascades after CREATE OR REPLACE FUNCTION fails and leaves the previous forc01() body active.
Implemented the first-mismatch fix by allowing unresolved anyarray -> anyarray resolution, then rejecting unresolved PL/pgSQL pseudotype arguments during PL/pgSQL compile.

Files touched:
.codex/task-notes/plpgsql-diff-diagnosis.md
crates/pgrust_analyze/src/functions.rs
src/pgrust/database_tests.rs
src/pl/plpgsql/compile.rs
src/pl/plpgsql/exec.rs

Tests run:
cargo fmt
git diff --check
CARGO_TARGET_DIR="/Volumes/OSCOO PSSD/pgrust/tmp/pgrust-target-buffalo-anyarray" cargo test -p pgrust_analyze --quiet anyarray_result_from_anyarray_pseudotype
CARGO_TARGET_DIR="/Volumes/OSCOO PSSD/pgrust/tmp/pgrust-target-buffalo-anyarray" cargo test --lib --quiet plpgsql_anyarray_pg_statistic_argument_rejected_at_compile
CARGO_TARGET_DIR="/Volumes/OSCOO PSSD/pgrust/tmp/pgrust-target-buffalo-anyarray" cargo check
CARGO_TARGET_DIR="/Volumes/OSCOO PSSD/pgrust/tmp/pgrust-target-buffalo-anyarray" scripts/run_regression.sh --test plpgsql --jobs 1 --timeout 180 --port 55507 --results-dir "/Volumes/OSCOO PSSD/pgrust/tmp/pgrust-plpgsql-anyarray-isolated"

Remaining:
The anyarray first mismatch is fixed. The isolated plpgsql run now reports 2237/2271 matched and
the first remaining hunk is the later RAISE compile-context mismatch. Latest copied diff is
/tmp/diffs/plpgsql.diff.
