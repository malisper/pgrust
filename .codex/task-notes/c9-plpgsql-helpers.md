Goal:
Make PL/pgSQL helper functions used by C9 regressions create and execute far enough to unblock later `explain` regression work.

Key decisions:
- Added PostgreSQL-style `CONTINUE WHEN <condition>` support as a conditional loop-control statement.
- Reused existing PL/pgSQL condition compilation/evaluation so regex and equality conditions behave like `EXIT WHEN`.
- Stopped short of EXPLAIN planner/output changes; remaining `explain` failures are option support, output formatting, JSON/YAML/XML formatting, window plan output, and deparse/planner details.

Files touched:
- `crates/pgrust_plpgsql_grammar/src/gram.pest`
- `src/pl/plpgsql/ast.rs`
- `src/pl/plpgsql/gram.rs`
- `src/pl/plpgsql/compile.rs`
- `src/pl/plpgsql/exec.rs`
- `src/pl/plpgsql/mod.rs`
- `src/backend/executor/tests.rs`

Tests run:
- `cargo fmt`
- `scripts/cargo_isolated.sh test --lib --quiet parse_continue`
- `scripts/cargo_isolated.sh test --lib --quiet plpgsql_continue_when_regex_skips_remaining_loop_body`
- `scripts/run_regression.sh --test plpgsql --port 55491 --results-dir /tmp/pgrust-c9-plpgsql`
- `scripts/run_regression.sh --test explain --port 55493 --results-dir /tmp/pgrust-c9-explain`
- `scripts/cargo_isolated.sh check`

Remaining:
- `plpgsql` still has 38 mismatched queries, mostly existing error/context text, cursor-variable FOR-loop behavior, composite return casting, and transition-table coverage.
- `explain_filter` and `explain_filter_to_json` now create and run. Remaining `explain` failures are C2/C3/C9.4 follow-ups rather than PL/pgSQL helper creation/runtime, except that pgrust's unindented text `Buffers:` output is not filtered by PostgreSQL's helper pattern.
