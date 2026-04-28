Goal:
Fix PL/pgSQL regression formatting diffs for context line numbers and message fields.

Key decisions:
- Carry parser statement line numbers into compiled PL/pgSQL statements with a transparent wrapper.
- Preserve RAISE and PL/pgSQL warning DETAIL/HINT fields through notice serialization.
- Resolve RAISE USING ERRCODE condition names through the existing PL/pgSQL condition map.
- Implement runtime fields for too_many_rows and strict_multi_assignment extra checks; leave shadowed_variables and cursor/stacked-diagnostics NOTICE misses for semantic follow-up work.

Files touched:
- `src/pl/plpgsql/ast.rs`
- `src/pl/plpgsql/gram.rs`
- `src/pl/plpgsql/compile.rs`
- `src/pl/plpgsql/exec.rs`
- `src/pl/plpgsql/mod.rs`
- `src/backend/libpq/pqformat.rs`
- `src/backend/tcop/postgres.rs`

Tests run:
- `cargo fmt`
- `scripts/cargo_isolated.sh test --lib --quiet plpgsql`
- `scripts/cargo_isolated.sh check`
- `git diff --check`
- `scripts/run_regression.sh --test plpgsql --jobs 1 --timeout 360 --port 55479 --results-dir /tmp/diffs/plpgsql-formatting-final-360 --skip-build`

Remaining:
- Final PL/pgSQL regression still fails: `2107/2271` queries matched, `2019` diff lines.
- Remaining counted formatting-like lines: `-HINT 8`, `+HINT 3`, `-DETAIL 8`, `+DETAIL 4`, `-WARNING 12`, `-NOTICE 101`, `+NOTICE 21`.
- Remaining WARNING misses are mostly compile-time `shadowed_variables`.
- Remaining NOTICE/context misses are mostly unsupported cursor loops, stacked diagnostics, dynamic SQL/cursor behavior, and broader PL/pgSQL semantics.
