Goal:
Fix PL/pgSQL leading `SELECT INTO` assignment target parsing for comma-separated targets without spaces.

Key decisions:
- Route the leading-form target text through the existing comma-aware target parser.
- Keep the existing query rewrite shape unchanged; only the assignment target list changed.
- Add a focused function test using `select into x,y ...` to match the regression form.

Files touched:
- `src/pl/plpgsql/compile.rs`
- `src/pgrust/database_tests.rs`

Tests run:
- `cargo fmt`
- `scripts/cargo_isolated.sh test --lib --quiet plpgsql_select_into_leading_form_splits_tight_targets`
- `scripts/cargo_isolated.sh test --lib --quiet plpgsql`
- `scripts/run_regression.sh --test plpgsql --jobs 1 --timeout 360 --port 55434 --results-dir /tmp/diffs/plpgsql-select-into-targets`

Remaining:
- Regression improved to `2178/2271` matched, `1104` diff lines.
- Remaining SELECT INTO gaps are separate SQL statement-form/error-format cases, including `RETURN QUERY ... SELECT INTO`.
