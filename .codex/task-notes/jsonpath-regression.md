Goal:
Diagnose PostgreSQL regression `jsonpath`.

Key decisions:
Ran only `jsonpath` via `scripts/run_regression.sh --test jsonpath --timeout 120 --jobs 1 --skip-build --port 55433 --results-dir /tmp/pgrust_regress_jsonpath`.
Default port 5433 was already occupied, so reran on 55433.
Failures are mostly jsonpath parser/input and canonical renderer mismatches, not SQL executor row behavior.

Files touched:
`src/backend/executor/jsonpath.rs`
`src/backend/executor/tests.rs`
Added this task note.

Tests run:
`scripts/run_regression.sh --test jsonpath --timeout 120 --jobs 1 --skip-build --port 55433 --results-dir /tmp/pgrust_regress_jsonpath`
Result: FAIL, 51/224 queries matched, 173 mismatched, 1448 diff lines.
`scripts/cargo_isolated.sh test --lib --quiet jsonpath_postfix_access_after_expression_work`
Result: pass.
`scripts/run_regression.sh --test jsonpath --timeout 120 --jobs 1 --port 55434 --results-dir /tmp/pgrust_regress_jsonpath_postfix`
Result: FAIL, 58/224 queries matched, 166 mismatched, 1393 diff lines.
`scripts/cargo_isolated.sh test --lib --quiet jsonpath_numeric`
Result: pass.
`scripts/cargo_isolated.sh check`
Result: pass.
`scripts/run_regression.sh --test jsonpath --timeout 120 --jobs 1 --port 55436 --results-dir /tmp/pgrust_regress_jsonpath_numeric2`
Result: FAIL, 118/224 queries matched, 106 mismatched, 884 diff lines.

Remaining:
Primary code area is `src/backend/executor/jsonpath.rs`.
Compare against upstream `../postgres/src/backend/utils/adt/jsonpath_scan.l`, `jsonpath_gram.y`, and `jsonpath.c`.
Original failure clusters:
- Query-level mismatch counts:
  - 46 postfix member/path after scalar or parenthesized expr unsupported.
  - 35 numeric lexer missing supported forms.
  - 27 numeric error surface differs.
  - 26 filter/predicate canonical rendering.
  - 7 variable canonical rendering.
  - 7 LIKE_REGEX canonical flag/rendering.
  - 5 operator parentheses/precedence rendering.
  - 4 numeric canonical rendering.
  - 4 string escape parsing incomplete.
  - 3 LIKE_REGEX error surface differs.
  - 2 list/method argument spacing rendering.
  - 2 missing last-context validation.
  - 2 numeric lexer accepts invalid leading-zero form.
  - 1 recursive wildcard canonical rendering.
  - 1 LIKE_REGEX parsing/flag validation.
  - 1 missing root-current (@) validation.
After the postfix-access fix, regression improved to 58/224 matched and 166
mismatched. The explicit parenthesized/member examples like `($).a.b`,
`($.a.b).c.d`, `($.a.b + -$.x.y).c.d`, `1.2.e`, and `(1.2).e3` now match.
The remaining formerly-postfix-looking failures are mostly numeric lexer gaps
around exponent/trailing-dot forms, plus canonical rendering precedence for a
few now-accepted expressions.
After the numeric lexer fix, regression improved to 118/224 matched and 106
mismatched. Query-level remaining groups:
  - 66 filter/predicate canonical rendering.
  - 10 operator parentheses/precedence rendering.
  - 7 variable canonical rendering.
  - 7 LIKE_REGEX canonical flag/rendering.
  - 5 unexpected parse rejection, mostly string escape forms.
  - 3 list/method argument spacing rendering.
  - 3 LIKE_REGEX error surface differs.
  - 2 missing last-context validation.
  - 1 recursive wildcard canonical rendering.
  - 1 other canonical rendering.
  - 1 missing root-current (@) validation.
- Canonical output formatting/parentheses/spacing/variable quoting/recursive wildcard printing.
- Escape handling does not accept PostgreSQL jsonpath escapes like `\v`, `\xNN`, `\u{...}`, and unrecognized escapes as literal chars.
- Numeric lexer is incomplete: no exponent, leading-dot/trailing-dot numbers, nondecimal prefixes, or underscore separators; also accepts some invalid leading-zero forms differently.
- Context validation missing for `last` outside array subscripts and `@` in root expressions.
- Postfix path access on parenthesized expressions is unsupported.
- LIKE_REGEX flag normalization/errors differ, including `smixq`, unknown flags, and regex validation error surfacing.
- Error reporting goes through generic `InvalidStorageValue`/`XX000` in `pg_input_error_info` instead of PostgreSQL jsonpath SQLSTATE/detail surface.
