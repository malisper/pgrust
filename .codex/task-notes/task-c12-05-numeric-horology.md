Goal:
- TASK-C12-05: fix numeric and horology coercion/error-surface regressions from run 25203427750 at commit 01357db61d7a1b865a1cbbb9ca9552e3049fffd1.
- Owned regression files: numeric, horology.

Key decisions:
- Treat decimal/numeric operands to `^` as PostgreSQL's numeric power path instead of the float8 operator; integer-only power remains float8-compatible.
- Bind two-argument `log()` to numeric because PostgreSQL exposes the binary logarithm overload as `log(numeric, numeric)`.
- Preserve timestamp shorthand comparison behavior so unknown `'now'` is coerced as a timestamp when compared with `timestamp without time zone 'tomorrow'`.
- Add a narrow exact numeric division fast path for scale-0 values when the divisor is an exact power of ten and the division is exact. This avoids the huge bigint division case introduced by numeric power while retaining PostgreSQL-style division scale.
- `sample` on the slow variance tail showed pgrust repeatedly decoded stored numerics by reparsing rendered decimal text, while PostgreSQL stores numeric values in a binary varlena representation. pgrust heap/internal numeric storage now uses a binary `BigInt`/scale/dscale format with fallback decoding for legacy text payloads.
- After binary numeric storage, the isolated variance-tail bench improved from about 1167ms/query to about 268ms/query in the dev build. The remaining hot path is BigInt multiplication in numeric variance aggregation.

Files touched:
- `src/bin/numeric_query_bench.rs`
- `src/backend/executor/exec_tuples.rs`
- `src/backend/parser/analyze/expr.rs`
- `src/backend/parser/analyze/infer.rs`
- `src/backend/parser/analyze/expr/func.rs`
- `src/backend/executor/expr_ops.rs`
- `src/backend/executor/tests.rs`
- `src/backend/executor/value_io.rs`

Tests run:
- `cargo fmt`
- `scripts/cargo_isolated.sh check` with isolated `/tmp` target settings: passed with existing unreachable-pattern warnings.
- `scripts/cargo_isolated.sh check --features tools --bin numeric_query_bench`: passed with existing unreachable-pattern warnings.
- `scripts/cargo_isolated.sh test --lib --quiet numeric_power_decimal_literals_use_numeric_overload`: passed.
- `scripts/cargo_isolated.sh test --lib --quiet binary_log_uses_numeric_overload`: passed.
- `scripts/cargo_isolated.sh test --lib --quiet timestamp_shorthand_unknown_now_comparison_uses_timestamp_type`: passed.
- `scripts/cargo_isolated.sh test --lib --quiet numeric_divides_by_large_power_of_ten_without_full_bigint_division`: passed.
- `scripts/cargo_isolated.sh test --lib --quiet numeric_storage_uses_binary_format_with_text_fallback`: passed.
- `scripts/run_regression.sh --test horology --port 65436 --results-dir /tmp/pgrust-task-c12-05-horology`: passed, 399/399 queries matched.
- `scripts/run_regression.sh --test numeric --port 65442 --results-dir /tmp/pgrust-task-c12-05-numeric`: local default 60s file timeout near the final variance/GCD/LCM tail after 1040/1057 queries; the prior operator/log/format mismatches were gone.
- `scripts/run_regression.sh --test numeric --port 65444 --timeout 300 --results-dir /tmp/pgrust-task-c12-05-numeric-extended`: passed, 1057/1057 queries matched.
- `sample` before numeric storage change: `/tmp/pgrust-num-variance.sample.txt`; isolated bench `/tmp/pgrust-target-c12-05/debug/numeric_query_bench --dir /tmp/pgrust-num-variance-bench --variant variance-huge --iterations 40` averaged about 1167ms/query.
- `sample` after numeric storage change: `/tmp/pgrust-num-variance-after.sample.txt`; isolated bench `/tmp/pgrust-target-c12-05/debug/numeric_query_bench --dir /tmp/pgrust-num-variance-bench-after-sample --variant variance-huge --iterations 80` averaged about 268ms/query.
- `scripts/run_regression.sh --test numeric --port 65446 --results-dir /tmp/pgrust-task-c12-05-numeric-fast`: passed, 1057/1057 queries matched under the default file timeout.
- `scripts/run_regression.sh --test horology --port 65448 --results-dir /tmp/pgrust-task-c12-05-horology-fast`: passed, 399/399 queries matched.

Remaining:
- No semantic mismatches remain in numeric or horology under the focused reruns.
- The default numeric regression now passes locally. Further speedups would likely require changing numeric variance aggregation away from repeated huge `BigInt` multiplication, closer to PostgreSQL's purpose-built `NumericAggState`/`NumericSumAccum` representation.
