Goal:
Tighten C14 namespace, GUC, random, and misc regression behavior.

Key decisions:
Fixed namespace by evaluating expression-index attstattarget -1 during ANALYZE while only persisting expression-index stats for positive targets.
Fixed GUC by normalizing quoted function-local SET values and supporting pg_settings_get_flags in value execution.
Kept misc legacy failures out of this slice: first remaining mismatches are SELECT INTO TABLE, COPY BINARY, legacy PostQUEL function expansion, and C regresslib helpers.

Files touched:
src/backend/commands/analyze.rs
src/backend/executor/exec_expr.rs
src/backend/executor/expr_agg_support.rs
src/backend/executor/srf.rs
src/backend/utils/misc/guc.rs

Tests run:
namespace regression: pass
guc regression: pass
random regression: pass
misc regression: fail, 34/61 matched
scripts/cargo_isolated.sh check: pass with existing unreachable-pattern warnings
targeted unit normalizes_quoted_function_guc_values: pass

Remaining:
misc still needs larger SQL/C compatibility work outside this slice.
