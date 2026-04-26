Goal:
Diagnose diffs from numeric.diff pasted at .context/attachments/pasted_text_2026-04-25_21-49-08.txt.

Key decisions:
First hunk is row order only for numeric Inf/NaN division query with no ORDER BY; values match.
PostgreSQL emits rows grouped by x2 after WHERE x2 != 0, while pgrust emits left-to-right cross join order grouped by x1.
The likely planner delta is in src/backend/optimizer/path/costsize.rs:
build_join_paths_with_root treats this as a physical Cross join with a pushed-down base filter on v2, but allow_base_cross_swap currently excludes Values paths.
PostgreSQL considers both nested-loop orientations for clauseless joins, and the filtered v2 side is cheaper as the outer side.
Allowing base Cross swaps for Values paths should let the cheaper filtered side become outer while the existing swapped-join setrefs logic preserves logical output column order.
Implemented by allowing VALUES-backed Cross swaps, preserving Cross kind for swapped candidates, and adding VALUES-backed cross join rescan costing.
Second hunk is an error text mismatch for numeric generate_series step infinity.

Files touched:
.codex/task-notes/numeric-diff.md

Tests run:
cargo fmt
cargo test --lib --quiet format_exec_error_renders_generate_series_step_infinity
cargo test --lib --quiet generate_series_rejects_non_finite_numeric_bounds
cargo test --lib --quiet cross_values_join
cargo test --lib --quiet swapped_join_candidate_keeps_logical_pathtarget_order
cargo test --lib --quiet cte_filtered_values_self_join_matches_postgres_outer_order
CARGO_TARGET_DIR=.context/cargo-target-numeric scripts/run_regression.sh --test numeric --skip-build --port 55453 --timeout 180

Remaining:
None for the pasted numeric.diff issues.
