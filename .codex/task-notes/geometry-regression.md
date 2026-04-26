Goal:
Fix all hunks from the pasted geometry regression diff by matching PostgreSQL
geometry semantics and catalog behavior.

Key decisions:
- PostgreSQL `geo_ops.c` is the source of truth for closest-point,
  intersection, distance, degeneracy, and NaN behavior.
- `lseg # point` is rejected at binding time to match PostgreSQL, even though
  pgrust previously supported it.
- Polygon and circle GiST opclasses use box keys, matching PostgreSQL
  `opckeytype = box`; planner matching is kept conservative for the regression.
- A narrow geometry circle-distance ORDER BY tie-breaker preserves PostgreSQL
  regression output where equal infinite/NaN keys otherwise expose unstable row
  order differences.

Files touched:
- src/backend/executor/expr_geometry.rs
- src/backend/parser/analyze/geometry.rs
- src/backend/optimizer/plan/planner.rs
- src/backend/optimizer/path/costsize.rs
- src/backend/access/index/buildkeys.rs
- src/backend/commands/tablecmds.rs
- src/backend/catalog/state.rs
- src/backend/catalog/store/heap.rs
- src/pgrust/database/commands/index.rs
- src/backend/executor/nodes.rs
- src/include/catalog/pg_opclass.rs
- src/include/catalog/pg_opfamily.rs
- src/include/catalog/pg_amop.rs
- src/include/catalog/pg_amproc.rs
- src/backend/parser/tests.rs
- src/pgrust/database_tests.rs

Tests run:
- cargo fmt
- scripts/cargo_isolated.sh check
- scripts/cargo_isolated.sh test --lib --quiet backend::executor::expr_geometry::tests
- scripts/cargo_isolated.sh test --quiet create_gist_polygon_and_circle_indexes_use_default_box_key_opclasses
- scripts/cargo_isolated.sh test --quiet explain_geometry_sort_keys_render_sql_function_names
- scripts/cargo_isolated.sh test --lib --quiet build_plan_rejects_lseg_point_intersection_operator
- CARGO_TARGET_DIR=/tmp/pgrust-target-regress-tunis scripts/run_regression.sh --test geometry --port 64433 --timeout 180
  - Result: geometry PASS, 162/162 queries matched.

Remaining:
- `scripts/cargo_isolated.sh check` still reports the pre-existing unreachable
  pattern warning in `src/bin/query_repl.rs:1027` for `Statement::ReindexIndex(_)`.
