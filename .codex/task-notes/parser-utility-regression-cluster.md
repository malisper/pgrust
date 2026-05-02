Goal:
Close first-wave PostgreSQL parser/utility regression failures for cluster, vacuum,
security_label, limit, misc, and sqljson from 2026-05-01T2044Z.

Key decisions:
- Added parser support for CLUSTER index ON table, CLUSTER table, bare CLUSTER,
  OFFSET before LIMIT, FETCH FIRST/NEXT, LIMIT ALL, SECURITY LABEL provider
  errors, SELECT ... INTO TABLE with non-star target lists, and legacy COPY
  BINARY relation syntax.
- Kept FETCH WITH TIES lowered to the existing LIMIT field; true tie semantics
  and dynamic LIMIT/OFFSET expressions need planner/executor work.
- Kept COPY BINARY through the existing session text shim with :HACK: comments;
  full binary COPY routing remains separate.
- Extended CLUSTER execution enough to reuse previously marked clustered indexes
  and bare CLUSTER marked tables; partitioned/toast-heavy CLUSTER remains broader.

Files touched:
- crates/pgrust_sql_grammar/src/gram.pest
- src/backend/parser/gram.rs
- src/backend/parser/mod.rs
- src/backend/parser/tests.rs
- src/backend/executor/driver.rs
- src/pgrust/database/commands/cluster.rs
- src/pgrust/database/commands/execute.rs
- src/pgrust/database_tests.rs
- src/pgrust/session.rs

Tests run:
- cargo fmt
- scripts/cargo_isolated.sh test --lib --quiet parse_cluster
- scripts/cargo_isolated.sh test --lib --quiet parse_select_with_
- scripts/cargo_isolated.sh test --lib --quiet parse_security_label_as_unsupported_statement
- scripts/cargo_isolated.sh test --lib --quiet parse_copy_binary_prefix_as_binary_format
- scripts/cargo_isolated.sh test --lib --quiet cluster_table_uses_previously_marked_index
- scripts/cargo_isolated.sh test --lib --quiet parse_insert_update_delete
- scripts/cargo_isolated.sh check
- scripts/run_regression.sh --test cluster --results-dir /tmp/pgrust-regress-addis-cluster --timeout 90
- scripts/run_regression.sh --skip-build --test vacuum --results-dir /tmp/pgrust-regress-addis-vacuum --timeout 90
- scripts/run_regression.sh --skip-build --test security_label --results-dir /tmp/pgrust-regress-addis-security_label --timeout 90
- scripts/run_regression.sh --skip-build --test limit --results-dir /tmp/pgrust-regress-addis-limit --timeout 90
- scripts/run_regression.sh --skip-build --test misc --results-dir /tmp/pgrust-regress-addis-misc --timeout 90
- scripts/run_regression.sh --skip-build --test sqljson --results-dir /tmp/pgrust-regress-addis-sqljson --timeout 90
- scripts/run_regression.sh --test misc --results-dir /tmp/pgrust-regress-addis-misc-rerun --timeout 90

Remaining:
- cluster still fails on toast-backed CLUSTER, partitioned CLUSTER semantics, and
  some planner-visible index/explain differences.
- limit still fails on dynamic LIMIT/OFFSET expressions, FETCH WITH TIES cursor
  behavior, and existing EXPLAIN/deparse plan-shape differences.
- misc still fails on regression helper functions such as reverse_name/name and
  composite equipment access; SELECT INTO TABLE and COPY BINARY setup improved.
- sqljson still has broad SQL/JSON deparse, runtime formatting, and error
  position differences; only parser support was kept in scope.
