Goal:
Fix subselect regression crash around lateral VALUES in IN and related correlated sublinks.
Key decisions:
PostgreSQL builds partition pruning from usable partition-key clauses and ignores unsupported clauses such as sublinks. pgrust was storing the whole base filter as the runtime partition-prune expression, so setrefs tried to lower a correlated sublink inside pruning and left a semantic Var in executable scalar context. Strip sublink-containing conjuncts from partition-prune filters; the full predicate still runs on the scan path.
Files touched:
crates/pgrust_optimizer/src/path/allpaths.rs; src/pgrust/database_tests.rs
Tests run:
scripts/cargo_isolated.sh test --lib --quiet subselect_regress_; scripts/run_regression.sh --test subselect --timeout 120; scripts/cargo_isolated.sh check
Remaining:
subselect regression completes with no errored/timed-out tests, but still has existing plan/output mismatches. The lateral VALUES query now produces a plan instead of crashing, though it remains a plan-shape mismatch versus PostgreSQL.
