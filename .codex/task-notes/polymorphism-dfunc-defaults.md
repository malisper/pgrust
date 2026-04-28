Goal:
- Implement CREATE FUNCTION defaults plus named/mixed user-defined function calls for polymorphism dfunc coverage.

Key decisions:
- Store defaults in existing pg_proc pronargdefaults/proargdefaults JSON format.
- Normalize user-defined function calls before binding so omitted and named args share overload resolution.
- Treat defaulted overloads as ambiguous when PostgreSQL does, instead of preferring fewer inserted defaults.
- Preserve builtin named-argument lowering separately.

Files touched:
- src/backend/parser/gram.rs
- src/backend/parser/analyze/functions.rs
- src/backend/parser/analyze/expr.rs
- src/backend/parser/analyze/expr/func.rs
- src/backend/parser/analyze/scope.rs
- src/pgrust/database/commands/create.rs
- src/pgrust/database/commands/drop.rs
- src/backend/executor/sqlfunc.rs
- src/backend/executor/exec_expr.rs
- parser/database tests

Tests run:
- scripts/cargo_isolated.sh check
- scripts/cargo_isolated.sh test --lib --quiet parse_create_function_statement_with_default_args
- scripts/cargo_isolated.sh test --lib --quiet parse_drop_function_statement_with_signature
- scripts/cargo_isolated.sh test --lib --quiet create_function_defaults_and_named_calls_work
- scripts/cargo_isolated.sh test --lib --quiet create_or_replace_function_preserves_default_contract
- scripts/run_regression.sh --port 55453 --test polymorphism --results-dir /tmp/diffs/polymorphism-kolkata

Remaining:
- polymorphism still fails overall: 342/455 queries matched, 1174 diff lines.
- Remaining dfunc diffs are mostly schema/display formatting, record-valued SQL-function execution, Date literalization in the lightweight SQL runtime, and some error-message/signature formatting.
