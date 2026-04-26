Goal:
Make the upstream hash_func regression pass while keeping PostgreSQL-compatible
SQL behavior except exact hash digest bits.

Key decisions:
Added shared HashValue/HashValueExtended builtins keyed by HashFunctionKind.
Catalog hash rows now use PostgreSQL names/OIDs where practical, with one
central :HACK: documenting the digest-value exception.
Standard SQL hash uses the low 32 bits of the seed-0 extended hash.
Integer-to-bit casts now use two's-complement bit semantics.
VALUES common-type handling preserves array wrappers and recognizes typed
unknown string literals for hash_func inputs.
CI fix keeps nested array literal inference scalar at each dimension and lets
integer min-value casts apply unary minus before narrowing.

Files touched:
src/include/nodes/primnodes.rs
src/include/catalog/pg_proc.rs
src/backend/parser/analyze/functions.rs
src/backend/parser/analyze/infer.rs
src/backend/parser/analyze/expr/func.rs
src/backend/parser/analyze/coerce.rs
src/backend/parser/analyze/scope.rs
src/backend/parser/analyze/expr.rs
src/backend/executor/exec_expr.rs
src/backend/executor/expr_casts.rs
src/backend/executor/expr_ops.rs
src/backend/access/hash/support.rs

Tests run:
cargo fmt
cargo build --bin pgrust_server -q
scripts/cargo_isolated.sh check
scripts/cargo_isolated.sh test --lib --quiet hash
scripts/cargo_isolated.sh test --lib --quiet bit
scripts/cargo_isolated.sh test --lib --quiet values
scripts/cargo_isolated.sh test --lib --quiet pg_proc
scripts/run_regression.sh --test hash_func --schedule .context/hash_func_schedule --port 55433 --skip-build
git diff --check
scripts/cargo_isolated.sh test --lib --quiet array
scripts/cargo_isolated.sh test --lib --quiet integer
individual CI failure filters for array append/cat/position/nested constructors
and min-int modulo/gcd/lcm

Remaining:
Full default hash_func harness still tries to build the post-create_index base
because hash_func follows create_index in the upstream schedule; create_index
has unrelated existing dependency failures in this workspace. The focused
one-test schedule avoids that and hash_func passes 43/43.
