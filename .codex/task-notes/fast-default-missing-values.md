Goal:
Fix fast-default missing-value behavior for volatile, timestamp/date expression, random(named args), and domain defaults.

Key decisions:
Stored missing values are catalog state for old physical tuples only; INSERT defaults use current column/type defaults.
Volatile ADD COLUMN defaults rewrite existing heap rows, clear prior missing values, and swap the heap relfilenode as part of the ADD COLUMN catalog mutation.
Stable ADD COLUMN defaults evaluate once into attmissingval/atthasmissing. Domain defaults are validated but not copied into pg_attrdef when no explicit column default exists.
Array missing values are serialized through text in attmissingval as a compatibility shim because anyarray storage cannot encode array elements today.

Files touched:
pg_attribute catalog row shape/codecs, catalog relation-desc loaders/caches, executor missing-column hydration, insert default binding, ALTER ADD COLUMN execution, ALTER TYPE missing-value preservation, date/timestamp coercion, named random() argument binding, char/varchar typmod parsing.

Tests run:
cargo fmt
CARGO_TARGET_DIR=/tmp/pgrust-target-little-rock-v2-check2 scripts/cargo_isolated.sh check
CARGO_TARGET_DIR=/tmp/pgrust-target-little-rock-v2-check2 scripts/run_regression.sh --test fast_default --timeout 240 --jobs 1 --port 56564 --results-dir /tmp/pgrust-regress-fast-default-little-rock-7

Remaining:
fast_default still has unrelated diffs for generated columns/tableoid, EXPLAIN output shape, and duplicate ALTER TYPE rewrite notices. Requested fast-default missing-value/default sections now match.
