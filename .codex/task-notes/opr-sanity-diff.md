Goal:
Diagnose pasted opr_sanity regression diff.

Key decisions:
The failures were catalog consistency issues, not executor row-computation issues. Fixed shared pg_proc prosrc rows with incompatible volatility/type contracts, multirange/range operator proc wiring, incomplete pg_amop/operator/hash closures, and opclass/opfamily/amproc metadata mismatches.
CI then exposed that PostgreSQL-shaped multirange GiST amproc rows use shared range support proc OIDs while pgrust keeps separate multirange runtime support code. Kept the catalog rows intact and routed shared range support procs to multirange runtime support when the indexed values are multiranges.

Files touched:
src/include/catalog/pg_proc.rs
src/include/catalog/pg_operator.rs
src/include/catalog/pg_amop.rs
src/include/catalog/pg_amproc.rs
src/include/catalog/pg_opclass.rs
src/include/catalog/pg_opfamily.rs
src/backend/access/gist/support/mod.rs
src/backend/access/gist/support/range_ops.rs

Tests run:
cargo fmt
scripts/cargo_isolated.sh check
scripts/cargo_isolated.sh test --lib --quiet gist_
scripts/cargo_isolated.sh test --lib --quiet pg_amop
scripts/cargo_isolated.sh test --lib --quiet pg_amproc
scripts/cargo_isolated.sh test --lib --quiet pg_opclass
scripts/cargo_isolated.sh test --lib --quiet pg_opfamily
scripts/cargo_isolated.sh test --lib --quiet pg_operator
scripts/cargo_isolated.sh test --lib --quiet pg_proc
scripts/run_regression.sh --test opr_sanity --jobs 1 --timeout 120 --port 55433 --results-dir /tmp/pgrust-opr-sanity-fix-5
CARGO_TARGET_DIR=/tmp/pgrust-target-boston-ci-fix scripts/run_regression.sh --test opr_sanity --jobs 1 --timeout 120 --port 55433 --results-dir /tmp/pgrust-opr-sanity-ci-fix

Remaining:
opr_sanity and the focused GiST CI failures pass.
