Goal:
Diagnose and fix the timestamptz regression mismatch where infinite interval
steps reported "infinity" instead of PostgreSQL's "infinite".

Key decisions:
Kept the fix local to the timestamptz generate_series executor path. PostgreSQL
hard-codes "step size cannot be infinite" for timestamp and timestamptz interval
steps, while the generic pgrust GenerateSeriesInvalidArg formatter still serves
numeric generate_series argument errors.

Files touched:
src/backend/executor/srf.rs

Tests run:
scripts/cargo_isolated.sh check
scripts/run_regression.sh --test timestamptz --jobs 1

Remaining:
None.
