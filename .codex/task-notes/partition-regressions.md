Goal:
Fix the requested partitioning regression slice: partitioned key/index attach metadata, replica identity indexes, range partition opclasses, inherited DROP NOT NULL propagation, temp partition/inherit ON COMMIT behavior, and generated-column partition compatibility.

Key decisions:
Use PostgreSQL parent/child catalog semantics where possible: inherited constraint fields, partitioned index attachment metadata, pg_partitioned_table partclass, and temp ON COMMIT actions over inheritance trees. Keep remaining generated-column SET EXPRESSION work narrow by rewriting stored generated rows for physical relations through the existing ALTER TYPE rewrite helpers.

Files touched:
Parser grammar/AST/analyze paths for ON COMMIT and generated partition column overrides; partition lowering/attach validation; catalog store updates for replica identity, not-null state, generated expression metadata, and partitioned key validity; temp ON COMMIT handling; opclass operator resolution; psql describe/deparse/regclass binding helpers; focused parser and database tests.

Tests run:
scripts/cargo_isolated.sh check
scripts/run_regression.sh --test replica_identity --jobs 1 --timeout 120
scripts/run_regression.sh --test temp --jobs 1 --timeout 120
scripts/run_regression.sh --test alter_table --jobs 1 --timeout 180
scripts/run_regression.sh --test generated_stored --jobs 1 --timeout 180
scripts/run_regression.sh --test generated_virtual --jobs 1 --timeout 180
Earlier targeted runs: inherit, parser tests, and focused database tests for generated overrides, attach validation, opclasses, temp ON COMMIT, inherited DROP NOT NULL, replica identity helpers, and regclass IN-list binding.

Remaining:
replica_identity passes. temp, alter_table, generated_stored, generated_virtual, and inherit still have unrelated or formatting-only diffs outside the requested partitioning slice; copied current useful diffs under /tmp/diffs.
