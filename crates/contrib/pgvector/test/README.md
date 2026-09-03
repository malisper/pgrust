# pgvector regression harness

`pgvector-regress.sh` runs upstream pgvector's regression tests
(`test/sql/*.sql`, expected output in `test/expected/*.out`, pinned to
`v0.8.5`, the version this port tracks) against a running pgrust server and
diffs the output the way `pg_regress` would (psql error-line prefixes
stripped, cwd = the upstream `test/` directory).

    crates/contrib/pgvector/test/pgvector-regress.sh -h /tmp -p 5440          # every upstream test but the skip list
    crates/contrib/pgvector/test/pgvector-regress.sh -h /tmp -p 5440 halfvec  # one test

`bit`, `hnsw_bit` and `ivfflat_*` are skipped: pgrust does not ship those
opclasses / that access method. With no arguments the script otherwise runs
every test under upstream's `test/sql/`, whether or not this port has caught
up to it. On the current tree (which carries the vector, halfvec and
sparsevec types, HNSW for all three, and the halfvec<->sparsevec casts) an
argument-less run passes all nine selected tests: `vector_type`,
`hnsw_vector`, `halfvec`, `hnsw_halfvec`, `sparsevec`, `hnsw_sparsevec`,
`btree`, `cast` and `copy` (the last three exercise both halfvec and
sparsevec together, including the cast pair). Pass explicit test names (as
above) to check a subset.

On FAIL the script leaves that test's diff (and upstream's raw output) in a
`mktemp -d` directory, printed in the failure line, for inspection.

This harness runs the unmodified upstream suite against a running server; the
trimmed copies under `crates/contrib/pgvector/sql/` are separate, in-repo
smoke tests. Set `PGVECTOR_SRC` to reuse an existing checkout, `PGVECTOR_REF`
to test another tag.
