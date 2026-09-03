# pgvector regression harness

`pgvector-regress.sh` runs upstream pgvector's regression tests
(`test/sql/*.sql`, expected output in `test/expected/*.out`, pinned to
`v0.8.5`, the version this port tracks) against a running pgrust server and
diffs the output the way `pg_regress` would (psql error-line prefixes
stripped, cwd = the upstream `test/` directory).

    crates/contrib/pgvector/test/pgvector-regress.sh -h /tmp -p 5440          # all shipped features
    crates/contrib/pgvector/test/pgvector-regress.sh -h /tmp -p 5440 halfvec  # one test

`bit`, `hnsw_bit` and `ivfflat_*` are skipped: pgrust does not ship those
opclasses / that access method. Set `PGVECTOR_SRC` to reuse an existing
checkout, `PGVECTOR_REF` to test another tag.
