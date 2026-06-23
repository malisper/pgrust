# pg_stat_statements extension control + SQL scripts

These are the `CREATE EXTENSION pg_stat_statements` control file and SQL install
scripts. pgrust resolves the extension directory from
`PGRUST_PGSHAREDIR=/tmp/pgrust_share` (the same path the `pg_prewarm` lane uses),
so these files must be installed into `/tmp/pgrust_share/extension/`:

    cp crates/contrib-pg-stat-statements/extension/pg_stat_statements.control \
       crates/contrib-pg-stat-statements/extension/pg_stat_statements--*.sql \
       /tmp/pgrust_share/extension/

## Files

* `pg_stat_statements.control` — `default_version = '1.12'`.
* `pg_stat_statements--1.12.sql` — **squashed base install script** (a pgrust
  packaging divergence): it creates the final 1.12 objects directly. Upstream
  ships a 1.4 base + a 1.4→…→1.12 upgrade chain, but the chain's intermediate
  steps use `ALTER EXTENSION … ADD|DROP`, which routes to pgrust's unported
  `process_utility_slow` arm. The single squashed script produces the identical
  1.12 catalog state, and the C function bodies it references are byte-faithful.
* `pg_stat_statements--1.4.sql` … `--1.11--1.12.sql` — the verbatim upstream
  base + upgrade scripts, kept for `ALTER EXTENSION … UPDATE` once the
  ALTER-EXTENSION utility arm lands (they are unused by a fresh
  `CREATE EXTENSION` while `default_version = '1.12'` selects the squashed base).

## Loading

`pg_stat_statements` must be preloaded to set up its shared memory:

    shared_preload_libraries = 'pg_stat_statements'
    compute_query_id = on

The module's C body is the in-process ported library `contrib-pg-stat-statements`
(registered via `init_seams`); the `LANGUAGE C AS 'MODULE_PATHNAME', '<sym>'`
functions resolve through the dynamic-loader builtin-library registry (the Rust
backend exposes no C ABI), exactly like `pg_prewarm`.
