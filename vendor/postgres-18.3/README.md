# Vendored PostgreSQL 18.3 data files

These files are vendored **verbatim** from PostgreSQL 18.3 (git tag
`REL_18_3`). They let pgrust build, boot, and run the regression / isolation
test suites **without any external PostgreSQL source or build tree** — a fresh
clone plus a Rust toolchain plus a PostgreSQL 18 `psql` client is enough.

## Layout

| Path | Source | Purpose |
|------|--------|---------|
| `share/` | the **built/installed** `share/postgresql/` | Bootstrap + runtime data dir contents: `postgres.bki`, `system_views.sql`, `system_functions.sql`, `system_constraints.sql`, `snowball_create.sql`, `information_schema.sql`, `sql_features.txt`, the `*.conf.sample` files, and the `timezone/`, `timezonesets/`, `tsearch_data/`, `extension/` directories. This is what pgrust's own `--initdb` reads via `-L`. (`postgres.bki` is generated at build time — the *built* copy is vendored.) |
| `regress/sql/`, `regress/expected/` | `src/test/regress/{sql,expected}` | The main regression suite SQL and expected output. |
| `regress/data/` | `src/test/regress/data` | COPY `.data` files (`onek.data`, `tenk.data`, …) loaded by `test_setup.sql` via `:abs_srcdir`. |
| `regress/parallel_schedule` | `src/test/regress/parallel_schedule` | Test ordering for the main suite. |
| `isolation/specs/`, `isolation/expected/`, `isolation/isolation_schedule` | `src/test/isolation/*` | The isolation suite (used by a separate `pg_isolation_regress`-based runner; not yet covered by the clone-only runner). |

## License

These files are part of PostgreSQL and are distributed under the **PostgreSQL
License** (a liberal Open Source license; see the `COPYRIGHT` file in the
upstream PostgreSQL 18.3 / `REL_18_3` source tree, or
<https://www.postgresql.org/about/licence/>). The PostgreSQL License explicitly
permits redistribution, with or without modification, of these files. They are
reproduced here unmodified.

PostgreSQL is Copyright © 1996–2024 The PostgreSQL Global Development Group,
and Copyright © 1994 The Regents of the University of California.

## Refreshing

To re-vendor against a different REL_18_x checkout, re-run the copy steps
documented in the commit that introduced this directory (copy the installed
`share/postgresql/` and `src/test/{regress,isolation}/...`).
