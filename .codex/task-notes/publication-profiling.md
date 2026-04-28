Goal:
Use macOS `sample` to identify why the publication regression is slow and remove the hottest avoidable catalog-cache rebuilds.

Key decisions:
The initial active worker sample was dominated by publication DDL and auth paths constructing the full backend catcache.
Publication CREATE/ALTER/DROP/COMMENT now use targeted syscache helpers for roles, memberships, namespaces, publications, publication_rel, and publication_namespace rows.
The first optimization pass used a generic all-row syscache helper, but that is not how PostgreSQL structures these paths.
The current version removes the all-row syscache helper. It keeps keyed syscache/list lookups for hot paths, uses explicit auth-only catalog scans while the pgrust `AuthCatalog` API still needs all auth rows, scans `pg_publication_namespace` by `pnpubid` through the publication namespace index like PostgreSQL's `GetPublicationSchemas`, and scans `pg_type` directly when an all-row type list is required.
Broad `LazyCatalogLookup` publication row methods are back on the existing backend catcache; narrow type oid/name lookups still use syscache.

Files touched:
src/backend/utils/cache/lsyscache.rs
src/backend/utils/cache/syscache.rs
src/pgrust/database.rs
src/pgrust/database/commands/publication.rs

Tests run:
cargo fmt
scripts/cargo_isolated.sh check
scripts/cargo_isolated.sh test --lib --quiet current_schema_publications_preserve_quoted_schema_and_table_names
scripts/cargo_isolated.sh test --lib --quiet alter_publication_owner_to
scripts/cargo_isolated.sh test --lib --quiet publication_column_list_rejects_schema_publication_mix
scripts/cargo_isolated.sh test --lib --quiet create_index
scripts/cargo_isolated.sh build --bin pgrust_server
scripts/run_regression.sh --skip-build --port 55502 --schedule .context/publication-only.schedule --test publication --timeout 180 --results-dir /tmp/diffs/publication-after-type-syscache-nosample2
scripts/run_regression.sh --skip-build --port 55507 --schedule .context/publication-only.schedule --test publication --timeout 180 --results-dir /tmp/diffs/publication-pg-like
scripts/run_regression.sh --skip-build --port 55511 --schedule .context/publication-only.schedule --test publication --timeout 180 --results-dir /tmp/diffs/publication-pg-like-type-scan

Remaining:
The publication regression still has expected output diffs: 660/710 queries matched, 399 diff lines.
The latest pg-like run had the same diff content as `/tmp/diffs/publication-after-type-syscache-nosample2` except for generated output path headers.
The standard publication schedule was not useful locally because unrelated create_index base setup can fail before publication starts.
The latest completed publication-only timing was noisy under concurrent machine load: real 142.95, user 68.07, sys 5.90. A prior run of the same optimized server reached real 78.27, user 46.39, sys 4.53 but failed while writing diff/status files due a transient /tmp no-space error.
The latest pg-like publication-only run completed normally with the same 660/710 matched query summary.
The remaining sampled backend_catcache cost is generic DDL cleanup, especially DROP TABLE statistics cleanup; that is the next likely optimization target.
