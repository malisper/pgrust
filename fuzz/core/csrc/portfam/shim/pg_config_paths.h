/*
 * SHIM pg_config_paths.h — NOT PostgreSQL code. The real file is generated
 * by the build from --prefix; these values match the shipped Rust
 * crates/port/pg_path defaults (PGRUST_* env unset, the documented
 * --prefix=/usr/local/pgsql layout) so make_relative_path diffs over the
 * SAME compiled-in constants on both sides.
 */
#ifndef PG_DIFFFUZZ_PORTFAM_SHIM_PG_CONFIG_PATHS_H
#define PG_DIFFFUZZ_PORTFAM_SHIM_PG_CONFIG_PATHS_H

#define PGBINDIR "/usr/local/pgsql/bin"
#define PGSHAREDIR "/usr/local/pgsql/share"
#define SYSCONFDIR "/usr/local/pgsql/etc"
#define INCLUDEDIR "/usr/local/pgsql/include"
#define PKGINCLUDEDIR "/usr/local/pgsql/include"
#define INCLUDEDIRSERVER "/usr/local/pgsql/include/server"
#define LIBDIR "/usr/local/pgsql/lib"
#define PKGLIBDIR "/usr/local/pgsql/lib"
#define LOCALEDIR "/usr/local/pgsql/share/locale"
#define DOCDIR "/usr/local/pgsql/share/doc"
#define HTMLDIR "/usr/local/pgsql/share/doc"
#define MANDIR "/usr/local/pgsql/share/man"

#endif
