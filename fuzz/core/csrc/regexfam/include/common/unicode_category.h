/*
 * SHIM HEADER (regexp_diff oracle, p1-laneag) — NOT vendored PostgreSQL.
 *
 * regc_pg_locale.c calls the pg_u_* classifiers only under
 * PG_REGEX_STRATEGY_BUILTIN, unreachable with collation pinned to
 * C_COLLATION_OID (strategy C).  Declarations satisfy the compiler; the
 * glue file provides abort() stubs so any reach is loud.
 */
#ifndef PG_REGEXFAM_UNICODE_CATEGORY_H
#define PG_REGEXFAM_UNICODE_CATEGORY_H

#include "mb/pg_wchar.h"

/* symbol isolation, as in mb/pg_wchar.h */
#define pg_u_isdigit pg_regexfam_u_isdigit
#define pg_u_isalpha pg_regexfam_u_isalpha
#define pg_u_isalnum pg_regexfam_u_isalnum
#define pg_u_isupper pg_regexfam_u_isupper
#define pg_u_islower pg_regexfam_u_islower
#define pg_u_isgraph pg_regexfam_u_isgraph
#define pg_u_isprint pg_regexfam_u_isprint
#define pg_u_ispunct pg_regexfam_u_ispunct
#define pg_u_isspace pg_regexfam_u_isspace

extern bool pg_u_isdigit(pg_wchar c, bool posix);
extern bool pg_u_isalpha(pg_wchar c);
extern bool pg_u_isalnum(pg_wchar c, bool posix);
extern bool pg_u_isupper(pg_wchar c);
extern bool pg_u_islower(pg_wchar c);
extern bool pg_u_isgraph(pg_wchar c);
extern bool pg_u_isprint(pg_wchar c);
extern bool pg_u_ispunct(pg_wchar c, bool posix);
extern bool pg_u_isspace(pg_wchar c);

#endif							/* PG_REGEXFAM_UNICODE_CATEGORY_H */
