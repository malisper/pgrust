/*
 * SHIM HEADER (regexp_diff oracle, p1-laneag) — NOT vendored PostgreSQL.
 *
 * regc_pg_locale.c calls unicode_uppercase_simple/unicode_lowercase_simple
 * only under PG_REGEX_STRATEGY_BUILTIN, which is unreachable with collation
 * pinned to C_COLLATION_OID (strategy C).  Declarations satisfy the
 * compiler; the glue file provides abort() stubs so any reach is loud.
 */
#ifndef PG_REGEXFAM_UNICODE_CASE_H
#define PG_REGEXFAM_UNICODE_CASE_H

#include "mb/pg_wchar.h"

/* symbol isolation, as in mb/pg_wchar.h */
#define unicode_uppercase_simple pg_regexfam_unicode_uppercase_simple
#define unicode_lowercase_simple pg_regexfam_unicode_lowercase_simple

extern pg_wchar unicode_uppercase_simple(pg_wchar code);
extern pg_wchar unicode_lowercase_simple(pg_wchar code);

#endif							/* PG_REGEXFAM_UNICODE_CASE_H */
