/*
 * SHIM HEADER (regexp_diff oracle, p1-laneag) — NOT vendored PostgreSQL.
 *
 * Minimal subset of src/include/mb/pg_wchar.h for the vendored regex engine
 * and the pg_regexp_io.c wrapper oracle.  Declarations only match the real
 * header's shapes; the definitions live in pg_regexfam_glue.c (the UTF-8
 * conversion routines there are VERBATIM from src/common/wchar.c).
 *
 * The database encoding is PINNED to PG_UTF8 for this oracle (both sides of
 * the differential pin it; documented in pg_regexp_io.c header), so
 * GetDatabaseEncoding() is a constant and the encoding-table dispatch of the
 * real mbutils.c is resolved to the UTF-8 routines at pin time.
 */
#ifndef PG_REGEXFAM_PG_WCHAR_H
#define PG_REGEXFAM_PG_WCHAR_H

#include "postgres.h"

/*
 * SYMBOL ISOLATION (the merge/p1-wave1 duplicate-globals lesson, see the
 * main build.rs header): other lane oracles vendor their own copies of the
 * same mb helpers (pg_like_io.c, pg_name_io.c use `static`; this family
 * needs cross-TU linkage between glue/engine/wrapper), so every extern glue
 * symbol carries a pg_regexfam_ prefix via these defines.  The vendored
 * engine files stay VERBATIM — they see the renamed declarations through
 * this shim header only.
 */
#define GetDatabaseEncoding pg_regexfam_GetDatabaseEncoding
#define pg_database_encoding_max_length pg_regexfam_database_encoding_max_length
#define pg_mb2wchar_with_len pg_regexfam_mb2wchar_with_len
#define pg_wchar2mb_with_len pg_regexfam_wchar2mb_with_len
#define pg_mblen pg_regexfam_mblen
#define pg_mblen_range pg_regexfam_mblen_range
#define pg_mbstrlen_with_len pg_regexfam_mbstrlen_with_len
#define pg_ascii_toupper pg_regexfam_ascii_toupper
#define pg_ascii_tolower pg_regexfam_ascii_tolower
#define pg_char_and_wchar_strncmp pg_regexfam_char_and_wchar_strncmp

typedef unsigned int pg_wchar;

/* real values from the real pg_wchar.h (enum pg_enc) */
#define PG_SQL_ASCII 0
#define PG_UTF8 6

extern int	GetDatabaseEncoding(void);
extern int	pg_database_encoding_max_length(void);

/* VERBATIM cores in pg_regexfam_glue.c (from src/common/wchar.c) */
extern int	pg_regexfam_utf2wchar_with_len(const unsigned char *from, pg_wchar *to, int len);
extern int	pg_regexfam_wchar2utf_with_len(const pg_wchar *from, unsigned char *to, int len);
extern int	pg_regexfam_utf_mblen(const unsigned char *s);

/* mbutils.c-shaped entry points (UTF-8-pinned dispatch; see glue file) */
extern int	pg_mb2wchar_with_len(const char *from, pg_wchar *to, int len);
extern int	pg_wchar2mb_with_len(const pg_wchar *from, char *to, int len);
extern int	pg_mblen(const char *mbstr);
extern int	pg_mblen_range(const char *mbstr, const char *end);
extern int	pg_mbstrlen_with_len(const char *mbstr, int limit);

/* src/port/pgstrcasecmp.c ascii case helpers (VERBATIM in glue file) */
extern unsigned char pg_ascii_toupper(unsigned char ch);
extern unsigned char pg_ascii_tolower(unsigned char ch);

/* src/backend/utils/mb/wstrncmp.c (VERBATIM in glue; regc_locale.c uses it
 * for [[:class:]] and [[.element.]] name comparison) */
extern int	pg_char_and_wchar_strncmp(const char *s1, const pg_wchar *s2, size_t n);

#endif							/* PG_REGEXFAM_PG_WCHAR_H */
