/*
 * SHIM mb/pg_wchar.h — NOT PostgreSQL code. (tsq oracle family)
 *
 * The pg_mblen family with the database encoding PINNED to UTF-8 (the
 * pgrust fuzz environment's database encoding). Bodies in pg_tsq_shim.c
 * are the verbatim upstream control flow (mbutils.c pg_mblen_cstr /
 * _with_len / _unbounded / _range) over the verbatim pg_utf_mblen, with
 * report_invalid_encoding_db -> ereport(ERROR,
 * ERRCODE_CHARACTER_NOT_IN_REPERTOIRE) ("invalid byte sequence").
 */
#ifndef PG_DIFFFUZZ_TSQ_SHIM_PG_WCHAR_H
#define PG_DIFFFUZZ_TSQ_SHIM_PG_WCHAR_H

#include "postgres.h"

extern int	pg_mblen_cstr(const char *mbstr);
extern int	pg_mblen_with_len(const char *mbstr, int limit);
extern int	pg_mblen_unbounded(const char *mbstr);
extern int	pg_mblen_range(const char *mbstr, const char *end);
extern int	pg_mblen(const char *mbstr);

extern bool pg_tsq_verify_mbstr_utf8(const char *mbstr, int len); /* driver use */

/* mbutils.c pg_database_encoding_max_length with the encoding pinned UTF-8 */
static inline int
pg_database_encoding_max_length(void)
{
	return 4;					/* pg_wchar_table[PG_UTF8].maxmblen */
}

#endif
