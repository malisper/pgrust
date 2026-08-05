/*
 * SHIM fmgr surface — NOT PostgreSQL code.
 *
 * Lets the SQL-callable bodies in the VERBATIM vendored pg_crc.c
 * (crc32_bytea / crc32c_bytea) compile against a two-field call frame
 * instead of the real fmgr: the bytea argument is (data, len) and
 * PG_RETURN_INT64 returns the C long long directly. Plumbing only — the
 * arithmetic between PG_GETARG and PG_RETURN is untouched vendored code.
 */
#ifndef PG_DIFF_FMGR_SHIM_H
#define PG_DIFF_FMGR_SHIM_H

typedef struct pg_diff_bytea_frame
{
	const void *data;
	size_t		len;
} pg_diff_bytea_frame;

typedef int64 Datum;
typedef pg_diff_bytea_frame bytea;

#define PG_FUNCTION_ARGS pg_diff_bytea_frame *fcinfo
#define PG_GETARG_BYTEA_PP(n) (fcinfo)
#define VARDATA_ANY(x) ((void *) (x)->data)
#define VARSIZE_ANY_EXHDR(x) ((x)->len)
#define PG_RETURN_INT64(x) return (Datum) (x)

#endif
