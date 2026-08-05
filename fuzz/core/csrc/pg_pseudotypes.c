/*
 * Vendored PostgreSQL C: pseudotypes — differential-fuzz oracle.
 *
 * Provenance (all bodies VERBATIM unless a shim is listed below):
 *   - src/backend/utils/adt/pseudotypes.c @ postgres-src
 *     62d6c7d3df6287f1bd83199c1a746e50d31571a0 (Stamp 18.3, the repo's
 *     vendored ground-truth checkout ../pgrust-fabled/vendor/postgres-src):
 *     the four PSEUDOTYPE_DUMMY_* macros + every instantiation line,
 *     cstring_in/cstring_out/cstring_recv/cstring_send,
 *     void_in/void_out/void_recv/void_send, shell_in/shell_out,
 *     pg_node_tree_out/pg_node_tree_send. The anyarray_out-class delegates
 *     (array_out/enum_out/range_out/multirange_out targets) are NOT
 *     vendored: their pgrust counterparts are deliberate unported-delegate
 *     panics, out of this target's scope.
 *   - src/include/utils/elog.h (PGSIXBIT, MAKE_SQLSTATE),
 *     src/backend/utils/errcodes.h: ERRCODE_FEATURE_NOT_SUPPORTED = 0A000,
 *     ERRCODE_CHARACTER_NOT_IN_REPERTOIRE = 22021.
 *   - src/backend/libpq/pqformat.c: pq_getmsgtext / pq_sendtext /
 *     pq_begintypsend / pq_endtypsend — reduced to their identity-encoding
 *     arms with the reduction argued per-function below.
 *   - src/backend/utils/adt/varlena.c: textout -> text_to_cstring,
 *     textsend cores (for the pg_node_tree delegates).
 *   - src/include/varatt.h: SET_VARSIZE 4B form (little-endian arm,
 *     matching this native LE host).
 *
 * This file is the native-build adaptation of the known-good Kani
 * vendoring proofs/pseudotypes/c/pg_pseudotypes.c (same shim arguments,
 * re-verified against Stamp 18.3 — REL_18_STABLE and 18.3 are identical
 * for this file), with two deltas for the fuzz build:
 *   1. ereport -> thread-local sqlstate capture (pg_pseudo_errcode_get /
 *      _reset) instead of out-params, following csrc/pg_float_io.c.
 *   2. cstring_recv models the pg_verify_mbstr arm that the Kani harness
 *      fenced away: pq_getmsgtext -> pg_client_to_server ->
 *      pg_any_to_server with ClientEncoding == DatabaseEncoding ==
 *      SQL_ASCII (the fuzz process's real single-encoding configuration;
 *      the Rust side's seam is pinned to the same identity arm).
 *      For a single-byte encoding pg_verify_mbstr rejects exactly the
 *      embedded NUL byte (wchar.c pg_ascii_verifystr stops at NUL;
 *      report_invalid_encoding -> ERRCODE_CHARACTER_NOT_IN_REPERTOIRE).
 *      High-bit bytes are legal in SQL_ASCII. Empty input short-circuits
 *      to the identity arm with no verification (pg_any_to_server's
 *      s.is_empty() / *s == '\0' early return — mbutils.c).
 *
 * Shims (plumbing only, never logic):
 *   - `pg_` prefix on every function name (macro instantiations included).
 *   - fmgr PG_FUNCTION_ARGS unwrapping -> plain C signatures:
 *     PG_GETARG_CSTRING -> const char* param; PG_RETURN_CSTRING /
 *     PG_RETURN_BYTEA_P -> caller-provided output buffer + returned
 *     length; PG_RETURN_VOID() -> return 0 (Datum 0).
 *   - ereport(ERROR, errcode(X), errmsg(...)) -> record X in the
 *     thread-local and return at the exact ereport program point (C
 *     longjmps there). Message text never crosses the comparator (the
 *     stable unit tests pin the message templates instead).
 *   - palloc-based pstrdup / palloc -> caller buffer; copy-through-NUL
 *     semantics verbatim.
 *   - pq_begintypsend/pq_endtypsend -> caller buffer with VARHDRSZ
 *     reserved header bytes + SET_VARSIZE stamped at end (4B form,
 *     little-endian arm written out explicitly).
 *   - pq_sendtext server->client conversion taken in its identity arm
 *     (single-encoding process, see above); transmission = memcpy.
 */

#include "postgres.h"

/* ---- elog.h, verbatim ---- */
#define PGSIXBIT(ch)	(((ch) - '0') & 0x3F)
#define MAKE_SQLSTATE(ch1,ch2,ch3,ch4,ch5) \
	(PGSIXBIT(ch1) + (PGSIXBIT(ch2) << 6) + (PGSIXBIT(ch3) << 12) + \
	 (PGSIXBIT(ch4) << 18) + (PGSIXBIT(ch5) << 24))

/* ---- errcodes.h (generated from errcodes.txt), verbatim values ---- */
#define ERRCODE_FEATURE_NOT_SUPPORTED MAKE_SQLSTATE('0','A','0','0','0')
#define ERRCODE_CHARACTER_NOT_IN_REPERTOIRE MAKE_SQLSTATE('2','2','0','2','1')

#define VARHDRSZ 4

/*
 * Thread-local sqlstate capture (csrc/pg_float_io.c convention; own
 * variable so the float/geo error classes never alias these).
 * 0 = no error since last reset.
 */
static _Thread_local int32 pg_pseudo_errcode;

int32
pg_pseudo_errcode_get(void)
{
	return pg_pseudo_errcode;
}

void
pg_pseudo_errcode_reset(void)
{
	pg_pseudo_errcode = 0;
}

#define PG_PSEUDO_EREPORT(code) (pg_pseudo_errcode = (code))

/*
 * The four dummy-I/O macros from pseudotypes.c Stamp 18.3, with the
 * ereport rewire applied once. The errcode and errmsg SHAPE (accept vs
 * display) per function kind are exactly the C macro's; message text is
 * out of the fuzz comparator (pinned by stable unit tests instead).
 */
#define PSEUDOTYPE_DUMMY_INPUT_FUNC(typname) \
int \
pg_##typname##_in(void) \
{ \
	/* ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), \
	 *                 errmsg("cannot accept a value of type %s", #typname))); */ \
	PG_PSEUDO_EREPORT(ERRCODE_FEATURE_NOT_SUPPORTED); \
	return 1; \
	/* PG_RETURN_VOID();  unreachable: C longjmps at the ereport */ \
} \
\
extern int no_such_variable

#define PSEUDOTYPE_DUMMY_IO_FUNCS(typname) \
PSEUDOTYPE_DUMMY_INPUT_FUNC(typname); \
\
int \
pg_##typname##_out(void) \
{ \
	/* ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), \
	 *                 errmsg("cannot display a value of type %s", #typname))); */ \
	PG_PSEUDO_EREPORT(ERRCODE_FEATURE_NOT_SUPPORTED); \
	return 1; \
} \
\
extern int no_such_variable

#define PSEUDOTYPE_DUMMY_RECEIVE_FUNC(typname) \
int \
pg_##typname##_recv(void) \
{ \
	/* ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), \
	 *                 errmsg("cannot accept a value of type %s", #typname))); */ \
	PG_PSEUDO_EREPORT(ERRCODE_FEATURE_NOT_SUPPORTED); \
	return 1; \
} \
\
extern int no_such_variable

#define PSEUDOTYPE_DUMMY_BINARY_IO_FUNCS(typname) \
PSEUDOTYPE_DUMMY_RECEIVE_FUNC(typname); \
\
int \
pg_##typname##_send(void) \
{ \
	/* ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), \
	 *                 errmsg("cannot display a value of type %s", #typname))); */ \
	PG_PSEUDO_EREPORT(ERRCODE_FEATURE_NOT_SUPPORTED); \
	return 1; \
} \
\
extern int no_such_variable

/* ---- pstrdup shim: copy-through-NUL into caller buffer, returns strlen ---- */
static int
pg_pseudo_strcpy(char *dst, const char *src)
{
	int			i = 0;

	while ((dst[i] = src[i]) != '\0')
		i++;
	return i;
}

/* ---- varatt.h SET_VARSIZE, 4B form, little-endian arm (native store) ---- */
static void
pg_pseudo_set_varsize_4b(uint8 *ptr, uint32 len)
{
	uint32		header = len << 2;	/* SET_VARSIZE_4B: va_header = len << 2 */

	ptr[0] = (uint8) (header & 0xFF);
	ptr[1] = (uint8) ((header >> 8) & 0xFF);
	ptr[2] = (uint8) ((header >> 16) & 0xFF);
	ptr[3] = (uint8) ((header >> 24) & 0xFF);
}

/*
 * cstring — real I/O functions (pseudotypes.c verbatim modulo the fmgr /
 * pstrdup shims).
 */
int
pg_cstring_in(const char *str, char *out)
{
	/* char *str = PG_GETARG_CSTRING(0); PG_RETURN_CSTRING(pstrdup(str)); */
	return pg_pseudo_strcpy(out, str);
}

int
pg_cstring_out(const char *str, char *out)
{
	/* char *str = PG_GETARG_CSTRING(0); PG_RETURN_CSTRING(pstrdup(str)); */
	return pg_pseudo_strcpy(out, str);
}

int
pg_cstring_recv(const uint8 *payload, int plen, char *out)
{
	/*
	 * cstring_recv (pseudotypes.c):
	 *   StringInfo buf = (StringInfo) PG_GETARG_POINTER(0);
	 *   str = pq_getmsgtext(buf, buf->len - buf->cursor, &nbytes);
	 *   PG_RETURN_CSTRING(str);
	 * pq_getmsgtext (pqformat.c) with rawbytes computed from the SAME
	 * fields it range-checks, so the "insufficient data" ereport is
	 * statically dead here. (payload, plen) models the unread region
	 * &data[cursor], len-cursor.
	 *
	 * pq_getmsgtext then calls pg_client_to_server(str, rawbytes). In the
	 * single-encoding SQL_ASCII configuration (see file header) that is
	 * pg_any_to_server's first arm: pg_verify_mbstr(db_encoding, s, false)
	 * then return the caller's pointer. For a single-byte encoding the
	 * verifier (wchar.c pg_ascii_verifystr via pg_verify_mbstr) accepts
	 * every byte except NUL; an embedded NUL ereports
	 * ERRCODE_CHARACTER_NOT_IN_REPERTOIRE (mbutils.c
	 * report_invalid_encoding). Empty input returns before verification
	 * (mbutils.c: if (len <= 0) return short-circuit).
	 *
	 * Identity arm then: p = palloc(rawbytes + 1); memcpy; p[rawbytes]=0.
	 * palloc -> caller buffer. Returns rawbytes, or -1 with the
	 * thread-local sqlstate set (the ereport point).
	 */
	int			i;

	if (plen > 0)
	{
		for (i = 0; i < plen; i++)
		{
			if (payload[i] == 0)
			{
				PG_PSEUDO_EREPORT(ERRCODE_CHARACTER_NOT_IN_REPERTOIRE);
				return -1;
			}
		}
	}
	for (i = 0; i < plen; i++)
		out[i] = (char) payload[i];
	out[plen] = '\0';
	return plen;
}

int
pg_cstring_send(const char *str, uint8 *out)
{
	/*
	 * cstring_send (pseudotypes.c):
	 *   pq_begintypsend(&buf);
	 *   pq_sendtext(&buf, str, strlen(str));
	 *   PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
	 * pq_sendtext identity arm (no server->client conversion; send side
	 * does not verify): append strlen bytes after the reserved VARHDRSZ
	 * header; endtypsend stamps SET_VARSIZE. Returns total image length.
	 */
	int			slen = 0;
	int			i;
	uint32		len;

	while (str[slen] != '\0')
		slen++;
	len = (uint32) VARHDRSZ + (uint32) slen;
	for (i = 0; i < slen; i++)
		out[VARHDRSZ + i] = (uint8) str[i];
	pg_pseudo_set_varsize_4b(out, len);
	return (int) len;
}

/* anyarray: dummy in/recv (out/send delegate to array_out/array_send) */
PSEUDOTYPE_DUMMY_INPUT_FUNC(anyarray);
PSEUDOTYPE_DUMMY_RECEIVE_FUNC(anyarray);

/* anycompatiblearray */
PSEUDOTYPE_DUMMY_INPUT_FUNC(anycompatiblearray);
PSEUDOTYPE_DUMMY_RECEIVE_FUNC(anycompatiblearray);

/* anyenum */
PSEUDOTYPE_DUMMY_INPUT_FUNC(anyenum);

/* anyrange */
PSEUDOTYPE_DUMMY_INPUT_FUNC(anyrange);

/* anycompatiblerange */
PSEUDOTYPE_DUMMY_INPUT_FUNC(anycompatiblerange);

/* anymultirange */
PSEUDOTYPE_DUMMY_INPUT_FUNC(anymultirange);

/* anycompatiblemultirange */
PSEUDOTYPE_DUMMY_INPUT_FUNC(anycompatiblemultirange);

/*
 * void
 */
uint64
pg_void_in(void)
{
	return 0;					/* PG_RETURN_VOID(): (Datum) 0 */
}

int
pg_void_out(char *out)
{
	/* PG_RETURN_CSTRING(pstrdup("")); */
	return pg_pseudo_strcpy(out, "");
}

uint64
pg_void_recv(void)
{
	/* consumes no bytes */
	return 0;					/* PG_RETURN_VOID(): (Datum) 0 */
}

int
pg_void_send(uint8 *out)
{
	/*
	 * pq_begintypsend(&buf); PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
	 * "send an empty string": VARHDRSZ reserved, nothing appended,
	 * SET_VARSIZE stamped. Returns total image length.
	 */
	uint32		len = (uint32) VARHDRSZ;

	pg_pseudo_set_varsize_4b(out, len);
	return (int) len;
}

/*
 * shell
 */
int
pg_shell_in(void)
{
	/* ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
	 *                 errmsg("cannot accept a value of a shell type"))); */
	PG_PSEUDO_EREPORT(ERRCODE_FEATURE_NOT_SUPPORTED);
	return 1;
}

int
pg_shell_out(void)
{
	/* ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
	 *                 errmsg("cannot display a value of a shell type"))); */
	PG_PSEUDO_EREPORT(ERRCODE_FEATURE_NOT_SUPPORTED);
	return 1;
}

/*
 * pg_node_tree: dummy in/recv; out/send delegate to textout/textsend.
 */
PSEUDOTYPE_DUMMY_INPUT_FUNC(pg_node_tree);
PSEUDOTYPE_DUMMY_RECEIVE_FUNC(pg_node_tree);

int
pg_pg_node_tree_out(const uint8 *payload, int plen, char *out)
{
	/*
	 * return textout(fcinfo);  -> text_to_cstring(t) (varlena.c):
	 *   result = palloc(len + 1); memcpy(result, VARDATA_ANY(t), len);
	 *   result[len] = '\0';
	 * Input is the pre-detoasted payload (ptr,len); palloc -> out.
	 */
	int			i;

	for (i = 0; i < plen; i++)
		out[i] = (char) payload[i];
	out[plen] = '\0';
	return plen;
}

int
pg_pg_node_tree_send(const uint8 *payload, int plen, uint8 *out)
{
	/*
	 * return textsend(fcinfo);  -> (varlena.c)
	 *   pq_begintypsend(&buf);
	 *   pq_sendtext(&buf, VARDATA_ANY(t), VARSIZE_ANY_EXHDR(t));
	 *   PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
	 * pq_sendtext identity arm: append the bytes after the reserved
	 * header; endtypsend stamps SET_VARSIZE.
	 */
	int			i;
	uint32		len = (uint32) VARHDRSZ + (uint32) plen;

	for (i = 0; i < plen; i++)
		out[VARHDRSZ + i] = payload[i];
	pg_pseudo_set_varsize_4b(out, len);
	return (int) len;
}

/*
 * pg_ddl_command
 */
PSEUDOTYPE_DUMMY_IO_FUNCS(pg_ddl_command);
PSEUDOTYPE_DUMMY_BINARY_IO_FUNCS(pg_ddl_command);

/*
 * Dummy I/O functions for various other pseudotypes.
 */
PSEUDOTYPE_DUMMY_IO_FUNCS(any);
PSEUDOTYPE_DUMMY_IO_FUNCS(trigger);
PSEUDOTYPE_DUMMY_IO_FUNCS(event_trigger);
PSEUDOTYPE_DUMMY_IO_FUNCS(language_handler);
PSEUDOTYPE_DUMMY_IO_FUNCS(fdw_handler);
PSEUDOTYPE_DUMMY_IO_FUNCS(table_am_handler);
PSEUDOTYPE_DUMMY_IO_FUNCS(index_am_handler);
PSEUDOTYPE_DUMMY_IO_FUNCS(tsm_handler);
PSEUDOTYPE_DUMMY_IO_FUNCS(internal);
PSEUDOTYPE_DUMMY_IO_FUNCS(anyelement);
PSEUDOTYPE_DUMMY_IO_FUNCS(anynonarray);
PSEUDOTYPE_DUMMY_IO_FUNCS(anycompatible);
PSEUDOTYPE_DUMMY_IO_FUNCS(anycompatiblenonarray);
