/*
 * Vendored PostgreSQL C for the pseudotypes proof family.
 *
 * Provenance:
 *   - src/backend/utils/adt/pseudotypes.c   (whole file: the four
 *     PSEUDOTYPE_DUMMY_* macros + their instantiations, shell_in/shell_out,
 *     cstring_in/cstring_out, void_in/void_out/void_recv/void_send,
 *     pg_node_tree_out/pg_node_tree_send)
 *   - src/include/utils/elog.h              (PGSIXBIT, MAKE_SQLSTATE, ERROR)
 *   - src/backend/utils/errcodes.h (generated from errcodes.txt):
 *     ERRCODE_FEATURE_NOT_SUPPORTED = MAKE_SQLSTATE('0','A','0','0','0')
 *   - src/include/varatt.h                  (SET_VARSIZE 4B form)
 *   ref: postgres/postgres REL_18_STABLE
 *        (raw.githubusercontent.com/postgres/postgres/REL_18_STABLE/...)
 *   fetched: 2026-07-28
 *
 * Shims (plumbing only, never logic):
 *   - `pg_` prefix on every function name (macro instantiations included:
 *     the PSEUDOTYPE_DUMMY_* macros below are the REL_18_STABLE macros with
 *     the ereport rewire applied once, then instantiated with the verbatim
 *     instantiation lines from pseudotypes.c).
 *   - ereport(ERROR, errcode(...), errmsg(...)) -> PROOF_EREPORT2(level,
 *     sqlstate, ...): writes the elog.h ERROR level and the MAKE_SQLSTATE
 *     errcode into out-params and the function returns 1 at the exact
 *     program point of the ereport (C aborts via longjmp there). This is
 *     the suite's PROOF_EREPORT_FLAG convention widened to carry the
 *     errcode + level for sqlstate/level parity (pg_proof_shim.h). Error
 *     message TEXT never crosses the seam.
 *   - fmgr PG_FUNCTION_ARGS unwrapping -> plain C signatures:
 *     PG_GETARG_CSTRING -> const char* param; PG_RETURN_CSTRING(p) /
 *     PG_RETURN_BYTEA_P(p) -> caller-provided output buffer + returned
 *     length; PG_RETURN_VOID() -> `return 0` with uint64 return type
 *     (fmgr.h: PG_RETURN_VOID is `return (Datum) 0`; Datum -> uint64 here).
 *   - palloc-based pstrdup -> pg_proof_strcpy into a caller buffer (the
 *     copy-through-NUL semantics are verbatim; only the allocation moved).
 *   - pq_begintypsend/pq_endtypsend (pqformat.c) -> caller buffer with
 *     VARHDRSZ reserved header bytes + SET_VARSIZE stamped at end; the
 *     SET_VARSIZE_4B encoding (varatt.h little-endian arm: len << 2,
 *     native-endian store) is written out explicitly. pq_sendtext's
 *     server->client encoding conversion is taken in its identity arm
 *     (ClientEncoding == ServerEncoding: pg_server_to_client returns the
 *     caller's pointer, transmission = memcpy) — the Rust harness pins the
 *     matching seam to identity, so both sides model the same arm.
 *   - textout/text_to_cstring (varlena.c) for pg_node_tree_out: input is
 *     the pre-detoasted payload (ptr,len) per the suite's VARLENA PATTERN
 *     (pg_detoast_datum_packed stays out of scope); the body is
 *     text_to_cstring's palloc(len+1) + memcpy + NUL-terminate with the
 *     palloc shimmed to the caller buffer.
 *
 * Function bodies between arg-fetch and return are otherwise verbatim.
 */

/* shared suite shim boilerplate: typedefs, VARHDRSZ, PROOF_EREPORT_FLAG */
#include "../../support/c/pg_proof_shim.h"

/* ---- elog.h, verbatim ---- */
#define PGSIXBIT(ch)	(((ch) - '0') & 0x3F)
#define MAKE_SQLSTATE(ch1,ch2,ch3,ch4,ch5) \
	(PGSIXBIT(ch1) + (PGSIXBIT(ch2) << 6) + (PGSIXBIT(ch3) << 12) + \
	 (PGSIXBIT(ch4) << 18) + (PGSIXBIT(ch5) << 24))
#define PG_PROOF_ERROR	21		/* elog.h: #define ERROR 21 */

/* ---- errcodes.h (generated from errcodes.txt), verbatim value ---- */
#define ERRCODE_FEATURE_NOT_SUPPORTED MAKE_SQLSTATE('0','A','0','0','0')

/* ---- ereport rewire (see header comment) ---- */
#define PROOF_EREPORT2(levelp, sqlstatep, lvl, code) \
	do { *(levelp) = (lvl); *(sqlstatep) = (code); } while (0)

/*
 * The four dummy-I/O macros from pseudotypes.c REL_18_STABLE, with the
 * ereport rewire applied. The errcode and errmsg-SHAPE (accept vs display)
 * per function kind are exactly the C macro's; message text is out of
 * scope by the ereport shim.
 */
#define PSEUDOTYPE_DUMMY_INPUT_FUNC(typname) \
int \
pg_##typname##_in(int32 *level, int32 *sqlstate) \
{ \
	/* ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), \
	 *                 errmsg("cannot accept a value of type %s", #typname))); */ \
	PROOF_EREPORT2(level, sqlstate, PG_PROOF_ERROR, ERRCODE_FEATURE_NOT_SUPPORTED); \
	return 1; \
	/* PG_RETURN_VOID();  unreachable: C longjmps at the ereport */ \
} \
\
extern int no_such_variable

#define PSEUDOTYPE_DUMMY_IO_FUNCS(typname) \
PSEUDOTYPE_DUMMY_INPUT_FUNC(typname); \
\
int \
pg_##typname##_out(int32 *level, int32 *sqlstate) \
{ \
	/* ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), \
	 *                 errmsg("cannot display a value of type %s", #typname))); */ \
	PROOF_EREPORT2(level, sqlstate, PG_PROOF_ERROR, ERRCODE_FEATURE_NOT_SUPPORTED); \
	return 1; \
} \
\
extern int no_such_variable

#define PSEUDOTYPE_DUMMY_RECEIVE_FUNC(typname) \
int \
pg_##typname##_recv(int32 *level, int32 *sqlstate) \
{ \
	/* ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), \
	 *                 errmsg("cannot accept a value of type %s", #typname))); */ \
	PROOF_EREPORT2(level, sqlstate, PG_PROOF_ERROR, ERRCODE_FEATURE_NOT_SUPPORTED); \
	return 1; \
} \
\
extern int no_such_variable

#define PSEUDOTYPE_DUMMY_BINARY_IO_FUNCS(typname) \
PSEUDOTYPE_DUMMY_RECEIVE_FUNC(typname); \
\
int \
pg_##typname##_send(int32 *level, int32 *sqlstate) \
{ \
	/* ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), \
	 *                 errmsg("cannot display a value of type %s", #typname))); */ \
	PROOF_EREPORT2(level, sqlstate, PG_PROOF_ERROR, ERRCODE_FEATURE_NOT_SUPPORTED); \
	return 1; \
} \
\
extern int no_such_variable

/* ---- pstrdup shim: copy-through-NUL into caller buffer, returns strlen ---- */
static int
pg_proof_strcpy(char *dst, const char *src)
{
	int			i = 0;

	while ((dst[i] = src[i]) != '\0')
		i++;
	return i;
}

/* ---- varatt.h SET_VARSIZE, 4B form, little-endian arm (native store) ---- */
static void
pg_proof_set_varsize_4b(uint8 *ptr, uint32 len)
{
	uint32		header = len << 2;	/* SET_VARSIZE_4B: va_header = len << 2 */

	ptr[0] = (uint8) (header & 0xFF);
	ptr[1] = (uint8) ((header >> 8) & 0xFF);
	ptr[2] = (uint8) ((header >> 16) & 0xFF);
	ptr[3] = (uint8) ((header >> 24) & 0xFF);
}

/*
 * cstring — real I/O functions (pseudotypes.c verbatim modulo the fmgr /
 * pstrdup shims). cstring_recv/cstring_send are not pg_proc-reachable in
 * pgrust (no registered callers) and are not vendored.
 */
int
pg_cstring_in(const char *str, char *out)
{
	/* char *str = PG_GETARG_CSTRING(0); PG_RETURN_CSTRING(pstrdup(str)); */
	return pg_proof_strcpy(out, str);
}

int
pg_cstring_out(const char *str, char *out)
{
	/* char *str = PG_GETARG_CSTRING(0); PG_RETURN_CSTRING(pstrdup(str)); */
	return pg_proof_strcpy(out, str);
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
	return pg_proof_strcpy(out, "");
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
	 * StringInfoData buf; pq_begintypsend(&buf);
	 * PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
	 *
	 * begintypsend reserves VARHDRSZ bytes; nothing appended ("send an
	 * empty string"); endtypsend stamps SET_VARSIZE(result, buf->len).
	 * Returns the total image length.
	 */
	uint32		len = (uint32) VARHDRSZ;

	pg_proof_set_varsize_4b(out, len);
	return (int) len;
}

/*
 * shell
 */
int
pg_shell_in(int32 *level, int32 *sqlstate)
{
	/* ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
	 *                 errmsg("cannot accept a value of a shell type"))); */
	PROOF_EREPORT2(level, sqlstate, PG_PROOF_ERROR, ERRCODE_FEATURE_NOT_SUPPORTED);
	return 1;
}

int
pg_shell_out(int32 *level, int32 *sqlstate)
{
	/* ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
	 *                 errmsg("cannot display a value of a shell type"))); */
	PROOF_EREPORT2(level, sqlstate, PG_PROOF_ERROR, ERRCODE_FEATURE_NOT_SUPPORTED);
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
	 * Input is the pre-detoasted payload (VARLENA PATTERN); palloc -> out.
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
	 * pq_sendtext identity arm (no client-encoding conversion): append the
	 * bytes after the reserved header; endtypsend stamps SET_VARSIZE.
	 */
	int			i;
	uint32		len = (uint32) VARHDRSZ + (uint32) plen;

	for (i = 0; i < plen; i++)
		out[VARHDRSZ + i] = payload[i];
	pg_proof_set_varsize_4b(out, len);
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
