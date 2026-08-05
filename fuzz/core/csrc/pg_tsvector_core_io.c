/*
 * pg_tsvector_core_io.c: runtime shims + fuzz driver entries for the
 * tsvector_core_diff differential fuzz target (100%-coverage campaign;
 * crate crates/backend/utils/adt/tsvector_core).
 *
 * THE VENDORED ORACLE IS NOT IN THIS FILE: the upstream C lives
 * byte-identical (shasum-verifiable against
 * ../pgrust-fabled/vendor/postgres-src @
 * 62d6c7d3df6287f1bd83199c1a746e50d31571a0, PostgreSQL 18.3 Stamp-18.3) in
 * csrc/tsvec/: tsvector.c, tsvector_parser.c, tsvector_op.c (three labeled
 * `#if 0 PG_DIFF CARVE` blocks: tsvector_unnest [SRF/funcapi],
 * TS_execute_locations[_recurse] [pg_list], ts_match_tt/tq + ts_stat* +
 * tsvector_update_trigger* [dictionary GUC / SPI / trigger machinery]),
 * qsort.c + qsort_arg.c + lib/sort_template.h (pg_qsort/qsort_arg: PG's own
 * sort, vendored so unstable-sort tie order matches real PG exactly),
 * include/tsearch/ts_type.h, include/tsearch/ts_utils.h,
 * include/tsearch/ts_locale.h, include/varatt.h, include/common/int.h,
 * include/lib/qunique.h (all verbatim upstream headers).
 *
 * Shims (plumbing only, never logic; see csrc/tsvec/postgres.h header):
 *   - palloc family -> TLS dynamic pointer arena reset per driver entry.
 *   - ereport/errcode/elog -> TLS errcode + longjmp; errsave/ereturn keep
 *     the real soft-error contract (record in ErrorSaveContext, return).
 *   - pqformat -> byte-exact big-endian wire helpers, identity encoding
 *     conversion (client==server); StringInfo per upstream layout.
 *   - pg_mblen_cstr/pg_mblen_range -> VERBATIM pg_utf_mblen core
 *     (src/common/wchar.c) + the upstream bound checks; DATABASE ENCODING
 *     PINNED UTF-8 on both sides (Rust driver calls
 *     SetDatabaseEncoding(PG_UTF8) per exec).
 *   - deconstruct_array_builtin/construct_array_builtin -> TLS-registered
 *     element lists (array (de)construction is the arrayfuncs crate's
 *     computation, not this crate's; the driver hands both sides the same
 *     element list).
 *   - cstring_to_text_with_len -> verbatim-equivalent varlena.c body.
 *
 * Errcode classes (values in csrc/tsvec/postgres.h, mirrored by the Rust
 * driver): 1=42601 syntax_error, 2=54000 program_limit_exceeded,
 * 3=22004 null_value_not_allowed, 4=2200F zero_length_character_string,
 * 5=22023 invalid_parameter_value, 6=08P01 protocol_violation,
 * 7=22021 character_not_in_repertoire, 99=elog internal (XX000).
 */

#include "tsvec/postgres.h"

#include <assert.h>
#include <setjmp.h>
#include <stdarg.h>

#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/fmgrprotos.h"
#include "libpq/pqformat.h"
#include "mb/pg_wchar.h"
#include "nodes/miscnodes.h"
#include "tsearch/ts_type.h"
#include "tsearch/ts_utils.h"
#include "varatt.h"

/* Shared TLS errcode channel (defined in csrc/pg_float_io.c). */
extern _Thread_local int pg_diff_errcode;

/* the vendored SQL-callable ops this driver invokes (tsvector_op.c) */
extern Datum tsvector_lt(FunctionCallInfo fcinfo);
extern Datum tsvector_le(FunctionCallInfo fcinfo);
extern Datum tsvector_eq(FunctionCallInfo fcinfo);
extern Datum tsvector_ne(FunctionCallInfo fcinfo);
extern Datum tsvector_ge(FunctionCallInfo fcinfo);
extern Datum tsvector_gt(FunctionCallInfo fcinfo);
extern Datum tsvector_cmp(FunctionCallInfo fcinfo);
extern Datum tsvector_strip(FunctionCallInfo fcinfo);
extern Datum tsvector_length(FunctionCallInfo fcinfo);
extern Datum tsvector_setweight(FunctionCallInfo fcinfo);
extern Datum tsvector_setweight_by_filter(FunctionCallInfo fcinfo);
extern Datum tsvector_concat(FunctionCallInfo fcinfo);
extern Datum tsvector_filter(FunctionCallInfo fcinfo);
extern Datum tsvector_delete_str(FunctionCallInfo fcinfo);
extern Datum tsvector_delete_arr(FunctionCallInfo fcinfo);
extern Datum tsvector_to_array(FunctionCallInfo fcinfo);
extern Datum array_to_tsvector(FunctionCallInfo fcinfo);
extern Datum ts_match_qv(FunctionCallInfo fcinfo);

/* ==================== palloc arena (dynamic; see header) ================= */

static _Thread_local void **pg_tsvec_arena;
static _Thread_local int pg_tsvec_arena_n;
static _Thread_local int pg_tsvec_arena_cap;

static void
pg_tsvec_arena_reset(void)
{
	int			i;

	for (i = 0; i < pg_tsvec_arena_n; i++)
		free(pg_tsvec_arena[i]);
	pg_tsvec_arena_n = 0;
}

static void
pg_tsvec_arena_track(void *p)
{
	if (pg_tsvec_arena_n == pg_tsvec_arena_cap)
	{
		pg_tsvec_arena_cap = pg_tsvec_arena_cap ? pg_tsvec_arena_cap * 2 : 256;
		pg_tsvec_arena = realloc(pg_tsvec_arena,
								 pg_tsvec_arena_cap * sizeof(void *));
		assert(pg_tsvec_arena != NULL);
	}
	pg_tsvec_arena[pg_tsvec_arena_n++] = p;
}

/* real palloc contract: requests beyond MaxAllocSize are rejected with
 * elog(ERROR, "invalid memory alloc request size") — enforced here so the
 * oracle can never balloon the fuzz host either. */
static void
pg_tsvec_alloc_guard(size_t n)
{
	if (n > (size_t) 0x3fffffff)
		pg_tsvec_elog_error("invalid memory alloc request size");
}

void *
pg_tsvec_palloc(size_t n)
{
	void	   *p;

	pg_tsvec_alloc_guard(n);
	p = malloc(n ? n : 1);

	assert(p != NULL);
	pg_tsvec_arena_track(p);
	return p;
}

void *
pg_tsvec_palloc0(size_t n)
{
	void	   *p;

	pg_tsvec_alloc_guard(n);
	p = calloc(1, n ? n : 1);

	assert(p != NULL);
	pg_tsvec_arena_track(p);
	return p;
}

void *
pg_tsvec_repalloc(void *old, size_t n)
{
	int			i;

	for (i = pg_tsvec_arena_n - 1; i >= 0; i--)
	{
		if (pg_tsvec_arena[i] == old)
		{
			void	   *p;

			pg_tsvec_alloc_guard(n);
			p = realloc(old, n);

			assert(p != NULL);
			pg_tsvec_arena[i] = p;
			return p;
		}
	}
	assert(!"repalloc of a pointer the arena never issued");
	abort();
}

void
pg_tsvec_pfree(void *p)
{
	int			i;

	for (i = pg_tsvec_arena_n - 1; i >= 0; i--)
	{
		if (pg_tsvec_arena[i] == p)
		{
			free(p);
			pg_tsvec_arena[i] = pg_tsvec_arena[--pg_tsvec_arena_n];
			return;
		}
	}
	assert(!"pfree of a pointer the arena never issued");
	abort();
}

/* ==================== error machinery ==================================== */

/* non-static: pg_tsrank_io.c driver entries longjmp-guard through the same
 * buffer (one error machinery for the whole tsvec oracle web) */
_Thread_local jmp_buf pg_tsvec_jmp;

int
errcode(int sqlerrcode)
{
	pg_diff_errcode = sqlerrcode;
	return 0;
}

int
errmsg(const char *fmt,...)
{
	(void) fmt;
	return 0;
}

int
errdetail(const char *fmt,...)
{
	(void) fmt;
	return 0;
}

int
errhint(const char *fmt,...)
{
	(void) fmt;
	return 0;
}

void
pg_tsvec_errthrow(void)
{
	if (pg_diff_errcode == 0)
		pg_diff_errcode = PG_DIFF_ERR_INTERNAL;
	longjmp(pg_tsvec_jmp, 1);
}

void
pg_tsvec_elog_error(const char *fmt,...)
{
	(void) fmt;
	pg_diff_errcode = PG_DIFF_ERR_INTERNAL;
	longjmp(pg_tsvec_jmp, 1);
}

/* errsave soft path: record into a live ErrorSaveContext, else throw */
bool
pg_tsvec_soft_save(Node *escontext)
{
	if (escontext != NULL && IsA(escontext, ErrorSaveContext))
	{
		((ErrorSaveContext *) escontext)->error_occurred = true;
		if (pg_diff_errcode == 0)
			pg_diff_errcode = PG_DIFF_ERR_INTERNAL;
		return true;
	}
	return false;
}

/* ==================== mini-fmgr helpers ================================== */

Datum
pg_tsvec_direct_call2(Datum (*func) (FunctionCallInfo), Datum arg1, Datum arg2)
{
	FunctionCallInfoBaseData fcinfo;

	memset(&fcinfo, 0, sizeof(fcinfo));
	fcinfo.nargs = 2;
	fcinfo.args[0].value = arg1;
	fcinfo.args[1].value = arg2;
	return func(&fcinfo);
}

/* ==================== StringInfo + pqformat shims ======================== */

void
initStringInfo(StringInfo str)
{
	str->maxlen = 1024;
	str->data = pg_tsvec_palloc(str->maxlen);
	str->len = 0;
	str->cursor = 0;
	str->data[0] = '\0';
}

static void
pg_tsvec_si_ensure(StringInfo str, int datalen)
{
	if (str->len + datalen + 1 > str->maxlen)
	{
		while (str->len + datalen + 1 > str->maxlen)
			str->maxlen *= 2;
		str->data = pg_tsvec_repalloc(str->data, str->maxlen);
	}
}

void
appendBinaryStringInfo(StringInfo str, const void *data, int datalen)
{
	pg_tsvec_si_ensure(str, datalen);
	memcpy(str->data + str->len, data, datalen);
	str->len += datalen;
	str->data[str->len] = '\0';
}

void
appendStringInfoChar(StringInfo str, char ch)
{
	appendBinaryStringInfo(str, &ch, 1);
}

void
pq_begintypsend(StringInfo buf)
{
	initStringInfo(buf);
	/* reserve four bytes for the bytea length word (pqformat.c) */
	appendStringInfoChar(buf, '\0');
	appendStringInfoChar(buf, '\0');
	appendStringInfoChar(buf, '\0');
	appendStringInfoChar(buf, '\0');
}

bytea *
pq_endtypsend(StringInfo buf)
{
	bytea	   *result = (bytea *) buf->data;

	assert(buf->len >= VARHDRSZ);
	SET_VARSIZE(result, buf->len);
	return result;
}

void
pq_sendbyte(StringInfo buf, uint8 byt)
{
	appendBinaryStringInfo(buf, &byt, 1);
}

void
pq_sendint16(StringInfo buf, uint16 i)
{
	uint8		b[2];

	b[0] = (i >> 8) & 0xff;
	b[1] = i & 0xff;
	appendBinaryStringInfo(buf, b, 2);
}

void
pq_sendint32(StringInfo buf, uint32 i)
{
	uint8		b[4];

	b[0] = (i >> 24) & 0xff;
	b[1] = (i >> 16) & 0xff;
	b[2] = (i >> 8) & 0xff;
	b[3] = i & 0xff;
	appendBinaryStringInfo(buf, b, 4);
}

/* identity client encoding conversion: client==server (UTF-8 pin) */
void
pq_sendtext(StringInfo buf, const char *str, int slen)
{
	appendBinaryStringInfo(buf, str, slen);
}

unsigned int
pq_getmsgint(StringInfo msg, int b)
{
	unsigned int result;
	const unsigned char *d;

	if (msg->cursor + b > msg->len)
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("insufficient data left in message")));
	d = (const unsigned char *) (msg->data + msg->cursor);
	msg->cursor += b;
	switch (b)
	{
		case 1:
			result = d[0];
			break;
		case 2:
			result = ((unsigned int) d[0] << 8) | d[1];
			break;
		case 4:
			result = ((unsigned int) d[0] << 24) | ((unsigned int) d[1] << 16) |
				((unsigned int) d[2] << 8) | d[3];
			break;
		default:
			abort();
	}
	return result;
}

/* VERBATIM from src/common/wchar.c @ 62d6c7d3df */
static bool
pg_utf8_islegal(const unsigned char *source, int length)
{
	unsigned char a;

	switch (length)
	{
		default:
			/* reject lengths 5 and 6 for now */
			return false;
		case 4:
			a = source[3];
			if (a < 0x80 || a > 0xBF)
				return false;
			/* FALL THRU */
		case 3:
			a = source[2];
			if (a < 0x80 || a > 0xBF)
				return false;
			/* FALL THRU */
		case 2:
			a = source[1];
			switch (*source)
			{
				case 0xE0:
					if (a < 0xA0 || a > 0xBF)
						return false;
					break;
				case 0xED:
					if (a < 0x80 || a > 0x9F)
						return false;
					break;
				case 0xF0:
					if (a < 0x90 || a > 0xBF)
						return false;
					break;
				case 0xF4:
					if (a < 0x80 || a > 0x8F)
						return false;
					break;
				default:
					if (a < 0x80 || a > 0xBF)
						return false;
					break;
			}
			/* FALL THRU */
		case 1:
			a = *source;
			if (a >= 0x80 && a < 0xC2)
				return false;
			if (a > 0xF4)
				return false;
			break;
	}
	return true;
}

/* VERBATIM from src/common/wchar.c @ 62d6c7d3df */
static int
pg_utf8_verifychar(const unsigned char *s, int len)
{
	int			l;

	if ((*s & 0x80) == 0)
	{
		if (*s == '\0')
			return -1;
		return 1;
	}
	else if ((*s & 0xe0) == 0xc0)
		l = 2;
	else if ((*s & 0xf0) == 0xe0)
		l = 3;
	else if ((*s & 0xf8) == 0xf0)
		l = 4;
	else
		l = 1;

	if (l > len)
		return -1;

	if (!pg_utf8_islegal(s, l))
		return -1;

	return l;
}

/* pg_verify_mbstr(PG_UTF8, ...) contract: invalid byte sequence -> 22021
 * (real pq_getmsgstring runs pg_client_to_server -> pg_any_to_server,
 * which VALIDATES even when client==server encoding). */
static void
pg_tsvec_verify_utf8(const char *s, int len)
{
	const unsigned char *p = (const unsigned char *) s;
	int			remaining = len;

	while (remaining > 0)
	{
		int			l = pg_utf8_verifychar(p, remaining);

		if (l < 0)
			ereport(ERROR,
					(errcode(ERRCODE_CHARACTER_NOT_IN_REPERTOIRE),
					 errmsg("invalid byte sequence for encoding")));
		p += l;
		remaining -= l;
	}
}

/* pq_getmsgrawstring + validating same-encoding conversion (pqformat.c +
 * mbutils.c pg_any_to_server contract) */
const char *
pq_getmsgstring(StringInfo msg)
{
	char	   *str = msg->data + msg->cursor;
	size_t		slen = strlen(str);	/* msg->data is NUL-terminated (driver) */

	if (msg->cursor + (int) slen >= msg->len)
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("invalid string in message")));
	msg->cursor += (int) slen + 1;
	pg_tsvec_verify_utf8(str, (int) slen);
	return str;
}

/* ==================== encoding shims (UTF-8 pin) ========================= */

/* VERBATIM from src/common/wchar.c @ 62d6c7d3df */
int
pg_utf_mblen(const unsigned char *s)
{
	int			len;

	if ((*s & 0x80) == 0)
		len = 1;
	else if ((*s & 0xe0) == 0xc0)
		len = 2;
	else if ((*s & 0xf0) == 0xe0)
		len = 3;
	else if ((*s & 0xf8) == 0xf0)
		len = 4;
#ifdef NOT_USED
	else if ((*s & 0xfc) == 0xf8)
		len = 5;
	else if ((*s & 0xfe) == 0xfc)
		len = 6;
#endif
	else
		len = 1;
	return len;
}

int
pg_database_encoding_max_length(void)
{
	return 4;					/* UTF-8 */
}

/* mbutils.c pg_mblen_cstr bound check; harness feeds valid UTF-8 so the
 * error arm is unreachable (kept for the exact contract). */
int
pg_mblen_cstr(const char *mbstr)
{
	int			length = pg_utf_mblen((const unsigned char *) mbstr);
	int			i;

	for (i = 1; i < length; ++i)
		if (unlikely(mbstr[i] == 0))
			ereport(ERROR,
					(errcode(ERRCODE_CHARACTER_NOT_IN_REPERTOIRE),
					 errmsg("invalid byte sequence for encoding")));
	return length;
}

int
pg_mblen_range(const char *mbstr, const char *end)
{
	int			length = pg_utf_mblen((const unsigned char *) mbstr);

	if (mbstr + length > end)
		ereport(ERROR,
				(errcode(ERRCODE_CHARACTER_NOT_IN_REPERTOIRE),
				 errmsg("invalid byte sequence for encoding")));
	return length;
}

/* ==================== text + array marshaling shims ====================== */

/* verbatim-equivalent varlena.c cstring_to_text_with_len */
text *
cstring_to_text_with_len(const char *s, int len)
{
	text	   *result = (text *) pg_tsvec_palloc(len + VARHDRSZ);

	SET_VARSIZE(result, len + VARHDRSZ);
	memcpy(VARDATA(result), s, len);
	return result;
}

/*
 * TLS-registered element lists (see file header): the driver loads the
 * array argument's elements before invoking the vendored function;
 * deconstruct_array_builtin hands them over. construct_array_builtin
 * captures the output element list for the driver to serialize.
 */
static _Thread_local Datum *pg_tsvec_arr_elems;
static _Thread_local bool *pg_tsvec_arr_nulls;
static _Thread_local int pg_tsvec_arr_n;

static _Thread_local Datum *pg_tsvec_outarr_elems;
static _Thread_local int pg_tsvec_outarr_n;

void
deconstruct_array_builtin(ArrayType *array, Oid elmtype,
						  Datum **elemsp, bool **nullsp, int *nelemsp)
{
	(void) array;
	(void) elmtype;
	*elemsp = pg_tsvec_arr_elems;
	*nullsp = pg_tsvec_arr_nulls;
	*nelemsp = pg_tsvec_arr_n;
}

ArrayType *
construct_array_builtin(Datum *elems, int nelems, Oid elmtype)
{
	(void) elmtype;
	pg_tsvec_outarr_elems = pg_tsvec_palloc(sizeof(Datum) * (nelems ? nelems : 1));
	memcpy(pg_tsvec_outarr_elems, elems, sizeof(Datum) * nelems);
	pg_tsvec_outarr_n = nelems;
	return (ArrayType *) pg_tsvec_outarr_elems; /* opaque to caller */
}

/* ==================== driver plumbing ==================================== */

#define PG_TSVEC_ECAP (-2)		/* caller buffer too small: harness bug */

static int
pg_tsvec_copyout(const void *src, int n, unsigned char *out, int outcap,
				 int *outlen)
{
	if (n > outcap)
		return PG_TSVEC_ECAP;
	memcpy(out, src, n);
	*outlen = n;
	return 0;
}

/* rebuild a full varlena TSVector/TSQuery from payload bytes, aligned
 * (non-static: shared with pg_tsrank_io.c) */
struct varlena *
pg_tsvec_mkvarlena(const unsigned char *payload, int len)
{
	struct varlena *v = pg_tsvec_palloc(VARHDRSZ + len);

	SET_VARSIZE(v, VARHDRSZ + len);
	memcpy(VARDATA(v), payload, len);
	return v;
}

/* per-entry state reset, shared with pg_tsrank_io.c (which pairs it with its
 * own setjmp on pg_tsvec_jmp — setjmp must live in the entry's frame) */
void
pg_tsvec_prep(void)
{
	pg_tsvec_arena_reset();
	pg_diff_errcode = 0;
	pg_tsvec_arr_elems = NULL;
	pg_tsvec_arr_nulls = NULL;
	pg_tsvec_arr_n = 0;
	pg_tsvec_outarr_elems = NULL;
	pg_tsvec_outarr_n = 0;
}

/* per-entry prologue; returns nonzero from the enclosing function on throw */
#define PG_TSVEC_ENTER() \
	do { \
		pg_tsvec_arena_reset(); \
		pg_diff_errcode = 0; \
		pg_tsvec_arr_elems = NULL; \
		pg_tsvec_arr_nulls = NULL; \
		pg_tsvec_arr_n = 0; \
		pg_tsvec_outarr_elems = NULL; \
		pg_tsvec_outarr_n = 0; \
		if (setjmp(pg_tsvec_jmp) != 0) \
			return 1; \
	} while (0)

static int
pg_tsvec_result_image(Datum d, unsigned char *out, int outcap, int *outlen)
{
	struct varlena *v = (struct varlena *) DatumGetPointer(d);

	return pg_tsvec_copyout(VARDATA(v), VARSIZE(v) - VARHDRSZ, out, outcap,
							outlen);
}

/* build the TLS text[] element list from a packed (buf, lens[]) pair;
 * lens[i] < 0 means SQL NULL */
static void
pg_tsvec_load_textarr(const unsigned char *buf, const int32 *lens, int n)
{
	int			i;
	const unsigned char *p = buf;

	pg_tsvec_arr_elems = pg_tsvec_palloc(sizeof(Datum) * (n ? n : 1));
	pg_tsvec_arr_nulls = pg_tsvec_palloc(sizeof(bool) * (n ? n : 1));
	pg_tsvec_arr_n = n;
	for (i = 0; i < n; i++)
	{
		if (lens[i] < 0)
		{
			pg_tsvec_arr_elems[i] = (Datum) 0;
			pg_tsvec_arr_nulls[i] = true;
		}
		else
		{
			pg_tsvec_arr_elems[i] =
				PointerGetDatum(cstring_to_text_with_len((const char *) p,
														 lens[i]));
			pg_tsvec_arr_nulls[i] = false;
			p += lens[i];
		}
	}
}

/* ==================== SECTION: fuzz-facing driver entries ================ */

int
pg_diff_tsvec_in(const char *input, int soft, unsigned char *out, int outcap,
				 int *outlen)
{
	FunctionCallInfoBaseData fcinfo;
	ErrorSaveContext esc;
	Datum		d;

	PG_TSVEC_ENTER();
	memset(&fcinfo, 0, sizeof(fcinfo));
	memset(&esc, 0, sizeof(esc));
	esc.type = T_ErrorSaveContext;
	fcinfo.nargs = 1;
	fcinfo.args[0].value = PointerGetDatum(input);
	if (soft)
		fcinfo.context = &esc;
	d = tsvectorin(&fcinfo);
	if (soft && esc.error_occurred)
		return 2;				/* soft error captured; errcode recorded */
	if (fcinfo.isnull)
		return 3;				/* NULL result without soft error: unexpected */
	return pg_tsvec_result_image(d, out, outcap, outlen);
}

int
pg_diff_tsvec_out(const unsigned char *img, int imglen, unsigned char *out,
				  int outcap, int *outlen)
{
	FunctionCallInfoBaseData fcinfo;
	char	   *res;

	PG_TSVEC_ENTER();
	memset(&fcinfo, 0, sizeof(fcinfo));
	fcinfo.nargs = 1;
	fcinfo.args[0].value = PointerGetDatum(pg_tsvec_mkvarlena(img, imglen));
	res = (char *) DatumGetPointer(tsvectorout(&fcinfo));
	return pg_tsvec_copyout(res, (int) strlen(res), out, outcap, outlen);
}

int
pg_diff_tsvec_send(const unsigned char *img, int imglen, unsigned char *out,
				   int outcap, int *outlen)
{
	FunctionCallInfoBaseData fcinfo;
	bytea	   *res;

	PG_TSVEC_ENTER();
	memset(&fcinfo, 0, sizeof(fcinfo));
	fcinfo.nargs = 1;
	fcinfo.args[0].value = PointerGetDatum(pg_tsvec_mkvarlena(img, imglen));
	res = (bytea *) DatumGetPointer(tsvectorsend(&fcinfo));
	return pg_tsvec_copyout(VARDATA(res), VARSIZE(res) - VARHDRSZ, out,
							outcap, outlen);
}

int
pg_diff_tsvec_recv(const unsigned char *wire, int wirelen, unsigned char *out,
				   int outcap, int *outlen)
{
	FunctionCallInfoBaseData fcinfo;
	StringInfoData buf;
	Datum		d;

	PG_TSVEC_ENTER();
	/* StringInfo over a NUL-terminated copy (pq_getmsgstring contract) */
	buf.data = pg_tsvec_palloc(wirelen + 1);
	memcpy(buf.data, wire, wirelen);
	buf.data[wirelen] = '\0';
	buf.len = wirelen;
	buf.maxlen = wirelen + 1;
	buf.cursor = 0;
	memset(&fcinfo, 0, sizeof(fcinfo));
	fcinfo.nargs = 1;
	fcinfo.args[0].value = PointerGetDatum(&buf);
	d = tsvectorrecv(&fcinfo);
	return pg_tsvec_result_image(d, out, outcap, outlen);
}

/*
 * All seven comparison wrappers over one pair: cmp value plus the six
 * boolean results packed as bits (lt,le,eq,ne,ge,gt = bits 0..5).
 */
int
pg_diff_tsvec_cmp(const unsigned char *a, int alen, const unsigned char *b,
				  int blen, int32 *cmp, int32 *boolbits)
{
	Datum		da,
				db;
	int32		bits = 0;

	PG_TSVEC_ENTER();
	da = PointerGetDatum(pg_tsvec_mkvarlena(a, alen));
	db = PointerGetDatum(pg_tsvec_mkvarlena(b, blen));
	*cmp = DatumGetInt32(pg_tsvec_direct_call2(tsvector_cmp, da, db));
	bits |= DatumGetBool(pg_tsvec_direct_call2(tsvector_lt, da, db)) << 0;
	bits |= DatumGetBool(pg_tsvec_direct_call2(tsvector_le, da, db)) << 1;
	bits |= DatumGetBool(pg_tsvec_direct_call2(tsvector_eq, da, db)) << 2;
	bits |= DatumGetBool(pg_tsvec_direct_call2(tsvector_ne, da, db)) << 3;
	bits |= DatumGetBool(pg_tsvec_direct_call2(tsvector_ge, da, db)) << 4;
	bits |= DatumGetBool(pg_tsvec_direct_call2(tsvector_gt, da, db)) << 5;
	*boolbits = bits;
	return 0;
}

int
pg_diff_tsvec_strip(const unsigned char *img, int imglen, unsigned char *out,
					int outcap, int *outlen)
{
	FunctionCallInfoBaseData fcinfo;

	PG_TSVEC_ENTER();
	memset(&fcinfo, 0, sizeof(fcinfo));
	fcinfo.nargs = 1;
	fcinfo.args[0].value = PointerGetDatum(pg_tsvec_mkvarlena(img, imglen));
	return pg_tsvec_result_image(tsvector_strip(&fcinfo), out, outcap, outlen);
}

int
pg_diff_tsvec_length(const unsigned char *img, int imglen, int32 *res)
{
	FunctionCallInfoBaseData fcinfo;

	PG_TSVEC_ENTER();
	memset(&fcinfo, 0, sizeof(fcinfo));
	fcinfo.nargs = 1;
	fcinfo.args[0].value = PointerGetDatum(pg_tsvec_mkvarlena(img, imglen));
	*res = DatumGetInt32(tsvector_length(&fcinfo));
	return 0;
}

int
pg_diff_tsvec_setweight(const unsigned char *img, int imglen, char w,
						unsigned char *out, int outcap, int *outlen)
{
	FunctionCallInfoBaseData fcinfo;

	PG_TSVEC_ENTER();
	memset(&fcinfo, 0, sizeof(fcinfo));
	fcinfo.nargs = 2;
	fcinfo.args[0].value = PointerGetDatum(pg_tsvec_mkvarlena(img, imglen));
	fcinfo.args[1].value = CharGetDatum(w);
	return pg_tsvec_result_image(tsvector_setweight(&fcinfo), out, outcap,
								 outlen);
}

int
pg_diff_tsvec_setweight_by_filter(const unsigned char *img, int imglen,
								  char w, const unsigned char *lexbuf,
								  const int32 *lexlens, int nlex,
								  unsigned char *out, int outcap, int *outlen)
{
	FunctionCallInfoBaseData fcinfo;

	PG_TSVEC_ENTER();
	pg_tsvec_load_textarr(lexbuf, lexlens, nlex);
	memset(&fcinfo, 0, sizeof(fcinfo));
	fcinfo.nargs = 3;
	fcinfo.args[0].value = PointerGetDatum(pg_tsvec_mkvarlena(img, imglen));
	fcinfo.args[1].value = CharGetDatum(w);
	fcinfo.args[2].value = (Datum) 1;	/* opaque; deconstruct shim ignores */
	return pg_tsvec_result_image(tsvector_setweight_by_filter(&fcinfo), out,
								 outcap, outlen);
}

int
pg_diff_tsvec_concat(const unsigned char *a, int alen, const unsigned char *b,
					 int blen, unsigned char *out, int outcap, int *outlen)
{
	FunctionCallInfoBaseData fcinfo;

	PG_TSVEC_ENTER();
	memset(&fcinfo, 0, sizeof(fcinfo));
	fcinfo.nargs = 2;
	fcinfo.args[0].value = PointerGetDatum(pg_tsvec_mkvarlena(a, alen));
	fcinfo.args[1].value = PointerGetDatum(pg_tsvec_mkvarlena(b, blen));
	return pg_tsvec_result_image(tsvector_concat(&fcinfo), out, outcap,
								 outlen);
}

int
pg_diff_tsvec_filter(const unsigned char *img, int imglen,
					 const char *weights, const unsigned char *wnulls, int nw,
					 unsigned char *out, int outcap, int *outlen)
{
	FunctionCallInfoBaseData fcinfo;
	int			i;

	PG_TSVEC_ENTER();
	/* "char"[] element list: Datum-packed chars */
	pg_tsvec_arr_elems = pg_tsvec_palloc(sizeof(Datum) * (nw ? nw : 1));
	pg_tsvec_arr_nulls = pg_tsvec_palloc(sizeof(bool) * (nw ? nw : 1));
	pg_tsvec_arr_n = nw;
	for (i = 0; i < nw; i++)
	{
		pg_tsvec_arr_elems[i] = CharGetDatum(weights[i]);
		pg_tsvec_arr_nulls[i] = wnulls[i] != 0;
	}
	memset(&fcinfo, 0, sizeof(fcinfo));
	fcinfo.nargs = 2;
	fcinfo.args[0].value = PointerGetDatum(pg_tsvec_mkvarlena(img, imglen));
	fcinfo.args[1].value = (Datum) 1;	/* opaque; deconstruct shim ignores */
	return pg_tsvec_result_image(tsvector_filter(&fcinfo), out, outcap,
								 outlen);
}

int
pg_diff_tsvec_delete_str(const unsigned char *img, int imglen,
						 const unsigned char *lex, int lexlen,
						 unsigned char *out, int outcap, int *outlen)
{
	FunctionCallInfoBaseData fcinfo;

	PG_TSVEC_ENTER();
	memset(&fcinfo, 0, sizeof(fcinfo));
	fcinfo.nargs = 2;
	fcinfo.args[0].value = PointerGetDatum(pg_tsvec_mkvarlena(img, imglen));
	fcinfo.args[1].value =
		PointerGetDatum(cstring_to_text_with_len((const char *) lex, lexlen));
	return pg_tsvec_result_image(tsvector_delete_str(&fcinfo), out, outcap,
								 outlen);
}

int
pg_diff_tsvec_delete_arr(const unsigned char *img, int imglen,
						 const unsigned char *lexbuf, const int32 *lexlens,
						 int nlex, unsigned char *out, int outcap,
						 int *outlen)
{
	FunctionCallInfoBaseData fcinfo;

	PG_TSVEC_ENTER();
	pg_tsvec_load_textarr(lexbuf, lexlens, nlex);
	memset(&fcinfo, 0, sizeof(fcinfo));
	fcinfo.nargs = 2;
	fcinfo.args[0].value = PointerGetDatum(pg_tsvec_mkvarlena(img, imglen));
	fcinfo.args[1].value = (Datum) 1;	/* opaque; deconstruct shim ignores */
	return pg_tsvec_result_image(tsvector_delete_arr(&fcinfo), out, outcap,
								 outlen);
}

/*
 * tsvector_to_array: output serialized as [int32 n][int32 len, bytes]...
 * (element-list plane; array-image construction is the arrayfuncs crate's
 * job on both sides).
 */
int
pg_diff_tsvec_to_array(const unsigned char *img, int imglen,
					   unsigned char *out, int outcap, int *outlen)
{
	FunctionCallInfoBaseData fcinfo;
	int			off = 0;
	int			i;

	PG_TSVEC_ENTER();
	memset(&fcinfo, 0, sizeof(fcinfo));
	fcinfo.nargs = 1;
	fcinfo.args[0].value = PointerGetDatum(pg_tsvec_mkvarlena(img, imglen));
	(void) tsvector_to_array(&fcinfo);
	if (off + 4 > outcap)
		return PG_TSVEC_ECAP;
	memcpy(out + off, &pg_tsvec_outarr_n, 4);
	off += 4;
	for (i = 0; i < pg_tsvec_outarr_n; i++)
	{
		text	   *t = (text *) DatumGetPointer(pg_tsvec_outarr_elems[i]);
		int32		tl = VARSIZE(t) - VARHDRSZ;

		if (off + 4 + tl > outcap)
			return PG_TSVEC_ECAP;
		memcpy(out + off, &tl, 4);
		off += 4;
		memcpy(out + off, VARDATA(t), tl);
		off += tl;
	}
	*outlen = off;
	return 0;
}

int
pg_diff_array_to_tsvector(const unsigned char *lexbuf, const int32 *lexlens,
						  int nlex, unsigned char *out, int outcap,
						  int *outlen)
{
	FunctionCallInfoBaseData fcinfo;

	PG_TSVEC_ENTER();
	pg_tsvec_load_textarr(lexbuf, lexlens, nlex);
	memset(&fcinfo, 0, sizeof(fcinfo));
	fcinfo.nargs = 1;
	fcinfo.args[0].value = (Datum) 1;	/* opaque; deconstruct shim ignores */
	return pg_tsvec_result_image(array_to_tsvector(&fcinfo), out, outcap,
								 outlen);
}

int
pg_diff_ts_match_vq(const unsigned char *vimg, int vlen,
					const unsigned char *qimg, int qlen, int32 *res)
{
	Datum		dv,
				dq;

	PG_TSVEC_ENTER();
	dv = PointerGetDatum(pg_tsvec_mkvarlena(vimg, vlen));
	dq = PointerGetDatum(pg_tsvec_mkvarlena(qimg, qlen));
	*res = DatumGetBool(pg_tsvec_direct_call2(ts_match_vq, dv, dq)) ? 1 : 0;
	return 0;
}

/* ts_match_qv: the vendored wrapper itself (argument swap via fmgr) */
int
pg_diff_ts_match_qv(const unsigned char *qimg, int qlen,
					const unsigned char *vimg, int vlen, int32 *res)
{
	FunctionCallInfoBaseData fcinfo;

	PG_TSVEC_ENTER();
	memset(&fcinfo, 0, sizeof(fcinfo));
	fcinfo.nargs = 2;
	fcinfo.args[0].value = PointerGetDatum(pg_tsvec_mkvarlena(qimg, qlen));
	fcinfo.args[1].value = PointerGetDatum(pg_tsvec_mkvarlena(vimg, vlen));
	*res = DatumGetBool(ts_match_qv(&fcinfo)) ? 1 : 0;
	return 0;
}
