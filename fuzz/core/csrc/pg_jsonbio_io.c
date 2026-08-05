/*
 * pg_jsonbio_io.c: vendored PostgreSQL C oracle for the jsonbio_diff
 * differential fuzz target (100%-coverage campaign; crate
 * crates/backend/utils/adt/jsonb).
 *
 * Provenance: PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df),
 * re-verified against the vendored ground-truth checkout
 * ../pgrust-fabled/vendor/postgres-src. The vendored text lives in
 * csrc/jsonbfam/ — whole-file TUs included below plus extracted .inc
 * segments, each with an exact line-range provenance header:
 *   jsonbfam/jsonapi.c        src/common/jsonapi.c (whole file, verbatim)
 *   jsonbfam/wchar.c          src/common/wchar.c (whole file, verbatim)
 *   jsonbfam/stringinfo.c     src/common/stringinfo.c (whole file, verbatim)
 *   jsonbfam/jsonb_util.c     src/backend/utils/adt/jsonb_util.c (whole file)
 *   jsonbfam/qsort_arg.c      src/port/qsort_arg.c (whole file, verbatim)
 *   jsonbfam/jsonb_c.inc      jsonb.c segments (io/parse/render/casts/builders)
 *   jsonbfam/jsonfuncs_c.inc  jsonfuncs.c segments (errsave, array_length,
 *                             strip_nulls, pretty)
 *   jsonbfam/numeric_c.inc    numeric.c segments (in/out/casts/var machinery)
 *   jsonbfam/json_escape_c.inc json.c escape_json family
 *   jsonbfam/pqformat_c.inc   pqformat.c segments (recv/send plumbing)
 *   jsonbfam/mbutils_c.inc    mbutils.c pg_unicode_to_server_noerror
 * Real headers vendored verbatim under jsonbfam/include/ (utils/jsonb.h,
 * common/jsonapi.h, lib/stringinfo.h, mb/pg_wchar.h, varatt.h, utils/
 * numeric.h, common/int.h, port/{pg_bitutils,pg_lfind,simd,pg_bswap}.h,
 * lib/sort_template.h, unicode tables). Shim headers (PLUMBING ONLY, each
 * self-describing) under jsonbfam/shim/.
 *
 * SHIM LIST (environment, never computation):
 *   - palloc family -> TLS pointer arena, reset at every pg_diff_* entry
 *     (models PG memory-context reset; LSan-quiet).
 *   - ereport/errsave/ereturn/elog -> record errcode class in the shared
 *     pg_diff_errcode channel (defined in pg_float_io.c) + longjmp.
 *     escontext is always NULL (hard-error lane, like the fc calls under
 *     test). Class map in shim/postgres.h; classes 1/2 shared with
 *     pg_float_io.c because numeric_float4/8 call its verbatim
 *     float4in/8in_internal (which SOFT-set the class and return; the cast
 *     drivers check the channel after the call).
 *   - encoding pinned UTF8, no client-encoding conversion:
 *     pg_server_to_client / pg_client_to_server identity,
 *     GetDatabaseEncoding() == PG_UTF8, Utf8ToServerConvProc == NULL.
 *   - check_stack_depth/CHECK_FOR_INTERRUPTS no-ops: the Rust driver caps
 *     input length and pre-screens nesting depth (see jsonbio_diff.rs
 *     header carve note).
 *   - pg_detoast_datum: 4-byte-header passthrough / 1-byte-short copy
 *     (no toast in this oracle).
 *   - abort-loud stubs for arms unreachable from this target's entries:
 *     add_jsonb / clone_parse_state (build workers called with nargs==0
 *     only), set_var_from_non_decimal_integer_str (JSON numbers are
 *     decimal by grammar), JsonEncodeDateTime (jbvDatetime never built
 *     here), varstr_cmp / numeric_cmp / hash_any* (compare/hash arms are
 *     the ops-target's charter; abort if reached), FunctionCall6
 *     (conversion procs never loaded).
 *   - qsort_arg's med3/swap internals are verbatim (whole-file vendor);
 *     comparator ties are total (lengthCompareJsonbPair breaks by order),
 *     so sort output is implementation-independent.
 */

#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdio.h>
#include <setjmp.h>

#include "postgres.h"			/* jsonbfam/shim/postgres.h */
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "mb/pg_wchar.h"
#include "utils/builtins.h"
#include "utils/jsonb.h"
#include "utils/jsonfuncs.h"
#include "utils/json.h"
#include "utils/fmgrprotos.h"
#include "libpq/pqformat.h"
#include "common/hashfn.h"

#include "pg_oracle_guard.h"	/* oracle-serialization holder check */

/* ---------------- error channel + non-local exit ---------------- */

_Thread_local jmp_buf pg_jsonbfam_jmp;

void
pg_jsonbfam_error_raise(void)
{
	longjmp(pg_jsonbfam_jmp, 1);
}

/* ---------------- palloc arena (lanej pattern, dynamic capacity) -------- */

static _Thread_local void **pg_jsonbfam_arena;
static _Thread_local size_t pg_jsonbfam_arena_n;
static _Thread_local size_t pg_jsonbfam_arena_cap;

void
pg_jsonbfam_arena_reset(void)		/* extern: pg_jsonbops.c entries reset too */
{
	size_t		i;

	for (i = 0; i < pg_jsonbfam_arena_n; i++)
		free(pg_jsonbfam_arena[i]);
	pg_jsonbfam_arena_n = 0;
}

static void
pg_jsonbfam_arena_track(void *p)
{
	if (pg_jsonbfam_arena_n == pg_jsonbfam_arena_cap)
	{
		pg_jsonbfam_arena_cap = pg_jsonbfam_arena_cap ? pg_jsonbfam_arena_cap * 2 : 1024;
		pg_jsonbfam_arena = realloc(pg_jsonbfam_arena,
									pg_jsonbfam_arena_cap * sizeof(void *));
		assert(pg_jsonbfam_arena);
	}
	pg_jsonbfam_arena[pg_jsonbfam_arena_n++] = p;
}

void *
pg_jsonbfam_palloc(Size n)
{
	void	   *p = malloc(n ? n : 1);

	assert(p);
	pg_jsonbfam_arena_track(p);
	return p;
}

void *
pg_jsonbfam_palloc0(Size n)
{
	void	   *p = calloc(1, n ? n : 1);

	assert(p);
	pg_jsonbfam_arena_track(p);
	return p;
}

void *
pg_jsonbfam_repalloc(void *old, Size n)
{
	size_t		i;
	void	   *p = realloc(old, n);

	assert(p);
	for (i = pg_jsonbfam_arena_n; i-- > 0;)
	{
		if (pg_jsonbfam_arena[i] == old)
		{
			pg_jsonbfam_arena[i] = p;
			return p;
		}
	}
	assert(!"repalloc of a pointer the arena never issued");
	return p;
}

void
pg_jsonbfam_pfree(void *p)
{
	size_t		i;

	for (i = pg_jsonbfam_arena_n; i-- > 0;)
	{
		if (pg_jsonbfam_arena[i] == p)
		{
			free(p);
			pg_jsonbfam_arena[i] = pg_jsonbfam_arena[--pg_jsonbfam_arena_n];
			return;
		}
	}
	assert(!"pfree of a pointer the arena never issued");
	abort();
}

char *
pg_jsonbfam_pstrdup(const char *s)
{
	size_t		n = strlen(s);
	char	   *p = pg_jsonbfam_palloc(n + 1);

	memcpy(p, s, n + 1);
	return p;
}

/* pnstrdup: verbatim semantics of src/common/fe_memutils/mcxt pnstrdup */
static char *
pnstrdup(const char *in, Size len)
{
	char	   *out;
	size_t		n;

	n = strnlen(in, len);
	out = pg_jsonbfam_palloc(n + 1);
	memcpy(out, in, n);
	out[n] = '\0';
	return out;
}

/* psprintf: plumbing (message text out of scope) */
char *
psprintf(const char *fmt,...)
{
	char		buf[1024];
	va_list		ap;
	int			n;

	va_start(ap, fmt);
	n = vsnprintf(buf, sizeof(buf), fmt, ap);
	va_end(ap);
	if (n < 0)
		n = 0;
	if ((size_t) n >= sizeof(buf))
		n = sizeof(buf) - 1;
	return pnstrdup(buf, n);
}

/* pvsnprintf for stringinfo.c appendStringInfoVA: plain vsnprintf plumbing */
size_t
pvsnprintf(char *buf, size_t len, const char *fmt, va_list args)
{
	int			n = vsnprintf(buf, len, fmt, args);

	if (n < 0)
		return len + 64;		/* ask for more, matches pvsnprintf contract */
	return (size_t) n;
}

/* ---------------- detoast (no toast in this oracle) ---------------- */

struct varlena *
pg_detoast_datum(struct varlena *datum)
{
	if (VARATT_IS_SHORT(datum))
	{
		Size		data_size = VARSIZE_SHORT(datum) - VARHDRSZ_SHORT;
		struct varlena *result = pg_jsonbfam_palloc(data_size + VARHDRSZ);

		SET_VARSIZE(result, data_size + VARHDRSZ);
		memcpy(VARDATA(result), VARDATA_SHORT(datum), data_size);
		return result;
	}
	assert(!VARATT_IS_EXTENDED(datum));
	return datum;
}

struct varlena *
pg_detoast_datum_copy(struct varlena *datum)
{
	if (VARATT_IS_SHORT(datum))
		return pg_detoast_datum(datum);
	assert(!VARATT_IS_EXTENDED(datum));
	{
		Size		len = VARSIZE(datum);
		struct varlena *result = pg_jsonbfam_palloc(len);

		memcpy(result, datum, len);
		return result;
	}
}

struct varlena *
pg_detoast_datum_packed(struct varlena *datum)
{
	assert(VARATT_IS_SHORT(datum) || !VARATT_IS_EXTENDED(datum));
	return datum;				/* short headers OK for _packed */
}

/* ---------------- text helpers (varlena.c cores, verbatim logic) ------- */

text *
cstring_to_text_with_len(const char *s, int len)
{
	text	   *result = (text *) pg_jsonbfam_palloc(len + VARHDRSZ);

	SET_VARSIZE(result, len + VARHDRSZ);
	memcpy(VARDATA(result), s, len);
	return result;
}

text *
cstring_to_text(const char *s)
{
	return cstring_to_text_with_len(s, strlen(s));
}

char *
text_to_cstring(const text *t)
{
	text	   *tunpacked = pg_detoast_datum_packed((struct varlena *) t);
	int			len = VARSIZE_ANY_EXHDR(tunpacked);
	char	   *result;

	result = (char *) pg_jsonbfam_palloc(len + 1);
	memcpy(result, VARDATA_ANY(tunpacked), len);
	result[len] = '\0';
	return result;
}

/* ---------------- encoding environment (UTF8 pin) ---------------- */

int
GetDatabaseEncoding(void)
{
	return PG_UTF8;
}

const char *
GetDatabaseEncodingName(void)
{
	return "UTF8";
}

/* no client encoding conversion in this oracle: identity */
char *
pg_server_to_client(const char *s, int len)
{
	(void) len;
	return (char *) s;
}

char *
pg_client_to_server(const char *s, int len)
{
	(void) len;
	return (char *) s;
}

/* conversion procs never loaded (UTF8 pin) */
static void *const Utf8ToServerConvProc = NULL;

static Datum
FunctionCall6(void *flinfo, Datum a, Datum b, Datum c, Datum d, Datum e,
			  Datum f)
{
	(void) flinfo; (void) a; (void) b; (void) c; (void) d; (void) e; (void) f;
	abort();					/* unreachable: Utf8ToServerConvProc == NULL */
}

/* ---------------- DirectFunctionCallN (fmgr.c semantics) ---------------- */

Datum
DirectFunctionCall1Coll(PGFunction func, Oid collation, Datum arg1)
{
	LOCAL_FCINFO(fcinfo, 1);
	Datum		result;

	InitFunctionCallInfoData(*fcinfo, NULL, 1, collation, NULL, NULL);
	fcinfo->args[0].value = arg1;
	fcinfo->args[0].isnull = false;
	result = (*func) (fcinfo);
	if (fcinfo->isnull)
		elog(ERROR, "function returned NULL");
	return result;
}

Datum
DirectFunctionCall2Coll(PGFunction func, Oid collation, Datum arg1, Datum arg2)
{
	LOCAL_FCINFO(fcinfo, 2);
	Datum		result;

	InitFunctionCallInfoData(*fcinfo, NULL, 2, collation, NULL, NULL);
	fcinfo->args[0].value = arg1;
	fcinfo->args[0].isnull = false;
	fcinfo->args[1].value = arg2;
	fcinfo->args[1].isnull = false;
	result = (*func) (fcinfo);
	if (fcinfo->isnull)
		elog(ERROR, "function returned NULL");
	return result;
}

Datum
DirectFunctionCall3Coll(PGFunction func, Oid collation, Datum arg1, Datum arg2,
						Datum arg3)
{
	LOCAL_FCINFO(fcinfo, 3);
	Datum		result;

	InitFunctionCallInfoData(*fcinfo, NULL, 3, collation, NULL, NULL);
	fcinfo->args[0].value = arg1;
	fcinfo->args[0].isnull = false;
	fcinfo->args[1].value = arg2;
	fcinfo->args[1].isnull = false;
	fcinfo->args[2].value = arg3;
	fcinfo->args[2].isnull = false;
	result = (*func) (fcinfo);
	if (fcinfo->isnull)
		elog(ERROR, "function returned NULL");
	return result;
}

/* ---------------- pg_strncasecmp (src/port/pgstrcasecmp.c, verbatim) ---- */

static int
pg_strncasecmp(const char *s1, const char *s2, size_t n)
{
	while (n-- > 0)
	{
		unsigned char ch1 = (unsigned char) *s1++;
		unsigned char ch2 = (unsigned char) *s2++;

		if (ch1 != ch2)
		{
			if (ch1 >= 'A' && ch1 <= 'Z')
				ch1 += 'a' - 'A';
			else if (IS_HIGHBIT_SET(ch1) && isupper(ch1))
				ch1 = tolower(ch1);

			if (ch2 >= 'A' && ch2 <= 'Z')
				ch2 += 'a' - 'A';
			else if (IS_HIGHBIT_SET(ch2) && isupper(ch2))
				ch2 = tolower(ch2);

			if (ch1 != ch2)
				return (int) ch1 - (int) ch2;
		}
		if (ch1 == 0)
			break;
	}
	return 0;
}

/* ---------------- abort-loud stubs (unreachable arms) ---------------- */

/* compare/hash arms (varstr_cmp / numeric_cmp / numeric_eq / hash_numeric
 * / hash_any) were abort-loud stubs while only jsonbio_diff existed; the
 * jsonbops_diff extension vendors the real bodies: varlena_cmp_c.inc +
 * numeric_cmp_c.inc below in this TU (the cmp_* helpers are static in
 * numeric.c and numeric_c.inc carries their file-head decls), hashfn.c as
 * its own family TU with the real common/hashfn.h header. */

/* jbvDatetime is never built by this target's entry points */
void
JsonEncodeDateTime(char *buf, Datum value, Oid typid, const int *tzp)
{
	(void) buf; (void) value; (void) typid; (void) tzp;
	abort();
}


/* float.c float8in/float4in fmgr bodies (VERBATIM from
 * src/backend/utils/adt/float.c @ 62d6c7d3df) over the shared verbatim
 * float8in_internal/float4in_internal in csrc/pg_float_io.c. */
Datum
float8in(PG_FUNCTION_ARGS)
{
	char	   *num = PG_GETARG_CSTRING(0);

	PG_RETURN_FLOAT8(float8in_internal(num, NULL, "double precision", num,
									   fcinfo->context));
}

Datum
float4in(PG_FUNCTION_ARGS)
{
	char	   *num = PG_GETARG_CSTRING(0);

	PG_RETURN_FLOAT4(float4in_internal(num, NULL, "real", num,
									   fcinfo->context));
}

/* VERBATIM from src/backend/access/hash/hashfunc.c @ 62d6c7d3df (lines
 * 47-57): the jbvBool hash arm (JsonbHashScalarValue[Extended]) calls these
 * via DirectFunctionCall; hash_uint32[_extended] are the real
 * common/hashfn.h inlines over jsonbfam/hashfn.c. */
Datum
hashchar(PG_FUNCTION_ARGS)
{
	return hash_uint32((int32) PG_GETARG_CHAR(0));
}

Datum
hashcharextended(PG_FUNCTION_ARGS)
{
	return hash_uint32_extended((int32) PG_GETARG_CHAR(0), PG_GETARG_INT64(1));
}


/* fmgr.c DirectInputFunctionCallSafe (verbatim semantics; escontext is
 * always NULL here so soft errors become hard longjmps). */
bool
DirectInputFunctionCallSafe(PGFunction func, char *str,
							Oid typioparam, int32 typmod,
							Node *escontext, Datum *result)
{
	LOCAL_FCINFO(fcinfo, 3);

	if (str == NULL)
	{
		*result = (Datum) 0;
		return true;
	}
	InitFunctionCallInfoData(*fcinfo, NULL, 3, InvalidOid, escontext, NULL);
	fcinfo->args[0].value = CStringGetDatum(str);
	fcinfo->args[0].isnull = false;
	fcinfo->args[1].value = ObjectIdGetDatum(typioparam);
	fcinfo->args[1].isnull = false;
	fcinfo->args[2].value = Int32GetDatum(typmod);
	fcinfo->args[2].isnull = false;
	*result = (*func) (fcinfo);
	if (SOFT_ERROR_OCCURRED(escontext))
		return false;
	if (fcinfo->isnull)
		elog(ERROR, "input function returned NULL");
	return true;
}


/* mbutils.c pg_mblen_range under the UTF8 pin (DatabaseEncoding == PG_UTF8;
 * VALGRIND client checks dropped — plumbing). Used only by
 * report_json_context; the invalid-encoding arm raises loud. */
int
pg_mblen_range(const char *mbstr, const char *end)
{
	int			length = pg_wchar_table[PG_UTF8].mblen((const unsigned char *) mbstr);

	if (unlikely(mbstr + length > end))
	{
		pg_diff_errcode = ERRCODE_INTERNAL_ERROR;
		pg_jsonbfam_error_raise();
	}
	return length;
}

/* ==================== vendored PostgreSQL text ==================== */

#include "jsonbfam/numeric_c.inc"
#include "jsonbfam/numeric_cmp_c.inc"
#include "utils/pg_locale.h"
#include "jsonbfam/varlena_cmp_c.inc"

/* numeric_c.inc forward-declares set_var_from_non_decimal_integer_str;
 * JSON number tokens are decimal by grammar, so the arm is unreachable. */
static bool set_var_from_non_decimal_integer_str(const char *str,
												 const char *cp, int sign,
												 int base, NumericVar *dest,
												 const char **endptr,
												 Node *escontext)
{
	(void) str; (void) cp; (void) sign; (void) base; (void) dest;
	(void) escontext;
	abort();
}

#include "port/simd.h"
#include "port/pg_lfind.h"
#include "jsonbfam/json_escape_c.inc"
#include "jsonbfam/jsonb_c.inc"

/* jsonb_c.inc forward-declares these; unreachable from this target
 * (build workers only ever run with nargs == 0). */
static void
add_jsonb(Datum val, bool is_null, JsonbInState *result,
		  Oid val_type, bool key_scalar)
{
	(void) val; (void) is_null; (void) result; (void) val_type;
	(void) key_scalar;
	abort();
}

static JsonbParseState *
clone_parse_state(JsonbParseState *state)
{
	(void) state;
	abort();
}

/* datum_to_jsonb_internal decl in jsonb_c.inc: referenced only by add_jsonb
 * (itself an abort stub); satisfy the linker the same way. */
static void
datum_to_jsonb_internal(Datum val, bool is_null, JsonbInState *result,
						JsonTypeCategory tcategory, Oid outfuncoid,
						bool key_scalar)
{
	(void) val; (void) is_null; (void) result; (void) tcategory;
	(void) outfuncoid; (void) key_scalar;
	abort();
}

/* static decl from jsonfuncs.c file head (extraction omits the head) */
static int report_json_context(JsonLexContext *lex);

#include "jsonbfam/jsonfuncs_c.inc"
#include "port/pg_bswap.h"
#include "jsonbfam/pqformat_c.inc"
#include "jsonbfam/mbutils_c.inc"

/* ========== fuzz-facing driver entries (NOT Postgres code) ========== */

/*
 * Error-class contract with jsonbio_diff.rs:
 *   0    success
 *   >0   error class per shim/postgres.h (1=22P02, 2=22003, 3=22023,
 *        4=54000, 5=22P05, 6=internal, 7=08P01, 9=22025)
 *   100+ parse-stage failure inside a derived-op entry (class + 100):
 *        the Rust arm only drives these after ITS parse succeeded, so any
 *        100+ return is itself a divergence.
 *   -1   SQL NULL result (cast arms: jsonb null)
 * All entries: arena+errcode reset, setjmp trampoline first.
 */

#define PG_JSONBFAM_ENTER() \
	do { \
		pg_jsonbfam_arena_reset(); \
		pg_diff_errcode = 0; \
		if (setjmp(pg_jsonbfam_jmp) != 0) \
			return pg_diff_errcode ? pg_diff_errcode : ERRCODE_INTERNAL_ERROR; \
	} while (0)

/* call the verbatim fmgr body with one cstring/pointer arg */
static Datum
pg_jsonbfam_call1(PGFunction fn, Datum arg0, bool *isnull)
{
	LOCAL_FCINFO(fcinfo, 1);
	Datum		d;

	InitFunctionCallInfoData(*fcinfo, NULL, 1, InvalidOid, NULL, NULL);
	fcinfo->args[0].value = arg0;
	fcinfo->args[0].isnull = false;
	d = fn(fcinfo);
	*isnull = fcinfo->isnull;
	return d;
}

/*
 * -2 = buffer too small; *outlen carries the needed length and the Rust
 * driver retries the whole entry with bigger buffers (a <=2KB input can
 * legally render tens of MB: numeric exponents up to ~1e131071).
 */
static int
pg_jsonbfam_copyout(const void *src, int len, unsigned char *buf, int cap,
					int *outlen)
{
	if (len > cap)
	{
		*outlen = len;
		return -2;
	}
	memcpy(buf, src, len);
	*outlen = len;
	return 0;
}

/*
 * jsonb_in + jsonb_out + jsonb_send in one pass.
 * img = jsonb payload after the 4-byte varlena header (the root container),
 * out = jsonb_out text (no NUL), send = jsonb_send bytea payload.
 */
int
pg_diff_jsonb_in_full(const char *str,
					  unsigned char *img, int imgcap, int *imglen,
					  unsigned char *out, int outcap, int *outlen,
					  unsigned char *send, int sendcap, int *sendlen)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	Datum		jb;
	Datum		txt;
	Datum		snd;
	bool		isnull;

	PG_JSONBFAM_ENTER();
	jb = pg_jsonbfam_call1(jsonb_in, CStringGetDatum(str), &isnull);
	{
		struct varlena *v = (struct varlena *) DatumGetPointer(jb);

		if (pg_jsonbfam_copyout(VARDATA(v), VARSIZE(v) - VARHDRSZ, img,
								imgcap, imglen) != 0)
			return -2;
	}
	txt = pg_jsonbfam_call1(jsonb_out, jb, &isnull);
	if (pg_jsonbfam_copyout(DatumGetCString(txt),
							strlen(DatumGetCString(txt)), out, outcap,
							outlen) != 0)
		return -2;
	snd = pg_jsonbfam_call1(jsonb_send, jb, &isnull);
	{
		struct varlena *v = (struct varlena *) DatumGetPointer(snd);

		if (pg_jsonbfam_copyout(VARDATA(v), VARSIZE(v) - VARHDRSZ, send,
								sendcap, sendlen) != 0)
			return -2;
	}
	return 0;
}

/* jsonb_recv over raw wire bytes (StringInfo cursor semantics verbatim) */
int
pg_diff_jsonb_recv(const unsigned char *wire, int wirelen,
				   unsigned char *img, int imgcap, int *imglen)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	StringInfoData buf;
	Datum		jb;
	bool		isnull;

	PG_JSONBFAM_ENTER();
	buf.data = (char *) wire;
	buf.len = wirelen;
	buf.maxlen = wirelen;
	buf.cursor = 0;
	jb = pg_jsonbfam_call1(jsonb_recv, PointerGetDatum(&buf), &isnull);
	{
		struct varlena *v = (struct varlena *) DatumGetPointer(jb);

		if (pg_jsonbfam_copyout(VARDATA(v), VARSIZE(v) - VARHDRSZ, img,
								imgcap, imglen) != 0)
			return -2;
	}
	return 0;
}

/* parse helper for derived-op entries: 100+class on parse failure */
static _Thread_local Datum pg_jsonbfam_parsed;

static int
pg_jsonbfam_parse(const char *str)
{
	bool		isnull;

	pg_jsonbfam_arena_reset();
	pg_diff_errcode = 0;
	if (setjmp(pg_jsonbfam_jmp) != 0)
		return 100 + (pg_diff_errcode ? pg_diff_errcode : ERRCODE_INTERNAL_ERROR);
	pg_jsonbfam_parsed = pg_jsonbfam_call1(jsonb_in, CStringGetDatum(str),
										   &isnull);
	return 0;
}

/*
 * Single-doc derived ops. op: 0 typeof (text out), 1 array_length (i32 out),
 * 2 pretty (text out), 3 strip_nulls (jsonb payload out).
 */
int
pg_diff_jsonb_op1(int op, int flag, const char *str,
				  unsigned char *out, int outcap, int *outlen)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	int			rc = pg_jsonbfam_parse(str);
	Datum		d;
	bool		isnull;

	if (rc != 0)
		return rc;
	if (setjmp(pg_jsonbfam_jmp) != 0)
		return pg_diff_errcode ? pg_diff_errcode : ERRCODE_INTERNAL_ERROR;
	switch (op)
	{
		case 0:
			d = pg_jsonbfam_call1(jsonb_typeof, pg_jsonbfam_parsed, &isnull);
			return pg_jsonbfam_copyout(VARDATA_ANY(DatumGetPointer(d)),
									   VARSIZE_ANY_EXHDR(DatumGetPointer(d)),
									   out, outcap, outlen);
		case 1:
			{
				int32		n;

				d = pg_jsonbfam_call1(jsonb_array_length, pg_jsonbfam_parsed,
									  &isnull);
				n = DatumGetInt32(d);
				return pg_jsonbfam_copyout(&n, 4, out, outcap, outlen);
			}
		case 2:
			d = pg_jsonbfam_call1(jsonb_pretty, pg_jsonbfam_parsed, &isnull);
			return pg_jsonbfam_copyout(VARDATA_ANY(DatumGetPointer(d)),
									   VARSIZE_ANY_EXHDR(DatumGetPointer(d)),
									   out, outcap, outlen);
		case 3:
			{
				struct varlena *v;
				/* catalog form is 2-arg (strip_in_arrays bool, PG17+) */
				LOCAL_FCINFO(fcinfo2, 2);

				InitFunctionCallInfoData(*fcinfo2, NULL, 2, InvalidOid, NULL,
										 NULL);
				fcinfo2->args[0].value = pg_jsonbfam_parsed;
				fcinfo2->args[0].isnull = false;
				fcinfo2->args[1].value = BoolGetDatum(flag != 0);
				fcinfo2->args[1].isnull = false;
				d = jsonb_strip_nulls(fcinfo2);
				isnull = fcinfo2->isnull;
				v = (struct varlena *) DatumGetPointer(d);
				return pg_jsonbfam_copyout(VARDATA(v), VARSIZE(v) - VARHDRSZ,
										   out, outcap, outlen);
			}
		default:
			abort();
	}
}

/*
 * Cast arms. which: 0 bool (1 byte), 1 int2 (2B le), 2 int4 (4B le),
 * 3 int8 (8B le), 4 float4 (raw 4B le bits), 5 float8 (raw 8B le bits),
 * 6 numeric (numeric_out text). Returns -1 for SQL NULL (jsonb null).
 */
int
pg_diff_jsonb_cast(int which, const char *str,
				   unsigned char *out, int outcap, int *outlen)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	static const PGFunction casts[] = {
		jsonb_bool, jsonb_int2, jsonb_int4, jsonb_int8,
		jsonb_float4, jsonb_float8, jsonb_numeric,
	};
	int			rc = pg_jsonbfam_parse(str);
	Datum		d;
	bool		isnull;

	if (rc != 0)
		return rc;
	if (setjmp(pg_jsonbfam_jmp) != 0)
		return pg_diff_errcode ? pg_diff_errcode : ERRCODE_INTERNAL_ERROR;
	d = pg_jsonbfam_call1(casts[which], pg_jsonbfam_parsed, &isnull);
	/* float4in/8in_internal (pg_float_io.c) soft-set the shared channel */
	if (pg_diff_errcode != 0)
		return pg_diff_errcode;
	if (isnull)
		return -1;
	switch (which)
	{
		case 0:
			{
				unsigned char b = DatumGetBool(d) ? 1 : 0;

				return pg_jsonbfam_copyout(&b, 1, out, outcap, outlen);
			}
		case 1:
			{
				int16		v = DatumGetInt16(d);

				return pg_jsonbfam_copyout(&v, 2, out, outcap, outlen);
			}
		case 2:
			{
				int32		v = DatumGetInt32(d);

				return pg_jsonbfam_copyout(&v, 4, out, outcap, outlen);
			}
		case 3:
			{
				int64		v = DatumGetInt64(d);

				return pg_jsonbfam_copyout(&v, 8, out, outcap, outlen);
			}
		case 4:
			{
				float4		v = DatumGetFloat4(d);

				return pg_jsonbfam_copyout(&v, 4, out, outcap, outlen);
			}
		case 5:
			{
				float8		v = DatumGetFloat8(d);

				return pg_jsonbfam_copyout(&v, 8, out, outcap, outlen);
			}
		case 6:
			{
				Datum		t = DirectFunctionCall1(numeric_out, d);

				return pg_jsonbfam_copyout(DatumGetCString(t),
										   strlen(DatumGetCString(t)),
										   out, outcap, outlen);
			}
		default:
			abort();
	}
}

/* jsonb_build_array_noargs / jsonb_build_object_noargs fixed arms */
int
pg_diff_jsonb_build_noargs(int isobj,
						   unsigned char *img, int imgcap, int *imglen)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	LOCAL_FCINFO(fcinfo, 0);
	Datum		d;
	struct varlena *v;

	PG_JSONBFAM_ENTER();
	InitFunctionCallInfoData(*fcinfo, NULL, 0, InvalidOid, NULL, NULL);
	d = isobj ? jsonb_build_object_noargs(fcinfo)
		: jsonb_build_array_noargs(fcinfo);
	v = (struct varlena *) DatumGetPointer(d);
	return pg_jsonbfam_copyout(VARDATA(v), VARSIZE(v) - VARHDRSZ, img, imgcap,
							   imglen);
}
