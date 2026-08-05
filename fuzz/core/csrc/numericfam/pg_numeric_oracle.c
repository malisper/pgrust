/*
 * Vendored PostgreSQL C: NUMERIC — differential-fuzz oracle.
 *
 * Provenance:
 *   - vendor/numeric.c: UNMODIFIED vendored REL_18_3
 *     src/backend/utils/adt/numeric.c (byte-identical copy of
 *     bench/cref/numeric_vendor/numeric.c, itself vendored from postgres-src
 *     62d6c7d3df6287f1bd83199c1a746e50d31571a0 / Stamp 18.3). #include'd
 *     whole-file below (cref_numeric precedent) so the oracle drives the
 *     exact shipped-reference bodies including statics.
 *   - vendor/common/hashfn.c: UNMODIFIED vendored REL_18_3
 *     src/common/hashfn.c (hash_bytes / hash_bytes_extended for
 *     hash_numeric / hash_numeric_extended).
 *   - vendor/postgres.h + vendor/{common,lib,libpq,nodes,utils,...}/*.h:
 *     the bench/cref/numeric_vendor shim environment (types/macros are
 *     value-exact copies of the PG headers they stand in for), with ONE
 *     divergence from the bench copy: the error macros CAPTURE the real
 *     MAKE_SQLSTATE errcode in a thread-local and longjmp back to the
 *     entry wrapper instead of aborting (see the FUZZ-ORACLE ERROR CAPTURE
 *     block in vendor/postgres.h). Plumbing only, never logic.
 *
 * Shims implemented here (all nv_-prefixed via vendor/postgres.h defines):
 *   - palloc/palloc0/pfree/pstrdup: per-call bump arena (reset at every
 *     pg_diff_num_call entry; PG frees by context reset, not pfree).
 *   - pq_begintypsend/pq_endtypsend/pq_sendint16/32: StringInfo over the
 *     arena, big-endian appends — value-exact vs pqformat.h inlines.
 *   - pq_getmsgint(16/32)/pq_getmsgend/initReadOnlyStringInfo: big-endian
 *     reads with pqformat.c's insufficient-data / trailing-garbage
 *     ereports (ERRCODE_PROTOCOL_VIOLATION), value-exact.
 *   - float4in/float8in: fcinfo wrappers over the fuzz workspace's
 *     verbatim float4in_internal/float8in_internal (pg_float_io.c);
 *     their thread-local error class is translated to the real sqlstates
 *     (22P02/22003) numeric.c's callers would surface.
 *   - hash_any/hash_any_extended: real vendored hash_bytes(+_extended).
 *   - DirectFunctionCall1Coll/2Coll: standard fcinfo trampoline (strict
 *     NULL handling not needed: numeric.c only calls it non-null).
 *   - ArrayGetIntegerTypmods: staged-int seam (mock the ENVIRONMENT rule:
 *     cstring-array decoding belongs to adt/arrayutils, not numeric.c;
 *     the numeric-owned logic — precision/scale validation and packing —
 *     runs verbatim on both sides).
 *   - AggCheckCallContext / SRF / planner-support / hyperloglog / prng /
 *     sortsupport externs: aborting stubs — those surfaces are carved
 *     (state/SRF/PRNG/planner) and the drivers never reach them.
 *
 * Comparison planes (driver contract): result bytes (numeric varlena
 * image / cstring / bytea / scalar) + error verdict + sqlstate. Message
 * text never crosses the seam.
 */

#include <setjmp.h>
#include <stdio.h>

#include "postgres.h"

#include "funcapi.h"
#include "lib/hyperloglog.h"
#include "libpq/pqformat.h"
#include "utils/array.h"
#include "utils/numeric.h"

/* ---------- error capture (declared in vendor/postgres.h) ---------- */

_Thread_local int nfz_sqlstate;
static _Thread_local jmp_buf nfz_jmp;
static _Thread_local int nfz_jmp_armed;

void
nfz_raise(void)
{
	if (!nfz_jmp_armed)
	{
		fprintf(stderr, "pg_numeric_oracle: error raised outside pg_diff_num_call\n");
		abort();
	}
	longjmp(nfz_jmp, 1);
}

/* ---------- per-call bump arena ---------- */

#define NFZ_ARENA_SZ (32u << 20)
static _Thread_local char *nfz_arena;
static _Thread_local size_t nfz_arena_off;

void *
nv_palloc(Size sz)
{
	size_t		off;

	if (nfz_arena == NULL)
		nfz_arena = malloc(NFZ_ARENA_SZ);
	off = (nfz_arena_off + 7) & ~(size_t) 7;
	if (off + sz > NFZ_ARENA_SZ)
	{
		/* driver caps operand sizes; treat exhaustion as a harness bug */
		fprintf(stderr, "pg_numeric_oracle: arena exhausted (%zu + %zu)\n", off, (size_t) sz);
		abort();
	}
	nfz_arena_off = off + sz;
	return nfz_arena + off;
}

void *
nv_palloc0(Size sz)
{
	void	   *p = nv_palloc(sz);

	memset(p, 0, sz);
	return p;
}

void
nv_pfree(void *p)
{
	(void) p;
}

char *
nv_pstrdup(const char *s)
{
	size_t		n = strlen(s) + 1;
	char	   *p = nv_palloc(n);

	memcpy(p, s, n);
	return p;
}

MemoryContext nv_CurrentMemoryContext = (MemoryContext) 1;

MemoryContext
nv_MemoryContextSwitchTo(MemoryContext ctx)
{
	MemoryContext old = nv_CurrentMemoryContext;

	nv_CurrentMemoryContext = ctx;
	return old;
}

/* ---------- pg_strcasecmp / pg_strncasecmp (src/port/pgstrcasecmp.c,
 * ASCII arms — value-exact for the "NaN"/"Infinity" keywords numeric.c
 * compares; the fuzz process locale is C) ---------- */

int
nv_pg_strncasecmp(const char *s1, const char *s2, size_t n)
{
	while (n-- > 0)
	{
		unsigned char ch1 = (unsigned char) *s1++;
		unsigned char ch2 = (unsigned char) *s2++;

		if (ch1 != ch2)
		{
			if (ch1 >= 'A' && ch1 <= 'Z')
				ch1 += 'a' - 'A';
			if (ch2 >= 'A' && ch2 <= 'Z')
				ch2 += 'a' - 'A';
			if (ch1 != ch2)
				return (int) ch1 - (int) ch2;
		}
		if (ch1 == 0)
			break;
	}
	return 0;
}

int
nv_pg_strcasecmp(const char *s1, const char *s2)
{
	for (;;)
	{
		unsigned char ch1 = (unsigned char) *s1++;
		unsigned char ch2 = (unsigned char) *s2++;

		if (ch1 != ch2)
		{
			if (ch1 >= 'A' && ch1 <= 'Z')
				ch1 += 'a' - 'A';
			if (ch2 >= 'A' && ch2 <= 'Z')
				ch2 += 'a' - 'A';
			if (ch1 != ch2)
				return (int) ch1 - (int) ch2;
		}
		if (ch1 == 0)
			return 0;
	}
}

/* ---------- pqformat shims (value-exact big-endian semantics) ---------- */

void
nv_pq_begintypsend(StringInfo buf)
{
	buf->maxlen = 1024;
	buf->data = nv_palloc(buf->maxlen);
	buf->len = 0;
	buf->cursor = 0;
}

static void
nfz_pq_grow(StringInfo buf, int need)
{
	if (buf->len + need > buf->maxlen)
	{
		int			newmax = buf->maxlen * 2;
		char	   *nd;

		while (buf->len + need > newmax)
			newmax *= 2;
		nd = nv_palloc(newmax);
		memcpy(nd, buf->data, buf->len);
		buf->data = nd;
		buf->maxlen = newmax;
	}
}

void
nv_pq_sendint16(StringInfo buf, uint16 i)
{
	nfz_pq_grow(buf, 2);
	buf->data[buf->len++] = (char) (i >> 8);
	buf->data[buf->len++] = (char) i;
}

void
nv_pq_sendint32(StringInfo buf, uint32 i)
{
	nfz_pq_grow(buf, 4);
	buf->data[buf->len++] = (char) (i >> 24);
	buf->data[buf->len++] = (char) (i >> 16);
	buf->data[buf->len++] = (char) (i >> 8);
	buf->data[buf->len++] = (char) i;
}

void
nv_pq_sendint64(StringInfo buf, uint64 i)
{
	nv_pq_sendint32(buf, (uint32) (i >> 32));
	nv_pq_sendint32(buf, (uint32) i);
}

bytea *
nv_pq_endtypsend(StringInfo buf)
{
	bytea	   *result = nv_palloc(VARHDRSZ + buf->len);

	SET_VARSIZE(result, VARHDRSZ + buf->len);
	memcpy(VARDATA(result), buf->data, buf->len);
	return result;
}

void
nv_initReadOnlyStringInfo(StringInfo str, char *data, int len)
{
	str->data = data;
	str->len = len;
	str->maxlen = 0;			/* read-only */
	str->cursor = 0;
}

/* pq_getmsgbytes' insufficient-data check, pqformat.c value-exact */
static const char *
nfz_getmsgbytes(StringInfo msg, int datalen)
{
	const char *res;

	if (datalen < 0 || datalen > (msg->len - msg->cursor))
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("insufficient data left in message")));
	res = &msg->data[msg->cursor];
	msg->cursor += datalen;
	return res;
}

unsigned int
nv_pq_getmsgint(StringInfo msg, int b)
{
	unsigned int result;
	const char *p;

	switch (b)
	{
		case 1:
			p = nfz_getmsgbytes(msg, 1);
			result = (unsigned char) p[0];
			break;
		case 2:
			p = nfz_getmsgbytes(msg, 2);
			result = ((unsigned int) (unsigned char) p[0] << 8) |
				(unsigned int) (unsigned char) p[1];
			break;
		case 4:
			p = nfz_getmsgbytes(msg, 4);
			result = ((unsigned int) (unsigned char) p[0] << 24) |
				((unsigned int) (unsigned char) p[1] << 16) |
				((unsigned int) (unsigned char) p[2] << 8) |
				(unsigned int) (unsigned char) p[3];
			break;
		default:
			elog(ERROR, "unsupported integer size");
			result = 0;
			break;
	}
	return result;
}

int64
nv_pq_getmsgint64(StringInfo msg)
{
	uint64		hi = nv_pq_getmsgint(msg, 4);
	uint64		lo = nv_pq_getmsgint(msg, 4);

	return (int64) ((hi << 32) | lo);
}

void
nv_pq_getmsgend(StringInfo msg)
{
	if (msg->cursor != msg->len)
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("invalid message format")));
}

/* ---------- hash: real vendored hash_bytes ---------- */

extern uint32 hash_bytes(const unsigned char *k, int keylen);
extern uint64 hash_bytes_extended(const unsigned char *k, int keylen, uint64 seed);

Datum
nv_hash_any(const unsigned char *k, int keylen)
{
	return UInt32GetDatum(hash_bytes(k, keylen));
}

Datum
nv_hash_any_extended(const unsigned char *k, int keylen, uint64 seed)
{
	return UInt64GetDatum(hash_bytes_extended(k, keylen, seed));
}

uint32
nv_hash_uint32(uint32 k)
{
	extern uint32 hash_bytes_uint32(uint32 k);

	return hash_bytes_uint32(k);
}

Datum
nv_hash_uint32_extended(uint32 k, uint64 seed)
{
	extern uint64 hash_bytes_uint32_extended(uint32 k, uint64 seed);

	return UInt64GetDatum(hash_bytes_uint32_extended(k, seed));
}

/* ---------- DirectFunctionCall trampolines ---------- */

Datum
nv_DirectFunctionCall1Coll(PGFunction func, Oid collation, Datum arg1)
{
	LOCAL_FCINFO(fcinfo, 1);
	Datum		result;

	memset(fcinfo, 0, sizeof(*fcinfo));
	fcinfo->fncollation = collation;
	fcinfo->nargs = 1;
	fcinfo->args[0].value = arg1;
	fcinfo->args[0].isnull = false;
	result = (*func) (fcinfo);
	if (fcinfo->isnull)
		elog(ERROR, "function returned NULL");
	return result;
}

Datum
nv_DirectFunctionCall2Coll(PGFunction func, Oid collation, Datum arg1, Datum arg2)
{
	LOCAL_FCINFO(fcinfo, 2);
	Datum		result;

	memset(fcinfo, 0, sizeof(*fcinfo));
	fcinfo->fncollation = collation;
	fcinfo->nargs = 2;
	fcinfo->args[0].value = arg1;
	fcinfo->args[0].isnull = false;
	fcinfo->args[1].value = arg2;
	fcinfo->args[1].isnull = false;
	result = (*func) (fcinfo);
	if (fcinfo->isnull)
		elog(ERROR, "function returned NULL");
	return result;
}

/* ---------- float4in / float8in over the verbatim pg_float_io cores ----- */

extern float float4in_internal(char *num, char **endptr_p,
							   const char *type_name, const char *orig_string,
							   struct Node *escontext);
extern double float8in_internal(char *num, char **endptr_p,
								const char *type_name, const char *orig_string,
								struct Node *escontext);
extern int pg_diff_errcode_get(void);
extern _Thread_local int pg_diff_errcode;

static void
nfz_float_err_translate(void)
{
	int			ec = pg_diff_errcode_get();

	if (ec == 0)
		return;
	pg_diff_errcode = 0;
	if (ec == 2)				/* pg_float_io's NUMERIC_VALUE_OUT_OF_RANGE */
		ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("")));
	ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION), errmsg("")));
}

Datum
nv_float4in(FunctionCallInfo fcinfo)
{
	char	   *num = PG_GETARG_CSTRING(0);
	float		r;

	pg_diff_errcode = 0;
	r = float4in_internal(num, NULL, "real", num, NULL);
	nfz_float_err_translate();
	PG_RETURN_FLOAT4(r);
}

Datum
nv_float8in(FunctionCallInfo fcinfo)
{
	char	   *num = PG_GETARG_CSTRING(0);
	double		r;

	pg_diff_errcode = 0;
	r = float8in_internal(num, NULL, "double precision", num, NULL);
	nfz_float_err_translate();
	PG_RETURN_FLOAT8(r);
}

/* ---------- ArrayGetIntegerTypmods: staged-int seam ---------- */

static _Thread_local int32 nfz_typmod_ints[8];
static _Thread_local int nfz_typmod_n;

int32 *
nv_ArrayGetIntegerTypmods(ArrayType *arr, int *n)
{
	(void) arr;
	*n = nfz_typmod_n;
	return nfz_typmod_ints;
}

/* ---------- carved-surface aborting stubs ---------- */

static void
nfz_stub(const char *name)
{
	fprintf(stderr, "pg_numeric_oracle: carved stub %s reached\n", name);
	abort();
}

bool		nv_trace_sort = false;

/*
 * The drivers exercise the pure transfns OUTSIDE an aggregate frame, on
 * both sides (the Rust fc wrappers see agg_context() == None): C takes the
 * copy branch. NOT an aborting stub — int2/int4_avg_accum(+inv) call it on
 * every invocation.
 */
int
nv_AggCheckCallContext(FunctionCallInfo fcinfo, MemoryContext *aggcontext)
{
	(void) fcinfo;
	if (aggcontext)
		*aggcontext = NULL;
	return 0;
}

int32
nv_exprTypmod(const Node *expr)
{
	nfz_stub("exprTypmod");
	return -1;
}

Node *
nv_relabel_to_typmod(Node *expr, int32 typmod)
{
	nfz_stub("relabel_to_typmod");
	return NULL;
}

bool
nv_is_funcclause(const void *clause)
{
	nfz_stub("is_funcclause");
	return false;
}

Node *
nv_estimate_expression_value(void *root, Node *node)
{
	nfz_stub("estimate_expression_value");
	return NULL;
}

void
nv_initHyperLogLog(hyperLogLogState *cState, uint8 bwidth)
{
	nfz_stub("initHyperLogLog");
}

void
nv_addHyperLogLog(hyperLogLogState *cState, uint32 hash)
{
	nfz_stub("addHyperLogLog");
}

double
nv_estimateHyperLogLog(hyperLogLogState *cState)
{
	nfz_stub("estimateHyperLogLog");
	return 0.0;
}

uint64
nv_pg_prng_uint64_range(pg_prng_state *state, uint64 rmin, uint64 rmax)
{
	nfz_stub("pg_prng_uint64_range");
	return 0;
}

FuncCallContext *
nv_srf_firstcall_init(FunctionCallInfo fcinfo)
{
	nfz_stub("SRF_FIRSTCALL_INIT");
	return NULL;
}

FuncCallContext *
nv_srf_percall_setup(FunctionCallInfo fcinfo)
{
	nfz_stub("SRF_PERCALL_SETUP");
	return NULL;
}

bool
nv_srf_is_firstcall(FunctionCallInfo fcinfo)
{
	nfz_stub("SRF_IS_FIRSTCALL");
	return false;
}

Datum
nv_srf_return_next(FunctionCallInfo fcinfo, Datum result)
{
	nfz_stub("SRF_RETURN_NEXT");
	return 0;
}

Datum
nv_srf_return_done(FunctionCallInfo fcinfo)
{
	nfz_stub("SRF_RETURN_DONE");
	return 0;
}

/* ================= the vendored TU, whole-file verbatim ================= */

#include "vendor/numeric.c"

/* ==================== pg_diff entry points (driver ABI) ================== */

/*
 * Op codes — MUST match fuzz/core/src/numericfam.rs NumOp.
 */
enum
{
	NFZ_OP_IN = 1,				/* a=cstring text, i32arg=typmod -> numeric */
	NFZ_OP_OUT = 2,				/* a=numeric -> cstring */
	NFZ_OP_APPLY_TYPMOD = 3,	/* a=numeric, i32arg=typmod -> numeric */
	NFZ_OP_RECV = 4,			/* a=wire bytes, i32arg=typmod -> numeric */
	NFZ_OP_SEND = 5,			/* a=numeric -> bytea payload */
	NFZ_OP_TYPMODIN = 6,		/* a=int32 LE array -> i32 scalar */
	NFZ_OP_OUT_SCI = 7,			/* a=numeric, i32arg=rscale -> cstring */

	NFZ_OP_ADD = 10,
	NFZ_OP_SUB = 11,
	NFZ_OP_MUL = 12,
	NFZ_OP_DIV = 13,
	NFZ_OP_DIV_TRUNC = 14,
	NFZ_OP_MOD = 15,
	NFZ_OP_MIN = 16,
	NFZ_OP_MAX = 17,
	NFZ_OP_GCD = 18,
	NFZ_OP_LCM = 19,

	NFZ_OP_CMP = 20,			/* -> i32 */
	NFZ_OP_EQ = 21,				/* -> bool */
	NFZ_OP_NE = 22,
	NFZ_OP_GT = 23,
	NFZ_OP_GE = 24,
	NFZ_OP_LT = 25,
	NFZ_OP_LE = 26,

	NFZ_OP_ABS = 30,
	NFZ_OP_UMINUS = 31,
	NFZ_OP_UPLUS = 32,
	NFZ_OP_SIGN = 33,
	NFZ_OP_ROUND = 34,			/* i32arg = scale */
	NFZ_OP_TRUNC = 35,			/* i32arg = scale */
	NFZ_OP_CEIL = 36,
	NFZ_OP_FLOOR = 37,
	NFZ_OP_INC = 38,
	NFZ_OP_SCALE = 39,			/* -> i32 */
	NFZ_OP_MIN_SCALE = 40,		/* -> i32 */
	NFZ_OP_TRIM_SCALE = 41,

	NFZ_OP_SQRT = 50,
	NFZ_OP_EXP = 51,
	NFZ_OP_LN = 52,
	NFZ_OP_LOG = 53,			/* log(a, b) */
	NFZ_OP_POWER = 54,
	NFZ_OP_FAC = 55,			/* i64arg = n */
	NFZ_OP_WIDTH_BUCKET = 56,	/* a=op, b=b1, c=b2, i32arg=count -> i32 */
	NFZ_OP_IN_RANGE = 57,		/* a=val,b=base,c=offset, i32arg bit0=sub bit1=less -> bool */

	NFZ_OP_TO_INT2 = 60,		/* -> i64 scalar */
	NFZ_OP_TO_INT4 = 61,
	NFZ_OP_TO_INT8 = 62,
	NFZ_OP_TO_FLOAT4 = 63,		/* -> f32 bits in scalar */
	NFZ_OP_TO_FLOAT8 = 64,		/* -> f64 bits in scalar */
	NFZ_OP_FROM_INT2 = 65,		/* i64arg -> numeric */
	NFZ_OP_FROM_INT4 = 66,
	NFZ_OP_FROM_INT8 = 67,
	NFZ_OP_FROM_FLOAT4 = 68,	/* f64arg (as float) -> numeric */
	NFZ_OP_FROM_FLOAT8 = 69,

	NFZ_OP_HASH = 70,			/* -> u32 in scalar */
	NFZ_OP_HASH_EXT = 71,		/* i64arg = seed -> u64 in scalar */

	NFZ_OP_INT8_SUM = 80,		/* a=state numeric|null(alen<0), i64arg=val, i32arg bit0=state-null bit1=val-null -> numeric|null */
	NFZ_OP_INT8_AVG = 81,		/* a=_int8 transarray varlena -> numeric */
	NFZ_OP_INT2_AVG_ACCUM = 82, /* a=transarray, i64arg=val -> transarray image */
	NFZ_OP_INT4_AVG_ACCUM = 83,
	NFZ_OP_INT2_AVG_ACCUM_INV = 84,
	NFZ_OP_INT4_AVG_ACCUM_INV = 85,
};

/*
 * Generic dual-exec entry. Returns 0 on success, the sqlstate int on a
 * captured ereport, -2 if the result was SQL NULL. Result bytes go to
 * out/outlen (varlena results INCLUDE the 4-byte header; cstrings are
 * NUL-terminated and outlen excludes the NUL); scalar results to *scalar.
 */
int
pg_diff_num_call(int op,
				 const uint8 *a, int alen,
				 const uint8 *b, int blen,
				 const uint8 *c, int clen,
				 int64 i64arg, int32 i32arg, double f64arg,
				 uint8 *out, int outcap, int *outlen,
				 uint64 *scalar)
{
	LOCAL_FCINFO(fcinfo, 8);
	Datum		d = 0;
	volatile int rc = 0;

	nfz_arena_off = 0;
	nfz_sqlstate = 0;
	*outlen = 0;
	*scalar = 0;

	if (setjmp(nfz_jmp) != 0)
	{
		nfz_jmp_armed = 0;
		return nfz_sqlstate ? nfz_sqlstate : -1;
	}
	nfz_jmp_armed = 1;

	memset(fcinfo, 0, sizeof(*fcinfo));
	fcinfo->nargs = 8;

	/* operands arrive as full varlena images (header included); the
	 * transarray ops mutate arg 0 in place (the shim's _P_COPY is a
	 * no-copy), so those get an arena copy rather than the driver's
	 * buffer */
	if (a && alen >= 0)
	{
		if (op >= NFZ_OP_INT8_AVG)
		{
			void	   *ac = nv_palloc(alen);

			memcpy(ac, a, alen);
			fcinfo->args[0].value = PointerGetDatum(ac);
		}
		else
			fcinfo->args[0].value = PointerGetDatum(a);
	}
	if (b && blen >= 0)
		fcinfo->args[1].value = PointerGetDatum(b);

#define CALL1(fn) (fcinfo->nargs = 1, d = fn(fcinfo))
#define CALL2(fn) (fcinfo->nargs = 2, d = fn(fcinfo))

	switch (op)
	{
		case NFZ_OP_IN:
			{
				/* a = raw text bytes (NUL added here), typmod = i32arg */
				char	   *s = nv_palloc(alen + 1);

				memcpy(s, a, alen);
				s[alen] = '\0';
				fcinfo->args[0].value = CStringGetDatum(s);
				fcinfo->args[1].value = ObjectIdGetDatum(0);
				fcinfo->args[2].value = Int32GetDatum(i32arg);
				fcinfo->nargs = 3;
				d = numeric_in(fcinfo);
				break;
			}
		case NFZ_OP_OUT:
			CALL1(numeric_out);
			break;
		case NFZ_OP_APPLY_TYPMOD:
			fcinfo->args[1].value = Int32GetDatum(i32arg);
			CALL2(numeric);
			break;
		case NFZ_OP_RECV:
			{
				StringInfoData buf;

				/* numeric_recv detoasts nothing: args = StringInfo, oid, typmod */
				buf.data = (char *) a;
				buf.len = alen;
				buf.maxlen = 0;
				buf.cursor = 0;
				fcinfo->args[0].value = PointerGetDatum(&buf);
				fcinfo->args[1].value = ObjectIdGetDatum(0);
				fcinfo->args[2].value = Int32GetDatum(i32arg);
				fcinfo->nargs = 3;
				d = numeric_recv(fcinfo);
				break;
			}
		case NFZ_OP_SEND:
			CALL1(numeric_send);
			break;
		case NFZ_OP_TYPMODIN:
			{
				int			i,
							n = alen / 4;

				if (n > 8)
					n = 8;
				nfz_typmod_n = n;
				for (i = 0; i < n; i++)
					memcpy(&nfz_typmod_ints[i], a + 4 * i, 4);
				fcinfo->args[0].value = PointerGetDatum(NULL);
				CALL1(numerictypmodin);
				break;
			}
		case NFZ_OP_OUT_SCI:
			{
				char	   *s = numeric_out_sci((Numeric) a, i32arg);

				d = CStringGetDatum(s);
				break;
			}

		case NFZ_OP_ADD:
			CALL2(numeric_add);
			break;
		case NFZ_OP_SUB:
			CALL2(numeric_sub);
			break;
		case NFZ_OP_MUL:
			CALL2(numeric_mul);
			break;
		case NFZ_OP_DIV:
			CALL2(numeric_div);
			break;
		case NFZ_OP_DIV_TRUNC:
			CALL2(numeric_div_trunc);
			break;
		case NFZ_OP_MOD:
			CALL2(numeric_mod);
			break;
		case NFZ_OP_MIN:
			CALL2(numeric_smaller);
			break;
		case NFZ_OP_MAX:
			CALL2(numeric_larger);
			break;
		case NFZ_OP_GCD:
			CALL2(numeric_gcd);
			break;
		case NFZ_OP_LCM:
			CALL2(numeric_lcm);
			break;

		case NFZ_OP_CMP:
			CALL2(numeric_cmp);
			*scalar = (uint64) (uint32) DatumGetInt32(d);
			goto scalar_done;
		case NFZ_OP_EQ:
			CALL2(numeric_eq);
			goto bool_done;
		case NFZ_OP_NE:
			CALL2(numeric_ne);
			goto bool_done;
		case NFZ_OP_GT:
			CALL2(numeric_gt);
			goto bool_done;
		case NFZ_OP_GE:
			CALL2(numeric_ge);
			goto bool_done;
		case NFZ_OP_LT:
			CALL2(numeric_lt);
			goto bool_done;
		case NFZ_OP_LE:
			CALL2(numeric_le);
			goto bool_done;

		case NFZ_OP_ABS:
			CALL1(numeric_abs);
			break;
		case NFZ_OP_UMINUS:
			CALL1(numeric_uminus);
			break;
		case NFZ_OP_UPLUS:
			CALL1(numeric_uplus);
			break;
		case NFZ_OP_SIGN:
			CALL1(numeric_sign);
			break;
		case NFZ_OP_ROUND:
			fcinfo->args[1].value = Int32GetDatum(i32arg);
			CALL2(numeric_round);
			break;
		case NFZ_OP_TRUNC:
			fcinfo->args[1].value = Int32GetDatum(i32arg);
			CALL2(numeric_trunc);
			break;
		case NFZ_OP_CEIL:
			CALL1(numeric_ceil);
			break;
		case NFZ_OP_FLOOR:
			CALL1(numeric_floor);
			break;
		case NFZ_OP_INC:
			CALL1(numeric_inc);
			break;
		case NFZ_OP_SCALE:
			CALL1(numeric_scale);
			*scalar = (uint64) (uint32) DatumGetInt32(d);
			goto scalar_done;
		case NFZ_OP_MIN_SCALE:
			CALL1(numeric_min_scale);
			*scalar = (uint64) (uint32) DatumGetInt32(d);
			goto scalar_done;
		case NFZ_OP_TRIM_SCALE:
			CALL1(numeric_trim_scale);
			break;

		case NFZ_OP_SQRT:
			CALL1(numeric_sqrt);
			break;
		case NFZ_OP_EXP:
			CALL1(numeric_exp);
			break;
		case NFZ_OP_LN:
			CALL1(numeric_ln);
			break;
		case NFZ_OP_LOG:
			CALL2(numeric_log);
			break;
		case NFZ_OP_POWER:
			CALL2(numeric_power);
			break;
		case NFZ_OP_FAC:
			fcinfo->args[0].value = Int64GetDatum(i64arg);
			CALL1(numeric_fac);
			break;
		case NFZ_OP_WIDTH_BUCKET:
			fcinfo->args[2].value = PointerGetDatum(c);
			fcinfo->args[3].value = Int32GetDatum(i32arg);
			fcinfo->nargs = 4;
			d = width_bucket_numeric(fcinfo);
			*scalar = (uint64) (uint32) DatumGetInt32(d);
			goto scalar_done;
		case NFZ_OP_IN_RANGE:
			fcinfo->args[2].value = PointerGetDatum(c);
			fcinfo->args[3].value = BoolGetDatum((i32arg & 1) != 0);
			fcinfo->args[4].value = BoolGetDatum((i32arg & 2) != 0);
			fcinfo->nargs = 5;
			d = in_range_numeric_numeric(fcinfo);
			goto bool_done;

		case NFZ_OP_TO_INT2:
			CALL1(numeric_int2);
			*scalar = (uint64) (uint16) DatumGetInt16(d);
			goto scalar_done;
		case NFZ_OP_TO_INT4:
			CALL1(numeric_int4);
			*scalar = (uint64) (uint32) DatumGetInt32(d);
			goto scalar_done;
		case NFZ_OP_TO_INT8:
			CALL1(numeric_int8);
			*scalar = (uint64) DatumGetInt64(d);
			goto scalar_done;
		case NFZ_OP_TO_FLOAT4:
			CALL1(numeric_float4);
			*scalar = (uint64) DatumGetUInt32(d);	/* f32 bits */
			goto scalar_done;
		case NFZ_OP_TO_FLOAT8:
			CALL1(numeric_float8);
			*scalar = (uint64) d;	/* f64 bits */
			goto scalar_done;
		case NFZ_OP_FROM_INT2:
			fcinfo->args[0].value = Int16GetDatum((int16) i64arg);
			CALL1(int2_numeric);
			break;
		case NFZ_OP_FROM_INT4:
			fcinfo->args[0].value = Int32GetDatum((int32) i64arg);
			CALL1(int4_numeric);
			break;
		case NFZ_OP_FROM_INT8:
			fcinfo->args[0].value = Int64GetDatum(i64arg);
			CALL1(int8_numeric);
			break;
		case NFZ_OP_FROM_FLOAT4:
			fcinfo->args[0].value = Float4GetDatum((float) f64arg);
			CALL1(float4_numeric);
			break;
		case NFZ_OP_FROM_FLOAT8:
			fcinfo->args[0].value = Float8GetDatum(f64arg);
			CALL1(float8_numeric);
			break;

		case NFZ_OP_HASH:
			CALL1(hash_numeric);
			*scalar = (uint64) DatumGetUInt32(d);
			goto scalar_done;
		case NFZ_OP_HASH_EXT:
			fcinfo->args[1].value = Int64GetDatum(i64arg);
			CALL2(hash_numeric_extended);
			*scalar = DatumGetUInt64(d);
			goto scalar_done;

		case NFZ_OP_INT8_SUM:
			fcinfo->args[0].isnull = (i32arg & 1) != 0;
			fcinfo->args[1].value = Int64GetDatum(i64arg);
			fcinfo->args[1].isnull = (i32arg & 2) != 0;
			CALL2(int8_sum);
			break;
		case NFZ_OP_INT8_AVG:
			CALL1(int8_avg);
			break;
		case NFZ_OP_INT2_AVG_ACCUM:
			fcinfo->args[1].value = Int16GetDatum((int16) i64arg);
			CALL2(int2_avg_accum);
			break;
		case NFZ_OP_INT4_AVG_ACCUM:
			fcinfo->args[1].value = Int32GetDatum((int32) i64arg);
			CALL2(int4_avg_accum);
			break;
		case NFZ_OP_INT2_AVG_ACCUM_INV:
			fcinfo->args[1].value = Int16GetDatum((int16) i64arg);
			CALL2(int2_avg_accum_inv);
			break;
		case NFZ_OP_INT4_AVG_ACCUM_INV:
			fcinfo->args[1].value = Int32GetDatum((int32) i64arg);
			CALL2(int4_avg_accum_inv);
			break;

		default:
			nfz_jmp_armed = 0;
			return -3;			/* unknown op */
	}

	/* pointer-result ops fall through to here */
	if (fcinfo->isnull)
		rc = -2;
	else
	{
		const char *p = DatumGetPointer(d);
		int			len;

		switch (op)
		{
			case NFZ_OP_OUT:
			case NFZ_OP_OUT_SCI:
				len = (int) strlen(p);
				break;
			case NFZ_OP_TYPMODIN:
				*scalar = (uint64) (uint32) DatumGetInt32(d);
				nfz_jmp_armed = 0;
				return 0;
			default:
				len = (int) VARSIZE(p);
				break;
		}
		if (len > outcap)
		{
			fprintf(stderr, "pg_numeric_oracle: result overflows outcap (%d > %d)\n", len, outcap);
			abort();
		}
		memcpy(out, p, len);
		*outlen = len;
	}
	nfz_jmp_armed = 0;
	return rc;

bool_done:
	*scalar = DatumGetBool(d) ? 1 : 0;
scalar_done:
	if (fcinfo->isnull)
		rc = -2;
	nfz_jmp_armed = 0;
	return rc;
}
