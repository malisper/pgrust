/*
 * Vendored PostgreSQL C: int2/int4 (smallint/integer) I/O, int2vector I/O,
 * arithmetic, comparison, bit-op, gcd/lcm and in_range family —
 * differential-fuzz oracle for adt/int (target int_diff).
 *
 * Provenance (all bodies VERBATIM unless a shim is listed below), all from
 * the repo's vendored ground-truth checkout
 * ../pgrust-fabled/vendor/postgres-src @
 * 62d6c7d3df6287f1bd83199c1a746e50d31571a0 ("Stamp 18.3", REL_18):
 *   - src/backend/utils/adt/numutils.c 29..97: DIGIT_TABLE,
 *     decimalLength32/64, hexlookup — verbatim.
 *   - src/backend/utils/adt/numutils.c 127..358 pg_strtoint16_safe,
 *     388..619 pg_strtoint32_safe, 1032..1133 pg_itoa/pg_ultoa_n/pg_ltoa —
 *     verbatim.
 *   - src/backend/utils/adt/int.c 58..253 (int2in, int2out, int2recv,
 *     int2send, buildint2vector, check_valid_int2vector, int2vectorin,
 *     int2vectorout), 311..359 (int4in, int4out, int4recv, int4send),
 *     361..1526 (i2toi4 .. int2shr: conversions, int4_bool/bool_int4,
 *     all relops, in_range family, arithmetic, mod/abs, gcd/lcm,
 *     larger/smaller, bit ops) — verbatim.
 *     Excluded: int2vectorrecv/int2vectorsend (array_recv/array_send
 *     machinery — not in crate scope), generate_series* (SRF machinery,
 *     series.rs carve), generate_series_int4_support (planner).
 *   - src/backend/libpq/pqformat.c 521..536 pq_copymsgbytes, 408..442
 *     pq_getmsgint — verbatim (the entire logic of int2recv/int4recv
 *     beyond arg plumbing).
 *   - src/include/libpq/pqformat.h pq_writeint16/pq_writeint32 — verbatim
 *     (the entire byte-image logic of int2send/int4send).
 *   - src/include/c.h 683..692: int2vector struct — verbatim.
 *
 * Shims (plumbing only, never logic):
 *   - fmgr: Datum = uintptr_t; MiniFcinfo carries typed args + the
 *     escontext pointer; PG_GETARG_* / PG_RETURN_* map onto it.
 *   - ereport(ERROR, ...) -> record errcode class, longjmp to dispatcher;
 *     ereturn(escontext,ret,...) -> record class; soft returns ret, hard
 *     longjmps (exactly errsave's control flow). elog(ERROR,...) -> class
 *     98 (internal; unreachable arms only).
 *   - errcode symbols -> small ints (PG_DIFF_ERR_*); errmsg/errdetail
 *     evaluate to 0 with arguments unevaluated (comparator checks the
 *     errcode class, not message text).
 *   - common/int.h pg_add/sub/mul_s16/s32/s64_overflow, pg_neg_u16/u32/
 *     u64_overflow: the HAVE__BUILTIN_OP_OVERFLOW branches verbatim.
 *   - port/pg_bitutils.h pg_leftmost_one_pos32 -> HAVE__BUILTIN_CLZ branch
 *     verbatim.
 *   - port/pg_bswap.h pg_ntoh16/32, pg_hton16/32 -> __builtin_bswap16/32
 *     on little-endian, their exact definitions.
 *   - palloc/palloc0/repalloc -> malloc-backed TLS pointer arena, reset at
 *     every dispatcher entry (PostgreSQL's memory-context reset, minimally).
 *   - SET_VARSIZE -> little-endian 4-byte varlena header (len << 2),
 *     the 4B-header definition from postgres.h varatt on LE builds.
 *   - StringInfoData: {data, len, maxlen, cursor}. For the send path,
 *     pq_begintypsend/pq_endtypsend are shimmed onto a caller-provided
 *     fixed 64-byte buffer (4 reserved zero bytes + SET_VARSIZE on end,
 *     exactly their effect for these 2/4-byte payloads); the byte-image
 *     logic (pg_hton + memcpy) is the verbatim pq_writeint16/32.
 *     enlargeStringInfo -> capacity assert (payloads are 2/4 bytes).
 *   - strtol/errno/isspace/isxdigit/isdigit: host libc, C locale
 *     (int2vectorin's parse; ASCII-range behavior is locale-invariant and
 *     the driver constrains inputs to non-NUL bytes).
 *   - Int2VectorSize/offsetof, FLEXIBLE_ARRAY_MEMBER -> [] ; INT2OID = 21
 *     (catalog/pg_type_d.h).
 */

/*
 * FAMILY SYMBOL ISOLATION (central symfix lane, 2026-08-01): this TU landed
 * with unprefixed verbatim-C exports that collide under GNU ld with the
 * incumbent oracle families (pg_numutils.c: pg_itoa/pg_ltoa/pg_ultoa_n/
 * pg_strtoint16_safe/pg_strtoint32_safe; pg_fmt_dch_io.c: int4out/pg_ltoa/
 * pg_ultoa_n; tsvec: pq_getmsgint). Apple ld64 only warns (first-definition-
 * wins member pull), so local checks passed while EVERY Linux fleet fuzz
 * build hard-errored (ld.lld duplicate symbols, first witnessed by
 * gram_core job -2ab6-60592). Preprocessor-layer rename ONLY — every C body
 * below stays verbatim (wcharfam/contribafam in-file-prefix precedent;
 * in-file rather than build.rs .define() because this TU shares the
 * pg_difffuzz_oracle cc::Build with pg_numutils.c, whose SAME-NAMED exports
 * must keep their unprefixed names). Durable lesson: verbatim oracle TUs
 * MUST ship with family symbol prefixes — ld64 warnings are ld.lld errors.
 */
#define int4out intio_int4out
#define pg_itoa intio_pg_itoa
#define pg_ltoa intio_pg_ltoa
#define pg_strtoint16_safe intio_pg_strtoint16_safe
#define pg_strtoint32_safe intio_pg_strtoint32_safe
#define pg_ultoa_n intio_pg_ultoa_n
#define pq_getmsgint intio_pq_getmsgint

#include "postgres.h"

#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <math.h>
#include <setjmp.h>
#include <stdio.h>
#include <assert.h>
#include "pg_oracle_guard.h"	/* oracle-serialization holder check */

/* ---- error-plane shim (same classes convention as pg_float_io.c) ---- */

#define PG_DIFF_ERR_INVALID_TEXT 1   /* 22P02 */
#define PG_DIFF_ERR_OUT_OF_RANGE 2   /* 22003 */
#define PG_DIFF_ERR_DIVISION_BY_ZERO 5  /* 22012 */
#define PG_DIFF_ERR_INVALID_PRECEDING_FOLLOWING 6 /* 22013 */
#define PG_DIFF_ERR_PROTOCOL_VIOLATION 7 /* 08P01 */
#define PG_DIFF_ERR_DATATYPE_MISMATCH 8 /* 42804 */

#define ERRCODE_INVALID_TEXT_REPRESENTATION PG_DIFF_ERR_INVALID_TEXT
#define ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE PG_DIFF_ERR_OUT_OF_RANGE
#define ERRCODE_DIVISION_BY_ZERO PG_DIFF_ERR_DIVISION_BY_ZERO
#define ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE PG_DIFF_ERR_INVALID_PRECEDING_FOLLOWING
#define ERRCODE_PROTOCOL_VIOLATION PG_DIFF_ERR_PROTOCOL_VIOLATION
#define ERRCODE_DATATYPE_MISMATCH PG_DIFF_ERR_DATATYPE_MISMATCH

static _Thread_local int pg_diff_int_errcode;
static _Thread_local jmp_buf pg_diff_int_jb;

int
pg_diff_int_errcode_get(void)
{
	return pg_diff_int_errcode;
}

static int
errcode(int code)
{
	pg_diff_int_errcode = code;
	return 0;
}

static int
errmsg(const char *fmt, ...)
{
	(void) fmt;
	return 0;
}

#define errdetail errmsg
#define errhint errmsg
#define ERROR 21
#define ereport(elevel, rest) \
	do { (void) (rest); longjmp(pg_diff_int_jb, 1); } while (0)
#define elog(elevel, ...) \
	do { pg_diff_int_errcode = 98; longjmp(pg_diff_int_jb, 1); } while (0)
/* errsave/ereturn: soft (escontext != NULL) records and returns dummy_value,
 * exactly the ErrorSaveContext control flow; hard raises. */
#define ereturn(escontext, dummy_value, rest) \
	do { \
		(void) (rest); \
		if ((escontext) != NULL) \
			return dummy_value; \
		longjmp(pg_diff_int_jb, 1); \
	} while (0)

typedef struct Node Node;

/* ---- fmgr mini-shim ---- */

typedef uintptr_t Datum;
typedef uint32 Oid;

typedef struct MiniFcinfo
{
	int64		i[5];
	Node	   *context;
} MiniFcinfo;

#define PG_FUNCTION_ARGS MiniFcinfo *fcinfo
#define PG_GETARG_INT32(n) ((int32) fcinfo->i[n])
#define PG_GETARG_INT16(n) ((int16) fcinfo->i[n])
#define PG_GETARG_INT64(n) (fcinfo->i[n])
#define PG_GETARG_BOOL(n) ((bool) fcinfo->i[n])
#define PG_GETARG_CSTRING(n) ((char *) (uintptr_t) fcinfo->i[n])
#define PG_GETARG_POINTER(n) ((void *) (uintptr_t) fcinfo->i[n])
#define PG_RETURN_INT32(x) return (Datum) (uint32) (int32) (x)
#define PG_RETURN_INT16(x) return (Datum) (uint16) (int16) (x)
#define PG_RETURN_BOOL(x) return (Datum) ((x) ? 1 : 0)
#define PG_RETURN_CSTRING(x) return (Datum) (uintptr_t) (x)
#define PG_RETURN_POINTER(x) return (Datum) (uintptr_t) (x)
#define PG_RETURN_NULL() return 0  /* unreachable post-ereport lines in div/mod bodies */
#define PG_RETURN_BYTEA_P(x) return (Datum) (uintptr_t) (x)

/* palloc arena shim: PostgreSQL frees these via memory-context reset; the
 * oracle mirrors that with a TLS pointer arena reset at every pg_diff_*
 * dispatcher entry, so error-path longjmp/ereturn exits cannot leak (LSan
 * artifact 2026-07-31, 88-byte int2vectorin result on the soft-error path). */
#define PG_DIFF_ARENA_MAX 64
static _Thread_local void *pg_diff_arena[PG_DIFF_ARENA_MAX];
static _Thread_local int pg_diff_arena_n;

static void
pg_diff_arena_reset(void)
{
	int			i;

	for (i = 0; i < pg_diff_arena_n; i++)
		free(pg_diff_arena[i]);
	pg_diff_arena_n = 0;
}

static void *
pg_diff_palloc_impl(size_t n)
{
	void	   *p = malloc(n);

	assert(pg_diff_arena_n < PG_DIFF_ARENA_MAX);
	pg_diff_arena[pg_diff_arena_n++] = p;
	return p;
}

static void *
pg_diff_palloc0_impl(size_t n)
{
	void	   *p = calloc(1, n);

	assert(pg_diff_arena_n < PG_DIFF_ARENA_MAX);
	pg_diff_arena[pg_diff_arena_n++] = p;
	return p;
}

static void *
pg_diff_repalloc_impl(void *old, size_t n)
{
	void	   *p = realloc(old, n);
	int			i;

	for (i = 0; i < pg_diff_arena_n; i++)
	{
		if (pg_diff_arena[i] == old)
		{
			pg_diff_arena[i] = p;
			return p;
		}
	}
	assert(!"repalloc of a pointer the arena never issued");
	return p;
}

#undef palloc
#define palloc(n) pg_diff_palloc_impl(n)
#define palloc0(n) pg_diff_palloc0_impl(n)
#define repalloc(p, n) pg_diff_repalloc_impl((p), (n))

#define PG_INT16_MIN INT16_MIN
#define PG_INT16_MAX INT16_MAX
#define PG_INT32_MIN INT32_MIN
#define PG_INT32_MAX INT32_MAX
#define PG_INT64_MIN INT64_MIN
#define PG_INT64_MAX INT64_MAX
#define PG_UINT16_MAX UINT16_MAX
#define PG_UINT32_MAX UINT32_MAX

/* catalog/pg_type_d.h */
#define INT2OID 21

/* common/int.h @ 62d6c7d3df — HAVE__BUILTIN_OP_OVERFLOW branches, verbatim */
static inline bool
pg_add_s16_overflow(int16 a, int16 b, int16 *result)
{
	return __builtin_add_overflow(a, b, result);
}

static inline bool
pg_sub_s16_overflow(int16 a, int16 b, int16 *result)
{
	return __builtin_sub_overflow(a, b, result);
}

static inline bool
pg_mul_s16_overflow(int16 a, int16 b, int16 *result)
{
	return __builtin_mul_overflow(a, b, result);
}

static inline bool
pg_neg_u16_overflow(uint16 a, int16 *result)
{
	return __builtin_sub_overflow(0, a, result);
}

static inline bool
pg_add_s32_overflow(int32 a, int32 b, int32 *result)
{
	return __builtin_add_overflow(a, b, result);
}

static inline bool
pg_sub_s32_overflow(int32 a, int32 b, int32 *result)
{
	return __builtin_sub_overflow(a, b, result);
}

static inline bool
pg_mul_s32_overflow(int32 a, int32 b, int32 *result)
{
	return __builtin_mul_overflow(a, b, result);
}

static inline bool
pg_neg_u32_overflow(uint32 a, int32 *result)
{
	return __builtin_sub_overflow(0, a, result);
}

static inline bool
pg_add_s64_overflow(int64 a, int64 b, int64 *result)
{
	return __builtin_add_overflow(a, b, result);
}

/* port/pg_bitutils.h @ 62d6c7d3df — HAVE__BUILTIN_CLZ branch, verbatim */
static inline int
pg_leftmost_one_pos32(uint32 word)
{
	return 31 - __builtin_clz(word);
}

static inline int
pg_leftmost_one_pos64(uint64 word)
{
	return 63 - __builtin_clzll(word);
}

/* port/pg_bswap.h — little-endian definitions */
#define pg_ntoh16(x) __builtin_bswap16(x)
#define pg_hton16(x) __builtin_bswap16(x)
#define pg_ntoh32(x) __builtin_bswap32(x)
#define pg_hton32(x) __builtin_bswap32(x)

/* c.h 683..692 @ 62d6c7d3df — verbatim (FLEXIBLE_ARRAY_MEMBER -> []) */
typedef struct
{
	int32		vl_len_;		/* these fields must match ArrayType! */
	int			ndim;			/* always 1 for int2vector */
	int32		dataoffset;		/* always 0 for int2vector */
	Oid			elemtype;
	int			dim1;
	int			lbound1;
	int16		values[];
} int2vector;

/* varatt.h 4B little-endian header: SET_VARSIZE / VARHDRSZ */
#define VARHDRSZ ((int32) sizeof(int32))
#define SET_VARSIZE(PTR, len) (*((uint32 *) (PTR)) = ((uint32) (len)) << 2)
typedef struct bytea { int32 vl_len_; char vl_dat[]; } bytea;

/* stringinfo shim: the fields pqformat reads/writes */
typedef struct StringInfoData
{
	char	   *data;
	int			len;
	int			maxlen;
	int			cursor;
} StringInfoData;
typedef StringInfoData *StringInfo;

/* send-path shim (see header): fixed caller buffer, no allocator */
static void
pq_begintypsend(StringInfo buf)
{
	/* Reserve four bytes for the bytea length word (verbatim effect) */
	memset(buf->data, 0, 4);
	buf->len = 4;
	buf->cursor = 0;
}

static bytea *
pq_endtypsend(StringInfo buf)
{
	bytea	   *result = (bytea *) buf->data;

	/* Insert correct length into bytea length word */
	assert(buf->len >= VARHDRSZ);
	SET_VARSIZE(result, buf->len);

	return result;
}

#define enlargeStringInfo(buf, needed) assert((buf)->len + (needed) <= (buf)->maxlen)

/* pqformat.h @ 62d6c7d3df — pq_writeint16/pq_writeint32 verbatim
 * (pg_restrict -> plain; Assert -> assert) */
static inline void
pq_writeint16(StringInfoData *buf, uint16 i)
{
	uint16		ni = pg_hton16(i);

	assert(buf->len + (int) sizeof(uint16) <= buf->maxlen);
	memcpy((char *) (buf->data + buf->len), &ni, sizeof(uint16));
	buf->len += sizeof(uint16);
}

static inline void
pq_writeint32(StringInfoData *buf, uint32 i)
{
	uint32		ni = pg_hton32(i);

	assert(buf->len + (int) sizeof(uint32) <= buf->maxlen);
	memcpy((char *) (buf->data + buf->len), &ni, sizeof(uint32));
	buf->len += sizeof(uint32);
}

static inline void
pq_sendint16(StringInfo buf, uint16 i)
{
	enlargeStringInfo(buf, sizeof(uint16));
	pq_writeint16(buf, i);
}

static inline void
pq_sendint32(StringInfo buf, uint32 i)
{
	enlargeStringInfo(buf, sizeof(uint32));
	pq_writeint32(buf, i);
}

void pq_copymsgbytes(StringInfo msg, void *buf, int datalen);
unsigned int pq_getmsgint(StringInfo msg, int b);

/* utils/builtins.h prototypes (plumbing: upstream declares these there) */
int			pg_itoa(int16 i, char *a);
int			pg_ultoa_n(uint32 value, char *a);
int			pg_ltoa(int32 value, char *a);
int16		pg_strtoint16_safe(const char *s, Node *escontext);
int32		pg_strtoint32_safe(const char *s, Node *escontext);

/* int.c 45 @ 62d6c7d3df — verbatim */
#include <stddef.h>
#define Int2VectorSize(n)	(offsetof(int2vector, values) + (n) * sizeof(int16))

/* fmgr.h Datum-conversion + DirectFunctionCall5 shim (plumbing): builds a
 * MiniFcinfo exactly as fmgr's DirectFunctionCall5Coll does, NULL context. */
#define Int16GetDatum(X) ((Datum) (X))
#define Int32GetDatum(X) ((Datum) (X))
#define Int64GetDatum(X) ((Datum) (X))
#define BoolGetDatum(X) ((Datum) ((X) ? 1 : 0))
#define PG_GETARG_DATUM(n) ((Datum) fcinfo->i[n])

static Datum
DirectFunctionCall5(Datum (*func) (MiniFcinfo *), Datum d0, Datum d1,
					Datum d2, Datum d3, Datum d4)
{
	MiniFcinfo	fc = {{0}};

	fc.i[0] = (int64) d0;
	fc.i[1] = (int64) d1;
	fc.i[2] = (int64) d2;
	fc.i[3] = (int64) d3;
	fc.i[4] = (int64) d4;
	return func(&fc);
}

/* ====================================================================
 * VERBATIM VENDORED BODIES BEGIN
 * ==================================================================== */

/* ---- numutils.c 29..97: DIGIT_TABLE, decimalLength32/64, hexlookup ---- */
static const char DIGIT_TABLE[200] =
"00" "01" "02" "03" "04" "05" "06" "07" "08" "09"
"10" "11" "12" "13" "14" "15" "16" "17" "18" "19"
"20" "21" "22" "23" "24" "25" "26" "27" "28" "29"
"30" "31" "32" "33" "34" "35" "36" "37" "38" "39"
"40" "41" "42" "43" "44" "45" "46" "47" "48" "49"
"50" "51" "52" "53" "54" "55" "56" "57" "58" "59"
"60" "61" "62" "63" "64" "65" "66" "67" "68" "69"
"70" "71" "72" "73" "74" "75" "76" "77" "78" "79"
"80" "81" "82" "83" "84" "85" "86" "87" "88" "89"
"90" "91" "92" "93" "94" "95" "96" "97" "98" "99";

/*
 * Adapted from http://graphics.stanford.edu/~seander/bithacks.html#IntegerLog10
 */
static inline int
decimalLength32(const uint32 v)
{
	int			t;
	static const uint32 PowersOfTen[] = {
		1, 10, 100,
		1000, 10000, 100000,
		1000000, 10000000, 100000000,
		1000000000
	};

	/*
	 * Compute base-10 logarithm by dividing the base-2 logarithm by a
	 * good-enough approximation of the base-2 logarithm of 10
	 */
	t = (pg_leftmost_one_pos32(v) + 1) * 1233 / 4096;
	return t + (v >= PowersOfTen[t]);
}

static inline int
decimalLength64(const uint64 v)
{
	int			t;
	static const uint64 PowersOfTen[] = {
		UINT64CONST(1), UINT64CONST(10),
		UINT64CONST(100), UINT64CONST(1000),
		UINT64CONST(10000), UINT64CONST(100000),
		UINT64CONST(1000000), UINT64CONST(10000000),
		UINT64CONST(100000000), UINT64CONST(1000000000),
		UINT64CONST(10000000000), UINT64CONST(100000000000),
		UINT64CONST(1000000000000), UINT64CONST(10000000000000),
		UINT64CONST(100000000000000), UINT64CONST(1000000000000000),
		UINT64CONST(10000000000000000), UINT64CONST(100000000000000000),
		UINT64CONST(1000000000000000000), UINT64CONST(10000000000000000000)
	};

	/*
	 * Compute base-10 logarithm by dividing the base-2 logarithm by a
	 * good-enough approximation of the base-2 logarithm of 10
	 */
	t = (pg_leftmost_one_pos64(v) + 1) * 1233 / 4096;
	return t + (v >= PowersOfTen[t]);
}

static const int8 hexlookup[128] = {
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	0, 1, 2, 3, 4, 5, 6, 7, 8, 9, -1, -1, -1, -1, -1, -1,
	-1, 10, 11, 12, 13, 14, 15, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, 10, 11, 12, 13, 14, 15, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
};
/* ---- numutils.c 127..358: pg_strtoint16_safe ---- */
int16
pg_strtoint16_safe(const char *s, Node *escontext)
{
	const char *ptr = s;
	const char *firstdigit;
	uint16		tmp = 0;
	bool		neg = false;
	unsigned char digit;
	int16		result;

	/*
	 * The majority of cases are likely to be base-10 digits without any
	 * underscore separator characters.  We'll first try to parse the string
	 * with the assumption that's the case and only fallback on a slower
	 * implementation which handles hex, octal and binary strings and
	 * underscores if the fastpath version cannot parse the string.
	 */

	/* leave it up to the slow path to look for leading spaces */

	if (*ptr == '-')
	{
		ptr++;
		neg = true;
	}

	/* a leading '+' is uncommon so leave that for the slow path */

	/* process the first digit */
	digit = (*ptr - '0');

	/*
	 * Exploit unsigned arithmetic to save having to check both the upper and
	 * lower bounds of the digit.
	 */
	if (likely(digit < 10))
	{
		ptr++;
		tmp = digit;
	}
	else
	{
		/* we need at least one digit */
		goto slow;
	}

	/* process remaining digits */
	for (;;)
	{
		digit = (*ptr - '0');

		if (digit >= 10)
			break;

		ptr++;

		if (unlikely(tmp > -(PG_INT16_MIN / 10)))
			goto out_of_range;

		tmp = tmp * 10 + digit;
	}

	/* when the string does not end in a digit, let the slow path handle it */
	if (unlikely(*ptr != '\0'))
		goto slow;

	if (neg)
	{
		if (unlikely(pg_neg_u16_overflow(tmp, &result)))
			goto out_of_range;
		return result;
	}

	if (unlikely(tmp > PG_INT16_MAX))
		goto out_of_range;

	return (int16) tmp;

slow:
	tmp = 0;
	ptr = s;
	/* no need to reset neg */

	/* skip leading spaces */
	while (isspace((unsigned char) *ptr))
		ptr++;

	/* handle sign */
	if (*ptr == '-')
	{
		ptr++;
		neg = true;
	}
	else if (*ptr == '+')
		ptr++;

	/* process digits */
	if (ptr[0] == '0' && (ptr[1] == 'x' || ptr[1] == 'X'))
	{
		firstdigit = ptr += 2;

		for (;;)
		{
			if (isxdigit((unsigned char) *ptr))
			{
				if (unlikely(tmp > -(PG_INT16_MIN / 16)))
					goto out_of_range;

				tmp = tmp * 16 + hexlookup[(unsigned char) *ptr++];
			}
			else if (*ptr == '_')
			{
				/* underscore must be followed by more digits */
				ptr++;
				if (*ptr == '\0' || !isxdigit((unsigned char) *ptr))
					goto invalid_syntax;
			}
			else
				break;
		}
	}
	else if (ptr[0] == '0' && (ptr[1] == 'o' || ptr[1] == 'O'))
	{
		firstdigit = ptr += 2;

		for (;;)
		{
			if (*ptr >= '0' && *ptr <= '7')
			{
				if (unlikely(tmp > -(PG_INT16_MIN / 8)))
					goto out_of_range;

				tmp = tmp * 8 + (*ptr++ - '0');
			}
			else if (*ptr == '_')
			{
				/* underscore must be followed by more digits */
				ptr++;
				if (*ptr == '\0' || *ptr < '0' || *ptr > '7')
					goto invalid_syntax;
			}
			else
				break;
		}
	}
	else if (ptr[0] == '0' && (ptr[1] == 'b' || ptr[1] == 'B'))
	{
		firstdigit = ptr += 2;

		for (;;)
		{
			if (*ptr >= '0' && *ptr <= '1')
			{
				if (unlikely(tmp > -(PG_INT16_MIN / 2)))
					goto out_of_range;

				tmp = tmp * 2 + (*ptr++ - '0');
			}
			else if (*ptr == '_')
			{
				/* underscore must be followed by more digits */
				ptr++;
				if (*ptr == '\0' || *ptr < '0' || *ptr > '1')
					goto invalid_syntax;
			}
			else
				break;
		}
	}
	else
	{
		firstdigit = ptr;

		for (;;)
		{
			if (*ptr >= '0' && *ptr <= '9')
			{
				if (unlikely(tmp > -(PG_INT16_MIN / 10)))
					goto out_of_range;

				tmp = tmp * 10 + (*ptr++ - '0');
			}
			else if (*ptr == '_')
			{
				/* underscore may not be first */
				if (unlikely(ptr == firstdigit))
					goto invalid_syntax;
				/* and it must be followed by more digits */
				ptr++;
				if (*ptr == '\0' || !isdigit((unsigned char) *ptr))
					goto invalid_syntax;
			}
			else
				break;
		}
	}

	/* require at least one digit */
	if (unlikely(ptr == firstdigit))
		goto invalid_syntax;

	/* allow trailing whitespace, but not other trailing chars */
	while (isspace((unsigned char) *ptr))
		ptr++;

	if (unlikely(*ptr != '\0'))
		goto invalid_syntax;

	if (neg)
	{
		if (unlikely(pg_neg_u16_overflow(tmp, &result)))
			goto out_of_range;
		return result;
	}

	if (tmp > PG_INT16_MAX)
		goto out_of_range;

	return (int16) tmp;

out_of_range:
	ereturn(escontext, 0,
			(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
			 errmsg("value \"%s\" is out of range for type %s",
					s, "smallint")));

invalid_syntax:
	ereturn(escontext, 0,
			(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
			 errmsg("invalid input syntax for type %s: \"%s\"",
					"smallint", s)));
}
/* ---- numutils.c 388..619: pg_strtoint32_safe ---- */
int32
pg_strtoint32_safe(const char *s, Node *escontext)
{
	const char *ptr = s;
	const char *firstdigit;
	uint32		tmp = 0;
	bool		neg = false;
	unsigned char digit;
	int32		result;

	/*
	 * The majority of cases are likely to be base-10 digits without any
	 * underscore separator characters.  We'll first try to parse the string
	 * with the assumption that's the case and only fallback on a slower
	 * implementation which handles hex, octal and binary strings and
	 * underscores if the fastpath version cannot parse the string.
	 */

	/* leave it up to the slow path to look for leading spaces */

	if (*ptr == '-')
	{
		ptr++;
		neg = true;
	}

	/* a leading '+' is uncommon so leave that for the slow path */

	/* process the first digit */
	digit = (*ptr - '0');

	/*
	 * Exploit unsigned arithmetic to save having to check both the upper and
	 * lower bounds of the digit.
	 */
	if (likely(digit < 10))
	{
		ptr++;
		tmp = digit;
	}
	else
	{
		/* we need at least one digit */
		goto slow;
	}

	/* process remaining digits */
	for (;;)
	{
		digit = (*ptr - '0');

		if (digit >= 10)
			break;

		ptr++;

		if (unlikely(tmp > -(PG_INT32_MIN / 10)))
			goto out_of_range;

		tmp = tmp * 10 + digit;
	}

	/* when the string does not end in a digit, let the slow path handle it */
	if (unlikely(*ptr != '\0'))
		goto slow;

	if (neg)
	{
		if (unlikely(pg_neg_u32_overflow(tmp, &result)))
			goto out_of_range;
		return result;
	}

	if (unlikely(tmp > PG_INT32_MAX))
		goto out_of_range;

	return (int32) tmp;

slow:
	tmp = 0;
	ptr = s;
	/* no need to reset neg */

	/* skip leading spaces */
	while (isspace((unsigned char) *ptr))
		ptr++;

	/* handle sign */
	if (*ptr == '-')
	{
		ptr++;
		neg = true;
	}
	else if (*ptr == '+')
		ptr++;

	/* process digits */
	if (ptr[0] == '0' && (ptr[1] == 'x' || ptr[1] == 'X'))
	{
		firstdigit = ptr += 2;

		for (;;)
		{
			if (isxdigit((unsigned char) *ptr))
			{
				if (unlikely(tmp > -(PG_INT32_MIN / 16)))
					goto out_of_range;

				tmp = tmp * 16 + hexlookup[(unsigned char) *ptr++];
			}
			else if (*ptr == '_')
			{
				/* underscore must be followed by more digits */
				ptr++;
				if (*ptr == '\0' || !isxdigit((unsigned char) *ptr))
					goto invalid_syntax;
			}
			else
				break;
		}
	}
	else if (ptr[0] == '0' && (ptr[1] == 'o' || ptr[1] == 'O'))
	{
		firstdigit = ptr += 2;

		for (;;)
		{
			if (*ptr >= '0' && *ptr <= '7')
			{
				if (unlikely(tmp > -(PG_INT32_MIN / 8)))
					goto out_of_range;

				tmp = tmp * 8 + (*ptr++ - '0');
			}
			else if (*ptr == '_')
			{
				/* underscore must be followed by more digits */
				ptr++;
				if (*ptr == '\0' || *ptr < '0' || *ptr > '7')
					goto invalid_syntax;
			}
			else
				break;
		}
	}
	else if (ptr[0] == '0' && (ptr[1] == 'b' || ptr[1] == 'B'))
	{
		firstdigit = ptr += 2;

		for (;;)
		{
			if (*ptr >= '0' && *ptr <= '1')
			{
				if (unlikely(tmp > -(PG_INT32_MIN / 2)))
					goto out_of_range;

				tmp = tmp * 2 + (*ptr++ - '0');
			}
			else if (*ptr == '_')
			{
				/* underscore must be followed by more digits */
				ptr++;
				if (*ptr == '\0' || *ptr < '0' || *ptr > '1')
					goto invalid_syntax;
			}
			else
				break;
		}
	}
	else
	{
		firstdigit = ptr;

		for (;;)
		{
			if (*ptr >= '0' && *ptr <= '9')
			{
				if (unlikely(tmp > -(PG_INT32_MIN / 10)))
					goto out_of_range;

				tmp = tmp * 10 + (*ptr++ - '0');
			}
			else if (*ptr == '_')
			{
				/* underscore may not be first */
				if (unlikely(ptr == firstdigit))
					goto invalid_syntax;
				/* and it must be followed by more digits */
				ptr++;
				if (*ptr == '\0' || !isdigit((unsigned char) *ptr))
					goto invalid_syntax;
			}
			else
				break;
		}
	}

	/* require at least one digit */
	if (unlikely(ptr == firstdigit))
		goto invalid_syntax;

	/* allow trailing whitespace, but not other trailing chars */
	while (isspace((unsigned char) *ptr))
		ptr++;

	if (unlikely(*ptr != '\0'))
		goto invalid_syntax;

	if (neg)
	{
		if (unlikely(pg_neg_u32_overflow(tmp, &result)))
			goto out_of_range;
		return result;
	}

	if (tmp > PG_INT32_MAX)
		goto out_of_range;

	return (int32) tmp;

out_of_range:
	ereturn(escontext, 0,
			(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
			 errmsg("value \"%s\" is out of range for type %s",
					s, "integer")));

invalid_syntax:
	ereturn(escontext, 0,
			(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
			 errmsg("invalid input syntax for type %s: \"%s\"",
					"integer", s)));
}
/* ---- numutils.c 1032..1133: pg_itoa, pg_ultoa_n, pg_ltoa ---- */
/*
 * pg_itoa: converts a signed 16-bit integer to its string representation
 * and returns strlen(a).
 *
 * Caller must ensure that 'a' points to enough memory to hold the result
 * (at least 7 bytes, counting a leading sign and trailing NUL).
 *
 * It doesn't seem worth implementing this separately.
 */
int
pg_itoa(int16 i, char *a)
{
	return pg_ltoa((int32) i, a);
}

/*
 * pg_ultoa_n: converts an unsigned 32-bit integer to its string representation,
 * not NUL-terminated, and returns the length of that string representation
 *
 * Caller must ensure that 'a' points to enough memory to hold the result (at
 * least 10 bytes)
 */
int
pg_ultoa_n(uint32 value, char *a)
{
	int			olength,
				i = 0;

	/* Degenerate case */
	if (value == 0)
	{
		*a = '0';
		return 1;
	}

	olength = decimalLength32(value);

	/* Compute the result string. */
	while (value >= 10000)
	{
		const uint32 c = value - 10000 * (value / 10000);
		const uint32 c0 = (c % 100) << 1;
		const uint32 c1 = (c / 100) << 1;

		char	   *pos = a + olength - i;

		value /= 10000;

		memcpy(pos - 2, DIGIT_TABLE + c0, 2);
		memcpy(pos - 4, DIGIT_TABLE + c1, 2);
		i += 4;
	}
	if (value >= 100)
	{
		const uint32 c = (value % 100) << 1;

		char	   *pos = a + olength - i;

		value /= 100;

		memcpy(pos - 2, DIGIT_TABLE + c, 2);
		i += 2;
	}
	if (value >= 10)
	{
		const uint32 c = value << 1;

		char	   *pos = a + olength - i;

		memcpy(pos - 2, DIGIT_TABLE + c, 2);
	}
	else
	{
		*a = (char) ('0' + value);
	}

	return olength;
}

/*
 * pg_ltoa: converts a signed 32-bit integer to its string representation and
 * returns strlen(a).
 *
 * It is the caller's responsibility to ensure that a is at least 12 bytes long,
 * which is enough room to hold a minus sign, a maximally long int32, and the
 * above terminating NUL.
 */
int
pg_ltoa(int32 value, char *a)
{
	uint32		uvalue = (uint32) value;
	int			len = 0;

	if (value < 0)
	{
		uvalue = (uint32) 0 - uvalue;
		a[len++] = '-';
	}
	len += pg_ultoa_n(uvalue, a + len);
	a[len] = '\0';
	return len;
}
/* ---- pqformat.c 521..536 pq_copymsgbytes, 408..442 pq_getmsgint ---- */
/* --------------------------------
 *		pq_copymsgbytes - copy raw data from a message buffer
 *
 *		Same as above, except data is copied to caller's buffer.
 * --------------------------------
 */
void
pq_copymsgbytes(StringInfo msg, void *buf, int datalen)
{
	if (datalen < 0 || datalen > (msg->len - msg->cursor))
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("insufficient data left in message")));
	memcpy(buf, &msg->data[msg->cursor], datalen);
	msg->cursor += datalen;
}

/* --------------------------------
 *		pq_getmsgint	- get a binary integer from a message buffer
 *
 *		Values are treated as unsigned.
 * --------------------------------
 */
unsigned int
pq_getmsgint(StringInfo msg, int b)
{
	unsigned int result;
	unsigned char n8;
	uint16		n16;
	uint32		n32;

	switch (b)
	{
		case 1:
			pq_copymsgbytes(msg, &n8, 1);
			result = n8;
			break;
		case 2:
			pq_copymsgbytes(msg, &n16, 2);
			result = pg_ntoh16(n16);
			break;
		case 4:
			pq_copymsgbytes(msg, &n32, 4);
			result = pg_ntoh32(n32);
			break;
		default:
			elog(ERROR, "unsupported integer size %d", b);
			result = 0;			/* keep compiler quiet */
			break;
	}
	return result;
}

/* ---- int.c 58..253: int2 io + int2vector family ---- */

/*
 *		int2in			- converts "num" to short
 */
Datum
int2in(PG_FUNCTION_ARGS)
{
	char	   *num = PG_GETARG_CSTRING(0);

	PG_RETURN_INT16(pg_strtoint16_safe(num, fcinfo->context));
}

/*
 *		int2out			- converts short to "num"
 */
Datum
int2out(PG_FUNCTION_ARGS)
{
	int16		arg1 = PG_GETARG_INT16(0);
	char	   *result = (char *) palloc(7);	/* sign, 5 digits, '\0' */

	pg_itoa(arg1, result);
	PG_RETURN_CSTRING(result);
}

/*
 *		int2recv			- converts external binary format to int2
 */
Datum
int2recv(PG_FUNCTION_ARGS)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);

	PG_RETURN_INT16((int16) pq_getmsgint(buf, sizeof(int16)));
}

/*
 *		int2send			- converts int2 to binary format
 */
Datum
int2send(PG_FUNCTION_ARGS)
{
	int16		arg1 = PG_GETARG_INT16(0);
	StringInfoData buf;

	pq_begintypsend(&buf);
	pq_sendint16(&buf, arg1);
	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

/*
 * construct int2vector given a raw array of int2s
 *
 * If int2s is NULL then caller must fill values[] afterward
 */
int2vector *
buildint2vector(const int16 *int2s, int n)
{
	int2vector *result;

	result = (int2vector *) palloc0(Int2VectorSize(n));

	if (n > 0 && int2s)
		memcpy(result->values, int2s, n * sizeof(int16));

	/*
	 * Attach standard array header.  For historical reasons, we set the index
	 * lower bound to 0 not 1.
	 */
	SET_VARSIZE(result, Int2VectorSize(n));
	result->ndim = 1;
	result->dataoffset = 0;		/* never any nulls */
	result->elemtype = INT2OID;
	result->dim1 = n;
	result->lbound1 = 0;

	return result;
}

/*
 * validate that an array object meets the restrictions of int2vector
 *
 * We need this because there are pathways by which a general int2[] array can
 * be cast to int2vector, allowing the type's restrictions to be violated.
 * All code that receives an int2vector as a SQL parameter should check this.
 */
static void
check_valid_int2vector(const int2vector *int2Array)
{
	/*
	 * We insist on ndim == 1 and dataoffset == 0 (that is, no nulls) because
	 * otherwise the array's layout will not be what calling code expects.  We
	 * needn't be picky about the index lower bound though.  Checking elemtype
	 * is just paranoia.
	 */
	if (int2Array->ndim != 1 ||
		int2Array->dataoffset != 0 ||
		int2Array->elemtype != INT2OID)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("array is not a valid int2vector")));
}

/*
 *		int2vectorin			- converts "num num ..." to internal form
 */
Datum
int2vectorin(PG_FUNCTION_ARGS)
{
	char	   *intString = PG_GETARG_CSTRING(0);
	Node	   *escontext = fcinfo->context;
	int2vector *result;
	int			nalloc;
	int			n;

	nalloc = 32;				/* arbitrary initial size guess */
	result = (int2vector *) palloc0(Int2VectorSize(nalloc));

	for (n = 0;; n++)
	{
		long		l;
		char	   *endp;

		while (*intString && isspace((unsigned char) *intString))
			intString++;
		if (*intString == '\0')
			break;

		if (n >= nalloc)
		{
			nalloc *= 2;
			result = (int2vector *) repalloc(result, Int2VectorSize(nalloc));
		}

		errno = 0;
		l = strtol(intString, &endp, 10);

		if (intString == endp)
			ereturn(escontext, (Datum) 0,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("invalid input syntax for type %s: \"%s\"",
							"smallint", intString)));

		if (errno == ERANGE || l < SHRT_MIN || l > SHRT_MAX)
			ereturn(escontext, (Datum) 0,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					 errmsg("value \"%s\" is out of range for type %s", intString,
							"smallint")));

		if (*endp && *endp != ' ')
			ereturn(escontext, (Datum) 0,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("invalid input syntax for type %s: \"%s\"",
							"smallint", intString)));

		result->values[n] = l;
		intString = endp;
	}

	SET_VARSIZE(result, Int2VectorSize(n));
	result->ndim = 1;
	result->dataoffset = 0;		/* never any nulls */
	result->elemtype = INT2OID;
	result->dim1 = n;
	result->lbound1 = 0;

	PG_RETURN_POINTER(result);
}

/*
 *		int2vectorout		- converts internal form to "num num ..."
 */
Datum
int2vectorout(PG_FUNCTION_ARGS)
{
	int2vector *int2Array = (int2vector *) PG_GETARG_POINTER(0);
	int			num,
				nnums;
	char	   *rp;
	char	   *result;

	/* validate input before fetching dim1 */
	check_valid_int2vector(int2Array);
	nnums = int2Array->dim1;

	/* assumes sign, 5 digits, ' ' */
	rp = result = (char *) palloc(nnums * 7 + 1);
	for (num = 0; num < nnums; num++)
	{
		if (num != 0)
			*rp++ = ' ';
		rp += pg_itoa(int2Array->values[num], rp);
	}
	*rp = '\0';
	PG_RETURN_CSTRING(result);
}
/* ---- int.c 311..359: int4 io ---- */

/*
 *		int4in			- converts "num" to int4
 */
Datum
int4in(PG_FUNCTION_ARGS)
{
	char	   *num = PG_GETARG_CSTRING(0);

	PG_RETURN_INT32(pg_strtoint32_safe(num, fcinfo->context));
}

/*
 *		int4out			- converts int4 to "num"
 */
Datum
int4out(PG_FUNCTION_ARGS)
{
	int32		arg1 = PG_GETARG_INT32(0);
	char	   *result = (char *) palloc(12);	/* sign, 10 digits, '\0' */

	pg_ltoa(arg1, result);
	PG_RETURN_CSTRING(result);
}

/*
 *		int4recv			- converts external binary format to int4
 */
Datum
int4recv(PG_FUNCTION_ARGS)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);

	PG_RETURN_INT32((int32) pq_getmsgint(buf, sizeof(int32)));
}

/*
 *		int4send			- converts int4 to binary format
 */
Datum
int4send(PG_FUNCTION_ARGS)
{
	int32		arg1 = PG_GETARG_INT32(0);
	StringInfoData buf;

	pq_begintypsend(&buf);
	pq_sendint32(&buf, arg1);
	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}
/* ---- int.c 361..1526: conversions, relops, in_range, arithmetic, bit ops ---- */

/*
 *		===================
 *		CONVERSION ROUTINES
 *		===================
 */

Datum
i2toi4(PG_FUNCTION_ARGS)
{
	int16		arg1 = PG_GETARG_INT16(0);

	PG_RETURN_INT32((int32) arg1);
}

Datum
i4toi2(PG_FUNCTION_ARGS)
{
	int32		arg1 = PG_GETARG_INT32(0);

	if (unlikely(arg1 < SHRT_MIN) || unlikely(arg1 > SHRT_MAX))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("smallint out of range")));

	PG_RETURN_INT16((int16) arg1);
}

/* Cast int4 -> bool */
Datum
int4_bool(PG_FUNCTION_ARGS)
{
	if (PG_GETARG_INT32(0) == 0)
		PG_RETURN_BOOL(false);
	else
		PG_RETURN_BOOL(true);
}

/* Cast bool -> int4 */
Datum
bool_int4(PG_FUNCTION_ARGS)
{
	if (PG_GETARG_BOOL(0) == false)
		PG_RETURN_INT32(0);
	else
		PG_RETURN_INT32(1);
}

/*
 *		============================
 *		COMPARISON OPERATOR ROUTINES
 *		============================
 */

/*
 *		inteq			- returns 1 iff arg1 == arg2
 *		intne			- returns 1 iff arg1 != arg2
 *		intlt			- returns 1 iff arg1 < arg2
 *		intle			- returns 1 iff arg1 <= arg2
 *		intgt			- returns 1 iff arg1 > arg2
 *		intge			- returns 1 iff arg1 >= arg2
 */

Datum
int4eq(PG_FUNCTION_ARGS)
{
	int32		arg1 = PG_GETARG_INT32(0);
	int32		arg2 = PG_GETARG_INT32(1);

	PG_RETURN_BOOL(arg1 == arg2);
}

Datum
int4ne(PG_FUNCTION_ARGS)
{
	int32		arg1 = PG_GETARG_INT32(0);
	int32		arg2 = PG_GETARG_INT32(1);

	PG_RETURN_BOOL(arg1 != arg2);
}

Datum
int4lt(PG_FUNCTION_ARGS)
{
	int32		arg1 = PG_GETARG_INT32(0);
	int32		arg2 = PG_GETARG_INT32(1);

	PG_RETURN_BOOL(arg1 < arg2);
}

Datum
int4le(PG_FUNCTION_ARGS)
{
	int32		arg1 = PG_GETARG_INT32(0);
	int32		arg2 = PG_GETARG_INT32(1);

	PG_RETURN_BOOL(arg1 <= arg2);
}

Datum
int4gt(PG_FUNCTION_ARGS)
{
	int32		arg1 = PG_GETARG_INT32(0);
	int32		arg2 = PG_GETARG_INT32(1);

	PG_RETURN_BOOL(arg1 > arg2);
}

Datum
int4ge(PG_FUNCTION_ARGS)
{
	int32		arg1 = PG_GETARG_INT32(0);
	int32		arg2 = PG_GETARG_INT32(1);

	PG_RETURN_BOOL(arg1 >= arg2);
}

Datum
int2eq(PG_FUNCTION_ARGS)
{
	int16		arg1 = PG_GETARG_INT16(0);
	int16		arg2 = PG_GETARG_INT16(1);

	PG_RETURN_BOOL(arg1 == arg2);
}

Datum
int2ne(PG_FUNCTION_ARGS)
{
	int16		arg1 = PG_GETARG_INT16(0);
	int16		arg2 = PG_GETARG_INT16(1);

	PG_RETURN_BOOL(arg1 != arg2);
}

Datum
int2lt(PG_FUNCTION_ARGS)
{
	int16		arg1 = PG_GETARG_INT16(0);
	int16		arg2 = PG_GETARG_INT16(1);

	PG_RETURN_BOOL(arg1 < arg2);
}

Datum
int2le(PG_FUNCTION_ARGS)
{
	int16		arg1 = PG_GETARG_INT16(0);
	int16		arg2 = PG_GETARG_INT16(1);

	PG_RETURN_BOOL(arg1 <= arg2);
}

Datum
int2gt(PG_FUNCTION_ARGS)
{
	int16		arg1 = PG_GETARG_INT16(0);
	int16		arg2 = PG_GETARG_INT16(1);

	PG_RETURN_BOOL(arg1 > arg2);
}

Datum
int2ge(PG_FUNCTION_ARGS)
{
	int16		arg1 = PG_GETARG_INT16(0);
	int16		arg2 = PG_GETARG_INT16(1);

	PG_RETURN_BOOL(arg1 >= arg2);
}

Datum
int24eq(PG_FUNCTION_ARGS)
{
	int16		arg1 = PG_GETARG_INT16(0);
	int32		arg2 = PG_GETARG_INT32(1);

	PG_RETURN_BOOL(arg1 == arg2);
}

Datum
int24ne(PG_FUNCTION_ARGS)
{
	int16		arg1 = PG_GETARG_INT16(0);
	int32		arg2 = PG_GETARG_INT32(1);

	PG_RETURN_BOOL(arg1 != arg2);
}

Datum
int24lt(PG_FUNCTION_ARGS)
{
	int16		arg1 = PG_GETARG_INT16(0);
	int32		arg2 = PG_GETARG_INT32(1);

	PG_RETURN_BOOL(arg1 < arg2);
}

Datum
int24le(PG_FUNCTION_ARGS)
{
	int16		arg1 = PG_GETARG_INT16(0);
	int32		arg2 = PG_GETARG_INT32(1);

	PG_RETURN_BOOL(arg1 <= arg2);
}

Datum
int24gt(PG_FUNCTION_ARGS)
{
	int16		arg1 = PG_GETARG_INT16(0);
	int32		arg2 = PG_GETARG_INT32(1);

	PG_RETURN_BOOL(arg1 > arg2);
}

Datum
int24ge(PG_FUNCTION_ARGS)
{
	int16		arg1 = PG_GETARG_INT16(0);
	int32		arg2 = PG_GETARG_INT32(1);

	PG_RETURN_BOOL(arg1 >= arg2);
}

Datum
int42eq(PG_FUNCTION_ARGS)
{
	int32		arg1 = PG_GETARG_INT32(0);
	int16		arg2 = PG_GETARG_INT16(1);

	PG_RETURN_BOOL(arg1 == arg2);
}

Datum
int42ne(PG_FUNCTION_ARGS)
{
	int32		arg1 = PG_GETARG_INT32(0);
	int16		arg2 = PG_GETARG_INT16(1);

	PG_RETURN_BOOL(arg1 != arg2);
}

Datum
int42lt(PG_FUNCTION_ARGS)
{
	int32		arg1 = PG_GETARG_INT32(0);
	int16		arg2 = PG_GETARG_INT16(1);

	PG_RETURN_BOOL(arg1 < arg2);
}

Datum
int42le(PG_FUNCTION_ARGS)
{
	int32		arg1 = PG_GETARG_INT32(0);
	int16		arg2 = PG_GETARG_INT16(1);

	PG_RETURN_BOOL(arg1 <= arg2);
}

Datum
int42gt(PG_FUNCTION_ARGS)
{
	int32		arg1 = PG_GETARG_INT32(0);
	int16		arg2 = PG_GETARG_INT16(1);

	PG_RETURN_BOOL(arg1 > arg2);
}

Datum
int42ge(PG_FUNCTION_ARGS)
{
	int32		arg1 = PG_GETARG_INT32(0);
	int16		arg2 = PG_GETARG_INT16(1);

	PG_RETURN_BOOL(arg1 >= arg2);
}


/*----------------------------------------------------------
 *	in_range functions for int4 and int2,
 *	including cross-data-type comparisons.
 *
 *	Note: we provide separate intN_int8 functions for performance
 *	reasons.  This forces also providing intN_int2, else cases with a
 *	smallint offset value would fail to resolve which function to use.
 *	But that's an unlikely situation, so don't duplicate code for it.
 *---------------------------------------------------------*/

Datum
in_range_int4_int4(PG_FUNCTION_ARGS)
{
	int32		val = PG_GETARG_INT32(0);
	int32		base = PG_GETARG_INT32(1);
	int32		offset = PG_GETARG_INT32(2);
	bool		sub = PG_GETARG_BOOL(3);
	bool		less = PG_GETARG_BOOL(4);
	int32		sum;

	if (offset < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE),
				 errmsg("invalid preceding or following size in window function")));

	if (sub)
		offset = -offset;		/* cannot overflow */

	if (unlikely(pg_add_s32_overflow(base, offset, &sum)))
	{
		/*
		 * If sub is false, the true sum is surely more than val, so correct
		 * answer is the same as "less".  If sub is true, the true sum is
		 * surely less than val, so the answer is "!less".
		 */
		PG_RETURN_BOOL(sub ? !less : less);
	}

	if (less)
		PG_RETURN_BOOL(val <= sum);
	else
		PG_RETURN_BOOL(val >= sum);
}

Datum
in_range_int4_int2(PG_FUNCTION_ARGS)
{
	/* Doesn't seem worth duplicating code for, so just invoke int4_int4 */
	return DirectFunctionCall5(in_range_int4_int4,
							   PG_GETARG_DATUM(0),
							   PG_GETARG_DATUM(1),
							   Int32GetDatum((int32) PG_GETARG_INT16(2)),
							   PG_GETARG_DATUM(3),
							   PG_GETARG_DATUM(4));
}

Datum
in_range_int4_int8(PG_FUNCTION_ARGS)
{
	/* We must do all the math in int64 */
	int64		val = (int64) PG_GETARG_INT32(0);
	int64		base = (int64) PG_GETARG_INT32(1);
	int64		offset = PG_GETARG_INT64(2);
	bool		sub = PG_GETARG_BOOL(3);
	bool		less = PG_GETARG_BOOL(4);
	int64		sum;

	if (offset < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE),
				 errmsg("invalid preceding or following size in window function")));

	if (sub)
		offset = -offset;		/* cannot overflow */

	if (unlikely(pg_add_s64_overflow(base, offset, &sum)))
	{
		/*
		 * If sub is false, the true sum is surely more than val, so correct
		 * answer is the same as "less".  If sub is true, the true sum is
		 * surely less than val, so the answer is "!less".
		 */
		PG_RETURN_BOOL(sub ? !less : less);
	}

	if (less)
		PG_RETURN_BOOL(val <= sum);
	else
		PG_RETURN_BOOL(val >= sum);
}

Datum
in_range_int2_int4(PG_FUNCTION_ARGS)
{
	/* We must do all the math in int32 */
	int32		val = (int32) PG_GETARG_INT16(0);
	int32		base = (int32) PG_GETARG_INT16(1);
	int32		offset = PG_GETARG_INT32(2);
	bool		sub = PG_GETARG_BOOL(3);
	bool		less = PG_GETARG_BOOL(4);
	int32		sum;

	if (offset < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE),
				 errmsg("invalid preceding or following size in window function")));

	if (sub)
		offset = -offset;		/* cannot overflow */

	if (unlikely(pg_add_s32_overflow(base, offset, &sum)))
	{
		/*
		 * If sub is false, the true sum is surely more than val, so correct
		 * answer is the same as "less".  If sub is true, the true sum is
		 * surely less than val, so the answer is "!less".
		 */
		PG_RETURN_BOOL(sub ? !less : less);
	}

	if (less)
		PG_RETURN_BOOL(val <= sum);
	else
		PG_RETURN_BOOL(val >= sum);
}

Datum
in_range_int2_int2(PG_FUNCTION_ARGS)
{
	/* Doesn't seem worth duplicating code for, so just invoke int2_int4 */
	return DirectFunctionCall5(in_range_int2_int4,
							   PG_GETARG_DATUM(0),
							   PG_GETARG_DATUM(1),
							   Int32GetDatum((int32) PG_GETARG_INT16(2)),
							   PG_GETARG_DATUM(3),
							   PG_GETARG_DATUM(4));
}

Datum
in_range_int2_int8(PG_FUNCTION_ARGS)
{
	/* Doesn't seem worth duplicating code for, so just invoke int4_int8 */
	return DirectFunctionCall5(in_range_int4_int8,
							   Int32GetDatum((int32) PG_GETARG_INT16(0)),
							   Int32GetDatum((int32) PG_GETARG_INT16(1)),
							   PG_GETARG_DATUM(2),
							   PG_GETARG_DATUM(3),
							   PG_GETARG_DATUM(4));
}


/*
 *		int[24]pl		- returns arg1 + arg2
 *		int[24]mi		- returns arg1 - arg2
 *		int[24]mul		- returns arg1 * arg2
 *		int[24]div		- returns arg1 / arg2
 */

Datum
int4um(PG_FUNCTION_ARGS)
{
	int32		arg = PG_GETARG_INT32(0);

	if (unlikely(arg == PG_INT32_MIN))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("integer out of range")));
	PG_RETURN_INT32(-arg);
}

Datum
int4up(PG_FUNCTION_ARGS)
{
	int32		arg = PG_GETARG_INT32(0);

	PG_RETURN_INT32(arg);
}

Datum
int4pl(PG_FUNCTION_ARGS)
{
	int32		arg1 = PG_GETARG_INT32(0);
	int32		arg2 = PG_GETARG_INT32(1);
	int32		result;

	if (unlikely(pg_add_s32_overflow(arg1, arg2, &result)))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("integer out of range")));
	PG_RETURN_INT32(result);
}

Datum
int4mi(PG_FUNCTION_ARGS)
{
	int32		arg1 = PG_GETARG_INT32(0);
	int32		arg2 = PG_GETARG_INT32(1);
	int32		result;

	if (unlikely(pg_sub_s32_overflow(arg1, arg2, &result)))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("integer out of range")));
	PG_RETURN_INT32(result);
}

Datum
int4mul(PG_FUNCTION_ARGS)
{
	int32		arg1 = PG_GETARG_INT32(0);
	int32		arg2 = PG_GETARG_INT32(1);
	int32		result;

	if (unlikely(pg_mul_s32_overflow(arg1, arg2, &result)))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("integer out of range")));
	PG_RETURN_INT32(result);
}

Datum
int4div(PG_FUNCTION_ARGS)
{
	int32		arg1 = PG_GETARG_INT32(0);
	int32		arg2 = PG_GETARG_INT32(1);
	int32		result;

	if (arg2 == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_DIVISION_BY_ZERO),
				 errmsg("division by zero")));
		/* ensure compiler realizes we mustn't reach the division (gcc bug) */
		PG_RETURN_NULL();
	}

	/*
	 * INT_MIN / -1 is problematic, since the result can't be represented on a
	 * two's-complement machine.  Some machines produce INT_MIN, some produce
	 * zero, some throw an exception.  We can dodge the problem by recognizing
	 * that division by -1 is the same as negation.
	 */
	if (arg2 == -1)
	{
		if (unlikely(arg1 == PG_INT32_MIN))
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					 errmsg("integer out of range")));
		result = -arg1;
		PG_RETURN_INT32(result);
	}

	/* No overflow is possible */

	result = arg1 / arg2;

	PG_RETURN_INT32(result);
}

Datum
int4inc(PG_FUNCTION_ARGS)
{
	int32		arg = PG_GETARG_INT32(0);
	int32		result;

	if (unlikely(pg_add_s32_overflow(arg, 1, &result)))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("integer out of range")));

	PG_RETURN_INT32(result);
}

Datum
int2um(PG_FUNCTION_ARGS)
{
	int16		arg = PG_GETARG_INT16(0);

	if (unlikely(arg == PG_INT16_MIN))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("smallint out of range")));
	PG_RETURN_INT16(-arg);
}

Datum
int2up(PG_FUNCTION_ARGS)
{
	int16		arg = PG_GETARG_INT16(0);

	PG_RETURN_INT16(arg);
}

Datum
int2pl(PG_FUNCTION_ARGS)
{
	int16		arg1 = PG_GETARG_INT16(0);
	int16		arg2 = PG_GETARG_INT16(1);
	int16		result;

	if (unlikely(pg_add_s16_overflow(arg1, arg2, &result)))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("smallint out of range")));
	PG_RETURN_INT16(result);
}

Datum
int2mi(PG_FUNCTION_ARGS)
{
	int16		arg1 = PG_GETARG_INT16(0);
	int16		arg2 = PG_GETARG_INT16(1);
	int16		result;

	if (unlikely(pg_sub_s16_overflow(arg1, arg2, &result)))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("smallint out of range")));
	PG_RETURN_INT16(result);
}

Datum
int2mul(PG_FUNCTION_ARGS)
{
	int16		arg1 = PG_GETARG_INT16(0);
	int16		arg2 = PG_GETARG_INT16(1);
	int16		result;

	if (unlikely(pg_mul_s16_overflow(arg1, arg2, &result)))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("smallint out of range")));

	PG_RETURN_INT16(result);
}

Datum
int2div(PG_FUNCTION_ARGS)
{
	int16		arg1 = PG_GETARG_INT16(0);
	int16		arg2 = PG_GETARG_INT16(1);
	int16		result;

	if (arg2 == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_DIVISION_BY_ZERO),
				 errmsg("division by zero")));
		/* ensure compiler realizes we mustn't reach the division (gcc bug) */
		PG_RETURN_NULL();
	}

	/*
	 * SHRT_MIN / -1 is problematic, since the result can't be represented on
	 * a two's-complement machine.  Some machines produce SHRT_MIN, some
	 * produce zero, some throw an exception.  We can dodge the problem by
	 * recognizing that division by -1 is the same as negation.
	 */
	if (arg2 == -1)
	{
		if (unlikely(arg1 == PG_INT16_MIN))
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					 errmsg("smallint out of range")));
		result = -arg1;
		PG_RETURN_INT16(result);
	}

	/* No overflow is possible */

	result = arg1 / arg2;

	PG_RETURN_INT16(result);
}

Datum
int24pl(PG_FUNCTION_ARGS)
{
	int16		arg1 = PG_GETARG_INT16(0);
	int32		arg2 = PG_GETARG_INT32(1);
	int32		result;

	if (unlikely(pg_add_s32_overflow((int32) arg1, arg2, &result)))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("integer out of range")));
	PG_RETURN_INT32(result);
}

Datum
int24mi(PG_FUNCTION_ARGS)
{
	int16		arg1 = PG_GETARG_INT16(0);
	int32		arg2 = PG_GETARG_INT32(1);
	int32		result;

	if (unlikely(pg_sub_s32_overflow((int32) arg1, arg2, &result)))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("integer out of range")));
	PG_RETURN_INT32(result);
}

Datum
int24mul(PG_FUNCTION_ARGS)
{
	int16		arg1 = PG_GETARG_INT16(0);
	int32		arg2 = PG_GETARG_INT32(1);
	int32		result;

	if (unlikely(pg_mul_s32_overflow((int32) arg1, arg2, &result)))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("integer out of range")));
	PG_RETURN_INT32(result);
}

Datum
int24div(PG_FUNCTION_ARGS)
{
	int16		arg1 = PG_GETARG_INT16(0);
	int32		arg2 = PG_GETARG_INT32(1);

	if (unlikely(arg2 == 0))
	{
		ereport(ERROR,
				(errcode(ERRCODE_DIVISION_BY_ZERO),
				 errmsg("division by zero")));
		/* ensure compiler realizes we mustn't reach the division (gcc bug) */
		PG_RETURN_NULL();
	}

	/* No overflow is possible */
	PG_RETURN_INT32((int32) arg1 / arg2);
}

Datum
int42pl(PG_FUNCTION_ARGS)
{
	int32		arg1 = PG_GETARG_INT32(0);
	int16		arg2 = PG_GETARG_INT16(1);
	int32		result;

	if (unlikely(pg_add_s32_overflow(arg1, (int32) arg2, &result)))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("integer out of range")));
	PG_RETURN_INT32(result);
}

Datum
int42mi(PG_FUNCTION_ARGS)
{
	int32		arg1 = PG_GETARG_INT32(0);
	int16		arg2 = PG_GETARG_INT16(1);
	int32		result;

	if (unlikely(pg_sub_s32_overflow(arg1, (int32) arg2, &result)))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("integer out of range")));
	PG_RETURN_INT32(result);
}

Datum
int42mul(PG_FUNCTION_ARGS)
{
	int32		arg1 = PG_GETARG_INT32(0);
	int16		arg2 = PG_GETARG_INT16(1);
	int32		result;

	if (unlikely(pg_mul_s32_overflow(arg1, (int32) arg2, &result)))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("integer out of range")));
	PG_RETURN_INT32(result);
}

Datum
int42div(PG_FUNCTION_ARGS)
{
	int32		arg1 = PG_GETARG_INT32(0);
	int16		arg2 = PG_GETARG_INT16(1);
	int32		result;

	if (unlikely(arg2 == 0))
	{
		ereport(ERROR,
				(errcode(ERRCODE_DIVISION_BY_ZERO),
				 errmsg("division by zero")));
		/* ensure compiler realizes we mustn't reach the division (gcc bug) */
		PG_RETURN_NULL();
	}

	/*
	 * INT_MIN / -1 is problematic, since the result can't be represented on a
	 * two's-complement machine.  Some machines produce INT_MIN, some produce
	 * zero, some throw an exception.  We can dodge the problem by recognizing
	 * that division by -1 is the same as negation.
	 */
	if (arg2 == -1)
	{
		if (unlikely(arg1 == PG_INT32_MIN))
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					 errmsg("integer out of range")));
		result = -arg1;
		PG_RETURN_INT32(result);
	}

	/* No overflow is possible */

	result = arg1 / arg2;

	PG_RETURN_INT32(result);
}

Datum
int4mod(PG_FUNCTION_ARGS)
{
	int32		arg1 = PG_GETARG_INT32(0);
	int32		arg2 = PG_GETARG_INT32(1);

	if (unlikely(arg2 == 0))
	{
		ereport(ERROR,
				(errcode(ERRCODE_DIVISION_BY_ZERO),
				 errmsg("division by zero")));
		/* ensure compiler realizes we mustn't reach the division (gcc bug) */
		PG_RETURN_NULL();
	}

	/*
	 * Some machines throw a floating-point exception for INT_MIN % -1, which
	 * is a bit silly since the correct answer is perfectly well-defined,
	 * namely zero.
	 */
	if (arg2 == -1)
		PG_RETURN_INT32(0);

	/* No overflow is possible */

	PG_RETURN_INT32(arg1 % arg2);
}

Datum
int2mod(PG_FUNCTION_ARGS)
{
	int16		arg1 = PG_GETARG_INT16(0);
	int16		arg2 = PG_GETARG_INT16(1);

	if (unlikely(arg2 == 0))
	{
		ereport(ERROR,
				(errcode(ERRCODE_DIVISION_BY_ZERO),
				 errmsg("division by zero")));
		/* ensure compiler realizes we mustn't reach the division (gcc bug) */
		PG_RETURN_NULL();
	}

	/*
	 * Some machines throw a floating-point exception for INT_MIN % -1, which
	 * is a bit silly since the correct answer is perfectly well-defined,
	 * namely zero.  (It's not clear this ever happens when dealing with
	 * int16, but we might as well have the test for safety.)
	 */
	if (arg2 == -1)
		PG_RETURN_INT16(0);

	/* No overflow is possible */

	PG_RETURN_INT16(arg1 % arg2);
}


/* int[24]abs()
 * Absolute value
 */
Datum
int4abs(PG_FUNCTION_ARGS)
{
	int32		arg1 = PG_GETARG_INT32(0);
	int32		result;

	if (unlikely(arg1 == PG_INT32_MIN))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("integer out of range")));
	result = (arg1 < 0) ? -arg1 : arg1;
	PG_RETURN_INT32(result);
}

Datum
int2abs(PG_FUNCTION_ARGS)
{
	int16		arg1 = PG_GETARG_INT16(0);
	int16		result;

	if (unlikely(arg1 == PG_INT16_MIN))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("smallint out of range")));
	result = (arg1 < 0) ? -arg1 : arg1;
	PG_RETURN_INT16(result);
}

/*
 * Greatest Common Divisor
 *
 * Returns the largest positive integer that exactly divides both inputs.
 * Special cases:
 *   - gcd(x, 0) = gcd(0, x) = abs(x)
 *   		because 0 is divisible by anything
 *   - gcd(0, 0) = 0
 *   		complies with the previous definition and is a common convention
 *
 * Special care must be taken if either input is INT_MIN --- gcd(0, INT_MIN),
 * gcd(INT_MIN, 0) and gcd(INT_MIN, INT_MIN) are all equal to abs(INT_MIN),
 * which cannot be represented as a 32-bit signed integer.
 */
static int32
int4gcd_internal(int32 arg1, int32 arg2)
{
	int32		swap;
	int32		a1,
				a2;

	/*
	 * Put the greater absolute value in arg1.
	 *
	 * This would happen automatically in the loop below, but avoids an
	 * expensive modulo operation, and simplifies the special-case handling
	 * for INT_MIN below.
	 *
	 * We do this in negative space in order to handle INT_MIN.
	 */
	a1 = (arg1 < 0) ? arg1 : -arg1;
	a2 = (arg2 < 0) ? arg2 : -arg2;
	if (a1 > a2)
	{
		swap = arg1;
		arg1 = arg2;
		arg2 = swap;
	}

	/* Special care needs to be taken with INT_MIN.  See comments above. */
	if (arg1 == PG_INT32_MIN)
	{
		if (arg2 == 0 || arg2 == PG_INT32_MIN)
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					 errmsg("integer out of range")));

		/*
		 * Some machines throw a floating-point exception for INT_MIN % -1,
		 * which is a bit silly since the correct answer is perfectly
		 * well-defined, namely zero.  Guard against this and just return the
		 * result, gcd(INT_MIN, -1) = 1.
		 */
		if (arg2 == -1)
			return 1;
	}

	/* Use the Euclidean algorithm to find the GCD */
	while (arg2 != 0)
	{
		swap = arg2;
		arg2 = arg1 % arg2;
		arg1 = swap;
	}

	/*
	 * Make sure the result is positive. (We know we don't have INT_MIN
	 * anymore).
	 */
	if (arg1 < 0)
		arg1 = -arg1;

	return arg1;
}

Datum
int4gcd(PG_FUNCTION_ARGS)
{
	int32		arg1 = PG_GETARG_INT32(0);
	int32		arg2 = PG_GETARG_INT32(1);
	int32		result;

	result = int4gcd_internal(arg1, arg2);

	PG_RETURN_INT32(result);
}

/*
 * Least Common Multiple
 */
Datum
int4lcm(PG_FUNCTION_ARGS)
{
	int32		arg1 = PG_GETARG_INT32(0);
	int32		arg2 = PG_GETARG_INT32(1);
	int32		gcd;
	int32		result;

	/*
	 * Handle lcm(x, 0) = lcm(0, x) = 0 as a special case.  This prevents a
	 * division-by-zero error below when x is zero, and an overflow error from
	 * the GCD computation when x = INT_MIN.
	 */
	if (arg1 == 0 || arg2 == 0)
		PG_RETURN_INT32(0);

	/* lcm(x, y) = abs(x / gcd(x, y) * y) */
	gcd = int4gcd_internal(arg1, arg2);
	arg1 = arg1 / gcd;

	if (unlikely(pg_mul_s32_overflow(arg1, arg2, &result)))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("integer out of range")));

	/* If the result is INT_MIN, it cannot be represented. */
	if (unlikely(result == PG_INT32_MIN))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("integer out of range")));

	if (result < 0)
		result = -result;

	PG_RETURN_INT32(result);
}

Datum
int2larger(PG_FUNCTION_ARGS)
{
	int16		arg1 = PG_GETARG_INT16(0);
	int16		arg2 = PG_GETARG_INT16(1);

	PG_RETURN_INT16((arg1 > arg2) ? arg1 : arg2);
}

Datum
int2smaller(PG_FUNCTION_ARGS)
{
	int16		arg1 = PG_GETARG_INT16(0);
	int16		arg2 = PG_GETARG_INT16(1);

	PG_RETURN_INT16((arg1 < arg2) ? arg1 : arg2);
}

Datum
int4larger(PG_FUNCTION_ARGS)
{
	int32		arg1 = PG_GETARG_INT32(0);
	int32		arg2 = PG_GETARG_INT32(1);

	PG_RETURN_INT32((arg1 > arg2) ? arg1 : arg2);
}

Datum
int4smaller(PG_FUNCTION_ARGS)
{
	int32		arg1 = PG_GETARG_INT32(0);
	int32		arg2 = PG_GETARG_INT32(1);

	PG_RETURN_INT32((arg1 < arg2) ? arg1 : arg2);
}

/*
 * Bit-pushing operators
 *
 *		int[24]and		- returns arg1 & arg2
 *		int[24]or		- returns arg1 | arg2
 *		int[24]xor		- returns arg1 # arg2
 *		int[24]not		- returns ~arg1
 *		int[24]shl		- returns arg1 << arg2
 *		int[24]shr		- returns arg1 >> arg2
 */

Datum
int4and(PG_FUNCTION_ARGS)
{
	int32		arg1 = PG_GETARG_INT32(0);
	int32		arg2 = PG_GETARG_INT32(1);

	PG_RETURN_INT32(arg1 & arg2);
}

Datum
int4or(PG_FUNCTION_ARGS)
{
	int32		arg1 = PG_GETARG_INT32(0);
	int32		arg2 = PG_GETARG_INT32(1);

	PG_RETURN_INT32(arg1 | arg2);
}

Datum
int4xor(PG_FUNCTION_ARGS)
{
	int32		arg1 = PG_GETARG_INT32(0);
	int32		arg2 = PG_GETARG_INT32(1);

	PG_RETURN_INT32(arg1 ^ arg2);
}

Datum
int4shl(PG_FUNCTION_ARGS)
{
	int32		arg1 = PG_GETARG_INT32(0);
	int32		arg2 = PG_GETARG_INT32(1);

	PG_RETURN_INT32(arg1 << arg2);
}

Datum
int4shr(PG_FUNCTION_ARGS)
{
	int32		arg1 = PG_GETARG_INT32(0);
	int32		arg2 = PG_GETARG_INT32(1);

	PG_RETURN_INT32(arg1 >> arg2);
}

Datum
int4not(PG_FUNCTION_ARGS)
{
	int32		arg1 = PG_GETARG_INT32(0);

	PG_RETURN_INT32(~arg1);
}

Datum
int2and(PG_FUNCTION_ARGS)
{
	int16		arg1 = PG_GETARG_INT16(0);
	int16		arg2 = PG_GETARG_INT16(1);

	PG_RETURN_INT16(arg1 & arg2);
}

Datum
int2or(PG_FUNCTION_ARGS)
{
	int16		arg1 = PG_GETARG_INT16(0);
	int16		arg2 = PG_GETARG_INT16(1);

	PG_RETURN_INT16(arg1 | arg2);
}

Datum
int2xor(PG_FUNCTION_ARGS)
{
	int16		arg1 = PG_GETARG_INT16(0);
	int16		arg2 = PG_GETARG_INT16(1);

	PG_RETURN_INT16(arg1 ^ arg2);
}

Datum
int2not(PG_FUNCTION_ARGS)
{
	int16		arg1 = PG_GETARG_INT16(0);

	PG_RETURN_INT16(~arg1);
}


Datum
int2shl(PG_FUNCTION_ARGS)
{
	int16		arg1 = PG_GETARG_INT16(0);
	int32		arg2 = PG_GETARG_INT32(1);

	PG_RETURN_INT16(arg1 << arg2);
}

Datum
int2shr(PG_FUNCTION_ARGS)
{
	int16		arg1 = PG_GETARG_INT16(0);
	int32		arg2 = PG_GETARG_INT32(1);

	PG_RETURN_INT16(arg1 >> arg2);
}

/* ====================================================================
 * Dispatchers (shim, not PG code): the exported pg_diff_* entry points
 * the Rust differential harness calls. Control flow only.
 * ==================================================================== */

/* int2in/int4in. Returns 0 = ok (*out valid); >0 = hard errclass; <0 =
 * -errclass caught softly (escontext path; *out = the ereturn dummy, 0). */
int
pg_diff_int2in(const char *num, int soft, int16_t *out)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	static _Thread_local int soft_sentinel;
	MiniFcinfo	fc = {{0}};

	pg_diff_arena_reset();
	pg_diff_int_errcode = 0;
	if (setjmp(pg_diff_int_jb))
		return pg_diff_int_errcode;
	fc.i[0] = (int64) (uintptr_t) num;
	fc.context = soft ? (Node *) &soft_sentinel : NULL;
	*out = (int16) (uint16) int2in(&fc);
	return pg_diff_int_errcode ? -pg_diff_int_errcode : 0;
}

int
pg_diff_int4in(const char *num, int soft, int32_t *out)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	static _Thread_local int soft_sentinel;
	MiniFcinfo	fc = {{0}};

	pg_diff_arena_reset();
	pg_diff_int_errcode = 0;
	if (setjmp(pg_diff_int_jb))
		return pg_diff_int_errcode;
	fc.i[0] = (int64) (uintptr_t) num;
	fc.context = soft ? (Node *) &soft_sentinel : NULL;
	*out = (int32) (uint32) int4in(&fc);
	return pg_diff_int_errcode ? -pg_diff_int_errcode : 0;
}

/* int2out/int4out through the shipped palloc'd-cstring shape; copies into
 * buf (>= 12) and returns strlen. */
int
pg_diff_int2out(int16_t val, char *buf)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	MiniFcinfo	fc = {{0}};
	char	   *res;
	int			len;

	pg_diff_arena_reset();
	pg_diff_int_errcode = 0;
	if (setjmp(pg_diff_int_jb))
		return -pg_diff_int_errcode;
	fc.i[0] = (int64) val;
	res = (char *) (uintptr_t) int2out(&fc);
	len = (int) strlen(res);
	memcpy(buf, res, len + 1);
	return len;
}

int
pg_diff_int4out(int32_t val, char *buf)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	MiniFcinfo	fc = {{0}};
	char	   *res;
	int			len;

	pg_diff_arena_reset();
	pg_diff_int_errcode = 0;
	if (setjmp(pg_diff_int_jb))
		return -pg_diff_int_errcode;
	fc.i[0] = (int64) val;
	res = (char *) (uintptr_t) int4out(&fc);
	len = (int) strlen(res);
	memcpy(buf, res, len + 1);
	return len;
}

/* int2vectorin. On ok copies the full varlena image (VARSIZE bytes) into
 * out_img (cap out_cap) and sets *out_len. Returns 0 ok / >0 hard errclass
 * / <0 soft errclass / 99 image-too-big (driver caps input length). */
int
pg_diff_int2vectorin(const char *str, int soft, unsigned char *out_img,
					 int out_cap, int *out_len)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	static _Thread_local int soft_sentinel;
	MiniFcinfo	fc = {{0}};
	int2vector *res;
	uint32		sz;

	pg_diff_arena_reset();
	pg_diff_int_errcode = 0;
	if (setjmp(pg_diff_int_jb))
		return pg_diff_int_errcode;
	fc.i[0] = (int64) (uintptr_t) str;
	fc.context = soft ? (Node *) &soft_sentinel : NULL;
	res = (int2vector *) (uintptr_t) int2vectorin(&fc);
	if (pg_diff_int_errcode)
		return -pg_diff_int_errcode;	/* soft error: result is (Datum) 0 */
	sz = (*(uint32 *) res) >> 2;		/* VARSIZE_4B, little-endian */
	if ((int) sz > out_cap)
		return 99;
	memcpy(out_img, res, sz);
	*out_len = (int) sz;
	return 0;
}

/* int2vectorout over a caller-supplied int2vector image. Copies the
 * cstring into buf (cap buflen) and returns strlen, or -errclass. */
int
pg_diff_int2vectorout(const unsigned char *img, char *buf, int buflen)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	MiniFcinfo	fc = {{0}};
	char	   *res;
	int			len;

	pg_diff_arena_reset();
	pg_diff_int_errcode = 0;
	if (setjmp(pg_diff_int_jb))
		return -pg_diff_int_errcode;
	fc.i[0] = (int64) (uintptr_t) img;
	res = (char *) (uintptr_t) int2vectorout(&fc);
	len = (int) strlen(res);
	assert(len < buflen);
	memcpy(buf, res, len + 1);
	return len;
}

/* int2recv/int4recv over a wire buffer. 0 = ok; errclass on ereport. */
int
pg_diff_int2recv(const unsigned char *data, int len, int16_t *out)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	StringInfoData msg;
	MiniFcinfo	fc = {{0}};

	pg_diff_arena_reset();
	pg_diff_int_errcode = 0;
	if (setjmp(pg_diff_int_jb))
		return pg_diff_int_errcode;
	msg.data = (char *) data;
	msg.len = len;
	msg.maxlen = len;
	msg.cursor = 0;
	fc.i[0] = (int64) (uintptr_t) &msg;
	*out = (int16) (uint16) int2recv(&fc);
	return 0;
}

int
pg_diff_int4recv(const unsigned char *data, int len, int32_t *out)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	StringInfoData msg;
	MiniFcinfo	fc = {{0}};

	pg_diff_arena_reset();
	pg_diff_int_errcode = 0;
	if (setjmp(pg_diff_int_jb))
		return pg_diff_int_errcode;
	msg.data = (char *) data;
	msg.len = len;
	msg.maxlen = len;
	msg.cursor = 0;
	fc.i[0] = (int64) (uintptr_t) &msg;
	*out = (int32) (uint32) int4recv(&fc);
	return 0;
}

/* int2send/int4send: full bytea image (4B varlena header + payload) into
 * buf (>= 64); returns image length. */
int
pg_diff_int2send(int16_t val, unsigned char *buf)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	MiniFcinfo	fc = {{0}};
	char		backing[64];
	StringInfoData si;
	bytea	   *res;
	uint32		sz;

	pg_diff_arena_reset();
	pg_diff_int_errcode = 0;
	if (setjmp(pg_diff_int_jb))
		return -pg_diff_int_errcode;
	si.data = backing;
	si.maxlen = (int) sizeof(backing);
	fc.i[0] = (int64) val;
	{
		/* int2send body, verbatim control flow on the shimmed StringInfo */
		int16		arg1 = (int16) fc.i[0];
		StringInfoData *bufp = &si;

		pq_begintypsend(bufp);
		pq_sendint16(bufp, arg1);
		res = pq_endtypsend(bufp);
	}
	sz = (*(uint32 *) res) >> 2;
	memcpy(buf, res, sz);
	return (int) sz;
}

int
pg_diff_int4send(int32_t val, unsigned char *buf)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	MiniFcinfo	fc = {{0}};
	char		backing[64];
	StringInfoData si;
	bytea	   *res;
	uint32		sz;

	pg_diff_arena_reset();
	pg_diff_int_errcode = 0;
	if (setjmp(pg_diff_int_jb))
		return -pg_diff_int_errcode;
	si.data = backing;
	si.maxlen = (int) sizeof(backing);
	fc.i[0] = (int64) val;
	{
		int32		arg1 = (int32) fc.i[0];
		StringInfoData *bufp = &si;

		pq_begintypsend(bufp);
		pq_sendint32(bufp, arg1);
		res = pq_endtypsend(bufp);
	}
	sz = (*(uint32 *) res) >> 2;
	memcpy(buf, res, sz);
	return (int) sz;
}

/*
 * Whole-family dispatcher. fn_id selects the verbatim function; a/b are
 * integer args (cross-width functions read them through the PG_GETARG_*
 * casts exactly as fmgr would). Returns 0 = ok, errclass on ereport.
 */
int
pg_diff_int_fn(int fn_id, int64_t a, int64_t b, int64_t c, int sub, int less,
			   int64_t *out)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	MiniFcinfo	fc = {{0}};
	Datum		d = 0;

	pg_diff_arena_reset();
	pg_diff_int_errcode = 0;
	if (setjmp(pg_diff_int_jb))
		return pg_diff_int_errcode;
	fc.i[0] = a;
	fc.i[1] = b;
	fc.i[2] = c;

	switch (fn_id)
	{
		case 1: d = int4um(&fc); break;
		case 2: d = int4up(&fc); break;
		case 3: d = int4pl(&fc); break;
		case 4: d = int4mi(&fc); break;
		case 5: d = int4mul(&fc); break;
		case 6: d = int4div(&fc); break;
		case 7: d = int4abs(&fc); break;
		case 8: d = int4mod(&fc); break;
		case 9: d = int4gcd(&fc); break;
		case 10: d = int4lcm(&fc); break;
		case 11: d = int4inc(&fc); break;
		case 12: d = int2um(&fc); break;
		case 13: d = int2up(&fc); break;
		case 14: d = int2pl(&fc); break;
		case 15: d = int2mi(&fc); break;
		case 16: d = int2mul(&fc); break;
		case 17: d = int2div(&fc); break;
		case 18: d = int2abs(&fc); break;
		case 19: d = int2mod(&fc); break;
		case 20: d = int2larger(&fc); break;
		case 21: d = int2smaller(&fc); break;
		case 22: d = int4larger(&fc); break;
		case 23: d = int4smaller(&fc); break;
		case 24: d = int24pl(&fc); break;
		case 25: d = int24mi(&fc); break;
		case 26: d = int24mul(&fc); break;
		case 27: d = int24div(&fc); break;
		case 28: d = int42pl(&fc); break;
		case 29: d = int42mi(&fc); break;
		case 30: d = int42mul(&fc); break;
		case 31: d = int42div(&fc); break;
		case 32: d = int4and(&fc); break;
		case 33: d = int4or(&fc); break;
		case 34: d = int4xor(&fc); break;
		case 35: d = int4not(&fc); break;
		case 36: d = int4shl(&fc); break;
		case 37: d = int4shr(&fc); break;
		case 38: d = int2and(&fc); break;
		case 39: d = int2or(&fc); break;
		case 40: d = int2xor(&fc); break;
		case 41: d = int2not(&fc); break;
		case 42: d = int2shl(&fc); break;
		case 43: d = int2shr(&fc); break;
		case 44: d = i2toi4(&fc); break;
		case 45: d = i4toi2(&fc); break;
		case 46: d = int4_bool(&fc); break;
		case 47: d = bool_int4(&fc); break;
		case 48: d = int4eq(&fc); break;
		case 49: d = int4ne(&fc); break;
		case 50: d = int4lt(&fc); break;
		case 51: d = int4le(&fc); break;
		case 52: d = int4gt(&fc); break;
		case 53: d = int4ge(&fc); break;
		case 54: d = int2eq(&fc); break;
		case 55: d = int2ne(&fc); break;
		case 56: d = int2lt(&fc); break;
		case 57: d = int2le(&fc); break;
		case 58: d = int2gt(&fc); break;
		case 59: d = int2ge(&fc); break;
		case 60: d = int24eq(&fc); break;
		case 61: d = int24ne(&fc); break;
		case 62: d = int24lt(&fc); break;
		case 63: d = int24le(&fc); break;
		case 64: d = int24gt(&fc); break;
		case 65: d = int24ge(&fc); break;
		case 66: d = int42eq(&fc); break;
		case 67: d = int42ne(&fc); break;
		case 68: d = int42lt(&fc); break;
		case 69: d = int42le(&fc); break;
		case 70: d = int42gt(&fc); break;
		case 71: d = int42ge(&fc); break;
		case 72:
			fc.i[3] = sub != 0;
			fc.i[4] = less != 0;
			d = in_range_int4_int4(&fc);
			break;
		case 73:
			fc.i[3] = sub != 0;
			fc.i[4] = less != 0;
			d = in_range_int4_int2(&fc);
			break;
		case 74:
			fc.i[3] = sub != 0;
			fc.i[4] = less != 0;
			d = in_range_int4_int8(&fc);
			break;
		case 75:
			fc.i[3] = sub != 0;
			fc.i[4] = less != 0;
			d = in_range_int2_int4(&fc);
			break;
		case 76:
			fc.i[3] = sub != 0;
			fc.i[4] = less != 0;
			d = in_range_int2_int2(&fc);
			break;
		case 77:
			fc.i[3] = sub != 0;
			fc.i[4] = less != 0;
			d = in_range_int2_int8(&fc);
			break;
		default: return -98;
	}
	*out = (int64_t) d;
	return pg_diff_int_errcode;
}
