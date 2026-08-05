/*
 * pg_contribb_io.c: vendored PostgreSQL C oracle for the contribb_diff
 * differential fuzz target (100%-coverage campaign, lane p1-mb-contribb).
 * Crates under test (see fuzz/core/src/contribb_diff.rs for the arm map):
 *   crates/contrib/seg, crates/contrib/cube.
 *
 * Provenance (all bodies VERBATIM sed-extracted from the vendor tree at
 * ~/dev/pgrust-fabled/vendor/postgres-src, Stamp-18.3, upstream sha
 * 62d6c7d3df6287f1bd83199c1a746e50d31571a0 — assembled by
 * scratchpad/assemble_contribb.sh, never hand-typed):
 *   - contrib/seg/seg.c lines 22-23 (SEG fmgr macros), 46-54 + 71-99 (the
 *     non-GiST PG_FUNCTION_INFO_V1 blocks + static decls), 102-186
 *     (seg_in/seg_out/seg_center/seg_lower/seg_upper), 538-1099 (everything
 *     from seg_contains to significant_digits: R-tree ops, seg_cmp family,
 *     restore, significant_digits). The GiST section (gseg_*, lines
 *     36-44 + 56-69 + 189-537) is the lane's excluded-state carve —
 *     crates/contrib/seg/src/gist.rs is the matching exception row.
 *   - contrib/cube/cube.c lines 25-29 (intarray macros), 31-49 + 64-93
 *     (non-GiST PG_FUNCTION_INFO_V1 blocks), 95-111 (internal decls; the
 *     g_cube_* decls stay — declarations only, definitions carved), 114-381
 *     (cube_in/cube_a_f8_f8/cube_a_f8/cube_subset/cube_out/cube_send/
 *     cube_recv), 749-1399 (cube_union_v0 .. distance_chebyshev),
 *     1505-1913 (distance_1D .. cube_c_f8_f8). GiST section (g_cube_*,
 *     lines 50-63 + 384-747 + 1402-1504) carved — cube/src/gist.rs is the
 *     exception row.
 *   - contrib/seg/segparse.y + segscan.l, contrib/cube/cubeparse.y +
 *     cubescan.l: compiled as their OWN translation units from the
 *     committed generated files csrc/contribb/{segparse,segscan,cubeparse,
 *     cubescan}.c (bison 2.3 / flex 2.6.4 over the verbatim grammars; see
 *     each file's provenance banner). They share this TU's arena palloc +
 *     unified error channel through csrc/contribb/include/postgres.h.
 *   - src/backend/utils/adt/arrayutils.c lines 56-61 + 66-102
 *     (ArrayGetNItems, ArrayGetNItemsSafe) [static-prefixed].
 *   - src/backend/utils/adt/arrayfuncs.c lines 3772-3807
 *     (array_contains_nulls) [static-prefixed].
 *   - src/include/lib/stringinfo.h lines 46-54 (StringInfoData) and
 *     231-234 (appendStringInfoCharMacro).
 *   - src/common/stringinfo.c: initStringInfoInternal (40-48, already
 *     static inline), initStringInfo (96-100), resetStringInfo (125-134),
 *     appendStringInfoString (229-233), appendStringInfoChar (241-252),
 *     appendBinaryStringInfo (280-298), enlargeStringInfo (336-400), each
 *     non-inline one [static-prefixed] via marker lines (the prefix is a
 *     marker line before the verbatim text, never an edit inside it).
 *   - src/include/libpq/pqformat.h lines 73-81 + 87-95 (pq_writeint32/64,
 *     already static inline) and 143-148 + 151-156 (pq_sendint32/64,
 *     already static inline).
 *   - src/backend/libpq/pqformat.c: pq_sendfloat8 (275-286), pq_begintypsend
 *     (325-334), pq_endtypsend (345-355), pq_getmsgint (414-442),
 *     pq_getmsgint64 (452-460), pq_getmsgfloat8 (487-498), pq_copymsgbytes
 *     (527-536) [static-prefixed].
 *   - float4in_internal / float8in_internal / float8out_internal: extern,
 *     resolved against the verbatim vendored copies in csrc/pg_float_io.c
 *     (one verbatim definition per symbol across the fuzz oracle build).
 *     float8out_internal is macro-routed through pg_cb_f8out (arena copy +
 *     free of the malloc'd original — memory plumbing only, the verbatim
 *     function still computes every byte).
 *
 * Shims (plumbing only, never logic) live in csrc/contribb/include/
 * (postgres.h, fmgr.h, varatt.h, nodes/miscnodes.h, utils/float.h,
 * utils/builtins.h) — see each header's comment. Key points:
 *   - soft-input-face error model, unified TLS channel with pg_float_io.c's
 *     pg_diff_errcode; errsave records + returns, ereport/elog(ERROR)
 *     record + longjmp to the armed driver entry.
 *   - palloc family -> per-exec bump arena (pg_cb_reset frees all), the
 *     per-query-context analogue; scanner yyalloc/yyrealloc/yyfree ride it.
 *   - compiled -funsigned-char + -ffp-contract=off (see build.rs comment):
 *     plain-char signedness pinned to the fleet Linux/aarch64 PG build the
 *     campaign ratified as oracle; contraction pinned off as everywhere.
 *   - array.h reduced to the fields/macros the vendored constructors reach
 *     (ArrayType header + ARR_* accessors, semantics identical).
 *   - pg_ntoh / pg_hton family -> __builtin_bswap (pg_bswap.h LE arm exactly).
 *
 * Driver entries (SECTION D, pg_cb_* prefix) are fuzz plumbing, NOT
 * Postgres code. Every entry arms the jmp_buf.
 */

#include "postgres.h"
#include "fmgr.h"
#include "varatt.h"
#include "nodes/miscnodes.h"
#include "utils/float.h"

#include <float.h>
#include <math.h>
#include <setjmp.h>

#include "segdata.h"
#include "cubedata.h"
#include "pg_oracle_guard.h"	/* oracle-serialization holder check */

/* ================= SHIM: unified error channel + arena ================= */

/* the float.c channel (defined in csrc/pg_float_io.c; codes 1/2) */
extern _Thread_local int pg_diff_errcode;

static _Thread_local int pg_cb_err;
static _Thread_local int pg_cb_pending;
static _Thread_local jmp_buf pg_cb_jmp;

int
pg_cb_geterr(void)
{
	if (pg_cb_err)
		return pg_cb_err;
	return pg_diff_errcode;
}

void
pg_cb_errstart(void)
{
	pg_cb_pending = PG_CB_ERR_INTERNAL;
}

int
pg_cb_errcode_set(int code)
{
	pg_cb_pending = code;
	return 0;
}

int
pg_cb_errnoop(const char *fmt,...)
{
	(void) fmt;
	return 0;
}

int
pg_cb_soft_occurred(void)
{
	return pg_cb_geterr() != 0;
}

void
pg_cb_soft_save(void)
{
	if (pg_cb_geterr() == 0)
		pg_cb_err = pg_cb_pending;
}

void
pg_cb_raise_hard(void)
{
	if (pg_cb_geterr() == 0)
		pg_cb_err = pg_cb_pending;
	longjmp(pg_cb_jmp, 1);
}

static void
pg_cb_errreset(void)
{
	pg_cb_err = 0;
	pg_cb_pending = 0;
	pg_diff_errcode = 0;
}

/* per-exec bump-tracked arena (each pointer individually malloc'd so
 * repalloc/pfree keep exact C semantics; all freed by pg_cb_reset) */
static _Thread_local void **pg_cb_ptrs;
static _Thread_local size_t pg_cb_nptrs;
static _Thread_local size_t pg_cb_capptrs;

static void
pg_cb_track(void *p)
{
	if (pg_cb_nptrs == pg_cb_capptrs)
	{
		pg_cb_capptrs = pg_cb_capptrs ? pg_cb_capptrs * 2 : 256;
		pg_cb_ptrs = (void **) realloc(pg_cb_ptrs, pg_cb_capptrs * sizeof(void *));
		if (!pg_cb_ptrs)
			abort();
	}
	pg_cb_ptrs[pg_cb_nptrs++] = p;
}

void *
(pg_cb_palloc) (Size size)
{
	void	   *p = malloc(size ? size : 1);

	if (!p)
		abort();
	pg_cb_track(p);
	return p;
}

void *
(pg_cb_palloc0) (Size size)
{
	void	   *p = calloc(1, size ? size : 1);

	if (!p)
		abort();
	pg_cb_track(p);
	return p;
}

void *
(pg_cb_repalloc) (void *ptr, Size size)
{
	size_t		i;
	void	   *p = realloc(ptr, size ? size : 1);

	if (!p)
		abort();
	for (i = pg_cb_nptrs; i-- > 0;)
	{
		if (pg_cb_ptrs[i] == ptr)
		{
			pg_cb_ptrs[i] = p;
			return p;
		}
	}
	pg_cb_track(p);
	return p;
}

void
(pg_cb_pfree) (void *ptr)
{
	size_t		i;

	for (i = pg_cb_nptrs; i-- > 0;)
	{
		if (pg_cb_ptrs[i] == ptr)
		{
			pg_cb_ptrs[i] = pg_cb_ptrs[--pg_cb_nptrs];
			free(ptr);
			return;
		}
	}
	abort();					/* pfree of a non-palloc'd pointer */
}

char *
(pg_cb_pstrdup) (const char *s)
{
	size_t		n = strlen(s) + 1;
	char	   *p = (char *) pg_cb_palloc(n);

	memcpy(p, s, n);
	return p;
}

void
pg_cb_reset(void)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	size_t		i;

	for (i = 0; i < pg_cb_nptrs; i++)
		free(pg_cb_ptrs[i]);
	pg_cb_nptrs = 0;
	pg_cb_errreset();
}

/* arena-copying wrapper over the verbatim float8out_internal (see header) */
char *
pg_cb_f8out(double num)
{
	/* parenthesized call defeats the function-like macro in utils/float.h */
	char	   *s = (float8out_internal) (num);
	char	   *p = pg_cb_pstrdup(s);

	free(s);					/* pg_float_io.c's palloc is malloc */
	return p;
}

/* ================= SHIM: %g PLATFORM PIN (see build.rs -O2 note) =========
 *
 * segparse.y's PLUMIN action derives significant digits from
 * snprintf("%g", boundary). Apple libc's %g KEEPS the trailing zeros when
 * the 6-digit rounding lands on an exact decimal tie (e.g. 6250005 ->
 * "6.25000e+06"); glibc — the fleet Linux build, the campaign's oracle of
 * record — strips them ("6.25e+06"), and seg's stored sigd differs with
 * it. Same class as the -funsigned-char pin: implementation-defined libc
 * surface pinned to the ratified platform. pg_cb_snprintf routes the "%g"
 * calls through a glibc-semantics %g (single %.5e rounding, C99 style
 * selection, trailing-zero strip); everything else passes through.
 * Witness input: seg_in("5(+-)6.25e6").
 */

static int
snprintf_g_copy(char *buf, size_t sz, const char *s)
{
	size_t		n = strlen(s);

	if (n >= sz)
		n = sz ? sz - 1 : 0;
	memcpy(buf, s, n);
	if (sz)
		buf[n] = '\0';
	return (int) strlen(s);
}

int
pg_cb_snprintf(char *buf, size_t sz, const char *fmt,...)
{
	va_list		ap;
	int			n;

	va_start(ap, fmt);
	if (strcmp(fmt, "%g") != 0)
	{
		n = vsnprintf(buf, sz, fmt, ap);
		va_end(ap);
		return n;
	}
	{
		double		v = va_arg(ap, double);
		char		tmp[64];
		char	   *p;
		int			x;

		va_end(ap);
		if (v != v)
			return snprintf_g_copy(buf, sz, "nan");
		if (v > 1.7976931348623157e308)
			return snprintf_g_copy(buf, sz, "inf");
		if (v < -1.7976931348623157e308)
			return snprintf_g_copy(buf, sz, "-inf");
		sprintf(tmp, "%.5e", v);
		p = strchr(tmp, 'e');
		x = atoi(p + 1);
		if (x < -4 || x >= 6)
		{
			char	   *q = p;	/* strip zeros in the e-mantissa */

			*p = '\0';
			while (q > tmp && *(q - 1) == '0')
				*(--q) = '\0';
			if (q > tmp && *(q - 1) == '.')
				*(--q) = '\0';
			sprintf(tmp + strlen(tmp), "e%+03d", x);
		}
		else
		{
			char	   *q;

			sprintf(tmp, "%.*f", 5 - x, v);
			if (strchr(tmp, '.'))
			{
				q = tmp + strlen(tmp);
				while (q > tmp && *(q - 1) == '0')
					*(--q) = '\0';
				if (q > tmp && *(q - 1) == '.')
					*(--q) = '\0';
			}
		}
		return snprintf_g_copy(buf, sz, tmp);
	}
}

/* ================= SHIM: pg_bswap.h LE arm ================= */

#if defined(__BYTE_ORDER__) && (__BYTE_ORDER__ != __ORDER_LITTLE_ENDIAN__)
#error "contribb oracle supports little-endian targets only"
#endif
#define pg_hton16(x) __builtin_bswap16(x)
#define pg_hton32(x) __builtin_bswap32(x)
#define pg_hton64(x) __builtin_bswap64(x)
#define pg_ntoh16(x) __builtin_bswap16(x)
#define pg_ntoh32(x) __builtin_bswap32(x)
#define pg_ntoh64(x) __builtin_bswap64(x)

/* ================= SHIM: utils/array.h reduction ================= */

typedef struct ArrayType
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	int			ndim;			/* # of dimensions */
	int32		dataoffset;		/* offset to data, or 0 if no bitmap */
	Oid			elemtype;		/* element type OID */
} ArrayType;

#define ARR_NDIM(a) ((a)->ndim)
#define ARR_HASNULL(a) ((a)->dataoffset != 0)
#define ARR_ELEMTYPE(a) ((a)->elemtype)
#define ARR_DIMS(a) ((int *) (((char *) (a)) + sizeof(ArrayType)))
#define ARR_LBOUND(a) ((int *) (((char *) (a)) + sizeof(ArrayType) + sizeof(int) * ARR_NDIM(a)))
#define ARR_NULLBITMAP(a) \
	(ARR_HASNULL(a) ? (bits8 *) (((char *) (a)) + sizeof(ArrayType) + 2 * sizeof(int) * ARR_NDIM(a)) : (bits8 *) NULL)
#define ARR_OVERHEAD_NONULLS(ndims) MAXALIGN(sizeof(ArrayType) + 2 * sizeof(int) * (ndims))
#define ARR_OVERHEAD_WITHNULLS(ndims, nitems) \
	MAXALIGN(sizeof(ArrayType) + 2 * sizeof(int) * (ndims) + ((nitems) + 7) / 8)
#define ARR_DATA_OFFSET(a) \
	(ARR_HASNULL(a) ? (a)->dataoffset : ARR_OVERHEAD_NONULLS(ARR_NDIM(a)))
#define ARR_DATA_PTR(a) (((char *) (a)) + ARR_DATA_OFFSET(a))

#define PG_GETARG_ARRAYTYPE_P(n) ((ArrayType *) PG_GETARG_POINTER(n))

#define MaxArraySize ((Size) (MaxAllocSize / sizeof(Datum)))

static int	ArrayGetNItemsSafe(int ndim, const int *dims, struct Node *escontext);

/* ---- VERBATIM src/backend/utils/adt/arrayutils.c lines 56-61
 * [static-prefixed] ---- */
static
int
ArrayGetNItems(int ndim, const int *dims)
{
	return ArrayGetNItemsSafe(ndim, dims, NULL);
}


/* ---- VERBATIM src/backend/utils/adt/arrayutils.c lines 66-102
 * [static-prefixed] ---- */
static
int
ArrayGetNItemsSafe(int ndim, const int *dims, struct Node *escontext)
{
	int32		ret;
	int			i;

	if (ndim <= 0)
		return 0;
	ret = 1;
	for (i = 0; i < ndim; i++)
	{
		int64		prod;

		/* A negative dimension implies that UB-LB overflowed ... */
		if (dims[i] < 0)
			ereturn(escontext, -1,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("array size exceeds the maximum allowed (%d)",
							(int) MaxArraySize)));

		prod = (int64) ret * (int64) dims[i];

		ret = (int32) prod;
		if ((int64) ret != prod)
			ereturn(escontext, -1,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("array size exceeds the maximum allowed (%d)",
							(int) MaxArraySize)));
	}
	Assert(ret >= 0);
	if ((Size) ret > MaxArraySize)
		ereturn(escontext, -1,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("array size exceeds the maximum allowed (%d)",
						(int) MaxArraySize)));
	return (int) ret;
}

/* ---- VERBATIM src/backend/utils/adt/arrayfuncs.c lines 3772-3807
 * [static-prefixed] ---- */
static
bool
array_contains_nulls(ArrayType *array)
{
	int			nelems;
	bits8	   *bitmap;
	int			bitmask;

	/* Easy answer if there's no null bitmap */
	if (!ARR_HASNULL(array))
		return false;

	nelems = ArrayGetNItems(ARR_NDIM(array), ARR_DIMS(array));

	bitmap = ARR_NULLBITMAP(array);

	/* check whole bytes of the bitmap byte-at-a-time */
	while (nelems >= 8)
	{
		if (*bitmap != 0xFF)
			return true;
		bitmap++;
		nelems -= 8;
	}

	/* check last partial byte */
	bitmask = 1;
	while (nelems > 0)
	{
		if ((*bitmap & bitmask) == 0)
			return true;
		bitmask <<= 1;
		nelems--;
	}

	return false;
}

/* ================= stringinfo (cube_out / cube_send / cube_recv) ========= */

/* ---- VERBATIM src/include/lib/stringinfo.h lines 46-54 ---- */
typedef struct StringInfoData
{
	char	   *data;
	int			len;
	int			maxlen;
	int			cursor;
} StringInfoData;

typedef StringInfoData *StringInfo;

/* ---- VERBATIM src/include/lib/stringinfo.h lines 231-234 ---- */
#define appendStringInfoCharMacro(str,ch) \
	(((str)->len + 1 >= (str)->maxlen) ? \
	 appendStringInfoChar(str, ch) : \
	 (void)((str)->data[(str)->len] = (ch), (str)->data[++(str)->len] = '\0'))

#define STRINGINFO_DEFAULT_SIZE 1024	/* stringinfo.h line 112, same value */

/* forward decls (stringinfo.h provided these in C) */
static void resetStringInfo(StringInfo str);
static void enlargeStringInfo(StringInfo str, int needed);
static void appendBinaryStringInfo(StringInfo str, const void *data, int datalen);
static void appendStringInfoChar(StringInfo str, char ch);

/* ---- VERBATIM src/common/stringinfo.c blocks (see header); non-inline
 * functions [static-prefixed] via the marker lines below ---- */
static inline void
initStringInfoInternal(StringInfo str, int initsize)
{
	Assert(initsize >= 1 && initsize <= MaxAllocSize);

	str->data = (char *) palloc(initsize);
	str->maxlen = initsize;
	resetStringInfo(str);
}

static
void
initStringInfo(StringInfo str)
{
	initStringInfoInternal(str, STRINGINFO_DEFAULT_SIZE);
}

static
void
resetStringInfo(StringInfo str)
{
	/* don't allow resets of read-only StringInfos */
	Assert(str->maxlen != 0);

	str->data[0] = '\0';
	str->len = 0;
	str->cursor = 0;
}

static
void
appendStringInfoString(StringInfo str, const char *s)
{
	appendBinaryStringInfo(str, s, strlen(s));
}

static
void
appendStringInfoChar(StringInfo str, char ch)
{
	/* Make more room if needed */
	if (str->len + 1 >= str->maxlen)
		enlargeStringInfo(str, 1);

	/* OK, append the character */
	str->data[str->len] = ch;
	str->len++;
	str->data[str->len] = '\0';
}

static
void
appendBinaryStringInfo(StringInfo str, const void *data, int datalen)
{
	Assert(str != NULL);

	/* Make more room if needed */
	enlargeStringInfo(str, datalen);

	/* OK, append the data */
	memcpy(str->data + str->len, data, datalen);
	str->len += datalen;

	/*
	 * Keep a trailing null in place, even though it's probably useless for
	 * binary data.  (Some callers are dealing with text but call this because
	 * their input isn't null-terminated.)
	 */
	str->data[str->len] = '\0';
}

static
void
enlargeStringInfo(StringInfo str, int needed)
{
	int			newlen;

	/* validate this is not a read-only StringInfo */
	Assert(str->maxlen != 0);

	/*
	 * Guard against out-of-range "needed" values.  Without this, we can get
	 * an overflow or infinite loop in the following.
	 */
	if (needed < 0)				/* should not happen */
	{
#ifndef FRONTEND
		elog(ERROR, "invalid string enlargement request size: %d", needed);
#else
		fprintf(stderr, "invalid string enlargement request size: %d\n", needed);
		exit(EXIT_FAILURE);
#endif
	}
	if (((Size) needed) >= (MaxAllocSize - (Size) str->len))
	{
#ifndef FRONTEND
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("string buffer exceeds maximum allowed length (%zu bytes)", MaxAllocSize),
				 errdetail("Cannot enlarge string buffer containing %d bytes by %d more bytes.",
						   str->len, needed)));
#else
		fprintf(stderr,
				_("string buffer exceeds maximum allowed length (%zu bytes)\n\nCannot enlarge string buffer containing %d bytes by %d more bytes.\n"),
				MaxAllocSize, str->len, needed);
		exit(EXIT_FAILURE);
#endif
	}

	needed += str->len + 1;		/* total space required now */

	/* Because of the above test, we now have needed <= MaxAllocSize */

	if (needed <= str->maxlen)
		return;					/* got enough space already */

	/*
	 * We don't want to allocate just a little more space with each append;
	 * for efficiency, double the buffer size each time it overflows.
	 * Actually, we might need to more than double it if 'needed' is big...
	 */
	newlen = 2 * str->maxlen;
	while (needed > newlen)
		newlen = 2 * newlen;

	/*
	 * Clamp to MaxAllocSize in case we went past it.  Note we are assuming
	 * here that MaxAllocSize <= INT_MAX/2, else the above loop could
	 * overflow.  We will still have newlen >= needed.
	 */
	if (newlen > (int) MaxAllocSize)
		newlen = (int) MaxAllocSize;

	str->data = (char *) repalloc(str->data, newlen);

	str->maxlen = newlen;
}

/* ================= pqformat (cube_send / cube_recv) ================= */

/* ---- VERBATIM src/include/libpq/pqformat.h lines 74-81, 88-95 (static
 * inline in the original too) ---- */
static inline void
pq_writeint32(StringInfoData *pg_restrict buf, uint32 i)
{
	uint32		ni = pg_hton32(i);

	Assert(buf->len + (int) sizeof(uint32) <= buf->maxlen);
	memcpy((char *pg_restrict) (buf->data + buf->len), &ni, sizeof(uint32));
	buf->len += sizeof(uint32);
}

static inline void
pq_writeint64(StringInfoData *pg_restrict buf, uint64 i)
{
	uint64		ni = pg_hton64(i);

	Assert(buf->len + (int) sizeof(uint64) <= buf->maxlen);
	memcpy((char *pg_restrict) (buf->data + buf->len), &ni, sizeof(uint64));
	buf->len += sizeof(uint64);
}

/* ---- VERBATIM src/include/libpq/pqformat.h lines 143-148, 151-156 (static
 * inline in the original too) ---- */
static inline void
pq_sendint32(StringInfo buf, uint32 i)
{
	enlargeStringInfo(buf, sizeof(uint32));
	pq_writeint32(buf, i);
}

static inline void
pq_sendint64(StringInfo buf, uint64 i)
{
	enlargeStringInfo(buf, sizeof(uint64));
	pq_writeint64(buf, i);
}

/* ---- VERBATIM src/backend/libpq/pqformat.c blocks (see header),
 * [static-prefixed] via marker lines ---- */
static void pq_copymsgbytes(StringInfo msg, void *buf, int datalen);

static
void
pq_sendfloat8(StringInfo buf, float8 f)
{
	union
	{
		float8		f;
		int64		i;
	}			swap;

	swap.f = f;
	pq_sendint64(buf, swap.i);
}

static
void
pq_begintypsend(StringInfo buf)
{
	initStringInfo(buf);
	/* Reserve four bytes for the bytea length word */
	appendStringInfoCharMacro(buf, '\0');
	appendStringInfoCharMacro(buf, '\0');
	appendStringInfoCharMacro(buf, '\0');
	appendStringInfoCharMacro(buf, '\0');
}

static
bytea *
pq_endtypsend(StringInfo buf)
{
	bytea	   *result = (bytea *) buf->data;

	/* Insert correct length into bytea length word */
	Assert(buf->len >= VARHDRSZ);
	SET_VARSIZE(result, buf->len);

	return result;
}

static
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

static
int64
pq_getmsgint64(StringInfo msg)
{
	uint64		n64;

	pq_copymsgbytes(msg, &n64, sizeof(n64));

	return pg_ntoh64(n64);
}

static
float8
pq_getmsgfloat8(StringInfo msg)
{
	union
	{
		float8		f;
		int64		i;
	}			swap;

	swap.i = pq_getmsgint64(msg);
	return swap.f;
}

static
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

/* ================= SECTION 1: contrib/seg (non-GiST) ================= */

/* ---- VERBATIM contrib/seg/seg.c lines 22-23 ---- */
#define DatumGetSegP(X) ((SEG *) DatumGetPointer(X))
#define PG_GETARG_SEG_P(n) DatumGetSegP(PG_GETARG_DATUM(n))

/* ---- VERBATIM contrib/seg/seg.c lines 46-54, 71-99 (non-GiST V1 blocks +
 * static decls) ---- */
/*
** Input/Output routines
*/
PG_FUNCTION_INFO_V1(seg_in);
PG_FUNCTION_INFO_V1(seg_out);
PG_FUNCTION_INFO_V1(seg_size);
PG_FUNCTION_INFO_V1(seg_lower);
PG_FUNCTION_INFO_V1(seg_upper);
PG_FUNCTION_INFO_V1(seg_center);
/*
** R-tree support functions
*/
PG_FUNCTION_INFO_V1(seg_same);
PG_FUNCTION_INFO_V1(seg_contains);
PG_FUNCTION_INFO_V1(seg_contained);
PG_FUNCTION_INFO_V1(seg_overlap);
PG_FUNCTION_INFO_V1(seg_left);
PG_FUNCTION_INFO_V1(seg_over_left);
PG_FUNCTION_INFO_V1(seg_right);
PG_FUNCTION_INFO_V1(seg_over_right);
PG_FUNCTION_INFO_V1(seg_union);
PG_FUNCTION_INFO_V1(seg_inter);
static void rt_seg_size(SEG *a, float *size);

/*
** Various operators
*/
PG_FUNCTION_INFO_V1(seg_cmp);
PG_FUNCTION_INFO_V1(seg_lt);
PG_FUNCTION_INFO_V1(seg_le);
PG_FUNCTION_INFO_V1(seg_gt);
PG_FUNCTION_INFO_V1(seg_ge);
PG_FUNCTION_INFO_V1(seg_different);

/*
** Auxiliary functions
*/
static int	restore(char *result, float val, int n);

/* ---- VERBATIM contrib/seg/seg.c lines 102-186 ---- */
/*****************************************************************************
 * Input/Output functions
 *****************************************************************************/

Datum
seg_in(PG_FUNCTION_ARGS)
{
	char	   *str = PG_GETARG_CSTRING(0);
	SEG		   *result = palloc(sizeof(SEG));
	yyscan_t	scanner;

	seg_scanner_init(str, &scanner);

	if (seg_yyparse(result, fcinfo->context, scanner) != 0)
		seg_yyerror(result, fcinfo->context, scanner, "bogus input");

	seg_scanner_finish(scanner);

	PG_RETURN_POINTER(result);
}

Datum
seg_out(PG_FUNCTION_ARGS)
{
	SEG		   *seg = PG_GETARG_SEG_P(0);
	char	   *result;
	char	   *p;

	p = result = (char *) palloc(40);

	if (seg->l_ext == '>' || seg->l_ext == '<' || seg->l_ext == '~')
		p += sprintf(p, "%c", seg->l_ext);

	if (seg->lower == seg->upper && seg->l_ext == seg->u_ext)
	{
		/*
		 * indicates that this interval was built by seg_in off a single point
		 */
		p += restore(p, seg->lower, seg->l_sigd);
	}
	else
	{
		if (seg->l_ext != '-')
		{
			/* print the lower boundary if exists */
			p += restore(p, seg->lower, seg->l_sigd);
			p += sprintf(p, " ");
		}
		p += sprintf(p, "..");
		if (seg->u_ext != '-')
		{
			/* print the upper boundary if exists */
			p += sprintf(p, " ");
			if (seg->u_ext == '>' || seg->u_ext == '<' || seg->l_ext == '~')
				p += sprintf(p, "%c", seg->u_ext);
			p += restore(p, seg->upper, seg->u_sigd);
		}
	}

	PG_RETURN_CSTRING(result);
}

Datum
seg_center(PG_FUNCTION_ARGS)
{
	SEG		   *seg = PG_GETARG_SEG_P(0);

	PG_RETURN_FLOAT4(((float) seg->lower + (float) seg->upper) / 2.0);
}

Datum
seg_lower(PG_FUNCTION_ARGS)
{
	SEG		   *seg = PG_GETARG_SEG_P(0);

	PG_RETURN_FLOAT4(seg->lower);
}

Datum
seg_upper(PG_FUNCTION_ARGS)
{
	SEG		   *seg = PG_GETARG_SEG_P(0);

	PG_RETURN_FLOAT4(seg->upper);
}

/* ---- VERBATIM contrib/seg/seg.c lines 538-1099 ---- */


Datum
seg_contains(PG_FUNCTION_ARGS)
{
	SEG		   *a = PG_GETARG_SEG_P(0);
	SEG		   *b = PG_GETARG_SEG_P(1);

	PG_RETURN_BOOL((a->lower <= b->lower) && (a->upper >= b->upper));
}

Datum
seg_contained(PG_FUNCTION_ARGS)
{
	Datum		a = PG_GETARG_DATUM(0);
	Datum		b = PG_GETARG_DATUM(1);

	PG_RETURN_DATUM(DirectFunctionCall2(seg_contains, b, a));
}

/*****************************************************************************
 * Operator class for R-tree indexing
 *****************************************************************************/

Datum
seg_same(PG_FUNCTION_ARGS)
{
	int			cmp = DatumGetInt32(DirectFunctionCall2(seg_cmp,
														PG_GETARG_DATUM(0),
														PG_GETARG_DATUM(1)));

	PG_RETURN_BOOL(cmp == 0);
}

/*	seg_overlap -- does a overlap b?
 */
Datum
seg_overlap(PG_FUNCTION_ARGS)
{
	SEG		   *a = PG_GETARG_SEG_P(0);
	SEG		   *b = PG_GETARG_SEG_P(1);

	PG_RETURN_BOOL(((a->upper >= b->upper) && (a->lower <= b->upper)) ||
				   ((b->upper >= a->upper) && (b->lower <= a->upper)));
}

/*	seg_over_left -- is the right edge of (a) located at or left of the right edge of (b)?
 */
Datum
seg_over_left(PG_FUNCTION_ARGS)
{
	SEG		   *a = PG_GETARG_SEG_P(0);
	SEG		   *b = PG_GETARG_SEG_P(1);

	PG_RETURN_BOOL(a->upper <= b->upper);
}

/*	seg_left -- is (a) entirely on the left of (b)?
 */
Datum
seg_left(PG_FUNCTION_ARGS)
{
	SEG		   *a = PG_GETARG_SEG_P(0);
	SEG		   *b = PG_GETARG_SEG_P(1);

	PG_RETURN_BOOL(a->upper < b->lower);
}

/*	seg_right -- is (a) entirely on the right of (b)?
 */
Datum
seg_right(PG_FUNCTION_ARGS)
{
	SEG		   *a = PG_GETARG_SEG_P(0);
	SEG		   *b = PG_GETARG_SEG_P(1);

	PG_RETURN_BOOL(a->lower > b->upper);
}

/*	seg_over_right -- is the left edge of (a) located at or right of the left edge of (b)?
 */
Datum
seg_over_right(PG_FUNCTION_ARGS)
{
	SEG		   *a = PG_GETARG_SEG_P(0);
	SEG		   *b = PG_GETARG_SEG_P(1);

	PG_RETURN_BOOL(a->lower >= b->lower);
}

Datum
seg_union(PG_FUNCTION_ARGS)
{
	SEG		   *a = PG_GETARG_SEG_P(0);
	SEG		   *b = PG_GETARG_SEG_P(1);
	SEG		   *n;

	n = (SEG *) palloc(sizeof(*n));

	/* take max of upper endpoints */
	if (a->upper > b->upper)
	{
		n->upper = a->upper;
		n->u_sigd = a->u_sigd;
		n->u_ext = a->u_ext;
	}
	else
	{
		n->upper = b->upper;
		n->u_sigd = b->u_sigd;
		n->u_ext = b->u_ext;
	}

	/* take min of lower endpoints */
	if (a->lower < b->lower)
	{
		n->lower = a->lower;
		n->l_sigd = a->l_sigd;
		n->l_ext = a->l_ext;
	}
	else
	{
		n->lower = b->lower;
		n->l_sigd = b->l_sigd;
		n->l_ext = b->l_ext;
	}

	PG_RETURN_POINTER(n);
}

Datum
seg_inter(PG_FUNCTION_ARGS)
{
	SEG		   *a = PG_GETARG_SEG_P(0);
	SEG		   *b = PG_GETARG_SEG_P(1);
	SEG		   *n;

	n = (SEG *) palloc(sizeof(*n));

	/* take min of upper endpoints */
	if (a->upper < b->upper)
	{
		n->upper = a->upper;
		n->u_sigd = a->u_sigd;
		n->u_ext = a->u_ext;
	}
	else
	{
		n->upper = b->upper;
		n->u_sigd = b->u_sigd;
		n->u_ext = b->u_ext;
	}

	/* take max of lower endpoints */
	if (a->lower > b->lower)
	{
		n->lower = a->lower;
		n->l_sigd = a->l_sigd;
		n->l_ext = a->l_ext;
	}
	else
	{
		n->lower = b->lower;
		n->l_sigd = b->l_sigd;
		n->l_ext = b->l_ext;
	}

	PG_RETURN_POINTER(n);
}

static void
rt_seg_size(SEG *a, float *size)
{
	if (a == (SEG *) NULL || a->upper <= a->lower)
		*size = 0.0;
	else
		*size = fabsf(a->upper - a->lower);
}

Datum
seg_size(PG_FUNCTION_ARGS)
{
	SEG		   *seg = PG_GETARG_SEG_P(0);

	PG_RETURN_FLOAT4(fabsf(seg->upper - seg->lower));
}


/*****************************************************************************
 *				   Miscellaneous operators
 *****************************************************************************/
Datum
seg_cmp(PG_FUNCTION_ARGS)
{
	SEG		   *a = PG_GETARG_SEG_P(0);
	SEG		   *b = PG_GETARG_SEG_P(1);

	/*
	 * First compare on lower boundary position
	 */
	if (a->lower < b->lower)
		PG_RETURN_INT32(-1);
	if (a->lower > b->lower)
		PG_RETURN_INT32(1);

	/*
	 * a->lower == b->lower, so consider type of boundary.
	 *
	 * A '-' lower bound is < any other kind (this could only be relevant if
	 * -HUGE_VAL is used as a regular data value). A '<' lower bound is < any
	 * other kind except '-'. A '>' lower bound is > any other kind.
	 */
	if (a->l_ext != b->l_ext)
	{
		if (a->l_ext == '-')
			PG_RETURN_INT32(-1);
		if (b->l_ext == '-')
			PG_RETURN_INT32(1);
		if (a->l_ext == '<')
			PG_RETURN_INT32(-1);
		if (b->l_ext == '<')
			PG_RETURN_INT32(1);
		if (a->l_ext == '>')
			PG_RETURN_INT32(1);
		if (b->l_ext == '>')
			PG_RETURN_INT32(-1);
	}

	/*
	 * For other boundary types, consider # of significant digits first.
	 */
	if (a->l_sigd < b->l_sigd)	/* (a) is blurred and is likely to include (b) */
		PG_RETURN_INT32(-1);
	if (a->l_sigd > b->l_sigd)	/* (a) is less blurred and is likely to be
								 * included in (b) */
		PG_RETURN_INT32(1);

	/*
	 * For same # of digits, an approximate boundary is more blurred than
	 * exact.
	 */
	if (a->l_ext != b->l_ext)
	{
		if (a->l_ext == '~')	/* (a) is approximate, while (b) is exact */
			PG_RETURN_INT32(-1);
		if (b->l_ext == '~')
			PG_RETURN_INT32(1);
		/* can't get here unless data is corrupt */
		elog(ERROR, "bogus lower boundary types %d %d",
			 (int) a->l_ext, (int) b->l_ext);
	}

	/* at this point, the lower boundaries are identical */

	/*
	 * First compare on upper boundary position
	 */
	if (a->upper < b->upper)
		PG_RETURN_INT32(-1);
	if (a->upper > b->upper)
		PG_RETURN_INT32(1);

	/*
	 * a->upper == b->upper, so consider type of boundary.
	 *
	 * A '-' upper bound is > any other kind (this could only be relevant if
	 * HUGE_VAL is used as a regular data value). A '<' upper bound is < any
	 * other kind. A '>' upper bound is > any other kind except '-'.
	 */
	if (a->u_ext != b->u_ext)
	{
		if (a->u_ext == '-')
			PG_RETURN_INT32(1);
		if (b->u_ext == '-')
			PG_RETURN_INT32(-1);
		if (a->u_ext == '<')
			PG_RETURN_INT32(-1);
		if (b->u_ext == '<')
			PG_RETURN_INT32(1);
		if (a->u_ext == '>')
			PG_RETURN_INT32(1);
		if (b->u_ext == '>')
			PG_RETURN_INT32(-1);
	}

	/*
	 * For other boundary types, consider # of significant digits first. Note
	 * result here is converse of the lower-boundary case.
	 */
	if (a->u_sigd < b->u_sigd)	/* (a) is blurred and is likely to include (b) */
		PG_RETURN_INT32(1);
	if (a->u_sigd > b->u_sigd)	/* (a) is less blurred and is likely to be
								 * included in (b) */
		PG_RETURN_INT32(-1);

	/*
	 * For same # of digits, an approximate boundary is more blurred than
	 * exact.  Again, result is converse of lower-boundary case.
	 */
	if (a->u_ext != b->u_ext)
	{
		if (a->u_ext == '~')	/* (a) is approximate, while (b) is exact */
			PG_RETURN_INT32(1);
		if (b->u_ext == '~')
			PG_RETURN_INT32(-1);
		/* can't get here unless data is corrupt */
		elog(ERROR, "bogus upper boundary types %d %d",
			 (int) a->u_ext, (int) b->u_ext);
	}

	PG_RETURN_INT32(0);
}

Datum
seg_lt(PG_FUNCTION_ARGS)
{
	int			cmp = DatumGetInt32(DirectFunctionCall2(seg_cmp,
														PG_GETARG_DATUM(0),
														PG_GETARG_DATUM(1)));

	PG_RETURN_BOOL(cmp < 0);
}

Datum
seg_le(PG_FUNCTION_ARGS)
{
	int			cmp = DatumGetInt32(DirectFunctionCall2(seg_cmp,
														PG_GETARG_DATUM(0),
														PG_GETARG_DATUM(1)));

	PG_RETURN_BOOL(cmp <= 0);
}

Datum
seg_gt(PG_FUNCTION_ARGS)
{
	int			cmp = DatumGetInt32(DirectFunctionCall2(seg_cmp,
														PG_GETARG_DATUM(0),
														PG_GETARG_DATUM(1)));

	PG_RETURN_BOOL(cmp > 0);
}

Datum
seg_ge(PG_FUNCTION_ARGS)
{
	int			cmp = DatumGetInt32(DirectFunctionCall2(seg_cmp,
														PG_GETARG_DATUM(0),
														PG_GETARG_DATUM(1)));

	PG_RETURN_BOOL(cmp >= 0);
}


Datum
seg_different(PG_FUNCTION_ARGS)
{
	int			cmp = DatumGetInt32(DirectFunctionCall2(seg_cmp,
														PG_GETARG_DATUM(0),
														PG_GETARG_DATUM(1)));

	PG_RETURN_BOOL(cmp != 0);
}



/*****************************************************************************
 *				   Auxiliary functions
 *****************************************************************************/

/*
 * The purpose of this routine is to print the given floating point
 * value with exactly n significant digits.  Its behaviour
 * is similar to %.ng except it prints 8.00 where %.ng would
 * print 8.  Returns the length of the string written at "result".
 *
 * Caller must provide a sufficiently large result buffer; 16 bytes
 * should be enough for all known float implementations.
 */
static int
restore(char *result, float val, int n)
{
	char		buf[25] = {
		'0', '0', '0', '0', '0',
		'0', '0', '0', '0', '0',
		'0', '0', '0', '0', '0',
		'0', '0', '0', '0', '0',
		'0', '0', '0', '0', '\0'
	};
	char	   *p;
	int			exp;
	int			i,
				dp,
				sign;

	/*
	 * Put a cap on the number of significant digits to avoid garbage in the
	 * output and ensure we don't overrun the result buffer.  (n should not be
	 * negative, but check to protect ourselves against corrupted data.)
	 */
	if (n <= 0)
		n = FLT_DIG;
	else
		n = Min(n, FLT_DIG);

	/* remember the sign */
	sign = (val < 0 ? 1 : 0);

	/* print, in %e style to start with */
	sprintf(result, "%.*e", n - 1, val);

	/* find the exponent */
	p = strchr(result, 'e');

	/* punt if we have 'inf' or similar */
	if (p == NULL)
		return strlen(result);

	exp = atoi(p + 1);
	if (exp == 0)
	{
		/* just truncate off the 'e+00' */
		*p = '\0';
	}
	else
	{
		if (abs(exp) <= 4)
		{
			/*
			 * remove the decimal point from the mantissa and write the digits
			 * to the buf array
			 */
			for (p = result + sign, i = 10, dp = 0; *p != 'e'; p++, i++)
			{
				buf[i] = *p;
				if (*p == '.')
				{
					dp = i--;	/* skip the decimal point */
				}
			}
			if (dp == 0)
				dp = i--;		/* no decimal point was found in the above
								 * for() loop */

			if (exp > 0)
			{
				if (dp - 10 + exp >= n)
				{
					/*
					 * the decimal point is behind the last significant digit;
					 * the digits in between must be converted to the exponent
					 * and the decimal point placed after the first digit
					 */
					exp = dp - 10 + exp - n;
					buf[10 + n] = '\0';

					/* insert the decimal point */
					if (n > 1)
					{
						dp = 11;
						for (i = 23; i > dp; i--)
							buf[i] = buf[i - 1];
						buf[dp] = '.';
					}

					/*
					 * adjust the exponent by the number of digits after the
					 * decimal point
					 */
					if (n > 1)
						sprintf(&buf[11 + n], "e%d", exp + n - 1);
					else
						sprintf(&buf[11], "e%d", exp + n - 1);

					if (sign)
					{
						buf[9] = '-';
						strcpy(result, &buf[9]);
					}
					else
						strcpy(result, &buf[10]);
				}
				else
				{				/* insert the decimal point */
					dp += exp;
					for (i = 23; i > dp; i--)
						buf[i] = buf[i - 1];
					buf[11 + n] = '\0';
					buf[dp] = '.';
					if (sign)
					{
						buf[9] = '-';
						strcpy(result, &buf[9]);
					}
					else
						strcpy(result, &buf[10]);
				}
			}
			else
			{					/* exp <= 0 */
				dp += exp - 1;
				buf[10 + n] = '\0';
				buf[dp] = '.';
				if (sign)
				{
					buf[dp - 2] = '-';
					strcpy(result, &buf[dp - 2]);
				}
				else
					strcpy(result, &buf[dp - 1]);
			}
		}

		/* do nothing for abs(exp) > 4; %e must be OK */
		/* just get rid of zeroes after [eE]- and +zeroes after [Ee]. */

		/* ... this is not done yet. */
	}
	return strlen(result);
}


/*
** Miscellany
*/

/* find out the number of significant digits in a string representing
 * a floating point number
 */
int
significant_digits(const char *s)
{
	const char *p = s;
	int			n,
				c,
				zeroes;

	zeroes = 1;
	/* skip leading zeroes and sign */
	for (c = *p; (c == '0' || c == '+' || c == '-') && c != 0; c = *(++p));

	/* skip decimal point and following zeroes */
	for (c = *p; (c == '0' || c == '.') && c != 0; c = *(++p))
	{
		if (c != '.')
			zeroes++;
	}

	/* count significant digits (n) */
	for (c = *p, n = 0; c != 0; c = *(++p))
	{
		if (!((c >= '0' && c <= '9') || (c == '.')))
			break;
		if (c != '.')
			n++;
	}

	if (!n)
		return zeroes;

	return n;
}

/* ================= SECTION 2: contrib/cube (non-GiST) ================= */

/* ---- VERBATIM contrib/cube/cube.c lines 25-29 ---- */
/*
 * Taken from the intarray contrib header
 */
#define ARRPTR(x)  ( (double *) ARR_DATA_PTR(x) )
#define ARRNELEMS(x)  ArrayGetNItems( ARR_NDIM(x), ARR_DIMS(x))

/* ---- VERBATIM contrib/cube/cube.c lines 31-49, 64-93 (non-GiST V1
 * blocks), 95-111 (internal decls; g_cube_* decls unused, definitions
 * carved) ---- */
/*
** Input/Output routines
*/
PG_FUNCTION_INFO_V1(cube_in);
PG_FUNCTION_INFO_V1(cube_a_f8_f8);
PG_FUNCTION_INFO_V1(cube_a_f8);
PG_FUNCTION_INFO_V1(cube_out);
PG_FUNCTION_INFO_V1(cube_send);
PG_FUNCTION_INFO_V1(cube_recv);
PG_FUNCTION_INFO_V1(cube_f8);
PG_FUNCTION_INFO_V1(cube_f8_f8);
PG_FUNCTION_INFO_V1(cube_c_f8);
PG_FUNCTION_INFO_V1(cube_c_f8_f8);
PG_FUNCTION_INFO_V1(cube_dim);
PG_FUNCTION_INFO_V1(cube_ll_coord);
PG_FUNCTION_INFO_V1(cube_ur_coord);
PG_FUNCTION_INFO_V1(cube_coord);
PG_FUNCTION_INFO_V1(cube_coord_llur);
PG_FUNCTION_INFO_V1(cube_subset);
/*
** B-tree support functions
*/
PG_FUNCTION_INFO_V1(cube_eq);
PG_FUNCTION_INFO_V1(cube_ne);
PG_FUNCTION_INFO_V1(cube_lt);
PG_FUNCTION_INFO_V1(cube_gt);
PG_FUNCTION_INFO_V1(cube_le);
PG_FUNCTION_INFO_V1(cube_ge);
PG_FUNCTION_INFO_V1(cube_cmp);

/*
** R-tree support functions
*/

PG_FUNCTION_INFO_V1(cube_contains);
PG_FUNCTION_INFO_V1(cube_contained);
PG_FUNCTION_INFO_V1(cube_overlap);
PG_FUNCTION_INFO_V1(cube_union);
PG_FUNCTION_INFO_V1(cube_inter);
PG_FUNCTION_INFO_V1(cube_size);

/*
** miscellaneous
*/
PG_FUNCTION_INFO_V1(distance_taxicab);
PG_FUNCTION_INFO_V1(cube_distance);
PG_FUNCTION_INFO_V1(distance_chebyshev);
PG_FUNCTION_INFO_V1(cube_is_point);
PG_FUNCTION_INFO_V1(cube_enlarge);
/*
** For internal use only
*/
int32		cube_cmp_v0(NDBOX *a, NDBOX *b);
bool		cube_contains_v0(NDBOX *a, NDBOX *b);
bool		cube_overlap_v0(NDBOX *a, NDBOX *b);
NDBOX	   *cube_union_v0(NDBOX *a, NDBOX *b);
void		rt_cube_size(NDBOX *a, double *size);
NDBOX	   *g_cube_binary_union(NDBOX *r1, NDBOX *r2, int *sizep);
bool		g_cube_leaf_consistent(NDBOX *key, NDBOX *query, StrategyNumber strategy);
bool		g_cube_internal_consistent(NDBOX *key, NDBOX *query, StrategyNumber strategy);

/*
** Auxiliary functions
*/
static double distance_1D(double a1, double a2, double b1, double b2);
static bool cube_is_point_internal(NDBOX *cube);

/* ---- VERBATIM contrib/cube/cube.c lines 114-381 ---- */
/*****************************************************************************
 * Input/Output functions
 *****************************************************************************/

/* NdBox = [(lowerleft),(upperright)] */
/* [(xLL(1)...xLL(N)),(xUR(1)...xUR(n))] */
Datum
cube_in(PG_FUNCTION_ARGS)
{
	char	   *str = PG_GETARG_CSTRING(0);
	NDBOX	   *result;
	Size		scanbuflen;
	yyscan_t	scanner;

	cube_scanner_init(str, &scanbuflen, &scanner);

	cube_yyparse(&result, scanbuflen, fcinfo->context, scanner);

	/* We might as well run this even on failure. */
	cube_scanner_finish(scanner);

	PG_RETURN_NDBOX_P(result);
}


/*
** Allows the construction of a cube from 2 float[]'s
*/
Datum
cube_a_f8_f8(PG_FUNCTION_ARGS)
{
	ArrayType  *ur = PG_GETARG_ARRAYTYPE_P(0);
	ArrayType  *ll = PG_GETARG_ARRAYTYPE_P(1);
	NDBOX	   *result;
	int			i;
	int			dim;
	int			size;
	bool		point;
	double	   *dur,
			   *dll;

	if (array_contains_nulls(ur) || array_contains_nulls(ll))
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_ELEMENT_ERROR),
				 errmsg("cannot work with arrays containing NULLs")));

	dim = ARRNELEMS(ur);
	if (dim > CUBE_MAX_DIM)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("can't extend cube"),
				 errdetail("A cube cannot have more than %d dimensions.",
						   CUBE_MAX_DIM)));

	if (ARRNELEMS(ll) != dim)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_ELEMENT_ERROR),
				 errmsg("UR and LL arrays must be of same length")));

	dur = ARRPTR(ur);
	dll = ARRPTR(ll);

	/* Check if it's a point */
	point = true;
	for (i = 0; i < dim; i++)
	{
		if (dur[i] != dll[i])
		{
			point = false;
			break;
		}
	}

	size = point ? POINT_SIZE(dim) : CUBE_SIZE(dim);
	result = (NDBOX *) palloc0(size);
	SET_VARSIZE(result, size);
	SET_DIM(result, dim);

	for (i = 0; i < dim; i++)
		result->x[i] = dur[i];

	if (!point)
	{
		for (i = 0; i < dim; i++)
			result->x[i + dim] = dll[i];
	}
	else
		SET_POINT_BIT(result);

	PG_RETURN_NDBOX_P(result);
}

/*
** Allows the construction of a zero-volume cube from a float[]
*/
Datum
cube_a_f8(PG_FUNCTION_ARGS)
{
	ArrayType  *ur = PG_GETARG_ARRAYTYPE_P(0);
	NDBOX	   *result;
	int			i;
	int			dim;
	int			size;
	double	   *dur;

	if (array_contains_nulls(ur))
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_ELEMENT_ERROR),
				 errmsg("cannot work with arrays containing NULLs")));

	dim = ARRNELEMS(ur);
	if (dim > CUBE_MAX_DIM)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("array is too long"),
				 errdetail("A cube cannot have more than %d dimensions.",
						   CUBE_MAX_DIM)));

	dur = ARRPTR(ur);

	size = POINT_SIZE(dim);
	result = (NDBOX *) palloc0(size);
	SET_VARSIZE(result, size);
	SET_DIM(result, dim);
	SET_POINT_BIT(result);

	for (i = 0; i < dim; i++)
		result->x[i] = dur[i];

	PG_RETURN_NDBOX_P(result);
}

Datum
cube_subset(PG_FUNCTION_ARGS)
{
	NDBOX	   *c = PG_GETARG_NDBOX_P(0);
	ArrayType  *idx = PG_GETARG_ARRAYTYPE_P(1);
	NDBOX	   *result;
	int			size,
				dim,
				i;
	int		   *dx;

	if (array_contains_nulls(idx))
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_ELEMENT_ERROR),
				 errmsg("cannot work with arrays containing NULLs")));

	dx = (int32 *) ARR_DATA_PTR(idx);

	dim = ARRNELEMS(idx);
	if (dim > CUBE_MAX_DIM)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("array is too long"),
				 errdetail("A cube cannot have more than %d dimensions.",
						   CUBE_MAX_DIM)));

	size = IS_POINT(c) ? POINT_SIZE(dim) : CUBE_SIZE(dim);
	result = (NDBOX *) palloc0(size);
	SET_VARSIZE(result, size);
	SET_DIM(result, dim);

	if (IS_POINT(c))
		SET_POINT_BIT(result);

	for (i = 0; i < dim; i++)
	{
		if ((dx[i] <= 0) || (dx[i] > DIM(c)))
			ereport(ERROR,
					(errcode(ERRCODE_ARRAY_ELEMENT_ERROR),
					 errmsg("Index out of bounds")));
		result->x[i] = c->x[dx[i] - 1];
		if (!IS_POINT(c))
			result->x[i + dim] = c->x[dx[i] + DIM(c) - 1];
	}

	PG_FREE_IF_COPY(c, 0);
	PG_RETURN_NDBOX_P(result);
}

Datum
cube_out(PG_FUNCTION_ARGS)
{
	NDBOX	   *cube = PG_GETARG_NDBOX_P(0);
	StringInfoData buf;
	int			dim = DIM(cube);
	int			i;

	initStringInfo(&buf);

	appendStringInfoChar(&buf, '(');
	for (i = 0; i < dim; i++)
	{
		if (i > 0)
			appendStringInfoString(&buf, ", ");
		appendStringInfoString(&buf, float8out_internal(LL_COORD(cube, i)));
	}
	appendStringInfoChar(&buf, ')');

	if (!cube_is_point_internal(cube))
	{
		appendStringInfoString(&buf, ",(");
		for (i = 0; i < dim; i++)
		{
			if (i > 0)
				appendStringInfoString(&buf, ", ");
			appendStringInfoString(&buf, float8out_internal(UR_COORD(cube, i)));
		}
		appendStringInfoChar(&buf, ')');
	}

	PG_FREE_IF_COPY(cube, 0);
	PG_RETURN_CSTRING(buf.data);
}

/*
 * cube_send - a binary output handler for cube type
 */
Datum
cube_send(PG_FUNCTION_ARGS)
{
	NDBOX	   *cube = PG_GETARG_NDBOX_P(0);
	StringInfoData buf;
	int32		i,
				nitems = DIM(cube);

	pq_begintypsend(&buf);
	pq_sendint32(&buf, cube->header);
	if (!IS_POINT(cube))
		nitems += nitems;
	/* for symmetry with cube_recv, we don't use LL_COORD/UR_COORD here */
	for (i = 0; i < nitems; i++)
		pq_sendfloat8(&buf, cube->x[i]);

	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

/*
 * cube_recv - a binary input handler for cube type
 */
Datum
cube_recv(PG_FUNCTION_ARGS)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);
	int32		header;
	int32		i,
				nitems;
	NDBOX	   *cube;

	header = pq_getmsgint(buf, sizeof(int32));
	nitems = (header & DIM_MASK);
	if (nitems > CUBE_MAX_DIM)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("cube dimension is too large"),
				 errdetail("A cube cannot have more than %d dimensions.",
						   CUBE_MAX_DIM)));
	if ((header & POINT_BIT) == 0)
		nitems += nitems;
	cube = palloc(offsetof(NDBOX, x) + sizeof(double) * nitems);
	SET_VARSIZE(cube, offsetof(NDBOX, x) + sizeof(double) * nitems);
	cube->header = header;
	for (i = 0; i < nitems; i++)
		cube->x[i] = pq_getmsgfloat8(buf);

	PG_RETURN_NDBOX_P(cube);
}

/* ---- VERBATIM contrib/cube/cube.c lines 749-1399 ---- */

/* cube_union_v0 */
NDBOX *
cube_union_v0(NDBOX *a, NDBOX *b)
{
	int			i;
	NDBOX	   *result;
	int			dim;
	int			size;

	/* trivial case */
	if (a == b)
		return a;

	/* swap the arguments if needed, so that 'a' is always larger than 'b' */
	if (DIM(a) < DIM(b))
	{
		NDBOX	   *tmp = b;

		b = a;
		a = tmp;
	}
	dim = DIM(a);

	size = CUBE_SIZE(dim);
	result = palloc0(size);
	SET_VARSIZE(result, size);
	SET_DIM(result, dim);

	/* First compute the union of the dimensions present in both args */
	for (i = 0; i < DIM(b); i++)
	{
		result->x[i] = Min(Min(LL_COORD(a, i), UR_COORD(a, i)),
						   Min(LL_COORD(b, i), UR_COORD(b, i)));
		result->x[i + DIM(a)] = Max(Max(LL_COORD(a, i), UR_COORD(a, i)),
									Max(LL_COORD(b, i), UR_COORD(b, i)));
	}
	/* continue on the higher dimensions only present in 'a' */
	for (; i < DIM(a); i++)
	{
		result->x[i] = Min(0,
						   Min(LL_COORD(a, i), UR_COORD(a, i))
			);
		result->x[i + dim] = Max(0,
								 Max(LL_COORD(a, i), UR_COORD(a, i))
			);
	}

	/*
	 * Check if the result was in fact a point, and set the flag in the datum
	 * accordingly. (we don't bother to repalloc it smaller)
	 */
	if (cube_is_point_internal(result))
	{
		size = POINT_SIZE(dim);
		SET_VARSIZE(result, size);
		SET_POINT_BIT(result);
	}

	return result;
}

Datum
cube_union(PG_FUNCTION_ARGS)
{
	NDBOX	   *a = PG_GETARG_NDBOX_P(0);
	NDBOX	   *b = PG_GETARG_NDBOX_P(1);
	NDBOX	   *res;

	res = cube_union_v0(a, b);

	PG_FREE_IF_COPY(a, 0);
	PG_FREE_IF_COPY(b, 1);
	PG_RETURN_NDBOX_P(res);
}

/* cube_inter */
Datum
cube_inter(PG_FUNCTION_ARGS)
{
	NDBOX	   *a = PG_GETARG_NDBOX_P(0);
	NDBOX	   *b = PG_GETARG_NDBOX_P(1);
	NDBOX	   *result;
	bool		swapped = false;
	int			i;
	int			dim;
	int			size;

	/* swap the arguments if needed, so that 'a' is always larger than 'b' */
	if (DIM(a) < DIM(b))
	{
		NDBOX	   *tmp = b;

		b = a;
		a = tmp;
		swapped = true;
	}
	dim = DIM(a);

	size = CUBE_SIZE(dim);
	result = (NDBOX *) palloc0(size);
	SET_VARSIZE(result, size);
	SET_DIM(result, dim);

	/* First compute intersection of the dimensions present in both args */
	for (i = 0; i < DIM(b); i++)
	{
		result->x[i] = Max(Min(LL_COORD(a, i), UR_COORD(a, i)),
						   Min(LL_COORD(b, i), UR_COORD(b, i)));
		result->x[i + DIM(a)] = Min(Max(LL_COORD(a, i), UR_COORD(a, i)),
									Max(LL_COORD(b, i), UR_COORD(b, i)));
	}
	/* continue on the higher dimensions only present in 'a' */
	for (; i < DIM(a); i++)
	{
		result->x[i] = Max(0,
						   Min(LL_COORD(a, i), UR_COORD(a, i))
			);
		result->x[i + DIM(a)] = Min(0,
									Max(LL_COORD(a, i), UR_COORD(a, i))
			);
	}

	/*
	 * Check if the result was in fact a point, and set the flag in the datum
	 * accordingly. (we don't bother to repalloc it smaller)
	 */
	if (cube_is_point_internal(result))
	{
		size = POINT_SIZE(dim);
		result = repalloc(result, size);
		SET_VARSIZE(result, size);
		SET_POINT_BIT(result);
	}

	if (swapped)
	{
		PG_FREE_IF_COPY(b, 0);
		PG_FREE_IF_COPY(a, 1);
	}
	else
	{
		PG_FREE_IF_COPY(a, 0);
		PG_FREE_IF_COPY(b, 1);
	}

	/*
	 * Is it OK to return a non-null intersection for non-overlapping boxes?
	 */
	PG_RETURN_NDBOX_P(result);
}

/* cube_size */
Datum
cube_size(PG_FUNCTION_ARGS)
{
	NDBOX	   *a = PG_GETARG_NDBOX_P(0);
	double		result;

	rt_cube_size(a, &result);
	PG_FREE_IF_COPY(a, 0);
	PG_RETURN_FLOAT8(result);
}

void
rt_cube_size(NDBOX *a, double *size)
{
	double		result;
	int			i;

	if (a == (NDBOX *) NULL)
	{
		/* special case for GiST */
		result = 0.0;
	}
	else if (IS_POINT(a) || DIM(a) == 0)
	{
		/* necessarily has zero size */
		result = 0.0;
	}
	else
	{
		result = 1.0;
		for (i = 0; i < DIM(a); i++)
			result *= fabs(UR_COORD(a, i) - LL_COORD(a, i));
	}
	*size = result;
}

/* make up a metric in which one box will be 'lower' than the other
   -- this can be useful for sorting and to determine uniqueness */
int32
cube_cmp_v0(NDBOX *a, NDBOX *b)
{
	int			i;
	int			dim;

	dim = Min(DIM(a), DIM(b));

	/* compare the common dimensions */
	for (i = 0; i < dim; i++)
	{
		if (Min(LL_COORD(a, i), UR_COORD(a, i)) >
			Min(LL_COORD(b, i), UR_COORD(b, i)))
			return 1;
		if (Min(LL_COORD(a, i), UR_COORD(a, i)) <
			Min(LL_COORD(b, i), UR_COORD(b, i)))
			return -1;
	}
	for (i = 0; i < dim; i++)
	{
		if (Max(LL_COORD(a, i), UR_COORD(a, i)) >
			Max(LL_COORD(b, i), UR_COORD(b, i)))
			return 1;
		if (Max(LL_COORD(a, i), UR_COORD(a, i)) <
			Max(LL_COORD(b, i), UR_COORD(b, i)))
			return -1;
	}

	/* compare extra dimensions to zero */
	if (DIM(a) > DIM(b))
	{
		for (i = dim; i < DIM(a); i++)
		{
			if (Min(LL_COORD(a, i), UR_COORD(a, i)) > 0)
				return 1;
			if (Min(LL_COORD(a, i), UR_COORD(a, i)) < 0)
				return -1;
		}
		for (i = dim; i < DIM(a); i++)
		{
			if (Max(LL_COORD(a, i), UR_COORD(a, i)) > 0)
				return 1;
			if (Max(LL_COORD(a, i), UR_COORD(a, i)) < 0)
				return -1;
		}

		/*
		 * if all common dimensions are equal, the cube with more dimensions
		 * wins
		 */
		return 1;
	}
	if (DIM(a) < DIM(b))
	{
		for (i = dim; i < DIM(b); i++)
		{
			if (Min(LL_COORD(b, i), UR_COORD(b, i)) > 0)
				return -1;
			if (Min(LL_COORD(b, i), UR_COORD(b, i)) < 0)
				return 1;
		}
		for (i = dim; i < DIM(b); i++)
		{
			if (Max(LL_COORD(b, i), UR_COORD(b, i)) > 0)
				return -1;
			if (Max(LL_COORD(b, i), UR_COORD(b, i)) < 0)
				return 1;
		}

		/*
		 * if all common dimensions are equal, the cube with more dimensions
		 * wins
		 */
		return -1;
	}

	/* They're really equal */
	return 0;
}

Datum
cube_cmp(PG_FUNCTION_ARGS)
{
	NDBOX	   *a = PG_GETARG_NDBOX_P(0),
			   *b = PG_GETARG_NDBOX_P(1);
	int32		res;

	res = cube_cmp_v0(a, b);

	PG_FREE_IF_COPY(a, 0);
	PG_FREE_IF_COPY(b, 1);
	PG_RETURN_INT32(res);
}


Datum
cube_eq(PG_FUNCTION_ARGS)
{
	NDBOX	   *a = PG_GETARG_NDBOX_P(0),
			   *b = PG_GETARG_NDBOX_P(1);
	int32		res;

	res = cube_cmp_v0(a, b);

	PG_FREE_IF_COPY(a, 0);
	PG_FREE_IF_COPY(b, 1);
	PG_RETURN_BOOL(res == 0);
}


Datum
cube_ne(PG_FUNCTION_ARGS)
{
	NDBOX	   *a = PG_GETARG_NDBOX_P(0),
			   *b = PG_GETARG_NDBOX_P(1);
	int32		res;

	res = cube_cmp_v0(a, b);

	PG_FREE_IF_COPY(a, 0);
	PG_FREE_IF_COPY(b, 1);
	PG_RETURN_BOOL(res != 0);
}


Datum
cube_lt(PG_FUNCTION_ARGS)
{
	NDBOX	   *a = PG_GETARG_NDBOX_P(0),
			   *b = PG_GETARG_NDBOX_P(1);
	int32		res;

	res = cube_cmp_v0(a, b);

	PG_FREE_IF_COPY(a, 0);
	PG_FREE_IF_COPY(b, 1);
	PG_RETURN_BOOL(res < 0);
}


Datum
cube_gt(PG_FUNCTION_ARGS)
{
	NDBOX	   *a = PG_GETARG_NDBOX_P(0),
			   *b = PG_GETARG_NDBOX_P(1);
	int32		res;

	res = cube_cmp_v0(a, b);

	PG_FREE_IF_COPY(a, 0);
	PG_FREE_IF_COPY(b, 1);
	PG_RETURN_BOOL(res > 0);
}


Datum
cube_le(PG_FUNCTION_ARGS)
{
	NDBOX	   *a = PG_GETARG_NDBOX_P(0),
			   *b = PG_GETARG_NDBOX_P(1);
	int32		res;

	res = cube_cmp_v0(a, b);

	PG_FREE_IF_COPY(a, 0);
	PG_FREE_IF_COPY(b, 1);
	PG_RETURN_BOOL(res <= 0);
}


Datum
cube_ge(PG_FUNCTION_ARGS)
{
	NDBOX	   *a = PG_GETARG_NDBOX_P(0),
			   *b = PG_GETARG_NDBOX_P(1);
	int32		res;

	res = cube_cmp_v0(a, b);

	PG_FREE_IF_COPY(a, 0);
	PG_FREE_IF_COPY(b, 1);
	PG_RETURN_BOOL(res >= 0);
}


/* Contains */
/* Box(A) CONTAINS Box(B) IFF pt(A) < pt(B) */
bool
cube_contains_v0(NDBOX *a, NDBOX *b)
{
	int			i;

	if ((a == NULL) || (b == NULL))
		return false;

	if (DIM(a) < DIM(b))
	{
		/*
		 * the further comparisons will make sense if the excess dimensions of
		 * (b) were zeroes Since both UL and UR coordinates must be zero, we
		 * can check them all without worrying about which is which.
		 */
		for (i = DIM(a); i < DIM(b); i++)
		{
			if (LL_COORD(b, i) != 0)
				return false;
			if (UR_COORD(b, i) != 0)
				return false;
		}
	}

	/* Can't care less about the excess dimensions of (a), if any */
	for (i = 0; i < Min(DIM(a), DIM(b)); i++)
	{
		if (Min(LL_COORD(a, i), UR_COORD(a, i)) >
			Min(LL_COORD(b, i), UR_COORD(b, i)))
			return false;
		if (Max(LL_COORD(a, i), UR_COORD(a, i)) <
			Max(LL_COORD(b, i), UR_COORD(b, i)))
			return false;
	}

	return true;
}

Datum
cube_contains(PG_FUNCTION_ARGS)
{
	NDBOX	   *a = PG_GETARG_NDBOX_P(0),
			   *b = PG_GETARG_NDBOX_P(1);
	bool		res;

	res = cube_contains_v0(a, b);

	PG_FREE_IF_COPY(a, 0);
	PG_FREE_IF_COPY(b, 1);
	PG_RETURN_BOOL(res);
}

/* Contained */
/* Box(A) Contained by Box(B) IFF Box(B) Contains Box(A) */
Datum
cube_contained(PG_FUNCTION_ARGS)
{
	NDBOX	   *a = PG_GETARG_NDBOX_P(0),
			   *b = PG_GETARG_NDBOX_P(1);
	bool		res;

	res = cube_contains_v0(b, a);

	PG_FREE_IF_COPY(a, 0);
	PG_FREE_IF_COPY(b, 1);
	PG_RETURN_BOOL(res);
}

/* Overlap */
/* Box(A) Overlap Box(B) IFF (pt(a)LL < pt(B)UR) && (pt(b)LL < pt(a)UR) */
bool
cube_overlap_v0(NDBOX *a, NDBOX *b)
{
	int			i;

	if ((a == NULL) || (b == NULL))
		return false;

	/* swap the box pointers if needed */
	if (DIM(a) < DIM(b))
	{
		NDBOX	   *tmp = b;

		b = a;
		a = tmp;
	}

	/* compare within the dimensions of (b) */
	for (i = 0; i < DIM(b); i++)
	{
		if (Min(LL_COORD(a, i), UR_COORD(a, i)) > Max(LL_COORD(b, i), UR_COORD(b, i)))
			return false;
		if (Max(LL_COORD(a, i), UR_COORD(a, i)) < Min(LL_COORD(b, i), UR_COORD(b, i)))
			return false;
	}

	/* compare to zero those dimensions in (a) absent in (b) */
	for (i = DIM(b); i < DIM(a); i++)
	{
		if (Min(LL_COORD(a, i), UR_COORD(a, i)) > 0)
			return false;
		if (Max(LL_COORD(a, i), UR_COORD(a, i)) < 0)
			return false;
	}

	return true;
}


Datum
cube_overlap(PG_FUNCTION_ARGS)
{
	NDBOX	   *a = PG_GETARG_NDBOX_P(0),
			   *b = PG_GETARG_NDBOX_P(1);
	bool		res;

	res = cube_overlap_v0(a, b);

	PG_FREE_IF_COPY(a, 0);
	PG_FREE_IF_COPY(b, 1);
	PG_RETURN_BOOL(res);
}


/* Distance */
/* The distance is computed as a per axis sum of the squared distances
   between 1D projections of the boxes onto Cartesian axes. Assuming zero
   distance between overlapping projections, this metric coincides with the
   "common sense" geometric distance */
Datum
cube_distance(PG_FUNCTION_ARGS)
{
	NDBOX	   *a = PG_GETARG_NDBOX_P(0),
			   *b = PG_GETARG_NDBOX_P(1);
	bool		swapped = false;
	double		d,
				distance;
	int			i;

	/* swap the box pointers if needed */
	if (DIM(a) < DIM(b))
	{
		NDBOX	   *tmp = b;

		b = a;
		a = tmp;
		swapped = true;
	}

	distance = 0.0;
	/* compute within the dimensions of (b) */
	for (i = 0; i < DIM(b); i++)
	{
		d = distance_1D(LL_COORD(a, i), UR_COORD(a, i), LL_COORD(b, i), UR_COORD(b, i));
		distance += d * d;
	}

	/* compute distance to zero for those dimensions in (a) absent in (b) */
	for (i = DIM(b); i < DIM(a); i++)
	{
		d = distance_1D(LL_COORD(a, i), UR_COORD(a, i), 0.0, 0.0);
		distance += d * d;
	}

	if (swapped)
	{
		PG_FREE_IF_COPY(b, 0);
		PG_FREE_IF_COPY(a, 1);
	}
	else
	{
		PG_FREE_IF_COPY(a, 0);
		PG_FREE_IF_COPY(b, 1);
	}

	PG_RETURN_FLOAT8(sqrt(distance));
}

Datum
distance_taxicab(PG_FUNCTION_ARGS)
{
	NDBOX	   *a = PG_GETARG_NDBOX_P(0),
			   *b = PG_GETARG_NDBOX_P(1);
	bool		swapped = false;
	double		distance;
	int			i;

	/* swap the box pointers if needed */
	if (DIM(a) < DIM(b))
	{
		NDBOX	   *tmp = b;

		b = a;
		a = tmp;
		swapped = true;
	}

	distance = 0.0;
	/* compute within the dimensions of (b) */
	for (i = 0; i < DIM(b); i++)
		distance += fabs(distance_1D(LL_COORD(a, i), UR_COORD(a, i),
									 LL_COORD(b, i), UR_COORD(b, i)));

	/* compute distance to zero for those dimensions in (a) absent in (b) */
	for (i = DIM(b); i < DIM(a); i++)
		distance += fabs(distance_1D(LL_COORD(a, i), UR_COORD(a, i),
									 0.0, 0.0));

	if (swapped)
	{
		PG_FREE_IF_COPY(b, 0);
		PG_FREE_IF_COPY(a, 1);
	}
	else
	{
		PG_FREE_IF_COPY(a, 0);
		PG_FREE_IF_COPY(b, 1);
	}

	PG_RETURN_FLOAT8(distance);
}

Datum
distance_chebyshev(PG_FUNCTION_ARGS)
{
	NDBOX	   *a = PG_GETARG_NDBOX_P(0),
			   *b = PG_GETARG_NDBOX_P(1);
	bool		swapped = false;
	double		d,
				distance;
	int			i;

	/* swap the box pointers if needed */
	if (DIM(a) < DIM(b))
	{
		NDBOX	   *tmp = b;

		b = a;
		a = tmp;
		swapped = true;
	}

	distance = 0.0;
	/* compute within the dimensions of (b) */
	for (i = 0; i < DIM(b); i++)
	{
		d = fabs(distance_1D(LL_COORD(a, i), UR_COORD(a, i),
							 LL_COORD(b, i), UR_COORD(b, i)));
		if (d > distance)
			distance = d;
	}

	/* compute distance to zero for those dimensions in (a) absent in (b) */
	for (i = DIM(b); i < DIM(a); i++)
	{
		d = fabs(distance_1D(LL_COORD(a, i), UR_COORD(a, i), 0.0, 0.0));
		if (d > distance)
			distance = d;
	}

	if (swapped)
	{
		PG_FREE_IF_COPY(b, 0);
		PG_FREE_IF_COPY(a, 1);
	}
	else
	{
		PG_FREE_IF_COPY(a, 0);
		PG_FREE_IF_COPY(b, 1);
	}

	PG_RETURN_FLOAT8(distance);
}

/* ---- VERBATIM contrib/cube/cube.c lines 1505-1913 ---- */

static double
distance_1D(double a1, double a2, double b1, double b2)
{
	/* interval (a) is entirely on the left of (b) */
	if ((a1 <= b1) && (a2 <= b1) && (a1 <= b2) && (a2 <= b2))
		return (Min(b1, b2) - Max(a1, a2));

	/* interval (a) is entirely on the right of (b) */
	if ((a1 > b1) && (a2 > b1) && (a1 > b2) && (a2 > b2))
		return (Min(a1, a2) - Max(b1, b2));

	/* the rest are all sorts of intersections */
	return 0.0;
}

/* Test if a box is also a point */
Datum
cube_is_point(PG_FUNCTION_ARGS)
{
	NDBOX	   *cube = PG_GETARG_NDBOX_P(0);
	bool		result;

	result = cube_is_point_internal(cube);
	PG_FREE_IF_COPY(cube, 0);
	PG_RETURN_BOOL(result);
}

static bool
cube_is_point_internal(NDBOX *cube)
{
	int			i;

	if (IS_POINT(cube))
		return true;

	/*
	 * Even if the point-flag is not set, all the lower-left coordinates might
	 * match the upper-right coordinates, so that the value is in fact a
	 * point. Such values don't arise with current code - the point flag is
	 * always set if appropriate - but they might be present on-disk in
	 * clusters upgraded from pre-9.4 versions.
	 */
	for (i = 0; i < DIM(cube); i++)
	{
		if (LL_COORD(cube, i) != UR_COORD(cube, i))
			return false;
	}
	return true;
}

/* Return dimensions in use in the data structure */
Datum
cube_dim(PG_FUNCTION_ARGS)
{
	NDBOX	   *c = PG_GETARG_NDBOX_P(0);
	int			dim = DIM(c);

	PG_FREE_IF_COPY(c, 0);
	PG_RETURN_INT32(dim);
}

/* Return a specific normalized LL coordinate */
Datum
cube_ll_coord(PG_FUNCTION_ARGS)
{
	NDBOX	   *c = PG_GETARG_NDBOX_P(0);
	int			n = PG_GETARG_INT32(1);
	double		result;

	if (DIM(c) >= n && n > 0)
		result = Min(LL_COORD(c, n - 1), UR_COORD(c, n - 1));
	else
		result = 0;

	PG_FREE_IF_COPY(c, 0);
	PG_RETURN_FLOAT8(result);
}

/* Return a specific normalized UR coordinate */
Datum
cube_ur_coord(PG_FUNCTION_ARGS)
{
	NDBOX	   *c = PG_GETARG_NDBOX_P(0);
	int			n = PG_GETARG_INT32(1);
	double		result;

	if (DIM(c) >= n && n > 0)
		result = Max(LL_COORD(c, n - 1), UR_COORD(c, n - 1));
	else
		result = 0;

	PG_FREE_IF_COPY(c, 0);
	PG_RETURN_FLOAT8(result);
}

/*
 * Function returns cube coordinate.
 * Numbers from 1 to DIM denotes first corner coordinates.
 * Numbers from DIM+1 to 2*DIM denotes second corner coordinates.
 */
Datum
cube_coord(PG_FUNCTION_ARGS)
{
	NDBOX	   *cube = PG_GETARG_NDBOX_P(0);
	int			coord = PG_GETARG_INT32(1);

	if (coord <= 0 || coord > 2 * DIM(cube))
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_ELEMENT_ERROR),
				 errmsg("cube index %d is out of bounds", coord)));

	if (IS_POINT(cube))
		PG_RETURN_FLOAT8(cube->x[(coord - 1) % DIM(cube)]);
	else
		PG_RETURN_FLOAT8(cube->x[coord - 1]);
}


/*----
 * This function works like cube_coord(), but rearranges coordinates in the
 * way suitable to support coordinate ordering using KNN-GiST.  For historical
 * reasons this extension allows us to create cubes in form ((2,1),(1,2)) and
 * instead of normalizing such cube to ((1,1),(2,2)) it stores cube in original
 * way.  But in order to get cubes ordered by one of dimensions from the index
 * without explicit sort step we need this representation-independent coordinate
 * getter.  Moreover, indexed dataset may contain cubes of different dimensions
 * number.  Accordingly, this coordinate getter should be able to return
 * lower/upper bound for particular dimension independently on number of cube
 * dimensions.  Also, KNN-GiST supports only ascending sorting.  In order to
 * support descending sorting, this function returns inverse of value when
 * negative coordinate is given.
 *
 * Long story short, this function uses following meaning of coordinates:
 * # (2 * N - 1) -- lower bound of Nth dimension,
 * # (2 * N) -- upper bound of Nth dimension,
 * # - (2 * N - 1) -- negative of lower bound of Nth dimension,
 * # - (2 * N) -- negative of upper bound of Nth dimension.
 *
 * When given coordinate exceeds number of cube dimensions, then 0 returned
 * (reproducing logic of GiST indexing of variable-length cubes).
 */
Datum
cube_coord_llur(PG_FUNCTION_ARGS)
{
	NDBOX	   *cube = PG_GETARG_NDBOX_P(0);
	int			coord = PG_GETARG_INT32(1);
	bool		inverse = false;
	float8		result;

	/* 0 is the only unsupported coordinate value */
	if (coord == 0)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_ELEMENT_ERROR),
				 errmsg("zero cube index is not defined")));

	/* Return inversed value for negative coordinate */
	if (coord < 0)
	{
		coord = -coord;
		inverse = true;
	}

	if (coord <= 2 * DIM(cube))
	{
		/* dimension index */
		int			index = (coord - 1) / 2;

		/* whether this is upper bound (lower bound otherwise) */
		bool		upper = ((coord - 1) % 2 == 1);

		if (IS_POINT(cube))
		{
			result = cube->x[index];
		}
		else
		{
			if (upper)
				result = Max(cube->x[index], cube->x[index + DIM(cube)]);
			else
				result = Min(cube->x[index], cube->x[index + DIM(cube)]);
		}
	}
	else
	{
		/*
		 * Return zero if coordinate is out of bound.  That reproduces logic
		 * of how cubes with low dimension number are expanded during GiST
		 * indexing.
		 */
		result = 0.0;
	}

	/* Inverse value if needed */
	if (inverse)
		result = -result;

	PG_RETURN_FLOAT8(result);
}

/* Increase or decrease box size by a radius in at least n dimensions. */
Datum
cube_enlarge(PG_FUNCTION_ARGS)
{
	NDBOX	   *a = PG_GETARG_NDBOX_P(0);
	double		r = PG_GETARG_FLOAT8(1);
	int32		n = PG_GETARG_INT32(2);
	NDBOX	   *result;
	int			dim = 0;
	int			size;
	int			i,
				j;

	if (n > CUBE_MAX_DIM)
		n = CUBE_MAX_DIM;
	if (r > 0 && n > 0)
		dim = n;
	if (DIM(a) > dim)
		dim = DIM(a);

	size = CUBE_SIZE(dim);
	result = (NDBOX *) palloc0(size);
	SET_VARSIZE(result, size);
	SET_DIM(result, dim);

	for (i = 0, j = dim; i < DIM(a); i++, j++)
	{
		if (LL_COORD(a, i) >= UR_COORD(a, i))
		{
			result->x[i] = UR_COORD(a, i) - r;
			result->x[j] = LL_COORD(a, i) + r;
		}
		else
		{
			result->x[i] = LL_COORD(a, i) - r;
			result->x[j] = UR_COORD(a, i) + r;
		}
		if (result->x[i] > result->x[j])
		{
			result->x[i] = (result->x[i] + result->x[j]) / 2;
			result->x[j] = result->x[i];
		}
	}
	/* dim > a->dim only if r > 0 */
	for (; i < dim; i++, j++)
	{
		result->x[i] = -r;
		result->x[j] = r;
	}

	/*
	 * Check if the result was in fact a point, and set the flag in the datum
	 * accordingly. (we don't bother to repalloc it smaller)
	 */
	if (cube_is_point_internal(result))
	{
		size = POINT_SIZE(dim);
		SET_VARSIZE(result, size);
		SET_POINT_BIT(result);
	}

	PG_FREE_IF_COPY(a, 0);
	PG_RETURN_NDBOX_P(result);
}

/* Create a one dimensional box with identical upper and lower coordinates */
Datum
cube_f8(PG_FUNCTION_ARGS)
{
	double		x = PG_GETARG_FLOAT8(0);
	NDBOX	   *result;
	int			size;

	size = POINT_SIZE(1);
	result = (NDBOX *) palloc0(size);
	SET_VARSIZE(result, size);
	SET_DIM(result, 1);
	SET_POINT_BIT(result);
	result->x[0] = x;

	PG_RETURN_NDBOX_P(result);
}

/* Create a one dimensional box */
Datum
cube_f8_f8(PG_FUNCTION_ARGS)
{
	double		x0 = PG_GETARG_FLOAT8(0);
	double		x1 = PG_GETARG_FLOAT8(1);
	NDBOX	   *result;
	int			size;

	if (x0 == x1)
	{
		size = POINT_SIZE(1);
		result = (NDBOX *) palloc0(size);
		SET_VARSIZE(result, size);
		SET_DIM(result, 1);
		SET_POINT_BIT(result);
		result->x[0] = x0;
	}
	else
	{
		size = CUBE_SIZE(1);
		result = (NDBOX *) palloc0(size);
		SET_VARSIZE(result, size);
		SET_DIM(result, 1);
		result->x[0] = x0;
		result->x[1] = x1;
	}

	PG_RETURN_NDBOX_P(result);
}

/* Add a dimension to an existing cube with the same values for the new
   coordinate */
Datum
cube_c_f8(PG_FUNCTION_ARGS)
{
	NDBOX	   *cube = PG_GETARG_NDBOX_P(0);
	double		x = PG_GETARG_FLOAT8(1);
	NDBOX	   *result;
	int			size;
	int			i;

	if (DIM(cube) + 1 > CUBE_MAX_DIM)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("can't extend cube"),
				 errdetail("A cube cannot have more than %d dimensions.",
						   CUBE_MAX_DIM)));

	if (IS_POINT(cube))
	{
		size = POINT_SIZE((DIM(cube) + 1));
		result = (NDBOX *) palloc0(size);
		SET_VARSIZE(result, size);
		SET_DIM(result, DIM(cube) + 1);
		SET_POINT_BIT(result);
		for (i = 0; i < DIM(cube); i++)
			result->x[i] = cube->x[i];
		result->x[DIM(result) - 1] = x;
	}
	else
	{
		size = CUBE_SIZE((DIM(cube) + 1));
		result = (NDBOX *) palloc0(size);
		SET_VARSIZE(result, size);
		SET_DIM(result, DIM(cube) + 1);
		for (i = 0; i < DIM(cube); i++)
		{
			result->x[i] = cube->x[i];
			result->x[DIM(result) + i] = cube->x[DIM(cube) + i];
		}
		result->x[DIM(result) - 1] = x;
		result->x[2 * DIM(result) - 1] = x;
	}

	PG_FREE_IF_COPY(cube, 0);
	PG_RETURN_NDBOX_P(result);
}

/* Add a dimension to an existing cube */
Datum
cube_c_f8_f8(PG_FUNCTION_ARGS)
{
	NDBOX	   *cube = PG_GETARG_NDBOX_P(0);
	double		x1 = PG_GETARG_FLOAT8(1);
	double		x2 = PG_GETARG_FLOAT8(2);
	NDBOX	   *result;
	int			size;
	int			i;

	if (DIM(cube) + 1 > CUBE_MAX_DIM)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("can't extend cube"),
				 errdetail("A cube cannot have more than %d dimensions.",
						   CUBE_MAX_DIM)));

	if (IS_POINT(cube) && (x1 == x2))
	{
		size = POINT_SIZE((DIM(cube) + 1));
		result = (NDBOX *) palloc0(size);
		SET_VARSIZE(result, size);
		SET_DIM(result, DIM(cube) + 1);
		SET_POINT_BIT(result);
		for (i = 0; i < DIM(cube); i++)
			result->x[i] = cube->x[i];
		result->x[DIM(result) - 1] = x1;
	}
	else
	{
		size = CUBE_SIZE((DIM(cube) + 1));
		result = (NDBOX *) palloc0(size);
		SET_VARSIZE(result, size);
		SET_DIM(result, DIM(cube) + 1);
		for (i = 0; i < DIM(cube); i++)
		{
			result->x[i] = LL_COORD(cube, i);
			result->x[DIM(result) + i] = UR_COORD(cube, i);
		}
		result->x[DIM(result) - 1] = x1;
		result->x[2 * DIM(result) - 1] = x2;
	}

	PG_FREE_IF_COPY(cube, 0);
	PG_RETURN_NDBOX_P(result);
}

/* ========== SECTION D: fuzz-facing driver entries (NOT Postgres code) ===== */

/*
 * Every entry arms the jmp_buf and reports the unified errcode (0 ok, else
 * the code table in include/postgres.h). The caller resets the arena +
 * channels once per exec via pg_cb_reset(); entries additionally reset the
 * ERROR CHANNELS (not the arena) on entry so multiple calls per exec don't
 * leak verdicts into each other.
 */

typedef char pg_cb_seg_is_12_bytes[sizeof(SEG) == 12 ? 1 : -1];
typedef char pg_cb_ndbox_hdr_is_8[offsetof(NDBOX, x) == 8 ? 1 : -1];

#define PG_CB_ENTRY \
	do { \
		pg_cb_errreset(); \
		if (setjmp(pg_cb_jmp) != 0) \
			return pg_cb_geterr(); \
	} while (0)

/* ---- seg ---- */

int
pg_cb_seg_in(const char *str, uint8 *out12)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	FunctionCallInfoBaseData fcinfo;
	Datum		d;

	PG_CB_ENTRY;
	memset(&fcinfo, 0, sizeof(fcinfo));
	fcinfo.nargs = 1;
	fcinfo.args[0].value = PointerGetDatum(str);
	d = seg_in(&fcinfo);
	if (pg_cb_geterr())
		return pg_cb_geterr();
	memcpy(out12, DatumGetPointer(d), sizeof(SEG));
	return 0;
}

int
pg_cb_seg_out(const uint8 *seg12, char *out, int outsz)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	FunctionCallInfoBaseData fcinfo;
	SEG			s;
	Datum		d;

	PG_CB_ENTRY;
	memcpy(&s, seg12, sizeof(SEG));
	memset(&fcinfo, 0, sizeof(fcinfo));
	fcinfo.nargs = 1;
	fcinfo.args[0].value = PointerGetDatum(&s);
	d = seg_out(&fcinfo);
	if (pg_cb_geterr())
		return pg_cb_geterr();
	snprintf(out, outsz, "%s", (char *) DatumGetPointer(d));
	return 0;
}

/*
 * op: 0 cmp -> *iout (int32); 1 lt 2 le 3 gt 4 ge 5 same 6 different
 * 7 contains 8 contained 9 overlap 10 left 11 right 12 over_left
 * 13 over_right -> *iout (bool); 14 union 15 inter -> segout12.
 */
int
pg_cb_seg_binop(int op, const uint8 *a12, const uint8 *b12,
				int32 *iout, uint8 *segout12)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	FunctionCallInfoBaseData fcinfo;
	SEG			a,
				b;
	Datum		d;

	PG_CB_ENTRY;
	memcpy(&a, a12, sizeof(SEG));
	memcpy(&b, b12, sizeof(SEG));
	memset(&fcinfo, 0, sizeof(fcinfo));
	fcinfo.nargs = 2;
	fcinfo.args[0].value = PointerGetDatum(&a);
	fcinfo.args[1].value = PointerGetDatum(&b);
	switch (op)
	{
		case 0:
			d = seg_cmp(&fcinfo);
			break;
		case 1:
			d = seg_lt(&fcinfo);
			break;
		case 2:
			d = seg_le(&fcinfo);
			break;
		case 3:
			d = seg_gt(&fcinfo);
			break;
		case 4:
			d = seg_ge(&fcinfo);
			break;
		case 5:
			d = seg_same(&fcinfo);
			break;
		case 6:
			d = seg_different(&fcinfo);
			break;
		case 7:
			d = seg_contains(&fcinfo);
			break;
		case 8:
			d = seg_contained(&fcinfo);
			break;
		case 9:
			d = seg_overlap(&fcinfo);
			break;
		case 10:
			d = seg_left(&fcinfo);
			break;
		case 11:
			d = seg_right(&fcinfo);
			break;
		case 12:
			d = seg_over_left(&fcinfo);
			break;
		case 13:
			d = seg_over_right(&fcinfo);
			break;
		case 14:
			d = seg_union(&fcinfo);
			break;
		case 15:
			d = seg_inter(&fcinfo);
			break;
		default:
			abort();
	}
	if (pg_cb_geterr())
		return pg_cb_geterr();
	if (op == 14 || op == 15)
		memcpy(segout12, DatumGetPointer(d), sizeof(SEG));
	else if (op == 0)
		*iout = DatumGetInt32(d);
	else
		*iout = (int32) DatumGetBool(d);
	return 0;
}

/* op: 0 center 1 lower 2 upper 3 size -> *bits (float4 image) */
int
pg_cb_seg_unop(int op, const uint8 *a12, uint32 *bits)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	FunctionCallInfoBaseData fcinfo;
	SEG			a;
	Datum		d;

	PG_CB_ENTRY;
	memcpy(&a, a12, sizeof(SEG));
	memset(&fcinfo, 0, sizeof(fcinfo));
	fcinfo.nargs = 1;
	fcinfo.args[0].value = PointerGetDatum(&a);
	switch (op)
	{
		case 0:
			d = seg_center(&fcinfo);
			break;
		case 1:
			d = seg_lower(&fcinfo);
			break;
		case 2:
			d = seg_upper(&fcinfo);
			break;
		case 3:
			d = seg_size(&fcinfo);
			break;
		default:
			abort();
	}
	if (pg_cb_geterr())
		return pg_cb_geterr();
	*bits = (uint32) d;
	return 0;
}

/* ---- cube ---- */

/* arena copy so NDBOX double access is 8-aligned regardless of caller */
static NDBOX *
pg_cb_cube_load(const uint8 *img, int len)
{
	NDBOX	   *p = (NDBOX *) pg_cb_palloc(len);

	memcpy(p, img, len);
	return p;
}

static int
pg_cb_cube_store(Datum d, uint8 *out, int cap, int *outlen)
{
	NDBOX	   *r = (NDBOX *) DatumGetPointer(d);
	int			n = (int) VARSIZE(r);

	if (n > cap)
		abort();				/* driver sizing bug, not a finding */
	memcpy(out, r, n);
	*outlen = n;
	return 0;
}

int
pg_cb_cube_in(const char *str, uint8 *out, int cap, int *outlen)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	FunctionCallInfoBaseData fcinfo;
	Datum		d;

	PG_CB_ENTRY;
	memset(&fcinfo, 0, sizeof(fcinfo));
	fcinfo.nargs = 1;
	fcinfo.args[0].value = PointerGetDatum(str);
	d = cube_in(&fcinfo);
	if (pg_cb_geterr())
		return pg_cb_geterr();
	return pg_cb_cube_store(d, out, cap, outlen);
}

int
pg_cb_cube_out(const uint8 *img, int len, char *out, int outsz)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	FunctionCallInfoBaseData fcinfo;
	Datum		d;
	const char *s;

	PG_CB_ENTRY;
	memset(&fcinfo, 0, sizeof(fcinfo));
	fcinfo.nargs = 1;
	fcinfo.args[0].value = PointerGetDatum(pg_cb_cube_load(img, len));
	d = cube_out(&fcinfo);
	if (pg_cb_geterr())
		return pg_cb_geterr();
	s = (const char *) DatumGetPointer(d);
	if ((int) strlen(s) >= outsz)
		abort();				/* driver sizing bug */
	strcpy(out, s);
	return 0;
}

int
pg_cb_cube_send(const uint8 *img, int len, uint8 *out, int cap, int *outlen)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	FunctionCallInfoBaseData fcinfo;
	Datum		d;
	bytea	   *b;
	int			n;

	PG_CB_ENTRY;
	memset(&fcinfo, 0, sizeof(fcinfo));
	fcinfo.nargs = 1;
	fcinfo.args[0].value = PointerGetDatum(pg_cb_cube_load(img, len));
	d = cube_send(&fcinfo);
	if (pg_cb_geterr())
		return pg_cb_geterr();
	b = (bytea *) DatumGetPointer(d);
	n = (int) VARSIZE(b) - VARHDRSZ;
	if (n > cap)
		abort();
	memcpy(out, VARDATA(b), n);
	*outlen = n;
	return 0;
}

int
pg_cb_cube_recv(const uint8 *msg, int msglen, uint8 *out, int cap, int *outlen)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	FunctionCallInfoBaseData fcinfo;
	StringInfoData buf;
	Datum		d;

	PG_CB_ENTRY;
	buf.data = (char *) pg_cb_palloc(msglen ? msglen : 1);
	memcpy(buf.data, msg, msglen);
	buf.len = msglen;
	buf.maxlen = msglen;
	buf.cursor = 0;
	memset(&fcinfo, 0, sizeof(fcinfo));
	fcinfo.nargs = 1;
	fcinfo.args[0].value = PointerGetDatum(&buf);
	d = cube_recv(&fcinfo);
	if (pg_cb_geterr())
		return pg_cb_geterr();
	return pg_cb_cube_store(d, out, cap, outlen);
}

/*
 * op: 0 cmp -> *iout; 1 eq 2 ne 3 lt 4 gt 5 le 6 ge 7 contains 8 contained
 * 9 overlap -> *iout (bool); 10 union 11 inter -> imgout; 12 distance
 * 13 taxicab 14 chebyshev -> *fbits (float8 image); 15 union with ONE
 * arena copy passed as both args (cube_union_v0's a==b pointer-identity
 * trivial case, which returns the input UNNORMALIZED).
 */
int
pg_cb_cube_binop(int op, const uint8 *a, int alen, const uint8 *b, int blen,
				 int32 *iout, uint64 *fbits, uint8 *imgout, int *imgoutlen)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	FunctionCallInfoBaseData fcinfo;
	Datum		d;

	PG_CB_ENTRY;
	memset(&fcinfo, 0, sizeof(fcinfo));
	fcinfo.nargs = 2;
	fcinfo.args[0].value = PointerGetDatum(pg_cb_cube_load(a, alen));
	fcinfo.args[1].value = (op == 15) ? fcinfo.args[0].value
		: PointerGetDatum(pg_cb_cube_load(b, blen));
	switch (op)
	{
		case 0:
			d = cube_cmp(&fcinfo);
			break;
		case 1:
			d = cube_eq(&fcinfo);
			break;
		case 2:
			d = cube_ne(&fcinfo);
			break;
		case 3:
			d = cube_lt(&fcinfo);
			break;
		case 4:
			d = cube_gt(&fcinfo);
			break;
		case 5:
			d = cube_le(&fcinfo);
			break;
		case 6:
			d = cube_ge(&fcinfo);
			break;
		case 7:
			d = cube_contains(&fcinfo);
			break;
		case 8:
			d = cube_contained(&fcinfo);
			break;
		case 9:
			d = cube_overlap(&fcinfo);
			break;
		case 10:
			d = cube_union(&fcinfo);
			break;
		case 11:
			d = cube_inter(&fcinfo);
			break;
		case 12:
			d = cube_distance(&fcinfo);
			break;
		case 13:
			d = distance_taxicab(&fcinfo);
			break;
		case 14:
			d = distance_chebyshev(&fcinfo);
			break;
		case 15:
			d = cube_union(&fcinfo);
			break;
		default:
			abort();
	}
	if (pg_cb_geterr())
		return pg_cb_geterr();
	if (op == 10 || op == 11 || op == 15)
		return pg_cb_cube_store(d, imgout, 2048, imgoutlen);
	if (op >= 12)
		*fbits = (uint64) d;
	else if (op == 0)
		*iout = DatumGetInt32(d);
	else
		*iout = (int32) DatumGetBool(d);
	return 0;
}

/*
 * op: 0 dim 1 is_point -> *iout; 2 size 3 ll_coord(n) 4 ur_coord(n)
 * 5 coord(n) 6 coord_llur(n) -> *fbits; 7 enlarge(f1, n) 8 cube_f8(f1)
 * 9 cube_f8_f8(f1, f2) 10 c_f8(f1) 11 c_f8_f8(f1, f2) -> imgout.
 * f1/f2 are float8 images (bits).
 */
int
pg_cb_cube_unop(int op, const uint8 *img, int len, int32 n,
				uint64 f1bits, uint64 f2bits,
				int32 *iout, uint64 *fbits, uint8 *imgout, int *imgoutlen)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	FunctionCallInfoBaseData fcinfo;
	Datum		d;

	PG_CB_ENTRY;
	memset(&fcinfo, 0, sizeof(fcinfo));
	fcinfo.args[0].value = PointerGetDatum(pg_cb_cube_load(img, len));
	switch (op)
	{
		case 0:
			fcinfo.nargs = 1;
			d = cube_dim(&fcinfo);
			break;
		case 1:
			fcinfo.nargs = 1;
			d = cube_is_point(&fcinfo);
			break;
		case 2:
			fcinfo.nargs = 1;
			d = cube_size(&fcinfo);
			break;
		case 3:
			fcinfo.nargs = 2;
			fcinfo.args[1].value = Int32GetDatum(n);
			d = cube_ll_coord(&fcinfo);
			break;
		case 4:
			fcinfo.nargs = 2;
			fcinfo.args[1].value = Int32GetDatum(n);
			d = cube_ur_coord(&fcinfo);
			break;
		case 5:
			fcinfo.nargs = 2;
			fcinfo.args[1].value = Int32GetDatum(n);
			d = cube_coord(&fcinfo);
			break;
		case 6:
			fcinfo.nargs = 2;
			fcinfo.args[1].value = Int32GetDatum(n);
			d = cube_coord_llur(&fcinfo);
			break;
		case 7:
			fcinfo.nargs = 3;
			fcinfo.args[1].value = (Datum) f1bits;
			fcinfo.args[2].value = Int32GetDatum(n);
			d = cube_enlarge(&fcinfo);
			break;
		case 8:
			fcinfo.nargs = 1;
			fcinfo.args[0].value = (Datum) f1bits;
			d = cube_f8(&fcinfo);
			break;
		case 9:
			fcinfo.nargs = 2;
			fcinfo.args[0].value = (Datum) f1bits;
			fcinfo.args[1].value = (Datum) f2bits;
			d = cube_f8_f8(&fcinfo);
			break;
		case 10:
			fcinfo.nargs = 2;
			fcinfo.args[1].value = (Datum) f1bits;
			d = cube_c_f8(&fcinfo);
			break;
		case 11:
			fcinfo.nargs = 3;
			fcinfo.args[1].value = (Datum) f1bits;
			fcinfo.args[2].value = (Datum) f2bits;
			d = cube_c_f8_f8(&fcinfo);
			break;
		default:
			abort();
	}
	if (pg_cb_geterr())
		return pg_cb_geterr();
	if (op >= 7)
		return pg_cb_cube_store(d, imgout, 2048, imgoutlen);
	if (op >= 2)
		*fbits = (uint64) d;
	else
		*iout = DatumGetInt32(d);
	return 0;
}

/*
 * op: 0 cube_a_f8_f8(arr1, arr2) 1 cube_a_f8(arr1) 2 cube_subset(cube=arr2
 * as image, arr1 int4 index array). arr blobs are raw ArrayType images.
 */
int
pg_cb_cube_arrayop(int op, const uint8 *arr1, int len1,
				   const uint8 *arr2, int len2,
				   uint8 *imgout, int *imgoutlen)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	FunctionCallInfoBaseData fcinfo;
	void	   *p1;
	void	   *p2;
	Datum		d;

	PG_CB_ENTRY;
	p1 = pg_cb_palloc(len1 ? len1 : 1);
	memcpy(p1, arr1, len1);
	p2 = pg_cb_palloc(len2 ? len2 : 1);
	memcpy(p2, arr2, len2);
	memset(&fcinfo, 0, sizeof(fcinfo));
	switch (op)
	{
		case 0:
			fcinfo.nargs = 2;
			fcinfo.args[0].value = PointerGetDatum(p1);
			fcinfo.args[1].value = PointerGetDatum(p2);
			d = cube_a_f8_f8(&fcinfo);
			break;
		case 1:
			fcinfo.nargs = 1;
			fcinfo.args[0].value = PointerGetDatum(p1);
			d = cube_a_f8(&fcinfo);
			break;
		case 2:
			fcinfo.nargs = 2;
			fcinfo.args[0].value = PointerGetDatum(p2);
			fcinfo.args[1].value = PointerGetDatum(p1);
			d = cube_subset(&fcinfo);
			break;
		default:
			abort();
	}
	if (pg_cb_geterr())
		return pg_cb_geterr();
	return pg_cb_cube_store(d, imgout, 2048, imgoutlen);
}
