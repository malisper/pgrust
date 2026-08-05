/*
 * Vendored PostgreSQL C: point_out image + on_ppath predicate —
 * differential-fuzz oracle for the geo `wall: CNF width` / `wall: 53-bit`
 * ledger classes.
 *
 * Provenance (bodies VERBATIM unless a shim is listed below), all from
 * postgres-src 62d6c7d3df6287f1bd83199c1a746e50d31571a0 (REL_18):
 *   - src/backend/utils/adt/geo_ops.c: pair_encode, path_encode, point_dt,
 *     point_inside, lseg_crossing, on_ppath core, pg_hypot.
 *   - src/include/utils/geo_decls.h: FPzero/FPeq/FPlt/FPle/FPgt/FPge,
 *     EPSILON, Point, HYPOT.
 *   - src/include/utils/float.h: float8_pl, float8_mi, float8_mul.
 *
 * Shims (plumbing only, never logic):
 *   - float_overflow_error / float_underflow_error (noreturn ereport in the
 *     real float.c) -> record errcode 2 (22003) and longjmp out, modeling
 *     the noreturn exactly (proofs/float-arith flag precedent, upgraded to
 *     longjmp so no garbage value is ever compared).
 *   - StringInfo -> fixed-capacity buffer struct; initStringInfo /
 *     appendStringInfoChar / appendStringInfo("%s,%s",...) implemented over
 *     it with snprintf. path_encode output here is <= 2*25+3 bytes for one
 *     point; capacity 512 asserts unhit.
 *   - palloc/pfree -> malloc/free (pair_encode's float8out_internal temps).
 *   - PG_FUNCTION_ARGS unwrapping -> plain signatures over (px,py,closed,
 *     npts,Point*), exactly what PG_GETARG_POINT_P/PATH_P deliver.
 *   - get_float8_infinity/nan duplicated from float.h (verbatim) as
 *     pg_geo_* statics to keep this translation unit standalone.
 */

#include "postgres.h"

#include <assert.h>
#include <math.h>
#include <limits.h>
#include <setjmp.h>
#include <stdio.h>

/* from pg_float_io.c */
extern _Thread_local int pg_diff_errcode;
extern char *float8out_internal(double num);

#define pfree free

/* ---- error shims (see header comment) ---- */

/*
 * THREAD-LOCAL (2026-07-30): cargo test runs the differential smoke tests
 * in parallel threads; a shared jmp_buf raced between geo tests (longjmp
 * into a half-written buffer => SIGSEGV) and the shared pg_diff_errcode
 * raced across oracles. Thread-local state removes both; single-threaded
 * libFuzzer behavior is unchanged.
 */
static _Thread_local jmp_buf pg_geo_jmp;

static void
float_overflow_error(void)
{
	pg_diff_errcode = 2;		/* ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE */
	longjmp(pg_geo_jmp, 1);
}

static void
float_underflow_error(void)
{
	pg_diff_errcode = 2;		/* ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE */
	longjmp(pg_geo_jmp, 1);
}

static void
float_zero_divide_error(void)
{
	pg_diff_errcode = 4;		/* ERRCODE_DIVISION_BY_ZERO (22012) */
	longjmp(pg_geo_jmp, 1);
}

/* ---- src/include/utils/float.h — VERBATIM ---- */

static inline float8
get_float8_infinity(void)
{
#ifdef INFINITY
	return (float8) INFINITY;
#else
	return (float8) (HUGE_VAL * HUGE_VAL);
#endif
}

static inline float8
get_float8_nan(void)
{
#if defined(NAN) && !(defined(__NetBSD__) && defined(__mips__))
	return (float8) NAN;
#else
	return (float8) (0.0 / 0.0);
#endif
}

static inline float8
float8_pl(const float8 val1, const float8 val2)
{
	float8		result;

	result = val1 + val2;
	if (unlikely(isinf(result)) && !isinf(val1) && !isinf(val2))
		float_overflow_error();

	return result;
}

static inline float8
float8_mi(const float8 val1, const float8 val2)
{
	float8		result;

	result = val1 - val2;
	if (unlikely(isinf(result)) && !isinf(val1) && !isinf(val2))
		float_overflow_error();

	return result;
}

static inline float8
float8_mul(const float8 val1, const float8 val2)
{
	float8		result;

	result = val1 * val2;
	if (unlikely(isinf(result)) && !isinf(val1) && !isinf(val2))
		float_overflow_error();
	if (unlikely(result == 0.0) && val1 != 0.0 && val2 != 0.0)
		float_underflow_error();

	return result;
}

/* ---- src/include/utils/geo_decls.h — VERBATIM ---- */

#define EPSILON					1.0E-06

#define FPzero(A)				(fabs(A) <= EPSILON)

static inline bool
FPeq(double A, double B)
{
	return A == B || fabs(A - B) <= EPSILON;
}

static inline bool
FPlt(double A, double B)
{
	return A + EPSILON < B;
}

static inline bool
FPle(double A, double B)
{
	return A <= B + EPSILON;
}

static inline bool
FPgt(double A, double B)
{
	return A > B + EPSILON;
}

static inline bool
FPge(double A, double B)
{
	return A + EPSILON >= B;
}

typedef struct
{
	float8		x,
				y;
} Point;

#define HYPOT(A, B)				pg_hypot(A, B)

/* ---- src/backend/utils/adt/geo_ops.c: pg_hypot — VERBATIM ---- */

static float8
pg_hypot(float8 x, float8 y)
{
	float8		yx,
				result;

	/* Handle INF and NaN properly */
	if (isinf(x) || isinf(y))
		return get_float8_infinity();

	if (isnan(x) || isnan(y))
		return get_float8_nan();

	/* Else, drop any minus signs */
	x = fabs(x);
	y = fabs(y);

	/* Swap x and y if needed to make x the larger one */
	if (x < y)
	{
		float8		temp = x;

		x = y;
		y = temp;
	}

	/*
	 * If y is zero, the hypotenuse is x.  This test saves a few cycles in
	 * such cases, but more importantly it also protects against
	 * divide-by-zero errors, since now x >= y.
	 */
	if (y == 0.0)
		return x;

	/* Determine the hypotenuse */
	yx = y / x;
	result = x * sqrt(1.0 + (yx * yx));

	if (unlikely(isinf(result)))
		float_overflow_error();
	if (unlikely(result == 0.0))
		float_underflow_error();

	return result;
}

/* ---- geo_ops.c: point_dt — VERBATIM ---- */

static float8
point_dt(Point *pt1, Point *pt2)
{
	return HYPOT(float8_mi(pt1->x, pt2->x), float8_mi(pt1->y, pt2->y));
}

/* ---- StringInfo shim (see header comment) ---- */

typedef struct
{
	char	   *data;
	int			len;
	int			maxlen;
}			StringInfoData;
typedef StringInfoData *StringInfo;

/* thread-local (parallel stable tests) + enlarged for multi-point path/
 * poly images (driver input cap 1024 chars bounds npts<=512; worst image
 * ~512*54B < 28KiB) */
static _Thread_local char pg_geo_strbuf[32768];

static void
initStringInfo(StringInfo str)
{
	str->data = pg_geo_strbuf;
	str->maxlen = (int) sizeof(pg_geo_strbuf);
	str->len = 0;
	str->data[0] = '\0';
}

static void
appendStringInfoChar(StringInfo str, char c)
{
	if (str->len + 1 >= str->maxlen)
		abort();
	str->data[str->len++] = c;
	str->data[str->len] = '\0';
}

static void
appendStringInfoString(StringInfo str, const char *s2)
{
	size_t		n = strlen(s2);

	if (str->len + (int) n >= str->maxlen)
		abort();
	memcpy(str->data + str->len, s2, n + 1);
	str->len += (int) n;
}

static void
appendStringInfo(StringInfo str, const char *fmt, const char *a, const char *b)
{
	int			n = snprintf(str->data + str->len, str->maxlen - str->len,
							 fmt, a, b);

	if (n < 0 || n >= str->maxlen - str->len)
		abort();
	str->len += n;
}

/* ---- geo_ops.c: pair_encode / path_encode — VERBATIM ---- */

#define LDELIM			'('
#define RDELIM			')'
#define DELIM			','
#define LDELIM_EP		'['
#define RDELIM_EP		']'

enum path_delim
{
	PATH_NONE, PATH_OPEN, PATH_CLOSED,
};

static void
pair_encode(float8 x, float8 y, StringInfo str)
{
	char	   *xstr = float8out_internal(x);
	char	   *ystr = float8out_internal(y);

	appendStringInfo(str, "%s,%s", xstr, ystr);
	pfree(xstr);
	pfree(ystr);
}

static char *
path_encode(enum path_delim path_delim, int npts, Point *pt)
{
	StringInfoData str;
	int			i;

	initStringInfo(&str);

	switch (path_delim)
	{
		case PATH_CLOSED:
			appendStringInfoChar(&str, LDELIM);
			break;
		case PATH_OPEN:
			appendStringInfoChar(&str, LDELIM_EP);
			break;
		case PATH_NONE:
			break;
	}

	for (i = 0; i < npts; i++)
	{
		if (i > 0)
			appendStringInfoChar(&str, DELIM);
		appendStringInfoChar(&str, LDELIM);
		pair_encode(pt->x, pt->y, &str);
		appendStringInfoChar(&str, RDELIM);
		pt++;
	}

	switch (path_delim)
	{
		case PATH_CLOSED:
			appendStringInfoChar(&str, RDELIM);
			break;
		case PATH_OPEN:
			appendStringInfoChar(&str, RDELIM_EP);
			break;
		case PATH_NONE:
			break;
	}

	return str.data;
}

/* ---- geo_ops.c: lseg_crossing / point_inside — VERBATIM ---- */

#define POINT_ON_POLYGON INT_MAX

static int
lseg_crossing(float8 x, float8 y, float8 prev_x, float8 prev_y)
{
	float8		z;
	int			y_sign;

	if (FPzero(y))
	{							/* y == 0, on X axis */
		if (FPzero(x))			/* (x,y) is (0,0)? */
			return POINT_ON_POLYGON;
		else if (FPgt(x, 0))
		{						/* x > 0 */
			if (FPzero(prev_y)) /* y and prev_y are zero */
				/* prev_x > 0? */
				return FPgt(prev_x, 0.0) ? 0 : POINT_ON_POLYGON;
			return FPlt(prev_y, 0.0) ? 1 : -1;
		}
		else
		{						/* x < 0, x not on positive X axis */
			if (FPzero(prev_y))
				/* prev_x < 0? */
				return FPlt(prev_x, 0.0) ? 0 : POINT_ON_POLYGON;
			return 0;
		}
	}
	else
	{							/* y != 0 */
		/* compute y crossing direction from previous point */
		y_sign = FPgt(y, 0.0) ? 1 : -1;

		if (FPzero(prev_y))
			/* previous point was on X axis, so new point is either off or on */
			return FPlt(prev_x, 0.0) ? 0 : y_sign;
		else if ((y_sign < 0 && FPlt(prev_y, 0.0)) ||
				 (y_sign > 0 && FPgt(prev_y, 0.0)))
			/* both above or below X axis */
			return 0;			/* same sign */
		else
		{						/* y and prev_y cross X-axis */
			if (FPge(x, 0.0) && FPgt(prev_x, 0.0))
				/* both non-negative so cross positive X-axis */
				return 2 * y_sign;
			if (FPlt(x, 0.0) && FPle(prev_x, 0.0))
				/* both non-positive so do not cross positive X-axis */
				return 0;

			/* x and y cross axes, see URL above point_inside() */
			z = float8_mi(float8_mul(float8_mi(x, prev_x), y),
						  float8_mul(float8_mi(y, prev_y), x));
			if (FPzero(z))
				return POINT_ON_POLYGON;
			if ((y_sign < 0 && FPlt(z, 0.0)) ||
				(y_sign > 0 && FPgt(z, 0.0)))
				return 0;
			return 2 * y_sign;
		}
	}
}

static int
point_inside(Point *p, int npts, Point *plist)
{
	float8		x0,
				y0;
	float8		prev_x,
				prev_y;
	int			i = 0;
	float8		x,
				y;
	int			cross,
				total_cross = 0;

	Assert(npts > 0);

	/* compute first polygon point relative to single point */
	x0 = float8_mi(plist[0].x, p->x);
	y0 = float8_mi(plist[0].y, p->y);

	prev_x = x0;
	prev_y = y0;
	/* loop over polygon points and aggregate total_cross */
	for (i = 1; i < npts; i++)
	{
		/* compute next polygon point relative to single point */
		x = float8_mi(plist[i].x, p->x);
		y = float8_mi(plist[i].y, p->y);

		/* compute previous to current point crossing */
		if ((cross = lseg_crossing(x, y, prev_x, prev_y)) == POINT_ON_POLYGON)
			return 2;
		total_cross += cross;

		prev_x = x;
		prev_y = y;
	}

	/* now do the first point */
	if ((cross = lseg_crossing(x0, y0, prev_x, prev_y)) == POINT_ON_POLYGON)
		return 2;
	total_cross += cross;

	if (total_cross != 0)
		return 1;
	return 0;
}

/* ---- fuzz-facing entry points (drivers, NOT Postgres code) ---- */

/*
 * point_out image: writes the exact cstring into buf, returns length,
 * or -1 with pg_diff_errcode set if the (unreachable for finite doubles)
 * error shim fired.
 */
int
pg_diff_point_out(double x, double y, char *buf, int buflen)
{
	Point		pt;
	char	   *s;
	size_t		n;

	pg_diff_errcode = 0;
	if (setjmp(pg_geo_jmp) != 0)
		return -1;
	pt.x = x;
	pt.y = y;
	s = path_encode(PATH_NONE, 1, &pt);
	n = strlen(s);
	if ((int) n + 1 > buflen)
		abort();
	memcpy(buf, s, n + 1);
	return (int) n;
}

/*
 * on_ppath — the PG_FUNCTION body VERBATIM modulo PG_GETARG/PG_RETURN
 * unwrapping. Returns 0/1 (bool result) or -1 with pg_diff_errcode set
 * when the float8_pl/point_dt overflow shim fired.
 */
int
pg_diff_on_ppath(double px, double py, int closed, int npts, const double *xys)
{
	Point		ptd;
	Point	   *volatile pts;	/* volatile: live across setjmp */
	int			i,
				n;
	float8		a,
				b;
	int			ret;

	pg_diff_errcode = 0;
	ptd.x = px;
	ptd.y = py;
	pts = (Point *) malloc(sizeof(Point) * npts);
	for (i = 0; i < npts; i++)
	{
		pts[i].x = xys[2 * i];
		pts[i].y = xys[2 * i + 1];
	}

	if (setjmp(pg_geo_jmp) != 0)
	{
		free(pts);
		return -1;
	}

	/*-- OPEN --*/
	if (!closed)
	{
		n = npts - 1;
		a = point_dt(&ptd, &pts[0]);
		for (i = 0; i < n; i++)
		{
			b = point_dt(&ptd, &pts[i + 1]);
			if (FPeq(float8_pl(a, b), point_dt(&pts[i], &pts[i + 1])))
			{
				free(pts);
				return 1;
			}
			a = b;
		}
		free(pts);
		return 0;
	}

	/*-- CLOSED --*/
	ret = point_inside(&ptd, npts, pts) != 0;
	free(pts);
	return ret;
}

/* ======================================================================== */
/* ==== SECTION: geo text-I/O family extension (p1-laner, 2026-07-31) ==== */
/* ======================================================================== */
/*
 * Adds the full geo text-I/O oracle surface: point/box/lseg/line/path/
 * poly/circle *_in and *_out. Provenance identical to the file header
 * (geo_ops.c + geo_decls.h @ 62d6c7d3df, bodies VERBATIM). Additional
 * shims, plumbing only:
 *   - ereturn/errcode/errmsg -> record class in pg_diff_errcode, return
 *     (Datum) 0 (the pg_float_io.c convention); escontext threaded through
 *     verbatim but never a real node (hard shape both sides).
 *   - SOFT_ERROR_OCCURRED(escontext) -> (pg_diff_errcode != 0): with the
 *     ereturn shim above, "an error was recorded" IS the soft-error state
 *     the decode cascade tests for; exact per-call reset in every entry.
 *   - errcode classes: 1 = 22P02 invalid-text, 2 = 22003 out-of-range
 *     (file convention), 3 = 54000 ERRCODE_PROGRAM_LIMIT_EXCEEDED.
 *   - fmgr unwrap: PG_GETARG_CSTRING(0) -> a char *str parameter;
 *     PG_RETURN_*_P(x) -> return PointerGetDatum(x); PG_RETURN_NULL() ->
 *     return (Datum) 0. Bodies otherwise verbatim.
 *   - palloc/palloc0 -> TLS pointer arena (reset at each pg_diff_geo_*
 *     entry; models the per-query context reset so ereturn exits cannot
 *     leak — the 2026-07-31 LSan incident class). pfree -> arena-aware
 *     free with plain-free fallback (float8out_internal's buffers come
 *     malloc'd from the pg_float_io.c TU).
 *   - psprintf (line_out) -> vsnprintf into an arena buffer.
 *   - float8in_internal: extern from pg_float_io.c (verbatim there; the
 *     parse core is the platform strtod exactly as in real PostgreSQL).
 *   - StringInfo: the file's fixed-capacity shim, enlarged + thread-local;
 *     appendStringInfoString added for single_encode.
 */

#include <ctype.h>
#include <stdarg.h>
#include <stddef.h>

typedef struct Node Node;
typedef uintptr_t Datum;

extern float8 float8in_internal(char *num, char **endptr_p,
								const char *type_name, const char *orig_string,
								struct Node *escontext);

#define PointerGetDatum(p) ((Datum) (p))
#define PG_RETURN_NULL() return (Datum) 0

#define ERRCODE_INVALID_TEXT_REPRESENTATION 1
#define ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE 2
#define ERRCODE_PROGRAM_LIMIT_EXCEEDED 3
#define errcode(c) (pg_diff_errcode = (c))
#define errmsg(...) 0
#define ereturn(escontext, ret, stuff) do { (void) (stuff); return (ret); } while (0)
#define SOFT_ERROR_OCCURRED(escontext) (pg_diff_errcode != 0)

/* ---- TLS palloc arena (see section header) ---- */

#define PG_GEO_ARENA_MAX 64
static _Thread_local void *pg_geo_arena[PG_GEO_ARENA_MAX];
static _Thread_local int pg_geo_arena_n;

static void
pg_geo_arena_reset(void)
{
	int			i;

	for (i = 0; i < pg_geo_arena_n; i++)
		free(pg_geo_arena[i]);
	pg_geo_arena_n = 0;
}

static void *
pg_geo_palloc_impl(size_t n)
{
	void	   *p = malloc(n);

	assert(pg_geo_arena_n < PG_GEO_ARENA_MAX);
	pg_geo_arena[pg_geo_arena_n++] = p;
	return p;
}

static void *
pg_geo_palloc0_impl(size_t n)
{
	void	   *p = calloc(1, n);

	assert(pg_geo_arena_n < PG_GEO_ARENA_MAX);
	pg_geo_arena[pg_geo_arena_n++] = p;
	return p;
}

/* arena-aware pfree; falls back to plain free for cross-TU mallocs
 * (float8out_internal results) */
static void
pg_geo_pfree_impl(void *p)
{
	int			i;

	for (i = 0; i < pg_geo_arena_n; i++)
	{
		if (pg_geo_arena[i] == p)
		{
			free(p);
			pg_geo_arena[i] = pg_geo_arena[--pg_geo_arena_n];
			return;
		}
	}
	free(p);
}

#define palloc(n) pg_geo_palloc_impl(n)
#define palloc0(n) pg_geo_palloc0_impl(n)
#undef pfree
#define pfree(p) pg_geo_pfree_impl(p)

static char *
psprintf(const char *fmt, ...)
{
	char	   *buf = pg_geo_palloc_impl(256);
	va_list		ap;
	int			n;

	va_start(ap, fmt);
	n = vsnprintf(buf, 256, fmt, ap);
	va_end(ap);
	if (n < 0 || n >= 256)
		abort();
	return buf;
}

/* ---- src/include/utils/float.h — VERBATIM (I/O-family additions) ---- */

static inline float8
float8_div(const float8 val1, const float8 val2)
{
	float8		result;

	if (unlikely(val2 == 0.0) && !isnan(val1))
		float_zero_divide_error();
	result = val1 / val2;
	if (unlikely(isinf(result)) && !isinf(val1))
		float_overflow_error();
	if (unlikely(result == 0.0) && val1 != 0.0 && !isinf(val2))
		float_underflow_error();

	return result;
}

/*
 * Routines for NaN-aware comparisons
 *
 * We consider all NaNs to be equal and larger than any non-NaN. This is
 * somewhat arbitrary; the important thing is to have a consistent sort
 * order.
 */

static inline bool
float8_eq(const float8 val1, const float8 val2)
{
	return isnan(val1) ? isnan(val2) : !isnan(val2) && val1 == val2;
}

static inline bool
float8_lt(const float8 val1, const float8 val2)
{
	return !isnan(val1) && (isnan(val2) || val1 < val2);
}

static inline bool
float8_gt(const float8 val1, const float8 val2)
{
	return !isnan(val2) && (isnan(val1) || val1 > val2);
}

/* ---- src/include/utils/geo_decls.h — VERBATIM (remaining types) ---- */

typedef struct
{
	Point		p[2];
} LSEG;

typedef struct
{
	Point		high,
				low;			/* corner POINTs */
} BOX;

typedef struct
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	int32		npts;
	int32		closed;			/* is this a closed polygon? */
	int32		dummy;			/* padding to make it double align */
	Point		p[];			/* variable length array of POINTs */
} PATH;

typedef struct
{
	float8		A,
				B,
				C;
} LINE;

typedef struct
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	int32		npts;
	BOX			boundbox;
	Point		p[];			/* variable length array of POINTs */
} POLYGON;

typedef struct
{
	Point		center;
	float8		radius;
} CIRCLE;

/* SET_VARSIZE: header bytes are irrelevant to the value comparison and the
 * struct here is a plain arena allocation — no-op shim. */
#define SET_VARSIZE(ptr, len) ((void) 0)

#define LDELIM_C		'<'
#define RDELIM_C		'>'
#define LDELIM_L		'{'
#define RDELIM_L		'}'

/* ---- geo_ops.c: decode helpers — VERBATIM ---- */

static bool
single_decode(char *num, float8 *x, char **endptr_p,
			  const char *type_name, const char *orig_string,
			  Node *escontext)
{
	*x = float8in_internal(num, endptr_p, type_name, orig_string, escontext);
	return (!SOFT_ERROR_OCCURRED(escontext));
}								/* single_decode() */

static void
single_encode(float8 x, StringInfo str)
{
	char	   *xstr = float8out_internal(x);

	appendStringInfoString(str, xstr);
	pfree(xstr);
}								/* single_encode() */

static bool
pair_decode(char *str, float8 *x, float8 *y, char **endptr_p,
			const char *type_name, const char *orig_string,
			Node *escontext)
{
	bool		has_delim;

	while (isspace((unsigned char) *str))
		str++;
	if ((has_delim = (*str == LDELIM)))
		str++;

	if (!single_decode(str, x, &str, type_name, orig_string, escontext))
		return false;

	if (*str++ != DELIM)
		goto fail;

	if (!single_decode(str, y, &str, type_name, orig_string, escontext))
		return false;

	if (has_delim)
	{
		if (*str++ != RDELIM)
			goto fail;
		while (isspace((unsigned char) *str))
			str++;
	}

	/* report stopping point if wanted, else complain if not end of string */
	if (endptr_p)
		*endptr_p = str;
	else if (*str != '\0')
		goto fail;
	return true;

fail:
	ereturn(escontext, false,
			(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
			 errmsg("invalid input syntax for type %s: \"%s\"",
					type_name, orig_string)));
}

static bool
path_decode(char *str, bool opentype, int npts, Point *p,
			bool *isopen, char **endptr_p,
			const char *type_name, const char *orig_string,
			Node *escontext)
{
	int			depth = 0;
	char	   *cp;
	int			i;

	while (isspace((unsigned char) *str))
		str++;
	if ((*isopen = (*str == LDELIM_EP)))
	{
		/* no open delimiter allowed? */
		if (!opentype)
			goto fail;
		depth++;
		str++;
	}
	else if (*str == LDELIM)
	{
		cp = (str + 1);
		while (isspace((unsigned char) *cp))
			cp++;
		if (*cp == LDELIM)
		{
			depth++;
			str = cp;
		}
		else if (strrchr(str, LDELIM) == str)
		{
			depth++;
			str = cp;
		}
	}

	for (i = 0; i < npts; i++)
	{
		if (!pair_decode(str, &(p->x), &(p->y), &str, type_name, orig_string,
						 escontext))
			return false;
		if (*str == DELIM)
			str++;
		p++;
	}

	while (depth > 0)
	{
		if (*str == RDELIM || (*str == RDELIM_EP && *isopen && depth == 1))
		{
			depth--;
			str++;
			while (isspace((unsigned char) *str))
				str++;
		}
		else
			goto fail;
	}

	/* report stopping point if wanted, else complain if not end of string */
	if (endptr_p)
		*endptr_p = str;
	else if (*str != '\0')
		goto fail;
	return true;

fail:
	ereturn(escontext, false,
			(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
			 errmsg("invalid input syntax for type %s: \"%s\"",
					type_name, orig_string)));
}								/* path_decode() */

/*-------------------------------------------------------------
 * pair_count - count the number of points
 * allow the following notation:
 * '((1,2),(3,4))'
 * '(1,3,2,4)'
 * require an odd number of delim characters in the string
 *-------------------------------------------------------------*/
static int
pair_count(char *s, char delim)
{
	int			ndelim = 0;

	while ((s = strchr(s, delim)) != NULL)
	{
		ndelim++;
		s++;
	}
	return (ndelim % 2) ? ((ndelim + 1) / 2) : -1;
}

/* ---- geo_ops.c: point/lseg/line slope + construct helpers — VERBATIM ---- */

static inline bool
point_eq_point(Point *pt1, Point *pt2)
{
	/* If any NaNs are involved, insist on exact equality */
	if (unlikely(isnan(pt1->x) || isnan(pt1->y) ||
				 isnan(pt2->x) || isnan(pt2->y)))
		return (float8_eq(pt1->x, pt2->x) && float8_eq(pt1->y, pt2->y));

	return (FPeq(pt1->x, pt2->x) && FPeq(pt1->y, pt2->y));
}

static inline float8
point_sl(Point *pt1, Point *pt2)
{
	if (FPeq(pt1->x, pt2->x))
		return get_float8_infinity();
	if (FPeq(pt1->y, pt2->y))
		return 0.0;
	return float8_div(float8_mi(pt1->y, pt2->y), float8_mi(pt1->x, pt2->x));
}

static inline float8
lseg_sl(LSEG *lseg)
{
	return point_sl(&lseg->p[0], &lseg->p[1]);
}

/*
 * Fill already-allocated LINE struct from the point and the slope
 */
static inline void
line_construct(LINE *result, Point *pt, float8 m)
{
	if (isinf(m))
	{
		/* vertical - use "x = C" */
		result->A = -1.0;
		result->B = 0.0;
		result->C = pt->x;
	}
	else if (m == 0)
	{
		/* horizontal - use "y = C" */
		result->A = 0.0;
		result->B = -1.0;
		result->C = pt->y;
	}
	else
	{
		/* use "mx - y + yinter = 0" */
		result->A = m;
		result->B = -1.0;
		result->C = float8_mi(pt->y, float8_mul(m, pt->x));
		/* on some platforms, the preceding expression tends to produce -0 */
		if (result->C == 0.0)
			result->C = 0.0;
	}
}

/* ---- geo_ops.c: *_in / *_out bodies — VERBATIM modulo fmgr unwrap ---- */
/* (PG_GETARG_CSTRING(0) -> str parameter; fcinfo->context -> escontext
 * parameter, always NULL here; PG_RETURN_*_P -> PointerGetDatum return) */

static Datum
pg_geo_point_in(char *str, Node *escontext)
{
	Point	   *point = (Point *) palloc(sizeof(Point));

	/* Ignore failure from pair_decode, since our return value won't matter */
	pair_decode(str, &point->x, &point->y, NULL, "point", str, escontext);
	return PointerGetDatum(point);
}

static Datum
pg_geo_box_in(char *str, Node *escontext)
{
	BOX		   *box = (BOX *) palloc(sizeof(BOX));
	bool		isopen;
	float8		x,
				y;

	if (!path_decode(str, false, 2, &(box->high), &isopen, NULL, "box", str,
					 escontext))
		PG_RETURN_NULL();

	/* reorder corners if necessary... */
	if (float8_lt(box->high.x, box->low.x))
	{
		x = box->high.x;
		box->high.x = box->low.x;
		box->low.x = x;
	}
	if (float8_lt(box->high.y, box->low.y))
	{
		y = box->high.y;
		box->high.y = box->low.y;
		box->low.y = y;
	}

	return PointerGetDatum(box);
}

static Datum
pg_geo_lseg_in(char *str, Node *escontext)
{
	LSEG	   *lseg = (LSEG *) palloc(sizeof(LSEG));
	bool		isopen;

	if (!path_decode(str, true, 2, &lseg->p[0], &isopen, NULL, "lseg", str,
					 escontext))
		PG_RETURN_NULL();

	return PointerGetDatum(lseg);
}

static bool
line_decode(char *s, const char *str, LINE *line, Node *escontext)
{
	/* s was already advanced over leading '{' */
	if (!single_decode(s, &line->A, &s, "line", str, escontext))
		return false;
	if (*s++ != DELIM)
		goto fail;
	if (!single_decode(s, &line->B, &s, "line", str, escontext))
		return false;
	if (*s++ != DELIM)
		goto fail;
	if (!single_decode(s, &line->C, &s, "line", str, escontext))
		return false;
	if (*s++ != RDELIM_L)
		goto fail;
	while (isspace((unsigned char) *s))
		s++;
	if (*s != '\0')
		goto fail;
	return true;

fail:
	ereturn(escontext, false,
			(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
			 errmsg("invalid input syntax for type %s: \"%s\"",
					"line", str)));
}

static Datum
pg_geo_line_in(char *str, Node *escontext)
{
	LINE	   *line = (LINE *) palloc(sizeof(LINE));
	LSEG		lseg;
	bool		isopen;
	char	   *s;

	s = str;
	while (isspace((unsigned char) *s))
		s++;
	if (*s == LDELIM_L)
	{
		if (!line_decode(s + 1, str, line, escontext))
			PG_RETURN_NULL();
		if (FPzero(line->A) && FPzero(line->B))
			ereturn(escontext, (Datum) 0,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("invalid line specification: A and B cannot both be zero")));
	}
	else
	{
		if (!path_decode(s, true, 2, &lseg.p[0], &isopen, NULL, "line", str,
						 escontext))
			PG_RETURN_NULL();
		if (point_eq_point(&lseg.p[0], &lseg.p[1]))
			ereturn(escontext, (Datum) 0,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("invalid line specification: must be two distinct points")));

		/*
		 * XXX lseg_sl() and line_construct() can throw overflow/underflow
		 * errors.  Eventually we should allow those to be soft, but the
		 * notational pain seems to outweigh the value for now.
		 */
		line_construct(line, &lseg.p[0], lseg_sl(&lseg));
	}

	return PointerGetDatum(line);
}

static Datum
pg_geo_path_in(char *str, Node *escontext)
{
	PATH	   *path;
	bool		isopen;
	char	   *s;
	int			npts;
	int			size;
	int			base_size;
	int			depth = 0;

	if ((npts = pair_count(str, ',')) <= 0)
		ereturn(escontext, (Datum) 0,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for type %s: \"%s\"",
						"path", str)));

	s = str;
	while (isspace((unsigned char) *s))
		s++;

	/* skip single leading paren */
	if ((*s == LDELIM) && (strrchr(s, LDELIM) == s))
	{
		s++;
		depth++;
	}

	base_size = sizeof(path->p[0]) * npts;
	size = offsetof(PATH, p) + base_size;

	/* Check for integer overflow */
	if (base_size / npts != sizeof(path->p[0]) || size <= base_size)
		ereturn(escontext, (Datum) 0,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("too many points requested")));

	path = (PATH *) palloc(size);

	SET_VARSIZE(path, size);
	path->npts = npts;

	if (!path_decode(s, true, npts, &(path->p[0]), &isopen, &s, "path", str,
					 escontext))
		PG_RETURN_NULL();

	if (depth >= 1)
	{
		if (*s++ != RDELIM)
			ereturn(escontext, (Datum) 0,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("invalid input syntax for type %s: \"%s\"",
							"path", str)));
		while (isspace((unsigned char) *s))
			s++;
	}
	if (*s != '\0')
		ereturn(escontext, (Datum) 0,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for type %s: \"%s\"",
						"path", str)));

	path->closed = (!isopen);
	/* prevent instability in unused pad bytes */
	path->dummy = 0;

	return PointerGetDatum(path);
}

/*---------------------------------------------------------------------
 * Make the smallest bounding box for the given polygon.
 *---------------------------------------------------------------------*/
static void
make_bound_box(POLYGON *poly)
{
	int			i;
	float8		x1,
				y1,
				x2,
				y2;

	assert(poly->npts > 0);		/* Assert -> assert (shim) */

	x1 = x2 = poly->p[0].x;
	y2 = y1 = poly->p[0].y;
	for (i = 1; i < poly->npts; i++)
	{
		if (float8_lt(poly->p[i].x, x1))
			x1 = poly->p[i].x;
		if (float8_gt(poly->p[i].x, x2))
			x2 = poly->p[i].x;
		if (float8_lt(poly->p[i].y, y1))
			y1 = poly->p[i].y;
		if (float8_gt(poly->p[i].y, y2))
			y2 = poly->p[i].y;
	}

	poly->boundbox.low.x = x1;
	poly->boundbox.high.x = x2;
	poly->boundbox.low.y = y1;
	poly->boundbox.high.y = y2;
}

static Datum
pg_geo_poly_in(char *str, Node *escontext)
{
	POLYGON    *poly;
	int			npts;
	int			size;
	int			base_size;
	bool		isopen;

	if ((npts = pair_count(str, ',')) <= 0)
		ereturn(escontext, (Datum) 0,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for type %s: \"%s\"",
						"polygon", str)));

	base_size = sizeof(poly->p[0]) * npts;
	size = offsetof(POLYGON, p) + base_size;

	/* Check for integer overflow */
	if (base_size / npts != sizeof(poly->p[0]) || size <= base_size)
		ereturn(escontext, (Datum) 0,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("too many points requested")));

	poly = (POLYGON *) palloc0(size);	/* zero any holes */

	SET_VARSIZE(poly, size);
	poly->npts = npts;

	if (!path_decode(str, false, npts, &(poly->p[0]), &isopen, NULL, "polygon",
					 str, escontext))
		PG_RETURN_NULL();

	make_bound_box(poly);

	return PointerGetDatum(poly);
}

static Datum
pg_geo_circle_in(char *str, Node *escontext)
{
	CIRCLE	   *circle = (CIRCLE *) palloc(sizeof(CIRCLE));
	char	   *s,
			   *cp;
	int			depth = 0;

	s = str;
	while (isspace((unsigned char) *s))
		s++;
	if (*s == LDELIM_C)
		depth++, s++;
	else if (*s == LDELIM)
	{
		/* If there are two left parens, consume the first one */
		cp = (s + 1);
		while (isspace((unsigned char) *cp))
			cp++;
		if (*cp == LDELIM)
			depth++, s = cp;
	}

	/* pair_decode will consume parens around the pair, if any */
	if (!pair_decode(s, &circle->center.x, &circle->center.y, &s, "circle", str,
					 escontext))
		PG_RETURN_NULL();

	if (*s == DELIM)
		s++;

	if (!single_decode(s, &circle->radius, &s, "circle", str, escontext))
		PG_RETURN_NULL();

	/* We have to accept NaN. */
	if (circle->radius < 0.0)
		ereturn(escontext, (Datum) 0,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for type %s: \"%s\"",
						"circle", str)));

	while (depth > 0)
	{
		if ((*s == RDELIM) || ((*s == RDELIM_C) && (depth == 1)))
		{
			depth--;
			s++;
			while (isspace((unsigned char) *s))
				s++;
		}
		else
			ereturn(escontext, (Datum) 0,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("invalid input syntax for type %s: \"%s\"",
							"circle", str)));
	}

	if (*s != '\0')
		ereturn(escontext, (Datum) 0,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for type %s: \"%s\"",
						"circle", str)));

	return PointerGetDatum(circle);
}

/* out bodies: box_out/lseg_out/path_out/poly_out are single path_encode
 * calls (vendored above); line_out and circle_out below, VERBATIM. */

static char *
pg_geo_line_out(LINE *line)
{
	char	   *astr = float8out_internal(line->A);
	char	   *bstr = float8out_internal(line->B);
	char	   *cstr = float8out_internal(line->C);

	char	   *r = psprintf("%c%s%c%s%c%s%c", LDELIM_L, astr, DELIM, bstr,
							 DELIM, cstr, RDELIM_L);

	/* shim: real PG leaves these to the context reset; the fuzz oracle's
	 * float8out_internal buffers are cross-TU mallocs, so free explicitly */
	pfree(astr);
	pfree(bstr);
	pfree(cstr);
	return r;
}

static char *
pg_geo_circle_out(CIRCLE *circle)
{
	StringInfoData str;

	initStringInfo(&str);

	appendStringInfoChar(&str, LDELIM_C);
	appendStringInfoChar(&str, LDELIM);
	pair_encode(circle->center.x, circle->center.y, &str);
	appendStringInfoChar(&str, RDELIM);
	appendStringInfoChar(&str, DELIM);
	single_encode(circle->radius, &str);
	appendStringInfoChar(&str, RDELIM_C);

	return str.data;
}

/* ---- fuzz-facing entry points for the I/O family (NOT Postgres code) ---- */
/*
 * Convention: reset arena + errcode, setjmp the float-error trampoline,
 * call the verbatim body, return the errcode class (0 = ok) and write
 * results through caller buffers. -1 = float error longjmp (the caller
 * compares it as its own verdict class).
 */

int
pg_diff_geo_point_in(const char *str, double *out)
{
	Datum		d;

	pg_geo_arena_reset();
	pg_diff_errcode = 0;
	if (setjmp(pg_geo_jmp) != 0)
		return -1;
	d = pg_geo_point_in((char *) str, NULL);
	if (pg_diff_errcode != 0)
		return pg_diff_errcode;
	out[0] = ((Point *) d)->x;
	out[1] = ((Point *) d)->y;
	return 0;
}

int
pg_diff_geo_box_in(const char *str, double *out)
{
	Datum		d;

	pg_geo_arena_reset();
	pg_diff_errcode = 0;
	if (setjmp(pg_geo_jmp) != 0)
		return -1;
	d = pg_geo_box_in((char *) str, NULL);
	if (pg_diff_errcode != 0 || d == 0)
		return pg_diff_errcode;
	memcpy(out, (BOX *) d, 4 * sizeof(double));
	return 0;
}

int
pg_diff_geo_lseg_in(const char *str, double *out)
{
	Datum		d;

	pg_geo_arena_reset();
	pg_diff_errcode = 0;
	if (setjmp(pg_geo_jmp) != 0)
		return -1;
	d = pg_geo_lseg_in((char *) str, NULL);
	if (pg_diff_errcode != 0 || d == 0)
		return pg_diff_errcode;
	memcpy(out, (LSEG *) d, 4 * sizeof(double));
	return 0;
}

int
pg_diff_geo_line_in(const char *str, double *out)
{
	Datum		d;

	pg_geo_arena_reset();
	pg_diff_errcode = 0;
	if (setjmp(pg_geo_jmp) != 0)
		return -1;
	d = pg_geo_line_in((char *) str, NULL);
	if (pg_diff_errcode != 0 || d == 0)
		return pg_diff_errcode;
	memcpy(out, (LINE *) d, 3 * sizeof(double));
	return 0;
}

int
pg_diff_geo_circle_in(const char *str, double *out)
{
	Datum		d;

	pg_geo_arena_reset();
	pg_diff_errcode = 0;
	if (setjmp(pg_geo_jmp) != 0)
		return -1;
	d = pg_geo_circle_in((char *) str, NULL);
	if (pg_diff_errcode != 0 || d == 0)
		return pg_diff_errcode;
	out[0] = ((CIRCLE *) d)->center.x;
	out[1] = ((CIRCLE *) d)->center.y;
	out[2] = ((CIRCLE *) d)->radius;
	return 0;
}

/*
 * path/poly: maxpts guards the caller buffer; the driver's input-length cap
 * makes an overrun impossible (npts <= strlen/2 < maxpts), abort if not.
 */
int
pg_diff_geo_path_in(const char *str, int32_t *npts, int32_t *closed, double *xys, int maxpts)
{
	Datum		d;
	PATH	   *path;

	pg_geo_arena_reset();
	pg_diff_errcode = 0;
	if (setjmp(pg_geo_jmp) != 0)
		return -1;
	d = pg_geo_path_in((char *) str, NULL);
	if (pg_diff_errcode != 0 || d == 0)
		return pg_diff_errcode;
	path = (PATH *) d;
	if (path->npts > maxpts)
		abort();
	*npts = path->npts;
	*closed = path->closed;
	memcpy(xys, path->p, sizeof(Point) * path->npts);
	return 0;
}

int
pg_diff_geo_poly_in(const char *str, int32_t *npts, double *bound, double *xys, int maxpts)
{
	Datum		d;
	POLYGON    *poly;

	pg_geo_arena_reset();
	pg_diff_errcode = 0;
	if (setjmp(pg_geo_jmp) != 0)
		return -1;
	d = pg_geo_poly_in((char *) str, NULL);
	if (pg_diff_errcode != 0 || d == 0)
		return pg_diff_errcode;
	poly = (POLYGON *) d;
	if (poly->npts > maxpts)
		abort();
	*npts = poly->npts;
	memcpy(bound, &poly->boundbox, sizeof(BOX));
	memcpy(xys, poly->p, sizeof(Point) * poly->npts);
	return 0;
}

/* out images: return length written into buf (caller-sized; abort guard) */

static int
pg_diff_geo_copyout(const char *s, char *buf, int buflen)
{
	size_t		n = strlen(s);

	if ((int) n + 1 > buflen)
		abort();
	memcpy(buf, s, n + 1);
	return (int) n;
}

int
pg_diff_geo_box_out(const double *in, char *buf, int buflen)
{
	BOX			b;

	pg_geo_arena_reset();
	pg_diff_errcode = 0;
	if (setjmp(pg_geo_jmp) != 0)
		return -1;
	memcpy(&b, in, sizeof(BOX));
	return pg_diff_geo_copyout(path_encode(PATH_NONE, 2, &(b.high)), buf, buflen);
}

int
pg_diff_geo_lseg_out(const double *in, char *buf, int buflen)
{
	LSEG		l;

	pg_geo_arena_reset();
	pg_diff_errcode = 0;
	if (setjmp(pg_geo_jmp) != 0)
		return -1;
	memcpy(&l, in, sizeof(LSEG));
	return pg_diff_geo_copyout(path_encode(PATH_OPEN, 2, &l.p[0]), buf, buflen);
}

int
pg_diff_geo_line_out(const double *in, char *buf, int buflen)
{
	LINE		l;
	char	   *s;

	pg_geo_arena_reset();
	pg_diff_errcode = 0;
	if (setjmp(pg_geo_jmp) != 0)
		return -1;
	memcpy(&l, in, sizeof(LINE));
	s = pg_geo_line_out(&l);
	return pg_diff_geo_copyout(s, buf, buflen);
}

int
pg_diff_geo_circle_out(const double *in, char *buf, int buflen)
{
	CIRCLE		c;

	pg_geo_arena_reset();
	pg_diff_errcode = 0;
	if (setjmp(pg_geo_jmp) != 0)
		return -1;
	memcpy(&c.center, in, sizeof(Point));
	c.radius = in[2];
	return pg_diff_geo_copyout(pg_geo_circle_out(&c), buf, buflen);
}

int
pg_diff_geo_path_out(int32_t npts, int32_t closed, const double *xys, char *buf, int buflen)
{
	pg_geo_arena_reset();
	pg_diff_errcode = 0;
	if (setjmp(pg_geo_jmp) != 0)
		return -1;
	return pg_diff_geo_copyout(
		path_encode(closed ? PATH_CLOSED : PATH_OPEN, npts, (Point *) xys),
		buf, buflen);
}

int
pg_diff_geo_poly_out(int32_t npts, const double *xys, char *buf, int buflen)
{
	pg_geo_arena_reset();
	pg_diff_errcode = 0;
	if (setjmp(pg_geo_jmp) != 0)
		return -1;
	return pg_diff_geo_copyout(
		path_encode(PATH_CLOSED, npts, (Point *) xys), buf, buflen);
}
