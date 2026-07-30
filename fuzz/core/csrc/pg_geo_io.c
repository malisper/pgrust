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

#include <math.h>
#include <limits.h>
#include <setjmp.h>
#include <stdio.h>

/* from pg_float_io.c */
extern int	pg_diff_errcode;
extern char *float8out_internal(double num);

#define pfree free

/* ---- error shims (see header comment) ---- */

static jmp_buf pg_geo_jmp;

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

static char pg_geo_strbuf[512];

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
