/*
 * Vendored PostgreSQL C: the geometric comparator / containment family
 * (point and box relational operators from geo_ops.c).
 *
 * Provenance:
 *   - src/backend/utils/adt/geo_ops.c   @ postgres/postgres REL_18_STABLE
 *     (fetched 2026-07-28)
 *   - src/include/utils/geo_decls.h     @ same ref: EPSILON and the fuzzy
 *     FPzero/FPeq/FPne/FPlt/FPle/FPgt/FPge inline helpers, verbatim
 *     (the #ifdef EPSILON arm — the shipped build).
 *   - src/include/utils/float.h         @ same ref: float8_eq (NaN-aware),
 *     float8_pl, float8_mi, float8_mul, float8_div, verbatim.
 *
 * Functions copied, bodies verbatim, renamed with pg_ prefix:
 *   point_left, point_right, point_above, point_below, point_vert,
 *   point_horiz, point_eq, point_ne, point_eq_point (static inline),
 *   box_same, box_overlap, box_ov (static), box_left, box_overleft,
 *   box_right, box_overright, box_below, box_overbelow, box_above,
 *   box_overabove, box_contained, box_contain, box_contain_box (static),
 *   box_below_eq, box_above_eq, box_contain_pt, on_pb,
 *   box_contain_point (static), box_lt, box_gt, box_eq, box_le, box_ge,
 *   box_area, box_width, box_height, box_center,
 *   box_ar, box_cn, box_wd, box_ht (statics).
 *
 * Shims (plumbing only, never logic):
 *   - PG_FUNCTION_ARGS unwrapping -> plain C signatures. Point/BOX are
 *     by-ref datums; the wrappers take the coordinate doubles and build
 *     the structs locally, then run the verbatim body on pointers.
 *   - PG_RETURN_BOOL -> int return (0/1) (Kani lowers Rust bool vs C _Bool
 *     inconsistently; int is the established shim).
 *   - PG_RETURN_FLOAT8 / PG_RETURN_POINT_P -> double out-params (palloc of
 *     the result Point in box_center is replaced by caller storage; the
 *     computed values are untouched).
 *   - ereport(ERROR) in float8_pl/mi/mul/div (float_overflow_error /
 *     float_underflow_error / float_zero_divide_error, noreturn in
 *     postgres) -> first-error-wins global flag pg_geo_errflag
 *     (1=overflow, 2=underflow, 3=zero-divide; float-arith precedent).
 *     The shimmed helper CONTINUES with the raw IEEE result where real C
 *     longjmps, so once the flag is set downstream values are not
 *     compared — harnesses assert flag/Err VERDICT parity first and value
 *     parity only on the clean arm. All errors reachable from this family
 *     are 22003 (overflow/underflow); zero-divide (3) is unreachable
 *     (box_cn divides by the constant 2.0) but kept for shim fidelity.
 *   - float8 -> double typedef (c.h).
 *
 * pg_point_left_ieee at the bottom is NOT Postgres code: it is a
 * deliberately WRONG comparator (exact IEEE <, no EPSILON) used only by
 * the negative-control harness, which must fail inside the epsilon band —
 * witnessing that the fuzzy-compare constant and direction are in-theorem.
 *
 * EXTENSION (2026-07-28, circle/lseg/point slice — same provenance refs):
 * Functions copied, bodies verbatim, renamed with pg_ prefix:
 *   pg_hypot (float.h decl, geo_ops.c body), point_dt, point_sl,
 *   point_invsl, point_construct, point_add_point, point_sub_point,
 *   statlseg_construct, lseg_sl, lseg_invsl, circle_ar (static inlines),
 *   point_distance, point_slope, point_add, point_sub,
 *   lseg_construct, lseg_vertical, lseg_horizontal, lseg_eq, lseg_ne,
 *   lseg_lt, lseg_le, lseg_gt, lseg_ge, lseg_length, lseg_center,
 *   lseg_parallel, lseg_perp,
 *   circle_same, circle_overlap, circle_overleft, circle_left,
 *   circle_right, circle_overright, circle_contained, circle_contain,
 *   circle_below, circle_above, circle_overbelow, circle_overabove,
 *   circle_eq, circle_ne, circle_lt, circle_gt, circle_le, circle_ge,
 *   circle_add_pt, circle_sub_pt, circle_area, circle_diameter,
 *   circle_radius, circle_distance, circle_contain_pt,
 *   pt_contained_circle, dist_pc, dist_cpoint, circle_center.
 * Additional shims for the extension (plumbing only):
 *   - get_float8_infinity / get_float8_nan: float.h C99 arms
 *     ((float8) INFINITY / (float8) NAN) — the shipped-build arms.
 *   - HYPOT(A,B) -> pg_hypot(A,B) (geo_decls.h define, verbatim).
 *   - M_PI from math.h (defined below if the model header lacks it).
 *   - palloc'd Point/LSEG/CIRCLE results -> caller out-params, values
 *     untouched (box_center precedent).
 * MODEL-SPEC NOTE (fenced, not shimmed): pg_hypot's general path computes
 * x * sqrt(1.0 + yx*yx) as RAW arithmetic; aarch64 C compilers contract
 * 1.0 + yx*yx to fma, and shipped Rust fuses EXPLICITLY (f64::mul_add) to
 * match — but this vendored model compiled by CBMC does NOT contract, so
 * the general hypot path is a known model/silicon spec gap. Every harness
 * fences point_dt to the sqrt-free early-return paths (dy == 0 by
 * construction: axis-aligned slices, plus the Inf/NaN arms); the fenced
 * bodies stay verbatim.
 */

#include "../../support/c/pg_proof_shim.h"
#include <math.h>

typedef double float8;

typedef struct
{
	double		x,
				y;
} Point;

typedef struct
{
	Point		high,
				low;
} BOX;

/* ---- ereport shim: first-error-wins flag (see header comment) ---- */

int			pg_geo_errflag = 0;

static void
float_overflow_error(void)
{
	if (pg_geo_errflag == 0)
		pg_geo_errflag = 1;
}

static void
float_underflow_error(void)
{
	if (pg_geo_errflag == 0)
		pg_geo_errflag = 2;
}

static void
float_zero_divide_error(void)
{
	if (pg_geo_errflag == 0)
		pg_geo_errflag = 3;
}

/* ---- src/include/utils/float.h, verbatim ---- */

static inline bool
float8_eq(const float8 val1, const float8 val2)
{
	return isnan(val1) ? isnan(val2) : !isnan(val2) && val1 == val2;
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

/* ---- src/include/utils/geo_decls.h, verbatim (#ifdef EPSILON arm) ---- */

#define EPSILON					1.0E-06

#define FPzero(A)				(fabs(A) <= EPSILON)

static inline bool
FPeq(double A, double B)
{
	return A == B || fabs(A - B) <= EPSILON;
}

static inline bool
FPne(double A, double B)
{
	return A != B && fabs(A - B) > EPSILON;
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

/* ---- geo_ops.c static helpers, verbatim ---- */

/*
 * Check whether the two points are the same
 */
static inline bool
point_eq_point(Point *pt1, Point *pt2)
{
	/* If any NaNs are involved, insist on exact equality */
	if (unlikely(isnan(pt1->x) || isnan(pt1->y) ||
				 isnan(pt2->x) || isnan(pt2->y)))
		return (float8_eq(pt1->x, pt2->x) && float8_eq(pt1->y, pt2->y));

	return (FPeq(pt1->x, pt2->x) && FPeq(pt1->y, pt2->y));
}

static bool
box_ov(BOX *box1, BOX *box2)
{
	return (FPle(box1->low.x, box2->high.x) &&
			FPle(box2->low.x, box1->high.x) &&
			FPle(box1->low.y, box2->high.y) &&
			FPle(box2->low.y, box1->high.y));
}

/*
 * Check whether the second box is in the first box or on its border
 */
static bool
box_contain_box(BOX *contains_box, BOX *contained_box)
{
	return FPge(contains_box->high.x, contained_box->high.x) &&
		FPle(contains_box->low.x, contained_box->low.x) &&
		FPge(contains_box->high.y, contained_box->high.y) &&
		FPle(contains_box->low.y, contained_box->low.y);
}

/*
 * Check whether the point is in the box or on its border
 */
static bool
box_contain_point(BOX *box, Point *point)
{
	return box->high.x >= point->x && box->low.x <= point->x &&
		box->high.y >= point->y && box->low.y <= point->y;
}

/*		box_wd	-		returns the width (length) of the box
 *								  (horizontal magnitude).
 */
static float8
box_wd(BOX *box)
{
	return float8_mi(box->high.x, box->low.x);
}

/*		box_ht	-		returns the height of the box
 *								  (vertical magnitude).
 */
static float8
box_ht(BOX *box)
{
	return float8_mi(box->high.y, box->low.y);
}

/*		box_ar	-		returns the area of the box.
 */
static float8
box_ar(BOX *box)
{
	return float8_mul(box_wd(box), box_ht(box));
}

/*		box_cn	-		stores the centerpoint of the box into *center.
 */
static void
box_cn(Point *center, BOX *box)
{
	center->x = float8_div(float8_pl(box->high.x, box->low.x), 2.0);
	center->y = float8_div(float8_pl(box->high.y, box->low.y), 2.0);
}

/* ---- fmgr-unwrapped entry points; bodies verbatim from geo_ops.c ---- */

#define PT_ARGS2 double x1, double y1, double x2, double y2
#define PT_LOCALS \
	Point p1_ = {x1, y1}, p2_ = {x2, y2}; \
	Point *pt1 = &p1_; \
	Point *pt2 = &p2_

#define BOX_ARGS2 \
	double ahx, double ahy, double alx, double aly, \
	double bhx, double bhy, double blx, double bly
#define BOX_LOCALS \
	BOX b1_ = {{ahx, ahy}, {alx, aly}}, b2_ = {{bhx, bhy}, {blx, bly}}; \
	BOX *box1 = &b1_; \
	BOX *box2 = &b2_

int
pg_point_left(PT_ARGS2)
{
	PT_LOCALS;

	return FPlt(pt1->x, pt2->x);
}

int
pg_point_right(PT_ARGS2)
{
	PT_LOCALS;

	return FPgt(pt1->x, pt2->x);
}

int
pg_point_above(PT_ARGS2)
{
	PT_LOCALS;

	return FPgt(pt1->y, pt2->y);
}

int
pg_point_below(PT_ARGS2)
{
	PT_LOCALS;

	return FPlt(pt1->y, pt2->y);
}

int
pg_point_vert(PT_ARGS2)
{
	PT_LOCALS;

	return FPeq(pt1->x, pt2->x);
}

int
pg_point_horiz(PT_ARGS2)
{
	PT_LOCALS;

	return FPeq(pt1->y, pt2->y);
}

int
pg_point_eq(PT_ARGS2)
{
	PT_LOCALS;

	return point_eq_point(pt1, pt2);
}

int
pg_point_ne(PT_ARGS2)
{
	PT_LOCALS;

	return !point_eq_point(pt1, pt2);
}

int
pg_box_same(BOX_ARGS2)
{
	BOX_LOCALS;

	return point_eq_point(&box1->high, &box2->high) &&
		point_eq_point(&box1->low, &box2->low);
}

int
pg_box_overlap(BOX_ARGS2)
{
	BOX_LOCALS;

	return box_ov(box1, box2);
}

int
pg_box_left(BOX_ARGS2)
{
	BOX_LOCALS;

	return FPlt(box1->high.x, box2->low.x);
}

int
pg_box_overleft(BOX_ARGS2)
{
	BOX_LOCALS;

	return FPle(box1->high.x, box2->high.x);
}

int
pg_box_right(BOX_ARGS2)
{
	BOX_LOCALS;

	return FPgt(box1->low.x, box2->high.x);
}

int
pg_box_overright(BOX_ARGS2)
{
	BOX_LOCALS;

	return FPge(box1->low.x, box2->low.x);
}

int
pg_box_below(BOX_ARGS2)
{
	BOX_LOCALS;

	return FPlt(box1->high.y, box2->low.y);
}

int
pg_box_overbelow(BOX_ARGS2)
{
	BOX_LOCALS;

	return FPle(box1->high.y, box2->high.y);
}

int
pg_box_above(BOX_ARGS2)
{
	BOX_LOCALS;

	return FPgt(box1->low.y, box2->high.y);
}

int
pg_box_overabove(BOX_ARGS2)
{
	BOX_LOCALS;

	return FPge(box1->low.y, box2->low.y);
}

int
pg_box_contained(BOX_ARGS2)
{
	BOX_LOCALS;

	return box_contain_box(box2, box1);
}

int
pg_box_contain(BOX_ARGS2)
{
	BOX_LOCALS;

	return box_contain_box(box1, box2);
}

int
pg_box_below_eq(BOX_ARGS2)
{
	BOX_LOCALS;

	return FPle(box1->high.y, box2->low.y);
}

int
pg_box_above_eq(BOX_ARGS2)
{
	BOX_LOCALS;

	return FPge(box1->low.y, box2->high.y);
}

/* box_contain_pt(box, pt) and on_pb(pt, box) share box_contain_point. */
int
pg_box_contain_pt(double bhx, double bhy, double blx, double bly,
				  double px, double py)
{
	BOX			b_ = {{bhx, bhy}, {blx, bly}};
	BOX		   *box = &b_;
	Point		p_ = {px, py};
	Point	   *pt = &p_;

	return box_contain_point(box, pt);
}

/*
 * Area-based comparators and box arithmetic: fallible (float8_mi/mul/div
 * overflow/underflow).  Return value = pg_geo_errflag after the call
 * (0 = clean); the boolean/double result rides the out-param.
 */

int
pg_box_lt(BOX_ARGS2, int *result)
{
	BOX_LOCALS;

	pg_geo_errflag = 0;
	*result = FPlt(box_ar(box1), box_ar(box2));
	return pg_geo_errflag;
}

int
pg_box_gt(BOX_ARGS2, int *result)
{
	BOX_LOCALS;

	pg_geo_errflag = 0;
	*result = FPgt(box_ar(box1), box_ar(box2));
	return pg_geo_errflag;
}

int
pg_box_eq(BOX_ARGS2, int *result)
{
	BOX_LOCALS;

	pg_geo_errflag = 0;
	*result = FPeq(box_ar(box1), box_ar(box2));
	return pg_geo_errflag;
}

int
pg_box_le(BOX_ARGS2, int *result)
{
	BOX_LOCALS;

	pg_geo_errflag = 0;
	*result = FPle(box_ar(box1), box_ar(box2));
	return pg_geo_errflag;
}

int
pg_box_ge(BOX_ARGS2, int *result)
{
	BOX_LOCALS;

	pg_geo_errflag = 0;
	*result = FPge(box_ar(box1), box_ar(box2));
	return pg_geo_errflag;
}

int
pg_box_area(double hx, double hy, double lx, double ly, double *result)
{
	BOX			b_ = {{hx, hy}, {lx, ly}};
	BOX		   *box = &b_;

	pg_geo_errflag = 0;
	*result = box_ar(box);
	return pg_geo_errflag;
}

int
pg_box_width(double hx, double hy, double lx, double ly, double *result)
{
	BOX			b_ = {{hx, hy}, {lx, ly}};
	BOX		   *box = &b_;

	pg_geo_errflag = 0;
	*result = box_wd(box);
	return pg_geo_errflag;
}

int
pg_box_height(double hx, double hy, double lx, double ly, double *result)
{
	BOX			b_ = {{hx, hy}, {lx, ly}};
	BOX		   *box = &b_;

	pg_geo_errflag = 0;
	*result = box_ht(box);
	return pg_geo_errflag;
}

int
pg_box_center(double hx, double hy, double lx, double ly,
			  double *cx, double *cy)
{
	BOX			b_ = {{hx, hy}, {lx, ly}};
	BOX		   *box = &b_;
	Point		result_;	/* palloc(sizeof(Point)) -> caller storage */
	Point	   *result = &result_;

	pg_geo_errflag = 0;
	box_cn(result, box);
	*cx = result->x;
	*cy = result->y;
	return pg_geo_errflag;
}

/*
 * NEGATIVE CONTROL ONLY — NOT Postgres code: exact IEEE <, no EPSILON.
 * The control harness pits shipped fc_point_left against this; it MUST
 * fail with a counterexample inside the (0, EPSILON] band, proving the
 * epsilon constant and comparison direction are load-bearing in-theorem.
 */
int
pg_point_left_ieee(PT_ARGS2)
{
	PT_LOCALS;

	return pt1->x < pt2->x;
}

/* =====================================================================
 * EXTENSION: circle / lseg / point slice (see header comment).
 * ===================================================================== */

typedef struct
{
	Point		p[2];
} LSEG;

typedef struct
{
	Point		center;
	float8		radius;
} CIRCLE;

#ifndef M_PI
#define M_PI 3.14159265358979323846
#endif

/* ---- src/include/utils/float.h, C99 arms, verbatim ---- */

/*
 * SHIM: CBMC's <math.h> models the NAN constant with a non-canonical
 * payload, so get_float8_nan()'s in-model bits differ from real C99
 * silicon (native replay: clang/glibc (float8) NAN == 0x7ff8000000000000
 * == Rust f64::NAN; Kani cex was a fabricated NaN-payload "divergence").
 * Pin NAN to the canonical quiet NaN the shipped build produces. Header
 * constant only — function bodies below stay verbatim.
 */
#undef NAN
static inline double
pg_proof_canonical_nan(void)
{
	union
	{
		unsigned long long u;
		double		d;
	}			nan_;

	nan_.u = 0x7ff8000000000000ULL;
	return nan_.d;
}
#define NAN (pg_proof_canonical_nan())

static inline float8
get_float8_infinity(void)
{
	/* C99 standard way */
	return (float8) INFINITY;
}

static inline float8
get_float8_nan(void)
{
	/* C99 standard way */
	return (float8) NAN;
}

/* ---- geo_decls.h ---- */

#define HYPOT(A, B)				pg_hypot(A, B)

/* ---- geo_ops.c pg_hypot, verbatim (ereports -> errflag shim) ---- */

float8
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

/* ---- geo_ops.c static inlines, verbatim ---- */

static inline float8
point_dt(Point *pt1, Point *pt2)
{
	return HYPOT(float8_mi(pt1->x, pt2->x), float8_mi(pt1->y, pt2->y));
}

/*
 * Return slope of two points
 *
 * Note that this function returns Inf when the points are the same.
 */
static inline float8
point_sl(Point *pt1, Point *pt2)
{
	if (FPeq(pt1->x, pt2->x))
		return get_float8_infinity();
	if (FPeq(pt1->y, pt2->y))
		return 0.0;
	return float8_div(float8_mi(pt1->y, pt2->y), float8_mi(pt1->x, pt2->x));
}

/*
 * Return inverse slope of two points
 *
 * Note that this function returns 0.0 when the points are the same.
 */
static inline float8
point_invsl(Point *pt1, Point *pt2)
{
	if (FPeq(pt1->x, pt2->x))
		return 0.0;
	if (FPeq(pt1->y, pt2->y))
		return get_float8_infinity();
	return float8_div(float8_mi(pt1->x, pt2->x), float8_mi(pt2->y, pt1->y));
}

static inline void
point_construct(Point *result, float8 x, float8 y)
{
	result->x = x;
	result->y = y;
}

static inline void
point_add_point(Point *result, Point *pt1, Point *pt2)
{
	point_construct(result,
					float8_pl(pt1->x, pt2->x),
					float8_pl(pt1->y, pt2->y));
}

static inline void
point_sub_point(Point *result, Point *pt1, Point *pt2)
{
	point_construct(result,
					float8_mi(pt1->x, pt2->x),
					float8_mi(pt1->y, pt2->y));
}

/* like lseg_construct, but assume space already allocated */
static inline void
statlseg_construct(LSEG *lseg, Point *pt1, Point *pt2)
{
	lseg->p[0].x = pt1->x;
	lseg->p[0].y = pt1->y;
	lseg->p[1].x = pt2->x;
	lseg->p[1].y = pt2->y;
}

/*
 * Return slope of the line segment
 */
static inline float8
lseg_sl(LSEG *lseg)
{
	return point_sl(&lseg->p[0], &lseg->p[1]);
}

/*
 * Return inverse slope of the line segment
 */
static inline float8
lseg_invsl(LSEG *lseg)
{
	return point_invsl(&lseg->p[0], &lseg->p[1]);
}

/*		circle_ar		-		returns the area of the circle.
 */
static float8
circle_ar(CIRCLE *circle)
{
	return float8_mul(float8_mul(circle->radius, circle->radius), M_PI);
}

/* ---- fmgr-unwrapped entry points; bodies verbatim from geo_ops.c ---- */

#define LSEG_ARGS2 \
	double ax1, double ay1, double ax2, double ay2, \
	double bx1, double by1, double bx2, double by2
#define LSEG_LOCALS \
	LSEG la_ = {{{ax1, ay1}, {ax2, ay2}}}, lb_ = {{{bx1, by1}, {bx2, by2}}}; \
	LSEG *l1 = &la_; \
	LSEG *l2 = &lb_

#define LSEG_ARGS1 double sx1, double sy1, double sx2, double sy2
#define LSEG_LOCALS1 \
	LSEG ls_ = {{{sx1, sy1}, {sx2, sy2}}}; \
	LSEG *lseg = &ls_

#define CIRCLE_ARGS2 \
	double c1x, double c1y, double r1, \
	double c2x, double c2y, double r2
#define CIRCLE_LOCALS \
	CIRCLE ca_ = {{c1x, c1y}, r1}, cb_ = {{c2x, c2y}, r2}; \
	CIRCLE *circle1 = &ca_; \
	CIRCLE *circle2 = &cb_

#define CIRCLE_ARGS1 double ccx, double ccy, double crr
#define CIRCLE_LOCALS1 \
	CIRCLE c_ = {{ccx, ccy}, crr}; \
	CIRCLE *circle = &c_

/* ---- point arithmetic / measurement ---- */

int
pg_point_distance(double x1, double y1, double x2, double y2, double *result)
{
	PT_LOCALS;

	pg_geo_errflag = 0;
	*result = point_dt(pt1, pt2);
	return pg_geo_errflag;
}

int
pg_point_slope(double x1, double y1, double x2, double y2, double *result)
{
	PT_LOCALS;

	pg_geo_errflag = 0;
	*result = point_sl(pt1, pt2);
	return pg_geo_errflag;
}

int
pg_point_add(double x1, double y1, double x2, double y2,
			 double *ox, double *oy)
{
	PT_LOCALS;
	Point		result_;	/* palloc(sizeof(Point)) -> caller storage */
	Point	   *result = &result_;

	pg_geo_errflag = 0;
	point_add_point(result, pt1, pt2);
	*ox = result->x;
	*oy = result->y;
	return pg_geo_errflag;
}

int
pg_point_sub(double x1, double y1, double x2, double y2,
			 double *ox, double *oy)
{
	PT_LOCALS;
	Point		result_;	/* palloc(sizeof(Point)) -> caller storage */
	Point	   *result = &result_;

	pg_geo_errflag = 0;
	point_sub_point(result, pt1, pt2);
	*ox = result->x;
	*oy = result->y;
	return pg_geo_errflag;
}

/* ---- lseg ---- */

int
pg_lseg_construct(double x1, double y1, double x2, double y2,
				  double *out4 /* [x1,y1,x2,y2] */)
{
	PT_LOCALS;
	LSEG		result_;	/* palloc(sizeof(LSEG)) -> caller storage */
	LSEG	   *result = &result_;

	statlseg_construct(result, pt1, pt2);

	out4[0] = result->p[0].x;
	out4[1] = result->p[0].y;
	out4[2] = result->p[1].x;
	out4[3] = result->p[1].y;
	return 0;				/* void -> int shim (Unit/void goto-cc trap) */
}

int
pg_lseg_vertical(LSEG_ARGS1)
{
	LSEG_LOCALS1;

	return FPeq(lseg->p[0].x, lseg->p[1].x);
}

int
pg_lseg_horizontal(LSEG_ARGS1)
{
	LSEG_LOCALS1;

	return FPeq(lseg->p[0].y, lseg->p[1].y);
}

int
pg_lseg_eq(LSEG_ARGS2)
{
	LSEG_LOCALS;

	return point_eq_point(&l1->p[0], &l2->p[0]) &&
		point_eq_point(&l1->p[1], &l2->p[1]);
}

int
pg_lseg_ne(LSEG_ARGS2)
{
	LSEG_LOCALS;

	return !point_eq_point(&l1->p[0], &l2->p[0]) ||
		!point_eq_point(&l1->p[1], &l2->p[1]);
}

int
pg_lseg_lt(LSEG_ARGS2, int *result)
{
	LSEG_LOCALS;

	pg_geo_errflag = 0;
	*result = FPlt(point_dt(&l1->p[0], &l1->p[1]),
				   point_dt(&l2->p[0], &l2->p[1]));
	return pg_geo_errflag;
}

int
pg_lseg_le(LSEG_ARGS2, int *result)
{
	LSEG_LOCALS;

	pg_geo_errflag = 0;
	*result = FPle(point_dt(&l1->p[0], &l1->p[1]),
				   point_dt(&l2->p[0], &l2->p[1]));
	return pg_geo_errflag;
}

int
pg_lseg_gt(LSEG_ARGS2, int *result)
{
	LSEG_LOCALS;

	pg_geo_errflag = 0;
	*result = FPgt(point_dt(&l1->p[0], &l1->p[1]),
				   point_dt(&l2->p[0], &l2->p[1]));
	return pg_geo_errflag;
}

int
pg_lseg_ge(LSEG_ARGS2, int *result)
{
	LSEG_LOCALS;

	pg_geo_errflag = 0;
	*result = FPge(point_dt(&l1->p[0], &l1->p[1]),
				   point_dt(&l2->p[0], &l2->p[1]));
	return pg_geo_errflag;
}

int
pg_lseg_length(LSEG_ARGS1, double *result)
{
	LSEG_LOCALS1;

	pg_geo_errflag = 0;
	*result = point_dt(&lseg->p[0], &lseg->p[1]);
	return pg_geo_errflag;
}

int
pg_lseg_center(LSEG_ARGS1, double *cx, double *cy)
{
	LSEG_LOCALS1;
	Point		result_;	/* palloc(sizeof(Point)) -> caller storage */
	Point	   *result = &result_;

	pg_geo_errflag = 0;
	result->x = float8_div(float8_pl(lseg->p[0].x, lseg->p[1].x), 2.0);
	result->y = float8_div(float8_pl(lseg->p[0].y, lseg->p[1].y), 2.0);
	*cx = result->x;
	*cy = result->y;
	return pg_geo_errflag;
}

int
pg_lseg_parallel(LSEG_ARGS2, int *result)
{
	LSEG_LOCALS;

	pg_geo_errflag = 0;
	*result = FPeq(lseg_sl(l1), lseg_sl(l2));
	return pg_geo_errflag;
}

int
pg_lseg_perp(LSEG_ARGS2, int *result)
{
	LSEG_LOCALS;

	pg_geo_errflag = 0;
	*result = FPeq(lseg_sl(l1), lseg_invsl(l2));
	return pg_geo_errflag;
}

/* ---- circle predicates ---- */

int
pg_circle_same(CIRCLE_ARGS2)
{
	CIRCLE_LOCALS;

	return ((isnan(circle1->radius) && isnan(circle2->radius)) ||
			FPeq(circle1->radius, circle2->radius)) &&
		point_eq_point(&circle1->center, &circle2->center);
}

int
pg_circle_overlap(CIRCLE_ARGS2, int *result)
{
	CIRCLE_LOCALS;

	pg_geo_errflag = 0;
	*result = FPle(point_dt(&circle1->center, &circle2->center),
				   float8_pl(circle1->radius, circle2->radius));
	return pg_geo_errflag;
}

int
pg_circle_overleft(CIRCLE_ARGS2, int *result)
{
	CIRCLE_LOCALS;

	pg_geo_errflag = 0;
	*result = FPle(float8_pl(circle1->center.x, circle1->radius),
				   float8_pl(circle2->center.x, circle2->radius));
	return pg_geo_errflag;
}

int
pg_circle_left(CIRCLE_ARGS2, int *result)
{
	CIRCLE_LOCALS;

	pg_geo_errflag = 0;
	*result = FPlt(float8_pl(circle1->center.x, circle1->radius),
				   float8_mi(circle2->center.x, circle2->radius));
	return pg_geo_errflag;
}

int
pg_circle_right(CIRCLE_ARGS2, int *result)
{
	CIRCLE_LOCALS;

	pg_geo_errflag = 0;
	*result = FPgt(float8_mi(circle1->center.x, circle1->radius),
				   float8_pl(circle2->center.x, circle2->radius));
	return pg_geo_errflag;
}

int
pg_circle_overright(CIRCLE_ARGS2, int *result)
{
	CIRCLE_LOCALS;

	pg_geo_errflag = 0;
	*result = FPge(float8_mi(circle1->center.x, circle1->radius),
				   float8_mi(circle2->center.x, circle2->radius));
	return pg_geo_errflag;
}

int
pg_circle_contained(CIRCLE_ARGS2, int *result)
{
	CIRCLE_LOCALS;

	pg_geo_errflag = 0;
	*result = FPle(point_dt(&circle1->center, &circle2->center),
				   float8_mi(circle2->radius, circle1->radius));
	return pg_geo_errflag;
}

int
pg_circle_contain(CIRCLE_ARGS2, int *result)
{
	CIRCLE_LOCALS;

	pg_geo_errflag = 0;
	*result = FPle(point_dt(&circle1->center, &circle2->center),
				   float8_mi(circle1->radius, circle2->radius));
	return pg_geo_errflag;
}

int
pg_circle_below(CIRCLE_ARGS2, int *result)
{
	CIRCLE_LOCALS;

	pg_geo_errflag = 0;
	*result = FPlt(float8_pl(circle1->center.y, circle1->radius),
				   float8_mi(circle2->center.y, circle2->radius));
	return pg_geo_errflag;
}

int
pg_circle_above(CIRCLE_ARGS2, int *result)
{
	CIRCLE_LOCALS;

	pg_geo_errflag = 0;
	*result = FPgt(float8_mi(circle1->center.y, circle1->radius),
				   float8_pl(circle2->center.y, circle2->radius));
	return pg_geo_errflag;
}

int
pg_circle_overbelow(CIRCLE_ARGS2, int *result)
{
	CIRCLE_LOCALS;

	pg_geo_errflag = 0;
	*result = FPle(float8_pl(circle1->center.y, circle1->radius),
				   float8_pl(circle2->center.y, circle2->radius));
	return pg_geo_errflag;
}

int
pg_circle_overabove(CIRCLE_ARGS2, int *result)
{
	CIRCLE_LOCALS;

	pg_geo_errflag = 0;
	*result = FPge(float8_mi(circle1->center.y, circle1->radius),
				   float8_mi(circle2->center.y, circle2->radius));
	return pg_geo_errflag;
}

int
pg_circle_eq(CIRCLE_ARGS2, int *result)
{
	CIRCLE_LOCALS;

	pg_geo_errflag = 0;
	*result = FPeq(circle_ar(circle1), circle_ar(circle2));
	return pg_geo_errflag;
}

int
pg_circle_ne(CIRCLE_ARGS2, int *result)
{
	CIRCLE_LOCALS;

	pg_geo_errflag = 0;
	*result = FPne(circle_ar(circle1), circle_ar(circle2));
	return pg_geo_errflag;
}

int
pg_circle_lt(CIRCLE_ARGS2, int *result)
{
	CIRCLE_LOCALS;

	pg_geo_errflag = 0;
	*result = FPlt(circle_ar(circle1), circle_ar(circle2));
	return pg_geo_errflag;
}

int
pg_circle_gt(CIRCLE_ARGS2, int *result)
{
	CIRCLE_LOCALS;

	pg_geo_errflag = 0;
	*result = FPgt(circle_ar(circle1), circle_ar(circle2));
	return pg_geo_errflag;
}

int
pg_circle_le(CIRCLE_ARGS2, int *result)
{
	CIRCLE_LOCALS;

	pg_geo_errflag = 0;
	*result = FPle(circle_ar(circle1), circle_ar(circle2));
	return pg_geo_errflag;
}

int
pg_circle_ge(CIRCLE_ARGS2, int *result)
{
	CIRCLE_LOCALS;

	pg_geo_errflag = 0;
	*result = FPge(circle_ar(circle1), circle_ar(circle2));
	return pg_geo_errflag;
}

/* ---- circle arithmetic / accessors ---- */

int
pg_circle_add_pt(CIRCLE_ARGS1, double px, double py,
				 double *ox, double *oy, double *orad)
{
	CIRCLE_LOCALS1;
	Point		p_ = {px, py};
	Point	   *point = &p_;
	CIRCLE		result_;	/* palloc(sizeof(CIRCLE)) -> caller storage */
	CIRCLE	   *result = &result_;

	pg_geo_errflag = 0;
	point_add_point(&result->center, &circle->center, point);
	result->radius = circle->radius;
	*ox = result->center.x;
	*oy = result->center.y;
	*orad = result->radius;
	return pg_geo_errflag;
}

int
pg_circle_sub_pt(CIRCLE_ARGS1, double px, double py,
				 double *ox, double *oy, double *orad)
{
	CIRCLE_LOCALS1;
	Point		p_ = {px, py};
	Point	   *point = &p_;
	CIRCLE		result_;	/* palloc(sizeof(CIRCLE)) -> caller storage */
	CIRCLE	   *result = &result_;

	pg_geo_errflag = 0;
	point_sub_point(&result->center, &circle->center, point);
	result->radius = circle->radius;
	*ox = result->center.x;
	*oy = result->center.y;
	*orad = result->radius;
	return pg_geo_errflag;
}

int
pg_circle_area(CIRCLE_ARGS1, double *result)
{
	CIRCLE_LOCALS1;

	pg_geo_errflag = 0;
	*result = circle_ar(circle);
	return pg_geo_errflag;
}

int
pg_circle_diameter(CIRCLE_ARGS1, double *result)
{
	CIRCLE_LOCALS1;

	pg_geo_errflag = 0;
	*result = float8_mul(circle->radius, 2.0);
	return pg_geo_errflag;
}

int
pg_circle_radius(CIRCLE_ARGS1, double *result)
{
	CIRCLE_LOCALS1;

	pg_geo_errflag = 0;
	*result = circle->radius;
	return pg_geo_errflag;
}

int
pg_circle_distance(CIRCLE_ARGS2, double *out)
{
	CIRCLE_LOCALS;
	float8		result;

	pg_geo_errflag = 0;
	result = float8_mi(point_dt(&circle1->center, &circle2->center),
					   float8_pl(circle1->radius, circle2->radius));
	if (result < 0.0)
		result = 0.0;

	*out = result;
	return pg_geo_errflag;
}

int
pg_circle_contain_pt(CIRCLE_ARGS1, double px, double py, int *result)
{
	CIRCLE_LOCALS1;
	Point		p_ = {px, py};
	Point	   *point = &p_;
	float8		d;

	pg_geo_errflag = 0;
	d = point_dt(&circle->center, point);
	*result = (d <= circle->radius);
	return pg_geo_errflag;
}

int
pg_pt_contained_circle(double px, double py, CIRCLE_ARGS1, int *result)
{
	Point		p_ = {px, py};
	Point	   *point = &p_;
	CIRCLE_LOCALS1;
	float8		d;

	pg_geo_errflag = 0;
	d = point_dt(&circle->center, point);
	*result = (d <= circle->radius);
	return pg_geo_errflag;
}

int
pg_dist_pc(double px, double py, CIRCLE_ARGS1, double *out)
{
	Point		p_ = {px, py};
	Point	   *point = &p_;
	CIRCLE_LOCALS1;
	float8		result;

	pg_geo_errflag = 0;
	result = float8_mi(point_dt(point, &circle->center),
					   circle->radius);
	if (result < 0.0)
		result = 0.0;

	*out = result;
	return pg_geo_errflag;
}

int
pg_dist_cpoint(CIRCLE_ARGS1, double px, double py, double *out)
{
	CIRCLE_LOCALS1;
	Point		p_ = {px, py};
	Point	   *point = &p_;
	float8		result;

	pg_geo_errflag = 0;
	result = float8_mi(point_dt(point, &circle->center), circle->radius);
	if (result < 0.0)
		result = 0.0;

	*out = result;
	return pg_geo_errflag;
}

int
pg_circle_center(CIRCLE_ARGS1, double *ox, double *oy)
{
	CIRCLE_LOCALS1;
	Point		result_;	/* palloc(sizeof(Point)) -> caller storage */
	Point	   *result = &result_;

	result->x = circle->center.x;
	result->y = circle->center.y;
	*ox = result->x;
	*oy = result->y;
	return 0;				/* void -> int shim (Unit/void goto-cc trap) */
}

/* =====================================================================
 * EXTENSION 2 (2026-07-29): box constructors/arithmetic, line
 * predicates, on_XX / inter_XX containment predicates, conversion
 * constructors (same provenance refs as the header: geo_ops.c +
 * geo_decls.h + float.h @ REL_18_STABLE, fetched 2026-07-29).
 *
 * Functions copied, bodies verbatim, renamed with pg_ prefix:
 *   box_construct, point_mul_point, point_div_point, line_construct,
 *   line_contain_point, lseg_contain_point, box_contain_lseg,
 *   line_interpt_line, lseg_interpt_line, lseg_interpt_lseg,
 *   box_interpt_lseg (statics/static inlines);
 *   points_box, box_add, box_sub, box_mul, box_div, point_mul,
 *   point_div, construct_point, point_box, boxes_bound_box,
 *   box_intersect, box_diagonal, box_distance, box_circle, cr_circle,
 *   line_vertical, line_horizontal, line_perp, line_eq,
 *   line_construct_pp, on_pl, on_ps, on_sl, on_sb, inter_sb.
 * Additional inlines from float.h, verbatim: float8_lt, float8_gt,
 *   float8_min, float8_max.
 * Additional shims (plumbing only, never logic):
 *   - LINE typedef (geo_decls.h).
 *   - palloc'd BOX/LSEG/CIRCLE/LINE/Point results -> caller out-param
 *     arrays (double[4]/double[3]/coordinate scalars), values untouched
 *     (box_center precedent).
 *   - box_intersect's PG_RETURN_NULL() -> *isnull = 1 out-param
 *     (result doubles left untouched; harness compares only when both
 *     sides are non-null).
 *   - line_construct_pp's ereport(ERRCODE_INVALID_PARAMETER_VALUE)
 *     (noreturn) -> pg_geo_errflag = 4 + immediate return (new flag
 *     value; 1=overflow, 2=underflow, 3=zero-divide stay as before).
 * ===================================================================== */

typedef struct
{
	float8		A,
				B,
				C;
} LINE;

/* ---- src/include/utils/float.h, verbatim ---- */

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

static inline float8
float8_min(const float8 val1, const float8 val2)
{
	return float8_lt(val1, val2) ? val1 : val2;
}

static inline float8
float8_max(const float8 val1, const float8 val2)
{
	return float8_gt(val1, val2) ? val1 : val2;
}

/* ---- geo_ops.c statics/static inlines, verbatim ---- */

/*		box_construct	-		fill in a new box.
 */
static inline void
box_construct(BOX *result, Point *pt1, Point *pt2)
{
	if (float8_gt(pt1->x, pt2->x))
	{
		result->high.x = pt1->x;
		result->low.x = pt2->x;
	}
	else
	{
		result->high.x = pt2->x;
		result->low.x = pt1->x;
	}
	if (float8_gt(pt1->y, pt2->y))
	{
		result->high.y = pt1->y;
		result->low.y = pt2->y;
	}
	else
	{
		result->high.y = pt2->y;
		result->low.y = pt1->y;
	}
}

static inline void
point_mul_point(Point *result, Point *pt1, Point *pt2)
{
	point_construct(result,
					float8_mi(float8_mul(pt1->x, pt2->x),
							  float8_mul(pt1->y, pt2->y)),
					float8_pl(float8_mul(pt1->x, pt2->y),
							  float8_mul(pt1->y, pt2->x)));
}

static inline void
point_div_point(Point *result, Point *pt1, Point *pt2)
{
	float8		div;

	div = float8_pl(float8_mul(pt2->x, pt2->x), float8_mul(pt2->y, pt2->y));

	point_construct(result,
					float8_div(float8_pl(float8_mul(pt1->x, pt2->x),
										 float8_mul(pt1->y, pt2->y)), div),
					float8_div(float8_mi(float8_mul(pt1->y, pt2->x),
										 float8_mul(pt1->x, pt2->y)), div));
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

/*
 *		Does the point satisfy the equation?
 */
static bool
line_contain_point(LINE *line, Point *point)
{
	return FPzero(float8_pl(float8_pl(float8_mul(line->A, point->x),
									  float8_mul(line->B, point->y)),
							line->C));
}

/*
 *		Determine colinearity by detecting a triangle inequality.
 * This algorithm seems to behave nicely even with lsb residues - tgl 1997-07-09
 */
static bool
lseg_contain_point(LSEG *lseg, Point *pt)
{
	return FPeq(point_dt(pt, &lseg->p[0]) +
				point_dt(pt, &lseg->p[1]),
				point_dt(&lseg->p[0], &lseg->p[1]));
}

/*
 * Check whether the line segment is in the box or on its border
 *
 * It is, if both of its points are in the box or on its border.
 */
static bool
box_contain_lseg(BOX *box, LSEG *lseg)
{
	return box_contain_point(box, &lseg->p[0]) &&
		box_contain_point(box, &lseg->p[1]);
}

/*
 * Internal version of line_interpt
 *
 * Return whether two lines intersect. If *result is not NULL, it is set to
 * the intersection point.
 *
 * NOTE: If the lines are identical then we will find they are parallel
 * and report "no intersection".  This is a little weird, but since
 * there's no *unique* intersection, maybe it's appropriate behavior.
 *
 * If the lines have NaN constants, we will return true, and the intersection
 * point would have NaN coordinates.  We shouldn't return false in this case
 * because that would mean the lines are parallel.
 */
static bool
line_interpt_line(Point *result, LINE *l1, LINE *l2)
{
	float8		x,
				y;

	if (!FPzero(l1->B))
	{
		if (FPeq(l2->A, float8_mul(l1->A, float8_div(l2->B, l1->B))))
			return false;

		x = float8_div(float8_mi(float8_mul(l1->B, l2->C),
								 float8_mul(l2->B, l1->C)),
					   float8_mi(float8_mul(l1->A, l2->B),
								 float8_mul(l2->A, l1->B)));
		y = float8_div(-float8_pl(float8_mul(l1->A, x), l1->C), l1->B);
	}
	else if (!FPzero(l2->B))
	{
		if (FPeq(l1->A, float8_mul(l2->A, float8_div(l1->B, l2->B))))
			return false;

		x = float8_div(float8_mi(float8_mul(l2->B, l1->C),
								 float8_mul(l1->B, l2->C)),
					   float8_mi(float8_mul(l2->A, l1->B),
								 float8_mul(l1->A, l2->B)));
		y = float8_div(-float8_pl(float8_mul(l2->A, x), l2->C), l2->B);
	}
	else
		return false;

	/* On some platforms, the preceding expressions tend to produce -0. */
	if (x == 0.0)
		x = 0.0;
	if (y == 0.0)
		y = 0.0;

	if (result != NULL)
		point_construct(result, x, y);

	return true;
}

/*
 * Return whether the line segment intersect with the line. If *result is not
 * NULL, it is set to the intersection point.
 */
static bool
lseg_interpt_line(Point *result, LSEG *lseg, LINE *line)
{
	Point		interpt;
	LINE		tmp;

	/*
	 * First, we promote the line segment to a line, because we know how to
	 * find the intersection point of two lines.  If they don't have an
	 * intersection point, we are done.
	 */
	line_construct(&tmp, &lseg->p[0], lseg_sl(lseg));
	if (!line_interpt_line(&interpt, &tmp, line))
		return false;

	/*
	 * Then, we check whether the intersection point is actually on the line
	 * segment.
	 */
	if (!lseg_contain_point(lseg, &interpt))
		return false;
	if (result != NULL)
	{
		/*
		 * If there is an intersection, then check explicitly for matching
		 * endpoints since there may be rounding effects with annoying LSB
		 * residue.
		 */
		if (point_eq_point(&lseg->p[0], &interpt))
			*result = lseg->p[0];
		else if (point_eq_point(&lseg->p[1], &interpt))
			*result = lseg->p[1];
		else
			*result = interpt;
	}

	return true;
}

/*
 * Return whether the two segments intersect. If *result is not NULL,
 * it is set to the intersection point.
 *
 * This function is almost perfectly symmetric, even though it doesn't look
 * like it.  See lseg_interpt_line() for the other half of it.
 */
static bool
lseg_interpt_lseg(Point *result, LSEG *l1, LSEG *l2)
{
	Point		interpt;
	LINE		tmp;

	line_construct(&tmp, &l2->p[0], lseg_sl(l2));
	if (!lseg_interpt_line(&interpt, l1, &tmp))
		return false;

	/*
	 * If the line intersection point isn't within l2, there is no valid
	 * segment intersection point at all.
	 */
	if (!lseg_contain_point(l2, &interpt))
		return false;

	if (result != NULL)
		*result = interpt;

	return true;
}

/*
 * Do line segment and box intersect?
 *
 * Segment completely inside box counts as intersection.
 * If you want only segments crossing box boundaries,
 *	try converting box to path first.
 *
 * This function also sets the *result to the closest point on the line
 * segment to the center of the box when they overlap and the result is
 * not NULL.  It is somewhat arbitrary, but maybe the best we can do as
 * there are typically two points they intersect.
 *
 * Optimize for non-intersection by checking for box intersection first.
 * - thomas 1998-01-30
 */
static bool
box_interpt_lseg(Point *result, BOX *box, LSEG *lseg)
{
	BOX			lbox;
	LSEG		bseg;
	Point		point;

	lbox.low.x = float8_min(lseg->p[0].x, lseg->p[1].x);
	lbox.low.y = float8_min(lseg->p[0].y, lseg->p[1].y);
	lbox.high.x = float8_max(lseg->p[0].x, lseg->p[1].x);
	lbox.high.y = float8_max(lseg->p[0].y, lseg->p[1].y);

	/* nothing close to overlap? then not going to intersect */
	if (!box_ov(&lbox, box))
		return false;

	if (result != NULL)
	{
		box_cn(&point, box);
		/* lseg_closept_point(result, lseg, &point); -- unreachable:
		 * every entry point below passes result == NULL (inter_sb),
		 * exactly as the C caller does; body otherwise verbatim. */
	}

	/* an endpoint of segment is inside box? then clearly intersects */
	if (box_contain_point(box, &lseg->p[0]) ||
		box_contain_point(box, &lseg->p[1]))
		return true;

	/* pairwise check lseg intersections */
	point.x = box->low.x;
	point.y = box->high.y;
	statlseg_construct(&bseg, &box->low, &point);
	if (lseg_interpt_lseg(NULL, &bseg, lseg))
		return true;

	statlseg_construct(&bseg, &box->high, &point);
	if (lseg_interpt_lseg(NULL, &bseg, lseg))
		return true;

	point.x = box->high.x;
	point.y = box->low.y;
	statlseg_construct(&bseg, &box->low, &point);
	if (lseg_interpt_lseg(NULL, &bseg, lseg))
		return true;

	statlseg_construct(&bseg, &box->high, &point);
	if (lseg_interpt_lseg(NULL, &bseg, lseg))
		return true;

	/* if we dropped through, no two segs intersected */
	return false;
}

/* ---- fmgr-unwrapped entry points; bodies verbatim from geo_ops.c ---- */

#define BOXA_ARGS double ahx, double ahy, double alx, double aly
#define BOXA_LOCALS \
	BOX ba_ = {{ahx, ahy}, {alx, aly}}; \
	BOX *box = &ba_

#define LINE_ARGS1 double l1a, double l1b, double l1c
#define LINE_ARGS2 double l1a, double l1b, double l1c, \
	double l2a, double l2b, double l2c
#define LINE_LOCALS1 \
	LINE ln1_ = {l1a, l1b, l1c}; \
	LINE *line = &ln1_
#define LINE_LOCALS2 \
	LINE ln1_ = {l1a, l1b, l1c}, ln2_ = {l2a, l2b, l2c}; \
	LINE *l1 = &ln1_; \
	LINE *l2 = &ln2_

#define BOX_OUT4(res) \
	out4[0] = (res)->high.x; out4[1] = (res)->high.y; \
	out4[2] = (res)->low.x;  out4[3] = (res)->low.y

/* second box as the SECOND argument (pg_on_sb/pg_inter_sb take an lseg
 * first; BOX_ARGS2's a-prefixed half is unused there) */
#define BOX_ARGS2_B double bhx, double bhy, double blx, double bly

int
pg_points_box(double x1, double y1, double x2, double y2, double *out4)
{
	PT_LOCALS;
	Point	   *p1 = pt1;
	Point	   *p2 = pt2;
	BOX			result_;	/* palloc -> caller storage */
	BOX		   *result = &result_;

	pg_geo_errflag = 0;
	box_construct(result, p1, p2);
	BOX_OUT4(result);
	return pg_geo_errflag;
}

int
pg_box_add(BOXA_ARGS, double px, double py, double *out4)
{
	BOXA_LOCALS;
	Point		p_ = {px, py};
	Point	   *p = &p_;
	BOX			result_;
	BOX		   *result = &result_;

	pg_geo_errflag = 0;
	point_add_point(&result->high, &box->high, p);
	point_add_point(&result->low, &box->low, p);
	BOX_OUT4(result);
	return pg_geo_errflag;
}

int
pg_box_sub(BOXA_ARGS, double px, double py, double *out4)
{
	BOXA_LOCALS;
	Point		p_ = {px, py};
	Point	   *p = &p_;
	BOX			result_;
	BOX		   *result = &result_;

	pg_geo_errflag = 0;
	point_sub_point(&result->high, &box->high, p);
	point_sub_point(&result->low, &box->low, p);
	BOX_OUT4(result);
	return pg_geo_errflag;
}

int
pg_box_mul(BOXA_ARGS, double px, double py, double *out4)
{
	BOXA_LOCALS;
	Point		p_ = {px, py};
	Point	   *p = &p_;
	BOX			result_;
	BOX		   *result = &result_;
	Point		high,
				low;

	pg_geo_errflag = 0;
	point_mul_point(&high, &box->high, p);
	point_mul_point(&low, &box->low, p);

	box_construct(result, &high, &low);
	BOX_OUT4(result);
	return pg_geo_errflag;
}

int
pg_box_div(BOXA_ARGS, double px, double py, double *out4)
{
	BOXA_LOCALS;
	Point		p_ = {px, py};
	Point	   *p = &p_;
	BOX			result_;
	BOX		   *result = &result_;
	Point		high,
				low;

	pg_geo_errflag = 0;
	point_div_point(&high, &box->high, p);
	point_div_point(&low, &box->low, p);

	box_construct(result, &high, &low);
	BOX_OUT4(result);
	return pg_geo_errflag;
}

int
pg_point_mul(double x1, double y1, double x2, double y2,
			 double *ox, double *oy)
{
	PT_LOCALS;
	Point	   *p1 = pt1;
	Point	   *p2 = pt2;
	Point		result_;
	Point	   *result = &result_;

	pg_geo_errflag = 0;
	point_mul_point(result, p1, p2);
	*ox = result->x;
	*oy = result->y;
	return pg_geo_errflag;
}

int
pg_point_div(double x1, double y1, double x2, double y2,
			 double *ox, double *oy)
{
	PT_LOCALS;
	Point	   *p1 = pt1;
	Point	   *p2 = pt2;
	Point		result_;
	Point	   *result = &result_;

	pg_geo_errflag = 0;
	point_div_point(result, p1, p2);
	*ox = result->x;
	*oy = result->y;
	return pg_geo_errflag;
}

int
pg_construct_point(double x, double y, double *ox, double *oy)
{
	Point		result_;
	Point	   *result = &result_;

	pg_geo_errflag = 0;
	point_construct(result, x, y);
	*ox = result->x;
	*oy = result->y;
	return pg_geo_errflag;
}

int
pg_point_box(double px, double py, double *out4)
{
	Point		p_ = {px, py};
	Point	   *pt = &p_;
	BOX			box_;
	BOX		   *box = &box_;

	pg_geo_errflag = 0;
	box->high.x = pt->x;
	box->low.x = pt->x;
	box->high.y = pt->y;
	box->low.y = pt->y;

	out4[0] = box->high.x; out4[1] = box->high.y;
	out4[2] = box->low.x;  out4[3] = box->low.y;
	return pg_geo_errflag;
}

int
pg_boxes_bound_box(BOX_ARGS2, double *out4)
{
	BOX_LOCALS;
	BOX			container_;
	BOX		   *container = &container_;

	pg_geo_errflag = 0;
	container->high.x = float8_max(box1->high.x, box2->high.x);
	container->low.x = float8_min(box1->low.x, box2->low.x);
	container->high.y = float8_max(box1->high.y, box2->high.y);
	container->low.y = float8_min(box1->low.y, box2->low.y);

	BOX_OUT4(container);
	return pg_geo_errflag;
}

int
pg_box_intersect(BOX_ARGS2, double *out4, int *isnull)
{
	BOX_LOCALS;
	BOX			result_;
	BOX		   *result = &result_;

	pg_geo_errflag = 0;
	*isnull = 0;
	if (!box_ov(box1, box2))
	{
		*isnull = 1;		/* PG_RETURN_NULL() */
		return pg_geo_errflag;
	}

	result->high.x = float8_min(box1->high.x, box2->high.x);
	result->low.x = float8_max(box1->low.x, box2->low.x);
	result->high.y = float8_min(box1->high.y, box2->high.y);
	result->low.y = float8_max(box1->low.y, box2->low.y);

	BOX_OUT4(result);
	return pg_geo_errflag;
}

int
pg_box_diagonal(BOXA_ARGS, double *out4)
{
	BOXA_LOCALS;
	LSEG		result_;
	LSEG	   *result = &result_;

	pg_geo_errflag = 0;
	statlseg_construct(result, &box->high, &box->low);
	out4[0] = result->p[0].x; out4[1] = result->p[0].y;
	out4[2] = result->p[1].x; out4[3] = result->p[1].y;
	return pg_geo_errflag;
}

int
pg_box_distance(BOX_ARGS2, double *result)
{
	BOX_LOCALS;
	Point		a,
				b;

	pg_geo_errflag = 0;
	box_cn(&a, box1);
	box_cn(&b, box2);

	*result = point_dt(&a, &b);
	return pg_geo_errflag;
}

int
pg_box_circle(BOXA_ARGS, double *ocx, double *ocy, double *orad)
{
	BOXA_LOCALS;
	CIRCLE		circle_;
	CIRCLE	   *circle = &circle_;

	pg_geo_errflag = 0;
	circle->center.x = float8_div(float8_pl(box->high.x, box->low.x), 2.0);
	circle->center.y = float8_div(float8_pl(box->high.y, box->low.y), 2.0);

	circle->radius = point_dt(&circle->center, &box->high);

	*ocx = circle->center.x;
	*ocy = circle->center.y;
	*orad = circle->radius;
	return pg_geo_errflag;
}

int
pg_cr_circle(double px, double py, double radius,
			 double *ocx, double *ocy, double *orad)
{
	Point		p_ = {px, py};
	Point	   *center = &p_;
	CIRCLE		result_;
	CIRCLE	   *result = &result_;

	pg_geo_errflag = 0;
	result->center.x = center->x;
	result->center.y = center->y;
	result->radius = radius;

	*ocx = result->center.x;
	*ocy = result->center.y;
	*orad = result->radius;
	return pg_geo_errflag;
}

int
pg_line_vertical(LINE_ARGS1)
{
	LINE_LOCALS1;

	return FPzero(line->B);
}

int
pg_line_horizontal(LINE_ARGS1)
{
	LINE_LOCALS1;

	return FPzero(line->A);
}

int
pg_line_perp(LINE_ARGS2, int *result)
{
	LINE_LOCALS2;

	pg_geo_errflag = 0;
	if (FPzero(l1->A))
	{
		*result = FPzero(l2->B);
		return pg_geo_errflag;
	}
	if (FPzero(l2->A))
	{
		*result = FPzero(l1->B);
		return pg_geo_errflag;
	}
	if (FPzero(l1->B))
	{
		*result = FPzero(l2->A);
		return pg_geo_errflag;
	}
	if (FPzero(l2->B))
	{
		*result = FPzero(l1->A);
		return pg_geo_errflag;
	}

	*result = FPeq(float8_div(float8_mul(l1->A, l2->A),
							  float8_mul(l1->B, l2->B)), -1.0);
	return pg_geo_errflag;
}

int
pg_line_eq(LINE_ARGS2, int *result)
{
	LINE_LOCALS2;
	float8		ratio;

	pg_geo_errflag = 0;
	/* If any NaNs are involved, insist on exact equality */
	if (unlikely(isnan(l1->A) || isnan(l1->B) || isnan(l1->C) ||
				 isnan(l2->A) || isnan(l2->B) || isnan(l2->C)))
	{
		*result = (float8_eq(l1->A, l2->A) &&
				   float8_eq(l1->B, l2->B) &&
				   float8_eq(l1->C, l2->C));
		return pg_geo_errflag;
	}

	/* Otherwise, lines whose parameters are proportional are the same */
	if (!FPzero(l2->A))
		ratio = float8_div(l1->A, l2->A);
	else if (!FPzero(l2->B))
		ratio = float8_div(l1->B, l2->B);
	else if (!FPzero(l2->C))
		ratio = float8_div(l1->C, l2->C);
	else
		ratio = 1.0;

	*result = (FPeq(l1->A, float8_mul(ratio, l2->A)) &&
			   FPeq(l1->B, float8_mul(ratio, l2->B)) &&
			   FPeq(l1->C, float8_mul(ratio, l2->C)));
	return pg_geo_errflag;
}

int
pg_line_construct_pp(double x1, double y1, double x2, double y2,
					 double *out3)
{
	PT_LOCALS;
	LINE		result_;
	LINE	   *result = &result_;

	pg_geo_errflag = 0;
	if (point_eq_point(pt1, pt2))
	{
		/* ereport(ERROR, ERRCODE_INVALID_PARAMETER_VALUE, "invalid line
		 * specification: must be two distinct points") -- noreturn ->
		 * flag 4 + return (shim; see extension header comment). */
		if (pg_geo_errflag == 0)
			pg_geo_errflag = 4;
		return pg_geo_errflag;
	}

	line_construct(result, pt1, point_sl(pt1, pt2));

	out3[0] = result->A;
	out3[1] = result->B;
	out3[2] = result->C;
	return pg_geo_errflag;
}

int
pg_on_pl(double px, double py, LINE_ARGS1, int *result)
{
	Point		p_ = {px, py};
	Point	   *pt = &p_;
	LINE_LOCALS1;

	pg_geo_errflag = 0;
	*result = line_contain_point(line, pt);
	return pg_geo_errflag;
}

int
pg_on_ps(double px, double py, LSEG_ARGS1, int *result)
{
	Point		p_ = {px, py};
	Point	   *pt = &p_;
	LSEG_LOCALS1;

	pg_geo_errflag = 0;
	*result = lseg_contain_point(lseg, pt);
	return pg_geo_errflag;
}

int
pg_on_sl(LSEG_ARGS1, LINE_ARGS1, int *result)
{
	LSEG_LOCALS1;
	LINE_LOCALS1;

	pg_geo_errflag = 0;
	*result = (line_contain_point(line, &lseg->p[0]) &&
			   line_contain_point(line, &lseg->p[1]));
	return pg_geo_errflag;
}

int
pg_on_sb(LSEG_ARGS1, BOX_ARGS2_B, int *result)
{
	LSEG_LOCALS1;
	BOX			bb_ = {{bhx, bhy}, {blx, bly}};
	BOX		   *box = &bb_;

	pg_geo_errflag = 0;
	*result = box_contain_lseg(box, lseg);
	return pg_geo_errflag;
}

int
pg_inter_sb(LSEG_ARGS1, BOX_ARGS2_B, int *result)
{
	LSEG_LOCALS1;
	BOX			bb_ = {{bhx, bhy}, {blx, bly}};
	BOX		   *box = &bb_;

	pg_geo_errflag = 0;
	*result = box_interpt_lseg(NULL, box, lseg);
	return pg_geo_errflag;
}

/* =====================================================================
 * EXTENSION 3 (2026-07-30, geo-varlena slice: PATH / POLYGON header and
 * boundbox family).
 *
 * Provenance:
 *   - src/backend/utils/adt/geo_ops.c @ postgres/postgres REL_18_STABLE
 *     (fetched 2026-07-30)
 *   - PATH / POLYGON layouts from src/include/utils/geo_decls.h @ same
 *     ref (int32 npts; int32 closed; int32 dummy pad / int32 npts; BOX
 *     boundbox; then the flexible Point array).
 *
 * Functions copied, bodies verbatim, renamed with pg_ prefix:
 *   path_n_lt, path_n_gt, path_n_eq, path_n_le, path_n_ge,
 *   path_isclosed, path_isopen, path_npoints,
 *   poly_left, poly_overleft, poly_right, poly_overright, poly_below,
 *   poly_overbelow, poly_above, poly_overabove, poly_same,
 *   plist_same (static), poly_npoints, poly_box.
 *
 * Shims (plumbing only, never logic):
 *   - PG_GETARG_PATH_P / PG_GETARG_POLYGON_P (detoast + unpack plumbing)
 *     -> the wrapper takes the header fields / point coordinate arrays
 *     the verbatim body reads and stages them into stack PATH_S / POLY_S
 *     structs mirroring the geo_decls.h layouts minus the varlena word.
 *     This is the TRUSTED-BUILDER fence: the harness feeds both sides
 *     field-identical images; only the Rust side parses a real varlena.
 *   - PG_FREE_IF_COPY -> no-op (memory plumbing).
 *   - PG_RETURN_BOOL -> int 0/1 (BoolGetDatum collapses any nonzero
 *     int32 to 1, preserved with ? 1 : 0); PG_RETURN_INT32 -> int.
 *   - poly_box's palloc'd BOX result -> caller out4 (hx,hy,lx,ly).
 * POLY_CAP bounds the staged point arrays (harness cells use n <= 4).
 */

#define POLY_CAP 4

typedef struct
{
	int32		npts;
	int32		closed;			/* is this a closed polygon? */
} PATH_S;

typedef struct
{
	int32		npts;
	BOX			boundbox;
	Point		p[POLY_CAP];
} POLY_S;

/* geo_ops.c plist_same, body verbatim */
static bool
pg_plist_same(int npts, Point *p1, Point *p2)
{
	int			i,
				ii,
				j;

	/* find match for first point */
	for (i = 0; i < npts; i++)
	{
		if (point_eq_point(&p2[i], &p1[0]))
		{

			/* match found? then look forward through remaining points */
			for (ii = 1, j = i + 1; ii < npts; ii++, j++)
			{
				if (j >= npts)
					j = 0;
				if (!point_eq_point(&p2[j], &p1[ii]))
					break;
			}
			if (ii == npts)
				return true;

			/* match not found forwards? then look backwards */
			for (ii = 1, j = i - 1; ii < npts; ii++, j--)
			{
				if (j < 0)
					j = (npts - 1);
				if (!point_eq_point(&p2[j], &p1[ii]))
					break;
			}
			if (ii == npts)
				return true;
		}
	}

	return false;
}

/* stage a POLY_S from harness fields (shim, not Postgres code) */
static void
pg_poly_stage(POLY_S *poly, int npts, double hx, double hy, double lx,
			  double ly, const double *xy)
{
	int			k;

	poly->npts = npts;
	poly->boundbox.high.x = hx;
	poly->boundbox.high.y = hy;
	poly->boundbox.low.x = lx;
	poly->boundbox.low.y = ly;
	for (k = 0; xy != NULL && k < npts && k < POLY_CAP; k++)
	{
		poly->p[k].x = xy[2 * k];
		poly->p[k].y = xy[2 * k + 1];
	}
}

/* path_n_lt .. path_n_ge, bodies verbatim on staged PATH_S */
int
pg_path_n_lt(int n1, int n2)
{
	PATH_S		p1_ = {n1, 0};
	PATH_S		p2_ = {n2, 0};
	PATH_S	   *p1 = &p1_;
	PATH_S	   *p2 = &p2_;

	return (p1->npts < p2->npts) ? 1 : 0;
}

int
pg_path_n_gt(int n1, int n2)
{
	PATH_S		p1_ = {n1, 0};
	PATH_S		p2_ = {n2, 0};
	PATH_S	   *p1 = &p1_;
	PATH_S	   *p2 = &p2_;

	return (p1->npts > p2->npts) ? 1 : 0;
}

int
pg_path_n_eq(int n1, int n2)
{
	PATH_S		p1_ = {n1, 0};
	PATH_S		p2_ = {n2, 0};
	PATH_S	   *p1 = &p1_;
	PATH_S	   *p2 = &p2_;

	return (p1->npts == p2->npts) ? 1 : 0;
}

int
pg_path_n_le(int n1, int n2)
{
	PATH_S		p1_ = {n1, 0};
	PATH_S		p2_ = {n2, 0};
	PATH_S	   *p1 = &p1_;
	PATH_S	   *p2 = &p2_;

	return (p1->npts <= p2->npts) ? 1 : 0;
}

int
pg_path_n_ge(int n1, int n2)
{
	PATH_S		p1_ = {n1, 0};
	PATH_S		p2_ = {n2, 0};
	PATH_S	   *p1 = &p1_;
	PATH_S	   *p2 = &p2_;

	return (p1->npts >= p2->npts) ? 1 : 0;
}

int
pg_path_isclosed(int closed)
{
	PATH_S		path_ = {0, closed};
	PATH_S	   *path = &path_;

	return (path->closed) ? 1 : 0;
}

int
pg_path_isopen(int closed)
{
	PATH_S		path_ = {0, closed};
	PATH_S	   *path = &path_;

	return (!path->closed) ? 1 : 0;
}

int
pg_path_npoints(int npts)
{
	PATH_S		path_ = {npts, 0};
	PATH_S	   *path = &path_;

	return path->npts;
}

/* poly position predicates: boundbox-only exact compares, verbatim */
int
pg_poly_left(double ahx, double ahy, double alx, double aly,
			 double bhx, double bhy, double blx, double bly)
{
	POLY_S		a_,
				b_;
	POLY_S	   *polya = &a_;
	POLY_S	   *polyb = &b_;
	bool		result;

	pg_poly_stage(&a_, 0, ahx, ahy, alx, aly, NULL);
	pg_poly_stage(&b_, 0, bhx, bhy, blx, bly, NULL);

	result = polya->boundbox.high.x < polyb->boundbox.low.x;

	return result ? 1 : 0;
}

int
pg_poly_overleft(double ahx, double ahy, double alx, double aly,
				 double bhx, double bhy, double blx, double bly)
{
	POLY_S		a_,
				b_;
	POLY_S	   *polya = &a_;
	POLY_S	   *polyb = &b_;
	bool		result;

	pg_poly_stage(&a_, 0, ahx, ahy, alx, aly, NULL);
	pg_poly_stage(&b_, 0, bhx, bhy, blx, bly, NULL);

	result = polya->boundbox.high.x <= polyb->boundbox.high.x;

	return result ? 1 : 0;
}

int
pg_poly_right(double ahx, double ahy, double alx, double aly,
			  double bhx, double bhy, double blx, double bly)
{
	POLY_S		a_,
				b_;
	POLY_S	   *polya = &a_;
	POLY_S	   *polyb = &b_;
	bool		result;

	pg_poly_stage(&a_, 0, ahx, ahy, alx, aly, NULL);
	pg_poly_stage(&b_, 0, bhx, bhy, blx, bly, NULL);

	result = polya->boundbox.low.x > polyb->boundbox.high.x;

	return result ? 1 : 0;
}

int
pg_poly_overright(double ahx, double ahy, double alx, double aly,
				  double bhx, double bhy, double blx, double bly)
{
	POLY_S		a_,
				b_;
	POLY_S	   *polya = &a_;
	POLY_S	   *polyb = &b_;
	bool		result;

	pg_poly_stage(&a_, 0, ahx, ahy, alx, aly, NULL);
	pg_poly_stage(&b_, 0, bhx, bhy, blx, bly, NULL);

	result = polya->boundbox.low.x >= polyb->boundbox.low.x;

	return result ? 1 : 0;
}

int
pg_poly_below(double ahx, double ahy, double alx, double aly,
			  double bhx, double bhy, double blx, double bly)
{
	POLY_S		a_,
				b_;
	POLY_S	   *polya = &a_;
	POLY_S	   *polyb = &b_;
	bool		result;

	pg_poly_stage(&a_, 0, ahx, ahy, alx, aly, NULL);
	pg_poly_stage(&b_, 0, bhx, bhy, blx, bly, NULL);

	result = polya->boundbox.high.y < polyb->boundbox.low.y;

	return result ? 1 : 0;
}

int
pg_poly_overbelow(double ahx, double ahy, double alx, double aly,
				  double bhx, double bhy, double blx, double bly)
{
	POLY_S		a_,
				b_;
	POLY_S	   *polya = &a_;
	POLY_S	   *polyb = &b_;
	bool		result;

	pg_poly_stage(&a_, 0, ahx, ahy, alx, aly, NULL);
	pg_poly_stage(&b_, 0, bhx, bhy, blx, bly, NULL);

	result = polya->boundbox.high.y <= polyb->boundbox.high.y;

	return result ? 1 : 0;
}

int
pg_poly_above(double ahx, double ahy, double alx, double aly,
			  double bhx, double bhy, double blx, double bly)
{
	POLY_S		a_,
				b_;
	POLY_S	   *polya = &a_;
	POLY_S	   *polyb = &b_;
	bool		result;

	pg_poly_stage(&a_, 0, ahx, ahy, alx, aly, NULL);
	pg_poly_stage(&b_, 0, bhx, bhy, blx, bly, NULL);

	result = polya->boundbox.low.y > polyb->boundbox.high.y;

	return result ? 1 : 0;
}

int
pg_poly_overabove(double ahx, double ahy, double alx, double aly,
				  double bhx, double bhy, double blx, double bly)
{
	POLY_S		a_,
				b_;
	POLY_S	   *polya = &a_;
	POLY_S	   *polyb = &b_;
	bool		result;

	pg_poly_stage(&a_, 0, ahx, ahy, alx, aly, NULL);
	pg_poly_stage(&b_, 0, bhx, bhy, blx, bly, NULL);

	result = polya->boundbox.low.y >= polyb->boundbox.low.y;

	return result ? 1 : 0;
}

/* poly_same, body verbatim (boundboxes not read; staged zero) */
int
pg_poly_same(int na, int nb, const double *pa, const double *pb)
{
	POLY_S		a_,
				b_;
	POLY_S	   *polya = &a_;
	POLY_S	   *polyb = &b_;
	bool		result;

	pg_poly_stage(&a_, na, 0.0, 0.0, 0.0, 0.0, pa);
	pg_poly_stage(&b_, nb, 0.0, 0.0, 0.0, 0.0, pb);

	if (polya->npts != polyb->npts)
		result = false;
	else
		result = pg_plist_same(polya->npts, polya->p, polyb->p);

	return result ? 1 : 0;
}

int
pg_poly_npoints(int npts)
{
	POLY_S		poly_;
	POLY_S	   *poly = &poly_;

	pg_poly_stage(&poly_, npts, 0.0, 0.0, 0.0, 0.0, NULL);
	poly_.npts = npts;

	return poly->npts;
}

int
pg_poly_box(double hx, double hy, double lx, double ly, double *out4)
{
	POLY_S		poly_;
	POLY_S	   *poly = &poly_;
	BOX			box_;
	BOX		   *box = &box_;

	pg_poly_stage(&poly_, 0, hx, hy, lx, ly, NULL);

	*box = poly->boundbox;

	out4[0] = box->high.x;
	out4[1] = box->high.y;
	out4[2] = box->low.x;
	out4[3] = box->low.y;
	return 0;
}

/* =====================================================================
 * EXTENSION 4 (2026-07-30, geo-wire slice: the send/recv binary I/O
 * family).
 *
 * Provenance:
 *   - src/backend/utils/adt/geo_ops.c @ postgres/postgres REL_18_STABLE
 *     (fetched 2026-07-30): point_recv, point_send, box_recv, box_send,
 *     lseg_recv, lseg_send, line_recv, line_send, circle_recv,
 *     circle_send, path_send, poly_send — bodies verbatim, pg_ prefix +
 *     _w suffix on the wrappers.
 *   - pq_getmsgint64/pq_getmsgfloat8/pq_sendfloat8/pq_sendbyte/
 *     pq_sendint32 semantics from src/backend/libpq/pqformat.c @ same
 *     ref: big-endian wire order, float8 <-> uint64 bit pun.
 *
 * Shims (plumbing only, never logic):
 *   - StringInfo -> PQ_BUF: a caller-provided fixed frame (recv: exact
 *     wire frame, cursor walk; send: 64-byte out image + running length).
 *     The pq_* helpers below are reimplemented over PQ_BUF with the wire
 *     semantics above (byte order and pun ARE part of the theorem; the
 *     StringInfo growth machinery is not). Recv harnesses feed EXACT
 *     frames, so the insufficient-data ereport path is out of proof
 *     (fenced; the Rust arm's error path is likewise unreachable there).
 *   - pq_begintypsend's 4-byte length placeholder + pq_endtypsend's
 *     SET_VARSIZE -> pg_pq_begintypsend/pg_pq_endtypsend over PQ_BUF
 *     (little-endian 4B header word = total << 2, as SET_VARSIZE_4B).
 *   - palloc'd result structs -> caller out-params (values untouched).
 *   - ereport(ERROR, 22P03 ...) in line_recv/circle_recv ->
 *     pg_geo_errflag = 5 + immediate return (new flag value, mapped to
 *     ERRCODE_INVALID_BINARY_REPRESENTATION by the harnesses).
 */

#include <string.h>

typedef struct
{
	const unsigned char *in;	/* recv frame */
	int			cursor;
	unsigned char out[64];		/* send image (varlena header + payload) */
	int			olen;
} PQ_BUF;

static uint64
pg_pq_getmsguint64(PQ_BUF *buf)
{
	/* unrolled (loop-free: the tight harness unwind bound belongs to the
	 * mcx registry, not this plumbing) */
	const unsigned char *p = buf->in + buf->cursor;
	uint64		v = ((uint64) p[0] << 56) | ((uint64) p[1] << 48) |
		((uint64) p[2] << 40) | ((uint64) p[3] << 32) |
		((uint64) p[4] << 24) | ((uint64) p[5] << 16) |
		((uint64) p[6] << 8) | (uint64) p[7];

	buf->cursor += 8;
	return v;
}

static double
pg_pq_getmsgfloat8(PQ_BUF *buf)
{
	union
	{
		double		f;
		uint64		i;
	}			swap;

	swap.i = pg_pq_getmsguint64(buf);
	return swap.f;
}

static int
pg_pq_getmsgbyte(PQ_BUF *buf)
{
	return (int) buf->in[buf->cursor++];
}

static int32
pg_pq_getmsgint32(PQ_BUF *buf)
{
	const unsigned char *p = buf->in + buf->cursor;
	uint32		v = ((uint32) p[0] << 24) | ((uint32) p[1] << 16) |
		((uint32) p[2] << 8) | (uint32) p[3];

	buf->cursor += 4;
	return (int32) v;
}

static void
pg_pq_begintypsend(PQ_BUF *buf)
{
	/* four-byte length placeholder (pq_begintypsend) */
	buf->out[0] = buf->out[1] = buf->out[2] = buf->out[3] = 0;
	buf->olen = 4;
}

static void
pg_pq_sendbyte(PQ_BUF *buf, unsigned char b)
{
	buf->out[buf->olen++] = b;
}

static void
pg_pq_sendint32(PQ_BUF *buf, uint32 v)
{
	buf->out[buf->olen++] = (unsigned char) (v >> 24);
	buf->out[buf->olen++] = (unsigned char) (v >> 16);
	buf->out[buf->olen++] = (unsigned char) (v >> 8);
	buf->out[buf->olen++] = (unsigned char) v;
}

static void
pg_pq_sendfloat8(PQ_BUF *buf, double f)
{
	union
	{
		double		f;
		uint64		i;
	}			swap;

	swap.f = f;
	buf->out[buf->olen++] = (unsigned char) (swap.i >> 56);
	buf->out[buf->olen++] = (unsigned char) (swap.i >> 48);
	buf->out[buf->olen++] = (unsigned char) (swap.i >> 40);
	buf->out[buf->olen++] = (unsigned char) (swap.i >> 32);
	buf->out[buf->olen++] = (unsigned char) (swap.i >> 24);
	buf->out[buf->olen++] = (unsigned char) (swap.i >> 16);
	buf->out[buf->olen++] = (unsigned char) (swap.i >> 8);
	buf->out[buf->olen++] = (unsigned char) swap.i;
}

static void
pg_pq_endtypsend(PQ_BUF *buf)
{
	/* SET_VARSIZE(result, buf->len): 4B LE header word = total << 2 */
	uint32		hdr = ((uint32) buf->olen) << 2;

	buf->out[0] = (unsigned char) hdr;
	buf->out[1] = (unsigned char) (hdr >> 8);
	buf->out[2] = (unsigned char) (hdr >> 16);
	buf->out[3] = (unsigned char) (hdr >> 24);
}

/* ---- recv wrappers: geo_ops.c bodies verbatim ---- */

int
pg_point_recv_w(const unsigned char *in, double *ox, double *oy)
{
	PQ_BUF		buf_ = {in, 0, {0}, 0};
	PQ_BUF	   *buf = &buf_;
	Point		point_;
	Point	   *point = &point_;

	pg_geo_errflag = 0;
	point->x = pg_pq_getmsgfloat8(buf);
	point->y = pg_pq_getmsgfloat8(buf);
	*ox = point->x;
	*oy = point->y;
	return pg_geo_errflag;
}

int
pg_box_recv_w(const unsigned char *in, double *out4)
{
	PQ_BUF		buf_ = {in, 0, {0}, 0};
	PQ_BUF	   *buf = &buf_;
	BOX			box_;
	BOX		   *box = &box_;
	float8		x,
				y;

	pg_geo_errflag = 0;

	box->high.x = pg_pq_getmsgfloat8(buf);
	box->high.y = pg_pq_getmsgfloat8(buf);
	box->low.x = pg_pq_getmsgfloat8(buf);
	box->low.y = pg_pq_getmsgfloat8(buf);

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

	out4[0] = box->high.x;
	out4[1] = box->high.y;
	out4[2] = box->low.x;
	out4[3] = box->low.y;
	return pg_geo_errflag;
}

int
pg_lseg_recv_w(const unsigned char *in, double *out4)
{
	PQ_BUF		buf_ = {in, 0, {0}, 0};
	PQ_BUF	   *buf = &buf_;
	LSEG		lseg_;
	LSEG	   *lseg = &lseg_;

	pg_geo_errflag = 0;

	lseg->p[0].x = pg_pq_getmsgfloat8(buf);
	lseg->p[0].y = pg_pq_getmsgfloat8(buf);
	lseg->p[1].x = pg_pq_getmsgfloat8(buf);
	lseg->p[1].y = pg_pq_getmsgfloat8(buf);

	out4[0] = lseg->p[0].x;
	out4[1] = lseg->p[0].y;
	out4[2] = lseg->p[1].x;
	out4[3] = lseg->p[1].y;
	return pg_geo_errflag;
}

int
pg_line_recv_w(const unsigned char *in, double *out3)
{
	PQ_BUF		buf_ = {in, 0, {0}, 0};
	PQ_BUF	   *buf = &buf_;
	LINE		line_;
	LINE	   *line = &line_;

	pg_geo_errflag = 0;

	line->A = pg_pq_getmsgfloat8(buf);
	line->B = pg_pq_getmsgfloat8(buf);
	line->C = pg_pq_getmsgfloat8(buf);

	if (FPzero(line->A) && FPzero(line->B))
	{
		pg_geo_errflag = 5;		/* ereport 22P03 */
		return pg_geo_errflag;
	}

	out3[0] = line->A;
	out3[1] = line->B;
	out3[2] = line->C;
	return pg_geo_errflag;
}

int
pg_circle_recv_w(const unsigned char *in, double *out3)
{
	PQ_BUF		buf_ = {in, 0, {0}, 0};
	PQ_BUF	   *buf = &buf_;
	CIRCLE		circle_;
	CIRCLE	   *circle = &circle_;

	pg_geo_errflag = 0;

	circle->center.x = pg_pq_getmsgfloat8(buf);
	circle->center.y = pg_pq_getmsgfloat8(buf);
	circle->radius = pg_pq_getmsgfloat8(buf);

	/* We have to accept NaN. */
	if (circle->radius < 0.0)
	{
		pg_geo_errflag = 5;		/* ereport 22P03 */
		return pg_geo_errflag;
	}

	out3[0] = circle->center.x;
	out3[1] = circle->center.y;
	out3[2] = circle->radius;
	return pg_geo_errflag;
}

/* ---- send wrappers: geo_ops.c bodies verbatim ---- */

int
pg_point_send_w(double x, double y, unsigned char *out, int *olen)
{
	Point		pt_ = {x, y};
	Point	   *pt = &pt_;
	PQ_BUF		buf_;
	PQ_BUF	   *buf = &buf_;
	int			k;

	pg_geo_errflag = 0;
	pg_pq_begintypsend(buf);
	pg_pq_sendfloat8(buf, pt->x);
	pg_pq_sendfloat8(buf, pt->y);
	pg_pq_endtypsend(buf);
	memcpy(out, buf->out, 64);	/* constant-size: loop-free */
	*olen = buf->olen;
	return pg_geo_errflag;
}

int
pg_box_send_w(double hx, double hy, double lx, double ly,
			  unsigned char *out, int *olen)
{
	BOX			box_ = {{hx, hy}, {lx, ly}};
	BOX		   *box = &box_;
	PQ_BUF		buf_;
	PQ_BUF	   *buf = &buf_;
	int			k;

	pg_geo_errflag = 0;
	pg_pq_begintypsend(buf);
	pg_pq_sendfloat8(buf, box->high.x);
	pg_pq_sendfloat8(buf, box->high.y);
	pg_pq_sendfloat8(buf, box->low.x);
	pg_pq_sendfloat8(buf, box->low.y);
	pg_pq_endtypsend(buf);
	memcpy(out, buf->out, 64);	/* constant-size: loop-free */
	*olen = buf->olen;
	return pg_geo_errflag;
}

int
pg_lseg_send_w(double x1, double y1, double x2, double y2,
			   unsigned char *out, int *olen)
{
	LSEG		ls_ = {{{x1, y1}, {x2, y2}}};
	LSEG	   *ls = &ls_;
	PQ_BUF		buf_;
	PQ_BUF	   *buf = &buf_;
	int			k;

	pg_geo_errflag = 0;
	pg_pq_begintypsend(buf);
	pg_pq_sendfloat8(buf, ls->p[0].x);
	pg_pq_sendfloat8(buf, ls->p[0].y);
	pg_pq_sendfloat8(buf, ls->p[1].x);
	pg_pq_sendfloat8(buf, ls->p[1].y);
	pg_pq_endtypsend(buf);
	memcpy(out, buf->out, 64);	/* constant-size: loop-free */
	*olen = buf->olen;
	return pg_geo_errflag;
}

int
pg_line_send_w(double A, double B, double C, unsigned char *out, int *olen)
{
	LINE		line_ = {A, B, C};
	LINE	   *line = &line_;
	PQ_BUF		buf_;
	PQ_BUF	   *buf = &buf_;
	int			k;

	pg_geo_errflag = 0;
	pg_pq_begintypsend(buf);
	pg_pq_sendfloat8(buf, line->A);
	pg_pq_sendfloat8(buf, line->B);
	pg_pq_sendfloat8(buf, line->C);
	pg_pq_endtypsend(buf);
	memcpy(out, buf->out, 64);	/* constant-size: loop-free */
	*olen = buf->olen;
	return pg_geo_errflag;
}

int
pg_circle_send_w(double cx, double cy, double r, unsigned char *out, int *olen)
{
	CIRCLE		circle_ = {{cx, cy}, r};
	CIRCLE	   *circle = &circle_;
	PQ_BUF		buf_;
	PQ_BUF	   *buf = &buf_;
	int			k;

	pg_geo_errflag = 0;
	pg_pq_begintypsend(buf);
	pg_pq_sendfloat8(buf, circle->center.x);
	pg_pq_sendfloat8(buf, circle->center.y);
	pg_pq_sendfloat8(buf, circle->radius);
	pg_pq_endtypsend(buf);
	memcpy(out, buf->out, 64);	/* constant-size: loop-free */
	*olen = buf->olen;
	return pg_geo_errflag;
}

int
pg_path_send_w(int closed, int npts, const double *xy,
			   unsigned char *out, int *olen)
{
	PATH_S		path_ = {npts, closed};
	PATH_S	   *path = &path_;
	Point		p[POLY_CAP];
	PQ_BUF		buf_;
	PQ_BUF	   *buf = &buf_;
	int32		i;
	int			k;

	for (k = 0; k < npts && k < POLY_CAP; k++)
	{
		p[k].x = xy[2 * k];
		p[k].y = xy[2 * k + 1];
	}

	pg_geo_errflag = 0;
	pg_pq_begintypsend(buf);
	pg_pq_sendbyte(buf, path->closed ? 1 : 0);
	pg_pq_sendint32(buf, (uint32) path->npts);
	for (i = 0; i < path->npts; i++)
	{
		pg_pq_sendfloat8(buf, p[i].x);
		pg_pq_sendfloat8(buf, p[i].y);
	}
	pg_pq_endtypsend(buf);
	memcpy(out, buf->out, 64);	/* constant-size: loop-free */
	*olen = buf->olen;
	return pg_geo_errflag;
}

int
pg_poly_send_w(int npts, const double *xy, unsigned char *out, int *olen)
{
	POLY_S		poly_;
	POLY_S	   *poly = &poly_;
	PQ_BUF		buf_;
	PQ_BUF	   *buf = &buf_;
	int32		i;
	int			k;

	pg_poly_stage(&poly_, npts, 0.0, 0.0, 0.0, 0.0, xy);

	pg_geo_errflag = 0;
	pg_pq_begintypsend(buf);
	pg_pq_sendint32(buf, (uint32) poly->npts);
	for (i = 0; i < poly->npts; i++)
	{
		pg_pq_sendfloat8(buf, poly->p[i].x);
		pg_pq_sendfloat8(buf, poly->p[i].y);
	}
	pg_pq_endtypsend(buf);
	memcpy(out, buf->out, 64);	/* constant-size: loop-free */
	*olen = buf->olen;
	return pg_geo_errflag;
}

/* =====================================================================
 * EXTENSION 5 (2026-07-30, path/poly PLANE slice: the scalar-verdict
 * planes of the ladder rows whose general bodies are ratified walls).
 *
 * Provenance: src/backend/utils/adt/geo_ops.c @ REL_18_STABLE (fetched
 * 2026-07-30): path_area, path_distance, path_inter, poly_contain_poly,
 * poly_overlap_internal, poly_contain, poly_contained, poly_overlap —
 * bodies verbatim, pg_ prefix + _w wrappers on staged PATH_S/POLY_S
 * (trusted-builder fence, as EXTENSION 3).
 *
 * OUT-OF-PLANE TRAPS (literal-planes law): the deep helpers that are
 * structurally unreachable inside every plane harness —
 * lseg_closept_lseg (path_distance min body), lseg_inside_poly
 * (poly_contain containment walk), point_inside (poly_overlap interior
 * test) — are NOT vendored; each is a trap stub setting
 * pg_geo_errflag = 99. A harness whose plane leaks into the deep arm
 * fails its cerr assertion LOUDLY instead of proving against a wrong
 * model. PG_RETURN_NULL -> *isnull out-param.
 */

/* out-of-plane traps (NOT Postgres code) */
static float8
lseg_closept_lseg(Point *result, LSEG *on_lseg, LSEG *to_lseg)
{
	pg_geo_errflag = 99;
	return 0.0;
}

static bool
lseg_inside_poly(Point *a, Point *b, POLY_S *poly, int start)
{
	pg_geo_errflag = 99;
	return true;
}

static bool
point_inside(Point *p, int npts, Point *plist)
{
	pg_geo_errflag = 99;
	return true;
}

/* path_area, body verbatim */
int
pg_path_area_w(int closed, int npts, const double *xy, double *out, int *isnull)
{
	PATH_S		path_ = {npts, closed};
	PATH_S	   *path = &path_;
	Point		p[POLY_CAP];
	float8		area = 0.0;
	int			i,
				j;
	int			k;

	for (k = 0; xy != NULL && k < npts && k < POLY_CAP; k++)
	{
		p[k].x = xy[2 * k];
		p[k].y = xy[2 * k + 1];
	}

	pg_geo_errflag = 0;
	*isnull = 0;

	if (!path->closed)
	{
		*isnull = 1;			/* PG_RETURN_NULL() */
		return pg_geo_errflag;
	}

	for (i = 0; i < path->npts; i++)
	{
		j = (i + 1) % path->npts;
		area = float8_pl(area, float8_mul(p[i].x, p[j].y));
		area = float8_mi(area, float8_mul(p[i].y, p[j].x));
	}

	*out = float8_div(fabs(area), 2.0);
	return pg_geo_errflag;
}

/* path_distance, body verbatim (lseg_closept_lseg = out-of-plane trap) */
int
pg_path_distance_w(int closed1, int n1, const double *xy1,
				   int closed2, int n2, const double *xy2,
				   double *out, int *isnull)
{
	PATH_S		p1_ = {n1, closed1};
	PATH_S		p2_ = {n2, closed2};
	PATH_S	   *p1 = &p1_;
	PATH_S	   *p2 = &p2_;
	Point		pa[POLY_CAP];
	Point		pb[POLY_CAP];
	float8		min = 0.0;		/* initialize to keep compiler quiet */
	bool		have_min = false;
	float8		tmp;
	int			i,
				j;
	LSEG		seg1,
				seg2;
	int			k;

	for (k = 0; xy1 != NULL && k < n1 && k < POLY_CAP; k++)
	{
		pa[k].x = xy1[2 * k];
		pa[k].y = xy1[2 * k + 1];
	}
	for (k = 0; xy2 != NULL && k < n2 && k < POLY_CAP; k++)
	{
		pb[k].x = xy2[2 * k];
		pb[k].y = xy2[2 * k + 1];
	}

	pg_geo_errflag = 0;
	*isnull = 0;

	for (i = 0; i < p1->npts; i++)
	{
		int			iprev;

		if (i > 0)
			iprev = i - 1;
		else
		{
			if (!p1->closed)
				continue;
			iprev = p1->npts - 1;	/* include the closure segment */
		}

		for (j = 0; j < p2->npts; j++)
		{
			int			jprev;

			if (j > 0)
				jprev = j - 1;
			else
			{
				if (!p2->closed)
					continue;
				jprev = p2->npts - 1;	/* include the closure segment */
			}

			statlseg_construct(&seg1, &pa[iprev], &pa[i]);
			statlseg_construct(&seg2, &pb[jprev], &pb[j]);

			tmp = lseg_closept_lseg(NULL, &seg1, &seg2);
			if (!have_min || float8_lt(tmp, min))
			{
				min = tmp;
				have_min = true;
			}
		}
	}

	if (!have_min)
	{
		*isnull = 1;			/* PG_RETURN_NULL() */
		return pg_geo_errflag;
	}

	*out = min;
	return pg_geo_errflag;
}

/* path_inter, body verbatim */
int
pg_path_inter_w(int closed1, int n1, const double *xy1,
				int closed2, int n2, const double *xy2, int *result)
{
	PATH_S		p1_ = {n1, closed1};
	PATH_S		p2_ = {n2, closed2};
	PATH_S	   *p1 = &p1_;
	PATH_S	   *p2 = &p2_;
	Point		pa[POLY_CAP];
	Point		pb[POLY_CAP];
	BOX			b1,
				b2;
	int			i,
				j;
	LSEG		seg1,
				seg2;
	int			k;

	for (k = 0; xy1 != NULL && k < n1 && k < POLY_CAP; k++)
	{
		pa[k].x = xy1[2 * k];
		pa[k].y = xy1[2 * k + 1];
	}
	for (k = 0; xy2 != NULL && k < n2 && k < POLY_CAP; k++)
	{
		pb[k].x = xy2[2 * k];
		pb[k].y = xy2[2 * k + 1];
	}

	pg_geo_errflag = 0;

	b1.high.x = b1.low.x = pa[0].x;
	b1.high.y = b1.low.y = pa[0].y;
	for (i = 1; i < p1->npts; i++)
	{
		b1.high.x = float8_max(pa[i].x, b1.high.x);
		b1.high.y = float8_max(pa[i].y, b1.high.y);
		b1.low.x = float8_min(pa[i].x, b1.low.x);
		b1.low.y = float8_min(pa[i].y, b1.low.y);
	}
	b2.high.x = b2.low.x = pb[0].x;
	b2.high.y = b2.low.y = pb[0].y;
	for (i = 1; i < p2->npts; i++)
	{
		b2.high.x = float8_max(pb[i].x, b2.high.x);
		b2.high.y = float8_max(pb[i].y, b2.high.y);
		b2.low.x = float8_min(pb[i].x, b2.low.x);
		b2.low.y = float8_min(pb[i].y, b2.low.y);
	}
	if (!box_ov(&b1, &b2))
	{
		*result = 0;
		return pg_geo_errflag;
	}

	/* pairwise check lseg intersections */
	for (i = 0; i < p1->npts; i++)
	{
		int			iprev;

		if (i > 0)
			iprev = i - 1;
		else
		{
			if (!p1->closed)
				continue;
			iprev = p1->npts - 1;	/* include the closure segment */
		}

		for (j = 0; j < p2->npts; j++)
		{
			int			jprev;

			if (j > 0)
				jprev = j - 1;
			else
			{
				if (!p2->closed)
					continue;
				jprev = p2->npts - 1;	/* include the closure segment */
			}

			statlseg_construct(&seg1, &pa[iprev], &pa[i]);
			statlseg_construct(&seg2, &pb[jprev], &pb[j]);
			if (lseg_interpt_lseg(NULL, &seg1, &seg2))
			{
				*result = 1;
				return pg_geo_errflag;
			}
		}
	}

	/* if we dropped through, no two segs intersected */
	*result = 0;
	return pg_geo_errflag;
}

/* poly_contain_poly, body verbatim (lseg_inside_poly = trap) */
static bool
pg_poly_contain_poly(POLY_S *contains_poly, POLY_S *contained_poly)
{
	int			i;
	LSEG		s;

	if (!box_contain_box(&contains_poly->boundbox, &contained_poly->boundbox))
		return false;

	s.p[0] = contained_poly->p[contained_poly->npts - 1];

	for (i = 0; i < contained_poly->npts; i++)
	{
		s.p[1] = contained_poly->p[i];
		if (!lseg_inside_poly(s.p, s.p + 1, contains_poly, 0))
			return false;
		s.p[0] = s.p[1];
	}

	return true;
}

int
pg_poly_contain_w(int na, const double *bba, const double *pa,
				  int nb, const double *bbb, const double *pb, int *result)
{
	POLY_S		a_,
				b_;

	pg_poly_stage(&a_, na, bba[0], bba[1], bba[2], bba[3], pa);
	pg_poly_stage(&b_, nb, bbb[0], bbb[1], bbb[2], bbb[3], pb);

	pg_geo_errflag = 0;
	*result = pg_poly_contain_poly(&a_, &b_) ? 1 : 0;
	return pg_geo_errflag;
}

int
pg_poly_contained_w(int na, const double *bba, const double *pa,
					int nb, const double *bbb, const double *pb, int *result)
{
	POLY_S		a_,
				b_;

	pg_poly_stage(&a_, na, bba[0], bba[1], bba[2], bba[3], pa);
	pg_poly_stage(&b_, nb, bbb[0], bbb[1], bbb[2], bbb[3], pb);

	pg_geo_errflag = 0;
	/* poly_contained(a, b) = poly_contain_poly(b, a) */
	*result = pg_poly_contain_poly(&b_, &a_) ? 1 : 0;
	return pg_geo_errflag;
}

/* poly_overlap_internal, body verbatim (point_inside = trap) */
static bool
pg_poly_overlap_internal(POLY_S *polya, POLY_S *polyb)
{
	bool		result;

	/* Quick check by bounding box */
	result = box_ov(&polya->boundbox, &polyb->boundbox);

	/*
	 * Brute-force algorithm - try to find intersected edges, if so then
	 * polygons are overlapped else check is one polygon inside other or not
	 * by testing single point of them.
	 */
	if (result)
	{
		int			ia,
					ib;
		LSEG		sa,
					sb;

		/* Init first of polya's edge with last point */
		sa.p[0] = polya->p[polya->npts - 1];
		result = false;

		for (ia = 0; ia < polya->npts && !result; ia++)
		{
			/* Second point of polya's edge is a current one */
			sa.p[1] = polya->p[ia];

			/* Init first of polyb's edge with last point */
			sb.p[0] = polyb->p[polyb->npts - 1];

			for (ib = 0; ib < polyb->npts && !result; ib++)
			{
				sb.p[1] = polyb->p[ib];
				result = lseg_interpt_lseg(NULL, &sa, &sb);
				sb.p[0] = sb.p[1];
			}

			/*
			 * move current endpoint to the first point of next edge
			 */
			sa.p[0] = sa.p[1];
		}

		if (!result)
		{
			result = (point_inside(polya->p, polyb->npts, polyb->p) ||
					  point_inside(polyb->p, polya->npts, polya->p));
		}
	}

	return result;
}

int
pg_poly_overlap_w(int na, const double *bba, const double *pa,
				  int nb, const double *bbb, const double *pb, int *result)
{
	POLY_S		a_,
				b_;

	pg_poly_stage(&a_, na, bba[0], bba[1], bba[2], bba[3], pa);
	pg_poly_stage(&b_, nb, bbb[0], bbb[1], bbb[2], bbb[3], pb);

	pg_geo_errflag = 0;
	*result = pg_poly_overlap_internal(&a_, &b_) ? 1 : 0;
	return pg_geo_errflag;
}

/* box_poly (EXTENSION 5 addendum): body verbatim; the palloc'd POLYGON
 * image -> caller out buffer (104 bytes: 4B varlena header + npts +
 * boundbox + 4 points; SET_VARSIZE = LE total<<2 as in EXTENSION 4).
 * box_construct is the vendored verbatim inline (EXTENSION 2). */
int
pg_box_poly_w(double hx, double hy, double lx, double ly, unsigned char *out)
{
	BOX			box_ = {{hx, hy}, {lx, ly}};
	BOX		   *box = &box_;
	struct
	{
		int32		vl_len_;
		int32		npts;
		BOX			boundbox;
		Point		p[4];
	}			poly_,
			   *poly = &poly_;
	int			size;

	pg_geo_errflag = 0;

	/* map four corners of the box to a polygon */
	size = 40 + sizeof(poly->p[0]) * 4;	/* offsetof(POLYGON, p) + 4 Points */

	poly->npts = 4;

	poly->p[0].x = box->low.x;
	poly->p[0].y = box->low.y;
	poly->p[1].x = box->low.x;
	poly->p[1].y = box->high.y;
	poly->p[2].x = box->high.x;
	poly->p[2].y = box->high.y;
	poly->p[3].x = box->high.x;
	poly->p[3].y = box->low.y;

	box_construct(&poly->boundbox, &box->high, &box->low);

	/* SET_VARSIZE(poly, size): LE 4B header word = total << 2 */
	poly->vl_len_ = (int32) (((uint32) size) << 2);
	memcpy(out, &poly_, 104);
	return pg_geo_errflag;
}
