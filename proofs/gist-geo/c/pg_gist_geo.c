/*
 * Vendored PostgreSQL C: GiST support procedures for 2-D geometric types
 * (gistproc.c family), for the proofs/gist-geo Kani harness crate.
 *
 * Provenance:
 *   - src/backend/access/gist/gistproc.c  @ postgres/postgres REL_18_STABLE
 *     (fetched 2026-07-29): rt_box_union, size_box, box_penalty, adjustBox,
 *     gist_box_leaf_consistent, rtree_internal_consistent,
 *     gist_point_consistent_internal, computeDistance, gist_bbox_distance,
 *     and the wrapper bodies of gist_box_consistent/union/penalty/same,
 *     gist_point_compress/fetch/consistent/distance, gist_poly_compress/
 *     consistent/distance, gist_circle_compress/consistent/distance,
 *     gist_box_distance.  (gist_box_picksplit is NOT vendored — ledger row
 *     2582 is blocked(alloc/sort); sortsupport/z-order is excluded
 *     non-surface.)
 *   - src/backend/access/gist/gistutil.c  @ same ref (fetched 2026-07-29):
 *     gist_translate_cmptype_common switch body.
 *   - src/include/access/stratnum.h       @ same ref: RT*StrategyNumber
 *     constants, verbatim values.
 *   - src/include/utils/float.h           @ same ref: float8_eq/lt/le/gt/ge,
 *     float8_min/max, float8_pl/mi/mul/div, get_float8_infinity/nan,
 *     verbatim (same text as proofs/geo-cmp/c/pg_geo_cmp.c).
 *   - src/backend/utils/adt/float.c       @ same ref: float8_cmp_internal.
 *   - src/include/utils/geo_decls.h       @ same ref: EPSILON + FP* fuzzy
 *     helpers (#ifdef EPSILON arm) and HYPOT.
 *   - src/backend/utils/adt/geo_ops.c     @ same ref: pg_hypot, point_dt,
 *     point_eq_point, box_ov, box_contain_box, and the box operator bodies
 *     (box_left/overleft/... — the one-line comparison expressions), all as
 *     already vendored verbatim in proofs/geo-cmp/c/pg_geo_cmp.c.
 *
 * Shims (plumbing only, never logic):
 *   - PG_FUNCTION_ARGS unwrapping -> plain C signatures.  GISTENTRY inputs
 *     become (page_is_leaf/leafkey ints + key coordinates); BOX/Point/CIRCLE
 *     datums become coordinate doubles (with an explicit *_isnull flag where
 *     the C wrapper checks DatumGetBoxP(..) == NULL); POLYGON queries become
 *     their boundbox coordinates (the only field these functions read — the
 *     varlena detoast itself is fmgr plumbing outside this family's claim;
 *     the Rust side's payload decode IS in-theorem against the harness-built
 *     image).
 *   - palloc'd BOX/Point/GISTENTRY results -> caller out-params, values
 *     untouched (box_center precedent in proofs/geo-cmp).
 *   - DirectFunctionCall2(box_left, ..) etc. -> direct calls of the
 *     pointer-taking helpers below whose bodies are the verbatim geo_ops.c
 *     comparison expressions (the fmgr trampoline is plumbing).
 *   - point_point_distance macro -> point_dt(..) (its exact expansion).
 *   - elog(ERROR, "unrecognized strategy number: %d") / elog(ERROR,
 *     "inconsistent point values") -> first-error-wins global flag
 *     pg_geo_errflag with an immediate return at the exact ereport point
 *     (C aborts via longjmp there).  Flag values:
 *       1 = float overflow, 2 = float underflow, 3 = zero divide (22003
 *           class, as proofs/geo-cmp), 8 = "inconsistent point values",
 *       9 = "unrecognized strategy number", 99 = PROOF TRAP (below).
 *     Once the flag is set downstream values are not compared — harnesses
 *     assert flag/Err verdict parity first, value parity on the clean arm.
 *   - PROOF TRAP (flag 99): pg_gist_point_consistent's PolygonStrategyNumber
 *     and CircleStrategyNumberGroup arms (groups 2/3) are OUT OF PLANE for
 *     this crate (they detoast a polygon / run poly_contain_pt or
 *     circle_contain_pt on leaf hits; ledger row 2179 notes groups 2/3 as a
 *     ladder follow-up).  The vendored switch keeps the arms but they set
 *     flag 99 instead of the body; every harness fences the strategy domain
 *     away from groups 2/3 and a flag of 99 fails the harness LOUDLY (plane
 *     vacuity is a failure, not a silent pass — wave-6 trap-flag pattern).
 *   - NAN pin: CBMC's <math.h> NAN constant carries a non-canonical payload
 *     (measured, proofs/geo-cmp ext lane 2026-07-28); get_float8_nan() is
 *     reachable here through point_dt -> pg_hypot (isnan arm).  NAN is
 *     pinned to the canonical quiet NaN (0x7ff8000000000000) real silicon
 *     produces.  Header constant only — bodies stay verbatim.
 *   - float8 -> double typedef (c.h).
 *
 * NOTE (screened divergence plane, adjudication at solve time):
 * gist_box_same compares coordinates with float8_eq, which is NaN-AWARE
 * (NaN == NaN is true); the shipped Rust fc_gist_box_same uses raw f64 `==`
 * (NaN != NaN).  probe_gist_box_same_nan_plane in src/lib.rs is the
 * expected-FAIL witness for that plane; the eq harness is fenced non-NaN.
 */

#include "../../support/c/pg_proof_shim.h"
#include <math.h>

typedef double float8;
typedef uint16 StrategyNumber;

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

/* ---- src/include/access/stratnum.h, verbatim values ---- */

#define RTLeftStrategyNumber			1
#define RTOverLeftStrategyNumber		2
#define RTOverlapStrategyNumber			3
#define RTOverRightStrategyNumber		4
#define RTRightStrategyNumber			5
#define RTSameStrategyNumber			6
#define RTContainsStrategyNumber		7
#define RTContainedByStrategyNumber		8
#define RTOverBelowStrategyNumber		9
#define RTBelowStrategyNumber			10
#define RTAboveStrategyNumber			11
#define RTOverAboveStrategyNumber		12
#define RTEqualStrategyNumber			18
#define RTLessStrategyNumber			20
#define RTLessEqualStrategyNumber		21
#define RTGreaterStrategyNumber			22
#define RTGreaterEqualStrategyNumber	23
#define RTOldBelowStrategyNumber		29
#define RTOldAboveStrategyNumber		30
#define InvalidStrategy					0

/* CompareType (src/include/nodes/primnodes.h COMPARE_* values, verbatim) */
#define COMPARE_LT		1
#define COMPARE_LE		2
#define COMPARE_EQ		3
#define COMPARE_GE		4
#define COMPARE_GT		5
#define COMPARE_OVERLAP	7
#define COMPARE_CONTAINED_BY 8

/* ---- ereport shim: first-error-wins flag (header comment) ---- */

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

static inline bool
float8_lt(const float8 val1, const float8 val2)
{
	return !isnan(val1) && (isnan(val2) || val1 < val2);
}

static inline bool
float8_le(const float8 val1, const float8 val2)
{
	return isnan(val2) || (!isnan(val1) && val1 <= val2);
}

static inline bool
float8_gt(const float8 val1, const float8 val2)
{
	return !isnan(val2) && (isnan(val1) || val1 > val2);
}

static inline bool
float8_ge(const float8 val1, const float8 val2)
{
	return isnan(val1) || (!isnan(val2) && val1 >= val2);
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

/* ---- src/backend/utils/adt/float.c, verbatim ---- */

static int
float8_cmp_internal(float8 a, float8 b)
{
	if (float8_gt(a, b))
		return 1;
	if (float8_lt(a, b))
		return -1;
	return 0;
}

/*
 * SHIM: canonical-NaN pin (see file header; measured CBMC defect,
 * proofs/geo-cmp precedent — header constant only, bodies verbatim).
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

/* ---- src/include/utils/geo_decls.h, verbatim (#ifdef EPSILON arm) ---- */

#define EPSILON					1.0E-06

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

#define HYPOT(A, B)				pg_hypot(A, B)

/* ---- geo_ops.c pg_hypot, verbatim (ereports -> errflag shim) ---- */

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

/* ---- geo_ops.c static inlines, verbatim ---- */

static inline float8
point_dt(Point *pt1, Point *pt2)
{
	return HYPOT(float8_mi(pt1->x, pt2->x), float8_mi(pt1->y, pt2->y));
}

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
 * geo_ops.c box operators used through DirectFunctionCall2 by the vendored
 * gistproc bodies below.  Bodies = the verbatim one-line comparison
 * expressions of the geo_ops.c fmgr functions (the fmgr trampoline is the
 * shimmed-away plumbing; same text as proofs/geo-cmp).
 */

static bool
box_left(BOX *box1, BOX *box2)
{
	return FPlt(box1->high.x, box2->low.x);
}

static bool
box_overleft(BOX *box1, BOX *box2)
{
	return FPle(box1->high.x, box2->high.x);
}

static bool
box_overlap(BOX *box1, BOX *box2)
{
	return box_ov(box1, box2);
}

static bool
box_overright(BOX *box1, BOX *box2)
{
	return FPge(box1->low.x, box2->low.x);
}

static bool
box_right(BOX *box1, BOX *box2)
{
	return FPgt(box1->low.x, box2->high.x);
}

static bool
box_same(BOX *box1, BOX *box2)
{
	return point_eq_point(&box1->high, &box2->high) &&
		point_eq_point(&box1->low, &box2->low);
}

static bool
box_contain(BOX *box1, BOX *box2)
{
	return box_contain_box(box1, box2);
}

static bool
box_contained(BOX *box1, BOX *box2)
{
	return box_contain_box(box2, box1);
}

static bool
box_overbelow(BOX *box1, BOX *box2)
{
	return FPle(box1->high.y, box2->high.y);
}

static bool
box_below(BOX *box1, BOX *box2)
{
	return FPlt(box1->high.y, box2->low.y);
}

static bool
box_above(BOX *box1, BOX *box2)
{
	return FPgt(box1->low.y, box2->high.y);
}

static bool
box_overabove(BOX *box1, BOX *box2)
{
	return FPge(box1->low.y, box2->low.y);
}

/**************************************************
 * gistproc.c, verbatim bodies
 **************************************************/

/*
 * Calculates union of two boxes, a and b. The result is stored in *n.
 */
static void
rt_box_union(BOX *n, const BOX *a, const BOX *b)
{
	n->high.x = float8_max(a->high.x, b->high.x);
	n->high.y = float8_max(a->high.y, b->high.y);
	n->low.x = float8_min(a->low.x, b->low.x);
	n->low.y = float8_min(a->low.y, b->low.y);
}

/*
 * Size of a BOX for penalty-calculation purposes.
 * The result can be +Infinity, but not NaN.
 */
static float8
size_box(const BOX *box)
{
	/*
	 * Check for zero-width cases.  Note that we define the size of a zero-
	 * by-infinity box as zero.  It's important to special-case this somehow,
	 * as naively multiplying infinity by zero will produce NaN.
	 *
	 * The less-than cases should not happen, but if they do, say "zero".
	 */
	if (float8_le(box->high.x, box->low.x) ||
		float8_le(box->high.y, box->low.y))
		return 0.0;

	/*
	 * We treat NaN as larger than +Infinity, so any distance involving a NaN
	 * and a non-NaN is infinite.  Note the previous check eliminated the
	 * possibility that the low fields are NaNs.
	 */
	if (isnan(box->high.x) || isnan(box->high.y))
		return get_float8_infinity();
	return float8_mul(float8_mi(box->high.x, box->low.x),
					  float8_mi(box->high.y, box->low.y));
}

/*
 * Return amount by which the union of the two boxes is larger than
 * the original BOX's area.  The result can be +Infinity, but not NaN.
 */
static float8
box_penalty(const BOX *original, const BOX *new)
{
	BOX			unionbox;

	rt_box_union(&unionbox, original, new);
	return float8_mi(size_box(&unionbox), size_box(original));
}

/*
 * Increase BOX b to include addon.
 */
static void
adjustBox(BOX *b, const BOX *addon)
{
	if (float8_lt(b->high.x, addon->high.x))
		b->high.x = addon->high.x;
	if (float8_gt(b->low.x, addon->low.x))
		b->low.x = addon->low.x;
	if (float8_lt(b->high.y, addon->high.y))
		b->high.y = addon->high.y;
	if (float8_gt(b->low.y, addon->low.y))
		b->low.y = addon->low.y;
}

/*
 * Leaf-level consistency for boxes: just apply the query operator
 * (DirectFunctionCall2 trampolines -> direct helper calls, bodies above).
 * elog(ERROR) -> errflag 9 + immediate return (longjmp point).
 */
static bool
gist_box_leaf_consistent(BOX *key, BOX *query, StrategyNumber strategy)
{
	bool		retval;

	switch (strategy)
	{
		case RTLeftStrategyNumber:
			retval = box_left(key, query);
			break;
		case RTOverLeftStrategyNumber:
			retval = box_overleft(key, query);
			break;
		case RTOverlapStrategyNumber:
			retval = box_overlap(key, query);
			break;
		case RTOverRightStrategyNumber:
			retval = box_overright(key, query);
			break;
		case RTRightStrategyNumber:
			retval = box_right(key, query);
			break;
		case RTSameStrategyNumber:
			retval = box_same(key, query);
			break;
		case RTContainsStrategyNumber:
			retval = box_contain(key, query);
			break;
		case RTContainedByStrategyNumber:
			retval = box_contained(key, query);
			break;
		case RTOverBelowStrategyNumber:
			retval = box_overbelow(key, query);
			break;
		case RTBelowStrategyNumber:
			retval = box_below(key, query);
			break;
		case RTAboveStrategyNumber:
			retval = box_above(key, query);
			break;
		case RTOverAboveStrategyNumber:
			retval = box_overabove(key, query);
			break;
		default:
			if (pg_geo_errflag == 0)
				pg_geo_errflag = 9;
			return false;
	}
	return retval;
}

/*
 * Internal-page consistency for all these types
 */
static bool
rtree_internal_consistent(BOX *key, BOX *query, StrategyNumber strategy)
{
	bool		retval;

	switch (strategy)
	{
		case RTLeftStrategyNumber:
			retval = !box_overright(key, query);
			break;
		case RTOverLeftStrategyNumber:
			retval = !box_right(key, query);
			break;
		case RTOverlapStrategyNumber:
			retval = box_overlap(key, query);
			break;
		case RTOverRightStrategyNumber:
			retval = !box_left(key, query);
			break;
		case RTRightStrategyNumber:
			retval = !box_overleft(key, query);
			break;
		case RTSameStrategyNumber:
		case RTContainsStrategyNumber:
			retval = box_contain(key, query);
			break;
		case RTContainedByStrategyNumber:
			retval = box_overlap(key, query);
			break;
		case RTOverBelowStrategyNumber:
			retval = !box_above(key, query);
			break;
		case RTBelowStrategyNumber:
			retval = !box_overabove(key, query);
			break;
		case RTAboveStrategyNumber:
			retval = !box_overbelow(key, query);
			break;
		case RTOverAboveStrategyNumber:
			retval = !box_below(key, query);
			break;
		default:
			if (pg_geo_errflag == 0)
				pg_geo_errflag = 9;
			return false;
	}
	return retval;
}

/* gistproc.c computeDistance, verbatim (point_point_distance -> point_dt;
 * elog(ERROR, "inconsistent point values") -> errflag 8 + return). */
static float8
computeDistance(bool isLeaf, BOX *box, Point *point)
{
	float8		result = 0.0;

	if (isLeaf)
	{
		/* simple point to point distance */
		result = point_dt(point, &box->low);
	}
	else if (point->x <= box->high.x && point->x >= box->low.x &&
			 point->y <= box->high.y && point->y >= box->low.y)
	{
		/* point inside the box */
		result = 0.0;
	}
	else if (point->x <= box->high.x && point->x >= box->low.x)
	{
		/* point is over or below box */
		Assert(box->low.y <= box->high.y);
		if (point->y > box->high.y)
			result = float8_mi(point->y, box->high.y);
		else if (point->y < box->low.y)
			result = float8_mi(box->low.y, point->y);
		else
		{
			if (pg_geo_errflag == 0)
				pg_geo_errflag = 8;
			return 0.0;
		}
	}
	else if (point->y <= box->high.y && point->y >= box->low.y)
	{
		/* point is to left or right of box */
		Assert(box->low.x <= box->high.x);
		if (point->x > box->high.x)
			result = float8_mi(point->x, box->high.x);
		else if (point->x < box->low.x)
			result = float8_mi(box->low.x, point->x);
		else
		{
			if (pg_geo_errflag == 0)
				pg_geo_errflag = 8;
			return 0.0;
		}
	}
	else
	{
		/* closest point will be a vertex */
		Point		p;
		float8		subresult;

		result = point_dt(point, &box->low);

		subresult = point_dt(point, &box->high);
		if (result > subresult)
			result = subresult;

		p.x = box->low.x;
		p.y = box->high.y;
		subresult = point_dt(point, &p);
		if (result > subresult)
			result = subresult;

		p.x = box->high.x;
		p.y = box->low.y;
		subresult = point_dt(point, &p);
		if (result > subresult)
			result = subresult;
	}

	return result;
}

static bool
gist_point_consistent_internal(StrategyNumber strategy,
							   bool isLeaf, BOX *key, Point *query)
{
	bool		result = false;

	switch (strategy)
	{
		case RTLeftStrategyNumber:
			result = FPlt(key->low.x, query->x);
			break;
		case RTRightStrategyNumber:
			result = FPgt(key->high.x, query->x);
			break;
		case RTAboveStrategyNumber:
			result = FPgt(key->high.y, query->y);
			break;
		case RTBelowStrategyNumber:
			result = FPlt(key->low.y, query->y);
			break;
		case RTSameStrategyNumber:
			if (isLeaf)
			{
				/* key.high must equal key.low, so we can disregard it */
				result = (FPeq(key->low.x, query->x) &&
						  FPeq(key->low.y, query->y));
			}
			else
			{
				result = (FPle(query->x, key->high.x) &&
						  FPge(query->x, key->low.x) &&
						  FPle(query->y, key->high.y) &&
						  FPge(query->y, key->low.y));
			}
			break;
		default:
			if (pg_geo_errflag == 0)
				pg_geo_errflag = 9;
			return false;
	}

	return result;
}

#define GeoStrategyNumberOffset		20
#define PointStrategyNumberGroup	0
#define BoxStrategyNumberGroup		1
#define PolygonStrategyNumberGroup	2
#define CircleStrategyNumberGroup	3

/**************************************************
 * fmgr-unwrapped entry points (wrapper bodies verbatim; signatures shimmed
 * per the file header).  All return pg_geo_errflag (0 = clean).
 **************************************************/

/* gist_box_consistent (oid 2578) */
int
pg_gist_box_consistent(int key_isnull,
					   double khx, double khy, double klx, double kly,
					   int query_isnull,
					   double qhx, double qhy, double qlx, double qly,
					   uint16 strategy, int page_is_leaf,
					   int *recheck, int *result)
{
	BOX			key_ = {{khx, khy}, {klx, kly}};
	BOX			query_ = {{qhx, qhy}, {qlx, qly}};

	pg_geo_errflag = 0;

	/* All cases served by this function are exact */
	*recheck = 0;

	if (key_isnull || query_isnull)
	{
		*result = 0;
		return pg_geo_errflag;
	}

	/*
	 * if entry is not leaf, use rtree_internal_consistent, else use
	 * gist_box_leaf_consistent
	 */
	if (page_is_leaf)
		*result = gist_box_leaf_consistent(&key_, &query_, strategy);
	else
		*result = rtree_internal_consistent(&key_, &query_, strategy);
	return pg_geo_errflag;
}

/* gist_box_union (oid 2583): entryvec -> flattened coords (hx,hy,lx,ly per
 * box); palloc'd result -> out[4].  Loop body verbatim. */
int
pg_gist_box_union(int numranges, const double *coords, int *sizep,
				  double *out)
{
	int			i;
	BOX			pageunion;

	pg_geo_errflag = 0;

	pageunion.high.x = coords[0];
	pageunion.high.y = coords[1];
	pageunion.low.x = coords[2];
	pageunion.low.y = coords[3];

	for (i = 1; i < numranges; i++)
	{
		BOX			cur;

		cur.high.x = coords[4 * i + 0];
		cur.high.y = coords[4 * i + 1];
		cur.low.x = coords[4 * i + 2];
		cur.low.y = coords[4 * i + 3];
		adjustBox(&pageunion, &cur);
	}
	*sizep = (int) sizeof(BOX);

	out[0] = pageunion.high.x;
	out[1] = pageunion.high.y;
	out[2] = pageunion.low.x;
	out[3] = pageunion.low.y;
	return pg_geo_errflag;
}

/* gist_box_penalty (oid 2581) */
int
pg_gist_box_penalty(double ohx, double ohy, double olx, double oly,
					double nhx, double nhy, double nlx, double nly,
					float *result)
{
	BOX			origbox = {{ohx, ohy}, {olx, oly}};
	BOX			newbox = {{nhx, nhy}, {nlx, nly}};

	pg_geo_errflag = 0;
	*result = (float) box_penalty(&origbox, &newbox);
	return pg_geo_errflag;
}

/* gist_box_same (oid 2584) — NaN-aware float8_eq (see file-header NOTE) */
int
pg_gist_box_same(int b1_isnull,
				 double ahx, double ahy, double alx, double aly,
				 int b2_isnull,
				 double bhx, double bhy, double blx, double bly,
				 int *result)
{
	BOX			b1_ = {{ahx, ahy}, {alx, aly}};
	BOX			b2_ = {{bhx, bhy}, {blx, bly}};

	pg_geo_errflag = 0;
	if (!b1_isnull && !b2_isnull)
		*result = (float8_eq(b1_.low.x, b2_.low.x) &&
				   float8_eq(b1_.low.y, b2_.low.y) &&
				   float8_eq(b1_.high.x, b2_.high.x) &&
				   float8_eq(b1_.high.y, b2_.high.y));
	else
		*result = (b1_isnull && b2_isnull);
	return pg_geo_errflag;
}

/* gist_point_compress (oid 1030), leaf arm: box->high = box->low = *point */
int
pg_gist_point_compress_leaf(double px, double py, double *out_box)
{
	Point		point_ = {px, py};
	BOX			box_;

	pg_geo_errflag = 0;
	box_.high = box_.low = point_;
	out_box[0] = box_.high.x;
	out_box[1] = box_.high.y;
	out_box[2] = box_.low.x;
	out_box[3] = box_.low.y;
	return pg_geo_errflag;
}

/* gist_point_fetch (oid 3282): r->x = in->high.x; r->y = in->high.y */
int
pg_gist_point_fetch(double khx, double khy, double klx, double kly,
					double *out_pt)
{
	BOX			in_ = {{khx, khy}, {klx, kly}};
	Point		r_;

	pg_geo_errflag = 0;
	r_.x = in_.high.x;
	r_.y = in_.high.y;
	out_pt[0] = r_.x;
	out_pt[1] = r_.y;
	return pg_geo_errflag;
}

/*
 * gist_point_consistent (oid 2179).  Wrapper body verbatim for groups 0/1
 * and the default (error) arm; groups 2/3 are PROOF-TRAPPED (flag 99, see
 * file header) — harnesses fence them out.  The point-group query rides
 * (qx, qy); the box-group query rides (qbhx..qbly).
 */
int
pg_gist_point_consistent(uint16 strategy_in, int page_is_leaf,
						 double khx, double khy, double klx, double kly,
						 double qx, double qy,
						 double qbhx, double qbhy, double qblx, double qbly,
						 int *recheck, int *result)
{
	BOX			key_ = {{khx, khy}, {klx, kly}};
	StrategyNumber strategy = strategy_in;
	StrategyNumber strategyGroup;

	pg_geo_errflag = 0;

	/*
	 * We have to remap these strategy numbers to get this klugy
	 * classification logic to work.
	 */
	if (strategy == RTOldBelowStrategyNumber)
		strategy = RTBelowStrategyNumber;
	else if (strategy == RTOldAboveStrategyNumber)
		strategy = RTAboveStrategyNumber;

	strategyGroup = strategy / GeoStrategyNumberOffset;
	switch (strategyGroup)
	{
		case PointStrategyNumberGroup:
			{
				Point		query_ = {qx, qy};

				*result = gist_point_consistent_internal(strategy % GeoStrategyNumberOffset,
														 page_is_leaf != 0,
														 &key_,
														 &query_);
				*recheck = 0;
			}
			break;
		case BoxStrategyNumberGroup:
			{
				/*
				 * The only operator in this group is point <@ box (on_pb),
				 * so we needn't examine strategy again.  (Non-fuzzy overlap
				 * test, verbatim.)
				 */
				BOX			query_ = {{qbhx, qbhy}, {qblx, qbly}};

				*result = (key_.high.x >= query_.low.x &&
						   key_.low.x <= query_.high.x &&
						   key_.high.y >= query_.low.y &&
						   key_.low.y <= query_.high.y);
				*recheck = 0;
			}
			break;
		case PolygonStrategyNumberGroup:
		case CircleStrategyNumberGroup:
			/* PROOF TRAP: out-of-plane arm (file header) */
			pg_geo_errflag = 99;
			*result = 0;
			break;
		default:
			if (pg_geo_errflag == 0)
				pg_geo_errflag = 9;
			*result = 0;
			break;
	}
	return pg_geo_errflag;
}

/* gist_point_distance (oid 3064): group 0 + default error arm, verbatim */
int
pg_gist_point_distance(uint16 strategy, int page_is_leaf,
					   double khx, double khy, double klx, double kly,
					   double qx, double qy, double *dist)
{
	BOX			key_ = {{khx, khy}, {klx, kly}};
	Point		query_ = {qx, qy};
	float8		distance;
	StrategyNumber strategyGroup = strategy / GeoStrategyNumberOffset;

	pg_geo_errflag = 0;
	switch (strategyGroup)
	{
		case PointStrategyNumberGroup:
			distance = computeDistance(page_is_leaf != 0, &key_, &query_);
			break;
		default:
			if (pg_geo_errflag == 0)
				pg_geo_errflag = 9;
			distance = 0.0;
			break;
	}
	*dist = distance;
	return pg_geo_errflag;
}

/* gistproc.c gist_bbox_distance, verbatim (group 0 + error arm) */
static float8
gist_bbox_distance(BOX *key, Point *query, StrategyNumber strategy)
{
	float8		distance;
	StrategyNumber strategyGroup = strategy / GeoStrategyNumberOffset;

	switch (strategyGroup)
	{
		case PointStrategyNumberGroup:
			distance = computeDistance(false, key, query);
			break;
		default:
			if (pg_geo_errflag == 0)
				pg_geo_errflag = 9;
			distance = 0.0;
			break;
	}
	return distance;
}

/* gist_box_distance (oid 3998): no recheck out-param in the C wrapper */
int
pg_gist_box_distance(uint16 strategy,
					 double khx, double khy, double klx, double kly,
					 double qx, double qy, double *dist)
{
	BOX			key_ = {{khx, khy}, {klx, kly}};
	Point		query_ = {qx, qy};

	pg_geo_errflag = 0;
	*dist = gist_bbox_distance(&key_, &query_, strategy);
	return pg_geo_errflag;
}

/* gist_circle_distance (oid 3280): bbox lower bound, *recheck = true */
int
pg_gist_circle_distance(uint16 strategy,
						double khx, double khy, double klx, double kly,
						double qx, double qy, int *recheck, double *dist)
{
	BOX			key_ = {{khx, khy}, {klx, kly}};
	Point		query_ = {qx, qy};

	pg_geo_errflag = 0;
	*dist = gist_bbox_distance(&key_, &query_, strategy);
	*recheck = 1;
	return pg_geo_errflag;
}

/* gist_poly_distance (oid 3288): same shape as circle_distance */
int
pg_gist_poly_distance(uint16 strategy,
					  double khx, double khy, double klx, double kly,
					  double qx, double qy, int *recheck, double *dist)
{
	BOX			key_ = {{khx, khy}, {klx, kly}};
	Point		query_ = {qx, qy};

	pg_geo_errflag = 0;
	*dist = gist_bbox_distance(&key_, &query_, strategy);
	*recheck = 1;
	return pg_geo_errflag;
}

/* gist_poly_consistent (oid 2585): query polygon -> its boundbox (the only
 * field read; detoast is shimmed-away plumbing) */
int
pg_gist_poly_consistent(int key_isnull,
						double khx, double khy, double klx, double kly,
						int query_isnull,
						double bbhx, double bbhy, double bblx, double bbly,
						uint16 strategy, int *recheck, int *result)
{
	BOX			key_ = {{khx, khy}, {klx, kly}};
	BOX			boundbox_ = {{bbhx, bbhy}, {bblx, bbly}};

	pg_geo_errflag = 0;

	/* All cases served by this function are inexact */
	*recheck = 1;

	if (key_isnull || query_isnull)
	{
		*result = 0;
		return pg_geo_errflag;
	}

	/*
	 * Since the operators require recheck anyway, we can just use
	 * rtree_internal_consistent even at leaf nodes.
	 */
	*result = rtree_internal_consistent(&key_, &boundbox_, strategy);
	return pg_geo_errflag;
}

/* gist_circle_consistent (oid 2591): bbox computation verbatim */
int
pg_gist_circle_consistent(int key_isnull,
						  double khx, double khy, double klx, double kly,
						  int query_isnull,
						  double cx, double cy, double r,
						  uint16 strategy, int *recheck, int *result)
{
	BOX			key_ = {{khx, khy}, {klx, kly}};
	BOX			bbox;

	pg_geo_errflag = 0;

	/* All cases served by this function are inexact */
	*recheck = 1;

	if (key_isnull || query_isnull)
	{
		*result = 0;
		return pg_geo_errflag;
	}

	bbox.high.x = float8_pl(cx, r);
	bbox.low.x = float8_mi(cx, r);
	bbox.high.y = float8_pl(cy, r);
	bbox.low.y = float8_mi(cy, r);

	*result = rtree_internal_consistent(&key_, &bbox, strategy);
	return pg_geo_errflag;
}

/* gist_circle_compress (oid 2592), leaf arm: bbox computation verbatim */
int
pg_gist_circle_compress_leaf(double cx, double cy, double r,
							 double *out_box)
{
	BOX			r_;

	pg_geo_errflag = 0;
	r_.high.x = float8_pl(cx, r);
	r_.low.x = float8_mi(cx, r);
	r_.high.y = float8_pl(cy, r);
	r_.low.y = float8_mi(cy, r);
	out_box[0] = r_.high.x;
	out_box[1] = r_.high.y;
	out_box[2] = r_.low.x;
	out_box[3] = r_.low.y;
	return pg_geo_errflag;
}

/* gist_poly_compress (oid 2586), leaf arm: memcpy(r, &in->boundbox) */
int
pg_gist_poly_compress_leaf(double bbhx, double bbhy, double bblx,
						   double bbly, double *out_box)
{
	BOX			boundbox_ = {{bbhx, bbhy}, {bblx, bbly}};

	pg_geo_errflag = 0;
	out_box[0] = boundbox_.high.x;
	out_box[1] = boundbox_.high.y;
	out_box[2] = boundbox_.low.x;
	out_box[3] = boundbox_.low.y;
	return pg_geo_errflag;
}

/* gistutil.c gist_translate_cmptype_common (oid 6347), switch verbatim */
int
pg_gist_translate_cmptype_common(int32 cmptype)
{
	switch (cmptype)
	{
		case COMPARE_EQ:
			return RTEqualStrategyNumber;
		case COMPARE_LT:
			return RTLessStrategyNumber;
		case COMPARE_LE:
			return RTLessEqualStrategyNumber;
		case COMPARE_GT:
			return RTGreaterStrategyNumber;
		case COMPARE_GE:
			return RTGreaterEqualStrategyNumber;
		case COMPARE_OVERLAP:
			return RTOverlapStrategyNumber;
		case COMPARE_CONTAINED_BY:
			return RTContainedByStrategyNumber;
		default:
			return InvalidStrategy;
	}
}
