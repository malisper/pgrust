/*
 * Vendored PostgreSQL C: SP-GiST opclasses over points and boxes, plus the
 * text/config row.
 *
 * Provenance (all @ postgres/postgres REL_18_STABLE, fetched 2026-07-30;
 * raw copies kept beside this file as rel18_*.c):
 *   - src/backend/access/spgist/spgquadtreeproc.c
 *       getQuadrant (here pgq_getQuadrant), spg_quad_config,
 *       spg_quad_choose, spg_quad_inner_consistent, spg_quad_leaf_consistent
 *   - src/backend/access/spgist/spgkdtreeproc.c
 *       getSide, spg_kd_config, spg_kd_choose, spg_kd_inner_consistent
 *   - src/backend/utils/adt/geo_spgist.c
 *       getQuadrant (here pgb_getQuadrant), getRangeBox, initRectBox,
 *       nextRectBox, overlap2D..overAbove4D, is_bounding_box_test_exact,
 *       spg_box_quad_get_scankey_bbox, spg_box_quad_config,
 *       spg_bbox_quad_config, spg_box_quad_choose,
 *       spg_box_quad_inner_consistent, spg_box_quad_leaf_consistent,
 *       spg_poly_quad_compress
 *   - src/backend/access/spgist/spgtextproc.c: spg_text_config
 *   - src/include/utils/geo_decls.h: EPSILON + FPeq/FPne/FPlt/FPle/FPgt/FPge
 *     (the #ifdef EPSILON arm — the shipped build), verbatim
 *   - src/backend/utils/adt/geo_ops.c: point_eq_point, box_ov,
 *     box_contain_box, box_contain_point + the point/box operator bodies
 *     reached via SPTEST/DirectFunctionCall2, verbatim (same text as
 *     proofs/geo-cmp/c/pg_geo_cmp.c)
 *   - src/include/utils/float.h: float8_eq, get_float8_infinity,
 *     get_float8_nan, verbatim
 *
 * Shims (plumbing only, never logic):
 *   - PG_FUNCTION_ARGS unwrapping -> plain (in, out) C signatures over
 *     protocol-mirror structs; PG_RETURN_BOOL -> int return.
 *   - SPTEST(f, x, y) / DatumGetBool(DirectFunctionCall2(f, a, b)) ->
 *     direct calls of the verbatim static predicate bodies.
 *   - palloc of out arrays / fresh geometry -> individually-NAMED static
 *     slots or caller storage (named-slot law); pfree -> no-op.
 *   - elog(ERROR) -> pg_spg_errflag = <code>, early return (harnesses fence
 *     or assert flag parity; flag 0 = clean).
 *   - ORDERBY machinery (spg_key_orderbys_distances, distance rows) is OUT
 *     OF SCOPE: every harness fences norderbys == 0; reaching an orderby
 *     arm sets pg_spg_trap = 1 (loud out-of-plane trap, never silent).
 *   - DatumGetPointP/DatumGetBoxP -> pointer cast (by-ref datum);
 *     DatumGetPolygonP -> pointer cast, 4B-header uncompressed varlena
 *     fence (the harness constructs the image; no detoast in-plane).
 *   - MemoryContextSwitchTo -> no-op (allocation strategy out of claim).
 *
 * NAN SHIM (mandatory, CBMC NAN model defect, ruled 2026-07-28): NAN is
 * pinned to the canonical quiet NaN (0x7ff8000000000000) exactly as in
 * proofs/geo-cmp/c/pg_geo_cmp.c; function bodies stay verbatim.
 */

#include "../../support/c/pg_proof_shim.h"

#include <math.h>

typedef double float8;
typedef unsigned short StrategyNumber;

/* ---- error/trap flags ---- */
int			pg_spg_errflag = 0; /* elog(ERROR) surrogate: 1 = impossible
								 * quadrant, 2 = bad strategy, 3 = bad
								 * scankey subtype, 4 = kd allTheSame */
int			pg_spg_trap = 0;	/* out-of-plane trap (orderby machinery) */

/* ---- NAN shim (verbatim from proofs/geo-cmp/c/pg_geo_cmp.c) ---- */
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

/* ---- src/include/utils/float.h, verbatim ---- */
static inline float8
get_float8_infinity(void)
{
	/* C99 standard way */
	return (float8) INFINITY;
}

static inline bool
float8_eq(const float8 val1, const float8 val2)
{
	return isnan(val1) ? isnan(val2) : !isnan(val2) && val1 == val2;
}

/* ---- geometric structs (geo_decls.h layout) ---- */
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

/* ---- geo_spgist.c structs, verbatim ---- */
typedef struct
{
	float8		low;
	float8		high;
} Range;

typedef struct
{
	Range		left;
	Range		right;
} RangeBox;

typedef struct
{
	RangeBox	range_box_x;
	RangeBox	range_box_y;
} RectBox;

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

/* ---- geo_ops.c bodies, verbatim (SPTEST/DirectFunctionCall2 targets) ---- */

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
point_left(Point *pt1, Point *pt2)
{
	return FPlt(pt1->x, pt2->x);
}

static bool
point_right(Point *pt1, Point *pt2)
{
	return FPgt(pt1->x, pt2->x);
}

static bool
point_above(Point *pt1, Point *pt2)
{
	return FPgt(pt1->y, pt2->y);
}

static bool
point_below(Point *pt1, Point *pt2)
{
	return FPlt(pt1->y, pt2->y);
}

static bool
point_vert(Point *pt1, Point *pt2)
{
	return FPeq(pt1->x, pt2->x);
}

static bool
point_horiz(Point *pt1, Point *pt2)
{
	return FPeq(pt1->y, pt2->y);
}

static bool
point_eq(Point *pt1, Point *pt2)
{
	return point_eq_point(pt1, pt2);
}

static bool
box_ov(BOX *box1, BOX *box2)
{
	return (FPle(box1->low.x, box2->high.x) &&
			FPle(box2->low.x, box1->high.x) &&
			FPle(box1->low.y, box2->high.y) &&
			FPle(box2->low.y, box1->high.y));
}

static bool
box_contain_box(BOX *contains_box, BOX *contained_box)
{
	return FPge(contains_box->high.x, contained_box->high.x) &&
		FPle(contains_box->low.x, contained_box->low.x) &&
		FPge(contains_box->high.y, contained_box->high.y) &&
		FPle(contains_box->low.y, contained_box->low.y);
}

static bool
box_contain_point(BOX *box, Point *point)
{
	return box->high.x >= point->x && box->low.x <= point->x &&
		box->high.y >= point->y && box->low.y <= point->y;
}

/* operator bodies as called through DirectFunctionCall2, verbatim */
static bool
box_overlap(BOX *box1, BOX *box2)
{
	return box_ov(box1, box2);
}

static bool
box_same(BOX *box1, BOX *box2)
{
	return point_eq_point(&box1->high, &box2->high) &&
		point_eq_point(&box1->low, &box2->low);
}

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
box_right(BOX *box1, BOX *box2)
{
	return FPgt(box1->low.x, box2->high.x);
}

static bool
box_overright(BOX *box1, BOX *box2)
{
	return FPge(box1->low.x, box2->low.x);
}

static bool
box_below(BOX *box1, BOX *box2)
{
	return FPlt(box1->high.y, box2->low.y);
}

static bool
box_overbelow(BOX *box1, BOX *box2)
{
	return FPle(box1->high.y, box2->high.y);
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

static bool
box_contained(BOX *box1, BOX *box2)
{
	return box_contain_box(box2, box1);
}

static bool
box_contain(BOX *box1, BOX *box2)
{
	return box_contain_box(box1, box2);
}

static bool
box_contain_pt(BOX *box, Point *pt)
{
	return box_contain_point(box, pt);
}

/* SPTEST shim: DirectFunctionCall2 unwrapped to the verbatim body */
#define SPTEST(f, x, y)	f(x, y)

/* ---- strategy numbers (access/stratnum.h) ---- */
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
#define RTOldBelowStrategyNumber		29
#define RTOldAboveStrategyNumber		30

/* ---- type oids (catalog/pg_type_d.h) ---- */
#define POINTOID	600
#define BOXOID		603
#define POLYGONOID	604
#define FLOAT8OID	701
#define TEXTOID		25
#define INT2OID		21
#define VOIDOID		2278

/* ---- protocol mirror structs (match the Rust harness reprs) ---- */
typedef struct
{
	uint32		prefixType;
	uint32		labelType;
	uint32		leafType;
	uint8		canReturnData;
	uint8		longValuesOK;
} CConfigOut;

typedef struct
{
	Size		datum;
	Size		leafDatum;
	int			level;
	uint8		allTheSame;
	uint8		hasPrefix;
	Size		prefixDatum;
	int			nNodes;
	Size		nodeLabels;
} CChooseIn;

typedef struct
{
	int			resultType;		/* 1 = spgMatchNode */
	int			nodeN;
	int			levelAdd;
	Size		restDatum;
} CChooseOut;					/* MatchNode-only mirror: every function
								 * here emits MatchNode or nothing */

typedef struct
{
	uint16		sk_strategy;
	uint32		sk_subtype;
	Size		sk_argument;
} CScanKey;

typedef struct
{
	Size		scankeys;
	Size		orderbys;
	int			nkeys;
	int			norderbys;
	Size		reconstructedValue;
	Size		traversalValue;
	Size		traversalMemoryContext;
	int			level;
	uint8		returnData;
	uint8		allTheSame;
	uint8		hasPrefix;
	Size		prefixDatum;
	int			nNodes;
	Size		nodeLabels;
} CInnerIn;

typedef struct
{
	int			nNodes;
	int			nodeNumbers[16];
	int			levelAdds[16];
} CInnerOut;					/* fixed-frame mirror of the palloc'd
								 * arrays (named-slot law) */

typedef struct
{
	Size		scankeys;
	Size		orderbys;
	int			nkeys;
	int			norderbys;
	Size		reconstructedValue;
	Size		traversalValue;
	int			level;
	uint8		returnData;
	Size		leafDatum;
} CLeafIn;

typedef struct
{
	Size		leafValue;
	uint8		recheck;
	uint8		recheckDistances;
} CLeafOut;

/* =====================================================================
 * spgquadtreeproc.c
 * ===================================================================== */

/* body verbatim (renamed pgq_; elog -> errflag) */
static int16
pgq_getQuadrant(Point *centroid, Point *tst)
{
	if ((SPTEST(point_above, tst, centroid) ||
		 SPTEST(point_horiz, tst, centroid)) &&
		(SPTEST(point_right, tst, centroid) ||
		 SPTEST(point_vert, tst, centroid)))
		return 1;

	if (SPTEST(point_below, tst, centroid) &&
		(SPTEST(point_right, tst, centroid) ||
		 SPTEST(point_vert, tst, centroid)))
		return 2;

	if ((SPTEST(point_below, tst, centroid) ||
		 SPTEST(point_horiz, tst, centroid)) &&
		SPTEST(point_left, tst, centroid))
		return 3;

	if (SPTEST(point_above, tst, centroid) &&
		SPTEST(point_left, tst, centroid))
		return 4;

	pg_spg_errflag = 1;			/* elog(ERROR, "getQuadrant: impossible
								 * case") */
	return 0;
}

/* exported probe for the err-plane parity theorem */
int
pg_quad_getQuadrant(double cx, double cy, double tx, double ty, int16 *q)
{
	Point		c_ = {cx, cy};
	Point		t_ = {tx, ty};

	pg_spg_errflag = 0;
	*q = pgq_getQuadrant(&c_, &t_);
	return pg_spg_errflag;
}

int
pg_spg_quad_config(CConfigOut *cfg)
{
	cfg->prefixType = POINTOID;
	cfg->labelType = VOIDOID;	/* we don't need node labels */
	cfg->canReturnData = true;
	cfg->longValuesOK = false;
	return 0;
}

int
pg_spg_quad_choose(const CChooseIn *in, CChooseOut *out)
{
	Point	   *inPoint = (Point *) in->datum;
	Point	   *centroid;

	pg_spg_errflag = 0;

	if (in->allTheSame)
	{
		out->resultType = 1;	/* spgMatchNode */
		/* nodeN will be set by core */
		out->levelAdd = 0;
		out->restDatum = in->datum;
		return 0;
	}

	Assert(in->hasPrefix);
	centroid = (Point *) in->prefixDatum;

	Assert(in->nNodes == 4);

	out->resultType = 1;		/* spgMatchNode */
	out->nodeN = pgq_getQuadrant(centroid, inPoint) - 1;
	out->levelAdd = 0;
	out->restDatum = in->datum;

	return pg_spg_errflag;
}

int
pg_spg_quad_inner_consistent(const CInnerIn *in, CInnerOut *out)
{
	Point	   *centroid;
	int			which;
	int			i;
	CScanKey   *scankeys = (CScanKey *) in->scankeys;

	pg_spg_errflag = 0;

	centroid = (Point *) in->prefixDatum;

	if (in->norderbys > 0)
		pg_spg_trap = 1;		/* orderby machinery out of plane */

	if (in->allTheSame)
	{
		/* Report that all nodes should be visited */
		out->nNodes = in->nNodes;
		for (i = 0; i < in->nNodes; i++)
			out->nodeNumbers[i] = i;
		return 0;
	}

	/* "which" is a bitmask of quadrants that satisfy all constraints */
	which = (1 << 1) | (1 << 2) | (1 << 3) | (1 << 4);

	for (i = 0; i < in->nkeys; i++)
	{
		Point	   *query = (Point *) scankeys[i].sk_argument;
		BOX		   *boxQuery;

		switch (scankeys[i].sk_strategy)
		{
			case RTLeftStrategyNumber:
				if (SPTEST(point_right, centroid, query))
					which &= (1 << 3) | (1 << 4);
				break;
			case RTRightStrategyNumber:
				if (SPTEST(point_left, centroid, query))
					which &= (1 << 1) | (1 << 2);
				break;
			case RTSameStrategyNumber:
				which &= (1 << pgq_getQuadrant(centroid, query));
				break;
			case RTBelowStrategyNumber:
			case RTOldBelowStrategyNumber:
				if (SPTEST(point_above, centroid, query))
					which &= (1 << 2) | (1 << 3);
				break;
			case RTAboveStrategyNumber:
			case RTOldAboveStrategyNumber:
				if (SPTEST(point_below, centroid, query))
					which &= (1 << 1) | (1 << 4);
				break;
			case RTContainedByStrategyNumber:
				boxQuery = (BOX *) scankeys[i].sk_argument;

				if (SPTEST(box_contain_pt, boxQuery, centroid))
				{
					/* centroid is in box, so all quadrants are OK */
				}
				else
				{
					/* identify quadrant(s) containing all corners of box */
					Point		p;
					int			r = 0;

					p = boxQuery->low;
					r |= 1 << pgq_getQuadrant(centroid, &p);
					p.y = boxQuery->high.y;
					r |= 1 << pgq_getQuadrant(centroid, &p);
					p = boxQuery->high;
					r |= 1 << pgq_getQuadrant(centroid, &p);
					p.x = boxQuery->low.x;
					r |= 1 << pgq_getQuadrant(centroid, &p);

					which &= r;
				}
				break;
			default:
				pg_spg_errflag = 2;
				return pg_spg_errflag;
		}

		if (which == 0)
			break;				/* no need to consider remaining conditions */
	}

	for (i = 0; i < 4; ++i)
		out->levelAdds[i] = 1;

	out->nNodes = 0;

	for (i = 1; i <= 4; i++)
	{
		if (which & (1 << i))
		{
			out->nodeNumbers[out->nNodes] = i - 1;
			out->nNodes++;
		}
	}

	return pg_spg_errflag;
}

int
pg_spg_quad_leaf_consistent(const CLeafIn *in, CLeafOut *out, int *res_out)
{
	Point	   *datum = (Point *) in->leafDatum;
	bool		res;
	int			i;
	CScanKey   *scankeys = (CScanKey *) in->scankeys;

	pg_spg_errflag = 0;

	/* all tests are exact */
	out->recheck = false;

	/* leafDatum is what it is... */
	out->leafValue = in->leafDatum;

	/* Perform the required comparison(s) */
	res = true;
	for (i = 0; i < in->nkeys; i++)
	{
		Point	   *query = (Point *) scankeys[i].sk_argument;

		switch (scankeys[i].sk_strategy)
		{
			case RTLeftStrategyNumber:
				res = SPTEST(point_left, datum, query);
				break;
			case RTRightStrategyNumber:
				res = SPTEST(point_right, datum, query);
				break;
			case RTSameStrategyNumber:
				res = SPTEST(point_eq, datum, query);
				break;
			case RTBelowStrategyNumber:
			case RTOldBelowStrategyNumber:
				res = SPTEST(point_below, datum, query);
				break;
			case RTAboveStrategyNumber:
			case RTOldAboveStrategyNumber:
				res = SPTEST(point_above, datum, query);
				break;
			case RTContainedByStrategyNumber:
				res = SPTEST(box_contain_pt, (BOX *) query, datum);
				break;
			default:
				pg_spg_errflag = 2;
				return pg_spg_errflag;
		}

		if (!res)
			break;
	}

	if (res && in->norderbys > 0)
		pg_spg_trap = 1;		/* orderby machinery out of plane */

	*res_out = res;
	return pg_spg_errflag;
}

/* =====================================================================
 * spgkdtreeproc.c
 * ===================================================================== */

static int
getSide(double coord, bool isX, Point *tst)
{
	double		tstcoord = (isX) ? tst->x : tst->y;

	if (coord == tstcoord)
		return 0;
	else if (coord > tstcoord)
		return 1;
	else
		return -1;
}

int
pg_spg_kd_config(CConfigOut *cfg)
{
	cfg->prefixType = FLOAT8OID;
	cfg->labelType = VOIDOID;	/* we don't need node labels */
	cfg->canReturnData = true;
	cfg->longValuesOK = false;
	return 0;
}

/* prefixDatum is a BY-VALUE float8 datum: passed as its double image */
int
pg_spg_kd_choose(const CChooseIn *in, double prefix_coord, CChooseOut *out)
{
	Point	   *inPoint = (Point *) in->datum;
	double		coord;

	pg_spg_errflag = 0;

	if (in->allTheSame)
	{
		pg_spg_errflag = 4;		/* elog(ERROR, "allTheSame should not occur
								 * for k-d trees") */
		return pg_spg_errflag;
	}

	Assert(in->hasPrefix);
	coord = prefix_coord;

	Assert(in->nNodes == 2);

	out->resultType = 1;		/* spgMatchNode */
	out->nodeN = (getSide(coord, in->level % 2, inPoint) > 0) ? 0 : 1;
	out->levelAdd = 1;
	out->restDatum = in->datum;

	return 0;
}

int
pg_spg_kd_inner_consistent(const CInnerIn *in, double prefix_coord,
						   CInnerOut *out)
{
	double		coord;
	int			which;
	int			i;
	CScanKey   *scankeys = (CScanKey *) in->scankeys;

	pg_spg_errflag = 0;

	coord = prefix_coord;

	if (in->allTheSame)
	{
		pg_spg_errflag = 4;
		return pg_spg_errflag;
	}

	which = (1 << 1) | (1 << 2);

	for (i = 0; i < in->nkeys; i++)
	{
		Point	   *query = (Point *) scankeys[i].sk_argument;
		BOX		   *boxQuery;

		switch (scankeys[i].sk_strategy)
		{
			case RTLeftStrategyNumber:
				if ((in->level % 2) != 0 && FPlt(query->x, coord))
					which &= (1 << 1);
				break;
			case RTRightStrategyNumber:
				if ((in->level % 2) != 0 && FPgt(query->x, coord))
					which &= (1 << 2);
				break;
			case RTSameStrategyNumber:
				if ((in->level % 2) != 0)
				{
					if (FPlt(query->x, coord))
						which &= (1 << 1);
					else if (FPgt(query->x, coord))
						which &= (1 << 2);
				}
				else
				{
					if (FPlt(query->y, coord))
						which &= (1 << 1);
					else if (FPgt(query->y, coord))
						which &= (1 << 2);
				}
				break;
			case RTBelowStrategyNumber:
			case RTOldBelowStrategyNumber:
				if ((in->level % 2) == 0 && FPlt(query->y, coord))
					which &= (1 << 1);
				break;
			case RTAboveStrategyNumber:
			case RTOldAboveStrategyNumber:
				if ((in->level % 2) == 0 && FPgt(query->y, coord))
					which &= (1 << 2);
				break;
			case RTContainedByStrategyNumber:
				boxQuery = (BOX *) scankeys[i].sk_argument;

				if ((in->level % 2) != 0)
				{
					if (FPlt(boxQuery->high.x, coord))
						which &= (1 << 1);
					else if (FPgt(boxQuery->low.x, coord))
						which &= (1 << 2);
				}
				else
				{
					if (FPlt(boxQuery->high.y, coord))
						which &= (1 << 1);
					else if (FPgt(boxQuery->low.y, coord))
						which &= (1 << 2);
				}
				break;
			default:
				pg_spg_errflag = 2;
				return pg_spg_errflag;
		}

		if (which == 0)
			break;				/* no need to consider remaining conditions */
	}

	/* We must descend into the children identified by which */
	out->nNodes = 0;

	/* Fast-path for no matching children */
	if (!which)
		return 0;

	if (in->norderbys > 0)
		pg_spg_trap = 1;		/* orderby machinery out of plane */

	for (i = 1; i <= 2; i++)
	{
		if (which & (1 << i))
		{
			out->nodeNumbers[out->nNodes] = i - 1;
			out->nNodes++;
		}
	}

	/* Set up level increments, too */
	out->levelAdds[0] = 1;
	out->levelAdds[1] = 1;

	return 0;
}

/* =====================================================================
 * geo_spgist.c
 * ===================================================================== */

/* body verbatim (renamed pgb_) */
static uint8
pgb_getQuadrant(BOX *centroid, BOX *inBox)
{
	uint8		quadrant = 0;

	if (inBox->low.x > centroid->low.x)
		quadrant |= 0x8;

	if (inBox->high.x > centroid->high.x)
		quadrant |= 0x4;

	if (inBox->low.y > centroid->low.y)
		quadrant |= 0x2;

	if (inBox->high.y > centroid->high.y)
		quadrant |= 0x1;

	return quadrant;
}

/* palloc -> caller storage (shim); body otherwise verbatim */
static void
getRangeBox(BOX *box, RangeBox *range_box)
{
	range_box->left.low = box->low.x;
	range_box->left.high = box->high.x;

	range_box->right.low = box->low.y;
	range_box->right.high = box->high.y;
}

/* palloc -> caller storage (shim); body otherwise verbatim */
static void
initRectBox(RectBox *rect_box)
{
	float8		infinity = get_float8_infinity();

	rect_box->range_box_x.left.low = -infinity;
	rect_box->range_box_x.left.high = infinity;

	rect_box->range_box_x.right.low = -infinity;
	rect_box->range_box_x.right.high = infinity;

	rect_box->range_box_y.left.low = -infinity;
	rect_box->range_box_y.left.high = infinity;

	rect_box->range_box_y.right.low = -infinity;
	rect_box->range_box_y.right.high = infinity;
}

/* palloc -> caller storage, memcpy -> struct assign (shim); else verbatim */
static void
nextRectBox(RectBox *rect_box, RangeBox *centroid, uint8 quadrant,
			RectBox *next_rect_box)
{
	*next_rect_box = *rect_box;

	if (quadrant & 0x8)
		next_rect_box->range_box_x.left.low = centroid->left.low;
	else
		next_rect_box->range_box_x.left.high = centroid->left.low;

	if (quadrant & 0x4)
		next_rect_box->range_box_x.right.low = centroid->left.high;
	else
		next_rect_box->range_box_x.right.high = centroid->left.high;

	if (quadrant & 0x2)
		next_rect_box->range_box_y.left.low = centroid->right.low;
	else
		next_rect_box->range_box_y.left.high = centroid->right.low;

	if (quadrant & 0x1)
		next_rect_box->range_box_y.right.low = centroid->right.high;
	else
		next_rect_box->range_box_y.right.high = centroid->right.high;
}

/* ---- 2D/4D predicates, verbatim ---- */
static bool
overlap2D(RangeBox *range_box, Range *query)
{
	return FPge(range_box->right.high, query->low) &&
		FPle(range_box->left.low, query->high);
}

static bool
overlap4D(RectBox *rect_box, RangeBox *query)
{
	return overlap2D(&rect_box->range_box_x, &query->left) &&
		overlap2D(&rect_box->range_box_y, &query->right);
}

static bool
contain2D(RangeBox *range_box, Range *query)
{
	return FPge(range_box->right.high, query->high) &&
		FPle(range_box->left.low, query->low);
}

static bool
contain4D(RectBox *rect_box, RangeBox *query)
{
	return contain2D(&rect_box->range_box_x, &query->left) &&
		contain2D(&rect_box->range_box_y, &query->right);
}

static bool
contained2D(RangeBox *range_box, Range *query)
{
	return FPle(range_box->left.low, query->high) &&
		FPge(range_box->left.high, query->low) &&
		FPle(range_box->right.low, query->high) &&
		FPge(range_box->right.high, query->low);
}

static bool
contained4D(RectBox *rect_box, RangeBox *query)
{
	return contained2D(&rect_box->range_box_x, &query->left) &&
		contained2D(&rect_box->range_box_y, &query->right);
}

static bool
lower2D(RangeBox *range_box, Range *query)
{
	return FPlt(range_box->left.low, query->low) &&
		FPlt(range_box->right.low, query->low);
}

static bool
overLower2D(RangeBox *range_box, Range *query)
{
	return FPle(range_box->left.low, query->high) &&
		FPle(range_box->right.low, query->high);
}

static bool
higher2D(RangeBox *range_box, Range *query)
{
	return FPgt(range_box->left.high, query->high) &&
		FPgt(range_box->right.high, query->high);
}

static bool
overHigher2D(RangeBox *range_box, Range *query)
{
	return FPge(range_box->left.high, query->low) &&
		FPge(range_box->right.high, query->low);
}

static bool
left4D(RectBox *rect_box, RangeBox *query)
{
	return lower2D(&rect_box->range_box_x, &query->left);
}

static bool
overLeft4D(RectBox *rect_box, RangeBox *query)
{
	return overLower2D(&rect_box->range_box_x, &query->left);
}

static bool
right4D(RectBox *rect_box, RangeBox *query)
{
	return higher2D(&rect_box->range_box_x, &query->left);
}

static bool
overRight4D(RectBox *rect_box, RangeBox *query)
{
	return overHigher2D(&rect_box->range_box_x, &query->left);
}

static bool
below4D(RectBox *rect_box, RangeBox *query)
{
	return lower2D(&rect_box->range_box_y, &query->right);
}

static bool
overBelow4D(RectBox *rect_box, RangeBox *query)
{
	return overLower2D(&rect_box->range_box_y, &query->right);
}

static bool
above4D(RectBox *rect_box, RangeBox *query)
{
	return higher2D(&rect_box->range_box_y, &query->right);
}

static bool
overAbove4D(RectBox *rect_box, RangeBox *query)
{
	return overHigher2D(&rect_box->range_box_y, &query->right);
}

/* ---- config / choose / consistent ---- */

int
pg_spg_box_quad_config(CConfigOut *cfg)
{
	cfg->prefixType = BOXOID;
	cfg->labelType = VOIDOID;	/* We don't need node labels. */
	cfg->canReturnData = true;
	cfg->longValuesOK = false;

	return 0;
}

int
pg_spg_bbox_quad_config(CConfigOut *cfg)
{
	cfg->prefixType = BOXOID;	/* A type represented by its bounding box */
	cfg->labelType = VOIDOID;	/* We don't need node labels. */
	cfg->leafType = BOXOID;
	cfg->canReturnData = false;
	cfg->longValuesOK = false;

	return 0;
}

int
pg_spg_box_quad_choose(const CChooseIn *in, CChooseOut *out)
{
	BOX		   *centroid = (BOX *) in->prefixDatum,
			   *box = (BOX *) in->leafDatum;

	out->resultType = 1;		/* spgMatchNode */
	out->restDatum = in->leafDatum;

	/* nodeN will be set by core, when allTheSame. */
	if (!in->allTheSame)
		out->nodeN = pgb_getQuadrant(centroid, box);

	return 0;
}

static bool
is_bounding_box_test_exact(StrategyNumber strategy)
{
	switch (strategy)
	{
		case RTLeftStrategyNumber:
		case RTOverLeftStrategyNumber:
		case RTOverRightStrategyNumber:
		case RTRightStrategyNumber:
		case RTOverBelowStrategyNumber:
		case RTBelowStrategyNumber:
		case RTAboveStrategyNumber:
		case RTOverAboveStrategyNumber:
			return true;

		default:
			return false;
	}
}

/*
 * POLYGON on-image layout fence: 4B varlena header, int32 npts, BOX
 * boundbox at byte offset 8 (uncompressed 4B-header image, as the harness
 * constructs it; DatumGetPolygonP's detoast is out of plane).
 */
static BOX *
spg_box_quad_get_scankey_bbox(CScanKey *sk, uint8 *recheck)
{
	switch (sk->sk_subtype)
	{
		case BOXOID:
			return (BOX *) sk->sk_argument;

		case POLYGONOID:
			if (recheck && !is_bounding_box_test_exact(sk->sk_strategy))
				*recheck = true;
			return (BOX *) ((char *) sk->sk_argument + 8);

		default:
			pg_spg_errflag = 3;
			return NULL;
	}
}

int
pg_spg_box_quad_inner_consistent(const CInnerIn *in, CInnerOut *out)
{
	int			i;
	RectBox		rect_box_storage;
	RectBox    *rect_box;
	uint8		quadrant;
	RangeBox	centroid_storage;
	RangeBox   *centroid;
	RangeBox	queries_storage[2];
	CScanKey   *scankeys = (CScanKey *) in->scankeys;
	static RectBox next_rect_box_slots[16];

	pg_spg_errflag = 0;

	if (in->norderbys > 0)
		pg_spg_trap = 1;		/* orderby machinery out of plane */

	if (in->traversalValue)
		rect_box = (RectBox *) in->traversalValue;
	else
	{
		initRectBox(&rect_box_storage);
		rect_box = &rect_box_storage;
	}

	if (in->allTheSame)
	{
		/* Report that all nodes should be visited */
		out->nNodes = in->nNodes;
		for (i = 0; i < in->nNodes; i++)
			out->nodeNumbers[i] = i;

		return 0;
	}

	centroid = &centroid_storage;
	getRangeBox((BOX *) in->prefixDatum, centroid);
	for (i = 0; i < in->nkeys; i++)
	{
		BOX		   *box = spg_box_quad_get_scankey_bbox(&scankeys[i], NULL);

		if (pg_spg_errflag)
			return pg_spg_errflag;
		getRangeBox(box, &queries_storage[i]);
	}

	out->nNodes = 0;

	for (quadrant = 0; quadrant < in->nNodes; quadrant++)
	{
		RectBox    *next_rect_box = &next_rect_box_slots[quadrant];
		bool		flag = true;

		nextRectBox(rect_box, centroid, quadrant, next_rect_box);

		for (i = 0; i < in->nkeys; i++)
		{
			StrategyNumber strategy = scankeys[i].sk_strategy;

			switch (strategy)
			{
				case RTOverlapStrategyNumber:
					flag = overlap4D(next_rect_box, &queries_storage[i]);
					break;

				case RTContainsStrategyNumber:
					flag = contain4D(next_rect_box, &queries_storage[i]);
					break;

				case RTSameStrategyNumber:
				case RTContainedByStrategyNumber:
					flag = contained4D(next_rect_box, &queries_storage[i]);
					break;

				case RTLeftStrategyNumber:
					flag = left4D(next_rect_box, &queries_storage[i]);
					break;

				case RTOverLeftStrategyNumber:
					flag = overLeft4D(next_rect_box, &queries_storage[i]);
					break;

				case RTRightStrategyNumber:
					flag = right4D(next_rect_box, &queries_storage[i]);
					break;

				case RTOverRightStrategyNumber:
					flag = overRight4D(next_rect_box, &queries_storage[i]);
					break;

				case RTAboveStrategyNumber:
					flag = above4D(next_rect_box, &queries_storage[i]);
					break;

				case RTOverAboveStrategyNumber:
					flag = overAbove4D(next_rect_box, &queries_storage[i]);
					break;

				case RTBelowStrategyNumber:
					flag = below4D(next_rect_box, &queries_storage[i]);
					break;

				case RTOverBelowStrategyNumber:
					flag = overBelow4D(next_rect_box, &queries_storage[i]);
					break;

				default:
					pg_spg_errflag = 2;
					return pg_spg_errflag;
			}

			/* If any check is failed, we have found our answer. */
			if (!flag)
				break;
		}

		if (flag)
		{
			out->nodeNumbers[out->nNodes] = quadrant;
			out->nNodes++;
		}
		/* else: pfree(next_rect_box) -> no-op (static slot) */
	}

	return 0;
}

int
pg_spg_box_quad_leaf_consistent(const CLeafIn *in, CLeafOut *out,
								int *res_out)
{
	BOX		   *leaf = (BOX *) in->leafDatum;
	bool		flag = true;
	int			i;
	CScanKey   *scankeys = (CScanKey *) in->scankeys;

	pg_spg_errflag = 0;

	/* All tests are exact. */
	out->recheck = false;

	/*
	 * Don't return leafValue unless told to; this is used for both box and
	 * polygon opclasses, and in the latter case the leaf datum is not even
	 * of the right type to return.
	 */
	if (in->returnData)
		out->leafValue = in->leafDatum;

	/* Perform the required comparison(s) */
	for (i = 0; i < in->nkeys; i++)
	{
		StrategyNumber strategy = scankeys[i].sk_strategy;
		BOX		   *box = spg_box_quad_get_scankey_bbox(&scankeys[i],
														&out->recheck);
		BOX		   *query = box;

		if (pg_spg_errflag)
			return pg_spg_errflag;

		switch (strategy)
		{
			case RTOverlapStrategyNumber:
				flag = box_overlap(leaf, query);
				break;

			case RTContainsStrategyNumber:
				flag = box_contain(leaf, query);
				break;

			case RTContainedByStrategyNumber:
				flag = box_contained(leaf, query);
				break;

			case RTSameStrategyNumber:
				flag = box_same(leaf, query);
				break;

			case RTLeftStrategyNumber:
				flag = box_left(leaf, query);
				break;

			case RTOverLeftStrategyNumber:
				flag = box_overleft(leaf, query);
				break;

			case RTRightStrategyNumber:
				flag = box_right(leaf, query);
				break;

			case RTOverRightStrategyNumber:
				flag = box_overright(leaf, query);
				break;

			case RTAboveStrategyNumber:
				flag = box_above(leaf, query);
				break;

			case RTOverAboveStrategyNumber:
				flag = box_overabove(leaf, query);
				break;

			case RTBelowStrategyNumber:
				flag = box_below(leaf, query);
				break;

			case RTOverBelowStrategyNumber:
				flag = box_overbelow(leaf, query);
				break;

			default:
				pg_spg_errflag = 2;
				return pg_spg_errflag;
		}

		/* If any check is failed, we have found our answer. */
		if (!flag)
			break;
	}

	if (flag && in->norderbys > 0)
		pg_spg_trap = 1;		/* orderby machinery out of plane */

	*res_out = flag;
	return 0;
}

/*
 * spg_poly_quad_compress: polygon (4B-header varlena image) -> fresh BOX
 * copy of its boundbox.  palloc of the result -> caller storage (shim).
 */
int
pg_spg_poly_quad_compress(const char *polygon_image, BOX *box)
{
	BOX		   *boundbox = (BOX *) (polygon_image + 8);

	*box = *boundbox;
	return 0;
}

/* =====================================================================
 * spgtextproc.c: spg_text_config
 * ===================================================================== */

int
pg_spg_text_config(CConfigOut *cfg)
{
	cfg->prefixType = TEXTOID;
	cfg->labelType = INT2OID;
	cfg->canReturnData = true;
	cfg->longValuesOK = true;	/* suffixing will shorten long values */
	return 0;
}
