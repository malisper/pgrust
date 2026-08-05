/*
 * pg_spgquad_io.c: vendored PostgreSQL C oracle for the spgquad_diff differential
 * fuzz target (100%-coverage campaign; crate crates/backend/access/spgist/spgist_quadtree).
 *
 * GENERATED SKELETON (fuzz/scaffold.py) — NOT yet a valid oracle. Every
 * TODO(scaffold) paste site below must be filled with VERBATIM upstream C,
 * and every #error compile gate removed WITH its paste, before the
 * .file("csrc/pg_spgquad_io.c") line in core/build.rs is uncommented. A
 * half-filled shim can therefore never silently build or link.
 *
 * Provenance (fill in as you paste; follow csrc/pg_uuid_io.c):
 *   - Vendor sections 1..N byte-for-byte from src/backend/utils/adt/spgquadtreeproc.c
 *     @ postgres-src 62d6c7d3df6287f1bd83199c1a746e50d31571a0
 *     (PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df); re-verify against the repo's vendored ground-truth
 *     checkout ../pgrust-fabled/vendor/postgres-src before pasting).
 *   - Functions to vendor: spg_quad_config, spg_quad_choose, spg_quad_picksplit, spg_quad_inner_consistent, spg_quad_leaf_consistent.
 *   - Bodies VERBATIM except documented shims; shims are PLUMBING ONLY
 *     (isxdigit/strtoul C-locale shims, ereturn -> int sentinel, fmgr
 *     PG_FUNCTION_ARGS unwrapped to plain C signatures, palloc'd results ->
 *     caller buffers, wire triples for recv/send), NEVER logic. List every
 *     shim in this header when you paste.
 *   - palloc/palloc0/repalloc/pfree -> the TLS pointer arena below (NOT
 *     bare malloc/free): models PG's memory-context reset; error paths
 *     strand allocations otherwise. Do NOT free() arena pointers by hand.
 *
 * Errcode capture follows csrc/pg_float_io.c: the shared _Thread_local
 * pg_diff_errcode (defined there) records the errcode class; map each
 * errcode this crate's C raises to a small class constant below.
 */

#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

/* Shared TLS errcode channel (defined in csrc/pg_float_io.c). */
extern _Thread_local int pg_diff_errcode;

/* TODO(scaffold): one class constant per distinct errcode the vendored C
 * raises, e.g.:
 *   #define PG_DIFF_ERR_INVALID_TEXT 1   (22P02)
 */

/* palloc arena shim: PostgreSQL frees these via memory-context reset; the
 * oracle mirrors that with a TLS pointer arena reset at every pg_diff_*
 * dispatcher entry, so error-path longjmp/ereturn/goto exits cannot leak.
 * (Three LSan incidents of the naive palloc->malloc mapping on 2026-07-31;
 * pattern proven on proofs/p1-lanej @ 7306d300196 — copied, not re-derived.
 * Final-exec allocations stay rooted in the arena, so LSan's exit scan is
 * quiet without any manual free().) */
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

static void
pg_diff_pfree_impl(void *p)
{
	int			i;

	for (i = 0; i < pg_diff_arena_n; i++)
	{
		if (pg_diff_arena[i] == p)
		{
			free(p);
			pg_diff_arena[i] = pg_diff_arena[--pg_diff_arena_n];
			return;
		}
	}
	/* abort-loud: freeing a pointer the arena never issued is a shim bug
	 * (double-free after reset, or a bare malloc that bypassed palloc). */
	assert(!"pfree of a pointer the arena never issued");
	abort();
}

#define palloc(n) pg_diff_palloc_impl(n)
#define palloc0(n) pg_diff_palloc0_impl(n)
#define repalloc(p, n) pg_diff_repalloc_impl((p), (n))
#define pfree(p) pg_diff_pfree_impl(p)


/* ==================== SECTION 1: shim header (PLUMBING ONLY) ====================
 *
 * Shims in this TU (documented per the header contract):
 *   - fixed-width typedefs matching c.h on LP64;
 *   - Float8GetDatum/DatumGetFloat8 = byval bit copies (USE_FLOAT8_BYVAL arm);
 *   - fmgr: PG_FUNCTION_ARGS unwrapped to a 2-slot fcinfo struct;
 *     DirectFunctionCall2 dispatches through the same struct;
 *   - elog(ERROR,...) -> errcode class 90 + longjmp (two reachable sites:
 *     getQuadrant impossible case (NaN coords), unrecognized strategy);
 *   - float_overflow_error/float_underflow_error -> errcode class 2 (22003)
 *     + longjmp, exactly the noreturn ereport arms of float.c;
 *   - MemoryContextSwitchTo -> no-op (single arena);
 *   - SYMBOL HYGIENE: every non-static vendored function is renamed via
 *     #define to a pg_quado_-prefixed TU-local name (GNU ld duplicate-symbol
 *     law; geo_ops.c/spgproc.c are shared upstream files).
 * Everything else below the VERBATIM markers is byte-verbatim vendored C
 * from postgres-src @ 62d6c7d3df (18.3 Stamp).
 */
#include <stdbool.h>
#include <stddef.h>
#include <math.h>
#include <setjmp.h>
typedef int16_t int16;
typedef int32_t int32;
typedef uint16_t uint16;
typedef uint32_t uint32;
typedef uint32 Oid;
typedef uintptr_t Datum;
typedef size_t Size;
typedef double float8;
#define Assert(x) ((void) 0)
#define unlikely(x) (x)
#define DatumGetPointer(X) ((char *) (X))
#define PointerGetDatum(X) ((Datum) (X))
#define POINTOID 600
#define VOIDOID 2278

/* Float8GetDatum/DatumGetFloat8: float8-byval (LP64) bit copies. */
static inline Datum Float8GetDatum(double f)
{ Datum d; memcpy(&d, &f, 8); return d; }
static inline double DatumGetFloat8(Datum d)
{ double f; memcpy(&f, &d, 8); return f; }
#define DatumGetBool(X) ((bool) ((X) != 0))
#define BoolGetDatum(X) ((Datum) ((X) ? 1 : 0))

/* utils/float.h helpers (verbatim semantics). */
static inline float8 get_float8_infinity(void) { return (float8) INFINITY; }
static inline float8 get_float8_nan(void) { return (float8) NAN; }

/* error channel: class 2 = 22003 float range; class 90 = elog(ERROR). */
static _Thread_local jmp_buf pg_quad_jmp;
#define PG_QUAD_ERR_VALUE_OUT_OF_RANGE 2
#define PG_QUAD_ERR_ELOG 90
static void float_overflow_error(void)
{ pg_diff_errcode = PG_QUAD_ERR_VALUE_OUT_OF_RANGE; longjmp(pg_quad_jmp, 1); }
static void float_underflow_error(void)
{ pg_diff_errcode = PG_QUAD_ERR_VALUE_OUT_OF_RANGE; longjmp(pg_quad_jmp, 1); }
static void pg_quado_elog_error(void)
{ pg_diff_errcode = PG_QUAD_ERR_ELOG; longjmp(pg_quad_jmp, 1); }
#define elog(elevel, ...) pg_quado_elog_error()

/* SYMBOL-HYGIENE renames (linkage names only; bodies stay verbatim). */
#define spg_quad_config pg_quado_spg_quad_config
#define spg_quad_choose pg_quado_spg_quad_choose
#define spg_quad_picksplit pg_quado_spg_quad_picksplit
#define spg_quad_inner_consistent pg_quado_spg_quad_inner_consistent
#define spg_quad_leaf_consistent pg_quado_spg_quad_leaf_consistent
#define spg_key_orderbys_distances pg_quado_spg_key_orderbys_distances
#define box_copy pg_quado_box_copy
#define pg_hypot pg_quado_pg_hypot
#define point_left pg_quado_point_left
#define point_right pg_quado_point_right
#define point_above pg_quado_point_above
#define point_below pg_quado_point_below
#define point_vert pg_quado_point_vert
#define point_horiz pg_quado_point_horiz
#define point_eq pg_quado_point_eq
#define point_distance pg_quado_point_distance
#define box_contain_pt pg_quado_box_contain_pt

/* fmgr shims (plumbing): all vendored callees take two Datum args. */
typedef struct { Datum arg[2]; } QuadFcinfo;
#define PG_FUNCTION_ARGS QuadFcinfo *fcinfo
#define PG_GETARG_POINTER(n) ((void *) fcinfo->arg[n])
#define PG_GETARG_POINT_P(n) ((Point *) fcinfo->arg[n])
#define PG_GETARG_BOX_P(n) ((BOX *) fcinfo->arg[n])
#define PG_RETURN_VOID() return (Datum) 0
#define PG_RETURN_BOOL(x) return BoolGetDatum(x)
#define PG_RETURN_FLOAT8(x) return Float8GetDatum(x)
static inline Datum
pg_quado_dfc2(Datum (*func) (QuadFcinfo *), Datum a0, Datum a1)
{
	QuadFcinfo	fc;

	fc.arg[0] = a0;
	fc.arg[1] = a1;
	return func(&fc);
}
#define DirectFunctionCall2(f, a, b) pg_quado_dfc2(f, (a), (b))

/* MemoryContext: single arena in this harness; switch is a no-op. */
typedef void *MemoryContext;
static MemoryContext MemoryContextSwitchTo(MemoryContext cxt) { (void) cxt; return NULL; }

/* access/stratnum.h @ 62d6c7d3df (VERBATIM values). */
#define RTLeftStrategyNumber			1
#define RTRightStrategyNumber			5
#define RTSameStrategyNumber			6
#define RTContainedByStrategyNumber		8
#define RTBelowStrategyNumber			10
#define RTAboveStrategyNumber			11
#define RTOldBelowStrategyNumber		29
#define RTOldAboveStrategyNumber		30

/* VERBATIM: utils/geo_decls.h @ 62d6c7d3df — EPSILON + FP fuzzy comparisons
 * (lines 41-89, EPSILON arm live), Point/BOX structs, HYPOT. */
#define EPSILON					1.0E-06

#ifdef EPSILON
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
#else
#define FPzero(A)				((A) == 0)
#define FPeq(A,B)				((A) == (B))
#define FPne(A,B)				((A) != (B))
#define FPlt(A,B)				((A) < (B))
#define FPle(A,B)				((A) <= (B))
#define FPgt(A,B)				((A) > (B))
#define FPge(A,B)				((A) >= (B))
#endif

#define HYPOT(A, B)				pg_hypot(A, B)

typedef struct
{
	float8		x,
				y;
} Point;

typedef struct
{
	Point		high,
				low;			/* corner POINTs */
} BOX;

#define DatumGetPointP(X) ((Point *) DatumGetPointer(X))
#define PointPGetDatum(X) PointerGetDatum(X)
#define DatumGetBoxP(X) ((BOX *) DatumGetPointer(X))
#define BoxPGetDatum(X) PointerGetDatum(X)

/* VERBATIM: utils/float.h @ 62d6c7d3df — float8_mi, float8_eq. */
static inline float8
float8_mi(const float8 val1, const float8 val2)
{
	float8		result;

	result = val1 - val2;
	if (unlikely(isinf(result)) && !isinf(val1) && !isinf(val2))
		float_overflow_error();

	return result;
}

static inline bool
float8_eq(const float8 val1, const float8 val2)
{
	return isnan(val1) ? isnan(val2) : !isnan(val2) && val1 == val2;
}

/* VERBATIM: utils/adt/geo_ops.c @ 62d6c7d3df — pg_hypot, point_eq_point,
 * box_contain_point, point_dt, and the SPTEST-called point relation
 * functions + box_contain_pt + point_distance. */
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
box_contain_point(BOX *box, Point *point)
{
	return box->high.x >= point->x && box->low.x <= point->x &&
		box->high.y >= point->y && box->low.y <= point->y;
}

static inline float8
point_dt(Point *pt1, Point *pt2)
{
	return HYPOT(float8_mi(pt1->x, pt2->x), float8_mi(pt1->y, pt2->y));
}

Datum
point_left(PG_FUNCTION_ARGS)
{
	Point	   *pt1 = PG_GETARG_POINT_P(0);
	Point	   *pt2 = PG_GETARG_POINT_P(1);

	PG_RETURN_BOOL(FPlt(pt1->x, pt2->x));
}

Datum
point_right(PG_FUNCTION_ARGS)
{
	Point	   *pt1 = PG_GETARG_POINT_P(0);
	Point	   *pt2 = PG_GETARG_POINT_P(1);

	PG_RETURN_BOOL(FPgt(pt1->x, pt2->x));
}

Datum
point_above(PG_FUNCTION_ARGS)
{
	Point	   *pt1 = PG_GETARG_POINT_P(0);
	Point	   *pt2 = PG_GETARG_POINT_P(1);

	PG_RETURN_BOOL(FPgt(pt1->y, pt2->y));
}

Datum
point_below(PG_FUNCTION_ARGS)
{
	Point	   *pt1 = PG_GETARG_POINT_P(0);
	Point	   *pt2 = PG_GETARG_POINT_P(1);

	PG_RETURN_BOOL(FPlt(pt1->y, pt2->y));
}

Datum
point_vert(PG_FUNCTION_ARGS)
{
	Point	   *pt1 = PG_GETARG_POINT_P(0);
	Point	   *pt2 = PG_GETARG_POINT_P(1);

	PG_RETURN_BOOL(FPeq(pt1->x, pt2->x));
}

Datum
point_horiz(PG_FUNCTION_ARGS)
{
	Point	   *pt1 = PG_GETARG_POINT_P(0);
	Point	   *pt2 = PG_GETARG_POINT_P(1);

	PG_RETURN_BOOL(FPeq(pt1->y, pt2->y));
}

Datum
point_eq(PG_FUNCTION_ARGS)
{
	Point	   *pt1 = PG_GETARG_POINT_P(0);
	Point	   *pt2 = PG_GETARG_POINT_P(1);

	PG_RETURN_BOOL(point_eq_point(pt1, pt2));
}

Datum
point_distance(PG_FUNCTION_ARGS)
{
	Point	   *pt1 = PG_GETARG_POINT_P(0);
	Point	   *pt2 = PG_GETARG_POINT_P(1);

	PG_RETURN_FLOAT8(point_dt(pt1, pt2));
}

Datum
box_contain_pt(PG_FUNCTION_ARGS)
{
	BOX		   *box = PG_GETARG_BOX_P(0);
	Point	   *pt = PG_GETARG_POINT_P(1);

	PG_RETURN_BOOL(box_contain_point(box, pt));
}

/* access/skey.h ScanKeyData: reduced to the two fields this opclass reads
 * (sk_strategy, sk_argument) — plumbing; the driver builds these from
 * parallel arrays, layout never crosses the FFI boundary. */
typedef struct
{
	uint16		sk_strategy;
	Datum		sk_argument;
} ScanKeyData;
typedef ScanKeyData *ScanKey;

/* VERBATIM: access/spgist.h @ 62d6c7d3df (opclass argument structs). */
typedef struct spgConfigIn
{
	Oid			attType;		/* Data type to be indexed */
} spgConfigIn;

typedef struct spgConfigOut
{
	Oid			prefixType;		/* Data type of inner-tuple prefixes */
	Oid			labelType;		/* Data type of inner-tuple node labels */
	Oid			leafType;		/* Data type of leaf-tuple values */
	bool		canReturnData;	/* Opclass can reconstruct original data */
	bool		longValuesOK;	/* Opclass can cope with values > 1 page */
} spgConfigOut;

/*
 * Argument structs for spg_choose method
 */
typedef struct spgChooseIn
{
	Datum		datum;			/* original datum to be indexed */
	Datum		leafDatum;		/* current datum to be stored at leaf */
	int			level;			/* current level (counting from zero) */

	/* Data from current inner tuple */
	bool		allTheSame;		/* tuple is marked all-the-same? */
	bool		hasPrefix;		/* tuple has a prefix? */
	Datum		prefixDatum;	/* if so, the prefix value */
	int			nNodes;			/* number of nodes in the inner tuple */
	Datum	   *nodeLabels;		/* node label values (NULL if none) */
} spgChooseIn;

typedef enum spgChooseResultType
{
	spgMatchNode = 1,			/* descend into existing node */
	spgAddNode,					/* add a node to the inner tuple */
	spgSplitTuple,				/* split inner tuple (change its prefix) */
} spgChooseResultType;

typedef struct spgChooseOut
{
	spgChooseResultType resultType; /* action code, see above */
	union
	{
		struct					/* results for spgMatchNode */
		{
			int			nodeN;	/* descend to this node (index from 0) */
			int			levelAdd;	/* increment level by this much */
			Datum		restDatum;	/* new leaf datum */
		}			matchNode;
		struct					/* results for spgAddNode */
		{
			Datum		nodeLabel;	/* new node's label */
			int			nodeN;	/* where to insert it (index from 0) */
		}			addNode;
		struct					/* results for spgSplitTuple */
		{
			/* Info to form new upper-level inner tuple with one child tuple */
			bool		prefixHasPrefix;	/* tuple should have a prefix? */
			Datum		prefixPrefixDatum;	/* if so, its value */
			int			prefixNNodes;	/* number of nodes */
			Datum	   *prefixNodeLabels;	/* their labels (or NULL for no
											 * labels) */
			int			childNodeN; /* which node gets child tuple */

			/* Info to form new lower-level inner tuple with all old nodes */
			bool		postfixHasPrefix;	/* tuple should have a prefix? */
			Datum		postfixPrefixDatum; /* if so, its value */
		}			splitTuple;
	}			result;
} spgChooseOut;

/*
 * Argument structs for spg_picksplit method
 */
typedef struct spgPickSplitIn
{
	int			nTuples;		/* number of leaf tuples */
	Datum	   *datums;			/* their datums (array of length nTuples) */
	int			level;			/* current level (counting from zero) */
} spgPickSplitIn;

typedef struct spgPickSplitOut
{
	bool		hasPrefix;		/* new inner tuple should have a prefix? */
	Datum		prefixDatum;	/* if so, its value */

	int			nNodes;			/* number of nodes for new inner tuple */
	Datum	   *nodeLabels;		/* their labels (or NULL for no labels) */

	int		   *mapTuplesToNodes;	/* node index for each leaf tuple */
	Datum	   *leafTupleDatums;	/* datum to store in each new leaf tuple */
} spgPickSplitOut;

/*
 * Argument structs for spg_inner_consistent method
 */
typedef struct spgInnerConsistentIn
{
	ScanKey		scankeys;		/* array of operators and comparison values */
	ScanKey		orderbys;		/* array of ordering operators and comparison
								 * values */
	int			nkeys;			/* length of scankeys array */
	int			norderbys;		/* length of orderbys array */

	Datum		reconstructedValue; /* value reconstructed at parent */
	void	   *traversalValue; /* opclass-specific traverse value */
	MemoryContext traversalMemoryContext;	/* put new traverse values here */
	int			level;			/* current level (counting from zero) */
	bool		returnData;		/* original data must be returned? */

	/* Data from current inner tuple */
	bool		allTheSame;		/* tuple is marked all-the-same? */
	bool		hasPrefix;		/* tuple has a prefix? */
	Datum		prefixDatum;	/* if so, the prefix value */
	int			nNodes;			/* number of nodes in the inner tuple */
	Datum	   *nodeLabels;		/* node label values (NULL if none) */
} spgInnerConsistentIn;

typedef struct spgInnerConsistentOut
{
	int			nNodes;			/* number of child nodes to be visited */
	int		   *nodeNumbers;	/* their indexes in the node array */
	int		   *levelAdds;		/* increment level by this much for each */
	Datum	   *reconstructedValues;	/* associated reconstructed values */
	void	  **traversalValues;	/* opclass-specific traverse values */
	double	  **distances;		/* associated distances */
} spgInnerConsistentOut;

/*
 * Argument structs for spg_leaf_consistent method
 */
typedef struct spgLeafConsistentIn
{
	ScanKey		scankeys;		/* array of operators and comparison values */
	ScanKey		orderbys;		/* array of ordering operators and comparison
								 * values */
	int			nkeys;			/* length of scankeys array */
	int			norderbys;		/* length of orderbys array */

	Datum		reconstructedValue; /* value reconstructed at parent */
	void	   *traversalValue; /* opclass-specific traverse value */
	int			level;			/* current level (counting from zero) */
	bool		returnData;		/* original data must be returned? */

	Datum		leafDatum;		/* datum in leaf tuple */
} spgLeafConsistentIn;

typedef struct spgLeafConsistentOut
{
	Datum		leafValue;		/* reconstructed original data, if any */
	bool		recheck;		/* set true if operator must be rechecked */
	bool		recheckDistances;	/* set true if distances must be rechecked */
	double	   *distances;		/* associated distances */
} spgLeafConsistentOut;

/* VERBATIM: access/spgist/spgproc.c @ 62d6c7d3df lines 25-88
 * (point_point_distance, point_box_distance, spg_key_orderbys_distances,
 * box_copy). */
#define point_point_distance(p1,p2) \
	DatumGetFloat8(DirectFunctionCall2(point_distance, \
									   PointPGetDatum(p1), PointPGetDatum(p2)))

/* Point-box distance in the assumption that box is aligned by axis */
static double
point_box_distance(Point *point, BOX *box)
{
	double		dx,
				dy;

	if (isnan(point->x) || isnan(box->low.x) ||
		isnan(point->y) || isnan(box->low.y))
		return get_float8_nan();

	if (point->x < box->low.x)
		dx = box->low.x - point->x;
	else if (point->x > box->high.x)
		dx = point->x - box->high.x;
	else
		dx = 0.0;

	if (point->y < box->low.y)
		dy = box->low.y - point->y;
	else if (point->y > box->high.y)
		dy = point->y - box->high.y;
	else
		dy = 0.0;

	return HYPOT(dx, dy);
}

/*
 * Returns distances from given key to array of ordering scan keys.  Leaf key
 * is expected to be point, non-leaf key is expected to be box.  Scan key
 * arguments are expected to be points.
 */
double *
spg_key_orderbys_distances(Datum key, bool isLeaf,
						   ScanKey orderbys, int norderbys)
{
	int			sk_num;
	double	   *distances = (double *) palloc(norderbys * sizeof(double)),
			   *distance = distances;

	for (sk_num = 0; sk_num < norderbys; ++sk_num, ++orderbys, ++distance)
	{
		Point	   *point = DatumGetPointP(orderbys->sk_argument);

		*distance = isLeaf ? point_point_distance(point, DatumGetPointP(key))
			: point_box_distance(point, DatumGetBoxP(key));
	}

	return distances;
}

BOX *
box_copy(BOX *orig)
{
	BOX		   *result = palloc(sizeof(BOX));

	*result = *orig;
	return result;
}

/* VERBATIM: access/spgist/spgquadtreeproc.c @ 62d6c7d3df lines 26-471
 * (spg_quad_config, getQuadrant, getQuadrantArea, spg_quad_choose,
 * USE_MEDIAN block (not defined upstream; dead verbatim), spg_quad_picksplit,
 * spg_quad_inner_consistent, spg_quad_leaf_consistent). SPTEST expands
 * through the DirectFunctionCall2 shim above; qsort is unreachable
 * (USE_MEDIAN undefined, matching the upstream build). */
Datum
spg_quad_config(PG_FUNCTION_ARGS)
{
	/* spgConfigIn *cfgin = (spgConfigIn *) PG_GETARG_POINTER(0); */
	spgConfigOut *cfg = (spgConfigOut *) PG_GETARG_POINTER(1);

	cfg->prefixType = POINTOID;
	cfg->labelType = VOIDOID;	/* we don't need node labels */
	cfg->canReturnData = true;
	cfg->longValuesOK = false;
	PG_RETURN_VOID();
}

#define SPTEST(f, x, y) \
	DatumGetBool(DirectFunctionCall2(f, PointPGetDatum(x), PointPGetDatum(y)))

/*
 * Determine which quadrant a point falls into, relative to the centroid.
 *
 * Quadrants are identified like this:
 *
 *	 4	|  1
 *	----+-----
 *	 3	|  2
 *
 * Points on one of the axes are taken to lie in the lowest-numbered
 * adjacent quadrant.
 */
static int16
getQuadrant(Point *centroid, Point *tst)
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

	elog(ERROR, "getQuadrant: impossible case");
	return 0;
}

/* Returns bounding box of a given quadrant inside given bounding box */
static BOX *
getQuadrantArea(BOX *bbox, Point *centroid, int quadrant)
{
	BOX		   *result = (BOX *) palloc(sizeof(BOX));

	switch (quadrant)
	{
		case 1:
			result->high = bbox->high;
			result->low = *centroid;
			break;
		case 2:
			result->high.x = bbox->high.x;
			result->high.y = centroid->y;
			result->low.x = centroid->x;
			result->low.y = bbox->low.y;
			break;
		case 3:
			result->high = *centroid;
			result->low = bbox->low;
			break;
		case 4:
			result->high.x = centroid->x;
			result->high.y = bbox->high.y;
			result->low.x = bbox->low.x;
			result->low.y = centroid->y;
			break;
	}

	return result;
}

Datum
spg_quad_choose(PG_FUNCTION_ARGS)
{
	spgChooseIn *in = (spgChooseIn *) PG_GETARG_POINTER(0);
	spgChooseOut *out = (spgChooseOut *) PG_GETARG_POINTER(1);
	Point	   *inPoint = DatumGetPointP(in->datum),
			   *centroid;

	if (in->allTheSame)
	{
		out->resultType = spgMatchNode;
		/* nodeN will be set by core */
		out->result.matchNode.levelAdd = 0;
		out->result.matchNode.restDatum = PointPGetDatum(inPoint);
		PG_RETURN_VOID();
	}

	Assert(in->hasPrefix);
	centroid = DatumGetPointP(in->prefixDatum);

	Assert(in->nNodes == 4);

	out->resultType = spgMatchNode;
	out->result.matchNode.nodeN = getQuadrant(centroid, inPoint) - 1;
	out->result.matchNode.levelAdd = 0;
	out->result.matchNode.restDatum = PointPGetDatum(inPoint);

	PG_RETURN_VOID();
}

#ifdef USE_MEDIAN
static int
x_cmp(const void *a, const void *b, void *arg)
{
	Point	   *pa = *(Point **) a;
	Point	   *pb = *(Point **) b;

	if (pa->x == pb->x)
		return 0;
	return (pa->x > pb->x) ? 1 : -1;
}

static int
y_cmp(const void *a, const void *b, void *arg)
{
	Point	   *pa = *(Point **) a;
	Point	   *pb = *(Point **) b;

	if (pa->y == pb->y)
		return 0;
	return (pa->y > pb->y) ? 1 : -1;
}
#endif

Datum
spg_quad_picksplit(PG_FUNCTION_ARGS)
{
	spgPickSplitIn *in = (spgPickSplitIn *) PG_GETARG_POINTER(0);
	spgPickSplitOut *out = (spgPickSplitOut *) PG_GETARG_POINTER(1);
	int			i;
	Point	   *centroid;

#ifdef USE_MEDIAN
	/* Use the median values of x and y as the centroid point */
	Point	  **sorted;

	sorted = palloc(sizeof(*sorted) * in->nTuples);
	for (i = 0; i < in->nTuples; i++)
		sorted[i] = DatumGetPointP(in->datums[i]);

	centroid = palloc(sizeof(*centroid));

	qsort(sorted, in->nTuples, sizeof(*sorted), x_cmp);
	centroid->x = sorted[in->nTuples >> 1]->x;
	qsort(sorted, in->nTuples, sizeof(*sorted), y_cmp);
	centroid->y = sorted[in->nTuples >> 1]->y;
#else
	/* Use the average values of x and y as the centroid point */
	centroid = palloc0(sizeof(*centroid));

	for (i = 0; i < in->nTuples; i++)
	{
		centroid->x += DatumGetPointP(in->datums[i])->x;
		centroid->y += DatumGetPointP(in->datums[i])->y;
	}

	centroid->x /= in->nTuples;
	centroid->y /= in->nTuples;
#endif

	out->hasPrefix = true;
	out->prefixDatum = PointPGetDatum(centroid);

	out->nNodes = 4;
	out->nodeLabels = NULL;		/* we don't need node labels */

	out->mapTuplesToNodes = palloc(sizeof(int) * in->nTuples);
	out->leafTupleDatums = palloc(sizeof(Datum) * in->nTuples);

	for (i = 0; i < in->nTuples; i++)
	{
		Point	   *p = DatumGetPointP(in->datums[i]);
		int			quadrant = getQuadrant(centroid, p) - 1;

		out->leafTupleDatums[i] = PointPGetDatum(p);
		out->mapTuplesToNodes[i] = quadrant;
	}

	PG_RETURN_VOID();
}


Datum
spg_quad_inner_consistent(PG_FUNCTION_ARGS)
{
	spgInnerConsistentIn *in = (spgInnerConsistentIn *) PG_GETARG_POINTER(0);
	spgInnerConsistentOut *out = (spgInnerConsistentOut *) PG_GETARG_POINTER(1);
	Point	   *centroid;
	BOX			infbbox;
	BOX		   *bbox = NULL;
	int			which;
	int			i;

	Assert(in->hasPrefix);
	centroid = DatumGetPointP(in->prefixDatum);

	/*
	 * When ordering scan keys are specified, we've to calculate distance for
	 * them.  In order to do that, we need calculate bounding boxes for all
	 * children nodes.  Calculation of those bounding boxes on non-zero level
	 * require knowledge of bounding box of upper node.  So, we save bounding
	 * boxes to traversalValues.
	 */
	if (in->norderbys > 0)
	{
		out->distances = (double **) palloc(sizeof(double *) * in->nNodes);
		out->traversalValues = (void **) palloc(sizeof(void *) * in->nNodes);

		if (in->level == 0)
		{
			double		inf = get_float8_infinity();

			infbbox.high.x = inf;
			infbbox.high.y = inf;
			infbbox.low.x = -inf;
			infbbox.low.y = -inf;
			bbox = &infbbox;
		}
		else
		{
			bbox = in->traversalValue;
			Assert(bbox);
		}
	}

	if (in->allTheSame)
	{
		/* Report that all nodes should be visited */
		out->nNodes = in->nNodes;
		out->nodeNumbers = (int *) palloc(sizeof(int) * in->nNodes);
		for (i = 0; i < in->nNodes; i++)
		{
			out->nodeNumbers[i] = i;

			if (in->norderbys > 0)
			{
				MemoryContext oldCtx = MemoryContextSwitchTo(in->traversalMemoryContext);

				/* Use parent quadrant box as traversalValue */
				BOX		   *quadrant = box_copy(bbox);

				MemoryContextSwitchTo(oldCtx);

				out->traversalValues[i] = quadrant;
				out->distances[i] = spg_key_orderbys_distances(BoxPGetDatum(quadrant), false,
															   in->orderbys, in->norderbys);
			}
		}
		PG_RETURN_VOID();
	}

	Assert(in->nNodes == 4);

	/* "which" is a bitmask of quadrants that satisfy all constraints */
	which = (1 << 1) | (1 << 2) | (1 << 3) | (1 << 4);

	for (i = 0; i < in->nkeys; i++)
	{
		Point	   *query = DatumGetPointP(in->scankeys[i].sk_argument);
		BOX		   *boxQuery;

		switch (in->scankeys[i].sk_strategy)
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
				which &= (1 << getQuadrant(centroid, query));
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

				/*
				 * For this operator, the query is a box not a point.  We
				 * cheat to the extent of assuming that DatumGetPointP won't
				 * do anything that would be bad for a pointer-to-box.
				 */
				boxQuery = DatumGetBoxP(in->scankeys[i].sk_argument);

				if (DatumGetBool(DirectFunctionCall2(box_contain_pt,
													 PointerGetDatum(boxQuery),
													 PointerGetDatum(centroid))))
				{
					/* centroid is in box, so all quadrants are OK */
				}
				else
				{
					/* identify quadrant(s) containing all corners of box */
					Point		p;
					int			r = 0;

					p = boxQuery->low;
					r |= 1 << getQuadrant(centroid, &p);
					p.y = boxQuery->high.y;
					r |= 1 << getQuadrant(centroid, &p);
					p = boxQuery->high;
					r |= 1 << getQuadrant(centroid, &p);
					p.x = boxQuery->low.x;
					r |= 1 << getQuadrant(centroid, &p);

					which &= r;
				}
				break;
			default:
				elog(ERROR, "unrecognized strategy number: %d",
					 in->scankeys[i].sk_strategy);
				break;
		}

		if (which == 0)
			break;				/* no need to consider remaining conditions */
	}

	out->levelAdds = palloc(sizeof(int) * 4);
	for (i = 0; i < 4; ++i)
		out->levelAdds[i] = 1;

	/* We must descend into the quadrant(s) identified by which */
	out->nodeNumbers = (int *) palloc(sizeof(int) * 4);
	out->nNodes = 0;

	for (i = 1; i <= 4; i++)
	{
		if (which & (1 << i))
		{
			out->nodeNumbers[out->nNodes] = i - 1;

			if (in->norderbys > 0)
			{
				MemoryContext oldCtx = MemoryContextSwitchTo(in->traversalMemoryContext);
				BOX		   *quadrant = getQuadrantArea(bbox, centroid, i);

				MemoryContextSwitchTo(oldCtx);

				out->traversalValues[out->nNodes] = quadrant;

				out->distances[out->nNodes] = spg_key_orderbys_distances(BoxPGetDatum(quadrant), false,
																		 in->orderbys, in->norderbys);
			}

			out->nNodes++;
		}
	}

	PG_RETURN_VOID();
}


Datum
spg_quad_leaf_consistent(PG_FUNCTION_ARGS)
{
	spgLeafConsistentIn *in = (spgLeafConsistentIn *) PG_GETARG_POINTER(0);
	spgLeafConsistentOut *out = (spgLeafConsistentOut *) PG_GETARG_POINTER(1);
	Point	   *datum = DatumGetPointP(in->leafDatum);
	bool		res;
	int			i;

	/* all tests are exact */
	out->recheck = false;

	/* leafDatum is what it is... */
	out->leafValue = in->leafDatum;

	/* Perform the required comparison(s) */
	res = true;
	for (i = 0; i < in->nkeys; i++)
	{
		Point	   *query = DatumGetPointP(in->scankeys[i].sk_argument);

		switch (in->scankeys[i].sk_strategy)
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

				/*
				 * For this operator, the query is a box not a point.  We
				 * cheat to the extent of assuming that DatumGetPointP won't
				 * do anything that would be bad for a pointer-to-box.
				 */
				res = SPTEST(box_contain_pt, query, datum);
				break;
			default:
				elog(ERROR, "unrecognized strategy number: %d",
					 in->scankeys[i].sk_strategy);
				break;
		}

		if (!res)
			break;
	}

	if (res && in->norderbys > 0)
		/* ok, it passes -> let's compute the distances */
		out->distances = spg_key_orderbys_distances(in->leafDatum, true,
													in->orderbys, in->norderbys);

	PG_RETURN_BOOL(res);
}

/* ========== SECTION 2: fuzz-facing driver entries (NOT Postgres code) ===== */

/*
 * Status protocol: 0 = ok; 100+class = vendored C errored
 * (102 = 22003 float value-out-of-range; 190 = elog(ERROR) — getQuadrant
 * impossible case (NaN coords) or unrecognized strategy, both driver-fenced
 * C-parity arms).
 * All double* buffers are 8-aligned caller buffers (points = 2 doubles,
 * boxes = 4 doubles: high.x, high.y, low.x, low.y — struct order).
 */

int
pg_diff_quad_config(uint32 *prefix_type, uint32 *label_type, uint32 *leaf_type,
					int *can_return, int *long_ok)
{
	spgConfigIn in;
	spgConfigOut out;
	QuadFcinfo	fc;

	pg_diff_arena_reset();
	pg_diff_errcode = 0;
	memset(&in, 0, sizeof(in));
	memset(&out, 0, sizeof(out));
	fc.arg[0] = PointerGetDatum(&in);
	fc.arg[1] = PointerGetDatum(&out);
	(void) spg_quad_config(&fc);
	*prefix_type = out.prefixType;
	*label_type = out.labelType;
	*leaf_type = out.leafType;
	*can_return = out.canReturnData ? 1 : 0;
	*long_ok = out.longValuesOK ? 1 : 0;
	return 0;
}

int
pg_diff_quad_choose(int all_the_same, const double *prefix2, int level,
					const double *pt2, int *node_n, int *level_add,
					double *rest2)
{
	spgChooseIn in;
	spgChooseOut out;
	QuadFcinfo	fc;
	Point	   *rest;

	pg_diff_arena_reset();
	pg_diff_errcode = 0;
	if (setjmp(pg_quad_jmp) != 0)
		return 100 + pg_diff_errcode;
	memset(&in, 0, sizeof(in));
	memset(&out, 0, sizeof(out));
	in.datum = PointerGetDatum(pt2);
	in.leafDatum = PointerGetDatum(pt2);
	in.level = level;
	in.allTheSame = all_the_same != 0;
	in.hasPrefix = true;
	in.prefixDatum = PointerGetDatum(prefix2);
	in.nNodes = 4;
	in.nodeLabels = NULL;
	fc.arg[0] = PointerGetDatum(&in);
	fc.arg[1] = PointerGetDatum(&out);
	(void) spg_quad_choose(&fc);
	if (out.resultType != spgMatchNode)
		abort();				/* this opclass only ever matches */
	*node_n = out.result.matchNode.nodeN;
	*level_add = out.result.matchNode.levelAdd;
	rest = DatumGetPointP(out.result.matchNode.restDatum);
	rest2[0] = rest->x;
	rest2[1] = rest->y;
	return 0;
}

int
pg_diff_quad_picksplit(int n, const double *pts2n, int level, int *has_prefix,
					   double *centroid2, int *n_nodes, int *map,
					   double *leaf2n)
{
	spgPickSplitIn in;
	spgPickSplitOut out;
	QuadFcinfo	fc;
	Datum	   *datums;
	int			i;

	pg_diff_arena_reset();
	pg_diff_errcode = 0;
	if (setjmp(pg_quad_jmp) != 0)
		return 100 + pg_diff_errcode;
	memset(&in, 0, sizeof(in));
	memset(&out, 0, sizeof(out));
	datums = (Datum *) palloc(sizeof(Datum) * n);
	for (i = 0; i < n; i++)
		datums[i] = PointerGetDatum(pts2n + 2 * i);
	in.nTuples = n;
	in.datums = datums;
	in.level = level;
	fc.arg[0] = PointerGetDatum(&in);
	fc.arg[1] = PointerGetDatum(&out);
	(void) spg_quad_picksplit(&fc);
	*has_prefix = out.hasPrefix ? 1 : 0;
	if (out.hasPrefix)
	{
		Point	   *c = DatumGetPointP(out.prefixDatum);

		centroid2[0] = c->x;
		centroid2[1] = c->y;
	}
	*n_nodes = out.nNodes;
	for (i = 0; i < n; i++)
	{
		Point	   *p = DatumGetPointP(out.leafTupleDatums[i]);

		map[i] = out.mapTuplesToNodes[i];
		leaf2n[2 * i] = p->x;
		leaf2n[2 * i + 1] = p->y;
	}
	return 0;
}

int
pg_diff_quad_inner(int all_the_same, int in_nnodes, const double *prefix2,
				   int level, int nkeys, const uint16 *strategies,
				   const double *args4, int norderbys, const double *obys2,
				   int has_tv, const double *tv4, int *n_nodes,
				   int *node_numbers, int *level_adds, int *has_level_adds,
				   double *tvout4, double *distout)
{
	spgInnerConsistentIn in;
	spgInnerConsistentOut out;
	QuadFcinfo	fc;
	ScanKeyData *keys;
	ScanKeyData *okeys;
	int			i,
				j;

	pg_diff_arena_reset();
	pg_diff_errcode = 0;
	if (setjmp(pg_quad_jmp) != 0)
		return 100 + pg_diff_errcode;
	memset(&in, 0, sizeof(in));
	memset(&out, 0, sizeof(out));
	keys = (ScanKeyData *) palloc(sizeof(ScanKeyData) * (nkeys > 0 ? nkeys : 1));
	for (i = 0; i < nkeys; i++)
	{
		keys[i].sk_strategy = strategies[i];
		keys[i].sk_argument = PointerGetDatum(args4 + 4 * i);
	}
	okeys = (ScanKeyData *) palloc(sizeof(ScanKeyData) * (norderbys > 0 ? norderbys : 1));
	for (i = 0; i < norderbys; i++)
	{
		okeys[i].sk_strategy = 0;
		okeys[i].sk_argument = PointerGetDatum(obys2 + 2 * i);
	}
	in.scankeys = keys;
	in.orderbys = okeys;
	in.nkeys = nkeys;
	in.norderbys = norderbys;
	in.traversalValue = has_tv ? (void *) tv4 : NULL;
	in.traversalMemoryContext = NULL;
	in.level = level;
	in.returnData = false;
	in.allTheSame = all_the_same != 0;
	in.hasPrefix = true;
	in.prefixDatum = PointerGetDatum(prefix2);
	in.nNodes = in_nnodes;
	in.nodeLabels = NULL;
	fc.arg[0] = PointerGetDatum(&in);
	fc.arg[1] = PointerGetDatum(&out);
	(void) spg_quad_inner_consistent(&fc);
	*n_nodes = out.nNodes;
	*has_level_adds = out.levelAdds != NULL ? 1 : 0;
	for (i = 0; i < out.nNodes; i++)
	{
		node_numbers[i] = out.nodeNumbers[i];
		if (out.levelAdds != NULL)
			level_adds[i] = out.levelAdds[i];
		if (norderbys > 0)
		{
			BOX		   *tv = (BOX *) out.traversalValues[i];

			tvout4[4 * i] = tv->high.x;
			tvout4[4 * i + 1] = tv->high.y;
			tvout4[4 * i + 2] = tv->low.x;
			tvout4[4 * i + 3] = tv->low.y;
			for (j = 0; j < norderbys; j++)
				distout[norderbys * i + j] = out.distances[i][j];
		}
	}
	return 0;
}

int
pg_diff_quad_leaf(const double *leaf2, int level, int nkeys,
				  const uint16 *strategies, const double *args4,
				  int norderbys, const double *obys2, int *res,
				  int *recheck, double *leafval2, double *dist)
{
	spgLeafConsistentIn in;
	spgLeafConsistentOut out;
	QuadFcinfo	fc;
	ScanKeyData *keys;
	ScanKeyData *okeys;
	Datum		ret;
	Point	   *lv;
	int			i;

	pg_diff_arena_reset();
	pg_diff_errcode = 0;
	if (setjmp(pg_quad_jmp) != 0)
		return 100 + pg_diff_errcode;
	memset(&in, 0, sizeof(in));
	memset(&out, 0, sizeof(out));
	keys = (ScanKeyData *) palloc(sizeof(ScanKeyData) * (nkeys > 0 ? nkeys : 1));
	for (i = 0; i < nkeys; i++)
	{
		keys[i].sk_strategy = strategies[i];
		keys[i].sk_argument = PointerGetDatum(args4 + 4 * i);
	}
	okeys = (ScanKeyData *) palloc(sizeof(ScanKeyData) * (norderbys > 0 ? norderbys : 1));
	for (i = 0; i < norderbys; i++)
	{
		okeys[i].sk_strategy = 0;
		okeys[i].sk_argument = PointerGetDatum(obys2 + 2 * i);
	}
	in.scankeys = keys;
	in.orderbys = okeys;
	in.nkeys = nkeys;
	in.norderbys = norderbys;
	in.level = level;
	in.returnData = false;
	in.leafDatum = PointerGetDatum(leaf2);
	fc.arg[0] = PointerGetDatum(&in);
	fc.arg[1] = PointerGetDatum(&out);
	ret = spg_quad_leaf_consistent(&fc);
	*res = DatumGetBool(ret) ? 1 : 0;
	*recheck = out.recheck ? 1 : 0;
	lv = DatumGetPointP(out.leafValue);
	leafval2[0] = lv->x;
	leafval2[1] = lv->y;
	if (DatumGetBool(ret) && norderbys > 0)
		for (i = 0; i < norderbys; i++)
			dist[i] = out.distances[i];
	return 0;
}
