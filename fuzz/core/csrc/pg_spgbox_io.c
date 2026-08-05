/*
 * pg_spgbox_io.c: vendored PostgreSQL C oracle for the spgbox_diff differential
 * fuzz target (100%-coverage campaign; crate crates/backend/access/spgist/spgist_box).
 *
 * GENERATED SKELETON (fuzz/scaffold.py) — NOT yet a valid oracle. Every
 * TODO(scaffold) paste site below must be filled with VERBATIM upstream C,
 * and every #error compile gate removed WITH its paste, before the
 * .file("csrc/pg_spgbox_io.c") line in core/build.rs is uncommented. A
 * half-filled shim can therefore never silently build or link.
 *
 * Provenance (fill in as you paste; follow csrc/pg_uuid_io.c):
 *   - Vendor sections 1..N byte-for-byte from src/backend/utils/adt/geo_spgist.c
 *     @ postgres-src 62d6c7d3df6287f1bd83199c1a746e50d31571a0
 *     (PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df); re-verify against the repo's vendored ground-truth
 *     checkout ../pgrust-fabled/vendor/postgres-src before pasting).
 *   - Functions to vendor: spg_bbox_quad_config, spg_poly_quad_compress, spg_box_quad_config, spg_box_quad_choose, spg_box_quad_picksplit, spg_box_quad_inner_consistent, spg_box_quad_leaf_consistent.
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
 * Shims (documented per the header contract):
 *   - fixed-width typedefs matching c.h on LP64; Datum = uintptr_t;
 *   - fmgr: PG_FUNCTION_ARGS unwrapped to a 2-slot fcinfo struct;
 *     DirectFunctionCall2 dispatches through the same struct;
 *   - DatumGetPolygonP / PG_GETARG_POLYGON_P = plain pointer cast (no toast
 *     in the harness; driver passes 8-aligned unpacked varlena images);
 *   - elog(ERROR,...) -> errcode class 90 + longjmp (two reachable sites:
 *     unrecognized scankey subtype, unrecognized strategy — driver-fenced);
 *   - float_overflow_error/float_underflow_error -> class 2 (22003) +
 *     longjmp, exactly float.c's noreturn ereport arms;
 *   - MemoryContextSwitchTo -> no-op (single arena);
 *   - qsort -> box_pg_qsort, the VERBATIM lib/sort_template.h pg_qsort
 *     instantiation below (port.h maps qsort -> pg_qsort in the backend;
 *     NEVER libc qsort — spgist_kdtree oracle-integrity incident);
 *   - SYMBOL HYGIENE: every vendored non-static function renamed to a
 *     pg_boxo_-prefixed TU-local name via #define.
 * Everything below the VERBATIM markers is byte-verbatim vendored C
 * from postgres-src @ 62d6c7d3df (18.3 Stamp).
 */
#include <stdbool.h>
#include <stddef.h>
#include <math.h>
#include <setjmp.h>
typedef int8_t int8;
typedef int16_t int16;
typedef int32_t int32;
typedef uint8_t uint8;
typedef uint16_t uint16;
typedef uint32_t uint32;
typedef uint32 Oid;
typedef uintptr_t Datum;
typedef size_t Size;
typedef double float8;
typedef uint16 StrategyNumber;
#define Assert(x) ((void) 0)
#define unlikely(x) (x)
#define DatumGetPointer(X) ((char *) (X))
#define PointerGetDatum(X) ((Datum) (X))
#define BOXOID 603
#define POLYGONOID 604
#define VOIDOID 2278
/* utils/fmgroids.h: dist_polyp (pg_proc.dat oid 3292). */
#define F_DIST_POLYP 3292
#define FLEXIBLE_ARRAY_MEMBER /* empty */

static inline Datum Float8GetDatum(double f)
{ Datum d; memcpy(&d, &f, 8); return d; }
static inline double DatumGetFloat8(Datum d)
{ double f; memcpy(&f, &d, 8); return f; }
#define DatumGetBool(X) ((bool) ((X) != 0))
#define BoolGetDatum(X) ((Datum) ((X) ? 1 : 0))

static inline float8 get_float8_infinity(void) { return (float8) INFINITY; }
static inline float8 get_float8_nan(void) { return (float8) NAN; }

static _Thread_local jmp_buf pg_box_jmp;
#define PG_BOX_ERR_VALUE_OUT_OF_RANGE 2
#define PG_BOX_ERR_ELOG 90
static void float_overflow_error(void)
{ pg_diff_errcode = PG_BOX_ERR_VALUE_OUT_OF_RANGE; longjmp(pg_box_jmp, 1); }
static void float_underflow_error(void)
{ pg_diff_errcode = PG_BOX_ERR_VALUE_OUT_OF_RANGE; longjmp(pg_box_jmp, 1); }
static void pg_boxo_elog_error(void)
{ pg_diff_errcode = PG_BOX_ERR_ELOG; longjmp(pg_box_jmp, 1); }
#define elog(elevel, ...) pg_boxo_elog_error()

/* SYMBOL-HYGIENE renames (linkage names only; bodies stay verbatim). */
#define spg_box_quad_config pg_boxo_spg_box_quad_config
#define spg_box_quad_choose pg_boxo_spg_box_quad_choose
#define spg_box_quad_picksplit pg_boxo_spg_box_quad_picksplit
#define spg_box_quad_inner_consistent pg_boxo_spg_box_quad_inner_consistent
#define spg_box_quad_leaf_consistent pg_boxo_spg_box_quad_leaf_consistent
#define spg_bbox_quad_config pg_boxo_spg_bbox_quad_config
#define spg_poly_quad_compress pg_boxo_spg_poly_quad_compress
#define spg_key_orderbys_distances pg_boxo_spg_key_orderbys_distances
#define box_copy pg_boxo_box_copy
#define pg_hypot pg_boxo_pg_hypot
#define box_overlap pg_boxo_box_overlap
#define box_contain pg_boxo_box_contain
#define box_contained pg_boxo_box_contained
#define box_same pg_boxo_box_same
#define box_left pg_boxo_box_left
#define box_overleft pg_boxo_box_overleft
#define box_right pg_boxo_box_right
#define box_overright pg_boxo_box_overright
#define box_above pg_boxo_box_above
#define box_overabove pg_boxo_box_overabove
#define box_below pg_boxo_box_below
#define box_overbelow pg_boxo_box_overbelow
#define point_distance pg_boxo_point_distance

/* fmgr shims (plumbing). */
typedef struct { Datum arg[2]; } BoxFcinfo;
#define PG_FUNCTION_ARGS BoxFcinfo *fcinfo
#define PG_GETARG_POINTER(n) ((void *) fcinfo->arg[n])
#define PG_GETARG_BOX_P(n) ((BOX *) fcinfo->arg[n])
#define PG_GETARG_POINT_P(n) ((Point *) fcinfo->arg[n])
#define PG_GETARG_POLYGON_P(n) ((POLYGON *) fcinfo->arg[n])
#define PG_RETURN_VOID() return (Datum) 0
#define PG_RETURN_BOOL(x) return BoolGetDatum(x)
#define PG_RETURN_FLOAT8(x) return Float8GetDatum(x)
#define PG_RETURN_BOX_P(x) return PointerGetDatum(x)
static inline Datum
pg_boxo_dfc2(Datum (*func) (BoxFcinfo *), Datum a0, Datum a1)
{
	BoxFcinfo	fc;

	fc.arg[0] = a0;
	fc.arg[1] = a1;
	return func(&fc);
}
#define DirectFunctionCall2(f, a, b) pg_boxo_dfc2(f, (a), (b))

typedef void *MemoryContext;
static MemoryContext MemoryContextSwitchTo(MemoryContext cxt) { (void) cxt; return NULL; }

/* access/stratnum.h @ 62d6c7d3df (VERBATIM values). */
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

/* VERBATIM: utils/geo_decls.h @ 62d6c7d3df — EPSILON + FP fuzzy block. */
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

/* VERBATIM: utils/geo_decls.h POLYGON (vl_len_ header + npts + boundbox). */
typedef struct
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	int32		npts;
	BOX			boundbox;
	Point		p[FLEXIBLE_ARRAY_MEMBER];
} POLYGON;

#define DatumGetPointP(X) ((Point *) DatumGetPointer(X))
#define PointPGetDatum(X) PointerGetDatum(X)
#define DatumGetBoxP(X) ((BOX *) DatumGetPointer(X))
#define BoxPGetDatum(X) PointerGetDatum(X)
/* no toast in the harness: unpacked, 8-aligned images only */
#define DatumGetPolygonP(X) ((POLYGON *) DatumGetPointer(X))

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

static inline bool
point_eq_point(Point *pt1, Point *pt2)
{
	/* If any NaNs are involved, insist on exact equality */
	if (unlikely(isnan(pt1->x) || isnan(pt1->y) ||
				 isnan(pt2->x) || isnan(pt2->y)))
		return (float8_eq(pt1->x, pt2->x) && float8_eq(pt1->y, pt2->y));

	return (FPeq(pt1->x, pt2->x) && FPeq(pt1->y, pt2->y));
}

/* VERBATIM: utils/adt/geo_ops.c @ 62d6c7d3df — pg_hypot, box_ov,
 * box_contain_box, point_dt (spgproc dependency), and the twelve box
 * relation functions the leaf arm dispatches through. */
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

static inline float8
point_dt(Point *pt1, Point *pt2)
{
	return HYPOT(float8_mi(pt1->x, pt2->x), float8_mi(pt1->y, pt2->y));
}

Datum
box_overlap(PG_FUNCTION_ARGS)
{
	BOX		   *box1 = PG_GETARG_BOX_P(0);
	BOX		   *box2 = PG_GETARG_BOX_P(1);

	PG_RETURN_BOOL(box_ov(box1, box2));
}

Datum
box_contain(PG_FUNCTION_ARGS)
{
	BOX		   *box1 = PG_GETARG_BOX_P(0);
	BOX		   *box2 = PG_GETARG_BOX_P(1);

	PG_RETURN_BOOL(box_contain_box(box1, box2));
}

Datum
box_contained(PG_FUNCTION_ARGS)
{
	BOX		   *box1 = PG_GETARG_BOX_P(0);
	BOX		   *box2 = PG_GETARG_BOX_P(1);

	PG_RETURN_BOOL(box_contain_box(box2, box1));
}

Datum
box_same(PG_FUNCTION_ARGS)
{
	BOX		   *box1 = PG_GETARG_BOX_P(0);
	BOX		   *box2 = PG_GETARG_BOX_P(1);

	PG_RETURN_BOOL(point_eq_point(&box1->high, &box2->high) &&
				   point_eq_point(&box1->low, &box2->low));
}

Datum
box_left(PG_FUNCTION_ARGS)
{
	BOX		   *box1 = PG_GETARG_BOX_P(0);
	BOX		   *box2 = PG_GETARG_BOX_P(1);

	PG_RETURN_BOOL(FPlt(box1->high.x, box2->low.x));
}

Datum
box_overleft(PG_FUNCTION_ARGS)
{
	BOX		   *box1 = PG_GETARG_BOX_P(0);
	BOX		   *box2 = PG_GETARG_BOX_P(1);

	PG_RETURN_BOOL(FPle(box1->high.x, box2->high.x));
}

Datum
box_right(PG_FUNCTION_ARGS)
{
	BOX		   *box1 = PG_GETARG_BOX_P(0);
	BOX		   *box2 = PG_GETARG_BOX_P(1);

	PG_RETURN_BOOL(FPgt(box1->low.x, box2->high.x));
}

Datum
box_overright(PG_FUNCTION_ARGS)
{
	BOX		   *box1 = PG_GETARG_BOX_P(0);
	BOX		   *box2 = PG_GETARG_BOX_P(1);

	PG_RETURN_BOOL(FPge(box1->low.x, box2->low.x));
}

Datum
box_above(PG_FUNCTION_ARGS)
{
	BOX		   *box1 = PG_GETARG_BOX_P(0);
	BOX		   *box2 = PG_GETARG_BOX_P(1);

	PG_RETURN_BOOL(FPgt(box1->low.y, box2->high.y));
}

Datum
box_overabove(PG_FUNCTION_ARGS)
{
	BOX		   *box1 = PG_GETARG_BOX_P(0);
	BOX		   *box2 = PG_GETARG_BOX_P(1);

	PG_RETURN_BOOL(FPge(box1->low.y, box2->low.y));
}

Datum
box_below(PG_FUNCTION_ARGS)
{
	BOX		   *box1 = PG_GETARG_BOX_P(0);
	BOX		   *box2 = PG_GETARG_BOX_P(1);

	PG_RETURN_BOOL(FPlt(box1->high.y, box2->low.y));
}

Datum
box_overbelow(PG_FUNCTION_ARGS)
{
	BOX		   *box1 = PG_GETARG_BOX_P(0);
	BOX		   *box2 = PG_GETARG_BOX_P(1);

	PG_RETURN_BOOL(FPle(box1->high.y, box2->high.y));
}

Datum
point_distance(PG_FUNCTION_ARGS)
{
	Point	   *pt1 = PG_GETARG_POINT_P(0);
	Point	   *pt2 = PG_GETARG_POINT_P(1);

	PG_RETURN_FLOAT8(point_dt(pt1, pt2));
}


/* access/skey.h ScanKeyData: reduced to the fields this opclass reads
 * (sk_strategy, sk_subtype, sk_argument, sk_func.fn_oid) — plumbing; the
 * driver builds these from parallel arrays. */
typedef struct
{
	Oid			fn_oid;
} FmgrInfo;
typedef struct
{
	uint16		sk_strategy;
	Oid			sk_subtype;
	FmgrInfo	sk_func;
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

/* VERBATIM: access/spgist/spgproc.c @ 62d6c7d3df lines 25-88. */
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

/* ==== pg_qsort (verbatim lib/sort_template.h instantiated as port/qsort.c
 * does: ST_SORT/ST_ELEMENT_TYPE_VOID/ST_COMPARE_RUNTIME_POINTER, ST_SCOPE
 * static; the backend's qsort IS pg_qsort, port.h line 478) ==== */
#define pg_noinline __attribute__((noinline))
#define Min(x, y)		((x) < (y) ? (x) : (y))
#define CppConcat(x, y) x##y
#define ST_SORT box_pg_qsort
#define ST_ELEMENT_TYPE_VOID
#define ST_COMPARE_RUNTIME_POINTER
#define ST_SCOPE static
#define ST_DECLARE
#define ST_DEFINE
#include "sort_template.h"
#define qsort(a,b,c,d) box_pg_qsort(a,b,c,d)

/* VERBATIM: utils/adt/geo_spgist.c @ 62d6c7d3df lines 85-885 (compareDoubles,
 * RangeBox/RectBox machinery, getQuadrant, getRangeBox, initRectBox,
 * nextRectBox, overlap/contain/contained/lower/higher 2D/4D,
 * pointToRectBoxDistance, spg_box_quad_config/choose/picksplit/
 * inner_consistent/leaf_consistent, is_bounding_box_test_exact,
 * spg_box_quad_get_scankey_bbox, spg_bbox_quad_config,
 * spg_poly_quad_compress). qsort here IS pg_qsort (the box_pg_qsort
 * instantiation above). */
/*
 * Comparator for qsort
 *
 * We don't need to use the floating point macros in here, because this
 * is only going to be used in a place to effect the performance
 * of the index, not the correctness.
 */
static int
compareDoubles(const void *a, const void *b)
{
	float8		x = *(float8 *) a;
	float8		y = *(float8 *) b;

	if (x == y)
		return 0;
	return (x > y) ? 1 : -1;
}

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

/*
 * Calculate the quadrant
 *
 * The quadrant is 8 bit unsigned integer with 4 least bits in use.
 * This function accepts BOXes as input.  They are not casted to
 * RangeBoxes, yet.  All 4 bits are set by comparing a corner of the box.
 * This makes 16 quadrants in total.
 */
static uint8
getQuadrant(BOX *centroid, BOX *inBox)
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

/*
 * Get RangeBox using BOX
 *
 * We are turning the BOX to our structures to emphasize their function
 * of representing points in 4D space.  It also is more convenient to
 * access the values with this structure.
 */
static RangeBox *
getRangeBox(BOX *box)
{
	RangeBox   *range_box = (RangeBox *) palloc(sizeof(RangeBox));

	range_box->left.low = box->low.x;
	range_box->left.high = box->high.x;

	range_box->right.low = box->low.y;
	range_box->right.high = box->high.y;

	return range_box;
}

/*
 * Initialize the traversal value
 *
 * In the beginning, we don't have any restrictions.  We have to
 * initialize the struct to cover the whole 4D space.
 */
static RectBox *
initRectBox(void)
{
	RectBox    *rect_box = (RectBox *) palloc(sizeof(RectBox));
	float8		infinity = get_float8_infinity();

	rect_box->range_box_x.left.low = -infinity;
	rect_box->range_box_x.left.high = infinity;

	rect_box->range_box_x.right.low = -infinity;
	rect_box->range_box_x.right.high = infinity;

	rect_box->range_box_y.left.low = -infinity;
	rect_box->range_box_y.left.high = infinity;

	rect_box->range_box_y.right.low = -infinity;
	rect_box->range_box_y.right.high = infinity;

	return rect_box;
}

/*
 * Calculate the next traversal value
 *
 * All centroids are bounded by RectBox, but SP-GiST only keeps
 * boxes.  When we are traversing the tree, we must calculate RectBox,
 * using centroid and quadrant.
 */
static RectBox *
nextRectBox(RectBox *rect_box, RangeBox *centroid, uint8 quadrant)
{
	RectBox    *next_rect_box = (RectBox *) palloc(sizeof(RectBox));

	memcpy(next_rect_box, rect_box, sizeof(RectBox));

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

	return next_rect_box;
}

/* Can any range from range_box overlap with this argument? */
static bool
overlap2D(RangeBox *range_box, Range *query)
{
	return FPge(range_box->right.high, query->low) &&
		FPle(range_box->left.low, query->high);
}

/* Can any rectangle from rect_box overlap with this argument? */
static bool
overlap4D(RectBox *rect_box, RangeBox *query)
{
	return overlap2D(&rect_box->range_box_x, &query->left) &&
		overlap2D(&rect_box->range_box_y, &query->right);
}

/* Can any range from range_box contain this argument? */
static bool
contain2D(RangeBox *range_box, Range *query)
{
	return FPge(range_box->right.high, query->high) &&
		FPle(range_box->left.low, query->low);
}

/* Can any rectangle from rect_box contain this argument? */
static bool
contain4D(RectBox *rect_box, RangeBox *query)
{
	return contain2D(&rect_box->range_box_x, &query->left) &&
		contain2D(&rect_box->range_box_y, &query->right);
}

/* Can any range from range_box be contained by this argument? */
static bool
contained2D(RangeBox *range_box, Range *query)
{
	return FPle(range_box->left.low, query->high) &&
		FPge(range_box->left.high, query->low) &&
		FPle(range_box->right.low, query->high) &&
		FPge(range_box->right.high, query->low);
}

/* Can any rectangle from rect_box be contained by this argument? */
static bool
contained4D(RectBox *rect_box, RangeBox *query)
{
	return contained2D(&rect_box->range_box_x, &query->left) &&
		contained2D(&rect_box->range_box_y, &query->right);
}

/* Can any range from range_box to be lower than this argument? */
static bool
lower2D(RangeBox *range_box, Range *query)
{
	return FPlt(range_box->left.low, query->low) &&
		FPlt(range_box->right.low, query->low);
}

/* Can any range from range_box not extend to the right side of the query? */
static bool
overLower2D(RangeBox *range_box, Range *query)
{
	return FPle(range_box->left.low, query->high) &&
		FPle(range_box->right.low, query->high);
}

/* Can any range from range_box to be higher than this argument? */
static bool
higher2D(RangeBox *range_box, Range *query)
{
	return FPgt(range_box->left.high, query->high) &&
		FPgt(range_box->right.high, query->high);
}

/* Can any range from range_box not extend to the left side of the query? */
static bool
overHigher2D(RangeBox *range_box, Range *query)
{
	return FPge(range_box->left.high, query->low) &&
		FPge(range_box->right.high, query->low);
}

/* Can any rectangle from rect_box be left of this argument? */
static bool
left4D(RectBox *rect_box, RangeBox *query)
{
	return lower2D(&rect_box->range_box_x, &query->left);
}

/* Can any rectangle from rect_box does not extend the right of this argument? */
static bool
overLeft4D(RectBox *rect_box, RangeBox *query)
{
	return overLower2D(&rect_box->range_box_x, &query->left);
}

/* Can any rectangle from rect_box be right of this argument? */
static bool
right4D(RectBox *rect_box, RangeBox *query)
{
	return higher2D(&rect_box->range_box_x, &query->left);
}

/* Can any rectangle from rect_box does not extend the left of this argument? */
static bool
overRight4D(RectBox *rect_box, RangeBox *query)
{
	return overHigher2D(&rect_box->range_box_x, &query->left);
}

/* Can any rectangle from rect_box be below of this argument? */
static bool
below4D(RectBox *rect_box, RangeBox *query)
{
	return lower2D(&rect_box->range_box_y, &query->right);
}

/* Can any rectangle from rect_box does not extend above this argument? */
static bool
overBelow4D(RectBox *rect_box, RangeBox *query)
{
	return overLower2D(&rect_box->range_box_y, &query->right);
}

/* Can any rectangle from rect_box be above of this argument? */
static bool
above4D(RectBox *rect_box, RangeBox *query)
{
	return higher2D(&rect_box->range_box_y, &query->right);
}

/* Can any rectangle from rect_box does not extend below of this argument? */
static bool
overAbove4D(RectBox *rect_box, RangeBox *query)
{
	return overHigher2D(&rect_box->range_box_y, &query->right);
}

/* Lower bound for the distance between point and rect_box */
static double
pointToRectBoxDistance(Point *point, RectBox *rect_box)
{
	double		dx;
	double		dy;

	if (point->x < rect_box->range_box_x.left.low)
		dx = rect_box->range_box_x.left.low - point->x;
	else if (point->x > rect_box->range_box_x.right.high)
		dx = point->x - rect_box->range_box_x.right.high;
	else
		dx = 0;

	if (point->y < rect_box->range_box_y.left.low)
		dy = rect_box->range_box_y.left.low - point->y;
	else if (point->y > rect_box->range_box_y.right.high)
		dy = point->y - rect_box->range_box_y.right.high;
	else
		dy = 0;

	return HYPOT(dx, dy);
}


/*
 * SP-GiST config function
 */
Datum
spg_box_quad_config(PG_FUNCTION_ARGS)
{
	spgConfigOut *cfg = (spgConfigOut *) PG_GETARG_POINTER(1);

	cfg->prefixType = BOXOID;
	cfg->labelType = VOIDOID;	/* We don't need node labels. */
	cfg->canReturnData = true;
	cfg->longValuesOK = false;

	PG_RETURN_VOID();
}

/*
 * SP-GiST choose function
 */
Datum
spg_box_quad_choose(PG_FUNCTION_ARGS)
{
	spgChooseIn *in = (spgChooseIn *) PG_GETARG_POINTER(0);
	spgChooseOut *out = (spgChooseOut *) PG_GETARG_POINTER(1);
	BOX		   *centroid = DatumGetBoxP(in->prefixDatum),
			   *box = DatumGetBoxP(in->leafDatum);

	out->resultType = spgMatchNode;
	out->result.matchNode.restDatum = BoxPGetDatum(box);

	/* nodeN will be set by core, when allTheSame. */
	if (!in->allTheSame)
		out->result.matchNode.nodeN = getQuadrant(centroid, box);

	PG_RETURN_VOID();
}

/*
 * SP-GiST pick-split function
 *
 * It splits a list of boxes into quadrants by choosing a central 4D
 * point as the median of the coordinates of the boxes.
 */
Datum
spg_box_quad_picksplit(PG_FUNCTION_ARGS)
{
	spgPickSplitIn *in = (spgPickSplitIn *) PG_GETARG_POINTER(0);
	spgPickSplitOut *out = (spgPickSplitOut *) PG_GETARG_POINTER(1);
	BOX		   *centroid;
	int			median,
				i;
	float8	   *lowXs = palloc(sizeof(float8) * in->nTuples);
	float8	   *highXs = palloc(sizeof(float8) * in->nTuples);
	float8	   *lowYs = palloc(sizeof(float8) * in->nTuples);
	float8	   *highYs = palloc(sizeof(float8) * in->nTuples);

	/* Calculate median of all 4D coordinates */
	for (i = 0; i < in->nTuples; i++)
	{
		BOX		   *box = DatumGetBoxP(in->datums[i]);

		lowXs[i] = box->low.x;
		highXs[i] = box->high.x;
		lowYs[i] = box->low.y;
		highYs[i] = box->high.y;
	}

	qsort(lowXs, in->nTuples, sizeof(float8), compareDoubles);
	qsort(highXs, in->nTuples, sizeof(float8), compareDoubles);
	qsort(lowYs, in->nTuples, sizeof(float8), compareDoubles);
	qsort(highYs, in->nTuples, sizeof(float8), compareDoubles);

	median = in->nTuples / 2;

	centroid = palloc(sizeof(BOX));

	centroid->low.x = lowXs[median];
	centroid->high.x = highXs[median];
	centroid->low.y = lowYs[median];
	centroid->high.y = highYs[median];

	/* Fill the output */
	out->hasPrefix = true;
	out->prefixDatum = BoxPGetDatum(centroid);

	out->nNodes = 16;
	out->nodeLabels = NULL;		/* We don't need node labels. */

	out->mapTuplesToNodes = palloc(sizeof(int) * in->nTuples);
	out->leafTupleDatums = palloc(sizeof(Datum) * in->nTuples);

	/*
	 * Assign ranges to corresponding nodes according to quadrants relative to
	 * the "centroid" range
	 */
	for (i = 0; i < in->nTuples; i++)
	{
		BOX		   *box = DatumGetBoxP(in->datums[i]);
		uint8		quadrant = getQuadrant(centroid, box);

		out->leafTupleDatums[i] = BoxPGetDatum(box);
		out->mapTuplesToNodes[i] = quadrant;
	}

	PG_RETURN_VOID();
}

/*
 * Check if result of consistent method based on bounding box is exact.
 */
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
 * Get bounding box for ScanKey.
 */
static BOX *
spg_box_quad_get_scankey_bbox(ScanKey sk, bool *recheck)
{
	switch (sk->sk_subtype)
	{
		case BOXOID:
			return DatumGetBoxP(sk->sk_argument);

		case POLYGONOID:
			if (recheck && !is_bounding_box_test_exact(sk->sk_strategy))
				*recheck = true;
			return &DatumGetPolygonP(sk->sk_argument)->boundbox;

		default:
			elog(ERROR, "unrecognized scankey subtype: %d", sk->sk_subtype);
			return NULL;
	}
}

/*
 * SP-GiST inner consistent function
 */
Datum
spg_box_quad_inner_consistent(PG_FUNCTION_ARGS)
{
	spgInnerConsistentIn *in = (spgInnerConsistentIn *) PG_GETARG_POINTER(0);
	spgInnerConsistentOut *out = (spgInnerConsistentOut *) PG_GETARG_POINTER(1);
	int			i;
	MemoryContext old_ctx;
	RectBox    *rect_box;
	uint8		quadrant;
	RangeBox   *centroid,
			  **queries;

	/*
	 * We are saving the traversal value or initialize it an unbounded one, if
	 * we have just begun to walk the tree.
	 */
	if (in->traversalValue)
		rect_box = in->traversalValue;
	else
		rect_box = initRectBox();

	if (in->allTheSame)
	{
		/* Report that all nodes should be visited */
		out->nNodes = in->nNodes;
		out->nodeNumbers = (int *) palloc(sizeof(int) * in->nNodes);
		for (i = 0; i < in->nNodes; i++)
			out->nodeNumbers[i] = i;

		if (in->norderbys > 0 && in->nNodes > 0)
		{
			double	   *distances = palloc(sizeof(double) * in->norderbys);
			int			j;

			for (j = 0; j < in->norderbys; j++)
			{
				Point	   *pt = DatumGetPointP(in->orderbys[j].sk_argument);

				distances[j] = pointToRectBoxDistance(pt, rect_box);
			}

			out->distances = (double **) palloc(sizeof(double *) * in->nNodes);
			out->distances[0] = distances;

			for (i = 1; i < in->nNodes; i++)
			{
				out->distances[i] = palloc(sizeof(double) * in->norderbys);
				memcpy(out->distances[i], distances,
					   sizeof(double) * in->norderbys);
			}
		}

		PG_RETURN_VOID();
	}

	/*
	 * We are casting the prefix and queries to RangeBoxes for ease of the
	 * following operations.
	 */
	centroid = getRangeBox(DatumGetBoxP(in->prefixDatum));
	queries = (RangeBox **) palloc(in->nkeys * sizeof(RangeBox *));
	for (i = 0; i < in->nkeys; i++)
	{
		BOX		   *box = spg_box_quad_get_scankey_bbox(&in->scankeys[i], NULL);

		queries[i] = getRangeBox(box);
	}

	/* Allocate enough memory for nodes */
	out->nNodes = 0;
	out->nodeNumbers = (int *) palloc(sizeof(int) * in->nNodes);
	out->traversalValues = (void **) palloc(sizeof(void *) * in->nNodes);
	if (in->norderbys > 0)
		out->distances = (double **) palloc(sizeof(double *) * in->nNodes);

	/*
	 * We switch memory context, because we want to allocate memory for new
	 * traversal values (next_rect_box) and pass these pieces of memory to
	 * further call of this function.
	 */
	old_ctx = MemoryContextSwitchTo(in->traversalMemoryContext);

	for (quadrant = 0; quadrant < in->nNodes; quadrant++)
	{
		RectBox    *next_rect_box = nextRectBox(rect_box, centroid, quadrant);
		bool		flag = true;

		for (i = 0; i < in->nkeys; i++)
		{
			StrategyNumber strategy = in->scankeys[i].sk_strategy;

			switch (strategy)
			{
				case RTOverlapStrategyNumber:
					flag = overlap4D(next_rect_box, queries[i]);
					break;

				case RTContainsStrategyNumber:
					flag = contain4D(next_rect_box, queries[i]);
					break;

				case RTSameStrategyNumber:
				case RTContainedByStrategyNumber:
					flag = contained4D(next_rect_box, queries[i]);
					break;

				case RTLeftStrategyNumber:
					flag = left4D(next_rect_box, queries[i]);
					break;

				case RTOverLeftStrategyNumber:
					flag = overLeft4D(next_rect_box, queries[i]);
					break;

				case RTRightStrategyNumber:
					flag = right4D(next_rect_box, queries[i]);
					break;

				case RTOverRightStrategyNumber:
					flag = overRight4D(next_rect_box, queries[i]);
					break;

				case RTAboveStrategyNumber:
					flag = above4D(next_rect_box, queries[i]);
					break;

				case RTOverAboveStrategyNumber:
					flag = overAbove4D(next_rect_box, queries[i]);
					break;

				case RTBelowStrategyNumber:
					flag = below4D(next_rect_box, queries[i]);
					break;

				case RTOverBelowStrategyNumber:
					flag = overBelow4D(next_rect_box, queries[i]);
					break;

				default:
					elog(ERROR, "unrecognized strategy: %d", strategy);
			}

			/* If any check is failed, we have found our answer. */
			if (!flag)
				break;
		}

		if (flag)
		{
			out->traversalValues[out->nNodes] = next_rect_box;
			out->nodeNumbers[out->nNodes] = quadrant;

			if (in->norderbys > 0)
			{
				double	   *distances = palloc(sizeof(double) * in->norderbys);
				int			j;

				out->distances[out->nNodes] = distances;

				for (j = 0; j < in->norderbys; j++)
				{
					Point	   *pt = DatumGetPointP(in->orderbys[j].sk_argument);

					distances[j] = pointToRectBoxDistance(pt, next_rect_box);
				}
			}

			out->nNodes++;
		}
		else
		{
			/*
			 * If this node is not selected, we don't need to keep the next
			 * traversal value in the memory context.
			 */
			pfree(next_rect_box);
		}
	}

	/* Switch back */
	MemoryContextSwitchTo(old_ctx);

	PG_RETURN_VOID();
}

/*
 * SP-GiST inner consistent function
 */
Datum
spg_box_quad_leaf_consistent(PG_FUNCTION_ARGS)
{
	spgLeafConsistentIn *in = (spgLeafConsistentIn *) PG_GETARG_POINTER(0);
	spgLeafConsistentOut *out = (spgLeafConsistentOut *) PG_GETARG_POINTER(1);
	Datum		leaf = in->leafDatum;
	bool		flag = true;
	int			i;

	/* All tests are exact. */
	out->recheck = false;

	/*
	 * Don't return leafValue unless told to; this is used for both box and
	 * polygon opclasses, and in the latter case the leaf datum is not even of
	 * the right type to return.
	 */
	if (in->returnData)
		out->leafValue = leaf;

	/* Perform the required comparison(s) */
	for (i = 0; i < in->nkeys; i++)
	{
		StrategyNumber strategy = in->scankeys[i].sk_strategy;
		BOX		   *box = spg_box_quad_get_scankey_bbox(&in->scankeys[i],
														&out->recheck);
		Datum		query = BoxPGetDatum(box);

		switch (strategy)
		{
			case RTOverlapStrategyNumber:
				flag = DatumGetBool(DirectFunctionCall2(box_overlap, leaf,
														query));
				break;

			case RTContainsStrategyNumber:
				flag = DatumGetBool(DirectFunctionCall2(box_contain, leaf,
														query));
				break;

			case RTContainedByStrategyNumber:
				flag = DatumGetBool(DirectFunctionCall2(box_contained, leaf,
														query));
				break;

			case RTSameStrategyNumber:
				flag = DatumGetBool(DirectFunctionCall2(box_same, leaf,
														query));
				break;

			case RTLeftStrategyNumber:
				flag = DatumGetBool(DirectFunctionCall2(box_left, leaf,
														query));
				break;

			case RTOverLeftStrategyNumber:
				flag = DatumGetBool(DirectFunctionCall2(box_overleft, leaf,
														query));
				break;

			case RTRightStrategyNumber:
				flag = DatumGetBool(DirectFunctionCall2(box_right, leaf,
														query));
				break;

			case RTOverRightStrategyNumber:
				flag = DatumGetBool(DirectFunctionCall2(box_overright, leaf,
														query));
				break;

			case RTAboveStrategyNumber:
				flag = DatumGetBool(DirectFunctionCall2(box_above, leaf,
														query));
				break;

			case RTOverAboveStrategyNumber:
				flag = DatumGetBool(DirectFunctionCall2(box_overabove, leaf,
														query));
				break;

			case RTBelowStrategyNumber:
				flag = DatumGetBool(DirectFunctionCall2(box_below, leaf,
														query));
				break;

			case RTOverBelowStrategyNumber:
				flag = DatumGetBool(DirectFunctionCall2(box_overbelow, leaf,
														query));
				break;

			default:
				elog(ERROR, "unrecognized strategy: %d", strategy);
		}

		/* If any check is failed, we have found our answer. */
		if (!flag)
			break;
	}

	if (flag && in->norderbys > 0)
	{
		Oid			distfnoid = in->orderbys[0].sk_func.fn_oid;

		out->distances = spg_key_orderbys_distances(leaf, false,
													in->orderbys, in->norderbys);

		/* Recheck is necessary when computing distance to polygon */
		out->recheckDistances = distfnoid == F_DIST_POLYP;
	}

	PG_RETURN_BOOL(flag);
}


/*
 * SP-GiST config function for 2-D types that are lossy represented by their
 * bounding boxes
 */
Datum
spg_bbox_quad_config(PG_FUNCTION_ARGS)
{
	spgConfigOut *cfg = (spgConfigOut *) PG_GETARG_POINTER(1);

	cfg->prefixType = BOXOID;	/* A type represented by its bounding box */
	cfg->labelType = VOIDOID;	/* We don't need node labels. */
	cfg->leafType = BOXOID;
	cfg->canReturnData = false;
	cfg->longValuesOK = false;

	PG_RETURN_VOID();
}

/*
 * SP-GiST compress function for polygons
 */
Datum
spg_poly_quad_compress(PG_FUNCTION_ARGS)
{
	POLYGON    *polygon = PG_GETARG_POLYGON_P(0);
	BOX		   *box;

	box = (BOX *) palloc(sizeof(BOX));
	*box = polygon->boundbox;

	PG_RETURN_BOX_P(box);
}

/* ========== SECTION 2: fuzz-facing driver entries (NOT Postgres code) ===== */

/*
 * Status protocol: 0 = ok; 102 = 22003 float value-out-of-range (KNN
 * distance overflow/underflow); 190 = elog(ERROR) (unrecognized scankey
 * subtype / unrecognized strategy, driver-fenced C-parity arms).
 * Box images = 4 doubles in struct order (high.x, high.y, low.x, low.y);
 * RectBox images = 8 doubles in struct order.
 */

int
pg_diff_box_config(int bbox, uint32 *prefix_type, uint32 *label_type,
				   uint32 *leaf_type, int *can_return, int *long_ok)
{
	spgConfigIn in;
	spgConfigOut out;
	BoxFcinfo	fc;

	pg_diff_arena_reset();
	pg_diff_errcode = 0;
	memset(&in, 0, sizeof(in));
	memset(&out, 0, sizeof(out));
	fc.arg[0] = PointerGetDatum(&in);
	fc.arg[1] = PointerGetDatum(&out);
	if (bbox)
		(void) spg_bbox_quad_config(&fc);
	else
		(void) spg_box_quad_config(&fc);
	*prefix_type = out.prefixType;
	*label_type = out.labelType;
	*leaf_type = out.leafType;
	*can_return = out.canReturnData ? 1 : 0;
	*long_ok = out.longValuesOK ? 1 : 0;
	return 0;
}

int
pg_diff_box_choose(int all_the_same, const double *prefix4, const double *leaf4,
				   int level, int *node_n, int *level_add, double *rest4)
{
	spgChooseIn in;
	spgChooseOut out;
	BoxFcinfo	fc;
	BOX		   *rest;
	int			i;

	pg_diff_arena_reset();
	pg_diff_errcode = 0;
	if (setjmp(pg_box_jmp) != 0)
		return 100 + pg_diff_errcode;
	memset(&in, 0, sizeof(in));
	memset(&out, 0, sizeof(out));
	in.datum = PointerGetDatum(leaf4);
	in.leafDatum = PointerGetDatum(leaf4);
	in.level = level;
	in.allTheSame = all_the_same != 0;
	in.hasPrefix = true;
	in.prefixDatum = PointerGetDatum(prefix4);
	in.nNodes = 16;
	in.nodeLabels = NULL;
	fc.arg[0] = PointerGetDatum(&in);
	fc.arg[1] = PointerGetDatum(&out);
	(void) spg_box_quad_choose(&fc);
	if (out.resultType != spgMatchNode)
		abort();
	*node_n = out.result.matchNode.nodeN;
	*level_add = out.result.matchNode.levelAdd;
	rest = DatumGetBoxP(out.result.matchNode.restDatum);
	for (i = 0; i < 4; i++)
		rest4[i] = ((const double *) rest)[i];
	return 0;
}

int
pg_diff_box_picksplit(int n, const double *boxes4n, int level, int *has_prefix,
					  double *centroid4, int *n_nodes, int *map, double *leaf4n)
{
	spgPickSplitIn in;
	spgPickSplitOut out;
	BoxFcinfo	fc;
	Datum	   *datums;
	int			i,
				j;

	pg_diff_arena_reset();
	pg_diff_errcode = 0;
	if (setjmp(pg_box_jmp) != 0)
		return 100 + pg_diff_errcode;
	memset(&in, 0, sizeof(in));
	memset(&out, 0, sizeof(out));
	datums = (Datum *) palloc(sizeof(Datum) * n);
	for (i = 0; i < n; i++)
		datums[i] = PointerGetDatum(boxes4n + 4 * i);
	in.nTuples = n;
	in.datums = datums;
	in.level = level;
	fc.arg[0] = PointerGetDatum(&in);
	fc.arg[1] = PointerGetDatum(&out);
	(void) spg_box_quad_picksplit(&fc);
	*has_prefix = out.hasPrefix ? 1 : 0;
	if (out.hasPrefix)
	{
		const double *c = (const double *) DatumGetBoxP(out.prefixDatum);

		for (i = 0; i < 4; i++)
			centroid4[i] = c[i];
	}
	*n_nodes = out.nNodes;
	for (i = 0; i < n; i++)
	{
		const double *b = (const double *) DatumGetBoxP(out.leafTupleDatums[i]);

		map[i] = out.mapTuplesToNodes[i];
		for (j = 0; j < 4; j++)
			leaf4n[4 * i + j] = b[j];
	}
	return 0;
}

int
pg_diff_box_inner(int all_the_same, int in_nnodes, const double *prefix4,
				  int has_tv, const double *tv8, int nkeys,
				  const uint16 *strategies, const uint32 *subtypes,
				  const uintptr_t *args, int norderbys, const double *obys2,
				  int *n_nodes, int *node_numbers, double *tvout8,
				  double *distout)
{
	spgInnerConsistentIn in;
	spgInnerConsistentOut out;
	BoxFcinfo	fc;
	ScanKeyData *keys;
	ScanKeyData *okeys;
	int			i,
				j;

	pg_diff_arena_reset();
	pg_diff_errcode = 0;
	if (setjmp(pg_box_jmp) != 0)
		return 100 + pg_diff_errcode;
	memset(&in, 0, sizeof(in));
	memset(&out, 0, sizeof(out));
	keys = (ScanKeyData *) palloc(sizeof(ScanKeyData) * (nkeys > 0 ? nkeys : 1));
	for (i = 0; i < nkeys; i++)
	{
		keys[i].sk_strategy = strategies[i];
		keys[i].sk_subtype = subtypes[i];
		keys[i].sk_func.fn_oid = 0;
		keys[i].sk_argument = (Datum) args[i];
	}
	okeys = (ScanKeyData *) palloc(sizeof(ScanKeyData) * (norderbys > 0 ? norderbys : 1));
	for (i = 0; i < norderbys; i++)
	{
		memset(&okeys[i], 0, sizeof(ScanKeyData));
		okeys[i].sk_argument = PointerGetDatum(obys2 + 2 * i);
	}
	in.scankeys = keys;
	in.orderbys = okeys;
	in.nkeys = nkeys;
	in.norderbys = norderbys;
	in.traversalValue = has_tv ? (void *) tv8 : NULL;
	in.traversalMemoryContext = NULL;
	in.level = 0;
	in.returnData = false;
	in.allTheSame = all_the_same != 0;
	in.hasPrefix = true;
	in.prefixDatum = PointerGetDatum(prefix4);
	in.nNodes = in_nnodes;
	in.nodeLabels = NULL;
	fc.arg[0] = PointerGetDatum(&in);
	fc.arg[1] = PointerGetDatum(&out);
	(void) spg_box_quad_inner_consistent(&fc);
	*n_nodes = out.nNodes;
	for (i = 0; i < out.nNodes; i++)
	{
		node_numbers[i] = out.nodeNumbers[i];
		if (!all_the_same)
		{
			const double *tv = (const double *) out.traversalValues[i];

			for (j = 0; j < 8; j++)
				tvout8[8 * i + j] = tv[j];
		}
		if (norderbys > 0)
			for (j = 0; j < norderbys; j++)
				distout[norderbys * i + j] = out.distances[i][j];
	}
	return 0;
}

int
pg_diff_box_leaf(const double *leaf4, int return_data, int nkeys,
				 const uint16 *strategies, const uint32 *subtypes,
				 const uintptr_t *args, uint32 oby_fn_oid, int norderbys,
				 const double *obys2, int *res, int *recheck,
				 int *recheck_dist, double *dist)
{
	spgLeafConsistentIn in;
	spgLeafConsistentOut out;
	BoxFcinfo	fc;
	ScanKeyData *keys;
	ScanKeyData *okeys;
	Datum		ret;
	int			i;

	pg_diff_arena_reset();
	pg_diff_errcode = 0;
	if (setjmp(pg_box_jmp) != 0)
		return 100 + pg_diff_errcode;
	memset(&in, 0, sizeof(in));
	memset(&out, 0, sizeof(out));
	keys = (ScanKeyData *) palloc(sizeof(ScanKeyData) * (nkeys > 0 ? nkeys : 1));
	for (i = 0; i < nkeys; i++)
	{
		keys[i].sk_strategy = strategies[i];
		keys[i].sk_subtype = subtypes[i];
		keys[i].sk_func.fn_oid = 0;
		keys[i].sk_argument = (Datum) args[i];
	}
	okeys = (ScanKeyData *) palloc(sizeof(ScanKeyData) * (norderbys > 0 ? norderbys : 1));
	for (i = 0; i < norderbys; i++)
	{
		memset(&okeys[i], 0, sizeof(ScanKeyData));
		okeys[i].sk_func.fn_oid = oby_fn_oid;
		okeys[i].sk_argument = PointerGetDatum(obys2 + 2 * i);
	}
	in.scankeys = keys;
	in.orderbys = okeys;
	in.nkeys = nkeys;
	in.norderbys = norderbys;
	in.level = 0;
	in.returnData = return_data != 0;
	in.leafDatum = PointerGetDatum(leaf4);
	fc.arg[0] = PointerGetDatum(&in);
	fc.arg[1] = PointerGetDatum(&out);
	ret = spg_box_quad_leaf_consistent(&fc);
	*res = DatumGetBool(ret) ? 1 : 0;
	*recheck = out.recheck ? 1 : 0;
	*recheck_dist = out.recheckDistances ? 1 : 0;
	if (DatumGetBool(ret) && norderbys > 0)
		for (i = 0; i < norderbys; i++)
			dist[i] = out.distances[i];
	return 0;
}

int
pg_diff_box_poly_compress(const void *poly, double *out4)
{
	BoxFcinfo	fc;
	Datum		ret;
	const double *b;
	int			i;

	pg_diff_arena_reset();
	pg_diff_errcode = 0;
	fc.arg[0] = PointerGetDatum(poly);
	fc.arg[1] = 0;
	ret = spg_poly_quad_compress(&fc);
	b = (const double *) DatumGetBoxP(ret);
	for (i = 0; i < 4; i++)
		out4[i] = b[i];
	return 0;
}
