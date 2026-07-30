/*
 * Vendored PostgreSQL C: polygon/path POSITION operators (bounding-box
 * comparisons) and the path/poly header-read converters.
 *
 * Provenance:
 *   - src/backend/utils/adt/geo_ops.c   @ postgres/postgres REL_18_STABLE
 *     (fetched 2026-07-30)
 *   - src/include/utils/geo_decls.h     @ same ref: POLYGON/PATH struct
 *     shapes (vl_len_ dropped by the fmgr shim, see below).
 *
 * Functions copied, bodies verbatim, renamed with pg_ prefix:
 *   poly_left, poly_overleft, poly_right, poly_overright,
 *   poly_below, poly_overbelow, poly_above, poly_overabove
 *     (NOTE the parity surface: unlike the box_* position operators these
 *     are RAW C comparisons on boundbox coordinates — no EPSILON fuzz.
 *     The negative control below witnesses that exactness is in-theorem.)
 *   path_isclosed, path_isopen, path_npoints, poly_npoints
 *     (pure header-field reads).
 *
 * Shims (plumbing only, never logic):
 *   - PG_FUNCTION_ARGS unwrapping -> plain C signatures. POLYGON/PATH are
 *     varlena datums; PG_GETARG_POLYGON_P/PG_GETARG_PATH_P detoast is the
 *     Rust wrapper's arg_varlena_packed seam (in-theorem on the Rust side
 *     for inline 4B-uncompressed images; toast arms out of proof). The C
 *     wrappers take the header fields the verbatim bodies read (boundbox
 *     coordinate doubles / npts / closed) and build the structs locally,
 *     then run the verbatim body on pointers. vl_len_ is dropped from the
 *     local struct: it is fmgr plumbing the bodies never read.
 *   - PG_RETURN_BOOL -> int return via `(x) ? 1 : 0` (BoolGetDatum's
 *     normalization, load-bearing for path_isclosed where `closed` is a
 *     full int32); PG_RETURN_INT32 -> int32 return.
 *   - PG_FREE_IF_COPY -> dropped (memory plumbing, no value effect).
 *   - float8 -> double typedef.
 *
 * pg_poly_left_fuzzy at the bottom is NOT Postgres code: it is a
 * deliberately WRONG comparator (EPSILON-fuzzy FPlt, the box_left shape)
 * used only by the negative-control harness, which must FAIL inside the
 * epsilon band — witnessing that the raw-IEEE exactness of the poly
 * position operators is in-theorem, not vacuously equal.
 *
 * NAN shim note (CBMC NAN model defect, ruled 2026-07-28): this file's
 * verbatim bodies never touch the NAN macro or get_float4/8_nan() — they
 * are pure comparisons; NaN enters only as symbolic INPUT bits from the
 * harness, which CBMC transports faithfully. The family's pinned
 * canonical-NAN shim lives in pg_geo_cmp.c (always linked alongside this
 * file); see proofs/geo-cmp/CBMC-NAN-BUG-REPORT.md.
 */

#include "../../support/c/pg_proof_shim.h"

typedef double float8;

typedef struct
{
	float8		x,
				y;
} Point;

typedef struct
{
	Point		high,
				low;
} BOX;

/* geo_decls.h POLYGON/PATH, vl_len_ dropped (fmgr shim, see header) */
typedef struct
{
	int32		npts;
	BOX			boundbox;
	Point		p[1];
} POLYGON;

typedef struct
{
	int32		npts;
	int32		closed;
	int32		dummy;
	Point		p[1];
} PATH;

#define POLY_ARGS2 \
	double ahx, double ahy, double alx, double aly, \
	double bhx, double bhy, double blx, double bly
#define POLY_LOCALS \
	POLYGON a_, b_; \
	POLYGON *polya = &a_; \
	POLYGON *polyb = &b_; \
	a_.npts = 1; b_.npts = 1; \
	a_.boundbox.high.x = ahx; a_.boundbox.high.y = ahy; \
	a_.boundbox.low.x = alx; a_.boundbox.low.y = aly; \
	b_.boundbox.high.x = bhx; b_.boundbox.high.y = bhy; \
	b_.boundbox.low.x = blx; b_.boundbox.low.y = bly

int
pg_poly_left(POLY_ARGS2)
{
	POLY_LOCALS;

	return (polya->boundbox.high.x < polyb->boundbox.low.x) ? 1 : 0;
}

int
pg_poly_overleft(POLY_ARGS2)
{
	POLY_LOCALS;

	return (polya->boundbox.high.x <= polyb->boundbox.high.x) ? 1 : 0;
}

int
pg_poly_right(POLY_ARGS2)
{
	POLY_LOCALS;

	return (polya->boundbox.low.x > polyb->boundbox.high.x) ? 1 : 0;
}

int
pg_poly_overright(POLY_ARGS2)
{
	POLY_LOCALS;

	return (polya->boundbox.low.x >= polyb->boundbox.low.x) ? 1 : 0;
}

int
pg_poly_below(POLY_ARGS2)
{
	POLY_LOCALS;

	return (polya->boundbox.high.y < polyb->boundbox.low.y) ? 1 : 0;
}

int
pg_poly_overbelow(POLY_ARGS2)
{
	POLY_LOCALS;

	return (polya->boundbox.high.y <= polyb->boundbox.high.y) ? 1 : 0;
}

int
pg_poly_above(POLY_ARGS2)
{
	POLY_LOCALS;

	return (polya->boundbox.low.y > polyb->boundbox.high.y) ? 1 : 0;
}

int
pg_poly_overabove(POLY_ARGS2)
{
	POLY_LOCALS;

	return (polya->boundbox.low.y >= polyb->boundbox.low.y) ? 1 : 0;
}

/* ---- header-read converters; bodies verbatim from geo_ops.c ---- */

int
pg_path_isclosed(int32 npts, int32 closed)
{
	PATH		p_;
	PATH	   *path = &p_;

	p_.npts = npts;
	p_.closed = closed;

	return (path->closed) ? 1 : 0;	/* PG_RETURN_BOOL(path->closed) */
}

int
pg_path_isopen(int32 npts, int32 closed)
{
	PATH		p_;
	PATH	   *path = &p_;

	p_.npts = npts;
	p_.closed = closed;

	return (!path->closed) ? 1 : 0;	/* PG_RETURN_BOOL(!path->closed) */
}

int32
pg_path_npoints(int32 npts, int32 closed)
{
	PATH		p_;
	PATH	   *path = &p_;

	p_.npts = npts;
	p_.closed = closed;

	return path->npts;			/* PG_RETURN_INT32(path->npts) */
}

int32
pg_poly_npoints(int32 npts)
{
	POLYGON		p_;
	POLYGON    *poly = &p_;

	p_.npts = npts;

	return poly->npts;			/* PG_RETURN_INT32(poly->npts) */
}

/*
 * NEGATIVE CONTROL ONLY — NOT Postgres code. A plausibly-wrong port of
 * poly_left that reuses the box_left EPSILON-fuzzy comparator shape
 * (FPlt from geo_decls.h). The control harness asserts shipped Rust
 * poly_left == this, and MUST fail with a counterexample inside the
 * epsilon band — witness that raw-IEEE exactness is load-bearing.
 */
#define PROOF_EPSILON	1.0E-06

static inline int
proof_FPlt(double A, double B)
{
	return (A + PROOF_EPSILON < B);
}

int
pg_poly_left_fuzzy(POLY_ARGS2)
{
	POLY_LOCALS;

	return proof_FPlt(polya->boundbox.high.x, polyb->boundbox.low.x) ? 1 : 0;
}
