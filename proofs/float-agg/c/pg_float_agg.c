/*
 * Vendored from postgres REL_18_STABLE (fetched 2026-07-28 via
 * raw.githubusercontent.com/postgres/postgres/REL_18_STABLE):
 *   src/backend/utils/adt/float.c — check_float8_array, float8_combine,
 *       float8_accum, float4_accum, float8_avg, float8_var_pop,
 *       float8_var_samp, float8_stddev_pop, float8_stddev_samp,
 *       float8_regr_accum, float8_regr_combine, float8_regr_sxx,
 *       float8_regr_syy, float8_regr_sxy, float8_regr_avgx,
 *       float8_regr_avgy, float8_covar_pop, float8_covar_samp,
 *       float8_corr, float8_regr_r2, float8_regr_slope,
 *       float8_regr_intercept (bodies verbatim)
 *   src/include/utils/float.h — float8_pl, get_float8_nan,
 *       get_float8_infinity inlines (bodies verbatim)
 *
 * SHIMS (everything else is verbatim; nothing here replaces logic under
 * proof):
 *
 *  - fmgr/array prologue: the PG_FUNCTION_ARGS entry points become plain C
 *    signatures. For the FINAL/transition functions the
 *    `transarray = PG_GETARG_ARRAYTYPE_P(k); transvalues =
 *    check_float8_array(transarray, ..., N);` prologue is replaced by a
 *    `const float8 *transvalues` parameter (the varlena fetch + shape check
 *    are proven separately by the pg_check_float8_array theorem pair);
 *    PG_GETARG_FLOAT8/FLOAT4 become typed parameters.
 *  - result protocol: PG_RETURN_NULL() -> `return 1;` and
 *    PG_RETURN_FLOAT8(x) -> `*out = (x); return 0;` via textual macros at
 *    the exact program points (int-returning shims — Kani lowers Rust ()
 *    as `struct Unit`, which goto-cc rejects against C void).
 *  - transition-value result: the trailing
 *    `if (AggCheckCallContext(...)) { in-place store } else {
 *    construct_array_builtin }` tail stores the SAME three/six values
 *    either way; it is replaced by stores into a caller-provided
 *    `float8 *out` (array construction/agg-context detection is executor
 *    plumbing, out of proof — the value claim is identical).
 *  - ereport/elog: float_overflow_error() -> `pg_agg_err = 2` (macro; the
 *    verbatim body continues where C longjmps — harnesses treat any
 *    nonzero flag as the error verdict and never read *out on that arm);
 *    check_float8_array's elog(ERROR) -> `pg_agg_err = 9; return NULL;`
 *    (flag classes: 2 = float overflow / C 22003, 9 = bad transition
 *    array / C elog XX000). Wrappers reset the flag on entry.
 *  - CANONICAL-NAN SHIM (MANDATORY — CBMC NAN model defect, ruled
 *    2026-07-28, see proofs/geo-cmp/CBMC-NAN-BUG-REPORT.md): this family
 *    REACHES get_float8_nan() (accum/regr_accum NaN/Inf routing), and
 *    CBMC's <math.h> NAN constant carries a non-canonical (signaling)
 *    payload where real compilers give quiet 0x7ff8000000000000 == Rust
 *    f64::NAN. NAN is pinned to the canonical quiet NaN below; bodies
 *    stay verbatim. Retire when the bundled CBMC ships the upstream fix.
 *  - ArrayType/ARR_* macros (array.h semantics, fields the vendored
 *    check_float8_array reads) are redeclared for the
 *    pg_check_float8_array theorem: vl_len_/ndim/dataoffset/elemtype
 *    header + dims/lbound ints, ARR_DATA_OFFSET = MAXALIGN(24) for the
 *    accepted 1-D no-null shape.
 *  - Assert compiled out (production postgres posture, pg_proof_shim.h).
 *  - fp-contraction note: float8_regr_accum / float8_accum's general
 *    (N0 > 0) arm is NOT harnessed — shipped Rust uses f64::mul_add to
 *    match the FMA the compiled C emits on aarch64, and CBMC's C model
 *    does NOT contract `newval * N - Sx` (same spec gap as pg_hypot,
 *    proofs/geo-cmp). Only the first-row (N0 == 0) planes, where no
 *    multiply-add is reachable, are in-theorem.
 */

#include "../../support/c/pg_proof_shim.h"
#include <math.h>

typedef double float8;
typedef float float4;

/* ---- canonical quiet NaN shim (see header comment) ---- */
#undef NAN
static inline float8
pg_proof_canonical_nan(void)
{
	union
	{
		uint64		u;
		float8		d;
	}			nan_;

	nan_.u = 0x7ff8000000000000ULL;
	return nan_.d;
}
#define NAN (pg_proof_canonical_nan())

/*
 * ---- CBMC-only sqrt canonicalization (dsqrt dual-mode artifact) ----
 * Under goto-cc, C sqrt() lowers to CBMC's sqrt model while the shipped
 * Rust f64::sqrt lowers differently — the two IN-MODEL sqrts disagree
 * bit-for-bit on some inputs even though both sides are identical on
 * silicon (native differential native_diff_float_agg: 0 diffs over the
 * exact grid domains + 8M random sweep, 2026-07-29; dsqrt parity itself
 * proven native in proofs/float-arith). Canonicalize by routing the C
 * sqrt (goto-cc/kani builds only: build.rs defines -DPG_PROOF_NATIVE for
 * native cc builds, which keep libm sqrt as the C ground truth) through
 * the SAME symbol the Rust side uses (pg_proof_sqrt, a
 * #[no_mangle] f64::sqrt wrapper in src/lib.rs, cfg(kani)) — the sqrt
 * MODEL leaves the proof, sqrt VALUE parity is owned by the native
 * differential. Native builds keep libm sqrt (the C ground truth).
 */
#ifndef PG_PROOF_NATIVE
extern double pg_proof_sqrt(double x);
#define sqrt(x) pg_proof_sqrt(x)
#endif

/* ---- error-flag convention (header comment; classes 2 and 9) ---- */
static int	pg_agg_err = 0;
#define float_overflow_error() do { pg_agg_err = 2; } while (0)
#define elog(elevel, ...) do { pg_agg_err = 9; return NULL; } while (0)

/* ---- float.h inlines, bodies verbatim ---- */

static inline float8
get_float8_infinity(void)
{
	return (float8) INFINITY;
}

static inline float8
get_float8_nan(void)
{
	/* C99 standard way */
	return (float8) NAN;
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

/* ---- ArrayType shims for the check_float8_array theorem ---- */

#define FLOAT8OID 701

typedef struct ArrayType
{
	int32		vl_len_;
	int			ndim;
	int32		dataoffset;
	Oid			elemtype;
} ArrayType;

#define TYPEALIGN(ALIGNVAL,LEN) \
	(((uintptr_t) (LEN) + ((ALIGNVAL) - 1)) & ~((uintptr_t) ((ALIGNVAL) - 1)))
#define MAXALIGN(LEN) TYPEALIGN(8, (LEN))

#define ARR_NDIM(a) ((a)->ndim)
#define ARR_HASNULL(a) ((a)->dataoffset != 0)
#define ARR_ELEMTYPE(a) ((a)->elemtype)
#define ARR_DIMS(a) ((int *) (((char *) (a)) + sizeof(ArrayType)))
#define ARR_OVERHEAD_NONULLS(ndims) \
	MAXALIGN(sizeof(ArrayType) + 2 * sizeof(int) * (ndims))
#define ARR_DATA_OFFSET(a) \
	(ARR_HASNULL(a) ? (a)->dataoffset : ARR_OVERHEAD_NONULLS(ARR_NDIM(a)))
#define ARR_DATA_PTR(a) (((char *) (a)) + ARR_DATA_OFFSET(a))

/* ---- float.c check_float8_array, body verbatim ---- */

static float8 *
check_float8_array(ArrayType *transarray, const char *caller, int n)
{
	/*
	 * We expect the input to be an N-element float array; verify that. We
	 * don't need to use deconstruct_array() since the array data is just
	 * going to look like a C array of N float8 values.
	 */
	if (ARR_NDIM(transarray) != 1 ||
		ARR_DIMS(transarray)[0] != n ||
		ARR_HASNULL(transarray) ||
		ARR_ELEMTYPE(transarray) != FLOAT8OID)
		elog(ERROR, "%s: expected %d-element float8 array", caller, n);
	return (float8 *) ARR_DATA_PTR(transarray);
}

/*
 * Exported check theorem entry: image is a harness-built array image.
 * Returns the data pointer (NULL + pg_agg_err=9 on reject).
 */
const float8 *
pg_check_float8_array(const void *image, int n, int *err)
{
	const float8 *r;

	pg_agg_err = 0;
	r = check_float8_array((ArrayType *) image, "proof", n);
	*err = pg_agg_err;
	return r;
}

/* ==================== transition functions ==================== */

/* float8_accum, body verbatim (prologue/result shims per header) */
int
pg_float8_accum(const float8 *transvalues, float8 newval, float8 *out)
{
	float8		N,
				Sx,
				Sxx,
				tmp;

	pg_agg_err = 0;

	N = transvalues[0];
	Sx = transvalues[1];
	Sxx = transvalues[2];

	/*
	 * Use the Youngs-Cramer algorithm to incorporate the new value into the
	 * transition values.
	 */
	N += 1.0;
	Sx += newval;
	if (transvalues[0] > 0.0)
	{
		tmp = newval * N - Sx;
		Sxx += tmp * tmp / (N * transvalues[0]);

		/*
		 * Overflow check.  We only report an overflow error when finite
		 * inputs lead to infinite results.  Note also that Sxx should be NaN
		 * if any of the inputs are infinite, so we intentionally prevent Sxx
		 * from becoming infinite.
		 */
		if (isinf(Sx) || isinf(Sxx))
		{
			if (!isinf(transvalues[1]) && !isinf(newval))
				float_overflow_error();

			Sxx = get_float8_nan();
		}
	}
	else
	{
		/*
		 * At the first input, we normally can leave Sxx as 0.  However, if
		 * the first input is Inf or NaN, we'd better force Sxx to NaN;
		 * otherwise we will falsely report variance zero when there are no
		 * more inputs.
		 */
		if (isnan(newval) || isinf(newval))
			Sxx = get_float8_nan();
	}

	out[0] = N;
	out[1] = Sx;
	out[2] = Sxx;
	return pg_agg_err;
}

/* float4_accum, body verbatim ("do computations as float8") */
int
pg_float4_accum(const float8 *transvalues, float4 newval4, float8 *out)
{
	/* do computations as float8 */
	float8		newval = newval4;
	float8		N,
				Sx,
				Sxx,
				tmp;

	pg_agg_err = 0;

	N = transvalues[0];
	Sx = transvalues[1];
	Sxx = transvalues[2];

	/*
	 * Use the Youngs-Cramer algorithm to incorporate the new value into the
	 * transition values.
	 */
	N += 1.0;
	Sx += newval;
	if (transvalues[0] > 0.0)
	{
		tmp = newval * N - Sx;
		Sxx += tmp * tmp / (N * transvalues[0]);

		/*
		 * Overflow check.  We only report an overflow error when finite
		 * inputs lead to infinite results.  Note also that Sxx should be NaN
		 * if any of the inputs are infinite, so we intentionally prevent Sxx
		 * from becoming infinite.
		 */
		if (isinf(Sx) || isinf(Sxx))
		{
			if (!isinf(transvalues[1]) && !isinf(newval))
				float_overflow_error();

			Sxx = get_float8_nan();
		}
	}
	else
	{
		/*
		 * At the first input, we normally can leave Sxx as 0.  However, if
		 * the first input is Inf or NaN, we'd better force Sxx to NaN;
		 * otherwise we will falsely report variance zero when there are no
		 * more inputs.
		 */
		if (isnan(newval) || isinf(newval))
			Sxx = get_float8_nan();
	}

	out[0] = N;
	out[1] = Sx;
	out[2] = Sxx;
	return pg_agg_err;
}

/* float8_combine, body verbatim */
int
pg_float8_combine(const float8 *transvalues1, const float8 *transvalues2,
				  float8 *out)
{
	float8		N1,
				Sx1,
				Sxx1,
				N2,
				Sx2,
				Sxx2,
				tmp,
				N,
				Sx,
				Sxx;

	pg_agg_err = 0;

	N1 = transvalues1[0];
	Sx1 = transvalues1[1];
	Sxx1 = transvalues1[2];

	N2 = transvalues2[0];
	Sx2 = transvalues2[1];
	Sxx2 = transvalues2[2];

	/*--------------------
	 * The transition values combine using a generalization of the
	 * Youngs-Cramer algorithm as follows:
	 *
	 *	N = N1 + N2
	 *	Sx = Sx1 + Sx2
	 *	Sxx = Sxx1 + Sxx2 + N1 * N2 * (Sx1/N1 - Sx2/N2)^2 / N;
	 *
	 * It's worth handling the special cases N1 = 0 and N2 = 0 separately
	 * since those cases are trivial, and we then don't need to worry about
	 * division-by-zero errors in the general case.
	 *--------------------
	 */
	if (N1 == 0.0)
	{
		N = N2;
		Sx = Sx2;
		Sxx = Sxx2;
	}
	else if (N2 == 0.0)
	{
		N = N1;
		Sx = Sx1;
		Sxx = Sxx1;
	}
	else
	{
		N = N1 + N2;
		Sx = float8_pl(Sx1, Sx2);
		tmp = Sx1 / N1 - Sx2 / N2;
		Sxx = Sxx1 + Sxx2 + N1 * N2 * tmp * tmp / N;
		if (unlikely(isinf(Sxx)) && !isinf(Sxx1) && !isinf(Sxx2))
			float_overflow_error();
	}

	out[0] = N;
	out[1] = Sx;
	out[2] = Sxx;
	return pg_agg_err;
}

/* float8_regr_accum, body verbatim */
int
pg_float8_regr_accum(const float8 *transvalues, float8 newvalY, float8 newvalX,
					 float8 *out)
{
	float8		N,
				Sx,
				Sxx,
				Sy,
				Syy,
				Sxy,
				tmpX,
				tmpY,
				scale;

	pg_agg_err = 0;

	N = transvalues[0];
	Sx = transvalues[1];
	Sxx = transvalues[2];
	Sy = transvalues[3];
	Syy = transvalues[4];
	Sxy = transvalues[5];

	/*
	 * Use the Youngs-Cramer algorithm to incorporate the new values into the
	 * transition values.
	 */
	N += 1.0;
	Sx += newvalX;
	Sy += newvalY;
	if (transvalues[0] > 0.0)
	{
		tmpX = newvalX * N - Sx;
		tmpY = newvalY * N - Sy;
		scale = 1.0 / (N * transvalues[0]);
		Sxx += tmpX * tmpX * scale;
		Syy += tmpY * tmpY * scale;
		Sxy += tmpX * tmpY * scale;

		/*
		 * Overflow check.  We only report an overflow error when finite
		 * inputs lead to infinite results.  Note also that Sxx, Syy and Sxy
		 * should be NaN if any of the relevant inputs are infinite, so we
		 * intentionally prevent them from becoming infinite.
		 */
		if (isinf(Sx) || isinf(Sxx) || isinf(Sy) || isinf(Syy) || isinf(Sxy))
		{
			if (((isinf(Sx) || isinf(Sxx)) &&
				 !isinf(transvalues[1]) && !isinf(newvalX)) ||
				((isinf(Sy) || isinf(Syy)) &&
				 !isinf(transvalues[3]) && !isinf(newvalY)) ||
				(isinf(Sxy) &&
				 !isinf(transvalues[1]) && !isinf(newvalX) &&
				 !isinf(transvalues[3]) && !isinf(newvalY)))
				float_overflow_error();

			if (isinf(Sxx))
				Sxx = get_float8_nan();
			if (isinf(Syy))
				Syy = get_float8_nan();
			if (isinf(Sxy))
				Sxy = get_float8_nan();
		}
	}
	else
	{
		/*
		 * At the first input, we normally can leave Sxx et al as 0.  However,
		 * if the first input is Inf or NaN, we'd better force the dependent
		 * sums to NaN; otherwise we will falsely report variance zero when
		 * there are no more inputs.
		 */
		if (isnan(newvalX) || isinf(newvalX))
			Sxx = Sxy = get_float8_nan();
		if (isnan(newvalY) || isinf(newvalY))
			Syy = Sxy = get_float8_nan();
	}

	out[0] = N;
	out[1] = Sx;
	out[2] = Sxx;
	out[3] = Sy;
	out[4] = Syy;
	out[5] = Sxy;
	return pg_agg_err;
}

/* float8_regr_combine, body verbatim */
int
pg_float8_regr_combine(const float8 *transvalues1, const float8 *transvalues2,
					   float8 *out)
{
	float8		N1,
				Sx1,
				Sxx1,
				Sy1,
				Syy1,
				Sxy1,
				N2,
				Sx2,
				Sxx2,
				Sy2,
				Syy2,
				Sxy2,
				tmp1,
				tmp2,
				N,
				Sx,
				Sxx,
				Sy,
				Syy,
				Sxy;

	pg_agg_err = 0;

	N1 = transvalues1[0];
	Sx1 = transvalues1[1];
	Sxx1 = transvalues1[2];
	Sy1 = transvalues1[3];
	Syy1 = transvalues1[4];
	Sxy1 = transvalues1[5];

	N2 = transvalues2[0];
	Sx2 = transvalues2[1];
	Sxx2 = transvalues2[2];
	Sy2 = transvalues2[3];
	Syy2 = transvalues2[4];
	Sxy2 = transvalues2[5];

	/*--------------------
	 * The transition values combine using a generalization of the
	 * Youngs-Cramer algorithm as follows:
	 *
	 *	N = N1 + N2
	 *	Sx = Sx1 + Sx2
	 *	Sxx = Sxx1 + Sxx2 + N1 * N2 * (Sx1/N1 - Sx2/N2)^2 / N
	 *	Sy = Sy1 + Sy2
	 *	Syy = Syy1 + Syy2 + N1 * N2 * (Sy1/N1 - Sy2/N2)^2 / N
	 *	Sxy = Sxy1 + Sxy2 + N1 * N2 * (Sx1/N1 - Sx2/N2) * (Sy1/N1 - Sy2/N2) / N
	 *
	 * It's worth handling the special cases N1 = 0 and N2 = 0 separately
	 * since those cases are trivial, and we then don't need to worry about
	 * division-by-zero errors in the general case.
	 *--------------------
	 */
	if (N1 == 0.0)
	{
		N = N2;
		Sx = Sx2;
		Sxx = Sxx2;
		Sy = Sy2;
		Syy = Syy2;
		Sxy = Sxy2;
	}
	else if (N2 == 0.0)
	{
		N = N1;
		Sx = Sx1;
		Sxx = Sxx1;
		Sy = Sy1;
		Syy = Syy1;
		Sxy = Sxy1;
	}
	else
	{
		N = N1 + N2;
		Sx = float8_pl(Sx1, Sx2);
		tmp1 = Sx1 / N1 - Sx2 / N2;
		Sxx = Sxx1 + Sxx2 + N1 * N2 * tmp1 * tmp1 / N;
		if (unlikely(isinf(Sxx)) && !isinf(Sxx1) && !isinf(Sxx2))
			float_overflow_error();
		Sy = float8_pl(Sy1, Sy2);
		tmp2 = Sy1 / N1 - Sy2 / N2;
		Syy = Syy1 + Syy2 + N1 * N2 * tmp2 * tmp2 / N;
		if (unlikely(isinf(Syy)) && !isinf(Syy1) && !isinf(Syy2))
			float_overflow_error();
		Sxy = Sxy1 + Sxy2 + N1 * N2 * tmp1 * tmp2 / N;
		if (unlikely(isinf(Sxy)) && !isinf(Sxy1) && !isinf(Sxy2))
			float_overflow_error();
	}

	out[0] = N;
	out[1] = Sx;
	out[2] = Sxx;
	out[3] = Sy;
	out[4] = Syy;
	out[5] = Sxy;
	return pg_agg_err;
}

/* ==================== final functions ==================== */

/*
 * Result protocol shims (header comment): each final takes the checked
 * transvalues and an out slot; returns 0 = value in *out, 1 = SQL NULL.
 */
#define PG_RETURN_NULL() return 1
#define PG_RETURN_FLOAT8(x) do { *out = (x); return 0; } while (0)

/* float8_avg, body verbatim */
int
pg_float8_avg(const float8 *transvalues, float8 *out)
{
	float8		N,
				Sx;

	N = transvalues[0];
	Sx = transvalues[1];
	/* ignore Sxx */

	/* SQL defines AVG of no values to be NULL */
	if (N == 0.0)
		PG_RETURN_NULL();

	PG_RETURN_FLOAT8(Sx / N);
}

/* float8_var_pop, body verbatim */
int
pg_float8_var_pop(const float8 *transvalues, float8 *out)
{
	float8		N,
				Sxx;

	N = transvalues[0];
	/* ignore Sx */
	Sxx = transvalues[2];

	/* Population variance is undefined when N is 0, so return NULL */
	if (N == 0.0)
		PG_RETURN_NULL();

	/* Note that Sxx is guaranteed to be non-negative */

	PG_RETURN_FLOAT8(Sxx / N);
}

/* float8_var_samp, body verbatim */
int
pg_float8_var_samp(const float8 *transvalues, float8 *out)
{
	float8		N,
				Sxx;

	N = transvalues[0];
	/* ignore Sx */
	Sxx = transvalues[2];

	/* Sample variance is undefined when N is 0 or 1, so return NULL */
	if (N <= 1.0)
		PG_RETURN_NULL();

	/* Note that Sxx is guaranteed to be non-negative */

	PG_RETURN_FLOAT8(Sxx / (N - 1.0));
}

/* float8_stddev_pop, body verbatim */
int
pg_float8_stddev_pop(const float8 *transvalues, float8 *out)
{
	float8		N,
				Sxx;

	N = transvalues[0];
	/* ignore Sx */
	Sxx = transvalues[2];

	/* Population stddev is undefined when N is 0, so return NULL */
	if (N == 0.0)
		PG_RETURN_NULL();

	/* Note that Sxx is guaranteed to be non-negative */

	PG_RETURN_FLOAT8(sqrt(Sxx / N));
}

/* float8_stddev_samp, body verbatim */
int
pg_float8_stddev_samp(const float8 *transvalues, float8 *out)
{
	float8		N,
				Sxx;

	N = transvalues[0];
	/* ignore Sx */
	Sxx = transvalues[2];

	/* Sample stddev is undefined when N is 0 or 1, so return NULL */
	if (N <= 1.0)
		PG_RETURN_NULL();

	/* Note that Sxx is guaranteed to be non-negative */

	PG_RETURN_FLOAT8(sqrt(Sxx / (N - 1.0)));
}

/* float8_regr_sxx, body verbatim */
int
pg_float8_regr_sxx(const float8 *transvalues, float8 *out)
{
	float8		N,
				Sxx;

	N = transvalues[0];
	Sxx = transvalues[2];

	/* if N is 0 we should return NULL */
	if (N < 1.0)
		PG_RETURN_NULL();

	/* Note that Sxx is guaranteed to be non-negative */

	PG_RETURN_FLOAT8(Sxx);
}

/* float8_regr_syy, body verbatim */
int
pg_float8_regr_syy(const float8 *transvalues, float8 *out)
{
	float8		N,
				Syy;

	N = transvalues[0];
	Syy = transvalues[4];

	/* if N is 0 we should return NULL */
	if (N < 1.0)
		PG_RETURN_NULL();

	/* Note that Syy is guaranteed to be non-negative */

	PG_RETURN_FLOAT8(Syy);
}

/* float8_regr_sxy, body verbatim */
int
pg_float8_regr_sxy(const float8 *transvalues, float8 *out)
{
	float8		N,
				Sxy;

	N = transvalues[0];
	Sxy = transvalues[5];

	/* if N is 0 we should return NULL */
	if (N < 1.0)
		PG_RETURN_NULL();

	/* A negative result is valid here */

	PG_RETURN_FLOAT8(Sxy);
}

/* float8_regr_avgx, body verbatim */
int
pg_float8_regr_avgx(const float8 *transvalues, float8 *out)
{
	float8		N,
				Sx;

	N = transvalues[0];
	Sx = transvalues[1];

	/* if N is 0 we should return NULL */
	if (N < 1.0)
		PG_RETURN_NULL();

	PG_RETURN_FLOAT8(Sx / N);
}

/* float8_regr_avgy, body verbatim */
int
pg_float8_regr_avgy(const float8 *transvalues, float8 *out)
{
	float8		N,
				Sy;

	N = transvalues[0];
	Sy = transvalues[3];

	/* if N is 0 we should return NULL */
	if (N < 1.0)
		PG_RETURN_NULL();

	PG_RETURN_FLOAT8(Sy / N);
}

/* float8_covar_pop, body verbatim */
int
pg_float8_covar_pop(const float8 *transvalues, float8 *out)
{
	float8		N,
				Sxy;

	N = transvalues[0];
	Sxy = transvalues[5];

	/* if N is 0 we should return NULL */
	if (N < 1.0)
		PG_RETURN_NULL();

	PG_RETURN_FLOAT8(Sxy / N);
}

/* float8_covar_samp, body verbatim */
int
pg_float8_covar_samp(const float8 *transvalues, float8 *out)
{
	float8		N,
				Sxy;

	N = transvalues[0];
	Sxy = transvalues[5];

	/* if N is <= 1 we should return NULL */
	if (N < 2.0)
		PG_RETURN_NULL();

	PG_RETURN_FLOAT8(Sxy / (N - 1.0));
}

/* float8_corr, body verbatim */
int
pg_float8_corr(const float8 *transvalues, float8 *out)
{
	float8		N,
				Sxx,
				Syy,
				Sxy;

	N = transvalues[0];
	Sxx = transvalues[2];
	Syy = transvalues[4];
	Sxy = transvalues[5];

	/* if N is 0 we should return NULL */
	if (N < 1.0)
		PG_RETURN_NULL();

	/* Note that Sxx and Syy are guaranteed to be non-negative */

	/* per spec, return NULL for horizontal and vertical lines */
	if (Sxx == 0 || Syy == 0)
		PG_RETURN_NULL();

	PG_RETURN_FLOAT8(Sxy / sqrt(Sxx * Syy));
}

/* float8_regr_r2, body verbatim */
int
pg_float8_regr_r2(const float8 *transvalues, float8 *out)
{
	float8		N,
				Sxx,
				Syy,
				Sxy;

	N = transvalues[0];
	Sxx = transvalues[2];
	Syy = transvalues[4];
	Sxy = transvalues[5];

	/* if N is 0 we should return NULL */
	if (N < 1.0)
		PG_RETURN_NULL();

	/* Note that Sxx and Syy are guaranteed to be non-negative */

	/* per spec, return NULL for a vertical line */
	if (Sxx == 0)
		PG_RETURN_NULL();

	/* per spec, return 1.0 for a horizontal line */
	if (Syy == 0)
		PG_RETURN_FLOAT8(1.0);

	PG_RETURN_FLOAT8((Sxy * Sxy) / (Sxx * Syy));
}

/* float8_regr_slope, body verbatim */
int
pg_float8_regr_slope(const float8 *transvalues, float8 *out)
{
	float8		N,
				Sxx,
				Sxy;

	N = transvalues[0];
	Sxx = transvalues[2];
	Sxy = transvalues[5];

	/* if N is 0 we should return NULL */
	if (N < 1.0)
		PG_RETURN_NULL();

	/* Note that Sxx is guaranteed to be non-negative */

	/* per spec, return NULL for a vertical line */
	if (Sxx == 0)
		PG_RETURN_NULL();

	PG_RETURN_FLOAT8(Sxy / Sxx);
}

/* float8_regr_intercept, body verbatim */
int
pg_float8_regr_intercept(const float8 *transvalues, float8 *out)
{
	float8		N,
				Sx,
				Sxx,
				Sy,
				Sxy;

	N = transvalues[0];
	Sx = transvalues[1];
	Sxx = transvalues[2];
	Sy = transvalues[3];
	Sxy = transvalues[5];

	/* if N is 0 we should return NULL */
	if (N < 1.0)
		PG_RETURN_NULL();

	/* Note that Sxx is guaranteed to be non-negative */

	/* per spec, return NULL for a vertical line */
	if (Sxx == 0)
		PG_RETURN_NULL();

	PG_RETURN_FLOAT8((Sy - Sx * Sxy / Sxx) / N);
}
