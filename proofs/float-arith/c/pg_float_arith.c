/*
 * Vendored PostgreSQL C: float unary + arithmetic operators.
 *
 * Provenance:
 *   - src/include/utils/float.h    @ postgres/postgres REL_18_STABLE
 *     (277122036c3382c5ab47034a180fde1176728c43, fetched 2026-07-28):
 *     RADIANS_PER_DEGREE, float4_pl/mi/mul/div, float8_pl/mi/mul/div —
 *     bodies verbatim.
 *   - src/backend/utils/adt/float.c @ same ref: float4abs/um/up,
 *     float8abs/um/up, float4pl/mi/mul/div, float8pl/mi/mul/div,
 *     float48pl/mi/mul/div, float84pl/mi/mul/div, degrees, radians, dpi —
 *     bodies verbatim.
 *   - src/backend/utils/adt/float.c @ same ref (rounding/classification
 *     wave, added 2026-07-28): dround, dceil, dfloor, dsign, dtrunc,
 *     dsqrt, in_range_float8_float8, in_range_float4_float8 — bodies
 *     verbatim.
 *
 * CANONICAL-NAN SHIM SCREENING (mandatory per prove-target; ruled
 * 2026-07-28): NO section vendored into this file reaches the NAN macro or
 * get_float8_nan() — the rounding wave uses rint/ceil/floor/sqrt plus
 * isnan/isinf tests, so NaN only PROPAGATES from inputs, which CBMC models
 * correctly (only the <math.h> NAN header CONSTANT is non-canonical; see
 * proofs/geo-cmp/CBMC-NAN-BUG-REPORT.md). The geo-cmp #undef NAN shim is
 * therefore NOT carried here; if any future section touches NAN /
 * get_float8_nan, copy the shim from proofs/geo-cmp/c/pg_geo_cmp.c and add
 * a native replay line. tests/semantics_check.rs pins the propagation
 * claim natively (dround(NaN) -> canonical quiet NaN bits).
 *
 * Shims (plumbing only, never logic):
 *   - PG_FUNCTION_ARGS unwrapping -> plain C signatures (float4/float8 args
 *     by value, exactly what PG_GETARG_FLOAT4/8 deliver).
 *   - float4 -> float, float8 -> double (their c.h typedefs).
 *   - unlikely(x) -> (x)  (branch hint only).
 *   - ereport(ERROR, ...) noreturn shim: float_overflow_error /
 *     float_underflow_error / float_zero_divide_error (float.c, pg_noinline
 *     void, each a single ereport) -> set a global pg_errflag and RETURN.
 *     Because the real functions never return, only the FIRST flag set is
 *     meaningful (first-error-wins guard below), and the value computed
 *     after a flagged error is garbage the real C never produces — the
 *     harness compares values only on the pg_errflag == 0 arm.
 *     Flag codes: 1 = overflow (ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
 *     2 = underflow (ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
 *     3 = zero divide (ERRCODE_DIVISION_BY_ZERO),
 *     4 = negative sqrt arg (ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION,
 *         dsqrt's inline ereport: noreturn in real C, so the shim returns
 *         the flag immediately without computing),
 *     5 = NaN/negative in_range offset
 *         (ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE, same
 *         return-immediately convention).
 *   - Fallible operators return the flag; the float result goes out via
 *     *out (same shape as proofs/cash's pg_cash_pl out-param shim).
 *   - fabsf/fabs/isinf/isnan come from <math.h>; CBMC models them
 *     bit-exactly on IEEE-754. M_PI fallback define for strict-ISO
 *     preprocessors; value is the same 3.14159265358979323846 literal
 *     <math.h> supplies.
 *
 * pg_float4pl_ieee / pg_float4um_wrong / pg_dtrunc_wrong /
 * pg_in_range_f8_noreject at the bottom are NOT Postgres code: they are
 * deliberately WRONG implementations used only by the negative-control
 * harnesses, which must fail. Never cite them as vendored C.
 */

#include <math.h>

#ifndef M_PI
#define M_PI 3.14159265358979323846
#endif

typedef float float4;
typedef double float8;

#define unlikely(x) (x)

/* ---- ereport shim: first-error-wins flag (see header comment) ---- */

int pg_errflag = 0;

static void
float_overflow_error(void)
{
	if (pg_errflag == 0)
		pg_errflag = 1;
}

static void
float_underflow_error(void)
{
	if (pg_errflag == 0)
		pg_errflag = 2;
}

static void
float_zero_divide_error(void)
{
	if (pg_errflag == 0)
		pg_errflag = 3;
}

/* ---- src/include/utils/float.h, verbatim ---- */

#define RADIANS_PER_DEGREE 0.0174532925199432957692

/*
 * Floating-point arithmetic with overflow/underflow reported as errors
 *
 * There isn't any way to check for underflow of addition/subtraction
 * because numbers near the underflow value have already been rounded to
 * the point where we can't detect that the two values were originally
 * different, e.g. on x86, '1e-45'::float4 == '2e-45'::float4 ==
 * 1.4013e-45.
 */

static inline float4
float4_pl(const float4 val1, const float4 val2)
{
	float4		result;

	result = val1 + val2;
	if (unlikely(isinf(result)) && !isinf(val1) && !isinf(val2))
		float_overflow_error();

	return result;
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

static inline float4
float4_mi(const float4 val1, const float4 val2)
{
	float4		result;

	result = val1 - val2;
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

static inline float4
float4_mul(const float4 val1, const float4 val2)
{
	float4		result;

	result = val1 * val2;
	if (unlikely(isinf(result)) && !isinf(val1) && !isinf(val2))
		float_overflow_error();
	if (unlikely(result == 0.0f) && val1 != 0.0f && val2 != 0.0f)
		float_underflow_error();

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

static inline float4
float4_div(const float4 val1, const float4 val2)
{
	float4		result;

	if (unlikely(val2 == 0.0f) && !isnan(val1))
		float_zero_divide_error();
	result = val1 / val2;
	if (unlikely(isinf(result)) && !isinf(val1))
		float_overflow_error();
	if (unlikely(result == 0.0f) && val1 != 0.0f && !isinf(val2))
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

/* ---- float.c: unary operators (infallible) ---- */

float4
pg_float4abs(float4 arg1)
{
	return fabsf(arg1);
}

float4
pg_float4um(float4 arg1)
{
	float4		result;

	result = -arg1;
	return result;
}

float4
pg_float4up(float4 arg)
{
	return arg;
}

float8
pg_float8abs(float8 arg1)
{
	return fabs(arg1);
}

float8
pg_float8um(float8 arg1)
{
	float8		result;

	result = -arg1;
	return result;
}

float8
pg_float8up(float8 arg)
{
	return arg;
}

/* ---- float.c: dpi (no args, returns pi) ---- */

float8
pg_dpi(void)
{
	return M_PI;
}

/* ---- float.c: fallible arithmetic (flag-returning shim wrappers) ----
 * Each body is the verbatim float.c Datum function reduced to its single
 * expression; the pg_errflag reset/return is the ereport shim's plumbing.
 */

int
pg_float4pl(float4 arg1, float4 arg2, float4 *out)
{
	pg_errflag = 0;
	*out = float4_pl(arg1, arg2);
	return pg_errflag;
}

int
pg_float4mi(float4 arg1, float4 arg2, float4 *out)
{
	pg_errflag = 0;
	*out = float4_mi(arg1, arg2);
	return pg_errflag;
}

int
pg_float4mul(float4 arg1, float4 arg2, float4 *out)
{
	pg_errflag = 0;
	*out = float4_mul(arg1, arg2);
	return pg_errflag;
}

int
pg_float4div(float4 arg1, float4 arg2, float4 *out)
{
	pg_errflag = 0;
	*out = float4_div(arg1, arg2);
	return pg_errflag;
}

int
pg_float8pl(float8 arg1, float8 arg2, float8 *out)
{
	pg_errflag = 0;
	*out = float8_pl(arg1, arg2);
	return pg_errflag;
}

int
pg_float8mi(float8 arg1, float8 arg2, float8 *out)
{
	pg_errflag = 0;
	*out = float8_mi(arg1, arg2);
	return pg_errflag;
}

int
pg_float8mul(float8 arg1, float8 arg2, float8 *out)
{
	pg_errflag = 0;
	*out = float8_mul(arg1, arg2);
	return pg_errflag;
}

int
pg_float8div(float8 arg1, float8 arg2, float8 *out)
{
	pg_errflag = 0;
	*out = float8_div(arg1, arg2);
	return pg_errflag;
}

/* float48*: C widens float4 -> float8 (exact), computes at float8. */

int
pg_float48pl(float4 arg1, float8 arg2, float8 *out)
{
	pg_errflag = 0;
	*out = float8_pl((float8) arg1, arg2);
	return pg_errflag;
}

int
pg_float48mi(float4 arg1, float8 arg2, float8 *out)
{
	pg_errflag = 0;
	*out = float8_mi((float8) arg1, arg2);
	return pg_errflag;
}

int
pg_float48mul(float4 arg1, float8 arg2, float8 *out)
{
	pg_errflag = 0;
	*out = float8_mul((float8) arg1, arg2);
	return pg_errflag;
}

int
pg_float48div(float4 arg1, float8 arg2, float8 *out)
{
	pg_errflag = 0;
	*out = float8_div((float8) arg1, arg2);
	return pg_errflag;
}

int
pg_float84pl(float8 arg1, float4 arg2, float8 *out)
{
	pg_errflag = 0;
	*out = float8_pl(arg1, (float8) arg2);
	return pg_errflag;
}

int
pg_float84mi(float8 arg1, float4 arg2, float8 *out)
{
	pg_errflag = 0;
	*out = float8_mi(arg1, (float8) arg2);
	return pg_errflag;
}

int
pg_float84mul(float8 arg1, float4 arg2, float8 *out)
{
	pg_errflag = 0;
	*out = float8_mul(arg1, (float8) arg2);
	return pg_errflag;
}

int
pg_float84div(float8 arg1, float4 arg2, float8 *out)
{
	pg_errflag = 0;
	*out = float8_div(arg1, (float8) arg2);
	return pg_errflag;
}

/* ---- float.c: degrees / radians (divide/multiply by constant) ---- */

int
pg_degrees(float8 arg1, float8 *out)
{
	pg_errflag = 0;
	*out = float8_div(arg1, RADIANS_PER_DEGREE);
	return pg_errflag;
}

int
pg_radians(float8 arg1, float8 *out)
{
	pg_errflag = 0;
	*out = float8_mul(arg1, RADIANS_PER_DEGREE);
	return pg_errflag;
}

/* ---- float.c: RANDOM FLOAT8 OPERATORS (rounding/sign family) ----
 * Infallible unary Datum fns reduced to plain float8 -> float8 signatures
 * (PG_GETARG_FLOAT8 / PG_RETURN_FLOAT8 unwrapping only); bodies verbatim.
 * rint/ceil/floor/sqrt come from <math.h> (CBMC IEEE-754 native models).
 */

/*
 *		dround			- returns	ROUND(arg1)
 */
float8
pg_dround(float8 arg1)
{
	return rint(arg1);
}

/*
 *		dceil			- returns the smallest integer greater than or
 *						  equal to the specified float
 */
float8
pg_dceil(float8 arg1)
{
	return ceil(arg1);
}

/*
 *		dfloor			- returns the largest integer lesser than or
 *						  equal to the specified float
 */
float8
pg_dfloor(float8 arg1)
{
	return floor(arg1);
}

/*
 *		dsign			- returns -1 if the argument is less than 0, 0
 *						  if the argument is equal to 0, and 1 if the
 *						  argument is greater than zero.
 */
float8
pg_dsign(float8 arg1)
{
	float8		result;

	if (arg1 > 0)
		result = 1.0;
	else if (arg1 < 0)
		result = -1.0;
	else
		result = 0.0;

	return result;
}

/*
 *		dtrunc			- returns truncation-towards-zero of arg1,
 *						  arg1 >= 0 ... the greatest integer less
 *										than or equal to arg1
 *						  arg1 < 0	... the least integer greater
 *										than or equal to arg1
 */
float8
pg_dtrunc(float8 arg1)
{
	float8		result;

	if (arg1 >= 0)
		result = floor(arg1);
	else
		result = -floor(-arg1);

	return result;
}

/*
 *		dsqrt			- returns square root of arg1
 * Flag-returning shim: the negative-arg ereport is noreturn in real C, so
 * the shim returns flag 4 immediately; overflow/underflow ride the shared
 * pg_errflag helpers exactly like the arithmetic wrappers above.
 */
int
pg_dsqrt(float8 arg1, float8 *out)
{
	float8		result;

	pg_errflag = 0;

	if (arg1 < 0)
		return 4;				/* ereport(ERROR, errcode(ERRCODE_INVALID_
								 * ARGUMENT_FOR_POWER_FUNCTION), "cannot
								 * take square root of a negative number") */

	result = sqrt(arg1);
	if (unlikely(isinf(result)) && !isinf(arg1))
		float_overflow_error();
	if (unlikely(result == 0.0) && arg1 != 0.0)
		float_underflow_error();

	*out = result;
	return pg_errflag;
}

/* ---- float.c: in_range support functions ----
 * PG_GETARG_* -> plain args (bool args as int per C ABI); each
 * PG_RETURN_BOOL(x) -> { *out = (x); return 0; }; the offset-reject
 * ereport (noreturn) -> return 5. Bodies otherwise verbatim, comments
 * included.
 */

int
pg_in_range_float8_float8(float8 val, float8 base, float8 offset,
						  int sub, int less, int *out)
{
	float8		sum;

	/*
	 * Reject negative or NaN offset.  Negative is per spec, and NaN is
	 * because appropriate semantics for that seem non-obvious.
	 */
	if (isnan(offset) || offset < 0)
		return 5;				/* ereport(ERROR, errcode(ERRCODE_INVALID_
								 * PRECEDING_OR_FOLLOWING_SIZE), "invalid
								 * preceding or following size in window
								 * function") */

	/*
	 * Deal with cases where val and/or base is NaN, following the rule that
	 * NaN sorts after non-NaN (cf float8_cmp_internal).  The offset cannot
	 * affect the conclusion.
	 */
	if (isnan(val))
	{
		if (isnan(base))
		{
			*out = 1;			/* NAN = NAN */
			return 0;
		}
		else
		{
			*out = !less;		/* NAN > non-NAN */
			return 0;
		}
	}
	else if (isnan(base))
	{
		*out = less;			/* non-NAN < NAN */
		return 0;
	}

	/*
	 * Deal with cases where both base and offset are infinite, and computing
	 * base +/- offset would produce NaN.  This corresponds to a window frame
	 * whose boundary infinitely precedes +inf or infinitely follows -inf,
	 * which is not well-defined.  For consistency with other cases involving
	 * infinities, such as the fact that +inf infinitely follows +inf, we
	 * choose to assume that +inf infinitely precedes +inf and -inf infinitely
	 * follows -inf, and therefore that all finite and infinite values are in
	 * such a window frame.
	 *
	 * offset is known positive, so we need only check the sign of base in
	 * this test.
	 */
	if (isinf(offset) && isinf(base) &&
		(sub ? base > 0 : base < 0))
	{
		*out = 1;
		return 0;
	}

	/*
	 * Otherwise it should be safe to compute base +/- offset.  We trust the
	 * FPU to cope if an input is +/-inf or the true sum would overflow, and
	 * produce a suitably signed infinity, which will compare properly against
	 * val whether or not that's infinity.
	 */
	if (sub)
		sum = base - offset;
	else
		sum = base + offset;

	if (less)
		*out = (val <= sum);
	else
		*out = (val >= sum);
	return 0;
}

int
pg_in_range_float4_float8(float4 val, float4 base, float8 offset,
						  int sub, int less, int *out)
{
	float8		sum;

	/*
	 * Reject negative or NaN offset.  Negative is per spec, and NaN is
	 * because appropriate semantics for that seem non-obvious.
	 */
	if (isnan(offset) || offset < 0)
		return 5;				/* same ereport shim as float8_float8 */

	/*
	 * Deal with cases where val and/or base is NaN, following the rule that
	 * NaN sorts after non-NaN (cf float8_cmp_internal).  The offset cannot
	 * affect the conclusion.
	 */
	if (isnan(val))
	{
		if (isnan(base))
		{
			*out = 1;			/* NAN = NAN */
			return 0;
		}
		else
		{
			*out = !less;		/* NAN > non-NAN */
			return 0;
		}
	}
	else if (isnan(base))
	{
		*out = less;			/* non-NAN < NAN */
		return 0;
	}

	/*
	 * Deal with cases where both base and offset are infinite, and computing
	 * base +/- offset would produce NaN.  This corresponds to a window frame
	 * whose boundary infinitely precedes +inf or infinitely follows -inf,
	 * which is not well-defined.  For consistency with other cases involving
	 * infinities, such as the fact that +inf infinitely follows +inf, we
	 * choose to assume that +inf infinitely precedes +inf and -inf infinitely
	 * follows -inf, and therefore that all finite and infinite values are in
	 * such a window frame.
	 *
	 * offset is known positive, so we need only check the sign of base in
	 * this test.
	 */
	if (isinf(offset) && isinf(base) &&
		(sub ? base > 0 : base < 0))
	{
		*out = 1;
		return 0;
	}

	/*
	 * Otherwise it should be safe to compute base +/- offset.  We trust the
	 * FPU to cope if an input is +/-inf or the true sum would overflow, and
	 * produce a suitably signed infinity, which will compare properly against
	 * val whether or not that's infinity.
	 *
	 * (base is float4, offset float8: C's usual arithmetic conversions
	 * promote base to double, exactly as here; likewise val in the final
	 * compares.)
	 */
	if (sub)
		sum = base - offset;
	else
		sum = base + offset;

	if (less)
		*out = (val <= sum);
	else
		*out = (val >= sum);
	return 0;
}

/* ---- NEGATIVE-CONTROL ONLY: not Postgres code ----
 * pg_float4um_wrong returns |x| instead of -x: differs from float4um on
 * every x whose negation is not its absolute value (x > 0, and -0/NaN sign
 * bits). The control harness pits fc_float4um against this and MUST fail.
 * pg_float4pl_ieee is plain IEEE + with NO overflow ereport: differs from
 * float4pl exactly on the finite+finite=Inf overflow arm — the error-arm
 * control harness MUST fail there.
 */
float4
pg_float4um_wrong(float4 arg1)
{
	return fabsf(arg1);
}

int
pg_float4pl_ieee(float4 arg1, float4 arg2, float4 *out)
{
	*out = arg1 + arg2;
	return 0;
}

/* pg_dtrunc_wrong rounds-to-nearest-even (rint) instead of truncating
 * toward zero: differs from dtrunc on any |frac| >= 0.5 that rounds away
 * (e.g. 1.5 -> trunc 1.0 vs rint 2.0). The rounding-section control
 * harness pits fc_dtrunc against this and MUST fail. */
float8
pg_dtrunc_wrong(float8 arg1)
{
	return rint(arg1);
}

/* pg_in_range_f8_noreject skips the NaN/negative offset reject (always
 * flag 0) and otherwise short-circuits to the val<=/>= compare against
 * base +/- offset with no NaN/Inf handling: the in_range-section control
 * pits fc_in_range_float8_float8 against this and MUST fail (e.g. any
 * NaN offset: Rust errors 22013, this returns 0). */
int
pg_in_range_f8_noreject(float8 val, float8 base, float8 offset,
						int sub, int less, int *out)
{
	float8		sum;

	if (sub)
		sum = base - offset;
	else
		sum = base + offset;

	if (less)
		*out = (val <= sum);
	else
		*out = (val >= sum);
	return 0;
}

/* ================= width_bucket wave (added 2026-07-29) =================
 *
 * width_bucket_float8 from src/backend/utils/adt/float.c @ REL_18_STABLE
 * (same ref as header). Body VERBATIM; shims per the file convention:
 *   - PG_FUNCTION_ARGS unwrapping -> plain C signature, result via *out.
 *   - ereport(ERROR, ...) -> return flag immediately (all four ereports in
 *     this function are noreturn in real C). New flag codes:
 *       6 = ERRCODE_INVALID_ARGUMENT_FOR_WIDTH_BUCKET_FUNCTION (2201G):
 *           count <= 0 / NaN args / infinite bounds / bound1 == bound2;
 *       7 = ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE (22003): count+1 overflow.
 *   - pg_add_s32_overflow vendored verbatim from
 *     src/include/common/int.h @ same ref (__builtin_add_overflow form).
 */

#include <stdbool.h>

typedef int int32;

static inline bool
pg_add_s32_overflow(int32 a, int32 b, int32 *result)
{
	return __builtin_add_overflow(a, b, result);
}

int
pg_width_bucket_float8(float8 operand, float8 bound1, float8 bound2,
					   int32 count, int32 *out)
{
	int32		result;

	if (count <= 0)
		return 6;

	if (isnan(operand) || isnan(bound1) || isnan(bound2))
		return 6;

	/* Note that we allow "operand" to be infinite */
	if (isinf(bound1) || isinf(bound2))
		return 6;

	if (bound1 < bound2)
	{
		if (operand < bound1)
			result = 0;
		else if (operand >= bound2)
		{
			if (pg_add_s32_overflow(count, 1, &result))
				return 7;
		}
		else
		{
			if (!isinf(bound2 - bound1))
			{
				/* The quotient is surely in [0,1], so this can't overflow */
				result = count * ((operand - bound1) / (bound2 - bound1));
			}
			else
			{
				/*
				 * We get here if bound2 - bound1 overflows DBL_MAX.  Since
				 * both bounds are finite, their difference can't exceed twice
				 * DBL_MAX; so we can perform the computation without overflow
				 * by dividing all the inputs by 2.  That should be exact too,
				 * except in the case where a very small operand underflows to
				 * zero, which would have negligible impact on the result
				 * given such large bounds.
				 */
				result = count * ((operand / 2 - bound1 / 2) / (bound2 / 2 - bound1 / 2));
			}
			/* The quotient could round to 1.0, which would be a lie */
			if (result >= count)
				result = count - 1;
			/* Having done that, we can add 1 without fear of overflow */
			result++;
		}
	}
	else if (bound1 > bound2)
	{
		if (operand > bound1)
			result = 0;
		else if (operand <= bound2)
		{
			if (pg_add_s32_overflow(count, 1, &result))
				return 7;
		}
		else
		{
			if (!isinf(bound1 - bound2))
				result = count * ((bound1 - operand) / (bound1 - bound2));
			else
				result = count * ((bound1 / 2 - operand / 2) / (bound1 / 2 - bound2 / 2));
			if (result >= count)
				result = count - 1;
			result++;
		}
	}
	else
	{
		return 6;
	}

	*out = result;
	return 0;
}

/* ================= send wave (added 2026-07-29) =================
 *
 * float4send/float8send wire images, int-arith pg_int4send precedent:
 * pq_begintypsend reserves 4 header bytes; pq_sendfloat4/pq_sendfloat8
 * (src/backend/libpq/pqformat.c @ REL_18_STABLE) union-bit-copy the float
 * into uint32/uint64 and append big-endian via pq_sendint32/64;
 * pq_endtypsend stamps the 4B little-endian varlena header (len << 2).
 * Shim: StringInfo -> caller buffer of exact total size.
 */

typedef unsigned int uint32;
typedef unsigned long long uint64;

/* shared endtypsend tail: 4B little-endian varlena header (len << 2) */
static void
pg_set_varsize_4b(unsigned char *out, int32 len)
{
	uint32		hdr = (uint32) len << 2;

	out[0] = (unsigned char) (hdr & 0xFF);
	out[1] = (unsigned char) ((hdr >> 8) & 0xFF);
	out[2] = (unsigned char) ((hdr >> 16) & 0xFF);
	out[3] = (unsigned char) ((hdr >> 24) & 0xFF);
}

int32
pg_float4send(float4 num, unsigned char *out /* [8] */ )
{
	/* pq_sendfloat4: union { float4 f; uint32 i; } swap; swap.f = f */
	union
	{
		float4		f;
		uint32		i;
	}			swap;

	swap.f = num;
	out[4] = (unsigned char) ((swap.i >> 24) & 0xFF);
	out[5] = (unsigned char) ((swap.i >> 16) & 0xFF);
	out[6] = (unsigned char) ((swap.i >> 8) & 0xFF);
	out[7] = (unsigned char) (swap.i & 0xFF);
	pg_set_varsize_4b(out, 8);
	return 8;
}

int32
pg_float8send(float8 num, unsigned char *out /* [12] */ )
{
	/* pq_sendfloat8: union { float8 f; int64 i; } swap; swap.f = f */
	union
	{
		float8		f;
		uint64		i;
	}			swap;
	int			i;

	swap.f = num;
	for (i = 0; i < 8; i++)
		out[4 + i] = (unsigned char) ((swap.i >> (8 * (7 - i))) & 0xFF);
	pg_set_varsize_4b(out, 12);
	return 12;
}
