/*
 * Vendored PostgreSQL C: float8 math functions (libm family) —
 * differential-fuzz oracle.
 *
 * Provenance (all bodies VERBATIM unless a shim is listed below):
 *   - src/backend/utils/adt/float.c @ postgres-src
 *     62d6c7d3df6287f1bd83199c1a746e50d31571a0 (REL_18, the repo's vendored
 *     ground-truth checkout ../pgrust-fabled/vendor/postgres-src, stamp
 *     18.3), extracted byte-for-byte by line range:
 *       lines 43-99: degree-constant globals (degree_consts_set ..
 *         degree_c_one), local prototypes (sind_q1, cosd_q1,
 *         init_degree_constants), float_overflow_error,
 *         float_underflow_error.  float_zero_divide_error (lines 101-108)
 *         is elided: unreachable from the vendored functions.
 *       lines 1465-2554: dcbrt, dpow, dexp, dlog1, dlog10, dacos, dasin,
 *         datan, datan2, dcos, dcot, dsin, dtan, init_degree_constants,
 *         INIT_DEGREE_CONSTANTS, asind_q1, acosd_q1, dacosd, dasind,
 *         datand, datan2d, sind_0_to_30, cosd_0_to_60, sind_q1, cosd_q1,
 *         dcosd, dcotd, dsind, dtand.
 *       lines 2591-2874: dsinh, dcosh, dtanh, dasinh, dacosh, datanh,
 *         derf, derfc, dgamma, dlgamma.  (degrees/dpi/radians, lines
 *         2556-2589, are not in the libm-blocked ledger set and are
 *         elided.)
 *   - src/include/utils/float.h @ same ref: RADIANS_PER_DEGREE define,
 *     get_float8_nan, get_float8_infinity — verbatim.
 *
 * Shims (plumbing only, never logic):
 *   - fmgr surface: PG_FUNCTION_ARGS -> a two-double fcinfo struct;
 *     PG_GETARG_FLOAT8(n) -> fcinfo->arg[n]; Datum -> float8;
 *     PG_RETURN_FLOAT8(x) -> return (Datum) (x).  The vendored bodies only
 *     ever pass float8 through these macros, so the Datum boxing they
 *     replace is pure plumbing.
 *   - ereport(ERROR, (errcode(X), errmsg(...))) -> record X in
 *     a thread-local errcode private to this TU and longjmp back to the
 *     pg_diff_float_math driver — the same non-returning control flow as
 *     PG's error longjmp.  errmsg(...) evaluates to 0 with arguments
 *     unevaluated (the comparator checks errcode class, not message text).
 *   - errcode symbols -> small ints extending the pg_float_io.c
 *     convention: 2 = ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE (22003),
 *     3 = ERRCODE_INVALID_ARGUMENT_FOR_LOG (2201E),
 *     4 = ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION (2201F).
 *   - pg_noinline -> empty (an optimization pragma, not semantics).
 *   - unlikely(x) -> (x) via shim postgres.h.
 *
 * NOTE the oracle's math core is the platform libm (exp/sin/tgamma/...),
 * exactly as in real PostgreSQL (which defers to libm); on this host that
 * is macOS libm.  Shipped Rust also resolves to the platform libm (std
 * float methods + the funcs.rs extern "C" bindings), so a mismatch here is
 * a wrapper-logic divergence, not a libm-version artifact — except where C
 * relies on errno while Rust classifies by value; those are exactly the
 * arms this fuzz is for.
 */

#include "postgres.h"

#include <errno.h>
#include <math.h>
#include <setjmp.h>

/* ---- shims (see header comment) ---- */

/*
 * Error plumbing is deliberately THREAD-LOCAL and private to this TU
 * (unlike pg_float_io.c's shared pg_diff_errcode): cargo test runs the
 * differential smoke tests in parallel threads, and a shared global raced
 * (another test's errcode reset between this oracle's errcode-record and
 * the post-longjmp read produced a phantom "success" verdict).
 */
static _Thread_local int pg_diff_fmath_errcode;
static _Thread_local jmp_buf pg_diff_fmath_jmp;

#define ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE 2
#define ERRCODE_INVALID_ARGUMENT_FOR_LOG 3
#define ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION 4
#define ERRCODE_DIVISION_BY_ZERO 5

#define errcode(c) (pg_diff_fmath_errcode = (c))
#define errmsg(...) 0
#define ereport(level, stuff) \
	do { (void) (stuff); longjmp(pg_diff_fmath_jmp, 1); } while (0)

#define pg_noinline

typedef float8 Datum;

typedef struct pg_diff_fcargs
{
	float8		arg[2];
} pg_diff_fcargs;

#define PG_FUNCTION_ARGS pg_diff_fcargs *fcinfo
#define PG_GETARG_FLOAT8(n) (fcinfo->arg[(n)])
#define PG_RETURN_FLOAT8(x) return ((Datum) (x))

/* ---- src/include/utils/float.h: RADIANS_PER_DEGREE — VERBATIM ---- */

#define RADIANS_PER_DEGREE 0.0174532925199432957692

/* ---- src/include/utils/float.h: special-value helpers — VERBATIM ---- */

static inline float8
get_float8_infinity(void)
{
#ifdef INFINITY
	/* C99 standard way */
	return (float8) INFINITY;
#else
	return (float8) (HUGE_VAL * HUGE_VAL);
#endif
}

static inline float8
get_float8_nan(void)
{
	/* (float8) NAN doesn't work on some NetBSD/MIPS releases */
#if defined(NAN) && !(defined(__NetBSD__) && defined(__mips__))
	/* C99 standard way */
	return (float8) NAN;
#else
	/* Assume we can get a NaN via zero divide */
	return (float8) (0.0 / 0.0);
#endif
}

/* ==== src/backend/utils/adt/float.c lines 43-99 — VERBATIM ==== */

static bool degree_consts_set = false;
static float8 sin_30 = 0;
static float8 one_minus_cos_60 = 0;
static float8 asin_0_5 = 0;
static float8 acos_0_5 = 0;
static float8 atan_1_0 = 0;
static float8 tan_45 = 0;
static float8 cot_45 = 0;

/*
 * These are intentionally not static; don't "fix" them.  They will never
 * be referenced by other files, much less changed; but we don't want the
 * compiler to know that, else it might try to precompute expressions
 * involving them.  See comments for init_degree_constants().
 *
 * The additional extern declarations are to silence
 * -Wmissing-variable-declarations.
 */
extern float8 degree_c_thirty;
extern float8 degree_c_forty_five;
extern float8 degree_c_sixty;
extern float8 degree_c_one_half;
extern float8 degree_c_one;
float8		degree_c_thirty = 30.0;
float8		degree_c_forty_five = 45.0;
float8		degree_c_sixty = 60.0;
float8		degree_c_one_half = 0.5;
float8		degree_c_one = 1.0;

/* Local function prototypes */
static double sind_q1(double x);
static double cosd_q1(double x);
static void init_degree_constants(void);


/*
 * We use these out-of-line ereport() calls to report float overflow,
 * underflow, and zero-divide, because following our usual practice of
 * repeating them at each call site would lead to a lot of code bloat.
 *
 * This does mean that you don't get a useful error location indicator.
 */
pg_noinline void
float_overflow_error(void)
{
	ereport(ERROR,
			(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
			 errmsg("value out of range: overflow")));
}

pg_noinline void
float_underflow_error(void)
{
	ereport(ERROR,
			(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
			 errmsg("value out of range: underflow")));
}

/* ==== src/backend/utils/adt/float.c lines 101-108 — VERBATIM (p1-lanead:
 * un-elided; reachable from the newly vendored float8_div path) ==== */

pg_noinline void
float_zero_divide_error(void)
{
	ereport(ERROR,
			(errcode(ERRCODE_DIVISION_BY_ZERO),
			 errmsg("division by zero")));
}

/* ==== src/include/utils/float.h float8_mul / float8_div — VERBATIM
 * (p1-lanead: callees of the newly vendored degrees/radians) ==== */

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

/* ==== src/backend/utils/adt/float.c lines 1465-2554 — VERBATIM ==== */

/*
 *		dcbrt			- returns cube root of arg1
 */
Datum
dcbrt(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		result;

	result = cbrt(arg1);
	if (unlikely(isinf(result)) && !isinf(arg1))
		float_overflow_error();
	if (unlikely(result == 0.0) && arg1 != 0.0)
		float_underflow_error();

	PG_RETURN_FLOAT8(result);
}


/*
 *		dpow			- returns pow(arg1,arg2)
 */
Datum
dpow(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		arg2 = PG_GETARG_FLOAT8(1);
	float8		result;

	/*
	 * The POSIX spec says that NaN ^ 0 = 1, and 1 ^ NaN = 1, while all other
	 * cases with NaN inputs yield NaN (with no error).  Many older platforms
	 * get one or more of these cases wrong, so deal with them via explicit
	 * logic rather than trusting pow(3).
	 */
	if (isnan(arg1))
	{
		if (isnan(arg2) || arg2 != 0.0)
			PG_RETURN_FLOAT8(get_float8_nan());
		PG_RETURN_FLOAT8(1.0);
	}
	if (isnan(arg2))
	{
		if (arg1 != 1.0)
			PG_RETURN_FLOAT8(get_float8_nan());
		PG_RETURN_FLOAT8(1.0);
	}

	/*
	 * The SQL spec requires that we emit a particular SQLSTATE error code for
	 * certain error conditions.  Specifically, we don't return a
	 * divide-by-zero error code for 0 ^ -1.
	 */
	if (arg1 == 0 && arg2 < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION),
				 errmsg("zero raised to a negative power is undefined")));
	if (arg1 < 0 && floor(arg2) != arg2)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION),
				 errmsg("a negative number raised to a non-integer power yields a complex result")));

	/*
	 * We don't trust the platform's pow() to handle infinity cases per POSIX
	 * spec either, so deal with those explicitly too.  It's easier to handle
	 * infinite y first, so that it doesn't matter if x is also infinite.
	 */
	if (isinf(arg2))
	{
		float8		absx = fabs(arg1);

		if (absx == 1.0)
			result = 1.0;
		else if (arg2 > 0.0)	/* y = +Inf */
		{
			if (absx > 1.0)
				result = arg2;
			else
				result = 0.0;
		}
		else					/* y = -Inf */
		{
			if (absx > 1.0)
				result = 0.0;
			else
				result = -arg2;
		}
	}
	else if (isinf(arg1))
	{
		if (arg2 == 0.0)
			result = 1.0;
		else if (arg1 > 0.0)	/* x = +Inf */
		{
			if (arg2 > 0.0)
				result = arg1;
			else
				result = 0.0;
		}
		else					/* x = -Inf */
		{
			/*
			 * Per POSIX, the sign of the result depends on whether y is an
			 * odd integer.  Since x < 0, we already know from the previous
			 * domain check that y is an integer.  It is odd if y/2 is not
			 * also an integer.
			 */
			float8		halfy = arg2 / 2;	/* should be computed exactly */
			bool		yisoddinteger = (floor(halfy) != halfy);

			if (arg2 > 0.0)
				result = yisoddinteger ? arg1 : -arg1;
			else
				result = yisoddinteger ? -0.0 : 0.0;
		}
	}
	else
	{
		/*
		 * pow() sets errno on only some platforms, depending on whether it
		 * follows _IEEE_, _POSIX_, _XOPEN_, or _SVID_, so we must check both
		 * errno and invalid output values.  (We can't rely on just the
		 * latter, either; some old platforms return a large-but-finite
		 * HUGE_VAL when reporting overflow.)
		 */
		errno = 0;
		result = pow(arg1, arg2);
		if (errno == EDOM || isnan(result))
		{
			/*
			 * We handled all possible domain errors above, so this should be
			 * impossible.  However, old glibc versions on x86 have a bug that
			 * causes them to fail this way for abs(y) greater than 2^63:
			 *
			 * https://sourceware.org/bugzilla/show_bug.cgi?id=3866
			 *
			 * Hence, if we get here, assume y is finite but large (large
			 * enough to be certainly even). The result should be 0 if x == 0,
			 * 1.0 if abs(x) == 1.0, otherwise an overflow or underflow error.
			 */
			if (arg1 == 0.0)
				result = 0.0;	/* we already verified y is positive */
			else
			{
				float8		absx = fabs(arg1);

				if (absx == 1.0)
					result = 1.0;
				else if (arg2 >= 0.0 ? (absx > 1.0) : (absx < 1.0))
					float_overflow_error();
				else
					float_underflow_error();
			}
		}
		else if (errno == ERANGE)
		{
			if (result != 0.0)
				float_overflow_error();
			else
				float_underflow_error();
		}
		else
		{
			if (unlikely(isinf(result)))
				float_overflow_error();
			if (unlikely(result == 0.0) && arg1 != 0.0)
				float_underflow_error();
		}
	}

	PG_RETURN_FLOAT8(result);
}


/*
 *		dexp			- returns the exponential function of arg1
 */
Datum
dexp(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		result;

	/*
	 * Handle NaN and Inf cases explicitly.  This avoids needing to assume
	 * that the platform's exp() conforms to POSIX for these cases, and it
	 * removes some edge cases for the overflow checks below.
	 */
	if (isnan(arg1))
		result = arg1;
	else if (isinf(arg1))
	{
		/* Per POSIX, exp(-Inf) is 0 */
		result = (arg1 > 0.0) ? arg1 : 0;
	}
	else
	{
		/*
		 * On some platforms, exp() will not set errno but just return Inf or
		 * zero to report overflow/underflow; therefore, test both cases.
		 */
		errno = 0;
		result = exp(arg1);
		if (unlikely(errno == ERANGE))
		{
			if (result != 0.0)
				float_overflow_error();
			else
				float_underflow_error();
		}
		else if (unlikely(isinf(result)))
			float_overflow_error();
		else if (unlikely(result == 0.0))
			float_underflow_error();
	}

	PG_RETURN_FLOAT8(result);
}


/*
 *		dlog1			- returns the natural logarithm of arg1
 */
Datum
dlog1(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		result;

	/*
	 * Emit particular SQLSTATE error codes for ln(). This is required by the
	 * SQL standard.
	 */
	if (arg1 == 0.0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_ARGUMENT_FOR_LOG),
				 errmsg("cannot take logarithm of zero")));
	if (arg1 < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_ARGUMENT_FOR_LOG),
				 errmsg("cannot take logarithm of a negative number")));

	result = log(arg1);
	if (unlikely(isinf(result)) && !isinf(arg1))
		float_overflow_error();
	if (unlikely(result == 0.0) && arg1 != 1.0)
		float_underflow_error();

	PG_RETURN_FLOAT8(result);
}


/*
 *		dlog10			- returns the base 10 logarithm of arg1
 */
Datum
dlog10(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		result;

	/*
	 * Emit particular SQLSTATE error codes for log(). The SQL spec doesn't
	 * define log(), but it does define ln(), so it makes sense to emit the
	 * same error code for an analogous error condition.
	 */
	if (arg1 == 0.0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_ARGUMENT_FOR_LOG),
				 errmsg("cannot take logarithm of zero")));
	if (arg1 < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_ARGUMENT_FOR_LOG),
				 errmsg("cannot take logarithm of a negative number")));

	result = log10(arg1);
	if (unlikely(isinf(result)) && !isinf(arg1))
		float_overflow_error();
	if (unlikely(result == 0.0) && arg1 != 1.0)
		float_underflow_error();

	PG_RETURN_FLOAT8(result);
}


/*
 *		dacos			- returns the arccos of arg1 (radians)
 */
Datum
dacos(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		result;

	/* Per the POSIX spec, return NaN if the input is NaN */
	if (isnan(arg1))
		PG_RETURN_FLOAT8(get_float8_nan());

	/*
	 * The principal branch of the inverse cosine function maps values in the
	 * range [-1, 1] to values in the range [0, Pi], so we should reject any
	 * inputs outside that range and the result will always be finite.
	 */
	if (arg1 < -1.0 || arg1 > 1.0)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("input is out of range")));

	result = acos(arg1);
	if (unlikely(isinf(result)))
		float_overflow_error();

	PG_RETURN_FLOAT8(result);
}


/*
 *		dasin			- returns the arcsin of arg1 (radians)
 */
Datum
dasin(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		result;

	/* Per the POSIX spec, return NaN if the input is NaN */
	if (isnan(arg1))
		PG_RETURN_FLOAT8(get_float8_nan());

	/*
	 * The principal branch of the inverse sine function maps values in the
	 * range [-1, 1] to values in the range [-Pi/2, Pi/2], so we should reject
	 * any inputs outside that range and the result will always be finite.
	 */
	if (arg1 < -1.0 || arg1 > 1.0)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("input is out of range")));

	result = asin(arg1);
	if (unlikely(isinf(result)))
		float_overflow_error();

	PG_RETURN_FLOAT8(result);
}


/*
 *		datan			- returns the arctan of arg1 (radians)
 */
Datum
datan(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		result;

	/* Per the POSIX spec, return NaN if the input is NaN */
	if (isnan(arg1))
		PG_RETURN_FLOAT8(get_float8_nan());

	/*
	 * The principal branch of the inverse tangent function maps all inputs to
	 * values in the range [-Pi/2, Pi/2], so the result should always be
	 * finite, even if the input is infinite.
	 */
	result = atan(arg1);
	if (unlikely(isinf(result)))
		float_overflow_error();

	PG_RETURN_FLOAT8(result);
}


/*
 *		atan2			- returns the arctan of arg1/arg2 (radians)
 */
Datum
datan2(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		arg2 = PG_GETARG_FLOAT8(1);
	float8		result;

	/* Per the POSIX spec, return NaN if either input is NaN */
	if (isnan(arg1) || isnan(arg2))
		PG_RETURN_FLOAT8(get_float8_nan());

	/*
	 * atan2 maps all inputs to values in the range [-Pi, Pi], so the result
	 * should always be finite, even if the inputs are infinite.
	 */
	result = atan2(arg1, arg2);
	if (unlikely(isinf(result)))
		float_overflow_error();

	PG_RETURN_FLOAT8(result);
}


/*
 *		dcos			- returns the cosine of arg1 (radians)
 */
Datum
dcos(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		result;

	/* Per the POSIX spec, return NaN if the input is NaN */
	if (isnan(arg1))
		PG_RETURN_FLOAT8(get_float8_nan());

	/*
	 * cos() is periodic and so theoretically can work for all finite inputs,
	 * but some implementations may choose to throw error if the input is so
	 * large that there are no significant digits in the result.  So we should
	 * check for errors.  POSIX allows an error to be reported either via
	 * errno or via fetestexcept(), but currently we only support checking
	 * errno.  (fetestexcept() is rumored to report underflow unreasonably
	 * early on some platforms, so it's not clear that believing it would be a
	 * net improvement anyway.)
	 *
	 * For infinite inputs, POSIX specifies that the trigonometric functions
	 * should return a domain error; but we won't notice that unless the
	 * platform reports via errno, so also explicitly test for infinite
	 * inputs.
	 */
	errno = 0;
	result = cos(arg1);
	if (errno != 0 || isinf(arg1))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("input is out of range")));
	if (unlikely(isinf(result)))
		float_overflow_error();

	PG_RETURN_FLOAT8(result);
}


/*
 *		dcot			- returns the cotangent of arg1 (radians)
 */
Datum
dcot(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		result;

	/* Per the POSIX spec, return NaN if the input is NaN */
	if (isnan(arg1))
		PG_RETURN_FLOAT8(get_float8_nan());

	/* Be sure to throw an error if the input is infinite --- see dcos() */
	errno = 0;
	result = tan(arg1);
	if (errno != 0 || isinf(arg1))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("input is out of range")));

	result = 1.0 / result;
	/* Not checking for overflow because cot(0) == Inf */

	PG_RETURN_FLOAT8(result);
}


/*
 *		dsin			- returns the sine of arg1 (radians)
 */
Datum
dsin(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		result;

	/* Per the POSIX spec, return NaN if the input is NaN */
	if (isnan(arg1))
		PG_RETURN_FLOAT8(get_float8_nan());

	/* Be sure to throw an error if the input is infinite --- see dcos() */
	errno = 0;
	result = sin(arg1);
	if (errno != 0 || isinf(arg1))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("input is out of range")));
	if (unlikely(isinf(result)))
		float_overflow_error();

	PG_RETURN_FLOAT8(result);
}


/*
 *		dtan			- returns the tangent of arg1 (radians)
 */
Datum
dtan(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		result;

	/* Per the POSIX spec, return NaN if the input is NaN */
	if (isnan(arg1))
		PG_RETURN_FLOAT8(get_float8_nan());

	/* Be sure to throw an error if the input is infinite --- see dcos() */
	errno = 0;
	result = tan(arg1);
	if (errno != 0 || isinf(arg1))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("input is out of range")));
	/* Not checking for overflow because tan(pi/2) == Inf */

	PG_RETURN_FLOAT8(result);
}


/* ========== DEGREE-BASED TRIGONOMETRIC FUNCTIONS ========== */


/*
 * Initialize the cached constants declared at the head of this file
 * (sin_30 etc).  The fact that we need those at all, let alone need this
 * Rube-Goldberg-worthy method of initializing them, is because there are
 * compilers out there that will precompute expressions such as sin(constant)
 * using a sin() function different from what will be used at runtime.  If we
 * want exact results, we must ensure that none of the scaling constants used
 * in the degree-based trig functions are computed that way.  To do so, we
 * compute them from the variables degree_c_thirty etc, which are also really
 * constants, but the compiler cannot assume that.
 *
 * Other hazards we are trying to forestall with this kluge include the
 * possibility that compilers will rearrange the expressions, or compute
 * some intermediate results in registers wider than a standard double.
 *
 * In the places where we use these constants, the typical pattern is like
 *		volatile float8 sin_x = sin(x * RADIANS_PER_DEGREE);
 *		return (sin_x / sin_30);
 * where we hope to get a value of exactly 1.0 from the division when x = 30.
 * The volatile temporary variable is needed on machines with wide float
 * registers, to ensure that the result of sin(x) is rounded to double width
 * the same as the value of sin_30 has been.  Experimentation with gcc shows
 * that marking the temp variable volatile is necessary to make the store and
 * reload actually happen; hopefully the same trick works for other compilers.
 * (gcc's documentation suggests using the -ffloat-store compiler switch to
 * ensure this, but that is compiler-specific and it also pessimizes code in
 * many places where we don't care about this.)
 */
static void
init_degree_constants(void)
{
	sin_30 = sin(degree_c_thirty * RADIANS_PER_DEGREE);
	one_minus_cos_60 = 1.0 - cos(degree_c_sixty * RADIANS_PER_DEGREE);
	asin_0_5 = asin(degree_c_one_half);
	acos_0_5 = acos(degree_c_one_half);
	atan_1_0 = atan(degree_c_one);
	tan_45 = sind_q1(degree_c_forty_five) / cosd_q1(degree_c_forty_five);
	cot_45 = cosd_q1(degree_c_forty_five) / sind_q1(degree_c_forty_five);
	degree_consts_set = true;
}

#define INIT_DEGREE_CONSTANTS() \
do { \
	if (!degree_consts_set) \
		init_degree_constants(); \
} while(0)


/*
 *		asind_q1		- returns the inverse sine of x in degrees, for x in
 *						  the range [0, 1].  The result is an angle in the
 *						  first quadrant --- [0, 90] degrees.
 *
 *						  For the 3 special case inputs (0, 0.5 and 1), this
 *						  function will return exact values (0, 30 and 90
 *						  degrees respectively).
 */
static double
asind_q1(double x)
{
	/*
	 * Stitch together inverse sine and cosine functions for the ranges [0,
	 * 0.5] and (0.5, 1].  Each expression below is guaranteed to return
	 * exactly 30 for x=0.5, so the result is a continuous monotonic function
	 * over the full range.
	 */
	if (x <= 0.5)
	{
		volatile float8 asin_x = asin(x);

		return (asin_x / asin_0_5) * 30.0;
	}
	else
	{
		volatile float8 acos_x = acos(x);

		return 90.0 - (acos_x / acos_0_5) * 60.0;
	}
}


/*
 *		acosd_q1		- returns the inverse cosine of x in degrees, for x in
 *						  the range [0, 1].  The result is an angle in the
 *						  first quadrant --- [0, 90] degrees.
 *
 *						  For the 3 special case inputs (0, 0.5 and 1), this
 *						  function will return exact values (0, 60 and 90
 *						  degrees respectively).
 */
static double
acosd_q1(double x)
{
	/*
	 * Stitch together inverse sine and cosine functions for the ranges [0,
	 * 0.5] and (0.5, 1].  Each expression below is guaranteed to return
	 * exactly 60 for x=0.5, so the result is a continuous monotonic function
	 * over the full range.
	 */
	if (x <= 0.5)
	{
		volatile float8 asin_x = asin(x);

		return 90.0 - (asin_x / asin_0_5) * 30.0;
	}
	else
	{
		volatile float8 acos_x = acos(x);

		return (acos_x / acos_0_5) * 60.0;
	}
}


/*
 *		dacosd			- returns the arccos of arg1 (degrees)
 */
Datum
dacosd(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		result;

	/* Per the POSIX spec, return NaN if the input is NaN */
	if (isnan(arg1))
		PG_RETURN_FLOAT8(get_float8_nan());

	INIT_DEGREE_CONSTANTS();

	/*
	 * The principal branch of the inverse cosine function maps values in the
	 * range [-1, 1] to values in the range [0, 180], so we should reject any
	 * inputs outside that range and the result will always be finite.
	 */
	if (arg1 < -1.0 || arg1 > 1.0)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("input is out of range")));

	if (arg1 >= 0.0)
		result = acosd_q1(arg1);
	else
		result = 90.0 + asind_q1(-arg1);

	if (unlikely(isinf(result)))
		float_overflow_error();

	PG_RETURN_FLOAT8(result);
}


/*
 *		dasind			- returns the arcsin of arg1 (degrees)
 */
Datum
dasind(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		result;

	/* Per the POSIX spec, return NaN if the input is NaN */
	if (isnan(arg1))
		PG_RETURN_FLOAT8(get_float8_nan());

	INIT_DEGREE_CONSTANTS();

	/*
	 * The principal branch of the inverse sine function maps values in the
	 * range [-1, 1] to values in the range [-90, 90], so we should reject any
	 * inputs outside that range and the result will always be finite.
	 */
	if (arg1 < -1.0 || arg1 > 1.0)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("input is out of range")));

	if (arg1 >= 0.0)
		result = asind_q1(arg1);
	else
		result = -asind_q1(-arg1);

	if (unlikely(isinf(result)))
		float_overflow_error();

	PG_RETURN_FLOAT8(result);
}


/*
 *		datand			- returns the arctan of arg1 (degrees)
 */
Datum
datand(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		result;
	volatile float8 atan_arg1;

	/* Per the POSIX spec, return NaN if the input is NaN */
	if (isnan(arg1))
		PG_RETURN_FLOAT8(get_float8_nan());

	INIT_DEGREE_CONSTANTS();

	/*
	 * The principal branch of the inverse tangent function maps all inputs to
	 * values in the range [-90, 90], so the result should always be finite,
	 * even if the input is infinite.  Additionally, we take care to ensure
	 * than when arg1 is 1, the result is exactly 45.
	 */
	atan_arg1 = atan(arg1);
	result = (atan_arg1 / atan_1_0) * 45.0;

	if (unlikely(isinf(result)))
		float_overflow_error();

	PG_RETURN_FLOAT8(result);
}


/*
 *		atan2d			- returns the arctan of arg1/arg2 (degrees)
 */
Datum
datan2d(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		arg2 = PG_GETARG_FLOAT8(1);
	float8		result;
	volatile float8 atan2_arg1_arg2;

	/* Per the POSIX spec, return NaN if either input is NaN */
	if (isnan(arg1) || isnan(arg2))
		PG_RETURN_FLOAT8(get_float8_nan());

	INIT_DEGREE_CONSTANTS();

	/*
	 * atan2d maps all inputs to values in the range [-180, 180], so the
	 * result should always be finite, even if the inputs are infinite.
	 *
	 * Note: this coding assumes that atan(1.0) is a suitable scaling constant
	 * to get an exact result from atan2().  This might well fail on us at
	 * some point, requiring us to decide exactly what inputs we think we're
	 * going to guarantee an exact result for.
	 */
	atan2_arg1_arg2 = atan2(arg1, arg2);
	result = (atan2_arg1_arg2 / atan_1_0) * 45.0;

	if (unlikely(isinf(result)))
		float_overflow_error();

	PG_RETURN_FLOAT8(result);
}


/*
 *		sind_0_to_30	- returns the sine of an angle that lies between 0 and
 *						  30 degrees.  This will return exactly 0 when x is 0,
 *						  and exactly 0.5 when x is 30 degrees.
 */
static double
sind_0_to_30(double x)
{
	volatile float8 sin_x = sin(x * RADIANS_PER_DEGREE);

	return (sin_x / sin_30) / 2.0;
}


/*
 *		cosd_0_to_60	- returns the cosine of an angle that lies between 0
 *						  and 60 degrees.  This will return exactly 1 when x
 *						  is 0, and exactly 0.5 when x is 60 degrees.
 */
static double
cosd_0_to_60(double x)
{
	volatile float8 one_minus_cos_x = 1.0 - cos(x * RADIANS_PER_DEGREE);

	return 1.0 - (one_minus_cos_x / one_minus_cos_60) / 2.0;
}


/*
 *		sind_q1			- returns the sine of an angle in the first quadrant
 *						  (0 to 90 degrees).
 */
static double
sind_q1(double x)
{
	/*
	 * Stitch together the sine and cosine functions for the ranges [0, 30]
	 * and (30, 90].  These guarantee to return exact answers at their
	 * endpoints, so the overall result is a continuous monotonic function
	 * that gives exact results when x = 0, 30 and 90 degrees.
	 */
	if (x <= 30.0)
		return sind_0_to_30(x);
	else
		return cosd_0_to_60(90.0 - x);
}


/*
 *		cosd_q1			- returns the cosine of an angle in the first quadrant
 *						  (0 to 90 degrees).
 */
static double
cosd_q1(double x)
{
	/*
	 * Stitch together the sine and cosine functions for the ranges [0, 60]
	 * and (60, 90].  These guarantee to return exact answers at their
	 * endpoints, so the overall result is a continuous monotonic function
	 * that gives exact results when x = 0, 60 and 90 degrees.
	 */
	if (x <= 60.0)
		return cosd_0_to_60(x);
	else
		return sind_0_to_30(90.0 - x);
}


/*
 *		dcosd			- returns the cosine of arg1 (degrees)
 */
Datum
dcosd(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		result;
	int			sign = 1;

	/*
	 * Per the POSIX spec, return NaN if the input is NaN and throw an error
	 * if the input is infinite.
	 */
	if (isnan(arg1))
		PG_RETURN_FLOAT8(get_float8_nan());

	if (isinf(arg1))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("input is out of range")));

	INIT_DEGREE_CONSTANTS();

	/* Reduce the range of the input to [0,90] degrees */
	arg1 = fmod(arg1, 360.0);

	if (arg1 < 0.0)
	{
		/* cosd(-x) = cosd(x) */
		arg1 = -arg1;
	}

	if (arg1 > 180.0)
	{
		/* cosd(360-x) = cosd(x) */
		arg1 = 360.0 - arg1;
	}

	if (arg1 > 90.0)
	{
		/* cosd(180-x) = -cosd(x) */
		arg1 = 180.0 - arg1;
		sign = -sign;
	}

	result = sign * cosd_q1(arg1);

	if (unlikely(isinf(result)))
		float_overflow_error();

	PG_RETURN_FLOAT8(result);
}


/*
 *		dcotd			- returns the cotangent of arg1 (degrees)
 */
Datum
dcotd(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		result;
	volatile float8 cot_arg1;
	int			sign = 1;

	/*
	 * Per the POSIX spec, return NaN if the input is NaN and throw an error
	 * if the input is infinite.
	 */
	if (isnan(arg1))
		PG_RETURN_FLOAT8(get_float8_nan());

	if (isinf(arg1))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("input is out of range")));

	INIT_DEGREE_CONSTANTS();

	/* Reduce the range of the input to [0,90] degrees */
	arg1 = fmod(arg1, 360.0);

	if (arg1 < 0.0)
	{
		/* cotd(-x) = -cotd(x) */
		arg1 = -arg1;
		sign = -sign;
	}

	if (arg1 > 180.0)
	{
		/* cotd(360-x) = -cotd(x) */
		arg1 = 360.0 - arg1;
		sign = -sign;
	}

	if (arg1 > 90.0)
	{
		/* cotd(180-x) = -cotd(x) */
		arg1 = 180.0 - arg1;
		sign = -sign;
	}

	cot_arg1 = cosd_q1(arg1) / sind_q1(arg1);
	result = sign * (cot_arg1 / cot_45);

	/*
	 * On some machines we get cotd(270) = minus zero, but this isn't always
	 * true.  For portability, and because the user constituency for this
	 * function probably doesn't want minus zero, force it to plain zero.
	 */
	if (result == 0.0)
		result = 0.0;

	/* Not checking for overflow because cotd(0) == Inf */

	PG_RETURN_FLOAT8(result);
}


/*
 *		dsind			- returns the sine of arg1 (degrees)
 */
Datum
dsind(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		result;
	int			sign = 1;

	/*
	 * Per the POSIX spec, return NaN if the input is NaN and throw an error
	 * if the input is infinite.
	 */
	if (isnan(arg1))
		PG_RETURN_FLOAT8(get_float8_nan());

	if (isinf(arg1))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("input is out of range")));

	INIT_DEGREE_CONSTANTS();

	/* Reduce the range of the input to [0,90] degrees */
	arg1 = fmod(arg1, 360.0);

	if (arg1 < 0.0)
	{
		/* sind(-x) = -sind(x) */
		arg1 = -arg1;
		sign = -sign;
	}

	if (arg1 > 180.0)
	{
		/* sind(360-x) = -sind(x) */
		arg1 = 360.0 - arg1;
		sign = -sign;
	}

	if (arg1 > 90.0)
	{
		/* sind(180-x) = sind(x) */
		arg1 = 180.0 - arg1;
	}

	result = sign * sind_q1(arg1);

	if (unlikely(isinf(result)))
		float_overflow_error();

	PG_RETURN_FLOAT8(result);
}


/*
 *		dtand			- returns the tangent of arg1 (degrees)
 */
Datum
dtand(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		result;
	volatile float8 tan_arg1;
	int			sign = 1;

	/*
	 * Per the POSIX spec, return NaN if the input is NaN and throw an error
	 * if the input is infinite.
	 */
	if (isnan(arg1))
		PG_RETURN_FLOAT8(get_float8_nan());

	if (isinf(arg1))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("input is out of range")));

	INIT_DEGREE_CONSTANTS();

	/* Reduce the range of the input to [0,90] degrees */
	arg1 = fmod(arg1, 360.0);

	if (arg1 < 0.0)
	{
		/* tand(-x) = -tand(x) */
		arg1 = -arg1;
		sign = -sign;
	}

	if (arg1 > 180.0)
	{
		/* tand(360-x) = -tand(x) */
		arg1 = 360.0 - arg1;
		sign = -sign;
	}

	if (arg1 > 90.0)
	{
		/* tand(180-x) = -tand(x) */
		arg1 = 180.0 - arg1;
		sign = -sign;
	}

	tan_arg1 = sind_q1(arg1) / cosd_q1(arg1);
	result = sign * (tan_arg1 / tan_45);

	/*
	 * On some machines we get tand(180) = minus zero, but this isn't always
	 * true.  For portability, and because the user constituency for this
	 * function probably doesn't want minus zero, force it to plain zero.
	 */
	if (result == 0.0)
		result = 0.0;

	/* Not checking for overflow because tand(90) == Inf */

	PG_RETURN_FLOAT8(result);
}

/* ==== src/backend/utils/adt/float.c lines 2591-2874 — VERBATIM ==== */
/* ========== HYPERBOLIC FUNCTIONS ========== */


/*
 *		dsinh			- returns the hyperbolic sine of arg1
 */
Datum
dsinh(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		result;

	errno = 0;
	result = sinh(arg1);

	/*
	 * if an ERANGE error occurs, it means there is an overflow.  For sinh,
	 * the result should be either -infinity or infinity, depending on the
	 * sign of arg1.
	 */
	if (errno == ERANGE)
	{
		if (arg1 < 0)
			result = -get_float8_infinity();
		else
			result = get_float8_infinity();
	}

	PG_RETURN_FLOAT8(result);
}


/*
 *		dcosh			- returns the hyperbolic cosine of arg1
 */
Datum
dcosh(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		result;

	errno = 0;
	result = cosh(arg1);

	/*
	 * if an ERANGE error occurs, it means there is an overflow.  As cosh is
	 * always positive, it always means the result is positive infinity.
	 */
	if (errno == ERANGE)
		result = get_float8_infinity();

	if (unlikely(result == 0.0))
		float_underflow_error();

	PG_RETURN_FLOAT8(result);
}

/*
 *		dtanh			- returns the hyperbolic tangent of arg1
 */
Datum
dtanh(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		result;

	/*
	 * For tanh, we don't need an errno check because it never overflows.
	 */
	result = tanh(arg1);

	if (unlikely(isinf(result)))
		float_overflow_error();

	PG_RETURN_FLOAT8(result);
}

/*
 *		dasinh			- returns the inverse hyperbolic sine of arg1
 */
Datum
dasinh(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		result;

	/*
	 * For asinh, we don't need an errno check because it never overflows.
	 */
	result = asinh(arg1);

	PG_RETURN_FLOAT8(result);
}

/*
 *		dacosh			- returns the inverse hyperbolic cosine of arg1
 */
Datum
dacosh(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		result;

	/*
	 * acosh is only defined for inputs >= 1.0.  By checking this ourselves,
	 * we need not worry about checking for an EDOM error, which is a good
	 * thing because some implementations will report that for NaN. Otherwise,
	 * no error is possible.
	 */
	if (arg1 < 1.0)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("input is out of range")));

	result = acosh(arg1);

	PG_RETURN_FLOAT8(result);
}

/*
 *		datanh			- returns the inverse hyperbolic tangent of arg1
 */
Datum
datanh(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		result;

	/*
	 * atanh is only defined for inputs between -1 and 1.  By checking this
	 * ourselves, we need not worry about checking for an EDOM error, which is
	 * a good thing because some implementations will report that for NaN.
	 */
	if (arg1 < -1.0 || arg1 > 1.0)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("input is out of range")));

	/*
	 * Also handle the infinity cases ourselves; this is helpful because old
	 * glibc versions may produce the wrong errno for this.  All other inputs
	 * cannot produce an error.
	 */
	if (arg1 == -1.0)
		result = -get_float8_infinity();
	else if (arg1 == 1.0)
		result = get_float8_infinity();
	else
		result = atanh(arg1);

	PG_RETURN_FLOAT8(result);
}


/* ========== ERROR FUNCTIONS ========== */


/*
 *		derf			- returns the error function: erf(arg1)
 */
Datum
derf(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		result;

	/*
	 * For erf, we don't need an errno check because it never overflows.
	 */
	result = erf(arg1);

	if (unlikely(isinf(result)))
		float_overflow_error();

	PG_RETURN_FLOAT8(result);
}

/*
 *		derfc			- returns the complementary error function: 1 - erf(arg1)
 */
Datum
derfc(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		result;

	/*
	 * For erfc, we don't need an errno check because it never overflows.
	 */
	result = erfc(arg1);

	if (unlikely(isinf(result)))
		float_overflow_error();

	PG_RETURN_FLOAT8(result);
}


/* ========== GAMMA FUNCTIONS ========== */


/*
 *		dgamma			- returns the gamma function of arg1
 */
Datum
dgamma(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		result;

	/*
	 * Handle NaN and Inf cases explicitly.  This simplifies the overflow
	 * checks on platforms that do not set errno.
	 */
	if (isnan(arg1))
		result = arg1;
	else if (isinf(arg1))
	{
		/* Per POSIX, an input of -Inf causes a domain error */
		if (arg1 < 0)
		{
			float_overflow_error();
			result = get_float8_nan();	/* keep compiler quiet */
		}
		else
			result = arg1;
	}
	else
	{
		/*
		 * Note: the POSIX/C99 gamma function is called "tgamma", not "gamma".
		 *
		 * On some platforms, tgamma() will not set errno but just return Inf,
		 * NaN, or zero to report overflow/underflow; therefore, test those
		 * cases explicitly (note that, like the exponential function, the
		 * gamma function has no zeros).
		 */
		errno = 0;
		result = tgamma(arg1);

		if (errno != 0 || isinf(result) || isnan(result))
		{
			if (result != 0.0)
				float_overflow_error();
			else
				float_underflow_error();
		}
		else if (result == 0.0)
			float_underflow_error();
	}

	PG_RETURN_FLOAT8(result);
}


/*
 *		dlgamma			- natural logarithm of absolute value of gamma of arg1
 */
Datum
dlgamma(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		result;

	/*
	 * Note: lgamma may not be thread-safe because it may write to a global
	 * variable signgam, which may not be thread-local. However, this doesn't
	 * matter to us, since we don't use signgam.
	 */
	errno = 0;
	result = lgamma(arg1);

	/*
	 * If an ERANGE error occurs, it means there was an overflow or a pole
	 * error (which happens for zero and negative integer inputs).
	 *
	 * On some platforms, lgamma() will not set errno but just return infinity
	 * to report overflow, but it should never underflow.
	 */
	if (errno == ERANGE || (isinf(result) && !isinf(arg1)))
		float_overflow_error();

	PG_RETURN_FLOAT8(result);
}

/* ==== src/backend/utils/adt/float.c lines 1364-1463 — VERBATIM
 * (p1-lanead: the "RANDOM FLOAT8 OPERATORS" rounding/sqrt family) ==== */

/*
 *		dround			- returns	ROUND(arg1)
 */
Datum
dround(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);

	PG_RETURN_FLOAT8(rint(arg1));
}

/*
 *		dceil			- returns the smallest integer greater than or
 *						  equal to the specified float
 */
Datum
dceil(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);

	PG_RETURN_FLOAT8(ceil(arg1));
}

/*
 *		dfloor			- returns the largest integer lesser than or
 *						  equal to the specified float
 */
Datum
dfloor(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);

	PG_RETURN_FLOAT8(floor(arg1));
}

/*
 *		dsign			- returns -1 if the argument is less than 0, 0
 *						  if the argument is equal to 0, and 1 if the
 *						  argument is greater than zero.
 */
Datum
dsign(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		result;

	if (arg1 > 0)
		result = 1.0;
	else if (arg1 < 0)
		result = -1.0;
	else
		result = 0.0;

	PG_RETURN_FLOAT8(result);
}

/*
 *		dtrunc			- returns truncation-towards-zero of arg1,
 *						  arg1 >= 0 ... the greatest integer less
 *										than or equal to arg1
 *						  arg1 < 0	... the least integer greater
 *										than or equal to arg1
 */
Datum
dtrunc(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		result;

	if (arg1 >= 0)
		result = floor(arg1);
	else
		result = -floor(-arg1);

	PG_RETURN_FLOAT8(result);
}


/*
 *		dsqrt			- returns square root of arg1
 */
Datum
dsqrt(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		result;

	if (arg1 < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION),
				 errmsg("cannot take square root of a negative number")));

	result = sqrt(arg1);
	if (unlikely(isinf(result)) && !isinf(arg1))
		float_overflow_error();
	if (unlikely(result == 0.0) && arg1 != 0.0)
		float_underflow_error();

	PG_RETURN_FLOAT8(result);
}

/* ==== src/backend/utils/adt/float.c lines 2556-2589 — VERBATIM
 * (p1-lanead: degrees/radians, previously elided; dpi is a bare constant
 * and stays elided) ==== */

/*
 *		degrees		- returns degrees converted from radians
 */
Datum
degrees(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);

	PG_RETURN_FLOAT8(float8_div(arg1, RADIANS_PER_DEGREE));
}


/*
 *		radians		- returns radians converted from degrees
 */
Datum
radians(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);

	PG_RETURN_FLOAT8(float8_mul(arg1, RADIANS_PER_DEGREE));
}

/* ---- fuzz-facing entry point (driver, NOT Postgres code) ---- */

/*
 * Function-id table shared with the Rust comparator (core/src/diff.rs).
 * ORDER IS LOAD-BEARING: ids 0..27 are the unary functions in alphabetical
 * order; 28..30 are the two-argument functions.  Keep in exact sync with
 * diff.rs FLOAT_MATH1_NAMES / FLOAT_MATH2_NAMES.
 */
typedef Datum (*pg_diff_fmath_fn) (pg_diff_fcargs *);

static const pg_diff_fmath_fn pg_diff_fmath_table[] = {
	/* 0 */ dacos,
	/* 1 */ dacosd,
	/* 2 */ dacosh,
	/* 3 */ dasin,
	/* 4 */ dasind,
	/* 5 */ dasinh,
	/* 6 */ datan,
	/* 7 */ datand,
	/* 8 */ datanh,
	/* 9 */ dcbrt,
	/* 10 */ dcos,
	/* 11 */ dcosd,
	/* 12 */ dcosh,
	/* 13 */ dcot,
	/* 14 */ dcotd,
	/* 15 */ derf,
	/* 16 */ derfc,
	/* 17 */ dexp,
	/* 18 */ dgamma,
	/* 19 */ dlgamma,
	/* 20 */ dlog1,
	/* 21 */ dlog10,
	/* 22 */ dsin,
	/* 23 */ dsind,
	/* 24 */ dsinh,
	/* 25 */ dtan,
	/* 26 */ dtand,
	/* 27 */ dtanh,
	/* 28 */ datan2,
	/* 29 */ datan2d,
	/* 30 */ dpow,
	/* p1-lanead appended block — ids 31.. are the rounding/sqrt/deg family;
	 * APPEND-ONLY: never renumber 0..30 (corpus selectors depend on them). */
	/* 31 */ degrees,
	/* 32 */ radians,
	/* 33 */ dsqrt,
	/* 34 */ dsign,
	/* 35 */ dtrunc,
	/* 36 */ dround,
	/* 37 */ dceil,
	/* 38 */ dfloor,
};

/*
 * Run vendored function fn_id on (a[, b]).  Returns 0 and writes *out on
 * success; returns the shimmed errcode class (2/3/4, see header) if the
 * vendored body raised via ereport(ERROR)/float_overflow_error/etc.
 */
int
pg_diff_float_math(int fn_id, double a, double b, double *out)
{
	pg_diff_fcargs fc;

	if (fn_id < 0 ||
		fn_id >= (int) (sizeof(pg_diff_fmath_table) / sizeof(pg_diff_fmath_table[0])))
		return -1;
	fc.arg[0] = a;
	fc.arg[1] = b;
	pg_diff_fmath_errcode = 0;
	if (setjmp(pg_diff_fmath_jmp))
		return pg_diff_fmath_errcode;
	*out = pg_diff_fmath_table[fn_id](&fc);
	return 0;
}
