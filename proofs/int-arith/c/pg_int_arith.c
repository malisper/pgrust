/*
 * Vendored PostgreSQL C for the integer arithmetic proof family.
 *
 * Provenance:
 *   - src/backend/utils/adt/int.c   (int2/int4/int24/int42 um, up, pl, mi,
 *                                    mul, div, mod, abs, larger, smaller;
 *                                    i2toi4, i4toi2, int4inc;
 *                                    int2/int4 and/or/xor/not/shl/shr;
 *                                    in_range_int4_int4/int4_int2/int4_int8,
 *                                    in_range_int2_int4/int2_int2/int2_int8)
 *   - src/backend/utils/adt/int8.c  (int8/int84/int48/int82/int28 um, up,
 *                                    pl, mi, mul, div, mod, abs, larger,
 *                                    smaller; int8inc, int8dec, int8inc_any,
 *                                    int8dec_any; int8 and/or/xor/not/shl/shr;
 *                                    int48, int84, int28, int82, i8tooid,
 *                                    oidtoi8; in_range_int8_int8)
 *   - src/include/common/int.h     (pg_add/sub/mul_s16/s32/s64_overflow)
 *   ref: postgres/postgres REL_18_STABLE
 *        @ 277122036c3382c5ab47034a180fde1176728c43
 *   fetched: 2026-07-28 (wave-3 sections re-fetched same day, same ref)
 *
 * Shims (plumbing only, never logic):
 *   - `pg_` prefix on every function name.
 *   - fmgr PG_FUNCTION_ARGS unwrapping -> plain C signatures:
 *       PG_GETARG_INT16/32/64 -> direct int16/int32/int64 params;
 *       PG_RETURN_INT16/32/64 -> direct return (infallible functions) or
 *       out-param + status return (fallible functions, next shim).
 *   - ereport(ERROR, ...) -> the fallible wrappers return a status int
 *     instead of raising:
 *         0 = success (*result set)
 *         1 = ereport(ERROR, errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE))
 *             ["integer/smallint/bigint out of range"]
 *         2 = ereport(ERROR, errcode(ERRCODE_DIVISION_BY_ZERO))
 *             ["division by zero"]
 *     The overflow/zero TESTS and every on-success value are verbatim.
 *     `PG_RETURN_NULL()` after the div-by-zero ereport is unreachable
 *     (ereport(ERROR) does not return); the status return models it.
 *   - pg_add/sub/mul_sNN_overflow: common/int.h's HAVE__BUILTIN_OP_OVERFLOW
 *     arm (__builtin_{add,sub,mul}_overflow), the arm every production
 *     compiler takes; CBMC models these builtins.
 *   - typedefs/macros normally supplied by headers: int16/int32/int64,
 *     bool, unlikely, PG_INT16_MIN/PG_INT32_MIN/PG_INT64_MIN.
 *
 * Function bodies between the arg-fetch lines and the returns are verbatim.
 * Postgres compiles with -fwrapv; CBMC's two's-complement wrap matches (no
 * signed overflow is reachable anyway: everything goes through the overflow
 * builtins or is branch-fenced).
 *
 * Wave-3 shims (2026-07-28), on top of the above:
 *   - in_range_*: ereport(ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE)
 *     -> status 3 (PG_ERR_INVALID_PRECEDING, sqlstate 22013); bool result
 *     via out-param.  The delegating variants (in_range_int4_int2,
 *     in_range_int2_int2, in_range_int2_int8) keep C's DirectFunctionCall5
 *     delegation as a plain C call with the same argument widening.
 *   - int8inc/int8dec: this build has USE_FLOAT8_BYVAL (int8 pass-by-value),
 *     so C's #ifndef USE_FLOAT8_BYVAL agg modify-in-place branch is compiled
 *     out; only the value branch is vendored (as compiled in production).
 *     int8inc_any/int8dec_any keep C's `return int8inc(fcinfo)` delegation.
 *   - i8tooid: PG_RETURN_OID -> uint32 out-param.
 *   - int2/int4/int8 shl/shr are vendored VERBATIM (`arg1 << arg2`), which
 *     is C-side UB for out-of-range counts: harnesses fence the count to
 *     the defined domain.  The pg_*_shl/shr_model functions below are NOT
 *     vendored C — they are the RATIFIED PLATFORM MODEL (masked count,
 *     TRIAGE "INT SHIFT UB PLANE" pre-ruling; ground-truthed on real PG
 *     18.4 ARM64) that the out-of-range arm is proved against.
 */

#include <stdint.h>
#include <stdbool.h>

typedef int16_t int16;
typedef int32_t int32;
typedef int64_t int64;
typedef uint16_t uint16;
typedef uint32_t uint32;
typedef uint64_t uint64;
typedef uint32 Oid;

#define unlikely(x) (x)
#define PG_INT16_MIN INT16_MIN
#define PG_INT16_MAX INT16_MAX
#define PG_INT32_MIN INT32_MIN
#define PG_INT32_MAX INT32_MAX
#define PG_INT64_MIN INT64_MIN
#define PG_UINT32_MAX UINT32_MAX
#define SHRT_MIN (-32768)
#define SHRT_MAX 32767

/* ereport shim status codes */
#define PG_OK 0
#define PG_ERR_OUT_OF_RANGE 1  /* ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE (22003) */
#define PG_ERR_DIV_BY_ZERO 2   /* ERRCODE_DIVISION_BY_ZERO (22012) */
#define PG_ERR_INVALID_PRECEDING 3 /* ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE (22013) */

/* ---------- common/int.h: HAVE__BUILTIN_OP_OVERFLOW arms ---------- */

static inline bool
pg_add_s16_overflow(int16 a, int16 b, int16 *result)
{
	return __builtin_add_overflow(a, b, result);
}

static inline bool
pg_sub_s16_overflow(int16 a, int16 b, int16 *result)
{
	return __builtin_sub_overflow(a, b, result);
}

static inline bool
pg_mul_s16_overflow(int16 a, int16 b, int16 *result)
{
	return __builtin_mul_overflow(a, b, result);
}

static inline bool
pg_add_s32_overflow(int32 a, int32 b, int32 *result)
{
	return __builtin_add_overflow(a, b, result);
}

static inline bool
pg_sub_s32_overflow(int32 a, int32 b, int32 *result)
{
	return __builtin_sub_overflow(a, b, result);
}

static inline bool
pg_mul_s32_overflow(int32 a, int32 b, int32 *result)
{
	return __builtin_mul_overflow(a, b, result);
}

static inline bool
pg_add_s64_overflow(int64 a, int64 b, int64 *result)
{
	return __builtin_add_overflow(a, b, result);
}

static inline bool
pg_sub_s64_overflow(int64 a, int64 b, int64 *result)
{
	return __builtin_sub_overflow(a, b, result);
}

static inline bool
pg_mul_s64_overflow(int64 a, int64 b, int64 *result)
{
	return __builtin_mul_overflow(a, b, result);
}

/* ================= int.c: int4 unary / arithmetic ================= */

int
pg_int4um(int32 arg, int32 *result)
{
	if (unlikely(arg == PG_INT32_MIN))
		return PG_ERR_OUT_OF_RANGE;
	*result = -arg;
	return PG_OK;
}

int32
pg_int4up(int32 arg)
{
	return arg;
}

int
pg_int4pl(int32 arg1, int32 arg2, int32 *resp)
{
	int32		result;

	if (unlikely(pg_add_s32_overflow(arg1, arg2, &result)))
		return PG_ERR_OUT_OF_RANGE;
	*resp = result;
	return PG_OK;
}

int
pg_int4mi(int32 arg1, int32 arg2, int32 *resp)
{
	int32		result;

	if (unlikely(pg_sub_s32_overflow(arg1, arg2, &result)))
		return PG_ERR_OUT_OF_RANGE;
	*resp = result;
	return PG_OK;
}

int
pg_int4mul(int32 arg1, int32 arg2, int32 *resp)
{
	int32		result;

	if (unlikely(pg_mul_s32_overflow(arg1, arg2, &result)))
		return PG_ERR_OUT_OF_RANGE;
	*resp = result;
	return PG_OK;
}

int
pg_int4div(int32 arg1, int32 arg2, int32 *resp)
{
	int32		result;

	if (arg2 == 0)
		return PG_ERR_DIV_BY_ZERO;

	/*
	 * INT_MIN / -1 is problematic, since the result can't be represented on a
	 * two's-complement machine.  Some machines produce INT_MIN, some produce
	 * zero, some throw an exception.  We can dodge the problem by recognizing
	 * that division by -1 is the same as negation.
	 */
	if (arg2 == -1)
	{
		if (unlikely(arg1 == PG_INT32_MIN))
			return PG_ERR_OUT_OF_RANGE;
		result = -arg1;
		*resp = result;
		return PG_OK;
	}

	/* No overflow is possible */

	result = arg1 / arg2;

	*resp = result;
	return PG_OK;
}

/* ================= int.c: int2 unary / arithmetic ================= */

int
pg_int2um(int16 arg, int16 *result)
{
	if (unlikely(arg == PG_INT16_MIN))
		return PG_ERR_OUT_OF_RANGE;
	*result = -arg;
	return PG_OK;
}

int16
pg_int2up(int16 arg)
{
	return arg;
}

int
pg_int2pl(int16 arg1, int16 arg2, int16 *resp)
{
	int16		result;

	if (unlikely(pg_add_s16_overflow(arg1, arg2, &result)))
		return PG_ERR_OUT_OF_RANGE;
	*resp = result;
	return PG_OK;
}

int
pg_int2mi(int16 arg1, int16 arg2, int16 *resp)
{
	int16		result;

	if (unlikely(pg_sub_s16_overflow(arg1, arg2, &result)))
		return PG_ERR_OUT_OF_RANGE;
	*resp = result;
	return PG_OK;
}

int
pg_int2mul(int16 arg1, int16 arg2, int16 *resp)
{
	int16		result;

	if (unlikely(pg_mul_s16_overflow(arg1, arg2, &result)))
		return PG_ERR_OUT_OF_RANGE;
	*resp = result;
	return PG_OK;
}

int
pg_int2div(int16 arg1, int16 arg2, int16 *resp)
{
	int16		result;

	if (arg2 == 0)
		return PG_ERR_DIV_BY_ZERO;

	/*
	 * SHRT_MIN / -1 is problematic, since the result can't be represented on
	 * a two's-complement machine.  Some machines produce SHRT_MIN, some
	 * produce zero, some throw an exception.  We can dodge the problem by
	 * recognizing that division by -1 is the same as negation.
	 */
	if (arg2 == -1)
	{
		if (unlikely(arg1 == PG_INT16_MIN))
			return PG_ERR_OUT_OF_RANGE;
		result = -arg1;
		*resp = result;
		return PG_OK;
	}

	/* No overflow is possible */

	result = arg1 / arg2;

	*resp = result;
	return PG_OK;
}

/* ================= int.c: int24 / int42 mixed width ================= */

int
pg_int24pl(int16 arg1, int32 arg2, int32 *resp)
{
	int32		result;

	if (unlikely(pg_add_s32_overflow((int32) arg1, arg2, &result)))
		return PG_ERR_OUT_OF_RANGE;
	*resp = result;
	return PG_OK;
}

int
pg_int24mi(int16 arg1, int32 arg2, int32 *resp)
{
	int32		result;

	if (unlikely(pg_sub_s32_overflow((int32) arg1, arg2, &result)))
		return PG_ERR_OUT_OF_RANGE;
	*resp = result;
	return PG_OK;
}

int
pg_int24mul(int16 arg1, int32 arg2, int32 *resp)
{
	int32		result;

	if (unlikely(pg_mul_s32_overflow((int32) arg1, arg2, &result)))
		return PG_ERR_OUT_OF_RANGE;
	*resp = result;
	return PG_OK;
}

int
pg_int24div(int16 arg1, int32 arg2, int32 *resp)
{
	if (unlikely(arg2 == 0))
		return PG_ERR_DIV_BY_ZERO;

	/* No overflow is possible */
	*resp = (int32) arg1 / arg2;
	return PG_OK;
}

int
pg_int42pl(int32 arg1, int16 arg2, int32 *resp)
{
	int32		result;

	if (unlikely(pg_add_s32_overflow(arg1, (int32) arg2, &result)))
		return PG_ERR_OUT_OF_RANGE;
	*resp = result;
	return PG_OK;
}

int
pg_int42mi(int32 arg1, int16 arg2, int32 *resp)
{
	int32		result;

	if (unlikely(pg_sub_s32_overflow(arg1, (int32) arg2, &result)))
		return PG_ERR_OUT_OF_RANGE;
	*resp = result;
	return PG_OK;
}

int
pg_int42mul(int32 arg1, int16 arg2, int32 *resp)
{
	int32		result;

	if (unlikely(pg_mul_s32_overflow(arg1, (int32) arg2, &result)))
		return PG_ERR_OUT_OF_RANGE;
	*resp = result;
	return PG_OK;
}

int
pg_int42div(int32 arg1, int16 arg2, int32 *resp)
{
	int32		result;

	if (unlikely(arg2 == 0))
		return PG_ERR_DIV_BY_ZERO;

	/*
	 * INT_MIN / -1 is problematic, since the result can't be represented on a
	 * two's-complement machine.  Some machines produce INT_MIN, some produce
	 * zero, some throw an exception.  We can dodge the problem by recognizing
	 * that division by -1 is the same as negation.
	 */
	if (arg2 == -1)
	{
		if (unlikely(arg1 == PG_INT32_MIN))
			return PG_ERR_OUT_OF_RANGE;
		result = -arg1;
		*resp = result;
		return PG_OK;
	}

	/* No overflow is possible */

	result = arg1 / arg2;

	*resp = result;
	return PG_OK;
}

/* ================= int.c: mod / abs / larger / smaller ================= */

int
pg_int4mod(int32 arg1, int32 arg2, int32 *resp)
{
	if (unlikely(arg2 == 0))
		return PG_ERR_DIV_BY_ZERO;

	/*
	 * Some machines throw a floating-point exception for INT_MIN % -1, which
	 * is a bit silly since the correct answer is perfectly well-defined,
	 * namely zero.
	 */
	if (arg2 == -1)
	{
		*resp = 0;
		return PG_OK;
	}

	/* No overflow is possible */

	*resp = arg1 % arg2;
	return PG_OK;
}

int
pg_int2mod(int16 arg1, int16 arg2, int16 *resp)
{
	if (unlikely(arg2 == 0))
		return PG_ERR_DIV_BY_ZERO;

	/*
	 * Some machines throw a floating-point exception for INT_MIN % -1, which
	 * is a bit silly since the correct answer is perfectly well-defined,
	 * namely zero.  (It's not clear this ever happens when dealing with
	 * int16, but we might as well have the test for safety.)
	 */
	if (arg2 == -1)
	{
		*resp = 0;
		return PG_OK;
	}

	/* No overflow is possible */

	*resp = arg1 % arg2;
	return PG_OK;
}

int
pg_int4abs(int32 arg1, int32 *resp)
{
	int32		result;

	if (unlikely(arg1 == PG_INT32_MIN))
		return PG_ERR_OUT_OF_RANGE;
	result = (arg1 < 0) ? -arg1 : arg1;
	*resp = result;
	return PG_OK;
}

int
pg_int2abs(int16 arg1, int16 *resp)
{
	int16		result;

	if (unlikely(arg1 == PG_INT16_MIN))
		return PG_ERR_OUT_OF_RANGE;
	result = (arg1 < 0) ? -arg1 : arg1;
	*resp = result;
	return PG_OK;
}

int16
pg_int2larger(int16 arg1, int16 arg2)
{
	return (arg1 > arg2) ? arg1 : arg2;
}

int16
pg_int2smaller(int16 arg1, int16 arg2)
{
	return (arg1 < arg2) ? arg1 : arg2;
}

int32
pg_int4larger(int32 arg1, int32 arg2)
{
	return (arg1 > arg2) ? arg1 : arg2;
}

int32
pg_int4smaller(int32 arg1, int32 arg2)
{
	return (arg1 < arg2) ? arg1 : arg2;
}

/* ================= int8.c: int8 unary / arithmetic ================= */

int
pg_int8um(int64 arg, int64 *resp)
{
	int64		result;

	if (unlikely(arg == PG_INT64_MIN))
		return PG_ERR_OUT_OF_RANGE;
	result = -arg;
	*resp = result;
	return PG_OK;
}

int64
pg_int8up(int64 arg)
{
	return arg;
}

int
pg_int8pl(int64 arg1, int64 arg2, int64 *resp)
{
	int64		result;

	if (unlikely(pg_add_s64_overflow(arg1, arg2, &result)))
		return PG_ERR_OUT_OF_RANGE;
	*resp = result;
	return PG_OK;
}

int
pg_int8mi(int64 arg1, int64 arg2, int64 *resp)
{
	int64		result;

	if (unlikely(pg_sub_s64_overflow(arg1, arg2, &result)))
		return PG_ERR_OUT_OF_RANGE;
	*resp = result;
	return PG_OK;
}

int
pg_int8mul(int64 arg1, int64 arg2, int64 *resp)
{
	int64		result;

	if (unlikely(pg_mul_s64_overflow(arg1, arg2, &result)))
		return PG_ERR_OUT_OF_RANGE;
	*resp = result;
	return PG_OK;
}

int
pg_int8div(int64 arg1, int64 arg2, int64 *resp)
{
	int64		result;

	if (arg2 == 0)
		return PG_ERR_DIV_BY_ZERO;

	/*
	 * INT64_MIN / -1 is problematic, since the result can't be represented on
	 * a two's-complement machine.  Some machines produce INT64_MIN, some
	 * produce zero, some throw an exception.  We can dodge the problem by
	 * recognizing that division by -1 is the same as negation.
	 */
	if (arg2 == -1)
	{
		if (unlikely(arg1 == PG_INT64_MIN))
			return PG_ERR_OUT_OF_RANGE;
		result = -arg1;
		*resp = result;
		return PG_OK;
	}

	/* No overflow is possible */

	result = arg1 / arg2;

	*resp = result;
	return PG_OK;
}

int
pg_int8abs(int64 arg1, int64 *resp)
{
	int64		result;

	if (unlikely(arg1 == PG_INT64_MIN))
		return PG_ERR_OUT_OF_RANGE;
	result = (arg1 < 0) ? -arg1 : arg1;
	*resp = result;
	return PG_OK;
}

int
pg_int8mod(int64 arg1, int64 arg2, int64 *resp)
{
	if (unlikely(arg2 == 0))
		return PG_ERR_DIV_BY_ZERO;

	/*
	 * Some machines throw a floating-point exception for INT64_MIN % -1,
	 * which is a bit silly since the correct answer is perfectly
	 * well-defined, namely zero.
	 */
	if (arg2 == -1)
	{
		*resp = 0;
		return PG_OK;
	}

	/* No overflow is possible */

	*resp = arg1 % arg2;
	return PG_OK;
}

int64
pg_int8larger(int64 arg1, int64 arg2)
{
	int64		result;

	result = ((arg1 > arg2) ? arg1 : arg2);

	return result;
}

int64
pg_int8smaller(int64 arg1, int64 arg2)
{
	int64		result;

	result = ((arg1 < arg2) ? arg1 : arg2);

	return result;
}

/* ================= int8.c: int84 / int48 / int82 / int28 ================= */

int
pg_int84pl(int64 arg1, int32 arg2, int64 *resp)
{
	int64		result;

	if (unlikely(pg_add_s64_overflow(arg1, (int64) arg2, &result)))
		return PG_ERR_OUT_OF_RANGE;
	*resp = result;
	return PG_OK;
}

int
pg_int84mi(int64 arg1, int32 arg2, int64 *resp)
{
	int64		result;

	if (unlikely(pg_sub_s64_overflow(arg1, (int64) arg2, &result)))
		return PG_ERR_OUT_OF_RANGE;
	*resp = result;
	return PG_OK;
}

int
pg_int84mul(int64 arg1, int32 arg2, int64 *resp)
{
	int64		result;

	if (unlikely(pg_mul_s64_overflow(arg1, (int64) arg2, &result)))
		return PG_ERR_OUT_OF_RANGE;
	*resp = result;
	return PG_OK;
}

int
pg_int84div(int64 arg1, int32 arg2, int64 *resp)
{
	int64		result;

	if (arg2 == 0)
		return PG_ERR_DIV_BY_ZERO;

	/*
	 * INT64_MIN / -1 is problematic, since the result can't be represented on
	 * a two's-complement machine.  Some machines produce INT64_MIN, some
	 * produce zero, some throw an exception.  We can dodge the problem by
	 * recognizing that division by -1 is the same as negation.
	 */
	if (arg2 == -1)
	{
		if (unlikely(arg1 == PG_INT64_MIN))
			return PG_ERR_OUT_OF_RANGE;
		result = -arg1;
		*resp = result;
		return PG_OK;
	}

	/* No overflow is possible */

	result = arg1 / arg2;

	*resp = result;
	return PG_OK;
}

int
pg_int48pl(int32 arg1, int64 arg2, int64 *resp)
{
	int64		result;

	if (unlikely(pg_add_s64_overflow((int64) arg1, arg2, &result)))
		return PG_ERR_OUT_OF_RANGE;
	*resp = result;
	return PG_OK;
}

int
pg_int48mi(int32 arg1, int64 arg2, int64 *resp)
{
	int64		result;

	if (unlikely(pg_sub_s64_overflow((int64) arg1, arg2, &result)))
		return PG_ERR_OUT_OF_RANGE;
	*resp = result;
	return PG_OK;
}

int
pg_int48mul(int32 arg1, int64 arg2, int64 *resp)
{
	int64		result;

	if (unlikely(pg_mul_s64_overflow((int64) arg1, arg2, &result)))
		return PG_ERR_OUT_OF_RANGE;
	*resp = result;
	return PG_OK;
}

int
pg_int48div(int32 arg1, int64 arg2, int64 *resp)
{
	if (unlikely(arg2 == 0))
		return PG_ERR_DIV_BY_ZERO;

	/* No overflow is possible */
	*resp = (int64) arg1 / arg2;
	return PG_OK;
}

int
pg_int82pl(int64 arg1, int16 arg2, int64 *resp)
{
	int64		result;

	if (unlikely(pg_add_s64_overflow(arg1, (int64) arg2, &result)))
		return PG_ERR_OUT_OF_RANGE;
	*resp = result;
	return PG_OK;
}

int
pg_int82mi(int64 arg1, int16 arg2, int64 *resp)
{
	int64		result;

	if (unlikely(pg_sub_s64_overflow(arg1, (int64) arg2, &result)))
		return PG_ERR_OUT_OF_RANGE;
	*resp = result;
	return PG_OK;
}

int
pg_int82mul(int64 arg1, int16 arg2, int64 *resp)
{
	int64		result;

	if (unlikely(pg_mul_s64_overflow(arg1, (int64) arg2, &result)))
		return PG_ERR_OUT_OF_RANGE;
	*resp = result;
	return PG_OK;
}

int
pg_int82div(int64 arg1, int16 arg2, int64 *resp)
{
	int64		result;

	if (unlikely(arg2 == 0))
		return PG_ERR_DIV_BY_ZERO;

	/*
	 * INT64_MIN / -1 is problematic, since the result can't be represented on
	 * a two's-complement machine.  Some machines produce INT64_MIN, some
	 * produce zero, some throw an exception.  We can dodge the problem by
	 * recognizing that division by -1 is the same as negation.
	 */
	if (arg2 == -1)
	{
		if (unlikely(arg1 == PG_INT64_MIN))
			return PG_ERR_OUT_OF_RANGE;
		result = -arg1;
		*resp = result;
		return PG_OK;
	}

	/* No overflow is possible */

	result = arg1 / arg2;

	*resp = result;
	return PG_OK;
}

int
pg_int28pl(int16 arg1, int64 arg2, int64 *resp)
{
	int64		result;

	if (unlikely(pg_add_s64_overflow((int64) arg1, arg2, &result)))
		return PG_ERR_OUT_OF_RANGE;
	*resp = result;
	return PG_OK;
}

int
pg_int28mi(int16 arg1, int64 arg2, int64 *resp)
{
	int64		result;

	if (unlikely(pg_sub_s64_overflow((int64) arg1, arg2, &result)))
		return PG_ERR_OUT_OF_RANGE;
	*resp = result;
	return PG_OK;
}

int
pg_int28mul(int16 arg1, int64 arg2, int64 *resp)
{
	int64		result;

	if (unlikely(pg_mul_s64_overflow((int64) arg1, arg2, &result)))
		return PG_ERR_OUT_OF_RANGE;
	*resp = result;
	return PG_OK;
}

int
pg_int28div(int16 arg1, int64 arg2, int64 *resp)
{
	if (unlikely(arg2 == 0))
		return PG_ERR_DIV_BY_ZERO;

	/* No overflow is possible */
	*resp = (int64) arg1 / arg2;
	return PG_OK;
}

/* ================= int.c: casts / inc ================= */

int32
pg_i2toi4(int16 arg1)
{
	return (int32) arg1;
}

int
pg_i4toi2(int32 arg1, int16 *resp)
{
	if (unlikely(arg1 < SHRT_MIN) || unlikely(arg1 > SHRT_MAX))
		return PG_ERR_OUT_OF_RANGE;

	*resp = (int16) arg1;
	return PG_OK;
}

int
pg_int4inc(int32 arg, int32 *resp)
{
	int32		result;

	if (unlikely(pg_add_s32_overflow(arg, 1, &result)))
		return PG_ERR_OUT_OF_RANGE;

	*resp = result;
	return PG_OK;
}

/* ================= int.c: int4 / int2 bit operations ================= */

int32
pg_int4and(int32 arg1, int32 arg2)
{
	return arg1 & arg2;
}

int32
pg_int4or(int32 arg1, int32 arg2)
{
	return arg1 | arg2;
}

int32
pg_int4xor(int32 arg1, int32 arg2)
{
	return arg1 ^ arg2;
}

/* C-side UB past count 31 / negative counts: harnesses fence the count. */
int32
pg_int4shl(int32 arg1, int32 arg2)
{
	return arg1 << arg2;
}

int32
pg_int4shr(int32 arg1, int32 arg2)
{
	return arg1 >> arg2;
}

int32
pg_int4not(int32 arg1)
{
	return ~arg1;
}

int16
pg_int2and(int16 arg1, int16 arg2)
{
	return arg1 & arg2;
}

int16
pg_int2or(int16 arg1, int16 arg2)
{
	return arg1 | arg2;
}

int16
pg_int2xor(int16 arg1, int16 arg2)
{
	return arg1 ^ arg2;
}

int16
pg_int2not(int16 arg1)
{
	return ~arg1;
}

/* C promotes arg1 to int before shifting; UB fenced by the harness. */
int16
pg_int2shl(int16 arg1, int32 arg2)
{
	return arg1 << arg2;
}

int16
pg_int2shr(int16 arg1, int32 arg2)
{
	return arg1 >> arg2;
}

/* ================= int8.c: int8 bit operations ================= */

int64
pg_int8and(int64 arg1, int64 arg2)
{
	return arg1 & arg2;
}

int64
pg_int8or(int64 arg1, int64 arg2)
{
	return arg1 | arg2;
}

int64
pg_int8xor(int64 arg1, int64 arg2)
{
	return arg1 ^ arg2;
}

int64
pg_int8not(int64 arg1)
{
	return ~arg1;
}

/* C-side UB past count 63 / negative counts: harnesses fence the count. */
int64
pg_int8shl(int64 arg1, int32 arg2)
{
	return arg1 << arg2;
}

int64
pg_int8shr(int64 arg1, int32 arg2)
{
	return arg1 >> arg2;
}

/*
 * ============== RATIFIED PLATFORM MODEL for shift counts ==============
 *
 * NOT vendored C.  For shift counts outside the C-defined domain, C's
 * behavior is UB; the observed behavior of C PostgreSQL on x86/ARM64 is
 * hardware count-masking (5-bit for 32-bit shifts, 6-bit for 64-bit),
 * ground-truthed on real PG 18.4 ARM64 (1<<32=1, 1<<33=2,
 * 1<<-1=-2147483648, 16>>33=8).  Per the TRIAGE "INT SHIFT UB PLANE"
 * pre-ruling, the out-of-range arm is proved Rust-vs-THIS-MODEL, never
 * vs the UB C expression.  Shifts are done in unsigned width (defined for
 * every value bit-pattern) and reinterpreted, matching Rust wrapping_shl /
 * wrapping_shr (arithmetic for signed >>).
 */

int32
pg_int4shl_model(int32 arg1, int32 arg2)
{
	return (int32) ((uint32) arg1 << ((uint32) arg2 & 31));
}

int32
pg_int4shr_model(int32 arg1, int32 arg2)
{
	return arg1 >> ((uint32) arg2 & 31); /* arithmetic shift, in-range count */
}

int16
pg_int2shl_model(int16 arg1, int32 arg2)
{
	/* promoted-to-int32 shift with a 5-bit-masked count, truncated back */
	return (int16) (uint16) ((uint32) (int32) arg1 << ((uint32) arg2 & 31));
}

int16
pg_int2shr_model(int16 arg1, int32 arg2)
{
	return (int16) ((int32) arg1 >> ((uint32) arg2 & 31));
}

int64
pg_int8shl_model(int64 arg1, int32 arg2)
{
	return (int64) ((uint64) arg1 << ((uint32) arg2 & 63));
}

int64
pg_int8shr_model(int64 arg1, int32 arg2)
{
	return arg1 >> ((uint32) arg2 & 63);
}

/* ================= int.c / int8.c: in_range support ================= */

int
pg_in_range_int4_int4(int32 val, int32 base, int32 offset, bool sub,
					  bool less, bool *resp)
{
	int32		sum;

	if (offset < 0)
		return PG_ERR_INVALID_PRECEDING;

	if (sub)
		offset = -offset;		/* cannot overflow */

	if (unlikely(pg_add_s32_overflow(base, offset, &sum)))
	{
		/*
		 * If sub is false, the true sum is surely more than val, so correct
		 * answer is the same as "less".  If sub is true, the true sum is
		 * surely less than val, so the answer is "!less".
		 */
		*resp = sub ? !less : less;
		return PG_OK;
	}

	if (less)
		*resp = val <= sum;
	else
		*resp = val >= sum;
	return PG_OK;
}

int
pg_in_range_int4_int2(int32 val, int32 base, int16 offset, bool sub,
					  bool less, bool *resp)
{
	/* Doesn't seem worth duplicating code for, so just invoke int4_int4 */
	return pg_in_range_int4_int4(val, base, (int32) offset, sub, less, resp);
}

int
pg_in_range_int4_int8(int32 val32, int32 base32, int64 offset, bool sub,
					  bool less, bool *resp)
{
	/* We must do all the math in int64 */
	int64		val = (int64) val32;
	int64		base = (int64) base32;
	int64		sum;

	if (offset < 0)
		return PG_ERR_INVALID_PRECEDING;

	if (sub)
		offset = -offset;		/* cannot overflow */

	if (unlikely(pg_add_s64_overflow(base, offset, &sum)))
	{
		*resp = sub ? !less : less;
		return PG_OK;
	}

	if (less)
		*resp = val <= sum;
	else
		*resp = val >= sum;
	return PG_OK;
}

int
pg_in_range_int2_int4(int16 val16, int16 base16, int32 offset, bool sub,
					  bool less, bool *resp)
{
	/* We must do all the math in int32 */
	int32		val = (int32) val16;
	int32		base = (int32) base16;
	int32		sum;

	if (offset < 0)
		return PG_ERR_INVALID_PRECEDING;

	if (sub)
		offset = -offset;		/* cannot overflow */

	if (unlikely(pg_add_s32_overflow(base, offset, &sum)))
	{
		*resp = sub ? !less : less;
		return PG_OK;
	}

	if (less)
		*resp = val <= sum;
	else
		*resp = val >= sum;
	return PG_OK;
}

int
pg_in_range_int2_int2(int16 val, int16 base, int16 offset, bool sub,
					  bool less, bool *resp)
{
	/* Doesn't seem worth duplicating code for, so just invoke int2_int4 */
	return pg_in_range_int2_int4(val, base, (int32) offset, sub, less, resp);
}

int
pg_in_range_int2_int8(int16 val, int16 base, int64 offset, bool sub,
					  bool less, bool *resp)
{
	/* Doesn't seem worth duplicating code for, so just invoke int4_int8 */
	return pg_in_range_int4_int8((int32) val, (int32) base, offset, sub,
								 less, resp);
}

int
pg_in_range_int8_int8(int64 val, int64 base, int64 offset, bool sub,
					  bool less, bool *resp)
{
	int64		sum;

	if (offset < 0)
		return PG_ERR_INVALID_PRECEDING;

	if (sub)
		offset = -offset;		/* cannot overflow */

	if (unlikely(pg_add_s64_overflow(base, offset, &sum)))
	{
		*resp = sub ? !less : less;
		return PG_OK;
	}

	if (less)
		*resp = val <= sum;
	else
		*resp = val >= sum;
	return PG_OK;
}

/* ================= int8.c: inc / dec / conversions ================= */

/*
 * USE_FLOAT8_BYVAL build: the #ifndef USE_FLOAT8_BYVAL agg in-place branch
 * is compiled out; this is the branch production takes.
 */
int
pg_int8inc(int64 arg, int64 *resp)
{
	/* Not called as an aggregate, so just do it the dumb way */
	int64		result;

	if (unlikely(pg_add_s64_overflow(arg, 1, &result)))
		return PG_ERR_OUT_OF_RANGE;

	*resp = result;
	return PG_OK;
}

int
pg_int8dec(int64 arg, int64 *resp)
{
	/* Not called as an aggregate, so just do it the dumb way */
	int64		result;

	if (unlikely(pg_sub_s64_overflow(arg, 1, &result)))
		return PG_ERR_OUT_OF_RANGE;

	*resp = result;
	return PG_OK;
}

int
pg_int8inc_any(int64 arg, int64 *resp)
{
	return pg_int8inc(arg, resp);
}

int
pg_int8dec_any(int64 arg, int64 *resp)
{
	return pg_int8dec(arg, resp);
}

int64
pg_int48(int32 arg)
{
	return (int64) arg;
}

int
pg_int84(int64 arg, int32 *resp)
{
	if (unlikely(arg < PG_INT32_MIN) || unlikely(arg > PG_INT32_MAX))
		return PG_ERR_OUT_OF_RANGE;

	*resp = (int32) arg;
	return PG_OK;
}

int64
pg_int28(int16 arg)
{
	return (int64) arg;
}

int
pg_int82(int64 arg, int16 *resp)
{
	if (unlikely(arg < PG_INT16_MIN) || unlikely(arg > PG_INT16_MAX))
		return PG_ERR_OUT_OF_RANGE;

	*resp = (int16) arg;
	return PG_OK;
}

int
pg_i8tooid(int64 arg, Oid *resp)
{
	if (unlikely(arg < 0) || unlikely(arg > PG_UINT32_MAX))
		return PG_ERR_OUT_OF_RANGE;

	*resp = (Oid) arg;
	return PG_OK;
}

int64
pg_oidtoi8(Oid arg)
{
	return (int64) arg;
}

/*
 * ================= int.c / int8.c recv/send over pqformat.c =================
 *
 * Provenance: src/backend/libpq/pqformat.c (pq_copymsgbytes, pq_getmsgint,
 * pq_getmsgint64, pq_begintypsend, pq_sendint16/32/64, pq_endtypsend) and
 * the int2/int4/int8 recv/send bodies from int.c / int8.c, same REL_18_STABLE
 * ref as above.
 *
 * Shims (plumbing only):
 *   - StringInfo -> (const unsigned char *data, int32 len, int32 *cursor)
 *     triple (recv side); the send side's palloc'd StringInfoData -> a
 *     caller-provided fixed buffer (payloads are 2/4/8 bytes + VARHDRSZ).
 *   - ereport(ERRCODE_PROTOCOL_VIOLATION, "insufficient data left in
 *     message") -> status 4 (PG_ERR_PROTOCOL, sqlstate 08P01).
 *   - pg_ntoh16/32/64 / pq_sendintNN byte emission: the little-endian
 *     byte-swap arm of port/pg_bswap.h, written as explicit byte
 *     shifts/stores (production targets are little-endian; the theorem is
 *     over the wire bytes, which are endian-invariant).
 *   - pq_endtypsend's SET_VARSIZE(result, buf->len): varatt.h 4B
 *     little-endian header ((uint32) len << 2), stored byte-wise.
 */

#include <string.h>

#define PG_ERR_PROTOCOL 4      /* ERRCODE_PROTOCOL_VIOLATION (08P01) */

static int
pg_pq_copymsgbytes(const unsigned char *data, int32 len, int32 *cursor,
				   void *buf, int32 datalen)
{
	if (datalen < 0 || datalen > (len - *cursor))
		return PG_ERR_PROTOCOL; /* insufficient data left in message */
	memcpy(buf, &data[*cursor], datalen);
	*cursor += datalen;
	return PG_OK;
}

int
pg_int2recv(const unsigned char *data, int32 len, int32 *cursor, int16 *out)
{
	/* pq_getmsgint(buf, 2): copy + pg_ntoh16 */
	unsigned char b[2];
	int			st = pg_pq_copymsgbytes(data, len, cursor, b, 2);

	if (st != PG_OK)
		return st;
	*out = (int16) (uint16) (((uint16) b[0] << 8) | (uint16) b[1]);
	return PG_OK;
}

int
pg_int4recv(const unsigned char *data, int32 len, int32 *cursor, int32 *out)
{
	/* pq_getmsgint(buf, 4): copy + pg_ntoh32 */
	unsigned char b[4];
	int			st = pg_pq_copymsgbytes(data, len, cursor, b, 4);

	if (st != PG_OK)
		return st;
	*out = (int32) (((uint32) b[0] << 24) | ((uint32) b[1] << 16) |
					((uint32) b[2] << 8) | (uint32) b[3]);
	return PG_OK;
}

int
pg_int8recv(const unsigned char *data, int32 len, int32 *cursor, int64 *out)
{
	/* pq_getmsgint64: copy + pg_ntoh64 */
	unsigned char b[8];
	int			st = pg_pq_copymsgbytes(data, len, cursor, b, 8);
	uint64		v = 0;
	int			i;

	if (st != PG_OK)
		return st;
	for (i = 0; i < 8; i++)
		v = (v << 8) | (uint64) b[i];
	*out = (int64) v;
	return PG_OK;
}

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
pg_int2send(int16 arg1, unsigned char *out /* [6] */ )
{
	/* pq_begintypsend reserves 4 bytes; pq_sendint16 appends BE bytes */
	out[4] = (unsigned char) (((uint16) arg1 >> 8) & 0xFF);
	out[5] = (unsigned char) ((uint16) arg1 & 0xFF);
	pg_set_varsize_4b(out, 6);	/* pq_endtypsend */
	return 6;
}

int32
pg_int4send(int32 arg1, unsigned char *out /* [8] */ )
{
	out[4] = (unsigned char) (((uint32) arg1 >> 24) & 0xFF);
	out[5] = (unsigned char) (((uint32) arg1 >> 16) & 0xFF);
	out[6] = (unsigned char) (((uint32) arg1 >> 8) & 0xFF);
	out[7] = (unsigned char) ((uint32) arg1 & 0xFF);
	pg_set_varsize_4b(out, 8);
	return 8;
}

int32
pg_int8send(int64 arg1, unsigned char *out /* [12] */ )
{
	uint64		v = (uint64) arg1;
	int			i;

	for (i = 0; i < 8; i++)
		out[4 + i] = (unsigned char) ((v >> (8 * (7 - i))) & 0xFF);
	pg_set_varsize_4b(out, 12);
	return 12;
}
