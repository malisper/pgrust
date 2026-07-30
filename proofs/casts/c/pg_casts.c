/*
 * Vendored PostgreSQL C for the scalar cast proof family
 * (int<->int width casts, bool<->int4, float widening/narrowing,
 * float<->int conversions).
 *
 * Provenance:
 *   - src/backend/utils/adt/float.c  (ftod, dtof, dtoi4, dtoi2, i4tod,
 *                                     i2tod, ftoi4, ftoi2, i4tof, i2tof)
 *   - src/backend/utils/adt/int8.c   (int48, int84, int28, int82,
 *                                     i8tod, dtoi8, i8tof, ftoi8)
 *   - src/backend/utils/adt/int.c    (i2toi4, i4toi2, int4_bool, bool_int4)
 *   - src/include/c.h                (FLOAT{4,8}_FITS_IN_INT{16,32,64}
 *                                     macros, verbatim)
 *   ref: postgres/postgres REL_18_STABLE, fetched 2026-07-28
 *
 * Shims (plumbing only, never logic):
 *   - `pg_` prefix on every function name.
 *   - fmgr PG_FUNCTION_ARGS unwrapping -> plain C signatures: PG_GETARG_*
 *     -> direct by-value params (exactly what the getters deliver);
 *     PG_RETURN_INT16/32/64/FLOAT4/FLOAT8 -> plain returns for infallible
 *     casts; PG_RETURN_BOOL -> int return (0/1; Kani lowers Rust bool vs C
 *     _Bool inconsistently, int is the established shim); PG_GETARG_BOOL
 *     -> int param compared against 0/1.
 *   - ereport(ERROR, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE, ...) -> the
 *     fallible casts return an error flag instead of raising: 0 = success
 *     with *result set, nonzero = would-have-raised (dtof distinguishes
 *     1 = float_overflow_error, 2 = float_underflow_error; both are
 *     ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE in float.c). The range TESTS and
 *     the on-success values are verbatim.
 *   - float_overflow_error()/float_underflow_error() (src/backend/utils/
 *     adt/float.c) are ereport(ERROR, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
 *     wrappers -> folded into the same error-flag shim.
 *   - typedefs/macros normally supplied by headers: float4/float8,
 *     int16/int32/int64, PG_INT*_MIN/MAX, unlikely. SHRT_MIN/SHRT_MAX come
 *     from <limits.h> exactly as int.c uses them.
 *   - rint/isnan/isinf come from <math.h>; CBMC models them per IEEE-754.
 *     Note the C bodies of ftoi2/ftoi4/ftoi8 do `num = rint(num);` on a
 *     float4 variable: the double-rint result is narrowed back to float —
 *     kept verbatim (that narrowing is part of the C spec under proof).
 *
 * Function bodies between the arg-fetch lines and the returns are verbatim.
 * Postgres compiles with -fwrapv; no signed overflow is reachable in these
 * bodies anyway (every narrowing is range-checked before the cast).
 *
 * pg_control_dtoi4_trunc at the bottom is NOT Postgres code: it is a
 * deliberately WRONG dtoi4 (C truncation cast instead of rint) used only by
 * the negative-control harness, which must fail on fractional inputs.
 * Never cite it as vendored C.
 */

#include <stdint.h>
#include <limits.h>
#include <math.h>

typedef int16_t int16;
typedef int32_t int32;
typedef int64_t int64;
typedef float float4;
typedef double float8;

#define PG_INT16_MIN	(-0x7FFF - 1)
#define PG_INT16_MAX	(0x7FFF)
#define PG_INT32_MIN	(-0x7FFFFFFF - 1)
#define PG_INT32_MAX	(0x7FFFFFFF)
#define PG_INT64_MIN	(-INT64_C(0x7FFFFFFFFFFFFFFF) - 1)
#define PG_INT64_MAX	INT64_C(0x7FFFFFFFFFFFFFFF)

#define unlikely(x) (x)

/* ---------- src/include/c.h: range-check macros, verbatim ---------- */

#define FLOAT4_FITS_IN_INT16(num) \
	((num) >= (float4) PG_INT16_MIN && (num) < -((float4) PG_INT16_MIN))
#define FLOAT4_FITS_IN_INT32(num) \
	((num) >= (float4) PG_INT32_MIN && (num) < -((float4) PG_INT32_MIN))
#define FLOAT4_FITS_IN_INT64(num) \
	((num) >= (float4) PG_INT64_MIN && (num) < -((float4) PG_INT64_MIN))
#define FLOAT8_FITS_IN_INT16(num) \
	((num) >= (float8) PG_INT16_MIN && (num) < -((float8) PG_INT16_MIN))
#define FLOAT8_FITS_IN_INT32(num) \
	((num) >= (float8) PG_INT32_MIN && (num) < -((float8) PG_INT32_MIN))
#define FLOAT8_FITS_IN_INT64(num) \
	((num) >= (float8) PG_INT64_MIN && (num) < -((float8) PG_INT64_MIN))

/* ---------- float.c: float widening / narrowing ---------- */

float8
pg_ftod(float4 num)
{
	return (float8) num;
}

/* returns 0 = ok, 1 = float_overflow_error, 2 = float_underflow_error */
int
pg_dtof(float8 num, float4 *presult)
{
	float4		result;

	result = (float4) num;
	if (unlikely(isinf(result)) && !isinf(num))
		return 1;				/* float_overflow_error() */
	if (unlikely(result == 0.0f) && num != 0.0)
		return 2;				/* float_underflow_error() */

	*presult = result;
	return 0;
}

/* ---------- float.c: float8 -> int ---------- */

int
pg_dtoi4(float8 num, int32 *presult)
{
	/*
	 * Get rid of any fractional part in the input.  This is so we don't fail
	 * on just-out-of-range values that would round into range.  Note
	 * assumption that rint() will pass through a NaN or Inf unchanged.
	 */
	num = rint(num);

	/* Range check */
	if (unlikely(isnan(num) || !FLOAT8_FITS_IN_INT32(num)))
		return 1;				/* ereport "integer out of range" */

	*presult = (int32) num;
	return 0;
}

int
pg_dtoi2(float8 num, int16 *presult)
{
	num = rint(num);

	if (unlikely(isnan(num) || !FLOAT8_FITS_IN_INT16(num)))
		return 1;				/* ereport "smallint out of range" */

	*presult = (int16) num;
	return 0;
}

/* ---------- float.c: int -> float8 ---------- */

float8
pg_i4tod(int32 num)
{
	return (float8) num;
}

float8
pg_i2tod(int16 num)
{
	return (float8) num;
}

/* ---------- float.c: float4 -> int ---------- */

int
pg_ftoi4(float4 num, int32 *presult)
{
	num = rint(num);

	if (unlikely(isnan(num) || !FLOAT4_FITS_IN_INT32(num)))
		return 1;				/* ereport "integer out of range" */

	*presult = (int32) num;
	return 0;
}

int
pg_ftoi2(float4 num, int16 *presult)
{
	num = rint(num);

	if (unlikely(isnan(num) || !FLOAT4_FITS_IN_INT16(num)))
		return 1;				/* ereport "smallint out of range" */

	*presult = (int16) num;
	return 0;
}

/* ---------- float.c: int -> float4 ---------- */

float4
pg_i4tof(int32 num)
{
	return (float4) num;
}

float4
pg_i2tof(int16 num)
{
	return (float4) num;
}

/* ---------- int8.c: int width casts ---------- */

int64
pg_int48(int32 arg)
{
	return (int64) arg;
}

int
pg_int84(int64 arg, int32 *presult)
{
	if (unlikely(arg < PG_INT32_MIN) || unlikely(arg > PG_INT32_MAX))
		return 1;				/* ereport "integer out of range" */

	*presult = (int32) arg;
	return 0;
}

int64
pg_int28(int16 arg)
{
	return (int64) arg;
}

int
pg_int82(int64 arg, int16 *presult)
{
	if (unlikely(arg < PG_INT16_MIN) || unlikely(arg > PG_INT16_MAX))
		return 1;				/* ereport "smallint out of range" */

	*presult = (int16) arg;
	return 0;
}

/* ---------- int8.c: int8 <-> float ---------- */

float8
pg_i8tod(int64 arg)
{
	float8		result;

	result = arg;

	return result;
}

int
pg_dtoi8(float8 num, int64 *presult)
{
	num = rint(num);

	if (unlikely(isnan(num) || !FLOAT8_FITS_IN_INT64(num)))
		return 1;				/* ereport "bigint out of range" */

	*presult = (int64) num;
	return 0;
}

float4
pg_i8tof(int64 arg)
{
	float4		result;

	result = arg;

	return result;
}

int
pg_ftoi8(float4 num, int64 *presult)
{
	num = rint(num);

	if (unlikely(isnan(num) || !FLOAT4_FITS_IN_INT64(num)))
		return 1;				/* ereport "bigint out of range" */

	*presult = (int64) num;
	return 0;
}

/* ---------- int.c: int2 <-> int4, bool <-> int4 ---------- */

int32
pg_i2toi4(int16 arg1)
{
	return (int32) arg1;
}

int
pg_i4toi2(int32 arg1, int16 *presult)
{
	if (unlikely(arg1 < SHRT_MIN) || unlikely(arg1 > SHRT_MAX))
		return 1;				/* ereport "smallint out of range" */

	*presult = (int16) arg1;
	return 0;
}

int
pg_int4_bool(int32 arg)
{
	if (arg == 0)
		return 0;				/* PG_RETURN_BOOL(false) */
	else
		return 1;				/* PG_RETURN_BOOL(true) */
}

int32
pg_bool_int4(int arg)
{
	if (arg == 0)				/* PG_GETARG_BOOL(0) == false */
		return 0;
	else
		return 1;
}

/* ---------- control-only, NOT Postgres code ---------- */

/* deliberately wrong dtoi4: truncating cast instead of rint. */
int
pg_control_dtoi4_trunc(float8 num, int32 *presult)
{
	if (unlikely(isnan(num) || !FLOAT8_FITS_IN_INT32(num)))
		return 1;

	*presult = (int32) num;
	return 0;
}
