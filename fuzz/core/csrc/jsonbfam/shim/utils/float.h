/* SHIM utils/float.h — get_floatN_infinity/nan VERBATIM from
 * src/include/utils/float.h @ 62d6c7d3df (lines 54-133); float4in/
 * float8in fmgr wrappers live in the driver TU over pg_float_io.c. */
#ifndef PG_JSONBFAM_SHIM_FLOAT_H
#define PG_JSONBFAM_SHIM_FLOAT_H
#include "postgres.h"
#include "fmgr.h"
#include <math.h>
/*
 * Routines to provide reasonably platform-independent handling of
 * infinity and NaN
 *
 * We assume that isinf() and isnan() are available and work per spec.
 * (On some platforms, we have to supply our own; see src/port.)  However,
 * generating an Infinity or NaN in the first place is less well standardized;
 * pre-C99 systems tend not to have C99's INFINITY and NaN macros.  We
 * centralize our workarounds for this here.
 */

/*
 * The funny placements of the two #pragmas is necessary because of a
 * long lived bug in the Microsoft compilers.
 * See http://support.microsoft.com/kb/120968/en-us for details
 */
#ifdef _MSC_VER
#pragma warning(disable:4756)
#endif
static inline float4
get_float4_infinity(void)
{
#ifdef INFINITY
	/* C99 standard way */
	return (float4) INFINITY;
#else
#ifdef _MSC_VER
#pragma warning(default:4756)
#endif

	/*
	 * On some platforms, HUGE_VAL is an infinity, elsewhere it's just the
	 * largest normal float8.  We assume forcing an overflow will get us a
	 * true infinity.
	 */
	return (float4) (HUGE_VAL * HUGE_VAL);
#endif
}

static inline float8
get_float8_infinity(void)
{
#ifdef INFINITY
	/* C99 standard way */
	return (float8) INFINITY;
#else

	/*
	 * On some platforms, HUGE_VAL is an infinity, elsewhere it's just the
	 * largest normal float8.  We assume forcing an overflow will get us a
	 * true infinity.
	 */
	return (float8) (HUGE_VAL * HUGE_VAL);
#endif
}

static inline float4
get_float4_nan(void)
{
#ifdef NAN
	/* C99 standard way */
	return (float4) NAN;
#else
	/* Assume we can get a NAN via zero divide */
	return (float4) (0.0 / 0.0);
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
extern Datum float4in(PG_FUNCTION_ARGS);
extern Datum float8in(PG_FUNCTION_ARGS);
#endif
