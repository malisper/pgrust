/* SHIM header for the jsonpathexec_diff oracle - NOT PostgreSQL code.
 * Declares the float.c extracts vendored verbatim in pg_jsonb_min.c. */
#ifndef FLOAT_H
#define FLOAT_H
#include <math.h>

#include "postgres.h"
/* ---- float.h:93-108 VERBATIM (get_float8_infinity) ---- */
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

/* ---- float.h:122-133 VERBATIM (get_float8_nan) ---- */
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

extern float8 float8in_internal(char *num, char **endptr_p,
								const char *type_name, const char *orig_string,
								struct Node *escontext);
extern char *float8out_internal(float8 num);
#endif
