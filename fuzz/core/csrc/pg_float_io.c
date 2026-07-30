/*
 * Vendored PostgreSQL C: float4/float8 text I/O — differential-fuzz oracle.
 *
 * Provenance (all bodies VERBATIM unless a shim is listed below):
 *   - src/backend/utils/adt/float.c @ postgres-src
 *     62d6c7d3df6287f1bd83199c1a746e50d31571a0 (REL_18, the repo's vendored
 *     ground-truth checkout ../pgrust-reference/vendor/postgres-src):
 *     float4in_internal, float8in_internal, float8out_internal — verbatim.
 *     float4out shortest-decimal arm — verbatim (see pg_diff_float4out).
 *   - src/port/pgstrcasecmp.c @ same ref: pg_strncasecmp — verbatim.
 *   - src/include/utils/float.h @ same ref: get_float4_nan,
 *     get_float8_nan, get_float4_infinity, get_float8_infinity — verbatim.
 *   - Ryu shortest-decimal emission: csrc/ryu/{d2s.c,f2s.c,*.h} are
 *     byte-for-byte copies of src/common/* at the same ref (compiled as
 *     their own translation units; see build.rs), providing
 *     double_to_shortest_decimal_buf / float_to_shortest_decimal_buf.
 *
 * Shims (plumbing only, never logic):
 *   - ereturn(escontext, ret, (errcode(X), errmsg(...))) -> record X in
 *     pg_diff_errcode and return ret. The fuzz comparator checks the
 *     errcode class, not message text, so errmsg(...) evaluates to 0 with
 *     arguments unevaluated. escontext is the hard-error (NULL) shape:
 *     both sides run without a soft-error context.
 *   - errcode symbols -> small ints: 1 = ERRCODE_INVALID_TEXT_REPRESENTATION
 *     (22P02), 2 = ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE (22003).
 *   - pstrdup -> bounded static-buffer copy (feeds only the unevaluated
 *     errmsg; kept so the body stays verbatim without leaking under ASAN).
 *   - palloc/pfree -> malloc/free (float8out_internal's 32-byte result).
 *   - IS_HIGHBIT_SET (c.h) -> (ch & 0x80), its exact definition.
 *   - unlikely(x) -> (x) via shim postgres.h.
 *
 * NOTE the oracle's parse core is the platform strtod/strtof, exactly as in
 * real PostgreSQL (which defers to libc); on this host that is macOS libc.
 */

#include "postgres.h"

#include <ctype.h>
#include <errno.h>
#include <float.h>
#include <math.h>
#include <stdio.h>

#include "common/shortest_dec.h"

/* ---- shims (see header comment) ---- */

/*
 * THREAD-LOCAL (2026-07-30): the stable test suite drives the oracles from
 * parallel threads; a shared errcode raced across oracles (another test's
 * reset between record and read produced phantom verdicts). Rust reads it
 * through pg_diff_errcode_get() because stable Rust cannot bind a C
 * thread-local as an extern static.
 */
_Thread_local int pg_diff_errcode;

int
pg_diff_errcode_get(void)
{
	return pg_diff_errcode;
}

#define ERRCODE_INVALID_TEXT_REPRESENTATION 1
#define ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE 2

#define errcode(c) (pg_diff_errcode = (c))
#define errmsg(...) 0
#define ereturn(escontext, ret, stuff) do { (void) (stuff); return (ret); } while (0)

struct Node;					/* opaque; escontext is always NULL here */

static char pg_diff_msgbuf[256];
static char *
pstrdup(const char *s)
{
	size_t		n = strlen(s);

	if (n >= sizeof(pg_diff_msgbuf))
		n = sizeof(pg_diff_msgbuf) - 1;
	memcpy(pg_diff_msgbuf, s, n);
	pg_diff_msgbuf[n] = '\0';
	return pg_diff_msgbuf;
}

#define palloc(n) malloc(n)

#define IS_HIGHBIT_SET(ch) ((unsigned char) (ch) & 0x80)

/* ---- src/port/pgstrcasecmp.c: pg_strncasecmp — VERBATIM ---- */

static int
pg_strncasecmp(const char *s1, const char *s2, size_t n)
{
	while (n-- > 0)
	{
		unsigned char ch1 = (unsigned char) *s1++;
		unsigned char ch2 = (unsigned char) *s2++;

		if (ch1 != ch2)
		{
			if (ch1 >= 'A' && ch1 <= 'Z')
				ch1 += 'a' - 'A';
			else if (IS_HIGHBIT_SET(ch1) && isupper(ch1))
				ch1 = tolower(ch1);

			if (ch2 >= 'A' && ch2 <= 'Z')
				ch2 += 'a' - 'A';
			else if (IS_HIGHBIT_SET(ch2) && isupper(ch2))
				ch2 = tolower(ch2);

			if (ch1 != ch2)
				return (int) ch1 - (int) ch2;
		}
		if (ch1 == 0)
			break;
	}
	return 0;
}

/* ---- src/include/utils/float.h: special-value helpers — VERBATIM ---- */

static inline float4
get_float4_infinity(void)
{
#ifdef INFINITY
	/* C99 standard way */
	return (float4) INFINITY;
#else
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

/* ---- src/backend/utils/adt/float.c: float4in_internal — VERBATIM ---- */

float4
float4in_internal(char *num, char **endptr_p,
				  const char *type_name, const char *orig_string,
				  struct Node *escontext)
{
	float		val;
	char	   *endptr;

	/*
	 * endptr points to the first character _after_ the sequence we recognized
	 * as a valid floating point number. orig_string points to the original
	 * input string.
	 */

	/* skip leading whitespace */
	while (*num != '\0' && isspace((unsigned char) *num))
		num++;

	/*
	 * Check for an empty-string input to begin with, to avoid the vagaries of
	 * strtod() on different platforms.
	 */
	if (*num == '\0')
		ereturn(escontext, 0,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for type %s: \"%s\"",
						type_name, orig_string)));

	errno = 0;
	val = strtof(num, &endptr);

	/* did we not see anything that looks like a double? */
	if (endptr == num || errno != 0)
	{
		int			save_errno = errno;

		/*
		 * C99 requires that strtof() accept NaN, [+-]Infinity, and [+-]Inf,
		 * but not all platforms support all of these (and some accept them
		 * but set ERANGE anyway...)  Therefore, we check for these inputs
		 * ourselves if strtof() fails.
		 *
		 * Note: C99 also requires hexadecimal input as well as some extended
		 * forms of NaN, but we consider these forms unportable and don't try
		 * to support them.  You can use 'em if your strtof() takes 'em.
		 */
		if (pg_strncasecmp(num, "NaN", 3) == 0)
		{
			val = get_float4_nan();
			endptr = num + 3;
		}
		else if (pg_strncasecmp(num, "Infinity", 8) == 0)
		{
			val = get_float4_infinity();
			endptr = num + 8;
		}
		else if (pg_strncasecmp(num, "+Infinity", 9) == 0)
		{
			val = get_float4_infinity();
			endptr = num + 9;
		}
		else if (pg_strncasecmp(num, "-Infinity", 9) == 0)
		{
			val = -get_float4_infinity();
			endptr = num + 9;
		}
		else if (pg_strncasecmp(num, "inf", 3) == 0)
		{
			val = get_float4_infinity();
			endptr = num + 3;
		}
		else if (pg_strncasecmp(num, "+inf", 4) == 0)
		{
			val = get_float4_infinity();
			endptr = num + 4;
		}
		else if (pg_strncasecmp(num, "-inf", 4) == 0)
		{
			val = -get_float4_infinity();
			endptr = num + 4;
		}
		else if (save_errno == ERANGE)
		{
			/*
			 * Some platforms return ERANGE for denormalized numbers (those
			 * that are not zero, but are too close to zero to have full
			 * precision).  We'd prefer not to throw error for that, so try to
			 * detect whether it's a "real" out-of-range condition by checking
			 * to see if the result is zero or huge.
			 */
			if (val == 0.0 ||
#if !defined(HUGE_VALF)
				isinf(val)
#else
				(val >= HUGE_VALF || val <= -HUGE_VALF)
#endif
				)
			{
				/* see comments in float8in_internal for rationale */
				char	   *errnumber = pstrdup(num);

				errnumber[endptr - num] = '\0';

				ereturn(escontext, 0,
						(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
						 errmsg("\"%s\" is out of range for type real",
								errnumber)));
			}
		}
		else
			ereturn(escontext, 0,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("invalid input syntax for type %s: \"%s\"",
							type_name, orig_string)));
	}

	/* skip trailing whitespace */
	while (*endptr != '\0' && isspace((unsigned char) *endptr))
		endptr++;

	/* report stopping point if wanted, else complain if not end of string */
	if (endptr_p)
		*endptr_p = endptr;
	else if (*endptr != '\0')
		ereturn(escontext, 0,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for type %s: \"%s\"",
						type_name, orig_string)));

	return val;
}

/* ---- src/backend/utils/adt/float.c: float8in_internal — VERBATIM ---- */

float8
float8in_internal(char *num, char **endptr_p,
				  const char *type_name, const char *orig_string,
				  struct Node *escontext)
{
	double		val;
	char	   *endptr;

	/* skip leading whitespace */
	while (*num != '\0' && isspace((unsigned char) *num))
		num++;

	/*
	 * Check for an empty-string input to begin with, to avoid the vagaries of
	 * strtod() on different platforms.
	 */
	if (*num == '\0')
		ereturn(escontext, 0,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for type %s: \"%s\"",
						type_name, orig_string)));

	errno = 0;
	val = strtod(num, &endptr);

	/* did we not see anything that looks like a double? */
	if (endptr == num || errno != 0)
	{
		int			save_errno = errno;

		/*
		 * C99 requires that strtod() accept NaN, [+-]Infinity, and [+-]Inf,
		 * but not all platforms support all of these (and some accept them
		 * but set ERANGE anyway...)  Therefore, we check for these inputs
		 * ourselves if strtod() fails.
		 *
		 * Note: C99 also requires hexadecimal input as well as some extended
		 * forms of NaN, but we consider these forms unportable and don't try
		 * to support them.  You can use 'em if your strtod() takes 'em.
		 */
		if (pg_strncasecmp(num, "NaN", 3) == 0)
		{
			val = get_float8_nan();
			endptr = num + 3;
		}
		else if (pg_strncasecmp(num, "Infinity", 8) == 0)
		{
			val = get_float8_infinity();
			endptr = num + 8;
		}
		else if (pg_strncasecmp(num, "+Infinity", 9) == 0)
		{
			val = get_float8_infinity();
			endptr = num + 9;
		}
		else if (pg_strncasecmp(num, "-Infinity", 9) == 0)
		{
			val = -get_float8_infinity();
			endptr = num + 9;
		}
		else if (pg_strncasecmp(num, "inf", 3) == 0)
		{
			val = get_float8_infinity();
			endptr = num + 3;
		}
		else if (pg_strncasecmp(num, "+inf", 4) == 0)
		{
			val = get_float8_infinity();
			endptr = num + 4;
		}
		else if (pg_strncasecmp(num, "-inf", 4) == 0)
		{
			val = -get_float8_infinity();
			endptr = num + 4;
		}
		else if (save_errno == ERANGE)
		{
			/*
			 * Some platforms return ERANGE for denormalized numbers (those
			 * that are not zero, but are too close to zero to have full
			 * precision).  We'd prefer not to throw error for that, so try to
			 * detect whether it's a "real" out-of-range condition by checking
			 * to see if the result is zero or huge.
			 *
			 * On error, we intentionally complain about double precision not
			 * the given type name, and we print only the part of the string
			 * that is the current number.
			 */
			if (val == 0.0 || val >= HUGE_VAL || val <= -HUGE_VAL)
			{
				char	   *errnumber = pstrdup(num);

				errnumber[endptr - num] = '\0';
				ereturn(escontext, 0,
						(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
						 errmsg("\"%s\" is out of range for type double precision",
								errnumber)));
			}
		}
		else
			ereturn(escontext, 0,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("invalid input syntax for type %s: \"%s\"",
							type_name, orig_string)));
	}

	/* skip trailing whitespace */
	while (*endptr != '\0' && isspace((unsigned char) *endptr))
		endptr++;

	/* report stopping point if wanted, else complain if not end of string */
	if (endptr_p)
		*endptr_p = endptr;
	else if (*endptr != '\0')
		ereturn(escontext, 0,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for type %s: \"%s\"",
						type_name, orig_string)));

	return val;
}

/*
 * float8out_internal — VERBATIM body except: extra_float_digits is the
 * default GUC value 1 (so the shortest-decimal arm, the shipped default,
 * always runs; the pg_strfromd arm is dead code here and elided — the
 * fuzz targets do not cover extra_float_digits <= 0).
 */
static const int extra_float_digits = 1;

char *
float8out_internal(double num)
{
	char	   *ascii = (char *) palloc(32);

	if (extra_float_digits > 0)
	{
		double_to_shortest_decimal_buf(num, ascii);
		return ascii;
	}

	abort();					/* unreachable: extra_float_digits == 1 */
}

/* ---- fuzz-facing entry points (drivers, NOT Postgres code) ---- */

double
pg_diff_float8in(const char *num)
{
	pg_diff_errcode = 0;
	return float8in_internal((char *) num, NULL, "double precision", num, NULL);
}

float
pg_diff_float4in(const char *num)
{
	pg_diff_errcode = 0;
	return float4in_internal((char *) num, NULL, "real", num, NULL);
}

/* Returns the NUL-terminated image length, exactly the C cstring image. */
int
pg_diff_float8out(double num, char *buf32)
{
	char	   *s = float8out_internal(num);
	size_t		n = strlen(s);

	memcpy(buf32, s, n + 1);
	free(s);
	return (int) n;
}

/*
 * float4out default arm: extra_float_digits = 1 > 0 selects the
 * float_to_shortest_decimal_buf path — VERBATIM from float4out.
 */
int
pg_diff_float4out(float num, char *buf32)
{
	char	   *ascii = (char *) palloc(32);
	size_t		n;

	float_to_shortest_decimal_buf(num, ascii);
	n = strlen(ascii);
	memcpy(buf32, ascii, n + 1);
	free(ascii);
	return (int) n;
}
