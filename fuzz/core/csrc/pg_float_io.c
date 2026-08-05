/*
 * Vendored PostgreSQL C: float4/float8 text I/O — differential-fuzz oracle.
 *
 * Provenance (all bodies VERBATIM unless a shim is listed below):
 *   - src/backend/utils/adt/float.c @ postgres-src
 *     62d6c7d3df6287f1bd83199c1a746e50d31571a0 (REL_18, the repo's vendored
 *     ground-truth checkout ../pgrust-fabled/vendor/postgres-src):
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

/*
 * pstrdup — THE SCRIBBLER's writer lived here (task #112; see
 * docs/conformance/scribbler-investigation-2026-08-02.md §8).
 *
 * PG's pstrdup returns a buffer of exactly strlen(s)+1 bytes, and the
 * verbatim bodies below RELY on that size:
 *
 *     char *errnumber = pstrdup(num);
 *     errnumber[endptr - num] = '\0';
 *
 * `endptr` is strtod's stop pointer, so the index runs up to strlen(num).
 * The previous shim returned a fixed 256-byte static and silently TRUNCATED,
 * which left that store in-bounds only for short inputs: for a longer `num`
 * it wrote one NUL byte at `buf + (endptr - num)`, i.e. arbitrarily far past
 * the buffer, into whatever static followed in .bss. Measured instance:
 * numeric_out(6E-1600) is a 1602-char string, so the store landed 1346 bytes
 * past the 256-byte buffer, exactly on byte index 2 of another TU's
 * `datecache[4]` — one byte of an otherwise-intact pointer zeroed, the banked
 * crash signature.
 *
 * The ALLOCATION SIZE is therefore load-bearing even though the message text
 * is out of scope for every comparator. Track the input exactly. Thread-local
 * (the suite drives oracles from parallel threads), grown by realloc and
 * never shrunk, so the error path still allocates nothing in steady state and
 * cannot leak per call.
 */
static _Thread_local char *pg_diff_msgbuf;
static _Thread_local size_t pg_diff_msgbuf_cap;
static _Thread_local size_t pg_diff_msgbuf_len;

/*
 * H6 DETECTOR (task #112): guard band + capacity invariant over the shim
 * message buffer, so a RE-INTRODUCED bounded/truncating shim — or any new
 * verbatim body that indexes past the string it was handed — is named at the
 * next oracle exit instead of silently scribbling on another TU's statics.
 *
 * The band is 64 bytes of 0xA5 past the NUL. `pg_diff_msgbuf_check()` is
 * called from OracleSerial::drop at depth 0, next to the H0 cache canary
 * (fuzz/core/src/lib.rs) — release-effective, no debug_assert, no sanitizer
 * (the debug-assert masking law).
 */
#define PG_DIFF_MSGBUF_GUARD 64
#define PG_DIFF_MSGBUF_FILL 0xA5

static char *
pstrdup(const char *s)
{
	size_t		n = strlen(s);
	size_t		want = n + 1 + PG_DIFF_MSGBUF_GUARD;

	/*
	 * EXACT, not grow-never-shrink. Sizing up only would leave slack from an
	 * earlier long call: after one 1602-byte pstrdup a subsequent 10-byte one
	 * would hand back ~1590 usable bytes, so the very overrun this shim exists
	 * to expose (an input-derived store past strlen) would land in slack and
	 * go UNSEEN by the guard band below. Real mcxt.c hands out a fresh chunk
	 * of exactly strlen+1 per call, and under this family's own doctrine the
	 * allocation SIZE is the load-bearing part of the contract. So realloc
	 * DOWN as well as up and keep the band immediately after the NUL.
	 */
	if (want != pg_diff_msgbuf_cap)
	{
		char	   *p = realloc(pg_diff_msgbuf, want);

		if (p == NULL)
			abort();			/* OOM in a shim: loud, never silent */
		pg_diff_msgbuf = p;
		pg_diff_msgbuf_cap = want;
	}
	memcpy(pg_diff_msgbuf, s, n);
	pg_diff_msgbuf[n] = '\0';
	pg_diff_msgbuf_len = n;
	memset(pg_diff_msgbuf + n + 1, PG_DIFF_MSGBUF_FILL, PG_DIFF_MSGBUF_GUARD);
	return pg_diff_msgbuf;
}

/*
 * 0 = intact. Else 1 = capacity smaller than the string it holds (a
 * truncating shim is back), or 2 + byte offset into the guard band of the
 * first clobbered byte (a body indexed past the string).
 */
/*
 * Slack probe for the EXACT-SIZING control: how many bytes the allocation
 * carries beyond strlen+1+GUARD. Exact sizing keeps this at 0 on every call; a
 * grow-never-shrink policy leaves the previous (longer) call's slack behind,
 * which is where an input-derived overrun would hide from the guard band.
 */
int
pg_diff_msgbuf_slack(void)
{
	if (pg_diff_msgbuf == NULL)
		return -1;
	return (int) (pg_diff_msgbuf_cap - (pg_diff_msgbuf_len + 1 + PG_DIFF_MSGBUF_GUARD));
}

int
pg_diff_msgbuf_check(void)
{
	if (pg_diff_msgbuf == NULL)
		return 0;				/* no error path taken on this thread yet */
	if (pg_diff_msgbuf_len + 1 + PG_DIFF_MSGBUF_GUARD > pg_diff_msgbuf_cap)
		return 1;
	for (size_t i = 0; i < PG_DIFF_MSGBUF_GUARD; i++)
	{
		if ((unsigned char) pg_diff_msgbuf[pg_diff_msgbuf_len + 1 + i]
			!= PG_DIFF_MSGBUF_FILL)
		{
			/* self-heal: re-arm the band so one hit cannot cascade */
			memset(pg_diff_msgbuf + pg_diff_msgbuf_len + 1,
				   PG_DIFF_MSGBUF_FILL, PG_DIFF_MSGBUF_GUARD);
			return 2 + (int) i;
		}
	}
	return 0;
}

/*
 * Must-fail control (fuzz/core/src/scribbler_bisect_tests.rs): drive a real
 * error path so the buffer exists, then clobber one guard byte exactly the
 * way an over-index would. Returns the guard offset poisoned, or -1 if the
 * buffer is not armed.
 */
int
pg_diff_msgbuf_poison_for_test(int off)
{
	if (pg_diff_msgbuf == NULL)
		return -1;
	if (off < 0 || off >= PG_DIFF_MSGBUF_GUARD)
		off = 0;
	pg_diff_msgbuf[pg_diff_msgbuf_len + 1 + off] = '\0';
	return off;
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

/* ======================================================================
 * p1-lanead additions (2026-07-31): the extra_float_digits <= 0 output
 * arm (pg_strfromd %.*g path), and the float4/8 recv/send wire images.
 *
 * Provenance (all bodies VERBATIM unless a shim is listed below):
 *   - src/port/snprintf.c @ the same vendored ref: PrintfTarget (struct),
 *     dostr, dopr_outch, pg_strfromd — verbatim.  flushbuffer is
 *     unreachable here (every PrintfTarget has stream == NULL) and is
 *     shimmed to abort(); Assert -> no-op; Min -> its c.h definition.
 *   - src/backend/utils/adt/float.c float4out / float8out_internal:
 *     the extra_float_digits <= 0 arm — verbatim bodies with
 *     extra_float_digits plumbed as a parameter (the GUC read is
 *     environment, not computation; mocked per the minimal-seaming rule).
 *   - src/backend/libpq/pqformat.c pq_copymsgbytes, pq_getmsgint,
 *     pq_getmsgint64, pq_getmsgfloat4, pq_getmsgfloat8, pq_sendfloat4,
 *     pq_sendfloat8 — verbatim, over a minimal StringInfo {data,len,
 *     cursor} shim (the three fields the bodies touch); pq_sendint32/64
 *     -> pg_hton32/64 stores into a fixed buffer (their exact effect for
 *     an 8-byte-headroom buffer); elog(ERROR,...) unreachable
 *     (pq_getmsgint is only called with b == 4) -> abort().
 *   - ereport(ERROR, ...) inside the pq functions -> record errcode +
 *     longjmp (the pg_float_math.c convention; the io in-functions above
 *     use soft ereturn and are untouched).
 *   - errcode symbols extend the TU convention: 6 = 08P01
 *     ERRCODE_PROTOCOL_VIOLATION.
 * ====================================================================== */

#include <setjmp.h>
#include <stdint.h>

#define ERRCODE_PROTOCOL_VIOLATION 6

static _Thread_local jmp_buf pg_diff_io_jmp;

/* ereport for the pq_* bodies only: record + longjmp (never returns). */
#define pq_ereport(level, stuff) \
	do { (void) (stuff); longjmp(pg_diff_io_jmp, 1); } while (0)

#define Min(x, y) ((x) < (y) ? (x) : (y))
#define Assert(condition) ((void) 0)

/* ---- src/port/snprintf.c PrintfTarget — VERBATIM ---- */

typedef struct
{
	char	   *bufptr;			/* next buffer output position */
	char	   *bufstart;		/* first buffer element */
	char	   *bufend;			/* last+1 buffer element, or NULL */
	/* bufend == NULL is for sprintf, where we assume buf is big enough */
	FILE	   *stream;			/* eventual output destination, or NULL */
	int			nchars;			/* # chars sent to stream, or dropped */
	bool		failed;			/* call is a failure; errno is set */
} PrintfTarget;

/* stream is always NULL in this TU: flushbuffer can never be reached. */
static void
flushbuffer(PrintfTarget *target)
{
	(void) target;
	abort();
}

/* ---- src/port/snprintf.c dostr / dopr_outch — VERBATIM ---- */

static void
dopr_outch(int c, PrintfTarget *target)
{
	if (target->bufend != NULL && target->bufptr >= target->bufend)
	{
		/* buffer full, can we dump to stream? */
		if (target->stream == NULL)
		{
			target->nchars++;	/* no, lose the data */
			return;
		}
		flushbuffer(target);
	}
	*(target->bufptr++) = c;
}

static void
dostr(const char *str, int slen, PrintfTarget *target)
{
	/* fast path for common case of slen == 1 */
	if (slen == 1)
	{
		dopr_outch(*str, target);
		return;
	}

	while (slen > 0)
	{
		int			avail;

		if (target->bufend != NULL)
			avail = target->bufend - target->bufptr;
		else
			avail = slen;
		if (avail <= 0)
		{
			/* buffer full, can we dump to stream? */
			if (target->stream == NULL)
			{
				target->nchars += slen; /* no, lose the data */
				return;
			}
			flushbuffer(target);
			continue;
		}
		avail = Min(avail, slen);
		memmove(target->bufptr, str, avail);
		target->bufptr += avail;
		str += avail;
		slen -= avail;
	}
}

/* ---- src/port/snprintf.c pg_strfromd — VERBATIM ---- */

/*
 * Nonstandard entry point that can be used by applications that want to
 * format a double with a given precision.  The general printf code can't
 * handle this because it doesn't know the precision until run-time.  We
 * assume here that the buffer is large enough for the result.
 */
int
pg_strfromd(char *str, size_t count, int precision, double value)
{
	PrintfTarget target;
	int			signvalue = 0;
	int			vallen;
	char		fmt[8];
	char		convert[64];

	/* Set up the target like pg_snprintf, but require nonempty buffer */
	Assert(count > 0);
	target.bufstart = target.bufptr = str;
	target.bufend = str + count - 1;
	target.stream = NULL;
	target.nchars = 0;
	target.failed = false;

	/*
	 * We bound precision to a reasonable range; the combination of this and
	 * the knowledge that we're using "g" format without padding allows the
	 * convert[] buffer to be reasonably small.
	 */
	if (precision < 1)
		precision = 1;
	else if (precision > 32)
		precision = 32;

	/*
	 * The rest is just an inlined version of the fmtfloat() logic above,
	 * simplified using the knowledge that no padding is wanted.
	 */
	if (isnan(value))
	{
		strcpy(convert, "NaN");
		vallen = 3;
	}
	else
	{
		static const double dzero = 0.0;

		if (value < 0.0 ||
			(value == 0.0 &&
			 memcmp(&value, &dzero, sizeof(double)) != 0))
		{
			signvalue = '-';
			value = -value;
		}

		if (isinf(value))
		{
			strcpy(convert, "Infinity");
			vallen = 8;
		}
		else
		{
			fmt[0] = '%';
			fmt[1] = '.';
			fmt[2] = '*';
			fmt[3] = 'g';
			fmt[4] = '\0';
			vallen = snprintf(convert, sizeof(convert), fmt, precision, value);
			if (vallen < 0)
			{
				target.failed = true;
				goto fail;
			}

#ifdef WIN32
			if (vallen >= 6 &&
				convert[vallen - 5] == 'e' &&
				convert[vallen - 3] == '0')
			{
				convert[vallen - 3] = convert[vallen - 2];
				convert[vallen - 2] = convert[vallen - 1];
				vallen--;
			}
#endif
		}
	}

	if (signvalue)
		dopr_outch(signvalue, &target);

	dostr(convert, vallen, &target);

fail:
	*(target.bufptr) = '\0';
	return target.failed ? -1 : (target.bufptr - target.bufstart
								 + target.nchars);
}

/*
 * float8out_internal / float4out, extra_float_digits <= 0 arm — VERBATIM
 * bodies with extra_float_digits as a parameter (see header note).  The
 * > 0 arm is the existing pg_diff_float8out/pg_diff_float4out above.
 */
static char *
float8out_internal_efd(double num, int efd)
{
	char	   *ascii = (char *) palloc(32);
	int			ndig = DBL_DIG + efd;

	if (efd > 0)
	{
		double_to_shortest_decimal_buf(num, ascii);
		return ascii;
	}

	(void) pg_strfromd(ascii, 32, ndig, num);
	return ascii;
}

static char *
float4out_efd(float num, int efd)
{
	char	   *ascii = (char *) palloc(32);
	int			ndig = FLT_DIG + efd;

	if (efd > 0)
	{
		float_to_shortest_decimal_buf(num, ascii);
		return ascii;
	}

	(void) pg_strfromd(ascii, 32, ndig, num);
	return ascii;
}

/* ---- src/backend/libpq/pqformat.c — VERBATIM over the StringInfo shim ---- */

typedef struct
{
	char	   *data;
	int			len;
	int			cursor;
}			pg_diff_stringinfo;

typedef pg_diff_stringinfo *StringInfo;

#define pg_ntoh16(x) ((uint16_t) ( \
	(((uint16_t) (x) & 0x00ff) << 8) | (((uint16_t) (x) & 0xff00) >> 8)))
#define pg_ntoh32(x) ((uint32_t) ( \
	(((uint32_t) (x) & 0x000000ff) << 24) | \
	(((uint32_t) (x) & 0x0000ff00) << 8) | \
	(((uint32_t) (x) & 0x00ff0000) >> 8) | \
	(((uint32_t) (x) & 0xff000000) >> 24)))
#define pg_ntoh64(x) ((uint64_t) ( \
	(((uint64_t) pg_ntoh32((uint32_t) ((x) & 0xffffffff))) << 32) | \
	(uint64_t) pg_ntoh32((uint32_t) ((x) >> 32))))
#define pg_hton32(x) pg_ntoh32(x)
#define pg_hton64(x) pg_ntoh64(x)

typedef uint16_t uint16;
typedef uint32_t uint32;
typedef uint64_t uint64;
typedef int64_t int64;

static void
pq_copymsgbytes(StringInfo msg, void *buf, int datalen)
{
	if (datalen < 0 || datalen > (msg->len - msg->cursor))
		pq_ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("insufficient data left in message")));
	memcpy(buf, &msg->data[msg->cursor], datalen);
	msg->cursor += datalen;
}

static unsigned int
pq_getmsgint(StringInfo msg, int b)
{
	unsigned int result;
	unsigned char n8;
	uint16		n16;
	uint32		n32;

	switch (b)
	{
		case 1:
			pq_copymsgbytes(msg, &n8, 1);
			result = n8;
			break;
		case 2:
			pq_copymsgbytes(msg, &n16, 2);
			result = pg_ntoh16(n16);
			break;
		case 4:
			pq_copymsgbytes(msg, &n32, 4);
			result = pg_ntoh32(n32);
			break;
		default:
			abort();			/* elog(ERROR, "unsupported integer size") */
	}
	return result;
}

static int64
pq_getmsgint64(StringInfo msg)
{
	uint64		n64;

	pq_copymsgbytes(msg, &n64, sizeof(n64));

	return pg_ntoh64(n64);
}

static float4
pq_getmsgfloat4(StringInfo msg)
{
	union
	{
		float4		f;
		uint32		i;
	}			swap;

	swap.i = pq_getmsgint(msg, 4);
	return swap.f;
}

static float8
pq_getmsgfloat8(StringInfo msg)
{
	union
	{
		float8		f;
		int64		i;
	}			swap;

	swap.i = pq_getmsgint64(msg);
	return swap.f;
}

/* pq_sendint32/64 effect for a fixed headroom buffer (see header note). */
static void
pq_sendfloat4(char out[4], float4 f)
{
	union
	{
		float4		f;
		uint32		i;
	}			swap;
	uint32		n;

	swap.f = f;
	n = pg_hton32(swap.i);
	memcpy(out, &n, 4);
}

static void
pq_sendfloat8(char out[8], float8 f)
{
	union
	{
		float8		f;
		int64		i;
	}			swap;
	uint64		n;

	swap.f = f;
	n = pg_hton64((uint64) swap.i);
	memcpy(out, &n, 8);
}

/* ---- fuzz-facing entry points (drivers, NOT Postgres code) ---- */

/* Returns the NUL-terminated image length. efd = extra_float_digits. */
int
pg_diff_float8out_efd(double num, int efd, char *buf32)
{
	char	   *s = float8out_internal_efd(num, efd);
	size_t		n = strlen(s);

	memcpy(buf32, s, n + 1);
	free(s);
	return (int) n;
}

int
pg_diff_float4out_efd(float num, int efd, char *buf32)
{
	char	   *s = float4out_efd(num, efd);
	size_t		n = strlen(s);

	memcpy(buf32, s, n + 1);
	free(s);
	return (int) n;
}

/* Returns 0 and writes *out on success, else the shimmed errcode class. */
int
pg_diff_float4recv(const char *data, int len, float *out)
{
	pg_diff_stringinfo si;

	si.data = (char *) data;
	si.len = len;
	si.cursor = 0;
	pg_diff_errcode = 0;
	if (setjmp(pg_diff_io_jmp))
		return pg_diff_errcode;
	*out = pq_getmsgfloat4(&si);
	return 0;
}

int
pg_diff_float8recv(const char *data, int len, double *out)
{
	pg_diff_stringinfo si;

	si.data = (char *) data;
	si.len = len;
	si.cursor = 0;
	pg_diff_errcode = 0;
	if (setjmp(pg_diff_io_jmp))
		return pg_diff_errcode;
	*out = pq_getmsgfloat8(&si);
	return 0;
}

void
pg_diff_float4send(float num, char *out4)
{
	pq_sendfloat4(out4, num);
}

void
pg_diff_float8send(double num, char *out8)
{
	pq_sendfloat8(out8, num);
}
