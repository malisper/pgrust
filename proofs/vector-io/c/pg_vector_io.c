/*
 * pg_vector_io.c — vendored C for the oidvector / int2vector in/out rows.
 *
 * Provenance (fetched 2026-07-30, REL_18_STABLE):
 *   src/backend/utils/adt/oid.c   — oidvectorin (loop core), oidvectorout
 *   src/backend/utils/adt/int.c   — int2vectorin (loop core), int2vectorout
 *   src/backend/utils/adt/numutils.c — uint32in_subr (copied VERBATIM,
 *       with its shims, from proofs/misc-ops/c/pg_misc_ops.c SHIM 4 —
 *       the model the recorded oidin/xidin/cidin proofs already ride)
 *
 * Shims (plumbing only, never logic):
 *   V1. palloc0/repalloc growth -> a caller-provided fixed value array
 *       (cap = harness dim). The REL_18 bodies are cap-free (nalloc
 *       doubling); the harness fences dim <= cap, so the repalloc arm is
 *       out of the fenced domain, not out of the spec.
 *   V2. ereturn/ereport -> status return codes (PG_SHIM_OK 0 /
 *       PG_SHIM_ERR_SYNTAX 1 = 22P02 / PG_SHIM_ERR_RANGE 2 = 22003),
 *       matching proofs/misc-ops conventions; escontext == NULL (hard
 *       error path) is the modeled plane, soft-error riding out of proof.
 *   V3. libc strtoul(s, &endp, 0) -> pg_shim_strtoull (SHIM 4 model,
 *       copied from proofs/misc-ops; glibc-flavored base-0 semantics);
 *       libc strtol(s, &endp, 10) -> pg_shim_strtol10 below: optional
 *       sign + decimal digit run, i64 accumulation saturating to
 *       LLONG_MAX/LLONG_MIN with an erange flag (glibc long is 64-bit on
 *       every modeled target). Under the harness length fences the
 *       saturation arm is unreachable; the SHRT_MIN/MAX range check that
 *       C applies afterwards is fully in-theorem.
 *   V4. Decimal emission: C oidvectorout body is sprintf("%u") plus a
 *       strlen walk — libc printf has no CBMC model. The harness links
 *       ../intout/c/pg_intout.c and this file emits via pg_ultoa_n, the
 *       documented SPEC-LEVEL ANCHOR for %u's canonical decimal (same
 *       ruling as the recorded oidout/cidout/xidout rows).
 *       int2vectorout upstream already calls pg_itoa — verbatim, also
 *       provided by pg_intout.c.
 *   V5. header-field stores (SET_VARSIZE/ndim/dataoffset/elemtype/dim1/
 *       lbound1) are asserted Rust-side against literals computed from
 *       the C-returned element count n (the C stores are constant
 *       assignments; V5 moves them into the theorem rather than out).
 *   V6. isspace(c) -> pg_shim_isspace (C locale, matches pg_proof_shim
 *       conventions and the Rust cores' explicit whitespace sets).
 */

#include "../../support/c/pg_proof_shim.h"

#define PG_SHIM_ULLONG_MAX 0xFFFFFFFFFFFFFFFFULL
#define PG_SHIM_LLONG_MAX 0x7FFFFFFFFFFFFFFFLL
#define PG_SHIM_LLONG_MIN (-PG_SHIM_LLONG_MAX - 1LL)

#define PG_SHIM_OK 0
#define PG_SHIM_ERR_SYNTAX 1	/* ERRCODE_INVALID_TEXT_REPRESENTATION */
#define PG_SHIM_ERR_RANGE 2		/* ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE */

#define SHRT_MIN_ (-32768)
#define SHRT_MAX_ 32767

/* ../intout/c/pg_intout.c (link as a second --c-lib): %u / int2 emitters */
extern int	pg_ultoa_n(uint32 value, char *a);
extern int	pg_itoa(int16 i, char *a);

/* ======================================================================
 * SHIM 4 block — copied verbatim from proofs/misc-ops/c/pg_misc_ops.c
 * (same soundness contract; see that file for the full narrowing notes).
 * ====================================================================== */

static int
pg_shim_isspace(char c)
{
	return c == ' ' || c == '\t' || c == '\n' ||
		c == '\v' || c == '\f' || c == '\r';
}

static int
pg_shim_hexval(char c)
{
	if (c >= '0' && c <= '9')
		return c - '0';
	if (c >= 'a' && c <= 'f')
		return c - 'a' + 10;
	if (c >= 'A' && c <= 'F')
		return c - 'A' + 10;
	return -1;
}

static uint64
pg_shim_strtoull(const char *nptr, const char **endptr, int base, int *erange)
{
	const char *s = nptr;
	int			neg = 0;
	uint64		v = 0;
	int			any = 0;
	int			overflow = 0;

	while (pg_shim_isspace(*s))
		s++;
	if (*s == '-')
	{
		neg = 1;
		s++;
	}
	else if (*s == '+')
		s++;

	if (base == 0)
	{
		if (s[0] == '0' && (s[1] == 'x' || s[1] == 'X') &&
			pg_shim_hexval(s[2]) >= 0)
		{
			s += 2;
			base = 16;
		}
		else if (s[0] == '0')
			base = 8;			/* the '0' itself is the first digit */
		else
			base = 10;
	}

	if (base == 10)
	{
		for (;; s++)
		{
			char		c = *s;

			if (c < '0' || c > '9')
				break;
			{
				uint64		d = (uint64) (c - '0');

				if (v > (PG_SHIM_ULLONG_MAX - d) / 10)
					overflow = 1;
				else
					v = v * 10 + d;
				any = 1;
			}
		}
	}
	else if (base == 16)
	{
		for (;; s++)
		{
			int			dv = pg_shim_hexval(*s);

			if (dv < 0)
				break;
			if (v > (PG_SHIM_ULLONG_MAX - (uint64) dv) / 16)
				overflow = 1;
			else
				v = v * 16 + (uint64) dv;
			any = 1;
		}
	}
	else						/* base == 8 */
	{
		for (;; s++)
		{
			char		c = *s;

			if (c < '0' || c > '7')
				break;
			if (v > (PG_SHIM_ULLONG_MAX - (uint64) (c - '0')) / 8)
				overflow = 1;
			else
				v = v * 8 + (uint64) (c - '0');
			any = 1;
		}
	}

	if (!any)
	{
		*endptr = nptr;
		return 0;
	}
	*endptr = s;
	if (overflow)
	{
		*erange = 1;
		return PG_SHIM_ULLONG_MAX;
	}
	if (neg)
		return (uint64) (-(int64) v);
	return v;
}

/* numutils.c uint32in_subr — verbatim body from proofs/misc-ops */
static int
pg_uint32in_subr(const char *s, uint32 *result_out, int want_endloc,
				 const char **endloc_out)
{
	uint32		result;
	uint64		cvt;
	const char *endptr;
	int			erange = 0;		/* errno = 0 */

	cvt = pg_shim_strtoull(s, &endptr, 0, &erange);

	if (endptr == s)
		return PG_SHIM_ERR_SYNTAX;

	if (erange)
		return PG_SHIM_ERR_RANGE;

	if (want_endloc)
		*endloc_out = endptr;
	else
	{
		while (*endptr && pg_shim_isspace(*endptr))
			endptr++;
		if (*endptr)
			return PG_SHIM_ERR_SYNTAX;
	}

	result = (uint32) cvt;

	if (cvt != (uint64) result &&
		cvt != (uint64) ((int64) ((int32) result)))
		return PG_SHIM_ERR_RANGE;

	*result_out = result;
	return PG_SHIM_OK;
}

/* [shim V3] glibc strtol(s, &endp, 10) model (signed, 64-bit long) */
static int64
pg_shim_strtol10(const char *nptr, const char **endptr, int *erange)
{
	const char *s = nptr;
	int			neg = 0;
	uint64		v = 0;
	int			any = 0;
	int			overflow = 0;

	while (pg_shim_isspace(*s))
		s++;
	if (*s == '-')
	{
		neg = 1;
		s++;
	}
	else if (*s == '+')
		s++;

	for (;; s++)
	{
		char		c = *s;

		if (c < '0' || c > '9')
			break;
		{
			uint64		d = (uint64) (c - '0');
			uint64		cut = neg ? ((uint64) PG_SHIM_LLONG_MAX + 1) :
				(uint64) PG_SHIM_LLONG_MAX;

			if (v > (cut - d) / 10)
				overflow = 1;
			else
				v = v * 10 + d;
			any = 1;
		}
	}

	if (!any)
	{
		*endptr = nptr;
		return 0;
	}
	*endptr = s;
	if (overflow)
	{
		*erange = 1;
		return neg ? PG_SHIM_LLONG_MIN : PG_SHIM_LLONG_MAX;
	}
	return neg ? -(int64) v : (int64) v;
}

/* ======================================================================
 * oid.c oidvectorin — loop core, verbatim below shims V1/V2.
 * escontext == NULL plane: uint32in_subr failure is a hard error status.
 * ====================================================================== */
int
pg_oidvectorin(const char *oidString, uint32 *values, int cap, int *n_out)
{
	int			n;
	int			st;

	for (n = 0;; n++)
	{
		while (*oidString && pg_shim_isspace(*oidString))
			oidString++;
		if (*oidString == '\0')
			break;

		if (n >= cap)			/* [shim V1] repalloc arm: fenced out */
			return 99;

		st = pg_uint32in_subr(oidString, &values[n], 1, &oidString);
		if (st != PG_SHIM_OK)
			return st;
	}

	*n_out = n;
	return PG_SHIM_OK;
}

/* ======================================================================
 * oid.c oidvectorout — verbatim loop; sprintf("%u")+strlen walk -> the
 * pg_ultoa_n spec anchor ([shim V4]). Returns strlen(result).
 * check_valid_oidvector is a harness-side fence (valid images only).
 * ====================================================================== */
int
pg_oidvectorout(const uint32 *values, int32 dim1, char *rp)
{
	char	   *result = rp;
	int			num;

	for (num = 0; num < dim1; num++)
	{
		if (num != 0)
			*rp++ = ' ';
		rp += pg_ultoa_n(values[num], rp);	/* [shim V4] sprintf %u anchor */
	}
	*rp = '\0';
	return (int) (rp - result);
}

/* ======================================================================
 * int.c int2vectorin — loop core, verbatim below shims V1/V2/V3.
 * ====================================================================== */
int
pg_int2vectorin(const char *intString, int16 *values, int cap, int *n_out)
{
	int			n;

	for (n = 0;; n++)
	{
		int64		l;
		const char *endp;
		int			erange = 0;	/* errno = 0 */

		while (*intString && pg_shim_isspace(*intString))
			intString++;
		if (*intString == '\0')
			break;

		if (n >= cap)			/* [shim V1] repalloc arm: fenced out */
			return 99;

		l = pg_shim_strtol10(intString, &endp, &erange);

		if (intString == endp)
			return PG_SHIM_ERR_SYNTAX;

		if (erange || l < SHRT_MIN_ || l > SHRT_MAX_)
			return PG_SHIM_ERR_RANGE;

		if (*endp && *endp != ' ')
			return PG_SHIM_ERR_SYNTAX;

		values[n] = (int16) l;
		intString = endp;
	}

	*n_out = n;
	return PG_SHIM_OK;
}

/* ======================================================================
 * int.c int2vectorout — verbatim loop (upstream already emits via
 * pg_itoa). Returns strlen(result).
 * ====================================================================== */
int
pg_int2vectorout(const int16 *values, int32 dim1, char *rp)
{
	char	   *result = rp;
	int			num;

	for (num = 0; num < dim1; num++)
	{
		if (num != 0)
			*rp++ = ' ';
		rp += pg_itoa(values[num], rp);
	}
	*rp = '\0';
	return (int) (rp - result);
}
