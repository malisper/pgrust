/*
 * Vendored from postgres master src/backend/utils/adt/numutils.c
 * (fetched 2026-07-28 from raw.githubusercontent.com/postgres/postgres/master).
 * REL_18_STABLE conformance: zero code drift vs REL_18_STABLE (provenance
 * audit, proofs/PROVENANCE-AUDIT.md, 2026-07-28).
 *
 * SHIM MANIFEST — every deviation from the upstream source, exhaustively:
 *   [S1] Signature: `int32 pg_strtoint32_safe(const char *s, Node *escontext)`
 *        -> `int32_t pgc_strtoint32_safe(const char *s, int32_t *err_kind)`.
 *        Renamed with pgc_ prefix to avoid clashing with the Rust symbol
 *        space; escontext (soft-error Node) replaced by an out-param error
 *        flag: 0 = OK, 1 = OUT_OF_RANGE, 2 = INVALID_SYNTAX.
 *   [S2] The two `ereturn(escontext, 0, (errcode(...), errmsg(...)))` tails
 *        at labels out_of_range / invalid_syntax are replaced by
 *        `*err_kind = 1; return 0;` and `*err_kind = 2; return 0;`.
 *        ereturn's errorval is 0 in upstream, preserved here. Error MESSAGE
 *        equivalence is out of scope; error KIND is what we prove.
 *   [S3] Postgres typedefs (int32/uint32/int8/bool/likely/unlikely and
 *        PG_INT32_MIN/MAX) replaced by <stdint.h>/<stdbool.h> equivalents
 *        and INT32_MIN/INT32_MAX; likely/unlikely dropped (semantically
 *        inert branch hints).
 *   [S4] pg_neg_u32_overflow() vendored inline from src/include/common/int.h
 *        (the HAVE__BUILTIN_OP_OVERFLOW variant rewritten as the portable
 *        int64 form, which is bit-identical for uint32 inputs; upstream's
 *        error-path *result poison value 0x5EED kept).
 *   [S5] isspace/isdigit/isxdigit calls kept, but backed by local ASCII/C-
 *        locale implementations (pg parses in C locale; CBMC's ctype model
 *        agrees, but local defs make the model explicit and self-contained).
 *
 * EVERYTHING between the function's opening brace and the two shimmed
 * labels is verbatim upstream parsing logic (modulo [S3] type spellings).
 */

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

/* [S5] C-locale ctype, explicit */
static bool pg_isspace(unsigned char c)
{
	return c == ' ' || c == '\t' || c == '\n' || c == '\v' || c == '\f' || c == '\r';
}
static bool pg_isdigit(unsigned char c) { return c >= '0' && c <= '9'; }
static bool pg_isxdigit(unsigned char c)
{
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
}
#define isspace(c) pg_isspace((unsigned char) (c))
#define isdigit(c) pg_isdigit((unsigned char) (c))
#define isxdigit(c) pg_isxdigit((unsigned char) (c))

/* verbatim upstream (numutils.c line 87), int8 -> int8_t per [S3] */
static const int8_t hexlookup[128] = {
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	0, 1, 2, 3, 4, 5, 6, 7, 8, 9, -1, -1, -1, -1, -1, -1,
	-1, 10, 11, 12, 13, 14, 15, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, 10, 11, 12, 13, 14, 15, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
};

/* [S4] vendored from src/include/common/int.h */
static bool
pg_neg_u32_overflow(uint32_t a, int32_t *result)
{
	int64_t		res = -((int64_t) a);

	if (res < INT32_MIN)
	{
		*result = 0x5EED;		/* to avoid spurious warnings */
		return true;
	}
	*result = (int32_t) res;
	return false;
}

/* [S1] shimmed signature; body verbatim upstream until the labels */
int32_t
pgc_strtoint32_safe(const char *s, int32_t *err_kind)
{
	const char *ptr = s;
	const char *firstdigit;
	uint32_t	tmp = 0;
	bool		neg = false;
	unsigned char digit;
	int32_t		result;

	*err_kind = 0;				/* [S2] flag init (upstream has no flag) */

	/* leave it up to the slow path to look for leading spaces */

	if (*ptr == '-')
	{
		ptr++;
		neg = true;
	}

	/* a leading '+' is uncommon so leave that for the slow path */

	/* process the first digit */
	digit = (*ptr - '0');

	if (digit < 10)
	{
		ptr++;
		tmp = digit;
	}
	else
	{
		/* we need at least one digit */
		goto slow;
	}

	/* process remaining digits */
	for (;;)
	{
		digit = (*ptr - '0');

		if (digit >= 10)
			break;

		ptr++;

		if (tmp > (uint32_t) -(INT32_MIN / 10))
			goto out_of_range;

		tmp = tmp * 10 + digit;
	}

	/* when the string does not end in a digit, let the slow path handle it */
	if (*ptr != '\0')
		goto slow;

	if (neg)
	{
		if (pg_neg_u32_overflow(tmp, &result))
			goto out_of_range;
		return result;
	}

	if (tmp > (uint32_t) INT32_MAX)
		goto out_of_range;

	return (int32_t) tmp;

slow:
	tmp = 0;
	ptr = s;
	/* no need to reset neg */

	/* skip leading spaces */
	while (isspace((unsigned char) *ptr))
		ptr++;

	/* handle sign */
	if (*ptr == '-')
	{
		ptr++;
		neg = true;
	}
	else if (*ptr == '+')
		ptr++;

	/* process digits */
	if (ptr[0] == '0' && (ptr[1] == 'x' || ptr[1] == 'X'))
	{
		firstdigit = ptr += 2;

		for (;;)
		{
			if (isxdigit((unsigned char) *ptr))
			{
				if (tmp > (uint32_t) -(INT32_MIN / 16))
					goto out_of_range;

				tmp = tmp * 16 + hexlookup[(unsigned char) *ptr++];
			}
			else if (*ptr == '_')
			{
				/* underscore must be followed by more digits */
				ptr++;
				if (*ptr == '\0' || !isxdigit((unsigned char) *ptr))
					goto invalid_syntax;
			}
			else
				break;
		}
	}
	else if (ptr[0] == '0' && (ptr[1] == 'o' || ptr[1] == 'O'))
	{
		firstdigit = ptr += 2;

		for (;;)
		{
			if (*ptr >= '0' && *ptr <= '7')
			{
				if (tmp > (uint32_t) -(INT32_MIN / 8))
					goto out_of_range;

				tmp = tmp * 8 + (*ptr++ - '0');
			}
			else if (*ptr == '_')
			{
				/* underscore must be followed by more digits */
				ptr++;
				if (*ptr == '\0' || *ptr < '0' || *ptr > '7')
					goto invalid_syntax;
			}
			else
				break;
		}
	}
	else if (ptr[0] == '0' && (ptr[1] == 'b' || ptr[1] == 'B'))
	{
		firstdigit = ptr += 2;

		for (;;)
		{
			if (*ptr >= '0' && *ptr <= '1')
			{
				if (tmp > (uint32_t) -(INT32_MIN / 2))
					goto out_of_range;

				tmp = tmp * 2 + (*ptr++ - '0');
			}
			else if (*ptr == '_')
			{
				/* underscore must be followed by more digits */
				ptr++;
				if (*ptr == '\0' || *ptr < '0' || *ptr > '1')
					goto invalid_syntax;
			}
			else
				break;
		}
	}
	else
	{
		firstdigit = ptr;

		for (;;)
		{
			if (*ptr >= '0' && *ptr <= '9')
			{
				if (tmp > (uint32_t) -(INT32_MIN / 10))
					goto out_of_range;

				tmp = tmp * 10 + (*ptr++ - '0');
			}
			else if (*ptr == '_')
			{
				/* underscore may not be first */
				if (ptr == firstdigit)
					goto invalid_syntax;
				/* and it must be followed by more digits */
				ptr++;
				if (*ptr == '\0' || !isdigit((unsigned char) *ptr))
					goto invalid_syntax;
			}
			else
				break;
		}
	}

	/* require at least one digit */
	if (ptr == firstdigit)
		goto invalid_syntax;

	/* allow trailing whitespace, but not other trailing chars */
	while (isspace((unsigned char) *ptr))
		ptr++;

	if (*ptr != '\0')
		goto invalid_syntax;

	if (neg)
	{
		if (pg_neg_u32_overflow(tmp, &result))
			goto out_of_range;
		return result;
	}

	if (tmp > (uint32_t) INT32_MAX)
		goto out_of_range;

	return (int32_t) tmp;

out_of_range:
	/* [S2] upstream: ereturn(escontext, 0, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg(...))) */
	*err_kind = 1;
	return 0;

invalid_syntax:
	/* [S2] upstream: ereturn(escontext, 0, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION), errmsg(...))) */
	*err_kind = 2;
	return 0;
}
