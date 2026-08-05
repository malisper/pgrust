/*
 * Vendored PostgreSQL C: money (cash) type — differential-fuzz oracle.
 *
 * Provenance (all bodies VERBATIM unless a shim is listed below):
 *   - src/backend/utils/adt/cash.c @ postgres-src
 *     62d6c7d3df6287f1bd83199c1a746e50d31571a0 (REL_18 / 18.3, the repo's
 *     vendored ground-truth checkout ../pgrust-fabled/vendor/postgres-src):
 *     cash_in, cash_out, cash_recv, cash_send, cash_eq/ne/lt/le/gt/ge,
 *     cash_cmp, cash_pl_cash (cash_pl), cash_mi_cash (cash_mi),
 *     cash_mul_int64 / cash_div_int64 cores + int8/int4/int2 wrappers,
 *     cash_mul_float8 / cash_div_float8 cores + flt8/flt4 wrappers,
 *     cash_div_cash, cashlarger, cashsmaller, append_num_word, cash_words,
 *     int4_cash, int8_cash.
 *   - src/include/common/int.h @ same ref: pg_add_s64_overflow,
 *     pg_sub_s64_overflow, pg_mul_s64_overflow (HAVE__BUILTIN_OP_OVERFLOW
 *     arm — the arm every production compiler takes), pg_abs_s64.
 *   - src/include/utils/float.h @ same ref: float8_mul, float8_div.
 *   - src/include/c.h @ same ref: FLOAT8_FITS_IN_INT64, PG_INT64_MIN.
 *   - src/backend/utils/adt/int8.c @ same ref: int8mul overflow core
 *     (int4_cash/int8_cash go through DirectFunctionCall2(int8mul)).
 *   This file follows the vendoring pattern of proofs/cash/c/pg_cash.c
 *   (same source rows, PROVENANCE-AUDIT 2026-07-28: zero drift vs
 *   REL_18_STABLE), re-shimmed for a native in-process oracle.
 *
 * ============================ C-LOCALE FENCE =============================
 * cash.c is locale-adjacent: every symbol comes from PGLC_localeconv().
 * The shipped Rust (pg_locale::pglc_localeconv) serves the static C-locale
 * PgLconv whenever lc_monetary/lc_numeric are "C" (the fuzz process never
 * sets the GUCs, so that arm is the only one live): all string symbols ""
 * and every char field CHAR_MAX (127). This oracle pins the same seam:
 * pg_cash_lconv_data below is a PGLC_localeconv() replacement carrying
 * exactly those C-locale values, so cash.c's own fallback arms ('.', ",",
 * "$", "+", "-", frac_digits->2, mon_grouping->3, the default sign_posn
 * case) are what BOTH sides execute. The shipped Rust exposes NO
 * fpoint/frac_digits parameter on cash_in/cash_out (they read the process
 * lconv), so non-default fpoint is not a reachable surface through the
 * shipped API and is deliberately not fuzzed here; proofs/cash/c/pg_cash.c
 * keeps a settable seam (pg_proof_set_lconv) for the Kani harnesses that
 * quantify over those fields. isspace/isdigit are shimmed to the
 * ASCII/C-locale sets (below) so the oracle cannot drift with the host
 * process locale; the shipped Rust is_space/is_ascii_digit are exactly
 * those sets.
 * =========================================================================
 *
 * Shims (plumbing only, never logic):
 *   - fmgr PG_FUNCTION_ARGS unwrapping -> plain C signatures
 *     (PG_GETARG_CASH -> int64 params, PG_RETURN_* -> return / out-param).
 *   - ereport/ereturn -> int error-class return codes, at the exact ereport
 *     program point; message text never crosses the seam:
 *       1 = ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE   (22003)
 *       2 = ERRCODE_DIVISION_BY_ZERO             (22012)
 *       3 = ERRCODE_INVALID_TEXT_REPRESENTATION  (22P02)
 *       4 = ERRCODE_PROTOCOL_VIOLATION           (08P01)
 *     (0 = success. Unlike pg_float_io.c's _Thread_local errcode — needed
 *     there because float8in_internal's verbatim body contains ereturn
 *     macros returning values — every cash body here already returns
 *     through a wrapper seam, so the error class rides the return value
 *     directly, the same convention as pg_diff_float_math.)
 *   - psprintf / StringInfo appends -> pg_cash_cat() into a caller-provided
 *     buffer, decomposing each format string into its literal/argument
 *     sequence IN ORDER (e.g. psprintf("(%s%s%s)", a, b, c) becomes
 *     cat("(") cat(a) cat(b) cat(c) cat(")")). Composition order and the
 *     conditional-space expressions are verbatim.
 *   - pq_getmsgint64/pq_sendint64 (pqformat.c): modeled as the big-endian
 *     8-byte read (with pq_getmsgbytes' insufficient-data check -> err 4)
 *     and write; the cash.c bodies are one-liners over that machinery.
 *   - pg_toupper (ascii path) -> pg_cash_toupper below.
 *   - i64abs -> the llabs identity spelled inline in pg_abs_s64 (verbatim
 *     modulo the libc call).
 *
 * KNOWN-DIVERGENCE NOTE (proofs ledger rows 865/867/3345): C cash_div_int64
 * has NO INT64_MIN / -1 guard (the 2024 money-overflow sweep 4f96281587
 * covered multiply and the float paths only), so that cell is
 * platform-dependent UB in C (x86-64 traps SIGFPE, aarch64 silently yields
 * INT64_MIN); pgrust raises 22003 like int8div (ruling 2026-07-29, reported
 * upstream). The Rust driver carves EXACTLY that cell and never calls this
 * oracle with it, keeping the oracle run UB-free.
 */

#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <math.h>
#include <stdbool.h>

typedef int16_t int16;
typedef int32_t int32;
typedef int64_t int64;
typedef uint8_t uint8;
typedef uint64_t uint64;
typedef int64 Cash;
typedef double float8;
typedef float float4;

#define unlikely(x) (x)

#define PG_INT64_MIN	(-INT64_C(0x7FFFFFFFFFFFFFFF) - 1)

/* c.h, verbatim */
#define FLOAT8_FITS_IN_INT64(num) \
	((num) >= (float8) PG_INT64_MIN && (num) < -((float8) PG_INT64_MIN))

/* error-class codes (see header comment) */
#define PG_CASH_ERR_NUM_OOR		1
#define PG_CASH_ERR_DIV_ZERO	2
#define PG_CASH_ERR_SYNTAX		3
#define PG_CASH_ERR_PROTOCOL	4

/* ---------- common/int.h: HAVE__BUILTIN_OP_OVERFLOW arm ---------- */

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

static inline uint64
pg_abs_s64(int64 a)
{
	if (unlikely(a == PG_INT64_MIN))
		return (uint64) PG_INT64_MIN;
	return (uint64) (a < 0 ? -a : a);	/* i64abs(a) */
}

/* ---------- C-locale ctype fence (see header) ---------- */

static inline int
pg_cash_isspace(unsigned char c)
{
	return c == ' ' || c == '\t' || c == '\n' || c == '\v' || c == '\f' || c == '\r';
}

static inline int
pg_cash_isdigit(unsigned char c)
{
	return c >= '0' && c <= '9';
}

static inline int
pg_cash_toupper(int c)
{
	return (c >= 'a' && c <= 'z') ? c - ('a' - 'A') : c;
}

#define isspace(c) pg_cash_isspace(c)
#define isdigit(c) pg_cash_isdigit(c)

/* ---------- locale seam: PGLC_localeconv() PINNED to the C locale ---------- */

struct pg_cash_lconv
{
	char	   *mon_decimal_point;
	char	   *mon_thousands_sep;
	char	   *mon_grouping;
	char	   *positive_sign;
	char	   *negative_sign;
	char	   *currency_symbol;
	signed char frac_digits;
	signed char p_cs_precedes;
	signed char n_cs_precedes;
	signed char p_sep_by_space;
	signed char n_sep_by_space;
	signed char p_sign_posn;
	signed char n_sign_posn;
};

static char pg_cash_empty[1] = "";

/* CHAR_MAX (127) char fields + "" symbols = what C-locale localeconv()
 * returns and exactly what pg_locale's C_LOCALE_LCONV carries. */
static struct pg_cash_lconv pg_cash_lconv_data = {
	pg_cash_empty, pg_cash_empty, pg_cash_empty,
	pg_cash_empty, pg_cash_empty, pg_cash_empty,
	127, 127, 127, 127, 127, 127, 127
};

#define lconv pg_cash_lconv
#define PGLC_localeconv() (&pg_cash_lconv_data)

/* ---------- utils/float.h: float8_mul / float8_div ----------
 * ereport shims: float_overflow_error()/float_underflow_error() ->
 * *err = OOR; float_zero_divide_error() -> *err = DIV_ZERO; each followed
 * by `return 0` (the C error functions are noreturn; callers below check
 * *err before using the value).
 */

static inline float8
pg_cash_float8_mul(const float8 val1, const float8 val2, int *err)
{
	float8		result;

	result = val1 * val2;
	if (unlikely(isinf(result)) && !isinf(val1) && !isinf(val2))
	{
		*err = PG_CASH_ERR_NUM_OOR;		/* float_overflow_error() */
		return 0;
	}
	if (unlikely(result == 0.0) && val1 != 0.0 && val2 != 0.0)
	{
		*err = PG_CASH_ERR_NUM_OOR;		/* float_underflow_error() */
		return 0;
	}

	return result;
}

static inline float8
pg_cash_float8_div(const float8 val1, const float8 val2, int *err)
{
	float8		result;

	if (unlikely(val2 == 0.0) && !isnan(val1))
	{
		*err = PG_CASH_ERR_DIV_ZERO;	/* float_zero_divide_error() */
		return 0;
	}
	result = val1 / val2;
	if (unlikely(isinf(result)) && !isinf(val1))
	{
		*err = PG_CASH_ERR_NUM_OOR;		/* float_overflow_error() */
		return 0;
	}
	if (unlikely(result == 0.0) && val1 != 0.0 && !isinf(val2))
	{
		*err = PG_CASH_ERR_NUM_OOR;		/* float_underflow_error() */
		return 0;
	}

	return result;
}

/* ---------- cash.c: cash_in (verbatim; ereturn -> err codes) ---------- */

int64
pg_diff_cash_in(const char *str, int *err)
{
	Cash		result;
	Cash		value = 0;
	Cash		dec = 0;
	Cash		sgn = 1;
	bool		seen_dot = false;
	const char *s = str;
	int			fpoint;
	char		dsymbol;
	const char *ssymbol,
			   *psymbol,
			   *nsymbol,
			   *csymbol;
	struct lconv *lconvert = PGLC_localeconv();

	*err = 0;

	fpoint = lconvert->frac_digits;
	if (fpoint < 0 || fpoint > 10)
		fpoint = 2;				/* best guess in this case, I think */

	/* we restrict dsymbol to be a single byte, but not the other symbols */
	if (*lconvert->mon_decimal_point != '\0' &&
		lconvert->mon_decimal_point[1] == '\0')
		dsymbol = *lconvert->mon_decimal_point;
	else
		dsymbol = '.';
	if (*lconvert->mon_thousands_sep != '\0')
		ssymbol = lconvert->mon_thousands_sep;
	else						/* ssymbol should not equal dsymbol */
		ssymbol = (dsymbol != ',') ? "," : ".";
	csymbol = (*lconvert->currency_symbol != '\0') ? lconvert->currency_symbol : "$";
	psymbol = (*lconvert->positive_sign != '\0') ? lconvert->positive_sign : "+";
	nsymbol = (*lconvert->negative_sign != '\0') ? lconvert->negative_sign : "-";

	/* we need to add all sorts of checking here.  For now just */
	/* strip all leading whitespace and any leading currency symbol */
	while (isspace((unsigned char) *s))
		s++;
	if (strncmp(s, csymbol, strlen(csymbol)) == 0)
		s += strlen(csymbol);
	while (isspace((unsigned char) *s))
		s++;

	/* a leading minus or paren signifies a negative number */
	/* again, better heuristics needed */
	/* XXX - doesn't properly check for balanced parens - djmc */
	if (strncmp(s, nsymbol, strlen(nsymbol)) == 0)
	{
		sgn = -1;
		s += strlen(nsymbol);
	}
	else if (*s == '(')
	{
		sgn = -1;
		s++;
	}
	else if (strncmp(s, psymbol, strlen(psymbol)) == 0)
		s += strlen(psymbol);

	/* allow whitespace and currency symbol after the sign, too */
	while (isspace((unsigned char) *s))
		s++;
	if (strncmp(s, csymbol, strlen(csymbol)) == 0)
		s += strlen(csymbol);
	while (isspace((unsigned char) *s))
		s++;

	for (; *s; s++)
	{
		/*
		 * We look for digits as long as we have found less than the required
		 * number of decimal places.
		 */
		if (isdigit((unsigned char) *s) && (!seen_dot || dec < fpoint))
		{
			int8_t		digit = *s - '0';

			if (pg_mul_s64_overflow(value, 10, &value) ||
				pg_sub_s64_overflow(value, digit, &value))
			{
				*err = PG_CASH_ERR_NUM_OOR;		/* ereturn "value out of range" */
				return 0;
			}

			if (seen_dot)
				dec++;
		}
		/* decimal point? then start counting fractions... */
		else if (*s == dsymbol && !seen_dot)
		{
			seen_dot = true;
		}
		/* ignore if "thousands" separator, else we're done */
		else if (strncmp(s, ssymbol, strlen(ssymbol)) == 0)
			s += strlen(ssymbol) - 1;
		else
			break;
	}

	/* round off if there's another digit */
	if (isdigit((unsigned char) *s) && *s >= '5')
	{
		/* remember we build the value in the negative */
		if (pg_sub_s64_overflow(value, 1, &value))
		{
			*err = PG_CASH_ERR_NUM_OOR;			/* ereturn "value out of range" */
			return 0;
		}
	}

	/* adjust for less than required decimal places */
	for (; dec < fpoint; dec++)
	{
		if (pg_mul_s64_overflow(value, 10, &value))
		{
			*err = PG_CASH_ERR_NUM_OOR;			/* ereturn "value out of range" */
			return 0;
		}
	}

	/*
	 * should only be trailing digits followed by whitespace, right paren,
	 * trailing sign, and/or trailing currency symbol
	 */
	while (isdigit((unsigned char) *s))
		s++;

	while (*s)
	{
		if (isspace((unsigned char) *s) || *s == ')')
			s++;
		else if (strncmp(s, nsymbol, strlen(nsymbol)) == 0)
		{
			sgn = -1;
			s += strlen(nsymbol);
		}
		else if (strncmp(s, psymbol, strlen(psymbol)) == 0)
			s += strlen(psymbol);
		else if (strncmp(s, csymbol, strlen(csymbol)) == 0)
			s += strlen(csymbol);
		else
		{
			*err = PG_CASH_ERR_SYNTAX;	/* ereturn "invalid input syntax" */
			return 0;
		}
	}

	/*
	 * If the value is supposed to be positive, flip the sign, but check for
	 * the most negative number.
	 */
	if (sgn > 0)
	{
		if (value == PG_INT64_MIN)
		{
			*err = PG_CASH_ERR_NUM_OOR;			/* ereturn "value out of range" */
			return 0;
		}
		result = -value;
	}
	else
		result = value;

	return result;
}

/* ---------- cash.c: cash_out (verbatim; psprintf -> pg_cash_cat) ---------- */

static char *
pg_cash_cat(char *dst, const char *s)
{
	while (*s)
		*dst++ = *s++;
	return dst;
}

int
pg_diff_cash_out(int64 value, char *out)
{
	uint64		uvalue;
	char		buf[128];
	char	   *bufptr;
	int			digit_pos;
	int			points,
				mon_group;
	char		dsymbol;
	const char *ssymbol,
			   *csymbol,
			   *signsymbol;
	char		sign_posn,
				cs_precedes,
				sep_by_space;
	char	   *p = out;
	struct lconv *lconvert = PGLC_localeconv();

	/* see comments about frac_digits in cash_in() */
	points = lconvert->frac_digits;
	if (points < 0 || points > 10)
		points = 2;				/* best guess in this case, I think */

	/*
	 * As with frac_digits, must apply a range check to mon_grouping to avoid
	 * being fooled by variant CHAR_MAX values.
	 */
	mon_group = *lconvert->mon_grouping;
	if (mon_group <= 0 || mon_group > 6)
		mon_group = 3;

	/* we restrict dsymbol to be a single byte, but not the other symbols */
	if (*lconvert->mon_decimal_point != '\0' &&
		lconvert->mon_decimal_point[1] == '\0')
		dsymbol = *lconvert->mon_decimal_point;
	else
		dsymbol = '.';
	if (*lconvert->mon_thousands_sep != '\0')
		ssymbol = lconvert->mon_thousands_sep;
	else						/* ssymbol should not equal dsymbol */
		ssymbol = (dsymbol != ',') ? "," : ".";
	csymbol = (*lconvert->currency_symbol != '\0') ? lconvert->currency_symbol : "$";

	if (value < 0)
	{
		/* set up formatting data */
		signsymbol = (*lconvert->negative_sign != '\0') ? lconvert->negative_sign : "-";
		sign_posn = lconvert->n_sign_posn;
		cs_precedes = lconvert->n_cs_precedes;
		sep_by_space = lconvert->n_sep_by_space;
	}
	else
	{
		signsymbol = lconvert->positive_sign;
		sign_posn = lconvert->p_sign_posn;
		cs_precedes = lconvert->p_cs_precedes;
		sep_by_space = lconvert->p_sep_by_space;
	}

	/* make the amount positive for digit-reconstruction loop */
	uvalue = pg_abs_s64(value);

	/* we build the digits+decimal-point+sep string right-to-left in buf[] */
	bufptr = buf + sizeof(buf) - 1;
	*bufptr = '\0';

	/*
	 * Generate digits till there are no non-zero digits left and we emitted
	 * at least one to the left of the decimal point.
	 */
	digit_pos = points;
	do
	{
		if (points && digit_pos == 0)
		{
			/* insert decimal point, but not if value cannot be fractional */
			*(--bufptr) = dsymbol;
		}
		else if (digit_pos < 0 && (digit_pos % mon_group) == 0)
		{
			/* insert thousands sep, but only to left of radix point */
			bufptr -= strlen(ssymbol);
			memcpy(bufptr, ssymbol, strlen(ssymbol));
		}

		*(--bufptr) = (uvalue % 10) + '0';
		uvalue = uvalue / 10;
		digit_pos--;
	} while (uvalue || digit_pos >= 0);

	/* psprintf cases decomposed into ordered pg_cash_cat appends (shim) */
	switch (sign_posn)
	{
		case 0:
			if (cs_precedes)
			{
				/* psprintf("(%s%s%s)", csymbol, sep1?, bufptr) */
				p = pg_cash_cat(p, "(");
				p = pg_cash_cat(p, csymbol);
				p = pg_cash_cat(p, (sep_by_space == 1) ? " " : "");
				p = pg_cash_cat(p, bufptr);
				p = pg_cash_cat(p, ")");
			}
			else
			{
				p = pg_cash_cat(p, "(");
				p = pg_cash_cat(p, bufptr);
				p = pg_cash_cat(p, (sep_by_space == 1) ? " " : "");
				p = pg_cash_cat(p, csymbol);
				p = pg_cash_cat(p, ")");
			}
			break;
		case 1:
		default:
			if (cs_precedes)
			{
				p = pg_cash_cat(p, signsymbol);
				p = pg_cash_cat(p, (sep_by_space == 2) ? " " : "");
				p = pg_cash_cat(p, csymbol);
				p = pg_cash_cat(p, (sep_by_space == 1) ? " " : "");
				p = pg_cash_cat(p, bufptr);
			}
			else
			{
				p = pg_cash_cat(p, signsymbol);
				p = pg_cash_cat(p, (sep_by_space == 2) ? " " : "");
				p = pg_cash_cat(p, bufptr);
				p = pg_cash_cat(p, (sep_by_space == 1) ? " " : "");
				p = pg_cash_cat(p, csymbol);
			}
			break;
		case 2:
			if (cs_precedes)
			{
				p = pg_cash_cat(p, csymbol);
				p = pg_cash_cat(p, (sep_by_space == 1) ? " " : "");
				p = pg_cash_cat(p, bufptr);
				p = pg_cash_cat(p, (sep_by_space == 2) ? " " : "");
				p = pg_cash_cat(p, signsymbol);
			}
			else
			{
				p = pg_cash_cat(p, bufptr);
				p = pg_cash_cat(p, (sep_by_space == 1) ? " " : "");
				p = pg_cash_cat(p, csymbol);
				p = pg_cash_cat(p, (sep_by_space == 2) ? " " : "");
				p = pg_cash_cat(p, signsymbol);
			}
			break;
		case 3:
			if (cs_precedes)
			{
				p = pg_cash_cat(p, signsymbol);
				p = pg_cash_cat(p, (sep_by_space == 2) ? " " : "");
				p = pg_cash_cat(p, csymbol);
				p = pg_cash_cat(p, (sep_by_space == 1) ? " " : "");
				p = pg_cash_cat(p, bufptr);
			}
			else
			{
				p = pg_cash_cat(p, bufptr);
				p = pg_cash_cat(p, (sep_by_space == 1) ? " " : "");
				p = pg_cash_cat(p, signsymbol);
				p = pg_cash_cat(p, (sep_by_space == 2) ? " " : "");
				p = pg_cash_cat(p, csymbol);
			}
			break;
		case 4:
			if (cs_precedes)
			{
				p = pg_cash_cat(p, csymbol);
				p = pg_cash_cat(p, (sep_by_space == 2) ? " " : "");
				p = pg_cash_cat(p, signsymbol);
				p = pg_cash_cat(p, (sep_by_space == 1) ? " " : "");
				p = pg_cash_cat(p, bufptr);
			}
			else
			{
				p = pg_cash_cat(p, bufptr);
				p = pg_cash_cat(p, (sep_by_space == 1) ? " " : "");
				p = pg_cash_cat(p, csymbol);
				p = pg_cash_cat(p, (sep_by_space == 2) ? " " : "");
				p = pg_cash_cat(p, signsymbol);
			}
			break;
	}

	return (int) (p - out);
}

/* ---------- cash.c: comparison functions ---------- */

int
pg_diff_cash_eq(Cash c1, Cash c2)
{
	return (c1 == c2);
}

int
pg_diff_cash_ne(Cash c1, Cash c2)
{
	return (c1 != c2);
}

int
pg_diff_cash_lt(Cash c1, Cash c2)
{
	return (c1 < c2);
}

int
pg_diff_cash_le(Cash c1, Cash c2)
{
	return (c1 <= c2);
}

int
pg_diff_cash_gt(Cash c1, Cash c2)
{
	return (c1 > c2);
}

int
pg_diff_cash_ge(Cash c1, Cash c2)
{
	return (c1 >= c2);
}

int
pg_diff_cash_cmp(Cash c1, Cash c2)
{
	if (c1 > c2)
		return 1;
	else if (c1 == c2)
		return 0;
	else
		return -1;
}

/* ---------- cash.c: cash_pl_cash / cash_mi_cash ---------- */

int
pg_diff_cash_pl(Cash c1, Cash c2, Cash *result)
{
	Cash		res;

	if (unlikely(pg_add_s64_overflow(c1, c2, &res)))
		return PG_CASH_ERR_NUM_OOR;		/* ereport "money out of range" */

	*result = res;
	return 0;
}

int
pg_diff_cash_mi(Cash c1, Cash c2, Cash *result)
{
	Cash		res;

	if (unlikely(pg_sub_s64_overflow(c1, c2, &res)))
		return PG_CASH_ERR_NUM_OOR;		/* ereport "money out of range" */

	*result = res;
	return 0;
}

/* ---------- cash.c: cash_mul_float8 / cash_div_float8 cores ---------- */

static inline int
pg_diff_cash_mul_float8_core(Cash c, float8 f, Cash *out)
{
	int			err = 0;
	float8		res = rint(pg_cash_float8_mul((float8) c, f, &err));

	if (err)
		return err;
	if (unlikely(isnan(res) || !FLOAT8_FITS_IN_INT64(res)))
		return PG_CASH_ERR_NUM_OOR;		/* "money out of range" */

	*out = (Cash) res;
	return 0;
}

static inline int
pg_diff_cash_div_float8_core(Cash c, float8 f, Cash *out)
{
	int			err = 0;
	float8		res = rint(pg_cash_float8_div((float8) c, f, &err));

	if (err)
		return err;
	if (unlikely(isnan(res) || !FLOAT8_FITS_IN_INT64(res)))
		return PG_CASH_ERR_NUM_OOR;		/* "money out of range" */

	*out = (Cash) res;
	return 0;
}

/* fmgr wrappers (flt4 forms cast to float8 exactly as the C wrappers do) */

int
pg_diff_cash_mul_flt8(int64 c, float8 f, int64 *out)
{
	return pg_diff_cash_mul_float8_core(c, f, out);
}

int
pg_diff_cash_div_flt8(int64 c, float8 f, int64 *out)
{
	return pg_diff_cash_div_float8_core(c, f, out);
}

int
pg_diff_cash_mul_flt4(int64 c, float4 f, int64 *out)
{
	return pg_diff_cash_mul_float8_core(c, (float8) f, out);
}

int
pg_diff_cash_div_flt4(int64 c, float4 f, int64 *out)
{
	return pg_diff_cash_div_float8_core(c, (float8) f, out);
}

/* ---------- cash.c: cash_mul_int64 / cash_div_int64 cores ---------- */

int
pg_diff_cash_mul_int64(int64 c, int64 i, int64 *out)
{
	Cash		res;

	if (unlikely(pg_mul_s64_overflow(c, i, &res)))
		return PG_CASH_ERR_NUM_OOR;		/* "money out of range" */

	*out = res;
	return 0;
}

/* NOTE: verbatim = NO INT64_MIN / -1 guard; the Rust driver carves that
 * exact cell (ledger rows 865/867/3345) and never calls this with it. */
int
pg_diff_cash_div_int64(int64 c, int64 i, int64 *out)
{
	if (unlikely(i == 0))
		return PG_CASH_ERR_DIV_ZERO;	/* "division by zero" */

	*out = c / i;
	return 0;
}

/* int-width fmgr wrappers: the casts are the wrappers' own code, verbatim */

int
pg_diff_cash_mul_int4(int64 c, int32 i, int64 *out)
{
	return pg_diff_cash_mul_int64(c, (int64) i, out);
}

int
pg_diff_cash_div_int4(int64 c, int32 i, int64 *out)
{
	return pg_diff_cash_div_int64(c, (int64) i, out);
}

int
pg_diff_cash_mul_int2(int64 c, int16 s, int64 *out)
{
	return pg_diff_cash_mul_int64(c, (int64) s, out);
}

int
pg_diff_cash_div_int2(int64 c, int16 s, int64 *out)
{
	return pg_diff_cash_div_int64(c, (int64) s, out);
}

/* ---------- cash.c: cash_div_cash ---------- */

int
pg_diff_cash_div_cash(int64 dividend, int64 divisor, float8 *out)
{
	float8		quotient;

	if (divisor == 0)
		return PG_CASH_ERR_DIV_ZERO;	/* "division by zero" */

	quotient = (float8) dividend / (float8) divisor;
	*out = quotient;
	return 0;
}

/* ---------- cash.c: cashlarger / cashsmaller ---------- */

int64
pg_diff_cashlarger(Cash c1, Cash c2)
{
	Cash		result;

	result = (c1 > c2) ? c1 : c2;

	return result;
}

int64
pg_diff_cashsmaller(Cash c1, Cash c2)
{
	Cash		result;

	result = (c1 < c2) ? c1 : c2;

	return result;
}

/* ---------- int8.c: int8mul core + cash.c int4_cash/int8_cash ---------- */

static inline int
pg_cash_int8mul(int64 arg1, int64 arg2, int64 *result)
{
	if (unlikely(pg_mul_s64_overflow(arg1, arg2, result)))
		return PG_CASH_ERR_NUM_OOR;		/* "bigint out of range" */
	return 0;
}

int
pg_diff_int8_cash(int64 amount, int64 *out)
{
	int			fpoint;
	int64		scale;
	int			i;
	struct lconv *lconvert = PGLC_localeconv();

	/* see comments about frac_digits in cash_in() */
	fpoint = lconvert->frac_digits;
	if (fpoint < 0 || fpoint > 10)
		fpoint = 2;

	/* compute required scale factor */
	scale = 1;
	for (i = 0; i < fpoint; i++)
		scale *= 10;

	/* compute amount * scale, checking for overflow */
	return pg_cash_int8mul(amount, scale, out);
}

int
pg_diff_int4_cash(int32 amount, int64 *out)
{
	/* identical body modulo the arg width; C routes both through int8mul */
	return pg_diff_int8_cash((int64) amount, out);
}

/* ---------- cash.c: append_num_word + cash_words ----------
 * StringInfo appends / appendStringInfo format strings decomposed into
 * ordered pg_cash_cat appends (shim; wording verbatim).
 */

static char *
pg_cash_append_num_word(char *p, Cash value)
{
	static const char *const small[] = {
		"zero", "one", "two", "three", "four", "five", "six", "seven",
		"eight", "nine", "ten", "eleven", "twelve", "thirteen", "fourteen",
		"fifteen", "sixteen", "seventeen", "eighteen", "nineteen", "twenty",
		"thirty", "forty", "fifty", "sixty", "seventy", "eighty", "ninety"
	};
	const char *const *big = small + 18;
	int			tu = value % 100;

	/* deal with the simple cases first */
	if (value <= 20)
		return pg_cash_cat(p, small[value]);

	/* is it an even multiple of 100? */
	if (!tu)
	{
		/* appendStringInfo(buf, "%s hundred", small[value / 100]) */
		p = pg_cash_cat(p, small[value / 100]);
		return pg_cash_cat(p, " hundred");
	}

	/* more than 99? */
	if (value > 99)
	{
		/* is it an even multiple of 10 other than 10? */
		if (value % 10 == 0 && tu > 10)
		{
			/* "%s hundred %s", small[value/100], big[tu/10] */
			p = pg_cash_cat(p, small[value / 100]);
			p = pg_cash_cat(p, " hundred ");
			p = pg_cash_cat(p, big[tu / 10]);
		}
		else if (tu < 20)
		{
			/* "%s hundred and %s", small[value/100], small[tu] */
			p = pg_cash_cat(p, small[value / 100]);
			p = pg_cash_cat(p, " hundred and ");
			p = pg_cash_cat(p, small[tu]);
		}
		else
		{
			/* "%s hundred %s %s", small[value/100], big[tu/10], small[tu%10] */
			p = pg_cash_cat(p, small[value / 100]);
			p = pg_cash_cat(p, " hundred ");
			p = pg_cash_cat(p, big[tu / 10]);
			p = pg_cash_cat(p, " ");
			p = pg_cash_cat(p, small[tu % 10]);
		}
	}
	else
	{
		/* is it an even multiple of 10 other than 10? */
		if (value % 10 == 0 && tu > 10)
			p = pg_cash_cat(p, big[tu / 10]);
		else if (tu < 20)
			p = pg_cash_cat(p, small[tu]);
		else
		{
			/* "%s %s", big[tu/10], small[tu%10] */
			p = pg_cash_cat(p, big[tu / 10]);
			p = pg_cash_cat(p, " ");
			p = pg_cash_cat(p, small[tu % 10]);
		}
	}
	return p;
}

int
pg_diff_cash_words(int64 value_in, char *out)
{
	Cash		value = value_in;
	uint64		val;
	char	   *p = out;
	Cash		dollars;
	Cash		m0;
	Cash		m1;
	Cash		m2;
	Cash		m3;
	Cash		m4;
	Cash		m5;
	Cash		m6;

	/* work with positive numbers */
	if (value < 0)
	{
		/* C negates under -fwrapv (pinned by build.rs): INT64_MIN wraps */
		value = -value;
		p = pg_cash_cat(p, "minus ");
	}

	/* Now treat as unsigned, to avoid trouble at INT_MIN */
	val = (uint64) value;

	dollars = val / INT64_C(100);
	m0 = val % INT64_C(100);	/* cents */
	m1 = (val / INT64_C(100)) % 1000;	/* hundreds */
	m2 = (val / INT64_C(100000)) % 1000;	/* thousands */
	m3 = (val / INT64_C(100000000)) % 1000; /* millions */
	m4 = (val / INT64_C(100000000000)) % 1000;	/* billions */
	m5 = (val / INT64_C(100000000000000)) % 1000;	/* trillions */
	m6 = (val / INT64_C(100000000000000000)) % 1000;	/* quadrillions */

	if (m6)
	{
		p = pg_cash_append_num_word(p, m6);
		p = pg_cash_cat(p, " quadrillion ");
	}

	if (m5)
	{
		p = pg_cash_append_num_word(p, m5);
		p = pg_cash_cat(p, " trillion ");
	}

	if (m4)
	{
		p = pg_cash_append_num_word(p, m4);
		p = pg_cash_cat(p, " billion ");
	}

	if (m3)
	{
		p = pg_cash_append_num_word(p, m3);
		p = pg_cash_cat(p, " million ");
	}

	if (m2)
	{
		p = pg_cash_append_num_word(p, m2);
		p = pg_cash_cat(p, " thousand ");
	}

	if (m1)
		p = pg_cash_append_num_word(p, m1);

	if (dollars == 0)
		p = pg_cash_cat(p, "zero");

	p = pg_cash_cat(p, dollars == 1 ? " dollar and " : " dollars and ");
	p = pg_cash_append_num_word(p, m0);
	p = pg_cash_cat(p, m0 == 1 ? " cent" : " cents");

	/* capitalize output */
	out[0] = pg_cash_toupper((unsigned char) out[0]);

	return (int) (p - out);
}

/* ---------- cash.c: cash_recv / cash_send (pqformat model) ---------- */

int
pg_diff_cash_recv(const uint8 *buf, uint64 len, int64 *out)
{
	uint64		u = 0;
	int			i;

	/* pq_getmsgbytes: insufficient data -> ereport(ERRCODE_PROTOCOL_VIOLATION) */
	if (len < 8)
		return PG_CASH_ERR_PROTOCOL;
	/* pg_ntoh64 over the next 8 message bytes */
	for (i = 0; i < 8; i++)
		u = (u << 8) | buf[i];
	*out = (int64) u;
	return 0;
}

int
pg_diff_cash_send(int64 arg1, uint8 *out8)
{
	int			i;

	/* pq_sendint64: big-endian image of the 8-byte value */
	for (i = 0; i < 8; i++)
		out8[i] = (uint8) (((uint64) arg1) >> (8 * (7 - i)));
	return 0;
}
