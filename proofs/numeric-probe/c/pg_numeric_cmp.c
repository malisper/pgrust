/*
 * Vendored PostgreSQL C for the numeric comparator family
 * (numeric_eq/ne/lt/le/gt/ge/cmp).
 *
 * Provenance: src/backend/utils/adt/numeric.c, branch REL_18_STABLE
 * (277122036c3382c5ab47034a180fde1176728c43), fetched 2026-07-28.
 *
 * Functions:
 *   - pg_cmp_abs_common   <- cmp_abs_common   (body VERBATIM)
 *   - pg_cmp_var_common   <- cmp_var_common   (body VERBATIM)
 *   - pg_cmp_numerics     <- cmp_numerics     (body verbatim EXCEPT the
 *     packed-header access shim below)
 *   - pg_numeric_eq/ne/lt/le/gt/ge <- the numeric_eq..ge fmgr bodies,
 *     which are single-expression PG_RETURN_BOOL(cmp_numerics(a,b) OP 0)
 *     wrappers; reproduced with the fmgr shim below.
 *
 * SHIMS (plumbing only, never logic):
 *   1. PACKED-HEADER ACCESS SHIM: cmp_numerics(Numeric num1, Numeric num2)
 *      reads the packed on-disk struct through macros
 *      (NUMERIC_IS_SPECIAL/IS_NAN/IS_PINF/IS_NINF and
 *      NUMERIC_DIGITS/NDIGITS/WEIGHT/SIGN).  Here each Numeric argument is
 *      replaced by the explicit tuple (sign, weight, digits[], ndigits)
 *      those macros produce, with the macro predicates rewritten on the
 *      explicit sign value exactly per the REL_18 macro definitions:
 *        NUMERIC_IS_SPECIAL(n) -> (sign & NUMERIC_SIGN_MASK) == NUMERIC_SPECIAL
 *        NUMERIC_IS_NAN(n)     -> sign == NUMERIC_NAN
 *        NUMERIC_IS_PINF(n)    -> sign == NUMERIC_PINF
 *        NUMERIC_IS_NINF(n)    -> sign == NUMERIC_NINF
 *      (NUMERIC_SIGN(n) returns NUMERIC_POS/NEG for finite values and the
 *      full NUMERIC_NAN/PINF/NINF code for specials, so one sign int is a
 *      faithful carrier.)  Consequence: the C side's packed-header DECODE
 *      (NumericShort/NumericLong bit extraction) is OUT of the theorem;
 *      the Rust side's decode (Num::sign/weight/ndigits/digits) is IN
 *      (the harness feeds Rust real packed images built per the on-disk
 *      spec, and feeds C the spec-level fields of the same value).
 *   2. FMGR SHIM: PG_FUNCTION_ARGS/PG_GETARG_NUMERIC/PG_RETURN_BOOL and
 *      the PG_FREE_IF_COPY calls (detoast bookkeeping, no value effect)
 *      are unwrapped to plain C signatures; detoasting is out of scope
 *      (pre-detoasted caller contract, bytea-cmp varlena precedent).
 *   3. Types: NumericDigit = int16 -> int16_t; dscale is not read by any
 *     comparator and is not passed.
 *
 * No other lines were changed.
 */

#include <stdint.h>

typedef int16_t NumericDigit;

#define NUMERIC_SIGN_MASK 0xC000
#define NUMERIC_POS 0x0000
#define NUMERIC_NEG 0x4000
#define NUMERIC_SPECIAL 0xC000
#define NUMERIC_NAN 0xC000
#define NUMERIC_PINF 0xD000
#define NUMERIC_NINF 0xF000

/* shim predicates: see SHIM 1 above */
#define NUMERIC_IS_SPECIAL_S(sign) (((sign) & NUMERIC_SIGN_MASK) == NUMERIC_SPECIAL)
#define NUMERIC_IS_NAN_S(sign) ((sign) == NUMERIC_NAN)
#define NUMERIC_IS_PINF_S(sign) ((sign) == NUMERIC_PINF)
#define NUMERIC_IS_NINF_S(sign) ((sign) == NUMERIC_NINF)

/* ----------
 * cmp_abs_common() — body VERBATIM from REL_18_STABLE numeric.c
 * ----------
 */
static int
pg_cmp_abs_common_impl(const NumericDigit *var1digits, int var1ndigits, int var1weight,
					   const NumericDigit *var2digits, int var2ndigits, int var2weight)
{
	int			i1 = 0;
	int			i2 = 0;

	/* Check any digits before the first common digit */

	while (var1weight > var2weight && i1 < var1ndigits)
	{
		if (var1digits[i1++] != 0)
			return 1;
		var1weight--;
	}
	while (var2weight > var1weight && i2 < var2ndigits)
	{
		if (var2digits[i2++] != 0)
			return -1;
		var2weight--;
	}

	/* At this point, either w1 == w2 or we've run out of digits */

	if (var1weight == var2weight)
	{
		while (i1 < var1ndigits && i2 < var2ndigits)
		{
			int			stat = var1digits[i1++] - var2digits[i2++];

			if (stat)
			{
				if (stat > 0)
					return 1;
				return -1;
			}
		}
	}

	/*
	 * At this point, we've run out of digits on one side or the other; so any
	 * remaining nonzero digits imply that side is larger
	 */
	while (i1 < var1ndigits)
	{
		if (var1digits[i1++] != 0)
			return 1;
	}
	while (i2 < var2ndigits)
	{
		if (var2digits[i2++] != 0)
			return -1;
	}

	return 0;
}

/* ----------
 * cmp_var_common() — body VERBATIM from REL_18_STABLE numeric.c
 * ----------
 */
int
pg_cmp_var_common(const NumericDigit *var1digits, int var1ndigits,
				  int var1weight, int var1sign,
				  const NumericDigit *var2digits, int var2ndigits,
				  int var2weight, int var2sign)
{
	if (var1ndigits == 0)
	{
		if (var2ndigits == 0)
			return 0;
		if (var2sign == NUMERIC_NEG)
			return 1;
		return -1;
	}
	if (var2ndigits == 0)
	{
		if (var1sign == NUMERIC_POS)
			return 1;
		return -1;
	}

	if (var1sign == NUMERIC_POS)
	{
		if (var2sign == NUMERIC_NEG)
			return 1;
		return pg_cmp_abs_common_impl(var1digits, var1ndigits, var1weight,
									  var2digits, var2ndigits, var2weight);
	}

	if (var2sign == NUMERIC_POS)
		return -1;

	return pg_cmp_abs_common_impl(var2digits, var2ndigits, var2weight,
								  var1digits, var1ndigits, var1weight);
}

/* ----------
 * cmp_numerics() — body verbatim from REL_18_STABLE numeric.c, with each
 * Numeric argument shimmed to (sign, weight, digits, ndigits) per SHIM 1.
 * ----------
 */
int
pg_cmp_numerics(int sign1, int weight1, const NumericDigit *digits1, int ndigits1,
				int sign2, int weight2, const NumericDigit *digits2, int ndigits2)
{
	int			result;

	/*
	 * We consider all NANs to be equal and larger than any non-NAN (including
	 * Infinity).  This is somewhat arbitrary; the important thing is to have
	 * a consistent sort order.
	 */
	if (NUMERIC_IS_SPECIAL_S(sign1))
	{
		if (NUMERIC_IS_NAN_S(sign1))
		{
			if (NUMERIC_IS_NAN_S(sign2))
				result = 0;		/* NAN = NAN */
			else
				result = 1;		/* NAN > non-NAN */
		}
		else if (NUMERIC_IS_PINF_S(sign1))
		{
			if (NUMERIC_IS_NAN_S(sign2))
				result = -1;	/* PINF < NAN */
			else if (NUMERIC_IS_PINF_S(sign2))
				result = 0;		/* PINF = PINF */
			else
				result = 1;		/* PINF > anything else */
		}
		else					/* num1 must be NINF */
		{
			if (NUMERIC_IS_NINF_S(sign2))
				result = 0;		/* NINF = NINF */
			else
				result = -1;	/* NINF < anything else */
		}
	}
	else if (NUMERIC_IS_SPECIAL_S(sign2))
	{
		if (NUMERIC_IS_NINF_S(sign2))
			result = 1;			/* normal > NINF */
		else
			result = -1;		/* normal < NAN or PINF */
	}
	else
	{
		result = pg_cmp_var_common(digits1, ndigits1, weight1, sign1,
								   digits2, ndigits2, weight2, sign2);
	}

	return result;
}

/*
 * numeric_eq..ge fmgr bodies (REL_18_STABLE numeric.c): each is
 * PG_RETURN_BOOL(cmp_numerics(num1, num2) OP 0) after PG_GETARG_NUMERIC +
 * PG_FREE_IF_COPY.  Reproduced under SHIM 2 (int return: 0/1; Kani lowers
 * Rust () oddly for void, and C bool needs no extra header this way).
 */
#define PG_NUMERIC_BOOL_OP(name, OP)											\
	int																			\
	name(int sign1, int weight1, const NumericDigit *digits1, int ndigits1,		\
		 int sign2, int weight2, const NumericDigit *digits2, int ndigits2)		\
	{																			\
		return pg_cmp_numerics(sign1, weight1, digits1, ndigits1,				\
							   sign2, weight2, digits2, ndigits2) OP 0;			\
	}

PG_NUMERIC_BOOL_OP(pg_numeric_eq, ==)
PG_NUMERIC_BOOL_OP(pg_numeric_ne, !=)
PG_NUMERIC_BOOL_OP(pg_numeric_lt, <)
PG_NUMERIC_BOOL_OP(pg_numeric_le, <=)
PG_NUMERIC_BOOL_OP(pg_numeric_gt, >)
PG_NUMERIC_BOOL_OP(pg_numeric_ge, >=)
