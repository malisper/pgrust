/*
 * Vendored PostgreSQL C for the FIRST numeric-arithmetic equivalence
 * theorems: add_var / sub_var (with add_abs / sub_abs / cmp_abs /
 * cmp_abs_common / strip_var / zero_var) and mul_var's zero + short-kernel
 * planes (mul_var_short).
 *
 * Rust side (shipped code, path-dep): adt_numeric::fixed::{add_var_fixed,
 * sub_var_fixed, mul_var_fixed} on FixedVar<N> — the fixed-buffer mirrors
 * landed 38f13030d1 (crates/backend/utils/adt/numeric/src/fixed.rs), built
 * because the allocating kernels' DigitBuf::realloc_uninit is a measured
 * symex wall (proofs/TRIAGE.md numeric-arithmetic wall entry).  The mirrors
 * are line-for-line the allocating kernels; the native mirror-consistency
 * test in src/lib.rs ties them to the shipped allocating add_var/sub_var/
 * mul_var at the tested tier.
 *
 * Provenance: src/backend/utils/adt/numeric.c, branch REL_18_STABLE,
 * fetched 2026-07-29 (same ref as ../numeric-probe/c/*).  Bodies verbatim
 * except the shims below.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * SHIMS (plumbing only, never logic; each marked at its site):
 *  A1. digitbuf_alloc -> bump pointer into a static NumericDigit arena
 *      reset at each entry point; digitbuf_free -> no-op (family
 *      convention, ../numeric-probe/c/pg_numeric_tail.c SHIM T4).  Arena
 *      overflow surfaces as a CBMC out-of-bounds violation.
 *  A2. ENTRY-POINT SHIM: the NumericVar inputs are built from explicit
 *      (sign, weight, dscale, digits, ndigits) parameters and the result
 *      var is copied to explicit out-params — pointer plumbing around the
 *      verbatim kernels (the harness feeds both sides the same spec tuple).
 *  A3. MUL PLANE FENCE (loud trap, NOT a shim of logic): mul_var is
 *      vendored verbatim through its zero shortcut and its delegation test
 *      `var1ndigits <= 6 && rscale == var1->dscale + var2->dscale`; the
 *      full pairwise base-NBASE^2 kernel that follows (TLS scratch in the
 *      shipped Rust; out of the fixed mirror's scope by design —
 *      mul_var_fixed returns None there) is REPLACED by `*trap = 1;
 *      return;`.  Every harness asserts trap == 0, so any input escaping
 *      the fenced plane is a LOUD verification failure on both sides
 *      (mul_var_fixed None -> Rust panic; trap -> C-side assert).
 *  Types: int16/uint32 etc. typedef'd locally; Assert -> no-op;
 *  Max -> macro.  Postgres compiles with -fwrapv; CBMC's two's-complement
 *  wrap matches (skill note) — int arithmetic left exactly as upstream.
 */

#include <stdint.h>
#include <stddef.h>

typedef int16_t int16;
typedef int32_t int32;
typedef uint16_t uint16;
typedef uint32_t uint32;
typedef uint64_t uint64;

#define Assert(x) ((void) 0)
#define Max(x, y) ((x) > (y) ? (x) : (y))

#define NBASE		10000
#define DEC_DIGITS	4

typedef int16 NumericDigit;

#define NUMERIC_POS			0x0000
#define NUMERIC_NEG			0x4000

typedef struct NumericVar
{
	int			ndigits;
	int			weight;
	int			sign;
	int			dscale;
	NumericDigit *buf;
	NumericDigit *digits;
} NumericVar;

/* ---- SHIM A1: digit-buffer arena ---- */

#define ARITH_DIGIT_ARENA 64
static NumericDigit arith_digit_arena[ARITH_DIGIT_ARENA];
static int	arith_digit_next = 0;

static NumericDigit *
digitbuf_alloc(int ndigits)
{
	NumericDigit *p = &arith_digit_arena[arith_digit_next];

	arith_digit_next += ndigits;
	return p;					/* arena overflow = CBMC OOB violation */
}

#define digitbuf_free(buf) ((void) 0)

/*
 * zero_var() - verbatim (digitbuf_free per SHIM A1).
 */
static void
zero_var(NumericVar *var)
{
	digitbuf_free(var->buf);
	var->buf = NULL;
	var->digits = NULL;
	var->ndigits = 0;
	var->weight = 0;			/* by convention; doesn't really matter */
	var->sign = NUMERIC_POS;	/* anything but NAN... */
}

/*
 * strip_var() - verbatim.
 */
static void
strip_var(NumericVar *var)
{
	NumericDigit *digits = var->digits;
	int			ndigits = var->ndigits;

	/* Strip leading zeroes */
	while (ndigits > 0 && *digits == 0)
	{
		digits++;
		var->weight--;
		ndigits--;
	}

	/* Strip trailing zeroes */
	while (ndigits > 0 && digits[ndigits - 1] == 0)
		ndigits--;

	/* If it's zero, normalize the sign and weight */
	if (ndigits == 0)
	{
		var->sign = NUMERIC_POS;
		var->weight = 0;
	}

	var->digits = digits;
	var->ndigits = ndigits;
}

/*
 * cmp_abs() / cmp_abs_common() - verbatim.
 */
static int
cmp_abs_common(const NumericDigit *var1digits, int var1ndigits, int var1weight,
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

static int
cmp_abs(const NumericVar *var1, const NumericVar *var2)
{
	return cmp_abs_common(var1->digits, var1->ndigits, var1->weight,
						  var2->digits, var2->ndigits, var2->weight);
}

/*
 * add_abs() - verbatim (digitbuf_alloc/free per SHIM A1).
 */
static void
add_abs(const NumericVar *var1, const NumericVar *var2, NumericVar *result)
{
	NumericDigit *res_buf;
	NumericDigit *res_digits;
	int			res_ndigits;
	int			res_weight;
	int			res_rscale,
				rscale1,
				rscale2;
	int			res_dscale;
	int			i,
				i1,
				i2;
	int			carry = 0;

	/* copy these values into local vars for speed in inner loop */
	int			var1ndigits = var1->ndigits;
	int			var2ndigits = var2->ndigits;
	NumericDigit *var1digits = var1->digits;
	NumericDigit *var2digits = var2->digits;

	res_weight = Max(var1->weight, var2->weight) + 1;

	res_dscale = Max(var1->dscale, var2->dscale);

	/* Note: here we are figuring rscale in base-NBASE digits */
	rscale1 = var1->ndigits - var1->weight - 1;
	rscale2 = var2->ndigits - var2->weight - 1;
	res_rscale = Max(rscale1, rscale2);

	res_ndigits = res_rscale + res_weight + 1;
	if (res_ndigits <= 0)
		res_ndigits = 1;

	res_buf = digitbuf_alloc(res_ndigits + 1);
	res_buf[0] = 0;				/* spare digit for later rounding */
	res_digits = res_buf + 1;

	i1 = res_rscale + var1->weight + 1;
	i2 = res_rscale + var2->weight + 1;
	for (i = res_ndigits - 1; i >= 0; i--)
	{
		i1--;
		i2--;
		if (i1 >= 0 && i1 < var1ndigits)
			carry += var1digits[i1];
		if (i2 >= 0 && i2 < var2ndigits)
			carry += var2digits[i2];

		if (carry >= NBASE)
		{
			res_digits[i] = carry - NBASE;
			carry = 1;
		}
		else
		{
			res_digits[i] = carry;
			carry = 0;
		}
	}

	Assert(carry == 0);			/* else we failed to allow for carry out */

	digitbuf_free(result->buf);
	result->ndigits = res_ndigits;
	result->buf = res_buf;
	result->digits = res_digits;
	result->weight = res_weight;
	result->dscale = res_dscale;

	/* Remove leading/trailing zeroes */
	strip_var(result);
}

/*
 * sub_abs() - verbatim.
 */
static void
sub_abs(const NumericVar *var1, const NumericVar *var2, NumericVar *result)
{
	NumericDigit *res_buf;
	NumericDigit *res_digits;
	int			res_ndigits;
	int			res_weight;
	int			res_rscale,
				rscale1,
				rscale2;
	int			res_dscale;
	int			i,
				i1,
				i2;
	int			borrow = 0;

	/* copy these values into local vars for speed in inner loop */
	int			var1ndigits = var1->ndigits;
	int			var2ndigits = var2->ndigits;
	NumericDigit *var1digits = var1->digits;
	NumericDigit *var2digits = var2->digits;

	res_weight = var1->weight;

	res_dscale = Max(var1->dscale, var2->dscale);

	/* Note: here we are figuring rscale in base-NBASE digits */
	rscale1 = var1->ndigits - var1->weight - 1;
	rscale2 = var2->ndigits - var2->weight - 1;
	res_rscale = Max(rscale1, rscale2);

	res_ndigits = res_rscale + res_weight + 1;
	if (res_ndigits <= 0)
		res_ndigits = 1;

	res_buf = digitbuf_alloc(res_ndigits + 1);
	res_buf[0] = 0;				/* spare digit for later rounding */
	res_digits = res_buf + 1;

	i1 = res_rscale + var1->weight + 1;
	i2 = res_rscale + var2->weight + 1;
	for (i = res_ndigits - 1; i >= 0; i--)
	{
		i1--;
		i2--;
		if (i1 >= 0 && i1 < var1ndigits)
			borrow += var1digits[i1];
		if (i2 >= 0 && i2 < var2ndigits)
			borrow -= var2digits[i2];

		if (borrow < 0)
		{
			res_digits[i] = borrow + NBASE;
			borrow = -1;
		}
		else
		{
			res_digits[i] = borrow;
			borrow = 0;
		}
	}

	Assert(borrow == 0);		/* else caller gave us var1 < var2 */

	digitbuf_free(result->buf);
	result->ndigits = res_ndigits;
	result->buf = res_buf;
	result->digits = res_digits;
	result->weight = res_weight;
	result->dscale = res_dscale;

	/* Remove leading/trailing zeroes */
	strip_var(result);
}

/*
 * add_var() - verbatim.
 */
static void
add_var(const NumericVar *var1, const NumericVar *var2, NumericVar *result)
{
	/*
	 * Decide on the signs of the two variables what to do
	 */
	if (var1->sign == NUMERIC_POS)
	{
		if (var2->sign == NUMERIC_POS)
		{
			/*
			 * Both are positive result = +(ABS(var1) + ABS(var2))
			 */
			add_abs(var1, var2, result);
			result->sign = NUMERIC_POS;
		}
		else
		{
			/*
			 * var1 is positive, var2 is negative Must compare absolute values
			 */
			switch (cmp_abs(var1, var2))
			{
				case 0:
					zero_var(result);
					result->dscale = Max(var1->dscale, var2->dscale);
					break;

				case 1:
					sub_abs(var1, var2, result);
					result->sign = NUMERIC_POS;
					break;

				case -1:
					sub_abs(var2, var1, result);
					result->sign = NUMERIC_NEG;
					break;
			}
		}
	}
	else
	{
		if (var2->sign == NUMERIC_POS)
		{
			switch (cmp_abs(var1, var2))
			{
				case 0:
					zero_var(result);
					result->dscale = Max(var1->dscale, var2->dscale);
					break;

				case 1:
					sub_abs(var1, var2, result);
					result->sign = NUMERIC_NEG;
					break;

				case -1:
					sub_abs(var2, var1, result);
					result->sign = NUMERIC_POS;
					break;
			}
		}
		else
		{
			/*
			 * Both are negative result = -(ABS(var1) + ABS(var2))
			 */
			add_abs(var1, var2, result);
			result->sign = NUMERIC_NEG;
		}
	}
}

/*
 * sub_var() - verbatim.
 */
static void
sub_var(const NumericVar *var1, const NumericVar *var2, NumericVar *result)
{
	/*
	 * Decide on the signs of the two variables what to do
	 */
	if (var1->sign == NUMERIC_POS)
	{
		if (var2->sign == NUMERIC_NEG)
		{
			add_abs(var1, var2, result);
			result->sign = NUMERIC_POS;
		}
		else
		{
			switch (cmp_abs(var1, var2))
			{
				case 0:
					zero_var(result);
					result->dscale = Max(var1->dscale, var2->dscale);
					break;

				case 1:
					sub_abs(var1, var2, result);
					result->sign = NUMERIC_POS;
					break;

				case -1:
					sub_abs(var2, var1, result);
					result->sign = NUMERIC_NEG;
					break;
			}
		}
	}
	else
	{
		if (var2->sign == NUMERIC_NEG)
		{
			switch (cmp_abs(var1, var2))
			{
				case 0:
					zero_var(result);
					result->dscale = Max(var1->dscale, var2->dscale);
					break;

				case 1:
					sub_abs(var1, var2, result);
					result->sign = NUMERIC_NEG;
					break;

				case -1:
					sub_abs(var2, var1, result);
					result->sign = NUMERIC_POS;
					break;
			}
		}
		else
		{
			add_abs(var1, var2, result);
			result->sign = NUMERIC_NEG;
		}
	}
}

/*
 * mul_var_short() - verbatim (Assert -> no-op).  var1 has 1-6 digits, var2
 * at least as many; exact product.
 */
static void
mul_var_short(const NumericVar *var1, const NumericVar *var2,
			  NumericVar *result)
{
	int			var1ndigits = var1->ndigits;
	int			var2ndigits = var2->ndigits;
	NumericDigit *var1digits = var1->digits;
	NumericDigit *var2digits = var2->digits;
	int			res_sign;
	int			res_weight;
	int			res_ndigits;
	NumericDigit *res_buf;
	NumericDigit *res_digits;
	uint32		carry = 0;
	uint32		term;

	/* Check preconditions */
	Assert(var1ndigits >= 1);
	Assert(var1ndigits <= 6);
	Assert(var2ndigits >= var1ndigits);

	/*
	 * Determine the result sign, weight, and number of digits to calculate.
	 */
	if (var1->sign == var2->sign)
		res_sign = NUMERIC_POS;
	else
		res_sign = NUMERIC_NEG;
	res_weight = var1->weight + var2->weight + 1;
	res_ndigits = var1ndigits + var2ndigits;

	/* Allocate result digit array */
	res_buf = digitbuf_alloc(res_ndigits + 1);
	res_buf[0] = 0;				/* spare digit for later rounding */
	res_digits = res_buf + 1;

#define PRODSUM1(v1,i1,v2,i2) ((v1)[(i1)] * (v2)[(i2)])
#define PRODSUM2(v1,i1,v2,i2) (PRODSUM1(v1,i1,v2,i2) + (v1)[(i1)+1] * (v2)[(i2)-1])
#define PRODSUM3(v1,i1,v2,i2) (PRODSUM2(v1,i1,v2,i2) + (v1)[(i1)+2] * (v2)[(i2)-2])
#define PRODSUM4(v1,i1,v2,i2) (PRODSUM3(v1,i1,v2,i2) + (v1)[(i1)+3] * (v2)[(i2)-3])
#define PRODSUM5(v1,i1,v2,i2) (PRODSUM4(v1,i1,v2,i2) + (v1)[(i1)+4] * (v2)[(i2)-4])
#define PRODSUM6(v1,i1,v2,i2) (PRODSUM5(v1,i1,v2,i2) + (v1)[(i1)+5] * (v2)[(i2)-5])

	switch (var1ndigits)
	{
		case 1:
			for (int i = var2ndigits - 1; i >= 0; i--)
			{
				term = PRODSUM1(var1digits, 0, var2digits, i) + carry;
				res_digits[i + 1] = (NumericDigit) (term % NBASE);
				carry = term / NBASE;
			}
			res_digits[0] = (NumericDigit) carry;
			break;

		case 2:
			/* last result digit and carry */
			term = PRODSUM1(var1digits, 1, var2digits, var2ndigits - 1);
			res_digits[res_ndigits - 1] = (NumericDigit) (term % NBASE);
			carry = term / NBASE;

			/* remaining digits, except for the first two */
			for (int i = var2ndigits - 1; i >= 1; i--)
			{
				term = PRODSUM2(var1digits, 0, var2digits, i) + carry;
				res_digits[i + 1] = (NumericDigit) (term % NBASE);
				carry = term / NBASE;
			}
			break;

		case 3:
			/* last two result digits */
			term = PRODSUM1(var1digits, 2, var2digits, var2ndigits - 1);
			res_digits[res_ndigits - 1] = (NumericDigit) (term % NBASE);
			carry = term / NBASE;

			term = PRODSUM2(var1digits, 1, var2digits, var2ndigits - 1) + carry;
			res_digits[res_ndigits - 2] = (NumericDigit) (term % NBASE);
			carry = term / NBASE;

			/* remaining digits, except for the first three */
			for (int i = var2ndigits - 1; i >= 2; i--)
			{
				term = PRODSUM3(var1digits, 0, var2digits, i) + carry;
				res_digits[i + 1] = (NumericDigit) (term % NBASE);
				carry = term / NBASE;
			}
			break;

		case 4:
			/* last three result digits */
			term = PRODSUM1(var1digits, 3, var2digits, var2ndigits - 1);
			res_digits[res_ndigits - 1] = (NumericDigit) (term % NBASE);
			carry = term / NBASE;

			term = PRODSUM2(var1digits, 2, var2digits, var2ndigits - 1) + carry;
			res_digits[res_ndigits - 2] = (NumericDigit) (term % NBASE);
			carry = term / NBASE;

			term = PRODSUM3(var1digits, 1, var2digits, var2ndigits - 1) + carry;
			res_digits[res_ndigits - 3] = (NumericDigit) (term % NBASE);
			carry = term / NBASE;

			/* remaining digits, except for the first four */
			for (int i = var2ndigits - 1; i >= 3; i--)
			{
				term = PRODSUM4(var1digits, 0, var2digits, i) + carry;
				res_digits[i + 1] = (NumericDigit) (term % NBASE);
				carry = term / NBASE;
			}
			break;

		case 5:
			/* last four result digits */
			term = PRODSUM1(var1digits, 4, var2digits, var2ndigits - 1);
			res_digits[res_ndigits - 1] = (NumericDigit) (term % NBASE);
			carry = term / NBASE;

			term = PRODSUM2(var1digits, 3, var2digits, var2ndigits - 1) + carry;
			res_digits[res_ndigits - 2] = (NumericDigit) (term % NBASE);
			carry = term / NBASE;

			term = PRODSUM3(var1digits, 2, var2digits, var2ndigits - 1) + carry;
			res_digits[res_ndigits - 3] = (NumericDigit) (term % NBASE);
			carry = term / NBASE;

			term = PRODSUM4(var1digits, 1, var2digits, var2ndigits - 1) + carry;
			res_digits[res_ndigits - 4] = (NumericDigit) (term % NBASE);
			carry = term / NBASE;

			/* remaining digits, except for the first five */
			for (int i = var2ndigits - 1; i >= 4; i--)
			{
				term = PRODSUM5(var1digits, 0, var2digits, i) + carry;
				res_digits[i + 1] = (NumericDigit) (term % NBASE);
				carry = term / NBASE;
			}
			break;

		case 6:
			/* last five result digits */
			term = PRODSUM1(var1digits, 5, var2digits, var2ndigits - 1);
			res_digits[res_ndigits - 1] = (NumericDigit) (term % NBASE);
			carry = term / NBASE;

			term = PRODSUM2(var1digits, 4, var2digits, var2ndigits - 1) + carry;
			res_digits[res_ndigits - 2] = (NumericDigit) (term % NBASE);
			carry = term / NBASE;

			term = PRODSUM3(var1digits, 3, var2digits, var2ndigits - 1) + carry;
			res_digits[res_ndigits - 3] = (NumericDigit) (term % NBASE);
			carry = term / NBASE;

			term = PRODSUM4(var1digits, 2, var2digits, var2ndigits - 1) + carry;
			res_digits[res_ndigits - 4] = (NumericDigit) (term % NBASE);
			carry = term / NBASE;

			term = PRODSUM5(var1digits, 1, var2digits, var2ndigits - 1) + carry;
			res_digits[res_ndigits - 5] = (NumericDigit) (term % NBASE);
			carry = term / NBASE;

			/* remaining digits, except for the first six */
			for (int i = var2ndigits - 1; i >= 5; i--)
			{
				term = PRODSUM6(var1digits, 0, var2digits, i) + carry;
				res_digits[i + 1] = (NumericDigit) (term % NBASE);
				carry = term / NBASE;
			}
			break;
	}

	/*
	 * Finally, for var1ndigits > 1, compute the remaining var1ndigits most
	 * significant result digits.
	 */
	switch (var1ndigits)
	{
		case 6:
			term = PRODSUM5(var1digits, 0, var2digits, 4) + carry;
			res_digits[5] = (NumericDigit) (term % NBASE);
			carry = term / NBASE;
			/* FALLTHROUGH */
		case 5:
			term = PRODSUM4(var1digits, 0, var2digits, 3) + carry;
			res_digits[4] = (NumericDigit) (term % NBASE);
			carry = term / NBASE;
			/* FALLTHROUGH */
		case 4:
			term = PRODSUM3(var1digits, 0, var2digits, 2) + carry;
			res_digits[3] = (NumericDigit) (term % NBASE);
			carry = term / NBASE;
			/* FALLTHROUGH */
		case 3:
			term = PRODSUM2(var1digits, 0, var2digits, 1) + carry;
			res_digits[2] = (NumericDigit) (term % NBASE);
			carry = term / NBASE;
			/* FALLTHROUGH */
		case 2:
			term = PRODSUM1(var1digits, 0, var2digits, 0) + carry;
			res_digits[1] = (NumericDigit) (term % NBASE);
			res_digits[0] = (NumericDigit) (term / NBASE);
			break;
	}

	/* Store the product in result */
	digitbuf_free(result->buf);
	result->ndigits = res_ndigits;
	result->buf = res_buf;
	result->digits = res_digits;
	result->weight = res_weight;
	result->sign = res_sign;
	result->dscale = var1->dscale + var2->dscale;

	/* Strip leading and trailing zeroes */
	strip_var(result);
}

/*
 * mul_var() - verbatim through the zero shortcut and the short-kernel
 * delegation; the full pairwise kernel is FENCED OUT via SHIM A3 (loud
 * trap).  The vendored normalization swap (var1 = shorter input) is the
 * upstream body's first step, kept verbatim.
 */
static void
mul_var_plane(const NumericVar *var1, const NumericVar *var2,
			  NumericVar *result, int rscale, int *trap)
{
	int			var1ndigits;

	/*
	 * Arrange for var1 to be the shorter of the two numbers.  (verbatim)
	 */
	if (var1->ndigits > var2->ndigits)
	{
		const NumericVar *tmp = var1;

		var1 = var2;
		var2 = tmp;
	}

	var1ndigits = var1->ndigits;

	if (var1ndigits == 0)
	{
		/* one or both inputs is zero; so is result */
		zero_var(result);
		result->dscale = rscale;
		return;
	}

	/*
	 * If var1 has 1-6 digits and the exact result was requested, delegate to
	 * mul_var_short() which uses a faster direct multiplication algorithm.
	 */
	if (var1ndigits <= 6 && rscale == var1->dscale + var2->dscale)
	{
		mul_var_short(var1, var2, result);
		return;
	}

	/* SHIM A3: full pairwise kernel is out of the fenced plane */
	*trap = 1;
}

/* ---- SHIM A2: entry points on explicit spec tuples ---- */

static void
load_var(NumericVar *v, int sign, int weight, int dscale,
		 const int16 *digits, int ndigits)
{
	v->ndigits = ndigits;
	v->weight = weight;
	v->sign = sign;
	v->dscale = dscale;
	v->buf = NULL;
	v->digits = (NumericDigit *) digits;
}

static void
store_result(const NumericVar *r, int *out_sign, int *out_weight,
			 int *out_dscale, int16 *out_digits, int *out_nd)
{
	int			i;

	*out_sign = r->sign;
	*out_weight = r->weight;
	*out_dscale = r->dscale;
	*out_nd = r->ndigits;
	for (i = 0; i < r->ndigits; i++)
		out_digits[i] = r->digits[i];
}

int
pg_arith_add_var(int sign1, int weight1, int dscale1, const int16 *digits1, int nd1,
				 int sign2, int weight2, int dscale2, const int16 *digits2, int nd2,
				 int *out_sign, int *out_weight, int *out_dscale,
				 int16 *out_digits, int *out_nd)
{
	NumericVar	v1, v2, r;

	arith_digit_next = 0;		/* SHIM A1 arena reset */
	load_var(&v1, sign1, weight1, dscale1, digits1, nd1);
	load_var(&v2, sign2, weight2, dscale2, digits2, nd2);
	r.ndigits = 0;
	r.weight = 0;
	r.sign = NUMERIC_POS;
	r.dscale = 0;
	r.buf = NULL;
	r.digits = NULL;
	add_var(&v1, &v2, &r);
	store_result(&r, out_sign, out_weight, out_dscale, out_digits, out_nd);
	return 0;
}

int
pg_arith_sub_var(int sign1, int weight1, int dscale1, const int16 *digits1, int nd1,
				 int sign2, int weight2, int dscale2, const int16 *digits2, int nd2,
				 int *out_sign, int *out_weight, int *out_dscale,
				 int16 *out_digits, int *out_nd)
{
	NumericVar	v1, v2, r;

	arith_digit_next = 0;
	load_var(&v1, sign1, weight1, dscale1, digits1, nd1);
	load_var(&v2, sign2, weight2, dscale2, digits2, nd2);
	r.ndigits = 0;
	r.weight = 0;
	r.sign = NUMERIC_POS;
	r.dscale = 0;
	r.buf = NULL;
	r.digits = NULL;
	sub_var(&v1, &v2, &r);
	store_result(&r, out_sign, out_weight, out_dscale, out_digits, out_nd);
	return 0;
}

int
pg_arith_mul_var(int sign1, int weight1, int dscale1, const int16 *digits1, int nd1,
				 int sign2, int weight2, int dscale2, const int16 *digits2, int nd2,
				 int rscale, int *trap,
				 int *out_sign, int *out_weight, int *out_dscale,
				 int16 *out_digits, int *out_nd)
{
	NumericVar	v1, v2, r;

	arith_digit_next = 0;
	*trap = 0;
	load_var(&v1, sign1, weight1, dscale1, digits1, nd1);
	load_var(&v2, sign2, weight2, dscale2, digits2, nd2);
	r.ndigits = 0;
	r.weight = 0;
	r.sign = NUMERIC_POS;
	r.dscale = 0;
	r.buf = NULL;
	r.digits = NULL;
	mul_var_plane(&v1, &v2, &r, rscale, trap);
	if (*trap)
		return -1;
	store_result(&r, out_sign, out_weight, out_dscale, out_digits, out_nd);
	return 0;
}
