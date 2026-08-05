/*
 * pg_numeric_min.c — VERBATIM extracts of src/backend/utils/adt/numeric.c
 * @ postgres-src 62d6c7d3df6287f1bd83199c1a746e50d31571a0 (PostgreSQL 18.3):
 * exactly the call graph of numeric_in / numeric_out / numeric_uminus as
 * used by the jsonpath oracle (DirectFunctionCall sites in jsonpath.c and
 * jsonpath_gram.y). Every extract below carries a "numeric.c:A-B VERBATIM"
 * provenance marker emitted by extract_verbatim.py.
 *
 * Shims (environment only, never logic):
 *   - include set below: the shim postgres.h/fmgr.h/utils/numeric.h provide
 *     the palloc arena, ereport/ereturn/errsave capture, fmgr and Datum
 *     plumbing (see include/postgres.h header comment);
 *   - the static forward-declaration block below replaces the dropped
 *     original declaration block (numeric.c:504-640) for exactly the
 *     extracted statics (the rest of that block names types the extract
 *     does not carry).
 * NO aggregate/arithmetic surface beyond the extracted set is vendored.
 */

#include "postgres.h"

#include <ctype.h>
#include <float.h>
#include <limits.h>
#include <math.h>

#include "fmgr.h"
#include "lib/stringinfo.h"
#include "nodes/miscnodes.h"
#include "utils/array.h"
#include "utils/numeric.h"

/* ---- numeric.c:46-321 VERBATIM ---- */
/* ----------
 * Uncomment the following to enable compilation of dump_numeric()
 * and dump_var() and to get a dump of any result produced by make_result().
 * ----------
#define NUMERIC_DEBUG
 */


/* ----------
 * Local data types
 *
 * Numeric values are represented in a base-NBASE floating point format.
 * Each "digit" ranges from 0 to NBASE-1.  The type NumericDigit is signed
 * and wide enough to store a digit.  We assume that NBASE*NBASE can fit in
 * an int.  Although the purely calculational routines could handle any even
 * NBASE that's less than sqrt(INT_MAX), in practice we are only interested
 * in NBASE a power of ten, so that I/O conversions and decimal rounding
 * are easy.  Also, it's actually more efficient if NBASE is rather less than
 * sqrt(INT_MAX), so that there is "headroom" for mul_var and div_var to
 * postpone processing carries.
 *
 * Values of NBASE other than 10000 are considered of historical interest only
 * and are no longer supported in any sense; no mechanism exists for the client
 * to discover the base, so every client supporting binary mode expects the
 * base-10000 format.  If you plan to change this, also note the numeric
 * abbreviation code, which assumes NBASE=10000.
 * ----------
 */

#if 0
#define NBASE		10
#define HALF_NBASE	5
#define DEC_DIGITS	1			/* decimal digits per NBASE digit */
#define MUL_GUARD_DIGITS	4	/* these are measured in NBASE digits */
#define DIV_GUARD_DIGITS	8

typedef signed char NumericDigit;
#endif

#if 0
#define NBASE		100
#define HALF_NBASE	50
#define DEC_DIGITS	2			/* decimal digits per NBASE digit */
#define MUL_GUARD_DIGITS	3	/* these are measured in NBASE digits */
#define DIV_GUARD_DIGITS	6

typedef signed char NumericDigit;
#endif

#if 1
#define NBASE		10000
#define HALF_NBASE	5000
#define DEC_DIGITS	4			/* decimal digits per NBASE digit */
#define MUL_GUARD_DIGITS	2	/* these are measured in NBASE digits */
#define DIV_GUARD_DIGITS	4

typedef int16 NumericDigit;
#endif

#define NBASE_SQR	(NBASE * NBASE)

/*
 * The Numeric type as stored on disk.
 *
 * If the high bits of the first word of a NumericChoice (n_header, or
 * n_short.n_header, or n_long.n_sign_dscale) are NUMERIC_SHORT, then the
 * numeric follows the NumericShort format; if they are NUMERIC_POS or
 * NUMERIC_NEG, it follows the NumericLong format. If they are NUMERIC_SPECIAL,
 * the value is a NaN or Infinity.  We currently always store SPECIAL values
 * using just two bytes (i.e. only n_header), but previous releases used only
 * the NumericLong format, so we might find 4-byte NaNs (though not infinities)
 * on disk if a database has been migrated using pg_upgrade.  In either case,
 * the low-order bits of a special value's header are reserved and currently
 * should always be set to zero.
 *
 * In the NumericShort format, the remaining 14 bits of the header word
 * (n_short.n_header) are allocated as follows: 1 for sign (positive or
 * negative), 6 for dynamic scale, and 7 for weight.  In practice, most
 * commonly-encountered values can be represented this way.
 *
 * In the NumericLong format, the remaining 14 bits of the header word
 * (n_long.n_sign_dscale) represent the display scale; and the weight is
 * stored separately in n_weight.
 *
 * NOTE: by convention, values in the packed form have been stripped of
 * all leading and trailing zero digits (where a "digit" is of base NBASE).
 * In particular, if the value is zero, there will be no digits at all!
 * The weight is arbitrary in that case, but we normally set it to zero.
 */

struct NumericShort
{
	uint16		n_header;		/* Sign + display scale + weight */
	NumericDigit n_data[FLEXIBLE_ARRAY_MEMBER]; /* Digits */
};

struct NumericLong
{
	uint16		n_sign_dscale;	/* Sign + display scale */
	int16		n_weight;		/* Weight of 1st digit	*/
	NumericDigit n_data[FLEXIBLE_ARRAY_MEMBER]; /* Digits */
};

union NumericChoice
{
	uint16		n_header;		/* Header word */
	struct NumericLong n_long;	/* Long form (4-byte header) */
	struct NumericShort n_short;	/* Short form (2-byte header) */
};

struct NumericData
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	union NumericChoice choice; /* choice of format */
};


/*
 * Interpretation of high bits.
 */

#define NUMERIC_SIGN_MASK	0xC000
#define NUMERIC_POS			0x0000
#define NUMERIC_NEG			0x4000
#define NUMERIC_SHORT		0x8000
#define NUMERIC_SPECIAL		0xC000

#define NUMERIC_FLAGBITS(n) ((n)->choice.n_header & NUMERIC_SIGN_MASK)
#define NUMERIC_IS_SHORT(n)		(NUMERIC_FLAGBITS(n) == NUMERIC_SHORT)
#define NUMERIC_IS_SPECIAL(n)	(NUMERIC_FLAGBITS(n) == NUMERIC_SPECIAL)

#define NUMERIC_HDRSZ	(VARHDRSZ + sizeof(uint16) + sizeof(int16))
#define NUMERIC_HDRSZ_SHORT (VARHDRSZ + sizeof(uint16))

/*
 * If the flag bits are NUMERIC_SHORT or NUMERIC_SPECIAL, we want the short
 * header; otherwise, we want the long one.  Instead of testing against each
 * value, we can just look at the high bit, for a slight efficiency gain.
 */
#define NUMERIC_HEADER_IS_SHORT(n)	(((n)->choice.n_header & 0x8000) != 0)
#define NUMERIC_HEADER_SIZE(n) \
	(VARHDRSZ + sizeof(uint16) + \
	 (NUMERIC_HEADER_IS_SHORT(n) ? 0 : sizeof(int16)))

/*
 * Definitions for special values (NaN, positive infinity, negative infinity).
 *
 * The two bits after the NUMERIC_SPECIAL bits are 00 for NaN, 01 for positive
 * infinity, 11 for negative infinity.  (This makes the sign bit match where
 * it is in a short-format value, though we make no use of that at present.)
 * We could mask off the remaining bits before testing the active bits, but
 * currently those bits must be zeroes, so masking would just add cycles.
 */
#define NUMERIC_EXT_SIGN_MASK	0xF000	/* high bits plus NaN/Inf flag bits */
#define NUMERIC_NAN				0xC000
#define NUMERIC_PINF			0xD000
#define NUMERIC_NINF			0xF000
#define NUMERIC_INF_SIGN_MASK	0x2000

#define NUMERIC_EXT_FLAGBITS(n)	((n)->choice.n_header & NUMERIC_EXT_SIGN_MASK)
#define NUMERIC_IS_NAN(n)		((n)->choice.n_header == NUMERIC_NAN)
#define NUMERIC_IS_PINF(n)		((n)->choice.n_header == NUMERIC_PINF)
#define NUMERIC_IS_NINF(n)		((n)->choice.n_header == NUMERIC_NINF)
#define NUMERIC_IS_INF(n) \
	(((n)->choice.n_header & ~NUMERIC_INF_SIGN_MASK) == NUMERIC_PINF)

/*
 * Short format definitions.
 */

#define NUMERIC_SHORT_SIGN_MASK			0x2000
#define NUMERIC_SHORT_DSCALE_MASK		0x1F80
#define NUMERIC_SHORT_DSCALE_SHIFT		7
#define NUMERIC_SHORT_DSCALE_MAX		\
	(NUMERIC_SHORT_DSCALE_MASK >> NUMERIC_SHORT_DSCALE_SHIFT)
#define NUMERIC_SHORT_WEIGHT_SIGN_MASK	0x0040
#define NUMERIC_SHORT_WEIGHT_MASK		0x003F
#define NUMERIC_SHORT_WEIGHT_MAX		NUMERIC_SHORT_WEIGHT_MASK
#define NUMERIC_SHORT_WEIGHT_MIN		(-(NUMERIC_SHORT_WEIGHT_MASK+1))

/*
 * Extract sign, display scale, weight.  These macros extract field values
 * suitable for the NumericVar format from the Numeric (on-disk) format.
 *
 * Note that we don't trouble to ensure that dscale and weight read as zero
 * for an infinity; however, that doesn't matter since we never convert
 * "special" numerics to NumericVar form.  Only the constants defined below
 * (const_nan, etc) ever represent a non-finite value as a NumericVar.
 */

#define NUMERIC_DSCALE_MASK			0x3FFF
#define NUMERIC_DSCALE_MAX			NUMERIC_DSCALE_MASK

#define NUMERIC_SIGN(n) \
	(NUMERIC_IS_SHORT(n) ? \
		(((n)->choice.n_short.n_header & NUMERIC_SHORT_SIGN_MASK) ? \
		 NUMERIC_NEG : NUMERIC_POS) : \
		(NUMERIC_IS_SPECIAL(n) ? \
		 NUMERIC_EXT_FLAGBITS(n) : NUMERIC_FLAGBITS(n)))
#define NUMERIC_DSCALE(n)	(NUMERIC_HEADER_IS_SHORT((n)) ? \
	((n)->choice.n_short.n_header & NUMERIC_SHORT_DSCALE_MASK) \
		>> NUMERIC_SHORT_DSCALE_SHIFT \
	: ((n)->choice.n_long.n_sign_dscale & NUMERIC_DSCALE_MASK))
#define NUMERIC_WEIGHT(n)	(NUMERIC_HEADER_IS_SHORT((n)) ? \
	(((n)->choice.n_short.n_header & NUMERIC_SHORT_WEIGHT_SIGN_MASK ? \
		~NUMERIC_SHORT_WEIGHT_MASK : 0) \
	 | ((n)->choice.n_short.n_header & NUMERIC_SHORT_WEIGHT_MASK)) \
	: ((n)->choice.n_long.n_weight))

/*
 * Maximum weight of a stored Numeric value (based on the use of int16 for the
 * weight in NumericLong).  Note that intermediate values held in NumericVar
 * and NumericSumAccum variables may have much larger weights.
 */
#define NUMERIC_WEIGHT_MAX			PG_INT16_MAX

/* ----------
 * NumericVar is the format we use for arithmetic.  The digit-array part
 * is the same as the NumericData storage format, but the header is more
 * complex.
 *
 * The value represented by a NumericVar is determined by the sign, weight,
 * ndigits, and digits[] array.  If it is a "special" value (NaN or Inf)
 * then only the sign field matters; ndigits should be zero, and the weight
 * and dscale fields are ignored.
 *
 * Note: the first digit of a NumericVar's value is assumed to be multiplied
 * by NBASE ** weight.  Another way to say it is that there are weight+1
 * digits before the decimal point.  It is possible to have weight < 0.
 *
 * buf points at the physical start of the palloc'd digit buffer for the
 * NumericVar.  digits points at the first digit in actual use (the one
 * with the specified weight).  We normally leave an unused digit or two
 * (preset to zeroes) between buf and digits, so that there is room to store
 * a carry out of the top digit without reallocating space.  We just need to
 * decrement digits (and increment weight) to make room for the carry digit.
 * (There is no such extra space in a numeric value stored in the database,
 * only in a NumericVar in memory.)
 *
 * If buf is NULL then the digit buffer isn't actually palloc'd and should
 * not be freed --- see the constants below for an example.
 *
 * dscale, or display scale, is the nominal precision expressed as number
 * of digits after the decimal point (it must always be >= 0 at present).
 * dscale may be more than the number of physically stored fractional digits,
 * implying that we have suppressed storage of significant trailing zeroes.
 * It should never be less than the number of stored digits, since that would
 * imply hiding digits that are present.  NOTE that dscale is always expressed
 * in *decimal* digits, and so it may correspond to a fractional number of
 * base-NBASE digits --- divide by DEC_DIGITS to convert to NBASE digits.
 *
 * rscale, or result scale, is the target precision for a computation.
 * Like dscale it is expressed as number of *decimal* digits after the decimal
 * point, and is always >= 0 at present.
 * Note that rscale is not stored in variables --- it's figured on-the-fly
 * from the dscales of the inputs.
 *
 * While we consistently use "weight" to refer to the base-NBASE weight of
 * a numeric value, it is convenient in some scale-related calculations to
 * make use of the base-10 weight (ie, the approximate log10 of the value).
 * To avoid confusion, such a decimal-units weight is called a "dweight".
 *
 * NB: All the variable-level functions are written in a style that makes it
 * possible to give one and the same variable as argument and destination.
 * This is feasible because the digit buffer is separate from the variable.
 * ----------
 */
typedef struct NumericVar
{
	int			ndigits;		/* # of digits in digits[] - can be 0! */
	int			weight;			/* weight of first digit */
	int			sign;			/* NUMERIC_POS, _NEG, _NAN, _PINF, or _NINF */
	int			dscale;			/* display scale */
	NumericDigit *buf;			/* start of palloc'd space for digits[] */
	NumericDigit *digits;		/* base-NBASE digits */
} NumericVar;

/* ---- numeric.c:420-503 VERBATIM ---- */
/* ----------
 * Some preinitialized constants
 * ----------
 */
static const NumericDigit const_zero_data[1] = {0};
static const NumericVar const_zero =
{0, 0, NUMERIC_POS, 0, NULL, (NumericDigit *) const_zero_data};

static const NumericDigit const_one_data[1] = {1};
static const NumericVar const_one =
{1, 0, NUMERIC_POS, 0, NULL, (NumericDigit *) const_one_data};

static const NumericVar const_minus_one =
{1, 0, NUMERIC_NEG, 0, NULL, (NumericDigit *) const_one_data};

static const NumericDigit const_two_data[1] = {2};
static const NumericVar const_two =
{1, 0, NUMERIC_POS, 0, NULL, (NumericDigit *) const_two_data};

#if DEC_DIGITS == 4
static const NumericDigit const_zero_point_nine_data[1] = {9000};
#elif DEC_DIGITS == 2
static const NumericDigit const_zero_point_nine_data[1] = {90};
#elif DEC_DIGITS == 1
static const NumericDigit const_zero_point_nine_data[1] = {9};
#endif
static const NumericVar const_zero_point_nine =
{1, -1, NUMERIC_POS, 1, NULL, (NumericDigit *) const_zero_point_nine_data};

#if DEC_DIGITS == 4
static const NumericDigit const_one_point_one_data[2] = {1, 1000};
#elif DEC_DIGITS == 2
static const NumericDigit const_one_point_one_data[2] = {1, 10};
#elif DEC_DIGITS == 1
static const NumericDigit const_one_point_one_data[2] = {1, 1};
#endif
static const NumericVar const_one_point_one =
{2, 0, NUMERIC_POS, 1, NULL, (NumericDigit *) const_one_point_one_data};

static const NumericVar const_nan =
{0, 0, NUMERIC_NAN, 0, NULL, NULL};

static const NumericVar const_pinf =
{0, 0, NUMERIC_PINF, 0, NULL, NULL};

static const NumericVar const_ninf =
{0, 0, NUMERIC_NINF, 0, NULL, NULL};

#if DEC_DIGITS == 4
static const int round_powers[4] = {0, 1000, 100, 10};
#endif


/* ----------
 * Local functions
 * ----------
 */

#ifdef NUMERIC_DEBUG
static void dump_numeric(const char *str, Numeric num);
static void dump_var(const char *str, NumericVar *var);
#else
#define dump_numeric(s,n)
#define dump_var(s,v)
#endif

#define digitbuf_alloc(ndigits)  \
	((NumericDigit *) palloc((ndigits) * sizeof(NumericDigit)))
#define digitbuf_free(buf)	\
	do { \
		 if ((buf) != NULL) \
			 pfree(buf); \
	} while (0)

#define init_var(v)		memset(v, 0, sizeof(NumericVar))

#define NUMERIC_DIGITS(num) (NUMERIC_HEADER_IS_SHORT(num) ? \
	(num)->choice.n_short.n_data : (num)->choice.n_long.n_data)
#define NUMERIC_NDIGITS(num) \
	((VARSIZE(num) - NUMERIC_HEADER_SIZE(num)) / sizeof(NumericDigit))
#define NUMERIC_CAN_BE_SHORT(scale,weight) \
	((scale) <= NUMERIC_SHORT_DSCALE_MAX && \
	(weight) <= NUMERIC_SHORT_WEIGHT_MAX && \
	(weight) >= NUMERIC_SHORT_WEIGHT_MIN)

/* ---- numeric.c:891-945 VERBATIM ---- */
/*
 * make_numeric_typmod() -
 *
 *	Pack numeric precision and scale values into a typmod.  The upper 16 bits
 *	are used for the precision (though actually not all these bits are needed,
 *	since the maximum allowed precision is 1000).  The lower 16 bits are for
 *	the scale, but since the scale is constrained to the range [-1000, 1000],
 *	we use just the lower 11 of those 16 bits, and leave the remaining 5 bits
 *	unset, for possible future use.
 *
 *	For purely historical reasons VARHDRSZ is then added to the result, thus
 *	the unused space in the upper 16 bits is not all as freely available as it
 *	might seem.  (We can't let the result overflow to a negative int32, as
 *	other parts of the system would interpret that as not-a-valid-typmod.)
 */
static inline int32
make_numeric_typmod(int precision, int scale)
{
	return ((precision << 16) | (scale & 0x7ff)) + VARHDRSZ;
}

/*
 * Because of the offset, valid numeric typmods are at least VARHDRSZ
 */
static inline bool
is_valid_numeric_typmod(int32 typmod)
{
	return typmod >= (int32) VARHDRSZ;
}

/*
 * numeric_typmod_precision() -
 *
 *	Extract the precision from a numeric typmod --- see make_numeric_typmod().
 */
static inline int
numeric_typmod_precision(int32 typmod)
{
	return ((typmod - VARHDRSZ) >> 16) & 0xffff;
}

/*
 * numeric_typmod_scale() -
 *
 *	Extract the scale from a numeric typmod --- see make_numeric_typmod().
 *
 *	Note that the scale may be negative, so we must do sign extension when
 *	unpacking it.  We do this using the bit hack (x^1024)-1024, which sign
 *	extends an 11-bit two's complement number x.
 */
static inline int
numeric_typmod_scale(int32 typmod)
{
	return (((typmod - VARHDRSZ) & 0x7ff) ^ 1024) - 1024;
}

/* ---- common/int.h VERBATIM (pg_abs_s64; used by the extracted set) ---- */
/* ---- int.h:233-259 VERBATIM (pg_add_s64_overflow) ---- */
static inline bool
pg_add_s64_overflow(int64 a, int64 b, int64 *result)
{
#if defined(HAVE__BUILTIN_OP_OVERFLOW)
	return __builtin_add_overflow(a, b, result);
#elif defined(HAVE_INT128)
	int128		res = (int128) a + (int128) b;

	if (res > PG_INT64_MAX || res < PG_INT64_MIN)
	{
		*result = 0x5EED;		/* to avoid spurious warnings */
		return true;
	}
	*result = (int64) res;
	return false;
#else
	if ((a > 0 && b > 0 && a > PG_INT64_MAX - b) ||
		(a < 0 && b < 0 && a < PG_INT64_MIN - b))
	{
		*result = 0x5EED;		/* to avoid spurious warnings */
		return true;
	}
	*result = a + b;
	return false;
#endif
}

/* ---- int.h:260-290 VERBATIM (pg_sub_s64_overflow) ---- */
static inline bool
pg_sub_s64_overflow(int64 a, int64 b, int64 *result)
{
#if defined(HAVE__BUILTIN_OP_OVERFLOW)
	return __builtin_sub_overflow(a, b, result);
#elif defined(HAVE_INT128)
	int128		res = (int128) a - (int128) b;

	if (res > PG_INT64_MAX || res < PG_INT64_MIN)
	{
		*result = 0x5EED;		/* to avoid spurious warnings */
		return true;
	}
	*result = (int64) res;
	return false;
#else
	/*
	 * Note: overflow is also possible when a == 0 and b < 0 (specifically,
	 * when b == PG_INT64_MIN).
	 */
	if ((a < 0 && b > 0 && a < PG_INT64_MIN + b) ||
		(a >= 0 && b < 0 && a > PG_INT64_MAX + b))
	{
		*result = 0x5EED;		/* to avoid spurious warnings */
		return true;
	}
	*result = a - b;
	return false;
#endif
}

/* ---- int.h:291-333 VERBATIM (pg_mul_s64_overflow) ---- */
static inline bool
pg_mul_s64_overflow(int64 a, int64 b, int64 *result)
{
#if defined(HAVE__BUILTIN_OP_OVERFLOW)
	return __builtin_mul_overflow(a, b, result);
#elif defined(HAVE_INT128)
	int128		res = (int128) a * (int128) b;

	if (res > PG_INT64_MAX || res < PG_INT64_MIN)
	{
		*result = 0x5EED;		/* to avoid spurious warnings */
		return true;
	}
	*result = (int64) res;
	return false;
#else
	/*
	 * Overflow can only happen if at least one value is outside the range
	 * sqrt(min)..sqrt(max) so check that first as the division can be quite a
	 * bit more expensive than the multiplication.
	 *
	 * Multiplying by 0 or 1 can't overflow of course and checking for 0
	 * separately avoids any risk of dividing by 0.  Be careful about dividing
	 * INT_MIN by -1 also, note reversing the a and b to ensure we're always
	 * dividing it by a positive value.
	 *
	 */
	if ((a > PG_INT32_MAX || a < PG_INT32_MIN ||
		 b > PG_INT32_MAX || b < PG_INT32_MIN) &&
		a != 0 && a != 1 && b != 0 && b != 1 &&
		((a > 0 && b > 0 && a > PG_INT64_MAX / b) ||
		 (a > 0 && b < 0 && b < PG_INT64_MIN / a) ||
		 (a < 0 && b > 0 && a < PG_INT64_MIN / b) ||
		 (a < 0 && b < 0 && a < PG_INT64_MAX / b)))
	{
		*result = 0x5EED;		/* to avoid spurious warnings */
		return true;
	}
	*result = a * b;
	return false;
#endif
}

/* ---- int.h:351-357 VERBATIM (pg_abs_s64) ---- */
static inline uint64
pg_abs_s64(int64 a)
{
	if (unlikely(a == PG_INT64_MIN))
		return (uint64) PG_INT64_MAX + 1;
	return (uint64) i64abs(a);
}

/* ---- shim: forward declarations for the extracted statics (replaces the
 * dropped numeric.c:504-640 declaration block for exactly this set) ---- */
static void alloc_var(NumericVar *var, int ndigits);
static void free_var(NumericVar *var);
static void zero_var(NumericVar *var);
static bool set_var_from_str(const char *str, const char *cp,
							 NumericVar *dest, const char **endptr,
							 Node *escontext);
static bool set_var_from_non_decimal_integer_str(const char *str,
												 const char *cp, int sign,
												 int base, NumericVar *dest,
												 const char **endptr,
												 Node *escontext);
static void init_var_from_num(Numeric num, NumericVar *dest);
static void set_var_from_var(const NumericVar *value, NumericVar *dest);
static char *get_str_from_var(const NumericVar *var);
static Numeric duplicate_numeric(Numeric num);
static Numeric make_result(const NumericVar *var);
static Numeric make_result_opt_error(const NumericVar *var, bool *have_error);
static bool apply_typmod(NumericVar *var, int32 typmod, Node *escontext);
static bool apply_typmod_special(Numeric num, int32 typmod, Node *escontext);
static void int64_to_numericvar(int64 val, NumericVar *var);
static void add_var(const NumericVar *var1, const NumericVar *var2,
					NumericVar *result);
static void mul_var(const NumericVar *var1, const NumericVar *var2,
					NumericVar *result,
					int rscale);
static void mul_var_short(const NumericVar *var1, const NumericVar *var2,
						  NumericVar *result);
static int	cmp_abs(const NumericVar *var1, const NumericVar *var2);
static int	cmp_abs_common(const NumericDigit *var1digits, int var1ndigits,
						   int var1weight,
						   const NumericDigit *var2digits, int var2ndigits,
						   int var2weight);
static void add_abs(const NumericVar *var1, const NumericVar *var2,
					NumericVar *result);
static void sub_abs(const NumericVar *var1, const NumericVar *var2,
					NumericVar *result);
static void round_var(NumericVar *var, int rscale);
static void trunc_var(NumericVar *var, int rscale);
static void strip_var(NumericVar *var);

/* ---- numeric.c:631-807 VERBATIM (numeric_in) ---- */
/*
 * numeric_in() -
 *
 *	Input function for numeric data type
 */
Datum
numeric_in(PG_FUNCTION_ARGS)
{
	char	   *str = PG_GETARG_CSTRING(0);
#ifdef NOT_USED
	Oid			typelem = PG_GETARG_OID(1);
#endif
	int32		typmod = PG_GETARG_INT32(2);
	Node	   *escontext = fcinfo->context;
	Numeric		res;
	const char *cp;
	const char *numstart;
	int			sign;

	/* Skip leading spaces */
	cp = str;
	while (*cp)
	{
		if (!isspace((unsigned char) *cp))
			break;
		cp++;
	}

	/*
	 * Process the number's sign. This duplicates logic in set_var_from_str(),
	 * but it's worth doing here, since it simplifies the handling of
	 * infinities and non-decimal integers.
	 */
	numstart = cp;
	sign = NUMERIC_POS;

	if (*cp == '+')
		cp++;
	else if (*cp == '-')
	{
		sign = NUMERIC_NEG;
		cp++;
	}

	/*
	 * Check for NaN and infinities.  We recognize the same strings allowed by
	 * float8in().
	 *
	 * Since all other legal inputs have a digit or a decimal point after the
	 * sign, we need only check for NaN/infinity if that's not the case.
	 */
	if (!isdigit((unsigned char) *cp) && *cp != '.')
	{
		/*
		 * The number must be NaN or infinity; anything else can only be a
		 * syntax error. Note that NaN mustn't have a sign.
		 */
		if (pg_strncasecmp(numstart, "NaN", 3) == 0)
		{
			res = make_result(&const_nan);
			cp = numstart + 3;
		}
		else if (pg_strncasecmp(cp, "Infinity", 8) == 0)
		{
			res = make_result(sign == NUMERIC_POS ? &const_pinf : &const_ninf);
			cp += 8;
		}
		else if (pg_strncasecmp(cp, "inf", 3) == 0)
		{
			res = make_result(sign == NUMERIC_POS ? &const_pinf : &const_ninf);
			cp += 3;
		}
		else
			goto invalid_syntax;

		/*
		 * Check for trailing junk; there should be nothing left but spaces.
		 *
		 * We intentionally do this check before applying the typmod because
		 * we would like to throw any trailing-junk syntax error before any
		 * semantic error resulting from apply_typmod_special().
		 */
		while (*cp)
		{
			if (!isspace((unsigned char) *cp))
				goto invalid_syntax;
			cp++;
		}

		if (!apply_typmod_special(res, typmod, escontext))
			PG_RETURN_NULL();
	}
	else
	{
		/*
		 * We have a normal numeric value, which may be a non-decimal integer
		 * or a regular decimal number.
		 */
		NumericVar	value;
		int			base;
		bool		have_error;

		init_var(&value);

		/*
		 * Determine the number's base by looking for a non-decimal prefix
		 * indicator ("0x", "0o", or "0b").
		 */
		if (cp[0] == '0')
		{
			switch (cp[1])
			{
				case 'x':
				case 'X':
					base = 16;
					break;
				case 'o':
				case 'O':
					base = 8;
					break;
				case 'b':
				case 'B':
					base = 2;
					break;
				default:
					base = 10;
			}
		}
		else
			base = 10;

		/* Parse the rest of the number and apply the sign */
		if (base == 10)
		{
			if (!set_var_from_str(str, cp, &value, &cp, escontext))
				PG_RETURN_NULL();
			value.sign = sign;
		}
		else
		{
			if (!set_var_from_non_decimal_integer_str(str, cp + 2, sign, base,
													  &value, &cp, escontext))
				PG_RETURN_NULL();
		}

		/*
		 * Should be nothing left but spaces. As above, throw any typmod error
		 * after finishing syntax check.
		 */
		while (*cp)
		{
			if (!isspace((unsigned char) *cp))
				goto invalid_syntax;
			cp++;
		}

		if (!apply_typmod(&value, typmod, escontext))
			PG_RETURN_NULL();

		res = make_result_opt_error(&value, &have_error);

		if (have_error)
			ereturn(escontext, (Datum) 0,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					 errmsg("value overflows numeric format")));

		free_var(&value);
	}

	PG_RETURN_NUMERIC(res);

invalid_syntax:
	ereturn(escontext, (Datum) 0,
			(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
			 errmsg("invalid input syntax for type %s: \"%s\"",
					"numeric", str)));
}

/* ---- numeric.c:810-843 VERBATIM (numeric_out) ---- */
/*
 * numeric_out() -
 *
 *	Output function for numeric data type
 */
Datum
numeric_out(PG_FUNCTION_ARGS)
{
	Numeric		num = PG_GETARG_NUMERIC(0);
	NumericVar	x;
	char	   *str;

	/*
	 * Handle NaN and infinities
	 */
	if (NUMERIC_IS_SPECIAL(num))
	{
		if (NUMERIC_IS_PINF(num))
			PG_RETURN_CSTRING(pstrdup("Infinity"));
		else if (NUMERIC_IS_NINF(num))
			PG_RETURN_CSTRING(pstrdup("-Infinity"));
		else
			PG_RETURN_CSTRING(pstrdup("NaN"));
	}

	/*
	 * Get the number in the variable format.
	 */
	init_var_from_num(num, &x);

	str = get_str_from_var(&x);

	PG_RETURN_CSTRING(str);
}

/* ---- numeric.c:1419-1458 VERBATIM (numeric_uminus) ---- */
Datum
numeric_uminus(PG_FUNCTION_ARGS)
{
	Numeric		num = PG_GETARG_NUMERIC(0);
	Numeric		res;

	/*
	 * Do it the easy way directly on the packed format
	 */
	res = duplicate_numeric(num);

	if (NUMERIC_IS_SPECIAL(num))
	{
		/* Flip the sign, if it's Inf or -Inf */
		if (!NUMERIC_IS_NAN(num))
			res->choice.n_short.n_header =
				num->choice.n_short.n_header ^ NUMERIC_INF_SIGN_MASK;
	}

	/*
	 * The packed format is known to be totally zero digit trimmed always. So
	 * once we've eliminated specials, we can identify a zero by the fact that
	 * there are no digits at all. Do nothing to a zero.
	 */
	else if (NUMERIC_NDIGITS(num) != 0)
	{
		/* Else, flip the sign */
		if (NUMERIC_IS_SHORT(num))
			res->choice.n_short.n_header =
				num->choice.n_short.n_header ^ NUMERIC_SHORT_SIGN_MASK;
		else if (NUMERIC_SIGN(num) == NUMERIC_POS)
			res->choice.n_long.n_sign_dscale =
				NUMERIC_NEG | NUMERIC_DSCALE(num);
		else
			res->choice.n_long.n_sign_dscale =
				NUMERIC_POS | NUMERIC_DSCALE(num);
	}

	PG_RETURN_NUMERIC(res);
}

/* ---- numeric.c:7066-7079 VERBATIM (alloc_var) ---- */
/*
 * alloc_var() -
 *
 *	Allocate a digit buffer of ndigits digits (plus a spare digit for rounding)
 */
static void
alloc_var(NumericVar *var, int ndigits)
{
	digitbuf_free(var->buf);
	var->buf = digitbuf_alloc(ndigits + 1);
	var->buf[0] = 0;			/* spare digit for rounding */
	var->digits = var->buf + 1;
	var->ndigits = ndigits;
}

/* ---- numeric.c:7082-7094 VERBATIM (free_var) ---- */
/*
 * free_var() -
 *
 *	Return the digit buffer of a variable to the free pool
 */
static void
free_var(NumericVar *var)
{
	digitbuf_free(var->buf);
	var->buf = NULL;
	var->digits = NULL;
	var->sign = NUMERIC_NAN;
}

/* ---- numeric.c:7097-7112 VERBATIM (zero_var) ---- */
/*
 * zero_var() -
 *
 *	Set a variable to ZERO.
 *	Note: its dscale is not touched.
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

/* ---- numeric.c:7115-7327 VERBATIM (set_var_from_str) ---- */
/*
 * set_var_from_str()
 *
 *	Parse a string and put the number into a variable
 *
 * This function does not handle leading or trailing spaces.  It returns
 * the end+1 position parsed into *endptr, so that caller can check for
 * trailing spaces/garbage if deemed necessary.
 *
 * cp is the place to actually start parsing; str is what to use in error
 * reports.  (Typically cp would be the same except advanced over spaces.)
 *
 * Returns true on success, false on failure (if escontext points to an
 * ErrorSaveContext; otherwise errors are thrown).
 */
static bool
set_var_from_str(const char *str, const char *cp,
				 NumericVar *dest, const char **endptr,
				 Node *escontext)
{
	bool		have_dp = false;
	int			i;
	unsigned char *decdigits;
	int			sign = NUMERIC_POS;
	int			dweight = -1;
	int			ddigits;
	int			dscale = 0;
	int			weight;
	int			ndigits;
	int			offset;
	NumericDigit *digits;

	/*
	 * We first parse the string to extract decimal digits and determine the
	 * correct decimal weight.  Then convert to NBASE representation.
	 */
	switch (*cp)
	{
		case '+':
			sign = NUMERIC_POS;
			cp++;
			break;

		case '-':
			sign = NUMERIC_NEG;
			cp++;
			break;
	}

	if (*cp == '.')
	{
		have_dp = true;
		cp++;
	}

	if (!isdigit((unsigned char) *cp))
		goto invalid_syntax;

	decdigits = (unsigned char *) palloc(strlen(cp) + DEC_DIGITS * 2);

	/* leading padding for digit alignment later */
	memset(decdigits, 0, DEC_DIGITS);
	i = DEC_DIGITS;

	while (*cp)
	{
		if (isdigit((unsigned char) *cp))
		{
			decdigits[i++] = *cp++ - '0';
			if (!have_dp)
				dweight++;
			else
				dscale++;
		}
		else if (*cp == '.')
		{
			if (have_dp)
				goto invalid_syntax;
			have_dp = true;
			cp++;
			/* decimal point must not be followed by underscore */
			if (*cp == '_')
				goto invalid_syntax;
		}
		else if (*cp == '_')
		{
			/* underscore must be followed by more digits */
			cp++;
			if (!isdigit((unsigned char) *cp))
				goto invalid_syntax;
		}
		else
			break;
	}

	ddigits = i - DEC_DIGITS;
	/* trailing padding for digit alignment later */
	memset(decdigits + i, 0, DEC_DIGITS - 1);

	/* Handle exponent, if any */
	if (*cp == 'e' || *cp == 'E')
	{
		int64		exponent = 0;
		bool		neg = false;

		/*
		 * At this point, dweight and dscale can't be more than about
		 * INT_MAX/2 due to the MaxAllocSize limit on string length, so
		 * constraining the exponent similarly should be enough to prevent
		 * integer overflow in this function.  If the value is too large to
		 * fit in storage format, make_result() will complain about it later;
		 * for consistency use the same ereport errcode/text as make_result().
		 */

		/* exponent sign */
		cp++;
		if (*cp == '+')
			cp++;
		else if (*cp == '-')
		{
			neg = true;
			cp++;
		}

		/* exponent digits */
		if (!isdigit((unsigned char) *cp))
			goto invalid_syntax;

		while (*cp)
		{
			if (isdigit((unsigned char) *cp))
			{
				exponent = exponent * 10 + (*cp++ - '0');
				if (exponent > PG_INT32_MAX / 2)
					goto out_of_range;
			}
			else if (*cp == '_')
			{
				/* underscore must be followed by more digits */
				cp++;
				if (!isdigit((unsigned char) *cp))
					goto invalid_syntax;
			}
			else
				break;
		}

		if (neg)
			exponent = -exponent;

		dweight += (int) exponent;
		dscale -= (int) exponent;
		if (dscale < 0)
			dscale = 0;
	}

	/*
	 * Okay, convert pure-decimal representation to base NBASE.  First we need
	 * to determine the converted weight and ndigits.  offset is the number of
	 * decimal zeroes to insert before the first given digit to have a
	 * correctly aligned first NBASE digit.
	 */
	if (dweight >= 0)
		weight = (dweight + 1 + DEC_DIGITS - 1) / DEC_DIGITS - 1;
	else
		weight = -((-dweight - 1) / DEC_DIGITS + 1);
	offset = (weight + 1) * DEC_DIGITS - (dweight + 1);
	ndigits = (ddigits + offset + DEC_DIGITS - 1) / DEC_DIGITS;

	alloc_var(dest, ndigits);
	dest->sign = sign;
	dest->weight = weight;
	dest->dscale = dscale;

	i = DEC_DIGITS - offset;
	digits = dest->digits;

	while (ndigits-- > 0)
	{
#if DEC_DIGITS == 4
		*digits++ = ((decdigits[i] * 10 + decdigits[i + 1]) * 10 +
					 decdigits[i + 2]) * 10 + decdigits[i + 3];
#elif DEC_DIGITS == 2
		*digits++ = decdigits[i] * 10 + decdigits[i + 1];
#elif DEC_DIGITS == 1
		*digits++ = decdigits[i];
#else
#error unsupported NBASE
#endif
		i += DEC_DIGITS;
	}

	pfree(decdigits);

	/* Strip any leading/trailing zeroes, and normalize weight if zero */
	strip_var(dest);

	/* Return end+1 position for caller */
	*endptr = cp;

	return true;

out_of_range:
	ereturn(escontext, false,
			(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
			 errmsg("value overflows numeric format")));

invalid_syntax:
	ereturn(escontext, false,
			(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
			 errmsg("invalid input syntax for type %s: \"%s\"",
					"numeric", str)));
}

/* ---- numeric.c:7330-7339 VERBATIM (xdigit_value) ---- */
/*
 * Return the numeric value of a single hex digit.
 */
static inline int
xdigit_value(char dig)
{
	return dig >= '0' && dig <= '9' ? dig - '0' :
		dig >= 'a' && dig <= 'f' ? dig - 'a' + 10 :
		dig >= 'A' && dig <= 'F' ? dig - 'A' + 10 : -1;
}

/* ---- numeric.c:7341-7530 VERBATIM (set_var_from_non_decimal_integer_str) ---- */
/*
 * set_var_from_non_decimal_integer_str()
 *
 *	Parse a string containing a non-decimal integer
 *
 * This function does not handle leading or trailing spaces.  It returns
 * the end+1 position parsed into *endptr, so that caller can check for
 * trailing spaces/garbage if deemed necessary.
 *
 * cp is the place to actually start parsing; str is what to use in error
 * reports.  The number's sign and base prefix indicator (e.g., "0x") are
 * assumed to have already been parsed, so cp should point to the number's
 * first digit in the base specified.
 *
 * base is expected to be 2, 8 or 16.
 *
 * Returns true on success, false on failure (if escontext points to an
 * ErrorSaveContext; otherwise errors are thrown).
 */
static bool
set_var_from_non_decimal_integer_str(const char *str, const char *cp, int sign,
									 int base, NumericVar *dest,
									 const char **endptr, Node *escontext)
{
	const char *firstdigit = cp;
	int64		tmp;
	int64		mul;
	NumericVar	tmp_var;

	init_var(&tmp_var);

	zero_var(dest);

	/*
	 * Process input digits in groups that fit in int64.  Here "tmp" is the
	 * value of the digits in the group, and "mul" is base^n, where n is the
	 * number of digits in the group.  Thus tmp < mul, and we must start a new
	 * group when mul * base threatens to overflow PG_INT64_MAX.
	 */
	tmp = 0;
	mul = 1;

	if (base == 16)
	{
		while (*cp)
		{
			if (isxdigit((unsigned char) *cp))
			{
				if (mul > PG_INT64_MAX / 16)
				{
					/* Add the contribution from this group of digits */
					int64_to_numericvar(mul, &tmp_var);
					mul_var(dest, &tmp_var, dest, 0);
					int64_to_numericvar(tmp, &tmp_var);
					add_var(dest, &tmp_var, dest);

					/* Result will overflow if weight overflows int16 */
					if (dest->weight > NUMERIC_WEIGHT_MAX)
						goto out_of_range;

					/* Begin a new group */
					tmp = 0;
					mul = 1;
				}

				tmp = tmp * 16 + xdigit_value(*cp++);
				mul = mul * 16;
			}
			else if (*cp == '_')
			{
				/* Underscore must be followed by more digits */
				cp++;
				if (!isxdigit((unsigned char) *cp))
					goto invalid_syntax;
			}
			else
				break;
		}
	}
	else if (base == 8)
	{
		while (*cp)
		{
			if (*cp >= '0' && *cp <= '7')
			{
				if (mul > PG_INT64_MAX / 8)
				{
					/* Add the contribution from this group of digits */
					int64_to_numericvar(mul, &tmp_var);
					mul_var(dest, &tmp_var, dest, 0);
					int64_to_numericvar(tmp, &tmp_var);
					add_var(dest, &tmp_var, dest);

					/* Result will overflow if weight overflows int16 */
					if (dest->weight > NUMERIC_WEIGHT_MAX)
						goto out_of_range;

					/* Begin a new group */
					tmp = 0;
					mul = 1;
				}

				tmp = tmp * 8 + (*cp++ - '0');
				mul = mul * 8;
			}
			else if (*cp == '_')
			{
				/* Underscore must be followed by more digits */
				cp++;
				if (*cp < '0' || *cp > '7')
					goto invalid_syntax;
			}
			else
				break;
		}
	}
	else if (base == 2)
	{
		while (*cp)
		{
			if (*cp >= '0' && *cp <= '1')
			{
				if (mul > PG_INT64_MAX / 2)
				{
					/* Add the contribution from this group of digits */
					int64_to_numericvar(mul, &tmp_var);
					mul_var(dest, &tmp_var, dest, 0);
					int64_to_numericvar(tmp, &tmp_var);
					add_var(dest, &tmp_var, dest);

					/* Result will overflow if weight overflows int16 */
					if (dest->weight > NUMERIC_WEIGHT_MAX)
						goto out_of_range;

					/* Begin a new group */
					tmp = 0;
					mul = 1;
				}

				tmp = tmp * 2 + (*cp++ - '0');
				mul = mul * 2;
			}
			else if (*cp == '_')
			{
				/* Underscore must be followed by more digits */
				cp++;
				if (*cp < '0' || *cp > '1')
					goto invalid_syntax;
			}
			else
				break;
		}
	}
	else
		/* Should never happen; treat as invalid input */
		goto invalid_syntax;

	/* Check that we got at least one digit */
	if (unlikely(cp == firstdigit))
		goto invalid_syntax;

	/* Add the contribution from the final group of digits */
	int64_to_numericvar(mul, &tmp_var);
	mul_var(dest, &tmp_var, dest, 0);
	int64_to_numericvar(tmp, &tmp_var);
	add_var(dest, &tmp_var, dest);

	if (dest->weight > NUMERIC_WEIGHT_MAX)
		goto out_of_range;

	dest->sign = sign;

	free_var(&tmp_var);

	/* Return end+1 position for caller */
	*endptr = cp;

	return true;

out_of_range:
	ereturn(escontext, false,
			(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
			 errmsg("value overflows numeric format")));

invalid_syntax:
	ereturn(escontext, false,
			(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
			 errmsg("invalid input syntax for type %s: \"%s\"",
					"numeric", str)));
}

/* ---- numeric.c:7555-7578 VERBATIM (init_var_from_num) ---- */
/*
 * init_var_from_num() -
 *
 *	Initialize a variable from packed db format. The digits array is not
 *	copied, which saves some cycles when the resulting var is not modified.
 *	Also, there's no need to call free_var(), as long as you don't assign any
 *	other value to it (with set_var_* functions, or by using the var as the
 *	destination of a function like add_var())
 *
 *	CAUTION: Do not modify the digits buffer of a var initialized with this
 *	function, e.g by calling round_var() or trunc_var(), as the changes will
 *	propagate to the original Numeric! It's OK to use it as the destination
 *	argument of one of the calculational functions, though.
 */
static void
init_var_from_num(Numeric num, NumericVar *dest)
{
	dest->ndigits = NUMERIC_NDIGITS(num);
	dest->weight = NUMERIC_WEIGHT(num);
	dest->sign = NUMERIC_SIGN(num);
	dest->dscale = NUMERIC_DSCALE(num);
	dest->digits = NUMERIC_DIGITS(num);
	dest->buf = NULL;			/* digits array is not palloc'd */
}

/* ---- numeric.c:7581-7602 VERBATIM (set_var_from_var) ---- */
/*
 * set_var_from_var() -
 *
 *	Copy one variable into another
 */
static void
set_var_from_var(const NumericVar *value, NumericVar *dest)
{
	NumericDigit *newbuf;

	newbuf = digitbuf_alloc(value->ndigits + 1);
	newbuf[0] = 0;				/* spare digit for rounding */
	if (value->ndigits > 0)		/* else value->digits might be null */
		memcpy(newbuf + 1, value->digits,
			   value->ndigits * sizeof(NumericDigit));

	digitbuf_free(dest->buf);

	memmove(dest, value, sizeof(NumericVar));
	dest->buf = newbuf;
	dest->digits = newbuf + 1;
}

/* ---- numeric.c:7605-7741 VERBATIM (get_str_from_var) ---- */
/*
 * get_str_from_var() -
 *
 *	Convert a var to text representation (guts of numeric_out).
 *	The var is displayed to the number of digits indicated by its dscale.
 *	Returns a palloc'd string.
 */
static char *
get_str_from_var(const NumericVar *var)
{
	int			dscale;
	char	   *str;
	char	   *cp;
	char	   *endcp;
	int			i;
	int			d;
	NumericDigit dig;

#if DEC_DIGITS > 1
	NumericDigit d1;
#endif

	dscale = var->dscale;

	/*
	 * Allocate space for the result.
	 *
	 * i is set to the # of decimal digits before decimal point. dscale is the
	 * # of decimal digits we will print after decimal point. We may generate
	 * as many as DEC_DIGITS-1 excess digits at the end, and in addition we
	 * need room for sign, decimal point, null terminator.
	 */
	i = (var->weight + 1) * DEC_DIGITS;
	if (i <= 0)
		i = 1;

	str = palloc(i + dscale + DEC_DIGITS + 2);
	cp = str;

	/*
	 * Output a dash for negative values
	 */
	if (var->sign == NUMERIC_NEG)
		*cp++ = '-';

	/*
	 * Output all digits before the decimal point
	 */
	if (var->weight < 0)
	{
		d = var->weight + 1;
		*cp++ = '0';
	}
	else
	{
		for (d = 0; d <= var->weight; d++)
		{
			dig = (d < var->ndigits) ? var->digits[d] : 0;
			/* In the first digit, suppress extra leading decimal zeroes */
#if DEC_DIGITS == 4
			{
				bool		putit = (d > 0);

				d1 = dig / 1000;
				dig -= d1 * 1000;
				putit |= (d1 > 0);
				if (putit)
					*cp++ = d1 + '0';
				d1 = dig / 100;
				dig -= d1 * 100;
				putit |= (d1 > 0);
				if (putit)
					*cp++ = d1 + '0';
				d1 = dig / 10;
				dig -= d1 * 10;
				putit |= (d1 > 0);
				if (putit)
					*cp++ = d1 + '0';
				*cp++ = dig + '0';
			}
#elif DEC_DIGITS == 2
			d1 = dig / 10;
			dig -= d1 * 10;
			if (d1 > 0 || d > 0)
				*cp++ = d1 + '0';
			*cp++ = dig + '0';
#elif DEC_DIGITS == 1
			*cp++ = dig + '0';
#else
#error unsupported NBASE
#endif
		}
	}

	/*
	 * If requested, output a decimal point and all the digits that follow it.
	 * We initially put out a multiple of DEC_DIGITS digits, then truncate if
	 * needed.
	 */
	if (dscale > 0)
	{
		*cp++ = '.';
		endcp = cp + dscale;
		for (i = 0; i < dscale; d++, i += DEC_DIGITS)
		{
			dig = (d >= 0 && d < var->ndigits) ? var->digits[d] : 0;
#if DEC_DIGITS == 4
			d1 = dig / 1000;
			dig -= d1 * 1000;
			*cp++ = d1 + '0';
			d1 = dig / 100;
			dig -= d1 * 100;
			*cp++ = d1 + '0';
			d1 = dig / 10;
			dig -= d1 * 10;
			*cp++ = d1 + '0';
			*cp++ = dig + '0';
#elif DEC_DIGITS == 2
			d1 = dig / 10;
			dig -= d1 * 10;
			*cp++ = d1 + '0';
			*cp++ = dig + '0';
#elif DEC_DIGITS == 1
			*cp++ = dig + '0';
#else
#error unsupported NBASE
#endif
		}
		cp = endcp;
	}

	/*
	 * terminate the string and return it
	 */
	*cp = '\0';
	return str;
}

/* ---- numeric.c:7876-7889 VERBATIM (duplicate_numeric) ---- */
/*
 * duplicate_numeric() - copy a packed-format Numeric
 *
 * This will handle NaN and Infinity cases.
 */
static Numeric
duplicate_numeric(Numeric num)
{
	Numeric		res;

	res = (Numeric) palloc(VARSIZE(num));
	memcpy(res, num, VARSIZE(num));
	return res;
}

/* ---- numeric.c:7891-8001 VERBATIM (make_result_opt_error) ---- */
/*
 * make_result_opt_error() -
 *
 *	Create the packed db numeric format in palloc()'d memory from
 *	a variable.  This will handle NaN and Infinity cases.
 *
 *	If "have_error" isn't NULL, on overflow *have_error is set to true and
 *	NULL is returned.  This is helpful when caller needs to handle errors.
 */
static Numeric
make_result_opt_error(const NumericVar *var, bool *have_error)
{
	Numeric		result;
	NumericDigit *digits = var->digits;
	int			weight = var->weight;
	int			sign = var->sign;
	int			n;
	Size		len;

	if (have_error)
		*have_error = false;

	if ((sign & NUMERIC_SIGN_MASK) == NUMERIC_SPECIAL)
	{
		/*
		 * Verify valid special value.  This could be just an Assert, perhaps,
		 * but it seems worthwhile to expend a few cycles to ensure that we
		 * never write any nonzero reserved bits to disk.
		 */
		if (!(sign == NUMERIC_NAN ||
			  sign == NUMERIC_PINF ||
			  sign == NUMERIC_NINF))
			elog(ERROR, "invalid numeric sign value 0x%x", sign);

		result = (Numeric) palloc(NUMERIC_HDRSZ_SHORT);

		SET_VARSIZE(result, NUMERIC_HDRSZ_SHORT);
		result->choice.n_header = sign;
		/* the header word is all we need */

		dump_numeric("make_result()", result);
		return result;
	}

	n = var->ndigits;

	/* truncate leading zeroes */
	while (n > 0 && *digits == 0)
	{
		digits++;
		weight--;
		n--;
	}
	/* truncate trailing zeroes */
	while (n > 0 && digits[n - 1] == 0)
		n--;

	/* If zero result, force to weight=0 and positive sign */
	if (n == 0)
	{
		weight = 0;
		sign = NUMERIC_POS;
	}

	/* Build the result */
	if (NUMERIC_CAN_BE_SHORT(var->dscale, weight))
	{
		len = NUMERIC_HDRSZ_SHORT + n * sizeof(NumericDigit);
		result = (Numeric) palloc(len);
		SET_VARSIZE(result, len);
		result->choice.n_short.n_header =
			(sign == NUMERIC_NEG ? (NUMERIC_SHORT | NUMERIC_SHORT_SIGN_MASK)
			 : NUMERIC_SHORT)
			| (var->dscale << NUMERIC_SHORT_DSCALE_SHIFT)
			| (weight < 0 ? NUMERIC_SHORT_WEIGHT_SIGN_MASK : 0)
			| (weight & NUMERIC_SHORT_WEIGHT_MASK);
	}
	else
	{
		len = NUMERIC_HDRSZ + n * sizeof(NumericDigit);
		result = (Numeric) palloc(len);
		SET_VARSIZE(result, len);
		result->choice.n_long.n_sign_dscale =
			sign | (var->dscale & NUMERIC_DSCALE_MASK);
		result->choice.n_long.n_weight = weight;
	}

	Assert(NUMERIC_NDIGITS(result) == n);
	if (n > 0)
		memcpy(NUMERIC_DIGITS(result), digits, n * sizeof(NumericDigit));

	/* Check for overflow of int16 fields */
	if (NUMERIC_WEIGHT(result) != weight ||
		NUMERIC_DSCALE(result) != var->dscale)
	{
		if (have_error)
		{
			*have_error = true;
			return NULL;
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					 errmsg("value overflows numeric format")));
		}
	}

	dump_numeric("make_result()", result);
	return result;
}

/* ---- numeric.c:8004-8013 VERBATIM (make_result) ---- */
/*
 * make_result() -
 *
 *	An interface to make_result_opt_error() without "have_error" argument.
 */
static Numeric
make_result(const NumericVar *var)
{
	return make_result_opt_error(var, NULL);
}

/* ---- numeric.c:8016-8099 VERBATIM (apply_typmod) ---- */
/*
 * apply_typmod() -
 *
 *	Do bounds checking and rounding according to the specified typmod.
 *	Note that this is only applied to normal finite values.
 *
 * Returns true on success, false on failure (if escontext points to an
 * ErrorSaveContext; otherwise errors are thrown).
 */
static bool
apply_typmod(NumericVar *var, int32 typmod, Node *escontext)
{
	int			precision;
	int			scale;
	int			maxdigits;
	int			ddigits;
	int			i;

	/* Do nothing if we have an invalid typmod */
	if (!is_valid_numeric_typmod(typmod))
		return true;

	precision = numeric_typmod_precision(typmod);
	scale = numeric_typmod_scale(typmod);
	maxdigits = precision - scale;

	/* Round to target scale (and set var->dscale) */
	round_var(var, scale);

	/* but don't allow var->dscale to be negative */
	if (var->dscale < 0)
		var->dscale = 0;

	/*
	 * Check for overflow - note we can't do this before rounding, because
	 * rounding could raise the weight.  Also note that the var's weight could
	 * be inflated by leading zeroes, which will be stripped before storage
	 * but perhaps might not have been yet. In any case, we must recognize a
	 * true zero, whose weight doesn't mean anything.
	 */
	ddigits = (var->weight + 1) * DEC_DIGITS;
	if (ddigits > maxdigits)
	{
		/* Determine true weight; and check for all-zero result */
		for (i = 0; i < var->ndigits; i++)
		{
			NumericDigit dig = var->digits[i];

			if (dig)
			{
				/* Adjust for any high-order decimal zero digits */
#if DEC_DIGITS == 4
				if (dig < 10)
					ddigits -= 3;
				else if (dig < 100)
					ddigits -= 2;
				else if (dig < 1000)
					ddigits -= 1;
#elif DEC_DIGITS == 2
				if (dig < 10)
					ddigits -= 1;
#elif DEC_DIGITS == 1
				/* no adjustment */
#else
#error unsupported NBASE
#endif
				if (ddigits > maxdigits)
					ereturn(escontext, false,
							(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
							 errmsg("numeric field overflow"),
							 errdetail("A field with precision %d, scale %d must round to an absolute value less than %s%d.",
									   precision, scale,
					/* Display 10^0 as 1 */
									   maxdigits ? "10^" : "",
									   maxdigits ? maxdigits : 1
									   )));
				break;
			}
			ddigits -= DEC_DIGITS;
		}
	}

	return true;
}

/* ---- numeric.c:8101-8139 VERBATIM (apply_typmod_special) ---- */
/*
 * apply_typmod_special() -
 *
 *	Do bounds checking according to the specified typmod, for an Inf or NaN.
 *	For convenience of most callers, the value is presented in packed form.
 *
 * Returns true on success, false on failure (if escontext points to an
 * ErrorSaveContext; otherwise errors are thrown).
 */
static bool
apply_typmod_special(Numeric num, int32 typmod, Node *escontext)
{
	int			precision;
	int			scale;

	Assert(NUMERIC_IS_SPECIAL(num));	/* caller error if not */

	/*
	 * NaN is allowed regardless of the typmod; that's rather dubious perhaps,
	 * but it's a longstanding behavior.  Inf is rejected if we have any
	 * typmod restriction, since an infinity shouldn't be claimed to fit in
	 * any finite number of digits.
	 */
	if (NUMERIC_IS_NAN(num))
		return true;

	/* Do nothing if we have a default typmod (-1) */
	if (!is_valid_numeric_typmod(typmod))
		return true;

	precision = numeric_typmod_precision(typmod);
	scale = numeric_typmod_scale(typmod);

	ereturn(escontext, false,
			(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
			 errmsg("numeric field overflow"),
			 errdetail("A field with precision %d, scale %d cannot hold an infinite value.",
					   precision, scale)));
}

/* ---- numeric.c:8219-8262 VERBATIM (int64_to_numericvar) ---- */
/*
 * Convert int8 value to numeric.
 */
static void
int64_to_numericvar(int64 val, NumericVar *var)
{
	uint64		uval,
				newuval;
	NumericDigit *ptr;
	int			ndigits;

	/* int64 can require at most 19 decimal digits; add one for safety */
	alloc_var(var, 20 / DEC_DIGITS);
	if (val < 0)
	{
		var->sign = NUMERIC_NEG;
		uval = pg_abs_s64(val);
	}
	else
	{
		var->sign = NUMERIC_POS;
		uval = val;
	}
	var->dscale = 0;
	if (val == 0)
	{
		var->ndigits = 0;
		var->weight = 0;
		return;
	}
	ptr = var->digits + var->ndigits;
	ndigits = 0;
	do
	{
		ptr--;
		ndigits++;
		newuval = uval / NBASE;
		*ptr = uval - newuval * NBASE;
		uval = newuval;
	} while (uval);
	var->digits = ptr;
	var->ndigits = ndigits;
	var->weight = ndigits - 1;
}

/* ---- numeric.c:8543-8657 VERBATIM (add_var) ---- */
/*
 * add_var() -
 *
 *	Full version of add functionality on variable level (handling signs).
 *	result might point to one of the operands too without danger.
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
					/* ----------
					 * ABS(var1) == ABS(var2)
					 * result = ZERO
					 * ----------
					 */
					zero_var(result);
					result->dscale = Max(var1->dscale, var2->dscale);
					break;

				case 1:
					/* ----------
					 * ABS(var1) > ABS(var2)
					 * result = +(ABS(var1) - ABS(var2))
					 * ----------
					 */
					sub_abs(var1, var2, result);
					result->sign = NUMERIC_POS;
					break;

				case -1:
					/* ----------
					 * ABS(var1) < ABS(var2)
					 * result = -(ABS(var2) - ABS(var1))
					 * ----------
					 */
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
			/* ----------
			 * var1 is negative, var2 is positive
			 * Must compare absolute values
			 * ----------
			 */
			switch (cmp_abs(var1, var2))
			{
				case 0:
					/* ----------
					 * ABS(var1) == ABS(var2)
					 * result = ZERO
					 * ----------
					 */
					zero_var(result);
					result->dscale = Max(var1->dscale, var2->dscale);
					break;

				case 1:
					/* ----------
					 * ABS(var1) > ABS(var2)
					 * result = -(ABS(var1) - ABS(var2))
					 * ----------
					 */
					sub_abs(var1, var2, result);
					result->sign = NUMERIC_NEG;
					break;

				case -1:
					/* ----------
					 * ABS(var1) < ABS(var2)
					 * result = +(ABS(var2) - ABS(var1))
					 * ----------
					 */
					sub_abs(var2, var1, result);
					result->sign = NUMERIC_POS;
					break;
			}
		}
		else
		{
			/* ----------
			 * Both are negative
			 * result = -(ABS(var1) + ABS(var2))
			 * ----------
			 */
			add_abs(var1, var2, result);
			result->sign = NUMERIC_NEG;
		}
	}
}

/* ---- numeric.c:8781-9068 VERBATIM (mul_var) ---- */
/*
 * mul_var() -
 *
 *	Multiplication on variable level. Product of var1 * var2 is stored
 *	in result.  Result is rounded to no more than rscale fractional digits.
 */
static void
mul_var(const NumericVar *var1, const NumericVar *var2, NumericVar *result,
		int rscale)
{
	int			res_ndigits;
	int			res_ndigitpairs;
	int			res_sign;
	int			res_weight;
	int			pair_offset;
	int			maxdigits;
	int			maxdigitpairs;
	uint64	   *dig,
			   *dig_i1_off;
	uint64		maxdig;
	uint64		carry;
	uint64		newdig;
	int			var1ndigits;
	int			var2ndigits;
	int			var1ndigitpairs;
	int			var2ndigitpairs;
	NumericDigit *var1digits;
	NumericDigit *var2digits;
	uint32		var1digitpair;
	uint32	   *var2digitpairs;
	NumericDigit *res_digits;
	int			i,
				i1,
				i2,
				i2limit;

	/*
	 * Arrange for var1 to be the shorter of the two numbers.  This improves
	 * performance because the inner multiplication loop is much simpler than
	 * the outer loop, so it's better to have a smaller number of iterations
	 * of the outer loop.  This also reduces the number of times that the
	 * accumulator array needs to be normalized.
	 */
	if (var1->ndigits > var2->ndigits)
	{
		const NumericVar *tmp = var1;

		var1 = var2;
		var2 = tmp;
	}

	/* copy these values into local vars for speed in inner loop */
	var1ndigits = var1->ndigits;
	var2ndigits = var2->ndigits;
	var1digits = var1->digits;
	var2digits = var2->digits;

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

	/* Determine result sign */
	if (var1->sign == var2->sign)
		res_sign = NUMERIC_POS;
	else
		res_sign = NUMERIC_NEG;

	/*
	 * Determine the number of result digits to compute and the (maximum
	 * possible) result weight.  If the exact result would have more than
	 * rscale fractional digits, truncate the computation with
	 * MUL_GUARD_DIGITS guard digits, i.e., ignore input digits that would
	 * only contribute to the right of that.  (This will give the exact
	 * rounded-to-rscale answer unless carries out of the ignored positions
	 * would have propagated through more than MUL_GUARD_DIGITS digits.)
	 *
	 * Note: an exact computation could not produce more than var1ndigits +
	 * var2ndigits digits, but we allocate at least one extra output digit in
	 * case rscale-driven rounding produces a carry out of the highest exact
	 * digit.
	 *
	 * The computation itself is done using base-NBASE^2 arithmetic, so we
	 * actually process the input digits in pairs, producing a base-NBASE^2
	 * intermediate result.  This significantly improves performance, since
	 * schoolbook multiplication is O(N^2) in the number of input digits, and
	 * working in base NBASE^2 effectively halves "N".
	 *
	 * Note: in a truncated computation, we must compute at least one extra
	 * output digit to ensure that all the guard digits are fully computed.
	 */
	/* digit pairs in each input */
	var1ndigitpairs = (var1ndigits + 1) / 2;
	var2ndigitpairs = (var2ndigits + 1) / 2;

	/* digits in exact result */
	res_ndigits = var1ndigits + var2ndigits;

	/* digit pairs in exact result with at least one extra output digit */
	res_ndigitpairs = res_ndigits / 2 + 1;

	/* pair offset to align result to end of dig[] */
	pair_offset = res_ndigitpairs - var1ndigitpairs - var2ndigitpairs + 1;

	/* maximum possible result weight (odd-length inputs shifted up below) */
	res_weight = var1->weight + var2->weight + 1 + 2 * res_ndigitpairs -
		res_ndigits - (var1ndigits & 1) - (var2ndigits & 1);

	/* rscale-based truncation with at least one extra output digit */
	maxdigits = res_weight + 1 + (rscale + DEC_DIGITS - 1) / DEC_DIGITS +
		MUL_GUARD_DIGITS;
	maxdigitpairs = maxdigits / 2 + 1;

	res_ndigitpairs = Min(res_ndigitpairs, maxdigitpairs);
	res_ndigits = 2 * res_ndigitpairs;

	/*
	 * In the computation below, digit pair i1 of var1 and digit pair i2 of
	 * var2 are multiplied and added to digit i1+i2+pair_offset of dig[]. Thus
	 * input digit pairs with index >= res_ndigitpairs - pair_offset don't
	 * contribute to the result, and can be ignored.
	 */
	if (res_ndigitpairs <= pair_offset)
	{
		/* All input digits will be ignored; so result is zero */
		zero_var(result);
		result->dscale = rscale;
		return;
	}
	var1ndigitpairs = Min(var1ndigitpairs, res_ndigitpairs - pair_offset);
	var2ndigitpairs = Min(var2ndigitpairs, res_ndigitpairs - pair_offset);

	/*
	 * We do the arithmetic in an array "dig[]" of unsigned 64-bit integers.
	 * Since PG_UINT64_MAX is much larger than NBASE^4, this gives us a lot of
	 * headroom to avoid normalizing carries immediately.
	 *
	 * maxdig tracks the maximum possible value of any dig[] entry; when this
	 * threatens to exceed PG_UINT64_MAX, we take the time to propagate
	 * carries.  Furthermore, we need to ensure that overflow doesn't occur
	 * during the carry propagation passes either.  The carry values could be
	 * as much as PG_UINT64_MAX / NBASE^2, so really we must normalize when
	 * digits threaten to exceed PG_UINT64_MAX - PG_UINT64_MAX / NBASE^2.
	 *
	 * To avoid overflow in maxdig itself, it actually represents the maximum
	 * possible value divided by NBASE^2-1, i.e., at the top of the loop it is
	 * known that no dig[] entry exceeds maxdig * (NBASE^2-1).
	 *
	 * The conversion of var1 to base NBASE^2 is done on the fly, as each new
	 * digit is required.  The digits of var2 are converted upfront, and
	 * stored at the end of dig[].  To avoid loss of precision, the input
	 * digits are aligned with the start of digit pair array, effectively
	 * shifting them up (multiplying by NBASE) if the inputs have an odd
	 * number of NBASE digits.
	 */
	dig = (uint64 *) palloc(res_ndigitpairs * sizeof(uint64) +
							var2ndigitpairs * sizeof(uint32));

	/* convert var2 to base NBASE^2, shifting up if its length is odd */
	var2digitpairs = (uint32 *) (dig + res_ndigitpairs);

	for (i2 = 0; i2 < var2ndigitpairs - 1; i2++)
		var2digitpairs[i2] = var2digits[2 * i2] * NBASE + var2digits[2 * i2 + 1];

	if (2 * i2 + 1 < var2ndigits)
		var2digitpairs[i2] = var2digits[2 * i2] * NBASE + var2digits[2 * i2 + 1];
	else
		var2digitpairs[i2] = var2digits[2 * i2] * NBASE;

	/*
	 * Start by multiplying var2 by the least significant contributing digit
	 * pair from var1, storing the results at the end of dig[], and filling
	 * the leading digits with zeros.
	 *
	 * The loop here is the same as the inner loop below, except that we set
	 * the results in dig[], rather than adding to them.  This is the
	 * performance bottleneck for multiplication, so we want to keep it simple
	 * enough so that it can be auto-vectorized.  Accordingly, process the
	 * digits left-to-right even though schoolbook multiplication would
	 * suggest right-to-left.  Since we aren't propagating carries in this
	 * loop, the order does not matter.
	 */
	i1 = var1ndigitpairs - 1;
	if (2 * i1 + 1 < var1ndigits)
		var1digitpair = var1digits[2 * i1] * NBASE + var1digits[2 * i1 + 1];
	else
		var1digitpair = var1digits[2 * i1] * NBASE;
	maxdig = var1digitpair;

	i2limit = Min(var2ndigitpairs, res_ndigitpairs - i1 - pair_offset);
	dig_i1_off = &dig[i1 + pair_offset];

	memset(dig, 0, (i1 + pair_offset) * sizeof(uint64));
	for (i2 = 0; i2 < i2limit; i2++)
		dig_i1_off[i2] = (uint64) var1digitpair * var2digitpairs[i2];

	/*
	 * Next, multiply var2 by the remaining digit pairs from var1, adding the
	 * results to dig[] at the appropriate offsets, and normalizing whenever
	 * there is a risk of any dig[] entry overflowing.
	 */
	for (i1 = i1 - 1; i1 >= 0; i1--)
	{
		var1digitpair = var1digits[2 * i1] * NBASE + var1digits[2 * i1 + 1];
		if (var1digitpair == 0)
			continue;

		/* Time to normalize? */
		maxdig += var1digitpair;
		if (maxdig > (PG_UINT64_MAX - PG_UINT64_MAX / NBASE_SQR) / (NBASE_SQR - 1))
		{
			/* Yes, do it (to base NBASE^2) */
			carry = 0;
			for (i = res_ndigitpairs - 1; i >= 0; i--)
			{
				newdig = dig[i] + carry;
				if (newdig >= NBASE_SQR)
				{
					carry = newdig / NBASE_SQR;
					newdig -= carry * NBASE_SQR;
				}
				else
					carry = 0;
				dig[i] = newdig;
			}
			Assert(carry == 0);
			/* Reset maxdig to indicate new worst-case */
			maxdig = 1 + var1digitpair;
		}

		/* Multiply and add */
		i2limit = Min(var2ndigitpairs, res_ndigitpairs - i1 - pair_offset);
		dig_i1_off = &dig[i1 + pair_offset];

		for (i2 = 0; i2 < i2limit; i2++)
			dig_i1_off[i2] += (uint64) var1digitpair * var2digitpairs[i2];
	}

	/*
	 * Now we do a final carry propagation pass to normalize back to base
	 * NBASE^2, and construct the base-NBASE result digits.  Note that this is
	 * still done at full precision w/guard digits.
	 */
	alloc_var(result, res_ndigits);
	res_digits = result->digits;
	carry = 0;
	for (i = res_ndigitpairs - 1; i >= 0; i--)
	{
		newdig = dig[i] + carry;
		if (newdig >= NBASE_SQR)
		{
			carry = newdig / NBASE_SQR;
			newdig -= carry * NBASE_SQR;
		}
		else
			carry = 0;
		res_digits[2 * i + 1] = (NumericDigit) ((uint32) newdig % NBASE);
		res_digits[2 * i] = (NumericDigit) ((uint32) newdig / NBASE);
	}
	Assert(carry == 0);

	pfree(dig);

	/*
	 * Finally, round the result to the requested precision.
	 */
	result->weight = res_weight;
	result->sign = res_sign;

	/* Round to target rscale (and set result->dscale) */
	round_var(result, rscale);

	/* Strip leading and trailing zeroes */
	strip_var(result);
}

/* ---- numeric.c:9071-9344 VERBATIM (mul_var_short) ---- */
/*
 * mul_var_short() -
 *
 *	Special-case multiplication function used when var1 has 1-6 digits, var2
 *	has at least as many digits as var1, and the exact product var1 * var2 is
 *	requested.
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
	 * The weight figured here is correct if the product has no leading zero
	 * digits; otherwise strip_var() will fix things up.  Note that, unlike
	 * mul_var(), we do not need to allocate an extra output digit, because we
	 * are not rounding here.
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

	/*
	 * Compute the result digits in reverse, in one pass, propagating the
	 * carry up as we go.  The i'th result digit consists of the sum of the
	 * products var1digits[i1] * var2digits[i2] for which i = i1 + i2 + 1.
	 */
#define PRODSUM1(v1,i1,v2,i2) ((v1)[(i1)] * (v2)[(i2)])
#define PRODSUM2(v1,i1,v2,i2) (PRODSUM1(v1,i1,v2,i2) + (v1)[(i1)+1] * (v2)[(i2)-1])
#define PRODSUM3(v1,i1,v2,i2) (PRODSUM2(v1,i1,v2,i2) + (v1)[(i1)+2] * (v2)[(i2)-2])
#define PRODSUM4(v1,i1,v2,i2) (PRODSUM3(v1,i1,v2,i2) + (v1)[(i1)+3] * (v2)[(i2)-3])
#define PRODSUM5(v1,i1,v2,i2) (PRODSUM4(v1,i1,v2,i2) + (v1)[(i1)+4] * (v2)[(i2)-4])
#define PRODSUM6(v1,i1,v2,i2) (PRODSUM5(v1,i1,v2,i2) + (v1)[(i1)+5] * (v2)[(i2)-5])

	switch (var1ndigits)
	{
		case 1:
			/* ---------
			 * 1-digit case:
			 *		var1ndigits = 1
			 *		var2ndigits >= 1
			 *		res_ndigits = var2ndigits + 1
			 * ----------
			 */
			for (int i = var2ndigits - 1; i >= 0; i--)
			{
				term = PRODSUM1(var1digits, 0, var2digits, i) + carry;
				res_digits[i + 1] = (NumericDigit) (term % NBASE);
				carry = term / NBASE;
			}
			res_digits[0] = (NumericDigit) carry;
			break;

		case 2:
			/* ---------
			 * 2-digit case:
			 *		var1ndigits = 2
			 *		var2ndigits >= 2
			 *		res_ndigits = var2ndigits + 2
			 * ----------
			 */
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
			/* ---------
			 * 3-digit case:
			 *		var1ndigits = 3
			 *		var2ndigits >= 3
			 *		res_ndigits = var2ndigits + 3
			 * ----------
			 */
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
			/* ---------
			 * 4-digit case:
			 *		var1ndigits = 4
			 *		var2ndigits >= 4
			 *		res_ndigits = var2ndigits + 4
			 * ----------
			 */
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
			/* ---------
			 * 5-digit case:
			 *		var1ndigits = 5
			 *		var2ndigits >= 5
			 *		res_ndigits = var2ndigits + 5
			 * ----------
			 */
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
			/* ---------
			 * 6-digit case:
			 *		var1ndigits = 6
			 *		var2ndigits >= 6
			 *		res_ndigits = var2ndigits + 6
			 * ----------
			 */
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

/* ---- numeric.c:11854-11868 VERBATIM (cmp_abs) ---- */
/* ----------
 * cmp_abs() -
 *
 *	Compare the absolute values of var1 and var2
 *	Returns:	-1 for ABS(var1) < ABS(var2)
 *				0  for ABS(var1) == ABS(var2)
 *				1  for ABS(var1) > ABS(var2)
 * ----------
 */
static int
cmp_abs(const NumericVar *var1, const NumericVar *var2)
{
	return cmp_abs_common(var1->digits, var1->ndigits, var1->weight,
						  var2->digits, var2->ndigits, var2->weight);
}

/* ---- numeric.c:11870-11932 VERBATIM (cmp_abs_common) ---- */
/* ----------
 * cmp_abs_common() -
 *
 *	Main routine of cmp_abs(). This function can be used by both
 *	NumericVar and Numeric.
 * ----------
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

/* ---- numeric.c:11935-12014 VERBATIM (add_abs) ---- */
/*
 * add_abs() -
 *
 *	Add the absolute values of two variables into result.
 *	result might point to one of the operands without danger.
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

/* ---- numeric.c:12017-12099 VERBATIM (sub_abs) ---- */
/*
 * sub_abs()
 *
 *	Subtract the absolute value of var2 from the absolute value of var1
 *	and store in result. result might point to one of the operands
 *	without danger.
 *
 *	ABS(var1) MUST BE GREATER OR EQUAL ABS(var2) !!!
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

/* ---- numeric.c:12101-12205 VERBATIM (round_var) ---- */
/*
 * round_var
 *
 * Round the value of a variable to no more than rscale decimal digits
 * after the decimal point.  NOTE: we allow rscale < 0 here, implying
 * rounding before the decimal point.
 */
static void
round_var(NumericVar *var, int rscale)
{
	NumericDigit *digits = var->digits;
	int			di;
	int			ndigits;
	int			carry;

	var->dscale = rscale;

	/* decimal digits wanted */
	di = (var->weight + 1) * DEC_DIGITS + rscale;

	/*
	 * If di = 0, the value loses all digits, but could round up to 1 if its
	 * first extra digit is >= 5.  If di < 0 the result must be 0.
	 */
	if (di < 0)
	{
		var->ndigits = 0;
		var->weight = 0;
		var->sign = NUMERIC_POS;
	}
	else
	{
		/* NBASE digits wanted */
		ndigits = (di + DEC_DIGITS - 1) / DEC_DIGITS;

		/* 0, or number of decimal digits to keep in last NBASE digit */
		di %= DEC_DIGITS;

		if (ndigits < var->ndigits ||
			(ndigits == var->ndigits && di > 0))
		{
			var->ndigits = ndigits;

#if DEC_DIGITS == 1
			/* di must be zero */
			carry = (digits[ndigits] >= HALF_NBASE) ? 1 : 0;
#else
			if (di == 0)
				carry = (digits[ndigits] >= HALF_NBASE) ? 1 : 0;
			else
			{
				/* Must round within last NBASE digit */
				int			extra,
							pow10;

#if DEC_DIGITS == 4
				pow10 = round_powers[di];
#elif DEC_DIGITS == 2
				pow10 = 10;
#else
#error unsupported NBASE
#endif
				extra = digits[--ndigits] % pow10;
				digits[ndigits] -= extra;
				carry = 0;
				if (extra >= pow10 / 2)
				{
					pow10 += digits[ndigits];
					if (pow10 >= NBASE)
					{
						pow10 -= NBASE;
						carry = 1;
					}
					digits[ndigits] = pow10;
				}
			}
#endif

			/* Propagate carry if needed */
			while (carry)
			{
				carry += digits[--ndigits];
				if (carry >= NBASE)
				{
					digits[ndigits] = carry - NBASE;
					carry = 1;
				}
				else
				{
					digits[ndigits] = carry;
					carry = 0;
				}
			}

			if (ndigits < 0)
			{
				Assert(ndigits == -1);	/* better not have added > 1 digit */
				Assert(var->digits > var->buf);
				var->digits--;
				var->ndigits++;
				var->weight++;
			}
		}
	}
}

/* ---- numeric.c:12207-12269 VERBATIM (trunc_var) ---- */
/*
 * trunc_var
 *
 * Truncate (towards zero) the value of a variable at rscale decimal digits
 * after the decimal point.  NOTE: we allow rscale < 0 here, implying
 * truncation before the decimal point.
 */
static void
trunc_var(NumericVar *var, int rscale)
{
	int			di;
	int			ndigits;

	var->dscale = rscale;

	/* decimal digits wanted */
	di = (var->weight + 1) * DEC_DIGITS + rscale;

	/*
	 * If di <= 0, the value loses all digits.
	 */
	if (di <= 0)
	{
		var->ndigits = 0;
		var->weight = 0;
		var->sign = NUMERIC_POS;
	}
	else
	{
		/* NBASE digits wanted */
		ndigits = (di + DEC_DIGITS - 1) / DEC_DIGITS;

		if (ndigits <= var->ndigits)
		{
			var->ndigits = ndigits;

#if DEC_DIGITS == 1
			/* no within-digit stuff to worry about */
#else
			/* 0, or number of decimal digits to keep in last NBASE digit */
			di %= DEC_DIGITS;

			if (di > 0)
			{
				/* Must truncate within last NBASE digit */
				NumericDigit *digits = var->digits;
				int			extra,
							pow10;

#if DEC_DIGITS == 4
				pow10 = round_powers[di];
#elif DEC_DIGITS == 2
				pow10 = 10;
#else
#error unsupported NBASE
#endif
				extra = digits[--ndigits] % pow10;
				digits[ndigits] -= extra;
			}
#endif
		}
	}
}

/* ---- numeric.c:12271-12303 VERBATIM (strip_var) ---- */
/*
 * strip_var
 *
 * Strip any leading and trailing zeroes from a numeric variable
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

/* ---- jsonpathexec_diff additions: forward declarations for the newly
 * extracted statics (extraction order differs from numeric.c's) ---- */
static int	cmp_var(const NumericVar *var1, const NumericVar *var2);
static int	cmp_var_common(const NumericDigit *var1digits, int var1ndigits,
						   int var1weight, int var1sign,
						   const NumericDigit *var2digits, int var2ndigits,
						   int var2weight, int var2sign);
static void sub_var(const NumericVar *var1, const NumericVar *var2,
					NumericVar *result);
static void div_var(const NumericVar *var1, const NumericVar *var2,
					NumericVar *result, int rscale, bool round, bool exact);
static void div_var_int(const NumericVar *var, int ival, int ival_weight,
						NumericVar *result, int rscale, bool round);
#ifdef HAVE_INT128
static void div_var_int64(const NumericVar *var, int64 ival, int ival_weight,
						  NumericVar *result, int rscale, bool round);
#endif
static int	select_div_scale(const NumericVar *var1, const NumericVar *var2);
static void mod_var(const NumericVar *var1, const NumericVar *var2,
					NumericVar *result);
static void div_mod_var(const NumericVar *var1, const NumericVar *var2,
						NumericVar *quot, NumericVar *rem);
static void ceil_var(const NumericVar *var, NumericVar *result);
static void floor_var(const NumericVar *var, NumericVar *result);
static bool numericvar_to_int32(const NumericVar *var, int32 *result);
static bool numericvar_to_int64(const NumericVar *var, int64 *result);
static int	cmp_numerics(Numeric num1, Numeric num2);
static void set_var_from_num(Numeric num, NumericVar *dest);
static int	numeric_sign_internal(Numeric num);


/* ---- jsonpathexec_diff additions: VERBATIM extracts (see header) ---- */
/* ---- numeric.c:845-854 VERBATIM (numeric_is_nan) ---- */
/*
 * numeric_is_nan() -
 *
 *	Is Numeric value a NaN?
 */
bool
numeric_is_nan(Numeric num)
{
	return NUMERIC_IS_NAN(num);
}

/* ---- numeric.c:856-865 VERBATIM (numeric_is_inf) ---- */
/*
 * numeric_is_inf() -
 *
 *	Is Numeric value an infinity?
 */
bool
numeric_is_inf(Numeric num)
{
	return NUMERIC_IS_INF(num);
}

/* ---- numeric.c:1323-1366 VERBATIM (numerictypmodin) ---- */
Datum
numerictypmodin(PG_FUNCTION_ARGS)
{
	ArrayType  *ta = PG_GETARG_ARRAYTYPE_P(0);
	int32	   *tl;
	int			n;
	int32		typmod;

	tl = ArrayGetIntegerTypmods(ta, &n);

	if (n == 2)
	{
		if (tl[0] < 1 || tl[0] > NUMERIC_MAX_PRECISION)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("NUMERIC precision %d must be between 1 and %d",
							tl[0], NUMERIC_MAX_PRECISION)));
		if (tl[1] < NUMERIC_MIN_SCALE || tl[1] > NUMERIC_MAX_SCALE)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("NUMERIC scale %d must be between %d and %d",
							tl[1], NUMERIC_MIN_SCALE, NUMERIC_MAX_SCALE)));
		typmod = make_numeric_typmod(tl[0], tl[1]);
	}
	else if (n == 1)
	{
		if (tl[0] < 1 || tl[0] > NUMERIC_MAX_PRECISION)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("NUMERIC precision %d must be between 1 and %d",
							tl[0], NUMERIC_MAX_PRECISION)));
		/* scale defaults to zero */
		typmod = make_numeric_typmod(tl[0], 0);
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid NUMERIC type modifier")));
		typmod = 0;				/* keep compiler quiet */
	}

	PG_RETURN_INT32(typmod);
}

/* ---- numeric.c:1392-1416 VERBATIM (numeric_abs) ---- */
Datum
numeric_abs(PG_FUNCTION_ARGS)
{
	Numeric		num = PG_GETARG_NUMERIC(0);
	Numeric		res;

	/*
	 * Do it the easy way directly on the packed format
	 */
	res = duplicate_numeric(num);

	if (NUMERIC_IS_SHORT(num))
		res->choice.n_short.n_header =
			num->choice.n_short.n_header & ~NUMERIC_SHORT_SIGN_MASK;
	else if (NUMERIC_IS_SPECIAL(num))
	{
		/* This changes -Inf to Inf, and doesn't affect NaN */
		res->choice.n_short.n_header =
			num->choice.n_short.n_header & ~NUMERIC_INF_SIGN_MASK;
	}
	else
		res->choice.n_long.n_sign_dscale = NUMERIC_POS | NUMERIC_DSCALE(num);

	PG_RETURN_NUMERIC(res);
}

/* ---- numeric.c:1589-1638 VERBATIM (numeric_trunc) ---- */
/*
 * numeric_trunc() -
 *
 *	Truncate a value to have 'scale' digits after the decimal point.
 *	We allow negative 'scale', implying a truncation before the decimal
 *	point --- Oracle interprets truncation that way.
 */
Datum
numeric_trunc(PG_FUNCTION_ARGS)
{
	Numeric		num = PG_GETARG_NUMERIC(0);
	int32		scale = PG_GETARG_INT32(1);
	Numeric		res;
	NumericVar	arg;

	/*
	 * Handle NaN and infinities
	 */
	if (NUMERIC_IS_SPECIAL(num))
		PG_RETURN_NUMERIC(duplicate_numeric(num));

	/*
	 * Limit the scale value to avoid possible overflow in calculations.
	 *
	 * These limits are based on the maximum number of digits a Numeric value
	 * can have before and after the decimal point.
	 */
	scale = Max(scale, -(NUMERIC_WEIGHT_MAX + 1) * DEC_DIGITS);
	scale = Min(scale, NUMERIC_DSCALE_MAX);

	/*
	 * Unpack the argument and truncate it at the proper digit position
	 */
	init_var(&arg);
	set_var_from_num(num, &arg);

	trunc_var(&arg, scale);

	/* We don't allow negative output dscale */
	if (scale < 0)
		arg.dscale = 0;

	/*
	 * Return the truncated result
	 */
	res = make_result(&arg);

	free_var(&arg);
	PG_RETURN_NUMERIC(res);
}

/* ---- numeric.c:1641-1666 VERBATIM (numeric_ceil) ---- */
/*
 * numeric_ceil() -
 *
 *	Return the smallest integer greater than or equal to the argument
 */
Datum
numeric_ceil(PG_FUNCTION_ARGS)
{
	Numeric		num = PG_GETARG_NUMERIC(0);
	Numeric		res;
	NumericVar	result;

	/*
	 * Handle NaN and infinities
	 */
	if (NUMERIC_IS_SPECIAL(num))
		PG_RETURN_NUMERIC(duplicate_numeric(num));

	init_var_from_num(num, &result);
	ceil_var(&result, &result);

	res = make_result(&result);
	free_var(&result);

	PG_RETURN_NUMERIC(res);
}

/* ---- numeric.c:1669-1694 VERBATIM (numeric_floor) ---- */
/*
 * numeric_floor() -
 *
 *	Return the largest integer equal to or less than the argument
 */
Datum
numeric_floor(PG_FUNCTION_ARGS)
{
	Numeric		num = PG_GETARG_NUMERIC(0);
	Numeric		res;
	NumericVar	result;

	/*
	 * Handle NaN and infinities
	 */
	if (NUMERIC_IS_SPECIAL(num))
		PG_RETURN_NUMERIC(duplicate_numeric(num));

	init_var_from_num(num, &result);
	floor_var(&result, &result);

	res = make_result(&result);
	free_var(&result);

	PG_RETURN_NUMERIC(res);
}

/* ---- numeric.c:2517-2530 VERBATIM (numeric_cmp) ---- */
Datum
numeric_cmp(PG_FUNCTION_ARGS)
{
	Numeric		num1 = PG_GETARG_NUMERIC(0);
	Numeric		num2 = PG_GETARG_NUMERIC(1);
	int			result;

	result = cmp_numerics(num1, num2);

	PG_FREE_IF_COPY(num1, 0);
	PG_FREE_IF_COPY(num2, 1);

	PG_RETURN_INT32(result);
}

/* ---- numeric.c:2533-2546 VERBATIM (numeric_eq) ---- */
Datum
numeric_eq(PG_FUNCTION_ARGS)
{
	Numeric		num1 = PG_GETARG_NUMERIC(0);
	Numeric		num2 = PG_GETARG_NUMERIC(1);
	bool		result;

	result = cmp_numerics(num1, num2) == 0;

	PG_FREE_IF_COPY(num1, 0);
	PG_FREE_IF_COPY(num2, 1);

	PG_RETURN_BOOL(result);
}

/* ---- numeric.c:2623-2675 VERBATIM (cmp_numerics) ---- */
static int
cmp_numerics(Numeric num1, Numeric num2)
{
	int			result;

	/*
	 * We consider all NANs to be equal and larger than any non-NAN (including
	 * Infinity).  This is somewhat arbitrary; the important thing is to have
	 * a consistent sort order.
	 */
	if (NUMERIC_IS_SPECIAL(num1))
	{
		if (NUMERIC_IS_NAN(num1))
		{
			if (NUMERIC_IS_NAN(num2))
				result = 0;		/* NAN = NAN */
			else
				result = 1;		/* NAN > non-NAN */
		}
		else if (NUMERIC_IS_PINF(num1))
		{
			if (NUMERIC_IS_NAN(num2))
				result = -1;	/* PINF < NAN */
			else if (NUMERIC_IS_PINF(num2))
				result = 0;		/* PINF = PINF */
			else
				result = 1;		/* PINF > anything else */
		}
		else					/* num1 must be NINF */
		{
			if (NUMERIC_IS_NINF(num2))
				result = 0;		/* NINF = NINF */
			else
				result = -1;	/* NINF < anything else */
		}
	}
	else if (NUMERIC_IS_SPECIAL(num2))
	{
		if (NUMERIC_IS_NINF(num2))
			result = 1;			/* normal > NINF */
		else
			result = -1;		/* normal < NAN or PINF */
	}
	else
	{
		result = cmp_var_common(NUMERIC_DIGITS(num1), NUMERIC_NDIGITS(num1),
								NUMERIC_WEIGHT(num1), NUMERIC_SIGN(num1),
								NUMERIC_DIGITS(num2), NUMERIC_NDIGITS(num2),
								NUMERIC_WEIGHT(num2), NUMERIC_SIGN(num2));
	}

	return result;
}

/* ---- numeric.c:2978-3035 VERBATIM (numeric_add_opt_error) ---- */
/*
 * numeric_add_opt_error() -
 *
 *	Internal version of numeric_add().  If "*have_error" flag is provided,
 *	on error it's set to true, NULL returned.  This is helpful when caller
 *	need to handle errors by itself.
 */
Numeric
numeric_add_opt_error(Numeric num1, Numeric num2, bool *have_error)
{
	NumericVar	arg1;
	NumericVar	arg2;
	NumericVar	result;
	Numeric		res;

	/*
	 * Handle NaN and infinities
	 */
	if (NUMERIC_IS_SPECIAL(num1) || NUMERIC_IS_SPECIAL(num2))
	{
		if (NUMERIC_IS_NAN(num1) || NUMERIC_IS_NAN(num2))
			return make_result(&const_nan);
		if (NUMERIC_IS_PINF(num1))
		{
			if (NUMERIC_IS_NINF(num2))
				return make_result(&const_nan); /* Inf + -Inf */
			else
				return make_result(&const_pinf);
		}
		if (NUMERIC_IS_NINF(num1))
		{
			if (NUMERIC_IS_PINF(num2))
				return make_result(&const_nan); /* -Inf + Inf */
			else
				return make_result(&const_ninf);
		}
		/* by here, num1 must be finite, so num2 is not */
		if (NUMERIC_IS_PINF(num2))
			return make_result(&const_pinf);
		Assert(NUMERIC_IS_NINF(num2));
		return make_result(&const_ninf);
	}

	/*
	 * Unpack the values, let add_var() compute the result and return it.
	 */
	init_var_from_num(num1, &arg1);
	init_var_from_num(num2, &arg2);

	init_var(&result);
	add_var(&arg1, &arg2, &result);

	res = make_result_opt_error(&result, have_error);

	free_var(&result);

	return res;
}

/* ---- numeric.c:3056-3113 VERBATIM (numeric_sub_opt_error) ---- */
/*
 * numeric_sub_opt_error() -
 *
 *	Internal version of numeric_sub().  If "*have_error" flag is provided,
 *	on error it's set to true, NULL returned.  This is helpful when caller
 *	need to handle errors by itself.
 */
Numeric
numeric_sub_opt_error(Numeric num1, Numeric num2, bool *have_error)
{
	NumericVar	arg1;
	NumericVar	arg2;
	NumericVar	result;
	Numeric		res;

	/*
	 * Handle NaN and infinities
	 */
	if (NUMERIC_IS_SPECIAL(num1) || NUMERIC_IS_SPECIAL(num2))
	{
		if (NUMERIC_IS_NAN(num1) || NUMERIC_IS_NAN(num2))
			return make_result(&const_nan);
		if (NUMERIC_IS_PINF(num1))
		{
			if (NUMERIC_IS_PINF(num2))
				return make_result(&const_nan); /* Inf - Inf */
			else
				return make_result(&const_pinf);
		}
		if (NUMERIC_IS_NINF(num1))
		{
			if (NUMERIC_IS_NINF(num2))
				return make_result(&const_nan); /* -Inf - -Inf */
			else
				return make_result(&const_ninf);
		}
		/* by here, num1 must be finite, so num2 is not */
		if (NUMERIC_IS_PINF(num2))
			return make_result(&const_ninf);
		Assert(NUMERIC_IS_NINF(num2));
		return make_result(&const_pinf);
	}

	/*
	 * Unpack the values, let sub_var() compute the result and return it.
	 */
	init_var_from_num(num1, &arg1);
	init_var_from_num(num2, &arg2);

	init_var(&result);
	sub_var(&arg1, &arg2, &result);

	res = make_result_opt_error(&result, have_error);

	free_var(&result);

	return res;
}

/* ---- numeric.c:3134-3234 VERBATIM (numeric_mul_opt_error) ---- */
/*
 * numeric_mul_opt_error() -
 *
 *	Internal version of numeric_mul().  If "*have_error" flag is provided,
 *	on error it's set to true, NULL returned.  This is helpful when caller
 *	need to handle errors by itself.
 */
Numeric
numeric_mul_opt_error(Numeric num1, Numeric num2, bool *have_error)
{
	NumericVar	arg1;
	NumericVar	arg2;
	NumericVar	result;
	Numeric		res;

	/*
	 * Handle NaN and infinities
	 */
	if (NUMERIC_IS_SPECIAL(num1) || NUMERIC_IS_SPECIAL(num2))
	{
		if (NUMERIC_IS_NAN(num1) || NUMERIC_IS_NAN(num2))
			return make_result(&const_nan);
		if (NUMERIC_IS_PINF(num1))
		{
			switch (numeric_sign_internal(num2))
			{
				case 0:
					return make_result(&const_nan); /* Inf * 0 */
				case 1:
					return make_result(&const_pinf);
				case -1:
					return make_result(&const_ninf);
			}
			Assert(false);
		}
		if (NUMERIC_IS_NINF(num1))
		{
			switch (numeric_sign_internal(num2))
			{
				case 0:
					return make_result(&const_nan); /* -Inf * 0 */
				case 1:
					return make_result(&const_ninf);
				case -1:
					return make_result(&const_pinf);
			}
			Assert(false);
		}
		/* by here, num1 must be finite, so num2 is not */
		if (NUMERIC_IS_PINF(num2))
		{
			switch (numeric_sign_internal(num1))
			{
				case 0:
					return make_result(&const_nan); /* 0 * Inf */
				case 1:
					return make_result(&const_pinf);
				case -1:
					return make_result(&const_ninf);
			}
			Assert(false);
		}
		Assert(NUMERIC_IS_NINF(num2));
		switch (numeric_sign_internal(num1))
		{
			case 0:
				return make_result(&const_nan); /* 0 * -Inf */
			case 1:
				return make_result(&const_ninf);
			case -1:
				return make_result(&const_pinf);
		}
		Assert(false);
	}

	/*
	 * Unpack the values, let mul_var() compute the result and return it.
	 * Unlike add_var() and sub_var(), mul_var() will round its result. In the
	 * case of numeric_mul(), which is invoked for the * operator on numerics,
	 * we request exact representation for the product (rscale = sum(dscale of
	 * arg1, dscale of arg2)).  If the exact result has more digits after the
	 * decimal point than can be stored in a numeric, we round it.  Rounding
	 * after computing the exact result ensures that the final result is
	 * correctly rounded (rounding in mul_var() using a truncated product
	 * would not guarantee this).
	 */
	init_var_from_num(num1, &arg1);
	init_var_from_num(num2, &arg2);

	init_var(&result);
	mul_var(&arg1, &arg2, &result, arg1.dscale + arg2.dscale);

	if (result.dscale > NUMERIC_DSCALE_MAX)
		round_var(&result, NUMERIC_DSCALE_MAX);

	res = make_result_opt_error(&result, have_error);

	free_var(&result);

	return res;
}

/* ---- numeric.c:3255-3369 VERBATIM (numeric_div_opt_error) ---- */
/*
 * numeric_div_opt_error() -
 *
 *	Internal version of numeric_div().  If "*have_error" flag is provided,
 *	on error it's set to true, NULL returned.  This is helpful when caller
 *	need to handle errors by itself.
 */
Numeric
numeric_div_opt_error(Numeric num1, Numeric num2, bool *have_error)
{
	NumericVar	arg1;
	NumericVar	arg2;
	NumericVar	result;
	Numeric		res;
	int			rscale;

	if (have_error)
		*have_error = false;

	/*
	 * Handle NaN and infinities
	 */
	if (NUMERIC_IS_SPECIAL(num1) || NUMERIC_IS_SPECIAL(num2))
	{
		if (NUMERIC_IS_NAN(num1) || NUMERIC_IS_NAN(num2))
			return make_result(&const_nan);
		if (NUMERIC_IS_PINF(num1))
		{
			if (NUMERIC_IS_SPECIAL(num2))
				return make_result(&const_nan); /* Inf / [-]Inf */
			switch (numeric_sign_internal(num2))
			{
				case 0:
					if (have_error)
					{
						*have_error = true;
						return NULL;
					}
					ereport(ERROR,
							(errcode(ERRCODE_DIVISION_BY_ZERO),
							 errmsg("division by zero")));
					break;
				case 1:
					return make_result(&const_pinf);
				case -1:
					return make_result(&const_ninf);
			}
			Assert(false);
		}
		if (NUMERIC_IS_NINF(num1))
		{
			if (NUMERIC_IS_SPECIAL(num2))
				return make_result(&const_nan); /* -Inf / [-]Inf */
			switch (numeric_sign_internal(num2))
			{
				case 0:
					if (have_error)
					{
						*have_error = true;
						return NULL;
					}
					ereport(ERROR,
							(errcode(ERRCODE_DIVISION_BY_ZERO),
							 errmsg("division by zero")));
					break;
				case 1:
					return make_result(&const_ninf);
				case -1:
					return make_result(&const_pinf);
			}
			Assert(false);
		}
		/* by here, num1 must be finite, so num2 is not */

		/*
		 * POSIX would have us return zero or minus zero if num1 is zero, and
		 * otherwise throw an underflow error.  But the numeric type doesn't
		 * really do underflow, so let's just return zero.
		 */
		return make_result(&const_zero);
	}

	/*
	 * Unpack the arguments
	 */
	init_var_from_num(num1, &arg1);
	init_var_from_num(num2, &arg2);

	init_var(&result);

	/*
	 * Select scale for division result
	 */
	rscale = select_div_scale(&arg1, &arg2);

	/*
	 * If "have_error" is provided, check for division by zero here
	 */
	if (have_error && (arg2.ndigits == 0 || arg2.digits[0] == 0))
	{
		*have_error = true;
		return NULL;
	}

	/*
	 * Do the divide and return the result
	 */
	div_var(&arg1, &arg2, &result, rscale, true, true);

	res = make_result_opt_error(&result, have_error);

	free_var(&result);

	return res;
}

/* ---- numeric.c:3479-3547 VERBATIM (numeric_mod_opt_error) ---- */
/*
 * numeric_mod_opt_error() -
 *
 *	Internal version of numeric_mod().  If "*have_error" flag is provided,
 *	on error it's set to true, NULL returned.  This is helpful when caller
 *	need to handle errors by itself.
 */
Numeric
numeric_mod_opt_error(Numeric num1, Numeric num2, bool *have_error)
{
	Numeric		res;
	NumericVar	arg1;
	NumericVar	arg2;
	NumericVar	result;

	if (have_error)
		*have_error = false;

	/*
	 * Handle NaN and infinities.  We follow POSIX fmod() on this, except that
	 * POSIX treats x-is-infinite and y-is-zero identically, raising EDOM and
	 * returning NaN.  We choose to throw error only for y-is-zero.
	 */
	if (NUMERIC_IS_SPECIAL(num1) || NUMERIC_IS_SPECIAL(num2))
	{
		if (NUMERIC_IS_NAN(num1) || NUMERIC_IS_NAN(num2))
			return make_result(&const_nan);
		if (NUMERIC_IS_INF(num1))
		{
			if (numeric_sign_internal(num2) == 0)
			{
				if (have_error)
				{
					*have_error = true;
					return NULL;
				}
				ereport(ERROR,
						(errcode(ERRCODE_DIVISION_BY_ZERO),
						 errmsg("division by zero")));
			}
			/* Inf % any nonzero = NaN */
			return make_result(&const_nan);
		}
		/* num2 must be [-]Inf; result is num1 regardless of sign of num2 */
		return duplicate_numeric(num1);
	}

	init_var_from_num(num1, &arg1);
	init_var_from_num(num2, &arg2);

	init_var(&result);

	/*
	 * If "have_error" is provided, check for division by zero here
	 */
	if (have_error && (arg2.ndigits == 0 || arg2.digits[0] == 0))
	{
		*have_error = true;
		return NULL;
	}

	mod_var(&arg1, &arg2, &result);

	res = make_result_opt_error(&result, NULL);

	free_var(&result);

	return res;
}

/* ---- numeric.c:4401-4416 VERBATIM (int64_to_numeric) ---- */
Numeric
int64_to_numeric(int64 val)
{
	Numeric		res;
	NumericVar	result;

	init_var(&result);

	int64_to_numericvar(val, &result);

	res = make_result(&result);

	free_var(&result);

	return res;
}

/* ---- numeric.c:4507-4513 VERBATIM (int4_numeric) ---- */
Datum
int4_numeric(PG_FUNCTION_ARGS)
{
	int32		val = PG_GETARG_INT32(0);

	PG_RETURN_NUMERIC(int64_to_numeric(val));
}

/* ---- numeric.c:4515-4563 VERBATIM (numeric_int4_opt_error) ---- */
int32
numeric_int4_opt_error(Numeric num, bool *have_error)
{
	NumericVar	x;
	int32		result;

	if (have_error)
		*have_error = false;

	if (NUMERIC_IS_SPECIAL(num))
	{
		if (have_error)
		{
			*have_error = true;
			return 0;
		}
		else
		{
			if (NUMERIC_IS_NAN(num))
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot convert NaN to %s", "integer")));
			else
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot convert infinity to %s", "integer")));
		}
	}

	/* Convert to variable format, then convert to int4 */
	init_var_from_num(num, &x);

	if (!numericvar_to_int32(&x, &result))
	{
		if (have_error)
		{
			*have_error = true;
			return 0;
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					 errmsg("integer out of range")));
		}
	}

	return result;
}

/* ---- numeric.c:4573-4593 VERBATIM (numericvar_to_int32) ---- */
/*
 * Given a NumericVar, convert it to an int32. If the NumericVar
 * exceeds the range of an int32, false is returned, otherwise true is returned.
 * The input NumericVar is *not* free'd.
 */
static bool
numericvar_to_int32(const NumericVar *var, int32 *result)
{
	int64		val;

	if (!numericvar_to_int64(var, &val))
		return false;

	if (unlikely(val < PG_INT32_MIN) || unlikely(val > PG_INT32_MAX))
		return false;

	/* Down-convert to int4 */
	*result = (int32) val;

	return true;
}

/* ---- numeric.c:4595-4601 VERBATIM (int8_numeric) ---- */
Datum
int8_numeric(PG_FUNCTION_ARGS)
{
	int64		val = PG_GETARG_INT64(0);

	PG_RETURN_NUMERIC(int64_to_numeric(val));
}

/* ---- numeric.c:4603-4651 VERBATIM (numeric_int8_opt_error) ---- */
int64
numeric_int8_opt_error(Numeric num, bool *have_error)
{
	NumericVar	x;
	int64		result;

	if (have_error)
		*have_error = false;

	if (NUMERIC_IS_SPECIAL(num))
	{
		if (have_error)
		{
			*have_error = true;
			return 0;
		}
		else
		{
			if (NUMERIC_IS_NAN(num))
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot convert NaN to %s", "bigint")));
			else
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot convert infinity to %s", "bigint")));
		}
	}

	/* Convert to variable format, then convert to int8 */
	init_var_from_num(num, &x);

	if (!numericvar_to_int64(&x, &result))
	{
		if (have_error)
		{
			*have_error = true;
			return 0;
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					 errmsg("bigint out of range")));
		}
	}

	return result;
}

/* ---- numeric.c:4662-4668 VERBATIM (int2_numeric) ---- */
Datum
int2_numeric(PG_FUNCTION_ARGS)
{
	int16		val = PG_GETARG_INT16(0);

	PG_RETURN_NUMERIC(int64_to_numeric(val));
}

/* ---- numeric.c:4711-4743 VERBATIM (float8_numeric) ---- */
Datum
float8_numeric(PG_FUNCTION_ARGS)
{
	float8		val = PG_GETARG_FLOAT8(0);
	Numeric		res;
	NumericVar	result;
	char		buf[DBL_DIG + 100];
	const char *endptr;

	if (isnan(val))
		PG_RETURN_NUMERIC(make_result(&const_nan));

	if (isinf(val))
	{
		if (val < 0)
			PG_RETURN_NUMERIC(make_result(&const_ninf));
		else
			PG_RETURN_NUMERIC(make_result(&const_pinf));
	}

	snprintf(buf, sizeof(buf), "%.*g", DBL_DIG, val);

	init_var(&result);

	/* Assume we need not worry about leading/trailing spaces */
	(void) set_var_from_str(buf, buf, &result, &endptr, NULL);

	res = make_result(&result);

	free_var(&result);

	PG_RETURN_NUMERIC(res);
}

/* ---- numeric.c:4805-4837 VERBATIM (float4_numeric) ---- */
Datum
float4_numeric(PG_FUNCTION_ARGS)
{
	float4		val = PG_GETARG_FLOAT4(0);
	Numeric		res;
	NumericVar	result;
	char		buf[FLT_DIG + 100];
	const char *endptr;

	if (isnan(val))
		PG_RETURN_NUMERIC(make_result(&const_nan));

	if (isinf(val))
	{
		if (val < 0)
			PG_RETURN_NUMERIC(make_result(&const_ninf));
		else
			PG_RETURN_NUMERIC(make_result(&const_pinf));
	}

	snprintf(buf, sizeof(buf), "%.*g", FLT_DIG, val);

	init_var(&result);

	/* Assume we need not worry about leading/trailing spaces */
	(void) set_var_from_str(buf, buf, &result, &endptr, NULL);

	res = make_result(&result);

	free_var(&result);

	PG_RETURN_NUMERIC(res);
}

/* ---- numeric.c:8142-8217 VERBATIM (numericvar_to_int64) ---- */
/*
 * Convert numeric to int8, rounding if needed.
 *
 * If overflow, return false (no error is raised).  Return true if okay.
 */
static bool
numericvar_to_int64(const NumericVar *var, int64 *result)
{
	NumericDigit *digits;
	int			ndigits;
	int			weight;
	int			i;
	int64		val;
	bool		neg;
	NumericVar	rounded;

	/* Round to nearest integer */
	init_var(&rounded);
	set_var_from_var(var, &rounded);
	round_var(&rounded, 0);

	/* Check for zero input */
	strip_var(&rounded);
	ndigits = rounded.ndigits;
	if (ndigits == 0)
	{
		*result = 0;
		free_var(&rounded);
		return true;
	}

	/*
	 * For input like 10000000000, we must treat stripped digits as real. So
	 * the loop assumes there are weight+1 digits before the decimal point.
	 */
	weight = rounded.weight;
	Assert(weight >= 0 && ndigits <= weight + 1);

	/*
	 * Construct the result. To avoid issues with converting a value
	 * corresponding to INT64_MIN (which can't be represented as a positive 64
	 * bit two's complement integer), accumulate value as a negative number.
	 */
	digits = rounded.digits;
	neg = (rounded.sign == NUMERIC_NEG);
	val = -digits[0];
	for (i = 1; i <= weight; i++)
	{
		if (unlikely(pg_mul_s64_overflow(val, NBASE, &val)))
		{
			free_var(&rounded);
			return false;
		}

		if (i < ndigits)
		{
			if (unlikely(pg_sub_s64_overflow(val, digits[i], &val)))
			{
				free_var(&rounded);
				return false;
			}
		}
	}

	free_var(&rounded);

	if (!neg)
	{
		if (unlikely(val == PG_INT64_MIN))
			return false;
		val = -val;
	}
	*result = val;

	return true;
}

/* ---- numeric.c:8485-8498 VERBATIM (cmp_var) ---- */
/*
 * cmp_var() -
 *
 *	Compare two values on variable level.  We assume zeroes have been
 *	truncated to no digits.
 */
static int
cmp_var(const NumericVar *var1, const NumericVar *var2)
{
	return cmp_var_common(var1->digits, var1->ndigits,
						  var1->weight, var1->sign,
						  var2->digits, var2->ndigits,
						  var2->weight, var2->sign);
}

/* ---- numeric.c:8500-8540 VERBATIM (cmp_var_common) ---- */
/*
 * cmp_var_common() -
 *
 *	Main routine of cmp_var(). This function can be used by both
 *	NumericVar and Numeric.
 */
static int
cmp_var_common(const NumericDigit *var1digits, int var1ndigits,
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
		return cmp_abs_common(var1digits, var1ndigits, var1weight,
							  var2digits, var2ndigits, var2weight);
	}

	if (var2sign == NUMERIC_POS)
		return -1;

	return cmp_abs_common(var2digits, var2ndigits, var2weight,
						  var1digits, var1ndigits, var1weight);
}

/* ---- numeric.c:8660-8778 VERBATIM (sub_var) ---- */
/*
 * sub_var() -
 *
 *	Full version of sub functionality on variable level (handling signs).
 *	result might point to one of the operands too without danger.
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
			/* ----------
			 * var1 is positive, var2 is negative
			 * result = +(ABS(var1) + ABS(var2))
			 * ----------
			 */
			add_abs(var1, var2, result);
			result->sign = NUMERIC_POS;
		}
		else
		{
			/* ----------
			 * Both are positive
			 * Must compare absolute values
			 * ----------
			 */
			switch (cmp_abs(var1, var2))
			{
				case 0:
					/* ----------
					 * ABS(var1) == ABS(var2)
					 * result = ZERO
					 * ----------
					 */
					zero_var(result);
					result->dscale = Max(var1->dscale, var2->dscale);
					break;

				case 1:
					/* ----------
					 * ABS(var1) > ABS(var2)
					 * result = +(ABS(var1) - ABS(var2))
					 * ----------
					 */
					sub_abs(var1, var2, result);
					result->sign = NUMERIC_POS;
					break;

				case -1:
					/* ----------
					 * ABS(var1) < ABS(var2)
					 * result = -(ABS(var2) - ABS(var1))
					 * ----------
					 */
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
			/* ----------
			 * Both are negative
			 * Must compare absolute values
			 * ----------
			 */
			switch (cmp_abs(var1, var2))
			{
				case 0:
					/* ----------
					 * ABS(var1) == ABS(var2)
					 * result = ZERO
					 * ----------
					 */
					zero_var(result);
					result->dscale = Max(var1->dscale, var2->dscale);
					break;

				case 1:
					/* ----------
					 * ABS(var1) > ABS(var2)
					 * result = -(ABS(var1) - ABS(var2))
					 * ----------
					 */
					sub_abs(var1, var2, result);
					result->sign = NUMERIC_NEG;
					break;

				case -1:
					/* ----------
					 * ABS(var1) < ABS(var2)
					 * result = +(ABS(var2) - ABS(var1))
					 * ----------
					 */
					sub_abs(var2, var1, result);
					result->sign = NUMERIC_POS;
					break;
			}
		}
		else
		{
			/* ----------
			 * var1 is negative, var2 is positive
			 * result = -(ABS(var1) + ABS(var2))
			 * ----------
			 */
			add_abs(var1, var2, result);
			result->sign = NUMERIC_NEG;
		}
	}
}

/* ---- numeric.c:9347-9897 VERBATIM (div_var) ---- */
/*
 * div_var() -
 *
 *	Compute the quotient var1 / var2 to rscale fractional digits.
 *
 *	If "round" is true, the result is rounded at the rscale'th digit; if
 *	false, it is truncated (towards zero) at that digit.
 *
 *	If "exact" is true, the exact result is computed to the specified rscale;
 *	if false, successive quotient digits are approximated up to rscale plus
 *	DIV_GUARD_DIGITS extra digits, ignoring all contributions from digits to
 *	the right of that, before rounding or truncating to the specified rscale.
 *	This can be significantly faster, and usually gives the same result as the
 *	exact computation, but it may occasionally be off by one in the final
 *	digit, if contributions from the ignored digits would have propagated
 *	through the guard digits.  This is good enough for the transcendental
 *	functions, where small errors are acceptable.
 */
static void
div_var(const NumericVar *var1, const NumericVar *var2, NumericVar *result,
		int rscale, bool round, bool exact)
{
	int			var1ndigits = var1->ndigits;
	int			var2ndigits = var2->ndigits;
	int			res_sign;
	int			res_weight;
	int			res_ndigits;
	int			var1ndigitpairs;
	int			var2ndigitpairs;
	int			res_ndigitpairs;
	int			div_ndigitpairs;
	int64	   *dividend;
	int32	   *divisor;
	double		fdivisor,
				fdivisorinverse,
				fdividend,
				fquotient;
	int64		maxdiv;
	int			qi;
	int32		qdigit;
	int64		carry;
	int64		newdig;
	int64	   *remainder;
	NumericDigit *res_digits;
	int			i;

	/*
	 * First of all division by zero check; we must not be handed an
	 * unnormalized divisor.
	 */
	if (var2ndigits == 0 || var2->digits[0] == 0)
		ereport(ERROR,
				(errcode(ERRCODE_DIVISION_BY_ZERO),
				 errmsg("division by zero")));

	/*
	 * If the divisor has just one or two digits, delegate to div_var_int(),
	 * which uses fast short division.
	 *
	 * Similarly, on platforms with 128-bit integer support, delegate to
	 * div_var_int64() for divisors with three or four digits.
	 */
	if (var2ndigits <= 2)
	{
		int			idivisor;
		int			idivisor_weight;

		idivisor = var2->digits[0];
		idivisor_weight = var2->weight;
		if (var2ndigits == 2)
		{
			idivisor = idivisor * NBASE + var2->digits[1];
			idivisor_weight--;
		}
		if (var2->sign == NUMERIC_NEG)
			idivisor = -idivisor;

		div_var_int(var1, idivisor, idivisor_weight, result, rscale, round);
		return;
	}
#ifdef HAVE_INT128
	if (var2ndigits <= 4)
	{
		int64		idivisor;
		int			idivisor_weight;

		idivisor = var2->digits[0];
		idivisor_weight = var2->weight;
		for (i = 1; i < var2ndigits; i++)
		{
			idivisor = idivisor * NBASE + var2->digits[i];
			idivisor_weight--;
		}
		if (var2->sign == NUMERIC_NEG)
			idivisor = -idivisor;

		div_var_int64(var1, idivisor, idivisor_weight, result, rscale, round);
		return;
	}
#endif

	/*
	 * Otherwise, perform full long division.
	 */

	/* Result zero check */
	if (var1ndigits == 0)
	{
		zero_var(result);
		result->dscale = rscale;
		return;
	}

	/*
	 * The approximate computation can be significantly faster than the exact
	 * one, since the working dividend is var2ndigitpairs base-NBASE^2 digits
	 * shorter below.  However, that comes with the tradeoff of computing
	 * DIV_GUARD_DIGITS extra base-NBASE result digits.  Ignoring all other
	 * overheads, that suggests that, in theory, the approximate computation
	 * will only be faster than the exact one when var2ndigits is greater than
	 * 2 * (DIV_GUARD_DIGITS + 1), independent of the size of var1.
	 *
	 * Thus, we're better off doing an exact computation when var2 is shorter
	 * than this.  Empirically, it has been found that the exact threshold is
	 * a little higher, due to other overheads in the outer division loop.
	 */
	if (var2ndigits <= 2 * (DIV_GUARD_DIGITS + 2))
		exact = true;

	/*
	 * Determine the result sign, weight and number of digits to calculate.
	 * The weight figured here is correct if the emitted quotient has no
	 * leading zero digits; otherwise strip_var() will fix things up.
	 */
	if (var1->sign == var2->sign)
		res_sign = NUMERIC_POS;
	else
		res_sign = NUMERIC_NEG;
	res_weight = var1->weight - var2->weight + 1;
	/* The number of accurate result digits we need to produce: */
	res_ndigits = res_weight + 1 + (rscale + DEC_DIGITS - 1) / DEC_DIGITS;
	/* ... but always at least 1 */
	res_ndigits = Max(res_ndigits, 1);
	/* If rounding needed, figure one more digit to ensure correct result */
	if (round)
		res_ndigits++;
	/* Add guard digits for roundoff error when producing approx result */
	if (!exact)
		res_ndigits += DIV_GUARD_DIGITS;

	/*
	 * The computation itself is done using base-NBASE^2 arithmetic, so we
	 * actually process the input digits in pairs, producing a base-NBASE^2
	 * intermediate result.  This significantly improves performance, since
	 * the computation is O(N^2) in the number of input digits, and working in
	 * base NBASE^2 effectively halves "N".
	 */
	var1ndigitpairs = (var1ndigits + 1) / 2;
	var2ndigitpairs = (var2ndigits + 1) / 2;
	res_ndigitpairs = (res_ndigits + 1) / 2;
	res_ndigits = 2 * res_ndigitpairs;

	/*
	 * We do the arithmetic in an array "dividend[]" of signed 64-bit
	 * integers.  Since PG_INT64_MAX is much larger than NBASE^4, this gives
	 * us a lot of headroom to avoid normalizing carries immediately.
	 *
	 * When performing an exact computation, the working dividend requires
	 * res_ndigitpairs + var2ndigitpairs digits.  If var1 is larger than that,
	 * the extra digits do not contribute to the result, and are ignored.
	 *
	 * When performing an approximate computation, the working dividend only
	 * requires res_ndigitpairs digits (which includes the extra guard
	 * digits).  All input digits beyond that are ignored.
	 */
	if (exact)
	{
		div_ndigitpairs = res_ndigitpairs + var2ndigitpairs;
		var1ndigitpairs = Min(var1ndigitpairs, div_ndigitpairs);
	}
	else
	{
		div_ndigitpairs = res_ndigitpairs;
		var1ndigitpairs = Min(var1ndigitpairs, div_ndigitpairs);
		var2ndigitpairs = Min(var2ndigitpairs, div_ndigitpairs);
	}

	/*
	 * Allocate room for the working dividend (div_ndigitpairs 64-bit digits)
	 * plus the divisor (var2ndigitpairs 32-bit base-NBASE^2 digits).
	 *
	 * For convenience, we allocate one extra dividend digit, which is set to
	 * zero and not counted in div_ndigitpairs, so that the main loop below
	 * can safely read and write the (qi+1)'th digit in the approximate case.
	 */
	dividend = (int64 *) palloc((div_ndigitpairs + 1) * sizeof(int64) +
								var2ndigitpairs * sizeof(int32));
	divisor = (int32 *) (dividend + div_ndigitpairs + 1);

	/* load var1 into dividend[0 .. var1ndigitpairs-1], zeroing the rest */
	for (i = 0; i < var1ndigitpairs - 1; i++)
		dividend[i] = var1->digits[2 * i] * NBASE + var1->digits[2 * i + 1];

	if (2 * i + 1 < var1ndigits)
		dividend[i] = var1->digits[2 * i] * NBASE + var1->digits[2 * i + 1];
	else
		dividend[i] = var1->digits[2 * i] * NBASE;

	memset(dividend + i + 1, 0, (div_ndigitpairs - i) * sizeof(int64));

	/* load var2 into divisor[0 .. var2ndigitpairs-1] */
	for (i = 0; i < var2ndigitpairs - 1; i++)
		divisor[i] = var2->digits[2 * i] * NBASE + var2->digits[2 * i + 1];

	if (2 * i + 1 < var2ndigits)
		divisor[i] = var2->digits[2 * i] * NBASE + var2->digits[2 * i + 1];
	else
		divisor[i] = var2->digits[2 * i] * NBASE;

	/*
	 * We estimate each quotient digit using floating-point arithmetic, taking
	 * the first 2 base-NBASE^2 digits of the (current) dividend and divisor.
	 * This must be float to avoid overflow.
	 *
	 * Since the floating-point dividend and divisor use 4 base-NBASE input
	 * digits, they include roughly 40-53 bits of information from their
	 * respective inputs (assuming NBASE is 10000), which fits well in IEEE
	 * double-precision variables.  The relative error in the floating-point
	 * quotient digit will then be less than around 2/NBASE^3, so the
	 * estimated base-NBASE^2 quotient digit will typically be correct, and
	 * should not be off by more than one from the correct value.
	 */
	fdivisor = (double) divisor[0] * NBASE_SQR;
	if (var2ndigitpairs > 1)
		fdivisor += (double) divisor[1];
	fdivisorinverse = 1.0 / fdivisor;

	/*
	 * maxdiv tracks the maximum possible absolute value of any dividend[]
	 * entry; when this threatens to exceed PG_INT64_MAX, we take the time to
	 * propagate carries.  Furthermore, we need to ensure that overflow
	 * doesn't occur during the carry propagation passes either.  The carry
	 * values may have an absolute value as high as PG_INT64_MAX/NBASE^2 + 1,
	 * so really we must normalize when digits threaten to exceed PG_INT64_MAX
	 * - PG_INT64_MAX/NBASE^2 - 1.
	 *
	 * To avoid overflow in maxdiv itself, it represents the max absolute
	 * value divided by NBASE^2-1, i.e., at the top of the loop it is known
	 * that no dividend[] entry has an absolute value exceeding maxdiv *
	 * (NBASE^2-1).
	 *
	 * Actually, though, that holds good only for dividend[] entries after
	 * dividend[qi]; the adjustment done at the bottom of the loop may cause
	 * dividend[qi + 1] to exceed the maxdiv limit, so that dividend[qi] in
	 * the next iteration is beyond the limit.  This does not cause problems,
	 * as explained below.
	 */
	maxdiv = 1;

	/*
	 * Outer loop computes next quotient digit, which goes in dividend[qi].
	 */
	for (qi = 0; qi < res_ndigitpairs; qi++)
	{
		/* Approximate the current dividend value */
		fdividend = (double) dividend[qi] * NBASE_SQR;
		fdividend += (double) dividend[qi + 1];

		/* Compute the (approximate) quotient digit */
		fquotient = fdividend * fdivisorinverse;
		qdigit = (fquotient >= 0.0) ? ((int32) fquotient) :
			(((int32) fquotient) - 1);	/* truncate towards -infinity */

		if (qdigit != 0)
		{
			/* Do we need to normalize now? */
			maxdiv += i64abs(qdigit);
			if (maxdiv > (PG_INT64_MAX - PG_INT64_MAX / NBASE_SQR - 1) / (NBASE_SQR - 1))
			{
				/*
				 * Yes, do it.  Note that if var2ndigitpairs is much smaller
				 * than div_ndigitpairs, we can save a significant amount of
				 * effort here by noting that we only need to normalise those
				 * dividend[] entries touched where prior iterations
				 * subtracted multiples of the divisor.
				 */
				carry = 0;
				for (i = Min(qi + var2ndigitpairs - 2, div_ndigitpairs - 1); i > qi; i--)
				{
					newdig = dividend[i] + carry;
					if (newdig < 0)
					{
						carry = -((-newdig - 1) / NBASE_SQR) - 1;
						newdig -= carry * NBASE_SQR;
					}
					else if (newdig >= NBASE_SQR)
					{
						carry = newdig / NBASE_SQR;
						newdig -= carry * NBASE_SQR;
					}
					else
						carry = 0;
					dividend[i] = newdig;
				}
				dividend[qi] += carry;

				/*
				 * All the dividend[] digits except possibly dividend[qi] are
				 * now in the range 0..NBASE^2-1.  We do not need to consider
				 * dividend[qi] in the maxdiv value anymore, so we can reset
				 * maxdiv to 1.
				 */
				maxdiv = 1;

				/*
				 * Recompute the quotient digit since new info may have
				 * propagated into the top two dividend digits.
				 */
				fdividend = (double) dividend[qi] * NBASE_SQR;
				fdividend += (double) dividend[qi + 1];
				fquotient = fdividend * fdivisorinverse;
				qdigit = (fquotient >= 0.0) ? ((int32) fquotient) :
					(((int32) fquotient) - 1);	/* truncate towards -infinity */

				maxdiv += i64abs(qdigit);
			}

			/*
			 * Subtract off the appropriate multiple of the divisor.
			 *
			 * The digits beyond dividend[qi] cannot overflow, because we know
			 * they will fall within the maxdiv limit.  As for dividend[qi]
			 * itself, note that qdigit is approximately trunc(dividend[qi] /
			 * divisor[0]), which would make the new value simply dividend[qi]
			 * mod divisor[0].  The lower-order terms in qdigit can change
			 * this result by not more than about twice PG_INT64_MAX/NBASE^2,
			 * so overflow is impossible.
			 *
			 * This inner loop is the performance bottleneck for division, so
			 * code it in the same way as the inner loop of mul_var() so that
			 * it can be auto-vectorized.
			 */
			if (qdigit != 0)
			{
				int			istop = Min(var2ndigitpairs, div_ndigitpairs - qi);
				int64	   *dividend_qi = &dividend[qi];

				for (i = 0; i < istop; i++)
					dividend_qi[i] -= (int64) qdigit * divisor[i];
			}
		}

		/*
		 * The dividend digit we are about to replace might still be nonzero.
		 * Fold it into the next digit position.
		 *
		 * There is no risk of overflow here, although proving that requires
		 * some care.  Much as with the argument for dividend[qi] not
		 * overflowing, if we consider the first two terms in the numerator
		 * and denominator of qdigit, we can see that the final value of
		 * dividend[qi + 1] will be approximately a remainder mod
		 * (divisor[0]*NBASE^2 + divisor[1]).  Accounting for the lower-order
		 * terms is a bit complicated but ends up adding not much more than
		 * PG_INT64_MAX/NBASE^2 to the possible range.  Thus, dividend[qi + 1]
		 * cannot overflow here, and in its role as dividend[qi] in the next
		 * loop iteration, it can't be large enough to cause overflow in the
		 * carry propagation step (if any), either.
		 *
		 * But having said that: dividend[qi] can be more than
		 * PG_INT64_MAX/NBASE^2, as noted above, which means that the product
		 * dividend[qi] * NBASE^2 *can* overflow.  When that happens, adding
		 * it to dividend[qi + 1] will always cause a canceling overflow so
		 * that the end result is correct.  We could avoid the intermediate
		 * overflow by doing the multiplication and addition using unsigned
		 * int64 arithmetic, which is modulo 2^64, but so far there appears no
		 * need.
		 */
		dividend[qi + 1] += dividend[qi] * NBASE_SQR;

		dividend[qi] = qdigit;
	}

	/*
	 * If an exact result was requested, use the remainder to correct the
	 * approximate quotient.  The remainder is in dividend[], immediately
	 * after the quotient digits.  Note, however, that although the remainder
	 * starts at dividend[qi = res_ndigitpairs], the first digit is the result
	 * of folding two remainder digits into one above, and the remainder
	 * currently only occupies var2ndigitpairs - 1 digits (the last digit of
	 * the working dividend was untouched by the computation above).  Thus we
	 * expand the remainder down by one base-NBASE^2 digit when we normalize
	 * it, so that it completely fills the last var2ndigitpairs digits of the
	 * dividend array.
	 */
	if (exact)
	{
		/* Normalize the remainder, expanding it down by one digit */
		remainder = &dividend[qi];
		carry = 0;
		for (i = var2ndigitpairs - 2; i >= 0; i--)
		{
			newdig = remainder[i] + carry;
			if (newdig < 0)
			{
				carry = -((-newdig - 1) / NBASE_SQR) - 1;
				newdig -= carry * NBASE_SQR;
			}
			else if (newdig >= NBASE_SQR)
			{
				carry = newdig / NBASE_SQR;
				newdig -= carry * NBASE_SQR;
			}
			else
				carry = 0;
			remainder[i + 1] = newdig;
		}
		remainder[0] = carry;

		if (remainder[0] < 0)
		{
			/*
			 * The remainder is negative, so the approximate quotient is too
			 * large.  Correct by reducing the quotient by one and adding the
			 * divisor to the remainder until the remainder is positive.  We
			 * expect the quotient to be off by at most one, which has been
			 * borne out in all testing, but not conclusively proven, so we
			 * allow for larger corrections, just in case.
			 */
			do
			{
				/* Add the divisor to the remainder */
				carry = 0;
				for (i = var2ndigitpairs - 1; i > 0; i--)
				{
					newdig = remainder[i] + divisor[i] + carry;
					if (newdig >= NBASE_SQR)
					{
						remainder[i] = newdig - NBASE_SQR;
						carry = 1;
					}
					else
					{
						remainder[i] = newdig;
						carry = 0;
					}
				}
				remainder[0] += divisor[0] + carry;

				/* Subtract 1 from the quotient (propagating carries later) */
				dividend[qi - 1]--;

			} while (remainder[0] < 0);
		}
		else
		{
			/*
			 * The remainder is nonnegative.  If it's greater than or equal to
			 * the divisor, then the approximate quotient is too small and
			 * must be corrected.  As above, we don't expect to have to apply
			 * more than one correction, but allow for it just in case.
			 */
			while (true)
			{
				bool		less = false;

				/* Is remainder < divisor? */
				for (i = 0; i < var2ndigitpairs; i++)
				{
					if (remainder[i] < divisor[i])
					{
						less = true;
						break;
					}
					if (remainder[i] > divisor[i])
						break;	/* remainder > divisor */
				}
				if (less)
					break;		/* quotient is correct */

				/* Subtract the divisor from the remainder */
				carry = 0;
				for (i = var2ndigitpairs - 1; i > 0; i--)
				{
					newdig = remainder[i] - divisor[i] + carry;
					if (newdig < 0)
					{
						remainder[i] = newdig + NBASE_SQR;
						carry = -1;
					}
					else
					{
						remainder[i] = newdig;
						carry = 0;
					}
				}
				remainder[0] = remainder[0] - divisor[0] + carry;

				/* Add 1 to the quotient (propagating carries later) */
				dividend[qi - 1]++;
			}
		}
	}

	/*
	 * Because the quotient digits were estimates that might have been off by
	 * one (and we didn't bother propagating carries when adjusting the
	 * quotient above), some quotient digits might be out of range, so do a
	 * final carry propagation pass to normalize back to base NBASE^2, and
	 * construct the base-NBASE result digits.  Note that this is still done
	 * at full precision w/guard digits.
	 */
	alloc_var(result, res_ndigits);
	res_digits = result->digits;
	carry = 0;
	for (i = res_ndigitpairs - 1; i >= 0; i--)
	{
		newdig = dividend[i] + carry;
		if (newdig < 0)
		{
			carry = -((-newdig - 1) / NBASE_SQR) - 1;
			newdig -= carry * NBASE_SQR;
		}
		else if (newdig >= NBASE_SQR)
		{
			carry = newdig / NBASE_SQR;
			newdig -= carry * NBASE_SQR;
		}
		else
			carry = 0;
		res_digits[2 * i + 1] = (NumericDigit) ((uint32) newdig % NBASE);
		res_digits[2 * i] = (NumericDigit) ((uint32) newdig / NBASE);
	}
	Assert(carry == 0);

	pfree(dividend);

	/*
	 * Finally, round or truncate the result to the requested precision.
	 */
	result->weight = res_weight;
	result->sign = res_sign;

	/* Round or truncate to target rscale (and set result->dscale) */
	if (round)
		round_var(result, rscale);
	else
		trunc_var(result, rscale);

	/* Strip leading and trailing zeroes */
	strip_var(result);
}

/* ---- numeric.c:9900-10009 VERBATIM (div_var_int) ---- */
/*
 * div_var_int() -
 *
 *	Divide a numeric variable by a 32-bit integer with the specified weight.
 *	The quotient var / (ival * NBASE^ival_weight) is stored in result.
 */
static void
div_var_int(const NumericVar *var, int ival, int ival_weight,
			NumericVar *result, int rscale, bool round)
{
	NumericDigit *var_digits = var->digits;
	int			var_ndigits = var->ndigits;
	int			res_sign;
	int			res_weight;
	int			res_ndigits;
	NumericDigit *res_buf;
	NumericDigit *res_digits;
	uint32		divisor;
	int			i;

	/* Guard against division by zero */
	if (ival == 0)
		ereport(ERROR,
				errcode(ERRCODE_DIVISION_BY_ZERO),
				errmsg("division by zero"));

	/* Result zero check */
	if (var_ndigits == 0)
	{
		zero_var(result);
		result->dscale = rscale;
		return;
	}

	/*
	 * Determine the result sign, weight and number of digits to calculate.
	 * The weight figured here is correct if the emitted quotient has no
	 * leading zero digits; otherwise strip_var() will fix things up.
	 */
	if (var->sign == NUMERIC_POS)
		res_sign = ival > 0 ? NUMERIC_POS : NUMERIC_NEG;
	else
		res_sign = ival > 0 ? NUMERIC_NEG : NUMERIC_POS;
	res_weight = var->weight - ival_weight;
	/* The number of accurate result digits we need to produce: */
	res_ndigits = res_weight + 1 + (rscale + DEC_DIGITS - 1) / DEC_DIGITS;
	/* ... but always at least 1 */
	res_ndigits = Max(res_ndigits, 1);
	/* If rounding needed, figure one more digit to ensure correct result */
	if (round)
		res_ndigits++;

	res_buf = digitbuf_alloc(res_ndigits + 1);
	res_buf[0] = 0;				/* spare digit for later rounding */
	res_digits = res_buf + 1;

	/*
	 * Now compute the quotient digits.  This is the short division algorithm
	 * described in Knuth volume 2, section 4.3.1 exercise 16, except that we
	 * allow the divisor to exceed the internal base.
	 *
	 * In this algorithm, the carry from one digit to the next is at most
	 * divisor - 1.  Therefore, while processing the next digit, carry may
	 * become as large as divisor * NBASE - 1, and so it requires a 64-bit
	 * integer if this exceeds UINT_MAX.
	 */
	divisor = abs(ival);

	if (divisor <= UINT_MAX / NBASE)
	{
		/* carry cannot overflow 32 bits */
		uint32		carry = 0;

		for (i = 0; i < res_ndigits; i++)
		{
			carry = carry * NBASE + (i < var_ndigits ? var_digits[i] : 0);
			res_digits[i] = (NumericDigit) (carry / divisor);
			carry = carry % divisor;
		}
	}
	else
	{
		/* carry may exceed 32 bits */
		uint64		carry = 0;

		for (i = 0; i < res_ndigits; i++)
		{
			carry = carry * NBASE + (i < var_ndigits ? var_digits[i] : 0);
			res_digits[i] = (NumericDigit) (carry / divisor);
			carry = carry % divisor;
		}
	}

	/* Store the quotient in result */
	digitbuf_free(result->buf);
	result->ndigits = res_ndigits;
	result->buf = res_buf;
	result->digits = res_digits;
	result->weight = res_weight;
	result->sign = res_sign;

	/* Round or truncate to target rscale (and set result->dscale) */
	if (round)
		round_var(result, rscale);
	else
		trunc_var(result, rscale);

	/* Strip leading/trailing zeroes */
	strip_var(result);
}

/* ---- numeric.c:10129-10195 VERBATIM (select_div_scale) ---- */
/*
 * Default scale selection for division
 *
 * Returns the appropriate result scale for the division result.
 */
static int
select_div_scale(const NumericVar *var1, const NumericVar *var2)
{
	int			weight1,
				weight2,
				qweight,
				i;
	NumericDigit firstdigit1,
				firstdigit2;
	int			rscale;

	/*
	 * The result scale of a division isn't specified in any SQL standard. For
	 * PostgreSQL we select a result scale that will give at least
	 * NUMERIC_MIN_SIG_DIGITS significant digits, so that numeric gives a
	 * result no less accurate than float8; but use a scale not less than
	 * either input's display scale.
	 */

	/* Get the actual (normalized) weight and first digit of each input */

	weight1 = 0;				/* values to use if var1 is zero */
	firstdigit1 = 0;
	for (i = 0; i < var1->ndigits; i++)
	{
		firstdigit1 = var1->digits[i];
		if (firstdigit1 != 0)
		{
			weight1 = var1->weight - i;
			break;
		}
	}

	weight2 = 0;				/* values to use if var2 is zero */
	firstdigit2 = 0;
	for (i = 0; i < var2->ndigits; i++)
	{
		firstdigit2 = var2->digits[i];
		if (firstdigit2 != 0)
		{
			weight2 = var2->weight - i;
			break;
		}
	}

	/*
	 * Estimate weight of quotient.  If the two first digits are equal, we
	 * can't be sure, but assume that var1 is less than var2.
	 */
	qweight = weight1 - weight2;
	if (firstdigit1 <= firstdigit2)
		qweight--;

	/* Select result scale */
	rscale = NUMERIC_MIN_SIG_DIGITS - qweight * DEC_DIGITS;
	rscale = Max(rscale, var1->dscale);
	rscale = Max(rscale, var2->dscale);
	rscale = Max(rscale, NUMERIC_MIN_DISPLAY_SCALE);
	rscale = Min(rscale, NUMERIC_MAX_DISPLAY_SCALE);

	return rscale;
}

/* ---- numeric.c:10198-10223 VERBATIM (mod_var) ---- */
/*
 * mod_var() -
 *
 *	Calculate the modulo of two numerics at variable level
 */
static void
mod_var(const NumericVar *var1, const NumericVar *var2, NumericVar *result)
{
	NumericVar	tmp;

	init_var(&tmp);

	/* ---------
	 * We do this using the equation
	 *		mod(x,y) = x - trunc(x/y)*y
	 * div_var can be persuaded to give us trunc(x/y) directly.
	 * ----------
	 */
	div_var(var1, var2, &tmp, 0, false, true);

	mul_var(var2, &tmp, &tmp, var2->dscale);

	sub_var(var1, &tmp, result);

	free_var(&tmp);
}

/* ---- numeric.c:10226-10293 VERBATIM (div_mod_var) ---- */
/*
 * div_mod_var() -
 *
 *	Calculate the truncated integer quotient and numeric remainder of two
 *	numeric variables.  The remainder is precise to var2's dscale.
 */
static void
div_mod_var(const NumericVar *var1, const NumericVar *var2,
			NumericVar *quot, NumericVar *rem)
{
	NumericVar	q;
	NumericVar	r;

	init_var(&q);
	init_var(&r);

	/*
	 * Use div_var() with exact = false to get an initial estimate for the
	 * integer quotient (truncated towards zero).  This might be slightly
	 * inaccurate, but we correct it below.
	 */
	div_var(var1, var2, &q, 0, false, false);

	/* Compute initial estimate of remainder using the quotient estimate. */
	mul_var(var2, &q, &r, var2->dscale);
	sub_var(var1, &r, &r);

	/*
	 * Adjust the results if necessary --- the remainder should have the same
	 * sign as var1, and its absolute value should be less than the absolute
	 * value of var2.
	 */
	while (r.ndigits != 0 && r.sign != var1->sign)
	{
		/* The absolute value of the quotient is too large */
		if (var1->sign == var2->sign)
		{
			sub_var(&q, &const_one, &q);
			add_var(&r, var2, &r);
		}
		else
		{
			add_var(&q, &const_one, &q);
			sub_var(&r, var2, &r);
		}
	}

	while (cmp_abs(&r, var2) >= 0)
	{
		/* The absolute value of the quotient is too small */
		if (var1->sign == var2->sign)
		{
			add_var(&q, &const_one, &q);
			sub_var(&r, var2, &r);
		}
		else
		{
			sub_var(&q, &const_one, &q);
			add_var(&r, var2, &r);
		}
	}

	set_var_from_var(&q, quot);
	set_var_from_var(&r, rem);

	free_var(&q);
	free_var(&r);
}

/* ---- numeric.c:10296-10317 VERBATIM (ceil_var) ---- */
/*
 * ceil_var() -
 *
 *	Return the smallest integer greater than or equal to the argument
 *	on variable level
 */
static void
ceil_var(const NumericVar *var, NumericVar *result)
{
	NumericVar	tmp;

	init_var(&tmp);
	set_var_from_var(var, &tmp);

	trunc_var(&tmp, 0);

	if (var->sign == NUMERIC_POS && cmp_var(var, &tmp) != 0)
		add_var(&tmp, &const_one, &tmp);

	set_var_from_var(&tmp, result);
	free_var(&tmp);
}

/* ---- numeric.c:10320-10341 VERBATIM (floor_var) ---- */
/*
 * floor_var() -
 *
 *	Return the largest integer equal to or less than the argument
 *	on variable level
 */
static void
floor_var(const NumericVar *var, NumericVar *result)
{
	NumericVar	tmp;

	init_var(&tmp);
	set_var_from_var(var, &tmp);

	trunc_var(&tmp, 0);

	if (var->sign == NUMERIC_NEG && cmp_var(var, &tmp) != 0)
		sub_var(&tmp, &const_one, &tmp);

	set_var_from_var(&tmp, result);
	free_var(&tmp);
}
/* ---- numeric.c:7533-7552 VERBATIM (set_var_from_num) ---- */
/*
 * set_var_from_num() -
 *
 *	Convert the packed db format into a variable
 */
static void
set_var_from_num(Numeric num, NumericVar *dest)
{
	int			ndigits;

	ndigits = NUMERIC_NDIGITS(num);

	alloc_var(dest, ndigits);

	dest->weight = NUMERIC_WEIGHT(num);
	dest->sign = NUMERIC_SIGN(num);
	dest->dscale = NUMERIC_DSCALE(num);

	memcpy(dest->digits, NUMERIC_DIGITS(num), ndigits * sizeof(NumericDigit));
}

/* ---- numeric.c:1470-1501 VERBATIM (numeric_sign_internal) ---- */
/*
 * numeric_sign_internal() -
 *
 * Returns -1 if the argument is less than 0, 0 if the argument is equal
 * to 0, and 1 if the argument is greater than zero.  Caller must have
 * taken care of the NaN case, but we can handle infinities here.
 */
static int
numeric_sign_internal(Numeric num)
{
	if (NUMERIC_IS_SPECIAL(num))
	{
		Assert(!NUMERIC_IS_NAN(num));
		/* Must be Inf or -Inf */
		if (NUMERIC_IS_PINF(num))
			return 1;
		else
			return -1;
	}

	/*
	 * The packed format is known to be totally zero digit trimmed always. So
	 * once we've eliminated specials, we can identify a zero by the fact that
	 * there are no digits at all.
	 */
	else if (NUMERIC_NDIGITS(num) == 0)
		return 0;
	else if (NUMERIC_SIGN(num) == NUMERIC_NEG)
		return -1;
	else
		return 1;
}
/* ---- numeric.c:10013-10125 VERBATIM (div_var_int64) ---- */
/*
 * div_var_int64() -
 *
 *	Divide a numeric variable by a 64-bit integer with the specified weight.
 *	The quotient var / (ival * NBASE^ival_weight) is stored in result.
 *
 *	This duplicates the logic in div_var_int(), so any changes made there
 *	should be made here too.
 */
static void
div_var_int64(const NumericVar *var, int64 ival, int ival_weight,
			  NumericVar *result, int rscale, bool round)
{
	NumericDigit *var_digits = var->digits;
	int			var_ndigits = var->ndigits;
	int			res_sign;
	int			res_weight;
	int			res_ndigits;
	NumericDigit *res_buf;
	NumericDigit *res_digits;
	uint64		divisor;
	int			i;

	/* Guard against division by zero */
	if (ival == 0)
		ereport(ERROR,
				errcode(ERRCODE_DIVISION_BY_ZERO),
				errmsg("division by zero"));

	/* Result zero check */
	if (var_ndigits == 0)
	{
		zero_var(result);
		result->dscale = rscale;
		return;
	}

	/*
	 * Determine the result sign, weight and number of digits to calculate.
	 * The weight figured here is correct if the emitted quotient has no
	 * leading zero digits; otherwise strip_var() will fix things up.
	 */
	if (var->sign == NUMERIC_POS)
		res_sign = ival > 0 ? NUMERIC_POS : NUMERIC_NEG;
	else
		res_sign = ival > 0 ? NUMERIC_NEG : NUMERIC_POS;
	res_weight = var->weight - ival_weight;
	/* The number of accurate result digits we need to produce: */
	res_ndigits = res_weight + 1 + (rscale + DEC_DIGITS - 1) / DEC_DIGITS;
	/* ... but always at least 1 */
	res_ndigits = Max(res_ndigits, 1);
	/* If rounding needed, figure one more digit to ensure correct result */
	if (round)
		res_ndigits++;

	res_buf = digitbuf_alloc(res_ndigits + 1);
	res_buf[0] = 0;				/* spare digit for later rounding */
	res_digits = res_buf + 1;

	/*
	 * Now compute the quotient digits.  This is the short division algorithm
	 * described in Knuth volume 2, section 4.3.1 exercise 16, except that we
	 * allow the divisor to exceed the internal base.
	 *
	 * In this algorithm, the carry from one digit to the next is at most
	 * divisor - 1.  Therefore, while processing the next digit, carry may
	 * become as large as divisor * NBASE - 1, and so it requires a 128-bit
	 * integer if this exceeds PG_UINT64_MAX.
	 */
	divisor = i64abs(ival);

	if (divisor <= PG_UINT64_MAX / NBASE)
	{
		/* carry cannot overflow 64 bits */
		uint64		carry = 0;

		for (i = 0; i < res_ndigits; i++)
		{
			carry = carry * NBASE + (i < var_ndigits ? var_digits[i] : 0);
			res_digits[i] = (NumericDigit) (carry / divisor);
			carry = carry % divisor;
		}
	}
	else
	{
		/* carry may exceed 64 bits */
		uint128		carry = 0;

		for (i = 0; i < res_ndigits; i++)
		{
			carry = carry * NBASE + (i < var_ndigits ? var_digits[i] : 0);
			res_digits[i] = (NumericDigit) (carry / divisor);
			carry = carry % divisor;
		}
	}

	/* Store the quotient in result */
	digitbuf_free(result->buf);
	result->ndigits = res_ndigits;
	result->buf = res_buf;
	result->digits = res_digits;
	result->weight = res_weight;
	result->sign = res_sign;

	/* Round or truncate to target rscale (and set result->dscale) */
	if (round)
		round_var(result, rscale);
	else
		trunc_var(result, rscale);

	/* Strip leading/trailing zeroes */
	strip_var(result);
}

