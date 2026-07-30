/*
 * Vendored PostgreSQL C for the numeric tail-triage rows (N1-N3):
 *   N1 header/inspection: numerictypmodin (make_numeric_typmod_safe), the
 *      typmod codec trio (is_valid_numeric_typmod / numeric_typmod_precision
 *      / numeric_typmod_scale — numerictypmodout's scalar core),
 *      numeric_sign (+ numeric_sign_internal), numeric_scale.
 *   N2 int->numeric conversions: int64_to_numeric (int2/int4/int8_numeric)
 *      via int64_to_numericvar + make_result_opt_error — the C digit
 *      emission AND packed-header encode are IN the theorem; the harness
 *      compares the full varlena image byte-for-byte.
 *   N3 int-agg transarray rows: int2/int4_sum, int2/int4_avg_accum,
 *      int2/int4_avg_accum_inv, int4_avg_combine over the Int8TransTypeData
 *      int8[2] {count,sum} state.
 *
 * Provenance: src/backend/utils/adt/numeric.c, branch REL_18_STABLE,
 * fetched 2026-07-29 (same ref as pg_numeric_cmp.c / pg_numeric_rows.c);
 * constants NUMERIC_MAX_PRECISION/NUMERIC_MIN_SCALE/NUMERIC_MAX_SCALE from
 * src/include/utils/numeric.h same ref.  Bodies verbatim except the shims
 * below.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * SHIMS (plumbing only, never logic; each marked at its site):
 *  T1. PACKED-HEADER ACCESS SHIM (family convention, see pg_numeric_cmp.c):
 *      numeric_sign_internal / numeric_sign / numeric_scale only CONSUME a
 *      Numeric through the accessor macros; they take the explicit
 *      (sign-code, ndigits, dscale) tuple those macros produce.  The C-side
 *      packed-header DECODE is out of the theorem for those rows; the Rust
 *      decode (fed real packed images) is in.  The C-side header ENCODE
 *      (make_result_opt_error) IS in the theorem for the N2 rows and for
 *      numeric_sign's result images (structs vendored verbatim below).
 *  T2. FMGR SHIM: PG_FUNCTION_ARGS / PG_GETARG_x / PG_RETURN_x /
 *      PG_ARGISNULL / PG_RETURN_NULL unwrapped to plain C signatures with
 *      explicit isnull in/out flags.  Detoasting out of scope
 *      (pre-detoasted caller contract).
 *  T3. ereport/ereturn/elog(ERROR) -> set *err = 1 and return a sentinel
 *      (PROOF_EREPORT_FLAG convention); message text out of proof.
 *      numerictypmodin passes escontext=NULL so both ereturn arms are hard
 *      errors, matching the shipped Rust wrapper.
 *  T4. palloc -> caller-provided / static buffer:
 *      - make_result_opt_error writes the packed image into a caller
 *        buffer (PG_TAIL_IMAGE_CAP bytes); SET_VARSIZE transcribed as the
 *        4-byte little-endian length<<2 word varattrib_4b produces on the
 *        target (identical to the shipped Rust image header).
 *      - alloc_var's digitbuf_alloc -> bump pointer into a static
 *        NumericDigit arena reset at each entry point (family convention;
 *        an overflow surfaces as a CBMC out-of-bounds violation).
 *      - dump_numeric (NUMERIC_DEBUG) is compiled out upstream; omitted.
 *  T5. ARRAY ACCESS SHIM (int8[2] transarray family): ARR_HASNULL /
 *      ARR_SIZE / ARR_DATA_PTR are rewritten onto explicit
 *      (count, sum, hasnull, size) parameters — the spec-level values those
 *      macros produce from an _int8 array image; outputs returned through
 *      out-params.  ARR_OVERHEAD_NONULLS(1) spelled as its target-platform
 *      value 24 (MAXALIGN(sizeof(ArrayType) + 1*sizeof(int) + 1*sizeof(int))
 *      with 8-byte MAXALIGN), parity-asserted against the shipped Rust
 *      constant in the harness.  The Rust side decodes REAL array images
 *      built per the on-disk spec, so its decode is in-theorem.
 *      PG_GETARG_ARRAYTYPE_P_COPY's copy itself is allocation plumbing
 *      (identity on the image) — the harness asserts the Rust copy arm's
 *      value/aliasing behavior directly.
 *  T6. USE_FLOAT8_BYVAL is defined on the target (int8 pass-by-value), so
 *      int2_sum/int4_sum's #ifndef in-place branch is compiled out —
 *      vendored accordingly (matches the shipped Rust comment).
 *  T7. pg_abs_s64 (src/include/common/int.h) -> unsigned negation
 *      transcription (i64-MIN-safe), semantics identical.
 *  Types: int16/int32/int64/uint16/uint64/Size typedef'd locally;
 *  Assert -> no-op; FLEXIBLE_ARRAY_MEMBER -> empty.
 */

#include <stdint.h>
#include <stddef.h>
#include <string.h>
#include <stdbool.h>

typedef int16_t int16;
typedef int32_t int32;
typedef int64_t int64;
typedef uint16_t uint16;
typedef uint32_t uint32;
typedef uint64_t uint64;
typedef size_t Size;

#define Assert(x) ((void) 0)
#define FLEXIBLE_ARRAY_MEMBER
#define VARHDRSZ ((int32) sizeof(uint32))

/* ---- numeric.c representation (verbatim, REL_18_STABLE) ---- */

#define NBASE		10000
#define HALF_NBASE	5000
#define DEC_DIGITS	4

typedef int16 NumericDigit;

struct NumericShort
{
	uint16		n_header;
	NumericDigit n_data[FLEXIBLE_ARRAY_MEMBER];
};

struct NumericLong
{
	uint16		n_sign_dscale;
	int16		n_weight;
	NumericDigit n_data[FLEXIBLE_ARRAY_MEMBER];
};

union NumericChoice
{
	uint16		n_header;
	struct NumericLong n_long;
	struct NumericShort n_short;
};

struct NumericData
{
	int32		vl_len_;
	union NumericChoice choice;
};

typedef struct NumericData *Numeric;

#define NUMERIC_SIGN_MASK	0xC000
#define NUMERIC_POS			0x0000
#define NUMERIC_NEG			0x4000
#define NUMERIC_SHORT		0x8000
#define NUMERIC_SPECIAL		0xC000

#define NUMERIC_HDRSZ	(VARHDRSZ + sizeof(uint16) + sizeof(int16))
#define NUMERIC_HDRSZ_SHORT (VARHDRSZ + sizeof(uint16))

#define NUMERIC_HEADER_IS_SHORT(n)	(((n)->choice.n_header & 0x8000) != 0)
#define NUMERIC_HEADER_SIZE(n) \
	(VARHDRSZ + sizeof(uint16) + \
	 (NUMERIC_HEADER_IS_SHORT(n) ? 0 : sizeof(int16)))

#define NUMERIC_NAN				0xC000
#define NUMERIC_PINF			0xD000
#define NUMERIC_NINF			0xF000
#define NUMERIC_INF_SIGN_MASK	0x2000

#define NUMERIC_SHORT_SIGN_MASK			0x2000
#define NUMERIC_SHORT_DSCALE_MASK		0x1F80
#define NUMERIC_SHORT_DSCALE_SHIFT		7
#define NUMERIC_SHORT_DSCALE_MAX		\
	(NUMERIC_SHORT_DSCALE_MASK >> NUMERIC_SHORT_DSCALE_SHIFT)
#define NUMERIC_SHORT_WEIGHT_SIGN_MASK	0x0040
#define NUMERIC_SHORT_WEIGHT_MASK		0x003F
#define NUMERIC_SHORT_WEIGHT_MAX		NUMERIC_SHORT_WEIGHT_MASK
#define NUMERIC_SHORT_WEIGHT_MIN		(-(NUMERIC_SHORT_WEIGHT_MASK+1))

#define NUMERIC_DSCALE_MASK			0x3FFF

#define NUMERIC_CAN_BE_SHORT(scale,weight) \
	((scale) <= NUMERIC_SHORT_DSCALE_MAX && \
	(weight) <= NUMERIC_SHORT_WEIGHT_MAX && \
	(weight) >= NUMERIC_SHORT_WEIGHT_MIN)

/* SHIM T4: VARSIZE/SET_VARSIZE on the out-buffer image (varattrib_4b) */
#define TAIL_VARSIZE(p)		(((uint32) *(uint32 *) (p)) >> 2)
#define TAIL_SET_VARSIZE(p, len) (*(uint32 *) (p) = ((uint32) (len)) << 2)

/* result-image accessor macros on the buffer-backed Numeric (verbatim
 * bodies; VARSIZE spelled through the shim above) */
#define NUMERIC_IS_SHORT_N(n)	(((n)->choice.n_header & NUMERIC_SIGN_MASK) == NUMERIC_SHORT)
#define NUMERIC_NDIGITS_N(n, size) \
	(((size) - NUMERIC_HEADER_SIZE(n)) / sizeof(NumericDigit))
#define NUMERIC_WEIGHT_N(n)	(NUMERIC_HEADER_IS_SHORT((n)) ? \
	(((n)->choice.n_short.n_header & NUMERIC_SHORT_WEIGHT_SIGN_MASK ? \
		~NUMERIC_SHORT_WEIGHT_MASK : 0) \
	 | ((n)->choice.n_short.n_header & NUMERIC_SHORT_WEIGHT_MASK)) \
	: ((n)->choice.n_long.n_weight))
#define NUMERIC_DSCALE_N(n)	(NUMERIC_HEADER_IS_SHORT((n)) ? \
	((n)->choice.n_short.n_header & NUMERIC_SHORT_DSCALE_MASK) \
		>> NUMERIC_SHORT_DSCALE_SHIFT \
	: ((n)->choice.n_long.n_sign_dscale & NUMERIC_DSCALE_MASK))
#define NUMERIC_DIGITS_N(n) (NUMERIC_HEADER_IS_SHORT(n) ? \
	(n)->choice.n_short.n_data : (n)->choice.n_long.n_data)

/* ---- NumericVar + digit-buffer arena (SHIM T4) ---- */

typedef struct NumericVar
{
	int			ndigits;
	int			weight;
	int			sign;
	int			dscale;
	NumericDigit *buf;
	NumericDigit *digits;
} NumericVar;

#define TAIL_DIGIT_ARENA 64
static NumericDigit tail_digit_arena[TAIL_DIGIT_ARENA];
static int	tail_digit_next = 0;

static NumericDigit *
tail_digitbuf_alloc(int ndigits)
{
	NumericDigit *p = &tail_digit_arena[tail_digit_next];

	tail_digit_next += ndigits;
	return p;					/* arena overflow = CBMC OOB violation */
}

/* verbatim body; digitbuf_free -> no-op, digitbuf_alloc -> arena */
static void
alloc_var(NumericVar *var, int ndigits)
{
	var->buf = tail_digitbuf_alloc(ndigits + 1);
	var->buf[0] = 0;			/* spare digit for rounding */
	var->digits = var->buf + 1;
	var->ndigits = ndigits;
}

/*
 * int64_to_numericvar() - verbatim REL_18 body (pg_abs_s64 per SHIM T7).
 */
static void
pg_tail_int64_to_numericvar(int64 val, NumericVar *var)
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
		uval = (uint64) 0 - (uint64) val;	/* SHIM T7: pg_abs_s64 */
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

/*
 * make_result_opt_error() - verbatim REL_18 body; palloc -> caller buffer
 * (SHIM T4), ereport -> *err (SHIM T3), dump_numeric omitted.  Returns the
 * image length written to `out`, or -1 with *err = 1.
 */
#define PG_TAIL_IMAGE_CAP 32

static int
pg_tail_make_result_opt_error(const NumericVar *var, unsigned char *out, int *err)
{
	Numeric		result;
	NumericDigit *digits = var->digits;
	int			weight = var->weight;
	int			sign = var->sign;
	int			n;
	Size		len;

	if ((sign & NUMERIC_SIGN_MASK) == NUMERIC_SPECIAL)
	{
		if (!(sign == NUMERIC_NAN ||
			  sign == NUMERIC_PINF ||
			  sign == NUMERIC_NINF))
		{
			*err = 1;			/* elog(ERROR, "invalid numeric sign value") */
			return -1;
		}

		result = (Numeric) out;

		TAIL_SET_VARSIZE(result, NUMERIC_HDRSZ_SHORT);
		result->choice.n_header = sign;
		/* the header word is all we need */

		return NUMERIC_HDRSZ_SHORT;
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
		result = (Numeric) out;
		TAIL_SET_VARSIZE(result, len);
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
		result = (Numeric) out;
		TAIL_SET_VARSIZE(result, len);
		result->choice.n_long.n_sign_dscale =
			sign | (var->dscale & NUMERIC_DSCALE_MASK);
		result->choice.n_long.n_weight = weight;
	}

	Assert(NUMERIC_NDIGITS_N(result, len) == n);
	if (n > 0)
		memcpy(NUMERIC_DIGITS_N(result), digits, n * sizeof(NumericDigit));

	/* Check for overflow of int16 fields */
	if (NUMERIC_WEIGHT_N(result) != weight ||
		NUMERIC_DSCALE_N(result) != var->dscale)
	{
		*err = 1;				/* ereport(ERROR, value overflows numeric format) */
		return -1;
	}

	return (int) len;
}

/*
 * int64_to_numeric() - verbatim structure (init_var/free_var are palloc
 * plumbing; the arena reset stands in).  Writes the packed varlena image
 * (header included) to `out`, returns its length.  Cannot fail for int64
 * inputs (weight <= 4, dscale 0), asserted in the harness via err == 0.
 */
int
pg_int64_to_numeric_image(int64 val, unsigned char *out, int *err)
{
	NumericVar	result;
	int			len;

	tail_digit_next = 0;		/* SHIM T4 arena reset */
	*err = 0;

	pg_tail_int64_to_numericvar(val, &result);

	len = pg_tail_make_result_opt_error(&result, out, err);

	return len;
}

/* ---- N1: typmod codec + numerictypmodin ---- */

/* utils/numeric.h, REL_18_STABLE */
#define NUMERIC_MAX_PRECISION		1000
#define NUMERIC_MIN_SCALE			(-1000)
#define NUMERIC_MAX_SCALE			1000

/* verbatim numeric.c bodies (inline fns there) */
static inline int32
make_numeric_typmod(int precision, int scale)
{
	return ((precision << 16) | (scale & 0x7ff)) + VARHDRSZ;
}

static inline bool
is_valid_numeric_typmod(int32 typmod)
{
	return typmod >= (int32) (VARHDRSZ);
}

static inline int
numeric_typmod_precision(int32 typmod)
{
	return ((typmod - VARHDRSZ) >> 16) & 0xffff;
}

static inline int
numeric_typmod_scale(int32 typmod)
{
	return (((typmod - VARHDRSZ) & 0x7ff) ^ 1024) - 1024;
}

int
pg_is_valid_numeric_typmod(int32 typmod)
{
	return is_valid_numeric_typmod(typmod);
}

int32
pg_numeric_typmod_precision(int32 typmod)
{
	return numeric_typmod_precision(typmod);
}

int32
pg_numeric_typmod_scale(int32 typmod)
{
	return numeric_typmod_scale(typmod);
}

/*
 * make_numeric_typmod_safe() - verbatim; escontext = NULL in the
 * numerictypmodin call, so ereturn is a hard error -> *err (SHIM T3).
 */
static int32
pg_tail_make_numeric_typmod_safe(int32 precision, int32 scale, int *err)
{
	if (precision < 1 || precision > NUMERIC_MAX_PRECISION)
	{
		*err = 1;
		return -1;
	}
	if (scale < NUMERIC_MIN_SCALE || scale > NUMERIC_MAX_SCALE)
	{
		*err = 1;
		return -1;
	}

	return make_numeric_typmod(precision, scale);
}

/*
 * numerictypmodin() - verbatim dispatch; ArrayGetIntegerTypmods -> explicit
 * (tl, n) per SHIM T2 (the shipped Rust wrapper decodes the real array;
 * its core takes the same decoded slice).
 */
int32
pg_numerictypmodin(const int32 *tl, int n, int *err)
{
	int32		typmod;

	*err = 0;

	if (n == 2)
		typmod = pg_tail_make_numeric_typmod_safe(tl[0], tl[1], err);
	else if (n == 1)
	{
		/* scale defaults to zero */
		typmod = pg_tail_make_numeric_typmod_safe(tl[0], 0, err);
	}
	else
	{
		*err = 1;				/* invalid NUMERIC type modifier */
		typmod = 0;				/* keep compiler quiet */
	}

	return typmod;
}

/* ---- N1: numeric_sign / numeric_scale (SHIM T1 explicit tuple) ---- */

/* NUMERIC_IS_* predicates on the explicit sign code (family convention) */
#define NUMERIC_IS_SPECIAL_S(sign)	(((sign) & NUMERIC_SIGN_MASK) == NUMERIC_SPECIAL)
#define NUMERIC_IS_NAN_S(sign)		((sign) == NUMERIC_NAN)
#define NUMERIC_IS_PINF_S(sign)		((sign) == NUMERIC_PINF)

/*
 * numeric_sign_internal() - verbatim on the explicit tuple.
 */
static int
pg_tail_numeric_sign_internal(int sign, int ndigits)
{
	if (NUMERIC_IS_SPECIAL_S(sign))
	{
		Assert(!NUMERIC_IS_NAN_S(sign));
		/* Must be Inf or -Inf */
		if (NUMERIC_IS_PINF_S(sign))
			return 1;
		else
			return -1;
	}
	else if (ndigits == 0)
		return 0;
	else if (sign == NUMERIC_NEG)
		return -1;
	else
		return 1;
}

/*
 * numeric_sign() - verbatim structure: NaN -> make_result(&const_nan);
 * otherwise make_result of const_zero/const_one/const_minus_one (numeric.c
 * constants: ndigits 0/1/1, weight 0, dscale 0, digit 1).  Emits the packed
 * result image to `out` (C header ENCODE in-theorem), returns its length.
 */
int
pg_numeric_sign_image(int sign, int ndigits, unsigned char *out, int *err)
{
	static const NumericDigit const_one_data[1] = {1};
	NumericVar	v;

	*err = 0;

	if (NUMERIC_IS_NAN_S(sign))
	{
		v.ndigits = 0;
		v.weight = 0;
		v.sign = NUMERIC_NAN;
		v.dscale = 0;
		v.buf = NULL;
		v.digits = NULL;
		return pg_tail_make_result_opt_error(&v, out, err);
	}

	v.weight = 0;
	v.dscale = 0;
	v.buf = NULL;
	v.digits = (NumericDigit *) const_one_data;

	switch (pg_tail_numeric_sign_internal(sign, ndigits))
	{
		case 0:
			v.sign = NUMERIC_POS;
			v.ndigits = 0;
			break;
		case 1:
			v.sign = NUMERIC_POS;
			v.ndigits = 1;
			break;
		default:
			v.sign = NUMERIC_NEG;
			v.ndigits = 1;
			break;
	}
	return pg_tail_make_result_opt_error(&v, out, err);
}

/*
 * numeric_scale() - verbatim on the explicit tuple: special -> NULL,
 * else NUMERIC_DSCALE.
 */
int32
pg_numeric_scale(int sign, int32 dscale, int *isnull)
{
	if (NUMERIC_IS_SPECIAL_S(sign))
	{
		*isnull = 1;
		return 0;
	}

	*isnull = 0;
	return dscale;
}

/* ---- N3: int2_sum / int4_sum (SHIM T2 + T6) ---- */

/*
 * int2_sum() - verbatim under USE_FLOAT8_BYVAL (in-place branch compiled
 * out); PG_ARGISNULL/PG_RETURN_NULL as explicit flags.
 */
int64
pg_int2_sum(int64 oldsum, int arg0null, int16 newval16, int arg1null, int *retnull)
{
	int64		newval;

	*retnull = 0;

	if (arg0null)
	{
		/* No non-null input seen so far... */
		if (arg1null)
		{
			*retnull = 1;		/* still no non-null */
			return 0;
		}
		/* This is the first non-null input. */
		newval = (int64) newval16;
		return newval;
	}

	{
		/* Leave sum unchanged if new input is null. */
		if (arg1null)
			return oldsum;

		/* OK to do the addition. */
		newval = oldsum + (int64) newval16;

		return newval;
	}
}

/*
 * int4_sum() - verbatim under USE_FLOAT8_BYVAL.
 */
int64
pg_int4_sum(int64 oldsum, int arg0null, int32 newval32, int arg1null, int *retnull)
{
	int64		newval;

	*retnull = 0;

	if (arg0null)
	{
		if (arg1null)
		{
			*retnull = 1;
			return 0;
		}
		newval = (int64) newval32;
		return newval;
	}

	{
		if (arg1null)
			return oldsum;

		newval = oldsum + (int64) newval32;

		return newval;
	}
}

/* ---- N3: int8[2] transarray family (SHIM T5) ---- */

typedef struct Int8TransTypeData
{
	int64		count;
	int64		sum;
} Int8TransTypeData;

/* ARR_OVERHEAD_NONULLS(1) on the target (MAXALIGN 8): 24 */
#define TAIL_ARR_OVERHEAD_NONULLS_1 24

/* shared verbatim validation (SHIM T5 explicit tuple) */
static int
pg_tail_check_int8_transarray(int hasnull, uint32 size)
{
	if (hasnull ||
		size != TAIL_ARR_OVERHEAD_NONULLS_1 + sizeof(Int8TransTypeData))
		return 0;				/* elog(ERROR, "expected 2-element int8 array") */
	return 1;
}

/*
 * int2_avg_accum() / int4_avg_accum() - verbatim mutation on the explicit
 * state tuple; the AggCheckCallContext copy choice is allocation plumbing
 * (SHIM T5) asserted on the Rust side.
 */
int
pg_int2_avg_accum(int64 count, int64 sum, int hasnull, uint32 size,
				  int16 newval, int64 *out_count, int64 *out_sum, int *err)
{
	Int8TransTypeData transdata;

	*err = 0;
	if (!pg_tail_check_int8_transarray(hasnull, size))
	{
		*err = 1;
		return 0;
	}
	transdata.count = count;
	transdata.sum = sum;
	transdata.count++;
	transdata.sum += newval;
	*out_count = transdata.count;
	*out_sum = transdata.sum;
	return 0;
}

int
pg_int4_avg_accum(int64 count, int64 sum, int hasnull, uint32 size,
				  int32 newval, int64 *out_count, int64 *out_sum, int *err)
{
	Int8TransTypeData transdata;

	*err = 0;
	if (!pg_tail_check_int8_transarray(hasnull, size))
	{
		*err = 1;
		return 0;
	}
	transdata.count = count;
	transdata.sum = sum;
	transdata.count++;
	transdata.sum += newval;
	*out_count = transdata.count;
	*out_sum = transdata.sum;
	return 0;
}

/*
 * int2_avg_accum_inv() / int4_avg_accum_inv() - verbatim (count--, sum -=).
 */
int
pg_int2_avg_accum_inv(int64 count, int64 sum, int hasnull, uint32 size,
					  int16 newval, int64 *out_count, int64 *out_sum, int *err)
{
	Int8TransTypeData transdata;

	*err = 0;
	if (!pg_tail_check_int8_transarray(hasnull, size))
	{
		*err = 1;
		return 0;
	}
	transdata.count = count;
	transdata.sum = sum;
	transdata.count--;
	transdata.sum -= newval;
	*out_count = transdata.count;
	*out_sum = transdata.sum;
	return 0;
}

int
pg_int4_avg_accum_inv(int64 count, int64 sum, int hasnull, uint32 size,
					  int32 newval, int64 *out_count, int64 *out_sum, int *err)
{
	Int8TransTypeData transdata;

	*err = 0;
	if (!pg_tail_check_int8_transarray(hasnull, size))
	{
		*err = 1;
		return 0;
	}
	transdata.count = count;
	transdata.sum = sum;
	transdata.count--;
	transdata.sum -= newval;
	*out_count = transdata.count;
	*out_sum = transdata.sum;
	return 0;
}

/*
 * int4_avg_combine() - verbatim; the non-aggregate-context elog and the two
 * validations become err codes 1/2/3 (context / array 1 / array 2) so the
 * harness can assert which arm fired.  in_agg is the explicit
 * AggCheckCallContext verdict (SHIM T2; the Rust side's real
 * fcinfo->context demux is in-theorem).
 */
int
pg_int4_avg_combine(int in_agg,
					int64 count1, int64 sum1, int hasnull1, uint32 size1,
					int64 count2, int64 sum2, int hasnull2, uint32 size2,
					int64 *out_count, int64 *out_sum, int *err)
{
	Int8TransTypeData state1;
	Int8TransTypeData state2;

	*err = 0;

	if (!in_agg)
	{
		*err = 1;				/* elog(ERROR, "aggregate function called in
								 * non-aggregate context") */
		return 0;
	}

	if (!pg_tail_check_int8_transarray(hasnull1, size1))
	{
		*err = 2;
		return 0;
	}
	if (!pg_tail_check_int8_transarray(hasnull2, size2))
	{
		*err = 3;
		return 0;
	}

	state1.count = count1;
	state1.sum = sum1;
	state2.count = count2;
	state2.sum = sum2;

	state1.count += state2.count;
	state1.sum += state2.sum;

	*out_count = state1.count;
	*out_sum = state1.sum;
	return 0;
}

/* ---- N1 ladder: numeric_send (2461) + numeric()'s special arm (1703) ---- */

/*
 * pq_sendint16 (src/backend/libpq/pqformat.h) emits network byte order;
 * transcribed as explicit big-endian byte writes into the caller buffer
 * (SHIM T2/T4 — the StringInfo is allocation plumbing).
 */
static int tail_send_off;

static void
tail_sendint16(unsigned char *out, uint16 v)
{
	out[tail_send_off++] = (unsigned char) (v >> 8);
	out[tail_send_off++] = (unsigned char) (v & 0xFF);
}

/*
 * numeric_send() - verbatim structure (REL_18: init_var_from_num then
 * pq_sendint16 of ndigits/weight/sign/dscale + each digit); the Numeric
 * access is the SHIM T1 explicit tuple.  Returns the payload length.
 */
int
pg_numeric_send(int sign, int weight, int dscale,
				const int16 *digits, int ndigits, unsigned char *out)
{
	int			i;

	tail_send_off = 0;
	tail_sendint16(out, (uint16) ndigits);
	tail_sendint16(out, (uint16) weight);
	tail_sendint16(out, (uint16) sign);
	tail_sendint16(out, (uint16) dscale);
	for (i = 0; i < ndigits; i++)
		tail_sendint16(out, (uint16) digits[i]);

	return tail_send_off;
}

/*
 * apply_typmod_special() - verbatim on the SHIM T1 explicit sign; the
 * ereturn (escontext = NULL in numeric()) -> *err per SHIM T3.  Returns
 * the C bool.
 */
int
pg_apply_typmod_special(int sign, int32 typmod, int *err)
{
	*err = 0;

	Assert(NUMERIC_IS_SPECIAL_S(sign));

	/* Simply allow NaN to pass through */
	if (NUMERIC_IS_NAN_S(sign))
		return 1;

	/* Assert(NUMERIC_IS_PINF(num) || NUMERIC_IS_NINF(num)); */

	/* Infinity allowed if not constraining precision */
	if (!is_valid_numeric_typmod(typmod))
		return 1;

	*err = 1;					/* ereport: numeric field overflow (cannot
								 * hold an infinite value) */
	return 0;
}
