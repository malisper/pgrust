/*
 * Vendored PostgreSQL C for the jsonb SCALAR-CAST rows (jsonb-probe wave 2):
 * oids 3449 jsonb_numeric, 3450 jsonb_int2, 3451 jsonb_int4, 3452
 * jsonb_int8, 2580 jsonb_float8, 3453 jsonb_float4.
 *
 * Kept in its OWN translation unit (linked together with c/pg_jsonb.c,
 * which owns the container/iterator machinery) so the numeric machinery
 * below does not bloat the goto programs of the established lookup/cmp
 * harnesses (C-shim-hygiene rule, proofs/TRIAGE.md).
 *
 * Provenance (all REL_18_STABLE, fetched 2026-07-30 from
 * https://raw.githubusercontent.com/postgres/postgres/REL_18_STABLE/):
 *   - src/backend/utils/adt/jsonb.c: jsonb_numeric, jsonb_int2, jsonb_int4,
 *     jsonb_int8, jsonb_float8, jsonb_float4, cannotCastJsonbValue (fmgr
 *     bodies extracted per the shim rules below)
 *   - src/backend/utils/adt/numeric.c: numericvar_to_int64,
 *     numericvar_to_int32, round_var, strip_var, alloc_var, init_var,
 *     set_var_from_var, init_var_from_num, free_var, the numeric_int2 /
 *     numeric_int4_opt_error / numeric_int8_opt_error bodies, and the
 *     NUMERIC_IS_SPECIAL arms of numeric_float8 / numeric_float4; the
 *     NumericData/NumericShort/NumericLong structs and NUMERIC_* accessor
 *     macros, verbatim.
 *   - src/include/common/int.h: pg_mul_s64_overflow / pg_sub_s64_overflow
 *     (builtin form — the target toolchain has
 *     __builtin_mul_overflow/__builtin_sub_overflow, and CBMC models them).
 *
 * SHIMS (everything else verbatim; each marked at its site):
 *  C1. fmgr unwrapping: PG_GETARG_JSONB_P(0) -> `JsonbContainer *c` param
 *      (pre-detoasted payload fence, family convention); PG_RETURN_* ->
 *      out-params; PG_FREE_IF_COPY -> no-op.
 *  C2. cannotCastJsonbValue / ereport(ERROR) -> error-CLASS out-param
 *      (extended PROOF_EREPORT_FLAG convention; distinct value per errcode
 *      so sqlstate parity is assertable):
 *        1 = ERRCODE_INVALID_PARAMETER_VALUE  (cannotCastJsonbValue 22023)
 *        2 = ERRCODE_FEATURE_NOT_SUPPORTED    (cannot convert NaN/inf 0A000)
 *        3 = ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE ("x out of range" 22003)
 *      cannotCastJsonbValue's lookup loop is kept verbatim; its ereport
 *      site records the class and the caller returns at the same program
 *      point C's longjmp would leave (message text out of proof; the
 *      harness also asserts C's jbvType against the Rust item kind).
 *  C3. DatumGetNumericCopy (jsonb_numeric) -> identity on the in-body
 *      image window: the copy is result-materialization plumbing; the
 *      CLAIM is numeric image SLICE identity (offset + length), so the
 *      entry returns the window (start pointer + VARSIZE length) instead.
 *  C4. palloc for set_var_from_var's digit buffer -> static digit arena,
 *      rewound by pgp_casts_reset (allocation strategy out of scope;
 *      overflow = CBMC OOB violation). free_var/digitbuf_free -> no-op.
 *  C5. VARSIZE/NUMERIC_HEADER access on the in-image numeric: the image
 *      carries a standard 4-byte varlena header (the only form the jsonb
 *      writer stores: convertJsonbScalar copies make_result output).
 *      VARSIZE spelled as the little-endian len<<2 word decode
 *      (varattrib_4b, target convention; numeric-probe precedent T4) over
 *      the byte image, since the harness buffer is a u8 object.
 *  C6. float results returned as IEEE-754 BITS (uint64/uint32 out-params;
 *      union bit-pun at the return boundary only — bodies verbatim).
 *      get_float8_nan()/get_float4_nan() -> canonical quiet NaN
 *      0x7ff8000000000000 / 0x7fc00000 constants (MANDATORY NAN shim,
 *      CBMC non-canonical NAN model defect, see
 *      proofs/geo-cmp/CBMC-NAN-BUG-REPORT.md); get_float8_infinity() ->
 *      0x7ff0000000000000 bits. The FINITE arm of numeric_float8/4
 *      (numeric_out + strtod cascade) is OUT OF FENCE and sets the abort
 *      sentinel — reaching it is a harness bug, matching the fenced claim
 *      "extraction + cast-error class + NaN/±Inf lattice".
 *  Types: the jsonb.h declaration block is byte-identical to the one in
 *  c/pg_jsonb.c (same layout across the two TUs; Numeric stays `void *`
 *  in JsonbValue and is cast to the NumericData view locally).
 */

#include <stddef.h>
#include <string.h>
#include "../../support/c/pg_proof_shim.h"

/* ---------------- jsonb.h declarations (verbatim; same as pg_jsonb.c) --- */

typedef uint32 JEntry;

typedef struct JsonbContainer
{
	uint32		header;
	JEntry		children[];		/* FLEXIBLE_ARRAY_MEMBER */
} JsonbContainer;

#define JB_CMASK				0x0FFFFFFF
#define JB_FSCALAR				0x10000000
#define JB_FOBJECT				0x20000000
#define JB_FARRAY				0x40000000

enum jbvType
{
	jbvNull = 0x0,
	jbvString,
	jbvNumeric,
	jbvBool,
	jbvArray = 0x10,
	jbvObject,
	jbvBinary,
	jbvDatetime = 0x20,
};

typedef void *Numeric_opaque;	/* the JsonbValue field type (as in pg_jsonb.c) */
typedef uintptr_t Datum;

typedef struct JsonbValue JsonbValue;

struct JsonbValue
{
	enum jbvType type;
	union
	{
		Numeric_opaque numeric;
		bool		boolean;
		struct
		{
			int			len;
			char	   *val;
		}			string;
		struct
		{
			int			nElems;
			JsonbValue *elems;
			bool		rawScalar;
		}			array;
		struct
		{
			int			nPairs;
			void	   *pairs;
		}			object;
		struct
		{
			int			len;
			JsonbContainer *data;
		}			binary;
		struct
		{
			Datum		value;
			Oid			typid;
			int32		typmod;
			int			tz;
		}			datetime;
	}			val;
};

/* provided by pg_jsonb.c (same declarations there) */
extern bool pg_JsonbExtractScalar(JsonbContainer *jbc, JsonbValue *res);

/* ---------------- numeric.c representation (verbatim) ---------------- */

#define NBASE		10000
#define HALF_NBASE	5000
#define DEC_DIGITS	4

typedef int16 NumericDigit;

struct NumericShort
{
	uint16		n_header;
	NumericDigit n_data[];
};

struct NumericLong
{
	uint16		n_sign_dscale;
	int16		n_weight;
	NumericDigit n_data[];
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

#define NUMERIC_FLAGBITS(n) ((n)->choice.n_header & NUMERIC_SIGN_MASK)
#define NUMERIC_IS_SHORT(n)		(NUMERIC_FLAGBITS(n) == NUMERIC_SHORT)
#define NUMERIC_IS_SPECIAL(n)	(NUMERIC_FLAGBITS(n) == NUMERIC_SPECIAL)

#define NUMERIC_EXT_SIGN_MASK	0xF000
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

#define NUMERIC_HDRSZ	(VARHDRSZ + sizeof(uint16) + sizeof(int16))
#define NUMERIC_HDRSZ_SHORT (VARHDRSZ + sizeof(uint16))

#define NUMERIC_HEADER_IS_SHORT(n)	(((n)->choice.n_header & 0x8000) != 0)
#define NUMERIC_HEADER_SIZE(n) \
	(VARHDRSZ + sizeof(uint16) + \
	 (NUMERIC_HEADER_IS_SHORT(n) ? 0 : sizeof(int16)))

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

/* SHIM C5: VARSIZE decode on the byte-image varlena header */
#define CASTS_VARSIZE(n)	(((uint32) *(uint32 *) (n)) >> 2)

/* verbatim accessor macros (VARSIZE via SHIM C5) */
#define NUMERIC_NDIGITS(num) \
	((CASTS_VARSIZE(num) - NUMERIC_HEADER_SIZE(num)) / sizeof(NumericDigit))
#define NUMERIC_DIGITS(num) (NUMERIC_HEADER_IS_SHORT(num) ? \
	(num)->choice.n_short.n_data : (num)->choice.n_long.n_data)
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

/* common/int.h (builtin form; CBMC models the builtins) */
static inline bool
pg_mul_s64_overflow(int64 a, int64 b, int64 *result)
{
	return __builtin_mul_overflow(a, b, result);
}

static inline bool
pg_sub_s64_overflow(int64 a, int64 b, int64 *result)
{
	return __builtin_sub_overflow(a, b, result);
}

#define PG_INT64_MIN	(-0x7FFFFFFFFFFFFFFFL - 1)
#define PG_INT16_MIN	(-0x7FFF-1)
#define PG_INT16_MAX	(0x7FFF)
#define PG_INT32_MIN	(-0x7FFFFFFF-1)
#define PG_INT32_MAX	(0x7FFFFFFF)

/* ---------------- NumericVar + digit arena (SHIM C4) ---------------- */

typedef struct NumericVar
{
	int			ndigits;
	int			weight;
	int			sign;
	int			dscale;
	NumericDigit *buf;
	NumericDigit *digits;
} NumericVar;

#define init_var(v)		memset(v, 0, sizeof(NumericVar))

#define CASTS_DIGIT_ARENA 32
static NumericDigit casts_digit_arena[CASTS_DIGIT_ARENA];
static int	casts_digit_next = 0;
static int	casts_abort = 0;	/* out-of-fence sentinel (own TU copy) */

int
pgp_casts_reset(void)
{
	casts_digit_next = 0;
	casts_abort = 0;
	return 0;
}

int
pgp_casts_take_abort(void)
{
	int			a = casts_abort;

	casts_abort = 0;
	return a;
}

static NumericDigit *
casts_digitbuf_alloc(int ndigits)
{
	NumericDigit *p = &casts_digit_arena[casts_digit_next];

	casts_digit_next += ndigits;
	return p;					/* arena overflow = CBMC OOB violation */
}

#define digitbuf_free(buf) ((void) 0)
#define free_var(v) ((void) 0)

/* numeric.c alloc_var, verbatim body (digitbuf_alloc -> arena) */
static void
alloc_var(NumericVar *var, int ndigits)
{
	var->buf = casts_digitbuf_alloc(ndigits + 1);
	var->buf[0] = 0;			/* spare digit for rounding */
	var->digits = var->buf + 1;
	var->ndigits = ndigits;
}

/* numeric.c set_var_from_var, verbatim body */
static void
set_var_from_var(const NumericVar *value, NumericVar *dest)
{
	NumericDigit *newbuf;

	newbuf = casts_digitbuf_alloc(value->ndigits + 1);
	newbuf[0] = 0;				/* spare digit for rounding */
	if (value->ndigits > 0)		/* else value->digits might be null */
	{
		/* SHIM C7 (typed staging, family law "byte-punned cross-language
		 * reads need typed staging"): upstream memcpy; CBMC's memcpy
		 * builtin mis-models the byte-punned int16 copy out of the u8
		 * harness image (measured: symbolic digits arrived corrupted while
		 * a direct x.digits[0] read decodes correctly), so the copy is
		 * spelled as the equivalent per-digit assignment loop. */
		int			i_;

		for (i_ = 0; i_ < value->ndigits; i_++)
			newbuf[1 + i_] = value->digits[i_];
	}

	digitbuf_free(dest->buf);

	memmove(dest, value, sizeof(NumericVar));
	dest->buf = newbuf;
	dest->digits = newbuf + 1;
}

/* numeric.c init_var_from_num, verbatim body */
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

/* numeric.c round_var, verbatim body (DEC_DIGITS == 4 arms kept) */
static const int round_powers[4] = {0, 1000, 100, 10};

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

			if (di == 0)
				carry = (digits[ndigits] >= HALF_NBASE) ? 1 : 0;
			else
			{
				/* Must round within last NBASE digit */
				int			extra,
							pow10;

				pow10 = round_powers[di];
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

/* numeric.c strip_var, verbatim body */
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

/* numeric.c numericvar_to_int64, verbatim body */
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

/* numeric.c numericvar_to_int32, verbatim body */
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

/* ---------------- jsonb.c cast bodies ---------------- */

/*
 * cannotCastJsonbValue, verbatim lookup loop; the ereport site records
 * error class 1 (ERRCODE_INVALID_PARAMETER_VALUE) and the jbvType, and the
 * caller returns at the same program point (SHIM C2).
 */
struct casts_msg
{
	enum jbvType type;
	int			msg;			/* message identity out of proof; slot kept
								 * so the lookup loop shape stays verbatim */
};

static void
cannotCastJsonbValue(enum jbvType type, int *errclass, int *errtype)
{
	static const struct casts_msg messages[] =
	{
		{jbvNull, 0},
		{jbvString, 1},
		{jbvNumeric, 2},
		{jbvBool, 3},
		{jbvArray, 4},
		{jbvObject, 5},
		{jbvBinary, 6}
	};
	int			i;

	for (i = 0; i < (int) lengthof(messages); i++)
		if (messages[i].type == type)
		{
			*errclass = 1;		/* ereport(ERROR, errcode(ERRCODE_INVALID_
								 * PARAMETER_VALUE), ...) */
			*errtype = (int) type;
			return;
		}

	/* should be unreachable */
	casts_abort = 1;			/* elog(ERROR, "unknown jsonb type") */
}

/*
 * jsonb_numeric body (SHIM C1/C2/C3). Return:
 *   0 = SQL NULL (jsonb null), 1 = numeric value (window out-params),
 *   2 = error (errclass/errtype set).
 */
int
pgp_jsonb_numeric(JsonbContainer *c, int *errclass, int *errtype,
				  const unsigned char **vdata, int *vlen)
{
	JsonbValue	v;
	Numeric		num;

	if (!pg_JsonbExtractScalar(c, &v))
	{
		cannotCastJsonbValue(v.type, errclass, errtype);
		return 2;
	}

	if (v.type == jbvNull)
		return 0;

	if (v.type != jbvNumeric)
	{
		cannotCastJsonbValue(v.type, errclass, errtype);
		return 2;
	}

	/* SHIM C3: DatumGetNumericCopy -> the in-body image window */
	num = (Numeric) v.val.numeric;
	*vdata = (const unsigned char *) num;
	*vlen = (int) CASTS_VARSIZE(num);

	return 1;
}

/*
 * Shared jsonb_int2/int4/int8 flow (bodies verbatim; the numeric_intN
 * DirectFunctionCall1 target is inlined at its call site, its ereports
 * recorded per SHIM C2). Return: 0 = SQL NULL, 1 = value, 2 = error.
 */
int
pgp_jsonb_int8(JsonbContainer *c, int *errclass, int *errtype, int64 *out)
{
	JsonbValue	v;
	Numeric		num;
	NumericVar	x;
	int64		result;

	if (!pg_JsonbExtractScalar(c, &v))
	{
		cannotCastJsonbValue(v.type, errclass, errtype);
		return 2;
	}
	if (v.type == jbvNull)
		return 0;
	if (v.type != jbvNumeric)
	{
		cannotCastJsonbValue(v.type, errclass, errtype);
		return 2;
	}

	/* numeric_int8_opt_error(num, NULL) body */
	num = (Numeric) v.val.numeric;
	if (NUMERIC_IS_SPECIAL(num))
	{
		*errclass = 2;			/* ERRCODE_FEATURE_NOT_SUPPORTED, "cannot
								 * convert NaN/infinity to bigint" */
		*errtype = NUMERIC_IS_NAN(num) ? 1 : 0;
		return 2;
	}
	init_var_from_num(num, &x);
	if (!numericvar_to_int64(&x, &result))
	{
		*errclass = 3;			/* ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
								 * "bigint out of range" */
		return 2;
	}
	*out = result;
	return 1;
}

int
pgp_jsonb_int4(JsonbContainer *c, int *errclass, int *errtype, int32 *out)
{
	JsonbValue	v;
	Numeric		num;
	NumericVar	x;
	int32		result;

	if (!pg_JsonbExtractScalar(c, &v))
	{
		cannotCastJsonbValue(v.type, errclass, errtype);
		return 2;
	}
	if (v.type == jbvNull)
		return 0;
	if (v.type != jbvNumeric)
	{
		cannotCastJsonbValue(v.type, errclass, errtype);
		return 2;
	}

	/* numeric_int4_opt_error(num, NULL) body */
	num = (Numeric) v.val.numeric;
	if (NUMERIC_IS_SPECIAL(num))
	{
		*errclass = 2;
		*errtype = NUMERIC_IS_NAN(num) ? 1 : 0;
		return 2;
	}
	init_var_from_num(num, &x);
	if (!numericvar_to_int32(&x, &result))
	{
		*errclass = 3;			/* "integer out of range" */
		return 2;
	}
	*out = result;
	return 1;
}

int
pgp_jsonb_int2(JsonbContainer *c, int *errclass, int *errtype, int16 *out)
{
	JsonbValue	v;
	Numeric		num;
	NumericVar	x;
	int64		val;
	int16		result;

	if (!pg_JsonbExtractScalar(c, &v))
	{
		cannotCastJsonbValue(v.type, errclass, errtype);
		return 2;
	}
	if (v.type == jbvNull)
		return 0;
	if (v.type != jbvNumeric)
	{
		cannotCastJsonbValue(v.type, errclass, errtype);
		return 2;
	}

	/* numeric_int2 body */
	num = (Numeric) v.val.numeric;
	if (NUMERIC_IS_SPECIAL(num))
	{
		*errclass = 2;
		*errtype = NUMERIC_IS_NAN(num) ? 1 : 0;
		return 2;
	}
	init_var_from_num(num, &x);
	if (!numericvar_to_int64(&x, &val))
	{
		*errclass = 3;			/* "smallint out of range" */
		return 2;
	}
	if (unlikely(val < PG_INT16_MIN) || unlikely(val > PG_INT16_MAX))
	{
		*errclass = 3;			/* "smallint out of range" */
		return 2;
	}
	/* Down-convert to int2 */
	result = (int16) val;
	*out = result;
	return 1;
}

/*
 * jsonb_float8 / jsonb_float4, SPECIAL arm only (SHIM C6): the finite arm
 * (numeric_out + float8in cascade) is out of fence and sets the abort
 * sentinel. Return: 0 = SQL NULL, 1 = special value (bits out), 2 = error.
 */
int
pgp_jsonb_float8_special(JsonbContainer *c, int *errclass, int *errtype,
						 uint64 *bits)
{
	JsonbValue	v;
	Numeric		num;

	if (!pg_JsonbExtractScalar(c, &v))
	{
		cannotCastJsonbValue(v.type, errclass, errtype);
		return 2;
	}
	if (v.type == jbvNull)
		return 0;
	if (v.type != jbvNumeric)
	{
		cannotCastJsonbValue(v.type, errclass, errtype);
		return 2;
	}

	/* numeric_float8 body, NUMERIC_IS_SPECIAL arm (verbatim structure;
	 * results as bits per SHIM C6) */
	num = (Numeric) v.val.numeric;
	if (NUMERIC_IS_SPECIAL(num))
	{
		if (NUMERIC_IS_PINF(num))
			*bits = 0x7ff0000000000000ULL;	/* get_float8_infinity() */
		else if (NUMERIC_IS_NINF(num))
			*bits = 0xfff0000000000000ULL;	/* -get_float8_infinity() */
		else
			*bits = 0x7ff8000000000000ULL;	/* get_float8_nan(), NAN shim */
		return 1;
	}

	casts_abort = 1;			/* finite arm out of fence */
	return 2;
}

int
pgp_jsonb_float4_special(JsonbContainer *c, int *errclass, int *errtype,
						 uint32 *bits)
{
	JsonbValue	v;
	Numeric		num;

	if (!pg_JsonbExtractScalar(c, &v))
	{
		cannotCastJsonbValue(v.type, errclass, errtype);
		return 2;
	}
	if (v.type == jbvNull)
		return 0;
	if (v.type != jbvNumeric)
	{
		cannotCastJsonbValue(v.type, errclass, errtype);
		return 2;
	}

	num = (Numeric) v.val.numeric;
	if (NUMERIC_IS_SPECIAL(num))
	{
		if (NUMERIC_IS_PINF(num))
			*bits = 0x7f800000U;	/* get_float4_infinity() */
		else if (NUMERIC_IS_NINF(num))
			*bits = 0xff800000U;	/* -get_float4_infinity() */
		else
			*bits = 0x7fc00000U;	/* get_float4_nan(), NAN shim */
		return 1;
	}

	casts_abort = 1;			/* finite arm out of fence */
	return 2;
}

/* Symbolic-decode diagnostic (harness plumbing, not a parity entry):
 * exposes the C-side view of the embedded numeric. */
int
pgp_probe_numeric_decode(JsonbContainer *c, int *sign, int *weight,
						 int *dsc, int *nd, int *d0)
{
	JsonbValue	v;
	Numeric		num;
	NumericVar	x;

	if (!pg_JsonbExtractScalar(c, &v) || v.type != jbvNumeric)
		return 0;
	num = (Numeric) v.val.numeric;
	if (NUMERIC_IS_SPECIAL(num))
		return 0;
	init_var_from_num(num, &x);
	*sign = x.sign;
	*weight = x.weight;
	*dsc = x.dscale;
	*nd = x.ndigits;
	*d0 = x.ndigits > 0 ? x.digits[0] : -1;
	return 1;
}
