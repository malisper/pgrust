/*
 * Vendored PostgreSQL C: pg_lsn family + minimal numeric support —
 * differential-fuzz oracle for the pg_lsn_diff target.
 *
 * Provenance (all bodies VERBATIM unless a shim is listed below), from the
 * repo's vendored ground-truth checkout ../pgrust-fabled/vendor/postgres-src
 * @ 62d6c7d "Stamp 18.3." (PostgreSQL 18.3 exactly — the campaign oracle
 * pin):
 *   - src/backend/utils/adt/pg_lsn.c: pg_lsn_in_internal, pg_lsn_in,
 *     pg_lsn_out, pg_lsn_eq/ne/lt/gt/le/ge, pg_lsn_larger, pg_lsn_smaller,
 *     pg_lsn_cmp, pg_lsn_mi, pg_lsn_pli, pg_lsn_mii — verbatim (18.3
 *     formats "%X/%X"; the PG19-devel "%X/%08X" drift is characterized in
 *     proofs/pg_lsn, out of scope here).  pg_lsn_recv / pg_lsn_send are
 *     bare pq_getmsgint64 / pq_sendint64 calls; the Rust driver constructs
 *     their wire contract directly (see lsn_diff.rs), so they are not
 *     vendored.
 *   - src/backend/utils/adt/numeric.c: NumericData/NumericChoice structs +
 *     header macros, NumericVar, const_nan/const_pinf/const_ninf,
 *     alloc_var, free_var, zero_var, init_var, init_var_from_num,
 *     set_var_from_var, set_var_from_str, strip_var, round_var, cmp_abs,
 *     cmp_abs_common, add_abs, sub_abs, add_var, sub_var,
 *     make_result_opt_error, make_result, apply_typmod,
 *     apply_typmod_special, is_valid_numeric_typmod,
 *     numeric_typmod_precision, numeric_typmod_scale, numericvar_to_uint64,
 *     numeric_in (core), numeric_is_nan, numeric_add_opt_error,
 *     numeric_sub_opt_error, numeric_pg_lsn — verbatim.
 *   - src/port/pgstrcasecmp.c: pg_strncasecmp — verbatim (same vendoring as
 *     pg_float_io.c; C-locale semantics on this host for high-bit bytes).
 *   - src/include/common/int.h: pg_mul_u64_overflow / pg_add_u64_overflow —
 *     verbatim (__builtin_*_overflow forms).
 *
 * SHIMS (plumbing only, never logic; numbered, each marked at its site):
 *   S1. fmgr unwrapping: PG_FUNCTION_ARGS / PG_GETARG_* / PG_RETURN_* /
 *       DirectFunctionCall* -> plain C signatures + direct calls. Bodies
 *       kept verbatim inside the plain-signature functions.
 *   S2. ereport/ereturn -> record the errcode class in a thread-local and
 *       return; errmsg/errdetail evaluate to 0 with arguments unevaluated
 *       (message text out of the comparison planes). escontext is always
 *       the hard-error (NULL) shape on the C side; the Rust soft path is
 *       compared against the same errcode class.
 *   S3. palloc/pfree -> thread-local bump arena, reset at every exported
 *       oracle entry (PostgreSQL's error paths rely on memory-context
 *       reset, so a plain malloc shim would leak per rejected input across
 *       millions of libFuzzer execs). pfree/free-of-arena-memory -> no-op;
 *       arena overflow -> abort() (loud, never silent; driver caps input
 *       sizes far below the arena). elog(ERROR) in make_result_opt_error's
 *       invalid-sign arm -> abort() (unreachable: every var sign here
 *       comes from the vendored constructors).
 *   S4. Varlena header: SET_VARSIZE/VARSIZE transcribed for a 4-byte
 *       little-endian header (va_header = len << 2), the layout of this
 *       host and of the shipped Rust NumericImage. Image comparisons are
 *       full-varlena byte comparisons (header included).
 *   S5. NON-DECIMAL CARVE: numeric_in's 0x/0o/0b branch
 *       (set_var_from_non_decimal_integer_str + mul_var) is NOT vendored;
 *       the oracle records PG_LSNFUZZ_CARVE_NONDECIMAL and the driver
 *       skips such inputs (they never arise from pg_lsn's own u64/i128
 *       decimal rendering; non-decimal numeric literals belong to the
 *       numeric family's own lanes). The driver's skip predicate is the
 *       exact C predicate (cp[0]=='0' && cp[1] in [xXoObB] after
 *       space/sign skip).
 *   S6. Assert -> no-op (release parity); dump_numeric/dump_var -> no-op.
 *   S7. UINT64_FORMAT -> PRIu64 (inttypes.h), the configure-time value of
 *       the same macro on this host.
 *
 * Numeric text inputs are capped by the DRIVER at 256 bytes, so the
 * fixed result buffers below (4 KiB) cannot overflow: ndigits <=
 * (256 digits + pad)/4 + 1 and every image is <= NUMERIC_HDRSZ +
 * 2*ndigits bytes; pg_lsn_mi/u64 renderings are <= 20 digits.
 */

#include <ctype.h>
#include <inttypes.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef int16_t int16;
typedef int32_t int32;
typedef int64_t int64;
typedef uint8_t uint8;
typedef uint16_t uint16;
typedef uint32_t uint32;
typedef uint64_t uint64;
typedef size_t Size;
typedef uint64 XLogRecPtr;
typedef int16 NumericDigit;

#define InvalidXLogRecPtr ((XLogRecPtr) 0)
#define LSN_FORMAT_ARGS(lsn) ((uint32) ((lsn) >> 32)), ((uint32) (lsn))
#define PG_INT16_MAX INT16_MAX
#define PG_INT32_MAX INT32_MAX
#define Assert(x) ((void) 0) /* S6 */
#define Max(x, y) ((x) > (y) ? (x) : (y))
#define Min(x, y) ((x) < (y) ? (x) : (y))
#define unlikely(x) (x)
#define FLEXIBLE_ARRAY_MEMBER /* empty */
#define VARHDRSZ ((int32) sizeof(int32))

struct Node; /* opaque; escontext is always NULL here (S2) */
typedef struct Node Node;

/* ---- S2: errcode capture ---- */

/* classes shared with the Rust driver (lsn_diff.rs) */
#define ERRCODE_INVALID_TEXT_REPRESENTATION 1 /* 22P02 */
#define ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE 2  /* 22003 */
#define ERRCODE_FEATURE_NOT_SUPPORTED 3       /* 0A000 */
#define ERRCODE_INVALID_PARAMETER_VALUE 4     /* 22023 */
#define PG_LSNFUZZ_CARVE_NONDECIMAL 98        /* S5 sentinel, never a verdict */

_Thread_local int pg_lsnfuzz_errcode;

int
pg_lsnfuzz_errcode_get(void)
{
	return pg_lsnfuzz_errcode;
}

#define errcode(c) (pg_lsnfuzz_errcode = (c))
#define errmsg(...) 0
#define errdetail(...) 0
#define ereport(level, stuff) do { (void) (stuff); } while (0)
#define ereturn(escontext, ret, stuff) do { (void) (stuff); return (ret); } while (0)

/* ---- S3: per-entry bump arena (memory-context reset stand-in) ---- */

/*
 * Sized for the int16-weight-bounded worst case: a NumericVar can carry up
 * to ~NUMERIC_WEIGHT_MAX (32767) NBASE digits through add/sub (~64 KiB per
 * digit buffer), and one entry allocates several such buffers plus the
 * packed result image. 2 MiB gives ~16x headroom; overflow still aborts
 * loudly. (First smoke run found the old 64 KiB cap via "1844...e101615".)
 */
#define PG_LSNFUZZ_ARENA_CAP (2u << 20)
static _Thread_local unsigned char pg_lsnfuzz_arena[PG_LSNFUZZ_ARENA_CAP];
static _Thread_local size_t pg_lsnfuzz_arena_used;

static void
pg_arena_reset(void)
{
	pg_lsnfuzz_arena_used = 0;
}

static void *
pg_arena_alloc(size_t n)
{
	size_t		aligned = (n + 15) & ~(size_t) 15;
	void	   *p;

	if (aligned > PG_LSNFUZZ_ARENA_CAP - pg_lsnfuzz_arena_used)
		abort();				/* loud overflow; see S3 */
	p = pg_lsnfuzz_arena + pg_lsnfuzz_arena_used;
	pg_lsnfuzz_arena_used += aligned;
	return p;
}

#define palloc(n) pg_arena_alloc(n) /* S3 */
#define pfree(p) ((void) (p))       /* S3 */

/* ---- src/port/pgstrcasecmp.c: pg_strncasecmp — VERBATIM ---- */

#define IS_HIGHBIT_SET(ch) ((unsigned char) (ch) & 0x80)

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

/* ---- src/include/common/int.h: u64 overflow helpers — VERBATIM ---- */

static inline bool
pg_add_u64_overflow(uint64 a, uint64 b, uint64 *result)
{
	return __builtin_add_overflow(a, b, result);
}

static inline bool
pg_mul_u64_overflow(uint64 a, uint64 b, uint64 *result)
{
	return __builtin_mul_overflow(a, b, result);
}

/* ---- numeric.c: on-disk struct + header macros — VERBATIM ---- */

#define NBASE		10000
#define HALF_NBASE	5000
#define DEC_DIGITS	4			/* decimal digits per NBASE digit */

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

typedef struct NumericData *Numeric;

/* S4: 4-byte little-endian varlena header, this host's + pgrust's layout */
#define SET_VARSIZE(n, l) (((struct NumericData *) (n))->vl_len_ = (int32) ((uint32) (l) << 2))
#define VARSIZE(n) ((uint32) ((struct NumericData *) (n))->vl_len_ >> 2)

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

#define NUMERIC_HEADER_IS_SHORT(n)	(((n)->choice.n_header & 0x8000) != 0)
#define NUMERIC_HEADER_SIZE(n) \
	(VARHDRSZ + sizeof(uint16) + \
	 (NUMERIC_HEADER_IS_SHORT(n) ? 0 : sizeof(int16)))

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

#define NUMERIC_WEIGHT_MAX			PG_INT16_MAX

typedef struct NumericVar
{
	int			ndigits;		/* # of digits in digits[] - can be 0! */
	int			weight;			/* weight of first digit */
	int			sign;			/* NUMERIC_POS, _NEG, _NAN, _PINF, or _NINF */
	int			dscale;			/* display scale */
	NumericDigit *buf;			/* start of palloc'd space for digits[] */
	NumericDigit *digits;		/* base-NBASE digits */
} NumericVar;

static const NumericVar const_nan =
{0, 0, NUMERIC_NAN, 0, NULL, NULL};

static const NumericVar const_pinf =
{0, 0, NUMERIC_PINF, 0, NULL, NULL};

static const NumericVar const_ninf =
{0, 0, NUMERIC_NINF, 0, NULL, NULL};

static const int round_powers[4] = {0, 1000, 100, 10};

#define dump_numeric(s, n) /* S6 */
#define dump_var(s, v)     /* S6 */

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

/* ---- numeric.c: typmod helpers — VERBATIM ---- */

static inline bool
is_valid_numeric_typmod(int32 typmod)
{
	return typmod >= (int32) VARHDRSZ;
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

/* ---- numeric.c: variable management — VERBATIM ---- */

static void
alloc_var(NumericVar *var, int ndigits)
{
	digitbuf_free(var->buf);
	var->buf = digitbuf_alloc(ndigits + 1);
	var->buf[0] = 0;			/* spare digit for rounding */
	var->digits = var->buf + 1;
	var->ndigits = ndigits;
}

static void
free_var(NumericVar *var)
{
	digitbuf_free(var->buf);
	var->buf = NULL;
	var->digits = NULL;
	var->sign = NUMERIC_NAN;
}

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

/* ---- numeric.c: strip_var / round_var — VERBATIM ---- */

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

/* ---- numeric.c: cmp_abs / add_abs / sub_abs — VERBATIM ---- */

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

/* ---- numeric.c: add_var / sub_var — VERBATIM ---- */

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

/* ---- numeric.c: set_var_from_str — VERBATIM ---- */

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
		*digits++ = ((decdigits[i] * 10 + decdigits[i + 1]) * 10 +
					 decdigits[i + 2]) * 10 + decdigits[i + 3];
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

/* ---- numeric.c: make_result_opt_error / make_result — VERBATIM ---- */

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
			abort();			/* S3: elog(ERROR) unreachable here */

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
			/* S2: hard ereport path records the class and returns NULL */
			(void) errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
			pfree(result);
			return NULL;
		}
	}

	dump_numeric("make_result()", result);
	return result;
}

static Numeric
make_result(const NumericVar *var)
{
	return make_result_opt_error(var, NULL);
}

/* ---- numeric.c: apply_typmod / apply_typmod_special — VERBATIM ---- */

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
				if (dig < 10)
					ddigits -= 3;
				else if (dig < 100)
					ddigits -= 2;
				else if (dig < 1000)
					ddigits -= 1;
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

/* ---- numeric.c: numericvar_to_uint64 — VERBATIM ---- */

static bool
numericvar_to_uint64(const NumericVar *var, uint64 *result)
{
	NumericDigit *digits;
	int			ndigits;
	int			weight;
	int			i;
	uint64		val;
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

	/* Check for negative input */
	if (rounded.sign == NUMERIC_NEG)
	{
		free_var(&rounded);
		return false;
	}

	/*
	 * For input like 10000000000, we must treat stripped digits as real. So
	 * the loop assumes there are weight+1 digits before the decimal point.
	 */
	weight = rounded.weight;
	Assert(weight >= 0 && ndigits <= weight + 1);

	/* Construct the result */
	digits = rounded.digits;
	val = digits[0];
	for (i = 1; i <= weight; i++)
	{
		if (unlikely(pg_mul_u64_overflow(val, NBASE, &val)))
		{
			free_var(&rounded);
			return false;
		}

		if (i < ndigits)
		{
			if (unlikely(pg_add_u64_overflow(val, digits[i], &val)))
			{
				free_var(&rounded);
				return false;
			}
		}
	}

	free_var(&rounded);

	*result = val;

	return true;
}

/* ---- numeric.c: numeric_is_nan — VERBATIM (S1 plain signature) ---- */

static bool
numeric_is_nan(Numeric num)
{
	return NUMERIC_IS_NAN(num);
}

/*
 * ---- numeric.c: numeric_in — body VERBATIM under S1/S2/S5 ----
 *
 * Plain-signature core: parses `str` with display typmod `typmod` (always
 * -1 at the pg_lsn call sites, matching Int32GetDatum(-1)); on success
 * returns the palloc'd packed Numeric, on error returns NULL with the
 * errcode class recorded. S5: the non-decimal branch records the carve
 * sentinel instead of parsing.
 */
static Numeric
pg_oracle_numeric_in(const char *str, int32 typmod)
{
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
			{
				pfree(res);
				goto invalid_syntax;
			}
			cp++;
		}

		if (!apply_typmod_special(res, typmod, NULL))
		{
			pfree(res);
			return NULL;
		}
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
			if (!set_var_from_str(str, cp, &value, &cp, NULL))
				return NULL;
			value.sign = sign;
		}
		else
		{
			/* S5: non-decimal path carved out of oracle scope */
			(void) errcode(PG_LSNFUZZ_CARVE_NONDECIMAL);
			return NULL;
		}

		/*
		 * Should be nothing left but spaces. As above, throw any typmod error
		 * after finishing syntax check.
		 */
		while (*cp)
		{
			if (!isspace((unsigned char) *cp))
			{
				free_var(&value);
				goto invalid_syntax;
			}
			cp++;
		}

		if (!apply_typmod(&value, typmod, NULL))
		{
			free_var(&value);
			return NULL;
		}

		res = make_result_opt_error(&value, &have_error);

		if (have_error)
		{
			free_var(&value);
			ereturn(NULL, NULL,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					 errmsg("value overflows numeric format")));
		}

		free_var(&value);
	}

	return res;

invalid_syntax:
	ereturn(NULL, NULL,
			(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
			 errmsg("invalid input syntax for type %s: \"%s\"",
					"numeric", str)));
}

/* ---- numeric.c: numeric_add/sub_opt_error — VERBATIM ---- */

static Numeric
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

static Numeric
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

/* ---- numeric.c: numeric_pg_lsn — body VERBATIM under S1/S2 ---- */

static int /* 0 ok, else errclass */
pg_oracle_numeric_pg_lsn(Numeric num, XLogRecPtr *out)
{
	NumericVar	x;
	XLogRecPtr	result;

	if (NUMERIC_IS_SPECIAL(num))
	{
		if (NUMERIC_IS_NAN(num))
			return errcode(ERRCODE_FEATURE_NOT_SUPPORTED);
		else
			return errcode(ERRCODE_FEATURE_NOT_SUPPORTED);
	}

	/* Convert to variable format and thence to pg_lsn */
	init_var_from_num(num, &x);

	if (!numericvar_to_uint64(&x, (uint64 *) &result))
		return errcode(ERRCODE_INVALID_PARAMETER_VALUE);

	*out = result;
	return 0;
}

/* ---- pg_lsn.c — VERBATIM under S1/S2 ---- */

#define MAXPG_LSNLEN			17
#define MAXPG_LSNCOMPONENT	8

static XLogRecPtr
pg_lsn_in_internal(const char *str, bool *have_error)
{
	int			len1,
				len2;
	uint32		id,
				off;
	XLogRecPtr	result;

	Assert(have_error != NULL);
	*have_error = false;

	/* Sanity check input format. */
	len1 = strspn(str, "0123456789abcdefABCDEF");
	if (len1 < 1 || len1 > MAXPG_LSNCOMPONENT || str[len1] != '/')
	{
		*have_error = true;
		return InvalidXLogRecPtr;
	}
	len2 = strspn(str + len1 + 1, "0123456789abcdefABCDEF");
	if (len2 < 1 || len2 > MAXPG_LSNCOMPONENT || str[len1 + 1 + len2] != '\0')
	{
		*have_error = true;
		return InvalidXLogRecPtr;
	}

	/* Decode result. */
	id = (uint32) strtoul(str, NULL, 16);
	off = (uint32) strtoul(str + len1 + 1, NULL, 16);
	result = ((uint64) id << 32) | off;

	return result;
}

/* ==== exported oracle entry points (S1 wrappers; bodies above) ==== */

/* pg_lsn_in: returns 0 ok / errclass; *out valid on 0 */
int
pg_lsnfuzz_in(const char *str, uint64 *out)
{
	XLogRecPtr	result;
	bool		have_error = false;

	pg_arena_reset();
	pg_lsnfuzz_errcode = 0;
	result = pg_lsn_in_internal(str, &have_error);
	if (have_error)
		return errcode(ERRCODE_INVALID_TEXT_REPRESENTATION);

	*out = result;
	return 0;
}

/* pg_lsn_out: snprintf image (verbatim format string); returns length */
int
pg_lsnfuzz_out(uint64 lsn, char *buf /* >= MAXPG_LSNLEN+1 */)
{
	pg_arena_reset();
	pg_lsnfuzz_errcode = 0;
	snprintf(buf, MAXPG_LSNLEN + 1, "%X/%X", LSN_FORMAT_ARGS(lsn));
	return (int) strlen(buf);
}

/*
 * comparison / minmax family: op selects the verbatim expression
 * (0 eq, 1 ne, 2 lt, 3 gt, 4 le, 5 ge, 6 cmp, 7 larger, 8 smaller).
 * Returns bool as 0/1, cmp as -1/0/1, larger/smaller writes *out64.
 */
int64_t
pg_lsnfuzz_cmp(int op, uint64 lsn1, uint64 lsn2, uint64 *out64)
{
	pg_arena_reset();
	pg_lsnfuzz_errcode = 0;
	switch (op)
	{
		case 0:
			return lsn1 == lsn2;
		case 1:
			return lsn1 != lsn2;
		case 2:
			return lsn1 < lsn2;
		case 3:
			return lsn1 > lsn2;
		case 4:
			return lsn1 <= lsn2;
		case 5:
			return lsn1 >= lsn2;
		case 6:
			if (lsn1 > lsn2)
				return 1;
			else if (lsn1 == lsn2)
				return 0;
			else
				return -1;
		case 7:
			*out64 = (lsn1 > lsn2) ? lsn1 : lsn2;
			return 0;
		case 8:
			*out64 = (lsn1 < lsn2) ? lsn1 : lsn2;
			return 0;
		default:
			abort();
	}
}

/* copy a packed Numeric into the caller's image buffer; returns byte len */
static int
copy_image(Numeric n, unsigned char *img_out, int img_cap)
{
	int			len = (int) VARSIZE(n);

	if (len > img_cap)
		abort();				/* driver caps inputs; see header comment */
	memcpy(img_out, n, len);
	return len;
}

/*
 * numeric_in oracle (typmod -1): 0 ok (+image) / errclass /
 * PG_LSNFUZZ_CARVE_NONDECIMAL.
 */
int
pg_lsnfuzz_numeric_in(const char *str, unsigned char *img_out, int img_cap,
					  int *img_len)
{
	Numeric		n;

	pg_arena_reset();
	pg_lsnfuzz_errcode = 0;
	n = pg_oracle_numeric_in(str, -1);
	if (n == NULL)
		return pg_lsnfuzz_errcode;
	*img_len = copy_image(n, img_out, img_cap);
	pfree(n);
	return 0;
}

/*
 * pg_lsn_mi — body VERBATIM (S1: DirectFunctionCall3(numeric_in, ...) ->
 * pg_oracle_numeric_in(buf, -1)); result image copied out.
 */
int
pg_lsnfuzz_mi(uint64 lsn1, uint64 lsn2, unsigned char *img_out, int img_cap,
			  int *img_len)
{
	char		buf[256];
	Numeric		result;

	pg_arena_reset();
	pg_lsnfuzz_errcode = 0;

	/* Output could be as large as plus or minus 2^63 - 1. */
	if (lsn1 < lsn2)
		snprintf(buf, sizeof buf, "-%" PRIu64, lsn2 - lsn1); /* S7 */
	else
		snprintf(buf, sizeof buf, "%" PRIu64, lsn1 - lsn2); /* S7 */

	/* Convert to numeric. */
	result = pg_oracle_numeric_in(buf, -1);
	if (result == NULL)
		return pg_lsnfuzz_errcode; /* unreachable: decimal digits only */

	*img_len = copy_image(result, img_out, img_cap);
	pfree(result);
	return 0;
}

/*
 * pg_lsn_pli / pg_lsn_mii — bodies VERBATIM (S1: DirectFunctionCall* ->
 * direct calls; nbytes arrives as numeric text and is packed via the same
 * numeric_in the SQL literal took). sub != 0 selects mii.
 * Returns 0 ok / errclass / carve sentinel.
 */
int
pg_lsnfuzz_plimii(uint64 lsn, const char *nbytes_text, int sub, uint64 *out)
{
	Numeric		nbytes;
	Numeric		num;
	Numeric		res;
	bool		have_error;
	char		buf[32];
	int			rc;

	pg_arena_reset();
	pg_lsnfuzz_errcode = 0;
	nbytes = pg_oracle_numeric_in(nbytes_text, -1);
	if (nbytes == NULL)
		return pg_lsnfuzz_errcode;

	if (numeric_is_nan(nbytes))
	{
		pfree(nbytes);
		return errcode(ERRCODE_FEATURE_NOT_SUPPORTED);
	}

	/* Convert to numeric */
	snprintf(buf, sizeof(buf), "%" PRIu64, lsn); /* S7 */
	num = pg_oracle_numeric_in(buf, -1);
	if (num == NULL)
		abort();				/* decimal digits only */

	/* Add / subtract two numerics */
	if (sub)
		res = numeric_sub_opt_error(num, nbytes, &have_error);
	else
		res = numeric_add_opt_error(num, nbytes, &have_error);
	pfree(num);
	pfree(nbytes);
	if (res == NULL)
		return errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);

	/* Convert to pg_lsn */
	rc = pg_oracle_numeric_pg_lsn(res, out);
	pfree(res);
	return rc;
}

/* numeric_pg_lsn oracle over numeric text (same packing path) */
int
pg_lsnfuzz_numeric_pg_lsn(const char *num_text, uint64 *out)
{
	Numeric		n;
	int			rc;

	pg_arena_reset();
	pg_lsnfuzz_errcode = 0;
	n = pg_oracle_numeric_in(num_text, -1);
	if (n == NULL)
		return pg_lsnfuzz_errcode;
	rc = pg_oracle_numeric_pg_lsn(n, out);
	pfree(n);
	return rc;
}
