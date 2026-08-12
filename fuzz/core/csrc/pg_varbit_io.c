/*
 * Vendored PostgreSQL C: bit / bit varying text-input parsers (bit_in,
 * varbit_in) — differential-fuzz oracle for adt/varbit (target
 * bits_in_diff / the VENDOR lane un-vendored bug-class surface).
 *
 * Provenance (bodies VERBATIM unless a shim is listed below), from the
 * repo's vendored ground-truth checkout
 * ../pgrust-reference/vendor/postgres-src @
 * 62d6c7d3df6287f1bd83199c1a746e50d31571a0 ("Stamp 18.3", REL_18):
 *   - src/backend/utils/adt/varbit.c 146..272 bit_in, 451..577 varbit_in —
 *     VERBATIM (the B/X/binary prefix scan, the bit-accumulate loop, the
 *     hex-accumulate loop, and every ereturn error site).
 *
 * WHY THIS TARGET (the campaign's un-vendored bug class): bit_in/varbit_in
 * are hand-rolled text parsers on an attacker-controlled cstring — exactly
 * the tid/ltree shape where a length/prefix/accumulate mistake is a
 * memory-safety bug. Until this lane there was NO verbatim-C oracle for
 * them in csrc/, so the PARSER edge bank (empty, bad prefix, odd hex length,
 * huge length, overflow bit count, embedded NUL, non-binary/hex chars) had
 * nothing to differentially fire against. The bar: pgrust
 * `adt_varbit::bits_in` must ACCEPT-or-REJECT each input IDENTICALLY to this
 * verbatim C — same value image, same error verdict, same SQLSTATE class.
 *
 * Shims (plumbing only, never logic — same conventions as pg_int_io.c):
 *   - fmgr: Datum = uintptr_t; MiniFcinfo carries typed args + the
 *     escontext pointer; PG_GETARG_* / PG_RETURN_* map onto it.
 *   - ereturn(escontext,ret,...) -> record errcode class; soft (escontext
 *     != NULL) returns ret, hard longjmps — exactly errsave's control flow.
 *   - errcode symbols -> small ints (PG_DIFF_ERR_*); errmsg/errdetail
 *     evaluate to 0 (args ARE evaluated, per the real varargs signature,
 *     then discarded: the comparator checks the errcode CLASS, not text).
 *   - pg_mblen_cstr(s) -> 1: it feeds only the discarded "%.*s" message
 *     width; the value/verdict/errclass planes never see it.
 *   - palloc0 -> calloc-backed TLS pointer arena, reset at every dispatcher
 *     entry (PostgreSQL's memory-context reset, minimally).
 *   - SET_VARSIZE -> little-endian 4-byte varlena header (len << 2); VARSIZE
 *     reads it back (the 4B-header definition from postgres.h varatt on LE).
 *   - VarBit struct / VARBITLEN / VARBITS / VARBITTOTALLEN / VARBITMAXLEN /
 *     HIGHBIT / BITS_PER_BYTE -> the c.h / varbit.h definitions verbatim.
 */

/* FAMILY SYMBOL ISOLATION (symfix lane precedent, pg_int_io.c header): this
 * TU shares the pg_difffuzz_oracle cc::Build; prefix the verbatim exports so
 * ld.lld (Linux CI cluster) does not hard-error on duplicate symbols. Preprocessor
 * rename ONLY — every C body below stays verbatim. */
#define bit_in vbio_bit_in
#define varbit_in vbio_varbit_in

#include "postgres.h"

#include <limits.h>
#include <setjmp.h>
#include <string.h>
#include <assert.h>
#include "pg_oracle_guard.h"	/* oracle-serialization holder check */

/* ---- error-plane shim (same convention as pg_int_io.c) ---- */

#define PG_DIFF_ERR_INVALID_TEXT 1        /* 22P02 */
#define PG_DIFF_ERR_PROGRAM_LIMIT 3       /* 54000 */
#define PG_DIFF_ERR_LENGTH_MISMATCH 4     /* 22026 */
#define PG_DIFF_ERR_RIGHT_TRUNCATION 9    /* 22001 */

#define ERRCODE_INVALID_TEXT_REPRESENTATION PG_DIFF_ERR_INVALID_TEXT
#define ERRCODE_PROGRAM_LIMIT_EXCEEDED PG_DIFF_ERR_PROGRAM_LIMIT
#define ERRCODE_STRING_DATA_LENGTH_MISMATCH PG_DIFF_ERR_LENGTH_MISMATCH
#define ERRCODE_STRING_DATA_RIGHT_TRUNCATION PG_DIFF_ERR_RIGHT_TRUNCATION

static _Thread_local int pg_diff_varbit_errcode;
static _Thread_local jmp_buf pg_diff_varbit_jb;

int
pg_diff_varbit_errcode_get(void)
{
	return pg_diff_varbit_errcode;
}

static int
errcode(int code)
{
	pg_diff_varbit_errcode = code;
	return 0;
}

static int
errmsg(const char *fmt, ...)
{
	(void) fmt;
	return 0;
}

#define errdetail errmsg
#define errhint errmsg
#define ERROR 21
#define ereport(elevel, rest) \
	do { (void) (rest); longjmp(pg_diff_varbit_jb, 1); } while (0)
#define elog(elevel, ...) \
	do { pg_diff_varbit_errcode = 98; longjmp(pg_diff_varbit_jb, 1); } while (0)
/* errsave/ereturn: soft (escontext != NULL) records and returns dummy_value,
 * exactly the ErrorSaveContext control flow; hard raises. */
#define ereturn(escontext, dummy_value, rest) \
	do { \
		(void) (rest); \
		if ((escontext) != NULL) \
			return dummy_value; \
		longjmp(pg_diff_varbit_jb, 1); \
	} while (0)

typedef struct Node Node;

/* pg_mblen_cstr: only the discarded "%.*s" width — shim to 1 (plumbing). */
static int
pg_mblen_cstr(const char *s)
{
	(void) s;
	return 1;
}

/* ---- fmgr mini-shim ---- */

typedef uintptr_t Datum;
typedef uint32 Oid;

typedef struct MiniFcinfo
{
	int64		i[5];
	Node	   *context;
} MiniFcinfo;

#define PG_FUNCTION_ARGS MiniFcinfo *fcinfo
#define PG_GETARG_INT32(n) ((int32) fcinfo->i[n])
#define PG_GETARG_CSTRING(n) ((char *) (uintptr_t) fcinfo->i[n])
#define PG_RETURN_VARBIT_P(x) return (Datum) (uintptr_t) (x)

/* palloc0 arena shim: reset at every pg_diff_* dispatcher entry so the
 * error-path longjmp/ereturn exits cannot leak (pg_int_io.c LSan lesson). */
#define PG_DIFF_ARENA_MAX 8
static _Thread_local void *pg_diff_varbit_arena[PG_DIFF_ARENA_MAX];
static _Thread_local int pg_diff_varbit_arena_n;

static void
pg_diff_varbit_arena_reset(void)
{
	int			i;

	for (i = 0; i < pg_diff_varbit_arena_n; i++)
		free(pg_diff_varbit_arena[i]);
	pg_diff_varbit_arena_n = 0;
}

static void *
pg_diff_palloc0_impl(size_t n)
{
	void	   *p = calloc(1, n);

	assert(pg_diff_varbit_arena_n < PG_DIFF_ARENA_MAX);
	pg_diff_varbit_arena[pg_diff_varbit_arena_n++] = p;
	return p;
}

#define palloc0(n) pg_diff_palloc0_impl(n)

/* ---- varatt 4B little-endian header ---- */
#define VARHDRSZ ((int32) sizeof(int32))
#define SET_VARSIZE(PTR, len) (*((uint32 *) (PTR)) = ((uint32) (len)) << 2)
#define VARSIZE(PTR) ((*((const uint32 *) (PTR))) >> 2)

/* ---- varbit.h VarBit + accessors (c.h/varbit.h, verbatim shapes) ---- */
typedef uint8 bits8;

#define BITS_PER_BYTE 8
#define HIGHBIT (0x80)
#define VARBITHDRSZ ((int32) sizeof(int32))

typedef struct
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	int32		bit_len;		/* number of valid bits */
	bits8		bit_dat[1];		/* bit string, most sig. byte first */
} VarBit;

/* VARBITTOTALLEN(BITLEN): total varlena bytes for a BITLEN-bit string. */
#define VARBITTOTALLEN(BITLEN) \
	(((BITLEN) + BITS_PER_BYTE - 1) / BITS_PER_BYTE + VARHDRSZ + VARBITHDRSZ)
/* VARBITS(PTR): pointer to the bit data. */
#define VARBITS(PTR) (((VarBit *) (PTR))->bit_dat)
/* VARBITLEN(PTR): lvalue for the bit length. */
#define VARBITLEN(PTR) (((VarBit *) (PTR))->bit_len)
/* varbit.h: INT_MAX - BITS_PER_BYTE + 1. */
#define VARBITMAXLEN (INT_MAX - BITS_PER_BYTE + 1)

#ifndef Min
#define Min(x, y) ((x) < (y) ? (x) : (y))
#endif

/* ====================================================================
 * VERBATIM bodies — varbit.c @ 62d6c7d3df.
 * ==================================================================== */

Datum
bit_in(PG_FUNCTION_ARGS)
{
	char	   *input_string = PG_GETARG_CSTRING(0);
#ifdef NOT_USED
	Oid			typelem = PG_GETARG_OID(1);
#endif
	int32		atttypmod = PG_GETARG_INT32(2);
	Node	   *escontext = fcinfo->context;
	VarBit	   *result;			/* The resulting bit string			  */
	char	   *sp;				/* pointer into the character string  */
	bits8	   *r;				/* pointer into the result */
	int			len,			/* Length of the whole data structure */
				bitlen,			/* Number of bits in the bit string   */
				slen;			/* Length of the input string		  */
	bool		bit_not_hex;	/* false = hex string  true = bit string */
	int			bc;
	bits8		x = 0;

	/* Check that the first character is a b or an x */
	if (input_string[0] == 'b' || input_string[0] == 'B')
	{
		bit_not_hex = true;
		sp = input_string + 1;
	}
	else if (input_string[0] == 'x' || input_string[0] == 'X')
	{
		bit_not_hex = false;
		sp = input_string + 1;
	}
	else
	{
		/*
		 * Otherwise it's binary.  This allows things like cast('1001' as bit)
		 * to work transparently.
		 */
		bit_not_hex = true;
		sp = input_string;
	}

	/*
	 * Determine bitlength from input string.  MaxAllocSize ensures a regular
	 * input is small enough, but we must check hex input.
	 */
	slen = strlen(sp);
	if (bit_not_hex)
		bitlen = slen;
	else
	{
		if (slen > VARBITMAXLEN / 4)
			ereturn(escontext, (Datum) 0,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("bit string length exceeds the maximum allowed (%d)",
							VARBITMAXLEN)));
		bitlen = slen * 4;
	}

	/*
	 * Sometimes atttypmod is not supplied. If it is supplied we need to make
	 * sure that the bitstring fits.
	 */
	if (atttypmod <= 0)
		atttypmod = bitlen;
	else if (bitlen != atttypmod)
		ereturn(escontext, (Datum) 0,
				(errcode(ERRCODE_STRING_DATA_LENGTH_MISMATCH),
				 errmsg("bit string length %d does not match type bit(%d)",
						bitlen, atttypmod)));

	len = VARBITTOTALLEN(atttypmod);
	/* set to 0 so that *r is always initialised and string is zero-padded */
	result = (VarBit *) palloc0(len);
	SET_VARSIZE(result, len);
	VARBITLEN(result) = atttypmod;

	r = VARBITS(result);
	if (bit_not_hex)
	{
		/* Parse the bit representation of the string */
		/* We know it fits, as bitlen was compared to atttypmod */
		x = HIGHBIT;
		for (; *sp; sp++)
		{
			if (*sp == '1')
				*r |= x;
			else if (*sp != '0')
				ereturn(escontext, (Datum) 0,
						(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
						 errmsg("\"%.*s\" is not a valid binary digit",
								pg_mblen_cstr(sp), sp)));

			x >>= 1;
			if (x == 0)
			{
				x = HIGHBIT;
				r++;
			}
		}
	}
	else
	{
		/* Parse the hex representation of the string */
		for (bc = 0; *sp; sp++)
		{
			if (*sp >= '0' && *sp <= '9')
				x = (bits8) (*sp - '0');
			else if (*sp >= 'A' && *sp <= 'F')
				x = (bits8) (*sp - 'A') + 10;
			else if (*sp >= 'a' && *sp <= 'f')
				x = (bits8) (*sp - 'a') + 10;
			else
				ereturn(escontext, (Datum) 0,
						(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
						 errmsg("\"%.*s\" is not a valid hexadecimal digit",
								pg_mblen_cstr(sp), sp)));

			if (bc)
			{
				*r++ |= x;
				bc = 0;
			}
			else
			{
				*r = x << 4;
				bc = 1;
			}
		}
	}

	PG_RETURN_VARBIT_P(result);
}

Datum
varbit_in(PG_FUNCTION_ARGS)
{
	char	   *input_string = PG_GETARG_CSTRING(0);
#ifdef NOT_USED
	Oid			typelem = PG_GETARG_OID(1);
#endif
	int32		atttypmod = PG_GETARG_INT32(2);
	Node	   *escontext = fcinfo->context;
	VarBit	   *result;			/* The resulting bit string			  */
	char	   *sp;				/* pointer into the character string  */
	bits8	   *r;				/* pointer into the result */
	int			len,			/* Length of the whole data structure */
				bitlen,			/* Number of bits in the bit string   */
				slen;			/* Length of the input string		  */
	bool		bit_not_hex;	/* false = hex string  true = bit string */
	int			bc;
	bits8		x = 0;

	/* Check that the first character is a b or an x */
	if (input_string[0] == 'b' || input_string[0] == 'B')
	{
		bit_not_hex = true;
		sp = input_string + 1;
	}
	else if (input_string[0] == 'x' || input_string[0] == 'X')
	{
		bit_not_hex = false;
		sp = input_string + 1;
	}
	else
	{
		bit_not_hex = true;
		sp = input_string;
	}

	/*
	 * Determine bitlength from input string.  MaxAllocSize ensures a regular
	 * input is small enough, but we must check hex input.
	 */
	slen = strlen(sp);
	if (bit_not_hex)
		bitlen = slen;
	else
	{
		if (slen > VARBITMAXLEN / 4)
			ereturn(escontext, (Datum) 0,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("bit string length exceeds the maximum allowed (%d)",
							VARBITMAXLEN)));
		bitlen = slen * 4;
	}

	/*
	 * Sometimes atttypmod is not supplied. If it is supplied we need to make
	 * sure that the bitstring fits.
	 */
	if (atttypmod <= 0)
		atttypmod = bitlen;
	else if (bitlen > atttypmod)
		ereturn(escontext, (Datum) 0,
				(errcode(ERRCODE_STRING_DATA_RIGHT_TRUNCATION),
				 errmsg("bit string too long for type bit varying(%d)",
						atttypmod)));

	len = VARBITTOTALLEN(bitlen);
	/* set to 0 so that *r is always initialised and string is zero-padded */
	result = (VarBit *) palloc0(len);
	SET_VARSIZE(result, len);
	VARBITLEN(result) = Min(bitlen, atttypmod);

	r = VARBITS(result);
	if (bit_not_hex)
	{
		/* Parse the bit representation of the string */
		/* We know it fits, as bitlen was compared to atttypmod */
		x = HIGHBIT;
		for (; *sp; sp++)
		{
			if (*sp == '1')
				*r |= x;
			else if (*sp != '0')
				ereturn(escontext, (Datum) 0,
						(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
						 errmsg("\"%.*s\" is not a valid binary digit",
								pg_mblen_cstr(sp), sp)));

			x >>= 1;
			if (x == 0)
			{
				x = HIGHBIT;
				r++;
			}
		}
	}
	else
	{
		/* Parse the hex representation of the string */
		for (bc = 0; *sp; sp++)
		{
			if (*sp >= '0' && *sp <= '9')
				x = (bits8) (*sp - '0');
			else if (*sp >= 'A' && *sp <= 'F')
				x = (bits8) (*sp - 'A') + 10;
			else if (*sp >= 'a' && *sp <= 'f')
				x = (bits8) (*sp - 'a') + 10;
			else
				ereturn(escontext, (Datum) 0,
						(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
						 errmsg("\"%.*s\" is not a valid hexadecimal digit",
								pg_mblen_cstr(sp), sp)));

			if (bc)
			{
				*r++ |= x;
				bc = 0;
			}
			else
			{
				*r = x << 4;
				bc = 1;
			}
		}
	}

	PG_RETURN_VARBIT_P(result);
}

/* ====================================================================
 * Dispatcher (shim, not PG code): the exported pg_diff_* entry the Rust
 * differential harness calls. Control flow only.
 *
 * `fixed`  : 1 => bit_in (fixed bit(N)); 0 => varbit_in (bit varying(N)).
 * `soft`   : 1 => escontext non-NULL (soft-error/ErrorSaveContext plane).
 * Returns: 0 = ok (*out_len bytes of the varlena image copied to out_img);
 *          >0 = hard errclass; <0 = -errclass caught softly (escontext path).
 * ==================================================================== */
int
pg_diff_bits_in(const char *input, int atttypmod, int fixed, int soft,
				unsigned char *out_img, int out_cap, int *out_len)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	static _Thread_local int soft_sentinel;
	MiniFcinfo	fc = {{0}, NULL};
	Datum		res;
	const unsigned char *img;
	int			sz;

	pg_diff_varbit_arena_reset();
	pg_diff_varbit_errcode = 0;
	*out_len = 0;
	if (setjmp(pg_diff_varbit_jb))
		return pg_diff_varbit_errcode;

	fc.i[0] = (int64) (uintptr_t) input;
	fc.i[2] = (int64) atttypmod;
	fc.context = soft ? (Node *) &soft_sentinel : NULL;

	if (fixed)
		res = bit_in(&fc);
	else
		res = varbit_in(&fc);

	/* Soft error caught: res is the ereturn dummy (0), errcode set. */
	if (pg_diff_varbit_errcode)
		return -pg_diff_varbit_errcode;

	img = (const unsigned char *) (uintptr_t) res;
	sz = (int) VARSIZE(img);
	assert(sz >= 0 && sz <= out_cap);
	if (sz > out_cap)
		sz = out_cap;
	memcpy(out_img, img, (size_t) sz);
	*out_len = sz;
	return 0;
}
