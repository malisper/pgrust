/*
 * Vendored PostgreSQL C for the varbit operator-row proofs
 * (proofs/varbit-rows).
 *
 * Provenance: src/backend/utils/adt/varbit.c, REL_18_STABLE, fetched
 * 2026-07-28 from
 * https://raw.githubusercontent.com/postgres/postgres/REL_18_STABLE/src/backend/utils/adt/varbit.c
 * Functions vendored verbatim (bodies unchanged modulo the shims below):
 *  - bit_cmp (lines ~817-838) and the operator wrappers biteq, bitne,
 *    bitlt, bitle, bitgt, bitge, bitcmp (lines ~840-965)
 *  - bit_and, bit_or, bitxor (lines ~1238-1358)
 *  - bitnot (lines ~1360-1386) incl. VARBIT_PAD_LAST (lines ~59-65)
 *  - bitshiftleft, bitshiftright (lines ~1388-1530)
 *
 * NOTE: varbit_larger/varbit_smaller do NOT exist in REL_18_STABLE (or
 * master as of 2026-07-28) varbit.c — there are no min/max support fns for
 * bit varying upstream. Nothing to vendor; nothing to prove.
 *
 * SHIMS (plumbing only, never logic):
 *  - names pg_-prefixed; postgres typedefs inlined (bits8 -> unsigned char,
 *    int32 -> int, Datum plumbing removed).
 *  - VarBit* accessors -> explicit parameters: VARBITS(x) -> bitsN,
 *    VARBITBYTES(x) -> bytelenN, VARBITLEN(x) -> bitlenN,
 *    VARBITEND(x) -> bitsN + bytelenN. The varlena header writes
 *    (SET_VARSIZE / VARBITLEN(result) =) are dropped: under the harness's
 *    valid-varbit fence (ceil(bitlen/8) == bytelen) both sides' headers are
 *    the same two integers (bytelen + VARHDRSZ + VARBITHDRSZ, bitlen), which
 *    the harness carries explicitly.
 *  - PG_GETARG_VARBIT_P / PG_FREE_IF_COPY / PG_RETURN_* -> plain C
 *    signatures; detoasting is out of scope (pre-detoasted caller contract).
 *  - palloc(VARSIZE(..)) -> caller-provided result buffer `r` of the
 *    argument's byte length. int return for C void/bool: Kani lowers Rust ()
 *    as struct Unit, which goto-cc rejects against void.
 *  - ereport(ERROR, ...) in bit_and/bit_or/bitxor -> return -1 sentinel at
 *    the exact program point where C raises (message text is outside the
 *    equivalence claim); success returns 0.
 *  - DirectFunctionCall2(bitshiftright/left, ...) in the negative-shift arms
 *    -> direct call of the sibling shim (same recursion shape).
 *  - VARBIT_PAD_LAST(vb, ptr) statement macro expanded inline where C
 *    invokes it, with VARBITPAD(vb) = bytelen*8 - bitlen and BITMASK = 0xFF.
 *    Its Assert(pad_ >= 0 && pad_ < BITS_PER_BYTE) is compiled out in
 *    production; the harness fences inputs to the asserted domain via the
 *    valid varbit relation. Likewise varbit.c's VARBIT_CORRECTLY_PADDED
 *    debug assert (lines ~68-77) documents the pad-bits-zero input
 *    invariant the harness assumes.
 *  - Min() inlined; MemSet -> memset; memcmp/memcpy/memset are CBMC's
 *    built-in models.
 */

#include <limits.h>
#include <string.h>

#define BITS_PER_BYTE 8
#define BITMASK 0xFF
/* varbit.h */
#define VARBITMAXLEN (INT_MAX - BITS_PER_BYTE + 1)

typedef unsigned char bits8;
#include <stdint.h>
typedef int32_t int32;
typedef int64_t int64;
typedef uint32_t uint32;
typedef uint64_t uint64;

/* ---------------- bit_cmp ---------------- */

static int
pg_bit_cmp_internal(const bits8 *bits1, int bytelen1, int bitlen1,
					const bits8 *bits2, int bytelen2, int bitlen2)
{
	int			cmp;

	cmp = memcmp(bits1, bits2, (bytelen1 < bytelen2) ? bytelen1 : bytelen2);
	if (cmp == 0)
	{
		if (bitlen1 != bitlen2)
			cmp = (bitlen1 < bitlen2) ? -1 : 1;
	}
	return cmp;
}

/* ---------------- operator wrappers ---------------- */

int
pg_biteq(const bits8 *bits1, int bytelen1, int bitlen1,
		 const bits8 *bits2, int bytelen2, int bitlen2)
{
	int			result;

	/* fast path for different-length inputs */
	if (bitlen1 != bitlen2)
		result = 0;
	else
		result = (pg_bit_cmp_internal(bits1, bytelen1, bitlen1,
									  bits2, bytelen2, bitlen2) == 0);
	return result;
}

int
pg_bitne(const bits8 *bits1, int bytelen1, int bitlen1,
		 const bits8 *bits2, int bytelen2, int bitlen2)
{
	int			result;

	/* fast path for different-length inputs */
	if (bitlen1 != bitlen2)
		result = 1;
	else
		result = (pg_bit_cmp_internal(bits1, bytelen1, bitlen1,
									  bits2, bytelen2, bitlen2) != 0);
	return result;
}

int
pg_bitlt(const bits8 *bits1, int bytelen1, int bitlen1,
		 const bits8 *bits2, int bytelen2, int bitlen2)
{
	return (pg_bit_cmp_internal(bits1, bytelen1, bitlen1,
								bits2, bytelen2, bitlen2) < 0);
}

int
pg_bitle(const bits8 *bits1, int bytelen1, int bitlen1,
		 const bits8 *bits2, int bytelen2, int bitlen2)
{
	return (pg_bit_cmp_internal(bits1, bytelen1, bitlen1,
								bits2, bytelen2, bitlen2) <= 0);
}

int
pg_bitgt(const bits8 *bits1, int bytelen1, int bitlen1,
		 const bits8 *bits2, int bytelen2, int bitlen2)
{
	return (pg_bit_cmp_internal(bits1, bytelen1, bitlen1,
								bits2, bytelen2, bitlen2) > 0);
}

int
pg_bitge(const bits8 *bits1, int bytelen1, int bitlen1,
		 const bits8 *bits2, int bytelen2, int bitlen2)
{
	return (pg_bit_cmp_internal(bits1, bytelen1, bitlen1,
								bits2, bytelen2, bitlen2) >= 0);
}

int
pg_bitcmp(const bits8 *bits1, int bytelen1, int bitlen1,
		  const bits8 *bits2, int bytelen2, int bitlen2)
{
	return pg_bit_cmp_internal(bits1, bytelen1, bitlen1,
							   bits2, bytelen2, bitlen2);
}

/* ---------------- bit_and / bit_or / bitxor ---------------- */

/* SHIM: returns -1 where C ereports (ERRCODE_STRING_DATA_LENGTH_MISMATCH) */
int
pg_bit_and(const bits8 *arg1bits, int bytelen1, int bitlen1,
		   const bits8 *arg2bits, int bytelen2, int bitlen2,
		   bits8 *result)
{
	int			i;
	const bits8 *p1,
			   *p2;
	bits8	   *r;

	if (bitlen1 != bitlen2)
		return -1;				/* SHIM: was ereport(ERROR, "cannot AND bit
								 * strings of different sizes") */

	p1 = arg1bits;
	p2 = arg2bits;
	r = result;
	for (i = 0; i < bytelen1; i++)
		*r++ = *p1++ & *p2++;

	/* Padding is not needed as & of 0 pads is 0 */

	return 0;
}

/* SHIM: returns -1 where C ereports (ERRCODE_STRING_DATA_LENGTH_MISMATCH) */
int
pg_bit_or(const bits8 *arg1bits, int bytelen1, int bitlen1,
		  const bits8 *arg2bits, int bytelen2, int bitlen2,
		  bits8 *result)
{
	int			i;
	const bits8 *p1,
			   *p2;
	bits8	   *r;

	if (bitlen1 != bitlen2)
		return -1;				/* SHIM: was ereport(ERROR, "cannot OR bit
								 * strings of different sizes") */

	p1 = arg1bits;
	p2 = arg2bits;
	r = result;
	for (i = 0; i < bytelen1; i++)
		*r++ = *p1++ | *p2++;

	/* Padding is not needed as | of 0 pads is 0 */

	return 0;
}

/* SHIM: returns -1 where C ereports (ERRCODE_STRING_DATA_LENGTH_MISMATCH) */
int
pg_bitxor(const bits8 *arg1bits, int bytelen1, int bitlen1,
		  const bits8 *arg2bits, int bytelen2, int bitlen2,
		  bits8 *result)
{
	int			i;
	const bits8 *p1,
			   *p2;
	bits8	   *r;

	if (bitlen1 != bitlen2)
		return -1;				/* SHIM: was ereport(ERROR, "cannot XOR bit
								 * strings of different sizes") */

	p1 = arg1bits;
	p2 = arg2bits;
	r = result;
	for (i = 0; i < bytelen1; i++)
		*r++ = *p1++ ^ *p2++;

	/* Padding is not needed as ^ of 0 pads is 0 */

	return 0;
}

/* ---------------- bitnot ---------------- */

int
pg_bitnot(const bits8 *argbits, int bytelen, int bitlen, bits8 *result)
{
	const bits8 *p;
	bits8	   *r;

	p = argbits;
	r = result;
	for (; p < argbits + bytelen; p++)
		*r++ = ~*p;

	/* Must zero-pad the result, because extra bits are surely 1's here */
	/* VARBIT_PAD_LAST(result, r) expanded: */
	{
		int			pad_ = bytelen * BITS_PER_BYTE - bitlen;

		/* Assert(pad_ >= 0 && pad_ < BITS_PER_BYTE); -- compiled out */
		if (pad_ > 0)
			*(r - 1) &= BITMASK << pad_;
	}

	return 0;
}

/* ---------------- bitshiftleft / bitshiftright ---------------- */

/*
 * SHIM (structure only, bodies verbatim): C's bitshiftleft/bitshiftright
 * negative arms call each other via DirectFunctionCall2, forming a syntactic
 * recursion cycle that can only ever go one level deep (the clamped negation
 * is >= 0). CBMC unwinds the syntactic cycle to the loop bound (~10x formula,
 * measured 40s vs ~2s), so the shim splits each function into a verbatim
 * positive-shift core (pg_bitshift*_pos) plus an acyclic dispatcher whose
 * negative arm calls the OTHER direction's positive core directly — the same
 * one-level path the C recursion always takes. Mirrors the shipped Rust
 * bitshift*_body/_pos split.
 */
static int pg_bitshiftright_pos(const bits8 *argbits, int bytelen, int bitlen,
								int shft, bits8 *result);

static int
pg_bitshiftleft_pos(const bits8 *argbits, int bytelen, int bitlen,
					int shft, bits8 *result)
{
	int			byte_shift,
				ishift,
				len;
	const bits8 *p;
	bits8	   *r;

	r = result;

	/* If we shifted all the bits out, return an all-zero string */
	if (shft >= bitlen)
	{
		memset(r, 0, bytelen);
		return 0;
	}

	byte_shift = shft / BITS_PER_BYTE;
	ishift = shft % BITS_PER_BYTE;
	p = argbits + byte_shift;

	if (ishift == 0)
	{
		/* Special case: we can do a memcpy */
		len = bytelen - byte_shift;
		memcpy(r, p, len);
		memset(r + len, 0, byte_shift);
	}
	else
	{
		for (; p < argbits + bytelen; r++)
		{
			*r = *p << ishift;
			if ((++p) < argbits + bytelen)
				*r |= *p >> (BITS_PER_BYTE - ishift);
		}
		for (; r < result + bytelen; r++)
			*r = 0;
	}

	/* The pad bits should be already zero at this point */

	return 0;
}

static int
pg_bitshiftright_pos(const bits8 *argbits, int bytelen, int bitlen,
					 int shft, bits8 *result)
{
	int			byte_shift,
				ishift,
				len;
	const bits8 *p;
	bits8	   *r;

	r = result;

	/* If we shifted all the bits out, return an all-zero string */
	if (shft >= bitlen)
	{
		memset(r, 0, bytelen);
		return 0;
	}

	byte_shift = shft / BITS_PER_BYTE;
	ishift = shft % BITS_PER_BYTE;
	p = argbits;

	/* Set the first part of the result to 0 */
	memset(r, 0, byte_shift);
	r += byte_shift;

	if (ishift == 0)
	{
		/* Special case: we can do a memcpy */
		len = bytelen - byte_shift;
		memcpy(r, p, len);
		r += len;
	}
	else
	{
		if (r < result + bytelen)
			*r = 0;				/* initialize first byte */
		for (; r < result + bytelen; p++)
		{
			*r |= *p >> ishift;
			if ((++r) < result + bytelen)
				*r = (*p << (BITS_PER_BYTE - ishift)) & BITMASK;
		}
	}

	/* We may have shifted 1's into the pad bits, so fix that */
	/* VARBIT_PAD_LAST(result, r) expanded: */
	{
		int			pad_ = bytelen * BITS_PER_BYTE - bitlen;

		/* Assert(pad_ >= 0 && pad_ < BITS_PER_BYTE); -- compiled out */
		if (pad_ > 0)
			*(r - 1) &= BITMASK << pad_;
	}

	return 0;
}

int
pg_bitshiftleft(const bits8 *argbits, int bytelen, int bitlen,
				int shft, bits8 *result)
{
	/* Negative shift is a shift to the right */
	if (shft < 0)
	{
		/* Prevent integer overflow in negation */
		if (shft < -VARBITMAXLEN)
			shft = -VARBITMAXLEN;
		return pg_bitshiftright_pos(argbits, bytelen, bitlen, -shft, result);
		/* SHIM: was PG_RETURN_DATUM(DirectFunctionCall2(bitshiftright, ..));
		 * acyclic dispatch, see header note above pg_bitshiftleft_pos */
	}
	return pg_bitshiftleft_pos(argbits, bytelen, bitlen, shft, result);
}

int
pg_bitshiftright(const bits8 *argbits, int bytelen, int bitlen,
				 int shft, bits8 *result)
{
	/* Negative shift is a shift to the left */
	if (shft < 0)
	{
		/* Prevent integer overflow in negation */
		if (shft < -VARBITMAXLEN)
			shft = -VARBITMAXLEN;
		return pg_bitshiftleft_pos(argbits, bytelen, bitlen, -shft, result);
		/* SHIM: was PG_RETURN_DATUM(DirectFunctionCall2(bitshiftleft, ..));
		 * acyclic dispatch, see header note above pg_bitshiftleft_pos */
	}
	return pg_bitshiftright_pos(argbits, bytelen, bitlen, shft, result);
}

/*
 * bitlength / bitoctetlength (pg_proc oids 1681/1682) and bittoint4 /
 * bittoint8 (oids 1684/2076), vendored from REL_18_STABLE
 * src/backend/utils/adt/varbit.c (fetched 2026-07-28).
 *
 * SHIMS (loop/shift expressions verbatim):
 *  - same VarBit-accessor flattening as above: VARBITS -> bits,
 *    VARBITBYTES -> bytelen, VARBITLEN -> bitlen, VARBITEND ->
 *    bits + bytelen, VARBITPAD -> bytelen*8 - bitlen.
 *  - bittoint4/8's ereport(ERROR, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
 *    "integer/bigint out of range") -> *err = 1 sentinel.
 */

int
pg_bitlength(int bitlen)
{
	return bitlen;				/* PG_RETURN_INT32(VARBITLEN(arg)) */
}

int
pg_bitoctetlength(int bytelen)
{
	return bytelen;				/* PG_RETURN_INT32(VARBITBYTES(arg)) */
}

int32
pg_bittoint4(const bits8 *bits, int bytelen, int bitlen, int *err)
{
	uint32		result;
	const bits8 *r;

	*err = 0;
	/* Check that the bit string is not too long */
	if (bitlen > (int) sizeof(result) * BITS_PER_BYTE)
	{
		*err = 1;				/* ereport: integer out of range */
		return 0;
	}

	result = 0;
	for (r = bits; r < bits + bytelen; r++)
	{
		result <<= BITS_PER_BYTE;
		result |= *r;
	}
	/* Now shift the result to take account of the padding at the end */
	result >>= bytelen * BITS_PER_BYTE - bitlen;

	return (int32) result;
}

int64
pg_bittoint8(const bits8 *bits, int bytelen, int bitlen, int *err)
{
	uint64		result;
	const bits8 *r;

	*err = 0;
	/* Check that the bit string is not too long */
	if (bitlen > (int) sizeof(result) * BITS_PER_BYTE)
	{
		*err = 1;				/* ereport: bigint out of range */
		return 0;
	}

	result = 0;
	for (r = bits; r < bits + bytelen; r++)
	{
		result <<= BITS_PER_BYTE;
		result |= *r;
	}
	/* Now shift the result to take account of the padding at the end */
	result >>= bytelen * BITS_PER_BYTE - bitlen;

	return (int64) result;
}

/* ======================================================================
 * WAVE-10 additions (2026-07-28): remaining varbit rows.
 *
 * Provenance: same file/ref as the header above — REL_18_STABLE
 * src/backend/utils/adt/varbit.c fetched 2026-07-28 — plus:
 *  - pg_number_of_ones table + the pg_popcount portable byte walk from
 *    src/port/pg_bitutils.c (bit_bit_count's pg_popcount; the word-level
 *    equivalence of that walk to popcount is already proved in
 *    proofs/bitutils — here the per-byte table walk is vendored verbatim)
 *  - pq_getmsgint(4)/pq_copymsgbytes from src/backend/libpq/pqformat.c and
 *    pg_add_s32_overflow's portable fallback from src/include/common/int.h
 *    (same shim text as proofs/int-arith, provenance there)
 *
 * Functions vendored verbatim (bodies unchanged modulo the shims already
 * documented in the header: VarBit accessor flattening, palloc -> caller
 * buffer, ereport/ereturn -> *err sentinel at the exact raising point,
 * int return for void):
 *   anybit_typmodin (checks only: ArrayGetIntegerTypmods -> caller (tl,n) —
 *     the array-literal parse itself stays in the tested tier both sides),
 *   bit_in, varbit_in (palloc0 -> memset of caller buffer; pg_mblen_cstr in
 *     the error message -> message text outside the claim; strlen kept),
 *   varbit_out (bit_out delegates to it in C),
 *   bit_recv, varbit_recv (StringInfo -> (data,len,cursor) triple),
 *   varbit_send (bit_send delegates; pq_begintypsend/pq_endtypsend ->
 *     caller buffer + 4B little-endian varlena header, as proofs/int-arith),
 *   bit (bit_coerce), varbit (varbit_coerce): identity arm -> return 0
 *     without writing (C returns the arg datum unchanged),
 *   bit_catenate, bitsubstring, bit_overlay (composed exactly as C:
 *     substring/substring/catenate/catenate over fixed temp buffers),
 *   bitfromint4, bitfromint8, bitsetbit, bitgetbit, bitposition,
 *   bit_bit_count.
 *
 * err sentinel map (message text always outside the claim):
 *   1 = ERRCODE_PROGRAM_LIMIT_EXCEEDED  (bit string length exceeds max)
 *   2 = ERRCODE_STRING_DATA_LENGTH_MISMATCH (bit) /
 *       ERRCODE_STRING_DATA_RIGHT_TRUNCATION (varbit)
 *   3 = ERRCODE_INVALID_TEXT_REPRESENTATION (bad digit)
 *   4 = ERRCODE_PROTOCOL_VIOLATION (insufficient data left in message)
 *   5 = ERRCODE_INVALID_BINARY_REPRESENTATION (invalid external length)
 *   6 = ERRCODE_SUBSTRING_ERROR (negative substring length)
 *   7 = ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE (integer out of range, overlay)
 *   8 = ERRCODE_ARRAY_SUBSCRIPT_ERROR (bit index out of range)
 *   9 = ERRCODE_INVALID_PARAMETER_VALUE (new bit / typmod checks)
 */

#define VARHDRSZ 4
#define VARBITHDRSZ 4
/* VARBITTOTALLEN counts header bytes; shims below track payload bytes via
 * VARBITBYTES-style ceil division, keeping the arithmetic verbatim. */
#define PG_VARBITBYTES(bitlen) (((bitlen) + BITS_PER_BYTE - 1) / BITS_PER_BYTE)

typedef int bool_c;
#define true 1
#define false 0

/* common/int.h pg_add_s32_overflow, portable fallback arm (verbatim) */
static inline bool_c
pg_add_s32_overflow(int32 a, int32 b, int32 *result)
{
	int64		res = (int64) a + (int64) b;

	if (res > INT_MAX || res < INT_MIN)
	{
		*result = 0x5EED;		/* to avoid spurious warnings */
		return true;
	}
	*result = (int32) res;
	return false;
}

/* pqformat.c shims (same text as proofs/int-arith) */
static int
pg_pq_copymsgbytes(const unsigned char *data, int32 len, int32 *cursor,
				   void *buf, int32 datalen)
{
	if (datalen < 0 || datalen > (len - *cursor))
		return 4;				/* insufficient data left in message */
	memcpy(buf, &data[*cursor], datalen);
	*cursor += datalen;
	return 0;
}

static int
pg_pq_getmsgint4(const unsigned char *data, int32 len, int32 *cursor,
				 int32 *out)
{
	unsigned char b[4];
	int			st = pg_pq_copymsgbytes(data, len, cursor, b, 4);

	if (st != 0)
		return st;
	*out = (int32) (((uint32) b[0] << 24) | ((uint32) b[1] << 16) |
					((uint32) b[2] << 8) | (uint32) b[3]);
	return 0;
}

/* ---------------- anybit_typmodin (checks; array parse in tested tier) --- */

#define MaxAttrSize (10 * 1024 * 1024)

int
pg_anybit_typmodin(const int32 *tl, int n, int *err)
{
	int32		typmod;

	*err = 0;
	if (n != 1)
	{
		*err = 9;				/* invalid type modifier */
		return -1;
	}
	if (*tl < 1)
	{
		*err = 9;				/* length must be at least 1 */
		return -1;
	}
	if (*tl > (MaxAttrSize * BITS_PER_BYTE))
	{
		*err = 9;				/* length cannot exceed ... */
		return -1;
	}
	typmod = *tl;

	return typmod;
}

/* ---------------- bit_in / varbit_in ---------------- */
/* palloc0 -> memset(rbuf); result header writes -> *rbitlen. */

#define HIGHBIT (0x80)

int
pg_bit_in(const char *input_string, int32 atttypmod,
		  bits8 *rbuf, int *rbitlen, int *err)
{
	const char *sp;
	bits8	   *r;
	int			len,
				bitlen,
				slen;
	bool_c		bit_not_hex;
	int			bc;
	bits8		x = 0;

	*err = 0;
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

	slen = strlen(sp);
	if (bit_not_hex)
		bitlen = slen;
	else
	{
		if (slen > VARBITMAXLEN / 4)
		{
			*err = 1;
			return -1;
		}
		bitlen = slen * 4;
	}

	if (atttypmod <= 0)
		atttypmod = bitlen;
	else if (bitlen != atttypmod)
	{
		*err = 2;
		return -1;
	}

	len = PG_VARBITBYTES(atttypmod);	/* payload bytes of VARBITTOTALLEN */
	memset(rbuf, 0, len);		/* palloc0 */
	*rbitlen = atttypmod;

	r = rbuf;
	if (bit_not_hex)
	{
		x = HIGHBIT;
		for (; *sp; sp++)
		{
			if (*sp == '1')
				*r |= x;
			else if (*sp != '0')
			{
				*err = 3;
				return -1;
			}

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
		for (bc = 0; *sp; sp++)
		{
			if (*sp >= '0' && *sp <= '9')
				x = (bits8) (*sp - '0');
			else if (*sp >= 'A' && *sp <= 'F')
				x = (bits8) (*sp - 'A') + 10;
			else if (*sp >= 'a' && *sp <= 'f')
				x = (bits8) (*sp - 'a') + 10;
			else
			{
				*err = 3;
				return -1;
			}

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

	return 0;
}

int
pg_varbit_in(const char *input_string, int32 atttypmod,
			 bits8 *rbuf, int *rbitlen, int *err)
{
	const char *sp;
	bits8	   *r;
	int			len,
				bitlen,
				slen;
	bool_c		bit_not_hex;
	int			bc;
	bits8		x = 0;

	*err = 0;
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

	slen = strlen(sp);
	if (bit_not_hex)
		bitlen = slen;
	else
	{
		if (slen > VARBITMAXLEN / 4)
		{
			*err = 1;
			return -1;
		}
		bitlen = slen * 4;
	}

	if (atttypmod <= 0)
		atttypmod = bitlen;
	else if (bitlen > atttypmod)
	{
		*err = 2;
		return -1;
	}

	len = PG_VARBITBYTES(bitlen);
	memset(rbuf, 0, len);		/* palloc0 */
	*rbitlen = (bitlen < atttypmod) ? bitlen : atttypmod;	/* Min */

	r = rbuf;
	if (bit_not_hex)
	{
		x = HIGHBIT;
		for (; *sp; sp++)
		{
			if (*sp == '1')
				*r |= x;
			else if (*sp != '0')
			{
				*err = 3;
				return -1;
			}

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
		for (bc = 0; *sp; sp++)
		{
			if (*sp >= '0' && *sp <= '9')
				x = (bits8) (*sp - '0');
			else if (*sp >= 'A' && *sp <= 'F')
				x = (bits8) (*sp - 'A') + 10;
			else if (*sp >= 'a' && *sp <= 'f')
				x = (bits8) (*sp - 'a') + 10;
			else
			{
				*err = 3;
				return -1;
			}

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

	return 0;
}

/* ---------------- varbit_out (bit_out delegates to it) ---------------- */

#define IS_HIGHBIT_SET(ch) ((unsigned char)(ch) & HIGHBIT)

int
pg_varbit_out(const bits8 *bits, int bytelen, int bitlen, char *result)
{
	char	   *r;
	const bits8 *sp;
	bits8		x;
	int			i,
				k,
				len;

	(void) bytelen;
	len = bitlen;
	sp = bits;
	r = result;
	for (i = 0; i <= len - BITS_PER_BYTE; i += BITS_PER_BYTE, sp++)
	{
		x = *sp;
		for (k = 0; k < BITS_PER_BYTE; k++)
		{
			*r++ = IS_HIGHBIT_SET(x) ? '1' : '0';
			x <<= 1;
		}
	}
	if (i < len)
	{
		x = *sp;
		for (k = i; k < len; k++)
		{
			*r++ = IS_HIGHBIT_SET(x) ? '1' : '0';
			x <<= 1;
		}
	}
	*r = '\0';

	return 0;
}

/* ---------------- bit_recv / varbit_recv ---------------- */

int
pg_bit_recv(const unsigned char *data, int32 len_msg, int32 *cursor,
			int32 atttypmod, bits8 *rbuf, int *rbitlen, int *err)
{
	int			bitlen;
	int			st;

	*err = 0;
	st = pg_pq_getmsgint4(data, len_msg, cursor, &bitlen);
	if (st != 0)
	{
		*err = st;				/* 4 = protocol violation */
		return -1;
	}
	if (bitlen < 0 || bitlen > VARBITMAXLEN)
	{
		*err = 5;				/* invalid length in external bit string */
		return -1;
	}

	if (atttypmod > 0 && bitlen != atttypmod)
	{
		*err = 2;				/* length does not match type bit(n) */
		return -1;
	}

	*rbitlen = bitlen;

	st = pg_pq_copymsgbytes(data, len_msg, cursor, rbuf,
							PG_VARBITBYTES(bitlen));
	if (st != 0)
	{
		*err = st;
		return -1;
	}

	/* VARBIT_PAD(result) expanded */
	{
		int			pad_ = PG_VARBITBYTES(bitlen) * BITS_PER_BYTE - bitlen;

		if (pad_ > 0)
			rbuf[PG_VARBITBYTES(bitlen) - 1] &= BITMASK << pad_;
	}

	return 0;
}

int
pg_varbit_recv(const unsigned char *data, int32 len_msg, int32 *cursor,
			   int32 atttypmod, bits8 *rbuf, int *rbitlen, int *err)
{
	int			bitlen;
	int			st;

	*err = 0;
	st = pg_pq_getmsgint4(data, len_msg, cursor, &bitlen);
	if (st != 0)
	{
		*err = st;
		return -1;
	}
	if (bitlen < 0 || bitlen > VARBITMAXLEN)
	{
		*err = 5;
		return -1;
	}

	if (atttypmod > 0 && bitlen > atttypmod)
	{
		*err = 2;				/* too long for type bit varying(n) */
		return -1;
	}

	*rbitlen = bitlen;

	st = pg_pq_copymsgbytes(data, len_msg, cursor, rbuf,
							PG_VARBITBYTES(bitlen));
	if (st != 0)
	{
		*err = st;
		return -1;
	}

	{
		int			pad_ = PG_VARBITBYTES(bitlen) * BITS_PER_BYTE - bitlen;

		if (pad_ > 0)
			rbuf[PG_VARBITBYTES(bitlen) - 1] &= BITMASK << pad_;
	}

	return 0;
}

/* ---------------- varbit_send (bit_send delegates) ---------------- */
/* pq_begintypsend/pq_endtypsend -> caller buffer with a 4B little-endian
 * varlena header (len << 2), then pq_sendint32 (big-endian) + bytes. */

int
pg_varbit_send(const bits8 *bits, int bytelen, int bitlen,
			   unsigned char *out)
{
	int			total = VARHDRSZ + 4 + bytelen;
	uint32		hdr = (uint32) total << 2;

	out[0] = (unsigned char) (hdr & 0xFF);
	out[1] = (unsigned char) ((hdr >> 8) & 0xFF);
	out[2] = (unsigned char) ((hdr >> 16) & 0xFF);
	out[3] = (unsigned char) ((hdr >> 24) & 0xFF);
	/* pq_sendint32(&buf, VARBITLEN(s)) */
	out[4] = (unsigned char) (((uint32) bitlen >> 24) & 0xFF);
	out[5] = (unsigned char) (((uint32) bitlen >> 16) & 0xFF);
	out[6] = (unsigned char) (((uint32) bitlen >> 8) & 0xFF);
	out[7] = (unsigned char) ((uint32) bitlen & 0xFF);
	/* pq_sendbytes(&buf, VARBITS(s), VARBITBYTES(s)) */
	memcpy(out + 8, bits, bytelen);
	return total;
}

/* ---------------- bit() / varbit() length coercions ---------------- */
/* return 0 = identity (C returns the arg unchanged; nothing written),
 *        1 = wrote an image of len bits into r, -1 = error. */

int
pg_bit_coerce(const bits8 *bits, int bytelen, int bitlen,
			  int32 len, int isExplicit, bits8 *r, int *err)
{
	int			rbytes;

	*err = 0;
	/* No work if typmod is invalid or supplied data matches it already */
	if (len <= 0 || len > VARBITMAXLEN || len == bitlen)
		return 0;

	if (!isExplicit)
	{
		*err = 2;				/* length does not match type bit(n) */
		return -1;
	}

	rbytes = PG_VARBITBYTES(len);
	memset(r, 0, rbytes);		/* palloc0 */

	memcpy(r, bits, (rbytes < bytelen) ? rbytes : bytelen);	/* Min */

	/* VARBIT_PAD(result) expanded */
	{
		int			pad_ = rbytes * BITS_PER_BYTE - len;

		if (pad_ > 0)
			r[rbytes - 1] &= BITMASK << pad_;
	}

	return 1;
}

int
pg_varbit_coerce(const bits8 *bits, int bytelen, int bitlen,
				 int32 len, int isExplicit, bits8 *r, int *err)
{
	int			rbytes;

	(void) bytelen;
	*err = 0;
	/* No work if typmod is invalid or supplied data matches it already */
	if (len <= 0 || len >= bitlen)
		return 0;

	if (!isExplicit)
	{
		*err = 2;				/* too long for type bit varying(n) */
		return -1;
	}

	rbytes = PG_VARBITBYTES(len);

	memcpy(r, bits, rbytes);	/* VARBITBYTES(result) */

	{
		int			pad_ = rbytes * BITS_PER_BYTE - len;

		if (pad_ > 0)
			r[rbytes - 1] &= BITMASK << pad_;
	}

	return 1;
}

/* ---------------- bit_catenate ---------------- */
/* returns the result bitlen, or -1 with *err = 1 (length exceeds max). */

int
pg_bit_catenate(const bits8 *bits1, int bytelen1, int bitlen1,
				const bits8 *bits2, int bytelen2, int bitlen2,
				bits8 *result, int *err)
{
	int			bit1pad,
				bit2shift;
	bits8	   *pr;
	const bits8 *pa;
	int			rbytes;

	*err = 0;
	if (bitlen1 > VARBITMAXLEN - bitlen2)
	{
		*err = 1;
		return -1;
	}
	rbytes = PG_VARBITBYTES(bitlen1 + bitlen2);

	/* Copy the first bitstring in */
	memcpy(result, bits1, bytelen1);

	/* Copy the second bit string */
	bit1pad = bytelen1 * BITS_PER_BYTE - bitlen1;	/* VARBITPAD(arg1) */
	if (bit1pad == 0)
	{
		memcpy(result + bytelen1, bits2, bytelen2);
	}
	else if (bitlen2 > 0)
	{
		bit2shift = BITS_PER_BYTE - bit1pad;
		pr = result + bytelen1 - 1;
		for (pa = bits2; pa < bits2 + bytelen2; pa++)
		{
			*pr |= ((*pa >> bit2shift) & BITMASK);
			pr++;
			if (pr < result + rbytes)
				*pr = (*pa << bit1pad) & BITMASK;
		}
	}

	/* The pad bits should be already zero at this point */

	return bitlen1 + bitlen2;
}

/* ---------------- bitsubstring ---------------- */
/* writes the result bits into r, bitlen into *rbitlen; -1/*err=6 on the
 * negative-length error. */

int
pg_bitsubstring(const bits8 *bits, int bytelen, int bitlen_arg,
				int32 s, int32 l, int length_not_specified,
				bits8 *r_out, int *rbitlen, int *err)
{
	int			bitlen,
				rbitlen_,
				len,
				ishift,
				i;
	int32		e,
				s1,
				e1;
	bits8	   *r;
	const bits8 *ps;

	*err = 0;
	bitlen = bitlen_arg;
	s1 = (s > 1) ? s : 1;		/* Max(s, 1) */
	if (length_not_specified)
	{
		e1 = bitlen + 1;
	}
	else if (l < 0)
	{
		*err = 6;				/* negative substring length not allowed */
		return -1;
	}
	else if (pg_add_s32_overflow(s, l, &e))
	{
		e1 = bitlen + 1;
	}
	else
	{
		e1 = (e < bitlen + 1) ? e : bitlen + 1;	/* Min */
	}
	if (s1 > bitlen || e1 <= s1)
	{
		/* Need to return a zero-length bitstring */
		*rbitlen = 0;
		return 0;
	}
	else
	{
		rbitlen_ = e1 - s1;
		*rbitlen = rbitlen_;
		len = PG_VARBITBYTES(rbitlen_);	/* payload bytes */
		if ((s1 - 1) % BITS_PER_BYTE == 0)
		{
			memcpy(r_out, bits + (s1 - 1) / BITS_PER_BYTE, len);
		}
		else
		{
			ishift = (s1 - 1) % BITS_PER_BYTE;
			r = r_out;
			ps = bits + (s1 - 1) / BITS_PER_BYTE;
			for (i = 0; i < len; i++)
			{
				*r = (*ps << ishift) & BITMASK;
				if ((++ps) < bits + bytelen)
					*r |= *ps >> (BITS_PER_BYTE - ishift);
				r++;
			}
		}

		/* VARBIT_PAD(result) expanded */
		{
			int			pad_ = len * BITS_PER_BYTE - rbitlen_;

			if (pad_ > 0)
				r_out[len - 1] &= BITMASK << pad_;
		}
	}

	return 0;
}

/* ---------------- bit_overlay ---------------- */
/* Composed exactly as C: substring/substring/catenate/catenate. Temp
 * buffers sized for the harness caps (<= 8 payload bytes per input). */

int
pg_bit_overlay(const bits8 *t1, int y1, int l1,
			   const bits8 *t2, int y2, int l2,
			   int32 sp, int32 sl,
			   bits8 *result, int *rbitlen, int *err)
{
	bits8		s1[16],
				s2[16],
				head[32];
	int			s1len,
				s2len,
				headlen;
	int			sp_pl_sl;
	int			st;

	*err = 0;
	if (sp <= 0)
	{
		*err = 6;				/* negative substring length not allowed */
		return -1;
	}
	if (pg_add_s32_overflow(sp, sl, &sp_pl_sl))
	{
		*err = 7;				/* integer out of range */
		return -1;
	}

	st = pg_bitsubstring(t1, y1, l1, 1, sp - 1, false, s1, &s1len, err);
	if (st != 0)
		return -1;
	st = pg_bitsubstring(t1, y1, l1, sp_pl_sl, -1, true, s2, &s2len, err);
	if (st != 0)
		return -1;
	headlen = pg_bit_catenate(s1, PG_VARBITBYTES(s1len), s1len,
							  t2, y2, l2, head, err);
	if (headlen < 0)
		return -1;
	*rbitlen = pg_bit_catenate(head, PG_VARBITBYTES(headlen), headlen,
							   s2, PG_VARBITBYTES(s2len), s2len,
							   result, err);
	if (*rbitlen < 0)
		return -1;
	return 0;
}

/* ---------------- bit_bit_count ---------------- */
/* pg_popcount portable per-byte table walk (src/port/pg_bitutils.c); the
 * word-level chunking is a performance path proved equivalent in
 * proofs/bitutils — the byte-table walk is the semantic body. */

typedef unsigned char uint8;

static const uint8 pg_number_of_ones[256] = {
	0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4,
	1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
	1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
	1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
	3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
	4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8
};

int64
pg_bit_bit_count(const bits8 *bits, int bytelen)
{
	int64		popcnt = 0;
	const bits8 *buf = bits;
	int			bytes = bytelen;

	while (bytes--)
		popcnt += pg_number_of_ones[(unsigned char) *buf++];

	return popcnt;
}

/* ---------------- bitfromint4 / bitfromint8 ---------------- */
/* palloc -> caller buffer (C writes every payload byte); returns the
 * result bitlen (the clamped typmod). */

int
pg_bitfromint4(int32 a, int32 typmod, bits8 *result)
{
	bits8	   *r;
	int			destbitsleft,
				srcbitsleft;

	if (typmod <= 0 || typmod > VARBITMAXLEN)
		typmod = 1;				/* default bit length */

	r = result;
	destbitsleft = typmod;
	srcbitsleft = 32;
	/* drop any input bits that don't fit */
	srcbitsleft = (srcbitsleft < destbitsleft) ? srcbitsleft : destbitsleft;
	/* sign-fill any excess bytes in output */
	while (destbitsleft >= srcbitsleft + 8)
	{
		*r++ = (bits8) ((a < 0) ? BITMASK : 0);
		destbitsleft -= 8;
	}
	/* store first fractional byte */
	if (destbitsleft > srcbitsleft)
	{
		unsigned int val = (unsigned int) (a >> (destbitsleft - 8));

		/* Force sign-fill in case the compiler implements >> as zero-fill */
		if (a < 0)
			val |= ((unsigned int) -1) << (srcbitsleft + 8 - destbitsleft);
		*r++ = (bits8) (val & BITMASK);
		destbitsleft -= 8;
	}
	/* store whole bytes */
	while (destbitsleft >= 8)
	{
		*r++ = (bits8) ((a >> (destbitsleft - 8)) & BITMASK);
		destbitsleft -= 8;
	}
	/* store last fractional byte */
	if (destbitsleft > 0)
		*r = (bits8) ((a << (8 - destbitsleft)) & BITMASK);

	return typmod;
}

int
pg_bitfromint8(int64 a, int32 typmod, bits8 *result)
{
	bits8	   *r;
	int			destbitsleft,
				srcbitsleft;

	if (typmod <= 0 || typmod > VARBITMAXLEN)
		typmod = 1;				/* default bit length */

	r = result;
	destbitsleft = typmod;
	srcbitsleft = 64;
	/* drop any input bits that don't fit */
	srcbitsleft = (srcbitsleft < destbitsleft) ? srcbitsleft : destbitsleft;
	/* sign-fill any excess bytes in output */
	while (destbitsleft >= srcbitsleft + 8)
	{
		*r++ = (bits8) ((a < 0) ? BITMASK : 0);
		destbitsleft -= 8;
	}
	/* store first fractional byte */
	if (destbitsleft > srcbitsleft)
	{
		unsigned int val = (unsigned int) (a >> (destbitsleft - 8));

		/* Force sign-fill in case the compiler implements >> as zero-fill */
		if (a < 0)
			val |= ((unsigned int) -1) << (srcbitsleft + 8 - destbitsleft);
		*r++ = (bits8) (val & BITMASK);
		destbitsleft -= 8;
	}
	/* store whole bytes */
	while (destbitsleft >= 8)
	{
		*r++ = (bits8) ((a >> (destbitsleft - 8)) & BITMASK);
		destbitsleft -= 8;
	}
	/* store last fractional byte */
	if (destbitsleft > 0)
		*r = (bits8) ((a << (8 - destbitsleft)) & BITMASK);

	return typmod;
}

/* ---------------- bitsetbit / bitgetbit ---------------- */

int
pg_bitsetbit(const bits8 *bits, int bytelen, int bitlen,
			 int32 n, int32 newBit, bits8 *r, int *err)
{
	int			byteNo,
				bitNo;

	*err = 0;
	if (n < 0 || n >= bitlen)
	{
		*err = 8;				/* bit index out of valid range */
		return -1;
	}

	/*
	 * sanity check!
	 */
	if (newBit != 0 && newBit != 1)
	{
		*err = 9;				/* new bit must be 0 or 1 */
		return -1;
	}

	memcpy(r, bits, bytelen);

	byteNo = n / BITS_PER_BYTE;
	bitNo = BITS_PER_BYTE - 1 - (n % BITS_PER_BYTE);

	/*
	 * Update the byte.
	 */
	if (newBit == 0)
		r[byteNo] &= (~(1 << bitNo));
	else
		r[byteNo] |= (1 << bitNo);

	return 0;
}

int32
pg_bitgetbit(const bits8 *bits, int bytelen, int bitlen,
			 int32 n, int *err)
{
	int			byteNo,
				bitNo;

	(void) bytelen;
	*err = 0;
	if (n < 0 || n >= bitlen)
	{
		*err = 8;				/* bit index out of valid range */
		return 0;
	}

	byteNo = n / BITS_PER_BYTE;
	bitNo = BITS_PER_BYTE - 1 - (n % BITS_PER_BYTE);

	if (bits[byteNo] & (1 << bitNo))
		return 1;
	else
		return 0;
}

/* ---------------- bitposition ---------------- */

int32
pg_bitposition(const bits8 *str_bits, int str_bytes, int str_len,
			   const bits8 *sub_bits, int sub_bytes, int sub_len)
{
	int			substr_length,
				str_length,
				i,
				is;
	const bits8 *s,
			   *p;
	bits8		cmp,
				mask1,
				mask2,
				end_mask,
				str_mask;
	bool_c		is_match;

	substr_length = sub_len;
	str_length = str_len;

	/* String has zero length or substring longer than string, return 0 */
	if ((str_length == 0) || (substr_length > str_length))
		return 0;

	/* zero-length substring means return 1 */
	if (substr_length == 0)
		return 1;

	/* Initialise the padding masks */
	end_mask = BITMASK << (sub_bytes * BITS_PER_BYTE - sub_len);
	str_mask = BITMASK << (str_bytes * BITS_PER_BYTE - str_len);
	for (i = 0; i < str_bytes - sub_bytes + 1; i++)
	{
		for (is = 0; is < BITS_PER_BYTE; is++)
		{
			is_match = true;
			p = str_bits + i;
			mask1 = BITMASK >> is;
			mask2 = ~mask1;
			for (s = sub_bits;
				 is_match && s < sub_bits + sub_bytes; s++)
			{
				cmp = *s >> is;
				if (s == sub_bits + sub_bytes - 1)
				{
					mask1 &= end_mask >> is;
					if (p == str_bits + str_bytes - 1)
					{
						/* Check that there is enough of str left */
						if (mask1 & ~str_mask)
						{
							is_match = false;
							break;
						}
						mask1 &= str_mask;
					}
				}
				is_match = ((cmp ^ *p) & mask1) == 0;
				if (!is_match)
					break;
				/* Move on to the next byte */
				p++;
				if (p == str_bits + str_bytes)
				{
					mask2 = end_mask << (BITS_PER_BYTE - is);
					is_match = mask2 == 0;
					break;
				}
				cmp = *s << (BITS_PER_BYTE - is);
				if (s == sub_bits + sub_bytes - 1)
				{
					mask2 &= end_mask << (BITS_PER_BYTE - is);
					if (p == str_bits + str_bytes - 1)
					{
						if (mask2 & ~str_mask)
						{
							is_match = false;
							break;
						}
						mask2 &= str_mask;
					}
				}
				is_match = ((cmp ^ *p) & mask2) == 0;
			}
			/* Have we found a match? */
			if (is_match)
				return i * BITS_PER_BYTE + is + 1;
		}
	}
	return 0;
}
