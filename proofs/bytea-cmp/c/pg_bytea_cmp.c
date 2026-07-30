/*
 * Vendored PostgreSQL C for the bytea comparator-family proofs.
 *
 * Provenance: fetched 2026-07-28 from postgres/postgres master
 * src/backend/utils/adt/bytea.c (byteaeq..byteacmp, lines ~813-980).
 * REL_18_STABLE ref: src/backend/utils/adt/varlena.c (byteaeq..byteacmp,
 * lines ~3918-4062) — REL_18 keeps these functions in varlena.c; the
 * bytea.c split is master-era (post-18). Bodies byte-identical, zero code
 * drift (provenance audit, proofs/PROVENANCE-AUDIT.md, 2026-07-28).
 *
 * SHIMS (everything else is verbatim):
 *  - names pg_-prefixed; postgres typedefs inlined (Size -> size_t,
 *    int32 -> int); Min() and VARHDRSZ defined per c.h / varatt.h.
 *  - DETOASTING IS OUT OF SCOPE.  The fmgr wrappers operate on possibly
 *    toasted varlena; the C caller contract post-PG_GETARG_BYTEA_PP is a
 *    detoasted (possibly short-header) varlena, from which the body only
 *    ever uses VARDATA_ANY (payload pointer) and VARSIZE_ANY_EXHDR
 *    (payload length).  Each function is therefore shimmed to plain
 *    (const unsigned char *data, len) pairs:
 *      PG_GETARG_BYTEA_PP(n) + VARDATA_ANY / VARSIZE_ANY_EXHDR
 *        -> (dN, lenN) parameters
 *      byteaeq/byteane's toast_raw_datum_size(argN)
 *        -> lenN + VARHDRSZ  (raw size = payload + 4-byte header; the
 *           fast-path inequality test and the later `len1 - VARHDRSZ`
 *           memcmp count are kept verbatim)
 *      PG_FREE_IF_COPY -> dropped (memory management, no value effect)
 *      PG_RETURN_BOOL  -> int return (0/1); Kani lowers Rust bool/() in
 *                         ways goto-cc rejects against C _Bool/void
 *      PG_RETURN_INT32 -> int return
 *  - memcmp is CBMC's built-in model (byte loop returning the difference
 *    of the first mismatching unsigned chars — the glibc convention the
 *    shipped Rust core documents at varlena/src/lib.rs:122).
 */

#include <stddef.h>
#include <string.h>

#define Min(x, y) ((x) < (y) ? (x) : (y))
#define VARHDRSZ ((size_t) 4)

int
pg_byteaeq(const unsigned char *d1, size_t rawlen1_exhdr,
		   const unsigned char *d2, size_t rawlen2_exhdr)
{
	int			result;			/* shim: bool -> int */
	size_t		len1,
				len2;

	/*
	 * We can use a fast path for unequal lengths, which might save us from
	 * having to detoast one or both values.
	 */
	len1 = rawlen1_exhdr + VARHDRSZ;	/* shim: toast_raw_datum_size(arg1) */
	len2 = rawlen2_exhdr + VARHDRSZ;	/* shim: toast_raw_datum_size(arg2) */
	if (len1 != len2)
		result = 0;
	else
	{
		result = (memcmp(d1, d2, len1 - VARHDRSZ) == 0);
	}

	return result;
}

int
pg_byteane(const unsigned char *d1, size_t rawlen1_exhdr,
		   const unsigned char *d2, size_t rawlen2_exhdr)
{
	int			result;			/* shim: bool -> int */
	size_t		len1,
				len2;

	/*
	 * We can use a fast path for unequal lengths, which might save us from
	 * having to detoast one or both values.
	 */
	len1 = rawlen1_exhdr + VARHDRSZ;	/* shim: toast_raw_datum_size(arg1) */
	len2 = rawlen2_exhdr + VARHDRSZ;	/* shim: toast_raw_datum_size(arg2) */
	if (len1 != len2)
		result = 1;
	else
	{
		result = (memcmp(d1, d2, len1 - VARHDRSZ) != 0);
	}

	return result;
}

int
pg_bytealt(const unsigned char *d1, int len1,
		   const unsigned char *d2, int len2)
{
	int			cmp;

	cmp = memcmp(d1, d2, Min(len1, len2));

	return (cmp < 0) || ((cmp == 0) && (len1 < len2));
}

int
pg_byteale(const unsigned char *d1, int len1,
		   const unsigned char *d2, int len2)
{
	int			cmp;

	cmp = memcmp(d1, d2, Min(len1, len2));

	return (cmp < 0) || ((cmp == 0) && (len1 <= len2));
}

int
pg_byteagt(const unsigned char *d1, int len1,
		   const unsigned char *d2, int len2)
{
	int			cmp;

	cmp = memcmp(d1, d2, Min(len1, len2));

	return (cmp > 0) || ((cmp == 0) && (len1 > len2));
}

int
pg_byteage(const unsigned char *d1, int len1,
		   const unsigned char *d2, int len2)
{
	int			cmp;

	cmp = memcmp(d1, d2, Min(len1, len2));

	return (cmp > 0) || ((cmp == 0) && (len1 >= len2));
}

int
pg_byteacmp(const unsigned char *d1, int len1,
			const unsigned char *d2, int len2)
{
	int			cmp;

	cmp = memcmp(d1, d2, Min(len1, len2));
	if ((cmp == 0) && (len1 != len2))
		cmp = (len1 < len2) ? -1 : 1;

	return cmp;
}

/*
 * bytea_larger / bytea_smaller (pg_proc oids 6393/6394).
 *
 * Provenance: src/backend/utils/adt/varlena.c, postgres/postgres
 * REL_18_STABLE, fetched 2026-07-28.
 *
 * SHIMS (comparison/selection expressions verbatim):
 *  - same (data, len) pair shim as the comparators above;
 *  - the C function returns the WINNING INPUT POINTER
 *    (PG_RETURN_BYTEA_P(result) where result is arg1 or arg2); shimmed to
 *    return 1 when result == arg1 and 2 when result == arg2, so the
 *    harness can assert winning-input identity against the Rust
 *    reference-returning core.
 */

int
pg_bytea_larger(const unsigned char *d1, int len1,
				const unsigned char *d2, int len2)
{
	int			cmp;

	cmp = memcmp(d1, d2, Min(len1, len2));

	return ((cmp > 0) || ((cmp == 0) && (len1 > len2)) ? 1 : 2);
}

int
pg_bytea_smaller(const unsigned char *d1, int len1,
				 const unsigned char *d2, int len2)
{
	int			cmp;

	cmp = memcmp(d1, d2, Min(len1, len2));

	return ((cmp < 0) || ((cmp == 0) && (len1 < len2)) ? 1 : 2);
}

/*
 * byteaGetByte / byteaGetBit / byteaSetByte / byteaSetBit
 * (pg_proc oids 721 / 723 / 722 / 724 — extraction-gap wave 2026-07-28).
 *
 * Provenance: src/backend/utils/adt/varlena.c, postgres/postgres
 * REL_18_STABLE, lines ~3305-3455, fetched 2026-07-28.  (REL_18 keeps
 * these in varlena.c; the bytea.c split is master-era.)
 *
 * SHIMS (all logic verbatim; this list is exhaustive):
 *  - shared typedefs via pg_proof_shim.h (int32/int64; its Min/VARHDRSZ
 *    redefinitions are token-identical to the ones above — benign).
 *  - Get*: PG_GETARG_BYTEA_PP(0) + VARSIZE_ANY_EXHDR(v)/VARDATA_ANY(v)
 *    -> (vdata, len) parameters, same pre-detoasted caller contract as the
 *    comparator shims above.  PG_GETARG_INT32/INT64 -> plain args;
 *    PG_RETURN_INT32 -> int return.
 *  - Set*: PG_GETARG_BYTEA_P_COPY(0) makes C mutate a private copy and
 *    return it; shimmed to a caller-provided mutable payload buffer `res`
 *    that the HARNESS pre-fills with the input bytes (the copy), with
 *    `len = VARSIZE(res) - VARHDRSZ` -> len parameter.  VARDATA(res) ->
 *    res.  PG_RETURN_BYTEA_P(res) -> return 0 (the result IMAGE is the
 *    mutated buffer, byte-compared by the harness); C's returned image
 *    length == input length is represented by the buffer having exactly
 *    len bytes.
 *  - ereport(ERROR, ...) -> PROOF_EREPORT_FLAG out-param + early return 0
 *    at the exact ereport program point (message text never crosses the
 *    seam).  Per the shim-header convention, distinct flag values encode
 *    the errcode: *err = 1 for ERRCODE_ARRAY_SUBSCRIPT_ERROR (2202E),
 *    *err = 2 for ERRCODE_INVALID_PARAMETER_VALUE (22023, byteaSetBit's
 *    "new bit must be 0 or 1").
 *  - THEOREM PLANES kept verbatim and in-proof: byteaSetByte's
 *    `((unsigned char *) VARDATA(res))[n] = newByte;` int->unsigned char
 *    truncating store (matches Rust `new_byte as u8`); byteaGetBit/
 *    SetBit's `(int64) len * 8` widening and n/8, n%8 index math; the
 *    range-check-THEN-bit-value-check order in byteaSetBit.
 */

#include "../../support/c/pg_proof_shim.h"

int
pg_byteaGetByte(const unsigned char *vdata, int len, int32 n, int *err)
{
	int			byte;

	/* shim: len = VARSIZE_ANY_EXHDR(v) */

	if (n < 0 || n >= len)
	{
		/* shim: ereport(ERROR, errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
		 * errmsg("index %d out of valid range, 0..%d", n, len - 1)) */
		*err = 1;
		return 0;
	}

	byte = vdata[n];			/* shim: ((unsigned char *) VARDATA_ANY(v))[n] */

	return byte;
}

int
pg_byteaGetBit(const unsigned char *vdata, int len, int64 n, int *err)
{
	int			byteNo,
				bitNo;
	int			byte;

	/* shim: len = VARSIZE_ANY_EXHDR(v) */

	if (n < 0 || n >= (int64) len * 8)
	{
		/* shim: ereport(ERROR, errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
		 * errmsg("index %PRId64 out of valid range, 0..%PRId64",
		 * n, (int64) len * 8 - 1)) */
		*err = 1;
		return 0;
	}

	/* n/8 is now known < len, so safe to cast to int */
	byteNo = (int) (n / 8);
	bitNo = (int) (n % 8);

	byte = vdata[byteNo];		/* shim: ((unsigned char *) VARDATA_ANY(v))[byteNo] */

	if (byte & (1 << bitNo))
		return 1;				/* shim: PG_RETURN_INT32(1) */
	else
		return 0;				/* shim: PG_RETURN_INT32(0) */
}

int
pg_byteaSetByte(unsigned char *res, int len, int32 n, int32 newByte, int *err)
{
	/* shim: res = payload of PG_GETARG_BYTEA_P_COPY(0);
	 * len = VARSIZE(res) - VARHDRSZ */

	if (n < 0 || n >= len)
	{
		/* shim: ereport(ERROR, errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR), ...) */
		*err = 1;
		return 0;
	}

	/*
	 * Now set the byte.
	 */
	res[n] = newByte;			/* shim: ((unsigned char *) VARDATA(res))[n]
								 * = newByte; — truncating store in-theorem */

	return 0;					/* shim: PG_RETURN_BYTEA_P(res) */
}

int
pg_byteaSetBit(unsigned char *res, int len, int64 n, int32 newBit, int *err)
{
	int			oldByte,
				newByte;
	int			byteNo,
				bitNo;

	/* shim: res = payload of PG_GETARG_BYTEA_P_COPY(0);
	 * len = VARSIZE(res) - VARHDRSZ */

	if (n < 0 || n >= (int64) len * 8)
	{
		/* shim: ereport(ERROR, errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR), ...) */
		*err = 1;
		return 0;
	}

	/* n/8 is now known < len, so safe to cast to int */
	byteNo = (int) (n / 8);
	bitNo = (int) (n % 8);

	/*
	 * sanity check!
	 */
	if (newBit != 0 && newBit != 1)
	{
		/* shim: ereport(ERROR, errcode(ERRCODE_INVALID_PARAMETER_VALUE),
		 * errmsg("new bit must be 0 or 1")) */
		*err = 2;
		return 0;
	}

	/*
	 * Update the byte.
	 */
	oldByte = res[byteNo];		/* shim: ((unsigned char *) VARDATA(res))[byteNo] */

	if (newBit == 0)
		newByte = oldByte & (~(1 << bitNo));
	else
		newByte = oldByte | (1 << bitNo);

	res[byteNo] = newByte;		/* shim: ((unsigned char *) VARDATA(res))[byteNo] */

	return 0;					/* shim: PG_RETURN_BYTEA_P(res) */
}

/* ====================================================================
 * bytea <-> int casts (varbit W10 continuation, 2026-07-30)
 *
 * Provenance: fetched 2026-07-30 from postgres/postgres REL_18_STABLE
 * src/backend/utils/adt/varlena.c — bytea_int4 (~line 4163), bytea_int8
 * (~4188), int4_bytea (~4219), int8_bytea (~4226).
 *
 * SHIMS (bodies otherwise verbatim):
 *  - family (data,len) convention replaces PG_GETARG_BYTEA_PP +
 *    VARDATA_ANY/VARSIZE_ANY_EXHDR (detoasting out of scope, as above);
 *  - ereport(ERROR, errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), ...)
 *    -> *err = 1 + return 0 sentinel (PROOF_EREPORT_FLAG convention);
 *  - BITS_PER_BYTE defined (c.h);
 *  - int4_bytea/int8_bytea are literally `return int4send(fcinfo)` /
 *    int8send: pq_begintypsend + pq_sendint32/64 + pq_endtypsend build a
 *    bytea whose payload is the 4/8-byte BIG-ENDIAN image of the value.
 *    The shim writes that payload into a caller buffer (StringInfo ->
 *    fixed caller buffer per the allowed-shim list); the varlena header
 *    is the same one integer both sides and is asserted at the harness
 *    level (varsize == VARHDRSZ + 4/8), matching the family image
 *    convention. pq_sendintN big-endian stores spelled as the explicit
 *    shift/mask bytes (pg_hton32/64 on little-endian hosts).
 */
#define BITS_PER_BYTE 8

int
pg_bytea_int4(const unsigned char *d, int len, int *err)
{
	unsigned int result;		/* uint32 */

	/* Check that the byte array is not too long */
	if (len > (int) sizeof(result))
	{
		/* shim: ereport(ERROR, errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
		 * errmsg("integer out of range")) */
		*err = 1;
		return 0;
	}

	/* Convert it to an integer; most significant bytes come first */
	result = 0;
	for (int i = 0; i < len; i++)
	{
		result <<= BITS_PER_BYTE;
		result |= d[i];			/* shim: ((unsigned char *) VARDATA_ANY(v))[i] */
	}

	return (int) result;		/* shim: PG_RETURN_INT32(result) */
}

long long
pg_bytea_int8(const unsigned char *d, int len, int *err)
{
	unsigned long long result;	/* uint64 */

	/* Check that the byte array is not too long */
	if (len > (int) sizeof(result))
	{
		/* shim: ereport(ERROR, errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
		 * errmsg("bigint out of range")) */
		*err = 1;
		return 0;
	}

	/* Convert it to an integer; most significant bytes come first */
	result = 0;
	for (int i = 0; i < len; i++)
	{
		result <<= BITS_PER_BYTE;
		result |= d[i];			/* shim: ((unsigned char *) VARDATA_ANY(v))[i] */
	}

	return (long long) result;	/* shim: PG_RETURN_INT64(result) */
}

/* int4_bytea = int4send: pq_sendint32(&buf, arg) payload image */
int
pg_int4_bytea(int a, unsigned char *out4)
{
	unsigned int u = (unsigned int) a;

	/* pq_sendint32: network (big-endian) byte order */
	out4[0] = (unsigned char) ((u >> 24) & 0xFF);
	out4[1] = (unsigned char) ((u >> 16) & 0xFF);
	out4[2] = (unsigned char) ((u >> 8) & 0xFF);
	out4[3] = (unsigned char) (u & 0xFF);
	return 0;
}

/* int8_bytea = int8send: pq_sendint64(&buf, arg) payload image */
int
pg_int8_bytea(long long a, unsigned char *out8)
{
	unsigned long long u = (unsigned long long) a;

	/* pq_sendint64: network (big-endian) byte order */
	out8[0] = (unsigned char) ((u >> 56) & 0xFF);
	out8[1] = (unsigned char) ((u >> 48) & 0xFF);
	out8[2] = (unsigned char) ((u >> 40) & 0xFF);
	out8[3] = (unsigned char) ((u >> 32) & 0xFF);
	out8[4] = (unsigned char) ((u >> 24) & 0xFF);
	out8[5] = (unsigned char) ((u >> 16) & 0xFF);
	out8[6] = (unsigned char) ((u >> 8) & 0xFF);
	out8[7] = (unsigned char) (u & 0xFF);
	return 0;
}

/* ====================================================================
 * bytea_int2 / int2_bytea siblings + bytea_bit_count + bytea_reverse
 * (varbit W10 continuation, 2026-07-30)
 *
 * Provenance: REL_18_STABLE varlena.c bytea_int2 (~line 4137),
 * int2_bytea (~4212), bytea_bit_count (~3252, delegating
 * src/port/pg_bitutils.c pg_popcount portable table walk — table + walk
 * vendored verbatim, same as proofs/varbit-rows pg_bit_bit_count),
 * bytea_reverse (~3461). Shims as documented for the int4/int8 pair
 * above (family (data,len) convention; ereport -> err flag; pq_sendint16
 * big-endian store; palloc image -> caller buffer for reverse).
 */

static const uint8 pg_number_of_ones_bc[256] = {
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

long long
pg_bytea_bit_count(const unsigned char *d, int len)
{
	/* pg_popcount portable path: table byte walk */
	long long	popcnt = 0;
	const unsigned char *buf = d;
	int			bytes = len;

	while (bytes--)
		popcnt += pg_number_of_ones_bc[(unsigned char) *buf++];

	return popcnt;			/* shim: PG_RETURN_INT64(pg_popcount(...)) */
}

short
pg_bytea_int2(const unsigned char *d, int len, int *err)
{
	unsigned short result;		/* uint16 */

	/* Check that the byte array is not too long */
	if (len > (int) sizeof(result))
	{
		/* shim: ereport(ERROR, errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
		 * errmsg("smallint out of range")) */
		*err = 1;
		return 0;
	}

	/* Convert it to an integer; most significant bytes come first */
	result = 0;
	for (int i = 0; i < len; i++)
	{
		result <<= BITS_PER_BYTE;
		result |= d[i];			/* shim: ((unsigned char *) VARDATA_ANY(v))[i] */
	}

	return (short) result;		/* shim: PG_RETURN_INT16(result) */
}

/* int2_bytea = int2send: pq_sendint16(&buf, arg) payload image */
int
pg_int2_bytea(short a, unsigned char *out2)
{
	unsigned short u = (unsigned short) a;

	/* pq_sendint16: network (big-endian) byte order */
	out2[0] = (unsigned char) ((u >> 8) & 0xFF);
	out2[1] = (unsigned char) (u & 0xFF);
	return 0;
}

/* bytea_reverse: palloc'd same-length image, bytes reversed.
 * REL_18_STABLE varlena.c:
 *   const char *p = VARDATA_ANY(v);
 *   ... char *dst = VARDATA(result) + VARSIZE(result) - VARHDRSZ;
 *   while (p < endp) *(--dst) = *p++;
 * Shim: result payload -> caller buffer (len bytes). */
int
pg_bytea_reverse(const unsigned char *d, int len, unsigned char *out)
{
	const unsigned char *p = d;
	const unsigned char *endp = d + len;
	unsigned char *dst = out + len;

	while (p < endp)
		*(--dst) = *p++;

	return 0;					/* shim: PG_RETURN_BYTEA_P(result) */
}

/* ====================================================================
 * byteain — traditional escaped arm (varbit W10 continuation 2026-07-30)
 *
 * Provenance: REL_18_STABLE varlena.c byteain (~line 299), fetched
 * 2026-07-30. The hex arm ("\x" prefix) delegates hex_decode_safe and is
 * FENCED OUT by the harness (hex decode is separately proved in
 * proofs/bytea-varbit); only the escaped-style two-pass body is vendored.
 *
 * SHIMS (bodies otherwise verbatim):
 *  - PG_GETARG_CSTRING -> const char *inputText (NUL-terminated; the
 *    harness appends the NUL and fences interior NULs per the cstring
 *    contract);
 *  - ereturn(escontext, ...) invalid input syntax -> *err = 1 + return
 *    (PROOF_EREPORT_FLAG convention; 22P02);
 *  - palloc(bc) result image -> caller buffer `out` (payload only);
 *    SET_VARSIZE -> *outlen (the same integer the harness checks against
 *    Rust's varsize);
 *  - VAL(CH) macro from varlena.c: ((CH) - '0').
 */
#define VAL(CH) ((CH) - '0')

int
pg_byteain_esc(const char *inputText, unsigned char *out, int *outlen, int *err)
{
	const char *tp;
	unsigned char *rp;
	int			bc;

	/* Else, it's the traditional escaped style */
	for (bc = 0, tp = inputText; *tp != '\0'; bc++)
	{
		if (tp[0] != '\\')
			tp++;
		else if ((tp[0] == '\\') &&
				 (tp[1] >= '0' && tp[1] <= '3') &&
				 (tp[2] >= '0' && tp[2] <= '7') &&
				 (tp[3] >= '0' && tp[3] <= '7'))
			tp += 4;
		else if ((tp[0] == '\\') &&
				 (tp[1] == '\\'))
			tp += 2;
		else
		{
			/*
			 * one backslash, not followed by another or ### valid octal
			 */
			/* shim: ereturn(escontext, ..., ERRCODE_INVALID_TEXT_REPRESENTATION,
			 * "invalid input syntax for type bytea") */
			*err = 1;
			return 0;
		}
	}

	*outlen = bc;				/* shim: bc += VARHDRSZ; SET_VARSIZE(result, bc) */

	tp = inputText;
	rp = out;					/* shim: rp = VARDATA(result) */
	while (*tp != '\0')
	{
		if (tp[0] != '\\')
			*rp++ = *tp++;
		else if ((tp[0] == '\\') &&
				 (tp[1] >= '0' && tp[1] <= '3') &&
				 (tp[2] >= '0' && tp[2] <= '7') &&
				 (tp[3] >= '0' && tp[3] <= '7'))
		{
			bc = VAL(tp[1]);
			bc <<= 3;
			bc += VAL(tp[2]);
			bc <<= 3;
			*rp++ = bc + VAL(tp[3]);

			tp += 4;
		}
		else if ((tp[0] == '\\') &&
				 (tp[1] == '\\'))
		{
			*rp++ = '\\';
			tp += 2;
		}

		/*
		 * We should never get here. The first pass should not allow it.
		 */
	}

	return 0;					/* shim: PG_RETURN_BYTEA_P(result) */
}

/* byteasend (REL_18_STABLE bytea_sendrecv section, varlena.c ~line 445):
 *   bytea *vlena = PG_GETARG_BYTEA_P_COPY(0);
 *   PG_RETURN_BYTEA_P(vlena);
 * The wire image IS the detoasted payload (identity copy). Shim: the
 * P_COPY copy -> caller buffer; header carried as the same integer both
 * sides (asserted at harness level as varsize == VARHDRSZ + len). */
int
pg_byteasend(const unsigned char *d, int len, unsigned char *out)
{
	int			i;

	for (i = 0; i < len; i++)	/* shim: PG_GETARG_BYTEA_P_COPY's memcpy */
		out[i] = d[i];
	return 0;					/* shim: PG_RETURN_BYTEA_P(vlena) */
}
