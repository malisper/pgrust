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

/* ================================================================
 * SCALAR-CAST / MINMAX / BIT-COUNT / TO-BASE WAVE (lane pick-a,
 * fetched 2026-07-30 from postgres/postgres REL_18_STABLE
 * src/backend/utils/adt/varlena.c: bytea_int2 (l.4139), bytea_int4
 * (l.4164), bytea_int8 (l.4189), bytea_larger (l.4084),
 * bytea_smaller (l.4103), bytea_bit_count (l.3254),
 * convert_to_base (l.5191); src/port/pg_bitutils.c:
 * pg_number_of_ones + pg_popcount_portable (l.104).
 *
 * SHIMS (bodies verbatim; everything shimmed is listed):
 *  - names pg_-prefixed; PG_GETARG_BYTEA_PP + VARDATA_ANY /
 *    VARSIZE_ANY_EXHDR -> (data, len) parameters (same detoasted
 *    caller contract as the comparator wave above).
 *  - postgres typedefs inlined: uint16/uint32/uint64 -> stdint
 *    equivalents; BITS_PER_BYTE = 8; Assert -> no-op.
 *  - ereport(ERROR, errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), ...)
 *    -> PROOF_EREPORT_FLAG convention: *err = 1, return 0.  Message
 *    text out of proof.
 *  - PG_RETURN_INT16/32/64 -> plain integer returns.
 *  - bytea_larger/smaller: PG_RETURN_BYTEA_P(result) -> WINNER
 *    IDENTITY return (1 if arg1 won, 2 if arg2) — the function
 *    returns one of its INPUT datums, so which datum won is the
 *    user-visible claim (winner-identity theorem).
 *  - bytea_bit_count: PG's pg_popcount() dispatch macro resolves to
 *    pg_popcount_portable (dispatch is a perf mechanism, value-
 *    identical by upstream contract); the SIZEOF_VOID_P>=8
 *    word-chunk block is compiled out (SIZEOF_VOID_P undefined
 *    here) — the proof domain len<=7 never enters it upstream
 *    either (word loop requires bytes >= 8).
 *  - convert_to_base: static inline dropped; cstring_to_text_with_len
 *    (allocation) -> caller-provided 64-byte frame `out` that the
 *    verbatim body writes through (buf -> out), returning the start
 *    offset ptr - out; the caller reads digits out[start..64].
 * ================================================================ */

#include <stdint.h>

typedef uint16_t uint16;
typedef uint32_t uint32;
typedef uint64_t uint64;
typedef uint8_t uint8;
#define BITS_PER_BYTE 8

/* Cast bytea -> int2 */
int16_t
pg_bytea_int2(const unsigned char *vdata, int len, int *err)
{
	uint16		result;

	/* Check that the byte array is not too long */
	if (len > sizeof(result))
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
		result |= vdata[i];		/* shim: ((unsigned char *) VARDATA_ANY(v))[i] */
	}

	return result;				/* shim: PG_RETURN_INT16(result) */
}

/* Cast bytea -> int4 */
int32_t
pg_bytea_int4(const unsigned char *vdata, int len, int *err)
{
	uint32		result;

	/* Check that the byte array is not too long */
	if (len > sizeof(result))
	{
		/* shim: ereport ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE "integer out of range" */
		*err = 1;
		return 0;
	}

	/* Convert it to an integer; most significant bytes come first */
	result = 0;
	for (int i = 0; i < len; i++)
	{
		result <<= BITS_PER_BYTE;
		result |= vdata[i];
	}

	return result;				/* shim: PG_RETURN_INT32(result) */
}

/* Cast bytea -> int8 */
int64_t
pg_bytea_int8(const unsigned char *vdata, int len, int *err)
{
	uint64		result;

	/* Check that the byte array is not too long */
	if (len > sizeof(result))
	{
		/* shim: ereport ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE "bigint out of range" */
		*err = 1;
		return 0;
	}

	/* Convert it to an integer; most significant bytes come first */
	result = 0;
	for (int i = 0; i < len; i++)
	{
		result <<= BITS_PER_BYTE;
		result |= vdata[i];
	}

	return result;				/* shim: PG_RETURN_INT64(result) */
}

/* bytea_larger / bytea_smaller: already vendored in the comparator wave
 * above (winner-identity shim) — not duplicated here. */

/* src/port/pg_bitutils.c pg_number_of_ones, verbatim */
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

/*
 * pg_popcount_portable
 *		Returns the number of 1-bits in buf
 */
static uint64
pg_popcount_portable(const char *buf, int bytes)
{
	uint64		popcnt = 0;

#if SIZEOF_VOID_P >= 8
	/* Process in 64-bit chunks if the buffer is aligned. */
	if (buf == (const char *) TYPEALIGN(8, buf))
	{
		const uint64 *words = (const uint64 *) buf;

		while (bytes >= 8)
		{
			popcnt += pg_popcount64(*words++);
			bytes -= 8;
		}

		buf = (const char *) words;
	}
#endif

	/* Process any remaining bytes */
	while (bytes--)
		popcnt += pg_number_of_ones[(unsigned char) *buf++];

	return popcnt;
}

/*
 * bit_count
 */
int64_t
pg_bytea_bit_count(const unsigned char *vdata, int len)
{
	/* shim: pg_popcount(VARDATA_ANY(t1), VARSIZE_ANY_EXHDR(t1)) dispatch
	 * -> pg_popcount_portable */
	return pg_popcount_portable((const char *) vdata, len);
}

/*
 * Workhorse for to_bin, to_oct, and to_hex.  Note that base must be > 1 and <=
 * 16.
 */
int
pg_convert_to_base(uint64 value, int base, unsigned char *out /* [64] */ )
{
	const char *digits = "0123456789abcdef";

	/* We size the buffer for to_bin's longest possible return value. */
	/* shim: char buf[sizeof(uint64) * BITS_PER_BYTE] -> caller frame `out` */
	char	   *const buf = (char *) out;
	char	   *const end = buf + sizeof(uint64) * BITS_PER_BYTE;
	char	   *ptr = end;

	/* Assert(base > 1); Assert(base <= 16);  shim: Assert -> no-op */

	do
	{
		*--ptr = digits[value % base];
		value /= base;
	} while (ptr > buf && value);

	/* shim: cstring_to_text_with_len(ptr, end - ptr) -> return start
	 * offset; caller reads out[start..64] */
	return (int) (ptr - buf);
}

/* ================================================================
 * INT -> BYTEA CAST WAVE (lane pick-a 2026-07-30, oids 6367/6368/6369).
 *
 * Provenance (fetched 2026-07-30, REL_18_STABLE):
 *  - varlena.c int2_bytea/int4_bytea/int8_bytea (l.4214-4230): each
 *    "can just use intNsend()".
 *  - int.c int2send/int4send, int8.c int8send.
 *  - libpq/pqformat.h pq_sendint16/32/64 + pq_writeint16/32/64.
 *  - port/pg_bswap.h pg_bswap16/32/64 shift fallbacks (verbatim);
 *    pg_htonN -> pg_bswapN is the little-endian arm of pg_bswap.h's
 *    "#ifndef WORDS_BIGENDIAN" — the proof pins the little-endian
 *    target platform (aarch64/x86-64), same as the shipped Rust's
 *    to_be_bytes.
 *
 * SHIMS (everything else verbatim):
 *  - StringInfoData -> minimal {data,len,maxlen} struct over a
 *    caller-provided fixed buffer; pq_begintypsend -> init at len 0
 *    (the real one reserves the varlena header hole; header assembly
 *    is compared via the total varsize on the Rust side, see the
 *    harness doc); enlargeStringInfo -> no-op (caller buffer is
 *    already wide enough; the Assert in pq_writeintN keeps the bound
 *    check); pq_endtypsend -> returns buf.len.
 *  - Assert -> proof_assert (live: goto-cc has no NDEBUG-free assert.h
 *    model; the bound check is part of the vendored contract).
 * ================================================================ */

typedef struct
{
	char	   *data;
	int			len;
	int			maxlen;
} pg_StringInfoData;

#define pg_proof_assert(c) do { if (!(c)) { *(volatile int *) 0 = 0; } } while (0)

static uint16
pg_bswap16(uint16 x)
{
	return
		((x << 8) & 0xff00) |
		((x >> 8) & 0x00ff);
}

static uint32
pg_bswap32(uint32 x)
{
	return
		((x << 24) & 0xff000000) |
		((x << 8) & 0x00ff0000) |
		((x >> 8) & 0x0000ff00) |
		((x >> 24) & 0x000000ff);
}

static uint64
pg_bswap64(uint64 x)
{
	return
		((x << 56) & 0xff00000000000000ULL) |
		((x << 40) & 0x00ff000000000000ULL) |
		((x << 24) & 0x0000ff0000000000ULL) |
		((x << 8) & 0x000000ff00000000ULL) |
		((x >> 8) & 0x00000000ff000000ULL) |
		((x >> 24) & 0x0000000000ff0000ULL) |
		((x >> 40) & 0x000000000000ff00ULL) |
		((x >> 56) & 0x00000000000000ffULL);
}

#define pg_hton16(x) pg_bswap16(x)	/* shim: little-endian arm */
#define pg_hton32(x) pg_bswap32(x)
#define pg_hton64(x) pg_bswap64(x)

static void
pg_pq_writeint16(pg_StringInfoData *buf, uint16 i)
{
	uint16		ni = pg_hton16(i);

	pg_proof_assert(buf->len + (int) sizeof(uint16) <= buf->maxlen);
	memcpy(buf->data + buf->len, &ni, sizeof(uint16));
	buf->len += sizeof(uint16);
}

static void
pg_pq_writeint32(pg_StringInfoData *buf, uint32 i)
{
	uint32		ni = pg_hton32(i);

	pg_proof_assert(buf->len + (int) sizeof(uint32) <= buf->maxlen);
	memcpy(buf->data + buf->len, &ni, sizeof(uint32));
	buf->len += sizeof(uint32);
}

static void
pg_pq_writeint64(pg_StringInfoData *buf, uint64 i)
{
	uint64		ni = pg_hton64(i);

	pg_proof_assert(buf->len + (int) sizeof(uint64) <= buf->maxlen);
	memcpy(buf->data + buf->len, &ni, sizeof(uint64));
	buf->len += sizeof(uint64);
}

/* int2_bytea "can just use int2send()": BE image into out[2], returns len */
int
pg_int2_bytea(int16_t arg1, unsigned char *out)
{
	pg_StringInfoData buf;

	buf.data = (char *) out;	/* shim: pq_begintypsend fixed frame */
	buf.len = 0;
	buf.maxlen = 2;
	pg_pq_writeint16(&buf, (uint16) arg1);	/* pq_sendint16: enlarge no-op */
	return buf.len;				/* shim: pq_endtypsend */
}

int
pg_int4_bytea(int32_t arg1, unsigned char *out)
{
	pg_StringInfoData buf;

	buf.data = (char *) out;
	buf.len = 0;
	buf.maxlen = 4;
	pg_pq_writeint32(&buf, (uint32) arg1);
	return buf.len;
}

int
pg_int8_bytea(int64_t arg1, unsigned char *out)
{
	pg_StringInfoData buf;

	buf.data = (char *) out;
	buf.len = 0;
	buf.maxlen = 8;
	pg_pq_writeint64(&buf, (uint64) arg1);
	return buf.len;
}

/* ================================================================
 * BYTEAIN ESCAPED-STYLE PASS ONE (lane pick-a 2026-07-30, oid 1244).
 *
 * Provenance: REL_18_STABLE varlena.c byteain (l.299), first loop
 * (count + validate) verbatim.  The hex arm delegates to
 * hex_decode_safe (proofs/hex family); pass two is the image build
 * (result-image class, out of this scalar claim).
 *
 * SHIMS: ereturn(escontext, ..., errcode(ERRCODE_INVALID_TEXT_
 * REPRESENTATION) "invalid input syntax") -> *err = 1, return -1.
 * Input contract: NUL-terminated cstring (fmgr CSTRING protocol; the
 * harness builds it from the slice + literal NUL).  The escape check
 * reads at most one byte past the last content byte (the NUL) — in
 * bounds by the cstring contract.
 * ================================================================ */

int
pg_byteain_escaped_count(const char *inputText, int *err)
{
	const char *tp;				/* shim: char *tp (const for the shim sig) */
	int			bc;

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
			/* shim: ereturn 22P02 "invalid input syntax for type bytea" */
			*err = 1;
			return -1;
		}
	}

	return bc;
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
