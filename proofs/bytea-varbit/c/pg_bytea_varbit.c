/*
 * Vendored PostgreSQL C for the bytea escape-format + varbit proofs.
 *
 * Provenance (all fetched 2026-07-28 from REL_15_STABLE — chosen to match
 * the proofs/hex lane; the esc_* / byteaout / bit_cmp bodies are identical
 * in shape on master, REL_15 avoids master's header churn):
 *  - src/backend/utils/adt/encode.c: esc_encode, esc_decode, esc_enc_len,
 *    esc_dec_len (lines ~404-545)
 *  - src/backend/utils/adt/varlena.c: byteaout escape branch (lines ~405-458)
 *  - src/backend/utils/adt/varbit.c: bit_cmp (lines ~817-838) and the
 *    VARBIT_PAD macro (lines ~47-53)
 *
 * SHIMS (everything else is verbatim):
 *  - names pg_-prefixed; postgres typedefs inlined (uint64 -> unsigned long
 *    long, size_t via <stddef.h>, int32 -> int).
 *  - ereport(ERROR, ...) has no model here: esc_dec_len / esc_decode return
 *    a -1 sentinel (signed long long return type instead of uint64) at the
 *    exact program point where C raises; message text is not part of the
 *    equivalence claim.
 *  - byteaout: PG_FUNCTION_ARGS / varlena unwrapping -> plain (src,len,dst)
 *    signatures; the two escape-branch loops are split into pg_byteaout_esc_len
 *    (the counting loop, verbatim incl. the initial len=1) and
 *    pg_byteaout_esc (the emit loop + the function's common trailing
 *    *rp = '\0'). palloc -> caller-provided buffer. The MaxAllocSize check
 *    is unreachable at harness sizes and is kept (constant inlined).
 *  - VARBIT_PAD is a statement macro over a VarBit*: expanded as
 *    pg_varbit_pad(bits, bytelen, bitlen) with VARBITPAD(vb) inlined as
 *    bytelen*8 - bitlen and BITMASK as 0xFF. The macro's Assert() is
 *    compiled out in production builds; the harness fences to the asserted
 *    domain (0 <= pad < 8).
 *  - bit_cmp: VarBit* header accessors (VARBITBYTES/VARBITS/VARBITLEN) ->
 *    explicit (bits, bytelen, bitlen) parameters; Min() inlined. memcmp is
 *    CBMC's built-in model.
 *  - IS_HIGHBIT_SET(ch) inlined per c.h: ((unsigned char)(ch) & 0x80) != 0.
 */

#include <stddef.h>
#include <string.h>

typedef unsigned long long pg_uint64;

#define VAL(CH)			((CH) - '0')
#define DIG(VAL)		((VAL) + '0')
#define IS_HIGHBIT_SET(ch)	((unsigned char)(ch) & 0x80)

/* ---------------- encode.c: escape format ---------------- */

pg_uint64
pg_esc_encode(const char *src, size_t srclen, char *dst)
{
	const char *end = src + srclen;
	char	   *rp = dst;
	pg_uint64	len = 0;

	while (src < end)
	{
		unsigned char c = (unsigned char) *src;

		if (c == '\0' || IS_HIGHBIT_SET(c))
		{
			rp[0] = '\\';
			rp[1] = DIG(c >> 6);
			rp[2] = DIG((c >> 3) & 7);
			rp[3] = DIG(c & 7);
			rp += 4;
			len += 4;
		}
		else if (c == '\\')
		{
			rp[0] = '\\';
			rp[1] = '\\';
			rp += 2;
			len += 2;
		}
		else
		{
			*rp++ = c;
			len++;
		}

		src++;
	}

	return len;
}

/* SHIM: returns -1 where C ereports (invalid input syntax for type bytea) */
long long
pg_esc_decode(const char *src, size_t srclen, char *dst)
{
	const char *end = src + srclen;
	char	   *rp = dst;
	long long	len = 0;

	while (src < end)
	{
		if (src[0] != '\\')
			*rp++ = *src++;
		else if (src + 3 < end &&
				 (src[1] >= '0' && src[1] <= '3') &&
				 (src[2] >= '0' && src[2] <= '7') &&
				 (src[3] >= '0' && src[3] <= '7'))
		{
			int			val;

			val = VAL(src[1]);
			val <<= 3;
			val += VAL(src[2]);
			val <<= 3;
			*rp++ = val + VAL(src[3]);
			src += 4;
		}
		else if (src + 1 < end &&
				 (src[1] == '\\'))
		{
			*rp++ = '\\';
			src += 2;
		}
		else
		{
			return -1;			/* SHIM: was ereport(ERROR, ...) */
		}

		len++;
	}

	return len;
}

pg_uint64
pg_esc_enc_len(const char *src, size_t srclen)
{
	const char *end = src + srclen;
	pg_uint64	len = 0;

	while (src < end)
	{
		if (*src == '\0' || IS_HIGHBIT_SET(*src))
			len += 4;
		else if (*src == '\\')
			len += 2;
		else
			len++;

		src++;
	}

	return len;
}

/* SHIM: returns -1 where C ereports (invalid input syntax for type bytea) */
long long
pg_esc_dec_len(const char *src, size_t srclen)
{
	const char *end = src + srclen;
	long long	len = 0;

	while (src < end)
	{
		if (src[0] != '\\')
			src++;
		else if (src + 3 < end &&
				 (src[1] >= '0' && src[1] <= '3') &&
				 (src[2] >= '0' && src[2] <= '7') &&
				 (src[3] >= '0' && src[3] <= '7'))
		{
			/*
			 * backslash + valid octal
			 */
			src += 4;
		}
		else if (src + 1 < end &&
				 (src[1] == '\\'))
		{
			/*
			 * two backslashes = backslash
			 */
			src += 2;
		}
		else
		{
			return -1;			/* SHIM: was ereport(ERROR, ...) */
		}

		len++;
	}

	return len;
}

/* ---------------- varlena.c: byteaout escape branch ---------------- */

/* the counting loop; initial len = 1 ("empty string has 1 char") verbatim */
pg_uint64
pg_byteaout_esc_len(const char *vlena_data, int vlena_len)
{
	char	   *vp;
	pg_uint64	len;
	int			i;

	len = 1;					/* empty string has 1 char */
	vp = (char *) vlena_data;
	for (i = vlena_len; i != 0; i--, vp++)
	{
		if (*vp == '\\')
			len += 2;
		else if ((unsigned char) *vp < 0x20 || (unsigned char) *vp > 0x7e)
			len += 4;
		else
			len++;
	}
	return len;
}

/* the emit loop + the function's common trailing *rp = '\0'.
 * int return: void/Unit FFI shim. */
int
pg_byteaout_esc(const char *vlena_data, int vlena_len, char *result)
{
	char	   *rp;
	char	   *vp;
	int			i;

	rp = result;
	vp = (char *) vlena_data;
	for (i = vlena_len; i != 0; i--, vp++)
	{
		if (*vp == '\\')
		{
			*rp++ = '\\';
			*rp++ = '\\';
		}
		else if ((unsigned char) *vp < 0x20 || (unsigned char) *vp > 0x7e)
		{
			int			val;	/* holds unprintable chars */

			val = *vp;
			rp[0] = '\\';
			rp[3] = DIG(val & 07);
			val >>= 3;
			rp[2] = DIG(val & 07);
			val >>= 3;
			rp[1] = DIG(val & 03);
			rp += 4;
		}
		else
			*rp++ = *vp;
	}
	*rp = '\0';
	return 0;
}

/* encode.c hextbl + hex_encode (verbatim), for byteaout's hex branch */
static const char pg_hextbl[] = "0123456789abcdef";

static pg_uint64
pg_hex_encode(const char *src, size_t len, char *dst)
{
	const char *end = src + len;

	while (src < end)
	{
		*dst++ = pg_hextbl[(*src >> 4) & 0xF];
		*dst++ = pg_hextbl[*src & 0xF];
		src++;
	}
	return (pg_uint64) len * 2;
}

/* byteaout hex branch: *rp++='\\'; *rp++='x'; rp += hex_encode(...);
 * + the common trailing *rp = '\0'. int return: void/Unit FFI shim. */
int
pg_byteaout_hex(const char *vlena_data, int vlena_len, char *result)
{
	char	   *rp = result;

	*rp++ = '\\';
	*rp++ = 'x';
	rp += pg_hex_encode(vlena_data, (size_t) vlena_len, rp);
	*rp = '\0';
	return 0;
}

/* ---------------- varbit.c ---------------- */

/*
 * bit_cmp — VarBit* accessors shimmed to explicit (bits, bytelen, bitlen).
 */
int
pg_bit_cmp(const unsigned char *bits1, int bytelen1, int bitlen1,
		   const unsigned char *bits2, int bytelen2, int bitlen2)
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

/*
 * VARBIT_PAD macro body; VARBITPAD(vb) inlined as bytelen*8 - bitlen,
 * BITMASK = 0xFF. Assert dropped (production builds compile it out);
 * harness fences 0 <= pad_ < 8. int return: void/Unit FFI shim.
 */
int
pg_varbit_pad(unsigned char *bits, int bytelen, int bitlen)
{
	int			pad_ = bytelen * 8 - bitlen;

	if (pad_ > 0)
		*(bits + bytelen - 1) &= 0xFF << pad_;
	return 0;
}
