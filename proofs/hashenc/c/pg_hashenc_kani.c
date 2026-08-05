/*
 * Vendored PostgreSQL C for the proofs/hashenc Kani harnesses (p1-lanee).
 *
 * Provenance (all bodies VERBATIM):
 *   - src/common/base64.c @ postgres-src
 *     62d6c7d3df6287f1bd83199c1a746e50d31571a0 (REL_18 "Stamp 18.3", the
 *     repo's vendored ground-truth checkout
 *     ../pgrust-fabled/vendor/postgres-src): _base64, b64lookup,
 *     pg_b64_encode, pg_b64_decode, pg_b64_enc_len, pg_b64_dec_len.
 *   - src/common/md5_common.c @ same ref: bytesToHex (exported as
 *     pg_kani_bytes_to_hex).
 *   - src/backend/utils/adt/ascii.c @ same ref: ascii_safe_strlcpy.
 *
 * Shim prelude (plumbing only, never logic): c.h typedefs; Assert no-op
 * (NDEBUG parity).
 */

#include <stdint.h>
#include <string.h>
#include <stddef.h>

typedef int8_t int8;
typedef uint8_t uint8;
typedef uint32_t uint32;

#define Assert(x) ((void) 0)

/* ---------------- src/common/base64.c (verbatim) ---------------- */

static const char _base64[] =
"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

static const int8 b64lookup[128] = {
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 62, -1, -1, -1, 63,
	52, 53, 54, 55, 56, 57, 58, 59, 60, 61, -1, -1, -1, -1, -1, -1,
	-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14,
	15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, -1, -1, -1, -1, -1,
	-1, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
	41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, -1, -1, -1, -1, -1,
};

int
pg_b64_encode(const uint8 *src, int len, char *dst, int dstlen)
{
	char	   *p;
	const uint8 *s,
			   *end = src + len;
	int			pos = 2;
	uint32		buf = 0;

	s = src;
	p = dst;

	while (s < end)
	{
		buf |= *s << (pos << 3);
		pos--;
		s++;

		/* write it out */
		if (pos < 0)
		{
			/*
			 * Leave if there is an overflow in the area allocated for the
			 * encoded string.
			 */
			if ((p - dst + 4) > dstlen)
				goto error;

			*p++ = _base64[(buf >> 18) & 0x3f];
			*p++ = _base64[(buf >> 12) & 0x3f];
			*p++ = _base64[(buf >> 6) & 0x3f];
			*p++ = _base64[buf & 0x3f];

			pos = 2;
			buf = 0;
		}
	}
	if (pos != 2)
	{
		/*
		 * Leave if there is an overflow in the area allocated for the encoded
		 * string.
		 */
		if ((p - dst + 4) > dstlen)
			goto error;

		*p++ = _base64[(buf >> 18) & 0x3f];
		*p++ = _base64[(buf >> 12) & 0x3f];
		*p++ = (pos == 0) ? _base64[(buf >> 6) & 0x3f] : '=';
		*p++ = '=';
	}

	Assert((p - dst) <= dstlen);
	return p - dst;

error:
	memset(dst, 0, dstlen);
	return -1;
}

int
pg_b64_decode(const char *src, int len, uint8 *dst, int dstlen)
{
	const char *srcend = src + len,
			   *s = src;
	uint8	   *p = dst;
	char		c;
	int			b = 0;
	uint32		buf = 0;
	int			pos = 0,
				end = 0;

	while (s < srcend)
	{
		c = *s++;

		/* Leave if a whitespace is found */
		if (c == ' ' || c == '\t' || c == '\n' || c == '\r')
			goto error;

		if (c == '=')
		{
			/* end sequence */
			if (!end)
			{
				if (pos == 2)
					end = 1;
				else if (pos == 3)
					end = 2;
				else
				{
					/*
					 * Unexpected "=" character found while decoding base64
					 * sequence.
					 */
					goto error;
				}
			}
			b = 0;
		}
		else
		{
			b = -1;
			if (c > 0 && c < 127)
				b = b64lookup[(unsigned char) c];
			if (b < 0)
			{
				/* invalid symbol found */
				goto error;
			}
		}
		/* add it to buffer */
		buf = (buf << 6) + b;
		pos++;
		if (pos == 4)
		{
			/*
			 * Leave if there is an overflow in the area allocated for the
			 * decoded string.
			 */
			if ((p - dst + 1) > dstlen)
				goto error;
			*p++ = (buf >> 16) & 255;

			if (end == 0 || end > 1)
			{
				/* overflow check */
				if ((p - dst + 1) > dstlen)
					goto error;
				*p++ = (buf >> 8) & 255;
			}
			if (end == 0 || end > 2)
			{
				/* overflow check */
				if ((p - dst + 1) > dstlen)
					goto error;
				*p++ = buf & 255;
			}
			buf = 0;
			pos = 0;
		}
	}

	if (pos != 0)
	{
		/*
		 * base64 end sequence is invalid.  Input data is missing padding, is
		 * truncated or is otherwise corrupted.
		 */
		goto error;
	}

	Assert((p - dst) <= dstlen);
	return p - dst;

error:
	memset(dst, 0, dstlen);
	return -1;
}

int
pg_b64_enc_len(int srclen)
{
	/* 3 bytes will be converted to 4 */
	return (srclen + 2) / 3 * 4;
}

int
pg_b64_dec_len(int srclen)
{
	return (srclen * 3) >> 2;
}

/* ------------- src/common/md5_common.c bytesToHex (verbatim) ------------- */

static void
bytesToHex(uint8 b[16], char *s)
{
	static const char *hex = "0123456789abcdef";
	int			q,
				w;

	for (q = 0, w = 0; q < 16; q++)
	{
		s[w++] = hex[(b[q] >> 4) & 0x0F];
		s[w++] = hex[b[q] & 0x0F];
	}
	s[w] = '\0';
}

/*
 * int-returning shim (Kani models a Rust `()` return as struct Unit, which
 * goto-cc rejects against a C void). Per-element copy, never memcpy of
 * symbolic values (CBMC byte-pun law).
 */
int
pg_kani_bytes_to_hex(const uint8 *b, char *s)
{
	uint8		tmp[16];
	int			i;

	for (i = 0; i < 16; i++)
		tmp[i] = b[i];
	bytesToHex(tmp, s);
	return 0;
}

/* ---------- src/backend/utils/adt/ascii.c ascii_safe_strlcpy (verbatim) ---------- */

void
ascii_safe_strlcpy(char *dest, const char *src, size_t destsiz)
{
	if (destsiz == 0)			/* corner case: no room for trailing nul */
		return;

	while (--destsiz > 0)
	{
		/* use unsigned char here to avoid compiler warning */
		unsigned char ch = *src++;

		if (ch == '\0')
			break;
		/* Keep printable ASCII characters */
		if (32 <= ch && ch <= 127)
			*dest = ch;
		/* White-space is also OK */
		else if (ch == '\n' || ch == '\r' || ch == '\t')
			*dest = ch;
		/* Everything else is replaced with '?' */
		else
			*dest = '?';
		dest++;
	}

	*dest = '\0';
}

/* int-returning shim (see pg_kani_bytes_to_hex). */
int
pg_kani_ascii_safe_strlcpy(char *dest, const char *src, size_t destsiz)
{
	ascii_safe_strlcpy(dest, src, destsiz);
	return 0;
}
