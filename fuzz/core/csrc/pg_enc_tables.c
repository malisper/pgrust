/*
 * Vendored PostgreSQL C: base64 codec, to_ascii kernel + ascii_safe_strlcpy,
 * and keyword lookup — differential-fuzz oracle for the p1-laneg batch
 * (common/base64, adt/adt_ascii, common/keywords).
 *
 * Provenance (all bodies VERBATIM unless a shim is listed below), all at
 * postgres-src 62d6c7d3df6287f1bd83199c1a746e50d31571a0 (REL_18 "Stamp
 * 18.3", the repo's vendored ground-truth checkout
 * ../pgrust-fabled/vendor/postgres-src):
 *   - src/common/base64.c: _base64 table, b64lookup table, pg_b64_encode,
 *     pg_b64_decode, pg_b64_enc_len, pg_b64_dec_len — verbatim.
 *   - src/backend/utils/adt/ascii.c: pg_to_ascii (incl. the three latin
 *     maps + win1250 map string literals) — verbatim modulo the ereport
 *     shim below; ascii_safe_strlcpy — verbatim.
 *   - src/common/kwlookup.c: ScanKeywordLookup — verbatim.
 *   - src/include/common/kwlookup.h: ScanKeywordList + GetScanKeyword —
 *     verbatim (inlined below; the fuzz build has no PG include tree).
 *   - kwlist_d.h: NOT copied — the build includes the SAME generated file
 *     the shipped Rust crate's build.rs transcribes
 *     (crates/common/keywords/kwlist_d.h, gen_keywordlist.pl output for
 *     18.3 kwlist.h), so C hash/table and Rust hash/table share one
 *     source of truth and any transcription defect is a divergence.
 *
 * Shims (plumbing only, never logic):
 *   - pg_to_ascii's unsupported-encoding ereport(ERROR) arm -> set
 *     pg_enc_tables_errcode = 1 (ERRCODE_FEATURE_NOT_SUPPORTED class) and
 *     return; the comparator checks the errcode class, not message text.
 *   - pg_enc values (PG_LATIN1=8, PG_LATIN2=9, PG_LATIN9=16,
 *     PG_WIN1250=29) stated as literals from the pg_wchar.h enum order.
 *   - size_t/typedefs via ../shim/postgres.h (fixed-width LP64, no-op
 *     Assert exactly like an NDEBUG production build).
 */

#include "postgres.h"
#include <stddef.h>

/* ------------------------------------------------------------------ */
/* src/common/base64.c — VERBATIM                                      */
/* ------------------------------------------------------------------ */

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

/* ------------------------------------------------------------------ */
/* src/backend/utils/adt/ascii.c — pg_to_ascii + ascii_safe_strlcpy    */
/* ------------------------------------------------------------------ */

/* pg_wchar.h enum values (order-derived literals, see header comment) */
#define PG_LATIN1	8
#define PG_LATIN2	9
#define PG_LATIN9	16
#define PG_WIN1250	29

/* errcode capture: 0 = ok, 1 = ERRCODE_FEATURE_NOT_SUPPORTED class */
static _Thread_local int pg_enc_tables_errcode = 0;

int
pg_enc_tables_errcode_get(void)
{
	return pg_enc_tables_errcode;
}

/* ereport(ERROR, ...) shim: record class 1, unwind via return (the arm is
 * the function's last statement-reachable path; body below stays verbatim
 * with `ereport(ERROR, ...)` expanded to this). */
#define ASCII_EREPORT_UNSUPPORTED() \
	do { pg_enc_tables_errcode = 1; return; } while (0)

void
pg_diff_to_ascii(unsigned char *src, unsigned char *src_end, unsigned char *dest, int enc)
{
	unsigned char *x;
	const unsigned char *ascii;
	int			range;

	pg_enc_tables_errcode = 0;

	/*
	 * relevant start for an encoding
	 */
#define RANGE_128	128
#define RANGE_160	160

	if (enc == PG_LATIN1)
	{
		/*
		 * ISO-8859-1 <range: 160 -- 255>
		 */
		ascii = (const unsigned char *) "  cL Y  \"Ca  -R     'u .,      ?AAAAAAACEEEEIIII NOOOOOxOUUUUYTBaaaaaaaceeeeiiii nooooo/ouuuuyty";
		range = RANGE_160;
	}
	else if (enc == PG_LATIN2)
	{
		/*
		 * ISO-8859-2 <range: 160 -- 255>
		 */
		ascii = (const unsigned char *) " A L LS \"SSTZ-ZZ a,l'ls ,sstz\"zzRAAAALCCCEEEEIIDDNNOOOOxRUUUUYTBraaaalccceeeeiiddnnoooo/ruuuuyt.";
		range = RANGE_160;
	}
	else if (enc == PG_LATIN9)
	{
		/*
		 * ISO-8859-15 <range: 160 -- 255>
		 */
		ascii = (const unsigned char *) "  cL YS sCa  -R     Zu .z   EeY?AAAAAAACEEEEIIII NOOOOOxOUUUUYTBaaaaaaaceeeeiiii nooooo/ouuuuyty";
		range = RANGE_160;
	}
	else if (enc == PG_WIN1250)
	{
		/*
		 * Window CP1250 <range: 128 -- 255>
		 */
		ascii = (const unsigned char *) "  ' \"    %S<STZZ `'\"\".--  s>stzz   L A  \"CS  -RZ  ,l'u .,as L\"lzRAAAALCCCEEEEIIDDNNOOOOxRUUUUYTBraaaalccceeeeiiddnnoooo/ruuuuyt ";
		range = RANGE_128;
	}
	else
	{
		ASCII_EREPORT_UNSUPPORTED();
	}

	/*
	 * Encode found character.
	 */
	for (x = src; x < src_end; x++)
	{
		if (*x < 128)
			*dest++ = *x;
		else if (*x < range)
			*dest++ = ' ';	/* bogus 128 to 'range' */
		else
			*dest++ = ascii[*x - range];
	}
}

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

/* ------------------------------------------------------------------ */
/* src/include/common/kwlookup.h — vendored VERBATIM at                */
/* csrc/shim/common/kwlookup.h (provides ScanKeywordList +             */
/* GetScanKeyword)                                                     */
/* ------------------------------------------------------------------ */

#include "common/kwlookup.h"

/* the generated 18.3 keyword data + perfect hash, SHARED with the Rust
 * crate's build.rs input (see provenance header) */
#include "kwlist_d.h"

/* ------------------------------------------------------------------ */
/* src/common/kwlookup.c — ScanKeywordLookup VERBATIM                  */
/* ------------------------------------------------------------------ */

int
ScanKeywordLookup(const char *str,
				  const ScanKeywordList *keywords)
{
	size_t		len;
	int			h;
	const char *kw;

	/*
	 * Reject immediately if too long to be any keyword.  This saves useless
	 * hashing and downcasing work on long strings.
	 */
	len = strlen(str);
	if (len > keywords->max_kw_len)
		return -1;

	/*
	 * Compute the hash function.  We assume it was generated to produce
	 * case-insensitive results.  Since it's a perfect hash, we need only
	 * match to the specific keyword it identifies.
	 */
	h = keywords->hash(str, len);

	/* An out-of-range result implies no match */
	if (h < 0 || h >= keywords->num_keywords)
		return -1;

	/*
	 * Compare character-by-character to see if we have a match, applying an
	 * ASCII-only downcasing to the input characters.  We must not use
	 * tolower() since it may produce the wrong translation in some locales
	 * (eg, Turkish).
	 */
	kw = GetScanKeyword(h, keywords);
	while (*str != '\0')
	{
		char		ch = *str++;

		if (ch >= 'A' && ch <= 'Z')
			ch += 'a' - 'A';
		if (ch != *kw++)
			return -1;
	}
	if (*kw != '\0')
		return -1;

	/* Success! */
	return h;
}

/* driver-facing accessors over the generated statics */
const ScanKeywordList *
pg_diff_scan_keywords(void)
{
	return &ScanKeywords;
}

const char *
pg_diff_get_scan_keyword(int n)
{
	return GetScanKeyword(n, &ScanKeywords);
}

int
pg_diff_scan_keywords_num(void)
{
	return ScanKeywords.num_keywords;
}

int
pg_diff_scan_keywords_max_len(void)
{
	return ScanKeywords.max_kw_len;
}

int
pg_diff_scan_keyword_lookup(const char *str)
{
	return ScanKeywordLookup(str, &ScanKeywords);
}
