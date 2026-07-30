/*
 * pg_mbconv_common.c — vendored PostgreSQL C for the mbconv proof family.
 *
 * PROVENANCE (all fetched 2026-07-28 from
 * https://raw.githubusercontent.com/postgres/postgres/REL_18_STABLE/):
 *   - src/common/wchar.c: the per-encoding mblen kernels, verifychar
 *     kernels, pg_utf_mblen, pg_utf8_islegal, pg_mule_mblen — bodies
 *     VERBATIM; `static` dropped so the Kani FFI bridge can link them
 *     (extraction script: proofs/mbconv/c/extract_fn.awk; original
 *     downloads kept in proofs/mbconv/c/orig/ for byte-audit).
 *   - src/backend/utils/mb/conv.c: local2local, latin2mic, mic2latin,
 *     latin2mic_with_table, mic2latin_with_table, compare3, compare4,
 *     store_coded_char, pg_mb_radix_conv, UtfToLocal, LocalToUtf —
 *     bodies VERBATIM, same static-drop.
 *
 * Shims (plumbing only, never logic; see also pg_mbconv.h):
 *   - report_invalid_encoding / report_untranslatable_char / ereport /
 *     elog are macro-rewired per the PROOF_EREPORT_FLAG convention
 *     (pg_mbconv.h): flag + early return at the exact ereport point.
 *   - pg_encoding_verifymbchar: PostgreSQL dispatches through
 *     pg_wchar_table[encoding].mbverifychar; vendoring the full table
 *     would drag in the wchar<->mb converters, so the dispatch is
 *     replaced by an if/else chain over the SAME verifychar kernels the
 *     REL_18_STABLE table rows name (transcribed below from the
 *     pg_wchar_table initializer; encodings not used by any conversion
 *     proc fall back to -1, unreachable in the harnesses which pin
 *     valid encodings). The kernels themselves are verbatim.
 *   - bsearch: CBMC has no libc model; a plain linear-scan model with
 *     bsearch's contract (works on any sorted array; the engines' cmap
 *     arrays are generated sorted) replaces it. The comparator under
 *     test (compare3/compare4) is still the verbatim PostgreSQL code.
 *   - pg_mbconv_err: the proof error-class flag (0 none, 1 invalid,
 *     2 untranslatable, 3 bad-encoding-id, 9 elog).
 */
#include "pg_mbconv.h"

int			pg_mbconv_err = 0;

/* linear bsearch model (see header comment) */
void *
bsearch(const void *key, const void *base, size_t nmemb, size_t size,
		int (*compar) (const void *, const void *))
{
	size_t		i;
	const char *b = (const char *) base;

	for (i = 0; i < nmemb; i++)
	{
		if (compar(key, b + i * size) == 0)
			return (void *) (b + i * size);
	}
	return 0;
}


/* forward decls for the verbatim kernels below */
int			pg_euc_mblen(const unsigned char *s);
int			pg_eucjp_mblen(const unsigned char *s);
int			pg_euckr_mblen(const unsigned char *s);
int			pg_euccn_mblen(const unsigned char *s);
int			pg_euctw_mblen(const unsigned char *s);
int			pg_johab_mblen(const unsigned char *s);
int			pg_latin1_mblen(const unsigned char *s);
int			pg_sjis_mblen(const unsigned char *s);
int			pg_big5_mblen(const unsigned char *s);
int			pg_gbk_mblen(const unsigned char *s);
int			pg_uhc_mblen(const unsigned char *s);
int			pg_gb18030_mblen(const unsigned char *s);
int			pg_eucjp_verifychar(const unsigned char *s, int len);
int			pg_euckr_verifychar(const unsigned char *s, int len);
int			pg_euctw_verifychar(const unsigned char *s, int len);
int			pg_johab_verifychar(const unsigned char *s, int len);
int			pg_mule_verifychar(const unsigned char *s, int len);
int			pg_latin1_verifychar(const unsigned char *s, int len);
int			pg_sjis_verifychar(const unsigned char *s, int len);
int			pg_big5_verifychar(const unsigned char *s, int len);
int			pg_gbk_verifychar(const unsigned char *s, int len);
int			pg_uhc_verifychar(const unsigned char *s, int len);
int			pg_gb18030_verifychar(const unsigned char *s, int len);
int			compare3(const void *p1, const void *p2);
int			compare4(const void *p1, const void *p2);
unsigned char *store_coded_char(unsigned char *dest, uint32 code);
uint32		pg_mb_radix_conv(const pg_mb_radix_tree *rt, int l,
							 unsigned char b1, unsigned char b2,
							 unsigned char b3, unsigned char b4);

/* pg_wchar_table.mbverifychar dispatch, transcribed row-for-row */
int
pg_encoding_verifymbchar(int encoding, const char *mbstr, int len)
{
	const unsigned char *s = (const unsigned char *) mbstr;

	if (encoding == PG_EUC_JP || encoding == PG_EUC_JIS_2004)
		return pg_eucjp_verifychar(s, len);
	if (encoding == PG_EUC_CN || encoding == PG_EUC_KR)
		return pg_euckr_verifychar(s, len);	/* pg_euccn_verifychar is a
											 * #define alias of euckr */
	if (encoding == PG_EUC_TW)
		return pg_euctw_verifychar(s, len);
	if (encoding == PG_MULE_INTERNAL)
		return pg_mule_verifychar(s, len);
	if ((encoding >= PG_LATIN1 && encoding <= PG_KOI8U) &&
		encoding != PG_UTF8 && encoding != PG_MULE_INTERNAL)
		return pg_latin1_verifychar(s, len);	/* all single-byte rows */
	if (encoding == PG_SJIS || encoding == PG_SHIFT_JIS_2004)
		return pg_sjis_verifychar(s, len);
	if (encoding == PG_BIG5)
		return pg_big5_verifychar(s, len);
	if (encoding == PG_GBK)
		return pg_gbk_verifychar(s, len);
	if (encoding == PG_UHC)
		return pg_uhc_verifychar(s, len);
	if (encoding == PG_GB18030)
		return pg_gb18030_verifychar(s, len);
	if (encoding == PG_JOHAB)
		return pg_johab_verifychar(s, len);
	return -1;					/* unreached: harnesses pin handled encodings */
}

/* ================= src/common/wchar.c (verbatim bodies) ================= */
int
pg_euc_mblen(const unsigned char *s)
{
	int			len;

	if (*s == SS2)
		len = 2;
	else if (*s == SS3)
		len = 3;
	else if (IS_HIGHBIT_SET(*s))
		len = 2;
	else
		len = 1;
	return len;
}

int
pg_eucjp_mblen(const unsigned char *s)
{
	return pg_euc_mblen(s);
}

int
pg_euckr_mblen(const unsigned char *s)
{
	return pg_euc_mblen(s);
}

int
pg_euccn_mblen(const unsigned char *s)
{
	int			len;

	if (*s == SS2)
		len = 3;
	else if (*s == SS3)
		len = 3;
	else if (IS_HIGHBIT_SET(*s))
		len = 2;
	else
		len = 1;
	return len;
}

int
pg_euctw_mblen(const unsigned char *s)
{
	int			len;

	if (*s == SS2)
		len = 4;
	else if (*s == SS3)
		len = 3;
	else if (IS_HIGHBIT_SET(*s))
		len = 2;
	else
		len = 1;
	return len;
}

int
pg_johab_mblen(const unsigned char *s)
{
	return pg_euc_mblen(s);
}

int
pg_utf_mblen(const unsigned char *s)
{
	int			len;

	if ((*s & 0x80) == 0)
		len = 1;
	else if ((*s & 0xe0) == 0xc0)
		len = 2;
	else if ((*s & 0xf0) == 0xe0)
		len = 3;
	else if ((*s & 0xf8) == 0xf0)
		len = 4;
#ifdef NOT_USED
	else if ((*s & 0xfc) == 0xf8)
		len = 5;
	else if ((*s & 0xfe) == 0xfc)
		len = 6;
#endif
	else
		len = 1;
	return len;
}

int
pg_mule_mblen(const unsigned char *s)
{
	int			len;

	if (IS_LC1(*s))
		len = 2;
	else if (IS_LCPRV1(*s))
		len = 3;
	else if (IS_LC2(*s))
		len = 3;
	else if (IS_LCPRV2(*s))
		len = 4;
	else
		len = 1;				/* assume ASCII */
	return len;
}

int
pg_latin1_mblen(const unsigned char *s)
{
	return 1;
}

int
pg_sjis_mblen(const unsigned char *s)
{
	int			len;

	if (*s >= 0xa1 && *s <= 0xdf)
		len = 1;				/* 1 byte kana? */
	else if (IS_HIGHBIT_SET(*s))
		len = 2;				/* kanji? */
	else
		len = 1;				/* should be ASCII */
	return len;
}

int
pg_big5_mblen(const unsigned char *s)
{
	int			len;

	if (IS_HIGHBIT_SET(*s))
		len = 2;				/* kanji? */
	else
		len = 1;				/* should be ASCII */
	return len;
}

int
pg_gbk_mblen(const unsigned char *s)
{
	int			len;

	if (IS_HIGHBIT_SET(*s))
		len = 2;				/* kanji? */
	else
		len = 1;				/* should be ASCII */
	return len;
}

int
pg_uhc_mblen(const unsigned char *s)
{
	int			len;

	if (IS_HIGHBIT_SET(*s))
		len = 2;				/* 2byte? */
	else
		len = 1;				/* should be ASCII */
	return len;
}

int
pg_gb18030_mblen(const unsigned char *s)
{
	int			len;

	if (!IS_HIGHBIT_SET(*s))
		len = 1;				/* ASCII */
	else if (*(s + 1) >= 0x30 && *(s + 1) <= 0x39)
		len = 4;
	else
		len = 2;
	return len;
}

int
pg_eucjp_verifychar(const unsigned char *s, int len)
{
	int			l;
	unsigned char c1,
				c2;

	c1 = *s++;

	switch (c1)
	{
		case SS2:				/* JIS X 0201 */
			l = 2;
			if (l > len)
				return -1;
			c2 = *s++;
			if (c2 < 0xa1 || c2 > 0xdf)
				return -1;
			break;

		case SS3:				/* JIS X 0212 */
			l = 3;
			if (l > len)
				return -1;
			c2 = *s++;
			if (!IS_EUC_RANGE_VALID(c2))
				return -1;
			c2 = *s++;
			if (!IS_EUC_RANGE_VALID(c2))
				return -1;
			break;

		default:
			if (IS_HIGHBIT_SET(c1)) /* JIS X 0208? */
			{
				l = 2;
				if (l > len)
					return -1;
				if (!IS_EUC_RANGE_VALID(c1))
					return -1;
				c2 = *s++;
				if (!IS_EUC_RANGE_VALID(c2))
					return -1;
			}
			else
				/* must be ASCII */
			{
				l = 1;
			}
			break;
	}

	return l;
}

int
pg_euckr_verifychar(const unsigned char *s, int len)
{
	int			l;
	unsigned char c1,
				c2;

	c1 = *s++;

	if (IS_HIGHBIT_SET(c1))
	{
		l = 2;
		if (l > len)
			return -1;
		if (!IS_EUC_RANGE_VALID(c1))
			return -1;
		c2 = *s++;
		if (!IS_EUC_RANGE_VALID(c2))
			return -1;
	}
	else
		/* must be ASCII */
	{
		l = 1;
	}

	return l;
}

int
pg_euctw_verifychar(const unsigned char *s, int len)
{
	int			l;
	unsigned char c1,
				c2;

	c1 = *s++;

	switch (c1)
	{
		case SS2:				/* CNS 11643 Plane 1-7 */
			l = 4;
			if (l > len)
				return -1;
			c2 = *s++;
			if (c2 < 0xa1 || c2 > 0xa7)
				return -1;
			c2 = *s++;
			if (!IS_EUC_RANGE_VALID(c2))
				return -1;
			c2 = *s++;
			if (!IS_EUC_RANGE_VALID(c2))
				return -1;
			break;

		case SS3:				/* unused */
			return -1;

		default:
			if (IS_HIGHBIT_SET(c1)) /* CNS 11643 Plane 1 */
			{
				l = 2;
				if (l > len)
					return -1;
				/* no further range check on c1? */
				c2 = *s++;
				if (!IS_EUC_RANGE_VALID(c2))
					return -1;
			}
			else
				/* must be ASCII */
			{
				l = 1;
			}
			break;
	}
	return l;
}

int
pg_johab_verifychar(const unsigned char *s, int len)
{
	int			l,
				mbl;
	unsigned char c;

	l = mbl = pg_johab_mblen(s);

	if (len < l)
		return -1;

	if (!IS_HIGHBIT_SET(*s))
		return mbl;

	while (--l > 0)
	{
		c = *++s;
		if (!IS_EUC_RANGE_VALID(c))
			return -1;
	}
	return mbl;
}

int
pg_mule_verifychar(const unsigned char *s, int len)
{
	int			l,
				mbl;
	unsigned char c;

	l = mbl = pg_mule_mblen(s);

	if (len < l)
		return -1;

	while (--l > 0)
	{
		c = *++s;
		if (!IS_HIGHBIT_SET(c))
			return -1;
	}
	return mbl;
}

int
pg_latin1_verifychar(const unsigned char *s, int len)
{
	return 1;
}

int
pg_sjis_verifychar(const unsigned char *s, int len)
{
	int			l,
				mbl;
	unsigned char c1,
				c2;

	l = mbl = pg_sjis_mblen(s);

	if (len < l)
		return -1;

	if (l == 1)					/* pg_sjis_mblen already verified it */
		return mbl;

	c1 = *s++;
	c2 = *s;
	if (!ISSJISHEAD(c1) || !ISSJISTAIL(c2))
		return -1;
	return mbl;
}

int
pg_big5_verifychar(const unsigned char *s, int len)
{
	int			l,
				mbl;

	l = mbl = pg_big5_mblen(s);

	if (len < l)
		return -1;

	if (l == 2 &&
		s[0] == NONUTF8_INVALID_BYTE0 &&
		s[1] == NONUTF8_INVALID_BYTE1)
		return -1;

	while (--l > 0)
	{
		if (*++s == '\0')
			return -1;
	}

	return mbl;
}

int
pg_gbk_verifychar(const unsigned char *s, int len)
{
	int			l,
				mbl;

	l = mbl = pg_gbk_mblen(s);

	if (len < l)
		return -1;

	if (l == 2 &&
		s[0] == NONUTF8_INVALID_BYTE0 &&
		s[1] == NONUTF8_INVALID_BYTE1)
		return -1;

	while (--l > 0)
	{
		if (*++s == '\0')
			return -1;
	}

	return mbl;
}

int
pg_uhc_verifychar(const unsigned char *s, int len)
{
	int			l,
				mbl;

	l = mbl = pg_uhc_mblen(s);

	if (len < l)
		return -1;

	if (l == 2 &&
		s[0] == NONUTF8_INVALID_BYTE0 &&
		s[1] == NONUTF8_INVALID_BYTE1)
		return -1;

	while (--l > 0)
	{
		if (*++s == '\0')
			return -1;
	}

	return mbl;
}

int
pg_gb18030_verifychar(const unsigned char *s, int len)
{
	int			l;

	if (!IS_HIGHBIT_SET(*s))
		l = 1;					/* ASCII */
	else if (len >= 4 && *(s + 1) >= 0x30 && *(s + 1) <= 0x39)
	{
		/* Should be 4-byte, validate remaining bytes */
		if (*s >= 0x81 && *s <= 0xfe &&
			*(s + 2) >= 0x81 && *(s + 2) <= 0xfe &&
			*(s + 3) >= 0x30 && *(s + 3) <= 0x39)
			l = 4;
		else
			l = -1;
	}
	else if (len >= 2 && *s >= 0x81 && *s <= 0xfe)
	{
		/* Should be 2-byte, validate */
		if ((*(s + 1) >= 0x40 && *(s + 1) <= 0x7e) ||
			(*(s + 1) >= 0x80 && *(s + 1) <= 0xfe))
			l = 2;
		else
			l = -1;
	}
	else
		l = -1;
	return l;
}

bool
pg_utf8_islegal(const unsigned char *source, int length)
{
	unsigned char a;

	switch (length)
	{
		default:
			/* reject lengths 5 and 6 for now */
			return false;
		case 4:
			a = source[3];
			if (a < 0x80 || a > 0xBF)
				return false;
			/* FALL THRU */
		case 3:
			a = source[2];
			if (a < 0x80 || a > 0xBF)
				return false;
			/* FALL THRU */
		case 2:
			a = source[1];
			switch (*source)
			{
				case 0xE0:
					if (a < 0xA0 || a > 0xBF)
						return false;
					break;
				case 0xED:
					if (a < 0x80 || a > 0x9F)
						return false;
					break;
				case 0xF0:
					if (a < 0x90 || a > 0xBF)
						return false;
					break;
				case 0xF4:
					if (a < 0x80 || a > 0x8F)
						return false;
					break;
				default:
					if (a < 0x80 || a > 0xBF)
						return false;
					break;
			}
			/* FALL THRU */
		case 1:
			a = *source;
			if (a >= 0x80 && a < 0xC2)
				return false;
			if (a > 0xF4)
				return false;
			break;
	}
	return true;
}

/* ================= src/backend/utils/mb/conv.c (verbatim bodies) ================= */
int
local2local(const unsigned char *l,
			unsigned char *p,
			int len,
			int src_encoding,
			int dest_encoding,
			const unsigned char *tab,
			bool noError)
{
	const unsigned char *start = l;
	unsigned char c1,
				c2;

	while (len > 0)
	{
		c1 = *l;
		if (c1 == 0)
		{
			if (noError)
				break;
			report_invalid_encoding(src_encoding, (const char *) l, len);
		}
		if (!IS_HIGHBIT_SET(c1))
			*p++ = c1;
		else
		{
			c2 = tab[c1 - HIGHBIT];
			if (c2)
				*p++ = c2;
			else
			{
				if (noError)
					break;
				report_untranslatable_char(src_encoding, dest_encoding,
										   (const char *) l, len);
			}
		}
		l++;
		len--;
	}
	*p = '\0';

	return l - start;
}

int
latin2mic(const unsigned char *l, unsigned char *p, int len,
		  int lc, int encoding, bool noError)
{
	const unsigned char *start = l;
	int			c1;

	while (len > 0)
	{
		c1 = *l;
		if (c1 == 0)
		{
			if (noError)
				break;
			report_invalid_encoding(encoding, (const char *) l, len);
		}
		if (IS_HIGHBIT_SET(c1))
			*p++ = lc;
		*p++ = c1;
		l++;
		len--;
	}
	*p = '\0';

	return l - start;
}

int
mic2latin(const unsigned char *mic, unsigned char *p, int len,
		  int lc, int encoding, bool noError)
{
	const unsigned char *start = mic;
	int			c1;

	while (len > 0)
	{
		c1 = *mic;
		if (c1 == 0)
		{
			if (noError)
				break;
			report_invalid_encoding(PG_MULE_INTERNAL, (const char *) mic, len);
		}
		if (!IS_HIGHBIT_SET(c1))
		{
			/* easy for ASCII */
			*p++ = c1;
			mic++;
			len--;
		}
		else
		{
			int			l = pg_mule_mblen(mic);

			if (len < l)
			{
				if (noError)
					break;
				report_invalid_encoding(PG_MULE_INTERNAL, (const char *) mic,
										len);
			}
			if (l != 2 || c1 != lc || !IS_HIGHBIT_SET(mic[1]))
			{
				if (noError)
					break;
				report_untranslatable_char(PG_MULE_INTERNAL, encoding,
										   (const char *) mic, len);
			}
			*p++ = mic[1];
			mic += 2;
			len -= 2;
		}
	}
	*p = '\0';

	return mic - start;
}

int
latin2mic_with_table(const unsigned char *l,
					 unsigned char *p,
					 int len,
					 int lc,
					 int encoding,
					 const unsigned char *tab,
					 bool noError)
{
	const unsigned char *start = l;
	unsigned char c1,
				c2;

	while (len > 0)
	{
		c1 = *l;
		if (c1 == 0)
		{
			if (noError)
				break;
			report_invalid_encoding(encoding, (const char *) l, len);
		}
		if (!IS_HIGHBIT_SET(c1))
			*p++ = c1;
		else
		{
			c2 = tab[c1 - HIGHBIT];
			if (c2)
			{
				*p++ = lc;
				*p++ = c2;
			}
			else
			{
				if (noError)
					break;
				report_untranslatable_char(encoding, PG_MULE_INTERNAL,
										   (const char *) l, len);
			}
		}
		l++;
		len--;
	}
	*p = '\0';

	return l - start;
}

int
mic2latin_with_table(const unsigned char *mic,
					 unsigned char *p,
					 int len,
					 int lc,
					 int encoding,
					 const unsigned char *tab,
					 bool noError)
{
	const unsigned char *start = mic;
	unsigned char c1,
				c2;

	while (len > 0)
	{
		c1 = *mic;
		if (c1 == 0)
		{
			if (noError)
				break;
			report_invalid_encoding(PG_MULE_INTERNAL, (const char *) mic, len);
		}
		if (!IS_HIGHBIT_SET(c1))
		{
			/* easy for ASCII */
			*p++ = c1;
			mic++;
			len--;
		}
		else
		{
			int			l = pg_mule_mblen(mic);

			if (len < l)
			{
				if (noError)
					break;
				report_invalid_encoding(PG_MULE_INTERNAL, (const char *) mic,
										len);
			}
			if (l != 2 || c1 != lc || !IS_HIGHBIT_SET(mic[1]) ||
				(c2 = tab[mic[1] - HIGHBIT]) == 0)
			{
				if (noError)
					break;
				report_untranslatable_char(PG_MULE_INTERNAL, encoding,
										   (const char *) mic, len);
				break;			/* keep compiler quiet */
			}
			*p++ = c2;
			mic += 2;
			len -= 2;
		}
	}
	*p = '\0';

	return mic - start;
}

int
compare3(const void *p1, const void *p2)
{
	uint32		s1,
				s2,
				d1,
				d2;

	s1 = *(const uint32 *) p1;
	s2 = *((const uint32 *) p1 + 1);
	d1 = ((const pg_utf_to_local_combined *) p2)->utf1;
	d2 = ((const pg_utf_to_local_combined *) p2)->utf2;
	return (s1 > d1 || (s1 == d1 && s2 > d2)) ? 1 : ((s1 == d1 && s2 == d2) ? 0 : -1);
}

int
compare4(const void *p1, const void *p2)
{
	uint32		v1,
				v2;

	v1 = *(const uint32 *) p1;
	v2 = ((const pg_local_to_utf_combined *) p2)->code;
	return (v1 > v2) ? 1 : ((v1 == v2) ? 0 : -1);
}

unsigned char *
store_coded_char(unsigned char *dest, uint32 code)
{
	if (code & 0xff000000)
		*dest++ = code >> 24;
	if (code & 0x00ff0000)
		*dest++ = code >> 16;
	if (code & 0x0000ff00)
		*dest++ = code >> 8;
	if (code & 0x000000ff)
		*dest++ = code;
	return dest;
}

uint32
pg_mb_radix_conv(const pg_mb_radix_tree *rt,
				 int l,
				 unsigned char b1,
				 unsigned char b2,
				 unsigned char b3,
				 unsigned char b4)
{
	if (l == 4)
	{
		/* 4-byte code */

		/* check code validity */
		if (b1 < rt->b4_1_lower || b1 > rt->b4_1_upper ||
			b2 < rt->b4_2_lower || b2 > rt->b4_2_upper ||
			b3 < rt->b4_3_lower || b3 > rt->b4_3_upper ||
			b4 < rt->b4_4_lower || b4 > rt->b4_4_upper)
			return 0;

		/* perform lookup */
		if (rt->chars32)
		{
			uint32		idx = rt->b4root;

			idx = rt->chars32[b1 + idx - rt->b4_1_lower];
			idx = rt->chars32[b2 + idx - rt->b4_2_lower];
			idx = rt->chars32[b3 + idx - rt->b4_3_lower];
			return rt->chars32[b4 + idx - rt->b4_4_lower];
		}
		else
		{
			uint16		idx = rt->b4root;

			idx = rt->chars16[b1 + idx - rt->b4_1_lower];
			idx = rt->chars16[b2 + idx - rt->b4_2_lower];
			idx = rt->chars16[b3 + idx - rt->b4_3_lower];
			return rt->chars16[b4 + idx - rt->b4_4_lower];
		}
	}
	else if (l == 3)
	{
		/* 3-byte code */

		/* check code validity */
		if (b2 < rt->b3_1_lower || b2 > rt->b3_1_upper ||
			b3 < rt->b3_2_lower || b3 > rt->b3_2_upper ||
			b4 < rt->b3_3_lower || b4 > rt->b3_3_upper)
			return 0;

		/* perform lookup */
		if (rt->chars32)
		{
			uint32		idx = rt->b3root;

			idx = rt->chars32[b2 + idx - rt->b3_1_lower];
			idx = rt->chars32[b3 + idx - rt->b3_2_lower];
			return rt->chars32[b4 + idx - rt->b3_3_lower];
		}
		else
		{
			uint16		idx = rt->b3root;

			idx = rt->chars16[b2 + idx - rt->b3_1_lower];
			idx = rt->chars16[b3 + idx - rt->b3_2_lower];
			return rt->chars16[b4 + idx - rt->b3_3_lower];
		}
	}
	else if (l == 2)
	{
		/* 2-byte code */

		/* check code validity - first byte */
		if (b3 < rt->b2_1_lower || b3 > rt->b2_1_upper ||
			b4 < rt->b2_2_lower || b4 > rt->b2_2_upper)
			return 0;

		/* perform lookup */
		if (rt->chars32)
		{
			uint32		idx = rt->b2root;

			idx = rt->chars32[b3 + idx - rt->b2_1_lower];
			return rt->chars32[b4 + idx - rt->b2_2_lower];
		}
		else
		{
			uint16		idx = rt->b2root;

			idx = rt->chars16[b3 + idx - rt->b2_1_lower];
			return rt->chars16[b4 + idx - rt->b2_2_lower];
		}
	}
	else if (l == 1)
	{
		/* 1-byte code */

		/* check code validity - first byte */
		if (b4 < rt->b1_lower || b4 > rt->b1_upper)
			return 0;

		/* perform lookup */
		if (rt->chars32)
			return rt->chars32[b4 + rt->b1root - rt->b1_lower];
		else
			return rt->chars16[b4 + rt->b1root - rt->b1_lower];
	}
	return 0;					/* shouldn't happen */
}

int
UtfToLocal(const unsigned char *utf, int len,
		   unsigned char *iso,
		   const pg_mb_radix_tree *map,
		   const pg_utf_to_local_combined *cmap, int cmapsize,
		   utf_local_conversion_func conv_func,
		   int encoding, bool noError)
{
	uint32		iutf;
	int			l;
	const pg_utf_to_local_combined *cp;
	const unsigned char *start = utf;

	if (!PG_VALID_ENCODING(encoding))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid encoding number: %d", encoding)));

	for (; len > 0; len -= l)
	{
		unsigned char b1 = 0;
		unsigned char b2 = 0;
		unsigned char b3 = 0;
		unsigned char b4 = 0;

		/* "break" cases all represent errors */
		if (*utf == '\0')
			break;

		l = pg_utf_mblen(utf);
		if (len < l)
			break;

		if (!pg_utf8_islegal(utf, l))
			break;

		if (l == 1)
		{
			/* ASCII case is easy, assume it's one-to-one conversion */
			*iso++ = *utf++;
			continue;
		}

		/* collect coded char of length l */
		if (l == 2)
		{
			b3 = *utf++;
			b4 = *utf++;
		}
		else if (l == 3)
		{
			b2 = *utf++;
			b3 = *utf++;
			b4 = *utf++;
		}
		else if (l == 4)
		{
			b1 = *utf++;
			b2 = *utf++;
			b3 = *utf++;
			b4 = *utf++;
		}
		else
		{
			elog(ERROR, "unsupported character length %d", l);
			iutf = 0;			/* keep compiler quiet */
		}
		iutf = (b1 << 24 | b2 << 16 | b3 << 8 | b4);

		/* First, try with combined map if possible */
		if (cmap && len > l)
		{
			const unsigned char *utf_save = utf;
			int			len_save = len;
			int			l_save = l;

			/* collect next character, same as above */
			len -= l;

			l = pg_utf_mblen(utf);
			if (len < l)
			{
				/* need more data to decide if this is a combined char */
				utf -= l_save;
				break;
			}

			if (!pg_utf8_islegal(utf, l))
			{
				if (!noError)
					report_invalid_encoding(PG_UTF8, (const char *) utf, len);
				utf -= l_save;
				break;
			}

			/* We assume ASCII character cannot be in combined map */
			if (l > 1)
			{
				uint32		iutf2;
				uint32		cutf[2];

				if (l == 2)
				{
					iutf2 = *utf++ << 8;
					iutf2 |= *utf++;
				}
				else if (l == 3)
				{
					iutf2 = *utf++ << 16;
					iutf2 |= *utf++ << 8;
					iutf2 |= *utf++;
				}
				else if (l == 4)
				{
					iutf2 = *utf++ << 24;
					iutf2 |= *utf++ << 16;
					iutf2 |= *utf++ << 8;
					iutf2 |= *utf++;
				}
				else
				{
					elog(ERROR, "unsupported character length %d", l);
					iutf2 = 0;	/* keep compiler quiet */
				}

				cutf[0] = iutf;
				cutf[1] = iutf2;

				cp = bsearch(cutf, cmap, cmapsize,
							 sizeof(pg_utf_to_local_combined), compare3);

				if (cp)
				{
					iso = store_coded_char(iso, cp->code);
					continue;
				}
			}

			/* fail, so back up to reprocess second character next time */
			utf = utf_save;
			len = len_save;
			l = l_save;
		}

		/* Now check ordinary map */
		if (map)
		{
			uint32		converted = pg_mb_radix_conv(map, l, b1, b2, b3, b4);

			if (converted)
			{
				iso = store_coded_char(iso, converted);
				continue;
			}
		}

		/* if there's a conversion function, try that */
		if (conv_func)
		{
			uint32		converted = (*conv_func) (iutf);

			if (converted)
			{
				iso = store_coded_char(iso, converted);
				continue;
			}
		}

		/* failed to translate this character */
		utf -= l;
		if (noError)
			break;
		report_untranslatable_char(PG_UTF8, encoding,
								   (const char *) utf, len);
	}

	/* if we broke out of loop early, must be invalid input */
	if (len > 0 && !noError)
		report_invalid_encoding(PG_UTF8, (const char *) utf, len);

	*iso = '\0';

	return utf - start;
}

int
LocalToUtf(const unsigned char *iso, int len,
		   unsigned char *utf,
		   const pg_mb_radix_tree *map,
		   const pg_local_to_utf_combined *cmap, int cmapsize,
		   utf_local_conversion_func conv_func,
		   int encoding,
		   bool noError)
{
	uint32		iiso;
	int			l;
	const pg_local_to_utf_combined *cp;
	const unsigned char *start = iso;

	if (!PG_VALID_ENCODING(encoding))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid encoding number: %d", encoding)));

	for (; len > 0; len -= l)
	{
		unsigned char b1 = 0;
		unsigned char b2 = 0;
		unsigned char b3 = 0;
		unsigned char b4 = 0;

		/* "break" cases all represent errors */
		if (*iso == '\0')
			break;

		if (!IS_HIGHBIT_SET(*iso))
		{
			/* ASCII case is easy, assume it's one-to-one conversion */
			*utf++ = *iso++;
			l = 1;
			continue;
		}

		l = pg_encoding_verifymbchar(encoding, (const char *) iso, len);
		if (l < 0)
			break;

		/* collect coded char of length l */
		if (l == 1)
			b4 = *iso++;
		else if (l == 2)
		{
			b3 = *iso++;
			b4 = *iso++;
		}
		else if (l == 3)
		{
			b2 = *iso++;
			b3 = *iso++;
			b4 = *iso++;
		}
		else if (l == 4)
		{
			b1 = *iso++;
			b2 = *iso++;
			b3 = *iso++;
			b4 = *iso++;
		}
		else
		{
			elog(ERROR, "unsupported character length %d", l);
			iiso = 0;			/* keep compiler quiet */
		}
		iiso = (b1 << 24 | b2 << 16 | b3 << 8 | b4);

		if (map)
		{
			uint32		converted = pg_mb_radix_conv(map, l, b1, b2, b3, b4);

			if (converted)
			{
				utf = store_coded_char(utf, converted);
				continue;
			}

			/* If there's a combined character map, try that */
			if (cmap)
			{
				cp = bsearch(&iiso, cmap, cmapsize,
							 sizeof(pg_local_to_utf_combined), compare4);

				if (cp)
				{
					utf = store_coded_char(utf, cp->utf1);
					utf = store_coded_char(utf, cp->utf2);
					continue;
				}
			}
		}

		/* if there's a conversion function, try that */
		if (conv_func)
		{
			uint32		converted = (*conv_func) (iiso);

			if (converted)
			{
				utf = store_coded_char(utf, converted);
				continue;
			}
		}

		/* failed to translate this character */
		iso -= l;
		if (noError)
			break;
		report_untranslatable_char(encoding, PG_UTF8,
								   (const char *) iso, len);
	}

	/* if we broke out of loop early, must be invalid input */
	if (len > 0 && !noError)
		report_invalid_encoding(encoding, (const char *) iso, len);

	*utf = '\0';

	return iso - start;
}

