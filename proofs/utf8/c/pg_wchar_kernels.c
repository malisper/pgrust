/*
 * Vendored verbatim from PostgreSQL src/common/wchar.c (master; bodies
 * cross-checked byte-equivalent to the REL_18_3 vendor copy at
 * pgrust-fabled/vendor/postgres-src, fetched 2026-07-28 from the repo's
 * proofs/utf8/wchar_full.c snapshot): the per-encoding mblen/dsplen
 * kernels plus the UTF-8 display-width chain
 * (utf8_to_unicode + mbbisearch + ucs_wcwidth + pg_utf_dsplen).
 *
 * Shims (plumbing only, never logic):
 *   - IS_HIGHBIT_SET / SS2 / SS3 / IS_LC1 / IS_LC2 / IS_LCPRV1 / IS_LCPRV2 /
 *     LCPRV1_A..LCPRV2_B: macro definitions copied from
 *     src/include/mb/pg_wchar.h (verbatim values).
 *   - pg_wchar typedef (unsigned int), as in pg_wchar.h.
 *   - `static` dropped from the kernels so the Kani FFI bridge can link
 *     them; every function keeps its original name and body.
 *   - The two mbinterval tables are the verbatim generated headers
 *     unicode_nonspacing_table.h / unicode_east_asian_fw_table.h
 *     (byte-identical copies of the REL_18_3 files, in this directory),
 *     #included inside ucs_wcwidth exactly as wchar.c does.
 */

typedef unsigned int pg_wchar;

#define IS_HIGHBIT_SET(ch)	((unsigned char)(ch) & 0x80)
#define SS2 0x8e				/* single shift 2 (JIS0201) */
#define SS3 0x8f				/* single shift 3 (JIS0212) */

#define LCPRV1_A		0x9a
#define LCPRV1_B		0x9b
#define LCPRV2_A		0x9c
#define LCPRV2_B		0x9d
#define IS_LC1(c)	((unsigned char)(c) >= 0x81 && (unsigned char)(c) <= 0x8d)
#define IS_LC2(c)	((unsigned char)(c) >= 0x90 && (unsigned char)(c) <= 0x99)
#define IS_LCPRV1(c)	((unsigned char)(c) == LCPRV1_A || (unsigned char)(c) == LCPRV1_B)
#define IS_LCPRV2(c)	((unsigned char)(c) == LCPRV2_A || (unsigned char)(c) == LCPRV2_B)

int
pg_ascii_mblen(const unsigned char *s)
{
	return 1;
}

int
pg_ascii_dsplen(const unsigned char *s)
{
	if (*s == '\0')
		return 0;
	if (*s < 0x20 || *s == 0x7f)
		return -1;

	return 1;
}

/*
 * EUC
 */
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
pg_euc_dsplen(const unsigned char *s)
{
	int			len;

	if (*s == SS2)
		len = 2;
	else if (*s == SS3)
		len = 2;
	else if (IS_HIGHBIT_SET(*s))
		len = 2;
	else
		len = pg_ascii_dsplen(s);
	return len;
}

/*
 * EUC_JP
 */
int
pg_eucjp_mblen(const unsigned char *s)
{
	return pg_euc_mblen(s);
}

int
pg_eucjp_dsplen(const unsigned char *s)
{
	int			len;

	if (*s == SS2)
		len = 1;
	else if (*s == SS3)
		len = 2;
	else if (IS_HIGHBIT_SET(*s))
		len = 2;
	else
		len = pg_ascii_dsplen(s);
	return len;
}

/*
 * EUC_KR
 */
int
pg_euckr_mblen(const unsigned char *s)
{
	return pg_euc_mblen(s);
}

int
pg_euckr_dsplen(const unsigned char *s)
{
	return pg_euc_dsplen(s);
}

/*
 * EUC_CN
 */
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
pg_euccn_dsplen(const unsigned char *s)
{
	int			len;

	if (IS_HIGHBIT_SET(*s))
		len = 2;
	else
		len = pg_ascii_dsplen(s);
	return len;
}

/*
 * EUC_TW
 */
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
pg_euctw_dsplen(const unsigned char *s)
{
	int			len;

	if (*s == SS2)
		len = 2;
	else if (*s == SS3)
		len = 2;
	else if (IS_HIGHBIT_SET(*s))
		len = 2;
	else
		len = pg_ascii_dsplen(s);
	return len;
}

/*
 * JOHAB
 */
int
pg_johab_mblen(const unsigned char *s)
{
	return pg_euc_mblen(s);
}

int
pg_johab_dsplen(const unsigned char *s)
{
	return pg_euc_dsplen(s);
}

/*
 * mbinterval binary search + UTF-8 display width
 */
struct mbinterval
{
	unsigned int first;
	unsigned int last;
};

/* auxiliary function for binary search in interval table */
int
mbbisearch(pg_wchar ucs, const struct mbinterval *table, int max)
{
	int			min = 0;
	int			mid;

	if (ucs < table[0].first || ucs > table[max].last)
		return 0;
	while (max >= min)
	{
		mid = (min + max) / 2;
		if (ucs > table[mid].last)
			min = mid + 1;
		else if (ucs < table[mid].first)
			max = mid - 1;
		else
			return 1;
	}

	return 0;
}

int
ucs_wcwidth(pg_wchar ucs)
{
#include "unicode_nonspacing_table.h"
#include "unicode_east_asian_fw_table.h"

	/* test for 8-bit control characters */
	if (ucs == 0)
		return 0;

	if (ucs < 0x20 || (ucs >= 0x7f && ucs < 0xa0) || ucs > 0x0010ffff)
		return -1;

	if (mbbisearch(ucs, nonspacing,
				   sizeof(nonspacing) / sizeof(struct mbinterval) - 1))
		return 0;

	/* binary search in table of wide characters */
	if (mbbisearch(ucs, east_asian_fw,
				   sizeof(east_asian_fw) / sizeof(struct mbinterval) - 1))
		return 2;

	return 1;
}

/* from src/include/mb/pg_wchar.h (static inline there; `static` dropped) */
pg_wchar
utf8_to_unicode(const unsigned char *c)
{
	if ((*c & 0x80) == 0)
		return (pg_wchar) c[0];
	else if ((*c & 0xe0) == 0xc0)
		return (pg_wchar) (((c[0] & 0x1f) << 6) |
						   (c[1] & 0x3f));
	else if ((*c & 0xf0) == 0xe0)
		return (pg_wchar) (((c[0] & 0x0f) << 12) |
						   ((c[1] & 0x3f) << 6) |
						   (c[2] & 0x3f));
	else if ((*c & 0xf8) == 0xf0)
		return (pg_wchar) (((c[0] & 0x07) << 18) |
						   ((c[1] & 0x3f) << 12) |
						   ((c[2] & 0x3f) << 6) |
						   (c[3] & 0x3f));
	else
		/* that is an invalid code on purpose */
		return 0xffffffff;
}

int
pg_utf_dsplen(const unsigned char *s)
{
	return ucs_wcwidth(utf8_to_unicode(s));
}

/*
 * ISO8859-1 (also used for all other single-byte server encodings)
 */
int
pg_latin1_mblen(const unsigned char *s)
{
	return 1;
}

int
pg_latin1_dsplen(const unsigned char *s)
{
	return pg_ascii_dsplen(s);
}

/*
 * SJIS
 */
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
pg_sjis_dsplen(const unsigned char *s)
{
	int			len;

	if (*s >= 0xa1 && *s <= 0xdf)
		len = 1;				/* 1 byte kana? */
	else if (IS_HIGHBIT_SET(*s))
		len = 2;				/* kanji? */
	else
		len = pg_ascii_dsplen(s);	/* should be ASCII */
	return len;
}

/*
 * Big5
 */
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
pg_big5_dsplen(const unsigned char *s)
{
	int			len;

	if (IS_HIGHBIT_SET(*s))
		len = 2;				/* kanji? */
	else
		len = pg_ascii_dsplen(s);	/* should be ASCII */
	return len;
}

/*
 * GBK
 */
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
pg_gbk_dsplen(const unsigned char *s)
{
	int			len;

	if (IS_HIGHBIT_SET(*s))
		len = 2;				/* kanji? */
	else
		len = pg_ascii_dsplen(s);	/* should be ASCII */
	return len;
}

/*
 * UHC
 */
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
pg_uhc_dsplen(const unsigned char *s)
{
	int			len;

	if (IS_HIGHBIT_SET(*s))
		len = 2;				/* 2byte? */
	else
		len = pg_ascii_dsplen(s);	/* should be ASCII */
	return len;
}

/*
 * GB18030
 *	Added by Bill Huang <bhuang@redhat.com>,<bill_huanghb@ybb.ne.jp>
 */
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
pg_gb18030_dsplen(const unsigned char *s)
{
	int			len;

	if (IS_HIGHBIT_SET(*s))
		len = 2;
	else
		len = pg_ascii_dsplen(s);	/* ASCII */
	return len;
}

/*
 * MULE internal (mblen exported in wchar.c; dsplen static there)
 */
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
pg_mule_dsplen(const unsigned char *s)
{
	int			len;

	/*
	 * Note: it's not really appropriate to assume that all multibyte charsets
	 * are double-wide on screen.  But this seems an okay approximation for
	 * the MULE charsets we currently support.
	 */

	if (IS_LC1(*s))
		len = 1;
	else if (IS_LCPRV1(*s))
		len = 1;
	else if (IS_LC2(*s))
		len = 2;
	else if (IS_LCPRV2(*s))
		len = 2;
	else
		len = 1;				/* assume ASCII */

	return len;
}

/* --- direct mbbisearch shims for the Kani FFI bridge (tables are
 * function-local statics in ucs_wcwidth, so re-include them here) --- */

int
c_in_nonspacing(pg_wchar ucs)
{
#include "unicode_nonspacing_table.h"
	return mbbisearch(ucs, nonspacing,
					  sizeof(nonspacing) / sizeof(struct mbinterval) - 1);
}

int
c_in_east_asian_fw(pg_wchar ucs)
{
#include "unicode_east_asian_fw_table.h"
	return mbbisearch(ucs, east_asian_fw,
					  sizeof(east_asian_fw) / sizeof(struct mbinterval) - 1);
}
