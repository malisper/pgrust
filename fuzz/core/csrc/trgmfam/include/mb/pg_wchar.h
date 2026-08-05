/* trgmfam shim mb/pg_wchar.h: pg_wchar typedef + the VERBATIM static-inline
 * UTF-8 helpers unicode_case.c uses (utf8_to_unicode, unicode_to_utf8,
 * unicode_utf8len: src/include/mb/pg_wchar.h lines 558-633 @ 62d6c7d3df)
 * + pg_utf_mblen resolved to the verbatim wfam copy in pg_wcharfam.c. */
#ifndef TRGMFAM_PG_WCHAR_H
#define TRGMFAM_PG_WCHAR_H
#include "postgres.h"
typedef unsigned int pg_wchar;
extern int wfam_pg_utf_mblen(const unsigned char *s);
#define pg_utf_mblen wfam_pg_utf_mblen
/* ---- VERBATIM src/include/mb/pg_wchar.h lines 558-633 @ 62d6c7d3df ---- */
/*
 * Convert a UTF-8 character to a Unicode code point.
 * This is a one-character version of pg_utf2wchar_with_len.
 *
 * No error checks here, c must point to a long-enough string.
 */
static inline pg_wchar
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

/*
 * Map a Unicode code point to UTF-8.  utf8string must have at least
 * unicode_utf8len(c) bytes available.
 */
static inline unsigned char *
unicode_to_utf8(pg_wchar c, unsigned char *utf8string)
{
	if (c <= 0x7F)
	{
		utf8string[0] = c;
	}
	else if (c <= 0x7FF)
	{
		utf8string[0] = 0xC0 | ((c >> 6) & 0x1F);
		utf8string[1] = 0x80 | (c & 0x3F);
	}
	else if (c <= 0xFFFF)
	{
		utf8string[0] = 0xE0 | ((c >> 12) & 0x0F);
		utf8string[1] = 0x80 | ((c >> 6) & 0x3F);
		utf8string[2] = 0x80 | (c & 0x3F);
	}
	else
	{
		utf8string[0] = 0xF0 | ((c >> 18) & 0x07);
		utf8string[1] = 0x80 | ((c >> 12) & 0x3F);
		utf8string[2] = 0x80 | ((c >> 6) & 0x3F);
		utf8string[3] = 0x80 | (c & 0x3F);
	}

	return utf8string;
}

/*
 * Number of bytes needed to represent the given char in UTF8.
 */
static inline int
unicode_utf8len(pg_wchar c)
{
	if (c <= 0x7F)
		return 1;
	else if (c <= 0x7FF)
		return 2;
	else if (c <= 0xFFFF)
		return 3;
	else
		return 4;
}
/* ---- end VERBATIM ---- */
#endif
