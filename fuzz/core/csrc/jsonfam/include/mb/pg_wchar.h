/*
 * jsonfam shim mb/pg_wchar.h — exactly the pieces the vendored jsonapi.c /
 * json oracle needs, with the server encoding PINNED to UTF8 (the fuzz
 * contract pins GetDatabaseEncoding() == PG_UTF8 on the Rust side too; the
 * non-UTF8 conversion arm is a recorded encoding-carve).
 *
 * All function bodies below are VERBATIM from PostgreSQL 18.3
 * (62d6c7d3df): is_valid_unicode_codepoint / is_utf16_surrogate_first /
 * is_utf16_surrogate_second / surrogate_pair_to_codepoint / unicode_to_utf8
 * from src/include/mb/pg_wchar.h; pg_utf_mblen from src/common/wchar.c.
 * pg_encoding_mblen_or_incomplete and pg_unicode_to_server_noerror are
 * UTF8-pinned shims (documented in pg_json_io.c's header).
 */
#ifndef PG_JSONFAM_PG_WCHAR_H
#define PG_JSONFAM_PG_WCHAR_H

#include "postgres.h"

typedef unsigned int pg_wchar;

#define PG_UTF8 6
#define MAX_UNICODE_EQUIVALENT_STRING	16

static inline bool
is_valid_unicode_codepoint(pg_wchar c)
{
	return (c > 0 && c <= 0x10FFFF);
}

static inline bool
is_utf16_surrogate_first(pg_wchar c)
{
	return (c >= 0xD800 && c <= 0xDBFF);
}

static inline bool
is_utf16_surrogate_second(pg_wchar c)
{
	return (c >= 0xDC00 && c <= 0xDFFF);
}

static inline pg_wchar
surrogate_pair_to_codepoint(pg_wchar first, pg_wchar second)
{
	return ((first & 0x3FF) << 10) + 0x10000 + (second & 0x3FF);
}

/* VERBATIM from src/include/mb/pg_wchar.h (unicode_to_utf8) */
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

/* VERBATIM from src/common/wchar.c (pg_utf_mblen) */
static inline int
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
	else
		len = 1;
	return len;
}

/*
 * UTF8-pinned shims. UTF8's mblen never reads past the first byte, so the
 * "or_incomplete" distinction collapses to pg_utf_mblen (upstream
 * pg_encoding_mblen_or_incomplete returns the same for PG_UTF8).
 */
static inline int
pg_encoding_mblen_or_incomplete(int encoding, const char *mbstr, size_t remaining)
{
	if (encoding != PG_UTF8)
		abort();				/* oracle is UTF8-pinned */
	(void) remaining;
	return pg_utf_mblen((const unsigned char *) mbstr);
}

static inline int
pg_encoding_mblen(int encoding, const char *mbstr)
{
	if (encoding != PG_UTF8)
		abort();				/* oracle is UTF8-pinned */
	return pg_utf_mblen((const unsigned char *) mbstr);
}

/* mbutils.c seams, UTF8-pinned (documented in pg_json_io.c header). */
static inline int
GetDatabaseEncoding(void)
{
	return PG_UTF8;
}

static inline const char *
GetDatabaseEncodingName(void)
{
	return "UTF8";
}

static inline bool
pg_unicode_to_server_noerror(pg_wchar c, unsigned char *s)
{
	/* UTF8 server encoding: every valid code point converts. */
	if (!is_valid_unicode_codepoint(c))
		return false;
	unicode_to_utf8(c, s);
	s[pg_utf_mblen(s)] = '\0';
	return true;
}

#endif							/* PG_JSONFAM_PG_WCHAR_H */
