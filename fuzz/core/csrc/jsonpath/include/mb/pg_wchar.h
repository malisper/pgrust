/*
 * SHIM mb/pg_wchar.h for the jsonpath_diff oracle — NOT PostgreSQL code.
 *
 * ENCODING PIN: the crate under test pins server encoding UTF-8 (the fuzz
 * harness Rust side calls mbutils::SetDatabaseEncoding(PG_UTF8)); this
 * header + pg_mbutils_min.c pin the C oracle the same way. Enum value
 * PG_UTF8 and the static inline unicode helpers below are VERBATIM from
 * src/include/mb/pg_wchar.h @ 62d6c7d3df (PostgreSQL 18.3); the rest is
 * declaration plumbing for the vendored function bodies.
 */
#ifndef PG_WCHAR_H
#define PG_WCHAR_H

#include "postgres.h"

typedef unsigned int pg_wchar;

/* pg_wchar.h VERBATIM values */
#define MAX_MULTIBYTE_CHAR_LEN	4
#define MAX_UNICODE_EQUIVALENT_STRING	16

typedef enum pg_enc
{
	PG_SQL_ASCII = 0,
	PG_EUC_JP,
	PG_EUC_CN,
	PG_EUC_KR,
	PG_EUC_TW,
	PG_EUC_JIS_2004,
	PG_UTF8,
	PG_MULE_INTERNAL,
	PG_LATIN1,
} pg_enc;

/* vendored bodies (pg_mbutils_min.c / pg_support_min.c) */
extern int	pg_mblen(const char *mbstr);
extern int	pg_mblen_unbounded(const char *mbstr);
extern int	pg_mblen_range(const char *mbstr, const char *end);
extern int	pg_mb2wchar_with_len(const char *from, pg_wchar *to, int len);
extern void pg_unicode_to_server(pg_wchar c, unsigned char *s);
extern bool pg_unicode_to_server_noerror(pg_wchar c, unsigned char *s);
extern int	GetDatabaseEncoding(void);
extern const char *GetDatabaseEncodingName(void);
extern char *pg_server_to_client(const char *s, int len);
extern int	pg_utf_mblen(const unsigned char *s);
extern bool pg_utf8_islegal(const unsigned char *source, int length);
extern int	pg_char_and_wchar_strncmp(const char *s1, const pg_wchar *s2, size_t n);

/* ---- static inline unicode helpers, VERBATIM pg_wchar.h @ 18.3 ---- */

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

/* same-encoding arm model (shim; see pg_jsonpath_exec_env.c): under the
 * UTF-8 pin every conversion is the identity, exactly mbutils.c's
 * src==dest arm. Only reachable when server encoding != UTF-8, i.e. never
 * here. */
extern char *pg_server_to_any(const char *s, int len, int encoding);
#endif							/* PG_WCHAR_H */
