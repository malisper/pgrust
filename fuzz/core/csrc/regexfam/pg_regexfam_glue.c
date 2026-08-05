/*
 * pg_regexfam_glue.c — mb/ctype glue for the regexp_diff oracle (p1-laneag).
 *
 * Provenance (bodies VERBATIM unless a shim is listed), from the repo's
 * vendored ground-truth checkout ../pgrust-fabled/vendor/postgres-src @
 * 62d6c7d3df6287f1bd83199c1a746e50d31571a0 (PostgreSQL 18.3, Stamp-18.3):
 *   - src/common/wchar.c: pg_utf2wchar_with_len, pg_wchar2utf_with_len
 *     (static there; exported here under pg_regexfam_ names — rename only,
 *     bodies verbatim incl. the MB2CHAR_NEED_AT_LEAST macro), pg_utf_mblen
 *     (verbatim; kept static to this TU + re-exported through the
 *     UTF8-pinned pg_mblen below).
 *   - src/include/mb/pg_wchar.h: unicode_to_utf8 (static inline, verbatim).
 *   - src/port/pgstrcasecmp.c: pg_ascii_toupper, pg_ascii_tolower
 *     (verbatim; linked under pg_regexfam_ prefix via the shim header).
 *
 * Shims (plumbing only, never logic):
 *   - ENCODING PIN: the oracle runs with the database encoding pinned to
 *     PG_UTF8 (the Rust side pins mbutils::SetDatabaseEncoding(PG_UTF8);
 *     see the regexp_diff.rs module header).  GetDatabaseEncoding() is the
 *     constant PG_UTF8 and pg_database_encoding_max_length() the constant 4
 *     (pg_wchar_table[PG_UTF8].maxmblen).  The mbutils.c entry points below
 *     (pg_mb2wchar_with_len, pg_wchar2mb_with_len, pg_mblen, pg_mblen_range,
 *     pg_mbstrlen_with_len) are the real functions' bodies with the
 *     pg_wchar_table[DatabaseEncoding->encoding] indirection resolved to the
 *     UTF-8 row at pin time — the same one-row resolution pg_like_io.c and
 *     pg_name_io.c document.
 *   - pg_mblen_range's report_invalid_encoding_db arm -> abort(): the driver
 *     enforces valid-UTF-8 NUL-free inputs (server text invariant), under
 *     which a character can never overrun its buffer, so the arm is dead;
 *     abort keeps it loud rather than silently mis-encoding.
 *   - Locale/unicode stubs (pg_newlocale_from_collation, the pg_u_* class
 *     probes, unicode_*case_simple): collation is pinned to C_COLLATION_OID
 *     so regc_pg_locale.c's strategy is PG_REGEX_STRATEGY_C and none of
 *     these can be reached; they abort() loudly if ever called.
 */

#include "postgres.h"
#include "mb/pg_wchar.h"
#include "common/unicode_case.h"
#include "common/unicode_category.h"
#include "utils/pg_locale.h"

/* ---- src/common/wchar.c line 67 (verbatim) ---- */
#define MB2CHAR_NEED_AT_LEAST(len, need) if ((len) < (need)) break

/* ---- src/include/mb/pg_wchar.h: unicode_to_utf8 (VERBATIM) ---- */
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

/* ---- src/common/wchar.c: pg_utf2wchar_with_len (VERBATIM body; static in
 * wchar.c, exported here under the pg_regexfam_ name) ---- */
int
pg_regexfam_utf2wchar_with_len(const unsigned char *from, pg_wchar *to, int len)
{
	int			cnt = 0;
	uint32		c1,
				c2,
				c3,
				c4;

	while (len > 0 && *from)
	{
		if ((*from & 0x80) == 0)
		{
			*to = *from++;
			len--;
		}
		else if ((*from & 0xe0) == 0xc0)
		{
			MB2CHAR_NEED_AT_LEAST(len, 2);
			c1 = *from++ & 0x1f;
			c2 = *from++ & 0x3f;
			*to = (c1 << 6) | c2;
			len -= 2;
		}
		else if ((*from & 0xf0) == 0xe0)
		{
			MB2CHAR_NEED_AT_LEAST(len, 3);
			c1 = *from++ & 0x0f;
			c2 = *from++ & 0x3f;
			c3 = *from++ & 0x3f;
			*to = (c1 << 12) | (c2 << 6) | c3;
			len -= 3;
		}
		else if ((*from & 0xf8) == 0xf0)
		{
			MB2CHAR_NEED_AT_LEAST(len, 4);
			c1 = *from++ & 0x07;
			c2 = *from++ & 0x3f;
			c3 = *from++ & 0x3f;
			c4 = *from++ & 0x3f;
			*to = (c1 << 18) | (c2 << 12) | (c3 << 6) | c4;
			len -= 4;
		}
		else
		{
			/* treat a bogus char as length 1; not ours to raise error */
			*to = *from++;
			len--;
		}
		to++;
		cnt++;
	}
	*to = 0;
	return cnt;
}

/* ---- src/common/wchar.c: pg_utf_mblen (VERBATIM) ---- */
static int
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
pg_regexfam_utf_mblen(const unsigned char *s)
{
	return pg_utf_mblen(s);
}

/* ---- src/common/wchar.c: pg_wchar2utf_with_len (VERBATIM body; static in
 * wchar.c, exported here under the pg_regexfam_ name) ---- */
int
pg_regexfam_wchar2utf_with_len(const pg_wchar *from, unsigned char *to, int len)
{
	int			cnt = 0;

	while (len > 0 && *from)
	{
		int			char_len;

		unicode_to_utf8(*from, to);
		char_len = pg_utf_mblen(to);
		cnt += char_len;
		to += char_len;
		from++;
		len--;
	}
	*to = 0;
	return cnt;
}

/* ---- src/port/pgstrcasecmp.c: ascii case folding (VERBATIM) ---- */
unsigned char
pg_ascii_toupper(unsigned char ch)
{
	if (ch >= 'a' && ch <= 'z')
		ch += 'A' - 'a';
	return ch;
}

unsigned char
pg_ascii_tolower(unsigned char ch)
{
	if (ch >= 'A' && ch <= 'Z')
		ch += 'a' - 'A';
	return ch;
}

/* ---- src/backend/utils/mb/wstrncmp.c: pg_char_and_wchar_strncmp
 * (VERBATIM) ---- */
int
pg_char_and_wchar_strncmp(const char *s1, const pg_wchar *s2, size_t n)
{
	if (n == 0)
		return 0;
	do
	{
		if ((pg_wchar) ((unsigned char) *s1) != *s2++)
			return ((pg_wchar) ((unsigned char) *s1) - *(s2 - 1));
		if (*s1++ == 0)
			break;
	} while (--n != 0);
	return 0;
}

/* ---- encoding pin (shim; see file header) ---- */
int
GetDatabaseEncoding(void)
{
	return PG_UTF8;
}

int
pg_database_encoding_max_length(void)
{
	return 4;					/* pg_wchar_table[PG_UTF8].maxmblen */
}

/* ---- mbutils.c entry points, UTF-8 row resolved (shim; see header) ---- */
int
pg_mb2wchar_with_len(const char *from, pg_wchar *to, int len)
{
	return pg_regexfam_utf2wchar_with_len((const unsigned char *) from, to, len);
}

int
pg_wchar2mb_with_len(const pg_wchar *from, char *to, int len)
{
	return pg_regexfam_wchar2utf_with_len(from, (unsigned char *) to, len);
}

int
pg_mblen(const char *mbstr)
{
	return pg_utf_mblen((const unsigned char *) mbstr);
}

/* mbutils.c pg_mblen_range — VERBATIM control flow; the
 * report_invalid_encoding_db arm is dead under the valid-UTF-8 input
 * invariant and aborts loudly (see header). */
int
pg_mblen_range(const char *mbstr, const char *end)
{
	int			length = pg_utf_mblen((const unsigned char *) mbstr);

	Assert(end > mbstr);

	if (mbstr + length > end)
	{
		fprintf(stderr, "regexfam oracle: pg_mblen_range overrun on validated UTF-8\n");
		abort();
	}

	return length;
}

/* mbutils.c pg_mblen_with_len — same dead error arm as pg_mblen_range. */
static int
pg_mblen_with_len(const char *mbstr, int limit)
{
	int			length = pg_utf_mblen((const unsigned char *) mbstr);

	Assert(limit >= 1);

	if (length > limit)
	{
		fprintf(stderr, "regexfam oracle: pg_mblen_with_len overrun on validated UTF-8\n");
		abort();
	}

	return length;
}

/* mbutils.c pg_mbstrlen_with_len — VERBATIM */
int
pg_mbstrlen_with_len(const char *mbstr, int limit)
{
	int			len = 0;

	/* optimization for single byte encoding */
	if (pg_database_encoding_max_length() == 1)
		return limit;

	while (limit > 0 && *mbstr)
	{
		int			l = pg_mblen_with_len(mbstr, limit);

		limit -= l;
		mbstr += l;
		len++;
	}
	return len;
}

/* ---- unreachable-locale stubs (shim; see header) ---- */
#define PG_REGEXFAM_LOCALE_STUB(name) \
	do { \
		fprintf(stderr, "regexfam oracle: %s reached under pinned C collation\n", name); \
		abort(); \
	} while (0)

pg_locale_t
pg_newlocale_from_collation(Oid collid)
{
	(void) collid;
	PG_REGEXFAM_LOCALE_STUB("pg_newlocale_from_collation");
}

pg_wchar
unicode_uppercase_simple(pg_wchar code)
{
	(void) code;
	PG_REGEXFAM_LOCALE_STUB("unicode_uppercase_simple");
}

pg_wchar
unicode_lowercase_simple(pg_wchar code)
{
	(void) code;
	PG_REGEXFAM_LOCALE_STUB("unicode_lowercase_simple");
}

bool
pg_u_isdigit(pg_wchar c, bool posix)
{
	(void) c;
	(void) posix;
	PG_REGEXFAM_LOCALE_STUB("pg_u_isdigit");
}

bool
pg_u_isalpha(pg_wchar c)
{
	(void) c;
	PG_REGEXFAM_LOCALE_STUB("pg_u_isalpha");
}

bool
pg_u_isalnum(pg_wchar c, bool posix)
{
	(void) c;
	(void) posix;
	PG_REGEXFAM_LOCALE_STUB("pg_u_isalnum");
}

bool
pg_u_isupper(pg_wchar c)
{
	(void) c;
	PG_REGEXFAM_LOCALE_STUB("pg_u_isupper");
}

bool
pg_u_islower(pg_wchar c)
{
	(void) c;
	PG_REGEXFAM_LOCALE_STUB("pg_u_islower");
}

bool
pg_u_isgraph(pg_wchar c)
{
	(void) c;
	PG_REGEXFAM_LOCALE_STUB("pg_u_isgraph");
}

bool
pg_u_isprint(pg_wchar c)
{
	(void) c;
	PG_REGEXFAM_LOCALE_STUB("pg_u_isprint");
}

bool
pg_u_ispunct(pg_wchar c, bool posix)
{
	(void) c;
	(void) posix;
	PG_REGEXFAM_LOCALE_STUB("pg_u_ispunct");
}

bool
pg_u_isspace(pg_wchar c)
{
	(void) c;
	PG_REGEXFAM_LOCALE_STUB("pg_u_isspace");
}
