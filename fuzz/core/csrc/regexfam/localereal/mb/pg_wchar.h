/* Locale-probe variant of vendor/mb/pg_wchar.h (p1-lanew): identical shim
 * plus the utf8 helper declarations common/unicode_case.c compiles
 * against (definitions: localereal/pg_wchar_utf8.c, verbatim REL_18_3). */
#ifndef LPROBE_PG_WCHAR_H
#define LPROBE_PG_WCHAR_H

typedef unsigned int pg_wchar;

typedef enum pg_enc
{
	PG_SQL_ASCII = 0,
	PG_UTF8 = 6,
} pg_enc;

extern int	cref_database_encoding;
static inline int GetDatabaseEncoding(void) { return cref_database_encoding; }

/* wstrncmp.c:55 (verbatim body; compile-time class-name lookup only) */
static inline int
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

/* real src/include/mb/pg_wchar.h surface unicode_case.c uses */
extern pg_wchar utf8_to_unicode(const unsigned char *c);
extern unsigned char *unicode_to_utf8(pg_wchar c, unsigned char *utf8string);
extern int	pg_utf_mblen(const unsigned char *s);
extern int	unicode_utf8len(pg_wchar c);

#endif
