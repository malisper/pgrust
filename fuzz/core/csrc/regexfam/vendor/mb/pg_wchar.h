#ifndef CREF_REGEX_PG_WCHAR_H
#define CREF_REGEX_PG_WCHAR_H

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

#endif
