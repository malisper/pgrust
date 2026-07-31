/* SHIM lib/stringinfo.h (tsvec oracle) — NOT PostgreSQL code.
 * StringInfoData layout per src/include/lib/stringinfo.h (fields the
 * vendored code and the pq shims in pg_tsvector_core_io.c touch). */
#ifndef PG_DIFFFUZZ_TSVEC_STRINGINFO_H
#define PG_DIFFFUZZ_TSVEC_STRINGINFO_H
typedef struct StringInfoData
{
	char	   *data;
	int			len;
	int			maxlen;
	int			cursor;
} StringInfoData;
typedef StringInfoData *StringInfo;
extern void initStringInfo(StringInfo str);
extern void appendBinaryStringInfo(StringInfo str, const void *data, int datalen);
extern void appendStringInfoChar(StringInfo str, char ch);
#endif
