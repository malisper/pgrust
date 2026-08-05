/*
 * SHIM lib/stringinfo.h — NOT PostgreSQL code. (tsq oracle family)
 * StringInfoData with the upstream field set (src/include/lib/stringinfo.h);
 * only the operations pqformat.h (shim) and ts_locale.h (struct field only)
 * need. Growth in initStringInfo/appendBinaryStringInfo is buffer plumbing
 * over the family arena, never compared output.
 */
#ifndef PG_DIFFFUZZ_TSQ_SHIM_STRINGINFO_H
#define PG_DIFFFUZZ_TSQ_SHIM_STRINGINFO_H

#include "postgres.h"

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

/* ErrorContextCallback appears only as a ts_locale.h struct field */
typedef struct ErrorContextCallback
{
	struct ErrorContextCallback *previous;
	void		(*callback) (void *arg);
	void	   *arg;
} ErrorContextCallback;

#endif
