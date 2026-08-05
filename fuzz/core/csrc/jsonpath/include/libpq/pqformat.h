/*
 * SHIM libpq/pqformat.h for the jsonpath_diff oracle — NOT PostgreSQL code.
 * Declarations for the VERBATIM pqformat.c extracts in pg_support_min.c,
 * plus the pq_writeint8/pq_sendint8 static inlines VERBATIM from
 * src/include/libpq/pqformat.h @ 18.3 (the only sendint width jsonpath
 * uses).
 */
#ifndef PQFORMAT_H
#define PQFORMAT_H

#include "postgres.h"
#include "lib/stringinfo.h"
#include "mb/pg_wchar.h"

extern void pq_begintypsend(StringInfo buf);
extern bytea *pq_endtypsend(StringInfo buf);
extern void pq_sendtext(StringInfo buf, const char *str, int slen);
extern unsigned int pq_getmsgint(StringInfo msg, int b);
extern const char *pq_getmsgbytes(StringInfo msg, int datalen);
extern void pq_copymsgbytes(StringInfo msg, void *buf, int datalen);
extern char *pq_getmsgtext(StringInfo msg, int rawbytes, int *nbytes);

/* ---- VERBATIM pqformat.h @ 18.3 ---- */

static inline void
pq_writeint8(StringInfoData *pg_restrict buf, uint8 i)
{
	uint8		ni = i;

	Assert(buf->len + (int) sizeof(uint8) <= buf->maxlen);
	memcpy((char *pg_restrict) (buf->data + buf->len), &ni, sizeof(uint8));
	buf->len += sizeof(uint8);
}

/* append a binary [u]int8 to a StringInfo buffer */
static inline void
pq_sendint8(StringInfo buf, uint8 i)
{
	enlargeStringInfo(buf, sizeof(uint8));
	pq_writeint8(buf, i);
}

#endif							/* PQFORMAT_H */
