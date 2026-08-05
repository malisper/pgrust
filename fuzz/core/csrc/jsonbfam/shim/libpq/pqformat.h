/* SHIM libpq/pqformat.h — decls for the verbatim pqformat.c segment pasted
 * into the driver TU; pq_sendint8 inline verbatim from the real header. */
#ifndef PG_JSONBFAM_SHIM_PQFORMAT_H
#define PG_JSONBFAM_SHIM_PQFORMAT_H
#include "postgres.h"
#include "lib/stringinfo.h"
extern void pq_begintypsend(StringInfo buf);
extern bytea *pq_endtypsend(StringInfo buf);
extern void pq_sendtext(StringInfo buf, const char *str, int slen);
extern unsigned int pq_getmsgint(StringInfo msg, int b);
extern const char *pq_getmsgbytes(StringInfo msg, int datalen);
extern char *pq_getmsgtext(StringInfo msg, int rawbytes, int *nbytes);
/* verbatim src/include/libpq/pqformat.h pq_sendintN cores */
static inline void
pq_sendint8(StringInfo buf, uint8 i)
{
	enlargeStringInfo(buf, sizeof(uint8));
	*((uint8 *) (buf->data + buf->len)) = i;
	buf->len += sizeof(uint8);
}
#endif
