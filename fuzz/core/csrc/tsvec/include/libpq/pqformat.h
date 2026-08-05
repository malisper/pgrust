/* SHIM libpq/pqformat.h (tsvec oracle) — NOT PostgreSQL code.
 * Declarations of the wire helpers tsvectorsend/tsvectorrecv call;
 * byte-exact implementations (big-endian ints, identity encoding
 * conversion — client==server encoding) in pg_tsvector_core_io.c.
 * Sanctioned "wire triples" plumbing shim (scaffold header). */
#ifndef PG_DIFFFUZZ_TSVEC_PQFORMAT_H
#define PG_DIFFFUZZ_TSVEC_PQFORMAT_H
#include "lib/stringinfo.h"
extern void pq_begintypsend(StringInfo buf);
extern bytea *pq_endtypsend(StringInfo buf);
extern void pq_sendbyte(StringInfo buf, uint8 byt);
extern void pq_sendint16(StringInfo buf, uint16 i);
extern void pq_sendint32(StringInfo buf, uint32 i);
extern void pq_sendtext(StringInfo buf, const char *str, int slen);
extern unsigned int pq_getmsgint(StringInfo msg, int b);
extern const char *pq_getmsgstring(StringInfo msg);
#endif
