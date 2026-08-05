/*
 * SHIM libpq/pqformat.h — NOT PostgreSQL code. (tsq oracle family)
 *
 * The six wire helpers tsquerysend/tsqueryrecv use, semantics of
 * src/backend/libpq/pqformat.c (big-endian ints; NUL-terminated strings):
 *   - pq_getmsgint(buf, n): insufficient data -> ereport(ERROR,
 *     ERRCODE_PROTOCOL_VIOLATION) exactly like pq_getmsgbytes.
 *   - pq_getmsgstring: reads to NUL (missing NUL -> protocol violation);
 *     the client->server encoding conversion arm is identity-with-
 *     verification here (database encoding pinned UTF-8, client ==
 *     server): pg_verify_mbstr failure -> ereport(ERROR,
 *     ERRCODE_CHARACTER_NOT_IN_REPERTOIRE), matching pg_any_to_server on
 *     a same-encoding server.
 * Implemented in pg_tsq_shim.c over the family arena.
 */
#ifndef PG_DIFFFUZZ_TSQ_SHIM_PQFORMAT_H
#define PG_DIFFFUZZ_TSQ_SHIM_PQFORMAT_H

#include "postgres.h"
#include "lib/stringinfo.h"

extern void pq_begintypsend(StringInfo buf);
extern bytea *pq_endtypsend(StringInfo buf);
extern void pq_sendint8(StringInfo buf, uint8 i);
extern void pq_sendint16(StringInfo buf, uint16 i);
extern void pq_sendint32(StringInfo buf, uint32 i);
extern void pq_sendstring(StringInfo buf, const char *str);
extern unsigned int pq_getmsgint(StringInfo msg, int b);
extern const char *pq_getmsgstring(StringInfo msg);

#endif
