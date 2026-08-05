/*
 * SHIM lib/stringinfo.h — NOT PostgreSQL code (libfam_diff oracle).
 *
 * lib/pairingheap.h includes this unconditionally, but the only consumer
 * (pairingheap_dump) is compiled out (PAIRINGHEAP_DEBUG undefined, upstream
 * default). Opaque declaration only; no StringInfo code is vendored or
 * fabricated.
 */
#ifndef STRINGINFO_H
#define STRINGINFO_H

typedef struct StringInfoData *StringInfo;

#endif							/* STRINGINFO_H */
