/*
 * SHIM varatt.h for the contribb_diff oracle: the 4-byte-header varlena
 * macros the vendored cube bodies use, little-endian encoding exactly as
 * src/include/varatt.h (the contribb build targets are LE-only; the
 * assemble script's smoke gate asserts it).
 */
#ifndef PG_CB_VARATT_H
#define PG_CB_VARATT_H

#include "postgres.h"

#if defined(__BYTE_ORDER__) && (__BYTE_ORDER__ != __ORDER_LITTLE_ENDIAN__)
#error "contribb oracle shim supports little-endian targets only"
#endif

#define VARHDRSZ		((int32) sizeof(int32))

#define SET_VARSIZE(PTR, len) \
	(((union { uint32 u; char c[4]; } *) (PTR))->u = ((uint32) (len)) << 2)
#define VARSIZE(PTR) \
	((((const union { uint32 u; char c[4]; } *) (PTR))->u) >> 2)
#define VARDATA(PTR) (((char *) (PTR)) + VARHDRSZ)

#endif							/* PG_CB_VARATT_H */
