/*
 * SHIM postgres_fe.h (cryptbe family) — NOT PostgreSQL code. Wraps the
 * cryptofam shim environment and adds the handful of c.h/memutils.h
 * plumbing symbols the verbatim vendored headers/files in this directory
 * reference (values transcribed verbatim from src/include/c.h and
 * src/include/utils/memutils.h):
 *   - pg_restrict -> restrict; pg_noreturn -> empty (declaration attribute);
 *   - Oid/Size typedefs; lengthof; MaxAllocSize 0x3fffffff.
 */
#ifndef PG_DIFFFUZZ_CRYPTBE_POSTGRES_FE_H
#define PG_DIFFFUZZ_CRYPTBE_POSTGRES_FE_H

#include "../../cryptofam/shim_fe/postgres_fe.h"

#define pg_restrict restrict
#ifndef pg_noreturn
#define pg_noreturn
#endif
typedef unsigned int Oid;
typedef size_t Size;
#ifndef lengthof
#define lengthof(array) (sizeof (array) / sizeof ((array)[0]))
#endif
#ifndef MaxAllocSize
#define MaxAllocSize ((Size) 0x3fffffff)	/* 1 gigabyte - 1 */
#endif

#endif
