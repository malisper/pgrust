/*
 * SHIM postgres_fe.h — NOT PostgreSQL code.
 *
 * Minimal FRONTEND environment so the VERBATIM vendored files under ../
 * (src/common/{md5,sha1,sha2,cryptohash,hmac,md5_common,scram-common,base64}.c,
 * src/port/pg_crc32c_sb8.c, src/backend/utils/hash/pg_crc.c, copied
 * unmodified from postgres-src @ 62d6c7d3df6287f1bd83199c1a746e50d31571a0,
 * REL_18 = PostgreSQL 18.3) compile standalone for the native
 * differential-fuzz build. Plumbing only, never logic:
 *   - fixed-width typedefs matching c.h on LP64;
 *   - Assert compiled out (matches a production NDEBUG PostgreSQL build);
 *   - _() gettext marker -> identity (feeds only error strings whose text
 *     is out of the comparison planes);
 *   - explicit_bzero -> memset(...,0,...): zeroization strength is a
 *     hygiene property of the oracle process, never a compared output;
 *   - no configure-driven USE_*_CRC32C / WORDS_BIGENDIAN symbols defined,
 *     so port/pg_crc32c.h selects the portable sb8 arm — the same code
 *     every PostgreSQL build without hardware CRC runs.
 *
 * The FRONTEND arms of the vendored files (malloc/free, no
 * CHECK_FOR_INTERRUPTS) are selected via -DFRONTEND in build.rs: identical
 * digest/keys logic, backend-only plumbing compiled out.
 */
#ifndef PG_DIFFFUZZ_SHIM_POSTGRES_FE_H
#define PG_DIFFFUZZ_SHIM_POSTGRES_FE_H

#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <stddef.h>

typedef int8_t int8;
typedef int16_t int16;
typedef int32_t int32;
typedef int64_t int64;
typedef uint8_t uint8;
typedef uint16_t uint16;
typedef uint32_t uint32;
typedef uint64_t uint64;

#define UINT64CONST(x) UINT64_C(x)
#define INT64CONST(x) INT64_C(x)

#ifndef Assert
#define Assert(x) ((void) 0)
#endif

#ifndef _
#define _(x) (x)
#endif

#ifndef PGDLLIMPORT
#define PGDLLIMPORT
#endif

#ifndef pg_nodiscard
#define pg_nodiscard
#endif

#ifndef explicit_bzero
#define explicit_bzero(p, n) memset((p), 0, (n))
#endif

#ifndef unlikely
#define unlikely(x) (x)
#endif
#ifndef likely
#define likely(x) (x)
#endif

#endif

/* c.h lengthof — its exact upstream definition. */
#ifndef lengthof
#define lengthof(array) (sizeof (array) / sizeof ((array)[0]))
#endif
