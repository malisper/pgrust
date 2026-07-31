/*
 * SHIM postgres.h — NOT PostgreSQL code.
 *
 * Minimal environment so the VERBATIM vendored files under ../ryu/
 * (src/common/d2s.c, f2s.c and their headers, copied unmodified from
 * postgres-src @ 62d6c7d3df6287f1bd83199c1a746e50d31571a0, REL_18) compile
 * standalone for the native differential-fuzz build. Plumbing only, never
 * logic: fixed-width typedefs matching c.h on LP64, UINT64CONST, and a
 * no-op Assert (the real NDEBUG build compiles Assert out the same way).
 */
#ifndef PG_DIFFFUZZ_SHIM_POSTGRES_H
#define PG_DIFFFUZZ_SHIM_POSTGRES_H

#include <stddef.h>					/* offsetof, for the abbrev-table builder */
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <stdlib.h>

typedef int8_t int8;
typedef int16_t int16;
typedef int32_t int32;
typedef int64_t int64;
typedef uint8_t uint8;
typedef uint16_t uint16;
typedef uint32_t uint32;
typedef uint64_t uint64;
typedef double float8;
typedef float float4;

#define UINT64CONST(x) UINT64_C(x)
#define INT64CONST(x) INT64_C(x)

/* d2s.c/f2s.c's palloc-returning wrappers (unused by the fuzz oracle) */
#define palloc(n) malloc(n)

#ifndef Assert
#define Assert(x) ((void) 0)
#endif

#ifndef unlikely
#define unlikely(x) (x)
#endif
#ifndef likely
#define likely(x) (x)
#endif

#endif
