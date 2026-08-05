/*
 * SHIM c.h — NOT PostgreSQL code.
 *
 * Minimal environment so the VERBATIM vendored files under ../ (src/common
 * base64.c, md5.c, md5_common.c, sha1.c, sha2.c, cryptohash.c, hmac.c,
 * scram-common.c + src/port/pg_crc32c_sb8.c + src/backend/utils/hash/pg_crc.c,
 * copied unmodified from postgres-src @
 * 62d6c7d3df6287f1bd83199c1a746e50d31571a0, REL_18 "Stamp 18.3") compile
 * standalone for the native differential-fuzz build. Plumbing only, never
 * logic: fixed-width typedefs matching c.h on LP64 little-endian (the fleet
 * oracle platform), no-op Assert (NDEBUG parity), explicit_bzero fallback
 * (memset — src/port/explicit_bzero.c semantics; timing hygiene is not a
 * differential surface).
 */
#ifndef PG_DIFFFUZZ_HASHENC_SHIM_C_H
#define PG_DIFFFUZZ_HASHENC_SHIM_C_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

typedef int8_t int8;
typedef int16_t int16;
typedef int32_t int32;
typedef int64_t int64;
typedef uint8_t uint8;
typedef uint16_t uint16;
typedef uint32_t uint32;
typedef uint64_t uint64;
typedef size_t Size;
typedef uint32 Oid;

/* gettext no-op — exactly c.h's !ENABLE_NLS definition. */
#define _(x) (x)

/* c.h: C99 flexible array members are supported. */
#define FLEXIBLE_ARRAY_MEMBER /* empty */

#define UINT64CONST(x) UINT64_C(x)
#define INT64CONST(x) INT64_C(x)

/* LP64 little-endian oracle platform (matches c8g/x86-64 fleet + laptop). */
#undef WORDS_BIGENDIAN
#define SIZEOF_VOID_P 8

#define PGDLLIMPORT
#define pg_attribute_unused() __attribute__((unused))
#define pg_attribute_no_sanitize_alignment() __attribute__((no_sanitize("alignment")))
#define pg_nodiscard
#define pg_noreturn _Noreturn
#define lengthof(array) (sizeof(array) / sizeof((array)[0]))
#define Min(x, y) ((x) < (y) ? (x) : (y))
#define Max(x, y) ((x) > (y) ? (x) : (y))

#ifndef Assert
#define Assert(x) ((void) 0)
#endif
#define AssertMacro(x) ((void) 0)
#define StaticAssertDecl(cond, msg) _Static_assert(cond, msg)
#define StaticAssertStmt(cond, msg) do { _Static_assert(cond, msg); } while (0)

#ifndef unlikely
#define unlikely(x) (x)
#endif
#ifndef likely
#define likely(x) (x)
#endif

/* macOS has no explicit_bzero; src/port supplies it there. Plumbing only. */
#if defined(__APPLE__)
#define explicit_bzero(p, n) memset((p), 0, (n))
#endif

/* struct varlena (c.h) — needed by varatt.h / pg_crc.c bytea wrappers. */
struct varlena
{
	char		vl_len_[4];
	char		vl_dat[];
};
typedef struct varlena bytea;
typedef struct varlena text;

#define VARHDRSZ ((int32) sizeof(uint32))

#endif
