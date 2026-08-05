/*
 * SHIM c.h — NOT PostgreSQL code.
 *
 * Minimal environment so the VERBATIM vendored files under ../
 * (src/port/pg_bitutils.c, src/port/pg_popcount_aarch64.c,
 * src/port/pg_crc32c_sb8.c, src/backend/utils/hash/pg_crc.c,
 * src/port/pgstrcasecmp.c, src/port/path.c, src/port/strlcpy.c,
 * src/backend/access/common/bufmask.c, copied unmodified from postgres-src @
 * 62d6c7d3df6287f1bd83199c1a746e50d31571a0, REL_18 "Stamp 18.3") compile
 * standalone for the native differential-fuzz build (portfam_diff target).
 * Plumbing only, never logic: fixed-width typedefs matching c.h on LP64
 * little-endian (the fleet oracle platform + this laptop), no-op Assert
 * (NDEBUG parity with real release builds).
 *
 * Config macros (matching every supported LP64 gcc/clang PG build):
 *   HAVE__BUILTIN_CLZ / CTZ / POPCOUNT, SIZEOF_LONG=8, SIZEOF_VOID_P=8,
 *   BLCKSZ=8192 (the shipped Rust types_core::BLCKSZ), MAXPGPATH=1024
 *   (pg_config_manual.h value, matching crates/port/pg_path).
 *
 * ORACLE-ARM NOTE (pg_bitutils): POPCNT_AARCH64 is decided inside the
 * vendored port/pg_bitutils.h (defined(__aarch64__) && defined(__ARM_NEON)),
 * exactly as in a real build — on aarch64 hosts the vendored Neon
 * pg_popcount_aarch64.c arm is the oracle, elsewhere the portable
 * pg_bitutils.c slow arm. Both are verbatim PostgreSQL code.
 */
#ifndef PG_DIFFFUZZ_PORTFAM_SHIM_C_H
#define PG_DIFFFUZZ_PORTFAM_SHIM_C_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>

typedef int8_t int8;
typedef int16_t int16;
typedef int32_t int32;
typedef int64_t int64;
typedef uint8_t uint8;
typedef uint16_t uint16;
typedef uint32_t uint32;
typedef uint64_t uint64;
typedef uint8 bits8;
typedef size_t Size;
typedef uint32 Oid;
typedef char *Pointer;
/* c.h/postgres_ext.h scalars bufpage.h needs. */
typedef uint32 TransactionId;
typedef uint32 CommandId;

/* gettext no-op — exactly c.h's !ENABLE_NLS definition. */
#define _(x) (x)

/* c.h: C99 flexible array members are supported. */
#define FLEXIBLE_ARRAY_MEMBER /* empty */

#define UINT64CONST(x) UINT64_C(x)
#define INT64CONST(x) INT64_C(x)

/* LP64 little-endian oracle platform (matches c8g fleet + laptop). */
#undef WORDS_BIGENDIAN
#define SIZEOF_VOID_P 8
#define SIZEOF_LONG 8
#define SIZEOF_SIZE_T 8

/* pg_config.h arms every supported gcc/clang build defines. */
#define HAVE__BUILTIN_CLZ 1
#define HAVE__BUILTIN_CTZ 1
#define HAVE__BUILTIN_POPCOUNT 1

#define PG_INT32_MAX INT32_MAX
#define PG_INT32_MIN INT32_MIN
#define PG_UINT32_MAX UINT32_MAX
#define PG_INT64_MAX INT64_MAX
#define PG_INT64_MIN INT64_MIN
#define PG_UINT64_MAX UINT64_MAX
#define PG_UINT16_MAX UINT16_MAX

#define PGDLLIMPORT
#define pg_attribute_unused() __attribute__((unused))
#define pg_attribute_no_sanitize_alignment() __attribute__((no_sanitize("alignment")))
#define pg_attribute_nonnull(...)
#define pg_nodiscard
#define pg_noreturn _Noreturn
#define PG_USED_FOR_ASSERTS_ONLY pg_attribute_unused()
#define lengthof(array) (sizeof(array) / sizeof((array)[0]))
#define Min(x, y) ((x) < (y) ? (x) : (y))
#define Max(x, y) ((x) > (y) ? (x) : (y))
#define unconstify(underlying_type, expr) ((underlying_type) (expr))

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

/* c.h TYPEALIGN family (MAXIMUM_ALIGNOF = 8 on LP64). */
#define MAXIMUM_ALIGNOF 8
#define TYPEALIGN(ALIGNVAL,LEN)  \
	(((uintptr_t) (LEN) + ((ALIGNVAL) - 1)) & ~((uintptr_t) ((ALIGNVAL) - 1)))
#define MAXALIGN(LEN) TYPEALIGN(MAXIMUM_ALIGNOF, (LEN))

/* c.h HIGHBIT test (pgstrcasecmp.c). */
#define HIGHBIT (0x80)
#define IS_HIGHBIT_SET(ch) ((unsigned char)(ch) & HIGHBIT)

/* pg_config_manual.h values of record. */
#define MAXPGPATH 1024
#define BLCKSZ 8192

/*
 * port.h pieces path.c uses, VERBATIM from src/include/port.h @ the same ref
 * (the !WIN32 arm, the arm every Unix build compiles).
 */
#define IS_NONWINDOWS_DIR_SEP(ch) ((ch) == '/')
#define is_nonwindows_absolute_path(filename) \
( \
	IS_NONWINDOWS_DIR_SEP((filename)[0]) \
)
#define IS_DIR_SEP(ch) IS_NONWINDOWS_DIR_SEP(ch)
#define is_absolute_path(filename) is_nonwindows_absolute_path(filename)
extern void canonicalize_path_enc(char *path, int encoding);

/*
 * SYMBOL ISOLATION for strlcpy (see core/build.rs PORTFAM_SYMS): the rename
 * cannot ride on -Dstrlcpy because Apple's <string.h> re-#defines strlcpy as
 * a _FORTIFY builtin macro AFTER the command-line -D is applied. Rename here,
 * after the system headers, so the vendored src/port/strlcpy.c definition and
 * every path.c call site both bind to portfam_strlcpy. Plumbing only.
 */
#ifdef strlcpy
#undef strlcpy
#endif
#define strlcpy portfam_strlcpy
extern size_t strlcpy(char *dst, const char *src, size_t siz);
extern int pg_strcasecmp(const char *s1, const char *s2);
extern int pg_strncasecmp(const char *s1, const char *s2, size_t n);
extern unsigned char pg_toupper(unsigned char ch);
extern unsigned char pg_tolower(unsigned char ch);
extern unsigned char pg_ascii_toupper(unsigned char ch);
extern unsigned char pg_ascii_tolower(unsigned char ch);

/* struct varlena (c.h) — needed by varatt.h / pg_crc.c bytea wrappers. */
struct varlena
{
	char		vl_len_[4];
	char		vl_dat[FLEXIBLE_ARRAY_MEMBER];
};
typedef struct varlena bytea;
typedef struct varlena text;

#define VARHDRSZ ((int32) sizeof(uint32))

#endif
