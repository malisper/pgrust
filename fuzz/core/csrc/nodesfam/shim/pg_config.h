/*
 * SHIM pg_config.h — NOT PostgreSQL code. Minimal fabricated config for
 * compiling the verbatim nodes/ walker sources standalone on LP64
 * little-endian gcc/clang (fleet aarch64-linux + macOS dev), matching the
 * values a real 18.3 build produces on those platforms.
 */
#ifndef PG_CONFIG_H_SHIM
#define PG_CONFIG_H_SHIM

#define PG_VERSION "18.3"
#define PG_VERSION_NUM 180003
#define PG_MAJORVERSION "18"
#define PG_MAJORVERSION_NUM 18
#define PG_MINORVERSION_NUM 3
#define PG_VERSION_STR "PostgreSQL 18.3 (pgrust nodesfam differential oracle)"
#define CONFIGURE_ARGS ""

#define MAXIMUM_ALIGNOF 8
#define ALIGNOF_DOUBLE 8
#define ALIGNOF_INT 4
#define ALIGNOF_LONG 8
#define ALIGNOF_SHORT 2
#define ALIGNOF_PG_INT128_TYPE 16

#define SIZEOF_BOOL 1
#define SIZEOF_LONG 8
#define SIZEOF_SIZE_T 8
#define SIZEOF_VOID_P 8
#define SIZEOF_OFF_T 8

#define HAVE_LONG_INT_64 1
#define PG_INT64_TYPE long int
#define PG_INT128_TYPE __int128
#define PG_PRINTF_ATTRIBUTE printf
#define INT64_MODIFIER "l"

#define HAVE_INTTYPES_H 1
#define HAVE_STDINT_H 1
#define HAVE_STDBOOL_H 1
#define HAVE_STRINGS_H 1
#define HAVE_UNISTD_H 1
#define HAVE_SYS_STAT_H 1
#define HAVE_SYS_TYPES_H 1
#define HAVE_STDLIB_H 1
#define HAVE_STRING_H 1
#define HAVE_MEMORY_H 1

#define HAVE__BUILTIN_BSWAP16 1
#define HAVE__BUILTIN_BSWAP32 1
#define HAVE__BUILTIN_BSWAP64 1
#define HAVE__BUILTIN_CLZ 1
#define HAVE__BUILTIN_CTZ 1
#define HAVE__BUILTIN_POPCOUNT 1
#define HAVE__BUILTIN_CONSTANT_P 1
#define HAVE__BUILTIN_TYPES_COMPATIBLE_P 1
#define HAVE__BUILTIN_UNREACHABLE 1
#define HAVE__BUILTIN_OP_OVERFLOW 1
#define HAVE_COMPUTED_GOTO 1

#define BLCKSZ 8192
#define XLOG_BLCKSZ 8192
#define RELSEG_SIZE 131072
#define DEF_PGPORT 5432
#define DEF_PGPORT_STR "5432"
#define PG_KRB_SRVNAM "postgres"

#define MEMSET_LOOP_LIMIT 1024

/* NLS off */
#define ENABLE_NLS 0
#undef ENABLE_NLS

/* decl availability, per platform (matches real 18.3 configure results:
 * macOS SDK declares strlcpy/strlcat/F_FULLFSYNC; glibc (fleet Linux)
 * does not declare strlcpy/strlcat before 2.38 — port/strlcpy.c and
 * port/strlcat.c are vendored and compiled for that arm). */
#if defined(__APPLE__)
#define HAVE_DECL_STRLCAT 1
#define HAVE_DECL_STRLCPY 1
#define HAVE_DECL_F_FULLFSYNC 1
#define HAVE_DECL_POSIX_FADVISE 0
#else
#define HAVE_DECL_STRLCAT 0
#define HAVE_DECL_STRLCPY 0
#define HAVE_DECL_F_FULLFSYNC 0
#define HAVE_DECL_POSIX_FADVISE 1
#endif
#define HAVE_DECL_STRNLEN 1
#define HAVE_DECL_STRSEP 1
#define HAVE_DECL_TIMINGSAFE_BCMP 0
#define HAVE_DECL_FDATASYNC 1
#define HAVE_DECL_PREADV 1
#define HAVE_DECL_PWRITEV 1
#define USE_ASSERT_CHECKING 0
#undef USE_ASSERT_CHECKING
#define pg_restrict __restrict
#define HAVE_GCC__ATOMIC_INT32_CAS 1
#define HAVE_GCC__ATOMIC_INT64_CAS 1
#define HAVE_GCC__SYNC_INT32_CAS 1
#define HAVE_GCC__SYNC_INT32_TAS 1
#define HAVE_GCC__SYNC_INT64_CAS 1
#define HAVE_GCC__SYNC_CHAR_TAS 1

#endif
