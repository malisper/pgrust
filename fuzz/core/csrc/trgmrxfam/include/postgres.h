/*
 * SHIM HEADER (trgm_diff arm 9 oracle, lane p1-trgm) — NOT vendored
 * PostgreSQL.  Minimal postgres.h replacement for the WHOLE-FILE verbatim
 * vendored infrastructure TUs in csrc/trgmrxfam/ (dynahash.c, list.c,
 * hashfn.c, pg_bitutils.c, qsort.c, regexport.c) and the assembled
 * csrc/pg_trgm_regexp_io.c.  Plumbing only — no logic:
 *
 *  - palloc family -> the SHARED bridge arena of pg_trgm_io.c
 *    (pg_diff_trgm_bridge_*): every pg_diff_trgm_* entry resets it, so
 *    error-path longjmps cannot leak.  MemoryContext values are identity
 *    tokens; MemoryContextAlloc ignores the context (see utils/memutils.h
 *    here).  Iteration-order-bearing structures (dynahash directories/
 *    buckets, list cells) depend on allocation CONTENT and sequence, never
 *    on pointer values, so the arena preserves C's orders exactly.
 *  - ereport/elog -> errcode class + longjmp through the bridge jmp_buf
 *    (set by the live pg_diff_trgm_* entry).  dynahash/list error arms are
 *    all internal-corruption arms — class 6.
 *  - Assert -> active abort (regexfam posture: invariants loud; a firing
 *    Assert is an oracle-side finding).
 *  - StaticAssert* -> _Static_assert; MemSet -> memset (macro semantics
 *    identical over the arena's char buffers).
 *  - CHECK_FOR_INTERRUPTS -> no-op (single-threaded harness, task carve).
 */
#ifndef PG_TRGMRXFAM_POSTGRES_H
#define PG_TRGMRXFAM_POSTGRES_H

#include <limits.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <setjmp.h>

typedef int8_t int8;
typedef int16_t int16;
typedef int32_t int32;
typedef int64_t int64;
typedef uint8_t uint8;
typedef uint16_t uint16;
typedef uint32_t uint32;
typedef uint64_t uint64;
typedef size_t Size;
typedef unsigned int Oid;
typedef uint32 TransactionId;
typedef float float4;
typedef double float8;
typedef uintptr_t Datum;

#define InvalidOid ((Oid) 0)
#define OidIsValid(objectId)  ((bool) ((objectId) != InvalidOid))

#define FLEXIBLE_ARRAY_MEMBER	/* empty */
#define pg_attribute_unused()
#define pg_attribute_noreturn __attribute__((noreturn))
#define pg_noinline __attribute__((noinline))
#define pg_nodiscard
#define pg_attribute_always_inline inline
#define pg_attribute_hot
#define pg_attribute_cold
#define PG_USED_FOR_ASSERTS_ONLY pg_attribute_unused()
#define likely(x) __builtin_expect((x) != 0, 1)
#define unlikely(x) __builtin_expect((x) != 0, 0)
#define lengthof(array) (sizeof (array) / sizeof ((array)[0]))
#define Max(x, y) ((x) > (y) ? (x) : (y))
#define Min(x, y) ((x) < (y) ? (x) : (y))
#define Abs(x) ((x) >= 0 ? (x) : -(x))
#define HIGHBIT (0x80)
#define IS_HIGHBIT_SET(ch) ((unsigned char)(ch) & HIGHBIT)
#define MemSet(start, val, len) memset(start, val, len)
#define MemSetAligned(start, val, len) memset(start, val, len)
#define StaticAssertStmt(cond, msg) _Static_assert(cond, msg)
#define StaticAssertDecl(cond, msg) _Static_assert(cond, msg)
#define StaticAssertExpr(cond, msg) ((void) sizeof(struct { int static_assert_failure : (cond) ? 1 : -1; }))
#define INT64_FORMAT "%lld"
#define UINT64_FORMAT "%llu"
#define SIZE_MAX_COMPAT SIZE_MAX
#define TYPEALIGN(ALIGNVAL,LEN)  \
	(((uintptr_t) (LEN) + ((ALIGNVAL) - 1)) & ~((uintptr_t) ((ALIGNVAL) - 1)))
#define MAXIMUM_ALIGNOF 8
#define MAXALIGN(LEN) TYPEALIGN(MAXIMUM_ALIGNOF, (LEN))
#define CppConcat(x, y) x##y
#define PGDLLIMPORT
#define PGDLLEXPORT
#define UINT64CONST(x) UINT64_C(x)
#define INT64CONST(x) INT64_C(x)
/* postgres.h Datum coercion macros (Datum is uintptr_t here) */
#define UInt32GetDatum(X) ((Datum) (X))
#define UInt64GetDatum(X) ((Datum) (X))
#define DatumGetUInt32(X) ((uint32) (X))
typedef void (*pg_funcptr_t) (void);
typedef uint8 bits8;
#define PG_UINT32_MAX UINT32_MAX
#define PG_UINT64_MAX UINT64_MAX
#define PG_INT32_MAX INT32_MAX
#define PG_INT64_MAX INT64_MAX
#define PG_INT8_MIN INT8_MIN
#define PG_INT8_MAX INT8_MAX
#define PG_UINT8_MAX UINT8_MAX
#define PG_INT16_MIN INT16_MIN
#define PG_INT16_MAX INT16_MAX
#define PG_UINT16_MAX UINT16_MAX
#define PG_INT32_MIN INT32_MIN
#define PG_INT64_MIN INT64_MIN
/* c.h i64abs/i32abs/i16abs (verbatim macro semantics) */
#define i16abs(i) abs(i)
#define i32abs(i) abs(i)
#define i64abs(i) llabs(i)
#define pg_noreturn _Noreturn
/* transaction / string plumbing referenced by dynahash (defs in
 * pg_trgm_regexp_io.c / trgmrxfam/strlcpy.c via build rename) */
extern int GetCurrentTransactionNestLevel(void);
/* strlcpy: vendored verbatim (trgmrxfam/strlcpy.c); renamed here at the
 * SOURCE level (not via -D) because Apple's fortified <string.h> owns the
 * bare name as a macro. The #undef strips that macro; the #define renames
 * both the vendored DEFINITION (strlcpy.c includes c.h -> this header) and
 * dynahash.c's use, so one verbatim copy serves the build. */
#undef strlcpy
#define strlcpy trgmrx_strlcpy
extern size_t trgmrx_strlcpy(char *dst, const char *src, size_t siz);
/* MemoryContext: forward typedef here so hsearch.h (verbatim) can use it
 * before utils/memutils.h is pulled in; the model lives in memutils.h. */
struct trgmrx_mcxt;
typedef struct trgmrx_mcxt *MemoryContext;
#define MemoryContextIsValid(cxt) ((cxt) != NULL)

/* active Assert (see header) */
static inline void
trgmrx_assert_fail(const char *cond, const char *file, int line)
{
	fprintf(stderr, "trgmrxfam Assert failed: %s (%s:%d)\n", cond, file, line);
	abort();
}
#define Assert(condition) \
	do { \
		if (!(condition)) \
			trgmrx_assert_fail(#condition, __FILE__, __LINE__); \
	} while (0)
#define AssertMacro(condition) ((void) ((condition) || (trgmrx_assert_fail(#condition, __FILE__, __LINE__), 0)))

#define CHECK_FOR_INTERRUPTS() ((void) 0)
/* recursion guard (regexport.c's NFA traversal): no-op — the driver caps
 * patterns at 128 bytes (regexfam posture), far below any real hazard. */
static inline void
check_stack_depth(void)
{
}

/* varlena/text: inline 4B-header images only (regexfam shape) */
typedef struct trgmrx_varlena
{
	int32		vl_len_;
	char		vl_dat[];
} varlena;
typedef varlena text;
#define VARHDRSZ ((int32) sizeof(int32))
#define VARSIZE(PTR) (*((const int32 *) (PTR)))
#define SET_VARSIZE(PTR, len) (*((int32 *) (PTR)) = (len))
#define VARDATA(PTR) (((char *) (PTR)) + VARHDRSZ)
#define VARDATA_ANY(PTR) VARDATA(PTR)
#define VARSIZE_ANY_EXHDR(PTR) (VARSIZE(PTR) - VARHDRSZ)

/* bridge allocator (shared arena of pg_trgm_io.c; header comment) */
extern void *pg_diff_trgm_bridge_palloc(size_t n);
extern void *pg_diff_trgm_bridge_palloc0(size_t n);
extern void *pg_diff_trgm_bridge_repalloc(void *p, size_t n);
extern void pg_diff_trgm_bridge_pfree(void *p);
extern void pg_diff_trgm_bridge_raise(int code) __attribute__((noreturn));

#ifndef TRGMRX_NO_ALLOC_MACROS			/* pg_trgm_regexp_io.c defines its own */
#define palloc(n) pg_diff_trgm_bridge_palloc(n)
#define palloc0(n) pg_diff_trgm_bridge_palloc0(n)
#define repalloc(p, n) pg_diff_trgm_bridge_repalloc((p), (n))
#define pfree(p) pg_diff_trgm_bridge_pfree(p)
#endif
/* engine-contract allocator (regcustom.h MALLOC): may return NULL */
static inline void *
palloc_extended(size_t size, int flags)
{
	(void) flags;
	return malloc(size);
}
#define MCXT_ALLOC_NO_OOM 0x02
#define MCXT_ALLOC_ZERO 0x04

/* ereport/elog -> class + longjmp for elevel >= ERROR (all raise arms here
 * are internal class 6). Sub-ERROR reports return so C continues, as the
 * backend's errfinish does (task #137): the family's only sub-ERROR sites
 * are dynahash.c's leaked-hash_seq_search elog(WARNING)s in the AtEOXact
 * cleanup paths — uncalled by this harness today, but a WARNING must never
 * be misreported as an oracle error if they ever become reachable. */
#ifndef TRGMRX_NO_EREPORT_MACROS		/* pg_trgm_regexp_io.c defines its own */
static inline int
trgmrx_swallow(const char *fmt,...)
{
	(void) fmt;
	return 0;
}
#define errcode(c) trgmrx_swallow("")
#define errmsg trgmrx_swallow
#define errdetail trgmrx_swallow
#define errhint trgmrx_swallow
#define ereport(level, rest) do { ((void) (rest)); if ((level) >= 21 /* ERROR */) pg_diff_trgm_bridge_raise(6); } while (0)
#define elog(level, ...) do { trgmrx_swallow(__VA_ARGS__); if ((level) >= 21 /* ERROR */) pg_diff_trgm_bridge_raise(6); } while (0)
#define ERROR 21
#define WARNING 19
#define FATAL 22
#define PANIC 23
#define LOG 15
#define DEBUG1 14
#endif

/* port.h @ 18.3: the backend's qsort IS pg_qsort (#define qsort(a,b,c,d)
 * pg_qsort(a,b,c,d)) — route every vendored TU in this family (list.c's
 * list_sort is the live caller) to the family's verbatim src/port/qsort.c
 * copy, renamed trgmrx_pg_qsort by build.rs. A bare `U qsort` would bind
 * LIBC and silently change tie order (task #98 sort-symbol hygiene). */
extern void trgmrx_pg_qsort(void *base, size_t nel, size_t elsize,
							int (*cmp) (const void *, const void *));
#define qsort(a,b,c,d) trgmrx_pg_qsort(a,b,c,d)

#endif							/* PG_TRGMRXFAM_POSTGRES_H */
