/* Standalone shim for the vendored REL_18_3 regex engine. MALLOC here is
 * palloc_extended(NO_OOM) = NULL-on-failure, so malloc is shape-identical;
 * Assert compiles out (production build); CHECK_FOR_INTERRUPTS keeps its
 * real global-load+branch; ereport arms are unreachable under the C
 * collation and abort if taken. */
#ifndef CREF_REGEX_POSTGRES_H
#define CREF_REGEX_POSTGRES_H

#include <limits.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

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

typedef struct varlena text;

#define FLEXIBLE_ARRAY_MEMBER	/* empty */
#define InvalidOid ((Oid) 0)
#define OidIsValid(objectId) ((bool) ((objectId) != InvalidOid))
#define Assert(condition) ((void) 0)
#define Max(x, y) ((x) > (y) ? (x) : (y))
#define Min(x, y) ((x) < (y) ? (x) : (y))
#define lengthof(array) (sizeof(array) / sizeof((array)[0]))

extern volatile int cref_InterruptPending;
#define CHECK_FOR_INTERRUPTS() \
	do { if (cref_InterruptPending) abort(); } while (0)

#define MCXT_ALLOC_NO_OOM 0x0002

/* Per-thread live-allocation balance over the engine's MALLOC/FREE/REALLOC
 * contract (counting shim only — allocation semantics are untouched libc).
 * The real server frees engine memory via pg_regfree OR by deleting the
 * memory context the engine palloc'd into (regcustom.h MALLOC =
 * palloc_extended @ 18.3); here context deletion does not exist, so any
 * caller that skips pg_regfree leaks silently — task #150 (trgm_diff arm 9)
 * accumulated ~25-30KB/exec exactly this way.  The balance lets harness
 * tests assert engine memory returns to a mark across execs.  Defined in
 * pg_regexfam.c; read via pg_diff_regexfam_live_allocs().
 * NB: regc_pg_locale.c's pg_ctype_cache uses RAW malloc/free by upstream
 * design (process-lifetime cache) and is intentionally outside this
 * balance. */
extern _Thread_local long cref_engine_live_allocs;
static inline void *
cref_counted_malloc(Size sz)
{
	void	   *p = malloc(sz);

	if (p)
		cref_engine_live_allocs++;
	return p;
}
static inline void *
cref_counted_realloc(void *p, Size sz)
{
	void	   *q = realloc(p, sz);

	if (q && p == NULL)
		cref_engine_live_allocs++;
	return q;
}
static inline void
cref_counted_free(void *p)
{
	if (p)
		cref_engine_live_allocs--;
	free(p);
}
#define palloc_extended(sz, flags) cref_counted_malloc(sz)
#define repalloc_extended(p, sz, flags) cref_counted_realloc((p), (sz))
static inline void *palloc(Size sz) { return cref_counted_malloc(sz); }
static inline void pfree(void *p) { cref_counted_free(p); }

#define ereport(elevel, rest) abort()
#define ERROR 21

/* pgstrcasecmp.c (verbatim bodies; upstream outlines them in libpgport) */
static inline unsigned char
pg_ascii_toupper(unsigned char ch)
{
	if (ch >= 'a' && ch <= 'z')
		ch += 'A' - 'a';
	return ch;
}

static inline unsigned char
pg_ascii_tolower(unsigned char ch)
{
	if (ch >= 'A' && ch <= 'Z')
		ch += 'a' - 'A';
	return ch;
}

#endif
