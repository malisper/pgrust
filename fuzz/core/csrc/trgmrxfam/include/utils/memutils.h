/* SHIM (trgmrxfam): MemoryContext model — contexts are identity tokens and
 * ALL allocation lands in the shared bridge arena (reset per entry). The
 * real code uses contexts only to bound cruft lifetime; per-entry reset
 * supersedes that. MaxAllocSize is the verbatim memutils.h value. */
#ifndef TRGMRX_MEMUTILS_H
#define TRGMRX_MEMUTILS_H
#include "postgres.h"
struct trgmrx_mcxt
{
	int			dummy;
};
typedef struct trgmrx_mcxt trgmrx_mcxt;
/* MemoryContext typedef lives in the shim postgres.h (hsearch.h needs it) */
extern MemoryContext CurrentMemoryContext;
extern MemoryContext TopMemoryContext;
#define MaxAllocSize	((Size) 0x3fffffff) /* 1 gigabyte - 1 */
#define AllocSizeIsValid(size)	((Size) (size) <= MaxAllocSize)
#define ALLOCSET_DEFAULT_SIZES 0, 0, 0
static inline MemoryContext
AllocSetContextCreate(MemoryContext parent, const char *name,
					  Size a, Size b, Size c)
{
	(void) parent; (void) name; (void) a; (void) b; (void) c;
	return CurrentMemoryContext;
}
static inline MemoryContext
MemoryContextSwitchTo(MemoryContext cxt)
{
	(void) cxt;
	return CurrentMemoryContext;
}
static inline void
MemoryContextDelete(MemoryContext cxt)
{
	(void) cxt;
}
static inline void
MemoryContextSetIdentifier(MemoryContext cxt, const char *id)
{
	(void) cxt; (void) id;
}
static inline void *
MemoryContextAlloc(MemoryContext cxt, Size size)
{
	(void) cxt;
	return pg_diff_trgm_bridge_palloc(size);
}
static inline void *
MemoryContextAllocZero(MemoryContext cxt, Size size)
{
	(void) cxt;
	return pg_diff_trgm_bridge_palloc0(size);
}
/* dynahash's DynaHashAlloc contract: MCXT_ALLOC_NO_OOM may return NULL;
 * the arena aborts on real OOM instead (environment, not behavior). */
static inline void *
MemoryContextAllocExtended(MemoryContext cxt, Size size, int flags)
{
	(void) cxt;
	if (flags & MCXT_ALLOC_ZERO)
		return pg_diff_trgm_bridge_palloc0(size);
	return pg_diff_trgm_bridge_palloc(size);
}
static inline MemoryContext
GetMemoryChunkContext(void *pointer)
{
	(void) pointer;
	return CurrentMemoryContext;
}
#endif
