/*
 * SHIM utils/memutils.h — NOT PostgreSQL code (libfam_diff oracle).
 *
 * csrc/libfam/vendor/integerset.c consumes MemoryContextAlloc,
 * GetMemoryChunkSpace and CurrentMemoryContext. Environment mocking only
 * (skill rule: mock the ENVIRONMENT, never the COMPUTATION):
 *   - MemoryContextAlloc -> the TU's TLS leak-tracking arena (models
 *     "allocate in the set's own context"; reset per driver exec).
 *   - GetMemoryChunkSpace -> 0. CARVE (documented in the driver header):
 *     intset_memory_usage reports aset chunk-header accounting, a
 *     malloc-layout non-surface with no Rust counterpart semantics; the
 *     memory-usage plane is NOT compared. Both sides' entry points are
 *     still executed every exec (no-panic plane).
 *   - CurrentMemoryContext -> opaque non-NULL cookie.
 */
#ifndef MEMUTILS_H
#define MEMUTILS_H

extern void *pg_diff_libfam_alloc(size_t n);

typedef void *MemoryContext;
#define CurrentMemoryContext ((MemoryContext) 1)
#define MemoryContextAlloc(context, n) pg_diff_libfam_alloc(n)
#define GetMemoryChunkSpace(p) ((Size) 0)

#endif							/* MEMUTILS_H */
