/*
 * SHIM utils/memutils.h — NOT PostgreSQL code (radixtree_diff oracle).
 *
 * lib/radixtree.h (non-RT_SHMEM arm) consumes MemoryContext handles for
 * node slabs + the leaf context, plus SLAB_DEFAULT_BLOCK_SIZE for its
 * block-size computation. Environment mocking only (skill rule: mock the
 * ENVIRONMENT, never the COMPUTATION):
 *   - MemoryContextData -> a chunk-list arena (doubly-linked headers, O(1)
 *     pfree), defined in pg_radixtree_io.c; contexts come from a per-exec
 *     TLS pool; pg_diff_rt_env_reset() frees everything.
 *   - SlabContextCreate -> new arena context (child bookkeeping only —
 *     slab-vs-aset is an allocation-strategy non-surface; the tree only
 *     ever allocs/frees fixed-size chunks in it).
 *   - MemoryContextMemAllocated -> total tracked payload bytes. CARVE
 *     (documented in the driver header): C reports malloc-block
 *     accounting, a memory-layout non-surface; the memory-usage plane is
 *     EXECUTED both sides every exec but the VALUES are not compared.
 *   - CurrentMemoryContext -> a dedicated pool context (tree/ctl/iter
 *     headers live there, exactly the objects C would put in the caller's
 *     current context and pfree explicitly).
 */
#ifndef MEMUTILS_H
#define MEMUTILS_H

typedef struct MemoryContextData MemoryContextData;
typedef MemoryContextData *MemoryContext;

extern MemoryContext pg_diff_rt_current_ctx(void);
extern MemoryContext pg_diff_rt_ctx_create(MemoryContext parent);
extern void *pg_diff_rt_ctx_alloc(MemoryContext ctx, size_t n);
extern void *pg_diff_rt_ctx_alloc0(MemoryContext ctx, size_t n);
extern void pg_diff_rt_ctx_pfree(void *p);
extern void pg_diff_rt_ctx_reset(MemoryContext ctx);
extern uint64 pg_diff_rt_ctx_mem_allocated(MemoryContext ctx);

#define CurrentMemoryContext (pg_diff_rt_current_ctx())
#define MemoryContextAlloc(ctx, n) pg_diff_rt_ctx_alloc((ctx), (n))
#define MemoryContextReset(ctx) pg_diff_rt_ctx_reset(ctx)
#define MemoryContextMemAllocated(ctx, recurse) pg_diff_rt_ctx_mem_allocated(ctx)
#define SlabContextCreate(parent, name, blocksize, chunksize) \
	pg_diff_rt_ctx_create(parent)

/* upstream src/include/utils/memutils.h value (verbatim constant) */
#define SLAB_DEFAULT_BLOCK_SIZE		(8 * 1024)

#endif							/* MEMUTILS_H */
