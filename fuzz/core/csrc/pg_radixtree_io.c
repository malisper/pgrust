/*
 * pg_radixtree_io.c: vendored PostgreSQL C oracle for the radixtree_diff
 * differential fuzz target (100%-coverage campaign; crate
 * crates/backend/lib/radixtree).
 *
 * Provenance (all PostgreSQL bodies VERBATIM):
 *   - csrc/radixtree/include/lib/radixtree.h = src/include/lib/radixtree.h
 *     BYTE-FOR-BYTE from postgres-src @
 *     62d6c7d3df6287f1bd83199c1a746e50d31571a0 (Stamp-18.3); `cmp` against
 *     the vendor tree passes.
 *   - csrc/radixtree/include/port/simd.h = src/include/port/simd.h,
 *     byte-for-byte (self-contained; NEON arm on both oracle platforms).
 *   - csrc/radixtree/include/nodes/bitmapset.h and port/pg_bitutils.h are
 *     REDUCED headers: verbatim pieces only, provenance in their headers.
 *
 * Template instantiations (both non-RT_SHMEM, both RT_USE_DELETE, matching
 * the two shipped RtValue shapes of crates/backend/lib/radixtree):
 *   - rtf_*: RT_VALUE_TYPE = uint64 (fixed-size, always-embeddable;
 *     test_radixtree.c's TestValueType shape) <-> Rust RadixTree<u64>.
 *   - rtv_*: RT_VALUE_TYPE = pg_diff_rtv_val with RT_VARLEN_VALUE_SIZE +
 *     RT_RUNTIME_EMBEDDABLE_VALUE (tidstore's BlocktableEntry shape: low
 *     bit of the first byte is the embedded tag) <-> Rust
 *     RadixTree<RtvVal> with VARLEN + RUNTIME_EMBEDDABLE.
 *
 * CARVES (documented here and in the driver header):
 *   - RT_SHMEM arm: never instantiated. C's shared flavor needs dsa +
 *     LWLock process infrastructure; the ranking cell carves it. The Rust
 *     SharedRadixTree (thread-native RwLock stand-in) is compared against
 *     the SAME non-shmem oracle: every tree-shape operation in the C
 *     template is byte-identical code between the two flavors (the flavor
 *     #ifdefs touch only allocation plumbing and locking), so the value
 *     plane is meaningful; the locking discipline itself is Rust-side-only
 *     surface (RwLock poisoning/panics = no-panic plane).
 *   - RT_MEMORY_USAGE: EXECUTED both sides every time the driver issues
 *     the op, but the VALUES are not compared — C reports memory-context
 *     block accounting (shimmed here as tracked payload bytes), a
 *     malloc-layout non-surface (intset memory_usage precedent).
 *   - RT_DEBUG: never defined (upstream default; the dump/stats code is
 *     compiled out on both sides — the Rust port has no counterpart).
 *
 * Shims (plumbing only, never logic):
 *   - shim postgres.h supplies fixed-width typedefs; this file adds
 *     Size/Min/Max/PG_UINT32_MAX/FLEXIBLE_ARRAY_MEMBER/lengthof/CppConcat/
 *     StaticAssertDecl/pg_attribute_unused with c.h's exact LP64 meanings.
 *   - palloc0/pfree and the MemoryContext surface -> the chunk-list arena
 *     below (models per-context alloc/reset; see
 *     csrc/radixtree/include/utils/memutils.h).
 *   - lib/radixtree.h raises NO elog/ereport (asserts only) — there is no
 *     error plane to capture in this TU.
 *
 * Driver entries (section 3) are fuzz plumbing, NOT Postgres code.
 */

#include "postgres.h"

#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

/* ---- SHIM: c.h surface on LP64 (exact upstream values) ---- */
typedef size_t Size;

#define FLEXIBLE_ARRAY_MEMBER	/* empty */
#define BITS_PER_BYTE 8
#define PG_UINT32_MAX UINT32_C(0xFFFFFFFF)
#define Min(x, y)		((x) < (y) ? (x) : (y))
#define Max(x, y)		((x) > (y) ? (x) : (y))
#define lengthof(array) (sizeof (array) / sizeof ((array)[0]))
#define CppConcat(x, y)			x##y
#define StaticAssertDecl(condition, errmessage) \
	_Static_assert(condition, errmessage)
#define pg_attribute_unused() __attribute__((unused))
#define PG_USED_FOR_ASSERTS_ONLY pg_attribute_unused()
/* c.h's exact gcc/clang definitions */
#define pg_noinline __attribute__((noinline))
#define pg_unreachable() __builtin_unreachable()

#include "utils/memutils.h"

/* ---- SHIM: chunk-list arena backing the MemoryContext surface ---- */

typedef struct pg_diff_rt_chunk
{
	struct pg_diff_rt_chunk *next;
	struct pg_diff_rt_chunk *prev;
	struct MemoryContextData *ctx;
	size_t		size;
}			pg_diff_rt_chunk;

struct MemoryContextData
{
	pg_diff_rt_chunk *head;
	uint64		total;
	int			live;
	struct MemoryContextData *parent;
};

#define PG_DIFF_RT_CTX_MAX 64
static _Thread_local struct MemoryContextData pg_diff_rt_ctxs[PG_DIFF_RT_CTX_MAX];
static _Thread_local int pg_diff_rt_ctx_n;

MemoryContext
pg_diff_rt_ctx_create(MemoryContext parent)
{
	MemoryContext ctx;

	assert(pg_diff_rt_ctx_n < PG_DIFF_RT_CTX_MAX);
	ctx = &pg_diff_rt_ctxs[pg_diff_rt_ctx_n++];
	ctx->head = NULL;
	ctx->total = 0;
	ctx->live = 1;
	ctx->parent = parent;
	return ctx;
}

MemoryContext
pg_diff_rt_current_ctx(void)
{
	/* context 0 stands in for the caller's CurrentMemoryContext */
	if (pg_diff_rt_ctx_n == 0)
		(void) pg_diff_rt_ctx_create(NULL);
	return &pg_diff_rt_ctxs[0];
}

void *
pg_diff_rt_ctx_alloc(MemoryContext ctx, size_t n)
{
	pg_diff_rt_chunk *c = (pg_diff_rt_chunk *) malloc(sizeof(pg_diff_rt_chunk) + n);

	assert(c != NULL && ctx != NULL && ctx->live);
	c->ctx = ctx;
	c->size = n;
	c->prev = NULL;
	c->next = ctx->head;
	if (ctx->head)
		ctx->head->prev = c;
	ctx->head = c;
	ctx->total += n;
	return (void *) (c + 1);
}

void *
pg_diff_rt_ctx_alloc0(MemoryContext ctx, size_t n)
{
	void	   *p = pg_diff_rt_ctx_alloc(ctx, n);

	memset(p, 0, n);
	return p;
}

void
pg_diff_rt_ctx_pfree(void *p)
{
	pg_diff_rt_chunk *c = ((pg_diff_rt_chunk *) p) - 1;
	MemoryContext ctx = c->ctx;

	assert(ctx != NULL && ctx->live);
	if (c->prev)
		c->prev->next = c->next;
	else
		ctx->head = c->next;
	if (c->next)
		c->next->prev = c->prev;
	ctx->total -= c->size;
	free(c);
}

void
pg_diff_rt_ctx_reset(MemoryContext ctx)
{
	pg_diff_rt_chunk *c = ctx->head;

	while (c)
	{
		pg_diff_rt_chunk *next = c->next;

		free(c);
		c = next;
	}
	ctx->head = NULL;
	ctx->total = 0;
}

uint64
pg_diff_rt_ctx_mem_allocated(MemoryContext ctx)
{
	/* models MemoryContextMemAllocated(ctx, recurse = true): sum ctx plus
	 * every pool context whose parent chain reaches it (node slabs are
	 * children of the leaf context, exactly C's RT_CREATE layout) */
	uint64		total = ctx->total;
	int			i;

	for (i = 0; i < pg_diff_rt_ctx_n; i++)
	{
		MemoryContext a = pg_diff_rt_ctxs[i].parent;

		while (a != NULL)
		{
			if (a == ctx)
			{
				total += pg_diff_rt_ctxs[i].total;
				break;
			}
			a = a->parent;
		}
	}
	return total;
}

/* per-exec environment reset (frees every context's chunks + the pool) */
void
pg_diff_rt_env_reset(void)
{
	int			i;

	for (i = 0; i < pg_diff_rt_ctx_n; i++)
	{
		pg_diff_rt_ctx_reset(&pg_diff_rt_ctxs[i]);
		pg_diff_rt_ctxs[i].live = 0;
	}
	pg_diff_rt_ctx_n = 0;
}

#define palloc0(n) pg_diff_rt_ctx_alloc0(pg_diff_rt_current_ctx(), (n))
#define pfree(p) pg_diff_rt_ctx_pfree(p)

/* ============ SECTION 2: the VERBATIM vendored template, twice ============ */

/* ---- instantiation 1: fixed-size uint64 values (rtf_*) ---- */
#define RT_PREFIX rtf
#define RT_SCOPE static pg_attribute_unused()
#define RT_DECLARE
#define RT_DEFINE
#define RT_USE_DELETE
#define RT_VALUE_TYPE uint64
#include "lib/radixtree.h"

/* ---- instantiation 2: varlen runtime-embeddable values (rtv_*) ---- */

/*
 * HARNESS value type modeling tidstore's BlocktableEntry shape: a small
 * header (first byte's low bit = the RT_RUNTIME_EMBEDDABLE_VALUE tag,
 * exactly C tidstore's `uint8 flags` low-bit convention) + a byte payload.
 * The Rust driver uses the byte-identical #[repr(C)] mirror.
 */
typedef struct pg_diff_rtv_val
{
	uint8		flags;			/* bit 0 = embedded tag (set iff image
								 * size <= sizeof(uintptr_t)) */
	uint8		len;			/* payload byte count */
	uint8		data[FLEXIBLE_ARRAY_MEMBER];
}			pg_diff_rtv_val;

#define PG_DIFF_RTV_HDR ((int) offsetof(pg_diff_rtv_val, data))
#define PG_DIFF_RTV_MAX_LEN 100
#define PG_DIFF_RTV_MAX_SIZE (PG_DIFF_RTV_HDR + PG_DIFF_RTV_MAX_LEN)

#define RT_PREFIX rtv
#define RT_SCOPE static pg_attribute_unused()
#define RT_DECLARE
#define RT_DEFINE
#define RT_USE_DELETE
#define RT_VALUE_TYPE pg_diff_rtv_val
#define RT_VARLEN_VALUE_SIZE(v) (offsetof(pg_diff_rtv_val, data) + (v)->len)
#define RT_RUNTIME_EMBEDDABLE_VALUE
#include "lib/radixtree.h"

/* ============ SECTION 3: driver entries (fuzz plumbing, NOT Postgres code) ============ */

static _Thread_local rtf_radix_tree *pg_diff_rtf;
static _Thread_local rtf_iter *pg_diff_rtf_it;
static _Thread_local rtv_radix_tree *pg_diff_rtv;
static _Thread_local rtv_iter *pg_diff_rtv_it;

/* callers must pg_diff_rt_env_reset() between execs; it invalidates all
 * tree/iter handles, so null them here */
void
pg_diff_rt_handles_reset(void)
{
	pg_diff_rtf = NULL;
	pg_diff_rtf_it = NULL;
	pg_diff_rtv = NULL;
	pg_diff_rtv_it = NULL;
}

/* ---- fixed (rtf) arm ---- */

void
pg_diff_rtf_create(void)
{
	assert(pg_diff_rtf == NULL);
	/* current ctx FIRST so tree/ctl never share the leaf context (C
	 * callers pass a dedicated context; resetting the leaf context must
	 * not free the tree header C pfrees right after) */
	(void) pg_diff_rt_current_ctx();
	pg_diff_rtf = rtf_create(pg_diff_rt_ctx_create(NULL));
}

void
pg_diff_rtf_free(void)
{
	assert(pg_diff_rtf != NULL && pg_diff_rtf_it == NULL);
	rtf_free(pg_diff_rtf);
	pg_diff_rtf = NULL;
}

int
pg_diff_rtf_set(uint64 key, uint64 val)
{
	return rtf_set(pg_diff_rtf, key, &val) ? 1 : 0;
}

int
pg_diff_rtf_find(uint64 key, uint64 *out)
{
	uint64	   *p = rtf_find(pg_diff_rtf, key);

	if (p == NULL)
		return 0;
	*out = *p;
	return 1;
}

/* find-and-overwrite through the returned pointer (RT_FIND's documented
 * mutation channel; pairs with Rust find_mut) */
int
pg_diff_rtf_find_set(uint64 key, uint64 newval)
{
	uint64	   *p = rtf_find(pg_diff_rtf, key);

	if (p == NULL)
		return 0;
	*p = newval;
	return 1;
}

int
pg_diff_rtf_delete(uint64 key)
{
	return rtf_delete(pg_diff_rtf, key) ? 1 : 0;
}

void
pg_diff_rtf_iter_begin(void)
{
	assert(pg_diff_rtf_it == NULL);
	pg_diff_rtf_it = rtf_begin_iterate(pg_diff_rtf);
}

int
pg_diff_rtf_iter_next(uint64 *key, uint64 *val)
{
	uint64	   *p;

	p = rtf_iterate_next(pg_diff_rtf_it, key);
	if (p == NULL)
		return 0;
	*val = *p;
	return 1;
}

void
pg_diff_rtf_iter_end(void)
{
	rtf_end_iterate(pg_diff_rtf_it);
	pg_diff_rtf_it = NULL;
}

uint64
pg_diff_rtf_memory_usage(void)
{
	return rtf_memory_usage(pg_diff_rtf);
}

int64
pg_diff_rtf_num_keys(void)
{
	/* harness window into the control struct (no public accessor in C;
	 * compared against Rust's num_keys() bookkeeping) */
	return pg_diff_rtf->ctl->num_keys;
}

/* ---- varlen (rtv) arm ---- */

void
pg_diff_rtv_create(void)
{
	assert(pg_diff_rtv == NULL);
	/* see pg_diff_rtf_create: current ctx must be distinct */
	(void) pg_diff_rt_current_ctx();
	pg_diff_rtv = rtv_create(pg_diff_rt_ctx_create(NULL));
}

void
pg_diff_rtv_free(void)
{
	assert(pg_diff_rtv != NULL && pg_diff_rtv_it == NULL);
	rtv_free(pg_diff_rtv);
	pg_diff_rtv = NULL;
}

/*
 * payload: len bytes (0 <= len <= PG_DIFF_RTV_MAX_LEN). Builds the value
 * image in a local buffer; flags bit 0 is set iff the image fits a child
 * pointer slot (the RT_RUNTIME_EMBEDDABLE_VALUE contract; the Rust driver
 * constructs the byte-identical image).
 */
int
pg_diff_rtv_set(uint64 key, const uint8 *payload, int len)
{
	uint8		buf[PG_DIFF_RTV_MAX_SIZE];
	pg_diff_rtv_val *v = (pg_diff_rtv_val *) buf;

	assert(len >= 0 && len <= PG_DIFF_RTV_MAX_LEN);
	v->len = (uint8) len;
	v->flags = (PG_DIFF_RTV_HDR + len <= (int) sizeof(uintptr_t)) ? 1 : 0;
	memcpy(v->data, payload, len);
	return rtv_set(pg_diff_rtv, key, v) ? 1 : 0;
}

/* out must hold PG_DIFF_RTV_MAX_SIZE bytes; returns full image length */
int
pg_diff_rtv_find(uint64 key, uint8 *out, int *outlen)
{
	pg_diff_rtv_val *p = rtv_find(pg_diff_rtv, key);
	int			sz;

	if (p == NULL)
		return 0;
	sz = PG_DIFF_RTV_HDR + p->len;
	memcpy(out, p, sz);
	*outlen = sz;
	return 1;
}

int
pg_diff_rtv_delete(uint64 key)
{
	return rtv_delete(pg_diff_rtv, key) ? 1 : 0;
}

void
pg_diff_rtv_iter_begin(void)
{
	assert(pg_diff_rtv_it == NULL);
	pg_diff_rtv_it = rtv_begin_iterate(pg_diff_rtv);
}

int
pg_diff_rtv_iter_next(uint64 *key, uint8 *out, int *outlen)
{
	pg_diff_rtv_val *p;
	int			sz;

	p = rtv_iterate_next(pg_diff_rtv_it, key);
	if (p == NULL)
		return 0;
	sz = PG_DIFF_RTV_HDR + p->len;
	memcpy(out, p, sz);
	*outlen = sz;
	return 1;
}

void
pg_diff_rtv_iter_end(void)
{
	rtv_end_iterate(pg_diff_rtv_it);
	pg_diff_rtv_it = NULL;
}

uint64
pg_diff_rtv_memory_usage(void)
{
	return rtv_memory_usage(pg_diff_rtv);
}

int64
pg_diff_rtv_num_keys(void)
{
	return pg_diff_rtv->ctl->num_keys;
}
