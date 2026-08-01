/*
 * pg_libfam_io.c: vendored PostgreSQL C oracle for the libfam_diff
 * differential fuzz target (100%-coverage campaign; crates
 * crates/backend/lib/{hyperloglog,binaryheap,pairingheap,bloomfilter,
 * integerset}).
 *
 * Provenance (all PostgreSQL bodies VERBATIM; the five .c files below are
 * BYTE-FOR-BYTE copies of postgres-src @
 * 62d6c7d3df6287f1bd83199c1a746e50d31571a0 (Stamp-18.3), included whole —
 * `cmp` against the vendor tree passes for every file):
 *   - csrc/libfam/vendor/hyperloglog.c  = src/backend/lib/hyperloglog.c
 *   - csrc/libfam/vendor/binaryheap.c   = src/common/binaryheap.c
 *                                         (FRONTEND undefined: backend arm)
 *   - csrc/libfam/vendor/pairingheap.c  = src/backend/lib/pairingheap.c
 *                                         (PAIRINGHEAP_DEBUG undefined,
 *                                         upstream default: dump code out)
 *   - csrc/libfam/vendor/bloomfilter.c  = src/backend/lib/bloomfilter.c
 *   - csrc/libfam/vendor/integerset.c   = src/backend/lib/integerset.c
 *   - csrc/libfam/include/lib/ headers  = src/include/lib/ headers (verbatim;
 *     stringinfo.h is a SHIM, see its header — its only consumer is
 *     compiled out)
 *   - csrc/libfam/include/port/pg_bitutils.h = REDUCED, verbatim pieces
 *     (provenance + linkage notes in that file's header)
 *   - csrc/libfam/include/common/hashfn.h    = REDUCED, verbatim pieces;
 *     the hash_bytes_extended DEFINITION is the verbatim body already
 *     vendored in csrc/pg_hashfn_io.c (same cc build, one definition)
 *
 * Shims (plumbing only, never logic — see also the SHIM headers under
 * csrc/libfam/include/{lib/stringinfo.h,utils/memutils.h}):
 *   - shim postgres.h supplies fixed-width typedefs matching c.h on LP64;
 *     this file adds Size/Datum/bits8/Min/Max/BITS_PER_BYTE/PG_UINT32_MAX/
 *     TYPEALIGN/FLEXIBLE_ARRAY_MEMBER/UInt64GetDatum/DatumGetInt64 with
 *     c.h's / postgres.h's exact LP64 definitions.
 *   - elog(ERROR, ...) -> record PG_DIFF_ERR_INTERNAL in the shared TLS
 *     pg_diff_errcode channel and longjmp to the armed driver entry
 *     (models PG's error longjmp). These five files use only elog(ERROR),
 *     never ereport: hyperloglog.c:71 (bad bwidth), binaryheap.c:123/161
 *     (out of slots), integerset.c:372/375/497 (iteration interlock,
 *     out-of-order add, max levels). Message text out of scope.
 *   - palloc/palloc0/pfree/MemoryContextAlloc -> TLS leak-tracking arena
 *     (models PG's memory-context reset so error-path longjmp exits cannot
 *     leak; scaffold-emitted pattern, proofs/p1-lanej precedent).
 *   - GetMemoryChunkSpace -> 0 (SHIM memutils.h). CARVE: C
 *     intset_memory_usage reports aset chunk-header accounting — a
 *     malloc-layout non-surface; the memory-usage plane is NOT compared
 *     (driver header documents it; both entry points still execute).
 *
 * Driver entries (section 3) are fuzz plumbing, NOT Postgres code. The
 * binaryheap comparator (int64 max-heap over Datum-carried values) and the
 * pairingheap fixture node type (pg_diff_ph_item embedding pairingheap_node,
 * pairingheap.h's documented "embed this in a larger struct" consumer
 * pattern) are HARNESS payload/comparator code — the container code itself
 * stays verbatim vendored C. The Rust driver uses byte-identical comparator
 * semantics.
 */

#include "postgres.h"

#include <assert.h>
#include <math.h>
#include <setjmp.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

extern _Thread_local int pg_diff_errcode;

#define PG_DIFF_ERR_INTERNAL 7	/* elog / cannot-happen class */

/* shim postgres.h maps palloc to bare malloc for the ryu TUs; this TU uses
 * the leak-tracking arena below instead. */
#undef palloc

/* ---- SHIM: c.h / postgres.h surface on LP64 (exact upstream values) ---- */
typedef size_t Size;
typedef uintptr_t Datum;
typedef uint8 bits8;

#define FLEXIBLE_ARRAY_MEMBER	/* empty */
#define BITS_PER_BYTE 8
#define PG_UINT32_MAX UINT32_C(0xFFFFFFFF)
#define Min(x, y)		((x) < (y) ? (x) : (y))
#define Max(x, y)		((x) > (y) ? (x) : (y))
#define TYPEALIGN(ALIGNVAL,LEN)  \
	(((uintptr_t) (LEN) + ((ALIGNVAL) - 1)) & ~((uintptr_t) ((ALIGNVAL) - 1)))
#define UInt64GetDatum(X) ((Datum) (X))
#define DatumGetUInt64(X) ((uint64) (X))
#define DatumGetInt64(X) ((int64) (X))
#define Int64GetDatum(X) ((Datum) (X))

/* ---- SHIM: error longjmp (per-TU jmp_buf; armed by every driver entry
 * that can reach an elog) ---- */
static _Thread_local jmp_buf pg_diff_libfam_jmp;

__attribute__((noreturn)) static void
pg_diff_libfam_throw(void)
{
	longjmp(pg_diff_libfam_jmp, 1);
}

#define elog(level, ...) \
	do { pg_diff_errcode = PG_DIFF_ERR_INTERNAL; pg_diff_libfam_throw(); } while (0)

/* ---- SHIM: TLS palloc arena (scaffold-emitted pattern) ---- */
#define PG_DIFF_ARENA_MAX 8192
static _Thread_local void *pg_diff_arena[PG_DIFF_ARENA_MAX];
static _Thread_local int pg_diff_arena_n;

static void
pg_diff_arena_reset(void)
{
	int			i;

	for (i = 0; i < pg_diff_arena_n; i++)
		free(pg_diff_arena[i]);
	pg_diff_arena_n = 0;
}

void *
pg_diff_libfam_alloc(size_t n)
{
	void	   *p = malloc(n);

	assert(pg_diff_arena_n < PG_DIFF_ARENA_MAX);
	pg_diff_arena[pg_diff_arena_n++] = p;
	return p;
}

static void *
pg_diff_libfam_alloc0(size_t n)
{
	void	   *p = calloc(1, n);

	assert(pg_diff_arena_n < PG_DIFF_ARENA_MAX);
	pg_diff_arena[pg_diff_arena_n++] = p;
	return p;
}

static void
pg_diff_libfam_pfree(void *p)
{
	int			i;

	for (i = 0; i < pg_diff_arena_n; i++)
	{
		if (pg_diff_arena[i] == p)
		{
			free(p);
			pg_diff_arena[i] = pg_diff_arena[--pg_diff_arena_n];
			return;
		}
	}
	assert(!"pfree of a pointer the arena never issued");
	abort();
}

#define palloc(n) pg_diff_libfam_alloc(n)
#define palloc0(n) pg_diff_libfam_alloc0(n)
#define pfree(p) pg_diff_libfam_pfree(p)

/* ============ SECTION 2: the VERBATIM vendored files ============ */

#include "libfam/vendor/hyperloglog.c"
#include "libfam/vendor/binaryheap.c"
#include "libfam/vendor/pairingheap.c"
#include "libfam/vendor/bloomfilter.c"
#include "libfam/vendor/integerset.c"

/* ============ SECTION 3: driver entries (fuzz plumbing, NOT Postgres code) ============ */

/*
 * Per-exec TLS instances. pg_diff_libfam_reset() drops everything (the
 * arena free models the per-query context reset every consumer of these
 * structures lives under).
 */
static _Thread_local hyperLogLogState pg_diff_hll_state;
static _Thread_local int pg_diff_hll_live;

static _Thread_local binaryheap *pg_diff_bh;
static _Thread_local pairingheap pg_diff_ph;

typedef struct pg_diff_ph_item
{
	pairingheap_node ph_node;
	int64		value;
} pg_diff_ph_item;

#define PG_DIFF_PH_MAX 600
static _Thread_local pg_diff_ph_item pg_diff_ph_slots[PG_DIFF_PH_MAX];
static _Thread_local int pg_diff_ph_nslots;

static _Thread_local bloom_filter *pg_diff_bloom;
static _Thread_local IntegerSet *pg_diff_intset;

/* HARNESS comparator: int64 max-heap (Rust driver uses the identical one). */
static int
pg_diff_bh_cmp(bh_node_type a, bh_node_type b, void *arg)
{
	int64		va = DatumGetInt64(a);
	int64		vb = DatumGetInt64(b);

	(void) arg;
	return (va > vb) - (va < vb);
}

/* HARNESS comparator: int64 max-heap over the fixture nodes. */
static int
pg_diff_ph_cmp(const pairingheap_node *a, const pairingheap_node *b, void *arg)
{
	const pg_diff_ph_item *ia = (const pg_diff_ph_item *) a;
	const pg_diff_ph_item *ib = (const pg_diff_ph_item *) b;

	(void) arg;
	return (ia->value > ib->value) - (ia->value < ib->value);
}

void
pg_diff_libfam_reset(void)
{
	pg_diff_hll_live = 0;
	pg_diff_bh = NULL;
	pg_diff_ph.ph_compare = pg_diff_ph_cmp;
	pg_diff_ph.ph_arg = NULL;
	pg_diff_ph.ph_root = NULL;
	pg_diff_ph_nslots = 0;
	pg_diff_bloom = NULL;
	pg_diff_intset = NULL;
	pg_diff_errcode = 0;
	pg_diff_arena_reset();
}

/* ---- hyperloglog ---- */

int
pg_diff_hll_init(int bwidth)
{
	pg_diff_errcode = 0;
	if (setjmp(pg_diff_libfam_jmp) != 0)
		return -1;
	initHyperLogLog(&pg_diff_hll_state, (uint8) bwidth);
	pg_diff_hll_live = 1;
	return 0;
}

void
pg_diff_hll_add(uint32 hash)
{
	addHyperLogLog(&pg_diff_hll_state, hash);
}

double
pg_diff_hll_estimate(void)
{
	return estimateHyperLogLog(&pg_diff_hll_state);
}

/* Single register readback (per-add touched-register plane). */
int
pg_diff_hll_reg_at(int idx)
{
	if (idx < 0 || (Size) idx >= pg_diff_hll_state.nRegisters)
		return -1;
	return pg_diff_hll_state.hashesArr[idx];
}

/* Copy out the register file (the full observable state). Returns the
 * register count (hashesArr's trailing +1 byte is C's historical alloc
 * quirk, never read). */
int
pg_diff_hll_regs(uint8 *out, int cap)
{
	int			n = (int) pg_diff_hll_state.nRegisters;

	if (n > cap)
		return -1;
	memcpy(out, pg_diff_hll_state.hashesArr, n);
	return n;
}

/* ---- binaryheap ---- */

void
pg_diff_bh_create(int capacity)
{
	pg_diff_bh = binaryheap_allocate(capacity, pg_diff_bh_cmp, NULL);
}

int
pg_diff_bh_add(int64 v)
{
	pg_diff_errcode = 0;
	if (setjmp(pg_diff_libfam_jmp) != 0)
		return -1;
	binaryheap_add(pg_diff_bh, Int64GetDatum(v));
	return 0;
}

int
pg_diff_bh_add_unordered(int64 v)
{
	pg_diff_errcode = 0;
	if (setjmp(pg_diff_libfam_jmp) != 0)
		return -1;
	binaryheap_add_unordered(pg_diff_bh, Int64GetDatum(v));
	return 0;
}

void
pg_diff_bh_build(void)
{
	binaryheap_build(pg_diff_bh);
}

int64
pg_diff_bh_first(void)
{
	return DatumGetInt64(binaryheap_first(pg_diff_bh));
}

int64
pg_diff_bh_remove_first(void)
{
	return DatumGetInt64(binaryheap_remove_first(pg_diff_bh));
}

void
pg_diff_bh_remove_node(int n)
{
	binaryheap_remove_node(pg_diff_bh, n);
}

void
pg_diff_bh_replace_first(int64 v)
{
	binaryheap_replace_first(pg_diff_bh, Int64GetDatum(v));
}

int
pg_diff_bh_size(void)
{
	return binaryheap_size(pg_diff_bh);
}

int64
pg_diff_bh_get(int n)
{
	return DatumGetInt64(binaryheap_get_node(pg_diff_bh, n));
}

void
pg_diff_bh_reset(void)
{
	binaryheap_reset(pg_diff_bh);
}

/* ---- pairingheap (fixture nodes; heap struct embedded, the documented
 * "embed this in a larger struct" consumer pattern from pairingheap.h) ---- */

int
pg_diff_ph_add(int64 v)
{
	pg_diff_ph_item *it;

	if (pg_diff_ph_nslots >= PG_DIFF_PH_MAX)
		return -1;				/* driver fences this */
	it = &pg_diff_ph_slots[pg_diff_ph_nslots];
	it->value = v;
	pairingheap_add(&pg_diff_ph, &it->ph_node);
	return pg_diff_ph_nslots++;
}

int
pg_diff_ph_is_empty(void)
{
	return pairingheap_is_empty(&pg_diff_ph);
}

int
pg_diff_ph_is_singular(void)
{
	return pairingheap_is_singular(&pg_diff_ph);
}

int64
pg_diff_ph_first(void)
{
	return ((pg_diff_ph_item *) pairingheap_first(&pg_diff_ph))->value;
}

/* Slot index of the current root (node identity plane; pairs with the Rust
 * driver's first_id so structural agreement is witnessed, not just values). */
int
pg_diff_ph_first_slot(void)
{
	pg_diff_ph_item *it = (pg_diff_ph_item *) pairingheap_first(&pg_diff_ph);

	return (int) (it - pg_diff_ph_slots);
}

int64
pg_diff_ph_remove_first(void)
{
	return ((pg_diff_ph_item *) pairingheap_remove_first(&pg_diff_ph))->value;
}

int64
pg_diff_ph_remove(int slot)
{
	pg_diff_ph_item *it = &pg_diff_ph_slots[slot];

	pairingheap_remove(&pg_diff_ph, &it->ph_node);
	return it->value;
}

void
pg_diff_ph_reset(void)
{
	pairingheap_reset(&pg_diff_ph);
	pg_diff_ph_nslots = 0;
}

/* ---- bloomfilter ---- */

void
pg_diff_bloom_create(int64 total_elems, int work_mem, uint64 seed)
{
	pg_diff_bloom = bloom_create(total_elems, work_mem, seed);
}

int
pg_diff_bloom_k(void)
{
	return pg_diff_bloom->k_hash_funcs;
}

uint64
pg_diff_bloom_m(void)
{
	return pg_diff_bloom->m;
}

void
pg_diff_bloom_add(const unsigned char *elem, size_t len)
{
	bloom_add_element(pg_diff_bloom, (unsigned char *) elem, len);
}

int
pg_diff_bloom_lacks(const unsigned char *elem, size_t len)
{
	return bloom_lacks_element(pg_diff_bloom, (unsigned char *) elem, len);
}

double
pg_diff_bloom_prop(void)
{
	return bloom_prop_bits_set(pg_diff_bloom);
}

/* Compare the Rust bitset image against C's without copying out. */
int
pg_diff_bloom_bitset_eq(const unsigned char *bits, size_t len)
{
	if (len != pg_diff_bloom->m / BITS_PER_BYTE)
		return 0;
	return memcmp(bits, pg_diff_bloom->bitset, len) == 0;
}

/* ---- integerset ---- */

void
pg_diff_intset_create(void)
{
	pg_diff_intset = intset_create();
}

int
pg_diff_intset_add(uint64 x)
{
	pg_diff_errcode = 0;
	if (setjmp(pg_diff_libfam_jmp) != 0)
		return -1;
	intset_add_member(pg_diff_intset, x);
	return 0;
}

int
pg_diff_intset_is_member(uint64 x)
{
	return intset_is_member(pg_diff_intset, x);
}

uint64
pg_diff_intset_num_entries(void)
{
	return intset_num_entries(pg_diff_intset);
}

uint64
pg_diff_intset_mem_usage(void)
{
	return intset_memory_usage(pg_diff_intset);
}

void
pg_diff_intset_begin_iterate(void)
{
	intset_begin_iterate(pg_diff_intset);
}

int
pg_diff_intset_iterate_next(uint64 *out)
{
	return intset_iterate_next(pg_diff_intset, out);
}
