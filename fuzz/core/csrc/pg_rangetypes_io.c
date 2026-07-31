/*
 * pg_rangetypes_io.c: vendored PostgreSQL C oracle for the rangetypes_diff differential
 * fuzz target (100%-coverage campaign; crate crates/backend/utils/adt/rangetypes).
 *
 * GENERATED SKELETON (fuzz/scaffold.py) — NOT yet a valid oracle. Every
 * TODO(scaffold) paste site below must be filled with VERBATIM upstream C,
 * and every #error compile gate removed WITH its paste, before the
 * .file("csrc/pg_rangetypes_io.c") line in core/build.rs is uncommented. A
 * half-filled shim can therefore never silently build or link.
 *
 * Provenance (fill in as you paste; follow csrc/pg_uuid_io.c):
 *   - Vendor sections 1..N byte-for-byte from src/backend/utils/adt/rangetypes.c
 *     @ postgres-src 62d6c7d3df6287f1bd83199c1a746e50d31571a0
 *     (PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df); re-verify against the repo's vendored ground-truth
 *     checkout ../pgrust-reference/vendor/postgres-src before pasting).
 *   - Functions to vendor: range_in, range_out, range_recv, range_send, range_constructor2, range_constructor3, range_lower, range_upper, range_empty, range_lower_inc, range_upper_inc, range_lower_inf, range_upper_inf, range_adjacent, range_overleft, range_overright, range_union, range_intersect, range_minus, range_merge, hash_range, hash_range_extended, int4range_canonical, int8range_canonical, daterange_canonical, int4range_subdiff, int8range_subdiff, numrange_subdiff, daterange_subdiff, tsrange_subdiff, tstzrange_subdiff.
 *   - Bodies VERBATIM except documented shims; shims are PLUMBING ONLY
 *     (isxdigit/strtoul C-locale shims, ereturn -> int sentinel, fmgr
 *     PG_FUNCTION_ARGS unwrapped to plain C signatures, palloc'd results ->
 *     caller buffers, wire triples for recv/send), NEVER logic. List every
 *     shim in this header when you paste.
 *   - palloc/palloc0/repalloc/pfree -> the TLS pointer arena below (NOT
 *     bare malloc/free): models PG's memory-context reset; error paths
 *     strand allocations otherwise. Do NOT free() arena pointers by hand.
 *
 * Errcode capture follows csrc/pg_float_io.c: the shared _Thread_local
 * pg_diff_errcode (defined there) records the errcode class; map each
 * errcode this crate's C raises to a small class constant below.
 */

#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

/* Shared TLS errcode channel (defined in csrc/pg_float_io.c). */
extern _Thread_local int pg_diff_errcode;

/* TODO(scaffold): one class constant per distinct errcode the vendored C
 * raises, e.g.:
 *   #define PG_DIFF_ERR_INVALID_TEXT 1   (22P02)
 */

/* palloc arena shim: PostgreSQL frees these via memory-context reset; the
 * oracle mirrors that with a TLS pointer arena reset at every pg_diff_*
 * dispatcher entry, so error-path longjmp/ereturn/goto exits cannot leak.
 * (Three LSan incidents of the naive palloc->malloc mapping on 2026-07-31;
 * pattern proven on proofs/p1-lanej @ 7306d300196 — copied, not re-derived.
 * Final-exec allocations stay rooted in the arena, so LSan's exit scan is
 * quiet without any manual free().) */
#define PG_DIFF_ARENA_MAX 64
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

static void *
pg_diff_palloc_impl(size_t n)
{
	void	   *p = malloc(n);

	assert(pg_diff_arena_n < PG_DIFF_ARENA_MAX);
	pg_diff_arena[pg_diff_arena_n++] = p;
	return p;
}

static void *
pg_diff_palloc0_impl(size_t n)
{
	void	   *p = calloc(1, n);

	assert(pg_diff_arena_n < PG_DIFF_ARENA_MAX);
	pg_diff_arena[pg_diff_arena_n++] = p;
	return p;
}

static void *
pg_diff_repalloc_impl(void *old, size_t n)
{
	void	   *p = realloc(old, n);
	int			i;

	for (i = 0; i < pg_diff_arena_n; i++)
	{
		if (pg_diff_arena[i] == old)
		{
			pg_diff_arena[i] = p;
			return p;
		}
	}
	assert(!"repalloc of a pointer the arena never issued");
	return p;
}

static void
pg_diff_pfree_impl(void *p)
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
	/* abort-loud: freeing a pointer the arena never issued is a shim bug
	 * (double-free after reset, or a bare malloc that bypassed palloc). */
	assert(!"pfree of a pointer the arena never issued");
	abort();
}

#define palloc(n) pg_diff_palloc_impl(n)
#define palloc0(n) pg_diff_palloc0_impl(n)
#define repalloc(p, n) pg_diff_repalloc_impl((p), (n))
#define pfree(p) pg_diff_pfree_impl(p)

/* ==================== SECTION 1: rangetypes.c (VERBATIM) ==================== */

/*
 * TODO(scaffold): paste here, byte-for-byte from
 * src/backend/utils/adt/rangetypes.c @ 62d6c7d3df6287f1bd83199c1a746e50d31571a0,
 * the bodies backing: range_in, range_out, range_recv, range_send, range_constructor2, range_constructor3, range_lower, range_upper, range_empty, range_lower_inc, range_upper_inc, range_lower_inf, range_upper_inf, range_adjacent, range_overleft, range_overright, range_union, range_intersect, range_minus, range_merge, hash_range, hash_range_extended, int4range_canonical, int8range_canonical, daterange_canonical, int4range_subdiff, int8range_subdiff, numrange_subdiff, daterange_subdiff, tsrange_subdiff, tstzrange_subdiff
 * (rename with a pg_ prefix; unwrap fmgr wrappers; document every shim in
 * the file header above). Remove the #error line together with the paste.
 */
#error "SCAFFOLD-TODO(rangetypes_diff): verbatim C from rangetypes.c not pasted yet"

/* ========== SECTION 2: fuzz-facing driver entries (NOT Postgres code) ===== */

/*
 * One thin pg_diff_* wrapper per fuzz arm: FIRST pg_diff_arena_reset()
 * (models PG's memory-context reset; error paths strand allocations
 * otherwise), then reset pg_diff_errcode = 0, call the vendored function,
 * return an int status (0 = ok, nonzero = error class) and write results
 * through caller-provided buffers. Shape them after csrc/pg_uuid_io.c
 * section 4, e.g.:
 *
 *   int pg_diff_uuid_in(const char *source, unsigned char *out)
 *   {
 *       pg_uuid_t u;
 *       pg_diff_arena_reset();
 *       pg_diff_errcode = 0;
 *       if (pg_string_to_uuid(source, &u) != 0)
 *       {
 *           pg_diff_errcode = PG_DIFF_ERR_INVALID_TEXT;
 *           return 1;
 *       }
 *       memcpy(out, u.data, UUID_LEN);
 *       return 0;
 *   }
 */
/*
 * TODO(scaffold): int pg_diff_range_in(...)   [oid 3834, rangetypes.c]
 * (first line of the body: pg_diff_arena_reset(); — see the arena header)
 */
#error "SCAFFOLD-TODO(rangetypes_diff): pg_diff_range_in driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_range_out(...)   [oid 3835, rangetypes.c]
 * (first line of the body: pg_diff_arena_reset(); — see the arena header)
 */
#error "SCAFFOLD-TODO(rangetypes_diff): pg_diff_range_out driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_range_recv(...)   [oid 3836, rangetypes.c]
 * (first line of the body: pg_diff_arena_reset(); — see the arena header)
 */
#error "SCAFFOLD-TODO(rangetypes_diff): pg_diff_range_recv driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_range_send(...)   [oid 3837, rangetypes.c]
 * (first line of the body: pg_diff_arena_reset(); — see the arena header)
 */
#error "SCAFFOLD-TODO(rangetypes_diff): pg_diff_range_send driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_range_constructor2(...)   [oid 3840, rangetypes.c]
 * (first line of the body: pg_diff_arena_reset(); — see the arena header)
 */
#error "SCAFFOLD-TODO(rangetypes_diff): pg_diff_range_constructor2 driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_range_constructor3(...)   [oid 3841, rangetypes.c]
 * (first line of the body: pg_diff_arena_reset(); — see the arena header)
 */
#error "SCAFFOLD-TODO(rangetypes_diff): pg_diff_range_constructor3 driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_range_lower(...)   [oid 3848, rangetypes.c]
 * (first line of the body: pg_diff_arena_reset(); — see the arena header)
 */
#error "SCAFFOLD-TODO(rangetypes_diff): pg_diff_range_lower driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_range_upper(...)   [oid 3849, rangetypes.c]
 * (first line of the body: pg_diff_arena_reset(); — see the arena header)
 */
#error "SCAFFOLD-TODO(rangetypes_diff): pg_diff_range_upper driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_range_empty(...)   [oid 3850, rangetypes.c]
 * (first line of the body: pg_diff_arena_reset(); — see the arena header)
 */
#error "SCAFFOLD-TODO(rangetypes_diff): pg_diff_range_empty driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_range_lower_inc(...)   [oid 3851, rangetypes.c]
 * (first line of the body: pg_diff_arena_reset(); — see the arena header)
 */
#error "SCAFFOLD-TODO(rangetypes_diff): pg_diff_range_lower_inc driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_range_upper_inc(...)   [oid 3852, rangetypes.c]
 * (first line of the body: pg_diff_arena_reset(); — see the arena header)
 */
#error "SCAFFOLD-TODO(rangetypes_diff): pg_diff_range_upper_inc driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_range_lower_inf(...)   [oid 3853, rangetypes.c]
 * (first line of the body: pg_diff_arena_reset(); — see the arena header)
 */
#error "SCAFFOLD-TODO(rangetypes_diff): pg_diff_range_lower_inf driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_range_upper_inf(...)   [oid 3854, rangetypes.c]
 * (first line of the body: pg_diff_arena_reset(); — see the arena header)
 */
#error "SCAFFOLD-TODO(rangetypes_diff): pg_diff_range_upper_inf driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_range_adjacent(...)   [oid 3862, rangetypes.c]
 * (first line of the body: pg_diff_arena_reset(); — see the arena header)
 */
#error "SCAFFOLD-TODO(rangetypes_diff): pg_diff_range_adjacent driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_range_overleft(...)   [oid 3865, rangetypes.c]
 * (first line of the body: pg_diff_arena_reset(); — see the arena header)
 */
#error "SCAFFOLD-TODO(rangetypes_diff): pg_diff_range_overleft driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_range_overright(...)   [oid 3866, rangetypes.c]
 * (first line of the body: pg_diff_arena_reset(); — see the arena header)
 */
#error "SCAFFOLD-TODO(rangetypes_diff): pg_diff_range_overright driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_range_union(...)   [oid 3867, rangetypes.c]
 * (first line of the body: pg_diff_arena_reset(); — see the arena header)
 */
#error "SCAFFOLD-TODO(rangetypes_diff): pg_diff_range_union driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_range_intersect(...)   [oid 3868, rangetypes.c]
 * (first line of the body: pg_diff_arena_reset(); — see the arena header)
 */
#error "SCAFFOLD-TODO(rangetypes_diff): pg_diff_range_intersect driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_range_minus(...)   [oid 3869, rangetypes.c]
 * (first line of the body: pg_diff_arena_reset(); — see the arena header)
 */
#error "SCAFFOLD-TODO(rangetypes_diff): pg_diff_range_minus driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_range_merge(...)   [oid 4057, rangetypes.c]
 * (first line of the body: pg_diff_arena_reset(); — see the arena header)
 */
#error "SCAFFOLD-TODO(rangetypes_diff): pg_diff_range_merge driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_hash_range(...)   [oid 3902, rangetypes.c]
 * (first line of the body: pg_diff_arena_reset(); — see the arena header)
 */
#error "SCAFFOLD-TODO(rangetypes_diff): pg_diff_hash_range driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_hash_range_extended(...)   [oid 3417, rangetypes.c]
 * (first line of the body: pg_diff_arena_reset(); — see the arena header)
 */
#error "SCAFFOLD-TODO(rangetypes_diff): pg_diff_hash_range_extended driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_int4range_canonical(...)   [oid 3914, rangetypes.c]
 * (first line of the body: pg_diff_arena_reset(); — see the arena header)
 */
#error "SCAFFOLD-TODO(rangetypes_diff): pg_diff_int4range_canonical driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_int8range_canonical(...)   [oid 3928, rangetypes.c]
 * (first line of the body: pg_diff_arena_reset(); — see the arena header)
 */
#error "SCAFFOLD-TODO(rangetypes_diff): pg_diff_int8range_canonical driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_daterange_canonical(...)   [oid 3915, rangetypes.c]
 * (first line of the body: pg_diff_arena_reset(); — see the arena header)
 */
#error "SCAFFOLD-TODO(rangetypes_diff): pg_diff_daterange_canonical driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_int4range_subdiff(...)   [oid 3922, rangetypes.c]
 * (first line of the body: pg_diff_arena_reset(); — see the arena header)
 */
#error "SCAFFOLD-TODO(rangetypes_diff): pg_diff_int4range_subdiff driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_int8range_subdiff(...)   [oid 3923, rangetypes.c]
 * (first line of the body: pg_diff_arena_reset(); — see the arena header)
 */
#error "SCAFFOLD-TODO(rangetypes_diff): pg_diff_int8range_subdiff driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_numrange_subdiff(...)   [oid 3924, rangetypes.c]
 * (first line of the body: pg_diff_arena_reset(); — see the arena header)
 */
#error "SCAFFOLD-TODO(rangetypes_diff): pg_diff_numrange_subdiff driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_daterange_subdiff(...)   [oid 3925, rangetypes.c]
 * (first line of the body: pg_diff_arena_reset(); — see the arena header)
 */
#error "SCAFFOLD-TODO(rangetypes_diff): pg_diff_daterange_subdiff driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_tsrange_subdiff(...)   [oid 3929, rangetypes.c]
 * (first line of the body: pg_diff_arena_reset(); — see the arena header)
 */
#error "SCAFFOLD-TODO(rangetypes_diff): pg_diff_tsrange_subdiff driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_tstzrange_subdiff(...)   [oid 3930, rangetypes.c]
 * (first line of the body: pg_diff_arena_reset(); — see the arena header)
 */
#error "SCAFFOLD-TODO(rangetypes_diff): pg_diff_tstzrange_subdiff driver entry not written yet"
