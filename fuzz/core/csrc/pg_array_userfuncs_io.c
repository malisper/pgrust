/*
 * pg_array_userfuncs_io.c: vendored PostgreSQL C oracle for the array_userfuncs_diff differential
 * fuzz target (100%-coverage campaign; crate crates/backend/utils/adt/array_userfuncs).
 *
 * GENERATED SKELETON (fuzz/scaffold.py) — NOT yet a valid oracle. Every
 * TODO(scaffold) paste site below must be filled with VERBATIM upstream C,
 * and every #error compile gate removed WITH its paste, before the
 * .file("csrc/pg_array_userfuncs_io.c") line in core/build.rs is uncommented. A
 * half-filled shim can therefore never silently build or link.
 *
 * Provenance (fill in as you paste; follow csrc/pg_uuid_io.c):
 *   - Vendor sections 1..N byte-for-byte from src/backend/utils/adt/array_userfuncs.c
 *     @ postgres-src 62d6c7d3df6287f1bd83199c1a746e50d31571a0
 *     (PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df); re-verify against the repo's vendored ground-truth
 *     checkout ../pgrust-reference/vendor/postgres-src before pasting).
 *   - Functions to vendor: array_append, array_prepend, array_cat, array_position, array_position_start, array_positions, trim_array, array_reverse, array_shuffle, array_sample, array_agg_array_serialize, array_agg_array_deserialize, array_agg_array_combine.
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

/* ==================== SECTION 1: array_userfuncs.c (VERBATIM) ==================== */

/*
 * TODO(scaffold): paste here, byte-for-byte from
 * src/backend/utils/adt/array_userfuncs.c @ 62d6c7d3df6287f1bd83199c1a746e50d31571a0,
 * the bodies backing: array_append, array_prepend, array_cat, array_position, array_position_start, array_positions, trim_array, array_reverse, array_shuffle, array_sample, array_agg_array_serialize, array_agg_array_deserialize, array_agg_array_combine
 * (rename with a pg_ prefix; unwrap fmgr wrappers; document every shim in
 * the file header above). Remove the #error line together with the paste.
 */
#error "SCAFFOLD-TODO(array_userfuncs_diff): verbatim C from array_userfuncs.c not pasted yet"

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
 * TODO(scaffold): int pg_diff_array_append(...)   [oid 378, array_userfuncs.c]
 * (first line of the body: pg_diff_arena_reset(); — see the arena header)
 */
#error "SCAFFOLD-TODO(array_userfuncs_diff): pg_diff_array_append driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_array_prepend(...)   [oid 379, array_userfuncs.c]
 * (first line of the body: pg_diff_arena_reset(); — see the arena header)
 */
#error "SCAFFOLD-TODO(array_userfuncs_diff): pg_diff_array_prepend driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_array_cat(...)   [oid 383, array_userfuncs.c]
 * (first line of the body: pg_diff_arena_reset(); — see the arena header)
 */
#error "SCAFFOLD-TODO(array_userfuncs_diff): pg_diff_array_cat driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_array_position(...)   [oid 3277, array_userfuncs.c]
 * (first line of the body: pg_diff_arena_reset(); — see the arena header)
 */
#error "SCAFFOLD-TODO(array_userfuncs_diff): pg_diff_array_position driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_array_position_start(...)   [oid 3278, array_userfuncs.c]
 * (first line of the body: pg_diff_arena_reset(); — see the arena header)
 */
#error "SCAFFOLD-TODO(array_userfuncs_diff): pg_diff_array_position_start driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_array_positions(...)   [oid 3279, array_userfuncs.c]
 * (first line of the body: pg_diff_arena_reset(); — see the arena header)
 */
#error "SCAFFOLD-TODO(array_userfuncs_diff): pg_diff_array_positions driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_trim_array(...)   [oid 6172, array_userfuncs.c]
 * (first line of the body: pg_diff_arena_reset(); — see the arena header)
 */
#error "SCAFFOLD-TODO(array_userfuncs_diff): pg_diff_trim_array driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_array_reverse(...)   [oid 6381, array_userfuncs.c]
 * (first line of the body: pg_diff_arena_reset(); — see the arena header)
 */
#error "SCAFFOLD-TODO(array_userfuncs_diff): pg_diff_array_reverse driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_array_shuffle(...)   [oid 6215, array_userfuncs.c]
 * (first line of the body: pg_diff_arena_reset(); — see the arena header)
 */
#error "SCAFFOLD-TODO(array_userfuncs_diff): pg_diff_array_shuffle driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_array_sample(...)   [oid 6216, array_userfuncs.c]
 * (first line of the body: pg_diff_arena_reset(); — see the arena header)
 */
#error "SCAFFOLD-TODO(array_userfuncs_diff): pg_diff_array_sample driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_array_agg_array_serialize(...)   [oid 6297, array_userfuncs.c]
 * (first line of the body: pg_diff_arena_reset(); — see the arena header)
 */
#error "SCAFFOLD-TODO(array_userfuncs_diff): pg_diff_array_agg_array_serialize driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_array_agg_array_deserialize(...)   [oid 6298, array_userfuncs.c]
 * (first line of the body: pg_diff_arena_reset(); — see the arena header)
 */
#error "SCAFFOLD-TODO(array_userfuncs_diff): pg_diff_array_agg_array_deserialize driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_array_agg_array_combine(...)   [oid 6296, array_userfuncs.c]
 * (first line of the body: pg_diff_arena_reset(); — see the arena header)
 */
#error "SCAFFOLD-TODO(array_userfuncs_diff): pg_diff_array_agg_array_combine driver entry not written yet"
