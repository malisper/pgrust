/*
 * pg_tsvector_core_io.c: vendored PostgreSQL C oracle for the tsvector_core_diff differential
 * fuzz target (100%-coverage campaign; crate crates/backend/utils/adt/tsvector_core).
 *
 * GENERATED SKELETON (fuzz/scaffold.py) — NOT yet a valid oracle. Every
 * TODO(scaffold) paste site below must be filled with VERBATIM upstream C,
 * and every #error compile gate removed WITH its paste, before the
 * .file("csrc/pg_tsvector_core_io.c") line in core/build.rs is uncommented. A
 * half-filled shim can therefore never silently build or link.
 *
 * Provenance (fill in as you paste; follow csrc/pg_uuid_io.c):
 *   - Vendor sections 1..N byte-for-byte from src/backend/utils/adt/tsvector.c / src/backend/utils/adt/tsvector_op.c
 *     @ postgres-src 62d6c7d3df6287f1bd83199c1a746e50d31571a0
 *     (PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df); re-verify against the repo's vendored ground-truth
 *     checkout ../pgrust-reference/vendor/postgres-src before pasting).
 *   - Functions to vendor: tsvectorin, tsvectorout, tsvectorsend, tsvectorrecv, tsvector_lt, tsvector_le, tsvector_eq, tsvector_ne, tsvector_ge, tsvector_gt, tsvector_cmp, tsvector_strip, tsvector_setweight, tsvector_concat, tsvector_length, tsvector_filter, tsvector_setweight_by_filter, tsvector_delete_str, tsvector_delete_arr, tsvector_to_array, array_to_tsvector, ts_match_vq, ts_match_qv.
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

/* ==================== SECTION 1: tsvector.c (VERBATIM) ==================== */

/*
 * TODO(scaffold): paste here, byte-for-byte from
 * src/backend/utils/adt/tsvector.c @ 62d6c7d3df6287f1bd83199c1a746e50d31571a0,
 * the bodies backing: tsvectorin, tsvectorout, tsvectorsend, tsvectorrecv
 * (rename with a pg_ prefix; unwrap fmgr wrappers; document every shim in
 * the file header above). Remove the #error line together with the paste.
 */
#error "SCAFFOLD-TODO(tsvector_core_diff): verbatim C from tsvector.c not pasted yet"

/* ==================== SECTION 2: tsvector_op.c (VERBATIM) ==================== */

/*
 * TODO(scaffold): paste here, byte-for-byte from
 * src/backend/utils/adt/tsvector_op.c @ 62d6c7d3df6287f1bd83199c1a746e50d31571a0,
 * the bodies backing: tsvector_lt, tsvector_le, tsvector_eq, tsvector_ne, tsvector_ge, tsvector_gt, tsvector_cmp, tsvector_strip, tsvector_setweight, tsvector_concat, tsvector_length, tsvector_filter, tsvector_setweight_by_filter, tsvector_delete_str, tsvector_delete_arr, tsvector_to_array, array_to_tsvector, ts_match_vq, ts_match_qv
 * (rename with a pg_ prefix; unwrap fmgr wrappers; document every shim in
 * the file header above). Remove the #error line together with the paste.
 */
#error "SCAFFOLD-TODO(tsvector_core_diff): verbatim C from tsvector_op.c not pasted yet"

/* ========== SECTION 3: fuzz-facing driver entries (NOT Postgres code) ===== */

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
 * TODO(scaffold): int pg_diff_tsvectorin(...)   [oid 3610, tsvector.c]
 * (first line of the body: pg_diff_arena_reset(); — see the arena header)
 */
#error "SCAFFOLD-TODO(tsvector_core_diff): pg_diff_tsvectorin driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_tsvectorout(...)   [oid 3611, tsvector.c]
 * (first line of the body: pg_diff_arena_reset(); — see the arena header)
 */
#error "SCAFFOLD-TODO(tsvector_core_diff): pg_diff_tsvectorout driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_tsvectorsend(...)   [oid 3638, tsvector.c]
 * (first line of the body: pg_diff_arena_reset(); — see the arena header)
 */
#error "SCAFFOLD-TODO(tsvector_core_diff): pg_diff_tsvectorsend driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_tsvectorrecv(...)   [oid 3639, tsvector.c]
 * (first line of the body: pg_diff_arena_reset(); — see the arena header)
 */
#error "SCAFFOLD-TODO(tsvector_core_diff): pg_diff_tsvectorrecv driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_tsvector_lt(...)   [oid 3616, tsvector_op.c]
 * (first line of the body: pg_diff_arena_reset(); — see the arena header)
 */
#error "SCAFFOLD-TODO(tsvector_core_diff): pg_diff_tsvector_lt driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_tsvector_le(...)   [oid 3617, tsvector_op.c]
 * (first line of the body: pg_diff_arena_reset(); — see the arena header)
 */
#error "SCAFFOLD-TODO(tsvector_core_diff): pg_diff_tsvector_le driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_tsvector_eq(...)   [oid 3618, tsvector_op.c]
 * (first line of the body: pg_diff_arena_reset(); — see the arena header)
 */
#error "SCAFFOLD-TODO(tsvector_core_diff): pg_diff_tsvector_eq driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_tsvector_ne(...)   [oid 3619, tsvector_op.c]
 * (first line of the body: pg_diff_arena_reset(); — see the arena header)
 */
#error "SCAFFOLD-TODO(tsvector_core_diff): pg_diff_tsvector_ne driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_tsvector_ge(...)   [oid 3620, tsvector_op.c]
 * (first line of the body: pg_diff_arena_reset(); — see the arena header)
 */
#error "SCAFFOLD-TODO(tsvector_core_diff): pg_diff_tsvector_ge driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_tsvector_gt(...)   [oid 3621, tsvector_op.c]
 * (first line of the body: pg_diff_arena_reset(); — see the arena header)
 */
#error "SCAFFOLD-TODO(tsvector_core_diff): pg_diff_tsvector_gt driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_tsvector_cmp(...)   [oid 3622, tsvector_op.c]
 * (first line of the body: pg_diff_arena_reset(); — see the arena header)
 */
#error "SCAFFOLD-TODO(tsvector_core_diff): pg_diff_tsvector_cmp driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_tsvector_strip(...)   [oid 3623, tsvector_op.c]
 * (first line of the body: pg_diff_arena_reset(); — see the arena header)
 */
#error "SCAFFOLD-TODO(tsvector_core_diff): pg_diff_tsvector_strip driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_tsvector_setweight(...)   [oid 3624, tsvector_op.c]
 * (first line of the body: pg_diff_arena_reset(); — see the arena header)
 */
#error "SCAFFOLD-TODO(tsvector_core_diff): pg_diff_tsvector_setweight driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_tsvector_concat(...)   [oid 3625, tsvector_op.c]
 * (first line of the body: pg_diff_arena_reset(); — see the arena header)
 */
#error "SCAFFOLD-TODO(tsvector_core_diff): pg_diff_tsvector_concat driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_tsvector_length(...)   [oid 3711, tsvector_op.c]
 * (first line of the body: pg_diff_arena_reset(); — see the arena header)
 */
#error "SCAFFOLD-TODO(tsvector_core_diff): pg_diff_tsvector_length driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_tsvector_filter(...)   [oid 3319, tsvector_op.c]
 * (first line of the body: pg_diff_arena_reset(); — see the arena header)
 */
#error "SCAFFOLD-TODO(tsvector_core_diff): pg_diff_tsvector_filter driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_tsvector_setweight_by_filter(...)   [oid 3320, tsvector_op.c]
 * (first line of the body: pg_diff_arena_reset(); — see the arena header)
 */
#error "SCAFFOLD-TODO(tsvector_core_diff): pg_diff_tsvector_setweight_by_filter driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_tsvector_delete_str(...)   [oid 3321, tsvector_op.c]
 * (first line of the body: pg_diff_arena_reset(); — see the arena header)
 */
#error "SCAFFOLD-TODO(tsvector_core_diff): pg_diff_tsvector_delete_str driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_tsvector_delete_arr(...)   [oid 3323, tsvector_op.c]
 * (first line of the body: pg_diff_arena_reset(); — see the arena header)
 */
#error "SCAFFOLD-TODO(tsvector_core_diff): pg_diff_tsvector_delete_arr driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_tsvector_to_array(...)   [oid 3326, tsvector_op.c]
 * (first line of the body: pg_diff_arena_reset(); — see the arena header)
 */
#error "SCAFFOLD-TODO(tsvector_core_diff): pg_diff_tsvector_to_array driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_array_to_tsvector(...)   [oid 3327, tsvector_op.c]
 * (first line of the body: pg_diff_arena_reset(); — see the arena header)
 */
#error "SCAFFOLD-TODO(tsvector_core_diff): pg_diff_array_to_tsvector driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_ts_match_vq(...)   [oid 3634, tsvector_op.c]
 * (first line of the body: pg_diff_arena_reset(); — see the arena header)
 */
#error "SCAFFOLD-TODO(tsvector_core_diff): pg_diff_ts_match_vq driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_ts_match_qv(...)   [oid 3635, tsvector_op.c]
 * (first line of the body: pg_diff_arena_reset(); — see the arena header)
 */
#error "SCAFFOLD-TODO(tsvector_core_diff): pg_diff_ts_match_qv driver entry not written yet"
