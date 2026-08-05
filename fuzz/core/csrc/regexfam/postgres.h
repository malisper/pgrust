/*
 * SHIM HEADER (regexp_diff oracle, p1-laneag) — NOT vendored PostgreSQL.
 *
 * Minimal postgres.h replacement for the VERBATIM vendored regex engine TUs
 * in this directory (regcomp.c/regexec.c/regerror.c/regfree.c and the files
 * they #include), plus the mb/errcode glue.  Every definition here is
 * PLUMBING ONLY — allocator + interrupt + assert plumbing the engine
 * expects from the real postgres.h; no engine logic lives here.
 *
 * Shims (each documented at its definition):
 *   - palloc_extended(n, MCXT_ALLOC_NO_OOM) / repalloc_extended / pfree ->
 *     plain malloc/realloc/free.  The engine's MALLOC/REALLOC contract is
 *     "may return NULL" (regcustom.h passes MCXT_ALLOC_NO_OOM), and the
 *     engine frees everything it owns through pg_regfree / its own failure
 *     paths, so libc malloc reproduces the real backend behavior exactly
 *     (a regex_t leak across an oracle ereport-longjmp is prevented by the
 *     live-regex hook in pg_regexp_io.c, documented there).
 *   - CHECK_FOR_INTERRUPTS() -> no-op: the fuzz harness is single-threaded
 *     with no signal sources; CANCEL_REQUESTED is pinned false (task carve).
 *   - stack_is_too_deep() -> false: pattern length is capped at 128 bytes by
 *     the driver, far below any real recursion hazard; real PG only trips
 *     this on pathologically nested patterns.
 *   - Assert -> <assert.h> assert (active in the fuzz build; cc does not
 *     define NDEBUG).
 */
#ifndef PG_REGEXFAM_POSTGRES_H
#define PG_REGEXFAM_POSTGRES_H

#include <assert.h>
#include <limits.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef int bool_compat_check[(sizeof(char) == 1) ? 1 : -1];
#include <stdbool.h>

typedef unsigned int Oid;
#define InvalidOid ((Oid) 0)
#define OidIsValid(objectId)  ((bool) ((objectId) != InvalidOid))

typedef uint8_t uint8;
typedef uint32_t uint32;
typedef int32_t int32;
typedef int64_t int64;

/*
 * text: the oracle's varlena shim (regex/regex.h declares text-typed
 * prototypes).  Every text inside this oracle is an inline 4-byte-header
 * varlena (little-endian length word << 2, as on every supported host);
 * no short/toasted forms exist here, so VARDATA_ANY == VARDATA.  The
 * accessor macros live in pg_regexp_io.c, the only TU that uses them.
 */
typedef struct pg_regexfam_varlena
{
	uint32		vl_len_;
	char		vl_dat[];
} text;

/*
 * Assert: self-contained (NOT defined via <assert.h>'s assert): regcustom.h
 * redefines `assert` to Assert, which would leave a bare unexpandable
 * assert() call if Assert itself expanded back to assert.  Active in the
 * fuzz build (matches a --enable-cassert oracle; the debug-assert-masking
 * law wants engine invariants loud here).
 */
static inline void
pg_regexfam_assert_fail(const char *cond, const char *file, int line)
{
	fprintf(stderr, "regexfam Assert failed: %s (%s:%d)\n", cond, file, line);
	abort();
}
#define Assert(condition) \
	do { \
		if (!(condition)) \
			pg_regexfam_assert_fail(#condition, __FILE__, __LINE__); \
	} while (0)

/* c.h: flexible array member spelling (empty on C99+ compilers) */
#define FLEXIBLE_ARRAY_MEMBER	/* empty */

/* allocator plumbing (see header comment) */
#define MCXT_ALLOC_NO_OOM 0x02
static inline void *
palloc_extended(size_t size, int flags)
{
	(void) flags;
	return malloc(size);
}
static inline void *
repalloc_extended(void *pointer, size_t size, int flags)
{
	(void) flags;
	return realloc(pointer, size);
}
static inline void
pfree(void *pointer)
{
	free(pointer);
}

/* lengthof / unlikely conveniences some vendored code uses */
#define lengthof(array) (sizeof(array) / sizeof((array)[0]))
#ifndef unlikely
#define unlikely(x) (x)
#endif
#ifndef likely
#define likely(x) (x)
#endif

/*
 * ereport shim for the engine side.  Under the pinned C collation
 * (C_COLLATION_OID) the only ereport sites reachable from the vendored
 * engine (both in regc_pg_locale.c pg_set_regex_collation) are DEAD:
 * collation is always valid and always takes the C strategy.  Keep them
 * loud rather than silent: abort() if ever reached.
 */
#define PG_REGEXFAM_EREPORT_UNREACHABLE() \
	do { \
		fprintf(stderr, "regexfam oracle: unreachable engine ereport fired\n"); \
		abort(); \
	} while (0)
#define ereport(level, ...) PG_REGEXFAM_EREPORT_UNREACHABLE()
#define errcode(c) 0
#define errmsg(...) 0
#define errhint(...) 0

#endif							/* PG_REGEXFAM_POSTGRES_H */
