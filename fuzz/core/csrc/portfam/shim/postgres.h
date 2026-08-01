/*
 * SHIM postgres.h — NOT PostgreSQL code. See shim/c.h for provenance.
 * Backend-flavored shim for the vendored bufmask.c and pg_crc.c:
 *   - elog(ERROR, ...) capture: sets the shared pg_diff_errcode flag and
 *     returns from the (void) containing function — bufmask.c's only elog
 *     site is mask_unused_space's invalid-page check. Plumbing only: the
 *     error VERDICT is the comparison plane, message text is out of scope.
 *   - Datum + the few fmgr macros pg_crc.c's two SQL wrappers expand to
 *     (the hashenc shim pattern).
 */
#ifndef PG_DIFFFUZZ_PORTFAM_SHIM_POSTGRES_H
#define PG_DIFFFUZZ_PORTFAM_SHIM_POSTGRES_H

#include "c.h"

/* Own TLS flag (NOT the main oracle lib's pg_diff_errcode) so this build
 * has no cross-archive link-order dependency. Defined in pg_portfam_io.c. */
extern _Thread_local int pg_portfam_errcode;

#define ERROR 21				/* elog.h value */
#define elog(level, ...) \
	do { pg_portfam_errcode = 1; return; } while (0)

typedef uintptr_t Datum;

/* Single-argument fmgr call shim: arg0 = the detoasted bytea pointer. */
typedef struct pg_shim_FunctionCallInfoBaseData
{
	void	   *arg0;
} pg_shim_FunctionCallInfoBaseData, *FunctionCallInfo;

#define PG_FUNCTION_ARGS FunctionCallInfo fcinfo
#define PG_GETARG_BYTEA_PP(n) ((bytea *) fcinfo->arg0)
#define PG_RETURN_INT64(x) return (Datum) (int64) (x)

#endif
