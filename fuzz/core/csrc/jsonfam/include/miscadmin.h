/*
 * jsonfam shim miscadmin.h — check_stack_depth() for the vendored parser.
 *
 * The backend guard errors with ERRCODE_STATEMENT_TOO_COMPLEX when the C
 * stack approaches max_stack_depth. The oracle mirrors the guard's ROLE with
 * a recursion counter pinned at a depth (100k) far above anything the fuzz
 * driver can reach (input length is capped by the driver), so the guard is
 * exercised on neither side of the differential; the Rust side's
 * stack_depth::check_stack_depth lines are the stack_depth crate's, not this
 * crate's. Plumbing shim, not logic.
 */
#ifndef PG_JSONFAM_MISCADMIN_H
#define PG_JSONFAM_MISCADMIN_H

#include "postgres.h"

extern _Thread_local int pg_jsonfam_stack_depth;

static inline void
check_stack_depth(void)
{
	if (++pg_jsonfam_stack_depth > 100000)
		pg_jsonfam_error_fire(ERRCODE_STATEMENT_TOO_COMPLEX);
}

#endif
