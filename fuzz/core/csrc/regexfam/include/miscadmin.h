/*
 * SHIM HEADER (regexp_diff oracle, p1-laneag) — NOT vendored PostgreSQL.
 *
 * regcustom.h includes miscadmin.h for CHECK_FOR_INTERRUPTS (INTERRUPT
 * macro) and stack_is_too_deep (rstacktoodeep).  Shims documented in the
 * regexfam postgres.h header: interrupts are pinned off (CANCEL_REQUESTED
 * false — task carve) and the stack-depth check is pinned false (pattern
 * length capped at 128 bytes by the driver).
 */
#ifndef PG_REGEXFAM_MISCADMIN_H
#define PG_REGEXFAM_MISCADMIN_H

#include "postgres.h"

#define CHECK_FOR_INTERRUPTS() ((void) 0)

static inline bool
stack_is_too_deep(void)
{
	return false;
}

#endif							/* PG_REGEXFAM_MISCADMIN_H */
