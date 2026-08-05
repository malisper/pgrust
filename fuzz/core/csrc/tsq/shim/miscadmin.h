/*
 * SHIM miscadmin.h — NOT PostgreSQL code. (tsq oracle family, p1-laneaf)
 *
 * check_stack_depth() and CHECK_FOR_INTERRUPTS() -> no-ops. The fuzz
 * DRIVER caps input length (and thereby recursion depth) far below any
 * real stack limit on both sides; C's stack-depth ereport (54001) is
 * therefore unreachable, mirroring the Rust port which recurses natively
 * under the same cap. Documented seam class: stack-depth-limit.
 */
#ifndef PG_DIFFFUZZ_TSQ_SHIM_MISCADMIN_H
#define PG_DIFFFUZZ_TSQ_SHIM_MISCADMIN_H

static inline void check_stack_depth(void) {}
#define CHECK_FOR_INTERRUPTS() ((void) 0)

#endif
