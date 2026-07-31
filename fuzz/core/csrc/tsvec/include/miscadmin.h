/* SHIM miscadmin.h (tsvec oracle) — NOT PostgreSQL code.
 * Stack-depth/interrupt checks are session machinery; recursion depth is
 * bounded by the harness's tsquery size cap (<= 32 items). */
#ifndef PG_DIFFFUZZ_TSVEC_MISCADMIN_H
#define PG_DIFFFUZZ_TSVEC_MISCADMIN_H
static inline void check_stack_depth(void) {}
#define CHECK_FOR_INTERRUPTS() ((void) 0)
#endif
