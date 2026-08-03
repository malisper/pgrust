/* SHIM miscadmin.h (tsvec oracle) — NOT PostgreSQL code.
 * Stack-depth/interrupt checks are session machinery; recursion depth is
 * bounded by the harness's tsquery generator cap: MAX_ITEMS = 96
 * (fuzz/core/src/tsq_gen.rs — raised from 32 for the pg_qsort med3 band,
 * which is what left this comment stale at "<= 32" until the shim-contract
 * census, task #129/#131). 96 levels is orders of magnitude under PG's
 * effective max_stack_depth default (2048 kB; vendor guc.c:1613-1635), so
 * the no-op stands in for a guard neither side can reach. If MAX_ITEMS
 * grows past a few thousand, revisit (the jsonfam shim is the byte-guard
 * pattern to copy). */
#ifndef PG_DIFFFUZZ_TSVEC_MISCADMIN_H
#define PG_DIFFFUZZ_TSVEC_MISCADMIN_H
static inline void check_stack_depth(void) {}
#define CHECK_FOR_INTERRUPTS() ((void) 0)
#endif
