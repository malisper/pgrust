/*
 * SHIM c.h — NOT PostgreSQL code. The verbatim csrc/tsq/qsort.c (and its
 * verbatim include/lib/sort_template.h) include "c.h" for the base
 * environment; the tsq shim postgres.h already provides that environment
 * (fixed-width typedefs, Assert compiled out, likely/unlikely). Plumbing
 * only, never logic.
 */
#ifndef PG_DIFFFUZZ_TSQ_SHIM_C_H
#define PG_DIFFFUZZ_TSQ_SHIM_C_H
#include "postgres.h"

/* c.h:364 upstream: token concatenation, exactly as defined there */
#define CppConcat(x, y) x##y

/* c.h:281 upstream (GCC/Clang arm): force out-of-line, same attribute */
#ifndef pg_noinline
#define pg_noinline __attribute__((noinline))
#endif

#endif
