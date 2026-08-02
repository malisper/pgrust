/*
 * pg_regexfam_qsort.c: VERBATIM port/qsort.c instantiation of
 * ../sort_template.h for the regexfam oracle, exported as
 * regexfam_pg_qsort (family prefix — see the pg_regexfam_ isolation
 * note in build.rs).
 *
 * oracle-integrity sweep (task #98): the verbatim Spencer engine
 * (regc_nfa.c via regcomp.c) calls qsort(), which in the backend IS
 * pg_qsort — port.h line 478 `#define qsort pg_qsort` is part of every
 * backend TU's header closure, and this family's shim closure had lost
 * it, so the verbatim bodies bound LIBC qsort (the spgkdtree
 * wrong-oracle class). build.rs now defines qsort=regexfam_pg_qsort
 * family-wide; this TU supplies the verbatim implementation.
 *
 * Self-contained prologue (kd_pg_qsort pattern): only the four support
 * definitions sort_template.h needs, so the family shim headers stay
 * out of this TU.
 */

#include <stdint.h>
#include <stddef.h>
#include <string.h>

typedef uint8_t uint8;
#define pg_noinline __attribute__((noinline))
#define Min(x, y)		((x) < (y) ? (x) : (y))
#define CppConcat(x, y) x##y

#define ST_SORT regexfam_pg_qsort
#define ST_ELEMENT_TYPE_VOID
#define ST_COMPARE_RUNTIME_POINTER
#define ST_SCOPE
#define ST_DECLARE
#define ST_DEFINE
#include "../sort_template.h"
