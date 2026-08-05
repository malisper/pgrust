/*
 * pg_regexfam_vendor_qsort.c: VERBATIM port/qsort.c instantiation of
 * ../sort_template.h for the regex_diff ENGINE oracle (the pristine-name
 * Spencer copy under vendor/), exported as rxocore_pg_qsort.
 *
 * Same task-#98 rationale as pg_regexfam_qsort.c (sibling file): the
 * verbatim engine (regc_nfa.c via regcomp.c) calls qsort(), which in the
 * backend IS pg_qsort (port.h line 478 `#define qsort pg_qsort` is part
 * of every backend TU's header closure). build.rs defines
 * qsort=rxocore_pg_qsort for the regexcorefam build; this TU supplies the
 * verbatim implementation under a name that cannot collide with the
 * wrapper family's regexfam_pg_qsort instantiation.
 *
 * Self-contained prologue (kd_pg_qsort pattern): only the four support
 * definitions sort_template.h needs, so no shim headers enter this TU.
 */

#include <stdint.h>
#include <stddef.h>
#include <string.h>

typedef uint8_t uint8;
#define pg_noinline __attribute__((noinline))
#define Min(x, y)		((x) < (y) ? (x) : (y))
#define CppConcat(x, y) x##y

#define ST_SORT rxocore_pg_qsort
#define ST_ELEMENT_TYPE_VOID
#define ST_COMPARE_RUNTIME_POINTER
#define ST_SCOPE
#define ST_DECLARE
#define ST_DEFINE
#include "../sort_template.h"
