/*
 *	pg_qsort.c: VERBATIM port/qsort.c instantiation of lib/sort_template.h
 *	for the jsonpath family (same shape as pg_qsort_arg.c next door).
 *
 *	oracle-integrity sweep (task #98): the family's vendored Spencer
 *	regex engine (regex/regc_nfa.c via regcomp.c) calls qsort(), which in
 *	the backend IS pg_qsort (port.h `#define qsort pg_qsort`) — but this
 *	family's shim closure had lost that define, so the verbatim bodies
 *	bound libc qsort (the spgkdtree wrong-oracle class). The build now
 *	defines qsort=jporcl_pg_qsort family-wide and this TU supplies the
 *	verbatim implementation (pg_qsort is renamed jporcl_pg_qsort by the
 *	JSONPATH_SHARED_SYMS build defines).
 */

#include "c.h"

#define ST_SORT pg_qsort
#define ST_ELEMENT_TYPE_VOID
#define ST_COMPARE_RUNTIME_POINTER
#define ST_SCOPE
#define ST_DECLARE
#define ST_DEFINE
#include "lib/sort_template.h"
