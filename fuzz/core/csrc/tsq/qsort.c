/*
 *	qsort.c: standard quicksort algorithm
 *
 * VERBATIM structure from src/port/qsort.c @ 62d6c7d3df (PostgreSQL 18.3):
 * the whole implementation is the ST_SORT instantiation of the verbatim
 * vendored include/lib/sort_template.h (shasum-matched), so tie decisions
 * (med3 pivot selection, presorted check, partition swaps) are exactly
 * real PostgreSQL's pg_qsort — NOT libc qsort, whose different tie order
 * is scalar-visible through QTNSort (p1-laneae found three such bugs in
 * the tsvector family; port.h:478 maps qsort -> pg_qsort in every real
 * backend TU).
 *
 * SHIM (plumbing only, documented): ST_SORT is spelled tsq_pg_qsort
 * instead of pg_qsort so this TU cannot duplicate-symbol against another
 * lane's verbatim pg_qsort instantiation (csrc/tsvec/qsort.c on
 * proofs/p1-laneae) when both land in the shared decoder_fuzz link. The
 * shim/postgres.h qsort macro routes the vendored tsquery TUs here, same
 * as port.h routes real backend code to pg_qsort. pg_qsort_strcmp is not
 * carried (no vendored tsquery TU uses it).
 */

#include "c.h"

#define ST_SORT tsq_pg_qsort
#define ST_ELEMENT_TYPE_VOID
#define ST_COMPARE_RUNTIME_POINTER
#define ST_SCOPE
#define ST_DECLARE
#define ST_DEFINE
#include "lib/sort_template.h"
