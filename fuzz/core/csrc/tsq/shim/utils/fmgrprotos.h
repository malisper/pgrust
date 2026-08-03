/*
 * SHIM utils/fmgrprotos.h — NOT PostgreSQL code. (tsq oracle family)
 * Prototypes for the vendored V1 functions (subset of the generated
 * upstream fmgrprotos.h that this family defines/calls).
 */
#ifndef PG_DIFFFUZZ_TSQ_SHIM_FMGRPROTOS_H
#define PG_DIFFFUZZ_TSQ_SHIM_FMGRPROTOS_H

#include "fmgr.h"

extern Datum tsqueryin(FunctionCallInfo fcinfo);
extern Datum tsqueryout(FunctionCallInfo fcinfo);
extern Datum tsquerysend(FunctionCallInfo fcinfo);
extern Datum tsqueryrecv(FunctionCallInfo fcinfo);
extern Datum tsquerytree(FunctionCallInfo fcinfo);
extern Datum tsquery_numnode(FunctionCallInfo fcinfo);
extern Datum tsquery_and(FunctionCallInfo fcinfo);
extern Datum tsquery_or(FunctionCallInfo fcinfo);
extern Datum tsquery_phrase(FunctionCallInfo fcinfo);
extern Datum tsquery_phrase_distance(FunctionCallInfo fcinfo);
extern Datum tsquery_not(FunctionCallInfo fcinfo);
extern Datum tsquery_lt(FunctionCallInfo fcinfo);
extern Datum tsquery_le(FunctionCallInfo fcinfo);
extern Datum tsquery_eq(FunctionCallInfo fcinfo);
extern Datum tsquery_ne(FunctionCallInfo fcinfo);
extern Datum tsquery_ge(FunctionCallInfo fcinfo);
extern Datum tsquery_gt(FunctionCallInfo fcinfo);
extern Datum tsquery_cmp(FunctionCallInfo fcinfo);
extern Datum tsq_mcontains(FunctionCallInfo fcinfo);
extern Datum tsq_mcontained(FunctionCallInfo fcinfo);
extern Datum tsquery_rewrite(FunctionCallInfo fcinfo);
extern Datum tsquery_rewrite_query(FunctionCallInfo fcinfo);

#endif
