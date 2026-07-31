/* SHIM utils/fmgrprotos.h — fmgr-callable protos used by pasted segments. */
#ifndef PG_JSONBFAM_SHIM_FMGRPROTOS_H
#define PG_JSONBFAM_SHIM_FMGRPROTOS_H
#include "fmgr.h"
extern Datum numeric_in(PG_FUNCTION_ARGS);
extern Datum numeric_out(PG_FUNCTION_ARGS);
extern Datum numeric_int2(PG_FUNCTION_ARGS);
extern Datum numeric_int4(PG_FUNCTION_ARGS);
extern Datum numeric_int8(PG_FUNCTION_ARGS);
extern Datum numeric_float4(PG_FUNCTION_ARGS);
extern Datum numeric_float8(PG_FUNCTION_ARGS);
#endif
extern Datum hash_numeric(PG_FUNCTION_ARGS);
extern Datum hash_numeric_extended(PG_FUNCTION_ARGS);
extern Datum hashchar(PG_FUNCTION_ARGS);
extern Datum hashcharextended(PG_FUNCTION_ARGS);
extern Datum timestamp_out(PG_FUNCTION_ARGS);
extern Datum timestamptz_out(PG_FUNCTION_ARGS);
extern Datum date_out(PG_FUNCTION_ARGS);
extern Datum time_out(PG_FUNCTION_ARGS);
extern Datum timetz_out(PG_FUNCTION_ARGS);
extern Datum numeric_eq(PG_FUNCTION_ARGS);
extern Datum numeric_cmp(PG_FUNCTION_ARGS);
