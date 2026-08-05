/* SHIM header for the jsonpathexec_diff oracle - NOT PostgreSQL code (plumbing only, never logic). */
#ifndef FMGRPROTOS_H
#define FMGRPROTOS_H
#include "fmgr.h"
/* vendored verbatim in pg_numeric_min.c */
extern Datum numerictypmodin(FunctionCallInfo fcinfo);
extern Datum numeric_in(FunctionCallInfo fcinfo);
extern Datum numeric_out(FunctionCallInfo fcinfo);
extern Datum numeric_uminus(FunctionCallInfo fcinfo);
extern Datum numeric_abs(FunctionCallInfo fcinfo);
extern Datum numeric_ceil(FunctionCallInfo fcinfo);
extern Datum numeric_floor(FunctionCallInfo fcinfo);
extern Datum numeric_trunc(FunctionCallInfo fcinfo);
extern Datum numeric_cmp(FunctionCallInfo fcinfo);
extern Datum numeric_eq(FunctionCallInfo fcinfo);
extern Datum int2_numeric(FunctionCallInfo fcinfo);
extern Datum int4_numeric(FunctionCallInfo fcinfo);
extern Datum int8_numeric(FunctionCallInfo fcinfo);
extern Datum float4_numeric(FunctionCallInfo fcinfo);
extern Datum float8_numeric(FunctionCallInfo fcinfo);
extern Datum numeric_float8(FunctionCallInfo fcinfo);
/* vendored verbatim in pg_jsonb_min.c (int.c / int8.c) */
extern Datum int4in(FunctionCallInfo fcinfo);
extern Datum int8in(FunctionCallInfo fcinfo);
/* hash opclass entries: unreachable from jsonpath execution — LOUD ABORT
 * stubs in pg_jsonpath_exec_env.c (see include/common/hashfn.h) */
extern Datum hash_numeric(FunctionCallInfo fcinfo);
extern Datum hash_numeric_extended(FunctionCallInfo fcinfo);
extern Datum hashchar(FunctionCallInfo fcinfo);
extern Datum hashcharextended(FunctionCallInfo fcinfo);
/* datetime family: DRIVER-LEVEL CARVE — LOUD ABORT sentinel stubs in
 * pg_jsonpath_exec_env.c (see include/utils/datetime.h header comment) */
extern Datum date_cmp(FunctionCallInfo fcinfo);
extern Datum date_timestamp(FunctionCallInfo fcinfo);
extern Datum date_timestamptz(FunctionCallInfo fcinfo);
extern Datum time_cmp(FunctionCallInfo fcinfo);
extern Datum time_timetz(FunctionCallInfo fcinfo);
extern Datum time_tz(FunctionCallInfo fcinfo);
extern Datum timetz_cmp(FunctionCallInfo fcinfo);
extern Datum timetz_time(FunctionCallInfo fcinfo);
/* unreachable from the driver (executor JSONOID conversion): loud stub */
extern Datum jsonb_in(FunctionCallInfo fcinfo);
extern char *format_type_be(Oid type_oid);
#endif
