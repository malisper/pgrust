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
/* jsonbops_diff additions (p1-lanev): ops/mutate/getfield fmgr entries */
extern Datum jsonb_in(PG_FUNCTION_ARGS);
extern Datum jsonb_object_field(PG_FUNCTION_ARGS);
extern Datum jsonb_object_field_text(PG_FUNCTION_ARGS);
extern Datum jsonb_array_element(PG_FUNCTION_ARGS);
extern Datum jsonb_array_element_text(PG_FUNCTION_ARGS);
extern Datum jsonb_extract_path(PG_FUNCTION_ARGS);
extern Datum jsonb_extract_path_text(PG_FUNCTION_ARGS);
extern Datum jsonb_exists(PG_FUNCTION_ARGS);
extern Datum jsonb_exists_any(PG_FUNCTION_ARGS);
extern Datum jsonb_exists_all(PG_FUNCTION_ARGS);
extern Datum jsonb_contains(PG_FUNCTION_ARGS);
extern Datum jsonb_contained(PG_FUNCTION_ARGS);
extern Datum jsonb_eq(PG_FUNCTION_ARGS);
extern Datum jsonb_ne(PG_FUNCTION_ARGS);
extern Datum jsonb_lt(PG_FUNCTION_ARGS);
extern Datum jsonb_gt(PG_FUNCTION_ARGS);
extern Datum jsonb_le(PG_FUNCTION_ARGS);
extern Datum jsonb_ge(PG_FUNCTION_ARGS);
extern Datum jsonb_cmp(PG_FUNCTION_ARGS);
extern Datum jsonb_hash(PG_FUNCTION_ARGS);
extern Datum jsonb_hash_extended(PG_FUNCTION_ARGS);
extern Datum jsonb_concat(PG_FUNCTION_ARGS);
extern Datum jsonb_delete(PG_FUNCTION_ARGS);
extern Datum jsonb_delete_idx(PG_FUNCTION_ARGS);
extern Datum jsonb_delete_array(PG_FUNCTION_ARGS);
extern Datum jsonb_delete_path(PG_FUNCTION_ARGS);
extern Datum jsonb_set(PG_FUNCTION_ARGS);
extern Datum jsonb_insert(PG_FUNCTION_ARGS);
extern Datum jsonb_object(PG_FUNCTION_ARGS);
extern Datum jsonb_object_two_arg(PG_FUNCTION_ARGS);
/* src/common/string.c strtoint (verbatim in jsonbfam/string_c.inc) */
extern int	strtoint(const char *str, char **endptr, int base);
/* varlena.c varstr_cmp + check_collation_set (jsonbfam/varlena_cmp_c.inc) */
extern int	varstr_cmp(const char *arg1, int len1, const char *arg2, int len2,
					   Oid collid);
