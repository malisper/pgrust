/* SHIM utils/json.h — escape_json* implemented verbatim in the driver TU
 * (bodies from src/backend/utils/adt/json.c @ REL_18_6 escape family,
 * json_escape_c.inc; decls match src/include/utils/json.h @ REL_18_6). */
#ifndef PG_JSONBFAM_SHIM_JSON_H
#define PG_JSONBFAM_SHIM_JSON_H
#include "postgres.h"
#include "lib/stringinfo.h"
extern void escape_json(StringInfo buf, const char *str);
extern void escape_json_with_len(StringInfo buf, const char *str, int len);
extern void JsonEncodeDateTime(char *buf, Datum value, Oid typid, const int *tzp);
#endif
