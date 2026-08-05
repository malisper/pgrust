/* SHIM utils/json.h — escape_json* implemented verbatim in the driver TU
 * (bodies from src/common/jsonapi.c REL_18 escape family). */
#ifndef PG_JSONBFAM_SHIM_JSON_H
#define PG_JSONBFAM_SHIM_JSON_H
#include "postgres.h"
#include "lib/stringinfo.h"
extern void escape_json(StringInfo buf, const char *str);
extern void escape_json_with_len(StringInfo buf, const char *str, int len);
extern void JsonEncodeDateTime(char *buf, Datum value, Oid typid, const int *tzp);
#endif
