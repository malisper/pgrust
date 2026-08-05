/* SHIM header for the jsonpath_diff oracle - NOT PostgreSQL code (plumbing only, never logic). */
#ifndef PG_TYPE_H
#define PG_TYPE_H
/* OID values VERBATIM from catalog/pg_type_d.h @ 18.3 */
#define BOOLOID 16
#define TEXTOID 25
#define OIDOID 26
#define INT4OID 23
#define DATEOID 1082
#define TIMEOID 1083
#define TIMESTAMPOID 1114
#define TIMESTAMPTZOID 1184
#define TIMETZOID 1266
#define NUMERICOID 1700
#define JSONBOID 3802
#define JSONPATHOID 4072
#define CSTRINGOID 2275
#define INT2OID 21
#define INT8OID 20
#define FLOAT4OID 700
#define FLOAT8OID 701
#define VARCHAROID 1043
#define JSONOID 114
#endif
