/* SHIM catalog/pg_type.h — type OIDs referenced by pasted segments. */
#ifndef PG_JSONBFAM_SHIM_PG_TYPE_H
#define PG_JSONBFAM_SHIM_PG_TYPE_H
#define BOOLOID 16
#define TEXTOID 25
#define INT2OID 21
#define INT4OID 23
#define INT8OID 20
#define FLOAT4OID 700
#define FLOAT8OID 701
#define NUMERICOID 1700
#define JSONOID 114
#define JSONBOID 3802
#endif
#ifndef PG_JSONBFAM_SHIM_PG_TYPE_H2
#define PG_JSONBFAM_SHIM_PG_TYPE_H2
#define DATEOID 1082
#define TIMEOID 1083
#define TIMETZOID 1266
#define TIMESTAMPOID 1114
#define TIMESTAMPTZOID 1184
#endif
#ifndef PG_JSONBFAM_SHIM_PG_TYPE_H3
#define PG_JSONBFAM_SHIM_PG_TYPE_H3
/* jsonbops_diff additions: oids named by the verbatim
 * deconstruct_array_builtin switch (only TEXTOID is ever passed here). */
#define CHAROID 18
#define CSTRINGOID 2275
#define OIDOID 26
#define TIDOID 27
/* c.h: float8 pass-by-value on 64-bit Datum */
#define FLOAT8PASSBYVAL true
/* storage/itemptr.h ItemPointerData: 3 uint16 fields, 6 bytes (only
 * sizeof() is taken, in the TIDOID arm) */
typedef struct ItemPointerData
{
	uint16		ip_blkid_hi;
	uint16		ip_blkid_lo;
	uint16		ip_posid;
} ItemPointerData;
#endif
