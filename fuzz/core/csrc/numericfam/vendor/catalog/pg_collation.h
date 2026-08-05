/* shim for formatting.c: collation OIDs (values from pg_collation.dat) */
#ifndef FMTV_PG_COLLATION_H
#define FMTV_PG_COLLATION_H
#define DEFAULT_COLLATION_OID 100
#define C_COLLATION_OID 950
#define POSIX_COLLATION_OID 951

/* compat the real postgres.h/c.h would provide */
#include <errno.h>
#define TYPEALIGN(ALIGNVAL,LEN) \
	(((uintptr_t) (LEN) + ((ALIGNVAL) - 1)) & ~((uintptr_t) ((ALIGNVAL) - 1)))
#define MAXIMUM_ALIGNOF 8
#define MAXALIGN(LEN) TYPEALIGN(MAXIMUM_ALIGNOF, (LEN))
#include "utils/float.h"
#define FLOAT4_FITS_IN_INT32(num) \
	((num) >= (float4) PG_INT32_MIN && (num) < -((float4) PG_INT32_MIN))
#define FLOAT8_FITS_IN_INT32(num) \
	((num) >= (float8) PG_INT32_MIN && (num) < -((float8) PG_INT32_MIN))
extern Datum DirectFunctionCall3Coll(PGFunction func, Oid collation,
									 Datum arg1, Datum arg2, Datum arg3);
#define DirectFunctionCall3(func, a1, a2, a3) \
	DirectFunctionCall3Coll(func, InvalidOid, a1, a2, a3)

static inline bool
pg_add_s32_overflow(int32 a, int32 b, int32 *result)
{
	return __builtin_add_overflow(a, b, result);
}

static inline bool
pg_sub_s32_overflow(int32 a, int32 b, int32 *result)
{
	return __builtin_sub_overflow(a, b, result);
}

static inline bool
pg_mul_s32_overflow(int32 a, int32 b, int32 *result)
{
	return __builtin_mul_overflow(a, b, result);
}
#endif
