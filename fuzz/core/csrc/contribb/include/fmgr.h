/*
 * SHIM fmgr.h for the contribb_diff oracle (plumbing only): the minimal
 * V1 calling convention the vendored seg.c/cube.c bodies use. Semantics
 * match src/include/fmgr.h + src/backend/utils/fmgr/fmgr.c on 64-bit
 * (float4/float8 pass-by-value); DirectFunctionCall{1,2} mirror
 * DirectFunctionCall{1,2}Coll incl. the NULL-result elog.
 */
#ifndef PG_CB_FMGR_H
#define PG_CB_FMGR_H

#include "postgres.h"

typedef struct NullableDatum
{
	Datum		value;
	bool		isnull;
} NullableDatum;

#define PG_CB_MAX_ARGS 5

typedef struct FunctionCallInfoBaseData
{
	void	   *flinfo;
	struct Node *context;
	void	   *resultinfo;
	Oid			fncollation;
	bool		isnull;
	short		nargs;
	NullableDatum args[PG_CB_MAX_ARGS];
} FunctionCallInfoBaseData;

typedef FunctionCallInfoBaseData *FunctionCallInfo;
typedef Datum (*PGFunction) (FunctionCallInfo fcinfo);

#define PG_FUNCTION_ARGS FunctionCallInfo fcinfo
/* info-record shim: keep only the prototype the macro implies */
#define PG_FUNCTION_INFO_V1(funcname) \
	extern Datum funcname(FunctionCallInfo fcinfo)

#define PG_GETARG_DATUM(n) (fcinfo->args[n].value)
#define PG_GETARG_POINTER(n) ((void *) DatumGetPointer(PG_GETARG_DATUM(n)))
#define PG_GETARG_CSTRING(n) ((char *) DatumGetPointer(PG_GETARG_DATUM(n)))
#define PG_GETARG_INT32(n) DatumGetInt32(PG_GETARG_DATUM(n))
#define PG_GETARG_UINT16(n) DatumGetUInt16(PG_GETARG_DATUM(n))
#define PG_GETARG_FLOAT4(n) DatumGetFloat4(PG_GETARG_DATUM(n))
#define PG_GETARG_FLOAT8(n) DatumGetFloat8(PG_GETARG_DATUM(n))

#define PG_RETURN_DATUM(x) return (x)
#define PG_RETURN_POINTER(x) return PointerGetDatum(x)
#define PG_RETURN_CSTRING(x) return PointerGetDatum(x)
#define PG_RETURN_BOOL(x) return BoolGetDatum(x)
#define PG_RETURN_INT32(x) return Int32GetDatum(x)
#define PG_RETURN_FLOAT4(x) return Float4GetDatum(x)
#define PG_RETURN_FLOAT8(x) return Float8GetDatum(x)
#define PG_RETURN_BYTEA_P(x) PG_RETURN_POINTER(x)

/* never toasted in the oracle (fixtures are plain palloc'd images) */
#define PG_DETOAST_DATUM(datum) ((struct varlena *) DatumGetPointer(datum))
#define PG_FREE_IF_COPY(ptr, n) ((void) 0)

static inline Datum
DirectFunctionCall1(PGFunction func, Datum arg1)
{
	FunctionCallInfoBaseData fcinfo;
	Datum		result;

	memset(&fcinfo, 0, sizeof(fcinfo));
	fcinfo.nargs = 1;
	fcinfo.args[0].value = arg1;
	result = (*func) (&fcinfo);
	if (fcinfo.isnull)
		elog(ERROR, "function returned NULL");
	return result;
}

static inline Datum
DirectFunctionCall2(PGFunction func, Datum arg1, Datum arg2)
{
	FunctionCallInfoBaseData fcinfo;
	Datum		result;

	memset(&fcinfo, 0, sizeof(fcinfo));
	fcinfo.nargs = 2;
	fcinfo.args[0].value = arg1;
	fcinfo.args[1].value = arg2;
	result = (*func) (&fcinfo);
	if (fcinfo.isnull)
		elog(ERROR, "function returned NULL");
	return result;
}

#endif							/* PG_CB_FMGR_H */
