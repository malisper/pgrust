/*
 * SHIM fmgr.h for the jsonpath_diff oracle — NOT PostgreSQL code.
 *
 * Mini-fmgr: exactly the call plumbing the vendored TUs use, with struct
 * shapes VERBATIM from src/include/fmgr.h + postgres.h @ 62d6c7d3df
 * (PostgreSQL 18.3): NullableDatum, FunctionCallInfoBaseData, LOCAL_FCINFO,
 * the PG_GETARG/PG_RETURN macro layer, and DirectFunctionCallN implemented
 * as thin inline dispatch (the real fmgr.c bodies add collation/flinfo
 * bookkeeping the vendored callees never read; the "function returned NULL"
 * elog matches fmgr.c behavior).
 */
#ifndef FMGR_H
#define FMGR_H

#include "postgres.h"

typedef struct FmgrInfo FmgrInfo;
typedef struct FunctionCallInfoBaseData *FunctionCallInfo;
typedef Datum (*PGFunction) (FunctionCallInfo fcinfo);

struct FmgrInfo
{
	PGFunction	fn_addr;
	Oid			fn_oid;
	short		fn_nargs;
	bool		fn_strict;
	bool		fn_retset;
	unsigned char fn_stats;
	void	   *fn_extra;
	void	   *fn_mcxt;
	Node	   *fn_expr;
};

typedef struct NullableDatum
{
	Datum		value;
	bool		isnull;
} NullableDatum;

typedef struct FunctionCallInfoBaseData
{
	FmgrInfo   *flinfo;
	Node	   *context;
	Node	   *resultinfo;
	Oid			fncollation;
	bool		isnull;
	short		nargs;
	NullableDatum args[FLEXIBLE_ARRAY_MEMBER];
} FunctionCallInfoBaseData;

#define SizeForFunctionCallInfo(nargs) \
	(offsetof(FunctionCallInfoBaseData, args) + \
	 sizeof(NullableDatum) * (nargs))

#define LOCAL_FCINFO(name, nargs) \
	union \
	{ \
		FunctionCallInfoBaseData fcinfo; \
		char fcinfo_data[SizeForFunctionCallInfo(nargs)]; \
	} name##data; \
	FunctionCallInfo name = &name##data.fcinfo

#define PG_FUNCTION_ARGS	FunctionCallInfo fcinfo

#define PG_NARGS() (fcinfo->nargs)
#define PG_ARGISNULL(n)  (fcinfo->args[n].isnull)
#define PG_GETARG_DATUM(n)	 (fcinfo->args[n].value)
#define PG_GETARG_INT32(n)	 DatumGetInt32(PG_GETARG_DATUM(n))
#define PG_GETARG_OID(n)	 ((Oid) PG_GETARG_DATUM(n))
#define PG_GETARG_POINTER(n) DatumGetPointer(PG_GETARG_DATUM(n))
#define PG_GETARG_CSTRING(n) DatumGetCString(PG_GETARG_DATUM(n))
#define PG_GETARG_BOOL(n)	 DatumGetBool(PG_GETARG_DATUM(n))

/* the shim PG_DETOAST_DATUM never copies (all fuzz inputs are plain
 * 4B-header varlenas), so FREE_IF_COPY is a documented no-op */
#define PG_FREE_IF_COPY(ptr,n) ((void) 0)

#define PG_RETURN_DATUM(x)	 return (x)
#define PG_RETURN_NULL()  \
	do { fcinfo->isnull = true; return (Datum) 0; } while (0)
#define PG_RETURN_POINTER(x) return PointerGetDatum(x)
#define PG_RETURN_CSTRING(x) return CStringGetDatum(x)
#define PG_RETURN_BOOL(x)	 return BoolGetDatum(x)
#define PG_RETURN_INT32(x)	 return Int32GetDatum(x)
#define PG_RETURN_INT64(x)	 return Int64GetDatum(x)
#define PG_RETURN_INT16(x)	 return Int32GetDatum((int32) (x))
#define PG_RETURN_FLOAT8(x)  return Float8GetDatum(x)
#define PG_GETARG_INT16(n)	 ((int16) PG_GETARG_DATUM(n))
#define PG_GETARG_INT64(n)	 DatumGetInt64(PG_GETARG_DATUM(n))
#define PG_GETARG_FLOAT4(n)	 DatumGetFloat4(PG_GETARG_DATUM(n))
#define PG_GETARG_FLOAT8(n)	 DatumGetFloat8(PG_GETARG_DATUM(n))
#define PG_GETARG_ARRAYTYPE_P(n) ((ArrayType *) PG_GETARG_POINTER(n))
#define PG_RETURN_BYTEA_P(x) PG_RETURN_POINTER(x)
#define PG_RETURN_TEXT_P(x)  PG_RETURN_POINTER(x)

/* thin DirectFunctionCallN dispatch (see header comment) */
static inline Datum
pg_jsonpath_direct_call(PGFunction func, Node *context, int nargs,
						Datum a0, Datum a1, Datum a2)
{
	LOCAL_FCINFO(fcinfo, 3);
	Datum		result;

	memset(fcinfo, 0, SizeForFunctionCallInfo(3));
	fcinfo->context = context;
	fcinfo->nargs = (short) nargs;
	fcinfo->args[0].value = a0;
	fcinfo->args[1].value = a1;
	fcinfo->args[2].value = a2;

	result = (*func) (fcinfo);

	/* Check for null result, since caller is clearly not expecting one */
	if (fcinfo->isnull)
		elog(ERROR, "function returned NULL");

	return result;
}

#define DirectFunctionCall1(func, a0) \
	pg_jsonpath_direct_call(func, NULL, 1, (a0), (Datum) 0, (Datum) 0)
#define DirectFunctionCall2(func, a0, a1) \
	pg_jsonpath_direct_call(func, NULL, 2, (a0), (a1), (Datum) 0)
#define DirectFunctionCall3(func, a0, a1, a2) \
	pg_jsonpath_direct_call(func, NULL, 3, (a0), (a1), (a2))

/*
 * DirectInputFunctionCallSafe: semantics VERBATIM-equivalent to fmgr.c @
 * 18.3 (the flinfo-less direct form; str is never NULL at the vendored call
 * sites). Soft errors are detected with SOFT_ERROR_OCCURRED, so the caller
 * sees exactly the real protocol.
 */
#include "nodes/miscnodes.h"

static inline bool
DirectInputFunctionCallSafe(PGFunction func, char *str,
							Oid typioparam, int32 typmod,
							struct Node *escontext,
							Datum *result)
{
	LOCAL_FCINFO(fcinfo, 3);

	memset(fcinfo, 0, SizeForFunctionCallInfo(3));
	fcinfo->nargs = 3;
	fcinfo->context = (Node *) escontext;
	fcinfo->args[0].value = CStringGetDatum(str);
	fcinfo->args[0].isnull = false;
	fcinfo->args[1].value = (Datum) typioparam;
	fcinfo->args[1].isnull = false;
	fcinfo->args[2].value = (Datum) (int64) typmod;
	fcinfo->args[2].isnull = false;

	*result = (*func) (fcinfo);

	if (SOFT_ERROR_OCCURRED(escontext))
		return false;

	if (fcinfo->isnull)
		elog(ERROR, "input function returned NULL");

	return true;
}

#endif							/* FMGR_H */
