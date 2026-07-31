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

#define PG_RETURN_DATUM(x)	 return (x)
#define PG_RETURN_NULL()  \
	do { fcinfo->isnull = true; return (Datum) 0; } while (0)
#define PG_RETURN_POINTER(x) return PointerGetDatum(x)
#define PG_RETURN_CSTRING(x) return CStringGetDatum(x)
#define PG_RETURN_BOOL(x)	 return BoolGetDatum(x)
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

#endif							/* FMGR_H */
