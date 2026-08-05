/*
 * SHIM fmgr.h — NOT PostgreSQL code. (tsq oracle family, p1-laneaf)
 *
 * Just enough fmgr machinery for the vendored V1 functions (Datum
 * fn(PG_FUNCTION_ARGS)) to compile VERBATIM and be callable from the
 * pg_diff_* driver entries. Faithful reductions of src/include/fmgr.h:
 *   - FunctionCallInfoBaseData reduced to the fields the vendored files
 *     touch (context for escontext, isnull, nargs, args[]); args[] is a
 *     fixed FUNC_MAX_ARGS_TSQ=4 (max arity here is 3).
 *   - PG_DETOAST_DATUM -> identity cast: the driver only ever passes flat,
 *     untoasted varlenas (documented in varatt.h shim).
 *   - PG_DETOAST_DATUM_COPY -> arena copy (upstream semantics: the callee
 *     may scribble on / return the copy).
 *   - PG_FREE_IF_COPY -> no-op: all copies live in the per-entry arena.
 *   - DirectFunctionCall1/2/3: literal reduction of fmgr.c's (strict
 *     null-free path; elog on NULL result becomes abort — unreachable for
 *     these functions, which never return NULL).
 */
#ifndef PG_DIFFFUZZ_TSQ_SHIM_FMGR_H
#define PG_DIFFFUZZ_TSQ_SHIM_FMGR_H

#include "postgres.h"
#include "varatt.h"

typedef struct NullableDatum
{
	Datum		value;
	bool		isnull;
} NullableDatum;

#define FUNC_MAX_ARGS_TSQ 4

typedef struct FunctionCallInfoBaseData
{
	void	   *flinfo;
	struct Node *context;
	void	   *resultinfo;
	Oid			fncollation;
	bool		isnull;
	short		nargs;
	NullableDatum args[FUNC_MAX_ARGS_TSQ];
}			FunctionCallInfoBaseData;

typedef FunctionCallInfoBaseData *FunctionCallInfo;

typedef Datum (*PGFunction) (FunctionCallInfo fcinfo);

#define PG_FUNCTION_ARGS FunctionCallInfo fcinfo

#define PG_GETARG_DATUM(n) (fcinfo->args[n].value)
#define PG_GETARG_INT32(n) DatumGetInt32(PG_GETARG_DATUM(n))
#define PG_GETARG_POINTER(n) DatumGetPointer(PG_GETARG_DATUM(n))
#define PG_GETARG_CSTRING(n) DatumGetCString(PG_GETARG_DATUM(n))

extern struct varlena *pg_tsq_detoast_copy(struct varlena *v); /* pg_tsq_shim.c */

#define PG_DETOAST_DATUM(datum) ((struct varlena *) DatumGetPointer(datum))
#define PG_DETOAST_DATUM_COPY(datum) \
	pg_tsq_detoast_copy((struct varlena *) DatumGetPointer(datum))

#define PG_GETARG_TEXT_PP(n) ((text *) PG_DETOAST_DATUM(PG_GETARG_DATUM(n)))
#define PG_FREE_IF_COPY(ptr, n) ((void) 0)

#define PG_RETURN_DATUM(x) return (x)
#define PG_RETURN_INT32(x) return Int32GetDatum(x)
#define PG_RETURN_BOOL(x) return BoolGetDatum(x)
#define PG_RETURN_POINTER(x) return PointerGetDatum(x)
#define PG_RETURN_CSTRING(x) return CStringGetDatum(x)
#define PG_RETURN_TEXT_P(x) return PointerGetDatum(x)
#define PG_RETURN_BYTEA_P(x) return PointerGetDatum(x)

static inline Datum
pg_tsq_direct_call(PGFunction func, int nargs,
				   Datum a0, Datum a1, Datum a2)
{
	FunctionCallInfoBaseData fcdata;
	Datum		result;

	memset(&fcdata, 0, sizeof(fcdata));
	fcdata.nargs = (short) nargs;
	fcdata.args[0].value = a0;
	fcdata.args[1].value = a1;
	fcdata.args[2].value = a2;
	result = func(&fcdata);
	if (fcdata.isnull)
		abort();				/* fmgr.c elogs; unreachable for this family */
	return result;
}

#define DirectFunctionCall1(func, a0) pg_tsq_direct_call(func, 1, (a0), 0, 0)
#define DirectFunctionCall2(func, a0, a1) pg_tsq_direct_call(func, 2, (a0), (a1), 0)
#define DirectFunctionCall3(func, a0, a1, a2) pg_tsq_direct_call(func, 3, (a0), (a1), (a2))

#endif							/* PG_DIFFFUZZ_TSQ_SHIM_FMGR_H */
