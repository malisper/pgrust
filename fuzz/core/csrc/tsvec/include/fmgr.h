/*
 * SHIM fmgr.h (tsvec oracle) — NOT PostgreSQL code.
 *
 * Mini-fmgr: exactly the src/include/fmgr.h surface the vendored tsvector
 * files touch, layout-per-original where code reads fields. Argument
 * passing is what real fmgr delivers to these functions; PG_DETOAST_DATUM
 * is identity because harness datums are never toasted/compressed/short
 * (documented carve in ../postgres.h).
 */
#ifndef PG_DIFFFUZZ_TSVEC_FMGR_H
#define PG_DIFFFUZZ_TSVEC_FMGR_H

typedef struct NullableDatum
{
	Datum		value;
	bool		isnull;
} NullableDatum;

#define FUNC_MAX_ARGS 8			/* shim cap; family max is 3 */

typedef struct FunctionCallInfoBaseData
{
	void	   *flinfo;
	void	   *context;		/* escontext rides here (soft-error input) */
	void	   *resultinfo;
	Oid			fncollation;
	bool		isnull;
	short		nargs;
	NullableDatum args[FUNC_MAX_ARGS];
} FunctionCallInfoBaseData;

typedef FunctionCallInfoBaseData *FunctionCallInfo;

#define PG_FUNCTION_ARGS FunctionCallInfo fcinfo

#define PG_GETARG_DATUM(n) (fcinfo->args[n].value)
#define PG_GETARG_POINTER(n) ((void *) PG_GETARG_DATUM(n))
#define PG_GETARG_CSTRING(n) ((char *) PG_GETARG_DATUM(n))
#define PG_GETARG_CHAR(n) DatumGetChar(PG_GETARG_DATUM(n))
#define PG_GETARG_INT32(n) DatumGetInt32(PG_GETARG_DATUM(n))

#define PG_DETOAST_DATUM(datum) ((struct varlena *) DatumGetPointer(datum))
#define PG_DETOAST_DATUM_COPY(datum) ((struct varlena *) DatumGetPointer(datum))
#define PG_GETARG_TEXT_PP(n) ((text *) PG_DETOAST_DATUM(PG_GETARG_DATUM(n)))
#define PG_GETARG_ARRAYTYPE_P(n) ((ArrayType *) PG_DETOAST_DATUM(PG_GETARG_DATUM(n)))

#define PG_FREE_IF_COPY(ptr, n) ((void) 0)	/* identity detoast => no copy */

#define PG_RETURN_DATUM(x) return (x)
#define PG_RETURN_POINTER(x) return PointerGetDatum(x)
#define PG_RETURN_CSTRING(x) return PointerGetDatum(x)
#define PG_RETURN_BOOL(x) return BoolGetDatum(x)
#define PG_RETURN_INT32(x) return Int32GetDatum(x)
#define PG_RETURN_BYTEA_P(x) return PointerGetDatum(x)

/* float4-by-value datum pun, per upstream src/include/postgres.h
 * Float4GetDatum/DatumGetFloat4 (USE_FLOAT4_BYVAL, the only modern config) */
static inline Datum
Float4GetDatum(float4 X)
{
	union
	{
		float4		f;
		int32		i;
	}			myunion;

	myunion.f = X;
	return Int32GetDatum(myunion.i);
}

static inline float4
DatumGetFloat4(Datum X)
{
	union
	{
		int32		i;
		float4		f;
	}			myunion;

	myunion.i = DatumGetInt32(X);
	return myunion.f;
}

#define PG_RETURN_FLOAT4(x) return Float4GetDatum(x)
#define PG_RETURN_NULL() \
	do { fcinfo->isnull = true; return (Datum) 0; } while (0)

/* DirectFunctionCall (fmgr.c contract: strict-by-construction, no null) */
extern Datum pg_tsvec_direct_call2(Datum (*func) (FunctionCallInfo),
								   Datum arg1, Datum arg2);
#define DirectFunctionCall2(func, a1, a2) pg_tsvec_direct_call2(func, a1, a2)

#endif							/* PG_DIFFFUZZ_TSVEC_FMGR_H */
