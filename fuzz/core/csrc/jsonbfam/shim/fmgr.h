/*
 * SHIM fmgr.h for the jsonbfam oracle — NOT PostgreSQL code (struct shapes
 * and macro semantics follow the real src/include/fmgr.h @ 62d6c7d3df so
 * the vendored fmgr bodies compile and run VERBATIM; only the machinery a
 * standalone oracle cannot have — fmgr lookup, toast — is simplified:
 * pg_detoast_datum handles 4-byte-header and 1-byte-short varlenas only
 * (this oracle never produces toasted/compressed values)).
 */
#ifndef PG_JSONBFAM_SHIM_FMGR_H
#define PG_JSONBFAM_SHIM_FMGR_H

#include "postgres.h"

typedef struct FunctionCallInfoBaseData *FunctionCallInfo;
typedef Datum (*PGFunction) (FunctionCallInfo fcinfo);

typedef struct FmgrInfo
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
} FmgrInfo;

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
		char		fcinfo_data[SizeForFunctionCallInfo(nargs)]; \
	}			name##data; \
	FunctionCallInfo name = &name##data.fcinfo

#define InitFunctionCallInfoData(Fcinfo, Flinfo, Nargs, Collation, Context, Resultinfo) \
	do { \
		(Fcinfo).flinfo = (Flinfo); \
		(Fcinfo).context = (Context); \
		(Fcinfo).resultinfo = (Resultinfo); \
		(Fcinfo).fncollation = (Collation); \
		(Fcinfo).isnull = false; \
		(Fcinfo).nargs = (Nargs); \
	} while (0)

#define PG_FUNCTION_ARGS FunctionCallInfo fcinfo

#define PG_NARGS() (fcinfo->nargs)
#define PG_ARGISNULL(n) (fcinfo->args[n].isnull)

/* detoast: 4-byte-header passthrough; 1-byte short -> palloc'd 4-byte copy */
extern struct varlena *pg_detoast_datum(struct varlena *datum);
extern struct varlena *pg_detoast_datum_copy(struct varlena *datum);
extern struct varlena *pg_detoast_datum_packed(struct varlena *datum);

#define PG_DETOAST_DATUM(datum) \
	pg_detoast_datum((struct varlena *) DatumGetPointer(datum))
#define PG_DETOAST_DATUM_COPY(datum) \
	pg_detoast_datum_copy((struct varlena *) DatumGetPointer(datum))
#define PG_DETOAST_DATUM_PACKED(datum) \
	pg_detoast_datum_packed((struct varlena *) DatumGetPointer(datum))

#define PG_GETARG_DATUM(n) (fcinfo->args[n].value)
#define PG_GETARG_INT32(n) DatumGetInt32(PG_GETARG_DATUM(n))
#define PG_GETARG_UINT32(n) DatumGetUInt32(PG_GETARG_DATUM(n))
#define PG_GETARG_INT16(n) DatumGetInt16(PG_GETARG_DATUM(n))
#define PG_GETARG_INT64(n) DatumGetInt64(PG_GETARG_DATUM(n))
#define PG_GETARG_CHAR(n) DatumGetChar(PG_GETARG_DATUM(n))
#define PG_GETARG_OID(n) DatumGetObjectId(PG_GETARG_DATUM(n))
#define PG_GETARG_BOOL(n) DatumGetBool(PG_GETARG_DATUM(n))
#define PG_GETARG_POINTER(n) DatumGetPointer(PG_GETARG_DATUM(n))
#define PG_GETARG_CSTRING(n) DatumGetCString(PG_GETARG_DATUM(n))
#define PG_GETARG_FLOAT4(n) DatumGetFloat4(PG_GETARG_DATUM(n))
#define PG_GETARG_FLOAT8(n) DatumGetFloat8(PG_GETARG_DATUM(n))
#define PG_GETARG_TEXT_PP(n) ((text *) PG_DETOAST_DATUM_PACKED(PG_GETARG_DATUM(n)))
#define PG_GETARG_TEXT_P(n) ((text *) PG_DETOAST_DATUM(PG_GETARG_DATUM(n)))
#define PG_GETARG_BYTEA_PP(n) ((bytea *) PG_DETOAST_DATUM_PACKED(PG_GETARG_DATUM(n)))

#define PG_RETURN_DATUM(x) return (x)
#define PG_RETURN_VOID() return (Datum) 0
#define PG_RETURN_NULL() \
	do { fcinfo->isnull = true; return (Datum) 0; } while (0)
#define PG_RETURN_INT32(x) return Int32GetDatum(x)
#define PG_RETURN_INT16(x) return Int16GetDatum(x)
#define PG_RETURN_INT64(x) return Int64GetDatum(x)
#define PG_RETURN_BOOL(x) return BoolGetDatum(x)
#define PG_RETURN_OID(x) return ObjectIdGetDatum(x)
#define PG_RETURN_POINTER(x) return PointerGetDatum(x)
#define PG_RETURN_CSTRING(x) return CStringGetDatum(x)
#define PG_RETURN_FLOAT4(x) return Float4GetDatum(x)
#define PG_RETURN_FLOAT8(x) return Float8GetDatum(x)
#define PG_RETURN_TEXT_P(x) PG_RETURN_POINTER(x)
#define PG_RETURN_BYTEA_P(x) PG_RETURN_POINTER(x)
#define PG_RETURN_UINT32(x) return UInt32GetDatum(x)
#define PG_RETURN_UINT64(x) return UInt64GetDatum(x)
/* fmgr.h DatumGetTextPP (verbatim) */
#define DatumGetTextPP(X) ((text *) PG_DETOAST_DATUM_PACKED(X))

/* DirectFunctionCallN: real fmgr.c semantics (elog on NULL result) */
extern Datum DirectFunctionCall1Coll(PGFunction func, Oid collation, Datum arg1);
extern Datum DirectFunctionCall2Coll(PGFunction func, Oid collation, Datum arg1, Datum arg2);
extern Datum DirectFunctionCall3Coll(PGFunction func, Oid collation, Datum arg1, Datum arg2, Datum arg3);
#define DirectFunctionCall1(func, arg1) \
	DirectFunctionCall1Coll(func, InvalidOid, arg1)
#define DirectFunctionCall2(func, arg1, arg2) \
	DirectFunctionCall2Coll(func, InvalidOid, arg1, arg2)
#define DirectFunctionCall3(func, arg1, arg2, arg3) \
	DirectFunctionCall3Coll(func, InvalidOid, arg1, arg2, arg3)

/* fmgr.h varlena size helpers used by pasted bodies */
#define PG_GETARG_RAW_VARLENA_P(n) ((struct varlena *) PG_GETARG_POINTER(n))


typedef char *Pointer;
#define PG_FREE_IF_COPY(ptr, n) \
	do { \
		if ((Pointer) (ptr) != PG_GETARG_POINTER(n)) \
			pfree(ptr); \
	} while (0)

/* fmgr.c DirectInputFunctionCallSafe semantics (escontext NULL here) */
extern bool DirectInputFunctionCallSafe(PGFunction func, char *str,
										Oid typioparam, int32 typmod,
										Node *escontext, Datum *result);

#endif							/* PG_JSONBFAM_SHIM_FMGR_H */
