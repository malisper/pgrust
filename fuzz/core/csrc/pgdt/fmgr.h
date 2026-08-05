/*
 * SHIM fmgr.h — NOT PostgreSQL code (differential-fuzz oracle plumbing).
 *
 * Minimal fmgr environment so the VERBATIM date.c entry wrappers vendored
 * into pg_datetime_io_io.c (date_in, time_in, ...) compile unchanged: a
 * flat FunctionCallInfo carrying Datum args + the escontext pointer, and
 * the PG_GETARG / PG_RETURN macros they use. Plumbing only, never logic:
 * Datum is the same 8-byte pass-by-value/pointer union as upstream LP64.
 * The vendored utils/{date,datetime,timestamp}.h headers include this in
 * place of the real fmgr.h.
 */
#ifndef PG_DIFFFUZZ_SHIM_FMGR_H
#define PG_DIFFFUZZ_SHIM_FMGR_H

#include "postgres.h"			/* csrc/shim/postgres.h: int64 etc. */

/* c.h bits the vendored headers use */
#define PGDLLIMPORT
#define FLEXIBLE_ARRAY_MEMBER	/* empty */
typedef size_t Size;

typedef uintptr_t Datum;
typedef unsigned int Oid;

/* Node: only ever used as an opaque escontext pointer (always NULL here =
 * hard-error shape; soft-error paths are exercised on the Rust side). */
typedef struct Node Node;

/* varlena/text: the oracle driver always builds 4-byte-header, uncompressed
 * varlenas, so the _ANY accessors reduce to the 4B form (see varatt.h). */
typedef struct varlena
{
	uint32		vl_len_;		/* (length + VARHDRSZ) << 2, little-endian */
	char		vl_dat[];
} varlena;
typedef struct varlena text;
#define VARHDRSZ ((int32) sizeof(uint32))
#define VARDATA_ANY(PTR) (((varlena *) (PTR))->vl_dat)
#define VARSIZE_ANY_EXHDR(PTR) ((int) ((((varlena *) (PTR))->vl_len_) >> 2) - VARHDRSZ)

/* Numeric: opaque; the retnumeric branches are unreachable in this oracle
 * (stubs abort), but time_part_common mentions the type. */
typedef struct NumericData *Numeric;

typedef struct NullableDatum
{
	Datum		value;
	bool		isnull;
} NullableDatum;

typedef struct FunctionCallInfoBaseData
{
	Node	   *context;		/* escontext slot: always NULL here */
	bool		isnull;
	short		nargs;
	NullableDatum args[8];
}		   *FunctionCallInfo;

#define PG_FUNCTION_ARGS FunctionCallInfo fcinfo

static inline int32
DatumGetInt32(Datum X)
{
	return (int32) X;
}
static inline Datum
Int32GetDatum(int32 X)
{
	return (Datum) (uint32) X;
}
static inline int64
DatumGetInt64(Datum X)
{
	return (int64) X;
}
static inline Datum
Int64GetDatum(int64 X)
{
	return (Datum) X;
}
static inline void *
DatumGetPointer(Datum X)
{
	return (void *) X;
}
static inline Datum
PointerGetDatum(const void *X)
{
	return (Datum) X;
}
static inline float8
DatumGetFloat8(Datum X)
{
	float8		r;

	memcpy(&r, &X, sizeof(r));
	return r;
}
static inline Datum
Float8GetDatum(float8 X)
{
	Datum		r;

	memcpy(&r, &X, sizeof(r));
	return r;
}

#define PG_GETARG_DATUM(n)	 (fcinfo->args[n].value)
#define PG_GETARG_CSTRING(n) ((char *) DatumGetPointer(PG_GETARG_DATUM(n)))
#define PG_GETARG_INT32(n)	 DatumGetInt32(PG_GETARG_DATUM(n))
#define PG_GETARG_INT64(n)	 DatumGetInt64(PG_GETARG_DATUM(n))
#define PG_GETARG_FLOAT8(n)	 DatumGetFloat8(PG_GETARG_DATUM(n))
#define PG_GETARG_TEXT_PP(n) ((text *) DatumGetPointer(PG_GETARG_DATUM(n)))

#define PG_RETURN_INT32(x)	 return Int32GetDatum(x)
#define PG_RETURN_INT64(x)	 return Int64GetDatum(x)
#define PG_RETURN_FLOAT8(x)	 return Float8GetDatum(x)
#define PG_RETURN_CSTRING(x) return PointerGetDatum(x)
#define PG_RETURN_TEXT_P(x)	 return PointerGetDatum(x)
#define PG_RETURN_NUMERIC(x) return PointerGetDatum(x)
#define PG_RETURN_BOOL(x)	 return ((Datum) ((x) ? 1 : 0))
#define PG_RETURN_NULL()	 do { fcinfo->isnull = true; return (Datum) 0; } while (0)

#endif
