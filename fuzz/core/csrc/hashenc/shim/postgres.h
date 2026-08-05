/*
 * SHIM postgres.h — NOT PostgreSQL code. See shim/c.h for provenance.
 * Backend-flavored shim for the vendored pg_crc.c (SQL crc32/crc32c
 * wrappers): Datum + the few fmgr macros those two functions expand to.
 * Plumbing only, never logic.
 */
#ifndef PG_DIFFFUZZ_HASHENC_SHIM_POSTGRES_H
#define PG_DIFFFUZZ_HASHENC_SHIM_POSTGRES_H

#include "c.h"

typedef uintptr_t Datum;

/* Single-argument fmgr call shim: arg0 = the detoasted bytea pointer. */
typedef struct pg_shim_FunctionCallInfoBaseData
{
	void	   *arg0;
} pg_shim_FunctionCallInfoBaseData, *FunctionCallInfo;

#define PG_FUNCTION_ARGS FunctionCallInfo fcinfo
#define PG_GETARG_BYTEA_PP(n) ((bytea *) fcinfo->arg0)
#define PG_RETURN_INT64(x) return (Datum) (int64) (x)

#endif
