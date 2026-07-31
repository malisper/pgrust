/* SHIM header for the jsonpathexec_diff oracle - NOT PostgreSQL code.
 *
 * ARRAY PLUMBING MODEL (environment, not logic): the only array use in the
 * vendored TUs is .decimal(p,s) building a 2-element CSTRING array with
 * construct_array_builtin() and numerictypmodin() immediately reading it
 * back via ArrayGetIntegerTypmods(). The array is a pure round-trip carrier
 * built from pg_ltoa output; the LOGIC (typmod range checks) lives in the
 * VERBATIM numerictypmodin/ArrayGetIntegerTypmods-equivalent path. Model:
 * a cstring vector; ArrayGetIntegerTypmods converts with the VERBATIM
 * pg_strtoint32 exactly like arrayutils.c does.
 */
#ifndef ARRAY_MODEL_H
#define ARRAY_MODEL_H
#include "postgres.h"

typedef struct ArrayType
{
	int			nelems;
	char	  **values;			/* cstrings */
} ArrayType;

extern ArrayType *construct_array_builtin(Datum *elems, int nelems, Oid elmtype);
extern int32 *ArrayGetIntegerTypmods(ArrayType *arr, int *n);
#endif
