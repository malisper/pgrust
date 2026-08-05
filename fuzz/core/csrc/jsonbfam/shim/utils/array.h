/* SHIM utils/array.h — VERBATIM subset of src/include/utils/array.h
 * @ 62d6c7d3df (18.3): ArrayType + the flat-array ARR_* accessor macros +
 * the deconstruction protos the vendored jsonb ops family calls
 * (jsonb_object[_two_arg], jsonb_exists_any/all, get_jsonb_path_all,
 * setPath callers, jsonb_delete_array). Expanded-array (AARR_*) surface
 * intentionally absent — no vendored caller touches it. */
#ifndef PG_JSONBFAM_SHIM_ARRAY_H
#define PG_JSONBFAM_SHIM_ARRAY_H

#include "postgres.h"
#include "fmgr.h"

/* array.h line 82 */
#define MaxArraySize ((Size) (MaxAllocSize / sizeof(Datum)))

/* array.h lines 92-98, verbatim */
typedef struct ArrayType
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	int			ndim;			/* # of dimensions */
	int32		dataoffset;		/* offset to data, or 0 if no bitmap */
	Oid			elemtype;		/* element type OID */
} ArrayType;

/* array.h lines 261-264, verbatim */
#define DatumGetArrayTypeP(X)		  ((ArrayType *) PG_DETOAST_DATUM(X))
#define DatumGetArrayTypePCopy(X)	  ((ArrayType *) PG_DETOAST_DATUM_COPY(X))
#define PG_GETARG_ARRAYTYPE_P(n)	  DatumGetArrayTypeP(PG_GETARG_DATUM(n))
#define PG_GETARG_ARRAYTYPE_P_COPY(n) DatumGetArrayTypePCopy(PG_GETARG_DATUM(n))

/* array.h lines 289-324, verbatim */
#define ARR_SIZE(a)				VARSIZE(a)
#define ARR_NDIM(a)				((a)->ndim)
#define ARR_HASNULL(a)			((a)->dataoffset != 0)
#define ARR_ELEMTYPE(a)			((a)->elemtype)

#define ARR_DIMS(a) \
		((int *) (((char *) (a)) + sizeof(ArrayType)))
#define ARR_LBOUND(a) \
		((int *) (((char *) (a)) + sizeof(ArrayType) + \
				  sizeof(int) * ARR_NDIM(a)))

#define ARR_NULLBITMAP(a) \
		(ARR_HASNULL(a) ? \
		 (bits8 *) (((char *) (a)) + sizeof(ArrayType) + \
					2 * sizeof(int) * ARR_NDIM(a)) \
		 : (bits8 *) NULL)

#define ARR_OVERHEAD_NONULLS(ndims) \
		MAXALIGN(sizeof(ArrayType) + 2 * sizeof(int) * (ndims))
#define ARR_OVERHEAD_WITHNULLS(ndims, nitems) \
		MAXALIGN(sizeof(ArrayType) + 2 * sizeof(int) * (ndims) + \
				 ((nitems) + 7) / 8)

#define ARR_DATA_OFFSET(a) \
		(ARR_HASNULL(a) ? (a)->dataoffset : ARR_OVERHEAD_NONULLS(ARR_NDIM(a)))

#define ARR_DATA_PTR(a) \
		(((char *) (a)) + ARR_DATA_OFFSET(a))

/* protos (array.h, subset) */
extern int	ArrayGetNItems(int ndim, const int *dims);
extern int	ArrayGetNItemsSafe(int ndim, const int *dims, struct Node *escontext);
extern void deconstruct_array(ArrayType *array,
							  Oid elmtype,
							  int elmlen, bool elmbyval, char elmalign,
							  Datum **elemsp, bool **nullsp, int *nelemsp);
extern void deconstruct_array_builtin(ArrayType *array,
									  Oid elmtype,
									  Datum **elemsp, bool **nullsp, int *nelemsp);
extern bool array_contains_nulls(ArrayType *array);

#endif
