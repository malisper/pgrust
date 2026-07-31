#ifndef NV_ARRAY_H
#define NV_ARRAY_H
typedef struct ArrayType
{
	int32		vl_len_;
	int			ndim;
	int32		dataoffset;
	Oid			elemtype;
} ArrayType;

#define PG_GETARG_ARRAYTYPE_P(n) ((ArrayType *) PG_GETARG_POINTER(n))
#define PG_GETARG_ARRAYTYPE_P_COPY(n) ((ArrayType *) PG_GETARG_POINTER(n))
#define PG_RETURN_ARRAYTYPE_P(x) PG_RETURN_POINTER(x)

extern int32 *ArrayGetIntegerTypmods(ArrayType *arr, int *n);
#define ARR_SIZE(a) VARSIZE(a)
#define ARR_NDIM(a) ((a)->ndim)
#define ARR_HASNULL(a) ((a)->dataoffset != 0)
#define ARR_ELEMTYPE(a) ((a)->elemtype)
#define ARR_OVERHEAD_NONULLS(ndims) \
	((Size) offsetof(ArrayType, elemtype) + sizeof(Oid) + 2 * sizeof(int) * (ndims))
#define ARR_DATA_PTR(a) \
	(((char *) (a)) + (ARR_HASNULL(a) ? (a)->dataoffset : ARR_OVERHEAD_NONULLS(ARR_NDIM(a))))
#endif
