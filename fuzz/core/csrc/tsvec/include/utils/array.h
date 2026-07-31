/* SHIM utils/array.h (tsvec oracle) — mixed shim + verbatim.
 *
 * Part 1 (SHIM, tsvector_op.c): array construction/deconstruction is ANOTHER
 * crate's computation (arrayfuncs); there it is argument plumbing: the driver
 * hands the C side the element list directly, and the two *_builtin shims
 * (implemented in pg_tsvector_core_io.c) marshal it, byte-equivalent to a
 * no-null 1-D text[]/"char"[] deconstruct.
 *
 * Part 2 (VERBATIM, tsrank.c getWeights): tsrank reads a real float4[] IMAGE
 * (ARR_NDIM/ARR_DIMS/ARR_DATA_PTR/ArrayGetNItems/array_contains_nulls), and
 * the Rust counterpart (adt_tsrank builtins.rs arg_weights) reads the same
 * image bytes — so the oracle needs the REAL array layout. ArrayType struct +
 * ARR_* accessor macros + MAXDIM/MaxArraySize are VERBATIM from upstream
 * src/include/utils/array.h @ 62d6c7d3df6287f1bd83199c1a746e50d31571a0
 * (lines 75-82 and 289-325); bits8 is verbatim from src/include/c.h.
 * ArrayGetNItems/ArrayGetNItemsSafe/array_contains_nulls bodies are pasted
 * verbatim in csrc/pg_tsrank_io.c.
 */
#ifndef PG_DIFFFUZZ_TSVEC_ARRAY_H
#define PG_DIFFFUZZ_TSVEC_ARRAY_H

/* ---- verbatim from src/include/c.h ---- */
typedef uint8 bits8;			/* >= 8 bits */

/* ---- verbatim from src/include/utils/array.h ---- */
typedef struct ArrayType
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	int			ndim;			/* # of dimensions */
	int32		dataoffset;		/* offset to data, or 0 if no bitmap */
	Oid			elemtype;		/* element type OID */
} ArrayType;

#define MAXDIM 6

#define MaxArraySize ((Size) (MaxAllocSize / sizeof(Datum)))

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

/* prototypes for the verbatim bodies pasted in pg_tsrank_io.c */
extern int	ArrayGetNItems(int ndim, const int *dims);
extern int	ArrayGetNItemsSafe(int ndim, const int *dims, struct Node *escontext);
extern bool array_contains_nulls(ArrayType *array);

/* ---- shim decls (tsvector_op.c argument plumbing; see header) ---- */
extern void deconstruct_array_builtin(ArrayType *array, Oid elmtype,
									  Datum **elemsp, bool **nullsp, int *nelemsp);
extern ArrayType *construct_array_builtin(Datum *elems, int nelems, Oid elmtype);
#endif
