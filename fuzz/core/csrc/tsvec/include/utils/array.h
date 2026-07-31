/* SHIM utils/array.h (tsvec oracle) — NOT PostgreSQL code.
 * Array construction/deconstruction is ANOTHER crate's computation
 * (arrayfuncs); here it is argument plumbing: the driver hands the C side
 * the element list directly, and these two shims (implemented in
 * pg_tsvector_core_io.c) marshal it, byte-equivalent to a no-null 1-D
 * text[]/"char"[] deconstruct. ArrayType is opaque to the retained code. */
#ifndef PG_DIFFFUZZ_TSVEC_ARRAY_H
#define PG_DIFFFUZZ_TSVEC_ARRAY_H
typedef struct ArrayType ArrayType;
extern void deconstruct_array_builtin(ArrayType *array, Oid elmtype,
									  Datum **elemsp, bool **nullsp, int *nelemsp);
extern ArrayType *construct_array_builtin(Datum *elems, int nelems, Oid elmtype);
#endif
