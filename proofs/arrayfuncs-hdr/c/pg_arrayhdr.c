/*
 * pg_arrayhdr.c — vendored PostgreSQL array header-read builtins for the
 * Kani C≡Rust equivalence suite (proofs/arrayfuncs-hdr).
 *
 * Provenance:
 *   - src/backend/utils/adt/arrayfuncs.c  @ REL_18_STABLE
 *     (fetched 2026-07-30 from raw.githubusercontent.com):
 *       array_ndims, array_dims, array_lower, array_upper, array_length,
 *       array_cardinality
 *   - src/backend/utils/adt/arrayutils.c  @ REL_18_STABLE:
 *       ArrayGetNItems, ArrayGetNItemsSafe
 *   - src/include/utils/array.h @ REL_18_STABLE: ArrayType layout,
 *     ARR_NDIM / ARR_HASNULL / ARR_DIMS / ARR_LBOUND, MAXDIM, MaxArraySize.
 *
 * Function bodies are verbatim. Shims (plumbing only), each documented:
 *   1. fmgr unwrapping: PG_FUNCTION_ARGS -> plain C signature.
 *      PG_GETARG_ANY_ARRAY_P(0) -> `const ArrayType *v` parameter.
 *      FLAT-ARRAY FENCE: the harness plane is flat (non-expanded,
 *      pre-detoasted) arrays only, so AARR_NDIM/AARR_DIMS/AARR_LBOUND
 *      reduce to their flat ARR_* branches (array.h's AARR_* macros
 *      dispatch on VARATT_IS_EXPANDED_HEADER; pgrust has no expanded
 *      arrays and the harness never builds one). PG_RETURN_NULL() ->
 *      *isnull = 1 + return 0; PG_RETURN_INT32(x) -> return x.
 *   2. ereport/ereturn -> PROOF_EREPORT_FLAG convention (see
 *      ../../support/c/pg_proof_shim.h): ArrayGetNItemsSafe takes an
 *      `int *err` out-param instead of `struct Node *escontext`; the
 *      error arm sets *err = 1 and returns -1 (the ereturn errorval).
 *      Message text is out of proof.
 *   3. array_dims's sprintf(p, "[%d:%d]", a, b) -> pg_proof_sprintf_bracket
 *      (libc model shim, same class as the shim header's ctype helpers:
 *      Kani/CBMC has no sprintf model). Implements exactly the
 *      "[%d:%d]" rendering incl. INT_MIN; p += strlen(p) -> uses the
 *      returned length (CBMC's strlen over a just-written symbolic-length
 *      buffer is needless circuit).
 *
 * No other edits; -fwrapv wrap semantics match CBMC's default
 * two's-complement model (array_upper's dimv[i] + lb[i] - 1 may wrap;
 * the harness fences to the in-contract non-overflow plane and documents
 * it).
 */

#include "../../support/c/pg_proof_shim.h"

/* ---- array.h layout + macros, verbatim ---- */

typedef struct ArrayType
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	int			ndim;			/* # of dimensions */
	int32		dataoffset;		/* offset to data, or 0 if no bitmap */
	Oid			elemtype;		/* element type OID */
} ArrayType;

#define MAXDIM 6

/* MaxAllocSize / sizeof(Datum) — c.h/memutils.h values, Datum is 8 bytes */
#define MaxAllocSize ((Size) 0x3fffffff)
#define MaxArraySize ((Size) (MaxAllocSize / 8))

#define ARR_SIZE(a)				VARSIZE(a)
#define ARR_NDIM(a)				((a)->ndim)
#define ARR_HASNULL(a)			((a)->dataoffset != 0)
#define ARR_ELEMTYPE(a)			((a)->elemtype)
#define ARR_DIMS(a) \
		((int *) (((char *) (a)) + sizeof(ArrayType)))
#define ARR_LBOUND(a) \
		((int *) (((char *) (a)) + sizeof(ArrayType) + \
				  sizeof(int) * ARR_NDIM(a)))

/*
 * AARR_* -> flat ARR_* under the FLAT-ARRAY FENCE (shim 1): the expanded
 * branch (VARATT_IS_EXPANDED_HEADER) is outside the harness plane.
 */
#define AARR_NDIM(v)	ARR_NDIM(v)
#define AARR_DIMS(v)	ARR_DIMS(v)
#define AARR_LBOUND(v)	ARR_LBOUND(v)

/* ---- arrayutils.c: ArrayGetNItemsSafe / ArrayGetNItems, verbatim
 * (ereturn -> PROOF_EREPORT_FLAG per shim 2) ---- */

static int
pg_ArrayGetNItemsSafe(int ndim, const int *dims, int *err)
{
	int32		ret;
	int			i;

	if (ndim <= 0)
		return 0;
	ret = 1;
	for (i = 0; i < ndim; i++)
	{
		int64		prod;

		/* A negative dimension implies that UB-LB overflowed ... */
		if (dims[i] < 0)
		{
			*err = 1;			/* ereturn(escontext, -1, ...54000...) */
			return -1;
		}

		prod = (int64) ret * (int64) dims[i];

		ret = (int32) prod;
		if ((int64) ret != prod)
		{
			*err = 1;			/* ereturn(escontext, -1, ...54000...) */
			return -1;
		}
	}
	Assert(ret >= 0);
	if ((Size) ret > MaxArraySize)
	{
		*err = 1;				/* ereturn(escontext, -1, ...54000...) */
		return -1;
	}
	return (int) ret;
}

static int
pg_ArrayGetNItems(int ndim, const int *dims, int *err)
{
	return pg_ArrayGetNItemsSafe(ndim, dims, err);
}

/* ---- arrayfuncs.c bodies, verbatim modulo shims 1-3 ---- */

/* array_ndims: returns the number of dimensions of the array pointed to by v */
int32
pg_array_ndims(const ArrayType *v, int *isnull)
{
	/* Sanity check: does it look like an array at all? */
	if (AARR_NDIM(v) <= 0 || AARR_NDIM(v) > MAXDIM)
	{
		*isnull = 1;
		return 0;
	}

	return AARR_NDIM(v);
}

/* array_lower: returns the lower dimension, of the DIM requested, ... */
int32
pg_array_lower(const ArrayType *v, int32 reqdim, int *isnull)
{
	int		   *lb;
	int			result;

	/* Sanity check: does it look like an array at all? */
	if (AARR_NDIM(v) <= 0 || AARR_NDIM(v) > MAXDIM)
	{
		*isnull = 1;
		return 0;
	}

	/* Sanity check: was the requested dim valid */
	if (reqdim <= 0 || reqdim > AARR_NDIM(v))
	{
		*isnull = 1;
		return 0;
	}

	lb = AARR_LBOUND(v);
	result = lb[reqdim - 1];

	return result;
}

/* array_upper: returns the upper dimension, of the DIM requested, ... */
int32
pg_array_upper(const ArrayType *v, int32 reqdim, int *isnull)
{
	int		   *dimv,
			   *lb;
	int			result;

	/* Sanity check: does it look like an array at all? */
	if (AARR_NDIM(v) <= 0 || AARR_NDIM(v) > MAXDIM)
	{
		*isnull = 1;
		return 0;
	}

	/* Sanity check: was the requested dim valid */
	if (reqdim <= 0 || reqdim > AARR_NDIM(v))
	{
		*isnull = 1;
		return 0;
	}

	lb = AARR_LBOUND(v);
	dimv = AARR_DIMS(v);

	result = dimv[reqdim - 1] + lb[reqdim - 1] - 1;

	return result;
}

/* array_length: returns the length, of the dimension requested */
int32
pg_array_length(const ArrayType *v, int32 reqdim, int *isnull)
{
	int		   *dimv;
	int			result;

	/* Sanity check: does it look like an array at all? */
	if (AARR_NDIM(v) <= 0 || AARR_NDIM(v) > MAXDIM)
	{
		*isnull = 1;
		return 0;
	}

	/* Sanity check: was the requested dim valid */
	if (reqdim <= 0 || reqdim > AARR_NDIM(v))
	{
		*isnull = 1;
		return 0;
	}

	dimv = AARR_DIMS(v);

	result = dimv[reqdim - 1];

	return result;
}

/* array_cardinality: returns the total number of elements in an array */
int32
pg_array_cardinality(const ArrayType *v, int *err)
{
	return pg_ArrayGetNItems(AARR_NDIM(v), AARR_DIMS(v), err);
}

/* ---- shim 3: sprintf "[%d:%d]" libc model for array_dims ---- */

/* decimal render of one int (INT_MIN-safe via int64 widening); returns len */
static int
pg_proof_itoa10(char *dst, int v)
{
	int64		x = (int64) v;
	char		tmp[12];
	int			n = 0,
				len = 0;

	if (x < 0)
	{
		dst[len++] = '-';
		x = -x;
	}
	do
	{
		tmp[n++] = (char) ('0' + (x % 10));
		x /= 10;
	} while (x != 0);
	while (n > 0)
		dst[len++] = tmp[--n];
	return len;
}

/* exactly sprintf(p, "[%d:%d]", a, b); returns chars written (excl NUL) */
static int
pg_proof_sprintf_bracket(char *p, int a, int b)
{
	int			len = 0;

	p[len++] = '[';
	len += pg_proof_itoa10(p + len, a);
	p[len++] = ':';
	len += pg_proof_itoa10(p + len, b);
	p[len++] = ']';
	p[len] = '\0';
	return len;
}

/*
 * array_dims: returns the dimensions of the array pointed to by "v", as a
 * "text" result. Body verbatim modulo shim 1 (fmgr/PG_RETURN_TEXT_P: the
 * rendered cstring is returned in caller buffer `out`, return value is its
 * length, isnull covers PG_RETURN_NULL; cstring_to_text is the Rust-side
 * comparison surface) and shim 3 (sprintf + p += strlen(p) -> the shim's
 * returned length).
 */
int32
pg_array_dims(const ArrayType *v, int *isnull, char *out)
{
	char	   *p;
	int			i;
	int		   *dimv,
			   *lb;

	/*
	 * 33 since we assume 15 digits per number + ':' +'[]'
	 *
	 * +1 for trailing null
	 */
	char		buf[MAXDIM * 33 + 1];

	/* Sanity check: does it look like an array at all? */
	if (AARR_NDIM(v) <= 0 || AARR_NDIM(v) > MAXDIM)
	{
		*isnull = 1;
		return 0;
	}

	dimv = AARR_DIMS(v);
	lb = AARR_LBOUND(v);

	p = buf;
	for (i = 0; i < AARR_NDIM(v); i++)
	{
		p += pg_proof_sprintf_bracket(p, lb[i], dimv[i] + lb[i] - 1);
	}

	/* PG_RETURN_TEXT_P(cstring_to_text(buf)) -> copy out, return length */
	{
		int			outlen = (int) (p - buf);

		for (i = 0; i < outlen; i++)
			out[i] = buf[i];
		out[outlen] = '\0';
		return outlen;
	}
}
