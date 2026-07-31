/*
 * pg_arrayutils_io.c: vendored PostgreSQL C oracle for the arrayutils_diff
 * differential fuzz target (100%-coverage campaign; crate
 * crates/backend/utils/adt/arrayutils).
 *
 * Provenance (all bodies VERBATIM unless a shim is listed below):
 *   - src/backend/utils/adt/arrayutils.c @ postgres-src
 *     62d6c7d3df6287f1bd83199c1a746e50d31571a0 (Stamp-18.3): lines 25..225
 *     byte-for-byte (ArrayGetOffset, ArrayGetNItems, ArrayGetNItemsSafe,
 *     ArrayCheckBounds, ArrayCheckBoundsSafe, mda_get_range, mda_get_prod,
 *     mda_get_offset_values, mda_next_tuple). ArrayGetIntegerTypmods
 *     (lines 227..264) is NOT vendored: it is array-Datum machinery
 *     (deconstruct_array/palloc/pg_strtoint32) outside the crate's ported
 *     surface.
 *   - src/include/common/int.h @ same ref: pg_add_s32_overflow, the
 *     HAVE__BUILTIN_OP_OVERFLOW arm (the arm every supported gcc/clang
 *     target compiles), reduced to that arm below.
 *
 * Shims (plumbing only, never logic):
 *   - ereturn(escontext, ret, (errcode(X), errmsg(...))) -> record X in
 *     pg_diff_errcode and return ret; errmsg evaluates to 0 unevaluated.
 *     escontext is the hard-error (NULL) shape on both sides.
 *   - errcode symbol -> small int: 1 = ERRCODE_PROGRAM_LIMIT_EXCEEDED (54000).
 *   - MaxArraySize = MaxAllocSize / sizeof(Datum) = 0x3fffffff / 8 with
 *     Size = size_t and 8-byte Datum, exactly utils/array.h's definition
 *     on every supported LP64 target.
 *   - PG_USED_FOR_ASSERTS_ONLY -> empty; Assert -> noop (NDEBUG parity).
 *
 * Driver entries (section 3) are fuzz plumbing, NOT Postgres code.
 * PRECONDITION CARVES (documented in the target header, enforced by the
 * driver): the mda_* family and ArrayGetOffset carry "caller has
 * validated" contracts in C (no overflow, span[i] >= 1); the driver
 * constrains inputs to those domains, since outside them C is UB.
 */

#include "postgres.h"

#include <stddef.h>

extern _Thread_local int pg_diff_errcode;

#define ERRCODE_PROGRAM_LIMIT_EXCEEDED 1

#define errcode(c) (pg_diff_errcode = (c))
#define errmsg(...) 0
#define ereturn(escontext, ret, stuff) do { (void) (stuff); return (ret); } while (0)

struct Node;					/* opaque; escontext is always NULL here */

typedef size_t Size;

/* utils/memutils.h / utils/array.h @ 62d6c7d3df: exact value chain */
#define MaxAllocSize ((Size) 0x3fffffff)	/* 1 gigabyte - 1 */
#define MaxArraySize (MaxAllocSize / 8)		/* sizeof(Datum) == 8 */

#define PG_USED_FOR_ASSERTS_ONLY

/* ==== src/include/common/int.h @ 62d6c7d3df (VERBATIM, the
 * HAVE__BUILTIN_OP_OVERFLOW arm) ==== */
static inline bool
pg_add_s32_overflow(int32 a, int32 b, int32 *result)
{
	return __builtin_add_overflow(a, b, result);
}

/* Prototypes the real utils/array.h provided (declarations only). */
extern int	ArrayGetNItemsSafe(int ndim, const int *dims, struct Node *escontext);
extern bool ArrayCheckBoundsSafe(int ndim, const int *dims, const int *lb,
								 struct Node *escontext);

/* ======= SECTION 1: src/backend/utils/adt/arrayutils.c lines 25..225 (VERBATIM) ======= */

/*
 * Convert subscript list into linear element number (from 0)
 *
 * We assume caller has already range-checked the dimensions and subscripts,
 * so no overflow is possible.
 */
int
ArrayGetOffset(int n, const int *dim, const int *lb, const int *indx)
{
	int			i,
				scale = 1,
				offset = 0;

	for (i = n - 1; i >= 0; i--)
	{
		offset += (indx[i] - lb[i]) * scale;
		scale *= dim[i];
	}
	return offset;
}

/*
 * Convert array dimensions into number of elements
 *
 * This must do overflow checking, since it is used to validate that a user
 * dimensionality request doesn't overflow what we can handle.
 *
 * The multiplication overflow check only works on machines that have int64
 * arithmetic, but that is nearly all platforms these days, and doing check
 * divides for those that don't seems way too expensive.
 */
int
ArrayGetNItems(int ndim, const int *dims)
{
	return ArrayGetNItemsSafe(ndim, dims, NULL);
}

/*
 * This entry point can return the error into an ErrorSaveContext
 * instead of throwing an exception.  -1 is returned after an error.
 */
int
ArrayGetNItemsSafe(int ndim, const int *dims, struct Node *escontext)
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
			ereturn(escontext, -1,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("array size exceeds the maximum allowed (%d)",
							(int) MaxArraySize)));

		prod = (int64) ret * (int64) dims[i];

		ret = (int32) prod;
		if ((int64) ret != prod)
			ereturn(escontext, -1,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("array size exceeds the maximum allowed (%d)",
							(int) MaxArraySize)));
	}
	Assert(ret >= 0);
	if ((Size) ret > MaxArraySize)
		ereturn(escontext, -1,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("array size exceeds the maximum allowed (%d)",
						(int) MaxArraySize)));
	return (int) ret;
}

/*
 * Verify sanity of proposed lower-bound values for an array
 *
 * The lower-bound values must not be so large as to cause overflow when
 * calculating subscripts, e.g. lower bound 2147483640 with length 10
 * must be disallowed.  We actually insist that dims[i] + lb[i] be
 * computable without overflow, meaning that an array with last subscript
 * equal to INT_MAX will be disallowed.
 *
 * It is assumed that the caller already called ArrayGetNItems, so that
 * overflowed (negative) dims[] values have been eliminated.
 */
void
ArrayCheckBounds(int ndim, const int *dims, const int *lb)
{
	(void) ArrayCheckBoundsSafe(ndim, dims, lb, NULL);
}

/*
 * This entry point can return the error into an ErrorSaveContext
 * instead of throwing an exception.
 */
bool
ArrayCheckBoundsSafe(int ndim, const int *dims, const int *lb,
					 struct Node *escontext)
{
	int			i;

	for (i = 0; i < ndim; i++)
	{
		/* PG_USED_FOR_ASSERTS_ONLY prevents variable-isn't-read warnings */
		int32		sum PG_USED_FOR_ASSERTS_ONLY;

		if (pg_add_s32_overflow(dims[i], lb[i], &sum))
			ereturn(escontext, false,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("array lower bound is too large: %d",
							lb[i])));
	}

	return true;
}

/*
 * Compute ranges (sub-array dimensions) for an array slice
 *
 * We assume caller has validated slice endpoints, so overflow is impossible
 */
void
mda_get_range(int n, int *span, const int *st, const int *endp)
{
	int			i;

	for (i = 0; i < n; i++)
		span[i] = endp[i] - st[i] + 1;
}

/*
 * Compute products of array dimensions, ie, scale factors for subscripts
 *
 * We assume caller has validated dimensions, so overflow is impossible
 */
void
mda_get_prod(int n, const int *range, int *prod)
{
	int			i;

	prod[n - 1] = 1;
	for (i = n - 2; i >= 0; i--)
		prod[i] = prod[i + 1] * range[i + 1];
}

/*
 * From products of whole-array dimensions and spans of a sub-array,
 * compute offset distances needed to step through subarray within array
 *
 * We assume caller has validated dimensions, so overflow is impossible
 */
void
mda_get_offset_values(int n, int *dist, const int *prod, const int *span)
{
	int			i,
				j;

	dist[n - 1] = 0;
	for (j = n - 2; j >= 0; j--)
	{
		dist[j] = prod[j] - 1;
		for (i = j + 1; i < n; i++)
			dist[j] -= (span[i] - 1) * prod[i];
	}
}

/*
 * Generates the tuple that is lexicographically one greater than the current
 * n-tuple in "curr", with the restriction that the i-th element of "curr" is
 * less than the i-th element of "span".
 *
 * Returns -1 if no next tuple exists, else the subscript position (0..n-1)
 * corresponding to the dimension to advance along.
 *
 * We assume caller has validated dimensions, so overflow is impossible
 */
int
mda_next_tuple(int n, int *curr, const int *span)
{
	int			i;

	if (n <= 0)
		return -1;

	curr[n - 1] = (curr[n - 1] + 1) % span[n - 1];
	for (i = n - 1; i && curr[i] == 0; i--)
		curr[i - 1] = (curr[i - 1] + 1) % span[i - 1];

	if (i)
		return i;
	if (curr[0])
		return 0;

	return -1;
}

/* ========== SECTION 2: fuzz-facing driver entries (NOT Postgres code) ===== */

int
pg_diff_array_get_offset(int n, const int *dim, const int *lb, const int *indx)
{
	pg_diff_errcode = 0;
	return ArrayGetOffset(n, dim, lb, indx);
}

int
pg_diff_array_get_n_items(int ndim, const int *dims)
{
	pg_diff_errcode = 0;
	return ArrayGetNItemsSafe(ndim, dims, NULL);
}

int
pg_diff_array_check_bounds(int ndim, const int *dims, const int *lb)
{
	pg_diff_errcode = 0;
	return ArrayCheckBoundsSafe(ndim, dims, lb, NULL) ? 1 : 0;
}

void
pg_diff_mda_get_range(int n, int *span, const int *st, const int *endp)
{
	pg_diff_errcode = 0;
	mda_get_range(n, span, st, endp);
}

void
pg_diff_mda_get_prod(int n, const int *range, int *prod)
{
	pg_diff_errcode = 0;
	mda_get_prod(n, range, prod);
}

void
pg_diff_mda_get_offset_values(int n, int *dist, const int *prod, const int *span)
{
	pg_diff_errcode = 0;
	mda_get_offset_values(n, dist, prod, span);
}

int
pg_diff_mda_next_tuple(int n, int *curr, const int *span)
{
	pg_diff_errcode = 0;
	return mda_next_tuple(n, curr, span);
}
