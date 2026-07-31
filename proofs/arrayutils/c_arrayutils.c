/*
 * Vendored PostgreSQL C for Kani dual-execution equivalence proofs
 * (compiled via `-Z c-ffi --c-lib`). Same verbatim sections as the
 * fuzz oracle csrc/pg_arrayutils_io.c; provenance:
 * src/backend/utils/adt/arrayutils.c lines 25..225 + common/int.h pg_add_s32_overflow @ postgres-src 62d6c7d3df6287f1bd83199c1a746e50d31571a0 (Stamp-18.3).
 * Shims are typedef/macro plumbing only, never logic. Assert() compiled
 * out (NDEBUG parity); harnesses fence preconditions with kani::assume.
 */
#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

typedef int32_t int32;
typedef int64_t int64;
typedef uint32_t uint32;
typedef uint64_t uint64;

#define UINT64CONST(x) UINT64_C(x)
#define unlikely(x) (x)
#define likely(x) (x)
#define Assert(x) ((void) 0)

extern int pg_diff_errcode_c; /* proof-side errcode channel */
int pg_diff_errcode_c;

#define ERRCODE_PROGRAM_LIMIT_EXCEEDED 1
#define errcode(c) (pg_diff_errcode_c = (c))
#define errmsg(...) 0
#define ereturn(escontext, ret, stuff) do { (void) (stuff); return (ret); } while (0)
struct Node;
typedef size_t Size;
#define MaxAllocSize ((Size) 0x3fffffff)
#define MaxArraySize (MaxAllocSize / 8)
#define PG_USED_FOR_ASSERTS_ONLY

static inline bool
pg_add_s32_overflow(int32 a, int32 b, int32 *result)
{
	return __builtin_add_overflow(a, b, result);
}

extern int ArrayGetNItemsSafe(int ndim, const int *dims, struct Node *escontext);
extern bool ArrayCheckBoundsSafe(int ndim, const int *dims, const int *lb, struct Node *escontext);

/* ==== VERBATIM src/backend/utils/adt/arrayutils.c lines 25..225 ==== */
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

int c_errcode_read(void) { return pg_diff_errcode_c; }
void c_errcode_reset(void) { pg_diff_errcode_c = 0; }

/* ==== proof-facing wrappers (plumbing only, NOT Postgres code): keep
 * struct Node out of the cross-language surface; escontext is always
 * NULL (hard-error shape) with the errcode channel above. ==== */

int c_array_get_offset(int n, const int *dim, const int *lb, const int *indx)
{ return ArrayGetOffset(n, dim, lb, indx); }

int c_array_get_n_items(int ndim, const int *dims)
{ pg_diff_errcode_c = 0; return ArrayGetNItemsSafe(ndim, dims, (struct Node *) 0); }

int c_array_check_bounds(int ndim, const int *dims, const int *lb)
{ pg_diff_errcode_c = 0; return ArrayCheckBoundsSafe(ndim, dims, lb, (struct Node *) 0) ? 1 : 0; }

int c_mda_get_range(int n, int *span, const int *st, const int *endp)
{ mda_get_range(n, span, st, endp); return 0; }

int c_mda_get_prod(int n, const int *range, int *prod)
{ mda_get_prod(n, range, prod); return 0; }

int c_mda_get_offset_values(int n, int *dist, const int *prod, const int *span)
{ mda_get_offset_values(n, dist, prod, span); return 0; }

int c_mda_next_tuple(int n, int *curr, const int *span)
{ return mda_next_tuple(n, curr, span); }
