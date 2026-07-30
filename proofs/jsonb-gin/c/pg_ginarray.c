/*
 * Vendored PostgreSQL C for the GIN array-opclass consistent proofs
 * (proofs/jsonb-gin, ginarrayconsistent oid 2744 / ginarraytriconsistent
 * oid 3920).
 *
 * Provenance (REL_18_STABLE, fetched 2026-07-28 from
 * https://raw.githubusercontent.com/postgres/postgres/REL_18_STABLE/):
 *   - src/backend/access/gin/ginarrayproc.c:
 *       ginarrayconsistent, ginarraytriconsistent (bodies verbatim)
 *   - src/include/access/gin.h: GinTernaryValue typedef + GIN_FALSE/
 *       GIN_TRUE/GIN_MAYBE values, verbatim.
 *
 * SHIMS (plumbing only, never logic):
 *   - typedefs/Assert via ../../support/c/pg_proof_shim.h.
 *   - fmgr unwrapping: PG_GETARG_POINTER/UINT16/INT32 -> plain C params;
 *     PG_RETURN_BOOL / PG_RETURN_GIN_TERNARY_VALUE -> int return.
 *   - elog(ERROR, ...) -> PROOF_EREPORT_FLAG(err) + fallthrough to the
 *     verbatim `res = false` the C body assigns after elog. All harnesses
 *     pass in-range strategies, so the arm must stay unreachable and each
 *     harness asserts *err == 0.
 *
 * Note on `check` typing: the BINARY consistent function's check array is
 * bool[] in C (values 0/1 by the ginlogic.c caller contract); the ternary
 * variant's is GinTernaryValue[] (0/1/2). Harnesses fence accordingly.
 * `nullFlags` is bool[] in C; the pgrust side reads its queryCategories
 * array as the same bytes (GIN_CAT_NULL_KEY == 1), so harnesses fence the
 * shared bytes to {0,1} and pass them to both sides unchanged.
 */

#include <stddef.h>
#include "../../support/c/pg_proof_shim.h"

/* access/stratnum.h */
typedef uint16 StrategyNumber;

/* access/gin.h (verbatim values) */
typedef char GinTernaryValue;
#define GIN_FALSE		0		/* item is not present / does not match */
#define GIN_TRUE		1		/* item is present / matches */
#define GIN_MAYBE		2		/* don't know if item is present / don't know
								 * if it matches */

/* ginarrayproc.c (verbatim) */
#define GinOverlapStrategy		1
#define GinContainsStrategy		2
#define GinContainedStrategy	3
#define GinEqualStrategy		4

/*
 * consistent support function
 * (fmgr unwrap shim: PG_GETARG_* -> params, PG_RETURN_BOOL -> int;
 * body verbatim from ginarrayproc.c ginarrayconsistent)
 */
int
pga_consistent(const unsigned char *check, unsigned strategy, int nkeys,
			   const unsigned char *nullFlags, int *recheck_out, int *err)
{
	bool	   *recheck = (bool *) 0;
	bool		recheck_local = false;
	bool		res;
	int32		i;

	recheck = &recheck_local;

	switch (strategy)
	{
		case GinOverlapStrategy:
			/* result is not lossy */
			*recheck = false;
			/* must have a match for at least one non-null element */
			res = false;
			for (i = 0; i < nkeys; i++)
			{
				if (check[i] && !nullFlags[i])
				{
					res = true;
					break;
				}
			}
			break;
		case GinContainsStrategy:
			/* result is not lossy */
			*recheck = false;
			/* must have all elements in check[] true, and no nulls */
			res = true;
			for (i = 0; i < nkeys; i++)
			{
				if (!check[i] || nullFlags[i])
				{
					res = false;
					break;
				}
			}
			break;
		case GinContainedStrategy:
			/* we will need recheck */
			*recheck = true;
			/* can't do anything else useful here */
			res = true;
			break;
		case GinEqualStrategy:
			/* we will need recheck */
			*recheck = true;

			/*
			 * Must have all elements in check[] true; no discrimination
			 * against nulls here.  This is because array_contain_compare and
			 * array_eq handle nulls differently ...
			 */
			res = true;
			for (i = 0; i < nkeys; i++)
			{
				if (!check[i])
				{
					res = false;
					break;
				}
			}
			break;
		default:
			/* shim: elog(ERROR, "ginarrayconsistent: unknown strategy
			 * number: %d", strategy) -> err flag; must be unreachable */
			PROOF_EREPORT_FLAG(err);
			res = false;
	}

	*recheck_out = recheck_local ? 1 : 0;
	return res ? 1 : 0;
}

/*
 * triconsistent support function
 * (fmgr unwrap shim: PG_GETARG_* -> params,
 * PG_RETURN_GIN_TERNARY_VALUE -> int; body verbatim from ginarrayproc.c
 * ginarraytriconsistent)
 */
int
pga_triconsistent(const signed char *check_in, unsigned strategy, int nkeys,
				  const unsigned char *nullFlags, int *err)
{
	const GinTernaryValue *check = (const GinTernaryValue *) check_in;
	GinTernaryValue res;
	int32		i;

	switch (strategy)
	{
		case GinOverlapStrategy:
			/* must have a match for at least one non-null element */
			res = GIN_FALSE;
			for (i = 0; i < nkeys; i++)
			{
				if (!nullFlags[i])
				{
					if (check[i] == GIN_TRUE)
					{
						res = GIN_TRUE;
						break;
					}
					else if (check[i] == GIN_MAYBE && res == GIN_FALSE)
					{
						res = GIN_MAYBE;
					}
				}
			}
			break;
		case GinContainsStrategy:
			/* must have all elements in check[] true, and no nulls */
			res = GIN_TRUE;
			for (i = 0; i < nkeys; i++)
			{
				if (check[i] == GIN_FALSE || nullFlags[i])
				{
					res = GIN_FALSE;
					break;
				}
				if (check[i] == GIN_MAYBE)
				{
					res = GIN_MAYBE;
				}
			}
			break;
		case GinContainedStrategy:
			/* can't do anything else useful here */
			res = GIN_MAYBE;
			break;
		case GinEqualStrategy:

			/*
			 * Must have all elements in check[] true; no discrimination
			 * against nulls here.  This is because array_contain_compare and
			 * array_eq handle nulls differently ...
			 */
			res = GIN_MAYBE;
			for (i = 0; i < nkeys; i++)
			{
				if (check[i] == GIN_FALSE)
				{
					res = GIN_FALSE;
					break;
				}
			}
			break;
		default:
			/* shim: elog(ERROR, ...) -> err flag; must be unreachable */
			PROOF_EREPORT_FLAG(err);
			res = false;
	}

	return (int) res;
}
