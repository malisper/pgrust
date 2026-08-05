/*
 * pg_adt_misc.c — vendored PostgreSQL misc.c rows (wave-2).
 *
 * Provenance: src/backend/utils/adt/misc.c, REL_18_STABLE, fetched
 * 2026-07-30. Functions: count_nulls (separate-arguments arm) +
 * pg_num_nulls (438) / pg_num_nonnulls (440), any_value_transfn (6292).
 * Bodies verbatim except the shims below.
 *
 * Shims (plumbing only, never logic):
 *  - fmgr unwrap: PG_NARGS()/PG_ARGISNULL(i) -> (int nargs, const int
 *    *argnull) parameters; PG_RETURN_NULL -> *isnull out-flag.
 *  - get_fn_expr_variadic(fcinfo->flinfo) -> `variadic` parameter. The
 *    harness plane is the SEPARATE-ARGUMENTS surface (flinfo->fn_expr NULL
 *    on both sides => false): the VARIADIC-array arm is replaced by an
 *    OUT-OF-PLANE TRAP (return 99) so reaching it fails loudly instead of
 *    passing vacuously. The array arm (detoast + nullbitmap walk) is a
 *    separate future surface, documented in the ledger rows.
 *  - any_value_transfn: PG_RETURN_DATUM(PG_GETARG_DATUM(0)) -> uint64
 *    datum passthrough. The aggregate null protocol (strict transfn) is
 *    fmgr surface on the C side, out of this body; the harness fences to
 *    the non-null-arg plane and documents the fence.
 */

#include "../../support/c/pg_proof_shim.h"

/*
 * count_nulls()
 *	Count the number of NULL arguments (separate-arguments arm verbatim;
 *	VARIADIC arm -> trap 99, see header).
 */
static int
pg_count_nulls(int variadic, int nargs, const int *argnull,
			   int32 *out_nargs, int32 *out_nulls, int *isnull)
{
	int32		count = 0;
	int			i;

	/* Did we get a VARIADIC array argument, or separate arguments? */
	if (variadic)
		return 99;				/* OUT-OF-PLANE TRAP: array arm */

	{
		/* Separate arguments, so just count 'em */
		for (i = 0; i < nargs; i++)
		{
			if (argnull[i])
				count++;
		}

		*out_nargs = nargs;
		*out_nulls = count;
	}

	return 0;					/* true */
}

int
pg_num_nulls(int variadic, int nargs, const int *argnull,
			 int32 *out, int *isnull)
{
	int32		nargs_,
				nulls;
	int			rc;

	rc = pg_count_nulls(variadic, nargs, argnull, &nargs_, &nulls, isnull);
	if (rc == 99)
		return 99;
	if (rc != 0)				/* count_nulls returned false */
	{
		*isnull = 1;			/* PG_RETURN_NULL() */
		return 0;
	}

	*out = nulls;
	return 0;
}

int
pg_num_nonnulls(int variadic, int nargs, const int *argnull,
				int32 *out, int *isnull)
{
	int32		nargs_,
				nulls;
	int			rc;

	rc = pg_count_nulls(variadic, nargs, argnull, &nargs_, &nulls, isnull);
	if (rc == 99)
		return 99;
	if (rc != 0)
	{
		*isnull = 1;			/* PG_RETURN_NULL() */
		return 0;
	}

	*out = nargs_ - nulls;
	return 0;
}

/*
 * Transition function for the ANY_VALUE aggregate
 */
int
pg_any_value_transfn(uint64 arg0, uint64 *out)
{
	*out = arg0;				/* PG_RETURN_DATUM(PG_GETARG_DATUM(0)) */
	return 0;
}
