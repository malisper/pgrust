/*
 * Vendored PostgreSQL C for the gin_compare_jsonb proof (oid 3480,
 * jsonb-probe family; self-contained — no deps on pg_jsonb.c).
 *
 * Provenance (REL_18_STABLE, fetched 2026-07-30 from
 * https://raw.githubusercontent.com/postgres/postgres/REL_18_STABLE/):
 *   - src/backend/utils/adt/jsonb_gin.c: gin_compare_jsonb (fmgr body,
 *     extracted per the shim rules below)
 *   - src/backend/utils/adt/varlena.c: varstr_cmp, the collate_is_c arm
 *     (see shim 2)
 *
 * SHIMS (everything else verbatim):
 *   1. fmgr unwrapping: PG_GETARG_TEXT_PP / VARDATA_ANY / VARSIZE_ANY_EXHDR
 *      -> plain (const unsigned char *data, int len) pairs (pre-detoasted
 *      payload fence, bytea-cmp/text-cmp precedent); PG_RETURN_INT32 -> int.
 *   2. varstr_cmp: gin_compare_jsonb hardwires C_COLLATION_OID, and
 *      pg_newlocale_from_collation(C_COLLATION_OID)->collate_is_c is true
 *      by definition (varlena.c "always using C collation" comment), so the
 *      vendored pg_varstr_cmp_c is varstr_cmp's collate_is_c arm VERBATIM;
 *      the locale-cache lookup machinery (check_collation_set /
 *      pg_newlocale_from_collation) is out of scope. Same projection the
 *      text-cmp family ratified for bttextcmp@COLL_C.
 *   3. memcmp is CBMC's built-in model (byte loop returning the difference
 *      of the first mismatching unsigned chars — the glibc convention the
 *      Rust side mirrors exactly; text-cmp precedent asserts the EXACT
 *      int32 value on the same grounds).
 */

#include <string.h>
#include "../../support/c/pg_proof_shim.h"

/* varlena.c varstr_cmp, collate_is_c arm (verbatim; see shim 2) */
static int
pg_varstr_cmp_c(const unsigned char *arg1, int len1,
				const unsigned char *arg2, int len2)
{
	int			result;

	result = memcmp(arg1, arg2, Min(len1, len2));
	if ((result == 0) && (len1 != len2))
		result = (len1 < len2) ? -1 : 1;

	return result;
}

/*
 * jsonb_gin.c gin_compare_jsonb body (fmgr unwrap per shim 1; the
 * varstr_cmp call site keeps its verbatim argument shape).
 */
int
pg_gin_compare_jsonb(const unsigned char *a1p, int len1,
					 const unsigned char *a2p, int len2)
{
	int32		result;

	/* Compare text as bttextcmp does, but always using C collation */
	result = pg_varstr_cmp_c(a1p, len1, a2p, len2);

	return result;
}
