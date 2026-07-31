/* VERBATIM from src/backend/utils/adt/varlena.c @ 62d6c7d3df (18.3):
 * check_collation_set + varstr_cmp. Environment pin: the shim
 * pg_newlocale_from_collation (shim/utils/pg_locale.h) reports
 * collate_is_c=true for every collation this family passes
 * (DEFAULT_COLLATION_OID from compareJsonbScalarValue) — the C-collation
 * database pin, mirrored on the Rust side by installing
 * pg_locale_seams::varstr_cmp_locale = varstrfastcmp_c. The non-C
 * pg_strncoll arm is compiled verbatim but unreachable (abort-loud shim).
 */

/* --- varlena.c check_collation_set (lines 1636-1650) --- */
static void
check_collation_set(Oid collid)
{
	if (!OidIsValid(collid))
	{
		/*
		 * This typically means that the parser could not resolve a conflict
		 * of implicit collations, so report it that way.
		 */
		ereport(ERROR,
				(errcode(ERRCODE_INDETERMINATE_COLLATION),
				 errmsg("could not determine which collation to use for string comparison"),
				 errhint("Use the COLLATE clause to set the collation explicitly.")));
	}
}

/* --- varlena.c varstr_cmp (lines 1652-1707) --- */
/*
 * varstr_cmp()
 *
 * Comparison function for text strings with given lengths, using the
 * appropriate locale. Returns an integer less than, equal to, or greater than
 * zero, indicating whether arg1 is less than, equal to, or greater than arg2.
 *
 * Note: many functions that depend on this are marked leakproof; therefore,
 * avoid reporting the actual contents of the input when throwing errors.
 * All errors herein should be things that can't happen except on corrupt
 * data, anyway; otherwise we will have trouble with indexing strings that
 * would cause them.
 */
int
varstr_cmp(const char *arg1, int len1, const char *arg2, int len2, Oid collid)
{
	int			result;
	pg_locale_t mylocale;

	check_collation_set(collid);

	mylocale = pg_newlocale_from_collation(collid);

	if (mylocale->collate_is_c)
	{
		result = memcmp(arg1, arg2, Min(len1, len2));
		if ((result == 0) && (len1 != len2))
			result = (len1 < len2) ? -1 : 1;
	}
	else
	{
		/*
		 * memcmp() can't tell us which of two unequal strings sorts first,
		 * but it's a cheap way to tell if they're equal.  Testing shows that
		 * memcmp() followed by strcoll() is only trivially slower than
		 * strcoll() by itself, so we don't lose much if this doesn't work out
		 * very often, and if it does - for example, because there are many
		 * equal strings in the input - then we win big by avoiding expensive
		 * collation-aware comparisons.
		 */
		if (len1 == len2 && memcmp(arg1, arg2, len1) == 0)
			return 0;

		result = pg_strncoll(arg1, len1, arg2, len2, mylocale);

		/* Break tie if necessary. */
		if (result == 0 && mylocale->deterministic)
		{
			result = memcmp(arg1, arg2, Min(len1, len2));
			if ((result == 0) && (len1 != len2))
				result = (len1 < len2) ? -1 : 1;
		}
	}

	return result;
}
