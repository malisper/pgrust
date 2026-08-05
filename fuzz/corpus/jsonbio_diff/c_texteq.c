Datum
texteq(PG_FUNCTION_ARGS)
{
	Oid			collid = PG_GET_COLLATION();
	pg_locale_t mylocale = 0;
	bool		result;

	check_collation_set(collid);

	mylocale = pg_newlocale_from_collation(collid);

	if (mylocale->deterministic)
	{
		Datum		arg1 = PG_GETARG_DATUM(0);
		Datum		arg2 = PG_GETARG_DATUM(1);
		Size		len1,
					len2;

		/*
		 * Since we only care about equality or not-equality, we can avoid all
		 * the expense of strcoll() here, and just do bitwise comparison.  In
		 * fact, we don't even have to do a bitwise comparison if we can show
		 * the lengths of the strings are unequal; which might save us from
		 * having to detoast one or both values.
		 */
		len1 = toast_raw_datum_size(arg1);
		len2 = toast_raw_datum_size(arg2);
		if (len1 != len2)
			result = false;
		else
		{
			text	   *targ1 = DatumGetTextPP(arg1);
			text	   *targ2 = DatumGetTextPP(arg2);

			result = (memcmp(VARDATA_ANY(targ1), VARDATA_ANY(targ2),
							 len1 - VARHDRSZ) == 0);

			PG_FREE_IF_COPY(targ1, 0);
			PG_FREE_IF_COPY(targ2, 1);
		}
	}
	else
	{
		text	   *arg1 = PG_GETARG_TEXT_PP(0);
		text	   *arg2 = PG_GETARG_TEXT_PP(1);

		result = (text_cmp(arg1, arg2, collid) == 0);

		PG_FREE_IF_COPY(arg1, 0);
		PG_FREE_IF_COPY(arg2, 1);
	}

	PG_RETURN_BOOL(result);
}
