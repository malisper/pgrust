Datum
numeric_to_number(PG_FUNCTION_ARGS)
{
	text	   *value = PG_GETARG_TEXT_PP(0);
	text	   *fmt = PG_GETARG_TEXT_PP(1);
	NUMDesc		Num;
	Datum		result;
	FormatNode *format;
	char	   *numstr;
	bool		shouldFree;
	int			len = 0;
	int			scale,
				precision;

	len = VARSIZE_ANY_EXHDR(fmt);

	if (len <= 0 || len >= INT_MAX / NUM_MAX_ITEM_SIZ)
		PG_RETURN_NULL();

	format = NUM_cache(len, &Num, fmt, &shouldFree);

	numstr = (char *) palloc((len * NUM_MAX_ITEM_SIZ) + 1);

	NUM_processor(format, &Num, VARDATA_ANY(value), numstr,
				  VARSIZE_ANY_EXHDR(value), 0, 0, false, PG_GET_COLLATION());

	scale = Num.post;
	precision = Num.pre + Num.multi + scale;

	if (shouldFree)
		pfree(format);

	result = DirectFunctionCall3(numeric_in,
								 CStringGetDatum(numstr),
								 ObjectIdGetDatum(InvalidOid),
								 Int32GetDatum(((precision << 16) | scale) + VARHDRSZ));

	if (IS_MULTI(&Num))
	{
		Numeric		x;
		Numeric		a = int64_to_numeric(10);
		Numeric		b = int64_to_numeric(-Num.multi);

		x = DatumGetNumeric(DirectFunctionCall2(numeric_power,
												NumericGetDatum(a),
												NumericGetDatum(b)));
		result = DirectFunctionCall2(numeric_mul,
									 result,
									 NumericGetDatum(x));
	}

	pfree(numstr);
	return result;
}

/* ------------------
 * NUMERIC to_char()
 * ------------------
 */
Datum
numeric_to_char(PG_FUNCTION_ARGS)
{
	Numeric		value = PG_GETARG_NUMERIC(0);
	text	   *fmt = PG_GETARG_TEXT_PP(1);
	NUMDesc		Num;
	FormatNode *format;
	text	   *result;
	bool		shouldFree;
	int			out_pre_spaces = 0,
				sign = 0;
	char	   *numstr,
			   *orgnum,
			   *p;

	NUM_TOCHAR_prepare;

	/*
	 * On DateType depend part (numeric)
	 */
	if (IS_ROMAN(&Num))
	{
		int32		intvalue;
		bool		err;

		/* Round and convert to int */
		intvalue = numeric_int4_opt_error(value, &err);
		/* On overflow, just use PG_INT32_MAX; int_to_roman will cope */
		if (err)
			intvalue = PG_INT32_MAX;
		numstr = int_to_roman(intvalue);
	}
	else if (IS_EEEE(&Num))
	{
		orgnum = numeric_out_sci(value, Num.post);

		/*
		 * numeric_out_sci() does not emit a sign for positive numbers.  We
		 * need to add a space in this case so that positive and negative
		 * numbers are aligned.  Also must check for NaN/infinity cases, which
		 * we handle the same way as in float8_to_char.
		 */
		if (strcmp(orgnum, "NaN") == 0 ||
			strcmp(orgnum, "Infinity") == 0 ||
			strcmp(orgnum, "-Infinity") == 0)
		{
			/*
			 * Allow 6 characters for the leading sign, the decimal point,
			 * "e", the exponent's sign and two exponent digits.
			 */
			numstr = (char *) palloc(Num.pre + Num.post + 7);
			fill_str(numstr, '#', Num.pre + Num.post + 6);
			*numstr = ' ';
			*(numstr + Num.pre + 1) = '.';
		}
		else if (*orgnum != '-')
		{
			numstr = (char *) palloc(strlen(orgnum) + 2);
			*numstr = ' ';
			strcpy(numstr + 1, orgnum);
		}
		else
		{
			numstr = orgnum;
		}
	}
	else
	{
		int			numstr_pre_len;
		Numeric		val = value;
		Numeric		x;

		if (IS_MULTI(&Num))
		{
			Numeric		a = int64_to_numeric(10);
			Numeric		b = int64_to_numeric(Num.multi);

			x = DatumGetNumeric(DirectFunctionCall2(numeric_power,
													NumericGetDatum(a),
													NumericGetDatum(b)));
			val = DatumGetNumeric(DirectFunctionCall2(numeric_mul,
													  NumericGetDatum(value),
													  NumericGetDatum(x)));
			Num.pre += Num.multi;
		}

		x = DatumGetNumeric(DirectFunctionCall2(numeric_round,
												NumericGetDatum(val),
												Int32GetDatum(Num.post)));
		orgnum = DatumGetCString(DirectFunctionCall1(numeric_out,
													 NumericGetDatum(x)));

		if (*orgnum == '-')
		{
			sign = '-';
			numstr = orgnum + 1;
		}
		else
		{
			sign = '+';
			numstr = orgnum;
		}

		if ((p = strchr(numstr, '.')))
			numstr_pre_len = p - numstr;
		else
			numstr_pre_len = strlen(numstr);

		/* needs padding? */
		if (numstr_pre_len < Num.pre)
			out_pre_spaces = Num.pre - numstr_pre_len;
		/* overflowed prefix digit format? */
		else if (numstr_pre_len > Num.pre)
		{
			numstr = (char *) palloc(Num.pre + Num.post + 2);
			fill_str(numstr, '#', Num.pre + Num.post + 1);
			*(numstr + Num.pre) = '.';
		}
	}

	NUM_TOCHAR_finish;
	PG_RETURN_TEXT_P(result);
}

/* ---------------
 * INT4 to_char()
 * ---------------
 */
Datum
int4_to_char(PG_FUNCTION_ARGS)
{
	int32		value = PG_GETARG_INT32(0);
	text	   *fmt = PG_GETARG_TEXT_PP(1);
	NUMDesc		Num;
	FormatNode *format;
	text	   *result;
	bool		shouldFree;
	int			out_pre_spaces = 0,
				sign = 0;
	char	   *numstr,
			   *orgnum;

	NUM_TOCHAR_prepare;

	/*
	 * On DateType depend part (int32)
	 */
	if (IS_ROMAN(&Num))
		numstr = int_to_roman(value);
	else if (IS_EEEE(&Num))
	{
		/* we can do it easily because float8 won't lose any precision */
		float8		val = (float8) value;

		orgnum = (char *) psprintf("%+.*e", Num.post, val);

		/*
		 * Swap a leading positive sign for a space.
		 */
		if (*orgnum == '+')
			*orgnum = ' ';

		numstr = orgnum;
	}
	else
	{
		int			numstr_pre_len;

		if (IS_MULTI(&Num))
		{
			orgnum = DatumGetCString(DirectFunctionCall1(int4out,
														 Int32GetDatum(value * ((int32) pow((double) 10, (double) Num.multi)))));
			Num.pre += Num.multi;
		}
		else
		{
			orgnum = DatumGetCString(DirectFunctionCall1(int4out,
														 Int32GetDatum(value)));
		}

		if (*orgnum == '-')
		{
			sign = '-';
			orgnum++;
		}
		else
			sign = '+';

		numstr_pre_len = strlen(orgnum);

		/* post-decimal digits?  Pad out with zeros. */
		if (Num.post)
		{
			numstr = (char *) palloc(numstr_pre_len + Num.post + 2);
			strcpy(numstr, orgnum);
			*(numstr + numstr_pre_len) = '.';
			memset(numstr + numstr_pre_len + 1, '0', Num.post);
			*(numstr + numstr_pre_len + Num.post + 1) = '\0';
		}
		else
			numstr = orgnum;

		/* needs padding? */
		if (numstr_pre_len < Num.pre)
			out_pre_spaces = Num.pre - numstr_pre_len;
		/* overflowed prefix digit format? */
		else if (numstr_pre_len > Num.pre)
		{
			numstr = (char *) palloc(Num.pre + Num.post + 2);
			fill_str(numstr, '#', Num.pre + Num.post + 1);
			*(numstr + Num.pre) = '.';
		}
	}

	NUM_TOCHAR_finish;
	PG_RETURN_TEXT_P(result);
}

/* ---------------
 * INT8 to_char()
 * ---------------
 */
Datum
int8_to_char(PG_FUNCTION_ARGS)
{
	int64		value = PG_GETARG_INT64(0);
	text	   *fmt = PG_GETARG_TEXT_PP(1);
	NUMDesc		Num;
	FormatNode *format;
	text	   *result;
	bool		shouldFree;
	int			out_pre_spaces = 0,
				sign = 0;
	char	   *numstr,
			   *orgnum;

	NUM_TOCHAR_prepare;

	/*
	 * On DateType depend part (int64)
	 */
	if (IS_ROMAN(&Num))
	{
		int32		intvalue;

		/* On overflow, just use PG_INT32_MAX; int_to_roman will cope */
		if (value <= PG_INT32_MAX && value >= PG_INT32_MIN)
			intvalue = (int32) value;
		else
			intvalue = PG_INT32_MAX;
		numstr = int_to_roman(intvalue);
	}
	else if (IS_EEEE(&Num))
	{
		/* to avoid loss of precision, must go via numeric not float8 */
		orgnum = numeric_out_sci(int64_to_numeric(value),
								 Num.post);

		/*
		 * numeric_out_sci() does not emit a sign for positive numbers.  We
		 * need to add a space in this case so that positive and negative
		 * numbers are aligned.  We don't have to worry about NaN/inf here.
		 */
		if (*orgnum != '-')
		{
			numstr = (char *) palloc(strlen(orgnum) + 2);
			*numstr = ' ';
			strcpy(numstr + 1, orgnum);
		}
		else
		{
			numstr = orgnum;
		}
	}
	else
	{
		int			numstr_pre_len;

		if (IS_MULTI(&Num))
		{
			double		multi = pow((double) 10, (double) Num.multi);

			value = DatumGetInt64(DirectFunctionCall2(int8mul,
													  Int64GetDatum(value),
													  DirectFunctionCall1(dtoi8,
																		  Float8GetDatum(multi))));
			Num.pre += Num.multi;
		}

		orgnum = DatumGetCString(DirectFunctionCall1(int8out,
													 Int64GetDatum(value)));

		if (*orgnum == '-')
		{
			sign = '-';
			orgnum++;
		}
		else
			sign = '+';

		numstr_pre_len = strlen(orgnum);

		/* post-decimal digits?  Pad out with zeros. */
		if (Num.post)
		{
			numstr = (char *) palloc(numstr_pre_len + Num.post + 2);
			strcpy(numstr, orgnum);
			*(numstr + numstr_pre_len) = '.';
			memset(numstr + numstr_pre_len + 1, '0', Num.post);
			*(numstr + numstr_pre_len + Num.post + 1) = '\0';
		}
		else
			numstr = orgnum;

		/* needs padding? */
		if (numstr_pre_len < Num.pre)
			out_pre_spaces = Num.pre - numstr_pre_len;
		/* overflowed prefix digit format? */
		else if (numstr_pre_len > Num.pre)
		{
			numstr = (char *) palloc(Num.pre + Num.post + 2);
			fill_str(numstr, '#', Num.pre + Num.post + 1);
			*(numstr + Num.pre) = '.';
		}
	}

	NUM_TOCHAR_finish;
	PG_RETURN_TEXT_P(result);
}

/* -----------------
 * FLOAT4 to_char()
 * -----------------
 */
Datum
float4_to_char(PG_FUNCTION_ARGS)
{
	float4		value = PG_GETARG_FLOAT4(0);
	text	   *fmt = PG_GETARG_TEXT_PP(1);
	NUMDesc		Num;
	FormatNode *format;
	text	   *result;
	bool		shouldFree;
	int			out_pre_spaces = 0,
				sign = 0;
	char	   *numstr,
			   *p;

	NUM_TOCHAR_prepare;

	if (IS_ROMAN(&Num))
	{
		int32		intvalue;

		/* See notes in ftoi4() */
		value = rint(value);
		/* On overflow, just use PG_INT32_MAX; int_to_roman will cope */
		if (!isnan(value) && FLOAT4_FITS_IN_INT32(value))
			intvalue = (int32) value;
		else
			intvalue = PG_INT32_MAX;
		numstr = int_to_roman(intvalue);
	}
	else if (IS_EEEE(&Num))
	{
		if (isnan(value) || isinf(value))
		{
			/*
			 * Allow 6 characters for the leading sign, the decimal point,
			 * "e", the exponent's sign and two exponent digits.
			 */
			numstr = (char *) palloc(Num.pre + Num.post + 7);
			fill_str(numstr, '#', Num.pre + Num.post + 6);
			*numstr = ' ';
			*(numstr + Num.pre + 1) = '.';
		}
		else
		{
			numstr = psprintf("%+.*e", Num.post, value);

			/*
			 * Swap a leading positive sign for a space.
			 */
			if (*numstr == '+')
				*numstr = ' ';
		}
	}
	else
	{
		float4		val = value;
		char	   *orgnum;
		int			numstr_pre_len;

		if (IS_MULTI(&Num))
		{
			float		multi = pow((double) 10, (double) Num.multi);

			val = value * multi;
			Num.pre += Num.multi;
		}

		orgnum = psprintf("%.0f", fabs(val));
		numstr_pre_len = strlen(orgnum);

		/* adjust post digits to fit max float digits */
		if (numstr_pre_len >= FLT_DIG)
			Num.post = 0;
		else if (numstr_pre_len + Num.post > FLT_DIG)
			Num.post = FLT_DIG - numstr_pre_len;
		orgnum = psprintf("%.*f", Num.post, val);

		if (*orgnum == '-')
		{						/* < 0 */
			sign = '-';
			numstr = orgnum + 1;
		}
		else
		{
			sign = '+';
			numstr = orgnum;
		}

		if ((p = strchr(numstr, '.')))
			numstr_pre_len = p - numstr;
		else
			numstr_pre_len = strlen(numstr);

		/* needs padding? */
		if (numstr_pre_len < Num.pre)
			out_pre_spaces = Num.pre - numstr_pre_len;
		/* overflowed prefix digit format? */
		else if (numstr_pre_len > Num.pre)
		{
			numstr = (char *) palloc(Num.pre + Num.post + 2);
			fill_str(numstr, '#', Num.pre + Num.post + 1);
			*(numstr + Num.pre) = '.';
		}
	}

	NUM_TOCHAR_finish;
	PG_RETURN_TEXT_P(result);
}

/* -----------------
 * FLOAT8 to_char()
 * -----------------
 */
Datum
float8_to_char(PG_FUNCTION_ARGS)
{
	float8		value = PG_GETARG_FLOAT8(0);
	text	   *fmt = PG_GETARG_TEXT_PP(1);
	NUMDesc		Num;
	FormatNode *format;
	text	   *result;
	bool		shouldFree;
	int			out_pre_spaces = 0,
				sign = 0;
	char	   *numstr,
			   *p;

	NUM_TOCHAR_prepare;

	if (IS_ROMAN(&Num))
	{
		int32		intvalue;

		/* See notes in dtoi4() */
		value = rint(value);
		/* On overflow, just use PG_INT32_MAX; int_to_roman will cope */
		if (!isnan(value) && FLOAT8_FITS_IN_INT32(value))
			intvalue = (int32) value;
		else
			intvalue = PG_INT32_MAX;
		numstr = int_to_roman(intvalue);
	}
	else if (IS_EEEE(&Num))
	{
		if (isnan(value) || isinf(value))
		{
			/*
			 * Allow 6 characters for the leading sign, the decimal point,
			 * "e", the exponent's sign and two exponent digits.
			 */
			numstr = (char *) palloc(Num.pre + Num.post + 7);
			fill_str(numstr, '#', Num.pre + Num.post + 6);
			*numstr = ' ';
			*(numstr + Num.pre + 1) = '.';
		}
		else
		{
			numstr = psprintf("%+.*e", Num.post, value);

			/*
			 * Swap a leading positive sign for a space.
			 */
			if (*numstr == '+')
				*numstr = ' ';
		}
	}
	else
	{
		float8		val = value;
		char	   *orgnum;
		int			numstr_pre_len;

		if (IS_MULTI(&Num))
		{
			double		multi = pow((double) 10, (double) Num.multi);

			val = value * multi;
			Num.pre += Num.multi;
		}

		orgnum = psprintf("%.0f", fabs(val));
		numstr_pre_len = strlen(orgnum);

		/* adjust post digits to fit max double digits */
		if (numstr_pre_len >= DBL_DIG)
			Num.post = 0;
		else if (numstr_pre_len + Num.post > DBL_DIG)
			Num.post = DBL_DIG - numstr_pre_len;
		orgnum = psprintf("%.*f", Num.post, val);

		if (*orgnum == '-')
		{						/* < 0 */
			sign = '-';
			numstr = orgnum + 1;
		}
		else
		{
			sign = '+';
			numstr = orgnum;
		}

		if ((p = strchr(numstr, '.')))
			numstr_pre_len = p - numstr;
		else
			numstr_pre_len = strlen(numstr);

		/* needs padding? */
		if (numstr_pre_len < Num.pre)
			out_pre_spaces = Num.pre - numstr_pre_len;
		/* overflowed prefix digit format? */
		else if (numstr_pre_len > Num.pre)
		{
			numstr = (char *) palloc(Num.pre + Num.post + 2);
			fill_str(numstr, '#', Num.pre + Num.post + 1);
			*(numstr + Num.pre) = '.';
		}
	}

	NUM_TOCHAR_finish;
	PG_RETURN_TEXT_P(result);
}
