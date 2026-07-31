/* VERBATIM bodies vendored from postgres-src @ 62d6c7d3df6287f1bd83199c1a746e50d31571a0
 * for the fmt_num_diff oracle (NUM SQL entries' numeric/int/float dependency closure).
 * Each block byte-identical to the cited file:lines. Environment = pg_fmt_dch_io.c prelude.
 */

/* --- src/backend/utils/adt/numeric.c:636-807 numeric_in (verbatim) --- */
Datum
numeric_in(PG_FUNCTION_ARGS)
{
	char	   *str = PG_GETARG_CSTRING(0);
#ifdef NOT_USED
	Oid			typelem = PG_GETARG_OID(1);
#endif
	int32		typmod = PG_GETARG_INT32(2);
	Node	   *escontext = fcinfo->context;
	Numeric		res;
	const char *cp;
	const char *numstart;
	int			sign;

	/* Skip leading spaces */
	cp = str;
	while (*cp)
	{
		if (!isspace((unsigned char) *cp))
			break;
		cp++;
	}

	/*
	 * Process the number's sign. This duplicates logic in set_var_from_str(),
	 * but it's worth doing here, since it simplifies the handling of
	 * infinities and non-decimal integers.
	 */
	numstart = cp;
	sign = NUMERIC_POS;

	if (*cp == '+')
		cp++;
	else if (*cp == '-')
	{
		sign = NUMERIC_NEG;
		cp++;
	}

	/*
	 * Check for NaN and infinities.  We recognize the same strings allowed by
	 * float8in().
	 *
	 * Since all other legal inputs have a digit or a decimal point after the
	 * sign, we need only check for NaN/infinity if that's not the case.
	 */
	if (!isdigit((unsigned char) *cp) && *cp != '.')
	{
		/*
		 * The number must be NaN or infinity; anything else can only be a
		 * syntax error. Note that NaN mustn't have a sign.
		 */
		if (pg_strncasecmp(numstart, "NaN", 3) == 0)
		{
			res = make_result(&const_nan);
			cp = numstart + 3;
		}
		else if (pg_strncasecmp(cp, "Infinity", 8) == 0)
		{
			res = make_result(sign == NUMERIC_POS ? &const_pinf : &const_ninf);
			cp += 8;
		}
		else if (pg_strncasecmp(cp, "inf", 3) == 0)
		{
			res = make_result(sign == NUMERIC_POS ? &const_pinf : &const_ninf);
			cp += 3;
		}
		else
			goto invalid_syntax;

		/*
		 * Check for trailing junk; there should be nothing left but spaces.
		 *
		 * We intentionally do this check before applying the typmod because
		 * we would like to throw any trailing-junk syntax error before any
		 * semantic error resulting from apply_typmod_special().
		 */
		while (*cp)
		{
			if (!isspace((unsigned char) *cp))
				goto invalid_syntax;
			cp++;
		}

		if (!apply_typmod_special(res, typmod, escontext))
			PG_RETURN_NULL();
	}
	else
	{
		/*
		 * We have a normal numeric value, which may be a non-decimal integer
		 * or a regular decimal number.
		 */
		NumericVar	value;
		int			base;
		bool		have_error;

		init_var(&value);

		/*
		 * Determine the number's base by looking for a non-decimal prefix
		 * indicator ("0x", "0o", or "0b").
		 */
		if (cp[0] == '0')
		{
			switch (cp[1])
			{
				case 'x':
				case 'X':
					base = 16;
					break;
				case 'o':
				case 'O':
					base = 8;
					break;
				case 'b':
				case 'B':
					base = 2;
					break;
				default:
					base = 10;
			}
		}
		else
			base = 10;

		/* Parse the rest of the number and apply the sign */
		if (base == 10)
		{
			if (!set_var_from_str(str, cp, &value, &cp, escontext))
				PG_RETURN_NULL();
			value.sign = sign;
		}
		else
		{
			if (!set_var_from_non_decimal_integer_str(str, cp + 2, sign, base,
													  &value, &cp, escontext))
				PG_RETURN_NULL();
		}

		/*
		 * Should be nothing left but spaces. As above, throw any typmod error
		 * after finishing syntax check.
		 */
		while (*cp)
		{
			if (!isspace((unsigned char) *cp))
				goto invalid_syntax;
			cp++;
		}

		if (!apply_typmod(&value, typmod, escontext))
			PG_RETURN_NULL();

		res = make_result_opt_error(&value, &have_error);

		if (have_error)
			ereturn(escontext, (Datum) 0,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					 errmsg("value overflows numeric format")));

		free_var(&value);
	}

	PG_RETURN_NUMERIC(res);

invalid_syntax:
	ereturn(escontext, (Datum) 0,
			(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
			 errmsg("invalid input syntax for type %s: \"%s\"",
					"numeric", str)));
}

/* --- src/backend/utils/adt/numeric.c:815-843 numeric_out (verbatim) --- */
Datum
numeric_out(PG_FUNCTION_ARGS)
{
	Numeric		num = PG_GETARG_NUMERIC(0);
	NumericVar	x;
	char	   *str;

	/*
	 * Handle NaN and infinities
	 */
	if (NUMERIC_IS_SPECIAL(num))
	{
		if (NUMERIC_IS_PINF(num))
			PG_RETURN_CSTRING(pstrdup("Infinity"));
		else if (NUMERIC_IS_NINF(num))
			PG_RETURN_CSTRING(pstrdup("-Infinity"));
		else
			PG_RETURN_CSTRING(pstrdup("NaN"));
	}

	/*
	 * Get the number in the variable format.
	 */
	init_var_from_num(num, &x);

	str = get_str_from_var(&x);

	PG_RETURN_CSTRING(str);
}

/* --- src/backend/utils/adt/numeric.c:991-1015 numeric_out_sci (verbatim) --- */
char *
numeric_out_sci(Numeric num, int scale)
{
	NumericVar	x;
	char	   *str;

	/*
	 * Handle NaN and infinities
	 */
	if (NUMERIC_IS_SPECIAL(num))
	{
		if (NUMERIC_IS_PINF(num))
			return pstrdup("Infinity");
		else if (NUMERIC_IS_NINF(num))
			return pstrdup("-Infinity");
		else
			return pstrdup("NaN");
	}

	init_var_from_num(num, &x);

	str = get_str_from_var_sci(&x, scale);

	return str;
}

/* --- src/backend/utils/adt/numeric.c:1542-1586 numeric_round (verbatim) --- */
Datum
numeric_round(PG_FUNCTION_ARGS)
{
	Numeric		num = PG_GETARG_NUMERIC(0);
	int32		scale = PG_GETARG_INT32(1);
	Numeric		res;
	NumericVar	arg;

	/*
	 * Handle NaN and infinities
	 */
	if (NUMERIC_IS_SPECIAL(num))
		PG_RETURN_NUMERIC(duplicate_numeric(num));

	/*
	 * Limit the scale value to avoid possible overflow in calculations.
	 *
	 * These limits are based on the maximum number of digits a Numeric value
	 * can have before and after the decimal point, but we must allow for one
	 * extra digit before the decimal point, in case the most significant
	 * digit rounds up; we must check if that causes Numeric overflow.
	 */
	scale = Max(scale, -(NUMERIC_WEIGHT_MAX + 1) * DEC_DIGITS - 1);
	scale = Min(scale, NUMERIC_DSCALE_MAX);

	/*
	 * Unpack the argument and round it at the proper digit position
	 */
	init_var(&arg);
	set_var_from_num(num, &arg);

	round_var(&arg, scale);

	/* We don't allow negative output dscale */
	if (scale < 0)
		arg.dscale = 0;

	/*
	 * Return the rounded result
	 */
	res = make_result(&arg);

	free_var(&arg);
	PG_RETURN_NUMERIC(res);
}

/* --- src/backend/utils/adt/numeric.c:3121-3131 numeric_mul (verbatim) --- */
Datum
numeric_mul(PG_FUNCTION_ARGS)
{
	Numeric		num1 = PG_GETARG_NUMERIC(0);
	Numeric		num2 = PG_GETARG_NUMERIC(1);
	Numeric		res;

	res = numeric_mul_opt_error(num1, num2, NULL);

	PG_RETURN_NUMERIC(res);
}

/* --- src/backend/utils/adt/numeric.c:3141-3234 numeric_mul_opt_error (verbatim) --- */
Numeric
numeric_mul_opt_error(Numeric num1, Numeric num2, bool *have_error)
{
	NumericVar	arg1;
	NumericVar	arg2;
	NumericVar	result;
	Numeric		res;

	/*
	 * Handle NaN and infinities
	 */
	if (NUMERIC_IS_SPECIAL(num1) || NUMERIC_IS_SPECIAL(num2))
	{
		if (NUMERIC_IS_NAN(num1) || NUMERIC_IS_NAN(num2))
			return make_result(&const_nan);
		if (NUMERIC_IS_PINF(num1))
		{
			switch (numeric_sign_internal(num2))
			{
				case 0:
					return make_result(&const_nan); /* Inf * 0 */
				case 1:
					return make_result(&const_pinf);
				case -1:
					return make_result(&const_ninf);
			}
			Assert(false);
		}
		if (NUMERIC_IS_NINF(num1))
		{
			switch (numeric_sign_internal(num2))
			{
				case 0:
					return make_result(&const_nan); /* -Inf * 0 */
				case 1:
					return make_result(&const_ninf);
				case -1:
					return make_result(&const_pinf);
			}
			Assert(false);
		}
		/* by here, num1 must be finite, so num2 is not */
		if (NUMERIC_IS_PINF(num2))
		{
			switch (numeric_sign_internal(num1))
			{
				case 0:
					return make_result(&const_nan); /* 0 * Inf */
				case 1:
					return make_result(&const_pinf);
				case -1:
					return make_result(&const_ninf);
			}
			Assert(false);
		}
		Assert(NUMERIC_IS_NINF(num2));
		switch (numeric_sign_internal(num1))
		{
			case 0:
				return make_result(&const_nan); /* 0 * -Inf */
			case 1:
				return make_result(&const_ninf);
			case -1:
				return make_result(&const_pinf);
		}
		Assert(false);
	}

	/*
	 * Unpack the values, let mul_var() compute the result and return it.
	 * Unlike add_var() and sub_var(), mul_var() will round its result. In the
	 * case of numeric_mul(), which is invoked for the * operator on numerics,
	 * we request exact representation for the product (rscale = sum(dscale of
	 * arg1, dscale of arg2)).  If the exact result has more digits after the
	 * decimal point than can be stored in a numeric, we round it.  Rounding
	 * after computing the exact result ensures that the final result is
	 * correctly rounded (rounding in mul_var() using a truncated product
	 * would not guarantee this).
	 */
	init_var_from_num(num1, &arg1);
	init_var_from_num(num2, &arg2);

	init_var(&result);
	mul_var(&arg1, &arg2, &result, arg1.dscale + arg2.dscale);

	if (result.dscale > NUMERIC_DSCALE_MAX)
		round_var(&result, NUMERIC_DSCALE_MAX);

	res = make_result_opt_error(&result, have_error);

	free_var(&result);

	return res;
}

/* --- src/backend/utils/adt/numeric.c:4053-4233 numeric_power (verbatim) --- */
Datum
numeric_power(PG_FUNCTION_ARGS)
{
	Numeric		num1 = PG_GETARG_NUMERIC(0);
	Numeric		num2 = PG_GETARG_NUMERIC(1);
	Numeric		res;
	NumericVar	arg1;
	NumericVar	arg2;
	NumericVar	result;
	int			sign1,
				sign2;

	/*
	 * Handle NaN and infinities
	 */
	if (NUMERIC_IS_SPECIAL(num1) || NUMERIC_IS_SPECIAL(num2))
	{
		/*
		 * We follow the POSIX spec for pow(3), which says that NaN ^ 0 = 1,
		 * and 1 ^ NaN = 1, while all other cases with NaN inputs yield NaN
		 * (with no error).
		 */
		if (NUMERIC_IS_NAN(num1))
		{
			if (!NUMERIC_IS_SPECIAL(num2))
			{
				init_var_from_num(num2, &arg2);
				if (cmp_var(&arg2, &const_zero) == 0)
					PG_RETURN_NUMERIC(make_result(&const_one));
			}
			PG_RETURN_NUMERIC(make_result(&const_nan));
		}
		if (NUMERIC_IS_NAN(num2))
		{
			if (!NUMERIC_IS_SPECIAL(num1))
			{
				init_var_from_num(num1, &arg1);
				if (cmp_var(&arg1, &const_one) == 0)
					PG_RETURN_NUMERIC(make_result(&const_one));
			}
			PG_RETURN_NUMERIC(make_result(&const_nan));
		}
		/* At least one input is infinite, but error rules still apply */
		sign1 = numeric_sign_internal(num1);
		sign2 = numeric_sign_internal(num2);
		if (sign1 == 0 && sign2 < 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION),
					 errmsg("zero raised to a negative power is undefined")));
		if (sign1 < 0 && !numeric_is_integral(num2))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION),
					 errmsg("a negative number raised to a non-integer power yields a complex result")));

		/*
		 * POSIX gives this series of rules for pow(3) with infinite inputs:
		 *
		 * For any value of y, if x is +1, 1.0 shall be returned.
		 */
		if (!NUMERIC_IS_SPECIAL(num1))
		{
			init_var_from_num(num1, &arg1);
			if (cmp_var(&arg1, &const_one) == 0)
				PG_RETURN_NUMERIC(make_result(&const_one));
		}

		/*
		 * For any value of x, if y is [-]0, 1.0 shall be returned.
		 */
		if (sign2 == 0)
			PG_RETURN_NUMERIC(make_result(&const_one));

		/*
		 * For any odd integer value of y > 0, if x is [-]0, [-]0 shall be
		 * returned.  For y > 0 and not an odd integer, if x is [-]0, +0 shall
		 * be returned.  (Since we don't deal in minus zero, we need not
		 * distinguish these two cases.)
		 */
		if (sign1 == 0 && sign2 > 0)
			PG_RETURN_NUMERIC(make_result(&const_zero));

		/*
		 * If x is -1, and y is [-]Inf, 1.0 shall be returned.
		 *
		 * For |x| < 1, if y is -Inf, +Inf shall be returned.
		 *
		 * For |x| > 1, if y is -Inf, +0 shall be returned.
		 *
		 * For |x| < 1, if y is +Inf, +0 shall be returned.
		 *
		 * For |x| > 1, if y is +Inf, +Inf shall be returned.
		 */
		if (NUMERIC_IS_INF(num2))
		{
			bool		abs_x_gt_one;

			if (NUMERIC_IS_SPECIAL(num1))
				abs_x_gt_one = true;	/* x is either Inf or -Inf */
			else
			{
				init_var_from_num(num1, &arg1);
				if (cmp_var(&arg1, &const_minus_one) == 0)
					PG_RETURN_NUMERIC(make_result(&const_one));
				arg1.sign = NUMERIC_POS;	/* now arg1 = abs(x) */
				abs_x_gt_one = (cmp_var(&arg1, &const_one) > 0);
			}
			if (abs_x_gt_one == (sign2 > 0))
				PG_RETURN_NUMERIC(make_result(&const_pinf));
			else
				PG_RETURN_NUMERIC(make_result(&const_zero));
		}

		/*
		 * For y < 0, if x is +Inf, +0 shall be returned.
		 *
		 * For y > 0, if x is +Inf, +Inf shall be returned.
		 */
		if (NUMERIC_IS_PINF(num1))
		{
			if (sign2 > 0)
				PG_RETURN_NUMERIC(make_result(&const_pinf));
			else
				PG_RETURN_NUMERIC(make_result(&const_zero));
		}

		Assert(NUMERIC_IS_NINF(num1));

		/*
		 * For y an odd integer < 0, if x is -Inf, -0 shall be returned.  For
		 * y < 0 and not an odd integer, if x is -Inf, +0 shall be returned.
		 * (Again, we need not distinguish these two cases.)
		 */
		if (sign2 < 0)
			PG_RETURN_NUMERIC(make_result(&const_zero));

		/*
		 * For y an odd integer > 0, if x is -Inf, -Inf shall be returned. For
		 * y > 0 and not an odd integer, if x is -Inf, +Inf shall be returned.
		 */
		init_var_from_num(num2, &arg2);
		if (arg2.ndigits > 0 && arg2.ndigits == arg2.weight + 1 &&
			(arg2.digits[arg2.ndigits - 1] & 1))
			PG_RETURN_NUMERIC(make_result(&const_ninf));
		else
			PG_RETURN_NUMERIC(make_result(&const_pinf));
	}

	/*
	 * The SQL spec requires that we emit a particular SQLSTATE error code for
	 * certain error conditions.  Specifically, we don't return a
	 * divide-by-zero error code for 0 ^ -1.  Raising a negative number to a
	 * non-integer power must produce the same error code, but that case is
	 * handled in power_var().
	 */
	sign1 = numeric_sign_internal(num1);
	sign2 = numeric_sign_internal(num2);

	if (sign1 == 0 && sign2 < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION),
				 errmsg("zero raised to a negative power is undefined")));

	/*
	 * Initialize things
	 */
	init_var(&result);
	init_var_from_num(num1, &arg1);
	init_var_from_num(num2, &arg2);

	/*
	 * Call power_var() to compute and return the result; note it handles
	 * scale selection itself.
	 */
	power_var(&arg1, &arg2, &result);

	res = make_result(&result);

	free_var(&result);

	PG_RETURN_NUMERIC(res);
}

/* --- src/backend/utils/adt/numeric.c:4401-4416 int64_to_numeric (verbatim) --- */
Numeric
int64_to_numeric(int64 val)
{
	Numeric		res;
	NumericVar	result;

	init_var(&result);

	int64_to_numericvar(val, &result);

	res = make_result(&result);

	free_var(&result);

	return res;
}

/* --- src/backend/utils/adt/numeric.c:4515-4563 numeric_int4_opt_error (verbatim) --- */
int32
numeric_int4_opt_error(Numeric num, bool *have_error)
{
	NumericVar	x;
	int32		result;

	if (have_error)
		*have_error = false;

	if (NUMERIC_IS_SPECIAL(num))
	{
		if (have_error)
		{
			*have_error = true;
			return 0;
		}
		else
		{
			if (NUMERIC_IS_NAN(num))
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot convert NaN to %s", "integer")));
			else
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot convert infinity to %s", "integer")));
		}
	}

	/* Convert to variable format, then convert to int4 */
	init_var_from_num(num, &x);

	if (!numericvar_to_int32(&x, &result))
	{
		if (have_error)
		{
			*have_error = true;
			return 0;
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					 errmsg("integer out of range")));
		}
	}

	return result;
}

/* --- src/backend/utils/adt/int.c:326-334 int4out (verbatim) --- */
Datum
int4out(PG_FUNCTION_ARGS)
{
	int32		arg1 = PG_GETARG_INT32(0);
	char	   *result = (char *) palloc(12);	/* sign, 10 digits, '\0' */

	pg_ltoa(arg1, result);
	PG_RETURN_CSTRING(result);
}

/* --- src/backend/utils/adt/int8.c:60-77 int8out (verbatim) --- */
Datum
int8out(PG_FUNCTION_ARGS)
{
	int64		val = PG_GETARG_INT64(0);
	char		buf[MAXINT8LEN + 1];
	char	   *result;
	int			len;

	len = pg_lltoa(val, buf) + 1;

	/*
	 * Since the length is already known, we do a manual palloc() and memcpy()
	 * to avoid the strlen() call that would otherwise be done in pstrdup().
	 */
	result = palloc(len);
	memcpy(result, buf, len);
	PG_RETURN_CSTRING(result);
}

/* --- src/backend/utils/adt/int8.c:489-501 int8mul (verbatim) --- */
Datum
int8mul(PG_FUNCTION_ARGS)
{
	int64		arg1 = PG_GETARG_INT64(0);
	int64		arg2 = PG_GETARG_INT64(1);
	int64		result;

	if (unlikely(pg_mul_s64_overflow(arg1, arg2, &result)))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("bigint out of range")));
	PG_RETURN_INT64(result);
}

/* --- src/backend/utils/adt/int8.c:1296-1315 dtoi8 (verbatim) --- */
Datum
dtoi8(PG_FUNCTION_ARGS)
{
	float8		num = PG_GETARG_FLOAT8(0);

	/*
	 * Get rid of any fractional part in the input.  This is so we don't fail
	 * on just-out-of-range values that would round into range.  Note
	 * assumption that rint() will pass through a NaN or Inf unchanged.
	 */
	num = rint(num);

	/* Range check */
	if (unlikely(isnan(num) || !FLOAT8_FITS_IN_INT64(num)))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("bigint out of range")));

	PG_RETURN_INT64((int64) num);
}

/* --- src/backend/utils/adt/float.c:1213-1232 dtoi4 (verbatim) --- */
Datum
dtoi4(PG_FUNCTION_ARGS)
{
	float8		num = PG_GETARG_FLOAT8(0);

	/*
	 * Get rid of any fractional part in the input.  This is so we don't fail
	 * on just-out-of-range values that would round into range.  Note
	 * assumption that rint() will pass through a NaN or Inf unchanged.
	 */
	num = rint(num);

	/* Range check */
	if (unlikely(isnan(num) || !FLOAT8_FITS_IN_INT32(num)))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("integer out of range")));

	PG_RETURN_INT32((int32) num);
}

/* --- src/backend/utils/adt/float.c:1287-1306 ftoi4 (verbatim) --- */
Datum
ftoi4(PG_FUNCTION_ARGS)
{
	float4		num = PG_GETARG_FLOAT4(0);

	/*
	 * Get rid of any fractional part in the input.  This is so we don't fail
	 * on just-out-of-range values that would round into range.  Note
	 * assumption that rint() will pass through a NaN or Inf unchanged.
	 */
	num = rint(num);

	/* Range check */
	if (unlikely(isnan(num) || !FLOAT4_FITS_IN_INT32(num)))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("integer out of range")));

	PG_RETURN_INT32((int32) num);
}

/* --- src/backend/utils/adt/numeric.c:1477-1501 numeric_sign_internal (verbatim) --- */
static int
numeric_sign_internal(Numeric num)
{
	if (NUMERIC_IS_SPECIAL(num))
	{
		Assert(!NUMERIC_IS_NAN(num));
		/* Must be Inf or -Inf */
		if (NUMERIC_IS_PINF(num))
			return 1;
		else
			return -1;
	}

	/*
	 * The packed format is known to be totally zero digit trimmed always. So
	 * once we've eliminated specials, we can identify a zero by the fact that
	 * there are no digits at all.
	 */
	else if (NUMERIC_NDIGITS(num) == 0)
		return 0;
	else if (NUMERIC_SIGN(num) == NUMERIC_NEG)
		return -1;
	else
		return 1;
}

/* --- src/backend/utils/adt/numeric.c:872-889 numeric_is_integral (verbatim) --- */
static bool
numeric_is_integral(Numeric num)
{
	NumericVar	arg;

	/* Reject NaN, but infinities are considered integral */
	if (NUMERIC_IS_SPECIAL(num))
	{
		if (NUMERIC_IS_NAN(num))
			return false;
		return true;
	}

	/* Integral if there are no digits to the right of the decimal point */
	init_var_from_num(num, &arg);

	return (arg.ndigits == 0 || arg.ndigits <= arg.weight + 1);
}

/* --- src/backend/utils/adt/numutils.c:1119-1133 pg_ltoa (verbatim) --- */
int
pg_ltoa(int32 value, char *a)
{
	uint32		uvalue = (uint32) value;
	int			len = 0;

	if (value < 0)
	{
		uvalue = (uint32) 0 - uvalue;
		a[len++] = '-';
	}
	len += pg_ultoa_n(uvalue, a + len);
	a[len] = '\0';
	return len;
}

/* --- src/backend/utils/adt/numutils.c:1226-1241 pg_lltoa (verbatim) --- */
int
pg_lltoa(int64 value, char *a)
{
	uint64		uvalue = value;
	int			len = 0;

	if (value < 0)
	{
		uvalue = (uint64) 0 - uvalue;
		a[len++] = '-';
	}

	len += pg_ulltoa_n(uvalue, a + len);
	a[len] = '\0';
	return len;
}

/* --- src/backend/utils/adt/numutils.c:1139-1217 pg_ulltoa_n (verbatim) --- */
int
pg_ulltoa_n(uint64 value, char *a)
{
	int			olength,
				i = 0;
	uint32		value2;

	/* Degenerate case */
	if (value == 0)
	{
		*a = '0';
		return 1;
	}

	olength = decimalLength64(value);

	/* Compute the result string. */
	while (value >= 100000000)
	{
		const uint64 q = value / 100000000;
		uint32		value3 = (uint32) (value - 100000000 * q);

		const uint32 c = value3 % 10000;
		const uint32 d = value3 / 10000;
		const uint32 c0 = (c % 100) << 1;
		const uint32 c1 = (c / 100) << 1;
		const uint32 d0 = (d % 100) << 1;
		const uint32 d1 = (d / 100) << 1;

		char	   *pos = a + olength - i;

		value = q;

		memcpy(pos - 2, DIGIT_TABLE + c0, 2);
		memcpy(pos - 4, DIGIT_TABLE + c1, 2);
		memcpy(pos - 6, DIGIT_TABLE + d0, 2);
		memcpy(pos - 8, DIGIT_TABLE + d1, 2);
		i += 8;
	}

	/* Switch to 32-bit for speed */
	value2 = (uint32) value;

	if (value2 >= 10000)
	{
		const uint32 c = value2 - 10000 * (value2 / 10000);
		const uint32 c0 = (c % 100) << 1;
		const uint32 c1 = (c / 100) << 1;

		char	   *pos = a + olength - i;

		value2 /= 10000;

		memcpy(pos - 2, DIGIT_TABLE + c0, 2);
		memcpy(pos - 4, DIGIT_TABLE + c1, 2);
		i += 4;
	}
	if (value2 >= 100)
	{
		const uint32 c = (value2 % 100) << 1;
		char	   *pos = a + olength - i;

		value2 /= 100;

		memcpy(pos - 2, DIGIT_TABLE + c, 2);
		i += 2;
	}
	if (value2 >= 10)
	{
		const uint32 c = value2 << 1;
		char	   *pos = a + olength - i;

		memcpy(pos - 2, DIGIT_TABLE + c, 2);
	}
	else
		*a = (char) ('0' + value2);

	return olength;
}

/* --- src/backend/utils/adt/numutils.c:1054-1109 pg_ultoa_n (verbatim) --- */
int
pg_ultoa_n(uint32 value, char *a)
{
	int			olength,
				i = 0;

	/* Degenerate case */
	if (value == 0)
	{
		*a = '0';
		return 1;
	}

	olength = decimalLength32(value);

	/* Compute the result string. */
	while (value >= 10000)
	{
		const uint32 c = value - 10000 * (value / 10000);
		const uint32 c0 = (c % 100) << 1;
		const uint32 c1 = (c / 100) << 1;

		char	   *pos = a + olength - i;

		value /= 10000;

		memcpy(pos - 2, DIGIT_TABLE + c0, 2);
		memcpy(pos - 4, DIGIT_TABLE + c1, 2);
		i += 4;
	}
	if (value >= 100)
	{
		const uint32 c = (value % 100) << 1;

		char	   *pos = a + olength - i;

		value /= 100;

		memcpy(pos - 2, DIGIT_TABLE + c, 2);
		i += 2;
	}
	if (value >= 10)
	{
		const uint32 c = value << 1;

		char	   *pos = a + olength - i;

		memcpy(pos - 2, DIGIT_TABLE + c, 2);
	}
	else
	{
		*a = (char) ('0' + value);
	}

	return olength;
}


/* --- src/backend/utils/adt/numeric.c:8009-8013 make_result (verbatim) --- */
static Numeric
make_result(const NumericVar *var)
{
	return make_result_opt_error(var, NULL);
}

/* --- src/backend/utils/adt/numeric.c:7900-8001 make_result_opt_error (verbatim) --- */
static Numeric
make_result_opt_error(const NumericVar *var, bool *have_error)
{
	Numeric		result;
	NumericDigit *digits = var->digits;
	int			weight = var->weight;
	int			sign = var->sign;
	int			n;
	Size		len;

	if (have_error)
		*have_error = false;

	if ((sign & NUMERIC_SIGN_MASK) == NUMERIC_SPECIAL)
	{
		/*
		 * Verify valid special value.  This could be just an Assert, perhaps,
		 * but it seems worthwhile to expend a few cycles to ensure that we
		 * never write any nonzero reserved bits to disk.
		 */
		if (!(sign == NUMERIC_NAN ||
			  sign == NUMERIC_PINF ||
			  sign == NUMERIC_NINF))
			elog(ERROR, "invalid numeric sign value 0x%x", sign);

		result = (Numeric) palloc(NUMERIC_HDRSZ_SHORT);

		SET_VARSIZE(result, NUMERIC_HDRSZ_SHORT);
		result->choice.n_header = sign;
		/* the header word is all we need */

		dump_numeric("make_result()", result);
		return result;
	}

	n = var->ndigits;

	/* truncate leading zeroes */
	while (n > 0 && *digits == 0)
	{
		digits++;
		weight--;
		n--;
	}
	/* truncate trailing zeroes */
	while (n > 0 && digits[n - 1] == 0)
		n--;

	/* If zero result, force to weight=0 and positive sign */
	if (n == 0)
	{
		weight = 0;
		sign = NUMERIC_POS;
	}

	/* Build the result */
	if (NUMERIC_CAN_BE_SHORT(var->dscale, weight))
	{
		len = NUMERIC_HDRSZ_SHORT + n * sizeof(NumericDigit);
		result = (Numeric) palloc(len);
		SET_VARSIZE(result, len);
		result->choice.n_short.n_header =
			(sign == NUMERIC_NEG ? (NUMERIC_SHORT | NUMERIC_SHORT_SIGN_MASK)
			 : NUMERIC_SHORT)
			| (var->dscale << NUMERIC_SHORT_DSCALE_SHIFT)
			| (weight < 0 ? NUMERIC_SHORT_WEIGHT_SIGN_MASK : 0)
			| (weight & NUMERIC_SHORT_WEIGHT_MASK);
	}
	else
	{
		len = NUMERIC_HDRSZ + n * sizeof(NumericDigit);
		result = (Numeric) palloc(len);
		SET_VARSIZE(result, len);
		result->choice.n_long.n_sign_dscale =
			sign | (var->dscale & NUMERIC_DSCALE_MASK);
		result->choice.n_long.n_weight = weight;
	}

	Assert(NUMERIC_NDIGITS(result) == n);
	if (n > 0)
		memcpy(NUMERIC_DIGITS(result), digits, n * sizeof(NumericDigit));

	/* Check for overflow of int16 fields */
	if (NUMERIC_WEIGHT(result) != weight ||
		NUMERIC_DSCALE(result) != var->dscale)
	{
		if (have_error)
		{
			*have_error = true;
			return NULL;
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					 errmsg("value overflows numeric format")));
		}
	}

	dump_numeric("make_result()", result);
	return result;
}

/* --- src/backend/utils/adt/numeric.c:8025-8099 apply_typmod (verbatim) --- */
static bool
apply_typmod(NumericVar *var, int32 typmod, Node *escontext)
{
	int			precision;
	int			scale;
	int			maxdigits;
	int			ddigits;
	int			i;

	/* Do nothing if we have an invalid typmod */
	if (!is_valid_numeric_typmod(typmod))
		return true;

	precision = numeric_typmod_precision(typmod);
	scale = numeric_typmod_scale(typmod);
	maxdigits = precision - scale;

	/* Round to target scale (and set var->dscale) */
	round_var(var, scale);

	/* but don't allow var->dscale to be negative */
	if (var->dscale < 0)
		var->dscale = 0;

	/*
	 * Check for overflow - note we can't do this before rounding, because
	 * rounding could raise the weight.  Also note that the var's weight could
	 * be inflated by leading zeroes, which will be stripped before storage
	 * but perhaps might not have been yet. In any case, we must recognize a
	 * true zero, whose weight doesn't mean anything.
	 */
	ddigits = (var->weight + 1) * DEC_DIGITS;
	if (ddigits > maxdigits)
	{
		/* Determine true weight; and check for all-zero result */
		for (i = 0; i < var->ndigits; i++)
		{
			NumericDigit dig = var->digits[i];

			if (dig)
			{
				/* Adjust for any high-order decimal zero digits */
#if DEC_DIGITS == 4
				if (dig < 10)
					ddigits -= 3;
				else if (dig < 100)
					ddigits -= 2;
				else if (dig < 1000)
					ddigits -= 1;
#elif DEC_DIGITS == 2
				if (dig < 10)
					ddigits -= 1;
#elif DEC_DIGITS == 1
				/* no adjustment */
#else
#error unsupported NBASE
#endif
				if (ddigits > maxdigits)
					ereturn(escontext, false,
							(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
							 errmsg("numeric field overflow"),
							 errdetail("A field with precision %d, scale %d must round to an absolute value less than %s%d.",
									   precision, scale,
					/* Display 10^0 as 1 */
									   maxdigits ? "10^" : "",
									   maxdigits ? maxdigits : 1
									   )));
				break;
			}
			ddigits -= DEC_DIGITS;
		}
	}

	return true;
}

/* --- src/backend/utils/adt/numeric.c:8110-8139 apply_typmod_special (verbatim) --- */
static bool
apply_typmod_special(Numeric num, int32 typmod, Node *escontext)
{
	int			precision;
	int			scale;

	Assert(NUMERIC_IS_SPECIAL(num));	/* caller error if not */

	/*
	 * NaN is allowed regardless of the typmod; that's rather dubious perhaps,
	 * but it's a longstanding behavior.  Inf is rejected if we have any
	 * typmod restriction, since an infinity shouldn't be claimed to fit in
	 * any finite number of digits.
	 */
	if (NUMERIC_IS_NAN(num))
		return true;

	/* Do nothing if we have a default typmod (-1) */
	if (!is_valid_numeric_typmod(typmod))
		return true;

	precision = numeric_typmod_precision(typmod);
	scale = numeric_typmod_scale(typmod);

	ereturn(escontext, false,
			(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
			 errmsg("numeric field overflow"),
			 errdetail("A field with precision %d, scale %d cannot hold an infinite value.",
					   precision, scale)));
}

/* --- src/backend/utils/adt/numeric.c:7130-7327 set_var_from_str (verbatim) --- */
static bool
set_var_from_str(const char *str, const char *cp,
				 NumericVar *dest, const char **endptr,
				 Node *escontext)
{
	bool		have_dp = false;
	int			i;
	unsigned char *decdigits;
	int			sign = NUMERIC_POS;
	int			dweight = -1;
	int			ddigits;
	int			dscale = 0;
	int			weight;
	int			ndigits;
	int			offset;
	NumericDigit *digits;

	/*
	 * We first parse the string to extract decimal digits and determine the
	 * correct decimal weight.  Then convert to NBASE representation.
	 */
	switch (*cp)
	{
		case '+':
			sign = NUMERIC_POS;
			cp++;
			break;

		case '-':
			sign = NUMERIC_NEG;
			cp++;
			break;
	}

	if (*cp == '.')
	{
		have_dp = true;
		cp++;
	}

	if (!isdigit((unsigned char) *cp))
		goto invalid_syntax;

	decdigits = (unsigned char *) palloc(strlen(cp) + DEC_DIGITS * 2);

	/* leading padding for digit alignment later */
	memset(decdigits, 0, DEC_DIGITS);
	i = DEC_DIGITS;

	while (*cp)
	{
		if (isdigit((unsigned char) *cp))
		{
			decdigits[i++] = *cp++ - '0';
			if (!have_dp)
				dweight++;
			else
				dscale++;
		}
		else if (*cp == '.')
		{
			if (have_dp)
				goto invalid_syntax;
			have_dp = true;
			cp++;
			/* decimal point must not be followed by underscore */
			if (*cp == '_')
				goto invalid_syntax;
		}
		else if (*cp == '_')
		{
			/* underscore must be followed by more digits */
			cp++;
			if (!isdigit((unsigned char) *cp))
				goto invalid_syntax;
		}
		else
			break;
	}

	ddigits = i - DEC_DIGITS;
	/* trailing padding for digit alignment later */
	memset(decdigits + i, 0, DEC_DIGITS - 1);

	/* Handle exponent, if any */
	if (*cp == 'e' || *cp == 'E')
	{
		int64		exponent = 0;
		bool		neg = false;

		/*
		 * At this point, dweight and dscale can't be more than about
		 * INT_MAX/2 due to the MaxAllocSize limit on string length, so
		 * constraining the exponent similarly should be enough to prevent
		 * integer overflow in this function.  If the value is too large to
		 * fit in storage format, make_result() will complain about it later;
		 * for consistency use the same ereport errcode/text as make_result().
		 */

		/* exponent sign */
		cp++;
		if (*cp == '+')
			cp++;
		else if (*cp == '-')
		{
			neg = true;
			cp++;
		}

		/* exponent digits */
		if (!isdigit((unsigned char) *cp))
			goto invalid_syntax;

		while (*cp)
		{
			if (isdigit((unsigned char) *cp))
			{
				exponent = exponent * 10 + (*cp++ - '0');
				if (exponent > PG_INT32_MAX / 2)
					goto out_of_range;
			}
			else if (*cp == '_')
			{
				/* underscore must be followed by more digits */
				cp++;
				if (!isdigit((unsigned char) *cp))
					goto invalid_syntax;
			}
			else
				break;
		}

		if (neg)
			exponent = -exponent;

		dweight += (int) exponent;
		dscale -= (int) exponent;
		if (dscale < 0)
			dscale = 0;
	}

	/*
	 * Okay, convert pure-decimal representation to base NBASE.  First we need
	 * to determine the converted weight and ndigits.  offset is the number of
	 * decimal zeroes to insert before the first given digit to have a
	 * correctly aligned first NBASE digit.
	 */
	if (dweight >= 0)
		weight = (dweight + 1 + DEC_DIGITS - 1) / DEC_DIGITS - 1;
	else
		weight = -((-dweight - 1) / DEC_DIGITS + 1);
	offset = (weight + 1) * DEC_DIGITS - (dweight + 1);
	ndigits = (ddigits + offset + DEC_DIGITS - 1) / DEC_DIGITS;

	alloc_var(dest, ndigits);
	dest->sign = sign;
	dest->weight = weight;
	dest->dscale = dscale;

	i = DEC_DIGITS - offset;
	digits = dest->digits;

	while (ndigits-- > 0)
	{
#if DEC_DIGITS == 4
		*digits++ = ((decdigits[i] * 10 + decdigits[i + 1]) * 10 +
					 decdigits[i + 2]) * 10 + decdigits[i + 3];
#elif DEC_DIGITS == 2
		*digits++ = decdigits[i] * 10 + decdigits[i + 1];
#elif DEC_DIGITS == 1
		*digits++ = decdigits[i];
#else
#error unsupported NBASE
#endif
		i += DEC_DIGITS;
	}

	pfree(decdigits);

	/* Strip any leading/trailing zeroes, and normalize weight if zero */
	strip_var(dest);

	/* Return end+1 position for caller */
	*endptr = cp;

	return true;

out_of_range:
	ereturn(escontext, false,
			(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
			 errmsg("value overflows numeric format")));

invalid_syntax:
	ereturn(escontext, false,
			(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
			 errmsg("invalid input syntax for type %s: \"%s\"",
					"numeric", str)));
}

/* --- src/backend/utils/adt/numeric.c:7360-7530 set_var_from_non_decimal_integer_str (verbatim) --- */
static bool
set_var_from_non_decimal_integer_str(const char *str, const char *cp, int sign,
									 int base, NumericVar *dest,
									 const char **endptr, Node *escontext)
{
	const char *firstdigit = cp;
	int64		tmp;
	int64		mul;
	NumericVar	tmp_var;

	init_var(&tmp_var);

	zero_var(dest);

	/*
	 * Process input digits in groups that fit in int64.  Here "tmp" is the
	 * value of the digits in the group, and "mul" is base^n, where n is the
	 * number of digits in the group.  Thus tmp < mul, and we must start a new
	 * group when mul * base threatens to overflow PG_INT64_MAX.
	 */
	tmp = 0;
	mul = 1;

	if (base == 16)
	{
		while (*cp)
		{
			if (isxdigit((unsigned char) *cp))
			{
				if (mul > PG_INT64_MAX / 16)
				{
					/* Add the contribution from this group of digits */
					int64_to_numericvar(mul, &tmp_var);
					mul_var(dest, &tmp_var, dest, 0);
					int64_to_numericvar(tmp, &tmp_var);
					add_var(dest, &tmp_var, dest);

					/* Result will overflow if weight overflows int16 */
					if (dest->weight > NUMERIC_WEIGHT_MAX)
						goto out_of_range;

					/* Begin a new group */
					tmp = 0;
					mul = 1;
				}

				tmp = tmp * 16 + xdigit_value(*cp++);
				mul = mul * 16;
			}
			else if (*cp == '_')
			{
				/* Underscore must be followed by more digits */
				cp++;
				if (!isxdigit((unsigned char) *cp))
					goto invalid_syntax;
			}
			else
				break;
		}
	}
	else if (base == 8)
	{
		while (*cp)
		{
			if (*cp >= '0' && *cp <= '7')
			{
				if (mul > PG_INT64_MAX / 8)
				{
					/* Add the contribution from this group of digits */
					int64_to_numericvar(mul, &tmp_var);
					mul_var(dest, &tmp_var, dest, 0);
					int64_to_numericvar(tmp, &tmp_var);
					add_var(dest, &tmp_var, dest);

					/* Result will overflow if weight overflows int16 */
					if (dest->weight > NUMERIC_WEIGHT_MAX)
						goto out_of_range;

					/* Begin a new group */
					tmp = 0;
					mul = 1;
				}

				tmp = tmp * 8 + (*cp++ - '0');
				mul = mul * 8;
			}
			else if (*cp == '_')
			{
				/* Underscore must be followed by more digits */
				cp++;
				if (*cp < '0' || *cp > '7')
					goto invalid_syntax;
			}
			else
				break;
		}
	}
	else if (base == 2)
	{
		while (*cp)
		{
			if (*cp >= '0' && *cp <= '1')
			{
				if (mul > PG_INT64_MAX / 2)
				{
					/* Add the contribution from this group of digits */
					int64_to_numericvar(mul, &tmp_var);
					mul_var(dest, &tmp_var, dest, 0);
					int64_to_numericvar(tmp, &tmp_var);
					add_var(dest, &tmp_var, dest);

					/* Result will overflow if weight overflows int16 */
					if (dest->weight > NUMERIC_WEIGHT_MAX)
						goto out_of_range;

					/* Begin a new group */
					tmp = 0;
					mul = 1;
				}

				tmp = tmp * 2 + (*cp++ - '0');
				mul = mul * 2;
			}
			else if (*cp == '_')
			{
				/* Underscore must be followed by more digits */
				cp++;
				if (*cp < '0' || *cp > '1')
					goto invalid_syntax;
			}
			else
				break;
		}
	}
	else
		/* Should never happen; treat as invalid input */
		goto invalid_syntax;

	/* Check that we got at least one digit */
	if (unlikely(cp == firstdigit))
		goto invalid_syntax;

	/* Add the contribution from the final group of digits */
	int64_to_numericvar(mul, &tmp_var);
	mul_var(dest, &tmp_var, dest, 0);
	int64_to_numericvar(tmp, &tmp_var);
	add_var(dest, &tmp_var, dest);

	if (dest->weight > NUMERIC_WEIGHT_MAX)
		goto out_of_range;

	dest->sign = sign;

	free_var(&tmp_var);

	/* Return end+1 position for caller */
	*endptr = cp;

	return true;

out_of_range:
	ereturn(escontext, false,
			(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
			 errmsg("value overflows numeric format")));

invalid_syntax:
	ereturn(escontext, false,
			(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
			 errmsg("invalid input syntax for type %s: \"%s\"",
					"numeric", str)));
}

/* --- src/backend/utils/adt/numeric.c:7087-7094 free_var (verbatim) --- */
static void
free_var(NumericVar *var)
{
	digitbuf_free(var->buf);
	var->buf = NULL;
	var->digits = NULL;
	var->sign = NUMERIC_NAN;
}

/* --- src/backend/utils/adt/numeric.c:7569-7578 init_var_from_num (verbatim) --- */
static void
init_var_from_num(Numeric num, NumericVar *dest)
{
	dest->ndigits = NUMERIC_NDIGITS(num);
	dest->weight = NUMERIC_WEIGHT(num);
	dest->sign = NUMERIC_SIGN(num);
	dest->dscale = NUMERIC_DSCALE(num);
	dest->digits = NUMERIC_DIGITS(num);
	dest->buf = NULL;			/* digits array is not palloc'd */
}

/* --- src/backend/utils/adt/numeric.c:7538-7552 set_var_from_num (verbatim) --- */
static void
set_var_from_num(Numeric num, NumericVar *dest)
{
	int			ndigits;

	ndigits = NUMERIC_NDIGITS(num);

	alloc_var(dest, ndigits);

	dest->weight = NUMERIC_WEIGHT(num);
	dest->sign = NUMERIC_SIGN(num);
	dest->dscale = NUMERIC_DSCALE(num);

	memcpy(dest->digits, NUMERIC_DIGITS(num), ndigits * sizeof(NumericDigit));
}

/* --- src/backend/utils/adt/numeric.c:7612-7741 get_str_from_var (verbatim) --- */
static char *
get_str_from_var(const NumericVar *var)
{
	int			dscale;
	char	   *str;
	char	   *cp;
	char	   *endcp;
	int			i;
	int			d;
	NumericDigit dig;

#if DEC_DIGITS > 1
	NumericDigit d1;
#endif

	dscale = var->dscale;

	/*
	 * Allocate space for the result.
	 *
	 * i is set to the # of decimal digits before decimal point. dscale is the
	 * # of decimal digits we will print after decimal point. We may generate
	 * as many as DEC_DIGITS-1 excess digits at the end, and in addition we
	 * need room for sign, decimal point, null terminator.
	 */
	i = (var->weight + 1) * DEC_DIGITS;
	if (i <= 0)
		i = 1;

	str = palloc(i + dscale + DEC_DIGITS + 2);
	cp = str;

	/*
	 * Output a dash for negative values
	 */
	if (var->sign == NUMERIC_NEG)
		*cp++ = '-';

	/*
	 * Output all digits before the decimal point
	 */
	if (var->weight < 0)
	{
		d = var->weight + 1;
		*cp++ = '0';
	}
	else
	{
		for (d = 0; d <= var->weight; d++)
		{
			dig = (d < var->ndigits) ? var->digits[d] : 0;
			/* In the first digit, suppress extra leading decimal zeroes */
#if DEC_DIGITS == 4
			{
				bool		putit = (d > 0);

				d1 = dig / 1000;
				dig -= d1 * 1000;
				putit |= (d1 > 0);
				if (putit)
					*cp++ = d1 + '0';
				d1 = dig / 100;
				dig -= d1 * 100;
				putit |= (d1 > 0);
				if (putit)
					*cp++ = d1 + '0';
				d1 = dig / 10;
				dig -= d1 * 10;
				putit |= (d1 > 0);
				if (putit)
					*cp++ = d1 + '0';
				*cp++ = dig + '0';
			}
#elif DEC_DIGITS == 2
			d1 = dig / 10;
			dig -= d1 * 10;
			if (d1 > 0 || d > 0)
				*cp++ = d1 + '0';
			*cp++ = dig + '0';
#elif DEC_DIGITS == 1
			*cp++ = dig + '0';
#else
#error unsupported NBASE
#endif
		}
	}

	/*
	 * If requested, output a decimal point and all the digits that follow it.
	 * We initially put out a multiple of DEC_DIGITS digits, then truncate if
	 * needed.
	 */
	if (dscale > 0)
	{
		*cp++ = '.';
		endcp = cp + dscale;
		for (i = 0; i < dscale; d++, i += DEC_DIGITS)
		{
			dig = (d >= 0 && d < var->ndigits) ? var->digits[d] : 0;
#if DEC_DIGITS == 4
			d1 = dig / 1000;
			dig -= d1 * 1000;
			*cp++ = d1 + '0';
			d1 = dig / 100;
			dig -= d1 * 100;
			*cp++ = d1 + '0';
			d1 = dig / 10;
			dig -= d1 * 10;
			*cp++ = d1 + '0';
			*cp++ = dig + '0';
#elif DEC_DIGITS == 2
			d1 = dig / 10;
			dig -= d1 * 10;
			*cp++ = d1 + '0';
			*cp++ = dig + '0';
#elif DEC_DIGITS == 1
			*cp++ = dig + '0';
#else
#error unsupported NBASE
#endif
		}
		cp = endcp;
	}

	/*
	 * terminate the string and return it
	 */
	*cp = '\0';
	return str;
}

/* --- src/backend/utils/adt/numeric.c:7765-7831 get_str_from_var_sci (verbatim) --- */
static char *
get_str_from_var_sci(const NumericVar *var, int rscale)
{
	int32		exponent;
	NumericVar	tmp_var;
	size_t		len;
	char	   *str;
	char	   *sig_out;

	if (rscale < 0)
		rscale = 0;

	/*
	 * Determine the exponent of this number in normalised form.
	 *
	 * This is the exponent required to represent the number with only one
	 * significant digit before the decimal place.
	 */
	if (var->ndigits > 0)
	{
		exponent = (var->weight + 1) * DEC_DIGITS;

		/*
		 * Compensate for leading decimal zeroes in the first numeric digit by
		 * decrementing the exponent.
		 */
		exponent -= DEC_DIGITS - (int) log10(var->digits[0]);
	}
	else
	{
		/*
		 * If var has no digits, then it must be zero.
		 *
		 * Zero doesn't technically have a meaningful exponent in normalised
		 * notation, but we just display the exponent as zero for consistency
		 * of output.
		 */
		exponent = 0;
	}

	/*
	 * Divide var by 10^exponent to get the significand, rounding to rscale
	 * decimal digits in the process.
	 */
	init_var(&tmp_var);

	power_ten_int(exponent, &tmp_var);
	div_var(var, &tmp_var, &tmp_var, rscale, true, true);
	sig_out = get_str_from_var(&tmp_var);

	free_var(&tmp_var);

	/*
	 * Allocate space for the result.
	 *
	 * In addition to the significand, we need room for the exponent
	 * decoration ("e"), the sign of the exponent, up to 10 digits for the
	 * exponent itself, and of course the null terminator.
	 */
	len = strlen(sig_out) + 13;
	str = palloc(len);
	snprintf(str, len, "%se%+03d", sig_out, exponent);

	pfree(sig_out);

	return str;
}

/* --- src/backend/utils/adt/numeric.c:7881-7889 duplicate_numeric (verbatim) --- */
static Numeric
duplicate_numeric(Numeric num)
{
	Numeric		res;

	res = (Numeric) palloc(VARSIZE(num));
	memcpy(res, num, VARSIZE(num));
	return res;
}

/* --- src/backend/utils/adt/numeric.c:12108-12205 round_var (verbatim) --- */
static void
round_var(NumericVar *var, int rscale)
{
	NumericDigit *digits = var->digits;
	int			di;
	int			ndigits;
	int			carry;

	var->dscale = rscale;

	/* decimal digits wanted */
	di = (var->weight + 1) * DEC_DIGITS + rscale;

	/*
	 * If di = 0, the value loses all digits, but could round up to 1 if its
	 * first extra digit is >= 5.  If di < 0 the result must be 0.
	 */
	if (di < 0)
	{
		var->ndigits = 0;
		var->weight = 0;
		var->sign = NUMERIC_POS;
	}
	else
	{
		/* NBASE digits wanted */
		ndigits = (di + DEC_DIGITS - 1) / DEC_DIGITS;

		/* 0, or number of decimal digits to keep in last NBASE digit */
		di %= DEC_DIGITS;

		if (ndigits < var->ndigits ||
			(ndigits == var->ndigits && di > 0))
		{
			var->ndigits = ndigits;

#if DEC_DIGITS == 1
			/* di must be zero */
			carry = (digits[ndigits] >= HALF_NBASE) ? 1 : 0;
#else
			if (di == 0)
				carry = (digits[ndigits] >= HALF_NBASE) ? 1 : 0;
			else
			{
				/* Must round within last NBASE digit */
				int			extra,
							pow10;

#if DEC_DIGITS == 4
				pow10 = round_powers[di];
#elif DEC_DIGITS == 2
				pow10 = 10;
#else
#error unsupported NBASE
#endif
				extra = digits[--ndigits] % pow10;
				digits[ndigits] -= extra;
				carry = 0;
				if (extra >= pow10 / 2)
				{
					pow10 += digits[ndigits];
					if (pow10 >= NBASE)
					{
						pow10 -= NBASE;
						carry = 1;
					}
					digits[ndigits] = pow10;
				}
			}
#endif

			/* Propagate carry if needed */
			while (carry)
			{
				carry += digits[--ndigits];
				if (carry >= NBASE)
				{
					digits[ndigits] = carry - NBASE;
					carry = 1;
				}
				else
				{
					digits[ndigits] = carry;
					carry = 0;
				}
			}

			if (ndigits < 0)
			{
				Assert(ndigits == -1);	/* better not have added > 1 digit */
				Assert(var->digits > var->buf);
				var->digits--;
				var->ndigits++;
				var->weight++;
			}
		}
	}
}

/* --- src/backend/utils/adt/numeric.c:8787-9068 mul_var (verbatim) --- */
static void
mul_var(const NumericVar *var1, const NumericVar *var2, NumericVar *result,
		int rscale)
{
	int			res_ndigits;
	int			res_ndigitpairs;
	int			res_sign;
	int			res_weight;
	int			pair_offset;
	int			maxdigits;
	int			maxdigitpairs;
	uint64	   *dig,
			   *dig_i1_off;
	uint64		maxdig;
	uint64		carry;
	uint64		newdig;
	int			var1ndigits;
	int			var2ndigits;
	int			var1ndigitpairs;
	int			var2ndigitpairs;
	NumericDigit *var1digits;
	NumericDigit *var2digits;
	uint32		var1digitpair;
	uint32	   *var2digitpairs;
	NumericDigit *res_digits;
	int			i,
				i1,
				i2,
				i2limit;

	/*
	 * Arrange for var1 to be the shorter of the two numbers.  This improves
	 * performance because the inner multiplication loop is much simpler than
	 * the outer loop, so it's better to have a smaller number of iterations
	 * of the outer loop.  This also reduces the number of times that the
	 * accumulator array needs to be normalized.
	 */
	if (var1->ndigits > var2->ndigits)
	{
		const NumericVar *tmp = var1;

		var1 = var2;
		var2 = tmp;
	}

	/* copy these values into local vars for speed in inner loop */
	var1ndigits = var1->ndigits;
	var2ndigits = var2->ndigits;
	var1digits = var1->digits;
	var2digits = var2->digits;

	if (var1ndigits == 0)
	{
		/* one or both inputs is zero; so is result */
		zero_var(result);
		result->dscale = rscale;
		return;
	}

	/*
	 * If var1 has 1-6 digits and the exact result was requested, delegate to
	 * mul_var_short() which uses a faster direct multiplication algorithm.
	 */
	if (var1ndigits <= 6 && rscale == var1->dscale + var2->dscale)
	{
		mul_var_short(var1, var2, result);
		return;
	}

	/* Determine result sign */
	if (var1->sign == var2->sign)
		res_sign = NUMERIC_POS;
	else
		res_sign = NUMERIC_NEG;

	/*
	 * Determine the number of result digits to compute and the (maximum
	 * possible) result weight.  If the exact result would have more than
	 * rscale fractional digits, truncate the computation with
	 * MUL_GUARD_DIGITS guard digits, i.e., ignore input digits that would
	 * only contribute to the right of that.  (This will give the exact
	 * rounded-to-rscale answer unless carries out of the ignored positions
	 * would have propagated through more than MUL_GUARD_DIGITS digits.)
	 *
	 * Note: an exact computation could not produce more than var1ndigits +
	 * var2ndigits digits, but we allocate at least one extra output digit in
	 * case rscale-driven rounding produces a carry out of the highest exact
	 * digit.
	 *
	 * The computation itself is done using base-NBASE^2 arithmetic, so we
	 * actually process the input digits in pairs, producing a base-NBASE^2
	 * intermediate result.  This significantly improves performance, since
	 * schoolbook multiplication is O(N^2) in the number of input digits, and
	 * working in base NBASE^2 effectively halves "N".
	 *
	 * Note: in a truncated computation, we must compute at least one extra
	 * output digit to ensure that all the guard digits are fully computed.
	 */
	/* digit pairs in each input */
	var1ndigitpairs = (var1ndigits + 1) / 2;
	var2ndigitpairs = (var2ndigits + 1) / 2;

	/* digits in exact result */
	res_ndigits = var1ndigits + var2ndigits;

	/* digit pairs in exact result with at least one extra output digit */
	res_ndigitpairs = res_ndigits / 2 + 1;

	/* pair offset to align result to end of dig[] */
	pair_offset = res_ndigitpairs - var1ndigitpairs - var2ndigitpairs + 1;

	/* maximum possible result weight (odd-length inputs shifted up below) */
	res_weight = var1->weight + var2->weight + 1 + 2 * res_ndigitpairs -
		res_ndigits - (var1ndigits & 1) - (var2ndigits & 1);

	/* rscale-based truncation with at least one extra output digit */
	maxdigits = res_weight + 1 + (rscale + DEC_DIGITS - 1) / DEC_DIGITS +
		MUL_GUARD_DIGITS;
	maxdigitpairs = maxdigits / 2 + 1;

	res_ndigitpairs = Min(res_ndigitpairs, maxdigitpairs);
	res_ndigits = 2 * res_ndigitpairs;

	/*
	 * In the computation below, digit pair i1 of var1 and digit pair i2 of
	 * var2 are multiplied and added to digit i1+i2+pair_offset of dig[]. Thus
	 * input digit pairs with index >= res_ndigitpairs - pair_offset don't
	 * contribute to the result, and can be ignored.
	 */
	if (res_ndigitpairs <= pair_offset)
	{
		/* All input digits will be ignored; so result is zero */
		zero_var(result);
		result->dscale = rscale;
		return;
	}
	var1ndigitpairs = Min(var1ndigitpairs, res_ndigitpairs - pair_offset);
	var2ndigitpairs = Min(var2ndigitpairs, res_ndigitpairs - pair_offset);

	/*
	 * We do the arithmetic in an array "dig[]" of unsigned 64-bit integers.
	 * Since PG_UINT64_MAX is much larger than NBASE^4, this gives us a lot of
	 * headroom to avoid normalizing carries immediately.
	 *
	 * maxdig tracks the maximum possible value of any dig[] entry; when this
	 * threatens to exceed PG_UINT64_MAX, we take the time to propagate
	 * carries.  Furthermore, we need to ensure that overflow doesn't occur
	 * during the carry propagation passes either.  The carry values could be
	 * as much as PG_UINT64_MAX / NBASE^2, so really we must normalize when
	 * digits threaten to exceed PG_UINT64_MAX - PG_UINT64_MAX / NBASE^2.
	 *
	 * To avoid overflow in maxdig itself, it actually represents the maximum
	 * possible value divided by NBASE^2-1, i.e., at the top of the loop it is
	 * known that no dig[] entry exceeds maxdig * (NBASE^2-1).
	 *
	 * The conversion of var1 to base NBASE^2 is done on the fly, as each new
	 * digit is required.  The digits of var2 are converted upfront, and
	 * stored at the end of dig[].  To avoid loss of precision, the input
	 * digits are aligned with the start of digit pair array, effectively
	 * shifting them up (multiplying by NBASE) if the inputs have an odd
	 * number of NBASE digits.
	 */
	dig = (uint64 *) palloc(res_ndigitpairs * sizeof(uint64) +
							var2ndigitpairs * sizeof(uint32));

	/* convert var2 to base NBASE^2, shifting up if its length is odd */
	var2digitpairs = (uint32 *) (dig + res_ndigitpairs);

	for (i2 = 0; i2 < var2ndigitpairs - 1; i2++)
		var2digitpairs[i2] = var2digits[2 * i2] * NBASE + var2digits[2 * i2 + 1];

	if (2 * i2 + 1 < var2ndigits)
		var2digitpairs[i2] = var2digits[2 * i2] * NBASE + var2digits[2 * i2 + 1];
	else
		var2digitpairs[i2] = var2digits[2 * i2] * NBASE;

	/*
	 * Start by multiplying var2 by the least significant contributing digit
	 * pair from var1, storing the results at the end of dig[], and filling
	 * the leading digits with zeros.
	 *
	 * The loop here is the same as the inner loop below, except that we set
	 * the results in dig[], rather than adding to them.  This is the
	 * performance bottleneck for multiplication, so we want to keep it simple
	 * enough so that it can be auto-vectorized.  Accordingly, process the
	 * digits left-to-right even though schoolbook multiplication would
	 * suggest right-to-left.  Since we aren't propagating carries in this
	 * loop, the order does not matter.
	 */
	i1 = var1ndigitpairs - 1;
	if (2 * i1 + 1 < var1ndigits)
		var1digitpair = var1digits[2 * i1] * NBASE + var1digits[2 * i1 + 1];
	else
		var1digitpair = var1digits[2 * i1] * NBASE;
	maxdig = var1digitpair;

	i2limit = Min(var2ndigitpairs, res_ndigitpairs - i1 - pair_offset);
	dig_i1_off = &dig[i1 + pair_offset];

	memset(dig, 0, (i1 + pair_offset) * sizeof(uint64));
	for (i2 = 0; i2 < i2limit; i2++)
		dig_i1_off[i2] = (uint64) var1digitpair * var2digitpairs[i2];

	/*
	 * Next, multiply var2 by the remaining digit pairs from var1, adding the
	 * results to dig[] at the appropriate offsets, and normalizing whenever
	 * there is a risk of any dig[] entry overflowing.
	 */
	for (i1 = i1 - 1; i1 >= 0; i1--)
	{
		var1digitpair = var1digits[2 * i1] * NBASE + var1digits[2 * i1 + 1];
		if (var1digitpair == 0)
			continue;

		/* Time to normalize? */
		maxdig += var1digitpair;
		if (maxdig > (PG_UINT64_MAX - PG_UINT64_MAX / NBASE_SQR) / (NBASE_SQR - 1))
		{
			/* Yes, do it (to base NBASE^2) */
			carry = 0;
			for (i = res_ndigitpairs - 1; i >= 0; i--)
			{
				newdig = dig[i] + carry;
				if (newdig >= NBASE_SQR)
				{
					carry = newdig / NBASE_SQR;
					newdig -= carry * NBASE_SQR;
				}
				else
					carry = 0;
				dig[i] = newdig;
			}
			Assert(carry == 0);
			/* Reset maxdig to indicate new worst-case */
			maxdig = 1 + var1digitpair;
		}

		/* Multiply and add */
		i2limit = Min(var2ndigitpairs, res_ndigitpairs - i1 - pair_offset);
		dig_i1_off = &dig[i1 + pair_offset];

		for (i2 = 0; i2 < i2limit; i2++)
			dig_i1_off[i2] += (uint64) var1digitpair * var2digitpairs[i2];
	}

	/*
	 * Now we do a final carry propagation pass to normalize back to base
	 * NBASE^2, and construct the base-NBASE result digits.  Note that this is
	 * still done at full precision w/guard digits.
	 */
	alloc_var(result, res_ndigits);
	res_digits = result->digits;
	carry = 0;
	for (i = res_ndigitpairs - 1; i >= 0; i--)
	{
		newdig = dig[i] + carry;
		if (newdig >= NBASE_SQR)
		{
			carry = newdig / NBASE_SQR;
			newdig -= carry * NBASE_SQR;
		}
		else
			carry = 0;
		res_digits[2 * i + 1] = (NumericDigit) ((uint32) newdig % NBASE);
		res_digits[2 * i] = (NumericDigit) ((uint32) newdig / NBASE);
	}
	Assert(carry == 0);

	pfree(dig);

	/*
	 * Finally, round the result to the requested precision.
	 */
	result->weight = res_weight;
	result->sign = res_sign;

	/* Round to target rscale (and set result->dscale) */
	round_var(result, rscale);

	/* Strip leading and trailing zeroes */
	strip_var(result);
}

/* --- src/backend/utils/adt/numeric.c:8491-8498 cmp_var (verbatim) --- */
static int
cmp_var(const NumericVar *var1, const NumericVar *var2)
{
	return cmp_var_common(var1->digits, var1->ndigits,
						  var1->weight, var1->sign,
						  var2->digits, var2->ndigits,
						  var2->weight, var2->sign);
}

/* --- src/backend/utils/adt/numeric.c:11288-11441 power_var (verbatim) --- */
static void
power_var(const NumericVar *base, const NumericVar *exp, NumericVar *result)
{
	int			res_sign;
	NumericVar	abs_base;
	NumericVar	ln_base;
	NumericVar	ln_num;
	int			ln_dweight;
	int			rscale;
	int			sig_digits;
	int			local_rscale;
	double		val;

	/* If exp can be represented as an integer, use power_var_int */
	if (exp->ndigits == 0 || exp->ndigits <= exp->weight + 1)
	{
		/* exact integer, but does it fit in int? */
		int64		expval64;

		if (numericvar_to_int64(exp, &expval64))
		{
			if (expval64 >= PG_INT32_MIN && expval64 <= PG_INT32_MAX)
			{
				/* Okay, use power_var_int */
				power_var_int(base, (int) expval64, exp->dscale, result);
				return;
			}
		}
	}

	/*
	 * This avoids log(0) for cases of 0 raised to a non-integer.  0 ^ 0 is
	 * handled by power_var_int().
	 */
	if (cmp_var(base, &const_zero) == 0)
	{
		set_var_from_var(&const_zero, result);
		result->dscale = NUMERIC_MIN_SIG_DIGITS;	/* no need to round */
		return;
	}

	init_var(&abs_base);
	init_var(&ln_base);
	init_var(&ln_num);

	/*
	 * If base is negative, insist that exp be an integer.  The result is then
	 * positive if exp is even and negative if exp is odd.
	 */
	if (base->sign == NUMERIC_NEG)
	{
		/*
		 * Check that exp is an integer.  This error code is defined by the
		 * SQL standard, and matches other errors in numeric_power().
		 */
		if (exp->ndigits > 0 && exp->ndigits > exp->weight + 1)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION),
					 errmsg("a negative number raised to a non-integer power yields a complex result")));

		/* Test if exp is odd or even */
		if (exp->ndigits > 0 && exp->ndigits == exp->weight + 1 &&
			(exp->digits[exp->ndigits - 1] & 1))
			res_sign = NUMERIC_NEG;
		else
			res_sign = NUMERIC_POS;

		/* Then work with abs(base) below */
		set_var_from_var(base, &abs_base);
		abs_base.sign = NUMERIC_POS;
		base = &abs_base;
	}
	else
		res_sign = NUMERIC_POS;

	/*----------
	 * Decide on the scale for the ln() calculation.  For this we need an
	 * estimate of the weight of the result, which we obtain by doing an
	 * initial low-precision calculation of exp * ln(base).
	 *
	 * We want result = e ^ (exp * ln(base))
	 * so result dweight = log10(result) = exp * ln(base) * log10(e)
	 *
	 * We also perform a crude overflow test here so that we can exit early if
	 * the full-precision result is sure to overflow, and to guard against
	 * integer overflow when determining the scale for the real calculation.
	 * exp_var() supports inputs up to NUMERIC_MAX_RESULT_SCALE * 3, so the
	 * result will overflow if exp * ln(base) >= NUMERIC_MAX_RESULT_SCALE * 3.
	 * Since the values here are only approximations, we apply a small fuzz
	 * factor to this overflow test and let exp_var() determine the exact
	 * overflow threshold so that it is consistent for all inputs.
	 *----------
	 */
	ln_dweight = estimate_ln_dweight(base);

	/*
	 * Set the scale for the low-precision calculation, computing ln(base) to
	 * around 8 significant digits.  Note that ln_dweight may be as small as
	 * -NUMERIC_DSCALE_MAX, so the scale may exceed NUMERIC_MAX_DISPLAY_SCALE
	 * here.
	 */
	local_rscale = 8 - ln_dweight;
	local_rscale = Max(local_rscale, NUMERIC_MIN_DISPLAY_SCALE);

	ln_var(base, &ln_base, local_rscale);

	mul_var(&ln_base, exp, &ln_num, local_rscale);

	val = numericvar_to_double_no_overflow(&ln_num);

	/* initial overflow/underflow test with fuzz factor */
	if (fabs(val) > NUMERIC_MAX_RESULT_SCALE * 3.01)
	{
		if (val > 0)
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					 errmsg("value overflows numeric format")));
		zero_var(result);
		result->dscale = NUMERIC_MAX_DISPLAY_SCALE;
		return;
	}

	val *= 0.434294481903252;	/* approximate decimal result weight */

	/* choose the result scale */
	rscale = NUMERIC_MIN_SIG_DIGITS - (int) val;
	rscale = Max(rscale, base->dscale);
	rscale = Max(rscale, exp->dscale);
	rscale = Max(rscale, NUMERIC_MIN_DISPLAY_SCALE);
	rscale = Min(rscale, NUMERIC_MAX_DISPLAY_SCALE);

	/* significant digits required in the result */
	sig_digits = rscale + (int) val;
	sig_digits = Max(sig_digits, 0);

	/* set the scale for the real exp * ln(base) calculation */
	local_rscale = sig_digits - ln_dweight + 8;
	local_rscale = Max(local_rscale, NUMERIC_MIN_DISPLAY_SCALE);

	/* and do the real calculation */

	ln_var(base, &ln_base, local_rscale);

	mul_var(&ln_base, exp, &ln_num, local_rscale);

	exp_var(&ln_num, result, rscale);

	if (res_sign == NUMERIC_NEG && result->ndigits > 0)
		result->sign = NUMERIC_NEG;

	free_var(&ln_num);
	free_var(&ln_base);
	free_var(&abs_base);
}

/* --- src/backend/utils/adt/numeric.c:8222-8262 int64_to_numericvar (verbatim) --- */
static void
int64_to_numericvar(int64 val, NumericVar *var)
{
	uint64		uval,
				newuval;
	NumericDigit *ptr;
	int			ndigits;

	/* int64 can require at most 19 decimal digits; add one for safety */
	alloc_var(var, 20 / DEC_DIGITS);
	if (val < 0)
	{
		var->sign = NUMERIC_NEG;
		uval = pg_abs_s64(val);
	}
	else
	{
		var->sign = NUMERIC_POS;
		uval = val;
	}
	var->dscale = 0;
	if (val == 0)
	{
		var->ndigits = 0;
		var->weight = 0;
		return;
	}
	ptr = var->digits + var->ndigits;
	ndigits = 0;
	do
	{
		ptr--;
		ndigits++;
		newuval = uval / NBASE;
		*ptr = uval - newuval * NBASE;
		uval = newuval;
	} while (uval);
	var->digits = ptr;
	var->ndigits = ndigits;
	var->weight = ndigits - 1;
}

/* --- src/backend/utils/adt/numeric.c:4578-4593 numericvar_to_int32 (verbatim) --- */
static bool
numericvar_to_int32(const NumericVar *var, int32 *result)
{
	int64		val;

	if (!numericvar_to_int64(var, &val))
		return false;

	if (unlikely(val < PG_INT32_MIN) || unlikely(val > PG_INT32_MAX))
		return false;

	/* Down-convert to int4 */
	*result = (int32) val;

	return true;
}

/* --- src/backend/utils/adt/numeric.c:7071-7079 alloc_var (verbatim) --- */
static void
alloc_var(NumericVar *var, int ndigits)
{
	digitbuf_free(var->buf);
	var->buf = digitbuf_alloc(ndigits + 1);
	var->buf[0] = 0;			/* spare digit for rounding */
	var->digits = var->buf + 1;
	var->ndigits = ndigits;
}

/* --- src/backend/utils/adt/numeric.c:7103-7112 zero_var (verbatim) --- */
static void
zero_var(NumericVar *var)
{
	digitbuf_free(var->buf);
	var->buf = NULL;
	var->digits = NULL;
	var->ndigits = 0;
	var->weight = 0;			/* by convention; doesn't really matter */
	var->sign = NUMERIC_POS;	/* anything but NAN... */
}

/* --- src/backend/utils/adt/numeric.c:12276-12303 strip_var (verbatim) --- */
static void
strip_var(NumericVar *var)
{
	NumericDigit *digits = var->digits;
	int			ndigits = var->ndigits;

	/* Strip leading zeroes */
	while (ndigits > 0 && *digits == 0)
	{
		digits++;
		var->weight--;
		ndigits--;
	}

	/* Strip trailing zeroes */
	while (ndigits > 0 && digits[ndigits - 1] == 0)
		ndigits--;

	/* If it's zero, normalize the sign and weight */
	if (ndigits == 0)
	{
		var->sign = NUMERIC_POS;
		var->weight = 0;
	}

	var->digits = digits;
	var->ndigits = ndigits;
}

/* --- src/backend/utils/adt/numeric.c:8549-8657 add_var (verbatim) --- */
static void
add_var(const NumericVar *var1, const NumericVar *var2, NumericVar *result)
{
	/*
	 * Decide on the signs of the two variables what to do
	 */
	if (var1->sign == NUMERIC_POS)
	{
		if (var2->sign == NUMERIC_POS)
		{
			/*
			 * Both are positive result = +(ABS(var1) + ABS(var2))
			 */
			add_abs(var1, var2, result);
			result->sign = NUMERIC_POS;
		}
		else
		{
			/*
			 * var1 is positive, var2 is negative Must compare absolute values
			 */
			switch (cmp_abs(var1, var2))
			{
				case 0:
					/* ----------
					 * ABS(var1) == ABS(var2)
					 * result = ZERO
					 * ----------
					 */
					zero_var(result);
					result->dscale = Max(var1->dscale, var2->dscale);
					break;

				case 1:
					/* ----------
					 * ABS(var1) > ABS(var2)
					 * result = +(ABS(var1) - ABS(var2))
					 * ----------
					 */
					sub_abs(var1, var2, result);
					result->sign = NUMERIC_POS;
					break;

				case -1:
					/* ----------
					 * ABS(var1) < ABS(var2)
					 * result = -(ABS(var2) - ABS(var1))
					 * ----------
					 */
					sub_abs(var2, var1, result);
					result->sign = NUMERIC_NEG;
					break;
			}
		}
	}
	else
	{
		if (var2->sign == NUMERIC_POS)
		{
			/* ----------
			 * var1 is negative, var2 is positive
			 * Must compare absolute values
			 * ----------
			 */
			switch (cmp_abs(var1, var2))
			{
				case 0:
					/* ----------
					 * ABS(var1) == ABS(var2)
					 * result = ZERO
					 * ----------
					 */
					zero_var(result);
					result->dscale = Max(var1->dscale, var2->dscale);
					break;

				case 1:
					/* ----------
					 * ABS(var1) > ABS(var2)
					 * result = -(ABS(var1) - ABS(var2))
					 * ----------
					 */
					sub_abs(var1, var2, result);
					result->sign = NUMERIC_NEG;
					break;

				case -1:
					/* ----------
					 * ABS(var1) < ABS(var2)
					 * result = +(ABS(var2) - ABS(var1))
					 * ----------
					 */
					sub_abs(var2, var1, result);
					result->sign = NUMERIC_POS;
					break;
			}
		}
		else
		{
			/* ----------
			 * Both are negative
			 * result = -(ABS(var1) + ABS(var2))
			 * ----------
			 */
			add_abs(var1, var2, result);
			result->sign = NUMERIC_NEG;
		}
	}
}

/* --- src/backend/utils/adt/numeric.c:8666-8778 sub_var (verbatim) --- */
static void
sub_var(const NumericVar *var1, const NumericVar *var2, NumericVar *result)
{
	/*
	 * Decide on the signs of the two variables what to do
	 */
	if (var1->sign == NUMERIC_POS)
	{
		if (var2->sign == NUMERIC_NEG)
		{
			/* ----------
			 * var1 is positive, var2 is negative
			 * result = +(ABS(var1) + ABS(var2))
			 * ----------
			 */
			add_abs(var1, var2, result);
			result->sign = NUMERIC_POS;
		}
		else
		{
			/* ----------
			 * Both are positive
			 * Must compare absolute values
			 * ----------
			 */
			switch (cmp_abs(var1, var2))
			{
				case 0:
					/* ----------
					 * ABS(var1) == ABS(var2)
					 * result = ZERO
					 * ----------
					 */
					zero_var(result);
					result->dscale = Max(var1->dscale, var2->dscale);
					break;

				case 1:
					/* ----------
					 * ABS(var1) > ABS(var2)
					 * result = +(ABS(var1) - ABS(var2))
					 * ----------
					 */
					sub_abs(var1, var2, result);
					result->sign = NUMERIC_POS;
					break;

				case -1:
					/* ----------
					 * ABS(var1) < ABS(var2)
					 * result = -(ABS(var2) - ABS(var1))
					 * ----------
					 */
					sub_abs(var2, var1, result);
					result->sign = NUMERIC_NEG;
					break;
			}
		}
	}
	else
	{
		if (var2->sign == NUMERIC_NEG)
		{
			/* ----------
			 * Both are negative
			 * Must compare absolute values
			 * ----------
			 */
			switch (cmp_abs(var1, var2))
			{
				case 0:
					/* ----------
					 * ABS(var1) == ABS(var2)
					 * result = ZERO
					 * ----------
					 */
					zero_var(result);
					result->dscale = Max(var1->dscale, var2->dscale);
					break;

				case 1:
					/* ----------
					 * ABS(var1) > ABS(var2)
					 * result = -(ABS(var1) - ABS(var2))
					 * ----------
					 */
					sub_abs(var1, var2, result);
					result->sign = NUMERIC_NEG;
					break;

				case -1:
					/* ----------
					 * ABS(var1) < ABS(var2)
					 * result = +(ABS(var2) - ABS(var1))
					 * ----------
					 */
					sub_abs(var2, var1, result);
					result->sign = NUMERIC_POS;
					break;
			}
		}
		else
		{
			/* ----------
			 * var1 is negative, var2 is positive
			 * result = -(ABS(var1) + ABS(var2))
			 * ----------
			 */
			add_abs(var1, var2, result);
			result->sign = NUMERIC_NEG;
		}
	}
}

/* --- src/backend/utils/adt/numeric.c:9365-9897 div_var (verbatim) --- */
static void
div_var(const NumericVar *var1, const NumericVar *var2, NumericVar *result,
		int rscale, bool round, bool exact)
{
	int			var1ndigits = var1->ndigits;
	int			var2ndigits = var2->ndigits;
	int			res_sign;
	int			res_weight;
	int			res_ndigits;
	int			var1ndigitpairs;
	int			var2ndigitpairs;
	int			res_ndigitpairs;
	int			div_ndigitpairs;
	int64	   *dividend;
	int32	   *divisor;
	double		fdivisor,
				fdivisorinverse,
				fdividend,
				fquotient;
	int64		maxdiv;
	int			qi;
	int32		qdigit;
	int64		carry;
	int64		newdig;
	int64	   *remainder;
	NumericDigit *res_digits;
	int			i;

	/*
	 * First of all division by zero check; we must not be handed an
	 * unnormalized divisor.
	 */
	if (var2ndigits == 0 || var2->digits[0] == 0)
		ereport(ERROR,
				(errcode(ERRCODE_DIVISION_BY_ZERO),
				 errmsg("division by zero")));

	/*
	 * If the divisor has just one or two digits, delegate to div_var_int(),
	 * which uses fast short division.
	 *
	 * Similarly, on platforms with 128-bit integer support, delegate to
	 * div_var_int64() for divisors with three or four digits.
	 */
	if (var2ndigits <= 2)
	{
		int			idivisor;
		int			idivisor_weight;

		idivisor = var2->digits[0];
		idivisor_weight = var2->weight;
		if (var2ndigits == 2)
		{
			idivisor = idivisor * NBASE + var2->digits[1];
			idivisor_weight--;
		}
		if (var2->sign == NUMERIC_NEG)
			idivisor = -idivisor;

		div_var_int(var1, idivisor, idivisor_weight, result, rscale, round);
		return;
	}
#ifdef HAVE_INT128
	if (var2ndigits <= 4)
	{
		int64		idivisor;
		int			idivisor_weight;

		idivisor = var2->digits[0];
		idivisor_weight = var2->weight;
		for (i = 1; i < var2ndigits; i++)
		{
			idivisor = idivisor * NBASE + var2->digits[i];
			idivisor_weight--;
		}
		if (var2->sign == NUMERIC_NEG)
			idivisor = -idivisor;

		div_var_int64(var1, idivisor, idivisor_weight, result, rscale, round);
		return;
	}
#endif

	/*
	 * Otherwise, perform full long division.
	 */

	/* Result zero check */
	if (var1ndigits == 0)
	{
		zero_var(result);
		result->dscale = rscale;
		return;
	}

	/*
	 * The approximate computation can be significantly faster than the exact
	 * one, since the working dividend is var2ndigitpairs base-NBASE^2 digits
	 * shorter below.  However, that comes with the tradeoff of computing
	 * DIV_GUARD_DIGITS extra base-NBASE result digits.  Ignoring all other
	 * overheads, that suggests that, in theory, the approximate computation
	 * will only be faster than the exact one when var2ndigits is greater than
	 * 2 * (DIV_GUARD_DIGITS + 1), independent of the size of var1.
	 *
	 * Thus, we're better off doing an exact computation when var2 is shorter
	 * than this.  Empirically, it has been found that the exact threshold is
	 * a little higher, due to other overheads in the outer division loop.
	 */
	if (var2ndigits <= 2 * (DIV_GUARD_DIGITS + 2))
		exact = true;

	/*
	 * Determine the result sign, weight and number of digits to calculate.
	 * The weight figured here is correct if the emitted quotient has no
	 * leading zero digits; otherwise strip_var() will fix things up.
	 */
	if (var1->sign == var2->sign)
		res_sign = NUMERIC_POS;
	else
		res_sign = NUMERIC_NEG;
	res_weight = var1->weight - var2->weight + 1;
	/* The number of accurate result digits we need to produce: */
	res_ndigits = res_weight + 1 + (rscale + DEC_DIGITS - 1) / DEC_DIGITS;
	/* ... but always at least 1 */
	res_ndigits = Max(res_ndigits, 1);
	/* If rounding needed, figure one more digit to ensure correct result */
	if (round)
		res_ndigits++;
	/* Add guard digits for roundoff error when producing approx result */
	if (!exact)
		res_ndigits += DIV_GUARD_DIGITS;

	/*
	 * The computation itself is done using base-NBASE^2 arithmetic, so we
	 * actually process the input digits in pairs, producing a base-NBASE^2
	 * intermediate result.  This significantly improves performance, since
	 * the computation is O(N^2) in the number of input digits, and working in
	 * base NBASE^2 effectively halves "N".
	 */
	var1ndigitpairs = (var1ndigits + 1) / 2;
	var2ndigitpairs = (var2ndigits + 1) / 2;
	res_ndigitpairs = (res_ndigits + 1) / 2;
	res_ndigits = 2 * res_ndigitpairs;

	/*
	 * We do the arithmetic in an array "dividend[]" of signed 64-bit
	 * integers.  Since PG_INT64_MAX is much larger than NBASE^4, this gives
	 * us a lot of headroom to avoid normalizing carries immediately.
	 *
	 * When performing an exact computation, the working dividend requires
	 * res_ndigitpairs + var2ndigitpairs digits.  If var1 is larger than that,
	 * the extra digits do not contribute to the result, and are ignored.
	 *
	 * When performing an approximate computation, the working dividend only
	 * requires res_ndigitpairs digits (which includes the extra guard
	 * digits).  All input digits beyond that are ignored.
	 */
	if (exact)
	{
		div_ndigitpairs = res_ndigitpairs + var2ndigitpairs;
		var1ndigitpairs = Min(var1ndigitpairs, div_ndigitpairs);
	}
	else
	{
		div_ndigitpairs = res_ndigitpairs;
		var1ndigitpairs = Min(var1ndigitpairs, div_ndigitpairs);
		var2ndigitpairs = Min(var2ndigitpairs, div_ndigitpairs);
	}

	/*
	 * Allocate room for the working dividend (div_ndigitpairs 64-bit digits)
	 * plus the divisor (var2ndigitpairs 32-bit base-NBASE^2 digits).
	 *
	 * For convenience, we allocate one extra dividend digit, which is set to
	 * zero and not counted in div_ndigitpairs, so that the main loop below
	 * can safely read and write the (qi+1)'th digit in the approximate case.
	 */
	dividend = (int64 *) palloc((div_ndigitpairs + 1) * sizeof(int64) +
								var2ndigitpairs * sizeof(int32));
	divisor = (int32 *) (dividend + div_ndigitpairs + 1);

	/* load var1 into dividend[0 .. var1ndigitpairs-1], zeroing the rest */
	for (i = 0; i < var1ndigitpairs - 1; i++)
		dividend[i] = var1->digits[2 * i] * NBASE + var1->digits[2 * i + 1];

	if (2 * i + 1 < var1ndigits)
		dividend[i] = var1->digits[2 * i] * NBASE + var1->digits[2 * i + 1];
	else
		dividend[i] = var1->digits[2 * i] * NBASE;

	memset(dividend + i + 1, 0, (div_ndigitpairs - i) * sizeof(int64));

	/* load var2 into divisor[0 .. var2ndigitpairs-1] */
	for (i = 0; i < var2ndigitpairs - 1; i++)
		divisor[i] = var2->digits[2 * i] * NBASE + var2->digits[2 * i + 1];

	if (2 * i + 1 < var2ndigits)
		divisor[i] = var2->digits[2 * i] * NBASE + var2->digits[2 * i + 1];
	else
		divisor[i] = var2->digits[2 * i] * NBASE;

	/*
	 * We estimate each quotient digit using floating-point arithmetic, taking
	 * the first 2 base-NBASE^2 digits of the (current) dividend and divisor.
	 * This must be float to avoid overflow.
	 *
	 * Since the floating-point dividend and divisor use 4 base-NBASE input
	 * digits, they include roughly 40-53 bits of information from their
	 * respective inputs (assuming NBASE is 10000), which fits well in IEEE
	 * double-precision variables.  The relative error in the floating-point
	 * quotient digit will then be less than around 2/NBASE^3, so the
	 * estimated base-NBASE^2 quotient digit will typically be correct, and
	 * should not be off by more than one from the correct value.
	 */
	fdivisor = (double) divisor[0] * NBASE_SQR;
	if (var2ndigitpairs > 1)
		fdivisor += (double) divisor[1];
	fdivisorinverse = 1.0 / fdivisor;

	/*
	 * maxdiv tracks the maximum possible absolute value of any dividend[]
	 * entry; when this threatens to exceed PG_INT64_MAX, we take the time to
	 * propagate carries.  Furthermore, we need to ensure that overflow
	 * doesn't occur during the carry propagation passes either.  The carry
	 * values may have an absolute value as high as PG_INT64_MAX/NBASE^2 + 1,
	 * so really we must normalize when digits threaten to exceed PG_INT64_MAX
	 * - PG_INT64_MAX/NBASE^2 - 1.
	 *
	 * To avoid overflow in maxdiv itself, it represents the max absolute
	 * value divided by NBASE^2-1, i.e., at the top of the loop it is known
	 * that no dividend[] entry has an absolute value exceeding maxdiv *
	 * (NBASE^2-1).
	 *
	 * Actually, though, that holds good only for dividend[] entries after
	 * dividend[qi]; the adjustment done at the bottom of the loop may cause
	 * dividend[qi + 1] to exceed the maxdiv limit, so that dividend[qi] in
	 * the next iteration is beyond the limit.  This does not cause problems,
	 * as explained below.
	 */
	maxdiv = 1;

	/*
	 * Outer loop computes next quotient digit, which goes in dividend[qi].
	 */
	for (qi = 0; qi < res_ndigitpairs; qi++)
	{
		/* Approximate the current dividend value */
		fdividend = (double) dividend[qi] * NBASE_SQR;
		fdividend += (double) dividend[qi + 1];

		/* Compute the (approximate) quotient digit */
		fquotient = fdividend * fdivisorinverse;
		qdigit = (fquotient >= 0.0) ? ((int32) fquotient) :
			(((int32) fquotient) - 1);	/* truncate towards -infinity */

		if (qdigit != 0)
		{
			/* Do we need to normalize now? */
			maxdiv += i64abs(qdigit);
			if (maxdiv > (PG_INT64_MAX - PG_INT64_MAX / NBASE_SQR - 1) / (NBASE_SQR - 1))
			{
				/*
				 * Yes, do it.  Note that if var2ndigitpairs is much smaller
				 * than div_ndigitpairs, we can save a significant amount of
				 * effort here by noting that we only need to normalise those
				 * dividend[] entries touched where prior iterations
				 * subtracted multiples of the divisor.
				 */
				carry = 0;
				for (i = Min(qi + var2ndigitpairs - 2, div_ndigitpairs - 1); i > qi; i--)
				{
					newdig = dividend[i] + carry;
					if (newdig < 0)
					{
						carry = -((-newdig - 1) / NBASE_SQR) - 1;
						newdig -= carry * NBASE_SQR;
					}
					else if (newdig >= NBASE_SQR)
					{
						carry = newdig / NBASE_SQR;
						newdig -= carry * NBASE_SQR;
					}
					else
						carry = 0;
					dividend[i] = newdig;
				}
				dividend[qi] += carry;

				/*
				 * All the dividend[] digits except possibly dividend[qi] are
				 * now in the range 0..NBASE^2-1.  We do not need to consider
				 * dividend[qi] in the maxdiv value anymore, so we can reset
				 * maxdiv to 1.
				 */
				maxdiv = 1;

				/*
				 * Recompute the quotient digit since new info may have
				 * propagated into the top two dividend digits.
				 */
				fdividend = (double) dividend[qi] * NBASE_SQR;
				fdividend += (double) dividend[qi + 1];
				fquotient = fdividend * fdivisorinverse;
				qdigit = (fquotient >= 0.0) ? ((int32) fquotient) :
					(((int32) fquotient) - 1);	/* truncate towards -infinity */

				maxdiv += i64abs(qdigit);
			}

			/*
			 * Subtract off the appropriate multiple of the divisor.
			 *
			 * The digits beyond dividend[qi] cannot overflow, because we know
			 * they will fall within the maxdiv limit.  As for dividend[qi]
			 * itself, note that qdigit is approximately trunc(dividend[qi] /
			 * divisor[0]), which would make the new value simply dividend[qi]
			 * mod divisor[0].  The lower-order terms in qdigit can change
			 * this result by not more than about twice PG_INT64_MAX/NBASE^2,
			 * so overflow is impossible.
			 *
			 * This inner loop is the performance bottleneck for division, so
			 * code it in the same way as the inner loop of mul_var() so that
			 * it can be auto-vectorized.
			 */
			if (qdigit != 0)
			{
				int			istop = Min(var2ndigitpairs, div_ndigitpairs - qi);
				int64	   *dividend_qi = &dividend[qi];

				for (i = 0; i < istop; i++)
					dividend_qi[i] -= (int64) qdigit * divisor[i];
			}
		}

		/*
		 * The dividend digit we are about to replace might still be nonzero.
		 * Fold it into the next digit position.
		 *
		 * There is no risk of overflow here, although proving that requires
		 * some care.  Much as with the argument for dividend[qi] not
		 * overflowing, if we consider the first two terms in the numerator
		 * and denominator of qdigit, we can see that the final value of
		 * dividend[qi + 1] will be approximately a remainder mod
		 * (divisor[0]*NBASE^2 + divisor[1]).  Accounting for the lower-order
		 * terms is a bit complicated but ends up adding not much more than
		 * PG_INT64_MAX/NBASE^2 to the possible range.  Thus, dividend[qi + 1]
		 * cannot overflow here, and in its role as dividend[qi] in the next
		 * loop iteration, it can't be large enough to cause overflow in the
		 * carry propagation step (if any), either.
		 *
		 * But having said that: dividend[qi] can be more than
		 * PG_INT64_MAX/NBASE^2, as noted above, which means that the product
		 * dividend[qi] * NBASE^2 *can* overflow.  When that happens, adding
		 * it to dividend[qi + 1] will always cause a canceling overflow so
		 * that the end result is correct.  We could avoid the intermediate
		 * overflow by doing the multiplication and addition using unsigned
		 * int64 arithmetic, which is modulo 2^64, but so far there appears no
		 * need.
		 */
		dividend[qi + 1] += dividend[qi] * NBASE_SQR;

		dividend[qi] = qdigit;
	}

	/*
	 * If an exact result was requested, use the remainder to correct the
	 * approximate quotient.  The remainder is in dividend[], immediately
	 * after the quotient digits.  Note, however, that although the remainder
	 * starts at dividend[qi = res_ndigitpairs], the first digit is the result
	 * of folding two remainder digits into one above, and the remainder
	 * currently only occupies var2ndigitpairs - 1 digits (the last digit of
	 * the working dividend was untouched by the computation above).  Thus we
	 * expand the remainder down by one base-NBASE^2 digit when we normalize
	 * it, so that it completely fills the last var2ndigitpairs digits of the
	 * dividend array.
	 */
	if (exact)
	{
		/* Normalize the remainder, expanding it down by one digit */
		remainder = &dividend[qi];
		carry = 0;
		for (i = var2ndigitpairs - 2; i >= 0; i--)
		{
			newdig = remainder[i] + carry;
			if (newdig < 0)
			{
				carry = -((-newdig - 1) / NBASE_SQR) - 1;
				newdig -= carry * NBASE_SQR;
			}
			else if (newdig >= NBASE_SQR)
			{
				carry = newdig / NBASE_SQR;
				newdig -= carry * NBASE_SQR;
			}
			else
				carry = 0;
			remainder[i + 1] = newdig;
		}
		remainder[0] = carry;

		if (remainder[0] < 0)
		{
			/*
			 * The remainder is negative, so the approximate quotient is too
			 * large.  Correct by reducing the quotient by one and adding the
			 * divisor to the remainder until the remainder is positive.  We
			 * expect the quotient to be off by at most one, which has been
			 * borne out in all testing, but not conclusively proven, so we
			 * allow for larger corrections, just in case.
			 */
			do
			{
				/* Add the divisor to the remainder */
				carry = 0;
				for (i = var2ndigitpairs - 1; i > 0; i--)
				{
					newdig = remainder[i] + divisor[i] + carry;
					if (newdig >= NBASE_SQR)
					{
						remainder[i] = newdig - NBASE_SQR;
						carry = 1;
					}
					else
					{
						remainder[i] = newdig;
						carry = 0;
					}
				}
				remainder[0] += divisor[0] + carry;

				/* Subtract 1 from the quotient (propagating carries later) */
				dividend[qi - 1]--;

			} while (remainder[0] < 0);
		}
		else
		{
			/*
			 * The remainder is nonnegative.  If it's greater than or equal to
			 * the divisor, then the approximate quotient is too small and
			 * must be corrected.  As above, we don't expect to have to apply
			 * more than one correction, but allow for it just in case.
			 */
			while (true)
			{
				bool		less = false;

				/* Is remainder < divisor? */
				for (i = 0; i < var2ndigitpairs; i++)
				{
					if (remainder[i] < divisor[i])
					{
						less = true;
						break;
					}
					if (remainder[i] > divisor[i])
						break;	/* remainder > divisor */
				}
				if (less)
					break;		/* quotient is correct */

				/* Subtract the divisor from the remainder */
				carry = 0;
				for (i = var2ndigitpairs - 1; i > 0; i--)
				{
					newdig = remainder[i] - divisor[i] + carry;
					if (newdig < 0)
					{
						remainder[i] = newdig + NBASE_SQR;
						carry = -1;
					}
					else
					{
						remainder[i] = newdig;
						carry = 0;
					}
				}
				remainder[0] = remainder[0] - divisor[0] + carry;

				/* Add 1 to the quotient (propagating carries later) */
				dividend[qi - 1]++;
			}
		}
	}

	/*
	 * Because the quotient digits were estimates that might have been off by
	 * one (and we didn't bother propagating carries when adjusting the
	 * quotient above), some quotient digits might be out of range, so do a
	 * final carry propagation pass to normalize back to base NBASE^2, and
	 * construct the base-NBASE result digits.  Note that this is still done
	 * at full precision w/guard digits.
	 */
	alloc_var(result, res_ndigits);
	res_digits = result->digits;
	carry = 0;
	for (i = res_ndigitpairs - 1; i >= 0; i--)
	{
		newdig = dividend[i] + carry;
		if (newdig < 0)
		{
			carry = -((-newdig - 1) / NBASE_SQR) - 1;
			newdig -= carry * NBASE_SQR;
		}
		else if (newdig >= NBASE_SQR)
		{
			carry = newdig / NBASE_SQR;
			newdig -= carry * NBASE_SQR;
		}
		else
			carry = 0;
		res_digits[2 * i + 1] = (NumericDigit) ((uint32) newdig % NBASE);
		res_digits[2 * i] = (NumericDigit) ((uint32) newdig / NBASE);
	}
	Assert(carry == 0);

	pfree(dividend);

	/*
	 * Finally, round or truncate the result to the requested precision.
	 */
	result->weight = res_weight;
	result->sign = res_sign;

	/* Round or truncate to target rscale (and set result->dscale) */
	if (round)
		round_var(result, rscale);
	else
		trunc_var(result, rscale);

	/* Strip leading and trailing zeroes */
	strip_var(result);
}

/* --- src/backend/utils/adt/numeric.c:9906-10009 div_var_int (verbatim) --- */
static void
div_var_int(const NumericVar *var, int ival, int ival_weight,
			NumericVar *result, int rscale, bool round)
{
	NumericDigit *var_digits = var->digits;
	int			var_ndigits = var->ndigits;
	int			res_sign;
	int			res_weight;
	int			res_ndigits;
	NumericDigit *res_buf;
	NumericDigit *res_digits;
	uint32		divisor;
	int			i;

	/* Guard against division by zero */
	if (ival == 0)
		ereport(ERROR,
				errcode(ERRCODE_DIVISION_BY_ZERO),
				errmsg("division by zero"));

	/* Result zero check */
	if (var_ndigits == 0)
	{
		zero_var(result);
		result->dscale = rscale;
		return;
	}

	/*
	 * Determine the result sign, weight and number of digits to calculate.
	 * The weight figured here is correct if the emitted quotient has no
	 * leading zero digits; otherwise strip_var() will fix things up.
	 */
	if (var->sign == NUMERIC_POS)
		res_sign = ival > 0 ? NUMERIC_POS : NUMERIC_NEG;
	else
		res_sign = ival > 0 ? NUMERIC_NEG : NUMERIC_POS;
	res_weight = var->weight - ival_weight;
	/* The number of accurate result digits we need to produce: */
	res_ndigits = res_weight + 1 + (rscale + DEC_DIGITS - 1) / DEC_DIGITS;
	/* ... but always at least 1 */
	res_ndigits = Max(res_ndigits, 1);
	/* If rounding needed, figure one more digit to ensure correct result */
	if (round)
		res_ndigits++;

	res_buf = digitbuf_alloc(res_ndigits + 1);
	res_buf[0] = 0;				/* spare digit for later rounding */
	res_digits = res_buf + 1;

	/*
	 * Now compute the quotient digits.  This is the short division algorithm
	 * described in Knuth volume 2, section 4.3.1 exercise 16, except that we
	 * allow the divisor to exceed the internal base.
	 *
	 * In this algorithm, the carry from one digit to the next is at most
	 * divisor - 1.  Therefore, while processing the next digit, carry may
	 * become as large as divisor * NBASE - 1, and so it requires a 64-bit
	 * integer if this exceeds UINT_MAX.
	 */
	divisor = abs(ival);

	if (divisor <= UINT_MAX / NBASE)
	{
		/* carry cannot overflow 32 bits */
		uint32		carry = 0;

		for (i = 0; i < res_ndigits; i++)
		{
			carry = carry * NBASE + (i < var_ndigits ? var_digits[i] : 0);
			res_digits[i] = (NumericDigit) (carry / divisor);
			carry = carry % divisor;
		}
	}
	else
	{
		/* carry may exceed 32 bits */
		uint64		carry = 0;

		for (i = 0; i < res_ndigits; i++)
		{
			carry = carry * NBASE + (i < var_ndigits ? var_digits[i] : 0);
			res_digits[i] = (NumericDigit) (carry / divisor);
			carry = carry % divisor;
		}
	}

	/* Store the quotient in result */
	digitbuf_free(result->buf);
	result->ndigits = res_ndigits;
	result->buf = res_buf;
	result->digits = res_digits;
	result->weight = res_weight;
	result->sign = res_sign;

	/* Round or truncate to target rscale (and set result->dscale) */
	if (round)
		round_var(result, rscale);
	else
		trunc_var(result, rscale);

	/* Strip leading/trailing zeroes */
	strip_var(result);
}

/* --- src/backend/utils/adt/numeric.c:10899-11014 exp_var (verbatim) --- */
static void
exp_var(const NumericVar *arg, NumericVar *result, int rscale)
{
	NumericVar	x;
	NumericVar	elem;
	int			ni;
	double		val;
	int			dweight;
	int			ndiv2;
	int			sig_digits;
	int			local_rscale;

	init_var(&x);
	init_var(&elem);

	set_var_from_var(arg, &x);

	/*
	 * Estimate the dweight of the result using floating point arithmetic, so
	 * that we can choose an appropriate local rscale for the calculation.
	 */
	val = numericvar_to_double_no_overflow(&x);

	/* Guard against overflow/underflow */
	/* If you change this limit, see also power_var()'s limit */
	if (fabs(val) >= NUMERIC_MAX_RESULT_SCALE * 3)
	{
		if (val > 0)
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					 errmsg("value overflows numeric format")));
		zero_var(result);
		result->dscale = rscale;
		return;
	}

	/* decimal weight = log10(e^x) = x * log10(e) */
	dweight = (int) (val * 0.434294481903252);

	/*
	 * Reduce x to the range -0.01 <= x <= 0.01 (approximately) by dividing by
	 * 2^ndiv2, to improve the convergence rate of the Taylor series.
	 *
	 * Note that the overflow check above ensures that fabs(x) < 6000, which
	 * means that ndiv2 <= 20 here.
	 */
	if (fabs(val) > 0.01)
	{
		ndiv2 = 1;
		val /= 2;

		while (fabs(val) > 0.01)
		{
			ndiv2++;
			val /= 2;
		}

		local_rscale = x.dscale + ndiv2;
		div_var_int(&x, 1 << ndiv2, 0, &x, local_rscale, true);
	}
	else
		ndiv2 = 0;

	/*
	 * Set the scale for the Taylor series expansion.  The final result has
	 * (dweight + rscale + 1) significant digits.  In addition, we have to
	 * raise the Taylor series result to the power 2^ndiv2, which introduces
	 * an error of up to around log10(2^ndiv2) digits, so work with this many
	 * extra digits of precision (plus a few more for good measure).
	 */
	sig_digits = 1 + dweight + rscale + (int) (ndiv2 * 0.301029995663981);
	sig_digits = Max(sig_digits, 0) + 8;

	local_rscale = sig_digits - 1;

	/*
	 * Use the Taylor series
	 *
	 * exp(x) = 1 + x + x^2/2! + x^3/3! + ...
	 *
	 * Given the limited range of x, this should converge reasonably quickly.
	 * We run the series until the terms fall below the local_rscale limit.
	 */
	add_var(&const_one, &x, result);

	mul_var(&x, &x, &elem, local_rscale);
	ni = 2;
	div_var_int(&elem, ni, 0, &elem, local_rscale, true);

	while (elem.ndigits != 0)
	{
		add_var(result, &elem, result);

		mul_var(&elem, &x, &elem, local_rscale);
		ni++;
		div_var_int(&elem, ni, 0, &elem, local_rscale, true);
	}

	/*
	 * Compensate for the argument range reduction.  Since the weight of the
	 * result doubles with each multiplication, we can reduce the local rscale
	 * as we proceed.
	 */
	while (ndiv2-- > 0)
	{
		local_rscale = sig_digits - result->weight * 2 * DEC_DIGITS;
		local_rscale = Max(local_rscale, NUMERIC_MIN_DISPLAY_SCALE);
		mul_var(result, result, result, local_rscale);
	}

	/* Round to requested rscale */
	round_var(result, rscale);

	free_var(&x);
	free_var(&elem);
}

/* --- src/backend/utils/adt/numeric.c:11110-11218 ln_var (verbatim) --- */
static void
ln_var(const NumericVar *arg, NumericVar *result, int rscale)
{
	NumericVar	x;
	NumericVar	xx;
	int			ni;
	NumericVar	elem;
	NumericVar	fact;
	int			nsqrt;
	int			local_rscale;
	int			cmp;

	cmp = cmp_var(arg, &const_zero);
	if (cmp == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_ARGUMENT_FOR_LOG),
				 errmsg("cannot take logarithm of zero")));
	else if (cmp < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_ARGUMENT_FOR_LOG),
				 errmsg("cannot take logarithm of a negative number")));

	init_var(&x);
	init_var(&xx);
	init_var(&elem);
	init_var(&fact);

	set_var_from_var(arg, &x);
	set_var_from_var(&const_two, &fact);

	/*
	 * Reduce input into range 0.9 < x < 1.1 with repeated sqrt() operations.
	 *
	 * The final logarithm will have up to around rscale+6 significant digits.
	 * Each sqrt() will roughly halve the weight of x, so adjust the local
	 * rscale as we work so that we keep this many significant digits at each
	 * step (plus a few more for good measure).
	 *
	 * Note that we allow local_rscale < 0 during this input reduction
	 * process, which implies rounding before the decimal point.  sqrt_var()
	 * explicitly supports this, and it significantly reduces the work
	 * required to reduce very large inputs to the required range.  Once the
	 * input reduction is complete, x.weight will be 0 and its display scale
	 * will be non-negative again.
	 */
	nsqrt = 0;
	while (cmp_var(&x, &const_zero_point_nine) <= 0)
	{
		local_rscale = rscale - x.weight * DEC_DIGITS / 2 + 8;
		sqrt_var(&x, &x, local_rscale);
		mul_var(&fact, &const_two, &fact, 0);
		nsqrt++;
	}
	while (cmp_var(&x, &const_one_point_one) >= 0)
	{
		local_rscale = rscale - x.weight * DEC_DIGITS / 2 + 8;
		sqrt_var(&x, &x, local_rscale);
		mul_var(&fact, &const_two, &fact, 0);
		nsqrt++;
	}

	/*
	 * We use the Taylor series for 0.5 * ln((1+z)/(1-z)),
	 *
	 * z + z^3/3 + z^5/5 + ...
	 *
	 * where z = (x-1)/(x+1) is in the range (approximately) -0.053 .. 0.048
	 * due to the above range-reduction of x.
	 *
	 * The convergence of this is not as fast as one would like, but is
	 * tolerable given that z is small.
	 *
	 * The Taylor series result will be multiplied by 2^(nsqrt+1), which has a
	 * decimal weight of (nsqrt+1) * log10(2), so work with this many extra
	 * digits of precision (plus a few more for good measure).
	 */
	local_rscale = rscale + (int) ((nsqrt + 1) * 0.301029995663981) + 8;

	sub_var(&x, &const_one, result);
	add_var(&x, &const_one, &elem);
	div_var(result, &elem, result, local_rscale, true, false);
	set_var_from_var(result, &xx);
	mul_var(result, result, &x, local_rscale);

	ni = 1;

	for (;;)
	{
		ni += 2;
		mul_var(&xx, &x, &xx, local_rscale);
		div_var_int(&xx, ni, 0, &elem, local_rscale, true);

		if (elem.ndigits == 0)
			break;

		add_var(result, &elem, result);

		if (elem.weight < (result->weight - local_rscale * 2 / DEC_DIGITS))
			break;
	}

	/* Compensate for argument range reduction, round to requested rscale */
	mul_var(result, &fact, result, rscale);

	free_var(&x);
	free_var(&xx);
	free_var(&elem);
	free_var(&fact);
}

/* --- src/backend/utils/adt/numeric.c:10419-10891 sqrt_var (verbatim) --- */
static void
sqrt_var(const NumericVar *arg, NumericVar *result, int rscale)
{
	int			stat;
	int			res_weight;
	int			res_ndigits;
	int			src_ndigits;
	int			step;
	int			ndigits[32];
	int			blen;
	int64		arg_int64;
	int			src_idx;
	int64		s_int64;
	int64		r_int64;
	NumericVar	s_var;
	NumericVar	r_var;
	NumericVar	a0_var;
	NumericVar	a1_var;
	NumericVar	q_var;
	NumericVar	u_var;

	stat = cmp_var(arg, &const_zero);
	if (stat == 0)
	{
		zero_var(result);
		result->dscale = rscale;
		return;
	}

	/*
	 * SQL2003 defines sqrt() in terms of power, so we need to emit the right
	 * SQLSTATE error code if the operand is negative.
	 */
	if (stat < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION),
				 errmsg("cannot take square root of a negative number")));

	init_var(&s_var);
	init_var(&r_var);
	init_var(&a0_var);
	init_var(&a1_var);
	init_var(&q_var);
	init_var(&u_var);

	/*
	 * The result weight is half the input weight, rounded towards minus
	 * infinity --- res_weight = floor(arg->weight / 2).
	 */
	if (arg->weight >= 0)
		res_weight = arg->weight / 2;
	else
		res_weight = -((-arg->weight - 1) / 2 + 1);

	/*
	 * Number of NBASE digits to compute.  To ensure correct rounding, compute
	 * at least 1 extra decimal digit.  We explicitly allow rscale to be
	 * negative here, but must always compute at least 1 NBASE digit.  Thus
	 * res_ndigits = res_weight + 1 + ceil((rscale + 1) / DEC_DIGITS) or 1.
	 */
	if (rscale + 1 >= 0)
		res_ndigits = res_weight + 1 + (rscale + DEC_DIGITS) / DEC_DIGITS;
	else
		res_ndigits = res_weight + 1 - (-rscale - 1) / DEC_DIGITS;
	res_ndigits = Max(res_ndigits, 1);

	/*
	 * Number of source NBASE digits logically required to produce a result
	 * with this precision --- every digit before the decimal point, plus 2
	 * for each result digit after the decimal point (or minus 2 for each
	 * result digit we round before the decimal point).
	 */
	src_ndigits = arg->weight + 1 + (res_ndigits - res_weight - 1) * 2;
	src_ndigits = Max(src_ndigits, 1);

	/* ----------
	 * From this point on, we treat the input and the result as integers and
	 * compute the integer square root and remainder using the Karatsuba
	 * Square Root algorithm, which may be written recursively as follows:
	 *
	 *	SqrtRem(n = a3*b^3 + a2*b^2 + a1*b + a0):
	 *		[ for some base b, and coefficients a0,a1,a2,a3 chosen so that
	 *		  0 <= a0,a1,a2 < b and a3 >= b/4 ]
	 *		Let (s,r) = SqrtRem(a3*b + a2)
	 *		Let (q,u) = DivRem(r*b + a1, 2*s)
	 *		Let s = s*b + q
	 *		Let r = u*b + a0 - q^2
	 *		If r < 0 Then
	 *			Let r = r + s
	 *			Let s = s - 1
	 *			Let r = r + s
	 *		Return (s,r)
	 *
	 * See "Karatsuba Square Root", Paul Zimmermann, INRIA Research Report
	 * RR-3805, November 1999.  At the time of writing this was available
	 * on the net at <https://hal.inria.fr/inria-00072854>.
	 *
	 * The way to read the assumption "n = a3*b^3 + a2*b^2 + a1*b + a0" is
	 * "choose a base b such that n requires at least four base-b digits to
	 * express; then those digits are a3,a2,a1,a0, with a3 possibly larger
	 * than b".  For optimal performance, b should have approximately a
	 * quarter the number of digits in the input, so that the outer square
	 * root computes roughly twice as many digits as the inner one.  For
	 * simplicity, we choose b = NBASE^blen, an integer power of NBASE.
	 *
	 * We implement the algorithm iteratively rather than recursively, to
	 * allow the working variables to be reused.  With this approach, each
	 * digit of the input is read precisely once --- src_idx tracks the number
	 * of input digits used so far.
	 *
	 * The array ndigits[] holds the number of NBASE digits of the input that
	 * will have been used at the end of each iteration, which roughly doubles
	 * each time.  Note that the array elements are stored in reverse order,
	 * so if the final iteration requires src_ndigits = 37 input digits, the
	 * array will contain [37,19,11,7,5,3], and we would start by computing
	 * the square root of the 3 most significant NBASE digits.
	 *
	 * In each iteration, we choose blen to be the largest integer for which
	 * the input number has a3 >= b/4, when written in the form above.  In
	 * general, this means blen = src_ndigits / 4 (truncated), but if
	 * src_ndigits is a multiple of 4, that might lead to the coefficient a3
	 * being less than b/4 (if the first input digit is less than NBASE/4), in
	 * which case we choose blen = src_ndigits / 4 - 1.  The number of digits
	 * in the inner square root is then src_ndigits - 2*blen.  So, for
	 * example, if we have src_ndigits = 26 initially, the array ndigits[]
	 * will be either [26,14,8,4] or [26,14,8,6,4], depending on the size of
	 * the first input digit.
	 *
	 * Additionally, we can put an upper bound on the number of steps required
	 * as follows --- suppose that the number of source digits is an n-bit
	 * number in the range [2^(n-1), 2^n-1], then blen will be in the range
	 * [2^(n-3)-1, 2^(n-2)-1] and the number of digits in the inner square
	 * root will be in the range [2^(n-2), 2^(n-1)+1].  In the next step, blen
	 * will be in the range [2^(n-4)-1, 2^(n-3)] and the number of digits in
	 * the next inner square root will be in the range [2^(n-3), 2^(n-2)+1].
	 * This pattern repeats, and in the worst case the array ndigits[] will
	 * contain [2^n-1, 2^(n-1)+1, 2^(n-2)+1, ... 9, 5, 3], and the computation
	 * will require n steps.  Therefore, since all digit array sizes are
	 * signed 32-bit integers, the number of steps required is guaranteed to
	 * be less than 32.
	 * ----------
	 */
	step = 0;
	while ((ndigits[step] = src_ndigits) > 4)
	{
		/* Choose b so that a3 >= b/4, as described above */
		blen = src_ndigits / 4;
		if (blen * 4 == src_ndigits && arg->digits[0] < NBASE / 4)
			blen--;

		/* Number of digits in the next step (inner square root) */
		src_ndigits -= 2 * blen;
		step++;
	}

	/*
	 * First iteration (innermost square root and remainder):
	 *
	 * Here src_ndigits <= 4, and the input fits in an int64.  Its square root
	 * has at most 9 decimal digits, so estimate it using double precision
	 * arithmetic, which will in fact almost certainly return the correct
	 * result with no further correction required.
	 */
	arg_int64 = arg->digits[0];
	for (src_idx = 1; src_idx < src_ndigits; src_idx++)
	{
		arg_int64 *= NBASE;
		if (src_idx < arg->ndigits)
			arg_int64 += arg->digits[src_idx];
	}

	s_int64 = (int64) sqrt((double) arg_int64);
	r_int64 = arg_int64 - s_int64 * s_int64;

	/*
	 * Use Newton's method to correct the result, if necessary.
	 *
	 * This uses integer division with truncation to compute the truncated
	 * integer square root by iterating using the formula x -> (x + n/x) / 2.
	 * This is known to converge to isqrt(n), unless n+1 is a perfect square.
	 * If n+1 is a perfect square, the sequence will oscillate between the two
	 * values isqrt(n) and isqrt(n)+1, so we can be assured of convergence by
	 * checking the remainder.
	 */
	while (r_int64 < 0 || r_int64 > 2 * s_int64)
	{
		s_int64 = (s_int64 + arg_int64 / s_int64) / 2;
		r_int64 = arg_int64 - s_int64 * s_int64;
	}

	/*
	 * Iterations with src_ndigits <= 8:
	 *
	 * The next 1 or 2 iterations compute larger (outer) square roots with
	 * src_ndigits <= 8, so the result still fits in an int64 (even though the
	 * input no longer does) and we can continue to compute using int64
	 * variables to avoid more expensive numeric computations.
	 *
	 * It is fairly easy to see that there is no risk of the intermediate
	 * values below overflowing 64-bit integers.  In the worst case, the
	 * previous iteration will have computed a 3-digit square root (of a
	 * 6-digit input less than NBASE^6 / 4), so at the start of this
	 * iteration, s will be less than NBASE^3 / 2 = 10^12 / 2, and r will be
	 * less than 10^12.  In this case, blen will be 1, so numer will be less
	 * than 10^17, and denom will be less than 10^12 (and hence u will also be
	 * less than 10^12).  Finally, since q^2 = u*b + a0 - r, we can also be
	 * sure that q^2 < 10^17.  Therefore all these quantities fit comfortably
	 * in 64-bit integers.
	 */
	step--;
	while (step >= 0 && (src_ndigits = ndigits[step]) <= 8)
	{
		int			b;
		int			a0;
		int			a1;
		int			i;
		int64		numer;
		int64		denom;
		int64		q;
		int64		u;

		blen = (src_ndigits - src_idx) / 2;

		/* Extract a1 and a0, and compute b */
		a0 = 0;
		a1 = 0;
		b = 1;

		for (i = 0; i < blen; i++, src_idx++)
		{
			b *= NBASE;
			a1 *= NBASE;
			if (src_idx < arg->ndigits)
				a1 += arg->digits[src_idx];
		}

		for (i = 0; i < blen; i++, src_idx++)
		{
			a0 *= NBASE;
			if (src_idx < arg->ndigits)
				a0 += arg->digits[src_idx];
		}

		/* Compute (q,u) = DivRem(r*b + a1, 2*s) */
		numer = r_int64 * b + a1;
		denom = 2 * s_int64;
		q = numer / denom;
		u = numer - q * denom;

		/* Compute s = s*b + q and r = u*b + a0 - q^2 */
		s_int64 = s_int64 * b + q;
		r_int64 = u * b + a0 - q * q;

		if (r_int64 < 0)
		{
			/* s is too large by 1; set r += s, s--, r += s */
			r_int64 += s_int64;
			s_int64--;
			r_int64 += s_int64;
		}

		Assert(src_idx == src_ndigits); /* All input digits consumed */
		step--;
	}

	/*
	 * On platforms with 128-bit integer support, we can further delay the
	 * need to use numeric variables.
	 */
#ifdef HAVE_INT128
	if (step >= 0)
	{
		int128		s_int128;
		int128		r_int128;

		s_int128 = s_int64;
		r_int128 = r_int64;

		/*
		 * Iterations with src_ndigits <= 16:
		 *
		 * The result fits in an int128 (even though the input doesn't) so we
		 * use int128 variables to avoid more expensive numeric computations.
		 */
		while (step >= 0 && (src_ndigits = ndigits[step]) <= 16)
		{
			int64		b;
			int64		a0;
			int64		a1;
			int64		i;
			int128		numer;
			int128		denom;
			int128		q;
			int128		u;

			blen = (src_ndigits - src_idx) / 2;

			/* Extract a1 and a0, and compute b */
			a0 = 0;
			a1 = 0;
			b = 1;

			for (i = 0; i < blen; i++, src_idx++)
			{
				b *= NBASE;
				a1 *= NBASE;
				if (src_idx < arg->ndigits)
					a1 += arg->digits[src_idx];
			}

			for (i = 0; i < blen; i++, src_idx++)
			{
				a0 *= NBASE;
				if (src_idx < arg->ndigits)
					a0 += arg->digits[src_idx];
			}

			/* Compute (q,u) = DivRem(r*b + a1, 2*s) */
			numer = r_int128 * b + a1;
			denom = 2 * s_int128;
			q = numer / denom;
			u = numer - q * denom;

			/* Compute s = s*b + q and r = u*b + a0 - q^2 */
			s_int128 = s_int128 * b + q;
			r_int128 = u * b + a0 - q * q;

			if (r_int128 < 0)
			{
				/* s is too large by 1; set r += s, s--, r += s */
				r_int128 += s_int128;
				s_int128--;
				r_int128 += s_int128;
			}

			Assert(src_idx == src_ndigits); /* All input digits consumed */
			step--;
		}

		/*
		 * All remaining iterations require numeric variables.  Convert the
		 * integer values to NumericVar and continue.  Note that in the final
		 * iteration we don't need the remainder, so we can save a few cycles
		 * there by not fully computing it.
		 */
		int128_to_numericvar(s_int128, &s_var);
		if (step >= 0)
			int128_to_numericvar(r_int128, &r_var);
	}
	else
	{
		int64_to_numericvar(s_int64, &s_var);
		/* step < 0, so we certainly don't need r */
	}
#else							/* !HAVE_INT128 */
	int64_to_numericvar(s_int64, &s_var);
	if (step >= 0)
		int64_to_numericvar(r_int64, &r_var);
#endif							/* HAVE_INT128 */

	/*
	 * The remaining iterations with src_ndigits > 8 (or 16, if have int128)
	 * use numeric variables.
	 */
	while (step >= 0)
	{
		int			tmp_len;

		src_ndigits = ndigits[step];
		blen = (src_ndigits - src_idx) / 2;

		/* Extract a1 and a0 */
		if (src_idx < arg->ndigits)
		{
			tmp_len = Min(blen, arg->ndigits - src_idx);
			alloc_var(&a1_var, tmp_len);
			memcpy(a1_var.digits, arg->digits + src_idx,
				   tmp_len * sizeof(NumericDigit));
			a1_var.weight = blen - 1;
			a1_var.sign = NUMERIC_POS;
			a1_var.dscale = 0;
			strip_var(&a1_var);
		}
		else
		{
			zero_var(&a1_var);
			a1_var.dscale = 0;
		}
		src_idx += blen;

		if (src_idx < arg->ndigits)
		{
			tmp_len = Min(blen, arg->ndigits - src_idx);
			alloc_var(&a0_var, tmp_len);
			memcpy(a0_var.digits, arg->digits + src_idx,
				   tmp_len * sizeof(NumericDigit));
			a0_var.weight = blen - 1;
			a0_var.sign = NUMERIC_POS;
			a0_var.dscale = 0;
			strip_var(&a0_var);
		}
		else
		{
			zero_var(&a0_var);
			a0_var.dscale = 0;
		}
		src_idx += blen;

		/* Compute (q,u) = DivRem(r*b + a1, 2*s) */
		set_var_from_var(&r_var, &q_var);
		q_var.weight += blen;
		add_var(&q_var, &a1_var, &q_var);
		add_var(&s_var, &s_var, &u_var);
		div_mod_var(&q_var, &u_var, &q_var, &u_var);

		/* Compute s = s*b + q */
		s_var.weight += blen;
		add_var(&s_var, &q_var, &s_var);

		/*
		 * Compute r = u*b + a0 - q^2.
		 *
		 * In the final iteration, we don't actually need r; we just need to
		 * know whether it is negative, so that we know whether to adjust s.
		 * So instead of the final subtraction we can just compare.
		 */
		u_var.weight += blen;
		add_var(&u_var, &a0_var, &u_var);
		mul_var(&q_var, &q_var, &q_var, 0);

		if (step > 0)
		{
			/* Need r for later iterations */
			sub_var(&u_var, &q_var, &r_var);
			if (r_var.sign == NUMERIC_NEG)
			{
				/* s is too large by 1; set r += s, s--, r += s */
				add_var(&r_var, &s_var, &r_var);
				sub_var(&s_var, &const_one, &s_var);
				add_var(&r_var, &s_var, &r_var);
			}
		}
		else
		{
			/* Don't need r anymore, except to test if s is too large by 1 */
			if (cmp_var(&u_var, &q_var) < 0)
				sub_var(&s_var, &const_one, &s_var);
		}

		Assert(src_idx == src_ndigits); /* All input digits consumed */
		step--;
	}

	/*
	 * Construct the final result, rounding it to the requested precision.
	 */
	set_var_from_var(&s_var, result);
	result->weight = res_weight;
	result->sign = NUMERIC_POS;

	/* Round to target rscale (and set result->dscale) */
	round_var(result, rscale);

	/* Strip leading and trailing zeroes */
	strip_var(result);

	free_var(&s_var);
	free_var(&r_var);
	free_var(&a0_var);
	free_var(&a1_var);
	free_var(&q_var);
	free_var(&u_var);
}

/* --- src/backend/utils/adt/numeric.c:12214-12269 trunc_var (verbatim) --- */
static void
trunc_var(NumericVar *var, int rscale)
{
	int			di;
	int			ndigits;

	var->dscale = rscale;

	/* decimal digits wanted */
	di = (var->weight + 1) * DEC_DIGITS + rscale;

	/*
	 * If di <= 0, the value loses all digits.
	 */
	if (di <= 0)
	{
		var->ndigits = 0;
		var->weight = 0;
		var->sign = NUMERIC_POS;
	}
	else
	{
		/* NBASE digits wanted */
		ndigits = (di + DEC_DIGITS - 1) / DEC_DIGITS;

		if (ndigits <= var->ndigits)
		{
			var->ndigits = ndigits;

#if DEC_DIGITS == 1
			/* no within-digit stuff to worry about */
#else
			/* 0, or number of decimal digits to keep in last NBASE digit */
			di %= DEC_DIGITS;

			if (di > 0)
			{
				/* Must truncate within last NBASE digit */
				NumericDigit *digits = var->digits;
				int			extra,
							pow10;

#if DEC_DIGITS == 4
				pow10 = round_powers[di];
#elif DEC_DIGITS == 2
				pow10 = 10;
#else
#error unsupported NBASE
#endif
				extra = digits[--ndigits] % pow10;
				digits[ndigits] -= extra;
			}
#endif
		}
	}
}

/* --- src/backend/utils/adt/numeric.c:11863-11868 cmp_abs (verbatim) --- */
static int
cmp_abs(const NumericVar *var1, const NumericVar *var2)
{
	return cmp_abs_common(var1->digits, var1->ndigits, var1->weight,
						  var2->digits, var2->ndigits, var2->weight);
}

/* --- src/backend/utils/adt/numeric.c:11877-11932 cmp_abs_common (verbatim) --- */
static int
cmp_abs_common(const NumericDigit *var1digits, int var1ndigits, int var1weight,
			   const NumericDigit *var2digits, int var2ndigits, int var2weight)
{
	int			i1 = 0;
	int			i2 = 0;

	/* Check any digits before the first common digit */

	while (var1weight > var2weight && i1 < var1ndigits)
	{
		if (var1digits[i1++] != 0)
			return 1;
		var1weight--;
	}
	while (var2weight > var1weight && i2 < var2ndigits)
	{
		if (var2digits[i2++] != 0)
			return -1;
		var2weight--;
	}

	/* At this point, either w1 == w2 or we've run out of digits */

	if (var1weight == var2weight)
	{
		while (i1 < var1ndigits && i2 < var2ndigits)
		{
			int			stat = var1digits[i1++] - var2digits[i2++];

			if (stat)
			{
				if (stat > 0)
					return 1;
				return -1;
			}
		}
	}

	/*
	 * At this point, we've run out of digits on one side or the other; so any
	 * remaining nonzero digits imply that side is larger
	 */
	while (i1 < var1ndigits)
	{
		if (var1digits[i1++] != 0)
			return 1;
	}
	while (i2 < var2ndigits)
	{
		if (var2digits[i2++] != 0)
			return -1;
	}

	return 0;
}

/* --- src/backend/utils/adt/numeric.c:11941-12014 add_abs (verbatim) --- */
static void
add_abs(const NumericVar *var1, const NumericVar *var2, NumericVar *result)
{
	NumericDigit *res_buf;
	NumericDigit *res_digits;
	int			res_ndigits;
	int			res_weight;
	int			res_rscale,
				rscale1,
				rscale2;
	int			res_dscale;
	int			i,
				i1,
				i2;
	int			carry = 0;

	/* copy these values into local vars for speed in inner loop */
	int			var1ndigits = var1->ndigits;
	int			var2ndigits = var2->ndigits;
	NumericDigit *var1digits = var1->digits;
	NumericDigit *var2digits = var2->digits;

	res_weight = Max(var1->weight, var2->weight) + 1;

	res_dscale = Max(var1->dscale, var2->dscale);

	/* Note: here we are figuring rscale in base-NBASE digits */
	rscale1 = var1->ndigits - var1->weight - 1;
	rscale2 = var2->ndigits - var2->weight - 1;
	res_rscale = Max(rscale1, rscale2);

	res_ndigits = res_rscale + res_weight + 1;
	if (res_ndigits <= 0)
		res_ndigits = 1;

	res_buf = digitbuf_alloc(res_ndigits + 1);
	res_buf[0] = 0;				/* spare digit for later rounding */
	res_digits = res_buf + 1;

	i1 = res_rscale + var1->weight + 1;
	i2 = res_rscale + var2->weight + 1;
	for (i = res_ndigits - 1; i >= 0; i--)
	{
		i1--;
		i2--;
		if (i1 >= 0 && i1 < var1ndigits)
			carry += var1digits[i1];
		if (i2 >= 0 && i2 < var2ndigits)
			carry += var2digits[i2];

		if (carry >= NBASE)
		{
			res_digits[i] = carry - NBASE;
			carry = 1;
		}
		else
		{
			res_digits[i] = carry;
			carry = 0;
		}
	}

	Assert(carry == 0);			/* else we failed to allow for carry out */

	digitbuf_free(result->buf);
	result->ndigits = res_ndigits;
	result->buf = res_buf;
	result->digits = res_digits;
	result->weight = res_weight;
	result->dscale = res_dscale;

	/* Remove leading/trailing zeroes */
	strip_var(result);
}

/* --- src/backend/utils/adt/numeric.c:12026-12099 sub_abs (verbatim) --- */
static void
sub_abs(const NumericVar *var1, const NumericVar *var2, NumericVar *result)
{
	NumericDigit *res_buf;
	NumericDigit *res_digits;
	int			res_ndigits;
	int			res_weight;
	int			res_rscale,
				rscale1,
				rscale2;
	int			res_dscale;
	int			i,
				i1,
				i2;
	int			borrow = 0;

	/* copy these values into local vars for speed in inner loop */
	int			var1ndigits = var1->ndigits;
	int			var2ndigits = var2->ndigits;
	NumericDigit *var1digits = var1->digits;
	NumericDigit *var2digits = var2->digits;

	res_weight = var1->weight;

	res_dscale = Max(var1->dscale, var2->dscale);

	/* Note: here we are figuring rscale in base-NBASE digits */
	rscale1 = var1->ndigits - var1->weight - 1;
	rscale2 = var2->ndigits - var2->weight - 1;
	res_rscale = Max(rscale1, rscale2);

	res_ndigits = res_rscale + res_weight + 1;
	if (res_ndigits <= 0)
		res_ndigits = 1;

	res_buf = digitbuf_alloc(res_ndigits + 1);
	res_buf[0] = 0;				/* spare digit for later rounding */
	res_digits = res_buf + 1;

	i1 = res_rscale + var1->weight + 1;
	i2 = res_rscale + var2->weight + 1;
	for (i = res_ndigits - 1; i >= 0; i--)
	{
		i1--;
		i2--;
		if (i1 >= 0 && i1 < var1ndigits)
			borrow += var1digits[i1];
		if (i2 >= 0 && i2 < var2ndigits)
			borrow -= var2digits[i2];

		if (borrow < 0)
		{
			res_digits[i] = borrow + NBASE;
			borrow = -1;
		}
		else
		{
			res_digits[i] = borrow;
			borrow = 0;
		}
	}

	Assert(borrow == 0);		/* else caller gave us var1 < var2 */

	digitbuf_free(result->buf);
	result->ndigits = res_ndigits;
	result->buf = res_buf;
	result->digits = res_digits;
	result->weight = res_weight;
	result->dscale = res_dscale;

	/* Remove leading/trailing zeroes */
	strip_var(result);
}

/* --- src/backend/utils/adt/numeric.c:7586-7602 set_var_from_var (verbatim) --- */
static void
set_var_from_var(const NumericVar *value, NumericVar *dest)
{
	NumericDigit *newbuf;

	newbuf = digitbuf_alloc(value->ndigits + 1);
	newbuf[0] = 0;				/* spare digit for rounding */
	if (value->ndigits > 0)		/* else value->digits might be null */
		memcpy(newbuf + 1, value->digits,
			   value->ndigits * sizeof(NumericDigit));

	digitbuf_free(dest->buf);

	memmove(dest, value, sizeof(NumericVar));
	dest->buf = newbuf;
	dest->digits = newbuf + 1;
}

/* --- src/backend/utils/adt/numeric.c:11028-11102 estimate_ln_dweight (verbatim) --- */
static int
estimate_ln_dweight(const NumericVar *var)
{
	int			ln_dweight;

	/* Caller should fail on ln(negative), but for the moment return zero */
	if (var->sign != NUMERIC_POS)
		return 0;

	if (cmp_var(var, &const_zero_point_nine) >= 0 &&
		cmp_var(var, &const_one_point_one) <= 0)
	{
		/*
		 * 0.9 <= var <= 1.1
		 *
		 * ln(var) has a negative weight (possibly very large).  To get a
		 * reasonably accurate result, estimate it using ln(1+x) ~= x.
		 */
		NumericVar	x;

		init_var(&x);
		sub_var(var, &const_one, &x);

		if (x.ndigits > 0)
		{
			/* Use weight of most significant decimal digit of x */
			ln_dweight = x.weight * DEC_DIGITS + (int) log10(x.digits[0]);
		}
		else
		{
			/* x = 0.  Since ln(1) = 0 exactly, we don't need extra digits */
			ln_dweight = 0;
		}

		free_var(&x);
	}
	else
	{
		/*
		 * Estimate the logarithm using the first couple of digits from the
		 * input number.  This will give an accurate result whenever the input
		 * is not too close to 1.
		 */
		if (var->ndigits > 0)
		{
			int			digits;
			int			dweight;
			double		ln_var;

			digits = var->digits[0];
			dweight = var->weight * DEC_DIGITS;

			if (var->ndigits > 1)
			{
				digits = digits * NBASE + var->digits[1];
				dweight -= DEC_DIGITS;
			}

			/*----------
			 * We have var ~= digits * 10^dweight
			 * so ln(var) ~= ln(digits) + dweight * ln(10)
			 *----------
			 */
			ln_var = log((double) digits) + dweight * 2.302585092994046;
			ln_dweight = (int) log10(fabs(ln_var));
		}
		else
		{
			/* Caller should fail on ln(0), but for the moment return zero */
			ln_dweight = 0;
		}
	}

	return ln_dweight;
}

/* --- src/backend/utils/adt/numeric.c:8147-8217 numericvar_to_int64 (verbatim) --- */
static bool
numericvar_to_int64(const NumericVar *var, int64 *result)
{
	NumericDigit *digits;
	int			ndigits;
	int			weight;
	int			i;
	int64		val;
	bool		neg;
	NumericVar	rounded;

	/* Round to nearest integer */
	init_var(&rounded);
	set_var_from_var(var, &rounded);
	round_var(&rounded, 0);

	/* Check for zero input */
	strip_var(&rounded);
	ndigits = rounded.ndigits;
	if (ndigits == 0)
	{
		*result = 0;
		free_var(&rounded);
		return true;
	}

	/*
	 * For input like 10000000000, we must treat stripped digits as real. So
	 * the loop assumes there are weight+1 digits before the decimal point.
	 */
	weight = rounded.weight;
	Assert(weight >= 0 && ndigits <= weight + 1);

	/*
	 * Construct the result. To avoid issues with converting a value
	 * corresponding to INT64_MIN (which can't be represented as a positive 64
	 * bit two's complement integer), accumulate value as a negative number.
	 */
	digits = rounded.digits;
	neg = (rounded.sign == NUMERIC_NEG);
	val = -digits[0];
	for (i = 1; i <= weight; i++)
	{
		if (unlikely(pg_mul_s64_overflow(val, NBASE, &val)))
		{
			free_var(&rounded);
			return false;
		}

		if (i < ndigits)
		{
			if (unlikely(pg_sub_s64_overflow(val, digits[i], &val)))
			{
				free_var(&rounded);
				return false;
			}
		}
	}

	free_var(&rounded);

	if (!neg)
	{
		if (unlikely(val == PG_INT64_MIN))
			return false;
		val = -val;
	}
	*result = val;

	return true;
}

/* --- src/backend/utils/adt/numeric.c:11450-11647 power_var_int (verbatim) --- */
static void
power_var_int(const NumericVar *base, int exp, int exp_dscale,
			  NumericVar *result)
{
	double		f;
	int			p;
	int			i;
	int			rscale;
	int			sig_digits;
	unsigned int mask;
	bool		neg;
	NumericVar	base_prod;
	int			local_rscale;

	/*
	 * Choose the result scale.  For this we need an estimate of the decimal
	 * weight of the result, which we obtain by approximating using double
	 * precision arithmetic.
	 *
	 * We also perform crude overflow/underflow tests here so that we can exit
	 * early if the result is sure to overflow/underflow, and to guard against
	 * integer overflow when choosing the result scale.
	 */
	if (base->ndigits != 0)
	{
		/*----------
		 * Choose f (double) and p (int) such that base ~= f * 10^p.
		 * Then log10(result) = log10(base^exp) ~= exp * (log10(f) + p).
		 *----------
		 */
		f = base->digits[0];
		p = base->weight * DEC_DIGITS;

		for (i = 1; i < base->ndigits && i * DEC_DIGITS < 16; i++)
		{
			f = f * NBASE + base->digits[i];
			p -= DEC_DIGITS;
		}

		f = exp * (log10(f) + p);	/* approximate decimal result weight */
	}
	else
		f = 0;					/* result is 0 or 1 (weight 0), or error */

	/* overflow/underflow tests with fuzz factors */
	if (f > (NUMERIC_WEIGHT_MAX + 1) * DEC_DIGITS)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("value overflows numeric format")));
	if (f + 1 < -NUMERIC_MAX_DISPLAY_SCALE)
	{
		zero_var(result);
		result->dscale = NUMERIC_MAX_DISPLAY_SCALE;
		return;
	}

	/*
	 * Choose the result scale in the same way as power_var(), so it has at
	 * least NUMERIC_MIN_SIG_DIGITS significant digits and is not less than
	 * either input's display scale.
	 */
	rscale = NUMERIC_MIN_SIG_DIGITS - (int) f;
	rscale = Max(rscale, base->dscale);
	rscale = Max(rscale, exp_dscale);
	rscale = Max(rscale, NUMERIC_MIN_DISPLAY_SCALE);
	rscale = Min(rscale, NUMERIC_MAX_DISPLAY_SCALE);

	/* Handle some common special cases, as well as corner cases */
	switch (exp)
	{
		case 0:

			/*
			 * While 0 ^ 0 can be either 1 or indeterminate (error), we treat
			 * it as 1 because most programming languages do this. SQL:2003
			 * also requires a return value of 1.
			 * https://en.wikipedia.org/wiki/Exponentiation#Zero_to_the_zero_power
			 */
			set_var_from_var(&const_one, result);
			result->dscale = rscale;	/* no need to round */
			return;
		case 1:
			set_var_from_var(base, result);
			round_var(result, rscale);
			return;
		case -1:
			div_var(&const_one, base, result, rscale, true, true);
			return;
		case 2:
			mul_var(base, base, result, rscale);
			return;
		default:
			break;
	}

	/* Handle the special case where the base is zero */
	if (base->ndigits == 0)
	{
		if (exp < 0)
			ereport(ERROR,
					(errcode(ERRCODE_DIVISION_BY_ZERO),
					 errmsg("division by zero")));
		zero_var(result);
		result->dscale = rscale;
		return;
	}

	/*
	 * The general case repeatedly multiplies base according to the bit
	 * pattern of exp.
	 *
	 * The local rscale used for each multiplication is varied to keep a fixed
	 * number of significant digits, sufficient to give the required result
	 * scale.
	 */

	/*
	 * Approximate number of significant digits in the result.  Note that the
	 * underflow test above, together with the choice of rscale, ensures that
	 * this approximation is necessarily > 0.
	 */
	sig_digits = 1 + rscale + (int) f;

	/*
	 * The multiplications to produce the result may introduce an error of up
	 * to around log10(abs(exp)) digits, so work with this many extra digits
	 * of precision (plus a few more for good measure).
	 */
	sig_digits += (int) log(fabs((double) exp)) + 8;

	/*
	 * Now we can proceed with the multiplications.
	 */
	neg = (exp < 0);
	mask = pg_abs_s32(exp);

	init_var(&base_prod);
	set_var_from_var(base, &base_prod);

	if (mask & 1)
		set_var_from_var(base, result);
	else
		set_var_from_var(&const_one, result);

	while ((mask >>= 1) > 0)
	{
		/*
		 * Do the multiplications using rscales large enough to hold the
		 * results to the required number of significant digits, but don't
		 * waste time by exceeding the scales of the numbers themselves.
		 */
		local_rscale = sig_digits - 2 * base_prod.weight * DEC_DIGITS;
		local_rscale = Min(local_rscale, 2 * base_prod.dscale);
		local_rscale = Max(local_rscale, NUMERIC_MIN_DISPLAY_SCALE);

		mul_var(&base_prod, &base_prod, &base_prod, local_rscale);

		if (mask & 1)
		{
			local_rscale = sig_digits -
				(base_prod.weight + result->weight) * DEC_DIGITS;
			local_rscale = Min(local_rscale,
							   base_prod.dscale + result->dscale);
			local_rscale = Max(local_rscale, NUMERIC_MIN_DISPLAY_SCALE);

			mul_var(&base_prod, result, result, local_rscale);
		}

		/*
		 * When abs(base) > 1, the number of digits to the left of the decimal
		 * point in base_prod doubles at each iteration, so if exp is large we
		 * could easily spend large amounts of time and memory space doing the
		 * multiplications.  But once the weight exceeds what will fit in
		 * int16, the final result is guaranteed to overflow (or underflow, if
		 * exp < 0), so we can give up before wasting too many cycles.
		 */
		if (base_prod.weight > NUMERIC_WEIGHT_MAX ||
			result->weight > NUMERIC_WEIGHT_MAX)
		{
			/* overflow, unless neg, in which case result should be 0 */
			if (!neg)
				ereport(ERROR,
						(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
						 errmsg("value overflows numeric format")));
			zero_var(result);
			neg = false;
			break;
		}
	}

	free_var(&base_prod);

	/* Compensate for input sign, and round to requested rscale */
	if (neg)
		div_var(&const_one, result, result, rscale, true, false);
	else
		round_var(result, rscale);
}

/* --- src/backend/utils/adt/numeric.c:10232-10293 div_mod_var (verbatim) --- */
static void
div_mod_var(const NumericVar *var1, const NumericVar *var2,
			NumericVar *quot, NumericVar *rem)
{
	NumericVar	q;
	NumericVar	r;

	init_var(&q);
	init_var(&r);

	/*
	 * Use div_var() with exact = false to get an initial estimate for the
	 * integer quotient (truncated towards zero).  This might be slightly
	 * inaccurate, but we correct it below.
	 */
	div_var(var1, var2, &q, 0, false, false);

	/* Compute initial estimate of remainder using the quotient estimate. */
	mul_var(var2, &q, &r, var2->dscale);
	sub_var(var1, &r, &r);

	/*
	 * Adjust the results if necessary --- the remainder should have the same
	 * sign as var1, and its absolute value should be less than the absolute
	 * value of var2.
	 */
	while (r.ndigits != 0 && r.sign != var1->sign)
	{
		/* The absolute value of the quotient is too large */
		if (var1->sign == var2->sign)
		{
			sub_var(&q, &const_one, &q);
			add_var(&r, var2, &r);
		}
		else
		{
			add_var(&q, &const_one, &q);
			sub_var(&r, var2, &r);
		}
	}

	while (cmp_abs(&r, var2) >= 0)
	{
		/* The absolute value of the quotient is too small */
		if (var1->sign == var2->sign)
		{
			add_var(&q, &const_one, &q);
			sub_var(&r, var2, &r);
		}
		else
		{
			sub_var(&q, &const_one, &q);
			add_var(&r, var2, &r);
		}
	}

	set_var_from_var(&q, quot);
	set_var_from_var(&r, rem);

	free_var(&q);
	free_var(&r);
}

/* --- src/backend/utils/adt/numeric.c:10203-10223 mod_var (verbatim) --- */
static void
mod_var(const NumericVar *var1, const NumericVar *var2, NumericVar *result)
{
	NumericVar	tmp;

	init_var(&tmp);

	/* ---------
	 * We do this using the equation
	 *		mod(x,y) = x - trunc(x/y)*y
	 * div_var can be persuaded to give us trunc(x/y) directly.
	 * ----------
	 */
	div_var(var1, var2, &tmp, 0, false, true);

	mul_var(var2, &tmp, &tmp, var2->dscale);

	sub_var(var1, &tmp, result);

	free_var(&tmp);
}

/* --- src/backend/utils/adt/numeric.c:10326-10341 floor_var (verbatim) --- */
static void
floor_var(const NumericVar *var, NumericVar *result)
{
	NumericVar	tmp;

	init_var(&tmp);
	set_var_from_var(var, &tmp);

	trunc_var(&tmp, 0);

	if (var->sign == NUMERIC_NEG && cmp_var(var, &tmp) != 0)
		sub_var(&tmp, &const_one, &tmp);

	set_var_from_var(&tmp, result);
	free_var(&tmp);
}

/* --- src/backend/utils/adt/numeric.c:10302-10317 ceil_var (verbatim) --- */
static void
ceil_var(const NumericVar *var, NumericVar *result)
{
	NumericVar	tmp;

	init_var(&tmp);
	set_var_from_var(var, &tmp);

	trunc_var(&tmp, 0);

	if (var->sign == NUMERIC_POS && cmp_var(var, &tmp) != 0)
		add_var(&tmp, &const_one, &tmp);

	set_var_from_var(&tmp, result);
	free_var(&tmp);
}

/* --- src/backend/utils/adt/numeric.c:8459-8482 numericvar_to_double_no_overflow (verbatim) --- */
static double
numericvar_to_double_no_overflow(const NumericVar *var)
{
	char	   *tmp;
	double		val;
	char	   *endptr;

	tmp = get_str_from_var(var);

	/* unlike float8in, we ignore ERANGE from strtod */
	val = strtod(tmp, &endptr);
	if (*endptr != '\0')
	{
		/* shouldn't happen ... */
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for type %s: \"%s\"",
						"double precision", tmp)));
	}

	pfree(tmp);

	return val;
}

/* --- src/backend/utils/adt/numeric.c:10134-10195 select_div_scale (verbatim) --- */
static int
select_div_scale(const NumericVar *var1, const NumericVar *var2)
{
	int			weight1,
				weight2,
				qweight,
				i;
	NumericDigit firstdigit1,
				firstdigit2;
	int			rscale;

	/*
	 * The result scale of a division isn't specified in any SQL standard. For
	 * PostgreSQL we select a result scale that will give at least
	 * NUMERIC_MIN_SIG_DIGITS significant digits, so that numeric gives a
	 * result no less accurate than float8; but use a scale not less than
	 * either input's display scale.
	 */

	/* Get the actual (normalized) weight and first digit of each input */

	weight1 = 0;				/* values to use if var1 is zero */
	firstdigit1 = 0;
	for (i = 0; i < var1->ndigits; i++)
	{
		firstdigit1 = var1->digits[i];
		if (firstdigit1 != 0)
		{
			weight1 = var1->weight - i;
			break;
		}
	}

	weight2 = 0;				/* values to use if var2 is zero */
	firstdigit2 = 0;
	for (i = 0; i < var2->ndigits; i++)
	{
		firstdigit2 = var2->digits[i];
		if (firstdigit2 != 0)
		{
			weight2 = var2->weight - i;
			break;
		}
	}

	/*
	 * Estimate weight of quotient.  If the two first digits are equal, we
	 * can't be sure, but assume that var1 is less than var2.
	 */
	qweight = weight1 - weight2;
	if (firstdigit1 <= firstdigit2)
		qweight--;

	/* Select result scale */
	rscale = NUMERIC_MIN_SIG_DIGITS - qweight * DEC_DIGITS;
	rscale = Max(rscale, var1->dscale);
	rscale = Max(rscale, var2->dscale);
	rscale = Max(rscale, NUMERIC_MIN_DISPLAY_SCALE);
	rscale = Min(rscale, NUMERIC_MAX_DISPLAY_SCALE);

	return rscale;
}

/* --- src/backend/utils/adt/numeric.c:915-919 is_valid_numeric_typmod (verbatim) --- */
static inline bool
is_valid_numeric_typmod(int32 typmod)
{
	return typmod >= (int32) VARHDRSZ;
}

/* --- src/backend/utils/adt/numeric.c:926-930 numeric_typmod_precision (verbatim) --- */
static inline int
numeric_typmod_precision(int32 typmod)
{
	return ((typmod - VARHDRSZ) >> 16) & 0xffff;
}

/* --- src/backend/utils/adt/numeric.c:941-945 numeric_typmod_scale (verbatim) --- */
static inline int
numeric_typmod_scale(int32 typmod)
{
	return (((typmod - VARHDRSZ) & 0x7ff) ^ 1024) - 1024;
}

/* --- src/backend/utils/adt/numeric.c:7333-7339 xdigit_value (verbatim) --- */
static inline int
xdigit_value(char dig)
{
	return dig >= '0' && dig <= '9' ? dig - '0' :
		dig >= 'a' && dig <= 'f' ? dig - 'a' + 10 :
		dig >= 'A' && dig <= 'F' ? dig - 'A' + 10 : -1;
}

/* --- src/backend/utils/adt/numeric.c:11655-11675 power_ten_int (verbatim) --- */
static void
power_ten_int(int exp, NumericVar *result)
{
	/* Construct the result directly, starting from 10^0 = 1 */
	set_var_from_var(&const_one, result);

	/* Scale needed to represent the result exactly */
	result->dscale = exp < 0 ? -exp : 0;

	/* Base-NBASE weight of result and remaining exponent */
	if (exp >= 0)
		result->weight = exp / DEC_DIGITS;
	else
		result->weight = (exp + 1) / DEC_DIGITS - 1;

	exp -= result->weight * DEC_DIGITS;

	/* Final adjustment of the result's single NBASE digit */
	while (exp-- > 0)
		result->digits[0] *= 10;
}

/* --- src/backend/utils/adt/numeric.c:9078-9344 mul_var_short (verbatim) --- */
static void
mul_var_short(const NumericVar *var1, const NumericVar *var2,
			  NumericVar *result)
{
	int			var1ndigits = var1->ndigits;
	int			var2ndigits = var2->ndigits;
	NumericDigit *var1digits = var1->digits;
	NumericDigit *var2digits = var2->digits;
	int			res_sign;
	int			res_weight;
	int			res_ndigits;
	NumericDigit *res_buf;
	NumericDigit *res_digits;
	uint32		carry = 0;
	uint32		term;

	/* Check preconditions */
	Assert(var1ndigits >= 1);
	Assert(var1ndigits <= 6);
	Assert(var2ndigits >= var1ndigits);

	/*
	 * Determine the result sign, weight, and number of digits to calculate.
	 * The weight figured here is correct if the product has no leading zero
	 * digits; otherwise strip_var() will fix things up.  Note that, unlike
	 * mul_var(), we do not need to allocate an extra output digit, because we
	 * are not rounding here.
	 */
	if (var1->sign == var2->sign)
		res_sign = NUMERIC_POS;
	else
		res_sign = NUMERIC_NEG;
	res_weight = var1->weight + var2->weight + 1;
	res_ndigits = var1ndigits + var2ndigits;

	/* Allocate result digit array */
	res_buf = digitbuf_alloc(res_ndigits + 1);
	res_buf[0] = 0;				/* spare digit for later rounding */
	res_digits = res_buf + 1;

	/*
	 * Compute the result digits in reverse, in one pass, propagating the
	 * carry up as we go.  The i'th result digit consists of the sum of the
	 * products var1digits[i1] * var2digits[i2] for which i = i1 + i2 + 1.
	 */
#define PRODSUM1(v1,i1,v2,i2) ((v1)[(i1)] * (v2)[(i2)])
#define PRODSUM2(v1,i1,v2,i2) (PRODSUM1(v1,i1,v2,i2) + (v1)[(i1)+1] * (v2)[(i2)-1])
#define PRODSUM3(v1,i1,v2,i2) (PRODSUM2(v1,i1,v2,i2) + (v1)[(i1)+2] * (v2)[(i2)-2])
#define PRODSUM4(v1,i1,v2,i2) (PRODSUM3(v1,i1,v2,i2) + (v1)[(i1)+3] * (v2)[(i2)-3])
#define PRODSUM5(v1,i1,v2,i2) (PRODSUM4(v1,i1,v2,i2) + (v1)[(i1)+4] * (v2)[(i2)-4])
#define PRODSUM6(v1,i1,v2,i2) (PRODSUM5(v1,i1,v2,i2) + (v1)[(i1)+5] * (v2)[(i2)-5])

	switch (var1ndigits)
	{
		case 1:
			/* ---------
			 * 1-digit case:
			 *		var1ndigits = 1
			 *		var2ndigits >= 1
			 *		res_ndigits = var2ndigits + 1
			 * ----------
			 */
			for (int i = var2ndigits - 1; i >= 0; i--)
			{
				term = PRODSUM1(var1digits, 0, var2digits, i) + carry;
				res_digits[i + 1] = (NumericDigit) (term % NBASE);
				carry = term / NBASE;
			}
			res_digits[0] = (NumericDigit) carry;
			break;

		case 2:
			/* ---------
			 * 2-digit case:
			 *		var1ndigits = 2
			 *		var2ndigits >= 2
			 *		res_ndigits = var2ndigits + 2
			 * ----------
			 */
			/* last result digit and carry */
			term = PRODSUM1(var1digits, 1, var2digits, var2ndigits - 1);
			res_digits[res_ndigits - 1] = (NumericDigit) (term % NBASE);
			carry = term / NBASE;

			/* remaining digits, except for the first two */
			for (int i = var2ndigits - 1; i >= 1; i--)
			{
				term = PRODSUM2(var1digits, 0, var2digits, i) + carry;
				res_digits[i + 1] = (NumericDigit) (term % NBASE);
				carry = term / NBASE;
			}
			break;

		case 3:
			/* ---------
			 * 3-digit case:
			 *		var1ndigits = 3
			 *		var2ndigits >= 3
			 *		res_ndigits = var2ndigits + 3
			 * ----------
			 */
			/* last two result digits */
			term = PRODSUM1(var1digits, 2, var2digits, var2ndigits - 1);
			res_digits[res_ndigits - 1] = (NumericDigit) (term % NBASE);
			carry = term / NBASE;

			term = PRODSUM2(var1digits, 1, var2digits, var2ndigits - 1) + carry;
			res_digits[res_ndigits - 2] = (NumericDigit) (term % NBASE);
			carry = term / NBASE;

			/* remaining digits, except for the first three */
			for (int i = var2ndigits - 1; i >= 2; i--)
			{
				term = PRODSUM3(var1digits, 0, var2digits, i) + carry;
				res_digits[i + 1] = (NumericDigit) (term % NBASE);
				carry = term / NBASE;
			}
			break;

		case 4:
			/* ---------
			 * 4-digit case:
			 *		var1ndigits = 4
			 *		var2ndigits >= 4
			 *		res_ndigits = var2ndigits + 4
			 * ----------
			 */
			/* last three result digits */
			term = PRODSUM1(var1digits, 3, var2digits, var2ndigits - 1);
			res_digits[res_ndigits - 1] = (NumericDigit) (term % NBASE);
			carry = term / NBASE;

			term = PRODSUM2(var1digits, 2, var2digits, var2ndigits - 1) + carry;
			res_digits[res_ndigits - 2] = (NumericDigit) (term % NBASE);
			carry = term / NBASE;

			term = PRODSUM3(var1digits, 1, var2digits, var2ndigits - 1) + carry;
			res_digits[res_ndigits - 3] = (NumericDigit) (term % NBASE);
			carry = term / NBASE;

			/* remaining digits, except for the first four */
			for (int i = var2ndigits - 1; i >= 3; i--)
			{
				term = PRODSUM4(var1digits, 0, var2digits, i) + carry;
				res_digits[i + 1] = (NumericDigit) (term % NBASE);
				carry = term / NBASE;
			}
			break;

		case 5:
			/* ---------
			 * 5-digit case:
			 *		var1ndigits = 5
			 *		var2ndigits >= 5
			 *		res_ndigits = var2ndigits + 5
			 * ----------
			 */
			/* last four result digits */
			term = PRODSUM1(var1digits, 4, var2digits, var2ndigits - 1);
			res_digits[res_ndigits - 1] = (NumericDigit) (term % NBASE);
			carry = term / NBASE;

			term = PRODSUM2(var1digits, 3, var2digits, var2ndigits - 1) + carry;
			res_digits[res_ndigits - 2] = (NumericDigit) (term % NBASE);
			carry = term / NBASE;

			term = PRODSUM3(var1digits, 2, var2digits, var2ndigits - 1) + carry;
			res_digits[res_ndigits - 3] = (NumericDigit) (term % NBASE);
			carry = term / NBASE;

			term = PRODSUM4(var1digits, 1, var2digits, var2ndigits - 1) + carry;
			res_digits[res_ndigits - 4] = (NumericDigit) (term % NBASE);
			carry = term / NBASE;

			/* remaining digits, except for the first five */
			for (int i = var2ndigits - 1; i >= 4; i--)
			{
				term = PRODSUM5(var1digits, 0, var2digits, i) + carry;
				res_digits[i + 1] = (NumericDigit) (term % NBASE);
				carry = term / NBASE;
			}
			break;

		case 6:
			/* ---------
			 * 6-digit case:
			 *		var1ndigits = 6
			 *		var2ndigits >= 6
			 *		res_ndigits = var2ndigits + 6
			 * ----------
			 */
			/* last five result digits */
			term = PRODSUM1(var1digits, 5, var2digits, var2ndigits - 1);
			res_digits[res_ndigits - 1] = (NumericDigit) (term % NBASE);
			carry = term / NBASE;

			term = PRODSUM2(var1digits, 4, var2digits, var2ndigits - 1) + carry;
			res_digits[res_ndigits - 2] = (NumericDigit) (term % NBASE);
			carry = term / NBASE;

			term = PRODSUM3(var1digits, 3, var2digits, var2ndigits - 1) + carry;
			res_digits[res_ndigits - 3] = (NumericDigit) (term % NBASE);
			carry = term / NBASE;

			term = PRODSUM4(var1digits, 2, var2digits, var2ndigits - 1) + carry;
			res_digits[res_ndigits - 4] = (NumericDigit) (term % NBASE);
			carry = term / NBASE;

			term = PRODSUM5(var1digits, 1, var2digits, var2ndigits - 1) + carry;
			res_digits[res_ndigits - 5] = (NumericDigit) (term % NBASE);
			carry = term / NBASE;

			/* remaining digits, except for the first six */
			for (int i = var2ndigits - 1; i >= 5; i--)
			{
				term = PRODSUM6(var1digits, 0, var2digits, i) + carry;
				res_digits[i + 1] = (NumericDigit) (term % NBASE);
				carry = term / NBASE;
			}
			break;
	}

	/*
	 * Finally, for var1ndigits > 1, compute the remaining var1ndigits most
	 * significant result digits.
	 */
	switch (var1ndigits)
	{
		case 6:
			term = PRODSUM5(var1digits, 0, var2digits, 4) + carry;
			res_digits[5] = (NumericDigit) (term % NBASE);
			carry = term / NBASE;
			/* FALLTHROUGH */
		case 5:
			term = PRODSUM4(var1digits, 0, var2digits, 3) + carry;
			res_digits[4] = (NumericDigit) (term % NBASE);
			carry = term / NBASE;
			/* FALLTHROUGH */
		case 4:
			term = PRODSUM3(var1digits, 0, var2digits, 2) + carry;
			res_digits[3] = (NumericDigit) (term % NBASE);
			carry = term / NBASE;
			/* FALLTHROUGH */
		case 3:
			term = PRODSUM2(var1digits, 0, var2digits, 1) + carry;
			res_digits[2] = (NumericDigit) (term % NBASE);
			carry = term / NBASE;
			/* FALLTHROUGH */
		case 2:
			term = PRODSUM1(var1digits, 0, var2digits, 0) + carry;
			res_digits[1] = (NumericDigit) (term % NBASE);
			res_digits[0] = (NumericDigit) (term / NBASE);
			break;
	}

	/* Store the product in result */
	digitbuf_free(result->buf);
	result->ndigits = res_ndigits;
	result->buf = res_buf;
	result->digits = res_digits;
	result->weight = res_weight;
	result->sign = res_sign;
	result->dscale = var1->dscale + var2->dscale;

	/* Strip leading and trailing zeroes */
	strip_var(result);
}

/* --- src/backend/utils/adt/numeric.c:8506-8540 cmp_var_common (verbatim) --- */
static int
cmp_var_common(const NumericDigit *var1digits, int var1ndigits,
			   int var1weight, int var1sign,
			   const NumericDigit *var2digits, int var2ndigits,
			   int var2weight, int var2sign)
{
	if (var1ndigits == 0)
	{
		if (var2ndigits == 0)
			return 0;
		if (var2sign == NUMERIC_NEG)
			return 1;
		return -1;
	}
	if (var2ndigits == 0)
	{
		if (var1sign == NUMERIC_POS)
			return 1;
		return -1;
	}

	if (var1sign == NUMERIC_POS)
	{
		if (var2sign == NUMERIC_NEG)
			return 1;
		return cmp_abs_common(var1digits, var1ndigits, var1weight,
							  var2digits, var2ndigits, var2weight);
	}

	if (var2sign == NUMERIC_POS)
		return -1;

	return cmp_abs_common(var2digits, var2ndigits, var2weight,
						  var1digits, var1ndigits, var1weight);
}
