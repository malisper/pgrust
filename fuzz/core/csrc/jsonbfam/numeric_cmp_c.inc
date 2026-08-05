/* VERBATIM segments from src/backend/utils/adt/numeric.c @ 62d6c7d3df
 * (18.3): comparison + hashing surface reachable from jsonb ops
 * (compareJsonbContainers -> numeric_cmp; equalsJsonbScalarValue ->
 * numeric_eq; JsonbHashScalarValue[Extended] -> hash_numeric[_extended]).
 * Static decls for cmp_* already ride numeric_c.inc (file-head decls);
 * bodies here. Included by pg_jsonbio_io.c AFTER numeric_c.inc (same TU:
 * the cmp_* helpers are static).
 */

/* --- numeric.c numeric_cmp (lines 2517-2530) --- */
Datum
numeric_cmp(PG_FUNCTION_ARGS)
{
	Numeric		num1 = PG_GETARG_NUMERIC(0);
	Numeric		num2 = PG_GETARG_NUMERIC(1);
	int			result;

	result = cmp_numerics(num1, num2);

	PG_FREE_IF_COPY(num1, 0);
	PG_FREE_IF_COPY(num2, 1);

	PG_RETURN_INT32(result);
}

/* --- numeric.c numeric_eq (lines 2533-2546) --- */
Datum
numeric_eq(PG_FUNCTION_ARGS)
{
	Numeric		num1 = PG_GETARG_NUMERIC(0);
	Numeric		num2 = PG_GETARG_NUMERIC(1);
	bool		result;

	result = cmp_numerics(num1, num2) == 0;

	PG_FREE_IF_COPY(num1, 0);
	PG_FREE_IF_COPY(num2, 1);

	PG_RETURN_BOOL(result);
}

/* --- numeric.c cmp_numerics (lines 2623-2675) --- */
static int
cmp_numerics(Numeric num1, Numeric num2)
{
	int			result;

	/*
	 * We consider all NANs to be equal and larger than any non-NAN (including
	 * Infinity).  This is somewhat arbitrary; the important thing is to have
	 * a consistent sort order.
	 */
	if (NUMERIC_IS_SPECIAL(num1))
	{
		if (NUMERIC_IS_NAN(num1))
		{
			if (NUMERIC_IS_NAN(num2))
				result = 0;		/* NAN = NAN */
			else
				result = 1;		/* NAN > non-NAN */
		}
		else if (NUMERIC_IS_PINF(num1))
		{
			if (NUMERIC_IS_NAN(num2))
				result = -1;	/* PINF < NAN */
			else if (NUMERIC_IS_PINF(num2))
				result = 0;		/* PINF = PINF */
			else
				result = 1;		/* PINF > anything else */
		}
		else					/* num1 must be NINF */
		{
			if (NUMERIC_IS_NINF(num2))
				result = 0;		/* NINF = NINF */
			else
				result = -1;	/* NINF < anything else */
		}
	}
	else if (NUMERIC_IS_SPECIAL(num2))
	{
		if (NUMERIC_IS_NINF(num2))
			result = 1;			/* normal > NINF */
		else
			result = -1;		/* normal < NAN or PINF */
	}
	else
	{
		result = cmp_var_common(NUMERIC_DIGITS(num1), NUMERIC_NDIGITS(num1),
								NUMERIC_WEIGHT(num1), NUMERIC_SIGN(num1),
								NUMERIC_DIGITS(num2), NUMERIC_NDIGITS(num2),
								NUMERIC_WEIGHT(num2), NUMERIC_SIGN(num2));
	}

	return result;
}

/* --- numeric.c hash_numeric (lines 2815-2889) --- */
Datum
hash_numeric(PG_FUNCTION_ARGS)
{
	Numeric		key = PG_GETARG_NUMERIC(0);
	Datum		digit_hash;
	Datum		result;
	int			weight;
	int			start_offset;
	int			end_offset;
	int			i;
	int			hash_len;
	NumericDigit *digits;

	/* If it's NaN or infinity, don't try to hash the rest of the fields */
	if (NUMERIC_IS_SPECIAL(key))
		PG_RETURN_UINT32(0);

	weight = NUMERIC_WEIGHT(key);
	start_offset = 0;
	end_offset = 0;

	/*
	 * Omit any leading or trailing zeros from the input to the hash. The
	 * numeric implementation *should* guarantee that leading and trailing
	 * zeros are suppressed, but we're paranoid. Note that we measure the
	 * starting and ending offsets in units of NumericDigits, not bytes.
	 */
	digits = NUMERIC_DIGITS(key);
	for (i = 0; i < NUMERIC_NDIGITS(key); i++)
	{
		if (digits[i] != (NumericDigit) 0)
			break;

		start_offset++;

		/*
		 * The weight is effectively the # of digits before the decimal point,
		 * so decrement it for each leading zero we skip.
		 */
		weight--;
	}

	/*
	 * If there are no non-zero digits, then the value of the number is zero,
	 * regardless of any other fields.
	 */
	if (NUMERIC_NDIGITS(key) == start_offset)
		PG_RETURN_UINT32(-1);

	for (i = NUMERIC_NDIGITS(key) - 1; i >= 0; i--)
	{
		if (digits[i] != (NumericDigit) 0)
			break;

		end_offset++;
	}

	/* If we get here, there should be at least one non-zero digit */
	Assert(start_offset + end_offset < NUMERIC_NDIGITS(key));

	/*
	 * Note that we don't hash on the Numeric's scale, since two numerics can
	 * compare equal but have different scales. We also don't hash on the
	 * sign, although we could: since a sign difference implies inequality,
	 * this shouldn't affect correctness.
	 */
	hash_len = NUMERIC_NDIGITS(key) - start_offset - end_offset;
	digit_hash = hash_any((unsigned char *) (NUMERIC_DIGITS(key) + start_offset),
						  hash_len * sizeof(NumericDigit));

	/* Mix in the weight, via XOR */
	result = digit_hash ^ weight;

	PG_RETURN_DATUM(result);
}

/* --- numeric.c hash_numeric_extended (lines 2891-2950) --- */
/*
 * Returns 64-bit value by hashing a value to a 64-bit value, with a seed.
 * Otherwise, similar to hash_numeric.
 */
Datum
hash_numeric_extended(PG_FUNCTION_ARGS)
{
	Numeric		key = PG_GETARG_NUMERIC(0);
	uint64		seed = PG_GETARG_INT64(1);
	Datum		digit_hash;
	Datum		result;
	int			weight;
	int			start_offset;
	int			end_offset;
	int			i;
	int			hash_len;
	NumericDigit *digits;

	/* If it's NaN or infinity, don't try to hash the rest of the fields */
	if (NUMERIC_IS_SPECIAL(key))
		PG_RETURN_UINT64(seed);

	weight = NUMERIC_WEIGHT(key);
	start_offset = 0;
	end_offset = 0;

	digits = NUMERIC_DIGITS(key);
	for (i = 0; i < NUMERIC_NDIGITS(key); i++)
	{
		if (digits[i] != (NumericDigit) 0)
			break;

		start_offset++;

		weight--;
	}

	if (NUMERIC_NDIGITS(key) == start_offset)
		PG_RETURN_UINT64(seed - 1);

	for (i = NUMERIC_NDIGITS(key) - 1; i >= 0; i--)
	{
		if (digits[i] != (NumericDigit) 0)
			break;

		end_offset++;
	}

	Assert(start_offset + end_offset < NUMERIC_NDIGITS(key));

	hash_len = NUMERIC_NDIGITS(key) - start_offset - end_offset;
	digit_hash = hash_any_extended((unsigned char *) (NUMERIC_DIGITS(key)
													  + start_offset),
								   hash_len * sizeof(NumericDigit),
								   seed);

	result = UInt64GetDatum(DatumGetUInt64(digit_hash) ^ weight);

	PG_RETURN_DATUM(result);
}

/* --- numeric.c cmp_var (lines 8485-8498) --- */
/*
 * cmp_var() -
 *
 *	Compare two values on variable level.  We assume zeroes have been
 *	truncated to no digits.
 */
static int
cmp_var(const NumericVar *var1, const NumericVar *var2)
{
	return cmp_var_common(var1->digits, var1->ndigits,
						  var1->weight, var1->sign,
						  var2->digits, var2->ndigits,
						  var2->weight, var2->sign);
}

/* --- numeric.c cmp_var_common (lines 8500-8540) --- */
/*
 * cmp_var_common() -
 *
 *	Main routine of cmp_var(). This function can be used by both
 *	NumericVar and Numeric.
 */
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

/* --- numeric.c cmp_abs (lines 11854-11868) --- */
/* ----------
 * cmp_abs() -
 *
 *	Compare the absolute values of var1 and var2
 *	Returns:	-1 for ABS(var1) < ABS(var2)
 *				0  for ABS(var1) == ABS(var2)
 *				1  for ABS(var1) > ABS(var2)
 * ----------
 */
static int
cmp_abs(const NumericVar *var1, const NumericVar *var2)
{
	return cmp_abs_common(var1->digits, var1->ndigits, var1->weight,
						  var2->digits, var2->ndigits, var2->weight);
}

/* --- numeric.c cmp_abs_common (lines 11870-11932) --- */
/* ----------
 * cmp_abs_common() -
 *
 *	Main routine of cmp_abs(). This function can be used by both
 *	NumericVar and Numeric.
 * ----------
 */
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
