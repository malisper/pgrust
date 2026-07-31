/* VERBATIM segments from src/backend/utils/adt/jsonb.c @ 62d6c7d3df (18.3):
 * jsonb_object + jsonb_object_two_arg (text[] constructors) for the
 * jsonbops_diff target. */

/* jsonb.c file-head JsonbInState (lines 26-40, verbatim; duplicated into
 * this TU because the io TU's copy is TU-local) */
typedef struct JsonbInState
{
	JsonbParseState *parseState;
	JsonbValue *res;
	bool		unique_keys;
	Node	   *escontext;
} JsonbInState;

/* --- jsonb.c jsonb_object (lines 1271-1370) --- */
/*
 * SQL function jsonb_object(text[])
 *
 * take a one or two dimensional array of text as name value pairs
 * for a jsonb object.
 *
 */
Datum
jsonb_object(PG_FUNCTION_ARGS)
{
	ArrayType  *in_array = PG_GETARG_ARRAYTYPE_P(0);
	int			ndims = ARR_NDIM(in_array);
	Datum	   *in_datums;
	bool	   *in_nulls;
	int			in_count,
				count,
				i;
	JsonbInState result;

	memset(&result, 0, sizeof(JsonbInState));

	(void) pushJsonbValue(&result.parseState, WJB_BEGIN_OBJECT, NULL);

	switch (ndims)
	{
		case 0:
			goto close_object;
			break;

		case 1:
			if ((ARR_DIMS(in_array)[0]) % 2)
				ereport(ERROR,
						(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
						 errmsg("array must have even number of elements")));
			break;

		case 2:
			if ((ARR_DIMS(in_array)[1]) != 2)
				ereport(ERROR,
						(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
						 errmsg("array must have two columns")));
			break;

		default:
			ereport(ERROR,
					(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
					 errmsg("wrong number of array subscripts")));
	}

	deconstruct_array_builtin(in_array, TEXTOID, &in_datums, &in_nulls, &in_count);

	count = in_count / 2;

	for (i = 0; i < count; ++i)
	{
		JsonbValue	v;
		char	   *str;
		int			len;

		if (in_nulls[i * 2])
			ereport(ERROR,
					(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
					 errmsg("null value not allowed for object key")));

		str = TextDatumGetCString(in_datums[i * 2]);
		len = strlen(str);

		v.type = jbvString;

		v.val.string.len = len;
		v.val.string.val = str;

		(void) pushJsonbValue(&result.parseState, WJB_KEY, &v);

		if (in_nulls[i * 2 + 1])
		{
			v.type = jbvNull;
		}
		else
		{
			str = TextDatumGetCString(in_datums[i * 2 + 1]);
			len = strlen(str);

			v.type = jbvString;

			v.val.string.len = len;
			v.val.string.val = str;
		}

		(void) pushJsonbValue(&result.parseState, WJB_VALUE, &v);
	}

	pfree(in_datums);
	pfree(in_nulls);

close_object:
	result.res = pushJsonbValue(&result.parseState, WJB_END_OBJECT, NULL);

	PG_RETURN_POINTER(JsonbValueToJsonb(result.res));
}

/* --- jsonb.c jsonb_object_two_arg (lines 1372-1462) --- */
/*
 * SQL function jsonb_object(text[], text[])
 *
 * take separate name and value arrays of text to construct a jsonb object
 * pairwise.
 */
Datum
jsonb_object_two_arg(PG_FUNCTION_ARGS)
{
	ArrayType  *key_array = PG_GETARG_ARRAYTYPE_P(0);
	ArrayType  *val_array = PG_GETARG_ARRAYTYPE_P(1);
	int			nkdims = ARR_NDIM(key_array);
	int			nvdims = ARR_NDIM(val_array);
	Datum	   *key_datums,
			   *val_datums;
	bool	   *key_nulls,
			   *val_nulls;
	int			key_count,
				val_count,
				i;
	JsonbInState result;

	memset(&result, 0, sizeof(JsonbInState));

	(void) pushJsonbValue(&result.parseState, WJB_BEGIN_OBJECT, NULL);

	if (nkdims > 1 || nkdims != nvdims)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("wrong number of array subscripts")));

	if (nkdims == 0)
		goto close_object;

	deconstruct_array_builtin(key_array, TEXTOID, &key_datums, &key_nulls, &key_count);
	deconstruct_array_builtin(val_array, TEXTOID, &val_datums, &val_nulls, &val_count);

	if (key_count != val_count)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("mismatched array dimensions")));

	for (i = 0; i < key_count; ++i)
	{
		JsonbValue	v;
		char	   *str;
		int			len;

		if (key_nulls[i])
			ereport(ERROR,
					(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
					 errmsg("null value not allowed for object key")));

		str = TextDatumGetCString(key_datums[i]);
		len = strlen(str);

		v.type = jbvString;

		v.val.string.len = len;
		v.val.string.val = str;

		(void) pushJsonbValue(&result.parseState, WJB_KEY, &v);

		if (val_nulls[i])
		{
			v.type = jbvNull;
		}
		else
		{
			str = TextDatumGetCString(val_datums[i]);
			len = strlen(str);

			v.type = jbvString;

			v.val.string.len = len;
			v.val.string.val = str;
		}

		(void) pushJsonbValue(&result.parseState, WJB_VALUE, &v);
	}

	pfree(key_datums);
	pfree(key_nulls);
	pfree(val_datums);
	pfree(val_nulls);

close_object:
	result.res = pushJsonbValue(&result.parseState, WJB_END_OBJECT, NULL);

	PG_RETURN_POINTER(JsonbValueToJsonb(result.res));
}
