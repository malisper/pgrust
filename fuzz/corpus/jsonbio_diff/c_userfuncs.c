/*-----------------------------------------------------------------------------
 * array_cat :
 *		concatenate two nD arrays to form an nD array, or
 *		push an (n-1)D array onto the end of an nD array
 *----------------------------------------------------------------------------
 */
Datum
array_cat(PG_FUNCTION_ARGS)
{
	ArrayType  *v1,
			   *v2;
	ArrayType  *result;
	int		   *dims,
			   *lbs,
				ndims,
				nitems,
				ndatabytes,
				nbytes;
	int		   *dims1,
			   *lbs1,
				ndims1,
				nitems1,
				ndatabytes1;
	int		   *dims2,
			   *lbs2,
				ndims2,
				nitems2,
				ndatabytes2;
	int			i;
	char	   *dat1,
			   *dat2;
	bits8	   *bitmap1,
			   *bitmap2;
	Oid			element_type;
	Oid			element_type1;
	Oid			element_type2;
	int32		dataoffset;

	/* Concatenating a null array is a no-op, just return the other input */
	if (PG_ARGISNULL(0))
	{
		if (PG_ARGISNULL(1))
			PG_RETURN_NULL();
		result = PG_GETARG_ARRAYTYPE_P(1);
		PG_RETURN_ARRAYTYPE_P(result);
	}
	if (PG_ARGISNULL(1))
	{
		result = PG_GETARG_ARRAYTYPE_P(0);
		PG_RETURN_ARRAYTYPE_P(result);
	}

	v1 = PG_GETARG_ARRAYTYPE_P(0);
	v2 = PG_GETARG_ARRAYTYPE_P(1);

	element_type1 = ARR_ELEMTYPE(v1);
	element_type2 = ARR_ELEMTYPE(v2);

	/* Check we have matching element types */
	if (element_type1 != element_type2)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("cannot concatenate incompatible arrays"),
				 errdetail("Arrays with element types %s and %s are not "
						   "compatible for concatenation.",
						   format_type_be(element_type1),
						   format_type_be(element_type2))));

	/* OK, use it */
	element_type = element_type1;

	/*----------
	 * We must have one of the following combinations of inputs:
	 * 1) one empty array, and one non-empty array
	 * 2) both arrays empty
	 * 3) two arrays with ndims1 == ndims2
	 * 4) ndims1 == ndims2 - 1
	 * 5) ndims1 == ndims2 + 1
	 *----------
	 */
	ndims1 = ARR_NDIM(v1);
	ndims2 = ARR_NDIM(v2);

	/*
	 * short circuit - if one input array is empty, and the other is not, we
	 * return the non-empty one as the result
	 *
	 * if both are empty, return the first one
	 */
	if (ndims1 == 0 && ndims2 > 0)
		PG_RETURN_ARRAYTYPE_P(v2);

	if (ndims2 == 0)
		PG_RETURN_ARRAYTYPE_P(v1);

	/* the rest fall under rule 3, 4, or 5 */
	if (ndims1 != ndims2 &&
		ndims1 != ndims2 - 1 &&
		ndims1 != ndims2 + 1)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("cannot concatenate incompatible arrays"),
				 errdetail("Arrays of %d and %d dimensions are not "
						   "compatible for concatenation.",
						   ndims1, ndims2)));

	/* get argument array details */
	lbs1 = ARR_LBOUND(v1);
	lbs2 = ARR_LBOUND(v2);
	dims1 = ARR_DIMS(v1);
	dims2 = ARR_DIMS(v2);
	dat1 = ARR_DATA_PTR(v1);
	dat2 = ARR_DATA_PTR(v2);
	bitmap1 = ARR_NULLBITMAP(v1);
	bitmap2 = ARR_NULLBITMAP(v2);
	nitems1 = ArrayGetNItems(ndims1, dims1);
	nitems2 = ArrayGetNItems(ndims2, dims2);
	ndatabytes1 = ARR_SIZE(v1) - ARR_DATA_OFFSET(v1);
	ndatabytes2 = ARR_SIZE(v2) - ARR_DATA_OFFSET(v2);

	if (ndims1 == ndims2)
	{
		/*
		 * resulting array is made up of the elements (possibly arrays
		 * themselves) of the input argument arrays
		 */
		ndims = ndims1;
		dims = (int *) palloc(ndims * sizeof(int));
		lbs = (int *) palloc(ndims * sizeof(int));

		dims[0] = dims1[0] + dims2[0];
		lbs[0] = lbs1[0];

		for (i = 1; i < ndims; i++)
		{
			if (dims1[i] != dims2[i] || lbs1[i] != lbs2[i])
				ereport(ERROR,
						(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
						 errmsg("cannot concatenate incompatible arrays"),
						 errdetail("Arrays with differing element dimensions are "
								   "not compatible for concatenation.")));

			dims[i] = dims1[i];
			lbs[i] = lbs1[i];
		}
	}
	else if (ndims1 == ndims2 - 1)
	{
		/*
		 * resulting array has the second argument as the outer array, with
		 * the first argument inserted at the front of the outer dimension
		 */
		ndims = ndims2;
		dims = (int *) palloc(ndims * sizeof(int));
		lbs = (int *) palloc(ndims * sizeof(int));
		memcpy(dims, dims2, ndims * sizeof(int));
		memcpy(lbs, lbs2, ndims * sizeof(int));

		/* increment number of elements in outer array */
		dims[0] += 1;

		/* make sure the added element matches our existing elements */
		for (i = 0; i < ndims1; i++)
		{
			if (dims1[i] != dims[i + 1] || lbs1[i] != lbs[i + 1])
				ereport(ERROR,
						(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
						 errmsg("cannot concatenate incompatible arrays"),
						 errdetail("Arrays with differing dimensions are not "
								   "compatible for concatenation.")));
		}
	}
	else
	{
		/*
		 * (ndims1 == ndims2 + 1)
		 *
		 * resulting array has the first argument as the outer array, with the
		 * second argument appended to the end of the outer dimension
		 */
		ndims = ndims1;
		dims = (int *) palloc(ndims * sizeof(int));
		lbs = (int *) palloc(ndims * sizeof(int));
		memcpy(dims, dims1, ndims * sizeof(int));
		memcpy(lbs, lbs1, ndims * sizeof(int));

		/* increment number of elements in outer array */
		dims[0] += 1;

		/* make sure the added element matches our existing elements */
		for (i = 0; i < ndims2; i++)
		{
			if (dims2[i] != dims[i + 1] || lbs2[i] != lbs[i + 1])
				ereport(ERROR,
						(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
						 errmsg("cannot concatenate incompatible arrays"),
						 errdetail("Arrays with differing dimensions are not "
								   "compatible for concatenation.")));
		}
	}

	/* Do this mainly for overflow checking */
	nitems = ArrayGetNItems(ndims, dims);
	ArrayCheckBounds(ndims, dims, lbs);

	/* build the result array */
	ndatabytes = ndatabytes1 + ndatabytes2;
	if (ARR_HASNULL(v1) || ARR_HASNULL(v2))
	{
		dataoffset = ARR_OVERHEAD_WITHNULLS(ndims, nitems);
		nbytes = ndatabytes + dataoffset;
	}
	else
	{
		dataoffset = 0;			/* marker for no null bitmap */
		nbytes = ndatabytes + ARR_OVERHEAD_NONULLS(ndims);
	}
	result = (ArrayType *) palloc0(nbytes);
	SET_VARSIZE(result, nbytes);
	result->ndim = ndims;
	result->dataoffset = dataoffset;
	result->elemtype = element_type;
	memcpy(ARR_DIMS(result), dims, ndims * sizeof(int));
	memcpy(ARR_LBOUND(result), lbs, ndims * sizeof(int));
	/* data area is arg1 then arg2 */
	memcpy(ARR_DATA_PTR(result), dat1, ndatabytes1);
	memcpy(ARR_DATA_PTR(result) + ndatabytes1, dat2, ndatabytes2);
	/* handle the null bitmap if needed */
	if (ARR_HASNULL(result))
	{
		array_bitmap_copy(ARR_NULLBITMAP(result), 0,
						  bitmap1, 0,
						  nitems1);
		array_bitmap_copy(ARR_NULLBITMAP(result), nitems1,
						  bitmap2, 0,
						  nitems2);
	}

	PG_RETURN_ARRAYTYPE_P(result);
}

/*
 * ARRAY_AGG(anyarray) aggregate function
 */
Datum
array_agg_array_transfn(PG_FUNCTION_ARGS)
{
	Oid			arg1_typeid = get_fn_expr_argtype(fcinfo->flinfo, 1);
	MemoryContext aggcontext;
	ArrayBuildStateArr *state;

	if (arg1_typeid == InvalidOid)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("could not determine input data type")));

	/*
	 * Note: we do not need a run-time check about whether arg1_typeid is a
	 * valid array type, because the parser would have verified that while
	 * resolving the input/result types of this polymorphic aggregate.
	 */

	if (!AggCheckCallContext(fcinfo, &aggcontext))
	{
		/* cannot be called directly because of internal-type argument */
		elog(ERROR, "array_agg_array_transfn called in non-aggregate context");
	}


	if (PG_ARGISNULL(0))
		state = initArrayResultArr(arg1_typeid, InvalidOid, aggcontext, false);
	else
		state = (ArrayBuildStateArr *) PG_GETARG_POINTER(0);

	state = accumArrayResultArr(state,
								PG_GETARG_DATUM(1),
								PG_ARGISNULL(1),
								arg1_typeid,
								aggcontext);

	/*
	 * The transition type for array_agg() is declared to be "internal", which
	 * is a pass-by-value type the same size as a pointer.  So we can safely
	 * pass the ArrayBuildStateArr pointer through nodeAgg.c's machinations.
	 */
	PG_RETURN_POINTER(state);
}

Datum
array_agg_array_combine(PG_FUNCTION_ARGS)
{
	ArrayBuildStateArr *state1;
	ArrayBuildStateArr *state2;
	MemoryContext agg_context;
	MemoryContext old_context;

	if (!AggCheckCallContext(fcinfo, &agg_context))
		elog(ERROR, "aggregate function called in non-aggregate context");

	state1 = PG_ARGISNULL(0) ? NULL : (ArrayBuildStateArr *) PG_GETARG_POINTER(0);
	state2 = PG_ARGISNULL(1) ? NULL : (ArrayBuildStateArr *) PG_GETARG_POINTER(1);

	if (state2 == NULL)
	{
		/*
		 * NULL state2 is easy, just return state1, which we know is already
		 * in the agg_context
		 */
		if (state1 == NULL)
			PG_RETURN_NULL();
		PG_RETURN_POINTER(state1);
	}

	if (state1 == NULL)
	{
		/* We must copy state2's data into the agg_context */
		old_context = MemoryContextSwitchTo(agg_context);

		state1 = initArrayResultArr(state2->array_type, InvalidOid,
									agg_context, false);

		state1->abytes = state2->abytes;
		state1->data = (char *) palloc(state1->abytes);

		if (state2->nullbitmap)
		{
			int			size = (state2->aitems + 7) / 8;

			state1->nullbitmap = (bits8 *) palloc(size);
			memcpy(state1->nullbitmap, state2->nullbitmap, size);
		}

		memcpy(state1->data, state2->data, state2->nbytes);
		state1->nbytes = state2->nbytes;
		state1->aitems = state2->aitems;
		state1->nitems = state2->nitems;
		state1->ndims = state2->ndims;
		memcpy(state1->dims, state2->dims, sizeof(state2->dims));
		memcpy(state1->lbs, state2->lbs, sizeof(state2->lbs));
		state1->array_type = state2->array_type;
		state1->element_type = state2->element_type;

		MemoryContextSwitchTo(old_context);

		PG_RETURN_POINTER(state1);
	}

	/* We only need to combine the two states if state2 has any items */
	else if (state2->nitems > 0)
	{
		MemoryContext oldContext;
		int			reqsize = state1->nbytes + state2->nbytes;
		int			i;

		/*
		 * Check the states are compatible with each other.  Ensure we use the
		 * same error messages that are listed in accumArrayResultArr so that
		 * the same error is shown as would have been if we'd not used the
		 * combine function for the aggregation.
		 */
		if (state1->ndims != state2->ndims)
			ereport(ERROR,
					(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
					 errmsg("cannot accumulate arrays of different dimensionality")));

		/* Check dimensions match ignoring the first dimension. */
		for (i = 1; i < state1->ndims; i++)
		{
			if (state1->dims[i] != state2->dims[i] || state1->lbs[i] != state2->lbs[i])
				ereport(ERROR,
						(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
						 errmsg("cannot accumulate arrays of different dimensionality")));
		}


		oldContext = MemoryContextSwitchTo(state1->mcontext);

		/*
		 * If there's not enough space in state1 then we'll need to reallocate
		 * more.
		 */
		if (state1->abytes < reqsize)
		{
			/* use a power of 2 size rather than allocating just reqsize */
			state1->abytes = pg_nextpower2_32(reqsize);
			state1->data = (char *) repalloc(state1->data, state1->abytes);
		}

		if (state2->nullbitmap)
		{
			int			newnitems = state1->nitems + state2->nitems;

			if (state1->nullbitmap == NULL)
			{
				/*
				 * First input with nulls; we must retrospectively handle any
				 * previous inputs by marking all their items non-null.
				 */
				state1->aitems = pg_nextpower2_32(Max(256, newnitems + 1));
				state1->nullbitmap = (bits8 *) palloc((state1->aitems + 7) / 8);
				array_bitmap_copy(state1->nullbitmap, 0,
								  NULL, 0,
								  state1->nitems);
			}
			else if (newnitems > state1->aitems)
			{
				int			newaitems = state1->aitems + state2->aitems;

				state1->aitems = pg_nextpower2_32(newaitems);
				state1->nullbitmap = (bits8 *)
					repalloc(state1->nullbitmap, (state1->aitems + 7) / 8);
			}
			array_bitmap_copy(state1->nullbitmap, state1->nitems,
							  state2->nullbitmap, 0,
							  state2->nitems);
		}

		memcpy(state1->data + state1->nbytes, state2->data, state2->nbytes);
		state1->nbytes += state2->nbytes;
		state1->nitems += state2->nitems;

		state1->dims[0] += state2->dims[0];
		/* remaining dims already match, per test above */

		Assert(state1->array_type == state2->array_type);
		Assert(state1->element_type == state2->element_type);

		MemoryContextSwitchTo(oldContext);
	}

	PG_RETURN_POINTER(state1);
}

/*
 * array_agg_array_serialize
 *		Serialize ArrayBuildStateArr into bytea.
 */
Datum
array_agg_array_serialize(PG_FUNCTION_ARGS)
{
	ArrayBuildStateArr *state;
	StringInfoData buf;
	bytea	   *result;

	/* cannot be called directly because of internal-type argument */
	Assert(AggCheckCallContext(fcinfo, NULL));

	state = (ArrayBuildStateArr *) PG_GETARG_POINTER(0);

	pq_begintypsend(&buf);

	/*
	 * element_type. Putting this first is more convenient in deserialization
	 * so that we can init the new state sooner.
	 */
	pq_sendint32(&buf, state->element_type);

	/* array_type */
	pq_sendint32(&buf, state->array_type);

	/* nbytes */
	pq_sendint32(&buf, state->nbytes);

	/* data */
	pq_sendbytes(&buf, state->data, state->nbytes);

	/* abytes */
	pq_sendint32(&buf, state->abytes);

	/* aitems */
	pq_sendint32(&buf, state->aitems);

	/* nullbitmap */
	if (state->nullbitmap)
	{
		Assert(state->aitems > 0);
		pq_sendbytes(&buf, state->nullbitmap, (state->aitems + 7) / 8);
	}

	/* nitems */
	pq_sendint32(&buf, state->nitems);

	/* ndims */
	pq_sendint32(&buf, state->ndims);

	/* dims: XXX should we just send ndims elements? */
	pq_sendbytes(&buf, state->dims, sizeof(state->dims));

	/* lbs */
	pq_sendbytes(&buf, state->lbs, sizeof(state->lbs));

	result = pq_endtypsend(&buf);

	PG_RETURN_BYTEA_P(result);
}

Datum
array_agg_array_deserialize(PG_FUNCTION_ARGS)
{
	bytea	   *sstate;
	ArrayBuildStateArr *result;
	StringInfoData buf;
	Oid			element_type;
	Oid			array_type;
	int			nbytes;
	const char *temp;

	/* cannot be called directly because of internal-type argument */
	Assert(AggCheckCallContext(fcinfo, NULL));

	sstate = PG_GETARG_BYTEA_PP(0);

	/*
	 * Initialize a StringInfo so that we can "receive" it using the standard
	 * recv-function infrastructure.
	 */
	initReadOnlyStringInfo(&buf, VARDATA_ANY(sstate),
						   VARSIZE_ANY_EXHDR(sstate));

	/* element_type */
	element_type = pq_getmsgint(&buf, 4);

	/* array_type */
	array_type = pq_getmsgint(&buf, 4);

	/* nbytes */
	nbytes = pq_getmsgint(&buf, 4);

	result = initArrayResultArr(array_type, element_type,
								CurrentMemoryContext, false);

	result->abytes = 1024;
	while (result->abytes < nbytes)
		result->abytes *= 2;

	result->data = (char *) palloc(result->abytes);

	/* data */
	temp = pq_getmsgbytes(&buf, nbytes);
	memcpy(result->data, temp, nbytes);
	result->nbytes = nbytes;

	/* abytes */
	result->abytes = pq_getmsgint(&buf, 4);

	/* aitems: might be 0 */
	result->aitems = pq_getmsgint(&buf, 4);

	/* nullbitmap */
	if (result->aitems > 0)
	{
		int			size = (result->aitems + 7) / 8;

		result->nullbitmap = (bits8 *) palloc(size);
		temp = pq_getmsgbytes(&buf, size);
		memcpy(result->nullbitmap, temp, size);
	}
	else
		result->nullbitmap = NULL;

	/* nitems */
	result->nitems = pq_getmsgint(&buf, 4);

	/* ndims */
	result->ndims = pq_getmsgint(&buf, 4);

	/* dims */
	temp = pq_getmsgbytes(&buf, sizeof(result->dims));
	memcpy(result->dims, temp, sizeof(result->dims));

	/* lbs */
	temp = pq_getmsgbytes(&buf, sizeof(result->lbs));
	memcpy(result->lbs, temp, sizeof(result->lbs));

	pq_getmsgend(&buf);

	PG_RETURN_POINTER(result);
}

Datum
array_agg_array_finalfn(PG_FUNCTION_ARGS)
{
	Datum		result;
	ArrayBuildStateArr *state;

	/* cannot be called directly because of internal-type argument */
	Assert(AggCheckCallContext(fcinfo, NULL));

	state = PG_ARGISNULL(0) ? NULL : (ArrayBuildStateArr *) PG_GETARG_POINTER(0);

	if (state == NULL)
		PG_RETURN_NULL();		/* returns null iff no input values */

	/*
	 * Make the result.  We cannot release the ArrayBuildStateArr because
	 * sometimes aggregate final functions are re-executed.  Rather, it is
	 * nodeAgg.c's responsibility to reset the aggcontext when it's safe to do
	 * so.
	 */
	result = makeArrayResultArr(state, CurrentMemoryContext, false);

	PG_RETURN_DATUM(result);
}

/*-----------------------------------------------------------------------------
 * array_position, array_position_start :
 *			return the offset of a value in an array.
 *
 * IS NOT DISTINCT FROM semantics are used for comparisons.  Return NULL when
 * the value is not found.
 *-----------------------------------------------------------------------------
 */
Datum
array_position(PG_FUNCTION_ARGS)
{
	return array_position_common(fcinfo);
}

Datum
array_position_start(PG_FUNCTION_ARGS)
{
	return array_position_common(fcinfo);
}

/*
 * array_position_common
 *		Common code for array_position and array_position_start
 *
 * These are separate wrappers for the sake of opr_sanity regression test.
 * They are not strict so we have to test for null inputs explicitly.
 */
static Datum
array_position_common(FunctionCallInfo fcinfo)
{
	ArrayType  *array;
	Oid			collation = PG_GET_COLLATION();
	Oid			element_type;
	Datum		searched_element,
				value;
	bool		isnull;
	int			position,
				position_min;
	bool		found = false;
	TypeCacheEntry *typentry;
	ArrayMetaState *my_extra;
	bool		null_search;
	ArrayIterator array_iterator;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	array = PG_GETARG_ARRAYTYPE_P(0);

	/*
	 * We refuse to search for elements in multi-dimensional arrays, since we
	 * have no good way to report the element's location in the array.
	 */
	if (ARR_NDIM(array) > 1)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("searching for elements in multidimensional arrays is not supported")));

	/* Searching in an empty array is well-defined, though: it always fails */
	if (ARR_NDIM(array) < 1)
		PG_RETURN_NULL();

	if (PG_ARGISNULL(1))
	{
		/* fast return when the array doesn't have nulls */
		if (!array_contains_nulls(array))
			PG_RETURN_NULL();
		searched_element = (Datum) 0;
		null_search = true;
	}
	else
	{
		searched_element = PG_GETARG_DATUM(1);
		null_search = false;
	}

	element_type = ARR_ELEMTYPE(array);
	position = (ARR_LBOUND(array))[0] - 1;

	/* figure out where to start */
	if (PG_NARGS() == 3)
	{
		if (PG_ARGISNULL(2))
			ereport(ERROR,
					(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
					 errmsg("initial position must not be null")));

		position_min = PG_GETARG_INT32(2);
	}
	else
		position_min = (ARR_LBOUND(array))[0];

	/*
	 * We arrange to look up type info for array_create_iterator only once per
	 * series of calls, assuming the element type doesn't change underneath
	 * us.
	 */
	my_extra = (ArrayMetaState *) fcinfo->flinfo->fn_extra;
	if (my_extra == NULL)
	{
		fcinfo->flinfo->fn_extra = MemoryContextAlloc(fcinfo->flinfo->fn_mcxt,
													  sizeof(ArrayMetaState));
		my_extra = (ArrayMetaState *) fcinfo->flinfo->fn_extra;
		my_extra->element_type = ~element_type;
	}

	if (my_extra->element_type != element_type)
	{
		get_typlenbyvalalign(element_type,
							 &my_extra->typlen,
							 &my_extra->typbyval,
							 &my_extra->typalign);

		typentry = lookup_type_cache(element_type, TYPECACHE_EQ_OPR_FINFO);

		if (!OidIsValid(typentry->eq_opr_finfo.fn_oid))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_FUNCTION),
					 errmsg("could not identify an equality operator for type %s",
							format_type_be(element_type))));

		my_extra->element_type = element_type;
		fmgr_info_cxt(typentry->eq_opr_finfo.fn_oid, &my_extra->proc,
					  fcinfo->flinfo->fn_mcxt);
	}

	/* Examine each array element until we find a match. */
	array_iterator = array_create_iterator(array, 0, my_extra);
	while (array_iterate(array_iterator, &value, &isnull))
	{
		position++;

		/* skip initial elements if caller requested so */
		if (position < position_min)
			continue;

		/*
		 * Can't look at the array element's value if it's null; but if we
		 * search for null, we have a hit and are done.
		 */
		if (isnull || null_search)
		{
			if (isnull && null_search)
			{
				found = true;
				break;
			}
			else
				continue;
		}

		/* not nulls, so run the operator */
		if (DatumGetBool(FunctionCall2Coll(&my_extra->proc, collation,
										   searched_element, value)))
		{
			found = true;
			break;
		}
	}

	array_free_iterator(array_iterator);

	/* Avoid leaking memory when handed toasted input */
	PG_FREE_IF_COPY(array, 0);

	if (!found)
		PG_RETURN_NULL();

	PG_RETURN_INT32(position);
}

/*-----------------------------------------------------------------------------
 * array_positions :
 *			return an array of positions of a value in an array.
 *
 * IS NOT DISTINCT FROM semantics are used for comparisons.  Returns NULL when
 * the input array is NULL.  When the value is not found in the array, returns
 * an empty array.
 *
 * This is not strict so we have to test for null inputs explicitly.
 *-----------------------------------------------------------------------------
 */
Datum
array_positions(PG_FUNCTION_ARGS)
{
	ArrayType  *array;
	Oid			collation = PG_GET_COLLATION();
	Oid			element_type;
	Datum		searched_element,
				value;
	bool		isnull;
	int			position;
	TypeCacheEntry *typentry;
	ArrayMetaState *my_extra;
	bool		null_search;
	ArrayIterator array_iterator;
	ArrayBuildState *astate = NULL;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	array = PG_GETARG_ARRAYTYPE_P(0);

	/*
	 * We refuse to search for elements in multi-dimensional arrays, since we
	 * have no good way to report the element's location in the array.
	 */
	if (ARR_NDIM(array) > 1)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("searching for elements in multidimensional arrays is not supported")));

	astate = initArrayResult(INT4OID, CurrentMemoryContext, false);

	/* Searching in an empty array is well-defined, though: it always fails */
	if (ARR_NDIM(array) < 1)
		PG_RETURN_DATUM(makeArrayResult(astate, CurrentMemoryContext));

	if (PG_ARGISNULL(1))
	{
		/* fast return when the array doesn't have nulls */
		if (!array_contains_nulls(array))
			PG_RETURN_DATUM(makeArrayResult(astate, CurrentMemoryContext));
		searched_element = (Datum) 0;
		null_search = true;
	}
	else
	{
		searched_element = PG_GETARG_DATUM(1);
		null_search = false;
	}

	element_type = ARR_ELEMTYPE(array);
	position = (ARR_LBOUND(array))[0] - 1;

	/*
	 * We arrange to look up type info for array_create_iterator only once per
	 * series of calls, assuming the element type doesn't change underneath
	 * us.
	 */
	my_extra = (ArrayMetaState *) fcinfo->flinfo->fn_extra;
	if (my_extra == NULL)
	{
		fcinfo->flinfo->fn_extra = MemoryContextAlloc(fcinfo->flinfo->fn_mcxt,
													  sizeof(ArrayMetaState));
		my_extra = (ArrayMetaState *) fcinfo->flinfo->fn_extra;
		my_extra->element_type = ~element_type;
	}

	if (my_extra->element_type != element_type)
	{
		get_typlenbyvalalign(element_type,
							 &my_extra->typlen,
							 &my_extra->typbyval,
							 &my_extra->typalign);

		typentry = lookup_type_cache(element_type, TYPECACHE_EQ_OPR_FINFO);

		if (!OidIsValid(typentry->eq_opr_finfo.fn_oid))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_FUNCTION),
					 errmsg("could not identify an equality operator for type %s",
							format_type_be(element_type))));

		my_extra->element_type = element_type;
		fmgr_info_cxt(typentry->eq_opr_finfo.fn_oid, &my_extra->proc,
					  fcinfo->flinfo->fn_mcxt);
	}

	/*
	 * Accumulate each array position iff the element matches the given
	 * element.
	 */
	array_iterator = array_create_iterator(array, 0, my_extra);
	while (array_iterate(array_iterator, &value, &isnull))
	{
		position += 1;

		/*
		 * Can't look at the array element's value if it's null; but if we
		 * search for null, we have a hit.
		 */
		if (isnull || null_search)
		{
			if (isnull && null_search)
				astate =
					accumArrayResult(astate, Int32GetDatum(position), false,
									 INT4OID, CurrentMemoryContext);

			continue;
		}

		/* not nulls, so run the operator */
		if (DatumGetBool(FunctionCall2Coll(&my_extra->proc, collation,
										   searched_element, value)))
			astate =
				accumArrayResult(astate, Int32GetDatum(position), false,
								 INT4OID, CurrentMemoryContext);
	}

	array_free_iterator(array_iterator);

	/* Avoid leaking memory when handed toasted input */
	PG_FREE_IF_COPY(array, 0);

	PG_RETURN_DATUM(makeArrayResult(astate, CurrentMemoryContext));
}

/*
 * array_shuffle_n
 *		Return a copy of array with n randomly chosen items.
 *
 * The number of items must not exceed the size of the first dimension of the
 * array.  We preserve the first dimension's lower bound if keep_lb,
 * else it's set to 1.  Lower-order dimensions are preserved in any case.
 *
 * NOTE: it would be cleaner to look up the elmlen/elmbval/elmalign info
 * from the system catalogs, given only the elmtyp. However, the caller is
 * in a better position to cache this info across multiple calls.
 */
static ArrayType *
array_shuffle_n(ArrayType *array, int n, bool keep_lb,
				Oid elmtyp, TypeCacheEntry *typentry)
{
	ArrayType  *result;
	int			ndim,
			   *dims,
			   *lbs,
				nelm,
				nitem,
				rdims[MAXDIM],
				rlbs[MAXDIM];
	int16		elmlen;
	bool		elmbyval;
	char		elmalign;
	Datum	   *elms,
			   *ielms;
	bool	   *nuls,
			   *inuls;

	ndim = ARR_NDIM(array);
	dims = ARR_DIMS(array);
	lbs = ARR_LBOUND(array);

	elmlen = typentry->typlen;
	elmbyval = typentry->typbyval;
	elmalign = typentry->typalign;

	/* If the target array is empty, exit fast */
	if (ndim < 1 || dims[0] < 1 || n < 1)
		return construct_empty_array(elmtyp);

	deconstruct_array(array, elmtyp, elmlen, elmbyval, elmalign,
					  &elms, &nuls, &nelm);

	nitem = dims[0];			/* total number of items */
	nelm /= nitem;				/* number of elements per item */

	Assert(n <= nitem);			/* else it's caller error */

	/*
	 * Shuffle array using Fisher-Yates algorithm.  Scan the array and swap
	 * current item (nelm datums starting at ielms) with a randomly chosen
	 * later item (nelm datums starting at jelms) in each iteration.  We can
	 * stop once we've done n iterations; then first n items are the result.
	 */
	ielms = elms;
	inuls = nuls;
	for (int i = 0; i < n; i++)
	{
		int			j = (int) pg_prng_uint64_range(&pg_global_prng_state, i, nitem - 1) * nelm;
		Datum	   *jelms = elms + j;
		bool	   *jnuls = nuls + j;

		/* Swap i'th and j'th items; advance ielms/inuls to next item */
		for (int k = 0; k < nelm; k++)
		{
			Datum		elm = *ielms;
			bool		nul = *inuls;

			*ielms++ = *jelms;
			*inuls++ = *jnuls;
			*jelms++ = elm;
			*jnuls++ = nul;
		}
	}

	/* Set up dimensions of the result */
	memcpy(rdims, dims, ndim * sizeof(int));
	memcpy(rlbs, lbs, ndim * sizeof(int));
	rdims[0] = n;
	if (!keep_lb)
		rlbs[0] = 1;

	result = construct_md_array(elms, nuls, ndim, rdims, rlbs,
								elmtyp, elmlen, elmbyval, elmalign);

	pfree(elms);
	pfree(nuls);

	return result;
}

/*
 * array_shuffle
 *
 * Returns an array with the same dimensions as the input array, with its
 * first-dimension elements in random order.
 */
Datum
array_shuffle(PG_FUNCTION_ARGS)
{
	ArrayType  *array = PG_GETARG_ARRAYTYPE_P(0);
	ArrayType  *result;
	Oid			elmtyp;
	TypeCacheEntry *typentry;

	/*
	 * There is no point in shuffling empty arrays or arrays with less than
	 * two items.
	 */
	if (ARR_NDIM(array) < 1 || ARR_DIMS(array)[0] < 2)
		PG_RETURN_ARRAYTYPE_P(array);

	elmtyp = ARR_ELEMTYPE(array);
	typentry = (TypeCacheEntry *) fcinfo->flinfo->fn_extra;
	if (typentry == NULL || typentry->type_id != elmtyp)
	{
		typentry = lookup_type_cache(elmtyp, 0);
		fcinfo->flinfo->fn_extra = typentry;
	}

	result = array_shuffle_n(array, ARR_DIMS(array)[0], true, elmtyp, typentry);

	PG_RETURN_ARRAYTYPE_P(result);
}

/*
 * array_sample
 *
 * Returns an array of n randomly chosen first-dimension elements
 * from the input array.
 */
Datum
array_sample(PG_FUNCTION_ARGS)
{
	ArrayType  *array = PG_GETARG_ARRAYTYPE_P(0);
	int			n = PG_GETARG_INT32(1);
	ArrayType  *result;
	Oid			elmtyp;
	TypeCacheEntry *typentry;
	int			nitem;

	nitem = (ARR_NDIM(array) < 1) ? 0 : ARR_DIMS(array)[0];

	if (n < 0 || n > nitem)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("sample size must be between 0 and %d", nitem)));

	elmtyp = ARR_ELEMTYPE(array);
	typentry = (TypeCacheEntry *) fcinfo->flinfo->fn_extra;
	if (typentry == NULL || typentry->type_id != elmtyp)
	{
		typentry = lookup_type_cache(elmtyp, 0);
		fcinfo->flinfo->fn_extra = typentry;
	}

	result = array_shuffle_n(array, n, false, elmtyp, typentry);

	PG_RETURN_ARRAYTYPE_P(result);
}

/*
 * array_reverse_n
 *		Return a copy of array with reversed items.
 *
 * NOTE: it would be cleaner to look up the elmlen/elmbval/elmalign info
 * from the system catalogs, given only the elmtyp. However, the caller is
 * in a better position to cache this info across multiple calls.
 */
static ArrayType *
array_reverse_n(ArrayType *array, Oid elmtyp, TypeCacheEntry *typentry)
{
	ArrayType  *result;
	int			ndim,
			   *dims,
			   *lbs,
				nelm,
				nitem,
				rdims[MAXDIM],
				rlbs[MAXDIM];
	int16		elmlen;
	bool		elmbyval;
	char		elmalign;
	Datum	   *elms,
			   *ielms;
	bool	   *nuls,
			   *inuls;

	ndim = ARR_NDIM(array);
	dims = ARR_DIMS(array);
	lbs = ARR_LBOUND(array);

	elmlen = typentry->typlen;
	elmbyval = typentry->typbyval;
	elmalign = typentry->typalign;

	deconstruct_array(array, elmtyp, elmlen, elmbyval, elmalign,
					  &elms, &nuls, &nelm);

	nitem = dims[0];			/* total number of items */
	nelm /= nitem;				/* number of elements per item */

	/* Reverse the array */
	ielms = elms;
	inuls = nuls;
	for (int i = 0; i < nitem / 2; i++)
	{
		int			j = (nitem - i - 1) * nelm;
		Datum	   *jelms = elms + j;
		bool	   *jnuls = nuls + j;

		/* Swap i'th and j'th items; advance ielms/inuls to next item */
		for (int k = 0; k < nelm; k++)
		{
			Datum		elm = *ielms;
			bool		nul = *inuls;

			*ielms++ = *jelms;
			*inuls++ = *jnuls;
			*jelms++ = elm;
			*jnuls++ = nul;
		}
	}

	/* Set up dimensions of the result */
	memcpy(rdims, dims, ndim * sizeof(int));
	memcpy(rlbs, lbs, ndim * sizeof(int));
	rdims[0] = nitem;

	result = construct_md_array(elms, nuls, ndim, rdims, rlbs,
								elmtyp, elmlen, elmbyval, elmalign);

	pfree(elms);
	pfree(nuls);

	return result;
}

/*
 * array_reverse
 *
 * Returns an array with the same dimensions as the input array, with its
 * first-dimension elements in reverse order.
 */
Datum
array_reverse(PG_FUNCTION_ARGS)
{
	ArrayType  *array = PG_GETARG_ARRAYTYPE_P(0);
	ArrayType  *result;
	Oid			elmtyp;
	TypeCacheEntry *typentry;

	/*
	 * There is no point in reversing empty arrays or arrays with less than
	 * two items.
	 */
	if (ARR_NDIM(array) < 1 || ARR_DIMS(array)[0] < 2)
		PG_RETURN_ARRAYTYPE_P(array);

	elmtyp = ARR_ELEMTYPE(array);
	typentry = (TypeCacheEntry *) fcinfo->flinfo->fn_extra;
	if (typentry == NULL || typentry->type_id != elmtyp)
	{
		typentry = lookup_type_cache(elmtyp, 0);
		fcinfo->flinfo->fn_extra = (void *) typentry;
	}

	result = array_reverse_n(array, elmtyp, typentry);

	PG_RETURN_ARRAYTYPE_P(result);
}
