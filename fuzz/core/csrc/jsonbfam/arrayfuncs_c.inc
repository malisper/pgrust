/* VERBATIM segments from src/backend/utils/adt/arrayfuncs.c +
 * src/backend/utils/adt/arrayutils.c @ 62d6c7d3df (18.3): the array
 * deconstruction path the vendored jsonb ops call (deconstruct_array,
 * deconstruct_array_builtin, array_contains_nulls, ArrayGetNItems[Safe]).
 */

/* --- arrayutils.c ArrayGetNItems (lines 46-60) --- */
/*
 * Convert array dimensions into number of elements
 *
 * This must do overflow checking, since it is used to validate that a user
 * dimensionality request doesn't overflow what we can handle.
 *
 * The multiplication overflow check only works on machines that have int64
 * arithmetic, but that is nearly all platforms these days, and doing check
 * divides for those that don't seems way too expensive.
 */
int
ArrayGetNItems(int ndim, const int *dims)
{
	return ArrayGetNItemsSafe(ndim, dims, NULL);
}

/* --- arrayutils.c ArrayGetNItemsSafe (lines 62-102) --- */
/*
 * This entry point can return the error into an ErrorSaveContext
 * instead of throwing an exception.  -1 is returned after an error.
 */
int
ArrayGetNItemsSafe(int ndim, const int *dims, struct Node *escontext)
{
	int32		ret;
	int			i;

	if (ndim <= 0)
		return 0;
	ret = 1;
	for (i = 0; i < ndim; i++)
	{
		int64		prod;

		/* A negative dimension implies that UB-LB overflowed ... */
		if (dims[i] < 0)
			ereturn(escontext, -1,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("array size exceeds the maximum allowed (%d)",
							(int) MaxArraySize)));

		prod = (int64) ret * (int64) dims[i];

		ret = (int32) prod;
		if ((int64) ret != prod)
			ereturn(escontext, -1,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("array size exceeds the maximum allowed (%d)",
							(int) MaxArraySize)));
	}
	Assert(ret >= 0);
	if ((Size) ret > MaxArraySize)
		ereturn(escontext, -1,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("array size exceeds the maximum allowed (%d)",
						(int) MaxArraySize)));
	return (int) ret;
}
/* --- arrayfuncs.c deconstruct_array (lines 3609-3689) --- */
/*
 * deconstruct_array  --- simple method for extracting data from an array
 *
 * array: array object to examine (must not be NULL)
 * elmtype, elmlen, elmbyval, elmalign: info for the datatype of the items
 * elemsp: return value, set to point to palloc'd array of Datum values
 * nullsp: return value, set to point to palloc'd array of isnull markers
 * nelemsp: return value, set to number of extracted values
 *
 * The caller may pass nullsp == NULL if it does not support NULLs in the
 * array.  Note that this produces a very uninformative error message,
 * so do it only in cases where a NULL is really not expected.
 *
 * If array elements are pass-by-ref data type, the returned Datums will
 * be pointers into the array object.
 *
 * NOTE: it would be cleaner to look up the elmlen/elmbval/elmalign info
 * from the system catalogs, given the elmtype.  However, the caller is
 * in a better position to cache this info across multiple uses, or even
 * to hard-wire values if the element type is hard-wired.
 */
void
deconstruct_array(ArrayType *array,
				  Oid elmtype,
				  int elmlen, bool elmbyval, char elmalign,
				  Datum **elemsp, bool **nullsp, int *nelemsp)
{
	Datum	   *elems;
	bool	   *nulls;
	int			nelems;
	char	   *p;
	bits8	   *bitmap;
	int			bitmask;
	int			i;

	Assert(ARR_ELEMTYPE(array) == elmtype);

	nelems = ArrayGetNItems(ARR_NDIM(array), ARR_DIMS(array));
	*elemsp = elems = (Datum *) palloc(nelems * sizeof(Datum));
	if (nullsp)
		*nullsp = nulls = (bool *) palloc0(nelems * sizeof(bool));
	else
		nulls = NULL;
	*nelemsp = nelems;

	p = ARR_DATA_PTR(array);
	bitmap = ARR_NULLBITMAP(array);
	bitmask = 1;

	for (i = 0; i < nelems; i++)
	{
		/* Get source element, checking for NULL */
		if (bitmap && (*bitmap & bitmask) == 0)
		{
			elems[i] = (Datum) 0;
			if (nulls)
				nulls[i] = true;
			else
				ereport(ERROR,
						(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
						 errmsg("null array element not allowed in this context")));
		}
		else
		{
			elems[i] = fetch_att(p, elmbyval, elmlen);
			p = att_addlength_pointer(p, elmlen, p);
			p = (char *) att_align_nominal(p, elmalign);
		}

		/* advance bitmap pointer if any */
		if (bitmap)
		{
			bitmask <<= 1;
			if (bitmask == 0x100)
			{
				bitmap++;
				bitmask = 1;
			}
		}
	}
}

/* --- arrayfuncs.c deconstruct_array_builtin (lines 3691-3764) --- */
/*
 * Like deconstruct_array(), where elmtype must be a built-in type, and
 * elmlen/elmbyval/elmalign is looked up from hardcoded data.  This is often
 * useful when manipulating arrays from/for system catalogs.
 */
void
deconstruct_array_builtin(ArrayType *array,
						  Oid elmtype,
						  Datum **elemsp, bool **nullsp, int *nelemsp)
{
	int			elmlen;
	bool		elmbyval;
	char		elmalign;

	switch (elmtype)
	{
		case CHAROID:
			elmlen = 1;
			elmbyval = true;
			elmalign = TYPALIGN_CHAR;
			break;

		case CSTRINGOID:
			elmlen = -2;
			elmbyval = false;
			elmalign = TYPALIGN_CHAR;
			break;

		case FLOAT8OID:
			elmlen = sizeof(float8);
			elmbyval = FLOAT8PASSBYVAL;
			elmalign = TYPALIGN_DOUBLE;
			break;

		case INT2OID:
			elmlen = sizeof(int16);
			elmbyval = true;
			elmalign = TYPALIGN_SHORT;
			break;

		case INT4OID:
			elmlen = sizeof(int32);
			elmbyval = true;
			elmalign = TYPALIGN_INT;
			break;

		case OIDOID:
			elmlen = sizeof(Oid);
			elmbyval = true;
			elmalign = TYPALIGN_INT;
			break;

		case TEXTOID:
			elmlen = -1;
			elmbyval = false;
			elmalign = TYPALIGN_INT;
			break;

		case TIDOID:
			elmlen = sizeof(ItemPointerData);
			elmbyval = false;
			elmalign = TYPALIGN_SHORT;
			break;

		default:
			elog(ERROR, "type %u not supported by deconstruct_array_builtin()", elmtype);
			/* keep compiler quiet */
			elmlen = 0;
			elmbyval = false;
			elmalign = 0;
	}

	deconstruct_array(array, elmtype, elmlen, elmbyval, elmalign, elemsp, nullsp, nelemsp);
}

/* --- arrayfuncs.c array_contains_nulls (lines 3766-3807) --- */
/*
 * array_contains_nulls --- detect whether an array has any null elements
 *
 * This gives an accurate answer, whereas testing ARR_HASNULL only tells
 * if the array *might* contain a null.
 */
bool
array_contains_nulls(ArrayType *array)
{
	int			nelems;
	bits8	   *bitmap;
	int			bitmask;

	/* Easy answer if there's no null bitmap */
	if (!ARR_HASNULL(array))
		return false;

	nelems = ArrayGetNItems(ARR_NDIM(array), ARR_DIMS(array));

	bitmap = ARR_NULLBITMAP(array);

	/* check whole bytes of the bitmap byte-at-a-time */
	while (nelems >= 8)
	{
		if (*bitmap != 0xFF)
			return true;
		bitmap++;
		nelems -= 8;
	}

	/* check last partial byte */
	bitmask = 1;
	while (nelems > 0)
	{
		if ((*bitmap & bitmask) == 0)
			return true;
		bitmask <<= 1;
		nelems--;
	}

	return false;
}
