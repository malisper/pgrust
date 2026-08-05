/* VERBATIM segments from src/backend/utils/adt/jsonfuncs.c @ 62d6c7d3df
 * (18.3), extracted at function-comment boundaries — the ops/mutate/getfield
 * family for the jsonbops_diff target (p1-lanev). Static helpers
 * (JsonbValueAsText, get_jsonb_path_all, push_*, IteratorConcat, setPath*)
 * live in this TU only. Forward decls below mirror the jsonfuncs.c file
 * head (extraction omits the head). JB_PATH_* flag block copied verbatim
 * from the file head (lines 44-53). */

/* --- jsonfuncs.c JB_PATH_* flags (lines 44-53, verbatim) --- */
#define JB_PATH_CREATE					0x0001
#define JB_PATH_DELETE					0x0002
#define JB_PATH_REPLACE					0x0004
#define JB_PATH_INSERT_BEFORE			0x0008
#define JB_PATH_INSERT_AFTER			0x0010
#define JB_PATH_CREATE_OR_INSERT \
	(JB_PATH_INSERT_BEFORE | JB_PATH_INSERT_AFTER | JB_PATH_CREATE)
#define JB_PATH_FILL_GAPS				0x0020
#define JB_PATH_CONSISTENT_POSITION		0x0040

/* file-head static decls (jsonfuncs.c lines ~360-460, subset) */
static text *JsonbValueAsText(JsonbValue *v);
static Datum get_jsonb_path_all(FunctionCallInfo fcinfo, bool as_text);
static void push_null_elements(JsonbParseState **ps, int num);
static void push_path(JsonbParseState **st, int level, Datum *path_elems,
					  bool *path_nulls, int path_len, JsonbValue *newval);
static JsonbValue *IteratorConcat(JsonbIterator **it1, JsonbIterator **it2,
								  JsonbParseState **state);
static JsonbValue *setPath(JsonbIterator **it, Datum *path_elems,
						   bool *path_nulls, int path_len,
						   JsonbParseState **st, int level, JsonbValue *newval,
						   int op_type);
static void setPathObject(JsonbIterator **it, Datum *path_elems,
						  bool *path_nulls, int path_len, JsonbParseState **st,
						  int level,
						  JsonbValue *newval, uint32 npairs, int op_type);
static void setPathArray(JsonbIterator **it, Datum *path_elems,
						 bool *path_nulls, int path_len, JsonbParseState **st,
						 int level,
						 JsonbValue *newval, uint32 nelems, int op_type);

/* --- jsonfuncs.c JsonbValueAsText (lines 1801-1846) --- */
/*
 * Return the text representation of the given JsonbValue.
 */
static text *
JsonbValueAsText(JsonbValue *v)
{
	switch (v->type)
	{
		case jbvNull:
			return NULL;

		case jbvBool:
			return v->val.boolean ?
				cstring_to_text_with_len("true", 4) :
				cstring_to_text_with_len("false", 5);

		case jbvString:
			return cstring_to_text_with_len(v->val.string.val,
											v->val.string.len);

		case jbvNumeric:
			{
				Datum		cstr;

				cstr = DirectFunctionCall1(numeric_out,
										   PointerGetDatum(v->val.numeric));

				return cstring_to_text(DatumGetCString(cstr));
			}

		case jbvBinary:
			{
				StringInfoData jtext;

				initStringInfo(&jtext);
				(void) JsonbToCString(&jtext, v->val.binary.data,
									  v->val.binary.len);

				return cstring_to_text_with_len(jtext.data, jtext.len);
			}

		default:
			elog(ERROR, "unrecognized jsonb type: %d", (int) v->type);
			return NULL;
	}
}

/* --- jsonfuncs.c jsonb_object_field (lines 861-881) --- */
Datum
jsonb_object_field(PG_FUNCTION_ARGS)
{
	Jsonb	   *jb = PG_GETARG_JSONB_P(0);
	text	   *key = PG_GETARG_TEXT_PP(1);
	JsonbValue *v;
	JsonbValue	vbuf;

	if (!JB_ROOT_IS_OBJECT(jb))
		PG_RETURN_NULL();

	v = getKeyJsonValueFromContainer(&jb->root,
									 VARDATA_ANY(key),
									 VARSIZE_ANY_EXHDR(key),
									 &vbuf);

	if (v != NULL)
		PG_RETURN_JSONB_P(JsonbValueToJsonb(v));

	PG_RETURN_NULL();
}

/* --- jsonfuncs.c jsonb_object_field_text (lines 899-919) --- */
Datum
jsonb_object_field_text(PG_FUNCTION_ARGS)
{
	Jsonb	   *jb = PG_GETARG_JSONB_P(0);
	text	   *key = PG_GETARG_TEXT_PP(1);
	JsonbValue *v;
	JsonbValue	vbuf;

	if (!JB_ROOT_IS_OBJECT(jb))
		PG_RETURN_NULL();

	v = getKeyJsonValueFromContainer(&jb->root,
									 VARDATA_ANY(key),
									 VARSIZE_ANY_EXHDR(key),
									 &vbuf);

	if (v != NULL && v->type != jbvNull)
		PG_RETURN_TEXT_P(JsonbValueAsText(v));

	PG_RETURN_NULL();
}

/* --- jsonfuncs.c jsonb_array_element (lines 936-962) --- */
Datum
jsonb_array_element(PG_FUNCTION_ARGS)
{
	Jsonb	   *jb = PG_GETARG_JSONB_P(0);
	int			element = PG_GETARG_INT32(1);
	JsonbValue *v;

	if (!JB_ROOT_IS_ARRAY(jb))
		PG_RETURN_NULL();

	/* Handle negative subscript */
	if (element < 0)
	{
		uint32		nelements = JB_ROOT_COUNT(jb);

		if (pg_abs_s32(element) > nelements)
			PG_RETURN_NULL();
		else
			element += nelements;
	}

	v = getIthJsonbValueFromContainer(&jb->root, element);
	if (v != NULL)
		PG_RETURN_JSONB_P(JsonbValueToJsonb(v));

	PG_RETURN_NULL();
}

/* --- jsonfuncs.c jsonb_array_element_text (lines 979-1006) --- */
Datum
jsonb_array_element_text(PG_FUNCTION_ARGS)
{
	Jsonb	   *jb = PG_GETARG_JSONB_P(0);
	int			element = PG_GETARG_INT32(1);
	JsonbValue *v;

	if (!JB_ROOT_IS_ARRAY(jb))
		PG_RETURN_NULL();

	/* Handle negative subscript */
	if (element < 0)
	{
		uint32		nelements = JB_ROOT_COUNT(jb);

		if (pg_abs_s32(element) > nelements)
			PG_RETURN_NULL();
		else
			element += nelements;
	}

	v = getIthJsonbValueFromContainer(&jb->root, element);

	if (v != NULL && v->type != jbvNull)
		PG_RETURN_TEXT_P(JsonbValueAsText(v));

	PG_RETURN_NULL();
}

/* --- jsonfuncs.c jsonb_extract_path (lines 1487-1491) --- */
Datum
jsonb_extract_path(PG_FUNCTION_ARGS)
{
	return get_jsonb_path_all(fcinfo, false);
}

/* --- jsonfuncs.c jsonb_extract_path_text (lines 1493-1497) --- */
Datum
jsonb_extract_path_text(PG_FUNCTION_ARGS)
{
	return get_jsonb_path_all(fcinfo, true);
}

/* --- jsonfuncs.c get_jsonb_path_all (lines 1499-1528) --- */
static Datum
get_jsonb_path_all(FunctionCallInfo fcinfo, bool as_text)
{
	Jsonb	   *jb = PG_GETARG_JSONB_P(0);
	ArrayType  *path = PG_GETARG_ARRAYTYPE_P(1);
	Datum	   *pathtext;
	bool	   *pathnulls;
	bool		isnull;
	int			npath;
	Datum		res;

	/*
	 * If the array contains any null elements, return NULL, on the grounds
	 * that you'd have gotten NULL if any RHS value were NULL in a nested
	 * series of applications of the -> operator.  (Note: because we also
	 * return NULL for error cases such as no-such-field, this is true
	 * regardless of the contents of the rest of the array.)
	 */
	if (array_contains_nulls(path))
		PG_RETURN_NULL();

	deconstruct_array_builtin(path, TEXTOID, &pathtext, &pathnulls, &npath);

	res = jsonb_get_element(jb, pathtext, npath, &isnull, as_text);

	if (isnull)
		PG_RETURN_NULL();
	else
		PG_RETURN_DATUM(res);
}

/* --- jsonfuncs.c jsonb_get_element (lines 1530-1676) --- */
Datum
jsonb_get_element(Jsonb *jb, Datum *path, int npath, bool *isnull, bool as_text)
{
	JsonbContainer *container = &jb->root;
	JsonbValue *jbvp = NULL;
	int			i;
	bool		have_object = false,
				have_array = false;

	*isnull = false;

	/* Identify whether we have object, array, or scalar at top-level */
	if (JB_ROOT_IS_OBJECT(jb))
		have_object = true;
	else if (JB_ROOT_IS_ARRAY(jb) && !JB_ROOT_IS_SCALAR(jb))
		have_array = true;
	else
	{
		Assert(JB_ROOT_IS_ARRAY(jb) && JB_ROOT_IS_SCALAR(jb));
		/* Extract the scalar value, if it is what we'll return */
		if (npath <= 0)
			jbvp = getIthJsonbValueFromContainer(container, 0);
	}

	/*
	 * If the array is empty, return the entire LHS object, on the grounds
	 * that we should do zero field or element extractions.  For the
	 * non-scalar case we can just hand back the object without much work. For
	 * the scalar case, fall through and deal with the value below the loop.
	 * (This inconsistency arises because there's no easy way to generate a
	 * JsonbValue directly for root-level containers.)
	 */
	if (npath <= 0 && jbvp == NULL)
	{
		if (as_text)
		{
			return PointerGetDatum(cstring_to_text(JsonbToCString(NULL,
																  container,
																  VARSIZE(jb))));
		}
		else
		{
			/* not text mode - just hand back the jsonb */
			PG_RETURN_JSONB_P(jb);
		}
	}

	for (i = 0; i < npath; i++)
	{
		if (have_object)
		{
			text	   *subscr = DatumGetTextPP(path[i]);

			jbvp = getKeyJsonValueFromContainer(container,
												VARDATA_ANY(subscr),
												VARSIZE_ANY_EXHDR(subscr),
												NULL);
		}
		else if (have_array)
		{
			int			lindex;
			uint32		index;
			char	   *indextext = TextDatumGetCString(path[i]);
			char	   *endptr;

			errno = 0;
			lindex = strtoint(indextext, &endptr, 10);
			if (endptr == indextext || *endptr != '\0' || errno != 0)
			{
				*isnull = true;
				return PointerGetDatum(NULL);
			}

			if (lindex >= 0)
			{
				index = (uint32) lindex;
			}
			else
			{
				/* Handle negative subscript */
				uint32		nelements;

				/* Container must be array, but make sure */
				if (!JsonContainerIsArray(container))
					elog(ERROR, "not a jsonb array");

				nelements = JsonContainerSize(container);

				if (lindex == INT_MIN || -lindex > nelements)
				{
					*isnull = true;
					return PointerGetDatum(NULL);
				}
				else
					index = nelements + lindex;
			}

			jbvp = getIthJsonbValueFromContainer(container, index);
		}
		else
		{
			/* scalar, extraction yields a null */
			*isnull = true;
			return PointerGetDatum(NULL);
		}

		if (jbvp == NULL)
		{
			*isnull = true;
			return PointerGetDatum(NULL);
		}
		else if (i == npath - 1)
			break;

		if (jbvp->type == jbvBinary)
		{
			container = jbvp->val.binary.data;
			have_object = JsonContainerIsObject(container);
			have_array = JsonContainerIsArray(container);
			Assert(!JsonContainerIsScalar(container));
		}
		else
		{
			Assert(IsAJsonbScalar(jbvp));
			have_object = false;
			have_array = false;
		}
	}

	if (as_text)
	{
		if (jbvp->type == jbvNull)
		{
			*isnull = true;
			return PointerGetDatum(NULL);
		}

		return PointerGetDatum(JsonbValueAsText(jbvp));
	}
	else
	{
		Jsonb	   *res = JsonbValueToJsonb(jbvp);

		/* not text mode - just hand back the jsonb */
		PG_RETURN_JSONB_P(res);
	}
}

/* --- jsonfuncs.c push_null_elements (lines 1701-1710) --- */
static void
push_null_elements(JsonbParseState **ps, int num)
{
	JsonbValue	null;

	null.type = jbvNull;

	while (num-- > 0)
		pushJsonbValue(ps, WJB_ELEM, &null);
}

/* --- jsonfuncs.c push_path (lines 1712-1799) --- */
/*
 * Prepare a new structure containing nested empty objects and arrays
 * corresponding to the specified path, and assign a new value at the end of
 * this path. E.g. the path [a][0][b] with the new value 1 will produce the
 * structure {a: [{b: 1}]}.
 *
 * Caller is responsible to make sure such path does not exist yet.
 */
static void
push_path(JsonbParseState **st, int level, Datum *path_elems,
		  bool *path_nulls, int path_len, JsonbValue *newval)
{
	/*
	 * tpath contains expected type of an empty jsonb created at each level
	 * higher or equal to the current one, either jbvObject or jbvArray. Since
	 * it contains only information about path slice from level to the end,
	 * the access index must be normalized by level.
	 */
	enum jbvType *tpath = palloc0((path_len - level) * sizeof(enum jbvType));
	JsonbValue	newkey;

	/*
	 * Create first part of the chain with beginning tokens. For the current
	 * level WJB_BEGIN_OBJECT/WJB_BEGIN_ARRAY was already created, so start
	 * with the next one.
	 */
	for (int i = level + 1; i < path_len; i++)
	{
		char	   *c,
				   *badp;
		int			lindex;

		if (path_nulls[i])
			break;

		/*
		 * Try to convert to an integer to find out the expected type, object
		 * or array.
		 */
		c = TextDatumGetCString(path_elems[i]);
		errno = 0;
		lindex = strtoint(c, &badp, 10);
		if (badp == c || *badp != '\0' || errno != 0)
		{
			/* text, an object is expected */
			newkey.type = jbvString;
			newkey.val.string.val = c;
			newkey.val.string.len = strlen(c);

			(void) pushJsonbValue(st, WJB_BEGIN_OBJECT, NULL);
			(void) pushJsonbValue(st, WJB_KEY, &newkey);

			tpath[i - level] = jbvObject;
		}
		else
		{
			/* integer, an array is expected */
			(void) pushJsonbValue(st, WJB_BEGIN_ARRAY, NULL);

			push_null_elements(st, lindex);

			tpath[i - level] = jbvArray;
		}
	}

	/* Insert an actual value for either an object or array */
	if (tpath[(path_len - level) - 1] == jbvArray)
	{
		(void) pushJsonbValue(st, WJB_ELEM, newval);
	}
	else
		(void) pushJsonbValue(st, WJB_VALUE, newval);

	/*
	 * Close everything up to the last but one level. The last one will be
	 * closed outside of this function.
	 */
	for (int i = path_len - 1; i > level; i--)
	{
		if (path_nulls[i])
			break;

		if (tpath[i - level] == jbvObject)
			(void) pushJsonbValue(st, WJB_END_OBJECT, NULL);
		else
			(void) pushJsonbValue(st, WJB_END_ARRAY, NULL);
	}
}

/* --- jsonfuncs.c jsonb_concat (lines 4617-4654) --- */
/*
 * SQL function jsonb_concat (jsonb, jsonb)
 *
 * function for || operator
 */
Datum
jsonb_concat(PG_FUNCTION_ARGS)
{
	Jsonb	   *jb1 = PG_GETARG_JSONB_P(0);
	Jsonb	   *jb2 = PG_GETARG_JSONB_P(1);
	JsonbParseState *state = NULL;
	JsonbValue *res;
	JsonbIterator *it1,
			   *it2;

	/*
	 * If one of the jsonb is empty, just return the other if it's not scalar
	 * and both are of the same kind.  If it's a scalar or they are of
	 * different kinds we need to perform the concatenation even if one is
	 * empty.
	 */
	if (JB_ROOT_IS_OBJECT(jb1) == JB_ROOT_IS_OBJECT(jb2))
	{
		if (JB_ROOT_COUNT(jb1) == 0 && !JB_ROOT_IS_SCALAR(jb2))
			PG_RETURN_JSONB_P(jb2);
		else if (JB_ROOT_COUNT(jb2) == 0 && !JB_ROOT_IS_SCALAR(jb1))
			PG_RETURN_JSONB_P(jb1);
	}

	it1 = JsonbIteratorInit(&jb1->root);
	it2 = JsonbIteratorInit(&jb2->root);

	res = IteratorConcat(&it1, &it2, &state);

	Assert(res != NULL);

	PG_RETURN_JSONB_P(JsonbValueToJsonb(res));
}

/* --- jsonfuncs.c jsonb_delete (lines 4657-4708) --- */
/*
 * SQL function jsonb_delete (jsonb, text)
 *
 * return a copy of the jsonb with the indicated item
 * removed.
 */
Datum
jsonb_delete(PG_FUNCTION_ARGS)
{
	Jsonb	   *in = PG_GETARG_JSONB_P(0);
	text	   *key = PG_GETARG_TEXT_PP(1);
	char	   *keyptr = VARDATA_ANY(key);
	int			keylen = VARSIZE_ANY_EXHDR(key);
	JsonbParseState *state = NULL;
	JsonbIterator *it;
	JsonbValue	v,
			   *res = NULL;
	bool		skipNested = false;
	JsonbIteratorToken r;

	if (JB_ROOT_IS_SCALAR(in))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cannot delete from scalar")));

	if (JB_ROOT_COUNT(in) == 0)
		PG_RETURN_JSONB_P(in);

	it = JsonbIteratorInit(&in->root);

	while ((r = JsonbIteratorNext(&it, &v, skipNested)) != WJB_DONE)
	{
		skipNested = true;

		if ((r == WJB_ELEM || r == WJB_KEY) &&
			(v.type == jbvString && keylen == v.val.string.len &&
			 memcmp(keyptr, v.val.string.val, keylen) == 0))
		{
			/* skip corresponding value as well */
			if (r == WJB_KEY)
				(void) JsonbIteratorNext(&it, &v, true);

			continue;
		}

		res = pushJsonbValue(&state, r, r < WJB_BEGIN_ARRAY ? &v : NULL);
	}

	Assert(res != NULL);

	PG_RETURN_JSONB_P(JsonbValueToJsonb(res));
}

/* --- jsonfuncs.c jsonb_delete_array (lines 4710-4794) --- */
/*
 * SQL function jsonb_delete (jsonb, variadic text[])
 *
 * return a copy of the jsonb with the indicated items
 * removed.
 */
Datum
jsonb_delete_array(PG_FUNCTION_ARGS)
{
	Jsonb	   *in = PG_GETARG_JSONB_P(0);
	ArrayType  *keys = PG_GETARG_ARRAYTYPE_P(1);
	Datum	   *keys_elems;
	bool	   *keys_nulls;
	int			keys_len;
	JsonbParseState *state = NULL;
	JsonbIterator *it;
	JsonbValue	v,
			   *res = NULL;
	bool		skipNested = false;
	JsonbIteratorToken r;

	if (ARR_NDIM(keys) > 1)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("wrong number of array subscripts")));

	if (JB_ROOT_IS_SCALAR(in))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cannot delete from scalar")));

	if (JB_ROOT_COUNT(in) == 0)
		PG_RETURN_JSONB_P(in);

	deconstruct_array_builtin(keys, TEXTOID, &keys_elems, &keys_nulls, &keys_len);

	if (keys_len == 0)
		PG_RETURN_JSONB_P(in);

	it = JsonbIteratorInit(&in->root);

	while ((r = JsonbIteratorNext(&it, &v, skipNested)) != WJB_DONE)
	{
		skipNested = true;

		if ((r == WJB_ELEM || r == WJB_KEY) && v.type == jbvString)
		{
			int			i;
			bool		found = false;

			for (i = 0; i < keys_len; i++)
			{
				char	   *keyptr;
				int			keylen;

				if (keys_nulls[i])
					continue;

				/* We rely on the array elements not being toasted */
				keyptr = VARDATA_ANY(keys_elems[i]);
				keylen = VARSIZE_ANY_EXHDR(keys_elems[i]);
				if (keylen == v.val.string.len &&
					memcmp(keyptr, v.val.string.val, keylen) == 0)
				{
					found = true;
					break;
				}
			}
			if (found)
			{
				/* skip corresponding value as well */
				if (r == WJB_KEY)
					(void) JsonbIteratorNext(&it, &v, true);

				continue;
			}
		}

		res = pushJsonbValue(&state, r, r < WJB_BEGIN_ARRAY ? &v : NULL);
	}

	Assert(res != NULL);

	PG_RETURN_JSONB_P(JsonbValueToJsonb(res));
}

/* --- jsonfuncs.c jsonb_delete_idx (lines 4796-4862) --- */
/*
 * SQL function jsonb_delete (jsonb, int)
 *
 * return a copy of the jsonb with the indicated item
 * removed. Negative int means count back from the
 * end of the items.
 */
Datum
jsonb_delete_idx(PG_FUNCTION_ARGS)
{
	Jsonb	   *in = PG_GETARG_JSONB_P(0);
	int			idx = PG_GETARG_INT32(1);
	JsonbParseState *state = NULL;
	JsonbIterator *it;
	uint32		i = 0,
				n;
	JsonbValue	v,
			   *res = NULL;
	JsonbIteratorToken r;

	if (JB_ROOT_IS_SCALAR(in))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cannot delete from scalar")));

	if (JB_ROOT_IS_OBJECT(in))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cannot delete from object using integer index")));

	if (JB_ROOT_COUNT(in) == 0)
		PG_RETURN_JSONB_P(in);

	it = JsonbIteratorInit(&in->root);

	r = JsonbIteratorNext(&it, &v, false);
	Assert(r == WJB_BEGIN_ARRAY);
	n = v.val.array.nElems;

	if (idx < 0)
	{
		if (pg_abs_s32(idx) > n)
			idx = n;
		else
			idx = n + idx;
	}

	if (idx >= n)
		PG_RETURN_JSONB_P(in);

	pushJsonbValue(&state, r, NULL);

	while ((r = JsonbIteratorNext(&it, &v, true)) != WJB_DONE)
	{
		if (r == WJB_ELEM)
		{
			if (i++ == idx)
				continue;
		}

		res = pushJsonbValue(&state, r, r < WJB_BEGIN_ARRAY ? &v : NULL);
	}

	Assert(res != NULL);

	PG_RETURN_JSONB_P(JsonbValueToJsonb(res));
}

/* --- jsonfuncs.c jsonb_set (lines 4864-4910) --- */
/*
 * SQL function jsonb_set(jsonb, text[], jsonb, boolean)
 */
Datum
jsonb_set(PG_FUNCTION_ARGS)
{
	Jsonb	   *in = PG_GETARG_JSONB_P(0);
	ArrayType  *path = PG_GETARG_ARRAYTYPE_P(1);
	Jsonb	   *newjsonb = PG_GETARG_JSONB_P(2);
	JsonbValue	newval;
	bool		create = PG_GETARG_BOOL(3);
	JsonbValue *res = NULL;
	Datum	   *path_elems;
	bool	   *path_nulls;
	int			path_len;
	JsonbIterator *it;
	JsonbParseState *st = NULL;

	JsonbToJsonbValue(newjsonb, &newval);

	if (ARR_NDIM(path) > 1)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("wrong number of array subscripts")));

	if (JB_ROOT_IS_SCALAR(in))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cannot set path in scalar")));

	if (JB_ROOT_COUNT(in) == 0 && !create)
		PG_RETURN_JSONB_P(in);

	deconstruct_array_builtin(path, TEXTOID, &path_elems, &path_nulls, &path_len);

	if (path_len == 0)
		PG_RETURN_JSONB_P(in);

	it = JsonbIteratorInit(&in->root);

	res = setPath(&it, path_elems, path_nulls, path_len, &st,
				  0, &newval, create ? JB_PATH_CREATE : JB_PATH_REPLACE);

	Assert(res != NULL);

	PG_RETURN_JSONB_P(JsonbValueToJsonb(res));
}

/* --- jsonfuncs.c jsonb_delete_path (lines 4980-5021) --- */
/*
 * SQL function jsonb_delete_path(jsonb, text[])
 */
Datum
jsonb_delete_path(PG_FUNCTION_ARGS)
{
	Jsonb	   *in = PG_GETARG_JSONB_P(0);
	ArrayType  *path = PG_GETARG_ARRAYTYPE_P(1);
	JsonbValue *res = NULL;
	Datum	   *path_elems;
	bool	   *path_nulls;
	int			path_len;
	JsonbIterator *it;
	JsonbParseState *st = NULL;

	if (ARR_NDIM(path) > 1)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("wrong number of array subscripts")));

	if (JB_ROOT_IS_SCALAR(in))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cannot delete path in scalar")));

	if (JB_ROOT_COUNT(in) == 0)
		PG_RETURN_JSONB_P(in);

	deconstruct_array_builtin(path, TEXTOID, &path_elems, &path_nulls, &path_len);

	if (path_len == 0)
		PG_RETURN_JSONB_P(in);

	it = JsonbIteratorInit(&in->root);

	res = setPath(&it, path_elems, path_nulls, path_len, &st,
				  0, NULL, JB_PATH_DELETE);

	Assert(res != NULL);

	PG_RETURN_JSONB_P(JsonbValueToJsonb(res));
}

/* --- jsonfuncs.c jsonb_insert (lines 5023-5066) --- */
/*
 * SQL function jsonb_insert(jsonb, text[], jsonb, boolean)
 */
Datum
jsonb_insert(PG_FUNCTION_ARGS)
{
	Jsonb	   *in = PG_GETARG_JSONB_P(0);
	ArrayType  *path = PG_GETARG_ARRAYTYPE_P(1);
	Jsonb	   *newjsonb = PG_GETARG_JSONB_P(2);
	JsonbValue	newval;
	bool		after = PG_GETARG_BOOL(3);
	JsonbValue *res = NULL;
	Datum	   *path_elems;
	bool	   *path_nulls;
	int			path_len;
	JsonbIterator *it;
	JsonbParseState *st = NULL;

	JsonbToJsonbValue(newjsonb, &newval);

	if (ARR_NDIM(path) > 1)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("wrong number of array subscripts")));

	if (JB_ROOT_IS_SCALAR(in))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cannot set path in scalar")));

	deconstruct_array_builtin(path, TEXTOID, &path_elems, &path_nulls, &path_len);

	if (path_len == 0)
		PG_RETURN_JSONB_P(in);

	it = JsonbIteratorInit(&in->root);

	res = setPath(&it, path_elems, path_nulls, path_len, &st, 0, &newval,
				  after ? JB_PATH_INSERT_AFTER : JB_PATH_INSERT_BEFORE);

	Assert(res != NULL);

	PG_RETURN_JSONB_P(JsonbValueToJsonb(res));
}

/* --- jsonfuncs.c IteratorConcat (lines 5068-5172) --- */
/*
 * Iterate over all jsonb objects and merge them into one.
 * The logic of this function copied from the same hstore function,
 * except the case, when it1 & it2 represents jbvObject.
 * In that case we just append the content of it2 to it1 without any
 * verifications.
 */
static JsonbValue *
IteratorConcat(JsonbIterator **it1, JsonbIterator **it2,
			   JsonbParseState **state)
{
	JsonbValue	v1,
				v2,
			   *res = NULL;
	JsonbIteratorToken r1,
				r2,
				rk1,
				rk2;

	rk1 = JsonbIteratorNext(it1, &v1, false);
	rk2 = JsonbIteratorNext(it2, &v2, false);

	/*
	 * JsonbIteratorNext reports raw scalars as if they were single-element
	 * arrays; hence we only need consider "object" and "array" cases here.
	 */
	if (rk1 == WJB_BEGIN_OBJECT && rk2 == WJB_BEGIN_OBJECT)
	{
		/*
		 * Both inputs are objects.
		 *
		 * Append all the tokens from v1 to res, except last WJB_END_OBJECT
		 * (because res will not be finished yet).
		 */
		pushJsonbValue(state, rk1, NULL);
		while ((r1 = JsonbIteratorNext(it1, &v1, true)) != WJB_END_OBJECT)
			pushJsonbValue(state, r1, &v1);

		/*
		 * Append all the tokens from v2 to res, including last WJB_END_OBJECT
		 * (the concatenation will be completed).  Any duplicate keys will
		 * automatically override the value from the first object.
		 */
		while ((r2 = JsonbIteratorNext(it2, &v2, true)) != WJB_DONE)
			res = pushJsonbValue(state, r2, r2 != WJB_END_OBJECT ? &v2 : NULL);
	}
	else if (rk1 == WJB_BEGIN_ARRAY && rk2 == WJB_BEGIN_ARRAY)
	{
		/*
		 * Both inputs are arrays.
		 */
		pushJsonbValue(state, rk1, NULL);

		while ((r1 = JsonbIteratorNext(it1, &v1, true)) != WJB_END_ARRAY)
		{
			Assert(r1 == WJB_ELEM);
			pushJsonbValue(state, r1, &v1);
		}

		while ((r2 = JsonbIteratorNext(it2, &v2, true)) != WJB_END_ARRAY)
		{
			Assert(r2 == WJB_ELEM);
			pushJsonbValue(state, WJB_ELEM, &v2);
		}

		res = pushJsonbValue(state, WJB_END_ARRAY, NULL /* signal to sort */ );
	}
	else if (rk1 == WJB_BEGIN_OBJECT)
	{
		/*
		 * We have object || array.
		 */
		Assert(rk2 == WJB_BEGIN_ARRAY);

		pushJsonbValue(state, WJB_BEGIN_ARRAY, NULL);

		pushJsonbValue(state, WJB_BEGIN_OBJECT, NULL);
		while ((r1 = JsonbIteratorNext(it1, &v1, true)) != WJB_DONE)
			pushJsonbValue(state, r1, r1 != WJB_END_OBJECT ? &v1 : NULL);

		while ((r2 = JsonbIteratorNext(it2, &v2, true)) != WJB_DONE)
			res = pushJsonbValue(state, r2, r2 != WJB_END_ARRAY ? &v2 : NULL);
	}
	else
	{
		/*
		 * We have array || object.
		 */
		Assert(rk1 == WJB_BEGIN_ARRAY);
		Assert(rk2 == WJB_BEGIN_OBJECT);

		pushJsonbValue(state, WJB_BEGIN_ARRAY, NULL);

		while ((r1 = JsonbIteratorNext(it1, &v1, true)) != WJB_END_ARRAY)
			pushJsonbValue(state, r1, &v1);

		pushJsonbValue(state, WJB_BEGIN_OBJECT, NULL);
		while ((r2 = JsonbIteratorNext(it2, &v2, true)) != WJB_DONE)
			pushJsonbValue(state, r2, r2 != WJB_END_OBJECT ? &v2 : NULL);

		res = pushJsonbValue(state, WJB_END_ARRAY, NULL);
	}

	return res;
}

/* --- jsonfuncs.c setPath (lines 5174-5280) --- */
/*
 * Do most of the heavy work for jsonb_set/jsonb_insert
 *
 * If JB_PATH_DELETE bit is set in op_type, the element is to be removed.
 *
 * If any bit mentioned in JB_PATH_CREATE_OR_INSERT is set in op_type,
 * we create the new value if the key or array index does not exist.
 *
 * Bits JB_PATH_INSERT_BEFORE and JB_PATH_INSERT_AFTER in op_type
 * behave as JB_PATH_CREATE if new value is inserted in JsonbObject.
 *
 * If JB_PATH_FILL_GAPS bit is set, this will change an assignment logic in
 * case if target is an array. The assignment index will not be restricted by
 * number of elements in the array, and if there are any empty slots between
 * last element of the array and a new one they will be filled with nulls. If
 * the index is negative, it still will be considered an index from the end
 * of the array. Of a part of the path is not present and this part is more
 * than just one last element, this flag will instruct to create the whole
 * chain of corresponding objects and insert the value.
 *
 * JB_PATH_CONSISTENT_POSITION for an array indicates that the caller wants to
 * keep values with fixed indices. Indices for existing elements could be
 * changed (shifted forward) in case if the array is prepended with a new value
 * and a negative index out of the range, so this behavior will be prevented
 * and return an error.
 *
 * All path elements before the last must already exist
 * whatever bits in op_type are set, or nothing is done.
 */
static JsonbValue *
setPath(JsonbIterator **it, Datum *path_elems,
		bool *path_nulls, int path_len,
		JsonbParseState **st, int level, JsonbValue *newval, int op_type)
{
	JsonbValue	v;
	JsonbIteratorToken r;
	JsonbValue *res;

	check_stack_depth();

	if (path_nulls[level])
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("path element at position %d is null",
						level + 1)));

	r = JsonbIteratorNext(it, &v, false);

	switch (r)
	{
		case WJB_BEGIN_ARRAY:

			/*
			 * If instructed complain about attempts to replace within a raw
			 * scalar value. This happens even when current level is equal to
			 * path_len, because the last path key should also correspond to
			 * an object or an array, not raw scalar.
			 */
			if ((op_type & JB_PATH_FILL_GAPS) && (level <= path_len - 1) &&
				v.val.array.rawScalar)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("cannot replace existing key"),
						 errdetail("The path assumes key is a composite object, "
								   "but it is a scalar value.")));

			(void) pushJsonbValue(st, r, NULL);
			setPathArray(it, path_elems, path_nulls, path_len, st, level,
						 newval, v.val.array.nElems, op_type);
			r = JsonbIteratorNext(it, &v, false);
			Assert(r == WJB_END_ARRAY);
			res = pushJsonbValue(st, r, NULL);
			break;
		case WJB_BEGIN_OBJECT:
			(void) pushJsonbValue(st, r, NULL);
			setPathObject(it, path_elems, path_nulls, path_len, st, level,
						  newval, v.val.object.nPairs, op_type);
			r = JsonbIteratorNext(it, &v, true);
			Assert(r == WJB_END_OBJECT);
			res = pushJsonbValue(st, r, NULL);
			break;
		case WJB_ELEM:
		case WJB_VALUE:

			/*
			 * If instructed complain about attempts to replace within a
			 * scalar value. This happens even when current level is equal to
			 * path_len, because the last path key should also correspond to
			 * an object or an array, not an element or value.
			 */
			if ((op_type & JB_PATH_FILL_GAPS) && (level <= path_len - 1))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("cannot replace existing key"),
						 errdetail("The path assumes key is a composite object, "
								   "but it is a scalar value.")));

			res = pushJsonbValue(st, r, &v);
			break;
		default:
			elog(ERROR, "unrecognized iterator result: %d", (int) r);
			res = NULL;			/* keep compiler quiet */
			break;
	}

	return res;
}

/* --- jsonfuncs.c setPathObject (lines 5282-5419) --- */
/*
 * Object walker for setPath
 */
static void
setPathObject(JsonbIterator **it, Datum *path_elems, bool *path_nulls,
			  int path_len, JsonbParseState **st, int level,
			  JsonbValue *newval, uint32 npairs, int op_type)
{
	text	   *pathelem = NULL;
	int			i;
	JsonbValue	k,
				v;
	bool		done = false;

	if (level >= path_len || path_nulls[level])
		done = true;
	else
	{
		/* The path Datum could be toasted, in which case we must detoast it */
		pathelem = DatumGetTextPP(path_elems[level]);
	}

	/* empty object is a special case for create */
	if ((npairs == 0) && (op_type & JB_PATH_CREATE_OR_INSERT) &&
		(level == path_len - 1))
	{
		JsonbValue	newkey;

		newkey.type = jbvString;
		newkey.val.string.val = VARDATA_ANY(pathelem);
		newkey.val.string.len = VARSIZE_ANY_EXHDR(pathelem);

		(void) pushJsonbValue(st, WJB_KEY, &newkey);
		(void) pushJsonbValue(st, WJB_VALUE, newval);
	}

	for (i = 0; i < npairs; i++)
	{
		JsonbIteratorToken r = JsonbIteratorNext(it, &k, true);

		Assert(r == WJB_KEY);

		if (!done &&
			k.val.string.len == VARSIZE_ANY_EXHDR(pathelem) &&
			memcmp(k.val.string.val, VARDATA_ANY(pathelem),
				   k.val.string.len) == 0)
		{
			done = true;

			if (level == path_len - 1)
			{
				/*
				 * called from jsonb_insert(), it forbids redefining an
				 * existing value
				 */
				if (op_type & (JB_PATH_INSERT_BEFORE | JB_PATH_INSERT_AFTER))
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("cannot replace existing key"),
							 errhint("Try using the function jsonb_set "
									 "to replace key value.")));

				r = JsonbIteratorNext(it, &v, true);	/* skip value */
				if (!(op_type & JB_PATH_DELETE))
				{
					(void) pushJsonbValue(st, WJB_KEY, &k);
					(void) pushJsonbValue(st, WJB_VALUE, newval);
				}
			}
			else
			{
				(void) pushJsonbValue(st, r, &k);
				setPath(it, path_elems, path_nulls, path_len,
						st, level + 1, newval, op_type);
			}
		}
		else
		{
			if ((op_type & JB_PATH_CREATE_OR_INSERT) && !done &&
				level == path_len - 1 && i == npairs - 1)
			{
				JsonbValue	newkey;

				newkey.type = jbvString;
				newkey.val.string.val = VARDATA_ANY(pathelem);
				newkey.val.string.len = VARSIZE_ANY_EXHDR(pathelem);

				(void) pushJsonbValue(st, WJB_KEY, &newkey);
				(void) pushJsonbValue(st, WJB_VALUE, newval);
			}

			(void) pushJsonbValue(st, r, &k);
			r = JsonbIteratorNext(it, &v, false);
			(void) pushJsonbValue(st, r, r < WJB_BEGIN_ARRAY ? &v : NULL);
			if (r == WJB_BEGIN_ARRAY || r == WJB_BEGIN_OBJECT)
			{
				int			walking_level = 1;

				while (walking_level != 0)
				{
					r = JsonbIteratorNext(it, &v, false);

					if (r == WJB_BEGIN_ARRAY || r == WJB_BEGIN_OBJECT)
						++walking_level;
					if (r == WJB_END_ARRAY || r == WJB_END_OBJECT)
						--walking_level;

					(void) pushJsonbValue(st, r, r < WJB_BEGIN_ARRAY ? &v : NULL);
				}
			}
		}
	}

	/*--
	 * If we got here there are only few possibilities:
	 * - no target path was found, and an open object with some keys/values was
	 *   pushed into the state
	 * - an object is empty, only WJB_BEGIN_OBJECT is pushed
	 *
	 * In both cases if instructed to create the path when not present,
	 * generate the whole chain of empty objects and insert the new value
	 * there.
	 */
	if (!done && (op_type & JB_PATH_FILL_GAPS) && (level < path_len - 1))
	{
		JsonbValue	newkey;

		newkey.type = jbvString;
		newkey.val.string.val = VARDATA_ANY(pathelem);
		newkey.val.string.len = VARSIZE_ANY_EXHDR(pathelem);

		(void) pushJsonbValue(st, WJB_KEY, &newkey);
		(void) push_path(st, level, path_elems, path_nulls,
						 path_len, newval);

		/* Result is closed with WJB_END_OBJECT outside of this function */
	}
}

/* --- jsonfuncs.c setPathArray (lines 5421-5588) --- */
/*
 * Array walker for setPath
 */
static void
setPathArray(JsonbIterator **it, Datum *path_elems, bool *path_nulls,
			 int path_len, JsonbParseState **st, int level,
			 JsonbValue *newval, uint32 nelems, int op_type)
{
	JsonbValue	v;
	int			idx,
				i;
	bool		done = false;

	/* pick correct index */
	if (level < path_len && !path_nulls[level])
	{
		char	   *c = TextDatumGetCString(path_elems[level]);
		char	   *badp;

		errno = 0;
		idx = strtoint(c, &badp, 10);
		if (badp == c || *badp != '\0' || errno != 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("path element at position %d is not an integer: \"%s\"",
							level + 1, c)));
	}
	else
		idx = nelems;

	if (idx < 0)
	{
		if (pg_abs_s32(idx) > nelems)
		{
			/*
			 * If asked to keep elements position consistent, it's not allowed
			 * to prepend the array.
			 */
			if (op_type & JB_PATH_CONSISTENT_POSITION)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("path element at position %d is out of range: %d",
								level + 1, idx)));
			else
				idx = PG_INT32_MIN;
		}
		else
			idx = nelems + idx;
	}

	/*
	 * Filling the gaps means there are no limits on the positive index are
	 * imposed, we can set any element. Otherwise limit the index by nelems.
	 */
	if (!(op_type & JB_PATH_FILL_GAPS))
	{
		if (idx > 0 && idx > nelems)
			idx = nelems;
	}

	/*
	 * if we're creating, and idx == INT_MIN, we prepend the new value to the
	 * array also if the array is empty - in which case we don't really care
	 * what the idx value is
	 */
	if ((idx == INT_MIN || nelems == 0) && (level == path_len - 1) &&
		(op_type & JB_PATH_CREATE_OR_INSERT))
	{
		Assert(newval != NULL);

		if (op_type & JB_PATH_FILL_GAPS && nelems == 0 && idx > 0)
			push_null_elements(st, idx);

		(void) pushJsonbValue(st, WJB_ELEM, newval);

		done = true;
	}

	/* iterate over the array elements */
	for (i = 0; i < nelems; i++)
	{
		JsonbIteratorToken r;

		if (i == idx && level < path_len)
		{
			done = true;

			if (level == path_len - 1)
			{
				r = JsonbIteratorNext(it, &v, true);	/* skip */

				if (op_type & (JB_PATH_INSERT_BEFORE | JB_PATH_CREATE))
					(void) pushJsonbValue(st, WJB_ELEM, newval);

				/*
				 * We should keep current value only in case of
				 * JB_PATH_INSERT_BEFORE or JB_PATH_INSERT_AFTER because
				 * otherwise it should be deleted or replaced
				 */
				if (op_type & (JB_PATH_INSERT_AFTER | JB_PATH_INSERT_BEFORE))
					(void) pushJsonbValue(st, r, &v);

				if (op_type & (JB_PATH_INSERT_AFTER | JB_PATH_REPLACE))
					(void) pushJsonbValue(st, WJB_ELEM, newval);
			}
			else
				(void) setPath(it, path_elems, path_nulls, path_len,
							   st, level + 1, newval, op_type);
		}
		else
		{
			r = JsonbIteratorNext(it, &v, false);

			(void) pushJsonbValue(st, r, r < WJB_BEGIN_ARRAY ? &v : NULL);

			if (r == WJB_BEGIN_ARRAY || r == WJB_BEGIN_OBJECT)
			{
				int			walking_level = 1;

				while (walking_level != 0)
				{
					r = JsonbIteratorNext(it, &v, false);

					if (r == WJB_BEGIN_ARRAY || r == WJB_BEGIN_OBJECT)
						++walking_level;
					if (r == WJB_END_ARRAY || r == WJB_END_OBJECT)
						--walking_level;

					(void) pushJsonbValue(st, r, r < WJB_BEGIN_ARRAY ? &v : NULL);
				}
			}
		}
	}

	if ((op_type & JB_PATH_CREATE_OR_INSERT) && !done && level == path_len - 1)
	{
		/*
		 * If asked to fill the gaps, idx could be bigger than nelems, so
		 * prepend the new element with nulls if that's the case.
		 */
		if (op_type & JB_PATH_FILL_GAPS && idx > nelems)
			push_null_elements(st, idx - nelems);

		(void) pushJsonbValue(st, WJB_ELEM, newval);
		done = true;
	}

	/*--
	 * If we got here there are only few possibilities:
	 * - no target path was found, and an open array with some keys/values was
	 *   pushed into the state
	 * - an array is empty, only WJB_BEGIN_ARRAY is pushed
	 *
	 * In both cases if instructed to create the path when not present,
	 * generate the whole chain of empty objects and insert the new value
	 * there.
	 */
	if (!done && (op_type & JB_PATH_FILL_GAPS) && (level < path_len - 1))
	{
		if (idx > 0)
			push_null_elements(st, idx - nelems);

		(void) push_path(st, level, path_elems, path_nulls,
						 path_len, newval);

		/* Result is closed with WJB_END_OBJECT outside of this function */
	}
}
