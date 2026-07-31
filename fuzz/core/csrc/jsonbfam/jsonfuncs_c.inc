/* VERBATIM segments from src/backend/utils/adt/jsonfuncs.c @ 62d6c7d3df
 * (18.3), extracted at function-comment boundaries. */

/* --- jsonfuncs.c pg_parse_json_or_errsave (lines 507-531) --- */
/*
 * pg_parse_json_or_errsave
 *
 * This function is like pg_parse_json, except that it does not return a
 * JsonParseErrorType. Instead, in case of any failure, this function will
 * save error data into *escontext if that's an ErrorSaveContext, otherwise
 * ereport(ERROR).
 *
 * Returns a boolean indicating success or failure (failure will only be
 * returned when escontext is an ErrorSaveContext).
 */
bool
pg_parse_json_or_errsave(JsonLexContext *lex, const JsonSemAction *sem,
						 Node *escontext)
{
	JsonParseErrorType result;

	result = pg_parse_json(lex, sem);
	if (result != JSON_SUCCESS)
	{
		json_errsave_error(result, lex, escontext);
		return false;
	}
	return true;
}

/* --- jsonfuncs.c makeJsonLexContext (lines 533-553) --- */
/*
 * makeJsonLexContext
 *
 * This is like makeJsonLexContextCstringLen, but it accepts a text value
 * directly.
 */
JsonLexContext *
makeJsonLexContext(JsonLexContext *lex, text *json, bool need_escapes)
{
	/*
	 * Most callers pass a detoasted datum, but it's not clear that they all
	 * do.  pg_detoast_datum_packed() is cheap insurance.
	 */
	json = pg_detoast_datum_packed(json);

	return makeJsonLexContextCstringLen(lex,
										VARDATA_ANY(json),
										VARSIZE_ANY_EXHDR(json),
										GetDatabaseEncoding(),
										need_escapes);
}

/* --- jsonfuncs.c json_errsave_error (lines 637-664) --- */
/*
 * Report a JSON error.
 */
void
json_errsave_error(JsonParseErrorType error, JsonLexContext *lex,
				   Node *escontext)
{
	if (error == JSON_UNICODE_HIGH_ESCAPE ||
		error == JSON_UNICODE_UNTRANSLATABLE ||
		error == JSON_UNICODE_CODE_POINT_ZERO)
		errsave(escontext,
				(errcode(ERRCODE_UNTRANSLATABLE_CHARACTER),
				 errmsg("unsupported Unicode escape sequence"),
				 errdetail_internal("%s", json_errdetail(error, lex)),
				 report_json_context(lex)));
	else if (error == JSON_SEM_ACTION_FAILED)
	{
		/* semantic action function had better have reported something */
		if (!SOFT_ERROR_OCCURRED(escontext))
			elog(ERROR, "JSON semantic action function did not provide error information");
	}
	else
		errsave(escontext,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for type %s", "json"),
				 errdetail_internal("%s", json_errdetail(error, lex)),
				 report_json_context(lex)));
}

/* --- jsonfuncs.c report_json_context (lines 666-728) --- */
/*
 * Report a CONTEXT line for bogus JSON input.
 *
 * lex->token_terminator must be set to identify the spot where we detected
 * the error.  Note that lex->token_start might be NULL, in case we recognized
 * error at EOF.
 *
 * The return value isn't meaningful, but we make it non-void so that this
 * can be invoked inside ereport().
 */
static int
report_json_context(JsonLexContext *lex)
{
	const char *context_start;
	const char *context_end;
	const char *line_start;
	char	   *ctxt;
	int			ctxtlen;
	const char *prefix;
	const char *suffix;

	/* Choose boundaries for the part of the input we will display */
	line_start = lex->line_start;
	context_start = line_start;
	context_end = lex->token_terminator;
	Assert(context_end >= context_start);

	/* Advance until we are close enough to context_end */
	while (context_end - context_start >= 50)
	{
		/* Advance to next multibyte character */
		if (IS_HIGHBIT_SET(*context_start))
			context_start += pg_mblen_range(context_start, context_end);
		else
			context_start++;
	}

	/*
	 * We add "..." to indicate that the excerpt doesn't start at the
	 * beginning of the line ... but if we're within 3 characters of the
	 * beginning of the line, we might as well just show the whole line.
	 */
	if (context_start - line_start <= 3)
		context_start = line_start;

	/* Get a null-terminated copy of the data to present */
	ctxtlen = context_end - context_start;
	ctxt = palloc(ctxtlen + 1);
	memcpy(ctxt, context_start, ctxtlen);
	ctxt[ctxtlen] = '\0';

	/*
	 * Show the context, prefixing "..." if not starting at start of line, and
	 * suffixing "..." if not ending at end of line.
	 */
	prefix = (context_start > line_start) ? "..." : "";
	suffix = (lex->token_type != JSON_TOKEN_END &&
			  context_end - lex->input < lex->input_length &&
			  *context_end != '\n' && *context_end != '\r') ? "..." : "";

	return errcontext("JSON data, line %d: %s%s%s",
					  lex->line_number, prefix, ctxt, suffix);
}

/* --- jsonfuncs.c jsonb_array_length (lines 1877-1892) --- */
Datum
jsonb_array_length(PG_FUNCTION_ARGS)
{
	Jsonb	   *jb = PG_GETARG_JSONB_P(0);

	if (JB_ROOT_IS_SCALAR(jb))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cannot get array length of a scalar")));
	else if (!JB_ROOT_IS_ARRAY(jb))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cannot get array length of a non-array")));

	PG_RETURN_INT32(JB_ROOT_COUNT(jb));
}

/* --- jsonfuncs.c jsonb_strip_nulls (lines 4536-4599) --- */
/*
 * SQL function jsonb_strip_nulls(jsonb, bool) -> jsonb
 */
Datum
jsonb_strip_nulls(PG_FUNCTION_ARGS)
{
	Jsonb	   *jb = PG_GETARG_JSONB_P(0);
	bool		strip_in_arrays = false;
	JsonbIterator *it;
	JsonbParseState *parseState = NULL;
	JsonbValue *res = NULL;
	JsonbValue	v,
				k;
	JsonbIteratorToken type;
	bool		last_was_key = false;

	if (PG_NARGS() == 2)
		strip_in_arrays = PG_GETARG_BOOL(1);

	if (JB_ROOT_IS_SCALAR(jb))
		PG_RETURN_POINTER(jb);

	it = JsonbIteratorInit(&jb->root);

	while ((type = JsonbIteratorNext(&it, &v, false)) != WJB_DONE)
	{
		Assert(!(type == WJB_KEY && last_was_key));

		if (type == WJB_KEY)
		{
			/* stash the key until we know if it has a null value */
			k = v;
			last_was_key = true;
			continue;
		}

		if (last_was_key)
		{
			/* if the last element was a key this one can't be */
			last_was_key = false;

			/* skip this field if value is null */
			if (type == WJB_VALUE && v.type == jbvNull)
				continue;

			/* otherwise, do a delayed push of the key */
			(void) pushJsonbValue(&parseState, WJB_KEY, &k);
		}

		/* if strip_in_arrays is set, also skip null array elements */
		if (strip_in_arrays)
			if (type == WJB_ELEM && v.type == jbvNull)
				continue;

		if (type == WJB_VALUE || type == WJB_ELEM)
			res = pushJsonbValue(&parseState, type, &v);
		else
			res = pushJsonbValue(&parseState, type, NULL);
	}

	Assert(res != NULL);

	PG_RETURN_POINTER(JsonbValueToJsonb(res));
}

/* --- jsonfuncs.c jsonb_pretty (lines 4601-4615) --- */
/*
 * SQL function jsonb_pretty (jsonb)
 *
 * Pretty-printed text for the jsonb
 */
Datum
jsonb_pretty(PG_FUNCTION_ARGS)
{
	Jsonb	   *jb = PG_GETARG_JSONB_P(0);
	StringInfo	str = makeStringInfo();

	JsonbToCStringIndent(str, &jb->root, VARSIZE(jb));

	PG_RETURN_TEXT_P(cstring_to_text_with_len(str->data, str->len));
}
