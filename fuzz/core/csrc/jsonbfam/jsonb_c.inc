/* VERBATIM segments from src/backend/utils/adt/jsonb.c @ 62d6c7d3df (18.3).
 * Head (includes/typedefs/decls) + io/parse/render core + build workers +
 * noargs wrappers + extract-scalar/cast family, extracted at
 * function-comment boundaries. datum_to_jsonb/typcache/agg family SKIPPED
 * (carve); add_jsonb/clone_parse_state/datum_to_jsonb_internal are
 * abort-shims in the driver TU. */
/*-------------------------------------------------------------------------
 *
 * jsonb.c
 *		I/O routines for jsonb type
 *
 * Copyright (c) 2014-2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/jsonb.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/json.h"
#include "utils/jsonb.h"
#include "utils/jsonfuncs.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"

typedef struct JsonbInState
{
	JsonbParseState *parseState;
	JsonbValue *res;
	bool		unique_keys;
	Node	   *escontext;
} JsonbInState;

typedef struct JsonbAggState
{
	JsonbInState *res;
	JsonTypeCategory key_category;
	Oid			key_output_func;
	JsonTypeCategory val_category;
	Oid			val_output_func;
} JsonbAggState;

static inline Datum jsonb_from_cstring(char *json, int len, bool unique_keys,
									   Node *escontext);
static bool checkStringLen(size_t len, Node *escontext);
static JsonParseErrorType jsonb_in_object_start(void *pstate);
static JsonParseErrorType jsonb_in_object_end(void *pstate);
static JsonParseErrorType jsonb_in_array_start(void *pstate);
static JsonParseErrorType jsonb_in_array_end(void *pstate);
static JsonParseErrorType jsonb_in_object_field_start(void *pstate, char *fname, bool isnull);
static void jsonb_put_escaped_value(StringInfo out, JsonbValue *scalarVal);
static JsonParseErrorType jsonb_in_scalar(void *pstate, char *token, JsonTokenType tokentype);
static void composite_to_jsonb(Datum composite, JsonbInState *result);
static void array_dim_to_jsonb(JsonbInState *result, int dim, int ndims, int *dims,
							   const Datum *vals, const bool *nulls, int *valcount,
							   JsonTypeCategory tcategory, Oid outfuncoid);
static void array_to_jsonb_internal(Datum array, JsonbInState *result);
static void datum_to_jsonb_internal(Datum val, bool is_null, JsonbInState *result,
									JsonTypeCategory tcategory, Oid outfuncoid,
									bool key_scalar);
static void add_jsonb(Datum val, bool is_null, JsonbInState *result,
					  Oid val_type, bool key_scalar);
static JsonbParseState *clone_parse_state(JsonbParseState *state);
static char *JsonbToCStringWorker(StringInfo out, JsonbContainer *in, int estimated_len, bool indent);
static void add_indent(StringInfo out, bool indent, int level);


/* --- jsonb.c jsonb_in (lines 69-78) --- */
/*
 * jsonb type input function
 */
Datum
jsonb_in(PG_FUNCTION_ARGS)
{
	char	   *json = PG_GETARG_CSTRING(0);

	return jsonb_from_cstring(json, strlen(json), false, fcinfo->context);
}

/* --- jsonb.c jsonb_recv (lines 80-102) --- */
/*
 * jsonb type recv function
 *
 * The type is sent as text in binary mode, so this is almost the same
 * as the input function, but it's prefixed with a version number so we
 * can change the binary format sent in future if necessary. For now,
 * only version 1 is supported.
 */
Datum
jsonb_recv(PG_FUNCTION_ARGS)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);
	int			version = pq_getmsgint(buf, 1);
	char	   *str;
	int			nbytes;

	if (version == 1)
		str = pq_getmsgtext(buf, buf->len - buf->cursor, &nbytes);
	else
		elog(ERROR, "unsupported jsonb version number %d", version);

	return jsonb_from_cstring(str, nbytes, false, NULL);
}

/* --- jsonb.c jsonb_out (lines 104-116) --- */
/*
 * jsonb type output function
 */
Datum
jsonb_out(PG_FUNCTION_ARGS)
{
	Jsonb	   *jb = PG_GETARG_JSONB_P(0);
	char	   *out;

	out = JsonbToCString(NULL, &jb->root, VARSIZE(jb));

	PG_RETURN_CSTRING(out);
}

/* --- jsonb.c jsonb_send (lines 118-139) --- */
/*
 * jsonb type send function
 *
 * Just send jsonb as a version number, then a string of text
 */
Datum
jsonb_send(PG_FUNCTION_ARGS)
{
	Jsonb	   *jb = PG_GETARG_JSONB_P(0);
	StringInfoData buf;
	StringInfo	jtext = makeStringInfo();
	int			version = 1;

	(void) JsonbToCString(jtext, &jb->root, VARSIZE(jb));

	pq_begintypsend(&buf);
	pq_sendint8(&buf, version);
	pq_sendtext(&buf, jtext->data, jtext->len);
	destroyStringInfo(jtext);

	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

/* --- jsonb.c jsonb_from_text (lines 141-153) --- */
/*
 * jsonb_from_text
 *
 * Turns json text string into a jsonb Datum.
 */
Datum
jsonb_from_text(text *js, bool unique_keys)
{
	return jsonb_from_cstring(VARDATA_ANY(js),
							  VARSIZE_ANY_EXHDR(js),
							  unique_keys,
							  NULL);
}

/* --- jsonb.c JsonbContainerTypeName (lines 155-174) --- */
/*
 * Get the type name of a jsonb container.
 */
static const char *
JsonbContainerTypeName(JsonbContainer *jbc)
{
	JsonbValue	scalar;

	if (JsonbExtractScalar(jbc, &scalar))
		return JsonbTypeName(&scalar);
	else if (JsonContainerIsArray(jbc))
		return "array";
	else if (JsonContainerIsObject(jbc))
		return "object";
	else
	{
		elog(ERROR, "invalid jsonb container type: 0x%08x", jbc->header);
		return "unknown";
	}
}

/* --- jsonb.c JsonbTypeName (lines 176-220) --- */
/*
 * Get the type name of a jsonb value.
 */
const char *
JsonbTypeName(JsonbValue *val)
{
	switch (val->type)
	{
		case jbvBinary:
			return JsonbContainerTypeName(val->val.binary.data);
		case jbvObject:
			return "object";
		case jbvArray:
			return "array";
		case jbvNumeric:
			return "number";
		case jbvString:
			return "string";
		case jbvBool:
			return "boolean";
		case jbvNull:
			return "null";
		case jbvDatetime:
			switch (val->val.datetime.typid)
			{
				case DATEOID:
					return "date";
				case TIMEOID:
					return "time without time zone";
				case TIMETZOID:
					return "time with time zone";
				case TIMESTAMPOID:
					return "timestamp without time zone";
				case TIMESTAMPTZOID:
					return "timestamp with time zone";
				default:
					elog(ERROR, "unrecognized jsonb value datetime type: %d",
						 val->val.datetime.typid);
			}
			return "unknown";
		default:
			elog(ERROR, "unrecognized jsonb value type: %d", val->type);
			return "unknown";
	}
}

/* --- jsonb.c jsonb_typeof (lines 222-235) --- */
/*
 * SQL function jsonb_typeof(jsonb) -> text
 *
 * This function is here because the analog json function is in json.c, since
 * it uses the json parser internals not exposed elsewhere.
 */
Datum
jsonb_typeof(PG_FUNCTION_ARGS)
{
	Jsonb	   *in = PG_GETARG_JSONB_P(0);
	const char *result = JsonbContainerTypeName(&in->root);

	PG_RETURN_TEXT_P(cstring_to_text(result));
}

/* --- jsonb.c jsonb_from_cstring (lines 237-274) --- */
/*
 * jsonb_from_cstring
 *
 * Turns json string into a jsonb Datum.
 *
 * Uses the json parser (with hooks) to construct a jsonb.
 *
 * If escontext points to an ErrorSaveContext, errors are reported there
 * instead of being thrown.
 */
static inline Datum
jsonb_from_cstring(char *json, int len, bool unique_keys, Node *escontext)
{
	JsonLexContext lex;
	JsonbInState state;
	JsonSemAction sem;

	memset(&state, 0, sizeof(state));
	memset(&sem, 0, sizeof(sem));
	makeJsonLexContextCstringLen(&lex, json, len, GetDatabaseEncoding(), true);

	state.unique_keys = unique_keys;
	state.escontext = escontext;
	sem.semstate = &state;

	sem.object_start = jsonb_in_object_start;
	sem.array_start = jsonb_in_array_start;
	sem.object_end = jsonb_in_object_end;
	sem.array_end = jsonb_in_array_end;
	sem.scalar = jsonb_in_scalar;
	sem.object_field_start = jsonb_in_object_field_start;

	if (!pg_parse_json_or_errsave(&lex, &sem, escontext))
		return (Datum) 0;

	/* after parsing, the item member has the composed jsonb structure */
	PG_RETURN_POINTER(JsonbValueToJsonb(state.res));
}

/* --- jsonb.c checkStringLen (lines 276-287) --- */
static bool
checkStringLen(size_t len, Node *escontext)
{
	if (len > JENTRY_OFFLENMASK)
		ereturn(escontext, false,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("string too long to represent as jsonb string"),
				 errdetail("Due to an implementation restriction, jsonb strings cannot exceed %d bytes.",
						   JENTRY_OFFLENMASK)));

	return true;
}

/* --- jsonb.c jsonb_in_object_start (lines 289-298) --- */
static JsonParseErrorType
jsonb_in_object_start(void *pstate)
{
	JsonbInState *_state = (JsonbInState *) pstate;

	_state->res = pushJsonbValue(&_state->parseState, WJB_BEGIN_OBJECT, NULL);
	_state->parseState->unique_keys = _state->unique_keys;

	return JSON_SUCCESS;
}

/* --- jsonb.c jsonb_in_object_end (lines 300-308) --- */
static JsonParseErrorType
jsonb_in_object_end(void *pstate)
{
	JsonbInState *_state = (JsonbInState *) pstate;

	_state->res = pushJsonbValue(&_state->parseState, WJB_END_OBJECT, NULL);

	return JSON_SUCCESS;
}

/* --- jsonb.c jsonb_in_array_start (lines 310-318) --- */
static JsonParseErrorType
jsonb_in_array_start(void *pstate)
{
	JsonbInState *_state = (JsonbInState *) pstate;

	_state->res = pushJsonbValue(&_state->parseState, WJB_BEGIN_ARRAY, NULL);

	return JSON_SUCCESS;
}

/* --- jsonb.c jsonb_in_array_end (lines 320-328) --- */
static JsonParseErrorType
jsonb_in_array_end(void *pstate)
{
	JsonbInState *_state = (JsonbInState *) pstate;

	_state->res = pushJsonbValue(&_state->parseState, WJB_END_ARRAY, NULL);

	return JSON_SUCCESS;
}

/* --- jsonb.c jsonb_in_object_field_start (lines 330-346) --- */
static JsonParseErrorType
jsonb_in_object_field_start(void *pstate, char *fname, bool isnull)
{
	JsonbInState *_state = (JsonbInState *) pstate;
	JsonbValue	v;

	Assert(fname != NULL);
	v.type = jbvString;
	v.val.string.len = strlen(fname);
	if (!checkStringLen(v.val.string.len, _state->escontext))
		return JSON_SEM_ACTION_FAILED;
	v.val.string.val = fname;

	_state->res = pushJsonbValue(&_state->parseState, WJB_KEY, &v);

	return JSON_SUCCESS;
}

/* --- jsonb.c jsonb_put_escaped_value (lines 348-373) --- */
static void
jsonb_put_escaped_value(StringInfo out, JsonbValue *scalarVal)
{
	switch (scalarVal->type)
	{
		case jbvNull:
			appendBinaryStringInfo(out, "null", 4);
			break;
		case jbvString:
			escape_json_with_len(out, scalarVal->val.string.val, scalarVal->val.string.len);
			break;
		case jbvNumeric:
			appendStringInfoString(out,
								   DatumGetCString(DirectFunctionCall1(numeric_out,
																	   PointerGetDatum(scalarVal->val.numeric))));
			break;
		case jbvBool:
			if (scalarVal->val.boolean)
				appendBinaryStringInfo(out, "true", 4);
			else
				appendBinaryStringInfo(out, "false", 5);
			break;
		default:
			elog(ERROR, "unknown jsonb scalar type");
	}
}

/* --- jsonb.c jsonb_in_scalar (lines 375-459) --- */
/*
 * For jsonb we always want the de-escaped value - that's what's in token
 */
static JsonParseErrorType
jsonb_in_scalar(void *pstate, char *token, JsonTokenType tokentype)
{
	JsonbInState *_state = (JsonbInState *) pstate;
	JsonbValue	v;
	Datum		numd;

	switch (tokentype)
	{

		case JSON_TOKEN_STRING:
			Assert(token != NULL);
			v.type = jbvString;
			v.val.string.len = strlen(token);
			if (!checkStringLen(v.val.string.len, _state->escontext))
				return JSON_SEM_ACTION_FAILED;
			v.val.string.val = token;
			break;
		case JSON_TOKEN_NUMBER:

			/*
			 * No need to check size of numeric values, because maximum
			 * numeric size is well below the JsonbValue restriction
			 */
			Assert(token != NULL);
			v.type = jbvNumeric;
			if (!DirectInputFunctionCallSafe(numeric_in, token,
											 InvalidOid, -1,
											 _state->escontext,
											 &numd))
				return JSON_SEM_ACTION_FAILED;
			v.val.numeric = DatumGetNumeric(numd);
			break;
		case JSON_TOKEN_TRUE:
			v.type = jbvBool;
			v.val.boolean = true;
			break;
		case JSON_TOKEN_FALSE:
			v.type = jbvBool;
			v.val.boolean = false;
			break;
		case JSON_TOKEN_NULL:
			v.type = jbvNull;
			break;
		default:
			/* should not be possible */
			elog(ERROR, "invalid json token type");
			break;
	}

	if (_state->parseState == NULL)
	{
		/* single scalar */
		JsonbValue	va;

		va.type = jbvArray;
		va.val.array.rawScalar = true;
		va.val.array.nElems = 1;

		_state->res = pushJsonbValue(&_state->parseState, WJB_BEGIN_ARRAY, &va);
		_state->res = pushJsonbValue(&_state->parseState, WJB_ELEM, &v);
		_state->res = pushJsonbValue(&_state->parseState, WJB_END_ARRAY, NULL);
	}
	else
	{
		JsonbValue *o = &_state->parseState->contVal;

		switch (o->type)
		{
			case jbvArray:
				_state->res = pushJsonbValue(&_state->parseState, WJB_ELEM, &v);
				break;
			case jbvObject:
				_state->res = pushJsonbValue(&_state->parseState, WJB_VALUE, &v);
				break;
			default:
				elog(ERROR, "unexpected parent of nested structure");
		}
	}

	return JSON_SUCCESS;
}

/* --- jsonb.c JsonbToCString (lines 461-476) --- */
/*
 * JsonbToCString
 *	   Converts jsonb value to a C-string.
 *
 * If 'out' argument is non-null, the resulting C-string is stored inside the
 * StringBuffer.  The resulting string is always returned.
 *
 * A typical case for passing the StringInfo in rather than NULL is where the
 * caller wants access to the len attribute without having to call strlen, e.g.
 * if they are converting it to a text* object.
 */
char *
JsonbToCString(StringInfo out, JsonbContainer *in, int estimated_len)
{
	return JsonbToCStringWorker(out, in, estimated_len, false);
}

/* --- jsonb.c JsonbToCStringIndent (lines 478-485) --- */
/*
 * same thing but with indentation turned on
 */
char *
JsonbToCStringIndent(StringInfo out, JsonbContainer *in, int estimated_len)
{
	return JsonbToCStringWorker(out, in, estimated_len, true);
}

/* --- jsonb.c JsonbToCStringWorker (lines 487-612) --- */
/*
 * common worker for above two functions
 */
static char *
JsonbToCStringWorker(StringInfo out, JsonbContainer *in, int estimated_len, bool indent)
{
	bool		first = true;
	JsonbIterator *it;
	JsonbValue	v;
	JsonbIteratorToken type = WJB_DONE;
	int			level = 0;
	bool		redo_switch = false;

	/* If we are indenting, don't add a space after a comma */
	int			ispaces = indent ? 1 : 2;

	/*
	 * Don't indent the very first item. This gets set to the indent flag at
	 * the bottom of the loop.
	 */
	bool		use_indent = false;
	bool		raw_scalar = false;
	bool		last_was_key = false;

	if (out == NULL)
		out = makeStringInfo();

	enlargeStringInfo(out, (estimated_len >= 0) ? estimated_len : 64);

	it = JsonbIteratorInit(in);

	while (redo_switch ||
		   ((type = JsonbIteratorNext(&it, &v, false)) != WJB_DONE))
	{
		redo_switch = false;
		switch (type)
		{
			case WJB_BEGIN_ARRAY:
				if (!first)
					appendBinaryStringInfo(out, ", ", ispaces);

				if (!v.val.array.rawScalar)
				{
					add_indent(out, use_indent && !last_was_key, level);
					appendStringInfoCharMacro(out, '[');
				}
				else
					raw_scalar = true;

				first = true;
				level++;
				break;
			case WJB_BEGIN_OBJECT:
				if (!first)
					appendBinaryStringInfo(out, ", ", ispaces);

				add_indent(out, use_indent && !last_was_key, level);
				appendStringInfoCharMacro(out, '{');

				first = true;
				level++;
				break;
			case WJB_KEY:
				if (!first)
					appendBinaryStringInfo(out, ", ", ispaces);
				first = true;

				add_indent(out, use_indent, level);

				/* json rules guarantee this is a string */
				jsonb_put_escaped_value(out, &v);
				appendBinaryStringInfo(out, ": ", 2);

				type = JsonbIteratorNext(&it, &v, false);
				if (type == WJB_VALUE)
				{
					first = false;
					jsonb_put_escaped_value(out, &v);
				}
				else
				{
					Assert(type == WJB_BEGIN_OBJECT || type == WJB_BEGIN_ARRAY);

					/*
					 * We need to rerun the current switch() since we need to
					 * output the object which we just got from the iterator
					 * before calling the iterator again.
					 */
					redo_switch = true;
				}
				break;
			case WJB_ELEM:
				if (!first)
					appendBinaryStringInfo(out, ", ", ispaces);
				first = false;

				if (!raw_scalar)
					add_indent(out, use_indent, level);
				jsonb_put_escaped_value(out, &v);
				break;
			case WJB_END_ARRAY:
				level--;
				if (!raw_scalar)
				{
					add_indent(out, use_indent, level);
					appendStringInfoCharMacro(out, ']');
				}
				first = false;
				break;
			case WJB_END_OBJECT:
				level--;
				add_indent(out, use_indent, level);
				appendStringInfoCharMacro(out, '}');
				first = false;
				break;
			default:
				elog(ERROR, "unknown jsonb iterator token type");
		}
		use_indent = indent;
		last_was_key = redo_switch;
	}

	Assert(level == 0);

	return out->data;
}

/* --- jsonb.c add_indent (lines 614-622) --- */
static void
add_indent(StringInfo out, bool indent, int level)
{
	if (indent)
	{
		appendStringInfoCharMacro(out, '\n');
		appendStringInfoSpaces(out, level * 4);
	}
}

/* --- jsonb.c jsonb_build_object_worker (lines 1124-1171) --- */
Datum
jsonb_build_object_worker(int nargs, const Datum *args, const bool *nulls, const Oid *types,
						  bool absent_on_null, bool unique_keys)
{
	int			i;
	JsonbInState result;

	if (nargs % 2 != 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("argument list must have even number of elements"),
		/* translator: %s is a SQL function name */
				 errhint("The arguments of %s must consist of alternating keys and values.",
						 "jsonb_build_object()")));

	memset(&result, 0, sizeof(JsonbInState));

	result.res = pushJsonbValue(&result.parseState, WJB_BEGIN_OBJECT, NULL);
	result.parseState->unique_keys = unique_keys;
	result.parseState->skip_nulls = absent_on_null;

	for (i = 0; i < nargs; i += 2)
	{
		/* process key */
		bool		skip;

		if (nulls[i])
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("argument %d: key must not be null", i + 1)));

		/* skip null values if absent_on_null */
		skip = absent_on_null && nulls[i + 1];

		/* we need to save skipped keys for the key uniqueness check */
		if (skip && !unique_keys)
			continue;

		add_jsonb(args[i], false, &result, types[i], true);

		/* process value */
		add_jsonb(args[i + 1], nulls[i + 1], &result, types[i + 1], false);
	}

	result.res = pushJsonbValue(&result.parseState, WJB_END_OBJECT, NULL);

	return JsonbPGetDatum(JsonbValueToJsonb(result.res));
}

/* --- jsonb.c jsonb_build_object_noargs (lines 1193-1207) --- */
/*
 * degenerate case of jsonb_build_object where it gets 0 arguments.
 */
Datum
jsonb_build_object_noargs(PG_FUNCTION_ARGS)
{
	JsonbInState result;

	memset(&result, 0, sizeof(JsonbInState));

	(void) pushJsonbValue(&result.parseState, WJB_BEGIN_OBJECT, NULL);
	result.res = pushJsonbValue(&result.parseState, WJB_END_OBJECT, NULL);

	PG_RETURN_POINTER(JsonbValueToJsonb(result.res));
}

/* --- jsonb.c jsonb_build_array_worker (lines 1209-1231) --- */
Datum
jsonb_build_array_worker(int nargs, const Datum *args, const bool *nulls, const Oid *types,
						 bool absent_on_null)
{
	int			i;
	JsonbInState result;

	memset(&result, 0, sizeof(JsonbInState));

	result.res = pushJsonbValue(&result.parseState, WJB_BEGIN_ARRAY, NULL);

	for (i = 0; i < nargs; i++)
	{
		if (absent_on_null && nulls[i])
			continue;

		add_jsonb(args[i], nulls[i], &result, types[i], false);
	}

	result.res = pushJsonbValue(&result.parseState, WJB_END_ARRAY, NULL);

	return JsonbPGetDatum(JsonbValueToJsonb(result.res));
}

/* --- jsonb.c jsonb_build_array_noargs (lines 1254-1268) --- */
/*
 * degenerate case of jsonb_build_array where it gets 0 arguments.
 */
Datum
jsonb_build_array_noargs(PG_FUNCTION_ARGS)
{
	JsonbInState result;

	memset(&result, 0, sizeof(JsonbInState));

	(void) pushJsonbValue(&result.parseState, WJB_BEGIN_ARRAY, NULL);
	result.res = pushJsonbValue(&result.parseState, WJB_END_ARRAY, NULL);

	PG_RETURN_POINTER(JsonbValueToJsonb(result.res));
}

/* --- jsonb.c JsonbExtractScalar (lines 1964-2002) --- */
/*
 * Extract scalar value from raw-scalar pseudo-array jsonb.
 */
bool
JsonbExtractScalar(JsonbContainer *jbc, JsonbValue *res)
{
	JsonbIterator *it;
	JsonbIteratorToken tok PG_USED_FOR_ASSERTS_ONLY;
	JsonbValue	tmp;

	if (!JsonContainerIsArray(jbc) || !JsonContainerIsScalar(jbc))
	{
		/* inform caller about actual type of container */
		res->type = (JsonContainerIsArray(jbc)) ? jbvArray : jbvObject;
		return false;
	}

	/*
	 * A root scalar is stored as an array of one element, so we get the array
	 * and then its first (and only) member.
	 */
	it = JsonbIteratorInit(jbc);

	tok = JsonbIteratorNext(&it, &tmp, true);
	Assert(tok == WJB_BEGIN_ARRAY);
	Assert(tmp.val.array.nElems == 1 && tmp.val.array.rawScalar);

	tok = JsonbIteratorNext(&it, res, true);
	Assert(tok == WJB_ELEM);
	Assert(IsAJsonbScalar(res));

	tok = JsonbIteratorNext(&it, &tmp, true);
	Assert(tok == WJB_END_ARRAY);

	tok = JsonbIteratorNext(&it, &tmp, true);
	Assert(tok == WJB_DONE);

	return true;
}

/* --- jsonb.c cannotCastJsonbValue (lines 2004-2035) --- */
/*
 * Emit correct, translatable cast error message
 */
static void
cannotCastJsonbValue(enum jbvType type, const char *sqltype)
{
	static const struct
	{
		enum jbvType type;
		const char *msg;
	}
				messages[] =
	{
		{jbvNull, gettext_noop("cannot cast jsonb null to type %s")},
		{jbvString, gettext_noop("cannot cast jsonb string to type %s")},
		{jbvNumeric, gettext_noop("cannot cast jsonb numeric to type %s")},
		{jbvBool, gettext_noop("cannot cast jsonb boolean to type %s")},
		{jbvArray, gettext_noop("cannot cast jsonb array to type %s")},
		{jbvObject, gettext_noop("cannot cast jsonb object to type %s")},
		{jbvBinary, gettext_noop("cannot cast jsonb array or object to type %s")}
	};
	int			i;

	for (i = 0; i < lengthof(messages); i++)
		if (messages[i].type == type)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg(messages[i].msg, sqltype)));

	/* should be unreachable */
	elog(ERROR, "unknown jsonb type: %d", (int) type);
}

/* --- jsonb.c jsonb_bool (lines 2037-2058) --- */
Datum
jsonb_bool(PG_FUNCTION_ARGS)
{
	Jsonb	   *in = PG_GETARG_JSONB_P(0);
	JsonbValue	v;

	if (!JsonbExtractScalar(&in->root, &v))
		cannotCastJsonbValue(v.type, "boolean");

	if (v.type == jbvNull)
	{
		PG_FREE_IF_COPY(in, 0);
		PG_RETURN_NULL();
	}

	if (v.type != jbvBool)
		cannotCastJsonbValue(v.type, "boolean");

	PG_FREE_IF_COPY(in, 0);

	PG_RETURN_BOOL(v.val.boolean);
}

/* --- jsonb.c jsonb_numeric (lines 2060-2088) --- */
Datum
jsonb_numeric(PG_FUNCTION_ARGS)
{
	Jsonb	   *in = PG_GETARG_JSONB_P(0);
	JsonbValue	v;
	Numeric		retValue;

	if (!JsonbExtractScalar(&in->root, &v))
		cannotCastJsonbValue(v.type, "numeric");

	if (v.type == jbvNull)
	{
		PG_FREE_IF_COPY(in, 0);
		PG_RETURN_NULL();
	}

	if (v.type != jbvNumeric)
		cannotCastJsonbValue(v.type, "numeric");

	/*
	 * v.val.numeric points into jsonb body, so we need to make a copy to
	 * return
	 */
	retValue = DatumGetNumericCopy(NumericGetDatum(v.val.numeric));

	PG_FREE_IF_COPY(in, 0);

	PG_RETURN_NUMERIC(retValue);
}

/* --- jsonb.c jsonb_int2 (lines 2090-2115) --- */
Datum
jsonb_int2(PG_FUNCTION_ARGS)
{
	Jsonb	   *in = PG_GETARG_JSONB_P(0);
	JsonbValue	v;
	Datum		retValue;

	if (!JsonbExtractScalar(&in->root, &v))
		cannotCastJsonbValue(v.type, "smallint");

	if (v.type == jbvNull)
	{
		PG_FREE_IF_COPY(in, 0);
		PG_RETURN_NULL();
	}

	if (v.type != jbvNumeric)
		cannotCastJsonbValue(v.type, "smallint");

	retValue = DirectFunctionCall1(numeric_int2,
								   NumericGetDatum(v.val.numeric));

	PG_FREE_IF_COPY(in, 0);

	PG_RETURN_DATUM(retValue);
}

/* --- jsonb.c jsonb_int4 (lines 2117-2142) --- */
Datum
jsonb_int4(PG_FUNCTION_ARGS)
{
	Jsonb	   *in = PG_GETARG_JSONB_P(0);
	JsonbValue	v;
	Datum		retValue;

	if (!JsonbExtractScalar(&in->root, &v))
		cannotCastJsonbValue(v.type, "integer");

	if (v.type == jbvNull)
	{
		PG_FREE_IF_COPY(in, 0);
		PG_RETURN_NULL();
	}

	if (v.type != jbvNumeric)
		cannotCastJsonbValue(v.type, "integer");

	retValue = DirectFunctionCall1(numeric_int4,
								   NumericGetDatum(v.val.numeric));

	PG_FREE_IF_COPY(in, 0);

	PG_RETURN_DATUM(retValue);
}

/* --- jsonb.c jsonb_int8 (lines 2144-2169) --- */
Datum
jsonb_int8(PG_FUNCTION_ARGS)
{
	Jsonb	   *in = PG_GETARG_JSONB_P(0);
	JsonbValue	v;
	Datum		retValue;

	if (!JsonbExtractScalar(&in->root, &v))
		cannotCastJsonbValue(v.type, "bigint");

	if (v.type == jbvNull)
	{
		PG_FREE_IF_COPY(in, 0);
		PG_RETURN_NULL();
	}

	if (v.type != jbvNumeric)
		cannotCastJsonbValue(v.type, "bigint");

	retValue = DirectFunctionCall1(numeric_int8,
								   NumericGetDatum(v.val.numeric));

	PG_FREE_IF_COPY(in, 0);

	PG_RETURN_DATUM(retValue);
}

/* --- jsonb.c jsonb_float4 (lines 2171-2196) --- */
Datum
jsonb_float4(PG_FUNCTION_ARGS)
{
	Jsonb	   *in = PG_GETARG_JSONB_P(0);
	JsonbValue	v;
	Datum		retValue;

	if (!JsonbExtractScalar(&in->root, &v))
		cannotCastJsonbValue(v.type, "real");

	if (v.type == jbvNull)
	{
		PG_FREE_IF_COPY(in, 0);
		PG_RETURN_NULL();
	}

	if (v.type != jbvNumeric)
		cannotCastJsonbValue(v.type, "real");

	retValue = DirectFunctionCall1(numeric_float4,
								   NumericGetDatum(v.val.numeric));

	PG_FREE_IF_COPY(in, 0);

	PG_RETURN_DATUM(retValue);
}

/* --- jsonb.c jsonb_float8 (lines 2198-2223) --- */
Datum
jsonb_float8(PG_FUNCTION_ARGS)
{
	Jsonb	   *in = PG_GETARG_JSONB_P(0);
	JsonbValue	v;
	Datum		retValue;

	if (!JsonbExtractScalar(&in->root, &v))
		cannotCastJsonbValue(v.type, "double precision");

	if (v.type == jbvNull)
	{
		PG_FREE_IF_COPY(in, 0);
		PG_RETURN_NULL();
	}

	if (v.type != jbvNumeric)
		cannotCastJsonbValue(v.type, "double precision");

	retValue = DirectFunctionCall1(numeric_float8,
								   NumericGetDatum(v.val.numeric));

	PG_FREE_IF_COPY(in, 0);

	PG_RETURN_DATUM(retValue);
}
