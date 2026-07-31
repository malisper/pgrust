/* SHIM utils/jsonfuncs.h — subset of declarations used by the pasted
 * jsonb.c / jsonfuncs.c segments; signatures + JsonTypeCategory enum
 * VERBATIM from src/include/utils/jsonfuncs.h @ 62d6c7d3df. */
#ifndef PG_JSONBFAM_SHIM_JSONFUNCS_H
#define PG_JSONBFAM_SHIM_JSONFUNCS_H
#include "postgres.h"
#include "common/jsonapi.h"
#include "utils/jsonb.h"

typedef enum
{
	JSONTYPE_NULL,				/* null, so we didn't bother to identify */
	JSONTYPE_BOOL,				/* boolean (built-in types only) */
	JSONTYPE_NUMERIC,			/* numeric (ditto) */
	JSONTYPE_DATE,				/* we use special formatting for datetimes */
	JSONTYPE_TIMESTAMP,
	JSONTYPE_TIMESTAMPTZ,
	JSONTYPE_JSON,				/* JSON (and JSONB, if not is_jsonb) */
	JSONTYPE_JSONB,				/* JSONB (if is_jsonb) */
	JSONTYPE_ARRAY,				/* array */
	JSONTYPE_COMPOSITE,			/* composite */
	JSONTYPE_CAST,				/* something with an explicit cast to JSON */
	JSONTYPE_OTHER,				/* all else */
} JsonTypeCategory;

extern JsonLexContext *makeJsonLexContext(JsonLexContext *lex, text *json,
										  bool need_escapes);
extern JsonLexContext *makeJsonLexContextCstringLen(JsonLexContext *lex,
													const char *json,
													size_t len,
													int encoding,
													bool need_escapes);
extern void pg_parse_json_or_ereport(JsonLexContext *lex, const JsonSemAction *sem);
extern bool pg_parse_json_or_errsave(JsonLexContext *lex, const JsonSemAction *sem,
									 struct Node *escontext);
extern void json_errsave_error(JsonParseErrorType error, JsonLexContext *lex,
							   struct Node *escontext);
#endif
