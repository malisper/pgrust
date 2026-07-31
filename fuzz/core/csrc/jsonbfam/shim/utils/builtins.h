/* SHIM utils/builtins.h — only the symbols the vendored segments use. */
#ifndef PG_JSONBFAM_SHIM_BUILTINS_H
#define PG_JSONBFAM_SHIM_BUILTINS_H
#include "postgres.h"
extern text *cstring_to_text(const char *s);
extern text *cstring_to_text_with_len(const char *s, int len);
extern char *text_to_cstring(const text *t);
/* utils/builtins.h TextDatumGetCString (verbatim) */
#define TextDatumGetCString(d) text_to_cstring((text *) DatumGetPointer(d))
/* float parse/render cores: verbatim in csrc/pg_float_io.c (shared) */
extern float4 float4in_internal(char *num, char **endptr_p,
								const char *type_name, const char *orig_string,
								struct Node *escontext);
extern float8 float8in_internal(char *num, char **endptr_p,
								const char *type_name, const char *orig_string,
								struct Node *escontext);
#endif
