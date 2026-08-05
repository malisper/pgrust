/* SHIM header for the jsonpathexec_diff oracle - NOT PostgreSQL code.
 * Declarations for the varlena.c extracts vendored in pg_jsonb_min.c. */
#ifndef VARLENA_H
#define VARLENA_H
#include "postgres.h"
extern int	varstr_cmp(const char *arg1, int len1, const char *arg2, int len2,
					   Oid collid);
extern text *cstring_to_text(const char *s);
extern text *cstring_to_text_with_len(const char *s, int len);
extern char *text_to_cstring(const text *t);
#endif
