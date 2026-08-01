/*
 * SHIM utils/float.h for the contribb_diff oracle: extern decls resolved
 * against the VERBATIM vendored definitions in csrc/pg_float_io.c (one
 * verbatim definition per symbol across the whole fuzz oracle build).
 * float8out_internal is macro-routed through the arena-copying wrapper in
 * pg_contribb_io.c (pg_cb_f8out) so cube_out's per-coordinate 32-byte
 * palloc doesn't leak across millions of execs; the wrapper calls the
 * verbatim function and frees the malloc'd original (plumbing only).
 */
#ifndef PG_CB_UTILS_FLOAT_H
#define PG_CB_UTILS_FLOAT_H

#include "postgres.h"

extern float4 float4in_internal(char *num, char **endptr_p,
								const char *type_name,
								const char *orig_string,
								struct Node *escontext);
extern float8 float8in_internal(char *num, char **endptr_p,
								const char *type_name,
								const char *orig_string,
								struct Node *escontext);
extern char *float8out_internal(double num);

extern char *pg_cb_f8out(double num);

#define float8out_internal(num) pg_cb_f8out(num)

#endif							/* PG_CB_UTILS_FLOAT_H */
