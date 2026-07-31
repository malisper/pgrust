/* SHIM header for the jsonpath_diff oracle - NOT PostgreSQL code (plumbing only, never logic). */
#ifndef BUILTINS_H
#define BUILTINS_H
#include "fmgr.h"
#include "utils/array.h"
#include "utils/varlena.h"
#include "nodes/nodes.h"
#include "utils/fmgrprotos.h"
/* numutils.c entry vendored in pg_support_min.c */
extern int32 pg_strtoint32(const char *s);
extern int32 pg_strtoint32_safe(const char *s, Node *escontext);
extern int64 pg_strtoint64(const char *s);
extern int64 pg_strtoint64_safe(const char *s, Node *escontext);
extern int	pg_ltoa(int32 value, char *a);
/* bool.c extract (pg_jsonb_min.c) */
extern bool parse_bool(const char *value, bool *result);
extern bool parse_bool_with_len(const char *value, size_t len, bool *result);
#endif
