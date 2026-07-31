/* SHIM header for the jsonpath_diff oracle - NOT PostgreSQL code (plumbing only, never logic). */
#ifndef BUILTINS_H
#define BUILTINS_H
#include "fmgr.h"
#include "nodes/nodes.h"
#include "utils/fmgrprotos.h"
/* numutils.c entry vendored in pg_support_min.c */
extern int32 pg_strtoint32(const char *s);
extern int32 pg_strtoint32_safe(const char *s, Node *escontext);
#endif
