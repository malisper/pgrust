/* SHIM header for the jsonpath_diff oracle - NOT PostgreSQL code (plumbing only, never logic). */
#ifndef _FORMATTING_H_
#define _FORMATTING_H_
#include "postgres.h"
extern bool datetime_format_has_tz(const char *fmt_str);
/* DATETIME CARVE sentinel (loud abort stub in pg_jsonpath_exec_env.c) */
extern Datum parse_datetime(text *date_txt, text *fmt, Oid collid, bool strict,
							Oid *typid, int32 *typmod, int *tz,
							struct Node *escontext);
#endif
