/* SHIM header for the jsonpath_diff oracle - NOT PostgreSQL code (plumbing only, never logic). */
#ifndef JSON_H
#define JSON_H
#include "lib/stringinfo.h"
extern void escape_json(StringInfo buf, const char *str);
extern void escape_json_with_len(StringInfo buf, const char *str, int len);
#endif
