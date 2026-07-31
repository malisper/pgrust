/* SHIM header for the jsonpath_diff oracle - NOT PostgreSQL code (plumbing only, never logic). */
#ifndef _FORMATTING_H_
#define _FORMATTING_H_
#include "postgres.h"
extern bool datetime_format_has_tz(const char *fmt_str);
#endif
