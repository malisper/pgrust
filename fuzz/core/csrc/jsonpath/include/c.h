/* SHIM header for the jsonpathexec_diff oracle - NOT PostgreSQL code.
 * qsort_arg.c includes "c.h"; the shim postgres.h carries everything it
 * needs (types, Assert, Min/Max). */
#ifndef C_H
#define C_H
#include "postgres.h"

/* CppConcat VERBATIM from c.h @ 18.3 (sort_template.h name construction) */
#define CppConcat(x, y)			x##y
#endif
