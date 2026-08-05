/* SHIM header for the jsonpath_diff oracle - NOT PostgreSQL code (plumbing only, never logic). */
#ifndef VALUE_H
#define VALUE_H
#include "nodes/nodes.h"
/* String node shape VERBATIM from nodes/value.h @ 18.3 */
typedef struct String
{
	NodeTag		type;
	char	   *sval;
} String;
extern String *makeString(char *str);
#endif
