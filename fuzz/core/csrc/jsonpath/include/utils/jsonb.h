/* SHIM header for the jsonpath_diff oracle - NOT PostgreSQL code (plumbing only, never logic). */
#ifndef __JSONB_H__
#define __JSONB_H__
#include "postgres.h"
#include "utils/numeric.h"
/* jbvType discriminants VERBATIM from utils/jsonb.h @ 18.3 (the first four
 * JsonPathItemType values alias them; on-disk format dependency) */
enum jbvType
{
	/* Scalar types */
	jbvNull = 0x0,
	jbvString,
	jbvNumeric,
	jbvBool,
	/* Composite types */
	jbvArray = 0x10,
	jbvObject,
	/* Binary (i.e. struct Jsonb) jbvArray/jbvObject */
	jbvBinary,
	jbvDatetime = 0x20,
};
typedef struct JsonbValue JsonbValue;	/* opaque here */
#endif
