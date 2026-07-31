/* SHIM header for the jsonpathexec_diff oracle - NOT PostgreSQL code.
 * utils/jsonb.h includes utils/array.h in real PG; nothing the vendored
 * jsonb/jsonpath TUs compile actually uses ArrayType, so this is empty. */
#ifndef ARRAY_H
#define ARRAY_H
#include "utils/array_model.h"
#endif
