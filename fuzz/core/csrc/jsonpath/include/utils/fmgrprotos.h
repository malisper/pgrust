/* SHIM header for the jsonpath_diff oracle - NOT PostgreSQL code (plumbing only, never logic). */
#ifndef FMGRPROTOS_H
#define FMGRPROTOS_H
#include "fmgr.h"
extern Datum numeric_in(FunctionCallInfo fcinfo);
extern Datum numeric_out(FunctionCallInfo fcinfo);
extern Datum numeric_uminus(FunctionCallInfo fcinfo);
#endif
