/* SHIM header for the jsonpath_diff oracle - NOT PostgreSQL code (plumbing only, never logic). */
#ifndef _PG_NUMERIC_H_
#define _PG_NUMERIC_H_
#include "postgres.h"
#include "fmgr.h"
/* constants + accessor shapes VERBATIM from utils/numeric.h @ 18.3 */
typedef struct NumericData *Numeric;
#define NUMERIC_MAX_PRECISION		1000
#define NUMERIC_MIN_SCALE			(-1000)
#define NUMERIC_MAX_SCALE			1000
#define NUMERIC_MAX_DISPLAY_SCALE	NUMERIC_MAX_PRECISION
#define NUMERIC_MIN_DISPLAY_SCALE	0
#define NUMERIC_MIN_SIG_DIGITS		16
static inline Numeric
DatumGetNumeric(Datum X)
{
	return (Numeric) PG_DETOAST_DATUM(X);
}
static inline Datum
NumericGetDatum(Numeric X)
{
	return PointerGetDatum(X);
}
#define PG_GETARG_NUMERIC(n)	  DatumGetNumeric(PG_GETARG_DATUM(n))
#define PG_RETURN_NUMERIC(x)	  return NumericGetDatum(x)
/* vendored verbatim in pg_numeric_min.c */
extern Numeric int64_to_numeric(int64 val);
extern int64 numeric_int8_safe(Numeric num, Node *escontext);
extern Numeric numeric_add_opt_error(Numeric num1, Numeric num2,
									 bool *have_error);
extern Numeric numeric_sub_opt_error(Numeric num1, Numeric num2,
									 bool *have_error);
extern Numeric numeric_mul_opt_error(Numeric num1, Numeric num2,
									 bool *have_error);
extern Numeric numeric_div_opt_error(Numeric num1, Numeric num2,
									 bool *have_error);
extern Numeric numeric_mod_opt_error(Numeric num1, Numeric num2,
									 bool *have_error);
extern int32 numeric_int4_opt_error(Numeric num, bool *have_error);
extern int64 numeric_int8_opt_error(Numeric num, bool *have_error);
extern bool numeric_is_nan(Numeric num);
extern bool numeric_is_inf(Numeric num);
#endif
