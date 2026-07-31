#ifndef NV_NUMERIC_H
#define NV_NUMERIC_H

#define NUMERIC_MAX_PRECISION 1000
#define NUMERIC_MIN_SCALE (-1000)
#define NUMERIC_MAX_SCALE 1000
#define NUMERIC_MAX_DISPLAY_SCALE NUMERIC_MAX_PRECISION
#define NUMERIC_MIN_DISPLAY_SCALE 0
#define NUMERIC_MAX_RESULT_SCALE (NUMERIC_MAX_PRECISION * 2)
#define NUMERIC_MIN_SIG_DIGITS 16

struct NumericData;
typedef struct NumericData *Numeric;

typedef struct pg_prng_state
{
	uint64		s0;
	uint64		s1;
} pg_prng_state;

extern uint64 pg_prng_uint64_range(pg_prng_state *state, uint64 rmin, uint64 rmax);

#define NumericGetDatum(X) PointerGetDatum(X)
#define DatumGetNumeric(X) ((Numeric) PG_DETOAST_DATUM(X))
#define DatumGetNumericCopy(X) ((Numeric) PG_DETOAST_DATUM(X))
#define PG_GETARG_NUMERIC(n) DatumGetNumeric(PG_GETARG_DATUM(n))
#define PG_GETARG_NUMERIC_COPY(n) DatumGetNumericCopy(PG_GETARG_DATUM(n))
#define PG_RETURN_NUMERIC(x) return NumericGetDatum(x)

extern bool numeric_is_nan(Numeric num);
extern bool numeric_is_inf(Numeric num);
extern int32 numeric_maximum_size(int32 typmod);
extern char *numeric_out_sci(Numeric num, int scale);
extern char *numeric_normalize(Numeric num);
extern Numeric int64_to_numeric(int64 val);
extern Numeric int64_div_fast_to_numeric(int64 val1, int log10val2);
extern Numeric numeric_add_opt_error(Numeric num1, Numeric num2, bool *have_error);
extern Numeric numeric_sub_opt_error(Numeric num1, Numeric num2, bool *have_error);
extern Numeric numeric_mul_opt_error(Numeric num1, Numeric num2, bool *have_error);
extern Numeric numeric_div_opt_error(Numeric num1, Numeric num2, bool *have_error);
extern Numeric numeric_mod_opt_error(Numeric num1, Numeric num2, bool *have_error);
extern int32 numeric_int4_opt_error(Numeric num, bool *have_error);
extern int64 numeric_int8_opt_error(Numeric num, bool *have_error);
extern Numeric random_numeric(pg_prng_state *state, Numeric rmin, Numeric rmax);

extern Datum generate_series_step_numeric(FunctionCallInfo fcinfo);
#endif
