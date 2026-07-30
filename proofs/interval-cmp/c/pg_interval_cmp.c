/*
 * Vendored PostgreSQL C for the interval comparator family.
 *
 * Provenance:
 *   - src/backend/utils/adt/timestamp.c @ REL_18_STABLE (fetched 2026-07-28):
 *     interval_cmp_value, interval_cmp_internal, interval_eq, interval_ne,
 *     interval_lt, interval_gt, interval_le, interval_ge, interval_cmp,
 *     interval_smaller, interval_larger — bodies verbatim, pg_ prefix.
 *   - src/include/common/int128.h @ REL_18_STABLE (fetched 2026-07-28):
 *     the USE_NATIVE_INT128 arm (int64_to_int128,
 *     int128_add_int64_mul_int64, int128_compare) — bodies verbatim,
 *     pg_ prefix. This is the arm every production int128-capable compiler
 *     (gcc/clang on 64-bit) takes; CBMC models __int128 natively.
 *
 * Shims (plumbing only, never logic):
 *   - typedefs int64/int32/int128/INT128 and the Interval struct copied from
 *     c.h / timestamp.h / datatype/timestamp.h (int64 time; int32 day;
 *     int32 month — same member order/offsets as the pgrust Interval).
 *   - INT64CONST / USECS_PER_DAY defined locally (datatype/timestamp.h
 *     values).
 *   - fmgr unwrapping: PG_FUNCTION_ARGS / PG_GETARG_INTERVAL_P(n) replaced
 *     by plain C signatures taking the six raw fields (time/day/month x2);
 *     each shim wrapper builds local Interval values and passes their
 *     addresses, then PG_RETURN_BOOL/INT32 becomes `return` of int.
 *   - interval_smaller/interval_larger: C returns the WINNING INPUT POINTER
 *     (PG_RETURN_INTERVAL_P(result), no allocation). The shim reports the
 *     winning-arg INDEX (0 = first input, 1 = second input) computed from
 *     the verbatim `result = interval1/interval2` selection, so the harness
 *     can assert the shipped wrapper returned the matching input datum
 *     (datetime-cmp timetz_larger precedent).
 */

typedef signed char int8;
typedef short int16;
typedef int int32;
typedef long long int64;
typedef unsigned long long uint64;

/* int128.h: USE_NATIVE_INT128 arm */
typedef __int128 int128;
typedef int128 INT128;

#define INT64CONST(x) (x##LL)
#define USECS_PER_DAY INT64CONST(86400000000)

/* timestamp.h Interval — layout-identical to pgrust adt_datetime::Interval */
typedef struct pg_Interval
{
	int64		time;			/* all time units other than days, months and
								 * years */
	int32		day;			/* days, after time for alignment */
	int32		month;			/* months and years, after time for alignment */
} pg_Interval;

/*
 * Add the 128-bit product of two int64 values into an INT128 variable.
 * (int128.h verbatim)
 */
static inline void
pg_int128_add_int64_mul_int64(INT128 *i128, int64 x, int64 y)
{
	*i128 += (int128) x * (int128) y;
}

/*
 * Compare two INT128 values, return -1, 0, or +1.
 * (int128.h verbatim)
 */
static inline int
pg_int128_compare(INT128 x, INT128 y)
{
	if (x < y)
		return -1;
	if (x > y)
		return 1;
	return 0;
}

/*
 * Widen int64 to INT128.
 * (int128.h verbatim)
 */
static inline INT128
pg_int64_to_int128(int64 v)
{
	return (INT128) v;
}

/* timestamp.c verbatim */
static inline INT128
pg_interval_cmp_value(const pg_Interval *interval)
{
	INT128		span;
	int64		days;

	/*
	 * Combine the month and day fields into an integral number of days.
	 * Because the inputs are int32, int64 arithmetic suffices here.
	 */
	days = interval->month * INT64CONST(30);
	days += interval->day;

	/* Widen time field to 128 bits */
	span = pg_int64_to_int128(interval->time);

	/* Scale up days to microseconds, forming a 128-bit product */
	pg_int128_add_int64_mul_int64(&span, days, USECS_PER_DAY);

	return span;
}

/* timestamp.c verbatim */
static int
pg_interval_cmp_internal(const pg_Interval *interval1, const pg_Interval *interval2)
{
	INT128		span1 = pg_interval_cmp_value(interval1);
	INT128		span2 = pg_interval_cmp_value(interval2);

	return pg_int128_compare(span1, span2);
}

/*
 * fmgr shim wrappers: raw fields in, verbatim comparison bodies.
 */

int
pg_interval_eq(int64 t1, int32 d1, int32 m1, int64 t2, int32 d2, int32 m2)
{
	pg_Interval iv1 = {t1, d1, m1};
	pg_Interval iv2 = {t2, d2, m2};
	pg_Interval *interval1 = &iv1;
	pg_Interval *interval2 = &iv2;

	return pg_interval_cmp_internal(interval1, interval2) == 0;
}

int
pg_interval_ne(int64 t1, int32 d1, int32 m1, int64 t2, int32 d2, int32 m2)
{
	pg_Interval iv1 = {t1, d1, m1};
	pg_Interval iv2 = {t2, d2, m2};
	pg_Interval *interval1 = &iv1;
	pg_Interval *interval2 = &iv2;

	return pg_interval_cmp_internal(interval1, interval2) != 0;
}

int
pg_interval_lt(int64 t1, int32 d1, int32 m1, int64 t2, int32 d2, int32 m2)
{
	pg_Interval iv1 = {t1, d1, m1};
	pg_Interval iv2 = {t2, d2, m2};
	pg_Interval *interval1 = &iv1;
	pg_Interval *interval2 = &iv2;

	return pg_interval_cmp_internal(interval1, interval2) < 0;
}

int
pg_interval_gt(int64 t1, int32 d1, int32 m1, int64 t2, int32 d2, int32 m2)
{
	pg_Interval iv1 = {t1, d1, m1};
	pg_Interval iv2 = {t2, d2, m2};
	pg_Interval *interval1 = &iv1;
	pg_Interval *interval2 = &iv2;

	return pg_interval_cmp_internal(interval1, interval2) > 0;
}

int
pg_interval_le(int64 t1, int32 d1, int32 m1, int64 t2, int32 d2, int32 m2)
{
	pg_Interval iv1 = {t1, d1, m1};
	pg_Interval iv2 = {t2, d2, m2};
	pg_Interval *interval1 = &iv1;
	pg_Interval *interval2 = &iv2;

	return pg_interval_cmp_internal(interval1, interval2) <= 0;
}

int
pg_interval_ge(int64 t1, int32 d1, int32 m1, int64 t2, int32 d2, int32 m2)
{
	pg_Interval iv1 = {t1, d1, m1};
	pg_Interval iv2 = {t2, d2, m2};
	pg_Interval *interval1 = &iv1;
	pg_Interval *interval2 = &iv2;

	return pg_interval_cmp_internal(interval1, interval2) >= 0;
}

int
pg_interval_cmp(int64 t1, int32 d1, int32 m1, int64 t2, int32 d2, int32 m2)
{
	pg_Interval iv1 = {t1, d1, m1};
	pg_Interval iv2 = {t2, d2, m2};
	pg_Interval *interval1 = &iv1;
	pg_Interval *interval2 = &iv2;

	return pg_interval_cmp_internal(interval1, interval2);
}

/*
 * interval_smaller/interval_larger: verbatim winning-pointer selection,
 * reported as the winning-arg index (0 = interval1, 1 = interval2) — see
 * header shim notes.
 */
int
pg_interval_smaller(int64 t1, int32 d1, int32 m1, int64 t2, int32 d2, int32 m2)
{
	pg_Interval iv1 = {t1, d1, m1};
	pg_Interval iv2 = {t2, d2, m2};
	pg_Interval *interval1 = &iv1;
	pg_Interval *interval2 = &iv2;
	pg_Interval *result;

	/* use interval_cmp_internal to be sure this agrees with comparisons */
	if (pg_interval_cmp_internal(interval1, interval2) < 0)
		result = interval1;
	else
		result = interval2;
	return result == interval1 ? 0 : 1;
}

int
pg_interval_larger(int64 t1, int32 d1, int32 m1, int64 t2, int32 d2, int32 m2)
{
	pg_Interval iv1 = {t1, d1, m1};
	pg_Interval iv2 = {t2, d2, m2};
	pg_Interval *interval1 = &iv1;
	pg_Interval *interval2 = &iv2;
	pg_Interval *result;

	if (pg_interval_cmp_internal(interval1, interval2) > 0)
		result = interval1;
	else
		result = interval2;
	return result == interval1 ? 0 : 1;
}
