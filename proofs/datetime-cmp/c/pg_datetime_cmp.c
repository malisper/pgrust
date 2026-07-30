/*
 * Vendored PostgreSQL C for the datetime-cmp proof family.
 *
 * Provenance:
 *   - src/backend/utils/adt/date.c       (date_*, time_*, timetz_* comparators
 *                                         + timetz_cmp_internal)
 *   - src/backend/utils/adt/timestamp.c  (timestamp_* comparators
 *                                         + timestamp_cmp_internal)
 *   ref: postgres/postgres master @ 239eabda41e39de73c376000ba74bbeb8fe32a5c
 *   fetched: 2026-07-28
 *
 * Master has integer timestamps only (float-timestamp ifdefs were removed in
 * PG 10); there is no float branch to take.
 *
 * REL_18_STABLE conformance (audit 2026-07-28, proofs/PROVENANCE-AUDIT.md):
 * identical to REL_18_STABLE except a characterized value-equivalent drift
 * in 4 date.c functions (date2timestamp[tz] + the cross-type cmp internals
 * of the 2026-07-28 EXTENSION below): the vendored text carries master's
 * escontext/ereturn error-channel shape, where REL_18 uses an
 * `int *overflow` out-parameter. Comparator RESULTS are proven
 * value-equivalent (overflow=+1 <=> error+NOEND, etc.); only the
 * error-transport protocol differs. Same pattern as pg_lsn's drift witness.
 *
 * Shims (plumbing only, never logic):
 *   - `pg_` prefix on every function name.
 *   - fmgr PG_FUNCTION_ARGS unwrapping -> plain C signatures:
 *       PG_GETARG_DATEADT/TIMEADT/TIMESTAMP -> direct int32/int64 params;
 *       PG_GETARG_TIMETZADT_P -> (int64 time, int32 zone) pairs, packed into
 *       a local TimeTzADT so timetz_cmp_internal's body stays verbatim.
 *       PG_RETURN_BOOL -> `return <expr>;` with int return type (Kani lowers
 *       Rust bool-compat via int shim); PG_RETURN_INT32 -> `return <expr>;`.
 *   - typedefs/macros normally supplied by headers: DateADT, TimeADT,
 *     Timestamp, TimeOffset, TimeTzADT, int32/int64, USECS_PER_SEC,
 *     INT64CONST.
 *
 * Function bodies between the arg-fetch lines and the returns are verbatim.
 * Postgres compiles with -fwrapv; CBMC's two's-complement wrap matches
 * (relevant to timetz_cmp_internal's t1/t2 additions on out-of-range input).
 *
 * EXTENSION 2026-07-28 (dt-minmax): larger/smaller rows + date<->timestamp[tz]
 * cross-type comparators, same ref. Additional shims:
 *   - PG_RETURN_DATEADT/TIMEADT/TIMESTAMP -> plain int32/int64 returns.
 *   - timetz_larger/smaller return the WINNING INPUT POINTER in C
 *     (PG_RETURN_TIMETZADT_P); shimmed to return the winning arg INDEX
 *     (0 or 1) so the harness can check the shipped wrapper returned the
 *     matching input datum.
 *   - ErrorSaveContext -> a plain { int error_occurred; } struct;
 *     ereturn(escontext, result, ...) -> set the flag and return result
 *     (message text leaves the proof; the returned sentinel and the
 *     error_occurred verdict stay in). The escontext-NULL hard-error arm is
 *     unreachable here: every vendored caller passes a local escontext,
 *     exactly as in C.
 *   - Assert() compiled out, as in a release build.
 *   - date2timestamptz_safe: the j2date + DetermineTimeZoneOffset(tm,
 *     session_timezone) seam is replaced by the shared model offset
 *     `pg_model_tz_offset` (set by the harness; the Rust side stubs its
 *     identical seam - adt_datetime::tz::DetermineTimeZoneOffset on the
 *     session timezone - to the same value). The tm fields j2date would
 *     compute feed ONLY that seam, so they are dead under the model.
 *     Everything else in the function is verbatim.
 *   - macros from datetime.h/timestamp.h with their literal values:
 *     POSTGRES_EPOCH_JDATE, TIMESTAMP_END_JULIAN, USECS_PER_DAY,
 *     DATEVAL_NOBEGIN/NOEND, DT_NOBEGIN/NOEND, MIN_TIMESTAMP, END_TIMESTAMP,
 *     IS_VALID_TIMESTAMP, DATE_IS_NOBEGIN/NOEND, TIMESTAMP_NOBEGIN/NOEND,
 *     TIMESTAMP_IS_NOBEGIN/NOEND, and timestamptz_cmp_internal ==
 *     timestamp_cmp_internal (both int64 microseconds).
 */

#include <stdint.h>

typedef int32_t int32;
typedef int64_t int64;
#define INT64CONST(x) INT64_C(x)

typedef int32 DateADT;
typedef int64 TimeADT;
typedef int64 Timestamp;
typedef int64 TimeOffset;

typedef struct
{
	TimeADT		time;			/* all time units other than time zone */
	int32		zone;			/* numeric time zone, in seconds */
} TimeTzADT;

#define USECS_PER_SEC	INT64CONST(1000000)

/* ---------- date.c: date comparators ---------- */

int
pg_date_eq(DateADT dateVal1, DateADT dateVal2)
{
	return (dateVal1 == dateVal2);
}

int
pg_date_ne(DateADT dateVal1, DateADT dateVal2)
{
	return (dateVal1 != dateVal2);
}

int
pg_date_lt(DateADT dateVal1, DateADT dateVal2)
{
	return (dateVal1 < dateVal2);
}

int
pg_date_le(DateADT dateVal1, DateADT dateVal2)
{
	return (dateVal1 <= dateVal2);
}

int
pg_date_gt(DateADT dateVal1, DateADT dateVal2)
{
	return (dateVal1 > dateVal2);
}

int
pg_date_ge(DateADT dateVal1, DateADT dateVal2)
{
	return (dateVal1 >= dateVal2);
}

int
pg_date_cmp(DateADT dateVal1, DateADT dateVal2)
{
	if (dateVal1 < dateVal2)
		return -1;
	else if (dateVal1 > dateVal2)
		return 1;
	return 0;
}

/* ---------- date.c: time comparators ---------- */

int
pg_time_eq(TimeADT time1, TimeADT time2)
{
	return (time1 == time2);
}

int
pg_time_ne(TimeADT time1, TimeADT time2)
{
	return (time1 != time2);
}

int
pg_time_lt(TimeADT time1, TimeADT time2)
{
	return (time1 < time2);
}

int
pg_time_le(TimeADT time1, TimeADT time2)
{
	return (time1 <= time2);
}

int
pg_time_gt(TimeADT time1, TimeADT time2)
{
	return (time1 > time2);
}

int
pg_time_ge(TimeADT time1, TimeADT time2)
{
	return (time1 >= time2);
}

int
pg_time_cmp(TimeADT time1, TimeADT time2)
{
	if (time1 < time2)
		return -1;
	if (time1 > time2)
		return 1;
	return 0;
}

/* ---------- date.c: timetz_cmp_internal + timetz comparators ---------- */

static int
pg_timetz_cmp_internal(TimeTzADT *time1, TimeTzADT *time2)
{
	TimeOffset	t1,
				t2;

	/* Primary sort is by true (GMT-equivalent) time */
	t1 = time1->time + (time1->zone * USECS_PER_SEC);
	t2 = time2->time + (time2->zone * USECS_PER_SEC);

	if (t1 > t2)
		return 1;
	if (t1 < t2)
		return -1;

	/*
	 * If same GMT time, sort by timezone; we only want to say that two
	 * timetz's are equal if both the time and zone parts are equal.
	 */
	if (time1->zone > time2->zone)
		return 1;
	if (time1->zone < time2->zone)
		return -1;

	return 0;
}

/* shim: (time, zone) pairs stand in for PG_GETARG_TIMETZADT_P pointers */

int
pg_timetz_eq(int64 t1, int32 z1, int64 t2, int32 z2)
{
	TimeTzADT	time1 = {t1, z1};
	TimeTzADT	time2 = {t2, z2};

	return (pg_timetz_cmp_internal(&time1, &time2) == 0);
}

int
pg_timetz_ne(int64 t1, int32 z1, int64 t2, int32 z2)
{
	TimeTzADT	time1 = {t1, z1};
	TimeTzADT	time2 = {t2, z2};

	return (pg_timetz_cmp_internal(&time1, &time2) != 0);
}

int
pg_timetz_lt(int64 t1, int32 z1, int64 t2, int32 z2)
{
	TimeTzADT	time1 = {t1, z1};
	TimeTzADT	time2 = {t2, z2};

	return (pg_timetz_cmp_internal(&time1, &time2) < 0);
}

int
pg_timetz_le(int64 t1, int32 z1, int64 t2, int32 z2)
{
	TimeTzADT	time1 = {t1, z1};
	TimeTzADT	time2 = {t2, z2};

	return (pg_timetz_cmp_internal(&time1, &time2) <= 0);
}

int
pg_timetz_gt(int64 t1, int32 z1, int64 t2, int32 z2)
{
	TimeTzADT	time1 = {t1, z1};
	TimeTzADT	time2 = {t2, z2};

	return (pg_timetz_cmp_internal(&time1, &time2) > 0);
}

int
pg_timetz_ge(int64 t1, int32 z1, int64 t2, int32 z2)
{
	TimeTzADT	time1 = {t1, z1};
	TimeTzADT	time2 = {t2, z2};

	return (pg_timetz_cmp_internal(&time1, &time2) >= 0);
}

int
pg_timetz_cmp(int64 t1, int32 z1, int64 t2, int32 z2)
{
	TimeTzADT	time1 = {t1, z1};
	TimeTzADT	time2 = {t2, z2};

	return pg_timetz_cmp_internal(&time1, &time2);
}

/* ---------- timestamp.c: timestamp_cmp_internal + comparators ----------
 * pg_proc maps the timestamptz comparator rows (oids 1152-1157, 2045) onto
 * these same C functions; Timestamp and TimestampTz are both int64 micros.
 */

int
pg_timestamp_cmp_internal(Timestamp dt1, Timestamp dt2)
{
	return (dt1 < dt2) ? -1 : ((dt1 > dt2) ? 1 : 0);
}

int
pg_timestamp_eq(Timestamp dt1, Timestamp dt2)
{
	return (pg_timestamp_cmp_internal(dt1, dt2) == 0);
}

int
pg_timestamp_ne(Timestamp dt1, Timestamp dt2)
{
	return (pg_timestamp_cmp_internal(dt1, dt2) != 0);
}

int
pg_timestamp_lt(Timestamp dt1, Timestamp dt2)
{
	return (pg_timestamp_cmp_internal(dt1, dt2) < 0);
}

int
pg_timestamp_gt(Timestamp dt1, Timestamp dt2)
{
	return (pg_timestamp_cmp_internal(dt1, dt2) > 0);
}

int
pg_timestamp_le(Timestamp dt1, Timestamp dt2)
{
	return (pg_timestamp_cmp_internal(dt1, dt2) <= 0);
}

int
pg_timestamp_ge(Timestamp dt1, Timestamp dt2)
{
	return (pg_timestamp_cmp_internal(dt1, dt2) >= 0);
}

int
pg_timestamp_cmp(Timestamp dt1, Timestamp dt2)
{
	return pg_timestamp_cmp_internal(dt1, dt2);
}

/* ==================== dt-minmax extension ==================== */

#define PG_INT32_MIN	(-0x7FFFFFFF-1)
#define PG_INT32_MAX	(0x7FFFFFFF)
#define PG_INT64_MIN	(-INT64CONST(0x7FFFFFFFFFFFFFFF) - 1)
#define PG_INT64_MAX	INT64CONST(0x7FFFFFFFFFFFFFFF)

typedef int64 TimestampTz;

#define USECS_PER_DAY	INT64CONST(86400000000)

#define POSTGRES_EPOCH_JDATE	2451545 /* == date2j(2000, 1, 1) */
#define TIMESTAMP_END_JULIAN	109203528	/* == date2j(294277, 1, 1) */

#define DATEVAL_NOBEGIN		((DateADT) PG_INT32_MIN)
#define DATEVAL_NOEND		((DateADT) PG_INT32_MAX)
#define DATE_IS_NOBEGIN(j)	((j) == DATEVAL_NOBEGIN)
#define DATE_IS_NOEND(j)	((j) == DATEVAL_NOEND)

#define DT_NOBEGIN		PG_INT64_MIN
#define DT_NOEND		PG_INT64_MAX
#define TIMESTAMP_NOBEGIN(j)	do {(j) = DT_NOBEGIN;} while (0)
#define TIMESTAMP_IS_NOBEGIN(j) ((j) == DT_NOBEGIN)
#define TIMESTAMP_NOEND(j)		do {(j) = DT_NOEND;} while (0)
#define TIMESTAMP_IS_NOEND(j)	((j) == DT_NOEND)

#define MIN_TIMESTAMP	INT64CONST(-211813488000000000)
#define END_TIMESTAMP	INT64CONST(9223371331200000000)
#define IS_VALID_TIMESTAMP(t)  (MIN_TIMESTAMP <= (t) && (t) < END_TIMESTAMP)

/* shim: ErrorSaveContext -> plain error flag; Assert compiled out (release) */
typedef struct
{
	int			error_occurred;
} ErrorSaveContext;

#define Assert(x)

/* shim: ereturn -> record soft error + return; errcode/errmsg text dropped.
 * escontext is non-NULL at every vendored call site (as in C). */
#define ereturn(escontext, result) \
	do { (escontext)->error_occurred = 1; return result; } while (0)

/* timestamp.h: TimestampTz shares Timestamp's comparator */
#define pg_timestamptz_cmp_internal(dt1, dt2) pg_timestamp_cmp_internal(dt1, dt2)

/* ---------- date.c / timestamp.c: larger/smaller ---------- */

int32
pg_date_larger(DateADT dateVal1, DateADT dateVal2)
{
	return (dateVal1 > dateVal2) ? dateVal1 : dateVal2;
}

int32
pg_date_smaller(DateADT dateVal1, DateADT dateVal2)
{
	return (dateVal1 < dateVal2) ? dateVal1 : dateVal2;
}

int64
pg_time_larger(TimeADT time1, TimeADT time2)
{
	return (time1 > time2) ? time1 : time2;
}

int64
pg_time_smaller(TimeADT time1, TimeADT time2)
{
	return (time1 < time2) ? time1 : time2;
}

/* shim: return winning arg index (0/1) instead of the winning pointer */
int
pg_timetz_larger(int64 t1, int32 z1, int64 t2, int32 z2)
{
	TimeTzADT	time1 = {t1, z1};
	TimeTzADT	time2 = {t2, z2};
	TimeTzADT  *result;

	if (pg_timetz_cmp_internal(&time1, &time2) > 0)
		result = &time1;
	else
		result = &time2;
	return (result == &time1) ? 0 : 1;
}

int
pg_timetz_smaller(int64 t1, int32 z1, int64 t2, int32 z2)
{
	TimeTzADT	time1 = {t1, z1};
	TimeTzADT	time2 = {t2, z2};
	TimeTzADT  *result;

	if (pg_timetz_cmp_internal(&time1, &time2) < 0)
		result = &time1;
	else
		result = &time2;
	return (result == &time1) ? 0 : 1;
}

int64
pg_timestamp_smaller(Timestamp dt1, Timestamp dt2)
{
	Timestamp	result;

	/* use timestamp_cmp_internal to be sure this agrees with comparisons */
	if (pg_timestamp_cmp_internal(dt1, dt2) < 0)
		result = dt1;
	else
		result = dt2;
	return result;
}

int64
pg_timestamp_larger(Timestamp dt1, Timestamp dt2)
{
	Timestamp	result;

	if (pg_timestamp_cmp_internal(dt1, dt2) > 0)
		result = dt1;
	else
		result = dt2;
	return result;
}

/* ---------- date.c: date -> timestamp[tz] promotion (soft-error form) ---- */

static Timestamp
pg_date2timestamp_safe(DateADT dateVal, ErrorSaveContext *escontext)
{
	Timestamp	result;

	if (DATE_IS_NOBEGIN(dateVal))
		TIMESTAMP_NOBEGIN(result);
	else if (DATE_IS_NOEND(dateVal))
		TIMESTAMP_NOEND(result);
	else
	{
		/*
		 * Since dates have the same minimum values as timestamps, only upper
		 * boundary need be checked for overflow.
		 */
		if (dateVal >= (TIMESTAMP_END_JULIAN - POSTGRES_EPOCH_JDATE))
		{
			TIMESTAMP_NOEND(result);
			ereturn(escontext, result);
		}

		/* date is days since 2000, timestamp is microseconds since same... */
		result = dateVal * USECS_PER_DAY;
	}

	return result;
}

/* SEAM MODEL: stands in for j2date + DetermineTimeZoneOffset(tm,
 * session_timezone); harness sets it, Rust stubs its identical seam to the
 * same value. */
int32		pg_model_tz_offset = 0;

static TimestampTz
pg_date2timestamptz_safe(DateADT dateVal, ErrorSaveContext *escontext)
{
	TimestampTz result;
	int			tz;

	if (DATE_IS_NOBEGIN(dateVal))
		TIMESTAMP_NOBEGIN(result);
	else if (DATE_IS_NOEND(dateVal))
		TIMESTAMP_NOEND(result);
	else
	{
		/*
		 * Since dates have the same minimum values as timestamps, only upper
		 * boundary need be checked for overflow.
		 */
		if (dateVal >= (TIMESTAMP_END_JULIAN - POSTGRES_EPOCH_JDATE))
		{
			TIMESTAMP_NOEND(result);
			ereturn(escontext, result);
		}

		/* shim (seam model): replaces
		 *   j2date(dateVal + POSTGRES_EPOCH_JDATE, &tm->tm_year, ...);
		 *   tm->tm_hour = tm->tm_min = tm->tm_sec = 0;
		 *   tz = DetermineTimeZoneOffset(tm, session_timezone);
		 */
		tz = pg_model_tz_offset;

		result = dateVal * USECS_PER_DAY + tz * USECS_PER_SEC;

		/*
		 * Since it is possible to go beyond allowed timestamptz range because
		 * of time zone, check for allowed timestamp range after adding tz.
		 */
		if (!IS_VALID_TIMESTAMP(result))
		{
			if (result < MIN_TIMESTAMP)
				TIMESTAMP_NOBEGIN(result);
			else
				TIMESTAMP_NOEND(result);

			ereturn(escontext, result);
		}
	}

	return result;
}

/* ---------- date.c: crosstype comparison internals ---------- */

static int32
pg_date_cmp_timestamp_internal(DateADT dateVal, Timestamp dt2)
{
	Timestamp	dt1;
	ErrorSaveContext escontext = {0};

	dt1 = pg_date2timestamp_safe(dateVal, &escontext);
	if (escontext.error_occurred)
	{
		Assert(TIMESTAMP_IS_NOEND(dt1));	/* NOBEGIN case cannot occur */

		/* dt1 is larger than any finite timestamp, but less than infinity */
		return TIMESTAMP_IS_NOEND(dt2) ? -1 : +1;
	}

	return pg_timestamp_cmp_internal(dt1, dt2);
}

static int32
pg_date_cmp_timestamptz_internal(DateADT dateVal, TimestampTz dt2)
{
	TimestampTz dt1;
	ErrorSaveContext escontext = {0};

	dt1 = pg_date2timestamptz_safe(dateVal, &escontext);

	if (escontext.error_occurred)
	{
		if (TIMESTAMP_IS_NOEND(dt1))
		{
			/* dt1 is larger than any finite timestamp, but less than infinity */
			return TIMESTAMP_IS_NOEND(dt2) ? -1 : +1;
		}
		if (TIMESTAMP_IS_NOBEGIN(dt1))
		{
			/* dt1 is less than any finite timestamp, but more than -infinity */
			return TIMESTAMP_IS_NOBEGIN(dt2) ? +1 : -1;
		}
	}

	return pg_timestamptz_cmp_internal(dt1, dt2);
}

/* ---------- date.c: date vs timestamp entry points ---------- */

int
pg_date_eq_timestamp(DateADT dateVal, Timestamp dt2)
{
	return (pg_date_cmp_timestamp_internal(dateVal, dt2) == 0);
}

int
pg_date_ne_timestamp(DateADT dateVal, Timestamp dt2)
{
	return (pg_date_cmp_timestamp_internal(dateVal, dt2) != 0);
}

int
pg_date_lt_timestamp(DateADT dateVal, Timestamp dt2)
{
	return (pg_date_cmp_timestamp_internal(dateVal, dt2) < 0);
}

int
pg_date_gt_timestamp(DateADT dateVal, Timestamp dt2)
{
	return (pg_date_cmp_timestamp_internal(dateVal, dt2) > 0);
}

int
pg_date_le_timestamp(DateADT dateVal, Timestamp dt2)
{
	return (pg_date_cmp_timestamp_internal(dateVal, dt2) <= 0);
}

int
pg_date_ge_timestamp(DateADT dateVal, Timestamp dt2)
{
	return (pg_date_cmp_timestamp_internal(dateVal, dt2) >= 0);
}

int
pg_date_cmp_timestamp(DateADT dateVal, Timestamp dt2)
{
	return pg_date_cmp_timestamp_internal(dateVal, dt2);
}

/* ---------- date.c: date vs timestamptz entry points ---------- */

int
pg_date_eq_timestamptz(DateADT dateVal, TimestampTz dt2)
{
	return (pg_date_cmp_timestamptz_internal(dateVal, dt2) == 0);
}

int
pg_date_ne_timestamptz(DateADT dateVal, TimestampTz dt2)
{
	return (pg_date_cmp_timestamptz_internal(dateVal, dt2) != 0);
}

int
pg_date_lt_timestamptz(DateADT dateVal, TimestampTz dt2)
{
	return (pg_date_cmp_timestamptz_internal(dateVal, dt2) < 0);
}

int
pg_date_gt_timestamptz(DateADT dateVal, TimestampTz dt2)
{
	return (pg_date_cmp_timestamptz_internal(dateVal, dt2) > 0);
}

int
pg_date_le_timestamptz(DateADT dateVal, TimestampTz dt2)
{
	return (pg_date_cmp_timestamptz_internal(dateVal, dt2) <= 0);
}

int
pg_date_ge_timestamptz(DateADT dateVal, TimestampTz dt2)
{
	return (pg_date_cmp_timestamptz_internal(dateVal, dt2) >= 0);
}

int
pg_date_cmp_timestamptz(DateADT dateVal, TimestampTz dt2)
{
	return pg_date_cmp_timestamptz_internal(dateVal, dt2);
}

/* ---------- date.c: timestamp vs date entry points ---------- */

int
pg_timestamp_eq_date(Timestamp dt1, DateADT dateVal)
{
	return (pg_date_cmp_timestamp_internal(dateVal, dt1) == 0);
}

int
pg_timestamp_ne_date(Timestamp dt1, DateADT dateVal)
{
	return (pg_date_cmp_timestamp_internal(dateVal, dt1) != 0);
}

int
pg_timestamp_lt_date(Timestamp dt1, DateADT dateVal)
{
	return (pg_date_cmp_timestamp_internal(dateVal, dt1) > 0);
}

int
pg_timestamp_gt_date(Timestamp dt1, DateADT dateVal)
{
	return (pg_date_cmp_timestamp_internal(dateVal, dt1) < 0);
}

int
pg_timestamp_le_date(Timestamp dt1, DateADT dateVal)
{
	return (pg_date_cmp_timestamp_internal(dateVal, dt1) >= 0);
}

int
pg_timestamp_ge_date(Timestamp dt1, DateADT dateVal)
{
	return (pg_date_cmp_timestamp_internal(dateVal, dt1) <= 0);
}

int
pg_timestamp_cmp_date(Timestamp dt1, DateADT dateVal)
{
	return -pg_date_cmp_timestamp_internal(dateVal, dt1);
}

/* ---------- date.c: timestamptz vs date entry points ---------- */

int
pg_timestamptz_eq_date(TimestampTz dt1, DateADT dateVal)
{
	return (pg_date_cmp_timestamptz_internal(dateVal, dt1) == 0);
}

int
pg_timestamptz_ne_date(TimestampTz dt1, DateADT dateVal)
{
	return (pg_date_cmp_timestamptz_internal(dateVal, dt1) != 0);
}

int
pg_timestamptz_lt_date(TimestampTz dt1, DateADT dateVal)
{
	return (pg_date_cmp_timestamptz_internal(dateVal, dt1) > 0);
}

int
pg_timestamptz_gt_date(TimestampTz dt1, DateADT dateVal)
{
	return (pg_date_cmp_timestamptz_internal(dateVal, dt1) < 0);
}

int
pg_timestamptz_le_date(TimestampTz dt1, DateADT dateVal)
{
	return (pg_date_cmp_timestamptz_internal(dateVal, dt1) >= 0);
}

int
pg_timestamptz_ge_date(TimestampTz dt1, DateADT dateVal)
{
	return (pg_date_cmp_timestamptz_internal(dateVal, dt1) <= 0);
}

int
pg_timestamptz_cmp_date(TimestampTz dt1, DateADT dateVal)
{
	return -pg_date_cmp_timestamptz_internal(dateVal, dt1);
}

/* ---------- timestamp.c: timestamp vs timestamptz cross-type ----------
 * (pg_proc oids 2520-2533), vendored from REL_18_STABLE
 * src/backend/utils/adt/timestamp.c (fetched 2026-07-28).
 *
 * SHIMS (comparison/selection expressions verbatim):
 *  - timestamp2timestamptz_opt_overflow's timestamp2tm +
 *    DetermineTimeZoneOffset block -> the SAME shared tz-seam model as
 *    pg_date2timestamptz_safe above (tz = pg_model_tz_offset; the Rust
 *    side stubs timestamp2tm to a fixed tm — whose only consumer,
 *    DetermineTimeZoneOffset, is itself stubbed to the model — so the
 *    modeled seam is the whole decompose->offset block on both sides).
 *    timestamp2tm's pro-forma failure arm (-> ereport) is ALWAYS-SUCCESS
 *    under the model on both sides: that arm is out of proof.
 *  - dt2local kept verbatim (int64 wrap is -fwrapv-defined; the Rust side
 *    uses wrapping_sub).
 *  - fmgr wrappers -> plain signatures, PG_RETURN_BOOL -> int.
 */

#define TIMESTAMP_NOT_FINITE(j) (TIMESTAMP_IS_NOBEGIN(j) || TIMESTAMP_IS_NOEND(j))

static Timestamp
pg_dt2local(Timestamp dt, int tz)
{
	dt -= (tz * USECS_PER_SEC);
	return dt;
}

static TimestampTz
pg_timestamp2timestamptz_opt_overflow(Timestamp timestamp, int *overflow)
{
	TimestampTz result;
	int			tz;

	if (overflow)
		*overflow = 0;

	if (TIMESTAMP_NOT_FINITE(timestamp))
		return timestamp;

	/* shim (seam model): replaces
	 *   if (timestamp2tm(timestamp, NULL, tm, &fsec, NULL, NULL) == 0)
	 *       tz = DetermineTimeZoneOffset(tm, session_timezone);
	 * always-success arm; the pro-forma failure arm (-> ereport) is out of
	 * proof. */
	{
		tz = pg_model_tz_offset;

		result = pg_dt2local(timestamp, -tz);

		if (IS_VALID_TIMESTAMP(result))
		{
			return result;
		}
		else if (overflow)
		{
			if (result < MIN_TIMESTAMP)
			{
				*overflow = -1;
				TIMESTAMP_NOBEGIN(result);
			}
			else
			{
				*overflow = 1;
				TIMESTAMP_NOEND(result);
			}
			return result;
		}
	}

	/* unreachable with a non-NULL overflow pointer (callers below always
	 * pass one); kept as a sentinel */
	return 0;
}

static int32
pg_timestamp_cmp_timestamptz_internal(Timestamp timestampVal, TimestampTz dt2)
{
	TimestampTz dt1;
	int			overflow;

	dt1 = pg_timestamp2timestamptz_opt_overflow(timestampVal, &overflow);
	if (overflow > 0)
	{
		/* dt1 is larger than any finite timestamp, but less than infinity */
		return TIMESTAMP_IS_NOEND(dt2) ? -1 : +1;
	}
	if (overflow < 0)
	{
		/* dt1 is less than any finite timestamp, but more than -infinity */
		return TIMESTAMP_IS_NOBEGIN(dt2) ? +1 : -1;
	}

	return pg_timestamp_cmp_internal(dt1, dt2);
}

int
pg_timestamp_eq_timestamptz(Timestamp timestampVal, TimestampTz dt2)
{
	return pg_timestamp_cmp_timestamptz_internal(timestampVal, dt2) == 0;
}

int
pg_timestamp_ne_timestamptz(Timestamp timestampVal, TimestampTz dt2)
{
	return pg_timestamp_cmp_timestamptz_internal(timestampVal, dt2) != 0;
}

int
pg_timestamp_lt_timestamptz(Timestamp timestampVal, TimestampTz dt2)
{
	return pg_timestamp_cmp_timestamptz_internal(timestampVal, dt2) < 0;
}

int
pg_timestamp_gt_timestamptz(Timestamp timestampVal, TimestampTz dt2)
{
	return pg_timestamp_cmp_timestamptz_internal(timestampVal, dt2) > 0;
}

int
pg_timestamp_le_timestamptz(Timestamp timestampVal, TimestampTz dt2)
{
	return pg_timestamp_cmp_timestamptz_internal(timestampVal, dt2) <= 0;
}

int
pg_timestamp_ge_timestamptz(Timestamp timestampVal, TimestampTz dt2)
{
	return pg_timestamp_cmp_timestamptz_internal(timestampVal, dt2) >= 0;
}

int32
pg_timestamp_cmp_timestamptz(Timestamp timestampVal, TimestampTz dt2)
{
	return pg_timestamp_cmp_timestamptz_internal(timestampVal, dt2);
}

int
pg_timestamptz_eq_timestamp(TimestampTz dt1, Timestamp timestampVal)
{
	return pg_timestamp_cmp_timestamptz_internal(timestampVal, dt1) == 0;
}

int
pg_timestamptz_ne_timestamp(TimestampTz dt1, Timestamp timestampVal)
{
	return pg_timestamp_cmp_timestamptz_internal(timestampVal, dt1) != 0;
}

int
pg_timestamptz_lt_timestamp(TimestampTz dt1, Timestamp timestampVal)
{
	return pg_timestamp_cmp_timestamptz_internal(timestampVal, dt1) > 0;
}

int
pg_timestamptz_gt_timestamp(TimestampTz dt1, Timestamp timestampVal)
{
	return pg_timestamp_cmp_timestamptz_internal(timestampVal, dt1) < 0;
}

int
pg_timestamptz_le_timestamp(TimestampTz dt1, Timestamp timestampVal)
{
	return pg_timestamp_cmp_timestamptz_internal(timestampVal, dt1) >= 0;
}

int
pg_timestamptz_ge_timestamp(TimestampTz dt1, Timestamp timestampVal)
{
	return pg_timestamp_cmp_timestamptz_internal(timestampVal, dt1) <= 0;
}

int32
pg_timestamptz_cmp_timestamp(TimestampTz dt1, Timestamp timestampVal)
{
	return -pg_timestamp_cmp_timestamptz_internal(timestampVal, dt1);
}

/* ==================== wave-6 arithmetic extension ====================
 *
 * Provenance:
 *   - src/backend/utils/adt/date.c       (date_pli, date_mii,
 *                                         time_pl_interval, time_mi_interval,
 *                                         timetz_pl_interval, timetz_mi_interval)
 *   - src/backend/utils/adt/timestamp.c  (interval_um_internal, interval_um,
 *                                         finite_interval_pl, interval_pl,
 *                                         finite_interval_mi, interval_mi,
 *                                         interval_justify_interval,
 *                                         interval_justify_hours,
 *                                         interval_justify_days, timestamp_mi,
 *                                         timestamp_pl_interval,
 *                                         timestamp_mi_interval)
 *   ref: postgres/postgres REL_18_STABLE, fetched 2026-07-28
 *   (raw.githubusercontent.com/postgres/postgres/REL_18_STABLE/...)
 *
 * Additional shims (plumbing only, never logic):
 *   - Interval rides as (int64 time, int32 day, int32 month) scalar triples
 *     packed into local Interval structs (PG_GETARG_INTERVAL_P), and results
 *     ride out through (int64 *rt, int32 *rd, int32 *rm) out-params
 *     (palloc(sizeof(Interval)) -> local struct; PG_RETURN_INTERVAL_P ->
 *     write out-params).  TimeTzADT likewise as (int64 time, int32 zone).
 *   - Every shimmed function RETURNS an int error flag per the
 *     PROOF_EREPORT_FLAG convention: ereport(ERROR, ...) -> `return 1;` at
 *     the exact program point (message text leaves the proof; the verdict
 *     stays in).  Bodies between arg-fetch and returns are otherwise
 *     verbatim.
 *   - pg_add/sub_s32/s64_overflow: common/int.h's __builtin arm verbatim
 *     (the only arm compiled on production toolchains; cash precedent).
 *   - timestamp_pl_interval's month!=0 / day!=0 blocks call
 *     timestamp2tm/date2j/j2date (the /146097 divider-chain wall, out of
 *     this proof's plane).  They are replaced by a LOUD out-of-plane trap
 *     `return 99` -- harnesses drive only span.month == 0 && span.day == 0
 *     (as literals) or infinity sentinels, where the blocks are dead in C
 *     AND in the shipped Rust; if a harness ever reached them the flag
 *     mismatch fails the proof rather than passing vacuously.
 *   - DirectFunctionCall1(interval_justify_hours, ...) in timestamp_mi ->
 *     direct call of the shimmed justify-hours body on the local result
 *     (same abort-on-error semantics via the flag).
 *   - macros with their literal REL_18 values: DAYS_PER_MONTH, TMODULO,
 *     INTERVAL_NOBEGIN/NOEND(+IS_/NOT_FINITE), IS_VALID_DATE bounds.
 */

#define likely(x) (x)
#define unlikely(x) (x)

typedef struct
{
	TimeOffset	time;			/* all time units other than days, months and
								 * years */
	int32		day;			/* days, after time for alignment */
	int32		month;			/* months and years, after time for alignment */
} Interval;

#define DAYS_PER_MONTH	30		/* assumes exactly 30 days per month */
#define MONTHS_PER_YEAR 12

/* datetime.h TMODULO(): int64 division truncates toward zero (C99) */
#define TMODULO(t,q,u) \
do { \
	(q) = ((t) / (u)); \
	if ((q) != 0) (t) -= ((q) * (u)); \
} while(0)

/* datatype/timestamp.h: infinite intervals = all fields min/max */
#define INTERVAL_NOBEGIN(i)	\
	do {	\
		(i)->time = PG_INT64_MIN;	\
		(i)->day = PG_INT32_MIN;	\
		(i)->month = PG_INT32_MIN;	\
	} while (0)

#define INTERVAL_IS_NOBEGIN(i)	\
	((i)->month == PG_INT32_MIN && (i)->day == PG_INT32_MIN && (i)->time == PG_INT64_MIN)

#define INTERVAL_NOEND(i)	\
	do {	\
		(i)->time = PG_INT64_MAX;	\
		(i)->day = PG_INT32_MAX;	\
		(i)->month = PG_INT32_MAX;	\
	} while (0)

#define INTERVAL_IS_NOEND(i)	\
	((i)->month == PG_INT32_MAX && (i)->day == PG_INT32_MAX && (i)->time == PG_INT64_MAX)

#define INTERVAL_NOT_FINITE(i) (INTERVAL_IS_NOBEGIN(i) || INTERVAL_IS_NOEND(i))

#define DATE_NOT_FINITE(j)	(DATE_IS_NOBEGIN(j) || DATE_IS_NOEND(j))

#define DATETIME_MIN_JULIAN (0)
#define DATE_END_JULIAN (2147483494)	/* == date2j(JULIAN_MAXYEAR, 1, 1) */
#define IS_VALID_DATE(d) \
	((DATETIME_MIN_JULIAN - POSTGRES_EPOCH_JDATE) <= (d) && \
	 (d) < (DATE_END_JULIAN - POSTGRES_EPOCH_JDATE))

/* common/int.h, __builtin arm (verbatim) */
static inline int
pg_add_s32_overflow(int32 a, int32 b, int32 *result)
{
	return __builtin_add_overflow(a, b, result);
}

static inline int
pg_sub_s32_overflow(int32 a, int32 b, int32 *result)
{
	return __builtin_sub_overflow(a, b, result);
}

static inline int
pg_add_s64_overflow(int64 a, int64 b, int64 *result)
{
	return __builtin_add_overflow(a, b, result);
}

static inline int
pg_sub_s64_overflow(int64 a, int64 b, int64 *result)
{
	return __builtin_sub_overflow(a, b, result);
}

/* ---------- date.c: date_pli / date_mii ---------- */

int
pg_date_pli(DateADT dateVal, int32 days, int32 *presult)
{
	DateADT		result;

	if (DATE_NOT_FINITE(dateVal))
	{
		*presult = dateVal;		/* can't change infinity */
		return 0;
	}

	result = dateVal + days;

	/* Check for integer overflow and out-of-allowed-range */
	if ((days >= 0 ? (result < dateVal) : (result > dateVal)) ||
		!IS_VALID_DATE(result))
		return 1;				/* ereport(ERROR, DATETIME_VALUE_OUT_OF_RANGE,
								 * "date out of range") */

	*presult = result;
	return 0;
}

int
pg_date_mii(DateADT dateVal, int32 days, int32 *presult)
{
	DateADT		result;

	if (DATE_NOT_FINITE(dateVal))
	{
		*presult = dateVal;		/* can't change infinity */
		return 0;
	}

	result = dateVal - days;

	/* Check for integer overflow and out-of-allowed-range */
	if ((days >= 0 ? (result > dateVal) : (result < dateVal)) ||
		!IS_VALID_DATE(result))
		return 1;

	*presult = result;
	return 0;
}

/* ---------- date.c: time +- interval ---------- */

int
pg_time_pl_interval(TimeADT time, int64 st, int32 sd, int32 sm, TimeADT *presult)
{
	Interval	span_ = {st, sd, sm};
	Interval   *span = &span_;
	TimeADT		result;

	if (INTERVAL_NOT_FINITE(span))
		return 1;				/* cannot add infinite interval to time */

	result = time + span->time;
	result -= result / USECS_PER_DAY * USECS_PER_DAY;
	if (result < INT64CONST(0))
		result += USECS_PER_DAY;

	*presult = result;
	return 0;
}

int
pg_time_mi_interval(TimeADT time, int64 st, int32 sd, int32 sm, TimeADT *presult)
{
	Interval	span_ = {st, sd, sm};
	Interval   *span = &span_;
	TimeADT		result;

	if (INTERVAL_NOT_FINITE(span))
		return 1;				/* cannot subtract infinite interval from time */

	result = time - span->time;
	result -= result / USECS_PER_DAY * USECS_PER_DAY;
	if (result < INT64CONST(0))
		result += USECS_PER_DAY;

	*presult = result;
	return 0;
}

/* ---------- date.c: timetz +- interval ---------- */

int
pg_timetz_pl_interval(int64 tt, int32 tz, int64 st, int32 sd, int32 sm,
					  int64 *rt, int32 *rz)
{
	TimeTzADT	time_ = {tt, tz};
	TimeTzADT  *time = &time_;
	Interval	span_ = {st, sd, sm};
	Interval   *span = &span_;
	TimeTzADT	result_;
	TimeTzADT  *result = &result_;	/* palloc(sizeof(TimeTzADT)) -> local */

	if (INTERVAL_NOT_FINITE(span))
		return 1;				/* cannot add infinite interval to time */

	result->time = time->time + span->time;
	result->time -= result->time / USECS_PER_DAY * USECS_PER_DAY;
	if (result->time < INT64CONST(0))
		result->time += USECS_PER_DAY;

	result->zone = time->zone;

	*rt = result->time;
	*rz = result->zone;
	return 0;
}

int
pg_timetz_mi_interval(int64 tt, int32 tz, int64 st, int32 sd, int32 sm,
					  int64 *rt, int32 *rz)
{
	TimeTzADT	time_ = {tt, tz};
	TimeTzADT  *time = &time_;
	Interval	span_ = {st, sd, sm};
	Interval   *span = &span_;
	TimeTzADT	result_;
	TimeTzADT  *result = &result_;

	if (INTERVAL_NOT_FINITE(span))
		return 1;				/* cannot subtract infinite interval from time */

	result->time = time->time - span->time;
	result->time -= result->time / USECS_PER_DAY * USECS_PER_DAY;
	if (result->time < INT64CONST(0))
		result->time += USECS_PER_DAY;

	result->zone = time->zone;

	*rt = result->time;
	*rz = result->zone;
	return 0;
}

/* ---------- timestamp.c: interval negation / addition / subtraction ---------- */

/* verbatim body; ereport -> return 1 through the caller's flag */
static int
pg_interval_um_internal_c(const Interval *interval, Interval *result)
{
	if (INTERVAL_IS_NOBEGIN(interval))
		INTERVAL_NOEND(result);
	else if (INTERVAL_IS_NOEND(interval))
		INTERVAL_NOBEGIN(result);
	else
	{
		/* Negate each field, guarding against overflow */
		if (pg_sub_s64_overflow(INT64CONST(0), interval->time, &result->time) ||
			pg_sub_s32_overflow(0, interval->day, &result->day) ||
			pg_sub_s32_overflow(0, interval->month, &result->month) ||
			INTERVAL_NOT_FINITE(result))
			return 1;			/* interval out of range */
	}
	return 0;
}

int
pg_interval_um(int64 t, int32 d, int32 m, int64 *rt, int32 *rd, int32 *rm)
{
	Interval	interval_ = {t, d, m};
	Interval	result;

	if (pg_interval_um_internal_c(&interval_, &result))
		return 1;

	*rt = result.time;
	*rd = result.day;
	*rm = result.month;
	return 0;
}

static int
pg_finite_interval_pl(const Interval *span1, const Interval *span2, Interval *result)
{
	Assert(!INTERVAL_NOT_FINITE(span1));
	Assert(!INTERVAL_NOT_FINITE(span2));

	if (pg_add_s32_overflow(span1->month, span2->month, &result->month) ||
		pg_add_s32_overflow(span1->day, span2->day, &result->day) ||
		pg_add_s64_overflow(span1->time, span2->time, &result->time) ||
		INTERVAL_NOT_FINITE(result))
		return 1;				/* interval out of range */
	return 0;
}

static int
pg_finite_interval_mi(const Interval *span1, const Interval *span2, Interval *result)
{
	Assert(!INTERVAL_NOT_FINITE(span1));
	Assert(!INTERVAL_NOT_FINITE(span2));

	if (pg_sub_s32_overflow(span1->month, span2->month, &result->month) ||
		pg_sub_s32_overflow(span1->day, span2->day, &result->day) ||
		pg_sub_s64_overflow(span1->time, span2->time, &result->time) ||
		INTERVAL_NOT_FINITE(result))
		return 1;				/* interval out of range */
	return 0;
}

int
pg_interval_pl(int64 t1, int32 d1, int32 m1, int64 t2, int32 d2, int32 m2,
			   int64 *rt, int32 *rd, int32 *rm)
{
	Interval	span1_ = {t1, d1, m1};
	Interval   *span1 = &span1_;
	Interval	span2_ = {t2, d2, m2};
	Interval   *span2 = &span2_;
	Interval	result_;
	Interval   *result = &result_;	/* palloc(sizeof(Interval)) -> local */

	/*
	 * Handle infinities.
	 *
	 * We treat anything that amounts to "infinity - infinity" as an error,
	 * since the interval type has nothing equivalent to NaN.
	 */
	if (INTERVAL_IS_NOBEGIN(span1))
	{
		if (INTERVAL_IS_NOEND(span2))
			return 1;			/* interval out of range */
		else
			INTERVAL_NOBEGIN(result);
	}
	else if (INTERVAL_IS_NOEND(span1))
	{
		if (INTERVAL_IS_NOBEGIN(span2))
			return 1;
		else
			INTERVAL_NOEND(result);
	}
	else if (INTERVAL_NOT_FINITE(span2))
		*result = *span2;		/* memcpy(result, span2, sizeof(Interval)) */
	else
	{
		if (pg_finite_interval_pl(span1, span2, result))
			return 1;
	}

	*rt = result->time;
	*rd = result->day;
	*rm = result->month;
	return 0;
}

int
pg_interval_mi(int64 t1, int32 d1, int32 m1, int64 t2, int32 d2, int32 m2,
			   int64 *rt, int32 *rd, int32 *rm)
{
	Interval	span1_ = {t1, d1, m1};
	Interval   *span1 = &span1_;
	Interval	span2_ = {t2, d2, m2};
	Interval   *span2 = &span2_;
	Interval	result_;
	Interval   *result = &result_;

	if (INTERVAL_IS_NOBEGIN(span1))
	{
		if (INTERVAL_IS_NOBEGIN(span2))
			return 1;			/* interval out of range */
		else
			INTERVAL_NOBEGIN(result);
	}
	else if (INTERVAL_IS_NOEND(span1))
	{
		if (INTERVAL_IS_NOEND(span2))
			return 1;
		else
			INTERVAL_NOEND(result);
	}
	else if (INTERVAL_IS_NOBEGIN(span2))
		INTERVAL_NOEND(result);
	else if (INTERVAL_IS_NOEND(span2))
		INTERVAL_NOBEGIN(result);
	else
	{
		if (pg_finite_interval_mi(span1, span2, result))
			return 1;
	}

	*rt = result->time;
	*rd = result->day;
	*rm = result->month;
	return 0;
}

/* ---------- timestamp.c: justify family ---------- */

/* verbatim interval_justify_hours body on a caller-held result */
static int
pg_interval_justify_hours_impl(Interval *result)
{
	TimeOffset	wholeday;

	/* do nothing for infinite intervals */
	if (INTERVAL_NOT_FINITE(result))
		return 0;

	TMODULO(result->time, wholeday, USECS_PER_DAY);
	if (pg_add_s32_overflow(result->day, wholeday, &result->day))
		return 1;				/* interval out of range */

	if (result->day > 0 && result->time < 0)
	{
		result->time += USECS_PER_DAY;
		result->day--;
	}
	else if (result->day < 0 && result->time > 0)
	{
		result->time -= USECS_PER_DAY;
		result->day++;
	}

	return 0;
}

int
pg_interval_justify_hours(int64 t, int32 d, int32 m, int64 *rt, int32 *rd, int32 *rm)
{
	Interval	result;

	result.month = m;
	result.day = d;
	result.time = t;

	if (pg_interval_justify_hours_impl(&result))
		return 1;

	*rt = result.time;
	*rd = result.day;
	*rm = result.month;
	return 0;
}

int
pg_interval_justify_days(int64 t, int32 d, int32 m, int64 *rt, int32 *rd, int32 *rm)
{
	Interval	result_;
	Interval   *result = &result_;
	int32		wholemonth;

	result->month = m;
	result->day = d;
	result->time = t;

	/* do nothing for infinite intervals */
	if (!INTERVAL_NOT_FINITE(result))
	{
		wholemonth = result->day / DAYS_PER_MONTH;
		result->day -= wholemonth * DAYS_PER_MONTH;
		if (pg_add_s32_overflow(result->month, wholemonth, &result->month))
			return 1;			/* interval out of range */

		if (result->month > 0 && result->day < 0)
		{
			result->day += DAYS_PER_MONTH;
			result->month--;
		}
		else if (result->month < 0 && result->day > 0)
		{
			result->day -= DAYS_PER_MONTH;
			result->month++;
		}
	}

	*rt = result->time;
	*rd = result->day;
	*rm = result->month;
	return 0;
}

int
pg_interval_justify_interval(int64 t, int32 d, int32 m, int64 *rt, int32 *rd, int32 *rm)
{
	Interval	result_;
	Interval   *result = &result_;
	TimeOffset	wholeday;
	int32		wholemonth;

	result->month = m;
	result->day = d;
	result->time = t;

	/* do nothing for infinite intervals */
	if (!INTERVAL_NOT_FINITE(result))
	{
		/* pre-justify days if it might prevent overflow */
		if ((result->day > 0 && result->time > 0) ||
			(result->day < 0 && result->time < 0))
		{
			wholemonth = result->day / DAYS_PER_MONTH;
			result->day -= wholemonth * DAYS_PER_MONTH;
			if (pg_add_s32_overflow(result->month, wholemonth, &result->month))
				return 1;		/* interval out of range */
		}

		/*
		 * Since TimeOffset is int64, abs(wholeday) can't exceed about 1.07e8.
		 * If we pre-justified then abs(result->day) is less than
		 * DAYS_PER_MONTH, so this addition can't overflow.  If we didn't
		 * pre-justify, then day and time are of different signs, so it still
		 * can't overflow.
		 */
		TMODULO(result->time, wholeday, USECS_PER_DAY);
		result->day += wholeday;

		wholemonth = result->day / DAYS_PER_MONTH;
		result->day -= wholemonth * DAYS_PER_MONTH;
		if (pg_add_s32_overflow(result->month, wholemonth, &result->month))
			return 1;

		if (result->month > 0 &&
			(result->day < 0 || (result->day == 0 && result->time < 0)))
		{
			result->day += DAYS_PER_MONTH;
			result->month--;
		}
		else if (result->month < 0 &&
				 (result->day > 0 || (result->day == 0 && result->time > 0)))
		{
			result->day -= DAYS_PER_MONTH;
			result->month++;
		}

		if (result->day > 0 && result->time < 0)
		{
			result->time += USECS_PER_DAY;
			result->day--;
		}
		else if (result->day < 0 && result->time > 0)
		{
			result->time -= USECS_PER_DAY;
			result->day++;
		}
	}

	*rt = result->time;
	*rd = result->day;
	*rm = result->month;
	return 0;
}

/* ---------- timestamp.c: timestamp_mi ---------- */

int
pg_timestamp_mi(Timestamp dt1, Timestamp dt2, int64 *rt, int32 *rd, int32 *rm)
{
	Interval	result_;
	Interval   *result = &result_;	/* palloc(sizeof(Interval)) -> local */

	/*
	 * Handle infinities.
	 *
	 * We treat anything that amounts to "infinity - infinity" as an error,
	 * since the interval type has nothing equivalent to NaN.
	 */
	if (TIMESTAMP_NOT_FINITE(dt1) || TIMESTAMP_NOT_FINITE(dt2))
	{
		if (TIMESTAMP_IS_NOBEGIN(dt1))
		{
			if (TIMESTAMP_IS_NOBEGIN(dt2))
				return 1;		/* interval out of range */
			else
				INTERVAL_NOBEGIN(result);
		}
		else if (TIMESTAMP_IS_NOEND(dt1))
		{
			if (TIMESTAMP_IS_NOEND(dt2))
				return 1;
			else
				INTERVAL_NOEND(result);
		}
		else if (TIMESTAMP_IS_NOBEGIN(dt2))
			INTERVAL_NOEND(result);
		else					/* TIMESTAMP_IS_NOEND(dt2) */
			INTERVAL_NOBEGIN(result);

		*rt = result->time;
		*rd = result->day;
		*rm = result->month;
		return 0;
	}

	if (unlikely(pg_sub_s64_overflow(dt1, dt2, &result->time)))
		return 1;				/* interval out of range */

	result->month = 0;
	result->day = 0;

	/*
	 * This is wrong, but removing it breaks a lot of regression tests (see
	 * the vendored comment in timestamp.c).  DirectFunctionCall1(
	 * interval_justify_hours, ...) -> direct call of the shimmed body.
	 */
	if (pg_interval_justify_hours_impl(result))
		return 1;

	*rt = result->time;
	*rd = result->day;
	*rm = result->month;
	return 0;
}

/* ---------- timestamp.c: timestamp +- interval (checked-op plane) ---------- */

int
pg_timestamp_pl_interval(Timestamp timestamp, int64 st, int32 sd, int32 sm,
						 Timestamp *presult)
{
	Interval	span_ = {st, sd, sm};
	Interval   *span = &span_;
	Timestamp	result;

	/*
	 * Handle infinities.
	 *
	 * We treat anything that amounts to "infinity - infinity" as an error,
	 * since the timestamp type has nothing equivalent to NaN.
	 */
	if (INTERVAL_IS_NOBEGIN(span))
	{
		if (TIMESTAMP_IS_NOEND(timestamp))
			return 1;			/* timestamp out of range */
		else
			TIMESTAMP_NOBEGIN(result);
	}
	else if (INTERVAL_IS_NOEND(span))
	{
		if (TIMESTAMP_IS_NOBEGIN(timestamp))
			return 1;
		else
			TIMESTAMP_NOEND(result);
	}
	else if (TIMESTAMP_NOT_FINITE(timestamp))
		result = timestamp;
	else
	{
		if (span->month != 0)
		{
			/* OUT-OF-PLANE TRAP: timestamp2tm/tm2timestamp month walk (the
			 * j2date divider chain) -- never reached by the wave-6
			 * harnesses, which pin span.month == 0 or a sentinel. */
			return 99;
		}

		if (span->day != 0)
		{
			/* OUT-OF-PLANE TRAP: julian day walk, as above */
			return 99;
		}

		if (pg_add_s64_overflow(timestamp, span->time, &timestamp))
			return 1;			/* timestamp out of range */

		if (!IS_VALID_TIMESTAMP(timestamp))
			return 1;

		result = timestamp;
	}

	*presult = result;
	return 0;
}

int
pg_timestamp_mi_interval(Timestamp timestamp, int64 st, int32 sd, int32 sm,
						 Timestamp *presult)
{
	Interval	span_ = {st, sd, sm};
	Interval	tspan;

	if (pg_interval_um_internal_c(&span_, &tspan))
		return 1;

	return pg_timestamp_pl_interval(timestamp, tspan.time, tspan.day,
									tspan.month, presult);
}

/* ==================== wave-7 extension ====================
 *
 * Timestamp/timestamptz remainder rows: finite/overlaps/hash/send,
 * timestamptz +- interval (checked-op planes), timestamp<->timestamptz
 * conversions (tz-seam model), zone/izone, typmod scale, in_range.
 *
 * Provenance:
 *   - src/backend/utils/adt/timestamp.c  (timestamp_finite,
 *     overlaps_timestamp, timestamp_smaller/larger [already above],
 *     timestamptz_pl_interval_internal + fmgr wrappers,
 *     timestamp2timestamptz_opt_overflow, timestamptz2timestamp,
 *     timestamp_zone/izone, timestamptz_zone/izone,
 *     AdjustTimestampForTypmod, timestamp_scale, tm2timestamp, time2t,
 *     interval_cmp_value, interval_sign, in_range_timestamp[tz]_interval,
 *     in_range_interval_interval)
 *   - src/backend/utils/adt/datetime.c   (date2j)
 *   - src/common/hashfn.c                (hash_bytes_uint32,
 *     hash_bytes_uint32_extended: rot/mix/final + bodies verbatim,
 *     matching the already-proved proofs/hash vendor)
 *   - src/backend/access/hash/hashfunc.c (hashint8, hashint8extended:
 *     the int64 fold, bodies verbatim; timestamp_hash[_extended] and
 *     timestamptz_hash[_extended] are `return hashint8[extended](fcinfo)`)
 *   - src/backend/libpq/pqformat.c       (pq_sendint64 -> 8 BE bytes;
 *     pq_begintypsend/pq_endtypsend shimmed as in proofs/int-arith:
 *     4-byte little-endian 4B varlena header (len<<2) + payload)
 *   ref: postgres/postgres REL_18_STABLE, fetched 2026-07-29
 *   (raw.githubusercontent.com/postgres/postgres/REL_18_STABLE/...)
 *
 * Additional shims (plumbing only, never logic):
 *   - PROOF_EREPORT_FLAG convention extended: return 0 = success,
 *     1 = ereport(ERROR, 22008 "timestamp out of range"),
 *     2 = ereport(ERROR, 22023 invalid parameter: izone finiteness /
 *         months-days message, typmod precision message),
 *     3 = ereport(ERROR, 22013 invalid preceding/following size),
 *     99 = OUT-OF-PLANE TRAP (julian month/day walk or DynTz/full-zone
 *          arm; must be dead on every harness plane -- loud, not vacuous).
 *   - overlaps_timestamp: PG_GETARG_DATUM/PG_ARGISNULL -> (int64, int)
 *     pairs; PG_RETURN_NULL() -> return 1 with *result untouched;
 *     PG_RETURN_BOOL(x) -> *result = x, return 0. The
 *     TIMESTAMP_GT/TIMESTAMP_LT macros (DirectFunctionCall2 of
 *     timestamp_gt/timestamp_lt) -> the same int64 comparisons those
 *     vendored comparators (above) are proved equivalent to.
 *   - timestamptz_pl_interval_internal: session-timezone/at-zone lookup
 *     (attimezone) is consumed ONLY by the month!=0/day!=0 julian arms,
 *     which are OUT-OF-PLANE traps here exactly as in wave-6
 *     timestamp_pl_interval; the lookup itself (session_timezone /
 *     lookup_timezone seam) therefore drops out of the vendored body.
 *   - timestamp2timestamptz_opt_overflow: the timestamp2tm ->
 *     DetermineTimeZoneOffset(tm, session_timezone) block -> the SAME
 *     shared tz-seam model as the cross-type section above
 *     (tz = pg_model_tz_offset); called with overflow == NULL by
 *     timestamp_timestamptz, so the !IS_VALID arm falls through to the
 *     ereport -> flag 1. timestamp2tm's pro-forma failure arm is
 *     always-success under the model on both sides (out of proof).
 *   - timestamptz2timestamp: the timestamp2tm DECOMPOSE seam is modeled as
 *     a literal tm (2000-01-01 00:00:00) + the shared symbolic
 *     pg_model_fsec + tz out-param (dead: tm2timestamp is called with
 *     tzp == NULL); the RECOMPOSE (tm2timestamp + date2j + time2t) is
 *     vendored VERBATIM and stays in the theorem (the literal tm constant-
 *     folds its julian arithmetic). decompose-failure arm out of proof.
 *   - timestamp_zone/timestamptz_zone: DecodeTimezoneName(tzname,&val,&tzp)
 *     -> seam model: type pinned TZNAME_FIXED_OFFSET, val =
 *     pg_model_tzname_val (the Rust side stubs DecodeTimezoneName to
 *     TzLookup::FixedOffset of the same value). The TZNAME_DYNTZ /
 *     full-zone arms are OUT-OF-PLANE traps (99). text_to_cstring_buffer
 *     name plumbing feeds only the seam -> drops out.
 *   - izone: DatumGetCString(DirectFunctionCall1(interval_out, ...)) is
 *     message text only -> flag 2.
 *   - interval_cmp_value/interval_sign: int128.h native-arm semantics
 *     (__int128) verbatim: span = (INT128) time; days = day + month*30;
 *     span += (INT128) days * USECS_PER_DAY.
 *   - pg_mul_s64_overflow: common/int.h __builtin arm verbatim.
 */

#include <stddef.h>			/* NULL */

typedef uint32_t uint32;
typedef uint64_t uint64;
typedef __int128 INT128;

/* ---------- timestamp.c: timestamp_finite ---------- */

int
pg_timestamp_finite(Timestamp timestamp)
{
	return !TIMESTAMP_NOT_FINITE(timestamp);
}

/* ---------- common/hashfn.c + hashfunc.c: timestamp[tz]_hash[_extended] -- */

static inline uint32
pg_w7_rotate_left32(uint32 word, int n)
{
	return (word << n) | (word >> (32 - n));
}

#define w7_rot(x,k) pg_w7_rotate_left32(x, k)

#define w7_mix(a,b,c) \
{ \
  a -= c;  a ^= w7_rot(c, 4);	c += b; \
  b -= a;  b ^= w7_rot(a, 6);	a += c; \
  c -= b;  c ^= w7_rot(b, 8);	b += a; \
  a -= c;  a ^= w7_rot(c,16);	c += b; \
  b -= a;  b ^= w7_rot(a,19);	a += c; \
  c -= b;  c ^= w7_rot(b, 4);	b += a; \
}

#define w7_final(a,b,c) \
{ \
  c ^= b; c -= w7_rot(b,14); \
  a ^= c; a -= w7_rot(c,11); \
  b ^= a; b -= w7_rot(a,25); \
  c ^= b; c -= w7_rot(b,16); \
  a ^= c; a -= w7_rot(c, 4); \
  b ^= a; b -= w7_rot(a,14); \
  c ^= b; c -= w7_rot(b,24); \
}

static uint32
pg_w7_hash_bytes_uint32(uint32 k)
{
	uint32		a,
				b,
				c;

	a = b = c = 0x9e3779b9 + (uint32) sizeof(uint32) + 3923095;
	a += k;

	w7_final(a, b, c);

	/* report the result */
	return c;
}

static uint64
pg_w7_hash_bytes_uint32_extended(uint32 k, uint64 seed)
{
	uint32		a,
				b,
				c;

	a = b = c = 0x9e3779b9 + (uint32) sizeof(uint32) + 3923095;

	if (seed != 0)
	{
		a += (uint32) (seed >> 32);
		b += (uint32) seed;
		w7_mix(a, b, c);
	}

	a += k;

	w7_final(a, b, c);

	/* report the result */
	return ((uint64) b << 32) | c;
}

/* hashfunc.c hashint8 (timestamp_hash / timestamptz_hash) */
uint32
pg_timestamp_hash(int64 val)
{
	/*
	 * The idea here is to produce a hash value compatible with the values
	 * produced by hashint2 and hashint4, so that apparent-type int8 can be
	 * used interchangeably with int2 or int4 in hash indexes and hash joins.
	 */
	uint32		lohalf = (uint32) val;
	uint32		hihalf = (uint32) (val >> 32);

	lohalf ^= (val >= 0) ? hihalf : ~hihalf;

	return pg_w7_hash_bytes_uint32(lohalf);
}

/* hashfunc.c hashint8extended */
uint64
pg_timestamp_hash_extended(int64 val, uint64 seed)
{
	/* Same approach as hashint8 */
	uint32		lohalf = (uint32) val;
	uint32		hihalf = (uint32) (val >> 32);

	lohalf ^= (val >= 0) ? hihalf : ~hihalf;

	return pg_w7_hash_bytes_uint32_extended(lohalf, seed);
}

/* ---------- timestamp.c: timestamp_send (pq_sendint64) ---------- */

/* pq_endtypsend's SET_VARSIZE: 4B little-endian varlena header (len<<2) */
static void
pg_w7_set_varsize_4b(unsigned char *out, uint32 len)
{
	uint32		hdr = len << 2;

	out[0] = (unsigned char) (hdr & 0xFF);
	out[1] = (unsigned char) ((hdr >> 8) & 0xFF);
	out[2] = (unsigned char) ((hdr >> 16) & 0xFF);
	out[3] = (unsigned char) ((hdr >> 24) & 0xFF);
}

int32
pg_timestamp_send(int64 timestamp, unsigned char *out /* [12] */ )
{
	/* pq_begintypsend reserves 4 bytes; pq_sendint64 appends BE bytes */
	out[4] = (unsigned char) (((uint64) timestamp >> 56) & 0xFF);
	out[5] = (unsigned char) (((uint64) timestamp >> 48) & 0xFF);
	out[6] = (unsigned char) (((uint64) timestamp >> 40) & 0xFF);
	out[7] = (unsigned char) (((uint64) timestamp >> 32) & 0xFF);
	out[8] = (unsigned char) (((uint64) timestamp >> 24) & 0xFF);
	out[9] = (unsigned char) (((uint64) timestamp >> 16) & 0xFF);
	out[10] = (unsigned char) (((uint64) timestamp >> 8) & 0xFF);
	out[11] = (unsigned char) ((uint64) timestamp & 0xFF);
	pg_w7_set_varsize_4b(out, 12);	/* pq_endtypsend */
	return 12;
}

/* ---------- timestamp.c: overlaps_timestamp ---------- */

/*
 * Returns 1 for SQL NULL (PG_RETURN_NULL), 0 otherwise with *result set.
 * Datums ride as int64 values + int null flags. TIMESTAMP_GT/LT are the
 * vendored timestamp_gt/timestamp_lt comparisons.
 */
int
pg_overlaps_timestamp(int64 ts1, int ts1IsNull, int64 te1, int te1IsNull,
					  int64 ts2, int ts2IsNull, int64 te2, int te2IsNull,
					  int *result)
{
#define W7_TIMESTAMP_GT(t1,t2) ((t1) > (t2))
#define W7_TIMESTAMP_LT(t1,t2) ((t1) < (t2))

	/*
	 * If both endpoints of interval 1 are null, the result is null (unknown).
	 * If just one endpoint is null, take ts1 as the non-null one. Otherwise,
	 * take ts1 as the lesser endpoint.
	 */
	if (ts1IsNull)
	{
		if (te1IsNull)
			return 1;
		/* swap null for non-null */
		ts1 = te1;
		te1IsNull = 1;
	}
	else if (!te1IsNull)
	{
		if (W7_TIMESTAMP_GT(ts1, te1))
		{
			int64		tt = ts1;

			ts1 = te1;
			te1 = tt;
		}
	}

	/* Likewise for interval 2. */
	if (ts2IsNull)
	{
		if (te2IsNull)
			return 1;
		/* swap null for non-null */
		ts2 = te2;
		te2IsNull = 1;
	}
	else if (!te2IsNull)
	{
		if (W7_TIMESTAMP_GT(ts2, te2))
		{
			int64		tt = ts2;

			ts2 = te2;
			te2 = tt;
		}
	}

	/*
	 * At this point neither ts1 nor ts2 is null, so we can consider three
	 * cases: ts1 > ts2, ts1 < ts2, ts1 = ts2
	 */
	if (W7_TIMESTAMP_GT(ts1, ts2))
	{
		if (te2IsNull)
			return 1;
		if (W7_TIMESTAMP_LT(ts1, te2))
		{
			*result = 1;
			return 0;
		}
		if (te1IsNull)
			return 1;

		*result = 0;
		return 0;
	}
	else if (W7_TIMESTAMP_LT(ts1, ts2))
	{
		if (te1IsNull)
			return 1;
		if (W7_TIMESTAMP_LT(ts2, te1))
		{
			*result = 1;
			return 0;
		}
		if (te2IsNull)
			return 1;

		*result = 0;
		return 0;
	}
	else
	{
		if (te1IsNull || te2IsNull)
			return 1;
		*result = 1;
		return 0;
	}
}

/* ---------- timestamp.c: timestamptz_pl_interval_internal planes -------- */

/*
 * Verbatim except: month!=0 / day!=0 julian arms -> OUT-OF-PLANE TRAP 99
 * (their attimezone consumer drops out with them, so the session/at-zone
 * lookup seam never enters the body); ereport -> flag 1.
 */
int
pg_timestamptz_pl_interval(Timestamp timestamp, int64 st, int32 sd, int32 sm,
						   Timestamp *presult)
{
	Interval	span_ = {st, sd, sm};
	Interval   *span = &span_;
	Timestamp	result;

	/*
	 * Handle infinities.
	 *
	 * We treat anything that amounts to "infinity - infinity" as an error,
	 * since the timestamptz type has nothing equivalent to NaN.
	 */
	if (INTERVAL_IS_NOBEGIN(span))
	{
		if (TIMESTAMP_IS_NOEND(timestamp))
			return 1;
		else
			TIMESTAMP_NOBEGIN(result);
	}
	else if (INTERVAL_IS_NOEND(span))
	{
		if (TIMESTAMP_IS_NOBEGIN(timestamp))
			return 1;
		else
			TIMESTAMP_NOEND(result);
	}
	else if (TIMESTAMP_NOT_FINITE(timestamp))
		result = timestamp;
	else
	{
		if (span->month != 0)
		{
			/* OUT-OF-PLANE TRAP: julian month walk (timestamp2tm +
			 * DetermineTimeZoneOffset + tm2timestamp) */
			return 99;
		}

		if (span->day != 0)
		{
			/* OUT-OF-PLANE TRAP: julian day walk */
			return 99;
		}

		if (pg_add_s64_overflow(timestamp, span->time, &timestamp))
			return 1;

		if (!IS_VALID_TIMESTAMP(timestamp))
			return 1;

		result = timestamp;
	}

	*presult = result;
	return 0;
}

int
pg_timestamptz_mi_interval(Timestamp timestamp, int64 st, int32 sd, int32 sm,
						   Timestamp *presult)
{
	Interval	span_ = {st, sd, sm};
	Interval	tspan;

	if (pg_interval_um_internal_c(&span_, &tspan))
		return 1;

	return pg_timestamptz_pl_interval(timestamp, tspan.time, tspan.day,
									  tspan.month, presult);
}

/* ---------- timestamp.c: timestamp <-> timestamptz conversions ---------- */

/*
 * timestamp_timestamptz = timestamp2timestamptz_opt_overflow(ts, NULL);
 * seam model as pg_timestamp2timestamptz_opt_overflow above, but the
 * overflow == NULL path falls through to ereport -> flag 1.
 */
int
pg_timestamp_timestamptz(Timestamp timestamp, Timestamp *presult)
{
	Timestamp	result;
	int			tz;

	if (TIMESTAMP_NOT_FINITE(timestamp))
	{
		*presult = timestamp;
		return 0;
	}

	/* shim (seam model): timestamp2tm + DetermineTimeZoneOffset block */
	tz = pg_model_tz_offset;

	result = pg_dt2local(timestamp, -tz);

	if (IS_VALID_TIMESTAMP(result))
	{
		*presult = result;
		return 0;
	}

	return 1;					/* ereport(ERROR, 22008) */
}

/* datetime.c date2j (verbatim) */
static int
pg_w7_date2j(int year, int month, int day)
{
	int			julian;
	int			century;

	if (month > 2)
	{
		month += 1;
		year += 4800;
	}
	else
	{
		month += 13;
		year += 4799;
	}

	century = year / 100;
	julian = year * 365 - 32167;
	julian += year / 4 - century + century / 4;
	julian += 7834 * month / 256 + day;

	return julian;
}								/* date2j() */

/* common/int.h __builtin arm (verbatim) */
static inline int
pg_mul_s64_overflow(int64 a, int64 b, int64 *result)
{
	return __builtin_mul_overflow(a, b, result);
}

/* timestamp.h julian-window macros (literal values) */
#define JULIAN_MINYEAR (-4713)
#define JULIAN_MINMONTH (11)
#define JULIAN_MINDAY (24)
#define JULIAN_MAXYEAR (5874898)
#define JULIAN_MAXMONTH (6)
#define JULIAN_MAXDAY (3)

#define IS_VALID_JULIAN(y,m,d) \
	(((y) > JULIAN_MINYEAR || \
	  ((y) == JULIAN_MINYEAR && ((m) >= JULIAN_MINMONTH))) && \
	 ((y) < JULIAN_MAXYEAR || \
	  ((y) == JULIAN_MAXYEAR && ((m) < JULIAN_MAXMONTH))))

typedef int32 fsec_t;

struct pg_w7_tm
{
	int			tm_sec;
	int			tm_min;
	int			tm_hour;
	int			tm_mday;
	int			tm_mon;
	int			tm_year;
};

#define MINS_PER_HOUR	60
#define SECS_PER_MINUTE 60

/* timestamp.c time2t (verbatim) */
static TimeOffset
pg_w7_time2t(const int hour, const int min, const int sec, const fsec_t fsec)
{
	return (((((hour * MINS_PER_HOUR) + min) * SECS_PER_MINUTE) + sec) * USECS_PER_SEC) + fsec;
}

/* timestamp.c tm2timestamp (verbatim) */
static int
pg_w7_tm2timestamp(struct pg_w7_tm *tm, fsec_t fsec, int *tzp, Timestamp *result)
{
	TimeOffset	date;
	TimeOffset	time;

	/* Prevent overflow in Julian-day routines */
	if (!IS_VALID_JULIAN(tm->tm_year, tm->tm_mon, tm->tm_mday))
	{
		*result = 0;			/* keep compiler quiet */
		return -1;
	}

	date = pg_w7_date2j(tm->tm_year, tm->tm_mon, tm->tm_mday) - POSTGRES_EPOCH_JDATE;
	time = pg_w7_time2t(tm->tm_hour, tm->tm_min, tm->tm_sec, fsec);

	if (unlikely(pg_mul_s64_overflow(date, USECS_PER_DAY, result) ||
				 pg_add_s64_overflow(*result, time, result)))
	{
		*result = 0;			/* keep compiler quiet */
		return -1;
	}
	if (tzp != NULL)
		*result = pg_dt2local(*result, -(*tzp));

	/* final range check catches just-out-of-range timestamps */
	if (!IS_VALID_TIMESTAMP(*result))
	{
		*result = 0;			/* keep compiler quiet */
		return -1;
	}

	return 0;
}

/* shared decompose-seam fsec (set by the harness on both sides) */
int32		pg_model_fsec = 0;

/*
 * timestamptz2timestamp: the timestamp2tm decompose is the seam (literal
 * 2000-01-01 00:00:00 tm + pg_model_fsec; the tz out-param is dead because
 * tm2timestamp gets tzp == NULL); the recompose is verbatim above.
 */
int
pg_timestamptz_timestamp(Timestamp timestamp, Timestamp *presult)
{
	Timestamp	result;

	if (TIMESTAMP_NOT_FINITE(timestamp))
		result = timestamp;
	else
	{
		struct pg_w7_tm tt,
				   *tm = &tt;
		fsec_t		fsec;

		/* shim (seam model): timestamp2tm(timestamp, &tz, tm, &fsec, NULL,
		 * NULL) -> literal tm + symbolic fsec; always-success (pro-forma
		 * failure arm out of proof) */
		tm->tm_year = 2000;
		tm->tm_mon = 1;
		tm->tm_mday = 1;
		tm->tm_hour = 0;
		tm->tm_min = 0;
		tm->tm_sec = 0;
		fsec = pg_model_fsec;

		if (pg_w7_tm2timestamp(tm, fsec, NULL, &result) != 0)
			return 1;			/* ereport(ERROR, 22008) */
	}
	*presult = result;
	return 0;
}

/* ---------- timestamp.c: timestamp_zone / timestamptz_zone -------------- */

/* shared zone-name-decode seam: TZNAME_FIXED_OFFSET value (set by harness) */
int32		pg_model_tzname_val = 0;

/*
 * Verbatim except: text_to_cstring_buffer + DecodeTimezoneName -> the seam
 * model (type pinned TZNAME_FIXED_OFFSET, val = pg_model_tzname_val); the
 * TZNAME_DYNTZ / full-zone arms are OUT-OF-PLANE traps.
 */
int
pg_timestamp_zone(Timestamp timestamp, Timestamp *presult)
{
	Timestamp	result;
	int			tz;
	int			val;

	if (TIMESTAMP_NOT_FINITE(timestamp))
	{
		*presult = timestamp;
		return 0;
	}

	/* shim (seam model): DecodeTimezoneName(tzname, &val, &tzp) */
	val = pg_model_tzname_val;

	/* if (type == TZNAME_FIXED_OFFSET) */
	{
		/* fixed-offset abbreviation */
		tz = val;
		result = pg_dt2local(timestamp, tz);
	}
	/* TZNAME_DYNTZ / full-zone arms: OUT-OF-PLANE (trap 99 by construction:
	 * the seam pins type to TZNAME_FIXED_OFFSET, matching the Rust stub) */

	if (!IS_VALID_TIMESTAMP(result))
		return 1;				/* ereport(ERROR, 22008) */

	*presult = result;
	return 0;
}

int
pg_timestamptz_zone(Timestamp timestamp, Timestamp *presult)
{
	Timestamp	result;
	int			tz;
	int			val;

	if (TIMESTAMP_NOT_FINITE(timestamp))
	{
		*presult = timestamp;
		return 0;
	}

	/* shim (seam model): DecodeTimezoneName(tzname, &val, &tzp) */
	val = pg_model_tzname_val;

	/* if (type == TZNAME_FIXED_OFFSET) */
	{
		/* fixed-offset abbreviation */
		tz = -val;
		result = pg_dt2local(timestamp, tz);
	}

	if (!IS_VALID_TIMESTAMP(result))
		return 1;				/* ereport(ERROR, 22008) */

	*presult = result;
	return 0;
}

/* ---------- timestamp.c: timestamp_izone / timestamptz_izone ------------ */

/*
 * Verbatim except: the two invalid-parameter ereports (whose message text
 * runs interval_out) -> flag 2; out-of-range ereport -> flag 1.
 */
int
pg_timestamp_izone(int64 zt, int32 zd, int32 zm, Timestamp timestamp,
				   Timestamp *presult)
{
	Interval	zone_ = {zt, zd, zm};
	Interval   *zone = &zone_;
	Timestamp	result;
	int			tz;

	if (TIMESTAMP_NOT_FINITE(timestamp))
	{
		*presult = timestamp;
		return 0;
	}

	if (INTERVAL_NOT_FINITE(zone))
		return 2;				/* "interval time zone \"%s\" must be finite" */

	if (zone->month != 0 || zone->day != 0)
		return 2;				/* "... must not include months or days" */

	tz = zone->time / USECS_PER_SEC;

	result = pg_dt2local(timestamp, tz);

	if (!IS_VALID_TIMESTAMP(result))
		return 1;

	*presult = result;
	return 0;
}

int
pg_timestamptz_izone(int64 zt, int32 zd, int32 zm, Timestamp timestamp,
					 Timestamp *presult)
{
	Interval	zone_ = {zt, zd, zm};
	Interval   *zone = &zone_;
	Timestamp	result;
	int			tz;

	if (TIMESTAMP_NOT_FINITE(timestamp))
	{
		*presult = timestamp;
		return 0;
	}

	if (INTERVAL_NOT_FINITE(zone))
		return 2;

	if (zone->month != 0 || zone->day != 0)
		return 2;

	tz = -(zone->time / USECS_PER_SEC);

	result = pg_dt2local(timestamp, tz);

	if (!IS_VALID_TIMESTAMP(result))
		return 1;

	*presult = result;
	return 0;
}

/* ---------- timestamp.c: AdjustTimestampForTypmod + timestamp_scale ----- */

#define MAX_TIMESTAMP_PRECISION 6

/*
 * Verbatim; ereturn(escontext, ...) -> flag 2 (every vendored caller here
 * passes escontext == NULL, so the error is thrown as in C's fmgr path).
 */
int
pg_adjust_timestamp_for_typmod(Timestamp *time, int32 typmod)
{
	static const int64 TimestampScales[MAX_TIMESTAMP_PRECISION + 1] = {
		INT64CONST(1000000),
		INT64CONST(100000),
		INT64CONST(10000),
		INT64CONST(1000),
		INT64CONST(100),
		INT64CONST(10),
		INT64CONST(1)
	};

	static const int64 TimestampOffsets[MAX_TIMESTAMP_PRECISION + 1] = {
		INT64CONST(500000),
		INT64CONST(50000),
		INT64CONST(5000),
		INT64CONST(500),
		INT64CONST(50),
		INT64CONST(5),
		INT64CONST(0)
	};

	if (!TIMESTAMP_NOT_FINITE(*time)
		&& (typmod != -1) && (typmod != MAX_TIMESTAMP_PRECISION))
	{
		if (typmod < 0 || typmod > MAX_TIMESTAMP_PRECISION)
			return 2;			/* ereturn 22023 "timestamp(%d) precision
								 * must be between %d and %d" */

		if (*time >= INT64CONST(0))
		{
			*time = ((*time + TimestampOffsets[typmod]) / TimestampScales[typmod]) *
				TimestampScales[typmod];
		}
		else
		{
			*time = -((((-*time) + TimestampOffsets[typmod]) / TimestampScales[typmod])
					  * TimestampScales[typmod]);
		}
	}

	return 0;
}

int
pg_timestamp_scale(Timestamp timestamp, int32 typmod, Timestamp *presult)
{
	Timestamp	result;
	int			rc;

	result = timestamp;

	rc = pg_adjust_timestamp_for_typmod(&result, typmod);
	if (rc)
		return rc;

	*presult = result;
	return 0;
}

/* ---------- timestamp.c: interval_sign + in_range family ---------------- */

/* interval_cmp_value with int128.h native-arm semantics (verbatim) */
static INT128
pg_w7_interval_cmp_value(const Interval *interval)
{
	INT128		span;
	int64		days;

	/*
	 * Combine the month and day fields into an integral number of days.
	 * Because the inputs are int32, int64 arithmetic suffices here.
	 */
	span = (INT128) interval->time;
	days = interval->day;
	days += interval->month * INT64CONST(30);
	span += (INT128) days * USECS_PER_DAY;

	return span;
}

static int
pg_w7_interval_sign(const Interval *interval)
{
	INT128		span = pg_w7_interval_cmp_value(interval);

	if (span < 0)
		return -1;
	if (span > 0)
		return 1;
	return 0;
}

static int
pg_w7_interval_cmp_internal(const Interval *interval1, const Interval *interval2)
{
	INT128		span1 = pg_w7_interval_cmp_value(interval1);
	INT128		span2 = pg_w7_interval_cmp_value(interval2);

	if (span1 < span2)
		return -1;
	if (span1 > span2)
		return 1;
	return 0;
}

/*
 * in_range flags: 0 ok (*result = bool), 3 = 22013 invalid offset,
 * 1 = 22008 from the pl/mi composition, 99 = out-of-plane trap.
 */
int
pg_in_range_timestamp_interval(Timestamp val, Timestamp base,
							   int64 ot, int32 od, int32 om,
							   int sub, int less, int *result)
{
	Interval	offset_ = {ot, od, om};
	Interval   *offset = &offset_;
	Timestamp	sum;
	int			rc;

	if (pg_w7_interval_sign(offset) < 0)
		return 3;

	if (INTERVAL_IS_NOEND(offset) &&
		(sub ? TIMESTAMP_IS_NOEND(base) : TIMESTAMP_IS_NOBEGIN(base)))
	{
		*result = 1;
		return 0;
	}

	/* We don't currently bother to avoid overflow hazards here */
	if (sub)
		rc = pg_timestamp_mi_interval(base, ot, od, om, &sum);
	else
		rc = pg_timestamp_pl_interval(base, ot, od, om, &sum);
	if (rc)
		return rc;

	if (less)
		*result = (val <= sum);
	else
		*result = (val >= sum);
	return 0;
}

int
pg_in_range_timestamptz_interval(Timestamp val, Timestamp base,
								 int64 ot, int32 od, int32 om,
								 int sub, int less, int *result)
{
	Interval	offset_ = {ot, od, om};
	Interval   *offset = &offset_;
	Timestamp	sum;
	int			rc;

	if (pg_w7_interval_sign(offset) < 0)
		return 3;

	if (INTERVAL_IS_NOEND(offset) &&
		(sub ? TIMESTAMP_IS_NOEND(base) : TIMESTAMP_IS_NOBEGIN(base)))
	{
		*result = 1;
		return 0;
	}

	if (sub)
		rc = pg_timestamptz_mi_interval(base, ot, od, om, &sum);
	else
		rc = pg_timestamptz_pl_interval(base, ot, od, om, &sum);
	if (rc)
		return rc;

	if (less)
		*result = (val <= sum);
	else
		*result = (val >= sum);
	return 0;
}

int
pg_in_range_interval_interval(int64 vt, int32 vd, int32 vm,
							  int64 bt, int32 bd, int32 bm,
							  int64 ot, int32 od, int32 om,
							  int sub, int less, int *result)
{
	Interval	val_ = {vt, vd, vm};
	Interval	base_ = {bt, bd, bm};
	Interval	offset_ = {ot, od, om};
	Interval   *val = &val_;
	Interval   *base = &base_;
	Interval   *offset = &offset_;
	Interval	sum;
	int			rc;

	if (pg_w7_interval_sign(offset) < 0)
		return 3;

	if (INTERVAL_IS_NOEND(offset) &&
		(sub ? INTERVAL_IS_NOEND(base) : INTERVAL_IS_NOBEGIN(base)))
	{
		*result = 1;
		return 0;
	}

	if (sub)
		rc = pg_interval_mi(base->time, base->day, base->month,
							offset->time, offset->day, offset->month,
							&sum.time, &sum.day, &sum.month);
	else
		rc = pg_interval_pl(base->time, base->day, base->month,
							offset->time, offset->day, offset->month,
							&sum.time, &sum.day, &sum.month);
	if (rc)
		return rc;				/* 22015 interval out of range in C; the
								 * harness checks verdict only on this arm */

	if (less)
		*result = (pg_w7_interval_cmp_internal(val, &sum) <= 0);
	else
		*result = (pg_w7_interval_cmp_internal(val, &sum) >= 0);
	return 0;
}
