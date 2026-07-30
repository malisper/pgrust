/*
 * Vendored PostgreSQL C for the datetime-b proof family (lane B/C tail:
 * interval typmod in/out, interval avg/sum aggregate cores, time/timetz
 * scale, make_date/make_time/make_timestamp, timetz_part tz field arms).
 *
 * Provenance:
 *   - src/backend/utils/adt/date.c       (AdjustTimeForTypmod, time_scale,
 *                                         timetz_scale, make_date, make_time,
 *                                         float_time_overflows, timetz2tm,
 *                                         timetz_part_common UNITS arms)
 *   - src/backend/utils/adt/timestamp.c  (intervaltypmodin, intervaltypmodout,
 *                                         make_timestamp_internal/make_timestamp,
 *                                         finite_interval_pl/mi,
 *                                         do_interval_accum/do_interval_discard,
 *                                         interval_avg_combine non-null path,
 *                                         interval_avg, interval_sum,
 *                                         interval_avg_serialize/deserialize,
 *                                         interval_um_internal, interval_div)
 *   - src/backend/utils/adt/datetime.c   (date2j, ValidateDate, day_tab)
 *   ref: postgres/postgres REL_18_STABLE, fetched 2026-07-29
 *
 * Shims (plumbing only, never logic) — every one listed:
 *   - `pg_` prefix on every function name; callers adjusted to the prefixed
 *     names.
 *   - fmgr PG_FUNCTION_ARGS unwrapping -> plain C signatures; PG_RETURN_* ->
 *     out-params / plain returns; PG_RETURN_NULL -> `*isnull = 1; return;`.
 *   - ereport(ERROR, ...) -> PROOF_EREPORT_FLAG(err) + return at the exact
 *     program point (longjmp -> early return). Callers of shimmed void
 *     helpers add `if (*err) return;` propagation lines, each marked
 *     "shim: longjmp propagation". Message text leaves the proof; the
 *     verdict (and, via the single sqlstate per arm, sqlstate parity
 *     asserted Rust-side) stays in.
 *   - ereport(WARNING, ...) in intervaltypmodin -> dropped (comment kept):
 *     WARNING emission is out of proof on BOTH sides (the Rust harness stubs
 *     elog::policy::message_level_is_interesting to false, making the
 *     shipped builder self-suppress); the VALUE path (precision clamped to
 *     MAX_INTERVAL_PRECISION) stays in-theorem verbatim.
 *   - palloc'd results -> caller-provided out-params/buffers.
 *   - intervaltypmodout's two variadic snprintf calls ("%s(%d)" / "%s") ->
 *     pg_proof_emit_str_paren_int / pg_proof_emit_str (fixed 64-byte buffer,
 *     C variadics are goto-cc-unsupported). The %d emission for the masked
 *     precision (0..65535, nonnegative) is standard decimal digits; the
 *     shims return the emitted length (replaces strlen, no libc model).
 *   - interval_avg_serialize's pq_begintypsend/pq_sendint64/pq_sendint32 ->
 *     big-endian byte emission into a caller 40-byte buffer at a cursor
 *     (exactly pqformat's wire layout); interval_avg_deserialize's
 *     initReadOnlyStringInfo/pq_getmsgint64/pq_getmsgint/pq_getmsgend ->
 *     cursor reads over (data,len) with the same insufficient-data /
 *     trailing-data ereports rewired to PROOF_EREPORT_FLAG.
 *   - interval_avg's DirectFunctionCall2(interval_div, ...) -> direct call
 *     of the vendored pg_interval_div_raw (fmgr dispatch removed, body
 *     verbatim).
 *   - AggCheckCallContext guards in serialize/deserialize -> dropped
 *     (aggregate-context plumbing is out of proof both sides; the Rust
 *     harness stubs fcinfo.agg_context to a dummy context).
 *   - ValidateDate's DOY arm calls j2date; that arm is out of every plane
 *     here (fmask is always the literal DTK_DATE_M, which has no DOY bit).
 *     j2date is NOT vendored: the arm is rewired to set the out-of-plane
 *     trap flag (harnesses assert it stayed 0), so a folding failure is a
 *     loud FAIL, never a silent wall or vacuous pass (wave-6 plane-fencing
 *     pattern).
 *   - timetz_part_common: the units-token decode prologue
 *     (downcase_truncate_identifier + DecodeUnits/DecodeSpecial) is a
 *     PER-CELL LITERAL SELECTOR seam: the vendored entry takes the decoded
 *     `val` directly and covers only the `type == UNITS`, retnumeric=false
 *     body (verbatim from the timetz2tm call down). Each harness cell pins
 *     val to one literal DTK_* selector; the Rust side runs its OWN full
 *     decode over the literal unit token in-theorem.
 *   - pg_add/sub/mul/neg_s32/s64_overflow: common/int.h __builtin arm
 *     verbatim.
 *   - isnan/isinf/rint/fabs come from <math.h>; CBMC models them. No code
 *     path here constructs NaN via the NAN macro or get_float*_nan(), so
 *     the geo-cmp canonical-NaN shim is not needed (NaN only PROPAGATES
 *     from harness inputs, which CBMC models correctly).
 *
 * Function bodies between the arg-fetch lines and the returns are verbatim.
 * Postgres compiles with -fwrapv; CBMC's two's-complement wrap matches
 * (build.rs also passes -fwrapv for the native tier).
 */

#include "../../support/c/pg_proof_shim.h"
#include <math.h>

typedef int64 Timestamp;
typedef int64 TimestampTz;
typedef int64 TimeADT;
typedef int64 TimeOffset;
typedef int32 DateADT;
typedef int32 fsec_t;
typedef double float8;

#define INT64CONST(x) INT64_C(x)
#define PG_INT32_MIN (-0x7FFFFFFF-1)
#define PG_INT32_MAX (0x7FFFFFFF)
#define PG_INT64_MIN (-INT64CONST(0x7FFFFFFFFFFFFFFF) - 1)
#define PG_INT64_MAX INT64CONST(0x7FFFFFFFFFFFFFFF)

/* ---- datatype/timestamp.h constants (verbatim) ---- */
#define MONTHS_PER_YEAR 12
#define DAYS_PER_MONTH	30
#define HOURS_PER_DAY	24
#define SECS_PER_DAY	86400
#define SECS_PER_HOUR	3600
#define SECS_PER_MINUTE 60
#define MINS_PER_HOUR	60
#define USECS_PER_DAY	INT64CONST(86400000000)
#define USECS_PER_HOUR	INT64CONST(3600000000)
#define USECS_PER_MINUTE INT64CONST(60000000)
#define USECS_PER_SEC	INT64CONST(1000000)

#define MAX_TIMESTAMP_PRECISION 6
#define MAX_INTERVAL_PRECISION 6
#define MAX_TIME_PRECISION 6

#define TS_PREC_INV 1000000.0
#define TSROUND(j) (rint(((double) (j)) * TS_PREC_INV) / TS_PREC_INV)

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

#define POSTGRES_EPOCH_JDATE	2451545 /* == date2j(2000, 1, 1) */
#define DATETIME_MIN_JULIAN (0)
#define DATE_END_JULIAN (2147483494)
#define TIMESTAMP_END_JULIAN (109203528)

#define MIN_TIMESTAMP	INT64CONST(-211813488000000000)
#define END_TIMESTAMP	INT64CONST(9223371331200000000)

#define IS_VALID_DATE(d) \
	((DATETIME_MIN_JULIAN - POSTGRES_EPOCH_JDATE) <= (d) && \
	 (d) < (DATE_END_JULIAN - POSTGRES_EPOCH_JDATE))
#define IS_VALID_TIMESTAMP(t)  (MIN_TIMESTAMP <= (t) && (t) < END_TIMESTAMP)

/* ---- utils/timestamp.h interval typmod codec (verbatim) ---- */
#define INTERVAL_MASK(b) (1 << (b))
#define INTERVAL_FULL_RANGE (0x7FFF)
#define INTERVAL_RANGE_MASK (0x7FFF)
#define INTERVAL_FULL_PRECISION (0xFFFF)
#define INTERVAL_PRECISION_MASK (0xFFFF)
#define INTERVAL_TYPMOD(p,r) ((((r) & INTERVAL_RANGE_MASK) << 16) | ((p) & INTERVAL_PRECISION_MASK))
#define INTERVAL_PRECISION(t) ((t) & INTERVAL_PRECISION_MASK)
#define INTERVAL_RANGE(t) (((t) >> 16) & INTERVAL_RANGE_MASK)

/* ---- datetime.h field/type tokens + DTERR codes (verbatim subset) ---- */
#define MONTH	1
#define YEAR	2
#define DAY		3
#define HOUR	10
#define MINUTE	11
#define SECOND	12
#define MILLISECOND 13
#define MICROSECOND 14
#define DOY		15

#define DTK_M(t)		(0x01 << (t))
#define DTK_ALL_SECS_M	(DTK_M(SECOND) | DTK_M(MILLISECOND) | DTK_M(MICROSECOND))
#define DTK_DATE_M		(DTK_M(YEAR) | DTK_M(MONTH) | DTK_M(DAY))

#define DTK_TZ			4
#define DTK_SECOND_V	18		/* datetime.h DTK_SECOND (renamed: SECOND
								 * above is the field-type token; both are
								 * verbatim values) */
#define DTK_MINUTE_V	19
#define DTK_HOUR_V		20
#define DTK_DAY_V		21
#define DTK_MONTH_V		23
#define DTK_QUARTER_V	24
#define DTK_YEAR_V		25
#define DTK_DECADE_V	26
#define DTK_CENTURY_V	27
#define DTK_MILLENNIUM_V 28
#define DTK_MILLISEC	29
#define DTK_MICROSEC	30
#define DTK_TZ_HOUR		34
#define DTK_TZ_MINUTE	35

#define DTERR_BAD_FORMAT		(-1)
#define DTERR_FIELD_OVERFLOW	(-2)
#define DTERR_MD_FIELD_OVERFLOW (-3)

#define isleap(y) (((y) % 4) == 0 && (((y) % 100) != 0 || ((y) % 400) == 0))

/* ---- common/int.h, __builtin arm (verbatim) ---- */
static inline bool
pg_add_s32_overflow(int32 a, int32 b, int32 *result)
{
	return __builtin_add_overflow(a, b, result);
}

static inline bool
pg_sub_s32_overflow(int32 a, int32 b, int32 *result)
{
	return __builtin_sub_overflow(a, b, result);
}

static inline bool
pg_neg_s32_overflow(int32 a, int32 *result)
{
	return __builtin_sub_overflow(0, a, result);
}

static inline bool
pg_add_s64_overflow(int64 a, int64 b, int64 *result)
{
	return __builtin_add_overflow(a, b, result);
}

static inline bool
pg_sub_s64_overflow(int64 a, int64 b, int64 *result)
{
	return __builtin_sub_overflow(a, b, result);
}

static inline bool
pg_mul_s64_overflow(int64 a, int64 b, int64 *result)
{
	return __builtin_mul_overflow(a, b, result);
}

/* ---- c.h float->int fit checks (verbatim) ---- */
#define FLOAT8_FITS_IN_INT32(num) \
	((num) >= (float8) PG_INT32_MIN && (num) < -((float8) PG_INT32_MIN))
#define FLOAT8_FITS_IN_INT64(num) \
	((num) >= (float8) PG_INT64_MIN && (num) < -((float8) PG_INT64_MIN))

/* ---- structs ---- */
struct pg_tm
{
	int			tm_sec;
	int			tm_min;
	int			tm_hour;
	int			tm_mday;
	int			tm_mon;
	int			tm_year;
	int			tm_yday;
};

typedef struct Interval
{
	TimeOffset	time;
	int32		day;
	int32		month;
} Interval;

typedef struct TimeTzADT
{
	TimeADT		time;
	int32		zone;
} TimeTzADT;

/* timestamp.c IntervalAggState (verbatim) */
typedef struct IntervalAggState
{
	int64		N;
	Interval	sumX;
	int64		pInfcount;
	int64		nInfcount;
} IntervalAggState;

#define IA_TOTAL_COUNT(ia) \
	((ia)->N + (ia)->pInfcount + (ia)->nInfcount)

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

/* ---- out-of-plane trap (wave-6 plane-fencing pattern) ---- */
static int pg_out_of_plane = 0;

int
pg_reset_out_of_plane(void)
{
	pg_out_of_plane = 0;
	return 0;
}

int
pg_out_of_plane_reached(void)
{
	return pg_out_of_plane;
}

/* ---------- datetime.c: day_tab (verbatim) ---------- */
static const int day_tab[2][13] =
{
	{31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31, 0},
	{31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31, 0}
};

/* ---------- datetime.c: date2j (verbatim) ---------- */
int
pg_date2j(int year, int month, int day)
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
}

/*
 * datetime.c ValidateDate (verbatim). The DOY arm's
 * j2date(date2j(...), ...) walk is out of every plane in this family
 * (fmask is always the literal DTK_DATE_M): rewired to the out-of-plane
 * trap so a folding failure FAILS loudly.
 */
int
pg_validate_date(int fmask, bool isjulian, bool is2digits, bool bc,
				 struct pg_tm *tm)
{
	if (fmask & DTK_M(YEAR))
	{
		if (isjulian)
		{
			/* tm_year is correct and should not be touched */
		}
		else if (bc)
		{
			/* there is no year zero in AD/BC notation */
			if (tm->tm_year <= 0)
				return DTERR_FIELD_OVERFLOW;
			/* internally, we represent 1 BC as year zero, 2 BC as -1, etc */
			tm->tm_year = -(tm->tm_year - 1);
		}
		else if (is2digits)
		{
			/* process 1 or 2-digit input as 1970-2069 AD, allow '0' and '00' */
			if (tm->tm_year < 0)	/* just paranoia */
				return DTERR_FIELD_OVERFLOW;
			if (tm->tm_year < 70)
				tm->tm_year += 2000;
			else if (tm->tm_year < 100)
				tm->tm_year += 1900;
		}
		else
		{
			/* there is no year zero in AD/BC notation */
			if (tm->tm_year <= 0)
				return DTERR_FIELD_OVERFLOW;
		}
	}

	/* now that we have correct year, decode DOY */
	if (fmask & DTK_M(DOY))
	{
		/* shim: j2date(date2j(...)) walk out of plane (see header) */
		pg_out_of_plane = 1;
		return DTERR_BAD_FORMAT;
	}

	/* check for valid month */
	if (fmask & DTK_M(MONTH))
	{
		if (tm->tm_mon < 1 || tm->tm_mon > MONTHS_PER_YEAR)
			return DTERR_MD_FIELD_OVERFLOW;
	}

	/* minimal check for valid day */
	if (fmask & DTK_M(DAY))
	{
		if (tm->tm_mday < 1 || tm->tm_mday > 31)
			return DTERR_MD_FIELD_OVERFLOW;
	}

	if ((fmask & DTK_DATE_M) == DTK_DATE_M)
	{
		/*
		 * Check for valid day of month, now that we know for sure the month
		 * and year.  Note we don't use MD_FIELD_OVERFLOW here, since it seems
		 * unlikely that "Feb 29" is a YMD-order error.
		 */
		if (tm->tm_mday > day_tab[isleap(tm->tm_year)][tm->tm_mon - 1])
			return DTERR_FIELD_OVERFLOW;
	}

	return 0;
}

/* ---------- date.c: float_time_overflows (verbatim) ---------- */
bool
pg_float_time_overflows(int hour, int min, double sec)
{
	/* Range-check the fields individually. */
	if (hour < 0 || hour > HOURS_PER_DAY ||
		min < 0 || min >= MINS_PER_HOUR)
		return true;

	/*
	 * "sec", being double, requires extra care.  Cope with NaN, and round off
	 * before applying the range check to avoid unexpected errors due to
	 * imprecise input.  (We assume rint() behaves sanely with infinities.)
	 */
	if (isnan(sec))
		return true;
	sec = rint(sec * USECS_PER_SEC);
	if (sec < 0 || sec > SECS_PER_MINUTE * USECS_PER_SEC)
		return true;

	/*
	 * Because we allow, eg, hour = 24 or sec = 60, we must check separately
	 * that the total time value doesn't exceed 24:00:00.  This must match the
	 * way that callers will convert the fields to a time.
	 */
	if (((((hour * MINS_PER_HOUR + min) * SECS_PER_MINUTE)
		  * USECS_PER_SEC) + (int64) sec) > USECS_PER_DAY)
		return true;

	return false;
}

/* ---------- date.c: AdjustTimeForTypmod (verbatim) ---------- */
void
pg_adjust_time_for_typmod(TimeADT *time, int32 typmod)
{
	static const int64 TimeScales[MAX_TIME_PRECISION + 1] = {
		INT64CONST(1000000),
		INT64CONST(100000),
		INT64CONST(10000),
		INT64CONST(1000),
		INT64CONST(100),
		INT64CONST(10),
		INT64CONST(1)
	};

	static const int64 TimeOffsets[MAX_TIME_PRECISION + 1] = {
		INT64CONST(500000),
		INT64CONST(50000),
		INT64CONST(5000),
		INT64CONST(500),
		INT64CONST(50),
		INT64CONST(5),
		INT64CONST(0)
	};

	if (typmod >= 0 && typmod <= MAX_TIME_PRECISION)
	{
		if (*time >= INT64CONST(0))
			*time = ((*time + TimeOffsets[typmod]) / TimeScales[typmod]) *
				TimeScales[typmod];
		else
			*time = -((((-*time) + TimeOffsets[typmod]) / TimeScales[typmod]) *
					  TimeScales[typmod]);
	}
}

/* ---------- date.c: time_scale (fmgr shim; body verbatim) ---------- */
int
pg_time_scale(int64 time, int32 typmod, int64 *out)
{
	TimeADT		result;

	result = time;
	pg_adjust_time_for_typmod(&result, typmod);

	*out = result;
	return 0;
}

/* ---------- date.c: timetz_scale (fmgr + palloc shim; body verbatim) ---- */
int
pg_timetz_scale(int64 t_time, int32 t_zone, int32 typmod,
				int64 *rt, int32 *rz)
{
	TimeTzADT	timev;
	TimeTzADT	resultv;
	TimeTzADT  *time = &timev;
	TimeTzADT  *result = &resultv;

	timev.time = t_time;
	timev.zone = t_zone;

	result->time = time->time;
	result->zone = time->zone;

	pg_adjust_time_for_typmod(&(result->time), typmod);

	*rt = result->time;
	*rz = result->zone;
	return 0;
}

/* ---------- date.c: make_date (fmgr shim; body verbatim; both error arms
 * are sqlstate 22008 -> single err flag) ---------- */
int
pg_make_date(int32 in_year, int32 in_mon, int32 in_mday,
			 int32 *out, int *err)
{
	struct pg_tm tm;
	DateADT		date;
	int			dterr;
	bool		bc = false;

	tm.tm_year = in_year;
	tm.tm_mon = in_mon;
	tm.tm_mday = in_mday;
	tm.tm_yday = 0;

	/* Handle negative years as BC */
	if (tm.tm_year < 0)
	{
		int			year = tm.tm_year;

		bc = true;
		if (pg_neg_s32_overflow(year, &year))
		{
			PROOF_EREPORT_FLAG(err);	/* 22008 date field value out of range */
			return 0;
		}
		tm.tm_year = year;
	}

	dterr = pg_validate_date(DTK_DATE_M, false, false, bc, &tm);

	if (dterr != 0)
	{
		PROOF_EREPORT_FLAG(err);	/* 22008 date field value out of range */
		return 0;
	}

	/* Prevent overflow in Julian-day routines */
	if (!IS_VALID_JULIAN(tm.tm_year, tm.tm_mon, tm.tm_mday))
	{
		PROOF_EREPORT_FLAG(err);	/* 22008 date out of range */
		return 0;
	}

	date = pg_date2j(tm.tm_year, tm.tm_mon, tm.tm_mday) - POSTGRES_EPOCH_JDATE;

	/* Now check for just-out-of-range dates */
	if (!IS_VALID_DATE(date))
	{
		PROOF_EREPORT_FLAG(err);	/* 22008 date out of range */
		return 0;
	}

	*out = date;
	return 0;
}

/* ---------- date.c: make_time (fmgr shim; body verbatim) ---------- */
int
pg_make_time(int32 tm_hour, int32 tm_min, double sec, int64 *out, int *err)
{
	TimeADT		time;

	/* Check for time overflow */
	if (pg_float_time_overflows(tm_hour, tm_min, sec))
	{
		PROOF_EREPORT_FLAG(err);	/* 22008 time field value out of range */
		return 0;
	}

	/* This should match tm2time */
	time = (((tm_hour * MINS_PER_HOUR + tm_min) * SECS_PER_MINUTE)
			* USECS_PER_SEC) + (int64) rint(sec * USECS_PER_SEC);

	*out = time;
	return 0;
}

/* ---------- timestamp.c: make_timestamp_internal + make_timestamp
 * (fmgr shim; bodies verbatim; every error arm is 22008 -> one flag) ---- */
static Timestamp
pg_make_timestamp_internal(int year, int month, int day,
						   int hour, int min, double sec, int *err)
{
	struct pg_tm tm;
	TimeOffset	date;
	TimeOffset	time;
	int			dterr;
	bool		bc = false;
	Timestamp	result;

	tm.tm_year = year;
	tm.tm_mon = month;
	tm.tm_mday = day;
	tm.tm_yday = 0;

	/* Handle negative years as BC */
	if (tm.tm_year < 0)
	{
		bc = true;
		tm.tm_year = -tm.tm_year;
	}

	dterr = pg_validate_date(DTK_DATE_M, false, false, bc, &tm);

	if (dterr != 0)
	{
		PROOF_EREPORT_FLAG(err);	/* 22008 date field value out of range */
		return 0;
	}

	if (!IS_VALID_JULIAN(tm.tm_year, tm.tm_mon, tm.tm_mday))
	{
		PROOF_EREPORT_FLAG(err);	/* 22008 date out of range */
		return 0;
	}

	date = pg_date2j(tm.tm_year, tm.tm_mon, tm.tm_mday) - POSTGRES_EPOCH_JDATE;

	/* Check for time overflow */
	if (pg_float_time_overflows(hour, min, sec))
	{
		PROOF_EREPORT_FLAG(err);	/* 22008 time field value out of range */
		return 0;
	}

	/* This should match tm2time */
	time = (((hour * MINS_PER_HOUR + min) * SECS_PER_MINUTE)
			* USECS_PER_SEC) + (int64) rint(sec * USECS_PER_SEC);

	if (unlikely(pg_mul_s64_overflow(date, USECS_PER_DAY, &result) ||
				 pg_add_s64_overflow(result, time, &result)))
	{
		PROOF_EREPORT_FLAG(err);	/* 22008 timestamp out of range */
		return 0;
	}

	/* final range check catches just-out-of-range timestamps */
	if (!IS_VALID_TIMESTAMP(result))
	{
		PROOF_EREPORT_FLAG(err);	/* 22008 timestamp out of range */
		return 0;
	}

	return result;
}

int
pg_make_timestamp(int32 year, int32 month, int32 mday,
				  int32 hour, int32 min, double sec,
				  int64 *out, int *err)
{
	Timestamp	result;

	result = pg_make_timestamp_internal(year, month, mday, hour, min, sec, err);
	if (*err)					/* shim: longjmp propagation */
		return 0;

	*out = result;
	return 0;
}

/* ---------- timestamp.c: intervaltypmodin (ArrayGetIntegerTypmods ->
 * (tl,n) params; body verbatim; ERROR arms are all 22023 -> one flag;
 * WARNING arm dropped per header) ---------- */
int
pg_intervaltypmodin(const int32 *tl, int n, int32 *out, int *err)
{
	int32		typmod;

	/*
	 * tl[0] - interval range (fields bitmask)	tl[1] - precision (optional)
	 *
	 * Note we must validate tl[0] even though it's normally guaranteed
	 * correct by the grammar --- consider SELECT 'foo'::"interval"(1000).
	 */
	if (n > 0)
	{
		switch (tl[0])
		{
			case INTERVAL_MASK(YEAR):
			case INTERVAL_MASK(MONTH):
			case INTERVAL_MASK(DAY):
			case INTERVAL_MASK(HOUR):
			case INTERVAL_MASK(MINUTE):
			case INTERVAL_MASK(SECOND):
			case INTERVAL_MASK(YEAR) | INTERVAL_MASK(MONTH):
			case INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR):
			case INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE):
			case INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND):
			case INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE):
			case INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND):
			case INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND):
			case INTERVAL_FULL_RANGE:
				/* all OK */
				break;
			default:
				PROOF_EREPORT_FLAG(err);	/* 22023 invalid INTERVAL type modifier */
				return 0;
		}
	}

	if (n == 1)
	{
		if (tl[0] != INTERVAL_FULL_RANGE)
			typmod = INTERVAL_TYPMOD(INTERVAL_FULL_PRECISION, tl[0]);
		else
			typmod = -1;
	}
	else if (n == 2)
	{
		if (tl[1] < 0)
		{
			PROOF_EREPORT_FLAG(err);	/* 22023 precision must not be negative */
			return 0;
		}
		if (tl[1] > MAX_INTERVAL_PRECISION)
		{
			/*
			 * shim: ereport(WARNING, "INTERVAL(%d) precision reduced to
			 * maximum allowed, %d") dropped — WARNING emission out of proof
			 * both sides (see header).
			 */
			typmod = INTERVAL_TYPMOD(MAX_INTERVAL_PRECISION, tl[0]);
		}
		else
			typmod = INTERVAL_TYPMOD(tl[1], tl[0]);
	}
	else
	{
		PROOF_EREPORT_FLAG(err);	/* 22023 invalid INTERVAL type modifier */
		return 0;
	}

	*out = typmod;
	return 0;
}

/* snprintf shims for intervaltypmodout (see header): fixed 64-byte buffer,
 * nonnegative %d (precision is masked to 0..65535). Return emitted length. */
static int
pg_proof_emit_str(char *res, const char *fieldstr)
{
	int			i = 0;

	while (fieldstr[i] != '\0')
	{
		res[i] = fieldstr[i];
		i++;
	}
	res[i] = '\0';
	return i;
}

static int
pg_proof_emit_str_paren_int(char *res, const char *fieldstr, int32 precision)
{
	char		digits[10];
	int			n = 0;
	int			len;
	int			i;
	uint32		p = (uint32) precision;

	len = pg_proof_emit_str(res, fieldstr);
	res[len++] = '(';
	do
	{
		digits[n++] = (char) ('0' + (p % 10));
		p /= 10;
	} while (p != 0);
	for (i = n - 1; i >= 0; i--)
		res[len++] = digits[i];
	res[len++] = ')';
	res[len] = '\0';
	return len;
}

/* ---------- timestamp.c: intervaltypmodout (fmgr + palloc shim: caller
 * 64-byte buffer; body verbatim; elog(ERROR) -> flag) ---------- */
int
pg_intervaltypmodout(int32 typmod, char *res, int *err)
{
	int			fields;
	int			precision;
	const char *fieldstr;

	if (typmod < 0)
	{
		*res = '\0';
		return 0;
	}

	fields = INTERVAL_RANGE(typmod);
	precision = INTERVAL_PRECISION(typmod);

	switch (fields)
	{
		case INTERVAL_MASK(YEAR):
			fieldstr = " year";
			break;
		case INTERVAL_MASK(MONTH):
			fieldstr = " month";
			break;
		case INTERVAL_MASK(DAY):
			fieldstr = " day";
			break;
		case INTERVAL_MASK(HOUR):
			fieldstr = " hour";
			break;
		case INTERVAL_MASK(MINUTE):
			fieldstr = " minute";
			break;
		case INTERVAL_MASK(SECOND):
			fieldstr = " second";
			break;
		case INTERVAL_MASK(YEAR) | INTERVAL_MASK(MONTH):
			fieldstr = " year to month";
			break;
		case INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR):
			fieldstr = " day to hour";
			break;
		case INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE):
			fieldstr = " day to minute";
			break;
		case INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND):
			fieldstr = " day to second";
			break;
		case INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE):
			fieldstr = " hour to minute";
			break;
		case INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND):
			fieldstr = " hour to second";
			break;
		case INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND):
			fieldstr = " minute to second";
			break;
		case INTERVAL_FULL_RANGE:
			fieldstr = "";
			break;
		default:
			PROOF_EREPORT_FLAG(err);	/* elog(ERROR, "invalid INTERVAL typmod: 0x%x") */
			return 0;
	}

	if (precision != INTERVAL_FULL_PRECISION)
		return pg_proof_emit_str_paren_int(res, fieldstr, precision);
	else
		return pg_proof_emit_str(res, fieldstr);
}

/* ---------- timestamp.c: interval_um_internal (verbatim) ---------- */
static void
pg_interval_um_internal(const Interval *interval, Interval *result, int *err)
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
		{
			PROOF_EREPORT_FLAG(err);	/* 22008 interval out of range */
			return;
		}
	}
}

/* ---------- timestamp.c: finite_interval_pl / finite_interval_mi
 * (verbatim; ereport -> flag + return) ---------- */
static void
pg_finite_interval_pl(const Interval *span1, const Interval *span2,
					  Interval *result, int *err)
{
	if (pg_add_s32_overflow(span1->month, span2->month, &result->month) ||
		pg_add_s32_overflow(span1->day, span2->day, &result->day) ||
		pg_add_s64_overflow(span1->time, span2->time, &result->time) ||
		INTERVAL_NOT_FINITE(result))
	{
		PROOF_EREPORT_FLAG(err);	/* 22008 interval out of range */
		return;
	}
}

static void
pg_finite_interval_mi(const Interval *span1, const Interval *span2,
					  Interval *result, int *err)
{
	if (pg_sub_s32_overflow(span1->month, span2->month, &result->month) ||
		pg_sub_s32_overflow(span1->day, span2->day, &result->day) ||
		pg_sub_s64_overflow(span1->time, span2->time, &result->time) ||
		INTERVAL_NOT_FINITE(result))
	{
		PROOF_EREPORT_FLAG(err);	/* 22008 interval out of range */
		return;
	}
}

/* ---------- timestamp.c: do_interval_accum (verbatim) ---------- */
int
pg_do_interval_accum(IntervalAggState *state, const Interval *newval, int *err)
{
	/* Infinite inputs are counted separately, and do not affect "N" */
	if (INTERVAL_IS_NOBEGIN(newval))
	{
		state->nInfcount++;
		return 0;
	}

	if (INTERVAL_IS_NOEND(newval))
	{
		state->pInfcount++;
		return 0;
	}

	pg_finite_interval_pl(&state->sumX, newval, &state->sumX, err);
	if (*err)					/* shim: longjmp propagation */
		return 0;
	state->N++;
	return 0;
}

/* ---------- timestamp.c: do_interval_discard (verbatim) ---------- */
int
pg_do_interval_discard(IntervalAggState *state, const Interval *newval, int *err)
{
	/* Infinite inputs are counted separately, and do not affect "N" */
	if (INTERVAL_IS_NOBEGIN(newval))
	{
		state->nInfcount--;
		return 0;
	}

	if (INTERVAL_IS_NOEND(newval))
	{
		state->pInfcount--;
		return 0;
	}

	/* Handle the to-be-discarded finite value. */
	state->N--;
	if (state->N > 0)
	{
		pg_finite_interval_mi(&state->sumX, newval, &state->sumX, err);
		if (*err)				/* shim: longjmp propagation */
			return 0;
	}
	else
	{
		/* All values discarded, reset the state */
		state->sumX.time = 0;	/* shim: memset(&state->sumX, 0, ...) spelled
								 * field-wise (no libc memset model needed) */
		state->sumX.day = 0;
		state->sumX.month = 0;
	}
	return 0;
}

/* ---------- timestamp.c: interval_avg_combine, both-states-non-null path
 * (verbatim; the state2==NULL / state1==NULL arms are fcinfo-null plumbing
 * handled in the shipped Rust wrapper and are not part of this core
 * theorem) ---------- */
int
pg_interval_avg_combine(IntervalAggState *state1, const IntervalAggState *state2,
						int *err)
{
	state1->N += state2->N;
	state1->pInfcount += state2->pInfcount;
	state1->nInfcount += state2->nInfcount;

	/* Accumulate finite interval values, if any. */
	if (state2->N > 0)
	{
		pg_finite_interval_pl(&state1->sumX, &state2->sumX, &state1->sumX, err);
		if (*err)				/* shim: longjmp propagation */
			return 0;
	}
	return 0;
}

/* ---------- timestamp.c: interval_div (fmgr shim; body verbatim;
 * ereport/goto out_of_range -> flag; used by pg_interval_avg's mean arm
 * and the native differential tier) ---------- */
void
pg_interval_div_raw(const Interval *span, double factor, Interval *result, int *err)
{
	double		month_remainder_days,
				sec_remainder,
				result_double;
	int32		orig_month = span->month,
				orig_day = span->day;

	if (factor == 0.0)
	{
		PROOF_EREPORT_FLAG(err);	/* 22012 division by zero */
		*err = 2;				/* distinct flag: 22012, not 22008 */
		return;
	}

	if (isnan(factor))
		goto out_of_range;

	if (INTERVAL_NOT_FINITE(span))
	{
		if (isinf(factor))
			goto out_of_range;

		if (factor < 0.0)
		{
			pg_interval_um_internal(span, result, err);
			if (*err)			/* shim: longjmp propagation */
				return;
		}
		else
			*result = *span;	/* shim: memcpy(result, span, sizeof) */

		return;
	}

	result_double = span->month / factor;
	if (isnan(result_double) || !FLOAT8_FITS_IN_INT32(result_double))
		goto out_of_range;
	result->month = (int32) result_double;

	result_double = span->day / factor;
	if (isnan(result_double) || !FLOAT8_FITS_IN_INT32(result_double))
		goto out_of_range;
	result->day = (int32) result_double;

	/*
	 * Fractional months full days into days.  See comment in interval_mul().
	 */
	month_remainder_days = (orig_month / factor - result->month) * DAYS_PER_MONTH;
	month_remainder_days = TSROUND(month_remainder_days);
	sec_remainder = (orig_day / factor - result->day +
					 month_remainder_days - (int) month_remainder_days) * SECS_PER_DAY;
	sec_remainder = TSROUND(sec_remainder);
	if (fabs(sec_remainder) >= SECS_PER_DAY)
	{
		if (pg_add_s32_overflow(result->day,
								(int) (sec_remainder / SECS_PER_DAY),
								&result->day))
			goto out_of_range;
		sec_remainder -= (int) (sec_remainder / SECS_PER_DAY) * SECS_PER_DAY;
	}

	/* cascade units down */
	if (pg_add_s32_overflow(result->day, (int32) month_remainder_days,
							&result->day))
		goto out_of_range;
	result_double = rint(span->time / factor + sec_remainder * USECS_PER_SEC);
	if (isnan(result_double) || !FLOAT8_FITS_IN_INT64(result_double))
		goto out_of_range;
	result->time = (int64) result_double;

	return;

out_of_range:
	PROOF_EREPORT_FLAG(err);	/* 22008 interval out of range */
}

/* ---------- timestamp.c: interval_avg (fmgr shim: state by pointer,
 * PG_RETURN_NULL -> *isnull, DirectFunctionCall2(interval_div) -> direct
 * call; body verbatim) ---------- */
int
pg_interval_avg(const IntervalAggState *state, Interval *result,
				int *isnull, int *err)
{
	/* If there were no non-null inputs, return NULL */
	if (state == NULL || IA_TOTAL_COUNT(state) == 0)
	{
		*isnull = 1;
		return 0;
	}

	/*
	 * Aggregating infinities that all have the same sign produces infinity
	 * with that sign.  Aggregating infinities with different signs results in
	 * an error.
	 */
	if (state->pInfcount > 0 || state->nInfcount > 0)
	{
		if (state->pInfcount > 0 && state->nInfcount > 0)
		{
			PROOF_EREPORT_FLAG(err);	/* 22008 interval out of range */
			return 0;
		}

		if (state->pInfcount > 0)
			INTERVAL_NOEND(result);
		else
			INTERVAL_NOBEGIN(result);

		return 0;
	}

	pg_interval_div_raw(&state->sumX, (double) state->N, result, err);
	return 0;
}

/* ---------- timestamp.c: interval_sum (fmgr shim as above; verbatim) ---- */
int
pg_interval_sum(const IntervalAggState *state, Interval *result,
				int *isnull, int *err)
{
	/* If there were no non-null inputs, return NULL */
	if (state == NULL || IA_TOTAL_COUNT(state) == 0)
	{
		*isnull = 1;
		return 0;
	}

	/*
	 * Aggregating infinities that all have the same sign produces infinity
	 * with that sign.  Aggregating infinities with different signs results in
	 * an error.
	 */
	if (state->pInfcount > 0 && state->nInfcount > 0)
	{
		PROOF_EREPORT_FLAG(err);	/* 22008 interval out of range */
		return 0;
	}

	if (state->pInfcount > 0)
		INTERVAL_NOEND(result);
	else if (state->nInfcount > 0)
		INTERVAL_NOBEGIN(result);
	else
		*result = state->sumX;	/* shim: memcpy(result, &state->sumX, sizeof) */
	return 0;
}

/* pqformat send shims for interval_avg_serialize: big-endian emission at a
 * cursor into a caller 40-byte buffer — exactly pq_sendint64/pq_sendint32's
 * wire bytes (pg_hton64/32 spelled as explicit shifts, per pg_bswap.h
 * semantics). */
static void
pg_proof_sendint64(uint8 *buf, int *cur, int64 v)
{
	uint64		u = (uint64) v;
	int			i;

	for (i = 0; i < 8; i++)
		buf[(*cur)++] = (uint8) (u >> (56 - 8 * i));
}

static void
pg_proof_sendint32(uint8 *buf, int *cur, int32 v)
{
	uint32		u = (uint32) v;
	int			i;

	for (i = 0; i < 4; i++)
		buf[(*cur)++] = (uint8) (u >> (24 - 8 * i));
}

/* ---------- timestamp.c: interval_avg_serialize (StringInfo -> 40-byte
 * caller buffer; AggCheckCallContext guard dropped per header; body
 * verbatim) ---------- */
int
pg_interval_avg_serialize(const IntervalAggState *state, uint8 *out40)
{
	int			cur = 0;

	/* N */
	pg_proof_sendint64(out40, &cur, state->N);

	/* sumX */
	pg_proof_sendint64(out40, &cur, state->sumX.time);
	pg_proof_sendint32(out40, &cur, state->sumX.day);
	pg_proof_sendint32(out40, &cur, state->sumX.month);

	/* pInfcount */
	pg_proof_sendint64(out40, &cur, state->pInfcount);

	/* nInfcount */
	pg_proof_sendint64(out40, &cur, state->nInfcount);
	return 0;
}

/* pqformat recv shims for interval_avg_deserialize: cursor reads over
 * (data,len); insufficient-data and pq_getmsgend trailing-data ereports
 * rewired to PROOF_EREPORT_FLAG, at the same program points. */
static int64
pg_proof_getmsgint64(const uint8 *data, int len, int *cur, int *err)
{
	uint64		u = 0;
	int			i;

	if (len - *cur < 8)
	{
		PROOF_EREPORT_FLAG(err);	/* insufficient data left in message */
		return 0;
	}
	for (i = 0; i < 8; i++)
		u = (u << 8) | data[(*cur)++];
	return (int64) u;
}

static int32
pg_proof_getmsgint32(const uint8 *data, int len, int *cur, int *err)
{
	uint32		u = 0;
	int			i;

	if (len - *cur < 4)
	{
		PROOF_EREPORT_FLAG(err);	/* insufficient data left in message */
		return 0;
	}
	for (i = 0; i < 4; i++)
		u = (u << 8) | data[(*cur)++];
	return (int32) u;
}

/* ---------- timestamp.c: interval_avg_deserialize (bytea/StringInfo ->
 * (data,len) + cursor; AggCheckCallContext guard dropped; body verbatim,
 * incl. the pq_getmsgend trailing-byte check) ---------- */
int
pg_interval_avg_deserialize(const uint8 *data, int len,
							IntervalAggState *result, int *err)
{
	int			cur = 0;

	/* N */
	result->N = pg_proof_getmsgint64(data, len, &cur, err);
	if (*err)					/* shim: longjmp propagation */
		return 0;

	/* sumX */
	result->sumX.time = pg_proof_getmsgint64(data, len, &cur, err);
	if (*err)					/* shim: longjmp propagation */
		return 0;
	result->sumX.day = pg_proof_getmsgint32(data, len, &cur, err);
	if (*err)					/* shim: longjmp propagation */
		return 0;
	result->sumX.month = pg_proof_getmsgint32(data, len, &cur, err);
	if (*err)					/* shim: longjmp propagation */
		return 0;

	/* pInfcount */
	result->pInfcount = pg_proof_getmsgint64(data, len, &cur, err);
	if (*err)					/* shim: longjmp propagation */
		return 0;

	/* nInfcount */
	result->nInfcount = pg_proof_getmsgint64(data, len, &cur, err);
	if (*err)					/* shim: longjmp propagation */
		return 0;

	/* pq_getmsgend(&buf) */
	if (cur != len)
	{
		PROOF_EREPORT_FLAG(err);	/* invalid message format (trailing data) */
		return 0;
	}
	return 0;
}

/* ---------- date.c: timetz2tm (verbatim) ---------- */
static int
pg_timetz2tm(const TimeTzADT *time, struct pg_tm *tm, fsec_t *fsec, int *tzp)
{
	TimeOffset	trem = time->time;

	tm->tm_hour = trem / USECS_PER_HOUR;
	trem -= tm->tm_hour * USECS_PER_HOUR;
	tm->tm_min = trem / USECS_PER_MINUTE;
	trem -= tm->tm_min * USECS_PER_MINUTE;
	tm->tm_sec = trem / USECS_PER_SEC;
	*fsec = trem - tm->tm_sec * USECS_PER_SEC;

	if (tzp != NULL)
		*tzp = time->zone;

	return 0;
}

/* ---------- date.c: timetz_part_common, type == UNITS, retnumeric=false
 * (decode prologue is a per-cell literal-selector seam per header; body
 * from the timetz2tm call to PG_RETURN_FLOAT8 verbatim; DTK_* value-token
 * spellings carry a _V suffix where datetime.h reuses a field-type name,
 * values verbatim) ---------- */
int
pg_timetz_part_units_float(int64 t_time, int32 t_zone, int32 val,
						   double *out, int *err)
{
	TimeTzADT	timev;
	TimeTzADT  *time = &timev;
	int64		intresult;

	{
		int			tz;
		fsec_t		fsec;
		struct pg_tm tt,
				   *tm = &tt;

		timev.time = t_time;
		timev.zone = t_zone;

		pg_timetz2tm(time, tm, &fsec, &tz);

		switch (val)
		{
			case DTK_TZ:
				intresult = -tz;
				break;

			case DTK_TZ_MINUTE:
				intresult = (-tz / SECS_PER_MINUTE) % MINS_PER_HOUR;
				break;

			case DTK_TZ_HOUR:
				intresult = -tz / SECS_PER_HOUR;
				break;

			case DTK_MICROSEC:
				intresult = tm->tm_sec * INT64CONST(1000000) + fsec;
				break;

			case DTK_MILLISEC:
				/* retnumeric=false arm verbatim */
				*out = tm->tm_sec * 1000.0 + fsec / 1000.0;
				return 0;

			case DTK_SECOND_V:
				/* retnumeric=false arm verbatim */
				*out = tm->tm_sec + fsec / 1000000.0;
				return 0;

			case DTK_MINUTE_V:
				intresult = tm->tm_min;
				break;

			case DTK_HOUR_V:
				intresult = tm->tm_hour;
				break;

			case DTK_DAY_V:
			case DTK_MONTH_V:
			case DTK_QUARTER_V:
			case DTK_YEAR_V:
			case DTK_DECADE_V:
			case DTK_CENTURY_V:
			case DTK_MILLENNIUM_V:
			default:
				PROOF_EREPORT_FLAG(err);	/* 0A000 unit not supported */
				return 0;
		}
	}

	*out = (double) intresult;	/* PG_RETURN_FLOAT8((float8) intresult) */
	return 0;
}

/* ==================== adt_date remainder (lane D) ====================
 *
 * Date/time/timetz/interval tail rows hosted in this crate per lane charter:
 *   1140 date_mi, 1373 date_finite, 1390 interval_finite, 1370 time_interval,
 *   1419 interval_time, 1690 time_mi_time, 2046 timetz_time,
 *   1308 overlaps_time, 1271 overlaps_timetz,
 *   6415/6416 hashdate[extended], 1688/3409 time_hash[_extended],
 *   1696/3410 timetz_hash[_extended], 1697/3418 interval_hash[_extended],
 *   2024 date_timestamp, 1272/2025 datetime_timestamp,
 *   1297/1359 datetimetz_timestamptz,
 *   2909/2911 time[tz]typmodin (core anytime_typmod_check),
 *   2910/2912 time[tz]typmodout (core anytime_typmodout),
 *   4133/4137/4138 in_range_{date,time,timetz}_interval,
 *   2038 timetz_izone, 1200 interval_scale,
 *   2468-2473/2478/2479 date/time/timetz/interval recv+send.
 *
 * Provenance:
 *   - src/backend/utils/adt/date.c       (anytime_typmod_check,
 *     anytime_typmodout, date_recv, date_send, hashdate, hashdateextended,
 *     date_finite, date_mi, date2timestamp_opt_overflow/date2timestamp,
 *     in_range_date_interval, time_recv, time_send, datetime_timestamp,
 *     time_interval, interval_time, time_mi_time, in_range_time_interval,
 *     timetz_recv, timetz_send, timetz_cmp_internal, timetz_hash,
 *     timetz_hash_extended, in_range_timetz_interval, overlaps_timetz,
 *     overlaps_time, timetz_time, datetimetz_timestamptz, timetz_izone)
 *   - src/backend/utils/adt/timestamp.c  (interval_finite, interval_hash,
 *     interval_hash_extended, interval_cmp_value, interval_sign,
 *     timestamp_pl_interval/timestamp_mi_interval [m0d0/noend planes],
 *     in_range_timestamp_interval, interval_recv, interval_send,
 *     interval_scale, AdjustIntervalForTypmod)
 *   - src/common/hashfn.c                (hash_bytes_uint32,
 *     hash_bytes_uint32_extended: rot/mix/final, bodies verbatim)
 *   - src/backend/access/hash/hashfunc.c (hashint8/hashint8extended int64
 *     fold — time_hash/timetz_hash/interval_hash route through it)
 *   ref: postgres/postgres REL_18_STABLE, fetched 2026-07-29
 *
 * Shims (plumbing only, never logic) — beyond the file-header conventions:
 *   - PROOF_EREPORT_FLAG convention extended with distinct flag values:
 *       1 = ereport(ERROR, 22008 datetime/interval value out of range)
 *       2 = ereport(ERROR, 22023 invalid parameter value)
 *       3 = ereport(ERROR, 22013 invalid preceding/following size)
 *       5 = ereport(ERROR, 22009 time zone displacement out of range)
 *       4 = elog(ERROR, "unrecognized interval typmod") [internal XX000]
 *       99 = OUT-OF-PLANE TRAP (julian month/day walk in
 *            timestamp_pl_interval; must be dead on every plane; loud)
 *   - anytime_typmod_check: ereport(WARNING, precision reduced) DROPPED
 *     (comment kept); the clamped VALUE path stays verbatim. Rust side
 *     stubs elog::message_level_is_interesting (intervaltypmodin
 *     precedent), so WARNING emission is out of proof on BOTH sides.
 *   - anytime_typmodout: psprintf("(%d)%s") -> pg_adr_emit_paren_int_str
 *     (fixed caller buffer, C variadics unsupported); pstrdup(tz) ->
 *     suffix copy. Returns emitted length.
 *   - overlaps_time/overlaps_timetz: PG_GETARG_DATUM/PG_ARGISNULL ->
 *     value+flag pairs; PG_RETURN_NULL -> return 1 (*result untouched);
 *     PG_RETURN_BOOL(x) -> *result = x, return 0. TIMETZ_GT/LT
 *     (DirectFunctionCall2 of timetz_gt/timetz_lt) -> vendored verbatim
 *     timetz_cmp_internal comparisons; TIME GT/LT -> int64 compares
 *     (== the proved eq_time_* comparators).
 *   - hash rows: DirectFunctionCall1(hashint8, ..) / hash_uint32(..) ->
 *     direct calls of the vendored fold + hash_bytes_uint32 (fmgr
 *     dispatch removed, bodies verbatim).
 *   - interval_hash: int128_to_int64(span) -> (int64) cast of native
 *     __int128 (int128.h native arm semantics).
 *   - timetz_izone: the two DatumGetCString(DirectFunctionCall1(
 *     interval_out, ..)) calls are message text only -> flag 2/flag 2.
 *   - recv rows: StringInfo -> (data,len) cursor reads via the existing
 *     pg_proof_getmsgint64/32 statics (exact pqformat wire semantics);
 *     send rows: pq_begintypsend 4-byte varlena header (LE, len<<2) +
 *     pq_sendint64/32 BE emission via pg_proof_sendint64/32.
 *   - interval_scale: palloc'd result -> caller out-params; escontext is
 *     NULL at this call site so ereturn == ereport -> flags.
 *   - timestamp_pl_interval month!=0 / day!=0 julian arms -> OUT-OF-PLANE
 *     TRAP 99 (wave-6/7 plane-fencing pattern); reachable only from
 *     in_range_date_interval planes that pin od == om == 0 or the literal
 *     NOEND offset (infinity shortcut precedes the arms).
 *
 * Function bodies between arg-fetch and returns are verbatim.
 */

typedef __int128 INT128;

#define DATEVAL_NOBEGIN ((DateADT) PG_INT32_MIN)
#define DATEVAL_NOEND	((DateADT) PG_INT32_MAX)
#define DATE_IS_NOBEGIN(j) ((j) == DATEVAL_NOBEGIN)
#define DATE_IS_NOEND(j) ((j) == DATEVAL_NOEND)
#define DATE_NOT_FINITE(j) (DATE_IS_NOBEGIN(j) || DATE_IS_NOEND(j))

#define DT_NOBEGIN PG_INT64_MIN
#define DT_NOEND PG_INT64_MAX
#define TIMESTAMP_NOBEGIN(j) do {(j) = DT_NOBEGIN;} while (0)
#define TIMESTAMP_IS_NOBEGIN(j) ((j) == DT_NOBEGIN)
#define TIMESTAMP_NOEND(j) do {(j) = DT_NOEND;} while (0)
#define TIMESTAMP_IS_NOEND(j) ((j) == DT_NOEND)
#define TIMESTAMP_NOT_FINITE(j) (TIMESTAMP_IS_NOBEGIN(j) || TIMESTAMP_IS_NOEND(j))

#define MAX_TZDISP_HOUR		15
#define TZDISP_LIMIT		((MAX_TZDISP_HOUR + 1) * SECS_PER_HOUR)

/* ---------- common/hashfn.c: hash_bytes_uint32[_extended] (verbatim) ----- */

static inline uint32
pg_adr_rotate_left32(uint32 word, int n)
{
	return (word << n) | (word >> (32 - n));
}

#define adr_rot(x,k) pg_adr_rotate_left32(x, k)

#define adr_mix(a,b,c) \
{ \
  a -= c;  a ^= adr_rot(c, 4);	c += b; \
  b -= a;  b ^= adr_rot(a, 6);	a += c; \
  c -= b;  c ^= adr_rot(b, 8);	b += a; \
  a -= c;  a ^= adr_rot(c,16);	c += b; \
  b -= a;  b ^= adr_rot(a,19);	a += c; \
  c -= b;  c ^= adr_rot(b, 4);	b += a; \
}

#define adr_final(a,b,c) \
{ \
  c ^= b; c -= adr_rot(b,14); \
  a ^= c; a -= adr_rot(c,11); \
  b ^= a; b -= adr_rot(a,25); \
  c ^= b; c -= adr_rot(b,16); \
  a ^= c; a -= adr_rot(c, 4); \
  b ^= a; b -= adr_rot(a,14); \
  c ^= b; c -= adr_rot(b,24); \
}

static uint32
pg_adr_hash_bytes_uint32(uint32 k)
{
	uint32		a,
				b,
				c;

	a = b = c = 0x9e3779b9 + (uint32) sizeof(uint32) + 3923095;
	a += k;

	adr_final(a, b, c);

	return c;
}

static uint64
pg_adr_hash_bytes_uint32_extended(uint32 k, uint64 seed)
{
	uint32		a,
				b,
				c;

	a = b = c = 0x9e3779b9 + (uint32) sizeof(uint32) + 3923095;

	if (seed != 0)
	{
		a += (uint32) (seed >> 32);
		b += (uint32) seed;
		adr_mix(a, b, c);
	}

	a += k;

	adr_final(a, b, c);

	return ((uint64) b << 32) | c;
}

/* hashfunc.c hashint8 / hashint8extended (verbatim fold) */
static uint32
pg_adr_hashint8(int64 val)
{
	uint32		lohalf = (uint32) val;
	uint32		hihalf = (uint32) (val >> 32);

	lohalf ^= (val >= 0) ? hihalf : ~hihalf;

	return pg_adr_hash_bytes_uint32(lohalf);
}

static uint64
pg_adr_hashint8_extended(int64 val, uint64 seed)
{
	uint32		lohalf = (uint32) val;
	uint32		hihalf = (uint32) (val >> 32);

	lohalf ^= (val >= 0) ? hihalf : ~hihalf;

	return pg_adr_hash_bytes_uint32_extended(lohalf, seed);
}

/* ---------- date.c: hashdate / hashdateextended ---------- */

uint32
pg_adr_hashdate(int32 date)
{
	return pg_adr_hash_bytes_uint32((uint32) date);
}

uint64
pg_adr_hashdate_extended(int32 date, uint64 seed)
{
	return pg_adr_hash_bytes_uint32_extended((uint32) date, seed);
}

/* ---------- date.c: time_hash rows == hashint8 on the time value -------- */

uint32
pg_adr_time_hash(int64 t)
{
	return pg_adr_hashint8(t);
}

uint64
pg_adr_time_hash_extended(int64 t, uint64 seed)
{
	return pg_adr_hashint8_extended(t, seed);
}

/* ---------- date.c: timetz_hash / timetz_hash_extended (verbatim) ------- */

uint32
pg_adr_timetz_hash(int64 t_time, int32 t_zone)
{
	TimeTzADT	key_ = {t_time, t_zone};
	TimeTzADT  *key = &key_;
	uint32		thash;

	/*
	 * To avoid any problems with padding bytes in the struct, we figure the
	 * field hashes separately and XOR them.
	 */
	thash = pg_adr_hashint8(key->time);
	thash ^= pg_adr_hash_bytes_uint32((uint32) key->zone);
	return thash;
}

uint64
pg_adr_timetz_hash_extended(int64 t_time, int32 t_zone, uint64 seed)
{
	TimeTzADT	key_ = {t_time, t_zone};
	TimeTzADT  *key = &key_;
	uint64		thash;

	/* Same approach as timetz_hash */
	thash = pg_adr_hashint8_extended(key->time, seed);
	thash ^= pg_adr_hash_bytes_uint32_extended((uint32) key->zone,
											   (uint64) seed);
	return thash;
}

/* ---------- timestamp.c: interval_cmp_value / interval_sign (verbatim,
 * int128.h native-arm semantics) ---------- */

static INT128
pg_adr_interval_cmp_value(const Interval *interval)
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
	span = (INT128) interval->time;

	/* Scale up days to microseconds, forming a 128-bit product */
	span += (INT128) days * USECS_PER_DAY;

	return span;
}

static int
pg_adr_interval_sign(const Interval *interval)
{
	INT128		span = pg_adr_interval_cmp_value(interval);

	if (span < 0)
		return -1;
	if (span > 0)
		return 1;
	return 0;
}

/* ---------- timestamp.c: interval_hash / interval_hash_extended --------- */

uint32
pg_adr_interval_hash(int64 t, int32 d, int32 m)
{
	Interval	interval_ = {t, d, m};
	Interval   *interval = &interval_;
	INT128		span = pg_adr_interval_cmp_value(interval);
	int64		span64;

	/*
	 * Use only the least significant 64 bits for hashing.  The upper 64 bits
	 * seldom add any useful information, and besides we must do it like this
	 * for compatibility with hashes calculated before use of INT128 was
	 * introduced.
	 */
	span64 = (int64) span;		/* int128_to_int64 */

	return pg_adr_hashint8(span64);
}

uint64
pg_adr_interval_hash_extended(int64 t, int32 d, int32 m, uint64 seed)
{
	Interval	interval_ = {t, d, m};
	Interval   *interval = &interval_;
	INT128		span = pg_adr_interval_cmp_value(interval);
	int64		span64;

	/* Same approach as interval_hash */
	span64 = (int64) span;		/* int128_to_int64 */

	return pg_adr_hashint8_extended(span64, seed);
}

/* ---------- date.c: date_finite / timestamp.c: interval_finite ---------- */

int
pg_adr_date_finite(int32 date)
{
	return !DATE_NOT_FINITE(date);
}

int
pg_adr_interval_finite(int64 t, int32 d, int32 m)
{
	Interval	interval_ = {t, d, m};
	Interval   *interval = &interval_;

	return !INTERVAL_NOT_FINITE(interval);
}

/* ---------- date.c: date_mi ---------- */

int
pg_adr_date_mi(int32 dateVal1, int32 dateVal2, int32 *out, int *err)
{
	if (DATE_NOT_FINITE(dateVal1) || DATE_NOT_FINITE(dateVal2))
	{
		PROOF_EREPORT_FLAG(err);	/* 22008 cannot subtract infinite dates */
		return 0;
	}

	*out = (int32) (dateVal1 - dateVal2);
	return 0;
}

/* ---------- date.c: date2timestamp_opt_overflow (verbatim; ereport ->
 * flag; used with overflow == NULL by all rows here) ---------- */

static Timestamp
pg_adr_date2timestamp_opt_overflow(DateADT dateVal, int *overflow, int *err)
{
	Timestamp	result;

	if (overflow)
		*overflow = 0;

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
			if (overflow)
			{
				*overflow = 1;
				TIMESTAMP_NOEND(result);
				return result;
			}
			else
			{
				PROOF_EREPORT_FLAG(err);	/* 22008 date out of range for
											 * timestamp */
				return 0;
			}
		}

		/* date is days since 2000, timestamp is microseconds since same... */
		result = dateVal * USECS_PER_DAY;
	}

	return result;
}

/* date.c date2timestamp (overflow == NULL arm) */
int
pg_adr_date_timestamp(int32 dateVal, int64 *out, int *err)
{
	*out = pg_adr_date2timestamp_opt_overflow(dateVal, NULL, err);
	return 0;
}

/* ---------- date.c: datetime_timestamp ---------- */

int
pg_adr_datetime_timestamp(int32 date, int64 time, int64 *out, int *err)
{
	Timestamp	result;

	result = pg_adr_date2timestamp_opt_overflow(date, NULL, err);
	if (*err)					/* shim: longjmp propagation */
		return 0;
	if (!TIMESTAMP_NOT_FINITE(result))
	{
		result += time;
		if (!IS_VALID_TIMESTAMP(result))
		{
			PROOF_EREPORT_FLAG(err);	/* 22008 timestamp out of range */
			return 0;
		}
	}

	*out = result;
	return 0;
}

/* ---------- date.c: datetimetz_timestamptz ---------- */

int
pg_adr_datetimetz_timestamptz(int32 date, int64 t_time, int32 t_zone,
							  int64 *out, int *err)
{
	TimeTzADT	time_ = {t_time, t_zone};
	TimeTzADT  *time = &time_;
	TimestampTz result;

	if (DATE_IS_NOBEGIN(date))
		TIMESTAMP_NOBEGIN(result);
	else if (DATE_IS_NOEND(date))
		TIMESTAMP_NOEND(result);
	else
	{
		/*
		 * Date's range is wider than timestamp's, so check for boundaries.
		 * Since dates have the same minimum values as timestamps, only upper
		 * boundary need be checked for overflow.
		 */
		if (date >= (TIMESTAMP_END_JULIAN - POSTGRES_EPOCH_JDATE))
		{
			PROOF_EREPORT_FLAG(err);	/* 22008 date out of range for
										 * timestamp */
			return 0;
		}
		result = date * USECS_PER_DAY + time->time + time->zone * USECS_PER_SEC;

		/*
		 * Since it is possible to go beyond allowed timestamptz range because
		 * of time zone, check for allowed timestamp range after adding tz.
		 */
		if (!IS_VALID_TIMESTAMP(result))
		{
			PROOF_EREPORT_FLAG(err);	/* 22008 date out of range for
										 * timestamp */
			return 0;
		}
	}

	*out = result;
	return 0;
}

/* ---------- date.c: time_interval / interval_time / time_mi_time -------- */

int
pg_adr_time_interval(int64 time, Interval *result)
{
	result->time = time;
	result->day = 0;
	result->month = 0;
	return 0;
}

int
pg_adr_interval_time(int64 t, int32 d, int32 m, int64 *out, int *err)
{
	Interval	span_ = {t, d, m};
	Interval   *span = &span_;
	TimeADT		result;

	if (INTERVAL_NOT_FINITE(span))
	{
		PROOF_EREPORT_FLAG(err);	/* 22008 cannot convert infinite interval
									 * to time */
		return 0;
	}

	result = span->time % USECS_PER_DAY;
	if (result < 0)
		result += USECS_PER_DAY;

	*out = result;
	return 0;
}

int
pg_adr_time_mi_time(int64 time1, int64 time2, Interval *result)
{
	result->month = 0;
	result->day = 0;
	result->time = time1 - time2;
	return 0;
}

/* ---------- date.c: timetz_time ---------- */

int64
pg_adr_timetz_time(int64 t_time, int32 t_zone)
{
	TimeTzADT	timetz_ = {t_time, t_zone};
	TimeTzADT  *timetz = &timetz_;
	TimeADT		result;

	/* swallow the time zone and just return the time */
	result = timetz->time;

	return result;
}

/* ---------- date.c: timetz_cmp_internal (verbatim) ---------- */

static int
pg_adr_timetz_cmp_internal(TimeTzADT *time1, TimeTzADT *time2)
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

/* ---------- date.c: overlaps_time (verbatim; null flags shim) ----------- */

int
pg_adr_overlaps_time(int64 ts1, int ts1IsNull, int64 te1, int te1IsNull,
					 int64 ts2, int ts2IsNull, int64 te2, int te2IsNull,
					 int *result)
{
#define ADR_TIME_GT(t1,t2) ((t1) > (t2))
#define ADR_TIME_LT(t1,t2) ((t1) < (t2))

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
		if (ADR_TIME_GT(ts1, te1))
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
		if (ADR_TIME_GT(ts2, te2))
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
	if (ADR_TIME_GT(ts1, ts2))
	{
		if (te2IsNull)
			return 1;
		if (ADR_TIME_LT(ts1, te2))
		{
			*result = 1;
			return 0;
		}
		if (te1IsNull)
			return 1;

		*result = 0;
		return 0;
	}
	else if (ADR_TIME_LT(ts1, ts2))
	{
		if (te1IsNull)
			return 1;
		if (ADR_TIME_LT(ts2, te1))
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
#undef ADR_TIME_GT
#undef ADR_TIME_LT
}

/* ---------- date.c: overlaps_timetz (verbatim; timetz value+flag pairs,
 * TIMETZ_GT/LT -> vendored timetz_cmp_internal) ---------- */

int
pg_adr_overlaps_timetz(int64 t1t, int32 t1z, int ts1IsNull,
					   int64 e1t, int32 e1z, int te1IsNull,
					   int64 t2t, int32 t2z, int ts2IsNull,
					   int64 e2t, int32 e2z, int te2IsNull,
					   int *result)
{
	TimeTzADT	v_ts1 = {t1t, t1z};
	TimeTzADT	v_te1 = {e1t, e1z};
	TimeTzADT	v_ts2 = {t2t, t2z};
	TimeTzADT	v_te2 = {e2t, e2z};
	TimeTzADT  *ts1 = &v_ts1;
	TimeTzADT  *te1 = &v_te1;
	TimeTzADT  *ts2 = &v_ts2;
	TimeTzADT  *te2 = &v_te2;

#define ADR_TIMETZ_GT(t1,t2) (pg_adr_timetz_cmp_internal(t1, t2) > 0)
#define ADR_TIMETZ_LT(t1,t2) (pg_adr_timetz_cmp_internal(t1, t2) < 0)

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
		if (ADR_TIMETZ_GT(ts1, te1))
		{
			TimeTzADT  *tt = ts1;

			ts1 = te1;
			te1 = tt;
		}
	}

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
		if (ADR_TIMETZ_GT(ts2, te2))
		{
			TimeTzADT  *tt = ts2;

			ts2 = te2;
			te2 = tt;
		}
	}

	if (ADR_TIMETZ_GT(ts1, ts2))
	{
		if (te2IsNull)
			return 1;
		if (ADR_TIMETZ_LT(ts1, te2))
		{
			*result = 1;
			return 0;
		}
		if (te1IsNull)
			return 1;

		*result = 0;
		return 0;
	}
	else if (ADR_TIMETZ_LT(ts1, ts2))
	{
		if (te1IsNull)
			return 1;
		if (ADR_TIMETZ_LT(ts2, te1))
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
#undef ADR_TIMETZ_GT
#undef ADR_TIMETZ_LT
}

/* ---------- date.c: anytime_typmod_check (WARNING dropped; value path
 * verbatim) ---------- */

int
pg_adr_anytime_typmod_check(int istz, int32 typmod, int32 *out, int *err)
{
	(void) istz;				/* only feeds message text */

	if (typmod < 0)
	{
		*err = 2;				/* 22023 precision must not be negative */
		return 0;
	}
	if (typmod > MAX_TIME_PRECISION)
	{
		/* ereport(WARNING, precision reduced to maximum allowed) — DROPPED
		 * (message emission out of proof both sides; see section header) */
		typmod = MAX_TIME_PRECISION;
	}

	*out = typmod;
	return 0;
}

/* ---------- date.c: anytime_typmodout (psprintf shim; verbatim shape) --- */

static int
pg_adr_emit_paren_int_str(char *res, int32 typmod, const char *suffix)
{
	char		digits[12];
	int			n = 0;
	int			len = 0;
	int			i;
	uint32		p = (uint32) typmod;	/* typmod >= 0 on this arm */

	res[len++] = '(';
	do
	{
		digits[n++] = (char) ('0' + (p % 10));
		p /= 10;
	} while (p != 0);
	for (i = n - 1; i >= 0; i--)
		res[len++] = digits[i];
	res[len++] = ')';
	for (i = 0; suffix[i] != '\0'; i++)
		res[len++] = suffix[i];
	res[len] = '\0';
	return len;
}

int
pg_adr_anytime_typmodout(int istz, int32 typmod, char *res /* [64] */ )
{
	const char *tz = istz ? " with time zone" : " without time zone";
	int			i;
	int			len = 0;

	if (typmod >= 0)
		return pg_adr_emit_paren_int_str(res, typmod, tz);

	/* pstrdup(tz) */
	for (i = 0; tz[i] != '\0'; i++)
		res[len++] = tz[i];
	res[len] = '\0';
	return len;
}

/* ---------- date.c: in_range_time_interval (verbatim) ---------- */

int
pg_adr_in_range_time_interval(int64 val, int64 base,
							  int64 ot, int32 od, int32 om,
							  int sub, int less, int *result, int *err)
{
	Interval	offset_ = {ot, od, om};
	Interval   *offset = &offset_;
	TimeADT		sum;

	/*
	 * Like time_pl_interval/time_mi_interval, we disregard the month and day
	 * fields of the offset.  So our test for negative should too.  This also
	 * catches -infinity, so we only need worry about +infinity below.
	 */
	if (offset->time < 0)
	{
		*err = 3;				/* 22013 invalid preceding/following size */
		return 0;
	}

	/*
	 * We can't use time_pl_interval/time_mi_interval here, because their
	 * wraparound behavior would give wrong (or at least undesirable) answers.
	 * Fortunately the equivalent non-wrapping behavior is trivial, except
	 * that adding an infinite (or very large) interval might cause integer
	 * overflow.  Subtraction cannot overflow here.
	 */
	if (sub)
		sum = base - offset->time;
	else if (pg_add_s64_overflow(base, offset->time, &sum))
	{
		*result = less;
		return 0;
	}

	if (less)
		*result = (val <= sum);
	else
		*result = (val >= sum);
	return 0;
}

/* ---------- date.c: in_range_timetz_interval (verbatim) ---------- */

int
pg_adr_in_range_timetz_interval(int64 vt, int32 vz, int64 bt, int32 bz,
								int64 ot, int32 od, int32 om,
								int sub, int less, int *result, int *err)
{
	TimeTzADT	val_ = {vt, vz};
	TimeTzADT	base_ = {bt, bz};
	TimeTzADT  *val = &val_;
	TimeTzADT  *base = &base_;
	Interval	offset_ = {ot, od, om};
	Interval   *offset = &offset_;
	TimeTzADT	sum;

	/*
	 * Like timetz_pl_interval/timetz_mi_interval, we disregard the month and
	 * day fields of the offset.  So our test for negative should too. This
	 * also catches -infinity, so we only need worry about +infinity below.
	 */
	if (offset->time < 0)
	{
		*err = 3;				/* 22013 invalid preceding/following size */
		return 0;
	}

	/*
	 * We can't use timetz_pl_interval/timetz_mi_interval here, because their
	 * wraparound behavior would give wrong (or at least undesirable) answers.
	 * Fortunately the equivalent non-wrapping behavior is trivial, except
	 * that adding an infinite (or very large) interval might cause integer
	 * overflow.  Subtraction cannot overflow here.
	 */
	if (sub)
		sum.time = base->time - offset->time;
	else if (pg_add_s64_overflow(base->time, offset->time, &sum.time))
	{
		*result = less;
		return 0;
	}
	sum.zone = base->zone;

	if (less)
		*result = (pg_adr_timetz_cmp_internal(val, &sum) <= 0);
	else
		*result = (pg_adr_timetz_cmp_internal(val, &sum) >= 0);
	return 0;
}

/* ---------- timestamp.c: timestamp_pl_interval / timestamp_mi_interval
 * (m0d0/noend planes: julian month/day arms -> OUT-OF-PLANE TRAP 99) ------ */

static int
pg_adr_timestamp_pl_interval(Timestamp timestamp, const Interval *span,
							 Timestamp *presult)
{
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
			return 1;			/* 22008 timestamp out of range */
		else
			TIMESTAMP_NOBEGIN(result);
	}
	else if (INTERVAL_IS_NOEND(span))
	{
		if (TIMESTAMP_IS_NOBEGIN(timestamp))
			return 1;			/* 22008 timestamp out of range */
		else
			TIMESTAMP_NOEND(result);
	}
	else if (TIMESTAMP_NOT_FINITE(timestamp))
		result = timestamp;
	else
	{
		if (span->month != 0)
			return 99;			/* OUT-OF-PLANE TRAP: julian month walk */

		if (span->day != 0)
			return 99;			/* OUT-OF-PLANE TRAP: julian day walk */

		if (pg_add_s64_overflow(timestamp, span->time, &timestamp))
			return 1;			/* 22008 timestamp out of range */

		if (!IS_VALID_TIMESTAMP(timestamp))
			return 1;			/* 22008 timestamp out of range */

		result = timestamp;
	}

	*presult = result;
	return 0;
}

static int
pg_adr_timestamp_mi_interval(Timestamp timestamp, const Interval *span,
							 Timestamp *presult)
{
	Interval	tspan;
	int			err = 0;

	pg_interval_um_internal(span, &tspan, &err);
	if (err)					/* shim: longjmp propagation */
		return 1;				/* 22008 interval out of range */

	return pg_adr_timestamp_pl_interval(timestamp, &tspan, presult);
}

/* ---------- timestamp.c: in_range_timestamp_interval (verbatim) --------- */

static int
pg_adr_in_range_timestamp_interval(Timestamp val, Timestamp base,
								   const Interval *offset,
								   int sub, int less, int *result)
{
	Timestamp	sum;
	int			rc;

	if (pg_adr_interval_sign(offset) < 0)
		return 3;				/* 22013 invalid preceding/following size */

	if (INTERVAL_IS_NOEND(offset) &&
		(sub ? TIMESTAMP_IS_NOEND(base) : TIMESTAMP_IS_NOBEGIN(base)))
	{
		*result = 1;
		return 0;
	}

	/* We don't currently bother to avoid overflow hazards here */
	if (sub)
		rc = pg_adr_timestamp_mi_interval(base, offset, &sum);
	else
		rc = pg_adr_timestamp_pl_interval(base, offset, &sum);
	if (rc)
		return rc;

	if (less)
		*result = (val <= sum);
	else
		*result = (val >= sum);
	return 0;
}

/* ---------- date.c: in_range_date_interval (verbatim composition) ------- */

int
pg_adr_in_range_date_interval(int32 val, int32 base,
							  int64 ot, int32 od, int32 om,
							  int sub, int less, int *result, int *err)
{
	Interval	offset_ = {ot, od, om};
	Interval   *offset = &offset_;
	Timestamp	valStamp;
	Timestamp	baseStamp;
	int			rc;

	/* XXX we could support out-of-range cases here, perhaps */
	valStamp = pg_adr_date2timestamp_opt_overflow(val, NULL, err);
	if (*err)					/* shim: longjmp propagation */
		return 0;
	baseStamp = pg_adr_date2timestamp_opt_overflow(base, NULL, err);
	if (*err)					/* shim: longjmp propagation */
		return 0;

	rc = pg_adr_in_range_timestamp_interval(valStamp, baseStamp, offset,
											sub, less, result);
	if (rc == 99)
		return 99;				/* out-of-plane trap propagates loudly */
	if (rc)
		*err = rc;
	return 0;
}

/* ---------- date.c: timetz_izone (verbatim; interval_out message text ->
 * flag) ---------- */

int
pg_adr_timetz_izone(int64 zt, int32 zd, int32 zm,
					int64 t_time, int32 t_zone,
					int64 *out_time, int32 *out_zone, int *err)
{
	Interval	zone_ = {zt, zd, zm};
	Interval   *zone = &zone_;
	TimeTzADT	time_ = {t_time, t_zone};
	TimeTzADT  *time = &time_;
	TimeTzADT	result_;
	TimeTzADT  *result = &result_;
	int			tz;

	if (INTERVAL_NOT_FINITE(zone))
	{
		*err = 2;				/* 22023 interval time zone must be finite */
		return 0;
	}

	if (zone->month != 0 || zone->day != 0)
	{
		*err = 2;				/* 22023 must not include months or days */
		return 0;
	}

	tz = -(zone->time / USECS_PER_SEC);

	result->time = time->time + (time->zone - tz) * USECS_PER_SEC;
	/* C99 modulo has the wrong sign convention for negative input */
	while (result->time < INT64CONST(0))
		result->time += USECS_PER_DAY;
	if (result->time >= USECS_PER_DAY)
		result->time %= USECS_PER_DAY;

	result->zone = tz;

	*out_time = result->time;
	*out_zone = result->zone;
	return 0;
}

/* ---------- timestamp.c: AdjustIntervalForTypmod + interval_scale
 * (verbatim; escontext == NULL at these call sites, so ereturn ==
 * ereport -> flags) ---------- */

static bool
pg_adr_AdjustIntervalForTypmod(Interval *interval, int32 typmod, int *err)
{
	static const int64 IntervalScales[MAX_INTERVAL_PRECISION + 1] = {
		INT64CONST(1000000),
		INT64CONST(100000),
		INT64CONST(10000),
		INT64CONST(1000),
		INT64CONST(100),
		INT64CONST(10),
		INT64CONST(1)
	};

	static const int64 IntervalOffsets[MAX_INTERVAL_PRECISION + 1] = {
		INT64CONST(500000),
		INT64CONST(50000),
		INT64CONST(5000),
		INT64CONST(500),
		INT64CONST(50),
		INT64CONST(5),
		INT64CONST(0)
	};

	/* Typmod has no effect on infinite intervals */
	if (INTERVAL_NOT_FINITE(interval))
		return true;

	/*
	 * Unspecified range and precision? Then not necessary to adjust. Setting
	 * typmod to -1 is the convention for all data types.
	 */
	if (typmod >= 0)
	{
		int			range = INTERVAL_RANGE(typmod);
		int			precision = INTERVAL_PRECISION(typmod);

		if (range == INTERVAL_FULL_RANGE)
		{
			/* Do nothing... */
		}
		else if (range == INTERVAL_MASK(YEAR))
		{
			interval->month = (interval->month / MONTHS_PER_YEAR) * MONTHS_PER_YEAR;
			interval->day = 0;
			interval->time = 0;
		}
		else if (range == INTERVAL_MASK(MONTH))
		{
			interval->day = 0;
			interval->time = 0;
		}
		/* YEAR TO MONTH */
		else if (range == (INTERVAL_MASK(YEAR) | INTERVAL_MASK(MONTH)))
		{
			interval->day = 0;
			interval->time = 0;
		}
		else if (range == INTERVAL_MASK(DAY))
		{
			interval->time = 0;
		}
		else if (range == INTERVAL_MASK(HOUR))
		{
			interval->time = (interval->time / USECS_PER_HOUR) *
				USECS_PER_HOUR;
		}
		else if (range == INTERVAL_MASK(MINUTE))
		{
			interval->time = (interval->time / USECS_PER_MINUTE) *
				USECS_PER_MINUTE;
		}
		else if (range == INTERVAL_MASK(SECOND))
		{
			/* fractional-second rounding will be dealt with below */
		}
		/* DAY TO HOUR */
		else if (range == (INTERVAL_MASK(DAY) |
						   INTERVAL_MASK(HOUR)))
		{
			interval->time = (interval->time / USECS_PER_HOUR) *
				USECS_PER_HOUR;
		}
		/* DAY TO MINUTE */
		else if (range == (INTERVAL_MASK(DAY) |
						   INTERVAL_MASK(HOUR) |
						   INTERVAL_MASK(MINUTE)))
		{
			interval->time = (interval->time / USECS_PER_MINUTE) *
				USECS_PER_MINUTE;
		}
		/* DAY TO SECOND */
		else if (range == (INTERVAL_MASK(DAY) |
						   INTERVAL_MASK(HOUR) |
						   INTERVAL_MASK(MINUTE) |
						   INTERVAL_MASK(SECOND)))
		{
			/* fractional-second rounding will be dealt with below */
		}
		/* HOUR TO MINUTE */
		else if (range == (INTERVAL_MASK(HOUR) |
						   INTERVAL_MASK(MINUTE)))
		{
			interval->time = (interval->time / USECS_PER_MINUTE) *
				USECS_PER_MINUTE;
		}
		/* HOUR TO SECOND */
		else if (range == (INTERVAL_MASK(HOUR) |
						   INTERVAL_MASK(MINUTE) |
						   INTERVAL_MASK(SECOND)))
		{
			/* fractional-second rounding will be dealt with below */
		}
		/* MINUTE TO SECOND */
		else if (range == (INTERVAL_MASK(MINUTE) |
						   INTERVAL_MASK(SECOND)))
		{
			/* fractional-second rounding will be dealt with below */
		}
		else
		{
			*err = 4;			/* elog(ERROR, "unrecognized interval
								 * typmod") */
			return false;
		}

		/* Need to adjust sub-second precision? */
		if (precision != INTERVAL_FULL_PRECISION)
		{
			if (precision < 0 || precision > MAX_INTERVAL_PRECISION)
			{
				*err = 2;		/* 22023 interval(%d) precision must be
								 * between 0 and 6 */
				return false;
			}

			if (interval->time >= INT64CONST(0))
			{
				if (pg_add_s64_overflow(interval->time,
										IntervalOffsets[precision],
										&interval->time))
				{
					*err = 1;	/* 22008 interval out of range */
					return false;
				}
				interval->time -= interval->time % IntervalScales[precision];
			}
			else
			{
				if (pg_sub_s64_overflow(interval->time,
										IntervalOffsets[precision],
										&interval->time))
				{
					*err = 1;	/* 22008 interval out of range */
					return false;
				}
				interval->time -= interval->time % IntervalScales[precision];
			}
		}
	}

	return true;
}

int
pg_adr_interval_scale(int64 t, int32 d, int32 m, int32 typmod,
					  Interval *result, int *err)
{
	Interval	interval_ = {t, d, m};
	Interval   *interval = &interval_;

	*result = *interval;

	pg_adr_AdjustIntervalForTypmod(result, typmod, err);

	return 0;
}

/* ---------- recv rows (cursor shims over (data,len); bodies verbatim) --- */

/* date.c date_recv */
int
pg_adr_date_recv(const uint8 *data, int len, int32 *out, int *err)
{
	int			cur = 0;
	DateADT		result;

	result = (DateADT) pg_proof_getmsgint32(data, len, &cur, err);
	if (*err)					/* shim: longjmp propagation */
		return 0;

	/* Limit to the same range that date_in() accepts. */
	if (DATE_NOT_FINITE(result))
		 /* ok */ ;
	else if (!IS_VALID_DATE(result))
	{
		PROOF_EREPORT_FLAG(err);	/* 22008 date out of range */
		return 0;
	}

	*out = result;
	return 0;
}

/* date.c time_recv (AdjustTimeForTypmod is the shipped/proved scale core;
 * typmod rides through verbatim — harness planes pin it to -1) */
int
pg_adr_time_recv(const uint8 *data, int len, int32 typmod, int64 *out,
				 int *err)
{
	int			cur = 0;
	TimeADT		result;

	result = pg_proof_getmsgint64(data, len, &cur, err);
	if (*err)					/* shim: longjmp propagation */
		return 0;

	if (result < INT64CONST(0) || result > USECS_PER_DAY)
	{
		PROOF_EREPORT_FLAG(err);	/* 22008 time out of range */
		return 0;
	}

	pg_adjust_time_for_typmod(&result, typmod);

	*out = result;
	return 0;
}

/* date.c timetz_recv */
int
pg_adr_timetz_recv(const uint8 *data, int len, int32 typmod,
				   int64 *out_time, int32 *out_zone, int *err)
{
	int			cur = 0;
	TimeTzADT	result_;
	TimeTzADT  *result = &result_;

	result->time = pg_proof_getmsgint64(data, len, &cur, err);
	if (*err)					/* shim: longjmp propagation */
		return 0;

	if (result->time < INT64CONST(0) || result->time > USECS_PER_DAY)
	{
		PROOF_EREPORT_FLAG(err);	/* 22008 time out of range */
		return 0;
	}

	result->zone = pg_proof_getmsgint32(data, len, &cur, err);
	if (*err)					/* shim: longjmp propagation */
		return 0;

	/* Check for sane GMT displacement; see notes in datatype/timestamp.h */
	if (result->zone <= -TZDISP_LIMIT || result->zone >= TZDISP_LIMIT)
	{
		*err = 5;				/* 22009 time zone displacement out of range */
		return 0;
	}

	pg_adjust_time_for_typmod(&(result->time), typmod);

	*out_time = result->time;
	*out_zone = result->zone;
	return 0;
}

/* timestamp.c interval_recv */
int
pg_adr_interval_recv(const uint8 *data, int len, int32 typmod,
					 Interval *interval, int *err)
{
	int			cur = 0;

	interval->time = pg_proof_getmsgint64(data, len, &cur, err);
	if (*err)					/* shim: longjmp propagation */
		return 0;
	interval->day = pg_proof_getmsgint32(data, len, &cur, err);
	if (*err)					/* shim: longjmp propagation */
		return 0;
	interval->month = pg_proof_getmsgint32(data, len, &cur, err);
	if (*err)					/* shim: longjmp propagation */
		return 0;

	pg_adr_AdjustIntervalForTypmod(interval, typmod, err);

	return 0;
}

/* ---------- send rows (pq_begintypsend/pq_endtypsend shim: 4-byte LE
 * varlena header len<<2 + BE payload) ---------- */

static void
pg_adr_set_varsize_4b(uint8 *out, uint32 len)
{
	uint32		hdr = len << 2;

	out[0] = (uint8) (hdr & 0xFF);
	out[1] = (uint8) ((hdr >> 8) & 0xFF);
	out[2] = (uint8) ((hdr >> 16) & 0xFF);
	out[3] = (uint8) ((hdr >> 24) & 0xFF);
}

int32
pg_adr_date_send(int32 date, uint8 *out /* [8] */ )
{
	int			cur = 4;

	pg_proof_sendint32(out, &cur, date);
	pg_adr_set_varsize_4b(out, 8);
	return 8;
}

int32
pg_adr_time_send(int64 time, uint8 *out /* [12] */ )
{
	int			cur = 4;

	pg_proof_sendint64(out, &cur, time);
	pg_adr_set_varsize_4b(out, 12);
	return 12;
}

int32
pg_adr_timetz_send(int64 t_time, int32 t_zone, uint8 *out /* [16] */ )
{
	int			cur = 4;

	pg_proof_sendint64(out, &cur, t_time);
	pg_proof_sendint32(out, &cur, t_zone);
	pg_adr_set_varsize_4b(out, 16);
	return 16;
}

int32
pg_adr_interval_send(int64 t, int32 d, int32 m, uint8 *out /* [20] */ )
{
	int			cur = 4;

	pg_proof_sendint64(out, &cur, t);
	pg_proof_sendint32(out, &cur, d);
	pg_proof_sendint32(out, &cur, m);
	pg_adr_set_varsize_4b(out, 20);
	return 20;
}
