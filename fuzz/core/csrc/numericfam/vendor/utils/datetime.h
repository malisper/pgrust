/* Shim for the vendored formatting.c: value-exact copies of the datetime
 * types/constants from utils/timestamp.h + utils/datetime.h + pgtime.h. */
#ifndef FMTV_DATETIME_H
#define FMTV_DATETIME_H

typedef int64 pg_time_t;
typedef struct pg_tz pg_tz;

struct pg_tm
{
	int			tm_sec;
	int			tm_min;
	int			tm_hour;
	int			tm_mday;
	int			tm_mon;			/* origin 1, not 0 */
	int			tm_year;		/* relative to 1900 */
	int			tm_wday;
	int			tm_yday;
	int			tm_isdst;
	long int	tm_gmtoff;
	const char *tm_zone;
};

typedef int64 Timestamp;
typedef int64 TimestampTz;
typedef int64 TimeADT;
typedef int32 DateADT;
typedef int32 fsec_t;

typedef struct
{
	TimeADT		time;
	int32		zone;
} TimeTzADT;

typedef struct
{
	int64		time;
	int32		day;
	int32		month;
} Interval;

struct pg_itm
{
	int			tm_usec;
	int			tm_sec;
	int			tm_min;
	int64		tm_hour;
	int			tm_mday;
	int			tm_mon;
	int			tm_year;
};

#define MONTHS_PER_YEAR 12
#define DAYS_PER_WEEK 7
#define HOURS_PER_DAY 24
#define MINS_PER_HOUR 60
#define SECS_PER_DAY 86400
#define SECS_PER_HOUR 3600
#define SECS_PER_MINUTE 60
#define USECS_PER_DAY INT64CONST(86400000000)
#define USECS_PER_HOUR INT64CONST(3600000000)
#define USECS_PER_MINUTE INT64CONST(60000000)
#define USECS_PER_SEC INT64CONST(1000000)

#define POSTGRES_EPOCH_JDATE 2451545
#define UNIX_EPOCH_JDATE 2440588

#define DT_NOBEGIN PG_INT64_MIN
#define DT_NOEND PG_INT64_MAX
#define TIMESTAMP_IS_NOBEGIN(j) ((j) == DT_NOBEGIN)
#define TIMESTAMP_IS_NOEND(j) ((j) == DT_NOEND)
#define TIMESTAMP_NOT_FINITE(j) (TIMESTAMP_IS_NOBEGIN(j) || TIMESTAMP_IS_NOEND(j))

#define MAXDATELEN 128

#define DTERR_BAD_FORMAT (-1)
#define DTERR_FIELD_OVERFLOW (-2)
#define DTERR_MD_FIELD_OVERFLOW (-3)
#define DTERR_INTERVAL_OVERFLOW (-4)
#define DTERR_TZDISP_OVERFLOW (-5)
#define DTERR_BAD_TIMEZONE (-6)
#define DTERR_BAD_ZONE_ABBREV (-7)

extern const char *const months[];
extern const char *const days[];
extern pg_tz *session_timezone;

extern int	date2j(int year, int month, int day);
extern void j2date(int jd, int *year, int *month, int *day);
extern int	j2day(int date);
extern int	timestamp2tm(Timestamp dt, int *tzp, struct pg_tm *tm,
						 fsec_t *fsec, const char **tzn, pg_tz *attimezone);
extern int	tm2timestamp(struct pg_tm *tm, fsec_t fsec, int *tzp, Timestamp *result);
extern void interval2itm(Interval span, struct pg_itm *itm);
extern int	date2isoweek(int year, int mon, int mday);
extern int	date2isoyear(int year, int mon, int mday);
extern int	date2isoyearday(int year, int mon, int mday);
extern int	isoweek2j(int year, int week);
extern void isoweek2date(int woy, int *year, int *mon, int *mday);
extern void isoweekdate2date(int isoweek, int wday, int *year, int *mon, int *mday);
extern bool ValidateDate(int fmask, bool isjulian, bool is2digits, bool bc,
						 struct pg_tm *tm);
extern int	DetermineTimeZoneOffset(struct pg_tm *tm, pg_tz *tzp);
extern int	DecodeTimezoneAbbrevPrefix(const char *str, int *offset, pg_tz **tz);
extern int	DecodeTimezoneNameToTz(const char *tzname, pg_tz **tz);

/* fmask bits (datetime.h) — ValidateDate/from_char use these */
#define DTK_M(t) (0x01 << (t))
#define YEAR 2
#define MONTH 1
#define DAY 3
#define JULIAN 14
#define TZ 5
#define DTZ 6
#define DTK_DATE_M (DTK_M(YEAR) | DTK_M(MONTH) | DTK_M(DAY))

#define DAYS_PER_MONTH 30
#define MAX_TIMESTAMP_PRECISION 6
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
#define DATETIME_MIN_JULIAN (0)
#define DATE_END_JULIAN (2147483494)
#define IS_VALID_DATE(d) \
	((DATETIME_MIN_JULIAN - POSTGRES_EPOCH_JDATE) <= (d) && \
	 (d) < (DATE_END_JULIAN - POSTGRES_EPOCH_JDATE))
#define INTERVAL_IS_NOBEGIN(i) \
	((i)->month == PG_INT32_MIN && (i)->day == PG_INT32_MIN && (i)->time == PG_INT64_MIN)
#define INTERVAL_IS_NOEND(i) \
	((i)->month == PG_INT32_MAX && (i)->day == PG_INT32_MAX && (i)->time == PG_INT64_MAX)
#define INTERVAL_NOT_FINITE(i) (INTERVAL_IS_NOBEGIN(i) || INTERVAL_IS_NOEND(i))
#define TimestampTzGetDatum(X) Int64GetDatum(X)
#define DateADTGetDatum(X) Int32GetDatum(X)
#define TimeADTGetDatum(X) Int64GetDatum(X)
#define TimeTzADTPGetDatum(X) PointerGetDatum(X)
#define MAX_TZDISP_HOUR 15
#define isleap(y) (((y) % 4) == 0 && (((y) % 100) != 0 || ((y) % 400) == 0))
typedef struct DateTimeErrorExtra
{
	int			dtee_timezone;
} DateTimeErrorExtra;
extern int	tm2time(struct pg_tm *tm, fsec_t fsec, TimeADT *result);
extern int	tm2timetz(struct pg_tm *tm, fsec_t fsec, int tz, TimeTzADT *result);
extern void AdjustTimeForTypmod(TimeADT *time, int32 typmod);
extern void DateTimeParseError(int dterr, DateTimeErrorExtra *extra,
							   const char *str, const char *datatype,
							   Node *escontext);
extern int	DetermineTimeZoneAbbrevOffset(struct pg_tm *tm, const char *abbr,
										  pg_tz *tzp);
extern void AdjustTimestampForTypmod(Timestamp *time, int32 typmod, Node *escontext);
#endif
