/*
 * Vendored PostgreSQL C: date / time / timetz text I/O + constructors +
 * part() — differential-fuzz oracle for the datetime_io_diff target
 * (100%-coverage campaign; crate crates/backend/utils/adt/adt_date, parse
 * engine crates/backend/utils/adt/adt_datetime).
 *
 * Provenance (all bodies VERBATIM, extracted mechanically by
 * csrc/extract_verbatim.py into csrc/pg_datetime_verbatim.inc which this
 * file #includes; re-run the script to refresh), all from postgres-src
 * 62d6c7d3df6287f1bd83199c1a746e50d31571a0 (REL_18, the repo's vendored
 * ground-truth checkout ../pgrust-reference/vendor/postgres-src — PostgreSQL
 * 18.3 "Stamp-18.3"):
 *   - src/backend/utils/adt/date.c: date_in, date_out, time_in, time_out,
 *     timetz_in, timetz_out, time_part(+time_part_common), make_time,
 *     make_date, EncodeSpecialDate, tm2time/time2tm/tm2timetz/timetz2tm,
 *     time_overflows, float_time_overflows, AdjustTimeForTypmod,
 *     anytime_typmod_check — verbatim, INCLUDING the fmgr entry wrappers
 *     (they compile over the shim pgdt/fmgr.h).
 *   - src/backend/utils/adt/datetime.c: the whole reachable parse/encode
 *     core — ParseDateTime, DecodeDateTime, DecodeTimeOnly, DecodeDate,
 *     DecodeTime(+Common), DecodeNumber(+Field), DecodeTimezone,
 *     DecodeTimezoneAbbrev, DecodeSpecial, DecodeUnits, ValidateDate,
 *     DateTimeParseError, datebsearch, date2j/j2date/j2day, ParseFraction,
 *     ParseFractionalSecond, AppendSeconds, EncodeTimezone, EncodeDateOnly,
 *     EncodeTimeOnly, DetermineTimeZoneOffset(+Internal),
 *     DetermineTimeZoneAbbrevOffset(+Internal), TimeZoneAbbrevIsKnown,
 *     ClearTimeZoneAbbrevCache, and the token tables datetktbl/deltatktbl/
 *     day_tab/months/days + lookup caches.
 *   - src/backend/utils/adt/timestamp.c: dt2time, GetEpochTime.
 *   - src/common/string.c: strtoint. src/backend/utils/adt/numutils.c:
 *     pg_ultostr, pg_ultostr_zeropad. src/backend/parser/scansup.c:
 *     downcase_truncate_identifier, downcase_identifier.
 *   - src/port/pgstrcasecmp.c: pg_tolower, pg_toupper. src/port/strlcpy.c:
 *     strlcpy (renamed pg_dt_strlcpy via #define — macOS libc declares its
 *     own).
 *   - src/include/utils/{datetime,date,timestamp}.h,
 *     src/include/datatype/timestamp.h, src/include/pgtime.h: byte-copies
 *     under csrc/pgdt/ (only fmgr.h there is a shim).
 *
 * PINNED ENVIRONMENT (mirrored exactly by the Rust driver — environment,
 * never computation; fuzzuproof-crate skill rules):
 *   - DateStyle/DateOrder: plain int globals, set per exec from the fuzz
 *     selector on BOTH sides (all 5 styles x 3 orders fuzzed).
 *   - session timezone = GMT: session_timezone is an opaque token; the
 *     localtime-library boundary (pg_next_dst_boundary,
 *     pg_timezone_abbrev_is_known, pg_interpret_timezone_abbrev) is shimmed
 *     with the exact GMT answers (no DST transitions, offset 0, sole abbrev
 *     "GMT"). The datetime.c logic above that boundary stays verbatim. The
 *     Rust side installs the real GMT zone via pgtz::pg_tzset(b"GMT"), so
 *     its real engine must produce these same answers — that equivalence is
 *     part of what is fuzzed.
 *   - current date/time pinned to 2026-06-15 12:30:45.123456 GMT on both
 *     sides (C: GetCurrentDateTime/GetCurrentTimeUsec shims below; Rust:
 *     timestamp_seams::get_current_datetime/get_current_time_usec installed
 *     in the driver), making "now"/"today"/"yesterday"/"tomorrow" and
 *     zone-less timetz input deterministic.
 *   - database encoding = UTF-8: pg_database_encoding_max_length() == 4, so
 *     downcase_identifier's high-bit tolower() arm (single-byte encodings
 *     only) is dead on both sides.
 *   - zoneabbrevtbl = NULL (timezone_abbreviations never installed): only
 *     numeric zone offsets and the session zone's own "GMT" abbrev resolve;
 *     DYNTZ paths are unreachable (FetchDynamicTimeZone stub aborts).
 *   - DOMAIN CARVE, mechanical: inputs whose parse consults pg_tzset with
 *     any name but GMT (tzdata names, POSIX "UTC+10" strings) are OUTSIDE
 *     the compared domain — pg_tzset flags the exec and the driver skips
 *     every plane comparison for it (Rust still executes for panic-safety).
 *     Those code paths are the tz-database state carve in the routes rows.
 *
 * Shims (plumbing only, never logic):
 *   - ereport/ereturn/errsave -> record the errcode class in the shared
 *     _Thread_local pg_diff_errcode (via a pending slot so non-throwing
 *     WARNING sites do not pollute it) and longjmp out; errmsg/errdetail/
 *     errhint evaluate to 0 with arguments unevaluated (message text is out
 *     of comparison scope). elog(ERROR) -> class 99 (internal).
 *   - palloc/pstrdup -> malloc/strdup (results copied out and freed by the
 *     pg_diff_* driver entries).
 *   - truncate_identifier stub aborts: the driver caps units at
 *     NAMEDATALEN-1 bytes so identifier truncation never fires.
 *   - int64_to_numeric / int64_div_fast_to_numeric stubs abort: only the
 *     retnumeric=false (float8) plane of time_part is driven here.
 *   - pg_gmtime answers only t=0 (GetEpochTime's sole call), with the
 *     constant POSIX epoch breakdown.
 *
 * Errcode classes (mapped from the Rust sqlstates in datetime_io_diff.rs):
 *   1 = 22007 invalid_datetime_format
 *   2 = 22008 datetime_field_overflow / datetime_value_out_of_range
 *   3 = 22009 invalid_time_zone_displacement_value
 *   4 = 22015 interval_field_overflow
 *   5 = 22023 invalid_parameter_value
 *   6 = 0A000 feature_not_supported
 *   7 = F0000 config_file_error
 *  99 = internal (elog paths; must never fire)
 */

#include "postgres.h"

#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <math.h>
#include <inttypes.h>
#include <setjmp.h>
#include <stdio.h>
#include <string.h>

#define pg_restrict __restrict

/* rename the vendored strlcpy: macOS/glibc>=2.38 declare their own */
#undef strlcpy
#define strlcpy pg_dt_strlcpy

#include "fmgr.h"				/* csrc/pgdt shim */
#include "pgtime.h"
#include "datatype/timestamp.h"
#include "utils/datetime.h"
#include "utils/date.h"

/* ---- miscadmin.h constants (verbatim values) ---- */
#define MAXTZLEN		10
#define USE_POSTGRES_DATES		0
#define USE_ISO_DATES			1
#define USE_SQL_DATES			2
#define USE_GERMAN_DATES		3
#define USE_XSD_DATES			4
#define DATEORDER_YMD			0
#define DATEORDER_DMY			1
#define DATEORDER_MDY			2

/* ---- pg_config_manual.h / c.h bits (verbatim definitions) ---- */
#define NAMEDATALEN 64
#define IS_HIGHBIT_SET(ch) ((unsigned char) (ch) & 0x80)
#define HAVE__BUILTIN_OP_OVERFLOW 1
#define PG_INT32_MIN	(-0x7FFFFFFF-1)
#define PG_INT32_MAX	(0x7FFFFFFF)
#define PG_INT64_MIN	(-INT64CONST(0x7FFFFFFFFFFFFFFF) - 1)
#define PG_INT64_MAX	INT64CONST(0x7FFFFFFFFFFFFFFF)
#define i64abs(i) llabs(i)
#define strtoi64(str, endptr, base) ((int64) strtoll(str, endptr, base))

/* mbutils.c shim: database encoding pinned to UTF-8 (max length 4) on both
 * sides — see the PINNED ENVIRONMENT block in the header. */
static int
pg_database_encoding_max_length(void)
{
	return 4;
}

/* prototypes for verbatim bodies below */
char	   *downcase_identifier(const char *ident, int len, bool warn,
								bool truncate);
unsigned char pg_toupper(unsigned char ch);
unsigned char pg_tolower(unsigned char ch);

/* ---- src/include/port/pg_bitutils.h pg_leftmost_one_pos32 — VERBATIM
 * (HAVE__BUILTIN_CLZ arm; clang/gcc both have it) ---- */
static inline int
pg_leftmost_one_pos32(uint32 word)
{
	Assert(word != 0);

	return 31 - __builtin_clz(word);
}

/* ---- miscadmin.h IntervalStyle constants (verbatim values) + the GUC
 * global (globals.c), set per exec by the interval driver entries ---- */
#define INTSTYLE_POSTGRES			0
#define INTSTYLE_POSTGRES_VERBOSE	1
#define INTSTYLE_SQL_STANDARD		2
#define INTSTYLE_ISO_8601			3
int			IntervalStyle = INTSTYLE_POSTGRES;

/* ---- src/include/common/int.h overflow helpers — VERBATIM
 * (HAVE__BUILTIN_OP_OVERFLOW arms) ---- */
static inline bool
pg_add_s32_overflow(int32 a, int32 b, int32 *result)
{
	return __builtin_add_overflow(a, b, result);
}

static inline bool
pg_mul_s32_overflow(int32 a, int32 b, int32 *result)
{
	return __builtin_mul_overflow(a, b, result);
}

static inline bool
pg_add_s64_overflow(int64 a, int64 b, int64 *result)
{
	return __builtin_add_overflow(a, b, result);
}

static inline bool
pg_mul_s64_overflow(int64 a, int64 b, int64 *result)
{
	return __builtin_mul_overflow(a, b, result);
}

/* ---- src/include/common/int.h pg_neg_s32_overflow — VERBATIM ---- */
static inline bool
pg_neg_s32_overflow(int32 a, int32 *result)
{
#if defined(HAVE__BUILTIN_OP_OVERFLOW)
	return __builtin_sub_overflow(0, a, result);
#else
	if (unlikely(a == PG_INT32_MIN))
	{
		*result = 0x5EED;		/* to avoid spurious warnings */
		return true;
	}
	*result = -a;
	return false;
#endif
}

/* ---- GUC globals (globals.c) — set per exec by the driver entries ---- */
int			DateStyle = USE_ISO_DATES;
int			DateOrder = DATEORDER_MDY;

/* ---- error shims ---- */

extern _Thread_local int pg_diff_errcode;	/* defined in pg_float_io.c */

static _Thread_local jmp_buf pg_dt_jmp;
static _Thread_local int pg_dt_pending;

#define PG_DT_ERR_INTERNAL 99

static void
pg_dt_throw(void)
{
	pg_diff_errcode = pg_dt_pending ? pg_dt_pending : PG_DT_ERR_INTERNAL;
	longjmp(pg_dt_jmp, 1);
}

#define ERRCODE_INVALID_DATETIME_FORMAT 1
#define ERRCODE_DATETIME_FIELD_OVERFLOW 2
#define ERRCODE_DATETIME_VALUE_OUT_OF_RANGE 2
#define ERRCODE_INVALID_TIME_ZONE_DISPLACEMENT_VALUE 3
#define ERRCODE_INTERVAL_FIELD_OVERFLOW 4
#define ERRCODE_INVALID_PARAMETER_VALUE 5
#define ERRCODE_FEATURE_NOT_SUPPORTED 6
#define ERRCODE_CONFIG_FILE_ERROR 7

#define WARNING 19
#define ERROR 21

#define errcode(c) (pg_dt_pending = (c))
#define errmsg(...) 0
#define errmsg_internal(...) 0
#define errdetail(...) 0
#define errhint(...) 0

#define ereport(elevel, ...) \
	do { \
		pg_dt_pending = 0; \
		(void) (__VA_ARGS__); \
		if ((elevel) >= ERROR) \
			pg_dt_throw(); \
	} while (0)

/* escontext is always NULL here (hard-error shape): both throw */
#define errsave(escontext, ...) \
	do { \
		pg_dt_pending = 0; \
		(void) (__VA_ARGS__); \
		pg_dt_throw(); \
	} while (0)

#define ereturn(escontext, dummy_value, ...) \
	errsave(escontext, __VA_ARGS__)

#define elog(elevel, ...) \
	do { \
		if ((elevel) >= ERROR) \
		{ \
			pg_dt_pending = PG_DT_ERR_INTERNAL; \
			pg_dt_throw(); \
		} \
	} while (0)

/* ---- allocator shims ---- */
#undef palloc
#define palloc(n) malloc(n)
#define pfree free
#define pstrdup strdup

/* ---- environment pins (see file header) ---- */

struct pg_tz
{
	char		token[4];		/* opaque; all queries answered by the GMT
								 * shims below */
};
static struct pg_tz pg_dt_gmt_tz = {"GMT"};
pg_tz	   *session_timezone = &pg_dt_gmt_tz;

/* GMT: no DST transitions ever */
int
pg_next_dst_boundary(const pg_time_t *timep,
					 long int *before_gmtoff, int *before_isdst,
					 pg_time_t *boundary,
					 long int *after_gmtoff, int *after_isdst,
					 const pg_tz *tz)
{
	(void) timep;
	(void) boundary;
	(void) after_gmtoff;
	(void) after_isdst;
	(void) tz;
	*before_gmtoff = 0;
	*before_isdst = 0;
	return 0;
}

/* GMT: the sole known abbreviation is fixed "GMT", offset 0 */
bool
pg_timezone_abbrev_is_known(const char *abbrev,
							bool *isfixed, long int *gmtoff, int *isdst,
							const pg_tz *tz)
{
	(void) tz;
	if (strcmp(abbrev, "GMT") == 0)
	{
		*isfixed = true;
		*gmtoff = 0;
		*isdst = 0;
		return true;
	}
	return false;
}

bool
pg_interpret_timezone_abbrev(const char *abbrev,
							 const pg_time_t *timep,
							 long int *gmtoff, int *isdst,
							 const pg_tz *tz)
{
	(void) timep;
	(void) tz;
	if (strcmp(abbrev, "GMT") == 0)
	{
		*gmtoff = 0;
		*isdst = 0;
		return true;
	}
	return false;
}

/* tz database pinned to {GMT} only (see header): named-zone lookups resolve
 * GMT (any case) and nothing else. The Rust side gets the identical answer
 * set by pointing PGRUST_TZDIR at a nonexistent directory, so its tzload
 * fails for every name while pg_tzset's GMT special case still works. */
_Thread_local int pg_dt_tzset_nongmt;

int
pg_diff_datetime_tzset_nongmt(void)
{
	return pg_dt_tzset_nongmt;
}

pg_tz *
pg_tzset(const char *name)
{
	if (strlen(name) == 3 &&
		pg_toupper((unsigned char) name[0]) == 'G' &&
		pg_toupper((unsigned char) name[1]) == 'M' &&
		pg_toupper((unsigned char) name[2]) == 'T')
		return &pg_dt_gmt_tz;

	/* DOMAIN CARVE (see header): the real pg_tzset also accepts tzdata
	 * names and POSIX zone strings ("UTC+10") via tzparse, an engine this
	 * oracle does not vendor. Any input that reaches this point leaves the
	 * compared domain: flag it so the driver SKIPS all plane comparisons
	 * for this exec (the Rust side still runs for panic-safety). */
	pg_dt_tzset_nongmt = 1;
	return NULL;
}

/* GMT is a fixed-offset zone: offset 0, no transitions */
bool
pg_get_timezone_offset(const pg_tz *tz, long int *gmtoff)
{
	(void) tz;
	*gmtoff = 0;
	return true;
}

/* GetEpochTime's sole caller passes t=0: the POSIX epoch breakdown */
struct pg_tm *
pg_gmtime(const pg_time_t *timep)
{
	static _Thread_local struct pg_tm epoch_tm;

	if (*timep != 0)
		abort();				/* only GetEpochTime(0) is reachable */
	memset(&epoch_tm, 0, sizeof(epoch_tm));
	epoch_tm.tm_year = 70;		/* 1970, POSIX 1900-based */
	epoch_tm.tm_mon = 0;		/* POSIX 0-based */
	epoch_tm.tm_mday = 1;
	epoch_tm.tm_wday = 4;		/* Thursday */
	epoch_tm.tm_yday = 0;
	epoch_tm.tm_isdst = 0;
	epoch_tm.tm_zone = "GMT";
	return &epoch_tm;
}

/* Pinned current date/time: 2026-06-15 12:30:45.123456 GMT (see header).
 * These OVERRIDE datetime.c's clock-reading originals (not extracted). */
void
GetCurrentTimeUsec(struct pg_tm *tm, fsec_t *fsec, int *tzp)
{
	memset(tm, 0, sizeof(*tm));
	tm->tm_year = 2026;			/* PG convention: 1-based, AD */
	tm->tm_mon = 6;
	tm->tm_mday = 15;
	tm->tm_hour = 12;
	tm->tm_min = 30;
	tm->tm_sec = 45;
	tm->tm_isdst = 0;
	tm->tm_gmtoff = 0;
	tm->tm_zone = "GMT";
	tm->tm_wday = j2day(date2j(2026, 6, 15));
	tm->tm_yday = date2j(2026, 6, 15) - date2j(2026, 1, 1);
	*fsec = 123456;
	if (tzp != NULL)
		*tzp = 0;
}

void
GetCurrentDateTime(struct pg_tm *tm)
{
	fsec_t		fsec;

	GetCurrentTimeUsec(tm, &fsec, NULL);
}

/* ---- unreachable-path stubs (documented in header) ---- */

static void
truncate_identifier(char *ident, int len, bool warn)
{
	(void) ident;
	(void) len;
	(void) warn;
	abort();					/* driver caps units < NAMEDATALEN */
}

Numeric
int64_to_numeric(int64 v)
{
	(void) v;
	abort();					/* retnumeric plane not driven here */
}

Numeric
int64_div_fast_to_numeric(int64 val1, int log10val2)
{
	(void) val1;
	(void) log10val2;
	abort();					/* retnumeric plane not driven here */
}

static pg_tz *
FetchDynamicTimeZone(TimeZoneAbbrevTable *tbl, const datetkn *tp,
					 DateTimeErrorExtra *extra)
{
	(void) tbl;
	(void) tp;
	(void) extra;
	abort();					/* zoneabbrevtbl == NULL: DYNTZ unreachable */
}

/* ---- static prototypes for the verbatim bodies below (order-free) ---- */

static int	DecodeNumber(int flen, char *str, bool haveTextMonth,
						 int fmask, int *tmask,
						 struct pg_tm *tm, fsec_t *fsec, bool *is2digits);
static int	DecodeNumberField(int len, char *str,
							  int fmask, int *tmask,
							  struct pg_tm *tm, fsec_t *fsec, bool *is2digits);
static int	DecodeTimeCommon(char *str, int fmask, int range,
							 int *tmask, struct pg_itm *itm);
static int	DecodeTime(char *str, int fmask, int range,
					   int *tmask, struct pg_tm *tm, fsec_t *fsec);
static int	DecodeDate(char *str, int fmask, int *tmask, bool *is2digits,
					   struct pg_tm *tm);
static char *AppendSeconds(char *cp, int sec, fsec_t fsec,
						   int precision, bool fillzeros);
static int	DetermineTimeZoneOffsetInternal(struct pg_tm *tm, pg_tz *tzp,
											pg_time_t *tp);
static bool DetermineTimeZoneAbbrevOffsetInternal(pg_time_t t,
												  const char *abbr, pg_tz *tzp,
												  int *offset, int *isdst);
static bool TimeZoneAbbrevIsKnown(const char *abbr, pg_tz *tzp,
								  bool *isfixed, int *offset, int *isdst);
static const datetkn *datebsearch(const char *key, const datetkn *base, int nel);
static char *EncodeTimezone(char *str, int tz, int style);
static int	ParseFraction(char *cp, double *frac);
static int	ParseFractionalSecond(char *cp, fsec_t *fsec);

/* verbatim date.c helpers used before their definitions */
int			anytime_typmod_check(bool istz, int32 typmod);
static Datum time_part_common(PG_FUNCTION_ARGS, bool retnumeric);

/* pg_dt_strlcpy (renamed vendored strlcpy) */
size_t		pg_dt_strlcpy(char *dst, const char *src, size_t siz);

/* from utils/numeric.h (only referenced, never executed) */

/* ==== the verbatim vendored bodies ==== */

/* datetime.c lookup caches + abbrev cache struct — VERBATIM
 * (src/backend/utils/adt/datetime.c) */
static TimeZoneAbbrevTable *zoneabbrevtbl = NULL;

/* Caches of recent lookup results in the above tables */

static const datetkn *datecache[MAXDATEFIELDS] = {NULL};

static const datetkn *deltacache[MAXDATEFIELDS] = {NULL};

/* Cache for results of timezone abbreviation lookups */

typedef struct TzAbbrevCache
{
	char		abbrev[TOKMAXLEN + 1];	/* always NUL-terminated */
	char		ftype;			/* TZ, DTZ, or DYNTZ */
	int			offset;			/* GMT offset, if fixed-offset */
	pg_tz	   *tz;				/* relevant zone, if variable-offset */
} TzAbbrevCache;

static TzAbbrevCache tzabbrevcache[MAXDATEFIELDS];

#include "pg_datetime_verbatim.inc"

/* ========== fuzz-facing driver entries (NOT Postgres code) ========== */

static void
pg_dt_reset(int style, int order)
{
	pg_diff_errcode = 0;
	pg_dt_pending = 0;
	pg_dt_tzset_nongmt = 0;
	DateStyle = style;
	DateOrder = order;
}

/* Build a minimal fcinfo; args filled by callers. */
static struct FunctionCallInfoBaseData pg_dt_fcinfo_data;

static FunctionCallInfo
pg_dt_fcinfo(void)
{
	memset(&pg_dt_fcinfo_data, 0, sizeof(pg_dt_fcinfo_data));
	return &pg_dt_fcinfo_data;
}

int
pg_diff_date_in(const char *str, int style, int order, int32 *out)
{
	FunctionCallInfo fcinfo = pg_dt_fcinfo();
	Datum		d;

	pg_dt_reset(style, order);
	if (setjmp(pg_dt_jmp))
		return pg_diff_errcode;
	fcinfo->args[0].value = PointerGetDatum(str);
	d = date_in(fcinfo);
	*out = DatumGetInt32(d);
	return 0;
}

int
pg_diff_date_out(int32 date, int style, int order, char *buf)
{
	FunctionCallInfo fcinfo = pg_dt_fcinfo();
	char	   *r;

	pg_dt_reset(style, order);
	if (setjmp(pg_dt_jmp))
		return pg_diff_errcode;
	fcinfo->args[0].value = Int32GetDatum(date);
	r = (char *) DatumGetPointer(date_out(fcinfo));
	strcpy(buf, r);
	free(r);
	return 0;
}

int
pg_diff_time_in(const char *str, int32 typmod, int style, int order, int64 *out)
{
	FunctionCallInfo fcinfo = pg_dt_fcinfo();
	Datum		d;

	pg_dt_reset(style, order);
	if (setjmp(pg_dt_jmp))
		return pg_diff_errcode;
	fcinfo->args[0].value = PointerGetDatum(str);
	fcinfo->args[1].value = Int32GetDatum(0);	/* typioparam, unused */
	fcinfo->args[2].value = Int32GetDatum(typmod);
	d = time_in(fcinfo);
	*out = DatumGetInt64(d);
	return 0;
}

int
pg_diff_time_out(int64 time, int style, int order, char *buf)
{
	FunctionCallInfo fcinfo = pg_dt_fcinfo();
	char	   *r;

	pg_dt_reset(style, order);
	if (setjmp(pg_dt_jmp))
		return pg_diff_errcode;
	fcinfo->args[0].value = Int64GetDatum(time);
	r = (char *) DatumGetPointer(time_out(fcinfo));
	strcpy(buf, r);
	free(r);
	return 0;
}

int
pg_diff_timetz_in(const char *str, int32 typmod, int style, int order,
				  int64 *out_time, int32 *out_zone)
{
	FunctionCallInfo fcinfo = pg_dt_fcinfo();
	TimeTzADT  *r;

	pg_dt_reset(style, order);
	if (setjmp(pg_dt_jmp))
		return pg_diff_errcode;
	fcinfo->args[0].value = PointerGetDatum(str);
	fcinfo->args[1].value = Int32GetDatum(0);	/* typioparam, unused */
	fcinfo->args[2].value = Int32GetDatum(typmod);
	r = (TimeTzADT *) DatumGetPointer(timetz_in(fcinfo));
	*out_time = r->time;
	*out_zone = r->zone;
	free(r);
	return 0;
}

int
pg_diff_timetz_out(int64 time, int32 zone, int style, int order, char *buf)
{
	FunctionCallInfo fcinfo = pg_dt_fcinfo();
	TimeTzADT	t;
	char	   *r;

	pg_dt_reset(style, order);
	if (setjmp(pg_dt_jmp))
		return pg_diff_errcode;
	t.time = time;
	t.zone = zone;
	fcinfo->args[0].value = PointerGetDatum(&t);
	r = (char *) DatumGetPointer(timetz_out(fcinfo));
	strcpy(buf, r);
	free(r);
	return 0;
}

int
pg_diff_time_part(const unsigned char *units, int units_len, int64 time,
				  double *out)
{
	FunctionCallInfo fcinfo = pg_dt_fcinfo();
	/* 4-byte-header varlena (see pgdt/fmgr.h) */
	static _Thread_local struct
	{
		uint32		hdr;
		char		data[NAMEDATALEN];
	}			vl;
	Datum		d;

	pg_dt_reset(USE_ISO_DATES, DATEORDER_YMD);
	if (setjmp(pg_dt_jmp))
		return pg_diff_errcode;
	if (units_len > NAMEDATALEN - 1)
		abort();				/* driver contract */
	vl.hdr = (uint32) ((units_len + VARHDRSZ) << 2);
	memcpy(vl.data, units, units_len);
	fcinfo->args[0].value = PointerGetDatum(&vl);
	fcinfo->args[1].value = Int64GetDatum(time);
	d = time_part(fcinfo);
	*out = DatumGetFloat8(d);
	return 0;
}

int
pg_diff_make_time(int32 hour, int32 min, double sec, int64 *out)
{
	FunctionCallInfo fcinfo = pg_dt_fcinfo();
	Datum		d;

	pg_dt_reset(USE_ISO_DATES, DATEORDER_YMD);
	if (setjmp(pg_dt_jmp))
		return pg_diff_errcode;
	fcinfo->args[0].value = Int32GetDatum(hour);
	fcinfo->args[1].value = Int32GetDatum(min);
	fcinfo->args[2].value = Float8GetDatum(sec);
	d = make_time(fcinfo);
	*out = DatumGetInt64(d);
	return 0;
}

int
pg_diff_make_date(int32 year, int32 month, int32 day, int32 *out)
{
	FunctionCallInfo fcinfo = pg_dt_fcinfo();
	Datum		d;

	pg_dt_reset(USE_ISO_DATES, DATEORDER_YMD);
	if (setjmp(pg_dt_jmp))
		return pg_diff_errcode;
	fcinfo->args[0].value = Int32GetDatum(year);
	fcinfo->args[1].value = Int32GetDatum(month);
	fcinfo->args[2].value = Int32GetDatum(day);
	d = make_date(fcinfo);
	*out = DatumGetInt32(d);
	return 0;
}

/* ====== interval_engine_diff driver entries (NOT Postgres code) ======
 * Engine-level differential over adt_datetime's interval parse/encode:
 * DecodeInterval / DecodeISO8601Interval return raw dterr codes (compared
 * directly, finer than errcode classes); EncodeInterval compared on the
 * text image. interval2itm (timestamp.c, verbatim) only PREPARES the pg_itm
 * input for both sides' encoders from a raw (time,day,month) triple — it is
 * shared input construction, not a compared surface. */

int
pg_diff_decode_interval(const char *str, int32 range, int istyle,
						int64 *usec, int32 *mday, int32 *mon, int32 *year,
						int32 *dtype)
{
	char		workbuf[MAXDATELEN + 1];
	char	   *field[MAXDATEFIELDS];
	int			ftype[MAXDATEFIELDS];
	int			nf;
	int			dterr;
	struct pg_itm_in itm_in;

	pg_diff_errcode = 0;
	pg_dt_pending = 0;
	pg_dt_tzset_nongmt = 0;
	IntervalStyle = istyle;
	if (setjmp(pg_dt_jmp))
		return 1000 + pg_diff_errcode;	/* ereport escape (deltatktbl abbrev
										 * paths do not ereport; guard) */
	dterr = ParseDateTime(str, workbuf, sizeof(workbuf),
						  field, ftype, MAXDATEFIELDS, &nf);
	if (dterr == 0)
		dterr = DecodeInterval(field, ftype, nf, range, dtype, &itm_in);
	if (dterr != 0)
		return dterr;			/* raw negative DTERR code */
	*usec = itm_in.tm_usec;
	*mday = itm_in.tm_mday;
	*mon = itm_in.tm_mon;
	*year = itm_in.tm_year;
	return 0;
}

int
pg_diff_decode_iso8601_interval(const char *str,
								int64 *usec, int32 *mday, int32 *mon,
								int32 *year, int32 *dtype)
{
	struct pg_itm_in itm_in;
	int			dterr;
	char		buf[256];

	pg_diff_errcode = 0;
	pg_dt_pending = 0;
	IntervalStyle = INTSTYLE_ISO_8601;
	if (setjmp(pg_dt_jmp))
		return 1000 + pg_diff_errcode;
	/* DecodeISO8601Interval writes through its char* (strtod-style walk) */
	strlcpy(buf, str, sizeof(buf));
	dterr = DecodeISO8601Interval(buf, dtype, &itm_in);
	if (dterr != 0)
		return dterr;
	*usec = itm_in.tm_usec;
	*mday = itm_in.tm_mday;
	*mon = itm_in.tm_mon;
	*year = itm_in.tm_year;
	return 0;
}

int
pg_diff_encode_interval(int64 time, int32 day, int32 month, int istyle,
						char *buf,
						int64 *itm_usec, int64 *itm_hour, int32 *itm_sec,
						int32 *itm_min, int32 *itm_mday, int32 *itm_mon,
						int32 *itm_year)
{
	Interval	span;
	struct pg_itm itm;

	pg_diff_errcode = 0;
	pg_dt_pending = 0;
	IntervalStyle = istyle;
	if (setjmp(pg_dt_jmp))
		return 1000 + pg_diff_errcode;
	span.time = time;
	span.day = day;
	span.month = month;
	interval2itm(span, &itm);
	/* hand the SAME itm to the Rust side */
	*itm_usec = itm.tm_usec;
	*itm_hour = itm.tm_hour;
	*itm_sec = itm.tm_sec;
	*itm_min = itm.tm_min;
	*itm_mday = itm.tm_mday;
	*itm_mon = itm.tm_mon;
	*itm_year = itm.tm_year;
	EncodeInterval(&itm, istyle, buf);
	return 0;
}
