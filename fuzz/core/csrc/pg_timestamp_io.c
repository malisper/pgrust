/*
 * pg_timestamp_io.c: vendored PostgreSQL C oracle for the timestamp_diff
 * differential fuzz target (100%-coverage campaign; crate
 * crates/backend/utils/adt/adt_timestamp, lane p1-laney).
 *
 * Provenance: all vendored bodies VERBATIM from postgres-src
 * 62d6c7d3df6287f1bd83199c1a746e50d31571a0 (PostgreSQL 18.3 "Stamp-18.3"),
 * extracted mechanically:
 *   - csrc/pg_datetime_verbatim.inc (extract_verbatim.py, shared with lane
 *     p1-lanel's datetime family targets): the whole datetime.c
 *     parse/encode core, timestamp2tm, dt2time, interval2itm, the interval
 *     decode/encode engine, and the date.c bodies.
 *   - csrc/pg_timestamp_verbatim.inc (extract_ts_verbatim.py, THIS lane):
 *     the timestamp.c SQL-entry bodies — timestamp[tz] text/binary I/O,
 *     typmod adjust, interval[tz] I/O, trunc/part/extract, age, bin,
 *     make_*, +/- interval, justify family, mul/div, min/max/cmp, izone,
 *     the interval-avg aggregate core, and datetime.c's
 *     DecodeTimezoneName(+ToTz).
 *
 * PINNED ENVIRONMENT + SHIMS: byte-identical prelude to lane p1-lanel's
 * csrc/pg_datetime_io_io.c (GMT session zone via the localtime-boundary
 * shims, pinned now = 2026-06-15 12:30:45.123456 GMT, ereport->longjmp with
 * errcode classes, TLS bump arena for palloc, tz-database DOMAIN CARVE via
 * pg_tzset flagging) — see that file's header for the full contract; the
 * only prelude difference is the numeric BOUNDARY shims (documented at
 * their definition below): extract_* arms record the (int64 value, decimal
 * scale) handed to numeric constructors instead of vendoring numeric.c,
 * and full numeric-op chains (epoch/julian) set a flag that demotes the
 * exec to verdict+sqlstate-only comparison.
 *
 * SYMBOL ISOLATION: this TU vendors the same verbatim datetime.c/date.c
 * TU-fractions as p1-lanel's oracle; core/build.rs compiles it with a
 * tsdiff_-prefix -D rename of every colliding global (the hashenc/cryptofam
 * precedent), so both oracles keep their own vendored copies.
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
#include "pg_oracle_guard.h"	/* oracle-serialization holder check */

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
/* _Thread_local for the same reason as DateStyle/DateOrder below. */
_Thread_local int IntervalStyle = INTSTYLE_POSTGRES;

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

/*
 * ---- GUC globals (globals.c) — set per exec by the driver entries ----
 *
 * _Thread_local, not plain globals: these model per-backend GUC state, and
 * pgrust is thread-per-backend, so the shipped Rust side holds them in
 * thread_local! cells (adt_datetime/src/settings.rs). Process-global C copies
 * were observably WRONG under the multi-threaded `cargo test` rails — one
 * test's pg_dt_reset() clobbered another's style between its reset and its
 * read, manufacturing "divergences" that were pure cross-test interference.
 * (libFuzzer runs one thread per process, so no campaign verdict was affected;
 * this makes the test rails trustworthy and matches the Rust storage class.)
 */
_Thread_local int DateStyle = USE_ISO_DATES;
_Thread_local int DateOrder = DATEORDER_MDY;

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

/* ---- allocator shims ----
 * Real PostgreSQL frees per-call scratch (e.g. downcase_truncate_identifier's
 * lowercased copy inside time_part) by resetting the per-tuple memory
 * context, never by explicit pfree.  A malloc shim therefore LEAKS every
 * such allocation (fleet LSan artifact leak-9b8209d924a8, 2-byte lowunits).
 * Model the context instead: a per-exec bump arena, reset in pg_dt_reset().
 * pfree becomes a no-op (context-owned), exactly PG's lifetime semantics. */
#define PG_DT_ARENA_SZ (64 * 1024)
static _Thread_local char pg_dt_arena[PG_DT_ARENA_SZ];
static _Thread_local size_t pg_dt_arena_off;

static void *
pg_dt_palloc(size_t n)
{
	size_t		off = (pg_dt_arena_off + 7) & ~(size_t) 7;

	if (n > PG_DT_ARENA_SZ - off)
		abort();				/* arena exhausted: driver contract broken */
	pg_dt_arena_off = off + n;
	return pg_dt_arena + off;
}

static char *
pg_dt_pstrdup(const char *s)
{
	size_t		len = strlen(s) + 1;
	char	   *p = pg_dt_palloc(len);

	memcpy(p, s, len);
	return p;
}

#undef palloc
#define palloc(n) pg_dt_palloc(n)
#define pfree(p) ((void) (p))
#define pstrdup pg_dt_pstrdup

/* ---- environment pins (see file header) ---- */

struct pg_tz
{
	char		token[4];		/* opaque; queries answered by the shims
								 * below from gmtoff (GMT = 0) */
	long		gmtoff;			/* fixed offset east of GMT (pg_tzset_offset
								 * zones; tz-database zones are carved) */
};
static struct pg_tz pg_dt_gmt_tz = {"GMT", 0};
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
	*before_gmtoff = tz ? tz->gmtoff : 0;
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

/* The non-GMT name this exec asked for, NUL-terminated (empty when none).
 * The driver keys its zone-name admission budget on this: the Rust engine's
 * pg_tzset cache is process-lifetime by design (C parity: pgtz.c's
 * timezone_cache HTAB is never evicted either), so an unbounded stream of
 * distinct fuzzer-invented names would grow RSS without bound. */
_Thread_local char pg_dt_tzset_name[TZ_STRLEN_MAX + 1];

int
pg_diff_datetime_tzset_nongmt(void)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	return pg_dt_tzset_nongmt;
}

const char *
pg_diff_datetime_tzset_name(void)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	return pg_dt_tzset_name;
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
	 * for this exec (the Rust side still runs for panic-safety, subject to
	 * the driver's distinct-name admission budget). */
	pg_dt_tzset_nongmt = 1;
	{
		/* plain copy: the vendored strlcpy is defined further down */
		size_t		i;

		for (i = 0; i + 1 < sizeof(pg_dt_tzset_name) && name[i] != '\0'; i++)
			pg_dt_tzset_name[i] = name[i];
		pg_dt_tzset_name[i] = '\0';
	}
	return NULL;
}

/* Fixed-offset zone constructor (pgtz.c pg_tzset_offset): the returned
 * token answers every localtime-boundary query from gmtoff. Arena-lived —
 * per-exec, matching the compared Rust side's zone-cache admission (the
 * fixed-offset zones the Rust pg_tzset_offset builds are equivalent by
 * construction; what is compared is the arithmetic done WITH the offset). */
pg_tz *
pg_tzset_offset(long gmtoffset)
{
	pg_tz	   *tz = pg_dt_palloc(sizeof(pg_tz));

	tz->token[0] = '-';
	tz->token[1] = '\0';
	tz->gmtoff = gmtoffset;
	return tz;
}

/* GMT is a fixed-offset zone: offset 0, no transitions */
bool
pg_get_timezone_offset(const pg_tz *tz, long int *gmtoff)
{
	*gmtoff = tz ? tz->gmtoff : 0;
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

/*
 * GMT pg_localtime: timestamp2tm's tzp!=NULL branch crosses the
 * localtime-library boundary here (same seam as pg_next_dst_boundary /
 * pg_interpret_timezone_abbrev above). The session zone is pinned to GMT, so
 * the answer is the UTC civil breakdown with offset 0 and no DST.
 *
 * The calendar arithmetic is PostgreSQL's OWN vendored j2date/j2day, not a
 * reimplementation: only the epoch-seconds -> (Julian day, second-of-day)
 * split is shim plumbing, which is precisely the part a real tzfile zone with
 * a single fixed 0 transition performs. The Rust side answers this through its
 * REAL pgtz GMT zone (installed by pg_tzset(b"GMT")), so C-shim == Rust-real
 * for GMT is itself part of what these arms fuzz.
 */
struct pg_tm *
pg_localtime(const pg_time_t *timep, const pg_tz *tz)
{
	static _Thread_local struct pg_tm tmbuf;
	pg_time_t	t = *timep + (tz ? tz->gmtoff : 0);
	pg_time_t	days = t / SECS_PER_DAY;
	pg_time_t	rem = t % SECS_PER_DAY;
	int			y,
				mo,
				d;

	if (rem < 0)
	{
		rem += SECS_PER_DAY;
		days -= 1;
	}
	j2date((int) (UNIX_EPOCH_JDATE + days), &y, &mo, &d);
	memset(&tmbuf, 0, sizeof(tmbuf));
	tmbuf.tm_year = y - 1900;	/* POSIX 1900-based, as timestamp2tm expects */
	tmbuf.tm_mon = mo - 1;		/* POSIX 0-based */
	tmbuf.tm_mday = d;
	tmbuf.tm_hour = (int) (rem / SECS_PER_HOUR);
	tmbuf.tm_min = (int) ((rem % SECS_PER_HOUR) / SECS_PER_MINUTE);
	tmbuf.tm_sec = (int) (rem % SECS_PER_MINUTE);
	tmbuf.tm_wday = j2day((int) (UNIX_EPOCH_JDATE + days));
	tmbuf.tm_yday = (int) (UNIX_EPOCH_JDATE + days) - date2j(y, 1, 1);
	tmbuf.tm_isdst = 0;
	tmbuf.tm_gmtoff = tz ? tz->gmtoff : 0;
	tmbuf.tm_zone = tz && tz->gmtoff == 0 ? "GMT" : "-";
	return &tmbuf;
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

/* NUMERIC BOUNDARY SHIMS (extract_* arms): the adt_timestamp lines under
 * test DECIDE the (int64 value, decimal scale) handed to numeric; the
 * numeric ENCODING itself is adt/numeric crate surface (owned by its own
 * lane). So the oracle records the constructor arguments instead of
 * vendoring numeric.c, and the driver compares pgrust's rendered numeric
 * text against the exact decimal string these arguments determine. Paths
 * that go through the full numeric op chain (epoch/julian: numeric_in,
 * numeric_add/sub/div_opt_error, numeric_round) set pg_ts_numchain and the
 * driver compares verdict+sqlstate planes only for that exec (documented
 * value-plane carve; the Rust lines still run under fuzz). */
static _Thread_local int64 pg_ts_num_val;
static _Thread_local int pg_ts_num_log10;
static _Thread_local int pg_ts_num_set;
_Thread_local int pg_ts_numchain;
static struct NumericData { int dummy; } pg_ts_num_token;

Numeric
int64_to_numeric(int64 v)
{
	pg_ts_num_val = v;
	pg_ts_num_log10 = 0;
	pg_ts_num_set = 1;
	return &pg_ts_num_token;
}

Numeric
int64_div_fast_to_numeric(int64 val1, int log10val2)
{
	pg_ts_num_val = val1;
	pg_ts_num_log10 = log10val2;
	pg_ts_num_set = 1;
	return &pg_ts_num_token;
}

static Numeric
pg_ts_numchain_touch(void)
{
	pg_ts_numchain = 1;
	pg_ts_num_set = 0;
	return &pg_ts_num_token;
}

Numeric
numeric_add_opt_error(Numeric num1, Numeric num2, bool *have_error)
{
	(void) num1; (void) num2;
	if (have_error) *have_error = false;
	return pg_ts_numchain_touch();
}

Numeric
numeric_sub_opt_error(Numeric num1, Numeric num2, bool *have_error)
{
	(void) num1; (void) num2;
	if (have_error) *have_error = false;
	return pg_ts_numchain_touch();
}

Numeric
numeric_div_opt_error(Numeric num1, Numeric num2, bool *have_error)
{
	(void) num1; (void) num2;
	if (have_error) *have_error = false;
	return pg_ts_numchain_touch();
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
static char *AppendTimestampSeconds(char *cp, struct pg_tm *tm, fsec_t fsec);
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

/* This TU stubs FetchDynamicTimeZone (DYNTZ unreachable: zoneabbrevtbl==NULL)
 * and has no tzEntry/guc_malloc support — skip the abbrev-table builders the
 * datetime_convert_diff oracle carries in the shared .inc. */
#define PG_DT_OMIT_ABBREV_BUILDERS 1
#include "pg_datetime_verbatim.inc"

/* ==== H0 SCRIBBLER detector (task #112) ====
 * Every legal entry of datecache[]/deltacache[] is NULL or a pointer INTO
 * datetktbl[]/deltatktbl[] — an exact validity predicate over the caches
 * (docs/conformance/scribbler-investigation-2026-08-02.md §5 H0). Called
 * from OracleSerial::drop at depth 0 so a cross-thread scribble is named
 * by the test whose oracle exit follows the write. On poison, both caches
 * are cleared (pure memoization — always safe) so one scribble does not
 * cascade-panic every subsequent oracle exit.
 * Returns 0 if sane, else 100+i (datecache[i] bad) or 200+i (deltacache[i]
 * bad) for the first bad slot found. */
int
pg_tsdiff_cache_check(void)
{
	int			code = 0;

	for (int i = 0; i < MAXDATEFIELDS && code == 0; i++)
	{
		const datetkn *dp = datecache[i];
		const datetkn *tp = deltacache[i];

		if (dp != NULL && (dp < datetktbl || dp >= datetktbl + szdatetktbl))
			code = 100 + i;
		else if (tp != NULL && (tp < deltatktbl || tp >= deltatktbl + szdeltatktbl))
			code = 200 + i;
	}
	if (code != 0)
	{
		memset(datecache, 0, sizeof(datecache));
		memset(deltacache, 0, sizeof(deltacache));
	}
	return code;
}

/* Evidence probe (task #112 follow-up): raw slot value, so a detector fire
 * can report the poisoned VALUE (the byte-level signature is the evidence),
 * not just the slot index. which: 0 = datecache, 1 = deltacache. Reads only;
 * never clears. */
uintptr_t
pg_tsdiff_cache_peek(int which, int idx)
{
	if (idx < 0 || idx >= MAXDATEFIELDS)
		return 0;
	return which == 0 ? (uintptr_t) datecache[idx] : (uintptr_t) deltacache[idx];
}

/* Table bounds for the evidence report: healthy slot values point into
 * [base, base+n) of the corresponding table. which as above. */
uintptr_t
pg_tsdiff_cache_table_base(int which)
{
	return which == 0 ? (uintptr_t) datetktbl : (uintptr_t) deltatktbl;
}

/* Address of the cache array itself (victim address for watchpoint-based
 * attribution; the arrays are TU-local statics invisible to nm). */
uintptr_t
pg_tsdiff_cache_addr(int which)
{
	return which == 0 ? (uintptr_t) datecache : (uintptr_t) deltacache;
}

int
pg_tsdiff_cache_table_nel(int which)
{
	return which == 0 ? szdatetktbl : szdeltatktbl;
}

/* Test-only: plant the SCRIBBLER's exact one-byte signature (a valid table
 * pointer with byte index 2 zeroed) so the detector's must-fail control can
 * prove the Drop-path wiring fires. */
void
pg_tsdiff_cache_poison_for_test(void)
{
	uintptr_t	p = (uintptr_t) &deltatktbl[1] & ~((uintptr_t) 0xff << 16);

	/* if byte 2 was already zero the scribbled pointer is still in-table;
	 * force invalidity so the control always plants real poison */
	if ((const datetkn *) p >= deltatktbl &&
		(const datetkn *) p < deltatktbl + szdeltatktbl)
		p = (uintptr_t) 0x1;
	deltacache[3] = (const datetkn *) p;
}

/* ================================================================== *
 *  adt_timestamp additions (lane p1-laney) — shims + verbatim bodies *
 * ================================================================== */

/* ---- common/int.h pg_sub_s*_overflow (verbatim semantics via the same
 * __builtin the upstream HAVE__BUILTIN_OP_OVERFLOW arm uses) ---- */
static inline bool
pg_sub_s32_overflow(int32 a, int32 b, int32 *result)
{
	return __builtin_sub_overflow(a, b, result);
}

static inline bool
pg_sub_s64_overflow(int64 a, int64 b, int64 *result)
{
	return __builtin_sub_overflow(a, b, result);
}

#include <assert.h>

/* ---- errcode classes this crate's C raises beyond the datetime set ---- */
#define ERRCODE_DIVISION_BY_ZERO 9
#define ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE 10

/* ---- c.h float-fit macros + utils/float.h helpers — VERBATIM semantics
 * (float8_mul's overflow/underflow checks reproduced from float.h; the
 * ereport goes through the same errcode-class channel) ---- */
#define FLOAT8_FITS_IN_INT32(num) \
	((num) >= (float8) PG_INT32_MIN && (num) < -((float8) PG_INT32_MIN))
#define FLOAT8_FITS_IN_INT64(num) \
	((num) >= (float8) PG_INT64_MIN && (num) < -((float8) PG_INT64_MIN))

static inline float8
get_float8_infinity(void)
{
	return (float8) INFINITY;
}

static void
float_overflow_error(void)
{
	ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)));
}

static void
float_underflow_error(void)
{
	ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)));
}

static inline float8
float8_mul(const float8 val1, const float8 val2)
{
	float8		result;

	result = val1 * val2;
	if (unlikely(isinf(result)) && !isinf(val1) && !isinf(val2))
		float_overflow_error();
	if (unlikely(result == 0.0) && val1 != 0.0 && val2 != 0.0)
		float_underflow_error();

	return result;
}

#define InvalidOid ((Oid) 0)

/* VERBATIM (timestamp.c:89) — rides the IntervalAggState typedef */
#define IA_TOTAL_COUNT(ia) \
	((ia)->N + (ia)->pInfcount + (ia)->nInfcount)

#define CStringGetDatum(X) PointerGetDatum(X)
#define ObjectIdGetDatum(X) ((Datum) (X))

/* ---- common/int128.h — VERBATIM header (native __int128 path) ---- */
typedef __int128 int128;
typedef unsigned __int128 uint128;
#define HAVE_INT128 1
#include "common/int128.h"

/* ---- extra fmgr plumbing the timestamp.c bodies use (shim; pgdt/utils/
 * timestamp.h already provides the Timestamp/Interval Datum casts and
 * PG_GETARG/PG_RETURN forms) ---- */
#define PG_GETARG_OID(n)		 ((Oid) PG_GETARG_INT32(n))
#define PG_GETARG_POINTER(n)	 DatumGetPointer(PG_GETARG_DATUM(n))
#define PG_RETURN_POINTER(x)	 return PointerGetDatum(x)
#define PG_RETURN_BYTEA_P(x)	 return PointerGetDatum(x)
#define PG_GETARG_BYTEA_PP(n)	 ((bytea *) DatumGetPointer(PG_GETARG_DATUM(n)))
#define PG_ARGISNULL(n)			 (fcinfo->args[n].isnull)
#define PG_GETARG_BOOL(n)		 ((bool) (PG_GETARG_DATUM(n) != 0))

typedef struct varlena bytea;

static inline Numeric
DatumGetNumeric(Datum X)
{
	return (Numeric) DatumGetPointer(X);
}
static inline Datum
NumericGetDatum(Numeric X)
{
	return PointerGetDatum(X);
}

/* DirectFunctionCall: real fmgr-style dispatch over the shim fcinfo
 * (plumbing; the callee is always a vendored verbatim body). */
typedef Datum (*PGFunction) (FunctionCallInfo fcinfo);

static Datum
pg_ts_dfc(PGFunction func, int nargs, Datum a0, Datum a1, Datum a2)
{
	struct FunctionCallInfoBaseData fc;

	memset(&fc, 0, sizeof(fc));
	fc.nargs = (short) nargs;
	fc.args[0].value = a0;
	fc.args[1].value = a1;
	fc.args[2].value = a2;
	return func(&fc);
}

#define DirectFunctionCall1(f, a)		pg_ts_dfc(f, 1, (a), 0, 0)
#define DirectFunctionCall2(f, a, b)	pg_ts_dfc(f, 2, (a), (b), 0)
#define DirectFunctionCall3(f, a, b, c) pg_ts_dfc(f, 3, (a), (b), (c))

/* numeric_in / numeric_round arrive via DirectFunctionCall: numeric-chain
 * carve (see the boundary shims above). */
static Datum
numeric_in(PG_FUNCTION_ARGS)
{
	(void) fcinfo;
	return PointerGetDatum(pg_ts_numchain_touch());
}

static Datum
numeric_round(PG_FUNCTION_ARGS)
{
	(void) fcinfo;
	return PointerGetDatum(pg_ts_numchain_touch());
}

/* ---- agg-context plumbing (nodeAgg.h): the agg arms drive the pure
 * accumulation cores; context checks always answer "in agg context". ---- */
#define AGG_CONTEXT_AGGREGATE 1
typedef void *MemoryContext;
static _Thread_local char pg_ts_dummy_mcx;

static int
AggCheckCallContext(FunctionCallInfo fcinfo, MemoryContext *aggcontext)
{
	(void) fcinfo;
	if (aggcontext)
		*aggcontext = &pg_ts_dummy_mcx;
	return AGG_CONTEXT_AGGREGATE;
}

static MemoryContext
MemoryContextSwitchTo(MemoryContext ctx)
{
	return ctx;
}

#define CurrentMemoryContext ((MemoryContext) &pg_ts_dummy_mcx)
#define palloc0(n) pg_ts_palloc0(n)

static void *
pg_ts_palloc0(size_t n)
{
	void	   *p = pg_dt_palloc(n);

	memset(p, 0, n);
	return p;
}

/* ---- pqformat/stringinfo shims (wire plumbing only; the vendored recv/
 * send/serialize bodies stay verbatim above them) ---- */
typedef struct StringInfoData
{
	char	   *data;
	int			len;
	int			maxlen;
	int			cursor;
} StringInfoData;
typedef StringInfoData *StringInfo;

#define PG_TS_WIREBUF 256

static void
pq_begintypsend(StringInfo buf)
{
	buf->data = pg_dt_palloc(PG_TS_WIREBUF);
	buf->maxlen = PG_TS_WIREBUF;
	buf->len = 4;				/* VARHDRSZ reserved, as upstream */
	buf->cursor = 0;
}

static void
pq_sendint32(StringInfo buf, uint32 v)
{
	assert(buf->len + 4 <= buf->maxlen);
	buf->data[buf->len++] = (char) (v >> 24);
	buf->data[buf->len++] = (char) (v >> 16);
	buf->data[buf->len++] = (char) (v >> 8);
	buf->data[buf->len++] = (char) v;
}

static void
pq_sendint64(StringInfo buf, uint64 v)
{
	pq_sendint32(buf, (uint32) (v >> 32));
	pq_sendint32(buf, (uint32) v);
}

static bytea *
pq_endtypsend(StringInfo buf)
{
	bytea	   *result = (bytea *) buf->data;

	result->vl_len_ = ((uint32) buf->len) << 2;	/* 4B varlena header */
	return result;
}

/* pq_getmsg*: insufficient/trailing data raise the same errcode class the
 * real pqformat.c does (ERRCODE_PROTOCOL_VIOLATION -> its own class). */
#define PG_TS_ERR_PROTOCOL 8

static void
pg_ts_msg_fail(void)
{
	pg_dt_pending = PG_TS_ERR_PROTOCOL;
	ereport(ERROR, (errcode(PG_TS_ERR_PROTOCOL)));
}

static uint32
pq_getmsgint(StringInfo msg, int b)
{
	uint32		v = 0;
	int			i;

	if (b != 4 || msg->cursor + b > msg->len)
		pg_ts_msg_fail();
	for (i = 0; i < b; i++)
		v = (v << 8) | (unsigned char) msg->data[msg->cursor++];
	return v;
}

static int64
pq_getmsgint64(StringInfo msg)
{
	uint64		hi,
				lo;

	if (msg->cursor + 8 > msg->len)
		pg_ts_msg_fail();
	hi = pq_getmsgint(msg, 4);
	lo = pq_getmsgint(msg, 4);
	return (int64) ((hi << 32) | lo);
}

static void
initReadOnlyStringInfo(StringInfo str, char *data, int len)
{
	str->data = data;
	str->len = len;
	str->maxlen = 0;
	str->cursor = 0;
}

static void
pq_getmsgend(StringInfo msg)
{
	if (msg->cursor != msg->len)
		pg_ts_msg_fail();
}

/* ---- text helpers (plumbing) ---- */
static text *
pg_ts_text(const char *s, int len)
{
	text	   *t = (text *) pg_dt_palloc((size_t) len + 4);

	t->vl_len_ = ((uint32) (len + 4)) << 2;
	memcpy(t->vl_dat, s, (size_t) len);
	return t;
}

static void
text_to_cstring_buffer(const text *src, char *dst, size_t dst_len)
{
	size_t		src_len = (size_t) VARSIZE_ANY_EXHDR(src);

	if (dst_len > 0)
	{
		if (src_len >= dst_len)
			src_len = dst_len - 1;
		memcpy(dst, VARDATA_ANY(src), src_len);
		dst[src_len] = '\0';
	}
}

static char *
text_to_cstring(const text *t)
{
	int			len = VARSIZE_ANY_EXHDR(t);
	char	   *s = pg_dt_palloc((size_t) len + 1);

	memcpy(s, VARDATA_ANY(t), (size_t) len);
	s[len] = '\0';
	return s;
}

/* ---- forward prototypes for the verbatim timestamp.c bodies ---- */
int32		anytimestamp_typmod_check(bool istz, int32 typmod);
Datum		timestamp_in(PG_FUNCTION_ARGS);
Datum		timestamp_out(PG_FUNCTION_ARGS);
Datum		timestamp_recv(PG_FUNCTION_ARGS);
Datum		timestamp_send(PG_FUNCTION_ARGS);
Datum		timestamptz_in(PG_FUNCTION_ARGS);
Datum		timestamptz_out(PG_FUNCTION_ARGS);
Datum		timestamptz_recv(PG_FUNCTION_ARGS);
Datum		timestamptz_send(PG_FUNCTION_ARGS);
bool		AdjustTimestampForTypmod(Timestamp *time, int32 typmod, Node *escontext);
Datum		timestamp_scale(PG_FUNCTION_ARGS);
void		EncodeSpecialTimestamp(Timestamp dt, char *str);
Datum		interval_in(PG_FUNCTION_ARGS);
Datum		interval_out(PG_FUNCTION_ARGS);
Datum		interval_recv(PG_FUNCTION_ARGS);
Datum		interval_send(PG_FUNCTION_ARGS);
Datum		interval_scale(PG_FUNCTION_ARGS);
static bool AdjustIntervalForTypmod(Interval *interval, int32 typmod, Node *escontext);
static void EncodeSpecialInterval(const Interval *interval, char *str);
int			tm2timestamp(struct pg_tm *tm, fsec_t fsec, int *tzp, Timestamp *result);
static Timestamp dt2local(Timestamp dt, int timezone);
int			itm2interval(struct pg_itm *itm, Interval *span);
int			itmin2interval(struct pg_itm_in *itm_in, Interval *span);
TimestampTz timestamp2timestamptz_opt_overflow(Timestamp timestamp, int *overflow);
static TimestampTz timestamp2timestamptz(Timestamp timestamp);
static Timestamp timestamptz2timestamp(TimestampTz timestamp);
static int	parse_sane_timezone(struct pg_tm *tm, text *zone);
static Timestamp make_timestamp_internal(int year, int month, int day,
										 int hour, int min, double sec);
Datum		make_timestamp(PG_FUNCTION_ARGS);
Datum		make_timestamptz(PG_FUNCTION_ARGS);
Datum		make_timestamptz_at_timezone(PG_FUNCTION_ARGS);
Datum		make_interval(PG_FUNCTION_ARGS);
static pg_tz *lookup_timezone(text *zone);
int			DecodeTimezoneName(const char *tzname, int *offset, pg_tz **tz);
pg_tz	   *DecodeTimezoneNameToTz(const char *tzname);
int			timestamp_cmp_internal(Timestamp dt1, Timestamp dt2);
Datum		timestamp_smaller(PG_FUNCTION_ARGS);
Datum		timestamp_larger(PG_FUNCTION_ARGS);
static int	interval_cmp_internal(const Interval *interval1, const Interval *interval2);
static int	interval_sign(const Interval *interval);
Datum		interval_smaller(PG_FUNCTION_ARGS);
Datum		interval_larger(PG_FUNCTION_ARGS);
Datum		timestamp_mi(PG_FUNCTION_ARGS);
Datum		timestamp_pl_interval(PG_FUNCTION_ARGS);
Datum		timestamp_mi_interval(PG_FUNCTION_ARGS);
static TimestampTz timestamptz_pl_interval_internal(TimestampTz timestamp,
													Interval *span,
													pg_tz *attimezone);
static TimestampTz timestamptz_mi_interval_internal(TimestampTz timestamp,
													Interval *span,
													pg_tz *attimezone);
Datum		timestamptz_pl_interval(PG_FUNCTION_ARGS);
Datum		timestamptz_mi_interval(PG_FUNCTION_ARGS);
static void interval_um_internal(const Interval *interval, Interval *result);
Datum		interval_um(PG_FUNCTION_ARGS);
Datum		interval_pl(PG_FUNCTION_ARGS);
Datum		interval_mi(PG_FUNCTION_ARGS);
Datum		interval_mul(PG_FUNCTION_ARGS);
Datum		mul_d_interval(PG_FUNCTION_ARGS);
Datum		interval_div(PG_FUNCTION_ARGS);
Datum		interval_justify_interval(PG_FUNCTION_ARGS);
Datum		interval_justify_hours(PG_FUNCTION_ARGS);
Datum		interval_justify_days(PG_FUNCTION_ARGS);
Datum		timestamp_age(PG_FUNCTION_ARGS);
Datum		timestamptz_age(PG_FUNCTION_ARGS);
Datum		timestamp_bin(PG_FUNCTION_ARGS);
Datum		timestamptz_bin(PG_FUNCTION_ARGS);
Datum		timestamp_izone(PG_FUNCTION_ARGS);
Datum		timestamptz_izone(PG_FUNCTION_ARGS);
Datum		timestamp_trunc(PG_FUNCTION_ARGS);
static TimestampTz timestamptz_trunc_internal(text *units, TimestampTz timestamp,
											  pg_tz *tzp);
Datum		timestamptz_trunc(PG_FUNCTION_ARGS);
Datum		timestamptz_trunc_zone(PG_FUNCTION_ARGS);
Datum		interval_trunc(PG_FUNCTION_ARGS);
static float8 NonFiniteTimestampTzPart(int type, int unit, char *lowunits,
									   bool isNegative, bool isTz);
static Datum timestamp_part_common(PG_FUNCTION_ARGS, bool retnumeric);
Datum		timestamp_part(PG_FUNCTION_ARGS);
Datum		extract_timestamp(PG_FUNCTION_ARGS);
static Datum timestamptz_part_common(PG_FUNCTION_ARGS, bool retnumeric);
Datum		timestamptz_part(PG_FUNCTION_ARGS);
Datum		extract_timestamptz(PG_FUNCTION_ARGS);
static float8 NonFiniteIntervalPart(int type, int unit, char *lowunits,
									bool isNegative);
static Datum interval_part_common(PG_FUNCTION_ARGS, bool retnumeric);
Datum		interval_part(PG_FUNCTION_ARGS);
Datum		extract_interval(PG_FUNCTION_ARGS);
Datum		interval_avg_combine(PG_FUNCTION_ARGS);
Datum		interval_avg_serialize(PG_FUNCTION_ARGS);
Datum		interval_avg_deserialize(PG_FUNCTION_ARGS);
Datum		interval_avg(PG_FUNCTION_ARGS);
Datum		interval_sum(PG_FUNCTION_ARGS);

/* ==== the verbatim vendored timestamp.c bodies ==== */

#include "pg_timestamp_verbatim.inc"

/* ========== fuzz-facing driver entries (NOT Postgres code) ========== */

static void
pg_ts_reset(int style, int order, int istyle)
{
	pg_diff_errcode = 0;
	pg_dt_pending = 0;
	pg_dt_tzset_nongmt = 0;
	pg_dt_tzset_name[0] = '\0';
	pg_dt_arena_off = 0;
	pg_ts_num_set = 0;
	pg_ts_numchain = 0;
	DateStyle = style;
	DateOrder = order;
	IntervalStyle = istyle;
}

static _Thread_local struct FunctionCallInfoBaseData pg_ts_fcinfo_data;

static FunctionCallInfo
pg_ts_fcinfo(void)
{
	memset(&pg_ts_fcinfo_data, 0, sizeof(pg_ts_fcinfo_data));
	return &pg_ts_fcinfo_data;
}

int
pg_tsdiff_timestamp_in(const char *str, int32 typmod, int style, int order,
					   int tz, int64 *out)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	FunctionCallInfo fcinfo = pg_ts_fcinfo();
	Datum		d;

	pg_ts_reset(style, order, INTSTYLE_POSTGRES);
	if (setjmp(pg_dt_jmp))
		return pg_diff_errcode;
	fcinfo->args[0].value = PointerGetDatum(str);
	fcinfo->args[1].value = Int32GetDatum(0);	/* typioparam, unused */
	fcinfo->args[2].value = Int32GetDatum(typmod);
	d = tz ? timestamptz_in(fcinfo) : timestamp_in(fcinfo);
	*out = DatumGetInt64(d);
	return 0;
}

int
pg_tsdiff_timestamp_out(int64 ts, int style, int order, int tz, char *buf)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	FunctionCallInfo fcinfo = pg_ts_fcinfo();
	Datum		d;

	pg_ts_reset(style, order, INTSTYLE_POSTGRES);
	if (setjmp(pg_dt_jmp))
		return pg_diff_errcode;
	fcinfo->args[0].value = Int64GetDatum(ts);
	d = tz ? timestamptz_out(fcinfo) : timestamp_out(fcinfo);
	strcpy(buf, (const char *) DatumGetPointer(d));
	return 0;
}

int
pg_tsdiff_interval_in(const char *str, int32 typmod, int istyle,
					  int64 *t, int32 *day, int32 *month)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	FunctionCallInfo fcinfo = pg_ts_fcinfo();
	Interval   *iv;

	pg_ts_reset(USE_ISO_DATES, DATEORDER_MDY, istyle);
	if (setjmp(pg_dt_jmp))
		return pg_diff_errcode;
	fcinfo->args[0].value = PointerGetDatum(str);
	fcinfo->args[1].value = Int32GetDatum(0);
	fcinfo->args[2].value = Int32GetDatum(typmod);
	iv = DatumGetIntervalP(interval_in(fcinfo));
	*t = iv->time;
	*day = iv->day;
	*month = iv->month;
	return 0;
}

int
pg_tsdiff_interval_out(int64 t, int32 day, int32 month, int istyle, char *buf)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	FunctionCallInfo fcinfo = pg_ts_fcinfo();
	Interval	iv;
	Datum		d;

	pg_ts_reset(USE_ISO_DATES, DATEORDER_MDY, istyle);
	if (setjmp(pg_dt_jmp))
		return pg_diff_errcode;
	iv.time = t;
	iv.day = day;
	iv.month = month;
	fcinfo->args[0].value = PointerGetDatum(&iv);
	d = interval_out(fcinfo);
	strcpy(buf, (const char *) DatumGetPointer(d));
	return 0;
}

int
pg_tsdiff_timestamp_recv(const unsigned char *bytes, int len, int32 typmod,
						 int tz, int64 *out)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	FunctionCallInfo fcinfo = pg_ts_fcinfo();
	StringInfoData buf;
	Datum		d;

	pg_ts_reset(USE_ISO_DATES, DATEORDER_MDY, INTSTYLE_POSTGRES);
	if (setjmp(pg_dt_jmp))
		return pg_diff_errcode;
	buf.data = (char *) bytes;
	buf.len = len;
	buf.maxlen = len;
	buf.cursor = 0;
	fcinfo->args[0].value = PointerGetDatum(&buf);
	fcinfo->args[1].value = Int32GetDatum(0);
	fcinfo->args[2].value = Int32GetDatum(typmod);
	d = tz ? timestamptz_recv(fcinfo) : timestamp_recv(fcinfo);
	*out = DatumGetInt64(d);
	return 0;
}

int
pg_tsdiff_timestamp_send(int64 ts, unsigned char *out8)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	FunctionCallInfo fcinfo = pg_ts_fcinfo();
	bytea	   *b;

	pg_ts_reset(USE_ISO_DATES, DATEORDER_MDY, INTSTYLE_POSTGRES);
	if (setjmp(pg_dt_jmp))
		return pg_diff_errcode;
	fcinfo->args[0].value = Int64GetDatum(ts);
	b = (bytea *) DatumGetPointer(timestamp_send(fcinfo));
	assert(VARSIZE_ANY_EXHDR(b) == 8);
	memcpy(out8, VARDATA_ANY(b), 8);
	return 0;
}

int
pg_tsdiff_interval_recv(const unsigned char *bytes, int len, int32 typmod,
						int64 *t, int32 *day, int32 *month)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	FunctionCallInfo fcinfo = pg_ts_fcinfo();
	StringInfoData buf;
	Interval   *iv;

	pg_ts_reset(USE_ISO_DATES, DATEORDER_MDY, INTSTYLE_POSTGRES);
	if (setjmp(pg_dt_jmp))
		return pg_diff_errcode;
	buf.data = (char *) bytes;
	buf.len = len;
	buf.maxlen = len;
	buf.cursor = 0;
	fcinfo->args[0].value = PointerGetDatum(&buf);
	fcinfo->args[1].value = Int32GetDatum(0);
	fcinfo->args[2].value = Int32GetDatum(typmod);
	iv = DatumGetIntervalP(interval_recv(fcinfo));
	*t = iv->time;
	*day = iv->day;
	*month = iv->month;
	return 0;
}

int
pg_tsdiff_interval_send(int64 t, int32 day, int32 month, unsigned char *out16)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	FunctionCallInfo fcinfo = pg_ts_fcinfo();
	Interval	iv;
	bytea	   *b;

	pg_ts_reset(USE_ISO_DATES, DATEORDER_MDY, INTSTYLE_POSTGRES);
	if (setjmp(pg_dt_jmp))
		return pg_diff_errcode;
	iv.time = t;
	iv.day = day;
	iv.month = month;
	fcinfo->args[0].value = PointerGetDatum(&iv);
	b = (bytea *) DatumGetPointer(interval_send(fcinfo));
	assert(VARSIZE_ANY_EXHDR(b) == 16);
	memcpy(out16, VARDATA_ANY(b), 16);
	return 0;
}

int
pg_tsdiff_timestamp_scale(int64 ts, int32 typmod, int64 *out)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	FunctionCallInfo fcinfo = pg_ts_fcinfo();

	pg_ts_reset(USE_ISO_DATES, DATEORDER_MDY, INTSTYLE_POSTGRES);
	if (setjmp(pg_dt_jmp))
		return pg_diff_errcode;
	fcinfo->args[0].value = Int64GetDatum(ts);
	fcinfo->args[1].value = Int32GetDatum(typmod);
	*out = DatumGetInt64(timestamp_scale(fcinfo));
	return 0;
}

int
pg_tsdiff_interval_scale(int64 t, int32 day, int32 month, int32 typmod,
						 int64 *ot, int32 *od, int32 *om)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	FunctionCallInfo fcinfo = pg_ts_fcinfo();
	Interval	iv;
	Interval   *r;

	pg_ts_reset(USE_ISO_DATES, DATEORDER_MDY, INTSTYLE_POSTGRES);
	if (setjmp(pg_dt_jmp))
		return pg_diff_errcode;
	iv.time = t;
	iv.day = day;
	iv.month = month;
	fcinfo->args[0].value = PointerGetDatum(&iv);
	fcinfo->args[1].value = Int32GetDatum(typmod);
	r = DatumGetIntervalP(interval_scale(fcinfo));
	*ot = r->time;
	*od = r->day;
	*om = r->month;
	return 0;
}

int
pg_tsdiff_timestamp_trunc(const char *units, int ulen, int64 ts, int tz,
						  int64 *out)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	FunctionCallInfo fcinfo = pg_ts_fcinfo();
	Datum		d;

	pg_ts_reset(USE_ISO_DATES, DATEORDER_MDY, INTSTYLE_POSTGRES);
	if (setjmp(pg_dt_jmp))
		return pg_diff_errcode;
	fcinfo->args[0].value = PointerGetDatum(pg_ts_text(units, ulen));
	fcinfo->args[1].value = Int64GetDatum(ts);
	d = tz ? timestamptz_trunc(fcinfo) : timestamp_trunc(fcinfo);
	*out = DatumGetInt64(d);
	return 0;
}

int
pg_tsdiff_timestamptz_trunc_zone(const char *units, int ulen,
								 const char *zone, int zlen,
								 int64 ts, int64 *out)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	FunctionCallInfo fcinfo = pg_ts_fcinfo();

	pg_ts_reset(USE_ISO_DATES, DATEORDER_MDY, INTSTYLE_POSTGRES);
	if (setjmp(pg_dt_jmp))
		return pg_diff_errcode;
	fcinfo->args[0].value = PointerGetDatum(pg_ts_text(units, ulen));
	fcinfo->args[1].value = Int64GetDatum(ts);
	fcinfo->args[2].value = PointerGetDatum(pg_ts_text(zone, zlen));
	*out = DatumGetInt64(timestamptz_trunc_zone(fcinfo));
	return 0;
}

int
pg_tsdiff_interval_trunc(const char *units, int ulen,
						 int64 t, int32 day, int32 month,
						 int64 *ot, int32 *od, int32 *om)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	FunctionCallInfo fcinfo = pg_ts_fcinfo();
	Interval	iv;
	Interval   *r;

	pg_ts_reset(USE_ISO_DATES, DATEORDER_MDY, INTSTYLE_POSTGRES);
	if (setjmp(pg_dt_jmp))
		return pg_diff_errcode;
	iv.time = t;
	iv.day = day;
	iv.month = month;
	fcinfo->args[0].value = PointerGetDatum(pg_ts_text(units, ulen));
	fcinfo->args[1].value = PointerGetDatum(&iv);
	r = DatumGetIntervalP(interval_trunc(fcinfo));
	*ot = r->time;
	*od = r->day;
	*om = r->month;
	return 0;
}

/* part/extract: retnumeric plane reports the recorded numeric-constructor
 * arguments; numchain flags the epoch/julian carve (verdict-only compare). */
int
pg_tsdiff_ts_part(const char *units, int ulen, int64 ts, int tz,
				  int retnumeric, double *fval, int *isnull,
				  int64 *nval, int *nlog10, int *numset, int *numchain)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	FunctionCallInfo fcinfo = pg_ts_fcinfo();
	Datum		d;

	pg_ts_reset(USE_ISO_DATES, DATEORDER_MDY, INTSTYLE_POSTGRES);
	if (setjmp(pg_dt_jmp))
		return pg_diff_errcode;
	fcinfo->args[0].value = PointerGetDatum(pg_ts_text(units, ulen));
	fcinfo->args[1].value = Int64GetDatum(ts);
	if (retnumeric)
		d = tz ? extract_timestamptz(fcinfo) : extract_timestamp(fcinfo);
	else
		d = tz ? timestamptz_part(fcinfo) : timestamp_part(fcinfo);
	*isnull = fcinfo->isnull ? 1 : 0;
	*fval = (!fcinfo->isnull && !retnumeric) ? DatumGetFloat8(d) : 0.0;
	*nval = pg_ts_num_val;
	*nlog10 = pg_ts_num_log10;
	*numset = pg_ts_num_set;
	*numchain = pg_ts_numchain;
	return 0;
}

int
pg_tsdiff_interval_part(const char *units, int ulen,
						int64 t, int32 day, int32 month,
						int retnumeric, double *fval, int *isnull,
						int64 *nval, int *nlog10, int *numset, int *numchain)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	FunctionCallInfo fcinfo = pg_ts_fcinfo();
	Interval	iv;
	Datum		d;

	pg_ts_reset(USE_ISO_DATES, DATEORDER_MDY, INTSTYLE_POSTGRES);
	if (setjmp(pg_dt_jmp))
		return pg_diff_errcode;
	iv.time = t;
	iv.day = day;
	iv.month = month;
	fcinfo->args[0].value = PointerGetDatum(pg_ts_text(units, ulen));
	fcinfo->args[1].value = PointerGetDatum(&iv);
	d = retnumeric ? extract_interval(fcinfo) : interval_part(fcinfo);
	*isnull = fcinfo->isnull ? 1 : 0;
	*fval = (!fcinfo->isnull && !retnumeric) ? DatumGetFloat8(d) : 0.0;
	*nval = pg_ts_num_val;
	*nlog10 = pg_ts_num_log10;
	*numset = pg_ts_num_set;
	*numchain = pg_ts_numchain;
	return 0;
}

int
pg_tsdiff_timestamp_age(int64 a, int64 b, int tz,
						int64 *ot, int32 *od, int32 *om)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	FunctionCallInfo fcinfo = pg_ts_fcinfo();
	Interval   *r;

	pg_ts_reset(USE_ISO_DATES, DATEORDER_MDY, INTSTYLE_POSTGRES);
	if (setjmp(pg_dt_jmp))
		return pg_diff_errcode;
	fcinfo->args[0].value = Int64GetDatum(a);
	fcinfo->args[1].value = Int64GetDatum(b);
	r = DatumGetIntervalP(tz ? timestamptz_age(fcinfo) : timestamp_age(fcinfo));
	*ot = r->time;
	*od = r->day;
	*om = r->month;
	return 0;
}

int
pg_tsdiff_make_timestamp(int32 y, int32 mo, int32 d, int32 h, int32 mi,
						 double sec, int tz, int64 *out)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	FunctionCallInfo fcinfo = pg_ts_fcinfo();
	Datum		r;

	pg_ts_reset(USE_ISO_DATES, DATEORDER_MDY, INTSTYLE_POSTGRES);
	if (setjmp(pg_dt_jmp))
		return pg_diff_errcode;
	fcinfo->args[0].value = Int32GetDatum(y);
	fcinfo->args[1].value = Int32GetDatum(mo);
	fcinfo->args[2].value = Int32GetDatum(d);
	fcinfo->args[3].value = Int32GetDatum(h);
	fcinfo->args[4].value = Int32GetDatum(mi);
	fcinfo->args[5].value = Float8GetDatum(sec);
	r = tz ? make_timestamptz(fcinfo) : make_timestamp(fcinfo);
	*out = DatumGetInt64(r);
	return 0;
}

int
pg_tsdiff_make_timestamptz_at_timezone(int32 y, int32 mo, int32 d, int32 h,
									   int32 mi, double sec,
									   const char *zone, int zlen, int64 *out)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	FunctionCallInfo fcinfo = pg_ts_fcinfo();

	pg_ts_reset(USE_ISO_DATES, DATEORDER_MDY, INTSTYLE_POSTGRES);
	if (setjmp(pg_dt_jmp))
		return pg_diff_errcode;
	fcinfo->args[0].value = Int32GetDatum(y);
	fcinfo->args[1].value = Int32GetDatum(mo);
	fcinfo->args[2].value = Int32GetDatum(d);
	fcinfo->args[3].value = Int32GetDatum(h);
	fcinfo->args[4].value = Int32GetDatum(mi);
	fcinfo->args[5].value = Float8GetDatum(sec);
	fcinfo->args[6].value = PointerGetDatum(pg_ts_text(zone, zlen));
	*out = DatumGetInt64(make_timestamptz_at_timezone(fcinfo));
	return 0;
}

int
pg_tsdiff_make_interval(int32 y, int32 mo, int32 w, int32 d, int32 h,
						int32 mi, double sec,
						int64 *ot, int32 *od, int32 *om)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	FunctionCallInfo fcinfo = pg_ts_fcinfo();
	Interval   *r;

	pg_ts_reset(USE_ISO_DATES, DATEORDER_MDY, INTSTYLE_POSTGRES);
	if (setjmp(pg_dt_jmp))
		return pg_diff_errcode;
	fcinfo->args[0].value = Int32GetDatum(y);
	fcinfo->args[1].value = Int32GetDatum(mo);
	fcinfo->args[2].value = Int32GetDatum(w);
	fcinfo->args[3].value = Int32GetDatum(d);
	fcinfo->args[4].value = Int32GetDatum(h);
	fcinfo->args[5].value = Int32GetDatum(mi);
	fcinfo->args[6].value = Float8GetDatum(sec);
	r = DatumGetIntervalP(make_interval(fcinfo));
	*ot = r->time;
	*od = r->day;
	*om = r->month;
	return 0;
}

int
pg_tsdiff_interval_muldiv(int isdiv, int64 t, int32 day, int32 month,
						  double factor, int64 *ot, int32 *od, int32 *om)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	FunctionCallInfo fcinfo = pg_ts_fcinfo();
	Interval	iv;
	Interval   *r;

	pg_ts_reset(USE_ISO_DATES, DATEORDER_MDY, INTSTYLE_POSTGRES);
	if (setjmp(pg_dt_jmp))
		return pg_diff_errcode;
	iv.time = t;
	iv.day = day;
	iv.month = month;
	fcinfo->args[0].value = PointerGetDatum(&iv);
	fcinfo->args[1].value = Float8GetDatum(factor);
	r = DatumGetIntervalP(isdiv ? interval_div(fcinfo) : interval_mul(fcinfo));
	*ot = r->time;
	*od = r->day;
	*om = r->month;
	return 0;
}

int
pg_tsdiff_timestamp_mi(int64 a, int64 b, int64 *ot, int32 *od, int32 *om)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	FunctionCallInfo fcinfo = pg_ts_fcinfo();
	Interval   *r;

	pg_ts_reset(USE_ISO_DATES, DATEORDER_MDY, INTSTYLE_POSTGRES);
	if (setjmp(pg_dt_jmp))
		return pg_diff_errcode;
	fcinfo->args[0].value = Int64GetDatum(a);
	fcinfo->args[1].value = Int64GetDatum(b);
	r = DatumGetIntervalP(timestamp_mi(fcinfo));
	*ot = r->time;
	*od = r->day;
	*om = r->month;
	return 0;
}

/* Pure difference helpers (no ereport paths; no setjmp needed). The C
 * TimestampDifference/TimestampDifferenceExceeds subtract raw (UB on
 * overflow; -fwrapv here) — the harness fences the compared domain to
 * non-overflowing (stop - start) pairs for those two entries. */
int
pg_tsdiff_timestamp_difference(int64 start, int64 stop, int64 *osecs, int32 *ousecs)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	long		secs;
	int			usecs;

	TimestampDifference(start, stop, &secs, &usecs);
	*osecs = (int64) secs;
	*ousecs = (int32) usecs;
	return 0;
}

int64
pg_tsdiff_timestamp_difference_ms(int64 start, int64 stop)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	return (int64) TimestampDifferenceMilliseconds(start, stop);
}

int
pg_tsdiff_timestamp_difference_exceeds(int64 start, int64 stop, int32 msec)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	return TimestampDifferenceExceeds(start, stop, msec) ? 1 : 0;
}

int
pg_tsdiff_timestamp_difference_exceeds_secs(int64 start, int64 stop, int32 threshold_sec)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	return TimestampDifferenceExceedsSeconds(start, stop, threshold_sec) ? 1 : 0;
}

int
pg_tsdiff_timestamp_plmi_interval(int tz, int ismi, int64 ts,
								  int64 t, int32 day, int32 month, int64 *out)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	FunctionCallInfo fcinfo = pg_ts_fcinfo();
	Interval	iv;
	Datum		d;

	pg_ts_reset(USE_ISO_DATES, DATEORDER_MDY, INTSTYLE_POSTGRES);
	if (setjmp(pg_dt_jmp))
		return pg_diff_errcode;
	iv.time = t;
	iv.day = day;
	iv.month = month;
	fcinfo->args[0].value = Int64GetDatum(ts);
	fcinfo->args[1].value = PointerGetDatum(&iv);
	if (tz)
		d = ismi ? timestamptz_mi_interval(fcinfo) : timestamptz_pl_interval(fcinfo);
	else
		d = ismi ? timestamp_mi_interval(fcinfo) : timestamp_pl_interval(fcinfo);
	*out = DatumGetInt64(d);
	return 0;
}

int
pg_tsdiff_justify(int which, int64 t, int32 day, int32 month,
				  int64 *ot, int32 *od, int32 *om)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	FunctionCallInfo fcinfo = pg_ts_fcinfo();
	Interval	iv;
	Interval   *r;
	Datum		d;

	pg_ts_reset(USE_ISO_DATES, DATEORDER_MDY, INTSTYLE_POSTGRES);
	if (setjmp(pg_dt_jmp))
		return pg_diff_errcode;
	iv.time = t;
	iv.day = day;
	iv.month = month;
	fcinfo->args[0].value = PointerGetDatum(&iv);
	if (which == 0)
		d = interval_justify_interval(fcinfo);
	else if (which == 1)
		d = interval_justify_hours(fcinfo);
	else
		d = interval_justify_days(fcinfo);
	r = DatumGetIntervalP(d);
	*ot = r->time;
	*od = r->day;
	*om = r->month;
	return 0;
}

int
pg_tsdiff_timestamp_bin(int tz, int64 st, int32 sd, int32 sm,
						int64 ts, int64 origin, int64 *out)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	FunctionCallInfo fcinfo = pg_ts_fcinfo();
	Interval	stride;
	Datum		d;

	pg_ts_reset(USE_ISO_DATES, DATEORDER_MDY, INTSTYLE_POSTGRES);
	if (setjmp(pg_dt_jmp))
		return pg_diff_errcode;
	stride.time = st;
	stride.day = sd;
	stride.month = sm;
	fcinfo->args[0].value = PointerGetDatum(&stride);
	fcinfo->args[1].value = Int64GetDatum(ts);
	fcinfo->args[2].value = Int64GetDatum(origin);
	d = tz ? timestamptz_bin(fcinfo) : timestamp_bin(fcinfo);
	*out = DatumGetInt64(d);
	return 0;
}

int
pg_tsdiff_interval_um(int64 t, int32 day, int32 month,
					  int64 *ot, int32 *od, int32 *om)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	FunctionCallInfo fcinfo = pg_ts_fcinfo();
	Interval	iv;
	Interval   *r;

	pg_ts_reset(USE_ISO_DATES, DATEORDER_MDY, INTSTYLE_POSTGRES);
	if (setjmp(pg_dt_jmp))
		return pg_diff_errcode;
	iv.time = t;
	iv.day = day;
	iv.month = month;
	fcinfo->args[0].value = PointerGetDatum(&iv);
	r = DatumGetIntervalP(interval_um(fcinfo));
	*ot = r->time;
	*od = r->day;
	*om = r->month;
	return 0;
}

int
pg_tsdiff_interval_plmi(int ismi, int64 t1, int32 d1, int32 m1,
						int64 t2, int32 d2, int32 m2,
						int64 *ot, int32 *od, int32 *om)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	FunctionCallInfo fcinfo = pg_ts_fcinfo();
	Interval	a,
				b;
	Interval   *r;

	pg_ts_reset(USE_ISO_DATES, DATEORDER_MDY, INTSTYLE_POSTGRES);
	if (setjmp(pg_dt_jmp))
		return pg_diff_errcode;
	a.time = t1;
	a.day = d1;
	a.month = m1;
	b.time = t2;
	b.day = d2;
	b.month = m2;
	fcinfo->args[0].value = PointerGetDatum(&a);
	fcinfo->args[1].value = PointerGetDatum(&b);
	r = DatumGetIntervalP(ismi ? interval_mi(fcinfo) : interval_pl(fcinfo));
	*ot = r->time;
	*od = r->day;
	*om = r->month;
	return 0;
}

int
pg_tsdiff_interval_minmax(int larger, int64 t1, int32 d1, int32 m1,
						  int64 t2, int32 d2, int32 m2,
						  int64 *ot, int32 *od, int32 *om, int *cmp)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	FunctionCallInfo fcinfo = pg_ts_fcinfo();
	Interval	a,
				b;
	Interval   *r;

	pg_ts_reset(USE_ISO_DATES, DATEORDER_MDY, INTSTYLE_POSTGRES);
	if (setjmp(pg_dt_jmp))
		return pg_diff_errcode;
	a.time = t1;
	a.day = d1;
	a.month = m1;
	b.time = t2;
	b.day = d2;
	b.month = m2;
	fcinfo->args[0].value = PointerGetDatum(&a);
	fcinfo->args[1].value = PointerGetDatum(&b);
	r = DatumGetIntervalP(larger ? interval_larger(fcinfo) : interval_smaller(fcinfo));
	*ot = r->time;
	*od = r->day;
	*om = r->month;
	*cmp = interval_cmp_internal(&a, &b);
	return 0;
}

int
pg_tsdiff_timestamp_izone(int tz, int64 zt, int32 zd, int32 zm,
						  int64 ts, int64 *out)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	FunctionCallInfo fcinfo = pg_ts_fcinfo();
	Interval	zone;
	Datum		d;

	pg_ts_reset(USE_ISO_DATES, DATEORDER_MDY, INTSTYLE_POSTGRES);
	if (setjmp(pg_dt_jmp))
		return pg_diff_errcode;
	zone.time = zt;
	zone.day = zd;
	zone.month = zm;
	fcinfo->args[0].value = PointerGetDatum(&zone);
	fcinfo->args[1].value = Int64GetDatum(ts);
	d = tz ? timestamptz_izone(fcinfo) : timestamp_izone(fcinfo);
	*out = DatumGetInt64(d);
	return 0;
}

/* interval aggregate core: accum/discard/combine over an explicit state. */
int
pg_tsdiff_interval_agg(int op,
					   int64 *N, int64 *st, int32 *sd, int32 *sm,
					   int64 *pinf, int64 *ninf,
					   int64 t, int32 day, int32 month)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	IntervalAggState state;
	Interval	nv;

	pg_ts_reset(USE_ISO_DATES, DATEORDER_MDY, INTSTYLE_POSTGRES);
	if (setjmp(pg_dt_jmp))
		return pg_diff_errcode;
	memset(&state, 0, sizeof(state));
	state.N = *N;
	state.sumX.time = *st;
	state.sumX.day = *sd;
	state.sumX.month = *sm;
	state.pInfcount = *pinf;
	state.nInfcount = *ninf;
	nv.time = t;
	nv.day = day;
	nv.month = month;
	if (op == 0)
		do_interval_accum(&state, &nv);
	else
		do_interval_discard(&state, &nv);
	*N = state.N;
	*st = state.sumX.time;
	*sd = state.sumX.day;
	*sm = state.sumX.month;
	*pinf = state.pInfcount;
	*ninf = state.nInfcount;
	return 0;
}

int
pg_tsdiff_interval_avg_final(int issum,
							 int64 N, int64 st, int32 sd, int32 sm,
							 int64 pinf, int64 ninf,
							 int64 *ot, int32 *od, int32 *om, int *isnull)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	FunctionCallInfo fcinfo = pg_ts_fcinfo();
	IntervalAggState state;
	Datum		d;

	pg_ts_reset(USE_ISO_DATES, DATEORDER_MDY, INTSTYLE_POSTGRES);
	if (setjmp(pg_dt_jmp))
		return pg_diff_errcode;
	memset(&state, 0, sizeof(state));
	state.N = N;
	state.sumX.time = st;
	state.sumX.day = sd;
	state.sumX.month = sm;
	state.pInfcount = pinf;
	state.nInfcount = ninf;
	fcinfo->args[0].value = PointerGetDatum(&state);
	d = issum ? interval_sum(fcinfo) : interval_avg(fcinfo);
	*isnull = fcinfo->isnull ? 1 : 0;
	if (!fcinfo->isnull)
	{
		Interval   *r = DatumGetIntervalP(d);

		*ot = r->time;
		*od = r->day;
		*om = r->month;
	}
	return 0;
}

int
pg_tsdiff_interval_avg_combine(int64 N1, int64 st1, int32 sd1, int32 sm1,
							   int64 pinf1, int64 ninf1,
							   int64 N2, int64 st2, int32 sd2, int32 sm2,
							   int64 pinf2, int64 ninf2,
							   int64 *N, int64 *st, int32 *sd, int32 *sm,
							   int64 *pinf, int64 *ninf)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	FunctionCallInfo fcinfo = pg_ts_fcinfo();
	IntervalAggState s1,
				s2;
	IntervalAggState *r;

	pg_ts_reset(USE_ISO_DATES, DATEORDER_MDY, INTSTYLE_POSTGRES);
	if (setjmp(pg_dt_jmp))
		return pg_diff_errcode;
	memset(&s1, 0, sizeof(s1));
	memset(&s2, 0, sizeof(s2));
	s1.N = N1;
	s1.sumX.time = st1;
	s1.sumX.day = sd1;
	s1.sumX.month = sm1;
	s1.pInfcount = pinf1;
	s1.nInfcount = ninf1;
	s2.N = N2;
	s2.sumX.time = st2;
	s2.sumX.day = sd2;
	s2.sumX.month = sm2;
	s2.pInfcount = pinf2;
	s2.nInfcount = ninf2;
	fcinfo->args[0].value = PointerGetDatum(&s1);
	fcinfo->args[1].value = PointerGetDatum(&s2);
	r = (IntervalAggState *) DatumGetPointer(interval_avg_combine(fcinfo));
	*N = r->N;
	*st = r->sumX.time;
	*sd = r->sumX.day;
	*sm = r->sumX.month;
	*pinf = r->pInfcount;
	*ninf = r->nInfcount;
	return 0;
}

int
pg_tsdiff_interval_avg_serialize(int64 N, int64 st, int32 sd, int32 sm,
								 int64 pinf, int64 ninf,
								 unsigned char *out, int *outlen)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	FunctionCallInfo fcinfo = pg_ts_fcinfo();
	IntervalAggState state;
	bytea	   *b;
	int			blen;

	pg_ts_reset(USE_ISO_DATES, DATEORDER_MDY, INTSTYLE_POSTGRES);
	if (setjmp(pg_dt_jmp))
		return pg_diff_errcode;
	memset(&state, 0, sizeof(state));
	state.N = N;
	state.sumX.time = st;
	state.sumX.day = sd;
	state.sumX.month = sm;
	state.pInfcount = pinf;
	state.nInfcount = ninf;
	fcinfo->args[0].value = PointerGetDatum(&state);
	b = (bytea *) DatumGetPointer(interval_avg_serialize(fcinfo));
	blen = VARSIZE_ANY_EXHDR(b);
	assert(blen <= 64);
	memcpy(out, VARDATA_ANY(b), (size_t) blen);
	*outlen = blen;
	return 0;
}

int
pg_tsdiff_interval_avg_deserialize(const unsigned char *bytes, int len,
								   int64 *N, int64 *st, int32 *sd, int32 *sm,
								   int64 *pinf, int64 *ninf)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	FunctionCallInfo fcinfo = pg_ts_fcinfo();
	IntervalAggState *r;

	pg_ts_reset(USE_ISO_DATES, DATEORDER_MDY, INTSTYLE_POSTGRES);
	if (setjmp(pg_dt_jmp))
		return pg_diff_errcode;
	fcinfo->args[0].value = PointerGetDatum(pg_ts_text((const char *) bytes, len));
	r = (IntervalAggState *) DatumGetPointer(interval_avg_deserialize(fcinfo));
	*N = r->N;
	*st = r->sumX.time;
	*sd = r->sumX.day;
	*sm = r->sumX.month;
	*pinf = r->pInfcount;
	*ninf = r->nInfcount;
	return 0;
}

/* tz-carve channel (same protocol as lanel's datetime oracle) */
int
pg_tsdiff_tz_carved(void)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	return pg_dt_tzset_nongmt;
}

const char *
pg_tsdiff_tz_carved_name(void)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	return pg_dt_tzset_name;
}

/* numchain flag (extract epoch/julian value-plane carve) */
int
pg_tsdiff_numchain(void)
{
	return pg_ts_numchain;
}
