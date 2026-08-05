/*
 * Vendored PostgreSQL C: date / time / timetz text I/O + constructors +
 * part() — differential-fuzz oracle for the datetime_io_diff,
 * interval_engine_diff and datetime_engine_diff targets (100%-coverage
 * campaign; crate crates/backend/utils/adt/adt_date, parse engine
 * crates/backend/utils/adt/adt_datetime).
 *
 * Provenance (all bodies VERBATIM, extracted mechanically by
 * csrc/extract_verbatim.py into csrc/pg_datetime_verbatim.inc which this
 * file #includes; re-run the script to refresh), all from postgres-src
 * 62d6c7d3df6287f1bd83199c1a746e50d31571a0 (REL_18, the repo's vendored
 * ground-truth checkout ../pgrust-fabled/vendor/postgres-src — PostgreSQL
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
 *     EncodeTimeOnly, AppendTimestampSeconds, EncodeDateTime,
 *     DetermineTimeZoneOffset(+Internal),
 *     DetermineTimeZoneAbbrevOffset(+Internal), TimeZoneAbbrevIsKnown,
 *     ClearTimeZoneAbbrevCache, and the token tables datetktbl/deltatktbl/
 *     day_tab/months/days + lookup caches.
 *   - src/backend/utils/adt/timestamp.c: dt2time, GetEpochTime, interval2itm,
 *     and the ISO week/year calendar helpers isoweek2j, isoweek2date,
 *     isoweekdate2date, date2isoweek, date2isoyear, date2isoyearday.
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
 *     every plane comparison for it (Rust still executes for panic-safety,
 *     bounded by the driver's distinct-zone-name admission budget — pgrust's
 *     pg_tzset cache is process-lifetime, matching pgtz.c's never-evicted
 *     timezone_cache, so unbounded invented names would grow RSS forever).
 *     Those code paths are the tz-database state carve in the routes rows.
 *
 * Shims (plumbing only, never logic):
 *   - ereport/ereturn/errsave -> record the errcode class in the shared
 *     _Thread_local pg_diff_errcode (via a pending slot so non-throwing
 *     WARNING sites do not pollute it) and longjmp out; errmsg/errdetail/
 *     errhint evaluate to 0 with arguments unevaluated (message text is out
 *     of comparison scope). elog(ERROR) -> class 99 (internal).
 *   - palloc/pstrdup/pfree -> per-exec bump arena, reset by pg_dt_reset()
 *     (models PG's per-tuple memory context; pfree is a no-op exactly as
 *     context reset makes it in the C originals' lifetimes; results are
 *     copied out by the pg_diff_* driver entries before the next reset).
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
#include "utils/tzparser.h"
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

/* The non-GMT name this exec asked for, NUL-terminated (empty when none).
 * The driver keys its zone-name admission budget on this: the Rust engine's
 * pg_tzset cache is process-lifetime by design (C parity: pgtz.c's
 * timezone_cache HTAB is never evicted either), so an unbounded stream of
 * distinct fuzzer-invented names would grow RSS without bound. */
_Thread_local char pg_dt_tzset_name[TZ_STRLEN_MAX + 1];

int
pg_diff_datetime_tzset_nongmt(void)
{
	return pg_dt_tzset_nongmt;
}

const char *
pg_diff_datetime_tzset_name(void)
{
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
	pg_time_t	t = *timep;
	pg_time_t	days = t / SECS_PER_DAY;
	pg_time_t	rem = t % SECS_PER_DAY;
	int			y,
				mo,
				d;

	(void) tz;					/* GMT only; pg_tzset admits nothing else */
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
	tmbuf.tm_gmtoff = 0;
	tmbuf.tm_zone = "GMT";
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

/* guc_malloc -> the per-exec arena is WRONG for the abbrev table (it must
 * outlive the exec that installed it, exactly as the real GUC extra does), so
 * it gets its own one-shot static block. LOG level is ignored: the only caller
 * here is the driver's pinned-table install, whose size is a compile-time
 * constant that fits. */
/* elog.h level the vendored guc_malloc call passes through (value irrelevant:
 * the shim ignores it — see pg_dt_guc_malloc). */
#define LOG 15

#define PG_DT_ABBREVTBL_SZ 1024
static _Thread_local char pg_dt_abbrevtbl_block[PG_DT_ABBREVTBL_SZ];

static void *
pg_dt_guc_malloc(int elevel, size_t sz)
{
	(void) elevel;
	if (sz > sizeof(pg_dt_abbrevtbl_block))
		abort();				/* driver contract: pinned table is small */
	return pg_dt_abbrevtbl_block;
}

#define guc_malloc(elevel, sz) pg_dt_guc_malloc(elevel, sz)
#define MAXALIGN(LEN) (((size_t) (LEN) + 7) & ~((size_t) 7))

/* ---- static prototypes for the verbatim bodies below (order-free) ---- */

static pg_tz *FetchDynamicTimeZone(TimeZoneAbbrevTable *tbl, const datetkn *tp,
								   DateTimeErrorExtra *extra);

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
 * (src/backend/utils/adt/datetime.c), except for the _Thread_local storage
 * class, for the same reason as pg_dt_fcinfo_data and the per-exec arena
 * below: in Postgres these are per-BACKEND (process-per-connection), so a
 * process-global is faithful there, but the multi-threaded `cargo test`
 * rails run several drivers in one process — and pg_dt_install_pinned_abbrevs
 * is already per-thread ("install once per thread"), so every thread wrote
 * these shared cells. DecodeTimezoneAbbrev's cache-hit arm matches on
 * tzc->abbrev and then reads tzc->tz, while the fill publishes abbrev BEFORE
 * tz (and InstallTimeZoneAbbrevs memsets the whole cache): a second thread
 * can match a freshly-published name and read a stale or zeroed pg_tz*,
 * which the DYNTZ path then dereferences. That SIGSEGVs on Linux aarch64 —
 * the same publish-order hazard as pg_dt_fcinfo_data, and likewise invisible
 * on macOS (6/6 clean locally, red on the fleet rail baseline).
 * _Thread_local restores the one-backend-per-thread semantics the vendored
 * bodies assume. Storage duration only; no computation is shimmed. */
static _Thread_local TimeZoneAbbrevTable *zoneabbrevtbl = NULL;

/* Caches of recent lookup results in the above tables */

static _Thread_local const datetkn *datecache[MAXDATEFIELDS] = {NULL};

static _Thread_local const datetkn *deltacache[MAXDATEFIELDS] = {NULL};

/* Cache for results of timezone abbreviation lookups */

typedef struct TzAbbrevCache
{
	char		abbrev[TOKMAXLEN + 1];	/* always NUL-terminated */
	char		ftype;			/* TZ, DTZ, or DYNTZ */
	int			offset;			/* GMT offset, if fixed-offset */
	pg_tz	   *tz;				/* relevant zone, if variable-offset */
} TzAbbrevCache;

static _Thread_local TzAbbrevCache tzabbrevcache[MAXDATEFIELDS];

#include "pg_datetime_verbatim.inc"

/* ========== fuzz-facing driver entries (NOT Postgres code) ========== */

static void
pg_dt_reset(int style, int order)
{
	pg_diff_errcode = 0;
	pg_dt_pending = 0;
	pg_dt_tzset_nongmt = 0;
	pg_dt_tzset_name[0] = '\0';
	pg_dt_arena_off = 0;		/* per-exec memory-context reset */
	DateStyle = style;
	DateOrder = order;
}

/* Build a minimal fcinfo; args filled by callers.
 *
 * _Thread_local for the same reason DateStyle/DateOrder above are (and the
 * per-exec arena below): the multi-threaded `cargo test` rails run several
 * drivers at once, and a process-global scratch fcinfo let one thread's
 * memset() land between another's arg store and the vendored entry's
 * PG_GETARG — reading a zeroed Datum as a TimeTzADT/Interval POINTER, which
 * segfaults. (libFuzzer runs one thread per process, so no campaign verdict
 * was affected; found by the datetime_convert_diff rails, whose entries are
 * the first fcinfo users to pass by-reference args.) */
static _Thread_local struct FunctionCallInfoBaseData pg_dt_fcinfo_data;

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
	/* workbuf sized as real interval_in's frame (timestamp.c:908,
	 * `char workbuf[256]`) — NOT date.c's MAXDATELEN+1. Sizing it 129 made
	 * the oracle reject 130..256-field-byte inputs with DTERR_BAD_FORMAT
	 * where real 18.3 interval_in parses on to DTERR_FIELD_OVERFLOW
	 * (known-divergences/interval-decode-sqlstd-dterr-1-vs-2, RESOLVED:
	 * shim defect, pgrust was right; docker postgres:18.3 = 22015). */
	char		workbuf[256];
	char	   *field[MAXDATEFIELDS];
	int			ftype[MAXDATEFIELDS];
	int			nf;
	int			dterr;
	struct pg_itm_in itm_in;

	pg_diff_errcode = 0;
	pg_dt_pending = 0;
	pg_dt_tzset_nongmt = 0;
	pg_dt_tzset_name[0] = '\0';
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

/*
 * ---- datetime_engine_diff entries (EncodeDateTime + ISO week/year) ----
 *
 * EncodeDateTime's only SQL callers (timestamp_out/timestamptz_out) live in
 * adt_timestamp, which this campaign lane has not claimed, so it is compared
 * at the engine level: the pg_tm is staged FIELD BY FIELD from the fuzz input
 * (identically on both sides) and handed to both encoders. tm_mon is fenced
 * to 1..MONTHS_PER_YEAR by the driver, which is EncodeDateTime's own declared
 * contract (datetime.c:4468 `Assert(tm->tm_mon >= 1 && tm->tm_mon <=
 * MONTHS_PER_YEAR)`) — outside it C indexes months[]/days[] out of bounds.
 */
int
pg_diff_encode_datetime(int32 year, int32 mon, int32 mday,
						int32 hour, int32 min, int32 sec, int32 isdst,
						int64 fsec, int print_tz, int32 tz, const char *tzn,
						int style, int order, char *buf, int32 *out_wday)
{
	struct pg_tm tm;

	pg_dt_reset(style, order);
	if (setjmp(pg_dt_jmp))
		return pg_diff_errcode;
	memset(&tm, 0, sizeof(tm));
	tm.tm_year = year;
	tm.tm_mon = mon;
	tm.tm_mday = mday;
	tm.tm_hour = hour;
	tm.tm_min = min;
	tm.tm_sec = sec;
	tm.tm_isdst = isdst;
	EncodeDateTime(&tm, fsec, print_tz ? true : false, tz, tzn, style, buf);
	/* the USE_POSTGRES_DATES arm writes tm_wday back — a compared field */
	*out_wday = tm.tm_wday;
	return 0;
}

int
pg_diff_date2isoweek(int32 year, int32 mon, int32 mday)
{
	return date2isoweek(year, mon, mday);
}

int
pg_diff_date2isoyear(int32 year, int32 mon, int32 mday)
{
	return date2isoyear(year, mon, mday);
}

int
pg_diff_date2isoyearday(int32 year, int32 mon, int32 mday)
{
	return date2isoyearday(year, mon, mday);
}

int
pg_diff_isoweek2j(int32 year, int32 week)
{
	return isoweek2j(year, week);
}

void
pg_diff_isoweek2date(int32 woy, int32 *year, int32 *mon, int32 *mday)
{
	isoweek2date(woy, (int *) year, (int *) mon, (int *) mday);
}

void
pg_diff_isoweekdate2date(int32 isoweek, int32 wday,
						 int32 *year, int32 *mon, int32 *mday)
{
	isoweekdate2date(isoweek, wday, (int *) year, (int *) mon, (int *) mday);
}

/*
 * ---- datetime_convert_diff entries ----
 *
 * timestamp<->date/time/timetz conversions and time/timetz +- interval
 * arithmetic: all twelve are VERBATIM date.c fmgr entry points, driven through
 * the shim fcinfo exactly like time_part/make_time above. Their shared kernel
 * timestamp2tm is verbatim timestamp.c; its tzp!=NULL branch resolves through
 * the GMT pg_localtime shim.
 *
 * Return convention (all entries): 0 = value returned, 1 = SQL NULL (the
 * PG_RETURN_NULL() arms of timestamp_time/timestamptz_time/
 * timestamptz_timetz), otherwise the errcode class (see header).
 */
#define PG_DT_NULLED	1

int
pg_diff_timestamp_date(int64 ts, int32 *out)
{
	FunctionCallInfo fcinfo = pg_dt_fcinfo();
	Datum		d;

	pg_dt_reset(USE_ISO_DATES, DATEORDER_YMD);
	if (setjmp(pg_dt_jmp))
		return pg_diff_errcode;
	fcinfo->args[0].value = Int64GetDatum(ts);
	d = timestamp_date(fcinfo);
	if (fcinfo->isnull)
		return PG_DT_NULLED;
	*out = DatumGetDateADT(d);
	return 0;
}

int
pg_diff_timestamptz_date(int64 ts, int32 *out)
{
	FunctionCallInfo fcinfo = pg_dt_fcinfo();
	Datum		d;

	pg_dt_reset(USE_ISO_DATES, DATEORDER_YMD);
	if (setjmp(pg_dt_jmp))
		return pg_diff_errcode;
	fcinfo->args[0].value = Int64GetDatum(ts);
	d = timestamptz_date(fcinfo);
	if (fcinfo->isnull)
		return PG_DT_NULLED;
	*out = DatumGetDateADT(d);
	return 0;
}

int
pg_diff_timestamp_time(int64 ts, int64 *out)
{
	FunctionCallInfo fcinfo = pg_dt_fcinfo();
	Datum		d;

	pg_dt_reset(USE_ISO_DATES, DATEORDER_YMD);
	if (setjmp(pg_dt_jmp))
		return pg_diff_errcode;
	fcinfo->args[0].value = Int64GetDatum(ts);
	d = timestamp_time(fcinfo);
	if (fcinfo->isnull)
		return PG_DT_NULLED;
	*out = DatumGetTimeADT(d);
	return 0;
}

int
pg_diff_timestamptz_time(int64 ts, int64 *out)
{
	FunctionCallInfo fcinfo = pg_dt_fcinfo();
	Datum		d;

	pg_dt_reset(USE_ISO_DATES, DATEORDER_YMD);
	if (setjmp(pg_dt_jmp))
		return pg_diff_errcode;
	fcinfo->args[0].value = Int64GetDatum(ts);
	d = timestamptz_time(fcinfo);
	if (fcinfo->isnull)
		return PG_DT_NULLED;
	*out = DatumGetTimeADT(d);
	return 0;
}

int
pg_diff_timestamptz_timetz(int64 ts, int64 *out_time, int32 *out_zone)
{
	FunctionCallInfo fcinfo = pg_dt_fcinfo();
	Datum		d;
	TimeTzADT  *r;

	pg_dt_reset(USE_ISO_DATES, DATEORDER_YMD);
	if (setjmp(pg_dt_jmp))
		return pg_diff_errcode;
	fcinfo->args[0].value = Int64GetDatum(ts);
	d = timestamptz_timetz(fcinfo);
	if (fcinfo->isnull)
		return PG_DT_NULLED;
	r = DatumGetTimeTzADTP(d);
	*out_time = r->time;
	*out_zone = r->zone;
	return 0;
}

int
pg_diff_date_timestamptz(int32 date, int64 *out)
{
	FunctionCallInfo fcinfo = pg_dt_fcinfo();
	Datum		d;

	pg_dt_reset(USE_ISO_DATES, DATEORDER_YMD);
	if (setjmp(pg_dt_jmp))
		return pg_diff_errcode;
	fcinfo->args[0].value = DateADTGetDatum(date);
	d = date_timestamptz(fcinfo);
	if (fcinfo->isnull)
		return PG_DT_NULLED;
	*out = DatumGetTimestampTz(d);
	return 0;
}

int
pg_diff_interval_time(int64 time, int32 day, int32 month, int64 *out)
{
	FunctionCallInfo fcinfo = pg_dt_fcinfo();
	Interval	span;
	Datum		d;

	pg_dt_reset(USE_ISO_DATES, DATEORDER_YMD);
	if (setjmp(pg_dt_jmp))
		return pg_diff_errcode;
	span.time = time;
	span.day = day;
	span.month = month;
	fcinfo->args[0].value = IntervalPGetDatum(&span);
	d = interval_time(fcinfo);
	if (fcinfo->isnull)
		return PG_DT_NULLED;
	*out = DatumGetTimeADT(d);
	return 0;
}

/* time +- interval; sub selects pl(0) / mi(1) */
int
pg_diff_time_pm_interval(int sub, int64 time, int64 sp_time, int32 sp_day,
						 int32 sp_month, int64 *out)
{
	FunctionCallInfo fcinfo = pg_dt_fcinfo();
	Interval	span;
	Datum		d;

	pg_dt_reset(USE_ISO_DATES, DATEORDER_YMD);
	if (setjmp(pg_dt_jmp))
		return pg_diff_errcode;
	span.time = sp_time;
	span.day = sp_day;
	span.month = sp_month;
	fcinfo->args[0].value = TimeADTGetDatum(time);
	fcinfo->args[1].value = IntervalPGetDatum(&span);
	d = sub ? time_mi_interval(fcinfo) : time_pl_interval(fcinfo);
	if (fcinfo->isnull)
		return PG_DT_NULLED;
	*out = DatumGetTimeADT(d);
	return 0;
}

/* timetz +- interval; sub selects pl(0) / mi(1) */
int
pg_diff_timetz_pm_interval(int sub, int64 time, int32 zone, int64 sp_time,
						   int32 sp_day, int32 sp_month,
						   int64 *out_time, int32 *out_zone)
{
	FunctionCallInfo fcinfo = pg_dt_fcinfo();
	Interval	span;
	TimeTzADT	arg;
	TimeTzADT  *r;
	Datum		d;

	pg_dt_reset(USE_ISO_DATES, DATEORDER_YMD);
	if (setjmp(pg_dt_jmp))
		return pg_diff_errcode;
	span.time = sp_time;
	span.day = sp_day;
	span.month = sp_month;
	arg.time = time;
	arg.zone = zone;
	fcinfo->args[0].value = TimeTzADTPGetDatum(&arg);
	fcinfo->args[1].value = IntervalPGetDatum(&span);
	d = sub ? timetz_mi_interval(fcinfo) : timetz_pl_interval(fcinfo);
	if (fcinfo->isnull)
		return PG_DT_NULLED;
	r = DatumGetTimeTzADTP(d);
	*out_time = r->time;
	*out_zone = r->zone;
	return 0;
}

/*
 * ---- datetime_convert_diff abbrev arms (DecodeTimezoneAbbrev{,Prefix}) ----
 *
 * These two are fuzz-uncovered with zoneabbrevtbl == NULL (the io target's
 * pinned environment never installs one), so they get a PINNED abbreviation
 * table installed through PostgreSQL's OWN ConvertTimeZoneAbbrevs +
 * InstallTimeZoneAbbrevs — vendored verbatim, so neither side hand-rolls the
 * TimeZoneAbbrevTable layout or the DYNTZ value-is-a-byte-offset encoding.
 *
 * The table (identical on both sides, sorted by strcmp as CheckDateTokenTable
 * requires): fixed-offset TZ and DTZ entries spanning positive/negative/zero
 * offsets, one abbrev of exactly TOKMAXLEN so the full-width NUL-terminated
 * token path is witnessed, and ONE DYNTZ entry whose zone is "GMT" — which keeps the DYNTZ branch
 * (FetchDynamicTimeZone -> pg_tzset) inside the compared domain, since GMT is
 * the one name the pinned tz database admits. A DYNTZ entry naming a tzdata
 * zone would leave the domain via the header's pg_tzset carve.
 */
static tzEntry pg_dt_pinned_abbrevs[] = {
	/* abbrev, zone, offset, is_dst, lineno, filename */
	{"aaa", NULL, -43200, false, 0, NULL},
	{"bbb", NULL, 0, true, 0, NULL},
	{"ccc", NULL, 3600, false, 0, NULL},
	{"dddddddddd", NULL, 50400, true, 0, NULL},	/* exactly TOKMAXLEN */
	{"eee", NULL, -1, false, 0, NULL},
	{"gmtdyn", "GMT", 0, false, 0, NULL},	/* DYNTZ, in-domain zone */
	{"zzz", NULL, 57599, false, 0, NULL},
};

/* install once per thread; the table must outlive the installing exec */
static void
pg_dt_install_pinned_abbrevs(void)
{
	static _Thread_local int done = 0;
	TimeZoneAbbrevTable *tbl;

	if (done)
		return;
	tbl = ConvertTimeZoneAbbrevs(pg_dt_pinned_abbrevs,
								 (int) (sizeof(pg_dt_pinned_abbrevs) /
										sizeof(pg_dt_pinned_abbrevs[0])));
	if (tbl == NULL)
		abort();
	InstallTimeZoneAbbrevs(tbl);
	done = 1;
}

/*
 * DecodeTimezoneAbbrev over the pinned table.
 * Returns the dterr; *ftype/*offset/*have_tz are the compared planes (the
 * pg_tz POINTER itself is not comparable across implementations, so the
 * plane is "did it resolve to a zone", plus the tzset-carve flag).
 */
int
pg_diff_decode_timezone_abbrev(const unsigned char *tok, int toklen,
							   int *ftype, int *offset, int *have_tz)
{
	char		lowtoken[TOKMAXLEN + 1];
	DateTimeErrorExtra extra;
	pg_tz	   *tz = NULL;
	int			dterr;
	int			i;

	pg_dt_reset(USE_ISO_DATES, DATEORDER_YMD);
	pg_dt_install_pinned_abbrevs();
	if (setjmp(pg_dt_jmp))
		return 1000 + pg_diff_errcode;
	if (toklen > TOKMAXLEN)
		abort();				/* driver contract */
	for (i = 0; i < toklen; i++)
		lowtoken[i] = (char) pg_tolower((unsigned char) tok[i]);
	lowtoken[toklen] = '\0';

	memset(&extra, 0, sizeof(extra));
	*ftype = UNKNOWN_FIELD;
	*offset = 0;
	dterr = DecodeTimezoneAbbrev(0, lowtoken, ftype, offset, &tz, &extra);
	*have_tz = (tz != NULL);
	return dterr;
}

/* DecodeTimezoneAbbrevPrefix returns the matched prefix LENGTH (or -1). */
int
pg_diff_decode_timezone_abbrev_prefix(const unsigned char *str, int len,
									  int *offset, int *have_tz)
{
	char		buf[64];
	pg_tz	   *tz = NULL;
	int			rc;

	pg_dt_reset(USE_ISO_DATES, DATEORDER_YMD);
	pg_dt_install_pinned_abbrevs();
	if (setjmp(pg_dt_jmp))
		return -1000 - pg_diff_errcode;
	if (len > (int) sizeof(buf) - 1)
		abort();				/* driver contract */
	memcpy(buf, str, len);
	buf[len] = '\0';

	*offset = 0;
	rc = DecodeTimezoneAbbrevPrefix(buf, offset, &tz);
	*have_tz = (tz != NULL);
	return rc;
}
