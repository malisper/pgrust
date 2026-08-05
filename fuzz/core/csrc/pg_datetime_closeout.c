/*
 * pg_datetime_closeout.c: vendored PostgreSQL C oracle for the
 * datetime_closeout_diff differential fuzz target (100%-coverage campaign;
 * crates adt_date + adt_datetime, lane p1-lanel2 closeout).
 *
 * Provenance: all vendored bodies VERBATIM from postgres-src
 * 62d6c7d3df6287f1bd83199c1a746e50d31571a0 (PostgreSQL 18.3 "Stamp-18.3"):
 *   - csrc/pg_datetime_verbatim.inc (shared with lanes p1-lanel / p1-laney):
 *     the datetime.c parse/encode core, timestamp2tm, and the date.c bodies
 *     including time_part_common (whose retnumeric=true face THIS oracle is
 *     the first to drive — the io oracle aborts in its numeric stubs).
 *   - date.c extract_date, timetz_part_common, date_decrement,
 *     date_increment: vendored verbatim BELOW (they are not in the shared
 *     .inc; extracted by hand from vendor/postgres-src date.c, byte-compared).
 *
 * PINNED ENVIRONMENT + SHIMS: the prelude below is a byte-identical copy of
 * lane p1-laney's csrc/pg_timestamp_io.c prelude (itself a copy of
 * p1-lanel's csrc/pg_datetime_io_io.c prelude): GMT session zone via the
 * localtime-boundary shims, pinned now = 2026-06-15 12:30:45.123456 GMT,
 * ereport->longjmp with errcode classes, TLS bump arena for palloc,
 * tz-database DOMAIN CARVE via pg_tzset flagging, and laney's NUMERIC
 * ARG-CAPTURE boundary shims (int64_to_numeric / int64_div_fast_to_numeric
 * record their (value, decimal-scale) arguments; the driver compares
 * pgrust's rendered numeric text against the decimal string those arguments
 * determine — numeric ENCODING is adt/numeric surface, owned by its lane).
 * One addition over laney: a `numeric_in` shim that records the
 * "Infinity"/"-Infinity" literal extract_date hands it on non-finite dates
 * (sign captured in pg_dtclo_num_inf) instead of flagging a numeric chain.
 *
 * ERRCODE CLASSES (identical to pg_timestamp_io.c): 1=22007, 2=22008/22003
 * datetime, 3=22009, 4=22015, 5=22023, 6=0A000, 7=F0000, 9=22012, 10=22003
 * numeric, 99=internal.
 *
 * SYMBOL ISOLATION: this TU vendors the same verbatim datetime.c/date.c
 * TU-fractions as the lanel and laney oracles; core/build.rs compiles it
 * with a dtclo_impl_ prefix -D rename of every colliding global (the same
 * list as TSDIFF_SHARED_SYMS, plus extract_date), so all three oracles keep
 * their own vendored copies.
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

/* ================================================================== *
 *  adt_date closeout additions (lane p1-lanel2) — shims + verbatim   *
 * ================================================================== */

#define InvalidOid ((Oid) 0)
#define CStringGetDatum(X) PointerGetDatum(X)
#define ObjectIdGetDatum(X) ((Datum) (X))

static inline Numeric
DatumGetNumeric(Datum X)
{
	return (Numeric) DatumGetPointer(X);
}

/* DirectFunctionCall3: real fmgr-style dispatch over the shim fcinfo
 * (plumbing; extract_date's sole use routes to the numeric_in shim below). */
typedef Datum (*PGFunction) (FunctionCallInfo fcinfo);

static Datum
pg_dtclo_dfc3(PGFunction func, Datum a0, Datum a1, Datum a2)
{
	struct FunctionCallInfoBaseData fc;

	memset(&fc, 0, sizeof(fc));
	fc.nargs = 3;
	fc.args[0].value = a0;
	fc.args[1].value = a1;
	fc.args[2].value = a2;
	return func(&fc);
}

#define DirectFunctionCall3(f, a, b, c) pg_dtclo_dfc3(f, (a), (b), (c))

/* numeric_in ARG-CAPTURE shim: extract_date reaches it only through
 * DirectFunctionCall3(numeric_in, CStringGetDatum("Infinity"/"-Infinity"),
 * InvalidOid, -1) on non-finite dates. Record the literal's SIGN instead of
 * vendoring numeric.c (the boundary-shim discipline documented in the file
 * header); the driver compares pgrust's rendered numeric text against the
 * recorded infinity. */
static _Thread_local int pg_dtclo_num_inf;	/* 0 none, +1 Infinity, -1 -Infinity */

static Datum
numeric_in(PG_FUNCTION_ARGS)
{
	const char *s = (const char *) DatumGetPointer(fcinfo->args[0].value);

	pg_dtclo_num_inf = (s[0] == '-') ? -1 : 1;
	return PointerGetDatum(&pg_ts_num_token);
}

/* skip-support callbacks take a Relation they never read */
typedef struct RelationData *Relation;

/* forward prototype for the verbatim body below */
Datum		extract_date(PG_FUNCTION_ARGS);
static Datum timetz_part_common(PG_FUNCTION_ARGS, bool retnumeric);
static Datum date_decrement(Relation rel, Datum existing, bool *underflow);
static Datum date_increment(Relation rel, Datum existing, bool *overflow);

/* ==== VERBATIM: src/backend/utils/adt/date.c date_decrement ==== */

static Datum
date_decrement(Relation rel, Datum existing, bool *underflow)
{
	DateADT		dexisting = DatumGetDateADT(existing);

	if (dexisting == DATEVAL_NOBEGIN)
	{
		/* return value is undefined */
		*underflow = true;
		return (Datum) 0;
	}

	*underflow = false;
	return DateADTGetDatum(dexisting - 1);
}

static Datum
date_increment(Relation rel, Datum existing, bool *overflow)
{
	DateADT		dexisting = DatumGetDateADT(existing);

	if (dexisting == DATEVAL_NOEND)
	{
		/* return value is undefined */
		*overflow = true;
		return (Datum) 0;
	}

	*overflow = false;
	return DateADTGetDatum(dexisting + 1);
}

/* ==== VERBATIM: src/backend/utils/adt/date.c extract_date ==== */

/* extract_date()
 * Extract specified field from date type.
 */
Datum
extract_date(PG_FUNCTION_ARGS)
{
	text	   *units = PG_GETARG_TEXT_PP(0);
	DateADT		date = PG_GETARG_DATEADT(1);
	int64		intresult;
	int			type,
				val;
	char	   *lowunits;
	int			year,
				mon,
				mday;

	lowunits = downcase_truncate_identifier(VARDATA_ANY(units),
											VARSIZE_ANY_EXHDR(units),
											false);

	type = DecodeUnits(0, lowunits, &val);
	if (type == UNKNOWN_FIELD)
		type = DecodeSpecial(0, lowunits, &val);

	if (DATE_NOT_FINITE(date) && (type == UNITS || type == RESERV))
	{
		switch (val)
		{
				/* Oscillating units */
			case DTK_DAY:
			case DTK_MONTH:
			case DTK_QUARTER:
			case DTK_WEEK:
			case DTK_DOW:
			case DTK_ISODOW:
			case DTK_DOY:
				PG_RETURN_NULL();
				break;

				/* Monotonically-increasing units */
			case DTK_YEAR:
			case DTK_DECADE:
			case DTK_CENTURY:
			case DTK_MILLENNIUM:
			case DTK_JULIAN:
			case DTK_ISOYEAR:
			case DTK_EPOCH:
				if (DATE_IS_NOBEGIN(date))
					PG_RETURN_NUMERIC(DatumGetNumeric(DirectFunctionCall3(numeric_in,
																		  CStringGetDatum("-Infinity"),
																		  ObjectIdGetDatum(InvalidOid),
																		  Int32GetDatum(-1))));
				else
					PG_RETURN_NUMERIC(DatumGetNumeric(DirectFunctionCall3(numeric_in,
																		  CStringGetDatum("Infinity"),
																		  ObjectIdGetDatum(InvalidOid),
																		  Int32GetDatum(-1))));
			default:
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("unit \"%s\" not supported for type %s",
								lowunits, format_type_be(DATEOID))));
		}
	}
	else if (type == UNITS)
	{
		j2date(date + POSTGRES_EPOCH_JDATE, &year, &mon, &mday);

		switch (val)
		{
			case DTK_DAY:
				intresult = mday;
				break;

			case DTK_MONTH:
				intresult = mon;
				break;

			case DTK_QUARTER:
				intresult = (mon - 1) / 3 + 1;
				break;

			case DTK_WEEK:
				intresult = date2isoweek(year, mon, mday);
				break;

			case DTK_YEAR:
				if (year > 0)
					intresult = year;
				else
					/* there is no year 0, just 1 BC and 1 AD */
					intresult = year - 1;
				break;

			case DTK_DECADE:
				/* see comments in timestamp_part */
				if (year >= 0)
					intresult = year / 10;
				else
					intresult = -((8 - (year - 1)) / 10);
				break;

			case DTK_CENTURY:
				/* see comments in timestamp_part */
				if (year > 0)
					intresult = (year + 99) / 100;
				else
					intresult = -((99 - (year - 1)) / 100);
				break;

			case DTK_MILLENNIUM:
				/* see comments in timestamp_part */
				if (year > 0)
					intresult = (year + 999) / 1000;
				else
					intresult = -((999 - (year - 1)) / 1000);
				break;

			case DTK_JULIAN:
				intresult = date + POSTGRES_EPOCH_JDATE;
				break;

			case DTK_ISOYEAR:
				intresult = date2isoyear(year, mon, mday);
				/* Adjust BC years */
				if (intresult <= 0)
					intresult -= 1;
				break;

			case DTK_DOW:
			case DTK_ISODOW:
				intresult = j2day(date + POSTGRES_EPOCH_JDATE);
				if (val == DTK_ISODOW && intresult == 0)
					intresult = 7;
				break;

			case DTK_DOY:
				intresult = date2j(year, mon, mday) - date2j(year, 1, 1) + 1;
				break;

			default:
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("unit \"%s\" not supported for type %s",
								lowunits, format_type_be(DATEOID))));
				intresult = 0;
		}
	}
	else if (type == RESERV)
	{
		switch (val)
		{
			case DTK_EPOCH:
				intresult = ((int64) date + POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY;
				break;

			default:
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("unit \"%s\" not supported for type %s",
								lowunits, format_type_be(DATEOID))));
				intresult = 0;
		}
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("unit \"%s\" not recognized for type %s",
						lowunits, format_type_be(DATEOID))));
		intresult = 0;
	}

	PG_RETURN_NUMERIC(int64_to_numeric(intresult));
}

/* ==== VERBATIM: src/backend/utils/adt/date.c timetz_part_common ==== */

/* timetz_part() and extract_timetz()
 * Extract specified field from time type.
 */
static Datum
timetz_part_common(PG_FUNCTION_ARGS, bool retnumeric)
{
	text	   *units = PG_GETARG_TEXT_PP(0);
	TimeTzADT  *time = PG_GETARG_TIMETZADT_P(1);
	int64		intresult;
	int			type,
				val;
	char	   *lowunits;

	lowunits = downcase_truncate_identifier(VARDATA_ANY(units),
											VARSIZE_ANY_EXHDR(units),
											false);

	type = DecodeUnits(0, lowunits, &val);
	if (type == UNKNOWN_FIELD)
		type = DecodeSpecial(0, lowunits, &val);

	if (type == UNITS)
	{
		int			tz;
		fsec_t		fsec;
		struct pg_tm tt,
				   *tm = &tt;

		timetz2tm(time, tm, &fsec, &tz);

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
				if (retnumeric)
					/*---
					 * tm->tm_sec * 1000 + fsec / 1000
					 * = (tm->tm_sec * 1'000'000 + fsec) / 1000
					 */
					PG_RETURN_NUMERIC(int64_div_fast_to_numeric(tm->tm_sec * INT64CONST(1000000) + fsec, 3));
				else
					PG_RETURN_FLOAT8(tm->tm_sec * 1000.0 + fsec / 1000.0);
				break;

			case DTK_SECOND:
				if (retnumeric)
					/*---
					 * tm->tm_sec + fsec / 1'000'000
					 * = (tm->tm_sec * 1'000'000 + fsec) / 1'000'000
					 */
					PG_RETURN_NUMERIC(int64_div_fast_to_numeric(tm->tm_sec * INT64CONST(1000000) + fsec, 6));
				else
					PG_RETURN_FLOAT8(tm->tm_sec + fsec / 1000000.0);
				break;

			case DTK_MINUTE:
				intresult = tm->tm_min;
				break;

			case DTK_HOUR:
				intresult = tm->tm_hour;
				break;

			case DTK_DAY:
			case DTK_MONTH:
			case DTK_QUARTER:
			case DTK_YEAR:
			case DTK_DECADE:
			case DTK_CENTURY:
			case DTK_MILLENNIUM:
			default:
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("unit \"%s\" not supported for type %s",
								lowunits, format_type_be(TIMETZOID))));
				intresult = 0;
		}
	}
	else if (type == RESERV && val == DTK_EPOCH)
	{
		if (retnumeric)
			/*---
			 * time->time / 1'000'000 + time->zone
			 * = (time->time + time->zone * 1'000'000) / 1'000'000
			 */
			PG_RETURN_NUMERIC(int64_div_fast_to_numeric(time->time + time->zone * INT64CONST(1000000), 6));
		else
			PG_RETURN_FLOAT8(time->time / 1000000.0 + time->zone);
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("unit \"%s\" not recognized for type %s",
						lowunits, format_type_be(TIMETZOID))));
		intresult = 0;
	}

	if (retnumeric)
		PG_RETURN_NUMERIC(int64_to_numeric(intresult));
	else
		PG_RETURN_FLOAT8(intresult);
}


/* ========== fuzz-facing driver entries (NOT Postgres code) ========== */

static void
pg_dtclo_reset(void)
{
	pg_diff_errcode = 0;
	pg_dt_pending = 0;
	pg_dt_tzset_nongmt = 0;
	pg_dt_tzset_name[0] = '\0';
	pg_dt_arena_off = 0;
	pg_ts_num_set = 0;
	pg_ts_numchain = 0;
	pg_dtclo_num_inf = 0;
	DateStyle = USE_ISO_DATES;
	DateOrder = DATEORDER_YMD;
	IntervalStyle = INTSTYLE_POSTGRES;
}

static _Thread_local struct FunctionCallInfoBaseData pg_dtclo_fcinfo_data;

static FunctionCallInfo
pg_dtclo_fcinfo(void)
{
	memset(&pg_dtclo_fcinfo_data, 0, sizeof(pg_dtclo_fcinfo_data));
	return &pg_dtclo_fcinfo_data;
}

/* 4-byte-header text varlena in the per-exec arena (see pgdt/fmgr.h) */
static text *
pg_dtclo_text(const char *s, int len)
{
	text	   *t = (text *) pg_dt_palloc((size_t) len + 4);

	t->vl_len_ = ((uint32) (len + 4)) << 2;
	memcpy(t->vl_dat, s, (size_t) len);
	return t;
}

/* extract_date: always returns numeric (or SQL NULL for oscillating units on
 * non-finite dates). The numeric plane reports the recorded constructor
 * arguments; inf reports the numeric_in infinity literal's sign. */
int
pg_dtclo_extract_date(const char *units, int ulen, int32 date,
					  int *isnull, int64 *nval, int *nlog10,
					  int *numset, int *inf)
{
	FunctionCallInfo fcinfo = pg_dtclo_fcinfo();

	pg_dtclo_reset();
	if (setjmp(pg_dt_jmp))
		return pg_diff_errcode;
	fcinfo->args[0].value = PointerGetDatum(pg_dtclo_text(units, ulen));
	fcinfo->args[1].value = Int32GetDatum(date);
	(void) extract_date(fcinfo);
	*isnull = fcinfo->isnull ? 1 : 0;
	*nval = pg_ts_num_val;
	*nlog10 = pg_ts_num_log10;
	*numset = pg_ts_num_set;
	*inf = pg_dtclo_num_inf;
	return 0;
}

/* time_part_common, BOTH retnumeric faces (the io oracle only ever drives
 * retnumeric=false; its numeric stubs abort). Never returns SQL NULL. */
int
pg_dtclo_time_part(const char *units, int ulen, int64 time, int retnumeric,
				   double *fval, int64 *nval, int *nlog10, int *numset)
{
	FunctionCallInfo fcinfo = pg_dtclo_fcinfo();
	Datum		d;

	pg_dtclo_reset();
	if (setjmp(pg_dt_jmp))
		return pg_diff_errcode;
	fcinfo->args[0].value = PointerGetDatum(pg_dtclo_text(units, ulen));
	fcinfo->args[1].value = Int64GetDatum(time);
	d = time_part_common(fcinfo, retnumeric != 0);
	*fval = retnumeric ? 0.0 : DatumGetFloat8(d);
	*nval = pg_ts_num_val;
	*nlog10 = pg_ts_num_log10;
	*numset = pg_ts_num_set;
	return 0;
}

int
pg_dtclo_timetz_part(const char *units, int ulen, int64 time, int32 zone,
					 int retnumeric, double *fval, int64 *nval, int *nlog10,
					 int *numset)
{
	FunctionCallInfo fcinfo = pg_dtclo_fcinfo();
	TimeTzADT	t;
	Datum		d;

	pg_dtclo_reset();
	if (setjmp(pg_dt_jmp))
		return pg_diff_errcode;
	t.time = time;
	t.zone = zone;
	fcinfo->args[0].value = PointerGetDatum(pg_dtclo_text(units, ulen));
	fcinfo->args[1].value = PointerGetDatum(&t);
	d = timetz_part_common(fcinfo, retnumeric != 0);
	*fval = retnumeric ? 0.0 : DatumGetFloat8(d);
	*nval = pg_ts_num_val;
	*nlog10 = pg_ts_num_log10;
	*numset = pg_ts_num_set;
	return 0;
}

/* skip-support callbacks (btree skip scan): cannot error. */
int
pg_dtclo_date_decrement(int32 date, int *underflow, int32 *out)
{
	bool		uf = false;
	Datum		d;

	pg_dtclo_reset();
	d = date_decrement((Relation) 0, Int32GetDatum(date), &uf);
	*underflow = uf ? 1 : 0;
	*out = DatumGetInt32(d);
	return 0;
}

int
pg_dtclo_date_increment(int32 date, int *overflow, int32 *out)
{
	bool		of = false;
	Datum		d;

	pg_dtclo_reset();
	d = date_increment((Relation) 0, Int32GetDatum(date), &of);
	*overflow = of ? 1 : 0;
	*out = DatumGetInt32(d);
	return 0;
}
