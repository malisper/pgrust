/*
 * Vendored PostgreSQL C: to_char/to_date/to_timestamp format-picture engine —
 * differential-fuzz oracle for fmt_dch_diff (and the shared engine for
 * fmt_num_diff; the NUM SQL entries live in pg_fmt_num_io.c).
 *
 * Provenance (all bodies VERBATIM unless a shim is listed below), from
 * postgres-src @ 62d6c7d3df6287f1bd83199c1a746e50d31571a0 (REL_18 "Stamp
 * 18.3", the repo's vendored ground-truth checkout
 * ../pgrust-fabled/vendor/postgres-src):
 *
 *   - pg_formatting_18_3.inc = src/backend/utils/adt/formatting.c lines
 *     93..6309 BYTE-IDENTICAL (verify: sed -n '93,6309p' formatting.c | cmp -
 *     pg_formatting_18_3.inc). That is the whole file after its #include
 *     block, up to (excluding) the NUM SQL-callable entries
 *     (numeric_to_number..float8_to_char, vendored by pg_fmt_num_io.c).
 *     Contents: DCH/NUM keyword tables, FormatNode types, parse_format, the
 *     DCH_to_char/DCH_from_char interpreters, DCH/NUM caches, NUM_processor,
 *     str_tolower/str_toupper/str_initcap/str_casefold + asc_* helpers,
 *     int_to_roman/roman_to_int, and the SQL entries timestamp_to_char,
 *     timestamptz_to_char, interval_to_char, to_timestamp, to_date,
 *     parse_datetime, datetime_format_has_tz.
 *   - pg_fmt_deps_18_3.inc = verbatim helper bodies cited per-block inside
 *     that file: timestamp.c (AdjustTimestampForTypmod, dt2time,
 *     timestamp2tm, tm2timestamp, interval2itm, ISO-week family),
 *     datetime.c (date2j, j2date, j2day, DetermineTimeZoneOffset[Internal],
 *     DetermineTimeZoneAbbrevOffset[TS/Internal], ValidateDate,
 *     DecodeTimezoneAbbrevPrefix, DateTimeParseError, datebsearch),
 *     date.c (tm2time, AdjustTimeForTypmod, tm2timetz),
 *     pgstrcasecmp.c (pg_strcasecmp, pg_strncasecmp, pg_ascii_toupper,
 *     pg_ascii_tolower, pg_tolower), scansup.c (scanner_isspace),
 *     wchar.c (pg_utf_mblen), mbutils.c (pg_mblen_cstr, pg_mblen_range,
 *     pg_mbstrlen, pg_mbstrlen_with_len).
 *   - hdr_datatype_timestamp_h.inc / hdr_utils_datetime_h.inc /
 *     hdr_utils_date_h.inc = src/include/datatype/timestamp.h,
 *     src/include/utils/datetime.h, src/include/utils/date.h with ONLY the
 *     '#include' lines removed (grep -v '^#include'), otherwise verbatim.
 *
 * Shims (plumbing + ENVIRONMENT PINS, never computation):
 *   - Memory: palloc/palloc0/repalloc/pfree over a per-call tracked arena,
 *     reset at each pg_diff_fmt_* entry (models the per-query memory
 *     context). MemoryContextAllocZero(TopMemoryContext, n) -> calloc,
 *     PERSISTENT across calls, exactly like the backend's DCH/NUM caches.
 *   - Errors: ereport/elog(>=ERROR) / ereturn / errsave record the errcode
 *     in thread-local pg_diff_errcode and longjmp back to the entry (all
 *     escontexts passed are NULL = hard-error shape, matching the Rust
 *     driver which passes escontext None). errmsg/errdetail/errhint args
 *     are UNEVALUATED (message text is out of comparison scope).
 *     Errcodes are mapped to small ints (PG_DIFF_ERRC table below); the
 *     Rust driver owns the same table.
 *   - ENCODING PIN: GetDatabaseEncoding() = PG_UTF8;
 *     pg_mblen = pg_utf_mblen (verbatim wchar.c body);
 *     pg_database_encoding_max_length() = 4. The Rust side pins UTF8 too.
 *   - COLLATION/LOCALE PIN: pg_newlocale_from_collation returns a static
 *     { ctype_is_c = true } locale (driver always passes C_COLLATION_OID),
 *     so str_tolower/str_toupper/str_initcap/str_casefold take their
 *     verbatim ASCII arms; pg_strlower/pg_strupper/pg_strtitle/pg_strfold
 *     are abort() stubs (unreachable under the pin; an abort = harness
 *     defect, never silently wrong data). cache_locale_time() fills the
 *     localized_* arrays with the C-locale strftime names (English), which
 *     is what real PostgreSQL produces under lc_time=C; TM-prefixed
 *     patterns therefore stay IN the comparison plane.
 *     PGLC_localeconv() = libc localeconv() under the process C locale
 *     (identical struct contents to PG's cached copy under
 *     lc_numeric=lc_monetary=C).
 *   - TIMEZONE PIN (session tz = GMT, both sides):
 *     session_timezone = a dummy pg_tz; pg_next_dst_boundary returns 0
 *     with gmtoff 0 / isdst false (exactly GMT's behavior);
 *     TimeZoneAbbrevIsKnown / pg_interpret_timezone_abbrev recognize only
 *     "gmt" at offset 0 (the only abbrev the real GMT pg_tz defines);
 *     zoneabbrevtbl = NULL (empty timezone_abbreviations, pinned the same
 *     on the Rust side); FetchDynamicTimeZone aborts (unreachable with a
 *     NULL zoneabbrevtbl).
 *   - fmgr: minimal FunctionCallInfo (args/isnull/fncollation) so the SQL
 *     entries compile verbatim; PG_RETURN_NULL sets fcinfo->isnull.
 *   - varlena: 4-byte-header varlenas only (the oracle builds every text
 *     itself), so VARDATA_ANY == VARDATA, VARSIZE_ANY_EXHDR == VARSIZE-4.
 *   - Assert = no-op (release-build parity).
 */

/*
 * FAMILY SYMBOL ISOLATION (central symfix lane, 2026-08-01): this TU landed
 * with unprefixed verbatim-C exports that collide under GNU ld with the
 * incumbent oracle families (pg_miscfam_io.c: pg_strcasecmp; pg_numutils.c:
 * pg_ltoa/pg_lltoa/pg_ultoa_n/pg_ulltoa_n; pg_int_io.c: int4out; dtio
 * family pg_datetime_io_io.c: the datetime helper cone below; tsvec:
 * pg_mblen_cstr/pg_mblen_range). Apple ld64 only warns, so local checks
 * passed while EVERY Linux fleet fuzz build hard-errored (ld.lld duplicate
 * symbols, first witnessed by gram_core job -2ab6-60592). Preprocessor-layer
 * rename ONLY — every C body stays verbatim (wcharfam/contribafam
 * in-file-prefix precedent; in-file rather than build.rs .define() because
 * this TU shares the pg_difffuzz_oracle cc::Build with pg_numutils.c/
 * pg_miscfam_io.c/pg_int_io.c, whose same-named exports must not be renamed
 * with it). The NINE numeric.c-extract exports of pg_numeric_deps_18_3.inc
 * (numeric_in/out/out_sci/round/mul/mul_opt_error/power/int4_opt_error/
 * int64_to_numeric, colliding with the numericfam oracle) are renamed
 * fmtdch_* by build.rs .define() instead — hunk adopted verbatim from
 * proofs/p1-queryjumble 9e61831839 so that lane rebases cleanly; ONE prefix
 * scheme (fmtdch_) for the whole family. Durable lesson: verbatim oracle
 * TUs MUST ship with family symbol prefixes — ld64 warnings are ld.lld
 * errors.
 */
#define AdjustTimeForTypmod fmtdch_AdjustTimeForTypmod
#define DateTimeParseError fmtdch_DateTimeParseError
#define DecodeTimezoneAbbrevPrefix fmtdch_DecodeTimezoneAbbrevPrefix
#define DetermineTimeZoneAbbrevOffset fmtdch_DetermineTimeZoneAbbrevOffset
#define DetermineTimeZoneOffset fmtdch_DetermineTimeZoneOffset
#define ValidateDate fmtdch_ValidateDate
#define date2isoweek fmtdch_date2isoweek
#define date2isoyear fmtdch_date2isoyear
#define date2isoyearday fmtdch_date2isoyearday
#define date2j fmtdch_date2j
#define day_tab fmtdch_day_tab
#define days fmtdch_days
#define int4out fmtdch_int4out
#define interval2itm fmtdch_interval2itm
#define isoweek2date fmtdch_isoweek2date
#define isoweek2j fmtdch_isoweek2j
#define isoweekdate2date fmtdch_isoweekdate2date
#define j2date fmtdch_j2date
#define j2day fmtdch_j2day
#define months fmtdch_months
#define pg_lltoa fmtdch_pg_lltoa
#define pg_ltoa fmtdch_pg_ltoa
#define pg_mblen_cstr fmtdch_pg_mblen_cstr
#define pg_mblen_range fmtdch_pg_mblen_range
#define pg_strcasecmp fmtdch_pg_strcasecmp
#define pg_tolower fmtdch_pg_tolower
#define pg_toupper fmtdch_pg_toupper
#define pg_ulltoa_n fmtdch_pg_ulltoa_n
#define pg_ultoa_n fmtdch_pg_ultoa_n
#define timestamp2tm fmtdch_timestamp2tm
#define tm2time fmtdch_tm2time
#define tm2timetz fmtdch_tm2timetz

#include <ctype.h>
#include <errno.h>
#include <float.h>
#include <limits.h>
#include <locale.h>
#include <math.h>
#include <setjmp.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>			/* ssize_t */
#include "pg_oracle_guard.h"	/* oracle-serialization holder check */

/* ---------------------------------------------------------------- */
/* c.h basics                                                        */
/* ---------------------------------------------------------------- */

typedef int8_t int8;
typedef int16_t int16;
typedef int32_t int32;
typedef int64_t int64;
typedef uint8_t uint8;
typedef uint16_t uint16;
typedef uint32_t uint32;
typedef uint64_t uint64;
typedef float float4;
typedef double float8;
typedef size_t Size;
typedef unsigned int Oid;
typedef int64 pg_time_t;

#define InvalidOid ((Oid) 0)
#define OidIsValid(objectId) ((bool) ((objectId) != InvalidOid))
#define C_COLLATION_OID 950
#define DEFAULT_COLLATION_OID 100

#define INT64CONST(x) INT64_C(x)
#define UINT64CONST(x) UINT64_C(x)
#define PG_INT32_MIN INT32_MIN
#define PG_INT32_MAX INT32_MAX
#define PG_INT64_MIN INT64_MIN
#define PG_INT16_MIN INT16_MIN
#define PG_INT16_MAX INT16_MAX
#define PG_UINT16_MAX UINT16_MAX
#define PG_UINT32_MAX UINT32_MAX
#define PG_INT64_MAX INT64_MAX

#define Assert(x) ((void) 0)
#define AssertMacro(x) ((void) 0)
#define lengthof(array) (sizeof(array) / sizeof((array)[0]))
#define Min(x, y) ((x) < (y) ? (x) : (y))
#define Max(x, y) ((x) > (y) ? (x) : (y))
#define Abs(x) ((x) >= 0 ? (x) : -(x))
#define unlikely(x) (x)
#define likely(x) (x)
#define MAXIMUM_ALIGNOF 8
#define TYPEALIGN(ALIGNVAL,LEN)  \
	(((uintptr_t) (LEN) + ((ALIGNVAL) - 1)) & ~((uintptr_t) ((ALIGNVAL) - 1)))
#define MAXALIGN(LEN) TYPEALIGN(MAXIMUM_ALIGNOF, (LEN))
#define MemSet(start, val, len) memset(start, val, len)
#define pg_attribute_unused()
#define pg_nodiscard
#define PGDLLIMPORT
#define gettext_noop(x) (x)
#define _(x) (x)
#define pg_attribute_printf(f, a)
#define pg_noinline
#define IS_HIGHBIT_SET(ch) ((unsigned char) (ch) & 0x80)
#define FLEXIBLE_ARRAY_MEMBER	/* empty */
#define TZ_STRLEN_MAX 255

/*
 * port.h parity (task #142): the verbatim extracts call strlcpy (datetime.c
 * DetermineTimeZoneAbbrevOffsetInternal in pg_fmt_deps_18_3.inc; the DCH/NUM
 * cache fills in pg_formatting_18_3.inc). macOS <string.h> declares it;
 * glibc only from 2.38, and the fleet pods are older — without a declaration
 * newer gcc rejects the TU (implicit function declaration is an error since
 * gcc 14). Same guarded declaration real port.h carries (!HAVE_DECL_STRLCPY
 * arm). The link-time definition is libc's where it exists, else the WEAK
 * compat copy in csrc/pg_strlcpy_compat.c.
 */
#ifndef __APPLE__
extern size_t strlcpy(char *dst, const char *src, size_t siz);
#endif

typedef struct Node Node;		/* opaque; every escontext here is NULL */
typedef void *MemoryContext;
static MemoryContext TopMemoryContext = NULL;

/* ---------------------------------------------------------------- */
/* Datum + minimal fmgr                                              */
/* ---------------------------------------------------------------- */

typedef uintptr_t Datum;

static inline Datum PointerGetDatum(const void *X) { return (Datum) X; }
static inline void *DatumGetPointer(Datum X) { return (void *) X; }
static inline int32 DatumGetInt32(Datum X) { return (int32) X; }
static inline Datum Int32GetDatum(int32 X) { return (Datum) (uint32) X; }
static inline int64 DatumGetInt64(Datum X) { return (int64) X; }
static inline Datum Int64GetDatum(int64 X) { return (Datum) X; }

typedef struct NullableDatum
{
	Datum		value;
	bool		isnull;
} NullableDatum;

typedef struct FunctionCallInfoBaseData
{
	void	   *context;		/* escontext slot (always NULL here) */
	Oid			fncollation;
	bool		isnull;
	short		nargs;
	NullableDatum args[8];
} FunctionCallInfoBaseData, *FunctionCallInfo;

#define PG_FUNCTION_ARGS FunctionCallInfo fcinfo
#define PG_GETARG_DATUM(n) (fcinfo->args[n].value)
#define PG_GETARG_INT32(n) DatumGetInt32(PG_GETARG_DATUM(n))
#define PG_GET_COLLATION() (fcinfo->fncollation)
#define PG_RETURN_NULL() do { fcinfo->isnull = true; return (Datum) 0; } while (0)
#define PG_RETURN_TEXT_P(x) return PointerGetDatum(x)

/* ---------------------------------------------------------------- */
/* varlena/text (4-byte-header shim; see file header)                */
/* ---------------------------------------------------------------- */

typedef struct varlena
{
	int32		vl_len_;
	char		vl_dat[];
} varlena;
typedef varlena text;

#define VARHDRSZ ((int32) sizeof(int32))
#define VARDATA(PTR) (((char *) (PTR)) + VARHDRSZ)
#define VARSIZE(PTR) (*((const int32 *) (PTR)))
#define SET_VARSIZE(PTR, len) (*((int32 *) (PTR)) = (len))
#define VARDATA_ANY(PTR) VARDATA(PTR)
#define VARSIZE_ANY_EXHDR(PTR) (VARSIZE(PTR) - VARHDRSZ)
#define PG_GETARG_TEXT_PP(n) ((text *) DatumGetPointer(PG_GETARG_DATUM(n)))

/* ---------------------------------------------------------------- */
/* error machinery (see file header)                                 */
/* ---------------------------------------------------------------- */

/* shared thread-local errcode cell, defined in pg_float_io.c */
extern _Thread_local int pg_diff_errcode;
extern int pg_diff_errcode_get(void);

/*
 * Small-int errcode classes for this oracle (the Rust driver mirrors this
 * table; values are target-local, unrelated to pg_float_io.c's).
 */
#define ERRCODE_SYNTAX_ERROR 101	/* 42601 */
#define ERRCODE_INVALID_DATETIME_FORMAT 102 /* 22007 */
#define ERRCODE_DATETIME_VALUE_OUT_OF_RANGE 103 /* 22008 */
#define ERRCODE_DATETIME_FIELD_OVERFLOW 104 /* 22008 */
#define ERRCODE_INVALID_TEXT_REPRESENTATION 105 /* 22P02 */
#define ERRCODE_FEATURE_NOT_SUPPORTED 106	/* 0A000 */
#define ERRCODE_INDETERMINATE_COLLATION 107 /* 42P22 */
#define ERRCODE_INVALID_PARAMETER_VALUE 108 /* 22023 */
#define ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE 109	/* 22003 */
#define ERRCODE_DIVISION_BY_ZERO 110	/* 22012 */
#define ERRCODE_INTERVAL_FIELD_OVERFLOW 111 /* 22015 */
#define ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION 118	/* 2201F */
#define ERRCODE_INVALID_ARGUMENT_FOR_LOG 119	/* 2201E */
#define ERRCODE_PROGRAM_LIMIT_EXCEEDED 114	/* 54000 */
#define ERRCODE_INVALID_TIME_ZONE_DISPLACEMENT_VALUE 115	/* 22009 */
#define ERRCODE_CONFIG_FILE_ERROR 116	/* F0000 */

static _Thread_local jmp_buf pg_diff_fmt_jmp;

__attribute__((noreturn)) static void
pg_diff_fmt_throw(void)
{
	longjmp(pg_diff_fmt_jmp, 1);
}

/* elevels: only the ordering vs ERROR matters */
#define DEBUG1 10
#define NOTICE 17
#define WARNING 19
#define ERROR 21

#define errcode(sqlerrcode) (pg_diff_errcode = (sqlerrcode))
#define errmsg(...) 0
#define errmsg_internal(...) 0
#define errdetail(...) 0
#define errdetail_internal(...) 0
#define errhint(...) 0
#define ereport(elevel, ...) \
	do { (void) (__VA_ARGS__); if ((elevel) >= ERROR) pg_diff_fmt_throw(); } while (0)
#define elog(elevel, ...) \
	do { if ((elevel) >= ERROR) { pg_diff_errcode = 0; pg_diff_fmt_throw(); } } while (0)
#define ereturn(escontext, dummy_value, ...) \
	do { (void) (__VA_ARGS__); pg_diff_fmt_throw(); } while (0)
#define errsave(escontext, ...) \
	do { (void) (__VA_ARGS__); pg_diff_fmt_throw(); } while (0)
#define SOFT_ERROR_OCCURRED(escontext) (false)

/* ---------------------------------------------------------------- */
/* per-call arena allocator (see file header)                        */
/* ---------------------------------------------------------------- */

typedef struct pg_diff_alloc_hdr
{
	struct pg_diff_alloc_hdr *next;
	size_t		size;
} pg_diff_alloc_hdr;

static _Thread_local pg_diff_alloc_hdr *pg_diff_fmt_allocs = NULL;

static void *
palloc(Size size)
{
	pg_diff_alloc_hdr *h = malloc(sizeof(pg_diff_alloc_hdr) + size);

	if (h == NULL)
		abort();
	h->next = pg_diff_fmt_allocs;
	h->size = size;
	pg_diff_fmt_allocs = h;
	return (void *) (h + 1);
}

static void *
palloc0(Size size)
{
	void	   *p = palloc(size);

	memset(p, 0, size);
	return p;
}

static void *
repalloc(void *pointer, Size size)
{
	pg_diff_alloc_hdr *h = ((pg_diff_alloc_hdr *) pointer) - 1;
	void	   *p = palloc(size);

	memcpy(p, pointer, Min(h->size, size));
	return p;
}

static void
pfree(void *pointer)
{
	(void) pointer;				/* freed in bulk at entry reset */
}

static void
pg_diff_fmt_reset(void)
{
	while (pg_diff_fmt_allocs != NULL)
	{
		pg_diff_alloc_hdr *h = pg_diff_fmt_allocs;

		pg_diff_fmt_allocs = h->next;
		free(h);
	}
	pg_diff_errcode = 0;
}

static void *
MemoryContextAllocZero(MemoryContext context, Size size)
{
	void	   *p = calloc(1, size);	/* PERSISTENT: cache storage */

	(void) context;
	if (p == NULL)
		abort();
	return p;
}

static char *
pstrdup(const char *in)
{
	size_t		len = strlen(in);
	char	   *out = palloc(len + 1);

	memcpy(out, in, len + 1);
	return out;
}

static char *
pnstrdup(const char *in, Size len)
{
	char	   *out;
	size_t		n = 0;

	while (n < len && in[n] != '\0')
		n++;
	out = palloc(n + 1);
	memcpy(out, in, n);
	out[n] = '\0';
	return out;
}

static text *
cstring_to_text_with_len(const char *s, int len)
{
	text	   *result = (text *) palloc(len + VARHDRSZ);

	SET_VARSIZE(result, len + VARHDRSZ);
	memcpy(VARDATA(result), s, len);
	return result;
}

static text *
cstring_to_text(const char *s)
{
	return cstring_to_text_with_len(s, strlen(s));
}

static char *
text_to_cstring(const text *t)
{
	int			len = VARSIZE_ANY_EXHDR(t);
	char	   *result = palloc(len + 1);

	memcpy(result, VARDATA_ANY(t), len);
	result[len] = '\0';
	return result;
}

/* ---------------------------------------------------------------- */
/* common/int.h overflow helpers (verbatim semantics via builtins)   */
/* ---------------------------------------------------------------- */

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
pg_sub_s64_overflow(int64 a, int64 b, int64 *result)
{
	return __builtin_sub_overflow(a, b, result);
}

static inline bool
pg_mul_s64_overflow(int64 a, int64 b, int64 *result)
{
	return __builtin_mul_overflow(a, b, result);
}

/* ---------------------------------------------------------------- */
/* datetime/timestamp/date headers (filtered verbatim copies)        */
/* ---------------------------------------------------------------- */

typedef int64 Timestamp;
typedef int64 TimestampTz;
typedef int64 TimeOffset;
typedef int32 fsec_t;
typedef int32 DateADT;
typedef int64 TimeADT;

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

#include "hdr_datatype_timestamp_h.inc"
#include "hdr_utils_date_h.inc"
#include "hdr_utils_datetime_h.inc"
#include "hdr_utils_formatting_h.inc"

/* utils/timestamp.h excerpts needed by the vendored bodies */
#define MAX_INTERVAL_PRECISION 6
#define TIMESTAMP_MASK(b) (1 << (b))
#define INTERVAL_MASK(b) (1 << (b))

/* pg_type_d.h oids used by parse_datetime */
#define DATEOID 1082
#define TIMEOID 1083
#define TIMESTAMPOID 1114
#define TIMESTAMPTZOID 1184
#define TIMETZOID 1266

/* utils/timestamp.h Datum converters + fmgr getters (pass-by-value int64) */
static inline Timestamp DatumGetTimestamp(Datum X) { return (Timestamp) DatumGetInt64(X); }
static inline TimestampTz DatumGetTimestampTz(Datum X) { return (TimestampTz) DatumGetInt64(X); }
static inline Datum TimestampGetDatum(Timestamp X) { return Int64GetDatum(X); }
static inline Datum TimestampTzGetDatum(TimestampTz X) { return Int64GetDatum(X); }
#define PG_GETARG_TIMESTAMP(n) DatumGetTimestamp(PG_GETARG_DATUM(n))
#define PG_GETARG_TIMESTAMPTZ(n) DatumGetTimestampTz(PG_GETARG_DATUM(n))
#define PG_GETARG_INTERVAL_P(n) ((Interval *) DatumGetPointer(PG_GETARG_DATUM(n)))
#define PG_RETURN_TIMESTAMP(x) return TimestampGetDatum(x)
#define PG_RETURN_TIMESTAMPTZ(x) return TimestampTzGetDatum(x)

/* mbutils.c ordering glue */
static int	pg_mblen_with_len(const char *mbstr, int limit);

/* ---------------------------------------------------------------- */
/* environment-pin shims: encoding, locale, timezone                 */
/* ---------------------------------------------------------------- */

#define PG_UTF8 6				/* pg_wchar.h enum value */
#define MAX_MULTIBYTE_CHAR_LEN 4
#define VALGRIND_CHECK_MEM_IS_DEFINED(a, b) ((void) 0)	/* no-valgrind build */

/*
 * ENCODING PIN: a one-encoding pg_wchar_table (UTF8 -> pg_utf_mblen) plus a
 * fixed DatabaseEncoding, so the mbutils.c bodies compile verbatim.
 */
static int	pg_utf_mblen(const unsigned char *s);	/* vendored below */

typedef struct
{
	int			(*mblen) (const unsigned char *mbstr);
} pg_wchar_tbl_shim;

static const pg_wchar_tbl_shim pg_wchar_table[PG_UTF8 + 1] = {
	[PG_UTF8] = {pg_utf_mblen},
};

static const struct
{
	int			encoding;
}			DatabaseEncodingData = {PG_UTF8}, *DatabaseEncoding = &DatabaseEncodingData;

/*
 * report_invalid_encoding_db raises ERRCODE_CHARACTER_NOT_IN_REPERTOIRE
 * (22021) in real PG; same class here, then throw.
 */
#define ERRCODE_CHARACTER_NOT_IN_REPERTOIRE 117 /* 22021 */

__attribute__((noreturn)) static void
report_invalid_encoding_db(const char *mbstr, int len, int i)
{
	(void) mbstr;
	(void) len;
	(void) i;
	pg_diff_errcode = ERRCODE_CHARACTER_NOT_IN_REPERTOIRE;
	pg_diff_fmt_throw();
}

static int
GetDatabaseEncoding(void)
{
	return PG_UTF8;
}

static int
pg_database_encoding_max_length(void)
{
	return 4;					/* UTF8 */
}

/* (decl moved above) */

static int
pg_mblen(const char *mbstr)
{
	return pg_utf_mblen((const unsigned char *) mbstr);	/* ENCODING PIN */
}

/* locale */
typedef struct pg_locale_struct
{
	bool		deterministic;
	bool		collate_is_c;
	bool		ctype_is_c;
} *pg_locale_t;

static struct pg_locale_struct pg_diff_c_locale = {true, true, true};

static pg_locale_t
pg_newlocale_from_collation(Oid collid)
{
	if (!OidIsValid(collid))
		abort();				/* callers check first; unreachable */
	return &pg_diff_c_locale;	/* COLLATION PIN: C */
}

/* unreachable under the ctype_is_c pin — loud stubs, never silent data */
static size_t
pg_strlower(char *dst, size_t dstsize, const char *src, ssize_t srclen,
			pg_locale_t locale)
{
	abort();
}

static size_t
pg_strupper(char *dst, size_t dstsize, const char *src, ssize_t srclen,
			pg_locale_t locale)
{
	abort();
}

static size_t
pg_strtitle(char *dst, size_t dstsize, const char *src, ssize_t srclen,
			pg_locale_t locale)
{
	abort();
}

static size_t
pg_strfold(char *dst, size_t dstsize, const char *src, ssize_t srclen,
		   pg_locale_t locale)
{
	abort();
}

/* C-locale (= lc_time C) day/month names, what cache_locale_time yields */
static char *localized_abbrev_days[7 + 1] = {
	"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat", NULL
};
static char *localized_full_days[7 + 1] = {
	"Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday",
	"Saturday", NULL
};
static char *localized_abbrev_months[12 + 1] = {
	"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct",
	"Nov", "Dec", NULL
};
static char *localized_full_months[12 + 1] = {
	"January", "February", "March", "April", "May", "June", "July",
	"August", "September", "October", "November", "December", NULL
};

static void
cache_locale_time(void)
{
	/* LOCALE PIN: arrays above are the lc_time=C strftime names */
}

static struct lconv *
PGLC_localeconv(void)
{
	return localeconv();		/* process locale is C; see file header */
}

/* timezone: GMT pin */
struct pg_tz
{
	int			pg_diff_dummy;
};

static pg_tz pg_diff_gmt_tz;
static pg_tz *session_timezone = &pg_diff_gmt_tz;

static int
pg_next_dst_boundary(const pg_time_t *timep,
					 long int *before_gmtoff, int *before_isdst,
					 pg_time_t *boundary,
					 long int *after_gmtoff, int *after_isdst,
					 const pg_tz *tz)
{
	(void) timep;
	(void) tz;
	*before_gmtoff = 0;
	*before_isdst = 0;
	*boundary = 0;
	*after_gmtoff = 0;
	*after_isdst = 0;
	return 0;					/* GMT: no DST transitions, offset 0 */
}

static bool
TimeZoneAbbrevIsKnown(const char *abbr, pg_tz *tzp,
					  bool *isfixed, int *gmtoff, int *isdst)
{
	(void) tzp;
	if (strcmp(abbr, "gmt") == 0)
	{
		*isfixed = true;
		*gmtoff = 0;
		*isdst = false;
		return true;			/* the only abbrev GMT defines */
	}
	return false;
}

static bool
pg_interpret_timezone_abbrev(const char *abbrev, const pg_time_t *timep,
							 long int *gmtoff, int *isdst, const pg_tz *tz)
{
	(void) timep;
	(void) tz;
	if (strcmp(abbrev, "gmt") == 0)
	{
		*gmtoff = 0;
		*isdst = false;
		return true;
	}
	return false;
}

static TimeZoneAbbrevTable *zoneabbrevtbl = NULL;	/* GUC pin: empty */

static pg_tz *
FetchDynamicTimeZone(TimeZoneAbbrevTable *tbl, const datetkn *tp,
					 DateTimeErrorExtra *extra)
{
	(void) tbl;
	(void) tp;
	(void) extra;
	abort();					/* unreachable: zoneabbrevtbl is NULL */
}

/* timestamp.c helper the vendored bodies call (verbatim semantics) */
static pg_time_t
timestamptz_to_time_t(TimestampTz t)
{
	return (pg_time_t) (t / USECS_PER_SEC +
						((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY));
}

/*
 * GMT-pin pg_localtime: byte-exact behavior of PG's pg_localtime for the
 * GMT zone (zero offset, no DST, abbrev "GMT") — pure calendar arithmetic.
 */
int			date2j(int year, int month, int day);	/* vendored below */
void		j2date(int jd, int *year, int *month, int *day);	/* vendored below */

static struct pg_tm *
pg_localtime(const pg_time_t *timep, const pg_tz *tz)
{
	static _Thread_local struct pg_tm tm;
	pg_time_t	t = *timep;
	int64		days = t / SECS_PER_DAY;
	int64		rem = t % SECS_PER_DAY;
	int			year,
				mon,
				mday;

	(void) tz;
	if (rem < 0)
	{
		rem += SECS_PER_DAY;
		days -= 1;
	}
	j2date((int) (days + UNIX_EPOCH_JDATE), &year, &mon, &mday);
	tm.tm_year = year - 1900;
	tm.tm_mon = mon - 1;
	tm.tm_mday = mday;
	tm.tm_hour = (int) (rem / SECS_PER_HOUR);
	tm.tm_min = (int) ((rem % SECS_PER_HOUR) / SECS_PER_MINUTE);
	tm.tm_sec = (int) (rem % SECS_PER_MINUTE);
	tm.tm_wday = (int) ((days + UNIX_EPOCH_JDATE + 1) % 7);
	tm.tm_yday = date2j(year, mon, mday) - date2j(year, 1, 1);
	tm.tm_isdst = 0;
	tm.tm_gmtoff = 0;
	tm.tm_zone = "GMT";
	return &tm;
}

/* forward decls for vendored bodies (order-of-definition glue); qualifiers
 * match the source files' own declarations */
static const datetkn *datebsearch(const char *key, const datetkn *base, int nel);
static void dt2time(Timestamp jd, int *hour, int *min, int *sec, fsec_t *fsec);
static TimeOffset time2t(const int hour, const int min, const int sec, const fsec_t fsec);
static Timestamp dt2local(Timestamp dt, int timezone);
static int	DetermineTimeZoneOffsetInternal(struct pg_tm *tm, pg_tz *tzp, pg_time_t *tp);
static bool DetermineTimeZoneAbbrevOffsetInternal(pg_time_t t, const char *abbr, pg_tz *tzp,
												  int *offset, int *isdst);
unsigned char pg_toupper(unsigned char ch);
unsigned char pg_tolower(unsigned char ch);

/* ---------------------------------------------------------------- */
/* vendored verbatim bodies                                          */
/* ---------------------------------------------------------------- */

#include "pg_fmt_deps_18_3.inc"
#include "pg_formatting_18_3.inc"

/* ---------------------------------------------------------------- */
/* pg_diff_* driver entries                                          */
/* ---------------------------------------------------------------- */

/*
 * Common return protocol:
 *   0  -> Ok, result copied to out (text: bytes; scalar: *out_v)
 *   1  -> SQL NULL result
 *  -1  -> ERROR raised; class in pg_diff_errcode_get()
 *  -2  -> result exceeded out_cap (harness sizing defect, not a verdict)
 */

static int
pg_diff_fmt_text_result(Datum d, bool isnull, uint8_t *out, int32_t out_cap,
						int32_t *out_len)
{
	text	   *t;
	int32		len;

	if (isnull)
		return 1;
	t = (text *) DatumGetPointer(d);
	len = VARSIZE_ANY_EXHDR(t);
	if (len > out_cap)
		return -2;
	memcpy(out, VARDATA_ANY(t), len);
	*out_len = len;
	return 0;
}

int
pg_diff_fmt_timestamp_to_char(int64_t ts, const uint8_t *fmt, int32_t fmt_len,
							  uint8_t *out, int32_t out_cap, int32_t *out_len)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	FunctionCallInfoBaseData fcdata;
	Datum		d;

	pg_diff_fmt_reset();
	if (setjmp(pg_diff_fmt_jmp) != 0)
		return -1;
	fcdata.context = NULL;
	fcdata.fncollation = C_COLLATION_OID;
	fcdata.isnull = false;
	fcdata.nargs = 2;
	fcdata.args[0].value = Int64GetDatum(ts);
	fcdata.args[0].isnull = false;
	fcdata.args[1].value = PointerGetDatum(cstring_to_text_with_len((const char *) fmt, fmt_len));
	fcdata.args[1].isnull = false;
	d = timestamp_to_char(&fcdata);
	return pg_diff_fmt_text_result(d, fcdata.isnull, out, out_cap, out_len);
}

int
pg_diff_fmt_timestamptz_to_char(int64_t ts, const uint8_t *fmt, int32_t fmt_len,
								uint8_t *out, int32_t out_cap, int32_t *out_len)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	FunctionCallInfoBaseData fcdata;
	Datum		d;

	pg_diff_fmt_reset();
	if (setjmp(pg_diff_fmt_jmp) != 0)
		return -1;
	fcdata.context = NULL;
	fcdata.fncollation = C_COLLATION_OID;
	fcdata.isnull = false;
	fcdata.nargs = 2;
	fcdata.args[0].value = Int64GetDatum(ts);
	fcdata.args[0].isnull = false;
	fcdata.args[1].value = PointerGetDatum(cstring_to_text_with_len((const char *) fmt, fmt_len));
	fcdata.args[1].isnull = false;
	d = timestamptz_to_char(&fcdata);
	return pg_diff_fmt_text_result(d, fcdata.isnull, out, out_cap, out_len);
}

int
pg_diff_fmt_interval_to_char(int64_t time_usec, int32_t day, int32_t month,
							 const uint8_t *fmt, int32_t fmt_len,
							 uint8_t *out, int32_t out_cap, int32_t *out_len)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	FunctionCallInfoBaseData fcdata;
	Interval	iv;
	Datum		d;

	pg_diff_fmt_reset();
	if (setjmp(pg_diff_fmt_jmp) != 0)
		return -1;
	iv.time = time_usec;
	iv.day = day;
	iv.month = month;
	fcdata.context = NULL;
	fcdata.fncollation = C_COLLATION_OID;
	fcdata.isnull = false;
	fcdata.nargs = 2;
	fcdata.args[0].value = PointerGetDatum(&iv);
	fcdata.args[0].isnull = false;
	fcdata.args[1].value = PointerGetDatum(cstring_to_text_with_len((const char *) fmt, fmt_len));
	fcdata.args[1].isnull = false;
	d = interval_to_char(&fcdata);
	return pg_diff_fmt_text_result(d, fcdata.isnull, out, out_cap, out_len);
}

int
pg_diff_fmt_to_timestamp(const uint8_t *txt, int32_t txt_len,
						 const uint8_t *fmt, int32_t fmt_len,
						 int64_t *out_ts)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	FunctionCallInfoBaseData fcdata;
	Datum		d;

	pg_diff_fmt_reset();
	if (setjmp(pg_diff_fmt_jmp) != 0)
		return -1;
	fcdata.context = NULL;
	fcdata.fncollation = C_COLLATION_OID;
	fcdata.isnull = false;
	fcdata.nargs = 2;
	fcdata.args[0].value = PointerGetDatum(cstring_to_text_with_len((const char *) txt, txt_len));
	fcdata.args[0].isnull = false;
	fcdata.args[1].value = PointerGetDatum(cstring_to_text_with_len((const char *) fmt, fmt_len));
	fcdata.args[1].isnull = false;
	d = to_timestamp(&fcdata);
	if (fcdata.isnull)
		return 1;
	*out_ts = DatumGetInt64(d);
	return 0;
}

int
pg_diff_fmt_to_date(const uint8_t *txt, int32_t txt_len,
					const uint8_t *fmt, int32_t fmt_len,
					int32_t *out_date)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	FunctionCallInfoBaseData fcdata;
	Datum		d;

	pg_diff_fmt_reset();
	if (setjmp(pg_diff_fmt_jmp) != 0)
		return -1;
	fcdata.context = NULL;
	fcdata.fncollation = C_COLLATION_OID;
	fcdata.isnull = false;
	fcdata.nargs = 2;
	fcdata.args[0].value = PointerGetDatum(cstring_to_text_with_len((const char *) txt, txt_len));
	fcdata.args[0].isnull = false;
	fcdata.args[1].value = PointerGetDatum(cstring_to_text_with_len((const char *) fmt, fmt_len));
	fcdata.args[1].isnull = false;
	d = to_date(&fcdata);
	if (fcdata.isnull)
		return 1;
	*out_date = DatumGetInt32(d);
	return 0;
}

/*
 * parse_datetime: out_kind is a small enum the Rust driver mirrors:
 * 1=date, 2=time, 3=timetz, 4=timestamp, 5=timestamptz.
 * timetz packs (TimeADT, zone) into out_v / out_v2.
 */
int
pg_diff_fmt_parse_datetime(const uint8_t *txt, int32_t txt_len,
						   const uint8_t *fmt, int32_t fmt_len,
						   int32_t strict,
						   int32_t *out_kind, int32_t *out_typmod,
						   int32_t *out_tz, int64_t *out_v, int32_t *out_v2)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	Datum		d;
	Oid			typid;
	int32		typmod;
	int			tz;
	text	   *dt_txt;
	text	   *fmt_txt;

	pg_diff_fmt_reset();
	if (setjmp(pg_diff_fmt_jmp) != 0)
		return -1;
	dt_txt = cstring_to_text_with_len((const char *) txt, txt_len);
	fmt_txt = cstring_to_text_with_len((const char *) fmt, fmt_len);
	d = parse_datetime(dt_txt, fmt_txt, C_COLLATION_OID, strict != 0,
					   &typid, &typmod, &tz, NULL);
	*out_typmod = typmod;
	*out_tz = tz;
	*out_v2 = 0;
	switch (typid)
	{
		case 1082:				/* DATEOID */
			*out_kind = 1;
			*out_v = (int64) DatumGetInt32(d);
			break;
		case 1083:				/* TIMEOID */
			*out_kind = 2;
			*out_v = DatumGetInt64(d);
			break;
		case 1266:				/* TIMETZOID */
			{
				TimeTzADT  *tt = (TimeTzADT *) DatumGetPointer(d);

				*out_kind = 3;
				*out_v = tt->time;
				*out_v2 = tt->zone;
			}
			break;
		case 1114:				/* TIMESTAMPOID */
			*out_kind = 4;
			*out_v = DatumGetInt64(d);
			break;
		case 1184:				/* TIMESTAMPTZOID */
			*out_kind = 5;
			*out_v = DatumGetInt64(d);
			break;
		default:
			abort();			/* parse_datetime yields no other type */
	}
	return 0;
}

int
pg_diff_fmt_datetime_format_has_tz(const uint8_t *fmt, int32_t fmt_len)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	char	   *fmt_str;
	bool		r;

	pg_diff_fmt_reset();
	if (setjmp(pg_diff_fmt_jmp) != 0)
		return -1;
	fmt_str = pnstrdup((const char *) fmt, fmt_len);
	r = datetime_format_has_tz(fmt_str);
	return r ? 1 : 0;
}

/* ================================================================== */
/* fmt_num_diff half (NUM SQL entries + numeric/int/float closure).    */
/* pg_fmt_num_io.c is NOT compiled: both halves share this TU because  */
/* the NUM entries call the static NUM_processor/NUM_cache above.      */
/* ================================================================== */

/* --- shims for the numeric.c slice (plumbing only) --- */

typedef struct SortSupportData *SortSupport;	/* decl-only in the slice */
typedef struct NumericVar NumericVar;	/* forward for decl block */

#define PG_GETARG_CSTRING(n) ((char *) DatumGetPointer(PG_GETARG_DATUM(n)))
#define PG_GETARG_INT64(n) DatumGetInt64(PG_GETARG_DATUM(n))
#define PG_GETARG_OID(n) ((Oid) PG_GETARG_DATUM(n))
#define PG_RETURN_CSTRING(x) return PointerGetDatum(x)
#define PG_RETURN_INT32(x) return Int32GetDatum(x)
#define PG_RETURN_INT64(x) return Int64GetDatum(x)
#define PG_RETURN_FLOAT8(x) return Float8GetDatum(x)
#define PG_RETURN_FLOAT4(x) return Float4GetDatum(x)
#define PG_RETURN_BOOL(x) return ((Datum) ((x) ? 1 : 0))
#define DatumGetCString(d) ((char *) DatumGetPointer(d))
#define CStringGetDatum(s) PointerGetDatum(s)
#define ObjectIdGetDatum(o) ((Datum) (o))

static inline Datum
Float8GetDatum(float8 X)
{
	union { float8 f; Datum d; } u;
	u.f = X;
	return u.d;
}

static inline float8
DatumGetFloat8(Datum X)
{
	union { Datum d; float8 f; } u;
	u.d = X;
	return u.f;
}

static inline Datum
Float4GetDatum(float4 X)
{
	union { struct { float4 f; float4 pad; } s; Datum d; } u;
	u.d = 0;
	u.s.f = X;
	return u.d;
}

static inline float4
DatumGetFloat4(Datum X)
{
	union { Datum d; struct { float4 f; float4 pad; } s; } u;
	u.d = X;
	return u.s.f;
}

#define PG_GETARG_FLOAT8(n) DatumGetFloat8(PG_GETARG_DATUM(n))
#define PG_GETARG_FLOAT4(n) DatumGetFloat4(PG_GETARG_DATUM(n))

/*
 * DirectFunctionCallN: the real fmgr raises "function returned NULL" if the
 * callee sets isnull; none of the vendored callees here do on the paths the
 * NUM entries use. fncollation = InvalidOid exactly as in fmgr.c.
 */
static Datum
pg_diff_dfc(Datum (*func) (FunctionCallInfo), int nargs,
			Datum a0, Datum a1, Datum a2)
{
	FunctionCallInfoBaseData fc;

	fc.context = NULL;
	fc.fncollation = InvalidOid;
	fc.isnull = false;
	fc.nargs = (short) nargs;
	fc.args[0].value = a0;
	fc.args[0].isnull = false;
	fc.args[1].value = a1;
	fc.args[1].isnull = false;
	fc.args[2].value = a2;
	fc.args[2].isnull = false;
	return func(&fc);
}

#define DirectFunctionCall1(f, a) pg_diff_dfc(f, 1, (a), 0, 0)
#define DirectFunctionCall2(f, a, b) pg_diff_dfc(f, 2, (a), (b), 0)
#define DirectFunctionCall3(f, a, b, c) pg_diff_dfc(f, 3, (a), (b), (c))

/* utils/float.h verbatim macros (used by float4/8_to_char + dtoi4/ftoi4) */
#define FLOAT4_FITS_IN_INT32(num) \
	((num) >= (float4) PG_INT32_MIN && (num) < -((float4) PG_INT32_MIN))
#define FLOAT8_FITS_IN_INT32(num) \
	((num) >= (float8) PG_INT32_MIN && (num) < -((float8) PG_INT32_MIN))
#define FLOAT8_FITS_IN_INT64(num) \
	((num) >= (float8) PG_INT64_MIN && (num) < -((float8) PG_INT64_MIN))

/*
 * psprintf shim. Real PG routes these through src/port/snprintf.c, whose
 * fmtfloat() renders NaN as "NaN" and infinities as "[sign]Infinity"
 * PLATFORM-INDEPENDENTLY (snprintf.c:1205-1232) before delegating finite
 * digits to the C library. libc printf would say "nan"/"inf", so the
 * special arm is reproduced here; finite values go to libc vsnprintf
 * exactly as PG's fmtfloat does. Only the three float shapes the NUM
 * entries use are accepted ("%+.*e", "%.*f", "%.0f") — anything else
 * aborts loudly (oracle guard, not silent data).
 */
static char *
psprintf(const char *fmt2, ...)
{
	char	   *buf = palloc(1024);
	va_list		ap;
	int			forcesign = 0;
	int			prec = 0;
	double		val;

	va_start(ap, fmt2);
	if (strcmp(fmt2, "%+.*e") == 0)
	{
		forcesign = 1;
		prec = va_arg(ap, int);
		val = va_arg(ap, double);
	}
	else if (strcmp(fmt2, "%.*f") == 0)
	{
		prec = va_arg(ap, int);
		val = va_arg(ap, double);
	}
	else if (strcmp(fmt2, "%.0f") == 0)
	{
		prec = 0;
		val = va_arg(ap, double);
	}
	else
		abort();
	va_end(ap);

	if (isnan(val))
	{
		strcpy(buf, "NaN");		/* NaNs have no sign (fmtfloat) */
		return buf;
	}
	if (isinf(val) || (val == 0.0 && signbit(val)) || val < 0.0 || 1)
	{
		char	   *pp = buf;
		int			neg = (val < 0.0) || (val == 0.0 && signbit(val));

		if (neg)
			val = -val;
		if (isinf(val))
		{
			if (neg)
				*pp++ = '-';
			else if (forcesign)
				*pp++ = '+';
			strcpy(pp, "Infinity");
			return buf;
		}
		if (neg)
			val = -val;			/* restore; libc handles the sign */
	}
	if (prec < 0)
		prec = 0;
	if (prec > 350)
		prec = 350;				/* fmtfloat's cap; zeropad tail unreachable
								 * here (NUM post digits < 350) */
	{
		char		f[8];

		snprintf(f, sizeof(f), "%%%s.%d%c", forcesign ? "+" : "", prec,
				 strchr(fmt2, 'e') ? 'e' : 'f');
		snprintf(buf, 1024, f, val);
	}
	return buf;
}

/* utils/numeric.h essentials (Numeric is a varlena; ours are always 4B) */
struct NumericData;
typedef struct NumericData *Numeric;
#define NUMERIC_MAX_PRECISION 1000
#define NUMERIC_MIN_SCALE (-1000)
#define NUMERIC_MAX_SCALE 1000
#define NUMERIC_MAX_DISPLAY_SCALE NUMERIC_MAX_PRECISION
#define NUMERIC_MIN_DISPLAY_SCALE 0
#define NUMERIC_MAX_RESULT_SCALE (NUMERIC_MAX_PRECISION * 2)
#define NUMERIC_MIN_SIG_DIGITS 16
#define PG_UINT64_MAX UINT64_MAX
#define DatumGetNumeric(d) ((Numeric) DatumGetPointer(d))
#define NumericGetDatum(n) PointerGetDatum(n)
#define PG_GETARG_NUMERIC(n) DatumGetNumeric(PG_GETARG_DATUM(n))
#define PG_RETURN_NUMERIC(n) return NumericGetDatum(n)

/* decl-only types referenced by the prologue's static-declaration block */
typedef struct StringInfoData *StringInfo;
typedef struct pg_prng_state pg_prng_state;
typedef struct hyperLogLogState
{
	int			pg_diff_dummy;	/* decl-shim: abbrev-sort machinery unused */
} hyperLogLogState;
#define MAXINT8LEN 20			/* int8.h */

#include "pg_numeric_prologue_18_3.inc"

/* order-of-definition glue for the extracted bodies */
static int	numeric_sign_internal(Numeric num);
static bool numeric_is_integral(Numeric num);
Numeric		numeric_mul_opt_error(Numeric num1, Numeric num2, bool *have_error);
/* common/int.h absolute-value helpers (verbatim semantics) */
static inline uint32 pg_abs_s32(int32 a) { return a < 0 ? 0 - (uint32) a : (uint32) a; }
static inline uint64 pg_abs_s64(int64 a) { return a < 0 ? 0 - (uint64) a : (uint64) a; }
#define i64abs(i) ((i) < 0 ? -(i) : (i))
static inline bool is_valid_numeric_typmod(int32 typmod);
static inline int numeric_typmod_precision(int32 typmod);
static inline int numeric_typmod_scale(int32 typmod);
static inline int xdigit_value(char dig);
int			pg_ltoa(int32 value, char *a);
int			pg_lltoa(int64 value, char *a);
int			pg_ulltoa_n(uint64 value, char *a);
/* pg_bitutils.h shims (builtin clz = the x86/arm codepath PG selects) */
static inline int pg_leftmost_one_pos32(uint32 word) { return 31 - __builtin_clz(word); }
static inline int pg_leftmost_one_pos64(uint64 word) { return 63 - __builtin_clzll(word); }

/* --- src/backend/utils/adt/numutils.c:44-61 decimalLength32 (verbatim) --- */
static inline int
decimalLength32(const uint32 v)
{
	int			t;
	static const uint32 PowersOfTen[] = {
		1, 10, 100,
		1000, 10000, 100000,
		1000000, 10000000, 100000000,
		1000000000
	};

	/*
	 * Compute base-10 logarithm by dividing the base-2 logarithm by a
	 * good-enough approximation of the base-2 logarithm of 10
	 */
	t = (pg_leftmost_one_pos32(v) + 1) * 1233 / 4096;
	return t + (v >= PowersOfTen[t]);
}

/* --- src/backend/utils/adt/numutils.c:63-86 decimalLength64 (verbatim) --- */
static inline int
decimalLength64(const uint64 v)
{
	int			t;
	static const uint64 PowersOfTen[] = {
		UINT64CONST(1), UINT64CONST(10),
		UINT64CONST(100), UINT64CONST(1000),
		UINT64CONST(10000), UINT64CONST(100000),
		UINT64CONST(1000000), UINT64CONST(10000000),
		UINT64CONST(100000000), UINT64CONST(1000000000),
		UINT64CONST(10000000000), UINT64CONST(100000000000),
		UINT64CONST(1000000000000), UINT64CONST(10000000000000),
		UINT64CONST(100000000000000), UINT64CONST(1000000000000000),
		UINT64CONST(10000000000000000), UINT64CONST(100000000000000000),
		UINT64CONST(1000000000000000000), UINT64CONST(10000000000000000000)
	};

	/*
	 * Compute base-10 logarithm by dividing the base-2 logarithm by a
	 * good-enough approximation of the base-2 logarithm of 10
	 */
	t = (pg_leftmost_one_pos64(v) + 1) * 1233 / 4096;
	return t + (v >= PowersOfTen[t]);
}

/* --- src/backend/utils/adt/numutils.c:29-39 DIGIT_TABLE (verbatim) --- */
static const char DIGIT_TABLE[200] =
"00" "01" "02" "03" "04" "05" "06" "07" "08" "09"
"10" "11" "12" "13" "14" "15" "16" "17" "18" "19"
"20" "21" "22" "23" "24" "25" "26" "27" "28" "29"
"30" "31" "32" "33" "34" "35" "36" "37" "38" "39"
"40" "41" "42" "43" "44" "45" "46" "47" "48" "49"
"50" "51" "52" "53" "54" "55" "56" "57" "58" "59"
"60" "61" "62" "63" "64" "65" "66" "67" "68" "69"
"70" "71" "72" "73" "74" "75" "76" "77" "78" "79"
"80" "81" "82" "83" "84" "85" "86" "87" "88" "89"
"90" "91" "92" "93" "94" "95" "96" "97" "98" "99";

int			pg_ultoa_n(uint32 value, char *a);

#include "pg_numeric_deps_18_3.inc"
#include "pg_formatting_num_18_3.inc"

/* ---------------------------------------------------------------- */
/* pg_diff_* driver entries — NUM half (fmt_num_diff)                */
/* Return protocol identical to the DCH entries above.               */
/* ---------------------------------------------------------------- */

static Datum
pg_diff_fmt_numeric_from_cstr(const uint8_t *num_str, int32_t num_len)
{
	char	   *cstr = pnstrdup((const char *) num_str, num_len);

	return DirectFunctionCall3(numeric_in, CStringGetDatum(cstr),
							   ObjectIdGetDatum(InvalidOid),
							   Int32GetDatum(-1));
}

int
pg_diff_fmt_numeric_to_char(const uint8_t *num_str, int32_t num_len,
							const uint8_t *fmt, int32_t fmt_len,
							uint8_t *out, int32_t out_cap, int32_t *out_len)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	FunctionCallInfoBaseData fcdata;
	Datum		d;

	pg_diff_fmt_reset();
	if (setjmp(pg_diff_fmt_jmp) != 0)
		return -1;
	fcdata.context = NULL;
	fcdata.fncollation = C_COLLATION_OID;
	fcdata.isnull = false;
	fcdata.nargs = 2;
	fcdata.args[0].value = pg_diff_fmt_numeric_from_cstr(num_str, num_len);
	fcdata.args[0].isnull = false;
	fcdata.args[1].value = PointerGetDatum(cstring_to_text_with_len((const char *) fmt, fmt_len));
	fcdata.args[1].isnull = false;
	d = numeric_to_char(&fcdata);
	return pg_diff_fmt_text_result(d, fcdata.isnull, out, out_cap, out_len);
}

static int
pg_diff_fmt_scalar_to_char(Datum arg0, Datum (*fn) (FunctionCallInfo),
						   const uint8_t *fmt, int32_t fmt_len,
						   uint8_t *out, int32_t out_cap, int32_t *out_len)
{
	FunctionCallInfoBaseData fcdata;
	Datum		d;

	/* caller did pg_diff_fmt_reset + setjmp */
	fcdata.context = NULL;
	fcdata.fncollation = C_COLLATION_OID;
	fcdata.isnull = false;
	fcdata.nargs = 2;
	fcdata.args[0].value = arg0;
	fcdata.args[0].isnull = false;
	fcdata.args[1].value = PointerGetDatum(cstring_to_text_with_len((const char *) fmt, fmt_len));
	fcdata.args[1].isnull = false;
	d = fn(&fcdata);
	return pg_diff_fmt_text_result(d, fcdata.isnull, out, out_cap, out_len);
}

int
pg_diff_fmt_int4_to_char(int32_t v, const uint8_t *fmt, int32_t fmt_len,
						 uint8_t *out, int32_t out_cap, int32_t *out_len)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	pg_diff_fmt_reset();
	if (setjmp(pg_diff_fmt_jmp) != 0)
		return -1;
	return pg_diff_fmt_scalar_to_char(Int32GetDatum(v), int4_to_char,
									  fmt, fmt_len, out, out_cap, out_len);
}

int
pg_diff_fmt_int8_to_char(int64_t v, const uint8_t *fmt, int32_t fmt_len,
						 uint8_t *out, int32_t out_cap, int32_t *out_len)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	pg_diff_fmt_reset();
	if (setjmp(pg_diff_fmt_jmp) != 0)
		return -1;
	return pg_diff_fmt_scalar_to_char(Int64GetDatum(v), int8_to_char,
									  fmt, fmt_len, out, out_cap, out_len);
}

int
pg_diff_fmt_float4_to_char(float v, const uint8_t *fmt, int32_t fmt_len,
						   uint8_t *out, int32_t out_cap, int32_t *out_len)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	pg_diff_fmt_reset();
	if (setjmp(pg_diff_fmt_jmp) != 0)
		return -1;
	return pg_diff_fmt_scalar_to_char(Float4GetDatum(v), float4_to_char,
									  fmt, fmt_len, out, out_cap, out_len);
}

int
pg_diff_fmt_float8_to_char(double v, const uint8_t *fmt, int32_t fmt_len,
						   uint8_t *out, int32_t out_cap, int32_t *out_len)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	pg_diff_fmt_reset();
	if (setjmp(pg_diff_fmt_jmp) != 0)
		return -1;
	return pg_diff_fmt_scalar_to_char(Float8GetDatum(v), float8_to_char,
									  fmt, fmt_len, out, out_cap, out_len);
}

/*
 * numeric_to_number: result numeric is rendered through the vendored
 * numeric_out so both sides compare canonical decimal strings.
 */
int
pg_diff_fmt_numeric_to_number(const uint8_t *txt, int32_t txt_len,
							  const uint8_t *fmt, int32_t fmt_len,
							  uint8_t *out, int32_t out_cap, int32_t *out_len)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	FunctionCallInfoBaseData fcdata;
	Datum		d;
	char	   *str;
	int32		len;

	pg_diff_fmt_reset();
	if (setjmp(pg_diff_fmt_jmp) != 0)
		return -1;
	fcdata.context = NULL;
	fcdata.fncollation = C_COLLATION_OID;
	fcdata.isnull = false;
	fcdata.nargs = 2;
	fcdata.args[0].value = PointerGetDatum(cstring_to_text_with_len((const char *) txt, txt_len));
	fcdata.args[0].isnull = false;
	fcdata.args[1].value = PointerGetDatum(cstring_to_text_with_len((const char *) fmt, fmt_len));
	fcdata.args[1].isnull = false;
	d = numeric_to_number(&fcdata);
	if (fcdata.isnull)
		return 1;
	str = DatumGetCString(DirectFunctionCall1(numeric_out, d));
	len = (int32) strlen(str);
	if (len > out_cap)
		return -2;
	memcpy(out, str, len);
	*out_len = len;
	return 0;
}
