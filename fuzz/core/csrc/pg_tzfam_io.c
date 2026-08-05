/*
 * pg_tzfam_io.c: vendored PostgreSQL C oracle for the tzfam_diff
 * differential fuzz target (100%-coverage campaign, lane p1-mb-tzfam).
 * Crates under test (see fuzz/core/src/tzfam_diff.rs):
 *   crates/backend/timezone/strftime, crates/backend/utils/misc/tzparser,
 *   crates/backend/tsearch/ts_locale.
 *
 * Provenance (all bodies VERBATIM sed-extracted from the vendor tree at
 * ~/dev/pgrust-fabled/vendor/postgres-src, Stamp-18.3, upstream sha
 * 62d6c7d3df6287f1bd83199c1a746e50d31571a0 — assembled by
 * scratchpad/assemble_tzfam.sh, never hand-typed):
 *   - src/include/pgtime.h lines 34-47 (struct pg_tm).
 *   - src/timezone/private.h lines 56-57 (TYPE_BIT/TYPE_SIGNED), 81-83
 *     (INT_STRLEN_MAXIMUM), 97-105 (SECSPERMIN..MONSPERYEAR), 128
 *     (TM_YEAR_BASE), 133 (isleap), 147 (isleap_sum).
 *   - src/timezone/strftime.c lines 48-571 (struct lc_time_T,
 *     C_time_locale, enum warn, pg_strftime, _fmt, _conv, _add, _yconv —
 *     the whole functional file after the #includes).
 *   - src/include/utils/tzparser.h lines 23-34 (tzEntry).
 *   - src/include/utils/datetime.h lines 95-97 (TZ/DTZ/DYNTZ), 204
 *     (TOKMAXLEN), 206-229 (datetkn, TimeZoneAbbrevTable,
 *     DynamicZoneAbbrev).
 *   - src/backend/utils/misc/tzparser.c lines 35-487 (WHITESPACE,
 *     validateTzEntry, splitTzLine, addToArray, ParseTzFile,
 *     load_tzoffsets — the whole functional file).
 *   - src/backend/utils/adt/datetime.c lines 4986-5071
 *     (ConvertTimeZoneAbbrevs).
 *   - src/port/pgstrcasecmp.c lines 32-62 (pg_strcasecmp), 64-95
 *     (pg_strncasecmp), 113-129 (pg_tolower).
 *   - src/port/strlcpy.c lines 38-71 (strlcpy, tzf_-prefixed: glibc has
 *     no strlcpy and macOS libc's must not be shadowed).
 *   - src/include/c.h lines 1126-1127 (HIGHBIT/IS_HIGHBIT_SET), 773-774
 *     (TYPEALIGN), 780 (MAXALIGN); TOUCHAR from c.h (single #define).
 *   - src/backend/tsearch/ts_locale.c lines 23-68 (WC_BUF_LEN comment +
 *     GENERATE_T_ISCLASS_DEF + alnum/alpha instantiations).
 *   - src/include/tsearch/ts_locale.h lines 37-38 (t_iseq).
 *
 * Shims (plumbing/environment only, never logic):
 *   - fixed-width typedefs matching c.h on LP64; Size = size_t; Assert
 *     no-op (release parity); FLEXIBLE_ARRAY_MEMBER -> empty.
 *   - palloc/pstrdup/repalloc/guc_malloc -> tracked malloc arena, freed by
 *     pg_tzf_reset() per exec (models the TZParserMemory temp context;
 *     AllocSetContextCreate/MemoryContextSwitchTo/MemoryContextDelete are
 *     no-ops accordingly).
 *   - GUC_check_errmsg/errdetail/errhint -> vsnprintf capture into static
 *     slots (guc.c check-hook protocol channel); %m expanded to
 *     strerror(errno) as elog.c does (the only %m consumers are the two
 *     filesystem arms + ferror arm, compared prefix-only by the driver:
 *     documented oracle-platform-variance carve for message tails).
 *   - AllocateFile/FreeFile/AllocateDir/ReadDir/FreeDir -> stdio/dirent
 *     (fd.c resource-owner bookkeeping is server plumbing).
 *   - get_share_path(my_exec_path, out) -> strcpy(getenv("PGRUST_PGSHAREDIR"))
 *     (environment mock; the Rust side pins the SAME env var by design —
 *     tzsets_dir()'s non-env fallbacks are the excluded-state carve).
 *   - ts_locale: database_ctype_is_c pinned 1 (the census C-locale arm;
 *     char2wchar stub aborts — the locale-dependent wide path is the
 *     carved-out arm on both sides), pg_mblen_with_len/_cstr/_unbounded
 *     resolved against the verbatim wfam_ copies in pg_wcharfam.c,
 *     pg_locale_t opaque pointer typedef.
 *   - Every extern definition is tzf_/pg_tzf_-prefixed via #define so this
 *     TU cannot collide with other oracle TUs in the fuzz cc build.
 *
 * Driver entries (SECTION D, pg_tzf_* prefix) are fuzz plumbing, NOT
 * Postgres code.
 */

#include <stddef.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <ctype.h>
#include <limits.h>
#include <errno.h>
#include <dirent.h>
#include <wchar.h>
#include <wctype.h>
#include "pg_oracle_guard.h"	/* oracle-serialization holder check */

typedef int8_t int8;
typedef int16_t int16;
typedef int32_t int32;
typedef int64_t int64;
typedef uint8_t uint8;
typedef uint16_t uint16;
typedef uint32_t uint32;
typedef uint64_t uint64;
typedef size_t Size;

#define Assert(x) ((void) 0)
#define FLEXIBLE_ARRAY_MEMBER	/* empty */
#define MAXPGPATH		1024	/* pg_config_manual.h line 100 */
#define MAXIMUM_ALIGNOF 8
#define TOUCHAR(ptr)	(*((const unsigned char *) (ptr)))

/* ==== VERBATIM: c.h lines 773-774, 780, 1126-1127 @ 62d6c7d3df ==== */
#define TYPEALIGN(ALIGNVAL,LEN)  \
	(((uintptr_t) (LEN) + ((ALIGNVAL) - 1)) & ~((uintptr_t) ((ALIGNVAL) - 1)))
#define MAXALIGN(LEN)			TYPEALIGN(MAXIMUM_ALIGNOF, (LEN))
#define HIGHBIT					(0x80)
#define IS_HIGHBIT_SET(ch)		((unsigned char)(ch) & HIGHBIT)

/* ==== symbol prefixing: every extern definition in this TU ==== */
#define pg_strftime				tzf_pg_strftime
#define load_tzoffsets			tzf_load_tzoffsets
#define ConvertTimeZoneAbbrevs	tzf_ConvertTimeZoneAbbrevs
#define pg_strcasecmp			tzf_pg_strcasecmp
#define pg_strncasecmp			tzf_pg_strncasecmp
#define pg_tolower				tzf_pg_tolower
#undef strlcpy
#define strlcpy					tzf_strlcpy
#define t_isalnum_with_len		tzf_t_isalnum_with_len
#define t_isalnum_cstr			tzf_t_isalnum_cstr
#define t_isalnum_unbounded		tzf_t_isalnum_unbounded
#define t_isalnum				tzf_t_isalnum
#define t_isalpha_with_len		tzf_t_isalpha_with_len
#define t_isalpha_cstr			tzf_t_isalpha_cstr
#define t_isalpha_unbounded		tzf_t_isalpha_unbounded
#define t_isalpha				tzf_t_isalpha
#define database_ctype_is_c		tzf_database_ctype_is_c
#define char2wchar				tzf_char2wchar
#define my_exec_path			tzf_my_exec_path

/* pg_mblen family: resolved against the VERBATIM wfam_ copies vendored in
 * pg_wcharfam.c (mbutils.c bodies; one verbatim definition per symbol
 * across the fuzz oracle build). Encoding pinned via wfam_x_set_db_enc. */
extern int	wfam_pg_mblen_with_len(const char *mbstr, int limit);
extern int	wfam_pg_mblen_cstr(const char *mbstr);
extern int	wfam_pg_mblen_unbounded(const char *mbstr);
#define pg_mblen_with_len	wfam_pg_mblen_with_len
#define pg_mblen_cstr		wfam_pg_mblen_cstr
#define pg_mblen_unbounded	wfam_pg_mblen_unbounded

/* ==== VERBATIM: struct pg_tm (pgtime.h lines 34-47 @ 62d6c7d3df) ==== */
struct pg_tm
{
	int			tm_sec;
	int			tm_min;
	int			tm_hour;
	int			tm_mday;
	int			tm_mon;			/* see above */
	int			tm_year;		/* see above */
	int			tm_wday;
	int			tm_yday;
	int			tm_isdst;
	long int	tm_gmtoff;
	const char *tm_zone;
};

/* ==== VERBATIM: private.h 56-57, 81-83, 97-105, 128, 133, 147 @ 62d6c7d3df ==== */
#define TYPE_BIT(type)	(sizeof (type) * CHAR_BIT)
#define TYPE_SIGNED(type) (((type) -1) < 0)
#define INT_STRLEN_MAXIMUM(type) \
	((TYPE_BIT(type) - TYPE_SIGNED(type)) * 302 / 1000 + \
	1 + TYPE_SIGNED(type))
#define SECSPERMIN	60
#define MINSPERHOUR 60
#define HOURSPERDAY 24
#define DAYSPERWEEK 7
#define DAYSPERNYEAR	365
#define DAYSPERLYEAR	366
#define SECSPERHOUR (SECSPERMIN * MINSPERHOUR)
#define SECSPERDAY	((int32) SECSPERHOUR * HOURSPERDAY)
#define MONSPERYEAR 12
#define TM_YEAR_BASE	1900
#define isleap(y) (((y) % 4) == 0 && (((y) % 100) != 0 || ((y) % 400) == 0))
#define isleap_sum(a, b)	isleap((a) % 400 + (b) % 400)

/* ==== VERBATIM: strftime.c lines 48-571 @ 62d6c7d3df ==== */
struct lc_time_T
{
	const char *mon[MONSPERYEAR];
	const char *month[MONSPERYEAR];
	const char *wday[DAYSPERWEEK];
	const char *weekday[DAYSPERWEEK];
	const char *X_fmt;
	const char *x_fmt;
	const char *c_fmt;
	const char *am;
	const char *pm;
	const char *date_fmt;
};

#define Locale	(&C_time_locale)

static const struct lc_time_T C_time_locale = {
	{
		"Jan", "Feb", "Mar", "Apr", "May", "Jun",
		"Jul", "Aug", "Sep", "Oct", "Nov", "Dec"
	}, {
		"January", "February", "March", "April", "May", "June",
		"July", "August", "September", "October", "November", "December"
	}, {
		"Sun", "Mon", "Tue", "Wed",
		"Thu", "Fri", "Sat"
	}, {
		"Sunday", "Monday", "Tuesday", "Wednesday",
		"Thursday", "Friday", "Saturday"
	},

	/* X_fmt */
	"%H:%M:%S",

	/*
	 * x_fmt
	 *
	 * C99 and later require this format. Using just numbers (as here) makes
	 * Quakers happier; it's also compatible with SVR4.
	 */
	"%m/%d/%y",

	/*
	 * c_fmt
	 *
	 * C99 and later require this format. Previously this code used "%D %X",
	 * but we now conform to C99. Note that "%a %b %d %H:%M:%S %Y" is used by
	 * Solaris 2.3.
	 */
	"%a %b %e %T %Y",

	/* am */
	"AM",

	/* pm */
	"PM",

	/* date_fmt */
	"%a %b %e %H:%M:%S %Z %Y"
};

enum warn
{
	IN_NONE, IN_SOME, IN_THIS, IN_ALL
};

static char *_add(const char *str, char *pt, const char *ptlim);
static char *_conv(int n, const char *format, char *pt, const char *ptlim);
static char *_fmt(const char *format, const struct pg_tm *t, char *pt, const char *ptlim,
				  enum warn *warnp);
static char *_yconv(int a, int b, bool convert_top, bool convert_yy, char *pt, char const *ptlim);


/*
 * Convert timestamp t to string s, a caller-allocated buffer of size maxsize,
 * using the given format pattern.
 *
 * See also timestamptz_to_str.
 */
size_t
pg_strftime(char *s, size_t maxsize, const char *format, const struct pg_tm *t)
{
	char	   *p;
	int			saved_errno = errno;
	enum warn	warn = IN_NONE;

	p = _fmt(format, t, s, s + maxsize, &warn);
	if (!p)
	{
		errno = EOVERFLOW;
		return 0;
	}
	if (p == s + maxsize)
	{
		errno = ERANGE;
		return 0;
	}
	*p = '\0';
	errno = saved_errno;
	return p - s;
}

static char *
_fmt(const char *format, const struct pg_tm *t, char *pt,
	 const char *ptlim, enum warn *warnp)
{
	for (; *format; ++format)
	{
		if (*format == '%')
		{
	label:
			switch (*++format)
			{
				case '\0':
					--format;
					break;
				case 'A':
					pt = _add((t->tm_wday < 0 ||
							   t->tm_wday >= DAYSPERWEEK) ?
							  "?" : Locale->weekday[t->tm_wday],
							  pt, ptlim);
					continue;
				case 'a':
					pt = _add((t->tm_wday < 0 ||
							   t->tm_wday >= DAYSPERWEEK) ?
							  "?" : Locale->wday[t->tm_wday],
							  pt, ptlim);
					continue;
				case 'B':
					pt = _add((t->tm_mon < 0 ||
							   t->tm_mon >= MONSPERYEAR) ?
							  "?" : Locale->month[t->tm_mon],
							  pt, ptlim);
					continue;
				case 'b':
				case 'h':
					pt = _add((t->tm_mon < 0 ||
							   t->tm_mon >= MONSPERYEAR) ?
							  "?" : Locale->mon[t->tm_mon],
							  pt, ptlim);
					continue;
				case 'C':

					/*
					 * %C used to do a... _fmt("%a %b %e %X %Y", t);
					 * ...whereas now POSIX 1003.2 calls for something
					 * completely different. (ado, 1993-05-24)
					 */
					pt = _yconv(t->tm_year, TM_YEAR_BASE,
								true, false, pt, ptlim);
					continue;
				case 'c':
					{
						enum warn	warn2 = IN_SOME;

						pt = _fmt(Locale->c_fmt, t, pt, ptlim, &warn2);
						if (warn2 == IN_ALL)
							warn2 = IN_THIS;
						if (warn2 > *warnp)
							*warnp = warn2;
					}
					continue;
				case 'D':
					pt = _fmt("%m/%d/%y", t, pt, ptlim, warnp);
					continue;
				case 'd':
					pt = _conv(t->tm_mday, "%02d", pt, ptlim);
					continue;
				case 'E':
				case 'O':

					/*
					 * Locale modifiers of C99 and later. The sequences %Ec
					 * %EC %Ex %EX %Ey %EY %Od %oe %OH %OI %Om %OM %OS %Ou %OU
					 * %OV %Ow %OW %Oy are supposed to provide alternative
					 * representations.
					 */
					goto label;
				case 'e':
					pt = _conv(t->tm_mday, "%2d", pt, ptlim);
					continue;
				case 'F':
					pt = _fmt("%Y-%m-%d", t, pt, ptlim, warnp);
					continue;
				case 'H':
					pt = _conv(t->tm_hour, "%02d", pt, ptlim);
					continue;
				case 'I':
					pt = _conv((t->tm_hour % 12) ?
							   (t->tm_hour % 12) : 12,
							   "%02d", pt, ptlim);
					continue;
				case 'j':
					pt = _conv(t->tm_yday + 1, "%03d", pt, ptlim);
					continue;
				case 'k':

					/*
					 * This used to be... _conv(t->tm_hour % 12 ? t->tm_hour %
					 * 12 : 12, 2, ' '); ...and has been changed to the below
					 * to match SunOS 4.1.1 and Arnold Robbins' strftime
					 * version 3.0. That is, "%k" and "%l" have been swapped.
					 * (ado, 1993-05-24)
					 */
					pt = _conv(t->tm_hour, "%2d", pt, ptlim);
					continue;
#ifdef KITCHEN_SINK
				case 'K':

					/*
					 * After all this time, still unclaimed!
					 */
					pt = _add("kitchen sink", pt, ptlim);
					continue;
#endif							/* defined KITCHEN_SINK */
				case 'l':

					/*
					 * This used to be... _conv(t->tm_hour, 2, ' '); ...and
					 * has been changed to the below to match SunOS 4.1.1 and
					 * Arnold Robbin's strftime version 3.0. That is, "%k" and
					 * "%l" have been swapped. (ado, 1993-05-24)
					 */
					pt = _conv((t->tm_hour % 12) ?
							   (t->tm_hour % 12) : 12,
							   "%2d", pt, ptlim);
					continue;
				case 'M':
					pt = _conv(t->tm_min, "%02d", pt, ptlim);
					continue;
				case 'm':
					pt = _conv(t->tm_mon + 1, "%02d", pt, ptlim);
					continue;
				case 'n':
					pt = _add("\n", pt, ptlim);
					continue;
				case 'p':
					pt = _add((t->tm_hour >= (HOURSPERDAY / 2)) ?
							  Locale->pm :
							  Locale->am,
							  pt, ptlim);
					continue;
				case 'R':
					pt = _fmt("%H:%M", t, pt, ptlim, warnp);
					continue;
				case 'r':
					pt = _fmt("%I:%M:%S %p", t, pt, ptlim, warnp);
					continue;
				case 'S':
					pt = _conv(t->tm_sec, "%02d", pt, ptlim);
					continue;
				case 'T':
					pt = _fmt("%H:%M:%S", t, pt, ptlim, warnp);
					continue;
				case 't':
					pt = _add("\t", pt, ptlim);
					continue;
				case 'U':
					pt = _conv((t->tm_yday + DAYSPERWEEK -
								t->tm_wday) / DAYSPERWEEK,
							   "%02d", pt, ptlim);
					continue;
				case 'u':

					/*
					 * From Arnold Robbins' strftime version 3.0: "ISO 8601:
					 * Weekday as a decimal number [1 (Monday) - 7]" (ado,
					 * 1993-05-24)
					 */
					pt = _conv((t->tm_wday == 0) ?
							   DAYSPERWEEK : t->tm_wday,
							   "%d", pt, ptlim);
					continue;
				case 'V':		/* ISO 8601 week number */
				case 'G':		/* ISO 8601 year (four digits) */
				case 'g':		/* ISO 8601 year (two digits) */
/*
 * From Arnold Robbins' strftime version 3.0: "the week number of the
 * year (the first Monday as the first day of week 1) as a decimal number
 * (01-53)."
 * (ado, 1993-05-24)
 *
 * From <https://www.cl.cam.ac.uk/~mgk25/iso-time.html> by Markus Kuhn:
 * "Week 01 of a year is per definition the first week which has the
 * Thursday in this year, which is equivalent to the week which contains
 * the fourth day of January. In other words, the first week of a new year
 * is the week which has the majority of its days in the new year. Week 01
 * might also contain days from the previous year and the week before week
 * 01 of a year is the last week (52 or 53) of the previous year even if
 * it contains days from the new year. A week starts with Monday (day 1)
 * and ends with Sunday (day 7). For example, the first week of the year
 * 1997 lasts from 1996-12-30 to 1997-01-05..."
 * (ado, 1996-01-02)
 */
					{
						int			year;
						int			base;
						int			yday;
						int			wday;
						int			w;

						year = t->tm_year;
						base = TM_YEAR_BASE;
						yday = t->tm_yday;
						wday = t->tm_wday;
						for (;;)
						{
							int			len;
							int			bot;
							int			top;

							len = isleap_sum(year, base) ?
								DAYSPERLYEAR :
								DAYSPERNYEAR;

							/*
							 * What yday (-3 ... 3) does the ISO year begin
							 * on?
							 */
							bot = ((yday + 11 - wday) %
								   DAYSPERWEEK) - 3;

							/*
							 * What yday does the NEXT ISO year begin on?
							 */
							top = bot -
								(len % DAYSPERWEEK);
							if (top < -3)
								top += DAYSPERWEEK;
							top += len;
							if (yday >= top)
							{
								++base;
								w = 1;
								break;
							}
							if (yday >= bot)
							{
								w = 1 + ((yday - bot) /
										 DAYSPERWEEK);
								break;
							}
							--base;
							yday += isleap_sum(year, base) ?
								DAYSPERLYEAR :
								DAYSPERNYEAR;
						}
						if (*format == 'V')
							pt = _conv(w, "%02d",
									   pt, ptlim);
						else if (*format == 'g')
						{
							*warnp = IN_ALL;
							pt = _yconv(year, base,
										false, true,
										pt, ptlim);
						}
						else
							pt = _yconv(year, base,
										true, true,
										pt, ptlim);
					}
					continue;
				case 'v':

					/*
					 * From Arnold Robbins' strftime version 3.0: "date as
					 * dd-bbb-YYYY" (ado, 1993-05-24)
					 */
					pt = _fmt("%e-%b-%Y", t, pt, ptlim, warnp);
					continue;
				case 'W':
					pt = _conv((t->tm_yday + DAYSPERWEEK -
								(t->tm_wday ?
								 (t->tm_wday - 1) :
								 (DAYSPERWEEK - 1))) / DAYSPERWEEK,
							   "%02d", pt, ptlim);
					continue;
				case 'w':
					pt = _conv(t->tm_wday, "%d", pt, ptlim);
					continue;
				case 'X':
					pt = _fmt(Locale->X_fmt, t, pt, ptlim, warnp);
					continue;
				case 'x':
					{
						enum warn	warn2 = IN_SOME;

						pt = _fmt(Locale->x_fmt, t, pt, ptlim, &warn2);
						if (warn2 == IN_ALL)
							warn2 = IN_THIS;
						if (warn2 > *warnp)
							*warnp = warn2;
					}
					continue;
				case 'y':
					*warnp = IN_ALL;
					pt = _yconv(t->tm_year, TM_YEAR_BASE,
								false, true,
								pt, ptlim);
					continue;
				case 'Y':
					pt = _yconv(t->tm_year, TM_YEAR_BASE,
								true, true,
								pt, ptlim);
					continue;
				case 'Z':
					if (t->tm_zone != NULL)
						pt = _add(t->tm_zone, pt, ptlim);

					/*
					 * C99 and later say that %Z must be replaced by the empty
					 * string if the time zone abbreviation is not
					 * determinable.
					 */
					continue;
				case 'z':
					{
						long		diff;
						char const *sign;
						bool		negative;

						if (t->tm_isdst < 0)
							continue;
						diff = t->tm_gmtoff;
						negative = diff < 0;
						if (diff == 0)
						{
							if (t->tm_zone != NULL)
								negative = t->tm_zone[0] == '-';
						}
						if (negative)
						{
							sign = "-";
							diff = -diff;
						}
						else
							sign = "+";
						pt = _add(sign, pt, ptlim);
						diff /= SECSPERMIN;
						diff = (diff / MINSPERHOUR) * 100 +
							(diff % MINSPERHOUR);
						pt = _conv(diff, "%04d", pt, ptlim);
					}
					continue;
				case '+':
					pt = _fmt(Locale->date_fmt, t, pt, ptlim,
							  warnp);
					continue;
				case '%':

					/*
					 * X311J/88-090 (4.12.3.5): if conversion char is
					 * undefined, behavior is undefined. Print out the
					 * character itself as printf(3) also does.
					 */
				default:
					break;
			}
		}
		if (pt == ptlim)
			break;
		*pt++ = *format;
	}
	return pt;
}

static char *
_conv(int n, const char *format, char *pt, const char *ptlim)
{
	char		buf[INT_STRLEN_MAXIMUM(int) + 1];

	sprintf(buf, format, n);
	return _add(buf, pt, ptlim);
}

static char *
_add(const char *str, char *pt, const char *ptlim)
{
	while (pt < ptlim && (*pt = *str++) != '\0')
		++pt;
	return pt;
}

/*
 * POSIX and the C Standard are unclear or inconsistent about
 * what %C and %y do if the year is negative or exceeds 9999.
 * Use the convention that %C concatenated with %y yields the
 * same output as %Y, and that %Y contains at least 4 bytes,
 * with more only if necessary.
 */

static char *
_yconv(int a, int b, bool convert_top, bool convert_yy,
	   char *pt, const char *ptlim)
{
	int			lead;
	int			trail;

#define DIVISOR	100
	trail = a % DIVISOR + b % DIVISOR;
	lead = a / DIVISOR + b / DIVISOR + trail / DIVISOR;
	trail %= DIVISOR;
	if (trail < 0 && lead > 0)
	{
		trail += DIVISOR;
		--lead;
	}
	else if (lead < 0 && trail > 0)
	{
		trail -= DIVISOR;
		++lead;
	}
	if (convert_top)
	{
		if (lead == 0 && trail < 0)
			pt = _add("-0", pt, ptlim);
		else
			pt = _conv(lead, "%02d", pt, ptlim);
	}
	if (convert_yy)
		pt = _conv(((trail < 0) ? -trail : trail), "%02d", pt, ptlim);
	return pt;
}

/* ==================== tzparser section ==================== */

/* ==== VERBATIM: tzEntry (tzparser.h lines 23-34 @ 62d6c7d3df) ==== */
typedef struct tzEntry
{
	/* the actual data */
	char	   *abbrev;			/* TZ abbreviation (downcased) */
	char	   *zone;			/* zone name if dynamic abbrev, else NULL */
	/* for a dynamic abbreviation, offset/is_dst are not used */
	int			offset;			/* offset in seconds from UTC */
	bool		is_dst;			/* true if a DST abbreviation */
	/* source information (for error messages) */
	int			lineno;
	const char *filename;
} tzEntry;

/* ==== VERBATIM: datetime.h 95-97, 204, 206-229 @ 62d6c7d3df ==== */
typedef struct pg_tz pg_tz;	/* opaque here, as in pgtime.h */
#define TZ		5				/* fixed-offset timezone abbreviation */
#define DTZ		6				/* fixed-offset timezone abbrev, DST */
#define DYNTZ	7				/* dynamic timezone abbreviation */
#define TOKMAXLEN		10
/* keep this struct small; it gets used a lot */
typedef struct
{
	char		token[TOKMAXLEN + 1];	/* always NUL-terminated */
	char		type;			/* see field type codes above */
	int32		value;			/* meaning depends on type */
} datetkn;

/* one of its uses is in tables of time zone abbreviations */
typedef struct TimeZoneAbbrevTable
{
	Size		tblsize;		/* size in bytes of TimeZoneAbbrevTable */
	int			numabbrevs;		/* number of entries in abbrevs[] array */
	datetkn		abbrevs[FLEXIBLE_ARRAY_MEMBER];
	/* DynamicZoneAbbrev(s) may follow the abbrevs[] array */
} TimeZoneAbbrevTable;

/* auxiliary data for a dynamic time zone abbreviation (non-fixed-offset) */
typedef struct DynamicZoneAbbrev
{
	pg_tz	   *tz;				/* NULL if not yet looked up */
	char		zone[FLEXIBLE_ARRAY_MEMBER];	/* NUL-terminated zone name */
} DynamicZoneAbbrev;


/* ==== VERBATIM: SECS_PER_HOUR (datatype/timestamp.h line 127) ==== */
#define SECS_PER_HOUR	3600

/* ---- shims for tzparser.c / datetime.c (see file header) ---- */
static void **tzf_allocs;
static size_t tzf_nallocs, tzf_aallocs;

static void *
tzf_track(void *p)
{
	if (tzf_nallocs == tzf_aallocs)
	{
		tzf_aallocs = tzf_aallocs ? tzf_aallocs * 2 : 256;
		tzf_allocs = realloc(tzf_allocs, tzf_aallocs * sizeof(void *));
	}
	tzf_allocs[tzf_nallocs++] = p;
	return p;
}

static void *
tzf_palloc(Size sz)
{
	return tzf_track(malloc(sz));
}

static char *
tzf_pstrdup(const char *s)
{
	return tzf_track(strdup(s));
}

static void *
tzf_repalloc(void *p, Size sz)
{
	for (size_t i = tzf_nallocs; i-- > 0;)
	{
		if (tzf_allocs[i] == p)
		{
			tzf_allocs[i] = realloc(p, sz);
			return tzf_allocs[i];
		}
	}
	abort();				/* repalloc of an untracked pointer */
}

#define palloc	tzf_palloc
#define pstrdup tzf_pstrdup
#define repalloc tzf_repalloc
#define guc_malloc(elevel, sz) tzf_palloc(sz)

/* GUC check-hook error channel capture (guc.h protocol; %m as elog.c) */
static char tzf_guc_msg[4096];
static char tzf_guc_detail[4096];
static char tzf_guc_hint[4096];
static int	tzf_guc_msg_set, tzf_guc_detail_set, tzf_guc_hint_set;

static void
tzf_guc_capture(char *slot, size_t slotsz, const char *fmt, va_list ap)
{
	char		expanded[1024];
	const char *m = strstr(fmt, "%m");

	if (m != NULL)
	{
		snprintf(expanded, sizeof(expanded), "%.*s%s%s",
				 (int) (m - fmt), fmt, strerror(errno), m + 2);
		fmt = expanded;
	}
	vsnprintf(slot, slotsz, fmt, ap);
}

static void
tzf_GUC_check_errmsg(const char *fmt,...)
{
	va_list		ap;

	va_start(ap, fmt);
	tzf_guc_capture(tzf_guc_msg, sizeof(tzf_guc_msg), fmt, ap);
	va_end(ap);
	tzf_guc_msg_set = 1;
}

static void
tzf_GUC_check_errdetail(const char *fmt,...)
{
	va_list		ap;

	va_start(ap, fmt);
	tzf_guc_capture(tzf_guc_detail, sizeof(tzf_guc_detail), fmt, ap);
	va_end(ap);
	tzf_guc_detail_set = 1;
}

static void
tzf_GUC_check_errhint(const char *fmt,...)
{
	va_list		ap;

	va_start(ap, fmt);
	tzf_guc_capture(tzf_guc_hint, sizeof(tzf_guc_hint), fmt, ap);
	va_end(ap);
	tzf_guc_hint_set = 1;
}

#define GUC_check_errmsg	tzf_GUC_check_errmsg
#define GUC_check_errdetail tzf_GUC_check_errdetail
#define GUC_check_errhint	tzf_GUC_check_errhint

/* fd.c wrappers -> plain stdio/dirent (resource-owner plumbing) */
#define AllocateFile(name, mode) fopen((name), (mode))
#define FreeFile(f) fclose(f)
#define AllocateDir(name) opendir(name)
#define FreeDir(d) closedir(d)

/* miscadmin/port path resolution: environment mock (see header) */
static char tzf_my_exec_path[MAXPGPATH] = "pg_tzfam_io-fuzz-oracle";

static void
tzf_get_share_path(const char *exec_path, char *out)
{
	const char *share = getenv("PGRUST_PGSHAREDIR");

	(void) exec_path;
	snprintf(out, MAXPGPATH, "%s", share ? share : "/nonexistent");
}

#define get_share_path(a, b) tzf_get_share_path((a), (b))

/* MemoryContext dance in load_tzoffsets: the temp context is modeled by
 * the tzf_track arena + pg_tzf_reset (environment, not logic) */
typedef void *MemoryContext;
#define AllocSetContextCreate(parent, name, sizes) ((MemoryContext) 0)
#define ALLOCSET_SMALL_SIZES 0
#define CurrentMemoryContext ((MemoryContext) 0)
#define MemoryContextSwitchTo(cxt) ((MemoryContext) 0)
#define MemoryContextDelete(cxt) ((void) 0)
#define LOG 15

extern unsigned char tzf_pg_tolower(unsigned char ch);
extern int	tzf_pg_strcasecmp(const char *s1, const char *s2);
extern int	tzf_pg_strncasecmp(const char *s1, const char *s2, size_t n);
extern size_t tzf_strlcpy(char *dst, const char *src, size_t siz);
extern TimeZoneAbbrevTable *tzf_ConvertTimeZoneAbbrevs(struct tzEntry *abbrevs, int n);

/* ==== VERBATIM: tzparser.c lines 35-487 @ 62d6c7d3df ==== */
#define WHITESPACE " \t\n\r"

static bool validateTzEntry(tzEntry *tzentry);
static bool splitTzLine(const char *filename, int lineno,
						char *line, tzEntry *tzentry);
static int	addToArray(tzEntry **base, int *arraysize, int n,
					   tzEntry *entry, bool override);
static int	ParseTzFile(const char *filename, int depth,
						tzEntry **base, int *arraysize, int n);


/*
 * Apply additional validation checks to a tzEntry
 *
 * Returns true if OK, else false
 */
static bool
validateTzEntry(tzEntry *tzentry)
{
	unsigned char *p;

	/*
	 * Check restrictions imposed by datetktbl storage format (see datetime.c)
	 */
	if (strlen(tzentry->abbrev) > TOKMAXLEN)
	{
		GUC_check_errmsg("time zone abbreviation \"%s\" is too long (maximum %d characters) in time zone file \"%s\", line %d",
						 tzentry->abbrev, TOKMAXLEN,
						 tzentry->filename, tzentry->lineno);
		return false;
	}

	/*
	 * Sanity-check the offset: shouldn't exceed 14 hours
	 */
	if (tzentry->offset > 14 * SECS_PER_HOUR ||
		tzentry->offset < -14 * SECS_PER_HOUR)
	{
		GUC_check_errmsg("time zone offset %d is out of range in time zone file \"%s\", line %d",
						 tzentry->offset,
						 tzentry->filename, tzentry->lineno);
		return false;
	}

	/*
	 * Convert abbrev to lowercase (must match datetime.c's conversion)
	 */
	for (p = (unsigned char *) tzentry->abbrev; *p; p++)
		*p = pg_tolower(*p);

	return true;
}

/*
 * Attempt to parse the line as a timezone abbrev spec
 *
 * Valid formats are:
 *	name  zone
 *	name  offset  dst
 *
 * Returns true if OK, else false; data is stored in *tzentry
 */
static bool
splitTzLine(const char *filename, int lineno, char *line, tzEntry *tzentry)
{
	char	   *brkl;
	char	   *abbrev;
	char	   *offset;
	char	   *offset_endptr;
	char	   *remain;
	char	   *is_dst;

	tzentry->lineno = lineno;
	tzentry->filename = filename;

	abbrev = strtok_r(line, WHITESPACE, &brkl);
	if (!abbrev)
	{
		GUC_check_errmsg("missing time zone abbreviation in time zone file \"%s\", line %d",
						 filename, lineno);
		return false;
	}
	tzentry->abbrev = pstrdup(abbrev);

	offset = strtok_r(NULL, WHITESPACE, &brkl);
	if (!offset)
	{
		GUC_check_errmsg("missing time zone offset in time zone file \"%s\", line %d",
						 filename, lineno);
		return false;
	}

	/* We assume zone names don't begin with a digit or sign */
	if (isdigit((unsigned char) *offset) || *offset == '+' || *offset == '-')
	{
		tzentry->zone = NULL;
		tzentry->offset = strtol(offset, &offset_endptr, 10);
		if (offset_endptr == offset || *offset_endptr != '\0')
		{
			GUC_check_errmsg("invalid number for time zone offset in time zone file \"%s\", line %d",
							 filename, lineno);
			return false;
		}

		is_dst = strtok_r(NULL, WHITESPACE, &brkl);
		if (is_dst && pg_strcasecmp(is_dst, "D") == 0)
		{
			tzentry->is_dst = true;
			remain = strtok_r(NULL, WHITESPACE, &brkl);
		}
		else
		{
			/* there was no 'D' dst specifier */
			tzentry->is_dst = false;
			remain = is_dst;
		}
	}
	else
	{
		/*
		 * Assume entry is a zone name.  We do not try to validate it by
		 * looking up the zone, because that would force loading of a lot of
		 * zones that probably will never be used in the current session.
		 */
		tzentry->zone = pstrdup(offset);
		tzentry->offset = 0 * SECS_PER_HOUR;
		tzentry->is_dst = false;
		remain = strtok_r(NULL, WHITESPACE, &brkl);
	}

	if (!remain)				/* no more non-whitespace chars */
		return true;

	if (remain[0] != '#')		/* must be a comment */
	{
		GUC_check_errmsg("invalid syntax in time zone file \"%s\", line %d",
						 filename, lineno);
		return false;
	}
	return true;
}

/*
 * Insert entry into sorted array
 *
 * *base: base address of array (changeable if must enlarge array)
 * *arraysize: allocated length of array (changeable if must enlarge array)
 * n: current number of valid elements in array
 * entry: new data to insert
 * override: true if OK to override
 *
 * Returns the new array length (new value for n), or -1 if error
 */
static int
addToArray(tzEntry **base, int *arraysize, int n,
		   tzEntry *entry, bool override)
{
	tzEntry    *arrayptr;
	int			low;
	int			high;

	/*
	 * Search the array for a duplicate; as a useful side effect, the array is
	 * maintained in sorted order.  We use strcmp() to ensure we match the
	 * sort order datetime.c expects.
	 */
	arrayptr = *base;
	low = 0;
	high = n - 1;
	while (low <= high)
	{
		int			mid = (low + high) >> 1;
		tzEntry    *midptr = arrayptr + mid;
		int			cmp;

		cmp = strcmp(entry->abbrev, midptr->abbrev);
		if (cmp < 0)
			high = mid - 1;
		else if (cmp > 0)
			low = mid + 1;
		else
		{
			/*
			 * Found a duplicate entry; complain unless it's the same.
			 */
			if ((midptr->zone == NULL && entry->zone == NULL &&
				 midptr->offset == entry->offset &&
				 midptr->is_dst == entry->is_dst) ||
				(midptr->zone != NULL && entry->zone != NULL &&
				 strcmp(midptr->zone, entry->zone) == 0))
			{
				/* return unchanged array */
				return n;
			}
			if (override)
			{
				/* same abbrev but something is different, override */
				midptr->zone = entry->zone;
				midptr->offset = entry->offset;
				midptr->is_dst = entry->is_dst;
				return n;
			}
			/* same abbrev but something is different, complain */
			GUC_check_errmsg("time zone abbreviation \"%s\" is multiply defined",
							 entry->abbrev);
			GUC_check_errdetail("Entry in time zone file \"%s\", line %d, conflicts with entry in file \"%s\", line %d.",
								midptr->filename, midptr->lineno,
								entry->filename, entry->lineno);
			return -1;
		}
	}

	/*
	 * No match, insert at position "low".
	 */
	if (n >= *arraysize)
	{
		*arraysize *= 2;
		*base = (tzEntry *) repalloc(*base, *arraysize * sizeof(tzEntry));
	}

	arrayptr = *base + low;

	memmove(arrayptr + 1, arrayptr, (n - low) * sizeof(tzEntry));

	memcpy(arrayptr, entry, sizeof(tzEntry));

	return n + 1;
}

/*
 * Parse a single timezone abbrev file --- can recurse to handle @INCLUDE
 *
 * filename: user-specified file name (does not include path)
 * depth: current recursion depth
 * *base: array for results (changeable if must enlarge array)
 * *arraysize: allocated length of array (changeable if must enlarge array)
 * n: current number of valid elements in array
 *
 * Returns the new array length (new value for n), or -1 if error
 */
static int
ParseTzFile(const char *filename, int depth,
			tzEntry **base, int *arraysize, int n)
{
	char		share_path[MAXPGPATH];
	char		file_path[MAXPGPATH];
	FILE	   *tzFile;
	char		tzbuf[1024];
	char	   *line;
	tzEntry		tzentry;
	int			lineno = 0;
	bool		override = false;
	const char *p;

	/*
	 * We enforce that the filename is all alpha characters.  This may be
	 * overly restrictive, but we don't want to allow access to anything
	 * outside the timezonesets directory, so for instance '/' *must* be
	 * rejected.
	 */
	for (p = filename; *p; p++)
	{
		if (!isalpha((unsigned char) *p))
		{
			/* at level 0, just use guc.c's regular "invalid value" message */
			if (depth > 0)
				GUC_check_errmsg("invalid time zone file name \"%s\"",
								 filename);
			return -1;
		}
	}

	/*
	 * The maximal recursion depth is a pretty arbitrary setting. It is hard
	 * to imagine that someone needs more than 3 levels so stick with this
	 * conservative setting until someone complains.
	 */
	if (depth > 3)
	{
		GUC_check_errmsg("time zone file recursion limit exceeded in file \"%s\"",
						 filename);
		return -1;
	}

	get_share_path(my_exec_path, share_path);
	snprintf(file_path, sizeof(file_path), "%s/timezonesets/%s",
			 share_path, filename);
	tzFile = AllocateFile(file_path, "r");
	if (!tzFile)
	{
		/*
		 * Check to see if the problem is not the filename but the directory.
		 * This is worth troubling over because if the installation share/
		 * directory is missing or unreadable, this is likely to be the first
		 * place we notice a problem during postmaster startup.
		 */
		int			save_errno = errno;
		DIR		   *tzdir;

		snprintf(file_path, sizeof(file_path), "%s/timezonesets",
				 share_path);
		tzdir = AllocateDir(file_path);
		if (tzdir == NULL)
		{
			GUC_check_errmsg("could not open directory \"%s\": %m",
							 file_path);
			GUC_check_errhint("This may indicate an incomplete PostgreSQL installation, or that the file \"%s\" has been moved away from its proper location.",
							  my_exec_path);
			return -1;
		}
		FreeDir(tzdir);
		errno = save_errno;

		/*
		 * otherwise, if file doesn't exist and it's level 0, guc.c's
		 * complaint is enough
		 */
		if (errno != ENOENT || depth > 0)
			GUC_check_errmsg("could not read time zone file \"%s\": %m",
							 filename);

		return -1;
	}

	while (!feof(tzFile))
	{
		lineno++;
		if (fgets(tzbuf, sizeof(tzbuf), tzFile) == NULL)
		{
			if (ferror(tzFile))
			{
				GUC_check_errmsg("could not read time zone file \"%s\": %m",
								 filename);
				n = -1;
				break;
			}
			/* else we're at EOF after all */
			break;
		}
		if (strlen(tzbuf) == sizeof(tzbuf) - 1)
		{
			/* the line is too long for tzbuf */
			GUC_check_errmsg("line is too long in time zone file \"%s\", line %d",
							 filename, lineno);
			n = -1;
			break;
		}

		/* skip over whitespace */
		line = tzbuf;
		while (*line && isspace((unsigned char) *line))
			line++;

		if (*line == '\0')		/* empty line */
			continue;
		if (*line == '#')		/* comment line */
			continue;

		if (pg_strncasecmp(line, "@INCLUDE", strlen("@INCLUDE")) == 0)
		{
			/* pstrdup so we can use filename in result data structure */
			char	   *includeFile = pstrdup(line + strlen("@INCLUDE"));
			char	   *brki;

			includeFile = strtok_r(includeFile, WHITESPACE, &brki);
			if (!includeFile || !*includeFile)
			{
				GUC_check_errmsg("@INCLUDE without file name in time zone file \"%s\", line %d",
								 filename, lineno);
				n = -1;
				break;
			}
			n = ParseTzFile(includeFile, depth + 1,
							base, arraysize, n);
			if (n < 0)
				break;
			continue;
		}

		if (pg_strncasecmp(line, "@OVERRIDE", strlen("@OVERRIDE")) == 0)
		{
			override = true;
			continue;
		}

		if (!splitTzLine(filename, lineno, line, &tzentry))
		{
			n = -1;
			break;
		}
		if (!validateTzEntry(&tzentry))
		{
			n = -1;
			break;
		}
		n = addToArray(base, arraysize, n, &tzentry, override);
		if (n < 0)
			break;
	}

	FreeFile(tzFile);

	return n;
}

/*
 * load_tzoffsets --- read and parse the specified timezone offset file
 *
 * On success, return a filled-in TimeZoneAbbrevTable, which must have been
 * guc_malloc'd not palloc'd.  On failure, return NULL, using GUC_check_errmsg
 * and friends to give details of the problem.
 */
TimeZoneAbbrevTable *
load_tzoffsets(const char *filename)
{
	TimeZoneAbbrevTable *result = NULL;
	MemoryContext tmpContext;
	MemoryContext oldContext;
	tzEntry    *array;
	int			arraysize;
	int			n;

	/*
	 * Create a temp memory context to work in.  This makes it easy to clean
	 * up afterwards.
	 */
	tmpContext = AllocSetContextCreate(CurrentMemoryContext,
									   "TZParserMemory",
									   ALLOCSET_SMALL_SIZES);
	oldContext = MemoryContextSwitchTo(tmpContext);

	/* Initialize array at a reasonable size */
	arraysize = 128;
	array = (tzEntry *) palloc(arraysize * sizeof(tzEntry));

	/* Parse the file(s) */
	n = ParseTzFile(filename, 0, &array, &arraysize, 0);

	/* If no errors so far, let datetime.c allocate memory & convert format */
	if (n >= 0)
	{
		result = ConvertTimeZoneAbbrevs(array, n);
		if (!result)
			GUC_check_errmsg("out of memory");
	}

	/* Clean up */
	MemoryContextSwitchTo(oldContext);
	MemoryContextDelete(tmpContext);

	return result;
}

/* ==== VERBATIM: ConvertTimeZoneAbbrevs (datetime.c 4986-5071 @ 62d6c7d3df) ==== */
/*
 * This function gets called during timezone config file load or reload
 * to create the final array of timezone tokens.  The argument array
 * is already sorted in name order.
 *
 * The result is a TimeZoneAbbrevTable (which must be a single guc_malloc'd
 * chunk) or NULL on alloc failure.  No other error conditions are defined.
 */
TimeZoneAbbrevTable *
ConvertTimeZoneAbbrevs(struct tzEntry *abbrevs, int n)
{
	TimeZoneAbbrevTable *tbl;
	Size		tbl_size;
	int			i;

	/* Space for fixed fields and datetkn array */
	tbl_size = offsetof(TimeZoneAbbrevTable, abbrevs) +
		n * sizeof(datetkn);
	tbl_size = MAXALIGN(tbl_size);
	/* Count up space for dynamic abbreviations */
	for (i = 0; i < n; i++)
	{
		struct tzEntry *abbr = abbrevs + i;

		if (abbr->zone != NULL)
		{
			Size		dsize;

			dsize = offsetof(DynamicZoneAbbrev, zone) +
				strlen(abbr->zone) + 1;
			tbl_size += MAXALIGN(dsize);
		}
	}

	/* Alloc the result ... */
	tbl = guc_malloc(LOG, tbl_size);
	if (!tbl)
		return NULL;

	/* ... and fill it in */
	tbl->tblsize = tbl_size;
	tbl->numabbrevs = n;
	/* in this loop, tbl_size reprises the space calculation above */
	tbl_size = offsetof(TimeZoneAbbrevTable, abbrevs) +
		n * sizeof(datetkn);
	tbl_size = MAXALIGN(tbl_size);
	for (i = 0; i < n; i++)
	{
		struct tzEntry *abbr = abbrevs + i;
		datetkn    *dtoken = tbl->abbrevs + i;

		/* use strlcpy to truncate name if necessary */
		strlcpy(dtoken->token, abbr->abbrev, TOKMAXLEN + 1);
		if (abbr->zone != NULL)
		{
			/* Allocate a DynamicZoneAbbrev for this abbreviation */
			DynamicZoneAbbrev *dtza;
			Size		dsize;

			dtza = (DynamicZoneAbbrev *) ((char *) tbl + tbl_size);
			dtza->tz = NULL;
			strcpy(dtza->zone, abbr->zone);

			dtoken->type = DYNTZ;
			/* value is offset from table start to DynamicZoneAbbrev */
			dtoken->value = (int32) tbl_size;

			dsize = offsetof(DynamicZoneAbbrev, zone) +
				strlen(abbr->zone) + 1;
			tbl_size += MAXALIGN(dsize);
		}
		else
		{
			dtoken->type = abbr->is_dst ? DTZ : TZ;
			dtoken->value = abbr->offset;
		}
	}

	/* Assert the two loops above agreed on size calculations */
	Assert(tbl->tblsize == tbl_size);

	/* Check the ordering, if testing */
	Assert(CheckDateTokenTable("timezone abbreviations", tbl->abbrevs, n));

	return tbl;
}

/* ==== VERBATIM: pg_strcasecmp (pgstrcasecmp.c 32-62 @ 62d6c7d3df) ==== */
/*
 * Case-independent comparison of two null-terminated strings.
 */
int
pg_strcasecmp(const char *s1, const char *s2)
{
	for (;;)
	{
		unsigned char ch1 = (unsigned char) *s1++;
		unsigned char ch2 = (unsigned char) *s2++;

		if (ch1 != ch2)
		{
			if (ch1 >= 'A' && ch1 <= 'Z')
				ch1 += 'a' - 'A';
			else if (IS_HIGHBIT_SET(ch1) && isupper(ch1))
				ch1 = tolower(ch1);

			if (ch2 >= 'A' && ch2 <= 'Z')
				ch2 += 'a' - 'A';
			else if (IS_HIGHBIT_SET(ch2) && isupper(ch2))
				ch2 = tolower(ch2);

			if (ch1 != ch2)
				return (int) ch1 - (int) ch2;
		}
		if (ch1 == 0)
			break;
	}
	return 0;
}

/* ==== VERBATIM: pg_strncasecmp (pgstrcasecmp.c 64-95 @ 62d6c7d3df) ==== */
/*
 * Case-independent comparison of two not-necessarily-null-terminated strings.
 * At most n bytes will be examined from each string.
 */
int
pg_strncasecmp(const char *s1, const char *s2, size_t n)
{
	while (n-- > 0)
	{
		unsigned char ch1 = (unsigned char) *s1++;
		unsigned char ch2 = (unsigned char) *s2++;

		if (ch1 != ch2)
		{
			if (ch1 >= 'A' && ch1 <= 'Z')
				ch1 += 'a' - 'A';
			else if (IS_HIGHBIT_SET(ch1) && isupper(ch1))
				ch1 = tolower(ch1);

			if (ch2 >= 'A' && ch2 <= 'Z')
				ch2 += 'a' - 'A';
			else if (IS_HIGHBIT_SET(ch2) && isupper(ch2))
				ch2 = tolower(ch2);

			if (ch1 != ch2)
				return (int) ch1 - (int) ch2;
		}
		if (ch1 == 0)
			break;
	}
	return 0;
}

/* ==== VERBATIM: pg_tolower (pgstrcasecmp.c 113-129 @ 62d6c7d3df) ==== */

/*
 * Fold a character to lower case.
 *
 * Unlike some versions of tolower(), this is safe to apply to characters
 * that aren't upper case letters.  Note however that the whole thing is
 * a bit bogus for multibyte character sets.
 */
unsigned char
pg_tolower(unsigned char ch)
{
	if (ch >= 'A' && ch <= 'Z')
		ch += 'a' - 'A';
	else if (IS_HIGHBIT_SET(ch) && isupper(ch))
		ch = tolower(ch);
	return ch;
}

/* ==== VERBATIM: strlcpy (strlcpy.c 38-71 @ 62d6c7d3df; tzf_-prefixed) ==== */
/*
 * Copy src to string dst of size siz.  At most siz-1 characters
 * will be copied.  Always NUL terminates (unless siz == 0).
 * Returns strlen(src); if retval >= siz, truncation occurred.
 * Function creation history:  http://www.gratisoft.us/todd/papers/strlcpy.html
 */
size_t
strlcpy(char *dst, const char *src, size_t siz)
{
	char	   *d = dst;
	const char *s = src;
	size_t		n = siz;

	/* Copy as many bytes as will fit */
	if (n != 0)
	{
		while (--n != 0)
		{
			if ((*d++ = *s++) == '\0')
				break;
		}
	}

	/* Not enough room in dst, add NUL and traverse rest of src */
	if (n == 0)
	{
		if (siz != 0)
			*d = '\0';			/* NUL-terminate dst */
		while (*s++)
			;
	}

	return (s - src - 1);		/* count does not include NUL */
}

/* ==================== ts_locale section ==================== */

/* database_ctype_is_c pinned to the census C-locale arm (see header) */
static const bool tzf_database_ctype_is_c = true;
typedef struct pg_locale_struct *pg_locale_t;

/* char2wchar: locale-dependent wide path — carved out on both sides
 * (unreachable under the database_ctype_is_c pin; abort audits the pin) */
static size_t
tzf_char2wchar(wchar_t *to, size_t tolen, const char *from, size_t fromlen,
			   pg_locale_t locale)
{
	abort();
}

extern int	tzf_t_isalnum_with_len(const char *ptr, int mblen);
extern int	tzf_t_isalnum_cstr(const char *ptr);
extern int	tzf_t_isalnum_unbounded(const char *ptr);
extern int	tzf_t_isalnum(const char *ptr);
extern int	tzf_t_isalpha_with_len(const char *ptr, int mblen);
extern int	tzf_t_isalpha_cstr(const char *ptr);
extern int	tzf_t_isalpha_unbounded(const char *ptr);
extern int	tzf_t_isalpha(const char *ptr);

/* ==== VERBATIM: ts_locale.c lines 23-68 @ 62d6c7d3df ==== */
/*
 * The reason these functions use a 3-wchar_t output buffer, not 2 as you
 * might expect, is that on Windows "wchar_t" is 16 bits and what we'll be
 * getting from char2wchar() is UTF16 not UTF32.  A single input character
 * may therefore produce a surrogate pair rather than just one wchar_t;
 * we also need room for a trailing null.  When we do get a surrogate pair,
 * we pass just the first code to iswdigit() etc, so that these functions will
 * always return false for characters outside the Basic Multilingual Plane.
 */
#define WC_BUF_LEN  3

#define GENERATE_T_ISCLASS_DEF(character_class) \
/* mblen shall be that of the first character */ \
int \
t_is##character_class##_with_len(const char *ptr, int mblen) \
{ \
	int			clen = pg_mblen_with_len(ptr, mblen); \
	wchar_t		character[WC_BUF_LEN]; \
	pg_locale_t mylocale = 0;	/* TODO */ \
	if (clen == 1 || database_ctype_is_c) \
		return is##character_class(TOUCHAR(ptr)); \
	char2wchar(character, WC_BUF_LEN, ptr, clen, mylocale); \
	return isw##character_class((wint_t) character[0]); \
} \
\
/* ptr shall point to a NUL-terminated string */ \
int \
t_is##character_class##_cstr(const char *ptr) \
{ \
	return t_is##character_class##_with_len(ptr, pg_mblen_cstr(ptr)); \
} \
/* ptr shall point to a string with pre-validated encoding */ \
int \
t_is##character_class##_unbounded(const char *ptr) \
{ \
	return t_is##character_class##_with_len(ptr, pg_mblen_unbounded(ptr)); \
} \
/* historical name for _unbounded */ \
int \
t_is##character_class(const char *ptr) \
{ \
	return t_is##character_class##_unbounded(ptr); \
}

GENERATE_T_ISCLASS_DEF(alnum)
GENERATE_T_ISCLASS_DEF(alpha)

/* ==== VERBATIM: t_iseq (ts_locale.h lines 37-38 @ 62d6c7d3df) ==== */
/* The second argument of t_iseq() must be a plain ASCII character */
#define t_iseq(x,c)		(TOUCHAR(x) == (unsigned char) (c))

/* ==================== SECTION D: driver entries ==================== */

/* strftime: verbatim entry over a field-decoded pg_tm; returns the C
 * size_t result, or -1 for the ERANGE (buffer full) verdict */
long long
pg_tzf_strftime(char *s, size_t maxsize, const char *format,
				int tm_sec, int tm_min, int tm_hour, int tm_mday,
				int tm_mon, int tm_year, int tm_wday, int tm_yday,
				int tm_isdst, long tm_gmtoff, const char *tm_zone)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	struct pg_tm t;
	size_t		n;

	t.tm_sec = tm_sec;
	t.tm_min = tm_min;
	t.tm_hour = tm_hour;
	t.tm_mday = tm_mday;
	t.tm_mon = tm_mon;
	t.tm_year = tm_year;
	t.tm_wday = tm_wday;
	t.tm_yday = tm_yday;
	t.tm_isdst = tm_isdst;
	t.tm_gmtoff = tm_gmtoff;
	t.tm_zone = tm_zone;		/* NULL ok */
	errno = 0;
	n = tzf_pg_strftime(s, maxsize, format, &t);
	if (n == 0 && errno == ERANGE)
		return -1;
	return (long long) n;
}

/* tzparser: verbatim load_tzoffsets; result table held until reset */
static TimeZoneAbbrevTable *tzf_last_tbl;

int
pg_tzf_load_tzoffsets(const char *filename)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	tzf_last_tbl = tzf_load_tzoffsets(filename);
	if (tzf_last_tbl == NULL)
		return -1;
	return tzf_last_tbl->numabbrevs;
}

void
pg_tzf_abbrev(int i, char *token_out /* >= TOKMAXLEN+1 */ ,
			  int *type_out, int *value_out)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	const datetkn *tk = tzf_last_tbl->abbrevs + i;

	memcpy(token_out, tk->token, TOKMAXLEN + 1);
	*type_out = (int) tk->type;
	*value_out = tk->value;
}

const char *
pg_tzf_dynzone(int value)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	DynamicZoneAbbrev *dtza = (DynamicZoneAbbrev *) ((char *) tzf_last_tbl + value);

	return dtza->zone;
}

const char *
pg_tzf_guc_msg(void)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	return tzf_guc_msg_set ? tzf_guc_msg : NULL;
}

const char *
pg_tzf_guc_detail(void)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	return tzf_guc_detail_set ? tzf_guc_detail : NULL;
}

const char *
pg_tzf_guc_hint(void)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	return tzf_guc_hint_set ? tzf_guc_hint : NULL;
}

void
pg_tzf_reset(void)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	for (size_t i = 0; i < tzf_nallocs; i++)
		free(tzf_allocs[i]);
	tzf_nallocs = 0;
	tzf_last_tbl = NULL;
	tzf_guc_msg_set = tzf_guc_detail_set = tzf_guc_hint_set = 0;
	tzf_guc_msg[0] = tzf_guc_detail[0] = tzf_guc_hint[0] = '\0';
}

/* ts_locale: verbatim t_is* over a NUL-padded buffer (see driver) */
int
pg_tzf_t_isalpha(const char *ptr)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	return tzf_t_isalpha(ptr);
}

int
pg_tzf_t_isalnum(const char *ptr)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	return tzf_t_isalnum(ptr);
}

int
pg_tzf_t_isalpha_with_len(const char *ptr, int mblen)
{
	return tzf_t_isalpha_with_len(ptr, mblen);
}

int
pg_tzf_t_isalnum_with_len(const char *ptr, int mblen)
{
	return tzf_t_isalnum_with_len(ptr, mblen);
}

int
pg_tzf_t_iseq(const char *x, char c)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	return t_iseq(x, c);
}

int
pg_tzf_isspace_c(int c)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	return isspace(c);
}
