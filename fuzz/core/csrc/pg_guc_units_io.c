/*
 * pg_guc_units_io.c: vendored PostgreSQL C oracle for the guc_units_diff differential
 * fuzz target (100%-coverage campaign; crate crates/backend/utils/misc/guc).
 *
 * GENERATED SKELETON (fuzz/scaffold.py) — NOT yet a valid oracle. Every
 * TODO(scaffold) paste site below must be filled with VERBATIM upstream C,
 * and every #error compile gate removed WITH its paste, before the
 * .file("csrc/pg_guc_units_io.c") line in core/build.rs is uncommented. A
 * half-filled shim can therefore never silently build or link.
 *
 * Provenance (fill in as you paste; follow csrc/pg_uuid_io.c):
 *   - Vendor sections 1..N byte-for-byte from src/backend/utils/adt/guc.c
 *     @ postgres-src 62d6c7d3df6287f1bd83199c1a746e50d31571a0
 *     (PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df); re-verify against the repo's vendored ground-truth
 *     checkout ../pgrust-fabled/vendor/postgres-src before pasting).
 *   - Functions to vendor: parse_int, parse_real, convert_to_base_unit, convert_int_from_base_unit, convert_real_from_base_unit, get_config_unit_name.
 *   - Bodies VERBATIM except documented shims; shims are PLUMBING ONLY
 *     (isxdigit/strtoul C-locale shims, ereturn -> int sentinel, fmgr
 *     PG_FUNCTION_ARGS unwrapped to plain C signatures, palloc'd results ->
 *     caller buffers, wire triples for recv/send), NEVER logic. List every
 *     shim in this header when you paste.
 *   - palloc/palloc0/repalloc/pfree -> the TLS pointer arena below (NOT
 *     bare malloc/free): models PG's memory-context reset; error paths
 *     strand allocations otherwise. Do NOT free() arena pointers by hand.
 *
 * Errcode capture follows csrc/pg_float_io.c: the shared _Thread_local
 * pg_diff_errcode (defined there) records the errcode class; map each
 * errcode this crate's C raises to a small class constant below.
 */

#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

/* Shared TLS errcode channel (defined in csrc/pg_float_io.c). */
extern _Thread_local int pg_diff_errcode;

/* TODO(scaffold): one class constant per distinct errcode the vendored C
 * raises, e.g.:
 *   #define PG_DIFF_ERR_INVALID_TEXT 1   (22P02)
 */

/* palloc arena shim: PostgreSQL frees these via memory-context reset; the
 * oracle mirrors that with a TLS pointer arena reset at every pg_diff_*
 * dispatcher entry, so error-path longjmp/ereturn/goto exits cannot leak.
 * (Three LSan incidents of the naive palloc->malloc mapping on 2026-07-31;
 * pattern proven on proofs/p1-lanej @ 7306d300196 — copied, not re-derived.
 * Final-exec allocations stay rooted in the arena, so LSan's exit scan is
 * quiet without any manual free().) */
#define PG_DIFF_ARENA_MAX 64
static _Thread_local void *pg_diff_arena[PG_DIFF_ARENA_MAX];
static _Thread_local int pg_diff_arena_n;

static void
pg_diff_arena_reset(void)
{
	int			i;

	for (i = 0; i < pg_diff_arena_n; i++)
		free(pg_diff_arena[i]);
	pg_diff_arena_n = 0;
}

static void *
pg_diff_palloc_impl(size_t n)
{
	void	   *p = malloc(n);

	assert(pg_diff_arena_n < PG_DIFF_ARENA_MAX);
	pg_diff_arena[pg_diff_arena_n++] = p;
	return p;
}

static void *
pg_diff_palloc0_impl(size_t n)
{
	void	   *p = calloc(1, n);

	assert(pg_diff_arena_n < PG_DIFF_ARENA_MAX);
	pg_diff_arena[pg_diff_arena_n++] = p;
	return p;
}

static void *
pg_diff_repalloc_impl(void *old, size_t n)
{
	void	   *p = realloc(old, n);
	int			i;

	for (i = 0; i < pg_diff_arena_n; i++)
	{
		if (pg_diff_arena[i] == old)
		{
			pg_diff_arena[i] = p;
			return p;
		}
	}
	assert(!"repalloc of a pointer the arena never issued");
	return p;
}

static void
pg_diff_pfree_impl(void *p)
{
	int			i;

	for (i = 0; i < pg_diff_arena_n; i++)
	{
		if (pg_diff_arena[i] == p)
		{
			free(p);
			pg_diff_arena[i] = pg_diff_arena[--pg_diff_arena_n];
			return;
		}
	}
	/* abort-loud: freeing a pointer the arena never issued is a shim bug
	 * (double-free after reset, or a bare malloc that bypassed palloc). */
	assert(!"pfree of a pointer the arena never issued");
	abort();
}

#define palloc(n) pg_diff_palloc_impl(n)
#define palloc0(n) pg_diff_palloc0_impl(n)
#define repalloc(p, n) pg_diff_repalloc_impl((p), (n))
#define pfree(p) pg_diff_pfree_impl(p)

/* ==================== SECTION 1: shim header (PLUMBING ONLY) ====================
 * Shims: fixed-width typedefs; GUC_UNIT_* constants verbatim from
 * utils/guc.h @ 62d6c7d3df; BLCKSZ/XLOG_BLCKSZ = 8192 (the shipped build's
 * values, compile-time constants in the C build too); gettext_noop = identity;
 * Assert = no-op (matching the release build); elog(ERROR) -> errcode class
 * 90 + longjmp (single reachable site: get_config_unit_name's unrecognized-
 * units default arm, driver-fenced C-parity); strtol/strtod/isspace = LIBC —
 * NOT a shim: the upstream functions call libc directly, that IS the modeled
 * behavior (glibc = oracle of record on the fleet floor).
 * SYMBOL HYGIENE: vendored functions renamed to pg_guco_-prefixed TU-local
 * names via #define (guc.c is a shared upstream file).
 */
#include <stdbool.h>
#include <stddef.h>
#include <math.h>
#include <errno.h>
#include <limits.h>
#include <ctype.h>
#include <stdio.h>
#include <setjmp.h>
typedef int64_t int64;

#define gettext_noop(x) (x)
#define Assert(x) ((void) 0)

/* utils/guc.h @ 62d6c7d3df (VERBATIM values). */
#define GUC_UNIT_KB			 0x01000000 /* value is in kilobytes */
#define GUC_UNIT_BLOCKS		 0x02000000 /* value is in blocks */
#define GUC_UNIT_XBLOCKS	 0x03000000 /* value is in xlog blocks */
#define GUC_UNIT_MB			 0x04000000 /* value is in megabytes */
#define GUC_UNIT_BYTE		 0x05000000 /* value is in bytes */
#define GUC_UNIT_MEMORY		 0x0F000000 /* mask for size-related units */
#define GUC_UNIT_MS			 0x10000000 /* value is in milliseconds */
#define GUC_UNIT_S			 0x20000000 /* value is in seconds */
#define GUC_UNIT_MIN		 0x30000000 /* value is in minutes */
#define GUC_UNIT_TIME		 0x70000000 /* mask for time-related units */
#define GUC_UNIT			 (GUC_UNIT_MEMORY | GUC_UNIT_TIME)

/* pg_config.h values of the shipped build (verified against
 * crates/backend/utils/misc/guc_tables/src/consts.rs). */
#define BLCKSZ 8192
#define XLOG_BLCKSZ 8192

/* error channel: class 90 = elog(ERROR) (unrecognized GUC units value). */
static _Thread_local jmp_buf pg_guco_jmp;
#define PG_GUCO_ERR_ELOG 90
static void pg_guco_elog_error(void)
{ pg_diff_errcode = PG_GUCO_ERR_ELOG; longjmp(pg_guco_jmp, 1); }
#define elog(elevel, ...) pg_guco_elog_error()

/* SYMBOL-HYGIENE renames (linkage names only; bodies stay verbatim). */
#define convert_to_base_unit pg_guco_convert_to_base_unit
#define convert_int_from_base_unit pg_guco_convert_int_from_base_unit
#define convert_real_from_base_unit pg_guco_convert_real_from_base_unit
#define get_config_unit_name pg_guco_get_config_unit_name
#define parse_int pg_guco_parse_int
#define parse_real pg_guco_parse_real

/* ============ VERBATIM: utils/misc/guc.c @ 62d6c7d3df ============ */
/* lines 101-186: MAX_UNIT_LEN, unit_conversion, hints, unit tables. */
#define MAX_UNIT_LEN		3	/* length of longest recognized unit string */

typedef struct
{
	char		unit[MAX_UNIT_LEN + 1]; /* unit, as a string, like "kB" or
										 * "min" */
	int			base_unit;		/* GUC_UNIT_XXX */
	double		multiplier;		/* Factor for converting unit -> base_unit */
} unit_conversion;

/* Ensure that the constants in the tables don't overflow or underflow */
#if BLCKSZ < 1024 || BLCKSZ > (1024*1024)
#error BLCKSZ must be between 1KB and 1MB
#endif
#if XLOG_BLCKSZ < 1024 || XLOG_BLCKSZ > (1024*1024)
#error XLOG_BLCKSZ must be between 1KB and 1MB
#endif

static const char *const memory_units_hint = gettext_noop("Valid units for this parameter are \"B\", \"kB\", \"MB\", \"GB\", and \"TB\".");

static const unit_conversion memory_unit_conversion_table[] =
{
	{"TB", GUC_UNIT_BYTE, 1024.0 * 1024.0 * 1024.0 * 1024.0},
	{"GB", GUC_UNIT_BYTE, 1024.0 * 1024.0 * 1024.0},
	{"MB", GUC_UNIT_BYTE, 1024.0 * 1024.0},
	{"kB", GUC_UNIT_BYTE, 1024.0},
	{"B", GUC_UNIT_BYTE, 1.0},

	{"TB", GUC_UNIT_KB, 1024.0 * 1024.0 * 1024.0},
	{"GB", GUC_UNIT_KB, 1024.0 * 1024.0},
	{"MB", GUC_UNIT_KB, 1024.0},
	{"kB", GUC_UNIT_KB, 1.0},
	{"B", GUC_UNIT_KB, 1.0 / 1024.0},

	{"TB", GUC_UNIT_MB, 1024.0 * 1024.0},
	{"GB", GUC_UNIT_MB, 1024.0},
	{"MB", GUC_UNIT_MB, 1.0},
	{"kB", GUC_UNIT_MB, 1.0 / 1024.0},
	{"B", GUC_UNIT_MB, 1.0 / (1024.0 * 1024.0)},

	{"TB", GUC_UNIT_BLOCKS, (1024.0 * 1024.0 * 1024.0) / (BLCKSZ / 1024)},
	{"GB", GUC_UNIT_BLOCKS, (1024.0 * 1024.0) / (BLCKSZ / 1024)},
	{"MB", GUC_UNIT_BLOCKS, 1024.0 / (BLCKSZ / 1024)},
	{"kB", GUC_UNIT_BLOCKS, 1.0 / (BLCKSZ / 1024)},
	{"B", GUC_UNIT_BLOCKS, 1.0 / BLCKSZ},

	{"TB", GUC_UNIT_XBLOCKS, (1024.0 * 1024.0 * 1024.0) / (XLOG_BLCKSZ / 1024)},
	{"GB", GUC_UNIT_XBLOCKS, (1024.0 * 1024.0) / (XLOG_BLCKSZ / 1024)},
	{"MB", GUC_UNIT_XBLOCKS, 1024.0 / (XLOG_BLCKSZ / 1024)},
	{"kB", GUC_UNIT_XBLOCKS, 1.0 / (XLOG_BLCKSZ / 1024)},
	{"B", GUC_UNIT_XBLOCKS, 1.0 / XLOG_BLCKSZ},

	{""}						/* end of table marker */
};

static const char *const time_units_hint = gettext_noop("Valid units for this parameter are \"us\", \"ms\", \"s\", \"min\", \"h\", and \"d\".");

static const unit_conversion time_unit_conversion_table[] =
{
	{"d", GUC_UNIT_MS, 1000 * 60 * 60 * 24},
	{"h", GUC_UNIT_MS, 1000 * 60 * 60},
	{"min", GUC_UNIT_MS, 1000 * 60},
	{"s", GUC_UNIT_MS, 1000},
	{"ms", GUC_UNIT_MS, 1},
	{"us", GUC_UNIT_MS, 1.0 / 1000},

	{"d", GUC_UNIT_S, 60 * 60 * 24},
	{"h", GUC_UNIT_S, 60 * 60},
	{"min", GUC_UNIT_S, 60},
	{"s", GUC_UNIT_S, 1},
	{"ms", GUC_UNIT_S, 1.0 / 1000},
	{"us", GUC_UNIT_S, 1.0 / (1000 * 1000)},

	{"d", GUC_UNIT_MIN, 60 * 24},
	{"h", GUC_UNIT_MIN, 60},
	{"min", GUC_UNIT_MIN, 1},
	{"s", GUC_UNIT_MIN, 1.0 / 60},
	{"ms", GUC_UNIT_MIN, 1.0 / (1000 * 60)},
	{"us", GUC_UNIT_MIN, 1.0 / (1000 * 1000 * 60)},

	{""}						/* end of table marker */
};

/*
 * To allow continued support of obsolete names for GUC variables, we apply
 * the following mappings to any unrecognized name.  Note that an old name

/* lines 2660-3011: convert_to_base_unit, convert_int_from_base_unit,
 * convert_real_from_base_unit, get_config_unit_name, parse_int, parse_real
 * (one contiguous verbatim region). */
/*
 * Convert a value from one of the human-friendly units ("kB", "min" etc.)
 * to the given base unit.  'value' and 'unit' are the input value and unit
 * to convert from (there can be trailing spaces in the unit string).
 * The converted value is stored in *base_value.
 * It's caller's responsibility to round off the converted value as necessary
 * and check for out-of-range.
 *
 * Returns true on success, false if the input unit is not recognized.
 */
static bool
convert_to_base_unit(double value, const char *unit,
					 int base_unit, double *base_value)
{
	char		unitstr[MAX_UNIT_LEN + 1];
	int			unitlen;
	const unit_conversion *table;
	int			i;

	/* extract unit string to compare to table entries */
	unitlen = 0;
	while (*unit != '\0' && !isspace((unsigned char) *unit) &&
		   unitlen < MAX_UNIT_LEN)
		unitstr[unitlen++] = *(unit++);
	unitstr[unitlen] = '\0';
	/* allow whitespace after unit */
	while (isspace((unsigned char) *unit))
		unit++;
	if (*unit != '\0')
		return false;			/* unit too long, or garbage after it */

	/* now search the appropriate table */
	if (base_unit & GUC_UNIT_MEMORY)
		table = memory_unit_conversion_table;
	else
		table = time_unit_conversion_table;

	for (i = 0; *table[i].unit; i++)
	{
		if (base_unit == table[i].base_unit &&
			strcmp(unitstr, table[i].unit) == 0)
		{
			double		cvalue = value * table[i].multiplier;

			/*
			 * If the user gave a fractional value such as "30.1GB", round it
			 * off to the nearest multiple of the next smaller unit, if there
			 * is one.
			 */
			if (*table[i + 1].unit &&
				base_unit == table[i + 1].base_unit)
				cvalue = rint(cvalue / table[i + 1].multiplier) *
					table[i + 1].multiplier;

			*base_value = cvalue;
			return true;
		}
	}
	return false;
}

/*
 * Convert an integer value in some base unit to a human-friendly unit.
 *
 * The output unit is chosen so that it's the greatest unit that can represent
 * the value without loss.  For example, if the base unit is GUC_UNIT_KB, 1024
 * is converted to 1 MB, but 1025 is represented as 1025 kB.
 */
static void
convert_int_from_base_unit(int64 base_value, int base_unit,
						   int64 *value, const char **unit)
{
	const unit_conversion *table;
	int			i;

	*unit = NULL;

	if (base_unit & GUC_UNIT_MEMORY)
		table = memory_unit_conversion_table;
	else
		table = time_unit_conversion_table;

	for (i = 0; *table[i].unit; i++)
	{
		if (base_unit == table[i].base_unit)
		{
			/*
			 * Accept the first conversion that divides the value evenly.  We
			 * assume that the conversions for each base unit are ordered from
			 * greatest unit to the smallest!
			 */
			if (table[i].multiplier <= 1.0 ||
				base_value % (int64) table[i].multiplier == 0)
			{
				*value = (int64) rint(base_value / table[i].multiplier);
				*unit = table[i].unit;
				break;
			}
		}
	}

	Assert(*unit != NULL);
}

/*
 * Convert a floating-point value in some base unit to a human-friendly unit.
 *
 * Same as above, except we have to do the math a bit differently, and
 * there's a possibility that we don't find any exact divisor.
 */
static void
convert_real_from_base_unit(double base_value, int base_unit,
							double *value, const char **unit)
{
	const unit_conversion *table;
	int			i;

	*unit = NULL;

	if (base_unit & GUC_UNIT_MEMORY)
		table = memory_unit_conversion_table;
	else
		table = time_unit_conversion_table;

	for (i = 0; *table[i].unit; i++)
	{
		if (base_unit == table[i].base_unit)
		{
			/*
			 * Accept the first conversion that divides the value evenly; or
			 * if there is none, use the smallest (last) target unit.
			 *
			 * What we actually care about here is whether snprintf with "%g"
			 * will print the value as an integer, so the obvious test of
			 * "*value == rint(*value)" is too strict; roundoff error might
			 * make us choose an unreasonably small unit.  As a compromise,
			 * accept a divisor that is within 1e-8 of producing an integer.
			 */
			*value = base_value / table[i].multiplier;
			*unit = table[i].unit;
			if (*value > 0 &&
				fabs((rint(*value) / *value) - 1.0) <= 1e-8)
				break;
		}
	}

	Assert(*unit != NULL);
}

/*
 * Return the name of a GUC's base unit (e.g. "ms") given its flags.
 * Return NULL if the GUC is unitless.
 */
const char *
get_config_unit_name(int flags)
{
	switch (flags & GUC_UNIT)
	{
		case 0:
			return NULL;		/* GUC has no units */
		case GUC_UNIT_BYTE:
			return "B";
		case GUC_UNIT_KB:
			return "kB";
		case GUC_UNIT_MB:
			return "MB";
		case GUC_UNIT_BLOCKS:
			{
				static char bbuf[8];

				/* initialize if first time through */
				if (bbuf[0] == '\0')
					snprintf(bbuf, sizeof(bbuf), "%dkB", BLCKSZ / 1024);
				return bbuf;
			}
		case GUC_UNIT_XBLOCKS:
			{
				static char xbuf[8];

				/* initialize if first time through */
				if (xbuf[0] == '\0')
					snprintf(xbuf, sizeof(xbuf), "%dkB", XLOG_BLCKSZ / 1024);
				return xbuf;
			}
		case GUC_UNIT_MS:
			return "ms";
		case GUC_UNIT_S:
			return "s";
		case GUC_UNIT_MIN:
			return "min";
		default:
			elog(ERROR, "unrecognized GUC units value: %d",
				 flags & GUC_UNIT);
			return NULL;
	}
}


/*
 * Try to parse value as an integer.  The accepted formats are the
 * usual decimal, octal, or hexadecimal formats, as well as floating-point
 * formats (which will be rounded to integer after any units conversion).
 * Optionally, the value can be followed by a unit name if "flags" indicates
 * a unit is allowed.
 *
 * If the string parses okay, return true, else false.
 * If okay and result is not NULL, return the value in *result.
 * If not okay and hintmsg is not NULL, *hintmsg is set to a suitable
 * HINT message, or NULL if no hint provided.
 */
bool
parse_int(const char *value, int *result, int flags, const char **hintmsg)
{
	/*
	 * We assume here that double is wide enough to represent any integer
	 * value with adequate precision.
	 */
	double		val;
	char	   *endptr;

	/* To suppress compiler warnings, always set output params */
	if (result)
		*result = 0;
	if (hintmsg)
		*hintmsg = NULL;

	/*
	 * Try to parse as an integer (allowing octal or hex input).  If the
	 * conversion stops at a decimal point or 'e', or overflows, re-parse as
	 * float.  This should work fine as long as we have no unit names starting
	 * with 'e'.  If we ever do, the test could be extended to check for a
	 * sign or digit after 'e', but for now that's unnecessary.
	 */
	errno = 0;
	val = strtol(value, &endptr, 0);
	if (*endptr == '.' || *endptr == 'e' || *endptr == 'E' ||
		errno == ERANGE)
	{
		errno = 0;
		val = strtod(value, &endptr);
	}

	if (endptr == value || errno == ERANGE)
		return false;			/* no HINT for these cases */

	/* reject NaN (infinities will fail range check below) */
	if (isnan(val))
		return false;			/* treat same as syntax error; no HINT */

	/* allow whitespace between number and unit */
	while (isspace((unsigned char) *endptr))
		endptr++;

	/* Handle possible unit */
	if (*endptr != '\0')
	{
		if ((flags & GUC_UNIT) == 0)
			return false;		/* this setting does not accept a unit */

		if (!convert_to_base_unit(val,
								  endptr, (flags & GUC_UNIT),
								  &val))
		{
			/* invalid unit, or garbage after the unit; set hint and fail. */
			if (hintmsg)
			{
				if (flags & GUC_UNIT_MEMORY)
					*hintmsg = memory_units_hint;
				else
					*hintmsg = time_units_hint;
			}
			return false;
		}
	}

	/* Round to int, then check for overflow */
	val = rint(val);

	if (val > INT_MAX || val < INT_MIN)
	{
		if (hintmsg)
			*hintmsg = gettext_noop("Value exceeds integer range.");
		return false;
	}

	if (result)
		*result = (int) val;
	return true;
}

/*
 * Try to parse value as a floating point number in the usual format.
 * Optionally, the value can be followed by a unit name if "flags" indicates
 * a unit is allowed.
 *
 * If the string parses okay, return true, else false.
 * If okay and result is not NULL, return the value in *result.
 * If not okay and hintmsg is not NULL, *hintmsg is set to a suitable
 * HINT message, or NULL if no hint provided.
 */
bool
parse_real(const char *value, double *result, int flags, const char **hintmsg)
{
	double		val;
	char	   *endptr;

	/* To suppress compiler warnings, always set output params */
	if (result)
		*result = 0;
	if (hintmsg)
		*hintmsg = NULL;

	errno = 0;
	val = strtod(value, &endptr);

	if (endptr == value || errno == ERANGE)
		return false;			/* no HINT for these cases */

	/* reject NaN (infinities will fail range checks later) */
	if (isnan(val))
		return false;			/* treat same as syntax error; no HINT */

	/* allow whitespace between number and unit */
	while (isspace((unsigned char) *endptr))
		endptr++;

	/* Handle possible unit */
	if (*endptr != '\0')
	{
		if ((flags & GUC_UNIT) == 0)
			return false;		/* this setting does not accept a unit */

		if (!convert_to_base_unit(val,
								  endptr, (flags & GUC_UNIT),
								  &val))
		{
			/* invalid unit, or garbage after the unit; set hint and fail. */
			if (hintmsg)
			{
				if (flags & GUC_UNIT_MEMORY)
					*hintmsg = memory_units_hint;
				else
					*hintmsg = time_units_hint;
			}
			return false;
		}
	}

	if (result)
		*result = val;
	return true;
}
/* ========== SECTION 2: fuzz-facing driver entries (NOT Postgres code) ===== */

/*
 * Status protocol: parse arms return 1 = parsed ok / 0 = rejected (the C
 * bool), matching the shipped ParseNum verdict; 190 = elog(ERROR) (only the
 * get_config_unit_name arm can raise it). Hint identity classes:
 *   0 = NULL, 1 = memory_units_hint, 2 = time_units_hint,
 *   3 = "Value exceeds integer range."
 */

static int
pg_guco_hint_class(const char *hint)
{
	if (hint == NULL)
		return 0;
	if (hint == memory_units_hint)
		return 1;
	if (hint == time_units_hint)
		return 2;
	return 3;
}

int
pg_diff_guc_parse_int(const char *value, int flags, int *result, int *hint_class)
{
	int			res = 0;
	const char *hint = NULL;
	bool		ok;

	pg_diff_arena_reset();
	pg_diff_errcode = 0;
	ok = parse_int(value, &res, flags, &hint);
	*result = res;
	*hint_class = pg_guco_hint_class(hint);
	return ok ? 1 : 0;
}

int
pg_diff_guc_parse_real(const char *value, int flags, double *result, int *hint_class)
{
	double		res = 0;
	const char *hint = NULL;
	bool		ok;

	pg_diff_arena_reset();
	pg_diff_errcode = 0;
	ok = parse_real(value, &res, flags, &hint);
	*result = res;
	*hint_class = pg_guco_hint_class(hint);
	return ok ? 1 : 0;
}

int
pg_diff_guc_convert_to_base_unit(double value, const char *unit, int base_unit,
								 double *base_value)
{
	pg_diff_arena_reset();
	pg_diff_errcode = 0;
	return convert_to_base_unit(value, unit, base_unit, base_value) ? 1 : 0;
}

/* has_unit out: 0 = C left *unit NULL (no table row matched; C leaves
 * *value unwritten too — the shipped Rust returns (base_value, "")). */
int
pg_diff_guc_convert_int_from_base_unit(int64_t base_value, int base_unit,
									   int64_t *value, char *unit8, int *has_unit)
{
	int64		v = 0;
	const char *unit = NULL;

	pg_diff_arena_reset();
	pg_diff_errcode = 0;
	convert_int_from_base_unit(base_value, base_unit, &v, &unit);
	*has_unit = unit != NULL;
	if (unit != NULL)
	{
		*value = v;
		snprintf(unit8, 8, "%s", unit);
	}
	return 0;
}

int
pg_diff_guc_convert_real_from_base_unit(double base_value, int base_unit,
										double *value, char *unit8, int *has_unit)
{
	double		v = 0;
	const char *unit = NULL;

	pg_diff_arena_reset();
	pg_diff_errcode = 0;
	convert_real_from_base_unit(base_value, base_unit, &v, &unit);
	*has_unit = unit != NULL;
	if (unit != NULL)
	{
		*value = v;
		snprintf(unit8, 8, "%s", unit);
	}
	return 0;
}

/* has_name out: 0 = NULL (unitless). Returns 190 on the elog arm. */
int
pg_diff_guc_get_config_unit_name(int flags, char *name8, int *has_name)
{
	const char *name;

	pg_diff_arena_reset();
	pg_diff_errcode = 0;
	if (setjmp(pg_guco_jmp) != 0)
		return 100 + pg_diff_errcode;
	name = get_config_unit_name(flags);
	*has_name = name != NULL;
	if (name != NULL)
		snprintf(name8, 8, "%s", name);
	return 0;
}

/*
 * fmt plane oracle: PG's snprintf (src/port/snprintf.c fmtfloat @
 * 62d6c7d3df) handles NaN/Infinity ITSELF for platform-independent output
 * ("NaN", "[-]Infinity", sign split off before the isinf check, -0.0
 * detected bytewise) and delegates finite values to the system snprintf
 * ("%.*g" / "%.*e"). This dispatcher replicates exactly those arms
 * (fmtfloat lines 1205-1249) over the two conversions the guc crate's
 * fmt_g_prec/fmt_e model; glibc on the fleet floor is the oracle of record
 * for the finite delegation.
 */
int
pg_diff_guc_fmt(double value, int prec, int style_e, char *out, int outlen)
{
	pg_diff_arena_reset();
	pg_diff_errcode = 0;
	if (isnan(value))
	{
		snprintf(out, outlen, "NaN");
		return 0;
	}
	{
		static const double dzero = 0.0;
		int			neg = (value < 0.0 ||
						   (value == 0.0 &&
							memcmp(&value, &dzero, sizeof(double)) != 0));

		if (neg)
			value = -value;
		if (isinf(value))
		{
			snprintf(out, outlen, "%sInfinity", neg ? "-" : "");
			return 0;
		}
		snprintf(out, outlen, neg ? (style_e ? "-%.*e" : "-%.*g")
				 : (style_e ? "%.*e" : "%.*g"), prec, value);
	}
	return 0;
}
