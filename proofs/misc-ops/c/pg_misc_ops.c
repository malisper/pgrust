/*
 * Vendored PostgreSQL C for Kani dual-execution proofs: misc-ops family
 * (booland/boolor statefuncs, date_pli/date_mii, tidin/tidout,
 *  oidin/oidout, xidin, xid8in, xid8out).
 *
 * PROVENANCE (all fetched 2026-07-28 from
 * raw.githubusercontent.com/postgres/postgres/REL_18_STABLE/):
 *   src/backend/utils/adt/bool.c      — booland_statefunc, boolor_statefunc
 *   src/backend/utils/adt/date.c      — date_pli, date_mii
 *   src/backend/utils/adt/tid.c       — tidin, tidout
 *   src/backend/utils/adt/oid.c       — oidin, oidout
 *   src/backend/utils/adt/xid.c       — xidin, xid8in, xid8out
 *   src/backend/utils/adt/numutils.c  — uint32in_subr, uint64in_subr,
 *                                       pg_ultoa_n, pg_ulltoa_n,
 *                                       decimalLength32/64, DIGIT_TABLE
 *   src/include/datatype/timestamp.h  — POSTGRES_EPOCH_JDATE (2451545),
 *                                       DATETIME_MIN_JULIAN (0),
 *                                       DATE_END_JULIAN (2147483494),
 *                                       IS_VALID_DATE
 *   src/include/utils/date.h          — DATEVAL_NOBEGIN/NOEND (INT32 MIN/MAX),
 *                                       DATE_NOT_FINITE
 *   src/include/port/pg_bitutils.h + src/port/pg_bitutils.c —
 *                                       pg_leftmost_one_pos32/64 (portable
 *                                       table-walk branch) + table
 *
 * SHIMS (plumbing only, never logic) — every departure from upstream:
 *   1. Types: int16/int32/int64/uint8/uint32/uint64 typedef'd here;
 *      unsigned long is modeled as unsigned long long with an explicit
 *      width argument: on BOTH the proof host (macOS aarch64) and the
 *      production targets (LP64 Linux) sizeof(long) == 8, so the
 *      SIZEOF_LONG > 4 / PG_UINT32_MAX != ULONG_MAX narrowing arms of
 *      tidin/uint32in_subr are COMPILED IN, exactly as production builds
 *      them. Constants (ULONGLONG_MAX_ = 2^64-1) spelled literally.
 *   2. fmgr plumbing (PG_FUNCTION_ARGS / PG_GETARG_* / PG_RETURN_*) ->
 *      plain C signatures; bool returns widened to int (Kani lowers Rust
 *      ()/bool poorly against C void/bool through goto-cc).
 *   3. ereport(ERROR)/ereturn(escontext, ...) -> integer error-class
 *      return codes (0 = OK, 1 = invalid syntax / generic error,
 *      2 = out of range), with results through out-params. The errcode
 *      macro arguments in the originals map 1:1:
 *        ERRCODE_INVALID_TEXT_REPRESENTATION -> 1 (sqlstate 22P02)
 *        ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE  -> 2 (sqlstate 22003)
 *        ERRCODE_DATETIME_VALUE_OUT_OF_RANGE -> date_pli/mii's single
 *                                               error class -> 1
 *      Message text leaves the proof (harnesses assert sqlstate class
 *      parity against the Rust side's shipped with_sqlstate calls).
 *   4. libc strtoul(str, &endptr, base) [and strtou64 = strtoull] ->
 *      pg_shim_strtoull: hand-vendored model (pg_lsn precedent, adapted
 *      from base 16 to bases 10 and 0). Modeled semantics, per C99/POSIX
 *      + glibc/macOS behavior:
 *        - skips leading isspace (C locale: ' ', \t, \n, \v, \f, \r);
 *        - optional single '+'/'-'; '-' negates by unsigned wrap;
 *        - base 0: "0x"/"0X" followed by a hex digit => base 16 with the
 *          prefix consumed (a bare "0x" backtracks: the '0' parses as the
 *          whole subject, endptr lands on the 'x' — modeled by the
 *          hex-digit lookahead); else a leading '0' => base 8 (the '0'
 *          itself is the first digit); else base 10;
 *        - digit accumulation per base (three literal-base loop copies so
 *          CBMC never sees a division by a symbolic base; semantics
 *          identical to the single cutoff-check loop of libc);
 *        - overflow: keeps consuming digits, returns ULLONG_MAX and sets
 *          the erange flag (errno = ERANGE);
 *        - no conversion (no digits after prefix handling): returns 0 and
 *          endptr = the ORIGINAL nptr (before whitespace/sign), errno
 *          untouched.
 *      NARROWING ARGUMENT per call site:
 *        - tidin calls strtoul(coord, &badp, 10): base fixed 10; EINVAL
 *          (some systems) is subsumed by tidin's *badp delimiter check
 *          combined with endptr==nptr semantics — with no conversion badp
 *          points at a byte tidin then compares against ','/')', which is
 *          exactly how glibc-built postgres behaves. errno is 0 on entry
 *          (tidin sets errno = 0 before the first call; the second call's
 *          errno check sees 0 unless THAT call overflowed — modeled by a
 *          fresh flag per shim call, matching errno-set-on-ERANGE-only).
 *        - uint32in_subr/uint64in_subr call strtoul/strtoull(s, &endptr,
 *          0): the vendored bodies keep their errno logic verbatim
 *          against the shim's erange flag; the "(errno && errno !=
 *          ERANGE)" arm is dead under the model (the shim only reports
 *          ERANGE), which matches glibc where base is valid.
 *   5. snprintf(buf, n, "%u"/"%lu"/UINT64_FORMAT/"(%u,%u)", ...) in
 *      oidout/xidout/xid8out/tidout -> the verbatim-vendored numutils.c
 *      pg_ultoa_n/pg_ulltoa_n digit emitters + explicit NUL/delimiters.
 *      This is a MODELED shim (proofs/mac %02x-shim style): printf %u
 *      (%llu) produces the minimal unsigned decimal representation, which
 *      is exactly pg_ultoa_n's (pg_ulltoa_n's) contract; postgres itself
 *      uses these emitters as its int4out/int8out cores and
 *      proofs/intout's harnesses exercise the vendored C bodies across
 *      the full band/spot set. Documented per-function below.
 *   6. palloc/pstrdup -> caller-provided fixed buffers.
 *   7. Assert -> no-op (production NDEBUG build); "static inline" and
 *      "static" dropped where goto-cc must export the symbol.
 *   8. Functions renamed with a pg_ prefix (pg_tidin, ...); helpers with
 *      a pg_shim_ prefix.
 *
 * Postgres compiles with -fwrapv; CBMC's default two's-complement wrap
 * matches, so the signed-overflow idiom in date_pli/date_mii
 * ("result = dateVal + days; if (days >= 0 ? (result < dateVal) : ...)")
 * is vendored verbatim and NOT "fixed".
 */

#include <string.h>				/* memcpy: CBMC has a builtin model */

typedef signed short int16;
typedef signed int int32;
typedef signed long long int64;
typedef unsigned char uint8;
typedef unsigned short uint16;
typedef unsigned int uint32;
typedef unsigned long long uint64;

#define UINT64CONST(x) (x##ULL)
#define Assert(x) ((void) 0)

#define PG_SHIM_ULLONG_MAX 0xFFFFFFFFFFFFFFFFULL

/* error-class codes (SHIM 3) */
#define PG_SHIM_OK 0
#define PG_SHIM_ERR_SYNTAX 1	/* ERRCODE_INVALID_TEXT_REPRESENTATION */
#define PG_SHIM_ERR_RANGE 2		/* ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE */

/* ======================================================================
 * bool.c — booland_statefunc / boolor_statefunc (bodies verbatim inside
 * the fmgr shim; the whole function IS the one-line expression).
 * ====================================================================== */

int
pg_booland_statefunc(int arg1, int arg2)
{
	return arg1 && arg2;
}

int
pg_boolor_statefunc(int arg1, int arg2)
{
	return arg1 || arg2;
}

/* ======================================================================
 * date.c — date_pli / date_mii.
 * Constants verbatim from datatype/timestamp.h + utils/date.h (REL_18).
 * ====================================================================== */

typedef int32 DateADT;

#define PG_INT32_MIN	(-0x7FFFFFFF-1)
#define PG_INT32_MAX	(0x7FFFFFFF)

#define DATEVAL_NOBEGIN ((DateADT) PG_INT32_MIN)
#define DATEVAL_NOEND	((DateADT) PG_INT32_MAX)

#define DATE_IS_NOBEGIN(j)	((j) == DATEVAL_NOBEGIN)
#define DATE_IS_NOEND(j)	((j) == DATEVAL_NOEND)
#define DATE_NOT_FINITE(j)	(DATE_IS_NOBEGIN(j) || DATE_IS_NOEND(j))

#define POSTGRES_EPOCH_JDATE	2451545 /* == date2j(2000, 1, 1) */
#define DATETIME_MIN_JULIAN (0)
#define DATE_END_JULIAN (2147483494)	/* == date2j(JULIAN_MAXYEAR, 1, 1) */

#define IS_VALID_DATE(d) \
	((DATETIME_MIN_JULIAN - POSTGRES_EPOCH_JDATE) <= (d) && \
	 (d) < (DATE_END_JULIAN - POSTGRES_EPOCH_JDATE))

/*
 * date_pli body verbatim; ereport(ERROR, ERRCODE_DATETIME_VALUE_OUT_OF_RANGE,
 * "date out of range") -> return 1 (SHIM 3), result through out-param.
 * Returns 0 = OK (including the infinity passthrough), 1 = the ereport.
 */
int
pg_date_pli(DateADT dateVal, int32 days, DateADT *result_out)
{
	DateADT		result;

	if (DATE_NOT_FINITE(dateVal))
	{
		*result_out = dateVal;	/* can't change infinity */
		return PG_SHIM_OK;
	}

	result = dateVal + days;

	/* Check for integer overflow and out-of-allowed-range */
	if ((days >= 0 ? (result < dateVal) : (result > dateVal)) ||
		!IS_VALID_DATE(result))
		return PG_SHIM_ERR_SYNTAX;	/* ereport(ERROR, ...VALUE_OUT_OF_RANGE) */

	*result_out = result;
	return PG_SHIM_OK;
}

/* date_mii body verbatim; same shim as pg_date_pli. */
int
pg_date_mii(DateADT dateVal, int32 days, DateADT *result_out)
{
	DateADT		result;

	if (DATE_NOT_FINITE(dateVal))
	{
		*result_out = dateVal;	/* can't change infinity */
		return PG_SHIM_OK;
	}

	result = dateVal - days;

	/* Check for integer overflow and out-of-allowed-range */
	if ((days >= 0 ? (result > dateVal) : (result < dateVal)) ||
		!IS_VALID_DATE(result))
		return PG_SHIM_ERR_SYNTAX;	/* ereport(ERROR, ...VALUE_OUT_OF_RANGE) */

	*result_out = result;
	return PG_SHIM_OK;
}

/* ======================================================================
 * SHIM 4 — libc strtoul/strtoull model (see header for full semantics
 * and per-call-site narrowing argument).
 * ====================================================================== */

static int
pg_shim_isspace(char c)
{
	return c == ' ' || c == '\t' || c == '\n' ||
		c == '\v' || c == '\f' || c == '\r';
}

/* hex digit value, or -1 */
static int
pg_shim_hexval(char c)
{
	if (c >= '0' && c <= '9')
		return c - '0';
	if (c >= 'a' && c <= 'f')
		return c - 'a' + 10;
	if (c >= 'A' && c <= 'F')
		return c - 'A' + 10;
	return -1;
}

/*
 * strtoull(nptr, endptr, base) for base in {0, 10}; base 16 reachable only
 * via base-0 prefix detection. Sets *erange = 1 (in place of errno=ERANGE)
 * on overflow; leaves *erange untouched otherwise (caller zeroes it, which
 * models tidin's/uint*in_subr's own "errno = 0" line).
 * Three literal-base accumulation loops so CBMC sees only constant
 * divisors; each loop is the standard cutoff/cutlim overflow check with
 * the base folded in.
 */
static uint64
pg_shim_strtoull(const char *nptr, const char **endptr, int base, int *erange)
{
	const char *s = nptr;
	int			neg = 0;
	uint64		v = 0;
	int			any = 0;
	int			overflow = 0;

	while (pg_shim_isspace(*s))
		s++;
	if (*s == '-')
	{
		neg = 1;
		s++;
	}
	else if (*s == '+')
		s++;

	if (base == 0)
	{
		if (s[0] == '0' && (s[1] == 'x' || s[1] == 'X') &&
			pg_shim_hexval(s[2]) >= 0)
		{
			s += 2;
			base = 16;
		}
		else if (s[0] == '0')
			base = 8;			/* the '0' itself is the first digit */
		else
			base = 10;
	}

	if (base == 10)
	{
		for (;; s++)
		{
			char		c = *s;

			if (c < '0' || c > '9')
				break;
			{
				uint64		d = (uint64) (c - '0');

				if (v > (PG_SHIM_ULLONG_MAX - d) / 10)
					overflow = 1;
				else
					v = v * 10 + d;
				any = 1;
			}
		}
	}
	else if (base == 16)
	{
		for (;; s++)
		{
			int			dv = pg_shim_hexval(*s);

			if (dv < 0)
				break;
			if (v > (PG_SHIM_ULLONG_MAX - (uint64) dv) / 16)
				overflow = 1;
			else
				v = v * 16 + (uint64) dv;
			any = 1;
		}
	}
	else						/* base == 8 */
	{
		for (;; s++)
		{
			char		c = *s;

			if (c < '0' || c > '7')
				break;
			{
				uint64		d = (uint64) (c - '0');

				if (v > (PG_SHIM_ULLONG_MAX - d) / 8)
					overflow = 1;
				else
					v = v * 8 + d;
				any = 1;
			}
		}
	}

	if (!any)
	{
		/* no conversion: endptr = original nptr, value 0, errno untouched */
		*endptr = nptr;
		return 0;
	}
	*endptr = s;
	if (overflow)
	{
		*erange = 1;
		return PG_SHIM_ULLONG_MAX;
	}
	return neg ? (uint64) 0 - v : v;
}

/* ======================================================================
 * tid.c — tidin / tidout.
 * ====================================================================== */

#define LDELIM			'('
#define RDELIM			')'
#define DELIM			','
#define NTIDARGS		2
#define USHRT_MAX_		65535

/*
 * tidin body verbatim (scan loop, both strtoul calls, all checks,
 * including the SIZEOF_LONG > 4 narrowing arm — compiled in, see SHIM 1).
 * ereturn(...ERRCODE_INVALID_TEXT_REPRESENTATION...) -> return 1 (all four
 * error sites carry the same errcode). Result through out-params
 * (palloc'd ItemPointer -> block/offset pair; ItemPointerSet is pure field
 * assignment). errno = 0 -> erange flag zeroed before the first call and
 * NOT rezeroed before the second, exactly like the original's single
 * "errno = 0" (a set flag from call 1 would already have errored out).
 * Returns 0 = OK, 1 = invalid syntax (sqlstate 22P02).
 */
int
pg_tidin(const char *str, uint32 *block_out, uint16 *offset_out)
{
	const char *p,
			   *coord[NTIDARGS];
	int			i;
	uint32		blockNumber;
	uint16		offsetNumber;
	const char *badp;
	uint64		cvt;			/* unsigned long, 64-bit (SHIM 1) */
	int			erange = 0;		/* errno = 0 */

	for (i = 0, p = str; *p && i < NTIDARGS && *p != RDELIM; p++)
		if (*p == DELIM || (*p == LDELIM && i == 0))
			coord[i++] = p + 1;

	if (i < NTIDARGS)
		return PG_SHIM_ERR_SYNTAX;

	cvt = pg_shim_strtoull(coord[0], &badp, 10, &erange);
	if (erange || *badp != DELIM)
		return PG_SHIM_ERR_SYNTAX;
	blockNumber = (uint32) cvt;

	/*
	 * Cope with possibility that unsigned long is wider than BlockNumber,
	 * in which case strtoul will not raise an error for some values that
	 * are out of the range of BlockNumber.  (See similar code in oidin().)
	 */
	/* #if SIZEOF_LONG > 4 — true on all modeled targets (SHIM 1) */
	if (cvt != (uint64) blockNumber &&
		cvt != (uint64) ((int64) ((int32) blockNumber)))
		return PG_SHIM_ERR_SYNTAX;

	cvt = pg_shim_strtoull(coord[1], &badp, 10, &erange);
	if (erange || *badp != RDELIM ||
		cvt > USHRT_MAX_)
		return PG_SHIM_ERR_SYNTAX;
	offsetNumber = (uint16) cvt;

	*block_out = blockNumber;	/* ItemPointerSet */
	*offset_out = offsetNumber;
	return PG_SHIM_OK;
}

/* forward decls of the verbatim numutils.c emitters vendored below */
int			pg_c_ultoa_n(uint32 value, char *a);
int			pg_c_ulltoa_n(uint64 value, char *a);

/*
 * tidout: snprintf(buf, sizeof(buf), "(%u,%u)", blockNumber, offsetNumber)
 * modeled per SHIM 5 as '(' + %u + ',' + %u + ')' + NUL with the verbatim
 * pg_ultoa_n as the %u emitter. Returns strlen (NUL excluded).
 */
int
pg_tidout(uint32 blockNumber, uint16 offsetNumber, char *buf)
{
	int			n = 0;

	buf[n++] = '(';
	n += pg_c_ultoa_n(blockNumber, buf + n);
	buf[n++] = ',';
	n += pg_c_ultoa_n((uint32) offsetNumber, buf + n);
	buf[n++] = ')';
	buf[n] = '\0';
	return n;
}

/* ======================================================================
 * numutils.c — uint32in_subr / uint64in_subr (bodies verbatim; strtoul ->
 * SHIM 4; ereturn -> SHIM 3 error classes; only the endloc == NULL branch
 * is reachable from the ledger rows under proof — oidin/xidin/xid8in all
 * pass NULL — but the endloc handling is kept verbatim with a paramized
 * flag so the vendored control flow is unchanged).
 * ====================================================================== */

/*
 * uint32in_subr. Returns PG_SHIM_OK / PG_SHIM_ERR_SYNTAX (22P02) /
 * PG_SHIM_ERR_RANGE (22003); value through *result_out.
 * want_endloc = 0 models endloc == NULL (both catalog callers).
 */
int
pg_uint32in_subr(const char *s, uint32 *result_out, int want_endloc,
				 const char **endloc_out)
{
	uint32		result;
	uint64		cvt;			/* unsigned long, 64-bit (SHIM 1) */
	const char *endptr;
	int			erange = 0;		/* errno = 0 */

	cvt = pg_shim_strtoull(s, &endptr, 0, &erange);

	/*
	 * strtoul() normally only sets ERANGE.  On some systems it may also set
	 * EINVAL, which simply means it couldn't parse the input string.  Be
	 * sure to report that the same way as the standard error indication
	 * (that endptr == s).  [EINVAL arm dead under the model — SHIM 4.]
	 */
	if (endptr == s)
		return PG_SHIM_ERR_SYNTAX;

	if (erange)
		return PG_SHIM_ERR_RANGE;

	if (want_endloc)
	{
		/* caller wants to deal with rest of string */
		*endloc_out = endptr;
	}
	else
	{
		/* allow only whitespace after number */
		while (*endptr && pg_shim_isspace(*endptr))
			endptr++;
		if (*endptr)
			return PG_SHIM_ERR_SYNTAX;
	}

	result = (uint32) cvt;

	/*
	 * Cope with possibility that unsigned long is wider than uint32 [it is
	 * on all modeled targets — SHIM 1] ... allow the input value if it
	 * matches after either signed or unsigned extension to long.
	 */
	if (cvt != (uint64) result &&
		cvt != (uint64) ((int64) ((int32) result)))
		return PG_SHIM_ERR_RANGE;

	*result_out = result;
	return PG_SHIM_OK;
}

/*
 * uint64in_subr. Same shims; strtou64 == strtoull on 64-bit-long targets.
 */
int
pg_uint64in_subr(const char *s, uint64 *result_out, int want_endloc,
				 const char **endloc_out)
{
	uint64		result;
	const char *endptr;
	int			erange = 0;		/* errno = 0 */

	result = pg_shim_strtoull(s, &endptr, 0, &erange);

	if (endptr == s)
		return PG_SHIM_ERR_SYNTAX;

	if (erange)
		return PG_SHIM_ERR_RANGE;

	if (want_endloc)
	{
		/* caller wants to deal with rest of string */
		*endloc_out = endptr;
	}
	else
	{
		/* allow only whitespace after number */
		while (*endptr && pg_shim_isspace(*endptr))
			endptr++;
		if (*endptr)
			return PG_SHIM_ERR_SYNTAX;
	}

	*result_out = result;
	return PG_SHIM_OK;
}

/* ======================================================================
 * oid.c oidout / xid.c xid8out — snprintf("%u") / snprintf(UINT64_FORMAT)
 * modeled per SHIM 5 with the verbatim numutils.c emitters + NUL.
 * (xidout, "%lu" of a uint32-widened value, is byte-identical to oidout's
 * "%u" on every input; xidin/oidin are the uint32in_subr theorem.)
 * Returns strlen (NUL excluded); harnesses also check the NUL.
 * ====================================================================== */

int
pg_oidout(uint32 o, char *buf)
{
	int			n = pg_c_ultoa_n(o, buf);

	buf[n] = '\0';
	return n;
}

int
pg_xid8out(uint64 fxid, char *buf)
{
	int			n = pg_c_ulltoa_n(fxid, buf);

	buf[n] = '\0';
	return n;
}

/* ======================================================================
 * numutils.c digit emitters + pg_bitutils.c helpers — verbatim (same
 * vendoring as proofs/intout; portable table-walk branch of
 * pg_leftmost_one_pos*, see that crate's header). Renamed pg_c_* to keep
 * the pg_ prefix convention without clashing with upstream names.
 * NOTE: the table-walk loop's exit depends on word != 0 — every harness
 * reaching these needs #[kani::unwind(N)].
 * ====================================================================== */

static const uint8 pg_leftmost_one_pos[256] = {
	0, 0, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3,
	4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4,
	5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5,
	5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5,
	6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
	6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
	6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
	6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
	7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
	7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
	7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
	7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
	7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
	7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
	7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
	7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7
};

static int
pg_shim_leftmost_one_pos32(uint32 word)
{
	int			shift = 32 - 8;

	Assert(word != 0);

	while ((word >> shift) == 0)
		shift -= 8;

	return shift + pg_leftmost_one_pos[(word >> shift) & 255];
}

static int
pg_shim_leftmost_one_pos64(uint64 word)
{
	int			shift = 64 - 8;

	Assert(word != 0);

	while ((word >> shift) == 0)
		shift -= 8;

	return shift + pg_leftmost_one_pos[(word >> shift) & 255];
}

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

static int
decimalLength32(const uint32 v)
{
	int			t;
	static const uint32 PowersOfTen[] = {
		1, 10, 100,
		1000, 10000, 100000,
		1000000, 10000000, 100000000,
		1000000000
	};

	t = (pg_shim_leftmost_one_pos32(v) + 1) * 1233 / 4096;
	return t + (v >= PowersOfTen[t]);
}

static int
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

	t = (pg_shim_leftmost_one_pos64(v) + 1) * 1233 / 4096;
	return t + (v >= PowersOfTen[t]);
}

int
pg_c_ultoa_n(uint32 value, char *a)
{
	int			olength,
				i = 0;

	/* Degenerate case */
	if (value == 0)
	{
		*a = '0';
		return 1;
	}

	olength = decimalLength32(value);

	/* Compute the result string. */
	while (value >= 10000)
	{
		const uint32 c = value - 10000 * (value / 10000);
		const uint32 c0 = (c % 100) << 1;
		const uint32 c1 = (c / 100) << 1;

		char	   *pos = a + olength - i;

		value /= 10000;

		memcpy(pos - 2, DIGIT_TABLE + c0, 2);
		memcpy(pos - 4, DIGIT_TABLE + c1, 2);
		i += 4;
	}
	if (value >= 100)
	{
		const uint32 c = (value % 100) << 1;

		char	   *pos = a + olength - i;

		value /= 100;

		memcpy(pos - 2, DIGIT_TABLE + c, 2);
		i += 2;
	}
	if (value >= 10)
	{
		const uint32 c = value << 1;

		char	   *pos = a + olength - i;

		memcpy(pos - 2, DIGIT_TABLE + c, 2);
	}
	else
	{
		*a = (char) ('0' + value);
	}

	return olength;
}

int
pg_c_ulltoa_n(uint64 value, char *a)
{
	int			olength,
				i = 0;
	uint32		value2;

	/* Degenerate case */
	if (value == 0)
	{
		*a = '0';
		return 1;
	}

	olength = decimalLength64(value);

	/* Compute the result string. */
	while (value >= 100000000)
	{
		const uint64 q = value / 100000000;
		uint32		value3 = (uint32) (value - 100000000 * q);

		const uint32 c = value3 % 10000;
		const uint32 d = value3 / 10000;
		const uint32 c0 = (c % 100) << 1;
		const uint32 c1 = (c / 100) << 1;
		const uint32 d0 = (d % 100) << 1;
		const uint32 d1 = (d / 100) << 1;

		char	   *pos = a + olength - i;

		value = q;

		memcpy(pos - 2, DIGIT_TABLE + c0, 2);
		memcpy(pos - 4, DIGIT_TABLE + c1, 2);
		memcpy(pos - 6, DIGIT_TABLE + d0, 2);
		memcpy(pos - 8, DIGIT_TABLE + d1, 2);
		i += 8;
	}

	/* Switch to 32-bit for speed */
	value2 = (uint32) value;

	if (value2 >= 10000)
	{
		const uint32 c = value2 - 10000 * (value2 / 10000);
		const uint32 c0 = (c % 100) << 1;
		const uint32 c1 = (c / 100) << 1;

		char	   *pos = a + olength - i;

		value2 /= 10000;

		memcpy(pos - 2, DIGIT_TABLE + c0, 2);
		memcpy(pos - 4, DIGIT_TABLE + c1, 2);
		i += 4;
	}
	if (value2 >= 100)
	{
		const uint32 c = (value2 % 100) << 1;
		char	   *pos = a + olength - i;

		value2 /= 100;

		memcpy(pos - 2, DIGIT_TABLE + c, 2);
		i += 2;
	}
	if (value2 >= 10)
	{
		const uint32 c = value2 << 1;
		char	   *pos = a + olength - i;

		memcpy(pos - 2, DIGIT_TABLE + c, 2);
	}
	else
		*a = (char) ('0' + value2);

	return olength;
}
