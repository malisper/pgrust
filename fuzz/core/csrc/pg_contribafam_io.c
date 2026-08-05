/*
 * pg_contribafam_io.c: vendored PostgreSQL C oracle for the contriba_diff
 * differential fuzz target (100%-coverage campaign, lane p1-mb-contriba).
 * Crates under test (see fuzz/core/src/contriba_diff.rs):
 *   crates/contrib/fuzzystrmatch (soundex, metaphone, dmetaphone,
 *   daitch_mokotoff, levenshtein fc wrappers), crates/contrib/isn.
 *
 * Provenance (all bodies VERBATIM sed-extracted from the vendor tree at
 * ~/dev/pgrust-fabled/vendor/postgres-src, Stamp-18.3, upstream sha
 * 62d6c7d3df6287f1bd83199c1a746e50d31571a0 — assembled by
 * scratchpad/assemble_contribafam.sh, never hand-typed):
 *   - contrib/fuzzystrmatch/fuzzystrmatch.c lines 51-70 (_soundex decl,
 *     SOUNDEX_LEN, soundex_table, soundex_code), 71-75 (MAX_METAPHONE_STRLEN),
 *     105-147 (SH/TH, decls, _codes, getcode, isvowel..NOGHTOF macros),
 *     300-343 (letter macros + Lookahead + Phonize macros), 346-704
 *     (_metaphone), 725-773 (_soundex); PG-wrapper BODIES verbatim inside
 *     driver shells (fmgr unwrapping, shim 1): metaphone 261-291, soundex
 *     714-723, difference 779-796.
 *   - contrib/fuzzystrmatch/dmetaphone.c lines 183-200 (META_MALLOC/
 *     REALLOC/FREE, the palloc arm — DMETAPHONE_MAIN undefined exactly as
 *     the PG build), 216-1421 (metastring + all statics + DoubleMetaphone);
 *     wrapper BODIES dmetaphone 132-151, dmetaphone_alt 161-180.
 *   - contrib/fuzzystrmatch/daitch_mokotoff.c lines 61-117 (DM_CODE_DIGITS,
 *     dm_node, start_node, end_codes, iso8859_1_to_ascii_upper, coding
 *     decl), 161-571 (all statics: initialize_node .. daitch_mokotoff_coding).
 *     Generated coding chart csrc/contribafam/daitch_mokotoff.h produced by
 *     the vendored contrib/fuzzystrmatch/daitch_mokotoff_header.pl (the PG
 *     build's own generation step, perl 5, deterministic output).
 *   - src/backend/utils/adt/varlena.c lines 6408-6423 (rest_of_char_same)
 *     and the two levenshtein.c expansions exactly as varlena.c does them
 *     (lines 6424-6427): src/backend/utils/adt/levenshtein.c pasted VERBATIM
 *     whole (1-403), then #define LEVENSHTEIN_LESS_EQUAL, then pasted whole
 *     again.
 *   - src/backend/utils/mb/mbutils.c lines 1076-1098 (pg_mblen_range),
 *     1100-1122 (pg_mblen_with_len), 1179-1200 (pg_mbstrlen_with_len), all
 *     static-prefixed via marker lines.
 *   - src/common/wchar.c lines 544-577 (pg_utf_mblen), static-prefixed.
 *   - src/include/mb/pg_wchar.h lines 558-584 (utf8_to_unicode, static
 *     inline verbatim).
 *   - contrib/isn/isn.c lines 36-48 (MAXEAN13LEN, isn_type, isn_names,
 *     g_weak) and 141-915 (dehyphenate, hyphenate, weight_checkdig,
 *     checkdig, ean2isn, ean2ISBN/ISMN/ISSN/UPC, str2ean, ean2string,
 *     string2ean). check_table/_PG_init (ISN_DEBUG assert plumbing + GUC
 *     registration) and the PG_FUNCTION_INFO_V1 wrappers are NOT vendored:
 *     the wrappers are pure fmgr plumbing (the Rust fc plane compares its
 *     wrappers against these cores directly), and g_weak is driven by the
 *     pg_ca_set_weak fixture setter (environment mock for the GUC store —
 *     the Rust side reads its isn.weak GUC through the same fuzz-selected
 *     value each exec).
 *   - contrib/isn/{EAN13.h,ISBN.h,ISMN.h,ISSN.h,UPC.h}: copied whole to
 *     csrc/contribafam/ (data tables), #included verbatim.
 *   - contrib/isn/isn.h line 25 typedef (ean13) transcribed below;
 *     EAN13_FORMAT resolved to PRIu64 (UINT64_FORMAT on LP64; feeds only
 *     swallowed errmsg text).
 *
 * Shims (plumbing only, never logic):
 *   - fixed-width typedefs matching c.h on LP64; Size = size_t; Min/Max;
 *     Assert(noop) (release parity); likely/unlikely passthrough; _()
 *     gettext passthrough; VALGRIND_CHECK_MEM_IS_DEFINED noop
 *     (instrumentation, not semantics).
 *   - ereport/elog(ERROR) -> record an errcode class in the TLS
 *     pg_ca_errcode channel and longjmp to the armed driver entry.
 *     ereturn(ctx,val,rest) (isn string2ean soft errors) -> when ctx is
 *     non-NULL record the class, bump the pg_ca_soft_fired witness counter
 *     (the C-branch-executes witness the soft plane asserts on), and return
 *     val; NULL ctx degrades to ereport exactly like elog.h's ereturn.
 *     Classes: 0 ok, 1 = 22P02 ERRCODE_INVALID_TEXT_REPRESENTATION,
 *     2 = 22003 ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE, 3 = 22023
 *     ERRCODE_INVALID_PARAMETER_VALUE, 4 = 2200F
 *     ERRCODE_ZERO_LENGTH_CHARACTER_STRING, 5 = 22021 invalid byte
 *     sequence (report_invalid_encoding_db), 6 = XX000 (elog ERROR).
 *   - palloc/repalloc -> TLS pointer arena (malloc-backed, reset by every
 *     driver entry; models PG's per-call context reset so error-path
 *     longjmps cannot leak — the pg_like_io.c LSan-incident pattern).
 *   - fmgr unwrapping (shim 1, pg_like_io.c precedent): PG_GETARG_TEXT_PP/
 *     PG_GETARG_DATUM -> TLS pg_ca_args slots; PG_GETARG_INT32 ->
 *     pg_ca_int_args; text_to_cstring/TextDatumGetCString/cstring_to_text
 *     -> identity over NUL-terminated char* (the drivers pass cstrings —
 *     text payloads with interior NULs are outside the PG text domain);
 *     PG_RETURN_TEXT_P -> snprintf into the TLS out buffer + return 0;
 *     PG_RETURN_INT32 -> TLS int out + return 0; typedef char text.
 *   - DATABASE ENCODING PIN = UTF8: pg_wchar_table reduced to the one row
 *     the pinned encoding dispatches to (mblen = verbatim pg_utf_mblen);
 *     DatabaseEncoding->encoding = 0 indexes it. The Rust driver pins
 *     mbutils::SetDatabaseEncoding(PG_UTF8) every levenshtein exec.
 *     pg_database_encoding_max_length -> 4 (the UTF8 row's maxmblen).
 *   - daitch array plumbing: ArrayBuildState is opaque here;
 *     cstring_to_text_with_len -> identity pointer; accumArrayResult ->
 *     append the 6-digit code to the driver's flat output list (the array
 *     ACCUMULATION is fmgr/array machinery, the CODING is verbatim above);
 *     palloc_object -> palloc(sizeof) (palloc.h's own definition);
 *     TEXTOID/CurrentMemoryContext dummies feeding only the shim.
 *   - struct Node -> dummy tag struct (string2ean only tests pointer
 *     nullness through the ereturn shim, mirroring escontext semantics).
 *   - varstr_levenshtein{,_less_equal} kept file-static (marker-line
 *     `static` before the verbatim paste) to keep this TU symbol-clean.
 *
 * Driver entries (SECTION D, pg_ca_ prefix) are fuzz plumbing, NOT
 * Postgres code. Every entry that can reach ereport/elog arms the jmp_buf
 * and resets the arena + error channel.
 */

#include <stddef.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <setjmp.h>
#include <ctype.h>
#include <assert.h>
#include <inttypes.h>

typedef int8_t int8;
typedef int16_t int16;
typedef int32_t int32;
typedef int64_t int64;
typedef uint8_t uint8;
typedef uint16_t uint16;
typedef uint32_t uint32;
typedef uint64_t uint64;
typedef size_t Size;
typedef unsigned int pg_wchar;
typedef char text;			/* shim 1: identity cstring texts */

#define Assert(x) ((void) 0)
#define unlikely(x) (x)
#define likely(x) (x)
#define _(x) (x)
#define Min(x, y) ((x) < (y) ? (x) : (y))
#define Max(x, y) ((x) > (y) ? (x) : (y))
#define pg_attribute_unused()
#define VALGRIND_CHECK_MEM_IS_DEFINED(a, b) ((void) 0)
#define UINT64CONST(x) UINT64_C(x)

/* ---- SHIM: TLS error channel + longjmp (armed by driver entries) ---- */

static _Thread_local int pg_ca_errcode;
static _Thread_local jmp_buf pg_ca_jmp;
static _Thread_local int pg_ca_soft_fired;	/* soft-plane C-branch witness */

#define PG_CA_ERR_INVALID_TEXT 1	/* 22P02 */
#define PG_CA_ERR_OUT_OF_RANGE 2	/* 22003 */
#define PG_CA_ERR_INVALID_PARAM 3	/* 22023 */
#define PG_CA_ERR_ZERO_LENGTH 4		/* 2200F */
#define PG_CA_ERR_INVALID_BYTE_SEQ 5	/* 22021 */
#define PG_CA_ERR_INTERNAL 6		/* XX000 */

#define ERRCODE_INVALID_TEXT_REPRESENTATION PG_CA_ERR_INVALID_TEXT
#define ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE PG_CA_ERR_OUT_OF_RANGE
#define ERRCODE_INVALID_PARAMETER_VALUE PG_CA_ERR_INVALID_PARAM
#define ERRCODE_ZERO_LENGTH_CHARACTER_STRING PG_CA_ERR_ZERO_LENGTH

static _Noreturn void
pg_ca_raise(int code)
{
	pg_ca_errcode = code;
	longjmp(pg_ca_jmp, 1);
}

static _Thread_local int pg_ca_pending_code;

static int
pg_ca_errcode_set(int code)
{
	pg_ca_pending_code = code;
	return 0;
}

static int
pg_ca_errmsg(const char *fmt,...)
{
	(void) fmt;
	return 0;
}

#define errcode(c) pg_ca_errcode_set(c)
#define errmsg pg_ca_errmsg
#define ereport(level, rest) do { pg_ca_pending_code = PG_CA_ERR_INTERNAL; ((void) (rest)); pg_ca_raise(pg_ca_pending_code); } while (0)
#define elog(level, ...) do { pg_ca_errmsg(__VA_ARGS__); pg_ca_raise(PG_CA_ERR_INTERNAL); } while (0)
#define ERROR 21

/* elog.h ereturn, soft-error shape: non-NULL context saves + returns (the
 * pg_ca_soft_fired bump is the witness the Rust soft plane asserts on);
 * NULL context is the hard ereport path. */
#define ereturn(context, dummy_value, rest) \
	do { \
		pg_ca_pending_code = PG_CA_ERR_INTERNAL; \
		((void) (rest)); \
		if ((context) != NULL) \
		{ \
			pg_ca_errcode = pg_ca_pending_code; \
			pg_ca_soft_fired++; \
			return dummy_value; \
		} \
		pg_ca_raise(pg_ca_pending_code); \
	} while (0)

/* ---- SHIM: TLS pointer arena (palloc; reset by every driver entry) ---- */

#define PG_CA_MAX_PTRS 65536
static _Thread_local void *pg_ca_ptrs[PG_CA_MAX_PTRS];
static _Thread_local int pg_ca_nptrs;

static void
pg_ca_arena_reset(void)
{
	while (pg_ca_nptrs > 0)
		free(pg_ca_ptrs[--pg_ca_nptrs]);
}

static void *
pg_ca_palloc(size_t n)
{
	void	   *p = malloc(n ? n : 1);

	if (p == NULL || pg_ca_nptrs >= PG_CA_MAX_PTRS)
		abort();				/* driver capacity, never PG semantics */
	pg_ca_ptrs[pg_ca_nptrs++] = p;
	return p;
}

static void *
pg_ca_repalloc(void *old, size_t n)
{
	void	   *p = realloc(old, n ? n : 1);
	int			i;

	if (p == NULL)
		abort();
	for (i = pg_ca_nptrs - 1; i >= 0; i--)
	{
		if (pg_ca_ptrs[i] == old)
		{
			pg_ca_ptrs[i] = p;
			return p;
		}
	}
	abort();					/* repalloc of an untracked pointer */
}

#define palloc(n) pg_ca_palloc(n)
#define repalloc(p, n) pg_ca_repalloc((p), (n))
/* palloc.h's own palloc_object definition, over the arena palloc */
#define palloc_object(type) ((type *) palloc(sizeof(type)))

/* ---- SHIM 1: fmgr unwrapping (TLS arg slots + out buffers) ---- */

static _Thread_local char *pg_ca_args[4];
static _Thread_local int32 pg_ca_int_args[4];
static _Thread_local char pg_ca_out[512];
static _Thread_local int32 pg_ca_int_out;

#define PG_GETARG_TEXT_PP(n) pg_ca_args[n]
#define PG_GETARG_DATUM(n) pg_ca_args[n]
#define PG_GETARG_INT32(n) pg_ca_int_args[n]
#define TextDatumGetCString(d) ((char *) (d))
#define text_to_cstring(t) ((char *) (t))
#define cstring_to_text(s) (s)
#define PG_RETURN_TEXT_P(x) do { snprintf(pg_ca_out, sizeof(pg_ca_out), "%s", (x)); return 0; } while (0)
#define PG_RETURN_INT32(x) do { pg_ca_int_out = (x); return 0; } while (0)

/* ================= SECTION 1: encoding plane (UTF8 pin) ================= */

/* ---- VERBATIM src/common/wchar.c lines 544-577 [static-prefixed] ---- */
static
/*
 * Return the byte length of a UTF8 character pointed to by s
 *
 * Note: in the current implementation we do not support UTF8 sequences
 * of more than 4 bytes; hence do NOT return a value larger than 4.
 * We return "1" for any leading byte that is either flat-out illegal or
 * indicates a length larger than we support.
 *
 * pg_utf2wchar_with_len(), utf8_to_unicode(), pg_utf8_islegal(), and perhaps
 * other places would need to be fixed to change this.
 */
int
pg_utf_mblen(const unsigned char *s)
{
	int			len;

	if ((*s & 0x80) == 0)
		len = 1;
	else if ((*s & 0xe0) == 0xc0)
		len = 2;
	else if ((*s & 0xf0) == 0xe0)
		len = 3;
	else if ((*s & 0xf8) == 0xf0)
		len = 4;
#ifdef NOT_USED
	else if ((*s & 0xfc) == 0xf8)
		len = 5;
	else if ((*s & 0xfe) == 0xfc)
		len = 6;
#endif
	else
		len = 1;
	return len;
}

/* ---- VERBATIM src/include/mb/pg_wchar.h lines 558-584 (static inline) ---- */
/*
 * Convert a UTF-8 character to a Unicode code point.
 * This is a one-character version of pg_utf2wchar_with_len.
 *
 * No error checks here, c must point to a long-enough string.
 */
static inline pg_wchar
utf8_to_unicode(const unsigned char *c)
{
	if ((*c & 0x80) == 0)
		return (pg_wchar) c[0];
	else if ((*c & 0xe0) == 0xc0)
		return (pg_wchar) (((c[0] & 0x1f) << 6) |
						   (c[1] & 0x3f));
	else if ((*c & 0xf0) == 0xe0)
		return (pg_wchar) (((c[0] & 0x0f) << 12) |
						   ((c[1] & 0x3f) << 6) |
						   (c[2] & 0x3f));
	else if ((*c & 0xf8) == 0xf0)
		return (pg_wchar) (((c[0] & 0x07) << 18) |
						   ((c[1] & 0x3f) << 12) |
						   ((c[2] & 0x3f) << 6) |
						   (c[3] & 0x3f));
	else
		/* that is an invalid code on purpose */
		return 0xffffffff;
}

/* SHIM: the one pg_wchar_table row the pinned UTF8 encoding dispatches to
 * (mblen = the verbatim pg_utf_mblen above); DatabaseEncoding->encoding = 0
 * indexes it, so the verbatim mbutils.c lookup lines compile unmodified. */
typedef struct
{
	int			(*mblen) (const unsigned char *mbstr);
} pg_ca_wchar_tbl;
static const pg_ca_wchar_tbl pg_wchar_table[] = {{pg_utf_mblen}};
static struct
{
	int			encoding;
}			pg_ca_dbenc = {0};
#define DatabaseEncoding (&pg_ca_dbenc)

/* SHIM: UTF8 row maxmblen (pg_wchar.h pg_wchar_table[PG_UTF8]) */
static int
pg_database_encoding_max_length(void)
{
	return 4;
}

/* SHIM: report_invalid_encoding_db -> errcode class 5 (22021) + longjmp
 * (mbutils.c raises ERRCODE_CHARACTER_NOT_IN_REPERTOIRE "invalid byte
 * sequence for encoding"; message text out of comparator scope). */
static void
report_invalid_encoding_db(const char *mbstr, int mblen, int len)
{
	(void) mbstr;
	(void) mblen;
	(void) len;
	pg_ca_raise(PG_CA_ERR_INVALID_BYTE_SEQ);
}

/* ---- VERBATIM src/backend/utils/mb/mbutils.c lines 1076-1098
 * (pg_mblen_range) [static-prefixed] ---- */
static
/*
 * Returns the byte length of a multibyte character sequence bounded by a range
 * [mbstr, end) of at least one byte in size.  Raises an illegal byte sequence
 * error if the sequence would exceed the range.
 */
int
pg_mblen_range(const char *mbstr, const char *end)
{
	int			length = pg_wchar_table[DatabaseEncoding->encoding].mblen((const unsigned char *) mbstr);

	Assert(end > mbstr);

	if (unlikely(mbstr + length > end))
		report_invalid_encoding_db(mbstr, length, end - mbstr);

#ifdef VALGRIND_EXPENSIVE
	VALGRIND_CHECK_MEM_IS_DEFINED(mbstr, end - mbstr);
#else
	VALGRIND_CHECK_MEM_IS_DEFINED(mbstr, length);
#endif

	return length;
}

/* ---- VERBATIM src/backend/utils/mb/mbutils.c lines 1100-1122
 * (pg_mblen_with_len) [static-prefixed] ---- */
static
/*
 * Returns the byte length of a multibyte character sequence bounded by a range
 * extending for 'limit' bytes, which must be at least one.  Raises an illegal
 * byte sequence error if the sequence would exceed the range.
 */
int
pg_mblen_with_len(const char *mbstr, int limit)
{
	int			length = pg_wchar_table[DatabaseEncoding->encoding].mblen((const unsigned char *) mbstr);

	Assert(limit >= 1);

	if (unlikely(length > limit))
		report_invalid_encoding_db(mbstr, length, limit);

#ifdef VALGRIND_EXPENSIVE
	VALGRIND_CHECK_MEM_IS_DEFINED(mbstr, limit);
#else
	VALGRIND_CHECK_MEM_IS_DEFINED(mbstr, length);
#endif

	return length;
}

/* ---- VERBATIM src/backend/utils/mb/mbutils.c lines 1179-1200
 * (pg_mbstrlen_with_len) [static-prefixed] ---- */
static
/* returns the length (counted in wchars) of a multibyte string
 * (stops at the first of "limit" or a NUL)
 */
int
pg_mbstrlen_with_len(const char *mbstr, int limit)
{
	int			len = 0;

	/* optimization for single byte encoding */
	if (pg_database_encoding_max_length() == 1)
		return limit;

	while (limit > 0 && *mbstr)
	{
		int			l = pg_mblen_with_len(mbstr, limit);

		limit -= l;
		mbstr += l;
		len++;
	}
	return len;
}

/* ================= SECTION 2: levenshtein ================= */

/* ---- VERBATIM src/backend/utils/adt/varlena.c lines 6408-6423
 * (rest_of_char_same) ---- */
/*
 * Helper function for Levenshtein distance functions. Faster than memcmp(),
 * for this use case.
 */
static inline bool
rest_of_char_same(const char *s1, const char *s2, int len)
{
	while (len > 0)
	{
		len--;
		if (s1[len] != s2[len])
			return false;
	}
	return true;
}


/* ---- the two levenshtein.c expansions exactly as varlena.c lines
 * 6424-6427 perform them; each whole-file paste [static-prefixed] ---- */
static
/*-------------------------------------------------------------------------
 *
 * levenshtein.c
 *	  Levenshtein distance implementation.
 *
 * Original author:  Joe Conway <mail@joeconway.com>
 *
 * This file is included by varlena.c twice, to provide matching code for (1)
 * Levenshtein distance with custom costings, and (2) Levenshtein distance with
 * custom costings and a "max" value above which exact distances are not
 * interesting.  Before the inclusion, we rely on the presence of the inline
 * function rest_of_char_same().
 *
 * Written based on a description of the algorithm by Michael Gilleland found
 * at http://www.merriampark.com/ld.htm.  Also looked at levenshtein.c in the
 * PHP 4.0.6 distribution for inspiration.  Configurable penalty costs
 * extension is introduced by Volkan YAZICI <volkan.yazici@gmail.com.
 *
 * Copyright (c) 2001-2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	src/backend/utils/adt/levenshtein.c
 *
 *-------------------------------------------------------------------------
 */
#define MAX_LEVENSHTEIN_STRLEN		255

/*
 * Calculates Levenshtein distance metric between supplied strings, which are
 * not necessarily null-terminated.
 *
 * source: source string, of length slen bytes.
 * target: target string, of length tlen bytes.
 * ins_c, del_c, sub_c: costs to charge for character insertion, deletion,
 *		and substitution respectively; (1, 1, 1) costs suffice for common
 *		cases, but your mileage may vary.
 * max_d: if provided and >= 0, maximum distance we care about; see below.
 * trusted: caller is trusted and need not obey MAX_LEVENSHTEIN_STRLEN.
 *
 * One way to compute Levenshtein distance is to incrementally construct
 * an (m+1)x(n+1) matrix where cell (i, j) represents the minimum number
 * of operations required to transform the first i characters of s into
 * the first j characters of t.  The last column of the final row is the
 * answer.
 *
 * We use that algorithm here with some modification.  In lieu of holding
 * the entire array in memory at once, we'll just use two arrays of size
 * m+1 for storing accumulated values. At each step one array represents
 * the "previous" row and one is the "current" row of the notional large
 * array.
 *
 * If max_d >= 0, we only need to provide an accurate answer when that answer
 * is less than or equal to max_d.  From any cell in the matrix, there is
 * theoretical "minimum residual distance" from that cell to the last column
 * of the final row.  This minimum residual distance is zero when the
 * untransformed portions of the strings are of equal length (because we might
 * get lucky and find all the remaining characters matching) and is otherwise
 * based on the minimum number of insertions or deletions needed to make them
 * equal length.  The residual distance grows as we move toward the upper
 * right or lower left corners of the matrix.  When the max_d bound is
 * usefully tight, we can use this property to avoid computing the entirety
 * of each row; instead, we maintain a start_column and stop_column that
 * identify the portion of the matrix close to the diagonal which can still
 * affect the final answer.
 */
int
#ifdef LEVENSHTEIN_LESS_EQUAL
varstr_levenshtein_less_equal(const char *source, int slen,
							  const char *target, int tlen,
							  int ins_c, int del_c, int sub_c,
							  int max_d, bool trusted)
#else
varstr_levenshtein(const char *source, int slen,
				   const char *target, int tlen,
				   int ins_c, int del_c, int sub_c,
				   bool trusted)
#endif
{
	int			m,
				n;
	int		   *prev;
	int		   *curr;
	int		   *s_char_len = NULL;
	int			j;
	const char *y;
	const char *send = source + slen;
	const char *tend = target + tlen;

	/*
	 * For varstr_levenshtein_less_equal, we have real variables called
	 * start_column and stop_column; otherwise it's just short-hand for 0 and
	 * m.
	 */
#ifdef LEVENSHTEIN_LESS_EQUAL
	int			start_column,
				stop_column;

#undef START_COLUMN
#undef STOP_COLUMN
#define START_COLUMN start_column
#define STOP_COLUMN stop_column
#else
#undef START_COLUMN
#undef STOP_COLUMN
#define START_COLUMN 0
#define STOP_COLUMN m
#endif

	/* Convert string lengths (in bytes) to lengths in characters */
	m = pg_mbstrlen_with_len(source, slen);
	n = pg_mbstrlen_with_len(target, tlen);

	/*
	 * We can transform an empty s into t with n insertions, or a non-empty t
	 * into an empty s with m deletions.
	 */
	if (!m)
		return n * ins_c;
	if (!n)
		return m * del_c;

	/*
	 * For security concerns, restrict excessive CPU+RAM usage. (This
	 * implementation uses O(m) memory and has O(mn) complexity.)  If
	 * "trusted" is true, caller is responsible for not making excessive
	 * requests, typically by using a small max_d along with strings that are
	 * bounded, though not necessarily to MAX_LEVENSHTEIN_STRLEN exactly.
	 */
	if (!trusted &&
		(m > MAX_LEVENSHTEIN_STRLEN ||
		 n > MAX_LEVENSHTEIN_STRLEN))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("levenshtein argument exceeds maximum length of %d characters",
						MAX_LEVENSHTEIN_STRLEN)));

#ifdef LEVENSHTEIN_LESS_EQUAL
	/* Initialize start and stop columns. */
	start_column = 0;
	stop_column = m + 1;

	/*
	 * If max_d >= 0, determine whether the bound is impossibly tight.  If so,
	 * return max_d + 1 immediately.  Otherwise, determine whether it's tight
	 * enough to limit the computation we must perform.  If so, figure out
	 * initial stop column.
	 */
	if (max_d >= 0)
	{
		int			min_theo_d; /* Theoretical minimum distance. */
		int			max_theo_d; /* Theoretical maximum distance. */
		int			net_inserts = n - m;

		min_theo_d = net_inserts < 0 ?
			-net_inserts * del_c : net_inserts * ins_c;
		if (min_theo_d > max_d)
			return max_d + 1;
		if (ins_c + del_c < sub_c)
			sub_c = ins_c + del_c;
		max_theo_d = min_theo_d + sub_c * Min(m, n);
		if (max_d >= max_theo_d)
			max_d = -1;
		else if (ins_c + del_c > 0)
		{
			/*
			 * Figure out how much of the first row of the notional matrix we
			 * need to fill in.  If the string is growing, the theoretical
			 * minimum distance already incorporates the cost of deleting the
			 * number of characters necessary to make the two strings equal in
			 * length.  Each additional deletion forces another insertion, so
			 * the best-case total cost increases by ins_c + del_c. If the
			 * string is shrinking, the minimum theoretical cost assumes no
			 * excess deletions; that is, we're starting no further right than
			 * column n - m.  If we do start further right, the best-case
			 * total cost increases by ins_c + del_c for each move right.
			 */
			int			slack_d = max_d - min_theo_d;
			int			best_column = net_inserts < 0 ? -net_inserts : 0;

			stop_column = best_column + (slack_d / (ins_c + del_c)) + 1;
			if (stop_column > m)
				stop_column = m + 1;
		}
	}
#endif

	/*
	 * In order to avoid calling pg_mblen_range() repeatedly on each character
	 * in s, we cache all the lengths before starting the main loop -- but if
	 * all the characters in both strings are single byte, then we skip this
	 * and use a fast-path in the main loop.  If only one string contains
	 * multi-byte characters, we still build the array, so that the fast-path
	 * needn't deal with the case where the array hasn't been initialized.
	 */
	if (m != slen || n != tlen)
	{
		int			i;
		const char *cp = source;

		s_char_len = (int *) palloc((m + 1) * sizeof(int));
		for (i = 0; i < m; ++i)
		{
			s_char_len[i] = pg_mblen_range(cp, send);
			cp += s_char_len[i];
		}
		s_char_len[i] = 0;
	}

	/* One more cell for initialization column and row. */
	++m;
	++n;

	/* Previous and current rows of notional array. */
	prev = (int *) palloc(2 * m * sizeof(int));
	curr = prev + m;

	/*
	 * To transform the first i characters of s into the first 0 characters of
	 * t, we must perform i deletions.
	 */
	for (int i = START_COLUMN; i < STOP_COLUMN; i++)
		prev[i] = i * del_c;

	/* Loop through rows of the notional array */
	for (y = target, j = 1; j < n; j++)
	{
		int		   *temp;
		const char *x = source;
		int			y_char_len = n != tlen + 1 ? pg_mblen_range(y, tend) : 1;
		int			i;

#ifdef LEVENSHTEIN_LESS_EQUAL

		/*
		 * In the best case, values percolate down the diagonal unchanged, so
		 * we must increment stop_column unless it's already on the right end
		 * of the array.  The inner loop will read prev[stop_column], so we
		 * have to initialize it even though it shouldn't affect the result.
		 */
		if (stop_column < m)
		{
			prev[stop_column] = max_d + 1;
			++stop_column;
		}

		/*
		 * The main loop fills in curr, but curr[0] needs a special case: to
		 * transform the first 0 characters of s into the first j characters
		 * of t, we must perform j insertions.  However, if start_column > 0,
		 * this special case does not apply.
		 */
		if (start_column == 0)
		{
			curr[0] = j * ins_c;
			i = 1;
		}
		else
			i = start_column;
#else
		curr[0] = j * ins_c;
		i = 1;
#endif

		/*
		 * This inner loop is critical to performance, so we include a
		 * fast-path to handle the (fairly common) case where no multibyte
		 * characters are in the mix.  The fast-path is entitled to assume
		 * that if s_char_len is not initialized then BOTH strings contain
		 * only single-byte characters.
		 */
		if (s_char_len != NULL)
		{
			for (; i < STOP_COLUMN; i++)
			{
				int			ins;
				int			del;
				int			sub;
				int			x_char_len = s_char_len[i - 1];

				/*
				 * Calculate costs for insertion, deletion, and substitution.
				 *
				 * When calculating cost for substitution, we compare the last
				 * character of each possibly-multibyte character first,
				 * because that's enough to rule out most mis-matches.  If we
				 * get past that test, then we compare the lengths and the
				 * remaining bytes.
				 */
				ins = prev[i] + ins_c;
				del = curr[i - 1] + del_c;
				if (x[x_char_len - 1] == y[y_char_len - 1]
					&& x_char_len == y_char_len &&
					(x_char_len == 1 || rest_of_char_same(x, y, x_char_len)))
					sub = prev[i - 1];
				else
					sub = prev[i - 1] + sub_c;

				/* Take the one with minimum cost. */
				curr[i] = Min(ins, del);
				curr[i] = Min(curr[i], sub);

				/* Point to next character. */
				x += x_char_len;
			}
		}
		else
		{
			for (; i < STOP_COLUMN; i++)
			{
				int			ins;
				int			del;
				int			sub;

				/* Calculate costs for insertion, deletion, and substitution. */
				ins = prev[i] + ins_c;
				del = curr[i - 1] + del_c;
				sub = prev[i - 1] + ((*x == *y) ? 0 : sub_c);

				/* Take the one with minimum cost. */
				curr[i] = Min(ins, del);
				curr[i] = Min(curr[i], sub);

				/* Point to next character. */
				x++;
			}
		}

		/* Swap current row with previous row. */
		temp = curr;
		curr = prev;
		prev = temp;

		/* Point to next character. */
		y += y_char_len;

#ifdef LEVENSHTEIN_LESS_EQUAL

		/*
		 * This chunk of code represents a significant performance hit if used
		 * in the case where there is no max_d bound.  This is probably not
		 * because the max_d >= 0 test itself is expensive, but rather because
		 * the possibility of needing to execute this code prevents tight
		 * optimization of the loop as a whole.
		 */
		if (max_d >= 0)
		{
			/*
			 * The "zero point" is the column of the current row where the
			 * remaining portions of the strings are of equal length.  There
			 * are (n - 1) characters in the target string, of which j have
			 * been transformed.  There are (m - 1) characters in the source
			 * string, so we want to find the value for zp where (n - 1) - j =
			 * (m - 1) - zp.
			 */
			int			zp = j - (n - m);

			/* Check whether the stop column can slide left. */
			while (stop_column > 0)
			{
				int			ii = stop_column - 1;
				int			net_inserts = ii - zp;

				if (prev[ii] + (net_inserts > 0 ? net_inserts * ins_c :
								-net_inserts * del_c) <= max_d)
					break;
				stop_column--;
			}

			/* Check whether the start column can slide right. */
			while (start_column < stop_column)
			{
				int			net_inserts = start_column - zp;

				if (prev[start_column] +
					(net_inserts > 0 ? net_inserts * ins_c :
					 -net_inserts * del_c) <= max_d)
					break;

				/*
				 * We'll never again update these values, so we must make sure
				 * there's nothing here that could confuse any future
				 * iteration of the outer loop.
				 */
				prev[start_column] = max_d + 1;
				curr[start_column] = max_d + 1;
				if (start_column != 0)
					source += (s_char_len != NULL) ? s_char_len[start_column - 1] : 1;
				start_column++;
			}

			/* If they cross, we're going to exceed the bound. */
			if (start_column >= stop_column)
				return max_d + 1;
		}
#endif
	}

	/*
	 * Because the final value was swapped from the previous row to the
	 * current row, that's where we'll find it.
	 */
	return prev[m - 1];
}

#undef MAX_LEVENSHTEIN_STRLEN
#define LEVENSHTEIN_LESS_EQUAL
static
/*-------------------------------------------------------------------------
 *
 * levenshtein.c
 *	  Levenshtein distance implementation.
 *
 * Original author:  Joe Conway <mail@joeconway.com>
 *
 * This file is included by varlena.c twice, to provide matching code for (1)
 * Levenshtein distance with custom costings, and (2) Levenshtein distance with
 * custom costings and a "max" value above which exact distances are not
 * interesting.  Before the inclusion, we rely on the presence of the inline
 * function rest_of_char_same().
 *
 * Written based on a description of the algorithm by Michael Gilleland found
 * at http://www.merriampark.com/ld.htm.  Also looked at levenshtein.c in the
 * PHP 4.0.6 distribution for inspiration.  Configurable penalty costs
 * extension is introduced by Volkan YAZICI <volkan.yazici@gmail.com.
 *
 * Copyright (c) 2001-2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	src/backend/utils/adt/levenshtein.c
 *
 *-------------------------------------------------------------------------
 */
#define MAX_LEVENSHTEIN_STRLEN		255

/*
 * Calculates Levenshtein distance metric between supplied strings, which are
 * not necessarily null-terminated.
 *
 * source: source string, of length slen bytes.
 * target: target string, of length tlen bytes.
 * ins_c, del_c, sub_c: costs to charge for character insertion, deletion,
 *		and substitution respectively; (1, 1, 1) costs suffice for common
 *		cases, but your mileage may vary.
 * max_d: if provided and >= 0, maximum distance we care about; see below.
 * trusted: caller is trusted and need not obey MAX_LEVENSHTEIN_STRLEN.
 *
 * One way to compute Levenshtein distance is to incrementally construct
 * an (m+1)x(n+1) matrix where cell (i, j) represents the minimum number
 * of operations required to transform the first i characters of s into
 * the first j characters of t.  The last column of the final row is the
 * answer.
 *
 * We use that algorithm here with some modification.  In lieu of holding
 * the entire array in memory at once, we'll just use two arrays of size
 * m+1 for storing accumulated values. At each step one array represents
 * the "previous" row and one is the "current" row of the notional large
 * array.
 *
 * If max_d >= 0, we only need to provide an accurate answer when that answer
 * is less than or equal to max_d.  From any cell in the matrix, there is
 * theoretical "minimum residual distance" from that cell to the last column
 * of the final row.  This minimum residual distance is zero when the
 * untransformed portions of the strings are of equal length (because we might
 * get lucky and find all the remaining characters matching) and is otherwise
 * based on the minimum number of insertions or deletions needed to make them
 * equal length.  The residual distance grows as we move toward the upper
 * right or lower left corners of the matrix.  When the max_d bound is
 * usefully tight, we can use this property to avoid computing the entirety
 * of each row; instead, we maintain a start_column and stop_column that
 * identify the portion of the matrix close to the diagonal which can still
 * affect the final answer.
 */
int
#ifdef LEVENSHTEIN_LESS_EQUAL
varstr_levenshtein_less_equal(const char *source, int slen,
							  const char *target, int tlen,
							  int ins_c, int del_c, int sub_c,
							  int max_d, bool trusted)
#else
varstr_levenshtein(const char *source, int slen,
				   const char *target, int tlen,
				   int ins_c, int del_c, int sub_c,
				   bool trusted)
#endif
{
	int			m,
				n;
	int		   *prev;
	int		   *curr;
	int		   *s_char_len = NULL;
	int			j;
	const char *y;
	const char *send = source + slen;
	const char *tend = target + tlen;

	/*
	 * For varstr_levenshtein_less_equal, we have real variables called
	 * start_column and stop_column; otherwise it's just short-hand for 0 and
	 * m.
	 */
#ifdef LEVENSHTEIN_LESS_EQUAL
	int			start_column,
				stop_column;

#undef START_COLUMN
#undef STOP_COLUMN
#define START_COLUMN start_column
#define STOP_COLUMN stop_column
#else
#undef START_COLUMN
#undef STOP_COLUMN
#define START_COLUMN 0
#define STOP_COLUMN m
#endif

	/* Convert string lengths (in bytes) to lengths in characters */
	m = pg_mbstrlen_with_len(source, slen);
	n = pg_mbstrlen_with_len(target, tlen);

	/*
	 * We can transform an empty s into t with n insertions, or a non-empty t
	 * into an empty s with m deletions.
	 */
	if (!m)
		return n * ins_c;
	if (!n)
		return m * del_c;

	/*
	 * For security concerns, restrict excessive CPU+RAM usage. (This
	 * implementation uses O(m) memory and has O(mn) complexity.)  If
	 * "trusted" is true, caller is responsible for not making excessive
	 * requests, typically by using a small max_d along with strings that are
	 * bounded, though not necessarily to MAX_LEVENSHTEIN_STRLEN exactly.
	 */
	if (!trusted &&
		(m > MAX_LEVENSHTEIN_STRLEN ||
		 n > MAX_LEVENSHTEIN_STRLEN))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("levenshtein argument exceeds maximum length of %d characters",
						MAX_LEVENSHTEIN_STRLEN)));

#ifdef LEVENSHTEIN_LESS_EQUAL
	/* Initialize start and stop columns. */
	start_column = 0;
	stop_column = m + 1;

	/*
	 * If max_d >= 0, determine whether the bound is impossibly tight.  If so,
	 * return max_d + 1 immediately.  Otherwise, determine whether it's tight
	 * enough to limit the computation we must perform.  If so, figure out
	 * initial stop column.
	 */
	if (max_d >= 0)
	{
		int			min_theo_d; /* Theoretical minimum distance. */
		int			max_theo_d; /* Theoretical maximum distance. */
		int			net_inserts = n - m;

		min_theo_d = net_inserts < 0 ?
			-net_inserts * del_c : net_inserts * ins_c;
		if (min_theo_d > max_d)
			return max_d + 1;
		if (ins_c + del_c < sub_c)
			sub_c = ins_c + del_c;
		max_theo_d = min_theo_d + sub_c * Min(m, n);
		if (max_d >= max_theo_d)
			max_d = -1;
		else if (ins_c + del_c > 0)
		{
			/*
			 * Figure out how much of the first row of the notional matrix we
			 * need to fill in.  If the string is growing, the theoretical
			 * minimum distance already incorporates the cost of deleting the
			 * number of characters necessary to make the two strings equal in
			 * length.  Each additional deletion forces another insertion, so
			 * the best-case total cost increases by ins_c + del_c. If the
			 * string is shrinking, the minimum theoretical cost assumes no
			 * excess deletions; that is, we're starting no further right than
			 * column n - m.  If we do start further right, the best-case
			 * total cost increases by ins_c + del_c for each move right.
			 */
			int			slack_d = max_d - min_theo_d;
			int			best_column = net_inserts < 0 ? -net_inserts : 0;

			stop_column = best_column + (slack_d / (ins_c + del_c)) + 1;
			if (stop_column > m)
				stop_column = m + 1;
		}
	}
#endif

	/*
	 * In order to avoid calling pg_mblen_range() repeatedly on each character
	 * in s, we cache all the lengths before starting the main loop -- but if
	 * all the characters in both strings are single byte, then we skip this
	 * and use a fast-path in the main loop.  If only one string contains
	 * multi-byte characters, we still build the array, so that the fast-path
	 * needn't deal with the case where the array hasn't been initialized.
	 */
	if (m != slen || n != tlen)
	{
		int			i;
		const char *cp = source;

		s_char_len = (int *) palloc((m + 1) * sizeof(int));
		for (i = 0; i < m; ++i)
		{
			s_char_len[i] = pg_mblen_range(cp, send);
			cp += s_char_len[i];
		}
		s_char_len[i] = 0;
	}

	/* One more cell for initialization column and row. */
	++m;
	++n;

	/* Previous and current rows of notional array. */
	prev = (int *) palloc(2 * m * sizeof(int));
	curr = prev + m;

	/*
	 * To transform the first i characters of s into the first 0 characters of
	 * t, we must perform i deletions.
	 */
	for (int i = START_COLUMN; i < STOP_COLUMN; i++)
		prev[i] = i * del_c;

	/* Loop through rows of the notional array */
	for (y = target, j = 1; j < n; j++)
	{
		int		   *temp;
		const char *x = source;
		int			y_char_len = n != tlen + 1 ? pg_mblen_range(y, tend) : 1;
		int			i;

#ifdef LEVENSHTEIN_LESS_EQUAL

		/*
		 * In the best case, values percolate down the diagonal unchanged, so
		 * we must increment stop_column unless it's already on the right end
		 * of the array.  The inner loop will read prev[stop_column], so we
		 * have to initialize it even though it shouldn't affect the result.
		 */
		if (stop_column < m)
		{
			prev[stop_column] = max_d + 1;
			++stop_column;
		}

		/*
		 * The main loop fills in curr, but curr[0] needs a special case: to
		 * transform the first 0 characters of s into the first j characters
		 * of t, we must perform j insertions.  However, if start_column > 0,
		 * this special case does not apply.
		 */
		if (start_column == 0)
		{
			curr[0] = j * ins_c;
			i = 1;
		}
		else
			i = start_column;
#else
		curr[0] = j * ins_c;
		i = 1;
#endif

		/*
		 * This inner loop is critical to performance, so we include a
		 * fast-path to handle the (fairly common) case where no multibyte
		 * characters are in the mix.  The fast-path is entitled to assume
		 * that if s_char_len is not initialized then BOTH strings contain
		 * only single-byte characters.
		 */
		if (s_char_len != NULL)
		{
			for (; i < STOP_COLUMN; i++)
			{
				int			ins;
				int			del;
				int			sub;
				int			x_char_len = s_char_len[i - 1];

				/*
				 * Calculate costs for insertion, deletion, and substitution.
				 *
				 * When calculating cost for substitution, we compare the last
				 * character of each possibly-multibyte character first,
				 * because that's enough to rule out most mis-matches.  If we
				 * get past that test, then we compare the lengths and the
				 * remaining bytes.
				 */
				ins = prev[i] + ins_c;
				del = curr[i - 1] + del_c;
				if (x[x_char_len - 1] == y[y_char_len - 1]
					&& x_char_len == y_char_len &&
					(x_char_len == 1 || rest_of_char_same(x, y, x_char_len)))
					sub = prev[i - 1];
				else
					sub = prev[i - 1] + sub_c;

				/* Take the one with minimum cost. */
				curr[i] = Min(ins, del);
				curr[i] = Min(curr[i], sub);

				/* Point to next character. */
				x += x_char_len;
			}
		}
		else
		{
			for (; i < STOP_COLUMN; i++)
			{
				int			ins;
				int			del;
				int			sub;

				/* Calculate costs for insertion, deletion, and substitution. */
				ins = prev[i] + ins_c;
				del = curr[i - 1] + del_c;
				sub = prev[i - 1] + ((*x == *y) ? 0 : sub_c);

				/* Take the one with minimum cost. */
				curr[i] = Min(ins, del);
				curr[i] = Min(curr[i], sub);

				/* Point to next character. */
				x++;
			}
		}

		/* Swap current row with previous row. */
		temp = curr;
		curr = prev;
		prev = temp;

		/* Point to next character. */
		y += y_char_len;

#ifdef LEVENSHTEIN_LESS_EQUAL

		/*
		 * This chunk of code represents a significant performance hit if used
		 * in the case where there is no max_d bound.  This is probably not
		 * because the max_d >= 0 test itself is expensive, but rather because
		 * the possibility of needing to execute this code prevents tight
		 * optimization of the loop as a whole.
		 */
		if (max_d >= 0)
		{
			/*
			 * The "zero point" is the column of the current row where the
			 * remaining portions of the strings are of equal length.  There
			 * are (n - 1) characters in the target string, of which j have
			 * been transformed.  There are (m - 1) characters in the source
			 * string, so we want to find the value for zp where (n - 1) - j =
			 * (m - 1) - zp.
			 */
			int			zp = j - (n - m);

			/* Check whether the stop column can slide left. */
			while (stop_column > 0)
			{
				int			ii = stop_column - 1;
				int			net_inserts = ii - zp;

				if (prev[ii] + (net_inserts > 0 ? net_inserts * ins_c :
								-net_inserts * del_c) <= max_d)
					break;
				stop_column--;
			}

			/* Check whether the start column can slide right. */
			while (start_column < stop_column)
			{
				int			net_inserts = start_column - zp;

				if (prev[start_column] +
					(net_inserts > 0 ? net_inserts * ins_c :
					 -net_inserts * del_c) <= max_d)
					break;

				/*
				 * We'll never again update these values, so we must make sure
				 * there's nothing here that could confuse any future
				 * iteration of the outer loop.
				 */
				prev[start_column] = max_d + 1;
				curr[start_column] = max_d + 1;
				if (start_column != 0)
					source += (s_char_len != NULL) ? s_char_len[start_column - 1] : 1;
				start_column++;
			}

			/* If they cross, we're going to exceed the bound. */
			if (start_column >= stop_column)
				return max_d + 1;
		}
#endif
	}

	/*
	 * Because the final value was swapped from the previous row to the
	 * current row, that's where we'll find it.
	 */
	return prev[m - 1];
}

/* ================= SECTION 3: fuzzystrmatch.c ================= */

/* ---- VERBATIM contrib/fuzzystrmatch/fuzzystrmatch.c lines 51-70 ---- */

/*
 * Soundex
 */
static void _soundex(const char *instr, char *outstr);

#define SOUNDEX_LEN 4

/*									ABCDEFGHIJKLMNOPQRSTUVWXYZ */
static const char *const soundex_table = "01230120022455012623010202";

static char
soundex_code(char letter)
{
	letter = toupper((unsigned char) letter);
	/* Defend against non-ASCII letters */
	if (letter >= 'A' && letter <= 'Z')
		return soundex_table[letter - 'A'];
	return letter;
}

/* ---- VERBATIM contrib/fuzzystrmatch/fuzzystrmatch.c lines 71-75 ---- */

/*
 * Metaphone
 */
#define MAX_METAPHONE_STRLEN		255

/* ---- VERBATIM contrib/fuzzystrmatch/fuzzystrmatch.c lines 105-147 ---- */
/* Special encodings */
#define  SH		'X'
#define  TH		'0'

static char Lookahead(char *word, int how_far);
static void _metaphone(char *word, int max_phonemes, char **phoned_word);

/* Metachar.h ... little bits about characters for metaphone */


/*-- Character encoding array & accessing macros --*/
/* Stolen directly out of the book... */
static const char _codes[26] = {
	1, 16, 4, 16, 9, 2, 4, 16, 9, 2, 0, 2, 2, 2, 1, 4, 0, 2, 4, 4, 1, 0, 0, 0, 8, 0
/*	a  b c	d e f g  h i j k l m n o p q r s t u v w x y z */
};

static int
getcode(char c)
{
	if (isalpha((unsigned char) c))
	{
		c = toupper((unsigned char) c);
		/* Defend against non-ASCII letters */
		if (c >= 'A' && c <= 'Z')
			return _codes[c - 'A'];
	}
	return 0;
}

#define isvowel(c)	(getcode(c) & 1)	/* AEIOU */

/* These letters are passed through unchanged */
#define NOCHANGE(c) (getcode(c) & 2)	/* FJMNR */

/* These form diphthongs when preceding H */
#define AFFECTH(c)	(getcode(c) & 4)	/* CGPST */

/* These make C and G soft */
#define MAKESOFT(c) (getcode(c) & 8)	/* EIY */

/* These prevent GH from becoming F */
#define NOGHTOF(c)	(getcode(c) & 16)	/* BDH */

/* ---- VERBATIM contrib/fuzzystrmatch/fuzzystrmatch.c lines 300-343 ---- */
/* I suppose I could have been using a character pointer instead of
 * accessing the array directly... */

/* Look at the next letter in the word */
#define Next_Letter (toupper((unsigned char) word[w_idx+1]))
/* Look at the current letter in the word */
#define Curr_Letter (toupper((unsigned char) word[w_idx]))
/* Go N letters back. */
#define Look_Back_Letter(n) \
	(w_idx >= (n) ? toupper((unsigned char) word[w_idx-(n)]) : '\0')
/* Previous letter.  I dunno, should this return null on failure? */
#define Prev_Letter (Look_Back_Letter(1))
/* Look two letters down.  It makes sure you don't walk off the string. */
#define After_Next_Letter \
	(Next_Letter != '\0' ? toupper((unsigned char) word[w_idx+2]) : '\0')
#define Look_Ahead_Letter(n) toupper((unsigned char) Lookahead(word+w_idx, n))


/* Allows us to safely look ahead an arbitrary # of letters */
/* I probably could have just used strlen... */
static char
Lookahead(char *word, int how_far)
{
	char		letter_ahead = '\0';	/* null by default */
	int			idx;

	for (idx = 0; word[idx] != '\0' && idx < how_far; idx++);
	/* Edge forward in the string... */

	letter_ahead = word[idx];	/* idx will be either == to how_far or at the
								 * end of the string */
	return letter_ahead;
}


/* phonize one letter */
#define Phonize(c)	do {(*phoned_word)[p_idx++] = c;} while (0)
/* Slap a null character on the end of the phoned word */
#define End_Phoned_Word do {(*phoned_word)[p_idx] = '\0';} while (0)
/* How long is the phoned word? */
#define Phone_Len	(p_idx)

/* Note is a letter is a 'break' in the word */
#define Isbreak(c)	(!isalpha((unsigned char) (c)))

/* ---- VERBATIM contrib/fuzzystrmatch/fuzzystrmatch.c lines 346-704
 * (_metaphone) ---- */
static void
_metaphone(char *word,			/* IN */
		   int max_phonemes,
		   char **phoned_word)	/* OUT */
{
	int			w_idx = 0;		/* point in the phonization we're at. */
	int			p_idx = 0;		/* end of the phoned phrase */

	/*-- Parameter checks --*/

	/*
	 * Shouldn't be necessary, but left these here anyway jec Aug 3, 2001
	 */

	/* Negative phoneme length is meaningless */
	if (!(max_phonemes > 0))
		/* internal error */
		elog(ERROR, "metaphone: Requested output length must be > 0");

	/* Empty/null string is meaningless */
	if ((word == NULL) || !(strlen(word) > 0))
		/* internal error */
		elog(ERROR, "metaphone: Input string length must be > 0");

	/*-- Allocate memory for our phoned_phrase --*/
	if (max_phonemes == 0)
	{							/* Assume largest possible */
		*phoned_word = palloc(sizeof(char) * strlen(word) + 1);
	}
	else
	{
		*phoned_word = palloc(sizeof(char) * max_phonemes + 1);
	}

	/*-- The first phoneme has to be processed specially. --*/
	/* Find our first letter */
	for (; !isalpha((unsigned char) (Curr_Letter)); w_idx++)
	{
		/* On the off chance we were given nothing but crap... */
		if (Curr_Letter == '\0')
		{
			End_Phoned_Word;
			return;
		}
	}

	switch (Curr_Letter)
	{
			/* AE becomes E */
		case 'A':
			if (Next_Letter == 'E')
			{
				Phonize('E');
				w_idx += 2;
			}
			/* Remember, preserve vowels at the beginning */
			else
			{
				Phonize('A');
				w_idx++;
			}
			break;
			/* [GKP]N becomes N */
		case 'G':
		case 'K':
		case 'P':
			if (Next_Letter == 'N')
			{
				Phonize('N');
				w_idx += 2;
			}
			break;

			/*
			 * WH becomes H, WR becomes R W if followed by a vowel
			 */
		case 'W':
			if (Next_Letter == 'H' ||
				Next_Letter == 'R')
			{
				Phonize(Next_Letter);
				w_idx += 2;
			}
			else if (isvowel(Next_Letter))
			{
				Phonize('W');
				w_idx += 2;
			}
			/* else ignore */
			break;
			/* X becomes S */
		case 'X':
			Phonize('S');
			w_idx++;
			break;
			/* Vowels are kept */

			/*
			 * We did A already case 'A': case 'a':
			 */
		case 'E':
		case 'I':
		case 'O':
		case 'U':
			Phonize(Curr_Letter);
			w_idx++;
			break;
		default:
			/* do nothing */
			break;
	}



	/* On to the metaphoning */
	for (; Curr_Letter != '\0' &&
		 (max_phonemes == 0 || Phone_Len < max_phonemes);
		 w_idx++)
	{
		/*
		 * How many letters to skip because an earlier encoding handled
		 * multiple letters
		 */
		unsigned short int skip_letter = 0;


		/*
		 * THOUGHT:  It would be nice if, rather than having things like...
		 * well, SCI.  For SCI you encode the S, then have to remember to skip
		 * the C.  So the phonome SCI invades both S and C.  It would be
		 * better, IMHO, to skip the C from the S part of the encoding. Hell,
		 * I'm trying it.
		 */

		/* Ignore non-alphas */
		if (!isalpha((unsigned char) (Curr_Letter)))
			continue;

		/* Drop duplicates, except CC */
		if (Curr_Letter == Prev_Letter &&
			Curr_Letter != 'C')
			continue;

		switch (Curr_Letter)
		{
				/* B -> B unless in MB */
			case 'B':
				if (Prev_Letter != 'M')
					Phonize('B');
				break;

				/*
				 * 'sh' if -CIA- or -CH, but not SCH, except SCHW. (SCHW is
				 * handled in S) S if -CI-, -CE- or -CY- dropped if -SCI-,
				 * SCE-, -SCY- (handed in S) else K
				 */
			case 'C':
				if (MAKESOFT(Next_Letter))
				{				/* C[IEY] */
					if (After_Next_Letter == 'A' &&
						Next_Letter == 'I')
					{			/* CIA */
						Phonize(SH);
					}
					/* SC[IEY] */
					else if (Prev_Letter == 'S')
					{
						/* Dropped */
					}
					else
						Phonize('S');
				}
				else if (Next_Letter == 'H')
				{
#ifndef USE_TRADITIONAL_METAPHONE
					if (After_Next_Letter == 'R' ||
						Prev_Letter == 'S')
					{			/* Christ, School */
						Phonize('K');
					}
					else
						Phonize(SH);
#else
					Phonize(SH);
#endif
					skip_letter++;
				}
				else
					Phonize('K');
				break;

				/*
				 * J if in -DGE-, -DGI- or -DGY- else T
				 */
			case 'D':
				if (Next_Letter == 'G' &&
					MAKESOFT(After_Next_Letter))
				{
					Phonize('J');
					skip_letter++;
				}
				else
					Phonize('T');
				break;

				/*
				 * F if in -GH and not B--GH, D--GH, -H--GH, -H---GH else
				 * dropped if -GNED, -GN, else dropped if -DGE-, -DGI- or
				 * -DGY- (handled in D) else J if in -GE-, -GI, -GY and not GG
				 * else K
				 */
			case 'G':
				if (Next_Letter == 'H')
				{
					if (!(NOGHTOF(Look_Back_Letter(3)) ||
						  Look_Back_Letter(4) == 'H'))
					{
						Phonize('F');
						skip_letter++;
					}
					else
					{
						/* silent */
					}
				}
				else if (Next_Letter == 'N')
				{
					if (Isbreak(After_Next_Letter) ||
						(After_Next_Letter == 'E' &&
						 Look_Ahead_Letter(3) == 'D'))
					{
						/* dropped */
					}
					else
						Phonize('K');
				}
				else if (MAKESOFT(Next_Letter) &&
						 Prev_Letter != 'G')
					Phonize('J');
				else
					Phonize('K');
				break;
				/* H if before a vowel and not after C,G,P,S,T */
			case 'H':
				if (isvowel(Next_Letter) &&
					!AFFECTH(Prev_Letter))
					Phonize('H');
				break;

				/*
				 * dropped if after C else K
				 */
			case 'K':
				if (Prev_Letter != 'C')
					Phonize('K');
				break;

				/*
				 * F if before H else P
				 */
			case 'P':
				if (Next_Letter == 'H')
					Phonize('F');
				else
					Phonize('P');
				break;

				/*
				 * K
				 */
			case 'Q':
				Phonize('K');
				break;

				/*
				 * 'sh' in -SH-, -SIO- or -SIA- or -SCHW- else S
				 */
			case 'S':
				if (Next_Letter == 'I' &&
					(After_Next_Letter == 'O' ||
					 After_Next_Letter == 'A'))
					Phonize(SH);
				else if (Next_Letter == 'H')
				{
					Phonize(SH);
					skip_letter++;
				}
#ifndef USE_TRADITIONAL_METAPHONE
				else if (Next_Letter == 'C' &&
						 Look_Ahead_Letter(2) == 'H' &&
						 Look_Ahead_Letter(3) == 'W')
				{
					Phonize(SH);
					skip_letter += 2;
				}
#endif
				else
					Phonize('S');
				break;

				/*
				 * 'sh' in -TIA- or -TIO- else 'th' before H else T
				 */
			case 'T':
				if (Next_Letter == 'I' &&
					(After_Next_Letter == 'O' ||
					 After_Next_Letter == 'A'))
					Phonize(SH);
				else if (Next_Letter == 'H')
				{
					Phonize(TH);
					skip_letter++;
				}
				else
					Phonize('T');
				break;
				/* F */
			case 'V':
				Phonize('F');
				break;
				/* W before a vowel, else dropped */
			case 'W':
				if (isvowel(Next_Letter))
					Phonize('W');
				break;
				/* KS */
			case 'X':
				Phonize('K');
				if (max_phonemes == 0 || Phone_Len < max_phonemes)
					Phonize('S');
				break;
				/* Y if followed by a vowel */
			case 'Y':
				if (isvowel(Next_Letter))
					Phonize('Y');
				break;
				/* S */
			case 'Z':
				Phonize('S');
				break;
				/* No transformation */
			case 'F':
			case 'J':
			case 'L':
			case 'M':
			case 'N':
			case 'R':
				Phonize(Curr_Letter);
				break;
			default:
				/* nothing */
				break;
		}						/* END SWITCH */

		w_idx += skip_letter;
	}							/* END FOR */

	End_Phoned_Word;
}								/* END metaphone */

/* ---- VERBATIM contrib/fuzzystrmatch/fuzzystrmatch.c lines 725-773
 * (_soundex) ---- */
static void
_soundex(const char *instr, char *outstr)
{
	int			count;

	Assert(instr);
	Assert(outstr);

	/* Skip leading non-alphabetic characters */
	while (*instr && !isalpha((unsigned char) *instr))
		++instr;

	/* If no string left, return all-zeroes buffer */
	if (!*instr)
	{
		memset(outstr, '\0', SOUNDEX_LEN + 1);
		return;
	}

	/* Take the first letter as is */
	*outstr++ = (char) toupper((unsigned char) *instr++);

	count = 1;
	while (*instr && count < SOUNDEX_LEN)
	{
		if (isalpha((unsigned char) *instr) &&
			soundex_code(*instr) != soundex_code(*(instr - 1)))
		{
			*outstr = soundex_code(*instr);
			if (*outstr != '0')
			{
				++outstr;
				++count;
			}
		}
		++instr;
	}

	/* Fill with 0's */
	while (count < SOUNDEX_LEN)
	{
		*outstr = '0';
		++outstr;
		++count;
	}

	/* And null-terminate */
	*outstr = '\0';
}

/* ================= SECTION 4: dmetaphone.c ================= */

/* ---- VERBATIM contrib/fuzzystrmatch/dmetaphone.c lines 183-200 (the
 * palloc arm — DMETAPHONE_MAIN undefined, exactly the PG build) ---- */
/* here is where we start the code imported from the perl module */

/* all memory handling is done with these macros */

#define META_MALLOC(v,n,t) \
		  (v = (t*)palloc(((n)*sizeof(t))))

#define META_REALLOC(v,n,t) \
					  (v = (t*)repalloc((v),((n)*sizeof(t))))

/*
 * Don't do pfree - it seems to cause a SIGSEGV sometimes - which might have just
 * been caused by reloading the module in development.
 * So we rely on context cleanup - Tom Lane says pfree shouldn't be necessary
 * in a case like this.
 */

#define META_FREE(x) ((void)true)	/* pfree((x)) */

/* prototype (dmetaphone.c line 120 verbatim) */
static void DoubleMetaphone(char *str, char **codes);

/* ---- VERBATIM contrib/fuzzystrmatch/dmetaphone.c lines 216-1421
 * (metastring + all statics + DoubleMetaphone) ---- */
/* this typedef was originally in the perl module's .h file */

typedef struct
{
	char	   *str;
	int			length;
	int			bufsize;
	int			free_string_on_destroy;
}

metastring;

/*
 * remaining perl module funcs unchanged except for declaring them static
 * and reformatting to PostgreSQL indentation and to fit in 80 cols.
 *
 */

static metastring *
NewMetaString(const char *init_str)
{
	metastring *s;
	char		empty_string[] = "";

	META_MALLOC(s, 1, metastring);
	assert(s != NULL);

	if (init_str == NULL)
		init_str = empty_string;
	s->length = strlen(init_str);
	/* preallocate a bit more for potential growth */
	s->bufsize = s->length + 7;

	META_MALLOC(s->str, s->bufsize, char);
	assert(s->str != NULL);

	memcpy(s->str, init_str, s->length + 1);
	s->free_string_on_destroy = 1;

	return s;
}


static void
DestroyMetaString(metastring *s)
{
	if (s == NULL)
		return;

	if (s->free_string_on_destroy && (s->str != NULL))
		META_FREE(s->str);

	META_FREE(s);
}


static void
IncreaseBuffer(metastring *s, int chars_needed)
{
	META_REALLOC(s->str, (s->bufsize + chars_needed + 10), char);
	assert(s->str != NULL);
	s->bufsize = s->bufsize + chars_needed + 10;
}


static void
MakeUpper(metastring *s)
{
	char	   *i;

	for (i = s->str; *i; i++)
		*i = toupper((unsigned char) *i);
}


static int
IsVowel(metastring *s, int pos)
{
	char		c;

	if ((pos < 0) || (pos >= s->length))
		return 0;

	c = *(s->str + pos);
	if ((c == 'A') || (c == 'E') || (c == 'I') || (c == 'O') ||
		(c == 'U') || (c == 'Y'))
		return 1;

	return 0;
}


static int
SlavoGermanic(metastring *s)
{
	if (strstr(s->str, "W"))
		return 1;
	else if (strstr(s->str, "K"))
		return 1;
	else if (strstr(s->str, "CZ"))
		return 1;
	else if (strstr(s->str, "WITZ"))
		return 1;
	else
		return 0;
}


static char
GetAt(metastring *s, int pos)
{
	if ((pos < 0) || (pos >= s->length))
		return '\0';

	return ((char) *(s->str + pos));
}


static void
SetAt(metastring *s, int pos, char c)
{
	if ((pos < 0) || (pos >= s->length))
		return;

	*(s->str + pos) = c;
}


/*
   Caveats: the START value is 0 based
*/
static int
StringAt(metastring *s, int start, int length,...)
{
	char	   *test;
	char	   *pos;
	va_list		ap;

	if ((start < 0) || (start >= s->length))
		return 0;

	pos = (s->str + start);
	va_start(ap, length);

	do
	{
		test = va_arg(ap, char *);
		if (*test && (strncmp(pos, test, length) == 0))
		{
			va_end(ap);
			return 1;
		}
	}
	while (strcmp(test, "") != 0);

	va_end(ap);

	return 0;
}


static void
MetaphAdd(metastring *s, const char *new_str)
{
	int			add_length;

	if (new_str == NULL)
		return;

	add_length = strlen(new_str);
	if ((s->length + add_length) > (s->bufsize - 1))
		IncreaseBuffer(s, add_length);

	strcat(s->str, new_str);
	s->length += add_length;
}


static void
DoubleMetaphone(char *str, char **codes)
{
	int			length;
	metastring *original;
	metastring *primary;
	metastring *secondary;
	int			current;
	int			last;

	current = 0;
	/* we need the real length and last prior to padding */
	length = strlen(str);
	last = length - 1;
	original = NewMetaString(str);
	/* Pad original so we can index beyond end */
	MetaphAdd(original, "     ");

	primary = NewMetaString("");
	secondary = NewMetaString("");
	primary->free_string_on_destroy = 0;
	secondary->free_string_on_destroy = 0;

	MakeUpper(original);

	/* skip these when at start of word */
	if (StringAt(original, 0, 2, "GN", "KN", "PN", "WR", "PS", ""))
		current += 1;

	/* Initial 'X' is pronounced 'Z' e.g. 'Xavier' */
	if (GetAt(original, 0) == 'X')
	{
		MetaphAdd(primary, "S");	/* 'Z' maps to 'S' */
		MetaphAdd(secondary, "S");
		current += 1;
	}

	/* main loop */
	while ((primary->length < 4) || (secondary->length < 4))
	{
		if (current >= length)
			break;

		switch (GetAt(original, current))
		{
			case 'A':
			case 'E':
			case 'I':
			case 'O':
			case 'U':
			case 'Y':
				if (current == 0)
				{
					/* all init vowels now map to 'A' */
					MetaphAdd(primary, "A");
					MetaphAdd(secondary, "A");
				}
				current += 1;
				break;

			case 'B':

				/* "-mb", e.g", "dumb", already skipped over... */
				MetaphAdd(primary, "P");
				MetaphAdd(secondary, "P");

				if (GetAt(original, current + 1) == 'B')
					current += 2;
				else
					current += 1;
				break;

			case '\xc7':		/* C with cedilla */
				MetaphAdd(primary, "S");
				MetaphAdd(secondary, "S");
				current += 1;
				break;

			case 'C':
				/* various germanic */
				if ((current > 1)
					&& !IsVowel(original, current - 2)
					&& StringAt(original, (current - 1), 3, "ACH", "")
					&& ((GetAt(original, current + 2) != 'I')
						&& ((GetAt(original, current + 2) != 'E')
							|| StringAt(original, (current - 2), 6, "BACHER",
										"MACHER", ""))))
				{
					MetaphAdd(primary, "K");
					MetaphAdd(secondary, "K");
					current += 2;
					break;
				}

				/* special case 'caesar' */
				if ((current == 0)
					&& StringAt(original, current, 6, "CAESAR", ""))
				{
					MetaphAdd(primary, "S");
					MetaphAdd(secondary, "S");
					current += 2;
					break;
				}

				/* italian 'chianti' */
				if (StringAt(original, current, 4, "CHIA", ""))
				{
					MetaphAdd(primary, "K");
					MetaphAdd(secondary, "K");
					current += 2;
					break;
				}

				if (StringAt(original, current, 2, "CH", ""))
				{
					/* find 'michael' */
					if ((current > 0)
						&& StringAt(original, current, 4, "CHAE", ""))
					{
						MetaphAdd(primary, "K");
						MetaphAdd(secondary, "X");
						current += 2;
						break;
					}

					/* greek roots e.g. 'chemistry', 'chorus' */
					if ((current == 0)
						&& (StringAt(original, (current + 1), 5,
									 "HARAC", "HARIS", "")
							|| StringAt(original, (current + 1), 3, "HOR",
										"HYM", "HIA", "HEM", ""))
						&& !StringAt(original, 0, 5, "CHORE", ""))
					{
						MetaphAdd(primary, "K");
						MetaphAdd(secondary, "K");
						current += 2;
						break;
					}

					/* germanic, greek, or otherwise 'ch' for 'kh' sound */
					if ((StringAt(original, 0, 4, "VAN ", "VON ", "")
						 || StringAt(original, 0, 3, "SCH", ""))
					/* 'architect but not 'arch', 'orchestra', 'orchid' */
						|| StringAt(original, (current - 2), 6, "ORCHES",
									"ARCHIT", "ORCHID", "")
						|| StringAt(original, (current + 2), 1, "T", "S",
									"")
						|| ((StringAt(original, (current - 1), 1,
									  "A", "O", "U", "E", "")
							 || (current == 0))

					/*
					 * e.g., 'wachtler', 'wechsler', but not 'tichner'
					 */
							&& StringAt(original, (current + 2), 1, "L", "R",
										"N", "M", "B", "H", "F", "V", "W",
										" ", "")))
					{
						MetaphAdd(primary, "K");
						MetaphAdd(secondary, "K");
					}
					else
					{
						if (current > 0)
						{
							if (StringAt(original, 0, 2, "MC", ""))
							{
								/* e.g., "McHugh" */
								MetaphAdd(primary, "K");
								MetaphAdd(secondary, "K");
							}
							else
							{
								MetaphAdd(primary, "X");
								MetaphAdd(secondary, "K");
							}
						}
						else
						{
							MetaphAdd(primary, "X");
							MetaphAdd(secondary, "X");
						}
					}
					current += 2;
					break;
				}
				/* e.g, 'czerny' */
				if (StringAt(original, current, 2, "CZ", "")
					&& !StringAt(original, (current - 2), 4, "WICZ", ""))
				{
					MetaphAdd(primary, "S");
					MetaphAdd(secondary, "X");
					current += 2;
					break;
				}

				/* e.g., 'focaccia' */
				if (StringAt(original, (current + 1), 3, "CIA", ""))
				{
					MetaphAdd(primary, "X");
					MetaphAdd(secondary, "X");
					current += 3;
					break;
				}

				/* double 'C', but not if e.g. 'McClellan' */
				if (StringAt(original, current, 2, "CC", "")
					&& !((current == 1) && (GetAt(original, 0) == 'M')))
				{
					/* 'bellocchio' but not 'bacchus' */
					if (StringAt(original, (current + 2), 1, "I", "E", "H", "")
						&& !StringAt(original, (current + 2), 2, "HU", ""))
					{
						/* 'accident', 'accede' 'succeed' */
						if (((current == 1)
							 && (GetAt(original, current - 1) == 'A'))
							|| StringAt(original, (current - 1), 5, "UCCEE",
										"UCCES", ""))
						{
							MetaphAdd(primary, "KS");
							MetaphAdd(secondary, "KS");
							/* 'bacci', 'bertucci', other italian */
						}
						else
						{
							MetaphAdd(primary, "X");
							MetaphAdd(secondary, "X");
						}
						current += 3;
						break;
					}
					else
					{			/* Pierce's rule */
						MetaphAdd(primary, "K");
						MetaphAdd(secondary, "K");
						current += 2;
						break;
					}
				}

				if (StringAt(original, current, 2, "CK", "CG", "CQ", ""))
				{
					MetaphAdd(primary, "K");
					MetaphAdd(secondary, "K");
					current += 2;
					break;
				}

				if (StringAt(original, current, 2, "CI", "CE", "CY", ""))
				{
					/* italian vs. english */
					if (StringAt
						(original, current, 3, "CIO", "CIE", "CIA", ""))
					{
						MetaphAdd(primary, "S");
						MetaphAdd(secondary, "X");
					}
					else
					{
						MetaphAdd(primary, "S");
						MetaphAdd(secondary, "S");
					}
					current += 2;
					break;
				}

				/* else */
				MetaphAdd(primary, "K");
				MetaphAdd(secondary, "K");

				/* name sent in 'mac caffrey', 'mac gregor */
				if (StringAt(original, (current + 1), 2, " C", " Q", " G", ""))
					current += 3;
				else if (StringAt(original, (current + 1), 1, "C", "K", "Q", "")
						 && !StringAt(original, (current + 1), 2,
									  "CE", "CI", ""))
					current += 2;
				else
					current += 1;
				break;

			case 'D':
				if (StringAt(original, current, 2, "DG", ""))
				{
					if (StringAt(original, (current + 2), 1,
								 "I", "E", "Y", ""))
					{
						/* e.g. 'edge' */
						MetaphAdd(primary, "J");
						MetaphAdd(secondary, "J");
						current += 3;
						break;
					}
					else
					{
						/* e.g. 'edgar' */
						MetaphAdd(primary, "TK");
						MetaphAdd(secondary, "TK");
						current += 2;
						break;
					}
				}

				if (StringAt(original, current, 2, "DT", "DD", ""))
				{
					MetaphAdd(primary, "T");
					MetaphAdd(secondary, "T");
					current += 2;
					break;
				}

				/* else */
				MetaphAdd(primary, "T");
				MetaphAdd(secondary, "T");
				current += 1;
				break;

			case 'F':
				if (GetAt(original, current + 1) == 'F')
					current += 2;
				else
					current += 1;
				MetaphAdd(primary, "F");
				MetaphAdd(secondary, "F");
				break;

			case 'G':
				if (GetAt(original, current + 1) == 'H')
				{
					if ((current > 0) && !IsVowel(original, current - 1))
					{
						MetaphAdd(primary, "K");
						MetaphAdd(secondary, "K");
						current += 2;
						break;
					}

					if (current < 3)
					{
						/* 'ghislane', ghiradelli */
						if (current == 0)
						{
							if (GetAt(original, current + 2) == 'I')
							{
								MetaphAdd(primary, "J");
								MetaphAdd(secondary, "J");
							}
							else
							{
								MetaphAdd(primary, "K");
								MetaphAdd(secondary, "K");
							}
							current += 2;
							break;
						}
					}

					/*
					 * Parker's rule (with some further refinements) - e.g.,
					 * 'hugh'
					 */
					if (((current > 1)
						 && StringAt(original, (current - 2), 1,
									 "B", "H", "D", ""))
					/* e.g., 'bough' */
						|| ((current > 2)
							&& StringAt(original, (current - 3), 1,
										"B", "H", "D", ""))
					/* e.g., 'broughton' */
						|| ((current > 3)
							&& StringAt(original, (current - 4), 1,
										"B", "H", "")))
					{
						current += 2;
						break;
					}
					else
					{
						/*
						 * e.g., 'laugh', 'McLaughlin', 'cough', 'gough',
						 * 'rough', 'tough'
						 */
						if ((current > 2)
							&& (GetAt(original, current - 1) == 'U')
							&& StringAt(original, (current - 3), 1, "C",
										"G", "L", "R", "T", ""))
						{
							MetaphAdd(primary, "F");
							MetaphAdd(secondary, "F");
						}
						else if ((current > 0)
								 && GetAt(original, current - 1) != 'I')
						{


							MetaphAdd(primary, "K");
							MetaphAdd(secondary, "K");
						}

						current += 2;
						break;
					}
				}

				if (GetAt(original, current + 1) == 'N')
				{
					if ((current == 1) && IsVowel(original, 0)
						&& !SlavoGermanic(original))
					{
						MetaphAdd(primary, "KN");
						MetaphAdd(secondary, "N");
					}
					else
						/* not e.g. 'cagney' */
						if (!StringAt(original, (current + 2), 2, "EY", "")
							&& (GetAt(original, current + 1) != 'Y')
							&& !SlavoGermanic(original))
					{
						MetaphAdd(primary, "N");
						MetaphAdd(secondary, "KN");
					}
					else
					{
						MetaphAdd(primary, "KN");
						MetaphAdd(secondary, "KN");
					}
					current += 2;
					break;
				}

				/* 'tagliaro' */
				if (StringAt(original, (current + 1), 2, "LI", "")
					&& !SlavoGermanic(original))
				{
					MetaphAdd(primary, "KL");
					MetaphAdd(secondary, "L");
					current += 2;
					break;
				}

				/* -ges-,-gep-,-gel-, -gie- at beginning */
				if ((current == 0)
					&& ((GetAt(original, current + 1) == 'Y')
						|| StringAt(original, (current + 1), 2, "ES", "EP",
									"EB", "EL", "EY", "IB", "IL", "IN", "IE",
									"EI", "ER", "")))
				{
					MetaphAdd(primary, "K");
					MetaphAdd(secondary, "J");
					current += 2;
					break;
				}

				/* -ger-,  -gy- */
				if ((StringAt(original, (current + 1), 2, "ER", "")
					 || (GetAt(original, current + 1) == 'Y'))
					&& !StringAt(original, 0, 6,
								 "DANGER", "RANGER", "MANGER", "")
					&& !StringAt(original, (current - 1), 1, "E", "I", "")
					&& !StringAt(original, (current - 1), 3, "RGY", "OGY", ""))
				{
					MetaphAdd(primary, "K");
					MetaphAdd(secondary, "J");
					current += 2;
					break;
				}

				/* italian e.g, 'biaggi' */
				if (StringAt(original, (current + 1), 1, "E", "I", "Y", "")
					|| StringAt(original, (current - 1), 4,
								"AGGI", "OGGI", ""))
				{
					/* obvious germanic */
					if ((StringAt(original, 0, 4, "VAN ", "VON ", "")
						 || StringAt(original, 0, 3, "SCH", ""))
						|| StringAt(original, (current + 1), 2, "ET", ""))
					{
						MetaphAdd(primary, "K");
						MetaphAdd(secondary, "K");
					}
					else
					{
						/* always soft if french ending */
						if (StringAt
							(original, (current + 1), 4, "IER ", ""))
						{
							MetaphAdd(primary, "J");
							MetaphAdd(secondary, "J");
						}
						else
						{
							MetaphAdd(primary, "J");
							MetaphAdd(secondary, "K");
						}
					}
					current += 2;
					break;
				}

				if (GetAt(original, current + 1) == 'G')
					current += 2;
				else
					current += 1;
				MetaphAdd(primary, "K");
				MetaphAdd(secondary, "K");
				break;

			case 'H':
				/* only keep if first & before vowel or btw. 2 vowels */
				if (((current == 0) || IsVowel(original, current - 1))
					&& IsVowel(original, current + 1))
				{
					MetaphAdd(primary, "H");
					MetaphAdd(secondary, "H");
					current += 2;
				}
				else
					/* also takes care of 'HH' */
					current += 1;
				break;

			case 'J':
				/* obvious spanish, 'jose', 'san jacinto' */
				if (StringAt(original, current, 4, "JOSE", "")
					|| StringAt(original, 0, 4, "SAN ", ""))
				{
					if (((current == 0)
						 && (GetAt(original, current + 4) == ' '))
						|| StringAt(original, 0, 4, "SAN ", ""))
					{
						MetaphAdd(primary, "H");
						MetaphAdd(secondary, "H");
					}
					else
					{
						MetaphAdd(primary, "J");
						MetaphAdd(secondary, "H");
					}
					current += 1;
					break;
				}

				if ((current == 0)
					&& !StringAt(original, current, 4, "JOSE", ""))
				{
					MetaphAdd(primary, "J");	/* Yankelovich/Jankelowicz */
					MetaphAdd(secondary, "A");
				}
				else
				{
					/* spanish pron. of e.g. 'bajador' */
					if (IsVowel(original, current - 1)
						&& !SlavoGermanic(original)
						&& ((GetAt(original, current + 1) == 'A')
							|| (GetAt(original, current + 1) == 'O')))
					{
						MetaphAdd(primary, "J");
						MetaphAdd(secondary, "H");
					}
					else
					{
						if (current == last)
						{
							MetaphAdd(primary, "J");
							MetaphAdd(secondary, "");
						}
						else
						{
							if (!StringAt(original, (current + 1), 1, "L", "T",
										  "K", "S", "N", "M", "B", "Z", "")
								&& !StringAt(original, (current - 1), 1,
											 "S", "K", "L", ""))
							{
								MetaphAdd(primary, "J");
								MetaphAdd(secondary, "J");
							}
						}
					}
				}

				if (GetAt(original, current + 1) == 'J')	/* it could happen! */
					current += 2;
				else
					current += 1;
				break;

			case 'K':
				if (GetAt(original, current + 1) == 'K')
					current += 2;
				else
					current += 1;
				MetaphAdd(primary, "K");
				MetaphAdd(secondary, "K");
				break;

			case 'L':
				if (GetAt(original, current + 1) == 'L')
				{
					/* spanish e.g. 'cabrillo', 'gallegos' */
					if (((current == (length - 3))
						 && StringAt(original, (current - 1), 4, "ILLO",
									 "ILLA", "ALLE", ""))
						|| ((StringAt(original, (last - 1), 2, "AS", "OS", "")
							 || StringAt(original, last, 1, "A", "O", ""))
							&& StringAt(original, (current - 1), 4,
										"ALLE", "")))
					{
						MetaphAdd(primary, "L");
						MetaphAdd(secondary, "");
						current += 2;
						break;
					}
					current += 2;
				}
				else
					current += 1;
				MetaphAdd(primary, "L");
				MetaphAdd(secondary, "L");
				break;

			case 'M':
				if ((StringAt(original, (current - 1), 3, "UMB", "")
					 && (((current + 1) == last)
						 || StringAt(original, (current + 2), 2, "ER", "")))
				/* 'dumb','thumb' */
					|| (GetAt(original, current + 1) == 'M'))
					current += 2;
				else
					current += 1;
				MetaphAdd(primary, "M");
				MetaphAdd(secondary, "M");
				break;

			case 'N':
				if (GetAt(original, current + 1) == 'N')
					current += 2;
				else
					current += 1;
				MetaphAdd(primary, "N");
				MetaphAdd(secondary, "N");
				break;

			case '\xd1':		/* N with tilde */
				current += 1;
				MetaphAdd(primary, "N");
				MetaphAdd(secondary, "N");
				break;

			case 'P':
				if (GetAt(original, current + 1) == 'H')
				{
					MetaphAdd(primary, "F");
					MetaphAdd(secondary, "F");
					current += 2;
					break;
				}

				/* also account for "campbell", "raspberry" */
				if (StringAt(original, (current + 1), 1, "P", "B", ""))
					current += 2;
				else
					current += 1;
				MetaphAdd(primary, "P");
				MetaphAdd(secondary, "P");
				break;

			case 'Q':
				if (GetAt(original, current + 1) == 'Q')
					current += 2;
				else
					current += 1;
				MetaphAdd(primary, "K");
				MetaphAdd(secondary, "K");
				break;

			case 'R':
				/* french e.g. 'rogier', but exclude 'hochmeier' */
				if ((current == last)
					&& !SlavoGermanic(original)
					&& StringAt(original, (current - 2), 2, "IE", "")
					&& !StringAt(original, (current - 4), 2, "ME", "MA", ""))
				{
					MetaphAdd(primary, "");
					MetaphAdd(secondary, "R");
				}
				else
				{
					MetaphAdd(primary, "R");
					MetaphAdd(secondary, "R");
				}

				if (GetAt(original, current + 1) == 'R')
					current += 2;
				else
					current += 1;
				break;

			case 'S':
				/* special cases 'island', 'isle', 'carlisle', 'carlysle' */
				if (StringAt(original, (current - 1), 3, "ISL", "YSL", ""))
				{
					current += 1;
					break;
				}

				/* special case 'sugar-' */
				if ((current == 0)
					&& StringAt(original, current, 5, "SUGAR", ""))
				{
					MetaphAdd(primary, "X");
					MetaphAdd(secondary, "S");
					current += 1;
					break;
				}

				if (StringAt(original, current, 2, "SH", ""))
				{
					/* germanic */
					if (StringAt
						(original, (current + 1), 4, "HEIM", "HOEK", "HOLM",
						 "HOLZ", ""))
					{
						MetaphAdd(primary, "S");
						MetaphAdd(secondary, "S");
					}
					else
					{
						MetaphAdd(primary, "X");
						MetaphAdd(secondary, "X");
					}
					current += 2;
					break;
				}

				/* italian & armenian */
				if (StringAt(original, current, 3, "SIO", "SIA", "")
					|| StringAt(original, current, 4, "SIAN", ""))
				{
					if (!SlavoGermanic(original))
					{
						MetaphAdd(primary, "S");
						MetaphAdd(secondary, "X");
					}
					else
					{
						MetaphAdd(primary, "S");
						MetaphAdd(secondary, "S");
					}
					current += 3;
					break;
				}

				/*
				 * german & anglicisations, e.g. 'smith' match 'schmidt',
				 * 'snider' match 'schneider' also, -sz- in slavic language
				 * although in hungarian it is pronounced 's'
				 */
				if (((current == 0)
					 && StringAt(original, (current + 1), 1,
								 "M", "N", "L", "W", ""))
					|| StringAt(original, (current + 1), 1, "Z", ""))
				{
					MetaphAdd(primary, "S");
					MetaphAdd(secondary, "X");
					if (StringAt(original, (current + 1), 1, "Z", ""))
						current += 2;
					else
						current += 1;
					break;
				}

				if (StringAt(original, current, 2, "SC", ""))
				{
					/* Schlesinger's rule */
					if (GetAt(original, current + 2) == 'H')
					{
						/* dutch origin, e.g. 'school', 'schooner' */
						if (StringAt(original, (current + 3), 2,
									 "OO", "ER", "EN",
									 "UY", "ED", "EM", ""))
						{
							/* 'schermerhorn', 'schenker' */
							if (StringAt(original, (current + 3), 2,
										 "ER", "EN", ""))
							{
								MetaphAdd(primary, "X");
								MetaphAdd(secondary, "SK");
							}
							else
							{
								MetaphAdd(primary, "SK");
								MetaphAdd(secondary, "SK");
							}
							current += 3;
							break;
						}
						else
						{
							if ((current == 0) && !IsVowel(original, 3)
								&& (GetAt(original, 3) != 'W'))
							{
								MetaphAdd(primary, "X");
								MetaphAdd(secondary, "S");
							}
							else
							{
								MetaphAdd(primary, "X");
								MetaphAdd(secondary, "X");
							}
							current += 3;
							break;
						}
					}

					if (StringAt(original, (current + 2), 1,
								 "I", "E", "Y", ""))
					{
						MetaphAdd(primary, "S");
						MetaphAdd(secondary, "S");
						current += 3;
						break;
					}
					/* else */
					MetaphAdd(primary, "SK");
					MetaphAdd(secondary, "SK");
					current += 3;
					break;
				}

				/* french e.g. 'resnais', 'artois' */
				if ((current == last)
					&& StringAt(original, (current - 2), 2, "AI", "OI", ""))
				{
					MetaphAdd(primary, "");
					MetaphAdd(secondary, "S");
				}
				else
				{
					MetaphAdd(primary, "S");
					MetaphAdd(secondary, "S");
				}

				if (StringAt(original, (current + 1), 1, "S", "Z", ""))
					current += 2;
				else
					current += 1;
				break;

			case 'T':
				if (StringAt(original, current, 4, "TION", ""))
				{
					MetaphAdd(primary, "X");
					MetaphAdd(secondary, "X");
					current += 3;
					break;
				}

				if (StringAt(original, current, 3, "TIA", "TCH", ""))
				{
					MetaphAdd(primary, "X");
					MetaphAdd(secondary, "X");
					current += 3;
					break;
				}

				if (StringAt(original, current, 2, "TH", "")
					|| StringAt(original, current, 3, "TTH", ""))
				{
					/* special case 'thomas', 'thames' or germanic */
					if (StringAt(original, (current + 2), 2, "OM", "AM", "")
						|| StringAt(original, 0, 4, "VAN ", "VON ", "")
						|| StringAt(original, 0, 3, "SCH", ""))
					{
						MetaphAdd(primary, "T");
						MetaphAdd(secondary, "T");
					}
					else
					{
						MetaphAdd(primary, "0");
						MetaphAdd(secondary, "T");
					}
					current += 2;
					break;
				}

				if (StringAt(original, (current + 1), 1, "T", "D", ""))
					current += 2;
				else
					current += 1;
				MetaphAdd(primary, "T");
				MetaphAdd(secondary, "T");
				break;

			case 'V':
				if (GetAt(original, current + 1) == 'V')
					current += 2;
				else
					current += 1;
				MetaphAdd(primary, "F");
				MetaphAdd(secondary, "F");
				break;

			case 'W':
				/* can also be in middle of word */
				if (StringAt(original, current, 2, "WR", ""))
				{
					MetaphAdd(primary, "R");
					MetaphAdd(secondary, "R");
					current += 2;
					break;
				}

				if ((current == 0)
					&& (IsVowel(original, current + 1)
						|| StringAt(original, current, 2, "WH", "")))
				{
					/* Wasserman should match Vasserman */
					if (IsVowel(original, current + 1))
					{
						MetaphAdd(primary, "A");
						MetaphAdd(secondary, "F");
					}
					else
					{
						/* need Uomo to match Womo */
						MetaphAdd(primary, "A");
						MetaphAdd(secondary, "A");
					}
				}

				/* Arnow should match Arnoff */
				if (((current == last) && IsVowel(original, current - 1))
					|| StringAt(original, (current - 1), 5, "EWSKI", "EWSKY",
								"OWSKI", "OWSKY", "")
					|| StringAt(original, 0, 3, "SCH", ""))
				{
					MetaphAdd(primary, "");
					MetaphAdd(secondary, "F");
					current += 1;
					break;
				}

				/* polish e.g. 'filipowicz' */
				if (StringAt(original, current, 4, "WICZ", "WITZ", ""))
				{
					MetaphAdd(primary, "TS");
					MetaphAdd(secondary, "FX");
					current += 4;
					break;
				}

				/* else skip it */
				current += 1;
				break;

			case 'X':
				/* french e.g. breaux */
				if (!((current == last)
					  && (StringAt(original, (current - 3), 3,
								   "IAU", "EAU", "")
						  || StringAt(original, (current - 2), 2,
									  "AU", "OU", ""))))
				{
					MetaphAdd(primary, "KS");
					MetaphAdd(secondary, "KS");
				}


				if (StringAt(original, (current + 1), 1, "C", "X", ""))
					current += 2;
				else
					current += 1;
				break;

			case 'Z':
				/* chinese pinyin e.g. 'zhao' */
				if (GetAt(original, current + 1) == 'H')
				{
					MetaphAdd(primary, "J");
					MetaphAdd(secondary, "J");
					current += 2;
					break;
				}
				else if (StringAt(original, (current + 1), 2,
								  "ZO", "ZI", "ZA", "")
						 || (SlavoGermanic(original)
							 && ((current > 0)
								 && GetAt(original, current - 1) != 'T')))
				{
					MetaphAdd(primary, "S");
					MetaphAdd(secondary, "TS");
				}
				else
				{
					MetaphAdd(primary, "S");
					MetaphAdd(secondary, "S");
				}

				if (GetAt(original, current + 1) == 'Z')
					current += 2;
				else
					current += 1;
				break;

			default:
				current += 1;
		}

		/*
		 * printf("PRIMARY: %s\n", primary->str); printf("SECONDARY: %s\n",
		 * secondary->str);
		 */
	}


	if (primary->length > 4)
		SetAt(primary, 4, '\0');

	if (secondary->length > 4)
		SetAt(secondary, 4, '\0');

	*codes = primary->str;
	*++codes = secondary->str;

	DestroyMetaString(original);
	DestroyMetaString(primary);
	DestroyMetaString(secondary);
}

/* ================= SECTION 5: daitch_mokotoff.c ================= */

/* SHIM: array plumbing for daitch_mokotoff_coding (see header) */
typedef struct ArrayBuildState ArrayBuildState;	/* opaque; pointer-only */
typedef void *Datum;
typedef void *MemoryContext;
#define TEXTOID 25
#define CurrentMemoryContext ((MemoryContext) 0)
#define PointerGetDatum(x) ((Datum) (x))
#define cstring_to_text_with_len(s, n) ((text *) (s))

static _Thread_local char *pg_ca_dm_out;
static _Thread_local int pg_ca_dm_cap;
static _Thread_local int pg_ca_dm_count;

static void
accumArrayResult(ArrayBuildState *state, Datum d, bool isnull,
				 unsigned oid, MemoryContext cx)
{
	(void) state;
	(void) isnull;
	(void) oid;
	(void) cx;
	if (pg_ca_dm_count >= pg_ca_dm_cap)
		abort();				/* driver capacity, never PG semantics */
	memcpy(pg_ca_dm_out + 6 * pg_ca_dm_count, (const char *) d, 6);
	pg_ca_dm_count++;
}

/* generated coding chart (vendored generation step; see header) */
#include "contribafam/daitch_mokotoff.h"

/* ---- VERBATIM contrib/fuzzystrmatch/daitch_mokotoff.c lines 61-117 ---- */
#define DM_CODE_DIGITS 6

/* Node in soundex code tree */
typedef struct dm_node
{
	int			soundex_length; /* Length of generated soundex code */
	char		soundex[DM_CODE_DIGITS];	/* Soundex code */
	int			is_leaf;		/* Candidate for complete soundex code */
	int			last_update;	/* Letter number for last update of node */
	char		code_digit;		/* Last code digit, 0 - 9 */

	/*
	 * One or two alternate code digits leading to this node. If there are two
	 * digits, one of them is always an 'X'. Repeated code digits and 'X' lead
	 * back to the same node.
	 */
	char		prev_code_digits[2];
	/* One or two alternate code digits moving forward. */
	char		next_code_digits[2];
	/* ORed together code index(es) used to reach current node. */
	int			prev_code_index;
	int			next_code_index;
	/* Possible nodes branching out from this node - digits 0-9. */
	struct dm_node *children[10];
	/* Next node in linked list. Alternating index for each iteration. */
	struct dm_node *next[2];
} dm_node;

/* Template for new node in soundex code tree. */
static const dm_node start_node = {
	.soundex_length = 0,
	.soundex = "000000",		/* Six digits */
	.is_leaf = 0,
	.last_update = 0,
	.code_digit = '\0',
	.prev_code_digits = {'\0', '\0'},
	.next_code_digits = {'\0', '\0'},
	.prev_code_index = 0,
	.next_code_index = 0,
	.children = {NULL},
	.next = {NULL}
};

/* Dummy soundex codes at end of input. */
static const dm_codes end_codes[2] =
{
	{
		"X", "X", "X"
	}
};

/* Mapping from ISO8859-1 to upper-case ASCII, covering the range 0x60..0xFF. */
static const char iso8859_1_to_ascii_upper[] =
"`ABCDEFGHIJKLMNOPQRSTUVWXYZ{|}~                                  !                             ?AAAAAAECEEEEIIIIDNOOOOO*OUUUUYDSAAAAAAECEEEEIIIIDNOOOOO/OUUUUYDY";

/* Internal C implementation */
static bool daitch_mokotoff_coding(const char *word, ArrayBuildState *soundex);

/* ---- VERBATIM contrib/fuzzystrmatch/daitch_mokotoff.c lines 161-571 ---- */

/* Initialize soundex code tree node for next code digit. */
static void
initialize_node(dm_node *node, int last_update)
{
	if (node->last_update < last_update)
	{
		node->prev_code_digits[0] = node->next_code_digits[0];
		node->prev_code_digits[1] = node->next_code_digits[1];
		node->next_code_digits[0] = '\0';
		node->next_code_digits[1] = '\0';
		node->prev_code_index = node->next_code_index;
		node->next_code_index = 0;
		node->is_leaf = 0;
		node->last_update = last_update;
	}
}


/* Update soundex code tree node with next code digit. */
static void
add_next_code_digit(dm_node *node, int code_index, char code_digit)
{
	/* OR in index 1 or 2. */
	node->next_code_index |= code_index;

	if (!node->next_code_digits[0])
		node->next_code_digits[0] = code_digit;
	else if (node->next_code_digits[0] != code_digit)
		node->next_code_digits[1] = code_digit;
}


/* Mark soundex code tree node as leaf. */
static void
set_leaf(dm_node *first_node[2], dm_node *last_node[2],
		 dm_node *node, int ix_node)
{
	if (!node->is_leaf)
	{
		node->is_leaf = 1;

		if (first_node[ix_node] == NULL)
			first_node[ix_node] = node;
		else
			last_node[ix_node]->next[ix_node] = node;

		last_node[ix_node] = node;
		node->next[ix_node] = NULL;
	}
}


/* Find next node corresponding to code digit, or create a new node. */
static dm_node *
find_or_create_child_node(dm_node *parent, char code_digit,
						  ArrayBuildState *soundex)
{
	int			i = code_digit - '0';
	dm_node   **nodes = parent->children;
	dm_node    *node = nodes[i];

	if (node)
	{
		/* Found existing child node. Skip completed nodes. */
		return node->soundex_length < DM_CODE_DIGITS ? node : NULL;
	}

	/* Create new child node. */
	node = palloc_object(dm_node);
	nodes[i] = node;

	*node = start_node;
	memcpy(node->soundex, parent->soundex, sizeof(parent->soundex));
	node->soundex_length = parent->soundex_length;
	node->soundex[node->soundex_length++] = code_digit;
	node->code_digit = code_digit;
	node->next_code_index = node->prev_code_index;

	if (node->soundex_length < DM_CODE_DIGITS)
	{
		return node;
	}
	else
	{
		/* Append completed soundex code to output array. */
		text	   *out = cstring_to_text_with_len(node->soundex,
												   DM_CODE_DIGITS);

		accumArrayResult(soundex,
						 PointerGetDatum(out),
						 false,
						 TEXTOID,
						 CurrentMemoryContext);
		return NULL;
	}
}


/* Update node for next code digit(s). */
static void
update_node(dm_node *first_node[2], dm_node *last_node[2],
			dm_node *node, int ix_node,
			int letter_no, int prev_code_index, int next_code_index,
			const char *next_code_digits, int digit_no,
			ArrayBuildState *soundex)
{
	int			i;
	char		next_code_digit = next_code_digits[digit_no];
	int			num_dirty_nodes = 0;
	dm_node    *dirty_nodes[2];

	initialize_node(node, letter_no);

	if (node->prev_code_index && !(node->prev_code_index & prev_code_index))
	{
		/*
		 * If the sound (vowel / consonant) of this letter encoding doesn't
		 * correspond to the coding index of the previous letter, we skip this
		 * letter encoding. Note that currently, only "J" can be either a
		 * vowel or a consonant.
		 */
		return;
	}

	if (next_code_digit == 'X' ||
		(digit_no == 0 &&
		 (node->prev_code_digits[0] == next_code_digit ||
		  node->prev_code_digits[1] == next_code_digit)))
	{
		/* The code digit is the same as one of the previous (i.e. not added). */
		dirty_nodes[num_dirty_nodes++] = node;
	}

	if (next_code_digit != 'X' &&
		(digit_no > 0 ||
		 node->prev_code_digits[0] != next_code_digit ||
		 node->prev_code_digits[1]))
	{
		/* The code digit is different from one of the previous (i.e. added). */
		node = find_or_create_child_node(node, next_code_digit, soundex);
		if (node)
		{
			initialize_node(node, letter_no);
			dirty_nodes[num_dirty_nodes++] = node;
		}
	}

	for (i = 0; i < num_dirty_nodes; i++)
	{
		/* Add code digit leading to the current node. */
		add_next_code_digit(dirty_nodes[i], next_code_index, next_code_digit);

		if (next_code_digits[++digit_no])
		{
			update_node(first_node, last_node, dirty_nodes[i], ix_node,
						letter_no, prev_code_index, next_code_index,
						next_code_digits, digit_no,
						soundex);
		}
		else
		{
			/* Add incomplete leaf node to linked list. */
			set_leaf(first_node, last_node, dirty_nodes[i], ix_node);
		}
	}
}


/* Update soundex tree leaf nodes. */
static void
update_leaves(dm_node *first_node[2], int *ix_node, int letter_no,
			  const dm_codes *codes, const dm_codes *next_codes,
			  ArrayBuildState *soundex)
{
	int			i,
				j,
				code_index;
	dm_node    *node,
			   *last_node[2];
	const dm_code *code,
			   *next_code;
	int			ix_node_next = (*ix_node + 1) & 1;	/* Alternating index: 0, 1 */

	/* Initialize for new linked list of leaves. */
	first_node[ix_node_next] = NULL;
	last_node[ix_node_next] = NULL;

	/* Process all nodes. */
	for (node = first_node[*ix_node]; node; node = node->next[*ix_node])
	{
		/* One or two alternate code sequences. */
		for (i = 0; i < 2 && (code = codes[i]) && code[0][0]; i++)
		{
			/* Coding for previous letter - before vowel: 1, all other: 2 */
			int			prev_code_index = (code[0][0] > '1') + 1;

			/* One or two alternate next code sequences. */
			for (j = 0; j < 2 && (next_code = next_codes[j]) && next_code[0][0]; j++)
			{
				/* Determine which code to use. */
				if (letter_no == 0)
				{
					/* This is the first letter. */
					code_index = 0;
				}
				else if (next_code[0][0] <= '1')
				{
					/* The next letter is a vowel. */
					code_index = 1;
				}
				else
				{
					/* All other cases. */
					code_index = 2;
				}

				/* One or two sequential code digits. */
				update_node(first_node, last_node, node, ix_node_next,
							letter_no, prev_code_index, code_index,
							code[code_index], 0,
							soundex);
			}
		}
	}

	*ix_node = ix_node_next;
}


/*
 * Return next character, converted from UTF-8 to uppercase ASCII.
 * *ix is the current string index and is incremented by the character length.
 */
static char
read_char(const unsigned char *str, int *ix)
{
	/* Substitute character for skipped code points. */
	const char	na = '\x1a';
	pg_wchar	c;

	/* Decode UTF-8 character to ISO 10646 code point. */
	str += *ix;
	c = utf8_to_unicode(str);

	/* Advance *ix, but (for safety) not if we've reached end of string. */
	if (c)
		*ix += pg_utf_mblen(str);

	/* Convert. */
	if (c >= (unsigned char) '[' && c <= (unsigned char) ']')
	{
		/* ASCII characters [, \, and ] are reserved for conversions below. */
		return na;
	}
	else if (c < 0x60)
	{
		/* Other non-lowercase ASCII characters can be used as-is. */
		return (char) c;
	}
	else if (c < 0x100)
	{
		/* ISO-8859-1 code point; convert to upper-case ASCII via table. */
		return iso8859_1_to_ascii_upper[c - 0x60];
	}
	else
	{
		/* Conversion of non-ASCII characters in the coding chart. */
		switch (c)
		{
			case 0x0104:		/* LATIN CAPITAL LETTER A WITH OGONEK */
			case 0x0105:		/* LATIN SMALL LETTER A WITH OGONEK */
				return '[';
			case 0x0118:		/* LATIN CAPITAL LETTER E WITH OGONEK */
			case 0x0119:		/* LATIN SMALL LETTER E WITH OGONEK */
				return '\\';
			case 0x0162:		/* LATIN CAPITAL LETTER T WITH CEDILLA */
			case 0x0163:		/* LATIN SMALL LETTER T WITH CEDILLA */
			case 0x021A:		/* LATIN CAPITAL LETTER T WITH COMMA BELOW */
			case 0x021B:		/* LATIN SMALL LETTER T WITH COMMA BELOW */
				return ']';
			default:
				return na;
		}
	}
}


/* Read next ASCII character, skipping any characters not in [A-\]]. */
static char
read_valid_char(const char *str, int *ix)
{
	char		c;

	while ((c = read_char((const unsigned char *) str, ix)) != '\0')
	{
		if (c >= 'A' && c <= ']')
			break;
	}

	return c;
}


/* Return sound coding for "letter" (letter sequence) */
static const dm_codes *
read_letter(const char *str, int *ix)
{
	char		c,
				cmp;
	int			i,
				j;
	const dm_letter *letters;
	const dm_codes *codes;

	/* First letter in sequence. */
	if ((c = read_valid_char(str, ix)) == '\0')
		return NULL;

	letters = &letter_[c - 'A'];
	codes = letters->codes;
	i = *ix;

	/* Any subsequent letters in sequence. */
	while ((letters = letters->letters) && (c = read_valid_char(str, &i)))
	{
		for (j = 0; (cmp = letters[j].letter); j++)
		{
			if (cmp == c)
			{
				/* Letter found. */
				letters = &letters[j];
				if (letters->codes)
				{
					/* Coding for letter sequence found. */
					codes = letters->codes;
					*ix = i;
				}
				break;
			}
		}
		if (!cmp)
		{
			/* The sequence of letters has no coding. */
			break;
		}
	}

	return codes;
}


/*
 * Generate all Daitch-Mokotoff soundex codes for word,
 * adding them to the "soundex" ArrayBuildState.
 * Returns false if string has no encodable characters, else true.
 */
static bool
daitch_mokotoff_coding(const char *word, ArrayBuildState *soundex)
{
	int			i = 0;
	int			letter_no = 0;
	int			ix_node = 0;
	const dm_codes *codes,
			   *next_codes;
	dm_node    *first_node[2],
			   *node;

	/* First letter. */
	if (!(codes = read_letter(word, &i)))
	{
		/* No encodable character in input. */
		return false;
	}

	/* Starting point. */
	first_node[ix_node] = palloc_object(dm_node);
	*first_node[ix_node] = start_node;

	/*
	 * Loop until either the word input is exhausted, or all generated soundex
	 * codes are completed to six digits.
	 */
	while (codes && first_node[ix_node])
	{
		next_codes = read_letter(word, &i);

		/* Update leaf nodes. */
		update_leaves(first_node, &ix_node, letter_no,
					  codes, next_codes ? next_codes : end_codes,
					  soundex);

		codes = next_codes;
		letter_no++;
	}

	/* Append all remaining (incomplete) soundex codes to output array. */
	for (node = first_node[ix_node]; node; node = node->next[ix_node])
	{
		text	   *out = cstring_to_text_with_len(node->soundex,
												   DM_CODE_DIGITS);

		accumArrayResult(soundex,
						 PointerGetDatum(out),
						 false,
						 TEXTOID,
						 CurrentMemoryContext);
	}

	return true;
}

/* ================= SECTION 6: isn.c ================= */

/* contrib/isn/isn.h line 25 (typedef) + EAN13_FORMAT resolved to PRIu64
 * (UINT64_FORMAT; feeds only swallowed errmsg text) */
typedef uint64 ean13;
#define EAN13_FORMAT "%" PRIu64

/* struct Node: string2ean only tests escontext pointer nullness through
 * the ereturn shim (see header) */
struct Node
{
	int			pg_ca_dummy;
};

/* data tables: contrib/isn headers copied whole to csrc/contribafam/ */
#include "contribafam/EAN13.h"
#include "contribafam/ISBN.h"
#include "contribafam/ISMN.h"
#include "contribafam/ISSN.h"
#include "contribafam/UPC.h"

/* ---- VERBATIM contrib/isn/isn.c lines 36-48 ---- */

#define MAXEAN13LEN 18

enum isn_type
{
	INVALID, ANY, EAN13, ISBN, ISMN, ISSN, UPC
};

static const char *const isn_names[] = {"EAN13/UPC/ISxN", "EAN13/UPC/ISxN", "EAN13", "ISBN", "ISMN", "ISSN", "UPC"};

/* GUC value */
static bool g_weak = false;


/* ---- VERBATIM contrib/isn/isn.c lines 141-915 ---- */

/*----------------------------------------------------------
 * Formatting and conversion routines.
 *---------------------------------------------------------*/

static unsigned
dehyphenate(char *bufO, char *bufI)
{
	unsigned	ret = 0;

	while (*bufI)
	{
		if (isdigit((unsigned char) *bufI))
		{
			*bufO++ = *bufI;
			ret++;
		}
		bufI++;
	}
	*bufO = '\0';
	return ret;
}

/*
 * hyphenate --- Try to hyphenate, in-place, the string starting at bufI
 *				  into bufO using the given hyphenation range TABLE.
 *				  Assumes the input string to be used is of only digits.
 *
 * Returns the number of characters actually hyphenated.
 */
static unsigned
hyphenate(char *bufO, char *bufI, const char *(*TABLE)[2], const unsigned TABLE_index[10][2])
{
	unsigned	ret = 0;
	const char *ean_aux1,
			   *ean_aux2,
			   *ean_p;
	char	   *firstdig,
			   *aux1,
			   *aux2;
	unsigned	search,
				upper,
				lower,
				step;
	bool		ean_in1,
				ean_in2;

	/* just compress the string if no further hyphenation is required */
	if (TABLE == NULL || TABLE_index == NULL)
	{
		while (*bufI)
		{
			*bufO++ = *bufI++;
			ret++;
		}
		*bufO = '\0';
		return (ret + 1);
	}

	/* add remaining hyphenations */

	search = *bufI - '0';
	upper = lower = TABLE_index[search][0];
	upper += TABLE_index[search][1];
	lower--;

	step = (upper - lower) / 2;
	if (step == 0)
		return 0;
	search = lower + step;

	firstdig = bufI;
	ean_in1 = ean_in2 = false;
	ean_aux1 = TABLE[search][0];
	ean_aux2 = TABLE[search][1];
	do
	{
		if ((ean_in1 || *firstdig >= *ean_aux1) && (ean_in2 || *firstdig <= *ean_aux2))
		{
			if (*firstdig > *ean_aux1)
				ean_in1 = true;
			if (*firstdig < *ean_aux2)
				ean_in2 = true;
			if (ean_in1 && ean_in2)
				break;

			firstdig++, ean_aux1++, ean_aux2++;
			if (!(*ean_aux1 && *ean_aux2 && *firstdig))
				break;
			if (!isdigit((unsigned char) *ean_aux1))
				ean_aux1++, ean_aux2++;
		}
		else
		{
			/*
			 * check in what direction we should go and move the pointer
			 * accordingly
			 */
			if (*firstdig < *ean_aux1 && !ean_in1)
				upper = search;
			else
				lower = search;

			step = (upper - lower) / 2;
			search = lower + step;

			/* Initialize stuff again: */
			firstdig = bufI;
			ean_in1 = ean_in2 = false;
			ean_aux1 = TABLE[search][0];
			ean_aux2 = TABLE[search][1];
		}
	} while (step);

	if (step)
	{
		aux1 = bufO;
		aux2 = bufI;
		ean_p = TABLE[search][0];
		while (*ean_p && *aux2)
		{
			if (*ean_p++ != '-')
				*aux1++ = *aux2++;
			else
				*aux1++ = '-';
			ret++;
		}
		*aux1++ = '-';
		*aux1 = *aux2;			/* add a lookahead char */
		return (ret + 1);
	}
	return ret;
}

/*
 * weight_checkdig -- Receives a buffer with a normalized ISxN string number,
 *					   and the length to weight.
 *
 * Returns the weight of the number (the check digit value, 0-10)
 */
static unsigned
weight_checkdig(char *isn, unsigned size)
{
	unsigned	weight = 0;

	while (*isn && size > 1)
	{
		if (isdigit((unsigned char) *isn))
		{
			weight += size-- * (*isn - '0');
		}
		isn++;
	}
	weight = weight % 11;
	if (weight != 0)
		weight = 11 - weight;
	return weight;
}


/*
 * checkdig --- Receives a buffer with a normalized ISxN string number,
 *				 and the length to check.
 *
 * Returns the check digit value (0-9)
 */
static unsigned
checkdig(char *num, unsigned size)
{
	unsigned	check = 0,
				check3 = 0;
	unsigned	pos = 0;

	if (*num == 'M')
	{							/* ISMN start with 'M' */
		check3 = 3;
		pos = 1;
	}
	while (*num && size > 1)
	{
		if (isdigit((unsigned char) *num))
		{
			if (pos++ % 2)
				check3 += *num - '0';
			else
				check += *num - '0';
			size--;
		}
		num++;
	}
	check = (check + 3 * check3) % 10;
	if (check != 0)
		check = 10 - check;
	return check;
}

/*
 * ean2isn --- Try to convert an ean13 number to a UPC/ISxN number.
 *			   This doesn't verify for a valid check digit.
 *
 * If errorOK is false, ereport a useful error message if the ean13 is bad.
 * If errorOK is true, just return "false" for bad input.
 */
static bool
ean2isn(ean13 ean, bool errorOK, ean13 *result, enum isn_type accept)
{
	enum isn_type type = INVALID;

	char		buf[MAXEAN13LEN + 1];
	char	   *aux;
	unsigned	digval;
	unsigned	search;
	ean13		ret = ean;

	ean >>= 1;
	/* verify it's in the EAN13 range */
	if (ean > UINT64CONST(9999999999999))
		goto eantoobig;

	/* convert the number */
	search = 0;
	aux = buf + 13;
	*aux = '\0';				/* terminate string; aux points to last digit */
	do
	{
		digval = (unsigned) (ean % 10); /* get the decimal value */
		ean /= 10;				/* get next digit */
		*--aux = (char) (digval + '0'); /* convert to ascii and store */
	} while (ean && search++ < 12);
	while (search++ < 12)
		*--aux = '0';			/* fill the remaining EAN13 with '0' */

	/* find out the data type: */
	if (strncmp("978", buf, 3) == 0)
	{							/* ISBN */
		type = ISBN;
	}
	else if (strncmp("977", buf, 3) == 0)
	{							/* ISSN */
		type = ISSN;
	}
	else if (strncmp("9790", buf, 4) == 0)
	{							/* ISMN */
		type = ISMN;
	}
	else if (strncmp("979", buf, 3) == 0)
	{							/* ISBN-13 */
		type = ISBN;
	}
	else if (*buf == '0')
	{							/* UPC */
		type = UPC;
	}
	else
	{
		type = EAN13;
	}
	if (accept != ANY && accept != EAN13 && accept != type)
		goto eanwrongtype;

	*result = ret;
	return true;

eanwrongtype:
	if (!errorOK)
	{
		if (type != EAN13)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("cannot cast EAN13(%s) to %s for number: \"%s\"",
							isn_names[type], isn_names[accept], buf)));
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("cannot cast %s to %s for number: \"%s\"",
							isn_names[type], isn_names[accept], buf)));
		}
	}
	return false;

eantoobig:
	if (!errorOK)
	{
		char		eanbuf[64];

		/*
		 * Format the number separately to keep the machine-dependent format
		 * code out of the translatable message text
		 */
		snprintf(eanbuf, sizeof(eanbuf), EAN13_FORMAT, ean);
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("value \"%s\" is out of range for %s type",
						eanbuf, isn_names[type])));
	}
	return false;
}

/*
 * ean2UPC/ISxN --- Convert in-place a normalized EAN13 string to the corresponding
 *					UPC/ISxN string number. Assumes the input string is normalized.
 */
static inline void
ean2ISBN(char *isn)
{
	char	   *aux;
	unsigned	check;

	/*
	 * The number should come in this format: 978-0-000-00000-0 or may be an
	 * ISBN-13 number, 979-..., which does not have a short representation. Do
	 * the short output version if possible.
	 */
	if (strncmp("978-", isn, 4) == 0)
	{
		/* Strip the first part and calculate the new check digit */
		hyphenate(isn, isn + 4, NULL, NULL);
		check = weight_checkdig(isn, 10);
		aux = strchr(isn, '\0');
		while (!isdigit((unsigned char) *--aux));
		if (check == 10)
			*aux = 'X';
		else
			*aux = check + '0';
	}
}

static inline void
ean2ISMN(char *isn)
{
	/* the number should come in this format: 979-0-000-00000-0 */
	/* Just strip the first part and change the first digit ('0') to 'M' */
	hyphenate(isn, isn + 4, NULL, NULL);
	isn[0] = 'M';
}

static inline void
ean2ISSN(char *isn)
{
	unsigned	check;

	/* the number should come in this format: 977-0000-000-00-0 */
	/* Strip the first part, crop, and calculate the new check digit */
	hyphenate(isn, isn + 4, NULL, NULL);
	check = weight_checkdig(isn, 8);
	if (check == 10)
		isn[8] = 'X';
	else
		isn[8] = check + '0';
	isn[9] = '\0';
}

static inline void
ean2UPC(char *isn)
{
	/* the number should come in this format: 000-000000000-0 */
	/* Strip the first part, crop, and dehyphenate */
	dehyphenate(isn, isn + 1);
	isn[12] = '\0';
}

/*
 * ean2* --- Converts a string of digits into an ean13 number.
 *			  Assumes the input string is a string with only digits
 *			  on it, and that it's within the range of ean13.
 *
 * Returns the ean13 value of the string.
 */
static ean13
str2ean(const char *num)
{
	ean13		ean = 0;		/* current ean */

	while (*num)
	{
		if (isdigit((unsigned char) *num))
			ean = 10 * ean + (*num - '0');
		num++;
	}
	return (ean << 1);			/* also give room to a flag */
}

/*
 * ean2string --- Try to convert an ean13 number to a hyphenated string.
 *				  Assumes there's enough space in result to hold
 *				  the string (maximum MAXEAN13LEN+1 bytes)
 *				  This doesn't verify for a valid check digit.
 *
 * If shortType is true, the returned string is in the old ISxN short format.
 * If errorOK is false, ereport a useful error message if the string is bad.
 * If errorOK is true, just return "false" for bad input.
 */
static bool
ean2string(ean13 ean, bool errorOK, char *result, bool shortType)
{
	const char *(*TABLE)[2];
	const unsigned (*TABLE_index)[2];
	enum isn_type type = INVALID;

	char	   *aux;
	unsigned	digval;
	unsigned	search;
	char		valid = '\0';	/* was the number initially written with a
								 * valid check digit? */

	TABLE_index = ISBN_index;

	if ((ean & 1) != 0)
		valid = '!';
	ean >>= 1;
	/* verify it's in the EAN13 range */
	if (ean > UINT64CONST(9999999999999))
		goto eantoobig;

	/* convert the number */
	search = 0;
	aux = result + MAXEAN13LEN;
	*aux = '\0';				/* terminate string; aux points to last digit */
	*--aux = valid;				/* append '!' for numbers with invalid but
								 * corrected check digit */
	do
	{
		digval = (unsigned) (ean % 10); /* get the decimal value */
		ean /= 10;				/* get next digit */
		*--aux = (char) (digval + '0'); /* convert to ascii and store */
		if (search == 0)
			*--aux = '-';		/* the check digit is always there */
	} while (ean && search++ < 13);
	while (search++ < 13)
		*--aux = '0';			/* fill the remaining EAN13 with '0' */

	/* The string should be in this form: ???DDDDDDDDDDDD-D" */
	search = hyphenate(result, result + 3, EAN13_range, EAN13_index);

	/* verify it's a logically valid EAN13 */
	if (search == 0)
	{
		search = hyphenate(result, result + 3, NULL, NULL);
		goto okay;
	}

	/* find out what type of hyphenation is needed: */
	if (strncmp("978-", result, search) == 0)
	{							/* ISBN -13 978-range */
		/* The string should be in this form: 978-??000000000-0" */
		type = ISBN;
		TABLE = ISBN_range;
		TABLE_index = ISBN_index;
	}
	else if (strncmp("977-", result, search) == 0)
	{							/* ISSN */
		/* The string should be in this form: 977-??000000000-0" */
		type = ISSN;
		TABLE = ISSN_range;
		TABLE_index = ISSN_index;
	}
	else if (strncmp("979-0", result, search + 1) == 0)
	{							/* ISMN */
		/* The string should be in this form: 979-0?000000000-0" */
		type = ISMN;
		TABLE = ISMN_range;
		TABLE_index = ISMN_index;
	}
	else if (strncmp("979-", result, search) == 0)
	{							/* ISBN-13 979-range */
		/* The string should be in this form: 979-??000000000-0" */
		type = ISBN;
		TABLE = ISBN_range_new;
		TABLE_index = ISBN_index_new;
	}
	else if (*result == '0')
	{							/* UPC */
		/* The string should be in this form: 000-00000000000-0" */
		type = UPC;
		TABLE = UPC_range;
		TABLE_index = UPC_index;
	}
	else
	{
		type = EAN13;
		TABLE = NULL;
		TABLE_index = NULL;
	}

	/* verify it's a logically valid EAN13/UPC/ISxN */
	digval = search;
	search = hyphenate(result + digval, result + digval + 2, TABLE, TABLE_index);

	/* verify it's a valid EAN13 */
	if (search == 0)
	{
		search = hyphenate(result + digval, result + digval + 2, NULL, NULL);
		goto okay;
	}

okay:
	/* convert to the old short type: */
	if (shortType)
		switch (type)
		{
			case ISBN:
				ean2ISBN(result);
				break;
			case ISMN:
				ean2ISMN(result);
				break;
			case ISSN:
				ean2ISSN(result);
				break;
			case UPC:
				ean2UPC(result);
				break;
			default:
				break;
		}
	return true;

eantoobig:
	if (!errorOK)
	{
		char		eanbuf[64];

		/*
		 * Format the number separately to keep the machine-dependent format
		 * code out of the translatable message text
		 */
		snprintf(eanbuf, sizeof(eanbuf), EAN13_FORMAT, ean);
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("value \"%s\" is out of range for %s type",
						eanbuf, isn_names[type])));
	}
	return false;
}

/*
 * string2ean --- try to parse a string into an ean13.
 *
 * ereturn false with a useful error message if the string is bad.
 * Otherwise return true.
 *
 * if the input string ends with '!' it will always be treated as invalid
 * (even if the check digit is valid)
 */
static bool
string2ean(const char *str, struct Node *escontext, ean13 *result,
		   enum isn_type accept)
{
	bool		digit,
				last;
	char		buf[17] = "                ";
	char	   *aux1 = buf + 3; /* leave space for the first part, in case
								 * it's needed */
	const char *aux2 = str;
	enum isn_type type = INVALID;
	unsigned	check = 0,
				rcheck = (unsigned) -1;
	unsigned	length = 0;
	bool		magic = false,
				valid = true;

	/* recognize and validate the number: */
	while (*aux2 && length <= 13)
	{
		last = (*(aux2 + 1) == '!' || *(aux2 + 1) == '\0'); /* is the last character */
		digit = (isdigit((unsigned char) *aux2) != 0);	/* is current character
														 * a digit? */
		if (*aux2 == '?' && last)	/* automagically calculate check digit if
									 * it's '?' */
			magic = digit = true;
		if (length == 0 && (*aux2 == 'M' || *aux2 == 'm'))
		{
			/* only ISMN can be here */
			if (type != INVALID)
				goto eaninvalid;
			type = ISMN;
			*aux1++ = 'M';
			length++;
		}
		else if (length == 7 && (digit || *aux2 == 'X' || *aux2 == 'x') && last)
		{
			/* only ISSN can be here */
			if (type != INVALID)
				goto eaninvalid;
			type = ISSN;
			*aux1++ = toupper((unsigned char) *aux2);
			length++;
		}
		else if (length == 9 && (digit || *aux2 == 'X' || *aux2 == 'x') && last)
		{
			/* only ISBN and ISMN can be here */
			if (type != INVALID && type != ISMN)
				goto eaninvalid;
			if (type == INVALID)
				type = ISBN;	/* ISMN must start with 'M' */
			*aux1++ = toupper((unsigned char) *aux2);
			length++;
		}
		else if (length == 11 && digit && last)
		{
			/* only UPC can be here */
			if (type != INVALID)
				goto eaninvalid;
			type = UPC;
			*aux1++ = *aux2;
			length++;
		}
		else if (*aux2 == '-' || *aux2 == ' ')
		{
			/* skip, we could validate but I think it's worthless */
		}
		else if (*aux2 == '!' && *(aux2 + 1) == '\0')
		{
			/* the invalid check digit suffix was found, set it */
			if (!magic)
				valid = false;
			magic = true;
		}
		else if (!digit)
		{
			goto eaninvalid;
		}
		else
		{
			*aux1++ = *aux2;
			if (++length > 13)
				goto eantoobig;
		}
		aux2++;
	}
	*aux1 = '\0';				/* terminate the string */

	/* find the current check digit value */
	if (length == 13)
	{
		/* only EAN13 can be here */
		if (type != INVALID)
			goto eaninvalid;
		type = EAN13;
		check = buf[15] - '0';
	}
	else if (length == 12)
	{
		/* only UPC can be here */
		if (type != UPC)
			goto eaninvalid;
		check = buf[14] - '0';
	}
	else if (length == 10)
	{
		if (type != ISBN && type != ISMN)
			goto eaninvalid;
		if (buf[12] == 'X')
			check = 10;
		else
			check = buf[12] - '0';
	}
	else if (length == 8)
	{
		if (type != INVALID && type != ISSN)
			goto eaninvalid;
		type = ISSN;
		if (buf[10] == 'X')
			check = 10;
		else
			check = buf[10] - '0';
	}
	else
		goto eaninvalid;

	if (type == INVALID)
		goto eaninvalid;

	/* obtain the real check digit value, validate, and convert to ean13: */
	if (accept == EAN13 && type != accept)
		goto eanwrongtype;
	if (accept != ANY && type != EAN13 && type != accept)
		goto eanwrongtype;
	switch (type)
	{
		case EAN13:
			valid = (valid && ((rcheck = checkdig(buf + 3, 13)) == check || magic));
			/* now get the subtype of EAN13: */
			if (buf[3] == '0')
				type = UPC;
			else if (strncmp("977", buf + 3, 3) == 0)
				type = ISSN;
			else if (strncmp("978", buf + 3, 3) == 0)
				type = ISBN;
			else if (strncmp("9790", buf + 3, 4) == 0)
				type = ISMN;
			else if (strncmp("979", buf + 3, 3) == 0)
				type = ISBN;
			if (accept != EAN13 && accept != ANY && type != accept)
				goto eanwrongtype;
			break;
		case ISMN:
			memcpy(buf, "9790", 4); /* this isn't for sure yet, for now ISMN
									 * it's only 9790 */
			valid = (valid && ((rcheck = checkdig(buf, 13)) == check || magic));
			break;
		case ISBN:
			memcpy(buf, "978", 3);
			valid = (valid && ((rcheck = weight_checkdig(buf + 3, 10)) == check || magic));
			break;
		case ISSN:
			memcpy(buf + 10, "00", 2);	/* append 00 as the normal issue
										 * publication code */
			memcpy(buf, "977", 3);
			valid = (valid && ((rcheck = weight_checkdig(buf + 3, 8)) == check || magic));
			break;
		case UPC:
			buf[2] = '0';
			valid = (valid && ((rcheck = checkdig(buf + 2, 13)) == check || magic));
		default:
			break;
	}

	/* fix the check digit: */
	for (aux1 = buf; *aux1 && *aux1 <= ' '; aux1++);
	aux1[12] = checkdig(aux1, 13) + '0';
	aux1[13] = '\0';

	if (!valid && !magic)
		goto eanbadcheck;

	*result = str2ean(aux1);
	*result |= valid ? 0 : 1;
	return true;

eanbadcheck:
	if (g_weak)
	{							/* weak input mode is activated: */
		/* set the "invalid-check-digit-on-input" flag */
		*result = str2ean(aux1);
		*result |= 1;
		return true;
	}

	if (rcheck == (unsigned) -1)
	{
		ereturn(escontext, false,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid %s number: \"%s\"",
						isn_names[accept], str)));
	}
	else
	{
		ereturn(escontext, false,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid check digit for %s number: \"%s\", should be %c",
						isn_names[accept], str, (rcheck == 10) ? ('X') : (rcheck + '0'))));
	}

eaninvalid:
	ereturn(escontext, false,
			(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
			 errmsg("invalid input syntax for %s number: \"%s\"",
					isn_names[accept], str)));

eanwrongtype:
	ereturn(escontext, false,
			(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
			 errmsg("cannot cast %s to %s for number: \"%s\"",
					isn_names[type], isn_names[accept], str)));

eantoobig:
	ereturn(escontext, false,
			(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
			 errmsg("value \"%s\" is out of range for %s type",
					str, isn_names[accept])));
}

/* ========== SECTION D: fuzz-facing driver entries (NOT Postgres code) ===== */

/* Every entry resets the arena + error channel and (where reachable) arms
 * the jmp_buf; on longjmp it reports the errcode class. */

const char *
pg_ca_out_get(void)
{
	return pg_ca_out;
}

int32
pg_ca_int_out_get(void)
{
	return pg_ca_int_out;
}

int
pg_ca_soft_fired_get(void)
{
	return pg_ca_soft_fired;
}

/* ---- fuzzystrmatch: wrapper-body shells (shim 1) ---- */

/* body = VERBATIM fuzzystrmatch.c 714-723 (SQL soundex) */
int
pg_ca_soundex(const char *arg0)
{
	pg_ca_arena_reset();
	pg_ca_errcode = 0;
	pg_ca_args[0] = (char *) arg0;
	if (setjmp(pg_ca_jmp) != 0)
		return pg_ca_errcode;
{
	char		outstr[SOUNDEX_LEN + 1];
	char	   *arg;

	arg = text_to_cstring(PG_GETARG_TEXT_PP(0));

	_soundex(arg, outstr);

	PG_RETURN_TEXT_P(cstring_to_text(outstr));
}
	/* not reached: the verbatim wrapper body always returns */
	abort();
}

/* body = VERBATIM fuzzystrmatch.c 779-796 (SQL difference) */
int
pg_ca_difference(const char *arg0, const char *arg1)
{
	pg_ca_arena_reset();
	pg_ca_errcode = 0;
	pg_ca_args[0] = (char *) arg0;
	pg_ca_args[1] = (char *) arg1;
	if (setjmp(pg_ca_jmp) != 0)
		return -pg_ca_errcode;
{
	char		sndx1[SOUNDEX_LEN + 1],
				sndx2[SOUNDEX_LEN + 1];
	int			i,
				result;

	_soundex(text_to_cstring(PG_GETARG_TEXT_PP(0)), sndx1);
	_soundex(text_to_cstring(PG_GETARG_TEXT_PP(1)), sndx2);

	result = 0;
	for (i = 0; i < SOUNDEX_LEN; i++)
	{
		if (sndx1[i] == sndx2[i])
			result++;
	}

	PG_RETURN_INT32(result);
}
	/* not reached: the verbatim wrapper body always returns */
	abort();
}

/* body = VERBATIM fuzzystrmatch.c 261-291 (SQL metaphone) */
int
pg_ca_metaphone(const char *arg0, int32 reqlen)
{
	pg_ca_arena_reset();
	pg_ca_errcode = 0;
	pg_ca_args[0] = (char *) arg0;
	pg_ca_int_args[1] = reqlen;
	if (setjmp(pg_ca_jmp) != 0)
		return pg_ca_errcode;
{
	char	   *str_i = TextDatumGetCString(PG_GETARG_DATUM(0));
	size_t		str_i_len = strlen(str_i);
	int			reqlen;
	char	   *metaph;

	/* return an empty string if we receive one */
	if (!(str_i_len > 0))
		PG_RETURN_TEXT_P(cstring_to_text(""));

	if (str_i_len > MAX_METAPHONE_STRLEN)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("argument exceeds the maximum length of %d bytes",
						MAX_METAPHONE_STRLEN)));

	reqlen = PG_GETARG_INT32(1);
	if (reqlen > MAX_METAPHONE_STRLEN)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("output exceeds the maximum length of %d bytes",
						MAX_METAPHONE_STRLEN)));

	if (!(reqlen > 0))
		ereport(ERROR,
				(errcode(ERRCODE_ZERO_LENGTH_CHARACTER_STRING),
				 errmsg("output cannot be empty string")));

	_metaphone(str_i, reqlen, &metaph);
	PG_RETURN_TEXT_P(cstring_to_text(metaph));
}
	/* not reached: the verbatim wrapper body always returns */
	abort();
}

/* body = VERBATIM dmetaphone.c 132-151 (SQL dmetaphone) */
int
pg_ca_dmetaphone(const char *arg0)
{
	pg_ca_arena_reset();
	pg_ca_errcode = 0;
	pg_ca_args[0] = (char *) arg0;
	if (setjmp(pg_ca_jmp) != 0)
		return pg_ca_errcode;
{
	text	   *arg;
	char	   *aptr,
			   *codes[2],
			   *code;

#ifdef DMETAPHONE_NOSTRICT
	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();
#endif
	arg = PG_GETARG_TEXT_PP(0);
	aptr = text_to_cstring(arg);

	DoubleMetaphone(aptr, codes);
	code = codes[0];
	if (!code)
		code = "";

	PG_RETURN_TEXT_P(cstring_to_text(code));
}
	/* not reached: the verbatim wrapper body always returns */
	abort();
}

/* body = VERBATIM dmetaphone.c 161-180 (SQL dmetaphone_alt) */
int
pg_ca_dmetaphone_alt(const char *arg0)
{
	pg_ca_arena_reset();
	pg_ca_errcode = 0;
	pg_ca_args[0] = (char *) arg0;
	if (setjmp(pg_ca_jmp) != 0)
		return pg_ca_errcode;
{
	text	   *arg;
	char	   *aptr,
			   *codes[2],
			   *code;

#ifdef DMETAPHONE_NOSTRICT
	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();
#endif
	arg = PG_GETARG_TEXT_PP(0);
	aptr = text_to_cstring(arg);

	DoubleMetaphone(aptr, codes);
	code = codes[1];
	if (!code)
		code = "";

	PG_RETURN_TEXT_P(cstring_to_text(code));
}
	/* not reached: the verbatim wrapper body always returns */
	abort();
}

/* ---- levenshtein cores (fc wrappers are arg plumbing; the Rust fc plane
 * compares its wrappers against these core results) ---- */

int
pg_ca_levenshtein(const char *s, int slen, const char *t, int tlen,
				  int ins_c, int del_c, int sub_c, int32 *out)
{
	pg_ca_arena_reset();
	pg_ca_errcode = 0;
	if (setjmp(pg_ca_jmp) != 0)
		return pg_ca_errcode;
	*out = varstr_levenshtein(s, slen, t, tlen, ins_c, del_c, sub_c, false);
	return 0;
}

int
pg_ca_levenshtein_less_equal(const char *s, int slen, const char *t, int tlen,
							 int ins_c, int del_c, int sub_c, int max_d,
							 int32 *out)
{
	pg_ca_arena_reset();
	pg_ca_errcode = 0;
	if (setjmp(pg_ca_jmp) != 0)
		return pg_ca_errcode;
	*out = varstr_levenshtein_less_equal(s, slen, t, tlen, ins_c, del_c,
										 sub_c, max_d, false);
	return 0;
}

/* ---- daitch_mokotoff core ---- */

/* out receives up to cap 6-byte codes; *count = number appended.
 * Returns 0 = coded, -1 = no encodable characters, >0 = errclass. */
int
pg_ca_daitch(const char *word, char *out, int cap, int *count)
{
	bool		ok;

	pg_ca_arena_reset();
	pg_ca_errcode = 0;
	pg_ca_dm_out = out;
	pg_ca_dm_cap = cap;
	pg_ca_dm_count = 0;
	if (setjmp(pg_ca_jmp) != 0)
		return pg_ca_errcode;
	ok = daitch_mokotoff_coding(word, (ArrayBuildState *) 0);
	*count = pg_ca_dm_count;
	return ok ? 0 : -1;
}

/* ---- isn cores ---- */

void
pg_ca_set_weak(int w)
{
	g_weak = (w != 0);
}

/* soft != 0 arms the escontext shape; *soft_fired reports the ereturn
 * soft-branch witness count for this call. */
int
pg_ca_string2ean(const char *str, int accept, int soft, uint64 *result,
				 int *soft_fired)
{
	static _Thread_local struct Node pg_ca_node;
	ean13		res = 0;
	bool		ok;
	int			fired0;

	pg_ca_arena_reset();
	pg_ca_errcode = 0;
	fired0 = pg_ca_soft_fired;
	*soft_fired = 0;
	if (setjmp(pg_ca_jmp) != 0)
		return pg_ca_errcode;
	ok = string2ean(str, soft ? &pg_ca_node : NULL, &res, (enum isn_type) accept);
	*soft_fired = pg_ca_soft_fired - fired0;
	*result = res;
	if (!ok)
		return pg_ca_errcode;
	return 0;
}

int
pg_ca_ean2string(uint64 ean, int short_type, char *out)
{
	char		buf[MAXEAN13LEN + 1];

	pg_ca_arena_reset();
	pg_ca_errcode = 0;
	if (setjmp(pg_ca_jmp) != 0)
		return pg_ca_errcode;
	(void) ean2string((ean13) ean, false, buf, short_type != 0);
	memcpy(out, buf, sizeof(buf));
	return 0;
}

int
pg_ca_ean2isn(uint64 ean, int accept, uint64 *result)
{
	ean13		res = 0;

	pg_ca_arena_reset();
	pg_ca_errcode = 0;
	if (setjmp(pg_ca_jmp) != 0)
		return pg_ca_errcode;
	(void) ean2isn((ean13) ean, false, &res, (enum isn_type) accept);
	*result = res;
	return 0;
}
