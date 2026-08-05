/*
 * pg_vlmisc_io.c: vendored PostgreSQL C oracle for the vlmisc_diff differential
 * fuzz target (100%-coverage campaign; crate crates/backend/utils/adt/varlena).
 *
 * Provenance (all bodies VERBATIM unless a numbered shim below says otherwise),
 * from the repo's vendored ground-truth checkout
 * ../pgrust-fabled/vendor/postgres-src @
 * 62d6c7d3df6287f1bd83199c1a746e50d31571a0 (PostgreSQL 18.3, "Stamp 18.3"):
 *
 *   - src/backend/utils/adt/varlena.c: cstring_to_text, cstring_to_text_with_len,
 *     convert_to_base, to_bin32/to_bin64/to_oct32/to_oct64/to_hex32/to_hex64,
 *     unicode_norm_form_from_string, unicode_version, unicode_assigned,
 *     unicode_normalize_func, unicode_is_normalized, isxdigits_n, hexval,
 *     hexval_n, unistr, rest_of_char_same, SplitIdentifierString, SplitGUCList.
 *   - src/backend/utils/adt/levenshtein.c: vendored whole as
 *     vlmisc/levenshtein.c and #included TWICE (without and with
 *     LEVENSHTEIN_LESS_EQUAL), exactly as varlena.c does.
 *   - src/common/unicode_norm.c: vendored whole as vlmisc/unicode_norm.c
 *     (backend arms; include block flattened — see its banner), with its
 *     generated data tables vendored under vlmisc/ (unicode_norm_table.h,
 *     unicode_norm_hashfunc.h, unicode_normprops_table.h, unicode_norm.h).
 *   - src/common/unicode_category.c: unicode_category (the one function
 *     unicode_assigned needs) pasted verbatim below; its generated table
 *     vendored as vlmisc/unicode_category_table.h (+ unicode_category.h).
 *   - src/include/common/unicode_version.h: PG_UNICODE_VERSION.
 *   - src/backend/parser/scansup.c: downcase_truncate_identifier,
 *     downcase_identifier, truncate_identifier, scanner_isspace.
 *   - src/backend/utils/mb/mbutils.c: pg_unicode_to_server (UTF8 arm),
 *     pg_mblen_range, pg_mblen_with_len, pg_mbstrlen_with_len,
 *     pg_mbcliplen/pg_encoding_mbcliplen/cliplen.
 *   - src/common/wchar.c: pg_utf_mblen.
 *   - src/include/mb/pg_wchar.h: utf8_to_unicode, unicode_to_utf8,
 *     is_valid_unicode_codepoint, is_utf16_surrogate_first/second,
 *     surrogate_pair_to_codepoint (static inline helpers).
 *   - src/port/pgstrcasecmp.c: pg_strcasecmp.
 *
 * ENCODING FENCE (decision of record, matches the Rust driver's setup()):
 * the database encoding is pinned to UTF8 on BOTH sides. unistr,
 * unicode_assigned and the normalization family are UTF8-dependent in real
 * PG (they ereport unless GetDatabaseEncoding()==PG_UTF8); the Rust driver
 * pins mbutils::SetDatabaseEncoding(PG_UTF8) per thread, and this oracle
 * resolves the same fence at compile time (shims 2/3 below). The C locale is
 * the process default and never changed (isxdigit/isupper/tolower see the C
 * locale, exactly PG's postmaster-start LC_CTYPE=C posture for these arms).
 *
 * Shims (numbered; PLUMBING ONLY, never logic):
 *   1. fmgr unwrapping: PG_FUNCTION_ARGS / PG_GETARG_* become plain C
 *      parameters; PG_RETURN_BOOL/PG_RETURN_TEXT_P/PG_RETURN_NULL are macros
 *      that plain-return. `text_to_cstring(PG_GETARG_TEXT_PP(1))` for the
 *      normalization form argument becomes a `char *formstr` parameter (the
 *      detoast+copy is varlena framing done by the caller on both sides).
 *   2. DATABASE ENCODING FIXED = UTF8 (fence above):
 *      GetDatabaseEncoding() -> PG_UTF8 constant;
 *      pg_database_encoding_max_length()/pg_encoding_max_length -> 4;
 *      the pg_wchar_table[...].mblen function-pointer lookups resolve to
 *      pg_utf_mblen (the same resolution pg_name_io.c documents); the
 *      non-UTF8 conversion-proc tail of pg_unicode_to_server is unreachable
 *      and replaced by abort() at its (dead) call site.
 *   3. ereport(ERROR, ...) -> record the errcode class in the shared
 *      _Thread_local pg_diff_errcode (defined in pg_float_io.c) and longjmp
 *      back to the pg_diff_* driver entry. errmsg/errhint/errdetail argument
 *      lists are evaluated (cheap format-arg reads) but produce no text:
 *      message text is out of comparison scope. ereport(NOTICE, ...) is a
 *      no-op record (never reached: every vendored call site passes
 *      warn=false). elog(ERROR, ...) maps to the INTERNAL class.
 *      report_invalid_encoding is a shim that raises the 22021 class the
 *      real function ereports (its message machinery is framing).
 *   4. text is a 4-byte-plain-length + payload struct; SET_VARSIZE/VARSIZE
 *      store the untagged length (the 2-bit varlena tag is storage framing,
 *      not adt logic; both drivers compare payload bytes). VARDATA_ANY /
 *      VARSIZE_ANY_EXHDR degenerate to the 4B form (inputs here are never
 *      short-header: the driver builds them).
 *   5. StringInfo (initStringInfo/appendBinaryStringInfo/appendStringInfoChar/
 *      appendStringInfoString) -> compact arena-append buffer; growth policy
 *      is stringinfo.c plumbing, append semantics identical.
 *   6. List/NIL/lappend (SplitIdentifierString/SplitGUCList output) -> a
 *      flat arena-backed pointer vector; the drivers re-serialize the list
 *      as sentinel-joined bytes for comparison.
 *   7. palloc/palloc0/repalloc/pfree -> the TLS pointer arena below (models
 *      PG's memory-context reset; error-path longjmps strand allocations
 *      otherwise). Every pg_diff_* entry calls pg_diff_arena_reset() first.
 *   8. static linkage / pg_vlmisc_ #define-renames for every vendored symbol
 *      that another oracle TU in this crate also vendors (SYMBOL ISOLATION
 *      note in fuzz/core/build.rs): unicode_category (tablesfam vendors the
 *      whole unicode_category.c), unicode_normalize,
 *      unicode_is_normalized_quickcheck, varstr_levenshtein[_less_equal],
 *      pg_strcasecmp, and all mb/wchar helpers (pg_name_io.c vendors
 *      pg_utf_mblen/pg_mbcliplen). Bodies stay verbatim.
 *   9. pg_hton32/pg_hton64 (port/pg_bswap.h) -> __builtin_bswap32/64:
 *      the little-endian arm of the real header, byte-for-byte semantics on
 *      every host this crate builds on (arm64/x86-64 are LE).
 *
 * Errcode class constants (comparator plane; message text out of scope):
 *   1 = ERRCODE_INVALID_PARAMETER_VALUE      (22023)
 *   2 = ERRCODE_SYNTAX_ERROR                 (42601)
 *   3 = ERRCODE_CHARACTER_NOT_IN_REPERTOIRE  (22021, invalid byte sequence)
 *   4 = ERRCODE_FEATURE_NOT_SUPPORTED        (0A000; unreachable, fence)
 *   9 = INTERNAL (elog; unreachable guards)
 */

#include <assert.h>
#include <ctype.h>
#include <limits.h>
#include <setjmp.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

/* Shared TLS errcode channel (defined in csrc/pg_float_io.c). */
extern _Thread_local int pg_diff_errcode;

/* errcode classes (see header) */
#define ERRCODE_INVALID_PARAMETER_VALUE 1
#define ERRCODE_SYNTAX_ERROR 2
#define ERRCODE_CHARACTER_NOT_IN_REPERTOIRE 3
#define ERRCODE_FEATURE_NOT_SUPPORTED 4
#define ERRCODE_NAME_TOO_LONG 5 /* 42622: only ever at a NOTICE site here */
#define PG_VLMISC_ERR_INTERNAL 9

/* ---- fixed-width typedefs matching c.h on LP64 (shim prelude) ---- */
typedef int8_t int8;
typedef int16_t int16;
typedef int32_t int32;
typedef int64_t int64;
typedef uint8_t uint8;
typedef uint16_t uint16;
typedef uint32_t uint32;
typedef uint64_t uint64;
typedef unsigned int pg_wchar;
typedef uintptr_t Datum;
typedef unsigned int Oid;

#define Assert(x) ((void) 0)	/* NDEBUG backend build posture */
#define Min(x, y) ((x) < (y) ? (x) : (y))
#define Max(x, y) ((x) > (y) ? (x) : (y))
#define lengthof(array) (sizeof (array) / sizeof ((array)[0]))
#define BITS_PER_BYTE 8
#define IS_HIGHBIT_SET(ch) ((unsigned char) (ch) & 0x80)
#define unlikely(x) (x)
#define NAMEDATALEN 64
#define MAX_UNICODE_EQUIVALENT_STRING 16
#define FLEXIBLE_ARRAY_MEMBER			/* empty */
#define pg_attribute_unused()			/* empty */

/* SHIM 9: port/pg_bswap.h little-endian arm */
#define pg_hton32(x) __builtin_bswap32(x)
#define pg_hton64(x) __builtin_bswap64(x)

/* SHIM 2: encoding fence — database encoding pinned UTF8 */
#define PG_UTF8 6				/* pg_enc value, matches pg_wchar.h */
#define GetDatabaseEncoding() PG_UTF8
#define pg_database_encoding_max_length() 4

/* ---- SHIM 3: error plane ---- */
static _Thread_local jmp_buf pg_vlmisc_jmp;

static void
pg_vlmisc_error_jump(void)
{
	longjmp(pg_vlmisc_jmp, 1);
}

#define ERROR 21
#define NOTICE 18
/* both ereport styles parse: extra parens make the args one comma-expr */
#define ereport(elevel, ...) \
	do { \
		(void) (__VA_ARGS__); \
		if ((elevel) >= ERROR) \
			pg_vlmisc_error_jump(); \
		else \
			pg_diff_errcode = 0; /* NOTICE: nothing recorded */ \
	} while (0)
#define elog(elevel, ...) \
	do { \
		pg_diff_errcode = PG_VLMISC_ERR_INTERNAL; \
		if ((elevel) >= ERROR) \
			pg_vlmisc_error_jump(); \
	} while (0)

static int
errcode(int sqlerrcode)
{
	pg_diff_errcode = sqlerrcode;
	return 0;
}

static int
errmsg(const char *fmt, ...)
{
	(void) fmt;
	return 0;
}

static int
errhint(const char *fmt, ...)
{
	(void) fmt;
	return 0;
}

/* ---- SHIM 7: palloc arena (models PG's memory-context reset; error paths
 * strand allocations otherwise. Pattern proven on proofs/p1-lanej @
 * 7306d300196 — copied, not re-derived. Final-exec allocations stay rooted
 * in the arena, so LSan's exit scan is quiet without any manual free().)
 * MAX raised 64 -> 512 for this target: SplitIdentifierString's per-name
 * downcase palloc is pfree'd immediately (net one slot), but the split List
 * vector, StringInfo buffers and the normalization scratch can hold a few
 * dozen live slots on 2 KiB inputs; 512 is comfortable headroom. ---- */
#define PG_DIFF_ARENA_MAX 512
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
	void	   *p;
	int			i;

	for (i = 0; i < pg_diff_arena_n; i++)
	{
		if (pg_diff_arena[i] == old)
		{
			p = realloc(old, n);
			pg_diff_arena[i] = p;
			return p;
		}
	}
	assert(!"repalloc of a pointer the arena never issued");
	abort();
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
	/* abort-loud: freeing a pointer the arena never issued is a shim bug */
	assert(!"pfree of a pointer the arena never issued");
	abort();
}

#define palloc(n) pg_diff_palloc_impl(n)
#define palloc0(n) pg_diff_palloc0_impl(n)
#define repalloc(p, n) pg_diff_repalloc_impl((p), (n))
#define pfree(p) pg_diff_pfree_impl(p)

/* ---- SHIM 4: text / varlena framing ---- */
typedef struct varlena
{
	int32		vl_len_;		/* plain length incl. header (no 2-bit tag) */
	char		vl_dat[FLEXIBLE_ARRAY_MEMBER];
} text;

#define VARHDRSZ ((int32) sizeof(int32))
#define SET_VARSIZE(PTR, len) (((text *) (PTR))->vl_len_ = (len))
#define VARSIZE(PTR) (((text *) (PTR))->vl_len_)
#define VARDATA(PTR) (((text *) (PTR))->vl_dat)
#define VARDATA_ANY(PTR) VARDATA(PTR)
#define VARSIZE_ANY_EXHDR(PTR) (VARSIZE(PTR) - VARHDRSZ)

/* SHIM 1: fmgr result macros plain-return */
#define PG_RETURN_TEXT_P(x) return (text *) (x)
#define PG_RETURN_BOOL(x) return (x)
#define PG_RETURN_NULL() return NULL

/* ==================== SECTION 1: pg_wchar.h inline helpers (VERBATIM) ==== */

static inline bool
is_valid_unicode_codepoint(pg_wchar c)
{
	return (c > 0 && c <= 0x10FFFF);
}

static inline bool
is_utf16_surrogate_first(pg_wchar c)
{
	return (c >= 0xD800 && c <= 0xDBFF);
}

static inline bool
is_utf16_surrogate_second(pg_wchar c)
{
	return (c >= 0xDC00 && c <= 0xDFFF);
}

static inline pg_wchar
surrogate_pair_to_codepoint(pg_wchar first, pg_wchar second)
{
	return ((first & 0x3FF) << 10) + 0x10000 + (second & 0x3FF);
}

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

static inline unsigned char *
unicode_to_utf8(pg_wchar c, unsigned char *utf8string)
{
	if (c <= 0x7F)
	{
		utf8string[0] = c;
	}
	else if (c <= 0x7FF)
	{
		utf8string[0] = 0xC0 | ((c >> 6) & 0x1F);
		utf8string[1] = 0x80 | (c & 0x3F);
	}
	else if (c <= 0xFFFF)
	{
		utf8string[0] = 0xE0 | ((c >> 12) & 0x0F);
		utf8string[1] = 0x80 | ((c >> 6) & 0x3F);
		utf8string[2] = 0x80 | (c & 0x3F);
	}
	else
	{
		utf8string[0] = 0xF0 | ((c >> 18) & 0x07);
		utf8string[1] = 0x80 | ((c >> 12) & 0x3F);
		utf8string[2] = 0x80 | ((c >> 6) & 0x3F);
		utf8string[3] = 0x80 | (c & 0x3F);
	}

	return utf8string;
}

/* ---- src/common/wchar.c: pg_utf_mblen (verbatim; static per SHIM 8) ---- */
static int
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

/* ============ SECTION 2: mbutils.c helpers (VERBATIM modulo SHIM 2/3) ==== */

/*
 * SHIM 3: report_invalid_encoding raises exactly the errcode class the real
 * mbutils.c function ereports (ERRCODE_CHARACTER_NOT_IN_REPERTOIRE, 22021);
 * its %-escaped byte-dump message text is out of comparison scope.
 */
static void
pg_vlmisc_report_invalid_encoding(void)
{
	pg_diff_errcode = ERRCODE_CHARACTER_NOT_IN_REPERTOIRE;
	pg_vlmisc_error_jump();
}

#define report_invalid_encoding_db(mbstr, len, limit) pg_vlmisc_report_invalid_encoding()
#define report_invalid_encoding_int(enc, mbstr, len, limit) pg_vlmisc_report_invalid_encoding()
#define VALGRIND_CHECK_MEM_IS_DEFINED(a, b) ((void) 0)

/*
 * mbutils.c pg_mblen_range — verbatim modulo SHIM 2 (mblen table lookup
 * resolved to pg_utf_mblen) and SHIM 3.
 */
static int
pg_mblen_range(const char *mbstr, const char *end)
{
	int			length = pg_utf_mblen((const unsigned char *) mbstr);	/* SHIM 2 */

	Assert(end > mbstr);

	if (unlikely(mbstr + length > end))
		report_invalid_encoding_db(mbstr, length, end - mbstr);

	VALGRIND_CHECK_MEM_IS_DEFINED(mbstr, length);

	return length;
}

/* mbutils.c pg_mblen_with_len — verbatim modulo SHIM 2/3 */
static int
pg_mblen_with_len(const char *mbstr, int limit)
{
	int			length = pg_utf_mblen((const unsigned char *) mbstr);	/* SHIM 2 */

	Assert(limit >= 1);

	if (unlikely(length > limit))
		report_invalid_encoding_db(mbstr, length, limit);

	VALGRIND_CHECK_MEM_IS_DEFINED(mbstr, length);

	return length;
}

/* mbutils.c pg_mbstrlen_with_len — verbatim modulo SHIM 2 (max_length 4 =>
 * the single-byte fast return is never taken, exactly the UTF8 build) */
static int
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

/* mbutils.c cliplen — verbatim (unreachable under the UTF8 fence) */
static int
cliplen(const char *str, int len, int limit)
{
	int			l = 0;

	len = Min(len, limit);
	while (l < len && str[l])
		l++;
	return l;
}

/* mbutils.c pg_encoding_mbcliplen — verbatim modulo SHIM 2 (max_length 4,
 * mblen_fn resolved to pg_utf_mblen; same resolution as pg_name_io.c) */
static int
pg_encoding_mbcliplen(int encoding, const char *mbstr,
					  int len, int limit)
{
	int			clen = 0;
	int			l;

	/* optimization for single byte encoding */
	if (pg_database_encoding_max_length() == 1)
		return cliplen(mbstr, len, limit);

	while (len > 0 && *mbstr)
	{
		l = pg_utf_mblen((const unsigned char *) mbstr);	/* SHIM 2 */
		if ((clen + l) > limit)
			break;
		clen += l;
		if (clen == limit)
			break;
		len -= l;
		mbstr += l;
	}
	return clen;
}

/* mbutils.c pg_mbcliplen — verbatim modulo SHIM 2 */
static int
pg_mbcliplen(const char *mbstr, int len, int limit)
{
	return pg_encoding_mbcliplen(PG_UTF8, mbstr, len, limit);
}

/*
 * mbutils.c pg_unicode_to_server — verbatim through the UTF8 return; the
 * non-UTF8 conversion-proc tail is unreachable under the fence (SHIM 2) and
 * replaced by abort() (real code would look up Utf8ToServerConvProc there).
 */
static void
pg_unicode_to_server(pg_wchar c, unsigned char *s)
{
	int			server_encoding;

	/*
	 * Complain if invalid Unicode code point.  The choice of errcode here is
	 * debatable, but really our caller should have checked this anyway.
	 */
	if (!is_valid_unicode_codepoint(c))
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("invalid Unicode code point")));

	/* Otherwise, if it's in ASCII range, conversion is trivial */
	if (c <= 0x7F)
	{
		s[0] = (unsigned char) c;
		s[1] = '\0';
		return;
	}

	/* If the server encoding is UTF-8, we just need to reformat the code */
	server_encoding = GetDatabaseEncoding();
	if (server_encoding == PG_UTF8)
	{
		unicode_to_utf8(c, s);
		s[pg_utf_mblen(s)] = '\0';
		return;
	}

	/* SHIM 2: non-UTF8 conversion-proc arm unreachable under the fence */
	abort();
}

/* ==== SECTION 3: pgstrcasecmp.c pg_strcasecmp (verbatim; static, SHIM 8;
 * C locale never changed — see the fence note in the header) ==== */
static int
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

/* ======== SECTION 4: unicode_norm.c + tables (vendored files) ============ */

/* SHIM 8: rename the two extern entry points so this TU's copies cannot
 * collide with any other lane's vendoring of unicode_norm.c. */
#define unicode_normalize pg_vlmisc_unicode_normalize
#define unicode_is_normalized_quickcheck pg_vlmisc_unicode_is_normalized_quickcheck

#include "vlmisc/unicode_norm.c"

/* ======== SECTION 5: unicode_category (verbatim excerpt + table) ========= */

#include "vlmisc/unicode_category_table.h"

/* SHIM 8: tablesfam's oracle vendors the whole unicode_category.c (extern
 * unicode_category symbol); rename this TU's static verbatim copy. Placed
 * AFTER the header include so unicode_category.h's extern declaration keeps
 * its own (never-defined, never-referenced) name. */
#define unicode_category pg_vlmisc_unicode_category

/*
 * src/common/unicode_category.c unicode_category — verbatim body (static per
 * SHIM 8; the rest of unicode_category.c is not needed by unicode_assigned).
 */
static pg_unicode_category
unicode_category(pg_wchar code)
{
	int			min = 0;
	int			mid;
	int			max = lengthof(unicode_categories) - 1;

	Assert(code <= 0x10ffff);

	if (code < 0x80)
		return unicode_opt_ascii[code].category;

	while (max >= min)
	{
		mid = (min + max) / 2;
		if (code > unicode_categories[mid].last)
			min = mid + 1;
		else if (code < unicode_categories[mid].first)
			max = mid - 1;
		else
			return unicode_categories[mid].category;
	}

	return PG_U_UNASSIGNED;
}

/* src/include/common/unicode_version.h (verbatim) */
#define PG_UNICODE_VERSION		"16.0"

/* ============ SECTION 6: scansup.c (VERBATIM; static per SHIM 8) ========= */

static void truncate_identifier(char *ident, int len, bool warn);

/* scansup.c downcase_identifier — verbatim */
static char *
downcase_identifier(const char *ident, int len, bool warn, bool truncate)
{
	char	   *result;
	int			i;
	bool		enc_is_single_byte;

	result = palloc(len + 1);
	enc_is_single_byte = pg_database_encoding_max_length() == 1;

	/*
	 * SQL99 specifies Unicode-aware case normalization, which we don't yet
	 * have the infrastructure for.  Instead we use tolower() to provide a
	 * locale-aware translation.  However, there are some locales where this
	 * is not right either (eg, Turkish may do strange things with 'i' and
	 * 'I').  Our current compromise is to use tolower() for characters with
	 * the high bit set, as long as they aren't part of a multi-byte
	 * character, and use an ASCII-only downcasing for 7-bit characters.
	 */
	for (i = 0; i < len; i++)
	{
		unsigned char ch = (unsigned char) ident[i];

		if (ch >= 'A' && ch <= 'Z')
			ch += 'a' - 'A';
		else if (enc_is_single_byte && IS_HIGHBIT_SET(ch) && isupper(ch))
			ch = tolower(ch);
		result[i] = (char) ch;
	}
	result[i] = '\0';

	if (i >= NAMEDATALEN && truncate)
		truncate_identifier(result, i, warn);

	return result;
}

/* scansup.c downcase_truncate_identifier — verbatim */
static char *
downcase_truncate_identifier(const char *ident, int len, bool warn)
{
	return downcase_identifier(ident, len, warn, true);
}

/* scansup.c truncate_identifier — verbatim (warn is always false at the
 * vendored call sites, so the NOTICE ereport is never reached) */
static void
truncate_identifier(char *ident, int len, bool warn)
{
	if (len >= NAMEDATALEN)
	{
		len = pg_mbcliplen(ident, len, NAMEDATALEN - 1);
		if (warn)
			ereport(NOTICE,
					(errcode(ERRCODE_NAME_TOO_LONG),
					 errmsg("identifier \"%s\" will be truncated to \"%.*s\"",
							ident, len, ident)));
		ident[len] = '\0';
	}
}

/* scansup.c scanner_isspace — verbatim */
static bool
scanner_isspace(char ch)
{
	/* This must match scan.l's list of {space} characters */
	if (ch == ' ' ||
		ch == '\t' ||
		ch == '\n' ||
		ch == '\r' ||
		ch == '\v' ||
		ch == '\f')
		return true;
	return false;
}

/* ============ SECTION 7: StringInfo + List shims (SHIM 5/6) ============== */

typedef struct StringInfoData
{
	char	   *data;
	int			len;
	int			maxlen;
} StringInfoData;
typedef StringInfoData *StringInfo;

static void
initStringInfo(StringInfo str)
{
	str->maxlen = 1024;
	str->data = palloc(str->maxlen);
	str->len = 0;
	str->data[0] = '\0';
}

static void
appendBinaryStringInfo(StringInfo str, const char *data, int datalen)
{
	while (str->len + datalen + 1 > str->maxlen)
	{
		str->maxlen *= 2;
		str->data = repalloc(str->data, str->maxlen);
	}
	memcpy(str->data + str->len, data, datalen);
	str->len += datalen;
	str->data[str->len] = '\0';
}

static void
appendStringInfoChar(StringInfo str, char ch)
{
	appendBinaryStringInfo(str, &ch, 1);
}

static void
appendStringInfoString(StringInfo str, const char *s)
{
	appendBinaryStringInfo(str, s, (int) strlen(s));
}

/* SHIM 6: minimal List — flat arena pointer vector */
typedef struct List
{
	int			n;
	int			cap;
	char	  **items;
} List;

#define NIL ((List *) NULL)

static List *
lappend(List *list, void *datum)
{
	if (list == NIL)
	{
		list = palloc(sizeof(List));
		list->n = 0;
		list->cap = 8;
		list->items = palloc(list->cap * sizeof(char *));
	}
	if (list->n == list->cap)
	{
		list->cap *= 2;
		list->items = repalloc(list->items, list->cap * sizeof(char *));
	}
	list->items[list->n++] = datum;
	return list;
}

/* ============ SECTION 8: varlena.c (VERBATIM) ============================ */

/* varlena.c cstring_to_text_with_len — verbatim */
static text *
cstring_to_text_with_len(const char *s, int len)
{
	text	   *result = (text *) palloc(len + VARHDRSZ);

	SET_VARSIZE(result, len + VARHDRSZ);
	memcpy(VARDATA(result), s, len);

	return result;
}

/* varlena.c cstring_to_text — verbatim */
static text *
cstring_to_text(const char *s)
{
	return cstring_to_text_with_len(s, strlen(s));
}

/*
 * Workhorse for to_bin, to_oct, and to_hex.  Note that base must be > 1 and <=
 * 16.
 */
static inline text *
convert_to_base(uint64 value, int base)
{
	const char *digits = "0123456789abcdef";

	/* We size the buffer for to_bin's longest possible return value. */
	char		buf[sizeof(uint64) * BITS_PER_BYTE];
	char	   *const end = buf + sizeof(buf);
	char	   *ptr = end;

	Assert(base > 1);
	Assert(base <= 16);

	do
	{
		*--ptr = digits[value % base];
		value /= base;
	} while (ptr > buf && value);

	return cstring_to_text_with_len(ptr, end - ptr);
}

/* varlena.c unicode_norm_form_from_string — verbatim (formstr is a cstring
 * on both sides; the encoding check constant-folds true under the fence) */
static UnicodeNormalizationForm
unicode_norm_form_from_string(const char *formstr)
{
	UnicodeNormalizationForm form = -1;

	/*
	 * Might as well check this while we're here.
	 */
	if (GetDatabaseEncoding() != PG_UTF8)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("Unicode normalization can only be performed if server encoding is UTF8")));

	if (pg_strcasecmp(formstr, "NFC") == 0)
		form = UNICODE_NFC;
	else if (pg_strcasecmp(formstr, "NFD") == 0)
		form = UNICODE_NFD;
	else if (pg_strcasecmp(formstr, "NFKC") == 0)
		form = UNICODE_NFKC;
	else if (pg_strcasecmp(formstr, "NFKD") == 0)
		form = UNICODE_NFKD;
	else
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid normalization form: %s", formstr)));

	return form;
}

/* varlena.c unicode_assigned — verbatim body (SHIM 1: text *input param) */
static bool
unicode_assigned_impl(text *input)
{
	unsigned char *p;
	int			size;

	if (GetDatabaseEncoding() != PG_UTF8)
		ereport(ERROR,
				(errmsg("Unicode categorization can only be performed if server encoding is UTF8")));

	/* convert to pg_wchar */
	size = pg_mbstrlen_with_len(VARDATA_ANY(input), VARSIZE_ANY_EXHDR(input));
	p = (unsigned char *) VARDATA_ANY(input);
	for (int i = 0; i < size; i++)
	{
		pg_wchar	uchar = utf8_to_unicode(p);
		int			category = unicode_category(uchar);

		if (category == PG_U_UNASSIGNED)
			PG_RETURN_BOOL(false);

		p += pg_utf_mblen(p);
	}

	PG_RETURN_BOOL(true);
}

/* varlena.c unicode_normalize_func — verbatim body (SHIM 1: text *input and
 * char *formstr params in place of PG_GETARG_TEXT_PP + text_to_cstring) */
static text *
unicode_normalize_func_impl(text *input, char *formstr)
{
	UnicodeNormalizationForm form;
	int			size;
	pg_wchar   *input_chars;
	pg_wchar   *output_chars;
	unsigned char *p;
	text	   *result;
	int			i;

	form = unicode_norm_form_from_string(formstr);

	/* convert to pg_wchar */
	size = pg_mbstrlen_with_len(VARDATA_ANY(input), VARSIZE_ANY_EXHDR(input));
	input_chars = palloc((size + 1) * sizeof(pg_wchar));
	p = (unsigned char *) VARDATA_ANY(input);
	for (i = 0; i < size; i++)
	{
		input_chars[i] = utf8_to_unicode(p);
		p += pg_utf_mblen(p);
	}
	input_chars[i] = (pg_wchar) '\0';
	Assert((char *) p == VARDATA_ANY(input) + VARSIZE_ANY_EXHDR(input));

	/* action */
	output_chars = unicode_normalize(form, input_chars);

	/* convert back to UTF-8 string */
	size = 0;
	for (pg_wchar *wp = output_chars; *wp; wp++)
	{
		unsigned char buf[4];

		unicode_to_utf8(*wp, buf);
		size += pg_utf_mblen(buf);
	}

	result = palloc(size + VARHDRSZ);
	SET_VARSIZE(result, size + VARHDRSZ);

	p = (unsigned char *) VARDATA_ANY(result);
	for (pg_wchar *wp = output_chars; *wp; wp++)
	{
		unicode_to_utf8(*wp, p);
		p += pg_utf_mblen(p);
	}
	Assert((char *) p == (char *) result + size + VARHDRSZ);

	PG_RETURN_TEXT_P(result);
}

/* varlena.c unicode_is_normalized — verbatim body (SHIM 1 as above) */
static bool
unicode_is_normalized_impl(text *input, char *formstr)
{
	UnicodeNormalizationForm form;
	int			size;
	pg_wchar   *input_chars;
	pg_wchar   *output_chars;
	unsigned char *p;
	int			i;
	UnicodeNormalizationQC quickcheck;
	int			output_size;
	bool		result;

	form = unicode_norm_form_from_string(formstr);

	/* convert to pg_wchar */
	size = pg_mbstrlen_with_len(VARDATA_ANY(input), VARSIZE_ANY_EXHDR(input));
	input_chars = palloc((size + 1) * sizeof(pg_wchar));
	p = (unsigned char *) VARDATA_ANY(input);
	for (i = 0; i < size; i++)
	{
		input_chars[i] = utf8_to_unicode(p);
		p += pg_utf_mblen(p);
	}
	input_chars[i] = (pg_wchar) '\0';
	Assert((char *) p == VARDATA_ANY(input) + VARSIZE_ANY_EXHDR(input));

	/* quick check (see UAX #15) */
	quickcheck = unicode_is_normalized_quickcheck(form, input_chars);
	if (quickcheck == UNICODE_NORM_QC_YES)
		PG_RETURN_BOOL(true);
	else if (quickcheck == UNICODE_NORM_QC_NO)
		PG_RETURN_BOOL(false);

	/* normalize and compare with original */
	output_chars = unicode_normalize(form, input_chars);

	output_size = 0;
	for (pg_wchar *wp = output_chars; *wp; wp++)
		output_size++;

	result = (size == output_size) &&
		(memcmp(input_chars, output_chars, size * sizeof(pg_wchar)) == 0);

	PG_RETURN_BOOL(result);
}

/*
 * Check if first n chars are hexadecimal digits
 */
static bool
isxdigits_n(const char *instr, size_t n)
{
	for (size_t i = 0; i < n; i++)
		if (!isxdigit((unsigned char) instr[i]))
			return false;

	return true;
}

static unsigned int
hexval(unsigned char c)
{
	if (c >= '0' && c <= '9')
		return c - '0';
	if (c >= 'a' && c <= 'f')
		return c - 'a' + 0xA;
	if (c >= 'A' && c <= 'F')
		return c - 'A' + 0xA;
	elog(ERROR, "invalid hexadecimal digit");
	return 0;					/* not reached */
}

/*
 * Translate string with hexadecimal digits to number
 */
static unsigned int
hexval_n(const char *instr, size_t n)
{
	unsigned int result = 0;

	for (size_t i = 0; i < n; i++)
		result += hexval(instr[i]) << (4 * (n - i - 1));

	return result;
}

/*
 * Replaces Unicode escape sequences by Unicode characters
 * — varlena.c unistr, verbatim body (SHIM 1: text *input_text param).
 */
static text *
unistr_impl(text *input_text)
{
	char	   *instr;
	int			len;
	StringInfoData str;
	text	   *result;
	pg_wchar	pair_first = 0;
	char		cbuf[MAX_UNICODE_EQUIVALENT_STRING + 1];

	instr = VARDATA_ANY(input_text);
	len = VARSIZE_ANY_EXHDR(input_text);

	initStringInfo(&str);

	while (len > 0)
	{
		if (instr[0] == '\\')
		{
			if (len >= 2 &&
				instr[1] == '\\')
			{
				if (pair_first)
					goto invalid_pair;
				appendStringInfoChar(&str, '\\');
				instr += 2;
				len -= 2;
			}
			else if ((len >= 5 && isxdigits_n(instr + 1, 4)) ||
					 (len >= 6 && instr[1] == 'u' && isxdigits_n(instr + 2, 4)))
			{
				pg_wchar	unicode;
				int			offset = instr[1] == 'u' ? 2 : 1;

				unicode = hexval_n(instr + offset, 4);

				if (!is_valid_unicode_codepoint(unicode))
					ereport(ERROR,
							errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							errmsg("invalid Unicode code point: %04X", unicode));

				if (pair_first)
				{
					if (is_utf16_surrogate_second(unicode))
					{
						unicode = surrogate_pair_to_codepoint(pair_first, unicode);
						pair_first = 0;
					}
					else
						goto invalid_pair;
				}
				else if (is_utf16_surrogate_second(unicode))
					goto invalid_pair;

				if (is_utf16_surrogate_first(unicode))
					pair_first = unicode;
				else
				{
					pg_unicode_to_server(unicode, (unsigned char *) cbuf);
					appendStringInfoString(&str, cbuf);
				}

				instr += 4 + offset;
				len -= 4 + offset;
			}
			else if (len >= 8 && instr[1] == '+' && isxdigits_n(instr + 2, 6))
			{
				pg_wchar	unicode;

				unicode = hexval_n(instr + 2, 6);

				if (!is_valid_unicode_codepoint(unicode))
					ereport(ERROR,
							errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							errmsg("invalid Unicode code point: %04X", unicode));

				if (pair_first)
				{
					if (is_utf16_surrogate_second(unicode))
					{
						unicode = surrogate_pair_to_codepoint(pair_first, unicode);
						pair_first = 0;
					}
					else
						goto invalid_pair;
				}
				else if (is_utf16_surrogate_second(unicode))
					goto invalid_pair;

				if (is_utf16_surrogate_first(unicode))
					pair_first = unicode;
				else
				{
					pg_unicode_to_server(unicode, (unsigned char *) cbuf);
					appendStringInfoString(&str, cbuf);
				}

				instr += 8;
				len -= 8;
			}
			else if (len >= 10 && instr[1] == 'U' && isxdigits_n(instr + 2, 8))
			{
				pg_wchar	unicode;

				unicode = hexval_n(instr + 2, 8);

				if (!is_valid_unicode_codepoint(unicode))
					ereport(ERROR,
							errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							errmsg("invalid Unicode code point: %04X", unicode));

				if (pair_first)
				{
					if (is_utf16_surrogate_second(unicode))
					{
						unicode = surrogate_pair_to_codepoint(pair_first, unicode);
						pair_first = 0;
					}
					else
						goto invalid_pair;
				}
				else if (is_utf16_surrogate_second(unicode))
					goto invalid_pair;

				if (is_utf16_surrogate_first(unicode))
					pair_first = unicode;
				else
				{
					pg_unicode_to_server(unicode, (unsigned char *) cbuf);
					appendStringInfoString(&str, cbuf);
				}

				instr += 10;
				len -= 10;
			}
			else
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("invalid Unicode escape"),
						 errhint("Unicode escapes must be \\XXXX, \\+XXXXXX, \\uXXXX, or \\UXXXXXXXX.")));
		}
		else
		{
			if (pair_first)
				goto invalid_pair;

			appendStringInfoChar(&str, *instr++);
			len--;
		}
	}

	/* unfinished surrogate pair? */
	if (pair_first)
		goto invalid_pair;

	result = cstring_to_text_with_len(str.data, str.len);
	pfree(str.data);

	PG_RETURN_TEXT_P(result);

invalid_pair:
	ereport(ERROR,
			(errcode(ERRCODE_SYNTAX_ERROR),
			 errmsg("invalid Unicode surrogate pair")));
	PG_RETURN_NULL();			/* keep compiler quiet */
}

/*
 * SplitIdentifierString --- parse a string containing identifiers
 * — varlena.c, verbatim.
 */
static bool
SplitIdentifierString(char *rawstring, char separator,
					  List **namelist)
{
	char	   *nextp = rawstring;
	bool		done = false;

	*namelist = NIL;

	while (scanner_isspace(*nextp))
		nextp++;				/* skip leading whitespace */

	if (*nextp == '\0')
		return true;			/* allow empty string */

	/* At the top of the loop, we are at start of a new identifier. */
	do
	{
		char	   *curname;
		char	   *endp;

		if (*nextp == '"')
		{
			/* Quoted name --- collapse quote-quote pairs, no downcasing */
			curname = nextp + 1;
			for (;;)
			{
				endp = strchr(nextp + 1, '"');
				if (endp == NULL)
					return false;	/* mismatched quotes */
				if (endp[1] != '"')
					break;		/* found end of quoted name */
				/* Collapse adjacent quotes into one quote, and look again */
				memmove(endp, endp + 1, strlen(endp));
				nextp = endp;
			}
			/* endp now points at the terminating quote */
			nextp = endp + 1;
		}
		else
		{
			/* Unquoted name --- extends to separator or whitespace */
			char	   *downname;
			int			len;

			curname = nextp;
			while (*nextp && *nextp != separator &&
				   !scanner_isspace(*nextp))
				nextp++;
			endp = nextp;
			if (curname == nextp)
				return false;	/* empty unquoted name not allowed */

			/*
			 * Downcase the identifier, using same code as main lexer does.
			 *
			 * XXX because we want to overwrite the input in-place, we cannot
			 * support a downcasing transformation that increases the string
			 * length.  This is not a problem given the current implementation
			 * of downcase_truncate_identifier, but we'll probably have to do
			 * something about this someday.
			 */
			len = endp - curname;
			downname = downcase_truncate_identifier(curname, len, false);
			Assert(strlen(downname) <= len);
			strncpy(curname, downname, len);	/* strncpy is required here */
			pfree(downname);
		}

		while (scanner_isspace(*nextp))
			nextp++;			/* skip trailing whitespace */

		if (*nextp == separator)
		{
			nextp++;
			while (scanner_isspace(*nextp))
				nextp++;		/* skip leading whitespace for next */
			/* we expect another name, so done remains false */
		}
		else if (*nextp == '\0')
			done = true;
		else
			return false;		/* invalid syntax */

		/* Now safe to overwrite separator with a null */
		*endp = '\0';

		/* Truncate name if it's overlength */
		truncate_identifier(curname, strlen(curname), false);

		/*
		 * Finished isolating current name --- add it to list
		 */
		*namelist = lappend(*namelist, curname);

		/* Loop back if we didn't reach end of string */
	} while (!done);

	return true;
}

/*
 * SplitGUCList --- parse a string containing identifiers or file names
 * — varlena.c, verbatim.
 */
static bool
SplitGUCList(char *rawstring, char separator,
			 List **namelist)
{
	char	   *nextp = rawstring;
	bool		done = false;

	*namelist = NIL;

	while (scanner_isspace(*nextp))
		nextp++;				/* skip leading whitespace */

	if (*nextp == '\0')
		return true;			/* allow empty string */

	/* At the top of the loop, we are at start of a new identifier. */
	do
	{
		char	   *curname;
		char	   *endp;

		if (*nextp == '"')
		{
			/* Quoted name --- collapse quote-quote pairs */
			curname = nextp + 1;
			for (;;)
			{
				endp = strchr(nextp + 1, '"');
				if (endp == NULL)
					return false;	/* mismatched quotes */
				if (endp[1] != '"')
					break;		/* found end of quoted name */
				/* Collapse adjacent quotes into one quote, and look again */
				memmove(endp, endp + 1, strlen(endp));
				nextp = endp;
			}
			/* endp now points at the terminating quote */
			nextp = endp + 1;
		}
		else
		{
			/* Unquoted name --- extends to separator or whitespace */
			curname = nextp;
			while (*nextp && *nextp != separator &&
				   !scanner_isspace(*nextp))
				nextp++;
			endp = nextp;
			if (curname == nextp)
				return false;	/* empty unquoted name not allowed */
		}

		while (scanner_isspace(*nextp))
			nextp++;			/* skip trailing whitespace */

		if (*nextp == separator)
		{
			nextp++;
			while (scanner_isspace(*nextp))
				nextp++;		/* skip leading whitespace for next */
			/* we expect another name, so done remains false */
		}
		else if (*nextp == '\0')
			done = true;
		else
			return false;		/* invalid syntax */

		/* Now safe to overwrite separator with a null */
		*endp = '\0';

		/*
		 * Finished isolating current name --- add it to list
		 */
		*namelist = lappend(*namelist, curname);

		/* Loop back if we didn't reach end of string */
	} while (!done);

	return true;
}

/* ============ SECTION 9: levenshtein.c (vendored file, included twice
 * exactly as varlena.c does; rest_of_char_same is varlena.c's, verbatim) === */

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

/* SHIM 8: prefix-rename the two instantiations; static via the int-return
 * declaration below is not possible without editing the vendored file, so
 * the rename alone provides the isolation. */
#define varstr_levenshtein pg_vlmisc_varstr_levenshtein
#define varstr_levenshtein_less_equal pg_vlmisc_varstr_levenshtein_less_equal

/* Expand each Levenshtein distance variant */
#include "vlmisc/levenshtein.c"
#define LEVENSHTEIN_LESS_EQUAL
#include "vlmisc/levenshtein.c"

/* ========== SECTION 10: fuzz-facing driver entries (NOT Postgres code) ==== */

/*
 * Every entry: arena reset FIRST, then errcode = 0, then setjmp arming for
 * the ereport shim. Return 0 on success, the errcode class (>0) on error.
 */
#define PG_VLMISC_ENTRY_PROLOGUE() \
	do { \
		pg_diff_arena_reset(); \
		pg_diff_errcode = 0; \
		if (setjmp(pg_vlmisc_jmp) != 0) \
			return pg_diff_errcode ? pg_diff_errcode : PG_VLMISC_ERR_INTERNAL; \
	} while (0)

/* copy a text result into the caller's buffer; abort-loud on cap overflow
 * (a shim bug: the Rust driver sizes caps from the documented bounds) */
static void
pg_vlmisc_copy_text(const text *t, unsigned char *out, int outcap, int *outlen)
{
	int			len = VARSIZE(t) - VARHDRSZ;

	if (len > outcap)
		abort();
	memcpy(out, VARDATA((text *) t), len);
	*outlen = len;
}

/* to_hex/to_bin/to_oct family [oids 2089/2090/6330/6331/6332/6333]:
 * driver bodies are the verbatim one-liners from varlena.c's to_* functions
 * (SHIM 1: PG_GETARG_INT32/64 -> parameter, PG_RETURN_TEXT_P -> copy-out). */
int
pg_diff_to_bin32(int32 arg, unsigned char *out, int outcap, int *outlen)
{
	uint64		value;

	PG_VLMISC_ENTRY_PROLOGUE();
	value = (uint32) arg;		/* verbatim cast from to_bin32 */
	pg_vlmisc_copy_text(convert_to_base(value, 2), out, outcap, outlen);
	return 0;
}

int
pg_diff_to_bin64(int64 arg, unsigned char *out, int outcap, int *outlen)
{
	uint64		value;

	PG_VLMISC_ENTRY_PROLOGUE();
	value = (uint64) arg;		/* verbatim cast from to_bin64 */
	pg_vlmisc_copy_text(convert_to_base(value, 2), out, outcap, outlen);
	return 0;
}

int
pg_diff_to_oct32(int32 arg, unsigned char *out, int outcap, int *outlen)
{
	uint64		value;

	PG_VLMISC_ENTRY_PROLOGUE();
	value = (uint32) arg;		/* verbatim cast from to_oct32 */
	pg_vlmisc_copy_text(convert_to_base(value, 8), out, outcap, outlen);
	return 0;
}

int
pg_diff_to_oct64(int64 arg, unsigned char *out, int outcap, int *outlen)
{
	uint64		value;

	PG_VLMISC_ENTRY_PROLOGUE();
	value = (uint64) arg;		/* verbatim cast from to_oct64 */
	pg_vlmisc_copy_text(convert_to_base(value, 8), out, outcap, outlen);
	return 0;
}

int
pg_diff_to_hex32(int32 arg, unsigned char *out, int outcap, int *outlen)
{
	uint64		value;

	PG_VLMISC_ENTRY_PROLOGUE();
	value = (uint32) arg;		/* verbatim cast from to_hex32 */
	pg_vlmisc_copy_text(convert_to_base(value, 16), out, outcap, outlen);
	return 0;
}

int
pg_diff_to_hex64(int64 arg, unsigned char *out, int outcap, int *outlen)
{
	uint64		value;

	PG_VLMISC_ENTRY_PROLOGUE();
	value = (uint64) arg;		/* verbatim cast from to_hex64 */
	pg_vlmisc_copy_text(convert_to_base(value, 16), out, outcap, outlen);
	return 0;
}

/* unistr [oid 6198]; output never exceeds the input length */
int
pg_diff_unistr(const unsigned char *in, int len,
			   unsigned char *out, int outcap, int *outlen)
{
	text	   *input_text;
	text	   *result;

	PG_VLMISC_ENTRY_PROLOGUE();
	input_text = cstring_to_text_with_len((const char *) in, len);
	result = unistr_impl(input_text);
	pg_vlmisc_copy_text(result, out, outcap, outlen);
	return 0;
}

/* unicode_version [oid 4549] — verbatim one-liner from varlena.c */
int
pg_diff_unicode_version(unsigned char *out, int outcap, int *outlen)
{
	PG_VLMISC_ENTRY_PROLOGUE();
	pg_vlmisc_copy_text(cstring_to_text(PG_UNICODE_VERSION), out, outcap, outlen);
	return 0;
}

/* unicode_assigned [oid 6105] */
int
pg_diff_unicode_assigned(const unsigned char *in, int len, int *result)
{
	text	   *input;

	PG_VLMISC_ENTRY_PROLOGUE();
	input = cstring_to_text_with_len((const char *) in, len);
	*result = unicode_assigned_impl(input) ? 1 : 0;
	return 0;
}

/* unicode_normalize_func [oid 4350] */
int
pg_diff_unicode_normalize(const unsigned char *in, int len, const char *formstr,
						  unsigned char *out, int outcap, int *outlen)
{
	text	   *input;
	text	   *result;

	PG_VLMISC_ENTRY_PROLOGUE();
	input = cstring_to_text_with_len((const char *) in, len);
	result = unicode_normalize_func_impl(input, (char *) formstr);
	pg_vlmisc_copy_text(result, out, outcap, outlen);
	return 0;
}

/* unicode_is_normalized [oid 4351] */
int
pg_diff_unicode_is_normalized(const unsigned char *in, int len,
							  const char *formstr, int *result)
{
	text	   *input;

	PG_VLMISC_ENTRY_PROLOGUE();
	input = cstring_to_text_with_len((const char *) in, len);
	*result = unicode_is_normalized_impl(input, (char *) formstr) ? 1 : 0;
	return 0;
}

/* varstr_levenshtein (levenshtein.c, plain instantiation; non-SQL helper) */
int
pg_diff_varstr_levenshtein(const unsigned char *s, int slen,
						   const unsigned char *t, int tlen,
						   int32 ins_c, int32 del_c, int32 sub_c,
						   int trusted, int32 *result)
{
	PG_VLMISC_ENTRY_PROLOGUE();
	*result = pg_vlmisc_varstr_levenshtein((const char *) s, slen,
										   (const char *) t, tlen,
										   ins_c, del_c, sub_c,
										   trusted != 0);
	return 0;
}

/* varstr_levenshtein_less_equal (LEVENSHTEIN_LESS_EQUAL instantiation) */
int
pg_diff_varstr_levenshtein_less_equal(const unsigned char *s, int slen,
									  const unsigned char *t, int tlen,
									  int32 ins_c, int32 del_c, int32 sub_c,
									  int32 max_d, int trusted, int32 *result)
{
	PG_VLMISC_ENTRY_PROLOGUE();
	*result = pg_vlmisc_varstr_levenshtein_less_equal((const char *) s, slen,
													  (const char *) t, tlen,
													  ins_c, del_c, sub_c,
													  max_d, trusted != 0);
	return 0;
}

/* join a List of cstrings with the 0x1F unit separator (comparison image) */
static void
pg_vlmisc_join_list(const List *namelist, unsigned char *out, int outcap,
					int *outlen)
{
	int			pos = 0;
	int			i;

	for (i = 0; namelist && i < namelist->n; i++)
	{
		int			l = (int) strlen(namelist->items[i]);

		if (pos + l + (i > 0 ? 1 : 0) > outcap)
			abort();			/* shim bug: caller sizes the cap */
		if (i > 0)
			out[pos++] = 0x1F;
		memcpy(out + pos, namelist->items[i], l);
		pos += l;
	}
	*outlen = pos;
}

/*
 * SplitIdentifierString / SplitGUCList (non-SQL helpers). Returns 0 with the
 * sentinel-joined identifier list on success, 1 for C `false` (syntax
 * error). rawstring is copied into the arena: the C API scribbles on it.
 */
int
pg_diff_split_identifier_string(const unsigned char *raw, int rawlen,
								char separator,
								unsigned char *out, int outcap, int *outlen)
{
	char	   *rawstring;
	List	   *namelist = NIL;

	PG_VLMISC_ENTRY_PROLOGUE();
	rawstring = palloc(rawlen + 1);
	memcpy(rawstring, raw, rawlen);
	rawstring[rawlen] = '\0';
	if (!SplitIdentifierString(rawstring, separator, &namelist))
		return 1;
	pg_vlmisc_join_list(namelist, out, outcap, outlen);
	return 0;
}

int
pg_diff_split_guc_list(const unsigned char *raw, int rawlen,
					   char separator,
					   unsigned char *out, int outcap, int *outlen)
{
	char	   *rawstring;
	List	   *namelist = NIL;

	PG_VLMISC_ENTRY_PROLOGUE();
	rawstring = palloc(rawlen + 1);
	memcpy(rawstring, raw, rawlen);
	rawstring[rawlen] = '\0';
	if (!SplitGUCList(rawstring, separator, &namelist))
		return 1;
	pg_vlmisc_join_list(namelist, out, outcap, outlen);
	return 0;
}
