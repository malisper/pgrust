/*
 * pg_like_io.c: vendored PostgreSQL 18.3 C oracle for the like_diff
 * differential fuzz target (decoder_fuzz::like_diff; crate under test
 * crates/backend/utils/adt/like).
 *
 * Provenance (all bodies VERBATIM modulo the documented shims), from the
 * repo's vendored ground-truth checkout ../pgrust-fabled/vendor/postgres-src
 * @ 62d6c7d3df6287f1bd83199c1a746e50d31571a0 (PostgreSQL 18.3, Stamp-18.3):
 *   - src/backend/utils/adt/like.c: LIKE_TRUE/FALSE/ABORT defines, static
 *     prototypes, wchareq, SB_lower_char, NextByte, the FOUR like_match.c
 *     stamping macro-setups, GenericMatchText, Generic_Text_IC_like, and the
 *     fmgr entry bodies (textlike/textnlike/namelike/namenlike/texticlike/
 *     texticnlike/nameiclike/nameicnlike/bytealike/byteanlike/like_escape/
 *     like_escape_bytea) unwrapped per shim 1.
 *   - src/backend/utils/adt/like_match.c: pasted VERBATIM once per stamping
 *     (SB_MatchText + SB_do_like_escape, MB_MatchText + MB_do_like_escape,
 *     SB_IMatchText, UTF8_MatchText), exactly as like.c #includes it.
 *   - src/backend/utils/adt/formatting.c: asc_tolower (the str_tolower
 *     ctype_is_c arm) — VERBATIM.
 *   - src/port/pgstrcasecmp.c: pg_ascii_tolower — VERBATIM.
 *   - src/common/wchar.c: pg_utf_mblen — VERBATIM.
 *   - src/backend/utils/mmgr/mcxt.c: pnstrdup — VERBATIM (palloc = arena).
 *   - src/backend/utils/adt/varlena.c: cstring_to_text,
 *     cstring_to_text_with_len — VERBATIM (palloc = arena).
 *   - src/backend/utils/mb/mbutils.c: pg_mblen_with_len — VERBATIM body with
 *     the pg_wchar_table mblen lookup resolved per shim 4 and the VALGRIND
 *     client checks dropped (instrumentation, not semantics).
 *
 * Shims (PLUMBING ONLY, never logic):
 *   1. fmgr unwrapping: PG_FUNCTION_ARGS -> plain C signatures over
 *      (ptr,len) byte views and 64-byte NameData blocks; PG_GETARG_TEXT_PP
 *      -> a 4B-uncompressed inline text image built by the entry
 *      (pg_like_mktext); PG_RETURN_BOOL -> *out int; PG_GET_COLLATION() ->
 *      explicit collation parameter.
 *   2. ereport(ERROR, (errcode(X), ...)) -> errcode() records X in the
 *      shared _Thread_local pg_diff_errcode plane (defined in
 *      pg_float_io.c) and ereport longjmps out through pg_like_jmp;
 *      errmsg/errhint are swallowed (message text out of comparator
 *      scope). Errcode classes: 1 = 22025 ERRCODE_INVALID_ESCAPE_SEQUENCE,
 *      2 = 42P22 ERRCODE_INDETERMINATE_COLLATION, 3 = 0A000
 *      ERRCODE_FEATURE_NOT_SUPPORTED, 4 = 22021 invalid byte sequence
 *      (report_invalid_encoding_db from pg_mblen_with_len).
 *   3. palloc/pfree -> the TLS pointer arena below (models PG's
 *      memory-context reset; every pg_diff_like_* entry resets it first, so
 *      error-path longjmps cannot leak — the 2026-07-31 LSan incident
 *      class).
 *   4. DATABASE ENCODING = settable two-plane static
 *      (pg_diff_like_set_encoding): UTF8 (max_length 4, mblen =
 *      pg_utf_mblen) or LATIN1 (max_length 1, mblen = 1) — exactly the two
 *      pg_wchar_table rows those planes dispatch to. The Rust driver pins
 *      mbutils::SetDatabaseEncoding to the same plane per exec.
 *      report_invalid_encoding_db -> errcode class 4 + longjmp (the 22021
 *      the real one raises; message text out of scope).
 *   5. LOCALE = C collation only (drivers pass C_COLLATION_OID or
 *      InvalidOid): pg_newlocale_from_collation -> a static C-locale
 *      pg_locale struct with deterministic = true, ctype_is_c = true,
 *      collate_is_c = true, is_default = false, provider = COLLPROVIDER_LIBC
 *      (matching the shipped pg_locale::C_LOCALE the Rust side resolves for
 *      collation 950). The arms this pins dead are shimmed ABORT-LOUD, not
 *      stubbed: pg_tolower / tolower_l (SB_lower_char's non-ctype_is_c
 *      arms) and pg_strncoll (like_match.c's nondeterministic-collation
 *      arm) abort() if ever reached, so a silent semantic drift is
 *      impossible.
 *   6. lower(): Generic_Text_IC_like's multibyte arm calls
 *      DirectFunctionCall1Coll(lower, ...). Under shim 5 (ctype_is_c) the
 *      real call chain is lower -> str_tolower -> asc_tolower; the macro
 *      below routes the call sites to pg_like_lower_call, whose body is
 *      lower()'s (formatting.c): str_tolower's ctype_is_c arm = VERBATIM
 *      asc_tolower, then cstring_to_text + pfree, both verbatim.
 *      DirectFunctionCall1(name_text, ...) in nameiclike/nameicnlike
 *      likewise routes to pg_like_name_text_call = name.c name_text's body
 *      (cstring_to_text(NameStr(*s))).
 *   7. check_stack_depth() -> no-op. MatchText recursion depth is bounded
 *      by the pattern length; the DRIVER caps every input piece at 512
 *      bytes (documented there), so the guard C uses against unbounded
 *      recursion cannot be needed. CHECK_FOR_INTERRUPTS() -> no-op
 *      (single-threaded fuzz exec, no pending interrupts plane).
 *   8. varlena macros (VARDATA/VARDATA_ANY/VARSIZE_ANY/VARSIZE_ANY_EXHDR/
 *      SET_VARSIZE): little-endian 4B-uncompressed images only — the only
 *      kind the entries build (no TOAST/short heads reach this oracle).
 *
 * Comparator planes owned by the Rust driver (core/src/like_diff.rs):
 * result value (bool / escape output bytes) + Ok-vs-Err verdict +
 * errcode class.
 */

#include <assert.h>
#include <setjmp.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include "pg_oracle_guard.h"	/* oracle-serialization holder check */

/* Shared TLS errcode channel (defined in csrc/pg_float_io.c). */
extern _Thread_local int pg_diff_errcode;

/* Errcode classes (shim 2). */
#define PG_DIFF_LIKE_ERR_INVALID_ESCAPE 1	/* 22025 */
#define PG_DIFF_LIKE_ERR_INDETERMINATE_COLLATION 2	/* 42P22 */
#define PG_DIFF_LIKE_ERR_FEATURE_NOT_SUPPORTED 3	/* 0A000 */
#define PG_DIFF_LIKE_ERR_INVALID_BYTE_SEQ 4 /* 22021 */

/* palloc arena shim: PostgreSQL frees these via memory-context reset; the
 * oracle mirrors that with a TLS pointer arena reset at every pg_diff_*
 * dispatcher entry, so error-path longjmp exits cannot leak. (Three LSan
 * incidents of the naive palloc->malloc mapping on 2026-07-31; pattern
 * proven on proofs/p1-lanej @ 7306d300196 — copied, not re-derived.
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
#define pfree(p) pg_diff_pfree_impl(p)

/* ---- shim 2: ereport -> errcode capture + longjmp ---- */

static _Thread_local jmp_buf pg_like_jmp;

static void
pg_like_ereport_longjmp(void)
{
	longjmp(pg_like_jmp, 1);
}

/* The parenthesized (errcode(..), errmsg(..), ...) auxiliary list is a comma
 * expression: errcode() records the class, the message calls are swallowed,
 * then control longjmps out — like PG's ERROR level, no fallthrough. */
#define ereport(elevel, rest) \
	do { (void) (rest); pg_like_ereport_longjmp(); } while (0)

static int
errcode(int c)
{
	pg_diff_errcode = c;
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

#define ERRCODE_INVALID_ESCAPE_SEQUENCE PG_DIFF_LIKE_ERR_INVALID_ESCAPE
#define ERRCODE_INDETERMINATE_COLLATION PG_DIFF_LIKE_ERR_INDETERMINATE_COLLATION
#define ERRCODE_FEATURE_NOT_SUPPORTED PG_DIFF_LIKE_ERR_FEATURE_NOT_SUPPORTED

/* ---- shim 7 ---- */
#define check_stack_depth() ((void) 0)
#define CHECK_FOR_INTERRUPTS() ((void) 0)
#define Assert(condition) assert(condition)
#ifndef unlikely
#define unlikely(x) (x)
#endif
typedef size_t Size;

/* ---- core PG types (c.h shapes on LP64) ---- */
typedef uint32_t Oid;
#define InvalidOid ((Oid) 0)
#define OidIsValid(objectId) ((bool) ((objectId) != InvalidOid))
#define C_COLLATION_OID 950		/* catalog/pg_collation.h */

typedef struct varlena
{
	char		vl_len_[4];
	char		vl_dat[];
} varlena;
typedef varlena text;
typedef varlena bytea;

#define NAMEDATALEN 64
typedef struct NameData
{
	char		data[NAMEDATALEN];
} NameData;
typedef NameData *Name;
#define NameStr(name) ((name).data)

/* ---- shim 8: 4B-uncompressed little-endian varlena macros ---- */
#define VARHDRSZ ((int32_t) sizeof(uint32_t))

static uint32_t
pg_like_varsize_4b(const void *ptr)
{
	uint32_t	w;

	memcpy(&w, ptr, 4);
	return w >> 2;
}

static void
pg_like_set_varsize_4b(void *ptr, uint32_t len)
{
	uint32_t	w = len << 2;

	memcpy(ptr, &w, 4);
}

#define VARSIZE_ANY(PTR) pg_like_varsize_4b(PTR)
#define VARSIZE_ANY_EXHDR(PTR) ((int) (pg_like_varsize_4b(PTR) - VARHDRSZ))
#define VARDATA(PTR) (((char *) (PTR)) + VARHDRSZ)
#define VARDATA_ANY(PTR) VARDATA(PTR)
#define SET_VARSIZE(PTR, len) pg_like_set_varsize_4b((PTR), (len))

/* ---- shim 4: two-plane settable database encoding ---- */
#define PG_UTF8 6				/* mb/pg_wchar.h */
#define PG_LATIN1 8

static _Thread_local int pg_like_encoding = PG_UTF8;

void
pg_diff_like_set_encoding(int utf8)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	pg_like_encoding = utf8 ? PG_UTF8 : PG_LATIN1;
}

static int
GetDatabaseEncoding(void)
{
	return pg_like_encoding;
}

static int
pg_database_encoding_max_length(void)
{
	return pg_like_encoding == PG_UTF8 ? 4 : 1;
}

/* src/common/wchar.c pg_utf_mblen — VERBATIM */
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

/* shim 4: the pg_wchar_table[GetDatabaseEncoding()].mblen lookup resolved to
 * this two-plane dispatch (UTF8 row = pg_utf_mblen; LATIN1 row =
 * pg_latin1_mblen, which returns 1 unconditionally). */
static int
pg_like_table_mblen(const unsigned char *mbstr)
{
	return pg_like_encoding == PG_UTF8 ? pg_utf_mblen(mbstr) : 1;
}

/* shim 4: report_invalid_encoding_db -> errcode class 4 (22021) + longjmp
 * (mbutils.c raises ERRCODE_CHARACTER_NOT_IN_REPERTOIRE "invalid byte
 * sequence for encoding"; message text out of comparator scope). */
static void
report_invalid_encoding_db(const char *mbstr, int mblen, int len)
{
	(void) mbstr;
	(void) mblen;
	(void) len;
	pg_diff_errcode = PG_DIFF_LIKE_ERR_INVALID_BYTE_SEQ;
	pg_like_ereport_longjmp();
}

/* src/backend/utils/mb/mbutils.c pg_mblen_with_len — VERBATIM body, mblen
 * table lookup per shim 4, VALGRIND client checks dropped (instrumentation).
 */
static int
pg_mblen_with_len(const char *mbstr, int limit)
{
	int			length = pg_like_table_mblen((const unsigned char *) mbstr);

	Assert(limit >= 1);

	if (unlikely(length > limit))
		report_invalid_encoding_db(mbstr, length, limit);

	return length;
}

/* ---- shim 5: static C-locale pg_locale ---- */
#define COLLPROVIDER_LIBC 'c'
#define COLLPROVIDER_ICU 'i'

typedef struct pg_locale_struct
{
	char		provider;
	bool		deterministic;
	bool		collate_is_c;
	bool		ctype_is_c;
	bool		is_default;
	struct
	{
		int			lt;			/* stand-in for locale_t; dead under shim 5 */
	}			info;
} pg_locale_struct;
typedef pg_locale_struct *pg_locale_t;

static pg_locale_struct pg_like_c_locale = {
	.provider = COLLPROVIDER_LIBC,
	.deterministic = true,
	.collate_is_c = true,
	.ctype_is_c = true,
	.is_default = false,
};

static pg_locale_t
pg_newlocale_from_collation(Oid collid)
{
	/* Only reached after the OidIsValid gates; the drivers pin 950. */
	assert(collid == C_COLLATION_OID);
	return &pg_like_c_locale;
}

/* shim 5 abort-loud dead arms: unreachable while ctype_is_c/deterministic
 * are pinned true; abort() rather than a stub so drift is impossible. */
static char
pg_tolower(unsigned char ch)
{
	(void) ch;
	assert(!"pg_tolower reached under the ctype_is_c C-locale pin");
	abort();
}

#define tolower_l(c, lt) (abort(), (c))

static int
pg_strncoll(const char *arg1, size_t len1, const char *arg2, size_t len2,
			pg_locale_t locale)
{
	(void) arg1;
	(void) len1;
	(void) arg2;
	(void) len2;
	(void) locale;
	assert(!"pg_strncoll reached under the deterministic C-locale pin");
	abort();
}

/* src/port/pgstrcasecmp.c pg_ascii_tolower — VERBATIM */
static unsigned char
pg_ascii_tolower(unsigned char ch)
{
	if (ch >= 'A' && ch <= 'Z')
		ch += 'a' - 'A';
	return ch;
}

/* src/backend/utils/mmgr/mcxt.c pnstrdup — VERBATIM (palloc = arena) */
static char *
pnstrdup(const char *in, Size len)
{
	char	   *out;

	len = strnlen(in, len);

	out = palloc(len + 1);
	memcpy(out, in, len);
	out[len] = '\0';

	return out;
}

/* src/backend/utils/adt/varlena.c cstring_to_text_with_len /
 * cstring_to_text — VERBATIM (palloc = arena) */
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

/* src/backend/utils/adt/formatting.c asc_tolower — VERBATIM (the
 * str_tolower ctype_is_c arm; shim 6) */
static char *
asc_tolower(const char *buff, size_t nbytes)
{
	char	   *result;
	char	   *p;

	if (!buff)
		return NULL;

	result = pnstrdup(buff, nbytes);

	for (p = result; *p; p++)
		*p = pg_ascii_tolower((unsigned char) *p);

	return result;
}

/* ---- shim 6: DirectFunctionCall1Coll(lower)/DirectFunctionCall1(name_text)
 * call-site routing. Both like.c sites pass through these macros; the
 * bodies are the callee fmgr bodies unwrapped (formatting.c lower ->
 * str_tolower ctype_is_c -> asc_tolower; name.c name_text). ---- */

static text *
pg_like_lower_call(text *in_string)
{
	/* formatting.c lower(): str_tolower(VARDATA_ANY, VARSIZE_ANY_EXHDR,
	 * collation) then cstring_to_text + pfree; str_tolower's ctype_is_c arm
	 * is asc_tolower (verbatim above). */
	char	   *out_string;
	text	   *result;

	out_string = asc_tolower(VARDATA_ANY(in_string), VARSIZE_ANY_EXHDR(in_string));
	result = cstring_to_text(out_string);
	pfree(out_string);
	return result;
}

static text *
pg_like_name_text_call(Name s)
{
	/* name.c name_text(): cstring_to_text(NameStr(*s)). */
	return cstring_to_text(NameStr(*s));
}

#define PointerGetDatum(X) ((void *) (X))
#define NameGetDatum(X) ((void *) (X))
#define DatumGetTextPP(X) ((text *) (X))
#define DirectFunctionCall1Coll(func, collation, arg) \
	((void *) pg_like_lower_call((text *) (arg)))
#define DirectFunctionCall1(func, arg) \
	((void *) pg_like_name_text_call((Name) (arg)))

/* =================== SECTION 1: like.c core (VERBATIM) =================== */

#define LIKE_TRUE						1
#define LIKE_FALSE						0
#define LIKE_ABORT						(-1)


static int	SB_MatchText(const char *t, int tlen, const char *p, int plen,
						 pg_locale_t locale);
static text *SB_do_like_escape(text *pat, text *esc);

static int	MB_MatchText(const char *t, int tlen, const char *p, int plen,
						 pg_locale_t locale);
static text *MB_do_like_escape(text *pat, text *esc);

static int	UTF8_MatchText(const char *t, int tlen, const char *p, int plen,
						   pg_locale_t locale);

static int	SB_IMatchText(const char *t, int tlen, const char *p, int plen,
						  pg_locale_t locale);

static int	GenericMatchText(const char *s, int slen, const char *p, int plen, Oid collation);
static int	Generic_Text_IC_like(text *str, text *pat, Oid collation);

/*--------------------
 * Support routine for MatchText. Compares given multibyte streams
 * as wide characters. If they match, returns 1 otherwise returns 0.
 *--------------------
 */
static inline int
wchareq(const char *p1, int p1len, const char *p2, int p2len)
{
	int			p1clen;

	/* Optimization:  quickly compare the first byte. */
	if (*p1 != *p2)
		return 0;

	p1clen = pg_mblen_with_len(p1, p1len);
	if (pg_mblen_with_len(p2, p2len) != p1clen)
		return 0;

	/* They are the same length */
	while (p1clen--)
	{
		if (*p1++ != *p2++)
			return 0;
	}
	return 1;
}

/*
 * Formerly we had a routine iwchareq() here that tried to do case-insensitive
 * comparison of multibyte characters.  It did not work at all, however,
 * because it relied on tolower() which has a single-byte API ... and
 * towlower() wouldn't be much better since we have no suitably cheap way
 * of getting a single character transformed to the system's wchar_t format.
 * So now, we just downcase the strings using lower() and apply regular LIKE
 * comparison.  This should be revisited when we install better locale support.
 */

/*
 * We do handle case-insensitive matching for single-byte encodings using
 * fold-on-the-fly processing, however.
 */
static char
SB_lower_char(unsigned char c, pg_locale_t locale)
{
	if (locale->ctype_is_c)
		return pg_ascii_tolower(c);
	else if (locale->is_default)
		return pg_tolower(c);
	else
		return tolower_l(c, locale->info.lt);
}


#define NextByte(p, plen)	((p)++, (plen)--)


/* Set up to compile like_match.c for multibyte characters */
#define CHAREQ(p1, p1len, p2, p2len) wchareq((p1), (p1len), (p2), (p2len))
#define NextChar(p, plen) \
	do { int __l = pg_mblen_with_len((p), (plen)); (p) +=__l; (plen) -=__l; } while (0)
#define CopyAdvChar(dst, src, srclen) \
	do { int __l = pg_mblen_with_len((src), (srclen)); \
		 (srclen) -= __l; \
		 while (__l-- > 0) \
			 *(dst)++ = *(src)++; \
	   } while (0)

#define MatchText	MB_MatchText
#define do_like_escape	MB_do_like_escape

/* ==== like_match.c pasted VERBATIM (one stamping), exactly as like.c
 * #includes it ==== */
/*-------------------------------------------------------------------------
 *
 * like_match.c
 *	  LIKE pattern matching internal code.
 *
 * This file is included by like.c four times, to provide matching code for
 * (1) single-byte encodings, (2) UTF8, (3) other multi-byte encodings,
 * and (4) case insensitive matches in single-byte encodings.
 * (UTF8 is a special case because we can use a much more efficient version
 * of NextChar than can be used for general multi-byte encodings.)
 *
 * Before the inclusion, we need to define the following macros:
 *
 * NextChar
 * MatchText - to name of function wanted
 * do_like_escape - name of function if wanted - needs CHAREQ and CopyAdvChar
 * MATCH_LOWER - define for case (4) to specify case folding for 1-byte chars
 *
 * Copyright (c) 1996-2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	src/backend/utils/adt/like_match.c
 *
 *-------------------------------------------------------------------------
 */

/*
 *	Originally written by Rich $alz, mirror!rs, Wed Nov 26 19:03:17 EST 1986.
 *	Rich $alz is now <rsalz@bbn.com>.
 *	Special thanks to Lars Mathiesen <thorinn@diku.dk> for the
 *	LIKE_ABORT code.
 *
 *	This code was shamelessly stolen from the "pql" code by myself and
 *	slightly modified :)
 *
 *	All references to the word "star" were replaced by "percent"
 *	All references to the word "wild" were replaced by "like"
 *
 *	All the nice shell RE matching stuff was replaced by just "_" and "%"
 *
 *	As I don't have a copy of the SQL standard handy I wasn't sure whether
 *	to leave in the '\' escape character handling.
 *
 *	Keith Parks. <keith@mtcc.demon.co.uk>
 *
 *	SQL lets you specify the escape character by saying
 *	LIKE <pattern> ESCAPE <escape character>. We are a small operation
 *	so we force you to use '\'. - ay 7/95
 *
 *	Now we have the like_escape() function that converts patterns with
 *	any specified escape character (or none at all) to the internal
 *	default escape character, which is still '\'. - tgl 9/2000
 *
 * The code is rewritten to avoid requiring null-terminated strings,
 * which in turn allows us to leave out some memcpy() operations.
 * This code should be faster and take less memory, but no promises...
 * - thomas 2000-08-06
 */


/*--------------------
 *	Match text and pattern, return LIKE_TRUE, LIKE_FALSE, or LIKE_ABORT.
 *
 *	LIKE_TRUE: they match
 *	LIKE_FALSE: they don't match
 *	LIKE_ABORT: not only don't they match, but the text is too short.
 *
 * If LIKE_ABORT is returned, then no suffix of the text can match the
 * pattern either, so an upper-level % scan can stop scanning now.
 *--------------------
 */

#ifdef MATCH_LOWER
#define GETCHAR(t, locale) MATCH_LOWER(t, locale)
#else
#define GETCHAR(t, locale) (t)
#endif

static int
MatchText(const char *t, int tlen, const char *p, int plen, pg_locale_t locale)
{
	/* Fast path for match-everything pattern */
	if (plen == 1 && *p == '%')
		return LIKE_TRUE;

	/* Since this function recurses, it could be driven to stack overflow */
	check_stack_depth();

	/*
	 * In this loop, we advance by char when matching wildcards (and thus on
	 * recursive entry to this function we are properly char-synced). On other
	 * occasions it is safe to advance by byte, as the text and pattern will
	 * be in lockstep. This allows us to perform all comparisons between the
	 * text and pattern on a byte by byte basis, even for multi-byte
	 * encodings.
	 */
	while (tlen > 0 && plen > 0)
	{
		if (*p == '\\')
		{
			/* Next pattern byte must match literally, whatever it is */
			NextByte(p, plen);
			/* ... and there had better be one, per SQL standard */
			if (plen <= 0)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_ESCAPE_SEQUENCE),
						 errmsg("LIKE pattern must not end with escape character")));
			if (GETCHAR(*p, locale) != GETCHAR(*t, locale))
				return LIKE_FALSE;
		}
		else if (*p == '%')
		{
			char		firstpat;

			/*
			 * % processing is essentially a search for a text position at
			 * which the remainder of the text matches the remainder of the
			 * pattern, using a recursive call to check each potential match.
			 *
			 * If there are wildcards immediately following the %, we can skip
			 * over them first, using the idea that any sequence of N _'s and
			 * one or more %'s is equivalent to N _'s and one % (ie, it will
			 * match any sequence of at least N text characters).  In this way
			 * we will always run the recursive search loop using a pattern
			 * fragment that begins with a literal character-to-match, thereby
			 * not recursing more than we have to.
			 */
			NextByte(p, plen);

			while (plen > 0)
			{
				if (*p == '%')
					NextByte(p, plen);
				else if (*p == '_')
				{
					/* If not enough text left to match the pattern, ABORT */
					if (tlen <= 0)
						return LIKE_ABORT;
					NextChar(t, tlen);
					NextByte(p, plen);
				}
				else
					break;		/* Reached a non-wildcard pattern char */
			}

			/*
			 * If we're at end of pattern, match: we have a trailing % which
			 * matches any remaining text string.
			 */
			if (plen <= 0)
				return LIKE_TRUE;

			/*
			 * Otherwise, scan for a text position at which we can match the
			 * rest of the pattern.  The first remaining pattern char is known
			 * to be a regular or escaped literal character, so we can compare
			 * the first pattern byte to each text byte to avoid recursing
			 * more than we have to.  This fact also guarantees that we don't
			 * have to consider a match to the zero-length substring at the
			 * end of the text.  With a nondeterministic collation, we can't
			 * rely on the first bytes being equal, so we have to recurse in
			 * any case.
			 */
			if (*p == '\\')
			{
				if (plen < 2)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_ESCAPE_SEQUENCE),
							 errmsg("LIKE pattern must not end with escape character")));
				firstpat = GETCHAR(p[1], locale);
			}
			else
				firstpat = GETCHAR(*p, locale);

			while (tlen > 0)
			{
				if (GETCHAR(*t, locale) == firstpat || (locale && !locale->deterministic))
				{
					int			matched = MatchText(t, tlen, p, plen, locale);

					if (matched != LIKE_FALSE)
						return matched; /* TRUE or ABORT */
				}

				NextChar(t, tlen);
			}

			/*
			 * End of text with no match, so no point in trying later places
			 * to start matching this pattern.
			 */
			return LIKE_ABORT;
		}
		else if (*p == '_')
		{
			/* _ matches any single character, and we know there is one */
			NextChar(t, tlen);
			NextByte(p, plen);
			continue;
		}
		else if (locale && !locale->deterministic)
		{
			/*
			 * For nondeterministic locales, we find the next substring of the
			 * pattern that does not contain wildcards and try to find a
			 * matching substring in the text.  Crucially, we cannot do this
			 * character by character, as in the normal case, but must do it
			 * substring by substring, partitioned by the wildcard characters.
			 * (This is per SQL standard.)
			 */
			const char *p1;
			size_t		p1len;
			const char *t1;
			size_t		t1len;
			bool		found_escape;
			const char *subpat;
			size_t		subpatlen;
			char	   *buf = NULL;

			/*
			 * Determine next substring of pattern without wildcards.  p is
			 * the start of the subpattern, p1 is one past the last byte. Also
			 * track if we found an escape character.
			 */
			p1 = p;
			p1len = plen;
			found_escape = false;
			while (p1len > 0)
			{
				if (*p1 == '\\')
				{
					found_escape = true;
					NextByte(p1, p1len);
					if (p1len == 0)
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_ESCAPE_SEQUENCE),
								 errmsg("LIKE pattern must not end with escape character")));
				}
				else if (*p1 == '_' || *p1 == '%')
					break;
				NextByte(p1, p1len);
			}

			/*
			 * If we found an escape character, then make an unescaped copy of
			 * the subpattern.
			 */
			if (found_escape)
			{
				char	   *b;

				b = buf = palloc(p1 - p);
				for (const char *c = p; c < p1; c++)
				{
					if (*c == '\\')
						;
					else
						*(b++) = *c;
				}

				subpat = buf;
				subpatlen = b - buf;
			}
			else
			{
				subpat = p;
				subpatlen = p1 - p;
			}

			/*
			 * Shortcut: If this is the end of the pattern, then the rest of
			 * the text has to match the rest of the pattern.
			 */
			if (p1len == 0)
			{
				int			cmp;

				cmp = pg_strncoll(subpat, subpatlen, t, tlen, locale);

				if (buf)
					pfree(buf);
				if (cmp == 0)
					return LIKE_TRUE;
				else
					return LIKE_FALSE;
			}

			/*
			 * Now build a substring of the text and try to match it against
			 * the subpattern.  t is the start of the text, t1 is one past the
			 * last byte.  We start with a zero-length string.
			 */
			t1 = t;
			t1len = tlen;
			for (;;)
			{
				int			cmp;

				CHECK_FOR_INTERRUPTS();

				cmp = pg_strncoll(subpat, subpatlen, t, (t1 - t), locale);

				/*
				 * If we found a match, we have to test if the rest of pattern
				 * can match against the rest of the string.  Otherwise we
				 * have to continue here try matching with a longer substring.
				 * (This is similar to the recursion for the '%' wildcard
				 * above.)
				 *
				 * Note that we can't just wind forward p and t and continue
				 * with the main loop.  This would fail for example with
				 *
				 * U&'\0061\0308bc' LIKE U&'\00E4_c' COLLATE ignore_accents
				 *
				 * You'd find that t=\0061 matches p=\00E4, but then the rest
				 * won't match; but t=\0061\0308 also matches p=\00E4, and
				 * then the rest will match.
				 */
				if (cmp == 0)
				{
					int			matched = MatchText(t1, t1len, p1, p1len, locale);

					if (matched == LIKE_TRUE)
					{
						if (buf)
							pfree(buf);
						return matched;
					}
				}

				/*
				 * Didn't match.  If we used up the whole text, then the match
				 * fails.  Otherwise, try again with a longer substring.
				 */
				if (t1len == 0)
				{
					if (buf)
						pfree(buf);
					return LIKE_FALSE;
				}
				else
					NextChar(t1, t1len);
			}
		}
		else if (GETCHAR(*p, locale) != GETCHAR(*t, locale))
		{
			/* non-wildcard pattern char fails to match text char */
			return LIKE_FALSE;
		}

		/*
		 * Pattern and text match, so advance.
		 *
		 * It is safe to use NextByte instead of NextChar here, even for
		 * multi-byte character sets, because we are not following immediately
		 * after a wildcard character. If we are in the middle of a multibyte
		 * character, we must already have matched at least one byte of the
		 * character from both text and pattern; so we cannot get out-of-sync
		 * on character boundaries.  And we know that no backend-legal
		 * encoding allows ASCII characters such as '%' to appear as non-first
		 * bytes of characters, so we won't mistakenly detect a new wildcard.
		 */
		NextByte(t, tlen);
		NextByte(p, plen);
	}

	if (tlen > 0)
		return LIKE_FALSE;		/* end of pattern, but not of text */

	/*
	 * End of text, but perhaps not of pattern.  Match iff the remaining
	 * pattern can match a zero-length string, ie, it's zero or more %'s.
	 */
	while (plen > 0 && *p == '%')
		NextByte(p, plen);
	if (plen <= 0)
		return LIKE_TRUE;

	/*
	 * End of text with no match, so no point in trying later places to start
	 * matching this pattern.
	 */
	return LIKE_ABORT;
}								/* MatchText() */

/*
 * like_escape() --- given a pattern and an ESCAPE string,
 * convert the pattern to use Postgres' standard backslash escape convention.
 */
#ifdef do_like_escape

static text *
do_like_escape(text *pat, text *esc)
{
	text	   *result;
	char	   *p,
			   *e,
			   *r;
	int			plen,
				elen;
	bool		afterescape;

	p = VARDATA_ANY(pat);
	plen = VARSIZE_ANY_EXHDR(pat);
	e = VARDATA_ANY(esc);
	elen = VARSIZE_ANY_EXHDR(esc);

	/*
	 * Worst-case pattern growth is 2x --- unlikely, but it's hardly worth
	 * trying to calculate the size more accurately than that.
	 */
	result = (text *) palloc(plen * 2 + VARHDRSZ);
	r = VARDATA(result);

	if (elen == 0)
	{
		/*
		 * No escape character is wanted.  Double any backslashes in the
		 * pattern to make them act like ordinary characters.
		 */
		while (plen > 0)
		{
			if (*p == '\\')
				*r++ = '\\';
			CopyAdvChar(r, p, plen);
		}
	}
	else
	{
		/*
		 * The specified escape must be only a single character.
		 */
		NextChar(e, elen);
		if (elen != 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_ESCAPE_SEQUENCE),
					 errmsg("invalid escape string"),
					 errhint("Escape string must be empty or one character.")));

		e = VARDATA_ANY(esc);
		elen = VARSIZE_ANY_EXHDR(esc);

		/*
		 * If specified escape is '\', just copy the pattern as-is.
		 */
		if (*e == '\\')
		{
			memcpy(result, pat, VARSIZE_ANY(pat));
			return result;
		}

		/*
		 * Otherwise, convert occurrences of the specified escape character to
		 * '\', and double occurrences of '\' --- unless they immediately
		 * follow an escape character!
		 */
		afterescape = false;
		while (plen > 0)
		{
			if (CHAREQ(p, plen, e, elen) && !afterescape)
			{
				*r++ = '\\';
				NextChar(p, plen);
				afterescape = true;
			}
			else if (*p == '\\')
			{
				*r++ = '\\';
				if (!afterescape)
					*r++ = '\\';
				NextChar(p, plen);
				afterescape = false;
			}
			else
			{
				CopyAdvChar(r, p, plen);
				afterescape = false;
			}
		}
	}

	SET_VARSIZE(result, r - ((char *) result));

	return result;
}
#endif							/* do_like_escape */

#ifdef CHAREQ
#undef CHAREQ
#endif

#undef NextChar
#undef CopyAdvChar
#undef MatchText

#ifdef do_like_escape
#undef do_like_escape
#endif

#undef GETCHAR

#ifdef MATCH_LOWER
#undef MATCH_LOWER

#endif



/* Set up to compile like_match.c for single-byte characters */
#define CHAREQ(p1, p1len, p2, p2len) (*(p1) == *(p2))
#define NextChar(p, plen) NextByte((p), (plen))
#define CopyAdvChar(dst, src, srclen) (*(dst)++ = *(src)++, (srclen)--)

#define MatchText	SB_MatchText
#define do_like_escape	SB_do_like_escape

/* ==== like_match.c pasted VERBATIM (one stamping), exactly as like.c
 * #includes it ==== */
/*-------------------------------------------------------------------------
 *
 * like_match.c
 *	  LIKE pattern matching internal code.
 *
 * This file is included by like.c four times, to provide matching code for
 * (1) single-byte encodings, (2) UTF8, (3) other multi-byte encodings,
 * and (4) case insensitive matches in single-byte encodings.
 * (UTF8 is a special case because we can use a much more efficient version
 * of NextChar than can be used for general multi-byte encodings.)
 *
 * Before the inclusion, we need to define the following macros:
 *
 * NextChar
 * MatchText - to name of function wanted
 * do_like_escape - name of function if wanted - needs CHAREQ and CopyAdvChar
 * MATCH_LOWER - define for case (4) to specify case folding for 1-byte chars
 *
 * Copyright (c) 1996-2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	src/backend/utils/adt/like_match.c
 *
 *-------------------------------------------------------------------------
 */

/*
 *	Originally written by Rich $alz, mirror!rs, Wed Nov 26 19:03:17 EST 1986.
 *	Rich $alz is now <rsalz@bbn.com>.
 *	Special thanks to Lars Mathiesen <thorinn@diku.dk> for the
 *	LIKE_ABORT code.
 *
 *	This code was shamelessly stolen from the "pql" code by myself and
 *	slightly modified :)
 *
 *	All references to the word "star" were replaced by "percent"
 *	All references to the word "wild" were replaced by "like"
 *
 *	All the nice shell RE matching stuff was replaced by just "_" and "%"
 *
 *	As I don't have a copy of the SQL standard handy I wasn't sure whether
 *	to leave in the '\' escape character handling.
 *
 *	Keith Parks. <keith@mtcc.demon.co.uk>
 *
 *	SQL lets you specify the escape character by saying
 *	LIKE <pattern> ESCAPE <escape character>. We are a small operation
 *	so we force you to use '\'. - ay 7/95
 *
 *	Now we have the like_escape() function that converts patterns with
 *	any specified escape character (or none at all) to the internal
 *	default escape character, which is still '\'. - tgl 9/2000
 *
 * The code is rewritten to avoid requiring null-terminated strings,
 * which in turn allows us to leave out some memcpy() operations.
 * This code should be faster and take less memory, but no promises...
 * - thomas 2000-08-06
 */


/*--------------------
 *	Match text and pattern, return LIKE_TRUE, LIKE_FALSE, or LIKE_ABORT.
 *
 *	LIKE_TRUE: they match
 *	LIKE_FALSE: they don't match
 *	LIKE_ABORT: not only don't they match, but the text is too short.
 *
 * If LIKE_ABORT is returned, then no suffix of the text can match the
 * pattern either, so an upper-level % scan can stop scanning now.
 *--------------------
 */

#ifdef MATCH_LOWER
#define GETCHAR(t, locale) MATCH_LOWER(t, locale)
#else
#define GETCHAR(t, locale) (t)
#endif

static int
MatchText(const char *t, int tlen, const char *p, int plen, pg_locale_t locale)
{
	/* Fast path for match-everything pattern */
	if (plen == 1 && *p == '%')
		return LIKE_TRUE;

	/* Since this function recurses, it could be driven to stack overflow */
	check_stack_depth();

	/*
	 * In this loop, we advance by char when matching wildcards (and thus on
	 * recursive entry to this function we are properly char-synced). On other
	 * occasions it is safe to advance by byte, as the text and pattern will
	 * be in lockstep. This allows us to perform all comparisons between the
	 * text and pattern on a byte by byte basis, even for multi-byte
	 * encodings.
	 */
	while (tlen > 0 && plen > 0)
	{
		if (*p == '\\')
		{
			/* Next pattern byte must match literally, whatever it is */
			NextByte(p, plen);
			/* ... and there had better be one, per SQL standard */
			if (plen <= 0)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_ESCAPE_SEQUENCE),
						 errmsg("LIKE pattern must not end with escape character")));
			if (GETCHAR(*p, locale) != GETCHAR(*t, locale))
				return LIKE_FALSE;
		}
		else if (*p == '%')
		{
			char		firstpat;

			/*
			 * % processing is essentially a search for a text position at
			 * which the remainder of the text matches the remainder of the
			 * pattern, using a recursive call to check each potential match.
			 *
			 * If there are wildcards immediately following the %, we can skip
			 * over them first, using the idea that any sequence of N _'s and
			 * one or more %'s is equivalent to N _'s and one % (ie, it will
			 * match any sequence of at least N text characters).  In this way
			 * we will always run the recursive search loop using a pattern
			 * fragment that begins with a literal character-to-match, thereby
			 * not recursing more than we have to.
			 */
			NextByte(p, plen);

			while (plen > 0)
			{
				if (*p == '%')
					NextByte(p, plen);
				else if (*p == '_')
				{
					/* If not enough text left to match the pattern, ABORT */
					if (tlen <= 0)
						return LIKE_ABORT;
					NextChar(t, tlen);
					NextByte(p, plen);
				}
				else
					break;		/* Reached a non-wildcard pattern char */
			}

			/*
			 * If we're at end of pattern, match: we have a trailing % which
			 * matches any remaining text string.
			 */
			if (plen <= 0)
				return LIKE_TRUE;

			/*
			 * Otherwise, scan for a text position at which we can match the
			 * rest of the pattern.  The first remaining pattern char is known
			 * to be a regular or escaped literal character, so we can compare
			 * the first pattern byte to each text byte to avoid recursing
			 * more than we have to.  This fact also guarantees that we don't
			 * have to consider a match to the zero-length substring at the
			 * end of the text.  With a nondeterministic collation, we can't
			 * rely on the first bytes being equal, so we have to recurse in
			 * any case.
			 */
			if (*p == '\\')
			{
				if (plen < 2)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_ESCAPE_SEQUENCE),
							 errmsg("LIKE pattern must not end with escape character")));
				firstpat = GETCHAR(p[1], locale);
			}
			else
				firstpat = GETCHAR(*p, locale);

			while (tlen > 0)
			{
				if (GETCHAR(*t, locale) == firstpat || (locale && !locale->deterministic))
				{
					int			matched = MatchText(t, tlen, p, plen, locale);

					if (matched != LIKE_FALSE)
						return matched; /* TRUE or ABORT */
				}

				NextChar(t, tlen);
			}

			/*
			 * End of text with no match, so no point in trying later places
			 * to start matching this pattern.
			 */
			return LIKE_ABORT;
		}
		else if (*p == '_')
		{
			/* _ matches any single character, and we know there is one */
			NextChar(t, tlen);
			NextByte(p, plen);
			continue;
		}
		else if (locale && !locale->deterministic)
		{
			/*
			 * For nondeterministic locales, we find the next substring of the
			 * pattern that does not contain wildcards and try to find a
			 * matching substring in the text.  Crucially, we cannot do this
			 * character by character, as in the normal case, but must do it
			 * substring by substring, partitioned by the wildcard characters.
			 * (This is per SQL standard.)
			 */
			const char *p1;
			size_t		p1len;
			const char *t1;
			size_t		t1len;
			bool		found_escape;
			const char *subpat;
			size_t		subpatlen;
			char	   *buf = NULL;

			/*
			 * Determine next substring of pattern without wildcards.  p is
			 * the start of the subpattern, p1 is one past the last byte. Also
			 * track if we found an escape character.
			 */
			p1 = p;
			p1len = plen;
			found_escape = false;
			while (p1len > 0)
			{
				if (*p1 == '\\')
				{
					found_escape = true;
					NextByte(p1, p1len);
					if (p1len == 0)
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_ESCAPE_SEQUENCE),
								 errmsg("LIKE pattern must not end with escape character")));
				}
				else if (*p1 == '_' || *p1 == '%')
					break;
				NextByte(p1, p1len);
			}

			/*
			 * If we found an escape character, then make an unescaped copy of
			 * the subpattern.
			 */
			if (found_escape)
			{
				char	   *b;

				b = buf = palloc(p1 - p);
				for (const char *c = p; c < p1; c++)
				{
					if (*c == '\\')
						;
					else
						*(b++) = *c;
				}

				subpat = buf;
				subpatlen = b - buf;
			}
			else
			{
				subpat = p;
				subpatlen = p1 - p;
			}

			/*
			 * Shortcut: If this is the end of the pattern, then the rest of
			 * the text has to match the rest of the pattern.
			 */
			if (p1len == 0)
			{
				int			cmp;

				cmp = pg_strncoll(subpat, subpatlen, t, tlen, locale);

				if (buf)
					pfree(buf);
				if (cmp == 0)
					return LIKE_TRUE;
				else
					return LIKE_FALSE;
			}

			/*
			 * Now build a substring of the text and try to match it against
			 * the subpattern.  t is the start of the text, t1 is one past the
			 * last byte.  We start with a zero-length string.
			 */
			t1 = t;
			t1len = tlen;
			for (;;)
			{
				int			cmp;

				CHECK_FOR_INTERRUPTS();

				cmp = pg_strncoll(subpat, subpatlen, t, (t1 - t), locale);

				/*
				 * If we found a match, we have to test if the rest of pattern
				 * can match against the rest of the string.  Otherwise we
				 * have to continue here try matching with a longer substring.
				 * (This is similar to the recursion for the '%' wildcard
				 * above.)
				 *
				 * Note that we can't just wind forward p and t and continue
				 * with the main loop.  This would fail for example with
				 *
				 * U&'\0061\0308bc' LIKE U&'\00E4_c' COLLATE ignore_accents
				 *
				 * You'd find that t=\0061 matches p=\00E4, but then the rest
				 * won't match; but t=\0061\0308 also matches p=\00E4, and
				 * then the rest will match.
				 */
				if (cmp == 0)
				{
					int			matched = MatchText(t1, t1len, p1, p1len, locale);

					if (matched == LIKE_TRUE)
					{
						if (buf)
							pfree(buf);
						return matched;
					}
				}

				/*
				 * Didn't match.  If we used up the whole text, then the match
				 * fails.  Otherwise, try again with a longer substring.
				 */
				if (t1len == 0)
				{
					if (buf)
						pfree(buf);
					return LIKE_FALSE;
				}
				else
					NextChar(t1, t1len);
			}
		}
		else if (GETCHAR(*p, locale) != GETCHAR(*t, locale))
		{
			/* non-wildcard pattern char fails to match text char */
			return LIKE_FALSE;
		}

		/*
		 * Pattern and text match, so advance.
		 *
		 * It is safe to use NextByte instead of NextChar here, even for
		 * multi-byte character sets, because we are not following immediately
		 * after a wildcard character. If we are in the middle of a multibyte
		 * character, we must already have matched at least one byte of the
		 * character from both text and pattern; so we cannot get out-of-sync
		 * on character boundaries.  And we know that no backend-legal
		 * encoding allows ASCII characters such as '%' to appear as non-first
		 * bytes of characters, so we won't mistakenly detect a new wildcard.
		 */
		NextByte(t, tlen);
		NextByte(p, plen);
	}

	if (tlen > 0)
		return LIKE_FALSE;		/* end of pattern, but not of text */

	/*
	 * End of text, but perhaps not of pattern.  Match iff the remaining
	 * pattern can match a zero-length string, ie, it's zero or more %'s.
	 */
	while (plen > 0 && *p == '%')
		NextByte(p, plen);
	if (plen <= 0)
		return LIKE_TRUE;

	/*
	 * End of text with no match, so no point in trying later places to start
	 * matching this pattern.
	 */
	return LIKE_ABORT;
}								/* MatchText() */

/*
 * like_escape() --- given a pattern and an ESCAPE string,
 * convert the pattern to use Postgres' standard backslash escape convention.
 */
#ifdef do_like_escape

static text *
do_like_escape(text *pat, text *esc)
{
	text	   *result;
	char	   *p,
			   *e,
			   *r;
	int			plen,
				elen;
	bool		afterescape;

	p = VARDATA_ANY(pat);
	plen = VARSIZE_ANY_EXHDR(pat);
	e = VARDATA_ANY(esc);
	elen = VARSIZE_ANY_EXHDR(esc);

	/*
	 * Worst-case pattern growth is 2x --- unlikely, but it's hardly worth
	 * trying to calculate the size more accurately than that.
	 */
	result = (text *) palloc(plen * 2 + VARHDRSZ);
	r = VARDATA(result);

	if (elen == 0)
	{
		/*
		 * No escape character is wanted.  Double any backslashes in the
		 * pattern to make them act like ordinary characters.
		 */
		while (plen > 0)
		{
			if (*p == '\\')
				*r++ = '\\';
			CopyAdvChar(r, p, plen);
		}
	}
	else
	{
		/*
		 * The specified escape must be only a single character.
		 */
		NextChar(e, elen);
		if (elen != 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_ESCAPE_SEQUENCE),
					 errmsg("invalid escape string"),
					 errhint("Escape string must be empty or one character.")));

		e = VARDATA_ANY(esc);
		elen = VARSIZE_ANY_EXHDR(esc);

		/*
		 * If specified escape is '\', just copy the pattern as-is.
		 */
		if (*e == '\\')
		{
			memcpy(result, pat, VARSIZE_ANY(pat));
			return result;
		}

		/*
		 * Otherwise, convert occurrences of the specified escape character to
		 * '\', and double occurrences of '\' --- unless they immediately
		 * follow an escape character!
		 */
		afterescape = false;
		while (plen > 0)
		{
			if (CHAREQ(p, plen, e, elen) && !afterescape)
			{
				*r++ = '\\';
				NextChar(p, plen);
				afterescape = true;
			}
			else if (*p == '\\')
			{
				*r++ = '\\';
				if (!afterescape)
					*r++ = '\\';
				NextChar(p, plen);
				afterescape = false;
			}
			else
			{
				CopyAdvChar(r, p, plen);
				afterescape = false;
			}
		}
	}

	SET_VARSIZE(result, r - ((char *) result));

	return result;
}
#endif							/* do_like_escape */

#ifdef CHAREQ
#undef CHAREQ
#endif

#undef NextChar
#undef CopyAdvChar
#undef MatchText

#ifdef do_like_escape
#undef do_like_escape
#endif

#undef GETCHAR

#ifdef MATCH_LOWER
#undef MATCH_LOWER

#endif



/* setup to compile like_match.c for single byte case insensitive matches */
#define MATCH_LOWER(t, locale) SB_lower_char((unsigned char) (t), locale)
#define NextChar(p, plen) NextByte((p), (plen))
#define MatchText SB_IMatchText

/* ==== like_match.c pasted VERBATIM (one stamping), exactly as like.c
 * #includes it ==== */
/*-------------------------------------------------------------------------
 *
 * like_match.c
 *	  LIKE pattern matching internal code.
 *
 * This file is included by like.c four times, to provide matching code for
 * (1) single-byte encodings, (2) UTF8, (3) other multi-byte encodings,
 * and (4) case insensitive matches in single-byte encodings.
 * (UTF8 is a special case because we can use a much more efficient version
 * of NextChar than can be used for general multi-byte encodings.)
 *
 * Before the inclusion, we need to define the following macros:
 *
 * NextChar
 * MatchText - to name of function wanted
 * do_like_escape - name of function if wanted - needs CHAREQ and CopyAdvChar
 * MATCH_LOWER - define for case (4) to specify case folding for 1-byte chars
 *
 * Copyright (c) 1996-2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	src/backend/utils/adt/like_match.c
 *
 *-------------------------------------------------------------------------
 */

/*
 *	Originally written by Rich $alz, mirror!rs, Wed Nov 26 19:03:17 EST 1986.
 *	Rich $alz is now <rsalz@bbn.com>.
 *	Special thanks to Lars Mathiesen <thorinn@diku.dk> for the
 *	LIKE_ABORT code.
 *
 *	This code was shamelessly stolen from the "pql" code by myself and
 *	slightly modified :)
 *
 *	All references to the word "star" were replaced by "percent"
 *	All references to the word "wild" were replaced by "like"
 *
 *	All the nice shell RE matching stuff was replaced by just "_" and "%"
 *
 *	As I don't have a copy of the SQL standard handy I wasn't sure whether
 *	to leave in the '\' escape character handling.
 *
 *	Keith Parks. <keith@mtcc.demon.co.uk>
 *
 *	SQL lets you specify the escape character by saying
 *	LIKE <pattern> ESCAPE <escape character>. We are a small operation
 *	so we force you to use '\'. - ay 7/95
 *
 *	Now we have the like_escape() function that converts patterns with
 *	any specified escape character (or none at all) to the internal
 *	default escape character, which is still '\'. - tgl 9/2000
 *
 * The code is rewritten to avoid requiring null-terminated strings,
 * which in turn allows us to leave out some memcpy() operations.
 * This code should be faster and take less memory, but no promises...
 * - thomas 2000-08-06
 */


/*--------------------
 *	Match text and pattern, return LIKE_TRUE, LIKE_FALSE, or LIKE_ABORT.
 *
 *	LIKE_TRUE: they match
 *	LIKE_FALSE: they don't match
 *	LIKE_ABORT: not only don't they match, but the text is too short.
 *
 * If LIKE_ABORT is returned, then no suffix of the text can match the
 * pattern either, so an upper-level % scan can stop scanning now.
 *--------------------
 */

#ifdef MATCH_LOWER
#define GETCHAR(t, locale) MATCH_LOWER(t, locale)
#else
#define GETCHAR(t, locale) (t)
#endif

static int
MatchText(const char *t, int tlen, const char *p, int plen, pg_locale_t locale)
{
	/* Fast path for match-everything pattern */
	if (plen == 1 && *p == '%')
		return LIKE_TRUE;

	/* Since this function recurses, it could be driven to stack overflow */
	check_stack_depth();

	/*
	 * In this loop, we advance by char when matching wildcards (and thus on
	 * recursive entry to this function we are properly char-synced). On other
	 * occasions it is safe to advance by byte, as the text and pattern will
	 * be in lockstep. This allows us to perform all comparisons between the
	 * text and pattern on a byte by byte basis, even for multi-byte
	 * encodings.
	 */
	while (tlen > 0 && plen > 0)
	{
		if (*p == '\\')
		{
			/* Next pattern byte must match literally, whatever it is */
			NextByte(p, plen);
			/* ... and there had better be one, per SQL standard */
			if (plen <= 0)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_ESCAPE_SEQUENCE),
						 errmsg("LIKE pattern must not end with escape character")));
			if (GETCHAR(*p, locale) != GETCHAR(*t, locale))
				return LIKE_FALSE;
		}
		else if (*p == '%')
		{
			char		firstpat;

			/*
			 * % processing is essentially a search for a text position at
			 * which the remainder of the text matches the remainder of the
			 * pattern, using a recursive call to check each potential match.
			 *
			 * If there are wildcards immediately following the %, we can skip
			 * over them first, using the idea that any sequence of N _'s and
			 * one or more %'s is equivalent to N _'s and one % (ie, it will
			 * match any sequence of at least N text characters).  In this way
			 * we will always run the recursive search loop using a pattern
			 * fragment that begins with a literal character-to-match, thereby
			 * not recursing more than we have to.
			 */
			NextByte(p, plen);

			while (plen > 0)
			{
				if (*p == '%')
					NextByte(p, plen);
				else if (*p == '_')
				{
					/* If not enough text left to match the pattern, ABORT */
					if (tlen <= 0)
						return LIKE_ABORT;
					NextChar(t, tlen);
					NextByte(p, plen);
				}
				else
					break;		/* Reached a non-wildcard pattern char */
			}

			/*
			 * If we're at end of pattern, match: we have a trailing % which
			 * matches any remaining text string.
			 */
			if (plen <= 0)
				return LIKE_TRUE;

			/*
			 * Otherwise, scan for a text position at which we can match the
			 * rest of the pattern.  The first remaining pattern char is known
			 * to be a regular or escaped literal character, so we can compare
			 * the first pattern byte to each text byte to avoid recursing
			 * more than we have to.  This fact also guarantees that we don't
			 * have to consider a match to the zero-length substring at the
			 * end of the text.  With a nondeterministic collation, we can't
			 * rely on the first bytes being equal, so we have to recurse in
			 * any case.
			 */
			if (*p == '\\')
			{
				if (plen < 2)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_ESCAPE_SEQUENCE),
							 errmsg("LIKE pattern must not end with escape character")));
				firstpat = GETCHAR(p[1], locale);
			}
			else
				firstpat = GETCHAR(*p, locale);

			while (tlen > 0)
			{
				if (GETCHAR(*t, locale) == firstpat || (locale && !locale->deterministic))
				{
					int			matched = MatchText(t, tlen, p, plen, locale);

					if (matched != LIKE_FALSE)
						return matched; /* TRUE or ABORT */
				}

				NextChar(t, tlen);
			}

			/*
			 * End of text with no match, so no point in trying later places
			 * to start matching this pattern.
			 */
			return LIKE_ABORT;
		}
		else if (*p == '_')
		{
			/* _ matches any single character, and we know there is one */
			NextChar(t, tlen);
			NextByte(p, plen);
			continue;
		}
		else if (locale && !locale->deterministic)
		{
			/*
			 * For nondeterministic locales, we find the next substring of the
			 * pattern that does not contain wildcards and try to find a
			 * matching substring in the text.  Crucially, we cannot do this
			 * character by character, as in the normal case, but must do it
			 * substring by substring, partitioned by the wildcard characters.
			 * (This is per SQL standard.)
			 */
			const char *p1;
			size_t		p1len;
			const char *t1;
			size_t		t1len;
			bool		found_escape;
			const char *subpat;
			size_t		subpatlen;
			char	   *buf = NULL;

			/*
			 * Determine next substring of pattern without wildcards.  p is
			 * the start of the subpattern, p1 is one past the last byte. Also
			 * track if we found an escape character.
			 */
			p1 = p;
			p1len = plen;
			found_escape = false;
			while (p1len > 0)
			{
				if (*p1 == '\\')
				{
					found_escape = true;
					NextByte(p1, p1len);
					if (p1len == 0)
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_ESCAPE_SEQUENCE),
								 errmsg("LIKE pattern must not end with escape character")));
				}
				else if (*p1 == '_' || *p1 == '%')
					break;
				NextByte(p1, p1len);
			}

			/*
			 * If we found an escape character, then make an unescaped copy of
			 * the subpattern.
			 */
			if (found_escape)
			{
				char	   *b;

				b = buf = palloc(p1 - p);
				for (const char *c = p; c < p1; c++)
				{
					if (*c == '\\')
						;
					else
						*(b++) = *c;
				}

				subpat = buf;
				subpatlen = b - buf;
			}
			else
			{
				subpat = p;
				subpatlen = p1 - p;
			}

			/*
			 * Shortcut: If this is the end of the pattern, then the rest of
			 * the text has to match the rest of the pattern.
			 */
			if (p1len == 0)
			{
				int			cmp;

				cmp = pg_strncoll(subpat, subpatlen, t, tlen, locale);

				if (buf)
					pfree(buf);
				if (cmp == 0)
					return LIKE_TRUE;
				else
					return LIKE_FALSE;
			}

			/*
			 * Now build a substring of the text and try to match it against
			 * the subpattern.  t is the start of the text, t1 is one past the
			 * last byte.  We start with a zero-length string.
			 */
			t1 = t;
			t1len = tlen;
			for (;;)
			{
				int			cmp;

				CHECK_FOR_INTERRUPTS();

				cmp = pg_strncoll(subpat, subpatlen, t, (t1 - t), locale);

				/*
				 * If we found a match, we have to test if the rest of pattern
				 * can match against the rest of the string.  Otherwise we
				 * have to continue here try matching with a longer substring.
				 * (This is similar to the recursion for the '%' wildcard
				 * above.)
				 *
				 * Note that we can't just wind forward p and t and continue
				 * with the main loop.  This would fail for example with
				 *
				 * U&'\0061\0308bc' LIKE U&'\00E4_c' COLLATE ignore_accents
				 *
				 * You'd find that t=\0061 matches p=\00E4, but then the rest
				 * won't match; but t=\0061\0308 also matches p=\00E4, and
				 * then the rest will match.
				 */
				if (cmp == 0)
				{
					int			matched = MatchText(t1, t1len, p1, p1len, locale);

					if (matched == LIKE_TRUE)
					{
						if (buf)
							pfree(buf);
						return matched;
					}
				}

				/*
				 * Didn't match.  If we used up the whole text, then the match
				 * fails.  Otherwise, try again with a longer substring.
				 */
				if (t1len == 0)
				{
					if (buf)
						pfree(buf);
					return LIKE_FALSE;
				}
				else
					NextChar(t1, t1len);
			}
		}
		else if (GETCHAR(*p, locale) != GETCHAR(*t, locale))
		{
			/* non-wildcard pattern char fails to match text char */
			return LIKE_FALSE;
		}

		/*
		 * Pattern and text match, so advance.
		 *
		 * It is safe to use NextByte instead of NextChar here, even for
		 * multi-byte character sets, because we are not following immediately
		 * after a wildcard character. If we are in the middle of a multibyte
		 * character, we must already have matched at least one byte of the
		 * character from both text and pattern; so we cannot get out-of-sync
		 * on character boundaries.  And we know that no backend-legal
		 * encoding allows ASCII characters such as '%' to appear as non-first
		 * bytes of characters, so we won't mistakenly detect a new wildcard.
		 */
		NextByte(t, tlen);
		NextByte(p, plen);
	}

	if (tlen > 0)
		return LIKE_FALSE;		/* end of pattern, but not of text */

	/*
	 * End of text, but perhaps not of pattern.  Match iff the remaining
	 * pattern can match a zero-length string, ie, it's zero or more %'s.
	 */
	while (plen > 0 && *p == '%')
		NextByte(p, plen);
	if (plen <= 0)
		return LIKE_TRUE;

	/*
	 * End of text with no match, so no point in trying later places to start
	 * matching this pattern.
	 */
	return LIKE_ABORT;
}								/* MatchText() */

/*
 * like_escape() --- given a pattern and an ESCAPE string,
 * convert the pattern to use Postgres' standard backslash escape convention.
 */
#ifdef do_like_escape

static text *
do_like_escape(text *pat, text *esc)
{
	text	   *result;
	char	   *p,
			   *e,
			   *r;
	int			plen,
				elen;
	bool		afterescape;

	p = VARDATA_ANY(pat);
	plen = VARSIZE_ANY_EXHDR(pat);
	e = VARDATA_ANY(esc);
	elen = VARSIZE_ANY_EXHDR(esc);

	/*
	 * Worst-case pattern growth is 2x --- unlikely, but it's hardly worth
	 * trying to calculate the size more accurately than that.
	 */
	result = (text *) palloc(plen * 2 + VARHDRSZ);
	r = VARDATA(result);

	if (elen == 0)
	{
		/*
		 * No escape character is wanted.  Double any backslashes in the
		 * pattern to make them act like ordinary characters.
		 */
		while (plen > 0)
		{
			if (*p == '\\')
				*r++ = '\\';
			CopyAdvChar(r, p, plen);
		}
	}
	else
	{
		/*
		 * The specified escape must be only a single character.
		 */
		NextChar(e, elen);
		if (elen != 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_ESCAPE_SEQUENCE),
					 errmsg("invalid escape string"),
					 errhint("Escape string must be empty or one character.")));

		e = VARDATA_ANY(esc);
		elen = VARSIZE_ANY_EXHDR(esc);

		/*
		 * If specified escape is '\', just copy the pattern as-is.
		 */
		if (*e == '\\')
		{
			memcpy(result, pat, VARSIZE_ANY(pat));
			return result;
		}

		/*
		 * Otherwise, convert occurrences of the specified escape character to
		 * '\', and double occurrences of '\' --- unless they immediately
		 * follow an escape character!
		 */
		afterescape = false;
		while (plen > 0)
		{
			if (CHAREQ(p, plen, e, elen) && !afterescape)
			{
				*r++ = '\\';
				NextChar(p, plen);
				afterescape = true;
			}
			else if (*p == '\\')
			{
				*r++ = '\\';
				if (!afterescape)
					*r++ = '\\';
				NextChar(p, plen);
				afterescape = false;
			}
			else
			{
				CopyAdvChar(r, p, plen);
				afterescape = false;
			}
		}
	}

	SET_VARSIZE(result, r - ((char *) result));

	return result;
}
#endif							/* do_like_escape */

#ifdef CHAREQ
#undef CHAREQ
#endif

#undef NextChar
#undef CopyAdvChar
#undef MatchText

#ifdef do_like_escape
#undef do_like_escape
#endif

#undef GETCHAR

#ifdef MATCH_LOWER
#undef MATCH_LOWER

#endif



/* setup to compile like_match.c for UTF8 encoding, using fast NextChar */

#define NextChar(p, plen) \
	do { (p)++; (plen)--; } while ((plen) > 0 && (*(p) & 0xC0) == 0x80 )
#define MatchText	UTF8_MatchText

/* ==== like_match.c pasted VERBATIM (one stamping), exactly as like.c
 * #includes it ==== */
/*-------------------------------------------------------------------------
 *
 * like_match.c
 *	  LIKE pattern matching internal code.
 *
 * This file is included by like.c four times, to provide matching code for
 * (1) single-byte encodings, (2) UTF8, (3) other multi-byte encodings,
 * and (4) case insensitive matches in single-byte encodings.
 * (UTF8 is a special case because we can use a much more efficient version
 * of NextChar than can be used for general multi-byte encodings.)
 *
 * Before the inclusion, we need to define the following macros:
 *
 * NextChar
 * MatchText - to name of function wanted
 * do_like_escape - name of function if wanted - needs CHAREQ and CopyAdvChar
 * MATCH_LOWER - define for case (4) to specify case folding for 1-byte chars
 *
 * Copyright (c) 1996-2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	src/backend/utils/adt/like_match.c
 *
 *-------------------------------------------------------------------------
 */

/*
 *	Originally written by Rich $alz, mirror!rs, Wed Nov 26 19:03:17 EST 1986.
 *	Rich $alz is now <rsalz@bbn.com>.
 *	Special thanks to Lars Mathiesen <thorinn@diku.dk> for the
 *	LIKE_ABORT code.
 *
 *	This code was shamelessly stolen from the "pql" code by myself and
 *	slightly modified :)
 *
 *	All references to the word "star" were replaced by "percent"
 *	All references to the word "wild" were replaced by "like"
 *
 *	All the nice shell RE matching stuff was replaced by just "_" and "%"
 *
 *	As I don't have a copy of the SQL standard handy I wasn't sure whether
 *	to leave in the '\' escape character handling.
 *
 *	Keith Parks. <keith@mtcc.demon.co.uk>
 *
 *	SQL lets you specify the escape character by saying
 *	LIKE <pattern> ESCAPE <escape character>. We are a small operation
 *	so we force you to use '\'. - ay 7/95
 *
 *	Now we have the like_escape() function that converts patterns with
 *	any specified escape character (or none at all) to the internal
 *	default escape character, which is still '\'. - tgl 9/2000
 *
 * The code is rewritten to avoid requiring null-terminated strings,
 * which in turn allows us to leave out some memcpy() operations.
 * This code should be faster and take less memory, but no promises...
 * - thomas 2000-08-06
 */


/*--------------------
 *	Match text and pattern, return LIKE_TRUE, LIKE_FALSE, or LIKE_ABORT.
 *
 *	LIKE_TRUE: they match
 *	LIKE_FALSE: they don't match
 *	LIKE_ABORT: not only don't they match, but the text is too short.
 *
 * If LIKE_ABORT is returned, then no suffix of the text can match the
 * pattern either, so an upper-level % scan can stop scanning now.
 *--------------------
 */

#ifdef MATCH_LOWER
#define GETCHAR(t, locale) MATCH_LOWER(t, locale)
#else
#define GETCHAR(t, locale) (t)
#endif

static int
MatchText(const char *t, int tlen, const char *p, int plen, pg_locale_t locale)
{
	/* Fast path for match-everything pattern */
	if (plen == 1 && *p == '%')
		return LIKE_TRUE;

	/* Since this function recurses, it could be driven to stack overflow */
	check_stack_depth();

	/*
	 * In this loop, we advance by char when matching wildcards (and thus on
	 * recursive entry to this function we are properly char-synced). On other
	 * occasions it is safe to advance by byte, as the text and pattern will
	 * be in lockstep. This allows us to perform all comparisons between the
	 * text and pattern on a byte by byte basis, even for multi-byte
	 * encodings.
	 */
	while (tlen > 0 && plen > 0)
	{
		if (*p == '\\')
		{
			/* Next pattern byte must match literally, whatever it is */
			NextByte(p, plen);
			/* ... and there had better be one, per SQL standard */
			if (plen <= 0)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_ESCAPE_SEQUENCE),
						 errmsg("LIKE pattern must not end with escape character")));
			if (GETCHAR(*p, locale) != GETCHAR(*t, locale))
				return LIKE_FALSE;
		}
		else if (*p == '%')
		{
			char		firstpat;

			/*
			 * % processing is essentially a search for a text position at
			 * which the remainder of the text matches the remainder of the
			 * pattern, using a recursive call to check each potential match.
			 *
			 * If there are wildcards immediately following the %, we can skip
			 * over them first, using the idea that any sequence of N _'s and
			 * one or more %'s is equivalent to N _'s and one % (ie, it will
			 * match any sequence of at least N text characters).  In this way
			 * we will always run the recursive search loop using a pattern
			 * fragment that begins with a literal character-to-match, thereby
			 * not recursing more than we have to.
			 */
			NextByte(p, plen);

			while (plen > 0)
			{
				if (*p == '%')
					NextByte(p, plen);
				else if (*p == '_')
				{
					/* If not enough text left to match the pattern, ABORT */
					if (tlen <= 0)
						return LIKE_ABORT;
					NextChar(t, tlen);
					NextByte(p, plen);
				}
				else
					break;		/* Reached a non-wildcard pattern char */
			}

			/*
			 * If we're at end of pattern, match: we have a trailing % which
			 * matches any remaining text string.
			 */
			if (plen <= 0)
				return LIKE_TRUE;

			/*
			 * Otherwise, scan for a text position at which we can match the
			 * rest of the pattern.  The first remaining pattern char is known
			 * to be a regular or escaped literal character, so we can compare
			 * the first pattern byte to each text byte to avoid recursing
			 * more than we have to.  This fact also guarantees that we don't
			 * have to consider a match to the zero-length substring at the
			 * end of the text.  With a nondeterministic collation, we can't
			 * rely on the first bytes being equal, so we have to recurse in
			 * any case.
			 */
			if (*p == '\\')
			{
				if (plen < 2)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_ESCAPE_SEQUENCE),
							 errmsg("LIKE pattern must not end with escape character")));
				firstpat = GETCHAR(p[1], locale);
			}
			else
				firstpat = GETCHAR(*p, locale);

			while (tlen > 0)
			{
				if (GETCHAR(*t, locale) == firstpat || (locale && !locale->deterministic))
				{
					int			matched = MatchText(t, tlen, p, plen, locale);

					if (matched != LIKE_FALSE)
						return matched; /* TRUE or ABORT */
				}

				NextChar(t, tlen);
			}

			/*
			 * End of text with no match, so no point in trying later places
			 * to start matching this pattern.
			 */
			return LIKE_ABORT;
		}
		else if (*p == '_')
		{
			/* _ matches any single character, and we know there is one */
			NextChar(t, tlen);
			NextByte(p, plen);
			continue;
		}
		else if (locale && !locale->deterministic)
		{
			/*
			 * For nondeterministic locales, we find the next substring of the
			 * pattern that does not contain wildcards and try to find a
			 * matching substring in the text.  Crucially, we cannot do this
			 * character by character, as in the normal case, but must do it
			 * substring by substring, partitioned by the wildcard characters.
			 * (This is per SQL standard.)
			 */
			const char *p1;
			size_t		p1len;
			const char *t1;
			size_t		t1len;
			bool		found_escape;
			const char *subpat;
			size_t		subpatlen;
			char	   *buf = NULL;

			/*
			 * Determine next substring of pattern without wildcards.  p is
			 * the start of the subpattern, p1 is one past the last byte. Also
			 * track if we found an escape character.
			 */
			p1 = p;
			p1len = plen;
			found_escape = false;
			while (p1len > 0)
			{
				if (*p1 == '\\')
				{
					found_escape = true;
					NextByte(p1, p1len);
					if (p1len == 0)
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_ESCAPE_SEQUENCE),
								 errmsg("LIKE pattern must not end with escape character")));
				}
				else if (*p1 == '_' || *p1 == '%')
					break;
				NextByte(p1, p1len);
			}

			/*
			 * If we found an escape character, then make an unescaped copy of
			 * the subpattern.
			 */
			if (found_escape)
			{
				char	   *b;

				b = buf = palloc(p1 - p);
				for (const char *c = p; c < p1; c++)
				{
					if (*c == '\\')
						;
					else
						*(b++) = *c;
				}

				subpat = buf;
				subpatlen = b - buf;
			}
			else
			{
				subpat = p;
				subpatlen = p1 - p;
			}

			/*
			 * Shortcut: If this is the end of the pattern, then the rest of
			 * the text has to match the rest of the pattern.
			 */
			if (p1len == 0)
			{
				int			cmp;

				cmp = pg_strncoll(subpat, subpatlen, t, tlen, locale);

				if (buf)
					pfree(buf);
				if (cmp == 0)
					return LIKE_TRUE;
				else
					return LIKE_FALSE;
			}

			/*
			 * Now build a substring of the text and try to match it against
			 * the subpattern.  t is the start of the text, t1 is one past the
			 * last byte.  We start with a zero-length string.
			 */
			t1 = t;
			t1len = tlen;
			for (;;)
			{
				int			cmp;

				CHECK_FOR_INTERRUPTS();

				cmp = pg_strncoll(subpat, subpatlen, t, (t1 - t), locale);

				/*
				 * If we found a match, we have to test if the rest of pattern
				 * can match against the rest of the string.  Otherwise we
				 * have to continue here try matching with a longer substring.
				 * (This is similar to the recursion for the '%' wildcard
				 * above.)
				 *
				 * Note that we can't just wind forward p and t and continue
				 * with the main loop.  This would fail for example with
				 *
				 * U&'\0061\0308bc' LIKE U&'\00E4_c' COLLATE ignore_accents
				 *
				 * You'd find that t=\0061 matches p=\00E4, but then the rest
				 * won't match; but t=\0061\0308 also matches p=\00E4, and
				 * then the rest will match.
				 */
				if (cmp == 0)
				{
					int			matched = MatchText(t1, t1len, p1, p1len, locale);

					if (matched == LIKE_TRUE)
					{
						if (buf)
							pfree(buf);
						return matched;
					}
				}

				/*
				 * Didn't match.  If we used up the whole text, then the match
				 * fails.  Otherwise, try again with a longer substring.
				 */
				if (t1len == 0)
				{
					if (buf)
						pfree(buf);
					return LIKE_FALSE;
				}
				else
					NextChar(t1, t1len);
			}
		}
		else if (GETCHAR(*p, locale) != GETCHAR(*t, locale))
		{
			/* non-wildcard pattern char fails to match text char */
			return LIKE_FALSE;
		}

		/*
		 * Pattern and text match, so advance.
		 *
		 * It is safe to use NextByte instead of NextChar here, even for
		 * multi-byte character sets, because we are not following immediately
		 * after a wildcard character. If we are in the middle of a multibyte
		 * character, we must already have matched at least one byte of the
		 * character from both text and pattern; so we cannot get out-of-sync
		 * on character boundaries.  And we know that no backend-legal
		 * encoding allows ASCII characters such as '%' to appear as non-first
		 * bytes of characters, so we won't mistakenly detect a new wildcard.
		 */
		NextByte(t, tlen);
		NextByte(p, plen);
	}

	if (tlen > 0)
		return LIKE_FALSE;		/* end of pattern, but not of text */

	/*
	 * End of text, but perhaps not of pattern.  Match iff the remaining
	 * pattern can match a zero-length string, ie, it's zero or more %'s.
	 */
	while (plen > 0 && *p == '%')
		NextByte(p, plen);
	if (plen <= 0)
		return LIKE_TRUE;

	/*
	 * End of text with no match, so no point in trying later places to start
	 * matching this pattern.
	 */
	return LIKE_ABORT;
}								/* MatchText() */

/*
 * like_escape() --- given a pattern and an ESCAPE string,
 * convert the pattern to use Postgres' standard backslash escape convention.
 */
#ifdef do_like_escape

static text *
do_like_escape(text *pat, text *esc)
{
	text	   *result;
	char	   *p,
			   *e,
			   *r;
	int			plen,
				elen;
	bool		afterescape;

	p = VARDATA_ANY(pat);
	plen = VARSIZE_ANY_EXHDR(pat);
	e = VARDATA_ANY(esc);
	elen = VARSIZE_ANY_EXHDR(esc);

	/*
	 * Worst-case pattern growth is 2x --- unlikely, but it's hardly worth
	 * trying to calculate the size more accurately than that.
	 */
	result = (text *) palloc(plen * 2 + VARHDRSZ);
	r = VARDATA(result);

	if (elen == 0)
	{
		/*
		 * No escape character is wanted.  Double any backslashes in the
		 * pattern to make them act like ordinary characters.
		 */
		while (plen > 0)
		{
			if (*p == '\\')
				*r++ = '\\';
			CopyAdvChar(r, p, plen);
		}
	}
	else
	{
		/*
		 * The specified escape must be only a single character.
		 */
		NextChar(e, elen);
		if (elen != 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_ESCAPE_SEQUENCE),
					 errmsg("invalid escape string"),
					 errhint("Escape string must be empty or one character.")));

		e = VARDATA_ANY(esc);
		elen = VARSIZE_ANY_EXHDR(esc);

		/*
		 * If specified escape is '\', just copy the pattern as-is.
		 */
		if (*e == '\\')
		{
			memcpy(result, pat, VARSIZE_ANY(pat));
			return result;
		}

		/*
		 * Otherwise, convert occurrences of the specified escape character to
		 * '\', and double occurrences of '\' --- unless they immediately
		 * follow an escape character!
		 */
		afterescape = false;
		while (plen > 0)
		{
			if (CHAREQ(p, plen, e, elen) && !afterescape)
			{
				*r++ = '\\';
				NextChar(p, plen);
				afterescape = true;
			}
			else if (*p == '\\')
			{
				*r++ = '\\';
				if (!afterescape)
					*r++ = '\\';
				NextChar(p, plen);
				afterescape = false;
			}
			else
			{
				CopyAdvChar(r, p, plen);
				afterescape = false;
			}
		}
	}

	SET_VARSIZE(result, r - ((char *) result));

	return result;
}
#endif							/* do_like_escape */

#ifdef CHAREQ
#undef CHAREQ
#endif

#undef NextChar
#undef CopyAdvChar
#undef MatchText

#ifdef do_like_escape
#undef do_like_escape
#endif

#undef GETCHAR

#ifdef MATCH_LOWER
#undef MATCH_LOWER

#endif



/* Generic for all cases not requiring inline case-folding */
static inline int
GenericMatchText(const char *s, int slen, const char *p, int plen, Oid collation)
{
	pg_locale_t locale;

	if (!OidIsValid(collation))
	{
		/*
		 * This typically means that the parser could not resolve a conflict
		 * of implicit collations, so report it that way.
		 */
		ereport(ERROR,
				(errcode(ERRCODE_INDETERMINATE_COLLATION),
				 errmsg("could not determine which collation to use for LIKE"),
				 errhint("Use the COLLATE clause to set the collation explicitly.")));
	}

	locale = pg_newlocale_from_collation(collation);

	if (pg_database_encoding_max_length() == 1)
		return SB_MatchText(s, slen, p, plen, locale);
	else if (GetDatabaseEncoding() == PG_UTF8)
		return UTF8_MatchText(s, slen, p, plen, locale);
	else
		return MB_MatchText(s, slen, p, plen, locale);
}

static inline int
Generic_Text_IC_like(text *str, text *pat, Oid collation)
{
	char	   *s,
			   *p;
	int			slen,
				plen;
	pg_locale_t locale;

	if (!OidIsValid(collation))
	{
		/*
		 * This typically means that the parser could not resolve a conflict
		 * of implicit collations, so report it that way.
		 */
		ereport(ERROR,
				(errcode(ERRCODE_INDETERMINATE_COLLATION),
				 errmsg("could not determine which collation to use for ILIKE"),
				 errhint("Use the COLLATE clause to set the collation explicitly.")));
	}

	locale = pg_newlocale_from_collation(collation);

	if (!locale->deterministic)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("nondeterministic collations are not supported for ILIKE")));

	/*
	 * For efficiency reasons, in the single byte case we don't call lower()
	 * on the pattern and text, but instead call SB_lower_char on each
	 * character.  In the multi-byte case we don't have much choice :-(. Also,
	 * ICU does not support single-character case folding, so we go the long
	 * way.
	 */

	if (pg_database_encoding_max_length() > 1 || (locale->provider == COLLPROVIDER_ICU))
	{
		pat = DatumGetTextPP(DirectFunctionCall1Coll(lower, collation,
													 PointerGetDatum(pat)));
		p = VARDATA_ANY(pat);
		plen = VARSIZE_ANY_EXHDR(pat);
		str = DatumGetTextPP(DirectFunctionCall1Coll(lower, collation,
													 PointerGetDatum(str)));
		s = VARDATA_ANY(str);
		slen = VARSIZE_ANY_EXHDR(str);
		if (GetDatabaseEncoding() == PG_UTF8)
			return UTF8_MatchText(s, slen, p, plen, 0);
		else
			return MB_MatchText(s, slen, p, plen, 0);
	}
	else
	{
		p = VARDATA_ANY(pat);
		plen = VARSIZE_ANY_EXHDR(pat);
		s = VARDATA_ANY(str);
		slen = VARSIZE_ANY_EXHDR(str);
		return SB_IMatchText(s, slen, p, plen, locale);
	}
}



/* ========== SECTION 2: fuzz-facing driver entries (NOT Postgres code) =====
 *
 * One pg_diff_like_* wrapper per fuzz arm (shim 1): FIRST
 * pg_diff_arena_reset(), then pg_diff_errcode = 0, setjmp the ereport
 * plane, build the 4B text images, run the VERBATIM interface body, and
 * return an int status (0 = ok, else the errcode class) writing results
 * through caller buffers. The statement bodies between the mktext lines and
 * the *out stores are like.c's fmgr bodies verbatim (PG_GETARG_* /
 * PG_RETURN_BOOL / PG_GET_COLLATION unwrapped per shim 1).
 */

static text *
pg_like_mktext(const char *data, int len)
{
	text	   *t = (text *) palloc(len + VARHDRSZ);

	SET_VARSIZE(t, len + VARHDRSZ);
	memcpy(VARDATA(t), data, len);
	return t;
}

#define PG_DIFF_LIKE_ENTRY_PROLOGUE() \
	do { \
		pg_diff_arena_reset(); \
		pg_diff_errcode = 0; \
		if (setjmp(pg_like_jmp) != 0) \
			return pg_diff_errcode; \
	} while (0)

/* textlike [oid 850] */
int
pg_diff_like_textlike(const char *sdat, int slen_in, const char *pdat,
					  int plen_in, unsigned int collation, int *out)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	PG_DIFF_LIKE_ENTRY_PROLOGUE();
	{
		text	   *str = pg_like_mktext(sdat, slen_in);
		text	   *pat = pg_like_mktext(pdat, plen_in);
		bool		result;
		char	   *s,
				   *p;
		int			slen,
					plen;

		s = VARDATA_ANY(str);
		slen = VARSIZE_ANY_EXHDR(str);
		p = VARDATA_ANY(pat);
		plen = VARSIZE_ANY_EXHDR(pat);

		result = (GenericMatchText(s, slen, p, plen, collation) == LIKE_TRUE);

		*out = result ? 1 : 0;
	}
	return 0;
}

/* textnlike [oid 851] */
int
pg_diff_like_textnlike(const char *sdat, int slen_in, const char *pdat,
					   int plen_in, unsigned int collation, int *out)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	PG_DIFF_LIKE_ENTRY_PROLOGUE();
	{
		text	   *str = pg_like_mktext(sdat, slen_in);
		text	   *pat = pg_like_mktext(pdat, plen_in);
		bool		result;
		char	   *s,
				   *p;
		int			slen,
					plen;

		s = VARDATA_ANY(str);
		slen = VARSIZE_ANY_EXHDR(str);
		p = VARDATA_ANY(pat);
		plen = VARSIZE_ANY_EXHDR(pat);

		result = (GenericMatchText(s, slen, p, plen, collation) != LIKE_TRUE);

		*out = result ? 1 : 0;
	}
	return 0;
}

/* namelike [oid 858]; name64 = 64-byte NameData block */
int
pg_diff_like_namelike(const char *name64, const char *pdat, int plen_in,
					  unsigned int collation, int *out)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	PG_DIFF_LIKE_ENTRY_PROLOGUE();
	{
		Name		str = (Name) name64;
		text	   *pat = pg_like_mktext(pdat, plen_in);
		bool		result;
		char	   *s,
				   *p;
		int			slen,
					plen;

		s = NameStr(*str);
		slen = strlen(s);
		p = VARDATA_ANY(pat);
		plen = VARSIZE_ANY_EXHDR(pat);

		result = (GenericMatchText(s, slen, p, plen, collation) == LIKE_TRUE);

		*out = result ? 1 : 0;
	}
	return 0;
}

/* namenlike [oid 859] */
int
pg_diff_like_namenlike(const char *name64, const char *pdat, int plen_in,
					   unsigned int collation, int *out)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	PG_DIFF_LIKE_ENTRY_PROLOGUE();
	{
		Name		str = (Name) name64;
		text	   *pat = pg_like_mktext(pdat, plen_in);
		bool		result;
		char	   *s,
				   *p;
		int			slen,
					plen;

		s = NameStr(*str);
		slen = strlen(s);
		p = VARDATA_ANY(pat);
		plen = VARSIZE_ANY_EXHDR(pat);

		result = (GenericMatchText(s, slen, p, plen, collation) != LIKE_TRUE);

		*out = result ? 1 : 0;
	}
	return 0;
}

/* texticlike [oid 1633] */
int
pg_diff_like_texticlike(const char *sdat, int slen_in, const char *pdat,
						int plen_in, unsigned int collation, int *out)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	PG_DIFF_LIKE_ENTRY_PROLOGUE();
	{
		text	   *str = pg_like_mktext(sdat, slen_in);
		text	   *pat = pg_like_mktext(pdat, plen_in);
		bool		result;

		result = (Generic_Text_IC_like(str, pat, collation) == LIKE_TRUE);

		*out = result ? 1 : 0;
	}
	return 0;
}

/* texticnlike [oid 1634] */
int
pg_diff_like_texticnlike(const char *sdat, int slen_in, const char *pdat,
						 int plen_in, unsigned int collation, int *out)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	PG_DIFF_LIKE_ENTRY_PROLOGUE();
	{
		text	   *str = pg_like_mktext(sdat, slen_in);
		text	   *pat = pg_like_mktext(pdat, plen_in);
		bool		result;

		result = (Generic_Text_IC_like(str, pat, collation) != LIKE_TRUE);

		*out = result ? 1 : 0;
	}
	return 0;
}

/* nameiclike [oid 1635] */
int
pg_diff_like_nameiclike(const char *name64, const char *pdat, int plen_in,
						unsigned int collation, int *out)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	PG_DIFF_LIKE_ENTRY_PROLOGUE();
	{
		Name		str = (Name) name64;
		text	   *pat = pg_like_mktext(pdat, plen_in);
		bool		result;
		text	   *strtext;

		strtext = DatumGetTextPP(DirectFunctionCall1(name_text,
													 NameGetDatum(str)));
		result = (Generic_Text_IC_like(strtext, pat, collation) == LIKE_TRUE);

		*out = result ? 1 : 0;
	}
	return 0;
}

/* nameicnlike [oid 1636] */
int
pg_diff_like_nameicnlike(const char *name64, const char *pdat, int plen_in,
						 unsigned int collation, int *out)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	PG_DIFF_LIKE_ENTRY_PROLOGUE();
	{
		Name		str = (Name) name64;
		text	   *pat = pg_like_mktext(pdat, plen_in);
		bool		result;
		text	   *strtext;

		strtext = DatumGetTextPP(DirectFunctionCall1(name_text,
													 NameGetDatum(str)));
		result = (Generic_Text_IC_like(strtext, pat, collation) != LIKE_TRUE);

		*out = result ? 1 : 0;
	}
	return 0;
}

/* bytealike [oid 2005] */
int
pg_diff_like_bytealike(const char *sdat, int slen_in, const char *pdat,
					   int plen_in, int *out)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	PG_DIFF_LIKE_ENTRY_PROLOGUE();
	{
		bytea	   *str = pg_like_mktext(sdat, slen_in);
		bytea	   *pat = pg_like_mktext(pdat, plen_in);
		bool		result;
		char	   *s,
				   *p;
		int			slen,
					plen;

		s = VARDATA_ANY(str);
		slen = VARSIZE_ANY_EXHDR(str);
		p = VARDATA_ANY(pat);
		plen = VARSIZE_ANY_EXHDR(pat);

		result = (SB_MatchText(s, slen, p, plen, 0) == LIKE_TRUE);

		*out = result ? 1 : 0;
	}
	return 0;
}

/* byteanlike [oid 2006] */
int
pg_diff_like_byteanlike(const char *sdat, int slen_in, const char *pdat,
						int plen_in, int *out)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	PG_DIFF_LIKE_ENTRY_PROLOGUE();
	{
		bytea	   *str = pg_like_mktext(sdat, slen_in);
		bytea	   *pat = pg_like_mktext(pdat, plen_in);
		bool		result;
		char	   *s,
				   *p;
		int			slen,
					plen;

		s = VARDATA_ANY(str);
		slen = VARSIZE_ANY_EXHDR(str);
		p = VARDATA_ANY(pat);
		plen = VARSIZE_ANY_EXHDR(pat);

		result = (SB_MatchText(s, slen, p, plen, 0) != LIKE_TRUE);

		*out = result ? 1 : 0;
	}
	return 0;
}

/* like_escape [oid 1637]; out must hold >= 2 * plen_in bytes */
int
pg_diff_like_escape(const char *pdat, int plen_in, const char *edat,
					int elen_in, char *out, int *outlen)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	PG_DIFF_LIKE_ENTRY_PROLOGUE();
	{
		text	   *pat = pg_like_mktext(pdat, plen_in);
		text	   *esc = pg_like_mktext(edat, elen_in);
		text	   *result;

		if (pg_database_encoding_max_length() == 1)
			result = SB_do_like_escape(pat, esc);
		else
			result = MB_do_like_escape(pat, esc);

		*outlen = VARSIZE_ANY_EXHDR(result);
		memcpy(out, VARDATA_ANY(result), *outlen);
	}
	return 0;
}

/* like_escape_bytea [oid 2009]; out must hold >= 2 * plen_in bytes */
int
pg_diff_like_escape_bytea(const char *pdat, int plen_in, const char *edat,
						  int elen_in, char *out, int *outlen)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	PG_DIFF_LIKE_ENTRY_PROLOGUE();
	{
		bytea	   *pat = pg_like_mktext(pdat, plen_in);
		bytea	   *esc = pg_like_mktext(edat, elen_in);
		bytea	   *result = SB_do_like_escape((text *) pat, (text *) esc);

		*outlen = VARSIZE_ANY_EXHDR(result);
		memcpy(out, VARDATA_ANY(result), *outlen);
	}
	return 0;
}

/* --- direct kernel entries (driver arms 12..14): the like_match.c
 * stampings themselves, returning the raw LIKE_TRUE/LIKE_FALSE/LIKE_ABORT
 * tristate.  NOT Postgres fmgr rows — these diff the shipped crate's pub
 * kernel wrappers (adt_like::sb_match_text / utf8_match_text /
 * sb_imatch_text) against the same stampings that GenericMatchText /
 * Generic_Text_IC_like dispatch to above.  None of these three stampings
 * consults pg_mblen (the UTF8 stamping's NextChar is the pure
 * continuation-byte skip), so raw bytes are in-domain; the only reachable
 * error is the trailing-escape 22025 (class 1).  use_locale selects a NULL
 * locale (C's bytealike / lowered-ILIKE call shape, `MatchText(..., 0)`)
 * vs the pinned C locale; SB_IMatchText always takes the C locale (its
 * GETCHAR case-folds through it). */
int
pg_diff_like_sb_match(const char *t, int tlen, const char *p, int plen,
					  int use_locale, int *out)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	PG_DIFF_LIKE_ENTRY_PROLOGUE();
	*out = SB_MatchText(t, tlen, p, plen,
						use_locale ? &pg_like_c_locale : NULL);
	return 0;
}

int
pg_diff_like_utf8_match(const char *t, int tlen, const char *p, int plen,
						int use_locale, int *out)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	PG_DIFF_LIKE_ENTRY_PROLOGUE();
	*out = UTF8_MatchText(t, tlen, p, plen,
						  use_locale ? &pg_like_c_locale : NULL);
	return 0;
}

int
pg_diff_like_sb_imatch(const char *t, int tlen, const char *p, int plen,
					   int *out)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	PG_DIFF_LIKE_ENTRY_PROLOGUE();
	*out = SB_IMatchText(t, tlen, p, plen, &pg_like_c_locale);
	return 0;
}
