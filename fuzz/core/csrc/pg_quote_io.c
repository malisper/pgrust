/*
 * pg_quote_io.c: vendored PostgreSQL C oracle for the quote_diff differential
 * fuzz target (100%-coverage campaign; crate crates/backend/utils/adt/quote).
 *
 * Provenance: PostgreSQL 18.3 (Stamp-18.3, upstream sha
 * 62d6c7d3df6287f1bd83199c1a746e50d31571a0), verified against the repo's
 * vendored ground-truth checkout ../pgrust-fabled/vendor/postgres-src.
 *   - SECTION 1: quote_literal_internal — VERBATIM from
 *     src/backend/utils/adt/quote.c (the whole value core of quote_literal /
 *     quote_nullable; the fmgr wrappers quote_literal/quote_nullable are
 *     pure plumbing reproduced in the SECTION 3 driver entries).
 *   - SECTION 2: quote_identifier — VERBATIM from
 *     src/backend/utils/adt/ruleutils.c (the value core of quote_ident;
 *     quote.c's quote_ident fmgr wrapper is text_to_cstring ->
 *     quote_identifier -> cstring_to_text plumbing, reproduced in the
 *     SECTION 3 driver entry).
 *
 * Shims (PLUMBING ONLY, each listed):
 *   - SQL_STR_DOUBLE / ESCAPE_STRING_SYNTAX: vendored VERBATIM from
 *     src/include/c.h (they are #defines the quote.c body expands).
 *   - UNRESERVED_KEYWORD: vendored VERBATIM from
 *     src/include/common/keywords.h (#define UNRESERVED_KEYWORD 0).
 *   - bool/true/false -> <stdbool.h>; palloc -> the TLS arena below.
 *   - quote_all_identifiers (GUC bool, ruleutils.c:339) -> a TLS bool the
 *     driver entry sets per call, #define'd over the verbatim body's name.
 *     The GUC is an INPUT PLANE of the differential (both values fuzzed),
 *     not a carve.
 *   - ScanKeywordLookup + ScanKeywords: extern from the pg_enc_tables.c TU
 *     in this same oracle library — that TU vendors src/common/kwlookup.c
 *     VERBATIM and includes THE SHIPPED CRATE'S generated kwlist_d.h
 *     (crates/common/keywords), so hash/table parity is by shared source of
 *     truth and a Rust-side transcription drift is a divergence.
 *   - ScanKeywordCategories: extern from csrc/tablesfam/keywords.c (verbatim
 *     18.3 src/common/keywords.c; the symbol is NOT on the tablesfam rename
 *     list, deliberately relied on here). That is the only two-sided oracle
 *     for the category table (lanef's DUPLICATION-LEDGER note).
 *
 * Errcode capture: none of the vendored bodies can ereport — quote_ident /
 * quote_literal / quote_nullable have NO error paths (all inputs quote to
 * something). The shared pg_diff_errcode channel is still reset per entry so
 * the harness verdict plane stays uniform.
 */

#include <assert.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#include "postgres.h"			/* shim: PG fixed-width typedefs (uint16 etc.) */
#include "common/kwlookup.h"	/* ScanKeywordList + ScanKeywordLookup decl */

/* the ryu shim postgres.h maps palloc->malloc for d2s.c; this oracle uses
 * the TLS arena below instead (memory-context-reset model) */
#undef palloc

/* Shared TLS errcode channel (defined in csrc/pg_float_io.c). */
extern _Thread_local int pg_diff_errcode;

/* Keyword tables: see the shim list in the header. */
extern const ScanKeywordList ScanKeywords;
extern const uint8_t ScanKeywordCategories[];

/* src/include/common/keywords.h VERBATIM */
#define UNRESERVED_KEYWORD		0

/* src/include/c.h VERBATIM */
#define SQL_STR_DOUBLE(ch, escape_backslash)	\
	((ch) == '\'' || ((ch) == '\\' && (escape_backslash)))
#define ESCAPE_STRING_SYNTAX	'E'

/* GUC shim: ruleutils.c:339 `bool quote_all_identifiers = false;` */
static _Thread_local bool pg_diff_quote_all_identifiers_v = false;
#define quote_all_identifiers pg_diff_quote_all_identifiers_v

/* palloc arena shim: PostgreSQL frees these via memory-context reset; the
 * oracle mirrors that with a TLS pointer arena reset at every pg_diff_*
 * dispatcher entry, so error-path longjmp/ereturn/goto exits cannot leak.
 * (Pattern proven on proofs/p1-lanej @ 7306d300196 — copied, not re-derived.) */
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

#define palloc(n) pg_diff_palloc_impl(n)

/* ==================== SECTION 1: quote.c (VERBATIM) ==================== */

/*
 * quote_literal_internal -
 *	  helper function for quote_literal and quote_literal_cstr
 *
 * NOTE: think not to make this function's behavior change with
 * standard_conforming_strings.  We don't know where the result
 * literal will be used, and so we must generate a result that
 * will work with either setting.  Take a look at what dblink
 * uses this for before thinking you know better.
 */
static size_t
quote_literal_internal(char *dst, const char *src, size_t len)
{
	const char *s;
	char	   *savedst = dst;

	for (s = src; s < src + len; s++)
	{
		if (*s == '\\')
		{
			*dst++ = ESCAPE_STRING_SYNTAX;
			break;
		}
	}

	*dst++ = '\'';
	while (len-- > 0)
	{
		if (SQL_STR_DOUBLE(*src, true))
			*dst++ = *src;
		*dst++ = *src++;
	}
	*dst++ = '\'';

	return dst - savedst;
}

/* ================= SECTION 2: ruleutils.c (VERBATIM) =================== */

/*
 * quote_identifier			- Quote an identifier only if needed
 *
 * When quotes are needed, we palloc the required space; slightly
 * space-wasteful but well worth it for notational simplicity.
 */
const char *
quote_identifier(const char *ident)
{
	/*
	 * Can avoid quoting if ident starts with a lowercase letter or underscore
	 * and contains only lowercase letters, digits, and underscores, *and* is
	 * not any SQL keyword.  Otherwise, supply quotes.
	 */
	int			nquotes = 0;
	bool		safe;
	const char *ptr;
	char	   *result;
	char	   *optr;

	/*
	 * would like to use <ctype.h> macros here, but they might yield unwanted
	 * locale-specific results...
	 */
	safe = ((ident[0] >= 'a' && ident[0] <= 'z') || ident[0] == '_');

	for (ptr = ident; *ptr; ptr++)
	{
		char		ch = *ptr;

		if ((ch >= 'a' && ch <= 'z') ||
			(ch >= '0' && ch <= '9') ||
			(ch == '_'))
		{
			/* okay */
		}
		else
		{
			safe = false;
			if (ch == '"')
				nquotes++;
		}
	}

	if (quote_all_identifiers)
		safe = false;

	if (safe)
	{
		/*
		 * Check for keyword.  We quote keywords except for unreserved ones.
		 * (In some cases we could avoid quoting a col_name or type_func_name
		 * keyword, but it seems much harder than it's worth to tell that.)
		 *
		 * Note: ScanKeywordLookup() does case-insensitive comparison, but
		 * that's fine, since we already know we have all-lower-case.
		 */
		int			kwnum = ScanKeywordLookup(ident, &ScanKeywords);

		if (kwnum >= 0 && ScanKeywordCategories[kwnum] != UNRESERVED_KEYWORD)
			safe = false;
	}

	if (safe)
		return ident;			/* no change needed */

	result = (char *) palloc(strlen(ident) + nquotes + 2 + 1);

	optr = result;
	*optr++ = '"';
	for (ptr = ident; *ptr; ptr++)
	{
		char		ch = *ptr;

		if (ch == '"')
			*optr++ = '"';
		*optr++ = ch;
	}
	*optr++ = '"';
	*optr = '\0';

	return result;
}

/* ========== SECTION 3: fuzz-facing driver entries (NOT Postgres code) ===== */

/*
 * pg_diff_quote_ident [oid 1282]: quote.c's quote_ident is
 * text_to_cstring -> quote_identifier -> cstring_to_text; the text<->cstring
 * plumbing is reproduced by the harness contract (NUL-free payloads only —
 * PG text never contains NUL). `out` must hold strlen(ident)*2 + 3 bytes
 * (worst case: all-quotes ident -> len + nquotes + 2 + NUL).
 * Returns the result byte length (excluding NUL).
 */
size_t
pg_diff_quote_ident(const char *ident, int quote_all, char *out)
{
	const char *r;

	pg_diff_arena_reset();
	pg_diff_errcode = 0;
	pg_diff_quote_all_identifiers_v = (quote_all != 0);
	r = quote_identifier(ident);
	strcpy(out, r);
	return strlen(out);
}

/*
 * pg_diff_quote_literal [oid 1283]: quote.c's quote_literal is a worst-case
 * palloc + quote_literal_internal over VARDATA bytes; byte-oriented, so
 * embedded NUL is in-domain. `dst` must hold len*2 + 3 bytes.
 * Returns the result byte length.
 */
size_t
pg_diff_quote_literal(const char *src, size_t len, char *dst)
{
	pg_diff_arena_reset();
	pg_diff_errcode = 0;
	return quote_literal_internal(dst, src, len);
}

/*
 * pg_diff_quote_nullable [oid 1289]: NULL -> the text "NULL"; otherwise
 * exactly quote_literal (quote.c dispatches via DirectFunctionCall1).
 */
size_t
pg_diff_quote_nullable(int isnull, const char *src, size_t len, char *dst)
{
	pg_diff_arena_reset();
	pg_diff_errcode = 0;
	if (isnull)
	{
		memcpy(dst, "NULL", 4);
		return 4;
	}
	return quote_literal_internal(dst, src, len);
}
