/*
 * ts_locale_excerpt.c — EXCERPT of src/backend/tsearch/ts_locale.c
 * @ postgres-src 62d6c7d3df6287f1bd83199c1a746e50d31571a0 (PostgreSQL 18.3).
 *
 * VERBATIM: lines 23-69 of the upstream file (the WC_BUF_LEN comment/define,
 * the GENERATE_T_ISCLASS_DEF macro, and its alnum/alpha instantiations) —
 * the only part of ts_locale.c the vendored tsquery family calls
 * (t_isalnum_cstr from tsquery.c parse_or_operator).
 *
 * CARVE (documented, not a shim): the tsearch_readline_* dictionary-file
 * machinery (upstream lines 71-end) is NOT vendored — it reads tsearch
 * config files via fd.c/error_context_stack, is unreachable from every
 * pg_diff_* entry in this family, and is session/catalog state the phase-1
 * filter excludes.
 *
 * Environment (see shim/utils/pg_locale.h + shim/pg_tsq_shim.c):
 *   - database_ctype_is_c: driver-settable knob (pg_tsq_set_database_ctype_is_c)
 *     mirroring the Rust seam ::pg_locale::database_ctype_is_c.
 *   - char2wchar -> mbstowcs (the pg_locale.c default-locale arm); the Rust
 *     side (crates/backend/tsearch/ts_locale/src/public.rs classify()) calls
 *     the same libc mbstowcs in the same process — parity by construction.
 */

#include "postgres.h"

#include "tsearch/ts_locale.h"

/* ---- BEGIN VERBATIM ts_locale.c:23-69 ---- */
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

/* ---- END VERBATIM ts_locale.c:23-69 ---- */
