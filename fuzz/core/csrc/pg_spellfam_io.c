/*
 * pg_spellfam_io.c: vendored PostgreSQL C oracle for the spellfam_diff
 * differential fuzz target (100%-coverage campaign, lane p1-spell).
 * Crate under test: crates/backend/tsearch/spell (the ispell/hunspell
 * dictionary loader + normalizer) — see fuzz/core/src/spellfam_diff.rs.
 *
 * Provenance (all bodies VERBATIM sed-extracted from the vendor tree at
 * ~/dev/pgrust-fabled/vendor/postgres-src, Stamp-18.3, upstream sha
 * 62d6c7d3df6287f1bd83199c1a746e50d31571a0 — assembled by
 * scratchpad/assemble_spellfam.sh, never hand-typed):
 *   - src/include/tsearch/ts_public.h lines 114-139 (TSLexeme).
 *   - src/include/tsearch/dicts/regis.h lines 16-48 (RegisNode/Regis +
 *     declarations).
 *   - src/include/tsearch/dicts/spell.h lines 21-240 (SPNodeData/SPNode/
 *     SPELL/AFFIX/CMPDAffix/AffixNodeData/AffixNode/IspellDict + the FF_*
 *     and flag-mode defines).
 *   - src/backend/tsearch/regis.c lines 19-252 (RS_isRegis, RS_compile,
 *     RS_free, RS_execute + internal helpers — the whole functional file).
 *   - src/backend/tsearch/ts_locale.c lines 20-204 (t_isalnum/t_isalpha
 *     families, tsearch_readline_begin/tsearch_readline/
 *     tsearch_readline_end, tsearch_readline_callback — the whole
 *     functional file).
 *   - src/common/pg_get_line.c lines 88-101 (pg_get_line_buf) and 124-180
 *     (pg_get_line_append) — the fgets-loop line reader tsearch_readline
 *     sits on (its embedded-NUL truncation IS the behavior under test).
 *   - src/common/stringinfo.c lines 33-56 (initStringInfoInternal), 91-100
 *     (initStringInfo), 120-134 (resetStringInfo), 325-393
 *     (enlargeStringInfo).
 *   - src/backend/utils/adt/formatting.c lines 1892-1912 (asc_tolower).
 *   - src/backend/tsearch/spell.c lines 72-2604: the ENTIRE functional
 *     file — NIStartBuild/NIFinishBuild, compact_palloc0, cpstrdup,
 *     lowerstr_ctx, the cmp* family, findchar/findchar2, strbcmp/strbncmp,
 *     cmpaffix, getNextFlagFromString, IsAffixFlagInUse, NIAddSpell,
 *     NIImportDictionary, FindWord, NIAddAffix, get_nextfield,
 *     parse_ooaffentry, parse_affentry, setCompoundAffixFlagValue,
 *     addCompoundAffixFlagValue, getCompoundAffixFlagValue,
 *     getAffixFlagSet, NIImportOOAffixes, NIImportAffixes, MergeAffix,
 *     makeCompoundFlags, mkSPNode, NISortDictionary, mkANode, mkVoidAffix,
 *     isAffixInUse, NISortAffixes, FindAffixes, CheckAffix, addToResult,
 *     NormalizeSubWord, CheckCompoundAffixes, CopyVar, AddStem,
 *     SplitToVariants, addNorm, NINormalizeWord.
 *
 * SHIMS (plumbing/environment only, never logic):
 *   - fixed-width typedefs as c.h on LP64; Assert no-op (release parity);
 *     palloc/palloc0/repalloc/pstrdup + MemoryContextAlloc/-Zero -> tracked
 *     malloc arena, freed by pg_spf_reset() per exec (models the dictionary
 *     cache context + buildCxt; MemoryContextSwitchTo/-Delete/
 *     AllocSetContextCreate are no-ops accordingly — the build/runtime
 *     context SPLIT is Rust-side-visible only through ni_finish_build,
 *     which the driver exercises).
 *   - ereport(ERROR)/elog(ERROR) -> record the real MAKE_SQLSTATE word in
 *     TLS + longjmp; driver entries setjmp and return -1. errmsg/errhint
 *     arguments are swallowed unevaluated (message text out of scope).
 *   - AllocateFile/FreeFile -> fopen/fclose (fd.c resource-owner
 *     bookkeeping is server plumbing); error_context_stack -> a plain TLS
 *     pointer cell (elog.c global); errcontext -> 0.
 *   - pg_verify_mbstr / pg_mblen / pg_mblen_cstr / pg_mb2wchar_with_len /
 *     GetDatabaseEncoding: resolved against the VERBATIM wfam_ copies in
 *     pg_wcharfam.c (one verbatim definition per symbol across the fuzz
 *     oracle build); encoding pinned per exec via wfam_x_set_db_encoding,
 *     the Rust side pinning the SAME value through mbutils.
 *   - pg_any_to_server -> the verbatim mbutils.c decision tree RESOLVED
 *     for the two pinned server encodings {UTF8, SQL_ASCII} with source
 *     encoding fixed PG_UTF8 (tsearch_readline's contract): both cells
 *     reduce to pg_verify_mbstr(PG_UTF8) + return input unchanged, with
 *     the verify failure carried on THIS TU's error channel as
 *     ERRCODE_CHARACTER_NOT_IN_REPERTOIRE (report_invalid_encoding's
 *     sqlstate). Calling wfam_pg_verify_mbstr with noError=true keeps the
 *     wcharfam TU's own longjmp channel disarmed (its armed-entry rule).
 *   - str_tolower(_, _, DEFAULT_COLLATION_OID) -> verbatim asc_tolower:
 *     the COLLATION CELL IS PINNED — the driver installs the C locale as
 *     the database default on the Rust side
 *     (pg_locale::set_default_locale_c_for_tests + set_database_ctype_is_c
 *     (true)), under which formatting.c's str_tolower takes exactly its
 *     ctype_is_c => asc_tolower arm. The pg_strlower (non-C locale) arm is
 *     the locale-dependent carve on both sides (exception rows; same carve
 *     the ts_locale crate's claim of record uses).
 *   - DEFAULT_COLLATION_OID is accordingly #defined to C_COLLATION_OID for
 *     the pg_regcomp call site (spell.c line 742): the vendored regexfam
 *     engine resolves C_COLLATION_OID to PG_REGEX_STRATEGY_C without
 *     catalog access, which is the SAME strategy the Rust side reaches via
 *     pg_newlocale_from_collation(DEFAULT)->ctype_is_c under the pin.
 *   - pg_regcomp/pg_regexec: the UNPREFIXED verbatim engine already linked
 *     into this build (csrc/regexfam/, see pg_regexp_io.c provenance).
 *     REG_ADVANCED|REG_NOSUB compile + rm_nmatch=0 exec, exactly spell.c's
 *     usage. pg_regerror is used only for the error MESSAGE (out of scope);
 *     the shim returns a fixed string.
 *   - check_stack_depth() -> no-op on the C side; the driver caps query
 *     words at 300 bytes, under which SplitToVariants' recursion (bounded
 *     by wordlen) stays ~3 orders of magnitude below any real stack limit
 *     on both sides, so neither side's guard can fire (the Rust guard is
 *     live code covered by its own crate).
 *   - Every extern definition is spf_/pg_spf_-prefixed via #define so this
 *     TU cannot collide with other oracle TUs in the fuzz cc build.
 *
 * Driver entries (SECTION D, pg_spf_* prefix) are fuzz plumbing, NOT
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
#include <setjmp.h>
#include <wchar.h>
#include <wctype.h>

typedef int8_t int8;
typedef int16_t int16;
typedef int32_t int32;
typedef int64_t int64;
typedef uint8_t uint8;
typedef uint16_t uint16;
typedef uint32_t uint32;
typedef uint64_t uint64;
typedef size_t Size;
typedef unsigned int Oid;
typedef unsigned int pg_wchar;	/* mb/pg_wchar.h */

#define Assert(x) ((void) 0)
#define AssertMacro(x) ((void) 0)
#define FLEXIBLE_ARRAY_MEMBER	/* empty */
#define pg_attribute_unused() __attribute__((unused))
#define pg_nodiscard
#define lengthof(array) (sizeof (array) / sizeof ((array)[0]))
#define Min(x, y) ((x) < (y) ? (x) : (y))
#define Max(x, y) ((x) > (y) ? (x) : (y))
#define CHECK_FOR_INTERRUPTS() ((void) 0)
/* check_stack_depth: FAITHFUL, not a no-op — body after the error shims below
 * (it needs spf_errcode/spf_raise/MAKE_SQLSTATE). Rationale: spell.c's
 * SplitToVariants recurses per compound split and PostgreSQL guards it with a
 * real check_stack_depth() that ereports ERRCODE_STATEMENT_TOO_COMPLEX
 * ("stack depth limit exceeded", utils/misc/stack_depth.c:94-105). Stubbing it
 * to a no-op gave the C side NO guard while the pgrust port has a live one, so a
 * deep-compound input crashed the ORACLE with an ASan stack-overflow instead of
 * erroring (fleet exec 4080). */
static void spf_check_stack_depth(void);
#define check_stack_depth() spf_check_stack_depth()
#define unlikely(x) __builtin_expect((x) != 0, 0)
#define likely(x) __builtin_expect((x) != 0, 1)
/* c.h lines 1126-1127 @ 62d6c7d3df */
#define HIGHBIT				(0x80)
#define IS_HIGHBIT_SET(ch)	((unsigned char)(ch) & HIGHBIT)
#define TOUCHAR(x)	(*((const unsigned char *) (x)))
/* pg_wchar.h enum pg_enc values referenced by the pasted bodies */
#define PG_SQL_ASCII 0
#define PG_UTF8 6
/* pg_collation.h: the collation-cell pin (see file header) */
#define C_COLLATION_OID 950
#define DEFAULT_COLLATION_OID C_COLLATION_OID
/* memutils.h ceiling, verbatim value */
#define MaxAllocSize	((Size) 0x3fffffff)
#define MAXIMUM_ALIGNOF 8
/* c.h lines 773-774, 780 @ 62d6c7d3df */
#define TYPEALIGN(ALIGNVAL,LEN)  \
	(((uintptr_t) (LEN) + ((ALIGNVAL) - 1)) & ~((uintptr_t) ((ALIGNVAL) - 1)))
#define MAXALIGN(LEN)			TYPEALIGN(MAXIMUM_ALIGNOF, (LEN))

/* ---- error protocol shims (see file header) ---- */
static _Thread_local jmp_buf spf_env;
static _Thread_local int spf_errcode;

/* verbatim MAKE_SQLSTATE encoding from src/include/utils/elog.h */
#define PGSIXBIT(ch)	(((ch) - '0') & 0x3f)
#define MAKE_SQLSTATE(ch1,ch2,ch3,ch4,ch5)	\
	(PGSIXBIT(ch1) + (PGSIXBIT(ch2) << 6) + (PGSIXBIT(ch3) << 12) + \
	 (PGSIXBIT(ch4) << 18) + (PGSIXBIT(ch5) << 24))
#define ERRCODE_INTERNAL_ERROR				MAKE_SQLSTATE('X','X','0','0','0')
#define ERRCODE_CHARACTER_NOT_IN_REPERTOIRE MAKE_SQLSTATE('2','2','0','2','1')
#define ERRCODE_OUT_OF_MEMORY				MAKE_SQLSTATE('5','3','2','0','0')
#define ERRCODE_CONFIG_FILE_ERROR			MAKE_SQLSTATE('F','0','0','0','0')
#define ERRCODE_INVALID_REGULAR_EXPRESSION	MAKE_SQLSTATE('2','2','0','1','B')
#define ERRCODE_PROGRAM_LIMIT_EXCEEDED		MAKE_SQLSTATE('5','4','0','0','0')

static void
spf_raise(void)
{
	longjmp(spf_env, 1);
}

/* Mirrors stack_is_too_deep(): distance from a base captured at driver entry to
 * a local, abs value, against a limit. The limit is a HARNESS value, not PG's
 * max_stack_depth GUC, so the exact THRESHOLD is a documented non-surface — the
 * driver carves any exec where either side reports the depth sqlstate. */
#define ERRCODE_STATEMENT_TOO_COMPLEX	MAKE_SQLSTATE('5','4','0','0','1')
static _Thread_local char *spf_stack_base;
#define SPF_MAX_STACK_BYTES (1024L * 1024L)	/* well under the smallest thread stack */

static void
spf_check_stack_depth(void)
{
	char		here;
	long		depth;

	if (spf_stack_base == NULL)
		return;					/* base not armed: no guard */
	depth = (long) (spf_stack_base - &here);
	if (depth < 0)
		depth = -depth;
	if (depth > SPF_MAX_STACK_BYTES)
	{
		spf_errcode = ERRCODE_STATEMENT_TOO_COMPLEX;
		spf_raise();
	}
}

#define errcode(c) (spf_errcode = (c), 0)
#define errmsg(...) 0
#define errmsg_internal(...) 0
#define errdetail(...) 0
#define errhint(...) 0
#define errcontext(...) 0
#define ereport(level, ...) do { (void) (__VA_ARGS__); spf_raise(); } while (0)
#define elog(level, ...) \
	do { spf_errcode = ERRCODE_INTERNAL_ERROR; spf_raise(); } while (0)

/* ---- palloc arena (models dictionary-cache context + buildCxt) ---- */
static _Thread_local void **spf_allocs;
static _Thread_local size_t spf_nallocs, spf_aallocs;

/* Size-tracked arena cell: 16-byte header keeps MAXALIGN and lets
 * repalloc copy the exact old size (chunk-header plumbing, not logic). */
static void *
spf_palloc(Size sz)
{
	char	   *p;

	if (sz > MaxAllocSize)
	{
		/* palloc's "invalid memory alloc request size" is elog-class */
		spf_errcode = ERRCODE_INTERNAL_ERROR;
		spf_raise();
	}
	p = malloc(sz + 16);
	if (p == NULL)
	{
		spf_errcode = ERRCODE_OUT_OF_MEMORY;
		spf_raise();
	}
	*(Size *) p = sz;
	if (spf_nallocs == spf_aallocs)
	{
		spf_aallocs = spf_aallocs ? spf_aallocs * 2 : 4096;
		spf_allocs = realloc(spf_allocs, spf_aallocs * sizeof(void *));
	}
	spf_allocs[spf_nallocs++] = p;
	return p + 16;
}

static void *
spf_repalloc(void *ptr, Size sz)
{
	Size		oldsz = *(Size *) ((char *) ptr - 16);
	void	   *p = spf_palloc(sz);

	memcpy(p, ptr, oldsz < sz ? oldsz : sz);
	return p;
}

static void *
spf_palloc0(Size sz)
{
	void	   *p = spf_palloc(sz);

	memset(p, 0, sz);
	return p;
}

static char *
spf_pstrdup(const char *s)
{
	size_t		n = strlen(s) + 1;
	char	   *r = spf_palloc(n);

	memcpy(r, s, n);
	return r;
}

#define palloc(n) spf_palloc(n)
#define palloc0(n) spf_palloc0(n)
#define pstrdup(s) spf_pstrdup(s)
#define repalloc(p, n) spf_repalloc((p), (n))
#define pfree(p) ((void) (p))	/* arena-freed at pg_spf_reset */

/* MemoryContext plumbing: one arena for everything (see file header) */
typedef void *MemoryContext;
static _Thread_local char spf_cxt_dummy;
#define CurrentMemoryContext ((MemoryContext) &spf_cxt_dummy)
#define MemoryContextAlloc(cxt, sz) ((void) (cxt), spf_palloc(sz))
#define MemoryContextAllocZero(cxt, sz) ((void) (cxt), spf_palloc0(sz))
#define MemoryContextSwitchTo(cxt) ((void) (cxt), (MemoryContext) &spf_cxt_dummy)
#define MemoryContextDelete(cxt) ((void) (cxt))
#define AllocSetContextCreate(parent, name, ...) ((void) (parent), (MemoryContext) &spf_cxt_dummy)
#define CurTransactionContext ((MemoryContext) &spf_cxt_dummy)
#define ALLOCSET_DEFAULT_SIZES 0

/* fd.c plumbing — WITH the resource-owner cleanup fd.c actually provides.
 * spell.c's error paths ereport (longjmp) straight past tsearch_readline_end,
 * so a bare fopen/fclose pair LEAKS the FILE* on every failed import. Real
 * PostgreSQL does not leak: AllocateFile registers the fd with fd.c, which
 * closes it during transaction abort — the bookkeeping originally shimmed away
 * as "server plumbing". Without it a floor run dies of EMFILE after ~1k errored
 * builds (observed: "Too many open files" at exec 806107). Track them and close
 * any stragglers in pg_spf_reset(), which is this oracle's abort boundary. */
#define SPF_MAX_FILES 64
static _Thread_local FILE *spf_files[SPF_MAX_FILES];

static FILE *
spf_allocate_file(const char *name, const char *mode)
{
	FILE	   *fp = fopen(name, mode);
	int			i;

	if (fp == NULL)
		return NULL;
	for (i = 0; i < SPF_MAX_FILES; i++)
	{
		if (spf_files[i] == NULL)
		{
			spf_files[i] = fp;
			return fp;
		}
	}
	/* Table full: legitimate use holds ONE file at a time, so a full table
	 * means FILE*s are accumulating across execs — i.e. the fd.c abort-time
	 * cleanup regressed. Fail LOUDLY. Returning NULL here instead would cap
	 * the leak silently, spuriously report "could not open affix file", and
	 * make the fd_leak_control test vacuous (it did exactly that once). */
	fprintf(stderr,
			"spellfam oracle: AllocateFile table full (%d) — fd cleanup regressed\n",
			SPF_MAX_FILES);
	fclose(fp);
	abort();
}

static int
spf_free_file(FILE *fp)
{
	int			i;

	for (i = 0; i < SPF_MAX_FILES; i++)
		if (spf_files[i] == fp)
			spf_files[i] = NULL;
	return fclose(fp);
}

static void
spf_close_all_files(void)
{
	int			i;

	for (i = 0; i < SPF_MAX_FILES; i++)
	{
		if (spf_files[i] != NULL)
		{
			fclose(spf_files[i]);
			spf_files[i] = NULL;
		}
	}
}

#define AllocateFile(name, mode) spf_allocate_file((name), (mode))
#define FreeFile(fp) spf_free_file(fp)

/* elog.c error-context plumbing (tsearch_readline arms it, we ignore it) */
typedef struct ErrorContextCallback
{
	struct ErrorContextCallback *previous;
	void		(*callback) (void *arg);
	void	   *arg;
} ErrorContextCallback;

static _Thread_local ErrorContextCallback *error_context_stack;

/* ---- wcharfam-resolved mb layer (verbatim wfam_ copies) ---- */
extern int	wfam_pg_mblen(const char *mbstr);
extern int	wfam_pg_mblen_cstr(const char *mbstr);
extern int	wfam_pg_mb2wchar_with_len(const char *from, pg_wchar *to, int len);
extern int	wfam_GetDatabaseEncoding(void);
extern bool wfam_pg_verify_mbstr(int encoding, const char *mbstr, int len,
								 bool noError);
extern void wfam_x_set_db_encoding(int encoding);

#define pg_mblen			wfam_pg_mblen
#define pg_mblen_cstr		wfam_pg_mblen_cstr
#define pg_mb2wchar_with_len wfam_pg_mb2wchar_with_len
#define GetDatabaseEncoding wfam_GetDatabaseEncoding

/* mbutils.c pg_any_to_server, resolved for the pinned cells (file header) */
static char *
spf_pg_any_to_server(const char *s, int len, int encoding)
{
	if (len <= 0)
		return (char *) s;
	/* server is pinned to UTF8 or SQL_ASCII; encoding is always PG_UTF8:
	 * both mbutils.c cells reduce to validate-as-UTF8, return unchanged */
	if (!wfam_pg_verify_mbstr(PG_UTF8, s, len, true))
	{
		spf_errcode = ERRCODE_CHARACTER_NOT_IN_REPERTOIRE;
		spf_raise();
	}
	return (char *) s;
}

#define pg_any_to_server(s, len, enc) spf_pg_any_to_server((s), (len), (enc))

/* ---- pg_locale / formatting layer (see file header) ---- */
static _Thread_local bool database_ctype_is_c = true;
typedef struct pg_locale_struct *pg_locale_t;

/* ts_locale.c's wide arm is unreachable under the ctype_is_c pin for
 * 1-byte chars, but multibyte chars DO take it (clen > 1 &&
 * !database_ctype_is_c is false => isalpha path; with the pin TRUE the
 * wide arm is dead). Keep it loud. */
static size_t
char2wchar(wchar_t *to, size_t tolen, const char *from, size_t fromlen,
		   pg_locale_t locale)
{
	(void) to; (void) tolen; (void) from; (void) fromlen; (void) locale;
	fprintf(stderr, "spellfam oracle: char2wchar reached under ctype_is_c pin\n");
	abort();
}

/* pg_mblen_with_len / pg_mblen_unbounded for the t_is* bodies */
extern int	wfam_pg_mblen_with_len(const char *mbstr, int limit);
extern int	wfam_pg_mblen_unbounded(const char *mbstr);
#define pg_mblen_with_len	wfam_pg_mblen_with_len
#define pg_mblen_unbounded	wfam_pg_mblen_unbounded

/* ---- regex engine: unprefixed verbatim csrc/regexfam objects ---- */
/* regex/regex.h essentials (verbatim signatures; struct is opaque here —
 * spell.c stores regex_t by value inside AFFIX, so we need the real
 * definition: include the vendored header). */
#define PG_SPELLFAM_WANT_REGEX_H
/* ==== VERBATIM: csrc/regexfam/include/regex/regex.h (the vendored 18.3 engine header) ==== */
#ifndef _PG_REGEX_H_
#define _PG_REGEX_H_			/* never again */
/*
 * regular expressions
 *
 * Copyright (c) 1998, 1999 Henry Spencer.  All rights reserved.
 *
 * Development of this software was funded, in part, by Cray Research Inc.,
 * UUNET Communications Services Inc., Sun Microsystems Inc., and Scriptics
 * Corporation, none of whom are responsible for the results.  The author
 * thanks all of them.
 *
 * Redistribution and use in source and binary forms -- with or without
 * modification -- are permitted for any purpose, provided that
 * redistributions in source form retain this entire copyright notice and
 * indicate the origin and nature of any modifications.
 *
 * I'd appreciate being given credit for this package in the documentation
 * of software which uses it, but that is not a requirement.
 *
 * THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESS OR IMPLIED WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL
 * HENRY SPENCER BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
 * OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
 * ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * src/include/regex/regex.h
 */

/*
 * This is an implementation of POSIX regex_t, so it clashes with the
 * system-provided <regex.h> header.  That header might be unintentionally
 * included already, so we force that to happen now on all systems to show that
 * we can cope and that we completely replace the system regex interfaces.
 *
 * Note that we avoided using _REGEX_H_ as an include guard, as that confuses
 * matters on BSD family systems including macOS that use the same include
 * guard.
 */
#ifndef _WIN32
#endif

/* Avoid redefinition errors due to the system header. */
#undef REG_UBACKREF
#undef REG_ULOOKAROUND
#undef REG_UBOUNDS
#undef REG_UBRACES
#undef REG_UBSALNUM
#undef REG_UPBOTCH
#undef REG_UBBS
#undef REG_UNONPOSIX
#undef REG_UUNSPEC
#undef REG_UUNPORT
#undef REG_ULOCALE
#undef REG_UEMPTYMATCH
#undef REG_UIMPOSSIBLE
#undef REG_USHORTEST
#undef REG_BASIC
#undef REG_EXTENDED
#undef REG_ADVF
#undef REG_ADVANCED
#undef REG_QUOTE
#undef REG_NOSPEC
#undef REG_ICASE
#undef REG_NOSUB
#undef REG_EXPANDED
#undef REG_NLSTOP
#undef REG_NLANCH
#undef REG_NEWLINE
#undef REG_PEND
#undef REG_EXPECT
#undef REG_BOSONLY
#undef REG_DUMP
#undef REG_FAKE
#undef REG_PROGRESS
#undef REG_NOTBOL
#undef REG_NOTEOL
#undef REG_STARTEND
#undef REG_FTRACE
#undef REG_MTRACE
#undef REG_SMALL
#undef REG_OKAY
#undef REG_NOMATCH
#undef REG_BADPAT
#undef REG_ECOLLATE
#undef REG_ECTYPE
#undef REG_EESCAPE
#undef REG_ESUBREG
#undef REG_EBRACK
#undef REG_EPAREN
#undef REG_EBRACE
#undef REG_BADBR
#undef REG_ERANGE
#undef REG_ESPACE
#undef REG_BADRPT
#undef REG_ASSERT
#undef REG_INVARG
#undef REG_MIXED
#undef REG_BADOPT
#undef REG_ETOOBIG
#undef REG_ECOLORS
#undef REG_ATOI
#undef REG_ITOA
#undef REG_PREFIX
#undef REG_EXACT

/*
 * Add your own defines, if needed, here.
 */

/*
 * interface types etc.
 */

/*
 * regoff_t has to be large enough to hold either off_t or ssize_t,
 * and must be signed; it's only a guess that long is suitable.
 */
typedef long pg_regoff_t;

/*
 * other interface types
 */

/* the biggie, a compiled RE (or rather, a front end to same) */
typedef struct
{
	int			re_magic;		/* magic number */
	size_t		re_nsub;		/* number of subexpressions */
	long		re_info;		/* bitmask of the following flags: */
#define  REG_UBACKREF		000001	/* has back-reference (\n) */
#define  REG_ULOOKAROUND	000002	/* has lookahead/lookbehind constraint */
#define  REG_UBOUNDS		000004	/* has bounded quantifier ({m,n}) */
#define  REG_UBRACES		000010	/* has { that doesn't begin a quantifier */
#define  REG_UBSALNUM		000020	/* has backslash-alphanumeric in non-ARE */
#define  REG_UPBOTCH		000040	/* has unmatched right paren in ERE (legal
									 * per spec, but that was a mistake) */
#define  REG_UBBS			000100	/* has backslash within bracket expr */
#define  REG_UNONPOSIX		000200	/* has any construct that extends POSIX */
#define  REG_UUNSPEC		000400	/* has any case disallowed by POSIX, e.g.
									 * an empty branch */
#define  REG_UUNPORT		001000	/* has numeric character code dependency */
#define  REG_ULOCALE		002000	/* has locale dependency */
#define  REG_UEMPTYMATCH	004000	/* can match a zero-length string */
#define  REG_UIMPOSSIBLE	010000	/* provably cannot match anything */
#define  REG_USHORTEST		020000	/* has non-greedy quantifier */
	int			re_csize;		/* sizeof(character) */
	char	   *re_endp;		/* backward compatibility kludge */
	Oid			re_collation;	/* Collation that defines LC_CTYPE behavior */
	/* the rest is opaque pointers to hidden innards */
	char	   *re_guts;		/* `char *' is more portable than `void *' */
	char	   *re_fns;
} pg_regex_t;

/* result reporting (may acquire more fields later) */
typedef struct
{
	pg_regoff_t rm_so;			/* start of substring */
	pg_regoff_t rm_eo;			/* end of substring */
} pg_regmatch_t;

/* supplementary control and reporting */
typedef struct
{
	pg_regmatch_t rm_extend;	/* see REG_EXPECT */
} rm_detail_t;



/*
 * regex compilation flags
 */
#define REG_BASIC	000000		/* BREs (convenience) */
#define REG_EXTENDED	000001	/* EREs */
#define REG_ADVF	000002		/* advanced features in EREs */
#define REG_ADVANCED	000003	/* AREs (which are also EREs) */
#define REG_QUOTE	000004		/* no special characters, none */
#define REG_NOSPEC	REG_QUOTE	/* historical synonym */
#define REG_ICASE	000010		/* ignore case */
#define REG_NOSUB	000020		/* caller doesn't need subexpr match data */
#define REG_EXPANDED	000040	/* expanded format, white space & comments */
#define REG_NLSTOP	000100		/* \n doesn't match . or [^ ] */
#define REG_NLANCH	000200		/* ^ matches after \n, $ before */
#define REG_NEWLINE 000300		/* newlines are line terminators */
#define REG_PEND	000400		/* ugh -- backward-compatibility hack */
#define REG_EXPECT	001000		/* report details on partial/limited matches */
#define REG_BOSONLY 002000		/* temporary kludge for BOS-only matches */
#define REG_DUMP	004000		/* none of your business :-) */
#define REG_FAKE	010000		/* none of your business :-) */
#define REG_PROGRESS	020000	/* none of your business :-) */



/*
 * regex execution flags
 */
#define REG_NOTBOL	0001		/* BOS is not BOL */
#define REG_NOTEOL	0002		/* EOS is not EOL */
#define REG_STARTEND	0004	/* backward compatibility kludge */
#define REG_FTRACE	0010		/* none of your business */
#define REG_MTRACE	0020		/* none of your business */
#define REG_SMALL	0040		/* none of your business */


/*
 * error reporting
 * Be careful if modifying the list of error codes -- the table used by
 * regerror() is generated automatically from this file!
 */
#define REG_OKAY	 0			/* no errors detected */
#define REG_NOMATCH  1			/* failed to match */
#define REG_BADPAT	 2			/* invalid regexp */
#define REG_ECOLLATE	 3		/* invalid collating element */
#define REG_ECTYPE	 4			/* invalid character class */
#define REG_EESCAPE  5			/* invalid escape \ sequence */
#define REG_ESUBREG  6			/* invalid backreference number */
#define REG_EBRACK	 7			/* brackets [] not balanced */
#define REG_EPAREN	 8			/* parentheses () not balanced */
#define REG_EBRACE	 9			/* braces {} not balanced */
#define REG_BADBR	10			/* invalid repetition count(s) */
#define REG_ERANGE	11			/* invalid character range */
#define REG_ESPACE	12			/* out of memory */
#define REG_BADRPT	13			/* quantifier operand invalid */
#define REG_ASSERT	15			/* "can't happen" -- you found a bug */
#define REG_INVARG	16			/* invalid argument to regex function */
#define REG_MIXED	17			/* character widths of regex and string differ */
#define REG_BADOPT	18			/* invalid embedded option */
#define REG_ETOOBIG 19			/* regular expression is too complex */
#define REG_ECOLORS 20			/* too many colors */
/* two specials for debugging and testing */
#define REG_ATOI	101			/* convert error-code name to number */
#define REG_ITOA	102			/* convert error-code number to name */
/* non-error result codes for pg_regprefix */
#define REG_PREFIX	(-1)		/* identified a common prefix */
#define REG_EXACT	(-2)		/* identified an exact match */


/* Redirect the standard typenames to our typenames. */
#define regoff_t pg_regoff_t
#define regex_t pg_regex_t
#define regmatch_t pg_regmatch_t


/*
 * the prototypes for exported functions
 */

/* regcomp.c */
extern int	pg_regcomp(regex_t *re, const pg_wchar *string, size_t len,
					   int flags, Oid collation);
extern int	pg_regexec(regex_t *re, const pg_wchar *string, size_t len,
					   size_t search_start, rm_detail_t *details,
					   size_t nmatch, regmatch_t pmatch[], int flags);
extern int	pg_regprefix(regex_t *re, pg_wchar **string, size_t *slength);
extern void pg_regfree(regex_t *re);
extern size_t pg_regerror(int errcode, const regex_t *preg, char *errbuf,
						  size_t errbuf_size);

/* regexp.c */

#endif							/* _PG_REGEX_H_ */

/* pg_regerror is message-only in spell.c (out of scope) */
#define pg_regerror(err, re, buf, siz) \
	((void) (err), (void) (re), snprintf((buf), (siz), "regex error"))

/* ==== symbol prefixing: every extern definition in this TU ==== */
#define t_isalnum_with_len		spf_t_isalnum_with_len
#define t_isalnum_cstr			spf_t_isalnum_cstr
#define t_isalnum_unbounded		spf_t_isalnum_unbounded
#define t_isalnum				spf_t_isalnum
#define t_isalpha_with_len		spf_t_isalpha_with_len
#define t_isalpha_cstr			spf_t_isalpha_cstr
#define t_isalpha_unbounded		spf_t_isalpha_unbounded
#define t_isalpha				spf_t_isalpha
#define tsearch_readline_begin	spf_tsearch_readline_begin
#define tsearch_readline		spf_tsearch_readline
#define tsearch_readline_end	spf_tsearch_readline_end
#define pg_get_line_buf			spf_pg_get_line_buf
#define pg_get_line_append		spf_pg_get_line_append
#define initStringInfoInternal	spf_initStringInfoInternal
#define initStringInfo			spf_initStringInfo
#define resetStringInfo			spf_resetStringInfo
#define enlargeStringInfo		spf_enlargeStringInfo
#define asc_tolower				spf_asc_tolower
#define RS_isRegis				spf_RS_isRegis
#define RS_compile				spf_RS_compile
#define RS_free					spf_RS_free
#define RS_execute				spf_RS_execute
#define NIStartBuild			spf_NIStartBuild
#define NIImportDictionary		spf_NIImportDictionary
#define NIImportAffixes			spf_NIImportAffixes
#define NISortDictionary		spf_NISortDictionary
#define NISortAffixes			spf_NISortAffixes
#define NIFinishBuild			spf_NIFinishBuild
#define NINormalizeWord			spf_NINormalizeWord

/* formatting.h face used by lowerstr_ctx */
static char *asc_tolower(const char *buff, size_t nbytes);
#define str_tolower(buff, nbytes, collid) ((void) (collid), asc_tolower((buff), (nbytes)))

/* common/string.h / pg_get_line plumbing */
typedef struct PromptInterruptContext
{
	void	   *jmpbuf;
	volatile bool *enabled;
	bool		canceled;
} PromptInterruptContext;

/* lib/stringinfo.h struct + declarations (verbatim layout) */
typedef struct StringInfoData
{
	char	   *data;
	int			len;
	int			maxlen;
	int			cursor;
} StringInfoData;
typedef StringInfoData *StringInfo;
#define STRINGINFO_DEFAULT_SIZE 1024

static void initStringInfoInternal(StringInfo str, int initsize);
static void initStringInfo(StringInfo str);
static void resetStringInfo(StringInfo str);
static void enlargeStringInfo(StringInfo str, int needed);
static bool pg_get_line_buf(FILE *stream, StringInfo buf);
static bool pg_get_line_append(FILE *stream, StringInfo buf,
							   PromptInterruptContext *prompt_ctx);


/* ==== VERBATIM: ts_public.h lines 114-139 (TSLexeme) @ 62d6c7d3df ==== */
/* return struct for any lexize function */
typedef struct
{
	/*----------
	 * Number of current variant of split word.  For example the Norwegian
	 * word 'fotballklubber' has two variants to split: ( fotball, klubb )
	 * and ( fot, ball, klubb ). So, dictionary should return:
	 *
	 * nvariant    lexeme
	 *	   1	   fotball
	 *	   1	   klubb
	 *	   2	   fot
	 *	   2	   ball
	 *	   2	   klubb
	 *
	 * In general, a TSLexeme will be considered to belong to the same split
	 * variant as the previous one if they have the same nvariant value.
	 * The exact values don't matter, only changes from one lexeme to next.
	 *----------
	 */
	uint16		nvariant;

	uint16		flags;			/* See flag bits below */

	char	   *lexeme;			/* C string */
} TSLexeme;
/* ==== VERBATIM: tsearch/dicts/regis.h lines 16-48 @ 62d6c7d3df ==== */

typedef struct RegisNode
{
	uint32
				type:2,
				len:16,
				unused:14;
	struct RegisNode *next;
	unsigned char data[FLEXIBLE_ARRAY_MEMBER];
} RegisNode;

#define  RNHDRSZ	(offsetof(RegisNode,data))

#define RSF_ONEOF	1
#define RSF_NONEOF	2

typedef struct Regis
{
	RegisNode  *node;
	uint32
				issuffix:1,
				nchar:16,
				unused:15;
} Regis;

extern bool RS_isRegis(const char *str);

extern void RS_compile(Regis *r, bool issuffix, const char *str);
extern void RS_free(Regis *r);

/*returns true if matches */
extern bool RS_execute(Regis *r, char *str);

/* ==== VERBATIM: tsearch/dicts/spell.h lines 21-240 @ 62d6c7d3df ==== */
/*
 * SPNode and SPNodeData are used to represent prefix tree (Trie) to store
 * a words list.
 */
struct SPNode;

typedef struct
{
	uint32		val:8,
				isword:1,
	/* Stores compound flags listed below */
				compoundflag:4,
	/* Reference to an entry of the AffixData field */
				affix:19;
	struct SPNode *node;
} SPNodeData;

/*
 * Names of FF_ are correlated with Hunspell options in affix file
 * https://hunspell.github.io/
 */
#define FF_COMPOUNDONLY		0x01
#define FF_COMPOUNDBEGIN	0x02
#define FF_COMPOUNDMIDDLE	0x04
#define FF_COMPOUNDLAST		0x08
#define FF_COMPOUNDFLAG		( FF_COMPOUNDBEGIN | FF_COMPOUNDMIDDLE | \
							FF_COMPOUNDLAST )
#define FF_COMPOUNDFLAGMASK		0x0f

typedef struct SPNode
{
	uint32		length;
	SPNodeData	data[FLEXIBLE_ARRAY_MEMBER];
} SPNode;

#define SPNHDRSZ	(offsetof(SPNode,data))

/*
 * Represents an entry in a words list.
 */
typedef struct spell_struct
{
	union
	{
		/*
		 * flag is filled in by NIImportDictionary(). After
		 * NISortDictionary(), d is used instead of flag.
		 */
		const char *flag;
		/* d is used in mkSPNode() */
		struct
		{
			/* Reference to an entry of the AffixData field */
			int			affix;
			/* Length of the word */
			int			len;
		}			d;
	}			p;
	char		word[FLEXIBLE_ARRAY_MEMBER];
} SPELL;

#define SPELLHDRSZ	(offsetof(SPELL, word))

/*
 * Represents an entry in an affix list.
 */
typedef struct aff_struct
{
	const char *flag;
	/* FF_SUFFIX or FF_PREFIX */
	uint32		type:1,
				flagflags:7,
				issimple:1,
				isregis:1,
				replen:14;
	const char *find;
	const char *repl;
	union
	{
		/*
		 * Arrays of AFFIX are moved and sorted.  We'll use a pointer to
		 * regex_t to keep this struct small, and avoid assuming that regex_t
		 * is movable.
		 */
		regex_t    *pregex;
		Regis		regis;
	}			reg;
} AFFIX;

/*
 * affixes use dictionary flags too
 */
#define FF_COMPOUNDPERMITFLAG	0x10
#define FF_COMPOUNDFORBIDFLAG	0x20
#define FF_CROSSPRODUCT			0x40

/*
 * Don't change the order of these. Initialization sorts by these,
 * and expects prefixes to come first after sorting.
 */
#define FF_SUFFIX				1
#define FF_PREFIX				0

/*
 * AffixNode and AffixNodeData are used to represent prefix tree (Trie) to store
 * an affix list.
 */
struct AffixNode;

typedef struct
{
	uint32		val:8,
				naff:24;
	AFFIX	  **aff;
	struct AffixNode *node;
} AffixNodeData;

typedef struct AffixNode
{
	uint32		isvoid:1,
				length:31;
	AffixNodeData data[FLEXIBLE_ARRAY_MEMBER];
} AffixNode;

#define ANHRDSZ		   (offsetof(AffixNode, data))

typedef struct
{
	const char *affix;
	int			len;
	bool		issuffix;
} CMPDAffix;

/*
 * Type of encoding affix flags in Hunspell dictionaries
 */
typedef enum
{
	FM_CHAR,					/* one character (like ispell) */
	FM_LONG,					/* two characters */
	FM_NUM,						/* number, >= 0 and < 65536 */
} FlagMode;

/*
 * Structure to store Hunspell options. Flag representation depends on flag
 * type. These flags are about support of compound words.
 */
typedef struct CompoundAffixFlag
{
	union
	{
		/* Flag name if flagMode is FM_CHAR or FM_LONG */
		const char *s;
		/* Flag name if flagMode is FM_NUM */
		uint32		i;
	}			flag;
	/* we don't have a bsearch_arg version, so, copy FlagMode */
	FlagMode	flagMode;
	uint32		value;
} CompoundAffixFlag;

#define FLAGNUM_MAXSIZE		(1 << 16)

typedef struct
{
	int			maffixes;
	int			naffixes;
	AFFIX	   *Affix;

	AffixNode  *Suffix;
	AffixNode  *Prefix;

	SPNode	   *Dictionary;
	/* Array of sets of affixes */
	const char **AffixData;
	int			lenAffixData;
	int			nAffixData;
	bool		useFlagAliases;

	CMPDAffix  *CompoundAffix;

	bool		usecompound;
	FlagMode	flagMode;

	/*
	 * All follow fields are actually needed only for initialization
	 */

	/* Array of Hunspell options in affix file */
	CompoundAffixFlag *CompoundAffixFlags;
	/* number of entries in CompoundAffixFlags array */
	int			nCompoundAffixFlag;
	/* allocated length of CompoundAffixFlags array */
	int			mCompoundAffixFlag;

	/*
	 * Remaining fields are only used during dictionary construction; they are
	 * set up by NIStartBuild and cleared by NIFinishBuild.
	 */
	MemoryContext buildCxt;		/* temp context for construction */

	/* Temporary array of all words in the dict file */
	SPELL	  **Spell;
	int			nspell;			/* number of valid entries in Spell array */
	int			mspell;			/* allocated length of Spell array */

	/* These are used to allocate "compact" data without palloc overhead */
	char	   *firstfree;		/* first free address (always maxaligned) */
	size_t		avail;			/* free space remaining at firstfree */
} IspellDict;

extern TSLexeme *NINormalizeWord(IspellDict *Conf, const char *word);

extern void NIStartBuild(IspellDict *Conf);
extern void NIImportAffixes(IspellDict *Conf, const char *filename);
extern void NIImportDictionary(IspellDict *Conf, const char *filename);
extern void NISortDictionary(IspellDict *Conf);
extern void NISortAffixes(IspellDict *Conf);
extern void NIFinishBuild(IspellDict *Conf);

/* ==== VERBATIM: tsearch/ts_locale.h lines 24-55 (tsearch_readline_state, t_iseq, ts_copychar/COPYCHAR) @ 62d6c7d3df ==== */
typedef struct
{
	FILE	   *fp;
	const char *filename;
	int			lineno;
	StringInfoData buf;			/* current input line, in UTF-8 */
	char	   *curline;		/* current input line, in DB's encoding */
	/* curline may be NULL, or equal to buf.data, or a palloc'd string */
	ErrorContextCallback cb;
} tsearch_readline_state;

#define TOUCHAR(x)	(*((const unsigned char *) (x)))

/* The second argument of t_iseq() must be a plain ASCII character */
#define t_iseq(x,c)		(TOUCHAR(x) == (unsigned char) (c))

/* Copy multibyte character of known byte length, return byte length. */
static inline int
ts_copychar_with_len(void *dest, const void *src, int length)
{
	memcpy(dest, src, length);
	return length;
}

/* Copy multibyte character from null-terminated string,  return byte length. */
static inline int
ts_copychar_cstr(void *dest, const void *src)
{
	return ts_copychar_with_len(dest, src, pg_mblen_cstr((const char *) src));
}

/* Historical macro for the above. */
/* ==== VERBATIM: common/stringinfo.c 40-56 (initStringInfoInternal), 95-100 (initStringInfo), 125-134 (resetStringInfo), 336-400 (enlargeStringInfo; its one repalloc resolved to the sized arena copy, see shim above) @ 62d6c7d3df ==== */
static inline void
initStringInfoInternal(StringInfo str, int initsize)
{
	Assert(initsize >= 1 && initsize <= MaxAllocSize);

	str->data = (char *) palloc(initsize);
	str->maxlen = initsize;
	resetStringInfo(str);
}

/*
 * makeStringInfoInternal(int initsize)
 *
 * Create an empty 'StringInfoData' & return a pointer to it.
 * The initial memory allocation size is specified by 'initsize'.
 * The valid range for 'initsize' is 1 to MaxAllocSize.
 */
static void
initStringInfo(StringInfo str)
{
	initStringInfoInternal(str, STRINGINFO_DEFAULT_SIZE);
}
static void
resetStringInfo(StringInfo str)
{
	/* don't allow resets of read-only StringInfos */
	Assert(str->maxlen != 0);

	str->data[0] = '\0';
	str->len = 0;
	str->cursor = 0;
}
static void
enlargeStringInfo(StringInfo str, int needed)
{
	int			newlen;

	/* validate this is not a read-only StringInfo */
	Assert(str->maxlen != 0);

	/*
	 * Guard against out-of-range "needed" values.  Without this, we can get
	 * an overflow or infinite loop in the following.
	 */
	if (needed < 0)				/* should not happen */
	{
#ifndef FRONTEND
		elog(ERROR, "invalid string enlargement request size: %d", needed);
#else
		fprintf(stderr, "invalid string enlargement request size: %d\n", needed);
		exit(EXIT_FAILURE);
#endif
	}
	if (((Size) needed) >= (MaxAllocSize - (Size) str->len))
	{
#ifndef FRONTEND
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("string buffer exceeds maximum allowed length (%zu bytes)", MaxAllocSize),
				 errdetail("Cannot enlarge string buffer containing %d bytes by %d more bytes.",
						   str->len, needed)));
#else
		fprintf(stderr,
				_("string buffer exceeds maximum allowed length (%zu bytes)\n\nCannot enlarge string buffer containing %d bytes by %d more bytes.\n"),
				MaxAllocSize, str->len, needed);
		exit(EXIT_FAILURE);
#endif
	}

	needed += str->len + 1;		/* total space required now */

	/* Because of the above test, we now have needed <= MaxAllocSize */

	if (needed <= str->maxlen)
		return;					/* got enough space already */

	/*
	 * We don't want to allocate just a little more space with each append;
	 * for efficiency, double the buffer size each time it overflows.
	 * Actually, we might need to more than double it if 'needed' is big...
	 */
	newlen = 2 * str->maxlen;
	while (needed > newlen)
		newlen = 2 * newlen;

	/*
	 * Clamp to MaxAllocSize in case we went past it.  Note we are assuming
	 * here that MaxAllocSize <= INT_MAX/2, else the above loop could
	 * overflow.  We will still have newlen >= needed.
	 */
	if (newlen > (int) MaxAllocSize)
		newlen = (int) MaxAllocSize;

	str->data = (char *) repalloc(str->data, newlen);

	str->maxlen = newlen;
}
/* ==== VERBATIM: common/pg_get_line.c 94-101, 123-180 @ 62d6c7d3df ==== */
static bool
pg_get_line_buf(FILE *stream, StringInfo buf)
{
	/* We just need to drop any data from the previous call */
	resetStringInfo(buf);
	return pg_get_line_append(stream, buf, NULL);
}

static bool
pg_get_line_append(FILE *stream, StringInfo buf,
				   PromptInterruptContext *prompt_ctx)
{
	int			orig_len = buf->len;

	if (prompt_ctx && sigsetjmp(*((sigjmp_buf *) prompt_ctx->jmpbuf), 1) != 0)
	{
		/* Got here with longjmp */
		prompt_ctx->canceled = true;
		/* Discard any data we collected before detecting error */
		buf->len = orig_len;
		buf->data[orig_len] = '\0';
		return false;
	}

	/* Loop until newline or EOF/error */
	for (;;)
	{
		char	   *res;

		/* Enable longjmp while waiting for input */
		if (prompt_ctx)
			*(prompt_ctx->enabled) = true;

		/* Read some data, appending it to whatever we already have */
		res = fgets(buf->data + buf->len, buf->maxlen - buf->len, stream);

		/* Disable longjmp again, then break if fgets failed */
		if (prompt_ctx)
			*(prompt_ctx->enabled) = false;

		if (res == NULL)
			break;

		/* Got data, so update buf->len */
		buf->len += strlen(buf->data + buf->len);

		/* Done if we have collected a newline */
		if (buf->len > orig_len && buf->data[buf->len - 1] == '\n')
			return true;

		/* Make some more room in the buffer, and loop to read more data */
		enlargeStringInfo(buf, 128);
	}

	/* Check for I/O errors and EOF */
	if (ferror(stream) || buf->len == orig_len)
	{
		/* Discard any data we collected before detecting error */
		buf->len = orig_len;
		buf->data[orig_len] = '\0';
		return false;
	}

	/* No newline at EOF, but we did collect some data */
	return true;
}
/* ==== VERBATIM: port/pgstrcasecmp.c 145-152 (pg_ascii_tolower) @ 62d6c7d3df ==== */
static unsigned char pg_ascii_tolower(unsigned char ch);
static unsigned char
pg_ascii_tolower(unsigned char ch)
{
	if (ch >= 'A' && ch <= 'Z')
		ch += 'a' - 'A';
	return ch;
}
/* ==== VERBATIM: utils/mmgr/mcxt.c 1735-1747 (pnstrdup) @ 62d6c7d3df ==== */
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
/* ==== VERBATIM: utils/adt/formatting.c 1897-1912 (asc_tolower) @ 62d6c7d3df ==== */
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
/* ==== VERBATIM: tsearch/ts_locale.c 20-204 @ 62d6c7d3df ==== */
static void tsearch_readline_callback(void *arg);


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

/*
 * Set up to read a file using tsearch_readline().  This facility is
 * better than just reading the file directly because it provides error
 * context pointing to the specific line where a problem is detected.
 *
 * Expected usage is:
 *
 *		tsearch_readline_state trst;
 *
 *		if (!tsearch_readline_begin(&trst, filename))
 *			ereport(ERROR,
 *					(errcode(ERRCODE_CONFIG_FILE_ERROR),
 *					 errmsg("could not open stop-word file \"%s\": %m",
 *							filename)));
 *		while ((line = tsearch_readline(&trst)) != NULL)
 *			process line;
 *		tsearch_readline_end(&trst);
 *
 * Note that the caller supplies the ereport() for file open failure;
 * this is so that a custom message can be provided.  The filename string
 * passed to tsearch_readline_begin() must remain valid through
 * tsearch_readline_end().
 */
bool
tsearch_readline_begin(tsearch_readline_state *stp,
					   const char *filename)
{
	if ((stp->fp = AllocateFile(filename, "r")) == NULL)
		return false;
	stp->filename = filename;
	stp->lineno = 0;
	initStringInfo(&stp->buf);
	stp->curline = NULL;
	/* Setup error traceback support for ereport() */
	stp->cb.callback = tsearch_readline_callback;
	stp->cb.arg = stp;
	stp->cb.previous = error_context_stack;
	error_context_stack = &stp->cb;
	return true;
}

/*
 * Read the next line from a tsearch data file (expected to be in UTF-8), and
 * convert it to database encoding if needed. The returned string is palloc'd.
 * NULL return means EOF.
 */
char *
tsearch_readline(tsearch_readline_state *stp)
{
	char	   *recoded;

	/* Advance line number to use in error reports */
	stp->lineno++;

	/* Clear curline, it's no longer relevant */
	if (stp->curline)
	{
		if (stp->curline != stp->buf.data)
			pfree(stp->curline);
		stp->curline = NULL;
	}

	/* Collect next line, if there is one */
	if (!pg_get_line_buf(stp->fp, &stp->buf))
		return NULL;

	/* Validate the input as UTF-8, then convert to DB encoding if needed */
	recoded = pg_any_to_server(stp->buf.data, stp->buf.len, PG_UTF8);

	/* Save the correctly-encoded string for possible error reports */
	stp->curline = recoded;		/* might be equal to buf.data */

	/*
	 * We always return a freshly pstrdup'd string.  This is clearly necessary
	 * if pg_any_to_server() returned buf.data, and we need a second copy even
	 * if encoding conversion did occur.  The caller is entitled to pfree the
	 * returned string at any time, which would leave curline pointing to
	 * recycled storage, causing problems if an error occurs after that point.
	 * (It's preferable to return the result of pstrdup instead of the output
	 * of pg_any_to_server, because the conversion result tends to be
	 * over-allocated.  Since callers might save the result string directly
	 * into a long-lived dictionary structure, we don't want it to be a larger
	 * palloc chunk than necessary.  We'll reclaim the conversion result on
	 * the next call.)
	 */
	return pstrdup(recoded);
}

/*
 * Close down after reading a file with tsearch_readline()
 */
void
tsearch_readline_end(tsearch_readline_state *stp)
{
	/* Suppress use of curline in any error reported below */
	if (stp->curline)
	{
		if (stp->curline != stp->buf.data)
			pfree(stp->curline);
		stp->curline = NULL;
	}

	/* Release other resources */
	pfree(stp->buf.data);
	FreeFile(stp->fp);

	/* Pop the error context stack */
	error_context_stack = stp->cb.previous;
}

/*
 * Error context callback for errors occurring while reading a tsearch
 * configuration file.
 */
static void
tsearch_readline_callback(void *arg)
{
	tsearch_readline_state *stp = (tsearch_readline_state *) arg;

	/*
	 * We can't include the text of the config line for errors that occur
	 * during tsearch_readline() itself.  The major cause of such errors is
	 * encoding violations, and we daren't try to print error messages
	 * containing badly-encoded data.
	 */
	if (stp->curline)
		errcontext("line %d of configuration file \"%s\": \"%s\"",
				   stp->lineno,
				   stp->filename,
				   stp->curline);
	else
		errcontext("line %d of configuration file \"%s\"",
				   stp->lineno,
				   stp->filename);
}
/* ==== VERBATIM: tsearch/regis.c 19-252 @ 62d6c7d3df ==== */

#define RS_IN_ONEOF 1
#define RS_IN_ONEOF_IN	2
#define RS_IN_NONEOF	3
#define RS_IN_WAIT	4


/*
 * Test whether a regex is of the subset supported here.
 * Keep this in sync with RS_compile!
 */
bool
RS_isRegis(const char *str)
{
	int			state = RS_IN_WAIT;
	const char *c = str;

	while (*c)
	{
		if (state == RS_IN_WAIT)
		{
			if (t_isalpha_cstr(c))
				 /* okay */ ;
			else if (t_iseq(c, '['))
				state = RS_IN_ONEOF;
			else
				return false;
		}
		else if (state == RS_IN_ONEOF)
		{
			if (t_iseq(c, '^'))
				state = RS_IN_NONEOF;
			else if (t_isalpha_cstr(c))
				state = RS_IN_ONEOF_IN;
			else
				return false;
		}
		else if (state == RS_IN_ONEOF_IN || state == RS_IN_NONEOF)
		{
			if (t_isalpha_cstr(c))
				 /* okay */ ;
			else if (t_iseq(c, ']'))
				state = RS_IN_WAIT;
			else
				return false;
		}
		else
			elog(ERROR, "internal error in RS_isRegis: state %d", state);
		c += pg_mblen_cstr(c);
	}

	return (state == RS_IN_WAIT);
}

static RegisNode *
newRegisNode(RegisNode *prev, int len)
{
	RegisNode  *ptr;

	ptr = (RegisNode *) palloc0(RNHDRSZ + len + 1);
	if (prev)
		prev->next = ptr;
	return ptr;
}

void
RS_compile(Regis *r, bool issuffix, const char *str)
{
	int			len = strlen(str);
	int			state = RS_IN_WAIT;
	const char *c = str;
	RegisNode  *ptr = NULL;

	memset(r, 0, sizeof(Regis));
	r->issuffix = (issuffix) ? 1 : 0;

	while (*c)
	{
		if (state == RS_IN_WAIT)
		{
			if (t_isalpha_cstr(c))
			{
				if (ptr)
					ptr = newRegisNode(ptr, len);
				else
					ptr = r->node = newRegisNode(NULL, len);
				ptr->type = RSF_ONEOF;
				ptr->len = ts_copychar_cstr(ptr->data, c);
			}
			else if (t_iseq(c, '['))
			{
				if (ptr)
					ptr = newRegisNode(ptr, len);
				else
					ptr = r->node = newRegisNode(NULL, len);
				ptr->type = RSF_ONEOF;
				state = RS_IN_ONEOF;
			}
			else				/* shouldn't get here */
				elog(ERROR, "invalid regis pattern: \"%s\"", str);
		}
		else if (state == RS_IN_ONEOF)
		{
			if (t_iseq(c, '^'))
			{
				ptr->type = RSF_NONEOF;
				state = RS_IN_NONEOF;
			}
			else if (t_isalpha_cstr(c))
			{
				ptr->len = ts_copychar_cstr(ptr->data, c);
				state = RS_IN_ONEOF_IN;
			}
			else				/* shouldn't get here */
				elog(ERROR, "invalid regis pattern: \"%s\"", str);
		}
		else if (state == RS_IN_ONEOF_IN || state == RS_IN_NONEOF)
		{
			if (t_isalpha_cstr(c))
				ptr->len += ts_copychar_cstr(ptr->data + ptr->len, c);
			else if (t_iseq(c, ']'))
				state = RS_IN_WAIT;
			else				/* shouldn't get here */
				elog(ERROR, "invalid regis pattern: \"%s\"", str);
		}
		else
			elog(ERROR, "internal error in RS_compile: state %d", state);
		c += pg_mblen_cstr(c);
	}

	if (state != RS_IN_WAIT)	/* shouldn't get here */
		elog(ERROR, "invalid regis pattern: \"%s\"", str);

	ptr = r->node;
	while (ptr)
	{
		r->nchar++;
		ptr = ptr->next;
	}
}

void
RS_free(Regis *r)
{
	RegisNode  *ptr = r->node,
			   *tmp;

	while (ptr)
	{
		tmp = ptr->next;
		pfree(ptr);
		ptr = tmp;
	}

	r->node = NULL;
}

static bool
mb_strchr(char *str, char *c)
{
	int			clen,
				plen,
				i;
	char	   *ptr = str;
	bool		res = false;

	clen = pg_mblen_cstr(c);
	while (*ptr && !res)
	{
		plen = pg_mblen_cstr(ptr);
		if (plen == clen)
		{
			i = plen;
			res = true;
			while (i--)
				if (*(ptr + i) != *(c + i))
				{
					res = false;
					break;
				}
		}

		ptr += plen;
	}

	return res;
}

bool
RS_execute(Regis *r, char *str)
{
	RegisNode  *ptr = r->node;
	char	   *c = str;
	int			len = 0;

	while (*c)
	{
		len++;
		c += pg_mblen_cstr(c);
	}

	if (len < r->nchar)
		return 0;

	c = str;
	if (r->issuffix)
	{
		len -= r->nchar;
		while (len-- > 0)
			c += pg_mblen_cstr(c);
	}


	while (ptr)
	{
		switch (ptr->type)
		{
			case RSF_ONEOF:
				if (!mb_strchr((char *) ptr->data, c))
					return false;
				break;
			case RSF_NONEOF:
				if (mb_strchr((char *) ptr->data, c))
					return false;
				break;
			default:
				elog(ERROR, "unrecognized regis node type: %d", ptr->type);
		}
		ptr = ptr->next;
		c += pg_mblen_cstr(c);
	}

	return true;
}
/* ==== VERBATIM: tsearch/spell.c 72-2604 @ 62d6c7d3df ==== */

/*
 * Initialization requires a lot of memory that's not needed
 * after the initialization is done.  During initialization,
 * CurrentMemoryContext is the long-lived memory context associated
 * with the dictionary cache entry.  We keep the short-lived stuff
 * in the Conf->buildCxt context.
 */
#define tmpalloc(sz)  MemoryContextAlloc(Conf->buildCxt, (sz))
#define tmpalloc0(sz)  MemoryContextAllocZero(Conf->buildCxt, (sz))

/*
 * Prepare for constructing an ISpell dictionary.
 *
 * The IspellDict struct is assumed to be zeroed when allocated.
 */
void
NIStartBuild(IspellDict *Conf)
{
	/*
	 * The temp context is a child of CurTransactionContext, so that it will
	 * go away automatically on error.
	 */
	Conf->buildCxt = AllocSetContextCreate(CurTransactionContext,
										   "Ispell dictionary init context",
										   ALLOCSET_DEFAULT_SIZES);
}

/*
 * Clean up when dictionary construction is complete.
 */
void
NIFinishBuild(IspellDict *Conf)
{
	/* Release no-longer-needed temp memory */
	MemoryContextDelete(Conf->buildCxt);
	/* Just for cleanliness, zero the now-dangling pointers */
	Conf->buildCxt = NULL;
	Conf->Spell = NULL;
	Conf->firstfree = NULL;
	Conf->CompoundAffixFlags = NULL;
}


/*
 * "Compact" palloc: allocate without extra palloc overhead.
 *
 * Since we have no need to free the ispell data items individually, there's
 * not much value in the per-chunk overhead normally consumed by palloc.
 * Getting rid of it is helpful since ispell can allocate a lot of small nodes.
 *
 * We currently pre-zero all data allocated this way, even though some of it
 * doesn't need that.  The cpalloc and cpalloc0 macros are just documentation
 * to indicate which allocations actually require zeroing.
 */
#define COMPACT_ALLOC_CHUNK 8192	/* amount to get from palloc at once */
#define COMPACT_MAX_REQ		1024	/* must be < COMPACT_ALLOC_CHUNK */

static void *
compact_palloc0(IspellDict *Conf, size_t size)
{
	void	   *result;

	/* Should only be called during init */
	Assert(Conf->buildCxt != NULL);

	/* No point in this for large chunks */
	if (size > COMPACT_MAX_REQ)
		return palloc0(size);

	/* Keep everything maxaligned */
	size = MAXALIGN(size);

	/* Need more space? */
	if (size > Conf->avail)
	{
		Conf->firstfree = palloc0(COMPACT_ALLOC_CHUNK);
		Conf->avail = COMPACT_ALLOC_CHUNK;
	}

	result = Conf->firstfree;
	Conf->firstfree += size;
	Conf->avail -= size;

	return result;
}

#define cpalloc(size) compact_palloc0(Conf, size)
#define cpalloc0(size) compact_palloc0(Conf, size)

static char *
cpstrdup(IspellDict *Conf, const char *str)
{
	char	   *res = cpalloc(strlen(str) + 1);

	strcpy(res, str);
	return res;
}


/*
 * Apply str_tolower(), producing a temporary result (in the buildCxt).
 */
static char *
lowerstr_ctx(IspellDict *Conf, const char *src)
{
	MemoryContext saveCtx;
	char	   *dst;

	saveCtx = MemoryContextSwitchTo(Conf->buildCxt);
	dst = str_tolower(src, strlen(src), DEFAULT_COLLATION_OID);
	MemoryContextSwitchTo(saveCtx);

	return dst;
}

#define MAX_NORM 1024
#define MAXNORMLEN 256

#define STRNCMP(s,p)	strncmp( (s), (p), strlen(p) )
#define GETWCHAR(W,L,N,T) ( ((const uint8*)(W))[ ((T)==FF_PREFIX) ? (N) : ( (L) - 1 - (N) ) ] )
#define GETCHAR(A,N,T)	  GETWCHAR( (A)->repl, (A)->replen, N, T )

static const char *VoidString = "";

static int
cmpspell(const void *s1, const void *s2)
{
	return strcmp((*(SPELL *const *) s1)->word, (*(SPELL *const *) s2)->word);
}

static int
cmpspellaffix(const void *s1, const void *s2)
{
	return strcmp((*(SPELL *const *) s1)->p.flag,
				  (*(SPELL *const *) s2)->p.flag);
}

static int
cmpcmdflag(const void *f1, const void *f2)
{
	CompoundAffixFlag *fv1 = (CompoundAffixFlag *) f1,
			   *fv2 = (CompoundAffixFlag *) f2;

	Assert(fv1->flagMode == fv2->flagMode);

	if (fv1->flagMode == FM_NUM)
	{
		if (fv1->flag.i == fv2->flag.i)
			return 0;

		return (fv1->flag.i > fv2->flag.i) ? 1 : -1;
	}

	return strcmp(fv1->flag.s, fv2->flag.s);
}

static char *
findchar(char *str, int c)
{
	while (*str)
	{
		if (t_iseq(str, c))
			return str;
		str += pg_mblen_cstr(str);
	}

	return NULL;
}

static char *
findchar2(char *str, int c1, int c2)
{
	while (*str)
	{
		if (t_iseq(str, c1) || t_iseq(str, c2))
			return str;
		str += pg_mblen_cstr(str);
	}

	return NULL;
}


/* backward string compare for suffix tree operations */
static int
strbcmp(const unsigned char *s1, const unsigned char *s2)
{
	int			l1 = strlen((const char *) s1) - 1,
				l2 = strlen((const char *) s2) - 1;

	while (l1 >= 0 && l2 >= 0)
	{
		if (s1[l1] < s2[l2])
			return -1;
		if (s1[l1] > s2[l2])
			return 1;
		l1--;
		l2--;
	}
	if (l1 < l2)
		return -1;
	if (l1 > l2)
		return 1;

	return 0;
}

static int
strbncmp(const unsigned char *s1, const unsigned char *s2, size_t count)
{
	int			l1 = strlen((const char *) s1) - 1,
				l2 = strlen((const char *) s2) - 1,
				l = count;

	while (l1 >= 0 && l2 >= 0 && l > 0)
	{
		if (s1[l1] < s2[l2])
			return -1;
		if (s1[l1] > s2[l2])
			return 1;
		l1--;
		l2--;
		l--;
	}
	if (l == 0)
		return 0;
	if (l1 < l2)
		return -1;
	if (l1 > l2)
		return 1;
	return 0;
}

/*
 * Compares affixes.
 * First compares the type of an affix. Prefixes should go before affixes.
 * If types are equal then compares replaceable string.
 */
static int
cmpaffix(const void *s1, const void *s2)
{
	const AFFIX *a1 = (const AFFIX *) s1;
	const AFFIX *a2 = (const AFFIX *) s2;

	if (a1->type < a2->type)
		return -1;
	if (a1->type > a2->type)
		return 1;
	if (a1->type == FF_PREFIX)
		return strcmp(a1->repl, a2->repl);
	else
		return strbcmp((const unsigned char *) a1->repl,
					   (const unsigned char *) a2->repl);
}

/* ==== pg_qsort (task #98 sort-symbol hygiene; spgbox/hstorefam pattern).
 * VERBATIM lib/sort_template.h instantiated exactly as port/qsort.c does
 * (ST_SORT/ST_ELEMENT_TYPE_VOID/ST_COMPARE_RUNTIME_POINTER), ST_SCOPE
 * static and family-prefixed so this archive neither exports an unprefixed
 * sort symbol (link race) nor binds LIBC qsort where the backend means
 * pg_qsort (port.h maps qsort -> pg_qsort), keeping spell.c's qsort TIE
 * ORDER the backend's own. ==== */
#define pg_noinline __attribute__((noinline))
#define CppConcat(x, y) x##y
#define ST_SORT spf_pg_qsort
#define ST_ELEMENT_TYPE_VOID
#define ST_COMPARE_RUNTIME_POINTER
#define ST_SCOPE static
#define ST_DECLARE
#define ST_DEFINE
#include "sort_template.h"
#define qsort(a,b,c,d) spf_pg_qsort(a,b,c,d)

/*
 * Gets an affix flag from the set of affix flags (sflagset).
 *
 * Several flags can be stored in a single string. Flags can be represented by:
 * - 1 character (FM_CHAR). A character may be Unicode.
 * - 2 characters (FM_LONG). A character may be Unicode.
 * - numbers from 1 to 65000 (FM_NUM).
 *
 * Depending on the flagMode an affix string can have the following format:
 * - FM_CHAR: ABCD
 *	 Here we have 4 flags: A, B, C and D
 * - FM_LONG: ABCDE*
 *	 Here we have 3 flags: AB, CD and E*
 * - FM_NUM: 200,205,50
 *	 Here we have 3 flags: 200, 205 and 50
 *
 * Conf: current dictionary.
 * sflagset: the set of affix flags. Returns a reference to the start of a next
 *			 affix flag.
 * sflag: returns an affix flag from sflagset.
 */
static void
getNextFlagFromString(IspellDict *Conf, const char **sflagset, char *sflag)
{
	int32		s;
	char	   *next;
	const char *sbuf = *sflagset;
	int			maxstep;
	int			clen;
	bool		stop = false;
	bool		met_comma = false;

	maxstep = (Conf->flagMode == FM_LONG) ? 2 : 1;

	while (**sflagset)
	{
		switch (Conf->flagMode)
		{
			case FM_LONG:
			case FM_CHAR:
				clen = ts_copychar_cstr(sflag, *sflagset);
				sflag += clen;

				/* Go to start of the next flag */
				*sflagset += clen;

				/* Check if we get all characters of flag */
				maxstep--;
				stop = (maxstep == 0);
				break;
			case FM_NUM:
				errno = 0;
				s = strtol(*sflagset, &next, 10);
				if (*sflagset == next || errno == ERANGE)
					ereport(ERROR,
							(errcode(ERRCODE_CONFIG_FILE_ERROR),
							 errmsg("invalid affix flag \"%s\"", *sflagset)));
				if (s < 0 || s > FLAGNUM_MAXSIZE)
					ereport(ERROR,
							(errcode(ERRCODE_CONFIG_FILE_ERROR),
							 errmsg("affix flag \"%s\" is out of range",
									*sflagset)));
				sflag += sprintf(sflag, "%0d", s);

				/* Go to start of the next flag */
				*sflagset = next;
				while (**sflagset)
				{
					if (isdigit((unsigned char) **sflagset))
					{
						if (!met_comma)
							ereport(ERROR,
									(errcode(ERRCODE_CONFIG_FILE_ERROR),
									 errmsg("invalid affix flag \"%s\"",
											*sflagset)));
						break;
					}
					else if (t_iseq(*sflagset, ','))
					{
						if (met_comma)
							ereport(ERROR,
									(errcode(ERRCODE_CONFIG_FILE_ERROR),
									 errmsg("invalid affix flag \"%s\"",
											*sflagset)));
						met_comma = true;
					}
					else if (!isspace((unsigned char) **sflagset))
					{
						ereport(ERROR,
								(errcode(ERRCODE_CONFIG_FILE_ERROR),
								 errmsg("invalid character in affix flag \"%s\"",
										*sflagset)));
					}

					*sflagset += pg_mblen_cstr(*sflagset);
				}
				stop = true;
				break;
			default:
				elog(ERROR, "unrecognized type of Conf->flagMode: %d",
					 Conf->flagMode);
		}

		if (stop)
			break;
	}

	if (Conf->flagMode == FM_LONG && maxstep > 0)
		ereport(ERROR,
				(errcode(ERRCODE_CONFIG_FILE_ERROR),
				 errmsg("invalid affix flag \"%s\" with \"long\" flag value",
						sbuf)));

	*sflag = '\0';
}

/*
 * Checks if the affix set Conf->AffixData[affix] contains affixflag.
 * Conf->AffixData[affix] does not contain affixflag if this flag is not used
 * actually by the .dict file.
 *
 * Conf: current dictionary.
 * affix: index of the Conf->AffixData array.
 * affixflag: the affix flag.
 *
 * Returns true if the string Conf->AffixData[affix] contains affixflag,
 * otherwise returns false.
 */
static bool
IsAffixFlagInUse(IspellDict *Conf, int affix, const char *affixflag)
{
	const char *flagcur;
	char		flag[BUFSIZ];

	if (*affixflag == 0)
		return true;

	Assert(affix < Conf->nAffixData);

	flagcur = Conf->AffixData[affix];

	while (*flagcur)
	{
		getNextFlagFromString(Conf, &flagcur, flag);
		/* Compare first affix flag in flagcur with affixflag */
		if (strcmp(flag, affixflag) == 0)
			return true;
	}

	/* Could not find affixflag */
	return false;
}

/*
 * Adds the new word into the temporary array Spell.
 *
 * Conf: current dictionary.
 * word: new word.
 * flag: set of affix flags. Single flag can be get by getNextFlagFromString().
 */
static void
NIAddSpell(IspellDict *Conf, const char *word, const char *flag)
{
	if (Conf->nspell >= Conf->mspell)
	{
		if (Conf->mspell)
		{
			Conf->mspell *= 2;
			Conf->Spell = (SPELL **) repalloc(Conf->Spell, Conf->mspell * sizeof(SPELL *));
		}
		else
		{
			Conf->mspell = 1024 * 20;
			Conf->Spell = (SPELL **) tmpalloc(Conf->mspell * sizeof(SPELL *));
		}
	}
	Conf->Spell[Conf->nspell] = (SPELL *) tmpalloc(SPELLHDRSZ + strlen(word) + 1);
	strcpy(Conf->Spell[Conf->nspell]->word, word);
	Conf->Spell[Conf->nspell]->p.flag = (*flag != '\0')
		? cpstrdup(Conf, flag) : VoidString;
	Conf->nspell++;
}

/*
 * Imports dictionary into the temporary array Spell.
 *
 * Note caller must already have applied get_tsearch_config_filename.
 *
 * Conf: current dictionary.
 * filename: path to the .dict file.
 */
void
NIImportDictionary(IspellDict *Conf, const char *filename)
{
	tsearch_readline_state trst;
	char	   *line;

	if (!tsearch_readline_begin(&trst, filename))
		ereport(ERROR,
				(errcode(ERRCODE_CONFIG_FILE_ERROR),
				 errmsg("could not open dictionary file \"%s\": %m",
						filename)));

	while ((line = tsearch_readline(&trst)) != NULL)
	{
		char	   *s,
				   *pstr;

		/* Set of affix flags */
		const char *flag;

		/* Extract flag from the line */
		flag = NULL;
		if ((s = findchar(line, '/')))
		{
			*s++ = '\0';
			flag = s;
			while (*s)
			{
				/* we allow only single encoded flags for faster works */
				if (pg_mblen_cstr(s) == 1 && isprint((unsigned char) *s) && !isspace((unsigned char) *s))
					s++;
				else
				{
					*s = '\0';
					break;
				}
			}
		}
		else
			flag = "";

		/* Remove trailing spaces */
		s = line;
		while (*s)
		{
			if (isspace((unsigned char) *s))
			{
				*s = '\0';
				break;
			}
			s += pg_mblen_cstr(s);
		}
		pstr = lowerstr_ctx(Conf, line);

		NIAddSpell(Conf, pstr, flag);
		pfree(pstr);

		pfree(line);
	}
	tsearch_readline_end(&trst);
}

/*
 * Searches a basic form of word in the prefix tree. This word was generated
 * using an affix rule. This rule may not be presented in an affix set of
 * a basic form of word.
 *
 * For example, we have the entry in the .dict file:
 * meter/GMD
 *
 * The affix rule with the flag S:
 * SFX S   y	 ies		[^aeiou]y
 * is not presented here.
 *
 * The affix rule with the flag M:
 * SFX M   0	 's         .
 * is presented here.
 *
 * Conf: current dictionary.
 * word: basic form of word.
 * affixflag: affix flag, by which a basic form of word was generated.
 * flag: compound flag used to compare with StopMiddle->compoundflag.
 *
 * Returns 1 if the word was found in the prefix tree, else returns 0.
 */
static int
FindWord(IspellDict *Conf, const char *word, const char *affixflag, int flag)
{
	SPNode	   *node = Conf->Dictionary;
	SPNodeData *StopLow,
			   *StopHigh,
			   *StopMiddle;
	const uint8 *ptr = (const uint8 *) word;

	flag &= FF_COMPOUNDFLAGMASK;

	while (node && *ptr)
	{
		StopLow = node->data;
		StopHigh = node->data + node->length;
		while (StopLow < StopHigh)
		{
			StopMiddle = StopLow + ((StopHigh - StopLow) >> 1);
			if (StopMiddle->val == *ptr)
			{
				if (*(ptr + 1) == '\0' && StopMiddle->isword)
				{
					if (flag == 0)
					{
						/*
						 * The word can be formed only with another word. And
						 * in the flag parameter there is not a sign that we
						 * search compound words.
						 */
						if (StopMiddle->compoundflag & FF_COMPOUNDONLY)
							return 0;
					}
					else if ((flag & StopMiddle->compoundflag) == 0)
						return 0;

					/*
					 * Check if this affix rule is presented in the affix set
					 * with index StopMiddle->affix.
					 */
					if (IsAffixFlagInUse(Conf, StopMiddle->affix, affixflag))
						return 1;
				}
				node = StopMiddle->node;
				ptr++;
				break;
			}
			else if (StopMiddle->val < *ptr)
				StopLow = StopMiddle + 1;
			else
				StopHigh = StopMiddle;
		}
		if (StopLow >= StopHigh)
			break;
	}
	return 0;
}

/*
 * Adds a new affix rule to the Affix field.
 *
 * Conf: current dictionary.
 * flag: affix flag ('\' in the below example).
 * flagflags: set of flags from the flagval field for this affix rule. This set
 *			  is listed after '/' character in the added string (repl).
 *
 *			  For example L flag in the hunspell_sample.affix:
 *			  SFX \   0 Y/L [^Y]
 *
 * mask: condition for search ('[^Y]' in the above example).
 * find: stripping characters from beginning (at prefix) or end (at suffix)
 *		 of the word ('0' in the above example, 0 means that there is not
 *		 stripping character).
 * repl: adding string after stripping ('Y' in the above example).
 * type: FF_SUFFIX or FF_PREFIX.
 */
static void
NIAddAffix(IspellDict *Conf, const char *flag, char flagflags, const char *mask,
		   const char *find, const char *repl, int type)
{
	AFFIX	   *Affix;

	if (Conf->naffixes >= Conf->maffixes)
	{
		if (Conf->maffixes)
		{
			Conf->maffixes *= 2;
			Conf->Affix = (AFFIX *) repalloc(Conf->Affix, Conf->maffixes * sizeof(AFFIX));
		}
		else
		{
			Conf->maffixes = 16;
			Conf->Affix = (AFFIX *) palloc(Conf->maffixes * sizeof(AFFIX));
		}
	}

	Affix = Conf->Affix + Conf->naffixes;

	/* This affix rule can be applied for words with any ending */
	if (strcmp(mask, ".") == 0 || *mask == '\0')
	{
		Affix->issimple = 1;
		Affix->isregis = 0;
	}
	/* This affix rule will use regis to search word ending */
	else if (RS_isRegis(mask))
	{
		Affix->issimple = 0;
		Affix->isregis = 1;
		RS_compile(&(Affix->reg.regis), (type == FF_SUFFIX),
				   *mask ? mask : VoidString);
	}
	/* This affix rule will use regex_t to search word ending */
	else
	{
		int			masklen;
		int			wmasklen;
		int			err;
		pg_wchar   *wmask;
		char	   *tmask;

		Affix->issimple = 0;
		Affix->isregis = 0;
		tmask = (char *) tmpalloc(strlen(mask) + 3);
		if (type == FF_SUFFIX)
			sprintf(tmask, "%s$", mask);
		else
			sprintf(tmask, "^%s", mask);

		masklen = strlen(tmask);
		wmask = (pg_wchar *) tmpalloc((masklen + 1) * sizeof(pg_wchar));
		wmasklen = pg_mb2wchar_with_len(tmask, wmask, masklen);

		/*
		 * The regex and all internal state created by pg_regcomp are
		 * allocated in the dictionary's memory context, and will be freed
		 * automatically when it is destroyed.
		 */
		Affix->reg.pregex = palloc(sizeof(regex_t));
		err = pg_regcomp(Affix->reg.pregex, wmask, wmasklen,
						 REG_ADVANCED | REG_NOSUB,
						 DEFAULT_COLLATION_OID);
		if (err)
		{
			char		errstr[100];

			pg_regerror(err, Affix->reg.pregex, errstr, sizeof(errstr));
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_REGULAR_EXPRESSION),
					 errmsg("invalid regular expression: %s", errstr)));
		}
	}

	Affix->flagflags = flagflags;
	if ((Affix->flagflags & FF_COMPOUNDONLY) || (Affix->flagflags & FF_COMPOUNDPERMITFLAG))
	{
		if ((Affix->flagflags & FF_COMPOUNDFLAG) == 0)
			Affix->flagflags |= FF_COMPOUNDFLAG;
	}
	Affix->flag = cpstrdup(Conf, flag);
	Affix->type = type;

	Affix->find = (find && *find) ? cpstrdup(Conf, find) : VoidString;
	if ((Affix->replen = strlen(repl)) > 0)
		Affix->repl = cpstrdup(Conf, repl);
	else
		Affix->repl = VoidString;
	Conf->naffixes++;
}

/* Parsing states for parse_affentry() and friends */
#define PAE_WAIT_MASK	0
#define PAE_INMASK		1
#define PAE_WAIT_FIND	2
#define PAE_INFIND		3
#define PAE_WAIT_REPL	4
#define PAE_INREPL		5
#define PAE_WAIT_TYPE	6
#define PAE_WAIT_FLAG	7

/*
 * Parse next space-separated field of an .affix file line.
 *
 * *str is the input pointer (will be advanced past field)
 * next is where to copy the field value to, with null termination
 *
 * The buffer at "next" must be of size BUFSIZ; we truncate the input to fit.
 *
 * Returns true if we found a field, false if not.
 */
static bool
get_nextfield(char **str, char *next)
{
	int			state = PAE_WAIT_MASK;
	int			avail = BUFSIZ;

	while (**str)
	{
		int			clen = pg_mblen_cstr(*str);

		if (state == PAE_WAIT_MASK)
		{
			if (t_iseq(*str, '#'))
				return false;
			else if (!isspace((unsigned char) **str))
			{
				if (clen < avail)
				{
					ts_copychar_with_len(next, *str, clen);
					next += clen;
					avail -= clen;
				}
				state = PAE_INMASK;
			}
		}
		else					/* state == PAE_INMASK */
		{
			if (isspace((unsigned char) **str))
			{
				*next = '\0';
				return true;
			}
			else
			{
				if (clen < avail)
				{
					ts_copychar_with_len(next, *str, clen);
					next += clen;
					avail -= clen;
				}
			}
		}
		*str += clen;
	}

	*next = '\0';

	return (state == PAE_INMASK);	/* OK if we got a nonempty field */
}

/*
 * Parses entry of an .affix file of MySpell or Hunspell format.
 *
 * An .affix file entry has the following format:
 * - header
 *	 <type>  <flag>  <cross_flag>  <flag_count>
 * - fields after header:
 *	 <type>  <flag>  <find>  <replace>	<mask>
 *
 * str is the input line
 * field values are returned to type etc, which must be buffers of size BUFSIZ.
 *
 * Returns number of fields found; any omitted fields are set to empty strings.
 */
static int
parse_ooaffentry(char *str, char *type, char *flag, char *find,
				 char *repl, char *mask)
{
	int			state = PAE_WAIT_TYPE;
	int			fields_read = 0;
	bool		valid = false;

	*type = *flag = *find = *repl = *mask = '\0';

	while (*str)
	{
		switch (state)
		{
			case PAE_WAIT_TYPE:
				valid = get_nextfield(&str, type);
				state = PAE_WAIT_FLAG;
				break;
			case PAE_WAIT_FLAG:
				valid = get_nextfield(&str, flag);
				state = PAE_WAIT_FIND;
				break;
			case PAE_WAIT_FIND:
				valid = get_nextfield(&str, find);
				state = PAE_WAIT_REPL;
				break;
			case PAE_WAIT_REPL:
				valid = get_nextfield(&str, repl);
				state = PAE_WAIT_MASK;
				break;
			case PAE_WAIT_MASK:
				valid = get_nextfield(&str, mask);
				state = -1;		/* force loop exit */
				break;
			default:
				elog(ERROR, "unrecognized state in parse_ooaffentry: %d",
					 state);
				break;
		}
		if (valid)
			fields_read++;
		else
			break;				/* early EOL */
		if (state < 0)
			break;				/* got all fields */
	}

	return fields_read;
}

/*
 * Parses entry of an .affix file of Ispell format
 *
 * An .affix file entry has the following format:
 * <mask>  >  [-<find>,]<replace>
 */
static bool
parse_affentry(char *str, char *mask, char *find, char *repl)
{
	int			state = PAE_WAIT_MASK;
	char	   *pmask = mask,
			   *pfind = find,
			   *prepl = repl;

	*mask = *find = *repl = '\0';

	while (*str)
	{
		int			clen = pg_mblen_cstr(str);

		if (state == PAE_WAIT_MASK)
		{
			if (t_iseq(str, '#'))
				return false;
			else if (!isspace((unsigned char) *str))
			{
				pmask += ts_copychar_with_len(pmask, str, clen);
				state = PAE_INMASK;
			}
		}
		else if (state == PAE_INMASK)
		{
			if (t_iseq(str, '>'))
			{
				*pmask = '\0';
				state = PAE_WAIT_FIND;
			}
			else if (!isspace((unsigned char) *str))
			{
				pmask += ts_copychar_with_len(pmask, str, clen);
			}
		}
		else if (state == PAE_WAIT_FIND)
		{
			if (t_iseq(str, '-'))
			{
				state = PAE_INFIND;
			}
			else if (t_isalpha_cstr(str) || t_iseq(str, '\'') /* english 's */ )
			{
				prepl += ts_copychar_with_len(prepl, str, clen);
				state = PAE_INREPL;
			}
			else if (!isspace((unsigned char) *str))
				ereport(ERROR,
						(errcode(ERRCODE_CONFIG_FILE_ERROR),
						 errmsg("syntax error")));
		}
		else if (state == PAE_INFIND)
		{
			if (t_iseq(str, ','))
			{
				*pfind = '\0';
				state = PAE_WAIT_REPL;
			}
			else if (t_isalpha_cstr(str))
			{
				pfind += ts_copychar_with_len(pfind, str, clen);
			}
			else if (!isspace((unsigned char) *str))
				ereport(ERROR,
						(errcode(ERRCODE_CONFIG_FILE_ERROR),
						 errmsg("syntax error")));
		}
		else if (state == PAE_WAIT_REPL)
		{
			if (t_iseq(str, '-'))
			{
				break;			/* void repl */
			}
			else if (t_isalpha_cstr(str))
			{
				prepl += ts_copychar_with_len(prepl, str, clen);
				state = PAE_INREPL;
			}
			else if (!isspace((unsigned char) *str))
				ereport(ERROR,
						(errcode(ERRCODE_CONFIG_FILE_ERROR),
						 errmsg("syntax error")));
		}
		else if (state == PAE_INREPL)
		{
			if (t_iseq(str, '#'))
			{
				*prepl = '\0';
				break;
			}
			else if (t_isalpha_cstr(str))
			{
				prepl += ts_copychar_with_len(prepl, str, clen);
			}
			else if (!isspace((unsigned char) *str))
				ereport(ERROR,
						(errcode(ERRCODE_CONFIG_FILE_ERROR),
						 errmsg("syntax error")));
		}
		else
			elog(ERROR, "unrecognized state in parse_affentry: %d", state);

		str += clen;
	}

	*pmask = *pfind = *prepl = '\0';

	return (*mask && (*find || *repl));
}

/*
 * Sets a Hunspell options depending on flag type.
 */
static void
setCompoundAffixFlagValue(IspellDict *Conf, CompoundAffixFlag *entry,
						  char *s, uint32 val)
{
	if (Conf->flagMode == FM_NUM)
	{
		char	   *next;
		int			i;

		errno = 0;
		i = strtol(s, &next, 10);
		if (s == next || errno == ERANGE)
			ereport(ERROR,
					(errcode(ERRCODE_CONFIG_FILE_ERROR),
					 errmsg("invalid affix flag \"%s\"", s)));
		if (i < 0 || i > FLAGNUM_MAXSIZE)
			ereport(ERROR,
					(errcode(ERRCODE_CONFIG_FILE_ERROR),
					 errmsg("affix flag \"%s\" is out of range", s)));

		entry->flag.i = i;
	}
	else
		entry->flag.s = cpstrdup(Conf, s);

	entry->flagMode = Conf->flagMode;
	entry->value = val;
}

/*
 * Sets up a correspondence for the affix parameter with the affix flag.
 *
 * Conf: current dictionary.
 * s: affix flag in string.
 * val: affix parameter.
 */
static void
addCompoundAffixFlagValue(IspellDict *Conf, char *s, uint32 val)
{
	CompoundAffixFlag *newValue;
	char		sbuf[BUFSIZ];
	char	   *sflag;

	while (*s && isspace((unsigned char) *s))
		s += pg_mblen_cstr(s);

	if (!*s)
		ereport(ERROR,
				(errcode(ERRCODE_CONFIG_FILE_ERROR),
				 errmsg("syntax error")));

	/* Get flag without \n */
	sflag = sbuf;
	while (*s && !isspace((unsigned char) *s) && *s != '\n')
	{
		int			clen = ts_copychar_cstr(sflag, s);

		sflag += clen;
		s += clen;
	}
	*sflag = '\0';

	/* Resize array or allocate memory for array CompoundAffixFlag */
	if (Conf->nCompoundAffixFlag >= Conf->mCompoundAffixFlag)
	{
		if (Conf->mCompoundAffixFlag)
		{
			Conf->mCompoundAffixFlag *= 2;
			Conf->CompoundAffixFlags = (CompoundAffixFlag *)
				repalloc(Conf->CompoundAffixFlags,
						 Conf->mCompoundAffixFlag * sizeof(CompoundAffixFlag));
		}
		else
		{
			Conf->mCompoundAffixFlag = 10;
			Conf->CompoundAffixFlags = (CompoundAffixFlag *)
				tmpalloc(Conf->mCompoundAffixFlag * sizeof(CompoundAffixFlag));
		}
	}

	newValue = Conf->CompoundAffixFlags + Conf->nCompoundAffixFlag;

	setCompoundAffixFlagValue(Conf, newValue, sbuf, val);

	Conf->usecompound = true;
	Conf->nCompoundAffixFlag++;
}

/*
 * Returns a set of affix parameters which correspondence to the set of affix
 * flags s.
 */
static int
getCompoundAffixFlagValue(IspellDict *Conf, const char *s)
{
	uint32		flag = 0;
	CompoundAffixFlag *found,
				key;
	char		sflag[BUFSIZ];
	const char *flagcur;

	if (Conf->nCompoundAffixFlag == 0)
		return 0;

	flagcur = s;
	while (*flagcur)
	{
		getNextFlagFromString(Conf, &flagcur, sflag);
		setCompoundAffixFlagValue(Conf, &key, sflag, 0);

		found = (CompoundAffixFlag *)
			bsearch(&key, Conf->CompoundAffixFlags,
					Conf->nCompoundAffixFlag, sizeof(CompoundAffixFlag),
					cmpcmdflag);
		if (found != NULL)
			flag |= found->value;
	}

	return flag;
}

/*
 * Returns a flag set using the s parameter.
 *
 * If Conf->useFlagAliases is true then the s parameter is index of the
 * Conf->AffixData array and function returns its entry.
 * Else function returns the s parameter.
 */
static const char *
getAffixFlagSet(IspellDict *Conf, char *s)
{
	if (Conf->useFlagAliases && *s != '\0')
	{
		int			curaffix;
		char	   *end;

		errno = 0;
		curaffix = strtol(s, &end, 10);
		if (s == end || errno == ERANGE)
			ereport(ERROR,
					(errcode(ERRCODE_CONFIG_FILE_ERROR),
					 errmsg("invalid affix alias \"%s\"", s)));

		if (curaffix > 0 && curaffix < Conf->nAffixData)

			/*
			 * Do not subtract 1 from curaffix because empty string was added
			 * in NIImportOOAffixes
			 */
			return Conf->AffixData[curaffix];
		else if (curaffix > Conf->nAffixData)
			ereport(ERROR,
					(errcode(ERRCODE_CONFIG_FILE_ERROR),
					 errmsg("invalid affix alias \"%s\"", s)));
		return VoidString;
	}
	else
		return s;
}

/*
 * Import an affix file that follows MySpell or Hunspell format.
 *
 * Conf: current dictionary.
 * filename: path to the .affix file.
 */
static void
NIImportOOAffixes(IspellDict *Conf, const char *filename)
{
	char		type[BUFSIZ],
			   *ptype = NULL;
	char		sflag[BUFSIZ];
	char		mask[BUFSIZ],
			   *pmask;
	char		find[BUFSIZ],
			   *pfind;
	char		repl[BUFSIZ],
			   *prepl;
	bool		isSuffix = false;
	int			naffix = 0,
				curaffix = 0;
	int			sflaglen = 0;
	char		flagflags = 0;
	tsearch_readline_state trst;
	char	   *recoded;

	/* read file to find any flag */
	Conf->usecompound = false;
	Conf->useFlagAliases = false;
	Conf->flagMode = FM_CHAR;

	if (!tsearch_readline_begin(&trst, filename))
		ereport(ERROR,
				(errcode(ERRCODE_CONFIG_FILE_ERROR),
				 errmsg("could not open affix file \"%s\": %m",
						filename)));

	while ((recoded = tsearch_readline(&trst)) != NULL)
	{
		if (*recoded == '\0' || isspace((unsigned char) *recoded) || t_iseq(recoded, '#'))
		{
			pfree(recoded);
			continue;
		}

		if (STRNCMP(recoded, "COMPOUNDFLAG") == 0)
			addCompoundAffixFlagValue(Conf, recoded + strlen("COMPOUNDFLAG"),
									  FF_COMPOUNDFLAG);
		else if (STRNCMP(recoded, "COMPOUNDBEGIN") == 0)
			addCompoundAffixFlagValue(Conf, recoded + strlen("COMPOUNDBEGIN"),
									  FF_COMPOUNDBEGIN);
		else if (STRNCMP(recoded, "COMPOUNDLAST") == 0)
			addCompoundAffixFlagValue(Conf, recoded + strlen("COMPOUNDLAST"),
									  FF_COMPOUNDLAST);
		/* COMPOUNDLAST and COMPOUNDEND are synonyms */
		else if (STRNCMP(recoded, "COMPOUNDEND") == 0)
			addCompoundAffixFlagValue(Conf, recoded + strlen("COMPOUNDEND"),
									  FF_COMPOUNDLAST);
		else if (STRNCMP(recoded, "COMPOUNDMIDDLE") == 0)
			addCompoundAffixFlagValue(Conf, recoded + strlen("COMPOUNDMIDDLE"),
									  FF_COMPOUNDMIDDLE);
		else if (STRNCMP(recoded, "ONLYINCOMPOUND") == 0)
			addCompoundAffixFlagValue(Conf, recoded + strlen("ONLYINCOMPOUND"),
									  FF_COMPOUNDONLY);
		else if (STRNCMP(recoded, "COMPOUNDPERMITFLAG") == 0)
			addCompoundAffixFlagValue(Conf,
									  recoded + strlen("COMPOUNDPERMITFLAG"),
									  FF_COMPOUNDPERMITFLAG);
		else if (STRNCMP(recoded, "COMPOUNDFORBIDFLAG") == 0)
			addCompoundAffixFlagValue(Conf,
									  recoded + strlen("COMPOUNDFORBIDFLAG"),
									  FF_COMPOUNDFORBIDFLAG);
		else if (STRNCMP(recoded, "FLAG") == 0)
		{
			char	   *s = recoded + strlen("FLAG");

			while (*s && isspace((unsigned char) *s))
				s += pg_mblen_cstr(s);

			if (*s)
			{
				if (STRNCMP(s, "long") == 0)
					Conf->flagMode = FM_LONG;
				else if (STRNCMP(s, "num") == 0)
					Conf->flagMode = FM_NUM;
				else if (STRNCMP(s, "default") != 0)
					ereport(ERROR,
							(errcode(ERRCODE_CONFIG_FILE_ERROR),
							 errmsg("Ispell dictionary supports only "
									"\"default\", \"long\", "
									"and \"num\" flag values")));
			}
		}

		pfree(recoded);
	}
	tsearch_readline_end(&trst);

	if (Conf->nCompoundAffixFlag > 1)
		qsort(Conf->CompoundAffixFlags, Conf->nCompoundAffixFlag,
			  sizeof(CompoundAffixFlag), cmpcmdflag);

	if (!tsearch_readline_begin(&trst, filename))
		ereport(ERROR,
				(errcode(ERRCODE_CONFIG_FILE_ERROR),
				 errmsg("could not open affix file \"%s\": %m",
						filename)));

	while ((recoded = tsearch_readline(&trst)) != NULL)
	{
		int			fields_read;

		if (*recoded == '\0' || isspace((unsigned char) *recoded) || t_iseq(recoded, '#'))
			goto nextline;

		fields_read = parse_ooaffentry(recoded, type, sflag, find, repl, mask);

		if (ptype)
			pfree(ptype);
		ptype = lowerstr_ctx(Conf, type);

		/* First try to parse AF parameter (alias compression) */
		if (STRNCMP(ptype, "af") == 0)
		{
			/* First line is the number of aliases */
			if (!Conf->useFlagAliases)
			{
				Conf->useFlagAliases = true;
				naffix = atoi(sflag);
				if (naffix <= 0)
					ereport(ERROR,
							(errcode(ERRCODE_CONFIG_FILE_ERROR),
							 errmsg("invalid number of flag vector aliases")));

				/* Also reserve place for empty flag set */
				naffix++;

				Conf->AffixData = (const char **) palloc0(naffix * sizeof(char *));
				Conf->lenAffixData = Conf->nAffixData = naffix;

				/* Add empty flag set into AffixData */
				Conf->AffixData[curaffix] = VoidString;
				curaffix++;
			}
			/* Other lines are aliases */
			else
			{
				if (curaffix < naffix)
				{
					Conf->AffixData[curaffix] = cpstrdup(Conf, sflag);
					curaffix++;
				}
				else
					ereport(ERROR,
							(errcode(ERRCODE_CONFIG_FILE_ERROR),
							 errmsg("number of aliases exceeds specified number %d",
									naffix - 1)));
			}
			goto nextline;
		}
		/* Else try to parse prefixes and suffixes */
		if (fields_read < 4 ||
			(STRNCMP(ptype, "sfx") != 0 && STRNCMP(ptype, "pfx") != 0))
			goto nextline;

		sflaglen = strlen(sflag);
		if (sflaglen == 0
			|| (sflaglen > 1 && Conf->flagMode == FM_CHAR)
			|| (sflaglen > 2 && Conf->flagMode == FM_LONG))
			goto nextline;

		/*--------
		 * Affix header. For example:
		 * SFX \ N 1
		 *--------
		 */
		if (fields_read == 4)
		{
			isSuffix = (STRNCMP(ptype, "sfx") == 0);
			if (t_iseq(find, 'y') || t_iseq(find, 'Y'))
				flagflags = FF_CROSSPRODUCT;
			else
				flagflags = 0;
		}
		/*--------
		 * Affix fields. For example:
		 * SFX \   0	Y/L [^Y]
		 *--------
		 */
		else
		{
			char	   *ptr;
			int			aflg = 0;

			/* Get flags after '/' (flags are case sensitive) */
			if ((ptr = strchr(repl, '/')) != NULL)
				aflg |= getCompoundAffixFlagValue(Conf,
												  getAffixFlagSet(Conf,
																  ptr + 1));
			/* Get lowercased version of string before '/' */
			prepl = lowerstr_ctx(Conf, repl);
			if ((ptr = strchr(prepl, '/')) != NULL)
				*ptr = '\0';
			pfind = lowerstr_ctx(Conf, find);
			pmask = lowerstr_ctx(Conf, mask);
			if (t_iseq(find, '0'))
				*pfind = '\0';
			if (t_iseq(repl, '0'))
				*prepl = '\0';

			NIAddAffix(Conf, sflag, flagflags | aflg, pmask, pfind, prepl,
					   isSuffix ? FF_SUFFIX : FF_PREFIX);
			pfree(prepl);
			pfree(pfind);
			pfree(pmask);
		}

nextline:
		pfree(recoded);
	}

	tsearch_readline_end(&trst);
	if (ptype)
		pfree(ptype);
}

/*
 * import affixes
 *
 * Note caller must already have applied get_tsearch_config_filename
 *
 * This function is responsible for parsing ispell ("old format") affix files.
 * If we realize that the file contains new-format commands, we pass off the
 * work to NIImportOOAffixes(), which will re-read the whole file.
 */
void
NIImportAffixes(IspellDict *Conf, const char *filename)
{
	char	   *pstr = NULL;
	char		flag[BUFSIZ];
	char		mask[BUFSIZ];
	char		find[BUFSIZ];
	char		repl[BUFSIZ];
	char	   *s;
	bool		suffixes = false;
	bool		prefixes = false;
	char		flagflags = 0;
	tsearch_readline_state trst;
	bool		oldformat = false;
	char	   *recoded = NULL;

	if (!tsearch_readline_begin(&trst, filename))
		ereport(ERROR,
				(errcode(ERRCODE_CONFIG_FILE_ERROR),
				 errmsg("could not open affix file \"%s\": %m",
						filename)));

	Conf->usecompound = false;
	Conf->useFlagAliases = false;
	Conf->flagMode = FM_CHAR;

	while ((recoded = tsearch_readline(&trst)) != NULL)
	{
		pstr = str_tolower(recoded, strlen(recoded), DEFAULT_COLLATION_OID);

		/* Skip comments and empty lines */
		if (*pstr == '#' || *pstr == '\n')
			goto nextline;

		if (STRNCMP(pstr, "compoundwords") == 0)
		{
			/* Find case-insensitive L flag in non-lowercased string */
			s = findchar2(recoded, 'l', 'L');
			if (s)
			{
				while (*s && !isspace((unsigned char) *s))
					s += pg_mblen_cstr(s);
				while (*s && isspace((unsigned char) *s))
					s += pg_mblen_cstr(s);

				if (*s && pg_mblen_cstr(s) == 1)
				{
					addCompoundAffixFlagValue(Conf, s, FF_COMPOUNDFLAG);
					Conf->usecompound = true;
				}
				oldformat = true;
				goto nextline;
			}
		}
		if (STRNCMP(pstr, "suffixes") == 0)
		{
			suffixes = true;
			prefixes = false;
			oldformat = true;
			goto nextline;
		}
		if (STRNCMP(pstr, "prefixes") == 0)
		{
			suffixes = false;
			prefixes = true;
			oldformat = true;
			goto nextline;
		}
		if (STRNCMP(pstr, "flag") == 0)
		{
			s = recoded + 4;	/* we need non-lowercased string */
			flagflags = 0;

			while (*s && isspace((unsigned char) *s))
				s += pg_mblen_cstr(s);

			if (*s == '*')
			{
				flagflags |= FF_CROSSPRODUCT;
				s++;
			}
			else if (*s == '~')
			{
				flagflags |= FF_COMPOUNDONLY;
				s++;
			}

			if (*s == '\\')
				s++;

			/*
			 * An old-format flag is a single ASCII character; we expect it to
			 * be followed by EOL, whitespace, or ':'.  Otherwise this is a
			 * new-format flag command.
			 */
			if (*s && pg_mblen_cstr(s) == 1)
			{
				flag[0] = *s++;
				flag[1] = '\0';

				if (*s == '\0' || *s == '#' || *s == '\n' || *s == ':' ||
					isspace((unsigned char) *s))
				{
					oldformat = true;
					goto nextline;
				}
			}
			goto isnewformat;
		}
		if (STRNCMP(recoded, "COMPOUNDFLAG") == 0 ||
			STRNCMP(recoded, "COMPOUNDMIN") == 0 ||
			STRNCMP(recoded, "PFX") == 0 ||
			STRNCMP(recoded, "SFX") == 0)
			goto isnewformat;

		if ((!suffixes) && (!prefixes))
			goto nextline;

		if (!parse_affentry(pstr, mask, find, repl))
			goto nextline;

		NIAddAffix(Conf, flag, flagflags, mask, find, repl, suffixes ? FF_SUFFIX : FF_PREFIX);

nextline:
		pfree(recoded);
		pfree(pstr);
	}
	tsearch_readline_end(&trst);
	return;

isnewformat:
	if (oldformat)
		ereport(ERROR,
				(errcode(ERRCODE_CONFIG_FILE_ERROR),
				 errmsg("affix file contains both old-style and new-style commands")));
	tsearch_readline_end(&trst);

	NIImportOOAffixes(Conf, filename);
}

/*
 * Merges two affix flag sets and stores a new affix flag set into
 * Conf->AffixData.
 *
 * Returns index of a new affix flag set.
 */
static int
MergeAffix(IspellDict *Conf, int a1, int a2)
{
	const char **ptr;

	Assert(a1 < Conf->nAffixData && a2 < Conf->nAffixData);

	/* Do not merge affix flags if one of affix flags is empty */
	if (*Conf->AffixData[a1] == '\0')
		return a2;
	else if (*Conf->AffixData[a2] == '\0')
		return a1;

	/* Double the size of AffixData if there's not enough space */
	if (Conf->nAffixData + 1 >= Conf->lenAffixData)
	{
		Conf->lenAffixData *= 2;
		Conf->AffixData = (const char **) repalloc(Conf->AffixData,
												   sizeof(char *) * Conf->lenAffixData);
	}

	ptr = Conf->AffixData + Conf->nAffixData;
	if (Conf->flagMode == FM_NUM)
	{
		char	   *p = cpalloc(strlen(Conf->AffixData[a1]) +
								strlen(Conf->AffixData[a2]) +
								1 /* comma */ + 1 /* \0 */ );

		sprintf(p, "%s,%s", Conf->AffixData[a1], Conf->AffixData[a2]);
		*ptr = p;
	}
	else
	{
		char	   *p = cpalloc(strlen(Conf->AffixData[a1]) +
								strlen(Conf->AffixData[a2]) +
								1 /* \0 */ );

		sprintf(p, "%s%s", Conf->AffixData[a1], Conf->AffixData[a2]);
		*ptr = p;
	}
	ptr++;
	*ptr = NULL;
	Conf->nAffixData++;

	return Conf->nAffixData - 1;
}

/*
 * Returns a set of affix parameters which correspondence to the set of affix
 * flags with the given index.
 */
static uint32
makeCompoundFlags(IspellDict *Conf, int affix)
{
	Assert(affix < Conf->nAffixData);

	return (getCompoundAffixFlagValue(Conf, Conf->AffixData[affix]) &
			FF_COMPOUNDFLAGMASK);
}

/*
 * Makes a prefix tree for the given level.
 *
 * Conf: current dictionary.
 * low: lower index of the Conf->Spell array.
 * high: upper index of the Conf->Spell array.
 * level: current prefix tree level.
 */
static SPNode *
mkSPNode(IspellDict *Conf, int low, int high, int level)
{
	int			i;
	int			nchar = 0;
	char		lastchar = '\0';
	SPNode	   *rs;
	SPNodeData *data;
	int			lownew = low;

	for (i = low; i < high; i++)
		if (Conf->Spell[i]->p.d.len > level && lastchar != Conf->Spell[i]->word[level])
		{
			nchar++;
			lastchar = Conf->Spell[i]->word[level];
		}

	if (!nchar)
		return NULL;

	rs = (SPNode *) cpalloc0(SPNHDRSZ + nchar * sizeof(SPNodeData));
	rs->length = nchar;
	data = rs->data;

	lastchar = '\0';
	for (i = low; i < high; i++)
		if (Conf->Spell[i]->p.d.len > level)
		{
			if (lastchar != Conf->Spell[i]->word[level])
			{
				if (lastchar)
				{
					/* Next level of the prefix tree */
					data->node = mkSPNode(Conf, lownew, i, level + 1);
					lownew = i;
					data++;
				}
				lastchar = Conf->Spell[i]->word[level];
			}
			data->val = ((uint8 *) (Conf->Spell[i]->word))[level];
			if (Conf->Spell[i]->p.d.len == level + 1)
			{
				bool		clearCompoundOnly = false;

				if (data->isword && data->affix != Conf->Spell[i]->p.d.affix)
				{
					/*
					 * MergeAffix called a few times. If one of word is
					 * allowed to be in compound word and another isn't, then
					 * clear FF_COMPOUNDONLY flag.
					 */

					clearCompoundOnly = (FF_COMPOUNDONLY & data->compoundflag
										 & makeCompoundFlags(Conf, Conf->Spell[i]->p.d.affix))
						? false : true;
					data->affix = MergeAffix(Conf, data->affix, Conf->Spell[i]->p.d.affix);
				}
				else
					data->affix = Conf->Spell[i]->p.d.affix;
				data->isword = 1;

				data->compoundflag = makeCompoundFlags(Conf, data->affix);

				if ((data->compoundflag & FF_COMPOUNDONLY) &&
					(data->compoundflag & FF_COMPOUNDFLAG) == 0)
					data->compoundflag |= FF_COMPOUNDFLAG;

				if (clearCompoundOnly)
					data->compoundflag &= ~FF_COMPOUNDONLY;
			}
		}

	/* Next level of the prefix tree */
	data->node = mkSPNode(Conf, lownew, high, level + 1);

	return rs;
}

/*
 * Builds the Conf->Dictionary tree and AffixData from the imported dictionary
 * and affixes.
 */
void
NISortDictionary(IspellDict *Conf)
{
	int			i;
	int			naffix;
	int			curaffix;

	/* compress affixes */

	/*
	 * If we use flag aliases then we need to use Conf->AffixData filled in
	 * the NIImportOOAffixes().
	 */
	if (Conf->useFlagAliases)
	{
		for (i = 0; i < Conf->nspell; i++)
		{
			char	   *end;

			if (*Conf->Spell[i]->p.flag != '\0')
			{
				errno = 0;
				curaffix = strtol(Conf->Spell[i]->p.flag, &end, 10);
				if (Conf->Spell[i]->p.flag == end || errno == ERANGE)
					ereport(ERROR,
							(errcode(ERRCODE_CONFIG_FILE_ERROR),
							 errmsg("invalid affix alias \"%s\"",
									Conf->Spell[i]->p.flag)));
				if (curaffix < 0 || curaffix >= Conf->nAffixData)
					ereport(ERROR,
							(errcode(ERRCODE_CONFIG_FILE_ERROR),
							 errmsg("invalid affix alias \"%s\"",
									Conf->Spell[i]->p.flag)));
				if (*end != '\0' && !isdigit((unsigned char) *end) && !isspace((unsigned char) *end))
					ereport(ERROR,
							(errcode(ERRCODE_CONFIG_FILE_ERROR),
							 errmsg("invalid affix alias \"%s\"",
									Conf->Spell[i]->p.flag)));
			}
			else
			{
				/*
				 * If Conf->Spell[i]->p.flag is empty, then get empty value of
				 * Conf->AffixData (0 index).
				 */
				curaffix = 0;
			}

			Conf->Spell[i]->p.d.affix = curaffix;
			Conf->Spell[i]->p.d.len = strlen(Conf->Spell[i]->word);
		}
	}
	/* Otherwise fill Conf->AffixData here */
	else
	{
		/* Count the number of different flags used in the dictionary */
		qsort(Conf->Spell, Conf->nspell, sizeof(SPELL *),
			  cmpspellaffix);

		naffix = 0;
		for (i = 0; i < Conf->nspell; i++)
		{
			if (i == 0 ||
				strcmp(Conf->Spell[i]->p.flag, Conf->Spell[i - 1]->p.flag) != 0)
				naffix++;
		}

		/*
		 * Fill in Conf->AffixData with the affixes that were used in the
		 * dictionary. Replace textual flag-field of Conf->Spell entries with
		 * indexes into Conf->AffixData array.
		 */
		Conf->AffixData = (const char **) palloc0(naffix * sizeof(const char *));

		curaffix = -1;
		for (i = 0; i < Conf->nspell; i++)
		{
			if (i == 0 ||
				strcmp(Conf->Spell[i]->p.flag, Conf->AffixData[curaffix]) != 0)
			{
				curaffix++;
				Assert(curaffix < naffix);
				Conf->AffixData[curaffix] = cpstrdup(Conf,
													 Conf->Spell[i]->p.flag);
			}

			Conf->Spell[i]->p.d.affix = curaffix;
			Conf->Spell[i]->p.d.len = strlen(Conf->Spell[i]->word);
		}

		Conf->lenAffixData = Conf->nAffixData = naffix;
	}

	/* Start build a prefix tree */
	qsort(Conf->Spell, Conf->nspell, sizeof(SPELL *), cmpspell);
	Conf->Dictionary = mkSPNode(Conf, 0, Conf->nspell, 0);
}

/*
 * Makes a prefix tree for the given level using the repl string of an affix
 * rule. Affixes with empty replace string do not include in the prefix tree.
 * This affixes are included by mkVoidAffix().
 *
 * Conf: current dictionary.
 * low: lower index of the Conf->Affix array.
 * high: upper index of the Conf->Affix array.
 * level: current prefix tree level.
 * type: FF_SUFFIX or FF_PREFIX.
 */
static AffixNode *
mkANode(IspellDict *Conf, int low, int high, int level, int type)
{
	int			i;
	int			nchar = 0;
	uint8		lastchar = '\0';
	AffixNode  *rs;
	AffixNodeData *data;
	int			lownew = low;
	int			naff;
	AFFIX	  **aff;

	for (i = low; i < high; i++)
		if (Conf->Affix[i].replen > level && lastchar != GETCHAR(Conf->Affix + i, level, type))
		{
			nchar++;
			lastchar = GETCHAR(Conf->Affix + i, level, type);
		}

	if (!nchar)
		return NULL;

	aff = (AFFIX **) tmpalloc(sizeof(AFFIX *) * (high - low + 1));
	naff = 0;

	rs = (AffixNode *) cpalloc0(ANHRDSZ + nchar * sizeof(AffixNodeData));
	rs->length = nchar;
	data = rs->data;

	lastchar = '\0';
	for (i = low; i < high; i++)
		if (Conf->Affix[i].replen > level)
		{
			if (lastchar != GETCHAR(Conf->Affix + i, level, type))
			{
				if (lastchar)
				{
					/* Next level of the prefix tree */
					data->node = mkANode(Conf, lownew, i, level + 1, type);
					if (naff)
					{
						data->naff = naff;
						data->aff = (AFFIX **) cpalloc(sizeof(AFFIX *) * naff);
						memcpy(data->aff, aff, sizeof(AFFIX *) * naff);
						naff = 0;
					}
					data++;
					lownew = i;
				}
				lastchar = GETCHAR(Conf->Affix + i, level, type);
			}
			data->val = GETCHAR(Conf->Affix + i, level, type);
			if (Conf->Affix[i].replen == level + 1)
			{					/* affix stopped */
				aff[naff++] = Conf->Affix + i;
			}
		}

	/* Next level of the prefix tree */
	data->node = mkANode(Conf, lownew, high, level + 1, type);
	if (naff)
	{
		data->naff = naff;
		data->aff = (AFFIX **) cpalloc(sizeof(AFFIX *) * naff);
		memcpy(data->aff, aff, sizeof(AFFIX *) * naff);
		naff = 0;
	}

	pfree(aff);

	return rs;
}

/*
 * Makes the root void node in the prefix tree. The root void node is created
 * for affixes which have empty replace string ("repl" field).
 */
static void
mkVoidAffix(IspellDict *Conf, bool issuffix, int startsuffix)
{
	int			i,
				cnt = 0;
	int			start = (issuffix) ? startsuffix : 0;
	int			end = (issuffix) ? Conf->naffixes : startsuffix;
	AffixNode  *Affix = (AffixNode *) palloc0(ANHRDSZ + sizeof(AffixNodeData));

	Affix->length = 1;
	Affix->isvoid = 1;

	if (issuffix)
	{
		Affix->data->node = Conf->Suffix;
		Conf->Suffix = Affix;
	}
	else
	{
		Affix->data->node = Conf->Prefix;
		Conf->Prefix = Affix;
	}

	/* Count affixes with empty replace string */
	for (i = start; i < end; i++)
		if (Conf->Affix[i].replen == 0)
			cnt++;

	/* There is not affixes with empty replace string */
	if (cnt == 0)
		return;

	Affix->data->aff = (AFFIX **) cpalloc(sizeof(AFFIX *) * cnt);
	Affix->data->naff = (uint32) cnt;

	cnt = 0;
	for (i = start; i < end; i++)
		if (Conf->Affix[i].replen == 0)
		{
			Affix->data->aff[cnt] = Conf->Affix + i;
			cnt++;
		}
}

/*
 * Checks if the affixflag is used by dictionary. Conf->AffixData does not
 * contain affixflag if this flag is not used actually by the .dict file.
 *
 * Conf: current dictionary.
 * affixflag: affix flag.
 *
 * Returns true if the Conf->AffixData array contains affixflag, otherwise
 * returns false.
 */
static bool
isAffixInUse(IspellDict *Conf, const char *affixflag)
{
	int			i;

	for (i = 0; i < Conf->nAffixData; i++)
		if (IsAffixFlagInUse(Conf, i, affixflag))
			return true;

	return false;
}

/*
 * Builds Conf->Prefix and Conf->Suffix trees from the imported affixes.
 */
void
NISortAffixes(IspellDict *Conf)
{
	AFFIX	   *Affix;
	size_t		i;
	CMPDAffix  *ptr;
	int			firstsuffix = Conf->naffixes;

	if (Conf->naffixes == 0)
		return;

	/* Store compound affixes in the Conf->CompoundAffix array */
	if (Conf->naffixes > 1)
		qsort(Conf->Affix, Conf->naffixes, sizeof(AFFIX), cmpaffix);
	Conf->CompoundAffix = ptr = (CMPDAffix *) palloc(sizeof(CMPDAffix) * Conf->naffixes);
	ptr->affix = NULL;

	for (i = 0; i < Conf->naffixes; i++)
	{
		Affix = &(((AFFIX *) Conf->Affix)[i]);
		if (Affix->type == FF_SUFFIX && i < firstsuffix)
			firstsuffix = i;

		if ((Affix->flagflags & FF_COMPOUNDFLAG) && Affix->replen > 0 &&
			isAffixInUse(Conf, Affix->flag))
		{
			bool		issuffix = (Affix->type == FF_SUFFIX);

			if (ptr == Conf->CompoundAffix ||
				issuffix != (ptr - 1)->issuffix ||
				strbncmp((const unsigned char *) (ptr - 1)->affix,
						 (const unsigned char *) Affix->repl,
						 (ptr - 1)->len))
			{
				/* leave only unique and minimal suffixes */
				ptr->affix = Affix->repl;
				ptr->len = Affix->replen;
				ptr->issuffix = issuffix;
				ptr++;
			}
		}
	}
	ptr->affix = NULL;
	Conf->CompoundAffix = (CMPDAffix *) repalloc(Conf->CompoundAffix, sizeof(CMPDAffix) * (ptr - Conf->CompoundAffix + 1));

	/* Start build a prefix tree */
	Conf->Prefix = mkANode(Conf, 0, firstsuffix, 0, FF_PREFIX);
	Conf->Suffix = mkANode(Conf, firstsuffix, Conf->naffixes, 0, FF_SUFFIX);
	mkVoidAffix(Conf, true, firstsuffix);
	mkVoidAffix(Conf, false, firstsuffix);
}

static AffixNodeData *
FindAffixes(AffixNode *node, const char *word, int wrdlen, int *level, int type)
{
	AffixNodeData *StopLow,
			   *StopHigh,
			   *StopMiddle;
	uint8 symbol;

	if (node->isvoid)
	{							/* search void affixes */
		if (node->data->naff)
			return node->data;
		node = node->data->node;
	}

	while (node && *level < wrdlen)
	{
		StopLow = node->data;
		StopHigh = node->data + node->length;
		while (StopLow < StopHigh)
		{
			StopMiddle = StopLow + ((StopHigh - StopLow) >> 1);
			symbol = GETWCHAR(word, wrdlen, *level, type);

			if (StopMiddle->val == symbol)
			{
				(*level)++;
				if (StopMiddle->naff)
					return StopMiddle;
				node = StopMiddle->node;
				break;
			}
			else if (StopMiddle->val < symbol)
				StopLow = StopMiddle + 1;
			else
				StopHigh = StopMiddle;
		}
		if (StopLow >= StopHigh)
			break;
	}
	return NULL;
}

static char *
CheckAffix(const char *word, size_t len, AFFIX *Affix, int flagflags, char *newword, int *baselen)
{
	/*
	 * Check compound allow flags
	 */

	if (flagflags == 0)
	{
		if (Affix->flagflags & FF_COMPOUNDONLY)
			return NULL;
	}
	else if (flagflags & FF_COMPOUNDBEGIN)
	{
		if (Affix->flagflags & FF_COMPOUNDFORBIDFLAG)
			return NULL;
		if ((Affix->flagflags & FF_COMPOUNDBEGIN) == 0)
			if (Affix->type == FF_SUFFIX)
				return NULL;
	}
	else if (flagflags & FF_COMPOUNDMIDDLE)
	{
		if ((Affix->flagflags & FF_COMPOUNDMIDDLE) == 0 ||
			(Affix->flagflags & FF_COMPOUNDFORBIDFLAG))
			return NULL;
	}
	else if (flagflags & FF_COMPOUNDLAST)
	{
		if (Affix->flagflags & FF_COMPOUNDFORBIDFLAG)
			return NULL;
		if ((Affix->flagflags & FF_COMPOUNDLAST) == 0)
			if (Affix->type == FF_PREFIX)
				return NULL;
	}

	/*
	 * make replace pattern of affix
	 */
	if (Affix->type == FF_SUFFIX)
	{
		strcpy(newword, word);
		strcpy(newword + len - Affix->replen, Affix->find);
		if (baselen)			/* store length of non-changed part of word */
			*baselen = len - Affix->replen;
	}
	else
	{
		/*
		 * if prefix is an all non-changed part's length then all word
		 * contains only prefix and suffix, so out
		 */
		if (baselen && *baselen + strlen(Affix->find) <= Affix->replen)
			return NULL;
		strcpy(newword, Affix->find);
		strcat(newword, word + Affix->replen);
	}

	/*
	 * check resulting word
	 */
	if (Affix->issimple)
		return newword;
	else if (Affix->isregis)
	{
		if (RS_execute(&(Affix->reg.regis), newword))
			return newword;
	}
	else
	{
		pg_wchar   *data;
		size_t		data_len;
		int			newword_len;

		/* Convert data string to wide characters */
		newword_len = strlen(newword);
		data = (pg_wchar *) palloc((newword_len + 1) * sizeof(pg_wchar));
		data_len = pg_mb2wchar_with_len(newword, data, newword_len);

		if (pg_regexec(Affix->reg.pregex, data, data_len,
					   0, NULL, 0, NULL, 0) == REG_OKAY)
		{
			pfree(data);
			return newword;
		}
		pfree(data);
	}

	return NULL;
}

static int
addToResult(char **forms, char **cur, char *word)
{
	if (cur - forms >= MAX_NORM - 1)
		return 0;
	if (forms == cur || strcmp(word, *(cur - 1)) != 0)
	{
		*cur = pstrdup(word);
		*(cur + 1) = NULL;
		return 1;
	}

	return 0;
}

static char **
NormalizeSubWord(IspellDict *Conf, const char *word, int flag)
{
	AffixNodeData *suffix = NULL,
			   *prefix = NULL;
	int			slevel = 0,
				plevel = 0;
	int			wrdlen = strlen(word),
				swrdlen;
	char	  **forms;
	char	  **cur;
	char		newword[2 * MAXNORMLEN] = "";
	char		pnewword[2 * MAXNORMLEN] = "";
	AffixNode  *snode = Conf->Suffix,
			   *pnode;
	int			i,
				j;

	if (wrdlen > MAXNORMLEN)
		return NULL;
	cur = forms = (char **) palloc(MAX_NORM * sizeof(char *));
	*cur = NULL;


	/* Check that the word itself is normal form */
	if (FindWord(Conf, word, VoidString, flag))
	{
		*cur = pstrdup(word);
		cur++;
		*cur = NULL;
	}

	/* Find all other NORMAL forms of the 'word' (check only prefix) */
	pnode = Conf->Prefix;
	plevel = 0;
	while (pnode)
	{
		prefix = FindAffixes(pnode, word, wrdlen, &plevel, FF_PREFIX);
		if (!prefix)
			break;
		for (j = 0; j < prefix->naff; j++)
		{
			if (CheckAffix(word, wrdlen, prefix->aff[j], flag, newword, NULL))
			{
				/* prefix success */
				if (FindWord(Conf, newword, prefix->aff[j]->flag, flag))
					cur += addToResult(forms, cur, newword);
			}
		}
		pnode = prefix->node;
	}

	/*
	 * Find all other NORMAL forms of the 'word' (check suffix and then
	 * prefix)
	 */
	while (snode)
	{
		int			baselen = 0;

		/* find possible suffix */
		suffix = FindAffixes(snode, word, wrdlen, &slevel, FF_SUFFIX);
		if (!suffix)
			break;
		/* foreach suffix check affix */
		for (i = 0; i < suffix->naff; i++)
		{
			if (CheckAffix(word, wrdlen, suffix->aff[i], flag, newword, &baselen))
			{
				/* suffix success */
				if (FindWord(Conf, newword, suffix->aff[i]->flag, flag))
					cur += addToResult(forms, cur, newword);

				/* now we will look changed word with prefixes */
				pnode = Conf->Prefix;
				plevel = 0;
				swrdlen = strlen(newword);
				while (pnode)
				{
					prefix = FindAffixes(pnode, newword, swrdlen, &plevel, FF_PREFIX);
					if (!prefix)
						break;
					for (j = 0; j < prefix->naff; j++)
					{
						if (CheckAffix(newword, swrdlen, prefix->aff[j], flag, pnewword, &baselen))
						{
							/* prefix success */
							const char *ff = (prefix->aff[j]->flagflags & suffix->aff[i]->flagflags & FF_CROSSPRODUCT) ?
								VoidString : prefix->aff[j]->flag;

							if (FindWord(Conf, pnewword, ff, flag))
								cur += addToResult(forms, cur, pnewword);
						}
					}
					pnode = prefix->node;
				}
			}
		}

		snode = suffix->node;
	}

	if (cur == forms)
	{
		pfree(forms);
		return NULL;
	}
	return forms;
}

typedef struct SplitVar
{
	int			nstem;
	int			lenstem;
	char	  **stem;
	struct SplitVar *next;
} SplitVar;

static int
CheckCompoundAffixes(CMPDAffix **ptr, const char *word, int len, bool CheckInPlace)
{
	bool		issuffix;

	/* in case CompoundAffix is null: */
	if (*ptr == NULL)
		return -1;

	if (CheckInPlace)
	{
		while ((*ptr)->affix)
		{
			if (len > (*ptr)->len && strncmp((*ptr)->affix, word, (*ptr)->len) == 0)
			{
				len = (*ptr)->len;
				issuffix = (*ptr)->issuffix;
				(*ptr)++;
				return (issuffix) ? len : 0;
			}
			(*ptr)++;
		}
	}
	else
	{
		char	   *affbegin;

		while ((*ptr)->affix)
		{
			if (len > (*ptr)->len && (affbegin = strstr(word, (*ptr)->affix)) != NULL)
			{
				len = (*ptr)->len + (affbegin - word);
				issuffix = (*ptr)->issuffix;
				(*ptr)++;
				return (issuffix) ? len : 0;
			}
			(*ptr)++;
		}
	}
	return -1;
}

static SplitVar *
CopyVar(SplitVar *s, int makedup)
{
	SplitVar   *v = (SplitVar *) palloc(sizeof(SplitVar));

	v->next = NULL;
	if (s)
	{
		int			i;

		v->lenstem = s->lenstem;
		v->stem = (char **) palloc(sizeof(char *) * v->lenstem);
		v->nstem = s->nstem;
		for (i = 0; i < s->nstem; i++)
			v->stem[i] = (makedup) ? pstrdup(s->stem[i]) : s->stem[i];
	}
	else
	{
		v->lenstem = 16;
		v->stem = (char **) palloc(sizeof(char *) * v->lenstem);
		v->nstem = 0;
	}
	return v;
}

static void
AddStem(SplitVar *v, char *word)
{
	if (v->nstem >= v->lenstem)
	{
		v->lenstem *= 2;
		v->stem = (char **) repalloc(v->stem, sizeof(char *) * v->lenstem);
	}

	v->stem[v->nstem] = word;
	v->nstem++;
}

static SplitVar *
SplitToVariants(IspellDict *Conf, SPNode *snode, SplitVar *orig, const char *word, int wordlen, int startpos, int minpos)
{
	SplitVar   *var = NULL;
	SPNodeData *StopLow,
			   *StopHigh,
			   *StopMiddle = NULL;
	SPNode	   *node = (snode) ? snode : Conf->Dictionary;
	int			level = (snode) ? minpos : startpos;	/* recursive
														 * minpos==level */
	int			lenaff;
	CMPDAffix  *caff;
	char	   *notprobed;
	int			compoundflag = 0;

	/* since this function recurses, it could be driven to stack overflow */
	check_stack_depth();

	notprobed = (char *) palloc(wordlen);
	memset(notprobed, 1, wordlen);
	var = CopyVar(orig, 1);

	while (level < wordlen)
	{
		/* find word with epenthetic or/and compound affix */
		caff = Conf->CompoundAffix;
		while (level > startpos && (lenaff = CheckCompoundAffixes(&caff, word + level, wordlen - level, (node) ? true : false)) >= 0)
		{
			/*
			 * there is one of compound affixes, so check word for existings
			 */
			char		buf[MAXNORMLEN];
			char	  **subres;

			lenaff = level - startpos + lenaff;

			if (!notprobed[startpos + lenaff - 1])
				continue;

			if (level + lenaff - 1 <= minpos)
				continue;

			if (lenaff >= MAXNORMLEN)
				continue;		/* skip too big value */
			if (lenaff > 0)
				memcpy(buf, word + startpos, lenaff);
			buf[lenaff] = '\0';

			if (level == 0)
				compoundflag = FF_COMPOUNDBEGIN;
			else if (level == wordlen - 1)
				compoundflag = FF_COMPOUNDLAST;
			else
				compoundflag = FF_COMPOUNDMIDDLE;
			subres = NormalizeSubWord(Conf, buf, compoundflag);
			if (subres)
			{
				/* Yes, it was a word from dictionary */
				SplitVar   *new = CopyVar(var, 0);
				SplitVar   *ptr = var;
				char	  **sptr = subres;

				notprobed[startpos + lenaff - 1] = 0;

				while (*sptr)
				{
					AddStem(new, *sptr);
					sptr++;
				}
				pfree(subres);

				while (ptr->next)
					ptr = ptr->next;
				ptr->next = SplitToVariants(Conf, NULL, new, word, wordlen, startpos + lenaff, startpos + lenaff);

				pfree(new->stem);
				pfree(new);
			}
		}

		if (!node)
			break;

		StopLow = node->data;
		StopHigh = node->data + node->length;
		while (StopLow < StopHigh)
		{
			StopMiddle = StopLow + ((StopHigh - StopLow) >> 1);
			if (StopMiddle->val == ((uint8 *) (word))[level])
				break;
			else if (StopMiddle->val < ((uint8 *) (word))[level])
				StopLow = StopMiddle + 1;
			else
				StopHigh = StopMiddle;
		}

		if (StopLow < StopHigh)
		{
			if (startpos == 0)
				compoundflag = FF_COMPOUNDBEGIN;
			else if (level == wordlen - 1)
				compoundflag = FF_COMPOUNDLAST;
			else
				compoundflag = FF_COMPOUNDMIDDLE;

			/* find infinitive */
			if (StopMiddle->isword &&
				(StopMiddle->compoundflag & compoundflag) &&
				notprobed[level])
			{
				/* ok, we found full compoundallowed word */
				if (level > minpos)
				{
					/* and its length more than minimal */
					if (wordlen == level + 1)
					{
						/* well, it was last word */
						AddStem(var, pnstrdup(word + startpos, wordlen - startpos));
						pfree(notprobed);
						return var;
					}
					else
					{
						/* then we will search more big word at the same point */
						SplitVar   *ptr = var;

						while (ptr->next)
							ptr = ptr->next;
						ptr->next = SplitToVariants(Conf, node, var, word, wordlen, startpos, level);
						/* we can find next word */
						level++;
						AddStem(var, pnstrdup(word + startpos, level - startpos));
						node = Conf->Dictionary;
						startpos = level;
						continue;
					}
				}
			}
			node = StopMiddle->node;
		}
		else
			node = NULL;
		level++;
	}

	AddStem(var, pnstrdup(word + startpos, wordlen - startpos));
	pfree(notprobed);
	return var;
}

static void
addNorm(TSLexeme **lres, TSLexeme **lcur, char *word, int flags, uint16 NVariant)
{
	if (*lres == NULL)
		*lcur = *lres = (TSLexeme *) palloc(MAX_NORM * sizeof(TSLexeme));

	if (*lcur - *lres < MAX_NORM - 1)
	{
		(*lcur)->lexeme = word;
		(*lcur)->flags = flags;
		(*lcur)->nvariant = NVariant;
		(*lcur)++;
		(*lcur)->lexeme = NULL;
	}
}

TSLexeme *
NINormalizeWord(IspellDict *Conf, const char *word)
{
	char	  **res;
	TSLexeme   *lcur = NULL,
			   *lres = NULL;
	uint16		NVariant = 1;

	res = NormalizeSubWord(Conf, word, 0);

	if (res)
	{
		char	  **ptr = res;

		while (*ptr && (lcur - lres) < MAX_NORM)
		{
			addNorm(&lres, &lcur, *ptr, 0, NVariant++);
			ptr++;
		}
		pfree(res);
	}

	if (Conf->usecompound)
	{
		int			wordlen = strlen(word);
		SplitVar   *ptr,
				   *var = SplitToVariants(Conf, NULL, NULL, word, wordlen, 0, -1);
		int			i;

		while (var)
		{
			if (var->nstem > 1)
			{
				char	  **subres = NormalizeSubWord(Conf, var->stem[var->nstem - 1], FF_COMPOUNDLAST);

				if (subres)
				{
					char	  **subptr = subres;

					while (*subptr)
					{
						for (i = 0; i < var->nstem - 1; i++)
						{
							addNorm(&lres, &lcur, (subptr == subres) ? var->stem[i] : pstrdup(var->stem[i]), 0, NVariant);
						}

						addNorm(&lres, &lcur, *subptr, 0, NVariant);
						subptr++;
						NVariant++;
					}

					pfree(subres);
					var->stem[0] = NULL;
					pfree(var->stem[var->nstem - 1]);
				}
			}

			for (i = 0; i < var->nstem && var->stem[i]; i++)
				pfree(var->stem[i]);
			ptr = var->next;
			pfree(var->stem);
			pfree(var);
			var = ptr;
		}
	}

	return lres;
}

/* ======================================================================
 * SECTION D: driver entries (fuzz plumbing, NOT Postgres code)
 * ====================================================================== */

static _Thread_local IspellDict *spf_conf;
static _Thread_local TSLexeme *spf_lexres;
static _Thread_local int spf_nlex;

void
pg_spf_reset(void)
{
	size_t		i;

	/* Free the regex-engine memory that pg_regcomp malloc'd OUTSIDE the
	 * tracked arena (spell.c stores it in AFFIX.reg.pregex for the
	 * !issimple && !isregis condition arm and relies on MemoryContextDelete
	 * to reclaim it — which this TU's context shim no-ops). Without this
	 * every affix carrying a regex condition (e.g. hunspell "[^E]") leaks
	 * its compiled NFA per exec (the fleet LSan abort class). The regis and
	 * simple arms allocate only through the arena and need no engine free. */
	if (spf_conf != NULL && spf_conf->Affix != NULL)
	{
		for (i = 0; i < (size_t) spf_conf->naffixes; i++)
		{
			AFFIX	   *a = spf_conf->Affix + i;

			if (!a->issimple && !a->isregis && a->reg.pregex != NULL)
				pg_regfree(a->reg.pregex);
		}
	}

	spf_close_all_files();		/* fd.c abort-time cleanup (see AllocateFile) */

	for (i = 0; i < spf_nallocs; i++)
		free(spf_allocs[i]);	/* header base pointers */
	spf_nallocs = 0;
	spf_conf = NULL;
	spf_lexres = NULL;
	spf_nlex = 0;
	spf_errcode = 0;
	error_context_stack = NULL;
}

int
pg_spf_sqlstate(void)
{
	return spf_errcode;
}

void
pg_spf_set_db_encoding(int encoding)
{
	wfam_x_set_db_encoding(encoding);
}

/* Build the dictionary from the two files. 0 = ok, -1 = ereport (sqlstate
 * via pg_spf_sqlstate). Sequence per dispell_init with AffFile first. */
int
pg_spf_build(const char *affpath, const char *dictpath)
{
	char		base;

	spf_stack_base = &base;		/* arm the depth guard (set_stack_base) */
	if (setjmp(spf_env) != 0)
		return -1;
	spf_conf = spf_palloc0(sizeof(IspellDict));
	NIStartBuild(spf_conf);
	NIImportAffixes(spf_conf, affpath);
	/* AF-alias empty-slot normalization (DIVERGENCE-OF-RECORD, driver env
	 * shim — NOT a spell.c edit): NIImportAffixes palloc0's AffixData, so the
	 * reserved index-0 alias and any slot the file declares but never fills
	 * stay NULL. spell.c then NULL-derefs those slots at every downstream use
	 * (MergeAffix's `*AffixData[a]`, getNextFlagFromString via
	 * IsAffixFlagInUse, makeCompoundFlags) whenever a dict word or affix line
	 * references such an alias index — a verbatim-18.3 backend-crash on a
	 * malformed/edge ispell alias table (found by this differential:
	 * fleet-div-segv2). The pgrust port represents the SAME slots as EMPTY
	 * (an empty PgVec) and is robust; getAffixFlagSet's own code already
	 * treats index 0 as the empty alias. Normalize NULL slots to "" here so
	 * the differential measures the loader's real logic over the whole AF
	 * domain against the port's hardened behavior, with the raw NULL-deref
	 * recorded as the finding. Match-or-fix ruling owed (likely upstream). */
	{
		int			i;

		for (i = 0; i < spf_conf->nAffixData; i++)
			if (spf_conf->AffixData[i] == NULL)
				spf_conf->AffixData[i] = "";
	}
	NIImportDictionary(spf_conf, dictpath);
	NISortDictionary(spf_conf);
	NISortAffixes(spf_conf);
	NIFinishBuild(spf_conf);
	return 0;
}

/* Structural planes over the built dictionary */
int
pg_spf_naffixes(void)
{
	return spf_conf->naffixes;
}

int
pg_spf_naffixdata(void)
{
	return spf_conf->nAffixData;
}

const char *
pg_spf_affixdata(int i)
{
	return spf_conf->AffixData[i];
}

int
pg_spf_usecompound(void)
{
	return spf_conf->usecompound ? 1 : 0;
}

int
pg_spf_flagmode(void)
{
	return (int) spf_conf->flagMode;
}

int
pg_spf_ncompound(void)
{
	int			n = 0;

	if (spf_conf->CompoundAffix == NULL)
		return 0;

	/* BOUNDED walk — do NOT trust C's NULL terminator alone (UPSTREAM OOB
	 * WRITE, spell.c:1987 vs :2015): NISortAffixes pallocs the CompoundAffix
	 * array with exactly `naffixes` elements, but writes its terminator at
	 * `ptr` AFTER the collection loop and only THEN repalloc's to
	 * (collected + 1). When every affix is collected (collected == naffixes)
	 * that terminator write lands ONE ELEMENT PAST THE END of the palloc'd
	 * array, and the subsequent repalloc — which legitimately copies only the
	 * old size — drops it, leaving heap garbage where the terminator should
	 * be. An unbounded walk then runs off into that garbage (observed:
	 * naffixes==1 yielding ncomp=137 then 109 across repeats — the C-side
	 * nondeterminism this decode leg caught).
	 *
	 * `collected <= naffixes` always, so bounding the scan by naffixes is
	 * exact in BOTH cases: if collected < naffixes the terminator is
	 * in-bounds and survives, so the scan stops on it; if collected ==
	 * naffixes the terminator was the lost OOB one and the bound itself is
	 * the correct count. This keeps the surface COMPARED (not carved) and the
	 * oracle deterministic, without editing the verbatim C. */
	while (n < spf_conf->naffixes && spf_conf->CompoundAffix[n].affix != NULL)
		n++;
	return n;
}

const char *
pg_spf_compound(int i, int *len, int *issuffix)
{
	CMPDAffix  *a = spf_conf->CompoundAffix + i;

	*len = a->len;
	*issuffix = a->issuffix ? 1 : 0;
	return a->affix;
}

/* Normalize one word. Returns lexeme count (0 = none), -1 = ereport. */
int
pg_spf_normalize(const char *word, int len)
{
	char	   *w;
	TSLexeme   *res;
	int			n = 0;
	char		base;

	spf_stack_base = &base;		/* arm the depth guard (set_stack_base) */
	if (setjmp(spf_env) != 0)
		return -1;
	w = spf_palloc(len + 1);
	memcpy(w, word, len);
	w[len] = '\0';
	res = NINormalizeWord(spf_conf, w);
	spf_lexres = res;
	if (res)
		for (n = 0; res[n].lexeme != NULL; n++)
			 /* skip */ ;
	spf_nlex = n;
	return n;
}

const char *
pg_spf_lex(int i, int *nvariant, int *flags)
{
	*nvariant = spf_lexres[i].nvariant;
	*flags = spf_lexres[i].flags;
	return spf_lexres[i].lexeme;
}
