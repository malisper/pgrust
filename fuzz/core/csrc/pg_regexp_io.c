/*
 * pg_regexp_io.c: vendored PostgreSQL C oracle for the regexp_diff differential
 * fuzz target (100%-coverage campaign; crate crates/backend/utils/adt/regexp).
 *
 * Provenance — all bodies VERBATIM unless a shim is listed below, from the
 * repo's vendored ground-truth checkout ../pgrust-fabled/vendor/postgres-src
 * @ 62d6c7d3df6287f1bd83199c1a746e50d31571a0 (PostgreSQL 18.3, Stamp-18.3):
 *   - src/backend/utils/adt/regexp.c: pg_re_flags, regexp_matches_ctx,
 *     RE_wchar_execute, RE_execute, RE_compile_and_execute, parse_re_flags,
 *     the textregexeq/textregexne/texticregexeq/texticregexne/nameregexeq/
 *     nameregexne/nameicregexeq/nameicregexne cores, textregexsubstr,
 *     similar_escape_internal (+ its three SQL faces),
 *     setup_regexp_matches, build_regexp_match_result,
 *     build_regexp_split_result, the regexp_count/instr/like/substr/
 *     match/matches/split_to_array cores, and regexp_fixed_prefix (its
 *     engine half, pg_regprefix, is the verbatim vendored
 *     csrc/regexfam/regprefix.c).
 *   - src/backend/utils/adt/varlena.c: charlen_to_bytelen,
 *     check_replace_text_has_escape, appendStringInfoRegexpSubstr,
 *     appendStringInfoText, replace_text_regexp (the regexp_replace-family
 *     oracle).
 *   - The regex ENGINE (pg_regcomp/pg_regexec/pg_regerror/pg_regfree and
 *     everything they #include) is vendored VERBATIM in csrc/regexfam/
 *     (own shim include tree; see csrc/regexfam/postgres.h and
 *     pg_regexfam_glue.c headers for the engine-side shims: malloc-family
 *     allocator, interrupts pinned off, stack depth pinned shallow, UTF-8
 *     encoding pin, C-locale strategy via pinned C_COLLATION_OID).
 *
 * Shims in THIS file (plumbing only, never logic):
 *   - RE_compile_and_cache -> compile-per-call (THE CACHE IS CARVED, per the
 *     lane charter: the cache runs on the Rust side, we don't assert on
 *     cache behavior).  The compile path itself (mb2wchar conversion,
 *     pg_regcomp, pg_regerror + ereport on failure) is the verbatim core of
 *     the real function minus the cache lookup/insert and memory-context
 *     bookkeeping.  The compiled regex_t lives in a TLS slot freed at the
 *     next compile, so an ereport-longjmp between compile and use cannot
 *     leak engine memory.
 *   - fmgr plumbing: PG_FUNCTION_ARGS shells are unwrapped to plain
 *     (ptr,len) C signatures `pg_diff_regexp_*`; PG_NARGS() > k tests for
 *     optional parameters become explicit has_* flags; PG_GET_COLLATION()
 *     is pinned to C_COLLATION_OID (both sides of the differential pin
 *     collation 950); PG_RETURN_* become out-parameters.
 *   - text: all oracle texts are inline 4-byte-header varlenas built by
 *     cstring_to_text_with_len below (no short/toasted forms exist inside
 *     the oracle), so VARDATA_ANY==VARDATA and VARSIZE_ANY_EXHDR==len.
 *   - DirectFunctionCall3(text_substr, s, so+1, eo-so) [textregexsubstr,
 *     regexp_substr, and the (dead under the UTF-8 pin) eml==1 arms of the
 *     match/split result builders] -> pg_diff_text_substr_chars(): the
 *     (so..eo) character slice of s via the verbatim charlen_to_bytelen.
 *     This is fmgr result plumbing, not wrapper logic; so/eo are already in
 *     range by construction (they came from the match engine).
 *   - construct_md_array / accumArrayResult / SRF machinery -> flat
 *     (ptr,len,isnull) lists handed back to the Rust comparator (fmgr/array
 *     result plumbing carve).  regexp_matches' per-row SRF loop IS the
 *     per-match core loop, driven directly (as_matches=1).
 *   - ereport(ERROR, (errcode(X), ...)) -> record the errcode class in the
 *     shared TLS pg_diff_errcode and longjmp out; errmsg/errhint arguments
 *     are swallowed unevaluated (message text out of scope).  elog(ERROR)
 *     -> internal-error class 6.  Errcode classes below.
 *   - palloc/palloc0/repalloc/pfree -> growable TLS pointer arena (models
 *     PG's memory-context reset; every pg_diff_* entry resets it first, so
 *     error-path longjmps cannot leak — the 2026-07-31 LSan incident class).
 *     The fixed-size scaffold arena was made growable: the match/split
 *     result loops allocate O(nmatches) texts (shim infrastructure, not
 *     vendored code).
 *   - StringInfo -> arena-backed grow buffer (lib/stringinfo.c plumbing;
 *     append semantics identical).
 *   - CHECK_FOR_INTERRUPTS() -> no-op (CANCEL_REQUESTED pinned false, task
 *     carve); MaxAllocSize = 0x3fffffff (verbatim value).
 *
 * Errcode classes (PG_DIFF_REGEXP_ERR_*):
 *   1 = ERRCODE_INVALID_REGULAR_EXPRESSION      (2201B)
 *   2 = ERRCODE_INVALID_PARAMETER_VALUE         (22023)
 *   3 = ERRCODE_INVALID_ESCAPE_SEQUENCE         (22025)
 *   4 = ERRCODE_INVALID_USE_OF_ESCAPE_CHARACTER (2200C)
 *   5 = ERRCODE_PROGRAM_LIMIT_EXCEEDED          (54000)
 *   6 = internal elog(ERROR)                    (XX000)
 */

#include <setjmp.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Rename the regex.h extern prototypes for the two wrapper functions this
 * TU pastes as statics (regex.h declares the real backend's exports; the
 * oracle's versions are TU-local).  Must precede the include. */
#define RE_compile_and_cache pg_diff_RE_compile_and_cache_impl
#define RE_compile_and_execute pg_diff_RE_compile_and_execute_impl

/* engine public header (vendored verbatim; pulls the regexfam shim tree) */
#include "regex/regex.h"
#include "catalog/pg_collation.h"

/* the regexfam postgres.h shim defines an abort()ing ereport for the engine
 * TUs; this wrapper TU replaces it with the errcode-capturing one below */
#undef ereport
#undef errcode
#undef errmsg
#undef errhint

/* ---- shared error plane (defined in csrc/pg_float_io.c) ---- */
extern _Thread_local int pg_diff_errcode;
int			pg_diff_errcode_get(void);

#define PG_DIFF_REGEXP_ERR_INVALID_RE 1
#define PG_DIFF_REGEXP_ERR_INVALID_PARAM 2
#define PG_DIFF_REGEXP_ERR_INVALID_ESCAPE 3
#define PG_DIFF_REGEXP_ERR_ESCAPE_CHAR 4
#define PG_DIFF_REGEXP_ERR_LIMIT 5
#define PG_DIFF_REGEXP_ERR_INTERNAL 6

/* errcode names used by the pasted code, mapped to class constants */
#define ERRCODE_INVALID_REGULAR_EXPRESSION PG_DIFF_REGEXP_ERR_INVALID_RE
#define ERRCODE_INVALID_PARAMETER_VALUE PG_DIFF_REGEXP_ERR_INVALID_PARAM
#define ERRCODE_INVALID_ESCAPE_SEQUENCE PG_DIFF_REGEXP_ERR_INVALID_ESCAPE
#define ERRCODE_INVALID_USE_OF_ESCAPE_CHARACTER PG_DIFF_REGEXP_ERR_ESCAPE_CHAR
#define ERRCODE_PROGRAM_LIMIT_EXCEEDED PG_DIFF_REGEXP_ERR_LIMIT

static _Thread_local jmp_buf pg_regexp_jmp;

static void
pg_regexp_ereport_fire(void)
{
	longjmp(pg_regexp_jmp, 1);
}

/* errcode(X) records the class; errmsg/errhint swallow their argument lists
 * unevaluated (translator plumbing).  ereport evaluates its rest-list (which
 * runs the errcode recorder) and longjmps. */
static int
pg_regexp_seterrcode(int code)
{
	pg_diff_errcode = code;
	return 0;
}

#define errcode(sqlerrcode) pg_regexp_seterrcode(sqlerrcode)
#define errmsg(...) 0
#define errhint(...) 0
#define ereport(elevel, rest) \
	do { (void) (rest); pg_regexp_ereport_fire(); } while (0)
#define elog(elevel, ...) \
	do { pg_regexp_seterrcode(PG_DIFF_REGEXP_ERR_INTERNAL); pg_regexp_ereport_fire(); } while (0)

#define CHECK_FOR_INTERRUPTS() ((void) 0)
#define MaxAllocSize ((size_t) 0x3fffffff)	/* verbatim: memutils.h */
#define PG_GET_COLLATION() C_COLLATION_OID	/* collation pin, see header */
#define lengthof(array) (sizeof(array) / sizeof((array)[0]))

/* ================= growable TLS palloc arena (shim; see header) ========= */

static _Thread_local void **pg_regexp_arena;
static _Thread_local int pg_regexp_arena_n;
static _Thread_local int pg_regexp_arena_cap;

static void
pg_diff_arena_reset(void)
{
	int			i;

	for (i = 0; i < pg_regexp_arena_n; i++)
		free(pg_regexp_arena[i]);
	pg_regexp_arena_n = 0;
}

static void
pg_regexp_arena_track(void *p)
{
	if (!p)
		abort();
	if (pg_regexp_arena_n >= pg_regexp_arena_cap)
	{
		pg_regexp_arena_cap = pg_regexp_arena_cap ? pg_regexp_arena_cap * 2 : 64;
		pg_regexp_arena = realloc(pg_regexp_arena,
								  pg_regexp_arena_cap * sizeof(void *));
		if (!pg_regexp_arena)
			abort();
	}
	pg_regexp_arena[pg_regexp_arena_n++] = p;
}

static void *
pg_regexp_palloc_impl(size_t n)
{
	void	   *p = malloc(n ? n : 1);

	pg_regexp_arena_track(p);
	return p;
}

static void *
pg_regexp_palloc0_impl(size_t n)
{
	void	   *p = calloc(1, n ? n : 1);

	pg_regexp_arena_track(p);
	return p;
}

static void *
pg_regexp_repalloc_impl(void *old, size_t n)
{
	int			i;

	for (i = 0; i < pg_regexp_arena_n; i++)
	{
		if (pg_regexp_arena[i] == old)
		{
			void	   *p = realloc(old, n);

			if (!p)
				abort();
			pg_regexp_arena[i] = p;
			return p;
		}
	}
	assert(!"repalloc of a pointer the arena never issued");
	abort();
}

static void
pg_regexp_pfree_impl(void *p)
{
	int			i;

	for (i = 0; i < pg_regexp_arena_n; i++)
	{
		if (pg_regexp_arena[i] == p)
		{
			free(p);
			pg_regexp_arena[i] = pg_regexp_arena[--pg_regexp_arena_n];
			return;
		}
	}
	/* abort-loud: freeing a pointer the arena never issued is a shim bug */
	assert(!"pfree of a pointer the arena never issued");
	abort();
}

/* macro overrides shadow the regexfam postgres.h inline pfree at the pasted
 * call sites in THIS TU only; the engine TUs keep their malloc-family shim */
#define palloc(n) pg_regexp_palloc_impl(n)
#define palloc0(n) pg_regexp_palloc0_impl(n)
#define repalloc(p, n) pg_regexp_repalloc_impl((p), (n))
#define pfree(p) pg_regexp_pfree_impl(p)

/* ================= text / Datum shims (see header) ====================== */

/* `text` itself is typedef'd in the regexfam postgres.h shim (regex.h needs
 * it); the varlena accessor macros live here, the only TU that uses them. */
#define VARHDRSZ ((int) sizeof(uint32))
/* 4-byte little-endian varlena length word, as on every supported host */
#define SET_VARSIZE(PTR, len) (((text *) (PTR))->vl_len_ = ((uint32) (len)) << 2)
#define VARSIZE(PTR) ((int) (((text *) (PTR))->vl_len_ >> 2))
#define VARDATA(PTR) (((text *) (PTR))->vl_dat)
#define VARSIZE_ANY(PTR) VARSIZE(PTR)
#define VARDATA_ANY(PTR) VARDATA(PTR)
#define VARSIZE_ANY_EXHDR(PTR) (VARSIZE(PTR) - VARHDRSZ)

typedef uintptr_t Datum;

#define PointerGetDatum(X) ((Datum) (X))
#define DatumGetPointer(X) ((void *) (X))

static text *
cstring_to_text_with_len(const char *s, int len)
{
	text	   *result = (text *) palloc(len + VARHDRSZ);

	SET_VARSIZE(result, len + VARHDRSZ);
	memcpy(VARDATA(result), s, len);

	return result;
}

/* driver-input text construction (fmgr arg plumbing) */
static text *
pg_diff_regexp_text(const unsigned char *ptr, int len)
{
	return cstring_to_text_with_len((const char *) ptr, len);
}

/* ---- src/backend/utils/adt/varlena.c: charlen_to_bytelen (VERBATIM;
 * pg_mblen_unbounded resolved to pg_mblen under the UTF-8 pin — both are
 * the encoding table's mblen row; see pg_regexfam_glue.c) ---- */
static int
charlen_to_bytelen(const char *p, int n)
{
	if (pg_database_encoding_max_length() == 1)
	{
		/* Optimization for single-byte encodings */
		return n;
	}
	else
	{
		const char *s;

		for (s = p; n > 0; n--)
			s += pg_mblen(s);	/* caller verified encoding */

		return s - p;
	}
}

/* SHIM (fmgr result plumbing; see header): the character slice
 * DirectFunctionCall3(text_substr, s, so+1, eo-so) produces. */
static text *
pg_diff_text_substr_chars(text *s, int so, int eo)
{
	char	   *sdata = VARDATA_ANY(s);
	int			b0 = charlen_to_bytelen(sdata, so);
	int			blen = charlen_to_bytelen(sdata + b0, eo - so);

	return cstring_to_text_with_len(sdata + b0, blen);
}

/* ================= StringInfo shim (plumbing; see header) =============== */

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
	if (str->len + datalen + 1 > str->maxlen)
	{
		while (str->len + datalen + 1 > str->maxlen)
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

/* ---- varlena.c appendStringInfoText (VERBATIM body) ---- */
static void
appendStringInfoText(StringInfo str, const text *t)
{
	appendBinaryStringInfo(str, VARDATA_ANY(t), VARSIZE_ANY_EXHDR(t));
}

/* ==================== SECTION 1: regexp.c (VERBATIM) ==================== */

/* all the options of interest for regex functions */
typedef struct pg_re_flags
{
	int			cflags;			/* compile flags for Spencer's regex code */
	bool		glob;			/* do it globally (for each occurrence) */
} pg_re_flags;

/* cross-call state for regexp_match and regexp_split functions */
typedef struct regexp_matches_ctx
{
	text	   *orig_str;		/* data string in original TEXT form */
	int			nmatches;		/* number of places where pattern matched */
	int			npatterns;		/* number of capturing subpatterns */
	/* We store start char index and end+1 char index for each match */
	/* so the number of entries in match_locs is nmatches * npatterns * 2 */
	int		   *match_locs;		/* 0-based character indexes */
	int			next_match;		/* 0-based index of next match to process */
	/* workspace for build_regexp_match_result() */
	Datum	   *elems;			/* has npatterns elements */
	bool	   *nulls;			/* has npatterns elements */
	pg_wchar   *wide_str;		/* wide-char version of original string */
	char	   *conv_buf;		/* conversion buffer, if needed */
	int			conv_bufsiz;	/* size thereof */
} regexp_matches_ctx;

/*
 * RE_compile_and_cache — SHIM: cache carved, compile-per-call (see file
 * header).  The conversion + compile + error-report core is verbatim from
 * the real function; the cache lookup/insert and memory-context management
 * are removed.  The compiled regex_t lives in a TLS slot freed at the next
 * compile so ereport-longjmp exits cannot leak engine memory.
 */
static _Thread_local regex_t pg_regexp_live_re;
static _Thread_local bool pg_regexp_live;

regex_t *
RE_compile_and_cache(text *text_re, int cflags, Oid collation)
{
	int			text_re_len = VARSIZE_ANY_EXHDR(text_re);
	char	   *text_re_val = VARDATA_ANY(text_re);
	pg_wchar   *pattern;
	int			pattern_len;
	int			regcomp_result;
	char		errMsg[100];

	/* SHIM: free the previous per-call compile (cache carve) */
	if (pg_regexp_live)
	{
		pg_regfree(&pg_regexp_live_re);
		pg_regexp_live = false;
	}

	/* Convert pattern string to wide characters */
	pattern = (pg_wchar *) palloc((text_re_len + 1) * sizeof(pg_wchar));
	pattern_len = pg_mb2wchar_with_len(text_re_val,
									   pattern,
									   text_re_len);

	regcomp_result = pg_regcomp(&pg_regexp_live_re,
								pattern,
								pattern_len,
								cflags,
								collation);

	pfree(pattern);

	if (regcomp_result != REG_OKAY)
	{
		/* re didn't compile (no need for pg_regfree, if so) */
		pg_regerror(regcomp_result, &pg_regexp_live_re, errMsg, sizeof(errMsg));
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_REGULAR_EXPRESSION),
				 errmsg("invalid regular expression: %s", errMsg)));
	}

	pg_regexp_live = true;
	return &pg_regexp_live_re;
}

/*
 * RE_wchar_execute - execute a RE on pg_wchar data (VERBATIM)
 */
static bool
RE_wchar_execute(regex_t *re, pg_wchar *data, int data_len,
				 int start_search, int nmatch, regmatch_t *pmatch)
{
	int			regexec_result;
	char		errMsg[100];

	/* Perform RE match and return result */
	regexec_result = pg_regexec(re,
								data,
								data_len,
								start_search,
								NULL,	/* no details */
								nmatch,
								pmatch,
								0);

	if (regexec_result != REG_OKAY && regexec_result != REG_NOMATCH)
	{
		/* re failed??? */
		pg_regerror(regexec_result, re, errMsg, sizeof(errMsg));
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_REGULAR_EXPRESSION),
				 errmsg("regular expression failed: %s", errMsg)));
	}

	return (regexec_result == REG_OKAY);
}

/*
 * RE_execute - execute a RE (VERBATIM)
 */
static bool
RE_execute(regex_t *re, char *dat, int dat_len,
		   int nmatch, regmatch_t *pmatch)
{
	pg_wchar   *data;
	int			data_len;
	bool		match;

	/* Convert data string to wide characters */
	data = (pg_wchar *) palloc((dat_len + 1) * sizeof(pg_wchar));
	data_len = pg_mb2wchar_with_len(dat, data, dat_len);

	/* Perform RE match and return result */
	match = RE_wchar_execute(re, data, data_len, 0, nmatch, pmatch);

	pfree(data);
	return match;
}

/*
 * RE_compile_and_execute - compile and execute a RE (VERBATIM)
 */
bool
RE_compile_and_execute(text *text_re, char *dat, int dat_len,
					   int cflags, Oid collation,
					   int nmatch, regmatch_t *pmatch)
{
	regex_t    *re;

	/* Use REG_NOSUB if caller does not want sub-match details */
	if (nmatch < 2)
		cflags |= REG_NOSUB;

	/* Compile RE */
	re = RE_compile_and_cache(text_re, cflags, collation);

	return RE_execute(re, dat, dat_len, nmatch, pmatch);
}

/*
 * parse_re_flags (VERBATIM)
 */
static void
parse_re_flags(pg_re_flags *flags, text *opts)
{
	/* regex flavor is always folded into the compile flags */
	flags->cflags = REG_ADVANCED;
	flags->glob = false;

	if (opts)
	{
		char	   *opt_p = VARDATA_ANY(opts);
		int			opt_len = VARSIZE_ANY_EXHDR(opts);
		int			i;

		for (i = 0; i < opt_len; i++)
		{
			switch (opt_p[i])
			{
				case 'g':
					flags->glob = true;
					break;
				case 'b':		/* BREs (but why???) */
					flags->cflags &= ~(REG_ADVANCED | REG_EXTENDED | REG_QUOTE);
					break;
				case 'c':		/* case sensitive */
					flags->cflags &= ~REG_ICASE;
					break;
				case 'e':		/* plain EREs */
					flags->cflags |= REG_EXTENDED;
					flags->cflags &= ~(REG_ADVANCED | REG_QUOTE);
					break;
				case 'i':		/* case insensitive */
					flags->cflags |= REG_ICASE;
					break;
				case 'm':		/* Perloid synonym for n */
				case 'n':		/* \n affects ^ $ . [^ */
					flags->cflags |= REG_NEWLINE;
					break;
				case 'p':		/* ~Perl, \n affects . [^ */
					flags->cflags |= REG_NLSTOP;
					flags->cflags &= ~REG_NLANCH;
					break;
				case 'q':		/* literal string */
					flags->cflags |= REG_QUOTE;
					flags->cflags &= ~(REG_ADVANCED | REG_EXTENDED);
					break;
				case 's':		/* single line, \n ordinary */
					flags->cflags &= ~REG_NEWLINE;
					break;
				case 't':		/* tight syntax */
					flags->cflags &= ~REG_EXPANDED;
					break;
				case 'w':		/* weird, \n affects ^ $ only */
					flags->cflags &= ~REG_NLSTOP;
					flags->cflags |= REG_NLANCH;
					break;
				case 'x':		/* expanded syntax */
					flags->cflags |= REG_EXPANDED;
					break;
				default:
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("invalid regular expression option: \"%.*s\"",
									pg_mblen_range(opt_p + i, opt_p + opt_len), opt_p + i)));
					break;
			}
		}
	}
}

/*
 * similar_escape_internal (VERBATIM)
 */
static text *
similar_escape_internal(text *pat_text, text *esc_text)
{
	text	   *result;
	char	   *p,
			   *e,
			   *r;
	int			plen,
				elen;
	const char *pend;
	bool		afterescape = false;
	int			nquotes = 0;
	int			bracket_depth = 0;	/* square bracket nesting level */
	int			charclass_pos = 0;	/* position inside a character class */

	p = VARDATA_ANY(pat_text);
	plen = VARSIZE_ANY_EXHDR(pat_text);
	pend = p + plen;
	if (esc_text == NULL)
	{
		/* No ESCAPE clause provided; default to backslash as escape */
		e = "\\";
		elen = 1;
	}
	else
	{
		e = VARDATA_ANY(esc_text);
		elen = VARSIZE_ANY_EXHDR(esc_text);
		if (elen == 0)
			e = NULL;			/* no escape character */
		else if (elen > 1)
		{
			int			escape_mblen = pg_mbstrlen_with_len(e, elen);

			if (escape_mblen > 1)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_ESCAPE_SEQUENCE),
						 errmsg("invalid escape string"),
						 errhint("Escape string must be empty or one character.")));
		}
	}

	/* (upstream comment block elided in paste; behavior identical) */

	/*
	 * We need room for the prefix/postfix and part separators, plus as many
	 * as 3 output bytes per input byte; since the input is at most 1GB this
	 * can't overflow size_t.
	 */
	result = (text *) palloc(VARHDRSZ + 23 + 3 * (size_t) plen);
	r = VARDATA(result);

	*r++ = '^';
	*r++ = '(';
	*r++ = '?';
	*r++ = ':';

	while (plen > 0)
	{
		char		pchar = *p;

		/*
		 * If both the escape character and the current character from the
		 * pattern are multi-byte, we need to take the slow path.
		 *
		 * But if one of them is single-byte, we can process the pattern one
		 * byte at a time, ignoring multi-byte characters.  (This works
		 * because all server-encodings have the property that a valid
		 * multi-byte character representation cannot contain the
		 * representation of a valid single-byte character.)
		 */

		if (elen > 1)
		{
			int			mblen = pg_mblen_range(p, pend);

			if (mblen > 1)
			{
				/* slow, multi-byte path */
				if (afterescape)
				{
					*r++ = '\\';
					memcpy(r, p, mblen);
					r += mblen;
					afterescape = false;
				}
				else if (e && elen == mblen && memcmp(e, p, mblen) == 0)
				{
					/* SQL escape character; do not send to output */
					afterescape = true;
				}
				else
				{
					/*
					 * We know it's a multi-byte character, so we don't need
					 * to do all the comparisons to single-byte characters
					 * that we do below.
					 */
					memcpy(r, p, mblen);
					r += mblen;
				}

				p += mblen;
				plen -= mblen;

				continue;
			}
		}

		/* fast path */
		if (afterescape)
		{
			if (pchar == '"' && bracket_depth < 1)	/* escape-double-quote? */
			{
				/* emit appropriate part separator, per notes above */
				if (nquotes == 0)
				{
					*r++ = ')';
					*r++ = '{';
					*r++ = '1';
					*r++ = ',';
					*r++ = '1';
					*r++ = '}';
					*r++ = '?';
					*r++ = '(';
				}
				else if (nquotes == 1)
				{
					*r++ = ')';
					*r++ = '{';
					*r++ = '1';
					*r++ = ',';
					*r++ = '1';
					*r++ = '}';
					*r++ = '(';
					*r++ = '?';
					*r++ = ':';
				}
				else
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_USE_OF_ESCAPE_CHARACTER),
							 errmsg("SQL regular expression may not contain more than two escape-double-quote separators")));
				nquotes++;
			}
			else
			{
				/*
				 * We allow any character at all to be escaped; notably, this
				 * allows access to POSIX character-class escapes such as
				 * "\d".  The SQL spec is considerably more restrictive.
				 */
				*r++ = '\\';
				*r++ = pchar;

				/*
				 * If we encounter an escaped character in a character class,
				 * we are no longer at the beginning.
				 */
				charclass_pos = 3;
			}
			afterescape = false;
		}
		else if (e && pchar == *e)
		{
			/* SQL escape character; do not send to output */
			afterescape = true;
		}
		else if (bracket_depth > 0)
		{
			/* inside a character class */
			if (pchar == '\\')
			{
				/*
				 * If we're here, backslash is not the SQL escape character,
				 * so treat it as a literal class element, which requires
				 * doubling it.  (This matches our behavior for backslashes
				 * outside character classes.)
				 */
				*r++ = '\\';
			}
			*r++ = pchar;

			/* parse the character class well enough to identify ending ']' */
			if (pchar == ']' && charclass_pos > 2)
			{
				/* found the real end of a bracket pair */
				bracket_depth--;
				/* don't reset charclass_pos, this may be an inner bracket */
			}
			else if (pchar == '[')
			{
				/* start of a nested bracket pair */
				bracket_depth++;

				/*
				 * We are no longer at the beginning of a character class.
				 * (The nested bracket pair is a collating element, not a
				 * character class in its own right.)
				 */
				charclass_pos = 3;
			}
			else if (pchar == '^')
			{
				/*
				 * A caret right after the opening bracket negates the
				 * character class.  In that case, the following will
				 * increment charclass_pos from 1 to 2, so that a following
				 * ']' is still a literal character and does not end the
				 * character class.  If we are further inside a character
				 * class, charclass_pos might get incremented past 3, which is
				 * fine.
				 */
				charclass_pos++;
			}
			else
			{
				/*
				 * Anything else (including a backslash or leading ']') is an
				 * element of the character class, so we are no longer at the
				 * beginning of the class.
				 */
				charclass_pos = 3;
			}
		}
		else if (pchar == '[')
		{
			/* start of a character class */
			*r++ = pchar;
			bracket_depth = 1;
			charclass_pos = 1;
		}
		else if (pchar == '%')
		{
			*r++ = '.';
			*r++ = '*';
		}
		else if (pchar == '_')
			*r++ = '.';
		else if (pchar == '(')
		{
			/* convert to non-capturing parenthesis */
			*r++ = '(';
			*r++ = '?';
			*r++ = ':';
		}
		else if (pchar == '\\' || pchar == '.' ||
				 pchar == '^' || pchar == '$')
		{
			*r++ = '\\';
			*r++ = pchar;
		}
		else
			*r++ = pchar;
		p++, plen--;
	}

	*r++ = ')';
	*r++ = '$';

	SET_VARSIZE(result, r - ((char *) result));

	return result;
}

/*
 * setup_regexp_matches (VERBATIM; fetching_unmatched retained)
 */
static regexp_matches_ctx *
setup_regexp_matches(text *orig_str, text *pattern, pg_re_flags *re_flags,
					 int start_search,
					 Oid collation,
					 bool use_subpatterns,
					 bool ignore_degenerate,
					 bool fetching_unmatched)
{
	regexp_matches_ctx *matchctx = palloc0(sizeof(regexp_matches_ctx));
	int			eml = pg_database_encoding_max_length();
	int			orig_len;
	pg_wchar   *wide_str;
	int			wide_len;
	int			cflags;
	regex_t    *cpattern;
	regmatch_t *pmatch;
	int			pmatch_len;
	int			array_len;
	int			array_idx;
	int			prev_match_end;
	int			prev_valid_match_end;
	int			maxlen = 0;		/* largest fetch length in characters */

	/* save original string --- we'll extract result substrings from it */
	matchctx->orig_str = orig_str;

	/* convert string to pg_wchar form for matching */
	orig_len = VARSIZE_ANY_EXHDR(orig_str);
	wide_str = (pg_wchar *) palloc(sizeof(pg_wchar) * (orig_len + 1));
	wide_len = pg_mb2wchar_with_len(VARDATA_ANY(orig_str), wide_str, orig_len);

	/* set up the compiled pattern */
	cflags = re_flags->cflags;
	if (!use_subpatterns)
		cflags |= REG_NOSUB;
	cpattern = RE_compile_and_cache(pattern, cflags, collation);

	/* do we want to remember subpatterns? */
	if (use_subpatterns && cpattern->re_nsub > 0)
	{
		matchctx->npatterns = cpattern->re_nsub;
		pmatch_len = cpattern->re_nsub + 1;
	}
	else
	{
		use_subpatterns = false;
		matchctx->npatterns = 1;
		pmatch_len = 1;
	}

	/* temporary output space for RE package */
	pmatch = palloc(sizeof(regmatch_t) * pmatch_len);

	/*
	 * the real output space (grown dynamically if needed)
	 *
	 * use values 2^n-1, not 2^n, so that we hit the limit at 2^28-1 rather
	 * than at 2^27
	 */
	array_len = re_flags->glob ? 255 : 31;
	matchctx->match_locs = (int *) palloc(sizeof(int) * array_len);
	array_idx = 0;

	/* search for the pattern, perhaps repeatedly */
	prev_match_end = 0;
	prev_valid_match_end = 0;
	while (RE_wchar_execute(cpattern, wide_str, wide_len, start_search,
							pmatch_len, pmatch))
	{
		/*
		 * If requested, ignore degenerate matches, which are zero-length
		 * matches occurring at the start or end of a string or just after a
		 * previous match.
		 */
		if (!ignore_degenerate ||
			(pmatch[0].rm_so < wide_len &&
			 pmatch[0].rm_eo > prev_match_end))
		{
			/* enlarge output space if needed */
			while (array_idx + matchctx->npatterns * 2 + 1 > array_len)
			{
				array_len += array_len + 1; /* 2^n-1 => 2^(n+1)-1 */
				if (array_len > MaxAllocSize / sizeof(int))
					ereport(ERROR,
							(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
							 errmsg("too many regular expression matches")));
				matchctx->match_locs = (int *) repalloc(matchctx->match_locs,
														sizeof(int) * array_len);
			}

			/* save this match's locations */
			if (use_subpatterns)
			{
				int			i;

				for (i = 1; i <= matchctx->npatterns; i++)
				{
					int			so = pmatch[i].rm_so;
					int			eo = pmatch[i].rm_eo;

					matchctx->match_locs[array_idx++] = so;
					matchctx->match_locs[array_idx++] = eo;
					if (so >= 0 && eo >= 0 && (eo - so) > maxlen)
						maxlen = (eo - so);
				}
			}
			else
			{
				int			so = pmatch[0].rm_so;
				int			eo = pmatch[0].rm_eo;

				matchctx->match_locs[array_idx++] = so;
				matchctx->match_locs[array_idx++] = eo;
				if (so >= 0 && eo >= 0 && (eo - so) > maxlen)
					maxlen = (eo - so);
			}
			matchctx->nmatches++;

			/*
			 * check length of unmatched portion between end of previous valid
			 * (nondegenerate, or degenerate but not ignored) match and start
			 * of current one
			 */
			if (fetching_unmatched &&
				pmatch[0].rm_so >= 0 &&
				(pmatch[0].rm_so - prev_valid_match_end) > maxlen)
				maxlen = (pmatch[0].rm_so - prev_valid_match_end);
			prev_valid_match_end = pmatch[0].rm_eo;
		}
		prev_match_end = pmatch[0].rm_eo;

		/* if not glob, stop after one match */
		if (!re_flags->glob)
			break;

		/*
		 * Advance search position.  Normally we start the next search at the
		 * end of the previous match; but if the match was of zero length, we
		 * have to advance by one character, or we'd just find the same match
		 * again.
		 */
		start_search = prev_match_end;
		if (pmatch[0].rm_so == pmatch[0].rm_eo)
			start_search++;
		if (start_search > wide_len)
			break;
	}

	/*
	 * check length of unmatched portion between end of last match and end of
	 * input string
	 */
	if (fetching_unmatched &&
		(wide_len - prev_valid_match_end) > maxlen)
		maxlen = (wide_len - prev_valid_match_end);

	/*
	 * Keep a note of the end position of the string for the benefit of
	 * splitting code.
	 */
	matchctx->match_locs[array_idx] = wide_len;

	if (eml > 1)
	{
		int64		maxsiz = eml * (int64) maxlen;
		int			conv_bufsiz;

		/*
		 * Make the conversion buffer large enough for any substring of
		 * interest.
		 *
		 * Worst case: assume we need the maximum size (maxlen*eml), but take
		 * advantage of the fact that the original string length in bytes is
		 * an upper bound on the byte length of any fetched substring (and we
		 * know that len+1 is safe to allocate because the varlena header is
		 * longer than 1 byte).
		 */
		if (maxsiz > orig_len)
			conv_bufsiz = orig_len + 1;
		else
			conv_bufsiz = maxsiz + 1;	/* safe since maxsiz < 2^30 */

		matchctx->conv_buf = palloc(conv_bufsiz);
		matchctx->conv_bufsiz = conv_bufsiz;
		matchctx->wide_str = wide_str;
	}
	else
	{
		/* No need to keep the wide string if we're in a single-byte charset. */
		pfree(wide_str);
		matchctx->wide_str = NULL;
		matchctx->conv_buf = NULL;
		matchctx->conv_bufsiz = 0;
	}

	/* Clean up temp storage */
	pfree(pmatch);

	return matchctx;
}

/*
 * build_regexp_match_result (VERBATIM loop; the trailing construct_md_array
 * call is fmgr/array result plumbing and is carved — the filled elems/nulls
 * workspace is what the driver entries hand to the Rust comparator)
 */
static void
build_regexp_match_result(regexp_matches_ctx *matchctx)
{
	char	   *buf = matchctx->conv_buf;
	Datum	   *elems = matchctx->elems;
	bool	   *nulls = matchctx->nulls;
	int			loc;
	int			i;

	/* Extract matching substrings from the original string */
	loc = matchctx->next_match * matchctx->npatterns * 2;
	for (i = 0; i < matchctx->npatterns; i++)
	{
		int			so = matchctx->match_locs[loc++];
		int			eo = matchctx->match_locs[loc++];

		if (so < 0 || eo < 0)
		{
			elems[i] = (Datum) 0;
			nulls[i] = true;
		}
		else if (buf)
		{
			int			len = pg_wchar2mb_with_len(matchctx->wide_str + so,
												   buf,
												   eo - so);

			assert(len < matchctx->conv_bufsiz);
			elems[i] = PointerGetDatum(cstring_to_text_with_len(buf, len));
			nulls[i] = false;
		}
		else
		{
			/* eml==1 arm, dead under the UTF-8 pin (text_substr shim) */
			elems[i] = PointerGetDatum(pg_diff_text_substr_chars(matchctx->orig_str, so, eo));
			nulls[i] = false;
		}
	}
}

/*
 * build_regexp_split_result (VERBATIM; Datum result = text pointer)
 */
static Datum
build_regexp_split_result(regexp_matches_ctx *splitctx)
{
	char	   *buf = splitctx->conv_buf;
	int			startpos;
	int			endpos;

	if (splitctx->next_match > 0)
		startpos = splitctx->match_locs[splitctx->next_match * 2 - 1];
	else
		startpos = 0;
	if (startpos < 0)
		elog(ERROR, "invalid match ending position");

	endpos = splitctx->match_locs[splitctx->next_match * 2];
	if (endpos < startpos)
		elog(ERROR, "invalid match starting position");

	if (buf)
	{
		int			len;

		len = pg_wchar2mb_with_len(splitctx->wide_str + startpos,
								   buf,
								   endpos - startpos);
		assert(len < splitctx->conv_bufsiz);
		return PointerGetDatum(cstring_to_text_with_len(buf, len));
	}
	else
	{
		/* eml==1 arm, dead under the UTF-8 pin (text_substr shim) */
		return PointerGetDatum(pg_diff_text_substr_chars(splitctx->orig_str,
														 startpos, endpos));
	}
}

/* ============ SECTION 2: varlena.c replace_text_regexp (VERBATIM) ======= */

/*
 * check_replace_text_has_escape (VERBATIM)
 */
static int
check_replace_text_has_escape(const text *replace_text)
{
	int			result = 0;
	const char *p = VARDATA_ANY(replace_text);
	const char *p_end = p + VARSIZE_ANY_EXHDR(replace_text);

	while (p < p_end)
	{
		/* Find next escape char, if any. */
		p = memchr(p, '\\', p_end - p);
		if (p == NULL)
			break;
		p++;
		/* Note: a backslash at the end doesn't require extra processing. */
		if (p < p_end)
		{
			if (*p >= '1' && *p <= '9')
				return 2;		/* Found a submatch specifier, so done */
			result = 1;			/* Found some other sequence, keep looking */
			p++;
		}
	}
	return result;
}

/*
 * appendStringInfoRegexpSubstr (VERBATIM)
 */
static void
appendStringInfoRegexpSubstr(StringInfo str, text *replace_text,
							 regmatch_t *pmatch,
							 char *start_ptr, int data_pos)
{
	const char *p = VARDATA_ANY(replace_text);
	const char *p_end = p + VARSIZE_ANY_EXHDR(replace_text);

	while (p < p_end)
	{
		const char *chunk_start = p;
		int			so;
		int			eo;

		/* Find next escape char, if any. */
		p = memchr(p, '\\', p_end - p);
		if (p == NULL)
			p = p_end;

		/* Copy the text we just scanned over, if any. */
		if (p > chunk_start)
			appendBinaryStringInfo(str, chunk_start, p - chunk_start);

		/* Done if at end of string, else advance over escape char. */
		if (p >= p_end)
			break;
		p++;

		if (p >= p_end)
		{
			/* Escape at very end of input.  Treat same as unexpected char */
			appendStringInfoChar(str, '\\');
			break;
		}

		if (*p >= '1' && *p <= '9')
		{
			/* Use the back reference of regexp. */
			int			idx = *p - '0';

			so = pmatch[idx].rm_so;
			eo = pmatch[idx].rm_eo;
			p++;
		}
		else if (*p == '&')
		{
			/* Use the entire matched string. */
			so = pmatch[0].rm_so;
			eo = pmatch[0].rm_eo;
			p++;
		}
		else if (*p == '\\')
		{
			/* \\ means transfer one \ to output. */
			appendStringInfoChar(str, '\\');
			p++;
			continue;
		}
		else
		{
			/*
			 * If escape char is not followed by any expected char, just treat
			 * it as ordinary data to copy.  (XXX would it be better to throw
			 * an error?)
			 */
			appendStringInfoChar(str, '\\');
			continue;
		}

		if (so >= 0 && eo >= 0)
		{
			/*
			 * Copy the text that is back reference of regexp.  Note so and eo
			 * are counted in characters not bytes.
			 */
			char	   *chunk_start;
			int			chunk_len;

			assert(so >= data_pos);
			chunk_start = start_ptr;
			chunk_start += charlen_to_bytelen(chunk_start, so - data_pos);
			chunk_len = charlen_to_bytelen(chunk_start, eo - so);
			appendBinaryStringInfo(str, chunk_start, chunk_len);
		}
	}
}

/*
 * replace_text_regexp (VERBATIM)
 */
static text *
replace_text_regexp(text *src_text, text *pattern_text,
					text *replace_text,
					int cflags, Oid collation,
					int search_start, int n)
{
	text	   *ret_text;
	regex_t    *re;
	int			src_text_len = VARSIZE_ANY_EXHDR(src_text);
	int			nmatches = 0;
	StringInfoData buf;
	regmatch_t	pmatch[10];		/* main match, plus \1 to \9 */
	int			nmatch = lengthof(pmatch);
	pg_wchar   *data;
	size_t		data_len;
	int			data_pos;
	char	   *start_ptr;
	int			escape_status;

	initStringInfo(&buf);

	/* Convert data string to wide characters. */
	data = (pg_wchar *) palloc((src_text_len + 1) * sizeof(pg_wchar));
	data_len = pg_mb2wchar_with_len(VARDATA_ANY(src_text), data, src_text_len);

	/* Check whether replace_text has escapes, especially regexp submatches. */
	escape_status = check_replace_text_has_escape(replace_text);

	/* If no regexp submatches, we can use REG_NOSUB. */
	if (escape_status < 2)
	{
		cflags |= REG_NOSUB;
		/* Also tell pg_regexec we only want the whole-match location. */
		nmatch = 1;
	}

	/* Prepare the regexp. */
	re = RE_compile_and_cache(pattern_text, cflags, collation);

	/* start_ptr points to the data_pos'th character of src_text */
	start_ptr = (char *) VARDATA_ANY(src_text);
	data_pos = 0;

	while (search_start <= data_len)
	{
		int			regexec_result;

		CHECK_FOR_INTERRUPTS();

		regexec_result = pg_regexec(re,
									data,
									data_len,
									search_start,
									NULL,	/* no details */
									nmatch,
									pmatch,
									0);

		if (regexec_result == REG_NOMATCH)
			break;

		if (regexec_result != REG_OKAY)
		{
			char		errMsg[100];

			pg_regerror(regexec_result, re, errMsg, sizeof(errMsg));
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_REGULAR_EXPRESSION),
					 errmsg("regular expression failed: %s", errMsg)));
		}

		/*
		 * Count matches, and decide whether to replace this match.
		 */
		nmatches++;
		if (n > 0 && nmatches != n)
		{
			/*
			 * No, so advance search_start, but not start_ptr/data_pos. (Thus,
			 * we treat the matched text as if it weren't matched, and copy it
			 * to the output later.)
			 */
			search_start = pmatch[0].rm_eo;
			if (pmatch[0].rm_so == pmatch[0].rm_eo)
				search_start++;
			continue;
		}

		/*
		 * Copy the text to the left of the match position.  Note we are given
		 * character not byte indexes.
		 */
		if (pmatch[0].rm_so - data_pos > 0)
		{
			int			chunk_len;

			chunk_len = charlen_to_bytelen(start_ptr,
										   pmatch[0].rm_so - data_pos);
			appendBinaryStringInfo(&buf, start_ptr, chunk_len);

			/*
			 * Advance start_ptr over that text, to avoid multiple rescans of
			 * it if the replace_text contains multiple back-references.
			 */
			start_ptr += chunk_len;
			data_pos = pmatch[0].rm_so;
		}

		/*
		 * Copy the replace_text, processing escapes if any are present.
		 */
		if (escape_status > 0)
			appendStringInfoRegexpSubstr(&buf, replace_text, pmatch,
										 start_ptr, data_pos);
		else
			appendStringInfoText(&buf, replace_text);

		/* Advance start_ptr and data_pos over the matched text. */
		start_ptr += charlen_to_bytelen(start_ptr,
										pmatch[0].rm_eo - data_pos);
		data_pos = pmatch[0].rm_eo;

		/*
		 * If we only want to replace one occurrence, we're done.
		 */
		if (n > 0)
			break;

		/*
		 * Advance search position.  Normally we start the next search at the
		 * end of the previous match; but if the match was of zero length, we
		 * have to advance by one character, or we'd just find the same match
		 * again.
		 */
		search_start = data_pos;
		if (pmatch[0].rm_so == pmatch[0].rm_eo)
			search_start++;
	}

	/*
	 * Copy the text to the right of the last match.
	 */
	if (data_pos < data_len)
	{
		int			chunk_len;

		chunk_len = ((char *) src_text + VARSIZE_ANY(src_text)) - start_ptr;
		appendBinaryStringInfo(&buf, start_ptr, chunk_len);
	}

	ret_text = cstring_to_text_with_len(buf.data, buf.len);
	pfree(buf.data);
	pfree(data);

	return ret_text;
}

/* ========== SECTION 3: fuzz-facing driver entries (NOT Postgres code) ==== */

/*
 * Entry prologue: arena reset (models PG's memory-context reset), errcode
 * clear, and the ereport longjmp catch frame.  A nonzero return is the
 * errcode class of the ereport that fired.
 */
#define PG_DIFF_REGEXP_ENTRY_BEGIN() \
	do { \
		pg_diff_arena_reset(); \
		pg_diff_errcode = 0; \
		if (setjmp(pg_regexp_jmp)) \
			return pg_diff_errcode; \
	} while (0)

/* text out-parameter writeback: the pointed-at bytes live in the TLS arena,
 * valid until the next pg_diff_regexp_* call on this thread */
static void
pg_diff_regexp_out_text(text *t, const unsigned char **out, int *outlen)
{
	*out = (const unsigned char *) VARDATA_ANY(t);
	*outlen = VARSIZE_ANY_EXHDR(t);
}

/* --- parse_re_flags [regexp.c] --------------------------------------- */
int
pg_diff_parse_re_flags(const unsigned char *opts, int optlen, int has_opts,
					   int *out_cflags, int *out_glob)
{
	pg_re_flags flags;
	text	   *opts_text;

	PG_DIFF_REGEXP_ENTRY_BEGIN();
	opts_text = has_opts ? pg_diff_regexp_text(opts, optlen) : NULL;
	parse_re_flags(&flags, opts_text);
	*out_cflags = flags.cflags;
	*out_glob = flags.glob ? 1 : 0;
	return 0;
}

/* --- textregexeq/ne, texticregexeq/ne, nameregexeq/ne [regexp.c] ------
 * The six fmgr shells are one-line calls over RE_compile_and_execute; the
 * verbatim call shapes (flags, negation) are reproduced per entry.  The
 * name variants' NameStr/strlen unwrap to (ptr,len): the driver enforces
 * NUL-free inputs, so len == strlen. */
int
pg_diff_textregexeq(const unsigned char *s, int slen,
					const unsigned char *p, int plen, int *out)
{
	text	   *pt;

	PG_DIFF_REGEXP_ENTRY_BEGIN();
	pt = pg_diff_regexp_text(p, plen);
	*out = RE_compile_and_execute(pt, (char *) s, slen,
								  REG_ADVANCED, PG_GET_COLLATION(), 0, NULL);
	return 0;
}

int
pg_diff_textregexne(const unsigned char *s, int slen,
					const unsigned char *p, int plen, int *out)
{
	text	   *pt;

	PG_DIFF_REGEXP_ENTRY_BEGIN();
	pt = pg_diff_regexp_text(p, plen);
	*out = !RE_compile_and_execute(pt, (char *) s, slen,
								   REG_ADVANCED, PG_GET_COLLATION(), 0, NULL);
	return 0;
}

int
pg_diff_texticregexeq(const unsigned char *s, int slen,
					  const unsigned char *p, int plen, int *out)
{
	text	   *pt;

	PG_DIFF_REGEXP_ENTRY_BEGIN();
	pt = pg_diff_regexp_text(p, plen);
	*out = RE_compile_and_execute(pt, (char *) s, slen,
								  REG_ADVANCED | REG_ICASE, PG_GET_COLLATION(), 0, NULL);
	return 0;
}

int
pg_diff_texticregexne(const unsigned char *s, int slen,
					  const unsigned char *p, int plen, int *out)
{
	text	   *pt;

	PG_DIFF_REGEXP_ENTRY_BEGIN();
	pt = pg_diff_regexp_text(p, plen);
	*out = !RE_compile_and_execute(pt, (char *) s, slen,
								   REG_ADVANCED | REG_ICASE, PG_GET_COLLATION(), 0, NULL);
	return 0;
}

int
pg_diff_nameregexeq(const unsigned char *n, int nlen,
					const unsigned char *p, int plen, int *out)
{
	text	   *pt;

	PG_DIFF_REGEXP_ENTRY_BEGIN();
	pt = pg_diff_regexp_text(p, plen);
	*out = RE_compile_and_execute(pt, (char *) n, nlen,
								  REG_ADVANCED, PG_GET_COLLATION(), 0, NULL);
	return 0;
}

int
pg_diff_nameregexne(const unsigned char *n, int nlen,
					const unsigned char *p, int plen, int *out)
{
	text	   *pt;

	PG_DIFF_REGEXP_ENTRY_BEGIN();
	pt = pg_diff_regexp_text(p, plen);
	*out = !RE_compile_and_execute(pt, (char *) n, nlen,
								   REG_ADVANCED, PG_GET_COLLATION(), 0, NULL);
	return 0;
}

/* --- nameicregexeq/ne [regexp.c oids 1240/1241]: verbatim call shapes
 * (REG_ADVANCED | REG_ICASE over NameStr/strlen, unwrapped to (ptr,len)
 * exactly as the case-sensitive name entries above). */
int
pg_diff_nameicregexeq(const unsigned char *n, int nlen,
					  const unsigned char *p, int plen, int *out)
{
	text	   *pt;

	PG_DIFF_REGEXP_ENTRY_BEGIN();
	pt = pg_diff_regexp_text(p, plen);
	*out = RE_compile_and_execute(pt, (char *) n, nlen,
								  REG_ADVANCED | REG_ICASE, PG_GET_COLLATION(), 0, NULL);
	return 0;
}

int
pg_diff_nameicregexne(const unsigned char *n, int nlen,
					  const unsigned char *p, int plen, int *out)
{
	text	   *pt;

	PG_DIFF_REGEXP_ENTRY_BEGIN();
	pt = pg_diff_regexp_text(p, plen);
	*out = !RE_compile_and_execute(pt, (char *) n, nlen,
								   REG_ADVANCED | REG_ICASE, PG_GET_COLLATION(), 0, NULL);
	return 0;
}

/* --- similar_escape family [regexp.c] ---------------------------------
 * One entry covers all three SQL faces: pat_isnull models the non-strict
 * legacy similar_escape(NULL, ...) -> NULL; has_esc=0 models the 1-arg
 * form and the legacy NULL escape (esc_text == NULL -> default escape). */
int
pg_diff_similar_escape(const unsigned char *pat, int patlen, int pat_isnull,
					   const unsigned char *esc, int esclen, int has_esc,
					   const unsigned char **out, int *outlen, int *out_isnull)
{
	text	   *pat_text;
	text	   *esc_text;
	text	   *result;

	PG_DIFF_REGEXP_ENTRY_BEGIN();
	if (pat_isnull)
	{
		*out_isnull = 1;
		return 0;
	}
	pat_text = pg_diff_regexp_text(pat, patlen);
	esc_text = has_esc ? pg_diff_regexp_text(esc, esclen) : NULL;
	result = similar_escape_internal(pat_text, esc_text);
	*out_isnull = 0;
	pg_diff_regexp_out_text(result, out, outlen);
	return 0;
}

/* --- textregexsubstr [regexp.c oid 2073] (body VERBATIM through the so/eo
 * selection; text_substr result plumbing shimmed, see header) ---------- */
int
pg_diff_textregexsubstr(const unsigned char *sb, int slen,
						const unsigned char *pb, int plen,
						const unsigned char **out, int *outlen, int *out_isnull)
{
	text	   *s;
	text	   *p;
	regex_t    *re;
	regmatch_t	pmatch[2];
	int			so,
				eo;

	PG_DIFF_REGEXP_ENTRY_BEGIN();
	s = pg_diff_regexp_text(sb, slen);
	p = pg_diff_regexp_text(pb, plen);

	/* Compile RE */
	re = RE_compile_and_cache(p, REG_ADVANCED, PG_GET_COLLATION());

	if (!RE_execute(re,
					VARDATA_ANY(s), VARSIZE_ANY_EXHDR(s),
					2, pmatch))
	{
		*out_isnull = 1;		/* definitely no match */
		return 0;
	}

	if (re->re_nsub > 0)
	{
		/* has parenthesized subexpressions, use the first one */
		so = pmatch[1].rm_so;
		eo = pmatch[1].rm_eo;
	}
	else
	{
		/* no parenthesized subexpression, use whole match */
		so = pmatch[0].rm_so;
		eo = pmatch[0].rm_eo;
	}

	/*
	 * It is possible to have a match to the whole pattern but no match for a
	 * subexpression; for example 'foo(bar)?' is considered to match 'foo' but
	 * there is no subexpression match.  So this extra test for match failure
	 * is not redundant.
	 */
	if (so < 0 || eo < 0)
	{
		*out_isnull = 1;
		return 0;
	}

	*out_isnull = 0;
	pg_diff_regexp_out_text(pg_diff_text_substr_chars(s, so, eo), out, outlen);
	return 0;
}

/* --- textregexreplace_noopt [oid 2284] -------------------------------- */
int
pg_diff_textregexreplace_noopt(const unsigned char *sb, int slen,
							   const unsigned char *pb, int plen,
							   const unsigned char *rb, int rlen,
							   const unsigned char **out, int *outlen)
{
	text	   *s;
	text	   *p;
	text	   *r;

	PG_DIFF_REGEXP_ENTRY_BEGIN();
	s = pg_diff_regexp_text(sb, slen);
	p = pg_diff_regexp_text(pb, plen);
	r = pg_diff_regexp_text(rb, rlen);
	pg_diff_regexp_out_text(replace_text_regexp(s, p, r,
												REG_ADVANCED, PG_GET_COLLATION(),
												0, 1),
							out, outlen);
	return 0;
}

/* --- textregexreplace [oid 2285] (body VERBATIM incl. the numeric-looking
 * flags HINT check; the HINT itself is message text, out of scope) ----- */
int
pg_diff_textregexreplace(const unsigned char *sb, int slen,
						 const unsigned char *pb, int plen,
						 const unsigned char *rb, int rlen,
						 const unsigned char *ob, int olen,
						 const unsigned char **out, int *outlen)
{
	text	   *s;
	text	   *p;
	text	   *r;
	text	   *opt;
	pg_re_flags flags;

	PG_DIFF_REGEXP_ENTRY_BEGIN();
	s = pg_diff_regexp_text(sb, slen);
	p = pg_diff_regexp_text(pb, plen);
	r = pg_diff_regexp_text(rb, rlen);
	opt = pg_diff_regexp_text(ob, olen);

	if (VARSIZE_ANY_EXHDR(opt) > 0)
	{
		char	   *opt_p = VARDATA_ANY(opt);
		const char *end_p = opt_p + VARSIZE_ANY_EXHDR(opt);

		if (*opt_p >= '0' && *opt_p <= '9')
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid regular expression option: \"%.*s\"",
							pg_mblen_range(opt_p, end_p), opt_p),
					 errhint("If you meant to use regexp_replace() with a start parameter, cast the fourth argument to integer explicitly.")));
	}

	parse_re_flags(&flags, opt);

	pg_diff_regexp_out_text(replace_text_regexp(s, p, r,
												flags.cflags, PG_GET_COLLATION(),
												0, flags.glob ? 0 : 1),
							out, outlen);
	return 0;
}

/* --- textregexreplace_extended [oids 6251/6252/6253] (PG_NARGS() > k ->
 * has_* flags, fmgr shim) --------------------------------------------- */
int
pg_diff_textregexreplace_extended(const unsigned char *sb, int slen,
								  const unsigned char *pb, int plen,
								  const unsigned char *rb, int rlen,
								  int start_arg, int has_start,
								  int n_arg, int has_n,
								  const unsigned char *fb, int flen, int has_flags,
								  const unsigned char **out, int *outlen)
{
	text	   *s;
	text	   *p;
	text	   *r;
	int			start = 1;
	int			n = 1;
	text	   *flags;
	pg_re_flags re_flags;

	PG_DIFF_REGEXP_ENTRY_BEGIN();
	s = pg_diff_regexp_text(sb, slen);
	p = pg_diff_regexp_text(pb, plen);
	r = pg_diff_regexp_text(rb, rlen);
	flags = has_flags ? pg_diff_regexp_text(fb, flen) : NULL;

	/* Collect optional parameters */
	if (has_start)
	{
		start = start_arg;
		if (start <= 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid value for parameter \"%s\": %d",
							"start", start)));
	}
	if (has_n)
	{
		n = n_arg;
		if (n < 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid value for parameter \"%s\": %d",
							"n", n)));
	}

	/* Determine options */
	parse_re_flags(&re_flags, flags);

	/* If N was not specified, deduce it from the 'g' flag */
	if (!has_n)
		n = re_flags.glob ? 0 : 1;

	/* Do the replacement(s) */
	pg_diff_regexp_out_text(replace_text_regexp(s, p, r,
												re_flags.cflags, PG_GET_COLLATION(),
												start - 1, n),
							out, outlen);
	return 0;
}

/* --- regexp_count [oids 6254/6255/6256] -------------------------------- */
int
pg_diff_regexp_count(const unsigned char *sb, int slen,
					 const unsigned char *pb, int plen,
					 int start_arg, int has_start,
					 const unsigned char *fb, int flen, int has_flags,
					 int *out)
{
	text	   *str;
	text	   *pattern;
	int			start = 1;
	text	   *flags;
	pg_re_flags re_flags;
	regexp_matches_ctx *matchctx;

	PG_DIFF_REGEXP_ENTRY_BEGIN();
	str = pg_diff_regexp_text(sb, slen);
	pattern = pg_diff_regexp_text(pb, plen);
	flags = has_flags ? pg_diff_regexp_text(fb, flen) : NULL;

	/* Collect optional parameters */
	if (has_start)
	{
		start = start_arg;
		if (start <= 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid value for parameter \"%s\": %d",
							"start", start)));
	}

	/* Determine options */
	parse_re_flags(&re_flags, flags);
	/* User mustn't specify 'g' */
	if (re_flags.glob)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("%s does not support the \"global\" option",
						"regexp_count()")));
	/* But we find all the matches anyway */
	re_flags.glob = true;

	/* Do the matching */
	matchctx = setup_regexp_matches(str, pattern, &re_flags, start - 1,
									PG_GET_COLLATION(),
									false,	/* can ignore subexprs */
									false, false);

	*out = matchctx->nmatches;
	return 0;
}

/* --- regexp_instr [oids 6257..6262] ------------------------------------ */
int
pg_diff_regexp_instr(const unsigned char *sb, int slen,
					 const unsigned char *pb, int plen,
					 int start_arg, int has_start,
					 int n_arg, int has_n,
					 int endoption_arg, int has_endoption,
					 const unsigned char *fb, int flen, int has_flags,
					 int subexpr_arg, int has_subexpr,
					 int *out)
{
	text	   *str;
	text	   *pattern;
	int			start = 1;
	int			n = 1;
	int			endoption = 0;
	text	   *flags;
	int			subexpr = 0;
	int			pos;
	pg_re_flags re_flags;
	regexp_matches_ctx *matchctx;

	PG_DIFF_REGEXP_ENTRY_BEGIN();
	str = pg_diff_regexp_text(sb, slen);
	pattern = pg_diff_regexp_text(pb, plen);
	flags = has_flags ? pg_diff_regexp_text(fb, flen) : NULL;

	/* Collect optional parameters */
	if (has_start)
	{
		start = start_arg;
		if (start <= 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid value for parameter \"%s\": %d",
							"start", start)));
	}
	if (has_n)
	{
		n = n_arg;
		if (n <= 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid value for parameter \"%s\": %d",
							"n", n)));
	}
	if (has_endoption)
	{
		endoption = endoption_arg;
		if (endoption != 0 && endoption != 1)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid value for parameter \"%s\": %d",
							"endoption", endoption)));
	}
	if (has_subexpr)
	{
		subexpr = subexpr_arg;
		if (subexpr < 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid value for parameter \"%s\": %d",
							"subexpr", subexpr)));
	}

	/* Determine options */
	parse_re_flags(&re_flags, flags);
	/* User mustn't specify 'g' */
	if (re_flags.glob)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("%s does not support the \"global\" option",
						"regexp_instr()")));
	/* But we find all the matches anyway */
	re_flags.glob = true;

	/* Do the matching */
	matchctx = setup_regexp_matches(str, pattern, &re_flags, start - 1,
									PG_GET_COLLATION(),
									(subexpr > 0),	/* need submatches? */
									false, false);

	/* When n exceeds matches return 0 (includes case of no matches) */
	if (n > matchctx->nmatches)
	{
		*out = 0;
		return 0;
	}

	/* When subexpr exceeds number of subexpressions return 0 */
	if (subexpr > matchctx->npatterns)
	{
		*out = 0;
		return 0;
	}

	/* Select the appropriate match position to return */
	pos = (n - 1) * matchctx->npatterns;
	if (subexpr > 0)
		pos += subexpr - 1;
	pos *= 2;
	if (endoption == 1)
		pos += 1;

	if (matchctx->match_locs[pos] >= 0)
		*out = matchctx->match_locs[pos] + 1;
	else
		*out = 0;				/* position not identifiable */
	return 0;
}

/* --- regexp_like [oids 6263/6264] --------------------------------------- */
int
pg_diff_regexp_like(const unsigned char *sb, int slen,
					const unsigned char *pb, int plen,
					const unsigned char *fb, int flen, int has_flags,
					int *out)
{
	text	   *str;
	text	   *pattern;
	text	   *flags;
	pg_re_flags re_flags;

	PG_DIFF_REGEXP_ENTRY_BEGIN();
	str = pg_diff_regexp_text(sb, slen);
	pattern = pg_diff_regexp_text(pb, plen);
	flags = has_flags ? pg_diff_regexp_text(fb, flen) : NULL;

	/* Determine options */
	parse_re_flags(&re_flags, flags);
	/* User mustn't specify 'g' */
	if (re_flags.glob)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("%s does not support the \"global\" option",
						"regexp_like()")));

	/* Otherwise it's like textregexeq/texticregexeq */
	*out = RE_compile_and_execute(pattern,
								  VARDATA_ANY(str),
								  VARSIZE_ANY_EXHDR(str),
								  re_flags.cflags,
								  PG_GET_COLLATION(),
								  0, NULL);
	return 0;
}

/* --- regexp_substr [oids 6265..6269] ------------------------------------ */
int
pg_diff_regexp_substr(const unsigned char *sb, int slen,
					  const unsigned char *pb, int plen,
					  int start_arg, int has_start,
					  int n_arg, int has_n,
					  const unsigned char *fb, int flen, int has_flags,
					  int subexpr_arg, int has_subexpr,
					  const unsigned char **out, int *outlen, int *out_isnull)
{
	text	   *str;
	text	   *pattern;
	int			start = 1;
	int			n = 1;
	text	   *flags;
	int			subexpr = 0;
	int			so,
				eo,
				pos;
	pg_re_flags re_flags;
	regexp_matches_ctx *matchctx;

	PG_DIFF_REGEXP_ENTRY_BEGIN();
	str = pg_diff_regexp_text(sb, slen);
	pattern = pg_diff_regexp_text(pb, plen);
	flags = has_flags ? pg_diff_regexp_text(fb, flen) : NULL;

	/* Collect optional parameters */
	if (has_start)
	{
		start = start_arg;
		if (start <= 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid value for parameter \"%s\": %d",
							"start", start)));
	}
	if (has_n)
	{
		n = n_arg;
		if (n <= 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid value for parameter \"%s\": %d",
							"n", n)));
	}
	if (has_subexpr)
	{
		subexpr = subexpr_arg;
		if (subexpr < 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid value for parameter \"%s\": %d",
							"subexpr", subexpr)));
	}

	/* Determine options */
	parse_re_flags(&re_flags, flags);
	/* User mustn't specify 'g' */
	if (re_flags.glob)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("%s does not support the \"global\" option",
						"regexp_substr()")));
	/* But we find all the matches anyway */
	re_flags.glob = true;

	/* Do the matching */
	matchctx = setup_regexp_matches(str, pattern, &re_flags, start - 1,
									PG_GET_COLLATION(),
									(subexpr > 0),	/* need submatches? */
									false, false);

	/* When n exceeds matches return NULL (includes case of no matches) */
	if (n > matchctx->nmatches)
	{
		*out_isnull = 1;
		return 0;
	}

	/* When subexpr exceeds number of subexpressions return NULL */
	if (subexpr > matchctx->npatterns)
	{
		*out_isnull = 1;
		return 0;
	}

	/* Select the appropriate match position to return */
	pos = (n - 1) * matchctx->npatterns;
	if (subexpr > 0)
		pos += subexpr - 1;
	pos *= 2;
	so = matchctx->match_locs[pos];
	eo = matchctx->match_locs[pos + 1];

	if (so < 0 || eo < 0)
	{
		*out_isnull = 1;		/* unidentifiable location */
		return 0;
	}

	*out_isnull = 0;
	pg_diff_regexp_out_text(pg_diff_text_substr_chars(str, so, eo), out, outlen);
	return 0;
}

/* --- regexp_match [oids 3396/3397] and regexp_matches [2763/2764] -------
 * as_matches=0: regexp_match core (glob rejected; 0 or 1 rows).
 * as_matches=1: regexp_matches semantics (glob allowed; the SRF per-row
 * loop driven directly — that IS what regexp_matches does per row).
 * Row-major flat output in the TLS arena: rows*cols lens (-1 = SQL NULL)
 * and byte pointers, valid until the next pg_diff_regexp_* call. */
int
pg_diff_regexp_match(const unsigned char *sb, int slen,
					 const unsigned char *pb, int plen,
					 const unsigned char *fb, int flen, int has_flags,
					 int as_matches,
					 int *out_nrows, int *out_ncols,
					 const unsigned char *const **out_ptrs,
					 const int **out_lens)
{
	text	   *orig_str;
	text	   *pattern;
	text	   *flags;
	pg_re_flags re_flags;
	regexp_matches_ctx *matchctx;
	const unsigned char **ptrs;
	int		   *lens;
	int			row;
	int			i;

	PG_DIFF_REGEXP_ENTRY_BEGIN();
	orig_str = pg_diff_regexp_text(sb, slen);
	pattern = pg_diff_regexp_text(pb, plen);
	flags = has_flags ? pg_diff_regexp_text(fb, flen) : NULL;

	/* Determine options */
	parse_re_flags(&re_flags, flags);
	if (!as_matches)
	{
		/* User mustn't specify 'g' */
		if (re_flags.glob)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("%s does not support the \"global\" option",
							"regexp_match()"),
					 errhint("Use the regexp_matches function instead.")));
	}

	matchctx = setup_regexp_matches(orig_str, pattern, &re_flags, 0,
									PG_GET_COLLATION(), true, false, false);

	if (!as_matches && matchctx->nmatches == 0)
	{
		*out_nrows = 0;
		*out_ncols = matchctx->npatterns;
		return 0;
	}
	if (!as_matches)
		assert(matchctx->nmatches == 1);

	/* Create workspace that build_regexp_match_result needs */
	matchctx->elems = (Datum *) palloc(sizeof(Datum) * matchctx->npatterns);
	matchctx->nulls = (bool *) palloc(sizeof(bool) * matchctx->npatterns);

	ptrs = palloc(sizeof(unsigned char *) *
				  (size_t) matchctx->nmatches * matchctx->npatterns);
	lens = palloc(sizeof(int) *
				  (size_t) matchctx->nmatches * matchctx->npatterns);

	for (row = 0; row < matchctx->nmatches; row++)
	{
		matchctx->next_match = row;
		build_regexp_match_result(matchctx);
		for (i = 0; i < matchctx->npatterns; i++)
		{
			int			idx = row * matchctx->npatterns + i;

			if (matchctx->nulls[i])
			{
				ptrs[idx] = NULL;
				lens[idx] = -1;
			}
			else
			{
				text	   *t = (text *) DatumGetPointer(matchctx->elems[i]);

				ptrs[idx] = (const unsigned char *) VARDATA_ANY(t);
				lens[idx] = VARSIZE_ANY_EXHDR(t);
			}
		}
	}

	*out_nrows = matchctx->nmatches;
	*out_ncols = matchctx->npatterns;
	*out_ptrs = (const unsigned char *const *) ptrs;
	*out_lens = lens;
	return 0;
}

/* --- regexp_split_to_array [oids 2767/2768] (accumArrayResult /
 * makeArrayResult carved to a flat text list, fmgr/array plumbing) ------ */
int
pg_diff_regexp_split(const unsigned char *sb, int slen,
					 const unsigned char *pb, int plen,
					 const unsigned char *fb, int flen, int has_flags,
					 int *out_n,
					 const unsigned char *const **out_ptrs,
					 const int **out_lens)
{
	pg_re_flags re_flags;
	regexp_matches_ctx *splitctx;
	const unsigned char **ptrs;
	int		   *lens;
	int			i;

	PG_DIFF_REGEXP_ENTRY_BEGIN();

	/* Determine options */
	parse_re_flags(&re_flags,
				   has_flags ? pg_diff_regexp_text(fb, flen) : NULL);
	/* User mustn't specify 'g' */
	if (re_flags.glob)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("%s does not support the \"global\" option",
						"regexp_split_to_array()")));
	/* But we find all the matches anyway */
	re_flags.glob = true;

	splitctx = setup_regexp_matches(pg_diff_regexp_text(sb, slen),
									pg_diff_regexp_text(pb, plen),
									&re_flags, 0,
									PG_GET_COLLATION(),
									false, true, true);

	ptrs = palloc(sizeof(unsigned char *) * (size_t) (splitctx->nmatches + 1));
	lens = palloc(sizeof(int) * (size_t) (splitctx->nmatches + 1));
	i = 0;
	while (splitctx->next_match <= splitctx->nmatches)
	{
		text	   *t = (text *) DatumGetPointer(build_regexp_split_result(splitctx));

		ptrs[i] = (const unsigned char *) VARDATA_ANY(t);
		lens[i] = VARSIZE_ANY_EXHDR(t);
		i++;
		splitctx->next_match++;
	}

	*out_n = i;
	*out_ptrs = (const unsigned char *const *) ptrs;
	*out_lens = lens;
	return 0;
}

/* --- regexp_fixed_prefix [regexp.c planner support; no SQL face] --------
 * Body VERBATIM from regexp.c regexp_fixed_prefix @ 62d6c7d3df (the engine
 * side, pg_regprefix, is the verbatim vendored csrc/regexfam/regprefix.c).
 * Shims (plumbing only): the (char *result, bool *exact) fmgr-less shell
 * becomes (out,outlen,out_exact,out_isnull) writeback (out_isnull models
 * the NULL "no fixed prefix" return); pfree(str) becomes free(str) because
 * pg_regprefix's MALLOC is malloc-family under the regexfam shim
 * (regcustom.h palloc_extended -> malloc), not this TU's palloc arena. */
int
pg_diff_regexp_fixed_prefix(const unsigned char *pb, int plen,
							int case_insensitive,
							const unsigned char **out, int *outlen,
							int *out_exact, int *out_isnull)
{
	text	   *text_re;
	char	   *result;
	regex_t    *re;
	int			cflags;
	int			re_result;
	pg_wchar   *str;
	size_t		slen;
	size_t		maxlen;
	char		errMsg[100];
	bool		exact;

	PG_DIFF_REGEXP_ENTRY_BEGIN();
	text_re = pg_diff_regexp_text(pb, plen);

	exact = false;				/* default result */

	/* Compile RE */
	cflags = REG_ADVANCED;
	if (case_insensitive)
		cflags |= REG_ICASE;

	re = RE_compile_and_cache(text_re, cflags | REG_NOSUB, PG_GET_COLLATION());

	/* Examine it to see if there's a fixed prefix */
	re_result = pg_regprefix(re, &str, &slen);

	switch (re_result)
	{
		case REG_NOMATCH:
			*out_isnull = 1;
			return 0;

		case REG_PREFIX:
			/* continue with wchar conversion */
			break;

		case REG_EXACT:
			exact = true;
			/* continue with wchar conversion */
			break;

		default:
			/* re failed??? */
			pg_regerror(re_result, re, errMsg, sizeof(errMsg));
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_REGULAR_EXPRESSION),
					 errmsg("regular expression failed: %s", errMsg)));
			break;
	}

	/* Convert pg_wchar result back to database encoding */
	maxlen = pg_database_encoding_max_length() * slen + 1;
	result = (char *) palloc(maxlen);
	slen = pg_wchar2mb_with_len(str, result, slen);
	Assert(slen < maxlen);

	free(str);					/* SHIM: engine MALLOC is malloc-family here */

	*out = (const unsigned char *) result;
	*outlen = (int) slen;
	*out_exact = exact ? 1 : 0;
	*out_isnull = 0;
	return 0;
}
