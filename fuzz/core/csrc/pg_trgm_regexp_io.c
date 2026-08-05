/*
 * pg_trgm_regexp_io.c: vendored PostgreSQL C oracle for trgm_diff ARM 9
 * (contrib/pg_trgm/trgm_regexp.c -- createTrgmNFA / trigramsMatchGraph).
 * 100%-coverage campaign, lane p1-trgm, phase B.
 *
 * Provenance: blocks marked "VERBATIM <file> lines A-B" are byte-for-byte
 * extractions from ../pgrust-fabled/vendor/postgres-src @
 * 62d6c7d3df6287f1bd83199c1a746e50d31571a0 (PostgreSQL 18.3), assembled by
 * scratchpad/gen_trgm_regexp_oracle.py.  The order-bearing infrastructure
 * is vendored WHOLE-FILE (cmp-auditable) under csrc/trgmrxfam/:
 * dynahash.c (hash_seq_search iteration order IS semantics here -- it
 * drives packGraph state numbering and arc collection order), list.c,
 * hashfn.c (tag_hash for HASH_BLOBS), pg_bitutils.c, qsort.c + lib/
 * sort_template.h (PG's qsort: colorTrgmInfoPenaltyCmp compares ONLY the
 * float penalty, ties are real, and removal order under the penalty sort
 * changes the selected trigram set -- libc qsort would diverge), and
 * regex/regexport.c (the NFA introspection API).
 *
 * This TU compiles in the trgmrxfam cc build: -I csrc/trgmrxfam/include
 * (shim postgres.h & friends -- see include/postgres.h header) then
 * -I csrc/regexfam + csrc/regexfam/include for the VERBATIM 18.3 Spencer
 * engine (pg_regcomp/pg_regfree/pg_regerror) and its mb glue
 * (pg_mb2wchar_with_len / pg_wchar2mb_with_len = the UTF-8-pinned
 * pg_regexfam_* one-row resolutions, documented in pg_regexfam_glue.c).
 *
 * SHIMS (plumbing only, never logic):
 *  - Arena/longjmp/locale/encoding/signedness state is SHARED with the
 *    trgm_op.c oracle TU via the pg_diff_trgm_bridge_* exports of
 *    pg_trgm_io.c: palloc family -> bridge arena (reset at every entry),
 *    ereport(ERROR) -> errcode class + longjmp through the bridge jmp_buf.
 *    Shared verbatim units are NOT re-vendored: str_tolower,
 *    t_isalnum_with_len (ISWORDCHR) and compact_trigram resolve to the
 *    trgmf_* globals of pg_trgm_io.c, so both oracle halves use the ONE
 *    vendored copy and the ONE locale model.
 *  - MemoryContext model: contexts are identity tokens; ALL allocation
 *    lands in the bridge arena and is reset per entry (the real code uses
 *    tmpcontext/rcontext only to separate cruft lifetime from result
 *    lifetime -- per-entry reset supersedes both).  AllocSetContextCreate/
 *    MemoryContextSwitchTo/MemoryContextDelete are no-op token plumbing;
 *    MemoryContextAlloc(cxt, n) ignores cxt.
 *  - RE_compile is VERBATIM (trgm_regexp.c body): palloc/pg_mb2wchar_with_
 *    len/pg_regcomp/ereport all resolve through the pins above.  The
 *    compiled regex_t lives in a TLS slot freed at the next entry so an
 *    ereport-longjmp cannot leak engine memory (pg_regexp_io.c precedent;
 *    the engine allocates via malloc per regcustom.h).
 *  - COLLATION PIN: locale arm 0 only (database ctype "C"); pg_regcomp is
 *    called with C_COLLATION_OID = 950 so regc_pg_locale.c's strategy is
 *    PG_REGEX_STRATEGY_C (the regexfam glue's unicode stubs stay dead).
 *    NAMED RESIDUAL: the builtin-C.UTF-8 colormap strategy (locale arm 1)
 *    is out of scope of this increment -- the Rust driver only ever calls
 *    arm 9 with locale_arm 0.
 *  - errcode classes (shared TLS pg_diff_errcode): 1 = 54000 (out of
 *    memory guards -- none in this file), 4 = 2201B
 *    ERRCODE_INVALID_REGULAR_EXPRESSION (RE_compile failure), 5 = the
 *    REG_ETOOBIG subset of 4 ("regular expression is too complex" -- the
 *    engine's stack guard / compile-space bound, split out so the driver
 *    can carve the stack band without widening it to every compile
 *    failure), 6 = internal elog.  Verdict channel of pg_diff_trgm_regexp:
 *    rc 0 = success, rc -1 = NULL fallback ("regex too complex/trivial"),
 *    rc>0 = errcode class.
 *  - STACK GUARD (2026-08-03 CONFIRM stack-overflow class): every entry
 *    anchors the engine's per-thread stack-guard base via
 *    pg_diff_regex_stack_arm() (pg_regexfam.c, 2048kB real-server budget).
 *    Unanchored, the engine's rstacktoodeep measured from NULL and the
 *    regc_nfa.c recursion guards were inert.
 *  - Assert -> active abort (matches the regexfam posture: engine/graph
 *    invariants loud in the fuzz build; the shipped Rust side has no
 *    equivalent checks, so a firing Assert is an oracle-side finding, not
 *    a plane).
 *  - qsort -> trgmrx_pg_qsort (the vendored PG qsort), via the port.h
 *    mapping (#define qsort(a,b,c,d) pg_qsort(a,b,c,d), port.h @ 18.3).
 */

#define TRGMRX_NO_ALLOC_MACROS 1
#define TRGMRX_NO_EREPORT_MACROS 1
#include "postgres.h"

#include "regex/regex.h"
#include "regex/regexport.h"
#include "nodes/pg_list.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"


/* ---- bridge to the trgm_op.c oracle TU (pg_trgm_io.c) ---- */
#include <setjmp.h>
extern _Thread_local int pg_diff_errcode;	/* defined in csrc/pg_float_io.c */
extern void pg_diff_trgm_bridge_enter(int locale_arm);
extern jmp_buf *pg_diff_trgm_bridge_jmp(void);
extern void pg_diff_trgm_bridge_raise(int code) __attribute__((noreturn));
extern int	pg_diff_trgm_bridge_pending_set(int code);
extern void *pg_diff_trgm_bridge_palloc(size_t n);
extern void *pg_diff_trgm_bridge_palloc0(size_t n);
extern void *pg_diff_trgm_bridge_repalloc(void *p, size_t n);
extern void pg_diff_trgm_bridge_pfree(void *p);

/* shared verbatim units living in pg_trgm_io.c (trgmf_ renames there) */
extern char *trgmf_str_tolower(const char *buff, size_t nbytes, Oid collid);
extern int	trgmf_t_isalnum_with_len(const char *ptr, int mblen);
/* trgm.h (pasted below, after the renames) carries the compact_trigram
 * declaration, typed with the real trgm typedef. */

#define str_tolower trgmf_str_tolower
#define t_isalnum_with_len trgmf_t_isalnum_with_len
#define compact_trigram trgmf_compact_trigram

/* rename this TU's non-static definitions (final-link isolation) */
#define createTrgmNFA trgmrx_createTrgmNFA
#define trigramsMatchGraph trgmrx_trigramsMatchGraph

/* errcode classes (same channel as pg_trgm_io.c; classes 4/5 are new here) */
#define PG_DIFF_TRGM_ERR_LIMIT 1
#define PG_DIFF_TRGM_ERR_INVALID_RE 4
/* class 5: REG_ETOOBIG through RE_compile ("regular expression is too
 * complex" — the engine's byte-based stack guard / compile-space bound).
 * Split from INVALID_RE so the driver can apply the RATIFIED stack-band
 * carve (regex_diff is_etoobig precedent) without widening it to every
 * compile failure. */
#define PG_DIFF_TRGM_ERR_RE_TOO_COMPLEX 5
#define PG_DIFF_TRGM_ERR_INTERNAL 6

/* Engine stack guard (the pristine-named engine copy this family binds =
 * regexcorefam's, whose stack_is_too_deep lives in pg_regexfam.c with a
 * per-thread lazily-anchored base at the real-server 2048kB budget). The
 * pg_diff_trgm_* entries never pass through pg_diff_regcomp's lazy anchor,
 * so before this call existed the base stayed NULL and the guard was INERT
 * — the 2026-08-03 trgm CONFIRM ASan stack-overflow class. */
extern void pg_diff_regex_stack_arm(void);

/* TLS slot for the live compiled regex (pg_regexp_io.c precedent): the
 * REAL server frees the engine's memory via MemoryContextDelete of
 * createTrgmNFA's tmpcontext (regcustom.h MALLOC = palloc_extended into
 * the current context @ 18.3); this TU's shim maps palloc_extended to raw
 * malloc and context deletion to a no-op, so the engine's guts MUST be
 * pg_regfree'd explicitly.  The compile wrapper below registers every
 * successful compile here; trgmrx_enter frees the previous one at the
 * next entry, so an ereport-longjmp between compile and use cannot leak
 * either.  (Task #150: this registration was missing — every arm-9
 * compile leaked its whole regex guts, ~25-30KB/exec RSS climb that
 * killed sustained fuzz runs at ~125k execs.) */
static _Thread_local regex_t trgmrx_live_re;
static _Thread_local bool trgmrx_live;

/* Capture pg_regcomp's result code so the driver entries can classify
 * REG_ETOOBIG separately (class 5 above), and register the live engine
 * memory for cleanup at the next entry (see trgmrx_live_re above).
 * Plumbing only: the verbatim RE_compile body below is unchanged — its
 * pg_regcomp call resolves to this wrapper via the #define. */
static _Thread_local int trgmrx_last_regcomp_code;
static int
trgmrx_pg_regcomp_capture(regex_t *re, const pg_wchar *w, size_t wlen,
						  int cflags, Oid collation)
{
	trgmrx_last_regcomp_code = pg_regcomp(re, w, wlen, cflags, collation);
	if (trgmrx_last_regcomp_code == REG_OKAY)
	{
		/* Shallow struct copy shares re_guts/re_fns with the caller's
		 * stack regex_t; pg_regfree through the copy frees the same
		 * engine memory (a failed compile frees itself — no
		 * registration). trgmrx_enter cleared the previous slot before
		 * this compile, so at most one regex is ever live. */
		trgmrx_live_re = *re;
		trgmrx_live = true;
	}
	return trgmrx_last_regcomp_code;
}
#define pg_regcomp trgmrx_pg_regcomp_capture
#define ERRCODE_PROGRAM_LIMIT_EXCEEDED PG_DIFF_TRGM_ERR_LIMIT
#define ERRCODE_INVALID_REGULAR_EXPRESSION PG_DIFF_TRGM_ERR_INVALID_RE
static int
trgmrx_errmsg(const char *fmt,...)
{
	(void) fmt;
	return 0;
}
#define errmsg trgmrx_errmsg
#define errcode(c) pg_diff_trgm_bridge_pending_set(c)
#define ereport(level, rest) do { pg_diff_trgm_bridge_pending_set(PG_DIFF_TRGM_ERR_INTERNAL); ((void) (rest)); pg_diff_trgm_bridge_raise(pg_diff_errcode_pending_fetch()); } while (0)
/* fetch-and-clear of the pending class set by errcode() inside `rest` --
 * implemented in pg_trgm_io.c next to trgmf_errcode_set */
extern int	pg_diff_errcode_pending_fetch(void);
#define elog(level, ...) do { trgmrx_errmsg(__VA_ARGS__); pg_diff_trgm_bridge_raise(PG_DIFF_TRGM_ERR_INTERNAL); } while (0)
#define ERROR 21

#define palloc(n) pg_diff_trgm_bridge_palloc(n)
#define palloc0(n) pg_diff_trgm_bridge_palloc0(n)
#define repalloc(p, n) pg_diff_trgm_bridge_repalloc((p), (n))
#define pfree(p) pg_diff_trgm_bridge_pfree(p)

/* port.h @ 18.3: the backend's qsort IS pg_qsort (hstore oracle precedent);
 * pg_qsort itself is renamed trgmrx_pg_qsort by the build (see build.rs). */
extern void trgmrx_pg_qsort(void *base, size_t nel, size_t elsize,
							int (*cmp) (const void *, const void *));
#define qsort(a,b,c,d) trgmrx_pg_qsort(a,b,c,d)

/* pg_wchar.h: MAX_MULTIBYTE_CHAR_LEN (verbatim value) */
#define MAX_MULTIBYTE_CHAR_LEN	4

/* trgm.h needs this gist constant only for the (unused here) SIGLEN_MAX */
#define GISTMaxIndexKeySize 8152

/* ---- VERBATIM contrib/pg_trgm/trgm.h lines 12-113 @ 62d6c7d3df ---- */
/*
 * Options ... but note that trgm_regexp.c effectively assumes these values
 * of LPADDING and RPADDING.
 */
#define LPADDING		2
#define RPADDING		1
/*
 * Caution: IGNORECASE macro means that trigrams are case-insensitive.
 * If this macro is disabled, the ~* and ~~* operators must be removed from
 * the operator classes, because we can't handle case-insensitive wildcard
 * search with case-sensitive trigrams.  Failure to do this will result in
 * "cannot handle ~*(~~*) with case-sensitive trigrams" errors.
 */
#define IGNORECASE
#define DIVUNION

/* operator strategy numbers */
#define SimilarityStrategyNumber			1
#define DistanceStrategyNumber				2
#define LikeStrategyNumber					3
#define ILikeStrategyNumber					4
#define RegExpStrategyNumber				5
#define RegExpICaseStrategyNumber			6
#define WordSimilarityStrategyNumber		7
#define WordDistanceStrategyNumber			8
#define StrictWordSimilarityStrategyNumber	9
#define StrictWordDistanceStrategyNumber	10
#define EqualStrategyNumber					11

typedef char trgm[3];

#define CPTRGM(a,b) do {				\
	*(((char*)(a))+0) = *(((char*)(b))+0);	\
	*(((char*)(a))+1) = *(((char*)(b))+1);	\
	*(((char*)(a))+2) = *(((char*)(b))+2);	\
} while(0)
extern int	(*CMPTRGM) (const void *a, const void *b);

#define ISWORDCHR(c, len)	(t_isalnum_with_len(c, len))
#define ISPRINTABLECHAR(a)	( isascii( *(unsigned char*)(a) ) && (isalnum( *(unsigned char*)(a) ) || *(unsigned char*)(a)==' ') )
#define ISPRINTABLETRGM(t)	( ISPRINTABLECHAR( ((char*)(t)) ) && ISPRINTABLECHAR( ((char*)(t))+1 ) && ISPRINTABLECHAR( ((char*)(t))+2 ) )

#define ISESCAPECHAR(x) (*(x) == '\\')	/* Wildcard escape character */
#define ISWILDCARDCHAR(x) (*(x) == '_' || *(x) == '%')	/* Wildcard
														 * meta-character */

typedef struct
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	uint8		flag;
	char		data[FLEXIBLE_ARRAY_MEMBER];
} TRGM;

#define TRGMHDRSIZE		  (VARHDRSZ + sizeof(uint8))

/* gist */
#define SIGLEN_DEFAULT	(sizeof(int) * 3)
#define SIGLEN_MAX		GISTMaxIndexKeySize
#define BITBYTE 8

#define SIGLENBIT(siglen) ((siglen) * BITBYTE - 1)	/* see makesign */

typedef char *BITVECP;

#define LOOPBYTE(siglen) \
			for (i = 0; i < (siglen); i++)

#define GETBYTE(x,i) ( *( (BITVECP)(x) + (int)( (i) / BITBYTE ) ) )
#define GETBITBYTE(x,i) ( (((char)(x)) >> (i)) & 0x01 )
#define CLRBIT(x,i)   GETBYTE(x,i) &= ~( 0x01 << ( (i) % BITBYTE ) )
#define SETBIT(x,i)   GETBYTE(x,i) |=  ( 0x01 << ( (i) % BITBYTE ) )
#define GETBIT(x,i) ( (GETBYTE(x,i) >> ( (i) % BITBYTE )) & 0x01 )

#define HASHVAL(val, siglen) (((unsigned int)(val)) % SIGLENBIT(siglen))
#define HASH(sign, val, siglen) SETBIT((sign), HASHVAL(val, siglen))

#define ARRKEY			0x01
#define SIGNKEY			0x02
#define ALLISTRUE		0x04

#define ISARRKEY(x) ( ((TRGM*)x)->flag & ARRKEY )
#define ISSIGNKEY(x)	( ((TRGM*)x)->flag & SIGNKEY )
#define ISALLTRUE(x)	( ((TRGM*)x)->flag & ALLISTRUE )

#define CALCGTSIZE(flag, len) ( TRGMHDRSIZE + ( ( (flag) & ARRKEY ) ? ((len)*sizeof(trgm)) : (((flag) & ALLISTRUE) ? 0 : (len)) ) )
#define GETSIGN(x)		( (BITVECP)( (char*)x+TRGMHDRSIZE ) )
#define GETARR(x)		( (trgm*)( (char*)x+TRGMHDRSIZE ) )
#define ARRNELEM(x) ( ( VARSIZE(x) - TRGMHDRSIZE )/sizeof(trgm) )

/*
 * If DIVUNION is defined then similarity formula is:
 * count / (len1 + len2 - count)
 * else if DIVUNION is not defined then similarity formula is:
 * count / max(len1, len2)
 */
#ifdef DIVUNION
#define CALCSML(count, len1, len2) ((float4) (count)) / ((float4) ((len1) + (len2) - (count)))
#else
#define CALCSML(count, len1, len2) ((float4) (count)) / ((float4) (((len1) > (len2)) ? (len1) : (len2)))
#endif

typedef struct TrgmPackedGraph TrgmPackedGraph;
/* ---- end VERBATIM contrib/pg_trgm/trgm.h lines 12-113 ---- */


/* trgm.h decls outside the pasted range, mapped to the shared trgmf_ units
 * (the #defines above rename them) */
extern void compact_trigram(trgm *tptr, char *str, int bytelen);
#define DEFAULT_COLLATION_OID 100	/* pg_collation_d.h verbatim value */

/* ---- VERBATIM contrib/pg_trgm/trgm_regexp.c lines 202-2114 @ 62d6c7d3df ---- */

/*
 * Uncomment (or use -DTRGM_REGEXP_DEBUG) to print debug info,
 * for exploring and debugging the algorithm implementation.
 * This produces three graph files in /tmp, in Graphviz .gv format.
 * Some progress information is also printed to postmaster stderr.
 */
/* #define TRGM_REGEXP_DEBUG */

/*
 * These parameters are used to limit the amount of work done.
 * Otherwise regex processing could be too slow and memory-consuming.
 *
 *	MAX_EXPANDED_STATES - How many states we allow in expanded graph
 *	MAX_EXPANDED_ARCS - How many arcs we allow in expanded graph
 *	MAX_TRGM_COUNT - How many simple trigrams we allow to be extracted
 *	WISH_TRGM_PENALTY - Maximum desired sum of color trigram penalties
 *	COLOR_COUNT_LIMIT - Maximum number of characters per color
 */
#define MAX_EXPANDED_STATES 128
#define MAX_EXPANDED_ARCS	1024
#define MAX_TRGM_COUNT		256
#define WISH_TRGM_PENALTY	16
#define COLOR_COUNT_LIMIT	256

/*
 * Penalty multipliers for trigram counts depending on whitespace contents.
 * Numbers based on analysis of real-life texts.
 */
static const float4 penalties[8] = {
	1.0f,						/* "aaa" */
	3.5f,						/* "aa " */
	0.0f,						/* "a a" (impossible) */
	0.0f,						/* "a  " (impossible) */
	4.2f,						/* " aa" */
	2.1f,						/* " a " */
	25.0f,						/* "  a" */
	0.0f						/* "   " (impossible) */
};

/* Struct representing a single pg_wchar, converted back to multibyte form */
typedef struct
{
	char		bytes[MAX_MULTIBYTE_CHAR_LEN];
} trgm_mb_char;

/*
 * Attributes of NFA colors:
 *
 *	expandable				- we know the character expansion of this color
 *	containsNonWord			- color contains non-word characters
 *							  (which will not be extracted into trigrams)
 *	wordCharsCount			- count of word characters in color
 *	wordChars				- array of this color's word characters
 *							  (which can be extracted into trigrams)
 *
 * When expandable is false, the other attributes don't matter; we just
 * assume this color represents unknown character(s).
 */
typedef struct
{
	bool		expandable;
	bool		containsNonWord;
	int			wordCharsCount;
	trgm_mb_char *wordChars;
} TrgmColorInfo;

/*
 * A "prefix" is information about the colors of the last two characters read
 * before reaching a specific NFA state.  These colors can have special values
 * COLOR_UNKNOWN and COLOR_BLANK.  COLOR_UNKNOWN means that we have no
 * information, for example because we read some character of an unexpandable
 * color.  COLOR_BLANK means that we read a non-word character.
 *
 * We call a prefix ambiguous if at least one of its colors is unknown.  It's
 * fully ambiguous if both are unknown, partially ambiguous if only the first
 * is unknown.  (The case of first color known, second unknown is not valid.)
 *
 * Wholly- or partly-blank prefixes are mostly handled the same as regular
 * color prefixes.  This allows us to generate appropriate partly-blank
 * trigrams when the NFA requires word character(s) to appear adjacent to
 * non-word character(s).
 */
typedef int TrgmColor;

/* We assume that colors returned by the regexp engine cannot be these: */
#define COLOR_UNKNOWN	(-3)
#define COLOR_BLANK		(-4)

typedef struct
{
	TrgmColor	colors[2];
} TrgmPrefix;

/*
 * Color-trigram data type.  Note that some elements of the trigram can be
 * COLOR_BLANK, but we don't allow COLOR_UNKNOWN.
 */
typedef struct
{
	TrgmColor	colors[3];
} ColorTrgm;

/*
 * Key identifying a state of our expanded graph: color prefix, and number
 * of the corresponding state in the underlying regex NFA.  The color prefix
 * shows how we reached the regex state (to the extent that we know it).
 */
typedef struct
{
	TrgmPrefix	prefix;
	int			nstate;
} TrgmStateKey;

/*
 * One state of the expanded graph.
 *
 *	stateKey - ID of this state
 *	arcs	 - outgoing arcs of this state (List of TrgmArc)
 *	enterKeys - enter keys reachable from this state without reading any
 *			   predictable trigram (List of TrgmStateKey)
 *	flags	 - flag bits
 *	snumber  - number of this state (initially assigned as -1, -2, etc,
 *			   for debugging purposes only; then at the packaging stage,
 *			   surviving states are renumbered with positive numbers)
 *	parent	 - parent state, if this state has been merged into another
 *	tentFlags - flags this state would acquire via planned merges
 *	tentParent - planned parent state, if considering a merge
 */
#define TSTATE_INIT		0x01	/* flag indicating this state is initial */
#define TSTATE_FIN		0x02	/* flag indicating this state is final */

typedef struct TrgmState
{
	TrgmStateKey stateKey;		/* hashtable key: must be first field */
	List	   *arcs;
	List	   *enterKeys;
	int			flags;
	int			snumber;
	struct TrgmState *parent;
	int			tentFlags;
	struct TrgmState *tentParent;
} TrgmState;

/*
 * One arc in the expanded graph.
 */
typedef struct
{
	ColorTrgm	ctrgm;			/* trigram needed to traverse arc */
	TrgmState  *target;			/* next state */
} TrgmArc;

/*
 * Information about arc of specific color trigram (used in stage 3)
 *
 * Contains pointers to the source and target states.
 */
typedef struct
{
	TrgmState  *source;
	TrgmState  *target;
} TrgmArcInfo;

/*
 * Information about color trigram (used in stage 3)
 *
 * ctrgm	- trigram itself
 * cnumber	- number of this trigram (used in the packaging stage)
 * count	- number of simple trigrams created from this color trigram
 * expanded - indicates this color trigram is expanded into simple trigrams
 * arcs		- list of all arcs labeled with this color trigram.
 */
typedef struct
{
	ColorTrgm	ctrgm;
	int			cnumber;
	int			count;
	float4		penalty;
	bool		expanded;
	List	   *arcs;
} ColorTrgmInfo;

/*
 * Data structure representing all the data we need during regex processing.
 *
 *	regex			- compiled regex
 *	colorInfo		- extracted information about regex's colors
 *	ncolors			- number of colors in colorInfo[]
 *	states			- hashtable of TrgmStates (states of expanded graph)
 *	initState		- pointer to initial state of expanded graph
 *	queue			- queue of to-be-processed TrgmStates
 *	keysQueue		- queue of to-be-processed TrgmStateKeys
 *	arcsCount		- total number of arcs of expanded graph (for resource
 *					  limiting)
 *	overflowed		- we have exceeded resource limit for transformation
 *	colorTrgms		- array of all color trigrams present in graph
 *	colorTrgmsCount - count of those color trigrams
 *	totalTrgmCount	- total count of extracted simple trigrams
 */
typedef struct
{
	/* Source regexp, and color information extracted from it (stage 1) */
	regex_t    *regex;
	TrgmColorInfo *colorInfo;
	int			ncolors;

	/* Expanded graph (stage 2) */
	HTAB	   *states;
	TrgmState  *initState;
	int			nstates;

	/* Workspace for stage 2 */
	List	   *queue;
	List	   *keysQueue;
	int			arcsCount;
	bool		overflowed;

	/* Information about distinct color trigrams in the graph (stage 3) */
	ColorTrgmInfo *colorTrgms;
	int			colorTrgmsCount;
	int			totalTrgmCount;
} TrgmNFA;

/*
 * Final, compact representation of expanded graph.
 */
typedef struct
{
	int			targetState;	/* index of target state (zero-based) */
	int			colorTrgm;		/* index of color trigram for transition */
} TrgmPackedArc;

typedef struct
{
	int			arcsCount;		/* number of out-arcs for this state */
	TrgmPackedArc *arcs;		/* array of arcsCount packed arcs */
} TrgmPackedState;

/* "typedef struct TrgmPackedGraph TrgmPackedGraph" appears in trgm.h */
struct TrgmPackedGraph
{
	/*
	 * colorTrigramsCount and colorTrigramGroups contain information about how
	 * trigrams are grouped into color trigrams.  "colorTrigramsCount" is the
	 * count of color trigrams and "colorTrigramGroups" contains number of
	 * simple trigrams for each color trigram.  The array of simple trigrams
	 * (stored separately from this struct) is ordered so that the simple
	 * trigrams for each color trigram are consecutive, and they're in order
	 * by color trigram number.
	 */
	int			colorTrigramsCount;
	int		   *colorTrigramGroups; /* array of size colorTrigramsCount */

	/*
	 * The states of the simplified NFA.  State number 0 is always initial
	 * state and state number 1 is always final state.
	 */
	int			statesCount;
	TrgmPackedState *states;	/* array of size statesCount */

	/* Temporary work space for trigramsMatchGraph() */
	bool	   *colorTrigramsActive;	/* array of size colorTrigramsCount */
	bool	   *statesActive;	/* array of size statesCount */
	int		   *statesQueue;	/* array of size statesCount */
};

/*
 * Temporary structure for representing an arc during packaging.
 */
typedef struct
{
	int			sourceState;
	int			targetState;
	int			colorTrgm;
} TrgmPackArcInfo;


/* prototypes for private functions */
static TRGM *createTrgmNFAInternal(regex_t *regex, TrgmPackedGraph **graph,
								   MemoryContext rcontext);
static void RE_compile(regex_t *regex, text *text_re,
					   int cflags, Oid collation);
static void getColorInfo(regex_t *regex, TrgmNFA *trgmNFA);
static int	convertPgWchar(pg_wchar c, trgm_mb_char *result);
static void transformGraph(TrgmNFA *trgmNFA);
static void processState(TrgmNFA *trgmNFA, TrgmState *state);
static void addKey(TrgmNFA *trgmNFA, TrgmState *state, TrgmStateKey *key);
static void addKeyToQueue(TrgmNFA *trgmNFA, TrgmStateKey *key);
static void addArcs(TrgmNFA *trgmNFA, TrgmState *state);
static void addArc(TrgmNFA *trgmNFA, TrgmState *state, TrgmStateKey *key,
				   TrgmColor co, TrgmStateKey *destKey);
static bool validArcLabel(TrgmStateKey *key, TrgmColor co);
static TrgmState *getState(TrgmNFA *trgmNFA, TrgmStateKey *key);
static bool prefixContains(TrgmPrefix *prefix1, TrgmPrefix *prefix2);
static bool selectColorTrigrams(TrgmNFA *trgmNFA);
static TRGM *expandColorTrigrams(TrgmNFA *trgmNFA, MemoryContext rcontext);
static void fillTrgm(trgm *ptrgm, trgm_mb_char s[3]);
static void mergeStates(TrgmState *state1, TrgmState *state2);
static int	colorTrgmInfoCmp(const void *p1, const void *p2);
static int	colorTrgmInfoPenaltyCmp(const void *p1, const void *p2);
static TrgmPackedGraph *packGraph(TrgmNFA *trgmNFA, MemoryContext rcontext);
static int	packArcInfoCmp(const void *a1, const void *a2);

#ifdef TRGM_REGEXP_DEBUG
static void printSourceNFA(regex_t *regex, TrgmColorInfo *colors, int ncolors);
static void printTrgmNFA(TrgmNFA *trgmNFA);
static void printTrgmColor(StringInfo buf, TrgmColor co);
static void printTrgmPackedGraph(TrgmPackedGraph *packedGraph, TRGM *trigrams);
#endif


/*
 * Main entry point to process a regular expression.
 *
 * Returns an array of trigrams required by the regular expression, or NULL if
 * the regular expression was too complex to analyze.  In addition, a packed
 * graph representation of the regex is returned into *graph.  The results
 * must be allocated in rcontext (which might or might not be the current
 * context).
 */
TRGM *
createTrgmNFA(text *text_re, Oid collation,
			  TrgmPackedGraph **graph, MemoryContext rcontext)
{
	TRGM	   *trg;
	regex_t		regex;
	MemoryContext tmpcontext;
	MemoryContext oldcontext;

	/*
	 * This processing generates a great deal of cruft, which we'd like to
	 * clean up before returning (since this function may be called in a
	 * query-lifespan memory context).  Make a temp context we can work in so
	 * that cleanup is easy.
	 */
	tmpcontext = AllocSetContextCreate(CurrentMemoryContext,
									   "createTrgmNFA temporary context",
									   ALLOCSET_DEFAULT_SIZES);
	oldcontext = MemoryContextSwitchTo(tmpcontext);

	/*
	 * Stage 1: Compile the regexp into a NFA, using the regexp library.
	 */
#ifdef IGNORECASE
	RE_compile(&regex, text_re,
			   REG_ADVANCED | REG_NOSUB | REG_ICASE, collation);
#else
	RE_compile(&regex, text_re,
			   REG_ADVANCED | REG_NOSUB, collation);
#endif

	trg = createTrgmNFAInternal(&regex, graph, rcontext);

	/* Clean up all the cruft we created (including regex) */
	MemoryContextSwitchTo(oldcontext);
	MemoryContextDelete(tmpcontext);

	return trg;
}

/*
 * Body of createTrgmNFA, exclusive of regex compilation/freeing.
 */
static TRGM *
createTrgmNFAInternal(regex_t *regex, TrgmPackedGraph **graph,
					  MemoryContext rcontext)
{
	TRGM	   *trg;
	TrgmNFA		trgmNFA;

	trgmNFA.regex = regex;

	/* Collect color information from the regex */
	getColorInfo(regex, &trgmNFA);

#ifdef TRGM_REGEXP_DEBUG
	printSourceNFA(regex, trgmNFA.colorInfo, trgmNFA.ncolors);
#endif

	/*
	 * Stage 2: Create an expanded graph from the source NFA.
	 */
	transformGraph(&trgmNFA);

#ifdef TRGM_REGEXP_DEBUG
	printTrgmNFA(&trgmNFA);
#endif

	/*
	 * Fail if we were unable to make a nontrivial graph, ie it is possible to
	 * get from the initial state to the final state without reading any
	 * predictable trigram.
	 */
	if (trgmNFA.initState->flags & TSTATE_FIN)
		return NULL;

	/*
	 * Stage 3: Select color trigrams to expand.  Fail if too many trigrams.
	 */
	if (!selectColorTrigrams(&trgmNFA))
		return NULL;

	/*
	 * Stage 4: Expand color trigrams and pack graph into final
	 * representation.
	 */
	trg = expandColorTrigrams(&trgmNFA, rcontext);

	*graph = packGraph(&trgmNFA, rcontext);

#ifdef TRGM_REGEXP_DEBUG
	printTrgmPackedGraph(*graph, trg);
#endif

	return trg;
}

/*
 * Main entry point for evaluating a graph during index scanning.
 *
 * The check[] array is indexed by trigram number (in the array of simple
 * trigrams returned by createTrgmNFA), and holds true for those trigrams
 * that are present in the index entry being checked.
 */
bool
trigramsMatchGraph(TrgmPackedGraph *graph, bool *check)
{
	int			i,
				j,
				k,
				queueIn,
				queueOut;

	/*
	 * Reset temporary working areas.
	 */
	memset(graph->colorTrigramsActive, 0,
		   sizeof(bool) * graph->colorTrigramsCount);
	memset(graph->statesActive, 0, sizeof(bool) * graph->statesCount);

	/*
	 * Check which color trigrams were matched.  A match for any simple
	 * trigram associated with a color trigram counts as a match of the color
	 * trigram.
	 */
	j = 0;
	for (i = 0; i < graph->colorTrigramsCount; i++)
	{
		int			cnt = graph->colorTrigramGroups[i];

		for (k = j; k < j + cnt; k++)
		{
			if (check[k])
			{
				/*
				 * Found one matched trigram in the group. Can skip the rest
				 * of them and go to the next group.
				 */
				graph->colorTrigramsActive[i] = true;
				break;
			}
		}
		j = j + cnt;
	}

	/*
	 * Initialize the statesQueue to hold just the initial state.  Note:
	 * statesQueue has room for statesCount entries, which is certainly enough
	 * since no state will be put in the queue more than once. The
	 * statesActive array marks which states have been queued.
	 */
	graph->statesActive[0] = true;
	graph->statesQueue[0] = 0;
	queueIn = 0;
	queueOut = 1;

	/* Process queued states as long as there are any. */
	while (queueIn < queueOut)
	{
		int			stateno = graph->statesQueue[queueIn++];
		TrgmPackedState *state = &graph->states[stateno];
		int			cnt = state->arcsCount;

		/* Loop over state's out-arcs */
		for (i = 0; i < cnt; i++)
		{
			TrgmPackedArc *arc = &state->arcs[i];

			/*
			 * If corresponding color trigram is present then activate the
			 * corresponding state.  We're done if that's the final state,
			 * otherwise queue the state if it's not been queued already.
			 */
			if (graph->colorTrigramsActive[arc->colorTrgm])
			{
				int			nextstate = arc->targetState;

				if (nextstate == 1)
					return true;	/* success: final state is reachable */

				if (!graph->statesActive[nextstate])
				{
					graph->statesActive[nextstate] = true;
					graph->statesQueue[queueOut++] = nextstate;
				}
			}
		}
	}

	/* Queue is empty, so match fails. */
	return false;
}

/*
 * Compile regex string into struct at *regex.
 * NB: pg_regfree must be applied to regex if this completes successfully.
 */
static void
RE_compile(regex_t *regex, text *text_re, int cflags, Oid collation)
{
	int			text_re_len = VARSIZE_ANY_EXHDR(text_re);
	char	   *text_re_val = VARDATA_ANY(text_re);
	pg_wchar   *pattern;
	int			pattern_len;
	int			regcomp_result;
	char		errMsg[100];

	/* Convert pattern string to wide characters */
	pattern = (pg_wchar *) palloc((text_re_len + 1) * sizeof(pg_wchar));
	pattern_len = pg_mb2wchar_with_len(text_re_val,
									   pattern,
									   text_re_len);

	/* Compile regex */
	regcomp_result = pg_regcomp(regex,
								pattern,
								pattern_len,
								cflags,
								collation);

	pfree(pattern);

	if (regcomp_result != REG_OKAY)
	{
		/* re didn't compile (no need for pg_regfree, if so) */
		pg_regerror(regcomp_result, regex, errMsg, sizeof(errMsg));
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_REGULAR_EXPRESSION),
				 errmsg("invalid regular expression: %s", errMsg)));
	}
}


/*---------------------
 * Subroutines for pre-processing the color map (stage 1).
 *---------------------
 */

/*
 * Fill TrgmColorInfo structure for each color using regex export functions.
 */
static void
getColorInfo(regex_t *regex, TrgmNFA *trgmNFA)
{
	int			colorsCount = pg_reg_getnumcolors(regex);
	int			i;

	trgmNFA->ncolors = colorsCount;
	trgmNFA->colorInfo = (TrgmColorInfo *)
		palloc0(colorsCount * sizeof(TrgmColorInfo));

	/*
	 * Loop over colors, filling TrgmColorInfo about each.  Note we include
	 * WHITE (0) even though we know it'll be reported as non-expandable.
	 */
	for (i = 0; i < colorsCount; i++)
	{
		TrgmColorInfo *colorInfo = &trgmNFA->colorInfo[i];
		int			charsCount = pg_reg_getnumcharacters(regex, i);
		pg_wchar   *chars;
		int			j;

		if (charsCount < 0 || charsCount > COLOR_COUNT_LIMIT)
		{
			/* Non expandable, or too large to work with */
			colorInfo->expandable = false;
			continue;
		}

		colorInfo->expandable = true;
		colorInfo->containsNonWord = false;
		colorInfo->wordChars = (trgm_mb_char *)
			palloc(sizeof(trgm_mb_char) * charsCount);
		colorInfo->wordCharsCount = 0;

		/* Extract all the chars in this color */
		chars = (pg_wchar *) palloc(sizeof(pg_wchar) * charsCount);
		pg_reg_getcharacters(regex, i, chars, charsCount);

		/*
		 * Convert characters back to multibyte form, and save only those that
		 * are word characters.  Set "containsNonWord" if any non-word
		 * character.  (Note: it'd probably be nicer to keep the chars in
		 * pg_wchar format for now, but ISWORDCHR wants to see multibyte.)
		 */
		for (j = 0; j < charsCount; j++)
		{
			trgm_mb_char c;
			int			clen = convertPgWchar(chars[j], &c);

			if (!clen)
				continue;		/* ok to ignore it altogether */
			if (ISWORDCHR(c.bytes, clen))
				colorInfo->wordChars[colorInfo->wordCharsCount++] = c;
			else
				colorInfo->containsNonWord = true;
		}

		pfree(chars);
	}
}

/*
 * Convert pg_wchar to multibyte format.
 * Returns 0 if the character should be ignored completely, else returns its
 * byte length.
 */
static int
convertPgWchar(pg_wchar c, trgm_mb_char *result)
{
	/* "s" has enough space for a multibyte character and a trailing NUL */
	char		s[MAX_MULTIBYTE_CHAR_LEN + 1];
	int			clen;

	/*
	 * We can ignore the NUL character, since it can never appear in a PG text
	 * string.  This avoids the need for various special cases when
	 * reconstructing trigrams.
	 */
	if (c == 0)
		return 0;

	/* Do the conversion, making sure the result is NUL-terminated */
	memset(s, 0, sizeof(s));
	clen = pg_wchar2mb_with_len(&c, s, 1);

	/*
	 * In IGNORECASE mode, we can ignore uppercase characters.  We assume that
	 * the regex engine generated both uppercase and lowercase equivalents
	 * within each color, since we used the REG_ICASE option; so there's no
	 * need to process the uppercase version.
	 *
	 * XXX this code is dependent on the assumption that str_tolower() works
	 * the same as the regex engine's internal case folding machinery.  Might
	 * be wiser to expose pg_wc_tolower and test whether c ==
	 * pg_wc_tolower(c). On the other hand, the trigrams in the index were
	 * created using str_tolower(), so we're probably screwed if there's any
	 * incompatibility anyway.
	 */
#ifdef IGNORECASE
	{
		char	   *lowerCased = str_tolower(s, clen, DEFAULT_COLLATION_OID);

		if (strcmp(lowerCased, s) != 0)
		{
			pfree(lowerCased);
			return 0;
		}
		pfree(lowerCased);
	}
#endif

	/* Fill result with exactly MAX_MULTIBYTE_CHAR_LEN bytes */
	memcpy(result->bytes, s, MAX_MULTIBYTE_CHAR_LEN);
	return clen;
}


/*---------------------
 * Subroutines for expanding original NFA graph into a trigram graph (stage 2).
 *---------------------
 */

/*
 * Transform the graph, given a regex and extracted color information.
 *
 * We create and process a queue of expanded-graph states until all the states
 * are processed.
 *
 * This algorithm may be stopped due to resource limitation. In this case we
 * force every unprocessed branch to immediately finish with matching (this
 * can give us false positives but no false negatives) by marking all
 * unprocessed states as final.
 */
static void
transformGraph(TrgmNFA *trgmNFA)
{
	HASHCTL		hashCtl;
	TrgmStateKey initkey;
	TrgmState  *initstate;
	ListCell   *lc;

	/* Initialize this stage's workspace in trgmNFA struct */
	trgmNFA->queue = NIL;
	trgmNFA->keysQueue = NIL;
	trgmNFA->arcsCount = 0;
	trgmNFA->overflowed = false;

	/* Create hashtable for states */
	hashCtl.keysize = sizeof(TrgmStateKey);
	hashCtl.entrysize = sizeof(TrgmState);
	hashCtl.hcxt = CurrentMemoryContext;
	trgmNFA->states = hash_create("Trigram NFA",
								  1024,
								  &hashCtl,
								  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	trgmNFA->nstates = 0;

	/* Create initial state: ambiguous prefix, NFA's initial state */
	MemSet(&initkey, 0, sizeof(initkey));
	initkey.prefix.colors[0] = COLOR_UNKNOWN;
	initkey.prefix.colors[1] = COLOR_UNKNOWN;
	initkey.nstate = pg_reg_getinitialstate(trgmNFA->regex);

	initstate = getState(trgmNFA, &initkey);
	initstate->flags |= TSTATE_INIT;
	trgmNFA->initState = initstate;

	/*
	 * Recursively build the expanded graph by processing queue of states
	 * (breadth-first search).  getState already put initstate in the queue.
	 * Note that getState will append new states to the queue within the loop,
	 * too; this works as long as we don't do repeat fetches using the "lc"
	 * pointer.
	 */
	foreach(lc, trgmNFA->queue)
	{
		TrgmState  *state = (TrgmState *) lfirst(lc);

		/*
		 * If we overflowed then just mark state as final.  Otherwise do
		 * actual processing.
		 */
		if (trgmNFA->overflowed)
			state->flags |= TSTATE_FIN;
		else
			processState(trgmNFA, state);

		/* Did we overflow? */
		if (trgmNFA->arcsCount > MAX_EXPANDED_ARCS ||
			hash_get_num_entries(trgmNFA->states) > MAX_EXPANDED_STATES)
			trgmNFA->overflowed = true;
	}
}

/*
 * Process one state: add enter keys and then add outgoing arcs.
 */
static void
processState(TrgmNFA *trgmNFA, TrgmState *state)
{
	ListCell   *lc;

	/* keysQueue should be NIL already, but make sure */
	trgmNFA->keysQueue = NIL;

	/*
	 * Add state's own key, and then process all keys added to keysQueue until
	 * queue is finished.  But we can quit if the state gets marked final.
	 */
	addKey(trgmNFA, state, &state->stateKey);
	foreach(lc, trgmNFA->keysQueue)
	{
		TrgmStateKey *key = (TrgmStateKey *) lfirst(lc);

		if (state->flags & TSTATE_FIN)
			break;
		addKey(trgmNFA, state, key);
	}

	/* Release keysQueue to clean up for next cycle */
	list_free(trgmNFA->keysQueue);
	trgmNFA->keysQueue = NIL;

	/*
	 * Add outgoing arcs only if state isn't final (we have no interest in
	 * outgoing arcs if we already match)
	 */
	if (!(state->flags & TSTATE_FIN))
		addArcs(trgmNFA, state);
}

/*
 * Add the given enter key into the state's enterKeys list, and determine
 * whether this should result in any further enter keys being added.
 * If so, add those keys to keysQueue so that processState will handle them.
 *
 * If the enter key is for the NFA's final state, mark state as TSTATE_FIN.
 * This situation means that we can reach the final state from this expanded
 * state without reading any predictable trigram, so we must consider this
 * state as an accepting one.
 *
 * The given key could be a duplicate of one already in enterKeys, or be
 * redundant with some enterKeys.  So we check that before doing anything.
 *
 * Note that we don't generate any actual arcs here.  addArcs will do that
 * later, after we have identified all the enter keys for this state.
 */
static void
addKey(TrgmNFA *trgmNFA, TrgmState *state, TrgmStateKey *key)
{
	regex_arc_t *arcs;
	TrgmStateKey destKey;
	ListCell   *cell;
	int			i,
				arcsCount;

	/*
	 * Ensure any pad bytes in destKey are zero, since it may get used as a
	 * hashtable key by getState.
	 */
	MemSet(&destKey, 0, sizeof(destKey));

	/*
	 * Compare key to each existing enter key of the state to check for
	 * redundancy.  We can drop either old key(s) or the new key if we find
	 * redundancy.
	 */
	foreach(cell, state->enterKeys)
	{
		TrgmStateKey *existingKey = (TrgmStateKey *) lfirst(cell);

		if (existingKey->nstate == key->nstate)
		{
			if (prefixContains(&existingKey->prefix, &key->prefix))
			{
				/* This old key already covers the new key. Nothing to do */
				return;
			}
			if (prefixContains(&key->prefix, &existingKey->prefix))
			{
				/*
				 * The new key covers this old key. Remove the old key, it's
				 * no longer needed once we add this key to the list.
				 */
				state->enterKeys = foreach_delete_current(state->enterKeys,
														  cell);
			}
		}
	}

	/* No redundancy, so add this key to the state's list */
	state->enterKeys = lappend(state->enterKeys, key);

	/* If state is now known final, mark it and we're done */
	if (key->nstate == pg_reg_getfinalstate(trgmNFA->regex))
	{
		state->flags |= TSTATE_FIN;
		return;
	}

	/*
	 * Loop through all outgoing arcs of the corresponding state in the
	 * original NFA.
	 */
	arcsCount = pg_reg_getnumoutarcs(trgmNFA->regex, key->nstate);
	arcs = (regex_arc_t *) palloc(sizeof(regex_arc_t) * arcsCount);
	pg_reg_getoutarcs(trgmNFA->regex, key->nstate, arcs, arcsCount);

	for (i = 0; i < arcsCount; i++)
	{
		regex_arc_t *arc = &arcs[i];

		if (pg_reg_colorisbegin(trgmNFA->regex, arc->co))
		{
			/*
			 * Start of line/string (^).  Trigram extraction treats start of
			 * line same as start of word: double space prefix is added.
			 * Hence, make an enter key showing we can reach the arc
			 * destination with all-blank prefix.
			 */
			destKey.prefix.colors[0] = COLOR_BLANK;
			destKey.prefix.colors[1] = COLOR_BLANK;
			destKey.nstate = arc->to;

			/* Add enter key to this state */
			addKeyToQueue(trgmNFA, &destKey);
		}
		else if (pg_reg_colorisend(trgmNFA->regex, arc->co))
		{
			/*
			 * End of line/string ($).  We must consider this arc as a
			 * transition that doesn't read anything.  The reason for adding
			 * this enter key to the state is that if the arc leads to the
			 * NFA's final state, we must mark this expanded state as final.
			 */
			destKey.prefix.colors[0] = COLOR_UNKNOWN;
			destKey.prefix.colors[1] = COLOR_UNKNOWN;
			destKey.nstate = arc->to;

			/* Add enter key to this state */
			addKeyToQueue(trgmNFA, &destKey);
		}
		else if (arc->co >= 0)
		{
			/* Regular color (including WHITE) */
			TrgmColorInfo *colorInfo = &trgmNFA->colorInfo[arc->co];

			if (colorInfo->expandable)
			{
				if (colorInfo->containsNonWord &&
					!validArcLabel(key, COLOR_BLANK))
				{
					/*
					 * We can reach the arc destination after reading a
					 * non-word character, but the prefix is not something
					 * that addArc will accept with COLOR_BLANK, so no trigram
					 * arc can get made for this transition.  We must make an
					 * enter key to show that the arc destination is
					 * reachable.  Set it up with an all-blank prefix, since
					 * that corresponds to what the trigram extraction code
					 * will do at a word starting boundary.
					 */
					destKey.prefix.colors[0] = COLOR_BLANK;
					destKey.prefix.colors[1] = COLOR_BLANK;
					destKey.nstate = arc->to;
					addKeyToQueue(trgmNFA, &destKey);
				}

				if (colorInfo->wordCharsCount > 0 &&
					!validArcLabel(key, arc->co))
				{
					/*
					 * We can reach the arc destination after reading a word
					 * character, but the prefix is not something that addArc
					 * will accept, so no trigram arc can get made for this
					 * transition.  We must make an enter key to show that the
					 * arc destination is reachable.  The prefix for the enter
					 * key should reflect the info we have for this arc.
					 */
					destKey.prefix.colors[0] = key->prefix.colors[1];
					destKey.prefix.colors[1] = arc->co;
					destKey.nstate = arc->to;
					addKeyToQueue(trgmNFA, &destKey);
				}
			}
			else
			{
				/*
				 * Unexpandable color.  Add enter key with ambiguous prefix,
				 * showing we can reach the destination from this state, but
				 * the preceding colors will be uncertain.  (We do not set the
				 * first prefix color to key->prefix.colors[1], because a
				 * prefix of known followed by unknown is invalid.)
				 */
				destKey.prefix.colors[0] = COLOR_UNKNOWN;
				destKey.prefix.colors[1] = COLOR_UNKNOWN;
				destKey.nstate = arc->to;
				addKeyToQueue(trgmNFA, &destKey);
			}
		}
		else
		{
			/* RAINBOW: treat as unexpandable color */
			destKey.prefix.colors[0] = COLOR_UNKNOWN;
			destKey.prefix.colors[1] = COLOR_UNKNOWN;
			destKey.nstate = arc->to;
			addKeyToQueue(trgmNFA, &destKey);
		}
	}

	pfree(arcs);
}

/*
 * Add copy of given key to keysQueue for later processing.
 */
static void
addKeyToQueue(TrgmNFA *trgmNFA, TrgmStateKey *key)
{
	TrgmStateKey *keyCopy = (TrgmStateKey *) palloc(sizeof(TrgmStateKey));

	memcpy(keyCopy, key, sizeof(TrgmStateKey));
	trgmNFA->keysQueue = lappend(trgmNFA->keysQueue, keyCopy);
}

/*
 * Add outgoing arcs from given state, whose enter keys are all now known.
 */
static void
addArcs(TrgmNFA *trgmNFA, TrgmState *state)
{
	TrgmStateKey destKey;
	ListCell   *cell;
	regex_arc_t *arcs;
	int			arcsCount,
				i;

	/*
	 * Ensure any pad bytes in destKey are zero, since it may get used as a
	 * hashtable key by getState.
	 */
	MemSet(&destKey, 0, sizeof(destKey));

	/*
	 * Iterate over enter keys associated with this expanded-graph state. This
	 * includes both the state's own stateKey, and any enter keys we added to
	 * it during addKey (which represent expanded-graph states that are not
	 * distinguishable from this one by means of trigrams).  For each such
	 * enter key, examine all the out-arcs of the key's underlying NFA state,
	 * and try to make a trigram arc leading to where the out-arc leads.
	 * (addArc will deal with whether the arc is valid or not.)
	 */
	foreach(cell, state->enterKeys)
	{
		TrgmStateKey *key = (TrgmStateKey *) lfirst(cell);

		arcsCount = pg_reg_getnumoutarcs(trgmNFA->regex, key->nstate);
		arcs = (regex_arc_t *) palloc(sizeof(regex_arc_t) * arcsCount);
		pg_reg_getoutarcs(trgmNFA->regex, key->nstate, arcs, arcsCount);

		for (i = 0; i < arcsCount; i++)
		{
			regex_arc_t *arc = &arcs[i];
			TrgmColorInfo *colorInfo;

			/*
			 * Ignore non-expandable colors; addKey already handled the case.
			 *
			 * We need no special check for WHITE or begin/end pseudocolors
			 * here.  We don't need to do any processing for them, and they
			 * will be marked non-expandable since the regex engine will have
			 * reported them that way.  We do have to watch out for RAINBOW,
			 * which has a negative color number.
			 */
			if (arc->co < 0)
				continue;
			Assert(arc->co < trgmNFA->ncolors);

			colorInfo = &trgmNFA->colorInfo[arc->co];
			if (!colorInfo->expandable)
				continue;

			if (colorInfo->containsNonWord)
			{
				/*
				 * Color includes non-word character(s).
				 *
				 * Generate an arc, treating this transition as occurring on
				 * BLANK.  This allows word-ending trigrams to be manufactured
				 * if possible.
				 */
				destKey.prefix.colors[0] = key->prefix.colors[1];
				destKey.prefix.colors[1] = COLOR_BLANK;
				destKey.nstate = arc->to;

				addArc(trgmNFA, state, key, COLOR_BLANK, &destKey);
			}

			if (colorInfo->wordCharsCount > 0)
			{
				/*
				 * Color includes word character(s).
				 *
				 * Generate an arc.  Color is pushed into prefix of target
				 * state.
				 */
				destKey.prefix.colors[0] = key->prefix.colors[1];
				destKey.prefix.colors[1] = arc->co;
				destKey.nstate = arc->to;

				addArc(trgmNFA, state, key, arc->co, &destKey);
			}
		}

		pfree(arcs);
	}
}

/*
 * Generate an out-arc of the expanded graph, if it's valid and not redundant.
 *
 * state: expanded-graph state we want to add an out-arc to
 * key: provides prefix colors (key->nstate is not used)
 * co: transition color
 * destKey: identifier for destination state of expanded graph
 */
static void
addArc(TrgmNFA *trgmNFA, TrgmState *state, TrgmStateKey *key,
	   TrgmColor co, TrgmStateKey *destKey)
{
	TrgmArc    *arc;
	ListCell   *cell;

	/* Do nothing if this wouldn't be a valid arc label trigram */
	if (!validArcLabel(key, co))
		return;

	/*
	 * Check if we are going to reach key which is covered by a key which is
	 * already listed in this state.  If so arc is useless: the NFA can bypass
	 * it through a path that doesn't require any predictable trigram, so
	 * whether the arc's trigram is present or not doesn't really matter.
	 */
	foreach(cell, state->enterKeys)
	{
		TrgmStateKey *existingKey = (TrgmStateKey *) lfirst(cell);

		if (existingKey->nstate == destKey->nstate &&
			prefixContains(&existingKey->prefix, &destKey->prefix))
			return;
	}

	/* Checks were successful, add new arc */
	arc = (TrgmArc *) palloc(sizeof(TrgmArc));
	arc->target = getState(trgmNFA, destKey);
	arc->ctrgm.colors[0] = key->prefix.colors[0];
	arc->ctrgm.colors[1] = key->prefix.colors[1];
	arc->ctrgm.colors[2] = co;

	state->arcs = lappend(state->arcs, arc);
	trgmNFA->arcsCount++;
}

/*
 * Can we make a valid trigram arc label from the given prefix and arc color?
 *
 * This is split out so that tests in addKey and addArc will stay in sync.
 */
static bool
validArcLabel(TrgmStateKey *key, TrgmColor co)
{
	/*
	 * We have to know full trigram in order to add outgoing arc.  So we can't
	 * do it if prefix is ambiguous.
	 */
	if (key->prefix.colors[0] == COLOR_UNKNOWN)
		return false;

	/* If key->prefix.colors[0] isn't unknown, its second color isn't either */
	Assert(key->prefix.colors[1] != COLOR_UNKNOWN);
	/* And we should not be called with an unknown arc color anytime */
	Assert(co != COLOR_UNKNOWN);

	/*
	 * We don't bother with making arcs representing three non-word
	 * characters, since that's useless for trigram extraction.
	 */
	if (key->prefix.colors[0] == COLOR_BLANK &&
		key->prefix.colors[1] == COLOR_BLANK &&
		co == COLOR_BLANK)
		return false;

	/*
	 * We also reject nonblank-blank-anything.  The nonblank-blank-nonblank
	 * case doesn't correspond to any trigram the trigram extraction code
	 * would make.  The nonblank-blank-blank case is also not possible with
	 * RPADDING = 1.  (Note that in many cases we'd fail to generate such a
	 * trigram even if it were valid, for example processing "foo bar" will
	 * not result in considering the trigram "o  ".  So if you want to support
	 * RPADDING = 2, there's more to do than just twiddle this test.)
	 */
	if (key->prefix.colors[0] != COLOR_BLANK &&
		key->prefix.colors[1] == COLOR_BLANK)
		return false;

	/*
	 * Other combinations involving blank are valid, in particular we assume
	 * blank-blank-nonblank is valid, which presumes that LPADDING is 2.
	 *
	 * Note: Using again the example "foo bar", we will not consider the
	 * trigram "  b", though this trigram would be found by the trigram
	 * extraction code.  Since we will find " ba", it doesn't seem worth
	 * trying to hack the algorithm to generate the additional trigram.
	 */

	/* arc label is valid */
	return true;
}

/*
 * Get state of expanded graph for given state key,
 * and queue the state for processing if it didn't already exist.
 */
static TrgmState *
getState(TrgmNFA *trgmNFA, TrgmStateKey *key)
{
	TrgmState  *state;
	bool		found;

	state = (TrgmState *) hash_search(trgmNFA->states, key, HASH_ENTER,
									  &found);
	if (!found)
	{
		/* New state: initialize and queue it */
		state->arcs = NIL;
		state->enterKeys = NIL;
		state->flags = 0;
		/* states are initially given negative numbers */
		state->snumber = -(++trgmNFA->nstates);
		state->parent = NULL;
		state->tentFlags = 0;
		state->tentParent = NULL;

		trgmNFA->queue = lappend(trgmNFA->queue, state);
	}
	return state;
}

/*
 * Check if prefix1 "contains" prefix2.
 *
 * "contains" means that any exact prefix (with no ambiguity) that satisfies
 * prefix2 also satisfies prefix1.
 */
static bool
prefixContains(TrgmPrefix *prefix1, TrgmPrefix *prefix2)
{
	if (prefix1->colors[1] == COLOR_UNKNOWN)
	{
		/* Fully ambiguous prefix contains everything */
		return true;
	}
	else if (prefix1->colors[0] == COLOR_UNKNOWN)
	{
		/*
		 * Prefix with only first unknown color contains every prefix with
		 * same second color.
		 */
		if (prefix1->colors[1] == prefix2->colors[1])
			return true;
		else
			return false;
	}
	else
	{
		/* Exact prefix contains only the exact same prefix */
		if (prefix1->colors[0] == prefix2->colors[0] &&
			prefix1->colors[1] == prefix2->colors[1])
			return true;
		else
			return false;
	}
}


/*---------------------
 * Subroutines for expanding color trigrams into regular trigrams (stage 3).
 *---------------------
 */

/*
 * Get vector of all color trigrams in graph and select which of them
 * to expand into simple trigrams.
 *
 * Returns true if OK, false if exhausted resource limits.
 */
static bool
selectColorTrigrams(TrgmNFA *trgmNFA)
{
	HASH_SEQ_STATUS scan_status;
	int			arcsCount = trgmNFA->arcsCount,
				i;
	TrgmState  *state;
	ColorTrgmInfo *colorTrgms;
	int64		totalTrgmCount;
	float4		totalTrgmPenalty;
	int			cnumber;

	/* Collect color trigrams from all arcs */
	colorTrgms = (ColorTrgmInfo *) palloc0(sizeof(ColorTrgmInfo) * arcsCount);
	trgmNFA->colorTrgms = colorTrgms;

	i = 0;
	hash_seq_init(&scan_status, trgmNFA->states);
	while ((state = (TrgmState *) hash_seq_search(&scan_status)) != NULL)
	{
		ListCell   *cell;

		foreach(cell, state->arcs)
		{
			TrgmArc    *arc = (TrgmArc *) lfirst(cell);
			TrgmArcInfo *arcInfo = (TrgmArcInfo *) palloc(sizeof(TrgmArcInfo));
			ColorTrgmInfo *trgmInfo = &colorTrgms[i];

			arcInfo->source = state;
			arcInfo->target = arc->target;
			trgmInfo->ctrgm = arc->ctrgm;
			trgmInfo->cnumber = -1;
			/* count and penalty will be set below */
			trgmInfo->expanded = true;
			trgmInfo->arcs = list_make1(arcInfo);
			i++;
		}
	}
	Assert(i == arcsCount);

	/* Remove duplicates, merging their arcs lists */
	if (arcsCount >= 2)
	{
		ColorTrgmInfo *p1,
				   *p2;

		/* Sort trigrams to ease duplicate detection */
		qsort(colorTrgms, arcsCount, sizeof(ColorTrgmInfo), colorTrgmInfoCmp);

		/* p1 is probe point, p2 is last known non-duplicate. */
		p2 = colorTrgms;
		for (p1 = colorTrgms + 1; p1 < colorTrgms + arcsCount; p1++)
		{
			if (colorTrgmInfoCmp(p1, p2) > 0)
			{
				p2++;
				*p2 = *p1;
			}
			else
			{
				p2->arcs = list_concat(p2->arcs, p1->arcs);
			}
		}
		trgmNFA->colorTrgmsCount = (p2 - colorTrgms) + 1;
	}
	else
	{
		trgmNFA->colorTrgmsCount = arcsCount;
	}

	/*
	 * Count number of simple trigrams generated by each color trigram, and
	 * also compute a penalty value, which is the number of simple trigrams
	 * times a multiplier that depends on its whitespace content.
	 *
	 * Note: per-color-trigram counts cannot overflow an int so long as
	 * COLOR_COUNT_LIMIT is not more than the cube root of INT_MAX, ie about
	 * 1290.  However, the grand total totalTrgmCount might conceivably
	 * overflow an int, so we use int64 for that within this routine.  Also,
	 * penalties are calculated in float4 arithmetic to avoid any overflow
	 * worries.
	 */
	totalTrgmCount = 0;
	totalTrgmPenalty = 0.0f;
	for (i = 0; i < trgmNFA->colorTrgmsCount; i++)
	{
		ColorTrgmInfo *trgmInfo = &colorTrgms[i];
		int			j,
					count = 1,
					typeIndex = 0;

		for (j = 0; j < 3; j++)
		{
			TrgmColor	c = trgmInfo->ctrgm.colors[j];

			typeIndex *= 2;
			if (c == COLOR_BLANK)
				typeIndex++;
			else
				count *= trgmNFA->colorInfo[c].wordCharsCount;
		}
		trgmInfo->count = count;
		totalTrgmCount += count;
		trgmInfo->penalty = penalties[typeIndex] * (float4) count;
		totalTrgmPenalty += trgmInfo->penalty;
	}

	/* Sort color trigrams in descending order of their penalties */
	qsort(colorTrgms, trgmNFA->colorTrgmsCount, sizeof(ColorTrgmInfo),
		  colorTrgmInfoPenaltyCmp);

	/*
	 * Remove color trigrams from the graph so long as total penalty of color
	 * trigrams exceeds WISH_TRGM_PENALTY.  (If we fail to get down to
	 * WISH_TRGM_PENALTY, it's OK so long as total count is no more than
	 * MAX_TRGM_COUNT.)  We prefer to remove color trigrams with higher
	 * penalty, since those are the most promising for reducing the total
	 * penalty.  When removing a color trigram we have to merge states
	 * connected by arcs labeled with that trigram.  It's necessary to not
	 * merge initial and final states, because our graph becomes useless if
	 * that happens; so we cannot always remove the trigram we'd prefer to.
	 */
	for (i = 0; i < trgmNFA->colorTrgmsCount; i++)
	{
		ColorTrgmInfo *trgmInfo = &colorTrgms[i];
		bool		canRemove = true;
		ListCell   *cell;

		/* Done if we've reached the target */
		if (totalTrgmPenalty <= WISH_TRGM_PENALTY)
			break;

#ifdef TRGM_REGEXP_DEBUG
		fprintf(stderr, "considering ctrgm %d %d %d, penalty %f, %d arcs\n",
				trgmInfo->ctrgm.colors[0],
				trgmInfo->ctrgm.colors[1],
				trgmInfo->ctrgm.colors[2],
				trgmInfo->penalty,
				list_length(trgmInfo->arcs));
#endif

		/*
		 * Does any arc of this color trigram connect initial and final
		 * states?	If so we can't remove it.
		 */
		foreach(cell, trgmInfo->arcs)
		{
			TrgmArcInfo *arcInfo = (TrgmArcInfo *) lfirst(cell);
			TrgmState  *source = arcInfo->source,
					   *target = arcInfo->target;
			int			source_flags,
						target_flags;

#ifdef TRGM_REGEXP_DEBUG
			fprintf(stderr, "examining arc to s%d (%x) from s%d (%x)\n",
					-target->snumber, target->flags,
					-source->snumber, source->flags);
#endif

			/* examine parent states, if any merging has already happened */
			while (source->parent)
				source = source->parent;
			while (target->parent)
				target = target->parent;

#ifdef TRGM_REGEXP_DEBUG
			fprintf(stderr, " ... after completed merges: to s%d (%x) from s%d (%x)\n",
					-target->snumber, target->flags,
					-source->snumber, source->flags);
#endif

			/* we must also consider merges we are planning right now */
			source_flags = source->flags | source->tentFlags;
			while (source->tentParent)
			{
				source = source->tentParent;
				source_flags |= source->flags | source->tentFlags;
			}
			target_flags = target->flags | target->tentFlags;
			while (target->tentParent)
			{
				target = target->tentParent;
				target_flags |= target->flags | target->tentFlags;
			}

#ifdef TRGM_REGEXP_DEBUG
			fprintf(stderr, " ... after tentative merges: to s%d (%x) from s%d (%x)\n",
					-target->snumber, target_flags,
					-source->snumber, source_flags);
#endif

			/* would fully-merged state have both INIT and FIN set? */
			if (((source_flags | target_flags) & (TSTATE_INIT | TSTATE_FIN)) ==
				(TSTATE_INIT | TSTATE_FIN))
			{
				canRemove = false;
				break;
			}

			/* ok so far, so remember planned merge */
			if (source != target)
			{
#ifdef TRGM_REGEXP_DEBUG
				fprintf(stderr, " ... tentatively merging s%d into s%d\n",
						-target->snumber, -source->snumber);
#endif
				target->tentParent = source;
				source->tentFlags |= target_flags;
			}
		}

		/*
		 * We must reset all the tentFlags/tentParent fields before
		 * continuing.  tentFlags could only have become set in states that
		 * are the source or parent or tentative parent of one of the current
		 * arcs; likewise tentParent could only have become set in states that
		 * are the target or parent or tentative parent of one of the current
		 * arcs.  There might be some overlap between those sets, but if we
		 * clear tentFlags in target states as well as source states, we
		 * should be okay even if we visit a state as target before visiting
		 * it as a source.
		 */
		foreach(cell, trgmInfo->arcs)
		{
			TrgmArcInfo *arcInfo = (TrgmArcInfo *) lfirst(cell);
			TrgmState  *source = arcInfo->source,
					   *target = arcInfo->target;
			TrgmState  *ttarget;

			/* no need to touch previously-merged states */
			while (source->parent)
				source = source->parent;
			while (target->parent)
				target = target->parent;

			while (source)
			{
				source->tentFlags = 0;
				source = source->tentParent;
			}

			while ((ttarget = target->tentParent) != NULL)
			{
				target->tentParent = NULL;
				target->tentFlags = 0;	/* in case it was also a source */
				target = ttarget;
			}
		}

		/* Now, move on if we can't drop this trigram */
		if (!canRemove)
		{
#ifdef TRGM_REGEXP_DEBUG
			fprintf(stderr, " ... not ok to merge\n");
#endif
			continue;
		}

		/* OK, merge states linked by each arc labeled by the trigram */
		foreach(cell, trgmInfo->arcs)
		{
			TrgmArcInfo *arcInfo = (TrgmArcInfo *) lfirst(cell);
			TrgmState  *source = arcInfo->source,
					   *target = arcInfo->target;

			while (source->parent)
				source = source->parent;
			while (target->parent)
				target = target->parent;
			if (source != target)
			{
#ifdef TRGM_REGEXP_DEBUG
				fprintf(stderr, "merging s%d into s%d\n",
						-target->snumber, -source->snumber);
#endif
				mergeStates(source, target);
				/* Assert we didn't merge initial and final states */
				Assert((source->flags & (TSTATE_INIT | TSTATE_FIN)) !=
					   (TSTATE_INIT | TSTATE_FIN));
			}
		}

		/* Mark trigram unexpanded, and update totals */
		trgmInfo->expanded = false;
		totalTrgmCount -= trgmInfo->count;
		totalTrgmPenalty -= trgmInfo->penalty;
	}

	/* Did we succeed in fitting into MAX_TRGM_COUNT? */
	if (totalTrgmCount > MAX_TRGM_COUNT)
		return false;

	trgmNFA->totalTrgmCount = (int) totalTrgmCount;

	/*
	 * Sort color trigrams by colors (will be useful for bsearch in packGraph)
	 * and enumerate the color trigrams that are expanded.
	 */
	cnumber = 0;
	qsort(colorTrgms, trgmNFA->colorTrgmsCount, sizeof(ColorTrgmInfo),
		  colorTrgmInfoCmp);
	for (i = 0; i < trgmNFA->colorTrgmsCount; i++)
	{
		if (colorTrgms[i].expanded)
		{
			colorTrgms[i].cnumber = cnumber;
			cnumber++;
		}
	}

	return true;
}

/*
 * Expand selected color trigrams into regular trigrams.
 *
 * Returns the TRGM array to be passed to the index machinery.
 * The array must be allocated in rcontext.
 */
static TRGM *
expandColorTrigrams(TrgmNFA *trgmNFA, MemoryContext rcontext)
{
	TRGM	   *trg;
	trgm	   *p;
	int			i;
	TrgmColorInfo blankColor;
	trgm_mb_char blankChar;

	/* Set up "blank" color structure containing a single zero character */
	memset(blankChar.bytes, 0, sizeof(blankChar.bytes));
	blankColor.wordCharsCount = 1;
	blankColor.wordChars = &blankChar;

	/* Construct the trgm array */
	trg = (TRGM *)
		MemoryContextAllocZero(rcontext,
							   TRGMHDRSIZE +
							   trgmNFA->totalTrgmCount * sizeof(trgm));
	trg->flag = ARRKEY;
	SET_VARSIZE(trg, CALCGTSIZE(ARRKEY, trgmNFA->totalTrgmCount));
	p = GETARR(trg);
	for (i = 0; i < trgmNFA->colorTrgmsCount; i++)
	{
		ColorTrgmInfo *colorTrgm = &trgmNFA->colorTrgms[i];
		TrgmColorInfo *c[3];
		trgm_mb_char s[3];
		int			j,
					i1,
					i2,
					i3;

		/* Ignore any unexpanded trigrams ... */
		if (!colorTrgm->expanded)
			continue;

		/* Get colors, substituting the dummy struct for COLOR_BLANK */
		for (j = 0; j < 3; j++)
		{
			if (colorTrgm->ctrgm.colors[j] != COLOR_BLANK)
				c[j] = &trgmNFA->colorInfo[colorTrgm->ctrgm.colors[j]];
			else
				c[j] = &blankColor;
		}

		/* Iterate over all possible combinations of colors' characters */
		for (i1 = 0; i1 < c[0]->wordCharsCount; i1++)
		{
			s[0] = c[0]->wordChars[i1];
			for (i2 = 0; i2 < c[1]->wordCharsCount; i2++)
			{
				s[1] = c[1]->wordChars[i2];
				for (i3 = 0; i3 < c[2]->wordCharsCount; i3++)
				{
					s[2] = c[2]->wordChars[i3];
					fillTrgm(p, s);
					p++;
				}
			}
		}
	}

	return trg;
}

/*
 * Convert trigram into trgm datatype.
 */
static void
fillTrgm(trgm *ptrgm, trgm_mb_char s[3])
{
	char		str[3 * MAX_MULTIBYTE_CHAR_LEN],
			   *p;
	int			i,
				j;

	/* Write multibyte string into "str" (we don't need null termination) */
	p = str;

	for (i = 0; i < 3; i++)
	{
		if (s[i].bytes[0] != 0)
		{
			for (j = 0; j < MAX_MULTIBYTE_CHAR_LEN && s[i].bytes[j]; j++)
				*p++ = s[i].bytes[j];
		}
		else
		{
			/* Emit a space in place of COLOR_BLANK */
			*p++ = ' ';
		}
	}

	/* Convert "str" to a standard trigram (possibly hashing it) */
	compact_trigram(ptrgm, str, p - str);
}

/*
 * Merge two states of graph.
 */
static void
mergeStates(TrgmState *state1, TrgmState *state2)
{
	Assert(state1 != state2);
	Assert(!state1->parent);
	Assert(!state2->parent);

	/* state1 absorbs state2's flags */
	state1->flags |= state2->flags;

	/* state2, and indirectly all its children, become children of state1 */
	state2->parent = state1;
}

/*
 * Compare function for sorting of color trigrams by their colors.
 */
static int
colorTrgmInfoCmp(const void *p1, const void *p2)
{
	const ColorTrgmInfo *c1 = (const ColorTrgmInfo *) p1;
	const ColorTrgmInfo *c2 = (const ColorTrgmInfo *) p2;

	return memcmp(&c1->ctrgm, &c2->ctrgm, sizeof(ColorTrgm));
}

/*
 * Compare function for sorting color trigrams in descending order of
 * their penalty fields.
 */
static int
colorTrgmInfoPenaltyCmp(const void *p1, const void *p2)
{
	float4		penalty1 = ((const ColorTrgmInfo *) p1)->penalty;
	float4		penalty2 = ((const ColorTrgmInfo *) p2)->penalty;

	if (penalty1 < penalty2)
		return 1;
	else if (penalty1 == penalty2)
		return 0;
	else
		return -1;
}


/*---------------------
 * Subroutines for packing the graph into final representation (stage 4).
 *---------------------
 */

/*
 * Pack expanded graph into final representation.
 *
 * The result data must be allocated in rcontext.
 */
static TrgmPackedGraph *
packGraph(TrgmNFA *trgmNFA, MemoryContext rcontext)
{
	int			snumber = 2,
				arcIndex,
				arcsCount;
	HASH_SEQ_STATUS scan_status;
	TrgmState  *state;
	TrgmPackArcInfo *arcs;
	TrgmPackedArc *packedArcs;
	TrgmPackedGraph *result;
	int			i,
				j;

	/* Enumerate surviving states, giving init and fin reserved numbers */
	hash_seq_init(&scan_status, trgmNFA->states);
	while ((state = (TrgmState *) hash_seq_search(&scan_status)) != NULL)
	{
		while (state->parent)
			state = state->parent;

		if (state->snumber < 0)
		{
			if (state->flags & TSTATE_INIT)
				state->snumber = 0;
			else if (state->flags & TSTATE_FIN)
				state->snumber = 1;
			else
			{
				state->snumber = snumber;
				snumber++;
			}
		}
	}

	/* Collect array of all arcs */
	arcs = (TrgmPackArcInfo *)
		palloc(sizeof(TrgmPackArcInfo) * trgmNFA->arcsCount);
	arcIndex = 0;
	hash_seq_init(&scan_status, trgmNFA->states);
	while ((state = (TrgmState *) hash_seq_search(&scan_status)) != NULL)
	{
		TrgmState  *source = state;
		ListCell   *cell;

		while (source->parent)
			source = source->parent;

		foreach(cell, state->arcs)
		{
			TrgmArc    *arc = (TrgmArc *) lfirst(cell);
			TrgmState  *target = arc->target;

			while (target->parent)
				target = target->parent;

			if (source->snumber != target->snumber)
			{
				ColorTrgmInfo *ctrgm;

				ctrgm = (ColorTrgmInfo *) bsearch(&arc->ctrgm,
												  trgmNFA->colorTrgms,
												  trgmNFA->colorTrgmsCount,
												  sizeof(ColorTrgmInfo),
												  colorTrgmInfoCmp);
				Assert(ctrgm != NULL);
				Assert(ctrgm->expanded);

				arcs[arcIndex].sourceState = source->snumber;
				arcs[arcIndex].targetState = target->snumber;
				arcs[arcIndex].colorTrgm = ctrgm->cnumber;
				arcIndex++;
			}
		}
	}

	/* Sort arcs to ease duplicate detection */
	qsort(arcs, arcIndex, sizeof(TrgmPackArcInfo), packArcInfoCmp);

	/* We could have duplicates because states were merged. Remove them. */
	if (arcIndex > 1)
	{
		/* p1 is probe point, p2 is last known non-duplicate. */
		TrgmPackArcInfo *p1,
				   *p2;

		p2 = arcs;
		for (p1 = arcs + 1; p1 < arcs + arcIndex; p1++)
		{
			if (packArcInfoCmp(p1, p2) > 0)
			{
				p2++;
				*p2 = *p1;
			}
		}
		arcsCount = (p2 - arcs) + 1;
	}
	else
		arcsCount = arcIndex;

	/* Create packed representation */
	result = (TrgmPackedGraph *)
		MemoryContextAlloc(rcontext, sizeof(TrgmPackedGraph));

	/* Pack color trigrams information */
	result->colorTrigramsCount = 0;
	for (i = 0; i < trgmNFA->colorTrgmsCount; i++)
	{
		if (trgmNFA->colorTrgms[i].expanded)
			result->colorTrigramsCount++;
	}
	result->colorTrigramGroups = (int *)
		MemoryContextAlloc(rcontext, sizeof(int) * result->colorTrigramsCount);
	j = 0;
	for (i = 0; i < trgmNFA->colorTrgmsCount; i++)
	{
		if (trgmNFA->colorTrgms[i].expanded)
		{
			result->colorTrigramGroups[j] = trgmNFA->colorTrgms[i].count;
			j++;
		}
	}

	/* Pack states and arcs information */
	result->statesCount = snumber;
	result->states = (TrgmPackedState *)
		MemoryContextAlloc(rcontext, snumber * sizeof(TrgmPackedState));
	packedArcs = (TrgmPackedArc *)
		MemoryContextAlloc(rcontext, arcsCount * sizeof(TrgmPackedArc));
	j = 0;
	for (i = 0; i < snumber; i++)
	{
		int			cnt = 0;

		result->states[i].arcs = &packedArcs[j];
		while (j < arcsCount && arcs[j].sourceState == i)
		{
			packedArcs[j].targetState = arcs[j].targetState;
			packedArcs[j].colorTrgm = arcs[j].colorTrgm;
			cnt++;
			j++;
		}
		result->states[i].arcsCount = cnt;
	}

	/* Allocate working memory for trigramsMatchGraph() */
	result->colorTrigramsActive = (bool *)
		MemoryContextAlloc(rcontext, sizeof(bool) * result->colorTrigramsCount);
	result->statesActive = (bool *)
		MemoryContextAlloc(rcontext, sizeof(bool) * result->statesCount);
	result->statesQueue = (int *)
		MemoryContextAlloc(rcontext, sizeof(int) * result->statesCount);

	return result;
}

/*
 * Comparison function for sorting TrgmPackArcInfos.
 *
 * Compares arcs in following order: sourceState, colorTrgm, targetState.
 */
static int
packArcInfoCmp(const void *a1, const void *a2)
{
	const TrgmPackArcInfo *p1 = (const TrgmPackArcInfo *) a1;
	const TrgmPackArcInfo *p2 = (const TrgmPackArcInfo *) a2;

	if (p1->sourceState < p2->sourceState)
		return -1;
	if (p1->sourceState > p2->sourceState)
		return 1;
	if (p1->colorTrgm < p2->colorTrgm)
		return -1;
	if (p1->colorTrgm > p2->colorTrgm)
		return 1;
	if (p1->targetState < p2->targetState)
		return -1;
	if (p1->targetState > p2->targetState)
		return 1;
	return 0;
}
/* ---- end VERBATIM contrib/pg_trgm/trgm_regexp.c lines 202-2114 ---- */


/* ========== fuzz-facing driver entries (NOT Postgres code) ========== */

#include <stdint.h>

/* ---- shim globals/stubs referenced by the vendored infrastructure ----
 * (decls in the trgmrxfam shim headers; see include/postgres.h header) */
static trgmrx_mcxt trgmrx_cxt_token;
MemoryContext CurrentMemoryContext = &trgmrx_cxt_token;
MemoryContext TopMemoryContext = &trgmrx_cxt_token;

/* dynahash seq-scan xact tracking: harness has no transactions; constant
 * nest level 1 (AtEOXact cleanup never runs). */
int
GetCurrentTransactionNestLevel(void)
{
	return 1;
}

/* node machinery referenced only by list.c's deep-copy/equality helpers
 * (list_copy_deep / list_member), which trgm_regexp never calls. */
void *
copyObjectImpl(const void *obj)
{
	(void) obj;
	abort();
}

bool
equal(const void *a, const void *b)
{
	(void) a; (void) b;
	abort();
}

/* shared-memory arms of dynahash: never requested (no HASH_SHARED_MEM);
 * loud if ever reached. */
void *
ShmemAllocNoError(Size size)
{
	(void) size;
	abort();
}

/* shmem.c add_size/mul_size semantics (overflow -> loud); reached only via
 * hash_estimate_size, which nothing here calls -- link plumbing. */
Size
add_size(Size s1, Size s2)
{
	Size		result = s1 + s2;

	if (result < s1)
		abort();
	return result;
}

Size
mul_size(Size s1, Size s2)
{
	if (s1 == 0 || s2 == 0)
		return 0;
	if (s1 * s2 / s2 != s1)
		abort();
	return s1 * s2;
}

/* The live-regex TLS slot (trgmrx_live_re/trgmrx_live) is declared next to
 * the compile wrapper that registers into it, above; trgmrx_enter below is
 * the release point. */

#define C_COLLATION_OID 950

static void
trgmrx_enter(void)
{
	/* Anchor this thread's engine stack-guard base (see the extern's
	 * comment block above) and clear the per-entry regcomp-code capture. */
	pg_diff_regex_stack_arm();
	trgmrx_last_regcomp_code = 0;
	if (trgmrx_live)
	{
		pg_regfree(&trgmrx_live_re);
		trgmrx_live = false;
	}
	pg_diff_trgm_bridge_enter(0);	/* locale arm 0 only (header) */
}

/* Post-longjmp class refinement: RE_compile maps every compile failure to
 * INVALID_RE; report REG_ETOOBIG as its own class (see class 5 above). */
static int
trgmrx_errclass(void)
{
	if (pg_diff_errcode == PG_DIFF_TRGM_ERR_INVALID_RE &&
		trgmrx_last_regcomp_code == REG_ETOOBIG)
		return PG_DIFF_TRGM_ERR_RE_TOO_COMPLEX;
	return pg_diff_errcode;
}

/*
 * Shared extraction core: compile `pat` and run createTrgmNFA.
 * Returns 0 success / -1 NULL-fallback; errors longjmp out.
 */
static int
trgmrx_extract(const uint8_t *pat, int len, TRGM **trg_out,
			   TrgmPackedGraph **graph_out)
{
	text	   *t;
	TRGM	   *trg;
	TrgmPackedGraph *graph = NULL;

	/* inline 4B-header text image (regexfam text shim shape) */
	t = (text *) palloc(len + 4);
	SET_VARSIZE(t, len + 4);
	memcpy(VARDATA(t), pat, len);

	trg = createTrgmNFA(t, C_COLLATION_OID, &graph, CurrentMemoryContext);
	if (trg == NULL)
		return -1;
	*trg_out = trg;
	*graph_out = graph;
	return 0;
}

/*
 * pg_diff_trgm_regexp: extraction verdict + trigram array + packed graph.
 *
 * rc: 0 = success, -1 = NULL fallback, >0 = errcode class.
 * On success:
 *   trg_out[0..3*ntrgms)          trigram bytes in C array order
 *   groups_out[0..ngroups)        colorTrigramGroups
 *   states_out[0..2*nstates)      per state: (arc offset, arc count) into
 *                                 arcs_out, in state-number order
 *   arcs_out[0..2*narcs)          per arc: (target state, color trigram)
 */
int
pg_diff_trgm_regexp(const uint8_t *pat, int len,
					uint8_t *trg_out, int trg_cap, int32_t *ntrgms,
					int32_t *groups_out, int groups_cap, int32_t *ngroups,
					int32_t *states_out, int states_cap, int32_t *nstates,
					int32_t *arcs_out, int arcs_cap, int32_t *narcs)
{
	TRGM	   *trg;
	TrgmPackedGraph *graph;
	int			rc;
	int			i;
	int			arcpos = 0;

	trgmrx_enter();
	if (setjmp(*pg_diff_trgm_bridge_jmp()) != 0)
		return trgmrx_errclass();
	rc = trgmrx_extract(pat, len, &trg, &graph);
	if (rc != 0)
		return rc;

	if (ARRNELEM(trg) * 3 > trg_cap)
		abort();				/* driver sizes caps from MAX_TRGM_COUNT */
	memcpy(trg_out, GETARR(trg), ARRNELEM(trg) * 3);
	*ntrgms = ARRNELEM(trg);

	if (graph->colorTrigramsCount > groups_cap)
		abort();
	for (i = 0; i < graph->colorTrigramsCount; i++)
		groups_out[i] = graph->colorTrigramGroups[i];
	*ngroups = graph->colorTrigramsCount;

	if (graph->statesCount * 2 > states_cap)
		abort();
	for (i = 0; i < graph->statesCount; i++)
	{
		int			j;

		states_out[2 * i] = arcpos;
		states_out[2 * i + 1] = graph->states[i].arcsCount;
		for (j = 0; j < graph->states[i].arcsCount; j++)
		{
			if (2 * (arcpos + 1) > arcs_cap)
				abort();
			arcs_out[2 * arcpos] = graph->states[i].arcs[j].targetState;
			arcs_out[2 * arcpos + 1] = graph->states[i].arcs[j].colorTrgm;
			arcpos++;
		}
	}
	*nstates = graph->statesCount;
	*narcs = arcpos;
	return 0;
}

/*
 * pg_diff_trgm_regexp_matches: re-extract (deterministic; arena makes it
 * cheap) and evaluate the VERBATIM trigramsMatchGraph on check[0..ncheck).
 * ncheck must equal the extraction's trigram count (abort = driver bug).
 * rc: 0 with *out_match set, -1 NULL fallback, >0 errcode class.
 */
int
pg_diff_trgm_regexp_matches(const uint8_t *pat, int len,
							const uint8_t *check, int ncheck,
							int32_t *out_match)
{
	TRGM	   *trg;
	TrgmPackedGraph *graph;
	int			rc;
	bool	   *boolcheck;
	int			i;

	trgmrx_enter();
	if (setjmp(*pg_diff_trgm_bridge_jmp()) != 0)
		return trgmrx_errclass();
	rc = trgmrx_extract(pat, len, &trg, &graph);
	if (rc != 0)
		return rc;
	if (ncheck != ARRNELEM(trg))
		abort();				/* driver passes the C-side count back */
	boolcheck = (bool *) palloc(ncheck ? ncheck : 1);
	for (i = 0; i < ncheck; i++)
		boolcheck[i] = (check[i] != 0);
	*out_match = trigramsMatchGraph(graph, boolcheck) ? 1 : 0;
	return 0;
}
