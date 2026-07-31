/*
 * pg_formatting_min.c — VERBATIM extracts of src/backend/utils/adt/formatting.c
 * @ postgres-src 62d6c7d3df6287f1bd83199c1a746e50d31571a0 (PostgreSQL 18.3):
 * exactly the call graph of datetime_format_has_tz() as used by
 * jspIsMutableWalker (jsonpath.c). Every extract carries a
 * "formatting.c:A-B VERBATIM" provenance marker (extract_verbatim.py).
 *
 * Extracted set: parser flag/index macros, KeySuffix/KeyWord/FormatNode
 * types, the DCH cache types + statics (DCH side only; the NUM-side cache
 * types are dropped, they name NUMDesc which this extract does not carry),
 * DCH_S_* suffix codes + DCH_suff, enum DCH_poz, DCH_keywords, DCH_index,
 * DCH_DATED/TIMED/ZONED, index_seq_search, suff_search, is_separator_char,
 * parse_format, DCH_datetime_type, DCH_cache_getnew, DCH_cache_search,
 * DCH_cache_fetch, datetime_format_has_tz.
 *
 * Shims (environment/plumbing only, never logic): the include set below
 * (shim postgres.h supplies palloc arena + ereport capture + Assert;
 * mb/pg_wchar.h supplies the pinned-UTF8 pg_mblen and
 * MAX_MULTIBYTE_CHAR_LEN); static forward decls replacing the dropped
 * originals (formatting.c:1119-1122 region names DCH-only entries here).
 * The DCH format-picture cache statics are carried verbatim: cache state is
 * keyed by the format string exactly as in a real backend and is
 * deterministic across iterations.
 */

#include "postgres.h"

#include <ctype.h>
#include <limits.h>

#include "mb/pg_wchar.h"
extern int pg_mblen_cstr(const char *mbstr);
#include "nodes/miscnodes.h"
#include "utils/formatting.h"
#include "utils/memutils.h"

/* shim: the DCH format-picture cache persists across calls BY DESIGN (real
 * PG allocates it once in a long-lived context and keeps pointers in the
 * static DCHCache[]); route this TU's palloc to plain malloc so the per-call
 * TLS arena reset cannot free live cache entries. Bounded state:
 * DCH_CACHE_ENTRIES (20) entries; oversized temps are pfree'd inside
 * datetime_format_has_tz itself. */
#undef palloc
#undef pfree
static inline void *
pg_formatting_persist_alloc(size_t n)
{
	void	   *p = malloc(n);

	if (p == NULL)
		abort();
	return p;
}
#define palloc(sz) pg_formatting_persist_alloc(sz)
#define pfree(p) free(p)

/* shim: persistent-cache allocation hooks (see the persist shim above) +
 * the datetime errcode this extract raises */
#define ERRCODE_INVALID_DATETIME_FORMAT MAKE_SQLSTATE('2','2','0','0','7')
#define TopMemoryContext ((void *) 0)
#define MemoryContextAllocZero(cxt, sz) pg_formatting_persist_calloc(sz)
static inline void *
pg_formatting_persist_calloc(size_t n)
{
	void	   *p = calloc(1, n);

	if (p == NULL)
		abort();
	return p;
}
static inline char *
pnstrdup_shim(const char *in, size_t len)
{
	char	   *p = pg_formatting_persist_alloc(len + 1);
	size_t		n = strnlen(in, len);

	memcpy(p, in, n);
	p[n] = '\0';
	return p;
}
#define pnstrdup(in, len) pnstrdup_shim((in), (len))


/* ---- formatting.c:95-172 VERBATIM ---- */
/* ----------
 * Routines flags
 * ----------
 */
#define DCH_FLAG		0x1		/* DATE-TIME flag	*/
#define NUM_FLAG		0x2		/* NUMBER flag	*/
#define STD_FLAG		0x4		/* STANDARD flag	*/

/* ----------
 * KeyWord Index (ascii from position 32 (' ') to 126 (~))
 * ----------
 */
#define KeyWord_INDEX_SIZE		('~' - ' ')
#define KeyWord_INDEX_FILTER(_c)	((_c) <= ' ' || (_c) >= '~' ? 0 : 1)

/* ----------
 * Maximal length of one node
 * ----------
 */
#define DCH_MAX_ITEM_SIZ	   12	/* max localized day name		*/
#define NUM_MAX_ITEM_SIZ		8	/* roman number (RN has 15 chars)	*/


/* ----------
 * Format parser structs
 * ----------
 */
typedef struct
{
	const char *name;			/* suffix string		*/
	int			len,			/* suffix length		*/
				id,				/* used in node->suffix */
				type;			/* prefix / postfix		*/
} KeySuffix;

/* ----------
 * FromCharDateMode
 * ----------
 *
 * This value is used to nominate one of several distinct (and mutually
 * exclusive) date conventions that a keyword can belong to.
 */
typedef enum
{
	FROM_CHAR_DATE_NONE = 0,	/* Value does not affect date mode. */
	FROM_CHAR_DATE_GREGORIAN,	/* Gregorian (day, month, year) style date */
	FROM_CHAR_DATE_ISOWEEK,		/* ISO 8601 week date */
} FromCharDateMode;

typedef struct
{
	const char *name;
	int			len;
	int			id;
	bool		is_digit;
	FromCharDateMode date_mode;
} KeyWord;

typedef struct
{
	uint8		type;			/* NODE_TYPE_XXX, see below */
	char		character[MAX_MULTIBYTE_CHAR_LEN + 1];	/* if type is CHAR */
	uint8		suffix;			/* keyword prefix/suffix code, if any */
	const KeyWord *key;			/* if type is ACTION */
} FormatNode;

#define NODE_TYPE_END		1
#define NODE_TYPE_ACTION	2
#define NODE_TYPE_CHAR		3
#define NODE_TYPE_SEPARATOR	4
#define NODE_TYPE_SPACE		5

#define SUFFTYPE_PREFIX		1
#define SUFFTYPE_POSTFIX	2

#define CLOCK_24_HOUR		0
#define CLOCK_12_HOUR		1


/* ---- formatting.c:381-401 VERBATIM ---- */
#define DCH_CACHE_OVERHEAD \
	MAXALIGN(sizeof(bool) + sizeof(int))
#define NUM_CACHE_OVERHEAD \
	MAXALIGN(sizeof(bool) + sizeof(int) + sizeof(NUMDesc))

#define DCH_CACHE_SIZE \
	((2048 - DCH_CACHE_OVERHEAD) / (sizeof(FormatNode) + sizeof(char)) - 1)
#define NUM_CACHE_SIZE \
	((1024 - NUM_CACHE_OVERHEAD) / (sizeof(FormatNode) + sizeof(char)) - 1)

#define DCH_CACHE_ENTRIES	20
#define NUM_CACHE_ENTRIES	20

typedef struct
{
	FormatNode	format[DCH_CACHE_SIZE + 1];
	char		str[DCH_CACHE_SIZE + 1];
	bool		std;
	bool		valid;
	int			age;
} DCHCacheEntry;

/* ---- formatting.c:411-415 VERBATIM ---- */

/* global cache for date/time format pictures */
static DCHCacheEntry *DCHCache[DCH_CACHE_ENTRIES];
static int	n_DCHCache = 0;		/* current number of entries */
static int	DCHCounter = 0;		/* aging-event counter */

/* ---- formatting.c:560-604 VERBATIM ---- */
/*****************************************************************************
 *			KeyWord definitions
 *****************************************************************************/

/* ----------
 * Suffixes (FormatNode.suffix is an OR of these codes)
 * ----------
 */
#define DCH_S_FM	0x01
#define DCH_S_TH	0x02
#define DCH_S_th	0x04
#define DCH_S_SP	0x08
#define DCH_S_TM	0x10

/* ----------
 * Suffix tests
 * ----------
 */
#define S_THth(_s)	((((_s) & DCH_S_TH) || ((_s) & DCH_S_th)) ? 1 : 0)
#define S_TH(_s)	(((_s) & DCH_S_TH) ? 1 : 0)
#define S_th(_s)	(((_s) & DCH_S_th) ? 1 : 0)
#define S_TH_TYPE(_s)	(((_s) & DCH_S_TH) ? TH_UPPER : TH_LOWER)

/* Oracle toggles FM behavior, we don't; see docs. */
#define S_FM(_s)	(((_s) & DCH_S_FM) ? 1 : 0)
#define S_SP(_s)	(((_s) & DCH_S_SP) ? 1 : 0)
#define S_TM(_s)	(((_s) & DCH_S_TM) ? 1 : 0)

/* ----------
 * Suffixes definition for DATE-TIME TO/FROM CHAR
 * ----------
 */
#define TM_SUFFIX_LEN	2

static const KeySuffix DCH_suff[] = {
	{"FM", 2, DCH_S_FM, SUFFTYPE_PREFIX},
	{"fm", 2, DCH_S_FM, SUFFTYPE_PREFIX},
	{"TM", TM_SUFFIX_LEN, DCH_S_TM, SUFFTYPE_PREFIX},
	{"tm", 2, DCH_S_TM, SUFFTYPE_PREFIX},
	{"TH", 2, DCH_S_TH, SUFFTYPE_POSTFIX},
	{"th", 2, DCH_S_th, SUFFTYPE_POSTFIX},
	{"SP", 2, DCH_S_SP, SUFFTYPE_POSTFIX},
	/* last */
	{NULL, 0, 0, 0}
};

/* ---- formatting.c:606-752 VERBATIM ---- */

/* ----------
 * Format-pictures (KeyWord).
 *
 * The KeyWord field; alphabetic sorted, *BUT* strings alike is sorted
 *		  complicated -to-> easy:
 *
 *	(example: "DDD","DD","Day","D" )
 *
 * (this specific sort needs the algorithm for sequential search for strings,
 * which not has exact end; -> How keyword is in "HH12blabla" ? - "HH"
 * or "HH12"? You must first try "HH12", because "HH" is in string, but
 * it is not good.
 *
 * (!)
 *	 - Position for the keyword is similar as position in the enum DCH/NUM_poz.
 * (!)
 *
 * For fast search is used the 'int index[]', index is ascii table from position
 * 32 (' ') to 126 (~), in this index is DCH_ / NUM_ enums for each ASCII
 * position or -1 if char is not used in the KeyWord. Search example for
 * string "MM":
 *	1)	see in index to index['M' - 32],
 *	2)	take keywords position (enum DCH_MI) from index
 *	3)	run sequential search in keywords[] from this position
 *
 * ----------
 */

typedef enum
{
	DCH_A_D,
	DCH_A_M,
	DCH_AD,
	DCH_AM,
	DCH_B_C,
	DCH_BC,
	DCH_CC,
	DCH_DAY,
	DCH_DDD,
	DCH_DD,
	DCH_DY,
	DCH_Day,
	DCH_Dy,
	DCH_D,
	DCH_FF1,					/* FFn codes must be consecutive */
	DCH_FF2,
	DCH_FF3,
	DCH_FF4,
	DCH_FF5,
	DCH_FF6,
	DCH_FX,						/* global suffix */
	DCH_HH24,
	DCH_HH12,
	DCH_HH,
	DCH_IDDD,
	DCH_ID,
	DCH_IW,
	DCH_IYYY,
	DCH_IYY,
	DCH_IY,
	DCH_I,
	DCH_J,
	DCH_MI,
	DCH_MM,
	DCH_MONTH,
	DCH_MON,
	DCH_MS,
	DCH_Month,
	DCH_Mon,
	DCH_OF,
	DCH_P_M,
	DCH_PM,
	DCH_Q,
	DCH_RM,
	DCH_SSSSS,
	DCH_SSSS,
	DCH_SS,
	DCH_TZH,
	DCH_TZM,
	DCH_TZ,
	DCH_US,
	DCH_WW,
	DCH_W,
	DCH_Y_YYY,
	DCH_YYYY,
	DCH_YYY,
	DCH_YY,
	DCH_Y,
	DCH_a_d,
	DCH_a_m,
	DCH_ad,
	DCH_am,
	DCH_b_c,
	DCH_bc,
	DCH_cc,
	DCH_day,
	DCH_ddd,
	DCH_dd,
	DCH_dy,
	DCH_d,
	DCH_ff1,
	DCH_ff2,
	DCH_ff3,
	DCH_ff4,
	DCH_ff5,
	DCH_ff6,
	DCH_fx,
	DCH_hh24,
	DCH_hh12,
	DCH_hh,
	DCH_iddd,
	DCH_id,
	DCH_iw,
	DCH_iyyy,
	DCH_iyy,
	DCH_iy,
	DCH_i,
	DCH_j,
	DCH_mi,
	DCH_mm,
	DCH_month,
	DCH_mon,
	DCH_ms,
	DCH_of,
	DCH_p_m,
	DCH_pm,
	DCH_q,
	DCH_rm,
	DCH_sssss,
	DCH_ssss,
	DCH_ss,
	DCH_tzh,
	DCH_tzm,
	DCH_tz,
	DCH_us,
	DCH_ww,
	DCH_w,
	DCH_y_yyy,
	DCH_yyyy,
	DCH_yyy,
	DCH_yy,
	DCH_y,

	/* last */
	_DCH_last_
}			DCH_poz;

/* ---- formatting.c:796-918 VERBATIM ---- */

/* ----------
 * KeyWords for DATE-TIME version
 * ----------
 */
static const KeyWord DCH_keywords[] = {
/*	name, len, id, is_digit, date_mode */
	{"A.D.", 4, DCH_A_D, false, FROM_CHAR_DATE_NONE},	/* A */
	{"A.M.", 4, DCH_A_M, false, FROM_CHAR_DATE_NONE},
	{"AD", 2, DCH_AD, false, FROM_CHAR_DATE_NONE},
	{"AM", 2, DCH_AM, false, FROM_CHAR_DATE_NONE},
	{"B.C.", 4, DCH_B_C, false, FROM_CHAR_DATE_NONE},	/* B */
	{"BC", 2, DCH_BC, false, FROM_CHAR_DATE_NONE},
	{"CC", 2, DCH_CC, true, FROM_CHAR_DATE_NONE},	/* C */
	{"DAY", 3, DCH_DAY, false, FROM_CHAR_DATE_NONE},	/* D */
	{"DDD", 3, DCH_DDD, true, FROM_CHAR_DATE_GREGORIAN},
	{"DD", 2, DCH_DD, true, FROM_CHAR_DATE_GREGORIAN},
	{"DY", 2, DCH_DY, false, FROM_CHAR_DATE_NONE},
	{"Day", 3, DCH_Day, false, FROM_CHAR_DATE_NONE},
	{"Dy", 2, DCH_Dy, false, FROM_CHAR_DATE_NONE},
	{"D", 1, DCH_D, true, FROM_CHAR_DATE_GREGORIAN},
	{"FF1", 3, DCH_FF1, true, FROM_CHAR_DATE_NONE}, /* F */
	{"FF2", 3, DCH_FF2, true, FROM_CHAR_DATE_NONE},
	{"FF3", 3, DCH_FF3, true, FROM_CHAR_DATE_NONE},
	{"FF4", 3, DCH_FF4, true, FROM_CHAR_DATE_NONE},
	{"FF5", 3, DCH_FF5, true, FROM_CHAR_DATE_NONE},
	{"FF6", 3, DCH_FF6, true, FROM_CHAR_DATE_NONE},
	{"FX", 2, DCH_FX, false, FROM_CHAR_DATE_NONE},
	{"HH24", 4, DCH_HH24, true, FROM_CHAR_DATE_NONE},	/* H */
	{"HH12", 4, DCH_HH12, true, FROM_CHAR_DATE_NONE},
	{"HH", 2, DCH_HH, true, FROM_CHAR_DATE_NONE},
	{"IDDD", 4, DCH_IDDD, true, FROM_CHAR_DATE_ISOWEEK},	/* I */
	{"ID", 2, DCH_ID, true, FROM_CHAR_DATE_ISOWEEK},
	{"IW", 2, DCH_IW, true, FROM_CHAR_DATE_ISOWEEK},
	{"IYYY", 4, DCH_IYYY, true, FROM_CHAR_DATE_ISOWEEK},
	{"IYY", 3, DCH_IYY, true, FROM_CHAR_DATE_ISOWEEK},
	{"IY", 2, DCH_IY, true, FROM_CHAR_DATE_ISOWEEK},
	{"I", 1, DCH_I, true, FROM_CHAR_DATE_ISOWEEK},
	{"J", 1, DCH_J, true, FROM_CHAR_DATE_NONE}, /* J */
	{"MI", 2, DCH_MI, true, FROM_CHAR_DATE_NONE},	/* M */
	{"MM", 2, DCH_MM, true, FROM_CHAR_DATE_GREGORIAN},
	{"MONTH", 5, DCH_MONTH, false, FROM_CHAR_DATE_GREGORIAN},
	{"MON", 3, DCH_MON, false, FROM_CHAR_DATE_GREGORIAN},
	{"MS", 2, DCH_MS, true, FROM_CHAR_DATE_NONE},
	{"Month", 5, DCH_Month, false, FROM_CHAR_DATE_GREGORIAN},
	{"Mon", 3, DCH_Mon, false, FROM_CHAR_DATE_GREGORIAN},
	{"OF", 2, DCH_OF, false, FROM_CHAR_DATE_NONE},	/* O */
	{"P.M.", 4, DCH_P_M, false, FROM_CHAR_DATE_NONE},	/* P */
	{"PM", 2, DCH_PM, false, FROM_CHAR_DATE_NONE},
	{"Q", 1, DCH_Q, true, FROM_CHAR_DATE_NONE}, /* Q */
	{"RM", 2, DCH_RM, false, FROM_CHAR_DATE_GREGORIAN}, /* R */
	{"SSSSS", 5, DCH_SSSS, true, FROM_CHAR_DATE_NONE},	/* S */
	{"SSSS", 4, DCH_SSSS, true, FROM_CHAR_DATE_NONE},
	{"SS", 2, DCH_SS, true, FROM_CHAR_DATE_NONE},
	{"TZH", 3, DCH_TZH, false, FROM_CHAR_DATE_NONE},	/* T */
	{"TZM", 3, DCH_TZM, true, FROM_CHAR_DATE_NONE},
	{"TZ", 2, DCH_TZ, false, FROM_CHAR_DATE_NONE},
	{"US", 2, DCH_US, true, FROM_CHAR_DATE_NONE},	/* U */
	{"WW", 2, DCH_WW, true, FROM_CHAR_DATE_GREGORIAN},	/* W */
	{"W", 1, DCH_W, true, FROM_CHAR_DATE_GREGORIAN},
	{"Y,YYY", 5, DCH_Y_YYY, true, FROM_CHAR_DATE_GREGORIAN},	/* Y */
	{"YYYY", 4, DCH_YYYY, true, FROM_CHAR_DATE_GREGORIAN},
	{"YYY", 3, DCH_YYY, true, FROM_CHAR_DATE_GREGORIAN},
	{"YY", 2, DCH_YY, true, FROM_CHAR_DATE_GREGORIAN},
	{"Y", 1, DCH_Y, true, FROM_CHAR_DATE_GREGORIAN},
	{"a.d.", 4, DCH_a_d, false, FROM_CHAR_DATE_NONE},	/* a */
	{"a.m.", 4, DCH_a_m, false, FROM_CHAR_DATE_NONE},
	{"ad", 2, DCH_ad, false, FROM_CHAR_DATE_NONE},
	{"am", 2, DCH_am, false, FROM_CHAR_DATE_NONE},
	{"b.c.", 4, DCH_b_c, false, FROM_CHAR_DATE_NONE},	/* b */
	{"bc", 2, DCH_bc, false, FROM_CHAR_DATE_NONE},
	{"cc", 2, DCH_CC, true, FROM_CHAR_DATE_NONE},	/* c */
	{"day", 3, DCH_day, false, FROM_CHAR_DATE_NONE},	/* d */
	{"ddd", 3, DCH_DDD, true, FROM_CHAR_DATE_GREGORIAN},
	{"dd", 2, DCH_DD, true, FROM_CHAR_DATE_GREGORIAN},
	{"dy", 2, DCH_dy, false, FROM_CHAR_DATE_NONE},
	{"d", 1, DCH_D, true, FROM_CHAR_DATE_GREGORIAN},
	{"ff1", 3, DCH_FF1, true, FROM_CHAR_DATE_NONE}, /* f */
	{"ff2", 3, DCH_FF2, true, FROM_CHAR_DATE_NONE},
	{"ff3", 3, DCH_FF3, true, FROM_CHAR_DATE_NONE},
	{"ff4", 3, DCH_FF4, true, FROM_CHAR_DATE_NONE},
	{"ff5", 3, DCH_FF5, true, FROM_CHAR_DATE_NONE},
	{"ff6", 3, DCH_FF6, true, FROM_CHAR_DATE_NONE},
	{"fx", 2, DCH_FX, false, FROM_CHAR_DATE_NONE},
	{"hh24", 4, DCH_HH24, true, FROM_CHAR_DATE_NONE},	/* h */
	{"hh12", 4, DCH_HH12, true, FROM_CHAR_DATE_NONE},
	{"hh", 2, DCH_HH, true, FROM_CHAR_DATE_NONE},
	{"iddd", 4, DCH_IDDD, true, FROM_CHAR_DATE_ISOWEEK},	/* i */
	{"id", 2, DCH_ID, true, FROM_CHAR_DATE_ISOWEEK},
	{"iw", 2, DCH_IW, true, FROM_CHAR_DATE_ISOWEEK},
	{"iyyy", 4, DCH_IYYY, true, FROM_CHAR_DATE_ISOWEEK},
	{"iyy", 3, DCH_IYY, true, FROM_CHAR_DATE_ISOWEEK},
	{"iy", 2, DCH_IY, true, FROM_CHAR_DATE_ISOWEEK},
	{"i", 1, DCH_I, true, FROM_CHAR_DATE_ISOWEEK},
	{"j", 1, DCH_J, true, FROM_CHAR_DATE_NONE}, /* j */
	{"mi", 2, DCH_MI, true, FROM_CHAR_DATE_NONE},	/* m */
	{"mm", 2, DCH_MM, true, FROM_CHAR_DATE_GREGORIAN},
	{"month", 5, DCH_month, false, FROM_CHAR_DATE_GREGORIAN},
	{"mon", 3, DCH_mon, false, FROM_CHAR_DATE_GREGORIAN},
	{"ms", 2, DCH_MS, true, FROM_CHAR_DATE_NONE},
	{"of", 2, DCH_OF, false, FROM_CHAR_DATE_NONE},	/* o */
	{"p.m.", 4, DCH_p_m, false, FROM_CHAR_DATE_NONE},	/* p */
	{"pm", 2, DCH_pm, false, FROM_CHAR_DATE_NONE},
	{"q", 1, DCH_Q, true, FROM_CHAR_DATE_NONE}, /* q */
	{"rm", 2, DCH_rm, false, FROM_CHAR_DATE_GREGORIAN}, /* r */
	{"sssss", 5, DCH_SSSS, true, FROM_CHAR_DATE_NONE},	/* s */
	{"ssss", 4, DCH_SSSS, true, FROM_CHAR_DATE_NONE},
	{"ss", 2, DCH_SS, true, FROM_CHAR_DATE_NONE},
	{"tzh", 3, DCH_TZH, false, FROM_CHAR_DATE_NONE},	/* t */
	{"tzm", 3, DCH_TZM, true, FROM_CHAR_DATE_NONE},
	{"tz", 2, DCH_tz, false, FROM_CHAR_DATE_NONE},
	{"us", 2, DCH_US, true, FROM_CHAR_DATE_NONE},	/* u */
	{"ww", 2, DCH_WW, true, FROM_CHAR_DATE_GREGORIAN},	/* w */
	{"w", 1, DCH_W, true, FROM_CHAR_DATE_GREGORIAN},
	{"y,yyy", 5, DCH_Y_YYY, true, FROM_CHAR_DATE_GREGORIAN},	/* y */
	{"yyyy", 4, DCH_YYYY, true, FROM_CHAR_DATE_GREGORIAN},
	{"yyy", 3, DCH_YYY, true, FROM_CHAR_DATE_GREGORIAN},
	{"yy", 2, DCH_YY, true, FROM_CHAR_DATE_GREGORIAN},
	{"y", 1, DCH_Y, true, FROM_CHAR_DATE_GREGORIAN},

	/* last */
	{NULL, 0, 0, 0, 0}
};

/* ---- formatting.c:968-992 VERBATIM ---- */


/* ----------
 * KeyWords index for DATE-TIME version
 * ----------
 */
static const int DCH_index[KeyWord_INDEX_SIZE] = {
/*
0	1	2	3	4	5	6	7	8	9
*/
	/*---- first 0..31 chars are skipped ----*/

	-1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, DCH_A_D, DCH_B_C, DCH_CC, DCH_DAY, -1,
	DCH_FF1, -1, DCH_HH24, DCH_IDDD, DCH_J, -1, -1, DCH_MI, -1, DCH_OF,
	DCH_P_M, DCH_Q, DCH_RM, DCH_SSSSS, DCH_TZH, DCH_US, -1, DCH_WW, -1, DCH_Y_YYY,
	-1, -1, -1, -1, -1, -1, -1, DCH_a_d, DCH_b_c, DCH_cc,
	DCH_day, -1, DCH_ff1, -1, DCH_hh24, DCH_iddd, DCH_j, -1, -1, DCH_mi,
	-1, DCH_of, DCH_p_m, DCH_q, DCH_rm, DCH_sssss, DCH_tzh, DCH_us, -1, DCH_ww,
	-1, DCH_y_yyy, -1, -1, -1, -1

	/*---- chars over 126 are skipped ----*/
};

/* ---- formatting.c:1050-1054 VERBATIM ---- */

/* Return flags for DCH_from_char() */
#define DCH_DATED	0x01
#define DCH_TIMED	0x02
#define DCH_ZONED	0x04

/* ---- shim: forward declarations for the extracted statics (the original
 * block at formatting.c:1119-1131 also names NUM-side entries not carried
 * here); NUMDesc stays opaque — parse_format is only ever called with
 * DCH_FLAG and Num == NULL in this extract, so NUMDesc_prepare is a
 * loud-abort stub (unreachable-arm shim, not vendored logic). ---- */
typedef struct NUMDesc NUMDesc;
static const KeyWord *index_seq_search(const char *str, const KeyWord *kw,
									   const int *index);
static const KeySuffix *suff_search(const char *str, const KeySuffix *suf,
									int type);
static bool is_separator_char(const char *str);
static void parse_format(FormatNode *node, const char *str, const KeyWord *kw,
						 const KeySuffix *suf, const int *index, uint32 flags,
						 NUMDesc *Num);
static int	DCH_datetime_type(FormatNode *node);
static DCHCacheEntry *DCH_cache_getnew(const char *str, bool std);
static DCHCacheEntry *DCH_cache_search(const char *str, bool std);
static DCHCacheEntry *DCH_cache_fetch(const char *str, bool std);

static void
NUMDesc_prepare(NUMDesc *num, FormatNode *n)
{
	/* shim: NUM-side parsing is outside this extract; DCH-only callers */
	abort();
}

/* ---- formatting.c:1128-1156 VERBATIM (index_seq_search) ---- */
/* ----------
 * Fast sequential search, use index for data selection which
 * go to seq. cycle (it is very fast for unwanted strings)
 * (can't be used binary search in format parsing)
 * ----------
 */
static const KeyWord *
index_seq_search(const char *str, const KeyWord *kw, const int *index)
{
	int			poz;

	if (!KeyWord_INDEX_FILTER(*str))
		return NULL;

	if ((poz = *(index + (*str - ' '))) > -1)
	{
		const KeyWord *k = kw + poz;

		do
		{
			if (strncmp(str, k->name, k->len) == 0)
				return k;
			k++;
			if (!k->name)
				return NULL;
		} while (*str == *k->name);
	}
	return NULL;
}

/* ---- formatting.c:1158-1172 VERBATIM (suff_search) ---- */
static const KeySuffix *
suff_search(const char *str, const KeySuffix *suf, int type)
{
	const KeySuffix *s;

	for (s = suf; s->name != NULL; s++)
	{
		if (s->type != type)
			continue;

		if (strncmp(str, s->name, s->len) == 0)
			return s;
	}
	return NULL;
}

/* ---- formatting.c:1174-1182 VERBATIM (is_separator_char) ---- */
static bool
is_separator_char(const char *str)
{
	/* ASCII printable character, but not letter or digit */
	return (*str > 0x20 && *str < 0x7F &&
			!(*str >= 'A' && *str <= 'Z') &&
			!(*str >= 'a' && *str <= 'z') &&
			!(*str >= '0' && *str <= '9'));
}

/* ---- formatting.c:1367-1515 VERBATIM (parse_format) ---- */
/* ----------
 * Format parser, search small keywords and keyword's suffixes, and make
 * format-node tree.
 *
 * for DATE-TIME & NUMBER version
 * ----------
 */
static void
parse_format(FormatNode *node, const char *str, const KeyWord *kw,
			 const KeySuffix *suf, const int *index, uint32 flags, NUMDesc *Num)
{
	FormatNode *n;

#ifdef DEBUG_TO_FROM_CHAR
	elog(DEBUG_elog_output, "to_char/number(): run parser");
#endif

	n = node;

	while (*str)
	{
		int			suffix = 0;
		const KeySuffix *s;

		/*
		 * Prefix
		 */
		if ((flags & DCH_FLAG) &&
			(s = suff_search(str, suf, SUFFTYPE_PREFIX)) != NULL)
		{
			suffix |= s->id;
			if (s->len)
				str += s->len;
		}

		/*
		 * Keyword
		 */
		if (*str && (n->key = index_seq_search(str, kw, index)) != NULL)
		{
			n->type = NODE_TYPE_ACTION;
			n->suffix = suffix;
			if (n->key->len)
				str += n->key->len;

			/*
			 * NUM version: Prepare global NUMDesc struct
			 */
			if (flags & NUM_FLAG)
				NUMDesc_prepare(Num, n);

			/*
			 * Postfix
			 */
			if ((flags & DCH_FLAG) && *str &&
				(s = suff_search(str, suf, SUFFTYPE_POSTFIX)) != NULL)
			{
				n->suffix |= s->id;
				if (s->len)
					str += s->len;
			}

			n++;
		}
		else if (*str)
		{
			int			chlen;

			if ((flags & STD_FLAG) && *str != '"')
			{
				/*
				 * Standard mode, allow only following separators: "-./,':; ".
				 * However, we support double quotes even in standard mode
				 * (see below).  This is our extension of standard mode.
				 */
				if (strchr("-./,':; ", *str) == NULL)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_DATETIME_FORMAT),
							 errmsg("invalid datetime format separator: \"%s\"",
									pnstrdup(str, pg_mblen_cstr(str)))));

				if (*str == ' ')
					n->type = NODE_TYPE_SPACE;
				else
					n->type = NODE_TYPE_SEPARATOR;

				n->character[0] = *str;
				n->character[1] = '\0';
				n->key = NULL;
				n->suffix = 0;
				n++;
				str++;
			}
			else if (*str == '"')
			{
				/*
				 * Process double-quoted literal string, if any
				 */
				str++;
				while (*str)
				{
					if (*str == '"')
					{
						str++;
						break;
					}
					/* backslash quotes the next character, if any */
					if (*str == '\\' && *(str + 1))
						str++;
					chlen = pg_mblen_cstr(str);
					n->type = NODE_TYPE_CHAR;
					memcpy(n->character, str, chlen);
					n->character[chlen] = '\0';
					n->key = NULL;
					n->suffix = 0;
					n++;
					str += chlen;
				}
			}
			else
			{
				/*
				 * Outside double-quoted strings, backslash is only special if
				 * it immediately precedes a double quote.
				 */
				if (*str == '\\' && *(str + 1) == '"')
					str++;
				chlen = pg_mblen_cstr(str);

				if ((flags & DCH_FLAG) && is_separator_char(str))
					n->type = NODE_TYPE_SEPARATOR;
				else if (isspace((unsigned char) *str))
					n->type = NODE_TYPE_SPACE;
				else
					n->type = NODE_TYPE_CHAR;

				memcpy(n->character, str, chlen);
				n->character[chlen] = '\0';
				n->key = NULL;
				n->suffix = 0;
				n++;
				str += chlen;
			}
		}
	}

	n->type = NODE_TYPE_END;
	n->suffix = 0;
}

/* ---- formatting.c:3702-3718 VERBATIM (DCH_prevent_counter_overflow) ---- */
/*
 * The invariant for DCH cache entry management is that DCHCounter is equal
 * to the maximum age value among the existing entries, and we increment it
 * whenever an access occurs.  If we approach overflow, deal with that by
 * halving all the age values, so that we retain a fairly accurate idea of
 * which entries are oldest.
 */
static inline void
DCH_prevent_counter_overflow(void)
{
	if (DCHCounter >= (INT_MAX - 1))
	{
		for (int i = 0; i < n_DCHCache; i++)
			DCHCache[i]->age >>= 1;
		DCHCounter >>= 1;
	}
}

/* ---- formatting.c:3720-3817 VERBATIM (DCH_datetime_type) ---- */
/*
 * Get mask of date/time/zone components present in format nodes.
 */
static int
DCH_datetime_type(FormatNode *node)
{
	FormatNode *n;
	int			flags = 0;

	for (n = node; n->type != NODE_TYPE_END; n++)
	{
		if (n->type != NODE_TYPE_ACTION)
			continue;

		switch (n->key->id)
		{
			case DCH_FX:
				break;
			case DCH_A_M:
			case DCH_P_M:
			case DCH_a_m:
			case DCH_p_m:
			case DCH_AM:
			case DCH_PM:
			case DCH_am:
			case DCH_pm:
			case DCH_HH:
			case DCH_HH12:
			case DCH_HH24:
			case DCH_MI:
			case DCH_SS:
			case DCH_MS:		/* millisecond */
			case DCH_US:		/* microsecond */
			case DCH_FF1:
			case DCH_FF2:
			case DCH_FF3:
			case DCH_FF4:
			case DCH_FF5:
			case DCH_FF6:
			case DCH_SSSS:
				flags |= DCH_TIMED;
				break;
			case DCH_tz:
			case DCH_TZ:
			case DCH_OF:
			case DCH_TZH:
			case DCH_TZM:
				flags |= DCH_ZONED;
				break;
			case DCH_A_D:
			case DCH_B_C:
			case DCH_a_d:
			case DCH_b_c:
			case DCH_AD:
			case DCH_BC:
			case DCH_ad:
			case DCH_bc:
			case DCH_MONTH:
			case DCH_Month:
			case DCH_month:
			case DCH_MON:
			case DCH_Mon:
			case DCH_mon:
			case DCH_MM:
			case DCH_DAY:
			case DCH_Day:
			case DCH_day:
			case DCH_DY:
			case DCH_Dy:
			case DCH_dy:
			case DCH_DDD:
			case DCH_IDDD:
			case DCH_DD:
			case DCH_D:
			case DCH_ID:
			case DCH_WW:
			case DCH_Q:
			case DCH_CC:
			case DCH_Y_YYY:
			case DCH_YYYY:
			case DCH_IYYY:
			case DCH_YYY:
			case DCH_IYY:
			case DCH_YY:
			case DCH_IY:
			case DCH_Y:
			case DCH_I:
			case DCH_RM:
			case DCH_rm:
			case DCH_W:
			case DCH_J:
				flags |= DCH_DATED;
				break;
		}
	}

	return flags;
}

/* ---- formatting.c:3819-3877 VERBATIM (DCH_cache_getnew) ---- */
/* select a DCHCacheEntry to hold the given format picture */
static DCHCacheEntry *
DCH_cache_getnew(const char *str, bool std)
{
	DCHCacheEntry *ent;

	/* Ensure we can advance DCHCounter below */
	DCH_prevent_counter_overflow();

	/*
	 * If cache is full, remove oldest entry (or recycle first not-valid one)
	 */
	if (n_DCHCache >= DCH_CACHE_ENTRIES)
	{
		DCHCacheEntry *old = DCHCache[0];

#ifdef DEBUG_TO_FROM_CHAR
		elog(DEBUG_elog_output, "cache is full (%d)", n_DCHCache);
#endif
		if (old->valid)
		{
			for (int i = 1; i < DCH_CACHE_ENTRIES; i++)
			{
				ent = DCHCache[i];
				if (!ent->valid)
				{
					old = ent;
					break;
				}
				if (ent->age < old->age)
					old = ent;
			}
		}
#ifdef DEBUG_TO_FROM_CHAR
		elog(DEBUG_elog_output, "OLD: '%s' AGE: %d", old->str, old->age);
#endif
		old->valid = false;
		strlcpy(old->str, str, DCH_CACHE_SIZE + 1);
		old->age = (++DCHCounter);
		/* caller is expected to fill format, then set valid */
		return old;
	}
	else
	{
#ifdef DEBUG_TO_FROM_CHAR
		elog(DEBUG_elog_output, "NEW (%d)", n_DCHCache);
#endif
		Assert(DCHCache[n_DCHCache] == NULL);
		DCHCache[n_DCHCache] = ent = (DCHCacheEntry *)
			MemoryContextAllocZero(TopMemoryContext, sizeof(DCHCacheEntry));
		ent->valid = false;
		strlcpy(ent->str, str, DCH_CACHE_SIZE + 1);
		ent->std = std;
		ent->age = (++DCHCounter);
		/* caller is expected to fill format, then set valid */
		++n_DCHCache;
		return ent;
	}
}

/* ---- formatting.c:3879-3898 VERBATIM (DCH_cache_search) ---- */
/* look for an existing DCHCacheEntry matching the given format picture */
static DCHCacheEntry *
DCH_cache_search(const char *str, bool std)
{
	/* Ensure we can advance DCHCounter below */
	DCH_prevent_counter_overflow();

	for (int i = 0; i < n_DCHCache; i++)
	{
		DCHCacheEntry *ent = DCHCache[i];

		if (ent->valid && strcmp(ent->str, str) == 0 && ent->std == std)
		{
			ent->age = (++DCHCounter);
			return ent;
		}
	}

	return NULL;
}

/* ---- formatting.c:3900-3921 VERBATIM (DCH_cache_fetch) ---- */
/* Find or create a DCHCacheEntry for the given format picture */
static DCHCacheEntry *
DCH_cache_fetch(const char *str, bool std)
{
	DCHCacheEntry *ent;

	if ((ent = DCH_cache_search(str, std)) == NULL)
	{
		/*
		 * Not in the cache, must run parser and save a new format-picture to
		 * the cache.  Do not mark the cache entry valid until parsing
		 * succeeds.
		 */
		ent = DCH_cache_getnew(str, std);

		parse_format(ent->format, str, DCH_keywords, DCH_suff, DCH_index,
					 DCH_FLAG | (std ? STD_FLAG : 0), NULL);

		ent->valid = true;
	}
	return ent;
}

/* ---- formatting.c:4361-4403 VERBATIM (datetime_format_has_tz) ---- */
/*
 * Parses the datetime format string in 'fmt_str' and returns true if it
 * contains a timezone specifier, false if not.
 */
bool
datetime_format_has_tz(const char *fmt_str)
{
	bool		incache;
	int			fmt_len = strlen(fmt_str);
	int			result;
	FormatNode *format;

	if (fmt_len > DCH_CACHE_SIZE)
	{
		/*
		 * Allocate new memory if format picture is bigger than static cache
		 * and do not use cache (call parser always)
		 */
		incache = false;

		format = (FormatNode *) palloc((fmt_len + 1) * sizeof(FormatNode));

		parse_format(format, fmt_str, DCH_keywords,
					 DCH_suff, DCH_index, DCH_FLAG, NULL);
	}
	else
	{
		/*
		 * Use cache buffers
		 */
		DCHCacheEntry *ent = DCH_cache_fetch(fmt_str, false);

		incache = true;
		format = ent->format;
	}

	result = DCH_datetime_type(format);

	if (!incache)
		pfree(format);

	return result & DCH_ZONED;
}
