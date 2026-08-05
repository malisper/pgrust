/*
 * pg_trgm_io.c: vendored PostgreSQL C oracle for the trgm_diff differential
 * fuzz target (100%-coverage campaign; crate crates/contrib/pg_trgm).
 *
 * Provenance: every block marked "VERBATIM <file> lines A-B" is a
 * byte-for-byte extraction from the repo's vendored ground-truth checkout
 * ../pgrust-fabled/vendor/postgres-src @ 62d6c7d3df6287f1bd83199c1a746e50d31571a0
 * (PostgreSQL 18.3, "Stamp 18.3"), assembled by
 * scratchpad/gen_trgm_oracle.py (line ranges auditable with sed).
 * Vendored bodies: contrib/pg_trgm/trgm.h (macros/typedefs),
 * lib/qunique.h, contrib/pg_trgm/trgm_op.c (everything except the fmgr
 * Datum shells, _PG_init GUC registration, and the set_limit/show_limit/
 * index_strategy_get_limit GUC-plumbing carve), tsearch/ts_locale.c
 * t_is* family, formatting.c str_tolower + asc_tolower, pg_locale_libc.c
 * char2wchar + the mbstowcs_l fallback, pg_locale.c pg_strlower,
 * pg_locale_builtin.c strlower_builtin, mcxt.c pnstrdup, pgstrcasecmp.c
 * pg_ascii_tolower, utils/hash/pg_crc.c pg_crc32_table (the LEGACY
 * reflected-code table -- NOT zlib CRC-32 and NOT CRC-32C; wrong-variant
 * was train-6 audit blocker B1), utils/pg_crc.h LEGACY macros,
 * common/int.h pg_cmp_s32. The builtin-provider Unicode lowercase engine
 * (src/common/unicode_case.c + tables) is vendored whole-file under
 * csrc/trgmfam/ and linked, not pasted.
 *
 * SHIMS (plumbing only, never logic):
 *  - fmgr: PG_FUNCTION_ARGS shells unwrapped to plain (ptr,len)
 *    pg_diff_trgm_* entries (pg_regexp_io.c precedent). show_trgm's
 *    construct_array_builtin is fmgr/array plumbing: the driver entry
 *    copies the per-element text bytes out in order instead (the element
 *    FORMATTING loop is verbatim trgm_op.c lines 1140-1153).
 *  - palloc/palloc0/repalloc/pfree -> growable TLS pointer arena, reset at
 *    every entry (models memory-context reset; error-path longjmps cannot
 *    leak). palloc0_array/repalloc0_array are the palloc.h macro bodies.
 *  - ereport(ERROR)/elog(ERROR) -> errcode class in shared TLS
 *    pg_diff_errcode + longjmp (message text out of comparator scope).
 *    Classes: 1 = 54000 PROGRAM_LIMIT_EXCEEDED ("out of memory" MaxAlloc
 *    guards); 2 = 42P22 INDETERMINATE_COLLATION (str_tolower, dead: collid
 *    pinned 100); 3 = 22021 CHARACTER_NOT_IN_REPERTOIRE (char2wchar,
 *    unreachable in-domain: harness feeds valid UTF-8 only); 6 = internal.
 *  - encoding pinned UTF-8: pg_mblen family + pg_verifymbstr resolve to
 *    the ONE verbatim wfam_ mbutils.c copies in pg_wcharfam.c; every entry
 *    calls wfam_x_set_db_encoding(PG_UTF8).
 *  - GetDefaultCharSignedness -> TLS pg_diff_trgm_char_signedness
 *    (default 1 = signed; live postgres:18.3 aarch64 ground truth
 *    2026-08-01: pg_control_init().default_char_signedness = t, so signed
 *    is the platform-of-record posture). CMPTRGM is reset to
 *    CMPTRGM_CHOOSE at every entry so the pin is re-read.
 *  - locale model (the ENVIRONMENT; both arms' UNITS are verbatim):
 *    pg_newlocale_from_collation(100) -> &trgmf_locale_model, a minimal
 *    pg_locale_t struct model configured per locale_arm:
 *      arm 0 = database ctype "C":       ctype_is_c=true  (t_isalnum
 *              byte path isalnum(TOUCHAR); str_tolower -> asc_tolower)
 *      arm 1 = builtin "C.UTF-8" (UTF8): ctype_is_c=false, provider
 *              COLLPROVIDER_BUILTIN, casemap_full=false (18.3
 *              pg_locale_builtin.c:160 -- casemap_full only for
 *              PG_UNICODE_FAST); t_isalnum multibyte path char2wchar +
 *              iswalnum under the PROCESS LC_CTYPE (harness init pins a
 *              UTF-8 LC_CTYPE; ts_locale.c passes mylocale = 0, so the
 *              mbstowcs_l branch is call-site-dead), str_tolower ->
 *              pg_strlower -> strlower_builtin -> unicode_strlower.
 *    strlower_libc/PGLOCALE_SUPPORT_ERROR arm of pg_strlower: provider is
 *    pinned BUILTIN whenever pg_strlower is reachable (ctype_is_c=false),
 *    so the libc arm is pin-dead; it aborts loudly if ever taken.
 *  - qsort -> trgmrx_pg_qsort (the family's verbatim src/port/qsort.c,
 *    csrc/trgmrxfam/qsort.c), matching port.h @ 18.3 (#define qsort
 *    pg_qsort); a bare libc qsort binding is banned by the task #98
 *    sort-symbol hygiene guard. Tie order is additionally unobservable in
 *    THIS TU half: comp_trgm equals are byte-identical 3-byte elements
 *    collapsed by qunique, and comp_ptrgm is a TOTAL order (trgm bytes
 *    then pg_cmp_s32 on index). (trgm_regexp.c's penalty-comparator ties
 *    are NOT total -- that arm lives in pg_trgm_regexp_io.c with the same
 *    trgmrx_pg_qsort binding.)
 *  - Assert -> ((void) 0) (release-C parity), CHECK_FOR_INTERRUPTS ->
 *    no-op, MaxAllocSize = 0x3fffffff (verbatim value).
 *  - All extern definitions #define-renamed trgmf_* (TU isolation in the
 *    single fuzz cc build); only pg_diff_trgm_* entries are exported.
 *
 * INJECTION SWEEP (2026-08-01, parent-mandated, seeds-only kills):
 *  results recorded by the lane after the sweep runs -- see module header
 *  of fuzz/core/src/trgm_diff.rs for the kill table.
 */

#include <assert.h>
#include <ctype.h>
#include <limits.h>
#include <locale.h>
#include <setjmp.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
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
typedef unsigned int pg_wchar;
typedef uint16 StrategyNumber;
typedef float float4;
typedef double float8;
typedef void *Datum;

#define PG_UTF8 6				/* pg_wchar.h enum pg_enc value */
#define DEFAULT_COLLATION_OID 100
#define OidIsValid(objectId)  ((bool) ((objectId) != 0))
#define Assert(x) ((void) 0)
#define CHECK_FOR_INTERRUPTS() ((void) 0)
#define Max(x, y) ((x) > (y) ? (x) : (y))
#define Min(x, y) ((x) < (y) ? (x) : (y))
#define FLEXIBLE_ARRAY_MEMBER	/* empty */
#define HIGHBIT (0x80)
#define IS_HIGHBIT_SET(ch) ((unsigned char)(ch) & HIGHBIT)
#define TOUCHAR(ptr) (*((const unsigned char *) (ptr)))
#define MaxAllocSize ((Size) 0x3fffffff)	/* memutils.h, verbatim value */
#define PointerGetDatum(X) ((Datum) (X))
#define DatumGetPointer(X) ((void *) (X))
#define pg_attribute_unused()
#define unlikely(x) __builtin_expect((x) != 0, 0)

/* port.h @ 18.3: the backend's qsort IS pg_qsort (#define qsort(a,b,c,d)
 * pg_qsort(a,b,c,d)) -- bind the vendored trgm_op.c body to the family's
 * verbatim src/port/qsort.c copy (trgmrx_pg_qsort, csrc/trgmrxfam/qsort.c)
 * instead of libc (task #98 sort-symbol hygiene; see the header note --
 * tie order is unobservable in this TU half, the binding is C-exactness). */
extern void trgmrx_pg_qsort(void *base, size_t nel, size_t elsize,
							int (*cmp) (const void *, const void *));
#define qsort(a,b,c,d) trgmrx_pg_qsort(a,b,c,d)

/* varlena model: the oracle only ever builds plain 4B-header LE varlenas */
typedef struct varlena
{
	int32		vl_len_;
	char		vl_dat[FLEXIBLE_ARRAY_MEMBER];
} varlena;
typedef varlena text;
#define VARHDRSZ ((int32) sizeof(int32))
#define VARSIZE(PTR) (*((const int32 *) (PTR)))
#define SET_VARSIZE(PTR, len) (*((int32 *) (PTR)) = (len))
#define VARDATA(PTR) (((char *) (PTR)) + VARHDRSZ)

/* Shared TLS errcode channel (defined in csrc/pg_float_io.c). */
extern _Thread_local int pg_diff_errcode;

#define PG_DIFF_TRGM_ERR_LIMIT 1	/* 54000 program_limit_exceeded */
#define PG_DIFF_TRGM_ERR_INDET_COLLATION 2	/* 42P22 (pin-dead) */
#define PG_DIFF_TRGM_ERR_BAD_MB 3	/* 22021 (out of harness domain) */
#define PG_DIFF_TRGM_ERR_INTERNAL 6

static _Thread_local jmp_buf pg_diff_trgm_jmp;
static _Thread_local int pg_diff_trgm_pending;

static void
pg_diff_trgm_raise(int code)
{
	pg_diff_errcode = code;
	longjmp(pg_diff_trgm_jmp, 1);
}

/* errcode(): map the pasted ERRCODE_* names to class constants */
#define ERRCODE_PROGRAM_LIMIT_EXCEEDED PG_DIFF_TRGM_ERR_LIMIT
#define ERRCODE_INDETERMINATE_COLLATION PG_DIFF_TRGM_ERR_INDET_COLLATION
#define ERRCODE_CHARACTER_NOT_IN_REPERTOIRE PG_DIFF_TRGM_ERR_BAD_MB
static int
trgmf_errcode_set(int c)
{
	pg_diff_trgm_pending = c;
	return 0;
}
#define errcode(c) trgmf_errcode_set(c)
static int
trgmf_errmsg(const char *fmt,...)
{
	(void) fmt;
	return 0;
}
#define errmsg trgmf_errmsg
#define errhint trgmf_errmsg
#define errdetail trgmf_errmsg
#define ereport(level, rest) do { pg_diff_trgm_pending = PG_DIFF_TRGM_ERR_INTERNAL; ((void) (rest)); pg_diff_trgm_raise(pg_diff_trgm_pending); } while (0)
#define elog(level, ...) do { trgmf_errmsg(__VA_ARGS__); pg_diff_trgm_raise(PG_DIFF_TRGM_ERR_INTERNAL); } while (0)
#define ERROR 21

/* ---- SHIM: growable TLS pointer arena (palloc family) ---- */

static _Thread_local void **trgmf_ptrs;
static _Thread_local int trgmf_nptrs;
static _Thread_local int trgmf_cap_ptrs;

static void
pg_diff_trgm_arena_reset(void)
{
	int			i;

	for (i = 0; i < trgmf_nptrs; i++)
		free(trgmf_ptrs[i]);
	trgmf_nptrs = 0;
}

static void
trgmf_arena_push(void *p)
{
	if (trgmf_nptrs == trgmf_cap_ptrs)
	{
		trgmf_cap_ptrs = trgmf_cap_ptrs ? trgmf_cap_ptrs * 2 : 1024;
		trgmf_ptrs = realloc(trgmf_ptrs, trgmf_cap_ptrs * sizeof(void *));
		if (!trgmf_ptrs)
			abort();
	}
	trgmf_ptrs[trgmf_nptrs++] = p;
}

static void *
trgmf_palloc(size_t n)
{
	void	   *p = malloc(n ? n : 1);

	if (!p)
		abort();
	trgmf_arena_push(p);
	return p;
}

static void *
trgmf_palloc0(size_t n)
{
	void	   *p = calloc(1, n ? n : 1);

	if (!p)
		abort();
	trgmf_arena_push(p);
	return p;
}

static void *
trgmf_repalloc(void *old, size_t n)
{
	int			i;

	for (i = trgmf_nptrs - 1; i >= 0; i--)
	{
		if (trgmf_ptrs[i] == old)
		{
			void	   *p = realloc(old, n);

			if (!p)
				abort();
			trgmf_ptrs[i] = p;
			return p;
		}
	}
	abort();					/* repalloc of a pointer the arena never
								 * issued: shim bug */
}

static void
trgmf_pfree(void *p)
{
	int			i;

	for (i = trgmf_nptrs - 1; i >= 0; i--)
	{
		if (trgmf_ptrs[i] == p)
		{
			free(p);
			trgmf_ptrs[i] = trgmf_ptrs[--trgmf_nptrs];
			return;
		}
	}
	abort();					/* pfree of a pointer the arena never issued */
}

#define palloc(n) trgmf_palloc(n)
#define palloc0(n) trgmf_palloc0(n)
#define repalloc(p, n) trgmf_repalloc((p), (n))
#define pfree(p) trgmf_pfree(p)
/* palloc.h macro bodies (verbatim macro semantics) */
#define palloc_array(type, count) ((type *) palloc(sizeof(type) * (count)))
#define palloc0_array(type, count) ((type *) palloc0(sizeof(type) * (count)))
#define repalloc0_array(pointer, type, oldcount, count) ((type *) repalloc0(pointer, sizeof(type) * (oldcount), sizeof(type) * (count)))

/* mcxt.c repalloc0: grow + zero the extension (verbatim semantics of the
 * mcxt.c body: repalloc then MemSetAligned of the tail) */
static void *
trgmf_repalloc0(void *pointer, size_t oldsize, size_t size)
{
	char	   *p = repalloc(pointer, size);

	if (size > oldsize)
		memset(p + oldsize, 0, size - oldsize);
	return p;
}
#define repalloc0(p, o, n) trgmf_repalloc0((p), (o), (n))

/* ---- pg_mblen family: the ONE verbatim mbutils.c copies (pg_wcharfam.c) */
extern int	wfam_pg_mblen_range(const char *mbstr, const char *end);
extern int	wfam_pg_mblen_with_len(const char *mbstr, int limit);
extern int	wfam_pg_mblen_unbounded(const char *mbstr);
extern int	wfam_pg_mblen_cstr(const char *mbstr);
extern int	wfam_GetDatabaseEncoding(void);
extern int	wfam_pg_database_encoding_max_length(void);
extern int	wfam_pg_encoding_max_length(int encoding);
extern bool wfam_pg_verifymbstr(const char *mbstr, int len, bool noError);
extern void wfam_x_set_db_encoding(int encoding);
#define pg_mblen_range wfam_pg_mblen_range
#define pg_mblen_with_len wfam_pg_mblen_with_len
#define pg_mblen_unbounded wfam_pg_mblen_unbounded
#define pg_mblen_cstr wfam_pg_mblen_cstr
#define GetDatabaseEncoding wfam_GetDatabaseEncoding
#define pg_database_encoding_max_length wfam_pg_database_encoding_max_length
#define pg_encoding_max_length wfam_pg_encoding_max_length
#define pg_verifymbstr wfam_pg_verifymbstr

/* ---- TU symbol isolation: rename every extern definition ---- */
#define CMPTRGM trgmf_CMPTRGM
#define compact_trigram trgmf_compact_trigram
#define generate_trgm trgmf_generate_trgm
#define generate_wildcard_trgm trgmf_generate_wildcard_trgm
#define trgm2int trgmf_trgm2int
#define cnt_sml trgmf_cnt_sml
#define trgm_contained_by trgmf_trgm_contained_by
#define trgm_presence_map trgmf_trgm_presence_map
#define similarity_threshold trgmf_similarity_threshold
#define word_similarity_threshold trgmf_word_similarity_threshold
#define strict_word_similarity_threshold trgmf_strict_word_similarity_threshold
#define t_isalnum_with_len trgmf_t_isalnum_with_len
#define t_isalnum_cstr trgmf_t_isalnum_cstr
#define t_isalnum_unbounded trgmf_t_isalnum_unbounded
#define t_isalnum trgmf_t_isalnum
#define t_isalpha_with_len trgmf_t_isalpha_with_len
#define t_isalpha_cstr trgmf_t_isalpha_cstr
#define t_isalpha_unbounded trgmf_t_isalpha_unbounded
#define t_isalpha trgmf_t_isalpha
#define str_tolower trgmf_str_tolower
#define asc_tolower trgmf_asc_tolower
#define char2wchar trgmf_char2wchar
#define pg_strlower trgmf_pg_strlower
#define strlower_builtin trgmf_strlower_builtin
#define pnstrdup trgmf_pnstrdup
#define pg_ascii_tolower trgmf_pg_ascii_tolower
#define pg_crc32_table trgmf_pg_crc32_table
#define calc_word_similarity trgmf_calc_word_similarity

/* ---- SHIM: minimal pg_locale_t model (fields the pasted arms read) ---- */
#define COLLPROVIDER_BUILTIN 'b'	/* pg_collation.h line 71, verbatim */
#define COLLPROVIDER_LIBC 'c'	/* pg_collation.h line 73, verbatim */
typedef struct pg_locale_struct
{
	char		provider;
	bool		ctype_is_c;
	union
	{
		struct
		{
			bool		casemap_full;
		}			builtin;
		locale_t	lt;
	}			info;
}		   *pg_locale_t;

static _Thread_local struct pg_locale_struct trgmf_locale_model;

static pg_locale_t
pg_newlocale_from_collation(Oid collid)
{
	(void) collid;				/* pinned DEFAULT_COLLATION_OID by entry */
	return &trgmf_locale_model;
}

/* database_ctype_is_c: globals.c session cell, TLS-modelled */
static _Thread_local bool database_ctype_is_c = false;

/* GetDefaultCharSignedness: pg_control cell, TLS-modelled (default
 * signed = live 18.3 initdb default, all platforms) */
static _Thread_local bool trgmf_char_signedness = true;
static bool
GetDefaultCharSignedness(void)
{
	return trgmf_char_signedness;
}

/* pin-dead pg_strlower arms abort loudly (see header) */
#define PGLOCALE_SUPPORT_ERROR(provider) abort()

/* builtin unicode lowercase engine, vendored whole-file in csrc/trgmfam/ */
extern size_t trgmf_unicode_strlower(char *dst, size_t dstsize, const char *src,
									 ssize_t srclen, bool full);
#define unicode_strlower trgmf_unicode_strlower

/* ---- VERBATIM src/include/common/int.h lines 644-649 @ 62d6c7d3df ---- */

static inline int
pg_cmp_s32(int32 a, int32 b)
{
	return (a > b) - (a < b);
}
/* ---- end VERBATIM src/include/common/int.h lines 644-649 ---- */

/* ---- VERBATIM src/port/pgstrcasecmp.c lines 142-151 @ 62d6c7d3df ---- */
/*
 * Fold a character to lower case, following C/POSIX locale rules.
 */
unsigned char
pg_ascii_tolower(unsigned char ch)
{
	if (ch >= 'A' && ch <= 'Z')
		ch += 'a' - 'A';
	return ch;
}
/* ---- end VERBATIM src/port/pgstrcasecmp.c lines 142-151 ---- */

/* ---- VERBATIM src/backend/utils/mmgr/mcxt.c lines 1730-1748 @ 62d6c7d3df ---- */
/*
 * pnstrdup
 *		Like pstrdup(), but append null byte to a
 *		not-necessarily-null-terminated input string.
 */
char *
pnstrdup(const char *in, Size len)
{
	char	   *out;

	len = strnlen(in, len);

	out = palloc(len + 1);
	memcpy(out, in, len);
	out[len] = '\0';

	return out;
}

/* ---- end VERBATIM src/backend/utils/mmgr/mcxt.c lines 1730-1748 ---- */

/* ---- VERBATIM src/include/utils/pg_crc.h lines 37-37 @ 62d6c7d3df ---- */
typedef uint32 pg_crc32;
/* ---- end VERBATIM src/include/utils/pg_crc.h lines 37-37 ---- */

/* ---- VERBATIM src/include/utils/pg_crc.h lines 53-99 @ 62d6c7d3df ---- */
#define COMP_CRC32_NORMAL_TABLE(crc, data, len, table)			  \
do {															  \
	const unsigned char *__data = (const unsigned char *) (data); \
	uint32		__len = (len); \
\
	while (__len-- > 0) \
	{ \
		int		__tab_index = ((int) (crc) ^ *__data++) & 0xFF; \
		(crc) = table[__tab_index] ^ ((crc) >> 8); \
	} \
} while (0)

/*
 * The CRC algorithm used for WAL et al in pre-9.5 versions.
 *
 * This closely resembles the normal CRC-32 algorithm, but is subtly
 * different. Using Williams' terms, we use the "normal" table, but with
 * "reflected" code. That's bogus, but it was like that for years before
 * anyone noticed. It does not correspond to any polynomial in a normal CRC
 * algorithm, so it's not clear what the error-detection properties of this
 * algorithm actually are.
 *
 * We still need to carry this around because it is used in a few on-disk
 * structures that need to be pg_upgradeable. It should not be used in new
 * code.
 */
#define INIT_LEGACY_CRC32(crc) ((crc) = 0xFFFFFFFF)
#define FIN_LEGACY_CRC32(crc)	((crc) ^= 0xFFFFFFFF)
#define COMP_LEGACY_CRC32(crc, data, len)	\
	COMP_CRC32_REFLECTED_TABLE(crc, data, len, pg_crc32_table)
#define EQ_LEGACY_CRC32(c1, c2) ((c1) == (c2))

/*
 * Sarwate's algorithm, for use with a "reflected" lookup table (but in the
 * legacy algorithm, we actually use it on a "normal" table, see above)
 */
#define COMP_CRC32_REFLECTED_TABLE(crc, data, len, table) \
do {															  \
	const unsigned char *__data = (const unsigned char *) (data); \
	uint32		__len = (len); \
\
	while (__len-- > 0) \
	{ \
		int		__tab_index = ((int) ((crc) >> 24) ^ *__data++) & 0xFF; \
		(crc) = table[__tab_index] ^ ((crc) << 8); \
	} \
} while (0)
/* ---- end VERBATIM src/include/utils/pg_crc.h lines 53-99 ---- */

/* ---- VERBATIM src/backend/utils/hash/pg_crc.c lines 27-100 @ 62d6c7d3df ---- */
/*
 * Lookup table for calculating CRC-32 using Sarwate's algorithm.
 *
 * This table is based on the polynomial
 *	x^32+x^26+x^23+x^22+x^16+x^12+x^11+x^10+x^8+x^7+x^5+x^4+x^2+x+1.
 * (This is the same polynomial used in Ethernet checksums, for instance.)
 * Using Williams' terms, this is the "normal", not "reflected" version.
 */
const uint32 pg_crc32_table[256] = {
	0x00000000, 0x77073096, 0xEE0E612C, 0x990951BA,
	0x076DC419, 0x706AF48F, 0xE963A535, 0x9E6495A3,
	0x0EDB8832, 0x79DCB8A4, 0xE0D5E91E, 0x97D2D988,
	0x09B64C2B, 0x7EB17CBD, 0xE7B82D07, 0x90BF1D91,
	0x1DB71064, 0x6AB020F2, 0xF3B97148, 0x84BE41DE,
	0x1ADAD47D, 0x6DDDE4EB, 0xF4D4B551, 0x83D385C7,
	0x136C9856, 0x646BA8C0, 0xFD62F97A, 0x8A65C9EC,
	0x14015C4F, 0x63066CD9, 0xFA0F3D63, 0x8D080DF5,
	0x3B6E20C8, 0x4C69105E, 0xD56041E4, 0xA2677172,
	0x3C03E4D1, 0x4B04D447, 0xD20D85FD, 0xA50AB56B,
	0x35B5A8FA, 0x42B2986C, 0xDBBBC9D6, 0xACBCF940,
	0x32D86CE3, 0x45DF5C75, 0xDCD60DCF, 0xABD13D59,
	0x26D930AC, 0x51DE003A, 0xC8D75180, 0xBFD06116,
	0x21B4F4B5, 0x56B3C423, 0xCFBA9599, 0xB8BDA50F,
	0x2802B89E, 0x5F058808, 0xC60CD9B2, 0xB10BE924,
	0x2F6F7C87, 0x58684C11, 0xC1611DAB, 0xB6662D3D,
	0x76DC4190, 0x01DB7106, 0x98D220BC, 0xEFD5102A,
	0x71B18589, 0x06B6B51F, 0x9FBFE4A5, 0xE8B8D433,
	0x7807C9A2, 0x0F00F934, 0x9609A88E, 0xE10E9818,
	0x7F6A0DBB, 0x086D3D2D, 0x91646C97, 0xE6635C01,
	0x6B6B51F4, 0x1C6C6162, 0x856530D8, 0xF262004E,
	0x6C0695ED, 0x1B01A57B, 0x8208F4C1, 0xF50FC457,
	0x65B0D9C6, 0x12B7E950, 0x8BBEB8EA, 0xFCB9887C,
	0x62DD1DDF, 0x15DA2D49, 0x8CD37CF3, 0xFBD44C65,
	0x4DB26158, 0x3AB551CE, 0xA3BC0074, 0xD4BB30E2,
	0x4ADFA541, 0x3DD895D7, 0xA4D1C46D, 0xD3D6F4FB,
	0x4369E96A, 0x346ED9FC, 0xAD678846, 0xDA60B8D0,
	0x44042D73, 0x33031DE5, 0xAA0A4C5F, 0xDD0D7CC9,
	0x5005713C, 0x270241AA, 0xBE0B1010, 0xC90C2086,
	0x5768B525, 0x206F85B3, 0xB966D409, 0xCE61E49F,
	0x5EDEF90E, 0x29D9C998, 0xB0D09822, 0xC7D7A8B4,
	0x59B33D17, 0x2EB40D81, 0xB7BD5C3B, 0xC0BA6CAD,
	0xEDB88320, 0x9ABFB3B6, 0x03B6E20C, 0x74B1D29A,
	0xEAD54739, 0x9DD277AF, 0x04DB2615, 0x73DC1683,
	0xE3630B12, 0x94643B84, 0x0D6D6A3E, 0x7A6A5AA8,
	0xE40ECF0B, 0x9309FF9D, 0x0A00AE27, 0x7D079EB1,
	0xF00F9344, 0x8708A3D2, 0x1E01F268, 0x6906C2FE,
	0xF762575D, 0x806567CB, 0x196C3671, 0x6E6B06E7,
	0xFED41B76, 0x89D32BE0, 0x10DA7A5A, 0x67DD4ACC,
	0xF9B9DF6F, 0x8EBEEFF9, 0x17B7BE43, 0x60B08ED5,
	0xD6D6A3E8, 0xA1D1937E, 0x38D8C2C4, 0x4FDFF252,
	0xD1BB67F1, 0xA6BC5767, 0x3FB506DD, 0x48B2364B,
	0xD80D2BDA, 0xAF0A1B4C, 0x36034AF6, 0x41047A60,
	0xDF60EFC3, 0xA867DF55, 0x316E8EEF, 0x4669BE79,
	0xCB61B38C, 0xBC66831A, 0x256FD2A0, 0x5268E236,
	0xCC0C7795, 0xBB0B4703, 0x220216B9, 0x5505262F,
	0xC5BA3BBE, 0xB2BD0B28, 0x2BB45A92, 0x5CB36A04,
	0xC2D7FFA7, 0xB5D0CF31, 0x2CD99E8B, 0x5BDEAE1D,
	0x9B64C2B0, 0xEC63F226, 0x756AA39C, 0x026D930A,
	0x9C0906A9, 0xEB0E363F, 0x72076785, 0x05005713,
	0x95BF4A82, 0xE2B87A14, 0x7BB12BAE, 0x0CB61B38,
	0x92D28E9B, 0xE5D5BE0D, 0x7CDCEFB7, 0x0BDBDF21,
	0x86D3D2D4, 0xF1D4E242, 0x68DDB3F8, 0x1FDA836E,
	0x81BE16CD, 0xF6B9265B, 0x6FB077E1, 0x18B74777,
	0x88085AE6, 0xFF0F6A70, 0x66063BCA, 0x11010B5C,
	0x8F659EFF, 0xF862AE69, 0x616BFFD3, 0x166CCF45,
	0xA00AE278, 0xD70DD2EE, 0x4E048354, 0x3903B3C2,
	0xA7672661, 0xD06016F7, 0x4969474D, 0x3E6E77DB,
	0xAED16A4A, 0xD9D65ADC, 0x40DF0B66, 0x37D83BF0,
	0xA9BCAE53, 0xDEBB9EC5, 0x47B2CF7F, 0x30B5FFE9,
	0xBDBDF21C, 0xCABAC28A, 0x53B39330, 0x24B4A3A6,
	0xBAD03605, 0xCDD70693, 0x54DE5729, 0x23D967BF,
	0xB3667A2E, 0xC4614AB8, 0x5D681B02, 0x2A6F2B94,
	0xB40BBE37, 0xC30C8EA1, 0x5A05DF1B, 0x2D02EF8D
};
/* ---- end VERBATIM src/backend/utils/hash/pg_crc.c lines 27-100 ---- */

/* ---- VERBATIM src/backend/utils/adt/pg_locale_libc.c lines 832-851 @ 62d6c7d3df ---- */
/*
 * POSIX doesn't define _l-variants of these functions, but several systems
 * have them.  We provide our own replacements here.
 */
#ifndef HAVE_MBSTOWCS_L
static size_t
mbstowcs_l(wchar_t *dest, const char *src, size_t n, locale_t loc)
{
#ifdef WIN32
	return _mbstowcs_l(dest, src, n, loc);
#else
	size_t		result;
	locale_t	save_locale = uselocale(loc);

	result = mbstowcs(dest, src, n);
	uselocale(save_locale);
	return result;
#endif
}
#endif
/* ---- end VERBATIM src/backend/utils/adt/pg_locale_libc.c lines 832-851 ---- */

/* ---- VERBATIM src/backend/utils/adt/pg_locale_libc.c lines 926-1005 @ 62d6c7d3df ---- */
/*
 * char2wchar --- convert multibyte characters to wide characters
 *
 * This has almost the API of mbstowcs_l(), except that *from need not be
 * null-terminated; instead, the number of input bytes is specified as
 * fromlen.  Also, we ereport() rather than returning -1 for invalid
 * input encoding.  tolen is the maximum number of wchar_t's to store at *to.
 * The output will be zero-terminated iff there is room.
 */
size_t
char2wchar(wchar_t *to, size_t tolen, const char *from, size_t fromlen,
		   pg_locale_t locale)
{
	size_t		result;

	if (tolen == 0)
		return 0;

#ifdef WIN32
	/* See WIN32 "Unicode" comment above */
	if (GetDatabaseEncoding() == PG_UTF8)
	{
		/* Win32 API does not work for zero-length input */
		if (fromlen == 0)
			result = 0;
		else
		{
			result = MultiByteToWideChar(CP_UTF8, 0, from, fromlen, to, tolen - 1);
			/* A zero return is failure */
			if (result == 0)
				result = -1;
		}

		if (result != -1)
		{
			Assert(result < tolen);
			/* Append trailing null wchar (MultiByteToWideChar() does not) */
			to[result] = 0;
		}
	}
	else
#endif							/* WIN32 */
	{
		/* mbstowcs requires ending '\0' */
		char	   *str = pnstrdup(from, fromlen);

		if (locale == (pg_locale_t) 0)
		{
			/* Use mbstowcs directly for the default locale */
			result = mbstowcs(to, str, tolen);
		}
		else
		{
			/* Use mbstowcs_l for nondefault locales */
			result = mbstowcs_l(to, str, tolen, locale->info.lt);
		}

		pfree(str);
	}

	if (result == -1)
	{
		/*
		 * Invalid multibyte character encountered.  We try to give a useful
		 * error message by letting pg_verifymbstr check the string.  But it's
		 * possible that the string is OK to us, and not OK to mbstowcs ---
		 * this suggests that the LC_CTYPE locale is different from the
		 * database encoding.  Give a generic error message if pg_verifymbstr
		 * can't find anything wrong.
		 */
		pg_verifymbstr(from, fromlen, false);	/* might not return */
		/* but if it does ... */
		ereport(ERROR,
				(errcode(ERRCODE_CHARACTER_NOT_IN_REPERTOIRE),
				 errmsg("invalid multibyte character for locale"),
				 errhint("The server's LC_CTYPE locale is probably incompatible with the database encoding.")));
	}

	return result;
}
/* ---- end VERBATIM src/backend/utils/adt/pg_locale_libc.c lines 926-1005 ---- */

/* ---- VERBATIM src/backend/tsearch/ts_locale.c lines 23-69 @ 62d6c7d3df ---- */
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

/* ---- end VERBATIM src/backend/tsearch/ts_locale.c lines 23-69 ---- */

/* ---- VERBATIM src/backend/utils/adt/pg_locale_builtin.c lines 80-86 @ 62d6c7d3df ---- */
size_t
strlower_builtin(char *dest, size_t destsize, const char *src, ssize_t srclen,
				 pg_locale_t locale)
{
	return unicode_strlower(dest, destsize, src, srclen,
							locale->info.builtin.casemap_full);
}
/* ---- end VERBATIM src/backend/utils/adt/pg_locale_builtin.c lines 80-86 ---- */


/* SHIM: pin-dead pg_strlower arm (see header) -- provider is BUILTIN
 * whenever pg_strlower is reachable, so this can never run. */
static size_t
strlower_libc(char *dst, size_t dstsize, const char *src, ssize_t srclen,
			  pg_locale_t locale)
{
	(void) dst; (void) dstsize; (void) src; (void) srclen; (void) locale;
	abort();
}

/* ---- VERBATIM src/backend/utils/adt/pg_locale.c lines 1270-1287 @ 62d6c7d3df ---- */
size_t
pg_strlower(char *dst, size_t dstsize, const char *src, ssize_t srclen,
			pg_locale_t locale)
{
	if (locale->provider == COLLPROVIDER_BUILTIN)
		return strlower_builtin(dst, dstsize, src, srclen, locale);
#ifdef USE_ICU
	else if (locale->provider == COLLPROVIDER_ICU)
		return strlower_icu(dst, dstsize, src, srclen, locale);
#endif
	else if (locale->provider == COLLPROVIDER_LIBC)
		return strlower_libc(dst, dstsize, src, srclen, locale);
	else
		/* shouldn't happen */
		PGLOCALE_SUPPORT_ERROR(locale->provider);

	return 0;					/* keep compiler quiet */
}
/* ---- end VERBATIM src/backend/utils/adt/pg_locale.c lines 1270-1287 ---- */

/* ---- VERBATIM src/backend/utils/adt/formatting.c lines 1891-1915 @ 62d6c7d3df ---- */
/*
 * ASCII-only lower function
 *
 * We pass the number of bytes so we can pass varlena and char*
 * to this function.  The result is a palloc'd, null-terminated string.
 */
char *
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

/*
 * ASCII-only upper function
/* ---- end VERBATIM src/backend/utils/adt/formatting.c lines 1891-1915 ---- */

/* ---- VERBATIM src/backend/utils/adt/formatting.c lines 1630-1692 @ 62d6c7d3df ---- */
/*
 * collation-aware, wide-character-aware lower function
 *
 * We pass the number of bytes so we can pass varlena and char*
 * to this function.  The result is a palloc'd, null-terminated string.
 */
char *
str_tolower(const char *buff, size_t nbytes, Oid collid)
{
	char	   *result;
	pg_locale_t mylocale;

	if (!buff)
		return NULL;

	if (!OidIsValid(collid))
	{
		/*
		 * This typically means that the parser could not resolve a conflict
		 * of implicit collations, so report it that way.
		 */
		ereport(ERROR,
				(errcode(ERRCODE_INDETERMINATE_COLLATION),
				 errmsg("could not determine which collation to use for %s function",
						"lower()"),
				 errhint("Use the COLLATE clause to set the collation explicitly.")));
	}

	mylocale = pg_newlocale_from_collation(collid);

	/* C/POSIX collations use this path regardless of database encoding */
	if (mylocale->ctype_is_c)
	{
		result = asc_tolower(buff, nbytes);
	}
	else
	{
		const char *src = buff;
		size_t		srclen = nbytes;
		size_t		dstsize;
		char	   *dst;
		size_t		needed;

		/* first try buffer of equal size plus terminating NUL */
		dstsize = srclen + 1;
		dst = palloc(dstsize);

		needed = pg_strlower(dst, dstsize, src, srclen, mylocale);
		if (needed + 1 > dstsize)
		{
			/* grow buffer if needed and retry */
			dstsize = needed + 1;
			dst = repalloc(dst, dstsize);
			needed = pg_strlower(dst, dstsize, src, srclen, mylocale);
			Assert(needed + 1 <= dstsize);
		}

		Assert(dst[needed] == '\0');
		result = dst;
	}

	return result;
}
/* ---- end VERBATIM src/backend/utils/adt/formatting.c lines 1630-1692 ---- */

/* ---- VERBATIM contrib/pg_trgm/trgm.h lines 12-111 @ 62d6c7d3df ---- */
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
/* ---- end VERBATIM contrib/pg_trgm/trgm.h lines 12-111 ---- */

/* ---- VERBATIM src/include/lib/qunique.h lines 15-40 @ 62d6c7d3df ---- */
/*
 * Remove duplicates from a pre-sorted array, according to a user-supplied
 * comparator.  Usually the array should have been sorted with qsort() using
 * the same arguments.  Return the new size.
 */
static inline size_t
qunique(void *array, size_t elements, size_t width,
		int (*compare) (const void *, const void *))
{
	char	   *bytes = (char *) array;
	size_t		i,
				j;

	if (elements <= 1)
		return elements;

	for (i = 1, j = 0; i < elements; ++i)
	{
		if (compare(bytes + i * width, bytes + j * width) != 0 &&
			++j != i)
			memcpy(bytes + j * width, bytes + i * width, width);
	}

	return j + 1;
}

/* ---- end VERBATIM src/include/lib/qunique.h lines 15-40 ---- */

/* ---- VERBATIM contrib/pg_trgm/trgm_op.c lines 26-29 @ 62d6c7d3df ---- */
/* GUC variables */
double		similarity_threshold = 0.3f;
double		word_similarity_threshold = 0.6f;
double		strict_word_similarity_threshold = 0.5f;
/* ---- end VERBATIM contrib/pg_trgm/trgm_op.c lines 26-29 ---- */

/* ---- VERBATIM contrib/pg_trgm/trgm_op.c lines 48-139 @ 62d6c7d3df ---- */
static int	CMPTRGM_CHOOSE(const void *a, const void *b);
int			(*CMPTRGM) (const void *a, const void *b) = CMPTRGM_CHOOSE;

/* Trigram with position */
typedef struct
{
	trgm		trg;
	int			index;
} pos_trgm;

/* Trigram bound type */
typedef uint8 TrgmBound;
#define TRGM_BOUND_LEFT				0x01	/* trigram is left bound of word */
#define TRGM_BOUND_RIGHT			0x02	/* trigram is right bound of word */

/* Word similarity flags */
#define WORD_SIMILARITY_CHECK_ONLY	0x01	/* only check existence of similar
											 * search pattern in text */
#define WORD_SIMILARITY_STRICT		0x02	/* force bounds of extent to match
											 * word bounds */

/*
 * A growable array of trigrams
 *
 * The actual array of trigrams is in 'datum'.  Note that the other fields in
 * 'datum', i.e. datum->flags and the varlena length, are not kept up to date
 * when items are added to the growable array.  We merely reserve the space
 * for them here.  You must fill those other fields before using 'datum' as a
 * proper TRGM datum.
 */
typedef struct
{
	TRGM	   *datum;			/* trigram array */
	int			length;			/* number of trigrams in the array */
	int			allocated;		/* allocated size of 'datum' (# of trigrams) */
} growable_trgm_array;

/*
 * Allocate a new growable array.
 *
 * 'slen' is the size of the source string that we're extracting the trigrams
 * from.  It is used to choose the initial size of the array.
 */
static void
init_trgm_array(growable_trgm_array *arr, int slen)
{
	size_t		init_size;

	/*
	 * In the extreme case, the input string consists entirely of one
	 * character words, like "a b c", where each word is expanded to two
	 * trigrams.  This is not a strict upper bound though, because when
	 * IGNORECASE is defined, we convert the input string to lowercase before
	 * extracting the trigrams, which in rare cases can expand one input
	 * character into multiple characters.
	 */
	init_size = (size_t) slen + 1;

	/*
	 * Guard against possible overflow in the palloc request.  (We don't worry
	 * about the additive constants, since palloc can detect requests that are
	 * a little above MaxAllocSize --- we just need to prevent integer
	 * overflow in the multiplications.)
	 */
	if (init_size > MaxAllocSize / sizeof(trgm))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("out of memory")));

	arr->datum = palloc(CALCGTSIZE(ARRKEY, init_size));
	arr->allocated = init_size;
	arr->length = 0;
}

/* Make sure the array can hold at least 'needed' more trigrams */
static void
enlarge_trgm_array(growable_trgm_array *arr, int needed)
{
	size_t		new_needed = (size_t) arr->length + needed;

	if (new_needed > arr->allocated)
	{
		/* Guard against possible overflow, like in init_trgm_array */
		if (new_needed > MaxAllocSize / sizeof(trgm))
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("out of memory")));

		arr->datum = repalloc(arr->datum, CALCGTSIZE(ARRKEY, new_needed));
		arr->allocated = new_needed;
	}
}
/* ---- end VERBATIM contrib/pg_trgm/trgm_op.c lines 48-139 ---- */

/* ---- VERBATIM contrib/pg_trgm/trgm_op.c lines 188-227 @ 62d6c7d3df ---- */
#define CMPCHAR(a,b) ( ((a)==(b)) ? 0 : ( ((a)<(b)) ? -1 : 1 ) )

/*
 * Functions for comparing two trgms while treating each char as "signed char" or
 * "unsigned char".
 */
static inline int
CMPTRGM_SIGNED(const void *a, const void *b)
{
#define CMPPCHAR_S(a,b,i)  CMPCHAR( *(((const signed char*)(a))+i), *(((const signed char*)(b))+i) )

	return CMPPCHAR_S(a, b, 0) ? CMPPCHAR_S(a, b, 0)
		: (CMPPCHAR_S(a, b, 1) ? CMPPCHAR_S(a, b, 1)
		   : CMPPCHAR_S(a, b, 2));
}

static inline int
CMPTRGM_UNSIGNED(const void *a, const void *b)
{
#define CMPPCHAR_UNS(a,b,i)  CMPCHAR( *(((const unsigned char*)(a))+i), *(((const unsigned char*)(b))+i) )

	return CMPPCHAR_UNS(a, b, 0) ? CMPPCHAR_UNS(a, b, 0)
		: (CMPPCHAR_UNS(a, b, 1) ? CMPPCHAR_UNS(a, b, 1)
		   : CMPPCHAR_UNS(a, b, 2));
}

/*
 * This gets called on the first call. It replaces the function pointer so
 * that subsequent calls are routed directly to the chosen implementation.
 */
static int
CMPTRGM_CHOOSE(const void *a, const void *b)
{
	if (GetDefaultCharSignedness())
		CMPTRGM = CMPTRGM_SIGNED;
	else
		CMPTRGM = CMPTRGM_UNSIGNED;

	return CMPTRGM(a, b);
}
/* ---- end VERBATIM contrib/pg_trgm/trgm_op.c lines 188-227 ---- */

/* ---- VERBATIM contrib/pg_trgm/trgm_op.c lines 284-1125 @ 62d6c7d3df ---- */
static int
comp_trgm(const void *a, const void *b)
{
	return CMPTRGM(a, b);
}

/*
 * Finds first word in string, returns pointer to the word,
 * endword points to the character after word
 */
static char *
find_word(char *str, int lenstr, char **endword)
{
	char	   *beginword = str;
	const char *endstr = str + lenstr;

	while (beginword < endstr)
	{
		int			clen = pg_mblen_range(beginword, endstr);

		if (ISWORDCHR(beginword, clen))
			break;
		beginword += clen;
	}

	if (beginword >= endstr)
		return NULL;

	*endword = beginword;
	while (*endword < endstr)
	{
		int			clen = pg_mblen_range(*endword, endstr);

		if (!ISWORDCHR(*endword, clen))
			break;
		*endword += clen;
	}

	return beginword;
}

/*
 * Reduce a trigram (three possibly multi-byte characters) to a trgm,
 * which is always exactly three bytes.  If we have three single-byte
 * characters, we just use them as-is; otherwise we form a hash value.
 */
void
compact_trigram(trgm *tptr, char *str, int bytelen)
{
	if (bytelen == 3)
	{
		CPTRGM(tptr, str);
	}
	else
	{
		pg_crc32	crc;

		INIT_LEGACY_CRC32(crc);
		COMP_LEGACY_CRC32(crc, str, bytelen);
		FIN_LEGACY_CRC32(crc);

		/*
		 * use only 3 upper bytes from crc, hope, it's good enough hashing
		 */
		CPTRGM(tptr, &crc);
	}
}

/*
 * Adds trigrams from the word in 'str' (already padded if necessary).
 */
static void
make_trigrams(growable_trgm_array *dst, char *str, int bytelen)
{
	trgm	   *tptr;
	char	   *ptr = str;

	if (bytelen < 3)
		return;

	/* max number of trigrams = strlen - 2 */
	enlarge_trgm_array(dst, bytelen - 2);
	tptr = GETARR(dst->datum) + dst->length;

	if (pg_encoding_max_length(GetDatabaseEncoding()) == 1)
	{
		while (ptr < str + bytelen - 2)
		{
			CPTRGM(tptr, ptr);
			ptr++;
			tptr++;
		}
	}
	else
	{
		int			lenfirst,
					lenmiddle,
					lenlast;
		char	   *endptr;

		/*
		 * Fast path as long as there are no multibyte characters
		 */
		if (!IS_HIGHBIT_SET(ptr[0]) && !IS_HIGHBIT_SET(ptr[1]))
		{
			while (!IS_HIGHBIT_SET(ptr[2]))
			{
				CPTRGM(tptr, ptr);
				ptr++;
				tptr++;

				if (ptr == str + bytelen - 2)
					goto done;
			}

			lenfirst = 1;
			lenmiddle = 1;
			lenlast = pg_mblen_unbounded(ptr + 2);
		}
		else
		{
			lenfirst = pg_mblen_unbounded(ptr);
			if (ptr + lenfirst >= str + bytelen)
				goto done;
			lenmiddle = pg_mblen_unbounded(ptr + lenfirst);
			if (ptr + lenfirst + lenmiddle >= str + bytelen)
				goto done;
			lenlast = pg_mblen_unbounded(ptr + lenfirst + lenmiddle);
		}

		/*
		 * Slow path to handle any remaining multibyte characters
		 *
		 * As we go, 'ptr' points to the beginning of the current
		 * three-character string and 'endptr' points to just past it.
		 */
		endptr = ptr + lenfirst + lenmiddle + lenlast;
		while (endptr <= str + bytelen)
		{
			compact_trigram(tptr, ptr, endptr - ptr);
			tptr++;

			/* Advance to the next character */
			if (endptr == str + bytelen)
				break;
			ptr += lenfirst;
			lenfirst = lenmiddle;
			lenmiddle = lenlast;
			lenlast = pg_mblen_unbounded(endptr);
			endptr += lenlast;
		}
	}

done:
	dst->length = tptr - GETARR(dst->datum);
	Assert(dst->length <= dst->allocated);
}

/*
 * Make array of trigrams without sorting and removing duplicate items.
 *
 * dst: where to return the array of trigrams.
 * str: source string, of length slen bytes.
 * bounds_p: where to return bounds of trigrams (if needed).
 */
static void
generate_trgm_only(growable_trgm_array *dst, char *str, int slen, TrgmBound **bounds_p)
{
	size_t		buflen;
	char	   *buf;
	int			bytelen;
	char	   *bword,
			   *eword;
	TrgmBound  *bounds = NULL;
	int			bounds_allocated = 0;

	init_trgm_array(dst, slen);

	/*
	 * If requested, allocate an array for the bounds, with the same size as
	 * the trigram array.
	 */
	if (bounds_p)
	{
		bounds_allocated = dst->allocated;
		bounds = *bounds_p = palloc0_array(TrgmBound, bounds_allocated);
	}

	if (slen + LPADDING + RPADDING < 3 || slen == 0)
		return;

	/*
	 * Allocate a buffer for case-folded, blank-padded words.
	 *
	 * As an initial guess, allocate a buffer large enough to hold the
	 * original string with padding, which is always enough when compiled with
	 * !IGNORECASE.  If the case-folding produces a string longer than the
	 * original, we'll grow the buffer.
	 */
	buflen = (size_t) slen + 4;
	buf = (char *) palloc(buflen);
	if (LPADDING > 0)
	{
		*buf = ' ';
		if (LPADDING > 1)
			*(buf + 1) = ' ';
	}

	eword = str;
	while ((bword = find_word(eword, slen - (eword - str), &eword)) != NULL)
	{
		int			oldlen;

		/* Convert word to lower case before extracting trigrams from it */
#ifdef IGNORECASE
		{
			char	   *lowered;

			lowered = str_tolower(bword, eword - bword, DEFAULT_COLLATION_OID);
			bytelen = strlen(lowered);

			/* grow the buffer if necessary */
			if (bytelen > buflen - 4)
			{
				pfree(buf);
				buflen = (size_t) bytelen + 4;
				buf = (char *) palloc(buflen);
				if (LPADDING > 0)
				{
					*buf = ' ';
					if (LPADDING > 1)
						*(buf + 1) = ' ';
				}
			}
			memcpy(buf + LPADDING, lowered, bytelen);
			pfree(lowered);
		}
#else
		bytelen = eword - bword;
		memcpy(buf + LPADDING, bword, bytelen);
#endif

		buf[LPADDING + bytelen] = ' ';
		buf[LPADDING + bytelen + 1] = ' ';

		/* Calculate trigrams marking their bounds if needed */
		oldlen = dst->length;
		make_trigrams(dst, buf, bytelen + LPADDING + RPADDING);
		if (bounds)
		{
			if (bounds_allocated < dst->length)
			{
				bounds = *bounds_p = repalloc0_array(bounds, TrgmBound, bounds_allocated, dst->allocated);
				bounds_allocated = dst->allocated;
			}

			bounds[oldlen] |= TRGM_BOUND_LEFT;
			bounds[dst->length - 1] |= TRGM_BOUND_RIGHT;
		}
	}

	pfree(buf);
}

/*
 * Make array of trigrams with sorting and removing duplicate items.
 *
 * str: source string, of length slen bytes.
 *
 * Returns the sorted array of unique trigrams.
 */
TRGM *
generate_trgm(char *str, int slen)
{
	TRGM	   *trg;
	growable_trgm_array arr;
	int			len;

	generate_trgm_only(&arr, str, slen, NULL);
	len = arr.length;
	trg = arr.datum;
	trg->flag = ARRKEY;

	/*
	 * Make trigrams unique.
	 */
	if (len > 1)
	{
		qsort(GETARR(trg), len, sizeof(trgm), comp_trgm);
		len = qunique(GETARR(trg), len, sizeof(trgm), comp_trgm);
	}

	SET_VARSIZE(trg, CALCGTSIZE(ARRKEY, len));

	return trg;
}

/*
 * Make array of positional trigrams from two trigram arrays trg1 and trg2.
 *
 * trg1: trigram array of search pattern, of length len1. trg1 is required
 *		 word which positions don't matter and replaced with -1.
 * trg2: trigram array of text, of length len2. trg2 is haystack where we
 *		 search and have to store its positions.
 *
 * Returns concatenated trigram array.
 */
static pos_trgm *
make_positional_trgm(trgm *trg1, int len1, trgm *trg2, int len2)
{
	pos_trgm   *result;
	int			i,
				len = len1 + len2;

	result = (pos_trgm *) palloc(sizeof(pos_trgm) * len);

	for (i = 0; i < len1; i++)
	{
		memcpy(&result[i].trg, &trg1[i], sizeof(trgm));
		result[i].index = -1;
	}

	for (i = 0; i < len2; i++)
	{
		memcpy(&result[i + len1].trg, &trg2[i], sizeof(trgm));
		result[i + len1].index = i;
	}

	return result;
}

/*
 * Compare position trigrams: compare trigrams first and position second.
 */
static int
comp_ptrgm(const void *v1, const void *v2)
{
	const pos_trgm *p1 = (const pos_trgm *) v1;
	const pos_trgm *p2 = (const pos_trgm *) v2;
	int			cmp;

	cmp = CMPTRGM(p1->trg, p2->trg);
	if (cmp != 0)
		return cmp;

	return pg_cmp_s32(p1->index, p2->index);
}

/*
 * Iterative search function which calculates maximum similarity with word in
 * the string. Maximum similarity is only calculated only if the flag
 * WORD_SIMILARITY_CHECK_ONLY isn't set.
 *
 * trg2indexes: array which stores indexes of the array "found".
 * found: array which stores true of false values.
 * ulen1: count of unique trigrams of array "trg1".
 * len2: length of array "trg2" and array "trg2indexes".
 * len: length of the array "found".
 * flags: set of boolean flags parameterizing similarity calculation.
 * bounds: whether each trigram is left/right bound of word.
 *
 * Returns word similarity.
 */
static float4
iterate_word_similarity(int *trg2indexes,
						bool *found,
						int ulen1,
						int len2,
						int len,
						uint8 flags,
						TrgmBound *bounds)
{
	int		   *lastpos,
				i,
				ulen2 = 0,
				count = 0,
				upper = -1,
				lower;
	float4		smlr_cur,
				smlr_max = 0.0f;
	double		threshold;

	Assert(bounds || !(flags & WORD_SIMILARITY_STRICT));

	/* Select appropriate threshold */
	threshold = (flags & WORD_SIMILARITY_STRICT) ?
		strict_word_similarity_threshold :
		word_similarity_threshold;

	/*
	 * Consider first trigram as initial lower bound for strict word
	 * similarity, or initialize it later with first trigram present for plain
	 * word similarity.
	 */
	lower = (flags & WORD_SIMILARITY_STRICT) ? 0 : -1;

	/* Memorise last position of each trigram */
	lastpos = (int *) palloc(sizeof(int) * len);
	memset(lastpos, -1, sizeof(int) * len);

	for (i = 0; i < len2; i++)
	{
		int			trgindex;

		CHECK_FOR_INTERRUPTS();

		/* Get index of next trigram */
		trgindex = trg2indexes[i];

		/* Update last position of this trigram */
		if (lower >= 0 || found[trgindex])
		{
			if (lastpos[trgindex] < 0)
			{
				ulen2++;
				if (found[trgindex])
					count++;
			}
			lastpos[trgindex] = i;
		}

		/*
		 * Adjust upper bound if trigram is upper bound of word for strict
		 * word similarity, or if trigram is present in required substring for
		 * plain word similarity
		 */
		if ((flags & WORD_SIMILARITY_STRICT) ? (bounds[i] & TRGM_BOUND_RIGHT)
			: found[trgindex])
		{
			int			prev_lower,
						tmp_ulen2,
						tmp_lower,
						tmp_count;

			upper = i;
			if (lower == -1)
			{
				lower = i;
				ulen2 = 1;
			}

			smlr_cur = CALCSML(count, ulen1, ulen2);

			/* Also try to adjust lower bound for greater similarity */
			tmp_count = count;
			tmp_ulen2 = ulen2;
			prev_lower = lower;
			for (tmp_lower = lower; tmp_lower <= upper; tmp_lower++)
			{
				float		smlr_tmp;
				int			tmp_trgindex;

				/*
				 * Adjust lower bound only if trigram is lower bound of word
				 * for strict word similarity, or consider every trigram as
				 * lower bound for plain word similarity.
				 */
				if (!(flags & WORD_SIMILARITY_STRICT)
					|| (bounds[tmp_lower] & TRGM_BOUND_LEFT))
				{
					smlr_tmp = CALCSML(tmp_count, ulen1, tmp_ulen2);
					if (smlr_tmp > smlr_cur)
					{
						smlr_cur = smlr_tmp;
						ulen2 = tmp_ulen2;
						lower = tmp_lower;
						count = tmp_count;
					}

					/*
					 * If we only check that word similarity is greater than
					 * threshold we do not need to calculate a maximum
					 * similarity.
					 */
					if ((flags & WORD_SIMILARITY_CHECK_ONLY)
						&& smlr_cur >= threshold)
						break;
				}

				tmp_trgindex = trg2indexes[tmp_lower];
				if (lastpos[tmp_trgindex] == tmp_lower)
				{
					tmp_ulen2--;
					if (found[tmp_trgindex])
						tmp_count--;
				}
			}

			smlr_max = Max(smlr_max, smlr_cur);

			/*
			 * if we only check that word similarity is greater than threshold
			 * we do not need to calculate a maximum similarity.
			 */
			if ((flags & WORD_SIMILARITY_CHECK_ONLY) && smlr_max >= threshold)
				break;

			for (tmp_lower = prev_lower; tmp_lower < lower; tmp_lower++)
			{
				int			tmp_trgindex;

				tmp_trgindex = trg2indexes[tmp_lower];
				if (lastpos[tmp_trgindex] == tmp_lower)
					lastpos[tmp_trgindex] = -1;
			}
		}
	}

	pfree(lastpos);

	return smlr_max;
}

/*
 * Calculate word similarity.
 * This function prepare two arrays: "trg2indexes" and "found". Then this arrays
 * are used to calculate word similarity using iterate_word_similarity().
 *
 * "trg2indexes" is array which stores indexes of the array "found".
 * In other words:
 * trg2indexes[j] = i;
 * found[i] = true (or false);
 * If found[i] == true then there is trigram trg2[j] in array "trg1".
 * If found[i] == false then there is not trigram trg2[j] in array "trg1".
 *
 * str1: search pattern string, of length slen1 bytes.
 * str2: text in which we are looking for a word, of length slen2 bytes.
 * flags: set of boolean flags parameterizing similarity calculation.
 *
 * Returns word similarity.
 */
static float4
calc_word_similarity(char *str1, int slen1, char *str2, int slen2,
					 uint8 flags)
{
	bool	   *found;
	pos_trgm   *ptrg;
	growable_trgm_array trg1;
	growable_trgm_array trg2;
	int			len1,
				len2,
				len,
				i,
				j,
				ulen1;
	int		   *trg2indexes;
	float4		result;
	TrgmBound  *bounds = NULL;

	/* Make positional trigrams */

	generate_trgm_only(&trg1, str1, slen1, NULL);
	len1 = trg1.length;
	generate_trgm_only(&trg2, str2, slen2, (flags & WORD_SIMILARITY_STRICT) ? &bounds : NULL);
	len2 = trg2.length;

	ptrg = make_positional_trgm(GETARR(trg1.datum), len1, GETARR(trg2.datum), len2);
	len = len1 + len2;
	qsort(ptrg, len, sizeof(pos_trgm), comp_ptrgm);

	pfree(trg1.datum);
	pfree(trg2.datum);

	/*
	 * Merge positional trigrams array: enumerate each trigram and find its
	 * presence in required word.
	 */
	trg2indexes = (int *) palloc(sizeof(int) * len2);
	found = (bool *) palloc0(sizeof(bool) * len);

	ulen1 = 0;
	j = 0;
	for (i = 0; i < len; i++)
	{
		if (i > 0)
		{
			int			cmp = CMPTRGM(ptrg[i - 1].trg, ptrg[i].trg);

			if (cmp != 0)
			{
				if (found[j])
					ulen1++;
				j++;
			}
		}

		if (ptrg[i].index >= 0)
		{
			trg2indexes[ptrg[i].index] = j;
		}
		else
		{
			found[j] = true;
		}
	}
	if (found[j])
		ulen1++;

	/* Run iterative procedure to find maximum similarity with word */
	result = iterate_word_similarity(trg2indexes, found, ulen1, len2, len,
									 flags, bounds);

	pfree(trg2indexes);
	pfree(found);
	pfree(ptrg);

	return result;
}


/*
 * Extract the next non-wildcard part of a search string, i.e. a word bounded
 * by '_' or '%' meta-characters, non-word characters or string end.
 *
 * str: source string, of length lenstr bytes (need not be null-terminated)
 * buf: where to return the substring (must be long enough)
 * *bytelen: receives byte length of the found substring
 *
 * Returns pointer to end+1 of the found substring in the source string.
 * Returns NULL if no word found (in which case buf, bytelen is not set)
 *
 * If the found word is bounded by non-word characters or string boundaries
 * then this function will include corresponding padding spaces into buf.
 */
static const char *
get_wildcard_part(const char *str, int lenstr,
				  char *buf, int *bytelen)
{
	const char *beginword = str;
	const char *endword;
	const char *endstr = str + lenstr;
	char	   *s = buf;
	bool		in_leading_wildcard_meta = false;
	bool		in_trailing_wildcard_meta = false;
	bool		in_escape = false;
	int			clen;

	/*
	 * Find the first word character, remembering whether preceding character
	 * was wildcard meta-character.  Note that the in_escape state persists
	 * from this loop to the next one, since we may exit at a word character
	 * that is in_escape.
	 */
	while (beginword < endstr)
	{
		clen = pg_mblen_range(beginword, endstr);

		if (in_escape)
		{
			if (ISWORDCHR(beginword, clen))
				break;
			in_escape = false;
			in_leading_wildcard_meta = false;
		}
		else
		{
			if (ISESCAPECHAR(beginword))
				in_escape = true;
			else if (ISWILDCARDCHAR(beginword))
				in_leading_wildcard_meta = true;
			else if (ISWORDCHR(beginword, clen))
				break;
			else
				in_leading_wildcard_meta = false;
		}
		beginword += clen;
	}

	/*
	 * Handle string end.
	 */
	if (beginword - str >= lenstr)
		return NULL;

	/*
	 * Add left padding spaces if preceding character wasn't wildcard
	 * meta-character.
	 */
	if (!in_leading_wildcard_meta)
	{
		if (LPADDING > 0)
		{
			*s++ = ' ';
			if (LPADDING > 1)
				*s++ = ' ';
		}
	}

	/*
	 * Copy data into buf until wildcard meta-character, non-word character or
	 * string boundary.  Strip escapes during copy.
	 */
	endword = beginword;
	while (endword < endstr)
	{
		clen = pg_mblen_range(endword, endstr);
		if (in_escape)
		{
			if (ISWORDCHR(endword, clen))
			{
				memcpy(s, endword, clen);
				s += clen;
			}
			else
			{
				/*
				 * Back up endword to the escape character when stopping at an
				 * escaped char, so that subsequent get_wildcard_part will
				 * restart from the escape character.  We assume here that
				 * escape chars are single-byte.
				 */
				endword--;
				break;
			}
			in_escape = false;
		}
		else
		{
			if (ISESCAPECHAR(endword))
				in_escape = true;
			else if (ISWILDCARDCHAR(endword))
			{
				in_trailing_wildcard_meta = true;
				break;
			}
			else if (ISWORDCHR(endword, clen))
			{
				memcpy(s, endword, clen);
				s += clen;
			}
			else
				break;
		}
		endword += clen;
	}

	/*
	 * Add right padding spaces if next character isn't wildcard
	 * meta-character.
	 */
	if (!in_trailing_wildcard_meta)
	{
		if (RPADDING > 0)
		{
			*s++ = ' ';
			if (RPADDING > 1)
				*s++ = ' ';
		}
	}

	*bytelen = s - buf;
	return endword;
}

/*
 * Generates trigrams for wildcard search string.
 *
 * Returns array of trigrams that must occur in any string that matches the
 * wildcard string.  For example, given pattern "a%bcd%" the trigrams
 * " a", "bcd" would be extracted.
 */
TRGM *
generate_wildcard_trgm(const char *str, int slen)
{
	TRGM	   *trg;
	growable_trgm_array arr;
	char	   *buf;
	int			len,
				bytelen;
	const char *eword;

	if (slen + LPADDING + RPADDING < 3 || slen == 0)
	{
		trg = (TRGM *) palloc(TRGMHDRSIZE);
		trg->flag = ARRKEY;
		SET_VARSIZE(trg, TRGMHDRSIZE);
		return trg;
	}

	init_trgm_array(&arr, slen);

	/* Allocate a buffer for blank-padded, but not yet case-folded, words */
	buf = palloc(sizeof(char) * (slen + 4));

	/*
	 * Extract trigrams from each substring extracted by get_wildcard_part.
	 */
	eword = str;
	while ((eword = get_wildcard_part(eword, slen - (eword - str),
									  buf, &bytelen)) != NULL)
	{
		char	   *word;

#ifdef IGNORECASE
		word = str_tolower(buf, bytelen, DEFAULT_COLLATION_OID);
		bytelen = strlen(word);
#else
		word = buf;
#endif

		/*
		 * count trigrams
		 */
		make_trigrams(&arr, word, bytelen);

#ifdef IGNORECASE
		pfree(word);
#endif
	}

	pfree(buf);

	/*
	 * Make trigrams unique.
	 */
	trg = arr.datum;
	len = arr.length;
	if (len > 1)
	{
		qsort(GETARR(trg), len, sizeof(trgm), comp_trgm);
		len = qunique(GETARR(trg), len, sizeof(trgm), comp_trgm);
	}

	trg->flag = ARRKEY;
	SET_VARSIZE(trg, CALCGTSIZE(ARRKEY, len));

	return trg;
}

uint32
trgm2int(trgm *ptr)
{
	uint32		val = 0;

	val |= *(((unsigned char *) ptr));
	val <<= 8;
	val |= *(((unsigned char *) ptr) + 1);
	val <<= 8;
	val |= *(((unsigned char *) ptr) + 2);

	return val;
}
/* ---- end VERBATIM contrib/pg_trgm/trgm_op.c lines 284-1125 ---- */

/* ---- VERBATIM contrib/pg_trgm/trgm_op.c lines 1169-1293 @ 62d6c7d3df ---- */
float4
cnt_sml(TRGM *trg1, TRGM *trg2, bool inexact)
{
	trgm	   *ptr1,
			   *ptr2;
	int			count = 0;
	int			len1,
				len2;

	ptr1 = GETARR(trg1);
	ptr2 = GETARR(trg2);

	len1 = ARRNELEM(trg1);
	len2 = ARRNELEM(trg2);

	/* explicit test is needed to avoid 0/0 division when both lengths are 0 */
	if (len1 <= 0 || len2 <= 0)
		return (float4) 0.0;

	while (ptr1 - GETARR(trg1) < len1 && ptr2 - GETARR(trg2) < len2)
	{
		int			res = CMPTRGM(ptr1, ptr2);

		if (res < 0)
			ptr1++;
		else if (res > 0)
			ptr2++;
		else
		{
			ptr1++;
			ptr2++;
			count++;
		}
	}

	/*
	 * If inexact then len2 is equal to count, because we don't know actual
	 * length of second string in inexact search and we can assume that count
	 * is a lower bound of len2.
	 */
	return CALCSML(count, len1, inexact ? count : len2);
}


/*
 * Returns whether trg2 contains all trigrams in trg1.
 * This relies on the trigram arrays being sorted.
 */
bool
trgm_contained_by(TRGM *trg1, TRGM *trg2)
{
	trgm	   *ptr1,
			   *ptr2;
	int			len1,
				len2;

	ptr1 = GETARR(trg1);
	ptr2 = GETARR(trg2);

	len1 = ARRNELEM(trg1);
	len2 = ARRNELEM(trg2);

	while (ptr1 - GETARR(trg1) < len1 && ptr2 - GETARR(trg2) < len2)
	{
		int			res = CMPTRGM(ptr1, ptr2);

		if (res < 0)
			return false;
		else if (res > 0)
			ptr2++;
		else
		{
			ptr1++;
			ptr2++;
		}
	}
	if (ptr1 - GETARR(trg1) < len1)
		return false;
	else
		return true;
}

/*
 * Return a palloc'd boolean array showing, for each trigram in "query",
 * whether it is present in the trigram array "key".
 * This relies on the "key" array being sorted, but "query" need not be.
 */
bool *
trgm_presence_map(TRGM *query, TRGM *key)
{
	bool	   *result;
	trgm	   *ptrq = GETARR(query),
			   *ptrk = GETARR(key);
	int			lenq = ARRNELEM(query),
				lenk = ARRNELEM(key),
				i;

	result = (bool *) palloc0(lenq * sizeof(bool));

	/* for each query trigram, do a binary search in the key array */
	for (i = 0; i < lenq; i++)
	{
		int			lo = 0;
		int			hi = lenk;

		while (lo < hi)
		{
			int			mid = (lo + hi) / 2;
			int			res = CMPTRGM(ptrq, ptrk + mid);

			if (res < 0)
				hi = mid;
			else if (res > 0)
				lo = mid + 1;
			else
			{
				result[i] = true;
				break;
			}
		}
		ptrq++;
	}

	return result;
}
/* ---- end VERBATIM contrib/pg_trgm/trgm_op.c lines 1169-1293 ---- */


/* ========== fuzz-facing driver entries (NOT Postgres code) ========== */

/* Every entry: arena reset, errcode reset, encoding pin, CMPTRGM pointer
 * reset (re-reads the signedness pin), locale-arm pin, setjmp. */

void
pg_diff_trgm_set_char_signedness(int is_signed)
{
	trgmf_char_signedness = (is_signed != 0);
}

static void
pg_diff_trgm_enter(int locale_arm)
{
	pg_diff_trgm_arena_reset();
	pg_diff_errcode = 0;
	pg_diff_trgm_pending = 0;
	/* locale_arm 2 = SQL_ASCII single-byte database (pg_enc value 0):
	 * pg_database_encoding_max_length() == 1, so make_trigrams takes its
	 * single-byte fast path and no multibyte walker is reachable; ctype
	 * model is the C-locale byte model, as for arm 0. */
	wfam_x_set_db_encoding(locale_arm == 2 ? 0 : PG_UTF8);
	CMPTRGM = CMPTRGM_CHOOSE;
	if (locale_arm == 0 || locale_arm == 2)
	{
		/* database ctype C */
		database_ctype_is_c = true;
		trgmf_locale_model.provider = COLLPROVIDER_LIBC;
		trgmf_locale_model.ctype_is_c = true;
		trgmf_locale_model.info.builtin.casemap_full = false;
	}
	else
	{
		/* builtin C.UTF-8 database */
		database_ctype_is_c = false;
		trgmf_locale_model.provider = COLLPROVIDER_BUILTIN;
		trgmf_locale_model.ctype_is_c = false;
		trgmf_locale_model.info.builtin.casemap_full = false;
	}
}

/* ---- bridge exports for the trgm_regexp oracle TU ----
 * pg_trgm_regexp_io.c (compiled in the trgmrxfam cc build, where the regex
 * engine's include tree lives) shares THIS TU's arena, longjmp channel and
 * locale/encoding/signedness pins, so a raise inside a shared verbatim unit
 * (str_tolower, t_isalnum) lands in whichever entry is live and its
 * allocations are reset by that entry. Thin wrappers only — the statics
 * stay static. */

void
pg_diff_trgm_bridge_enter(int locale_arm)
{
	pg_diff_trgm_enter(locale_arm);
}

jmp_buf *
pg_diff_trgm_bridge_jmp(void)
{
	return &pg_diff_trgm_jmp;
}

void
pg_diff_trgm_bridge_raise(int code)
{
	pg_diff_trgm_raise(code);
}

int
pg_diff_trgm_bridge_pending_set(int code)
{
	return trgmf_errcode_set(code);
}

int
pg_diff_errcode_pending_fetch(void)
{
	return pg_diff_trgm_pending;
}

void *
pg_diff_trgm_bridge_palloc(size_t n)
{
	return trgmf_palloc(n);
}

void *
pg_diff_trgm_bridge_palloc0(size_t n)
{
	return trgmf_palloc0(n);
}

void *
pg_diff_trgm_bridge_repalloc(void *p, size_t n)
{
	return trgmf_repalloc(p, n);
}

void
pg_diff_trgm_bridge_pfree(void *p)
{
	trgmf_pfree(p);
}

/* copy a TRGM's trigram array bytes out, in stored order */
static int
trgmf_copy_out(TRGM *trg, uint8_t *out, int cap, int32_t *n)
{
	int			len = ARRNELEM(trg);

	if (len * 3 > cap)
		abort();				/* driver sizes caps from input length */
	memcpy(out, GETARR(trg), len * 3);
	*n = len;
	return 0;
}

int
pg_diff_trgm_generate(int locale_arm, const uint8_t *s, int len,
					  uint8_t *out, int cap, int32_t *n)
{
	TRGM	   *trg;

	pg_diff_trgm_enter(locale_arm);
	if (setjmp(pg_diff_trgm_jmp) != 0)
		return pg_diff_errcode;
	trg = generate_trgm((char *) s, len);
	return trgmf_copy_out(trg, out, cap, n);
}

int
pg_diff_trgm_wildcard(int locale_arm, const uint8_t *s, int len,
					  uint8_t *out, int cap, int32_t *n)
{
	TRGM	   *trg;

	pg_diff_trgm_enter(locale_arm);
	if (setjmp(pg_diff_trgm_jmp) != 0)
		return pg_diff_errcode;
	trg = generate_wildcard_trgm((const char *) s, len);
	return trgmf_copy_out(trg, out, cap, n);
}

/*
 * show_trgm minus the fmgr/array plumbing: per-element text bytes emitted
 * '\n'-joined in array order. The element-formatting loop body is VERBATIM
 * trgm_op.c lines 1140-1153 (ISPRINTABLETRGM split + snprintf 0x%%06x),
 * with construct_array_builtin/Datum plumbing replaced by the copy-out.
 */
int
pg_diff_trgm_show(int locale_arm, const uint8_t *s, int len,
				  uint8_t *out, int cap, int32_t *outlen, int32_t *nelems)
{
	TRGM	   *trg;
	trgm	   *ptr;
	int			i;
	int			pos = 0;

	pg_diff_trgm_enter(locale_arm);
	if (setjmp(pg_diff_trgm_jmp) != 0)
		return pg_diff_errcode;
	trg = generate_trgm((char *) s, len);
	for (i = 0, ptr = GETARR(trg); i < ARRNELEM(trg); i++, ptr++)
	{
		text	   *item = (text *) palloc(VARHDRSZ + Max(12, pg_database_encoding_max_length() * 3));

		if (pg_database_encoding_max_length() > 1 && !ISPRINTABLETRGM(ptr))
		{
			snprintf(VARDATA(item), 12, "0x%06x", trgm2int(ptr));
			SET_VARSIZE(item, VARHDRSZ + strlen(VARDATA(item)));
		}
		else
		{
			SET_VARSIZE(item, VARHDRSZ + 3);
			CPTRGM(VARDATA(item), ptr);
		}
		/* copy-out (replaces d[i] = PointerGetDatum(item)) */
		{
			int			elen = VARSIZE(item) - VARHDRSZ;

			if (pos + elen + 1 > cap)
				abort();
			memcpy(out + pos, VARDATA(item), elen);
			pos += elen;
			out[pos++] = '\n';
		}
		pfree(item);
	}
	*outlen = pos;
	*nelems = ARRNELEM(trg);
	return 0;
}

int
pg_diff_trgm_similarity(int locale_arm, const uint8_t *a, int alen,
						const uint8_t *b, int blen, float *res)
{
	TRGM	   *trg1;
	TRGM	   *trg2;

	pg_diff_trgm_enter(locale_arm);
	if (setjmp(pg_diff_trgm_jmp) != 0)
		return pg_diff_errcode;
	/* verbatim similarity() core, fmgr shell unwrapped */
	trg1 = generate_trgm((char *) a, alen);
	trg2 = generate_trgm((char *) b, blen);
	*res = cnt_sml(trg1, trg2, false);
	return 0;
}

int
pg_diff_trgm_cnt_sml_inexact(int locale_arm, const uint8_t *a, int alen,
							 const uint8_t *b, int blen, float *res)
{
	TRGM	   *trg1;
	TRGM	   *trg2;

	pg_diff_trgm_enter(locale_arm);
	if (setjmp(pg_diff_trgm_jmp) != 0)
		return pg_diff_errcode;
	trg1 = generate_trgm((char *) a, alen);
	trg2 = generate_trgm((char *) b, blen);
	*res = cnt_sml(trg1, trg2, true);
	return 0;
}

int
pg_diff_trgm_word_similarity(int locale_arm, const uint8_t *a, int alen,
							 const uint8_t *b, int blen, uint8_t flags,
							 float *res)
{
	pg_diff_trgm_enter(locale_arm);
	if (setjmp(pg_diff_trgm_jmp) != 0)
		return pg_diff_errcode;
	*res = calc_word_similarity((char *) a, alen, (char *) b, blen, flags);
	return 0;
}

int
pg_diff_trgm_contained_by(int locale_arm, const uint8_t *a, int alen,
						  const uint8_t *b, int blen, int32_t *res)
{
	TRGM	   *trg1;
	TRGM	   *trg2;

	pg_diff_trgm_enter(locale_arm);
	if (setjmp(pg_diff_trgm_jmp) != 0)
		return pg_diff_errcode;
	trg1 = generate_trgm((char *) a, alen);
	trg2 = generate_trgm((char *) b, blen);
	*res = trgm_contained_by(trg1, trg2) ? 1 : 0;
	return 0;
}

int
pg_diff_trgm_presence_map(int locale_arm, const uint8_t *q, int qlen,
						  const uint8_t *k, int klen,
						  uint8_t *out, int cap, int32_t *n)
{
	TRGM	   *query;
	TRGM	   *key;
	bool	   *map;
	int			lenq;
	int			i;

	pg_diff_trgm_enter(locale_arm);
	if (setjmp(pg_diff_trgm_jmp) != 0)
		return pg_diff_errcode;
	query = generate_trgm((char *) q, qlen);
	key = generate_trgm((char *) k, klen);
	map = trgm_presence_map(query, key);
	lenq = ARRNELEM(query);
	if (lenq > cap)
		abort();
	for (i = 0; i < lenq; i++)
		out[i] = map[i] ? 1 : 0;
	*n = lenq;
	return 0;
}

uint32_t
pg_diff_trgm_trgm2int(const uint8_t t[3])
{
	return trgm2int((trgm *) t);
}

void
pg_diff_trgm_compact(const uint8_t *s, int len, uint8_t out[3])
{
	trgm		t;

	/* no entry reset: pure over its inputs, called in exhaustive loops */
	compact_trigram(&t, (char *) s, len);
	memcpy(out, t, 3);
}

int
pg_diff_trgm_cmp(const uint8_t a[3], const uint8_t b[3], int is_signed)
{
	trgmf_char_signedness = (is_signed != 0);
	CMPTRGM = CMPTRGM_CHOOSE;
	return CMPTRGM(a, b);
}
