/*
 * pg_ltreefam_io.c: vendored PostgreSQL C oracle for the ltree_diff
 * differential fuzz target (100%-coverage campaign, lane p1-ltree-t74,
 * task #74). Crate under test: crates/contrib/ltree
 * (see fuzz/core/src/ltree_diff.rs).
 *
 * ORACLE PROPER: this TU #includes the banked verbatim family files
 * csrc/ltreefam/{crc32.c,ltree_io.c,ltree_op.c,lquery_op.c,ltxtquery_io.c,
 * ltxtquery_op.c,_ltree_op.c} (cmp-verified byte-identical to
 * ~/dev/pgrust-fabled/vendor/postgres-src contrib/ltree, Stamp-18.3,
 * upstream sha 62d6c7d3df) after an environment block assembled by
 * scratchpad/assemble_ltreefam.py — verbatim blocks are extracted
 * MECHANICALLY from csrc/pg_hstorefam_io.c (itself verbatim @ 62d6c7d3df,
 * hst_ prefix renamed lt_) and from the vendor tree; never hand-typed.
 *
 * Additional verbatim vendor blocks (all @ 62d6c7d3df):
 *   - c.h varlena + varatt.h 18-325 + array.h ArrayType/ARR_ macros +
 *     stringinfo.h/.c + pqformat.c + varlena.c cstring_to_text /
 *     text_to_cstring + arrayutils.c ArrayGetNItems(Safe)/CheckBounds(Safe)
 *     (via pg_hstorefam_io.c blocks).
 *   - arrayfuncs.c array_contains_nulls (3767-3812).
 *   - pqformat.h pq_writeint8 (45-54) + pq_sendint8 (126-132).
 *   - pgstrcasecmp.c pg_ascii_tolower (145-151).
 *   - ts_locale.h TOUCHAR/t_iseq (35-38); ts_locale.c WC_BUF_LEN +
 *     GENERATE_T_ISCLASS_DEF + alnum/alpha instantiations (32-70).
 *   - pg_crc.h pg_crc32 + TRADITIONAL_CRC32 macros + COMP_CRC32_NORMAL_TABLE
 *     (37-70); backend/utils/hash/pg_crc.c pg_crc32_table (whole table).
 *
 * SHIMS (plumbing/environment only, never logic):
 *   - palloc/palloc0/repalloc -> tracked TLS malloc arena, freed by
 *     pg_lt_reset() per exec (lanej LSan pattern via hstorefam); pfree no-op.
 *   - ereport(ERROR)/elog(ERROR) -> real MAKE_SQLSTATE in TLS + longjmp;
 *     errsave/ereturn honor a non-NULL escontext (soft-error protocol) and
 *     bump lt_soft_fires, the c-escontext-branch-executed witness counter
 *     the driver asserts (vacuous-plane rule).
 *   - check_stack_depth(): REAL byte-based guard (miscadmin.h semantics:
 *     TLS base armed once per thread at first dispatcher entry, limit
 *     2048kB - STACK_DEPTH_SLOP 512kB, PG's booted default), raising 54001
 *     through the ereport shim. The two sides' guards trip at different
 *     depths (pgrust frames are ~5x larger; measured claim-row gap
 *     3741 vs 18665) -> the DRIVER treats 54001 on EITHER side as the
 *     documented CAPACITY CARVE and skips value comparison for the exec.
 *   - CHECK_FOR_INTERRUPTS() -> no-op.
 *   - LOCALE PIN (dead-lane finding 4, cost 8 false divergences): both
 *     sides run the C-ctype/C-collation database (docker probe DB
 *     LC_CTYPE 'C' ENCODING 'UTF8'). database_ctype_is_c = true;
 *     pg_newlocale_from_collation returns a static { ctype_is_c = true }.
 *     The non-C-ctype branches (pg_strfold, char2wchar) are therefore
 *     UNREACHABLE UNDER THE PIN and are abort()-loud link stubs, NOT
 *     fabricated folds (never-fabricate-C-bodies law).
 *   - pg_mblen_cstr/pg_mblen_range/pg_mblen_with_len/pg_mblen_unbounded ->
 *     the verbatim wfam_ mbutils copies exported by csrc/pg_wcharfam.c
 *     (encoding pinned UTF8 on both sides).
 *   - hash_any/hash_any_extended -> the verbatim hashfn.c copies exported
 *     by csrc/pg_mac_io.c (pg_hash_bytes/pg_hash_bytes_extended).
 *   - fmgr: minimal FunctionCallInfoBaseData + PG_GETARG/PG_RETURN macros
 *     (hstorefam precedent); PG_DETOAST_DATUM -> identity (driver
 *     precondition: plain 4B-header images); DirectFunctionCall1/2 ->
 *     local frame helpers; PG_FREE_IF_COPY no-op.
 *   - ltreeparentsel is OUT of scope (planner selectivity, claim row):
 *     generic_restriction_selectivity + its PlannerInfo/List args are
 *     abort()-loud stubs the driver can never dispatch.
 *   - GiST (ltree_gist.c/_ltree_gist.c) NOT compiled: out of scope.
 *
 * CHAR SIGNEDNESS NOTE: the oracle of record is aarch64 Linux (glibc),
 * where plain char is UNSIGNED; macOS aarch64 char is signed. Audited
 * char-sensitive spots: isdigit/isspace calls all cast (unsigned char);
 * t_iseq/TOUCHAR reads via unsigned char; pg_ascii_tolower takes unsigned
 * char; ltree_compare uses memcmp (unsigned on both). ltree_crc32_sz's
 * `char c = pg_ascii_tolower(*p)` stores back to char: the CRC byte is
 * read via unsigned char pointer in COMP_CRC32_NORMAL_TABLE, so the bit
 * pattern is identical either way. No behavioral difference found; the
 * fleet floor runs on the platform of record anyway.
 *
 * Driver entries (SECTION D, pg_lt_* prefix) are fuzz plumbing, NOT
 * Postgres code. Every extern this TU defines is lt_/pg_lt_-prefixed
 * (in-file #define renames, hstorefam precedent; nm census in the lane
 * report).
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

#include "pg_oracle_guard.h"

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
typedef uintptr_t Datum;
typedef uint8 bits8;
#define InvalidOid ((Oid) 0)

#define Assert(x) ((void) 0)
#define AssertMacro(x) ((void) 0)
#define StaticAssertStmt(c, m) ((void) 0)
#define StaticAssertDecl(c, m) extern void lt_static_assert_decl(void)
#define FLEXIBLE_ARRAY_MEMBER	/* empty */
#define PGDLLEXPORT				/* empty */
#define pg_attribute_always_inline __attribute__((always_inline)) inline
#define pg_attribute_unused() __attribute__((unused))
#define pg_noinline __attribute__((noinline))
#define pg_restrict __restrict
#define unlikely(x) __builtin_expect((x) != 0, 0)
#define likely(x)	__builtin_expect((x) != 0, 1)
#define HAVE__BUILTIN_OP_OVERFLOW 1
#define MAXIMUM_ALIGNOF 8
#define lengthof(array) (sizeof (array) / sizeof ((array)[0]))
#define Min(x, y)		((x) < (y) ? (x) : (y))
#define Max(x, y)		((x) > (y) ? (x) : (y))
#define TYPEALIGN(ALIGNVAL,LEN)  \
	(((uintptr_t) (LEN) + ((ALIGNVAL) - 1)) & ~((uintptr_t) ((ALIGNVAL) - 1)))
#define SHORTALIGN(LEN)			TYPEALIGN(2, (LEN))
#define INTALIGN(LEN)			TYPEALIGN(4, (LEN))
#define DOUBLEALIGN(LEN)		TYPEALIGN(8, (LEN))
#define MAXALIGN(LEN)			TYPEALIGN(MAXIMUM_ALIGNOF, (LEN))
#define CHECK_FOR_INTERRUPTS() ((void) 0)

/* pg_config_manual.h / memutils.h line 40 @ 62d6c7d3df */
#define MaxAllocSize	((Size) 0x3fffffff) /* 1 gigabyte - 1 */

/* pg_type.h typalign symbols (verbatim values) */
#define TYPALIGN_CHAR	'c'
#define TYPALIGN_SHORT	's'
#define TYPALIGN_INT	'i'
#define TYPALIGN_DOUBLE 'd'

/* pg_type_d.h oids referenced by the pasted bodies (verbatim values) */
#define CHAROID 18
#define TEXTOID 25
#define OIDOID 26
#define TIDOID 27
#define CSTRINGOID 2275

/* Datum access macros (postgres.h, reduced to the pasted bodies' needs) */
#define DatumGetPointer(X) ((char *) (X))
#define PointerGetDatum(X) ((Datum) (X))
#define DatumGetCString(X) ((char *) DatumGetPointer(X))
#define CStringGetDatum(X) PointerGetDatum(X)
#define DatumGetInt32(X) ((int32) (X))
#define Int32GetDatum(X) ((Datum) (int32) (X))
#define DatumGetInt64(X) ((int64) (X))
#define Int64GetDatum(X) ((Datum) (X))
#define DatumGetUInt32(X) ((uint32) (X))
#define UInt32GetDatum(X) ((Datum) (X))
#define DatumGetUInt64(X) ((uint64) (X))
#define UInt64GetDatum(X) ((Datum) (X))
#define DatumGetBool(X) ((bool) ((X) != 0))
#define BoolGetDatum(X) ((Datum) ((X) ? 1 : 0))
#define DatumGetObjectId(X) ((Oid) (X))
#define ObjectIdGetDatum(X) ((Datum) (X))
#define DatumGetChar(X) ((char) (X))
#define CharGetDatum(X) ((Datum) (X))
#define Int8GetDatum(X) ((Datum) (X))
#define Int16GetDatum(X) ((Datum) (X))
#define DatumGetInt16(X) ((int16) (X))
#define SET_8_BYTES(value) ((Datum) (value))
#define CppConcat(x, y) x##y

/* c.h lines 1126-1127 @ 62d6c7d3df */
#define HIGHBIT					(0x80)
#define IS_HIGHBIT_SET(ch)		((unsigned char)(ch) & HIGHBIT)

/* stringinfo.h @ 62d6c7d3df */
#define STRINGINFO_DEFAULT_SIZE 1024

#define PG_USED_FOR_ASSERTS_ONLY pg_attribute_unused()
/* memutils.h line 42 @ 62d6c7d3df */
#define AllocSizeIsValid(size)	((Size) (size) <= MaxAllocSize)

/* pg_type_d.h oids + supporting typedefs for the pasted builtin tables
 * (verbatim catalog values @ 62d6c7d3df) */
typedef float float4;
typedef double float8;
#define FLOAT8PASSBYVAL true	/* USE_FLOAT8_BYVAL on LP64 */
/* fmgr.h detoast macros -> identity (driver precondition: plain images) */
#define PG_DETOAST_DATUM(d)	 ((struct varlena *) DatumGetPointer(d))
#define PG_DETOAST_DATUM_PACKED(d) ((struct varlena *) DatumGetPointer(d))
#define NAMEDATALEN 64
typedef struct ItemPointerData
{
	uint16		bi_hi;
	uint16		bi_lo;
	uint16		ip_posid;
} ItemPointerData;				/* itemptr.h shape: sizeof == 6 */
#define BOOLOID 16
#define NAMEOID 19
#define INT8OID 20
#define INT2OID 21
#define INT4OID 23
#define REGPROCOID 24
#define REGTYPEOID 2206
typedef uint32 TransactionId;
typedef uint32 CommandId;
#define XIDOID 28
#define CIDOID 29
#define FLOAT4OID 700
#define FLOAT8OID 701

/* ---- error protocol shims (see file header) ---- */
static _Thread_local jmp_buf lt_env;
static _Thread_local int lt_sqlstate;

/* verbatim MAKE_SQLSTATE encoding from src/include/utils/elog.h */
#define PGSIXBIT(ch)	(((ch) - '0') & 0x3f)
#define MAKE_SQLSTATE(ch1,ch2,ch3,ch4,ch5)	\
	(PGSIXBIT(ch1) + (PGSIXBIT(ch2) << 6) + (PGSIXBIT(ch3) << 12) + \
	 (PGSIXBIT(ch4) << 18) + (PGSIXBIT(ch5) << 24))
#define ERRCODE_SYNTAX_ERROR				MAKE_SQLSTATE('4','2','6','0','1')
#define ERRCODE_STRING_DATA_RIGHT_TRUNCATION MAKE_SQLSTATE('2','2','0','0','1')
#define ERRCODE_NULL_VALUE_NOT_ALLOWED		MAKE_SQLSTATE('2','2','0','0','4')
#define ERRCODE_PROGRAM_LIMIT_EXCEEDED		MAKE_SQLSTATE('5','4','0','0','0')
#define ERRCODE_ARRAY_SUBSCRIPT_ERROR		MAKE_SQLSTATE('2','2','0','2','E')
#define ERRCODE_PROTOCOL_VIOLATION			MAKE_SQLSTATE('0','8','P','0','1')
#define ERRCODE_CHARACTER_NOT_IN_REPERTOIRE MAKE_SQLSTATE('2','2','0','2','1')
#define ERRCODE_INVALID_PARAMETER_VALUE		MAKE_SQLSTATE('2','2','0','2','3')
#define ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE	MAKE_SQLSTATE('2','2','0','0','3')
#define ERRCODE_INVALID_BINARY_REPRESENTATION MAKE_SQLSTATE('2','2','P','0','3')
#define ERRCODE_OUT_OF_MEMORY				MAKE_SQLSTATE('5','3','2','0','0')
#define ERRCODE_INTERNAL_ERROR				MAKE_SQLSTATE('X','X','0','0','0')
#define ERRCODE_DATATYPE_MISMATCH			MAKE_SQLSTATE('4','2','8','0','4')

static void
lt_raise(void)
{
	longjmp(lt_env, 1);
}

/* Node reduced to the SOFT_ERROR_OCCURRED flag (miscnodes.h protocol) */
typedef struct Node
{
	int			error_occurred;
} Node;

static _Thread_local int lt_soft_sqlstate;

static void
lt_errsave_fire(void *escontext)
{
	if (escontext != NULL)
	{
		((Node *) escontext)->error_occurred = 1;
		lt_soft_sqlstate = lt_sqlstate;
	}
	else
		lt_raise();
}

#define errcode(c) (lt_sqlstate = (c), 0)
#define errmsg(...) 0
#define errmsg_internal(...) 0
#define errdetail(...) 0
#define errhint(...) 0
#define ereport(level, ...) do { (void) (__VA_ARGS__); lt_raise(); } while (0)
#define errsave(escontext, ...) \
	do { (void) (__VA_ARGS__); lt_errsave_fire(escontext); } while (0)
#define ereturn(escontext, dummy_value, ...) \
	do { errsave(escontext, __VA_ARGS__); return dummy_value; } while (0)
#define SOFT_ERROR_OCCURRED(escontext) \
	((escontext) != NULL && ((Node *) (escontext))->error_occurred)
#define elog(level, ...) \
	do { lt_sqlstate = ERRCODE_INTERNAL_ERROR; lt_raise(); } while (0)

/* ---- palloc arena (models per-exec memory context reset) ---- */
static _Thread_local void **lt_allocs;
static _Thread_local size_t lt_nallocs, lt_aallocs;

/* SHIM: additional errcodes the ltree family raises (verbatim MAKE_SQLSTATE
 * spellings from utils/errcodes.h @ 62d6c7d3df) */
#define ERRCODE_NAME_TOO_LONG				MAKE_SQLSTATE('4','2','6','2','2')
#define ERRCODE_STATEMENT_TOO_COMPLEX		MAKE_SQLSTATE('5','4','0','0','1')

/* SHIM: c-escontext-branch-executed witness counter (vacuity rule). The
 * rename below folds it into lt_errsave_fire's body: see lt_soft_fires use
 * in SECTION D. */
static _Thread_local int lt_soft_fires;


static void *
lt_track(void *p)
{
	if (lt_nallocs == lt_aallocs)
	{
		lt_aallocs = lt_aallocs ? lt_aallocs * 2 : 1024;
		lt_allocs = realloc(lt_allocs, lt_aallocs * sizeof(void *));
	}
	lt_allocs[lt_nallocs++] = p;
	return p;
}

static void *
lt_palloc(Size sz)
{
	/* mcxt.c parity: palloc enforces MaxAllocSize (elog ERROR path) */
	if (sz > MaxAllocSize)
	{
		lt_sqlstate = ERRCODE_INTERNAL_ERROR;
		lt_raise();
	}
	void	   *p = malloc(sz ? sz : 1);

	if (p == NULL)
	{
		lt_sqlstate = ERRCODE_OUT_OF_MEMORY;
		lt_raise();
	}
	return lt_track(p);
}

static void *
lt_palloc0(Size sz)
{
	void	   *p = lt_palloc(sz);

	memset(p, 0, sz);
	return p;
}

static void *
lt_repalloc(void *p, Size sz)
{
	if (sz > MaxAllocSize)
	{
		lt_sqlstate = ERRCODE_INTERNAL_ERROR;
		lt_raise();
	}
	for (size_t i = lt_nallocs; i-- > 0;)
	{
		if (lt_allocs[i] == p)
		{
			void	   *np = realloc(p, sz);

			if (np == NULL)
			{
				lt_sqlstate = ERRCODE_OUT_OF_MEMORY;
				lt_raise();
			}
			lt_allocs[i] = np;
			return np;
		}
	}
	abort();					/* repalloc of an untracked pointer */
}

static char *
lt_pstrdup(const char *s)
{
	size_t		n = strlen(s) + 1;
	char	   *r = lt_palloc(n);

	memcpy(r, s, n);
	return r;
}

#define palloc(n) lt_palloc(n)
#define palloc0(n) lt_palloc0(n)
#define repalloc(p, n) lt_repalloc((p), (n))
#define pfree(p) ((void) (p))	/* arena-freed at pg_lt_reset */
#define pstrdup(s) lt_pstrdup(s)

/* pvsnprintf -> libc vsnprintf (see header; \u%04x arm only) */
static size_t
lt_pvsnprintf(char *buf, size_t len, const char *fmt, va_list args)
{
	int			n = vsnprintf(buf, len, fmt, args);

	if (n < 0)
		abort();
	return (size_t) n;
}

#define pvsnprintf lt_pvsnprintf

/* ==== symbol prefixing: every extern definition in this TU ==== */
#define pg_strcasecmp			lt_pg_strcasecmp
#define scanner_isspace			lt_scanner_isspace
#define initStringInfo			lt_initStringInfo
#define resetStringInfo			lt_resetStringInfo
#define appendStringInfo		lt_appendStringInfo
#define appendStringInfoVA		lt_appendStringInfoVA
#define appendStringInfoString	lt_appendStringInfoString
#define appendStringInfoChar	lt_appendStringInfoChar
#define appendBinaryStringInfo	lt_appendBinaryStringInfo
#define appendBinaryStringInfoNT lt_appendBinaryStringInfoNT

/* SHIM: per-exec arena reset (pg_lt_reset) */
static void
lt_arena_reset(void)
{
	for (size_t i = 0; i < lt_nallocs; i++)
		free(lt_allocs[i]);
	lt_nallocs = 0;
}

/* ==== VERBATIM: c.h lines 655-669 @ 62d6c7d3df ==== */
struct varlena
{
	char		vl_len_[4];		/* Do not touch this field directly! */
	char		vl_dat[FLEXIBLE_ARRAY_MEMBER];	/* Data content is here */
};

#define VARHDRSZ		((int32) sizeof(int32))

/*
 * These widely-used datatypes are just a varlena header and the data bytes.
 * There is no terminating null or anything like that --- the data length is
 * always VARSIZE_ANY_EXHDR(ptr).
 */
typedef struct varlena bytea;
typedef struct varlena text;

/* ==== VERBATIM: varatt.h lines 18-325 @ 62d6c7d3df ==== */
/*
 * struct varatt_external is a traditional "TOAST pointer", that is, the
 * information needed to fetch a Datum stored out-of-line in a TOAST table.
 * The data is compressed if and only if the external size stored in
 * va_extinfo is less than va_rawsize - VARHDRSZ.
 *
 * This struct must not contain any padding, because we sometimes compare
 * these pointers using memcmp.
 *
 * Note that this information is stored unaligned within actual tuples, so
 * you need to memcpy from the tuple into a local struct variable before
 * you can look at these fields!  (The reason we use memcmp is to avoid
 * having to do that just to detect equality of two TOAST pointers...)
 */
typedef struct varatt_external
{
	int32		va_rawsize;		/* Original data size (includes header) */
	uint32		va_extinfo;		/* External saved size (without header) and
								 * compression method */
	Oid			va_valueid;		/* Unique ID of value within TOAST table */
	Oid			va_toastrelid;	/* RelID of TOAST table containing it */
}			varatt_external;

/*
 * These macros define the "saved size" portion of va_extinfo.  Its remaining
 * two high-order bits identify the compression method.
 */
#define VARLENA_EXTSIZE_BITS	30
#define VARLENA_EXTSIZE_MASK	((1U << VARLENA_EXTSIZE_BITS) - 1)

/*
 * struct varatt_indirect is a "TOAST pointer" representing an out-of-line
 * Datum that's stored in memory, not in an external toast relation.
 * The creator of such a Datum is entirely responsible that the referenced
 * storage survives for as long as referencing pointer Datums can exist.
 *
 * Note that just as for struct varatt_external, this struct is stored
 * unaligned within any containing tuple.
 */
typedef struct varatt_indirect
{
	struct varlena *pointer;	/* Pointer to in-memory varlena */
}			varatt_indirect;

/*
 * struct varatt_expanded is a "TOAST pointer" representing an out-of-line
 * Datum that is stored in memory, in some type-specific, not necessarily
 * physically contiguous format that is convenient for computation not
 * storage.  APIs for this, in particular the definition of struct
 * ExpandedObjectHeader, are in src/include/utils/expandeddatum.h.
 *
 * Note that just as for struct varatt_external, this struct is stored
 * unaligned within any containing tuple.
 */
typedef struct ExpandedObjectHeader ExpandedObjectHeader;

typedef struct varatt_expanded
{
	ExpandedObjectHeader *eohptr;
} varatt_expanded;

/*
 * Type tag for the various sorts of "TOAST pointer" datums.  The peculiar
 * value for VARTAG_ONDISK comes from a requirement for on-disk compatibility
 * with a previous notion that the tag field was the pointer datum's length.
 */
typedef enum vartag_external
{
	VARTAG_INDIRECT = 1,
	VARTAG_EXPANDED_RO = 2,
	VARTAG_EXPANDED_RW = 3,
	VARTAG_ONDISK = 18
} vartag_external;

/* this test relies on the specific tag values above */
#define VARTAG_IS_EXPANDED(tag) \
	(((tag) & ~1) == VARTAG_EXPANDED_RO)

#define VARTAG_SIZE(tag) \
	((tag) == VARTAG_INDIRECT ? sizeof(varatt_indirect) : \
	 VARTAG_IS_EXPANDED(tag) ? sizeof(varatt_expanded) : \
	 (tag) == VARTAG_ONDISK ? sizeof(varatt_external) : \
	 (AssertMacro(false), 0))

/*
 * These structs describe the header of a varlena object that may have been
 * TOASTed.  Generally, don't reference these structs directly, but use the
 * macros below.
 *
 * We use separate structs for the aligned and unaligned cases because the
 * compiler might otherwise think it could generate code that assumes
 * alignment while touching fields of a 1-byte-header varlena.
 */
typedef union
{
	struct						/* Normal varlena (4-byte length) */
	{
		uint32		va_header;
		char		va_data[FLEXIBLE_ARRAY_MEMBER];
	}			va_4byte;
	struct						/* Compressed-in-line format */
	{
		uint32		va_header;
		uint32		va_tcinfo;	/* Original data size (excludes header) and
								 * compression method; see va_extinfo */
		char		va_data[FLEXIBLE_ARRAY_MEMBER]; /* Compressed data */
	}			va_compressed;
} varattrib_4b;

typedef struct
{
	uint8		va_header;
	char		va_data[FLEXIBLE_ARRAY_MEMBER]; /* Data begins here */
} varattrib_1b;

/* TOAST pointers are a subset of varattrib_1b with an identifying tag byte */
typedef struct
{
	uint8		va_header;		/* Always 0x80 or 0x01 */
	uint8		va_tag;			/* Type of datum */
	char		va_data[FLEXIBLE_ARRAY_MEMBER]; /* Type-specific data */
} varattrib_1b_e;

/*
 * Bit layouts for varlena headers on big-endian machines:
 *
 * 00xxxxxx 4-byte length word, aligned, uncompressed data (up to 1G)
 * 01xxxxxx 4-byte length word, aligned, *compressed* data (up to 1G)
 * 10000000 1-byte length word, unaligned, TOAST pointer
 * 1xxxxxxx 1-byte length word, unaligned, uncompressed data (up to 126b)
 *
 * Bit layouts for varlena headers on little-endian machines:
 *
 * xxxxxx00 4-byte length word, aligned, uncompressed data (up to 1G)
 * xxxxxx10 4-byte length word, aligned, *compressed* data (up to 1G)
 * 00000001 1-byte length word, unaligned, TOAST pointer
 * xxxxxxx1 1-byte length word, unaligned, uncompressed data (up to 126b)
 *
 * The "xxx" bits are the length field (which includes itself in all cases).
 * In the big-endian case we mask to extract the length, in the little-endian
 * case we shift.  Note that in both cases the flag bits are in the physically
 * first byte.  Also, it is not possible for a 1-byte length word to be zero;
 * this lets us disambiguate alignment padding bytes from the start of an
 * unaligned datum.  (We now *require* pad bytes to be filled with zero!)
 *
 * In TOAST pointers the va_tag field (see varattrib_1b_e) is used to discern
 * the specific type and length of the pointer datum.
 */

/*
 * Endian-dependent macros.  These are considered internal --- use the
 * external macros below instead of using these directly.
 *
 * Note: IS_1B is true for external toast records but VARSIZE_1B will return 0
 * for such records. Hence you should usually check for IS_EXTERNAL before
 * checking for IS_1B.
 */

#ifdef WORDS_BIGENDIAN

#define VARATT_IS_4B(PTR) \
	((((varattrib_1b *) (PTR))->va_header & 0x80) == 0x00)
#define VARATT_IS_4B_U(PTR) \
	((((varattrib_1b *) (PTR))->va_header & 0xC0) == 0x00)
#define VARATT_IS_4B_C(PTR) \
	((((varattrib_1b *) (PTR))->va_header & 0xC0) == 0x40)
#define VARATT_IS_1B(PTR) \
	((((varattrib_1b *) (PTR))->va_header & 0x80) == 0x80)
#define VARATT_IS_1B_E(PTR) \
	((((varattrib_1b *) (PTR))->va_header) == 0x80)
#define VARATT_NOT_PAD_BYTE(PTR) \
	(*((uint8 *) (PTR)) != 0)

/* VARSIZE_4B() should only be used on known-aligned data */
#define VARSIZE_4B(PTR) \
	(((varattrib_4b *) (PTR))->va_4byte.va_header & 0x3FFFFFFF)
#define VARSIZE_1B(PTR) \
	(((varattrib_1b *) (PTR))->va_header & 0x7F)
#define VARTAG_1B_E(PTR) \
	(((varattrib_1b_e *) (PTR))->va_tag)

#define SET_VARSIZE_4B(PTR,len) \
	(((varattrib_4b *) (PTR))->va_4byte.va_header = (len) & 0x3FFFFFFF)
#define SET_VARSIZE_4B_C(PTR,len) \
	(((varattrib_4b *) (PTR))->va_4byte.va_header = ((len) & 0x3FFFFFFF) | 0x40000000)
#define SET_VARSIZE_1B(PTR,len) \
	(((varattrib_1b *) (PTR))->va_header = (len) | 0x80)
#define SET_VARTAG_1B_E(PTR,tag) \
	(((varattrib_1b_e *) (PTR))->va_header = 0x80, \
	 ((varattrib_1b_e *) (PTR))->va_tag = (tag))

#else							/* !WORDS_BIGENDIAN */

#define VARATT_IS_4B(PTR) \
	((((varattrib_1b *) (PTR))->va_header & 0x01) == 0x00)
#define VARATT_IS_4B_U(PTR) \
	((((varattrib_1b *) (PTR))->va_header & 0x03) == 0x00)
#define VARATT_IS_4B_C(PTR) \
	((((varattrib_1b *) (PTR))->va_header & 0x03) == 0x02)
#define VARATT_IS_1B(PTR) \
	((((varattrib_1b *) (PTR))->va_header & 0x01) == 0x01)
#define VARATT_IS_1B_E(PTR) \
	((((varattrib_1b *) (PTR))->va_header) == 0x01)
#define VARATT_NOT_PAD_BYTE(PTR) \
	(*((uint8 *) (PTR)) != 0)

/* VARSIZE_4B() should only be used on known-aligned data */
#define VARSIZE_4B(PTR) \
	((((varattrib_4b *) (PTR))->va_4byte.va_header >> 2) & 0x3FFFFFFF)
#define VARSIZE_1B(PTR) \
	((((varattrib_1b *) (PTR))->va_header >> 1) & 0x7F)
#define VARTAG_1B_E(PTR) \
	(((varattrib_1b_e *) (PTR))->va_tag)

#define SET_VARSIZE_4B(PTR,len) \
	(((varattrib_4b *) (PTR))->va_4byte.va_header = (((uint32) (len)) << 2))
#define SET_VARSIZE_4B_C(PTR,len) \
	(((varattrib_4b *) (PTR))->va_4byte.va_header = (((uint32) (len)) << 2) | 0x02)
#define SET_VARSIZE_1B(PTR,len) \
	(((varattrib_1b *) (PTR))->va_header = (((uint8) (len)) << 1) | 0x01)
#define SET_VARTAG_1B_E(PTR,tag) \
	(((varattrib_1b_e *) (PTR))->va_header = 0x01, \
	 ((varattrib_1b_e *) (PTR))->va_tag = (tag))

#endif							/* WORDS_BIGENDIAN */

#define VARDATA_4B(PTR)		(((varattrib_4b *) (PTR))->va_4byte.va_data)
#define VARDATA_4B_C(PTR)	(((varattrib_4b *) (PTR))->va_compressed.va_data)
#define VARDATA_1B(PTR)		(((varattrib_1b *) (PTR))->va_data)
#define VARDATA_1B_E(PTR)	(((varattrib_1b_e *) (PTR))->va_data)

/*
 * Externally visible TOAST macros begin here.
 */

#define VARHDRSZ_EXTERNAL		offsetof(varattrib_1b_e, va_data)
#define VARHDRSZ_COMPRESSED		offsetof(varattrib_4b, va_compressed.va_data)
#define VARHDRSZ_SHORT			offsetof(varattrib_1b, va_data)

#define VARATT_SHORT_MAX		0x7F
#define VARATT_CAN_MAKE_SHORT(PTR) \
	(VARATT_IS_4B_U(PTR) && \
	 (VARSIZE(PTR) - VARHDRSZ + VARHDRSZ_SHORT) <= VARATT_SHORT_MAX)
#define VARATT_CONVERTED_SHORT_SIZE(PTR) \
	(VARSIZE(PTR) - VARHDRSZ + VARHDRSZ_SHORT)

/*
 * In consumers oblivious to data alignment, call PG_DETOAST_DATUM_PACKED(),
 * VARDATA_ANY(), VARSIZE_ANY() and VARSIZE_ANY_EXHDR().  Elsewhere, call
 * PG_DETOAST_DATUM(), VARDATA() and VARSIZE().  Directly fetching an int16,
 * int32 or wider field in the struct representing the datum layout requires
 * aligned data.  memcpy() is alignment-oblivious, as are most operations on
 * datatypes, such as text, whose layout struct contains only char fields.
 *
 * Code assembling a new datum should call VARDATA() and SET_VARSIZE().
 * (Datums begin life untoasted.)
 *
 * Other macros here should usually be used only by tuple assembly/disassembly
 * code and code that specifically wants to work with still-toasted Datums.
 */
#define VARDATA(PTR)						VARDATA_4B(PTR)
#define VARSIZE(PTR)						VARSIZE_4B(PTR)

#define VARSIZE_SHORT(PTR)					VARSIZE_1B(PTR)
#define VARDATA_SHORT(PTR)					VARDATA_1B(PTR)

#define VARTAG_EXTERNAL(PTR)				VARTAG_1B_E(PTR)
#define VARSIZE_EXTERNAL(PTR)				(VARHDRSZ_EXTERNAL + VARTAG_SIZE(VARTAG_EXTERNAL(PTR)))
#define VARDATA_EXTERNAL(PTR)				VARDATA_1B_E(PTR)

#define VARATT_IS_COMPRESSED(PTR)			VARATT_IS_4B_C(PTR)
#define VARATT_IS_EXTERNAL(PTR)				VARATT_IS_1B_E(PTR)
#define VARATT_IS_EXTERNAL_ONDISK(PTR) \
	(VARATT_IS_EXTERNAL(PTR) && VARTAG_EXTERNAL(PTR) == VARTAG_ONDISK)
#define VARATT_IS_EXTERNAL_INDIRECT(PTR) \
	(VARATT_IS_EXTERNAL(PTR) && VARTAG_EXTERNAL(PTR) == VARTAG_INDIRECT)
#define VARATT_IS_EXTERNAL_EXPANDED_RO(PTR) \
	(VARATT_IS_EXTERNAL(PTR) && VARTAG_EXTERNAL(PTR) == VARTAG_EXPANDED_RO)
#define VARATT_IS_EXTERNAL_EXPANDED_RW(PTR) \
	(VARATT_IS_EXTERNAL(PTR) && VARTAG_EXTERNAL(PTR) == VARTAG_EXPANDED_RW)
#define VARATT_IS_EXTERNAL_EXPANDED(PTR) \
	(VARATT_IS_EXTERNAL(PTR) && VARTAG_IS_EXPANDED(VARTAG_EXTERNAL(PTR)))
#define VARATT_IS_EXTERNAL_NON_EXPANDED(PTR) \
	(VARATT_IS_EXTERNAL(PTR) && !VARTAG_IS_EXPANDED(VARTAG_EXTERNAL(PTR)))
#define VARATT_IS_SHORT(PTR)				VARATT_IS_1B(PTR)
#define VARATT_IS_EXTENDED(PTR)				(!VARATT_IS_4B_U(PTR))

#define SET_VARSIZE(PTR, len)				SET_VARSIZE_4B(PTR, len)
#define SET_VARSIZE_SHORT(PTR, len)			SET_VARSIZE_1B(PTR, len)
#define SET_VARSIZE_COMPRESSED(PTR, len)	SET_VARSIZE_4B_C(PTR, len)

#define SET_VARTAG_EXTERNAL(PTR, tag)		SET_VARTAG_1B_E(PTR, tag)

#define VARSIZE_ANY(PTR) \
	(VARATT_IS_1B_E(PTR) ? VARSIZE_EXTERNAL(PTR) : \
	 (VARATT_IS_1B(PTR) ? VARSIZE_1B(PTR) : \
	  VARSIZE_4B(PTR)))

/* Size of a varlena data, excluding header */
#define VARSIZE_ANY_EXHDR(PTR) \
	(VARATT_IS_1B_E(PTR) ? VARSIZE_EXTERNAL(PTR)-VARHDRSZ_EXTERNAL : \
	 (VARATT_IS_1B(PTR) ? VARSIZE_1B(PTR)-VARHDRSZ_SHORT : \
	  VARSIZE_4B(PTR)-VARHDRSZ))

/* caution: this will not work on an external or compressed-in-line Datum */
/* caution: this will return a possibly unaligned pointer */
#define VARDATA_ANY(PTR) \
	 (VARATT_IS_1B(PTR) ? VARDATA_1B(PTR) : VARDATA_4B(PTR))

/* ==== VERBATIM: array.h 75, 77-82, 84-98, 276-323 @ 62d6c7d3df ==== */
#define MAXDIM 6
/*
 * Maximum number of elements in an array.  We limit this to at most about a
 * quarter billion elements, so that it's not necessary to check for overflow
 * in quite so many places --- for instance when palloc'ing Datum arrays.
 */
#define MaxArraySize ((Size) (MaxAllocSize / sizeof(Datum)))
/*
 * Arrays are varlena objects, so must meet the varlena convention that
 * the first int32 of the object contains the total object size in bytes.
 * Be sure to use VARSIZE() and SET_VARSIZE() to access it, though!
 *
 * CAUTION: if you change the header for ordinary arrays you will also
 * need to change the headers for oidvector and int2vector!
 */
typedef struct ArrayType
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	int			ndim;			/* # of dimensions */
	int32		dataoffset;		/* offset to data, or 0 if no bitmap */
	Oid			elemtype;		/* element type OID */
} ArrayType;
/*
 * Access macros for varlena array header fields.
 *
 * ARR_DIMS returns a pointer to an array of array dimensions (number of
 * elements along the various array axes).
 *
 * ARR_LBOUND returns a pointer to an array of array lower bounds.
 *
 * That is: if the third axis of an array has elements 5 through 8, then
 * ARR_DIMS(a)[2] == 4 and ARR_LBOUND(a)[2] == 5.
 *
 * Unlike C, the default lower bound is 1.
 */
#define ARR_SIZE(a)				VARSIZE(a)
#define ARR_NDIM(a)				((a)->ndim)
#define ARR_HASNULL(a)			((a)->dataoffset != 0)
#define ARR_ELEMTYPE(a)			((a)->elemtype)

#define ARR_DIMS(a) \
		((int *) (((char *) (a)) + sizeof(ArrayType)))
#define ARR_LBOUND(a) \
		((int *) (((char *) (a)) + sizeof(ArrayType) + \
				  sizeof(int) * ARR_NDIM(a)))

#define ARR_NULLBITMAP(a) \
		(ARR_HASNULL(a) ? \
		 (bits8 *) (((char *) (a)) + sizeof(ArrayType) + \
					2 * sizeof(int) * ARR_NDIM(a)) \
		 : (bits8 *) NULL)

/*
 * The total array header size (in bytes) for an array with the specified
 * number of dimensions and total number of items.
 */
#define ARR_OVERHEAD_NONULLS(ndims) \
		MAXALIGN(sizeof(ArrayType) + 2 * sizeof(int) * (ndims))
#define ARR_OVERHEAD_WITHNULLS(ndims, nitems) \
		MAXALIGN(sizeof(ArrayType) + 2 * sizeof(int) * (ndims) + \
				 ((nitems) + 7) / 8)

#define ARR_DATA_OFFSET(a) \
		(ARR_HASNULL(a) ? (a)->dataoffset : ARR_OVERHEAD_NONULLS(ARR_NDIM(a)))

/*
 * Returns a pointer to the actual array data.
 */
#define ARR_DATA_PTR(a) \
		(((char *) (a)) + ARR_DATA_OFFSET(a))

/* ==== VERBATIM: common/int.h lines 147-202 @ 62d6c7d3df ==== */
/*
 * INT32
 */
static inline bool
pg_add_s32_overflow(int32 a, int32 b, int32 *result)
{
#if defined(HAVE__BUILTIN_OP_OVERFLOW)
	return __builtin_add_overflow(a, b, result);
#else
	int64		res = (int64) a + (int64) b;

	if (res > PG_INT32_MAX || res < PG_INT32_MIN)
	{
		*result = 0x5EED;		/* to avoid spurious warnings */
		return true;
	}
	*result = (int32) res;
	return false;
#endif
}

static inline bool
pg_sub_s32_overflow(int32 a, int32 b, int32 *result)
{
#if defined(HAVE__BUILTIN_OP_OVERFLOW)
	return __builtin_sub_overflow(a, b, result);
#else
	int64		res = (int64) a - (int64) b;

	if (res > PG_INT32_MAX || res < PG_INT32_MIN)
	{
		*result = 0x5EED;		/* to avoid spurious warnings */
		return true;
	}
	*result = (int32) res;
	return false;
#endif
}

static inline bool
pg_mul_s32_overflow(int32 a, int32 b, int32 *result)
{
#if defined(HAVE__BUILTIN_OP_OVERFLOW)
	return __builtin_mul_overflow(a, b, result);
#else
	int64		res = (int64) a * (int64) b;

	if (res > PG_INT32_MAX || res < PG_INT32_MIN)
	{
		*result = 0x5EED;		/* to avoid spurious warnings */
		return true;
	}
	*result = (int32) res;
	return false;
#endif
}


/* ==== symbol prefixing: every extern definition in this TU ==== */
#define initStringInfo			lt_initStringInfo
#define resetStringInfo			lt_resetStringInfo
#define appendStringInfo		lt_appendStringInfo
#define appendStringInfoVA		lt_appendStringInfoVA
#define appendStringInfoString	lt_appendStringInfoString
#define appendStringInfoChar	lt_appendStringInfoChar
#define appendBinaryStringInfo	lt_appendBinaryStringInfo
#define appendBinaryStringInfoNT lt_appendBinaryStringInfoNT
#define enlargeStringInfo		lt_enlargeStringInfo
#define pq_begintypsend			lt_pq_begintypsend
#define pq_endtypsend			lt_pq_endtypsend
#define pq_sendtext				lt_pq_sendtext
#define pq_getmsgint			lt_pq_getmsgint
#define pq_getmsgbytes			lt_pq_getmsgbytes
#define pq_copymsgbytes			lt_pq_copymsgbytes
#define pq_getmsgtext			lt_pq_getmsgtext
#define cstring_to_text			lt_cstring_to_text
#define cstring_to_text_with_len lt_cstring_to_text_with_len
#define text_to_cstring			lt_text_to_cstring
#define ArrayGetOffset			lt_ArrayGetOffset
#define ArrayGetNItems			lt_ArrayGetNItems
#define ArrayGetNItemsSafe		lt_ArrayGetNItemsSafe
#define ArrayCheckBounds		lt_ArrayCheckBounds
#define ArrayCheckBoundsSafe	lt_ArrayCheckBoundsSafe
#define array_contains_nulls	lt_array_contains_nulls
#define pg_ascii_tolower		lt_pg_ascii_tolower
#define t_isalnum_with_len		lt_t_isalnum_with_len
#define t_isalnum_cstr			lt_t_isalnum_cstr
#define t_isalnum_unbounded		lt_t_isalnum_unbounded
#define t_isalnum				lt_t_isalnum
#define t_isalpha_with_len		lt_t_isalpha_with_len
#define t_isalpha_cstr			lt_t_isalpha_cstr
#define t_isalpha_unbounded		lt_t_isalpha_unbounded
#define t_isalpha				lt_t_isalpha
#define pg_newlocale_from_collation lt_pg_newlocale_from_collation
#define pg_strfold				lt_pg_strfold
#define char2wchar				lt_char2wchar
#define generic_restriction_selectivity lt_generic_restriction_selectivity
#define pg_crc32_table			lt_pg_crc32_table
/* family externs (contrib/ltree) */
#define ltree_in				lt_ltree_in
#define ltree_out				lt_ltree_out
#define ltree_send				lt_ltree_send
#define ltree_recv				lt_ltree_recv
#define lquery_in				lt_lquery_in
#define lquery_out				lt_lquery_out
#define lquery_send				lt_lquery_send
#define lquery_recv				lt_lquery_recv
#define ltxtq_in				lt_ltxtq_in
#define ltxtq_out				lt_ltxtq_out
#define ltxtq_send				lt_ltxtq_send
#define ltxtq_recv				lt_ltxtq_recv
#define ltree_cmp				lt_ltree_cmp
#define ltree_lt				lt_ltree_lt
#define ltree_le				lt_ltree_le
#define ltree_eq				lt_ltree_eq
#define ltree_ne				lt_ltree_ne
#define ltree_ge				lt_ltree_ge
#define ltree_gt				lt_ltree_gt
#define hash_ltree				lt_hash_ltree
#define hash_ltree_extended		lt_hash_ltree_extended
#define nlevel					lt_nlevel
#define ltree_isparent			lt_ltree_isparent
#define ltree_risparent			lt_ltree_risparent
#define subltree				lt_subltree
#define subpath					lt_subpath
#define ltree_index				lt_ltree_index
#define ltree_addltree			lt_ltree_addltree
#define ltree_addtext			lt_ltree_addtext
#define ltree_textadd			lt_ltree_textadd
#define lca						lt_lca
#define ltree2text				lt_ltree2text
#define text2ltree				lt_text2ltree
#define ltreeparentsel			lt_ltreeparentsel
#define ltq_regex				lt_ltq_regex
#define ltq_rregex				lt_ltq_rregex
#define lt_q_regex				lt_lt_q_regex
#define lt_q_rregex				lt_lt_q_rregex
#define ltxtq_exec				lt_ltxtq_exec
#define ltxtq_rexec				lt_ltxtq_rexec
#define _ltree_isparent			lt__ltree_isparent
#define _ltree_r_isparent		lt__ltree_r_isparent
#define _ltree_risparent		lt__ltree_risparent
#define _ltree_r_risparent		lt__ltree_r_risparent
#define _ltq_regex				lt__ltq_regex
#define _ltq_rregex				lt__ltq_rregex
#define _lt_q_regex				lt__lt_q_regex
#define _lt_q_rregex			lt__lt_q_rregex
#define _ltxtq_exec				lt__ltxtq_exec
#define _ltxtq_rexec			lt__ltxtq_rexec
#define _ltree_extract_isparent	lt__ltree_extract_isparent
#define _ltree_extract_risparent lt__ltree_extract_risparent
#define _ltq_extract_regex		lt__ltq_extract_regex
#define _ltxtq_extract_exec		lt__ltxtq_extract_exec
#define _lca					lt__lca
#define ltree_compare			lt_ltree_compare
#define inner_isparent			lt_inner_isparent
#define compare_subnode			lt_compare_subnode
#define lca_inner				lt_lca_inner
#define ltree_prefix_eq			lt_ltree_prefix_eq
#define ltree_prefix_eq_ci		lt_ltree_prefix_eq_ci
#define ltree_execute			lt_ltree_execute
#define ltree_gist_alloc		lt_ltree_gist_alloc
#define ltree_crc32_sz			lt_ltree_crc32_sz

/* ==== VERBATIM: StringInfoData (stringinfo.h 46-54 @ 62d6c7d3df) ==== */
typedef struct StringInfoData
{
	char	   *data;
	int			len;
	int			maxlen;
	int			cursor;
} StringInfoData;

typedef StringInfoData *StringInfo;

/* ==== VERBATIM: appendStringInfoCharMacro (stringinfo.h @ 62d6c7d3df) ==== */
#define appendStringInfoCharMacro(str,ch) \
	(((str)->len + 1 >= (str)->maxlen) ? \
	 appendStringInfoChar(str, ch) : \
	 (void)((str)->data[(str)->len] = (ch), (str)->data[++(str)->len] = '\0'))


/* forward decls for the pasted stringinfo.c bodies */
static void initStringInfoInternal(StringInfo str, int initsize);
extern void resetStringInfo(StringInfo str);
extern void enlargeStringInfo(StringInfo str, int needed);
extern void appendStringInfoString(StringInfo str, const char *s);
extern void appendBinaryStringInfo(StringInfo str, const void *data, int datalen);
extern int	appendStringInfoVA(StringInfo str, const char *fmt, va_list args) __attribute__((format(printf, 2, 0)));

/* ==== VERBATIM: stringinfo.c bodies @ 62d6c7d3df ==== */
static inline void
initStringInfoInternal(StringInfo str, int initsize)
{
	Assert(initsize >= 1 && initsize <= MaxAllocSize);

	str->data = (char *) palloc(initsize);
	str->maxlen = initsize;
	resetStringInfo(str);
}

void
initStringInfo(StringInfo str)
{
	initStringInfoInternal(str, STRINGINFO_DEFAULT_SIZE);
}

void
resetStringInfo(StringInfo str)
{
	/* don't allow resets of read-only StringInfos */
	Assert(str->maxlen != 0);

	str->data[0] = '\0';
	str->len = 0;
	str->cursor = 0;
}

void
appendStringInfo(StringInfo str, const char *fmt,...)
{
	int			save_errno = errno;

	for (;;)
	{
		va_list		args;
		int			needed;

		/* Try to format the data. */
		errno = save_errno;
		va_start(args, fmt);
		needed = appendStringInfoVA(str, fmt, args);
		va_end(args);

		if (needed == 0)
			break;				/* success */

		/* Increase the buffer size and try again. */
		enlargeStringInfo(str, needed);
	}
}

int
appendStringInfoVA(StringInfo str, const char *fmt, va_list args)
{
	int			avail;
	size_t		nprinted;

	Assert(str != NULL);

	/*
	 * If there's hardly any space, don't bother trying, just fail to make the
	 * caller enlarge the buffer first.  We have to guess at how much to
	 * enlarge, since we're skipping the formatting work.
	 */
	avail = str->maxlen - str->len;
	if (avail < 16)
		return 32;

	nprinted = pvsnprintf(str->data + str->len, (size_t) avail, fmt, args);

	if (nprinted < (size_t) avail)
	{
		/* Success.  Note nprinted does not include trailing null. */
		str->len += (int) nprinted;
		return 0;
	}

	/* Restore the trailing null so that str is unmodified. */
	str->data[str->len] = '\0';

	/*
	 * Return pvsnprintf's estimate of the space needed.  (Although this is
	 * given as a size_t, we know it will fit in int because it's not more
	 * than MaxAllocSize.)
	 */
	return (int) nprinted;
}

void
appendStringInfoString(StringInfo str, const char *s)
{
	appendBinaryStringInfo(str, s, strlen(s));
}

void
appendStringInfoChar(StringInfo str, char ch)
{
	/* Make more room if needed */
	if (str->len + 1 >= str->maxlen)
		enlargeStringInfo(str, 1);

	/* OK, append the character */
	str->data[str->len] = ch;
	str->len++;
	str->data[str->len] = '\0';
}

void
appendBinaryStringInfo(StringInfo str, const void *data, int datalen)
{
	Assert(str != NULL);

	/* Make more room if needed */
	enlargeStringInfo(str, datalen);

	/* OK, append the data */
	memcpy(str->data + str->len, data, datalen);
	str->len += datalen;

	/*
	 * Keep a trailing null in place, even though it's probably useless for
	 * binary data.  (Some callers are dealing with text but call this because
	 * their input isn't null-terminated.)
	 */
	str->data[str->len] = '\0';
}

void
appendBinaryStringInfoNT(StringInfo str, const void *data, int datalen)
{
	Assert(str != NULL);

	/* Make more room if needed */
	enlargeStringInfo(str, datalen);

	/* OK, append the data */
	memcpy(str->data + str->len, data, datalen);
	str->len += datalen;
}

void
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

/* ---- mbutils shims: UTF8 pinned, verify via the verbatim wfam_ copies ---- */
extern bool wfam_pg_verify_mbstr(int encoding, const char *mbstr, int len, bool noError);
#define LT_PG_UTF8 6			/* pg_wchar.h enum pg_enc PG_UTF8 */

static char *
lt_client_to_server(const char *s, int len)
{
	/* pg_any_to_server, same-encoding arm (mbutils.c): verify, return s */
	if (!wfam_pg_verify_mbstr(LT_PG_UTF8, s, len, true))
	{
		/* report_invalid_encoding's sqlstate (mbutils.c) */
		lt_sqlstate = ERRCODE_CHARACTER_NOT_IN_REPERTOIRE;
		lt_raise();
	}
	return (char *) s;
}

#define pg_client_to_server(s, len) lt_client_to_server((s), (len))
#define pg_server_to_client(s, len) ((char *) (s))

/* pg_bswap.h on little-endian targets */
#define pg_hton32(x) __builtin_bswap32(x)
#define pg_ntoh32(x) __builtin_bswap32(x)
#define pg_ntoh16(x) __builtin_bswap16(x)
/* ==== VERBATIM: pqformat.h pq_writeint8 (45-54) + pq_sendint8 (126-132) @ 62d6c7d3df ==== */
static inline void
pq_writeint8(StringInfoData *pg_restrict buf, uint8 i)
{
	uint8		ni = i;

	Assert(buf->len + (int) sizeof(uint8) <= buf->maxlen);
	memcpy((char *pg_restrict) (buf->data + buf->len), &ni, sizeof(uint8));
	buf->len += sizeof(uint8);
}

/* append a binary [u]int8 to a StringInfo buffer */
static inline void
pq_sendint8(StringInfo buf, uint8 i)
{
	enlargeStringInfo(buf, sizeof(uint8));
	pq_writeint8(buf, i);
}

/* forward decls for the pasted bodies */
extern void pq_copymsgbytes(StringInfo msg, void *buf, int datalen);
extern text *cstring_to_text_with_len(const char *s, int len);
extern int	ArrayGetNItemsSafe(int ndim, const int *dims, struct Node *escontext);
extern bool ArrayCheckBoundsSafe(int ndim, const int *dims, const int *lb, struct Node *escontext);
typedef struct varlena bytea_fwd_unused;

/* ==== VERBATIM: pqformat.c bodies @ 62d6c7d3df ==== */

void
pq_begintypsend(StringInfo buf)
{
	initStringInfo(buf);
	/* Reserve four bytes for the bytea length word */
	appendStringInfoCharMacro(buf, '\0');
	appendStringInfoCharMacro(buf, '\0');
	appendStringInfoCharMacro(buf, '\0');
	appendStringInfoCharMacro(buf, '\0');
}

bytea *
pq_endtypsend(StringInfo buf)
{
	bytea	   *result = (bytea *) buf->data;

	/* Insert correct length into bytea length word */
	Assert(buf->len >= VARHDRSZ);
	SET_VARSIZE(result, buf->len);

	return result;
}

void
pq_sendtext(StringInfo buf, const char *str, int slen)
{
	char	   *p;

	p = pg_server_to_client(str, slen);
	if (p != str)				/* actual conversion has been done? */
	{
		slen = strlen(p);
		appendBinaryStringInfo(buf, p, slen);
		pfree(p);
	}
	else
		appendBinaryStringInfo(buf, str, slen);
}

unsigned int
pq_getmsgint(StringInfo msg, int b)
{
	unsigned int result;
	unsigned char n8;
	uint16		n16;
	uint32		n32;

	switch (b)
	{
		case 1:
			pq_copymsgbytes(msg, &n8, 1);
			result = n8;
			break;
		case 2:
			pq_copymsgbytes(msg, &n16, 2);
			result = pg_ntoh16(n16);
			break;
		case 4:
			pq_copymsgbytes(msg, &n32, 4);
			result = pg_ntoh32(n32);
			break;
		default:
			elog(ERROR, "unsupported integer size %d", b);
			result = 0;			/* keep compiler quiet */
			break;
	}
	return result;
}

const char *
pq_getmsgbytes(StringInfo msg, int datalen)
{
	const char *result;

	if (datalen < 0 || datalen > (msg->len - msg->cursor))
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("insufficient data left in message")));
	result = &msg->data[msg->cursor];
	msg->cursor += datalen;
	return result;
}

void
pq_copymsgbytes(StringInfo msg, void *buf, int datalen)
{
	if (datalen < 0 || datalen > (msg->len - msg->cursor))
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("insufficient data left in message")));
	memcpy(buf, &msg->data[msg->cursor], datalen);
	msg->cursor += datalen;
}

char *
pq_getmsgtext(StringInfo msg, int rawbytes, int *nbytes)
{
	char	   *str;
	char	   *p;

	if (rawbytes < 0 || rawbytes > (msg->len - msg->cursor))
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("insufficient data left in message")));
	str = &msg->data[msg->cursor];
	msg->cursor += rawbytes;

	p = pg_client_to_server(str, rawbytes);
	if (p != str)				/* actual conversion has been done? */
		*nbytes = strlen(p);
	else
	{
		p = (char *) palloc(rawbytes + 1);
		memcpy(p, str, rawbytes);
		p[rawbytes] = '\0';
		*nbytes = rawbytes;
	}
	return p;
}

/* ==== VERBATIM: varlena.c cstring_to_text(_with_len) @ 62d6c7d3df ==== */
text *
cstring_to_text(const char *s)
{
	return cstring_to_text_with_len(s, strlen(s));
}

text *
cstring_to_text_with_len(const char *s, int len)
{
	text	   *result = (text *) palloc(len + VARHDRSZ);

	SET_VARSIZE(result, len + VARHDRSZ);
	memcpy(VARDATA(result), s, len);

	return result;
}


/* SHIM: identity detoast for text_to_cstring (driver precondition: plain
 * 4B-header images) + c.h unconstify */
#define pg_detoast_datum_packed(p) (p)
#define unconstify(underlying_type, expr) ((underlying_type) (expr))
/* ==== VERBATIM: varlena.c text_to_cstring (216-243 @ 62d6c7d3df) ==== */

char *
text_to_cstring(const text *t)
{
	/* must cast away the const, unfortunately */
	text	   *tunpacked = pg_detoast_datum_packed(unconstify(text *, t));
	int			len = VARSIZE_ANY_EXHDR(tunpacked);
	char	   *result;

	result = (char *) palloc(len + 1);
	memcpy(result, VARDATA_ANY(tunpacked), len);
	result[len] = '\0';

	if (tunpacked != t)
		pfree(tunpacked);

	return result;
}

/*
/* ==== VERBATIM: arrayutils.c bodies @ 62d6c7d3df ==== */

int
ArrayGetOffset(int n, const int *dim, const int *lb, const int *indx)
{
	int			i,
				scale = 1,
				offset = 0;

	for (i = n - 1; i >= 0; i--)
	{
		offset += (indx[i] - lb[i]) * scale;
		scale *= dim[i];
	}
	return offset;
}

int
ArrayGetNItems(int ndim, const int *dims)
{
	return ArrayGetNItemsSafe(ndim, dims, NULL);
}

int
ArrayGetNItemsSafe(int ndim, const int *dims, struct Node *escontext)
{
	int32		ret;
	int			i;

	if (ndim <= 0)
		return 0;
	ret = 1;
	for (i = 0; i < ndim; i++)
	{
		int64		prod;

		/* A negative dimension implies that UB-LB overflowed ... */
		if (dims[i] < 0)
			ereturn(escontext, -1,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("array size exceeds the maximum allowed (%d)",
							(int) MaxArraySize)));

		prod = (int64) ret * (int64) dims[i];

		ret = (int32) prod;
		if ((int64) ret != prod)
			ereturn(escontext, -1,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("array size exceeds the maximum allowed (%d)",
							(int) MaxArraySize)));
	}
	Assert(ret >= 0);
	if ((Size) ret > MaxArraySize)
		ereturn(escontext, -1,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("array size exceeds the maximum allowed (%d)",
						(int) MaxArraySize)));
	return (int) ret;
}

void
ArrayCheckBounds(int ndim, const int *dims, const int *lb)
{
	(void) ArrayCheckBoundsSafe(ndim, dims, lb, NULL);
}

bool
ArrayCheckBoundsSafe(int ndim, const int *dims, const int *lb,
					 struct Node *escontext)
{
	int			i;

	for (i = 0; i < ndim; i++)
	{
		/* PG_USED_FOR_ASSERTS_ONLY prevents variable-isn't-read warnings */
		int32		sum PG_USED_FOR_ASSERTS_ONLY;

		if (pg_add_s32_overflow(dims[i], lb[i], &sum))
			ereturn(escontext, false,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("array lower bound is too large: %d",
							lb[i])));
	}

	return true;
}

/* ==== VERBATIM: arrayfuncs.c array_contains_nulls (3766-3812 @ 62d6c7d3df) ==== */
/*
 * array_contains_nulls --- detect whether an array has any null elements
 *
 * This gives an accurate answer, whereas testing ARR_HASNULL only tells
 * if the array *might* contain a null.
 */
bool
array_contains_nulls(ArrayType *array)
{
	int			nelems;
	bits8	   *bitmap;
	int			bitmask;

	/* Easy answer if there's no null bitmap */
	if (!ARR_HASNULL(array))
		return false;

	nelems = ArrayGetNItems(ARR_NDIM(array), ARR_DIMS(array));

	bitmap = ARR_NULLBITMAP(array);

	/* check whole bytes of the bitmap byte-at-a-time */
	while (nelems >= 8)
	{
		if (*bitmap != 0xFF)
			return true;
		bitmap++;
		nelems -= 8;
	}

	/* check last partial byte */
	bitmask = 1;
	while (nelems > 0)
	{
		if ((*bitmap & bitmask) == 0)
			return true;
		bitmask <<= 1;
		nelems--;
	}

	return false;
}


/*
 * array_eq :
 *		  compares two arrays for equality
/* ==== VERBATIM: pgstrcasecmp.c pg_ascii_tolower (142-151 @ 62d6c7d3df) ==== */
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

/* ==== VERBATIM: ts_locale.h TOUCHAR + t_iseq (35-38 @ 62d6c7d3df) ==== */
#define TOUCHAR(x)	(*((const unsigned char *) (x)))

/* The second argument of t_iseq() must be a plain ASCII character */
#define t_iseq(x,c)		(TOUCHAR(x) == (unsigned char) (c))

/* SHIM: locale pin (see header). pg_locale_t reduced to the one field the
 * vendored bodies read; the returned locale is the C-ctype pin. */
typedef struct pg_locale_struct { bool ctype_is_c; } *pg_locale_t;
#define DEFAULT_COLLATION_OID 100
static struct pg_locale_struct lt_c_locale = { true };
static pg_locale_t pg_newlocale_from_collation(Oid collid) { (void) collid; return &lt_c_locale; }
/* UNICODE_CASEMAP_BUFSZ = UNICODE_CASEMAP_LEN(3) * MAX_MULTIBYTE_CHAR_LEN(4)
 * (pg_locale.h 38-39 + pg_wchar.h 33 @ 62d6c7d3df) */
#define UNICODE_CASEMAP_BUFSZ	(3 * 4)
/* SHIM: unreachable under the C-ctype pin (header); abort-loud, never a
 * fabricated fold. */
static size_t pg_strfold(char *dst, size_t dstsize, const char *src, ssize_t srclen, pg_locale_t locale)
{ (void) dst; (void) dstsize; (void) src; (void) srclen; (void) locale; abort(); }
static void char2wchar(wchar_t *to, size_t tolen, const char *from, size_t fromlen, pg_locale_t locale)
{ (void) to; (void) tolen; (void) from; (void) fromlen; (void) locale; abort(); }

/* SHIM: database ctype pin */
static const bool database_ctype_is_c = true;

/* The BOUNDS-CHECKING mblen wrappers are VENDORED HERE rather than resolved
 * against csrc/pg_wcharfam.c — the wparserfam precedent, and here it is
 * load-bearing: wcharfam's copies raise through wcharfam's OWN error channel,
 * so an invalid byte sequence longjmp'd to a jmp_buf this TU never armed and
 * SEGV'd the worker. Only the pure, non-erroring table lookup
 * (pg_wchar_table[...].mblen) is resolved against wcharfam's verbatim
 * wfam_pg_mblen_unbounded, whose encoding cell the driver pins to UTF8 via
 * wfam_x_set_db_encoding — so both sides walk the same verbatim UTF8 mblen.
 * report_invalid_encoding_db routes to THIS TU's ereport (22021, the errcode
 * report_invalid_encoding_int raises). */
extern int	wfam_pg_mblen_unbounded(const char *mbstr);
#define pg_wchar_table_mblen_db(p) wfam_pg_mblen_unbounded((const char *) (p))
#define VALGRIND_CHECK_MEM_IS_DEFINED(p, n) ((void) 0)

static void
report_invalid_encoding_db(const char *mbstr, int mblen, int len)
{
	(void) mbstr; (void) mblen; (void) len;
	ereport(ERROR,
			(errcode(ERRCODE_CHARACTER_NOT_IN_REPERTOIRE),
			 errmsg("invalid byte sequence")));
}

/* ==== VERBATIM: mbutils.c pg_mblen_cstr (1042-1073), pg_mblen_range
 * (1081-1098), pg_mblen_with_len (1104-1122), pg_mblen_unbounded
 * (1134-1141) @ 62d6c7d3df; the single shimmed token in each is the
 * pg_wchar_table[DatabaseEncoding->encoding].mblen(...) lookup above ==== */
static int
pg_mblen_cstr(const char *mbstr)
{
	int			length = pg_wchar_table_mblen_db(mbstr);

	for (int i = 1; i < length; ++i)
		if (unlikely(mbstr[i] == 0))
			report_invalid_encoding_db(mbstr, length, i);

	if (mbstr[0] != '\0')
	{
		VALGRIND_CHECK_MEM_IS_DEFINED(mbstr + length, 1);
	}

	return length;
}

static int
pg_mblen_range(const char *mbstr, const char *end)
{
	int			length = pg_wchar_table_mblen_db(mbstr);

	Assert(end > mbstr);

	if (unlikely(mbstr + length > end))
		report_invalid_encoding_db(mbstr, length, end - mbstr);

	VALGRIND_CHECK_MEM_IS_DEFINED(mbstr, length);

	return length;
}

static int
pg_mblen_with_len(const char *mbstr, int limit)
{
	int			length = pg_wchar_table_mblen_db(mbstr);

	Assert(limit >= 1);

	if (unlikely(length > limit))
		report_invalid_encoding_db(mbstr, length, limit);

	VALGRIND_CHECK_MEM_IS_DEFINED(mbstr, length);

	return length;
}

static int
pg_mblen_unbounded(const char *mbstr)
{
	int			length = pg_wchar_table_mblen_db(mbstr);

	VALGRIND_CHECK_MEM_IS_DEFINED(mbstr, length);

	return length;
}

#include <wchar.h>
#include <wctype.h>
/* ==== VERBATIM: ts_locale.c WC_BUF_LEN + GENERATE_T_ISCLASS_DEF + alnum
 * instantiation (32-70 @ 62d6c7d3df; alpha instantiation dropped: unused
 * by ltree, would be a dead extern) ==== */

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

/* ==== VERBATIM: pg_crc.h 37-70 @ 62d6c7d3df ==== */
typedef uint32 pg_crc32;

/*
 * CRC-32, the same used e.g. in Ethernet.
 *
 * This is currently only used in ltree and hstore contrib modules. It uses
 * the same lookup table as the legacy algorithm below. New code should
 * use the Castagnoli version instead.
 */
#define INIT_TRADITIONAL_CRC32(crc) ((crc) = 0xFFFFFFFF)
#define FIN_TRADITIONAL_CRC32(crc)	((crc) ^= 0xFFFFFFFF)
#define COMP_TRADITIONAL_CRC32(crc, data, len)	\
	COMP_CRC32_NORMAL_TABLE(crc, data, len, pg_crc32_table)
#define EQ_TRADITIONAL_CRC32(c1, c2) ((c1) == (c2))

/* Sarwate's algorithm, for use with a "normal" lookup table */
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
/* ==== VERBATIM: backend/utils/hash/pg_crc.c pg_crc32_table @ 62d6c7d3df ==== */
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

/* SHIM: byte-based check_stack_depth (miscadmin.h semantics; see header) */
static _Thread_local char *lt_stack_base;
/* C stack_depth.c: max_stack_depth_bytes = max_stack_depth * 1024 exactly
 * (STACK_DEPTH_SLOP only bounds the GUC against RLIMIT_STACK, it is NOT
 * subtracted from the live limit). PG's boot logic caps max_stack_depth at
 * 2048kB, which is what the Rust worker arms via assign_max_stack_depth. */
static _Thread_local long lt_max_stack_depth_bytes = 2048L * 1024L;
static bool
lt_stack_is_too_deep(void)
{
	char		stack_top_loc;
	long		stack_depth;

	if (lt_stack_base == NULL)
		return false;
	stack_depth = (long) (lt_stack_base - &stack_top_loc);
	if (stack_depth < 0)
		stack_depth = -stack_depth;
	return stack_depth > lt_max_stack_depth_bytes;
}
#define check_stack_depth() \
	do { \
		if (lt_stack_is_too_deep()) \
			ereport(ERROR, \
					(errcode(ERRCODE_STATEMENT_TOO_COMPLEX), \
					 errmsg("stack depth limit exceeded"))); \
	} while (0)

/* SHIM: misc environment */
#define PG_UINT16_MAX	(0xFFFF)
#define PG_MODULE_MAGIC_EXT(...) extern int lt_pg_module_magic_dummy
#define PG_VERSION "18.3"
#define PGDLLEXPORT_ALREADY_DEFINED 1

/* hash_any -> the verbatim hashfn.c copies exported by pg_mac_io.c */
extern uint32 pg_hash_bytes(const unsigned char *k, int keylen);
extern uint64 pg_hash_bytes_extended(const unsigned char *k, int keylen, uint64 seed);
#define hash_any(k, l) UInt32GetDatum(pg_hash_bytes((k), (l)))
#define hash_any_extended(k, l, s) UInt64GetDatum(pg_hash_bytes_extended((k), (l), (s)))

/* SHIM: fmgr (hstorefam precedent, widened to nargs<=8 for lca) */
typedef struct NullableDatum
{
	Datum		value;
	bool		isnull;
} NullableDatum;

typedef struct FunctionCallInfoBaseData
{
	void	   *context;
	bool		isnull;
	short		nargs;
	NullableDatum args[8];
} FunctionCallInfoBaseData;

typedef FunctionCallInfoBaseData *FunctionCallInfo;

#define PG_FUNCTION_ARGS FunctionCallInfo fcinfo
#define PG_FUNCTION_INFO_V1(f) extern Datum f(FunctionCallInfo fcinfo)
#define PG_GETARG_DATUM(n)	 (fcinfo->args[n].value)
#define PG_ARGISNULL(n)		 (fcinfo->args[n].isnull)
#define PG_GETARG_CSTRING(n) DatumGetCString(PG_GETARG_DATUM(n))
#define PG_GETARG_POINTER(n) ((void *) DatumGetPointer(PG_GETARG_DATUM(n)))
#define PG_GETARG_INT32(n)	 DatumGetInt32(PG_GETARG_DATUM(n))
#define PG_GETARG_INT64(n)	 DatumGetInt64(PG_GETARG_DATUM(n))
#define PG_GETARG_OID(n)	 DatumGetObjectId(PG_GETARG_DATUM(n))
#define PG_GETARG_TEXT_PP(n) ((text *) PG_DETOAST_DATUM_PACKED(PG_GETARG_DATUM(n)))
#define PG_GETARG_ARRAYTYPE_P(n) ((ArrayType *) PG_DETOAST_DATUM(PG_GETARG_DATUM(n)))
#define PG_RETURN_DATUM(x)	 return (x)
#define PG_RETURN_POINTER(x) return PointerGetDatum(x)
#define PG_RETURN_CSTRING(x) return CStringGetDatum(x)
#define PG_RETURN_BOOL(x)	 return BoolGetDatum(x)
#define PG_RETURN_INT32(x)	 return Int32GetDatum(x)
#define PG_RETURN_UINT32(x)	 return UInt32GetDatum(x)
#define PG_RETURN_UINT64(x)	 return UInt64GetDatum(x)
#define PG_RETURN_FLOAT8(x)	 return lt_float8_get_datum(x)
#define PG_RETURN_TEXT_P(x)	 return PointerGetDatum(x)
#define PG_RETURN_BYTEA_P(x) return PointerGetDatum(x)
#define PG_RETURN_NULL() \
	do { fcinfo->isnull = true; return (Datum) 0; } while (0)
#define PG_FREE_IF_COPY(ptr, n) ((void) 0)

static Datum lt_float8_get_datum(float8 f) { Datum d; memcpy(&d, &f, sizeof(d)); return d; }

typedef Datum (*lt_pgfunc) (FunctionCallInfo fcinfo);

static Datum
lt_direct_call1(lt_pgfunc func, Datum a)
{
	FunctionCallInfoBaseData fc;

	fc.context = NULL;
	fc.isnull = false;
	fc.nargs = 1;
	fc.args[0].value = a;
	fc.args[0].isnull = false;
	return func(&fc);
}

static Datum
lt_direct_call2(lt_pgfunc func, Datum a, Datum b)
{
	FunctionCallInfoBaseData fc;

	fc.context = NULL;
	fc.isnull = false;
	fc.nargs = 2;
	fc.args[0].value = a;
	fc.args[0].isnull = false;
	fc.args[1].value = b;
	fc.args[1].isnull = false;
	return func(&fc);
}

#define DirectFunctionCall1(func, a) lt_direct_call1((func), (a))
#define DirectFunctionCall2(func, a, b) lt_direct_call2((func), (a), (b))

/* SHIM: ltreeparentsel environment — OUT of scope (planner), abort-loud */
typedef struct PlannerInfo PlannerInfo;
typedef struct List List;
static double generic_restriction_selectivity(PlannerInfo *root, Oid oproid, Oid collid, List *args, int varRelid, double default_selectivity)
{ (void) root; (void) oproid; (void) collid; (void) args; (void) varRelid; (void) default_selectivity; abort(); }


/* SHIM: witness that the C escontext branch actually executed (vacuity
 * rule). Redefine errsave/ereturn over the B2 definitions so every soft
 * fire increments lt_soft_fires; hard raises are unaffected. */
#undef errsave
#undef ereturn
#define errsave(escontext, ...) \
	do { (void) (__VA_ARGS__); \
		 if ((escontext) != NULL) lt_soft_fires++; \
		 lt_errsave_fire(escontext); } while (0)
#define ereturn(escontext, dummy_value, ...) \
	do { errsave(escontext, __VA_ARGS__); return dummy_value; } while (0)


/* ==== ORACLE PROPER: the banked verbatim contrib/ltree family ==== */
#include "ltreefam/crc32.c"
#include "ltreefam/ltree_io.c"
#include "ltreefam/ltree_op.c"
#include "ltreefam/lquery_op.c"
#undef NEXTVAL					/* lquery_op.c and _ltree_op.c both define it */
#include "ltreefam/ltxtquery_io.c"
#include "ltreefam/ltxtquery_op.c"
#include "ltreefam/_ltree_op.c"


/* =====================================================================
 * SECTION D: driver entries (pg_lt_* prefix) — fuzz plumbing, NOT PG code.
 * Convention: return 0 = value produced; -1 = hard error (sqlstate in
 * pg_lt_sqlstate()); 1 = soft error captured in the escontext (sqlstate in
 * pg_lt_soft_sqlstate()). setjmp re-entry per call.
 * ===================================================================== */

void
pg_lt_reset(void)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	lt_arena_reset();
	lt_sqlstate = 0;
	lt_soft_sqlstate = 0;
	lt_soft_fires = 0;
	if (lt_stack_base == NULL)
	{
		/* arm once per thread, near the dispatcher frame (headroom note in
		 * the file header; Rust arms its base at worker init likewise) */
		char		probe;

		lt_stack_base = &probe;
	}
}

int pg_lt_sqlstate(void)
{ PG_ORACLE_GUARD_CHECK(__func__); return lt_sqlstate; }
int pg_lt_soft_sqlstate(void)
{ PG_ORACLE_GUARD_CHECK(__func__); return lt_soft_sqlstate; }
int pg_lt_soft_fired(void)
{ PG_ORACLE_GUARD_CHECK(__func__); return lt_soft_fires; }

/*
 * Every driver entry opens with the release-effective holder check (task
 * #125): this TU's arena list, setjmp channel and soft-error counters are
 * process-global statics, so a call from a thread that does not hold
 * `oracle_serial()` is a wild-write generator. See pg_oracle_guard.h.
 */
#define LT_ENTER() \
	do { \
		PG_ORACLE_GUARD_CHECK(__func__); \
		if (setjmp(lt_env)) return -1; \
	} while (0)

/* which: 0 ltree_in, 1 lquery_in, 2 ltxtq_in */
int
pg_lt_in(int which, const char *str, int soft, const unsigned char **img, int *len)
{
	Node		node = {0};
	FunctionCallInfoBaseData fc;
	Datum		d;

	LT_ENTER();
	fc.context = soft ? &node : NULL;
	fc.isnull = false;
	fc.nargs = 1;
	fc.args[0].value = CStringGetDatum(str);
	fc.args[0].isnull = false;
	d = (which == 0) ? lt_ltree_in(&fc)
		: (which == 1) ? lt_lquery_in(&fc)
		: lt_ltxtq_in(&fc);
	if (soft && node.error_occurred)
		return 1;
	*img = (const unsigned char *) DatumGetPointer(d);
	*len = VARSIZE(DatumGetPointer(d));
	return 0;
}

int
pg_lt_out(int which, const unsigned char *in, const char **out)
{
	FunctionCallInfoBaseData fc;
	Datum		d;

	LT_ENTER();
	fc.context = NULL;
	fc.isnull = false;
	fc.nargs = 1;
	fc.args[0].value = PointerGetDatum(in);
	fc.args[0].isnull = false;
	d = (which == 0) ? lt_ltree_out(&fc)
		: (which == 1) ? lt_lquery_out(&fc)
		: lt_ltxtq_out(&fc);
	*out = DatumGetCString(d);
	return 0;
}

int
pg_lt_send(int which, const unsigned char *in, const unsigned char **out, int *outlen)
{
	FunctionCallInfoBaseData fc;
	Datum		d;

	LT_ENTER();
	fc.context = NULL;
	fc.isnull = false;
	fc.nargs = 1;
	fc.args[0].value = PointerGetDatum(in);
	fc.args[0].isnull = false;
	d = (which == 0) ? lt_ltree_send(&fc)
		: (which == 1) ? lt_lquery_send(&fc)
		: lt_ltxtq_send(&fc);
	*out = (const unsigned char *) DatumGetPointer(d);
	*outlen = VARSIZE(DatumGetPointer(d));
	return 0;
}

int
pg_lt_recv(int which, const unsigned char *wire, int wirelen, const unsigned char **img, int *len)
{
	StringInfoData buf;
	FunctionCallInfoBaseData fc;
	Datum		d;

	LT_ENTER();
	buf.data = (char *) palloc(wirelen + 1);
	memcpy(buf.data, wire, wirelen);
	buf.data[wirelen] = '\0';
	buf.len = wirelen;
	buf.maxlen = wirelen + 1;
	buf.cursor = 0;
	fc.context = NULL;
	fc.isnull = false;
	fc.nargs = 1;
	fc.args[0].value = PointerGetDatum(&buf);
	fc.args[0].isnull = false;
	d = (which == 0) ? lt_ltree_recv(&fc)
		: (which == 1) ? lt_lquery_recv(&fc)
		: lt_ltxtq_recv(&fc);
	*img = (const unsigned char *) DatumGetPointer(d);
	*len = VARSIZE(DatumGetPointer(d));
	return 0;
}

/* cmp + the six boolean comparators, one guarded call */
int
pg_lt_cmp(const unsigned char *a, const unsigned char *b, int32 *cmp, unsigned char *bools)
{
	LT_ENTER();
	*cmp = DatumGetInt32(lt_direct_call2(lt_ltree_cmp, PointerGetDatum(a), PointerGetDatum(b)));
	*bools = (unsigned char)
		((DatumGetBool(lt_direct_call2(lt_ltree_lt, PointerGetDatum(a), PointerGetDatum(b))) << 0)
		 | (DatumGetBool(lt_direct_call2(lt_ltree_le, PointerGetDatum(a), PointerGetDatum(b))) << 1)
		 | (DatumGetBool(lt_direct_call2(lt_ltree_eq, PointerGetDatum(a), PointerGetDatum(b))) << 2)
		 | (DatumGetBool(lt_direct_call2(lt_ltree_ne, PointerGetDatum(a), PointerGetDatum(b))) << 3)
		 | (DatumGetBool(lt_direct_call2(lt_ltree_ge, PointerGetDatum(a), PointerGetDatum(b))) << 4)
		 | (DatumGetBool(lt_direct_call2(lt_ltree_gt, PointerGetDatum(a), PointerGetDatum(b))) << 5));
	return 0;
}

/* rev=0: ltree_isparent(a,b); rev=1: ltree_risparent(a,b) */
int
pg_lt_isparent(int rev, const unsigned char *a, const unsigned char *b, int *out)
{
	LT_ENTER();
	*out = DatumGetBool(lt_direct_call2(rev ? lt_ltree_risparent : lt_ltree_isparent,
										PointerGetDatum(a), PointerGetDatum(b)));
	return 0;
}

int
pg_lt_hash(const unsigned char *a, uint32 *out)
{
	LT_ENTER();
	*out = DatumGetUInt32(lt_direct_call1(lt_hash_ltree, PointerGetDatum(a)));
	return 0;
}

int
pg_lt_hash_ext(const unsigned char *a, uint64 seed, uint64 *out)
{
	LT_ENTER();
	*out = DatumGetUInt64(lt_direct_call2(lt_hash_ltree_extended, PointerGetDatum(a), UInt64GetDatum(seed)));
	return 0;
}

int
pg_lt_nlevel(const unsigned char *a, int32 *out)
{
	LT_ENTER();
	*out = DatumGetInt32(lt_direct_call1(lt_nlevel, PointerGetDatum(a)));
	return 0;
}

int
pg_lt_addltree(const unsigned char *a, const unsigned char *b, const unsigned char **img, int *len)
{
	Datum		d;

	LT_ENTER();
	d = lt_direct_call2(lt_ltree_addltree, PointerGetDatum(a), PointerGetDatum(b));
	*img = (const unsigned char *) DatumGetPointer(d);
	*len = VARSIZE(DatumGetPointer(d));
	return 0;
}

/* which: 0 ltree_addtext(ltree, text), 1 ltree_textadd(text, ltree) */
int
pg_lt_addtext(int which, const unsigned char *a, const unsigned char *txt, int txtlen, const unsigned char **img, int *len)
{
	text	   *t;
	Datum		d;

	LT_ENTER();
	t = lt_cstring_to_text_with_len((const char *) txt, txtlen);
	d = which == 0
		? lt_direct_call2(lt_ltree_addtext, PointerGetDatum(a), PointerGetDatum(t))
		: lt_direct_call2(lt_ltree_textadd, PointerGetDatum(t), PointerGetDatum(a));
	*img = (const unsigned char *) DatumGetPointer(d);
	*len = VARSIZE(DatumGetPointer(d));
	return 0;
}

int
pg_lt_text2ltree(const unsigned char *txt, int txtlen, const unsigned char **img, int *len)
{
	text	   *t;
	Datum		d;

	LT_ENTER();
	t = lt_cstring_to_text_with_len((const char *) txt, txtlen);
	d = lt_direct_call1(lt_text2ltree, PointerGetDatum(t));
	*img = (const unsigned char *) DatumGetPointer(d);
	*len = VARSIZE(DatumGetPointer(d));
	return 0;
}

int
pg_lt_ltree2text(const unsigned char *a, const unsigned char **payload, int *len)
{
	Datum		d;
	text	   *t;

	LT_ENTER();
	d = lt_direct_call1(lt_ltree2text, PointerGetDatum(a));
	t = (text *) DatumGetPointer(d);
	*payload = (const unsigned char *) VARDATA(t);
	*len = VARSIZE(t) - VARHDRSZ;
	return 0;
}

int
pg_lt_subltree(const unsigned char *a, int32 s, int32 e, const unsigned char **img, int *len)
{
	FunctionCallInfoBaseData fc;
	Datum		d;

	LT_ENTER();
	fc.context = NULL;
	fc.isnull = false;
	fc.nargs = 3;
	fc.args[0].value = PointerGetDatum(a);
	fc.args[0].isnull = false;
	fc.args[1].value = Int32GetDatum(s);
	fc.args[1].isnull = false;
	fc.args[2].value = Int32GetDatum(e);
	fc.args[2].isnull = false;
	d = lt_subltree(&fc);
	*img = (const unsigned char *) DatumGetPointer(d);
	*len = VARSIZE(DatumGetPointer(d));
	return 0;
}

int
pg_lt_subpath(const unsigned char *a, int32 s, int32 l, int nargs, const unsigned char **img, int *len)
{
	FunctionCallInfoBaseData fc;
	Datum		d;

	LT_ENTER();
	fc.context = NULL;
	fc.isnull = false;
	fc.nargs = nargs;
	fc.args[0].value = PointerGetDatum(a);
	fc.args[0].isnull = false;
	fc.args[1].value = Int32GetDatum(s);
	fc.args[1].isnull = false;
	fc.args[2].value = Int32GetDatum(l);
	fc.args[2].isnull = false;
	d = lt_subpath(&fc);
	*img = (const unsigned char *) DatumGetPointer(d);
	*len = VARSIZE(DatumGetPointer(d));
	return 0;
}

int
pg_lt_index(const unsigned char *a, const unsigned char *b, int32 start, int nargs, int32 *out)
{
	FunctionCallInfoBaseData fc;

	LT_ENTER();
	fc.context = NULL;
	fc.isnull = false;
	fc.nargs = nargs;
	fc.args[0].value = PointerGetDatum(a);
	fc.args[0].isnull = false;
	fc.args[1].value = PointerGetDatum(b);
	fc.args[1].isnull = false;
	fc.args[2].value = Int32GetDatum(start);
	fc.args[2].isnull = false;
	*out = DatumGetInt32(lt_ltree_index(&fc));
	return 0;
}

int
pg_lt_lca2(const unsigned char *a, const unsigned char *b, const unsigned char **img, int *len, int *isnull)
{
	FunctionCallInfoBaseData fc;
	Datum		d;

	LT_ENTER();
	fc.context = NULL;
	fc.isnull = false;
	fc.nargs = 2;
	fc.args[0].value = PointerGetDatum(a);
	fc.args[0].isnull = false;
	fc.args[1].value = PointerGetDatum(b);
	fc.args[1].isnull = false;
	d = lt_lca(&fc);
	*isnull = fc.isnull;
	if (!fc.isnull)
	{
		*img = (const unsigned char *) DatumGetPointer(d);
		*len = VARSIZE(DatumGetPointer(d));
	}
	return 0;
}

/* which: 0 ltq_regex(tree,q), 1 ltq_rregex(q,tree), 2 ltxtq_exec(tree,tq),
 * 3 ltxtq_rexec(tq,tree) */
int
pg_lt_match(int which, const unsigned char *l, const unsigned char *r, int *out)
{
	Datum		d;

	LT_ENTER();
	switch (which)
	{
		case 0: d = lt_direct_call2(lt_ltq_regex, PointerGetDatum(l), PointerGetDatum(r)); break;
		case 1: d = lt_direct_call2(lt_ltq_rregex, PointerGetDatum(l), PointerGetDatum(r)); break;
		case 2: d = lt_direct_call2(lt_ltxtq_exec, PointerGetDatum(l), PointerGetDatum(r)); break;
		default: d = lt_direct_call2(lt_ltxtq_rexec, PointerGetDatum(l), PointerGetDatum(r)); break;
	}
	*out = DatumGetBool(d);
	return 0;
}

/* Array-taking entries. which:
 *  0 _ltree_isparent(arr,lt)   1 _ltree_risparent(arr,lt)
 *  2 _ltq_regex(arr,lq)        3 _ltxtq_exec(arr,tq)
 *  4 lt_q_regex(lt,lqarr)      5 _lt_q_regex(arr,lqarr)
 *  6 _ltree_extract_isparent   7 _ltree_extract_risparent
 *  8 _ltq_extract_regex        9 _ltxtq_extract_exec
 * 10 _lca(arr)
 * 11 _ltree_r_isparent(lt,arr) 12 _ltree_r_risparent(lt,arr)
 * 13 _ltq_rregex(lq,arr)       14 _ltxtq_rexec(tq,arr)
 * 15 lt_q_rregex(lqarr,lt)     16 _lt_q_rregex(lqarr,arr)
 * bout gets booleans; img/len/isnull the extract/_lca results. */
int
pg_lt_arr(int which, const unsigned char *arr, const unsigned char *rhs,
		  int *bout, const unsigned char **img, int *len, int *isnull)
{
	FunctionCallInfoBaseData fc;
	Datum		d;

	LT_ENTER();
	fc.context = NULL;
	fc.isnull = false;
	fc.nargs = (which == 10) ? 1 : 2;
	fc.args[0].value = PointerGetDatum(arr);
	fc.args[0].isnull = false;
	fc.args[1].value = PointerGetDatum(rhs);
	fc.args[1].isnull = false;
	switch (which)
	{
		case 0: *bout = DatumGetBool(lt__ltree_isparent(&fc)); break;
		case 1: *bout = DatumGetBool(lt__ltree_risparent(&fc)); break;
		case 2: *bout = DatumGetBool(lt__ltq_regex(&fc)); break;
		case 3: *bout = DatumGetBool(lt__ltxtq_exec(&fc)); break;
		case 4: *bout = DatumGetBool(lt_lt_q_regex(&fc)); break;
		case 5: *bout = DatumGetBool(lt__lt_q_regex(&fc)); break;
		case 6: case 7: case 8: case 9: case 10:
			d = (which == 6) ? lt__ltree_extract_isparent(&fc)
				: (which == 7) ? lt__ltree_extract_risparent(&fc)
				: (which == 8) ? lt__ltq_extract_regex(&fc)
				: (which == 9) ? lt__ltxtq_extract_exec(&fc)
				: lt__lca(&fc);
			*isnull = fc.isnull;
			if (!fc.isnull)
			{
				*img = (const unsigned char *) DatumGetPointer(d);
				*len = VARSIZE(DatumGetPointer(d));
			}
			break;
		case 11: *bout = DatumGetBool(lt__ltree_r_isparent(&fc)); break;
		case 12: *bout = DatumGetBool(lt__ltree_r_risparent(&fc)); break;
		case 13: *bout = DatumGetBool(lt__ltq_rregex(&fc)); break;
		case 14: *bout = DatumGetBool(lt__ltxtq_rexec(&fc)); break;
		case 15: *bout = DatumGetBool(lt_lt_q_rregex(&fc)); break;
		default: *bout = DatumGetBool(lt__lt_q_rregex(&fc)); break;
	}
	return 0;
}

int
pg_lt_crc(const unsigned char *buf, int len, uint32 *out)
{
	LT_ENTER();
	*out = lt_ltree_crc32_sz((const char *) buf, len);
	return 0;
}

