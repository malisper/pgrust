/*
 * pg_rowtypes_io.c: vendored PostgreSQL C oracle for the rowtypes_diff
 * differential fuzz target (100%-coverage campaign; crate
 * crates/backend/utils/adt/rowtypes).
 *
 * ASSEMBLED by an extraction script (scratchpad assemble_rowtypes_oracle.py):
 * every section marked "VERBATIM <path>: <symbol>" is extracted byte-for-byte
 * from the repo's vendored ground-truth checkout
 * ../pgrust-fabled/vendor/postgres-src @ 62d6c7d3df6287f1bd83199c1a746e50d31571a0
 * (PostgreSQL 18.3, Stamp-18.3). Hand-written sections are marked "SHIM".
 *
 * VERBATIM computation under test:
 *   - rowtypes.c: record_in, record_out, record_recv, record_send,
 *     record_cmp, record_larger, record_smaller, record_image_cmp,
 *     record_image_eq, hash_record, hash_record_extended.
 *   - heaptuple.c: heap_compute_data_size, fill_val, heap_fill_tuple,
 *     heap_form_tuple, heap_deform_tuple, heap_freetuple (the tuple images
 *     ARE part of the compared value plane).
 *   - tupdesc.c: populate_compact_attribute_internal (+ wrapper).
 *   - datum.c: datum_image_eq;  detoast.c: toast_raw_datum_size.
 *   - common/stringinfo.c + libpq/pqformat.c framing functions.
 *   - Header machinery: varatt.h, htup_details.h, htup.h, tupdesc.h,
 *     tupmacs.h, stringinfo.h, pqformat.h pieces (structs/macros/inlines).
 *   - hashfn: pg_hash_bytes / pg_hash_bytes_extended / pg_hash_bytes_uint32
 *     / pg_hash_bytes_uint32_extended are EXTERNed from csrc/pg_mac_io.c,
 *     which vendors src/common/hashfn.c verbatim under the pg_ prefix.
 *
 * SHIMS (environment only, never the computation under test; each listed):
 *   - Base typedefs (Datum/Oid/Size/... as c.h on LP64), MAXALIGN family,
 *     Max/Min/MemSet/CppConcat, HIGHBIT, Datum<->pointer converters: exact
 *     one-line transcriptions of c.h / postgres.h definitions.
 *   - Assert -> no-op (NDEBUG parity with the release oracle build).
 *   - ereport(ERROR)/elog(ERROR) -> record errcode class in the shared TLS
 *     pg_diff_errcode channel and longjmp to the driver entry (models PG's
 *     error longjmp; the arena below makes it leak-free).
 *     errsave(escontext,...) -> record errcode; longjmp only when escontext
 *     is NULL (hard mode), else fall through (the vendored code then runs
 *     its own `goto fail`, exactly as errsave returns in soft mode).
 *     ereturn(escontext, v, ...) -> record errcode; return v in soft mode,
 *     longjmp in hard mode. errmsg/errdetail/errhint arguments are NOT
 *     evaluated (message text out of scope; format_type_be etc. never run).
 *   - check_stack_depth() -> no-op (never recursing: no composite columns).
 *   - MemoryContextAlloc(cxt, n) -> arena palloc (fn_mcxt is inert here).
 *   - lookup_rowtype_tupdesc -> static descriptor menu (see SECTION D);
 *     ReleaseTupleDesc/PG_FREE_IF_COPY -> no-ops (refcount/toast inert).
 *   - getType{Input,Output,BinaryInput,BinaryOutput}Info + the fmgr call
 *     wrappers (InputFunctionCallSafe/OutputFunctionCall/ReceiveFunctionCall/
 *     SendFunctionCall/FunctionCall1Coll-shaped invocations) -> direct
 *     dispatch to the pinned column codecs of SECTION D. The codecs are the
 *     HARNESS CONTRACT: transcribed identically on the Rust driver side
 *     (fuzz/core/src/rowtypes_diff.rs), so they are environment, and any
 *     asymmetry is a harness bug, not a divergence.
 *   - lookup_type_cache -> static entries carrying the pinned cmp/hash
 *     codecs; format_type_be -> static type-name strings (feeds only
 *     unevaluated errmsg args and is otherwise unused).
 *   - pg_detoast_datum_packed / detoast_attr: no TOASTed inputs exist in
 *     this harness (documented carve: external/compressed varlenas are out
 *     of scope) -> identity / aborting stub. toast_raw_datum_size is
 *     VERBATIM; its external arms are unreachable here.
 *   - Expanded-object hooks (EOH_get_flat_size/EOH_flatten_into/DatumGetEOHP)
 *     and getmissingattr -> aborting stubs (unreachable: no expanded datums,
 *     descriptor natts always equals tuple natts).
 *   - pg_hton32 -> __builtin_bswap32 (little-endian hosts only, same result
 *     as port/pg_bswap.h there; both fuzz hosts are LE).
 *   - pg_attribute genbki scaffolding (CATALOG/BKI_*): the exact
 *     genbki.h no-op expansions, so FormData_pg_attribute is verbatim.
 *
 * Errcode classes (shared TLS pg_diff_errcode, defined in pg_float_io.c):
 *   1 = ERRCODE_INVALID_TEXT_REPRESENTATION  (22P02)
 *   2 = ERRCODE_FEATURE_NOT_SUPPORTED        (0A000)
 *   3 = ERRCODE_DATATYPE_MISMATCH            (42804)
 *   4 = ERRCODE_INVALID_BINARY_REPRESENTATION(22P03)
 *   5 = ERRCODE_UNDEFINED_FUNCTION           (42883)
 *   6 = ERRCODE_TOO_MANY_COLUMNS             (54011)
 *   7 = internal (elog / cannot-happen)
 *   8 = ERRCODE_PROGRAM_LIMIT_EXCEEDED       (54000; enlargeStringInfo)
 */

#include <assert.h>
#include <setjmp.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <stdio.h>

/* ---- SHIM: base typedefs (c.h on LP64) ---- */
typedef int8_t int8;
typedef int16_t int16;
typedef int32_t int32;
typedef int64_t int64;
typedef uint8_t uint8;
typedef uint16_t uint16;
typedef uint32_t uint32;
typedef uint64_t uint64;
typedef size_t Size;
typedef char *Pointer;
typedef uint8 bits8;
typedef uintptr_t Datum;
typedef unsigned int Oid;
typedef uint32 TransactionId;
typedef uint32 CommandId;
typedef uint16 OffsetNumber;
typedef uint32 BlockNumber;
typedef Oid regproc;

#define InvalidOid ((Oid) 0)
#define OidIsValid(objectId) ((bool) ((objectId) != InvalidOid))
#define RECORDOID 2249
#define InvalidBlockNumber ((BlockNumber) 0xFFFFFFFF)
#define InvalidOffsetNumber ((OffsetNumber) 0)
#define NAMEDATALEN 64
#define FLEXIBLE_ARRAY_MEMBER /* empty */
#define FirstGenbkiObjectId 10000

#ifndef Assert
#define Assert(x) ((void) 0)
#endif
#define AssertMacro(x) ((void) 0)
#define StaticAssertStmt(cond, msg) ((void) 0)
#define StaticAssertDecl(cond, msg) struct pg_diff_unused_sad
#define pg_attribute_packed() __attribute__((packed))
#define pg_attribute_aligned(a) __attribute__((aligned(a)))
#define pg_restrict __restrict
#define PointerIsValid(pointer) ((const void *) (pointer) != NULL)
#ifndef unlikely
#define unlikely(x) (x)
#define likely(x) (x)
#endif
#define CppConcat(x, y) x##y

/* c.h alignment macros — exact transcriptions */
#define TYPEALIGN(ALIGNVAL,LEN)  \
	(((uintptr_t) (LEN) + ((ALIGNVAL) - 1)) & ~((uintptr_t) ((ALIGNVAL) - 1)))
#define SHORTALIGN(LEN)			TYPEALIGN(2, (LEN))
#define INTALIGN(LEN)			TYPEALIGN(4, (LEN))
#define LONGALIGN(LEN)			TYPEALIGN(8, (LEN))
#define DOUBLEALIGN(LEN)		TYPEALIGN(8, (LEN))
#define MAXIMUM_ALIGNOF 8
#define MAXALIGN(LEN)			TYPEALIGN(MAXIMUM_ALIGNOF, (LEN))
#define TYPEALIGN_DOWN(ALIGNVAL,LEN)  \
	(((uintptr_t) (LEN)) & ~((uintptr_t) ((ALIGNVAL) - 1)))
#define MAXALIGN_DOWN(LEN)		TYPEALIGN_DOWN(MAXIMUM_ALIGNOF, (LEN))
#define Max(x, y)		((x) > (y) ? (x) : (y))
#define Min(x, y)		((x) < (y) ? (x) : (y))
#define MemSet(start, val, len) memset((start), (val), (len))
#define HIGHBIT (0x80)
#define IS_HIGHBIT_SET(ch) ((unsigned char) (ch) & HIGHBIT)
#define MaxAllocSize ((Size) 0x3fffffff)
#define STRINGINFO_DEFAULT_SIZE 1024
#define SIZEOF_DATUM 8
#define USE_FLOAT8_BYVAL 1
#define FLOAT8PASSBYVAL true

/* pg_type.h alignment/storage codes */
#define TYPALIGN_CHAR 'c'
#define TYPALIGN_SHORT 's'
#define TYPALIGN_INT 'i'
#define TYPALIGN_DOUBLE 'd'
#define TYPSTORAGE_PLAIN 'p'
#define TYPSTORAGE_EXTERNAL 'e'
#define TYPSTORAGE_EXTENDED 'x'
#define TYPSTORAGE_MAIN 'm'

/* postgres.h Datum converters — exact transcriptions */
static inline Pointer DatumGetPointer(Datum X) { return (Pointer) X; }
static inline Datum PointerGetDatum(const void *X) { return (Datum) X; }
static inline char *DatumGetCString(Datum X) { return (char *) X; }
static inline Datum CStringGetDatum(const char *X) { return PointerGetDatum(X); }
static inline Oid DatumGetObjectId(Datum X) { return (Oid) X; }
static inline Datum ObjectIdGetDatum(Oid X) { return (Datum) X; }
static inline int32 DatumGetInt32(Datum X) { return (int32) X; }
static inline Datum Int32GetDatum(int32 X) { return (Datum) X; }
static inline uint32 DatumGetUInt32(Datum X) { return (uint32) X; }
static inline Datum UInt32GetDatum(uint32 X) { return (Datum) X; }
static inline int64 DatumGetInt64(Datum X) { return (int64) X; }
static inline Datum Int64GetDatum(int64 X) { return (Datum) X; }
static inline uint64 DatumGetUInt64(Datum X) { return (uint64) X; }
static inline Datum UInt64GetDatum(uint64 X) { return (Datum) X; }
static inline bool DatumGetBool(Datum X) { return (X != 0); }
static inline Datum BoolGetDatum(bool X) { return (Datum) (X ? 1 : 0); }

/* ---- SHIM: shared TLS errcode channel (defined in csrc/pg_float_io.c) ---- */
extern _Thread_local int pg_diff_errcode;

#define PG_DIFF_ERR_INVALID_TEXT 1
#define PG_DIFF_ERR_FEATURE_NOT_SUPPORTED 2
#define PG_DIFF_ERR_DATATYPE_MISMATCH 3
#define PG_DIFF_ERR_INVALID_BINARY 4
#define PG_DIFF_ERR_UNDEFINED_FUNCTION 5
#define PG_DIFF_ERR_TOO_MANY_COLUMNS 6
#define PG_DIFF_ERR_INTERNAL 7
#define PG_DIFF_ERR_PROGRAM_LIMIT 8

#define ERRCODE_INVALID_TEXT_REPRESENTATION PG_DIFF_ERR_INVALID_TEXT
#define ERRCODE_FEATURE_NOT_SUPPORTED PG_DIFF_ERR_FEATURE_NOT_SUPPORTED
#define ERRCODE_DATATYPE_MISMATCH PG_DIFF_ERR_DATATYPE_MISMATCH
#define ERRCODE_INVALID_BINARY_REPRESENTATION PG_DIFF_ERR_INVALID_BINARY
#define ERRCODE_UNDEFINED_FUNCTION PG_DIFF_ERR_UNDEFINED_FUNCTION
#define ERRCODE_TOO_MANY_COLUMNS PG_DIFF_ERR_TOO_MANY_COLUMNS
#define ERRCODE_PROGRAM_LIMIT_EXCEEDED PG_DIFF_ERR_PROGRAM_LIMIT

/* ---- SHIM: error longjmp (per-TU jmp_buf; armed by every driver entry) ---- */
static _Thread_local jmp_buf pg_diff_rowtypes_jmp;

__attribute__((noreturn)) static void
pg_diff_rowtypes_throw(void)
{
	longjmp(pg_diff_rowtypes_jmp, 1);
}

#define ereport(level, stuff) \
	do { (void) (stuff); pg_diff_rowtypes_throw(); } while (0)
#define elog(level, ...) \
	do { pg_diff_errcode = PG_DIFF_ERR_INTERNAL; pg_diff_rowtypes_throw(); } while (0)
#define errcode(c) (pg_diff_errcode = (c))
#define errmsg(...) 0
#define errdetail(...) 0
#define errhint(...) 0
#define ereturn(escontext_, dummy_value_, stuff_) \
	do { \
		(void) (stuff_); \
		if ((escontext_) != NULL) \
			return (dummy_value_); \
		pg_diff_rowtypes_throw(); \
	} while (0)
#define errsave(escontext_, stuff_) \
	do { \
		(void) (stuff_); \
		if ((escontext_) == NULL) \
			pg_diff_rowtypes_throw(); \
	} while (0)

struct Node;					/* escontext: NULL = hard, non-NULL = soft */
typedef struct Node Node;

#define check_stack_depth() ((void) 0)

/* format_type_be feeds only unevaluated errmsg args; harmless static name. */
static char *
format_type_be(Oid t)
{
	(void) t;
	return (char *) "pg_diff_type";
}
#define FORMAT_TYPE_ALLOW_INVALID 0x2
#define format_type_extended(t, m, f) format_type_be(t)

/* ---- SHIM: TLS palloc arena (scaffold-emitted pattern; models PG's
 * memory-context reset so error-path longjmp exits cannot leak; proofs/
 * p1-lanej @ 7306d300196 precedent) ---- */
#define PG_DIFF_ARENA_MAX 256
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
	void	   *p = realloc(old, n);
	int			i;

	for (i = 0; i < pg_diff_arena_n; i++)
	{
		if (pg_diff_arena[i] == old)
		{
			pg_diff_arena[i] = p;
			return p;
		}
	}
	assert(!"repalloc of a pointer the arena never issued");
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
	assert(!"pfree of a pointer the arena never issued");
	abort();
}

#define palloc(n) pg_diff_palloc_impl(n)
#define palloc0(n) pg_diff_palloc0_impl(n)
#define repalloc(p, n) pg_diff_repalloc_impl((p), (n))
#define pfree(p) pg_diff_pfree_impl(p)
#define MemoryContextAlloc(cxt, n) pg_diff_palloc_impl(n)

/* hashfn.c symbols (VERBATIM bodies live in csrc/pg_mac_io.c) */
extern uint32 pg_hash_bytes(const unsigned char *k, int keylen);
extern uint64 pg_hash_bytes_extended(const unsigned char *k, int keylen, uint64 seed);
extern uint32 pg_hash_bytes_uint32(uint32 k);
extern uint64 pg_hash_bytes_uint32_extended(uint32 k, uint64 seed);

/* pg_bswap.h: little-endian hosts (documented shim) */
#define pg_hton32(x) __builtin_bswap32(x)
#define pg_ntoh32(x) __builtin_bswap32(x)

/* remaining postgres.h converters used by tupmacs store/fetch paths */
typedef double float8;
typedef float float4;
static inline char DatumGetChar(Datum X) { return (char) X; }
static inline Datum CharGetDatum(char X) { return (Datum) X; }
static inline int16 DatumGetInt16(Datum X) { return (int16) X; }
static inline Datum Int16GetDatum(int16 X) { return (Datum) X; }

/* c.h ALIGNOF constants (LP64) + htup_details.h column limit */
#define ALIGNOF_SHORT 2
#define ALIGNOF_INT 4
#define ALIGNOF_DOUBLE 8
#define MaxTupleAttributeNumber 1664	/* 8 * 208 */

#define pg_ntoh16(x) __builtin_bswap16(x)
/* pq_getmsgint bad-length arm; class = internal */
#define ERRCODE_PROTOCOL_VIOLATION PG_DIFF_ERR_INTERNAL
/* catalog.c IsCatalogRelationOid: our fabricated attrs carry attrelid = 0,
 * which is never a catalog oid */
#define IsCatalogRelationOid(oid) (false)

/* ---- VERBATIM include/varatt.h:15-358 (varatt structs + macros, incl guard) ---- */
#ifndef VARATT_H
#define VARATT_H

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

/* Decompressed size and compression method of a compressed-in-line Datum */
#define VARDATA_COMPRESSED_GET_EXTSIZE(PTR) \
	(((varattrib_4b *) (PTR))->va_compressed.va_tcinfo & VARLENA_EXTSIZE_MASK)
#define VARDATA_COMPRESSED_GET_COMPRESS_METHOD(PTR) \
	(((varattrib_4b *) (PTR))->va_compressed.va_tcinfo >> VARLENA_EXTSIZE_BITS)

/* Same for external Datums; but note argument is a struct varatt_external */
#define VARATT_EXTERNAL_GET_EXTSIZE(toast_pointer) \
	((toast_pointer).va_extinfo & VARLENA_EXTSIZE_MASK)
#define VARATT_EXTERNAL_GET_COMPRESS_METHOD(toast_pointer) \
	((toast_pointer).va_extinfo >> VARLENA_EXTSIZE_BITS)

#define VARATT_EXTERNAL_SET_SIZE_AND_COMPRESS_METHOD(toast_pointer, len, cm) \
	do { \
		Assert((cm) == TOAST_PGLZ_COMPRESSION_ID || \
			   (cm) == TOAST_LZ4_COMPRESSION_ID); \
		((toast_pointer).va_extinfo = \
			(len) | ((uint32) (cm) << VARLENA_EXTSIZE_BITS)); \
	} while (0)

/*
 * Testing whether an externally-stored value is compressed now requires
 * comparing size stored in va_extinfo (the actual length of the external data)
 * to rawsize (the original uncompressed datum's size).  The latter includes
 * VARHDRSZ overhead, the former doesn't.  We never use compression unless it
 * actually saves space, so we expect either equality or less-than.
 */
#define VARATT_EXTERNAL_IS_COMPRESSED(toast_pointer) \
	(VARATT_EXTERNAL_GET_EXTSIZE(toast_pointer) < \
	 (toast_pointer).va_rawsize - VARHDRSZ)

#endif

/* ---- SHIM: block.h/itemptr.h essentials (exact transcriptions) ---- */
typedef struct BlockIdData
{
	uint16		bi_hi;
	uint16		bi_lo;
} BlockIdData;

typedef BlockIdData *BlockId;

static inline void
BlockIdSet(BlockIdData *blockId, BlockNumber blockNumber)
{
	blockId->bi_hi = blockNumber >> 16;
	blockId->bi_lo = blockNumber & 0xffff;
}

typedef struct ItemPointerData
{
	BlockIdData ip_blkid;
	OffsetNumber ip_posid;
}
			pg_attribute_packed()
			pg_attribute_aligned(2)
ItemPointerData;

typedef ItemPointerData *ItemPointer;

static inline void
ItemPointerSetInvalid(ItemPointerData *pointer)
{
	Assert(PointerIsValid(pointer));
	BlockIdSet(&pointer->ip_blkid, InvalidBlockNumber);
	pointer->ip_posid = InvalidOffsetNumber;
}

/* SHIM: forward typedefs (htup.h carries these) */
typedef struct HeapTupleHeaderData HeapTupleHeaderData;
typedef HeapTupleHeaderData *HeapTupleHeader;

/* ---- VERBATIM include/access/htup_details.h:122-311 (heap tuple header structs + infomask bits) ---- */
typedef struct HeapTupleFields
{
	TransactionId t_xmin;		/* inserting xact ID */
	TransactionId t_xmax;		/* deleting or locking xact ID */

	union
	{
		CommandId	t_cid;		/* inserting or deleting command ID, or both */
		TransactionId t_xvac;	/* old-style VACUUM FULL xact ID */
	}			t_field3;
} HeapTupleFields;

typedef struct DatumTupleFields
{
	int32		datum_len_;		/* varlena header (do not touch directly!) */

	int32		datum_typmod;	/* -1, or identifier of a record type */

	Oid			datum_typeid;	/* composite type OID, or RECORDOID */

	/*
	 * datum_typeid cannot be a domain over composite, only plain composite,
	 * even if the datum is meant as a value of a domain-over-composite type.
	 * This is in line with the general principle that CoerceToDomain does not
	 * change the physical representation of the base type value.
	 *
	 * Note: field ordering is chosen with thought that Oid might someday
	 * widen to 64 bits.
	 */
} DatumTupleFields;

struct HeapTupleHeaderData
{
	union
	{
		HeapTupleFields t_heap;
		DatumTupleFields t_datum;
	}			t_choice;

	ItemPointerData t_ctid;		/* current TID of this or newer tuple (or a
								 * speculative insertion token) */

	/* Fields below here must match MinimalTupleData! */

#define FIELDNO_HEAPTUPLEHEADERDATA_INFOMASK2 2
	uint16		t_infomask2;	/* number of attributes + various flags */

#define FIELDNO_HEAPTUPLEHEADERDATA_INFOMASK 3
	uint16		t_infomask;		/* various flag bits, see below */

#define FIELDNO_HEAPTUPLEHEADERDATA_HOFF 4
	uint8		t_hoff;			/* sizeof header incl. bitmap, padding */

	/* ^ - 23 bytes - ^ */

#define FIELDNO_HEAPTUPLEHEADERDATA_BITS 5
	bits8		t_bits[FLEXIBLE_ARRAY_MEMBER];	/* bitmap of NULLs */

	/* MORE DATA FOLLOWS AT END OF STRUCT */
};

/* typedef appears in htup.h */

#define SizeofHeapTupleHeader offsetof(HeapTupleHeaderData, t_bits)

/*
 * information stored in t_infomask:
 */
#define HEAP_HASNULL			0x0001	/* has null attribute(s) */
#define HEAP_HASVARWIDTH		0x0002	/* has variable-width attribute(s) */
#define HEAP_HASEXTERNAL		0x0004	/* has external stored attribute(s) */
#define HEAP_HASOID_OLD			0x0008	/* has an object-id field */
#define HEAP_XMAX_KEYSHR_LOCK	0x0010	/* xmax is a key-shared locker */
#define HEAP_COMBOCID			0x0020	/* t_cid is a combo CID */
#define HEAP_XMAX_EXCL_LOCK		0x0040	/* xmax is exclusive locker */
#define HEAP_XMAX_LOCK_ONLY		0x0080	/* xmax, if valid, is only a locker */

 /* xmax is a shared locker */
#define HEAP_XMAX_SHR_LOCK	(HEAP_XMAX_EXCL_LOCK | HEAP_XMAX_KEYSHR_LOCK)

#define HEAP_LOCK_MASK	(HEAP_XMAX_SHR_LOCK | HEAP_XMAX_EXCL_LOCK | \
						 HEAP_XMAX_KEYSHR_LOCK)
#define HEAP_XMIN_COMMITTED		0x0100	/* t_xmin committed */
#define HEAP_XMIN_INVALID		0x0200	/* t_xmin invalid/aborted */
#define HEAP_XMIN_FROZEN		(HEAP_XMIN_COMMITTED|HEAP_XMIN_INVALID)
#define HEAP_XMAX_COMMITTED		0x0400	/* t_xmax committed */
#define HEAP_XMAX_INVALID		0x0800	/* t_xmax invalid/aborted */
#define HEAP_XMAX_IS_MULTI		0x1000	/* t_xmax is a MultiXactId */
#define HEAP_UPDATED			0x2000	/* this is UPDATEd version of row */
#define HEAP_MOVED_OFF			0x4000	/* moved to another place by pre-9.0
										 * VACUUM FULL; kept for binary
										 * upgrade support */
#define HEAP_MOVED_IN			0x8000	/* moved from another place by pre-9.0
										 * VACUUM FULL; kept for binary
										 * upgrade support */
#define HEAP_MOVED (HEAP_MOVED_OFF | HEAP_MOVED_IN)

#define HEAP_XACT_MASK			0xFFF0	/* visibility-related bits */

/*
 * A tuple is only locked (i.e. not updated by its Xmax) if the
 * HEAP_XMAX_LOCK_ONLY bit is set; or, for pg_upgrade's sake, if the Xmax is
 * not a multi and the EXCL_LOCK bit is set.
 *
 * See also HeapTupleHeaderIsOnlyLocked, which also checks for a possible
 * aborted updater transaction.
 */
static inline bool
HEAP_XMAX_IS_LOCKED_ONLY(uint16 infomask)
{
	return (infomask & HEAP_XMAX_LOCK_ONLY) ||
		(infomask & (HEAP_XMAX_IS_MULTI | HEAP_LOCK_MASK)) == HEAP_XMAX_EXCL_LOCK;
}

/*
 * A tuple that has HEAP_XMAX_IS_MULTI and HEAP_XMAX_LOCK_ONLY but neither of
 * HEAP_XMAX_EXCL_LOCK and HEAP_XMAX_KEYSHR_LOCK must come from a tuple that was
 * share-locked in 9.2 or earlier and then pg_upgrade'd.
 *
 * In 9.2 and prior, HEAP_XMAX_IS_MULTI was only set when there were multiple
 * FOR SHARE lockers of that tuple.  That set HEAP_XMAX_LOCK_ONLY (with a
 * different name back then) but neither of HEAP_XMAX_EXCL_LOCK and
 * HEAP_XMAX_KEYSHR_LOCK.  That combination is no longer possible in 9.3 and
 * up, so if we see that combination we know for certain that the tuple was
 * locked in an earlier release; since all such lockers are gone (they cannot
 * survive through pg_upgrade), such tuples can safely be considered not
 * locked.
 *
 * We must not resolve such multixacts locally, because the result would be
 * bogus, regardless of where they stand with respect to the current valid
 * multixact range.
 */
static inline bool
HEAP_LOCKED_UPGRADED(uint16 infomask)
{
	return
		(infomask & HEAP_XMAX_IS_MULTI) != 0 &&
		(infomask & HEAP_XMAX_LOCK_ONLY) != 0 &&
		(infomask & (HEAP_XMAX_EXCL_LOCK | HEAP_XMAX_KEYSHR_LOCK)) == 0;
}

/*
 * Use these to test whether a particular lock is applied to a tuple
 */
static inline bool
HEAP_XMAX_IS_SHR_LOCKED(int16 infomask)
{
	return (infomask & HEAP_LOCK_MASK) == HEAP_XMAX_SHR_LOCK;
}

static inline bool
HEAP_XMAX_IS_EXCL_LOCKED(int16 infomask)
{
	return (infomask & HEAP_LOCK_MASK) == HEAP_XMAX_EXCL_LOCK;
}

static inline bool
HEAP_XMAX_IS_KEYSHR_LOCKED(int16 infomask)
{
	return (infomask & HEAP_LOCK_MASK) == HEAP_XMAX_KEYSHR_LOCK;
}

/* turn these all off when Xmax is to change */
#define HEAP_XMAX_BITS (HEAP_XMAX_COMMITTED | HEAP_XMAX_INVALID | \
						HEAP_XMAX_IS_MULTI | HEAP_LOCK_MASK | HEAP_XMAX_LOCK_ONLY)

/*
 * information stored in t_infomask2:
 */
#define HEAP_NATTS_MASK			0x07FF	/* 11 bits for number of attributes */
/* bits 0x1800 are available */
#define HEAP_KEYS_UPDATED		0x2000	/* tuple was updated and key cols
										 * modified, or tuple deleted */
#define HEAP_HOT_UPDATED		0x4000	/* tuple was HOT-updated */
#define HEAP_ONLY_TUPLE			0x8000	/* this is heap-only tuple */

#define HEAP2_XACT_MASK			0xE000	/* visibility-related bits */

/*
 * HEAP_TUPLE_HAS_MATCH is a temporary flag used during hash joins.  It is
 * only used in tuples that are in the hash table, and those don't need
 * any visibility information, so we can overlay it on a visibility flag
 * instead of using up a dedicated bit.
 */
#define HEAP_TUPLE_HAS_MATCH	HEAP_ONLY_TUPLE /* tuple has a join match */

/*
 * HeapTupleHeader accessor functions
 */


/* ---- VERBATIM include/access/htup_details.h: HeapTupleHeaderGetDatumLength ---- */
static inline uint32
HeapTupleHeaderGetDatumLength(const HeapTupleHeaderData *tup)
{
	return VARSIZE(tup);
}

/* ---- VERBATIM include/access/htup_details.h: HeapTupleHeaderSetDatumLength ---- */
static inline void
HeapTupleHeaderSetDatumLength(HeapTupleHeaderData *tup, uint32 len)
{
	SET_VARSIZE(tup, len);
}

/* ---- VERBATIM include/access/htup_details.h: HeapTupleHeaderGetTypeId ---- */
static inline Oid
HeapTupleHeaderGetTypeId(const HeapTupleHeaderData *tup)
{
	return tup->t_choice.t_datum.datum_typeid;
}

/* ---- VERBATIM include/access/htup_details.h: HeapTupleHeaderSetTypeId ---- */
static inline void
HeapTupleHeaderSetTypeId(HeapTupleHeaderData *tup, Oid datum_typeid)
{
	tup->t_choice.t_datum.datum_typeid = datum_typeid;
}

/* ---- VERBATIM include/access/htup_details.h: HeapTupleHeaderGetTypMod ---- */
static inline int32
HeapTupleHeaderGetTypMod(const HeapTupleHeaderData *tup)
{
	return tup->t_choice.t_datum.datum_typmod;
}

/* ---- VERBATIM include/access/htup_details.h: HeapTupleHeaderSetTypMod ---- */
static inline void
HeapTupleHeaderSetTypMod(HeapTupleHeaderData *tup, int32 typmod)
{
	tup->t_choice.t_datum.datum_typmod = typmod;
}

/* ---- VERBATIM include/access/htup_details.h:577-603 (natts macros + BITMAPLEN) ---- */
/*
 * These are used with both HeapTuple and MinimalTuple, so they must be
 * macros.
 */

#define HeapTupleHeaderGetNatts(tup) \
	((tup)->t_infomask2 & HEAP_NATTS_MASK)

#define HeapTupleHeaderSetNatts(tup, natts) \
( \
	(tup)->t_infomask2 = ((tup)->t_infomask2 & ~HEAP_NATTS_MASK) | (natts) \
)

#define HeapTupleHeaderHasExternal(tup) \
		(((tup)->t_infomask & HEAP_HASEXTERNAL) != 0)


/*
 * BITMAPLEN(NATTS) -
 *		Computes size of null bitmap given number of data columns.
 */
static inline int
BITMAPLEN(int NATTS)
{
	return (NATTS + 7) / 8;
}


/* ---- VERBATIM include/access/htup.h:62-74 (HeapTupleData + HEAPTUPLESIZE) ---- */
typedef struct HeapTupleData
{
	uint32		t_len;			/* length of *t_data */
	ItemPointerData t_self;		/* SelfItemPointer */
	Oid			t_tableOid;		/* table the tuple came from */
#define FIELDNO_HEAPTUPLEDATA_DATA 3
	HeapTupleHeader t_data;		/* -> tuple header and data */
} HeapTupleData;

typedef HeapTupleData *HeapTuple;

#define HEAPTUPLESIZE	MAXALIGN(sizeof(HeapTupleData))


/* ---- VERBATIM include/access/htup_details.h: HeapTupleHasNulls ---- */
static inline bool
HeapTupleHasNulls(const HeapTupleData *tuple)
{
	return (tuple->t_data->t_infomask & HEAP_HASNULL) != 0;
}

/* ---- VERBATIM include/access/htup_details.h: HeapTupleNoNulls ---- */
static inline bool
HeapTupleNoNulls(const HeapTupleData *tuple)
{
	return !HeapTupleHasNulls(tuple);
}

/* ---- VERBATIM include/access/htup_details.h: HeapTupleHasVarWidth ---- */
static inline bool
HeapTupleHasVarWidth(const HeapTupleData *tuple)
{
	return (tuple->t_data->t_infomask & HEAP_HASVARWIDTH) != 0;
}

/* ---- SHIM: genbki.h no-op expansions + NameData so the pg_attribute
 * struct below is VERBATIM ---- */
typedef struct nameData { char data[NAMEDATALEN]; } NameData;
typedef NameData *Name;
#define CATALOG(name,oid,oidmacro)	typedef struct CppConcat(FormData_,name)
#define BKI_BOOTSTRAP
#define BKI_SHARED_RELATION
#define BKI_ROWTYPE_OID(oid,oidmacro)
#define BKI_SCHEMA_MACRO
#define BKI_DEFAULT(value)
#define BKI_ARRAY_DEFAULT(value)
#define BKI_LOOKUP(catalog)
#define BKI_LOOKUP_OPT(catalog)
#define BKI_FORCE_NULL
#define BKI_FORCE_NOT_NULL
/* ---- VERBATIM include/catalog/pg_attribute.h:37-186 + 202 ---- */
CATALOG(pg_attribute,1249,AttributeRelationId) BKI_BOOTSTRAP BKI_ROWTYPE_OID(75,AttributeRelation_Rowtype_Id) BKI_SCHEMA_MACRO
{
	Oid			attrelid BKI_LOOKUP(pg_class);	/* OID of relation containing
												 * this attribute */
	NameData	attname;		/* name of attribute */

	/*
	 * atttypid is the OID of the instance in Catalog Class pg_type that
	 * defines the data type of this attribute (e.g. int4).  Information in
	 * that instance is redundant with the attlen, attbyval, and attalign
	 * attributes of this instance, so they had better match or Postgres will
	 * fail.  In an entry for a dropped column, this field is set to zero
	 * since the pg_type entry may no longer exist; but we rely on attlen,
	 * attbyval, and attalign to still tell us how large the values in the
	 * table are.
	 */
	Oid			atttypid BKI_LOOKUP_OPT(pg_type);

	/*
	 * attlen is a copy of the typlen field from pg_type for this attribute.
	 * See atttypid comments above.
	 */
	int16		attlen;

	/*
	 * attnum is the "attribute number" for the attribute:	A value that
	 * uniquely identifies this attribute within its class. For user
	 * attributes, Attribute numbers are greater than 0 and not greater than
	 * the number of attributes in the class. I.e. if the Class pg_class says
	 * that Class XYZ has 10 attributes, then the user attribute numbers in
	 * Class pg_attribute must be 1-10.
	 *
	 * System attributes have attribute numbers less than 0 that are unique
	 * within the class, but not constrained to any particular range.
	 *
	 * Note that (attnum - 1) is often used as the index to an array.
	 */
	int16		attnum;

	/*
	 * atttypmod records type-specific data supplied at table creation time
	 * (for example, the max length of a varchar field).  It is passed to
	 * type-specific input and output functions as the third argument. The
	 * value will generally be -1 for types that do not need typmod.
	 */
	int32		atttypmod BKI_DEFAULT(-1);

	/*
	 * attndims is the declared number of dimensions, if an array type,
	 * otherwise zero.
	 */
	int16		attndims;

	/*
	 * attbyval is a copy of the typbyval field from pg_type for this
	 * attribute.  See atttypid comments above.
	 */
	bool		attbyval;

	/*
	 * attalign is a copy of the typalign field from pg_type for this
	 * attribute.  See atttypid comments above.
	 */
	char		attalign;

	/*----------
	 * attstorage tells for VARLENA attributes, what the heap access
	 * methods can do to it if a given tuple doesn't fit into a page.
	 * Possible values are as for pg_type.typstorage (see TYPSTORAGE macros).
	 *----------
	 */
	char		attstorage;

	/*
	 * attcompression sets the current compression method of the attribute.
	 * Typically this is InvalidCompressionMethod ('\0') to specify use of the
	 * current default setting (see default_toast_compression).  Otherwise,
	 * 'p' selects pglz compression, while 'l' selects LZ4 compression.
	 * However, this field is ignored whenever attstorage does not allow
	 * compression.
	 */
	char		attcompression BKI_DEFAULT('\0');

	/*
	 * Whether a (possibly invalid) not-null constraint exists for the column
	 */
	bool		attnotnull;

	/* Has DEFAULT value or not */
	bool		atthasdef BKI_DEFAULT(f);

	/* Has a missing value or not */
	bool		atthasmissing BKI_DEFAULT(f);

	/* One of the ATTRIBUTE_IDENTITY_* constants below, or '\0' */
	char		attidentity BKI_DEFAULT('\0');

	/* One of the ATTRIBUTE_GENERATED_* constants below, or '\0' */
	char		attgenerated BKI_DEFAULT('\0');

	/* Is dropped (ie, logically invisible) or not */
	bool		attisdropped BKI_DEFAULT(f);

	/*
	 * This flag specifies whether this column has ever had a local
	 * definition.  It is set for normal non-inherited columns, but also for
	 * columns that are inherited from parents if also explicitly listed in
	 * CREATE TABLE INHERITS.  It is also set when inheritance is removed from
	 * a table with ALTER TABLE NO INHERIT.  If the flag is set, the column is
	 * not dropped by a parent's DROP COLUMN even if this causes the column's
	 * attinhcount to become zero.
	 */
	bool		attislocal BKI_DEFAULT(t);

	/* Number of times inherited from direct parent relation(s) */
	int16		attinhcount BKI_DEFAULT(0);

	/* attribute's collation, if any */
	Oid			attcollation BKI_LOOKUP_OPT(pg_collation);

#ifdef CATALOG_VARLEN			/* variable-length/nullable fields start here */
	/* NOTE: The following fields are not present in tuple descriptors. */

	/*
	 * attstattarget is the target number of statistics datapoints to collect
	 * during VACUUM ANALYZE of this column.  A zero here indicates that we do
	 * not wish to collect any stats about this column. A null value here
	 * indicates that no value has been explicitly set for this column, so
	 * ANALYZE should use the default setting.
	 *
	 * int16 is sufficient for the current max value (MAX_STATISTICS_TARGET).
	 */
	int16		attstattarget BKI_DEFAULT(_null_) BKI_FORCE_NULL;

	/* Column-level access permissions */
	aclitem		attacl[1] BKI_DEFAULT(_null_);

	/* Column-level options */
	text		attoptions[1] BKI_DEFAULT(_null_);

	/* Column-level FDW options */
	text		attfdwoptions[1] BKI_DEFAULT(_null_);

	/*
	 * Missing value for added columns. This is a one element array which lets
	 * us store a value of the attribute type here.
	 */
	anyarray	attmissingval BKI_DEFAULT(_null_);
#endif
} FormData_pg_attribute;
typedef FormData_pg_attribute *Form_pg_attribute;

/* ---- VERBATIM include/access/tupdesc.h:68-88 (CompactAttribute) ---- */
typedef struct CompactAttribute
{
	int32		attcacheoff;	/* fixed offset into tuple, if known, or -1 */
	int16		attlen;			/* attr len in bytes or -1 = varlen, -2 =
								 * cstring */
	bool		attbyval;		/* as FormData_pg_attribute.attbyval */
	bool		attispackable;	/* FormData_pg_attribute.attstorage !=
								 * TYPSTORAGE_PLAIN */
	bool		atthasmissing;	/* as FormData_pg_attribute.atthasmissing */
	bool		attisdropped;	/* as FormData_pg_attribute.attisdropped */
	bool		attgenerated;	/* FormData_pg_attribute.attgenerated != '\0' */
	char		attnullability; /* status of not-null constraint, see below */
	uint8		attalignby;		/* alignment requirement in bytes */
} CompactAttribute;

/* Valid values for CompactAttribute->attnullability */
#define	ATTNULLABLE_UNRESTRICTED 'f'	/* No constraint exists */
#define	ATTNULLABLE_UNKNOWN		'u' /* constraint exists, validity unknown */
#define	ATTNULLABLE_VALID		'v' /* valid constraint exists */
#define	ATTNULLABLE_INVALID		'i' /* constraint exists, marked invalid */


/* ---- SHIM: TupleConstr is never consulted here (constr == NULL) ---- */
typedef struct TupleConstr { int pg_diff_unused; } TupleConstr;

/* ---- VERBATIM include/access/tupdesc.h:135-145 (TupleDescData) ---- */
typedef struct TupleDescData
{
	int			natts;			/* number of attributes in the tuple */
	Oid			tdtypeid;		/* composite type ID for tuple type */
	int32		tdtypmod;		/* typmod for tuple type */
	int			tdrefcount;		/* reference count, or -1 if not counting */
	TupleConstr *constr;		/* constraints, or NULL if none */
	/* compact_attrs[N] is the compact metadata of Attribute Number N+1 */
	CompactAttribute compact_attrs[FLEXIBLE_ARRAY_MEMBER];
}			TupleDescData;
typedef struct TupleDescData *TupleDesc;

/* ---- VERBATIM include/access/tupdesc.h:149-167 (TupleDescAttr) ---- */
/*
 * Calculates the base address of the Form_pg_attribute at the end of the
 * TupleDescData struct.
 */
#define TupleDescAttrAddress(desc) \
	(Form_pg_attribute) ((char *) (desc) + \
	 (offsetof(struct TupleDescData, compact_attrs) + \
	 (desc)->natts * sizeof(CompactAttribute)))

/* Accessor for the i'th FormData_pg_attribute element of tupdesc. */
static inline FormData_pg_attribute *
TupleDescAttr(TupleDesc tupdesc, int i)
{
	FormData_pg_attribute *attrs = TupleDescAttrAddress(tupdesc);

	return &attrs[i];
}

#undef TupleDescAttrAddress

/* ---- VERBATIM include/access/tupdesc.h TupleDescCompactAttr, with the
 * USE_ASSERT_CHECKING verify_compact_attribute call compiled out exactly
 * as a production build does ---- */
/*
 * Accessor for the i'th CompactAttribute element of tupdesc.
 */
static inline CompactAttribute *
TupleDescCompactAttr(TupleDesc tupdesc, int i)
{
	CompactAttribute *cattr = &tupdesc->compact_attrs[i];

#ifdef USE_ASSERT_CHECKING

	/* Check that the CompactAttribute is correctly populated */
	verify_compact_attribute(tupdesc, i);
#endif

	return cattr;
}

/* ---- VERBATIM backend/access/common/heaptuple.c: COMPACT_ATTR_IS_PACKABLE ---- */
#define COMPACT_ATTR_IS_PACKABLE(att) \
	((att)->attlen == -1 && (att)->attispackable)

/* ---- VERBATIM include/access/detoast.h: VARATT_EXTERNAL_GET_POINTER ---- */
#define VARATT_EXTERNAL_GET_POINTER(toast_pointer, attr) \
do { \
	varattrib_1b_e *attre = (varattrib_1b_e *) (attr); \
	Assert(VARATT_IS_EXTERNAL(attre)); \
	Assert(VARSIZE_EXTERNAL(attre) == sizeof(toast_pointer) + VARHDRSZ_EXTERNAL); \
	memcpy(&(toast_pointer), VARDATA_EXTERNAL(attre), sizeof(toast_pointer)); \
} while (0)

/* ---- VERBATIM include/access/tupmacs.h:20-233 (tupmacs) ---- */
/*
 * Check a tuple's null bitmap to determine whether the attribute is null.
 * Note that a 0 in the null bitmap indicates a null, while 1 indicates
 * non-null.
 */
static inline bool
att_isnull(int ATT, const bits8 *BITS)
{
	return !(BITS[ATT >> 3] & (1 << (ATT & 0x07)));
}

#ifndef FRONTEND
/*
 * Given an attbyval and an attlen from either a Form_pg_attribute or
 * CompactAttribute and a pointer into a tuple's data area, return the
 * correct value or pointer.
 *
 * We return a Datum value in all cases.  If attbyval is false,  we return the
 * same pointer into the tuple data area that we're passed.  Otherwise, we
 * return the correct number of bytes fetched from the data area and extended
 * to Datum form.
 *
 * On machines where Datum is 8 bytes, we support fetching 8-byte byval
 * attributes; otherwise, only 1, 2, and 4-byte values are supported.
 *
 * Note that T must already be properly aligned for this to work correctly.
 */
#define fetchatt(A,T) fetch_att(T, (A)->attbyval, (A)->attlen)

/*
 * Same, but work from byval/len parameters rather than Form_pg_attribute.
 */
static inline Datum
fetch_att(const void *T, bool attbyval, int attlen)
{
	if (attbyval)
	{
		switch (attlen)
		{
			case sizeof(char):
				return CharGetDatum(*((const char *) T));
			case sizeof(int16):
				return Int16GetDatum(*((const int16 *) T));
			case sizeof(int32):
				return Int32GetDatum(*((const int32 *) T));
#if SIZEOF_DATUM == 8
			case sizeof(Datum):
				return *((const Datum *) T);
#endif
			default:
				elog(ERROR, "unsupported byval length: %d", attlen);
				return 0;
		}
	}
	else
		return PointerGetDatum(T);
}
#endif							/* FRONTEND */

/*
 * att_align_datum aligns the given offset as needed for a datum of alignment
 * requirement attalign and typlen attlen.  attdatum is the Datum variable
 * we intend to pack into a tuple (it's only accessed if we are dealing with
 * a varlena type).  Note that this assumes the Datum will be stored as-is;
 * callers that are intending to convert non-short varlena datums to short
 * format have to account for that themselves.
 */
#define att_align_datum(cur_offset, attalign, attlen, attdatum) \
( \
	((attlen) == -1 && VARATT_IS_SHORT(DatumGetPointer(attdatum))) ? \
	(uintptr_t) (cur_offset) : \
	att_align_nominal(cur_offset, attalign) \
)

/*
 * Similar to att_align_datum, but accepts a number of bytes, typically from
 * CompactAttribute.attalignby to align the Datum by.
 */
#define att_datum_alignby(cur_offset, attalignby, attlen, attdatum) \
	( \
	((attlen) == -1 && VARATT_IS_SHORT(DatumGetPointer(attdatum))) ? \
	(uintptr_t) (cur_offset) : \
	TYPEALIGN(attalignby, cur_offset))

/*
 * att_align_pointer performs the same calculation as att_align_datum,
 * but is used when walking a tuple.  attptr is the current actual data
 * pointer; when accessing a varlena field we have to "peek" to see if we
 * are looking at a pad byte or the first byte of a 1-byte-header datum.
 * (A zero byte must be either a pad byte, or the first byte of a correctly
 * aligned 4-byte length word; in either case we can align safely.  A non-zero
 * byte must be either a 1-byte length word, or the first byte of a correctly
 * aligned 4-byte length word; in either case we need not align.)
 *
 * Note: some callers pass a "char *" pointer for cur_offset.  This is
 * a bit of a hack but should work all right as long as uintptr_t is the
 * correct width.
 */
#define att_align_pointer(cur_offset, attalign, attlen, attptr) \
( \
	((attlen) == -1 && VARATT_NOT_PAD_BYTE(attptr)) ? \
	(uintptr_t) (cur_offset) : \
	att_align_nominal(cur_offset, attalign) \
)

/*
 * Similar to att_align_pointer, but accepts a number of bytes, typically from
 * CompactAttribute.attalignby to align the pointer by.
 */
#define att_pointer_alignby(cur_offset, attalignby, attlen, attptr) \
	( \
	((attlen) == -1 && VARATT_NOT_PAD_BYTE(attptr)) ? \
	(uintptr_t) (cur_offset) : \
	TYPEALIGN(attalignby, cur_offset))

/*
 * att_align_nominal aligns the given offset as needed for a datum of alignment
 * requirement attalign, ignoring any consideration of packed varlena datums.
 * There are three main use cases for using this macro directly:
 *	* we know that the att in question is not varlena (attlen != -1);
 *	  in this case it is cheaper than the above macros and just as good.
 *	* we need to estimate alignment padding cost abstractly, ie without
 *	  reference to a real tuple.  We must assume the worst case that
 *	  all varlenas are aligned.
 *	* within arrays and multiranges, we unconditionally align varlenas (XXX this
 *	  should be revisited, probably).
 *
 * The attalign cases are tested in what is hopefully something like their
 * frequency of occurrence.
 */
#define att_align_nominal(cur_offset, attalign) \
( \
	((attalign) == TYPALIGN_INT) ? INTALIGN(cur_offset) : \
	 (((attalign) == TYPALIGN_CHAR) ? (uintptr_t) (cur_offset) : \
	  (((attalign) == TYPALIGN_DOUBLE) ? DOUBLEALIGN(cur_offset) : \
	   ( \
			AssertMacro((attalign) == TYPALIGN_SHORT), \
			SHORTALIGN(cur_offset) \
	   ))) \
)

/*
 * Similar to att_align_nominal, but accepts a number of bytes, typically from
 * CompactAttribute.attalignby to align the offset by.
 */
#define att_nominal_alignby(cur_offset, attalignby) \
	TYPEALIGN(attalignby, cur_offset)

/*
 * att_addlength_datum increments the given offset by the space needed for
 * the given Datum variable.  attdatum is only accessed if we are dealing
 * with a variable-length attribute.
 */
#define att_addlength_datum(cur_offset, attlen, attdatum) \
	att_addlength_pointer(cur_offset, attlen, DatumGetPointer(attdatum))

/*
 * att_addlength_pointer performs the same calculation as att_addlength_datum,
 * but is used when walking a tuple --- attptr is the pointer to the field
 * within the tuple.
 *
 * Note: some callers pass a "char *" pointer for cur_offset.  This is
 * actually perfectly OK, but probably should be cleaned up along with
 * the same practice for att_align_pointer.
 */
#define att_addlength_pointer(cur_offset, attlen, attptr) \
( \
	((attlen) > 0) ? \
	( \
		(cur_offset) + (attlen) \
	) \
	: (((attlen) == -1) ? \
	( \
		(cur_offset) + VARSIZE_ANY(attptr) \
	) \
	: \
	( \
		AssertMacro((attlen) == -2), \
		(cur_offset) + (strlen((char *) (attptr)) + 1) \
	)) \
)

#ifndef FRONTEND
/*
 * store_att_byval is a partial inverse of fetch_att: store a given Datum
 * value into a tuple data area at the specified address.  However, it only
 * handles the byval case, because in typical usage the caller needs to
 * distinguish by-val and by-ref cases anyway, and so a do-it-all function
 * wouldn't be convenient.
 */
static inline void
store_att_byval(void *T, Datum newdatum, int attlen)
{
	switch (attlen)
	{
		case sizeof(char):
			*(char *) T = DatumGetChar(newdatum);
			break;
		case sizeof(int16):
			*(int16 *) T = DatumGetInt16(newdatum);
			break;
		case sizeof(int32):
			*(int32 *) T = DatumGetInt32(newdatum);
			break;
#if SIZEOF_DATUM == 8
		case sizeof(Datum):
			*(Datum *) T = newdatum;
			break;
#endif
		default:
			elog(ERROR, "unsupported byval length: %d", attlen);
	}
}
#endif							/* FRONTEND */

/* ---- VERBATIM include/lib/stringinfo.h:46-54 (StringInfoData) ---- */
typedef struct StringInfoData
{
	char	   *data;
	int			len;
	int			maxlen;
	int			cursor;
} StringInfoData;

typedef StringInfoData *StringInfo;

/* ---- VERBATIM include/lib/stringinfo.h: initReadOnlyStringInfo ---- */
static inline void
initReadOnlyStringInfo(StringInfo str, char *data, int len)
{
	str->data = data;
	str->len = len;
	str->maxlen = 0;			/* read-only */
	str->cursor = 0;
}

/* ---- SHIM: c.h varlena typedefs (exact transcriptions) ---- */
#define VARHDRSZ		((int32) sizeof(int32))
typedef struct varlena bytea;
typedef struct varlena text;

/* ---- SHIM: fmgr environment (plumbing only). The vendored rowtypes.c
 * bodies only touch: flinfo->fn_extra / fn_mcxt, fcinfo->context /
 * args[n] / isnull, and the call wrappers below, which dispatch straight
 * to the SECTION D pinned codecs. ---- */
#define FUNC_MAX_ARGS_SHIM 8

typedef struct FmgrInfo FmgrInfo;
typedef struct FunctionCallInfoBaseData *FunctionCallInfo;
typedef Datum (*PGFunction) (FunctionCallInfo fcinfo);

struct FmgrInfo
{
	PGFunction	fn_addr;
	Oid			fn_oid;
	bool		fn_strict;
	void	   *fn_extra;
	void	   *fn_mcxt;
};

typedef struct NullableDatum
{
	Datum		value;
	bool		isnull;
} NullableDatum;

typedef struct FunctionCallInfoBaseData
{
	FmgrInfo   *flinfo;
	Node	   *context;
	void	   *resultinfo;
	Oid			fncollation;
	bool		isnull;
	short		nargs;
	NullableDatum args[FUNC_MAX_ARGS_SHIM];
}			FunctionCallInfoBaseData;

#define PG_FUNCTION_ARGS FunctionCallInfo fcinfo

#define LOCAL_FCINFO(name, nargs_) \
	FunctionCallInfoBaseData name##data; \
	FunctionCallInfo name = &name##data

#define InitFunctionCallInfoData(Fcinfo, Flinfo, Nargs, Collation, Context, Resultinfo) \
	do { \
		(Fcinfo).flinfo = (Flinfo); \
		(Fcinfo).context = (Context); \
		(Fcinfo).resultinfo = (Resultinfo); \
		(Fcinfo).fncollation = (Collation); \
		(Fcinfo).isnull = false; \
		(Fcinfo).nargs = (Nargs); \
	} while (0)

#define FunctionCallInvoke(fcinfo) ((*(fcinfo)->flinfo->fn_addr) (fcinfo))

#define PG_GETARG_DATUM(n)	 (fcinfo->args[n].value)
#define PG_GETARG_CSTRING(n) DatumGetCString(PG_GETARG_DATUM(n))
#define PG_GETARG_OID(n)	 DatumGetObjectId(PG_GETARG_DATUM(n))
#define PG_GETARG_INT32(n)	 DatumGetInt32(PG_GETARG_DATUM(n))
#define PG_GETARG_INT64(n)	 DatumGetInt64(PG_GETARG_DATUM(n))
#define PG_GETARG_BOOL(n)	 DatumGetBool(PG_GETARG_DATUM(n))
#define PG_GETARG_INT16(n)	 DatumGetInt16(PG_GETARG_DATUM(n))
#define PG_GETARG_POINTER(n) ((void *) DatumGetPointer(PG_GETARG_DATUM(n)))
#define PG_RETURN_DATUM(x)	 return (x)
#define PG_RETURN_CSTRING(x) return CStringGetDatum(x)
#define PG_RETURN_BOOL(x)	 return BoolGetDatum(x)
#define PG_RETURN_INT32(x)	 return Int32GetDatum(x)
#define PG_RETURN_UINT32(x)	 return UInt32GetDatum(x)
#define PG_RETURN_UINT64(x)	 return UInt64GetDatum(x)
#define PG_RETURN_NULL() \
	do { fcinfo->isnull = true; return (Datum) 0; } while (0)

/* No TOASTed inputs exist in this harness (documented carve). */
static struct varlena *
pg_diff_detoast_guard(struct varlena *v)
{
	if (VARATT_IS_EXTERNAL(v) || VARATT_IS_COMPRESSED(v))
		abort();				/* unreachable: harness never builds toast */
	return v;
}

static struct varlena *
pg_detoast_datum_packed_shim(struct varlena *v)
{
	return pg_diff_detoast_guard(v);
}

static struct varlena *
pg_detoast_datum_shim(struct varlena *v)
{
	v = pg_diff_detoast_guard(v);
	if (VARATT_IS_SHORT(v))
		abort();				/* unreachable: composite datums have 4B headers */
	return v;
}

#define PG_DETOAST_DATUM(datum) \
	pg_detoast_datum_shim((struct varlena *) DatumGetPointer(datum))
#define PG_DETOAST_DATUM_PACKED(datum) \
	pg_detoast_datum_packed_shim((struct varlena *) DatumGetPointer(datum))
#define PG_GETARG_HEAPTUPLEHEADER(n) \
	((HeapTupleHeader) PG_DETOAST_DATUM(PG_GETARG_DATUM(n)))
#define PG_RETURN_HEAPTUPLEHEADER(x) return PointerGetDatum(x)
#define PG_RETURN_BYTEA_P(x) return PointerGetDatum(x)
#define PG_FREE_IF_COPY(ptr, n) ((void) 0)	/* no detoast copies here */

/* ---- SHIM: typcache surface (entries pinned in SECTION D) ---- */
typedef struct TypeCacheEntry
{
	Oid			type_id;
	FmgrInfo	cmp_proc_finfo;
	FmgrInfo	eq_opr_finfo;
	FmgrInfo	hash_proc_finfo;
	FmgrInfo	hash_extended_proc_finfo;
} TypeCacheEntry;

#define TYPECACHE_EQ_OPR_FINFO 0x0008
#define TYPECACHE_CMP_PROC_FINFO 0x0080
#define TYPECACHE_HASH_PROC_FINFO 0x0100
#define TYPECACHE_HASH_EXTENDED_PROC_FINFO 0x8000

static TypeCacheEntry *lookup_type_cache(Oid type_id, int flags);
static TupleDesc lookup_rowtype_tupdesc(Oid type_id, int32 typmod);
#define ReleaseTupleDesc(tupdesc) ((void) 0)

static void getTypeInputInfo(Oid type, Oid *typInput, Oid *typIOParam);
static void getTypeOutputInfo(Oid type, Oid *typOutput, bool *typIsVarlena);
static void getTypeBinaryInputInfo(Oid type, Oid *typReceive, Oid *typIOParam);
static void getTypeBinaryOutputInfo(Oid type, Oid *typSend, bool *typIsVarlena);
static void fmgr_info_cxt(Oid functionId, FmgrInfo *finfo, void *mcxt);
static bool InputFunctionCallSafe(FmgrInfo *flinfo, char *str,
								  Oid typioparam, int32 typmod,
								  Node *escontext, Datum *result);
static char *OutputFunctionCall(FmgrInfo *flinfo, Datum val);
static Datum ReceiveFunctionCall(FmgrInfo *flinfo, StringInfo buf,
								 Oid typioparam, int32 typmod);
static bytea *SendFunctionCall(FmgrInfo *flinfo, Datum val);
static Datum FunctionCall2Coll(FmgrInfo *flinfo, Oid collation,
							   Datum arg1, Datum arg2);

/* ---- SHIM: unreachable-machinery stubs (documented in the header) ---- */
typedef struct ExpandedObjectHeader ExpandedObjectHeader;
static ExpandedObjectHeader *
DatumGetEOHP(Datum d)
{
	(void) d;
	abort();					/* unreachable: no expanded datums */
}
static Size
EOH_get_flat_size(ExpandedObjectHeader *eohptr)
{
	(void) eohptr;
	abort();
}
static void
EOH_flatten_into(ExpandedObjectHeader *eohptr, void *result, Size allocated_size)
{
	(void) eohptr; (void) result; (void) allocated_size;
	abort();
}
#define VARATT_IS_EXTERNAL_EXPANDED_SHIM_KEEP 1

static Datum
getmissingattr(TupleDesc tupleDesc, int attnum, bool *isnull)
{
	(void) tupleDesc; (void) attnum; (void) isnull;
	abort();					/* unreachable: descriptor natts == tuple natts */
}

static struct varlena *
detoast_attr(struct varlena *attr)
{
	(void) attr;
	abort();					/* unreachable: no toast in this harness */
}

static struct varlena *
detoast_external_attr(struct varlena *attr)
{
	(void) attr;
	abort();					/* unreachable: no toast in this harness */
}

/* SHIM: intra-TU prototypes for the verbatim stringinfo/pq bodies */
static void enlargeStringInfo(StringInfo str, int needed);
static void resetStringInfo(StringInfo str);
static void appendBinaryStringInfo(StringInfo str, const void *data, int datalen);

/* ---- VERBATIM common/stringinfo.c: initStringInfoInternal ---- */
static inline void
initStringInfoInternal(StringInfo str, int initsize)
{
	Assert(initsize >= 1 && initsize <= MaxAllocSize);

	str->data = (char *) palloc(initsize);
	str->maxlen = initsize;
	resetStringInfo(str);
}

/* ---- VERBATIM common/stringinfo.c: initStringInfo [static-prefixed] ---- */
static void
initStringInfo(StringInfo str)
{
	initStringInfoInternal(str, STRINGINFO_DEFAULT_SIZE);
}

/* ---- VERBATIM common/stringinfo.c: resetStringInfo [static-prefixed] ---- */
static void
resetStringInfo(StringInfo str)
{
	/* don't allow resets of read-only StringInfos */
	Assert(str->maxlen != 0);

	str->data[0] = '\0';
	str->len = 0;
	str->cursor = 0;
}

/* ---- VERBATIM common/stringinfo.c: appendStringInfoChar [static-prefixed] ---- */
static void
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

/* ---- VERBATIM common/stringinfo.c: appendBinaryStringInfo [static-prefixed] ---- */
static void
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

/* ---- VERBATIM common/stringinfo.c: enlargeStringInfo [static-prefixed] ---- */
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

/* ---- VERBATIM include/lib/stringinfo.h:231-234 (appendStringInfoCharMacro) ---- */
#define appendStringInfoCharMacro(str,ch) \
	(((str)->len + 1 >= (str)->maxlen) ? \
	 appendStringInfoChar(str, ch) : \
	 (void)((str)->data[(str)->len] = (ch), (str)->data[++(str)->len] = '\0'))

/* ---- VERBATIM include/libpq/pqformat.h: pq_writeint32 ---- */
static inline void
pq_writeint32(StringInfoData *pg_restrict buf, uint32 i)
{
	uint32		ni = pg_hton32(i);

	Assert(buf->len + (int) sizeof(uint32) <= buf->maxlen);
	memcpy((char *pg_restrict) (buf->data + buf->len), &ni, sizeof(uint32));
	buf->len += sizeof(uint32);
}

/* ---- VERBATIM backend/libpq/pqformat.c: pq_begintypsend [static-prefixed] ---- */
static void
pq_begintypsend(StringInfo buf)
{
	initStringInfo(buf);
	/* Reserve four bytes for the bytea length word */
	appendStringInfoCharMacro(buf, '\0');
	appendStringInfoCharMacro(buf, '\0');
	appendStringInfoCharMacro(buf, '\0');
	appendStringInfoCharMacro(buf, '\0');
}

/* ---- VERBATIM backend/libpq/pqformat.c: pq_endtypsend [static-prefixed] ---- */
static bytea *
pq_endtypsend(StringInfo buf)
{
	bytea	   *result = (bytea *) buf->data;

	/* Insert correct length into bytea length word */
	Assert(buf->len >= VARHDRSZ);
	SET_VARSIZE(result, buf->len);

	return result;
}

/* ---- VERBATIM backend/libpq/pqformat.c: pq_sendbytes [static-prefixed] ---- */
static void
pq_sendbytes(StringInfo buf, const void *data, int datalen)
{
	/* use variant that maintains a trailing null-byte, out of caution */
	appendBinaryStringInfo(buf, data, datalen);
}

/* ---- VERBATIM backend/libpq/pqformat.c: pq_copymsgbytes [static-prefixed] ---- */
static void
pq_copymsgbytes(StringInfo msg, void *buf, int datalen)
{
	if (datalen < 0 || datalen > (msg->len - msg->cursor))
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("insufficient data left in message")));
	memcpy(buf, &msg->data[msg->cursor], datalen);
	msg->cursor += datalen;
}

/* ---- VERBATIM backend/libpq/pqformat.c: pq_getmsgint [static-prefixed] ---- */
static unsigned int
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

/* ---- VERBATIM include/libpq/pqformat.h:142-149 (pq_sendint32 inline) ---- */
/* append a binary [u]int32 to a StringInfo buffer */
static inline void
pq_sendint32(StringInfo buf, uint32 i)
{
	enlargeStringInfo(buf, sizeof(uint32));
	pq_writeint32(buf, i);
}


/* ---- VERBATIM backend/access/common/tupdesc.c: populate_compact_attribute_internal ---- */
static inline void
populate_compact_attribute_internal(Form_pg_attribute src,
									CompactAttribute *dst)
{
	memset(dst, 0, sizeof(CompactAttribute));

	dst->attcacheoff = -1;
	dst->attlen = src->attlen;

	dst->attbyval = src->attbyval;
	dst->attispackable = (src->attstorage != TYPSTORAGE_PLAIN);
	dst->atthasmissing = src->atthasmissing;
	dst->attisdropped = src->attisdropped;
	dst->attgenerated = (src->attgenerated != '\0');

	/*
	 * Assign nullability status for this column.  Assuming that a constraint
	 * exists, at this point we don't know if a not-null constraint is valid,
	 * so we assign UNKNOWN unless the table is a catalog, in which case we
	 * know it's valid.
	 */
	dst->attnullability = !src->attnotnull ? ATTNULLABLE_UNRESTRICTED :
		IsCatalogRelationOid(src->attrelid) ? ATTNULLABLE_VALID :
		ATTNULLABLE_UNKNOWN;

	switch (src->attalign)
	{
		case TYPALIGN_INT:
			dst->attalignby = ALIGNOF_INT;
			break;
		case TYPALIGN_CHAR:
			dst->attalignby = sizeof(char);
			break;
		case TYPALIGN_DOUBLE:
			dst->attalignby = ALIGNOF_DOUBLE;
			break;
		case TYPALIGN_SHORT:
			dst->attalignby = ALIGNOF_SHORT;
			break;
		default:
			dst->attalignby = 0;
			elog(ERROR, "invalid attalign value: %c", src->attalign);
			break;
	}
}

/* ---- VERBATIM backend/access/common/tupdesc.c: populate_compact_attribute [static-prefixed] ---- */
static void
populate_compact_attribute(TupleDesc tupdesc, int attnum)
{
	Form_pg_attribute src = TupleDescAttr(tupdesc, attnum);
	CompactAttribute *dst;

	/*
	 * Don't use TupleDescCompactAttr to prevent infinite recursion in assert
	 * builds.
	 */
	dst = &tupdesc->compact_attrs[attnum];

	populate_compact_attribute_internal(src, dst);
}

/* ---- VERBATIM backend/access/common/heaptuple.c: heap_compute_data_size [static-prefixed] ---- */
static Size
heap_compute_data_size(TupleDesc tupleDesc,
					   const Datum *values,
					   const bool *isnull)
{
	Size		data_length = 0;
	int			i;
	int			numberOfAttributes = tupleDesc->natts;

	for (i = 0; i < numberOfAttributes; i++)
	{
		Datum		val;
		CompactAttribute *atti;

		if (isnull[i])
			continue;

		val = values[i];
		atti = TupleDescCompactAttr(tupleDesc, i);

		if (COMPACT_ATTR_IS_PACKABLE(atti) &&
			VARATT_CAN_MAKE_SHORT(DatumGetPointer(val)))
		{
			/*
			 * we're anticipating converting to a short varlena header, so
			 * adjust length and don't count any alignment
			 */
			data_length += VARATT_CONVERTED_SHORT_SIZE(DatumGetPointer(val));
		}
		else if (atti->attlen == -1 &&
				 VARATT_IS_EXTERNAL_EXPANDED(DatumGetPointer(val)))
		{
			/*
			 * we want to flatten the expanded value so that the constructed
			 * tuple doesn't depend on it
			 */
			data_length = att_nominal_alignby(data_length, atti->attalignby);
			data_length += EOH_get_flat_size(DatumGetEOHP(val));
		}
		else
		{
			data_length = att_datum_alignby(data_length, atti->attalignby,
											atti->attlen, val);
			data_length = att_addlength_datum(data_length, atti->attlen,
											  val);
		}
	}

	return data_length;
}

/* ---- VERBATIM backend/access/common/heaptuple.c: fill_val ---- */
static inline void
fill_val(CompactAttribute *att,
		 bits8 **bit,
		 int *bitmask,
		 char **dataP,
		 uint16 *infomask,
		 Datum datum,
		 bool isnull)
{
	Size		data_length;
	char	   *data = *dataP;

	/*
	 * If we're building a null bitmap, set the appropriate bit for the
	 * current column value here.
	 */
	if (bit != NULL)
	{
		if (*bitmask != HIGHBIT)
			*bitmask <<= 1;
		else
		{
			*bit += 1;
			**bit = 0x0;
			*bitmask = 1;
		}

		if (isnull)
		{
			*infomask |= HEAP_HASNULL;
			return;
		}

		**bit |= *bitmask;
	}

	/*
	 * XXX we use the att_nominal_alignby macro on the pointer value itself,
	 * not on an offset.  This is a bit of a hack.
	 */
	if (att->attbyval)
	{
		/* pass-by-value */
		data = (char *) att_nominal_alignby(data, att->attalignby);
		store_att_byval(data, datum, att->attlen);
		data_length = att->attlen;
	}
	else if (att->attlen == -1)
	{
		/* varlena */
		Pointer		val = DatumGetPointer(datum);

		*infomask |= HEAP_HASVARWIDTH;
		if (VARATT_IS_EXTERNAL(val))
		{
			if (VARATT_IS_EXTERNAL_EXPANDED(val))
			{
				/*
				 * we want to flatten the expanded value so that the
				 * constructed tuple doesn't depend on it
				 */
				ExpandedObjectHeader *eoh = DatumGetEOHP(datum);

				data = (char *) att_nominal_alignby(data, att->attalignby);
				data_length = EOH_get_flat_size(eoh);
				EOH_flatten_into(eoh, data, data_length);
			}
			else
			{
				*infomask |= HEAP_HASEXTERNAL;
				/* no alignment, since it's short by definition */
				data_length = VARSIZE_EXTERNAL(val);
				memcpy(data, val, data_length);
			}
		}
		else if (VARATT_IS_SHORT(val))
		{
			/* no alignment for short varlenas */
			data_length = VARSIZE_SHORT(val);
			memcpy(data, val, data_length);
		}
		else if (att->attispackable && VARATT_CAN_MAKE_SHORT(val))
		{
			/* convert to short varlena -- no alignment */
			data_length = VARATT_CONVERTED_SHORT_SIZE(val);
			SET_VARSIZE_SHORT(data, data_length);
			memcpy(data + 1, VARDATA(val), data_length - 1);
		}
		else
		{
			/* full 4-byte header varlena */
			data = (char *) att_nominal_alignby(data, att->attalignby);
			data_length = VARSIZE(val);
			memcpy(data, val, data_length);
		}
	}
	else if (att->attlen == -2)
	{
		/* cstring ... never needs alignment */
		*infomask |= HEAP_HASVARWIDTH;
		Assert(att->attalignby == sizeof(char));
		data_length = strlen(DatumGetCString(datum)) + 1;
		memcpy(data, DatumGetPointer(datum), data_length);
	}
	else
	{
		/* fixed-length pass-by-reference */
		data = (char *) att_nominal_alignby(data, att->attalignby);
		Assert(att->attlen > 0);
		data_length = att->attlen;
		memcpy(data, DatumGetPointer(datum), data_length);
	}

	data += data_length;
	*dataP = data;
}

/* ---- VERBATIM backend/access/common/heaptuple.c: heap_fill_tuple [static-prefixed] ---- */
static void
heap_fill_tuple(TupleDesc tupleDesc,
				const Datum *values, const bool *isnull,
				char *data, Size data_size,
				uint16 *infomask, bits8 *bit)
{
	bits8	   *bitP;
	int			bitmask;
	int			i;
	int			numberOfAttributes = tupleDesc->natts;

#ifdef USE_ASSERT_CHECKING
	char	   *start = data;
#endif

	if (bit != NULL)
	{
		bitP = &bit[-1];
		bitmask = HIGHBIT;
	}
	else
	{
		/* just to keep compiler quiet */
		bitP = NULL;
		bitmask = 0;
	}

	*infomask &= ~(HEAP_HASNULL | HEAP_HASVARWIDTH | HEAP_HASEXTERNAL);

	for (i = 0; i < numberOfAttributes; i++)
	{
		CompactAttribute *attr = TupleDescCompactAttr(tupleDesc, i);

		fill_val(attr,
				 bitP ? &bitP : NULL,
				 &bitmask,
				 &data,
				 infomask,
				 values ? values[i] : PointerGetDatum(NULL),
				 isnull ? isnull[i] : true);
	}

	Assert((data - start) == data_size);
}

/* ---- VERBATIM backend/access/common/heaptuple.c: heap_form_tuple [static-prefixed] ---- */
static HeapTuple
heap_form_tuple(TupleDesc tupleDescriptor,
				const Datum *values,
				const bool *isnull)
{
	HeapTuple	tuple;			/* return tuple */
	HeapTupleHeader td;			/* tuple data */
	Size		len,
				data_len;
	int			hoff;
	bool		hasnull = false;
	int			numberOfAttributes = tupleDescriptor->natts;
	int			i;

	if (numberOfAttributes > MaxTupleAttributeNumber)
		ereport(ERROR,
				(errcode(ERRCODE_TOO_MANY_COLUMNS),
				 errmsg("number of columns (%d) exceeds limit (%d)",
						numberOfAttributes, MaxTupleAttributeNumber)));

	/*
	 * Check for nulls
	 */
	for (i = 0; i < numberOfAttributes; i++)
	{
		if (isnull[i])
		{
			hasnull = true;
			break;
		}
	}

	/*
	 * Determine total space needed
	 */
	len = offsetof(HeapTupleHeaderData, t_bits);

	if (hasnull)
		len += BITMAPLEN(numberOfAttributes);

	hoff = len = MAXALIGN(len); /* align user data safely */

	data_len = heap_compute_data_size(tupleDescriptor, values, isnull);

	len += data_len;

	/*
	 * Allocate and zero the space needed.  Note that the tuple body and
	 * HeapTupleData management structure are allocated in one chunk.
	 */
	tuple = (HeapTuple) palloc0(HEAPTUPLESIZE + len);
	tuple->t_data = td = (HeapTupleHeader) ((char *) tuple + HEAPTUPLESIZE);

	/*
	 * And fill in the information.  Note we fill the Datum fields even though
	 * this tuple may never become a Datum.  This lets HeapTupleHeaderGetDatum
	 * identify the tuple type if needed.
	 */
	tuple->t_len = len;
	ItemPointerSetInvalid(&(tuple->t_self));
	tuple->t_tableOid = InvalidOid;

	HeapTupleHeaderSetDatumLength(td, len);
	HeapTupleHeaderSetTypeId(td, tupleDescriptor->tdtypeid);
	HeapTupleHeaderSetTypMod(td, tupleDescriptor->tdtypmod);
	/* We also make sure that t_ctid is invalid unless explicitly set */
	ItemPointerSetInvalid(&(td->t_ctid));

	HeapTupleHeaderSetNatts(td, numberOfAttributes);
	td->t_hoff = hoff;

	heap_fill_tuple(tupleDescriptor,
					values,
					isnull,
					(char *) td + hoff,
					data_len,
					&td->t_infomask,
					(hasnull ? td->t_bits : NULL));

	return tuple;
}

/* ---- VERBATIM backend/access/common/heaptuple.c: heap_deform_tuple [static-prefixed] ---- */
static void
heap_deform_tuple(HeapTuple tuple, TupleDesc tupleDesc,
				  Datum *values, bool *isnull)
{
	HeapTupleHeader tup = tuple->t_data;
	bool		hasnulls = HeapTupleHasNulls(tuple);
	int			tdesc_natts = tupleDesc->natts;
	int			natts;			/* number of atts to extract */
	int			attnum;
	char	   *tp;				/* ptr to tuple data */
	uint32		off;			/* offset in tuple data */
	bits8	   *bp = tup->t_bits;	/* ptr to null bitmap in tuple */
	bool		slow = false;	/* can we use/set attcacheoff? */

	natts = HeapTupleHeaderGetNatts(tup);

	/*
	 * In inheritance situations, it is possible that the given tuple actually
	 * has more fields than the caller is expecting.  Don't run off the end of
	 * the caller's arrays.
	 */
	natts = Min(natts, tdesc_natts);

	tp = (char *) tup + tup->t_hoff;

	off = 0;

	for (attnum = 0; attnum < natts; attnum++)
	{
		CompactAttribute *thisatt = TupleDescCompactAttr(tupleDesc, attnum);

		if (hasnulls && att_isnull(attnum, bp))
		{
			values[attnum] = (Datum) 0;
			isnull[attnum] = true;
			slow = true;		/* can't use attcacheoff anymore */
			continue;
		}

		isnull[attnum] = false;

		if (!slow && thisatt->attcacheoff >= 0)
			off = thisatt->attcacheoff;
		else if (thisatt->attlen == -1)
		{
			/*
			 * We can only cache the offset for a varlena attribute if the
			 * offset is already suitably aligned, so that there would be no
			 * pad bytes in any case: then the offset will be valid for either
			 * an aligned or unaligned value.
			 */
			if (!slow &&
				off == att_nominal_alignby(off, thisatt->attalignby))
				thisatt->attcacheoff = off;
			else
			{
				off = att_pointer_alignby(off, thisatt->attalignby, -1,
										  tp + off);
				slow = true;
			}
		}
		else
		{
			/* not varlena, so safe to use att_nominal_alignby */
			off = att_nominal_alignby(off, thisatt->attalignby);

			if (!slow)
				thisatt->attcacheoff = off;
		}

		values[attnum] = fetchatt(thisatt, tp + off);

		off = att_addlength_pointer(off, thisatt->attlen, tp + off);

		if (thisatt->attlen <= 0)
			slow = true;		/* can't use attcacheoff anymore */
	}

	/*
	 * If tuple doesn't have all the atts indicated by tupleDesc, read the
	 * rest as nulls or missing values as appropriate.
	 */
	for (; attnum < tdesc_natts; attnum++)
		values[attnum] = getmissingattr(tupleDesc, attnum + 1, &isnull[attnum]);
}

/* ---- VERBATIM backend/access/common/heaptuple.c: heap_freetuple [static-prefixed] ---- */
static void
heap_freetuple(HeapTuple htup)
{
	pfree(htup);
}

/* ---- VERBATIM backend/access/common/detoast.c: toast_raw_datum_size [static-prefixed] ---- */
static Size
toast_raw_datum_size(Datum value)
{
	struct varlena *attr = (struct varlena *) DatumGetPointer(value);
	Size		result;

	if (VARATT_IS_EXTERNAL_ONDISK(attr))
	{
		/* va_rawsize is the size of the original datum -- including header */
		struct varatt_external toast_pointer;

		VARATT_EXTERNAL_GET_POINTER(toast_pointer, attr);
		result = toast_pointer.va_rawsize;
	}
	else if (VARATT_IS_EXTERNAL_INDIRECT(attr))
	{
		struct varatt_indirect toast_pointer;

		VARATT_EXTERNAL_GET_POINTER(toast_pointer, attr);

		/* nested indirect Datums aren't allowed */
		Assert(!VARATT_IS_EXTERNAL_INDIRECT(toast_pointer.pointer));

		return toast_raw_datum_size(PointerGetDatum(toast_pointer.pointer));
	}
	else if (VARATT_IS_EXTERNAL_EXPANDED(attr))
	{
		result = EOH_get_flat_size(DatumGetEOHP(value));
	}
	else if (VARATT_IS_COMPRESSED(attr))
	{
		/* here, va_rawsize is just the payload size */
		result = VARDATA_COMPRESSED_GET_EXTSIZE(attr) + VARHDRSZ;
	}
	else if (VARATT_IS_SHORT(attr))
	{
		/*
		 * we have to normalize the header length to VARHDRSZ or else the
		 * callers of this function will be confused.
		 */
		result = VARSIZE_SHORT(attr) - VARHDRSZ_SHORT + VARHDRSZ;
	}
	else
	{
		/* plain untoasted datum */
		result = VARSIZE(attr);
	}
	return result;
}

/* ---- VERBATIM backend/utils/adt/datum.c: datum_image_eq [static-prefixed] ---- */
static bool
datum_image_eq(Datum value1, Datum value2, bool typByVal, int typLen)
{
	Size		len1,
				len2;
	bool		result = true;

	if (typByVal)
	{
		result = (value1 == value2);
	}
	else if (typLen > 0)
	{
		result = (memcmp(DatumGetPointer(value1),
						 DatumGetPointer(value2),
						 typLen) == 0);
	}
	else if (typLen == -1)
	{
		len1 = toast_raw_datum_size(value1);
		len2 = toast_raw_datum_size(value2);
		/* No need to de-toast if lengths don't match. */
		if (len1 != len2)
			result = false;
		else
		{
			struct varlena *arg1val;
			struct varlena *arg2val;

			arg1val = PG_DETOAST_DATUM_PACKED(value1);
			arg2val = PG_DETOAST_DATUM_PACKED(value2);

			result = (memcmp(VARDATA_ANY(arg1val),
							 VARDATA_ANY(arg2val),
							 len1 - VARHDRSZ) == 0);

			/* Only free memory if it's a copy made here. */
			if ((Pointer) arg1val != (Pointer) value1)
				pfree(arg1val);
			if ((Pointer) arg2val != (Pointer) value2)
				pfree(arg2val);
		}
	}
	else if (typLen == -2)
	{
		char	   *s1,
				   *s2;

		/* Compare cstring datums */
		s1 = DatumGetCString(value1);
		s2 = DatumGetCString(value2);
		len1 = strlen(s1) + 1;
		len2 = strlen(s2) + 1;
		if (len1 != len2)
			return false;
		result = (memcmp(s1, s2, len1) == 0);
	}
	else
		elog(ERROR, "unexpected typLen: %d", typLen);

	return result;
}

/* ==================== SECTION D: pinned environment ==================== */
/*
 * Column codecs — THE HARNESS CONTRACT. Each is transcribed identically on
 * the Rust driver side (fuzz/core/src/rowtypes_diff.rs, same doc-comment
 * names); they are pinned ENVIRONMENT (what typcache/fmgr would dispatch
 * to), never the computation under test. Any C-vs-Rust asymmetry in them
 * is a harness bug.
 *
 * Codec algorithms (contract of record, keep in sync with the Rust side):
 *   mytextin(s):    text varlena with payload = the cstring bytes.
 *   mytextout(t):   cstring with the (possibly short-header) payload bytes.
 *   mytextrecv(b):  consumes ALL remaining buffer bytes as the payload.
 *   mytextsend(t):  bytea with the payload bytes.
 *   myint4in(s):    optional '-', then 1+ ASCII digits, nothing else;
 *                   value must fit int32 (accumulate in int64, bail on
 *                   |acc| > 2^31); errors are class 1 (22P02), soft-aware.
 *   myint4out(v):   sprintf("%d").
 *   myint4recv(b):  exactly-4-byte big-endian read; short buffer = class 4.
 *   myint4send(v):  4-byte big-endian bytea.
 *   myint4cmp:      (a<b) -1 / (a>b) 1 / 0.
 *   mytextcmp:      memcmp over min(len) payload bytes, ties by length.
 *   myint4hash(v):      pg_hash_bytes_uint32((uint32) v)   [hashint4 shape]
 *   myint4hashext(v,s): pg_hash_bytes_uint32_extended(v, s)
 *   mytexthash(t):      pg_hash_bytes(payload, len)        [hashtext shape]
 *   mytexthashext(t,s): pg_hash_bytes_extended(payload, len, s)
 *
 *   myint4eq/mytexteq:  == of the cmp codecs' 0 verdict (record_eq arms).
 *   myboolin(s):    exactly "t"/"f" (class 1 otherwise, soft-aware);
 *   myboolout: "t"/"f"; myboolrecv: 1 byte !=0 (short = class 4);
 *   myboolsend: 1 byte 0/1.
 *   myint2in/out/recv/send: myint4 shapes bounded to int16 / 2-byte BE.
 *   myint8in: '-' + 1..18 digits (the 18-digit cap IS the contract);
 *   myint8out: %lld; myint8recv/send: 8-byte BE.
 *   myfix8in(s):    first <=8 cstring bytes zero-padded into an 8-byte
 *   BY-REF buffer (never errors); myfix8out: 16 lowercase hex chars;
 *   myfix8recv: exactly 8 buffer bytes (short = class 4); myfix8send:
 *   the 8 raw bytes.
 *
 * Column type menu:  INT4OID(23) / TEXTOID(25) / FAKETYPE(7777) /
 * BOOLOID(16) / INT2OID(21) / INT8OID(20) / FIX8TYPE(7778).
 * FAKETYPE has text I/O but NO cmp/hash/eq support: it drives the
 * could-not-identify-function error arms. bool/int2/int8/fix8 also have
 * NO cmp/hash/eq support (they exist for the datum_image byval-width and
 * fixed-byref arms; support lookups on them witness error arms).
 *
 * Descriptor menu (typmod = index; MUST match the Rust driver's
 * registration order):
 *   0: (text, text)
 *   1: (int4, text)
 *   2: (text, [dropped], text)
 *   3: (int4, faketype)
 *   4: (text)
 *   5: (bool, int2, int8)
 *   6: (fix8, bool)
 */
#define INT4OID 23
#define TEXTOID 25
#define PG_DIFF_FAKETYPE 7777
#define BOOLOID 16
#define INT2OID 21
#define INT8OID 20
#define PG_DIFF_FIX8TYPE 7778

#define MYTEXTIN 91001
#define MYTEXTOUT 91002
#define MYTEXTRECV 91003
#define MYTEXTSEND 91004
#define MYINT4IN 91011
#define MYINT4OUT 91012
#define MYINT4RECV 91013
#define MYINT4SEND 91014
#define MYINT4CMP 91021
#define MYTEXTCMP 91022
#define MYINT4HASH 91031
#define MYINT4HASHEXT 91032
#define MYTEXTHASH 91033
#define MYTEXTHASHEXT 91034
#define MYINT4EQ 91023
#define MYTEXTEQ 91024
#define MYBOOLIN 91041
#define MYBOOLOUT 91042
#define MYBOOLRECV 91043
#define MYBOOLSEND 91044
#define MYINT2IN 91051
#define MYINT2OUT 91052
#define MYINT2RECV 91053
#define MYINT2SEND 91054
#define MYINT8IN 91061
#define MYINT8OUT 91062
#define MYINT8RECV 91063
#define MYINT8SEND 91064
#define MYFIX8IN 91071
#define MYFIX8OUT 91072
#define MYFIX8RECV 91073
#define MYFIX8SEND 91074

/* soft-error flag for the codec input path (InputFunctionCallSafe shim) */
static _Thread_local bool pg_diff_codec_failed;

static Datum
pg_diff_mytextin(FunctionCallInfo fcinfo)
{
	char	   *s = PG_GETARG_CSTRING(0);
	Size		n = strlen(s);
	text	   *t = (text *) palloc(VARHDRSZ + n);

	SET_VARSIZE(t, VARHDRSZ + n);
	memcpy(VARDATA(t), s, n);
	return PointerGetDatum(t);
}

static Datum
pg_diff_mytextout(FunctionCallInfo fcinfo)
{
	text	   *t = (text *) PG_DETOAST_DATUM_PACKED(PG_GETARG_DATUM(0));
	Size		n = VARSIZE_ANY_EXHDR(t);
	char	   *out = (char *) palloc(n + 1);

	memcpy(out, VARDATA_ANY(t), n);
	out[n] = '\0';
	return CStringGetDatum(out);
}

static Datum
pg_diff_mytextrecv(FunctionCallInfo fcinfo)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);
	int			n = buf->len - buf->cursor;
	text	   *t = (text *) palloc(VARHDRSZ + n);

	SET_VARSIZE(t, VARHDRSZ + n);
	memcpy(VARDATA(t), &buf->data[buf->cursor], n);
	buf->cursor += n;
	return PointerGetDatum(t);
}

static Datum
pg_diff_mytextsend(FunctionCallInfo fcinfo)
{
	text	   *t = (text *) PG_DETOAST_DATUM_PACKED(PG_GETARG_DATUM(0));
	Size		n = VARSIZE_ANY_EXHDR(t);
	bytea	   *b = (bytea *) palloc(VARHDRSZ + n);

	SET_VARSIZE(b, VARHDRSZ + n);
	memcpy(VARDATA(b), VARDATA_ANY(t), n);
	return PointerGetDatum(b);
}

static Datum
pg_diff_myint4in(FunctionCallInfo fcinfo)
{
	char	   *s = PG_GETARG_CSTRING(0);
	Node	   *escontext = fcinfo->context;
	const char *p = s;
	bool		neg = false;
	int64		acc = 0;
	int			ndigits = 0;

	if (*p == '-')
	{
		neg = true;
		p++;
	}
	for (; *p; p++)
	{
		if (*p < '0' || *p > '9')
			break;
		acc = acc * 10 + (*p - '0');
		ndigits++;
		if (acc > ((int64) 1 << 31))
			break;				/* bail: overflow (or later junk) fails below */
	}
	if (ndigits == 0 || *p != '\0' ||
		(!neg && acc > 2147483647LL) || (neg && acc > 2147483648LL))
	{
		pg_diff_errcode = PG_DIFF_ERR_INVALID_TEXT;
		pg_diff_codec_failed = true;
		if (escontext != NULL)
			return (Datum) 0;
		pg_diff_rowtypes_throw();
	}
	return Int32GetDatum((int32) (neg ? -acc : acc));
}

static Datum
pg_diff_myint4out(FunctionCallInfo fcinfo)
{
	int32		v = PG_GETARG_INT32(0);
	char	   *out = (char *) palloc(12);

	snprintf(out, 12, "%d", v);
	return CStringGetDatum(out);
}

static Datum
pg_diff_myint4recv(FunctionCallInfo fcinfo)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);
	uint32		v;

	if (buf->len - buf->cursor < 4)
	{
		pg_diff_errcode = PG_DIFF_ERR_INVALID_BINARY;
		pg_diff_rowtypes_throw();
	}
	memcpy(&v, &buf->data[buf->cursor], 4);
	buf->cursor += 4;
	return Int32GetDatum((int32) pg_ntoh32(v));
}

static Datum
pg_diff_myint4send(FunctionCallInfo fcinfo)
{
	uint32		v = pg_hton32((uint32) PG_GETARG_INT32(0));
	bytea	   *b = (bytea *) palloc(VARHDRSZ + 4);

	SET_VARSIZE(b, VARHDRSZ + 4);
	memcpy(VARDATA(b), &v, 4);
	return PointerGetDatum(b);
}

static Datum
pg_diff_myint4cmp(FunctionCallInfo fcinfo)
{
	int32		a = PG_GETARG_INT32(0);
	int32		b = PG_GETARG_INT32(1);

	return Int32GetDatum((a < b) ? -1 : (a > b) ? 1 : 0);
}

static Datum
pg_diff_mytextcmp(FunctionCallInfo fcinfo)
{
	text	   *ta = (text *) PG_DETOAST_DATUM_PACKED(PG_GETARG_DATUM(0));
	text	   *tb = (text *) PG_DETOAST_DATUM_PACKED(PG_GETARG_DATUM(1));
	Size		la = VARSIZE_ANY_EXHDR(ta);
	Size		lb = VARSIZE_ANY_EXHDR(tb);
	int			c = memcmp(VARDATA_ANY(ta), VARDATA_ANY(tb), Min(la, lb));

	if (c == 0 && la != lb)
		c = (la < lb) ? -1 : 1;
	else if (c < 0)
		c = -1;
	else if (c > 0)
		c = 1;
	return Int32GetDatum(c);
}

static Datum
pg_diff_myint4hash(FunctionCallInfo fcinfo)
{
	return UInt32GetDatum(pg_hash_bytes_uint32((uint32) PG_GETARG_INT32(0)));
}

static Datum
pg_diff_myint4hashext(FunctionCallInfo fcinfo)
{
	return UInt64GetDatum(pg_hash_bytes_uint32_extended((uint32) PG_GETARG_INT32(0),
														(uint64) PG_GETARG_INT64(1)));
}

static Datum
pg_diff_mytexthash(FunctionCallInfo fcinfo)
{
	text	   *t = (text *) PG_DETOAST_DATUM_PACKED(PG_GETARG_DATUM(0));

	return UInt32GetDatum(pg_hash_bytes((const unsigned char *) VARDATA_ANY(t),
										(int) VARSIZE_ANY_EXHDR(t)));
}

static Datum
pg_diff_mytexthashext(FunctionCallInfo fcinfo)
{
	text	   *t = (text *) PG_DETOAST_DATUM_PACKED(PG_GETARG_DATUM(0));

	return UInt64GetDatum(pg_hash_bytes_extended((const unsigned char *) VARDATA_ANY(t),
												 (int) VARSIZE_ANY_EXHDR(t),
												 (uint64) PG_GETARG_INT64(1)));
}

/* eq codecs (contract: same value semantics as the cmp codecs' ==0) */
static Datum
pg_diff_myint4eq(FunctionCallInfo fcinfo)
{
	return BoolGetDatum(PG_GETARG_INT32(0) == PG_GETARG_INT32(1));
}

static Datum
pg_diff_mytexteq(FunctionCallInfo fcinfo)
{
	text	   *ta = (text *) PG_DETOAST_DATUM_PACKED(PG_GETARG_DATUM(0));
	text	   *tb = (text *) PG_DETOAST_DATUM_PACKED(PG_GETARG_DATUM(1));
	Size		la = VARSIZE_ANY_EXHDR(ta);
	Size		lb = VARSIZE_ANY_EXHDR(tb);

	return BoolGetDatum(la == lb &&
						memcmp(VARDATA_ANY(ta), VARDATA_ANY(tb), la) == 0);
}

/* bool codec: in = "t"/"f" exactly (class-1 otherwise, soft-aware);
 * out = "t"/"f"; recv = 1 byte (!=0 -> true), short = class 4;
 * send = 1 byte 0/1. */
static Datum
pg_diff_myboolin(FunctionCallInfo fcinfo)
{
	char	   *s = PG_GETARG_CSTRING(0);
	Node	   *escontext = fcinfo->context;

	if (s[0] == 't' && s[1] == '\0')
		return BoolGetDatum(true);
	if (s[0] == 'f' && s[1] == '\0')
		return BoolGetDatum(false);
	pg_diff_errcode = PG_DIFF_ERR_INVALID_TEXT;
	pg_diff_codec_failed = true;
	if (escontext != NULL)
		return (Datum) 0;
	pg_diff_rowtypes_throw();
	return (Datum) 0;			/* unreachable */
}

static Datum
pg_diff_myboolout(FunctionCallInfo fcinfo)
{
	char	   *out = (char *) palloc(2);

	out[0] = PG_GETARG_BOOL(0) ? 't' : 'f';
	out[1] = '\0';
	return CStringGetDatum(out);
}

static Datum
pg_diff_myboolrecv(FunctionCallInfo fcinfo)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);

	if (buf->len - buf->cursor < 1)
	{
		pg_diff_errcode = PG_DIFF_ERR_INVALID_BINARY;
		pg_diff_rowtypes_throw();
	}
	return BoolGetDatum(buf->data[buf->cursor++] != 0);
}

static Datum
pg_diff_myboolsend(FunctionCallInfo fcinfo)
{
	bytea	   *b = (bytea *) palloc(VARHDRSZ + 1);

	SET_VARSIZE(b, VARHDRSZ + 1);
	VARDATA(b)[0] = PG_GETARG_BOOL(0) ? 1 : 0;
	return PointerGetDatum(b);
}

/* int2 codec: myint4in shape bounded to int16; recv/send 2-byte BE */
static Datum
pg_diff_myint2in(FunctionCallInfo fcinfo)
{
	char	   *s = PG_GETARG_CSTRING(0);
	Node	   *escontext = fcinfo->context;
	const char *p = s;
	bool		neg = false;
	int64		acc = 0;
	int			ndigits = 0;

	if (*p == '-')
	{
		neg = true;
		p++;
	}
	for (; *p; p++)
	{
		if (*p < '0' || *p > '9')
			break;
		acc = acc * 10 + (*p - '0');
		ndigits++;
		if (acc > ((int64) 1 << 31))
			break;
	}
	if (ndigits == 0 || *p != '\0' ||
		(!neg && acc > 32767LL) || (neg && acc > 32768LL))
	{
		pg_diff_errcode = PG_DIFF_ERR_INVALID_TEXT;
		pg_diff_codec_failed = true;
		if (escontext != NULL)
			return (Datum) 0;
		pg_diff_rowtypes_throw();
	}
	return Int16GetDatum((int16) (neg ? -acc : acc));
}

static Datum
pg_diff_myint2out(FunctionCallInfo fcinfo)
{
	char	   *out = (char *) palloc(8);

	snprintf(out, 8, "%d", (int) PG_GETARG_INT16(0));
	return CStringGetDatum(out);
}

static Datum
pg_diff_myint2recv(FunctionCallInfo fcinfo)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);
	unsigned char b0, b1;

	if (buf->len - buf->cursor < 2)
	{
		pg_diff_errcode = PG_DIFF_ERR_INVALID_BINARY;
		pg_diff_rowtypes_throw();
	}
	b0 = (unsigned char) buf->data[buf->cursor++];
	b1 = (unsigned char) buf->data[buf->cursor++];
	return Int16GetDatum((int16) (((uint16) b0 << 8) | b1));
}

static Datum
pg_diff_myint2send(FunctionCallInfo fcinfo)
{
	uint16		v = (uint16) PG_GETARG_INT16(0);
	bytea	   *b = (bytea *) palloc(VARHDRSZ + 2);

	SET_VARSIZE(b, VARHDRSZ + 2);
	VARDATA(b)[0] = (char) (v >> 8);
	VARDATA(b)[1] = (char) (v & 0xff);
	return PointerGetDatum(b);
}

/* int8 codec: optional '-', 1..18 digits (18-digit cap is the CONTRACT,
 * not int8in semantics); recv/send 8-byte BE */
static Datum
pg_diff_myint8in(FunctionCallInfo fcinfo)
{
	char	   *s = PG_GETARG_CSTRING(0);
	Node	   *escontext = fcinfo->context;
	const char *p = s;
	bool		neg = false;
	int64		acc = 0;
	int			ndigits = 0;

	if (*p == '-')
	{
		neg = true;
		p++;
	}
	for (; *p; p++)
	{
		if (*p < '0' || *p > '9' || ndigits >= 18)
			break;
		acc = acc * 10 + (*p - '0');
		ndigits++;
	}
	if (ndigits == 0 || *p != '\0')
	{
		pg_diff_errcode = PG_DIFF_ERR_INVALID_TEXT;
		pg_diff_codec_failed = true;
		if (escontext != NULL)
			return (Datum) 0;
		pg_diff_rowtypes_throw();
	}
	return Int64GetDatum(neg ? -acc : acc);
}

static Datum
pg_diff_myint8out(FunctionCallInfo fcinfo)
{
	char	   *out = (char *) palloc(24);

	snprintf(out, 24, "%lld", (long long) PG_GETARG_INT64(0));
	return CStringGetDatum(out);
}

static Datum
pg_diff_myint8recv(FunctionCallInfo fcinfo)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);
	uint64		v = 0;
	int			i;

	if (buf->len - buf->cursor < 8)
	{
		pg_diff_errcode = PG_DIFF_ERR_INVALID_BINARY;
		pg_diff_rowtypes_throw();
	}
	for (i = 0; i < 8; i++)
		v = (v << 8) | (unsigned char) buf->data[buf->cursor++];
	return Int64GetDatum((int64) v);
}

static Datum
pg_diff_myint8send(FunctionCallInfo fcinfo)
{
	uint64		v = (uint64) PG_GETARG_INT64(0);
	bytea	   *b = (bytea *) palloc(VARHDRSZ + 8);
	int			i;

	SET_VARSIZE(b, VARHDRSZ + 8);
	for (i = 0; i < 8; i++)
		VARDATA(b)[i] = (char) (v >> (56 - 8 * i));
	return PointerGetDatum(b);
}

/* fix8 codec (fixed-length BY-REF, attlen 8): in = first <=8 cstring bytes
 * zero-padded into an 8-byte buffer (never errors); out = 16 lowercase hex
 * chars; recv = exactly 8 buffer bytes (short = class 4); send = the 8 raw
 * bytes. */
static Datum
pg_diff_myfix8in(FunctionCallInfo fcinfo)
{
	char	   *s = PG_GETARG_CSTRING(0);
	unsigned char *buf = (unsigned char *) palloc(8);
	int			i;

	memset(buf, 0, 8);
	for (i = 0; i < 8 && s[i]; i++)
		buf[i] = (unsigned char) s[i];
	return PointerGetDatum(buf);
}

static Datum
pg_diff_myfix8out(FunctionCallInfo fcinfo)
{
	const unsigned char *v = (const unsigned char *) PG_GETARG_POINTER(0);
	char	   *out = (char *) palloc(17);
	static const char hx[] = "0123456789abcdef";
	int			i;

	for (i = 0; i < 8; i++)
	{
		out[2 * i] = hx[v[i] >> 4];
		out[2 * i + 1] = hx[v[i] & 0xf];
	}
	out[16] = '\0';
	return CStringGetDatum(out);
}

static Datum
pg_diff_myfix8recv(FunctionCallInfo fcinfo)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);
	unsigned char *v = (unsigned char *) palloc(8);

	if (buf->len - buf->cursor < 8)
	{
		pg_diff_errcode = PG_DIFF_ERR_INVALID_BINARY;
		pg_diff_rowtypes_throw();
	}
	memcpy(v, &buf->data[buf->cursor], 8);
	buf->cursor += 8;
	return PointerGetDatum(v);
}

static Datum
pg_diff_myfix8send(FunctionCallInfo fcinfo)
{
	bytea	   *b = (bytea *) palloc(VARHDRSZ + 8);

	SET_VARSIZE(b, VARHDRSZ + 8);
	memcpy(VARDATA(b), (const void *) PG_GETARG_POINTER(0), 8);
	return PointerGetDatum(b);
}

static PGFunction
pg_diff_resolve_codec(Oid functionId)
{
	switch (functionId)
	{
		case MYTEXTIN: return pg_diff_mytextin;
		case MYTEXTOUT: return pg_diff_mytextout;
		case MYTEXTRECV: return pg_diff_mytextrecv;
		case MYTEXTSEND: return pg_diff_mytextsend;
		case MYINT4IN: return pg_diff_myint4in;
		case MYINT4OUT: return pg_diff_myint4out;
		case MYINT4RECV: return pg_diff_myint4recv;
		case MYINT4SEND: return pg_diff_myint4send;
		case MYINT4CMP: return pg_diff_myint4cmp;
		case MYTEXTCMP: return pg_diff_mytextcmp;
		case MYINT4HASH: return pg_diff_myint4hash;
		case MYINT4HASHEXT: return pg_diff_myint4hashext;
		case MYTEXTHASH: return pg_diff_mytexthash;
		case MYTEXTHASHEXT: return pg_diff_mytexthashext;
		case MYINT4EQ: return pg_diff_myint4eq;
		case MYTEXTEQ: return pg_diff_mytexteq;
		case MYBOOLIN: return pg_diff_myboolin;
		case MYBOOLOUT: return pg_diff_myboolout;
		case MYBOOLRECV: return pg_diff_myboolrecv;
		case MYBOOLSEND: return pg_diff_myboolsend;
		case MYINT2IN: return pg_diff_myint2in;
		case MYINT2OUT: return pg_diff_myint2out;
		case MYINT2RECV: return pg_diff_myint2recv;
		case MYINT2SEND: return pg_diff_myint2send;
		case MYINT8IN: return pg_diff_myint8in;
		case MYINT8OUT: return pg_diff_myint8out;
		case MYINT8RECV: return pg_diff_myint8recv;
		case MYINT8SEND: return pg_diff_myint8send;
		case MYFIX8IN: return pg_diff_myfix8in;
		case MYFIX8OUT: return pg_diff_myfix8out;
		case MYFIX8RECV: return pg_diff_myfix8recv;
		case MYFIX8SEND: return pg_diff_myfix8send;
		default:
			abort();			/* harness bug: unknown codec oid */
	}
}

static void
fmgr_info_cxt(Oid functionId, FmgrInfo *finfo, void *mcxt)
{
	finfo->fn_addr = pg_diff_resolve_codec(functionId);
	finfo->fn_oid = functionId;
	finfo->fn_strict = true;
	finfo->fn_extra = NULL;
	finfo->fn_mcxt = mcxt;
}

static void
getTypeInputInfo(Oid type, Oid *typInput, Oid *typIOParam)
{
	*typIOParam = type;
	switch (type)
	{
		case TEXTOID: case PG_DIFF_FAKETYPE: *typInput = MYTEXTIN; break;
		case INT4OID: *typInput = MYINT4IN; break;
		case BOOLOID: *typInput = MYBOOLIN; break;
		case INT2OID: *typInput = MYINT2IN; break;
		case INT8OID: *typInput = MYINT8IN; break;
		case PG_DIFF_FIX8TYPE: *typInput = MYFIX8IN; break;
		default: abort();
	}
}

static void
getTypeOutputInfo(Oid type, Oid *typOutput, bool *typIsVarlena)
{
	switch (type)
	{
		case TEXTOID: case PG_DIFF_FAKETYPE: *typOutput = MYTEXTOUT; *typIsVarlena = true; break;
		case INT4OID: *typOutput = MYINT4OUT; *typIsVarlena = false; break;
		case BOOLOID: *typOutput = MYBOOLOUT; *typIsVarlena = false; break;
		case INT2OID: *typOutput = MYINT2OUT; *typIsVarlena = false; break;
		case INT8OID: *typOutput = MYINT8OUT; *typIsVarlena = false; break;
		case PG_DIFF_FIX8TYPE: *typOutput = MYFIX8OUT; *typIsVarlena = false; break;
		default: abort();
	}
}

static void
getTypeBinaryInputInfo(Oid type, Oid *typReceive, Oid *typIOParam)
{
	*typIOParam = type;
	switch (type)
	{
		case TEXTOID: case PG_DIFF_FAKETYPE: *typReceive = MYTEXTRECV; break;
		case INT4OID: *typReceive = MYINT4RECV; break;
		case BOOLOID: *typReceive = MYBOOLRECV; break;
		case INT2OID: *typReceive = MYINT2RECV; break;
		case INT8OID: *typReceive = MYINT8RECV; break;
		case PG_DIFF_FIX8TYPE: *typReceive = MYFIX8RECV; break;
		default: abort();
	}
}

static void
getTypeBinaryOutputInfo(Oid type, Oid *typSend, bool *typIsVarlena)
{
	switch (type)
	{
		case TEXTOID: case PG_DIFF_FAKETYPE: *typSend = MYTEXTSEND; *typIsVarlena = true; break;
		case INT4OID: *typSend = MYINT4SEND; *typIsVarlena = false; break;
		case BOOLOID: *typSend = MYBOOLSEND; *typIsVarlena = false; break;
		case INT2OID: *typSend = MYINT2SEND; *typIsVarlena = false; break;
		case INT8OID: *typSend = MYINT8SEND; *typIsVarlena = false; break;
		case PG_DIFF_FIX8TYPE: *typSend = MYFIX8SEND; *typIsVarlena = false; break;
		default: abort();
	}
}

/* fmgr.c call-wrapper shims (strict-fn semantics transcribed) */
static bool
InputFunctionCallSafe(FmgrInfo *flinfo, char *str,
					  Oid typioparam, int32 typmod,
					  Node *escontext, Datum *result)
{
	LOCAL_FCINFO(fcinfo, 3);

	if (str == NULL)
	{
		*result = (Datum) 0;	/* strict fn: NULL in, NULL out, no error */
		return true;
	}
	InitFunctionCallInfoData(*fcinfo, flinfo, 3, InvalidOid, escontext, NULL);
	fcinfo->args[0].value = CStringGetDatum(str);
	fcinfo->args[0].isnull = false;
	fcinfo->args[1].value = ObjectIdGetDatum(typioparam);
	fcinfo->args[1].isnull = false;
	fcinfo->args[2].value = Int32GetDatum(typmod);
	fcinfo->args[2].isnull = false;
	pg_diff_codec_failed = false;
	*result = FunctionCallInvoke(fcinfo);
	return !pg_diff_codec_failed;
}

static char *
OutputFunctionCall(FmgrInfo *flinfo, Datum val)
{
	LOCAL_FCINFO(fcinfo, 1);

	InitFunctionCallInfoData(*fcinfo, flinfo, 1, InvalidOid, NULL, NULL);
	fcinfo->args[0].value = val;
	fcinfo->args[0].isnull = false;
	return DatumGetCString(FunctionCallInvoke(fcinfo));
}

static Datum
ReceiveFunctionCall(FmgrInfo *flinfo, StringInfo buf,
					Oid typioparam, int32 typmod)
{
	LOCAL_FCINFO(fcinfo, 3);

	if (buf == NULL)
		return (Datum) 0;		/* strict fn: NULL in, NULL out */
	InitFunctionCallInfoData(*fcinfo, flinfo, 3, InvalidOid, NULL, NULL);
	fcinfo->args[0].value = PointerGetDatum(buf);
	fcinfo->args[0].isnull = false;
	fcinfo->args[1].value = ObjectIdGetDatum(typioparam);
	fcinfo->args[1].isnull = false;
	fcinfo->args[2].value = Int32GetDatum(typmod);
	fcinfo->args[2].isnull = false;
	return FunctionCallInvoke(fcinfo);
}

static bytea *
SendFunctionCall(FmgrInfo *flinfo, Datum val)
{
	LOCAL_FCINFO(fcinfo, 1);

	InitFunctionCallInfoData(*fcinfo, flinfo, 1, InvalidOid, NULL, NULL);
	fcinfo->args[0].value = val;
	fcinfo->args[0].isnull = false;
	return (bytea *) DatumGetPointer(FunctionCallInvoke(fcinfo));
}

static Datum
FunctionCall2Coll(FmgrInfo *flinfo, Oid collation, Datum arg1, Datum arg2)
{
	LOCAL_FCINFO(fcinfo, 2);

	InitFunctionCallInfoData(*fcinfo, flinfo, 2, collation, NULL, NULL);
	fcinfo->args[0].value = arg1;
	fcinfo->args[0].isnull = false;
	fcinfo->args[1].value = arg2;
	fcinfo->args[1].isnull = false;
	return FunctionCallInvoke(fcinfo);
}

/* ---- pinned typcache entries ---- */
static TypeCacheEntry pg_diff_tce_int4;
static TypeCacheEntry pg_diff_tce_text;
static TypeCacheEntry pg_diff_tce_fake;
static TypeCacheEntry pg_diff_tce_bool;
static TypeCacheEntry pg_diff_tce_int2;
static TypeCacheEntry pg_diff_tce_int8;
static TypeCacheEntry pg_diff_tce_fix8;
static bool pg_diff_tce_ready;

static void
pg_diff_tce_init(void)
{
	if (pg_diff_tce_ready)
		return;
	pg_diff_tce_int4.type_id = INT4OID;
	fmgr_info_cxt(MYINT4CMP, &pg_diff_tce_int4.cmp_proc_finfo, NULL);
	fmgr_info_cxt(MYINT4HASH, &pg_diff_tce_int4.hash_proc_finfo, NULL);
	fmgr_info_cxt(MYINT4HASHEXT, &pg_diff_tce_int4.hash_extended_proc_finfo, NULL);
	pg_diff_tce_text.type_id = TEXTOID;
	fmgr_info_cxt(MYTEXTCMP, &pg_diff_tce_text.cmp_proc_finfo, NULL);
	fmgr_info_cxt(MYTEXTHASH, &pg_diff_tce_text.hash_proc_finfo, NULL);
	fmgr_info_cxt(MYTEXTHASHEXT, &pg_diff_tce_text.hash_extended_proc_finfo, NULL);
	fmgr_info_cxt(MYINT4EQ, &pg_diff_tce_int4.eq_opr_finfo, NULL);
	fmgr_info_cxt(MYTEXTEQ, &pg_diff_tce_text.eq_opr_finfo, NULL);
	pg_diff_tce_fake.type_id = PG_DIFF_FAKETYPE;
	/* fn_oid stays InvalidOid: drives the no-support-function error arms */
	/* bool/int2/int8/fix8: NO cmp/hash/eq support (menu types for the
	 * datum_image byval-width + fixed-byref arms; support-fn lookups on
	 * them witness the error arms on BOTH sides) */
	pg_diff_tce_bool.type_id = BOOLOID;
	pg_diff_tce_int2.type_id = INT2OID;
	pg_diff_tce_int8.type_id = INT8OID;
	pg_diff_tce_fix8.type_id = PG_DIFF_FIX8TYPE;
	pg_diff_tce_ready = true;
}

static TypeCacheEntry *
lookup_type_cache(Oid type_id, int flags)
{
	(void) flags;
	pg_diff_tce_init();
	switch (type_id)
	{
		case INT4OID: return &pg_diff_tce_int4;
		case TEXTOID: return &pg_diff_tce_text;
		case PG_DIFF_FAKETYPE: return &pg_diff_tce_fake;
		case BOOLOID: return &pg_diff_tce_bool;
		case INT2OID: return &pg_diff_tce_int2;
		case INT8OID: return &pg_diff_tce_int8;
		case PG_DIFF_FIX8TYPE: return &pg_diff_tce_fix8;
		default: abort();
	}
}

/* ---- descriptor menu (typmod = index; matches the Rust registration) ---- */
#define PG_DIFF_NDESC 7

static TupleDesc pg_diff_descs[PG_DIFF_NDESC];

static void
pg_diff_fill_att(TupleDesc td, int i, const char *name, Oid typid,
				 int16 typlen, bool byval, char align, char storage,
				 bool dropped)
{
	Form_pg_attribute a = TupleDescAttr(td, i);

	memset(a, 0, sizeof(FormData_pg_attribute));
	strncpy(a->attname.data, name, NAMEDATALEN - 1);
	a->atttypid = typid;
	a->attlen = typlen;
	a->attnum = (int16) (i + 1);
	a->atttypmod = -1;
	a->attbyval = byval;
	a->attalign = align;
	a->attstorage = storage;
	a->attisdropped = dropped;
	a->attcollation = InvalidOid;
	populate_compact_attribute(td, i);
}

static TupleDesc
pg_diff_make_desc(int natts, int32 typmod)
{
	Size		sz = offsetof(struct TupleDescData, compact_attrs)
		+ natts * sizeof(CompactAttribute)
		+ natts * sizeof(FormData_pg_attribute);
	TupleDesc	td = (TupleDesc) calloc(1, sz);	/* process-lifetime, not arena */

	td->natts = natts;
	td->tdtypeid = RECORDOID;
	td->tdtypmod = typmod;
	td->tdrefcount = -1;
	td->constr = NULL;
	return td;
}

static void
pg_diff_descs_init(void)
{
	TupleDesc	td;

	if (pg_diff_descs[0] != NULL)
		return;
	/* 0: (text, text) */
	td = pg_diff_make_desc(2, 0);
	pg_diff_fill_att(td, 0, "c1", TEXTOID, -1, false, TYPALIGN_INT, TYPSTORAGE_EXTENDED, false);
	pg_diff_fill_att(td, 1, "c2", TEXTOID, -1, false, TYPALIGN_INT, TYPSTORAGE_EXTENDED, false);
	pg_diff_descs[0] = td;
	/* 1: (int4, text) */
	td = pg_diff_make_desc(2, 1);
	pg_diff_fill_att(td, 0, "c1", INT4OID, 4, true, TYPALIGN_INT, TYPSTORAGE_PLAIN, false);
	pg_diff_fill_att(td, 1, "c2", TEXTOID, -1, false, TYPALIGN_INT, TYPSTORAGE_EXTENDED, false);
	pg_diff_descs[1] = td;
	/* 2: (text, [dropped], text) */
	td = pg_diff_make_desc(3, 2);
	pg_diff_fill_att(td, 0, "c1", TEXTOID, -1, false, TYPALIGN_INT, TYPSTORAGE_EXTENDED, false);
	pg_diff_fill_att(td, 1, "c2", InvalidOid, -1, false, TYPALIGN_INT, TYPSTORAGE_EXTENDED, true);
	pg_diff_fill_att(td, 2, "c3", TEXTOID, -1, false, TYPALIGN_INT, TYPSTORAGE_EXTENDED, false);
	pg_diff_descs[2] = td;
	/* 3: (int4, faketype) */
	td = pg_diff_make_desc(2, 3);
	pg_diff_fill_att(td, 0, "c1", INT4OID, 4, true, TYPALIGN_INT, TYPSTORAGE_PLAIN, false);
	pg_diff_fill_att(td, 1, "c2", PG_DIFF_FAKETYPE, -1, false, TYPALIGN_INT, TYPSTORAGE_EXTENDED, false);
	pg_diff_descs[3] = td;
	/* 4: (text) */
	td = pg_diff_make_desc(1, 4);
	pg_diff_fill_att(td, 0, "c1", TEXTOID, -1, false, TYPALIGN_INT, TYPSTORAGE_EXTENDED, false);
	pg_diff_descs[4] = td;
	/* 5: (bool, int2, int8) — byval widths 1/2/8 for the datum_image arms */
	td = pg_diff_make_desc(3, 5);
	pg_diff_fill_att(td, 0, "c1", BOOLOID, 1, true, TYPALIGN_CHAR, TYPSTORAGE_PLAIN, false);
	pg_diff_fill_att(td, 1, "c2", INT2OID, 2, true, TYPALIGN_SHORT, TYPSTORAGE_PLAIN, false);
	pg_diff_fill_att(td, 2, "c3", INT8OID, 8, true, TYPALIGN_DOUBLE, TYPSTORAGE_PLAIN, false);
	pg_diff_descs[5] = td;
	/* 6: (fix8, bool) — fixed-length BY-REF column */
	td = pg_diff_make_desc(2, 6);
	pg_diff_fill_att(td, 0, "c1", PG_DIFF_FIX8TYPE, 8, false, TYPALIGN_DOUBLE, TYPSTORAGE_PLAIN, false);
	pg_diff_fill_att(td, 1, "c2", BOOLOID, 1, true, TYPALIGN_CHAR, TYPSTORAGE_PLAIN, false);
	pg_diff_descs[6] = td;
}

static TupleDesc
lookup_rowtype_tupdesc(Oid type_id, int32 typmod)
{
	pg_diff_descs_init();
	if (type_id != RECORDOID || typmod < 0 || typmod >= PG_DIFF_NDESC)
		abort();				/* harness bug: unknown descriptor */
	return pg_diff_descs[typmod];
}

/* ---- rowtypes.c private structs (VERBATIM, moved above the bodies) ---- */
typedef struct ColumnIOData
{
	Oid			column_type;
	Oid			typiofunc;
	Oid			typioparam;
	bool		typisvarlena;
	FmgrInfo	proc;
} ColumnIOData;

typedef struct RecordIOData
{
	Oid			record_type;
	int32		record_typmod;
	int			ncolumns;
	ColumnIOData columns[FLEXIBLE_ARRAY_MEMBER];
} RecordIOData;

typedef struct ColumnCompareData
{
	TypeCacheEntry *typentry;	/* has everything we need, actually */
} ColumnCompareData;

typedef struct RecordCompareData
{
	int			ncolumns;		/* allocated length of columns[] */
	Oid			record1_type;
	int32		record1_typmod;
	Oid			record2_type;
	int32		record2_typmod;
	ColumnCompareData columns[FLEXIBLE_ARRAY_MEMBER];
} RecordCompareData;

/* ---- VERBATIM backend/utils/adt/rowtypes.c: record_in [static-prefixed] ---- */
static Datum
record_in(PG_FUNCTION_ARGS)
{
	char	   *string = PG_GETARG_CSTRING(0);
	Oid			tupType = PG_GETARG_OID(1);
	int32		tupTypmod = PG_GETARG_INT32(2);
	Node	   *escontext = fcinfo->context;
	HeapTupleHeader result;
	TupleDesc	tupdesc;
	HeapTuple	tuple;
	RecordIOData *my_extra;
	bool		needComma = false;
	int			ncolumns;
	int			i;
	char	   *ptr;
	Datum	   *values;
	bool	   *nulls;
	StringInfoData buf;

	check_stack_depth();		/* recurses for record-type columns */

	/*
	 * Give a friendly error message if we did not get enough info to identify
	 * the target record type.  (lookup_rowtype_tupdesc would fail anyway, but
	 * with a non-user-friendly message.)  In ordinary SQL usage, we'll get -1
	 * for typmod, since composite types and RECORD have no type modifiers at
	 * the SQL level, and thus must fail for RECORD.  However some callers can
	 * supply a valid typmod, and then we can do something useful for RECORD.
	 */
	if (tupType == RECORDOID && tupTypmod < 0)
		ereturn(escontext, (Datum) 0,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("input of anonymous composite types is not implemented")));

	/*
	 * This comes from the composite type's pg_type.oid and stores system oids
	 * in user tables, specifically DatumTupleFields. This oid must be
	 * preserved by binary upgrades.
	 */
	tupdesc = lookup_rowtype_tupdesc(tupType, tupTypmod);
	ncolumns = tupdesc->natts;

	/*
	 * We arrange to look up the needed I/O info just once per series of
	 * calls, assuming the record type doesn't change underneath us.
	 */
	my_extra = (RecordIOData *) fcinfo->flinfo->fn_extra;
	if (my_extra == NULL ||
		my_extra->ncolumns != ncolumns)
	{
		fcinfo->flinfo->fn_extra =
			MemoryContextAlloc(fcinfo->flinfo->fn_mcxt,
							   offsetof(RecordIOData, columns) +
							   ncolumns * sizeof(ColumnIOData));
		my_extra = (RecordIOData *) fcinfo->flinfo->fn_extra;
		my_extra->record_type = InvalidOid;
		my_extra->record_typmod = 0;
	}

	if (my_extra->record_type != tupType ||
		my_extra->record_typmod != tupTypmod)
	{
		MemSet(my_extra, 0,
			   offsetof(RecordIOData, columns) +
			   ncolumns * sizeof(ColumnIOData));
		my_extra->record_type = tupType;
		my_extra->record_typmod = tupTypmod;
		my_extra->ncolumns = ncolumns;
	}

	values = (Datum *) palloc(ncolumns * sizeof(Datum));
	nulls = (bool *) palloc(ncolumns * sizeof(bool));

	/*
	 * Scan the string.  We use "buf" to accumulate the de-quoted data for
	 * each column, which is then fed to the appropriate input converter.
	 */
	ptr = string;
	/* Allow leading whitespace */
	while (*ptr && isspace((unsigned char) *ptr))
		ptr++;
	if (*ptr++ != '(')
	{
		errsave(escontext,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("malformed record literal: \"%s\"", string),
				 errdetail("Missing left parenthesis.")));
		goto fail;
	}

	initStringInfo(&buf);

	for (i = 0; i < ncolumns; i++)
	{
		Form_pg_attribute att = TupleDescAttr(tupdesc, i);
		ColumnIOData *column_info = &my_extra->columns[i];
		Oid			column_type = att->atttypid;
		char	   *column_data;

		/* Ignore dropped columns in datatype, but fill with nulls */
		if (att->attisdropped)
		{
			values[i] = (Datum) 0;
			nulls[i] = true;
			continue;
		}

		if (needComma)
		{
			/* Skip comma that separates prior field from this one */
			if (*ptr == ',')
				ptr++;
			else
				/* *ptr must be ')' */
			{
				errsave(escontext,
						(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
						 errmsg("malformed record literal: \"%s\"", string),
						 errdetail("Too few columns.")));
				goto fail;
			}
		}

		/* Check for null: completely empty input means null */
		if (*ptr == ',' || *ptr == ')')
		{
			column_data = NULL;
			nulls[i] = true;
		}
		else
		{
			/* Extract string for this column */
			bool		inquote = false;

			resetStringInfo(&buf);
			while (inquote || !(*ptr == ',' || *ptr == ')'))
			{
				char		ch = *ptr++;

				if (ch == '\0')
				{
					errsave(escontext,
							(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
							 errmsg("malformed record literal: \"%s\"",
									string),
							 errdetail("Unexpected end of input.")));
					goto fail;
				}
				if (ch == '\\')
				{
					if (*ptr == '\0')
					{
						errsave(escontext,
								(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
								 errmsg("malformed record literal: \"%s\"",
										string),
								 errdetail("Unexpected end of input.")));
						goto fail;
					}
					appendStringInfoChar(&buf, *ptr++);
				}
				else if (ch == '"')
				{
					if (!inquote)
						inquote = true;
					else if (*ptr == '"')
					{
						/* doubled quote within quote sequence */
						appendStringInfoChar(&buf, *ptr++);
					}
					else
						inquote = false;
				}
				else
					appendStringInfoChar(&buf, ch);
			}

			column_data = buf.data;
			nulls[i] = false;
		}

		/*
		 * Convert the column value
		 */
		if (column_info->column_type != column_type)
		{
			getTypeInputInfo(column_type,
							 &column_info->typiofunc,
							 &column_info->typioparam);
			fmgr_info_cxt(column_info->typiofunc, &column_info->proc,
						  fcinfo->flinfo->fn_mcxt);
			column_info->column_type = column_type;
		}

		if (!InputFunctionCallSafe(&column_info->proc,
								   column_data,
								   column_info->typioparam,
								   att->atttypmod,
								   escontext,
								   &values[i]))
			goto fail;

		/*
		 * Prep for next column
		 */
		needComma = true;
	}

	if (*ptr++ != ')')
	{
		errsave(escontext,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("malformed record literal: \"%s\"", string),
				 errdetail("Too many columns.")));
		goto fail;
	}
	/* Allow trailing whitespace */
	while (*ptr && isspace((unsigned char) *ptr))
		ptr++;
	if (*ptr)
	{
		errsave(escontext,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("malformed record literal: \"%s\"", string),
				 errdetail("Junk after right parenthesis.")));
		goto fail;
	}

	tuple = heap_form_tuple(tupdesc, values, nulls);

	/*
	 * We cannot return tuple->t_data because heap_form_tuple allocates it as
	 * part of a larger chunk, and our caller may expect to be able to pfree
	 * our result.  So must copy the info into a new palloc chunk.
	 */
	result = (HeapTupleHeader) palloc(tuple->t_len);
	memcpy(result, tuple->t_data, tuple->t_len);

	heap_freetuple(tuple);
	pfree(buf.data);
	pfree(values);
	pfree(nulls);
	ReleaseTupleDesc(tupdesc);

	PG_RETURN_HEAPTUPLEHEADER(result);

	/* exit here once we've done lookup_rowtype_tupdesc */
fail:
	ReleaseTupleDesc(tupdesc);
	PG_RETURN_NULL();
}

/* ---- VERBATIM backend/utils/adt/rowtypes.c: record_out [static-prefixed] ---- */
static Datum
record_out(PG_FUNCTION_ARGS)
{
	HeapTupleHeader rec = PG_GETARG_HEAPTUPLEHEADER(0);
	Oid			tupType;
	int32		tupTypmod;
	TupleDesc	tupdesc;
	HeapTupleData tuple;
	RecordIOData *my_extra;
	bool		needComma = false;
	int			ncolumns;
	int			i;
	Datum	   *values;
	bool	   *nulls;
	StringInfoData buf;

	check_stack_depth();		/* recurses for record-type columns */

	/* Extract type info from the tuple itself */
	tupType = HeapTupleHeaderGetTypeId(rec);
	tupTypmod = HeapTupleHeaderGetTypMod(rec);
	tupdesc = lookup_rowtype_tupdesc(tupType, tupTypmod);
	ncolumns = tupdesc->natts;

	/* Build a temporary HeapTuple control structure */
	tuple.t_len = HeapTupleHeaderGetDatumLength(rec);
	ItemPointerSetInvalid(&(tuple.t_self));
	tuple.t_tableOid = InvalidOid;
	tuple.t_data = rec;

	/*
	 * We arrange to look up the needed I/O info just once per series of
	 * calls, assuming the record type doesn't change underneath us.
	 */
	my_extra = (RecordIOData *) fcinfo->flinfo->fn_extra;
	if (my_extra == NULL ||
		my_extra->ncolumns != ncolumns)
	{
		fcinfo->flinfo->fn_extra =
			MemoryContextAlloc(fcinfo->flinfo->fn_mcxt,
							   offsetof(RecordIOData, columns) +
							   ncolumns * sizeof(ColumnIOData));
		my_extra = (RecordIOData *) fcinfo->flinfo->fn_extra;
		my_extra->record_type = InvalidOid;
		my_extra->record_typmod = 0;
	}

	if (my_extra->record_type != tupType ||
		my_extra->record_typmod != tupTypmod)
	{
		MemSet(my_extra, 0,
			   offsetof(RecordIOData, columns) +
			   ncolumns * sizeof(ColumnIOData));
		my_extra->record_type = tupType;
		my_extra->record_typmod = tupTypmod;
		my_extra->ncolumns = ncolumns;
	}

	values = (Datum *) palloc(ncolumns * sizeof(Datum));
	nulls = (bool *) palloc(ncolumns * sizeof(bool));

	/* Break down the tuple into fields */
	heap_deform_tuple(&tuple, tupdesc, values, nulls);

	/* And build the result string */
	initStringInfo(&buf);

	appendStringInfoChar(&buf, '(');

	for (i = 0; i < ncolumns; i++)
	{
		Form_pg_attribute att = TupleDescAttr(tupdesc, i);
		ColumnIOData *column_info = &my_extra->columns[i];
		Oid			column_type = att->atttypid;
		Datum		attr;
		char	   *value;
		char	   *tmp;
		bool		nq;

		/* Ignore dropped columns in datatype */
		if (att->attisdropped)
			continue;

		if (needComma)
			appendStringInfoChar(&buf, ',');
		needComma = true;

		if (nulls[i])
		{
			/* emit nothing... */
			continue;
		}

		/*
		 * Convert the column value to text
		 */
		if (column_info->column_type != column_type)
		{
			getTypeOutputInfo(column_type,
							  &column_info->typiofunc,
							  &column_info->typisvarlena);
			fmgr_info_cxt(column_info->typiofunc, &column_info->proc,
						  fcinfo->flinfo->fn_mcxt);
			column_info->column_type = column_type;
		}

		attr = values[i];
		value = OutputFunctionCall(&column_info->proc, attr);

		/* Detect whether we need double quotes for this value */
		nq = (value[0] == '\0');	/* force quotes for empty string */
		for (tmp = value; *tmp; tmp++)
		{
			char		ch = *tmp;

			if (ch == '"' || ch == '\\' ||
				ch == '(' || ch == ')' || ch == ',' ||
				isspace((unsigned char) ch))
			{
				nq = true;
				break;
			}
		}

		/* And emit the string */
		if (nq)
			appendStringInfoCharMacro(&buf, '"');
		for (tmp = value; *tmp; tmp++)
		{
			char		ch = *tmp;

			if (ch == '"' || ch == '\\')
				appendStringInfoCharMacro(&buf, ch);
			appendStringInfoCharMacro(&buf, ch);
		}
		if (nq)
			appendStringInfoCharMacro(&buf, '"');
	}

	appendStringInfoChar(&buf, ')');

	pfree(values);
	pfree(nulls);
	ReleaseTupleDesc(tupdesc);

	PG_RETURN_CSTRING(buf.data);
}

/* ---- VERBATIM backend/utils/adt/rowtypes.c: record_recv [static-prefixed] ---- */
static Datum
record_recv(PG_FUNCTION_ARGS)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);
	Oid			tupType = PG_GETARG_OID(1);
	int32		tupTypmod = PG_GETARG_INT32(2);
	HeapTupleHeader result;
	TupleDesc	tupdesc;
	HeapTuple	tuple;
	RecordIOData *my_extra;
	int			ncolumns;
	int			usercols;
	int			validcols;
	int			i;
	Datum	   *values;
	bool	   *nulls;

	check_stack_depth();		/* recurses for record-type columns */

	/*
	 * Give a friendly error message if we did not get enough info to identify
	 * the target record type.  (lookup_rowtype_tupdesc would fail anyway, but
	 * with a non-user-friendly message.)  In ordinary SQL usage, we'll get -1
	 * for typmod, since composite types and RECORD have no type modifiers at
	 * the SQL level, and thus must fail for RECORD.  However some callers can
	 * supply a valid typmod, and then we can do something useful for RECORD.
	 */
	if (tupType == RECORDOID && tupTypmod < 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("input of anonymous composite types is not implemented")));

	tupdesc = lookup_rowtype_tupdesc(tupType, tupTypmod);
	ncolumns = tupdesc->natts;

	/*
	 * We arrange to look up the needed I/O info just once per series of
	 * calls, assuming the record type doesn't change underneath us.
	 */
	my_extra = (RecordIOData *) fcinfo->flinfo->fn_extra;
	if (my_extra == NULL ||
		my_extra->ncolumns != ncolumns)
	{
		fcinfo->flinfo->fn_extra =
			MemoryContextAlloc(fcinfo->flinfo->fn_mcxt,
							   offsetof(RecordIOData, columns) +
							   ncolumns * sizeof(ColumnIOData));
		my_extra = (RecordIOData *) fcinfo->flinfo->fn_extra;
		my_extra->record_type = InvalidOid;
		my_extra->record_typmod = 0;
	}

	if (my_extra->record_type != tupType ||
		my_extra->record_typmod != tupTypmod)
	{
		MemSet(my_extra, 0,
			   offsetof(RecordIOData, columns) +
			   ncolumns * sizeof(ColumnIOData));
		my_extra->record_type = tupType;
		my_extra->record_typmod = tupTypmod;
		my_extra->ncolumns = ncolumns;
	}

	values = (Datum *) palloc(ncolumns * sizeof(Datum));
	nulls = (bool *) palloc(ncolumns * sizeof(bool));

	/* Fetch number of columns user thinks it has */
	usercols = pq_getmsgint(buf, 4);

	/* Need to scan to count nondeleted columns */
	validcols = 0;
	for (i = 0; i < ncolumns; i++)
	{
		if (!TupleDescAttr(tupdesc, i)->attisdropped)
			validcols++;
	}
	if (usercols != validcols)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("wrong number of columns: %d, expected %d",
						usercols, validcols)));

	/* Process each column */
	for (i = 0; i < ncolumns; i++)
	{
		Form_pg_attribute att = TupleDescAttr(tupdesc, i);
		ColumnIOData *column_info = &my_extra->columns[i];
		Oid			column_type = att->atttypid;
		Oid			coltypoid;
		int			itemlen;
		StringInfoData item_buf;
		StringInfo	bufptr;

		/* Ignore dropped columns in datatype, but fill with nulls */
		if (att->attisdropped)
		{
			values[i] = (Datum) 0;
			nulls[i] = true;
			continue;
		}

		/* Check column type recorded in the data */
		coltypoid = pq_getmsgint(buf, sizeof(Oid));

		/*
		 * From a security standpoint, it doesn't matter whether the input's
		 * column type matches what we expect: the column type's receive
		 * function has to be robust enough to cope with invalid data.
		 * However, from a user-friendliness standpoint, it's nicer to
		 * complain about type mismatches than to throw "improper binary
		 * format" errors.  But there's a problem: only built-in types have
		 * OIDs that are stable enough to believe that a mismatch is a real
		 * issue.  So complain only if both OIDs are in the built-in range.
		 * Otherwise, carry on with the column type we "should" be getting.
		 */
		if (coltypoid != column_type &&
			coltypoid < FirstGenbkiObjectId &&
			column_type < FirstGenbkiObjectId)
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("binary data has type %u (%s) instead of expected %u (%s) in record column %d",
							coltypoid,
							format_type_extended(coltypoid, -1,
												 FORMAT_TYPE_ALLOW_INVALID),
							column_type,
							format_type_extended(column_type, -1,
												 FORMAT_TYPE_ALLOW_INVALID),
							i + 1)));

		/* Get and check the item length */
		itemlen = pq_getmsgint(buf, 4);
		if (itemlen < -1 || itemlen > (buf->len - buf->cursor))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
					 errmsg("insufficient data left in message")));

		if (itemlen == -1)
		{
			/* -1 length means NULL */
			bufptr = NULL;
			nulls[i] = true;
		}
		else
		{
			char	   *strbuff;

			/*
			 * Rather than copying data around, we just initialize a
			 * StringInfo pointing to the correct portion of the message
			 * buffer.
			 */
			strbuff = &buf->data[buf->cursor];
			buf->cursor += itemlen;
			initReadOnlyStringInfo(&item_buf, strbuff, itemlen);

			bufptr = &item_buf;
			nulls[i] = false;
		}

		/* Now call the column's receiveproc */
		if (column_info->column_type != column_type)
		{
			getTypeBinaryInputInfo(column_type,
								   &column_info->typiofunc,
								   &column_info->typioparam);
			fmgr_info_cxt(column_info->typiofunc, &column_info->proc,
						  fcinfo->flinfo->fn_mcxt);
			column_info->column_type = column_type;
		}

		values[i] = ReceiveFunctionCall(&column_info->proc,
										bufptr,
										column_info->typioparam,
										att->atttypmod);

		if (bufptr)
		{
			/* Trouble if it didn't eat the whole buffer */
			if (item_buf.cursor != itemlen)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
						 errmsg("improper binary format in record column %d",
								i + 1)));
		}
	}

	tuple = heap_form_tuple(tupdesc, values, nulls);

	/*
	 * We cannot return tuple->t_data because heap_form_tuple allocates it as
	 * part of a larger chunk, and our caller may expect to be able to pfree
	 * our result.  So must copy the info into a new palloc chunk.
	 */
	result = (HeapTupleHeader) palloc(tuple->t_len);
	memcpy(result, tuple->t_data, tuple->t_len);

	heap_freetuple(tuple);
	pfree(values);
	pfree(nulls);
	ReleaseTupleDesc(tupdesc);

	PG_RETURN_HEAPTUPLEHEADER(result);
}

/* ---- VERBATIM backend/utils/adt/rowtypes.c: record_send [static-prefixed] ---- */
static Datum
record_send(PG_FUNCTION_ARGS)
{
	HeapTupleHeader rec = PG_GETARG_HEAPTUPLEHEADER(0);
	Oid			tupType;
	int32		tupTypmod;
	TupleDesc	tupdesc;
	HeapTupleData tuple;
	RecordIOData *my_extra;
	int			ncolumns;
	int			validcols;
	int			i;
	Datum	   *values;
	bool	   *nulls;
	StringInfoData buf;

	check_stack_depth();		/* recurses for record-type columns */

	/* Extract type info from the tuple itself */
	tupType = HeapTupleHeaderGetTypeId(rec);
	tupTypmod = HeapTupleHeaderGetTypMod(rec);
	tupdesc = lookup_rowtype_tupdesc(tupType, tupTypmod);
	ncolumns = tupdesc->natts;

	/* Build a temporary HeapTuple control structure */
	tuple.t_len = HeapTupleHeaderGetDatumLength(rec);
	ItemPointerSetInvalid(&(tuple.t_self));
	tuple.t_tableOid = InvalidOid;
	tuple.t_data = rec;

	/*
	 * We arrange to look up the needed I/O info just once per series of
	 * calls, assuming the record type doesn't change underneath us.
	 */
	my_extra = (RecordIOData *) fcinfo->flinfo->fn_extra;
	if (my_extra == NULL ||
		my_extra->ncolumns != ncolumns)
	{
		fcinfo->flinfo->fn_extra =
			MemoryContextAlloc(fcinfo->flinfo->fn_mcxt,
							   offsetof(RecordIOData, columns) +
							   ncolumns * sizeof(ColumnIOData));
		my_extra = (RecordIOData *) fcinfo->flinfo->fn_extra;
		my_extra->record_type = InvalidOid;
		my_extra->record_typmod = 0;
	}

	if (my_extra->record_type != tupType ||
		my_extra->record_typmod != tupTypmod)
	{
		MemSet(my_extra, 0,
			   offsetof(RecordIOData, columns) +
			   ncolumns * sizeof(ColumnIOData));
		my_extra->record_type = tupType;
		my_extra->record_typmod = tupTypmod;
		my_extra->ncolumns = ncolumns;
	}

	values = (Datum *) palloc(ncolumns * sizeof(Datum));
	nulls = (bool *) palloc(ncolumns * sizeof(bool));

	/* Break down the tuple into fields */
	heap_deform_tuple(&tuple, tupdesc, values, nulls);

	/* And build the result string */
	pq_begintypsend(&buf);

	/* Need to scan to count nondeleted columns */
	validcols = 0;
	for (i = 0; i < ncolumns; i++)
	{
		if (!TupleDescAttr(tupdesc, i)->attisdropped)
			validcols++;
	}
	pq_sendint32(&buf, validcols);

	for (i = 0; i < ncolumns; i++)
	{
		Form_pg_attribute att = TupleDescAttr(tupdesc, i);
		ColumnIOData *column_info = &my_extra->columns[i];
		Oid			column_type = att->atttypid;
		Datum		attr;
		bytea	   *outputbytes;

		/* Ignore dropped columns in datatype */
		if (att->attisdropped)
			continue;

		pq_sendint32(&buf, column_type);

		if (nulls[i])
		{
			/* emit -1 data length to signify a NULL */
			pq_sendint32(&buf, -1);
			continue;
		}

		/*
		 * Convert the column value to binary
		 */
		if (column_info->column_type != column_type)
		{
			getTypeBinaryOutputInfo(column_type,
									&column_info->typiofunc,
									&column_info->typisvarlena);
			fmgr_info_cxt(column_info->typiofunc, &column_info->proc,
						  fcinfo->flinfo->fn_mcxt);
			column_info->column_type = column_type;
		}

		attr = values[i];
		outputbytes = SendFunctionCall(&column_info->proc, attr);
		pq_sendint32(&buf, VARSIZE(outputbytes) - VARHDRSZ);
		pq_sendbytes(&buf, VARDATA(outputbytes),
					 VARSIZE(outputbytes) - VARHDRSZ);
	}

	pfree(values);
	pfree(nulls);
	ReleaseTupleDesc(tupdesc);

	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

/* ---- VERBATIM backend/utils/adt/rowtypes.c: record_cmp ---- */
static int
record_cmp(FunctionCallInfo fcinfo)
{
	HeapTupleHeader record1 = PG_GETARG_HEAPTUPLEHEADER(0);
	HeapTupleHeader record2 = PG_GETARG_HEAPTUPLEHEADER(1);
	int			result = 0;
	Oid			tupType1;
	Oid			tupType2;
	int32		tupTypmod1;
	int32		tupTypmod2;
	TupleDesc	tupdesc1;
	TupleDesc	tupdesc2;
	HeapTupleData tuple1;
	HeapTupleData tuple2;
	int			ncolumns1;
	int			ncolumns2;
	RecordCompareData *my_extra;
	int			ncols;
	Datum	   *values1;
	Datum	   *values2;
	bool	   *nulls1;
	bool	   *nulls2;
	int			i1;
	int			i2;
	int			j;

	check_stack_depth();		/* recurses for record-type columns */

	/* Extract type info from the tuples */
	tupType1 = HeapTupleHeaderGetTypeId(record1);
	tupTypmod1 = HeapTupleHeaderGetTypMod(record1);
	tupdesc1 = lookup_rowtype_tupdesc(tupType1, tupTypmod1);
	ncolumns1 = tupdesc1->natts;
	tupType2 = HeapTupleHeaderGetTypeId(record2);
	tupTypmod2 = HeapTupleHeaderGetTypMod(record2);
	tupdesc2 = lookup_rowtype_tupdesc(tupType2, tupTypmod2);
	ncolumns2 = tupdesc2->natts;

	/* Build temporary HeapTuple control structures */
	tuple1.t_len = HeapTupleHeaderGetDatumLength(record1);
	ItemPointerSetInvalid(&(tuple1.t_self));
	tuple1.t_tableOid = InvalidOid;
	tuple1.t_data = record1;
	tuple2.t_len = HeapTupleHeaderGetDatumLength(record2);
	ItemPointerSetInvalid(&(tuple2.t_self));
	tuple2.t_tableOid = InvalidOid;
	tuple2.t_data = record2;

	/*
	 * We arrange to look up the needed comparison info just once per series
	 * of calls, assuming the record types don't change underneath us.
	 */
	ncols = Max(ncolumns1, ncolumns2);
	my_extra = (RecordCompareData *) fcinfo->flinfo->fn_extra;
	if (my_extra == NULL ||
		my_extra->ncolumns < ncols)
	{
		fcinfo->flinfo->fn_extra =
			MemoryContextAlloc(fcinfo->flinfo->fn_mcxt,
							   offsetof(RecordCompareData, columns) +
							   ncols * sizeof(ColumnCompareData));
		my_extra = (RecordCompareData *) fcinfo->flinfo->fn_extra;
		my_extra->ncolumns = ncols;
		my_extra->record1_type = InvalidOid;
		my_extra->record1_typmod = 0;
		my_extra->record2_type = InvalidOid;
		my_extra->record2_typmod = 0;
	}

	if (my_extra->record1_type != tupType1 ||
		my_extra->record1_typmod != tupTypmod1 ||
		my_extra->record2_type != tupType2 ||
		my_extra->record2_typmod != tupTypmod2)
	{
		MemSet(my_extra->columns, 0, ncols * sizeof(ColumnCompareData));
		my_extra->record1_type = tupType1;
		my_extra->record1_typmod = tupTypmod1;
		my_extra->record2_type = tupType2;
		my_extra->record2_typmod = tupTypmod2;
	}

	/* Break down the tuples into fields */
	values1 = (Datum *) palloc(ncolumns1 * sizeof(Datum));
	nulls1 = (bool *) palloc(ncolumns1 * sizeof(bool));
	heap_deform_tuple(&tuple1, tupdesc1, values1, nulls1);
	values2 = (Datum *) palloc(ncolumns2 * sizeof(Datum));
	nulls2 = (bool *) palloc(ncolumns2 * sizeof(bool));
	heap_deform_tuple(&tuple2, tupdesc2, values2, nulls2);

	/*
	 * Scan corresponding columns, allowing for dropped columns in different
	 * places in the two rows.  i1 and i2 are physical column indexes, j is
	 * the logical column index.
	 */
	i1 = i2 = j = 0;
	while (i1 < ncolumns1 || i2 < ncolumns2)
	{
		Form_pg_attribute att1;
		Form_pg_attribute att2;
		TypeCacheEntry *typentry;
		Oid			collation;

		/*
		 * Skip dropped columns
		 */
		if (i1 < ncolumns1 && TupleDescAttr(tupdesc1, i1)->attisdropped)
		{
			i1++;
			continue;
		}
		if (i2 < ncolumns2 && TupleDescAttr(tupdesc2, i2)->attisdropped)
		{
			i2++;
			continue;
		}
		if (i1 >= ncolumns1 || i2 >= ncolumns2)
			break;				/* we'll deal with mismatch below loop */

		att1 = TupleDescAttr(tupdesc1, i1);
		att2 = TupleDescAttr(tupdesc2, i2);

		/*
		 * Have two matching columns, they must be same type
		 */
		if (att1->atttypid != att2->atttypid)
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("cannot compare dissimilar column types %s and %s at record column %d",
							format_type_be(att1->atttypid),
							format_type_be(att2->atttypid),
							j + 1)));

		/*
		 * If they're not same collation, we don't complain here, but the
		 * comparison function might.
		 */
		collation = att1->attcollation;
		if (collation != att2->attcollation)
			collation = InvalidOid;

		/*
		 * Lookup the comparison function if not done already
		 */
		typentry = my_extra->columns[j].typentry;
		if (typentry == NULL ||
			typentry->type_id != att1->atttypid)
		{
			typentry = lookup_type_cache(att1->atttypid,
										 TYPECACHE_CMP_PROC_FINFO);
			if (!OidIsValid(typentry->cmp_proc_finfo.fn_oid))
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_FUNCTION),
						 errmsg("could not identify a comparison function for type %s",
								format_type_be(typentry->type_id))));
			my_extra->columns[j].typentry = typentry;
		}

		/*
		 * We consider two NULLs equal; NULL > not-NULL.
		 */
		if (!nulls1[i1] || !nulls2[i2])
		{
			LOCAL_FCINFO(locfcinfo, 2);
			int32		cmpresult;

			if (nulls1[i1])
			{
				/* arg1 is greater than arg2 */
				result = 1;
				break;
			}
			if (nulls2[i2])
			{
				/* arg1 is less than arg2 */
				result = -1;
				break;
			}

			/* Compare the pair of elements */
			InitFunctionCallInfoData(*locfcinfo, &typentry->cmp_proc_finfo, 2,
									 collation, NULL, NULL);
			locfcinfo->args[0].value = values1[i1];
			locfcinfo->args[0].isnull = false;
			locfcinfo->args[1].value = values2[i2];
			locfcinfo->args[1].isnull = false;
			cmpresult = DatumGetInt32(FunctionCallInvoke(locfcinfo));

			/* We don't expect comparison support functions to return null */
			Assert(!locfcinfo->isnull);

			if (cmpresult < 0)
			{
				/* arg1 is less than arg2 */
				result = -1;
				break;
			}
			else if (cmpresult > 0)
			{
				/* arg1 is greater than arg2 */
				result = 1;
				break;
			}
		}

		/* equal, so continue to next column */
		i1++, i2++, j++;
	}

	/*
	 * If we didn't break out of the loop early, check for column count
	 * mismatch.  (We do not report such mismatch if we found unequal column
	 * values; is that a feature or a bug?)
	 */
	if (result == 0)
	{
		if (i1 != ncolumns1 || i2 != ncolumns2)
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("cannot compare record types with different numbers of columns")));
	}

	pfree(values1);
	pfree(nulls1);
	pfree(values2);
	pfree(nulls2);
	ReleaseTupleDesc(tupdesc1);
	ReleaseTupleDesc(tupdesc2);

	/* Avoid leaking memory when handed toasted input. */
	PG_FREE_IF_COPY(record1, 0);
	PG_FREE_IF_COPY(record2, 1);

	return result;
}

/* ---- VERBATIM backend/utils/adt/rowtypes.c: record_larger [static-prefixed] ---- */
static Datum
record_larger(PG_FUNCTION_ARGS)
{
	if (record_cmp(fcinfo) > 0)
		PG_RETURN_DATUM(PG_GETARG_DATUM(0));
	else
		PG_RETURN_DATUM(PG_GETARG_DATUM(1));
}

/* ---- VERBATIM backend/utils/adt/rowtypes.c: record_smaller [static-prefixed] ---- */
static Datum
record_smaller(PG_FUNCTION_ARGS)
{
	if (record_cmp(fcinfo) < 0)
		PG_RETURN_DATUM(PG_GETARG_DATUM(0));
	else
		PG_RETURN_DATUM(PG_GETARG_DATUM(1));
}

/* ---- VERBATIM backend/utils/adt/rowtypes.c: record_image_cmp ---- */
static int
record_image_cmp(FunctionCallInfo fcinfo)
{
	HeapTupleHeader record1 = PG_GETARG_HEAPTUPLEHEADER(0);
	HeapTupleHeader record2 = PG_GETARG_HEAPTUPLEHEADER(1);
	int			result = 0;
	Oid			tupType1;
	Oid			tupType2;
	int32		tupTypmod1;
	int32		tupTypmod2;
	TupleDesc	tupdesc1;
	TupleDesc	tupdesc2;
	HeapTupleData tuple1;
	HeapTupleData tuple2;
	int			ncolumns1;
	int			ncolumns2;
	RecordCompareData *my_extra;
	int			ncols;
	Datum	   *values1;
	Datum	   *values2;
	bool	   *nulls1;
	bool	   *nulls2;
	int			i1;
	int			i2;
	int			j;

	/* Extract type info from the tuples */
	tupType1 = HeapTupleHeaderGetTypeId(record1);
	tupTypmod1 = HeapTupleHeaderGetTypMod(record1);
	tupdesc1 = lookup_rowtype_tupdesc(tupType1, tupTypmod1);
	ncolumns1 = tupdesc1->natts;
	tupType2 = HeapTupleHeaderGetTypeId(record2);
	tupTypmod2 = HeapTupleHeaderGetTypMod(record2);
	tupdesc2 = lookup_rowtype_tupdesc(tupType2, tupTypmod2);
	ncolumns2 = tupdesc2->natts;

	/* Build temporary HeapTuple control structures */
	tuple1.t_len = HeapTupleHeaderGetDatumLength(record1);
	ItemPointerSetInvalid(&(tuple1.t_self));
	tuple1.t_tableOid = InvalidOid;
	tuple1.t_data = record1;
	tuple2.t_len = HeapTupleHeaderGetDatumLength(record2);
	ItemPointerSetInvalid(&(tuple2.t_self));
	tuple2.t_tableOid = InvalidOid;
	tuple2.t_data = record2;

	/*
	 * We arrange to look up the needed comparison info just once per series
	 * of calls, assuming the record types don't change underneath us.
	 */
	ncols = Max(ncolumns1, ncolumns2);
	my_extra = (RecordCompareData *) fcinfo->flinfo->fn_extra;
	if (my_extra == NULL ||
		my_extra->ncolumns < ncols)
	{
		fcinfo->flinfo->fn_extra =
			MemoryContextAlloc(fcinfo->flinfo->fn_mcxt,
							   offsetof(RecordCompareData, columns) +
							   ncols * sizeof(ColumnCompareData));
		my_extra = (RecordCompareData *) fcinfo->flinfo->fn_extra;
		my_extra->ncolumns = ncols;
		my_extra->record1_type = InvalidOid;
		my_extra->record1_typmod = 0;
		my_extra->record2_type = InvalidOid;
		my_extra->record2_typmod = 0;
	}

	if (my_extra->record1_type != tupType1 ||
		my_extra->record1_typmod != tupTypmod1 ||
		my_extra->record2_type != tupType2 ||
		my_extra->record2_typmod != tupTypmod2)
	{
		MemSet(my_extra->columns, 0, ncols * sizeof(ColumnCompareData));
		my_extra->record1_type = tupType1;
		my_extra->record1_typmod = tupTypmod1;
		my_extra->record2_type = tupType2;
		my_extra->record2_typmod = tupTypmod2;
	}

	/* Break down the tuples into fields */
	values1 = (Datum *) palloc(ncolumns1 * sizeof(Datum));
	nulls1 = (bool *) palloc(ncolumns1 * sizeof(bool));
	heap_deform_tuple(&tuple1, tupdesc1, values1, nulls1);
	values2 = (Datum *) palloc(ncolumns2 * sizeof(Datum));
	nulls2 = (bool *) palloc(ncolumns2 * sizeof(bool));
	heap_deform_tuple(&tuple2, tupdesc2, values2, nulls2);

	/*
	 * Scan corresponding columns, allowing for dropped columns in different
	 * places in the two rows.  i1 and i2 are physical column indexes, j is
	 * the logical column index.
	 */
	i1 = i2 = j = 0;
	while (i1 < ncolumns1 || i2 < ncolumns2)
	{
		Form_pg_attribute att1;
		Form_pg_attribute att2;

		/*
		 * Skip dropped columns
		 */
		if (i1 < ncolumns1 && TupleDescAttr(tupdesc1, i1)->attisdropped)
		{
			i1++;
			continue;
		}
		if (i2 < ncolumns2 && TupleDescAttr(tupdesc2, i2)->attisdropped)
		{
			i2++;
			continue;
		}
		if (i1 >= ncolumns1 || i2 >= ncolumns2)
			break;				/* we'll deal with mismatch below loop */

		att1 = TupleDescAttr(tupdesc1, i1);
		att2 = TupleDescAttr(tupdesc2, i2);

		/*
		 * Have two matching columns, they must be same type
		 */
		if (att1->atttypid != att2->atttypid)
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("cannot compare dissimilar column types %s and %s at record column %d",
							format_type_be(att1->atttypid),
							format_type_be(att2->atttypid),
							j + 1)));

		/*
		 * The same type should have the same length (or both should be
		 * variable).
		 */
		Assert(att1->attlen == att2->attlen);

		/*
		 * We consider two NULLs equal; NULL > not-NULL.
		 */
		if (!nulls1[i1] || !nulls2[i2])
		{
			int			cmpresult = 0;

			if (nulls1[i1])
			{
				/* arg1 is greater than arg2 */
				result = 1;
				break;
			}
			if (nulls2[i2])
			{
				/* arg1 is less than arg2 */
				result = -1;
				break;
			}

			/* Compare the pair of elements */
			if (att1->attbyval)
			{
				if (values1[i1] != values2[i2])
					cmpresult = (values1[i1] < values2[i2]) ? -1 : 1;
			}
			else if (att1->attlen > 0)
			{
				cmpresult = memcmp(DatumGetPointer(values1[i1]),
								   DatumGetPointer(values2[i2]),
								   att1->attlen);
			}
			else if (att1->attlen == -1)
			{
				Size		len1,
							len2;
				struct varlena *arg1val;
				struct varlena *arg2val;

				len1 = toast_raw_datum_size(values1[i1]);
				len2 = toast_raw_datum_size(values2[i2]);
				arg1val = PG_DETOAST_DATUM_PACKED(values1[i1]);
				arg2val = PG_DETOAST_DATUM_PACKED(values2[i2]);

				cmpresult = memcmp(VARDATA_ANY(arg1val),
								   VARDATA_ANY(arg2val),
								   Min(len1, len2) - VARHDRSZ);
				if ((cmpresult == 0) && (len1 != len2))
					cmpresult = (len1 < len2) ? -1 : 1;

				if ((Pointer) arg1val != (Pointer) values1[i1])
					pfree(arg1val);
				if ((Pointer) arg2val != (Pointer) values2[i2])
					pfree(arg2val);
			}
			else
				elog(ERROR, "unexpected attlen: %d", att1->attlen);

			if (cmpresult < 0)
			{
				/* arg1 is less than arg2 */
				result = -1;
				break;
			}
			else if (cmpresult > 0)
			{
				/* arg1 is greater than arg2 */
				result = 1;
				break;
			}
		}

		/* equal, so continue to next column */
		i1++, i2++, j++;
	}

	/*
	 * If we didn't break out of the loop early, check for column count
	 * mismatch.  (We do not report such mismatch if we found unequal column
	 * values; is that a feature or a bug?)
	 */
	if (result == 0)
	{
		if (i1 != ncolumns1 || i2 != ncolumns2)
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("cannot compare record types with different numbers of columns")));
	}

	pfree(values1);
	pfree(nulls1);
	pfree(values2);
	pfree(nulls2);
	ReleaseTupleDesc(tupdesc1);
	ReleaseTupleDesc(tupdesc2);

	/* Avoid leaking memory when handed toasted input. */
	PG_FREE_IF_COPY(record1, 0);
	PG_FREE_IF_COPY(record2, 1);

	return result;
}

/* ---- VERBATIM backend/utils/adt/rowtypes.c: record_image_eq [static-prefixed] ---- */
static Datum
record_image_eq(PG_FUNCTION_ARGS)
{
	HeapTupleHeader record1 = PG_GETARG_HEAPTUPLEHEADER(0);
	HeapTupleHeader record2 = PG_GETARG_HEAPTUPLEHEADER(1);
	bool		result = true;
	Oid			tupType1;
	Oid			tupType2;
	int32		tupTypmod1;
	int32		tupTypmod2;
	TupleDesc	tupdesc1;
	TupleDesc	tupdesc2;
	HeapTupleData tuple1;
	HeapTupleData tuple2;
	int			ncolumns1;
	int			ncolumns2;
	RecordCompareData *my_extra;
	int			ncols;
	Datum	   *values1;
	Datum	   *values2;
	bool	   *nulls1;
	bool	   *nulls2;
	int			i1;
	int			i2;
	int			j;

	/* Extract type info from the tuples */
	tupType1 = HeapTupleHeaderGetTypeId(record1);
	tupTypmod1 = HeapTupleHeaderGetTypMod(record1);
	tupdesc1 = lookup_rowtype_tupdesc(tupType1, tupTypmod1);
	ncolumns1 = tupdesc1->natts;
	tupType2 = HeapTupleHeaderGetTypeId(record2);
	tupTypmod2 = HeapTupleHeaderGetTypMod(record2);
	tupdesc2 = lookup_rowtype_tupdesc(tupType2, tupTypmod2);
	ncolumns2 = tupdesc2->natts;

	/* Build temporary HeapTuple control structures */
	tuple1.t_len = HeapTupleHeaderGetDatumLength(record1);
	ItemPointerSetInvalid(&(tuple1.t_self));
	tuple1.t_tableOid = InvalidOid;
	tuple1.t_data = record1;
	tuple2.t_len = HeapTupleHeaderGetDatumLength(record2);
	ItemPointerSetInvalid(&(tuple2.t_self));
	tuple2.t_tableOid = InvalidOid;
	tuple2.t_data = record2;

	/*
	 * We arrange to look up the needed comparison info just once per series
	 * of calls, assuming the record types don't change underneath us.
	 */
	ncols = Max(ncolumns1, ncolumns2);
	my_extra = (RecordCompareData *) fcinfo->flinfo->fn_extra;
	if (my_extra == NULL ||
		my_extra->ncolumns < ncols)
	{
		fcinfo->flinfo->fn_extra =
			MemoryContextAlloc(fcinfo->flinfo->fn_mcxt,
							   offsetof(RecordCompareData, columns) +
							   ncols * sizeof(ColumnCompareData));
		my_extra = (RecordCompareData *) fcinfo->flinfo->fn_extra;
		my_extra->ncolumns = ncols;
		my_extra->record1_type = InvalidOid;
		my_extra->record1_typmod = 0;
		my_extra->record2_type = InvalidOid;
		my_extra->record2_typmod = 0;
	}

	if (my_extra->record1_type != tupType1 ||
		my_extra->record1_typmod != tupTypmod1 ||
		my_extra->record2_type != tupType2 ||
		my_extra->record2_typmod != tupTypmod2)
	{
		MemSet(my_extra->columns, 0, ncols * sizeof(ColumnCompareData));
		my_extra->record1_type = tupType1;
		my_extra->record1_typmod = tupTypmod1;
		my_extra->record2_type = tupType2;
		my_extra->record2_typmod = tupTypmod2;
	}

	/* Break down the tuples into fields */
	values1 = (Datum *) palloc(ncolumns1 * sizeof(Datum));
	nulls1 = (bool *) palloc(ncolumns1 * sizeof(bool));
	heap_deform_tuple(&tuple1, tupdesc1, values1, nulls1);
	values2 = (Datum *) palloc(ncolumns2 * sizeof(Datum));
	nulls2 = (bool *) palloc(ncolumns2 * sizeof(bool));
	heap_deform_tuple(&tuple2, tupdesc2, values2, nulls2);

	/*
	 * Scan corresponding columns, allowing for dropped columns in different
	 * places in the two rows.  i1 and i2 are physical column indexes, j is
	 * the logical column index.
	 */
	i1 = i2 = j = 0;
	while (i1 < ncolumns1 || i2 < ncolumns2)
	{
		Form_pg_attribute att1;
		Form_pg_attribute att2;

		/*
		 * Skip dropped columns
		 */
		if (i1 < ncolumns1 && TupleDescAttr(tupdesc1, i1)->attisdropped)
		{
			i1++;
			continue;
		}
		if (i2 < ncolumns2 && TupleDescAttr(tupdesc2, i2)->attisdropped)
		{
			i2++;
			continue;
		}
		if (i1 >= ncolumns1 || i2 >= ncolumns2)
			break;				/* we'll deal with mismatch below loop */

		att1 = TupleDescAttr(tupdesc1, i1);
		att2 = TupleDescAttr(tupdesc2, i2);

		/*
		 * Have two matching columns, they must be same type
		 */
		if (att1->atttypid != att2->atttypid)
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("cannot compare dissimilar column types %s and %s at record column %d",
							format_type_be(att1->atttypid),
							format_type_be(att2->atttypid),
							j + 1)));

		/*
		 * We consider two NULLs equal; NULL > not-NULL.
		 */
		if (!nulls1[i1] || !nulls2[i2])
		{
			if (nulls1[i1] || nulls2[i2])
			{
				result = false;
				break;
			}

			/* Compare the pair of elements */
			result = datum_image_eq(values1[i1], values2[i2], att1->attbyval, att2->attlen);
			if (!result)
				break;
		}

		/* equal, so continue to next column */
		i1++, i2++, j++;
	}

	/*
	 * If we didn't break out of the loop early, check for column count
	 * mismatch.  (We do not report such mismatch if we found unequal column
	 * values; is that a feature or a bug?)
	 */
	if (result)
	{
		if (i1 != ncolumns1 || i2 != ncolumns2)
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("cannot compare record types with different numbers of columns")));
	}

	pfree(values1);
	pfree(nulls1);
	pfree(values2);
	pfree(nulls2);
	ReleaseTupleDesc(tupdesc1);
	ReleaseTupleDesc(tupdesc2);

	/* Avoid leaking memory when handed toasted input. */
	PG_FREE_IF_COPY(record1, 0);
	PG_FREE_IF_COPY(record2, 1);

	PG_RETURN_BOOL(result);
}

/* ---- VERBATIM backend/utils/adt/rowtypes.c: hash_record [static-prefixed] ---- */
static Datum
hash_record(PG_FUNCTION_ARGS)
{
	HeapTupleHeader record = PG_GETARG_HEAPTUPLEHEADER(0);
	uint32		result = 0;
	Oid			tupType;
	int32		tupTypmod;
	TupleDesc	tupdesc;
	HeapTupleData tuple;
	int			ncolumns;
	RecordCompareData *my_extra;
	Datum	   *values;
	bool	   *nulls;

	check_stack_depth();		/* recurses for record-type columns */

	/* Extract type info from tuple */
	tupType = HeapTupleHeaderGetTypeId(record);
	tupTypmod = HeapTupleHeaderGetTypMod(record);
	tupdesc = lookup_rowtype_tupdesc(tupType, tupTypmod);
	ncolumns = tupdesc->natts;

	/* Build temporary HeapTuple control structure */
	tuple.t_len = HeapTupleHeaderGetDatumLength(record);
	ItemPointerSetInvalid(&(tuple.t_self));
	tuple.t_tableOid = InvalidOid;
	tuple.t_data = record;

	/*
	 * We arrange to look up the needed hashing info just once per series of
	 * calls, assuming the record type doesn't change underneath us.
	 */
	my_extra = (RecordCompareData *) fcinfo->flinfo->fn_extra;
	if (my_extra == NULL ||
		my_extra->ncolumns < ncolumns)
	{
		fcinfo->flinfo->fn_extra =
			MemoryContextAlloc(fcinfo->flinfo->fn_mcxt,
							   offsetof(RecordCompareData, columns) +
							   ncolumns * sizeof(ColumnCompareData));
		my_extra = (RecordCompareData *) fcinfo->flinfo->fn_extra;
		my_extra->ncolumns = ncolumns;
		my_extra->record1_type = InvalidOid;
		my_extra->record1_typmod = 0;
	}

	if (my_extra->record1_type != tupType ||
		my_extra->record1_typmod != tupTypmod)
	{
		MemSet(my_extra->columns, 0, ncolumns * sizeof(ColumnCompareData));
		my_extra->record1_type = tupType;
		my_extra->record1_typmod = tupTypmod;
	}

	/* Break down the tuple into fields */
	values = (Datum *) palloc(ncolumns * sizeof(Datum));
	nulls = (bool *) palloc(ncolumns * sizeof(bool));
	heap_deform_tuple(&tuple, tupdesc, values, nulls);

	for (int i = 0; i < ncolumns; i++)
	{
		Form_pg_attribute att;
		TypeCacheEntry *typentry;
		uint32		element_hash;

		att = TupleDescAttr(tupdesc, i);

		if (att->attisdropped)
			continue;

		/*
		 * Lookup the hash function if not done already
		 */
		typentry = my_extra->columns[i].typentry;
		if (typentry == NULL ||
			typentry->type_id != att->atttypid)
		{
			typentry = lookup_type_cache(att->atttypid,
										 TYPECACHE_HASH_PROC_FINFO);
			if (!OidIsValid(typentry->hash_proc_finfo.fn_oid))
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_FUNCTION),
						 errmsg("could not identify a hash function for type %s",
								format_type_be(typentry->type_id))));
			my_extra->columns[i].typentry = typentry;
		}

		/* Compute hash of element */
		if (nulls[i])
		{
			element_hash = 0;
		}
		else
		{
			LOCAL_FCINFO(locfcinfo, 1);

			InitFunctionCallInfoData(*locfcinfo, &typentry->hash_proc_finfo, 1,
									 att->attcollation, NULL, NULL);
			locfcinfo->args[0].value = values[i];
			locfcinfo->args[0].isnull = false;
			element_hash = DatumGetUInt32(FunctionCallInvoke(locfcinfo));

			/* We don't expect hash support functions to return null */
			Assert(!locfcinfo->isnull);
		}

		/* see hash_array() */
		result = (result << 5) - result + element_hash;
	}

	pfree(values);
	pfree(nulls);
	ReleaseTupleDesc(tupdesc);

	/* Avoid leaking memory when handed toasted input. */
	PG_FREE_IF_COPY(record, 0);

	PG_RETURN_UINT32(result);
}

/* ---- VERBATIM backend/utils/adt/rowtypes.c: hash_record_extended [static-prefixed] ---- */
static Datum
hash_record_extended(PG_FUNCTION_ARGS)
{
	HeapTupleHeader record = PG_GETARG_HEAPTUPLEHEADER(0);
	uint64		seed = PG_GETARG_INT64(1);
	uint64		result = 0;
	Oid			tupType;
	int32		tupTypmod;
	TupleDesc	tupdesc;
	HeapTupleData tuple;
	int			ncolumns;
	RecordCompareData *my_extra;
	Datum	   *values;
	bool	   *nulls;

	check_stack_depth();		/* recurses for record-type columns */

	/* Extract type info from tuple */
	tupType = HeapTupleHeaderGetTypeId(record);
	tupTypmod = HeapTupleHeaderGetTypMod(record);
	tupdesc = lookup_rowtype_tupdesc(tupType, tupTypmod);
	ncolumns = tupdesc->natts;

	/* Build temporary HeapTuple control structure */
	tuple.t_len = HeapTupleHeaderGetDatumLength(record);
	ItemPointerSetInvalid(&(tuple.t_self));
	tuple.t_tableOid = InvalidOid;
	tuple.t_data = record;

	/*
	 * We arrange to look up the needed hashing info just once per series of
	 * calls, assuming the record type doesn't change underneath us.
	 */
	my_extra = (RecordCompareData *) fcinfo->flinfo->fn_extra;
	if (my_extra == NULL ||
		my_extra->ncolumns < ncolumns)
	{
		fcinfo->flinfo->fn_extra =
			MemoryContextAlloc(fcinfo->flinfo->fn_mcxt,
							   offsetof(RecordCompareData, columns) +
							   ncolumns * sizeof(ColumnCompareData));
		my_extra = (RecordCompareData *) fcinfo->flinfo->fn_extra;
		my_extra->ncolumns = ncolumns;
		my_extra->record1_type = InvalidOid;
		my_extra->record1_typmod = 0;
	}

	if (my_extra->record1_type != tupType ||
		my_extra->record1_typmod != tupTypmod)
	{
		MemSet(my_extra->columns, 0, ncolumns * sizeof(ColumnCompareData));
		my_extra->record1_type = tupType;
		my_extra->record1_typmod = tupTypmod;
	}

	/* Break down the tuple into fields */
	values = (Datum *) palloc(ncolumns * sizeof(Datum));
	nulls = (bool *) palloc(ncolumns * sizeof(bool));
	heap_deform_tuple(&tuple, tupdesc, values, nulls);

	for (int i = 0; i < ncolumns; i++)
	{
		Form_pg_attribute att;
		TypeCacheEntry *typentry;
		uint64		element_hash;

		att = TupleDescAttr(tupdesc, i);

		if (att->attisdropped)
			continue;

		/*
		 * Lookup the hash function if not done already
		 */
		typentry = my_extra->columns[i].typentry;
		if (typentry == NULL ||
			typentry->type_id != att->atttypid)
		{
			typentry = lookup_type_cache(att->atttypid,
										 TYPECACHE_HASH_EXTENDED_PROC_FINFO);
			if (!OidIsValid(typentry->hash_extended_proc_finfo.fn_oid))
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_FUNCTION),
						 errmsg("could not identify an extended hash function for type %s",
								format_type_be(typentry->type_id))));
			my_extra->columns[i].typentry = typentry;
		}

		/* Compute hash of element */
		if (nulls[i])
		{
			element_hash = 0;
		}
		else
		{
			LOCAL_FCINFO(locfcinfo, 2);

			InitFunctionCallInfoData(*locfcinfo, &typentry->hash_extended_proc_finfo, 2,
									 att->attcollation, NULL, NULL);
			locfcinfo->args[0].value = values[i];
			locfcinfo->args[0].isnull = false;
			locfcinfo->args[1].value = Int64GetDatum(seed);
			locfcinfo->args[0].isnull = false;
			element_hash = DatumGetUInt64(FunctionCallInvoke(locfcinfo));

			/* We don't expect hash support functions to return null */
			Assert(!locfcinfo->isnull);
		}

		/* see hash_array_extended() */
		result = (result << 5) - result + element_hash;
	}

	pfree(values);
	pfree(nulls);
	ReleaseTupleDesc(tupdesc);

	/* Avoid leaking memory when handed toasted input. */
	PG_FREE_IF_COPY(record, 0);

	PG_RETURN_UINT64(result);
}

/* ---- VERBATIM backend/utils/adt/rowtypes.c: record_eq [static-prefixed] ---- */
static Datum
record_eq(PG_FUNCTION_ARGS)
{
	HeapTupleHeader record1 = PG_GETARG_HEAPTUPLEHEADER(0);
	HeapTupleHeader record2 = PG_GETARG_HEAPTUPLEHEADER(1);
	bool		result = true;
	Oid			tupType1;
	Oid			tupType2;
	int32		tupTypmod1;
	int32		tupTypmod2;
	TupleDesc	tupdesc1;
	TupleDesc	tupdesc2;
	HeapTupleData tuple1;
	HeapTupleData tuple2;
	int			ncolumns1;
	int			ncolumns2;
	RecordCompareData *my_extra;
	int			ncols;
	Datum	   *values1;
	Datum	   *values2;
	bool	   *nulls1;
	bool	   *nulls2;
	int			i1;
	int			i2;
	int			j;

	check_stack_depth();		/* recurses for record-type columns */

	/* Extract type info from the tuples */
	tupType1 = HeapTupleHeaderGetTypeId(record1);
	tupTypmod1 = HeapTupleHeaderGetTypMod(record1);
	tupdesc1 = lookup_rowtype_tupdesc(tupType1, tupTypmod1);
	ncolumns1 = tupdesc1->natts;
	tupType2 = HeapTupleHeaderGetTypeId(record2);
	tupTypmod2 = HeapTupleHeaderGetTypMod(record2);
	tupdesc2 = lookup_rowtype_tupdesc(tupType2, tupTypmod2);
	ncolumns2 = tupdesc2->natts;

	/* Build temporary HeapTuple control structures */
	tuple1.t_len = HeapTupleHeaderGetDatumLength(record1);
	ItemPointerSetInvalid(&(tuple1.t_self));
	tuple1.t_tableOid = InvalidOid;
	tuple1.t_data = record1;
	tuple2.t_len = HeapTupleHeaderGetDatumLength(record2);
	ItemPointerSetInvalid(&(tuple2.t_self));
	tuple2.t_tableOid = InvalidOid;
	tuple2.t_data = record2;

	/*
	 * We arrange to look up the needed comparison info just once per series
	 * of calls, assuming the record types don't change underneath us.
	 */
	ncols = Max(ncolumns1, ncolumns2);
	my_extra = (RecordCompareData *) fcinfo->flinfo->fn_extra;
	if (my_extra == NULL ||
		my_extra->ncolumns < ncols)
	{
		fcinfo->flinfo->fn_extra =
			MemoryContextAlloc(fcinfo->flinfo->fn_mcxt,
							   offsetof(RecordCompareData, columns) +
							   ncols * sizeof(ColumnCompareData));
		my_extra = (RecordCompareData *) fcinfo->flinfo->fn_extra;
		my_extra->ncolumns = ncols;
		my_extra->record1_type = InvalidOid;
		my_extra->record1_typmod = 0;
		my_extra->record2_type = InvalidOid;
		my_extra->record2_typmod = 0;
	}

	if (my_extra->record1_type != tupType1 ||
		my_extra->record1_typmod != tupTypmod1 ||
		my_extra->record2_type != tupType2 ||
		my_extra->record2_typmod != tupTypmod2)
	{
		MemSet(my_extra->columns, 0, ncols * sizeof(ColumnCompareData));
		my_extra->record1_type = tupType1;
		my_extra->record1_typmod = tupTypmod1;
		my_extra->record2_type = tupType2;
		my_extra->record2_typmod = tupTypmod2;
	}

	/* Break down the tuples into fields */
	values1 = (Datum *) palloc(ncolumns1 * sizeof(Datum));
	nulls1 = (bool *) palloc(ncolumns1 * sizeof(bool));
	heap_deform_tuple(&tuple1, tupdesc1, values1, nulls1);
	values2 = (Datum *) palloc(ncolumns2 * sizeof(Datum));
	nulls2 = (bool *) palloc(ncolumns2 * sizeof(bool));
	heap_deform_tuple(&tuple2, tupdesc2, values2, nulls2);

	/*
	 * Scan corresponding columns, allowing for dropped columns in different
	 * places in the two rows.  i1 and i2 are physical column indexes, j is
	 * the logical column index.
	 */
	i1 = i2 = j = 0;
	while (i1 < ncolumns1 || i2 < ncolumns2)
	{
		LOCAL_FCINFO(locfcinfo, 2);
		Form_pg_attribute att1;
		Form_pg_attribute att2;
		TypeCacheEntry *typentry;
		Oid			collation;
		bool		oprresult;

		/*
		 * Skip dropped columns
		 */
		if (i1 < ncolumns1 && TupleDescAttr(tupdesc1, i1)->attisdropped)
		{
			i1++;
			continue;
		}
		if (i2 < ncolumns2 && TupleDescAttr(tupdesc2, i2)->attisdropped)
		{
			i2++;
			continue;
		}
		if (i1 >= ncolumns1 || i2 >= ncolumns2)
			break;				/* we'll deal with mismatch below loop */

		att1 = TupleDescAttr(tupdesc1, i1);
		att2 = TupleDescAttr(tupdesc2, i2);

		/*
		 * Have two matching columns, they must be same type
		 */
		if (att1->atttypid != att2->atttypid)
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("cannot compare dissimilar column types %s and %s at record column %d",
							format_type_be(att1->atttypid),
							format_type_be(att2->atttypid),
							j + 1)));

		/*
		 * If they're not same collation, we don't complain here, but the
		 * equality function might.
		 */
		collation = att1->attcollation;
		if (collation != att2->attcollation)
			collation = InvalidOid;

		/*
		 * Lookup the equality function if not done already
		 */
		typentry = my_extra->columns[j].typentry;
		if (typentry == NULL ||
			typentry->type_id != att1->atttypid)
		{
			typentry = lookup_type_cache(att1->atttypid,
										 TYPECACHE_EQ_OPR_FINFO);
			if (!OidIsValid(typentry->eq_opr_finfo.fn_oid))
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_FUNCTION),
						 errmsg("could not identify an equality operator for type %s",
								format_type_be(typentry->type_id))));
			my_extra->columns[j].typentry = typentry;
		}

		/*
		 * We consider two NULLs equal; NULL > not-NULL.
		 */
		if (!nulls1[i1] || !nulls2[i2])
		{
			if (nulls1[i1] || nulls2[i2])
			{
				result = false;
				break;
			}

			/* Compare the pair of elements */
			InitFunctionCallInfoData(*locfcinfo, &typentry->eq_opr_finfo, 2,
									 collation, NULL, NULL);
			locfcinfo->args[0].value = values1[i1];
			locfcinfo->args[0].isnull = false;
			locfcinfo->args[1].value = values2[i2];
			locfcinfo->args[1].isnull = false;
			oprresult = DatumGetBool(FunctionCallInvoke(locfcinfo));
			if (locfcinfo->isnull || !oprresult)
			{
				result = false;
				break;
			}
		}

		/* equal, so continue to next column */
		i1++, i2++, j++;
	}

	/*
	 * If we didn't break out of the loop early, check for column count
	 * mismatch.  (We do not report such mismatch if we found unequal column
	 * values; is that a feature or a bug?)
	 */
	if (result)
	{
		if (i1 != ncolumns1 || i2 != ncolumns2)
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("cannot compare record types with different numbers of columns")));
	}

	pfree(values1);
	pfree(nulls1);
	pfree(values2);
	pfree(nulls2);
	ReleaseTupleDesc(tupdesc1);
	ReleaseTupleDesc(tupdesc2);

	/* Avoid leaking memory when handed toasted input. */
	PG_FREE_IF_COPY(record1, 0);
	PG_FREE_IF_COPY(record2, 1);

	PG_RETURN_BOOL(result);
}

/* ---- VERBATIM backend/utils/adt/rowtypes.c: record_ne..btrecordcmp [static-prefixed] ---- */
static Datum
record_ne(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(!DatumGetBool(record_eq(fcinfo)));
}

static Datum
record_lt(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(record_cmp(fcinfo) < 0);
}

static Datum
record_gt(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(record_cmp(fcinfo) > 0);
}

static Datum
record_le(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(record_cmp(fcinfo) <= 0);
}

static Datum
record_ge(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(record_cmp(fcinfo) >= 0);
}

static Datum
btrecordcmp(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT32(record_cmp(fcinfo));
}

/* ---- VERBATIM backend/utils/adt/rowtypes.c: record_image_ne..record_image_ge [static-prefixed] ---- */
static Datum
record_image_ne(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(!DatumGetBool(record_image_eq(fcinfo)));
}

static Datum
record_image_lt(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(record_image_cmp(fcinfo) < 0);
}

static Datum
record_image_gt(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(record_image_cmp(fcinfo) > 0);
}

static Datum
record_image_le(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(record_image_cmp(fcinfo) <= 0);
}

static Datum
record_image_ge(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(record_image_cmp(fcinfo) >= 0);
}

/* ========== SECTION E: fuzz-facing driver entries (NOT Postgres code) ===== */
/*
 * Shape: pg_diff_arena_reset() then pg_diff_errcode = 0, arm the longjmp,
 * call the vendored function through a shim fcinfo, and write results into
 * caller buffers. Return: 0 = ok, 1 = error (class in pg_diff_errcode),
 * -2 = output buffer too small (harness sizing bug), -3 = internal harness
 * assertion (memo-call mismatch).
 */

static int pg_diff_soft_token;	/* non-NULL escontext token (soft mode) */

#define PG_DIFF_ENTRY() \
	do { \
		pg_diff_arena_reset(); \
		pg_diff_errcode = 0; \
		pg_diff_codec_failed = false; \
		pg_diff_descs_init(); \
		if (setjmp(pg_diff_rowtypes_jmp)) \
			return 1; \
	} while (0)

static int
pg_diff_copy_out(const void *src, Size n, unsigned char *out, int *outlen)
{
	if ((Size) *outlen < n)
		return -2;
	memcpy(out, src, n);
	*outlen = (int) n;
	return 0;
}

/* copy an image into a fresh MAXALIGNed arena chunk */
static Datum
pg_diff_image_datum(const unsigned char *img, int imglen)
{
	void	   *p = palloc((Size) imglen);

	memcpy(p, img, (Size) imglen);
	return PointerGetDatum(p);
}

int
pg_diff_record_in(int desc, int soft, const char *literal,
				  unsigned char *out, int *outlen)
{
	FmgrInfo	flinfo;
	FunctionCallInfoBaseData fc;
	Datum		r1,
				r2;
	Size		len;

	PG_DIFF_ENTRY();
	memset(&flinfo, 0, sizeof(flinfo));
	InitFunctionCallInfoData(fc, &flinfo, 3, InvalidOid,
							 soft ? (Node *) &pg_diff_soft_token : NULL, NULL);
	fc.args[0].value = CStringGetDatum(literal);
	fc.args[0].isnull = false;
	fc.args[1].value = ObjectIdGetDatum(RECORDOID);
	fc.args[1].isnull = false;
	fc.args[2].value = Int32GetDatum(desc);
	fc.args[2].isnull = false;
	r1 = record_in(&fc);
	/* soft-mode error: malformed paths set fc.isnull (PG_RETURN_NULL after
	 * errsave); the anonymous-record arm ereturns a 0 Datum WITHOUT setting
	 * isnull, so also treat a recorded errcode as the soft-error verdict
	 * (mirrors SOFT_ERROR_OCCURRED in real callers). */
	if (fc.isnull || (soft && pg_diff_errcode != 0))
		return 1;
	/* second call, same flinfo: the fn_extra memo-hit path */
	fc.isnull = false;
	r2 = record_in(&fc);
	if (fc.isnull || (soft && pg_diff_errcode != 0))
		return -3;
	len = HeapTupleHeaderGetDatumLength((HeapTupleHeaderData *) DatumGetPointer(r1));
	if (len != HeapTupleHeaderGetDatumLength((HeapTupleHeaderData *) DatumGetPointer(r2)) ||
		memcmp(DatumGetPointer(r1), DatumGetPointer(r2), len) != 0)
		return -3;
	return pg_diff_copy_out(DatumGetPointer(r1), len, out, outlen);
}

int
pg_diff_record_out(const unsigned char *img, int imglen,
				   unsigned char *out, int *outlen)
{
	FmgrInfo	flinfo;
	FunctionCallInfoBaseData fc;
	Datum		r1,
				r2;

	PG_DIFF_ENTRY();
	memset(&flinfo, 0, sizeof(flinfo));
	InitFunctionCallInfoData(fc, &flinfo, 1, InvalidOid, NULL, NULL);
	fc.args[0].value = pg_diff_image_datum(img, imglen);
	fc.args[0].isnull = false;
	r1 = record_out(&fc);
	r2 = record_out(&fc);
	if (strcmp(DatumGetCString(r1), DatumGetCString(r2)) != 0)
		return -3;
	return pg_diff_copy_out(DatumGetCString(r1),
							strlen(DatumGetCString(r1)) + 1, out, outlen);
}

int
pg_diff_record_recv(int desc, const unsigned char *wire, int wirelen,
					unsigned char *out, int *outlen)
{
	FmgrInfo	flinfo;
	FunctionCallInfoBaseData fc;
	StringInfoData buf;
	char	   *copy;
	Datum		r;
	Size		len;

	PG_DIFF_ENTRY();
	copy = (char *) palloc((Size) wirelen + 1);
	memcpy(copy, wire, (Size) wirelen);
	copy[wirelen] = '\0';
	initReadOnlyStringInfo(&buf, copy, wirelen);
	memset(&flinfo, 0, sizeof(flinfo));
	InitFunctionCallInfoData(fc, &flinfo, 3, InvalidOid, NULL, NULL);
	fc.args[0].value = PointerGetDatum(&buf);
	fc.args[0].isnull = false;
	fc.args[1].value = ObjectIdGetDatum(RECORDOID);
	fc.args[1].isnull = false;
	fc.args[2].value = Int32GetDatum(desc);
	fc.args[2].isnull = false;
	r = record_recv(&fc);
	len = HeapTupleHeaderGetDatumLength((HeapTupleHeaderData *) DatumGetPointer(r));
	return pg_diff_copy_out(DatumGetPointer(r), len, out, outlen);
}

int
pg_diff_record_send(const unsigned char *img, int imglen,
					unsigned char *out, int *outlen)
{
	FmgrInfo	flinfo;
	FunctionCallInfoBaseData fc;
	Datum		r;
	bytea	   *b;

	PG_DIFF_ENTRY();
	memset(&flinfo, 0, sizeof(flinfo));
	InitFunctionCallInfoData(fc, &flinfo, 1, InvalidOid, NULL, NULL);
	fc.args[0].value = pg_diff_image_datum(img, imglen);
	fc.args[0].isnull = false;
	r = record_send(&fc);
	b = (bytea *) DatumGetPointer(r);
	return pg_diff_copy_out(VARDATA(b), VARSIZE(b) - VARHDRSZ, out, outlen);
}

/* shared shape for the two-record arms */
static int
pg_diff_two_rec_setup(FunctionCallInfoBaseData *fc, FmgrInfo *flinfo,
					  const unsigned char *img1, int len1,
					  const unsigned char *img2, int len2)
{
	memset(flinfo, 0, sizeof(*flinfo));
	InitFunctionCallInfoData(*fc, flinfo, 2, InvalidOid, NULL, NULL);
	fc->args[0].value = pg_diff_image_datum(img1, len1);
	fc->args[0].isnull = false;
	fc->args[1].value = pg_diff_image_datum(img2, len2);
	fc->args[1].isnull = false;
	return 0;
}

int
pg_diff_record_image_cmp(const unsigned char *img1, int len1,
						 const unsigned char *img2, int len2,
						 int *cmp_out)
{
	FmgrInfo	flinfo;
	FunctionCallInfoBaseData fc;

	PG_DIFF_ENTRY();
	pg_diff_two_rec_setup(&fc, &flinfo, img1, len1, img2, len2);
	*cmp_out = record_image_cmp(&fc);
	return 0;
}

int
pg_diff_record_image_eq(const unsigned char *img1, int len1,
						const unsigned char *img2, int len2,
						int *eq_out)
{
	FmgrInfo	flinfo;
	FunctionCallInfoBaseData fc;

	PG_DIFF_ENTRY();
	pg_diff_two_rec_setup(&fc, &flinfo, img1, len1, img2, len2);
	*eq_out = DatumGetBool(record_image_eq(&fc)) ? 1 : 0;
	return 0;
}

int
pg_diff_hash_record(const unsigned char *img, int imglen, uint32 *h)
{
	FmgrInfo	flinfo;
	FunctionCallInfoBaseData fc;

	PG_DIFF_ENTRY();
	memset(&flinfo, 0, sizeof(flinfo));
	InitFunctionCallInfoData(fc, &flinfo, 1, InvalidOid, NULL, NULL);
	fc.args[0].value = pg_diff_image_datum(img, imglen);
	fc.args[0].isnull = false;
	*h = DatumGetUInt32(hash_record(&fc));
	return 0;
}

int
pg_diff_hash_record_extended(const unsigned char *img, int imglen,
							 uint64 seed, uint64 *h)
{
	FmgrInfo	flinfo;
	FunctionCallInfoBaseData fc;

	PG_DIFF_ENTRY();
	memset(&flinfo, 0, sizeof(flinfo));
	InitFunctionCallInfoData(fc, &flinfo, 2, InvalidOid, NULL, NULL);
	fc.args[0].value = pg_diff_image_datum(img, imglen);
	fc.args[0].isnull = false;
	fc.args[1].value = Int64GetDatum((int64) seed);
	fc.args[1].isnull = false;
	*h = DatumGetUInt64(hash_record_extended(&fc));
	return 0;
}

static int
pg_diff_larger_smaller(const unsigned char *img1, int len1,
					   const unsigned char *img2, int len2,
					   bool larger, int *which)
{
	FmgrInfo	flinfo;
	FunctionCallInfoBaseData fc;
	Datum		r;

	pg_diff_two_rec_setup(&fc, &flinfo, img1, len1, img2, len2);
	r = larger ? record_larger(&fc) : record_smaller(&fc);
	*which = (r == fc.args[0].value) ? 0 : 1;
	return 0;
}

int
pg_diff_record_larger(const unsigned char *img1, int len1,
					  const unsigned char *img2, int len2, int *which)
{
	PG_DIFF_ENTRY();
	return pg_diff_larger_smaller(img1, len1, img2, len2, true, which);
}

int
pg_diff_record_smaller(const unsigned char *img1, int len1,
					   const unsigned char *img2, int len2, int *which)
{
	PG_DIFF_ENTRY();
	return pg_diff_larger_smaller(img1, len1, img2, len2, false, which);
}

/* heap_form_tuple oracle access for the harness (builds the shared input
 * images for the image/hash/larger arms on the C side check-path; the Rust
 * driver builds its own via the shipped heaptuple crate and the two must
 * agree byte-for-byte — that agreement is itself a compared plane). */
int
pg_diff_form_record(int desc, const unsigned char *const *fields,
					const int *fieldlens, const int *isnull,
					unsigned char *out, int *outlen)
{
	TupleDesc	td;
	Datum		values[8];
	bool		nulls[8];
	HeapTuple	tuple;
	int			i;

	PG_DIFF_ENTRY();
	td = lookup_rowtype_tupdesc(RECORDOID, desc);
	for (i = 0; i < td->natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(td, i);

		if (isnull[i] || att->attisdropped)
		{
			values[i] = (Datum) 0;
			nulls[i] = true;
			continue;
		}
		nulls[i] = false;
		if (att->attbyval)
		{
			/* stage min(fieldlen, attlen) little-endian bytes into a
			 * zeroed word of the column's width (contract: mirrored by
			 * the Rust driver's build_record) */
			int			w = att->attlen;
			int64		v64 = 0;
			int32		v32 = 0;
			int16		v16 = 0;
			char		v8 = 0;

			switch (w)
			{
				case 1:
					memcpy(&v8, fields[i], fieldlens[i] < 1 ? fieldlens[i] : 1);
					values[i] = BoolGetDatum(v8 & 1);
					break;
				case 2:
					memcpy(&v16, fields[i], fieldlens[i] < 2 ? fieldlens[i] : 2);
					values[i] = Int16GetDatum(v16);
					break;
				case 8:
					memcpy(&v64, fields[i], fieldlens[i] < 8 ? fieldlens[i] : 8);
					values[i] = Int64GetDatum(v64);
					break;
				default:
					memcpy(&v32, fields[i], fieldlens[i] < 4 ? fieldlens[i] : 4);
					values[i] = Int32GetDatum(v32);
					break;
			}
		}
		else if (att->attlen > 0)
		{
			/* fixed-length by-ref: attlen buffer, zero-padded */
			unsigned char *buf = (unsigned char *) palloc((Size) att->attlen);
			int			n = fieldlens[i] < att->attlen ? fieldlens[i] : att->attlen;

			memset(buf, 0, (Size) att->attlen);
			memcpy(buf, fields[i], (Size) n);
			values[i] = PointerGetDatum(buf);
		}
		else
		{
			text	   *t = (text *) palloc(VARHDRSZ + (Size) fieldlens[i]);

			SET_VARSIZE(t, VARHDRSZ + fieldlens[i]);
			memcpy(VARDATA(t), fields[i], (Size) fieldlens[i]);
			values[i] = PointerGetDatum(t);
		}
	}
	tuple = heap_form_tuple(td, values, nulls);
	HeapTupleHeaderSetTypeId(tuple->t_data, RECORDOID);
	HeapTupleHeaderSetTypMod(tuple->t_data, desc);
	return pg_diff_copy_out(tuple->t_data, tuple->t_len, out, outlen);
}

/* record_eq/ne/lt/gt/le/ge/btrecordcmp family: `which` selects the wrapper
 * (0 eq / 1 ne / 2 lt / 3 gt / 4 le / 5 ge / 6 btrecordcmp); *val_out gets
 * the bool as 0/1 (or the int32 cmp for btrecordcmp). Two calls through one
 * flinfo (fn_extra memo-hit path), results must agree. */
int
pg_diff_record_cmpfam(int which,
					  const unsigned char *img1, int len1,
					  const unsigned char *img2, int len2,
					  int *val_out)
{
	FmgrInfo	flinfo;
	FunctionCallInfoBaseData fc;
	Datum		r1,
				r2;

	PG_DIFF_ENTRY();
	pg_diff_two_rec_setup(&fc, &flinfo, img1, len1, img2, len2);
	switch (which)
	{
		case 0: r1 = record_eq(&fc); r2 = record_eq(&fc); break;
		case 1: r1 = record_ne(&fc); r2 = record_ne(&fc); break;
		case 2: r1 = record_lt(&fc); r2 = record_lt(&fc); break;
		case 3: r1 = record_gt(&fc); r2 = record_gt(&fc); break;
		case 4: r1 = record_le(&fc); r2 = record_le(&fc); break;
		case 5: r1 = record_ge(&fc); r2 = record_ge(&fc); break;
		case 6: r1 = btrecordcmp(&fc); r2 = btrecordcmp(&fc); break;
		default: return -3;
	}
	if (r1 != r2)
		return -3;
	*val_out = (which == 6) ? DatumGetInt32(r1) : (DatumGetBool(r1) ? 1 : 0);
	return 0;
}

/* record_image_ne/lt/gt/le/ge wrappers: `which` 0 ne / 1 lt / 2 gt /
 * 3 le / 4 ge; *val_out gets the bool as 0/1. */
int
pg_diff_record_imagefam(int which,
						const unsigned char *img1, int len1,
						const unsigned char *img2, int len2,
						int *val_out)
{
	FmgrInfo	flinfo;
	FunctionCallInfoBaseData fc;
	Datum		r;

	PG_DIFF_ENTRY();
	pg_diff_two_rec_setup(&fc, &flinfo, img1, len1, img2, len2);
	switch (which)
	{
		case 0: r = record_image_ne(&fc); break;
		case 1: r = record_image_lt(&fc); break;
		case 2: r = record_image_gt(&fc); break;
		case 3: r = record_image_le(&fc); break;
		case 4: r = record_image_ge(&fc); break;
		default: return -3;
	}
	*val_out = DatumGetBool(r) ? 1 : 0;
	return 0;
}
