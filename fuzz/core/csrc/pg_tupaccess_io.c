/*
 * pg_tupaccess_io.c: vendored PostgreSQL C oracle for the tupaccess_diff
 * differential fuzz target (phase-1 verification campaign; crates
 * backend/access/common/heaptuple, backend/access/common/tupdesc, and the
 * deform half of _support/types/types_tuple).
 *
 * ASSEMBLED by scratchpad/assemble_tupaccess_oracle.py: every section marked
 * "VERBATIM <path>: <symbol>" is extracted byte-for-byte from the repo's
 * vendored ground-truth checkout
 * ../pgrust-fabled/vendor/postgres-src @ 62d6c7d3df6287f1bd83199c1a746e50d31571a0
 * (PostgreSQL 18.3, Stamp-18.3), or re-used from csrc/pg_rowtypes_io.c whose
 * sections carry the same provenance (marked "VERBATIM via pg_rowtypes_io.c").
 * Hand-written sections are marked "SHIM" (environment only, never the
 * computation under test) or "driver".
 *
 * VERBATIM computation under test:
 *   - heaptuple.c: getmissingattr, heap_compute_data_size, fill_val,
 *     heap_fill_tuple, heap_attisnull, nocachegetattr, heap_copytuple,
 *     heap_copytuple_with_tuple, expand_tuple, minimal_expand_tuple,
 *     heap_expand_tuple, heap_copy_tuple_as_datum, heap_form_tuple,
 *     heap_modify_tuple, heap_modify_tuple_by_cols, heap_deform_tuple,
 *     heap_freetuple, heap_form_minimal_tuple, heap_free_minimal_tuple,
 *     heap_copy_minimal_tuple, heap_tuple_from_minimal_tuple,
 *     minimal_tuple_from_heap_tuple.
 *   - tupdesc.c: populate_compact_attribute_internal(+wrapper),
 *     verify_compact_attribute, CreateTemplateTupleDesc, CreateTupleDesc,
 *     CreateTupleDescCopy, CreateTupleDescTruncatedCopy,
 *     CreateTupleDescCopyConstr, TupleDescCopy, TupleDescCopyEntry,
 *     FreeTupleDesc, equalTupleDescs, equalRowTypes, hashRowType,
 *     TupleDescInitEntry, TupleDescInitBuiltinEntry,
 *     TupleDescInitEntryCollation, BuildDescFromLists.
 *   - attmap.c: make_attrmap, free_attrmap, build_attrmap_by_position,
 *     build_attrmap_by_name, build_attrmap_by_name_if_req,
 *     check_attrmap_match.
 *   - tupconvert.c: convert_tuples_by_position, convert_tuples_by_name,
 *     convert_tuples_by_name_attrmap, execute_attr_map_tuple,
 *     free_conversion_map.
 *   - htup_details.h: fastgetattr, heap_getattr, GETSTRUCT,
 *     HeapTupleHasExternal + the header/macro machinery re-used from
 *     pg_rowtypes_io.c (varatt.h, htup_details.h, htup.h, pg_attribute,
 *     tupdesc.h, tupmacs.h).
 *   - datum.c: datumGetSize, datumCopy, datumIsEqual.
 *   - adt/name.c: namestrcpy (+ port/strlcpy.c strlcpy).
 *
 * ASSERTIONS ARE ON in this TU (USE_ASSERT_CHECKING + live Assert): the task
 * charter requires verify_compact_attribute to audit every TupleDescCompactAttr
 * access. An assert failure aborts the process = fuzz crash. NOTE this is the
 * opposite of pg_rowtypes_io.c's NDEBUG parity; the functions under test hold
 * their asserts on well-formed harness inputs by construction, and
 * equalTupleDescs' ATTNULLABLE_UNKNOWN assert is satisfied by the driver
 * staging attnullability VALID/INVALID whenever attnotnull (mirrors relcache
 * resolution; staged identically on the Rust side).
 *
 * SHIMS (environment only; each documented at its site):
 *   - base typedefs/macros, arena palloc, ereport/elog longjmp shim: the
 *     pg_rowtypes_io.c pattern (re-used verbatim from that file).
 *   - dynahash (hash_create/hash_search) -> linear-array table with identical
 *     HASH_ENTER/found semantics (pins getmissingattr's missing_cache
 *     ENVIRONMENT; the cached-copy computation runs verbatim).
 *   - SearchSysCache1(TYPEOID) -> pinned static pg_type row menu (catalog
 *     DATA pinned, never the computation).
 *   - pg_list.h List -> tiny array-backed list sufficient for
 *     BuildDescFromLists' forfour walk (SHIM, transcribed macros).
 *   - MemoryContext machinery -> arena (contexts are inert here).
 *   - heap_getsysattr -> aborting stub: system columns are OUT OF SCOPE
 *     (needs xact state); the drivers never pass attnum <= 0. CARVE.
 *   - toast_flatten_tuple_to_datum -> aborting stub: the external arm of
 *     heap_copy_tuple_as_datum is a documented CARVE (driver skips it for
 *     HeapTupleHasExternal tuples).
 *   - TupleDescGetDefault: NOT extracted (stringToNode unported) - CARVE;
 *     the defval plane is compared through the descriptor field-plane
 *     serializer + equalTupleDescs instead.
 *
 * RATIFIED 2026-08-01 (Michael): platform non-surface — width-1 byval Datum upper 56
 * bits (C fetch_att `*((char *) T)`, tupmacs.h; char signedness is
 * platform-defined — signed macOS-aarch64/x86_64-Linux, unsigned
 * Linux-aarch64; consumers truncate via DatumGetChar). Both datum
 * serializers mask width-1 words to the low 8 bits; found by the first
 * fleet CONFIRM on Linux-aarch64 (local macOS matched only because its char
 * is signed like the Rust port's i8). Widths 2/4/8 are signed on all
 * platforms and are NOT masked. The equalTupleDescs missing-value plane
 * needs no mask: datumIsEqual's byval word compare runs same-side only
 * (C-vs-C, Rust-vs-Rust) over stagings that are injective in the low byte,
 * so the verdict is platform-stable.
 *
 * Errcode classes (shared TLS pg_diff_errcode, defined in pg_float_io.c):
 * same numbering as pg_rowtypes_io.c (see that header); this family uses
 * 3 = ERRCODE_DATATYPE_MISMATCH, 6 = ERRCODE_TOO_MANY_COLUMNS, 7 = internal.
 */


/* ---- VERBATIM via pg_rowtypes_io.c: base typedef/shim region ---- */

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

/* SHIM CHANGE vs pg_rowtypes_io.c: assertions are ON in this TU (see
 * header). Assert aborts the process; a failure is a fuzz crash. */
#define USE_ASSERT_CHECKING 1
#define Assert(x) assert(x)
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
#define PG_DIFF_ARENA_MAX 65536	/* SHIM: tupdesc ops allocate more chunks */
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


/* SHIM: htup.h forward typedefs for the minimal-tuple structs */

typedef struct MinimalTupleData MinimalTupleData;
typedef MinimalTupleData *MinimalTuple;

/* ---- VERBATIM include/access/htup_details.h:674-704 (MINIMAL_TUPLE_* + MinimalTupleData + SizeofMinimalTupleHeader) ---- */

#define MINIMAL_TUPLE_OFFSET \
	((offsetof(HeapTupleHeaderData, t_infomask2) - sizeof(uint32)) / MAXIMUM_ALIGNOF * MAXIMUM_ALIGNOF)
#define MINIMAL_TUPLE_PADDING \
	((offsetof(HeapTupleHeaderData, t_infomask2) - sizeof(uint32)) % MAXIMUM_ALIGNOF)
#define MINIMAL_TUPLE_DATA_OFFSET \
	offsetof(MinimalTupleData, t_infomask2)

struct MinimalTupleData
{
	uint32		t_len;			/* actual length of minimal tuple */

	char		mt_padding[MINIMAL_TUPLE_PADDING];

	/* Fields below here must match HeapTupleHeaderData! */

	uint16		t_infomask2;	/* number of attributes + various flags */

	uint16		t_infomask;		/* various flag bits, see below */

	uint8		t_hoff;			/* sizeof header incl. bitmap, padding */

	/* ^ - 23 bytes - ^ */

	bits8		t_bits[FLEXIBLE_ARRAY_MEMBER];	/* bitmap of NULLs */

	/* MORE DATA FOLLOWS AT END OF STRUCT */
};

/* typedef appears in htup.h */

#define SizeofMinimalTupleHeader offsetof(MinimalTupleData, t_bits)

/* ---- VERBATIM include/catalog/pg_attribute.h:188-195 (ATTRIBUTE_FIXED_PART_SIZE) ---- */

/*
 * ATTRIBUTE_FIXED_PART_SIZE is the size of the fixed-layout,
 * guaranteed-not-null part of a pg_attribute row.  This is in fact as much
 * of the row as gets copied into tuple descriptors, so don't expect you
 * can access the variable-length fields except in a real tuple!
 */
#define ATTRIBUTE_FIXED_PART_SIZE \
	(offsetof(FormData_pg_attribute,attcollation) + sizeof(Oid))

/* SHIM: attnum.h AttrNumber (exact transcription) */

typedef int16 AttrNumber;
#define InvalidAttrNumber 0

/* ---- VERBATIM include/access/tupdesc_details.h:22-26 (AttrMissing) ---- */


/*
 * Structure used to represent value to be used when the attribute is not
 * present at all in a tuple, i.e. when the column was created after the tuple
 */
typedef struct AttrMissing
{
	bool		am_present;		/* true if non-NULL missing value exists */
	Datum		am_value;		/* value when attribute is missing */
} AttrMissing;

/* ---- VERBATIM include/access/tupdesc.h:22-48 (AttrDefault/ConstrCheck/TupleConstr) ---- */

typedef struct AttrDefault
{
	AttrNumber	adnum;
	char	   *adbin;			/* nodeToString representation of expr */
} AttrDefault;

typedef struct ConstrCheck
{
	char	   *ccname;
	char	   *ccbin;			/* nodeToString representation of expr */
	bool		ccenforced;
	bool		ccvalid;
	bool		ccnoinherit;	/* this is a non-inheritable constraint */
} ConstrCheck;

/* This structure contains constraints of a tuple */
typedef struct TupleConstr
{
	AttrDefault *defval;		/* array */
	ConstrCheck *check;			/* array */
	struct AttrMissing *missing;	/* missing attributes values, NULL if none */
	uint16		num_defval;
	uint16		num_check;
	bool		has_not_null;	/* any not-null, including not valid ones */
	bool		has_generated_stored;
	bool		has_generated_virtual;
} TupleConstr;

/* SHIM: forward decl (verify_compact_attribute is VERBATIM below and is
 * called by TupleDescCompactAttr now that USE_ASSERT_CHECKING is on) */

struct TupleDescData;
static void verify_compact_attribute(struct TupleDescData *tupdesc, int attnum);

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

/* ---- VERBATIM include/access/tupdesc.h TupleDescCompactAttr (the
 * USE_ASSERT_CHECKING verify_compact_attribute call is LIVE here) ---- */
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


/* ---- VERBATIM include/access/tupdesc.h:198-201 (TupleDescSize) ---- */

#define TupleDescSize(src) \
	(offsetof(struct TupleDescData, compact_attrs) + \
	 (src)->natts * sizeof(CompactAttribute) + \
	 (src)->natts * sizeof(FormData_pg_attribute))

/* ---- SHIM: c.h varlena typedefs (exact transcriptions) ---- */
#define VARHDRSZ		((int32) sizeof(int32))
typedef struct varlena bytea;
typedef struct varlena text;

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


/* ---- SHIM: c.h / limits transcriptions ---- */
#define PG_INT16_MAX (0x7FFF)
/* datumGetSize's invalid-pointer arm; class = internal (unreachable here) */
#define ERRCODE_DATA_EXCEPTION PG_DIFF_ERR_INTERNAL
#define _(x) (x)
#define errmsg_internal(...) 0
#define errdetail_internal(...) 0
#define gettext_noop(x) (x)
#define NameStr(name) ((name).data)

/* ---- SHIM: pinned catalog type oids (pg_type.h values; DATA pin) ---- */
#define TEXTOID 25
#define BOOLOID 16
#define INT4OID 23
#define INT8OID 20
#define OIDOID 26
#define TEXTARRAYOID 1009
#define DEFAULT_COLLATION_OID 100

/* ---- SHIM: toast_compression.h transcription ---- */
#define InvalidCompressionMethod '\0'

/* ---- SHIM: sysattr.h transcriptions ---- */
#define SelfItemPointerAttributeNumber			(-1)
#define MinTransactionIdAttributeNumber			(-2)
#define MinCommandIdAttributeNumber				(-3)
#define MaxTransactionIdAttributeNumber			(-4)
#define MaxCommandIdAttributeNumber				(-5)
#define TableOidAttributeNumber					(-6)
#define FirstLowInvalidHeapAttributeNumber		(-7)

/* ---- SHIM: hashfn surface; bodies are the VERBATIM vendored hashfn.c in
 * csrc/pg_mac_io.c under the pg_ prefix ---- */
#define hash_bytes pg_hash_bytes
#define hash_uint32 pg_hash_bytes_uint32


/* ---- VERBATIM include/common/hashfn.h: hash_combine ---- */
/*
 * Combine two 32-bit hash values, resulting in another hash value, with
 * decent bit mixing.
 *
 * Similar to boost's hash_combine().
 */
static inline uint32
hash_combine(uint32 a, uint32 b)
{
	a ^= b + 0x9e3779b9 + (a << 6) + (a >> 2);
	return a;
}


/* ---- SHIM: MemoryContext machinery. Allocations normally go to the
 * per-iteration arena; while switched to TopMemoryContext they go to plain
 * malloc and are NEVER freed - honoring PG's TopMemoryContext lifetime for
 * getmissingattr's missing_cache datumCopy (the cache outlives iterations,
 * so its values must too). ---- */
typedef void *MemoryContext;
static char pg_ta_top_tag;
static MemoryContext TopMemoryContext = (MemoryContext) &pg_ta_top_tag;
static _Thread_local MemoryContext pg_ta_current_cxt_init;
#define CurrentMemoryContext \
	(pg_ta_current_cxt_init ? pg_ta_current_cxt_init : (MemoryContext) &pg_ta_current_cxt_init)
static _Thread_local bool pg_ta_in_top_context;

static inline MemoryContext
MemoryContextSwitchTo(MemoryContext cxt)
{
	MemoryContext old = pg_ta_in_top_context ? TopMemoryContext : NULL;

	pg_ta_in_top_context = (cxt == TopMemoryContext);
	return old;
}

static void *
pg_ta_palloc_route(size_t n)
{
	if (pg_ta_in_top_context)
		return malloc(n);		/* TopMemoryContext: process lifetime */
	return pg_diff_palloc_impl(n);
}
#undef palloc
#define palloc(n) pg_ta_palloc_route(n)
#define MemoryContextAllocZero(cxt, n) pg_diff_palloc0_impl(n)

static char *
pstrdup(const char *s)
{
	size_t		n = strlen(s) + 1;
	char	   *p = palloc(n);

	memcpy(p, s, n);
	return p;
}

/* ---- SHIM: dynahash -> linear-array table. Pins the missing_cache
 * ENVIRONMENT of getmissingattr: hash_search(HASH_ENTER) either finds an
 * entry the caller's match function deems equal, or hands back a fresh
 * zero-keyed slot with *found=false - identical to dynahash's contract as
 * getmissingattr consumes it. The cached-copy computation stays verbatim. ---- */
typedef uint32 (*HashValueFunc) (const void *key, Size keysize);
typedef int (*HashCompareFunc) (const void *key1, const void *key2, Size keysize);
typedef struct HASHCTL
{
	Size		keysize;
	Size		entrysize;
	MemoryContext hcxt;
	HashValueFunc hash;
	HashCompareFunc match;
} HASHCTL;
#define HASH_ELEM 0x0001
#define HASH_CONTEXT 0x0002
#define HASH_FUNCTION 0x0004
#define HASH_COMPARE 0x0008
typedef enum { HASH_FIND, HASH_ENTER } HASHACTION;
#define PG_TA_HTAB_MAX 512
typedef struct HTAB
{
	HASHCTL		ctl;
	int			nents;
	/* PG_TA_HTAB_MAX slots of ctl.entrysize each, malloc'd (never freed:
	 * models TopMemoryContext lifetime) */
	char	   *ents;
} HTAB;

static HTAB *
hash_create(const char *name, long nelem, const HASHCTL *ctl, int flags)
{
	HTAB	   *h = malloc(sizeof(HTAB));

	(void) name; (void) nelem; (void) flags;
	h->ctl = *ctl;
	h->nents = 0;
	h->ents = malloc(PG_TA_HTAB_MAX * ctl->entrysize);
	return h;
}

static void *
hash_search(HTAB *h, const void *key, HASHACTION action, bool *found)
{
	int			i;

	for (i = 0; i < h->nents; i++)
	{
		char	   *e = h->ents + (Size) i * h->ctl.entrysize;

		if (h->ctl.match(e, key, h->ctl.keysize) == 0)
		{
			*found = true;
			return e;
		}
	}
	*found = false;
	if (action == HASH_FIND)
		return NULL;
	/* full: reset (fuzz-bounded stand-in for a process-lifetime cache; the
	 * cache is content-keyed, so eviction only changes WHICH copy callers
	 * get, never its bytes - the compared planes are unaffected) */
	if (h->nents >= PG_TA_HTAB_MAX)
		h->nents = 0;
	{
		char	   *e = h->ents + (Size) h->nents * h->ctl.entrysize;

		memset(e, 0, h->ctl.entrysize);
		memcpy(e, key, h->ctl.keysize);
		h->nents++;
		return e;
	}
}

/* ---- SHIM: syscache TYPEOID -> pinned static pg_type row menu. Pins
 * catalog DATA (the shape of each menu type), never the computation:
 * TupleDescInitEntry's field assignments run verbatim over the row. The
 * returned HeapTuple is a real little heap tuple so GETSTRUCT is the
 * verbatim accessor. ---- */
typedef struct PgTaFormPgType
{
	int16		typlen;
	bool		typbyval;
	char		typalign;
	char		typstorage;
	Oid			typcollation;
} PgTaFormPgType;
typedef PgTaFormPgType *Form_pg_type;
#define TYPEOID 0				/* cache-id token; only TYPEOID is used here */

/* forward: the menu lives in SECTION D */
static bool pg_ta_type_shape(Oid oid, PgTaFormPgType *shape);

static HeapTuple
SearchSysCache1(int cacheid, Datum key)
{
	static _Thread_local char buf[MAXALIGN(SizeofHeapTupleHeader) + MAXALIGN(sizeof(PgTaFormPgType))]
	__attribute__((aligned(8)));
	static _Thread_local struct HeapTupleData tup;
	HeapTupleHeaderData *hdr = (HeapTupleHeaderData *) buf;
	PgTaFormPgType shape;

	assert(cacheid == TYPEOID);
	if (!pg_ta_type_shape(DatumGetObjectId(key), &shape))
		return NULL;
	memset(buf, 0, sizeof(buf));
	hdr->t_hoff = MAXALIGN(SizeofHeapTupleHeader);
	memcpy(buf + hdr->t_hoff, &shape, sizeof(shape));
	tup.t_len = sizeof(buf);
	tup.t_tableOid = 0;
	tup.t_data = hdr;
	return &tup;
}
#define ReleaseSysCache(tuple) ((void) 0)
#define HeapTupleIsValid(tuple) PointerIsValid(tuple)

/* ---- SHIM: pg_list.h -> tiny array-backed list; just enough for
 * BuildDescFromLists' forfour walk. Macro shapes transcribed from
 * pg_list.h. ---- */
typedef union ListCell
{
	void	   *ptr_value;
	int			int_value;
	Oid			oid_value;
} ListCell;
typedef struct List
{
	int			length;
	ListCell	elements[64];
} List;
#define list_length(l) ((l) ? (l)->length : 0)
#define lfirst(lc) ((lc)->ptr_value)
#define lfirst_int(lc) ((lc)->int_value)
#define lfirst_oid(lc) ((lc)->oid_value)
#define list_nth_cell(l, n) ((ListCell *) &(l)->elements[n])
#define forfour(cell1, list1, cell2, list2, cell3, list3, cell4, list4) \
	for (int pg_ta_i_ = 0; \
		 pg_ta_i_ < list_length(list1) && \
		 ((cell1) = list_nth_cell(list1, pg_ta_i_), \
		  (cell2) = list_nth_cell(list2, pg_ta_i_), \
		  (cell3) = list_nth_cell(list3, pg_ta_i_), \
		  (cell4) = list_nth_cell(list4, pg_ta_i_), true); \
		 pg_ta_i_++)
/* value.h String node, projected to what strVal reads */
typedef struct String
{
	char	   *sval;
} String;
#define strVal(v) (((String *) (v))->sval)

/* ---- SHIM (CARVE): heap_getsysattr needs xact state; system columns are
 * out of scope for this family. The drivers never pass attnum <= 0. ---- */
static Datum
heap_getsysattr(HeapTuple tup, int attnum, struct TupleDescData *tupleDesc, bool *isnull)
{
	(void) tup; (void) attnum; (void) tupleDesc; (void) isnull;
	abort();					/* unreachable: drivers keep attnum >= 1 */
}

/* ---- SHIM (CARVE): the HeapTupleHasExternal arm of heap_copy_tuple_as_datum
 * requires the TOAST flattener; the driver skips that arm. ---- */
#define toast_flatten_tuple_to_datum(tup, tuplen, tupleDesc) \
	(abort(), (Datum) 0)


/* ---- VERBATIM include/access/attmap.h:34-38 (AttrMap) ---- */

typedef struct AttrMap
{
	AttrNumber *attnums;
	int			maplen;
} AttrMap;

/* ---- VERBATIM include/access/htup_details.h:729-737 (GETSTRUCT) ---- */

/*
 * GETSTRUCT - given a HeapTuple pointer, return address of the user data
 */
static inline void *
GETSTRUCT(const HeapTupleData *tuple)
{
	return ((char *) (tuple->t_data) + tuple->t_data->t_hoff);
}


/* ---- VERBATIM include/access/htup_details.h:766-770 (HeapTupleHasExternal) ---- */

static inline bool
HeapTupleHasExternal(const HeapTupleData *tuple)
{
	return (tuple->t_data->t_infomask & HEAP_HASEXTERNAL) != 0;
}

/* SHIM: intra-TU prototypes for the verbatim bodies below */
static Datum getmissingattr(struct TupleDescData *tupleDesc, int attnum, bool *isnull);
static Datum nocachegetattr(HeapTuple tup, int attnum, struct TupleDescData *tupleDesc);
static void populate_compact_attribute(struct TupleDescData *tupdesc, int attnum);
static Size datumGetSize(Datum value, bool typByVal, int typLen);
static Datum datumCopy(Datum value, bool typByVal, int typLen);
static bool datumIsEqual(Datum value1, Datum value2, bool typByVal, int typLen);
static void namestrcpy(NameData *name, const char *str);
static size_t pg_ta_strlcpy(char *dst, const char *src, size_t siz);
#undef strlcpy
#define strlcpy pg_ta_strlcpy
static struct TupleDescData *CreateTemplateTupleDesc(int natts);
static void TupleDescInitEntry(struct TupleDescData *desc, AttrNumber attributeNumber,
							   const char *attributeName, Oid oidtypeid, int32 typmod, int attdim);
static void TupleDescInitEntryCollation(struct TupleDescData *desc,
										AttrNumber attributeNumber, Oid collationid);
static void heap_fill_tuple(struct TupleDescData *tupleDesc,
							const Datum *values, const bool *isnull,
							char *data, Size data_size, uint16 *infomask, bits8 *bit);
static Size heap_compute_data_size(struct TupleDescData *tupleDesc,
								   const Datum *values, const bool *isnull);
static HeapTuple heap_form_tuple(struct TupleDescData *tupleDescriptor,
								 const Datum *values, const bool *isnull);
static void heap_deform_tuple(HeapTuple tuple, struct TupleDescData *tupleDesc,
							  Datum *values, bool *isnull);
static bool check_attrmap_match(struct TupleDescData *indesc, struct TupleDescData *outdesc, AttrMap *attrMap);
static AttrMap *make_attrmap(int maplen);
struct TupleConversionMap;
static struct TupleConversionMap *convert_tuples_by_name_attrmap(struct TupleDescData *indesc,
	struct TupleDescData *outdesc, AttrMap *attrMap);


/* ---- VERBATIM backend/utils/adt/name.c: namestrcpy [static-prefixed] ---- */
static void
namestrcpy(Name name, const char *str)
{
	/* NB: We need to zero-pad the destination. */
	strncpy(NameStr(*name), str, NAMEDATALEN);
	NameStr(*name)[NAMEDATALEN - 1] = '\0';
}

/* ---- VERBATIM port/strlcpy.c: strlcpy [static-prefixed] ---- */
/*
 * Copy src to string dst of size siz.  At most siz-1 characters
 * will be copied.  Always NUL terminates (unless siz == 0).
 * Returns strlen(src); if retval >= siz, truncation occurred.
 * Function creation history:  http://www.gratisoft.us/todd/papers/strlcpy.html
 */
static size_t
strlcpy(char *dst, const char *src, size_t siz)
{
	char	   *d = dst;
	const char *s = src;
	size_t		n = siz;

	/* Copy as many bytes as will fit */
	if (n != 0)
	{
		while (--n != 0)
		{
			if ((*d++ = *s++) == '\0')
				break;
		}
	}

	/* Not enough room in dst, add NUL and traverse rest of src */
	if (n == 0)
	{
		if (siz != 0)
			*d = '\0';			/* NUL-terminate dst */
		while (*s++)
			;
	}

	return (s - src - 1);		/* count does not include NUL */
}

/* ---- VERBATIM backend/utils/adt/datum.c: datumGetSize [static-prefixed] ---- */
/*-------------------------------------------------------------------------
 * datumGetSize
 *
 * Find the "real" size of a datum, given the datum value,
 * whether it is a "by value", and the declared type length.
 * (For TOAST pointer datums, this is the size of the pointer datum.)
 *
 * This is essentially an out-of-line version of the att_addlength_datum()
 * macro in access/tupmacs.h.  We do a tad more error checking though.
 *-------------------------------------------------------------------------
 */
static Size
datumGetSize(Datum value, bool typByVal, int typLen)
{
	Size		size;

	if (typByVal)
	{
		/* Pass-by-value types are always fixed-length */
		Assert(typLen > 0 && typLen <= sizeof(Datum));
		size = (Size) typLen;
	}
	else
	{
		if (typLen > 0)
		{
			/* Fixed-length pass-by-ref type */
			size = (Size) typLen;
		}
		else if (typLen == -1)
		{
			/* It is a varlena datatype */
			struct varlena *s = (struct varlena *) DatumGetPointer(value);

			if (!PointerIsValid(s))
				ereport(ERROR,
						(errcode(ERRCODE_DATA_EXCEPTION),
						 errmsg("invalid Datum pointer")));

			size = (Size) VARSIZE_ANY(s);
		}
		else if (typLen == -2)
		{
			/* It is a cstring datatype */
			char	   *s = (char *) DatumGetPointer(value);

			if (!PointerIsValid(s))
				ereport(ERROR,
						(errcode(ERRCODE_DATA_EXCEPTION),
						 errmsg("invalid Datum pointer")));

			size = (Size) (strlen(s) + 1);
		}
		else
		{
			elog(ERROR, "invalid typLen: %d", typLen);
			size = 0;			/* keep compiler quiet */
		}
	}

	return size;
}

/* ---- VERBATIM backend/utils/adt/datum.c: datumCopy [static-prefixed] ---- */
/*-------------------------------------------------------------------------
 * datumCopy
 *
 * Make a copy of a non-NULL datum.
 *
 * If the datatype is pass-by-reference, memory is obtained with palloc().
 *
 * If the value is a reference to an expanded object, we flatten into memory
 * obtained with palloc().  We need to copy because one of the main uses of
 * this function is to copy a datum out of a transient memory context that's
 * about to be destroyed, and the expanded object is probably in a child
 * context that will also go away.  Moreover, many callers assume that the
 * result is a single pfree-able chunk.
 *-------------------------------------------------------------------------
 */
static Datum
datumCopy(Datum value, bool typByVal, int typLen)
{
	Datum		res;

	if (typByVal)
		res = value;
	else if (typLen == -1)
	{
		/* It is a varlena datatype */
		struct varlena *vl = (struct varlena *) DatumGetPointer(value);

		if (VARATT_IS_EXTERNAL_EXPANDED(vl))
		{
			/* Flatten into the caller's memory context */
			ExpandedObjectHeader *eoh = DatumGetEOHP(value);
			Size		resultsize;
			char	   *resultptr;

			resultsize = EOH_get_flat_size(eoh);
			resultptr = (char *) palloc(resultsize);
			EOH_flatten_into(eoh, resultptr, resultsize);
			res = PointerGetDatum(resultptr);
		}
		else
		{
			/* Otherwise, just copy the varlena datum verbatim */
			Size		realSize;
			char	   *resultptr;

			realSize = (Size) VARSIZE_ANY(vl);
			resultptr = (char *) palloc(realSize);
			memcpy(resultptr, vl, realSize);
			res = PointerGetDatum(resultptr);
		}
	}
	else
	{
		/* Pass by reference, but not varlena, so not toasted */
		Size		realSize;
		char	   *resultptr;

		realSize = datumGetSize(value, typByVal, typLen);

		resultptr = (char *) palloc(realSize);
		memcpy(resultptr, DatumGetPointer(value), realSize);
		res = PointerGetDatum(resultptr);
	}
	return res;
}

/* ---- VERBATIM backend/utils/adt/datum.c: datumIsEqual [static-prefixed] ---- */
/*-------------------------------------------------------------------------
 * datumIsEqual
 *
 * Return true if two datums are equal, false otherwise
 *
 * NOTE: XXX!
 * We just compare the bytes of the two values, one by one.
 * This routine will return false if there are 2 different
 * representations of the same value (something along the lines
 * of say the representation of zero in one's complement arithmetic).
 * Also, it will probably not give the answer you want if either
 * datum has been "toasted".
 *
 * Do not try to make this any smarter than it currently is with respect
 * to "toasted" datums, because some of the callers could be working in the
 * context of an aborted transaction.
 *-------------------------------------------------------------------------
 */
static bool
datumIsEqual(Datum value1, Datum value2, bool typByVal, int typLen)
{
	bool		res;

	if (typByVal)
	{
		/*
		 * just compare the two datums. NOTE: just comparing "len" bytes will
		 * not do the work, because we do not know how these bytes are aligned
		 * inside the "Datum".  We assume instead that any given datatype is
		 * consistent about how it fills extraneous bits in the Datum.
		 */
		res = (value1 == value2);
	}
	else
	{
		Size		size1,
					size2;
		char	   *s1,
				   *s2;

		/*
		 * Compare the bytes pointed by the pointers stored in the datums.
		 */
		size1 = datumGetSize(value1, typByVal, typLen);
		size2 = datumGetSize(value2, typByVal, typLen);
		if (size1 != size2)
			return false;
		s1 = (char *) DatumGetPointer(value1);
		s2 = (char *) DatumGetPointer(value2);
		res = (memcmp(s1, s2, size1) == 0);
	}
	return res;
}

/* ---- VERBATIM backend/access/common/heaptuple.c:95-140 (missing_cache) ---- */

typedef struct
{
	int			len;
	Datum		value;
} missing_cache_key;

static HTAB *missing_cache = NULL;

static uint32
missing_hash(const void *key, Size keysize)
{
	const missing_cache_key *entry = (missing_cache_key *) key;

	return hash_bytes((const unsigned char *) entry->value, entry->len);
}

static int
missing_match(const void *key1, const void *key2, Size keysize)
{
	const missing_cache_key *entry1 = (missing_cache_key *) key1;
	const missing_cache_key *entry2 = (missing_cache_key *) key2;

	if (entry1->len != entry2->len)
		return entry1->len > entry2->len ? 1 : -1;

	return memcmp(DatumGetPointer(entry1->value),
				  DatumGetPointer(entry2->value),
				  entry1->len);
}

static void
init_missing_cache()
{
	HASHCTL		hash_ctl;

	hash_ctl.keysize = sizeof(missing_cache_key);
	hash_ctl.entrysize = sizeof(missing_cache_key);
	hash_ctl.hcxt = TopMemoryContext;
	hash_ctl.hash = missing_hash;
	hash_ctl.match = missing_match;
	missing_cache =
		hash_create("Missing Values Cache",
					32,
					&hash_ctl,
					HASH_ELEM | HASH_CONTEXT | HASH_FUNCTION | HASH_COMPARE);
}

/* ---- VERBATIM backend/access/common/heaptuple.c: getmissingattr [static-prefixed] ---- */
/*
 * Return the missing value of an attribute, or NULL if there isn't one.
 */
static Datum
getmissingattr(TupleDesc tupleDesc,
			   int attnum, bool *isnull)
{
	CompactAttribute *att;

	Assert(attnum <= tupleDesc->natts);
	Assert(attnum > 0);

	att = TupleDescCompactAttr(tupleDesc, attnum - 1);

	if (att->atthasmissing)
	{
		AttrMissing *attrmiss;

		Assert(tupleDesc->constr);
		Assert(tupleDesc->constr->missing);

		attrmiss = tupleDesc->constr->missing + (attnum - 1);

		if (attrmiss->am_present)
		{
			missing_cache_key key;
			missing_cache_key *entry;
			bool		found;
			MemoryContext oldctx;

			*isnull = false;

			/* no  need to cache by-value attributes */
			if (att->attbyval)
				return attrmiss->am_value;

			/* set up cache if required */
			if (missing_cache == NULL)
				init_missing_cache();

			/* check if there's a cache entry */
			Assert(att->attlen > 0 || att->attlen == -1);
			if (att->attlen > 0)
				key.len = att->attlen;
			else
				key.len = VARSIZE_ANY(attrmiss->am_value);
			key.value = attrmiss->am_value;

			entry = hash_search(missing_cache, &key, HASH_ENTER, &found);

			if (!found)
			{
				/* cache miss, so we need a non-transient copy of the datum */
				oldctx = MemoryContextSwitchTo(TopMemoryContext);
				entry->value =
					datumCopy(attrmiss->am_value, false, att->attlen);
				MemoryContextSwitchTo(oldctx);
			}

			return entry->value;
		}
	}

	*isnull = true;
	return PointerGetDatum(NULL);
}

/* ---- VERBATIM backend/access/common/heaptuple.c: heap_compute_data_size [static-prefixed] ---- */
/*
 * heap_compute_data_size
 *		Determine size of the data area of a tuple to be constructed
 */
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
/*
 * Per-attribute helper for heap_fill_tuple and other routines building tuples.
 *
 * Fill in either a data value or a bit in the null bitmask
 */
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
/*
 * heap_fill_tuple
 *		Load data portion of a tuple from values/isnull arrays
 *
 * We also fill the null bitmap (if any) and set the infomask bits
 * that reflect the tuple's data contents.
 *
 * NOTE: it is now REQUIRED that the caller have pre-zeroed the data area.
 */
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

/* ---- VERBATIM backend/access/common/heaptuple.c: heap_attisnull [static-prefixed] ---- */
/* ----------------
 *		heap_attisnull	- returns true iff tuple attribute is not present
 * ----------------
 */
static bool
heap_attisnull(HeapTuple tup, int attnum, TupleDesc tupleDesc)
{
	/*
	 * We allow a NULL tupledesc for relations not expected to have missing
	 * values, such as catalog relations and indexes.
	 */
	Assert(!tupleDesc || attnum <= tupleDesc->natts);
	if (attnum > (int) HeapTupleHeaderGetNatts(tup->t_data))
	{
		if (tupleDesc &&
			TupleDescCompactAttr(tupleDesc, attnum - 1)->atthasmissing)
			return false;
		else
			return true;
	}

	if (attnum > 0)
	{
		if (HeapTupleNoNulls(tup))
			return false;
		return att_isnull(attnum - 1, tup->t_data->t_bits);
	}

	switch (attnum)
	{
		case TableOidAttributeNumber:
		case SelfItemPointerAttributeNumber:
		case MinTransactionIdAttributeNumber:
		case MinCommandIdAttributeNumber:
		case MaxTransactionIdAttributeNumber:
		case MaxCommandIdAttributeNumber:
			/* these are never null */
			break;

		default:
			elog(ERROR, "invalid attnum: %d", attnum);
	}

	return false;
}

/* ---- VERBATIM backend/access/common/heaptuple.c: nocachegetattr [static-prefixed] ---- */
/* ----------------
 *		nocachegetattr
 *
 *		This only gets called from fastgetattr(), in cases where we
 *		can't use a cacheoffset and the value is not null.
 *
 *		This caches attribute offsets in the attribute descriptor.
 *
 *		An alternative way to speed things up would be to cache offsets
 *		with the tuple, but that seems more difficult unless you take
 *		the storage hit of actually putting those offsets into the
 *		tuple you send to disk.  Yuck.
 *
 *		This scheme will be slightly slower than that, but should
 *		perform well for queries which hit large #'s of tuples.  After
 *		you cache the offsets once, examining all the other tuples using
 *		the same attribute descriptor will go much quicker. -cim 5/4/91
 *
 *		NOTE: if you need to change this code, see also heap_deform_tuple.
 *		Also see nocache_index_getattr, which is the same code for index
 *		tuples.
 * ----------------
 */
static Datum
nocachegetattr(HeapTuple tup,
			   int attnum,
			   TupleDesc tupleDesc)
{
	HeapTupleHeader td = tup->t_data;
	char	   *tp;				/* ptr to data part of tuple */
	bits8	   *bp = td->t_bits;	/* ptr to null bitmap in tuple */
	bool		slow = false;	/* do we have to walk attrs? */
	int			off;			/* current offset within data */

	/* ----------------
	 *	 Three cases:
	 *
	 *	 1: No nulls and no variable-width attributes.
	 *	 2: Has a null or a var-width AFTER att.
	 *	 3: Has nulls or var-widths BEFORE att.
	 * ----------------
	 */

	attnum--;

	if (!HeapTupleNoNulls(tup))
	{
		/*
		 * there's a null somewhere in the tuple
		 *
		 * check to see if any preceding bits are null...
		 */
		int			byte = attnum >> 3;
		int			finalbit = attnum & 0x07;

		/* check for nulls "before" final bit of last byte */
		if ((~bp[byte]) & ((1 << finalbit) - 1))
			slow = true;
		else
		{
			/* check for nulls in any "earlier" bytes */
			int			i;

			for (i = 0; i < byte; i++)
			{
				if (bp[i] != 0xFF)
				{
					slow = true;
					break;
				}
			}
		}
	}

	tp = (char *) td + td->t_hoff;

	if (!slow)
	{
		CompactAttribute *att;

		/*
		 * If we get here, there are no nulls up to and including the target
		 * attribute.  If we have a cached offset, we can use it.
		 */
		att = TupleDescCompactAttr(tupleDesc, attnum);
		if (att->attcacheoff >= 0)
			return fetchatt(att, tp + att->attcacheoff);

		/*
		 * Otherwise, check for non-fixed-length attrs up to and including
		 * target.  If there aren't any, it's safe to cheaply initialize the
		 * cached offsets for these attrs.
		 */
		if (HeapTupleHasVarWidth(tup))
		{
			int			j;

			for (j = 0; j <= attnum; j++)
			{
				if (TupleDescCompactAttr(tupleDesc, j)->attlen <= 0)
				{
					slow = true;
					break;
				}
			}
		}
	}

	if (!slow)
	{
		int			natts = tupleDesc->natts;
		int			j = 1;

		/*
		 * If we get here, we have a tuple with no nulls or var-widths up to
		 * and including the target attribute, so we can use the cached offset
		 * ... only we don't have it yet, or we'd not have got here.  Since
		 * it's cheap to compute offsets for fixed-width columns, we take the
		 * opportunity to initialize the cached offsets for *all* the leading
		 * fixed-width columns, in hope of avoiding future visits to this
		 * routine.
		 */
		TupleDescCompactAttr(tupleDesc, 0)->attcacheoff = 0;

		/* we might have set some offsets in the slow path previously */
		while (j < natts && TupleDescCompactAttr(tupleDesc, j)->attcacheoff > 0)
			j++;

		off = TupleDescCompactAttr(tupleDesc, j - 1)->attcacheoff +
			TupleDescCompactAttr(tupleDesc, j - 1)->attlen;

		for (; j < natts; j++)
		{
			CompactAttribute *att = TupleDescCompactAttr(tupleDesc, j);

			if (att->attlen <= 0)
				break;

			off = att_nominal_alignby(off, att->attalignby);

			att->attcacheoff = off;

			off += att->attlen;
		}

		Assert(j > attnum);

		off = TupleDescCompactAttr(tupleDesc, attnum)->attcacheoff;
	}
	else
	{
		bool		usecache = true;
		int			i;

		/*
		 * Now we know that we have to walk the tuple CAREFULLY.  But we still
		 * might be able to cache some offsets for next time.
		 *
		 * Note - This loop is a little tricky.  For each non-null attribute,
		 * we have to first account for alignment padding before the attr,
		 * then advance over the attr based on its length.  Nulls have no
		 * storage and no alignment padding either.  We can use/set
		 * attcacheoff until we reach either a null or a var-width attribute.
		 */
		off = 0;
		for (i = 0;; i++)		/* loop exit is at "break" */
		{
			CompactAttribute *att = TupleDescCompactAttr(tupleDesc, i);

			if (HeapTupleHasNulls(tup) && att_isnull(i, bp))
			{
				usecache = false;
				continue;		/* this cannot be the target att */
			}

			/* If we know the next offset, we can skip the rest */
			if (usecache && att->attcacheoff >= 0)
				off = att->attcacheoff;
			else if (att->attlen == -1)
			{
				/*
				 * We can only cache the offset for a varlena attribute if the
				 * offset is already suitably aligned, so that there would be
				 * no pad bytes in any case: then the offset will be valid for
				 * either an aligned or unaligned value.
				 */
				if (usecache &&
					off == att_nominal_alignby(off, att->attalignby))
					att->attcacheoff = off;
				else
				{
					off = att_pointer_alignby(off, att->attalignby, -1,
											  tp + off);
					usecache = false;
				}
			}
			else
			{
				/* not varlena, so safe to use att_nominal_alignby */
				off = att_nominal_alignby(off, att->attalignby);

				if (usecache)
					att->attcacheoff = off;
			}

			if (i == attnum)
				break;

			off = att_addlength_pointer(off, att->attlen, tp + off);

			if (usecache && att->attlen <= 0)
				usecache = false;
		}
	}

	return fetchatt(TupleDescCompactAttr(tupleDesc, attnum), tp + off);
}

/* ---- VERBATIM backend/access/common/heaptuple.c: heap_copytuple [static-prefixed] ---- */
/* ----------------
 *		heap_copytuple
 *
 *		returns a copy of an entire tuple
 *
 * The HeapTuple struct, tuple header, and tuple data are all allocated
 * as a single palloc() block.
 * ----------------
 */
static HeapTuple
heap_copytuple(HeapTuple tuple)
{
	HeapTuple	newTuple;

	if (!HeapTupleIsValid(tuple) || tuple->t_data == NULL)
		return NULL;

	newTuple = (HeapTuple) palloc(HEAPTUPLESIZE + tuple->t_len);
	newTuple->t_len = tuple->t_len;
	newTuple->t_self = tuple->t_self;
	newTuple->t_tableOid = tuple->t_tableOid;
	newTuple->t_data = (HeapTupleHeader) ((char *) newTuple + HEAPTUPLESIZE);
	memcpy(newTuple->t_data, tuple->t_data, tuple->t_len);
	return newTuple;
}

/* ---- VERBATIM backend/access/common/heaptuple.c: heap_copytuple_with_tuple [static-prefixed] ---- */
/* ----------------
 *		heap_copytuple_with_tuple
 *
 *		copy a tuple into a caller-supplied HeapTuple management struct
 *
 * Note that after calling this function, the "dest" HeapTuple will not be
 * allocated as a single palloc() block (unlike with heap_copytuple()).
 * ----------------
 */
static void
heap_copytuple_with_tuple(HeapTuple src, HeapTuple dest)
{
	if (!HeapTupleIsValid(src) || src->t_data == NULL)
	{
		dest->t_data = NULL;
		return;
	}

	dest->t_len = src->t_len;
	dest->t_self = src->t_self;
	dest->t_tableOid = src->t_tableOid;
	dest->t_data = (HeapTupleHeader) palloc(src->t_len);
	memcpy(dest->t_data, src->t_data, src->t_len);
}

/* ---- VERBATIM backend/access/common/heaptuple.c: expand_tuple ---- */
/*
 * Expand a tuple which has fewer attributes than required. For each attribute
 * not present in the sourceTuple, if there is a missing value that will be
 * used. Otherwise the attribute will be set to NULL.
 *
 * The source tuple must have fewer attributes than the required number.
 *
 * Only one of targetHeapTuple and targetMinimalTuple may be supplied. The
 * other argument must be NULL.
 */
static void
expand_tuple(HeapTuple *targetHeapTuple,
			 MinimalTuple *targetMinimalTuple,
			 HeapTuple sourceTuple,
			 TupleDesc tupleDesc)
{
	AttrMissing *attrmiss = NULL;
	int			attnum;
	int			firstmissingnum;
	bool		hasNulls = HeapTupleHasNulls(sourceTuple);
	HeapTupleHeader targetTHeader;
	HeapTupleHeader sourceTHeader = sourceTuple->t_data;
	int			sourceNatts = HeapTupleHeaderGetNatts(sourceTHeader);
	int			natts = tupleDesc->natts;
	int			sourceNullLen;
	int			targetNullLen;
	Size		sourceDataLen = sourceTuple->t_len - sourceTHeader->t_hoff;
	Size		targetDataLen;
	Size		len;
	int			hoff;
	bits8	   *nullBits = NULL;
	int			bitMask = 0;
	char	   *targetData;
	uint16	   *infoMask;

	Assert((targetHeapTuple && !targetMinimalTuple)
		   || (!targetHeapTuple && targetMinimalTuple));

	Assert(sourceNatts < natts);

	sourceNullLen = (hasNulls ? BITMAPLEN(sourceNatts) : 0);

	targetDataLen = sourceDataLen;

	if (tupleDesc->constr &&
		tupleDesc->constr->missing)
	{
		/*
		 * If there are missing values we want to put them into the tuple.
		 * Before that we have to compute the extra length for the values
		 * array and the variable length data.
		 */
		attrmiss = tupleDesc->constr->missing;

		/*
		 * Find the first item in attrmiss for which we don't have a value in
		 * the source. We can ignore all the missing entries before that.
		 */
		for (firstmissingnum = sourceNatts;
			 firstmissingnum < natts;
			 firstmissingnum++)
		{
			if (attrmiss[firstmissingnum].am_present)
				break;
			else
				hasNulls = true;
		}

		/*
		 * Now walk the missing attributes. If there is a missing value make
		 * space for it. Otherwise, it's going to be NULL.
		 */
		for (attnum = firstmissingnum;
			 attnum < natts;
			 attnum++)
		{
			if (attrmiss[attnum].am_present)
			{
				CompactAttribute *att = TupleDescCompactAttr(tupleDesc, attnum);

				targetDataLen = att_datum_alignby(targetDataLen,
												  att->attalignby,
												  att->attlen,
												  attrmiss[attnum].am_value);

				targetDataLen = att_addlength_pointer(targetDataLen,
													  att->attlen,
													  attrmiss[attnum].am_value);
			}
			else
			{
				/* no missing value, so it must be null */
				hasNulls = true;
			}
		}
	}							/* end if have missing values */
	else
	{
		/*
		 * If there are no missing values at all then NULLS must be allowed,
		 * since some of the attributes are known to be absent.
		 */
		hasNulls = true;
	}

	len = 0;

	if (hasNulls)
	{
		targetNullLen = BITMAPLEN(natts);
		len += targetNullLen;
	}
	else
		targetNullLen = 0;

	/*
	 * Allocate and zero the space needed.  Note that the tuple body and
	 * HeapTupleData management structure are allocated in one chunk.
	 */
	if (targetHeapTuple)
	{
		len += offsetof(HeapTupleHeaderData, t_bits);
		hoff = len = MAXALIGN(len); /* align user data safely */
		len += targetDataLen;

		*targetHeapTuple = (HeapTuple) palloc0(HEAPTUPLESIZE + len);
		(*targetHeapTuple)->t_data
			= targetTHeader
			= (HeapTupleHeader) ((char *) *targetHeapTuple + HEAPTUPLESIZE);
		(*targetHeapTuple)->t_len = len;
		(*targetHeapTuple)->t_tableOid = sourceTuple->t_tableOid;
		(*targetHeapTuple)->t_self = sourceTuple->t_self;

		targetTHeader->t_infomask = sourceTHeader->t_infomask;
		targetTHeader->t_hoff = hoff;
		HeapTupleHeaderSetNatts(targetTHeader, natts);
		HeapTupleHeaderSetDatumLength(targetTHeader, len);
		HeapTupleHeaderSetTypeId(targetTHeader, tupleDesc->tdtypeid);
		HeapTupleHeaderSetTypMod(targetTHeader, tupleDesc->tdtypmod);
		/* We also make sure that t_ctid is invalid unless explicitly set */
		ItemPointerSetInvalid(&(targetTHeader->t_ctid));
		if (targetNullLen > 0)
			nullBits = (bits8 *) ((char *) (*targetHeapTuple)->t_data
								  + offsetof(HeapTupleHeaderData, t_bits));
		targetData = (char *) (*targetHeapTuple)->t_data + hoff;
		infoMask = &(targetTHeader->t_infomask);
	}
	else
	{
		len += SizeofMinimalTupleHeader;
		hoff = len = MAXALIGN(len); /* align user data safely */
		len += targetDataLen;

		*targetMinimalTuple = (MinimalTuple) palloc0(len);
		(*targetMinimalTuple)->t_len = len;
		(*targetMinimalTuple)->t_hoff = hoff + MINIMAL_TUPLE_OFFSET;
		(*targetMinimalTuple)->t_infomask = sourceTHeader->t_infomask;
		/* Same macro works for MinimalTuples */
		HeapTupleHeaderSetNatts(*targetMinimalTuple, natts);
		if (targetNullLen > 0)
			nullBits = (bits8 *) ((char *) *targetMinimalTuple
								  + offsetof(MinimalTupleData, t_bits));
		targetData = (char *) *targetMinimalTuple + hoff;
		infoMask = &((*targetMinimalTuple)->t_infomask);
	}

	if (targetNullLen > 0)
	{
		if (sourceNullLen > 0)
		{
			/* if bitmap pre-existed copy in - all is set */
			memcpy(nullBits,
				   ((char *) sourceTHeader)
				   + offsetof(HeapTupleHeaderData, t_bits),
				   sourceNullLen);
			nullBits += sourceNullLen - 1;
		}
		else
		{
			sourceNullLen = BITMAPLEN(sourceNatts);
			/* Set NOT NULL for all existing attributes */
			memset(nullBits, 0xff, sourceNullLen);

			nullBits += sourceNullLen - 1;

			if (sourceNatts & 0x07)
			{
				/* build the mask (inverted!) */
				bitMask = 0xff << (sourceNatts & 0x07);
				/* Voila */
				*nullBits = ~bitMask;
			}
		}

		bitMask = (1 << ((sourceNatts - 1) & 0x07));
	}							/* End if have null bitmap */

	memcpy(targetData,
		   ((char *) sourceTuple->t_data) + sourceTHeader->t_hoff,
		   sourceDataLen);

	targetData += sourceDataLen;

	/* Now fill in the missing values */
	for (attnum = sourceNatts; attnum < natts; attnum++)
	{
		CompactAttribute *attr = TupleDescCompactAttr(tupleDesc, attnum);

		if (attrmiss && attrmiss[attnum].am_present)
		{
			fill_val(attr,
					 nullBits ? &nullBits : NULL,
					 &bitMask,
					 &targetData,
					 infoMask,
					 attrmiss[attnum].am_value,
					 false);
		}
		else
		{
			fill_val(attr,
					 &nullBits,
					 &bitMask,
					 &targetData,
					 infoMask,
					 (Datum) 0,
					 true);
		}
	}							/* end loop over missing attributes */
}

/* ---- VERBATIM backend/access/common/heaptuple.c: minimal_expand_tuple [static-prefixed] ---- */
/*
 * Fill in the missing values for a minimal HeapTuple
 */
static MinimalTuple
minimal_expand_tuple(HeapTuple sourceTuple, TupleDesc tupleDesc)
{
	MinimalTuple minimalTuple;

	expand_tuple(NULL, &minimalTuple, sourceTuple, tupleDesc);
	return minimalTuple;
}

/* ---- VERBATIM backend/access/common/heaptuple.c: heap_expand_tuple [static-prefixed] ---- */
/*
 * Fill in the missing values for an ordinary HeapTuple
 */
static HeapTuple
heap_expand_tuple(HeapTuple sourceTuple, TupleDesc tupleDesc)
{
	HeapTuple	heapTuple;

	expand_tuple(&heapTuple, NULL, sourceTuple, tupleDesc);
	return heapTuple;
}

/* ---- VERBATIM backend/access/common/heaptuple.c: heap_copy_tuple_as_datum [static-prefixed] ---- */
/* ----------------
 *		heap_copy_tuple_as_datum
 *
 *		copy a tuple as a composite-type Datum
 * ----------------
 */
static Datum
heap_copy_tuple_as_datum(HeapTuple tuple, TupleDesc tupleDesc)
{
	HeapTupleHeader td;

	/*
	 * If the tuple contains any external TOAST pointers, we have to inline
	 * those fields to meet the conventions for composite-type Datums.
	 */
	if (HeapTupleHasExternal(tuple))
		return toast_flatten_tuple_to_datum(tuple->t_data,
											tuple->t_len,
											tupleDesc);

	/*
	 * Fast path for easy case: just make a palloc'd copy and insert the
	 * correct composite-Datum header fields (since those may not be set if
	 * the given tuple came from disk, rather than from heap_form_tuple).
	 */
	td = (HeapTupleHeader) palloc(tuple->t_len);
	memcpy(td, tuple->t_data, tuple->t_len);

	HeapTupleHeaderSetDatumLength(td, tuple->t_len);
	HeapTupleHeaderSetTypeId(td, tupleDesc->tdtypeid);
	HeapTupleHeaderSetTypMod(td, tupleDesc->tdtypmod);

	return PointerGetDatum(td);
}

/* ---- VERBATIM backend/access/common/heaptuple.c: heap_form_tuple [static-prefixed] ---- */
/*
 * heap_form_tuple
 *		construct a tuple from the given values[] and isnull[] arrays,
 *		which are of the length indicated by tupleDescriptor->natts
 *
 * The result is allocated in the current memory context.
 */
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

/* ---- VERBATIM backend/access/common/heaptuple.c: heap_modify_tuple [static-prefixed] ---- */
/*
 * heap_modify_tuple
 *		form a new tuple from an old tuple and a set of replacement values.
 *
 * The replValues, replIsnull, and doReplace arrays must be of the length
 * indicated by tupleDesc->natts.  The new tuple is constructed using the data
 * from replValues/replIsnull at columns where doReplace is true, and using
 * the data from the old tuple at columns where doReplace is false.
 *
 * The result is allocated in the current memory context.
 */
static HeapTuple
heap_modify_tuple(HeapTuple tuple,
				  TupleDesc tupleDesc,
				  const Datum *replValues,
				  const bool *replIsnull,
				  const bool *doReplace)
{
	int			numberOfAttributes = tupleDesc->natts;
	int			attoff;
	Datum	   *values;
	bool	   *isnull;
	HeapTuple	newTuple;

	/*
	 * allocate and fill values and isnull arrays from either the tuple or the
	 * repl information, as appropriate.
	 *
	 * NOTE: it's debatable whether to use heap_deform_tuple() here or just
	 * heap_getattr() only the non-replaced columns.  The latter could win if
	 * there are many replaced columns and few non-replaced ones. However,
	 * heap_deform_tuple costs only O(N) while the heap_getattr way would cost
	 * O(N^2) if there are many non-replaced columns, so it seems better to
	 * err on the side of linear cost.
	 */
	values = (Datum *) palloc(numberOfAttributes * sizeof(Datum));
	isnull = (bool *) palloc(numberOfAttributes * sizeof(bool));

	heap_deform_tuple(tuple, tupleDesc, values, isnull);

	for (attoff = 0; attoff < numberOfAttributes; attoff++)
	{
		if (doReplace[attoff])
		{
			values[attoff] = replValues[attoff];
			isnull[attoff] = replIsnull[attoff];
		}
	}

	/*
	 * create a new tuple from the values and isnull arrays
	 */
	newTuple = heap_form_tuple(tupleDesc, values, isnull);

	pfree(values);
	pfree(isnull);

	/*
	 * copy the identification info of the old tuple: t_ctid, t_self
	 */
	newTuple->t_data->t_ctid = tuple->t_data->t_ctid;
	newTuple->t_self = tuple->t_self;
	newTuple->t_tableOid = tuple->t_tableOid;

	return newTuple;
}

/* ---- VERBATIM backend/access/common/heaptuple.c: heap_modify_tuple_by_cols [static-prefixed] ---- */
/*
 * heap_modify_tuple_by_cols
 *		form a new tuple from an old tuple and a set of replacement values.
 *
 * This is like heap_modify_tuple, except that instead of specifying which
 * column(s) to replace by a boolean map, an array of target column numbers
 * is used.  This is often more convenient when a fixed number of columns
 * are to be replaced.  The replCols, replValues, and replIsnull arrays must
 * be of length nCols.  Target column numbers are indexed from 1.
 *
 * The result is allocated in the current memory context.
 */
static HeapTuple
heap_modify_tuple_by_cols(HeapTuple tuple,
						  TupleDesc tupleDesc,
						  int nCols,
						  const int *replCols,
						  const Datum *replValues,
						  const bool *replIsnull)
{
	int			numberOfAttributes = tupleDesc->natts;
	Datum	   *values;
	bool	   *isnull;
	HeapTuple	newTuple;
	int			i;

	/*
	 * allocate and fill values and isnull arrays from the tuple, then replace
	 * selected columns from the input arrays.
	 */
	values = (Datum *) palloc(numberOfAttributes * sizeof(Datum));
	isnull = (bool *) palloc(numberOfAttributes * sizeof(bool));

	heap_deform_tuple(tuple, tupleDesc, values, isnull);

	for (i = 0; i < nCols; i++)
	{
		int			attnum = replCols[i];

		if (attnum <= 0 || attnum > numberOfAttributes)
			elog(ERROR, "invalid column number %d", attnum);
		values[attnum - 1] = replValues[i];
		isnull[attnum - 1] = replIsnull[i];
	}

	/*
	 * create a new tuple from the values and isnull arrays
	 */
	newTuple = heap_form_tuple(tupleDesc, values, isnull);

	pfree(values);
	pfree(isnull);

	/*
	 * copy the identification info of the old tuple: t_ctid, t_self
	 */
	newTuple->t_data->t_ctid = tuple->t_data->t_ctid;
	newTuple->t_self = tuple->t_self;
	newTuple->t_tableOid = tuple->t_tableOid;

	return newTuple;
}

/* ---- VERBATIM backend/access/common/heaptuple.c: heap_deform_tuple [static-prefixed] ---- */
/*
 * heap_deform_tuple
 *		Given a tuple, extract data into values/isnull arrays; this is
 *		the inverse of heap_form_tuple.
 *
 *		Storage for the values/isnull arrays is provided by the caller;
 *		it should be sized according to tupleDesc->natts not
 *		HeapTupleHeaderGetNatts(tuple->t_data).
 *
 *		Note that for pass-by-reference datatypes, the pointer placed
 *		in the Datum will point into the given tuple.
 *
 *		When all or most of a tuple's fields need to be extracted,
 *		this routine will be significantly quicker than a loop around
 *		heap_getattr; the loop will become O(N^2) as soon as any
 *		noncacheable attribute offsets are involved.
 */
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
/*
 * heap_freetuple
 */
static void
heap_freetuple(HeapTuple htup)
{
	pfree(htup);
}

/* ---- VERBATIM backend/access/common/heaptuple.c: heap_form_minimal_tuple [static-prefixed] ---- */
/*
 * heap_form_minimal_tuple
 *		construct a MinimalTuple from the given values[] and isnull[] arrays,
 *		which are of the length indicated by tupleDescriptor->natts
 *
 * This is exactly like heap_form_tuple() except that the result is a
 * "minimal" tuple lacking a HeapTupleData header as well as room for system
 * columns.
 *
 * The result is allocated in the current memory context.
 */
static MinimalTuple
heap_form_minimal_tuple(TupleDesc tupleDescriptor,
						const Datum *values,
						const bool *isnull,
						Size extra)
{
	MinimalTuple tuple;			/* return tuple */
	char	   *mem;
	Size		len,
				data_len;
	int			hoff;
	bool		hasnull = false;
	int			numberOfAttributes = tupleDescriptor->natts;
	int			i;

	Assert(extra == MAXALIGN(extra));

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
	len = SizeofMinimalTupleHeader;

	if (hasnull)
		len += BITMAPLEN(numberOfAttributes);

	hoff = len = MAXALIGN(len); /* align user data safely */

	data_len = heap_compute_data_size(tupleDescriptor, values, isnull);

	len += data_len;

	/*
	 * Allocate and zero the space needed.
	 */
	mem = palloc0(len + extra);
	memset(mem, 0, extra);
	tuple = (MinimalTuple) (mem + extra);

	/*
	 * And fill in the information.
	 */
	tuple->t_len = len;
	HeapTupleHeaderSetNatts(tuple, numberOfAttributes);
	tuple->t_hoff = hoff + MINIMAL_TUPLE_OFFSET;

	heap_fill_tuple(tupleDescriptor,
					values,
					isnull,
					(char *) tuple + hoff,
					data_len,
					&tuple->t_infomask,
					(hasnull ? tuple->t_bits : NULL));

	return tuple;
}

/* ---- VERBATIM backend/access/common/heaptuple.c: heap_free_minimal_tuple [static-prefixed] ---- */
/*
 * heap_free_minimal_tuple
 */
static void
heap_free_minimal_tuple(MinimalTuple mtup)
{
	pfree(mtup);
}

/* ---- VERBATIM backend/access/common/heaptuple.c: heap_copy_minimal_tuple [static-prefixed] ---- */
/*
 * heap_copy_minimal_tuple
 *		copy a MinimalTuple
 *
 * The result is allocated in the current memory context.
 */
static MinimalTuple
heap_copy_minimal_tuple(MinimalTuple mtup, Size extra)
{
	MinimalTuple result;
	char	   *mem;

	Assert(extra == MAXALIGN(extra));
	mem = palloc(mtup->t_len + extra);
	memset(mem, 0, extra);
	result = (MinimalTuple) (mem + extra);
	memcpy(result, mtup, mtup->t_len);
	return result;
}

/* ---- VERBATIM backend/access/common/heaptuple.c: heap_tuple_from_minimal_tuple [static-prefixed] ---- */
/*
 * heap_tuple_from_minimal_tuple
 *		create a HeapTuple by copying from a MinimalTuple;
 *		system columns are filled with zeroes
 *
 * The result is allocated in the current memory context.
 * The HeapTuple struct, tuple header, and tuple data are all allocated
 * as a single palloc() block.
 */
static HeapTuple
heap_tuple_from_minimal_tuple(MinimalTuple mtup)
{
	HeapTuple	result;
	uint32		len = mtup->t_len + MINIMAL_TUPLE_OFFSET;

	result = (HeapTuple) palloc(HEAPTUPLESIZE + len);
	result->t_len = len;
	ItemPointerSetInvalid(&(result->t_self));
	result->t_tableOid = InvalidOid;
	result->t_data = (HeapTupleHeader) ((char *) result + HEAPTUPLESIZE);
	memcpy((char *) result->t_data + MINIMAL_TUPLE_OFFSET, mtup, mtup->t_len);
	memset(result->t_data, 0, offsetof(HeapTupleHeaderData, t_infomask2));
	return result;
}

/* ---- VERBATIM backend/access/common/heaptuple.c: minimal_tuple_from_heap_tuple [static-prefixed] ---- */
/*
 * minimal_tuple_from_heap_tuple
 *		create a MinimalTuple by copying from a HeapTuple
 *
 * The result is allocated in the current memory context.
 */
static MinimalTuple
minimal_tuple_from_heap_tuple(HeapTuple htup, Size extra)
{
	MinimalTuple result;
	char	   *mem;
	uint32		len;

	Assert(extra == MAXALIGN(extra));
	Assert(htup->t_len > MINIMAL_TUPLE_OFFSET);
	len = htup->t_len - MINIMAL_TUPLE_OFFSET;
	mem = palloc(len + extra);
	memset(mem, 0, extra);
	result = (MinimalTuple) (mem + extra);
	memcpy(result, (char *) htup->t_data + MINIMAL_TUPLE_OFFSET, len);

	result->t_len = len;
	return result;
}

/* ---- VERBATIM include/access/htup_details.h: fastgetattr ---- */
/*
 *	fastgetattr
 *		Fetch a user attribute's value as a Datum (might be either a
 *		value, or a pointer into the data area of the tuple).
 *
 *		This must not be used when a system attribute might be requested.
 *		Furthermore, the passed attnum MUST be valid.  Use heap_getattr()
 *		instead, if in doubt.
 *
 *		This gets called many times, so we macro the cacheable and NULL
 *		lookups, and call nocachegetattr() for the rest.
 */
static inline Datum
fastgetattr(HeapTuple tup, int attnum, TupleDesc tupleDesc, bool *isnull)
{
	Assert(attnum > 0);

	*isnull = false;
	if (HeapTupleNoNulls(tup))
	{
		CompactAttribute *att;

		att = TupleDescCompactAttr(tupleDesc, attnum - 1);
		if (att->attcacheoff >= 0)
			return fetchatt(att, (char *) tup->t_data + tup->t_data->t_hoff +
							att->attcacheoff);
		else
			return nocachegetattr(tup, attnum, tupleDesc);
	}
	else
	{
		if (att_isnull(attnum - 1, tup->t_data->t_bits))
		{
			*isnull = true;
			return (Datum) NULL;
		}
		else
			return nocachegetattr(tup, attnum, tupleDesc);
	}
}

/* ---- VERBATIM include/access/htup_details.h: heap_getattr ---- */
/*
 *	heap_getattr
 *		Extract an attribute of a heap tuple and return it as a Datum.
 *		This works for either system or user attributes.  The given attnum
 *		is properly range-checked.
 *
 *		If the field in question has a NULL value, we return a zero Datum
 *		and set *isnull == true.  Otherwise, we set *isnull == false.
 *
 *		<tup> is the pointer to the heap tuple.  <attnum> is the attribute
 *		number of the column (field) caller wants.  <tupleDesc> is a
 *		pointer to the structure describing the row and all its fields.
 *
 */
static inline Datum
heap_getattr(HeapTuple tup, int attnum, TupleDesc tupleDesc, bool *isnull)
{
	if (attnum > 0)
	{
		if (attnum > (int) HeapTupleHeaderGetNatts(tup->t_data))
			return getmissingattr(tupleDesc, attnum, isnull);
		else
			return fastgetattr(tup, attnum, tupleDesc, isnull);
	}
	else
		return heap_getsysattr(tup, attnum, tupleDesc, isnull);
}

/* ---- VERBATIM backend/access/common/tupdesc.c: populate_compact_attribute_internal ---- */
/*
 * populate_compact_attribute_internal
 *		Helper function for populate_compact_attribute()
 */
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
/*
 * populate_compact_attribute
 *		Fill in the corresponding CompactAttribute element from the
 *		Form_pg_attribute for the given attribute number.  This must be called
 *		whenever a change is made to a Form_pg_attribute in the TupleDesc.
 */
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

/* ---- VERBATIM backend/access/common/tupdesc.c: verify_compact_attribute [static-prefixed] ---- */
/*
 * verify_compact_attribute
 *		In Assert enabled builds, we verify that the CompactAttribute is
 *		populated correctly.  This helps find bugs in places such as ALTER
 *		TABLE where code makes changes to the FormData_pg_attribute but
 *		forgets to call populate_compact_attribute().
 *
 * This is used in TupleDescCompactAttr(), but declared here to allow access
 * to populate_compact_attribute_internal().
 */
static void
verify_compact_attribute(TupleDesc tupdesc, int attnum)
{
#ifdef USE_ASSERT_CHECKING
	CompactAttribute cattr;
	Form_pg_attribute attr = TupleDescAttr(tupdesc, attnum);
	CompactAttribute tmp;

	/*
	 * Make a temp copy of the TupleDesc's CompactAttribute.  This may be a
	 * shared TupleDesc and the attcacheoff might get changed by another
	 * backend.
	 */
	memcpy(&cattr, &tupdesc->compact_attrs[attnum], sizeof(CompactAttribute));

	/*
	 * Populate the temporary CompactAttribute from the corresponding
	 * Form_pg_attribute
	 */
	populate_compact_attribute_internal(attr, &tmp);

	/*
	 * Make the attcacheoff match since it's been reset to -1 by
	 * populate_compact_attribute_internal.  Same with attnullability.
	 */
	tmp.attcacheoff = cattr.attcacheoff;
	tmp.attnullability = cattr.attnullability;

	/* Check the freshly populated CompactAttribute matches the TupleDesc's */
	Assert(memcmp(&tmp, &cattr, sizeof(CompactAttribute)) == 0);
#endif
}

/* ---- VERBATIM backend/access/common/tupdesc.c: CreateTemplateTupleDesc [static-prefixed] ---- */
/*
 * CreateTemplateTupleDesc
 *		This function allocates an empty tuple descriptor structure.
 *
 * Tuple type ID information is initially set for an anonymous record type;
 * caller can overwrite this if needed.
 */
static TupleDesc
CreateTemplateTupleDesc(int natts)
{
	TupleDesc	desc;

	/*
	 * sanity checks
	 */
	Assert(natts >= 0);

	/*
	 * Allocate enough memory for the tuple descriptor, the CompactAttribute
	 * array and also an array of FormData_pg_attribute.
	 *
	 * Note: the FormData_pg_attribute array stride is
	 * sizeof(FormData_pg_attribute), since we declare the array elements as
	 * FormData_pg_attribute for notational convenience.  However, we only
	 * guarantee that the first ATTRIBUTE_FIXED_PART_SIZE bytes of each entry
	 * are valid; most code that copies tupdesc entries around copies just
	 * that much.  In principle that could be less due to trailing padding,
	 * although with the current definition of pg_attribute there probably
	 * isn't any padding.
	 */
	desc = (TupleDesc) palloc(offsetof(struct TupleDescData, compact_attrs) +
							  natts * sizeof(CompactAttribute) +
							  natts * sizeof(FormData_pg_attribute));

	/*
	 * Initialize other fields of the tupdesc.
	 */
	desc->natts = natts;
	desc->constr = NULL;
	desc->tdtypeid = RECORDOID;
	desc->tdtypmod = -1;
	desc->tdrefcount = -1;		/* assume not reference-counted */

	return desc;
}

/* ---- VERBATIM backend/access/common/tupdesc.c: CreateTupleDesc [static-prefixed] ---- */
/*
 * CreateTupleDesc
 *		This function allocates a new TupleDesc by copying a given
 *		Form_pg_attribute array.
 *
 * Tuple type ID information is initially set for an anonymous record type;
 * caller can overwrite this if needed.
 */
static TupleDesc
CreateTupleDesc(int natts, Form_pg_attribute *attrs)
{
	TupleDesc	desc;
	int			i;

	desc = CreateTemplateTupleDesc(natts);

	for (i = 0; i < natts; ++i)
	{
		memcpy(TupleDescAttr(desc, i), attrs[i], ATTRIBUTE_FIXED_PART_SIZE);
		populate_compact_attribute(desc, i);
	}
	return desc;
}

/* ---- VERBATIM backend/access/common/tupdesc.c: CreateTupleDescCopy [static-prefixed] ---- */
/*
 * CreateTupleDescCopy
 *		This function creates a new TupleDesc by copying from an existing
 *		TupleDesc.
 *
 * !!! Constraints and defaults are not copied !!!
 */
static TupleDesc
CreateTupleDescCopy(TupleDesc tupdesc)
{
	TupleDesc	desc;
	int			i;

	desc = CreateTemplateTupleDesc(tupdesc->natts);

	/* Flat-copy the attribute array */
	memcpy(TupleDescAttr(desc, 0),
		   TupleDescAttr(tupdesc, 0),
		   desc->natts * sizeof(FormData_pg_attribute));

	/*
	 * Since we're not copying constraints and defaults, clear fields
	 * associated with them.
	 */
	for (i = 0; i < desc->natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(desc, i);

		att->attnotnull = false;
		att->atthasdef = false;
		att->atthasmissing = false;
		att->attidentity = '\0';
		att->attgenerated = '\0';

		populate_compact_attribute(desc, i);
	}

	/* We can copy the tuple type identification, too */
	desc->tdtypeid = tupdesc->tdtypeid;
	desc->tdtypmod = tupdesc->tdtypmod;

	return desc;
}

/* ---- VERBATIM backend/access/common/tupdesc.c: CreateTupleDescTruncatedCopy [static-prefixed] ---- */
/*
 * CreateTupleDescTruncatedCopy
 *		This function creates a new TupleDesc with only the first 'natts'
 *		attributes from an existing TupleDesc
 *
 * !!! Constraints and defaults are not copied !!!
 */
static TupleDesc
CreateTupleDescTruncatedCopy(TupleDesc tupdesc, int natts)
{
	TupleDesc	desc;
	int			i;

	Assert(natts <= tupdesc->natts);

	desc = CreateTemplateTupleDesc(natts);

	/* Flat-copy the attribute array */
	memcpy(TupleDescAttr(desc, 0),
		   TupleDescAttr(tupdesc, 0),
		   desc->natts * sizeof(FormData_pg_attribute));

	/*
	 * Since we're not copying constraints and defaults, clear fields
	 * associated with them.
	 */
	for (i = 0; i < desc->natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(desc, i);

		att->attnotnull = false;
		att->atthasdef = false;
		att->atthasmissing = false;
		att->attidentity = '\0';
		att->attgenerated = '\0';

		populate_compact_attribute(desc, i);
	}

	/* We can copy the tuple type identification, too */
	desc->tdtypeid = tupdesc->tdtypeid;
	desc->tdtypmod = tupdesc->tdtypmod;

	return desc;
}

/* ---- VERBATIM backend/access/common/tupdesc.c: CreateTupleDescCopyConstr [static-prefixed] ---- */
/*
 * CreateTupleDescCopyConstr
 *		This function creates a new TupleDesc by copying from an existing
 *		TupleDesc (including its constraints and defaults).
 */
static TupleDesc
CreateTupleDescCopyConstr(TupleDesc tupdesc)
{
	TupleDesc	desc;
	TupleConstr *constr = tupdesc->constr;
	int			i;

	desc = CreateTemplateTupleDesc(tupdesc->natts);

	/* Flat-copy the attribute array */
	memcpy(TupleDescAttr(desc, 0),
		   TupleDescAttr(tupdesc, 0),
		   desc->natts * sizeof(FormData_pg_attribute));

	for (i = 0; i < desc->natts; i++)
	{
		populate_compact_attribute(desc, i);

		TupleDescCompactAttr(desc, i)->attnullability =
			TupleDescCompactAttr(tupdesc, i)->attnullability;
	}

	/* Copy the TupleConstr data structure, if any */
	if (constr)
	{
		TupleConstr *cpy = (TupleConstr *) palloc0(sizeof(TupleConstr));

		cpy->has_not_null = constr->has_not_null;
		cpy->has_generated_stored = constr->has_generated_stored;
		cpy->has_generated_virtual = constr->has_generated_virtual;

		if ((cpy->num_defval = constr->num_defval) > 0)
		{
			cpy->defval = (AttrDefault *) palloc(cpy->num_defval * sizeof(AttrDefault));
			memcpy(cpy->defval, constr->defval, cpy->num_defval * sizeof(AttrDefault));
			for (i = cpy->num_defval - 1; i >= 0; i--)
				cpy->defval[i].adbin = pstrdup(constr->defval[i].adbin);
		}

		if (constr->missing)
		{
			cpy->missing = (AttrMissing *) palloc(tupdesc->natts * sizeof(AttrMissing));
			memcpy(cpy->missing, constr->missing, tupdesc->natts * sizeof(AttrMissing));
			for (i = tupdesc->natts - 1; i >= 0; i--)
			{
				if (constr->missing[i].am_present)
				{
					CompactAttribute *attr = TupleDescCompactAttr(tupdesc, i);

					cpy->missing[i].am_value = datumCopy(constr->missing[i].am_value,
														 attr->attbyval,
														 attr->attlen);
				}
			}
		}

		if ((cpy->num_check = constr->num_check) > 0)
		{
			cpy->check = (ConstrCheck *) palloc(cpy->num_check * sizeof(ConstrCheck));
			memcpy(cpy->check, constr->check, cpy->num_check * sizeof(ConstrCheck));
			for (i = cpy->num_check - 1; i >= 0; i--)
			{
				cpy->check[i].ccname = pstrdup(constr->check[i].ccname);
				cpy->check[i].ccbin = pstrdup(constr->check[i].ccbin);
				cpy->check[i].ccenforced = constr->check[i].ccenforced;
				cpy->check[i].ccvalid = constr->check[i].ccvalid;
				cpy->check[i].ccnoinherit = constr->check[i].ccnoinherit;
			}
		}

		desc->constr = cpy;
	}

	/* We can copy the tuple type identification, too */
	desc->tdtypeid = tupdesc->tdtypeid;
	desc->tdtypmod = tupdesc->tdtypmod;

	return desc;
}

/* ---- VERBATIM backend/access/common/tupdesc.c: TupleDescCopy [static-prefixed] ---- */
/*
 * TupleDescCopy
 *		Copy a tuple descriptor into caller-supplied memory.
 *		The memory may be shared memory mapped at any address, and must
 *		be sufficient to hold TupleDescSize(src) bytes.
 *
 * !!! Constraints and defaults are not copied !!!
 */
static void
TupleDescCopy(TupleDesc dst, TupleDesc src)
{
	int			i;

	/* Flat-copy the header and attribute arrays */
	memcpy(dst, src, TupleDescSize(src));

	/*
	 * Since we're not copying constraints and defaults, clear fields
	 * associated with them.
	 */
	for (i = 0; i < dst->natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(dst, i);

		att->attnotnull = false;
		att->atthasdef = false;
		att->atthasmissing = false;
		att->attidentity = '\0';
		att->attgenerated = '\0';

		populate_compact_attribute(dst, i);
	}
	dst->constr = NULL;

	/*
	 * Also, assume the destination is not to be ref-counted.  (Copying the
	 * source's refcount would be wrong in any case.)
	 */
	dst->tdrefcount = -1;
}

/* ---- VERBATIM backend/access/common/tupdesc.c: TupleDescCopyEntry [static-prefixed] ---- */
/*
 * TupleDescCopyEntry
 *		This function copies a single attribute structure from one tuple
 *		descriptor to another.
 *
 * !!! Constraints and defaults are not copied !!!
 */
static void
TupleDescCopyEntry(TupleDesc dst, AttrNumber dstAttno,
				   TupleDesc src, AttrNumber srcAttno)
{
	Form_pg_attribute dstAtt = TupleDescAttr(dst, dstAttno - 1);
	Form_pg_attribute srcAtt = TupleDescAttr(src, srcAttno - 1);

	/*
	 * sanity checks
	 */
	Assert(PointerIsValid(src));
	Assert(PointerIsValid(dst));
	Assert(srcAttno >= 1);
	Assert(srcAttno <= src->natts);
	Assert(dstAttno >= 1);
	Assert(dstAttno <= dst->natts);

	memcpy(dstAtt, srcAtt, ATTRIBUTE_FIXED_PART_SIZE);

	dstAtt->attnum = dstAttno;

	/* since we're not copying constraints or defaults, clear these */
	dstAtt->attnotnull = false;
	dstAtt->atthasdef = false;
	dstAtt->atthasmissing = false;
	dstAtt->attidentity = '\0';
	dstAtt->attgenerated = '\0';

	populate_compact_attribute(dst, dstAttno - 1);
}

/* ---- VERBATIM backend/access/common/tupdesc.c: FreeTupleDesc [static-prefixed] ---- */
/*
 * Free a TupleDesc including all substructure
 */
static void
FreeTupleDesc(TupleDesc tupdesc)
{
	int			i;

	/*
	 * Possibly this should assert tdrefcount == 0, to disallow explicit
	 * freeing of un-refcounted tupdescs?
	 */
	Assert(tupdesc->tdrefcount <= 0);

	if (tupdesc->constr)
	{
		if (tupdesc->constr->num_defval > 0)
		{
			AttrDefault *attrdef = tupdesc->constr->defval;

			for (i = tupdesc->constr->num_defval - 1; i >= 0; i--)
				pfree(attrdef[i].adbin);
			pfree(attrdef);
		}
		if (tupdesc->constr->missing)
		{
			AttrMissing *attrmiss = tupdesc->constr->missing;

			for (i = tupdesc->natts - 1; i >= 0; i--)
			{
				if (attrmiss[i].am_present
					&& !TupleDescAttr(tupdesc, i)->attbyval)
					pfree(DatumGetPointer(attrmiss[i].am_value));
			}
			pfree(attrmiss);
		}
		if (tupdesc->constr->num_check > 0)
		{
			ConstrCheck *check = tupdesc->constr->check;

			for (i = tupdesc->constr->num_check - 1; i >= 0; i--)
			{
				pfree(check[i].ccname);
				pfree(check[i].ccbin);
			}
			pfree(check);
		}
		pfree(tupdesc->constr);
	}

	pfree(tupdesc);
}

/* ---- VERBATIM backend/access/common/tupdesc.c: equalTupleDescs [static-prefixed] ---- */
/*
 * Compare two TupleDesc structures for logical equality
 */
static bool
equalTupleDescs(TupleDesc tupdesc1, TupleDesc tupdesc2)
{
	int			i,
				n;

	if (tupdesc1->natts != tupdesc2->natts)
		return false;
	if (tupdesc1->tdtypeid != tupdesc2->tdtypeid)
		return false;

	/* tdtypmod and tdrefcount are not checked */

	for (i = 0; i < tupdesc1->natts; i++)
	{
		Form_pg_attribute attr1 = TupleDescAttr(tupdesc1, i);
		Form_pg_attribute attr2 = TupleDescAttr(tupdesc2, i);

		/*
		 * We do not need to check every single field here: we can disregard
		 * attrelid and attnum (which were used to place the row in the attrs
		 * array in the first place).  It might look like we could dispense
		 * with checking attlen/attbyval/attalign, since these are derived
		 * from atttypid; but in the case of dropped columns we must check
		 * them (since atttypid will be zero for all dropped columns) and in
		 * general it seems safer to check them always.
		 *
		 * We intentionally ignore atthasmissing, since that's not very
		 * relevant in tupdescs, which lack the attmissingval field.
		 */
		if (strcmp(NameStr(attr1->attname), NameStr(attr2->attname)) != 0)
			return false;
		if (attr1->atttypid != attr2->atttypid)
			return false;
		if (attr1->attlen != attr2->attlen)
			return false;
		if (attr1->attndims != attr2->attndims)
			return false;
		if (attr1->atttypmod != attr2->atttypmod)
			return false;
		if (attr1->attbyval != attr2->attbyval)
			return false;
		if (attr1->attalign != attr2->attalign)
			return false;
		if (attr1->attstorage != attr2->attstorage)
			return false;
		if (attr1->attcompression != attr2->attcompression)
			return false;
		if (attr1->attnotnull != attr2->attnotnull)
			return false;

		/*
		 * When the column has a not-null constraint, we also need to consider
		 * its validity aspect, which only manifests in CompactAttribute->
		 * attnullability, so verify that.
		 */
		if (attr1->attnotnull)
		{
			CompactAttribute *cattr1 = TupleDescCompactAttr(tupdesc1, i);
			CompactAttribute *cattr2 = TupleDescCompactAttr(tupdesc2, i);

			Assert(cattr1->attnullability != ATTNULLABLE_UNKNOWN);
			Assert((cattr1->attnullability == ATTNULLABLE_UNKNOWN) ==
				   (cattr2->attnullability == ATTNULLABLE_UNKNOWN));

			if (cattr1->attnullability != cattr2->attnullability)
				return false;
		}
		if (attr1->atthasdef != attr2->atthasdef)
			return false;
		if (attr1->attidentity != attr2->attidentity)
			return false;
		if (attr1->attgenerated != attr2->attgenerated)
			return false;
		if (attr1->attisdropped != attr2->attisdropped)
			return false;
		if (attr1->attislocal != attr2->attislocal)
			return false;
		if (attr1->attinhcount != attr2->attinhcount)
			return false;
		if (attr1->attcollation != attr2->attcollation)
			return false;
		/* variable-length fields are not even present... */
	}

	if (tupdesc1->constr != NULL)
	{
		TupleConstr *constr1 = tupdesc1->constr;
		TupleConstr *constr2 = tupdesc2->constr;

		if (constr2 == NULL)
			return false;
		if (constr1->has_not_null != constr2->has_not_null)
			return false;
		if (constr1->has_generated_stored != constr2->has_generated_stored)
			return false;
		if (constr1->has_generated_virtual != constr2->has_generated_virtual)
			return false;
		n = constr1->num_defval;
		if (n != (int) constr2->num_defval)
			return false;
		/* We assume here that both AttrDefault arrays are in adnum order */
		for (i = 0; i < n; i++)
		{
			AttrDefault *defval1 = constr1->defval + i;
			AttrDefault *defval2 = constr2->defval + i;

			if (defval1->adnum != defval2->adnum)
				return false;
			if (strcmp(defval1->adbin, defval2->adbin) != 0)
				return false;
		}
		if (constr1->missing)
		{
			if (!constr2->missing)
				return false;
			for (i = 0; i < tupdesc1->natts; i++)
			{
				AttrMissing *missval1 = constr1->missing + i;
				AttrMissing *missval2 = constr2->missing + i;

				if (missval1->am_present != missval2->am_present)
					return false;
				if (missval1->am_present)
				{
					CompactAttribute *missatt1 = TupleDescCompactAttr(tupdesc1, i);

					if (!datumIsEqual(missval1->am_value, missval2->am_value,
									  missatt1->attbyval, missatt1->attlen))
						return false;
				}
			}
		}
		else if (constr2->missing)
			return false;
		n = constr1->num_check;
		if (n != (int) constr2->num_check)
			return false;

		/*
		 * Similarly, we rely here on the ConstrCheck entries being sorted by
		 * name.  If there are duplicate names, the outcome of the comparison
		 * is uncertain, but that should not happen.
		 */
		for (i = 0; i < n; i++)
		{
			ConstrCheck *check1 = constr1->check + i;
			ConstrCheck *check2 = constr2->check + i;

			if (!(strcmp(check1->ccname, check2->ccname) == 0 &&
				  strcmp(check1->ccbin, check2->ccbin) == 0 &&
				  check1->ccenforced == check2->ccenforced &&
				  check1->ccvalid == check2->ccvalid &&
				  check1->ccnoinherit == check2->ccnoinherit))
				return false;
		}
	}
	else if (tupdesc2->constr != NULL)
		return false;
	return true;
}

/* ---- VERBATIM backend/access/common/tupdesc.c: equalRowTypes [static-prefixed] ---- */
/*
 * equalRowTypes
 *
 * This determines whether two tuple descriptors have equal row types.  This
 * only checks those fields in pg_attribute that are applicable for row types,
 * while ignoring those fields that define the physical row storage or those
 * that define table column metadata.
 *
 * Specifically, this checks:
 *
 * - same number of attributes
 * - same composite type ID (but could both be zero)
 * - corresponding attributes (in order) have same the name, type, typmod,
 *   collation
 *
 * This is used to check whether two record types are compatible, whether
 * function return row types are the same, and other similar situations.
 *
 * (XXX There was some discussion whether attndims should be checked here, but
 * for now it has been decided not to.)
 *
 * Note: We deliberately do not check the tdtypmod field.  This allows
 * typcache.c to use this routine to see if a cached record type matches a
 * requested type.
 */
static bool
equalRowTypes(TupleDesc tupdesc1, TupleDesc tupdesc2)
{
	if (tupdesc1->natts != tupdesc2->natts)
		return false;
	if (tupdesc1->tdtypeid != tupdesc2->tdtypeid)
		return false;

	for (int i = 0; i < tupdesc1->natts; i++)
	{
		Form_pg_attribute attr1 = TupleDescAttr(tupdesc1, i);
		Form_pg_attribute attr2 = TupleDescAttr(tupdesc2, i);

		if (strcmp(NameStr(attr1->attname), NameStr(attr2->attname)) != 0)
			return false;
		if (attr1->atttypid != attr2->atttypid)
			return false;
		if (attr1->atttypmod != attr2->atttypmod)
			return false;
		if (attr1->attcollation != attr2->attcollation)
			return false;

		/* Record types derived from tables could have dropped fields. */
		if (attr1->attisdropped != attr2->attisdropped)
			return false;
	}

	return true;
}

/* ---- VERBATIM backend/access/common/tupdesc.c: hashRowType [static-prefixed] ---- */
/*
 * hashRowType
 *
 * If two tuple descriptors would be considered equal by equalRowTypes()
 * then their hash value will be equal according to this function.
 */
static uint32
hashRowType(TupleDesc desc)
{
	uint32		s;
	int			i;

	s = hash_combine(0, hash_uint32(desc->natts));
	s = hash_combine(s, hash_uint32(desc->tdtypeid));
	for (i = 0; i < desc->natts; ++i)
		s = hash_combine(s, hash_uint32(TupleDescAttr(desc, i)->atttypid));

	return s;
}

/* ---- VERBATIM backend/access/common/tupdesc.c: TupleDescInitEntry [static-prefixed] ---- */
/*
 * TupleDescInitEntry
 *		This function initializes a single attribute structure in
 *		a previously allocated tuple descriptor.
 *
 * If attributeName is NULL, the attname field is set to an empty string
 * (this is for cases where we don't know or need a name for the field).
 * Also, some callers use this function to change the datatype-related fields
 * in an existing tupdesc; they pass attributeName = NameStr(att->attname)
 * to indicate that the attname field shouldn't be modified.
 *
 * Note that attcollation is set to the default for the specified datatype.
 * If a nondefault collation is needed, insert it afterwards using
 * TupleDescInitEntryCollation.
 */
static void
TupleDescInitEntry(TupleDesc desc,
				   AttrNumber attributeNumber,
				   const char *attributeName,
				   Oid oidtypeid,
				   int32 typmod,
				   int attdim)
{
	HeapTuple	tuple;
	Form_pg_type typeForm;
	Form_pg_attribute att;

	/*
	 * sanity checks
	 */
	Assert(PointerIsValid(desc));
	Assert(attributeNumber >= 1);
	Assert(attributeNumber <= desc->natts);
	Assert(attdim >= 0);
	Assert(attdim <= PG_INT16_MAX);

	/*
	 * initialize the attribute fields
	 */
	att = TupleDescAttr(desc, attributeNumber - 1);

	att->attrelid = 0;			/* dummy value */

	/*
	 * Note: attributeName can be NULL, because the planner doesn't always
	 * fill in valid resname values in targetlists, particularly for resjunk
	 * attributes. Also, do nothing if caller wants to re-use the old attname.
	 */
	if (attributeName == NULL)
		MemSet(NameStr(att->attname), 0, NAMEDATALEN);
	else if (attributeName != NameStr(att->attname))
		namestrcpy(&(att->attname), attributeName);

	att->atttypmod = typmod;

	att->attnum = attributeNumber;
	att->attndims = attdim;

	att->attnotnull = false;
	att->atthasdef = false;
	att->atthasmissing = false;
	att->attidentity = '\0';
	att->attgenerated = '\0';
	att->attisdropped = false;
	att->attislocal = true;
	att->attinhcount = 0;
	/* variable-length fields are not present in tupledescs */

	tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(oidtypeid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for type %u", oidtypeid);
	typeForm = (Form_pg_type) GETSTRUCT(tuple);

	att->atttypid = oidtypeid;
	att->attlen = typeForm->typlen;
	att->attbyval = typeForm->typbyval;
	att->attalign = typeForm->typalign;
	att->attstorage = typeForm->typstorage;
	att->attcompression = InvalidCompressionMethod;
	att->attcollation = typeForm->typcollation;

	populate_compact_attribute(desc, attributeNumber - 1);

	ReleaseSysCache(tuple);
}

/* ---- VERBATIM backend/access/common/tupdesc.c: TupleDescInitBuiltinEntry [static-prefixed] ---- */
/*
 * TupleDescInitBuiltinEntry
 *		Initialize a tuple descriptor without catalog access.  Only
 *		a limited range of builtin types are supported.
 */
static void
TupleDescInitBuiltinEntry(TupleDesc desc,
						  AttrNumber attributeNumber,
						  const char *attributeName,
						  Oid oidtypeid,
						  int32 typmod,
						  int attdim)
{
	Form_pg_attribute att;

	/* sanity checks */
	Assert(PointerIsValid(desc));
	Assert(attributeNumber >= 1);
	Assert(attributeNumber <= desc->natts);
	Assert(attdim >= 0);
	Assert(attdim <= PG_INT16_MAX);

	/* initialize the attribute fields */
	att = TupleDescAttr(desc, attributeNumber - 1);
	att->attrelid = 0;			/* dummy value */

	/* unlike TupleDescInitEntry, we require an attribute name */
	Assert(attributeName != NULL);
	namestrcpy(&(att->attname), attributeName);

	att->atttypmod = typmod;

	att->attnum = attributeNumber;
	att->attndims = attdim;

	att->attnotnull = false;
	att->atthasdef = false;
	att->atthasmissing = false;
	att->attidentity = '\0';
	att->attgenerated = '\0';
	att->attisdropped = false;
	att->attislocal = true;
	att->attinhcount = 0;
	/* variable-length fields are not present in tupledescs */

	att->atttypid = oidtypeid;

	/*
	 * Our goal here is to support just enough types to let basic builtin
	 * commands work without catalog access - e.g. so that we can do certain
	 * things even in processes that are not connected to a database.
	 */
	switch (oidtypeid)
	{
		case TEXTOID:
		case TEXTARRAYOID:
			att->attlen = -1;
			att->attbyval = false;
			att->attalign = TYPALIGN_INT;
			att->attstorage = TYPSTORAGE_EXTENDED;
			att->attcompression = InvalidCompressionMethod;
			att->attcollation = DEFAULT_COLLATION_OID;
			break;

		case BOOLOID:
			att->attlen = 1;
			att->attbyval = true;
			att->attalign = TYPALIGN_CHAR;
			att->attstorage = TYPSTORAGE_PLAIN;
			att->attcompression = InvalidCompressionMethod;
			att->attcollation = InvalidOid;
			break;

		case INT4OID:
			att->attlen = 4;
			att->attbyval = true;
			att->attalign = TYPALIGN_INT;
			att->attstorage = TYPSTORAGE_PLAIN;
			att->attcompression = InvalidCompressionMethod;
			att->attcollation = InvalidOid;
			break;

		case INT8OID:
			att->attlen = 8;
			att->attbyval = FLOAT8PASSBYVAL;
			att->attalign = TYPALIGN_DOUBLE;
			att->attstorage = TYPSTORAGE_PLAIN;
			att->attcompression = InvalidCompressionMethod;
			att->attcollation = InvalidOid;
			break;

		case OIDOID:
			att->attlen = 4;
			att->attbyval = true;
			att->attalign = TYPALIGN_INT;
			att->attstorage = TYPSTORAGE_PLAIN;
			att->attcompression = InvalidCompressionMethod;
			att->attcollation = InvalidOid;
			break;

		default:
			elog(ERROR, "unsupported type %u", oidtypeid);
	}

	populate_compact_attribute(desc, attributeNumber - 1);
}

/* ---- VERBATIM backend/access/common/tupdesc.c: TupleDescInitEntryCollation [static-prefixed] ---- */
/*
 * TupleDescInitEntryCollation
 *
 * Assign a nondefault collation to a previously initialized tuple descriptor
 * entry.
 */
static void
TupleDescInitEntryCollation(TupleDesc desc,
							AttrNumber attributeNumber,
							Oid collationid)
{
	/*
	 * sanity checks
	 */
	Assert(PointerIsValid(desc));
	Assert(attributeNumber >= 1);
	Assert(attributeNumber <= desc->natts);

	TupleDescAttr(desc, attributeNumber - 1)->attcollation = collationid;
}

/* ---- VERBATIM backend/access/common/tupdesc.c: BuildDescFromLists [static-prefixed] ---- */
/*
 * BuildDescFromLists
 *
 * Build a TupleDesc given lists of column names (as String nodes),
 * column type OIDs, typmods, and collation OIDs.
 *
 * No constraints are generated.
 *
 * This is for use with functions returning RECORD.
 */
static TupleDesc
BuildDescFromLists(const List *names, const List *types, const List *typmods, const List *collations)
{
	int			natts;
	AttrNumber	attnum;
	ListCell   *l1;
	ListCell   *l2;
	ListCell   *l3;
	ListCell   *l4;
	TupleDesc	desc;

	natts = list_length(names);
	Assert(natts == list_length(types));
	Assert(natts == list_length(typmods));
	Assert(natts == list_length(collations));

	/*
	 * allocate a new tuple descriptor
	 */
	desc = CreateTemplateTupleDesc(natts);

	attnum = 0;
	forfour(l1, names, l2, types, l3, typmods, l4, collations)
	{
		char	   *attname = strVal(lfirst(l1));
		Oid			atttypid = lfirst_oid(l2);
		int32		atttypmod = lfirst_int(l3);
		Oid			attcollation = lfirst_oid(l4);

		attnum++;

		TupleDescInitEntry(desc, attnum, attname, atttypid, atttypmod, 0);
		TupleDescInitEntryCollation(desc, attnum, attcollation);
	}

	return desc;
}

/* ---- VERBATIM backend/access/common/attmap.c: make_attrmap [static-prefixed] ---- */
/*
 * make_attrmap
 *
 * Utility routine to allocate an attribute map in the current memory
 * context.
 */
static AttrMap *
make_attrmap(int maplen)
{
	AttrMap    *res;

	res = (AttrMap *) palloc0(sizeof(AttrMap));
	res->maplen = maplen;
	res->attnums = (AttrNumber *) palloc0(sizeof(AttrNumber) * maplen);
	return res;
}

/* ---- VERBATIM backend/access/common/attmap.c: free_attrmap [static-prefixed] ---- */
/*
 * free_attrmap
 *
 * Utility routine to release an attribute map.
 */
static void
free_attrmap(AttrMap *map)
{
	pfree(map->attnums);
	pfree(map);
}

/* ---- VERBATIM backend/access/common/attmap.c: build_attrmap_by_position [static-prefixed] ---- */
/*
 * build_attrmap_by_position
 *
 * Return a palloc'd bare attribute map for tuple conversion, matching input
 * and output columns by position.  Dropped columns are ignored in both input
 * and output, marked as 0.  This is normally a subroutine for
 * convert_tuples_by_position in tupconvert.c, but it can be used standalone.
 *
 * Note: the errdetail messages speak of indesc as the "returned" rowtype,
 * outdesc as the "expected" rowtype.  This is okay for current uses but
 * might need generalization in future.
 */
static AttrMap *
build_attrmap_by_position(TupleDesc indesc,
						  TupleDesc outdesc,
						  const char *msg)
{
	AttrMap    *attrMap;
	int			nincols;
	int			noutcols;
	int			n;
	int			i;
	int			j;
	bool		same;

	/*
	 * The length is computed as the number of attributes of the expected
	 * rowtype as it includes dropped attributes in its count.
	 */
	n = outdesc->natts;
	attrMap = make_attrmap(n);

	j = 0;						/* j is next physical input attribute */
	nincols = noutcols = 0;		/* these count non-dropped attributes */
	same = true;
	for (i = 0; i < n; i++)
	{
		Form_pg_attribute outatt = TupleDescAttr(outdesc, i);

		if (outatt->attisdropped)
			continue;			/* attrMap->attnums[i] is already 0 */
		noutcols++;
		for (; j < indesc->natts; j++)
		{
			Form_pg_attribute inatt = TupleDescAttr(indesc, j);

			if (inatt->attisdropped)
				continue;
			nincols++;

			/* Found matching column, now check type */
			if (outatt->atttypid != inatt->atttypid ||
				(outatt->atttypmod != inatt->atttypmod && outatt->atttypmod >= 0))
				ereport(ERROR,
						(errcode(ERRCODE_DATATYPE_MISMATCH),
						 errmsg_internal("%s", _(msg)),
						 errdetail("Returned type %s does not match expected type %s in column \"%s\" (position %d).",
								   format_type_with_typemod(inatt->atttypid,
															inatt->atttypmod),
								   format_type_with_typemod(outatt->atttypid,
															outatt->atttypmod),
								   NameStr(outatt->attname),
								   noutcols)));
			attrMap->attnums[i] = (AttrNumber) (j + 1);
			j++;
			break;
		}
		if (attrMap->attnums[i] == 0)
			same = false;		/* we'll complain below */
	}

	/* Check for unused input columns */
	for (; j < indesc->natts; j++)
	{
		if (TupleDescCompactAttr(indesc, j)->attisdropped)
			continue;
		nincols++;
		same = false;			/* we'll complain below */
	}

	/* Report column count mismatch using the non-dropped-column counts */
	if (!same)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg_internal("%s", _(msg)),
				 errdetail("Number of returned columns (%d) does not match "
						   "expected column count (%d).",
						   nincols, noutcols)));

	/* Check if the map has a one-to-one match */
	if (check_attrmap_match(indesc, outdesc, attrMap))
	{
		/* Runtime conversion is not needed */
		free_attrmap(attrMap);
		return NULL;
	}

	return attrMap;
}

/* ---- VERBATIM backend/access/common/attmap.c: build_attrmap_by_name [static-prefixed] ---- */
/*
 * build_attrmap_by_name
 *
 * Return a palloc'd bare attribute map for tuple conversion, matching input
 * and output columns by name.  (Dropped columns are ignored in both input and
 * output.)  This is normally a subroutine for convert_tuples_by_name in
 * tupconvert.c, but can be used standalone.
 *
 * If 'missing_ok' is true, a column from 'outdesc' not being present in
 * 'indesc' is not flagged as an error; AttrMap.attnums[] entry for such an
 * outdesc column will be 0 in that case.
 */
static AttrMap *
build_attrmap_by_name(TupleDesc indesc,
					  TupleDesc outdesc,
					  bool missing_ok)
{
	AttrMap    *attrMap;
	int			outnatts;
	int			innatts;
	int			i;
	int			nextindesc = -1;

	outnatts = outdesc->natts;
	innatts = indesc->natts;

	attrMap = make_attrmap(outnatts);
	for (i = 0; i < outnatts; i++)
	{
		Form_pg_attribute outatt = TupleDescAttr(outdesc, i);
		char	   *attname;
		Oid			atttypid;
		int32		atttypmod;
		int			j;

		if (outatt->attisdropped)
			continue;			/* attrMap->attnums[i] is already 0 */
		attname = NameStr(outatt->attname);
		atttypid = outatt->atttypid;
		atttypmod = outatt->atttypmod;

		/*
		 * Now search for an attribute with the same name in the indesc. It
		 * seems likely that a partitioned table will have the attributes in
		 * the same order as the partition, so the search below is optimized
		 * for that case.  It is possible that columns are dropped in one of
		 * the relations, but not the other, so we use the 'nextindesc'
		 * counter to track the starting point of the search.  If the inner
		 * loop encounters dropped columns then it will have to skip over
		 * them, but it should leave 'nextindesc' at the correct position for
		 * the next outer loop.
		 */
		for (j = 0; j < innatts; j++)
		{
			Form_pg_attribute inatt;

			nextindesc++;
			if (nextindesc >= innatts)
				nextindesc = 0;

			inatt = TupleDescAttr(indesc, nextindesc);
			if (inatt->attisdropped)
				continue;
			if (strcmp(attname, NameStr(inatt->attname)) == 0)
			{
				/* Found it, check type */
				if (atttypid != inatt->atttypid || atttypmod != inatt->atttypmod)
					ereport(ERROR,
							(errcode(ERRCODE_DATATYPE_MISMATCH),
							 errmsg("could not convert row type"),
							 errdetail("Attribute \"%s\" of type %s does not match corresponding attribute of type %s.",
									   attname,
									   format_type_be(outdesc->tdtypeid),
									   format_type_be(indesc->tdtypeid))));
				attrMap->attnums[i] = inatt->attnum;
				break;
			}
		}
		if (attrMap->attnums[i] == 0 && !missing_ok)
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("could not convert row type"),
					 errdetail("Attribute \"%s\" of type %s does not exist in type %s.",
							   attname,
							   format_type_be(outdesc->tdtypeid),
							   format_type_be(indesc->tdtypeid))));
	}
	return attrMap;
}

/* ---- VERBATIM backend/access/common/attmap.c: build_attrmap_by_name_if_req [static-prefixed] ---- */
/*
 * build_attrmap_by_name_if_req
 *
 * Returns mapping created by build_attrmap_by_name, or NULL if no
 * conversion is required.  This is a convenience routine for
 * convert_tuples_by_name() in tupconvert.c and other functions, but it
 * can be used standalone.
 */
static AttrMap *
build_attrmap_by_name_if_req(TupleDesc indesc,
							 TupleDesc outdesc,
							 bool missing_ok)
{
	AttrMap    *attrMap;

	/* Verify compatibility and prepare attribute-number map */
	attrMap = build_attrmap_by_name(indesc, outdesc, missing_ok);

	/* Check if the map has a one-to-one match */
	if (check_attrmap_match(indesc, outdesc, attrMap))
	{
		/* Runtime conversion is not needed */
		free_attrmap(attrMap);
		return NULL;
	}

	return attrMap;
}

/* ---- VERBATIM backend/access/common/attmap.c: check_attrmap_match ---- */
/*
 * check_attrmap_match
 *
 * Check to see if the map is a one-to-one match, in which case we need
 * not to do a tuple conversion, and the attribute map is not necessary.
 */
static bool
check_attrmap_match(TupleDesc indesc,
					TupleDesc outdesc,
					AttrMap *attrMap)
{
	int			i;

	/* no match if attribute numbers are not the same */
	if (indesc->natts != outdesc->natts)
		return false;

	for (i = 0; i < attrMap->maplen; i++)
	{
		CompactAttribute *inatt = TupleDescCompactAttr(indesc, i);
		CompactAttribute *outatt;

		/*
		 * If the input column has a missing attribute, we need a conversion.
		 */
		if (inatt->atthasmissing)
			return false;

		if (attrMap->attnums[i] == (i + 1))
			continue;

		outatt = TupleDescCompactAttr(outdesc, i);

		/*
		 * If it's a dropped column and the corresponding input column is also
		 * dropped, we don't need a conversion.  However, attlen and
		 * attalignby must agree.
		 */
		if (attrMap->attnums[i] == 0 &&
			inatt->attisdropped &&
			inatt->attlen == outatt->attlen &&
			inatt->attalignby == outatt->attalignby)
			continue;

		return false;
	}

	return true;
}

/* ---- VERBATIM include/access/tupconvert.h:24-33 (TupleConversionMap) ---- */

typedef struct TupleConversionMap
{
	TupleDesc	indesc;			/* tupdesc for source rowtype */
	TupleDesc	outdesc;		/* tupdesc for result rowtype */
	AttrMap    *attrMap;		/* indexes of input fields, or 0 for null */
	Datum	   *invalues;		/* workspace for deconstructing source */
	bool	   *inisnull;
	Datum	   *outvalues;		/* workspace for constructing result */
	bool	   *outisnull;
} TupleConversionMap;

/* ---- VERBATIM backend/access/common/tupconvert.c: convert_tuples_by_position [static-prefixed] ---- */
/*
 * Set up for tuple conversion, matching input and output columns by
 * position.  (Dropped columns are ignored in both input and output.)
 */
static TupleConversionMap *
convert_tuples_by_position(TupleDesc indesc,
						   TupleDesc outdesc,
						   const char *msg)
{
	TupleConversionMap *map;
	int			n;
	AttrMap    *attrMap;

	/* Verify compatibility and prepare attribute-number map */
	attrMap = build_attrmap_by_position(indesc, outdesc, msg);

	if (attrMap == NULL)
	{
		/* runtime conversion is not needed */
		return NULL;
	}

	/* Prepare the map structure */
	map = (TupleConversionMap *) palloc(sizeof(TupleConversionMap));
	map->indesc = indesc;
	map->outdesc = outdesc;
	map->attrMap = attrMap;
	/* preallocate workspace for Datum arrays */
	n = outdesc->natts + 1;		/* +1 for NULL */
	map->outvalues = (Datum *) palloc(n * sizeof(Datum));
	map->outisnull = (bool *) palloc(n * sizeof(bool));
	n = indesc->natts + 1;		/* +1 for NULL */
	map->invalues = (Datum *) palloc(n * sizeof(Datum));
	map->inisnull = (bool *) palloc(n * sizeof(bool));
	map->invalues[0] = (Datum) 0;	/* set up the NULL entry */
	map->inisnull[0] = true;

	return map;
}

/* ---- VERBATIM backend/access/common/tupconvert.c: convert_tuples_by_name [static-prefixed] ---- */
/*
 * Set up for tuple conversion, matching input and output columns by name.
 * (Dropped columns are ignored in both input and output.)	This is intended
 * for use when the rowtypes are related by inheritance, so we expect an exact
 * match of both type and typmod.  The error messages will be a bit unhelpful
 * unless both rowtypes are named composite types.
 */
static TupleConversionMap *
convert_tuples_by_name(TupleDesc indesc,
					   TupleDesc outdesc)
{
	AttrMap    *attrMap;

	/* Verify compatibility and prepare attribute-number map */
	attrMap = build_attrmap_by_name_if_req(indesc, outdesc, false);

	if (attrMap == NULL)
	{
		/* runtime conversion is not needed */
		return NULL;
	}

	return convert_tuples_by_name_attrmap(indesc, outdesc, attrMap);
}

/* ---- VERBATIM backend/access/common/tupconvert.c: convert_tuples_by_name_attrmap [static-prefixed] ---- */
/*
 * Set up tuple conversion for input and output TupleDescs using the given
 * AttrMap.
 */
static TupleConversionMap *
convert_tuples_by_name_attrmap(TupleDesc indesc,
							   TupleDesc outdesc,
							   AttrMap *attrMap)
{
	int			n = outdesc->natts;
	TupleConversionMap *map;

	Assert(attrMap != NULL);

	/* Prepare the map structure */
	map = (TupleConversionMap *) palloc(sizeof(TupleConversionMap));
	map->indesc = indesc;
	map->outdesc = outdesc;
	map->attrMap = attrMap;
	/* preallocate workspace for Datum arrays */
	map->outvalues = (Datum *) palloc(n * sizeof(Datum));
	map->outisnull = (bool *) palloc(n * sizeof(bool));
	n = indesc->natts + 1;		/* +1 for NULL */
	map->invalues = (Datum *) palloc(n * sizeof(Datum));
	map->inisnull = (bool *) palloc(n * sizeof(bool));
	map->invalues[0] = (Datum) 0;	/* set up the NULL entry */
	map->inisnull[0] = true;

	return map;
}

/* ---- VERBATIM backend/access/common/tupconvert.c: execute_attr_map_tuple [static-prefixed] ---- */
/*
 * Perform conversion of a tuple according to the map.
 */
static HeapTuple
execute_attr_map_tuple(HeapTuple tuple, TupleConversionMap *map)
{
	AttrMap    *attrMap = map->attrMap;
	Datum	   *invalues = map->invalues;
	bool	   *inisnull = map->inisnull;
	Datum	   *outvalues = map->outvalues;
	bool	   *outisnull = map->outisnull;
	int			i;

	/*
	 * Extract all the values of the old tuple, offsetting the arrays so that
	 * invalues[0] is left NULL and invalues[1] is the first source attribute;
	 * this exactly matches the numbering convention in attrMap.
	 */
	heap_deform_tuple(tuple, map->indesc, invalues + 1, inisnull + 1);

	/*
	 * Transpose into proper fields of the new tuple.
	 */
	Assert(attrMap->maplen == map->outdesc->natts);
	for (i = 0; i < attrMap->maplen; i++)
	{
		int			j = attrMap->attnums[i];

		outvalues[i] = invalues[j];
		outisnull[i] = inisnull[j];
	}

	/*
	 * Now form the new tuple.
	 */
	return heap_form_tuple(map->outdesc, outvalues, outisnull);
}

/* ---- VERBATIM backend/access/common/tupconvert.c: free_conversion_map [static-prefixed] ---- */
/*
 * Free a TupleConversionMap structure.
 */
static void
free_conversion_map(TupleConversionMap *map)
{
	/* indesc and outdesc are not ours to free */
	free_attrmap(map->attrMap);
	pfree(map->invalues);
	pfree(map->inisnull);
	pfree(map->outvalues);
	pfree(map->outisnull);
	pfree(map);
}

/* ==================== SECTION D: pinned environment (SHIM) ================ */
/*
 * Type menu + wire-spec decoding. The wire formats here are the HARNESS
 * CONTRACT: fuzz/core/src/tupaccess_diff.rs encodes descriptors/values into
 * these byte layouts and transcribes the same decoders on the Rust side; any
 * asymmetry between the two transcriptions is a harness bug, never a
 * divergence. All multi-byte wire integers are little-endian.
 *
 * DESC SPEC:
 *   u8 natts; u8 dflags (bit0 has_constr, bit1 has_not_null,
 *   bit2 has_generated_stored, bit3 has_generated_virtual,
 *   bit4 tdtypeid = 424242 instead of RECORDOID); i32 tdtypmod;
 *   natts x { u8 menu; u8 aflags (bit0 dropped, bit1 notnull,
 *             bit2 hasmissing, bit3 nullability INVALID (else VALID),
 *             bit4 attislocal = false, bit5 attndims = 1,
 *             bit6 atttypmod = 77, bit7 attcollation = 999);
 *             u8 nameidx (low 5 bits index, bit5 uppercase 'C');
 *             u8 xflags (bit0 atthasdef, bit1 attidentity = 'a',
 *             bit2 attgenerated = 's', bit3 atttypid += 100000,
 *             bit4 attinhcount = 1, bit5 attcompression = 'l');
 *             [u16 mlen + datum image bytes when hasmissing] }
 *   when has_constr: u8 ndefval x { u8 adnum, u8 blen, bytes };
 *                    u8 ncheck  x { u8 cflags, u8 nlen, bytes, u8 blen, bytes }
 *   (driver normalizes: hasmissing implies has_constr; adnum strictly
 *   increasing; check entries sorted by name)
 *
 * VALUES: natts x { u8 isnull; [u16 len + datum image bytes] }
 *   byval: low min(len, attlen) LE bytes into a zeroed word, sign-extended
 *   from attlen width (CharGetDatum/Int16GetDatum/... semantics, mirrored on
 *   the Rust side); fixed byref: min(len, attlen) bytes zero-padded; varlena:
 *   the full image (1B/4B header or 18-byte TOAST pointer), passed as-is;
 *   cstring: payload bytes (no NUL), NUL appended.
 */

#define PG_TA_NMENU 12
typedef struct PgTaMenuEnt
{
	Oid			typid;
	int16		attlen;
	bool		attbyval;
	char		attalign;
	char		attstorage;
	Oid			attcollation;
}			PgTaMenuEnt;

static const PgTaMenuEnt pg_ta_menu[PG_TA_NMENU] = {
	{91101, 1, true, TYPALIGN_CHAR, TYPSTORAGE_PLAIN, 0},
	{91102, 2, true, TYPALIGN_SHORT, TYPSTORAGE_PLAIN, 0},
	{91103, 4, true, TYPALIGN_INT, TYPSTORAGE_PLAIN, 0},
	{91104, 8, true, TYPALIGN_DOUBLE, TYPSTORAGE_PLAIN, 0},
	{91105, 8, false, TYPALIGN_DOUBLE, TYPSTORAGE_PLAIN, 0},
	{91106, 12, false, TYPALIGN_INT, TYPSTORAGE_PLAIN, 0},
	{91107, 64, false, TYPALIGN_CHAR, TYPSTORAGE_PLAIN, 0},
	{91108, -1, false, TYPALIGN_INT, TYPSTORAGE_EXTENDED, 100},
	{91109, -1, false, TYPALIGN_INT, TYPSTORAGE_PLAIN, 0},
	{91110, -1, false, TYPALIGN_DOUBLE, TYPSTORAGE_EXTENDED, 100},
	{91111, -2, false, TYPALIGN_CHAR, TYPSTORAGE_PLAIN, 0},
	/* differs from entry 5 ONLY in attlen (single-field witness) */
	{91112, 16, false, TYPALIGN_INT, TYPSTORAGE_PLAIN, 0},
};

/* pinned pg_type DATA for TupleDescInitEntry's SearchSysCache1 */
static bool
pg_ta_type_shape(Oid oid, PgTaFormPgType *shape)
{
	int			i;

	for (i = 0; i < PG_TA_NMENU; i++)
	{
		if (pg_ta_menu[i].typid == oid)
		{
			shape->typlen = pg_ta_menu[i].attlen;
			shape->typbyval = pg_ta_menu[i].attbyval;
			shape->typalign = pg_ta_menu[i].attalign;
			shape->typstorage = pg_ta_menu[i].attstorage;
			shape->typcollation = pg_ta_menu[i].attcollation;
			return true;
		}
	}
	return false;
}

/* ---- wire cursor (driver always sends well-formed blobs; overrun = harness
 * bug = abort) ---- */
typedef struct PgTaCur
{
	const unsigned char *p;
	int			len;
	int			off;
}			PgTaCur;

static uint32
pg_ta_u8(PgTaCur *c)
{
	assert(c->off + 1 <= c->len);
	return c->p[c->off++];
}

static uint32
pg_ta_u16(PgTaCur *c)
{
	uint32		v;

	assert(c->off + 2 <= c->len);
	v = (uint32) c->p[c->off] | ((uint32) c->p[c->off + 1] << 8);
	c->off += 2;
	return v;
}

static int32
pg_ta_i32(PgTaCur *c)
{
	uint32		v;

	assert(c->off + 4 <= c->len);
	v = (uint32) c->p[c->off] | ((uint32) c->p[c->off + 1] << 8) |
		((uint32) c->p[c->off + 2] << 16) | ((uint32) c->p[c->off + 3] << 24);
	c->off += 4;
	return (int32) v;
}

static const unsigned char *
pg_ta_bytes(PgTaCur *c, int n)
{
	const unsigned char *r;

	assert(c->off + n <= c->len);
	r = c->p + c->off;
	c->off += n;
	return r;
}

/* datum staging (see contract above) */
static Datum
pg_ta_stage(int16 attlen, bool attbyval, const unsigned char *b, int len)
{
	if (attbyval)
	{
		uint64		w = 0;
		int			n = len < attlen ? len : attlen;

		memcpy(&w, b, n);		/* LE host */
		switch (attlen)
		{
			case 1:
				return CharGetDatum((char) (uint8) w);
			case 2:
				return Int16GetDatum((int16) (uint16) w);
			case 4:
				return Int32GetDatum((int32) (uint32) w);
			default:
				return Int64GetDatum((int64) w);
		}
	}
	if (attlen > 0)
	{
		char	   *p = palloc0(attlen);
		int			n = len < attlen ? len : attlen;

		memcpy(p, b, n);
		return PointerGetDatum(p);
	}
	if (attlen == -1)
	{
		char	   *p = palloc(len > 0 ? len : 1);

		memcpy(p, b, len);
		return PointerGetDatum(p);
	}
	/* cstring */
	{
		char	   *p = palloc(len + 1);

		memcpy(p, b, len);
		p[len] = '\0';
		return PointerGetDatum(p);
	}
}

/* build a descriptor from the wire spec (environment staging: direct
 * FormData writes + verbatim populate_compact_attribute, the same way
 * relcache builds descriptors from pg_attribute rows) */
static TupleDesc
pg_ta_build_desc(PgTaCur *c)
{
	int			natts = (int) pg_ta_u8(c);
	uint32		dflags = pg_ta_u8(c);
	int32		tdtypmod = pg_ta_i32(c);
	TupleDesc	desc;
	bool		any_missing = false;
	AttrMissing *missing = NULL;
	int			i;

	assert(natts <= 40);
	desc = CreateTemplateTupleDesc(natts);
	desc->tdtypeid = (dflags & 0x10) ? (Oid) 424242 : (Oid) RECORDOID;
	desc->tdtypmod = tdtypmod;

	missing = palloc0((natts > 0 ? natts : 1) * sizeof(AttrMissing));

	for (i = 0; i < natts; i++)
	{
		uint32		menu = pg_ta_u8(c) % PG_TA_NMENU;
		uint32		aflags = pg_ta_u8(c);
		uint32		nameidx = pg_ta_u8(c);
		uint32		xflags = pg_ta_u8(c);
		const PgTaMenuEnt *m = &pg_ta_menu[menu];
		FormData_pg_attribute *att = TupleDescAttr(desc, i);
		char		nm[8];

		memset(att, 0, sizeof(*att));
		snprintf(nm, sizeof(nm), "%c%d",
				 (nameidx & 0x20) ? 'C' : 'c', (int) (nameidx & 0x1f));
		namestrcpy(&att->attname, nm);
		att->attrelid = 0;
		att->attnum = (AttrNumber) (i + 1);
		att->atttypid = (aflags & 0x01) ? InvalidOid
			: m->typid + ((xflags & 0x08) ? 100000 : 0);
		att->attlen = m->attlen;
		att->attbyval = m->attbyval;
		att->attalign = m->attalign;
		att->attstorage = m->attstorage;
		att->attcompression = (xflags & 0x20) ? 'l' : InvalidCompressionMethod;
		att->attcollation = (aflags & 0x80) ? (Oid) 999 : m->attcollation;
		att->atttypmod = (aflags & 0x40) ? 77 : -1;
		att->attndims = (aflags & 0x20) ? 1 : 0;
		att->attisdropped = (aflags & 0x01) != 0;
		att->attnotnull = (aflags & 0x02) != 0;
		att->atthasmissing = false;
		att->attislocal = (aflags & 0x10) == 0;
		att->attinhcount = (xflags & 0x10) ? 1 : 0;
		att->atthasdef = (xflags & 0x01) != 0;
		att->attidentity = (xflags & 0x02) ? 'a' : '\0';
		att->attgenerated = (xflags & 0x04) ? 's' : '\0';

		if ((aflags & 0x04) && (dflags & 0x01) && !att->attisdropped)
		{
			int			mlen = (int) pg_ta_u16(c);
			const unsigned char *mb = pg_ta_bytes(c, mlen);

			att->atthasmissing = true;
			any_missing = true;
			missing[i].am_present = true;
			missing[i].am_value = pg_ta_stage(m->attlen, m->attbyval, mb, mlen);
		}

		populate_compact_attribute(desc, i);
		/* stage resolved nullability (relcache-style; mirrored in Rust) */
		if (att->attnotnull)
			desc->compact_attrs[i].attnullability =
				(aflags & 0x08) ? ATTNULLABLE_INVALID : ATTNULLABLE_VALID;
	}

	if (dflags & 0x01)
	{
		TupleConstr *constr = palloc0(sizeof(TupleConstr));
		int			ndefval = (int) pg_ta_u8(c);
		int			ncheck;
		int			j;

		constr->has_not_null = (dflags & 0x02) != 0;
		constr->has_generated_stored = (dflags & 0x04) != 0;
		constr->has_generated_virtual = (dflags & 0x08) != 0;
		if (ndefval > 0)
		{
			constr->num_defval = ndefval;
			constr->defval = palloc0(ndefval * sizeof(AttrDefault));
			for (j = 0; j < ndefval; j++)
			{
				int			adnum = (int) pg_ta_u8(c);
				int			blen = (int) pg_ta_u8(c);
				const unsigned char *b = pg_ta_bytes(c, blen);
				char	   *s = palloc(blen + 1);

				memcpy(s, b, blen);
				s[blen] = '\0';
				constr->defval[j].adnum = (AttrNumber) adnum;
				constr->defval[j].adbin = s;
			}
		}
		ncheck = (int) pg_ta_u8(c);
		if (ncheck > 0)
		{
			constr->num_check = ncheck;
			constr->check = palloc0(ncheck * sizeof(ConstrCheck));
			for (j = 0; j < ncheck; j++)
			{
				uint32		cflags = pg_ta_u8(c);
				int			nlen = (int) pg_ta_u8(c);
				const unsigned char *nb = pg_ta_bytes(c, nlen);
				int			blen;
				const unsigned char *bb;
				char	   *s;

				s = palloc(nlen + 1);
				memcpy(s, nb, nlen);
				s[nlen] = '\0';
				constr->check[j].ccname = s;
				blen = (int) pg_ta_u8(c);
				bb = pg_ta_bytes(c, blen);
				s = palloc(blen + 1);
				memcpy(s, bb, blen);
				s[blen] = '\0';
				constr->check[j].ccbin = s;
				constr->check[j].ccenforced = (cflags & 0x01) != 0;
				constr->check[j].ccvalid = (cflags & 0x02) != 0;
				constr->check[j].ccnoinherit = (cflags & 0x04) != 0;
			}
		}
		if (any_missing)
			constr->missing = missing;
		desc->constr = constr;
	}

	return desc;
}

/* decode a VALUES blob against a descriptor prefix of nvals attributes */
static void
pg_ta_read_values(PgTaCur *c, TupleDesc desc, int nvals,
				  Datum *values, bool *isnull)
{
	int			i;

	for (i = 0; i < nvals; i++)
	{
		uint32		nul = pg_ta_u8(c);

		if (nul & 1)
		{
			values[i] = (Datum) 0;
			isnull[i] = true;
		}
		else
		{
			int			vlen = (int) pg_ta_u16(c);
			const unsigned char *b = pg_ta_bytes(c, vlen);
			FormData_pg_attribute *att = TupleDescAttr(desc, i);

			values[i] = pg_ta_stage(att->attlen, att->attbyval, b, vlen);
			isnull[i] = false;
		}
	}
}

/* ---- result writer ---- */
typedef struct PgTaW
{
	unsigned char *out;
	int			cap;
	int			off;
}			PgTaW;

static void
pg_ta_put_bytes(PgTaW *w, const void *src, int n)
{
	assert(w->off + n <= w->cap);
	memcpy(w->out + w->off, src, n);
	w->off += n;
}

static void
pg_ta_put_u8(PgTaW *w, uint32 v)
{
	unsigned char b = (unsigned char) v;

	pg_ta_put_bytes(w, &b, 1);
}

static void
pg_ta_put_u16(PgTaW *w, uint32 v)
{
	unsigned char b[2] = {(unsigned char) v, (unsigned char) (v >> 8)};

	pg_ta_put_bytes(w, b, 2);
}

static void
pg_ta_put_u32(PgTaW *w, uint32 v)
{
	unsigned char b[4] = {(unsigned char) v, (unsigned char) (v >> 8),
	(unsigned char) (v >> 16), (unsigned char) (v >> 24)};

	pg_ta_put_bytes(w, b, 4);
}

static void
pg_ta_put_u64(PgTaW *w, uint64 v)
{
	pg_ta_put_u32(w, (uint32) v);
	pg_ta_put_u32(w, (uint32) (v >> 32));
}

/* serialize one fetched attribute value: [isnull u8] then for byval a
 * bit-exact u64 Datum word, for byref the pointed-to bytes per attlen
 * semantics */
/* RATIFIED 2026-08-01 (Michael): platform non-surface — width-1 byval Datum upper 56
 * bits. fetch_att for attlen==1 is `*((char *) T)` (tupmacs.h) and C char
 * signedness is platform-defined (signed on macOS-aarch64/x86_64-Linux,
 * unsigned on Linux-aarch64), so this TU itself produces different upper
 * Datum bits per platform; consumers truncate via DatumGetChar. Width-1
 * words are therefore serialized masked to the low 8 bits on BOTH sides.
 * Widths 2/4/8 use int16/int32/int64 (signed everywhere): NOT masked. */
static void
pg_ta_put_datum(PgTaW *w, Datum d, bool isnull, int16 attlen, bool attbyval)
{
	pg_ta_put_u8(w, isnull ? 1 : 0);
	if (isnull)
		return;
	if (attbyval)
	{
		pg_ta_put_u8(w, 0);
		pg_ta_put_u64(w, attlen == 1 ? ((uint64) d & 0xff) : (uint64) d);
	}
	else
	{
		const unsigned char *p = (const unsigned char *) DatumGetPointer(d);
		Size		n;

		if (attlen > 0)
			n = (Size) attlen;
		else if (attlen == -1)
			n = VARSIZE_ANY(p);
		else
			n = strlen((const char *) p) + 1;
		pg_ta_put_u8(w, 1);
		pg_ta_put_u32(w, (uint32) n);
		pg_ta_put_bytes(w, p, (int) n);
	}
}

static void
pg_ta_put_image(PgTaW *w, const void *img, uint32 len)
{
	pg_ta_put_u32(w, len);
	pg_ta_put_bytes(w, img, (int) len);
}

/* full descriptor field-plane serializer (attcacheoff excluded: stateful) */
static void
pg_ta_put_desc_plane(PgTaW *w, TupleDesc d)
{
	int			i;

	pg_ta_put_u32(w, (uint32) d->natts);
	pg_ta_put_u32(w, d->tdtypeid);
	pg_ta_put_u32(w, (uint32) d->tdtypmod);
	for (i = 0; i < d->natts; i++)
	{
		FormData_pg_attribute *a = TupleDescAttr(d, i);

		pg_ta_put_u32(w, a->attrelid);
		pg_ta_put_bytes(w, NameStr(a->attname), NAMEDATALEN);
		pg_ta_put_u32(w, a->atttypid);
		pg_ta_put_u16(w, (uint16) a->attlen);
		pg_ta_put_u16(w, (uint16) a->attnum);
		pg_ta_put_u32(w, (uint32) a->atttypmod);
		pg_ta_put_u16(w, (uint16) a->attndims);
		pg_ta_put_u8(w, a->attbyval ? 1 : 0);
		pg_ta_put_u8(w, (uint8) a->attalign);
		pg_ta_put_u8(w, (uint8) a->attstorage);
		pg_ta_put_u8(w, (uint8) a->attcompression);
		pg_ta_put_u8(w, a->attnotnull ? 1 : 0);
		pg_ta_put_u8(w, a->atthasdef ? 1 : 0);
		pg_ta_put_u8(w, a->atthasmissing ? 1 : 0);
		pg_ta_put_u8(w, (uint8) a->attidentity);
		pg_ta_put_u8(w, (uint8) a->attgenerated);
		pg_ta_put_u8(w, a->attisdropped ? 1 : 0);
		pg_ta_put_u8(w, a->attislocal ? 1 : 0);
		pg_ta_put_u16(w, (uint16) a->attinhcount);
		pg_ta_put_u32(w, a->attcollation);
	}
	for (i = 0; i < d->natts; i++)
	{
		CompactAttribute *ca = &d->compact_attrs[i];

		pg_ta_put_u16(w, (uint16) ca->attlen);
		pg_ta_put_u8(w, ca->attbyval ? 1 : 0);
		pg_ta_put_u8(w, ca->attispackable ? 1 : 0);
		pg_ta_put_u8(w, ca->atthasmissing ? 1 : 0);
		pg_ta_put_u8(w, ca->attisdropped ? 1 : 0);
		pg_ta_put_u8(w, ca->attgenerated ? 1 : 0);
		pg_ta_put_u8(w, (uint8) ca->attnullability);
		pg_ta_put_u8(w, ca->attalignby);
	}
	if (d->constr == NULL)
		pg_ta_put_u8(w, 0);
	else
	{
		TupleConstr *cs = d->constr;

		pg_ta_put_u8(w, 1);
		pg_ta_put_u8(w, cs->has_not_null ? 1 : 0);
		pg_ta_put_u8(w, cs->has_generated_stored ? 1 : 0);
		pg_ta_put_u8(w, cs->has_generated_virtual ? 1 : 0);
		pg_ta_put_u16(w, cs->num_defval);
		for (i = 0; i < cs->num_defval; i++)
		{
			Size		n = strlen(cs->defval[i].adbin);

			pg_ta_put_u16(w, (uint16) cs->defval[i].adnum);
			pg_ta_put_u16(w, (uint16) n);
			pg_ta_put_bytes(w, cs->defval[i].adbin, (int) n);
		}
		pg_ta_put_u16(w, cs->num_check);
		for (i = 0; i < cs->num_check; i++)
		{
			Size		n = strlen(cs->check[i].ccname);
			Size		b = strlen(cs->check[i].ccbin);

			pg_ta_put_u16(w, (uint16) n);
			pg_ta_put_bytes(w, cs->check[i].ccname, (int) n);
			pg_ta_put_u16(w, (uint16) b);
			pg_ta_put_bytes(w, cs->check[i].ccbin, (int) b);
			pg_ta_put_u8(w, cs->check[i].ccenforced ? 1 : 0);
			pg_ta_put_u8(w, cs->check[i].ccvalid ? 1 : 0);
			pg_ta_put_u8(w, cs->check[i].ccnoinherit ? 1 : 0);
		}
		if (cs->missing == NULL)
			pg_ta_put_u8(w, 0);
		else
		{
			pg_ta_put_u8(w, 1);
			for (i = 0; i < d->natts; i++)
			{
				pg_ta_put_u8(w, cs->missing[i].am_present ? 1 : 0);
				if (cs->missing[i].am_present)
				{
					CompactAttribute *ca = &d->compact_attrs[i];

					if (ca->attbyval)
					{
						uint64		word = (uint64) cs->missing[i].am_value;

						pg_ta_put_u16(w, (uint16) ca->attlen);
						pg_ta_put_bytes(w, &word, ca->attlen);	/* LE low bytes */
					}
					else
					{
						const unsigned char *p =
						(const unsigned char *) DatumGetPointer(cs->missing[i].am_value);
						Size		n;

						if (ca->attlen > 0)
							n = (Size) ca->attlen;
						else if (ca->attlen == -1)
							n = VARSIZE_ANY(p);
						else
							n = strlen((const char *) p) + 1;
						pg_ta_put_u16(w, (uint16) n);
						pg_ta_put_bytes(w, p, (int) n);
					}
				}
			}
		}
	}
}

/* ========== SECTION E: fuzz-facing driver entries (NOT Postgres code) ===== */
/*
 * Shape: arena reset, errcode = 0, arm the longjmp, run the vendored
 * functions, serialize results. Return: 0 = ok, 1 = vendored ereport error
 * (class in pg_diff_errcode), -3 = internal harness assertion.
 */

#define PG_TA_ENTRY() \
	do { \
		pg_diff_arena_reset(); \
		pg_diff_errcode = 0; \
		pg_ta_in_top_context = false; \
		if (setjmp(pg_diff_rowtypes_jmp)) \
			return 1; \
	} while (0)

#define PG_TA_MAXATTS 48

int
pg_ta_form(const unsigned char *spec, int speclen,
		   const unsigned char *vals, int valslen,
		   unsigned char *out, int outcap, int *outlen)
{
	PgTaCur		cs = {spec, speclen, 0};
	PgTaCur		cv = {vals, valslen, 0};
	PgTaW		w = {out, outcap, 0};
	TupleDesc	desc;
	Datum		values[PG_TA_MAXATTS];
	bool		isnull[PG_TA_MAXATTS];
	Datum		dv[PG_TA_MAXATTS];
	bool		dn[PG_TA_MAXATTS];
	HeapTuple	ht;
	int			i;

	PG_TA_ENTRY();
	desc = pg_ta_build_desc(&cs);
	pg_ta_read_values(&cv, desc, desc->natts, values, isnull);
	ht = heap_form_tuple(desc, values, isnull);
	pg_ta_put_image(&w, ht->t_data, ht->t_len);
	heap_deform_tuple(ht, desc, dv, dn);
	for (i = 0; i < desc->natts; i++)
	{
		CompactAttribute *ca = &desc->compact_attrs[i];

		pg_ta_put_datum(&w, dv[i], dn[i], ca->attlen, ca->attbyval);
	}
	*outlen = w.off;
	return 0;
}

/* MaxTupleAttributeNumber error arm spot-check: natts = 1665 > 1664 */
int
pg_ta_form_toomany(void)
{
	TupleDesc	desc;
	Datum	   *values;
	bool	   *isnull;
	int			i;
	const int	natts = MaxTupleAttributeNumber + 1;

	PG_TA_ENTRY();
	desc = CreateTemplateTupleDesc(natts);
	for (i = 0; i < natts; i++)
	{
		FormData_pg_attribute *att = TupleDescAttr(desc, i);

		memset(att, 0, sizeof(*att));
		namestrcpy(&att->attname, "c");
		att->attnum = (AttrNumber) (i + 1);
		att->atttypid = pg_ta_menu[2].typid;
		att->attlen = 4;
		att->attbyval = true;
		att->attalign = TYPALIGN_INT;
		att->attstorage = TYPSTORAGE_PLAIN;
		att->attislocal = true;
		populate_compact_attribute(desc, i);
	}
	values = palloc0(natts * sizeof(Datum));
	isnull = palloc(natts * sizeof(bool));
	memset(isnull, 1, natts);
	(void) heap_form_tuple(desc, values, isnull);
	return -3;					/* must have thrown TOO_MANY_COLUMNS */
}

int
pg_ta_minimal(const unsigned char *spec, int speclen,
			  const unsigned char *vals, int valslen,
			  unsigned char *out, int outcap, int *outlen)
{
	PgTaCur		cs = {spec, speclen, 0};
	PgTaCur		cv = {vals, valslen, 0};
	PgTaW		w = {out, outcap, 0};
	TupleDesc	desc;
	Datum		values[PG_TA_MAXATTS];
	bool		isnull[PG_TA_MAXATTS];
	MinimalTuple mt,
				mt2,
				mt3;
	HeapTuple	ht,
				ht2;

	PG_TA_ENTRY();
	desc = pg_ta_build_desc(&cs);
	pg_ta_read_values(&cv, desc, desc->natts, values, isnull);
	mt = heap_form_minimal_tuple(desc, values, isnull, 0);
	pg_ta_put_image(&w, mt, mt->t_len);
	mt2 = heap_copy_minimal_tuple(mt, 0);
	pg_ta_put_image(&w, mt2, mt2->t_len);
	ht = heap_tuple_from_minimal_tuple(mt);
	pg_ta_put_image(&w, ht->t_data, ht->t_len);
	ht2 = heap_form_tuple(desc, values, isnull);
	mt3 = minimal_tuple_from_heap_tuple(ht2, 0);
	pg_ta_put_image(&w, mt3, mt3->t_len);
	*outlen = w.off;
	return 0;
}

/*
 * getattr plane: form under a (possibly truncated) source descriptor, read
 * under the full descriptor. Runs heap_getattr TWICE (cold attcacheoff, then
 * warmed) and heap_attisnull; the two getattr serializations must agree
 * (-3 on drift). Output: [image][getattr datum][attisnull u8].
 */
int
pg_ta_getattr(const unsigned char *spec, int speclen, int src_natts, int attnum,
			  const unsigned char *vals, int valslen,
			  unsigned char *out, int outcap, int *outlen)
{
	PgTaCur		cs = {spec, speclen, 0};
	PgTaCur		cv = {vals, valslen, 0};
	PgTaW		w = {out, outcap, 0};
	TupleDesc	desc,
				src;
	Datum		values[PG_TA_MAXATTS];
	bool		isnull[PG_TA_MAXATTS];
	HeapTuple	ht;
	Datum		d1,
				d2;
	bool		n1,
				n2;
	CompactAttribute *ca;
	int			probe1,
				probe2;

	PG_TA_ENTRY();
	desc = pg_ta_build_desc(&cs);
	assert(attnum >= 1 && attnum <= desc->natts);
	assert(src_natts >= 0 && src_natts <= desc->natts);
	src = src_natts < desc->natts
		? CreateTupleDescTruncatedCopy(desc, src_natts) : desc;
	pg_ta_read_values(&cv, src, src_natts, values, isnull);
	ht = heap_form_tuple(src, values, isnull);
	pg_ta_put_image(&w, ht->t_data, ht->t_len);

	ca = &desc->compact_attrs[attnum - 1];
	d1 = heap_getattr(ht, attnum, desc, &n1);
	probe1 = w.off;
	pg_ta_put_datum(&w, d1, n1, ca->attlen, ca->attbyval);
	d2 = heap_getattr(ht, attnum, desc, &n2);	/* warmed attcacheoff */
	probe2 = w.off;
	pg_ta_put_datum(&w, d2, n2, ca->attlen, ca->attbyval);
	if (probe2 - probe1 != w.off - probe2 ||
		memcmp(w.out + probe1, w.out + probe2, probe2 - probe1) != 0)
		return -3;				/* cold/warm getattr drift inside the oracle */
	w.off = probe2;				/* keep one copy */
	pg_ta_put_u8(&w, heap_attisnull(ht, attnum, desc) ? 1 : 0);
	*outlen = w.off;
	return 0;
}

/*
 * modify plane. replspec: when by_cols == 0, natts x { u8 do_replace;
 * [u8 isnull; [u16 len + bytes]] }; when by_cols == 1: u8 ncols x
 * { u8 attnum; u8 isnull; [u16 len + bytes] }.
 */
int
pg_ta_modify(const unsigned char *spec, int speclen,
			 const unsigned char *vals, int valslen,
			 const unsigned char *repl, int repllen, int by_cols,
			 unsigned char *out, int outcap, int *outlen)
{
	PgTaCur		cs = {spec, speclen, 0};
	PgTaCur		cv = {vals, valslen, 0};
	PgTaCur		cr = {repl, repllen, 0};
	PgTaW		w = {out, outcap, 0};
	TupleDesc	desc;
	Datum		values[PG_TA_MAXATTS];
	bool		isnull[PG_TA_MAXATTS];
	HeapTuple	ht,
				nt;
	int			i;

	PG_TA_ENTRY();
	desc = pg_ta_build_desc(&cs);
	pg_ta_read_values(&cv, desc, desc->natts, values, isnull);
	ht = heap_form_tuple(desc, values, isnull);

	if (!by_cols)
	{
		Datum		rv[PG_TA_MAXATTS];
		bool		rn[PG_TA_MAXATTS];
		bool		dorepl[PG_TA_MAXATTS];

		for (i = 0; i < desc->natts; i++)
		{
			dorepl[i] = (pg_ta_u8(&cr) & 1) != 0;
			rv[i] = (Datum) 0;
			rn[i] = true;
			if (dorepl[i])
			{
				uint32		nul = pg_ta_u8(&cr);

				if (!(nul & 1))
				{
					int			vlen = (int) pg_ta_u16(&cr);
					const unsigned char *b = pg_ta_bytes(&cr, vlen);
					FormData_pg_attribute *att = TupleDescAttr(desc, i);

					rv[i] = pg_ta_stage(att->attlen, att->attbyval, b, vlen);
					rn[i] = false;
				}
			}
		}
		nt = heap_modify_tuple(ht, desc, rv, rn, dorepl);
	}
	else
	{
		int			ncols = (int) pg_ta_u8(&cr);
		int			cols[PG_TA_MAXATTS];
		Datum		rv[PG_TA_MAXATTS];
		bool		rn[PG_TA_MAXATTS];

		assert(ncols <= desc->natts);
		for (i = 0; i < ncols; i++)
		{
			uint32		nul;

			cols[i] = (int) pg_ta_u8(&cr);
			assert(cols[i] >= 1 && cols[i] <= desc->natts);
			nul = pg_ta_u8(&cr);
			rv[i] = (Datum) 0;
			rn[i] = true;
			if (!(nul & 1))
			{
				int			vlen = (int) pg_ta_u16(&cr);
				const unsigned char *b = pg_ta_bytes(&cr, vlen);
				FormData_pg_attribute *att = TupleDescAttr(desc, cols[i] - 1);

				rv[i] = pg_ta_stage(att->attlen, att->attbyval, b, vlen);
				rn[i] = false;
			}
		}
		nt = heap_modify_tuple_by_cols(ht, desc, ncols, cols, rv, rn);
	}
	pg_ta_put_image(&w, nt->t_data, nt->t_len);
	*outlen = w.off;
	return 0;
}

int
pg_ta_copy(const unsigned char *spec, int speclen,
		   const unsigned char *vals, int valslen,
		   unsigned char *out, int outcap, int *outlen)
{
	PgTaCur		cs = {spec, speclen, 0};
	PgTaCur		cv = {vals, valslen, 0};
	PgTaW		w = {out, outcap, 0};
	TupleDesc	desc;
	Datum		values[PG_TA_MAXATTS];
	bool		isnull[PG_TA_MAXATTS];
	HeapTuple	ht,
				c1;
	struct HeapTupleData c2;

	PG_TA_ENTRY();
	desc = pg_ta_build_desc(&cs);
	pg_ta_read_values(&cv, desc, desc->natts, values, isnull);
	ht = heap_form_tuple(desc, values, isnull);
	c1 = heap_copytuple(ht);
	pg_ta_put_image(&w, c1->t_data, c1->t_len);
	heap_copytuple_with_tuple(ht, &c2);
	pg_ta_put_image(&w, c2.t_data, c2.t_len);
	if (HeapTupleHasExternal(ht))
		pg_ta_put_u8(&w, 1);	/* copy_as_datum arm carved (toast_flatten) */
	else
	{
		Datum		d = heap_copy_tuple_as_datum(ht, desc);
		HeapTupleHeaderData *hdr = (HeapTupleHeaderData *) DatumGetPointer(d);

		pg_ta_put_u8(&w, 0);
		pg_ta_put_image(&w, hdr, (uint32) HeapTupleHeaderGetDatumLength(hdr));
	}
	*outlen = w.off;
	return 0;
}

int
pg_ta_expand(const unsigned char *spec, int speclen, int src_natts,
			 const unsigned char *vals, int valslen,
			 unsigned char *out, int outcap, int *outlen)
{
	PgTaCur		cs = {spec, speclen, 0};
	PgTaCur		cv = {vals, valslen, 0};
	PgTaW		w = {out, outcap, 0};
	TupleDesc	desc,
				src;
	Datum		values[PG_TA_MAXATTS];
	bool		isnull[PG_TA_MAXATTS];
	HeapTuple	ht,
				he;
	MinimalTuple me;

	PG_TA_ENTRY();
	desc = pg_ta_build_desc(&cs);
	assert(src_natts < desc->natts);
	src = CreateTupleDescTruncatedCopy(desc, src_natts);
	pg_ta_read_values(&cv, src, src_natts, values, isnull);
	ht = heap_form_tuple(src, values, isnull);
	pg_ta_put_image(&w, ht->t_data, ht->t_len);
	he = heap_expand_tuple(ht, desc);
	pg_ta_put_image(&w, he->t_data, he->t_len);
	me = minimal_expand_tuple(ht, desc);
	pg_ta_put_image(&w, me, me->t_len);
	*outlen = w.off;
	return 0;
}

int
pg_ta_desc_cmp(const unsigned char *spec1, int len1,
			   const unsigned char *spec2, int len2,
			   unsigned char *out, int outcap, int *outlen)
{
	PgTaCur		c1 = {spec1, len1, 0};
	PgTaCur		c2 = {spec2, len2, 0};
	PgTaW		w = {out, outcap, 0};
	TupleDesc	d1,
				d2;

	PG_TA_ENTRY();
	d1 = pg_ta_build_desc(&c1);
	d2 = pg_ta_build_desc(&c2);
	pg_ta_put_u8(&w, equalTupleDescs(d1, d2) ? 1 : 0);
	pg_ta_put_u8(&w, equalRowTypes(d1, d2) ? 1 : 0);
	pg_ta_put_u32(&w, hashRowType(d1));
	pg_ta_put_u32(&w, hashRowType(d2));
	*outlen = w.off;
	return 0;
}

/*
 * which: 0 CreateTupleDescCopy, 1 CreateTupleDescTruncatedCopy(arg1),
 * 2 CreateTupleDescCopyConstr (then FreeTupleDesc exercises the free path),
 * 3 TupleDescCopy into a template, 4 CreateTupleDescCopy +
 * TupleDescCopyEntry(dst arg1 <- src arg2).
 * Output: field plane of the result + equalTupleDescs/equalRowTypes(src, copy).
 */
int
pg_ta_desc_copy(const unsigned char *spec, int speclen, int which,
				int arg1, int arg2,
				unsigned char *out, int outcap, int *outlen)
{
	PgTaCur		cs = {spec, speclen, 0};
	PgTaW		w = {out, outcap, 0};
	TupleDesc	d,
				r;

	PG_TA_ENTRY();
	d = pg_ta_build_desc(&cs);
	switch (which)
	{
		case 0:
			r = CreateTupleDescCopy(d);
			break;
		case 1:
			assert(arg1 >= 0 && arg1 <= d->natts);
			r = CreateTupleDescTruncatedCopy(d, arg1);
			break;
		case 2:
			r = CreateTupleDescCopyConstr(d);
			break;
		case 3:
			r = CreateTemplateTupleDesc(d->natts);
			TupleDescCopy(r, d);
			break;
		default:
			assert(arg1 >= 1 && arg1 <= d->natts);
			assert(arg2 >= 1 && arg2 <= d->natts);
			r = CreateTupleDescCopy(d);
			TupleDescCopyEntry(r, (AttrNumber) arg1, d, (AttrNumber) arg2);
			break;
	}
	pg_ta_put_desc_plane(&w, r);
	pg_ta_put_u8(&w, equalTupleDescs(d, r) ? 1 : 0);
	pg_ta_put_u8(&w, equalRowTypes(d, r) ? 1 : 0);
	if (which == 2)
		FreeTupleDesc(r);		/* exercise the deep-free path */
	*outlen = w.off;
	return 0;
}

/*
 * init plane. entryspec: u8 mode (0 = entry loop, 1 = BuildDescFromLists);
 * u8 n; n x { u8 kind, u8 code, u8 nameidx, u8 tm, u8 dim }.
 * kind: 0 InitEntry(menu type), 1 InitEntry(unknown oid 4242: error arm),
 * 2 InitBuiltinEntry(builtin), 3 InitBuiltinEntry(unsupported: error arm),
 * 4 InitEntry(menu) + InitEntryCollation(999).
 * nameidx 0xFF = NULL attributeName (InitEntry only).
 * BuildDescFromLists mode uses kind/code as InitEntry menu picks.
 */
static const Oid pg_ta_builtin_menu[6] = {TEXTOID, BOOLOID, INT4OID, INT8OID,
OIDOID, TEXTARRAYOID};

int
pg_ta_desc_init(const unsigned char *es, int eslen,
				unsigned char *out, int outcap, int *outlen)
{
	PgTaCur		c = {es, eslen, 0};
	PgTaW		w = {out, outcap, 0};
	TupleDesc	desc;
	int			mode,
				n,
				i;

	PG_TA_ENTRY();
	mode = (int) pg_ta_u8(&c);
	n = (int) pg_ta_u8(&c);
	assert(n >= 0 && n <= 8);

	if (mode == 1)
	{
		static _Thread_local List names,
					types,
					typmods,
					colls;
		static _Thread_local String strs[8];
		static _Thread_local char nmbuf[8][8];

		names.length = types.length = typmods.length = colls.length = n;
		for (i = 0; i < n; i++)
		{
			uint32		kind = pg_ta_u8(&c);
			uint32		code = pg_ta_u8(&c);
			uint32		nameidx = pg_ta_u8(&c);
			int32		tm = (int32) pg_ta_u8(&c) - 1;
			uint32		dim = pg_ta_u8(&c);

			(void) kind;
			(void) dim;
			snprintf(nmbuf[i], 8, "%c%d", (nameidx & 0x20) ? 'C' : 'c',
					 (int) (nameidx & 0x1f));
			strs[i].sval = nmbuf[i];
			names.elements[i].ptr_value = &strs[i];
			types.elements[i].oid_value = pg_ta_menu[code % PG_TA_NMENU].typid;
			typmods.elements[i].int_value = tm;
			colls.elements[i].oid_value = (code & 0x40) ? 999 : InvalidOid;
		}
		desc = BuildDescFromLists(&names, &types, &typmods, &colls);
	}
	else
	{
		desc = CreateTemplateTupleDesc(n);
		for (i = 0; i < n; i++)
		{
			uint32		kind = pg_ta_u8(&c) % 5;
			uint32		code = pg_ta_u8(&c);
			uint32		nameidx = pg_ta_u8(&c);
			int32		tm = (int32) pg_ta_u8(&c) - 1;
			int			dim = (int) (pg_ta_u8(&c) & 1);
			char		nm[8];
			const char *name;

			snprintf(nm, sizeof(nm), "%c%d", (nameidx & 0x20) ? 'C' : 'c',
					 (int) (nameidx & 0x1f));
			name = (nameidx == 0xFF) ? NULL : nm;
			switch (kind)
			{
				case 0:
					TupleDescInitEntry(desc, (AttrNumber) (i + 1), name,
									   pg_ta_menu[code % PG_TA_NMENU].typid,
									   tm, dim);
					break;
				case 1:
					TupleDescInitEntry(desc, (AttrNumber) (i + 1), name,
									   (Oid) 4242, tm, dim);
					break;
				case 2:
					TupleDescInitBuiltinEntry(desc, (AttrNumber) (i + 1), nm,
											  pg_ta_builtin_menu[code % 6],
											  tm, dim);
					break;
				case 3:
					TupleDescInitBuiltinEntry(desc, (AttrNumber) (i + 1), nm,
											  (Oid) 4242, tm, dim);
					break;
				default:
					TupleDescInitEntry(desc, (AttrNumber) (i + 1), name,
									   pg_ta_menu[code % PG_TA_NMENU].typid,
									   tm, dim);
					TupleDescInitEntryCollation(desc, (AttrNumber) (i + 1),
												(Oid) 999);
					break;
			}
		}
	}
	pg_ta_put_desc_plane(&w, desc);
	*outlen = w.off;
	return 0;
}

/*
 * attmap/tupconvert plane. which: 0/1 build_attrmap_by_name(missing_ok =
 * which), 2/3 build_attrmap_by_name_if_req(missing_ok = which - 2),
 * 4 build_attrmap_by_position, 5 convert_tuples_by_name +
 * execute_attr_map_tuple, 6 convert_tuples_by_position +
 * execute_attr_map_tuple. vals feeds the source (indesc) tuple for 5/6.
 */
int
pg_ta_attmap(const unsigned char *spec_in, int ilen,
			 const unsigned char *spec_out, int olen,
			 int which,
			 const unsigned char *vals, int valslen,
			 unsigned char *out, int outcap, int *outlen)
{
	PgTaCur		ci = {spec_in, ilen, 0};
	PgTaCur		co = {spec_out, olen, 0};
	PgTaCur		cv = {vals, valslen, 0};
	PgTaW		w = {out, outcap, 0};
	TupleDesc	indesc,
				outdesc;
	int			i;

	PG_TA_ENTRY();
	indesc = pg_ta_build_desc(&ci);
	outdesc = pg_ta_build_desc(&co);

	if (which <= 4)
	{
		AttrMap    *m;

		if (which <= 1)
			m = build_attrmap_by_name(indesc, outdesc, which == 1);
		else if (which <= 3)
			m = build_attrmap_by_name_if_req(indesc, outdesc, which == 3);
		else
			m = build_attrmap_by_position(indesc, outdesc, "pg_ta position mismatch");
		if (m == NULL)
			pg_ta_put_u8(&w, 1);
		else
		{
			pg_ta_put_u8(&w, 0);
			pg_ta_put_u16(&w, (uint16) m->maplen);
			for (i = 0; i < m->maplen; i++)
				pg_ta_put_u16(&w, (uint16) m->attnums[i]);
			free_attrmap(m);
		}
	}
	else
	{
		TupleConversionMap *map;

		if (which == 5)
			map = convert_tuples_by_name(indesc, outdesc);
		else
			map = convert_tuples_by_position(indesc, outdesc, "pg_ta position mismatch");
		if (map == NULL)
			pg_ta_put_u8(&w, 1);
		else
		{
			Datum		values[PG_TA_MAXATTS];
			bool		isnull[PG_TA_MAXATTS];
			HeapTuple	ht,
						ct;

			pg_ta_put_u8(&w, 0);
			pg_ta_put_u16(&w, (uint16) map->attrMap->maplen);
			for (i = 0; i < map->attrMap->maplen; i++)
				pg_ta_put_u16(&w, (uint16) map->attrMap->attnums[i]);
			pg_ta_read_values(&cv, indesc, indesc->natts, values, isnull);
			ht = heap_form_tuple(indesc, values, isnull);
			ct = execute_attr_map_tuple(ht, map);
			pg_ta_put_image(&w, ct->t_data, ct->t_len);
			free_conversion_map(map);
		}
	}
	*outlen = w.off;
	return 0;
}

