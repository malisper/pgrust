/*
 * pg_arrayfuncs_io.c: vendored PostgreSQL C oracle for the arrayfuncs_diff
 * differential fuzz target (100%-coverage campaign; crate
 * crates/backend/utils/adt/arrayfuncs).
 *
 * Provenance (all bodies VERBATIM unless a shim is listed below), from the
 * repo's vendored ground-truth checkout ../pgrust-fabled/vendor/postgres-src
 * @ 62d6c7d3df6287f1bd83199c1a746e50d31571a0 (REL_18 "Stamp 18.3"). Every
 * pasted range is labelled inline with its exact source lines; the ranges
 * were extracted mechanically (sed) so pasted logic is byte-for-byte:
 *   - src/backend/utils/adt/arrayfuncs.c: Array_nulls (40..43), ASSGN (44..48),
 *     ArrayToken (56..65), array_in tail (237..376), ReadArrayDimensions /
 *     ReadDimensionInt / ReadArrayStr / ReadArrayToken / CopyArrayEls
 *     (378..1008), array_out tail (1081..1260), array_get_element
 *     (1819..1915), array_get_slice (2029..2166), array_set_element
 *     (2200..2491), array_set_slice (2805..3136), construct_md_array
 *     (3493..3574), construct_empty_array (3579..3590), deconstruct_array
 *     (3630..3689), array_contains_nulls (3772..3807), support routines
 *     array_get_isnull / array_set_isnull / ArrayCast / ArrayCastAndSet /
 *     array_seek / array_nelems_size / array_copy / array_bitmap_copy /
 *     array_slice_size / array_extract_slice / array_insert_slice
 *     (4776..5267), width_bucket_array_float8/_fixed/_variable (6754..6921),
 *     width_bucket_array dispatcher checks (6704..6714).
 *   - src/backend/utils/adt/arrayutils.c: lines 25..225 (ArrayGetOffset,
 *     ArrayGetNItems(Safe), ArrayCheckBounds(Safe), mda_*) and
 *     ArrayGetIntegerTypmods (227..264).
 *   - src/backend/utils/adt/numutils.c: DIGIT_TABLE + decimalLength32
 *     (24..61), hexlookup (87..97), pg_strtoint32 + pg_strtoint32_safe
 *     (382..619; their doc comment 358..381 omitted), pg_ultoa_n (1046..1109), pg_ltoa (1111..1133).
 *   - src/backend/parser/scansup.c: scanner_isspace (107..128).
 *   - src/port/pgstrcasecmp.c: pg_strcasecmp (32..62).
 *   - src/include/varatt.h: varattrib structs + varatt macros (18..325).
 *   - src/include/utils/array.h: ArrayType (84..98), MAXDIM (75),
 *     MaxArraySize (77..82), ARR_ macros (276..323).
 *   - src/include/access/tupmacs.h: fetch_att (49..76), att_align_nominal
 *     (135..161), att_addlength_datum/pointer (168..200), store_att_byval
 *     (203..232).
 *   - src/include/common/int.h: pg_add/sub/mul_s32_overflow (147..202),
 *     pg_neg_u32_overflow (492..508) (HAVE__BUILTIN_OP_OVERFLOW arm, the
 *     arm every supported gcc/clang target compiles).
 *   - src/include/port/pg_bitutils.h: pg_leftmost_one_pos32 (35..65)
 *     (HAVE__BUILTIN_CLZ arm).
 *   - src/backend/access/nbtree/nbtcompare.c btint4cmp (199..212) and
 *     src/backend/utils/adt/varlena.c varstr_cmp collate-is-c arm
 *     (1675..1679): comparison cores transcribed into the pinned
 *     FunctionCallInvoke shim (see fmgr shims below).
 *
 * SHIMS (plumbing only, never logic — Michael's rule: mock the ENVIRONMENT,
 * never the COMPUTATION):
 *   - ereport(ERROR,...) / ereturn(escontext,...) / errsave(escontext,...):
 *     escontext is ALWAYS the NULL/hard shape on the C side, so all three
 *     record the errcode class in the shared _Thread_local pg_diff_errcode
 *     and longjmp out through pg_afx_jmp; driver entries setjmp and report
 *     verdict=error + recorded class. errmsg/errdetail/... evaluate to 0.
 *   - errcode class constants (comparator plane; Rust side maps its PgError
 *     sqlstate to the same class — see arrayfuncs_diff.rs):
 *       1 = ERRCODE_INVALID_TEXT_REPRESENTATION (22P02)
 *       2 = ERRCODE_PROGRAM_LIMIT_EXCEEDED      (54000)
 *       3 = ERRCODE_ARRAY_SUBSCRIPT_ERROR / ERRCODE_ARRAY_ELEMENT_ERROR
 *           (both 2202E in errcodes.txt — one class)
 *       5 = ERRCODE_FEATURE_NOT_SUPPORTED       (0A000)
 *       6 = ERRCODE_NULL_VALUE_NOT_ALLOWED      (22004)
 *       7 = ERRCODE_INVALID_PARAMETER_VALUE     (22023)
 *       8 = ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE  (22003)
 *       9 = ERRCODE_INTERNAL_ERROR              (XX000, elog's default)
 *   - palloc/palloc0/repalloc/pfree/pstrdup -> growable TLS pointer arena
 *     (models PG's memory-context reset; every pg_diff_* entry calls
 *     pg_afx_arena_reset() first so error-path longjmps cannot leak — the
 *     2026-07-31 LSan incident class, proofs/p1-lanej @ 7306d300196).
 *     Driver out-pointers point INTO the arena: valid until the next
 *     pg_diff_* call on the thread; the Rust driver copies eagerly.
 *   - fmgr: PG_FUNCTION_ARGS entry points (array_in, array_out,
 *     width_bucket_array) are unwrapped to plain C signatures; the
 *     my_extra/ArrayMetaState typcache lookup blocks are EXCISED and
 *     replaced by pinned element metadata selected by `elemsel`, exactly
 *     the values pg_type.dat @ this stamp carries for each type:
 *       0 int4   (23, len 4, byval, 'i', delim ',')
 *       1 text   (25, len -1, byref, 'i', delim ',')
 *       2 char   (18, len 1, byval, 'c')
 *       3 int2   (21, len 2, byval, 's')
 *       4 int8   (20, len 8, byval, 'd')
 *       5 float4 (700, len 4, byval, 'i')
 *       6 float8 (701, len 8, byval, 'd')
 *       7 name   (19, len 64 = NAMEDATALEN, byREF fixed-length, 'c')
 *       8 oid    (26, len 4, byval, 'i')
 *       9 tid    (27, len 6 = sizeof(ItemPointerData), byref, 's')
 *      10 bool   (16, len 1, byval, 'c')
 *      11 xid    (28, len 4, byval, 'i')
 *      12 cstring(2275, len -2, byref, 'c')
 *     Selectors 2..12 drive the IMAGE-OPS arms only (get/set element and
 *     slice, deconstruct, construct, contains_nulls, and the builtin-table
 *     mode); those C bodies never call an element input/output function, so
 *     no new in/out procs are shimmed. Selectors 0/1 are the only ones the
 *     array_in/array_out arms use. Excised regions are marked
 *     "SHIM: pinned element meta" with the excised source lines cited.
 *   - InputFunctionCallSafe -> direct call: int4 -> pasted
 *     pg_strtoint32_safe wrapped to Datum; text -> 4-byte-varlena-header
 *     copy of the bytes (cstring_to_text equivalent; plumbing). NULL input
 *     -> null Datum (strict input function shape, fmgr.c behavior).
 *   - OutputFunctionCall -> direct call: int4 -> pasted pg_ltoa/pg_ultoa_n
 *     chain into a palloc'd 12-byte buffer; text -> palloc'd payload + NUL.
 *   - width_bucket_array's TypeCacheEntry lookup -> pinned entries; its
 *     cmp_proc_finfo FunctionCallInvoke -> pg_afx_cmp_invoke, which runs
 *     the VERBATIM-transcribed btint4cmp comparison (elemsel 0) or the
 *     varstr_cmp collate-is-c memcmp arm (elemsel 2, text with C
 *     collation), never fmgr. elemsel 2 exists to execute the
 *     width_bucket_array_variable body.
 *   - PG_DETOAST_DATUM / DatumGetArrayTypeP -> identity casts. DRIVER
 *     PRECONDITION: every image handed to this oracle is a well-formed
 *     plain 4B-header varlena built by construct_md_array/array_in on the
 *     Rust side (or by this file's own entries); no toast, no short
 *     headers, no expanded datums. The
 *     array_get_element_expanded/array_set_element_expanded branches are
 *     therefore unreachable and stubbed with abort().
 *   - AnyArrayType / AARR_ macros / array_iter (array_out iteration): reduced to the
 *     flat-array arms of utils/array.h (328..343) and utils/arrayaccess.h
 *     (33..139); the expanded-header arms are dead under the driver
 *     precondition above.
 *   - deconstruct_array_builtin: pasted VERBATIM (arrayfuncs.c
 *     3696..3764) so its hardcoded elmlen/elmbyval/elmalign TABLE is itself
 *     dual-executed against the crate's construct.rs builtin_meta, rather
 *     than aliased to the pinned-meta shim. FLOAT8PASSBYVAL = 1 (every
 *     supported LP64 target). Its default: arm calls elog(ERROR) -> abort();
 *     Its default: arm is a live error path (class 9) — NOTE C carries TWO
 *     DIFFERENT tables: construct_array_builtin (3380..3492) accepts 12
 *     element types while deconstruct_array_builtin (3696..3764) accepts
 *     only 8 (no FLOAT4/INT8/NAME/REGTYPE/XID). Both are pasted so the
 *     asymmetry itself is dual-executed against the crate's single shared
 *     builtin_meta.
 *   - Assert/AssertMacro -> no-op (NDEBUG parity); elog(ERROR,...) records
 *     class 9 (ERRCODE_INTERNAL_ERROR / XX000, elog.c's default sqlstate
 *     when no errcode() is given) and longjmps, exactly like ereport(ERROR):
 *     the *_builtin tables' default: arms are live, reachable error paths,
 *     not unreachable internal sites.
 *   - Array_nulls GUC pasted at its default (true), matching the Rust
 *     crate's fixed behavior.
 *   - Every global symbol pasted here is renamed with a pg_afx_ prefix at
 *     preprocessor level (bodies stay verbatim) so this oracle can never
 *     cross-bind with other lanes' oracles (the merge/p1-wave1 symbol-
 *     isolation lesson in core/build.rs).
 */

#include <assert.h>
#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <math.h>
#include <setjmp.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* ---- shared error plane (defined in pg_float_io.c) ---- */
extern _Thread_local int pg_diff_errcode;

/* ---- symbol isolation: pg_afx_ prefix for every pasted global ---- */
#define array_in pg_afx_array_in
#define array_out pg_afx_array_out
#define CopyArrayEls pg_afx_CopyArrayEls
#define array_get_element pg_afx_array_get_element
#define array_get_slice pg_afx_array_get_slice
#define array_set_element pg_afx_array_set_element
#define array_set_slice pg_afx_array_set_slice
#define construct_md_array pg_afx_construct_md_array
#define construct_empty_array pg_afx_construct_empty_array
#define deconstruct_array pg_afx_deconstruct_array
#define deconstruct_array_builtin pg_afx_deconstruct_array_builtin
#define array_contains_nulls pg_afx_array_contains_nulls
#define array_bitmap_copy pg_afx_array_bitmap_copy
#define width_bucket_array pg_afx_width_bucket_array
#define ArrayGetOffset pg_afx_ArrayGetOffset
#define ArrayGetNItems pg_afx_ArrayGetNItems
#define ArrayGetNItemsSafe pg_afx_ArrayGetNItemsSafe
#define ArrayCheckBounds pg_afx_ArrayCheckBounds
#define ArrayCheckBoundsSafe pg_afx_ArrayCheckBoundsSafe
#define mda_get_range pg_afx_mda_get_range
#define mda_get_prod pg_afx_mda_get_prod
#define mda_get_offset_values pg_afx_mda_get_offset_values
#define mda_next_tuple pg_afx_mda_next_tuple
#define ArrayGetIntegerTypmods pg_afx_ArrayGetIntegerTypmods
#define Array_nulls pg_afx_Array_nulls
#define pg_strtoint32 pg_afx_pg_strtoint32
#define pg_strtoint32_safe pg_afx_pg_strtoint32_safe
#define pg_ultoa_n pg_afx_pg_ultoa_n
#define pg_ltoa pg_afx_pg_ltoa
#define scanner_isspace pg_afx_scanner_isspace
#define pg_strcasecmp pg_afx_pg_strcasecmp

/* ---- c.h base types (LP64, exactly what configure produces) ---- */
typedef int8_t int8;
typedef int16_t int16;
typedef int32_t int32;
typedef int64_t int64;
typedef uint8_t uint8;
typedef uint16_t uint16;
typedef uint32_t uint32;
typedef uint64_t uint64;
typedef double float8;
typedef size_t Size;
typedef uint32 Oid;
typedef uint8 bits8;
typedef uintptr_t Datum;
typedef char *Pointer;
#define SIZEOF_DATUM 8
#define InvalidOid ((Oid) 0)
#define FLEXIBLE_ARRAY_MEMBER /* empty */
#define PG_INT32_MIN INT32_MIN
#define PG_INT32_MAX INT32_MAX
#define UINT64CONST(x) UINT64_C(x)
#define HAVE__BUILTIN_OP_OVERFLOW 1
#define HAVE__BUILTIN_CLZ 1
#define likely(x) __builtin_expect((x) != 0, 1)
#define unlikely(x) __builtin_expect((x) != 0, 0)
#define Min(x, y) ((x) < (y) ? (x) : (y))
#define Max(x, y) ((x) > (y) ? (x) : (y))
#define Assert(x) ((void) 0)
#define AssertMacro(x) ((void) 0)
#define TrapMacro(x, y) (true)
#define PG_USED_FOR_ASSERTS_ONLY
#define pg_attribute_unused()
#define pg_nodiscard
#define IS_HIGHBIT_SET(ch) ((unsigned char) (ch) & 0x80)
struct Node; /* opaque; escontext is always NULL here */
typedef struct Node Node;

/* c.h alignment macros (LP64 values) */
#define ALIGNOF_SHORT 2
#define ALIGNOF_INT 4
#define ALIGNOF_DOUBLE 8
#define MAXIMUM_ALIGNOF 8
#define TYPEALIGN(ALIGNVAL,LEN)  \
	(((uintptr_t) (LEN) + ((ALIGNVAL) - 1)) & ~((uintptr_t) ((ALIGNVAL) - 1)))
#define SHORTALIGN(LEN) TYPEALIGN(ALIGNOF_SHORT, (LEN))
#define INTALIGN(LEN) TYPEALIGN(ALIGNOF_INT, (LEN))
#define DOUBLEALIGN(LEN) TYPEALIGN(ALIGNOF_DOUBLE, (LEN))
#define MAXALIGN(LEN) TYPEALIGN(MAXIMUM_ALIGNOF, (LEN))

/* catalog/pg_type_d.h */
#define TYPALIGN_CHAR 'c'
#define TYPALIGN_SHORT 's'
#define TYPALIGN_INT 'i'
#define TYPALIGN_DOUBLE 'd'
#define BOOLOID 16
#define CHAROID 18
#define NAMEOID 19
#define INT8OID 20
#define INT2OID 21
#define INT4OID 23
#define TEXTOID 25
#define OIDOID 26
#define TIDOID 27
#define XIDOID 28
#define FLOAT4OID 700
#define FLOAT8OID 701
#define CSTRINGOID 2275
/* pg_config.h on every supported LP64 target */
#define FLOAT8PASSBYVAL 1
/* c.h / itemptr.h: NAMEDATALEN 64, sizeof(ItemPointerData) 6 */
typedef struct ItemPointerData
{
	uint16		bi_hi;
	uint16		bi_lo;
	uint16		ip_posid;
}			ItemPointerData;

/* utils/memutils.h */
#define MaxAllocSize	((Size) 0x3fffffff) /* 1 gigabyte - 1 */
#define AllocSizeIsValid(size)	((Size) (size) <= MaxAllocSize)

/* postgres.h Datum converters (LP64 arms) */
#define DatumGetPointer(X) ((Pointer) (X))
#define PointerGetDatum(X) ((Datum) (X))
#define DatumGetCString(X) ((char *) DatumGetPointer(X))
#define CStringGetDatum(X) PointerGetDatum(X)
#define DatumGetInt32(X) ((int32) (X))
#define DatumGetInt16(X) ((int16) (X))
#define DatumGetChar(X) ((char) (X))
#define Int32GetDatum(X) ((Datum) (X))
#define Int16GetDatum(X) ((Datum) (X))
#define CharGetDatum(X) ((Datum) (X))
#define DatumGetBool(X) ((bool) ((X) != 0))
#define BoolGetDatum(X) ((Datum) ((X) ? 1 : 0))
#define VARHDRSZ ((int32) sizeof(int32))
static inline float8
DatumGetFloat8(Datum X)
{
	float8		r;
	memcpy(&r, &X, sizeof(r));
	return r;
}

/* ---- error plane: hard-shape ereport/ereturn/errsave -> longjmp ---- */
#define ERRCODE_INVALID_TEXT_REPRESENTATION 1
#define ERRCODE_PROGRAM_LIMIT_EXCEEDED 2
#define ERRCODE_ARRAY_SUBSCRIPT_ERROR 3
#define ERRCODE_ARRAY_ELEMENT_ERROR 3	/* same sqlstate 2202E */
#define ERRCODE_FEATURE_NOT_SUPPORTED 5
#define ERRCODE_NULL_VALUE_NOT_ALLOWED 6
#define ERRCODE_INVALID_PARAMETER_VALUE 7
#define ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE 8

static _Thread_local jmp_buf pg_afx_jmp;

static void
pg_afx_raise(void)
{
	longjmp(pg_afx_jmp, 1);
}

#define errcode(c) (pg_diff_errcode = (c))
#define errmsg(...) 0
#define errmsg_plural(...) 0
#define errdetail(...) 0
#define errdetail_plural(...) 0
#define errhint(...) 0
#define ereport(level, ...) do { (void) (__VA_ARGS__); pg_afx_raise(); } while (0)
#define ereturn(escontext, ret, ...) do { (void) (__VA_ARGS__); pg_afx_raise(); } while (0)
#define errsave(escontext, ...) do { (void) (__VA_ARGS__); pg_afx_raise(); } while (0)
/* elog(ERROR, ...) is a REAL PostgreSQL error (sqlstate XX000
 * internal_error, elog.c's default when no errcode() is supplied), not an
 * abort: mapping it to the error plane is what the comparator must see. The
 * *_builtin tables' default: arms are the live example. */
#define ERRCODE_INTERNAL_ERROR 9
#define elog(level, ...) \
	do { if (getenv("PG_AFX_DEBUG")) fprintf(stderr, "elog fired at pg_arrayfuncs_io.c:%d\n", __LINE__); pg_diff_errcode = ERRCODE_INTERNAL_ERROR; pg_afx_raise(); } while (0)

/* ---- palloc arena (growable; see header) ---- */
static _Thread_local void **pg_afx_arena;
static _Thread_local int pg_afx_arena_n;
static _Thread_local int pg_afx_arena_cap;

static void
pg_afx_arena_reset(void)
{
	int			i;

	for (i = 0; i < pg_afx_arena_n; i++)
		free(pg_afx_arena[i]);
	pg_afx_arena_n = 0;
}

static void
pg_afx_arena_track(void *p)
{
	if (pg_afx_arena_n == pg_afx_arena_cap)
	{
		pg_afx_arena_cap = pg_afx_arena_cap ? pg_afx_arena_cap * 2 : 64;
		pg_afx_arena = realloc(pg_afx_arena,
							   pg_afx_arena_cap * sizeof(void *));
		if (!pg_afx_arena)
			abort();
	}
	pg_afx_arena[pg_afx_arena_n++] = p;
}

static void *
pg_afx_palloc(size_t n)
{
	void	   *p = malloc(n ? n : 1);

	if (!p)
		abort();
	pg_afx_arena_track(p);
	return p;
}

static void *
pg_afx_palloc0(size_t n)
{
	void	   *p = calloc(1, n ? n : 1);

	if (!p)
		abort();
	pg_afx_arena_track(p);
	return p;
}

static void *
pg_afx_repalloc(void *old, size_t n)
{
	int			i;

	for (i = pg_afx_arena_n - 1; i >= 0; i--)
	{
		if (pg_afx_arena[i] == old)
		{
			void	   *p = realloc(old, n);

			if (!p)
				abort();
			pg_afx_arena[i] = p;
			return p;
		}
	}
	abort();					/* repalloc of a pointer the arena never issued */
}

static void
pg_afx_pfree(void *p)
{
	int			i;

	for (i = pg_afx_arena_n - 1; i >= 0; i--)
	{
		if (pg_afx_arena[i] == p)
		{
			free(p);
			pg_afx_arena[i] = pg_afx_arena[--pg_afx_arena_n];
			return;
		}
	}
	abort();					/* pfree of a pointer the arena never issued */
}

#define palloc(n) pg_afx_palloc(n)
#define palloc0(n) pg_afx_palloc0(n)
#define repalloc(p, n) pg_afx_repalloc((p), (n))
#define pfree(p) pg_afx_pfree(p)
/* utils/palloc.h type-safe allocator wrappers (verbatim shapes) */
#define palloc_array(type, count) ((type *) palloc(sizeof(type) * (count)))
#define repalloc_array(pointer, type, count) ((type *) repalloc(pointer, sizeof(type) * (count)))

static char *
pstrdup(const char *s)
{
	size_t		n = strlen(s) + 1;
	char	   *r = pg_afx_palloc(n);

	memcpy(r, s, n);
	return r;
}

/* ==== VERBATIM: varatt structs + macros (varatt.h lines 18..325 @ 62d6c7d3df) ==== */
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

/* ==== VERBATIM: utils/array.h ArrayType + limits + ARR_* macros
 * (lines 75, 77..82, 86..98, 271..323 @ 62d6c7d3df) ==== */
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

/* SHIM: AnyArrayType / AARR_* reduced to the flat-array arms of
 * utils/array.h 326..343 (expanded-header arm dead under the driver
 * precondition; see header). */
typedef union AnyArrayType
{
	ArrayType	flt;
} AnyArrayType;
#define AARR_NDIM(a) ARR_NDIM((ArrayType *) (a))
#define AARR_HASNULL(a) ARR_HASNULL((ArrayType *) (a))
#define AARR_ELEMTYPE(a) ARR_ELEMTYPE((ArrayType *) (a))
#define AARR_DIMS(a) ARR_DIMS((ArrayType *) (a))
#define AARR_LBOUND(a) ARR_LBOUND((ArrayType *) (a))

/* SHIM utils/arrayaccess.h array_iter (lines 33..139): struct with the
 * flat-array fields only; setup/next (flat arms, bodies verbatim) are
 * defined before array_out below. */
typedef struct array_iter
{
	/* Fields used when we have a flat array */
	char	   *dataptr;		/* Current spot in the data area */
	bits8	   *bitmapptr;		/* Current byte of the nulls bitmap, or NULL */
	int			bitmask;		/* mask for current bit in nulls bitmap */
} array_iter;

/* ==== VERBATIM: fetch_att (tupmacs.h lines 49..76 @ 62d6c7d3df) ==== */
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

/* ==== VERBATIM: att_align_nominal / att_addlength_* (tupmacs.h lines 135..200 @ 62d6c7d3df) ==== */
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

/* ==== VERBATIM: store_att_byval (tupmacs.h lines 203..232 @ 62d6c7d3df) ==== */
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

/* ==== VERBATIM: pg_add/sub/mul_s32_overflow (int.h lines 147..202 @ 62d6c7d3df) ==== */
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

/* ==== VERBATIM: pg_neg_u32_overflow (int.h lines 492..508 @ 62d6c7d3df) ==== */
static inline bool
pg_neg_u32_overflow(uint32 a, int32 *result)
{
#if defined(HAVE__BUILTIN_OP_OVERFLOW)
	return __builtin_sub_overflow(0, a, result);
#else
	int64		res = -((int64) a);

	if (unlikely(res < PG_INT32_MIN))
	{
		*result = 0x5EED;		/* to avoid spurious warnings */
		return true;
	}
	*result = res;
	return false;
#endif
}

/* ==== VERBATIM: pg_leftmost_one_pos32 (pg_bitutils.h lines 35..65 @ 62d6c7d3df) ==== */
/*
 * pg_leftmost_one_pos32
 *		Returns the position of the most significant set bit in "word",
 *		measured from the least significant bit.  word must not be 0.
 */
static inline int
pg_leftmost_one_pos32(uint32 word)
{
#ifdef HAVE__BUILTIN_CLZ
	Assert(word != 0);

	return 31 - __builtin_clz(word);
#elif defined(_MSC_VER)
	unsigned long result;
	bool		non_zero;

	Assert(word != 0);

	non_zero = _BitScanReverse(&result, word);
	return (int) result;
#else
	int			shift = 32 - 8;

	Assert(word != 0);

	while ((word >> shift) == 0)
		shift -= 8;

	return shift + pg_leftmost_one_pos[(word >> shift) & 255];
#endif							/* HAVE__BUILTIN_CLZ */
}

/* array.h declares Array_nulls extern; the definition is pasted below. */

/* ==== VERBATIM: scanner_isspace (scansup.c lines 107..128 @ 62d6c7d3df) ==== */
/*
 * scanner_isspace() --- return true if flex scanner considers char whitespace
 *
 * This should be used instead of the potentially locale-dependent isspace()
 * function when it's important to match the lexer's behavior.
 *
 * In principle we might need similar functions for isalnum etc, but for the
 * moment only isspace seems needed.
 */
bool
scanner_isspace(char ch)
{
	/* This must match scan.l's list of {space} characters */
	if (ch == ' ' ||
		ch == '\t' ||
		ch == '\n' ||
		ch == '\r' ||
		ch == '\v' ||
		ch == '\f')
		return true;
	return false;
}

/* ==== VERBATIM: pg_strcasecmp (pgstrcasecmp.c lines 32..62 @ 62d6c7d3df) ==== */
/*
 * Case-independent comparison of two null-terminated strings.
 */
int
pg_strcasecmp(const char *s1, const char *s2)
{
	for (;;)
	{
		unsigned char ch1 = (unsigned char) *s1++;
		unsigned char ch2 = (unsigned char) *s2++;

		if (ch1 != ch2)
		{
			if (ch1 >= 'A' && ch1 <= 'Z')
				ch1 += 'a' - 'A';
			else if (IS_HIGHBIT_SET(ch1) && isupper(ch1))
				ch1 = tolower(ch1);

			if (ch2 >= 'A' && ch2 <= 'Z')
				ch2 += 'a' - 'A';
			else if (IS_HIGHBIT_SET(ch2) && isupper(ch2))
				ch2 = tolower(ch2);

			if (ch1 != ch2)
				return (int) ch1 - (int) ch2;
		}
		if (ch1 == 0)
			break;
	}
	return 0;
}

/* Prototypes (utils/builtins.h / utils/array.h shapes) for the pastes
 * below; forward references only, symbols carry the pg_afx_ renames. */
int32 pg_strtoint32_safe(const char *s, Node *escontext);
int32 pg_strtoint32(const char *s);
int pg_ultoa_n(uint32 value, char *a);
int pg_ltoa(int32 value, char *a);
int ArrayGetNItemsSafe(int ndim, const int *dims, struct Node *escontext);
bool ArrayCheckBoundsSafe(int ndim, const int *dims, const int *lb,
						  struct Node *escontext);

/* ==== VERBATIM: DIGIT_TABLE + decimalLength32 (numutils.c lines 24..61 @ 62d6c7d3df) ==== */

/*
 * A table of all two-digit numbers. This is used to speed up decimal digit
 * generation by copying pairs of digits into the final output.
 */
static const char DIGIT_TABLE[200] =
"00" "01" "02" "03" "04" "05" "06" "07" "08" "09"
"10" "11" "12" "13" "14" "15" "16" "17" "18" "19"
"20" "21" "22" "23" "24" "25" "26" "27" "28" "29"
"30" "31" "32" "33" "34" "35" "36" "37" "38" "39"
"40" "41" "42" "43" "44" "45" "46" "47" "48" "49"
"50" "51" "52" "53" "54" "55" "56" "57" "58" "59"
"60" "61" "62" "63" "64" "65" "66" "67" "68" "69"
"70" "71" "72" "73" "74" "75" "76" "77" "78" "79"
"80" "81" "82" "83" "84" "85" "86" "87" "88" "89"
"90" "91" "92" "93" "94" "95" "96" "97" "98" "99";

/*
 * Adapted from http://graphics.stanford.edu/~seander/bithacks.html#IntegerLog10
 */
static inline int
decimalLength32(const uint32 v)
{
	int			t;
	static const uint32 PowersOfTen[] = {
		1, 10, 100,
		1000, 10000, 100000,
		1000000, 10000000, 100000000,
		1000000000
	};

	/*
	 * Compute base-10 logarithm by dividing the base-2 logarithm by a
	 * good-enough approximation of the base-2 logarithm of 10
	 */
	t = (pg_leftmost_one_pos32(v) + 1) * 1233 / 4096;
	return t + (v >= PowersOfTen[t]);
}

/* ==== VERBATIM: hexlookup (numutils.c lines 87..97 @ 62d6c7d3df) ==== */

static const int8 hexlookup[128] = {
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	0, 1, 2, 3, 4, 5, 6, 7, 8, 9, -1, -1, -1, -1, -1, -1,
	-1, 10, 11, 12, 13, 14, 15, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, 10, 11, 12, 13, 14, 15, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
};

/* ==== VERBATIM: pg_strtoint32 + pg_strtoint32_safe (doc comment 358..381 omitted) (numutils.c lines 382..619 @ 62d6c7d3df) ==== */
int32
pg_strtoint32(const char *s)
{
	return pg_strtoint32_safe(s, NULL);
}

int32
pg_strtoint32_safe(const char *s, Node *escontext)
{
	const char *ptr = s;
	const char *firstdigit;
	uint32		tmp = 0;
	bool		neg = false;
	unsigned char digit;
	int32		result;

	/*
	 * The majority of cases are likely to be base-10 digits without any
	 * underscore separator characters.  We'll first try to parse the string
	 * with the assumption that's the case and only fallback on a slower
	 * implementation which handles hex, octal and binary strings and
	 * underscores if the fastpath version cannot parse the string.
	 */

	/* leave it up to the slow path to look for leading spaces */

	if (*ptr == '-')
	{
		ptr++;
		neg = true;
	}

	/* a leading '+' is uncommon so leave that for the slow path */

	/* process the first digit */
	digit = (*ptr - '0');

	/*
	 * Exploit unsigned arithmetic to save having to check both the upper and
	 * lower bounds of the digit.
	 */
	if (likely(digit < 10))
	{
		ptr++;
		tmp = digit;
	}
	else
	{
		/* we need at least one digit */
		goto slow;
	}

	/* process remaining digits */
	for (;;)
	{
		digit = (*ptr - '0');

		if (digit >= 10)
			break;

		ptr++;

		if (unlikely(tmp > -(PG_INT32_MIN / 10)))
			goto out_of_range;

		tmp = tmp * 10 + digit;
	}

	/* when the string does not end in a digit, let the slow path handle it */
	if (unlikely(*ptr != '\0'))
		goto slow;

	if (neg)
	{
		if (unlikely(pg_neg_u32_overflow(tmp, &result)))
			goto out_of_range;
		return result;
	}

	if (unlikely(tmp > PG_INT32_MAX))
		goto out_of_range;

	return (int32) tmp;

slow:
	tmp = 0;
	ptr = s;
	/* no need to reset neg */

	/* skip leading spaces */
	while (isspace((unsigned char) *ptr))
		ptr++;

	/* handle sign */
	if (*ptr == '-')
	{
		ptr++;
		neg = true;
	}
	else if (*ptr == '+')
		ptr++;

	/* process digits */
	if (ptr[0] == '0' && (ptr[1] == 'x' || ptr[1] == 'X'))
	{
		firstdigit = ptr += 2;

		for (;;)
		{
			if (isxdigit((unsigned char) *ptr))
			{
				if (unlikely(tmp > -(PG_INT32_MIN / 16)))
					goto out_of_range;

				tmp = tmp * 16 + hexlookup[(unsigned char) *ptr++];
			}
			else if (*ptr == '_')
			{
				/* underscore must be followed by more digits */
				ptr++;
				if (*ptr == '\0' || !isxdigit((unsigned char) *ptr))
					goto invalid_syntax;
			}
			else
				break;
		}
	}
	else if (ptr[0] == '0' && (ptr[1] == 'o' || ptr[1] == 'O'))
	{
		firstdigit = ptr += 2;

		for (;;)
		{
			if (*ptr >= '0' && *ptr <= '7')
			{
				if (unlikely(tmp > -(PG_INT32_MIN / 8)))
					goto out_of_range;

				tmp = tmp * 8 + (*ptr++ - '0');
			}
			else if (*ptr == '_')
			{
				/* underscore must be followed by more digits */
				ptr++;
				if (*ptr == '\0' || *ptr < '0' || *ptr > '7')
					goto invalid_syntax;
			}
			else
				break;
		}
	}
	else if (ptr[0] == '0' && (ptr[1] == 'b' || ptr[1] == 'B'))
	{
		firstdigit = ptr += 2;

		for (;;)
		{
			if (*ptr >= '0' && *ptr <= '1')
			{
				if (unlikely(tmp > -(PG_INT32_MIN / 2)))
					goto out_of_range;

				tmp = tmp * 2 + (*ptr++ - '0');
			}
			else if (*ptr == '_')
			{
				/* underscore must be followed by more digits */
				ptr++;
				if (*ptr == '\0' || *ptr < '0' || *ptr > '1')
					goto invalid_syntax;
			}
			else
				break;
		}
	}
	else
	{
		firstdigit = ptr;

		for (;;)
		{
			if (*ptr >= '0' && *ptr <= '9')
			{
				if (unlikely(tmp > -(PG_INT32_MIN / 10)))
					goto out_of_range;

				tmp = tmp * 10 + (*ptr++ - '0');
			}
			else if (*ptr == '_')
			{
				/* underscore may not be first */
				if (unlikely(ptr == firstdigit))
					goto invalid_syntax;
				/* and it must be followed by more digits */
				ptr++;
				if (*ptr == '\0' || !isdigit((unsigned char) *ptr))
					goto invalid_syntax;
			}
			else
				break;
		}
	}

	/* require at least one digit */
	if (unlikely(ptr == firstdigit))
		goto invalid_syntax;

	/* allow trailing whitespace, but not other trailing chars */
	while (isspace((unsigned char) *ptr))
		ptr++;

	if (unlikely(*ptr != '\0'))
		goto invalid_syntax;

	if (neg)
	{
		if (unlikely(pg_neg_u32_overflow(tmp, &result)))
			goto out_of_range;
		return result;
	}

	if (tmp > PG_INT32_MAX)
		goto out_of_range;

	return (int32) tmp;

out_of_range:
	ereturn(escontext, 0,
			(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
			 errmsg("value \"%s\" is out of range for type %s",
					s, "integer")));

invalid_syntax:
	ereturn(escontext, 0,
			(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
			 errmsg("invalid input syntax for type %s: \"%s\"",
					"integer", s)));
}

/* ==== VERBATIM: pg_ultoa_n (numutils.c lines 1046..1109 @ 62d6c7d3df) ==== */

/*
 * pg_ultoa_n: converts an unsigned 32-bit integer to its string representation,
 * not NUL-terminated, and returns the length of that string representation
 *
 * Caller must ensure that 'a' points to enough memory to hold the result (at
 * least 10 bytes)
 */
int
pg_ultoa_n(uint32 value, char *a)
{
	int			olength,
				i = 0;

	/* Degenerate case */
	if (value == 0)
	{
		*a = '0';
		return 1;
	}

	olength = decimalLength32(value);

	/* Compute the result string. */
	while (value >= 10000)
	{
		const uint32 c = value - 10000 * (value / 10000);
		const uint32 c0 = (c % 100) << 1;
		const uint32 c1 = (c / 100) << 1;

		char	   *pos = a + olength - i;

		value /= 10000;

		memcpy(pos - 2, DIGIT_TABLE + c0, 2);
		memcpy(pos - 4, DIGIT_TABLE + c1, 2);
		i += 4;
	}
	if (value >= 100)
	{
		const uint32 c = (value % 100) << 1;

		char	   *pos = a + olength - i;

		value /= 100;

		memcpy(pos - 2, DIGIT_TABLE + c, 2);
		i += 2;
	}
	if (value >= 10)
	{
		const uint32 c = value << 1;

		char	   *pos = a + olength - i;

		memcpy(pos - 2, DIGIT_TABLE + c, 2);
	}
	else
	{
		*a = (char) ('0' + value);
	}

	return olength;
}

/* ==== VERBATIM: pg_ltoa (numutils.c lines 1111..1133 @ 62d6c7d3df) ==== */
/*
 * pg_ltoa: converts a signed 32-bit integer to its string representation and
 * returns strlen(a).
 *
 * It is the caller's responsibility to ensure that a is at least 12 bytes long,
 * which is enough room to hold a minus sign, a maximally long int32, and the
 * above terminating NUL.
 */
int
pg_ltoa(int32 value, char *a)
{
	uint32		uvalue = (uint32) value;
	int			len = 0;

	if (value < 0)
	{
		uvalue = (uint32) 0 - uvalue;
		a[len++] = '-';
	}
	len += pg_ultoa_n(uvalue, a + len);
	a[len] = '\0';
	return len;
}

/* ==== VERBATIM: ArrayGetOffset..mda_next_tuple (arrayutils.c lines 25..225 @ 62d6c7d3df) ==== */
/*
 * Convert subscript list into linear element number (from 0)
 *
 * We assume caller has already range-checked the dimensions and subscripts,
 * so no overflow is possible.
 */
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

/*
 * Convert array dimensions into number of elements
 *
 * This must do overflow checking, since it is used to validate that a user
 * dimensionality request doesn't overflow what we can handle.
 *
 * The multiplication overflow check only works on machines that have int64
 * arithmetic, but that is nearly all platforms these days, and doing check
 * divides for those that don't seems way too expensive.
 */
int
ArrayGetNItems(int ndim, const int *dims)
{
	return ArrayGetNItemsSafe(ndim, dims, NULL);
}

/*
 * This entry point can return the error into an ErrorSaveContext
 * instead of throwing an exception.  -1 is returned after an error.
 */
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

/*
 * Verify sanity of proposed lower-bound values for an array
 *
 * The lower-bound values must not be so large as to cause overflow when
 * calculating subscripts, e.g. lower bound 2147483640 with length 10
 * must be disallowed.  We actually insist that dims[i] + lb[i] be
 * computable without overflow, meaning that an array with last subscript
 * equal to INT_MAX will be disallowed.
 *
 * It is assumed that the caller already called ArrayGetNItems, so that
 * overflowed (negative) dims[] values have been eliminated.
 */
void
ArrayCheckBounds(int ndim, const int *dims, const int *lb)
{
	(void) ArrayCheckBoundsSafe(ndim, dims, lb, NULL);
}

/*
 * This entry point can return the error into an ErrorSaveContext
 * instead of throwing an exception.
 */
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

/*
 * Compute ranges (sub-array dimensions) for an array slice
 *
 * We assume caller has validated slice endpoints, so overflow is impossible
 */
void
mda_get_range(int n, int *span, const int *st, const int *endp)
{
	int			i;

	for (i = 0; i < n; i++)
		span[i] = endp[i] - st[i] + 1;
}

/*
 * Compute products of array dimensions, ie, scale factors for subscripts
 *
 * We assume caller has validated dimensions, so overflow is impossible
 */
void
mda_get_prod(int n, const int *range, int *prod)
{
	int			i;

	prod[n - 1] = 1;
	for (i = n - 2; i >= 0; i--)
		prod[i] = prod[i + 1] * range[i + 1];
}

/*
 * From products of whole-array dimensions and spans of a sub-array,
 * compute offset distances needed to step through subarray within array
 *
 * We assume caller has validated dimensions, so overflow is impossible
 */
void
mda_get_offset_values(int n, int *dist, const int *prod, const int *span)
{
	int			i,
				j;

	dist[n - 1] = 0;
	for (j = n - 2; j >= 0; j--)
	{
		dist[j] = prod[j] - 1;
		for (i = j + 1; i < n; i++)
			dist[j] -= (span[i] - 1) * prod[i];
	}
}

/*
 * Generates the tuple that is lexicographically one greater than the current
 * n-tuple in "curr", with the restriction that the i-th element of "curr" is
 * less than the i-th element of "span".
 *
 * Returns -1 if no next tuple exists, else the subscript position (0..n-1)
 * corresponding to the dimension to advance along.
 *
 * We assume caller has validated dimensions, so overflow is impossible
 */
int
mda_next_tuple(int n, int *curr, const int *span)
{
	int			i;

	if (n <= 0)
		return -1;

	curr[n - 1] = (curr[n - 1] + 1) % span[n - 1];
	for (i = n - 1; i && curr[i] == 0; i--)
		curr[i - 1] = (curr[i - 1] + 1) % span[i - 1];

	if (i)
		return i;
	if (curr[0])
		return 0;

	return -1;
}

/* ================= SHIMS: StringInfo / fmgr environment ================= */

/* StringInfo: compact palloc-append buffer. Buffer growth is
 * lib/stringinfo.c plumbing, not arrayfuncs.c logic; append semantics
 * (data always NUL-terminated, len excludes the NUL) are identical. */
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
resetStringInfo(StringInfo str)
{
	str->len = 0;
	str->data[0] = '\0';
}

static void
appendStringInfoChar(StringInfo str, char ch)
{
	if (str->len + 2 > str->maxlen)
	{
		str->maxlen *= 2;
		str->data = repalloc(str->data, str->maxlen);
	}
	str->data[str->len++] = ch;
	str->data[str->len] = '\0';
}

/* fmgr: FmgrInfo carries only the pinned element selector. */
typedef struct FmgrInfo
{
	int			elemsel;		/* 0 = int4, 1 = text */
} FmgrInfo;

/* utils/array.h ArrayMetaState, with the shim FmgrInfo above. */
typedef struct ArrayMetaState
{
	Oid			element_type;
	int16		typlen;
	bool		typbyval;
	char		typalign;
	char		typdelim;
	Oid			typioparam;
	Oid			typiofunc;
	FmgrInfo	proc;
} ArrayMetaState;

/* SHIM: pinned element metadata table. Every row is the (typlen, typbyval,
 * typalign) triple pg_type.dat @ 62d6c7d3df carries for that type; NAMEDATALEN
 * is 64 and sizeof(ItemPointerData) is 6 on every supported target.
 * PG_AFX_NSEL rows; selectors 0/1 keep their original meaning so the banked
 * corpus stays meaningful. */
#define PG_AFX_NSEL 13
static const struct
{
	Oid			oid;
	int16		typlen;
	bool		typbyval;
	char		typalign;
}			pg_afx_metatab[PG_AFX_NSEL] = {
	{INT4OID, 4, true, TYPALIGN_INT},		/* 0  int4 */
	{TEXTOID, -1, false, TYPALIGN_INT},		/* 1  text */
	{CHAROID, 1, true, TYPALIGN_CHAR},		/* 2  "char" */
	{INT2OID, 2, true, TYPALIGN_SHORT},		/* 3  int2 */
	{INT8OID, 8, true, TYPALIGN_DOUBLE},	/* 4  int8 */
	{FLOAT4OID, 4, true, TYPALIGN_INT},		/* 5  float4 */
	{FLOAT8OID, 8, true, TYPALIGN_DOUBLE},	/* 6  float8 */
	{NAMEOID, 64, false, TYPALIGN_CHAR},	/* 7  name (byref fixed-len) */
	{OIDOID, 4, true, TYPALIGN_INT},		/* 8  oid */
	{TIDOID, 6, false, TYPALIGN_SHORT},		/* 9  tid (byref fixed-len) */
	{BOOLOID, 1, true, TYPALIGN_CHAR},		/* 10 bool */
	{XIDOID, 4, true, TYPALIGN_INT},		/* 11 xid */
	{CSTRINGOID, -2, false, TYPALIGN_CHAR},	/* 12 cstring */
};

static void
pg_afx_fill_meta(ArrayMetaState *m, int elemsel)
{
	if (elemsel < 0 || elemsel >= PG_AFX_NSEL)
		abort();				/* driver contract */
	m->element_type = pg_afx_metatab[elemsel].oid;
	m->typlen = pg_afx_metatab[elemsel].typlen;
	m->typbyval = pg_afx_metatab[elemsel].typbyval;
	m->typalign = pg_afx_metatab[elemsel].typalign;
	m->typdelim = ',';			/* every type above carries typdelim ',' */
	m->typioparam = m->element_type;
	m->typiofunc = InvalidOid;
	/* in/out procs exist for int4 (0) and text (1) only; the image-ops arms
	 * never call them (see header). */
	m->proc.elemsel = elemsel <= 1 ? elemsel : -1;
}

/* SHIM cstring_to_text equivalent: plain 4B-header varlena (plumbing). */
static Datum
pg_afx_make_text(const char *bytes, size_t len)
{
	char	   *v = palloc(VARHDRSZ + len);

	SET_VARSIZE(v, VARHDRSZ + len);
	memcpy(v + VARHDRSZ, bytes, len);
	return PointerGetDatum(v);
}

/* SHIM InputFunctionCallSafe (fmgr.c shape): NULL input -> null Datum
 * (strict input function); escontext is hard so element errors longjmp. */
static bool
InputFunctionCallSafe(FmgrInfo *flinfo, char *str, Oid typioparam,
					  int32 typmod, struct Node *escontext, Datum *result)
{
	(void) typioparam;
	(void) typmod;
	if (str == NULL)
	{
		*result = (Datum) 0;
		return true;
	}
	if (flinfo->elemsel == 0)
		*result = Int32GetDatum(pg_strtoint32_safe(str, escontext));
	else if (flinfo->elemsel == 1)
		*result = pg_afx_make_text(str, strlen(str));
	else
		abort();				/* image-ops selector reached an io proc */
	return true;
}

/* SHIM OutputFunctionCall: int4 -> pasted pg_ltoa chain; text -> payload+NUL. */
static char *
OutputFunctionCall(FmgrInfo *flinfo, Datum d)
{
	if (flinfo->elemsel == 0)
	{
		char	   *buf = palloc(12);

		pg_ltoa(DatumGetInt32(d), buf);
		return buf;
	}
	else if (flinfo->elemsel != 1)
		abort();				/* image-ops selector reached an io proc */
	else
	{
		char	   *v = DatumGetPointer(d);
		size_t		len = VARSIZE_ANY_EXHDR(v);
		char	   *buf = palloc(len + 1);

		memcpy(buf, VARDATA_ANY(v), len);
		buf[len] = '\0';
		return buf;
	}
}

/* SHIM PG_DETOAST_DATUM / DatumGetArrayTypeP: identity (driver
 * precondition: plain 4B images only). */
#define PG_DETOAST_DATUM(d) ((void *) DatumGetPointer(d))
#define DatumGetArrayTypeP(d) ((ArrayType *) DatumGetPointer(d))
#define PG_RETURN_ARRAYTYPE_P(x) return PointerGetDatum(x)
#define PG_RETURN_CSTRING(x) return PointerGetDatum(x)
#define PG_RETURN_INT32(x) return Int32GetDatum(x)
#define PG_FREE_IF_COPY(ptr, n) ((void) 0)	/* never a copy: no toast here */

/* SHIM typcache: pinned entries for width_bucket_array. */
typedef struct TypeCacheEntry
{
	Oid			type_id;
	int16		typlen;
	bool		typbyval;
	char		typalign;
	FmgrInfo	cmp_proc_finfo;
} TypeCacheEntry;

static TypeCacheEntry *
pg_afx_typentry(int elemsel)
{
	static _Thread_local TypeCacheEntry e;

	if (elemsel == 0)
	{
		e.type_id = INT4OID;
		e.typlen = 4;
		e.typbyval = true;
		e.typalign = TYPALIGN_INT;
	}
	else
	{
		e.type_id = TEXTOID;
		e.typlen = -1;
		e.typbyval = false;
		e.typalign = TYPALIGN_INT;
	}
	e.cmp_proc_finfo.elemsel = elemsel;
	return &e;
}

/* SHIM LOCAL_FCINFO family: two-arg frame + pinned comparator dispatch. */
typedef struct NullableDatum
{
	Datum		value;
	bool		isnull;
} NullableDatum;
typedef struct FunctionCallInfoBaseData
{
	FmgrInfo   *flinfo;
	bool		isnull;
	NullableDatum args[2];
} FunctionCallInfoBaseData;
typedef FunctionCallInfoBaseData *FunctionCallInfo;
#define LOCAL_FCINFO(name, nargs) \
	FunctionCallInfoBaseData name##_data; \
	FunctionCallInfo name = &name##_data
#define InitFunctionCallInfoData(Fcinfo, Flinfo, Nargs, Collation, Context, Resultinfo) \
	((Fcinfo).flinfo = (Flinfo), (Fcinfo).isnull = false)

/* Pinned comparators (see header): btint4cmp comparison transcribed
 * verbatim from nbtcompare.c 199..212 (A_GREATER_THAN_B=1,
 * A_LESS_THAN_B=-1); text comparison is the varstr_cmp collate-is-c arm
 * transcribed verbatim from varlena.c 1675..1679 over VARDATA_ANY/
 * VARSIZE_ANY_EXHDR (bttextcmp -> text_cmp -> varstr_cmp, C collation). */
static Datum
pg_afx_cmp_invoke(FunctionCallInfo fcinfo)
{
	if (fcinfo->flinfo->elemsel == 0)
	{
		int32		a = DatumGetInt32(fcinfo->args[0].value);
		int32		b = DatumGetInt32(fcinfo->args[1].value);

		if (a > b)
			return Int32GetDatum(1);
		else if (a == b)
			return Int32GetDatum(0);
		else
			return Int32GetDatum(-1);
	}
	else
	{
		char	   *t1 = DatumGetPointer(fcinfo->args[0].value);
		char	   *t2 = DatumGetPointer(fcinfo->args[1].value);
		const char *arg1 = VARDATA_ANY(t1);
		const char *arg2 = VARDATA_ANY(t2);
		int			len1 = (int) VARSIZE_ANY_EXHDR(t1);
		int			len2 = (int) VARSIZE_ANY_EXHDR(t2);
		int			result;

		result = memcmp(arg1, arg2, Min(len1, len2));
		if ((result == 0) && (len1 != len2))
			result = (len1 < len2) ? -1 : 1;
		return Int32GetDatum(result);
	}
}
#define FunctionCallInvoke(fcinfo) pg_afx_cmp_invoke(fcinfo)

/* Static prototypes transcribed from arrayfuncs.c 100..168 for the
 * forward references inside the pasted bodies. */
static bool ReadArrayDimensions(char **srcptr, int *ndim_p, int *dim,
								int *lBound, const char *origStr,
								Node *escontext);
static bool ReadDimensionInt(char **srcptr, int *result,
							 const char *origStr, Node *escontext);
static Datum pg_afx_array_get_element_expanded(Datum arraydatum,
											   int nSubscripts, int *indx,
											   int arraytyplen,
											   int elmlen, bool elmbyval,
											   char elmalign, bool *isNull);
static Datum pg_afx_array_set_element_expanded(Datum arraydatum,
											   int nSubscripts, int *indx,
											   Datum dataValue, bool isNull,
											   int arraytyplen,
											   int elmlen, bool elmbyval,
											   char elmalign);
#define array_get_element_expanded pg_afx_array_get_element_expanded
#define array_set_element_expanded pg_afx_array_set_element_expanded
/* VARATT_IS_EXTERNAL_EXPANDED is real (varatt.h paste): plain 4B images
 * never satisfy it, so these stubs are unreachable (driver precondition). */
static Datum
pg_afx_array_get_element_expanded(Datum arraydatum, int nSubscripts,
								  int *indx, int arraytyplen, int elmlen,
								  bool elmbyval, char elmalign, bool *isNull)
{
	abort();
}
static Datum
pg_afx_array_set_element_expanded(Datum arraydatum, int nSubscripts,
								  int *indx, Datum dataValue, bool isNull,
								  int arraytyplen, int elmlen, bool elmbyval,
								  char elmalign)
{
	abort();
}
ArrayType  *construct_empty_array(Oid elmtype);
ArrayType  *construct_array(Datum *elems, int nelems, Oid elmtype,
							int elmlen, bool elmbyval, char elmalign);
ArrayType  *construct_array_builtin(Datum *elems, int nelems, Oid elmtype);
#define construct_array_builtin pg_afx_construct_array_builtin
/* catalog/pg_type_d.h + c.h shapes the builtin tables reference */
#define REGTYPEOID 2206
#define NAMEDATALEN 64
typedef uint32 TransactionId;
typedef float float4;
static int	width_bucket_array_float8(Datum operand, ArrayType *thresholds);
static int	width_bucket_array_fixed(Datum operand, ArrayType *thresholds,
									 Oid collation, TypeCacheEntry *typentry);
static int	width_bucket_array_variable(Datum operand, ArrayType *thresholds,
										Oid collation,
										TypeCacheEntry *typentry);
void		deconstruct_array_builtin(ArrayType *array, Oid elmtype,
									  Datum **elemsp, bool **nullsp,
									  int *nelemsp);

/* ==== VERBATIM: Array_nulls GUC (default true) (arrayfuncs.c lines 40..43 @ 62d6c7d3df) ==== */
/*
 * GUC parameter
 */
bool		Array_nulls = true;

/* ==== VERBATIM: ASSGN (arrayfuncs.c lines 44..48 @ 62d6c7d3df) ==== */

/*
 * Local definitions
 */
#define ASSGN	 "="

/* ==== VERBATIM: ArrayToken (arrayfuncs.c lines 56..65 @ 62d6c7d3df) ==== */
/* ReadArrayToken return type */
typedef enum
{
	ATOK_LEVEL_START,
	ATOK_LEVEL_END,
	ATOK_DELIM,
	ATOK_ELEM,
	ATOK_ELEM_NULL,
	ATOK_ERROR,
} ArrayToken;

/* Static prototype (arrayfuncs.c 105..106 shape) for the forward
 * reference from ReadArrayStr. */
static ArrayToken ReadArrayToken(char **srcptr, StringInfo elembuf,
								 char typdelim, const char *origStr,
								 Node *escontext);

/* ==== VERBATIM: support routines (array_get_isnull..array_insert_slice) (arrayfuncs.c lines 4776..5267 @ 62d6c7d3df) ==== */
/***************************************************************************/
/******************|		  Support  Routines			  |*****************/
/***************************************************************************/

/*
 * Check whether a specific array element is NULL
 *
 * nullbitmap: pointer to array's null bitmap (NULL if none)
 * offset: 0-based linear element number of array element
 */
static bool
array_get_isnull(const bits8 *nullbitmap, int offset)
{
	if (nullbitmap == NULL)
		return false;			/* assume not null */
	if (nullbitmap[offset / 8] & (1 << (offset % 8)))
		return false;			/* not null */
	return true;
}

/*
 * Set a specific array element's null-bitmap entry
 *
 * nullbitmap: pointer to array's null bitmap (mustn't be NULL)
 * offset: 0-based linear element number of array element
 * isNull: null status to set
 */
static void
array_set_isnull(bits8 *nullbitmap, int offset, bool isNull)
{
	int			bitmask;

	nullbitmap += offset / 8;
	bitmask = 1 << (offset % 8);
	if (isNull)
		*nullbitmap &= ~bitmask;
	else
		*nullbitmap |= bitmask;
}

/*
 * Fetch array element at pointer, converted correctly to a Datum
 *
 * Caller must have handled case of NULL element
 */
static Datum
ArrayCast(char *value, bool byval, int len)
{
	return fetch_att(value, byval, len);
}

/*
 * Copy datum to *dest and return total space used (including align padding)
 *
 * Caller must have handled case of NULL element
 */
static int
ArrayCastAndSet(Datum src,
				int typlen,
				bool typbyval,
				char typalign,
				char *dest)
{
	int			inc;

	if (typlen > 0)
	{
		if (typbyval)
			store_att_byval(dest, src, typlen);
		else
			memmove(dest, DatumGetPointer(src), typlen);
		inc = att_align_nominal(typlen, typalign);
	}
	else
	{
		Assert(!typbyval);
		inc = att_addlength_datum(0, typlen, src);
		memmove(dest, DatumGetPointer(src), inc);
		inc = att_align_nominal(inc, typalign);
	}

	return inc;
}

/*
 * Advance ptr over nitems array elements
 *
 * ptr: starting location in array
 * offset: 0-based linear element number of first element (the one at *ptr)
 * nullbitmap: start of array's null bitmap, or NULL if none
 * nitems: number of array elements to advance over (>= 0)
 * typlen, typbyval, typalign: storage parameters of array element datatype
 *
 * It is caller's responsibility to ensure that nitems is within range
 */
static char *
array_seek(char *ptr, int offset, bits8 *nullbitmap, int nitems,
		   int typlen, bool typbyval, char typalign)
{
	int			bitmask;
	int			i;

	/* easy if fixed-size elements and no NULLs */
	if (typlen > 0 && !nullbitmap)
		return ptr + nitems * ((Size) att_align_nominal(typlen, typalign));

	/* seems worth having separate loops for NULL and no-NULLs cases */
	if (nullbitmap)
	{
		nullbitmap += offset / 8;
		bitmask = 1 << (offset % 8);

		for (i = 0; i < nitems; i++)
		{
			if (*nullbitmap & bitmask)
			{
				ptr = att_addlength_pointer(ptr, typlen, ptr);
				ptr = (char *) att_align_nominal(ptr, typalign);
			}
			bitmask <<= 1;
			if (bitmask == 0x100)
			{
				nullbitmap++;
				bitmask = 1;
			}
		}
	}
	else
	{
		for (i = 0; i < nitems; i++)
		{
			ptr = att_addlength_pointer(ptr, typlen, ptr);
			ptr = (char *) att_align_nominal(ptr, typalign);
		}
	}
	return ptr;
}

/*
 * Compute total size of the nitems array elements starting at *ptr
 *
 * Parameters same as for array_seek
 */
static int
array_nelems_size(char *ptr, int offset, bits8 *nullbitmap, int nitems,
				  int typlen, bool typbyval, char typalign)
{
	return array_seek(ptr, offset, nullbitmap, nitems,
					  typlen, typbyval, typalign) - ptr;
}

/*
 * Copy nitems array elements from srcptr to destptr
 *
 * destptr: starting destination location (must be enough room!)
 * nitems: number of array elements to copy (>= 0)
 * srcptr: starting location in source array
 * offset: 0-based linear element number of first element (the one at *srcptr)
 * nullbitmap: start of source array's null bitmap, or NULL if none
 * typlen, typbyval, typalign: storage parameters of array element datatype
 *
 * Returns number of bytes copied
 *
 * NB: this does not take care of setting up the destination's null bitmap!
 */
static int
array_copy(char *destptr, int nitems,
		   char *srcptr, int offset, bits8 *nullbitmap,
		   int typlen, bool typbyval, char typalign)
{
	int			numbytes;

	numbytes = array_nelems_size(srcptr, offset, nullbitmap, nitems,
								 typlen, typbyval, typalign);
	memcpy(destptr, srcptr, numbytes);
	return numbytes;
}

/*
 * Copy nitems null-bitmap bits from source to destination
 *
 * destbitmap: start of destination array's null bitmap (mustn't be NULL)
 * destoffset: 0-based linear element number of first dest element
 * srcbitmap: start of source array's null bitmap, or NULL if none
 * srcoffset: 0-based linear element number of first source element
 * nitems: number of bits to copy (>= 0)
 *
 * If srcbitmap is NULL then we assume the source is all-non-NULL and
 * fill 1's into the destination bitmap.  Note that only the specified
 * bits in the destination map are changed, not any before or after.
 *
 * Note: this could certainly be optimized using standard bitblt methods.
 * However, it's not clear that the typical Postgres array has enough elements
 * to make it worth worrying too much.  For the moment, KISS.
 */
void
array_bitmap_copy(bits8 *destbitmap, int destoffset,
				  const bits8 *srcbitmap, int srcoffset,
				  int nitems)
{
	int			destbitmask,
				destbitval,
				srcbitmask,
				srcbitval;

	Assert(destbitmap);
	if (nitems <= 0)
		return;					/* don't risk fetch off end of memory */
	destbitmap += destoffset / 8;
	destbitmask = 1 << (destoffset % 8);
	destbitval = *destbitmap;
	if (srcbitmap)
	{
		srcbitmap += srcoffset / 8;
		srcbitmask = 1 << (srcoffset % 8);
		srcbitval = *srcbitmap;
		while (nitems-- > 0)
		{
			if (srcbitval & srcbitmask)
				destbitval |= destbitmask;
			else
				destbitval &= ~destbitmask;
			destbitmask <<= 1;
			if (destbitmask == 0x100)
			{
				*destbitmap++ = destbitval;
				destbitmask = 1;
				if (nitems > 0)
					destbitval = *destbitmap;
			}
			srcbitmask <<= 1;
			if (srcbitmask == 0x100)
			{
				srcbitmap++;
				srcbitmask = 1;
				if (nitems > 0)
					srcbitval = *srcbitmap;
			}
		}
		if (destbitmask != 1)
			*destbitmap = destbitval;
	}
	else
	{
		while (nitems-- > 0)
		{
			destbitval |= destbitmask;
			destbitmask <<= 1;
			if (destbitmask == 0x100)
			{
				*destbitmap++ = destbitval;
				destbitmask = 1;
				if (nitems > 0)
					destbitval = *destbitmap;
			}
		}
		if (destbitmask != 1)
			*destbitmap = destbitval;
	}
}

/*
 * Compute space needed for a slice of an array
 *
 * We assume the caller has verified that the slice coordinates are valid.
 */
static int
array_slice_size(char *arraydataptr, bits8 *arraynullsptr,
				 int ndim, int *dim, int *lb,
				 int *st, int *endp,
				 int typlen, bool typbyval, char typalign)
{
	int			src_offset,
				span[MAXDIM],
				prod[MAXDIM],
				dist[MAXDIM],
				indx[MAXDIM];
	char	   *ptr;
	int			i,
				j,
				inc;
	int			count = 0;

	mda_get_range(ndim, span, st, endp);

	/* Pretty easy for fixed element length without nulls ... */
	if (typlen > 0 && !arraynullsptr)
		return ArrayGetNItems(ndim, span) * att_align_nominal(typlen, typalign);

	/* Else gotta do it the hard way */
	src_offset = ArrayGetOffset(ndim, dim, lb, st);
	ptr = array_seek(arraydataptr, 0, arraynullsptr, src_offset,
					 typlen, typbyval, typalign);
	mda_get_prod(ndim, dim, prod);
	mda_get_offset_values(ndim, dist, prod, span);
	for (i = 0; i < ndim; i++)
		indx[i] = 0;
	j = ndim - 1;
	do
	{
		if (dist[j])
		{
			ptr = array_seek(ptr, src_offset, arraynullsptr, dist[j],
							 typlen, typbyval, typalign);
			src_offset += dist[j];
		}
		if (!array_get_isnull(arraynullsptr, src_offset))
		{
			inc = att_addlength_pointer(0, typlen, ptr);
			inc = att_align_nominal(inc, typalign);
			ptr += inc;
			count += inc;
		}
		src_offset++;
	} while ((j = mda_next_tuple(ndim, indx, span)) != -1);
	return count;
}

/*
 * Extract a slice of an array into consecutive elements in the destination
 * array.
 *
 * We assume the caller has verified that the slice coordinates are valid,
 * allocated enough storage for the result, and initialized the header
 * of the new array.
 */
static void
array_extract_slice(ArrayType *newarray,
					int ndim,
					int *dim,
					int *lb,
					char *arraydataptr,
					bits8 *arraynullsptr,
					int *st,
					int *endp,
					int typlen,
					bool typbyval,
					char typalign)
{
	char	   *destdataptr = ARR_DATA_PTR(newarray);
	bits8	   *destnullsptr = ARR_NULLBITMAP(newarray);
	char	   *srcdataptr;
	int			src_offset,
				dest_offset,
				prod[MAXDIM],
				span[MAXDIM],
				dist[MAXDIM],
				indx[MAXDIM];
	int			i,
				j,
				inc;

	src_offset = ArrayGetOffset(ndim, dim, lb, st);
	srcdataptr = array_seek(arraydataptr, 0, arraynullsptr, src_offset,
							typlen, typbyval, typalign);
	mda_get_prod(ndim, dim, prod);
	mda_get_range(ndim, span, st, endp);
	mda_get_offset_values(ndim, dist, prod, span);
	for (i = 0; i < ndim; i++)
		indx[i] = 0;
	dest_offset = 0;
	j = ndim - 1;
	do
	{
		if (dist[j])
		{
			/* skip unwanted elements */
			srcdataptr = array_seek(srcdataptr, src_offset, arraynullsptr,
									dist[j],
									typlen, typbyval, typalign);
			src_offset += dist[j];
		}
		inc = array_copy(destdataptr, 1,
						 srcdataptr, src_offset, arraynullsptr,
						 typlen, typbyval, typalign);
		if (destnullsptr)
			array_bitmap_copy(destnullsptr, dest_offset,
							  arraynullsptr, src_offset,
							  1);
		destdataptr += inc;
		srcdataptr += inc;
		src_offset++;
		dest_offset++;
	} while ((j = mda_next_tuple(ndim, indx, span)) != -1);
}

/*
 * Insert a slice into an array.
 *
 * ndim/dim[]/lb[] are dimensions of the original array.  A new array with
 * those same dimensions is to be constructed.  destArray must already
 * have been allocated and its header initialized.
 *
 * st[]/endp[] identify the slice to be replaced.  Elements within the slice
 * volume are taken from consecutive elements of the srcArray; elements
 * outside it are copied from origArray.
 *
 * We assume the caller has verified that the slice coordinates are valid.
 */
static void
array_insert_slice(ArrayType *destArray,
				   ArrayType *origArray,
				   ArrayType *srcArray,
				   int ndim,
				   int *dim,
				   int *lb,
				   int *st,
				   int *endp,
				   int typlen,
				   bool typbyval,
				   char typalign)
{
	char	   *destPtr = ARR_DATA_PTR(destArray);
	char	   *origPtr = ARR_DATA_PTR(origArray);
	char	   *srcPtr = ARR_DATA_PTR(srcArray);
	bits8	   *destBitmap = ARR_NULLBITMAP(destArray);
	bits8	   *origBitmap = ARR_NULLBITMAP(origArray);
	bits8	   *srcBitmap = ARR_NULLBITMAP(srcArray);
	int			orignitems = ArrayGetNItems(ARR_NDIM(origArray),
											ARR_DIMS(origArray));
	int			dest_offset,
				orig_offset,
				src_offset,
				prod[MAXDIM],
				span[MAXDIM],
				dist[MAXDIM],
				indx[MAXDIM];
	int			i,
				j,
				inc;

	dest_offset = ArrayGetOffset(ndim, dim, lb, st);
	/* copy items before the slice start */
	inc = array_copy(destPtr, dest_offset,
					 origPtr, 0, origBitmap,
					 typlen, typbyval, typalign);
	destPtr += inc;
	origPtr += inc;
	if (destBitmap)
		array_bitmap_copy(destBitmap, 0, origBitmap, 0, dest_offset);
	orig_offset = dest_offset;
	mda_get_prod(ndim, dim, prod);
	mda_get_range(ndim, span, st, endp);
	mda_get_offset_values(ndim, dist, prod, span);
	for (i = 0; i < ndim; i++)
		indx[i] = 0;
	src_offset = 0;
	j = ndim - 1;
	do
	{
		/* Copy/advance over elements between here and next part of slice */
		if (dist[j])
		{
			inc = array_copy(destPtr, dist[j],
							 origPtr, orig_offset, origBitmap,
							 typlen, typbyval, typalign);
			destPtr += inc;
			origPtr += inc;
			if (destBitmap)
				array_bitmap_copy(destBitmap, dest_offset,
								  origBitmap, orig_offset,
								  dist[j]);
			dest_offset += dist[j];
			orig_offset += dist[j];
		}
		/* Copy new element at this slice position */
		inc = array_copy(destPtr, 1,
						 srcPtr, src_offset, srcBitmap,
						 typlen, typbyval, typalign);
		if (destBitmap)
			array_bitmap_copy(destBitmap, dest_offset,
							  srcBitmap, src_offset,
							  1);
		destPtr += inc;
		srcPtr += inc;
		dest_offset++;
		src_offset++;
		/* Advance over old element at this slice position */
		origPtr = array_seek(origPtr, orig_offset, origBitmap, 1,
							 typlen, typbyval, typalign);
		orig_offset++;
	} while ((j = mda_next_tuple(ndim, indx, span)) != -1);

	/* don't miss any data at the end */
	array_copy(destPtr, orignitems - orig_offset,
			   origPtr, orig_offset, origBitmap,
			   typlen, typbyval, typalign);
	if (destBitmap)
		array_bitmap_copy(destBitmap, dest_offset,
						  origBitmap, orig_offset,
						  orignitems - orig_offset);
}

/* ==== VERBATIM: ReadArrayDimensions/ReadDimensionInt/ReadArrayStr/ReadArrayToken/CopyArrayEls (arrayfuncs.c lines 378..1008 @ 62d6c7d3df) ==== */
/*
 * ReadArrayDimensions
 *	 parses the array dimensions part of the input and converts the values
 *	 to internal format.
 *
 * On entry, *srcptr points to the string to parse. It is advanced to point
 * after whitespace (if any) and dimension info (if any).
 *
 * *ndim_p, dim[], and lBound[] are output variables. They are filled with the
 * number of dimensions (<= MAXDIM), the lengths of each dimension, and the
 * lower subscript bounds, respectively.  If no dimension info appears,
 * *ndim_p will be set to zero, and dim[] and lBound[] are unchanged.
 *
 * 'origStr' is the original input string, used only in error messages.
 * If *escontext points to an ErrorSaveContext, details of any error are
 * reported there.
 *
 * Result:
 *	true for success, false for failure (if escontext is provided).
 *
 * Note that dim[] and lBound[] are allocated by the caller, and must have
 * MAXDIM elements.
 */
static bool
ReadArrayDimensions(char **srcptr, int *ndim_p, int *dim, int *lBound,
					const char *origStr, Node *escontext)
{
	char	   *p = *srcptr;
	int			ndim;

	/*
	 * Dimension info takes the form of one or more [n] or [m:n] items.  This
	 * loop iterates once per dimension item.
	 */
	ndim = 0;
	for (;;)
	{
		char	   *q;
		int			ub;
		int			i;

		/*
		 * Note: we currently allow whitespace between, but not within,
		 * dimension items.
		 */
		while (scanner_isspace(*p))
			p++;
		if (*p != '[')
			break;				/* no more dimension items */
		p++;
		if (ndim >= MAXDIM)
			ereturn(escontext, false,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("number of array dimensions exceeds the maximum allowed (%d)",
							MAXDIM)));

		q = p;
		if (!ReadDimensionInt(&p, &i, origStr, escontext))
			return false;
		if (p == q)				/* no digits? */
			ereturn(escontext, false,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("malformed array literal: \"%s\"", origStr),
					 errdetail("\"[\" must introduce explicitly-specified array dimensions.")));

		if (*p == ':')
		{
			/* [m:n] format */
			lBound[ndim] = i;
			p++;
			q = p;
			if (!ReadDimensionInt(&p, &ub, origStr, escontext))
				return false;
			if (p == q)			/* no digits? */
				ereturn(escontext, false,
						(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
						 errmsg("malformed array literal: \"%s\"", origStr),
						 errdetail("Missing array dimension value.")));
		}
		else
		{
			/* [n] format */
			lBound[ndim] = 1;
			ub = i;
		}
		if (*p != ']')
			ereturn(escontext, false,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("malformed array literal: \"%s\"", origStr),
					 errdetail("Missing \"%s\" after array dimensions.",
							   "]")));
		p++;

		/*
		 * Note: we could accept ub = lb-1 to represent a zero-length
		 * dimension.  However, that would result in an empty array, for which
		 * we don't keep any dimension data, so that e.g. [1:0] and [101:100]
		 * would be equivalent.  Given the lack of field demand, there seems
		 * little point in allowing such cases.
		 */
		if (ub < lBound[ndim])
			ereturn(escontext, false,
					(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
					 errmsg("upper bound cannot be less than lower bound")));

		/* Upper bound of INT_MAX must be disallowed, cf ArrayCheckBounds() */
		if (ub == INT_MAX)
			ereturn(escontext, false,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("array upper bound is too large: %d", ub)));

		/* Compute "ub - lBound[ndim] + 1", detecting overflow */
		if (pg_sub_s32_overflow(ub, lBound[ndim], &ub) ||
			pg_add_s32_overflow(ub, 1, &ub))
			ereturn(escontext, false,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("array size exceeds the maximum allowed (%d)",
							(int) MaxArraySize)));

		dim[ndim] = ub;
		ndim++;
	}

	*srcptr = p;
	*ndim_p = ndim;
	return true;
}

/*
 * ReadDimensionInt
 *	 parse an integer, for the array dimensions
 *
 * On entry, *srcptr points to the string to parse. It is advanced past the
 * digits of the integer. If there are no digits, returns true and leaves
 * *srcptr unchanged.
 *
 * Result:
 *	true for success, false for failure (if escontext is provided).
 *  On success, the parsed integer is returned in *result.
 */
static bool
ReadDimensionInt(char **srcptr, int *result,
				 const char *origStr, Node *escontext)
{
	char	   *p = *srcptr;
	long		l;

	/* don't accept leading whitespace */
	if (!isdigit((unsigned char) *p) && *p != '-' && *p != '+')
	{
		*result = 0;
		return true;
	}

	errno = 0;
	l = strtol(p, srcptr, 10);

	if (errno == ERANGE || l > PG_INT32_MAX || l < PG_INT32_MIN)
		ereturn(escontext, false,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("array bound is out of integer range")));

	*result = (int) l;
	return true;
}

/*
 * ReadArrayStr :
 *	 parses the array string pointed to by *srcptr and converts the values
 *	 to internal format.  Determines the array dimensions as it goes.
 *
 * On entry, *srcptr points to the string to parse (it must point to a '{').
 * On successful return, it is advanced to point past the closing '}'.
 *
 * If dimensions were specified explicitly, they are passed in *ndim_p and
 * dim[].  This function will check that the array values match the specified
 * dimensions.  If dimensions were not given, caller must pass *ndim_p == 0
 * and initialize all elements of dim[] to -1.  Then this function will
 * deduce the dimensions from the structure of the input and store them in
 * *ndim_p and the dim[] array.
 *
 * Element type information:
 *	inputproc: type-specific input procedure for element datatype.
 *	typioparam, typmod: auxiliary values to pass to inputproc.
 *	typdelim: the value delimiter (type-specific).
 *	typlen, typbyval, typalign: storage parameters of element datatype.
 *
 * Outputs:
 *  *ndim_p, dim: dimensions deduced from the input structure.
 *  *nitems_p: total number of elements.
 *	*values_p[]: palloc'd array, filled with converted data values.
 *	*nulls_p[]: palloc'd array, filled with is-null markers.
 *
 * 'origStr' is the original input string, used only in error messages.
 * If *escontext points to an ErrorSaveContext, details of any error are
 * reported there.
 *
 * Result:
 *	true for success, false for failure (if escontext is provided).
 */
static bool
ReadArrayStr(char **srcptr,
			 FmgrInfo *inputproc,
			 Oid typioparam,
			 int32 typmod,
			 char typdelim,
			 int typlen,
			 bool typbyval,
			 char typalign,
			 int *ndim_p,
			 int *dim,
			 int *nitems_p,
			 Datum **values_p,
			 bool **nulls_p,
			 const char *origStr,
			 Node *escontext)
{
	int			ndim = *ndim_p;
	bool		dimensions_specified = (ndim != 0);
	int			maxitems;
	Datum	   *values;
	bool	   *nulls;
	StringInfoData elembuf;
	int			nest_level;
	int			nitems;
	bool		ndim_frozen;
	bool		expect_delim;
	int			nelems[MAXDIM];

	/* Allocate some starting output workspace; we'll enlarge as needed */
	maxitems = 16;
	values = palloc_array(Datum, maxitems);
	nulls = palloc_array(bool, maxitems);

	/* Allocate workspace to hold (string representation of) one element */
	initStringInfo(&elembuf);

	/* Loop below assumes first token is ATOK_LEVEL_START */
	Assert(**srcptr == '{');

	/* Parse tokens until we reach the matching right brace */
	nest_level = 0;
	nitems = 0;
	ndim_frozen = dimensions_specified;
	expect_delim = false;
	do
	{
		ArrayToken	tok;

		tok = ReadArrayToken(srcptr, &elembuf, typdelim, origStr, escontext);

		switch (tok)
		{
			case ATOK_LEVEL_START:
				/* Can't write left brace where delim is expected */
				if (expect_delim)
					ereturn(escontext, false,
							(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
							 errmsg("malformed array literal: \"%s\"", origStr),
							 errdetail("Unexpected \"%c\" character.", '{')));

				/* Initialize element counting in the new level */
				if (nest_level >= MAXDIM)
					ereturn(escontext, false,
							(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
							 errmsg("number of array dimensions exceeds the maximum allowed (%d)",
									MAXDIM)));

				nelems[nest_level] = 0;
				nest_level++;
				if (nest_level > ndim)
				{
					/* Can't increase ndim once it's frozen */
					if (ndim_frozen)
						goto dimension_error;
					ndim = nest_level;
				}
				break;

			case ATOK_LEVEL_END:
				/* Can't get here with nest_level == 0 */
				Assert(nest_level > 0);

				/*
				 * We allow a right brace to terminate an empty sub-array,
				 * otherwise it must occur where we expect a delimiter.
				 */
				if (nelems[nest_level - 1] > 0 && !expect_delim)
					ereturn(escontext, false,
							(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
							 errmsg("malformed array literal: \"%s\"", origStr),
							 errdetail("Unexpected \"%c\" character.",
									   '}')));
				nest_level--;
				/* Nested sub-arrays count as elements of outer level */
				if (nest_level > 0)
					nelems[nest_level - 1]++;

				/*
				 * Note: if we had dimensionality info, then dim[nest_level]
				 * is initially non-negative, and we'll check each sub-array's
				 * length against that.
				 */
				if (dim[nest_level] < 0)
				{
					/* Save length of first sub-array of this level */
					dim[nest_level] = nelems[nest_level];
				}
				else if (nelems[nest_level] != dim[nest_level])
				{
					/* Subsequent sub-arrays must have same length */
					goto dimension_error;
				}

				/*
				 * Must have a delim or another right brace following, unless
				 * we have reached nest_level 0, where this won't matter.
				 */
				expect_delim = true;
				break;

			case ATOK_DELIM:
				if (!expect_delim)
					ereturn(escontext, false,
							(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
							 errmsg("malformed array literal: \"%s\"", origStr),
							 errdetail("Unexpected \"%c\" character.",
									   typdelim)));
				expect_delim = false;
				break;

			case ATOK_ELEM:
			case ATOK_ELEM_NULL:
				/* Can't get here with nest_level == 0 */
				Assert(nest_level > 0);

				/* Disallow consecutive ELEM tokens */
				if (expect_delim)
					ereturn(escontext, false,
							(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
							 errmsg("malformed array literal: \"%s\"", origStr),
							 errdetail("Unexpected array element.")));

				/* Enlarge the values/nulls arrays if needed */
				if (nitems >= maxitems)
				{
					if (maxitems >= MaxArraySize)
						ereturn(escontext, false,
								(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
								 errmsg("array size exceeds the maximum allowed (%d)",
										(int) MaxArraySize)));
					maxitems = Min(maxitems * 2, MaxArraySize);
					values = repalloc_array(values, Datum, maxitems);
					nulls = repalloc_array(nulls, bool, maxitems);
				}

				/* Read the element's value, or check that NULL is allowed */
				if (!InputFunctionCallSafe(inputproc,
										   (tok == ATOK_ELEM_NULL) ? NULL : elembuf.data,
										   typioparam, typmod,
										   escontext,
										   &values[nitems]))
					return false;
				nulls[nitems] = (tok == ATOK_ELEM_NULL);
				nitems++;

				/*
				 * Once we have found an element, the number of dimensions can
				 * no longer increase, and subsequent elements must all be at
				 * the same nesting depth.
				 */
				ndim_frozen = true;
				if (nest_level != ndim)
					goto dimension_error;
				/* Count the new element */
				nelems[nest_level - 1]++;

				/* Must have a delim or a right brace following */
				expect_delim = true;
				break;

			case ATOK_ERROR:
				return false;
		}
	} while (nest_level > 0);

	/* Clean up and return results */
	pfree(elembuf.data);

	*ndim_p = ndim;
	*nitems_p = nitems;
	*values_p = values;
	*nulls_p = nulls;
	return true;

dimension_error:
	if (dimensions_specified)
		ereturn(escontext, false,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("malformed array literal: \"%s\"", origStr),
				 errdetail("Specified array dimensions do not match array contents.")));
	else
		ereturn(escontext, false,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("malformed array literal: \"%s\"", origStr),
				 errdetail("Multidimensional arrays must have sub-arrays with matching dimensions.")));
}

/*
 * ReadArrayToken
 *	 read one token from an array value string
 *
 * Starts scanning from *srcptr.  On non-error return, *srcptr is
 * advanced past the token.
 *
 * If the token is ATOK_ELEM, the de-escaped string is returned in elembuf.
 */
static ArrayToken
ReadArrayToken(char **srcptr, StringInfo elembuf, char typdelim,
			   const char *origStr, Node *escontext)
{
	char	   *p = *srcptr;
	int			dstlen;
	bool		has_escapes;

	resetStringInfo(elembuf);

	/* Identify token type.  Loop advances over leading whitespace. */
	for (;;)
	{
		switch (*p)
		{
			case '\0':
				goto ending_error;
			case '{':
				*srcptr = p + 1;
				return ATOK_LEVEL_START;
			case '}':
				*srcptr = p + 1;
				return ATOK_LEVEL_END;
			case '"':
				p++;
				goto quoted_element;
			default:
				if (*p == typdelim)
				{
					*srcptr = p + 1;
					return ATOK_DELIM;
				}
				if (scanner_isspace(*p))
				{
					p++;
					continue;
				}
				goto unquoted_element;
		}
	}

quoted_element:
	for (;;)
	{
		switch (*p)
		{
			case '\0':
				goto ending_error;
			case '\\':
				/* Skip backslash, copy next character as-is. */
				p++;
				if (*p == '\0')
					goto ending_error;
				appendStringInfoChar(elembuf, *p++);
				break;
			case '"':

				/*
				 * If next non-whitespace isn't typdelim or a brace, complain
				 * about incorrect quoting.  While we could leave such cases
				 * to be detected as incorrect token sequences, the resulting
				 * message wouldn't be as helpful.  (We could also give the
				 * incorrect-quoting error when next is '{', but treating that
				 * as a token sequence error seems better.)
				 */
				while (*(++p) != '\0')
				{
					if (*p == typdelim || *p == '}' || *p == '{')
					{
						*srcptr = p;
						return ATOK_ELEM;
					}
					if (!scanner_isspace(*p))
						ereturn(escontext, ATOK_ERROR,
								(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
								 errmsg("malformed array literal: \"%s\"", origStr),
								 errdetail("Incorrectly quoted array element.")));
				}
				goto ending_error;
			default:
				appendStringInfoChar(elembuf, *p++);
				break;
		}
	}

unquoted_element:

	/*
	 * We don't include trailing whitespace in the result.  dstlen tracks how
	 * much of the output string is known to not be trailing whitespace.
	 */
	dstlen = 0;
	has_escapes = false;
	for (;;)
	{
		switch (*p)
		{
			case '\0':
				goto ending_error;
			case '{':
				ereturn(escontext, ATOK_ERROR,
						(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
						 errmsg("malformed array literal: \"%s\"", origStr),
						 errdetail("Unexpected \"%c\" character.",
								   '{')));
			case '"':
				/* Must double-quote all or none of an element. */
				ereturn(escontext, ATOK_ERROR,
						(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
						 errmsg("malformed array literal: \"%s\"", origStr),
						 errdetail("Incorrectly quoted array element.")));
			case '\\':
				/* Skip backslash, copy next character as-is. */
				p++;
				if (*p == '\0')
					goto ending_error;
				appendStringInfoChar(elembuf, *p++);
				dstlen = elembuf->len;	/* treat it as non-whitespace */
				has_escapes = true;
				break;
			default:
				/* End of elem? */
				if (*p == typdelim || *p == '}')
				{
					/* hack: truncate the output string to dstlen */
					elembuf->data[dstlen] = '\0';
					elembuf->len = dstlen;
					*srcptr = p;
					/* Check if it's unquoted "NULL" */
					if (Array_nulls && !has_escapes &&
						pg_strcasecmp(elembuf->data, "NULL") == 0)
						return ATOK_ELEM_NULL;
					else
						return ATOK_ELEM;
				}
				appendStringInfoChar(elembuf, *p);
				if (!scanner_isspace(*p))
					dstlen = elembuf->len;
				p++;
				break;
		}
	}

ending_error:
	ereturn(escontext, ATOK_ERROR,
			(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
			 errmsg("malformed array literal: \"%s\"", origStr),
			 errdetail("Unexpected end of input.")));
}

/*
 * Copy data into an array object from a temporary array of Datums.
 *
 * array: array object (with header fields already filled in)
 * values: array of Datums to be copied
 * nulls: array of is-null flags (can be NULL if no nulls)
 * nitems: number of Datums to be copied
 * typbyval, typlen, typalign: info about element datatype
 * freedata: if true and element type is pass-by-ref, pfree data values
 * referenced by Datums after copying them.
 *
 * If the input data is of varlena type, the caller must have ensured that
 * the values are not toasted.  (Doing it here doesn't work since the
 * caller has already allocated space for the array...)
 */
void
CopyArrayEls(ArrayType *array,
			 Datum *values,
			 bool *nulls,
			 int nitems,
			 int typlen,
			 bool typbyval,
			 char typalign,
			 bool freedata)
{
	char	   *p = ARR_DATA_PTR(array);
	bits8	   *bitmap = ARR_NULLBITMAP(array);
	int			bitval = 0;
	int			bitmask = 1;
	int			i;

	if (typbyval)
		freedata = false;

	for (i = 0; i < nitems; i++)
	{
		if (nulls && nulls[i])
		{
			if (!bitmap)		/* shouldn't happen */
				elog(ERROR, "null array element where not supported");
			/* bitmap bit stays 0 */
		}
		else
		{
			bitval |= bitmask;
			p += ArrayCastAndSet(values[i], typlen, typbyval, typalign, p);
			if (freedata)
				pfree(DatumGetPointer(values[i]));
		}
		if (bitmap)
		{
			bitmask <<= 1;
			if (bitmask == 0x100)
			{
				*bitmap++ = bitval;
				bitval = 0;
				bitmask = 1;
			}
		}
	}

	if (bitmap && bitmask != 1)
		*bitmap = bitval;
}

/* ==== VERBATIM: construct_md_array (arrayfuncs.c lines 3493..3574 @ 62d6c7d3df) ==== */
ArrayType *
construct_md_array(Datum *elems,
				   bool *nulls,
				   int ndims,
				   int *dims,
				   int *lbs,
				   Oid elmtype, int elmlen, bool elmbyval, char elmalign)
{
	ArrayType  *result;
	bool		hasnulls;
	int32		nbytes;
	int32		dataoffset;
	int			i;
	int			nelems;

	if (ndims < 0)				/* we do allow zero-dimension arrays */
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid number of dimensions: %d", ndims)));
	if (ndims > MAXDIM)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("number of array dimensions (%d) exceeds the maximum allowed (%d)",
						ndims, MAXDIM)));

	/* This checks for overflow of the array dimensions */
	nelems = ArrayGetNItems(ndims, dims);
	ArrayCheckBounds(ndims, dims, lbs);

	/* if ndims <= 0 or any dims[i] == 0, return empty array */
	if (nelems <= 0)
		return construct_empty_array(elmtype);

	/* compute required space */
	nbytes = 0;
	hasnulls = false;
	for (i = 0; i < nelems; i++)
	{
		if (nulls && nulls[i])
		{
			hasnulls = true;
			continue;
		}
		/* make sure data is not toasted */
		if (elmlen == -1)
			elems[i] = PointerGetDatum(PG_DETOAST_DATUM(elems[i]));
		nbytes = att_addlength_datum(nbytes, elmlen, elems[i]);
		nbytes = att_align_nominal(nbytes, elmalign);
		/* check for overflow of total request */
		if (!AllocSizeIsValid(nbytes))
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("array size exceeds the maximum allowed (%d)",
							(int) MaxAllocSize)));
	}

	/* Allocate and initialize result array */
	if (hasnulls)
	{
		dataoffset = ARR_OVERHEAD_WITHNULLS(ndims, nelems);
		nbytes += dataoffset;
	}
	else
	{
		dataoffset = 0;			/* marker for no null bitmap */
		nbytes += ARR_OVERHEAD_NONULLS(ndims);
	}
	result = (ArrayType *) palloc0(nbytes);
	SET_VARSIZE(result, nbytes);
	result->ndim = ndims;
	result->dataoffset = dataoffset;
	result->elemtype = elmtype;
	memcpy(ARR_DIMS(result), dims, ndims * sizeof(int));
	memcpy(ARR_LBOUND(result), lbs, ndims * sizeof(int));

	CopyArrayEls(result,
				 elems, nulls, nelems,
				 elmlen, elmbyval, elmalign,
				 false);

	return result;
}

/* ==== VERBATIM: construct_empty_array (arrayfuncs.c lines 3579..3590 @ 62d6c7d3df) ==== */
ArrayType *
construct_empty_array(Oid elmtype)
{
	ArrayType  *result;

	result = (ArrayType *) palloc0(sizeof(ArrayType));
	SET_VARSIZE(result, sizeof(ArrayType));
	result->ndim = 0;
	result->dataoffset = 0;
	result->elemtype = elmtype;
	return result;
}

/* ==== VERBATIM: deconstruct_array (arrayfuncs.c lines 3630..3689 @ 62d6c7d3df) ==== */
void
deconstruct_array(ArrayType *array,
				  Oid elmtype,
				  int elmlen, bool elmbyval, char elmalign,
				  Datum **elemsp, bool **nullsp, int *nelemsp)
{
	Datum	   *elems;
	bool	   *nulls;
	int			nelems;
	char	   *p;
	bits8	   *bitmap;
	int			bitmask;
	int			i;

	Assert(ARR_ELEMTYPE(array) == elmtype);

	nelems = ArrayGetNItems(ARR_NDIM(array), ARR_DIMS(array));
	*elemsp = elems = (Datum *) palloc(nelems * sizeof(Datum));
	if (nullsp)
		*nullsp = nulls = (bool *) palloc0(nelems * sizeof(bool));
	else
		nulls = NULL;
	*nelemsp = nelems;

	p = ARR_DATA_PTR(array);
	bitmap = ARR_NULLBITMAP(array);
	bitmask = 1;

	for (i = 0; i < nelems; i++)
	{
		/* Get source element, checking for NULL */
		if (bitmap && (*bitmap & bitmask) == 0)
		{
			elems[i] = (Datum) 0;
			if (nulls)
				nulls[i] = true;
			else
				ereport(ERROR,
						(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
						 errmsg("null array element not allowed in this context")));
		}
		else
		{
			elems[i] = fetch_att(p, elmbyval, elmlen);
			p = att_addlength_pointer(p, elmlen, p);
			p = (char *) att_align_nominal(p, elmalign);
		}

		/* advance bitmap pointer if any */
		if (bitmap)
		{
			bitmask <<= 1;
			if (bitmask == 0x100)
			{
				bitmap++;
				bitmask = 1;
			}
		}
	}
}

/* ==== VERBATIM: array_contains_nulls (arrayfuncs.c lines 3772..3807 @ 62d6c7d3df) ==== */
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
 * array_in (arrayfuncs.c 171..376). SHIM: fmgr entry unwrapped + pinned
 * element meta — the excised region is arrayfuncs.c 180..235
 * (PG_GETARG_* + the my_extra/ArrayMetaState get_type_io_data lookup);
 * locals below are the verbatim declaration list, meta values come from
 * pg_afx_fill_meta. escontext is the NULL/hard shape. The verbatim body
 * resumes at line 237.
 */
static Datum
array_in(char *string, int elemsel, int32 typmod)
{
	Node	   *escontext = NULL;	/* SHIM: hard-error shape */
	Oid			element_type;
	int			typlen;
	bool		typbyval;
	char		typalign;
	char		typdelim;
	Oid			typioparam;
	char	   *p;
	int			nitems;
	Datum	   *values;
	bool	   *nulls;
	bool		hasnulls;
	int32		nbytes;
	int32		dataoffset;
	ArrayType  *retval;
	int			ndim,
				dim[MAXDIM],
				lBound[MAXDIM];
	ArrayMetaState my_extra_data;
	ArrayMetaState *my_extra = &my_extra_data;	/* SHIM: pinned element meta, see header */

	pg_afx_fill_meta(my_extra, elemsel);	/* SHIM */
	element_type = my_extra->element_type;
	typlen = my_extra->typlen;
	typbyval = my_extra->typbyval;
	typalign = my_extra->typalign;
	typdelim = my_extra->typdelim;
	typioparam = my_extra->typioparam;

/* ==== VERBATIM: array_in body tail (arrayfuncs.c lines 237..376 @ 62d6c7d3df) ==== */
	/*
	 * Initialize dim[] and lBound[] for ReadArrayStr, in case there is no
	 * explicit dimension info.  (If there is, ReadArrayDimensions will
	 * overwrite this.)
	 */
	for (int i = 0; i < MAXDIM; i++)
	{
		dim[i] = -1;			/* indicates "not yet known" */
		lBound[i] = 1;			/* default lower bound */
	}

	/*
	 * Start processing the input string.
	 *
	 * If the input string starts with dimension info, read and use that.
	 * Otherwise, we'll determine the dimensions during ReadArrayStr.
	 */
	p = string;
	if (!ReadArrayDimensions(&p, &ndim, dim, lBound, string, escontext))
		return (Datum) 0;

	if (ndim == 0)
	{
		/* No array dimensions, so next character should be a left brace */
		if (*p != '{')
			ereturn(escontext, (Datum) 0,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("malformed array literal: \"%s\"", string),
					 errdetail("Array value must start with \"{\" or dimension information.")));
	}
	else
	{
		/* If array dimensions are given, expect '=' operator */
		if (strncmp(p, ASSGN, strlen(ASSGN)) != 0)
			ereturn(escontext, (Datum) 0,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("malformed array literal: \"%s\"", string),
					 errdetail("Missing \"%s\" after array dimensions.",
							   ASSGN)));
		p += strlen(ASSGN);
		/* Allow whitespace after it */
		while (scanner_isspace(*p))
			p++;

		if (*p != '{')
			ereturn(escontext, (Datum) 0,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("malformed array literal: \"%s\"", string),
					 errdetail("Array contents must start with \"{\".")));
	}

	/* Parse the value part, in the curly braces: { ... } */
	if (!ReadArrayStr(&p,
					  &my_extra->proc, typioparam, typmod,
					  typdelim,
					  typlen, typbyval, typalign,
					  &ndim,
					  dim,
					  &nitems,
					  &values, &nulls,
					  string,
					  escontext))
		return (Datum) 0;

	/* only whitespace is allowed after the closing brace */
	while (*p)
	{
		if (!scanner_isspace(*p++))
			ereturn(escontext, (Datum) 0,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("malformed array literal: \"%s\"", string),
					 errdetail("Junk after closing right brace.")));
	}

	/* Empty array? */
	if (nitems == 0)
		PG_RETURN_ARRAYTYPE_P(construct_empty_array(element_type));

	/*
	 * Check for nulls, compute total data space needed
	 */
	hasnulls = false;
	nbytes = 0;
	for (int i = 0; i < nitems; i++)
	{
		if (nulls[i])
			hasnulls = true;
		else
		{
			/* let's just make sure data is not toasted */
			if (typlen == -1)
				values[i] = PointerGetDatum(PG_DETOAST_DATUM(values[i]));
			nbytes = att_addlength_datum(nbytes, typlen, values[i]);
			nbytes = att_align_nominal(nbytes, typalign);
			/* check for overflow of total request */
			if (!AllocSizeIsValid(nbytes))
				ereturn(escontext, (Datum) 0,
						(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
						 errmsg("array size exceeds the maximum allowed (%d)",
								(int) MaxAllocSize)));
		}
	}
	if (hasnulls)
	{
		dataoffset = ARR_OVERHEAD_WITHNULLS(ndim, nitems);
		nbytes += dataoffset;
	}
	else
	{
		dataoffset = 0;			/* marker for no null bitmap */
		nbytes += ARR_OVERHEAD_NONULLS(ndim);
	}

	/*
	 * Construct the final array datum
	 */
	retval = (ArrayType *) palloc0(nbytes);
	SET_VARSIZE(retval, nbytes);
	retval->ndim = ndim;
	retval->dataoffset = dataoffset;

	/*
	 * This comes from the array's pg_type.typelem (which points to the base
	 * data type's pg_type.oid) and stores system oids in user tables. This
	 * oid must be preserved by binary upgrades.
	 */
	retval->elemtype = element_type;
	memcpy(ARR_DIMS(retval), dim, ndim * sizeof(int));
	memcpy(ARR_LBOUND(retval), lBound, ndim * sizeof(int));

	CopyArrayEls(retval,
				 values, nulls, nitems,
				 typlen, typbyval, typalign,
				 true);

	pfree(values);
	pfree(nulls);

	PG_RETURN_ARRAYTYPE_P(retval);
}

/* SHIM utils/arrayaccess.h array_iter_setup/array_iter_next (33..139),
 * flat-array arms only; the else-branch bodies are verbatim. */
static inline void
array_iter_setup(array_iter *it, AnyArrayType *a)
{
	it->dataptr = ARR_DATA_PTR((ArrayType *) a);
	it->bitmapptr = ARR_NULLBITMAP((ArrayType *) a);
	it->bitmask = 1;
}

static inline Datum
array_iter_next(array_iter *it, bool *isnull, int i,
				int elmlen, bool elmbyval, char elmalign)
{
	Datum		ret;

	{
		if (it->bitmapptr && (*(it->bitmapptr) & it->bitmask) == 0)
		{
			*isnull = true;
			ret = (Datum) 0;
		}
		else
		{
			*isnull = false;
			ret = fetch_att(it->dataptr, elmbyval, elmlen);
			it->dataptr = att_addlength_pointer(it->dataptr, elmlen,
												it->dataptr);
			it->dataptr = (char *) att_align_nominal(it->dataptr, elmalign);
		}
		it->bitmask <<= 1;
		if (it->bitmask == 0x100)
		{
			if (it->bitmapptr)
				it->bitmapptr++;
			it->bitmask = 1;
		}
	}

	return ret;
}

/*
 * array_out (arrayfuncs.c 1010..1260). SHIM: fmgr entry unwrapped + pinned
 * element meta — the excised region is arrayfuncs.c 1018..1079
 * (PG_GETARG_ANY_ARRAY_P + the my_extra/ArrayMetaState get_type_io_data
 * lookup); locals below are the verbatim declaration list. The verbatim
 * body resumes at line 1081.
 */
static char *
pg_afx_array_out_core(AnyArrayType *v, int elemsel)
{
	Oid			element_type = AARR_ELEMTYPE(v);
	int			typlen;
	bool		typbyval;
	char		typalign;
	char		typdelim;
	char	   *p,
			   *tmp,
			   *retval,
			  **values,
				dims_str[(MAXDIM * 33) + 2];

	/*
	 * 33 per dim since we assume 15 digits per number + ':' +'[]'
	 *
	 * +2 allows for assignment operator + trailing null
	 */
	bool	   *needquotes,
				needdims = false;
	size_t		overall_length;
	int			nitems,
				i,
				j,
				k,
				indx[MAXDIM];
	int			ndim,
			   *dims,
			   *lb;
	array_iter	iter;
	ArrayMetaState my_extra_data;
	ArrayMetaState *my_extra = &my_extra_data;	/* SHIM: pinned element meta, see header */

	(void) element_type;
	pg_afx_fill_meta(my_extra, elemsel);	/* SHIM */
	typlen = my_extra->typlen;
	typbyval = my_extra->typbyval;
	typalign = my_extra->typalign;
	typdelim = my_extra->typdelim;

#undef PG_RETURN_CSTRING
#define PG_RETURN_CSTRING(x) return (x)	/* SHIM: core returns char* */

/* ==== VERBATIM: array_out body tail (arrayfuncs.c lines 1081..1260 @ 62d6c7d3df) ==== */
	ndim = AARR_NDIM(v);
	dims = AARR_DIMS(v);
	lb = AARR_LBOUND(v);
	nitems = ArrayGetNItems(ndim, dims);

	if (nitems == 0)
	{
		retval = pstrdup("{}");
		PG_RETURN_CSTRING(retval);
	}

	/*
	 * we will need to add explicit dimensions if any dimension has a lower
	 * bound other than one
	 */
	for (i = 0; i < ndim; i++)
	{
		if (lb[i] != 1)
		{
			needdims = true;
			break;
		}
	}

	/*
	 * Convert all values to string form, count total space needed (including
	 * any overhead such as escaping backslashes), and detect whether each
	 * item needs double quotes.
	 */
	values = (char **) palloc(nitems * sizeof(char *));
	needquotes = (bool *) palloc(nitems * sizeof(bool));
	overall_length = 0;

	array_iter_setup(&iter, v);

	for (i = 0; i < nitems; i++)
	{
		Datum		itemvalue;
		bool		isnull;
		bool		needquote;

		/* Get source element, checking for NULL */
		itemvalue = array_iter_next(&iter, &isnull, i,
									typlen, typbyval, typalign);

		if (isnull)
		{
			values[i] = pstrdup("NULL");
			overall_length += 4;
			needquote = false;
		}
		else
		{
			values[i] = OutputFunctionCall(&my_extra->proc, itemvalue);

			/* count data plus backslashes; detect chars needing quotes */
			if (values[i][0] == '\0')
				needquote = true;	/* force quotes for empty string */
			else if (pg_strcasecmp(values[i], "NULL") == 0)
				needquote = true;	/* force quotes for literal NULL */
			else
				needquote = false;

			for (tmp = values[i]; *tmp != '\0'; tmp++)
			{
				char		ch = *tmp;

				overall_length += 1;
				if (ch == '"' || ch == '\\')
				{
					needquote = true;
					overall_length += 1;
				}
				else if (ch == '{' || ch == '}' || ch == typdelim ||
						 scanner_isspace(ch))
					needquote = true;
			}
		}

		needquotes[i] = needquote;

		/* Count the pair of double quotes, if needed */
		if (needquote)
			overall_length += 2;
		/* and the comma (or other typdelim delimiter) */
		overall_length += 1;
	}

	/*
	 * The very last array element doesn't have a typdelim delimiter after it,
	 * but that's OK; that space is needed for the trailing '\0'.
	 *
	 * Now count total number of curly brace pairs in output string.
	 */
	for (i = j = 0, k = 1; i < ndim; i++)
	{
		j += k, k *= dims[i];
	}
	overall_length += 2 * j;

	/* Format explicit dimensions if required */
	dims_str[0] = '\0';
	if (needdims)
	{
		char	   *ptr = dims_str;

		for (i = 0; i < ndim; i++)
		{
			sprintf(ptr, "[%d:%d]", lb[i], lb[i] + dims[i] - 1);
			ptr += strlen(ptr);
		}
		*ptr++ = *ASSGN;
		*ptr = '\0';
		overall_length += ptr - dims_str;
	}

	/* Now construct the output string */
	retval = (char *) palloc(overall_length);
	p = retval;

#define APPENDSTR(str)	(strcpy(p, (str)), p += strlen(p))
#define APPENDCHAR(ch)	(*p++ = (ch), *p = '\0')

	if (needdims)
		APPENDSTR(dims_str);
	APPENDCHAR('{');
	for (i = 0; i < ndim; i++)
		indx[i] = 0;
	j = 0;
	k = 0;
	do
	{
		for (i = j; i < ndim - 1; i++)
			APPENDCHAR('{');

		if (needquotes[k])
		{
			APPENDCHAR('"');
			for (tmp = values[k]; *tmp; tmp++)
			{
				char		ch = *tmp;

				if (ch == '"' || ch == '\\')
					*p++ = '\\';
				*p++ = ch;
			}
			*p = '\0';
			APPENDCHAR('"');
		}
		else
			APPENDSTR(values[k]);
		pfree(values[k++]);

		for (i = ndim - 1; i >= 0; i--)
		{
			if (++(indx[i]) < dims[i])
			{
				APPENDCHAR(typdelim);
				break;
			}
			else
			{
				indx[i] = 0;
				APPENDCHAR('}');
			}
		}
		j = i;
	} while (j != -1);

#undef APPENDSTR
#undef APPENDCHAR

	/* Assert that we calculated the string length accurately */
	Assert(overall_length == (p - retval + 1));

	pfree(values);
	pfree(needquotes);

	PG_RETURN_CSTRING(retval);
}
#undef PG_RETURN_CSTRING
#define PG_RETURN_CSTRING(x) return PointerGetDatum(x)

/*
 * array_get_element (arrayfuncs.c 1786..1915; doc comment omitted, body
 * verbatim from the signature).
 */

/* ==== VERBATIM: array_get_element (arrayfuncs.c lines 1819..1915 @ 62d6c7d3df) ==== */
Datum
array_get_element(Datum arraydatum,
				  int nSubscripts,
				  int *indx,
				  int arraytyplen,
				  int elmlen,
				  bool elmbyval,
				  char elmalign,
				  bool *isNull)
{
	int			i,
				ndim,
			   *dim,
			   *lb,
				offset,
				fixedDim[1],
				fixedLb[1];
	char	   *arraydataptr,
			   *retptr;
	bits8	   *arraynullsptr;

	if (arraytyplen > 0)
	{
		/*
		 * fixed-length arrays -- these are assumed to be 1-d, 0-based
		 */
		ndim = 1;
		fixedDim[0] = arraytyplen / elmlen;
		fixedLb[0] = 0;
		dim = fixedDim;
		lb = fixedLb;
		arraydataptr = (char *) DatumGetPointer(arraydatum);
		arraynullsptr = NULL;
	}
	else if (VARATT_IS_EXTERNAL_EXPANDED(DatumGetPointer(arraydatum)))
	{
		/* expanded array: let's do this in a separate function */
		return array_get_element_expanded(arraydatum,
										  nSubscripts,
										  indx,
										  arraytyplen,
										  elmlen,
										  elmbyval,
										  elmalign,
										  isNull);
	}
	else
	{
		/* detoast array if necessary, producing normal varlena input */
		ArrayType  *array = DatumGetArrayTypeP(arraydatum);

		ndim = ARR_NDIM(array);
		dim = ARR_DIMS(array);
		lb = ARR_LBOUND(array);
		arraydataptr = ARR_DATA_PTR(array);
		arraynullsptr = ARR_NULLBITMAP(array);
	}

	/*
	 * Return NULL for invalid subscript
	 */
	if (ndim != nSubscripts || ndim <= 0 || ndim > MAXDIM)
	{
		*isNull = true;
		return (Datum) 0;
	}
	for (i = 0; i < ndim; i++)
	{
		if (indx[i] < lb[i] || indx[i] >= (dim[i] + lb[i]))
		{
			*isNull = true;
			return (Datum) 0;
		}
	}

	/*
	 * Calculate the element number
	 */
	offset = ArrayGetOffset(nSubscripts, dim, lb, indx);

	/*
	 * Check for NULL array element
	 */
	if (array_get_isnull(arraynullsptr, offset))
	{
		*isNull = true;
		return (Datum) 0;
	}

	/*
	 * OK, get the element
	 */
	*isNull = false;
	retptr = array_seek(arraydataptr, 0, arraynullsptr, offset,
						elmlen, elmbyval, elmalign);
	return ArrayCast(retptr, elmbyval, elmlen);
}

/* ==== VERBATIM: array_get_slice (arrayfuncs.c lines 2029..2166 @ 62d6c7d3df) ==== */
Datum
array_get_slice(Datum arraydatum,
				int nSubscripts,
				int *upperIndx,
				int *lowerIndx,
				bool *upperProvided,
				bool *lowerProvided,
				int arraytyplen,
				int elmlen,
				bool elmbyval,
				char elmalign)
{
	ArrayType  *array;
	ArrayType  *newarray;
	int			i,
				ndim,
			   *dim,
			   *lb,
			   *newlb;
	int			fixedDim[1],
				fixedLb[1];
	Oid			elemtype;
	char	   *arraydataptr;
	bits8	   *arraynullsptr;
	int32		dataoffset;
	int			bytes,
				span[MAXDIM];

	if (arraytyplen > 0)
	{
		/*
		 * fixed-length arrays -- currently, cannot slice these because parser
		 * labels output as being of the fixed-length array type! Code below
		 * shows how we could support it if the parser were changed to label
		 * output as a suitable varlena array type.
		 */
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("slices of fixed-length arrays not implemented")));

		/*
		 * fixed-length arrays -- these are assumed to be 1-d, 0-based
		 *
		 * XXX where would we get the correct ELEMTYPE from?
		 */
		ndim = 1;
		fixedDim[0] = arraytyplen / elmlen;
		fixedLb[0] = 0;
		dim = fixedDim;
		lb = fixedLb;
		elemtype = InvalidOid;	/* XXX */
		arraydataptr = (char *) DatumGetPointer(arraydatum);
		arraynullsptr = NULL;
	}
	else
	{
		/* detoast input array if necessary */
		array = DatumGetArrayTypeP(arraydatum);

		ndim = ARR_NDIM(array);
		dim = ARR_DIMS(array);
		lb = ARR_LBOUND(array);
		elemtype = ARR_ELEMTYPE(array);
		arraydataptr = ARR_DATA_PTR(array);
		arraynullsptr = ARR_NULLBITMAP(array);
	}

	/*
	 * Check provided subscripts.  A slice exceeding the current array limits
	 * is silently truncated to the array limits.  If we end up with an empty
	 * slice, return an empty array.
	 */
	if (ndim < nSubscripts || ndim <= 0 || ndim > MAXDIM)
		return PointerGetDatum(construct_empty_array(elemtype));

	for (i = 0; i < nSubscripts; i++)
	{
		if (!lowerProvided[i] || lowerIndx[i] < lb[i])
			lowerIndx[i] = lb[i];
		if (!upperProvided[i] || upperIndx[i] >= (dim[i] + lb[i]))
			upperIndx[i] = dim[i] + lb[i] - 1;
		if (lowerIndx[i] > upperIndx[i])
			return PointerGetDatum(construct_empty_array(elemtype));
	}
	/* fill any missing subscript positions with full array range */
	for (; i < ndim; i++)
	{
		lowerIndx[i] = lb[i];
		upperIndx[i] = dim[i] + lb[i] - 1;
		if (lowerIndx[i] > upperIndx[i])
			return PointerGetDatum(construct_empty_array(elemtype));
	}

	mda_get_range(ndim, span, lowerIndx, upperIndx);

	bytes = array_slice_size(arraydataptr, arraynullsptr,
							 ndim, dim, lb,
							 lowerIndx, upperIndx,
							 elmlen, elmbyval, elmalign);

	/*
	 * Currently, we put a null bitmap in the result if the source has one;
	 * could be smarter ...
	 */
	if (arraynullsptr)
	{
		dataoffset = ARR_OVERHEAD_WITHNULLS(ndim, ArrayGetNItems(ndim, span));
		bytes += dataoffset;
	}
	else
	{
		dataoffset = 0;			/* marker for no null bitmap */
		bytes += ARR_OVERHEAD_NONULLS(ndim);
	}

	newarray = (ArrayType *) palloc0(bytes);
	SET_VARSIZE(newarray, bytes);
	newarray->ndim = ndim;
	newarray->dataoffset = dataoffset;
	newarray->elemtype = elemtype;
	memcpy(ARR_DIMS(newarray), span, ndim * sizeof(int));

	/*
	 * Lower bounds of the new array are set to 1.  Formerly (before 7.3) we
	 * copied the given lowerIndx values ... but that seems confusing.
	 */
	newlb = ARR_LBOUND(newarray);
	for (i = 0; i < ndim; i++)
		newlb[i] = 1;

	array_extract_slice(newarray,
						ndim, dim, lb,
						arraydataptr, arraynullsptr,
						lowerIndx, upperIndx,
						elmlen, elmbyval, elmalign);

	return PointerGetDatum(newarray);
}

/* ==== VERBATIM: array_set_element (arrayfuncs.c lines 2200..2491 @ 62d6c7d3df) ==== */
Datum
array_set_element(Datum arraydatum,
				  int nSubscripts,
				  int *indx,
				  Datum dataValue,
				  bool isNull,
				  int arraytyplen,
				  int elmlen,
				  bool elmbyval,
				  char elmalign)
{
	ArrayType  *array;
	ArrayType  *newarray;
	int			i,
				ndim,
				dim[MAXDIM],
				lb[MAXDIM],
				offset;
	char	   *elt_ptr;
	bool		newhasnulls;
	bits8	   *oldnullbitmap;
	int			oldnitems,
				newnitems,
				olddatasize,
				newsize,
				olditemlen,
				newitemlen,
				overheadlen,
				oldoverheadlen,
				addedbefore,
				addedafter,
				lenbefore,
				lenafter;

	if (arraytyplen > 0)
	{
		/*
		 * fixed-length arrays -- these are assumed to be 1-d, 0-based. We
		 * cannot extend them, either.
		 */
		char	   *resultarray;

		if (nSubscripts != 1)
			ereport(ERROR,
					(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
					 errmsg("wrong number of array subscripts")));

		if (indx[0] < 0 || indx[0] >= arraytyplen / elmlen)
			ereport(ERROR,
					(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
					 errmsg("array subscript out of range")));

		if (isNull)
			ereport(ERROR,
					(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
					 errmsg("cannot assign null value to an element of a fixed-length array")));

		resultarray = (char *) palloc(arraytyplen);
		memcpy(resultarray, DatumGetPointer(arraydatum), arraytyplen);
		elt_ptr = (char *) resultarray + indx[0] * elmlen;
		ArrayCastAndSet(dataValue, elmlen, elmbyval, elmalign, elt_ptr);
		return PointerGetDatum(resultarray);
	}

	if (nSubscripts <= 0 || nSubscripts > MAXDIM)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("wrong number of array subscripts")));

	/* make sure item to be inserted is not toasted */
	if (elmlen == -1 && !isNull)
		dataValue = PointerGetDatum(PG_DETOAST_DATUM(dataValue));

	if (VARATT_IS_EXTERNAL_EXPANDED(DatumGetPointer(arraydatum)))
	{
		/* expanded array: let's do this in a separate function */
		return array_set_element_expanded(arraydatum,
										  nSubscripts,
										  indx,
										  dataValue,
										  isNull,
										  arraytyplen,
										  elmlen,
										  elmbyval,
										  elmalign);
	}

	/* detoast input array if necessary */
	array = DatumGetArrayTypeP(arraydatum);

	ndim = ARR_NDIM(array);

	/*
	 * if number of dims is zero, i.e. an empty array, create an array with
	 * nSubscripts dimensions, and set the lower bounds to the supplied
	 * subscripts
	 */
	if (ndim == 0)
	{
		Oid			elmtype = ARR_ELEMTYPE(array);

		for (i = 0; i < nSubscripts; i++)
		{
			dim[i] = 1;
			lb[i] = indx[i];
		}

		return PointerGetDatum(construct_md_array(&dataValue, &isNull,
												  nSubscripts, dim, lb,
												  elmtype,
												  elmlen, elmbyval, elmalign));
	}

	if (ndim != nSubscripts)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("wrong number of array subscripts")));

	/* copy dim/lb since we may modify them */
	memcpy(dim, ARR_DIMS(array), ndim * sizeof(int));
	memcpy(lb, ARR_LBOUND(array), ndim * sizeof(int));

	newhasnulls = (ARR_HASNULL(array) || isNull);
	addedbefore = addedafter = 0;

	/*
	 * Check subscripts.  We assume the existing subscripts passed
	 * ArrayCheckBounds, so that dim[i] + lb[i] can be computed without
	 * overflow.  But we must beware of other overflows in our calculations of
	 * new dim[] values.
	 */
	if (ndim == 1)
	{
		if (indx[0] < lb[0])
		{
			/* addedbefore = lb[0] - indx[0]; */
			/* dim[0] += addedbefore; */
			if (pg_sub_s32_overflow(lb[0], indx[0], &addedbefore) ||
				pg_add_s32_overflow(dim[0], addedbefore, &dim[0]))
				ereport(ERROR,
						(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
						 errmsg("array size exceeds the maximum allowed (%d)",
								(int) MaxArraySize)));
			lb[0] = indx[0];
			if (addedbefore > 1)
				newhasnulls = true; /* will insert nulls */
		}
		if (indx[0] >= (dim[0] + lb[0]))
		{
			/* addedafter = indx[0] - (dim[0] + lb[0]) + 1; */
			/* dim[0] += addedafter; */
			if (pg_sub_s32_overflow(indx[0], dim[0] + lb[0], &addedafter) ||
				pg_add_s32_overflow(addedafter, 1, &addedafter) ||
				pg_add_s32_overflow(dim[0], addedafter, &dim[0]))
				ereport(ERROR,
						(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
						 errmsg("array size exceeds the maximum allowed (%d)",
								(int) MaxArraySize)));
			if (addedafter > 1)
				newhasnulls = true; /* will insert nulls */
		}
	}
	else
	{
		/*
		 * XXX currently we do not support extending multi-dimensional arrays
		 * during assignment
		 */
		for (i = 0; i < ndim; i++)
		{
			if (indx[i] < lb[i] ||
				indx[i] >= (dim[i] + lb[i]))
				ereport(ERROR,
						(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
						 errmsg("array subscript out of range")));
		}
	}

	/* This checks for overflow of the array dimensions */
	newnitems = ArrayGetNItems(ndim, dim);
	ArrayCheckBounds(ndim, dim, lb);

	/*
	 * Compute sizes of items and areas to copy
	 */
	if (newhasnulls)
		overheadlen = ARR_OVERHEAD_WITHNULLS(ndim, newnitems);
	else
		overheadlen = ARR_OVERHEAD_NONULLS(ndim);
	oldnitems = ArrayGetNItems(ndim, ARR_DIMS(array));
	oldnullbitmap = ARR_NULLBITMAP(array);
	oldoverheadlen = ARR_DATA_OFFSET(array);
	olddatasize = ARR_SIZE(array) - oldoverheadlen;
	if (addedbefore)
	{
		offset = 0;
		lenbefore = 0;
		olditemlen = 0;
		lenafter = olddatasize;
	}
	else if (addedafter)
	{
		offset = oldnitems;
		lenbefore = olddatasize;
		olditemlen = 0;
		lenafter = 0;
	}
	else
	{
		offset = ArrayGetOffset(nSubscripts, dim, lb, indx);
		elt_ptr = array_seek(ARR_DATA_PTR(array), 0, oldnullbitmap, offset,
							 elmlen, elmbyval, elmalign);
		lenbefore = (int) (elt_ptr - ARR_DATA_PTR(array));
		if (array_get_isnull(oldnullbitmap, offset))
			olditemlen = 0;
		else
		{
			olditemlen = att_addlength_pointer(0, elmlen, elt_ptr);
			olditemlen = att_align_nominal(olditemlen, elmalign);
		}
		lenafter = (int) (olddatasize - lenbefore - olditemlen);
	}

	if (isNull)
		newitemlen = 0;
	else
	{
		newitemlen = att_addlength_datum(0, elmlen, dataValue);
		newitemlen = att_align_nominal(newitemlen, elmalign);
	}

	newsize = overheadlen + lenbefore + newitemlen + lenafter;

	/*
	 * OK, create the new array and fill in header/dimensions
	 */
	newarray = (ArrayType *) palloc0(newsize);
	SET_VARSIZE(newarray, newsize);
	newarray->ndim = ndim;
	newarray->dataoffset = newhasnulls ? overheadlen : 0;
	newarray->elemtype = ARR_ELEMTYPE(array);
	memcpy(ARR_DIMS(newarray), dim, ndim * sizeof(int));
	memcpy(ARR_LBOUND(newarray), lb, ndim * sizeof(int));

	/*
	 * Fill in data
	 */
	memcpy((char *) newarray + overheadlen,
		   (char *) array + oldoverheadlen,
		   lenbefore);
	if (!isNull)
		ArrayCastAndSet(dataValue, elmlen, elmbyval, elmalign,
						(char *) newarray + overheadlen + lenbefore);
	memcpy((char *) newarray + overheadlen + lenbefore + newitemlen,
		   (char *) array + oldoverheadlen + lenbefore + olditemlen,
		   lenafter);

	/*
	 * Fill in nulls bitmap if needed
	 *
	 * Note: it's possible we just replaced the last NULL with a non-NULL, and
	 * could get rid of the bitmap.  Seems not worth testing for though.
	 */
	if (newhasnulls)
	{
		bits8	   *newnullbitmap = ARR_NULLBITMAP(newarray);

		/* palloc0 above already marked any inserted positions as nulls */
		/* Fix the inserted value */
		if (addedafter)
			array_set_isnull(newnullbitmap, newnitems - 1, isNull);
		else
			array_set_isnull(newnullbitmap, offset, isNull);
		/* Fix the copied range(s) */
		if (addedbefore)
			array_bitmap_copy(newnullbitmap, addedbefore,
							  oldnullbitmap, 0,
							  oldnitems);
		else
		{
			array_bitmap_copy(newnullbitmap, 0,
							  oldnullbitmap, 0,
							  offset);
			if (addedafter == 0)
				array_bitmap_copy(newnullbitmap, offset + 1,
								  oldnullbitmap, offset + 1,
								  oldnitems - offset - 1);
		}
	}

	return PointerGetDatum(newarray);
}

/* ==== VERBATIM: array_set_slice (arrayfuncs.c lines 2805..3136 @ 62d6c7d3df) ==== */
Datum
array_set_slice(Datum arraydatum,
				int nSubscripts,
				int *upperIndx,
				int *lowerIndx,
				bool *upperProvided,
				bool *lowerProvided,
				Datum srcArrayDatum,
				bool isNull,
				int arraytyplen,
				int elmlen,
				bool elmbyval,
				char elmalign)
{
	ArrayType  *array;
	ArrayType  *srcArray;
	ArrayType  *newarray;
	int			i,
				ndim,
				dim[MAXDIM],
				lb[MAXDIM],
				span[MAXDIM];
	bool		newhasnulls;
	int			nitems,
				nsrcitems,
				olddatasize,
				newsize,
				olditemsize,
				newitemsize,
				overheadlen,
				oldoverheadlen,
				addedbefore,
				addedafter,
				lenbefore,
				lenafter,
				itemsbefore,
				itemsafter,
				nolditems;

	/* Currently, assignment from a NULL source array is a no-op */
	if (isNull)
		return arraydatum;

	if (arraytyplen > 0)
	{
		/*
		 * fixed-length arrays -- not got round to doing this...
		 */
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("updates on slices of fixed-length arrays not implemented")));
	}

	/* detoast arrays if necessary */
	array = DatumGetArrayTypeP(arraydatum);
	srcArray = DatumGetArrayTypeP(srcArrayDatum);

	/* note: we assume srcArray contains no toasted elements */

	ndim = ARR_NDIM(array);

	/*
	 * if number of dims is zero, i.e. an empty array, create an array with
	 * nSubscripts dimensions, and set the upper and lower bounds to the
	 * supplied subscripts
	 */
	if (ndim == 0)
	{
		Datum	   *dvalues;
		bool	   *dnulls;
		int			nelems;
		Oid			elmtype = ARR_ELEMTYPE(array);

		deconstruct_array(srcArray, elmtype, elmlen, elmbyval, elmalign,
						  &dvalues, &dnulls, &nelems);

		for (i = 0; i < nSubscripts; i++)
		{
			if (!upperProvided[i] || !lowerProvided[i])
				ereport(ERROR,
						(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
						 errmsg("array slice subscript must provide both boundaries"),
						 errdetail("When assigning to a slice of an empty array value,"
								   " slice boundaries must be fully specified.")));

			/* compute "upperIndx[i] - lowerIndx[i] + 1", detecting overflow */
			if (pg_sub_s32_overflow(upperIndx[i], lowerIndx[i], &dim[i]) ||
				pg_add_s32_overflow(dim[i], 1, &dim[i]))
				ereport(ERROR,
						(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
						 errmsg("array size exceeds the maximum allowed (%d)",
								(int) MaxArraySize)));

			lb[i] = lowerIndx[i];
		}

		/* complain if too few source items; we ignore extras, however */
		if (nelems < ArrayGetNItems(nSubscripts, dim))
			ereport(ERROR,
					(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
					 errmsg("source array too small")));

		return PointerGetDatum(construct_md_array(dvalues, dnulls, nSubscripts,
												  dim, lb, elmtype,
												  elmlen, elmbyval, elmalign));
	}

	if (ndim < nSubscripts || ndim <= 0 || ndim > MAXDIM)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("wrong number of array subscripts")));

	/* copy dim/lb since we may modify them */
	memcpy(dim, ARR_DIMS(array), ndim * sizeof(int));
	memcpy(lb, ARR_LBOUND(array), ndim * sizeof(int));

	newhasnulls = (ARR_HASNULL(array) || ARR_HASNULL(srcArray));
	addedbefore = addedafter = 0;

	/*
	 * Check subscripts.  We assume the existing subscripts passed
	 * ArrayCheckBounds, so that dim[i] + lb[i] can be computed without
	 * overflow.  But we must beware of other overflows in our calculations of
	 * new dim[] values.
	 */
	if (ndim == 1)
	{
		Assert(nSubscripts == 1);
		if (!lowerProvided[0])
			lowerIndx[0] = lb[0];
		if (!upperProvided[0])
			upperIndx[0] = dim[0] + lb[0] - 1;
		if (lowerIndx[0] > upperIndx[0])
			ereport(ERROR,
					(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
					 errmsg("upper bound cannot be less than lower bound")));
		if (lowerIndx[0] < lb[0])
		{
			/* addedbefore = lb[0] - lowerIndx[0]; */
			/* dim[0] += addedbefore; */
			if (pg_sub_s32_overflow(lb[0], lowerIndx[0], &addedbefore) ||
				pg_add_s32_overflow(dim[0], addedbefore, &dim[0]))
				ereport(ERROR,
						(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
						 errmsg("array size exceeds the maximum allowed (%d)",
								(int) MaxArraySize)));
			lb[0] = lowerIndx[0];
			if (addedbefore > 1)
				newhasnulls = true; /* will insert nulls */
		}
		if (upperIndx[0] >= (dim[0] + lb[0]))
		{
			/* addedafter = upperIndx[0] - (dim[0] + lb[0]) + 1; */
			/* dim[0] += addedafter; */
			if (pg_sub_s32_overflow(upperIndx[0], dim[0] + lb[0], &addedafter) ||
				pg_add_s32_overflow(addedafter, 1, &addedafter) ||
				pg_add_s32_overflow(dim[0], addedafter, &dim[0]))
				ereport(ERROR,
						(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
						 errmsg("array size exceeds the maximum allowed (%d)",
								(int) MaxArraySize)));
			if (addedafter > 1)
				newhasnulls = true; /* will insert nulls */
		}
	}
	else
	{
		/*
		 * XXX currently we do not support extending multi-dimensional arrays
		 * during assignment
		 */
		for (i = 0; i < nSubscripts; i++)
		{
			if (!lowerProvided[i])
				lowerIndx[i] = lb[i];
			if (!upperProvided[i])
				upperIndx[i] = dim[i] + lb[i] - 1;
			if (lowerIndx[i] > upperIndx[i])
				ereport(ERROR,
						(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
						 errmsg("upper bound cannot be less than lower bound")));
			if (lowerIndx[i] < lb[i] ||
				upperIndx[i] >= (dim[i] + lb[i]))
				ereport(ERROR,
						(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
						 errmsg("array subscript out of range")));
		}
		/* fill any missing subscript positions with full array range */
		for (; i < ndim; i++)
		{
			lowerIndx[i] = lb[i];
			upperIndx[i] = dim[i] + lb[i] - 1;
			if (lowerIndx[i] > upperIndx[i])
				ereport(ERROR,
						(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
						 errmsg("upper bound cannot be less than lower bound")));
		}
	}

	/* Do this mainly to check for overflow */
	nitems = ArrayGetNItems(ndim, dim);
	ArrayCheckBounds(ndim, dim, lb);

	/*
	 * Make sure source array has enough entries.  Note we ignore the shape of
	 * the source array and just read entries serially.
	 */
	mda_get_range(ndim, span, lowerIndx, upperIndx);
	nsrcitems = ArrayGetNItems(ndim, span);
	if (nsrcitems > ArrayGetNItems(ARR_NDIM(srcArray), ARR_DIMS(srcArray)))
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("source array too small")));

	/*
	 * Compute space occupied by new entries, space occupied by replaced
	 * entries, and required space for new array.
	 */
	if (newhasnulls)
		overheadlen = ARR_OVERHEAD_WITHNULLS(ndim, nitems);
	else
		overheadlen = ARR_OVERHEAD_NONULLS(ndim);
	newitemsize = array_nelems_size(ARR_DATA_PTR(srcArray), 0,
									ARR_NULLBITMAP(srcArray), nsrcitems,
									elmlen, elmbyval, elmalign);
	oldoverheadlen = ARR_DATA_OFFSET(array);
	olddatasize = ARR_SIZE(array) - oldoverheadlen;
	if (ndim > 1)
	{
		/*
		 * here we do not need to cope with extension of the array; it would
		 * be a lot more complicated if we had to do so...
		 */
		olditemsize = array_slice_size(ARR_DATA_PTR(array),
									   ARR_NULLBITMAP(array),
									   ndim, dim, lb,
									   lowerIndx, upperIndx,
									   elmlen, elmbyval, elmalign);
		lenbefore = lenafter = 0;	/* keep compiler quiet */
		itemsbefore = itemsafter = nolditems = 0;
	}
	else
	{
		/*
		 * here we must allow for possibility of slice larger than orig array
		 * and/or not adjacent to orig array subscripts
		 */
		int			oldlb = ARR_LBOUND(array)[0];
		int			oldub = oldlb + ARR_DIMS(array)[0] - 1;
		int			slicelb = Max(oldlb, lowerIndx[0]);
		int			sliceub = Min(oldub, upperIndx[0]);
		char	   *oldarraydata = ARR_DATA_PTR(array);
		bits8	   *oldarraybitmap = ARR_NULLBITMAP(array);

		/* count/size of old array entries that will go before the slice */
		itemsbefore = Min(slicelb, oldub + 1) - oldlb;
		lenbefore = array_nelems_size(oldarraydata, 0, oldarraybitmap,
									  itemsbefore,
									  elmlen, elmbyval, elmalign);
		/* count/size of old array entries that will be replaced by slice */
		if (slicelb > sliceub)
		{
			nolditems = 0;
			olditemsize = 0;
		}
		else
		{
			nolditems = sliceub - slicelb + 1;
			olditemsize = array_nelems_size(oldarraydata + lenbefore,
											itemsbefore, oldarraybitmap,
											nolditems,
											elmlen, elmbyval, elmalign);
		}
		/* count/size of old array entries that will go after the slice */
		itemsafter = oldub + 1 - Max(sliceub + 1, oldlb);
		lenafter = olddatasize - lenbefore - olditemsize;
	}

	newsize = overheadlen + olddatasize - olditemsize + newitemsize;

	newarray = (ArrayType *) palloc0(newsize);
	SET_VARSIZE(newarray, newsize);
	newarray->ndim = ndim;
	newarray->dataoffset = newhasnulls ? overheadlen : 0;
	newarray->elemtype = ARR_ELEMTYPE(array);
	memcpy(ARR_DIMS(newarray), dim, ndim * sizeof(int));
	memcpy(ARR_LBOUND(newarray), lb, ndim * sizeof(int));

	if (ndim > 1)
	{
		/*
		 * here we do not need to cope with extension of the array; it would
		 * be a lot more complicated if we had to do so...
		 */
		array_insert_slice(newarray, array, srcArray,
						   ndim, dim, lb,
						   lowerIndx, upperIndx,
						   elmlen, elmbyval, elmalign);
	}
	else
	{
		/* fill in data */
		memcpy((char *) newarray + overheadlen,
			   (char *) array + oldoverheadlen,
			   lenbefore);
		memcpy((char *) newarray + overheadlen + lenbefore,
			   ARR_DATA_PTR(srcArray),
			   newitemsize);
		memcpy((char *) newarray + overheadlen + lenbefore + newitemsize,
			   (char *) array + oldoverheadlen + lenbefore + olditemsize,
			   lenafter);
		/* fill in nulls bitmap if needed */
		if (newhasnulls)
		{
			bits8	   *newnullbitmap = ARR_NULLBITMAP(newarray);
			bits8	   *oldnullbitmap = ARR_NULLBITMAP(array);

			/* palloc0 above already marked any inserted positions as nulls */
			array_bitmap_copy(newnullbitmap, addedbefore,
							  oldnullbitmap, 0,
							  itemsbefore);
			array_bitmap_copy(newnullbitmap, lowerIndx[0] - lb[0],
							  ARR_NULLBITMAP(srcArray), 0,
							  nsrcitems);
			array_bitmap_copy(newnullbitmap, addedbefore + itemsbefore + nolditems,
							  oldnullbitmap, itemsbefore + nolditems,
							  itemsafter);
		}
	}

	return PointerGetDatum(newarray);
}

/* ==== VERBATIM: width_bucket_array_float8/_fixed/_variable (arrayfuncs.c lines 6754..6921 @ 62d6c7d3df) ==== */

/*
 * width_bucket_array for float8 data.
 */
static int
width_bucket_array_float8(Datum operand, ArrayType *thresholds)
{
	float8		op = DatumGetFloat8(operand);
	float8	   *thresholds_data;
	int			left;
	int			right;

	/*
	 * Since we know the array contains no NULLs, we can just index it
	 * directly.
	 */
	thresholds_data = (float8 *) ARR_DATA_PTR(thresholds);

	left = 0;
	right = ArrayGetNItems(ARR_NDIM(thresholds), ARR_DIMS(thresholds));

	/*
	 * If the probe value is a NaN, it's greater than or equal to all possible
	 * threshold values (including other NaNs), so we need not search.  Note
	 * that this would give the same result as searching even if the array
	 * contains multiple NaNs (as long as they're correctly sorted), since the
	 * loop logic will find the rightmost of multiple equal threshold values.
	 */
	if (isnan(op))
		return right;

	/* Find the bucket */
	while (left < right)
	{
		int			mid = (left + right) / 2;

		if (isnan(thresholds_data[mid]) || op < thresholds_data[mid])
			right = mid;
		else
			left = mid + 1;
	}

	return left;
}

/*
 * width_bucket_array for generic fixed-width data types.
 */
static int
width_bucket_array_fixed(Datum operand,
						 ArrayType *thresholds,
						 Oid collation,
						 TypeCacheEntry *typentry)
{
	LOCAL_FCINFO(locfcinfo, 2);
	char	   *thresholds_data;
	int			typlen = typentry->typlen;
	bool		typbyval = typentry->typbyval;
	int			left;
	int			right;

	/*
	 * Since we know the array contains no NULLs, we can just index it
	 * directly.
	 */
	thresholds_data = (char *) ARR_DATA_PTR(thresholds);

	InitFunctionCallInfoData(*locfcinfo, &typentry->cmp_proc_finfo, 2,
							 collation, NULL, NULL);

	/* Find the bucket */
	left = 0;
	right = ArrayGetNItems(ARR_NDIM(thresholds), ARR_DIMS(thresholds));
	while (left < right)
	{
		int			mid = (left + right) / 2;
		char	   *ptr;
		int32		cmpresult;

		ptr = thresholds_data + mid * typlen;

		locfcinfo->args[0].value = operand;
		locfcinfo->args[0].isnull = false;
		locfcinfo->args[1].value = fetch_att(ptr, typbyval, typlen);
		locfcinfo->args[1].isnull = false;

		cmpresult = DatumGetInt32(FunctionCallInvoke(locfcinfo));

		/* We don't expect comparison support functions to return null */
		Assert(!locfcinfo->isnull);

		if (cmpresult < 0)
			right = mid;
		else
			left = mid + 1;
	}

	return left;
}

/*
 * width_bucket_array for generic variable-width data types.
 */
static int
width_bucket_array_variable(Datum operand,
							ArrayType *thresholds,
							Oid collation,
							TypeCacheEntry *typentry)
{
	LOCAL_FCINFO(locfcinfo, 2);
	char	   *thresholds_data;
	int			typlen = typentry->typlen;
	bool		typbyval = typentry->typbyval;
	char		typalign = typentry->typalign;
	int			left;
	int			right;

	thresholds_data = (char *) ARR_DATA_PTR(thresholds);

	InitFunctionCallInfoData(*locfcinfo, &typentry->cmp_proc_finfo, 2,
							 collation, NULL, NULL);

	/* Find the bucket */
	left = 0;
	right = ArrayGetNItems(ARR_NDIM(thresholds), ARR_DIMS(thresholds));
	while (left < right)
	{
		int			mid = (left + right) / 2;
		char	   *ptr;
		int			i;
		int32		cmpresult;

		/* Locate mid'th array element by advancing from left element */
		ptr = thresholds_data;
		for (i = left; i < mid; i++)
		{
			ptr = att_addlength_pointer(ptr, typlen, ptr);
			ptr = (char *) att_align_nominal(ptr, typalign);
		}

		locfcinfo->args[0].value = operand;
		locfcinfo->args[0].isnull = false;
		locfcinfo->args[1].value = fetch_att(ptr, typbyval, typlen);
		locfcinfo->args[1].isnull = false;

		cmpresult = DatumGetInt32(FunctionCallInvoke(locfcinfo));

		/* We don't expect comparison support functions to return null */
		Assert(!locfcinfo->isnull);

		if (cmpresult < 0)
			right = mid;
		else
		{
			left = mid + 1;

			/*
			 * Move the thresholds pointer to match new "left" index, so we
			 * don't have to seek over those elements again.  This trick
			 * ensures we do only O(N) array indexing work, not O(N^2).
			 */
			ptr = att_addlength_pointer(ptr, typlen, ptr);
			thresholds_data = (char *) att_align_nominal(ptr, typalign);
		}
	}

	return left;
}

/*
 * width_bucket_array dispatcher (arrayfuncs.c 6684..6753). SHIM: fmgr
 * entry unwrapped (excised head = 6695..6703: PG_GETARG_* + collation);
 * the TypeCacheEntry lookup block (6720..6736) is replaced by the pinned
 * pg_afx_typentry; the input checks (6704..6714) and the typlen>0
 * fixed/variable dispatch are verbatim. Collation is unused: the
 * comparator is pinned (see pg_afx_cmp_invoke).
 */
static Datum
width_bucket_array(Datum operand, ArrayType *thresholds, int elemsel)
{
	Oid			collation = InvalidOid;	/* SHIM: comparator pinned */
	Oid			element_type = ARR_ELEMTYPE(thresholds);
	int			result;

/* ==== VERBATIM: width_bucket_array input checks (arrayfuncs.c lines 6704..6714 @ 62d6c7d3df) ==== */
	/* Check input */
	if (ARR_NDIM(thresholds) > 1)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("thresholds must be one-dimensional array")));

	if (array_contains_nulls(thresholds))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("thresholds array must not contain NULLs")));

	/* We have a dedicated implementation for float8 data */
	if (element_type == FLOAT8OID)
		result = width_bucket_array_float8(operand, thresholds);
	else
	{
		TypeCacheEntry *typentry;

		typentry = pg_afx_typentry(elemsel);	/* SHIM: pinned element meta, see header */

		/*
		 * We have separate implementation paths for fixed- and variable-width
		 * types, since indexing the array is a lot cheaper in the first case.
		 */
		if (typentry->typlen > 0)
			result = width_bucket_array_fixed(operand, thresholds,
											  collation, typentry);
		else
			result = width_bucket_array_variable(operand, thresholds,
												 collation, typentry);
	}

	/* Avoid leaking memory when handed toasted input. */
	PG_FREE_IF_COPY(thresholds, 1);

	PG_RETURN_INT32(result);
}

/* ==== VERBATIM: deconstruct_array_builtin (hardcoded meta TABLE, dual-executed) (arrayfuncs.c lines 3696..3764 @ 62d6c7d3df) ==== */
void
deconstruct_array_builtin(ArrayType *array,
						  Oid elmtype,
						  Datum **elemsp, bool **nullsp, int *nelemsp)
{
	int			elmlen;
	bool		elmbyval;
	char		elmalign;

	switch (elmtype)
	{
		case CHAROID:
			elmlen = 1;
			elmbyval = true;
			elmalign = TYPALIGN_CHAR;
			break;

		case CSTRINGOID:
			elmlen = -2;
			elmbyval = false;
			elmalign = TYPALIGN_CHAR;
			break;

		case FLOAT8OID:
			elmlen = sizeof(float8);
			elmbyval = FLOAT8PASSBYVAL;
			elmalign = TYPALIGN_DOUBLE;
			break;

		case INT2OID:
			elmlen = sizeof(int16);
			elmbyval = true;
			elmalign = TYPALIGN_SHORT;
			break;

		case INT4OID:
			elmlen = sizeof(int32);
			elmbyval = true;
			elmalign = TYPALIGN_INT;
			break;

		case OIDOID:
			elmlen = sizeof(Oid);
			elmbyval = true;
			elmalign = TYPALIGN_INT;
			break;

		case TEXTOID:
			elmlen = -1;
			elmbyval = false;
			elmalign = TYPALIGN_INT;
			break;

		case TIDOID:
			elmlen = sizeof(ItemPointerData);
			elmbyval = false;
			elmalign = TYPALIGN_SHORT;
			break;

		default:
			elog(ERROR, "type %u not supported by deconstruct_array_builtin()", elmtype);
			/* keep compiler quiet */
			elmlen = 0;
			elmbyval = false;
			elmalign = 0;
	}

	deconstruct_array(array, elmtype, elmlen, elmbyval, elmalign, elemsp, nullsp, nelemsp);
}

/* ==== VERBATIM: construct_array (1-D wrapper) (arrayfuncs.c lines 3360..3374 @ 62d6c7d3df) ==== */
ArrayType *
construct_array(Datum *elems, int nelems,
				Oid elmtype,
				int elmlen, bool elmbyval, char elmalign)
{
	int			dims[1];
	int			lbs[1];

	dims[0] = nelems;
	lbs[0] = 1;

	return construct_md_array(elems, NULL, 1, dims, lbs,
							  elmtype, elmlen, elmbyval, elmalign);
}


/* ==== VERBATIM: construct_array_builtin (hardcoded meta TABLE, 12 rows) (arrayfuncs.c lines 3380..3492 @ 62d6c7d3df) ==== */
ArrayType *
construct_array_builtin(Datum *elems, int nelems, Oid elmtype)
{
	int			elmlen;
	bool		elmbyval;
	char		elmalign;

	switch (elmtype)
	{
		case CHAROID:
			elmlen = 1;
			elmbyval = true;
			elmalign = TYPALIGN_CHAR;
			break;

		case CSTRINGOID:
			elmlen = -2;
			elmbyval = false;
			elmalign = TYPALIGN_CHAR;
			break;

		case FLOAT4OID:
			elmlen = sizeof(float4);
			elmbyval = true;
			elmalign = TYPALIGN_INT;
			break;

		case FLOAT8OID:
			elmlen = sizeof(float8);
			elmbyval = FLOAT8PASSBYVAL;
			elmalign = TYPALIGN_DOUBLE;
			break;

		case INT2OID:
			elmlen = sizeof(int16);
			elmbyval = true;
			elmalign = TYPALIGN_SHORT;
			break;

		case INT4OID:
			elmlen = sizeof(int32);
			elmbyval = true;
			elmalign = TYPALIGN_INT;
			break;

		case INT8OID:
			elmlen = sizeof(int64);
			elmbyval = FLOAT8PASSBYVAL;
			elmalign = TYPALIGN_DOUBLE;
			break;

		case NAMEOID:
			elmlen = NAMEDATALEN;
			elmbyval = false;
			elmalign = TYPALIGN_CHAR;
			break;

		case OIDOID:
		case REGTYPEOID:
			elmlen = sizeof(Oid);
			elmbyval = true;
			elmalign = TYPALIGN_INT;
			break;

		case TEXTOID:
			elmlen = -1;
			elmbyval = false;
			elmalign = TYPALIGN_INT;
			break;

		case TIDOID:
			elmlen = sizeof(ItemPointerData);
			elmbyval = false;
			elmalign = TYPALIGN_SHORT;
			break;

		case XIDOID:
			elmlen = sizeof(TransactionId);
			elmbyval = true;
			elmalign = TYPALIGN_INT;
			break;

		default:
			elog(ERROR, "type %u not supported by construct_array_builtin()", elmtype);
			/* keep compiler quiet */
			elmlen = 0;
			elmbyval = false;
			elmalign = 0;
	}

	return construct_array(elems, nelems, elmtype, elmlen, elmbyval, elmalign);
}

/*
 * construct_md_array	--- simple method for constructing an array object
 *							with arbitrary dimensions and possible NULLs
 *
 * elems: array of Datum items to become the array contents
 * nulls: array of is-null flags (can be NULL if no nulls)
 * ndims: number of dimensions
 * dims: integer array with size of each dimension
 * lbs: integer array with lower bound of each dimension
 * elmtype, elmlen, elmbyval, elmalign: info for the datatype of the items
 *
 * A palloc'd ndims-D array object is constructed and returned.  Note that
 * elem values will be copied into the object even if pass-by-ref type.
 * Also note the result will be 0-D not ndims-D if any dims[i] = 0.
 *
 * NOTE: it would be cleaner to look up the elmlen/elmbval/elmalign info
 * from the system catalogs, given the elmtype.  However, the caller is
 * in a better position to cache this info across multiple uses, or even
 * to hard-wire values if the element type is hard-wired.
 */

/* ==== VERBATIM: ArrayGetIntegerTypmods (arrayutils.c lines 227..264 @ 62d6c7d3df) ==== */
/*
 * ArrayGetIntegerTypmods: verify that argument is a 1-D cstring array,
 * and get the contents converted to integers.  Returns a palloc'd array
 * and places the length at *n.
 */
int32 *
ArrayGetIntegerTypmods(ArrayType *arr, int *n)
{
	int32	   *result;
	Datum	   *elem_values;
	int			i;

	if (ARR_ELEMTYPE(arr) != CSTRINGOID)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_ELEMENT_ERROR),
				 errmsg("typmod array must be type cstring[]")));

	if (ARR_NDIM(arr) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("typmod array must be one-dimensional")));

	if (array_contains_nulls(arr))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("typmod array must not contain nulls")));

	deconstruct_array_builtin(arr, CSTRINGOID, &elem_values, NULL, n);

	result = (int32 *) palloc(*n * sizeof(int32));

	for (i = 0; i < *n; i++)
		result[i] = pg_strtoint32(DatumGetCString(elem_values[i]));

	pfree(elem_values);

	return result;
}

/* ========== SECTION 3: fuzz-facing driver entries (NOT Postgres code) ===== */

/*
 * Every entry: arena reset FIRST (memory-context-reset model), errcode
 * cleared, setjmp armed. Returns 0 on success or the errcode class on
 * error. Out-pointers reference arena memory valid until the next
 * pg_diff_* call on this thread (the Rust driver copies eagerly).
 */
#define PG_AFX_ENTRY() \
	do { \
		pg_afx_arena_reset(); \
		pg_diff_errcode = 0; \
		if (setjmp(pg_afx_jmp)) \
			return pg_diff_errcode ? pg_diff_errcode : 99; \
	} while (0)

int
pg_diff_array_in(int elemsel, const char *str, int32 typmod,
				 unsigned char **out_img, size_t *out_len)
{
	Datum		d;
	ArrayType  *a;

	PG_AFX_ENTRY();
	d = array_in(pstrdup(str), elemsel, typmod);
	a = (ArrayType *) DatumGetPointer(d);
	*out_img = (unsigned char *) a;
	*out_len = VARSIZE(a);
	return 0;
}

int
pg_diff_array_out(int elemsel, const unsigned char *img, size_t len,
				  char **out_str)
{
	char	   *img_copy;

	PG_AFX_ENTRY();
	img_copy = palloc(len);
	memcpy(img_copy, img, len);
	*out_str = pg_afx_array_out_core((AnyArrayType *) img_copy, elemsel);
	return 0;
}

/* Pinned storage parameters for the element/slice entries (arraytyplen -1,
 * elmlen/elmbyval/elmalign from the pinned meta). */
static void
pg_afx_elem_params(int elemsel, int *elmlen, bool *elmbyval, char *elmalign)
{
	ArrayMetaState m;

	pg_afx_fill_meta(&m, elemsel);
	*elmlen = m.typlen;
	*elmbyval = m.typbyval;
	*elmalign = m.typalign;
}

/* Byref element bytes for an out-param: fixed-length byref types (name,
 * tid) have no length header, so the driver passes elmlen; varlena (-1)
 * uses VARSIZE_ANY and cstring (-2) uses strlen+1 — exactly
 * att_addlength_pointer's three cases. */
static size_t
pg_afx_byref_size(int elmlen, const unsigned char *p)
{
	if (elmlen > 0)
		return (size_t) elmlen;
	if (elmlen == -1)
		return VARSIZE_ANY(p);
	return strlen((const char *) p) + 1;
}

int
pg_diff_array_get_element(int elemsel, const unsigned char *img, size_t len,
						  int nsub, const int *indx, int arraytyplen,
						  uint64_t *out_val, unsigned char **out_ptr,
						  size_t *out_size, int *out_isnull)
{
	int			elmlen;
	bool		elmbyval;
	char		elmalign;
	char	   *img_copy;
	int			indx_copy[MAXDIM];
	Datum		r;
	bool		isnull = false;

	PG_AFX_ENTRY();
	pg_afx_elem_params(elemsel, &elmlen, &elmbyval, &elmalign);
	img_copy = palloc(len);
	memcpy(img_copy, img, len);
	memcpy(indx_copy, indx, nsub * sizeof(int));
	r = array_get_element(PointerGetDatum(img_copy), nsub, indx_copy,
						  arraytyplen, elmlen, elmbyval, elmalign, &isnull);
	*out_isnull = isnull ? 1 : 0;
	if (!isnull && !elmbyval)
	{
		*out_ptr = (unsigned char *) DatumGetPointer(r);
		*out_size = pg_afx_byref_size(elmlen,
									  (unsigned char *) DatumGetPointer(r));
		*out_val = 0;
	}
	else
	{
		*out_val = (uint64_t) r;
		*out_ptr = NULL;
		*out_size = 0;
	}
	return 0;
}

int
pg_diff_array_get_slice(int elemsel, const unsigned char *img, size_t len,
						int nsub, int *upper, int *lower,
						const unsigned char *upperProvided,
						const unsigned char *lowerProvided, int arraytyplen,
						unsigned char **out_img, size_t *out_len)
{
	int			elmlen;
	bool		elmbyval;
	char		elmalign;
	char	   *img_copy;
	bool		upb[MAXDIM],
				lob[MAXDIM];
	int			i;
	Datum		r;

	PG_AFX_ENTRY();
	pg_afx_elem_params(elemsel, &elmlen, &elmbyval, &elmalign);
	img_copy = palloc(len);
	memcpy(img_copy, img, len);
	for (i = 0; i < nsub; i++)
	{
		upb[i] = upperProvided[i] != 0;
		lob[i] = lowerProvided[i] != 0;
	}
	r = array_get_slice(PointerGetDatum(img_copy), nsub, upper, lower,
						upb, lob, arraytyplen, elmlen, elmbyval, elmalign);
	*out_img = (unsigned char *) DatumGetPointer(r);
	*out_len = VARSIZE(DatumGetPointer(r));
	return 0;
}

/*
 * Build one element Datum from driver bytes. byval types: the low elmlen
 * bytes fetched through the SAME fetch_att the pasted C bodies use, so the
 * word shape (sign/zero extension per width) is C's, never the driver's.
 * text (-1): 4B varlena. cstring (-2): NUL-terminated copy. Fixed-length
 * byref (name 64, tid 6): raw elmlen-byte blob.
 */
static Datum
pg_afx_elem_datum(int elemsel, const unsigned char *elem, size_t elem_len)
{
	int			elmlen = pg_afx_metatab[elemsel].typlen;
	bool		elmbyval = pg_afx_metatab[elemsel].typbyval;

	if (elmbyval)
	{
		unsigned char word[8] = {0};

		memcpy(word, elem, (size_t) elmlen);
		return fetch_att(word, true, elmlen);
	}
	if (elmlen == -1)
		return pg_afx_make_text((const char *) elem, elem_len);
	if (elmlen == -2)
	{
		char	   *v = palloc(elem_len + 1);

		memcpy(v, elem, elem_len);
		v[elem_len] = '\0';
		return PointerGetDatum(v);
	}
	{
		char	   *v = palloc((size_t) elmlen);

		memcpy(v, elem, (size_t) elmlen);
		return PointerGetDatum(v);
	}
}

int
pg_diff_array_set_element(int elemsel, const unsigned char *img, size_t len,
						  int nsub, const int *indx,
						  const unsigned char *elem, size_t elem_len,
						  int elem_isnull, int arraytyplen,
						  unsigned char **out_img, size_t *out_len)
{
	int			elmlen;
	bool		elmbyval;
	char		elmalign;
	char	   *img_copy;
	int			indx_copy[MAXDIM];
	Datum		dv = (Datum) 0;
	Datum		r;

	PG_AFX_ENTRY();
	pg_afx_elem_params(elemsel, &elmlen, &elmbyval, &elmalign);
	img_copy = palloc(len);
	memcpy(img_copy, img, len);
	memcpy(indx_copy, indx, nsub * sizeof(int));
	if (!elem_isnull)
		dv = pg_afx_elem_datum(elemsel, elem, elem_len);
	r = array_set_element(PointerGetDatum(img_copy), nsub, indx_copy,
						  dv, elem_isnull != 0,
						  arraytyplen, elmlen, elmbyval, elmalign);
	*out_img = (unsigned char *) DatumGetPointer(r);
	/* fixed-length container result is a bare arraytyplen-byte blob, not a
	 * varlena (C: array_set_element palloc(arraytyplen) + memcpy). */
	*out_len = arraytyplen > 0 ? (size_t) arraytyplen
		: VARSIZE(DatumGetPointer(r));
	return 0;
}

int
pg_diff_array_set_slice(int elemsel, const unsigned char *img, size_t len,
						int nsub, int *upper, int *lower,
						const unsigned char *upperProvided,
						const unsigned char *lowerProvided,
						const unsigned char *src, size_t src_len,
						int arraytyplen,
						unsigned char **out_img, size_t *out_len)
{
	int			elmlen;
	bool		elmbyval;
	char		elmalign;
	char	   *img_copy,
			   *src_copy;
	bool		upb[MAXDIM],
				lob[MAXDIM];
	int			i;
	Datum		r;

	PG_AFX_ENTRY();
	pg_afx_elem_params(elemsel, &elmlen, &elmbyval, &elmalign);
	img_copy = palloc(len);
	memcpy(img_copy, img, len);
	src_copy = palloc(src_len);
	memcpy(src_copy, src, src_len);
	for (i = 0; i < nsub; i++)
	{
		upb[i] = upperProvided[i] != 0;
		lob[i] = lowerProvided[i] != 0;
	}
	r = array_set_slice(PointerGetDatum(img_copy), nsub, upper, lower,
						upb, lob, PointerGetDatum(src_copy), false,
						arraytyplen, elmlen, elmbyval, elmalign);
	*out_img = (unsigned char *) DatumGetPointer(r);
	*out_len = VARSIZE(DatumGetPointer(r));
	return 0;
}

int
pg_diff_deconstruct_array(int elemsel, const unsigned char *img, size_t len,
						  int allow_nulls, int builtin_mode,
						  uint64_t **out_vals, unsigned char **out_nulls,
						  int *out_n)
{
	int			elmlen;
	bool		elmbyval;
	char		elmalign;
	char	   *img_copy;
	Datum	   *elems;
	bool	   *nulls = NULL;
	int			nelems;
	uint64_t   *vals;
	unsigned char *nl;
	int			i;

	PG_AFX_ENTRY();
	pg_afx_elem_params(elemsel, &elmlen, &elmbyval, &elmalign);
	img_copy = palloc(len);
	memcpy(img_copy, img, len);
	if (builtin_mode)
		/* the hardcoded-table entry: C looks the meta up itself */
		deconstruct_array_builtin((ArrayType *) img_copy,
								  pg_afx_metatab[elemsel].oid,
								  &elems, allow_nulls ? &nulls : NULL,
								  &nelems);
	else
		deconstruct_array((ArrayType *) img_copy,
						  pg_afx_metatab[elemsel].oid,
						  elmlen, elmbyval, elmalign,
						  &elems, allow_nulls ? &nulls : NULL, &nelems);
	vals = palloc(sizeof(uint64_t) * (nelems ? nelems : 1));
	nl = palloc(nelems ? nelems : 1);
	for (i = 0; i < nelems; i++)
	{
		vals[i] = (uint64_t) elems[i];
		nl[i] = (nulls && nulls[i]) ? 1 : 0;
	}
	*out_vals = vals;
	*out_nulls = nl;
	*out_n = nelems;
	return 0;
}

/*
 * elems encoding: int4 -> 4*nitems little-endian bytes (elem_lens NULL);
 * text -> elem_lens[nitems] + concatenated payloads. nulls may be NULL.
 * Covers construct_empty_array via nelems==0 / ndims==0.
 */
int
pg_diff_construct_md_array(int elemsel, const unsigned char *elem_data,
						   const int *elem_lens,
						   const unsigned char *nulls, int nitems,
						   int ndims, const int *dims, const int *lbs,
						   int wrapper_1d,
						   unsigned char **out_img, size_t *out_len)
{
	Datum	   *elems;
	bool	   *nl = NULL;
	ArrayType  *r;
	int			i;
	size_t		off = 0;
	int			dims_copy[MAXDIM],
				lbs_copy[MAXDIM];

	PG_AFX_ENTRY();
	elems = palloc(sizeof(Datum) * (nitems ? nitems : 1));
	if (nulls)
		nl = palloc(sizeof(bool) * (nitems ? nitems : 1));
	for (i = 0; i < nitems; i++)
	{
		if (nulls && nulls[i])
		{
			elems[i] = (Datum) 0;
			nl[i] = true;
			continue;
		}
		if (nl)
			nl[i] = false;
		/* uniform encoding: elem_lens[i] bytes per element, concatenated
		 * (fixed-width types get their exact width from the driver). */
		elems[i] = pg_afx_elem_datum(elemsel, elem_data + off,
									 (size_t) elem_lens[i]);
		off += (size_t) elem_lens[i];
	}
	if (ndims > 0 && ndims <= MAXDIM)
	{
		memcpy(dims_copy, dims, ndims * sizeof(int));
		memcpy(lbs_copy, lbs, ndims * sizeof(int));
	}
	if (wrapper_1d == 2)
		/* construct_array_builtin: C looks the meta up in its own 12-row
		 * table (dual-executed against construct.rs builtin_meta) */
		r = construct_array_builtin(elems, nitems,
									pg_afx_metatab[elemsel].oid);
	else if (wrapper_1d)
		/* construct_array: 1-D wrapper, nulls unsupported (C passes NULL) */
		r = construct_array(elems, nitems,
							pg_afx_metatab[elemsel].oid,
							pg_afx_metatab[elemsel].typlen,
							pg_afx_metatab[elemsel].typbyval,
							pg_afx_metatab[elemsel].typalign);
	else
		r = construct_md_array(elems, nl, ndims, dims_copy, lbs_copy,
							   pg_afx_metatab[elemsel].oid,
							   pg_afx_metatab[elemsel].typlen,
							   pg_afx_metatab[elemsel].typbyval,
							   pg_afx_metatab[elemsel].typalign);
	*out_img = (unsigned char *) r;
	*out_len = VARSIZE(r);
	return 0;
}

int
pg_diff_array_contains_nulls(const unsigned char *img, size_t len)
{
	char	   *img_copy;
	bool		r;

	PG_AFX_ENTRY();
	img_copy = palloc(len);
	memcpy(img_copy, img, len);
	r = array_contains_nulls((ArrayType *) img_copy);
	return r ? -1 : -2;			/* -1 = true, -2 = false; >0 = error class */
}

/*
 * elemsel: 0 = int4 (fixed path, operand = int4 value), 1 = float8
 * (dedicated path, operand = f64 bits), 2 = text (variable path,
 * operand = operand_payload/operand_len built into a varlena here).
 */
int
pg_diff_width_bucket_array(int elemsel, uint64_t operand_bits,
						   const unsigned char *operand_payload,
						   size_t operand_len,
						   const unsigned char *img, size_t len,
						   int *out_result)
{
	char	   *img_copy;
	Datum		operand;
	Datum		r;

	PG_AFX_ENTRY();
	img_copy = palloc(len);
	memcpy(img_copy, img, len);
	if (elemsel == 2)
		operand = pg_afx_make_text((const char *) operand_payload,
								   operand_len);
	else
		operand = (Datum) operand_bits;
	r = width_bucket_array(operand, (ArrayType *) img_copy, elemsel);
	*out_result = DatumGetInt32(r);
	return 0;
}

int
pg_diff_array_get_integer_typmods(int nelems, const char *const *strs,
								  int shape,
								  int **out_vals, int *out_n)
{
	Datum	   *elems;
	bool	   *nl;
	ArrayType  *arr;
	int32	   *r;
	int			i;
	int			dims[MAXDIM],
				lbs[MAXDIM];
	int			ndims;

	PG_AFX_ENTRY();
	elems = palloc(sizeof(Datum) * (nelems ? nelems : 1));
	nl = palloc0(sizeof(bool) * (nelems ? nelems : 1));
	for (i = 0; i < nelems; i++)
		elems[i] = CStringGetDatum(pstrdup(strs[i]));
	/* shape: 0 = normal 1-D cstring[]; 1 = wrong elemtype (int4[]);
	 * 2 = 2-D cstring[]; 3 = 1-D cstring[] with a NULL element. */
	if (shape == 2 && nelems >= 2 && nelems % 2 == 0)
	{
		ndims = 2;
		dims[0] = nelems / 2;
		dims[1] = 2;
		lbs[0] = lbs[1] = 1;
	}
	else
	{
		ndims = nelems > 0 ? 1 : 0;
		dims[0] = nelems;
		lbs[0] = 1;
	}
	if (shape == 3 && nelems > 0)
		nl[0] = true;
	arr = construct_md_array(elems, nl, ndims, dims, lbs,
							 shape == 1 ? INT4OID : CSTRINGOID,
							 -2, false, TYPALIGN_CHAR);
	{
		int			n = 0;

		r = ArrayGetIntegerTypmods(arr, &n);
		*out_vals = (int *) r;
		*out_n = n;
	}
	return 0;
}
