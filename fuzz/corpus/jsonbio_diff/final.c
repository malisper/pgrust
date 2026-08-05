/*
 * pg_array_userfuncs_io.c: vendored PostgreSQL C oracle for the array_userfuncs_diff differential
 * fuzz target (100%-coverage campaign; crate crates/backend/utils/adt/array_userfuncs).
 *
 * Assembled oracle: see SHIM INVENTORY below.
 * TODO(scaffold) paste site below must be filled with VERBATIM upstream C,
 * and every #error compile gate removed WITH its paste, before the
 * .file("csrc/pg_array_userfuncs_io.c") line in core/build.rs is uncommented. A
 * half-filled shim can therefore never silently build or link.
 *
 * Provenance (fill in as you paste; follow csrc/pg_uuid_io.c):
 *   - Vendor sections 1..N byte-for-byte from src/backend/utils/adt/array_userfuncs.c
 *     @ postgres-src 62d6c7d3df6287f1bd83199c1a746e50d31571a0
 *     (PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df); re-verify against the repo's vendored ground-truth
 *     checkout ../pgrust-fabled/vendor/postgres-src before pasting).
 *   - Functions to vendor: array_append, array_prepend, array_cat, array_position, array_position_start, array_positions, trim_array, array_reverse, array_shuffle, array_sample, array_agg_array_serialize, array_agg_array_deserialize, array_agg_array_combine.
 *   - Bodies VERBATIM except documented shims; shims are PLUMBING ONLY
 *     (isxdigit/strtoul C-locale shims, ereturn -> int sentinel, fmgr
 *     PG_FUNCTION_ARGS unwrapped to plain C signatures, palloc'd results ->
 *     caller buffers, wire triples for recv/send), NEVER logic. List every
 *     shim in this header when you paste.
 *   - palloc/palloc0/repalloc/pfree -> the TLS pointer arena below (NOT
 *     bare malloc/free): models PG's memory-context reset; error paths
 *     strand allocations otherwise. Do NOT free() arena pointers by hand.
 *
 * Errcode capture follows csrc/pg_float_io.c: the shared _Thread_local
 * pg_diff_errcode (defined there) records the errcode class; map each
 * errcode this crate's C raises to a small class constant below.
 */

#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

/* Shared TLS errcode channel (defined in csrc/pg_float_io.c). */
extern _Thread_local int pg_diff_errcode;

/* TODO(scaffold): one class constant per distinct errcode the vendored C
 * raises, e.g.:
 *   #define PG_DIFF_ERR_INVALID_TEXT 1   (22P02)
 */

/* palloc arena shim: PostgreSQL frees these via memory-context reset; the
 * oracle mirrors that with a TLS pointer arena reset at every pg_diff_*
 * dispatcher entry, so error-path longjmp/ereturn/goto exits cannot leak.
 * (Three LSan incidents of the naive palloc->malloc mapping on 2026-07-31;
 * pattern proven on proofs/p1-lanej @ 7306d300196 — copied, not re-derived.
 * Final-exec allocations stay rooted in the arena, so LSan's exit scan is
 * quiet without any manual free().) */
#define PG_DIFF_ARENA_MAX 1024
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
	/* abort-loud: freeing a pointer the arena never issued is a shim bug
	 * (double-free after reset, or a bare malloc that bypassed palloc). */
	assert(!"pfree of a pointer the arena never issued");
	abort();
}

#define palloc(n) pg_diff_palloc_impl(n)
#define palloc0(n) pg_diff_palloc0_impl(n)
#define repalloc(p, n) pg_diff_repalloc_impl((p), (n))
#define pfree(p) pg_diff_pfree_impl(p)
#include <stdbool.h>

/*
 * SHIM INVENTORY (plumbing only, never logic):
 *  - fmgr environment: FunctionCallInfoBaseData/FmgrInfo/PG_* macros so the
 *    fmgr-wrapped bodies compile VERBATIM; FunctionCall2Coll local frame.
 *  - FLAT-ARRAY FENCE: VARATT_IS_EXTERNAL_EXPANDED constant-false (pgrust
 *    has no expanded arrays); array_append/array_prepend transcribed over
 *    the flat image with VERBATIM index/error arms (see SECTION 3 header).
 *  - ereport/ereturn -> TLS errcode class + longjmp (pg_geo_io.c pattern);
 *    elog(ERROR) -> class 12; message text out of scope.
 *  - palloc family -> TLS arena (reset per driver entry).
 *  - catalog pins (environment): get_typlenbyvalalign / get_element_type /
 *    lookup_type_cache / fmgr_info_cxt pinned to int4(23)/text(25)/int8(20)
 *    and eq procs int4eq(65)/texteq(67) (texteq deterministic-collation core
 *    VERBATIM varlena.c; locale machinery pinned deterministic).
 *  - get_fn_expr_argtype -> TLS pin set per driver entry.
 *  - AggCheckCallContext -> fcinfo->context != NULL.
 *  - StringInfo/pqformat subset with upstream semantics (network byte order,
 *    enlarge doubling, read-only cursor + insufficient-data error).
 *  - pg_prng xoroshiro128** VERBATIM src/common/pg_prng.c; pg_global_prng_state
 *    is TLS; BOTH sides seeded identically per exec by the driver.
 *  - datumCopy (datum.c semantics, flat fence).
 *  - CHECK_FOR_INTERRUPTS/PG_FREE_IF_COPY/MemoryContext* -> no-ops (arena).
 *  - pgdiffau_ link-prefix renames on every vendored symbol.
 *
 * VERBATIM provenance (PostgreSQL 18.3, upstream 62d6c7d3df, re-verified
 * against ../pgrust-fabled/vendor/postgres-src):
 *  - src/backend/utils/adt/arrayutils.c: ArrayGetOffset, ArrayGetNItems(Safe),
 *    ArrayCheckBounds(Safe), mda_get_range, mda_get_prod,
 *    mda_get_offset_values, mda_next_tuple.
 *  - src/backend/utils/adt/arrayfuncs.c: CopyArrayEls, array_get_slice,
 *    array_set_element (flat path), construct_md_array, construct_empty_array,
 *    deconstruct_array, array_contains_nulls, array_create_iterator,
 *    array_iterate, array_free_iterator, array_get_isnull, array_set_isnull,
 *    ArrayCastAndSet, array_seek, array_nelems_size, array_copy,
 *    array_bitmap_copy, array_slice_size, array_extract_slice,
 *    initArrayResult(WithSize), accumArrayResult, makeArrayResult,
 *    makeMdArrayResult, initArrayResultArr, accumArrayResultArr,
 *    makeArrayResultArr, trim_array.  (arrayfuncs.c is lane p1-lanex's crate
 *    on the Rust side; the C paste here is oracle plumbing for the
 *    array_userfuncs differential — divergences inside these callees are
 *    triaged cross-crate.)
 *  - src/backend/utils/adt/array_userfuncs.c: array_cat,
 *    array_agg_array_transfn/combine/serialize/deserialize/finalfn,
 *    array_position(_start), array_position_common, array_positions,
 *    array_shuffle_n, array_shuffle, array_sample, array_reverse_n,
 *    array_reverse.
 *  - src/include/utils/array.h: ArrayType layout, ARR_* macros, MAXDIM,
 *    MaxArraySize, ArrayBuildState(Arr), ArrayMetaState.
 *  - src/backend/utils/adt/arrayfuncs.c head: ArrayIteratorData.
 *  - src/include/access/tupmacs.h: fetch_att, store_att_byval,
 *    att_align_nominal/pointer, att_addlength_pointer/datum.
 *  - src/common/pg_prng.c: xoroshiro128ss, splitmix64, pg_prng_seed,
 *    pg_prng_uint64(_range); src/port/pg_bitutils.h: pg_nextpower2_32,
 *    pg_leftmost_one_pos32/64.
 *  - src/backend/utils/adt/int.c: int4eq core; varlena.c: texteq
 *    deterministic core.
 */

/* ==================== SHIM ENVIRONMENT (NOT Postgres code) =================
 * Plumbing-only environment so the VERBATIM sections below compile
 * standalone. Every shim is listed in the file header. */

#include <setjmp.h>
#include <stdio.h>

typedef uintptr_t Datum;
typedef uint32_t Oid;
typedef uint8_t bits8;
typedef size_t Size;
typedef char *Pointer;
typedef int32_t int32;
typedef int16_t int16;
typedef int64_t int64;
typedef uint8_t uint8;
typedef uint16_t uint16;
typedef uint32_t uint32;
typedef uint64_t uint64;

#define InvalidOid ((Oid) 0)
#define AllocSizeIsValid(size) ((Size) (size) <= MaxAllocSize)
#define PointerIsValid(pointer) ((const void*)(pointer) != NULL)
#define OidIsValid(objectId)  ((bool) ((objectId) != InvalidOid))
#define UINT64CONST(x) UINT64_C(x)
#define PG_INT32_MAX INT32_MAX
#define PG_INT32_MIN INT32_MIN
#define PG_UINT32_MAX UINT32_MAX
#define Max(x, y) ((x) > (y) ? (x) : (y))
#define Min(x, y) ((x) < (y) ? (x) : (y))
#define Assert(x) assert(x)
#define AssertMacro(x) ((void) 0)
#define StaticAssertStmt(c, m) ((void) 0)
#define pg_attribute_noreturn() __attribute__((noreturn))
#define pg_attribute_unused() __attribute__((unused))
#define PG_USED_FOR_ASSERTS_ONLY pg_attribute_unused()
#define unlikely(x) __builtin_expect((x) != 0, 0)
#define likely(x) __builtin_expect((x) != 0, 1)
#define lengthof(array) (sizeof (array) / sizeof ((array)[0]))
#define CHECK_FOR_INTERRUPTS() ((void) 0)
#define INT4OID ((Oid) 23)
#define TEXTOID ((Oid) 25)
#define INT8OID ((Oid) 20)
#define VOIDOID ((Oid) 2278)

/* MAXALIGN on both target platforms (aarch64/x86_64 LP64) is 8. */
#define MAXIMUM_ALIGNOF 8
#define TYPEALIGN(ALIGNVAL,LEN)  \
	(((uintptr_t) (LEN) + ((ALIGNVAL) - 1)) & ~((uintptr_t) ((ALIGNVAL) - 1)))
#define MAXALIGN(LEN)			TYPEALIGN(MAXIMUM_ALIGNOF, (LEN))
#define SHORTALIGN(LEN)			TYPEALIGN(2, (LEN))
#define INTALIGN(LEN)			TYPEALIGN(4, (LEN))
#define DOUBLEALIGN(LEN)		TYPEALIGN(8, (LEN))
#define ALIGNOF_SHORT 2
#define ALIGNOF_INT 4
#define ALIGNOF_DOUBLE 8

/* Datum conversions (postgres.h semantics on LP64). */
#define DatumGetPointer(X) ((Pointer) (X))
#define PointerGetDatum(X) ((Datum) (X))
#define DatumGetInt32(X) ((int32) (X))
#define Int32GetDatum(X) ((Datum) (X))
#define DatumGetInt64(X) ((int64) (X))
#define Int64GetDatum(X) ((Datum) (X))
#define DatumGetBool(X) ((bool) ((X) != 0))
#define BoolGetDatum(X) ((Datum) ((X) ? 1 : 0))
#define DatumGetObjectId(X) ((Oid) (X))
#define ObjectIdGetDatum(X) ((Datum) (X))
#define DatumGetUInt32(X) ((uint32) (X))
#define UInt32GetDatum(X) ((Datum) (X))
#define DatumGetInt16(X) ((int16) (X))
#define Int16GetDatum(X) ((Datum) (X))
#define DatumGetChar(X) ((char) (X))
#define CharGetDatum(X) ((Datum) (X))

/* varatt.h subset (little-endian 4B/1B varlena headers, matching the shipped
 * datum crate's set_varsize_4b). Flat fence: no TOAST, no expanded headers —
 * VARATT_IS_EXTERNAL* are constant-false so the verbatim expanded-array
 * dispatch arm in array_set_element is dead (pgrust has no expanded arrays;
 * documented FLAT-ARRAY FENCE, proofs/arrayfuncs-hdr precedent). */
typedef struct varlena
{
	char		vl_len_[4];
	char		vl_dat[];
} varlena;
typedef struct varlena bytea;
typedef struct varlena text;
#define VARHDRSZ ((int32) sizeof(int32))
#define VARHDRSZ_SHORT 1
#define VARSIZE(PTR) ((*((const uint32 *) (PTR))) >> 2)
#define SET_VARSIZE(PTR, len) (*((uint32 *) (PTR)) = ((uint32) (len)) << 2)
#define VARDATA(PTR) (((char *) (PTR)) + VARHDRSZ)
#define VARATT_IS_1B(PTR) ((*((const uint8_t *) (PTR)) & 0x01) == 0x01)
#define VARATT_IS_1B_E(PTR) ((*((const uint8_t *) (PTR))) == 0x01)
#define VARATT_IS_4B(PTR) ((*((const uint8_t *) (PTR)) & 0x03) == 0x00)
#define VARATT_IS_4B_U(PTR) ((*((const uint8_t *) (PTR)) & 0x03) == 0x00)
#define VARATT_IS_4B_C(PTR) ((*((const uint8_t *) (PTR)) & 0x03) == 0x02)
#define VARSIZE_1B(PTR) (((*((const uint8_t *) (PTR))) >> 1) & 0x7F)
#define VARDATA_1B(PTR) (((char *) (PTR)) + 1)
#define VARATT_IS_SHORT(PTR) VARATT_IS_1B(PTR)
#define VARATT_IS_EXTERNAL(PTR) VARATT_IS_1B_E(PTR)
#define VARATT_IS_EXTERNAL_EXPANDED(PTR) (0)
#define VARATT_IS_EXTENDED(PTR) (!VARATT_IS_4B_U(PTR))
#define VARSIZE_ANY(PTR) \
	(VARATT_IS_1B(PTR) ? VARSIZE_1B(PTR) : VARSIZE(PTR))
#define VARSIZE_ANY_EXHDR(PTR) \
	(VARATT_IS_1B(PTR) ? VARSIZE_1B(PTR) - VARHDRSZ_SHORT : VARSIZE(PTR) - VARHDRSZ)
#define VARDATA_ANY(PTR) \
	(VARATT_IS_1B(PTR) ? VARDATA_1B(PTR) : VARDATA(PTR))
#define PG_DETOAST_DATUM(datum) ((struct varlena *) DatumGetPointer(datum))
#define PG_DETOAST_DATUM_COPY(datum) ((struct varlena *) DatumGetPointer(datum))
#define PG_DETOAST_DATUM_PACKED(datum) ((struct varlena *) DatumGetPointer(datum))
#define DatumGetByteaPP(X) ((bytea *) PG_DETOAST_DATUM_PACKED(X))
#define DatumGetTextPP(X) ((text *) PG_DETOAST_DATUM_PACKED(X))

/* ---- ereport shim: errcode class -> TLS channel, longjmp unwind (the
 * pg_geo_io.c pattern). Distinct small class per distinct errcode. ---- */
#define PG_DIFF_ERR_NUM_OUT_OF_RANGE 1	/* 22003 */
#define PG_DIFF_ERR_DATA_EXCEPTION 2	/* 22000 not one-dimensional */
#define PG_DIFF_ERR_DATATYPE_MISMATCH 3 /* 42804 */
#define PG_DIFF_ERR_ARRAY_SUBSCRIPT 4	/* 2202E */
#define PG_DIFF_ERR_PROGRAM_LIMIT 5		/* 54000 */
#define PG_DIFF_ERR_NULL_NOT_ALLOWED 6	/* 22004 */
#define PG_DIFF_ERR_FEATURE_NOT_SUPPORTED 7 /* 0A000 */
#define PG_DIFF_ERR_UNDEFINED_FUNCTION 8	/* 42883 */
#define PG_DIFF_ERR_INVALID_PARAMETER 9		/* 22023 */
#define PG_DIFF_ERR_ARRAY_ELEMENT 10		/* 2202E ARRAY_ELEMENT_ERROR */
#define PG_DIFF_ERR_PROTOCOL_VIOLATION 11	/* 08P01 pq insufficient/extra data */
#define PG_DIFF_ERR_ELOG 12					/* elog(ERROR, ...) XX000 class */

#define ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE PG_DIFF_ERR_NUM_OUT_OF_RANGE
#define ERRCODE_DATA_EXCEPTION PG_DIFF_ERR_DATA_EXCEPTION
#define ERRCODE_DATATYPE_MISMATCH PG_DIFF_ERR_DATATYPE_MISMATCH
#define ERRCODE_ARRAY_SUBSCRIPT_ERROR PG_DIFF_ERR_ARRAY_SUBSCRIPT
#define ERRCODE_PROGRAM_LIMIT_EXCEEDED PG_DIFF_ERR_PROGRAM_LIMIT
#define ERRCODE_NULL_VALUE_NOT_ALLOWED PG_DIFF_ERR_NULL_NOT_ALLOWED
#define ERRCODE_FEATURE_NOT_SUPPORTED PG_DIFF_ERR_FEATURE_NOT_SUPPORTED
#define ERRCODE_UNDEFINED_FUNCTION PG_DIFF_ERR_UNDEFINED_FUNCTION
#define ERRCODE_INVALID_PARAMETER_VALUE PG_DIFF_ERR_INVALID_PARAMETER
#define ERRCODE_ARRAY_ELEMENT_ERROR PG_DIFF_ERR_ARRAY_ELEMENT
#define ERRCODE_PROTOCOL_VIOLATION PG_DIFF_ERR_PROTOCOL_VIOLATION

static _Thread_local jmp_buf pg_diff_au_jmp;

static void pg_diff_au_error(void) pg_attribute_noreturn();
static void
pg_diff_au_error(void)
{
	longjmp(pg_diff_au_jmp, 1);
}

static int
pg_diff_au_errcode(int c)
{
	pg_diff_errcode = c;
	return 0;
}

#define ereport(elevel, rest) do { (void) rest; pg_diff_au_error(); } while (0)
#define errcode(c) pg_diff_au_errcode(c)
#define errmsg(...) 0
#define errmsg_plural(...) 0
#define errdetail(...) 0
#define errhint(...) 0
#define elog(elevel, ...) do { pg_diff_errcode = PG_DIFF_ERR_ELOG; pg_diff_au_error(); } while (0)
#define ERROR 21

/* format_type_be: message-plane only (message text out of scope). */
static const char *
format_type_be(Oid t)
{
	return "?";
}

/* ---- memory-context shims: everything rides the TLS arena. ---- */
typedef void *MemoryContext;
static char pg_diff_au_dummycxt;
#define CurrentMemoryContext ((MemoryContext) &pg_diff_au_dummycxt)
#define MemoryContextSwitchTo(cxt) ((MemoryContext) &pg_diff_au_dummycxt)
#define MemoryContextAlloc(cxt, sz) palloc(sz)
#define MemoryContextAllocZero(cxt, sz) palloc0(sz)
#define MemoryContextDelete(cxt) ((void) 0)
#define AllocSetContextCreate(parent, name, sizes) ((MemoryContext) &pg_diff_au_dummycxt)
#define ALLOCSET_DEFAULT_SIZES 0
#define pstrdup(s) pg_diff_au_pstrdup(s)
static char *
pg_diff_au_pstrdup(const char *s)
{
	size_t		n = strlen(s) + 1;
	char	   *p = palloc(n);

	memcpy(p, s, n);
	return p;
}

/* ---- common/int.h overflow helpers (verbatim semantics, builtin arms) ---- */
static inline bool
pg_add_s32_overflow(int32 a, int32 b, int32 *result)
{
	return __builtin_add_overflow(a, b, result);
}
static inline bool
pg_sub_s32_overflow(int32 a, int32 b, int32 *result)
{
	return __builtin_sub_overflow(a, b, result);
}
static inline bool
pg_mul_s32_overflow(int32 a, int32 b, int32 *result)
{
	return __builtin_mul_overflow(a, b, result);
}

/* ---- port/pg_bitutils.h: pg_leftmost_one_pos32 + pg_nextpower2_32
 * (verbatim, __builtin_clz arm) ---- */
static inline int
pg_leftmost_one_pos32(uint32 word)
{
	Assert(word != 0);
	return 31 - __builtin_clz(word);
}
static inline uint32
pg_nextpower2_32(uint32 num)
{
	Assert(num > 0 && num <= PG_UINT32_MAX / 2 + 1);
	if ((num & (num - 1)) == 0)
		return num;
	return ((uint32) 1) << (pg_leftmost_one_pos32(num) + 1);
}

/* ---- fmgr environment (plumbing): the PG_FUNCTION_ARGS surface the
 * verbatim fmgr-wrapped bodies below read. Layout mirrors fmgr.h
 * semantically; never byte-compared. ---- */
typedef struct FmgrInfo FmgrInfo;
typedef struct FunctionCallInfoBaseData *FunctionCallInfo;
typedef Datum (*PGFunction) (FunctionCallInfo fcinfo);

struct FmgrInfo
{
	PGFunction	fn_addr;
	Oid			fn_oid;
	short		fn_nargs;
	bool		fn_strict;
	void	   *fn_extra;
	MemoryContext fn_mcxt;
};

typedef struct NullableDatum
{
	Datum		value;
	bool		isnull;
} NullableDatum;

#define FUNC_MAX_ARGS 8
typedef struct FunctionCallInfoBaseData
{
	FmgrInfo   *flinfo;
	void	   *context;		/* non-NULL = "aggregate call" for
								 * AggCheckCallContext (plumbing) */
	Oid			fncollation;
	bool		isnull;
	short		nargs;
	NullableDatum args[FUNC_MAX_ARGS];
} FunctionCallInfoBaseData;

#define PG_FUNCTION_ARGS FunctionCallInfo fcinfo
#define PG_NARGS() (fcinfo->nargs)
#define PG_ARGISNULL(n) (fcinfo->args[n].isnull)
#define PG_GETARG_DATUM(n) (fcinfo->args[n].value)
#define PG_GETARG_INT32(n) DatumGetInt32(PG_GETARG_DATUM(n))
#define PG_GETARG_OID(n) DatumGetObjectId(PG_GETARG_DATUM(n))
#define PG_GETARG_POINTER(n) DatumGetPointer(PG_GETARG_DATUM(n))
#define PG_GETARG_BOOL(n) DatumGetBool(PG_GETARG_DATUM(n))
#define PG_GETARG_ARRAYTYPE_P(n) ((ArrayType *) DatumGetPointer(PG_GETARG_DATUM(n)))
#define PG_GETARG_BYTEA_PP(n) DatumGetByteaPP(PG_GETARG_DATUM(n))
#define PG_GET_COLLATION() (fcinfo->fncollation)
#define PG_RETURN_DATUM(x) return (x)
#define PG_RETURN_INT32(x) return Int32GetDatum(x)
#define PG_RETURN_BOOL(x) return BoolGetDatum(x)
#define PG_RETURN_POINTER(x) return PointerGetDatum(x)
#define PG_RETURN_ARRAYTYPE_P(x) PG_RETURN_POINTER(x)
#define PG_RETURN_BYTEA_P(x) PG_RETURN_POINTER(x)
#define PG_RETURN_NULL() do { fcinfo->isnull = true; return (Datum) 0; } while (0)
#define PG_FREE_IF_COPY(ptr, n) ((void) 0)

/* FunctionCall2Coll (fmgr.c semantics; local frame, plumbing only). */
static Datum
FunctionCall2Coll(FmgrInfo *flinfo, Oid collation, Datum arg1, Datum arg2)
{
	FunctionCallInfoBaseData fcdata;
	Datum		result;

	memset(&fcdata, 0, sizeof(fcdata));
	fcdata.flinfo = flinfo;
	fcdata.fncollation = collation;
	fcdata.nargs = 2;
	fcdata.args[0].value = arg1;
	fcdata.args[0].isnull = false;
	fcdata.args[1].value = arg2;
	fcdata.args[1].isnull = false;
	result = flinfo->fn_addr(&fcdata);
	if (fcdata.isnull)
	{
		pg_diff_errcode = PG_DIFF_ERR_ELOG;
		pg_diff_au_error();
	}
	return result;
}

/* AggCheckCallContext shim: "aggregate context" iff the driver armed
 * fcinfo->context; the returned MemoryContext is the arena dummy. */
static int
AggCheckCallContext(FunctionCallInfo fcinfo, MemoryContext *aggcontext)
{
	if (fcinfo->context != NULL)
	{
		if (aggcontext)
			*aggcontext = CurrentMemoryContext;
		return 1;
	}
	if (aggcontext)
		*aggcontext = NULL;
	return 0;
}

/* ---- catalog pins (environment, never computation): int4 / text / int8.
 * get_fn_expr_argtype -> a TLS pin the driver entry sets per call. ---- */
static _Thread_local Oid pg_diff_au_argtype_pin;

static Oid
get_fn_expr_argtype(FmgrInfo *flinfo, int argnum)
{
	return pg_diff_au_argtype_pin;
}

static void
get_typlenbyvalalign(Oid typid, int16 *typlen, bool *typbyval, char *typalign)
{
	switch (typid)
	{
		case INT4OID:
			*typlen = 4;
			*typbyval = true;
			*typalign = 'i';
			break;
		case TEXTOID:
			*typlen = -1;
			*typbyval = false;
			*typalign = 'i';
			break;
		case INT8OID:
			*typlen = 8;
			*typbyval = true;
			*typalign = 'd';
			break;
		default:
			/* unknown element type: loud (the driver never sends others) */
			abort();
	}
}

static Oid
get_element_type(Oid typid)
{
	switch (typid)
	{
		case 1007:				/* int4[] */
			return INT4OID;
		case 1009:				/* text[] */
			return TEXTOID;
		case 1016:				/* int8[] */
			return INT8OID;
		default:
			return InvalidOid;
	}
}

/* ---- ArrayType layout + macros: VERBATIM src/include/utils/array.h ---- */
typedef struct ArrayType
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	int			ndim;			/* # of dimensions */
	int32		dataoffset;		/* offset to data, or 0 if no bitmap */
	Oid			elemtype;		/* element type OID */
} ArrayType;

typedef struct ArrayType AnyArrayType;	/* flat fence: no expanded form */

#define MAXDIM 6
#define MaxAllocSize ((Size) 0x3fffffff)
#define MaxArraySize ((Size) (MaxAllocSize / sizeof(Datum)))

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
#define ARR_OVERHEAD_NONULLS(ndims) \
		MAXALIGN(sizeof(ArrayType) + 2 * sizeof(int) * (ndims))
#define ARR_OVERHEAD_WITHNULLS(ndims, nitems) \
		MAXALIGN(sizeof(ArrayType) + 2 * sizeof(int) * (ndims) + \
				 ((nitems) + 7) / 8)
#define ARR_DATA_OFFSET(a) \
		(ARR_HASNULL(a) ? (a)->dataoffset : ARR_OVERHEAD_NONULLS(ARR_NDIM(a)))
#define ARR_DATA_PTR(a) \
		(((char *) (a)) + ARR_DATA_OFFSET(a))
#define DatumGetArrayTypeP(X) ((ArrayType *) DatumGetPointer(X))
#define DatumGetArrayTypePCopy(X) ((ArrayType *) DatumGetPointer(X))

/* ---- build-state structs: VERBATIM src/include/utils/array.h ---- */
typedef struct ArrayBuildState
{
	MemoryContext mcontext;		/* where all the temp stuff is kept */
	Datum	   *dvalues;		/* array of accumulated Datums */
	bool	   *dnulls;			/* array of is-null flags for Datums */
	int			alen;			/* allocated length of above arrays */
	int			nelems;			/* number of valid entries in above arrays */
	Oid			element_type;	/* data type of the Datums */
	int16		typlen;			/* needed info about datatype */
	bool		typbyval;
	char		typalign;
	bool		private_cxt;	/* use private memory context */
} ArrayBuildState;

typedef struct ArrayBuildStateArr
{
	MemoryContext mcontext;		/* where all the temp stuff is kept */
	char	   *data;			/* accumulated data */
	bits8	   *nullbitmap;		/* bitmap of is-null flags, or NULL if none */
	int			abytes;			/* allocated length of "data" */
	int			nbytes;			/* number of bytes used so far */
	int			aitems;			/* allocated length of bitmap (in elements) */
	int			nitems;			/* total number of elements in result */
	int			ndims;			/* current dimensions of result */
	int			dims[MAXDIM];
	int			lbs[MAXDIM];
	Oid			array_type;		/* data type of the arrays */
	Oid			element_type;	/* data type of the array elements */
	bool		private_cxt;	/* use private memory context */
} ArrayBuildStateArr;

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

/* Working state for array_iterate(): VERBATIM arrayfuncs.c */
typedef struct ArrayIteratorData
{
	/* basic info about the array, set up during array_create_iterator() */
	ArrayType  *arr;			/* array we're iterating through */
	bits8	   *nullbitmap;		/* its null bitmap, if any */
	int			nitems;			/* total number of elements in array */
	int16		typlen;			/* element type's length */
	bool		typbyval;		/* element type's byval property */
	char		typalign;		/* element type's align property */

	/* information about the requested slice size */
	int			slice_ndim;		/* slice dimension, or 0 if not slicing */
	int			slice_len;		/* number of elements per slice */
	int		   *slice_dims;		/* slice dims array */
	int		   *slice_lbound;	/* slice lbound array */
	Datum	   *slice_values;	/* workspace of length slice_len */
	bool	   *slice_nulls;	/* workspace of length slice_len */

	/* current position information, updated on each iteration */
	char	   *data_ptr;		/* our current position in the array */
	int			current_item;	/* the item # we're at in the array */
}			ArrayIteratorData;
typedef struct ArrayIteratorData *ArrayIterator;

/* ---- access/tupmacs.h subset: VERBATIM (fetch_att / store-side helpers;
 * the att_align/addlength macros expand exactly as upstream) ---- */
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
			case sizeof(Datum):
				return *((const Datum *) T);
			default:
				elog(ERROR, "unsupported byval length: %d", attlen);
				return 0;
		}
	}
	else
		return PointerGetDatum(T);
}

#define att_align_nominal(cur_offset, attalign) \
( \
	((attalign) == 'i') ? INTALIGN(cur_offset) : \
	 (((attalign) == 'c') ? (uintptr_t) (cur_offset) : \
	  (((attalign) == 'd') ? DOUBLEALIGN(cur_offset) : \
	   ( \
		AssertMacro((attalign) == 's'), \
		SHORTALIGN(cur_offset) \
	   ))) \
)

#define att_align_pointer(cur_offset, attalign, attlen, attptr) \
( \
	((attlen) == -1 && VARATT_NOT_PAD_BYTE(attptr)) ? \
	(uintptr_t) (cur_offset) : \
	att_align_nominal(cur_offset, attalign) \
)

#define VARATT_NOT_PAD_BYTE(PTR) \
	(*((uint8 *) (PTR)) != 0)

#define att_addlength_datum(cur_offset, attlen, attdatum) \
	att_addlength_pointer(cur_offset, attlen, DatumGetPointer(attdatum))

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
		case sizeof(Datum):
			*(Datum *) T = newdatum;
			break;
		default:
			elog(ERROR, "unsupported byval length: %d", attlen);
	}
}

/* ---- StringInfo + pqformat subset (wire byte-law; semantics VERBATIM
 * stringinfo.c/pqformat.c, allocation on the arena) ---- */
typedef struct StringInfoData
{
	char	   *data;
	int			len;
	int			maxlen;
	int			cursor;
} StringInfoData;
typedef StringInfoData *StringInfo;

static void
initStringInfo(StringInfo str)
{
	int			size = 1024;

	str->data = (char *) palloc(size);
	str->maxlen = size;
	str->len = 0;
	str->data[0] = '\0';
	str->cursor = 0;
}

static void
initReadOnlyStringInfo(StringInfo str, char *data, int len)
{
	str->data = data;
	str->len = len;
	str->maxlen = 0;			/* read-only */
	str->cursor = 0;
}

static void
enlargeStringInfo(StringInfo str, int needed)
{
	int			newlen;

	if (needed < 0 || ((Size) needed) >= (MaxAllocSize - (Size) str->len))
	{
		pg_diff_errcode = PG_DIFF_ERR_PROGRAM_LIMIT;
		pg_diff_au_error();
	}
	needed += str->len + 1;
	if (needed <= str->maxlen)
		return;
	newlen = 2 * str->maxlen;
	while (needed > newlen)
		newlen = 2 * newlen;
	if (newlen > (int) MaxAllocSize)
		newlen = (int) MaxAllocSize;
	str->data = (char *) repalloc(str->data, newlen);
	str->maxlen = newlen;
}

static void
appendBinaryStringInfoNT(StringInfo str, const void *data, int datalen)
{
	enlargeStringInfo(str, datalen);
	memcpy(str->data + str->len, data, datalen);
	str->len += datalen;
}

static void
appendBinaryStringInfo(StringInfo str, const void *data, int datalen)
{
	appendBinaryStringInfoNT(str, data, datalen);
	str->data[str->len] = '\0';
}

/* pqformat.c/pqformat.h subset (network byte order = big-endian). */
static void
pq_begintypsend(StringInfo buf)
{
	initStringInfo(buf);
	appendBinaryStringInfoNT(buf, "\0\0\0\0", 4);
}

static bytea *
pq_endtypsend(StringInfo buf)
{
	bytea	   *result = (bytea *) buf->data;

	Assert(buf->len >= (int) VARHDRSZ);
	SET_VARSIZE(result, buf->len);
	return result;
}

static void
pq_sendint32(StringInfo buf, uint32 i)
{
	uint32		ni = __builtin_bswap32(i);

	appendBinaryStringInfoNT(buf, &ni, 4);
}

static void
pq_sendbytes(StringInfo buf, const void *data, int datalen)
{
	appendBinaryStringInfoNT(buf, data, datalen);
}

static void
pq_insufficient_data(void) pg_attribute_noreturn();
static void
pq_insufficient_data(void)
{
	pg_diff_errcode = PG_DIFF_ERR_PROTOCOL_VIOLATION;
	pg_diff_au_error();
}

static const char *
pq_getmsgbytes(StringInfo msg, int datalen)
{
	const char *result;

	if (datalen < 0 || datalen > (msg->len - msg->cursor))
		pq_insufficient_data();
	result = &msg->data[msg->cursor];
	msg->cursor += datalen;
	return result;
}

static uint32
pq_getmsgint(StringInfo msg, int b)
{
	uint32		result;
	uint32		n32;

	Assert(b == 4);
	if (b > (msg->len - msg->cursor))
		pq_insufficient_data();
	memcpy(&n32, &msg->data[msg->cursor], 4);
	msg->cursor += 4;
	result = __builtin_bswap32(n32);
	return result;
}

static void
pq_getmsgend(StringInfo msg)
{
	if (msg->cursor != msg->len)
		pq_insufficient_data();
}

/* ---- pinned comparators: the eq procs lookup_type_cache would resolve.
 * int4eq VERBATIM int.c; texteq core VERBATIM varlena.c under the
 * deterministic-collation arm (collation pinned C/deterministic on both
 * sides; locale machinery shimmed to a constant deterministic locale). ---- */
#define PG_GETARG_INT32_EQ(n) DatumGetInt32(fcinfo->args[n].value)

static Datum
pg_diff_int4eq(PG_FUNCTION_ARGS)
{
	int32		arg1 = PG_GETARG_INT32(0);
	int32		arg2 = PG_GETARG_INT32(1);

	PG_RETURN_BOOL(arg1 == arg2);
}

static Datum
pg_diff_texteq(PG_FUNCTION_ARGS)
{
	bool		result;
	Datum		arg1 = PG_GETARG_DATUM(0);
	Datum		arg2 = PG_GETARG_DATUM(1);
	Size		len1,
				len2;

	/* deterministic-collation arm of varlena.c texteq, VERBATIM core */
	len1 = VARSIZE_ANY_EXHDR(DatumGetPointer(arg1)) + VARHDRSZ;
	len2 = VARSIZE_ANY_EXHDR(DatumGetPointer(arg2)) + VARHDRSZ;
	if (len1 != len2)
		result = false;
	else
	{
		text	   *targ1 = (text *) DatumGetPointer(arg1);
		text	   *targ2 = (text *) DatumGetPointer(arg2);

		result = (memcmp(VARDATA_ANY(targ1), VARDATA_ANY(targ2),
						 len1 - VARHDRSZ) == 0);
	}
	PG_RETURN_BOOL(result);
}

/* lookup_type_cache: static per-type entries (typcache pin; eq_opr_finfo
 * carries the pinned comparator; only the fields the vendored bodies read). */
typedef struct TypeCacheEntry
{
	Oid			type_id;
	int16		typlen;
	bool		typbyval;
	char		typalign;
	FmgrInfo	eq_opr_finfo;
} TypeCacheEntry;

#define TYPECACHE_EQ_OPR_FINFO 0x0008

static TypeCacheEntry *
lookup_type_cache(Oid type_id, int flags)
{
	static _Thread_local TypeCacheEntry ent;

	memset(&ent, 0, sizeof(ent));
	ent.type_id = type_id;
	get_typlenbyvalalign(type_id, &ent.typlen, &ent.typbyval, &ent.typalign);
	switch (type_id)
	{
		case INT4OID:
			ent.eq_opr_finfo.fn_oid = 65;	/* int4eq */
			ent.eq_opr_finfo.fn_addr = pg_diff_int4eq;
			break;
		case TEXTOID:
			ent.eq_opr_finfo.fn_oid = 67;	/* texteq */
			ent.eq_opr_finfo.fn_addr = pg_diff_texteq;
			break;
		default:
			ent.eq_opr_finfo.fn_oid = InvalidOid;
			break;
	}
	ent.eq_opr_finfo.fn_nargs = 2;
	ent.eq_opr_finfo.fn_strict = true;
	return &ent;
}

static void
fmgr_info_cxt(Oid functionId, FmgrInfo *finfo, MemoryContext mcxt)
{
	memset(finfo, 0, sizeof(*finfo));
	finfo->fn_oid = functionId;
	finfo->fn_nargs = 2;
	finfo->fn_strict = true;
	switch (functionId)
	{
		case 65:
			finfo->fn_addr = pg_diff_int4eq;
			break;
		case 67:
			finfo->fn_addr = pg_diff_texteq;
			break;
		default:
			abort();
	}
}

/* ---- ereturn (elog.h soft-error): Safe variants take Node *escontext;
 * NULL context = hard ereport (the only mode the drivers use is NULL or a
 * flag capture; errcode lands on the TLS channel either way). ---- */
typedef struct Node Node;
static _Thread_local bool pg_diff_au_soft_error;
#define ereturn(context, dummy_value, rest) \
	do { \
		(void) rest; \
		if ((context) != NULL) \
		{ \
			pg_diff_au_soft_error = true; \
			return dummy_value; \
		} \
		pg_diff_au_error(); \
	} while (0)

/* ---- datum.c datumCopy (flat fence: no expanded objects) ---- */
static Datum
datumCopy(Datum value, bool typByVal, int typLen)
{
	Datum		res;

	if (typByVal)
		res = value;
	else if (typLen == -1)
	{
		struct varlena *vl = (struct varlena *) DatumGetPointer(value);
		Size		realSize = (Size) VARSIZE_ANY(vl);
		char	   *resultptr = (char *) palloc(realSize);

		memcpy(resultptr, vl, realSize);
		res = PointerGetDatum(resultptr);
	}
	else
	{
		Size		realSize;
		char	   *resultptr;

		Assert(typLen > 0);
		realSize = (Size) typLen;
		resultptr = (char *) palloc(realSize);
		memcpy(resultptr, DatumGetPointer(value), realSize);
		res = PointerGetDatum(resultptr);
	}
	return res;
}

/* ---- common/pg_prng.c: VERBATIM xoroshiro128** core (both sides seeded
 * identically per exec by the driver; pg_global_prng_state is TLS like the
 * shipped crate's thread-local) ---- */
typedef struct pg_prng_state
{
	uint64		s0,
				s1;
} pg_prng_state;

static _Thread_local pg_prng_state pg_global_prng_state;

static inline uint64
rotl(uint64 x, int bits)
{
	return (x << bits) | (x >> (64 - bits));
}

static inline int
pg_leftmost_one_pos64(uint64 word)
{
	Assert(word != 0);
	return 63 - __builtin_clzll(word);
}

static bool
pg_prng_seed_check(pg_prng_state *state)
{
	/*
	 * If the seeding mechanism chanced to produce all-zeroes, insert
	 * something nonzero.  Anything would do; use Knuth's LCG parameters.
	 */
	if (unlikely(state->s0 == 0 && state->s1 == 0))
	{
		state->s0 = UINT64CONST(0x5851F42D4C957F2D);
		state->s1 = UINT64CONST(0x14057B7EF767814F);
	}
	return true;
}
/*
 * The basic xoroshiro128** algorithm.
 * Generates and returns a 64-bit uniformly distributed number,
 * updating the state vector for next time.
 *
 * Note: the state vector must not be all-zeroes, as that is a fixed point.
 */
static uint64
xoroshiro128ss(pg_prng_state *state)
{
	uint64		s0 = state->s0,
				sx = state->s1 ^ s0,
				val = rotl(s0 * 5, 7) * 9;

	/* update state */
	state->s0 = rotl(s0, 24) ^ sx ^ (sx << 16);
	state->s1 = rotl(sx, 37);

	return val;
}

/*
 * We use this generator just to fill the xoroshiro128** state vector
 * from a 64-bit seed.
 */
static uint64
splitmix64(uint64 *state)
{
	/* state update */
	uint64		val = (*state += UINT64CONST(0x9E3779B97f4A7C15));

	/* value extraction */
	val = (val ^ (val >> 30)) * UINT64CONST(0xBF58476D1CE4E5B9);
	val = (val ^ (val >> 27)) * UINT64CONST(0x94D049BB133111EB);

	return val ^ (val >> 31);
}

/*
 * Initialize the PRNG state from a 64-bit integer,
 * taking care that we don't produce all-zeroes.
 */
static void
pg_prng_seed(pg_prng_state *state, uint64 seed)
{
	state->s0 = splitmix64(&seed);
	state->s1 = splitmix64(&seed);
	/* Let's just make sure we didn't get all-zeroes */
	(void) pg_prng_seed_check(state);
}

/*
 * Select a random uint64 uniformly from the range [0, PG_UINT64_MAX].
 */
static uint64
pg_prng_uint64(pg_prng_state *state)
{
	return xoroshiro128ss(state);
}

/*
 * Select a random uint64 uniformly from the range [rmin, rmax].
 * If the range is empty, rmin is always produced.
 */
static uint64
pg_prng_uint64_range(pg_prng_state *state, uint64 rmin, uint64 rmax)
{
	uint64		val;

	if (likely(rmax > rmin))
	{
		/*
		 * Use bitmask rejection method to generate an offset in 0..range.
		 * Each generated val is less than twice "range", so on average we
		 * should not have to iterate more than twice.
		 */
		uint64		range = rmax - rmin;
		uint32		rshift = 63 - pg_leftmost_one_pos64(range);

		do
		{
			val = xoroshiro128ss(state) >> rshift;
		} while (val > range);
	}
	else
		val = 0;

	return rmin + val;
}

/* ---- forward declarations for the VERBATIM sections below (paste order is
 * upstream file order, not dependency order) + pgdiffau_ link-prefix renames
 * (this TU exports only pg_diff_* driver symbols; everything vendored is
 * renamed to avoid cross-oracle-TU clashes) ---- */
#define ArrayGetOffset pgdiffau_ArrayGetOffset
#define ArrayGetNItemsSafe pgdiffau_ArrayGetNItemsSafe
#define ArrayGetNItems pgdiffau_ArrayGetNItems
#define ArrayCheckBoundsSafe pgdiffau_ArrayCheckBoundsSafe
#define ArrayCheckBounds pgdiffau_ArrayCheckBounds
#define mda_get_range pgdiffau_mda_get_range
#define mda_get_prod pgdiffau_mda_get_prod
#define mda_get_offset_values pgdiffau_mda_get_offset_values
#define mda_next_tuple pgdiffau_mda_next_tuple
#define CopyArrayEls pgdiffau_CopyArrayEls
#define array_get_slice pgdiffau_array_get_slice
#define array_set_element pgdiffau_array_set_element
#define construct_md_array pgdiffau_construct_md_array
#define construct_empty_array pgdiffau_construct_empty_array
#define deconstruct_array pgdiffau_deconstruct_array
#define array_contains_nulls pgdiffau_array_contains_nulls
#define array_create_iterator pgdiffau_array_create_iterator
#define array_iterate pgdiffau_array_iterate
#define array_free_iterator pgdiffau_array_free_iterator
#define array_bitmap_copy pgdiffau_array_bitmap_copy
#define initArrayResult pgdiffau_initArrayResult
#define initArrayResultWithSize pgdiffau_initArrayResultWithSize
#define accumArrayResult pgdiffau_accumArrayResult
#define makeArrayResult pgdiffau_makeArrayResult
#define makeMdArrayResult pgdiffau_makeMdArrayResult
#define initArrayResultArr pgdiffau_initArrayResultArr
#define accumArrayResultArr pgdiffau_accumArrayResultArr
#define makeArrayResultArr pgdiffau_makeArrayResultArr
#define trim_array pgdiffau_trim_array
#define array_cat pgdiffau_array_cat
#define array_agg_array_transfn pgdiffau_array_agg_array_transfn
#define array_agg_array_combine pgdiffau_array_agg_array_combine
#define array_agg_array_serialize pgdiffau_array_agg_array_serialize
#define array_agg_array_deserialize pgdiffau_array_agg_array_deserialize
#define array_agg_array_finalfn pgdiffau_array_agg_array_finalfn
#define array_position pgdiffau_array_position
#define array_position_start pgdiffau_array_position_start
#define array_positions pgdiffau_array_positions
#define array_shuffle pgdiffau_array_shuffle
#define array_sample pgdiffau_array_sample
#define array_reverse pgdiffau_array_reverse

int			ArrayGetOffset(int n, const int *dim, const int *lb, const int *indx);
int			ArrayGetNItemsSafe(int ndim, const int *dims, Node *escontext);
int			ArrayGetNItems(int ndim, const int *dims);
bool		ArrayCheckBoundsSafe(int ndim, const int *dims, const int *lb, Node *escontext);
void		ArrayCheckBounds(int ndim, const int *dims, const int *lb);
void		mda_get_range(int n, int *span, const int *st, const int *endp);
void		mda_get_prod(int n, const int *range, int *prod);
void		mda_get_offset_values(int n, int *dist, const int *prod, const int *span);
int			mda_next_tuple(int n, int *curr, const int *span);
void		CopyArrayEls(ArrayType *array, Datum *values, bool *nulls, int nitems,
						 int typlen, bool typbyval, char typalign, bool freedata);
Datum		array_get_slice(Datum arraydatum, int nSubscripts, int *upperIndx,
							int *lowerIndx, bool *upperProvided, bool *lowerProvided,
							int arraytyplen, int elmlen, bool elmbyval, char elmalign);
Datum		array_set_element(Datum arraydatum, int nSubscripts, int *indx,
							  Datum dataValue, bool isNull, int arraytyplen,
							  int elmlen, bool elmbyval, char elmalign);
ArrayType  *construct_md_array(Datum *elems, bool *nulls, int ndims, int *dims,
							   int *lbs, Oid elmtype, int elmlen, bool elmbyval,
							   char elmalign);
ArrayType  *construct_empty_array(Oid elmtype);
void		deconstruct_array(ArrayType *array, Oid elmtype, int elmlen,
							  bool elmbyval, char elmalign, Datum **elemsp,
							  bool **nullsp, int *nelemsp);
bool		array_contains_nulls(ArrayType *array);
ArrayIterator array_create_iterator(ArrayType *arr, int slice_ndim, ArrayMetaState *mstate);
bool		array_iterate(ArrayIterator iterator, Datum *value, bool *isnull);
void		array_free_iterator(ArrayIterator iterator);
void		array_bitmap_copy(bits8 *destbitmap, int destoffset,
							  const bits8 *srcbitmap, int srcoffset, int nitems);
ArrayBuildState *initArrayResult(Oid element_type, MemoryContext rcontext, bool subcontext);
ArrayBuildState *initArrayResultWithSize(Oid element_type, MemoryContext rcontext,
										 bool subcontext, int initsize);
ArrayBuildState *accumArrayResult(ArrayBuildState *astate, Datum dvalue, bool disnull,
								  Oid element_type, MemoryContext rcontext);
Datum		makeArrayResult(ArrayBuildState *astate, MemoryContext rcontext);
Datum		makeMdArrayResult(ArrayBuildState *astate, int ndims, int *dims, int *lbs,
							  MemoryContext rcontext, bool release);
ArrayBuildStateArr *initArrayResultArr(Oid array_type, Oid element_type,
									   MemoryContext rcontext, bool subcontext);
ArrayBuildStateArr *accumArrayResultArr(ArrayBuildStateArr *astate, Datum dvalue,
										bool disnull, Oid array_type,
										MemoryContext rcontext);
Datum		makeArrayResultArr(ArrayBuildStateArr *astate, MemoryContext rcontext,
							   bool release);
Datum		trim_array(PG_FUNCTION_ARGS);
Datum		array_cat(PG_FUNCTION_ARGS);
Datum		array_agg_array_transfn(PG_FUNCTION_ARGS);
Datum		array_agg_array_combine(PG_FUNCTION_ARGS);
Datum		array_agg_array_serialize(PG_FUNCTION_ARGS);
Datum		array_agg_array_deserialize(PG_FUNCTION_ARGS);
Datum		array_agg_array_finalfn(PG_FUNCTION_ARGS);
Datum		array_position(PG_FUNCTION_ARGS);
Datum		array_position_start(PG_FUNCTION_ARGS);
Datum		array_positions(PG_FUNCTION_ARGS);
Datum		array_shuffle(PG_FUNCTION_ARGS);
Datum		array_sample(PG_FUNCTION_ARGS);
Datum		array_reverse(PG_FUNCTION_ARGS);

/* static-function forward decls (verbatim prototypes from arrayfuncs.c /
 * array_userfuncs.c heads; paste order below is not dependency order) */
static bool array_get_isnull(const bits8 *nullbitmap, int offset);
static void array_set_isnull(bits8 *nullbitmap, int offset, bool isNull);
static int	ArrayCastAndSet(Datum src, int typlen, bool typbyval, char typalign,
							char *dest);
static char *array_seek(char *ptr, int offset, bits8 *nullbitmap, int nitems,
						int typlen, bool typbyval, char typalign);
static int	array_nelems_size(char *ptr, int offset, bits8 *nullbitmap,
							  int nitems, int typlen, bool typbyval, char typalign);
static int	array_copy(char *destptr, int nitems,
					   char *srcptr, int offset, bits8 *nullbitmap,
					   int typlen, bool typbyval, char typalign);
static int	array_slice_size(char *arraydataptr, bits8 *arraynullsptr,
							 int ndim, int *dim, int *lb,
							 int *st, int *endp,
							 int typlen, bool typbyval, char typalign);
static void array_extract_slice(ArrayType *newarray,
								int ndim, int *dim, int *lb,
								char *arraydataptr, bits8 *arraynullsptr,
								int *st, int *endp,
								int typlen, bool typbyval, char typalign);
static Datum array_position_common(FunctionCallInfo fcinfo);
static ArrayType *array_shuffle_n(ArrayType *array, int n, bool keep_lb,
								  Oid elmtyp, TypeCacheEntry *typentry);
static ArrayType *array_reverse_n(ArrayType *array, Oid elmtyp, TypeCacheEntry *typentry);

/* FLAT-ARRAY FENCE: VARATT_IS_EXTERNAL_EXPANDED is constant-false above, so
 * the verbatim call is dead code; the stub satisfies the linker only. */
static Datum
array_set_element_expanded(Datum arraydatum, int nSubscripts, int *indx,
						   Datum dataValue, bool isNull, int arraytyplen,
						   int elmlen, bool elmbyval, char elmalign)
{
	abort();
}

/* ==================== SECTION 1: arrayutils.c (VERBATIM) ==================== */

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

/* ==================== SECTION 2a: arrayfuncs.c (VERBATIM) =================== */

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

/*
 * array_get_slice :
 *		   This routine takes an array and a range of indices (upperIndx and
 *		   lowerIndx), creates a new array structure for the referred elements
 *		   and returns a pointer to it.
 *
 * This handles both ordinary varlena arrays and fixed-length arrays.
 *
 * Inputs:
 *	arraydatum: the array object (mustn't be NULL)
 *	nSubscripts: number of subscripts supplied (must be same for upper/lower)
 *	upperIndx[]: the upper subscript values
 *	lowerIndx[]: the lower subscript values
 *	upperProvided[]: true for provided upper subscript values
 *	lowerProvided[]: true for provided lower subscript values
 *	arraytyplen: pg_type.typlen for the array type
 *	elmlen: pg_type.typlen for the array's element type
 *	elmbyval: pg_type.typbyval for the array's element type
 *	elmalign: pg_type.typalign for the array's element type
 *
 * Outputs:
 *	The return value is the new array Datum (it's never NULL)
 *
 * Omitted upper and lower subscript values are replaced by the corresponding
 * array bound.
 *
 * NOTE: we assume it is OK to scribble on the provided subscript arrays
 * lowerIndx[] and upperIndx[]; also, these arrays must be of size MAXDIM
 * even when nSubscripts is less.  These are generally just temporaries.
 */
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

/*
 * array_set_element :
 *		  This routine sets the value of one array element (specified by
 *		  a subscript array) to a new value specified by "dataValue".
 *
 * This handles both ordinary varlena arrays and fixed-length arrays.
 *
 * Inputs:
 *	arraydatum: the initial array object (mustn't be NULL)
 *	nSubscripts: number of subscripts supplied
 *	indx[]: the subscript values
 *	dataValue: the datum to be inserted at the given position
 *	isNull: whether dataValue is NULL
 *	arraytyplen: pg_type.typlen for the array type
 *	elmlen: pg_type.typlen for the array's element type
 *	elmbyval: pg_type.typbyval for the array's element type
 *	elmalign: pg_type.typalign for the array's element type
 *
 * Result:
 *		  A new array is returned, just like the old except for the one
 *		  modified entry.  The original array object is not changed,
 *		  unless what is passed is a read-write reference to an expanded
 *		  array object; in that case the expanded array is updated in-place.
 *
 * For one-dimensional arrays only, we allow the array to be extended
 * by assigning to a position outside the existing subscript range; any
 * positions between the existing elements and the new one are set to NULLs.
 * (XXX TODO: allow a corresponding behavior for multidimensional arrays)
 *
 * NOTE: For assignments, we throw an error for invalid subscripts etc,
 * rather than returning a NULL as the fetch operations do.
 */
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

/*
 * construct_empty_array	--- make a zero-dimensional array of given type
 */
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

/*
 * deconstruct_array  --- simple method for extracting data from an array
 *
 * array: array object to examine (must not be NULL)
 * elmtype, elmlen, elmbyval, elmalign: info for the datatype of the items
 * elemsp: return value, set to point to palloc'd array of Datum values
 * nullsp: return value, set to point to palloc'd array of isnull markers
 * nelemsp: return value, set to number of extracted values
 *
 * The caller may pass nullsp == NULL if it does not support NULLs in the
 * array.  Note that this produces a very uninformative error message,
 * so do it only in cases where a NULL is really not expected.
 *
 * If array elements are pass-by-ref data type, the returned Datums will
 * be pointers into the array object.
 *
 * NOTE: it would be cleaner to look up the elmlen/elmbval/elmalign info
 * from the system catalogs, given the elmtype.  However, the caller is
 * in a better position to cache this info across multiple uses, or even
 * to hard-wire values if the element type is hard-wired.
 */
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
 * array_create_iterator --- set up to iterate through an array
 *
 * If slice_ndim is zero, we will iterate element-by-element; the returned
 * datums are of the array's element type.
 *
 * If slice_ndim is 1..ARR_NDIM(arr), we will iterate by slices: the
 * returned datums are of the same array type as 'arr', but of size
 * equal to the rightmost N dimensions of 'arr'.
 *
 * The passed-in array must remain valid for the lifetime of the iterator.
 */
ArrayIterator
array_create_iterator(ArrayType *arr, int slice_ndim, ArrayMetaState *mstate)
{
	ArrayIterator iterator = palloc0(sizeof(ArrayIteratorData));

	/*
	 * Sanity-check inputs --- caller should have got this right already
	 */
	Assert(PointerIsValid(arr));
	if (slice_ndim < 0 || slice_ndim > ARR_NDIM(arr))
		elog(ERROR, "invalid arguments to array_create_iterator");

	/*
	 * Remember basic info about the array and its element type
	 */
	iterator->arr = arr;
	iterator->nullbitmap = ARR_NULLBITMAP(arr);
	iterator->nitems = ArrayGetNItems(ARR_NDIM(arr), ARR_DIMS(arr));

	if (mstate != NULL)
	{
		Assert(mstate->element_type == ARR_ELEMTYPE(arr));

		iterator->typlen = mstate->typlen;
		iterator->typbyval = mstate->typbyval;
		iterator->typalign = mstate->typalign;
	}
	else
		get_typlenbyvalalign(ARR_ELEMTYPE(arr),
							 &iterator->typlen,
							 &iterator->typbyval,
							 &iterator->typalign);

	/*
	 * Remember the slicing parameters.
	 */
	iterator->slice_ndim = slice_ndim;

	if (slice_ndim > 0)
	{
		/*
		 * Get pointers into the array's dims and lbound arrays to represent
		 * the dims/lbound arrays of a slice.  These are the same as the
		 * rightmost N dimensions of the array.
		 */
		iterator->slice_dims = ARR_DIMS(arr) + ARR_NDIM(arr) - slice_ndim;
		iterator->slice_lbound = ARR_LBOUND(arr) + ARR_NDIM(arr) - slice_ndim;

		/*
		 * Compute number of elements in a slice.
		 */
		iterator->slice_len = ArrayGetNItems(slice_ndim,
											 iterator->slice_dims);

		/*
		 * Create workspace for building sub-arrays.
		 */
		iterator->slice_values = (Datum *)
			palloc(iterator->slice_len * sizeof(Datum));
		iterator->slice_nulls = (bool *)
			palloc(iterator->slice_len * sizeof(bool));
	}

	/*
	 * Initialize our data pointer and linear element number.  These will
	 * advance through the array during array_iterate().
	 */
	iterator->data_ptr = ARR_DATA_PTR(arr);
	iterator->current_item = 0;

	return iterator;
}

/*
 * Iterate through the array referenced by 'iterator'.
 *
 * As long as there is another element (or slice), return it into
 * *value / *isnull, and return true.  Return false when no more data.
 */
bool
array_iterate(ArrayIterator iterator, Datum *value, bool *isnull)
{
	/* Done if we have reached the end of the array */
	if (iterator->current_item >= iterator->nitems)
		return false;

	if (iterator->slice_ndim == 0)
	{
		/*
		 * Scalar case: return one element.
		 */
		if (array_get_isnull(iterator->nullbitmap, iterator->current_item++))
		{
			*isnull = true;
			*value = (Datum) 0;
		}
		else
		{
			/* non-NULL, so fetch the individual Datum to return */
			char	   *p = iterator->data_ptr;

			*isnull = false;
			*value = fetch_att(p, iterator->typbyval, iterator->typlen);

			/* Move our data pointer forward to the next element */
			p = att_addlength_pointer(p, iterator->typlen, p);
			p = (char *) att_align_nominal(p, iterator->typalign);
			iterator->data_ptr = p;
		}
	}
	else
	{
		/*
		 * Slice case: build and return an array of the requested size.
		 */
		ArrayType  *result;
		Datum	   *values = iterator->slice_values;
		bool	   *nulls = iterator->slice_nulls;
		char	   *p = iterator->data_ptr;
		int			i;

		for (i = 0; i < iterator->slice_len; i++)
		{
			if (array_get_isnull(iterator->nullbitmap,
								 iterator->current_item++))
			{
				nulls[i] = true;
				values[i] = (Datum) 0;
			}
			else
			{
				nulls[i] = false;
				values[i] = fetch_att(p, iterator->typbyval, iterator->typlen);

				/* Move our data pointer forward to the next element */
				p = att_addlength_pointer(p, iterator->typlen, p);
				p = (char *) att_align_nominal(p, iterator->typalign);
			}
		}

		iterator->data_ptr = p;

		result = construct_md_array(values,
									nulls,
									iterator->slice_ndim,
									iterator->slice_dims,
									iterator->slice_lbound,
									ARR_ELEMTYPE(iterator->arr),
									iterator->typlen,
									iterator->typbyval,
									iterator->typalign);

		*isnull = false;
		*value = PointerGetDatum(result);
	}

	return true;
}

/*
 * Release an ArrayIterator data structure
 */
void
array_free_iterator(ArrayIterator iterator)
{
	if (iterator->slice_ndim > 0)
	{
		pfree(iterator->slice_values);
		pfree(iterator->slice_nulls);
	}
	pfree(iterator);
}

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
 * initArrayResult - initialize an empty ArrayBuildState
 *
 *	element_type is the array element type (must be a valid array element type)
 *	rcontext is where to keep working state
 *	subcontext is a flag determining whether to use a separate memory context
 *
 * Note: there are two common schemes for using accumArrayResult().
 * In the older scheme, you start with a NULL ArrayBuildState pointer, and
 * call accumArrayResult once per element.  In this scheme you end up with
 * a NULL pointer if there were no elements, which you need to special-case.
 * In the newer scheme, call initArrayResult and then call accumArrayResult
 * once per element.  In this scheme you always end with a non-NULL pointer
 * that you can pass to makeArrayResult; you get an empty array if there
 * were no elements.  This is preferred if an empty array is what you want.
 *
 * It's possible to choose whether to create a separate memory context for the
 * array build state, or whether to allocate it directly within rcontext.
 *
 * When there are many concurrent small states (e.g. array_agg() using hash
 * aggregation of many small groups), using a separate memory context for each
 * one may result in severe memory bloat. In such cases, use the same memory
 * context to initialize all such array build states, and pass
 * subcontext=false.
 *
 * In cases when the array build states have different lifetimes, using a
 * single memory context is impractical. Instead, pass subcontext=true so that
 * the array build states can be freed individually.
 */
ArrayBuildState *
initArrayResult(Oid element_type, MemoryContext rcontext, bool subcontext)
{
	/*
	 * When using a subcontext, we can afford to start with a somewhat larger
	 * initial array size.  Without subcontexts, we'd better hope that most of
	 * the states stay small ...
	 */
	return initArrayResultWithSize(element_type, rcontext, subcontext,
								   subcontext ? 64 : 8);
}

/*
 * initArrayResultWithSize
 *		As initArrayResult, but allow the initial size of the allocated arrays
 *		to be specified.
 */
ArrayBuildState *
initArrayResultWithSize(Oid element_type, MemoryContext rcontext,
						bool subcontext, int initsize)
{
	ArrayBuildState *astate;
	MemoryContext arr_context = rcontext;

	/* Make a temporary context to hold all the junk */
	if (subcontext)
		arr_context = AllocSetContextCreate(rcontext,
											"accumArrayResult",
											ALLOCSET_DEFAULT_SIZES);

	astate = (ArrayBuildState *)
		MemoryContextAlloc(arr_context, sizeof(ArrayBuildState));
	astate->mcontext = arr_context;
	astate->private_cxt = subcontext;
	astate->alen = initsize;
	astate->dvalues = (Datum *)
		MemoryContextAlloc(arr_context, astate->alen * sizeof(Datum));
	astate->dnulls = (bool *)
		MemoryContextAlloc(arr_context, astate->alen * sizeof(bool));
	astate->nelems = 0;
	astate->element_type = element_type;
	get_typlenbyvalalign(element_type,
						 &astate->typlen,
						 &astate->typbyval,
						 &astate->typalign);

	return astate;
}

/*
 * accumArrayResult - accumulate one (more) Datum for an array result
 *
 *	astate is working state (can be NULL on first call)
 *	dvalue/disnull represent the new Datum to append to the array
 *	element_type is the Datum's type (must be a valid array element type)
 *	rcontext is where to keep working state
 */
ArrayBuildState *
accumArrayResult(ArrayBuildState *astate,
				 Datum dvalue, bool disnull,
				 Oid element_type,
				 MemoryContext rcontext)
{
	MemoryContext oldcontext;

	if (astate == NULL)
	{
		/* First time through --- initialize */
		astate = initArrayResult(element_type, rcontext, true);
	}
	else
	{
		Assert(astate->element_type == element_type);
	}

	oldcontext = MemoryContextSwitchTo(astate->mcontext);

	/* enlarge dvalues[]/dnulls[] if needed */
	if (astate->nelems >= astate->alen)
	{
		astate->alen *= 2;
		/* give an array-related error if we go past MaxAllocSize */
		if (!AllocSizeIsValid(astate->alen * sizeof(Datum)))
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("array size exceeds the maximum allowed (%d)",
							(int) MaxAllocSize)));
		astate->dvalues = (Datum *)
			repalloc(astate->dvalues, astate->alen * sizeof(Datum));
		astate->dnulls = (bool *)
			repalloc(astate->dnulls, astate->alen * sizeof(bool));
	}

	/*
	 * Ensure pass-by-ref stuff is copied into mcontext; and detoast it too if
	 * it's varlena.  (You might think that detoasting is not needed here
	 * because construct_md_array can detoast the array elements later.
	 * However, we must not let construct_md_array modify the ArrayBuildState
	 * because that would mean array_agg_finalfn damages its input, which is
	 * verboten.  Also, this way frequently saves one copying step.)
	 */
	if (!disnull && !astate->typbyval)
	{
		if (astate->typlen == -1)
			dvalue = PointerGetDatum(PG_DETOAST_DATUM_COPY(dvalue));
		else
			dvalue = datumCopy(dvalue, astate->typbyval, astate->typlen);
	}

	astate->dvalues[astate->nelems] = dvalue;
	astate->dnulls[astate->nelems] = disnull;
	astate->nelems++;

	MemoryContextSwitchTo(oldcontext);

	return astate;
}

/*
 * makeArrayResult - produce 1-D final result of accumArrayResult
 *
 * Note: only releases astate if it was initialized within a separate memory
 * context (i.e. using subcontext=true when calling initArrayResult).
 *
 *	astate is working state (must not be NULL)
 *	rcontext is where to construct result
 */
Datum
makeArrayResult(ArrayBuildState *astate,
				MemoryContext rcontext)
{
	int			ndims;
	int			dims[1];
	int			lbs[1];

	/* If no elements were presented, we want to create an empty array */
	ndims = (astate->nelems > 0) ? 1 : 0;
	dims[0] = astate->nelems;
	lbs[0] = 1;

	return makeMdArrayResult(astate, ndims, dims, lbs, rcontext,
							 astate->private_cxt);
}

/*
 * makeMdArrayResult - produce multi-D final result of accumArrayResult
 *
 * beware: no check that specified dimensions match the number of values
 * accumulated.
 *
 * Note: if the astate was not initialized within a separate memory context
 * (that is, initArrayResult was called with subcontext=false), then using
 * release=true is illegal. Instead, release astate along with the rest of its
 * context when appropriate.
 *
 *	astate is working state (must not be NULL)
 *	rcontext is where to construct result
 *	release is true if okay to release working state
 */
Datum
makeMdArrayResult(ArrayBuildState *astate,
				  int ndims,
				  int *dims,
				  int *lbs,
				  MemoryContext rcontext,
				  bool release)
{
	ArrayType  *result;
	MemoryContext oldcontext;

	/* Build the final array result in rcontext */
	oldcontext = MemoryContextSwitchTo(rcontext);

	result = construct_md_array(astate->dvalues,
								astate->dnulls,
								ndims,
								dims,
								lbs,
								astate->element_type,
								astate->typlen,
								astate->typbyval,
								astate->typalign);

	MemoryContextSwitchTo(oldcontext);

	/* Clean up all the junk */
	if (release)
	{
		Assert(astate->private_cxt);
		MemoryContextDelete(astate->mcontext);
	}

	return PointerGetDatum(result);
}

/*
 * initArrayResultArr - initialize an empty ArrayBuildStateArr
 *
 *	array_type is the array type (must be a valid varlena array type)
 *	element_type is the type of the array's elements (lookup if InvalidOid)
 *	rcontext is where to keep working state
 *	subcontext is a flag determining whether to use a separate memory context
 */
ArrayBuildStateArr *
initArrayResultArr(Oid array_type, Oid element_type, MemoryContext rcontext,
				   bool subcontext)
{
	ArrayBuildStateArr *astate;
	MemoryContext arr_context = rcontext;	/* by default use the parent ctx */

	/* Lookup element type, unless element_type already provided */
	if (!OidIsValid(element_type))
	{
		element_type = get_element_type(array_type);

		if (!OidIsValid(element_type))
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("data type %s is not an array type",
							format_type_be(array_type))));
	}

	/* Make a temporary context to hold all the junk */
	if (subcontext)
		arr_context = AllocSetContextCreate(rcontext,
											"accumArrayResultArr",
											ALLOCSET_DEFAULT_SIZES);

	/* Note we initialize all fields to zero */
	astate = (ArrayBuildStateArr *)
		MemoryContextAllocZero(arr_context, sizeof(ArrayBuildStateArr));
	astate->mcontext = arr_context;
	astate->private_cxt = subcontext;

	/* Save relevant datatype information */
	astate->array_type = array_type;
	astate->element_type = element_type;

	return astate;
}

/*
 * accumArrayResultArr - accumulate one (more) sub-array for an array result
 *
 *	astate is working state (can be NULL on first call)
 *	dvalue/disnull represent the new sub-array to append to the array
 *	array_type is the array type (must be a valid varlena array type)
 *	rcontext is where to keep working state
 */
ArrayBuildStateArr *
accumArrayResultArr(ArrayBuildStateArr *astate,
					Datum dvalue, bool disnull,
					Oid array_type,
					MemoryContext rcontext)
{
	ArrayType  *arg;
	MemoryContext oldcontext;
	int		   *dims,
			   *lbs,
				ndims,
				nitems,
				ndatabytes;
	char	   *data;
	int			i;

	/*
	 * We disallow accumulating null subarrays.  Another plausible definition
	 * is to ignore them, but callers that want that can just skip calling
	 * this function.
	 */
	if (disnull)
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("cannot accumulate null arrays")));

	/* Detoast input array in caller's context */
	arg = DatumGetArrayTypeP(dvalue);

	if (astate == NULL)
		astate = initArrayResultArr(array_type, InvalidOid, rcontext, true);
	else
		Assert(astate->array_type == array_type);

	oldcontext = MemoryContextSwitchTo(astate->mcontext);

	/* Collect this input's dimensions */
	ndims = ARR_NDIM(arg);
	dims = ARR_DIMS(arg);
	lbs = ARR_LBOUND(arg);
	data = ARR_DATA_PTR(arg);
	nitems = ArrayGetNItems(ndims, dims);
	ndatabytes = ARR_SIZE(arg) - ARR_DATA_OFFSET(arg);

	if (astate->ndims == 0)
	{
		/* First input; check/save the dimensionality info */

		/* Should we allow empty inputs and just produce an empty output? */
		if (ndims == 0)
			ereport(ERROR,
					(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
					 errmsg("cannot accumulate empty arrays")));
		if (ndims + 1 > MAXDIM)
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("number of array dimensions (%d) exceeds the maximum allowed (%d)",
							ndims + 1, MAXDIM)));

		/*
		 * The output array will have n+1 dimensions, with the ones after the
		 * first matching the input's dimensions.
		 */
		astate->ndims = ndims + 1;
		astate->dims[0] = 0;
		memcpy(&astate->dims[1], dims, ndims * sizeof(int));
		astate->lbs[0] = 1;
		memcpy(&astate->lbs[1], lbs, ndims * sizeof(int));

		/* Allocate at least enough data space for this item */
		astate->abytes = pg_nextpower2_32(Max(1024, ndatabytes + 1));
		astate->data = (char *) palloc(astate->abytes);
	}
	else
	{
		/* Second or later input: must match first input's dimensionality */
		if (astate->ndims != ndims + 1)
			ereport(ERROR,
					(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
					 errmsg("cannot accumulate arrays of different dimensionality")));
		for (i = 0; i < ndims; i++)
		{
			if (astate->dims[i + 1] != dims[i] || astate->lbs[i + 1] != lbs[i])
				ereport(ERROR,
						(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
						 errmsg("cannot accumulate arrays of different dimensionality")));
		}

		/* Enlarge data space if needed */
		if (astate->nbytes + ndatabytes > astate->abytes)
		{
			astate->abytes = Max(astate->abytes * 2,
								 astate->nbytes + ndatabytes);
			astate->data = (char *) repalloc(astate->data, astate->abytes);
		}
	}

	/*
	 * Copy the data portion of the sub-array.  Note we assume that the
	 * advertised data length of the sub-array is properly aligned.  We do not
	 * have to worry about detoasting elements since whatever's in the
	 * sub-array should be OK already.
	 */
	memcpy(astate->data + astate->nbytes, data, ndatabytes);
	astate->nbytes += ndatabytes;

	/* Deal with null bitmap if needed */
	if (astate->nullbitmap || ARR_HASNULL(arg))
	{
		int			newnitems = astate->nitems + nitems;

		if (astate->nullbitmap == NULL)
		{
			/*
			 * First input with nulls; we must retrospectively handle any
			 * previous inputs by marking all their items non-null.
			 */
			astate->aitems = pg_nextpower2_32(Max(256, newnitems + 1));
			astate->nullbitmap = (bits8 *) palloc((astate->aitems + 7) / 8);
			array_bitmap_copy(astate->nullbitmap, 0,
							  NULL, 0,
							  astate->nitems);
		}
		else if (newnitems > astate->aitems)
		{
			astate->aitems = Max(astate->aitems * 2, newnitems);
			astate->nullbitmap = (bits8 *)
				repalloc(astate->nullbitmap, (astate->aitems + 7) / 8);
		}
		array_bitmap_copy(astate->nullbitmap, astate->nitems,
						  ARR_NULLBITMAP(arg), 0,
						  nitems);
	}

	astate->nitems += nitems;
	astate->dims[0] += 1;

	MemoryContextSwitchTo(oldcontext);

	/* Release detoasted copy if any */
	if ((Pointer) arg != DatumGetPointer(dvalue))
		pfree(arg);

	return astate;
}

/*
 * makeArrayResultArr - produce N+1-D final result of accumArrayResultArr
 *
 *	astate is working state (must not be NULL)
 *	rcontext is where to construct result
 *	release is true if okay to release working state
 */
Datum
makeArrayResultArr(ArrayBuildStateArr *astate,
				   MemoryContext rcontext,
				   bool release)
{
	ArrayType  *result;
	MemoryContext oldcontext;

	/* Build the final array result in rcontext */
	oldcontext = MemoryContextSwitchTo(rcontext);

	if (astate->ndims == 0)
	{
		/* No inputs, return empty array */
		result = construct_empty_array(astate->element_type);
	}
	else
	{
		int			dataoffset,
					nbytes;

		/* Check for overflow of the array dimensions */
		(void) ArrayGetNItems(astate->ndims, astate->dims);
		ArrayCheckBounds(astate->ndims, astate->dims, astate->lbs);

		/* Compute required space */
		nbytes = astate->nbytes;
		if (astate->nullbitmap != NULL)
		{
			dataoffset = ARR_OVERHEAD_WITHNULLS(astate->ndims, astate->nitems);
			nbytes += dataoffset;
		}
		else
		{
			dataoffset = 0;
			nbytes += ARR_OVERHEAD_NONULLS(astate->ndims);
		}

		result = (ArrayType *) palloc0(nbytes);
		SET_VARSIZE(result, nbytes);
		result->ndim = astate->ndims;
		result->dataoffset = dataoffset;
		result->elemtype = astate->element_type;

		memcpy(ARR_DIMS(result), astate->dims, astate->ndims * sizeof(int));
		memcpy(ARR_LBOUND(result), astate->lbs, astate->ndims * sizeof(int));
		memcpy(ARR_DATA_PTR(result), astate->data, astate->nbytes);

		if (astate->nullbitmap != NULL)
			array_bitmap_copy(ARR_NULLBITMAP(result), 0,
							  astate->nullbitmap, 0,
							  astate->nitems);
	}

	MemoryContextSwitchTo(oldcontext);

	/* Clean up all the junk */
	if (release)
	{
		Assert(astate->private_cxt);
		MemoryContextDelete(astate->mcontext);
	}

	return PointerGetDatum(result);
}

/*
 * Trim the last N elements from an array by building an appropriate slice.
 * Only the first dimension is trimmed.
 */
Datum
trim_array(PG_FUNCTION_ARGS)
{
	ArrayType  *v = PG_GETARG_ARRAYTYPE_P(0);
	int			n = PG_GETARG_INT32(1);
	int			array_length = (ARR_NDIM(v) > 0) ? ARR_DIMS(v)[0] : 0;
	int16		elmlen;
	bool		elmbyval;
	char		elmalign;
	int			lower[MAXDIM];
	int			upper[MAXDIM];
	bool		lowerProvided[MAXDIM];
	bool		upperProvided[MAXDIM];
	Datum		result;

	/* Per spec, throw an error if out of bounds */
	if (n < 0 || n > array_length)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_ELEMENT_ERROR),
				 errmsg("number of elements to trim must be between 0 and %d",
						array_length)));

	/* Set all the bounds as unprovided except the first upper bound */
	memset(lowerProvided, false, sizeof(lowerProvided));
	memset(upperProvided, false, sizeof(upperProvided));
	if (ARR_NDIM(v) > 0)
	{
		upper[0] = ARR_LBOUND(v)[0] + array_length - n - 1;
		upperProvided[0] = true;
	}

	/* Fetch the needed information about the element type */
	get_typlenbyvalalign(ARR_ELEMTYPE(v), &elmlen, &elmbyval, &elmalign);

	/* Get the slice */
	result = array_get_slice(PointerGetDatum(v), 1,
							 upper, lower, upperProvided, lowerProvided,
							 -1, elmlen, elmbyval, elmalign);

	PG_RETURN_DATUM(result);
}

/* ==================== SECTION 2b: array_userfuncs.c (VERBATIM) ============== */

/*-----------------------------------------------------------------------------
 * array_cat :
 *		concatenate two nD arrays to form an nD array, or
 *		push an (n-1)D array onto the end of an nD array
 *----------------------------------------------------------------------------
 */
Datum
array_cat(PG_FUNCTION_ARGS)
{
	ArrayType  *v1,
			   *v2;
	ArrayType  *result;
	int		   *dims,
			   *lbs,
				ndims,
				nitems,
				ndatabytes,
				nbytes;
	int		   *dims1,
			   *lbs1,
				ndims1,
				nitems1,
				ndatabytes1;
	int		   *dims2,
			   *lbs2,
				ndims2,
				nitems2,
				ndatabytes2;
	int			i;
	char	   *dat1,
			   *dat2;
	bits8	   *bitmap1,
			   *bitmap2;
	Oid			element_type;
	Oid			element_type1;
	Oid			element_type2;
	int32		dataoffset;

	/* Concatenating a null array is a no-op, just return the other input */
	if (PG_ARGISNULL(0))
	{
		if (PG_ARGISNULL(1))
			PG_RETURN_NULL();
		result = PG_GETARG_ARRAYTYPE_P(1);
		PG_RETURN_ARRAYTYPE_P(result);
	}
	if (PG_ARGISNULL(1))
	{
		result = PG_GETARG_ARRAYTYPE_P(0);
		PG_RETURN_ARRAYTYPE_P(result);
	}

	v1 = PG_GETARG_ARRAYTYPE_P(0);
	v2 = PG_GETARG_ARRAYTYPE_P(1);

	element_type1 = ARR_ELEMTYPE(v1);
	element_type2 = ARR_ELEMTYPE(v2);

	/* Check we have matching element types */
	if (element_type1 != element_type2)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("cannot concatenate incompatible arrays"),
				 errdetail("Arrays with element types %s and %s are not "
						   "compatible for concatenation.",
						   format_type_be(element_type1),
						   format_type_be(element_type2))));

	/* OK, use it */
	element_type = element_type1;

	/*----------
	 * We must have one of the following combinations of inputs:
	 * 1) one empty array, and one non-empty array
	 * 2) both arrays empty
	 * 3) two arrays with ndims1 == ndims2
	 * 4) ndims1 == ndims2 - 1
	 * 5) ndims1 == ndims2 + 1
	 *----------
	 */
	ndims1 = ARR_NDIM(v1);
	ndims2 = ARR_NDIM(v2);

	/*
	 * short circuit - if one input array is empty, and the other is not, we
	 * return the non-empty one as the result
	 *
	 * if both are empty, return the first one
	 */
	if (ndims1 == 0 && ndims2 > 0)
		PG_RETURN_ARRAYTYPE_P(v2);

	if (ndims2 == 0)
		PG_RETURN_ARRAYTYPE_P(v1);

	/* the rest fall under rule 3, 4, or 5 */
	if (ndims1 != ndims2 &&
		ndims1 != ndims2 - 1 &&
		ndims1 != ndims2 + 1)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("cannot concatenate incompatible arrays"),
				 errdetail("Arrays of %d and %d dimensions are not "
						   "compatible for concatenation.",
						   ndims1, ndims2)));

	/* get argument array details */
	lbs1 = ARR_LBOUND(v1);
	lbs2 = ARR_LBOUND(v2);
	dims1 = ARR_DIMS(v1);
	dims2 = ARR_DIMS(v2);
	dat1 = ARR_DATA_PTR(v1);
	dat2 = ARR_DATA_PTR(v2);
	bitmap1 = ARR_NULLBITMAP(v1);
	bitmap2 = ARR_NULLBITMAP(v2);
	nitems1 = ArrayGetNItems(ndims1, dims1);
	nitems2 = ArrayGetNItems(ndims2, dims2);
	ndatabytes1 = ARR_SIZE(v1) - ARR_DATA_OFFSET(v1);
	ndatabytes2 = ARR_SIZE(v2) - ARR_DATA_OFFSET(v2);

	if (ndims1 == ndims2)
	{
		/*
		 * resulting array is made up of the elements (possibly arrays
		 * themselves) of the input argument arrays
		 */
		ndims = ndims1;
		dims = (int *) palloc(ndims * sizeof(int));
		lbs = (int *) palloc(ndims * sizeof(int));

		dims[0] = dims1[0] + dims2[0];
		lbs[0] = lbs1[0];

		for (i = 1; i < ndims; i++)
		{
			if (dims1[i] != dims2[i] || lbs1[i] != lbs2[i])
				ereport(ERROR,
						(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
						 errmsg("cannot concatenate incompatible arrays"),
						 errdetail("Arrays with differing element dimensions are "
								   "not compatible for concatenation.")));

			dims[i] = dims1[i];
			lbs[i] = lbs1[i];
		}
	}
	else if (ndims1 == ndims2 - 1)
	{
		/*
		 * resulting array has the second argument as the outer array, with
		 * the first argument inserted at the front of the outer dimension
		 */
		ndims = ndims2;
		dims = (int *) palloc(ndims * sizeof(int));
		lbs = (int *) palloc(ndims * sizeof(int));
		memcpy(dims, dims2, ndims * sizeof(int));
		memcpy(lbs, lbs2, ndims * sizeof(int));

		/* increment number of elements in outer array */
		dims[0] += 1;

		/* make sure the added element matches our existing elements */
		for (i = 0; i < ndims1; i++)
		{
			if (dims1[i] != dims[i + 1] || lbs1[i] != lbs[i + 1])
				ereport(ERROR,
						(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
						 errmsg("cannot concatenate incompatible arrays"),
						 errdetail("Arrays with differing dimensions are not "
								   "compatible for concatenation.")));
		}
	}
	else
	{
		/*
		 * (ndims1 == ndims2 + 1)
		 *
		 * resulting array has the first argument as the outer array, with the
		 * second argument appended to the end of the outer dimension
		 */
		ndims = ndims1;
		dims = (int *) palloc(ndims * sizeof(int));
		lbs = (int *) palloc(ndims * sizeof(int));
		memcpy(dims, dims1, ndims * sizeof(int));
		memcpy(lbs, lbs1, ndims * sizeof(int));

		/* increment number of elements in outer array */
		dims[0] += 1;

		/* make sure the added element matches our existing elements */
		for (i = 0; i < ndims2; i++)
		{
			if (dims2[i] != dims[i + 1] || lbs2[i] != lbs[i + 1])
				ereport(ERROR,
						(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
						 errmsg("cannot concatenate incompatible arrays"),
						 errdetail("Arrays with differing dimensions are not "
								   "compatible for concatenation.")));
		}
	}

	/* Do this mainly for overflow checking */
	nitems = ArrayGetNItems(ndims, dims);
	ArrayCheckBounds(ndims, dims, lbs);

	/* build the result array */
	ndatabytes = ndatabytes1 + ndatabytes2;
	if (ARR_HASNULL(v1) || ARR_HASNULL(v2))
	{
		dataoffset = ARR_OVERHEAD_WITHNULLS(ndims, nitems);
		nbytes = ndatabytes + dataoffset;
	}
	else
	{
		dataoffset = 0;			/* marker for no null bitmap */
		nbytes = ndatabytes + ARR_OVERHEAD_NONULLS(ndims);
	}
	result = (ArrayType *) palloc0(nbytes);
	SET_VARSIZE(result, nbytes);
	result->ndim = ndims;
	result->dataoffset = dataoffset;
	result->elemtype = element_type;
	memcpy(ARR_DIMS(result), dims, ndims * sizeof(int));
	memcpy(ARR_LBOUND(result), lbs, ndims * sizeof(int));
	/* data area is arg1 then arg2 */
	memcpy(ARR_DATA_PTR(result), dat1, ndatabytes1);
	memcpy(ARR_DATA_PTR(result) + ndatabytes1, dat2, ndatabytes2);
	/* handle the null bitmap if needed */
	if (ARR_HASNULL(result))
	{
		array_bitmap_copy(ARR_NULLBITMAP(result), 0,
						  bitmap1, 0,
						  nitems1);
		array_bitmap_copy(ARR_NULLBITMAP(result), nitems1,
						  bitmap2, 0,
						  nitems2);
	}

	PG_RETURN_ARRAYTYPE_P(result);
}

/*
 * ARRAY_AGG(anyarray) aggregate function
 */
Datum
array_agg_array_transfn(PG_FUNCTION_ARGS)
{
	Oid			arg1_typeid = get_fn_expr_argtype(fcinfo->flinfo, 1);
	MemoryContext aggcontext;
	ArrayBuildStateArr *state;

	if (arg1_typeid == InvalidOid)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("could not determine input data type")));

	/*
	 * Note: we do not need a run-time check about whether arg1_typeid is a
	 * valid array type, because the parser would have verified that while
	 * resolving the input/result types of this polymorphic aggregate.
	 */

	if (!AggCheckCallContext(fcinfo, &aggcontext))
	{
		/* cannot be called directly because of internal-type argument */
		elog(ERROR, "array_agg_array_transfn called in non-aggregate context");
	}


	if (PG_ARGISNULL(0))
		state = initArrayResultArr(arg1_typeid, InvalidOid, aggcontext, false);
	else
		state = (ArrayBuildStateArr *) PG_GETARG_POINTER(0);

	state = accumArrayResultArr(state,
								PG_GETARG_DATUM(1),
								PG_ARGISNULL(1),
								arg1_typeid,
								aggcontext);

	/*
	 * The transition type for array_agg() is declared to be "internal", which
	 * is a pass-by-value type the same size as a pointer.  So we can safely
	 * pass the ArrayBuildStateArr pointer through nodeAgg.c's machinations.
	 */
	PG_RETURN_POINTER(state);
}

Datum
array_agg_array_combine(PG_FUNCTION_ARGS)
{
	ArrayBuildStateArr *state1;
	ArrayBuildStateArr *state2;
	MemoryContext agg_context;
	MemoryContext old_context;

	if (!AggCheckCallContext(fcinfo, &agg_context))
		elog(ERROR, "aggregate function called in non-aggregate context");

	state1 = PG_ARGISNULL(0) ? NULL : (ArrayBuildStateArr *) PG_GETARG_POINTER(0);
	state2 = PG_ARGISNULL(1) ? NULL : (ArrayBuildStateArr *) PG_GETARG_POINTER(1);

	if (state2 == NULL)
	{
		/*
		 * NULL state2 is easy, just return state1, which we know is already
		 * in the agg_context
		 */
		if (state1 == NULL)
			PG_RETURN_NULL();
		PG_RETURN_POINTER(state1);
	}

	if (state1 == NULL)
	{
		/* We must copy state2's data into the agg_context */
		old_context = MemoryContextSwitchTo(agg_context);

		state1 = initArrayResultArr(state2->array_type, InvalidOid,
									agg_context, false);

		state1->abytes = state2->abytes;
		state1->data = (char *) palloc(state1->abytes);

		if (state2->nullbitmap)
		{
			int			size = (state2->aitems + 7) / 8;

			state1->nullbitmap = (bits8 *) palloc(size);
			memcpy(state1->nullbitmap, state2->nullbitmap, size);
		}

		memcpy(state1->data, state2->data, state2->nbytes);
		state1->nbytes = state2->nbytes;
		state1->aitems = state2->aitems;
		state1->nitems = state2->nitems;
		state1->ndims = state2->ndims;
		memcpy(state1->dims, state2->dims, sizeof(state2->dims));
		memcpy(state1->lbs, state2->lbs, sizeof(state2->lbs));
		state1->array_type = state2->array_type;
		state1->element_type = state2->element_type;

		MemoryContextSwitchTo(old_context);

		PG_RETURN_POINTER(state1);
	}

	/* We only need to combine the two states if state2 has any items */
	else if (state2->nitems > 0)
	{
		MemoryContext oldContext;
		int			reqsize = state1->nbytes + state2->nbytes;
		int			i;

		/*
		 * Check the states are compatible with each other.  Ensure we use the
		 * same error messages that are listed in accumArrayResultArr so that
		 * the same error is shown as would have been if we'd not used the
		 * combine function for the aggregation.
		 */
		if (state1->ndims != state2->ndims)
			ereport(ERROR,
					(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
					 errmsg("cannot accumulate arrays of different dimensionality")));

		/* Check dimensions match ignoring the first dimension. */
		for (i = 1; i < state1->ndims; i++)
		{
			if (state1->dims[i] != state2->dims[i] || state1->lbs[i] != state2->lbs[i])
				ereport(ERROR,
						(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
						 errmsg("cannot accumulate arrays of different dimensionality")));
		}


		oldContext = MemoryContextSwitchTo(state1->mcontext);

		/*
		 * If there's not enough space in state1 then we'll need to reallocate
		 * more.
		 */
		if (state1->abytes < reqsize)
		{
			/* use a power of 2 size rather than allocating just reqsize */
			state1->abytes = pg_nextpower2_32(reqsize);
			state1->data = (char *) repalloc(state1->data, state1->abytes);
		}

		if (state2->nullbitmap)
		{
			int			newnitems = state1->nitems + state2->nitems;

			if (state1->nullbitmap == NULL)
			{
				/*
				 * First input with nulls; we must retrospectively handle any
				 * previous inputs by marking all their items non-null.
				 */
				state1->aitems = pg_nextpower2_32(Max(256, newnitems + 1));
				state1->nullbitmap = (bits8 *) palloc((state1->aitems + 7) / 8);
				array_bitmap_copy(state1->nullbitmap, 0,
								  NULL, 0,
								  state1->nitems);
			}
			else if (newnitems > state1->aitems)
			{
				int			newaitems = state1->aitems + state2->aitems;

				state1->aitems = pg_nextpower2_32(newaitems);
				state1->nullbitmap = (bits8 *)
					repalloc(state1->nullbitmap, (state1->aitems + 7) / 8);
			}
			array_bitmap_copy(state1->nullbitmap, state1->nitems,
							  state2->nullbitmap, 0,
							  state2->nitems);
		}

		memcpy(state1->data + state1->nbytes, state2->data, state2->nbytes);
		state1->nbytes += state2->nbytes;
		state1->nitems += state2->nitems;

		state1->dims[0] += state2->dims[0];
		/* remaining dims already match, per test above */

		Assert(state1->array_type == state2->array_type);
		Assert(state1->element_type == state2->element_type);

		MemoryContextSwitchTo(oldContext);
	}

	PG_RETURN_POINTER(state1);
}

/*
 * array_agg_array_serialize
 *		Serialize ArrayBuildStateArr into bytea.
 */
Datum
array_agg_array_serialize(PG_FUNCTION_ARGS)
{
	ArrayBuildStateArr *state;
	StringInfoData buf;
	bytea	   *result;

	/* cannot be called directly because of internal-type argument */
	Assert(AggCheckCallContext(fcinfo, NULL));

	state = (ArrayBuildStateArr *) PG_GETARG_POINTER(0);

	pq_begintypsend(&buf);

	/*
	 * element_type. Putting this first is more convenient in deserialization
	 * so that we can init the new state sooner.
	 */
	pq_sendint32(&buf, state->element_type);

	/* array_type */
	pq_sendint32(&buf, state->array_type);

	/* nbytes */
	pq_sendint32(&buf, state->nbytes);

	/* data */
	pq_sendbytes(&buf, state->data, state->nbytes);

	/* abytes */
	pq_sendint32(&buf, state->abytes);

	/* aitems */
	pq_sendint32(&buf, state->aitems);

	/* nullbitmap */
	if (state->nullbitmap)
	{
		Assert(state->aitems > 0);
		pq_sendbytes(&buf, state->nullbitmap, (state->aitems + 7) / 8);
	}

	/* nitems */
	pq_sendint32(&buf, state->nitems);

	/* ndims */
	pq_sendint32(&buf, state->ndims);

	/* dims: XXX should we just send ndims elements? */
	pq_sendbytes(&buf, state->dims, sizeof(state->dims));

	/* lbs */
	pq_sendbytes(&buf, state->lbs, sizeof(state->lbs));

	result = pq_endtypsend(&buf);

	PG_RETURN_BYTEA_P(result);
}

Datum
array_agg_array_deserialize(PG_FUNCTION_ARGS)
{
	bytea	   *sstate;
	ArrayBuildStateArr *result;
	StringInfoData buf;
	Oid			element_type;
	Oid			array_type;
	int			nbytes;
	const char *temp;

	/* cannot be called directly because of internal-type argument */
	Assert(AggCheckCallContext(fcinfo, NULL));

	sstate = PG_GETARG_BYTEA_PP(0);

	/*
	 * Initialize a StringInfo so that we can "receive" it using the standard
	 * recv-function infrastructure.
	 */
	initReadOnlyStringInfo(&buf, VARDATA_ANY(sstate),
						   VARSIZE_ANY_EXHDR(sstate));

	/* element_type */
	element_type = pq_getmsgint(&buf, 4);

	/* array_type */
	array_type = pq_getmsgint(&buf, 4);

	/* nbytes */
	nbytes = pq_getmsgint(&buf, 4);

	result = initArrayResultArr(array_type, element_type,
								CurrentMemoryContext, false);

	result->abytes = 1024;
	while (result->abytes < nbytes)
		result->abytes *= 2;

	result->data = (char *) palloc(result->abytes);

	/* data */
	temp = pq_getmsgbytes(&buf, nbytes);
	memcpy(result->data, temp, nbytes);
	result->nbytes = nbytes;

	/* abytes */
	result->abytes = pq_getmsgint(&buf, 4);

	/* aitems: might be 0 */
	result->aitems = pq_getmsgint(&buf, 4);

	/* nullbitmap */
	if (result->aitems > 0)
	{
		int			size = (result->aitems + 7) / 8;

		result->nullbitmap = (bits8 *) palloc(size);
		temp = pq_getmsgbytes(&buf, size);
		memcpy(result->nullbitmap, temp, size);
	}
	else
		result->nullbitmap = NULL;

	/* nitems */
	result->nitems = pq_getmsgint(&buf, 4);

	/* ndims */
	result->ndims = pq_getmsgint(&buf, 4);

	/* dims */
	temp = pq_getmsgbytes(&buf, sizeof(result->dims));
	memcpy(result->dims, temp, sizeof(result->dims));

	/* lbs */
	temp = pq_getmsgbytes(&buf, sizeof(result->lbs));
	memcpy(result->lbs, temp, sizeof(result->lbs));

	pq_getmsgend(&buf);

	PG_RETURN_POINTER(result);
}

Datum
array_agg_array_finalfn(PG_FUNCTION_ARGS)
{
	Datum		result;
	ArrayBuildStateArr *state;

	/* cannot be called directly because of internal-type argument */
	Assert(AggCheckCallContext(fcinfo, NULL));

	state = PG_ARGISNULL(0) ? NULL : (ArrayBuildStateArr *) PG_GETARG_POINTER(0);

	if (state == NULL)
		PG_RETURN_NULL();		/* returns null iff no input values */

	/*
	 * Make the result.  We cannot release the ArrayBuildStateArr because
	 * sometimes aggregate final functions are re-executed.  Rather, it is
	 * nodeAgg.c's responsibility to reset the aggcontext when it's safe to do
	 * so.
	 */
	result = makeArrayResultArr(state, CurrentMemoryContext, false);

	PG_RETURN_DATUM(result);
}

/*-----------------------------------------------------------------------------
 * array_position, array_position_start :
 *			return the offset of a value in an array.
 *
 * IS NOT DISTINCT FROM semantics are used for comparisons.  Return NULL when
 * the value is not found.
 *-----------------------------------------------------------------------------
 */
Datum
array_position(PG_FUNCTION_ARGS)
{
	return array_position_common(fcinfo);
}

Datum
array_position_start(PG_FUNCTION_ARGS)
{
	return array_position_common(fcinfo);
}

/*
 * array_position_common
 *		Common code for array_position and array_position_start
 *
 * These are separate wrappers for the sake of opr_sanity regression test.
 * They are not strict so we have to test for null inputs explicitly.
 */
static Datum
array_position_common(FunctionCallInfo fcinfo)
{
	ArrayType  *array;
	Oid			collation = PG_GET_COLLATION();
	Oid			element_type;
	Datum		searched_element,
				value;
	bool		isnull;
	int			position,
				position_min;
	bool		found = false;
	TypeCacheEntry *typentry;
	ArrayMetaState *my_extra;
	bool		null_search;
	ArrayIterator array_iterator;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	array = PG_GETARG_ARRAYTYPE_P(0);

	/*
	 * We refuse to search for elements in multi-dimensional arrays, since we
	 * have no good way to report the element's location in the array.
	 */
	if (ARR_NDIM(array) > 1)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("searching for elements in multidimensional arrays is not supported")));

	/* Searching in an empty array is well-defined, though: it always fails */
	if (ARR_NDIM(array) < 1)
		PG_RETURN_NULL();

	if (PG_ARGISNULL(1))
	{
		/* fast return when the array doesn't have nulls */
		if (!array_contains_nulls(array))
			PG_RETURN_NULL();
		searched_element = (Datum) 0;
		null_search = true;
	}
	else
	{
		searched_element = PG_GETARG_DATUM(1);
		null_search = false;
	}

	element_type = ARR_ELEMTYPE(array);
	position = (ARR_LBOUND(array))[0] - 1;

	/* figure out where to start */
	if (PG_NARGS() == 3)
	{
		if (PG_ARGISNULL(2))
			ereport(ERROR,
					(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
					 errmsg("initial position must not be null")));

		position_min = PG_GETARG_INT32(2);
	}
	else
		position_min = (ARR_LBOUND(array))[0];

	/*
	 * We arrange to look up type info for array_create_iterator only once per
	 * series of calls, assuming the element type doesn't change underneath
	 * us.
	 */
	my_extra = (ArrayMetaState *) fcinfo->flinfo->fn_extra;
	if (my_extra == NULL)
	{
		fcinfo->flinfo->fn_extra = MemoryContextAlloc(fcinfo->flinfo->fn_mcxt,
													  sizeof(ArrayMetaState));
		my_extra = (ArrayMetaState *) fcinfo->flinfo->fn_extra;
		my_extra->element_type = ~element_type;
	}

	if (my_extra->element_type != element_type)
	{
		get_typlenbyvalalign(element_type,
							 &my_extra->typlen,
							 &my_extra->typbyval,
							 &my_extra->typalign);

		typentry = lookup_type_cache(element_type, TYPECACHE_EQ_OPR_FINFO);

		if (!OidIsValid(typentry->eq_opr_finfo.fn_oid))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_FUNCTION),
					 errmsg("could not identify an equality operator for type %s",
							format_type_be(element_type))));

		my_extra->element_type = element_type;
		fmgr_info_cxt(typentry->eq_opr_finfo.fn_oid, &my_extra->proc,
					  fcinfo->flinfo->fn_mcxt);
	}

	/* Examine each array element until we find a match. */
	array_iterator = array_create_iterator(array, 0, my_extra);
	while (array_iterate(array_iterator, &value, &isnull))
	{
		position++;

		/* skip initial elements if caller requested so */
		if (position < position_min)
			continue;

		/*
		 * Can't look at the array element's value if it's null; but if we
		 * search for null, we have a hit and are done.
		 */
		if (isnull || null_search)
		{
			if (isnull && null_search)
			{
				found = true;
				break;
			}
			else
				continue;
		}

		/* not nulls, so run the operator */
		if (DatumGetBool(FunctionCall2Coll(&my_extra->proc, collation,
										   searched_element, value)))
		{
			found = true;
			break;
		}
	}

	array_free_iterator(array_iterator);

	/* Avoid leaking memory when handed toasted input */
	PG_FREE_IF_COPY(array, 0);

	if (!found)
		PG_RETURN_NULL();

	PG_RETURN_INT32(position);
}

/*-----------------------------------------------------------------------------
 * array_positions :
 *			return an array of positions of a value in an array.
 *
 * IS NOT DISTINCT FROM semantics are used for comparisons.  Returns NULL when
 * the input array is NULL.  When the value is not found in the array, returns
 * an empty array.
 *
 * This is not strict so we have to test for null inputs explicitly.
 *-----------------------------------------------------------------------------
 */
Datum
array_positions(PG_FUNCTION_ARGS)
{
	ArrayType  *array;
	Oid			collation = PG_GET_COLLATION();
	Oid			element_type;
	Datum		searched_element,
				value;
	bool		isnull;
	int			position;
	TypeCacheEntry *typentry;
	ArrayMetaState *my_extra;
	bool		null_search;
	ArrayIterator array_iterator;
	ArrayBuildState *astate = NULL;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	array = PG_GETARG_ARRAYTYPE_P(0);

	/*
	 * We refuse to search for elements in multi-dimensional arrays, since we
	 * have no good way to report the element's location in the array.
	 */
	if (ARR_NDIM(array) > 1)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("searching for elements in multidimensional arrays is not supported")));

	astate = initArrayResult(INT4OID, CurrentMemoryContext, false);

	/* Searching in an empty array is well-defined, though: it always fails */
	if (ARR_NDIM(array) < 1)
		PG_RETURN_DATUM(makeArrayResult(astate, CurrentMemoryContext));

	if (PG_ARGISNULL(1))
	{
		/* fast return when the array doesn't have nulls */
		if (!array_contains_nulls(array))
			PG_RETURN_DATUM(makeArrayResult(astate, CurrentMemoryContext));
		searched_element = (Datum) 0;
		null_search = true;
	}
	else
	{
		searched_element = PG_GETARG_DATUM(1);
		null_search = false;
	}

	element_type = ARR_ELEMTYPE(array);
	position = (ARR_LBOUND(array))[0] - 1;

	/*
	 * We arrange to look up type info for array_create_iterator only once per
	 * series of calls, assuming the element type doesn't change underneath
	 * us.
	 */
	my_extra = (ArrayMetaState *) fcinfo->flinfo->fn_extra;
	if (my_extra == NULL)
	{
		fcinfo->flinfo->fn_extra = MemoryContextAlloc(fcinfo->flinfo->fn_mcxt,
													  sizeof(ArrayMetaState));
		my_extra = (ArrayMetaState *) fcinfo->flinfo->fn_extra;
		my_extra->element_type = ~element_type;
	}

	if (my_extra->element_type != element_type)
	{
		get_typlenbyvalalign(element_type,
							 &my_extra->typlen,
							 &my_extra->typbyval,
							 &my_extra->typalign);

		typentry = lookup_type_cache(element_type, TYPECACHE_EQ_OPR_FINFO);

		if (!OidIsValid(typentry->eq_opr_finfo.fn_oid))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_FUNCTION),
					 errmsg("could not identify an equality operator for type %s",
							format_type_be(element_type))));

		my_extra->element_type = element_type;
		fmgr_info_cxt(typentry->eq_opr_finfo.fn_oid, &my_extra->proc,
					  fcinfo->flinfo->fn_mcxt);
	}

	/*
	 * Accumulate each array position iff the element matches the given
	 * element.
	 */
	array_iterator = array_create_iterator(array, 0, my_extra);
	while (array_iterate(array_iterator, &value, &isnull))
	{
		position += 1;

		/*
		 * Can't look at the array element's value if it's null; but if we
		 * search for null, we have a hit.
		 */
		if (isnull || null_search)
		{
			if (isnull && null_search)
				astate =
					accumArrayResult(astate, Int32GetDatum(position), false,
									 INT4OID, CurrentMemoryContext);

			continue;
		}

		/* not nulls, so run the operator */
		if (DatumGetBool(FunctionCall2Coll(&my_extra->proc, collation,
										   searched_element, value)))
			astate =
				accumArrayResult(astate, Int32GetDatum(position), false,
								 INT4OID, CurrentMemoryContext);
	}

	array_free_iterator(array_iterator);

	/* Avoid leaking memory when handed toasted input */
	PG_FREE_IF_COPY(array, 0);

	PG_RETURN_DATUM(makeArrayResult(astate, CurrentMemoryContext));
}

/*
 * array_shuffle_n
 *		Return a copy of array with n randomly chosen items.
 *
 * The number of items must not exceed the size of the first dimension of the
 * array.  We preserve the first dimension's lower bound if keep_lb,
 * else it's set to 1.  Lower-order dimensions are preserved in any case.
 *
 * NOTE: it would be cleaner to look up the elmlen/elmbval/elmalign info
 * from the system catalogs, given only the elmtyp. However, the caller is
 * in a better position to cache this info across multiple calls.
 */
static ArrayType *
array_shuffle_n(ArrayType *array, int n, bool keep_lb,
				Oid elmtyp, TypeCacheEntry *typentry)
{
	ArrayType  *result;
	int			ndim,
			   *dims,
			   *lbs,
				nelm,
				nitem,
				rdims[MAXDIM],
				rlbs[MAXDIM];
	int16		elmlen;
	bool		elmbyval;
	char		elmalign;
	Datum	   *elms,
			   *ielms;
	bool	   *nuls,
			   *inuls;

	ndim = ARR_NDIM(array);
	dims = ARR_DIMS(array);
	lbs = ARR_LBOUND(array);

	elmlen = typentry->typlen;
	elmbyval = typentry->typbyval;
	elmalign = typentry->typalign;

	/* If the target array is empty, exit fast */
	if (ndim < 1 || dims[0] < 1 || n < 1)
		return construct_empty_array(elmtyp);

	deconstruct_array(array, elmtyp, elmlen, elmbyval, elmalign,
					  &elms, &nuls, &nelm);

	nitem = dims[0];			/* total number of items */
	nelm /= nitem;				/* number of elements per item */

	Assert(n <= nitem);			/* else it's caller error */

	/*
	 * Shuffle array using Fisher-Yates algorithm.  Scan the array and swap
	 * current item (nelm datums starting at ielms) with a randomly chosen
	 * later item (nelm datums starting at jelms) in each iteration.  We can
	 * stop once we've done n iterations; then first n items are the result.
	 */
	ielms = elms;
	inuls = nuls;
	for (int i = 0; i < n; i++)
	{
		int			j = (int) pg_prng_uint64_range(&pg_global_prng_state, i, nitem - 1) * nelm;
		Datum	   *jelms = elms + j;
		bool	   *jnuls = nuls + j;

		/* Swap i'th and j'th items; advance ielms/inuls to next item */
		for (int k = 0; k < nelm; k++)
		{
			Datum		elm = *ielms;
			bool		nul = *inuls;

			*ielms++ = *jelms;
			*inuls++ = *jnuls;
			*jelms++ = elm;
			*jnuls++ = nul;
		}
	}

	/* Set up dimensions of the result */
	memcpy(rdims, dims, ndim * sizeof(int));
	memcpy(rlbs, lbs, ndim * sizeof(int));
	rdims[0] = n;
	if (!keep_lb)
		rlbs[0] = 1;

	result = construct_md_array(elms, nuls, ndim, rdims, rlbs,
								elmtyp, elmlen, elmbyval, elmalign);

	pfree(elms);
	pfree(nuls);

	return result;
}

/*
 * array_shuffle
 *
 * Returns an array with the same dimensions as the input array, with its
 * first-dimension elements in random order.
 */
Datum
array_shuffle(PG_FUNCTION_ARGS)
{
	ArrayType  *array = PG_GETARG_ARRAYTYPE_P(0);
	ArrayType  *result;
	Oid			elmtyp;
	TypeCacheEntry *typentry;

	/*
	 * There is no point in shuffling empty arrays or arrays with less than
	 * two items.
	 */
	if (ARR_NDIM(array) < 1 || ARR_DIMS(array)[0] < 2)
		PG_RETURN_ARRAYTYPE_P(array);

	elmtyp = ARR_ELEMTYPE(array);
	typentry = (TypeCacheEntry *) fcinfo->flinfo->fn_extra;
	if (typentry == NULL || typentry->type_id != elmtyp)
	{
		typentry = lookup_type_cache(elmtyp, 0);
		fcinfo->flinfo->fn_extra = typentry;
	}

	result = array_shuffle_n(array, ARR_DIMS(array)[0], true, elmtyp, typentry);

	PG_RETURN_ARRAYTYPE_P(result);
}

/*
 * array_sample
 *
 * Returns an array of n randomly chosen first-dimension elements
 * from the input array.
 */
Datum
array_sample(PG_FUNCTION_ARGS)
{
	ArrayType  *array = PG_GETARG_ARRAYTYPE_P(0);
	int			n = PG_GETARG_INT32(1);
	ArrayType  *result;
	Oid			elmtyp;
	TypeCacheEntry *typentry;
	int			nitem;

	nitem = (ARR_NDIM(array) < 1) ? 0 : ARR_DIMS(array)[0];

	if (n < 0 || n > nitem)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("sample size must be between 0 and %d", nitem)));

	elmtyp = ARR_ELEMTYPE(array);
	typentry = (TypeCacheEntry *) fcinfo->flinfo->fn_extra;
	if (typentry == NULL || typentry->type_id != elmtyp)
	{
		typentry = lookup_type_cache(elmtyp, 0);
		fcinfo->flinfo->fn_extra = typentry;
	}

	result = array_shuffle_n(array, n, false, elmtyp, typentry);

	PG_RETURN_ARRAYTYPE_P(result);
}

/*
 * array_reverse_n
 *		Return a copy of array with reversed items.
 *
 * NOTE: it would be cleaner to look up the elmlen/elmbval/elmalign info
 * from the system catalogs, given only the elmtyp. However, the caller is
 * in a better position to cache this info across multiple calls.
 */
static ArrayType *
array_reverse_n(ArrayType *array, Oid elmtyp, TypeCacheEntry *typentry)
{
	ArrayType  *result;
	int			ndim,
			   *dims,
			   *lbs,
				nelm,
				nitem,
				rdims[MAXDIM],
				rlbs[MAXDIM];
	int16		elmlen;
	bool		elmbyval;
	char		elmalign;
	Datum	   *elms,
			   *ielms;
	bool	   *nuls,
			   *inuls;

	ndim = ARR_NDIM(array);
	dims = ARR_DIMS(array);
	lbs = ARR_LBOUND(array);

	elmlen = typentry->typlen;
	elmbyval = typentry->typbyval;
	elmalign = typentry->typalign;

	deconstruct_array(array, elmtyp, elmlen, elmbyval, elmalign,
					  &elms, &nuls, &nelm);

	nitem = dims[0];			/* total number of items */
	nelm /= nitem;				/* number of elements per item */

	/* Reverse the array */
	ielms = elms;
	inuls = nuls;
	for (int i = 0; i < nitem / 2; i++)
	{
		int			j = (nitem - i - 1) * nelm;
		Datum	   *jelms = elms + j;
		bool	   *jnuls = nuls + j;

		/* Swap i'th and j'th items; advance ielms/inuls to next item */
		for (int k = 0; k < nelm; k++)
		{
			Datum		elm = *ielms;
			bool		nul = *inuls;

			*ielms++ = *jelms;
			*inuls++ = *jnuls;
			*jelms++ = elm;
			*jnuls++ = nul;
		}
	}

	/* Set up dimensions of the result */
	memcpy(rdims, dims, ndim * sizeof(int));
	memcpy(rlbs, lbs, ndim * sizeof(int));
	rdims[0] = nitem;

	result = construct_md_array(elms, nuls, ndim, rdims, rlbs,
								elmtyp, elmlen, elmbyval, elmalign);

	pfree(elms);
	pfree(nuls);

	return result;
}

/*
 * array_reverse
 *
 * Returns an array with the same dimensions as the input array, with its
 * first-dimension elements in reverse order.
 */
Datum
array_reverse(PG_FUNCTION_ARGS)
{
	ArrayType  *array = PG_GETARG_ARRAYTYPE_P(0);
	ArrayType  *result;
	Oid			elmtyp;
	TypeCacheEntry *typentry;

	/*
	 * There is no point in reversing empty arrays or arrays with less than
	 * two items.
	 */
	if (ARR_NDIM(array) < 1 || ARR_DIMS(array)[0] < 2)
		PG_RETURN_ARRAYTYPE_P(array);

	elmtyp = ARR_ELEMTYPE(array);
	typentry = (TypeCacheEntry *) fcinfo->flinfo->fn_extra;
	if (typentry == NULL || typentry->type_id != elmtyp)
	{
		typentry = lookup_type_cache(elmtyp, 0);
		fcinfo->flinfo->fn_extra = (void *) typentry;
	}

	result = array_reverse_n(array, elmtyp, typentry);

	PG_RETURN_ARRAYTYPE_P(result);
}

/* ========== SECTION 3: fuzz-facing driver entries (NOT Postgres code) ===== */

/*
 * array_append / array_prepend, FLAT-ARRAY TRANSCRIPTION (documented fence):
 * upstream array_userfuncs.c works over ExpandedArrayHeader
 * (fetch_array_arg_replace_nulls / EOHPGetRWDatum); pgrust has no expanded
 * arrays and ports the same computation over the flat image, so the oracle
 * mirrors that: the index computation, overflow arms, error arms and the
 * prepend lower-bound restore are VERBATIM fragments of array_append /
 * array_prepend; the expanded-array plumbing is replaced by the flat
 * array_set_element call (array_set_element's flat path is verbatim above).
 * A NULL array argument takes fetch_array_arg_replace_nulls' construct-empty
 * arm (verbatim error arms included, driven by the argtype pin).
 */
static ArrayType *
pg_diff_au_fetch_or_empty(const uint8_t *arr, Oid *elemtype_out)
{
	if (arr != NULL)
	{
		ArrayType  *a = (ArrayType *) arr;

		*elemtype_out = ARR_ELEMTYPE(a);
		return a;
	}
	else
	{
		Oid			arr_typeid = pg_diff_au_argtype_pin;
		Oid			element_type;

		if (!OidIsValid(arr_typeid))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("could not determine input data type")));
		element_type = get_element_type(arr_typeid);
		if (!OidIsValid(element_type))
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("input data type is not an array")));
		*elemtype_out = element_type;
		return construct_empty_array(element_type);
	}
}

static int
pg_diff_au_emit(Datum d, uint8_t *out, int outcap)
{
	ArrayType  *r = (ArrayType *) DatumGetPointer(d);
	int			sz = (int) ARR_SIZE(r);

	if (sz > outcap)
	{
		/* driver bug: caller caps inputs so results fit */
		abort();
	}
	memcpy(out, r, sz);
	return sz;
}

/* element selector -> pinned oids/meta */
static Oid
pg_diff_au_elemsel_arrtype(int elemsel)
{
	return elemsel == 0 ? 1007 : 1009;
}

static Datum
pg_diff_au_elem_datum(int elemsel, const uint8_t *elem)
{
	if (elemsel == 0)
	{
		int32		v;

		memcpy(&v, elem, 4);
		return Int32GetDatum(v);
	}
	return PointerGetDatum(elem);
}

void
pg_diff_au_seed_prng(uint64_t seed)
{
	pg_prng_seed(&pg_global_prng_state, seed);
}

static int
pg_diff_au_append_prepend(int is_append, int elemsel, const uint8_t *arr,
						  int elem_null, const uint8_t *elem,
						  uint8_t *out, int outcap)
{
	ArrayType  *array;
	Oid			element_type;
	int16		typlen;
	bool		typbyval;
	char		typalign;
	Datum		newelem = (Datum) 0;
	int			indx;
	int			lb0 = 1;
	Datum		result;

	pg_diff_arena_reset();
	pg_diff_errcode = 0;
	pg_diff_au_argtype_pin = pg_diff_au_elemsel_arrtype(elemsel);
	if (setjmp(pg_diff_au_jmp) != 0)
		return -1;

	array = pg_diff_au_fetch_or_empty(arr, &element_type);
	get_typlenbyvalalign(element_type, &typlen, &typbyval, &typalign);
	if (!elem_null)
		newelem = pg_diff_au_elem_datum(elemsel, elem);

	/* VERBATIM index computation + error arms (array_append/array_prepend) */
	if (ARR_NDIM(array) == 1)
	{
		int		   *lb = ARR_LBOUND(array);
		int		   *dimv = ARR_DIMS(array);

		if (is_append)
		{
			if (pg_add_s32_overflow(lb[0], dimv[0], &indx))
				ereport(ERROR,
						(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
						 errmsg("integer out of range")));
			lb0 = lb[0];
		}
		else
		{
			lb0 = lb[0];
			if (pg_sub_s32_overflow(lb0, 1, &indx))
				ereport(ERROR,
						(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
						 errmsg("integer out of range")));
		}
	}
	else if (ARR_NDIM(array) == 0)
	{
		indx = 1;
		lb0 = 1;
	}
	else
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("argument must be empty or one-dimensional array")));

	result = array_set_element(PointerGetDatum(array),
							   1, &indx, newelem, elem_null,
							   -1, typlen, typbyval, typalign);

	/* Readjust result's LB to match the input's, as expected for prepend */
	if (!is_append)
	{
		ArrayType  *r = (ArrayType *) DatumGetPointer(result);

		if (ARR_NDIM(r) == 1)
			ARR_LBOUND(r)[0] = lb0;
	}

	return pg_diff_au_emit(result, out, outcap);
}

int
pg_diff_array_append(int elemsel, const uint8_t *arr, int elem_null,
					 const uint8_t *elem, uint8_t *out, int outcap)
{
	return pg_diff_au_append_prepend(1, elemsel, arr, elem_null, elem, out, outcap);
}

int
pg_diff_array_prepend(int elemsel, const uint8_t *arr, int elem_null,
					  const uint8_t *elem, uint8_t *out, int outcap)
{
	return pg_diff_au_append_prepend(0, elemsel, arr, elem_null, elem, out, outcap);
}

/* array_cat through the VERBATIM fmgr wrapper. NULL image = SQL NULL arg.
 * ret -2 = SQL NULL result. */
int
pg_diff_array_cat(const uint8_t *a1, const uint8_t *a2, uint8_t *out, int outcap)
{
	FunctionCallInfoBaseData fcdata;
	FmgrInfo	flinfo;
	Datum		d;

	pg_diff_arena_reset();
	pg_diff_errcode = 0;
	if (setjmp(pg_diff_au_jmp) != 0)
		return -1;

	memset(&fcdata, 0, sizeof(fcdata));
	memset(&flinfo, 0, sizeof(flinfo));
	fcdata.flinfo = &flinfo;
	fcdata.nargs = 2;
	fcdata.args[0].value = PointerGetDatum(a1);
	fcdata.args[0].isnull = (a1 == NULL);
	fcdata.args[1].value = PointerGetDatum(a2);
	fcdata.args[1].isnull = (a2 == NULL);
	d = array_cat(&fcdata);
	if (fcdata.isnull)
		return -2;
	return pg_diff_au_emit(d, out, outcap);
}

/* array_position / array_position_start / array_positions through the
 * VERBATIM wrappers (fn_extra fresh per call = first-call-of-series tier).
 * position: ret -1 err, -2 SQL NULL, 1 found (*pos_out).
 * positions: ret -1 err, -2 SQL NULL, else result image length. */
int
pg_diff_array_position(int elemsel, const uint8_t *arr, int elem_null,
					   const uint8_t *elem, int has_start, int start_null,
					   int32_t start, Oid collation, int32_t *pos_out)
{
	FunctionCallInfoBaseData fcdata;
	FmgrInfo	flinfo;
	Datum		d;

	pg_diff_arena_reset();
	pg_diff_errcode = 0;
	if (setjmp(pg_diff_au_jmp) != 0)
		return -1;

	memset(&fcdata, 0, sizeof(fcdata));
	memset(&flinfo, 0, sizeof(flinfo));
	flinfo.fn_mcxt = CurrentMemoryContext;
	fcdata.flinfo = &flinfo;
	fcdata.fncollation = collation;
	fcdata.nargs = has_start ? 3 : 2;
	fcdata.args[0].value = PointerGetDatum(arr);
	fcdata.args[0].isnull = (arr == NULL);
	fcdata.args[1].value = elem_null ? (Datum) 0 : pg_diff_au_elem_datum(elemsel, elem);
	fcdata.args[1].isnull = (elem_null != 0);
	if (has_start)
	{
		fcdata.args[2].value = Int32GetDatum(start);
		fcdata.args[2].isnull = (start_null != 0);
	}
	d = has_start ? array_position_start(&fcdata) : array_position(&fcdata);
	if (fcdata.isnull)
		return -2;
	*pos_out = DatumGetInt32(d);
	return 1;
}

int
pg_diff_array_positions(int elemsel, const uint8_t *arr, int elem_null,
						const uint8_t *elem, Oid collation,
						uint8_t *out, int outcap)
{
	FunctionCallInfoBaseData fcdata;
	FmgrInfo	flinfo;
	Datum		d;

	pg_diff_arena_reset();
	pg_diff_errcode = 0;
	if (setjmp(pg_diff_au_jmp) != 0)
		return -1;

	memset(&fcdata, 0, sizeof(fcdata));
	memset(&flinfo, 0, sizeof(flinfo));
	flinfo.fn_mcxt = CurrentMemoryContext;
	fcdata.flinfo = &flinfo;
	fcdata.fncollation = collation;
	fcdata.nargs = 2;
	fcdata.args[0].value = PointerGetDatum(arr);
	fcdata.args[0].isnull = (arr == NULL);
	fcdata.args[1].value = elem_null ? (Datum) 0 : pg_diff_au_elem_datum(elemsel, elem);
	fcdata.args[1].isnull = (elem_null != 0);
	d = array_positions(&fcdata);
	if (fcdata.isnull)
		return -2;
	return pg_diff_au_emit(d, out, outcap);
}

/* one-array-arg wrappers (trim/reverse/shuffle/sample), VERBATIM bodies */
static int
pg_diff_au_call1(Datum (*fn) (FunctionCallInfo), const uint8_t *arr,
				 int has_n, int32_t n, uint8_t *out, int outcap)
{
	FunctionCallInfoBaseData fcdata;
	FmgrInfo	flinfo;
	Datum		d;

	pg_diff_arena_reset();
	pg_diff_errcode = 0;
	if (setjmp(pg_diff_au_jmp) != 0)
		return -1;

	memset(&fcdata, 0, sizeof(fcdata));
	memset(&flinfo, 0, sizeof(flinfo));
	flinfo.fn_mcxt = CurrentMemoryContext;
	fcdata.flinfo = &flinfo;
	fcdata.nargs = has_n ? 2 : 1;
	fcdata.args[0].value = PointerGetDatum(arr);
	fcdata.args[0].isnull = false;
	if (has_n)
	{
		fcdata.args[1].value = Int32GetDatum(n);
		fcdata.args[1].isnull = false;
	}
	d = fn(&fcdata);
	if (fcdata.isnull)
		return -2;
	return pg_diff_au_emit(d, out, outcap);
}

int
pg_diff_trim_array(const uint8_t *arr, int32_t n, uint8_t *out, int outcap)
{
	return pg_diff_au_call1(trim_array, arr, 1, n, out, outcap);
}

int
pg_diff_array_reverse(const uint8_t *arr, uint8_t *out, int outcap)
{
	return pg_diff_au_call1(array_reverse, arr, 0, 0, out, outcap);
}

int
pg_diff_array_shuffle(const uint8_t *arr, uint64_t seed, uint8_t *out, int outcap)
{
	pg_diff_au_seed_prng(seed);
	return pg_diff_au_call1(array_shuffle, arr, 0, 0, out, outcap);
}

int
pg_diff_array_sample(const uint8_t *arr, int32_t n, uint64_t seed,
					 uint8_t *out, int outcap)
{
	pg_diff_au_seed_prng(seed);
	return pg_diff_au_call1(array_sample, arr, 1, n, out, outcap);
}

/*
 * array_agg_array pipeline: transfn over imgs[0..split) into state1 and
 * imgs[split..nimgs) into state2 (both through the VERBATIM wrappers with an
 * armed agg context), then serialize(state2) -> ser_out, deserialize those
 * bytes, combine(state1, deserialized), finalfn -> out.
 * ret: -1 err, -2 SQL NULL final, else final image length. *ser_len = -1
 * when state2 never existed.
 */
int
pg_diff_array_agg_pipeline(int elemsel, int nimgs, const uint8_t *imgs,
						   const int32_t *offs, const uint8_t *nullflags,
						   int split, uint8_t *ser_out, int ser_cap,
						   int32_t *ser_len, uint8_t *out, int outcap)
{
	FunctionCallInfoBaseData fcdata;
	FmgrInfo	flinfo;
	Datum		state1 = (Datum) 0;
	bool		state1_null = true;
	Datum		state2 = (Datum) 0;
	bool		state2_null = true;
	Datum		d;
	int			i;

	pg_diff_arena_reset();
	pg_diff_errcode = 0;
	pg_diff_au_argtype_pin = pg_diff_au_elemsel_arrtype(elemsel);
	*ser_len = -1;
	if (setjmp(pg_diff_au_jmp) != 0)
		return -1;

	memset(&flinfo, 0, sizeof(flinfo));
	flinfo.fn_mcxt = CurrentMemoryContext;

	for (i = 0; i < nimgs; i++)
	{
		Datum	   *st = (i < split) ? &state1 : &state2;
		bool	   *stnull = (i < split) ? &state1_null : &state2_null;

		memset(&fcdata, 0, sizeof(fcdata));
		fcdata.flinfo = &flinfo;
		fcdata.context = (void *) &pg_diff_au_dummycxt;	/* agg call */
		fcdata.nargs = 2;
		fcdata.args[0].value = *st;
		fcdata.args[0].isnull = *stnull;
		fcdata.args[1].value = PointerGetDatum(imgs + offs[i]);
		fcdata.args[1].isnull = (nullflags[i] != 0);
		*st = array_agg_array_transfn(&fcdata);
		*stnull = fcdata.isnull;
	}

	if (!state2_null)
	{
		bytea	   *ser;
		int			slen;

		memset(&fcdata, 0, sizeof(fcdata));
		fcdata.flinfo = &flinfo;
		fcdata.context = (void *) &pg_diff_au_dummycxt;
		fcdata.nargs = 1;
		fcdata.args[0].value = state2;
		fcdata.args[0].isnull = false;
		d = array_agg_array_serialize(&fcdata);
		ser = (bytea *) DatumGetPointer(d);
		slen = (int) VARSIZE(ser) - VARHDRSZ;
		if (slen > ser_cap)
			abort();
		memcpy(ser_out, VARDATA(ser), slen);
		*ser_len = slen;

		/* deserialize the serialized image back into a fresh state2 */
		memset(&fcdata, 0, sizeof(fcdata));
		fcdata.flinfo = &flinfo;
		fcdata.context = (void *) &pg_diff_au_dummycxt;
		fcdata.nargs = 2;
		fcdata.args[0].value = d;
		fcdata.args[0].isnull = false;
		fcdata.args[1].value = (Datum) 0;
		fcdata.args[1].isnull = true;
		state2 = array_agg_array_deserialize(&fcdata);
		state2_null = fcdata.isnull;
	}

	/* combine(state1, state2) */
	memset(&fcdata, 0, sizeof(fcdata));
	fcdata.flinfo = &flinfo;
	fcdata.context = (void *) &pg_diff_au_dummycxt;
	fcdata.nargs = 2;
	fcdata.args[0].value = state1;
	fcdata.args[0].isnull = state1_null;
	fcdata.args[1].value = state2;
	fcdata.args[1].isnull = state2_null;
	d = array_agg_array_combine(&fcdata);
	state1 = d;
	state1_null = fcdata.isnull;

	/* finalfn */
	memset(&fcdata, 0, sizeof(fcdata));
	fcdata.flinfo = &flinfo;
	fcdata.context = (void *) &pg_diff_au_dummycxt;
	fcdata.nargs = 2;
	fcdata.args[0].value = state1;
	fcdata.args[0].isnull = state1_null;
	fcdata.args[1].value = (Datum) 0;
	fcdata.args[1].isnull = true;
	d = array_agg_array_finalfn(&fcdata);
	if (fcdata.isnull)
		return -2;
	return pg_diff_au_emit(d, out, outcap);
}

/* deserialize arbitrary wire bytes (error plane), then re-serialize the
 * resulting state for canonical comparison. ret -1 err, else length. */
int
pg_diff_array_agg_deserialize_raw(int elemsel, const uint8_t *bytes, int len,
								  uint8_t *ser_out, int ser_cap)
{
	FunctionCallInfoBaseData fcdata;
	FmgrInfo	flinfo;
	Datum		d;
	bytea	   *input;
	bytea	   *ser;
	int			slen;

	pg_diff_arena_reset();
	pg_diff_errcode = 0;
	pg_diff_au_argtype_pin = pg_diff_au_elemsel_arrtype(elemsel);
	if (setjmp(pg_diff_au_jmp) != 0)
		return -1;

	input = (bytea *) palloc(len + VARHDRSZ);
	SET_VARSIZE(input, len + VARHDRSZ);
	memcpy(VARDATA(input), bytes, len);

	memset(&flinfo, 0, sizeof(flinfo));
	flinfo.fn_mcxt = CurrentMemoryContext;
	memset(&fcdata, 0, sizeof(fcdata));
	fcdata.flinfo = &flinfo;
	fcdata.context = (void *) &pg_diff_au_dummycxt;
	fcdata.nargs = 2;
	fcdata.args[0].value = PointerGetDatum(input);
	fcdata.args[0].isnull = false;
	fcdata.args[1].value = (Datum) 0;
	fcdata.args[1].isnull = true;
	d = array_agg_array_deserialize(&fcdata);

	memset(&fcdata, 0, sizeof(fcdata));
	fcdata.flinfo = &flinfo;
	fcdata.context = (void *) &pg_diff_au_dummycxt;
	fcdata.nargs = 1;
	fcdata.args[0].value = d;
	fcdata.args[0].isnull = false;
	d = array_agg_array_serialize(&fcdata);
	ser = (bytea *) DatumGetPointer(d);
	slen = (int) VARSIZE(ser) - VARHDRSZ;
	if (slen > ser_cap)
		abort();
	memcpy(ser_out, VARDATA(ser), slen);
	return slen;
}
