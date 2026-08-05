/*
 * pg_hstorefam_io.c: vendored PostgreSQL C oracle for the hstore_diff
 * differential fuzz target (100%-coverage campaign, lane p1-mb-contribc).
 * Crate under test: crates/contrib/hstore (see fuzz/core/src/hstorefam_diff.rs).
 *
 * Provenance (all bodies VERBATIM, extracted mechanically by
 * scratchpad/assemble_hstorefam.sh from the vendor tree at
 * ~/dev/pgrust-fabled/vendor/postgres-src, Stamp-18.3, upstream sha
 * 62d6c7d3df6287f1bd83199c1a746e50d31571a0 — never hand-typed):
 *   - src/include/c.h 655..669 (struct varlena, VARHDRSZ, bytea/text).
 *   - src/include/varatt.h 18..325 (varattrib structs + varatt macros).
 *   - src/include/utils/array.h 75, 77..82, 84..98 (MAXDIM, MaxArraySize,
 *     ArrayType), 276..323 (ARR_ macros).
 *   - src/include/access/tupmacs.h 49..76, 135..161, 168..200, 203..232.
 *   - src/include/common/int.h 147..202 (HAVE__BUILTIN_OP_OVERFLOW arm).
 *   - src/include/port/simd.h whole file (escape_json_with_len's Vector8).
 *   - src/include/lib/sort_template.h whole file, instantiated exactly as
 *     src/port/qsort.c does (ST_SORT=hst_pg_qsort, void element, runtime
 *     comparator) — the backend's qsort IS pg_qsort (port.h line 478), and
 *     the duplicate-key survivor in hstoreUniquePairs depends on it.
 *   - src/port/pgstrcasecmp.c 32..62 (pg_strcasecmp).
 *   - src/backend/parser/scansup.c scanner_isspace.
 *   - src/include/lib/stringinfo.h 46..54 (StringInfoData) and
 *     appendStringInfoCharMacro; src/common/stringinfo.c
 *     initStringInfoInternal/initStringInfo/resetStringInfo/appendStringInfo/
 *     appendStringInfoVA/appendStringInfoString/appendStringInfoChar/
 *     appendBinaryStringInfo/appendBinaryStringInfoNT/enlargeStringInfo.
 *   - src/include/libpq/pqformat.h pq_writeint32 + pq_sendint32;
 *     src/backend/libpq/pqformat.c pq_begintypsend/pq_endtypsend/pq_sendtext/
 *     pq_getmsgint/pq_getmsgbytes/pq_copymsgbytes/pq_getmsgtext.
 *   - src/backend/utils/adt/varlena.c cstring_to_text(_with_len).
 *   - src/backend/utils/adt/arrayutils.c ArrayGetOffset/ArrayGetNItems(Safe)/
 *     ArrayCheckBounds(Safe).
 *   - src/backend/utils/adt/arrayfuncs.c ArrayCast/ArrayCastAndSet/
 *     CopyArrayEls/construct_md_array/construct_empty_array/
 *     deconstruct_array/deconstruct_array_builtin.
 *   - src/common/jsonapi.c JSON_ALPHANUMERIC_CHAR, json_lex_number,
 *     IsValidJsonNumber.
 *   - src/backend/utils/adt/json.c escape_json_char/escape_json/
 *     ESCAPE_JSON_FLUSH_AFTER/escape_json_with_len.
 *   - contrib/hstore/hstore.h 11..205 (HSTORE_POLLUTE_NAMESPACE pinned 0).
 *   - contrib/hstore/hstore_io.c: HSParser..hstorePairs block, hstore_in,
 *     hstore_recv, hstore_from_text, hstore_from_arrays, hstore_from_array,
 *     cpw, hstore_out, hstore_send, hstore_to_json_loose, hstore_to_json.
 *   - contrib/hstore/hstore_op.c: hstoreFindKey, hstoreArrayToPairs,
 *     hstore_fetchval/exists/exists_any/exists_all/defined/delete/
 *     delete_array/delete_hstore/concat/slice_to_array/slice_to_hstore/
 *     akeys/avals, hstore_to_array_internal, hstore_to_array/to_matrix,
 *     hstore_contains/contained, hstore_cmp/eq/ne/gt/ge/lt/le,
 *     hstore_hash/hash_extended.
 *
 * CARVED OUT (exception rows in the lane bank):
 *   - hstore_from_record / hstore_populate_record (composite typcache +
 *     per-column fmgr IO — server catalog environment).
 *   - hstore_skeys/svals/each (SRF FuncCallContext machinery; the underlying
 *     iteration kernels are the same HSTORE_KEY/VAL walks driven via
 *     akeys/avals/to_array).
 *   - hstore_to_jsonb(_loose) (JsonbValue/pushJsonbValue + numeric_in drag).
 *   - hstore_compat.c old-format upgrade (no new-format producer exists on
 *     either side; DatumGetHStoreP is shimmed to the identity cast under the
 *     driver precondition that every image is new-format).
 *   - hstore_gist.c / hstore_gin.c opclasses (index AM environment).
 *
 * SHIMS (plumbing/environment only, never logic):
 *   - fixed-width typedefs as c.h on LP64; Datum = uintptr_t (8B); Assert
 *     no-op (release parity); palloc/palloc0/repalloc/pstrdup -> tracked
 *     malloc arena freed by pg_hst_reset() per exec; pfree -> no-op (arena).
 *   - ereport(ERROR)/elog(ERROR): errcode records the REAL MAKE_SQLSTATE
 *     value in TLS hst_sqlstate, longjmp out; driver entries setjmp and
 *     return -1. errsave/ereturn honor a non-NULL escontext (soft-error
 *     protocol: record + return, elog.c semantics); Node is reduced to the
 *     error_occurred flag SOFT_ERROR_OCCURRED reads.
 *   - fmgr: minimal FunctionCallInfoBaseData + PG_GETARG/PG_RETURN macros
 *     (rowtypes-oracle precedent); PG_DETOAST_DATUM(_PACKED) -> identity
 *     (driver precondition: plain 4B-header images, no toast/short/expanded);
 *     PG_FREE_IF_COPY no-op; DirectFunctionCall2 -> local frame helper;
 *     PG_FUNCTION_INFO_V1 -> plain extern declaration.
 *   - pg_client_to_server -> UTF8 pg_verify_mbstr via the verbatim wfam_
 *     copies in pg_wcharfam.c (encoding pinned PG_UTF8=6 on both sides;
 *     failure raises ERRCODE_CHARACTER_NOT_IN_REPERTOIRE exactly as
 *     report_invalid_encoding does); pg_server_to_client -> identity
 *     (same-encoding send path returns the input pointer unchanged).
 *   - hash_any/hash_any_extended -> the verbatim hashfn.c copies exported by
 *     pg_mac_io.c (pg_hash_bytes/pg_hash_bytes_extended).
 *   - pg_hton32/pg_ntoh32 -> __builtin_bswap32 (little-endian targets only:
 *     macOS/Linux aarch64 + x86_64, the fleet's platforms).
 *   - pvsnprintf -> libc vsnprintf wrapper (only consumer is the \u%%04x
 *     control-char arm of escape_json_char; rendering is identical).
 *   - JsonLexContext reduced to the fields json_lex_number touches with
 *     incremental pinned false (IsValidJsonNumber's dummy_lex = {0} shape).
 *   - Every extern definition is hst_/pg_hst_-prefixed via #define so this
 *     TU cannot collide with other oracle TUs in the fuzz cc build.
 *
 * Driver entries (SECTION D, pg_hst_* prefix) are fuzz plumbing, NOT
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
#include "pg_oracle_guard.h"	/* oracle-serialization holder check */

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
#define StaticAssertDecl(c, m) extern void hst_static_assert_decl(void)
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
static _Thread_local jmp_buf hst_env;
static _Thread_local int hst_sqlstate;

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
hst_raise(void)
{
	longjmp(hst_env, 1);
}

/* Node reduced to the SOFT_ERROR_OCCURRED flag (miscnodes.h protocol) */
typedef struct Node
{
	int			error_occurred;
} Node;

static _Thread_local int hst_soft_sqlstate;

static void
hst_errsave_fire(void *escontext)
{
	if (escontext != NULL)
	{
		((Node *) escontext)->error_occurred = 1;
		hst_soft_sqlstate = hst_sqlstate;
	}
	else
		hst_raise();
}

#define errcode(c) (hst_sqlstate = (c), 0)
#define errmsg(...) 0
#define errmsg_internal(...) 0
#define errdetail(...) 0
#define errhint(...) 0
#define ereport(level, ...) do { (void) (__VA_ARGS__); hst_raise(); } while (0)
#define errsave(escontext, ...) \
	do { (void) (__VA_ARGS__); hst_errsave_fire(escontext); } while (0)
#define ereturn(escontext, dummy_value, ...) \
	do { errsave(escontext, __VA_ARGS__); return dummy_value; } while (0)
#define SOFT_ERROR_OCCURRED(escontext) \
	((escontext) != NULL && ((Node *) (escontext))->error_occurred)
#define elog(level, ...) \
	do { hst_sqlstate = ERRCODE_INTERNAL_ERROR; hst_raise(); } while (0)

/* ---- palloc arena (models per-exec memory context reset) ---- */
static _Thread_local void **hst_allocs;
static _Thread_local size_t hst_nallocs, hst_aallocs;

static void *
hst_track(void *p)
{
	if (hst_nallocs == hst_aallocs)
	{
		hst_aallocs = hst_aallocs ? hst_aallocs * 2 : 1024;
		hst_allocs = realloc(hst_allocs, hst_aallocs * sizeof(void *));
	}
	hst_allocs[hst_nallocs++] = p;
	return p;
}

static void *
hst_palloc(Size sz)
{
	/* mcxt.c parity: palloc enforces MaxAllocSize (elog ERROR path) */
	if (sz > MaxAllocSize)
	{
		hst_sqlstate = ERRCODE_INTERNAL_ERROR;
		hst_raise();
	}
	void	   *p = malloc(sz ? sz : 1);

	if (p == NULL)
	{
		hst_sqlstate = ERRCODE_OUT_OF_MEMORY;
		hst_raise();
	}
	return hst_track(p);
}

static void *
hst_palloc0(Size sz)
{
	void	   *p = hst_palloc(sz);

	memset(p, 0, sz);
	return p;
}

static void *
hst_repalloc(void *p, Size sz)
{
	if (sz > MaxAllocSize)
	{
		hst_sqlstate = ERRCODE_INTERNAL_ERROR;
		hst_raise();
	}
	for (size_t i = hst_nallocs; i-- > 0;)
	{
		if (hst_allocs[i] == p)
		{
			void	   *np = realloc(p, sz);

			if (np == NULL)
			{
				hst_sqlstate = ERRCODE_OUT_OF_MEMORY;
				hst_raise();
			}
			hst_allocs[i] = np;
			return np;
		}
	}
	abort();					/* repalloc of an untracked pointer */
}

static char *
hst_pstrdup(const char *s)
{
	size_t		n = strlen(s) + 1;
	char	   *r = hst_palloc(n);

	memcpy(r, s, n);
	return r;
}

#define palloc(n) hst_palloc(n)
#define palloc0(n) hst_palloc0(n)
#define repalloc(p, n) hst_repalloc((p), (n))
#define pfree(p) ((void) (p))	/* arena-freed at pg_hst_reset */
#define pstrdup(s) hst_pstrdup(s)

/* pvsnprintf -> libc vsnprintf (see header; \u%04x arm only) */
static size_t
hst_pvsnprintf(char *buf, size_t len, const char *fmt, va_list args)
{
	int			n = vsnprintf(buf, len, fmt, args);

	if (n < 0)
		abort();
	return (size_t) n;
}

#define pvsnprintf hst_pvsnprintf

/* ==== symbol prefixing: every extern definition in this TU ==== */
#define pg_strcasecmp			hst_pg_strcasecmp
#define scanner_isspace			hst_scanner_isspace
#define initStringInfo			hst_initStringInfo
#define resetStringInfo			hst_resetStringInfo
#define appendStringInfo		hst_appendStringInfo
#define appendStringInfoVA		hst_appendStringInfoVA
#define appendStringInfoString	hst_appendStringInfoString
#define appendStringInfoChar	hst_appendStringInfoChar
#define appendBinaryStringInfo	hst_appendBinaryStringInfo
#define appendBinaryStringInfoNT hst_appendBinaryStringInfoNT
#define enlargeStringInfo		hst_enlargeStringInfo
#define pq_begintypsend			hst_pq_begintypsend
#define pq_endtypsend			hst_pq_endtypsend
#define pq_sendtext				hst_pq_sendtext
#define pq_getmsgint			hst_pq_getmsgint
#define pq_getmsgbytes			hst_pq_getmsgbytes
#define pq_copymsgbytes			hst_pq_copymsgbytes
#define pq_getmsgtext			hst_pq_getmsgtext
#define cstring_to_text			hst_cstring_to_text
#define cstring_to_text_with_len hst_cstring_to_text_with_len
#define ArrayGetOffset			hst_ArrayGetOffset
#define ArrayGetNItems			hst_ArrayGetNItems
#define ArrayGetNItemsSafe		hst_ArrayGetNItemsSafe
#define ArrayCheckBounds		hst_ArrayCheckBounds
#define ArrayCheckBoundsSafe	hst_ArrayCheckBoundsSafe
#define CopyArrayEls			hst_CopyArrayEls
#define construct_md_array		hst_construct_md_array
#define construct_array			hst_construct_array
#define construct_array_builtin	hst_construct_array_builtin
#define construct_empty_array	hst_construct_empty_array
#define deconstruct_array		hst_deconstruct_array
#define deconstruct_array_builtin hst_deconstruct_array_builtin
#define escape_json				hst_escape_json
#define escape_json_with_len	hst_escape_json_with_len
#define IsValidJsonNumber		hst_IsValidJsonNumber
#define hstoreUniquePairs		hst_hstoreUniquePairs
#define hstoreCheckKeyLen		hst_hstoreCheckKeyLen
#define hstoreCheckValLen		hst_hstoreCheckValLen
#define hstorePairs				hst_hstorePairs
#define hstoreFindKey			hst_hstoreFindKey
#define hstoreArrayToPairs		hst_hstoreArrayToPairs
#define hstore_in				hst_hstore_in
#define hstore_recv				hst_hstore_recv
#define hstore_from_text		hst_hstore_from_text
#define hstore_from_arrays		hst_hstore_from_arrays
#define hstore_from_array		hst_hstore_from_array
#define hstore_out				hst_hstore_out
#define hstore_send				hst_hstore_send
#define hstore_to_json_loose	hst_hstore_to_json_loose
#define hstore_to_json			hst_hstore_to_json
#define hstore_fetchval			hst_hstore_fetchval
#define hstore_exists			hst_hstore_exists
#define hstore_exists_any		hst_hstore_exists_any
#define hstore_exists_all		hst_hstore_exists_all
#define hstore_defined			hst_hstore_defined
#define hstore_delete			hst_hstore_delete
#define hstore_delete_array		hst_hstore_delete_array
#define hstore_delete_hstore	hst_hstore_delete_hstore
#define hstore_concat			hst_hstore_concat
#define hstore_slice_to_array	hst_hstore_slice_to_array
#define hstore_slice_to_hstore	hst_hstore_slice_to_hstore
#define hstore_akeys			hst_hstore_akeys
#define hstore_avals			hst_hstore_avals
#define hstore_to_array			hst_hstore_to_array
#define hstore_to_matrix		hst_hstore_to_matrix
#define hstore_contains			hst_hstore_contains
#define hstore_contained		hst_hstore_contained
#define hstore_cmp				hst_hstore_cmp
#define hstore_eq				hst_hstore_eq
#define hstore_ne				hst_hstore_ne
#define hstore_gt				hst_hstore_gt
#define hstore_ge				hst_hstore_ge
#define hstore_lt				hst_hstore_lt
#define hstore_le				hst_hstore_le
#define hstore_hash				hst_hstore_hash
#define hstore_hash_extended	hst_hstore_hash_extended

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

/* ==== VERBATIM: tupmacs.h 49-76, 135-161, 168-200, 203-232 @ 62d6c7d3df ==== */
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

/* ==== VERBATIM: simd.h whole file @ 62d6c7d3df ==== */
/*-------------------------------------------------------------------------
 *
 * simd.h
 *	  Support for platform-specific vector operations.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/port/simd.h
 *
 * NOTES
 * - VectorN in this file refers to a register where the element operands
 * are N bits wide. The vector width is platform-specific, so users that care
 * about that will need to inspect "sizeof(VectorN)".
 *
 *-------------------------------------------------------------------------
 */
#ifndef SIMD_H
#define SIMD_H

#if (defined(__x86_64__) || defined(_M_AMD64))
/*
 * SSE2 instructions are part of the spec for the 64-bit x86 ISA. We assume
 * that compilers targeting this architecture understand SSE2 intrinsics.
 *
 * We use emmintrin.h rather than the comprehensive header immintrin.h in
 * order to exclude extensions beyond SSE2. This is because MSVC, at least,
 * will allow the use of intrinsics that haven't been enabled at compile
 * time.
 */
#include <emmintrin.h>
#define USE_SSE2
typedef __m128i Vector8;
typedef __m128i Vector32;

#elif defined(__aarch64__) && defined(__ARM_NEON)
/*
 * We use the Neon instructions if the compiler provides access to them (as
 * indicated by __ARM_NEON) and we are on aarch64.  While Neon support is
 * technically optional for aarch64, it appears that all available 64-bit
 * hardware does have it.  Neon exists in some 32-bit hardware too, but we
 * could not realistically use it there without a run-time check, which seems
 * not worth the trouble for now.
 */
#include <arm_neon.h>
#define USE_NEON
typedef uint8x16_t Vector8;
typedef uint32x4_t Vector32;

#else
/*
 * If no SIMD instructions are available, we can in some cases emulate vector
 * operations using bitwise operations on unsigned integers.  Note that many
 * of the functions in this file presently do not have non-SIMD
 * implementations.  In particular, none of the functions involving Vector32
 * are implemented without SIMD since it's likely not worthwhile to represent
 * two 32-bit integers using a uint64.
 */
#define USE_NO_SIMD
typedef uint64 Vector8;
#endif

/* load/store operations */
static inline void vector8_load(Vector8 *v, const uint8 *s);
#ifndef USE_NO_SIMD
static inline void vector32_load(Vector32 *v, const uint32 *s);
#endif

/* assignment operations */
static inline Vector8 vector8_broadcast(const uint8 c);
#ifndef USE_NO_SIMD
static inline Vector32 vector32_broadcast(const uint32 c);
#endif

/* element-wise comparisons to a scalar */
static inline bool vector8_has(const Vector8 v, const uint8 c);
static inline bool vector8_has_zero(const Vector8 v);
static inline bool vector8_has_le(const Vector8 v, const uint8 c);
static inline bool vector8_is_highbit_set(const Vector8 v);
#ifndef USE_NO_SIMD
static inline bool vector32_is_highbit_set(const Vector32 v);
static inline uint32 vector8_highbit_mask(const Vector8 v);
#endif

/* arithmetic operations */
static inline Vector8 vector8_or(const Vector8 v1, const Vector8 v2);
#ifndef USE_NO_SIMD
static inline Vector32 vector32_or(const Vector32 v1, const Vector32 v2);
static inline Vector8 vector8_ssub(const Vector8 v1, const Vector8 v2);
#endif

/*
 * comparisons between vectors
 *
 * Note: These return a vector rather than boolean, which is why we don't
 * have non-SIMD implementations.
 */
#ifndef USE_NO_SIMD
static inline Vector8 vector8_eq(const Vector8 v1, const Vector8 v2);
static inline Vector8 vector8_min(const Vector8 v1, const Vector8 v2);
static inline Vector32 vector32_eq(const Vector32 v1, const Vector32 v2);
#endif

/*
 * Load a chunk of memory into the given vector.
 */
static inline void
vector8_load(Vector8 *v, const uint8 *s)
{
#if defined(USE_SSE2)
	*v = _mm_loadu_si128((const __m128i *) s);
#elif defined(USE_NEON)
	*v = vld1q_u8(s);
#else
	memcpy(v, s, sizeof(Vector8));
#endif
}

#ifndef USE_NO_SIMD
static inline void
vector32_load(Vector32 *v, const uint32 *s)
{
#ifdef USE_SSE2
	*v = _mm_loadu_si128((const __m128i *) s);
#elif defined(USE_NEON)
	*v = vld1q_u32(s);
#endif
}
#endif							/* ! USE_NO_SIMD */

/*
 * Create a vector with all elements set to the same value.
 */
static inline Vector8
vector8_broadcast(const uint8 c)
{
#if defined(USE_SSE2)
	return _mm_set1_epi8(c);
#elif defined(USE_NEON)
	return vdupq_n_u8(c);
#else
	return ~UINT64CONST(0) / 0xFF * c;
#endif
}

#ifndef USE_NO_SIMD
static inline Vector32
vector32_broadcast(const uint32 c)
{
#ifdef USE_SSE2
	return _mm_set1_epi32(c);
#elif defined(USE_NEON)
	return vdupq_n_u32(c);
#endif
}
#endif							/* ! USE_NO_SIMD */

/*
 * Return true if any elements in the vector are equal to the given scalar.
 */
static inline bool
vector8_has(const Vector8 v, const uint8 c)
{
	bool		result;

	/* pre-compute the result for assert checking */
#ifdef USE_ASSERT_CHECKING
	bool		assert_result = false;

	for (Size i = 0; i < sizeof(Vector8); i++)
	{
		if (((const uint8 *) &v)[i] == c)
		{
			assert_result = true;
			break;
		}
	}
#endif							/* USE_ASSERT_CHECKING */

#if defined(USE_NO_SIMD)
	/* any bytes in v equal to c will evaluate to zero via XOR */
	result = vector8_has_zero(v ^ vector8_broadcast(c));
#else
	result = vector8_is_highbit_set(vector8_eq(v, vector8_broadcast(c)));
#endif

	Assert(assert_result == result);
	return result;
}

/*
 * Convenience function equivalent to vector8_has(v, 0)
 */
static inline bool
vector8_has_zero(const Vector8 v)
{
#if defined(USE_NO_SIMD)
	/*
	 * We cannot call vector8_has() here, because that would lead to a
	 * circular definition.
	 */
	return vector8_has_le(v, 0);
#else
	return vector8_has(v, 0);
#endif
}

/*
 * Return true if any elements in the vector are less than or equal to the
 * given scalar.
 */
static inline bool
vector8_has_le(const Vector8 v, const uint8 c)
{
	bool		result = false;

	/* pre-compute the result for assert checking */
#ifdef USE_ASSERT_CHECKING
	bool		assert_result = false;

	for (Size i = 0; i < sizeof(Vector8); i++)
	{
		if (((const uint8 *) &v)[i] <= c)
		{
			assert_result = true;
			break;
		}
	}
#endif							/* USE_ASSERT_CHECKING */

#if defined(USE_NO_SIMD)

	/*
	 * To find bytes <= c, we can use bitwise operations to find bytes < c+1,
	 * but it only works if c+1 <= 128 and if the highest bit in v is not set.
	 * Adapted from
	 * https://graphics.stanford.edu/~seander/bithacks.html#HasLessInWord
	 */
	if ((int64) v >= 0 && c < 0x80)
		result = (v - vector8_broadcast(c + 1)) & ~v & vector8_broadcast(0x80);
	else
	{
		/* one byte at a time */
		for (Size i = 0; i < sizeof(Vector8); i++)
		{
			if (((const uint8 *) &v)[i] <= c)
			{
				result = true;
				break;
			}
		}
	}
#else

	/*
	 * Use saturating subtraction to find bytes <= c, which will present as
	 * NUL bytes.  This approach is a workaround for the lack of unsigned
	 * comparison instructions on some architectures.
	 */
	result = vector8_has_zero(vector8_ssub(v, vector8_broadcast(c)));
#endif

	Assert(assert_result == result);
	return result;
}

/*
 * Return true if the high bit of any element is set
 */
static inline bool
vector8_is_highbit_set(const Vector8 v)
{
#ifdef USE_SSE2
	return _mm_movemask_epi8(v) != 0;
#elif defined(USE_NEON)
	return vmaxvq_u8(v) > 0x7F;
#else
	return v & vector8_broadcast(0x80);
#endif
}

/*
 * Exactly like vector8_is_highbit_set except for the input type, so it
 * looks at each byte separately.
 *
 * XXX x86 uses the same underlying type for 8-bit, 16-bit, and 32-bit
 * integer elements, but Arm does not, hence the need for a separate
 * function. We could instead adopt the behavior of Arm's vmaxvq_u32(), i.e.
 * check each 32-bit element, but that would require an additional mask
 * operation on x86.
 */
#ifndef USE_NO_SIMD
static inline bool
vector32_is_highbit_set(const Vector32 v)
{
#if defined(USE_NEON)
	return vector8_is_highbit_set((Vector8) v);
#else
	return vector8_is_highbit_set(v);
#endif
}
#endif							/* ! USE_NO_SIMD */

/*
 * Return a bitmask formed from the high-bit of each element.
 */
#ifndef USE_NO_SIMD
static inline uint32
vector8_highbit_mask(const Vector8 v)
{
#ifdef USE_SSE2
	return (uint32) _mm_movemask_epi8(v);
#elif defined(USE_NEON)
	/*
	 * Note: It would be faster to use vget_lane_u64 and vshrn_n_u16, but that
	 * returns a uint64, making it inconvenient to combine mask values from
	 * multiple vectors.
	 */
	static const uint8 mask[16] = {
		1 << 0, 1 << 1, 1 << 2, 1 << 3,
		1 << 4, 1 << 5, 1 << 6, 1 << 7,
		1 << 0, 1 << 1, 1 << 2, 1 << 3,
		1 << 4, 1 << 5, 1 << 6, 1 << 7,
	};

	uint8x16_t	masked = vandq_u8(vld1q_u8(mask), (uint8x16_t) vshrq_n_s8((int8x16_t) v, 7));
	uint8x16_t	maskedhi = vextq_u8(masked, masked, 8);

	return (uint32) vaddvq_u16((uint16x8_t) vzip1q_u8(masked, maskedhi));
#endif
}
#endif							/* ! USE_NO_SIMD */

/*
 * Return the bitwise OR of the inputs
 */
static inline Vector8
vector8_or(const Vector8 v1, const Vector8 v2)
{
#ifdef USE_SSE2
	return _mm_or_si128(v1, v2);
#elif defined(USE_NEON)
	return vorrq_u8(v1, v2);
#else
	return v1 | v2;
#endif
}

#ifndef USE_NO_SIMD
static inline Vector32
vector32_or(const Vector32 v1, const Vector32 v2)
{
#ifdef USE_SSE2
	return _mm_or_si128(v1, v2);
#elif defined(USE_NEON)
	return vorrq_u32(v1, v2);
#endif
}
#endif							/* ! USE_NO_SIMD */

/*
 * Return the result of subtracting the respective elements of the input
 * vectors using saturation (i.e., if the operation would yield a value less
 * than zero, zero is returned instead).  For more information on saturation
 * arithmetic, see https://en.wikipedia.org/wiki/Saturation_arithmetic
 */
#ifndef USE_NO_SIMD
static inline Vector8
vector8_ssub(const Vector8 v1, const Vector8 v2)
{
#ifdef USE_SSE2
	return _mm_subs_epu8(v1, v2);
#elif defined(USE_NEON)
	return vqsubq_u8(v1, v2);
#endif
}
#endif							/* ! USE_NO_SIMD */

/*
 * Return a vector with all bits set in each lane where the corresponding
 * lanes in the inputs are equal.
 */
#ifndef USE_NO_SIMD
static inline Vector8
vector8_eq(const Vector8 v1, const Vector8 v2)
{
#ifdef USE_SSE2
	return _mm_cmpeq_epi8(v1, v2);
#elif defined(USE_NEON)
	return vceqq_u8(v1, v2);
#endif
}
#endif							/* ! USE_NO_SIMD */

#ifndef USE_NO_SIMD
static inline Vector32
vector32_eq(const Vector32 v1, const Vector32 v2)
{
#ifdef USE_SSE2
	return _mm_cmpeq_epi32(v1, v2);
#elif defined(USE_NEON)
	return vceqq_u32(v1, v2);
#endif
}
#endif							/* ! USE_NO_SIMD */

/*
 * Given two vectors, return a vector with the minimum element of each.
 */
#ifndef USE_NO_SIMD
static inline Vector8
vector8_min(const Vector8 v1, const Vector8 v2)
{
#ifdef USE_SSE2
	return _mm_min_epu8(v1, v2);
#elif defined(USE_NEON)
	return vminq_u8(v1, v2);
#endif
}
#endif							/* ! USE_NO_SIMD */

#endif							/* SIMD_H */

/* ==== VERBATIM: sort_template.h whole file, instantiated as port/qsort.c
 * does (ST_SORT/ST_ELEMENT_TYPE_VOID/ST_COMPARE_RUNTIME_POINTER; ST_SCOPE
 * static to keep it TU-local) @ 62d6c7d3df ==== */
#define ST_SORT hst_pg_qsort
#define ST_ELEMENT_TYPE_VOID
#define ST_COMPARE_RUNTIME_POINTER
#define ST_SCOPE static
#define ST_DECLARE
#define ST_DEFINE
/*-------------------------------------------------------------------------
 *
 * sort_template.h
 *
 *	  A template for a sort algorithm that supports varying degrees of
 *	  specialization.
 *
 * Copyright (c) 2021-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1992-1994, Regents of the University of California
 *
 * Usage notes:
 *
 *	  To generate functions specialized for a type, the following parameter
 *	  macros should be #define'd before this file is included.
 *
 *	  - ST_SORT - the name of a sort function to be generated
 *	  - ST_ELEMENT_TYPE - type of the referenced elements
 *	  - ST_DECLARE - if defined the functions and types are declared
 *	  - ST_DEFINE - if defined the functions and types are defined
 *	  - ST_SCOPE - scope (e.g. extern, static inline) for functions
 *	  - ST_CHECK_FOR_INTERRUPTS - if defined the sort is interruptible
 *
 *	  Instead of ST_ELEMENT_TYPE, ST_ELEMENT_TYPE_VOID can be defined.  Then
 *	  the generated functions will automatically gain an "element_size"
 *	  parameter.  This allows us to generate a traditional qsort function.
 *
 *	  One of the following macros must be defined, to show how to compare
 *	  elements.  The first two options are arbitrary expressions depending
 *	  on whether an extra pass-through argument is desired, and the third
 *	  option should be defined if the sort function should receive a
 *	  function pointer at runtime.
 *
 *	  - ST_COMPARE(a, b) - a simple comparison expression
 *	  - ST_COMPARE(a, b, arg) - variant that takes an extra argument
 *	  - ST_COMPARE_RUNTIME_POINTER - sort function takes a function pointer
 *
 *	  NB: If the comparator function is inlined, some compilers may produce
 *	  worse code with the optimized comparison routines in common/int.h than
 *	  with code with the following form:
 *
 *	      if (a < b)
 *	          return -1;
 *	      if (a > b)
 *	          return 1;
 *	      return 0;
 *
 *	  To say that the comparator and therefore also sort function should
 *	  receive an extra pass-through argument, specify the type of the
 *	  argument.
 *
 *	  - ST_COMPARE_ARG_TYPE - type of extra argument
 *
 *	  The prototype of the generated sort function is:
 *
 *	  void ST_SORT(ST_ELEMENT_TYPE *data, size_t n,
 *				   [size_t element_size,]
 *				   [ST_SORT_compare_function compare,]
 *				   [ST_COMPARE_ARG_TYPE *arg]);
 *
 *	  ST_SORT_compare_function is a function pointer of the following type:
 *
 *	  int (*)(const ST_ELEMENT_TYPE *a, const ST_ELEMENT_TYPE *b,
 *			  [ST_COMPARE_ARG_TYPE *arg])
 *
 * HISTORY
 *
 *	  Modifications from vanilla NetBSD source:
 *	  - Add do ... while() macro fix
 *	  - Remove __inline, _DIAGASSERTs, __P
 *	  - Remove ill-considered "swap_cnt" switch to insertion sort, in favor
 *		of a simple check for presorted input.
 *	  - Take care to recurse on the smaller partition, to bound stack usage
 *	  - Convert into a header that can generate specialized functions
 *
 * IDENTIFICATION
 *		src/include/lib/sort_template.h
 *
 *-------------------------------------------------------------------------
 */

/*	  $NetBSD: qsort.c,v 1.13 2003/08/07 16:43:42 agc Exp $   */

/*-
 * Copyright (c) 1992, 1993
 *	  The Regents of the University of California.  All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *	  notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *	  notice, this list of conditions and the following disclaimer in the
 *	  documentation and/or other materials provided with the distribution.
 * 3. Neither the name of the University nor the names of its contributors
 *	  may be used to endorse or promote products derived from this software
 *	  without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE REGENTS AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

/*
 * Qsort routine based on J. L. Bentley and M. D. McIlroy,
 * "Engineering a sort function",
 * Software--Practice and Experience 23 (1993) 1249-1265.
 *
 * We have modified their original by adding a check for already-sorted
 * input, which seems to be a win per discussions on pgsql-hackers around
 * 2006-03-21.
 *
 * Also, we recurse on the smaller partition and iterate on the larger one,
 * which ensures we cannot recurse more than log(N) levels (since the
 * partition recursed to is surely no more than half of the input).  Bentley
 * and McIlroy explicitly rejected doing this on the grounds that it's "not
 * worth the effort", but we have seen crashes in the field due to stack
 * overrun, so that judgment seems wrong.
 */

#define ST_MAKE_PREFIX(a) CppConcat(a,_)
#define ST_MAKE_NAME(a,b) ST_MAKE_NAME_(ST_MAKE_PREFIX(a),b)
#define ST_MAKE_NAME_(a,b) CppConcat(a,b)

/*
 * If the element type is void, we'll also need an element_size argument
 * because we don't know the size.
 */
#ifdef ST_ELEMENT_TYPE_VOID
#define ST_ELEMENT_TYPE void
#define ST_SORT_PROTO_ELEMENT_SIZE , size_t element_size
#define ST_SORT_INVOKE_ELEMENT_SIZE , element_size
#else
#define ST_SORT_PROTO_ELEMENT_SIZE
#define ST_SORT_INVOKE_ELEMENT_SIZE
#endif

/*
 * If the user wants to be able to pass in compare functions at runtime,
 * we'll need to make that an argument of the sort and med3 functions.
 */
#ifdef ST_COMPARE_RUNTIME_POINTER
/*
 * The type of the comparator function pointer that ST_SORT will take, unless
 * you've already declared a type name manually and want to use that instead of
 * having a new one defined.
 */
#ifndef ST_COMPARATOR_TYPE_NAME
#define ST_COMPARATOR_TYPE_NAME ST_MAKE_NAME(ST_SORT, compare_function)
#endif
#define ST_COMPARE compare
#ifndef ST_COMPARE_ARG_TYPE
#define ST_SORT_PROTO_COMPARE , ST_COMPARATOR_TYPE_NAME compare
#define ST_SORT_INVOKE_COMPARE , compare
#else
#define ST_SORT_PROTO_COMPARE , ST_COMPARATOR_TYPE_NAME compare
#define ST_SORT_INVOKE_COMPARE , compare
#endif
#else
#define ST_SORT_PROTO_COMPARE
#define ST_SORT_INVOKE_COMPARE
#endif

/*
 * If the user wants to use a compare function or expression that takes an
 * extra argument, we'll need to make that an argument of the sort, compare and
 * med3 functions.
 */
#ifdef ST_COMPARE_ARG_TYPE
#define ST_SORT_PROTO_ARG , ST_COMPARE_ARG_TYPE *arg
#define ST_SORT_INVOKE_ARG , arg
#else
#define ST_SORT_PROTO_ARG
#define ST_SORT_INVOKE_ARG
#endif

#ifdef ST_DECLARE

#ifdef ST_COMPARE_RUNTIME_POINTER
typedef int (*ST_COMPARATOR_TYPE_NAME) (const ST_ELEMENT_TYPE *,
										const ST_ELEMENT_TYPE * ST_SORT_PROTO_ARG);
#endif

/* Declare the sort function.  Note optional arguments at end. */
ST_SCOPE void ST_SORT(ST_ELEMENT_TYPE * first, size_t n
					  ST_SORT_PROTO_ELEMENT_SIZE
					  ST_SORT_PROTO_COMPARE
					  ST_SORT_PROTO_ARG);

#endif

#ifdef ST_DEFINE

/* sort private helper functions */
#define ST_MED3 ST_MAKE_NAME(ST_SORT, med3)
#define ST_SWAP ST_MAKE_NAME(ST_SORT, swap)
#define ST_SWAPN ST_MAKE_NAME(ST_SORT, swapn)

/* Users expecting to run very large sorts may need them to be interruptible. */
#ifdef ST_CHECK_FOR_INTERRUPTS
#define DO_CHECK_FOR_INTERRUPTS() CHECK_FOR_INTERRUPTS()
#else
#define DO_CHECK_FOR_INTERRUPTS()
#endif

/*
 * Create wrapper macros that know how to invoke compare, med3 and sort with
 * the right arguments.
 */
#ifdef ST_COMPARE_RUNTIME_POINTER
#define DO_COMPARE(a_, b_) ST_COMPARE((a_), (b_) ST_SORT_INVOKE_ARG)
#elif defined(ST_COMPARE_ARG_TYPE)
#define DO_COMPARE(a_, b_) ST_COMPARE((a_), (b_), arg)
#else
#define DO_COMPARE(a_, b_) ST_COMPARE((a_), (b_))
#endif
#define DO_MED3(a_, b_, c_)												\
	ST_MED3((a_), (b_), (c_)											\
			ST_SORT_INVOKE_COMPARE										\
			ST_SORT_INVOKE_ARG)
#define DO_SORT(a_, n_)													\
	ST_SORT((a_), (n_)													\
			ST_SORT_INVOKE_ELEMENT_SIZE									\
			ST_SORT_INVOKE_COMPARE										\
			ST_SORT_INVOKE_ARG)

/*
 * If we're working with void pointers, we'll use pointer arithmetic based on
 * uint8, and use the runtime element_size to step through the array and swap
 * elements.  Otherwise we'll work with ST_ELEMENT_TYPE.
 */
#ifndef ST_ELEMENT_TYPE_VOID
#define ST_POINTER_TYPE ST_ELEMENT_TYPE
#define ST_POINTER_STEP 1
#define DO_SWAPN(a_, b_, n_) ST_SWAPN((a_), (b_), (n_))
#define DO_SWAP(a_, b_) ST_SWAP((a_), (b_))
#else
#define ST_POINTER_TYPE uint8
#define ST_POINTER_STEP element_size
#define DO_SWAPN(a_, b_, n_) ST_SWAPN((a_), (b_), (n_))
#define DO_SWAP(a_, b_) DO_SWAPN((a_), (b_), element_size)
#endif

/*
 * Find the median of three values.  Currently, performance seems to be best
 * if the comparator is inlined here, but the med3 function is not inlined
 * in the qsort function.
 *
 * Refer to the comment at the top of this file for known caveats to consider
 * when writing inlined comparator functions.
 */
static pg_noinline ST_ELEMENT_TYPE *
ST_MED3(ST_ELEMENT_TYPE * a,
		ST_ELEMENT_TYPE * b,
		ST_ELEMENT_TYPE * c
		ST_SORT_PROTO_COMPARE
		ST_SORT_PROTO_ARG)
{
	return DO_COMPARE(a, b) < 0 ?
		(DO_COMPARE(b, c) < 0 ? b : (DO_COMPARE(a, c) < 0 ? c : a))
		: (DO_COMPARE(b, c) > 0 ? b : (DO_COMPARE(a, c) < 0 ? a : c));
}

static inline void
ST_SWAP(ST_POINTER_TYPE * a, ST_POINTER_TYPE * b)
{
	ST_POINTER_TYPE tmp = *a;

	*a = *b;
	*b = tmp;
}

static inline void
ST_SWAPN(ST_POINTER_TYPE * a, ST_POINTER_TYPE * b, size_t n)
{
	for (size_t i = 0; i < n; ++i)
		ST_SWAP(&a[i], &b[i]);
}

/*
 * Sort an array.
 */
ST_SCOPE void
ST_SORT(ST_ELEMENT_TYPE * data, size_t n
		ST_SORT_PROTO_ELEMENT_SIZE
		ST_SORT_PROTO_COMPARE
		ST_SORT_PROTO_ARG)
{
	ST_POINTER_TYPE *a = (ST_POINTER_TYPE *) data,
			   *pa,
			   *pb,
			   *pc,
			   *pd,
			   *pl,
			   *pm,
			   *pn;
	size_t		d1,
				d2;
	int			r,
				presorted;

loop:
	DO_CHECK_FOR_INTERRUPTS();
	if (n < 7)
	{
		for (pm = a + ST_POINTER_STEP; pm < a + n * ST_POINTER_STEP;
			 pm += ST_POINTER_STEP)
			for (pl = pm; pl > a && DO_COMPARE(pl - ST_POINTER_STEP, pl) > 0;
				 pl -= ST_POINTER_STEP)
				DO_SWAP(pl, pl - ST_POINTER_STEP);
		return;
	}
	presorted = 1;
	for (pm = a + ST_POINTER_STEP; pm < a + n * ST_POINTER_STEP;
		 pm += ST_POINTER_STEP)
	{
		DO_CHECK_FOR_INTERRUPTS();
		if (DO_COMPARE(pm - ST_POINTER_STEP, pm) > 0)
		{
			presorted = 0;
			break;
		}
	}
	if (presorted)
		return;
	pm = a + (n / 2) * ST_POINTER_STEP;
	if (n > 7)
	{
		pl = a;
		pn = a + (n - 1) * ST_POINTER_STEP;
		if (n > 40)
		{
			size_t		d = (n / 8) * ST_POINTER_STEP;

			pl = DO_MED3(pl, pl + d, pl + 2 * d);
			pm = DO_MED3(pm - d, pm, pm + d);
			pn = DO_MED3(pn - 2 * d, pn - d, pn);
		}
		pm = DO_MED3(pl, pm, pn);
	}
	DO_SWAP(a, pm);
	pa = pb = a + ST_POINTER_STEP;
	pc = pd = a + (n - 1) * ST_POINTER_STEP;
	for (;;)
	{
		while (pb <= pc && (r = DO_COMPARE(pb, a)) <= 0)
		{
			if (r == 0)
			{
				DO_SWAP(pa, pb);
				pa += ST_POINTER_STEP;
			}
			pb += ST_POINTER_STEP;
			DO_CHECK_FOR_INTERRUPTS();
		}
		while (pb <= pc && (r = DO_COMPARE(pc, a)) >= 0)
		{
			if (r == 0)
			{
				DO_SWAP(pc, pd);
				pd -= ST_POINTER_STEP;
			}
			pc -= ST_POINTER_STEP;
			DO_CHECK_FOR_INTERRUPTS();
		}
		if (pb > pc)
			break;
		DO_SWAP(pb, pc);
		pb += ST_POINTER_STEP;
		pc -= ST_POINTER_STEP;
	}
	pn = a + n * ST_POINTER_STEP;
	d1 = Min(pa - a, pb - pa);
	DO_SWAPN(a, pb - d1, d1);
	d1 = Min(pd - pc, pn - pd - ST_POINTER_STEP);
	DO_SWAPN(pb, pn - d1, d1);
	d1 = pb - pa;
	d2 = pd - pc;
	if (d1 <= d2)
	{
		/* Recurse on left partition, then iterate on right partition */
		if (d1 > ST_POINTER_STEP)
			DO_SORT(a, d1 / ST_POINTER_STEP);
		if (d2 > ST_POINTER_STEP)
		{
			/* Iterate rather than recurse to save stack space */
			/* DO_SORT(pn - d2, d2 / ST_POINTER_STEP) */
			a = pn - d2;
			n = d2 / ST_POINTER_STEP;
			goto loop;
		}
	}
	else
	{
		/* Recurse on right partition, then iterate on left partition */
		if (d2 > ST_POINTER_STEP)
			DO_SORT(pn - d2, d2 / ST_POINTER_STEP);
		if (d1 > ST_POINTER_STEP)
		{
			/* Iterate rather than recurse to save stack space */
			/* DO_SORT(a, d1 / ST_POINTER_STEP) */
			n = d1 / ST_POINTER_STEP;
			goto loop;
		}
	}
}
#endif

#undef DO_CHECK_FOR_INTERRUPTS
#undef DO_COMPARE
#undef DO_MED3
#undef DO_SORT
#undef DO_SWAP
#undef DO_SWAPN
#undef ST_CHECK_FOR_INTERRUPTS
#undef ST_COMPARATOR_TYPE_NAME
#undef ST_COMPARE
#undef ST_COMPARE_ARG_TYPE
#undef ST_COMPARE_RUNTIME_POINTER
#undef ST_ELEMENT_TYPE
#undef ST_ELEMENT_TYPE_VOID
#undef ST_MAKE_NAME
#undef ST_MAKE_NAME_
#undef ST_MAKE_PREFIX
#undef ST_MED3
#undef ST_POINTER_STEP
#undef ST_POINTER_TYPE
#undef ST_SCOPE
#undef ST_SORT
#undef ST_SORT_INVOKE_ARG
#undef ST_SORT_INVOKE_COMPARE
#undef ST_SORT_INVOKE_ELEMENT_SIZE
#undef ST_SORT_PROTO_ARG
#undef ST_SORT_PROTO_COMPARE
#undef ST_SORT_PROTO_ELEMENT_SIZE
#undef ST_SWAP
#undef ST_SWAPN

/* the backend's qsort IS pg_qsort: port.h line 478 @ 62d6c7d3df */
#define qsort(a,b,c,d) hst_pg_qsort(a,b,c,d)

/* ==== VERBATIM: pg_strcasecmp (pgstrcasecmp.c 32-62 @ 62d6c7d3df) ==== */
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

/* ==== VERBATIM: scanner_isspace (scansup.c @ 62d6c7d3df) ==== */
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
#define HST_PG_UTF8 6			/* pg_wchar.h enum pg_enc PG_UTF8 */

static char *
hst_client_to_server(const char *s, int len)
{
	/* pg_any_to_server, same-encoding arm (mbutils.c): verify, return s */
	if (!wfam_pg_verify_mbstr(HST_PG_UTF8, s, len, true))
	{
		/* report_invalid_encoding's sqlstate (mbutils.c) */
		hst_sqlstate = ERRCODE_CHARACTER_NOT_IN_REPERTOIRE;
		hst_raise();
	}
	return (char *) s;
}

#define pg_client_to_server(s, len) hst_client_to_server((s), (len))
#define pg_server_to_client(s, len) ((char *) (s))

/* pg_bswap.h on little-endian targets */
#define pg_hton32(x) __builtin_bswap32(x)
#define pg_ntoh32(x) __builtin_bswap32(x)
#define pg_ntoh16(x) __builtin_bswap16(x)

/* ==== VERBATIM: pq_writeint32 + pq_sendint32 (pqformat.h @ 62d6c7d3df) ==== */
static inline void
pq_writeint32(StringInfoData *pg_restrict buf, uint32 i)
{
	uint32		ni = pg_hton32(i);

	Assert(buf->len + (int) sizeof(uint32) <= buf->maxlen);
	memcpy((char *pg_restrict) (buf->data + buf->len), &ni, sizeof(uint32));
	buf->len += sizeof(uint32);
}

static inline void
pq_sendint32(StringInfo buf, uint32 i)
{
	enlargeStringInfo(buf, sizeof(uint32));
	pq_writeint32(buf, i);
}


/* forward decls for the pasted bodies (definition order differs from the
 * original files' internal ordering) */
extern void pq_copymsgbytes(StringInfo msg, void *buf, int datalen);
extern text *cstring_to_text_with_len(const char *s, int len);
extern int	ArrayGetNItemsSafe(int ndim, const int *dims, struct Node *escontext);
extern bool ArrayCheckBoundsSafe(int ndim, const int *dims, const int *lb, struct Node *escontext);
extern ArrayType *construct_empty_array(Oid elmtype);
extern void CopyArrayEls(ArrayType *array, Datum *values, bool *nulls, int nitems, int typlen, bool typbyval, char typalign, bool freedata);
extern void deconstruct_array(ArrayType *array, Oid elmtype, int elmlen, bool elmbyval, char elmalign, Datum **elemsp, bool **nullsp, int *nelemsp);
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

/* ==== VERBATIM: arrayfuncs.c bodies @ 62d6c7d3df ==== */
static int	ArrayCastAndSet(Datum src, int typlen, bool typbyval, char typalign, char *dest);

static Datum
ArrayCast(char *value, bool byval, int len)
{
	return fetch_att(value, byval, len);
}

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

/* ---- jsonapi shims: JsonLexContext reduced to the json_lex_number fields
 * (dummy_lex = {0} shape; incremental pinned false) ---- */
typedef enum
{
	JSON_SUCCESS = 0,
	JSON_INCOMPLETE,
	JSON_INVALID_TOKEN
} JsonParseErrorType;

typedef struct JsonIncrementalState JsonIncrementalState;
typedef struct JsonLexContext
{
	const char *input;
	size_t		input_length;
	const char *token_start;
	const char *token_terminator;
	const char *prev_token_terminator;
	bool		incremental;
	JsonIncrementalState *inc_state;
} JsonLexContext;

struct JsonIncrementalState
{
	bool		is_last_chunk;
};

#define jsonapi_appendBinaryStringInfo(a, b, c) abort()	/* incremental=false */

/* ==== VERBATIM: JSON_ALPHANUMERIC_CHAR (jsonapi.c 325-331 @ 62d6c7d3df) ==== */
/* chars to consider as part of an alphanumeric token */
#define JSON_ALPHANUMERIC_CHAR(c)  \
	(((c) >= 'a' && (c) <= 'z') || \
	 ((c) >= 'A' && (c) <= 'Z') || \
	 ((c) >= '0' && (c) <= '9') || \
	 (c) == '_' || \
	 IS_HIGHBIT_SET(c))

/* ==== VERBATIM: json_lex_number (jsonapi.c @ 62d6c7d3df) ==== */
static inline JsonParseErrorType
json_lex_number(JsonLexContext *lex, const char *s,
				bool *num_err, size_t *total_len)
{
	bool		error = false;
	int			len = s - lex->input;

	/* Part (1): leading sign indicator. */
	/* Caller already did this for us; so do nothing. */

	/* Part (2): parse main digit string. */
	if (len < lex->input_length && *s == '0')
	{
		s++;
		len++;
	}
	else if (len < lex->input_length && *s >= '1' && *s <= '9')
	{
		do
		{
			s++;
			len++;
		} while (len < lex->input_length && *s >= '0' && *s <= '9');
	}
	else
		error = true;

	/* Part (3): parse optional decimal portion. */
	if (len < lex->input_length && *s == '.')
	{
		s++;
		len++;
		if (len == lex->input_length || *s < '0' || *s > '9')
			error = true;
		else
		{
			do
			{
				s++;
				len++;
			} while (len < lex->input_length && *s >= '0' && *s <= '9');
		}
	}

	/* Part (4): parse optional exponent. */
	if (len < lex->input_length && (*s == 'e' || *s == 'E'))
	{
		s++;
		len++;
		if (len < lex->input_length && (*s == '+' || *s == '-'))
		{
			s++;
			len++;
		}
		if (len == lex->input_length || *s < '0' || *s > '9')
			error = true;
		else
		{
			do
			{
				s++;
				len++;
			} while (len < lex->input_length && *s >= '0' && *s <= '9');
		}
	}

	/*
	 * Check for trailing garbage.  As in json_lex(), any alphanumeric stuff
	 * here should be considered part of the token for error-reporting
	 * purposes.
	 */
	for (; len < lex->input_length && JSON_ALPHANUMERIC_CHAR(*s); s++, len++)
		error = true;

	if (total_len != NULL)
		*total_len = len;

	if (lex->incremental && !lex->inc_state->is_last_chunk &&
		len >= lex->input_length)
	{
		jsonapi_appendBinaryStringInfo(&lex->inc_state->partial_token,
									   lex->token_start, s - lex->token_start);
		if (num_err != NULL)
			*num_err = error;

		return JSON_INCOMPLETE;
	}
	else if (num_err != NULL)
	{
		/* let the caller handle any error */
		*num_err = error;
	}
	else
	{
		/* return token endpoint */
		lex->prev_token_terminator = lex->token_terminator;
		lex->token_terminator = s;
		/* handle error if any */
		if (error)
			return JSON_INVALID_TOKEN;
	}

	return JSON_SUCCESS;
}

/* ==== VERBATIM: IsValidJsonNumber (jsonapi.c @ 62d6c7d3df) ==== */
bool
IsValidJsonNumber(const char *str, size_t len)
{
	bool		numeric_error;
	size_t		total_len;
	JsonLexContext dummy_lex = {0};

	if (len <= 0)
		return false;

	/*
	 * json_lex_number expects a leading  '-' to have been eaten already.
	 *
	 * having to cast away the constness of str is ugly, but there's not much
	 * easy alternative.
	 */
	if (*str == '-')
	{
		dummy_lex.input = str + 1;
		dummy_lex.input_length = len - 1;
	}
	else
	{
		dummy_lex.input = str;
		dummy_lex.input_length = len;
	}

	dummy_lex.token_start = dummy_lex.input;

	json_lex_number(&dummy_lex, dummy_lex.input, &numeric_error, &total_len);

	return (!numeric_error) && (total_len == dummy_lex.input_length);
}

/* ==== VERBATIM: json.c escape family @ 62d6c7d3df ==== */
static pg_attribute_always_inline void
escape_json_char(StringInfo buf, char c)
{
	switch (c)
	{
		case '\b':
			appendStringInfoString(buf, "\\b");
			break;
		case '\f':
			appendStringInfoString(buf, "\\f");
			break;
		case '\n':
			appendStringInfoString(buf, "\\n");
			break;
		case '\r':
			appendStringInfoString(buf, "\\r");
			break;
		case '\t':
			appendStringInfoString(buf, "\\t");
			break;
		case '"':
			appendStringInfoString(buf, "\\\"");
			break;
		case '\\':
			appendStringInfoString(buf, "\\\\");
			break;
		default:
			if ((unsigned char) c < ' ')
				appendStringInfo(buf, "\\u%04x", (int) c);
			else
				appendStringInfoCharMacro(buf, c);
			break;
	}
}

void
escape_json(StringInfo buf, const char *str)
{
	appendStringInfoCharMacro(buf, '"');

	for (; *str != '\0'; str++)
		escape_json_char(buf, *str);

	appendStringInfoCharMacro(buf, '"');
}

#define ESCAPE_JSON_FLUSH_AFTER 512	/* json.c line 1622 @ 62d6c7d3df */
void
escape_json_with_len(StringInfo buf, const char *str, int len)
{
	int			vlen;

	Assert(len >= 0);

	/*
	 * Since we know the minimum length we'll need to append, let's just
	 * enlarge the buffer now rather than incrementally making more space when
	 * we run out.  Add two extra bytes for the enclosing quotes.
	 */
	enlargeStringInfo(buf, len + 2);

	/*
	 * Figure out how many bytes to process using SIMD.  Round 'len' down to
	 * the previous multiple of sizeof(Vector8), assuming that's a power-of-2.
	 */
	vlen = len & (int) (~(sizeof(Vector8) - 1));

	appendStringInfoCharMacro(buf, '"');

	for (int i = 0, copypos = 0;;)
	{
		/*
		 * To speed this up, try searching sizeof(Vector8) bytes at once for
		 * special characters that we need to escape.  When we find one, we
		 * fall out of the Vector8 loop and copy the portion we've vector
		 * searched and then we process sizeof(Vector8) bytes one byte at a
		 * time.  Once done, come back and try doing vector searching again.
		 * We'll also process any remaining bytes at the tail end of the
		 * string byte-by-byte.  This optimization assumes that most chunks of
		 * sizeof(Vector8) bytes won't contain any special characters.
		 */
		for (; i < vlen; i += sizeof(Vector8))
		{
			Vector8		chunk;

			vector8_load(&chunk, (const uint8 *) &str[i]);

			/*
			 * Break on anything less than ' ' or if we find a '"' or '\\'.
			 * Those need special handling.  That's done in the per-byte loop.
			 */
			if (vector8_has_le(chunk, (unsigned char) 0x1F) ||
				vector8_has(chunk, (unsigned char) '"') ||
				vector8_has(chunk, (unsigned char) '\\'))
				break;

#ifdef ESCAPE_JSON_FLUSH_AFTER

			/*
			 * Flush what's been checked so far out to the destination buffer
			 * every so often to avoid having to re-read cachelines when
			 * escaping large strings.
			 */
			if (i - copypos >= ESCAPE_JSON_FLUSH_AFTER)
			{
				appendBinaryStringInfo(buf, &str[copypos], i - copypos);
				copypos = i;
			}
#endif
		}

		/*
		 * Write to the destination up to the point that we've vector searched
		 * so far.  Do this only when switching into per-byte mode rather than
		 * once every sizeof(Vector8) bytes.
		 */
		if (copypos < i)
		{
			appendBinaryStringInfo(buf, &str[copypos], i - copypos);
			copypos = i;
		}

		/*
		 * Per-byte loop for Vector8s containing special chars and for
		 * processing the tail of the string.
		 */
		for (int b = 0; b < sizeof(Vector8); b++)
		{
			/* check if we've finished */
			if (i == len)
				goto done;

			Assert(i < len);

			escape_json_char(buf, str[i++]);
		}

		copypos = i;
		/* We're not done yet.  Try the vector search again. */
	}

done:
	appendStringInfoCharMacro(buf, '"');
}

/* ---- fmgr shim (rowtypes-oracle precedent; plumbing only) ---- */
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
	NullableDatum args[2];
} FunctionCallInfoBaseData;

typedef FunctionCallInfoBaseData *FunctionCallInfo;

#define PG_FUNCTION_ARGS FunctionCallInfo fcinfo
#define PG_FUNCTION_INFO_V1(f) extern Datum f(FunctionCallInfo fcinfo)
#define PG_GETARG_DATUM(n)	 (fcinfo->args[n].value)
#define PG_ARGISNULL(n)		 (fcinfo->args[n].isnull)
#define PG_GETARG_CSTRING(n) DatumGetCString(PG_GETARG_DATUM(n))
#define PG_GETARG_POINTER(n) ((void *) DatumGetPointer(PG_GETARG_DATUM(n)))
#define PG_GETARG_INT64(n)	 DatumGetInt64(PG_GETARG_DATUM(n))
#define PG_GETARG_TEXT_PP(n) ((text *) PG_DETOAST_DATUM_PACKED(PG_GETARG_DATUM(n)))
#define PG_GETARG_ARRAYTYPE_P(n) ((ArrayType *) PG_DETOAST_DATUM(PG_GETARG_DATUM(n)))
#define PG_RETURN_DATUM(x)	 return (x)
#define PG_RETURN_POINTER(x) return PointerGetDatum(x)
#define PG_RETURN_CSTRING(x) return CStringGetDatum(x)
#define PG_RETURN_BOOL(x)	 return BoolGetDatum(x)
#define PG_RETURN_INT32(x)	 return Int32GetDatum(x)
#define PG_RETURN_TEXT_P(x)	 return PointerGetDatum(x)
#define PG_RETURN_BYTEA_P(x) return PointerGetDatum(x)
#define PG_RETURN_NULL() \
	do { fcinfo->isnull = true; return (Datum) 0; } while (0)
#define PG_FREE_IF_COPY(ptr, n) ((void) 0)

/* DirectFunctionCall2 (fmgr.c semantics; strict-notnull caller contract) */
typedef Datum (*hst_pgfunc) (FunctionCallInfo fcinfo);

static Datum
hst_direct_call2(hst_pgfunc func, Datum a, Datum b)
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

#define DirectFunctionCall2(func, a, b) hst_direct_call2(hst_##func, (a), (b))

/* hash_any -> the verbatim hashfn.c copies exported by pg_mac_io.c */
extern uint32 pg_hash_bytes(const unsigned char *k, int keylen);
extern uint64 pg_hash_bytes_extended(const unsigned char *k, int keylen, uint64 seed);
#define hash_any(k, l) UInt32GetDatum(pg_hash_bytes((k), (l)))
#define hash_any_extended(k, l, s) UInt64GetDatum(pg_hash_bytes_extended((k), (l), (s)))

/* ==== VERBATIM: hstore.h lines 11-205 @ 62d6c7d3df (POLLUTE pinned off) ==== */
#define HSTORE_POLLUTE_NAMESPACE 0
#ifndef __HSTORE_H__
#define __HSTORE_H__



/*
 * HEntry: there is one of these for each key _and_ value in an hstore
 *
 * the position offset points to the _end_ so that we can get the length
 * by subtraction from the previous entry.  the ISFIRST flag lets us tell
 * whether there is a previous entry.
 */
typedef struct
{
	uint32		entry;
} HEntry;

#define HENTRY_ISFIRST 0x80000000
#define HENTRY_ISNULL  0x40000000
#define HENTRY_POSMASK 0x3FFFFFFF

/* note possible multiple evaluations, also access to prior array element */
#define HSE_ISFIRST(he_) (((he_).entry & HENTRY_ISFIRST) != 0)
#define HSE_ISNULL(he_) (((he_).entry & HENTRY_ISNULL) != 0)
#define HSE_ENDPOS(he_) ((he_).entry & HENTRY_POSMASK)
#define HSE_OFF(he_) (HSE_ISFIRST(he_) ? 0 : HSE_ENDPOS((&(he_))[-1]))
#define HSE_LEN(he_) (HSE_ISFIRST(he_)	\
					  ? HSE_ENDPOS(he_) \
					  : HSE_ENDPOS(he_) - HSE_ENDPOS((&(he_))[-1]))

/*
 * determined by the size of "endpos" (ie HENTRY_POSMASK), though this is a
 * bit academic since currently varlenas (and hence both the input and the
 * whole hstore) have the same limit
 */
#define HSTORE_MAX_KEY_LEN 0x3FFFFFFF
#define HSTORE_MAX_VALUE_LEN 0x3FFFFFFF

typedef struct
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	uint32		size_;			/* flags and number of items in hstore */
	/* array of HEntry follows */
} HStore;

/*
 * It's not possible to get more than 2^28 items into an hstore, so we reserve
 * the top few bits of the size field.  See hstore_compat.c for one reason
 * why.  Some bits are left for future use here.  MaxAllocSize makes the
 * practical count limit slightly more than 2^28 / 3, or INT_MAX / 24, the
 * limit for an hstore full of 4-byte keys and null values.  Therefore, we
 * don't explicitly check the format-imposed limit.
 */
#define HS_FLAG_NEWVERSION 0x80000000

#define HS_COUNT(hsp_) ((hsp_)->size_ & 0x0FFFFFFF)
#define HS_SETCOUNT(hsp_,c_) ((hsp_)->size_ = (c_) | HS_FLAG_NEWVERSION)


/*
 * "x" comes from an existing HS_COUNT() (as discussed, <= INT_MAX/24) or a
 * Pairs array length (due to MaxAllocSize, <= INT_MAX/40).  "lenstr" is no
 * more than INT_MAX, that extreme case arising in hstore_from_arrays().
 * Therefore, this calculation is limited to about INT_MAX / 5 + INT_MAX.
 */
#define HSHRDSIZE	(sizeof(HStore))
#define CALCDATASIZE(x, lenstr) ( (x) * 2 * sizeof(HEntry) + HSHRDSIZE + (lenstr) )

/* note multiple evaluations of x */
#define ARRPTR(x)		( (HEntry*) ( (HStore*)(x) + 1 ) )
#define STRPTR(x)		( (char*)(ARRPTR(x) + HS_COUNT((HStore*)(x)) * 2) )

/* note multiple/non evaluations */
#define HSTORE_KEY(arr_,str_,i_)	((str_) + HSE_OFF((arr_)[2*(i_)]))
#define HSTORE_VAL(arr_,str_,i_)	((str_) + HSE_OFF((arr_)[2*(i_)+1]))
#define HSTORE_KEYLEN(arr_,i_)		(HSE_LEN((arr_)[2*(i_)]))
#define HSTORE_VALLEN(arr_,i_)		(HSE_LEN((arr_)[2*(i_)+1]))
#define HSTORE_VALISNULL(arr_,i_)	(HSE_ISNULL((arr_)[2*(i_)+1]))

/*
 * currently, these following macros are the _only_ places that rely
 * on internal knowledge of HEntry. Everything else should be using
 * the above macros. Exception: the in-place upgrade in hstore_compat.c
 * messes with entries directly.
 */

/*
 * copy one key/value pair (which must be contiguous starting at
 * sptr_) into an under-construction hstore; dent_ is an HEntry*,
 * dbuf_ is the destination's string buffer, dptr_ is the current
 * position in the destination. lots of modification and multiple
 * evaluation here.
 */
#define HS_COPYITEM(dent_,dbuf_,dptr_,sptr_,klen_,vlen_,vnull_)			\
	do {																\
		memcpy((dptr_), (sptr_), (klen_)+(vlen_));						\
		(dptr_) += (klen_)+(vlen_);										\
		(dent_)++->entry = ((dptr_) - (dbuf_) - (vlen_)) & HENTRY_POSMASK; \
		(dent_)++->entry = ((((dptr_) - (dbuf_)) & HENTRY_POSMASK)		\
							 | ((vnull_) ? HENTRY_ISNULL : 0));			\
	} while(0)

/*
 * add one key/item pair, from a Pairs structure, into an
 * under-construction hstore
 */
#define HS_ADDITEM(dent_,dbuf_,dptr_,pair_)								\
	do {																\
		memcpy((dptr_), (pair_).key, (pair_).keylen);					\
		(dptr_) += (pair_).keylen;										\
		(dent_)++->entry = ((dptr_) - (dbuf_)) & HENTRY_POSMASK;		\
		if ((pair_).isnull)												\
			(dent_)++->entry = ((((dptr_) - (dbuf_)) & HENTRY_POSMASK)	\
								 | HENTRY_ISNULL);						\
		else															\
		{																\
			memcpy((dptr_), (pair_).val, (pair_).vallen);				\
			(dptr_) += (pair_).vallen;									\
			(dent_)++->entry = ((dptr_) - (dbuf_)) & HENTRY_POSMASK;	\
		}																\
	} while (0)

/* finalize a newly-constructed hstore */
#define HS_FINALIZE(hsp_,count_,buf_,ptr_)							\
	do {															\
		int _buflen = (ptr_) - (buf_);								\
		if ((count_))												\
			ARRPTR(hsp_)[0].entry |= HENTRY_ISFIRST;				\
		if ((count_) != HS_COUNT((hsp_)))							\
		{															\
			HS_SETCOUNT((hsp_),(count_));							\
			memmove(STRPTR(hsp_), (buf_), _buflen);					\
		}															\
		SET_VARSIZE((hsp_), CALCDATASIZE((count_), _buflen));		\
	} while (0)

/* ensure the varlena size of an existing hstore is correct */
#define HS_FIXSIZE(hsp_,count_)											\
	do {																\
		int bl = (count_) ? HSE_ENDPOS(ARRPTR(hsp_)[2*(count_)-1]) : 0; \
		SET_VARSIZE((hsp_), CALCDATASIZE((count_),bl));					\
	} while (0)

/* DatumGetHStoreP includes support for reading old-format hstore values */
extern PGDLLEXPORT HStore *hstoreUpgrade(Datum orig);

#define DatumGetHStoreP(d) hstoreUpgrade(d)

#define PG_GETARG_HSTORE_P(x) DatumGetHStoreP(PG_GETARG_DATUM(x))


/*
 * Pairs is a "decompressed" representation of one key/value pair.
 * The two strings are not necessarily null-terminated.
 */
typedef struct
{
	char	   *key;
	char	   *val;
	size_t		keylen;
	size_t		vallen;
	bool		isnull;			/* value is null? */
	bool		needfree;		/* need to pfree the value? */
} Pairs;

extern PGDLLEXPORT int hstoreUniquePairs(Pairs *a, int32 l, int32 *buflen);
extern PGDLLEXPORT HStore *hstorePairs(Pairs *pairs, int32 pcount, int32 buflen);

extern PGDLLEXPORT size_t hstoreCheckKeyLen(size_t len);
extern PGDLLEXPORT size_t hstoreCheckValLen(size_t len);

extern PGDLLEXPORT int hstoreFindKey(HStore *hs, int *lowbound, char *key, int keylen);
extern PGDLLEXPORT Pairs *hstoreArrayToPairs(ArrayType *a, int *npairs);

#define HStoreContainsStrategyNumber	7
#define HStoreExistsStrategyNumber		9
#define HStoreExistsAnyStrategyNumber	10
#define HStoreExistsAllStrategyNumber	11
#define HStoreOldContainsStrategyNumber 13	/* backwards compatibility */

/*
 * defining HSTORE_POLLUTE_NAMESPACE=0 will prevent use of old function names;
 * for now, we default to on for the benefit of people restoring old dumps
 */
#ifndef HSTORE_POLLUTE_NAMESPACE
#define HSTORE_POLLUTE_NAMESPACE 1
#endif

#if HSTORE_POLLUTE_NAMESPACE
#define HSTORE_POLLUTE(newname_,oldname_) \
	PG_FUNCTION_INFO_V1(oldname_);		  \
	extern PGDLLEXPORT Datum newname_(PG_FUNCTION_ARGS);	  \
	Datum oldname_(PG_FUNCTION_ARGS) { return newname_(fcinfo); } \
	extern int no_such_variable
#else
#define HSTORE_POLLUTE(newname_,oldname_) \
	extern int no_such_variable
#endif

#endif							/* __HSTORE_H__ */

/* SHIM: DatumGetHStoreP without the hstore_compat.c old-format upgrade.
 * DRIVER PRECONDITION: every image handed to this oracle is a new-format
 * (HS_FLAG_NEWVERSION) 4B-header image built by hstorePairs on either side;
 * hstoreUpgrade on such images is the identity cast (hstore_compat.c line
 * 240 arm). The old-format path is a carved exception (no producer). */
#undef DatumGetHStoreP
#define DatumGetHStoreP(d) ((HStore *) DatumGetPointer(d))

/* ==== VERBATIM: hstore_io.c lines 33-478 @ 62d6c7d3df (HSParser fsm,
 * comparePairs, hstoreUniquePairs, length checks, hstorePairs) ==== */
typedef struct
{
	char	   *begin;
	char	   *ptr;
	char	   *cur;
	char	   *word;
	int			wordlen;
	Node	   *escontext;

	Pairs	   *pairs;
	int			pcur;
	int			plen;
} HSParser;

static bool hstoreCheckKeyLength(size_t len, HSParser *state);
static bool hstoreCheckValLength(size_t len, HSParser *state);


#define RESIZEPRSBUF \
do { \
		if ( state->cur - state->word + 1 >= state->wordlen ) \
		{ \
				int32 clen = state->cur - state->word; \
				state->wordlen *= 2; \
				state->word = (char*)repalloc( (void*)state->word, state->wordlen ); \
				state->cur = state->word + clen; \
		} \
} while (0)

#define PRSSYNTAXERROR return prssyntaxerror(state)

static bool
prssyntaxerror(HSParser *state)
{
	errsave(state->escontext,
			(errcode(ERRCODE_SYNTAX_ERROR),
			 errmsg("syntax error in hstore, near \"%.*s\" at position %d",
					pg_mblen_cstr(state->ptr), state->ptr,
					(int) (state->ptr - state->begin))));
	/* In soft error situation, return false as convenience for caller */
	return false;
}

#define PRSEOF return prseof(state)

static bool
prseof(HSParser *state)
{
	errsave(state->escontext,
			(errcode(ERRCODE_SYNTAX_ERROR),
			 errmsg("syntax error in hstore: unexpected end of string")));
	/* In soft error situation, return false as convenience for caller */
	return false;
}


#define GV_WAITVAL 0
#define GV_INVAL 1
#define GV_INESCVAL 2
#define GV_WAITESCIN 3
#define GV_WAITESCESCIN 4

static bool
get_val(HSParser *state, bool ignoreeq, bool *escaped)
{
	int			st = GV_WAITVAL;

	state->wordlen = 32;
	state->cur = state->word = palloc(state->wordlen);
	*escaped = false;

	while (1)
	{
		if (st == GV_WAITVAL)
		{
			if (*(state->ptr) == '"')
			{
				*escaped = true;
				st = GV_INESCVAL;
			}
			else if (*(state->ptr) == '\0')
			{
				return false;
			}
			else if (*(state->ptr) == '=' && !ignoreeq)
			{
				PRSSYNTAXERROR;
			}
			else if (*(state->ptr) == '\\')
			{
				st = GV_WAITESCIN;
			}
			else if (!scanner_isspace((unsigned char) *(state->ptr)))
			{
				*(state->cur) = *(state->ptr);
				state->cur++;
				st = GV_INVAL;
			}
		}
		else if (st == GV_INVAL)
		{
			if (*(state->ptr) == '\\')
			{
				st = GV_WAITESCIN;
			}
			else if (*(state->ptr) == '=' && !ignoreeq)
			{
				state->ptr--;
				return true;
			}
			else if (*(state->ptr) == ',' && ignoreeq)
			{
				state->ptr--;
				return true;
			}
			else if (scanner_isspace((unsigned char) *(state->ptr)))
			{
				return true;
			}
			else if (*(state->ptr) == '\0')
			{
				state->ptr--;
				return true;
			}
			else
			{
				RESIZEPRSBUF;
				*(state->cur) = *(state->ptr);
				state->cur++;
			}
		}
		else if (st == GV_INESCVAL)
		{
			if (*(state->ptr) == '\\')
			{
				st = GV_WAITESCESCIN;
			}
			else if (*(state->ptr) == '"')
			{
				return true;
			}
			else if (*(state->ptr) == '\0')
			{
				PRSEOF;
			}
			else
			{
				RESIZEPRSBUF;
				*(state->cur) = *(state->ptr);
				state->cur++;
			}
		}
		else if (st == GV_WAITESCIN)
		{
			if (*(state->ptr) == '\0')
				PRSEOF;
			RESIZEPRSBUF;
			*(state->cur) = *(state->ptr);
			state->cur++;
			st = GV_INVAL;
		}
		else if (st == GV_WAITESCESCIN)
		{
			if (*(state->ptr) == '\0')
				PRSEOF;
			RESIZEPRSBUF;
			*(state->cur) = *(state->ptr);
			state->cur++;
			st = GV_INESCVAL;
		}
		else
			elog(ERROR, "unrecognized get_val state: %d", st);

		state->ptr++;
	}
}

#define WKEY	0
#define WVAL	1
#define WEQ 2
#define WGT 3
#define WDEL	4


static bool
parse_hstore(HSParser *state)
{
	int			st = WKEY;
	bool		escaped = false;

	state->plen = 16;
	state->pairs = (Pairs *) palloc(sizeof(Pairs) * state->plen);
	state->pcur = 0;
	state->ptr = state->begin;
	state->word = NULL;

	while (1)
	{
		if (st == WKEY)
		{
			if (!get_val(state, false, &escaped))
			{
				if (SOFT_ERROR_OCCURRED(state->escontext))
					return false;
				return true;	/* EOF, all okay */
			}
			if (state->pcur >= state->plen)
			{
				state->plen *= 2;
				state->pairs = (Pairs *) repalloc(state->pairs, sizeof(Pairs) * state->plen);
			}
			if (!hstoreCheckKeyLength(state->cur - state->word, state))
				return false;
			state->pairs[state->pcur].key = state->word;
			state->pairs[state->pcur].keylen = state->cur - state->word;
			state->pairs[state->pcur].val = NULL;
			state->word = NULL;
			st = WEQ;
		}
		else if (st == WEQ)
		{
			if (*(state->ptr) == '=')
			{
				st = WGT;
			}
			else if (*(state->ptr) == '\0')
			{
				PRSEOF;
			}
			else if (!scanner_isspace((unsigned char) *(state->ptr)))
			{
				PRSSYNTAXERROR;
			}
		}
		else if (st == WGT)
		{
			if (*(state->ptr) == '>')
			{
				st = WVAL;
			}
			else if (*(state->ptr) == '\0')
			{
				PRSEOF;
			}
			else
			{
				PRSSYNTAXERROR;
			}
		}
		else if (st == WVAL)
		{
			if (!get_val(state, true, &escaped))
			{
				if (SOFT_ERROR_OCCURRED(state->escontext))
					return false;
				PRSEOF;
			}
			if (!hstoreCheckValLength(state->cur - state->word, state))
				return false;
			state->pairs[state->pcur].val = state->word;
			state->pairs[state->pcur].vallen = state->cur - state->word;
			state->pairs[state->pcur].isnull = false;
			state->pairs[state->pcur].needfree = true;
			if (state->cur - state->word == 4 && !escaped)
			{
				state->word[4] = '\0';
				if (pg_strcasecmp(state->word, "null") == 0)
					state->pairs[state->pcur].isnull = true;
			}
			state->word = NULL;
			state->pcur++;
			st = WDEL;
		}
		else if (st == WDEL)
		{
			if (*(state->ptr) == ',')
			{
				st = WKEY;
			}
			else if (*(state->ptr) == '\0')
			{
				return true;
			}
			else if (!scanner_isspace((unsigned char) *(state->ptr)))
			{
				PRSSYNTAXERROR;
			}
		}
		else
			elog(ERROR, "unrecognized parse_hstore state: %d", st);

		state->ptr++;
	}
}

static int
comparePairs(const void *a, const void *b)
{
	const Pairs *pa = a;
	const Pairs *pb = b;

	if (pa->keylen == pb->keylen)
	{
		int			res = memcmp(pa->key, pb->key, pa->keylen);

		if (res)
			return res;

		/* guarantee that needfree will be later */
		if (pb->needfree == pa->needfree)
			return 0;
		else if (pa->needfree)
			return 1;
		else
			return -1;
	}
	return (pa->keylen > pb->keylen) ? 1 : -1;
}

/*
 * this code still respects pairs.needfree, even though in general
 * it should never be called in a context where anything needs freeing.
 * we keep it because (a) those calls are in a rare code path anyway,
 * and (b) who knows whether they might be needed by some caller.
 */
int
hstoreUniquePairs(Pairs *a, int32 l, int32 *buflen)
{
	Pairs	   *ptr,
			   *res;

	*buflen = 0;
	if (l < 2)
	{
		if (l == 1)
			*buflen = a->keylen + ((a->isnull) ? 0 : a->vallen);
		return l;
	}

	qsort(a, l, sizeof(Pairs), comparePairs);

	/*
	 * We can't use qunique here because we have some clean-up code to run on
	 * removed elements.
	 */
	ptr = a + 1;
	res = a;
	while (ptr - a < l)
	{
		if (ptr->keylen == res->keylen &&
			memcmp(ptr->key, res->key, res->keylen) == 0)
		{
			if (ptr->needfree)
			{
				pfree(ptr->key);
				if (ptr->val != NULL)
					pfree(ptr->val);
			}
		}
		else
		{
			*buflen += res->keylen + ((res->isnull) ? 0 : res->vallen);
			res++;
			if (res != ptr)
				memcpy(res, ptr, sizeof(Pairs));
		}

		ptr++;
	}

	*buflen += res->keylen + ((res->isnull) ? 0 : res->vallen);
	return res + 1 - a;
}

size_t
hstoreCheckKeyLen(size_t len)
{
	if (len > HSTORE_MAX_KEY_LEN)
		ereport(ERROR,
				(errcode(ERRCODE_STRING_DATA_RIGHT_TRUNCATION),
				 errmsg("string too long for hstore key")));
	return len;
}

static bool
hstoreCheckKeyLength(size_t len, HSParser *state)
{
	if (len > HSTORE_MAX_KEY_LEN)
		ereturn(state->escontext, false,
				(errcode(ERRCODE_STRING_DATA_RIGHT_TRUNCATION),
				 errmsg("string too long for hstore key")));
	return true;
}

size_t
hstoreCheckValLen(size_t len)
{
	if (len > HSTORE_MAX_VALUE_LEN)
		ereport(ERROR,
				(errcode(ERRCODE_STRING_DATA_RIGHT_TRUNCATION),
				 errmsg("string too long for hstore value")));
	return len;
}

static bool
hstoreCheckValLength(size_t len, HSParser *state)
{
	if (len > HSTORE_MAX_VALUE_LEN)
		ereturn(state->escontext, false,
				(errcode(ERRCODE_STRING_DATA_RIGHT_TRUNCATION),
				 errmsg("string too long for hstore value")));
	return true;
}


HStore *
hstorePairs(Pairs *pairs, int32 pcount, int32 buflen)
{
	HStore	   *out;
	HEntry	   *entry;
	char	   *ptr;
	char	   *buf;
	int32		len;
	int32		i;

	len = CALCDATASIZE(pcount, buflen);
	out = palloc(len);
	SET_VARSIZE(out, len);
	HS_SETCOUNT(out, pcount);

	if (pcount == 0)
		return out;

	entry = ARRPTR(out);
	buf = ptr = STRPTR(out);

	for (i = 0; i < pcount; i++)
		HS_ADDITEM(entry, buf, ptr, pairs[i]);

	HS_FINALIZE(out, pcount, buf, ptr);

	return out;
}


PG_FUNCTION_INFO_V1(hstore_in);

/* ==== VERBATIM: hstore_io.c entry bodies @ 62d6c7d3df ==== */

PG_FUNCTION_INFO_V1(hstore_in);
Datum
hstore_in(PG_FUNCTION_ARGS)
{
	char	   *str = PG_GETARG_CSTRING(0);
	Node	   *escontext = fcinfo->context;
	HSParser	state;
	int32		buflen;
	HStore	   *out;

	state.begin = str;
	state.escontext = escontext;

	if (!parse_hstore(&state))
		PG_RETURN_NULL();

	state.pcur = hstoreUniquePairs(state.pairs, state.pcur, &buflen);

	out = hstorePairs(state.pairs, state.pcur, buflen);

	PG_RETURN_POINTER(out);
}

PG_FUNCTION_INFO_V1(hstore_recv);
Datum
hstore_recv(PG_FUNCTION_ARGS)
{
	int32		buflen;
	HStore	   *out;
	Pairs	   *pairs;
	int32		i;
	int32		pcount;
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);

	pcount = pq_getmsgint(buf, 4);

	if (pcount == 0)
	{
		out = hstorePairs(NULL, 0, 0);
		PG_RETURN_POINTER(out);
	}

	if (pcount < 0 || pcount > MaxAllocSize / sizeof(Pairs))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("number of pairs (%d) exceeds the maximum allowed (%d)",
						pcount, (int) (MaxAllocSize / sizeof(Pairs)))));
	pairs = palloc(pcount * sizeof(Pairs));

	for (i = 0; i < pcount; ++i)
	{
		int			rawlen = pq_getmsgint(buf, 4);
		int			len;

		if (rawlen < 0)
			ereport(ERROR,
					(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
					 errmsg("null value not allowed for hstore key")));

		pairs[i].key = pq_getmsgtext(buf, rawlen, &len);
		pairs[i].keylen = hstoreCheckKeyLen(len);
		pairs[i].needfree = true;

		rawlen = pq_getmsgint(buf, 4);
		if (rawlen < 0)
		{
			pairs[i].val = NULL;
			pairs[i].vallen = 0;
			pairs[i].isnull = true;
		}
		else
		{
			pairs[i].val = pq_getmsgtext(buf, rawlen, &len);
			pairs[i].vallen = hstoreCheckValLen(len);
			pairs[i].isnull = false;
		}
	}

	pcount = hstoreUniquePairs(pairs, pcount, &buflen);

	out = hstorePairs(pairs, pcount, buflen);

	PG_RETURN_POINTER(out);
}

PG_FUNCTION_INFO_V1(hstore_from_text);
Datum
hstore_from_text(PG_FUNCTION_ARGS)
{
	text	   *key;
	text	   *val = NULL;
	Pairs		p;
	HStore	   *out;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	p.needfree = false;
	key = PG_GETARG_TEXT_PP(0);
	p.key = VARDATA_ANY(key);
	p.keylen = hstoreCheckKeyLen(VARSIZE_ANY_EXHDR(key));

	if (PG_ARGISNULL(1))
	{
		p.vallen = 0;
		p.isnull = true;
	}
	else
	{
		val = PG_GETARG_TEXT_PP(1);
		p.val = VARDATA_ANY(val);
		p.vallen = hstoreCheckValLen(VARSIZE_ANY_EXHDR(val));
		p.isnull = false;
	}

	out = hstorePairs(&p, 1, p.keylen + p.vallen);

	PG_RETURN_POINTER(out);
}

PG_FUNCTION_INFO_V1(hstore_from_arrays);
Datum
hstore_from_arrays(PG_FUNCTION_ARGS)
{
	int32		buflen;
	HStore	   *out;
	Pairs	   *pairs;
	Datum	   *key_datums;
	bool	   *key_nulls;
	int			key_count;
	Datum	   *value_datums;
	bool	   *value_nulls;
	int			value_count;
	ArrayType  *key_array;
	ArrayType  *value_array;
	int			i;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	key_array = PG_GETARG_ARRAYTYPE_P(0);

	Assert(ARR_ELEMTYPE(key_array) == TEXTOID);

	/*
	 * must check >1 rather than != 1 because empty arrays have 0 dimensions,
	 * not 1
	 */

	if (ARR_NDIM(key_array) > 1)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("wrong number of array subscripts")));

	deconstruct_array_builtin(key_array, TEXTOID, &key_datums, &key_nulls, &key_count);

	/* see discussion in hstoreArrayToPairs() */
	if (key_count > MaxAllocSize / sizeof(Pairs))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("number of pairs (%d) exceeds the maximum allowed (%d)",
						key_count, (int) (MaxAllocSize / sizeof(Pairs)))));

	/* value_array might be NULL */

	if (PG_ARGISNULL(1))
	{
		value_array = NULL;
		value_count = key_count;
		value_datums = NULL;
		value_nulls = NULL;
	}
	else
	{
		value_array = PG_GETARG_ARRAYTYPE_P(1);

		Assert(ARR_ELEMTYPE(value_array) == TEXTOID);

		if (ARR_NDIM(value_array) > 1)
			ereport(ERROR,
					(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
					 errmsg("wrong number of array subscripts")));

		if ((ARR_NDIM(key_array) > 0 || ARR_NDIM(value_array) > 0) &&
			(ARR_NDIM(key_array) != ARR_NDIM(value_array) ||
			 ARR_DIMS(key_array)[0] != ARR_DIMS(value_array)[0] ||
			 ARR_LBOUND(key_array)[0] != ARR_LBOUND(value_array)[0]))
			ereport(ERROR,
					(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
					 errmsg("arrays must have same bounds")));

		deconstruct_array_builtin(value_array, TEXTOID, &value_datums, &value_nulls, &value_count);

		Assert(key_count == value_count);
	}

	pairs = palloc(key_count * sizeof(Pairs));

	for (i = 0; i < key_count; ++i)
	{
		if (key_nulls[i])
			ereport(ERROR,
					(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
					 errmsg("null value not allowed for hstore key")));

		if (!value_nulls || value_nulls[i])
		{
			pairs[i].key = VARDATA(key_datums[i]);
			pairs[i].val = NULL;
			pairs[i].keylen =
				hstoreCheckKeyLen(VARSIZE(key_datums[i]) - VARHDRSZ);
			pairs[i].vallen = 4;
			pairs[i].isnull = true;
			pairs[i].needfree = false;
		}
		else
		{
			pairs[i].key = VARDATA(key_datums[i]);
			pairs[i].val = VARDATA(value_datums[i]);
			pairs[i].keylen =
				hstoreCheckKeyLen(VARSIZE(key_datums[i]) - VARHDRSZ);
			pairs[i].vallen =
				hstoreCheckValLen(VARSIZE(value_datums[i]) - VARHDRSZ);
			pairs[i].isnull = false;
			pairs[i].needfree = false;
		}
	}

	key_count = hstoreUniquePairs(pairs, key_count, &buflen);

	out = hstorePairs(pairs, key_count, buflen);

	PG_RETURN_POINTER(out);
}

PG_FUNCTION_INFO_V1(hstore_from_array);
Datum
hstore_from_array(PG_FUNCTION_ARGS)
{
	ArrayType  *in_array = PG_GETARG_ARRAYTYPE_P(0);
	int			ndims = ARR_NDIM(in_array);
	int			count;
	int32		buflen;
	HStore	   *out;
	Pairs	   *pairs;
	Datum	   *in_datums;
	bool	   *in_nulls;
	int			in_count;
	int			i;

	Assert(ARR_ELEMTYPE(in_array) == TEXTOID);

	switch (ndims)
	{
		case 0:
			out = hstorePairs(NULL, 0, 0);
			PG_RETURN_POINTER(out);

		case 1:
			if ((ARR_DIMS(in_array)[0]) % 2)
				ereport(ERROR,
						(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
						 errmsg("array must have even number of elements")));
			break;

		case 2:
			if ((ARR_DIMS(in_array)[1]) != 2)
				ereport(ERROR,
						(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
						 errmsg("array must have two columns")));
			break;

		default:
			ereport(ERROR,
					(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
					 errmsg("wrong number of array subscripts")));
	}

	deconstruct_array_builtin(in_array, TEXTOID, &in_datums, &in_nulls, &in_count);

	count = in_count / 2;

	/* see discussion in hstoreArrayToPairs() */
	if (count > MaxAllocSize / sizeof(Pairs))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("number of pairs (%d) exceeds the maximum allowed (%d)",
						count, (int) (MaxAllocSize / sizeof(Pairs)))));

	pairs = palloc(count * sizeof(Pairs));

	for (i = 0; i < count; ++i)
	{
		if (in_nulls[i * 2])
			ereport(ERROR,
					(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
					 errmsg("null value not allowed for hstore key")));

		if (in_nulls[i * 2 + 1])
		{
			pairs[i].key = VARDATA(in_datums[i * 2]);
			pairs[i].val = NULL;
			pairs[i].keylen =
				hstoreCheckKeyLen(VARSIZE(in_datums[i * 2]) - VARHDRSZ);
			pairs[i].vallen = 4;
			pairs[i].isnull = true;
			pairs[i].needfree = false;
		}
		else
		{
			pairs[i].key = VARDATA(in_datums[i * 2]);
			pairs[i].val = VARDATA(in_datums[i * 2 + 1]);
			pairs[i].keylen =
				hstoreCheckKeyLen(VARSIZE(in_datums[i * 2]) - VARHDRSZ);
			pairs[i].vallen =
				hstoreCheckValLen(VARSIZE(in_datums[i * 2 + 1]) - VARHDRSZ);
			pairs[i].isnull = false;
			pairs[i].needfree = false;
		}
	}

	count = hstoreUniquePairs(pairs, count, &buflen);

	out = hstorePairs(pairs, count, buflen);

	PG_RETURN_POINTER(out);
}

static char *
cpw(char *dst, char *src, int len)
{
	char	   *ptr = src;

	while (ptr - src < len)
	{
		if (*ptr == '"' || *ptr == '\\')
			*dst++ = '\\';
		*dst++ = *ptr++;
	}
	return dst;
}

PG_FUNCTION_INFO_V1(hstore_out);
Datum
hstore_out(PG_FUNCTION_ARGS)
{
	HStore	   *in = PG_GETARG_HSTORE_P(0);
	int			buflen,
				i;
	int			count = HS_COUNT(in);
	char	   *out,
			   *ptr;
	char	   *base = STRPTR(in);
	HEntry	   *entries = ARRPTR(in);

	if (count == 0)
		PG_RETURN_CSTRING(pstrdup(""));

	buflen = 0;

	/*
	 * this loop overestimates due to pessimistic assumptions about escaping,
	 * so very large hstore values can't be output. this could be fixed, but
	 * many other data types probably have the same issue. This replaced code
	 * that used the original varlena size for calculations, which was wrong
	 * in some subtle ways.
	 */

	for (i = 0; i < count; i++)
	{
		/* include "" and => and comma-space */
		buflen += 6 + 2 * HSTORE_KEYLEN(entries, i);
		/* include "" only if nonnull */
		buflen += 2 + (HSTORE_VALISNULL(entries, i)
					   ? 2
					   : 2 * HSTORE_VALLEN(entries, i));
	}

	out = ptr = palloc(buflen);

	for (i = 0; i < count; i++)
	{
		*ptr++ = '"';
		ptr = cpw(ptr, HSTORE_KEY(entries, base, i), HSTORE_KEYLEN(entries, i));
		*ptr++ = '"';
		*ptr++ = '=';
		*ptr++ = '>';
		if (HSTORE_VALISNULL(entries, i))
		{
			*ptr++ = 'N';
			*ptr++ = 'U';
			*ptr++ = 'L';
			*ptr++ = 'L';
		}
		else
		{
			*ptr++ = '"';
			ptr = cpw(ptr, HSTORE_VAL(entries, base, i), HSTORE_VALLEN(entries, i));
			*ptr++ = '"';
		}

		if (i + 1 != count)
		{
			*ptr++ = ',';
			*ptr++ = ' ';
		}
	}
	*ptr = '\0';

	PG_RETURN_CSTRING(out);
}

PG_FUNCTION_INFO_V1(hstore_send);
Datum
hstore_send(PG_FUNCTION_ARGS)
{
	HStore	   *in = PG_GETARG_HSTORE_P(0);
	int			i;
	int			count = HS_COUNT(in);
	char	   *base = STRPTR(in);
	HEntry	   *entries = ARRPTR(in);
	StringInfoData buf;

	pq_begintypsend(&buf);

	pq_sendint32(&buf, count);

	for (i = 0; i < count; i++)
	{
		int32		keylen = HSTORE_KEYLEN(entries, i);

		pq_sendint32(&buf, keylen);
		pq_sendtext(&buf, HSTORE_KEY(entries, base, i), keylen);
		if (HSTORE_VALISNULL(entries, i))
		{
			pq_sendint32(&buf, -1);
		}
		else
		{
			int32		vallen = HSTORE_VALLEN(entries, i);

			pq_sendint32(&buf, vallen);
			pq_sendtext(&buf, HSTORE_VAL(entries, base, i), vallen);
		}
	}

	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

PG_FUNCTION_INFO_V1(hstore_to_json_loose);
Datum
hstore_to_json_loose(PG_FUNCTION_ARGS)
{
	HStore	   *in = PG_GETARG_HSTORE_P(0);
	int			i;
	int			count = HS_COUNT(in);
	char	   *base = STRPTR(in);
	HEntry	   *entries = ARRPTR(in);
	StringInfoData dst;

	if (count == 0)
		PG_RETURN_TEXT_P(cstring_to_text_with_len("{}", 2));

	initStringInfo(&dst);

	appendStringInfoChar(&dst, '{');

	for (i = 0; i < count; i++)
	{
		escape_json_with_len(&dst,
							 HSTORE_KEY(entries, base, i),
							 HSTORE_KEYLEN(entries, i));
		appendStringInfoString(&dst, ": ");
		if (HSTORE_VALISNULL(entries, i))
			appendStringInfoString(&dst, "null");
		/* guess that values of 't' or 'f' are booleans */
		else if (HSTORE_VALLEN(entries, i) == 1 &&
				 *(HSTORE_VAL(entries, base, i)) == 't')
			appendStringInfoString(&dst, "true");
		else if (HSTORE_VALLEN(entries, i) == 1 &&
				 *(HSTORE_VAL(entries, base, i)) == 'f')
			appendStringInfoString(&dst, "false");
		else
		{
			char	   *str = HSTORE_VAL(entries, base, i);
			int			len = HSTORE_VALLEN(entries, i);

			if (IsValidJsonNumber(str, len))
				appendBinaryStringInfo(&dst, str, len);
			else
				escape_json_with_len(&dst, str, len);
		}

		if (i + 1 != count)
			appendStringInfoString(&dst, ", ");
	}
	appendStringInfoChar(&dst, '}');

	PG_RETURN_TEXT_P(cstring_to_text_with_len(dst.data, dst.len));
}

PG_FUNCTION_INFO_V1(hstore_to_json);
Datum
hstore_to_json(PG_FUNCTION_ARGS)
{
	HStore	   *in = PG_GETARG_HSTORE_P(0);
	int			i;
	int			count = HS_COUNT(in);
	char	   *base = STRPTR(in);
	HEntry	   *entries = ARRPTR(in);
	StringInfoData dst;

	if (count == 0)
		PG_RETURN_TEXT_P(cstring_to_text_with_len("{}", 2));

	initStringInfo(&dst);

	appendStringInfoChar(&dst, '{');

	for (i = 0; i < count; i++)
	{
		escape_json_with_len(&dst,
							 HSTORE_KEY(entries, base, i),
							 HSTORE_KEYLEN(entries, i));
		appendStringInfoString(&dst, ": ");
		if (HSTORE_VALISNULL(entries, i))
			appendStringInfoString(&dst, "null");
		else
		{
			escape_json_with_len(&dst,
								 HSTORE_VAL(entries, base, i),
								 HSTORE_VALLEN(entries, i));
		}

		if (i + 1 != count)
			appendStringInfoString(&dst, ", ");
	}
	appendStringInfoChar(&dst, '}');

	PG_RETURN_TEXT_P(cstring_to_text_with_len(dst.data, dst.len));
}

/* ==== VERBATIM: hstore_op.c bodies @ 62d6c7d3df ==== */
int
hstoreFindKey(HStore *hs, int *lowbound, char *key, int keylen)
{
	HEntry	   *entries = ARRPTR(hs);
	int			stopLow = lowbound ? *lowbound : 0;
	int			stopHigh = HS_COUNT(hs);
	int			stopMiddle;
	char	   *base = STRPTR(hs);

	while (stopLow < stopHigh)
	{
		int			difference;

		stopMiddle = stopLow + (stopHigh - stopLow) / 2;

		if (HSTORE_KEYLEN(entries, stopMiddle) == keylen)
			difference = memcmp(HSTORE_KEY(entries, base, stopMiddle), key, keylen);
		else
			difference = (HSTORE_KEYLEN(entries, stopMiddle) > keylen) ? 1 : -1;

		if (difference == 0)
		{
			if (lowbound)
				*lowbound = stopMiddle + 1;
			return stopMiddle;
		}
		else if (difference < 0)
			stopLow = stopMiddle + 1;
		else
			stopHigh = stopMiddle;
	}

	if (lowbound)
		*lowbound = stopLow;
	return -1;
}

Pairs *
hstoreArrayToPairs(ArrayType *a, int *npairs)
{
	Datum	   *key_datums;
	bool	   *key_nulls;
	int			key_count;
	Pairs	   *key_pairs;
	int			bufsiz;
	int			i,
				j;

	deconstruct_array_builtin(a, TEXTOID, &key_datums, &key_nulls, &key_count);

	if (key_count == 0)
	{
		*npairs = 0;
		return NULL;
	}

	/*
	 * A text array uses at least eight bytes per element, so any overflow in
	 * "key_count * sizeof(Pairs)" is small enough for palloc() to catch.
	 * However, credible improvements to the array format could invalidate
	 * that assumption.  Therefore, use an explicit check rather than relying
	 * on palloc() to complain.
	 */
	if (key_count > MaxAllocSize / sizeof(Pairs))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("number of pairs (%d) exceeds the maximum allowed (%d)",
						key_count, (int) (MaxAllocSize / sizeof(Pairs)))));

	key_pairs = palloc(sizeof(Pairs) * key_count);

	for (i = 0, j = 0; i < key_count; i++)
	{
		if (!key_nulls[i])
		{
			key_pairs[j].key = VARDATA(key_datums[i]);
			key_pairs[j].keylen = VARSIZE(key_datums[i]) - VARHDRSZ;
			key_pairs[j].val = NULL;
			key_pairs[j].vallen = 0;
			key_pairs[j].needfree = 0;
			key_pairs[j].isnull = 1;
			j++;
		}
	}

	*npairs = hstoreUniquePairs(key_pairs, j, &bufsiz);

	return key_pairs;
}

PG_FUNCTION_INFO_V1(hstore_fetchval);
Datum
hstore_fetchval(PG_FUNCTION_ARGS)
{
	HStore	   *hs = PG_GETARG_HSTORE_P(0);
	text	   *key = PG_GETARG_TEXT_PP(1);
	HEntry	   *entries = ARRPTR(hs);
	text	   *out;
	int			idx = hstoreFindKey(hs, NULL,
									VARDATA_ANY(key), VARSIZE_ANY_EXHDR(key));

	if (idx < 0 || HSTORE_VALISNULL(entries, idx))
		PG_RETURN_NULL();

	out = cstring_to_text_with_len(HSTORE_VAL(entries, STRPTR(hs), idx),
								   HSTORE_VALLEN(entries, idx));

	PG_RETURN_TEXT_P(out);
}

PG_FUNCTION_INFO_V1(hstore_exists);
Datum
hstore_exists(PG_FUNCTION_ARGS)
{
	HStore	   *hs = PG_GETARG_HSTORE_P(0);
	text	   *key = PG_GETARG_TEXT_PP(1);
	int			idx = hstoreFindKey(hs, NULL,
									VARDATA_ANY(key), VARSIZE_ANY_EXHDR(key));

	PG_RETURN_BOOL(idx >= 0);
}

PG_FUNCTION_INFO_V1(hstore_exists_any);
Datum
hstore_exists_any(PG_FUNCTION_ARGS)
{
	HStore	   *hs = PG_GETARG_HSTORE_P(0);
	ArrayType  *keys = PG_GETARG_ARRAYTYPE_P(1);
	int			nkeys;
	Pairs	   *key_pairs = hstoreArrayToPairs(keys, &nkeys);
	int			i;
	int			lowbound = 0;
	bool		res = false;

	/*
	 * we exploit the fact that the pairs list is already sorted into strictly
	 * increasing order to narrow the hstoreFindKey search; each search can
	 * start one entry past the previous "found" entry, or at the lower bound
	 * of the last search.
	 */
	for (i = 0; i < nkeys; i++)
	{
		int			idx = hstoreFindKey(hs, &lowbound,
										key_pairs[i].key, key_pairs[i].keylen);

		if (idx >= 0)
		{
			res = true;
			break;
		}
	}

	PG_RETURN_BOOL(res);
}

PG_FUNCTION_INFO_V1(hstore_exists_all);
Datum
hstore_exists_all(PG_FUNCTION_ARGS)
{
	HStore	   *hs = PG_GETARG_HSTORE_P(0);
	ArrayType  *keys = PG_GETARG_ARRAYTYPE_P(1);
	int			nkeys;
	Pairs	   *key_pairs = hstoreArrayToPairs(keys, &nkeys);
	int			i;
	int			lowbound = 0;
	bool		res = true;

	/*
	 * we exploit the fact that the pairs list is already sorted into strictly
	 * increasing order to narrow the hstoreFindKey search; each search can
	 * start one entry past the previous "found" entry, or at the lower bound
	 * of the last search.
	 */
	for (i = 0; i < nkeys; i++)
	{
		int			idx = hstoreFindKey(hs, &lowbound,
										key_pairs[i].key, key_pairs[i].keylen);

		if (idx < 0)
		{
			res = false;
			break;
		}
	}

	PG_RETURN_BOOL(res);
}

PG_FUNCTION_INFO_V1(hstore_defined);
Datum
hstore_defined(PG_FUNCTION_ARGS)
{
	HStore	   *hs = PG_GETARG_HSTORE_P(0);
	text	   *key = PG_GETARG_TEXT_PP(1);
	HEntry	   *entries = ARRPTR(hs);
	int			idx = hstoreFindKey(hs, NULL,
									VARDATA_ANY(key), VARSIZE_ANY_EXHDR(key));
	bool		res = (idx >= 0 && !HSTORE_VALISNULL(entries, idx));

	PG_RETURN_BOOL(res);
}

PG_FUNCTION_INFO_V1(hstore_delete);
Datum
hstore_delete(PG_FUNCTION_ARGS)
{
	HStore	   *hs = PG_GETARG_HSTORE_P(0);
	text	   *key = PG_GETARG_TEXT_PP(1);
	char	   *keyptr = VARDATA_ANY(key);
	int			keylen = VARSIZE_ANY_EXHDR(key);
	HStore	   *out = palloc(VARSIZE(hs));
	char	   *bufs,
			   *bufd,
			   *ptrd;
	HEntry	   *es,
			   *ed;
	int			i;
	int			count = HS_COUNT(hs);
	int			outcount = 0;

	SET_VARSIZE(out, VARSIZE(hs));
	HS_SETCOUNT(out, count);	/* temporary! */

	bufs = STRPTR(hs);
	es = ARRPTR(hs);
	bufd = ptrd = STRPTR(out);
	ed = ARRPTR(out);

	for (i = 0; i < count; ++i)
	{
		int			len = HSTORE_KEYLEN(es, i);
		char	   *ptrs = HSTORE_KEY(es, bufs, i);

		if (!(len == keylen && memcmp(ptrs, keyptr, keylen) == 0))
		{
			int			vallen = HSTORE_VALLEN(es, i);

			HS_COPYITEM(ed, bufd, ptrd, ptrs, len, vallen,
						HSTORE_VALISNULL(es, i));
			++outcount;
		}
	}

	HS_FINALIZE(out, outcount, bufd, ptrd);

	PG_RETURN_POINTER(out);
}

PG_FUNCTION_INFO_V1(hstore_delete_array);
Datum
hstore_delete_array(PG_FUNCTION_ARGS)
{
	HStore	   *hs = PG_GETARG_HSTORE_P(0);
	HStore	   *out = palloc(VARSIZE(hs));
	int			hs_count = HS_COUNT(hs);
	char	   *ps,
			   *bufd,
			   *pd;
	HEntry	   *es,
			   *ed;
	int			i,
				j;
	int			outcount = 0;
	ArrayType  *key_array = PG_GETARG_ARRAYTYPE_P(1);
	int			nkeys;
	Pairs	   *key_pairs = hstoreArrayToPairs(key_array, &nkeys);

	SET_VARSIZE(out, VARSIZE(hs));
	HS_SETCOUNT(out, hs_count); /* temporary! */

	ps = STRPTR(hs);
	es = ARRPTR(hs);
	bufd = pd = STRPTR(out);
	ed = ARRPTR(out);

	if (nkeys == 0)
	{
		/* return a copy of the input, unchanged */
		memcpy(out, hs, VARSIZE(hs));
		HS_FIXSIZE(out, hs_count);
		HS_SETCOUNT(out, hs_count);
		PG_RETURN_POINTER(out);
	}

	/*
	 * this is in effect a merge between hs and key_pairs, both of which are
	 * already sorted by (keylen,key); we take keys from hs only
	 */

	for (i = j = 0; i < hs_count;)
	{
		int			difference;

		if (j >= nkeys)
			difference = -1;
		else
		{
			int			skeylen = HSTORE_KEYLEN(es, i);

			if (skeylen == key_pairs[j].keylen)
				difference = memcmp(HSTORE_KEY(es, ps, i),
									key_pairs[j].key,
									key_pairs[j].keylen);
			else
				difference = (skeylen > key_pairs[j].keylen) ? 1 : -1;
		}

		if (difference > 0)
			++j;
		else if (difference == 0)
			++i, ++j;
		else
		{
			HS_COPYITEM(ed, bufd, pd,
						HSTORE_KEY(es, ps, i), HSTORE_KEYLEN(es, i),
						HSTORE_VALLEN(es, i), HSTORE_VALISNULL(es, i));
			++outcount;
			++i;
		}
	}

	HS_FINALIZE(out, outcount, bufd, pd);

	PG_RETURN_POINTER(out);
}

PG_FUNCTION_INFO_V1(hstore_delete_hstore);
Datum
hstore_delete_hstore(PG_FUNCTION_ARGS)
{
	HStore	   *hs = PG_GETARG_HSTORE_P(0);
	HStore	   *hs2 = PG_GETARG_HSTORE_P(1);
	HStore	   *out = palloc(VARSIZE(hs));
	int			hs_count = HS_COUNT(hs);
	int			hs2_count = HS_COUNT(hs2);
	char	   *ps,
			   *ps2,
			   *bufd,
			   *pd;
	HEntry	   *es,
			   *es2,
			   *ed;
	int			i,
				j;
	int			outcount = 0;

	SET_VARSIZE(out, VARSIZE(hs));
	HS_SETCOUNT(out, hs_count); /* temporary! */

	ps = STRPTR(hs);
	es = ARRPTR(hs);
	ps2 = STRPTR(hs2);
	es2 = ARRPTR(hs2);
	bufd = pd = STRPTR(out);
	ed = ARRPTR(out);

	if (hs2_count == 0)
	{
		/* return a copy of the input, unchanged */
		memcpy(out, hs, VARSIZE(hs));
		HS_FIXSIZE(out, hs_count);
		HS_SETCOUNT(out, hs_count);
		PG_RETURN_POINTER(out);
	}

	/*
	 * this is in effect a merge between hs and hs2, both of which are already
	 * sorted by (keylen,key); we take keys from hs only; for equal keys, we
	 * take the value from hs unless the values are equal
	 */

	for (i = j = 0; i < hs_count;)
	{
		int			difference;

		if (j >= hs2_count)
			difference = -1;
		else
		{
			int			skeylen = HSTORE_KEYLEN(es, i);
			int			s2keylen = HSTORE_KEYLEN(es2, j);

			if (skeylen == s2keylen)
				difference = memcmp(HSTORE_KEY(es, ps, i),
									HSTORE_KEY(es2, ps2, j),
									skeylen);
			else
				difference = (skeylen > s2keylen) ? 1 : -1;
		}

		if (difference > 0)
			++j;
		else if (difference == 0)
		{
			int			svallen = HSTORE_VALLEN(es, i);
			int			snullval = HSTORE_VALISNULL(es, i);

			if (snullval != HSTORE_VALISNULL(es2, j) ||
				(!snullval && (svallen != HSTORE_VALLEN(es2, j) ||
							   memcmp(HSTORE_VAL(es, ps, i),
									  HSTORE_VAL(es2, ps2, j),
									  svallen) != 0)))
			{
				HS_COPYITEM(ed, bufd, pd,
							HSTORE_KEY(es, ps, i), HSTORE_KEYLEN(es, i),
							svallen, snullval);
				++outcount;
			}
			++i, ++j;
		}
		else
		{
			HS_COPYITEM(ed, bufd, pd,
						HSTORE_KEY(es, ps, i), HSTORE_KEYLEN(es, i),
						HSTORE_VALLEN(es, i), HSTORE_VALISNULL(es, i));
			++outcount;
			++i;
		}
	}

	HS_FINALIZE(out, outcount, bufd, pd);

	PG_RETURN_POINTER(out);
}

PG_FUNCTION_INFO_V1(hstore_concat);
Datum
hstore_concat(PG_FUNCTION_ARGS)
{
	HStore	   *s1 = PG_GETARG_HSTORE_P(0);
	HStore	   *s2 = PG_GETARG_HSTORE_P(1);
	HStore	   *out = palloc(VARSIZE(s1) + VARSIZE(s2));
	char	   *ps1,
			   *ps2,
			   *bufd,
			   *pd;
	HEntry	   *es1,
			   *es2,
			   *ed;
	int			s1idx;
	int			s2idx;
	int			s1count = HS_COUNT(s1);
	int			s2count = HS_COUNT(s2);
	int			outcount = 0;

	SET_VARSIZE(out, VARSIZE(s1) + VARSIZE(s2) - HSHRDSIZE);
	HS_SETCOUNT(out, s1count + s2count);

	if (s1count == 0)
	{
		/* return a copy of the input, unchanged */
		memcpy(out, s2, VARSIZE(s2));
		HS_FIXSIZE(out, s2count);
		HS_SETCOUNT(out, s2count);
		PG_RETURN_POINTER(out);
	}

	if (s2count == 0)
	{
		/* return a copy of the input, unchanged */
		memcpy(out, s1, VARSIZE(s1));
		HS_FIXSIZE(out, s1count);
		HS_SETCOUNT(out, s1count);
		PG_RETURN_POINTER(out);
	}

	ps1 = STRPTR(s1);
	ps2 = STRPTR(s2);
	bufd = pd = STRPTR(out);
	es1 = ARRPTR(s1);
	es2 = ARRPTR(s2);
	ed = ARRPTR(out);

	/*
	 * this is in effect a merge between s1 and s2, both of which are already
	 * sorted by (keylen,key); we take s2 for equal keys
	 */

	for (s1idx = s2idx = 0; s1idx < s1count || s2idx < s2count; ++outcount)
	{
		int			difference;

		if (s1idx >= s1count)
			difference = 1;
		else if (s2idx >= s2count)
			difference = -1;
		else
		{
			int			s1keylen = HSTORE_KEYLEN(es1, s1idx);
			int			s2keylen = HSTORE_KEYLEN(es2, s2idx);

			if (s1keylen == s2keylen)
				difference = memcmp(HSTORE_KEY(es1, ps1, s1idx),
									HSTORE_KEY(es2, ps2, s2idx),
									s1keylen);
			else
				difference = (s1keylen > s2keylen) ? 1 : -1;
		}

		if (difference >= 0)
		{
			HS_COPYITEM(ed, bufd, pd,
						HSTORE_KEY(es2, ps2, s2idx), HSTORE_KEYLEN(es2, s2idx),
						HSTORE_VALLEN(es2, s2idx), HSTORE_VALISNULL(es2, s2idx));
			++s2idx;
			if (difference == 0)
				++s1idx;
		}
		else
		{
			HS_COPYITEM(ed, bufd, pd,
						HSTORE_KEY(es1, ps1, s1idx), HSTORE_KEYLEN(es1, s1idx),
						HSTORE_VALLEN(es1, s1idx), HSTORE_VALISNULL(es1, s1idx));
			++s1idx;
		}
	}

	HS_FINALIZE(out, outcount, bufd, pd);

	PG_RETURN_POINTER(out);
}

PG_FUNCTION_INFO_V1(hstore_slice_to_array);
Datum
hstore_slice_to_array(PG_FUNCTION_ARGS)
{
	HStore	   *hs = PG_GETARG_HSTORE_P(0);
	HEntry	   *entries = ARRPTR(hs);
	char	   *ptr = STRPTR(hs);
	ArrayType  *key_array = PG_GETARG_ARRAYTYPE_P(1);
	ArrayType  *aout;
	Datum	   *key_datums;
	bool	   *key_nulls;
	Datum	   *out_datums;
	bool	   *out_nulls;
	int			key_count;
	int			i;

	deconstruct_array_builtin(key_array, TEXTOID, &key_datums, &key_nulls, &key_count);

	if (key_count == 0)
	{
		aout = construct_empty_array(TEXTOID);
		PG_RETURN_POINTER(aout);
	}

	out_datums = palloc(sizeof(Datum) * key_count);
	out_nulls = palloc(sizeof(bool) * key_count);

	for (i = 0; i < key_count; ++i)
	{
		text	   *key = (text *) DatumGetPointer(key_datums[i]);
		int			idx;

		if (key_nulls[i])
			idx = -1;
		else
			idx = hstoreFindKey(hs, NULL, VARDATA(key), VARSIZE(key) - VARHDRSZ);

		if (idx < 0 || HSTORE_VALISNULL(entries, idx))
		{
			out_nulls[i] = true;
			out_datums[i] = (Datum) 0;
		}
		else
		{
			out_datums[i] =
				PointerGetDatum(cstring_to_text_with_len(HSTORE_VAL(entries, ptr, idx),
														 HSTORE_VALLEN(entries, idx)));
			out_nulls[i] = false;
		}
	}

	aout = construct_md_array(out_datums, out_nulls,
							  ARR_NDIM(key_array),
							  ARR_DIMS(key_array),
							  ARR_LBOUND(key_array),
							  TEXTOID, -1, false, TYPALIGN_INT);

	PG_RETURN_POINTER(aout);
}

PG_FUNCTION_INFO_V1(hstore_slice_to_hstore);
Datum
hstore_slice_to_hstore(PG_FUNCTION_ARGS)
{
	HStore	   *hs = PG_GETARG_HSTORE_P(0);
	HEntry	   *entries = ARRPTR(hs);
	char	   *ptr = STRPTR(hs);
	ArrayType  *key_array = PG_GETARG_ARRAYTYPE_P(1);
	HStore	   *out;
	int			nkeys;
	Pairs	   *key_pairs = hstoreArrayToPairs(key_array, &nkeys);
	Pairs	   *out_pairs;
	int			bufsiz;
	int			lastidx = 0;
	int			i;
	int			out_count = 0;

	if (nkeys == 0)
	{
		out = hstorePairs(NULL, 0, 0);
		PG_RETURN_POINTER(out);
	}

	/* hstoreArrayToPairs() checked overflow */
	out_pairs = palloc(sizeof(Pairs) * nkeys);
	bufsiz = 0;

	/*
	 * we exploit the fact that the pairs list is already sorted into strictly
	 * increasing order to narrow the hstoreFindKey search; each search can
	 * start one entry past the previous "found" entry, or at the lower bound
	 * of the last search.
	 */

	for (i = 0; i < nkeys; ++i)
	{
		int			idx = hstoreFindKey(hs, &lastidx,
										key_pairs[i].key, key_pairs[i].keylen);

		if (idx >= 0)
		{
			out_pairs[out_count].key = key_pairs[i].key;
			bufsiz += (out_pairs[out_count].keylen = key_pairs[i].keylen);
			out_pairs[out_count].val = HSTORE_VAL(entries, ptr, idx);
			bufsiz += (out_pairs[out_count].vallen = HSTORE_VALLEN(entries, idx));
			out_pairs[out_count].isnull = HSTORE_VALISNULL(entries, idx);
			out_pairs[out_count].needfree = false;
			++out_count;
		}
	}

	/*
	 * we don't use hstoreUniquePairs here because we know that the pairs list
	 * is already sorted and uniq'ed.
	 */

	out = hstorePairs(out_pairs, out_count, bufsiz);

	PG_RETURN_POINTER(out);
}

PG_FUNCTION_INFO_V1(hstore_akeys);
Datum
hstore_akeys(PG_FUNCTION_ARGS)
{
	HStore	   *hs = PG_GETARG_HSTORE_P(0);
	Datum	   *d;
	ArrayType  *a;
	HEntry	   *entries = ARRPTR(hs);
	char	   *base = STRPTR(hs);
	int			count = HS_COUNT(hs);
	int			i;

	if (count == 0)
	{
		a = construct_empty_array(TEXTOID);
		PG_RETURN_POINTER(a);
	}

	d = (Datum *) palloc(sizeof(Datum) * count);

	for (i = 0; i < count; ++i)
	{
		text	   *t = cstring_to_text_with_len(HSTORE_KEY(entries, base, i),
												 HSTORE_KEYLEN(entries, i));

		d[i] = PointerGetDatum(t);
	}

	a = construct_array_builtin(d, count, TEXTOID);

	PG_RETURN_POINTER(a);
}

PG_FUNCTION_INFO_V1(hstore_avals);
Datum
hstore_avals(PG_FUNCTION_ARGS)
{
	HStore	   *hs = PG_GETARG_HSTORE_P(0);
	Datum	   *d;
	bool	   *nulls;
	ArrayType  *a;
	HEntry	   *entries = ARRPTR(hs);
	char	   *base = STRPTR(hs);
	int			count = HS_COUNT(hs);
	int			lb = 1;
	int			i;

	if (count == 0)
	{
		a = construct_empty_array(TEXTOID);
		PG_RETURN_POINTER(a);
	}

	d = (Datum *) palloc(sizeof(Datum) * count);
	nulls = (bool *) palloc(sizeof(bool) * count);

	for (i = 0; i < count; ++i)
	{
		if (HSTORE_VALISNULL(entries, i))
		{
			d[i] = (Datum) 0;
			nulls[i] = true;
		}
		else
		{
			text	   *item = cstring_to_text_with_len(HSTORE_VAL(entries, base, i),
														HSTORE_VALLEN(entries, i));

			d[i] = PointerGetDatum(item);
			nulls[i] = false;
		}
	}

	a = construct_md_array(d, nulls, 1, &count, &lb,
						   TEXTOID, -1, false, TYPALIGN_INT);

	PG_RETURN_POINTER(a);
}

static ArrayType *
hstore_to_array_internal(HStore *hs, int ndims)
{
	HEntry	   *entries = ARRPTR(hs);
	char	   *base = STRPTR(hs);
	int			count = HS_COUNT(hs);
	int			out_size[2] = {0, 2};
	int			lb[2] = {1, 1};
	Datum	   *out_datums;
	bool	   *out_nulls;
	int			i;

	Assert(ndims < 3);

	if (count == 0 || ndims == 0)
		return construct_empty_array(TEXTOID);

	out_size[0] = count * 2 / ndims;
	out_datums = palloc(sizeof(Datum) * count * 2);
	out_nulls = palloc(sizeof(bool) * count * 2);

	for (i = 0; i < count; ++i)
	{
		text	   *key = cstring_to_text_with_len(HSTORE_KEY(entries, base, i),
												   HSTORE_KEYLEN(entries, i));

		out_datums[i * 2] = PointerGetDatum(key);
		out_nulls[i * 2] = false;

		if (HSTORE_VALISNULL(entries, i))
		{
			out_datums[i * 2 + 1] = (Datum) 0;
			out_nulls[i * 2 + 1] = true;
		}
		else
		{
			text	   *item = cstring_to_text_with_len(HSTORE_VAL(entries, base, i),
														HSTORE_VALLEN(entries, i));

			out_datums[i * 2 + 1] = PointerGetDatum(item);
			out_nulls[i * 2 + 1] = false;
		}
	}

	return construct_md_array(out_datums, out_nulls,
							  ndims, out_size, lb,
							  TEXTOID, -1, false, TYPALIGN_INT);
}

PG_FUNCTION_INFO_V1(hstore_to_array);
Datum
hstore_to_array(PG_FUNCTION_ARGS)
{
	HStore	   *hs = PG_GETARG_HSTORE_P(0);
	ArrayType  *out = hstore_to_array_internal(hs, 1);

	PG_RETURN_POINTER(out);
}

PG_FUNCTION_INFO_V1(hstore_to_matrix);
Datum
hstore_to_matrix(PG_FUNCTION_ARGS)
{
	HStore	   *hs = PG_GETARG_HSTORE_P(0);
	ArrayType  *out = hstore_to_array_internal(hs, 2);

	PG_RETURN_POINTER(out);
}

PG_FUNCTION_INFO_V1(hstore_contains);
Datum
hstore_contains(PG_FUNCTION_ARGS)
{
	HStore	   *val = PG_GETARG_HSTORE_P(0);
	HStore	   *tmpl = PG_GETARG_HSTORE_P(1);
	bool		res = true;
	HEntry	   *te = ARRPTR(tmpl);
	char	   *tstr = STRPTR(tmpl);
	HEntry	   *ve = ARRPTR(val);
	char	   *vstr = STRPTR(val);
	int			tcount = HS_COUNT(tmpl);
	int			lastidx = 0;
	int			i;

	/*
	 * we exploit the fact that keys in "tmpl" are in strictly increasing
	 * order to narrow the hstoreFindKey search; each search can start one
	 * entry past the previous "found" entry, or at the lower bound of the
	 * search
	 */

	for (i = 0; res && i < tcount; ++i)
	{
		int			idx = hstoreFindKey(val, &lastidx,
										HSTORE_KEY(te, tstr, i),
										HSTORE_KEYLEN(te, i));

		if (idx >= 0)
		{
			bool		nullval = HSTORE_VALISNULL(te, i);
			int			vallen = HSTORE_VALLEN(te, i);

			if (nullval != HSTORE_VALISNULL(ve, idx) ||
				(!nullval && (vallen != HSTORE_VALLEN(ve, idx) ||
							  memcmp(HSTORE_VAL(te, tstr, i),
									 HSTORE_VAL(ve, vstr, idx),
									 vallen) != 0)))
				res = false;
		}
		else
			res = false;
	}

	PG_RETURN_BOOL(res);
}

PG_FUNCTION_INFO_V1(hstore_contained);
Datum
hstore_contained(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATUM(DirectFunctionCall2(hstore_contains,
										PG_GETARG_DATUM(1),
										PG_GETARG_DATUM(0)
										));
}

PG_FUNCTION_INFO_V1(hstore_cmp);
Datum
hstore_cmp(PG_FUNCTION_ARGS)
{
	HStore	   *hs1 = PG_GETARG_HSTORE_P(0);
	HStore	   *hs2 = PG_GETARG_HSTORE_P(1);
	int			hcount1 = HS_COUNT(hs1);
	int			hcount2 = HS_COUNT(hs2);
	int			res = 0;

	if (hcount1 == 0 || hcount2 == 0)
	{
		/*
		 * if either operand is empty, and the other is nonempty, the nonempty
		 * one is larger. If both are empty they are equal.
		 */
		if (hcount1 > 0)
			res = 1;
		else if (hcount2 > 0)
			res = -1;
	}
	else
	{
		/* here we know both operands are nonempty */
		char	   *str1 = STRPTR(hs1);
		char	   *str2 = STRPTR(hs2);
		HEntry	   *ent1 = ARRPTR(hs1);
		HEntry	   *ent2 = ARRPTR(hs2);
		size_t		len1 = HSE_ENDPOS(ent1[2 * hcount1 - 1]);
		size_t		len2 = HSE_ENDPOS(ent2[2 * hcount2 - 1]);

		res = memcmp(str1, str2, Min(len1, len2));

		if (res == 0)
		{
			if (len1 > len2)
				res = 1;
			else if (len1 < len2)
				res = -1;
			else if (hcount1 > hcount2)
				res = 1;
			else if (hcount2 > hcount1)
				res = -1;
			else
			{
				int			count = hcount1 * 2;
				int			i;

				for (i = 0; i < count; ++i)
					if (HSE_ENDPOS(ent1[i]) != HSE_ENDPOS(ent2[i]) ||
						HSE_ISNULL(ent1[i]) != HSE_ISNULL(ent2[i]))
						break;
				if (i < count)
				{
					if (HSE_ENDPOS(ent1[i]) < HSE_ENDPOS(ent2[i]))
						res = -1;
					else if (HSE_ENDPOS(ent1[i]) > HSE_ENDPOS(ent2[i]))
						res = 1;
					else if (HSE_ISNULL(ent1[i]))
						res = 1;
					else if (HSE_ISNULL(ent2[i]))
						res = -1;
				}
			}
		}
		else
		{
			res = (res > 0) ? 1 : -1;
		}
	}

	/*
	 * this is a btree support function; this is one of the few places where
	 * memory needs to be explicitly freed.
	 */
	PG_FREE_IF_COPY(hs1, 0);
	PG_FREE_IF_COPY(hs2, 1);
	PG_RETURN_INT32(res);
}

PG_FUNCTION_INFO_V1(hstore_eq);
Datum
hstore_eq(PG_FUNCTION_ARGS)
{
	int			res = DatumGetInt32(DirectFunctionCall2(hstore_cmp,
														PG_GETARG_DATUM(0),
														PG_GETARG_DATUM(1)));

	PG_RETURN_BOOL(res == 0);
}

PG_FUNCTION_INFO_V1(hstore_ne);
Datum
hstore_ne(PG_FUNCTION_ARGS)
{
	int			res = DatumGetInt32(DirectFunctionCall2(hstore_cmp,
														PG_GETARG_DATUM(0),
														PG_GETARG_DATUM(1)));

	PG_RETURN_BOOL(res != 0);
}

PG_FUNCTION_INFO_V1(hstore_gt);
Datum
hstore_gt(PG_FUNCTION_ARGS)
{
	int			res = DatumGetInt32(DirectFunctionCall2(hstore_cmp,
														PG_GETARG_DATUM(0),
														PG_GETARG_DATUM(1)));

	PG_RETURN_BOOL(res > 0);
}

PG_FUNCTION_INFO_V1(hstore_ge);
Datum
hstore_ge(PG_FUNCTION_ARGS)
{
	int			res = DatumGetInt32(DirectFunctionCall2(hstore_cmp,
														PG_GETARG_DATUM(0),
														PG_GETARG_DATUM(1)));

	PG_RETURN_BOOL(res >= 0);
}

PG_FUNCTION_INFO_V1(hstore_lt);
Datum
hstore_lt(PG_FUNCTION_ARGS)
{
	int			res = DatumGetInt32(DirectFunctionCall2(hstore_cmp,
														PG_GETARG_DATUM(0),
														PG_GETARG_DATUM(1)));

	PG_RETURN_BOOL(res < 0);
}

PG_FUNCTION_INFO_V1(hstore_le);
Datum
hstore_le(PG_FUNCTION_ARGS)
{
	int			res = DatumGetInt32(DirectFunctionCall2(hstore_cmp,
														PG_GETARG_DATUM(0),
														PG_GETARG_DATUM(1)));

	PG_RETURN_BOOL(res <= 0);
}

PG_FUNCTION_INFO_V1(hstore_hash);
Datum
hstore_hash(PG_FUNCTION_ARGS)
{
	HStore	   *hs = PG_GETARG_HSTORE_P(0);
	Datum		hval = hash_any((unsigned char *) VARDATA(hs),
								VARSIZE(hs) - VARHDRSZ);

	/*
	 * This (along with hstore_hash_extended) is the only place in the code
	 * that cares whether the overall varlena size exactly matches the true
	 * data size; this assertion should be maintained by all the other code,
	 * but we make it explicit here.
	 */
	Assert(VARSIZE(hs) ==
		   (HS_COUNT(hs) != 0 ?
			CALCDATASIZE(HS_COUNT(hs),
						 HSE_ENDPOS(ARRPTR(hs)[2 * HS_COUNT(hs) - 1])) :
			HSHRDSIZE));

	PG_FREE_IF_COPY(hs, 0);
	PG_RETURN_DATUM(hval);
}

PG_FUNCTION_INFO_V1(hstore_hash_extended);
Datum
hstore_hash_extended(PG_FUNCTION_ARGS)
{
	HStore	   *hs = PG_GETARG_HSTORE_P(0);
	uint64		seed = PG_GETARG_INT64(1);
	Datum		hval;

	hval = hash_any_extended((unsigned char *) VARDATA(hs),
							 VARSIZE(hs) - VARHDRSZ,
							 seed);

	/* See comment in hstore_hash */
	Assert(VARSIZE(hs) ==
		   (HS_COUNT(hs) != 0 ?
			CALCDATASIZE(HS_COUNT(hs),
						 HSE_ENDPOS(ARRPTR(hs)[2 * HS_COUNT(hs) - 1])) :
			HSHRDSIZE));

	PG_FREE_IF_COPY(hs, 0);
	PG_RETURN_DATUM(hval);
}

/* ==================== SECTION D: driver entries ==================== */
/* All entries: reset error state, setjmp; -1 = hard error (sqlstate via
 * pg_hst_sqlstate). Returned pointers live in the arena until the next
 * pg_hst_reset on the thread; the Rust driver copies eagerly. */

void
pg_hst_reset(void)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	for (size_t i = 0; i < hst_nallocs; i++)
		free(hst_allocs[i]);
	hst_nallocs = 0;
}

int
pg_hst_sqlstate(void)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	return hst_sqlstate;
}

#define HST_TRY() \
	hst_sqlstate = 0; \
	if (setjmp(hst_env) != 0) \
		return -1

static Datum
hst_call1(hst_pgfunc func, Datum a, bool *isnull, void *context)
{
	FunctionCallInfoBaseData fc;
	Datum		r;

	fc.context = context;
	fc.isnull = false;
	fc.nargs = 1;
	fc.args[0].value = a;
	fc.args[0].isnull = false;
	r = func(&fc);
	*isnull = fc.isnull;
	return r;
}

static Datum
hst_call2(hst_pgfunc func, Datum a, bool a_null, Datum b, bool b_null,
		  bool *isnull)
{
	FunctionCallInfoBaseData fc;
	Datum		r;

	fc.context = NULL;
	fc.isnull = false;
	fc.nargs = 2;
	fc.args[0].value = a;
	fc.args[0].isnull = a_null;
	fc.args[1].value = b;
	fc.args[1].isnull = b_null;
	r = func(&fc);
	*isnull = fc.isnull;
	return r;
}

/* hstore_in. mode: 0 hard, 1 soft. Returns 0 ok (img/len set), -1 hard
 * error, 1 soft error occurred (returned NULL; sqlstate via
 * pg_hst_soft_sqlstate). */
static _Thread_local Node hst_soft_node;

int
pg_hst_soft_sqlstate(void)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	return hst_soft_sqlstate;
}

int
pg_hst_in(const char *str, int soft, const unsigned char **img, int *imglen)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	bool		isnull = false;
	Datum		d;

	HST_TRY();
	hst_soft_node.error_occurred = 0;
	hst_soft_sqlstate = 0;
	d = hst_call1(hst_hstore_in, CStringGetDatum(str), &isnull,
				  soft ? (void *) &hst_soft_node : NULL);
	if (hst_soft_node.error_occurred)
		return 1;
	*img = (const unsigned char *) DatumGetPointer(d);
	*imglen = VARSIZE(DatumGetPointer(d));
	return 0;
}

int
pg_hst_recv(const unsigned char *wire, int wirelen,
			const unsigned char **img, int *imglen)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	StringInfoData buf;
	bool		isnull = false;
	Datum		d;

	HST_TRY();
	/* recv StringInfo shape: data/len/cursor over the wire bytes */
	buf.data = (char *) wire;
	buf.len = wirelen;
	buf.maxlen = wirelen;
	buf.cursor = 0;
	d = hst_call1(hst_hstore_recv, PointerGetDatum(&buf), &isnull, NULL);
	*img = (const unsigned char *) DatumGetPointer(d);
	*imglen = VARSIZE(DatumGetPointer(d));
	return 0;
}

int
pg_hst_out(const unsigned char *img, const char **out)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	bool		isnull = false;

	HST_TRY();
	*out = DatumGetCString(hst_call1(hst_hstore_out, PointerGetDatum(img),
									 &isnull, NULL));
	return 0;
}

int
pg_hst_send(const unsigned char *img, const unsigned char **out, int *outlen)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	bool		isnull = false;
	Datum		d;

	HST_TRY();
	d = hst_call1(hst_hstore_send, PointerGetDatum(img), &isnull, NULL);
	*out = (const unsigned char *) VARDATA(DatumGetPointer(d));
	*outlen = VARSIZE(DatumGetPointer(d)) - VARHDRSZ;
	return 0;
}

/* build a 4B-header text in the arena (driver-arg plumbing) */
static text *
hst_mk_text(const unsigned char *bytes, int len)
{
	text	   *t = hst_palloc(len + VARHDRSZ);

	SET_VARSIZE(t, len + VARHDRSZ);
	if (len > 0)
		memcpy(VARDATA(t), bytes, len);
	return t;
}

int
pg_hst_from_text(const unsigned char *key, int keylen, int key_null,
				 const unsigned char *val, int vallen, int val_null,
				 const unsigned char **img, int *imglen)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	bool		isnull = false;
	Datum		d;

	HST_TRY();
	d = hst_call2(hst_hstore_from_text,
				  key_null ? (Datum) 0 : PointerGetDatum(hst_mk_text(key, keylen)),
				  key_null != 0,
				  val_null ? (Datum) 0 : PointerGetDatum(hst_mk_text(val, vallen)),
				  val_null != 0,
				  &isnull);
	if (isnull)
		return 1;
	*img = (const unsigned char *) DatumGetPointer(d);
	*imglen = VARSIZE(DatumGetPointer(d));
	return 0;
}

int
pg_hst_from_arrays(const unsigned char *karr, int k_null,
				   const unsigned char *varr, int v_null,
				   const unsigned char **img, int *imglen)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	bool		isnull = false;
	Datum		d;

	HST_TRY();
	d = hst_call2(hst_hstore_from_arrays,
				  k_null ? (Datum) 0 : PointerGetDatum(karr), k_null != 0,
				  v_null ? (Datum) 0 : PointerGetDatum(varr), v_null != 0,
				  &isnull);
	if (isnull)
		return 1;
	*img = (const unsigned char *) DatumGetPointer(d);
	*imglen = VARSIZE(DatumGetPointer(d));
	return 0;
}

int
pg_hst_from_array(const unsigned char *arr,
				  const unsigned char **img, int *imglen)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	bool		isnull = false;
	Datum		d;

	HST_TRY();
	d = hst_call1(hst_hstore_from_array, PointerGetDatum(arr), &isnull, NULL);
	*img = (const unsigned char *) DatumGetPointer(d);
	*imglen = VARSIZE(DatumGetPointer(d));
	return 0;
}

/* img (X) text ops. ret: 0 = text out (out/outlen), 1 = NULL, -1 = error */
int
pg_hst_fetchval(const unsigned char *img, const unsigned char *key, int keylen,
				const unsigned char **out, int *outlen)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	bool		isnull = false;
	Datum		d;

	HST_TRY();
	d = hst_call2(hst_hstore_fetchval, PointerGetDatum(img), false,
				  PointerGetDatum(hst_mk_text(key, keylen)), false, &isnull);
	if (isnull)
		return 1;
	*out = (const unsigned char *) VARDATA(DatumGetPointer(d));
	*outlen = VARSIZE(DatumGetPointer(d)) - VARHDRSZ;
	return 0;
}

int
pg_hst_exists(const unsigned char *img, const unsigned char *key, int keylen)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	bool		isnull = false;

	HST_TRY();
	return DatumGetBool(hst_call2(hst_hstore_exists, PointerGetDatum(img), false,
								  PointerGetDatum(hst_mk_text(key, keylen)),
								  false, &isnull)) ? 1 : 0;
}

int
pg_hst_defined(const unsigned char *img, const unsigned char *key, int keylen)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	bool		isnull = false;

	HST_TRY();
	return DatumGetBool(hst_call2(hst_hstore_defined, PointerGetDatum(img), false,
								  PointerGetDatum(hst_mk_text(key, keylen)),
								  false, &isnull)) ? 1 : 0;
}

/* which: 0 exists_any, 1 exists_all, 2 contains, 3 contained */
int
pg_hst_bool2(int which, const unsigned char *a, const unsigned char *b)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	static hst_pgfunc const tab[] = {
		hst_hstore_exists_any, hst_hstore_exists_all,
		hst_hstore_contains, hst_hstore_contained,
	};
	bool		isnull = false;

	HST_TRY();
	return DatumGetBool(hst_call2(tab[which], PointerGetDatum(a), false,
								  PointerGetDatum(b), false, &isnull)) ? 1 : 0;
}

/* which: 0 delete(text), 1 delete_array, 2 delete_hstore, 3 concat,
 * 4 slice_to_hstore. b is a text image for 0, array image for 1/4, hstore
 * image for 2/3. Result is an hstore image. */
int
pg_hst_binop(int which, const unsigned char *a,
			 const unsigned char *b, int blen_for_text,
			 const unsigned char **img, int *imglen)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	static hst_pgfunc const tab[] = {
		hst_hstore_delete, hst_hstore_delete_array, hst_hstore_delete_hstore,
		hst_hstore_concat, hst_hstore_slice_to_hstore,
	};
	bool		isnull = false;
	Datum		bd;
	Datum		d;

	HST_TRY();
	if (which == 0)
		bd = PointerGetDatum(hst_mk_text(b, blen_for_text));
	else
		bd = PointerGetDatum(b);
	d = hst_call2(tab[which], PointerGetDatum(a), false, bd, false, &isnull);
	*img = (const unsigned char *) DatumGetPointer(d);
	*imglen = VARSIZE(DatumGetPointer(d));
	return 0;
}

/* which: 0 akeys, 1 avals, 2 to_array, 3 to_matrix; result = array image */
int
pg_hst_unop_array(int which, const unsigned char *a,
				  const unsigned char **img, int *imglen)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	static hst_pgfunc const tab[] = {
		hst_hstore_akeys, hst_hstore_avals,
		hst_hstore_to_array, hst_hstore_to_matrix,
	};
	bool		isnull = false;
	Datum		d;

	HST_TRY();
	d = hst_call1(tab[which], PointerGetDatum(a), &isnull, NULL);
	*img = (const unsigned char *) DatumGetPointer(d);
	*imglen = VARSIZE(DatumGetPointer(d));
	return 0;
}

int
pg_hst_slice_to_array(const unsigned char *a, const unsigned char *keys,
					  const unsigned char **img, int *imglen)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	bool		isnull = false;
	Datum		d;

	HST_TRY();
	d = hst_call2(hst_hstore_slice_to_array, PointerGetDatum(a), false,
				  PointerGetDatum(keys), false, &isnull);
	*img = (const unsigned char *) DatumGetPointer(d);
	*imglen = VARSIZE(DatumGetPointer(d));
	return 0;
}

/* out[0..6] = cmp, eq, ne, gt, ge, lt, le */
int
pg_hst_cmp_ops(const unsigned char *a, const unsigned char *b, int32 *out)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	bool		isnull = false;

	HST_TRY();
	out[0] = DatumGetInt32(hst_call2(hst_hstore_cmp, PointerGetDatum(a), false,
									 PointerGetDatum(b), false, &isnull));
	out[1] = DatumGetBool(hst_call2(hst_hstore_eq, PointerGetDatum(a), false,
									PointerGetDatum(b), false, &isnull));
	out[2] = DatumGetBool(hst_call2(hst_hstore_ne, PointerGetDatum(a), false,
									PointerGetDatum(b), false, &isnull));
	out[3] = DatumGetBool(hst_call2(hst_hstore_gt, PointerGetDatum(a), false,
									PointerGetDatum(b), false, &isnull));
	out[4] = DatumGetBool(hst_call2(hst_hstore_ge, PointerGetDatum(a), false,
									PointerGetDatum(b), false, &isnull));
	out[5] = DatumGetBool(hst_call2(hst_hstore_lt, PointerGetDatum(a), false,
									PointerGetDatum(b), false, &isnull));
	out[6] = DatumGetBool(hst_call2(hst_hstore_le, PointerGetDatum(a), false,
									PointerGetDatum(b), false, &isnull));
	return 0;
}

int
pg_hst_hash(const unsigned char *img, uint32 *out)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	bool		isnull = false;

	HST_TRY();
	*out = DatumGetUInt32(hst_call1(hst_hstore_hash, PointerGetDatum(img),
									&isnull, NULL));
	return 0;
}

int
pg_hst_hash_extended(const unsigned char *img, uint64 seed, uint64 *out)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	bool		isnull = false;

	HST_TRY();
	*out = DatumGetUInt64(hst_call2(hst_hstore_hash_extended,
									PointerGetDatum(img), false,
									Int64GetDatum((int64) seed), false,
									&isnull));
	return 0;
}

/* parse-only entry: runs the verbatim parser (hard mode) and exposes the
 * PRE-hstoreUniquePairs pairs — the tie-relaxation candidate set for the
 * duplicate-key survivor non-surface (pg_qsort tie order vs stable sort;
 * PG documents the surviving duplicate as unspecified). */
static _Thread_local HSParser hst_pp_state;

int
pg_hst_parse_pairs(const char *str)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	HST_TRY();
	hst_pp_state.begin = (char *) str;
	hst_pp_state.escontext = NULL;
	if (!parse_hstore(&hst_pp_state))
		return -2;				/* unreachable in hard mode */
	return hst_pp_state.pcur;
}

void
pg_hst_parse_pair(int i, const char **k, int *klen,
				  const char **v, int *vlen, int *isnull)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	*k = hst_pp_state.pairs[i].key;
	*klen = (int) hst_pp_state.pairs[i].keylen;
	*v = hst_pp_state.pairs[i].val;
	*vlen = (int) hst_pp_state.pairs[i].vallen;
	*isnull = hst_pp_state.pairs[i].isnull ? 1 : 0;
}

/* loose: 0 hstore_to_json, 1 hstore_to_json_loose. out = text payload */
int
pg_hst_to_json(const unsigned char *img, int loose,
			   const unsigned char **out, int *outlen)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	bool		isnull = false;
	Datum		d;

	HST_TRY();
	d = hst_call1(loose ? hst_hstore_to_json_loose : hst_hstore_to_json,
				  PointerGetDatum(img), &isnull, NULL);
	*out = (const unsigned char *) VARDATA(DatumGetPointer(d));
	*outlen = VARSIZE(DatumGetPointer(d)) - VARHDRSZ;
	return 0;
}
