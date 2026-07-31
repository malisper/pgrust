/*
 * SHIM postgres.h for the jsonbfam differential-fuzz oracle — NOT PostgreSQL
 * code. Minimal environment so the VERBATIM vendored TUs under csrc/jsonbfam/
 * (jsonapi.c, wchar.c, stringinfo.c, jsonb_util.c, qsort_arg.c and the
 * extracted jsonb.c / jsonfuncs.c / numeric.c / pqformat.c segments) compile
 * natively. Plumbing only, never logic:
 *   - c.h type/macro subset (LP64), Datum + conversion macros, varatt.h
 *     (vendored verbatim, included below).
 *   - palloc family -> TLS pointer arena (pg_jsonbfam_palloc, driver file);
 *     models PG memory-context reset so error-path longjmps cannot leak.
 *   - ereport/errsave/ereturn -> record an errcode class in the shared
 *     pg_diff_errcode channel and longjmp (pg_jsonbfam_error_jmp). errmsg
 *     and friends swallow their arguments (message text out of scope).
 *   - CHECK_FOR_INTERRUPTS/check_stack_depth -> no-ops (driver caps input
 *     size and nesting depth; documented in the target header).
 * Environment pins: server encoding UTF8, C locale, no client-encoding
 * conversion (pq_sendtext/pq_getmsgtext identity), LP64.
 */
#ifndef PG_JSONBFAM_SHIM_POSTGRES_H
#define PG_JSONBFAM_SHIM_POSTGRES_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include <string.h>
#include <stdlib.h>
#include <stdarg.h>
#include <limits.h>
#include <setjmp.h>
#include <ctype.h>
#include <errno.h>

/*
 * SYMBOL ISOLATION (build.rs landing-fix pattern, merge/p1-wave1
 * 2026-07-30): every external symbol this family defines is renamed with a
 * jbfam_ prefix so it can never cross-bind against another lane's vendored
 * copy of the same C name (psprintf/hash_any/GetDatabaseEncoding/... all
 * exist in sibling oracles; first-definition-wins archive binding mixed
 * allocators and aborted in the arena pfree guard). pg_diff_jsonb_* driver
 * entries, pg_jsonbfam_* shim exports, the shared pg_diff_errcode channel
 * and the float4in/8in_internal externs (defined in pg_float_io.c) keep
 * their names.
 */
#define appendBinaryStringInfo jbfam_appendBinaryStringInfo
#define appendBinaryStringInfoNT jbfam_appendBinaryStringInfoNT
#define appendStringInfo jbfam_appendStringInfo
#define appendStringInfoChar jbfam_appendStringInfoChar
#define appendStringInfoSpaces jbfam_appendStringInfoSpaces
#define appendStringInfoString jbfam_appendStringInfoString
#define appendStringInfoVA jbfam_appendStringInfoVA
#define compareJsonbContainers jbfam_compareJsonbContainers
#define cstring_to_text jbfam_cstring_to_text
#define cstring_to_text_with_len jbfam_cstring_to_text_with_len
#define destroyStringInfo jbfam_destroyStringInfo
#define DirectFunctionCall1Coll jbfam_DirectFunctionCall1Coll
#define DirectFunctionCall2Coll jbfam_DirectFunctionCall2Coll
#define DirectFunctionCall3Coll jbfam_DirectFunctionCall3Coll
#define DirectInputFunctionCallSafe jbfam_DirectInputFunctionCallSafe
#define enlargeStringInfo jbfam_enlargeStringInfo
#define escape_json jbfam_escape_json
#define escape_json_with_len jbfam_escape_json_with_len
#define findJsonbValueFromContainer jbfam_findJsonbValueFromContainer
#define float4in jbfam_float4in
#define float8in jbfam_float8in
#define freeJsonLexContext jbfam_freeJsonLexContext
#define GetDatabaseEncoding jbfam_GetDatabaseEncoding
#define GetDatabaseEncodingName jbfam_GetDatabaseEncodingName
#define getIthJsonbValueFromContainer jbfam_getIthJsonbValueFromContainer
#define getJsonbLength jbfam_getJsonbLength
#define getJsonbOffset jbfam_getJsonbOffset
#define getKeyJsonValueFromContainer jbfam_getKeyJsonValueFromContainer
#define hash_any jbfam_hash_any
#define hash_any_extended jbfam_hash_any_extended
#define hash_numeric jbfam_hash_numeric
#define hash_numeric_extended jbfam_hash_numeric_extended
#define hashchar jbfam_hashchar
#define hashcharextended jbfam_hashcharextended
#define initStringInfo jbfam_initStringInfo
#define initStringInfoExt jbfam_initStringInfoExt
#define IsValidJsonNumber jbfam_IsValidJsonNumber
#define json_count_array_elements jbfam_json_count_array_elements
#define json_errdetail jbfam_json_errdetail
#define json_errsave_error jbfam_json_errsave_error
#define json_lex jbfam_json_lex
#define jsonb_array_length jbfam_jsonb_array_length
#define jsonb_bool jbfam_jsonb_bool
#define jsonb_build_array_noargs jbfam_jsonb_build_array_noargs
#define jsonb_build_array_worker jbfam_jsonb_build_array_worker
#define jsonb_build_object_noargs jbfam_jsonb_build_object_noargs
#define jsonb_build_object_worker jbfam_jsonb_build_object_worker
#define jsonb_float4 jbfam_jsonb_float4
#define jsonb_float8 jbfam_jsonb_float8
#define jsonb_from_text jbfam_jsonb_from_text
#define jsonb_in jbfam_jsonb_in
#define jsonb_int2 jbfam_jsonb_int2
#define jsonb_int4 jbfam_jsonb_int4
#define jsonb_int8 jbfam_jsonb_int8
#define jsonb_numeric jbfam_jsonb_numeric
#define jsonb_out jbfam_jsonb_out
#define jsonb_pretty jbfam_jsonb_pretty
#define jsonb_recv jbfam_jsonb_recv
#define jsonb_send jbfam_jsonb_send
#define jsonb_strip_nulls jbfam_jsonb_strip_nulls
#define jsonb_typeof jbfam_jsonb_typeof
#define JsonbDeepContains jbfam_JsonbDeepContains
#define JsonbExtractScalar jbfam_JsonbExtractScalar
#define JsonbHashScalarValue jbfam_JsonbHashScalarValue
#define JsonbHashScalarValueExtended jbfam_JsonbHashScalarValueExtended
#define JsonbIteratorInit jbfam_JsonbIteratorInit
#define JsonbIteratorNext jbfam_JsonbIteratorNext
#define JsonbToCString jbfam_JsonbToCString
#define JsonbToCStringIndent jbfam_JsonbToCStringIndent
#define JsonbToJsonbValue jbfam_JsonbToJsonbValue
#define JsonbTypeName jbfam_JsonbTypeName
#define JsonbValueToJsonb jbfam_JsonbValueToJsonb
#define JsonEncodeDateTime jbfam_JsonEncodeDateTime
#define makeJsonLexContext jbfam_makeJsonLexContext
#define makeJsonLexContextCstringLen jbfam_makeJsonLexContextCstringLen
#define makeJsonLexContextIncremental jbfam_makeJsonLexContextIncremental
#define makeStringInfo jbfam_makeStringInfo
#define makeStringInfoExt jbfam_makeStringInfoExt
#define nullSemAction jbfam_nullSemAction
#define numeric_cmp jbfam_numeric_cmp
#define numeric_eq jbfam_numeric_eq
#define numeric_float4 jbfam_numeric_float4
#define numeric_float8 jbfam_numeric_float8
#define numeric_in jbfam_numeric_in
#define numeric_int2 jbfam_numeric_int2
#define numeric_int4 jbfam_numeric_int4
#define numeric_int4_opt_error jbfam_numeric_int4_opt_error
#define numeric_int8 jbfam_numeric_int8
#define numeric_int8_opt_error jbfam_numeric_int8_opt_error
#define numeric_is_inf jbfam_numeric_is_inf
#define numeric_is_nan jbfam_numeric_is_nan
#define numeric_out jbfam_numeric_out
#define pg_client_to_server jbfam_pg_client_to_server
#define pg_detoast_datum jbfam_pg_detoast_datum
#define pg_detoast_datum_copy jbfam_pg_detoast_datum_copy
#define pg_detoast_datum_packed jbfam_pg_detoast_datum_packed
#define pg_encoding_dsplen jbfam_pg_encoding_dsplen
#define pg_encoding_max_length jbfam_pg_encoding_max_length
#define pg_encoding_mblen jbfam_pg_encoding_mblen
#define pg_encoding_mblen_bounded jbfam_pg_encoding_mblen_bounded
#define pg_encoding_mblen_or_incomplete jbfam_pg_encoding_mblen_or_incomplete
#define pg_encoding_set_invalid jbfam_pg_encoding_set_invalid
#define pg_encoding_verifymbchar jbfam_pg_encoding_verifymbchar
#define pg_encoding_verifymbstr jbfam_pg_encoding_verifymbstr
#define pg_mblen_range jbfam_pg_mblen_range
#define pg_mule_mblen jbfam_pg_mule_mblen
#define pg_parse_json jbfam_pg_parse_json
#define pg_parse_json_incremental jbfam_pg_parse_json_incremental
#define pg_parse_json_or_errsave jbfam_pg_parse_json_or_errsave
#define pg_server_to_client jbfam_pg_server_to_client
#define pg_unicode_to_server_noerror jbfam_pg_unicode_to_server_noerror
#define pg_utf_mblen_private jbfam_pg_utf_mblen_private
#define pg_utf8_islegal jbfam_pg_utf8_islegal
#define pg_wchar_table jbfam_pg_wchar_table
#define pq_begintypsend jbfam_pq_begintypsend
#define pq_copymsgbytes jbfam_pq_copymsgbytes
#define pq_endtypsend jbfam_pq_endtypsend
#define pq_getmsgbytes jbfam_pq_getmsgbytes
#define pq_getmsgend jbfam_pq_getmsgend
#define pq_getmsgint jbfam_pq_getmsgint
#define pq_getmsgtext jbfam_pq_getmsgtext
#define pq_sendtext jbfam_pq_sendtext
#define psprintf jbfam_psprintf
#define pushJsonbValue jbfam_pushJsonbValue
#define pvsnprintf jbfam_pvsnprintf
#define qsort_arg jbfam_qsort_arg
#define resetStringInfo jbfam_resetStringInfo
#define setJsonLexContextOwnsTokens jbfam_setJsonLexContextOwnsTokens
#define text_to_cstring jbfam_text_to_cstring
#define varstr_cmp jbfam_varstr_cmp

/* jsonbops_diff additions (p1-lanev): every extern the ops extension
 * defines, same isolation rule as above. */
#define ArrayGetNItems jbfam_ArrayGetNItems
#define ArrayGetNItemsSafe jbfam_ArrayGetNItemsSafe
#define array_contains_nulls jbfam_array_contains_nulls
#define check_collation_set jbfam_check_collation_set
#define deconstruct_array jbfam_deconstruct_array
#define deconstruct_array_builtin jbfam_deconstruct_array_builtin
#define hash_bytes jbfam_hash_bytes
#define hash_bytes_extended jbfam_hash_bytes_extended
#define hash_bytes_uint32 jbfam_hash_bytes_uint32
#define hash_bytes_uint32_extended jbfam_hash_bytes_uint32_extended
#define jsonb_array_element jbfam_jsonb_array_element
#define jsonb_array_element_text jbfam_jsonb_array_element_text
#define jsonb_cmp jbfam_jsonb_cmp
#define jsonb_concat jbfam_jsonb_concat
#define jsonb_contained jbfam_jsonb_contained
#define jsonb_contains jbfam_jsonb_contains
#define jsonb_delete jbfam_jsonb_delete
#define jsonb_delete_array jbfam_jsonb_delete_array
#define jsonb_delete_idx jbfam_jsonb_delete_idx
#define jsonb_delete_path jbfam_jsonb_delete_path
#define jsonb_eq jbfam_jsonb_eq
#define jsonb_exists jbfam_jsonb_exists
#define jsonb_exists_all jbfam_jsonb_exists_all
#define jsonb_exists_any jbfam_jsonb_exists_any
#define jsonb_extract_path jbfam_jsonb_extract_path
#define jsonb_extract_path_text jbfam_jsonb_extract_path_text
#define jsonb_ge jbfam_jsonb_ge
#define jsonb_get_element jbfam_jsonb_get_element
#define jsonb_gt jbfam_jsonb_gt
#define jsonb_hash jbfam_jsonb_hash
#define jsonb_hash_extended jbfam_jsonb_hash_extended
#define jsonb_insert jbfam_jsonb_insert
#define jsonb_le jbfam_jsonb_le
#define jsonb_lt jbfam_jsonb_lt
#define jsonb_ne jbfam_jsonb_ne
#define jsonb_object jbfam_jsonb_object
#define jsonb_object_field jbfam_jsonb_object_field
#define jsonb_object_field_text jbfam_jsonb_object_field_text
#define jsonb_object_two_arg jbfam_jsonb_object_two_arg
#define jsonb_set jbfam_jsonb_set
#define strtoint jbfam_strtoint

/* ---------------- c.h subset ---------------- */

typedef int8_t int8;
typedef int16_t int16;
typedef int32_t int32;
typedef int64_t int64;
typedef uint8_t uint8;
typedef uint16_t uint16;
typedef uint32_t uint32;
typedef uint64_t uint64;
typedef float float4;
typedef double float8;
typedef size_t Size;
typedef unsigned int Oid;
#define InvalidOid ((Oid) 0)
#define OidIsValid(objectId)  ((bool) ((objectId) != InvalidOid))
typedef uintptr_t Datum;
typedef int32 fixed_part;		/* unused placeholder */

#define UINT64CONST(x) UINT64_C(x)
#define INT64CONST(x) INT64_C(x)
#define PG_INT8_MIN   INT8_MIN
#define PG_INT8_MAX   INT8_MAX
#define PG_INT16_MIN  INT16_MIN
#define PG_INT16_MAX  INT16_MAX
#define PG_INT32_MIN  INT32_MIN
#define PG_INT32_MAX  INT32_MAX
#define PG_INT64_MIN  INT64_MIN
#define PG_INT64_MAX  INT64_MAX
#define PG_UINT8_MAX  UINT8_MAX
#define PG_UINT16_MAX UINT16_MAX
#define PG_UINT32_MAX UINT32_MAX
#define PG_UINT64_MAX UINT64_MAX

#define ALIGNOF_DOUBLE 8
#define ALIGNOF_INT 4
#define ALIGNOF_SHORT 2
#define MAXIMUM_ALIGNOF 8

#define TYPEALIGN(ALIGNVAL,LEN)  \
	(((uintptr_t) (LEN) + ((ALIGNVAL) - 1)) & ~((uintptr_t) ((ALIGNVAL) - 1)))
#define SHORTALIGN(LEN)			TYPEALIGN(ALIGNOF_SHORT, (LEN))
#define INTALIGN(LEN)			TYPEALIGN(ALIGNOF_INT, (LEN))
#define MAXALIGN(LEN)			TYPEALIGN(MAXIMUM_ALIGNOF, (LEN))
#define DOUBLEALIGN(LEN)		TYPEALIGN(ALIGNOF_DOUBLE, (LEN))

#define lengthof(array) (sizeof (array) / sizeof ((array)[0]))
#define Min(x, y)		((x) < (y) ? (x) : (y))
#define Max(x, y)		((x) > (y) ? (x) : (y))
#define Abs(x)			((x) >= 0 ? (x) : -(x))

#define HIGHBIT					(0x80)
#define IS_HIGHBIT_SET(ch)		((unsigned char)(ch) & HIGHBIT)

#ifndef unlikely
#define unlikely(x) __builtin_expect((x) != 0, 0)
#define likely(x) __builtin_expect((x) != 0, 1)
#endif

#define pg_attribute_unused() __attribute__((unused))
#define pg_attribute_noreturn() __attribute__((noreturn))
#define pg_attribute_always_inline __attribute__((always_inline)) inline
#define pg_attribute_packed() __attribute__((packed))
#define pg_noinline __attribute__((noinline))
#define pg_nodiscard
#define PG_USED_FOR_ASSERTS_ONLY pg_attribute_unused()
#define pg_attribute_printf(f,a) __attribute__((format(printf, f, a)))
#define pg_attribute_nonnull(...)
#define pg_attribute_target(...)
#define pg_unreachable() __builtin_unreachable()
#define pg_restrict __restrict

#define StaticAssertStmt(cond, msg) _Static_assert(cond, msg)
#define StaticAssertDecl(cond, msg) _Static_assert(cond, msg)
#define StaticAssertExpr(cond, msg) ((void) 0)

/* NDEBUG-style build: Assert compiled out exactly like a release PG */
#ifndef Assert
#define Assert(x) ((void) 0)
#endif
#define AssertMacro(x) ((void) 0)

/* compiler barrier-ish helpers some headers want */
#define pg_memory_barrier() __sync_synchronize()

/* HAVE_ macros so vendored headers pick the builtin paths (clang/gcc) */
#define HAVE__BUILTIN_CLZ 1
#define HAVE__BUILTIN_CTZ 1
#define HAVE__BUILTIN_POPCOUNT 1
#define HAVE__BUILTIN_BSWAP16 1
#define HAVE__BUILTIN_BSWAP32 1
#define HAVE__BUILTIN_BSWAP64 1
#define HAVE__BUILTIN_OP_OVERFLOW 1
#define HAVE__BUILTIN_UNREACHABLE 1
#define HAVE_LONG_INT_64 1
#define USE_FLOAT8_BYVAL 1
#define SIZEOF_DATUM 8
#define SIZEOF_VOID_P 8
#define SIZEOF_SIZE_T 8
#define SIZEOF_LONG 8
#define INT64_FORMAT "%ld"
#define UINT64_FORMAT "%lu"
#define PG_INT128_TYPE __int128
typedef PG_INT128_TYPE int128;
typedef unsigned PG_INT128_TYPE uint128;
#define HAVE_INT128 1

#define FLEXIBLE_ARRAY_MEMBER	/* empty */
#define PGDLLIMPORT
#define PGDLLEXPORT

#define gettext_noop(x) (x)
#define _(x) (x)

/* ---------------- Datum conversions (postgres.h subset, LP64) -------- */

#define DatumGetBool(X) ((bool) ((X) != 0))
#define BoolGetDatum(X) ((Datum) ((X) ? 1 : 0))
#define DatumGetChar(X) ((char) (X))
#define CharGetDatum(X) ((Datum) (X))
#define DatumGetInt8(X) ((int8) (X))
#define Int8GetDatum(X) ((Datum) (X))
#define DatumGetUInt8(X) ((uint8) (X))
#define UInt8GetDatum(X) ((Datum) (X))
#define DatumGetInt16(X) ((int16) (X))
#define Int16GetDatum(X) ((Datum) (X))
#define DatumGetUInt16(X) ((uint16) (X))
#define UInt16GetDatum(X) ((Datum) (X))
#define DatumGetInt32(X) ((int32) (X))
#define Int32GetDatum(X) ((Datum) (X))
#define DatumGetUInt32(X) ((uint32) (X))
#define UInt32GetDatum(X) ((Datum) (X))
#define DatumGetInt64(X) ((int64) (X))
#define Int64GetDatum(X) ((Datum) (X))
#define DatumGetUInt64(X) ((uint64) (X))
#define UInt64GetDatum(X) ((Datum) (X))
#define DatumGetObjectId(X) ((Oid) (X))
#define ObjectIdGetDatum(X) ((Datum) (X))
#define DatumGetPointer(X) ((void *) (X))
#define PointerGetDatum(X) ((Datum) (X))
#define DatumGetCString(X) ((char *) DatumGetPointer(X))
#define CStringGetDatum(X) PointerGetDatum(X)

static inline float8
DatumGetFloat8(Datum X)
{
	union { int64 value; float8 retval; } myunion;
	myunion.value = (int64) X;
	return myunion.retval;
}

static inline Datum
Float8GetDatum(float8 X)
{
	union { float8 value; int64 retval; } myunion;
	myunion.value = X;
	return (Datum) myunion.retval;
}

static inline float4
DatumGetFloat4(Datum X)
{
	union { int32 value; float4 retval; } myunion;
	myunion.value = (int32) X;
	return myunion.retval;
}

static inline Datum
Float4GetDatum(float4 X)
{
	union { float4 value; int32 retval; } myunion;
	myunion.value = X;
	return (Datum) (uint32) myunion.retval;
}

/* varlena basics live in the vendored varatt.h */
struct varlena
{
	char		vl_len_[4];
	char		vl_dat[FLEXIBLE_ARRAY_MEMBER];
};
typedef struct varlena bytea;
typedef struct varlena text;
typedef struct varlena BpChar;
typedef struct varlena VarChar;

#include "varatt.h"

#define VARHDRSZ ((int32) sizeof(int32))

/* SET_VARSIZE et al come from varatt.h */

/* Node: only escontext plumbing flows through here; always NULL. */
typedef struct Node Node;

/* ---------------- error reporting shim ---------------- */

/*
 * Class constants for pg_diff_errcode (shared TLS channel defined in
 * csrc/pg_float_io.c; classes 1 and 2 MUST stay aligned with that file
 * because numeric_float4/8 reuse its float4in/8in_internal, which soft-set
 * classes 1/2 there).
 */
#define ERRCODE_INVALID_TEXT_REPRESENTATION 1	/* 22P02 */
#define ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE 2	/* 22003 */
#define ERRCODE_INVALID_PARAMETER_VALUE 3	/* 22023 */
#define ERRCODE_PROGRAM_LIMIT_EXCEEDED 4	/* 54000 */
#define ERRCODE_UNTRANSLATABLE_CHARACTER 5	/* 22P05 */
#define ERRCODE_INVALID_ESCAPE_SEQUENCE 9	/* 22025 */
#define ERRCODE_INTERNAL_ERROR 6	/* XX000 (elog) */
#define ERRCODE_PROTOCOL_VIOLATION 7	/* 08P01 */
#define ERRCODE_STATEMENT_TOO_COMPLEX 8 /* 54001 */
#define ERRCODE_FEATURE_NOT_SUPPORTED 6 /* unreachable under UTF8 pin */
#define ERRCODE_UNIQUE_VIOLATION 6		/* unreachable: unique_keys=false */
#define ERRCODE_DUPLICATE_JSON_OBJECT_KEY_VALUE 6	/* as above */
#define ERRCODE_DATATYPE_MISMATCH 6
#define ERRCODE_DATA_CORRUPTED 6
#define ERRCODE_SYNTAX_ERROR 6
/* jsonbops_diff additions (p1-lanev): REACHABLE via the text[]-driven ops
 * (jsonb_object null key / setPath null path element / dim errors), so they
 * carry their own classes; Rust map in jsonbio_diff.rs err_class. */
#define ERRCODE_ARRAY_SUBSCRIPT_ERROR 10	/* 2202E */
#define ERRCODE_NULL_VALUE_NOT_ALLOWED 11	/* 22004 */
#define ERRCODE_INDETERMINATE_COLLATION 6	/* unreachable: collid pinned valid */

extern _Thread_local int pg_diff_errcode;	/* defined in pg_float_io.c */
extern _Thread_local jmp_buf pg_jsonbfam_jmp;	/* defined in driver */
extern void pg_jsonbfam_error_raise(void) __attribute__((noreturn));

#define ERROR 21
#define WARNING 19
#define NOTICE 18
#define DEBUG1 14
#define LOG 15

/* errcode() RECORDS; the surrounding ereport/errsave raises. */
static inline int
errcode(int c)
{
	pg_diff_errcode = c;
	return 0;
}

static inline int errmsg(const char *fmt,...) { (void) fmt; return 0; }
static inline int errmsg_internal(const char *fmt,...) { (void) fmt; return 0; }
static inline int errdetail(const char *fmt,...) { (void) fmt; return 0; }
static inline int errdetail_internal(const char *fmt,...) { (void) fmt; return 0; }
static inline int errhint(const char *fmt,...) { (void) fmt; return 0; }
static inline int errcontext_msg(const char *fmt,...) { (void) fmt; return 0; }
#define errcontext errcontext_msg

/*
 * ereport: evaluate the args (recording any errcode), then raise for
 * ERROR-and-up. elog(ERROR) records class 6 (internal).
 */
#define ereport(elevel, ...) \
	do { \
		(void) (__VA_ARGS__); \
		if ((elevel) >= ERROR) \
			pg_jsonbfam_error_raise(); \
	} while (0)

#define elog(elevel, ...) \
	do { \
		if ((elevel) >= ERROR) \
		{ \
			pg_diff_errcode = ERRCODE_INTERNAL_ERROR; \
			pg_jsonbfam_error_raise(); \
		} \
	} while (0)

/*
 * Soft-error plumbing: escontext is ALWAYS NULL in this oracle (the fc
 * calls under test pass no ErrorSaveContext), so errsave == ereport(ERROR)
 * and ereturn's return value is never used. SOFT_ERROR_OCCURRED(NULL) is
 * false.
 */
#define errsave(escontext, ...) ereport(ERROR, __VA_ARGS__)
#define ereturn(escontext, dummy_value, ...) \
	do { errsave(escontext, __VA_ARGS__); return dummy_value; } while (0)
#define SOFT_ERROR_OCCURRED(escontext) ((escontext) != NULL && false)

/* ---------------- memory management shim ---------------- */

extern void *pg_jsonbfam_palloc(Size n);
extern void *pg_jsonbfam_palloc0(Size n);
extern void *pg_jsonbfam_repalloc(void *p, Size n);
extern void pg_jsonbfam_pfree(void *p);
extern char *pg_jsonbfam_pstrdup(const char *s);

#define palloc(n) pg_jsonbfam_palloc(n)
#define palloc0(n) pg_jsonbfam_palloc0(n)
#define repalloc(p, n) pg_jsonbfam_repalloc((p), (n))
#define pfree(p) pg_jsonbfam_pfree(p)
#define pstrdup(s) pg_jsonbfam_pstrdup(s)
#define palloc_extended(n, flags) pg_jsonbfam_palloc(n)
#define repalloc_extended(p, n, flags) pg_jsonbfam_repalloc((p), (n))
#define palloc_array(type, count) ((type *) palloc(sizeof(type) * (count)))
#define palloc0_array(type, count) ((type *) palloc0(sizeof(type) * (count)))
#define palloc_object(type) ((type *) palloc(sizeof(type)))
#define repalloc_array(pointer, type, count) \
	((type *) repalloc(pointer, sizeof(type) * (count)))

#define MaxAllocSize ((Size) 0x3fffffff)
#define MaxAllocHugeSize (SIZE_MAX / 2)
#define AllocSizeIsValid(size) ((Size) (size) <= MaxAllocSize)

/* pvsnprintf: plumbing for appendStringInfoVA (driver TU implements) */
extern size_t pvsnprintf(char *buf, size_t len, const char *fmt, va_list args)
			pg_attribute_printf(3, 0);

/* interrupt/stack plumbing: driver bounds input size + nesting instead */
#define CHECK_FOR_INTERRUPTS() ((void) 0)
static inline void check_stack_depth(void) {}



/* pg_noreturn (c.h) */
#define pg_noreturn _Noreturn

/* c.h token paste helpers */
#define CppConcat(x, y) x##y
#define CppAsString(identifier) #identifier
#define CppAsString2(x) CppAsString(x)

/* port.h qsort_arg (typedef + proto, verbatim signatures) */
typedef int (*qsort_arg_comparator) (const void *a, const void *b, void *arg);
extern void qsort_arg(void *base, size_t nel, size_t elsize, qsort_arg_comparator cmp, void *arg);

/* c.h bit-string types + palloc.h psprintf (driver TU implements) */
typedef uint8 bits8;
typedef uint16 bits16;
typedef uint32 bits32;
extern char *psprintf(const char *fmt,...) pg_attribute_printf(1, 2);

/* c.h iNNabs helpers (verbatim one-liners) */
static inline int16 i16abs(int16 i) { return (int16) (i < 0 ? -i : i); }
static inline int32 i32abs(int32 i) { return (i < 0 ? -i : i); }
static inline int64 i64abs(int64 i) { return (i < 0 ? -i : i); }



#endif							/* PG_JSONBFAM_SHIM_POSTGRES_H */
