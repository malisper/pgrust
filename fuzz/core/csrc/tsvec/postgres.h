/*
 * SHIM postgres.h (tsvec oracle) — NOT PostgreSQL code.
 *
 * Minimal environment so the VERBATIM vendored files in this directory
 * (tsvector.c, tsvector_parser.c, tsvector_op.c, qsort.c, qsort_arg.c,
 * copied byte-identical — modulo labeled `#if 0 PG_DIFF CARVE` blocks in
 * tsvector_op.c — from postgres-src @
 * 62d6c7d3df6287f1bd83199c1a746e50d31571a0, PostgreSQL 18.3 Stamp-18.3)
 * compile standalone for the native differential-fuzz build.
 *
 * PLUMBING ONLY, never logic:
 *   - fixed-width typedefs matching c.h on LP64; Datum/varlena/text per
 *     src/include/c.h + postgres.h (layout-identical fields the vendored
 *     code touches).
 *   - palloc/palloc0/repalloc/pfree -> TLS pointer arena in
 *     pg_tsvector_core_io.c (models memory-context reset; error-path
 *     longjmps cannot leak).
 *   - ereport/errcode/errmsg/elog -> TLS errcode capture + longjmp
 *     (csrc/pg_geo_io.c precedent); errsave/ereturn implement the real
 *     soft-error contract: with a live ErrorSaveContext the error is
 *     recorded and control RETURNS, otherwise it throws like ereport.
 *   - mini-fmgr: FunctionCallInfoBaseData, PG_GETARG_ and PG_RETURN_ macros
     carrying exactly what the Datum functions read; PG_DETOAST_DATUM is
 *     identity: harness inputs are never toasted or short (documented carve).
 *   - ENCODING PIN: database encoding is pinned to UTF-8 on both sides
 *     (pg_mblen_cstr/pg_mblen_range/pg_database_encoding_max_length are
 *     pg_utf_mblen-based; the Rust driver calls SetDatabaseEncoding(UTF8)).
 *     Harness feeds valid UTF-8 only, mirroring the server's pg_verifymbstr
 *     precondition on cstring input.
 */
#ifndef PG_DIFFFUZZ_TSVEC_POSTGRES_H
#define PG_DIFFFUZZ_TSVEC_POSTGRES_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <ctype.h>
#include <limits.h>

typedef int8_t int8;
typedef int16_t int16;
typedef int32_t int32;
typedef int64_t int64;
typedef uint8_t uint8;
typedef uint16_t uint16;
typedef uint32_t uint32;
typedef uint64_t uint64;
typedef double float8;
typedef float float4;
typedef size_t Size;
typedef unsigned int Oid;
typedef uintptr_t Datum;

#define UINT64CONST(x) UINT64_C(x)
#define INT64CONST(x) INT64_C(x)
#define FLEXIBLE_ARRAY_MEMBER	/* empty */
#define pg_attribute_unused()
#define pg_attribute_noreturn()
#define pg_noinline
#define pg_nodiscard
#define PGDLLIMPORT
#define INT64_FORMAT "%lld"

#ifndef Assert
#define Assert(x) ((void) 0)
#endif
#define AssertMacro(x) ((void) 0)
#define StaticAssertStmt(cond, msg) ((void) 0)
#define StaticAssertDecl(cond, msg) struct pg_static_assert_dummy

#ifndef unlikely
#define unlikely(x) (x)
#endif
#ifndef likely
#define likely(x) (x)
#endif

#define CppConcat(x, y) x##y

#define PG_INT8_MIN (-0x7F - 1)
#define PG_INT8_MAX (0x7F)
#define PG_UINT8_MAX (0xFF)
#define PG_INT16_MIN (-0x7FFF - 1)
#define PG_INT16_MAX (0x7FFF)
#define PG_UINT16_MAX (0xFFFF)
#define PG_INT32_MIN (-0x7FFFFFFF - 1)
#define PG_INT32_MAX (0x7FFFFFFF)
#define PG_UINT32_MAX (0xFFFFFFFFU)
#define PG_INT64_MIN (-INT64CONST(0x7FFFFFFFFFFFFFFF) - 1)
#define PG_INT64_MAX INT64CONST(0x7FFFFFFFFFFFFFFF)
#define PG_UINT64_MAX UINT64CONST(0xFFFFFFFFFFFFFFFF)

#define Min(x, y) ((x) < (y) ? (x) : (y))
#define Max(x, y) ((x) > (y) ? (x) : (y))

/* c.h MemSet is a word-wise memset optimization with identical semantics;
 * plain memset is the same computation (plumbing shim) */
#define MemSet(start, val, len) memset((start), (val), (len))
#define lengthof(array) (sizeof(array) / sizeof((array)[0]))

#define SHORTALIGN(LEN) (((uintptr_t) (LEN) + 1) & ~((uintptr_t) 1))
#define MAXALIGN(LEN) (((uintptr_t) (LEN) + 7) & ~((uintptr_t) 7))

#define i64abs(i) llabs(i)

/*
 * port.h subset: PG's own sort replaces libc qsort tree-wide
 * (#define qsort pg_qsort). Vendored verbatim in qsort.c/qsort_arg.c +
 * lib/sort_template.h so unstable-sort TIE ORDER matches real PG.
 */
typedef int (*qsort_arg_comparator) (const void *a, const void *b, void *arg);
extern void qsort_arg(void *base, size_t nel, size_t elsize,
					  qsort_arg_comparator cmp, void *arg);
extern void pg_qsort(void *base, size_t nel, size_t elsize,
					 int (*cmp) (const void *, const void *));
#define qsort(a, b, c, d) pg_qsort(a, b, c, d)

/* ---- struct varlena / text (c.h, layout-identical) ---- */
struct varlena
{
	char		vl_len_[4];
	char		vl_dat[FLEXIBLE_ARRAY_MEMBER];
};
typedef struct varlena text;
typedef struct varlena bytea;
#define VARHDRSZ ((int32) sizeof(int32))

/* ---- Datum access macros (postgres.h originals, verbatim-equivalent) ---- */
#define PointerGetDatum(X) ((Datum) (X))
#define DatumGetPointer(X) ((void *) (X))
#define DatumGetChar(X) ((char) (X))
#define CharGetDatum(X) ((Datum) (X))
#define BoolGetDatum(X) ((Datum) ((X) ? 1 : 0))
#define DatumGetBool(X) ((bool) ((X) != 0))
#define Int32GetDatum(X) ((Datum) (int32) (X))
#define DatumGetInt32(X) ((int32) (X))
#define UInt16GetDatum(X) ((Datum) (X))
#define ObjectIdGetDatum(X) ((Datum) (X))
#define DatumGetTextPP(X) ((text *) DatumGetPointer(X))
#define Int64GetDatum(X) ((Datum) (int64) (X))
#define DatumGetInt64(X) ((int64) (X))

/* ---- palloc arena (implemented in ../pg_tsvector_core_io.c) ---- */
extern void *pg_tsvec_palloc(size_t n);
extern void *pg_tsvec_palloc0(size_t n);
extern void *pg_tsvec_repalloc(void *p, size_t n);
extern void pg_tsvec_pfree(void *p);
#define palloc(n) pg_tsvec_palloc(n)
#define palloc0(n) pg_tsvec_palloc0(n)
#define repalloc(p, n) pg_tsvec_repalloc((p), (n))
#define pfree(p) pg_tsvec_pfree(p)

/* ---- error machinery (implemented in ../pg_tsvector_core_io.c) ---- */
typedef struct Node
{
	int			type;
} Node;
typedef int NodeTag;
typedef struct ErrorData ErrorData;	/* details never materialized here */
typedef struct ErrorContextCallback
{
	struct ErrorContextCallback *previous;
	void		(*callback) (void *arg);
	void	   *arg;
} ErrorContextCallback;

#define T_ErrorSaveContext 391	/* value irrelevant; only equality is used */
#define IsA(nodeptr, _type_) (((const Node *) (nodeptr))->type == T_##_type_)
#define nodeTag(nodeptr) (((const Node *) (nodeptr))->type)

#define ERROR 21				/* elevel, matches utils/elog.h */

/* errcodes this family raises (recorded raw; Rust maps classes) */
#define ERRCODE_SYNTAX_ERROR 1
#define ERRCODE_PROGRAM_LIMIT_EXCEEDED 2
#define ERRCODE_NULL_VALUE_NOT_ALLOWED 3
#define ERRCODE_ZERO_LENGTH_CHARACTER_STRING 4
#define ERRCODE_INVALID_PARAMETER_VALUE 5
#define ERRCODE_PROTOCOL_VIOLATION 6
#define ERRCODE_CHARACTER_NOT_IN_REPERTOIRE 7
#define ERRCODE_ARRAY_SUBSCRIPT_ERROR 8	/* tsrank getWeights (2202E) */
#define PG_DIFF_ERR_INTERNAL 99	/* elog(ERROR) class */

extern int	errcode(int sqlerrcode);
extern int	errmsg(const char *fmt, ...);
extern int	errdetail(const char *fmt, ...);
extern int	errhint(const char *fmt, ...);
extern void pg_tsvec_errthrow(void);
extern void pg_tsvec_elog_error(const char *fmt, ...);
extern bool pg_tsvec_soft_save(Node *escontext);

/*
 * ereport: argument list evaluated (errcode() records the code), then throw.
 * Both 18.3 invocation styles appear in the vendored files:
 *   ereport(ERROR, (errcode(..), errmsg(..)));   and
 *   ereport(ERROR, errcode(..), errmsg(..));
 * A variadic swallow keeps both compiling; the (void) evaluation keeps
 * errcode()'s side effect.
 */
#define ereport(elevel, ...) \
	do { (void) (__VA_ARGS__); pg_tsvec_errthrow(); } while (0)

#define elog(elevel, ...) \
	do { pg_tsvec_elog_error(__VA_ARGS__); } while (0)

/*
 * errsave/ereturn (elog.h contract): with a live ErrorSaveContext record
 * the error and fall through; otherwise throw like ereport(ERROR).
 */
#define errsave(context, ...) \
	do { \
		(void) (__VA_ARGS__); \
		if (!pg_tsvec_soft_save((Node *) (context))) \
			pg_tsvec_errthrow(); \
	} while (0)

#define ereturn(context, dummy_value, ...) \
	do { \
		errsave(context, __VA_ARGS__); \
		return dummy_value; \
	} while (0)

/* ---- mini-fmgr (fmgr.h subset the vendored Datum functions touch) ---- */
#include "fmgr.h"

/* varlena access macros: upstream code gets varatt.h via htup_details.h;
 * the vendored varatt.h is pulled in here instead (topology plumbing). */
#include "varatt.h"

/* ---- misc ---- */
#define MaxAllocSize ((Size) 0x3fffffff)

#endif							/* PG_DIFFFUZZ_TSVEC_POSTGRES_H */
