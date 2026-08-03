/*
 * SHIM postgres.h — NOT PostgreSQL code.  (tsq oracle family, p1-laneaf)
 *
 * Minimal backend environment so the VERBATIM vendored files under ../
 * (src/backend/utils/adt/{tsquery,tsquery_op,tsquery_cleanup,
 * tsvector_parser}.c, the ts_locale.c t_is* excerpt, and — in the same
 * build unit — ../../tsqrw/{tsquery_util,tsquery_rewrite}.c, all from
 * postgres-src @ 62d6c7d3df6287f1bd83199c1a746e50d31571a0, PostgreSQL 18.3)
 * compile standalone for the native differential-fuzz build.
 *
 * Every definition here is PLUMBING, never logic. Inventory of shims, with
 * upstream provenance:
 *   - fixed-width typedefs, Size/Oid/Datum machinery: c.h / postgres.h on
 *     LP64 (Datum = uintptr_t; the Datum<->pointer/int casts are identical).
 *   - struct varlena / text / bytea: c.h. Only flat (4-byte-header,
 *     uncompressed, untoasted) values ever cross this oracle's boundary.
 *   - Assert compiled out (matches a production NDEBUG PostgreSQL build).
 *   - ereport/elog/errsave/ereturn -> pg_diff_errcode class recording plus
 *     setjmp/longjmp non-local exit for >= ERROR, notice counter for
 *     NOTICE (utils/elog.h semantics; message strings are out of the
 *     comparison planes so errmsg/errdetail/... swallow their arguments
 *     unevaluated except for side-effect-free format args).
 *     ereturn/errsave follow the verbatim upstream shape (elog.h:
 *     ereturn(context, dummy_value, ...) = errsave + return): a soft-error
 *     context (nodes/miscnodes.h ErrorSaveContext) records error_occurred
 *     and control RETURNS; a NULL/non-ErrorSaveContext context throws.
 *   - palloc/palloc0/repalloc/pfree/pstrdup -> thread-local bump arena with
 *     per-allocation size headers (memory-context-reset stand-in; the
 *     pg_lsn_oracle.c S3 precedent). Reset at every pg_diff_* entry.
 *   - check_stack_depth / CHECK_FOR_INTERRUPTS -> no-ops (miscadmin.h);
 *     the fuzz DRIVER caps input length far below any real stack risk, and
 *     the Rust side recurses natively under the same cap. Documented seam:
 *     C's stack-depth ereport (54001) is unreachable under the driver cap
 *     on BOTH sides.
 *   - _() gettext identity; unlikely/likely passthrough.
 */
#ifndef PG_DIFFFUZZ_TSQ_SHIM_POSTGRES_H
#define PG_DIFFFUZZ_TSQ_SHIM_POSTGRES_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include <string.h>
#include <stdlib.h>
#include <setjmp.h>
#include <ctype.h>
#include <limits.h>
#include <errno.h>				/* c.h includes it; tsquery.c uses strtol/errno */
#include <stdio.h>				/* c.h includes it; tsquery.c uses sprintf (glibc
								 * does not leak stdio via other headers the way
								 * macOS libc does — first-Linux-compile fix) */

typedef int8_t int8;
typedef int16_t int16;
typedef int32_t int32;
typedef int64_t int64;
typedef uint8_t uint8;
typedef uint16_t uint16;
typedef uint32_t uint32;
typedef uint64_t uint64;
typedef size_t Size;
typedef uint32 Oid;

#define UINT64CONST(x) UINT64_C(x)
#define INT64CONST(x) INT64_C(x)

#define BITS_PER_BYTE 8
#define FLEXIBLE_ARRAY_MEMBER	/* empty */
#define lengthof(array) (sizeof (array) / sizeof ((array)[0]))
#define Min(x, y) ((x) < (y) ? (x) : (y))
#define Max(x, y) ((x) > (y) ? (x) : (y))

#ifndef Assert
#define Assert(x) ((void) 0)
#endif
#define AssertMacro(x) ((void) 0)
#define StaticAssertStmt(cond, msg) ((void) 0)
#define StaticAssertDecl(cond, msg) extern int pg_tsq_static_assert_dummy

#ifndef unlikely
#define unlikely(x) (x)
#endif
#ifndef likely
#define likely(x) (x)
#endif

#ifndef PGDLLIMPORT
#define PGDLLIMPORT
#endif
#define pg_attribute_unused()
#define pg_nodiscard
#define pg_noreturn _Noreturn void
#define pg_attribute_noreturn() /* trailing form, older style */
#define pg_attribute_printf(f, a)

#ifndef _
#define _(x) (x)
#endif
#define gettext_noop(x) (x)

/* ---- Datum (postgres.h on LP64) ---- */
typedef uintptr_t Datum;

#define DatumGetPointer(X) ((void *) (X))
#define PointerGetDatum(X) ((Datum) (X))
#define DatumGetInt32(X) ((int32) (X))
#define Int32GetDatum(X) ((Datum) (int32) (X))
#define DatumGetInt64(X) ((int64) (X))
#define Int64GetDatum(X) ((Datum) (X))
#define DatumGetUInt32(X) ((uint32) (X))
#define UInt32GetDatum(X) ((Datum) (X))
#define DatumGetInt16(X) ((int16) (X))
#define Int16GetDatum(X) ((Datum) (int16) (X))
#define DatumGetBool(X) ((bool) ((X) != 0))
#define BoolGetDatum(X) ((Datum) ((X) ? 1 : 0))
#define DatumGetChar(X) ((char) (X))
#define CharGetDatum(X) ((Datum) (X))
#define DatumGetCString(X) ((char *) DatumGetPointer(X))
#define CStringGetDatum(X) PointerGetDatum(X)

/* ---- varlena (c.h) ---- */
struct varlena
{
	char		vl_len_[4];		/* opaque; use varatt.h macros */
	char		vl_dat[FLEXIBLE_ARRAY_MEMBER];
};
typedef struct varlena text;
typedef struct varlena bytea;

/* ---- error levels (utils/elog.h values) ---- */
#define DEBUG1 14
#define LOG 15
#define INFO 17
#define NOTICE 18
#define WARNING 19
#define ERROR 21

/*
 * Shared TLS errcode channel (defined in csrc/pg_float_io.c) + tsq-family
 * error state (defined in tsq/shim/pg_tsq_shim.c).
 */
extern _Thread_local int pg_diff_errcode;
extern _Thread_local jmp_buf pg_tsq_error_jmp;
extern _Thread_local int pg_tsq_notice_count;

/*
 * Errcode CLASS constants for this family (the Rust driver maps each class
 * to the expected SQLSTATE; convention of csrc/pg_strfam.c / pg_float_io.c).
 */
#define PG_DIFF_ERR_SYNTAX 1				/* 42601 syntax_error */
#define PG_DIFF_ERR_INVALID_PARAMETER_VALUE 2	/* 22023 */
#define PG_DIFF_ERR_PROGRAM_LIMIT_EXCEEDED 3	/* 54000 */
#define PG_DIFF_ERR_INVALID_ENCODING 4		/* 22021 character_not_in_repertoire */
#define PG_DIFF_ERR_PROTOCOL_VIOLATION 5	/* 08P01 (pqformat message decode) */
#define PG_DIFF_ERR_INTERNAL 100			/* elog(ERROR, ...) XX000 */

/* upstream errcodes.h names -> class constants */
#define ERRCODE_SYNTAX_ERROR PG_DIFF_ERR_SYNTAX
#define ERRCODE_INVALID_PARAMETER_VALUE PG_DIFF_ERR_INVALID_PARAMETER_VALUE
#define ERRCODE_PROGRAM_LIMIT_EXCEEDED PG_DIFF_ERR_PROGRAM_LIMIT_EXCEEDED
#define ERRCODE_CHARACTER_NOT_IN_REPERTOIRE PG_DIFF_ERR_INVALID_ENCODING
#define ERRCODE_PROTOCOL_VIOLATION PG_DIFF_ERR_PROTOCOL_VIOLATION

#define errcode(c) (pg_diff_errcode = (c))
#define errmsg(...) (0)
#define errmsg_internal(...) (0)
#define errdetail(...) (0)
#define errdetail_internal(...) (0)
#define errhint(...) (0)
#define errposition(n) (0)

struct Node;					/* fwd; nodes/nodes.h */

/* implemented in pg_tsq_shim.c */
extern void pg_tsq_ereport_finish(int elevel);
extern void pg_tsq_errsave_finish(struct Node *escontext);

/*
 * ereport: evaluate the auxiliary-info arguments (recording the errcode
 * class via the errcode() macro above), then longjmp for >= ERROR /
 * count for NOTICE. Both the parenthesized and the bare argument-list
 * upstream spellings parse through __VA_ARGS__.
 */
#define ereport(elevel, ...) \
	do { (void) (__VA_ARGS__); pg_tsq_ereport_finish(elevel); } while (0)

/* elog carries no errcode -> internal-error class (utils/elog.h) */
#define elog(elevel, ...) \
	do { pg_diff_errcode = PG_DIFF_ERR_INTERNAL; pg_tsq_ereport_finish(elevel); } while (0)

/* elog.h errsave/ereturn, faithful control flow (see file header) */
#define errsave(context, ...) \
	do { (void) (__VA_ARGS__); pg_tsq_errsave_finish((struct Node *) (context)); } while (0)

#define ereturn(context, dummy_value, ...) \
	do { errsave(context, __VA_ARGS__); return dummy_value; } while (0)

/* ---- palloc family over the per-entry bump arena (pg_tsq_shim.c) ---- */
extern void *pg_tsq_arena_alloc(Size n, bool zero);
extern void *pg_tsq_arena_repalloc(void *p, Size n);
extern void pg_tsq_arena_reset(void);

#define palloc(n) pg_tsq_arena_alloc((n), false)
#define palloc0(n) pg_tsq_arena_alloc((n), true)
#define repalloc(p, n) pg_tsq_arena_repalloc((p), (n))
#define pfree(p) ((void) (p))
extern char *pstrdup(const char *s);

/* src/port/pgstrcasecmp.c pg_strncasecmp, VERBATIM in pg_tsq_shim.c */
#define IS_HIGHBIT_SET(ch) ((unsigned char) (ch) & 0x80)
extern int	pg_strncasecmp(const char *s1, const char *s2, size_t n);

/*
 * port.h:471-478 (upstream @ 62d6c7d3df): every backend TU's qsort() call
 * is pg_qsort, PostgreSQL's own med3 quicksort — NOT libc qsort. Tie order
 * differs between the two and is scalar-visible through QTNSort
 * (tsquery_util.c:176) and tsq_mcontains' value sort (tsquery_op.c:322,325),
 * so the oracle must run the real thing: verbatim sort_template.h
 * instantiation in csrc/tsq/qsort.c (symbol spelled tsq_pg_qsort to avoid
 * cross-lane duplicate pg_qsort definitions in the shared link; see that
 * file's header).
 */
extern void tsq_pg_qsort(void *base, size_t nel, size_t elsize,
						 int (*cmp) (const void *, const void *));
#define qsort(a,b,c,d) tsq_pg_qsort(a,b,c,d)

#endif							/* PG_DIFFFUZZ_TSQ_SHIM_POSTGRES_H */
