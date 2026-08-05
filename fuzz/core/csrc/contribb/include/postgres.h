/*
 * SHIM postgres.h for the contribb_diff oracle TUs (lane p1-mb-contribb):
 * pg_contribb_io.c + the generated segparse/segscan/cubeparse/cubescan TUs.
 * Plumbing only, never logic — every vendored body in those TUs is verbatim
 * PostgreSQL 18.3 (Stamp-18.3, upstream 62d6c7d3df).
 *
 * Error model (soft-input face): the fuzz driver drives both sides the way
 * SQL's soft-error input face does (pg_input_is_valid / COPY ON_ERROR):
 *   - errsave(escontext, ...) records the FIRST errcode into the TLS channel
 *     and returns control (soft), exactly the armed-ErrorSaveContext path.
 *   - ereport(ERROR)/elog(ERROR)/ereturn(NULL ctx) record and longjmp to the
 *     armed driver entry (hard), modelling PG's error longjmp.
 * The channel is unified with pg_float_io.c's pg_diff_errcode (codes 1/2),
 * first error wins across both. Codes (driver maps to SQLSTATEs):
 *   1 22P02  ERRCODE_INVALID_TEXT_REPRESENTATION
 *   2 22003  ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE (float.c only)
 *   3 42601  ERRCODE_SYNTAX_ERROR
 *   4 22023  ERRCODE_INVALID_PARAMETER_VALUE
 *   5 54000  ERRCODE_PROGRAM_LIMIT_EXCEEDED
 *   6 2202E  ERRCODE_ARRAY_ELEMENT_ERROR
 *   7 XX000  internal (elog ERROR / ereport default)
 *   8 08P01  ERRCODE_PROTOCOL_VIOLATION
 *
 * palloc/repalloc/pfree -> per-exec bump arena (pg_cb_reset frees all);
 * prevents the seg_in/cube_in/cube_out per-call leak from OOMing long runs
 * while keeping error-path leaks harmless, like a per-query memory context.
 *
 * NOTE the contribb objects are compiled -funsigned-char: plain char
 * signedness is implementation-defined in C and PG inherits the platform
 * default; the oracle of record is the fleet's Linux/aarch64 build where
 * char is UNSIGNED (the pgrust port also chose u8). Without the flag a
 * macOS (signed-char) local build of seg_cmp's sigd comparisons would
 * diverge from the ratified oracle on sigd >= 128.
 */
#ifndef PG_CB_POSTGRES_H
#define PG_CB_POSTGRES_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <limits.h>

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
typedef uintptr_t Datum;
typedef uint32 Oid;
typedef uint8 bits8;
typedef uint16 OffsetNumber;
typedef uint16 StrategyNumber;

struct Node;					/* opaque; see error model above */

#define Min(x, y) ((x) < (y) ? (x) : (y))
#define Max(x, y) ((x) > (y) ? (x) : (y))
#define Abs(x) ((x) >= 0 ? (x) : -(x))
#define lengthof(array) (sizeof (array) / sizeof ((array)[0]))
#define Assert(condition) ((void) 0)
#define AssertMacro(condition) ((void) true)
#define FLEXIBLE_ARRAY_MEMBER	/* empty */
#define pg_restrict __restrict
#define unlikely(x) (x)
#define likely(x) (x)
#define _(x) (x)
#define pg_attribute_unused()
#define PG_USED_FOR_ASSERTS_ONLY pg_attribute_unused()

/* c.h alignment macros (MAXIMUM_ALIGNOF 8 on both target platforms) */
#define TYPEALIGN(ALIGNVAL,LEN)  \
	(((uintptr_t) (LEN) + ((ALIGNVAL) - 1)) & ~((uintptr_t) ((ALIGNVAL) - 1)))
#define MAXALIGN(LEN) TYPEALIGN(8, (LEN))
#define INTALIGN(LEN) TYPEALIGN(4, (LEN))

/* memutils.h verbatim value */
#define MaxAllocSize	((Size) 0x3fffffff) /* 1 gigabyte - 1 */

/* c.h varlena (fields only; header access via varatt.h shim) */
struct varlena
{
	char		vl_len_[4];
	char		vl_dat[FLEXIBLE_ARRAY_MEMBER];
};
typedef struct varlena bytea;

/* ---- Datum conversions (postgres.h semantics, 64-bit float-by-value) ---- */

#define DatumGetPointer(X) ((void *) (X))
#define PointerGetDatum(X) ((Datum) (X))
#define DatumGetBool(X) ((bool) ((X) != 0))
#define BoolGetDatum(X) ((Datum) ((X) ? 1 : 0))
#define DatumGetInt32(X) ((int32) (X))
#define Int32GetDatum(X) ((Datum) (int32) (X))
#define DatumGetUInt16(X) ((uint16) (X))
#define DatumGetInt16(X) ((int16) (X))

static inline float4
DatumGetFloat4(Datum X)
{
	union
	{
		int32		value;
		float4		retval;
	}			myunion;

	myunion.value = (int32) X;
	return myunion.retval;
}

static inline Datum
Float4GetDatum(float4 X)
{
	union
	{
		float4		value;
		int32		retval;
	}			myunion;

	myunion.value = X;
	return (Datum) (uint32) myunion.retval;
}

static inline float8
DatumGetFloat8(Datum X)
{
	union
	{
		int64		value;
		float8		retval;
	}			myunion;

	myunion.value = (int64) X;
	return myunion.retval;
}

static inline Datum
Float8GetDatum(float8 X)
{
	union
	{
		float8		value;
		int64		retval;
	}			myunion;

	myunion.value = X;
	return (Datum) myunion.retval;
}

/* ---- per-exec arena palloc (see header comment) ---- */

extern void *pg_cb_palloc(Size size);
extern void *pg_cb_palloc0(Size size);
extern void *pg_cb_repalloc(void *ptr, Size size);
extern void pg_cb_pfree(void *ptr);
extern char *pg_cb_pstrdup(const char *s);

#define palloc pg_cb_palloc
#define palloc0 pg_cb_palloc0
#define repalloc pg_cb_repalloc
#define pfree pg_cb_pfree
#define pstrdup pg_cb_pstrdup

/* ---- unified TLS error channel (see header comment) ---- */

#define ERRCODE_INVALID_TEXT_REPRESENTATION 1
#define ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE 2
#define ERRCODE_SYNTAX_ERROR 3
#define ERRCODE_INVALID_PARAMETER_VALUE 4
#define ERRCODE_PROGRAM_LIMIT_EXCEEDED 5
#define ERRCODE_ARRAY_ELEMENT_ERROR 6
#define PG_CB_ERR_INTERNAL 7
#define ERRCODE_PROTOCOL_VIOLATION 8

#define ERROR 21

extern void pg_cb_errstart(void);	/* pending = PG_CB_ERR_INTERNAL */
extern int	pg_cb_errcode_set(int code);	/* pending = code, returns 0 */
extern int	pg_cb_errnoop(const char *fmt,...);	/* errmsg/errdetail sink */
extern void pg_cb_soft_save(void);	/* first-wins record, then return */
extern void pg_cb_raise_hard(void); /* first-wins record + longjmp */
extern int	pg_cb_soft_occurred(void);	/* either channel non-zero */

/* %g PLATFORM PIN — see pg_contribb_io.c's pg_cb_snprintf comment: Apple
 * libc %g keeps trailing zeros on exact rounding ties; glibc (the fleet
 * oracle of record) strips them, and seg's PLUMIN sigd depends on it. */
extern int	pg_cb_snprintf(char *buf, size_t sz, const char *fmt,...);

#define snprintf pg_cb_snprintf

#define errcode(c) pg_cb_errcode_set(c)
#define errmsg pg_cb_errnoop
#define errmsg_internal pg_cb_errnoop
#define errdetail pg_cb_errnoop
#define errhint pg_cb_errnoop

#define errsave(escontext, rest) \
	do { pg_cb_errstart(); ((void) (rest)); pg_cb_soft_save(); } while (0)
#define ereport(level, rest) \
	do { pg_cb_errstart(); ((void) (rest)); pg_cb_raise_hard(); } while (0)
#define elog(level, ...) \
	do { pg_cb_errstart(); (void) pg_cb_errnoop(__VA_ARGS__); pg_cb_raise_hard(); } while (0)
/* ereturn only reachable here with a NULL escontext (ArrayGetNItemsSafe) */
#define ereturn(escontext, dummy_value, rest) \
	do { pg_cb_errstart(); ((void) (rest)); pg_cb_raise_hard(); return dummy_value; } while (0)

#endif							/* PG_CB_POSTGRES_H */
