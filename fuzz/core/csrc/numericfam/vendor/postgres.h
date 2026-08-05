/* Shim environment for the UNMODIFIED vendored REL_18_3 numeric.c
 * (established wchar/dynahash vendor pattern). Types/macros are value-exact
 * copies of the PG headers they stand in for; palloc is the adtint-style
 * noinline bump arena (a LOWER bound on real AllocSetAlloc cost); everything
 * a bench lane never reaches links to an aborting stub in numeric_stubs.c. */
#ifndef NUMERIC_VENDOR_POSTGRES_H
#define NUMERIC_VENDOR_POSTGRES_H

/* Isolate this vendor TU: every shim-provided extern is nv_-prefixed so it
 * cannot collide with the other cref TUs' shims/lifts at link time. */
#define palloc nv_palloc
#define palloc0 nv_palloc0
#define pfree nv_pfree
#define pstrdup nv_pstrdup
#define MemoryContextSwitchTo nv_MemoryContextSwitchTo
#define CurrentMemoryContext nv_CurrentMemoryContext
#define pg_strncasecmp nv_pg_strncasecmp
#define pg_strcasecmp nv_pg_strcasecmp
#define AggCheckCallContext nv_AggCheckCallContext
#define hash_uint32 nv_hash_uint32
#define hash_uint32_extended nv_hash_uint32_extended
#define hash_any nv_hash_any
#define hash_any_extended nv_hash_any_extended
#define DirectFunctionCall1Coll nv_DirectFunctionCall1Coll
#define DirectFunctionCall2Coll nv_DirectFunctionCall2Coll
#define float4in nv_float4in
#define float8in nv_float8in
#define trace_sort nv_trace_sort
#define pq_begintypsend nv_pq_begintypsend
#define pq_endtypsend nv_pq_endtypsend
#define pq_sendint16 nv_pq_sendint16
#define pq_sendint32 nv_pq_sendint32
#define pq_sendint64 nv_pq_sendint64
#define pq_getmsgint nv_pq_getmsgint
#define pq_getmsgint64 nv_pq_getmsgint64
#define pq_getmsgend nv_pq_getmsgend
#define initReadOnlyStringInfo nv_initReadOnlyStringInfo
#define ArrayGetIntegerTypmods nv_ArrayGetIntegerTypmods
#define exprTypmod nv_exprTypmod
#define relabel_to_typmod nv_relabel_to_typmod
#define is_funcclause nv_is_funcclause
#define estimate_expression_value nv_estimate_expression_value
#define initHyperLogLog nv_initHyperLogLog
#define addHyperLogLog nv_addHyperLogLog
#define estimateHyperLogLog nv_estimateHyperLogLog
#define pg_prng_uint64_range nv_pg_prng_uint64_range

#include <ctype.h>
#include <math.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef int8_t int8;
typedef int16_t int16;
typedef int32_t int32;
typedef int64_t int64;
typedef uint8_t uint8;
typedef uint16_t uint16;
typedef uint32_t uint32;
typedef uint64_t uint64;
typedef __int128 int128;
typedef unsigned __int128 uint128;
#define HAVE_INT128 1
typedef size_t Size;
typedef uintptr_t Datum;
typedef unsigned int Oid;
typedef unsigned int TransactionId;
typedef uint64 XLogRecPtr;
#define InvalidOid ((Oid) 0)
#define SIZEOF_DATUM 8
#define BITS_PER_BYTE 8

#define PG_INT16_MIN INT16_MIN
#define PG_INT16_MAX INT16_MAX
#define PG_INT32_MIN INT32_MIN
#define PG_INT32_MAX INT32_MAX
#define PG_INT64_MIN INT64_MIN
#define PG_INT64_MAX INT64_MAX
#define PG_UINT64_MAX UINT64_MAX
#define UINT64CONST(x) UINT64_C(x)
#define INT64CONST(x) INT64_C(x)

#define Assert(x) ((void) 0)
#define AssertPointerAlignment(p, a) ((void) 0)
#define StaticAssertDecl(cond, msg) _Static_assert(cond, msg)
#define StaticAssertStmt(cond, msg) _Static_assert(cond, msg)
#define lengthof(a) (sizeof(a) / sizeof((a)[0]))
#define Min(a, b) ((a) < (b) ? (a) : (b))
#define Max(a, b) ((a) > (b) ? (a) : (b))
#define Abs(x) ((x) >= 0 ? (x) : -(x))
#define unlikely(x) __builtin_expect((x) != 0, 0)
#define likely(x) __builtin_expect((x) != 0, 1)
#define FLEXIBLE_ARRAY_MEMBER /* empty */
#define pg_attribute_unused() __attribute__((unused))
#define pg_noinline __attribute__((noinline))
#define PG_USED_FOR_ASSERTS_ONLY pg_attribute_unused()

static inline int64
i64abs(int64 i)
{
	return (i < 0) ? -i : i;
}

static inline uint32
pg_abs_s32(int32 a)
{
	if (unlikely(a == PG_INT32_MIN))
		return (uint32) PG_INT32_MAX + 1;
	return (uint32) (a < 0 ? -a : a);
}

static inline uint64
pg_abs_s64(int64 a)
{
	if (unlikely(a == PG_INT64_MIN))
		return (uint64) PG_INT64_MAX + 1;
	return (uint64) i64abs(a);
}

/* pointer/datum conversions (postgres.h originals) */
#define DatumGetPointer(X) ((char *) (X))
#define PointerGetDatum(X) ((Datum) (X))
#define DatumGetInt16(X) ((int16) (X))
#define DatumGetInt32(X) ((int32) (X))
#define DatumGetInt64(X) ((int64) (X))
#define DatumGetUInt32(X) ((uint32) (X))
#define DatumGetUInt64(X) ((uint64) (X))
#define Int64GetDatumFast(X) Int64GetDatum(X)
typedef char *Pointer;
#define VARATT_SHORT_MAX 0x7F
#define Int16GetDatum(X) ((Datum) (X))
#define Int32GetDatum(X) ((Datum) (X))
#define Int64GetDatum(X) ((Datum) (X))
#define UInt32GetDatum(X) ((Datum) (X))
#define UInt64GetDatum(X) ((Datum) (X))
#define ObjectIdGetDatum(X) ((Datum) (X))
#define DatumGetObjectId(X) ((Oid) (X))
#define BoolGetDatum(X) ((Datum) ((X) ? 1 : 0))
#define DatumGetBool(X) ((bool) ((X) != 0))
#define DatumGetCString(X) ((char *) DatumGetPointer(X))
#define CStringGetDatum(X) PointerGetDatum(X)

static inline double
DatumGetFloat8(Datum X)
{
	double		r;

	memcpy(&r, &X, sizeof(r));
	return r;
}

static inline Datum
Float8GetDatum(double X)
{
	Datum		r = 0;

	memcpy(&r, &X, sizeof(X));
	return r;
}

static inline float
DatumGetFloat4(Datum X)
{
	uint32		bits = (uint32) X;
	float		r;

	memcpy(&r, &bits, sizeof(r));
	return r;
}

static inline Datum
Float4GetDatum(float X)
{
	uint32		bits;

	memcpy(&bits, &X, sizeof(X));
	return (Datum) bits;
}

/* varlena (4-byte little-endian headers, postgres.h originals) */
struct varlena
{
	char		vl_len_[4];
	char		vl_dat[FLEXIBLE_ARRAY_MEMBER];
};
typedef struct varlena bytea;
typedef struct varlena text;

#define VARHDRSZ ((int32) sizeof(int32))
#define VARHDRSZ_SHORT 1
#define VARDATA(PTR) (((struct varlena *) (PTR))->vl_dat)
#define VARSIZE(PTR) ((*((uint32 *) (PTR))) >> 2)
#define SET_VARSIZE(PTR, len) (*((uint32 *) (PTR)) = ((uint32) (len)) << 2)
#define VARATT_IS_SHORT(PTR) ((*((uint8 *) (PTR)) & 0x01) == 0x01)
#define VARSIZE_SHORT(PTR) ((*((uint8 *) (PTR)) >> 1) & 0x7F)
#define VARDATA_SHORT(PTR) (((char *) (PTR)) + 1)
#define VARSIZE_ANY_EXHDR(PTR) \
	(VARATT_IS_SHORT(PTR) ? VARSIZE_SHORT(PTR) - VARHDRSZ_SHORT : \
	 VARSIZE(PTR) - VARHDRSZ)
#define VARDATA_ANY(PTR) \
	(VARATT_IS_SHORT(PTR) ? VARDATA_SHORT(PTR) : VARDATA(PTR))

/* memory */
extern void *palloc(Size sz);
extern void *palloc0(Size sz);
extern void pfree(void *p);
extern char *pstrdup(const char *s);

typedef void *MemoryContext;
extern MemoryContext CurrentMemoryContext;
extern MemoryContext MemoryContextSwitchTo(MemoryContext ctx);

/* ============= FUZZ-ORACLE ERROR CAPTURE (differs from bench) =============
 * The bench vendor aborts on any error path; the differential-fuzz oracle
 * must instead CAPTURE the sqlstate and unwind (fuzz inputs hit error arms
 * constantly, and errcode/sqlstate parity IS a comparison plane). Plumbing
 * only, never logic: errcode() records the real MAKE_SQLSTATE value in a
 * thread-local at the exact ereport program point; ereport(>=ERROR)
 * longjmps back to the pg_diff_num_* entry wrapper; ereturn() escalates to
 * ERROR exactly as errsave() does with a NULL escontext (the fuzz drivers
 * never pass a soft-error context, matching the shipped fc_* call shape).
 * elog(<ERROR) (trace_sort chatter) is a no-op; elog(ERROR) records
 * sqlstate ERRCODE_INTERNAL_ERROR (XX000) like the real elog. */
#define ERROR 21
#define LOG 15
#define WARNING 19

/* src/include/utils/elog.h MAKE_SQLSTATE, value-exact */
#define PGSIXBIT(ch)	(((ch) - '0') & 0x3F)
#define MAKE_SQLSTATE(ch1,ch2,ch3,ch4,ch5) \
	(PGSIXBIT(ch1) + (PGSIXBIT(ch2) << 6) + (PGSIXBIT(ch3) << 12) + \
	 (PGSIXBIT(ch4) << 18) + (PGSIXBIT(ch5) << 24))

/* utils/errcodes.h rows numeric.c reaches, value-exact */
#define ERRCODE_DIVISION_BY_ZERO				MAKE_SQLSTATE('2','2','0','1','2')
#define ERRCODE_FEATURE_NOT_SUPPORTED			MAKE_SQLSTATE('0','A','0','0','0')
#define ERRCODE_INVALID_ARGUMENT_FOR_LOG		MAKE_SQLSTATE('2','2','0','1','E')
#define ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION	MAKE_SQLSTATE('2','2','0','1','F')
#define ERRCODE_INVALID_ARGUMENT_FOR_WIDTH_BUCKET_FUNCTION	MAKE_SQLSTATE('2','2','0','1','G')
#define ERRCODE_INVALID_BINARY_REPRESENTATION	MAKE_SQLSTATE('2','2','P','0','3')
#define ERRCODE_INVALID_PARAMETER_VALUE			MAKE_SQLSTATE('2','2','0','2','3')
#define ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE	MAKE_SQLSTATE('2','2','0','1','3')
#define ERRCODE_INVALID_TEXT_REPRESENTATION		MAKE_SQLSTATE('2','2','P','0','2')
#define ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE		MAKE_SQLSTATE('2','2','0','0','3')
#define ERRCODE_PROTOCOL_VIOLATION				MAKE_SQLSTATE('0','8','P','0','1')
#define ERRCODE_INTERNAL_ERROR					MAKE_SQLSTATE('X','X','0','0','0')

extern _Thread_local int nfz_sqlstate;	/* defined in pg_numeric_oracle.c */
extern void nfz_raise(void) __attribute__((noreturn));

static inline int
errcode(int sqlerrcode)
{
	nfz_sqlstate = sqlerrcode;
	return 0;
}

/* message machinery: text never crosses the diff seam */
#define errmsg(...) 0
#define errmsg_internal(...) 0
#define errdetail(...) 0
#define errdetail_internal(...) 0
#define errhint(...) 0
#define errcontext(...) 0

#define ereport(level, ...) \
	do { __VA_ARGS__; if ((level) >= ERROR) nfz_raise(); } while (0)
#define elog(level, ...) \
	do { if ((level) >= ERROR) { nfz_sqlstate = ERRCODE_INTERNAL_ERROR; nfz_raise(); } } while (0)
#define ereturn(escontext, dummy, ...) ereport(ERROR, __VA_ARGS__)

#define CHECK_FOR_INTERRUPTS() ((void) 0)

extern int pg_strncasecmp(const char *s1, const char *s2, size_t n);
extern int pg_strcasecmp(const char *s1, const char *s2);

/* common/int.h (value-exact) */
static inline bool
pg_add_s64_overflow(int64 a, int64 b, int64 *result)
{
	return __builtin_add_overflow(a, b, result);
}

static inline bool
pg_sub_s64_overflow(int64 a, int64 b, int64 *result)
{
	return __builtin_sub_overflow(a, b, result);
}

static inline bool
pg_mul_s64_overflow(int64 a, int64 b, int64 *result)
{
	return __builtin_mul_overflow(a, b, result);
}

static inline bool
pg_add_u64_overflow(uint64 a, uint64 b, uint64 *result)
{
	return __builtin_add_overflow(a, b, result);
}

static inline bool
pg_mul_u64_overflow(uint64 a, uint64 b, uint64 *result)
{
	return __builtin_mul_overflow(a, b, result);
}

/* fmgr.h (fields numeric.c touches) */
typedef struct NullableDatum
{
	Datum		value;
	bool		isnull;
} NullableDatum;

typedef struct Node Node;

typedef struct FmgrInfo
{
	void	   *fn_addr;
	Oid			fn_oid;
	short		fn_nargs;
	bool		fn_strict;
	bool		fn_retset;
	unsigned char fn_stats;
	void	   *fn_extra;
	MemoryContext fn_mcxt;
	Node	   *fn_expr;
} FmgrInfo;

typedef struct FunctionCallInfoBaseData
{
	FmgrInfo   *flinfo;
	Node	   *context;
	Node	   *resultinfo;
	Oid			fncollation;
	bool		isnull;
	short		nargs;
	NullableDatum args[8];
} FunctionCallInfoBaseData;

typedef FunctionCallInfoBaseData *FunctionCallInfo;

#define LOCAL_FCINFO(name, nargs) \
	FunctionCallInfoBaseData name##data; \
	FunctionCallInfo name = &name##data

#define PG_FUNCTION_ARGS FunctionCallInfo fcinfo
#define PG_NARGS() (fcinfo->nargs)

#define PG_ARGISNULL(n) (fcinfo->args[n].isnull)
#define PG_GETARG_DATUM(n) (fcinfo->args[n].value)
#define PG_GETARG_INT16(n) DatumGetInt16(PG_GETARG_DATUM(n))
#define PG_GETARG_INT32(n) DatumGetInt32(PG_GETARG_DATUM(n))
#define PG_GETARG_INT64(n) DatumGetInt64(PG_GETARG_DATUM(n))
#define PG_GETARG_OID(n) DatumGetObjectId(PG_GETARG_DATUM(n))
#define PG_GETARG_BOOL(n) DatumGetBool(PG_GETARG_DATUM(n))
#define PG_GETARG_CSTRING(n) DatumGetCString(PG_GETARG_DATUM(n))
#define PG_GETARG_POINTER(n) DatumGetPointer(PG_GETARG_DATUM(n))
#define PG_GETARG_FLOAT4(n) DatumGetFloat4(PG_GETARG_DATUM(n))
#define PG_GETARG_FLOAT8(n) DatumGetFloat8(PG_GETARG_DATUM(n))
#define PG_GETARG_BYTEA_PP(n) ((bytea *) DatumGetPointer(PG_GETARG_DATUM(n)))
#define PG_FREE_IF_COPY(ptr, n) ((void) 0)
#define PG_DETOAST_DATUM(d) ((struct varlena *) DatumGetPointer(d))
#define PG_DETOAST_DATUM_PACKED(d) ((struct varlena *) DatumGetPointer(d))

#define PG_RETURN_DATUM(x) return (x)
#define PG_RETURN_NULL() do { fcinfo->isnull = true; return (Datum) 0; } while (0)
#define PG_RETURN_VOID() return (Datum) 0
#define PG_RETURN_INT16(x) return Int16GetDatum(x)
#define PG_RETURN_INT32(x) return Int32GetDatum(x)
#define PG_RETURN_INT64(x) return Int64GetDatum(x)
#define PG_RETURN_UINT32(x) return UInt32GetDatum(x)
#define PG_RETURN_UINT64(x) return UInt64GetDatum(x)
#define PG_RETURN_BOOL(x) return BoolGetDatum(x)
#define PG_RETURN_CSTRING(x) return CStringGetDatum(x)
#define PG_RETURN_POINTER(x) return PointerGetDatum(x)
#define PG_RETURN_FLOAT4(x) return Float4GetDatum(x)
#define PG_RETURN_FLOAT8(x) return Float8GetDatum(x)
#define PG_RETURN_BYTEA_P(x) return PointerGetDatum(x)

typedef Datum (*PGFunction) (FunctionCallInfo fcinfo);

extern int AggCheckCallContext(FunctionCallInfo fcinfo, MemoryContext *aggcontext);
extern uint32 hash_uint32(uint32 k);
extern Datum hash_uint32_extended(uint32 k, uint64 seed);

extern Datum DirectFunctionCall1Coll(PGFunction func, Oid collation, Datum arg1);
extern Datum DirectFunctionCall2Coll(PGFunction func, Oid collation, Datum arg1, Datum arg2);
#define DirectFunctionCall1(func, a1) DirectFunctionCall1Coll(func, InvalidOid, a1)
#define DirectFunctionCall2(func, a1, a2) DirectFunctionCall2Coll(func, InvalidOid, a1, a2)

extern Datum float4in(PG_FUNCTION_ARGS);
extern Datum float8in(PG_FUNCTION_ARGS);
extern Datum numeric_out(PG_FUNCTION_ARGS);
extern Datum numeric_div(PG_FUNCTION_ARGS);

/* nodes (numeric_support / escontext surface) */
typedef enum NodeTag
{
	T_Invalid = 0,
	T_Const,
	T_SupportRequestSimplify,
	T_SupportRequestRows,
	T_FuncExpr,
	T_OpExpr,
	T_ErrorSaveContext
} NodeTag;

struct Node
{
	NodeTag		type;
};

#define IsA(nodeptr, _type_) (((const Node *) (nodeptr))->type == T_##_type_)
#define nodeTag(nodeptr) (((const Node *) (nodeptr))->type)

typedef struct List
{
	NodeTag		type;
	int			length;
	void	  **elements;
} List;

#define NIL ((List *) NULL)

static inline int
list_length(const List *l)
{
	return l ? l->length : 0;
}

#define linitial(l) ((l)->elements[0])
#define lsecond(l) ((l)->elements[1])

typedef struct Const
{
	NodeTag		type;
	Oid			consttype;
	int32		consttypmod;
	bool		constisnull;
	Datum		constvalue;
} Const;

typedef struct FuncExpr
{
	NodeTag		type;
	Oid			funcid;
	List	   *args;
} FuncExpr;

#endif /* NUMERIC_VENDOR_POSTGRES_H */
