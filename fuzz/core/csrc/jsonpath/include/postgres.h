/*
 * SHIM postgres.h for the jsonpath_diff oracle family — NOT PostgreSQL code.
 *
 * Minimal environment so the VERBATIM vendored jsonpath/regex/numeric/
 * formatting/stringinfo/pqformat/mbutils TUs in this directory compile
 * standalone (postgres-src @ 62d6c7d3df6287f1bd83199c1a746e50d31571a0,
 * PostgreSQL 18.3). Plumbing only, never logic:
 *
 *   - fixed-width typedefs matching c.h on LP64; Datum machinery verbatim
 *     in shape (uintptr_t) with the postgres.h conversion macros;
 *   - Assert compiled out (matches a production/NDEBUG PostgreSQL build,
 *     which is the ground-truth docker postgres:18.3 behavior);
 *   - palloc family -> the TLS pointer arena in pg_jsonpath_env.c (models
 *     PG's per-query memory-context reset; error longjmps cannot leak);
 *   - ereport/elog/errsave/ereturn -> TLS errcode + message capture with
 *     per-level dispatch: ERROR longjmps (siglongjmp-per-iteration model),
 *     errsave against a live ErrorSaveContext records a soft error and
 *     falls through exactly like the real errsave_start/errsave_finish
 *     protocol (nodes/miscnodes.h is vendored VERBATIM);
 *   - ERRCODE_* values are the real MAKE_SQLSTATE encodings (utils/elog.h +
 *     utils/errcodes.h verbatim values) so the sqlstate comparison plane
 *     sees genuine PostgreSQL sqlstates;
 *   - CHECK_FOR_INTERRUPTS/check_stack_depth are no-ops: the fuzz driver
 *     caps input length (see jsonpath_diff.rs), which bounds parser/printer
 *     recursion far below either side's real guard, so the 54001 plane is
 *     structurally out of domain (documented carve in the driver header).
 */
#ifndef PG_JSONPATH_DIFF_SHIM_POSTGRES_H
#define PG_JSONPATH_DIFF_SHIM_POSTGRES_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <setjmp.h>
#include <errno.h>

/* ---------------- c.h type layer (LP64) ---------------- */

/*
 * port.h parity (task #142): pg_formatting_min.c's verbatim DCH/NUM cache
 * fills call strlcpy. macOS <string.h> declares it; glibc only from 2.38,
 * and the fleet pods are older — without a declaration newer gcc rejects
 * the TU (implicit function declaration is an error since gcc 14). Same
 * guarded declaration real port.h carries (!HAVE_DECL_STRLCPY arm). The
 * link-time definition is libc's where it exists, else this family's WEAK
 * compat copy (csrc/pg_strlcpy_compat.c, compiled into this archive).
 */
#ifndef __APPLE__
extern size_t strlcpy(char *dst, const char *src, size_t siz);
#endif

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
typedef char *Pointer;

#define HAVE_INT128 1
typedef __int128 int128;
typedef unsigned __int128 uint128;

#define InvalidOid ((Oid) 0)
#define OidIsValid(objectId)  ((bool) ((objectId) != InvalidOid))

#define UINT64CONST(x) UINT64_C(x)
#define INT64CONST(x) INT64_C(x)

#define PG_INT8_MIN		(-0x7F-1)
#define PG_INT8_MAX		(0x7F)
#define PG_UINT8_MAX	(0xFF)
#define PG_INT16_MIN	(-0x7FFF-1)
#define PG_INT16_MAX	(0x7FFF)
#define PG_UINT16_MAX	(0xFFFF)
#define PG_INT32_MIN	(-0x7FFFFFFF-1)
#define PG_INT32_MAX	(0x7FFFFFFF)
#define PG_UINT32_MAX	(0xFFFFFFFFU)
#define PG_INT64_MIN	(-INT64CONST(0x7FFFFFFFFFFFFFFF) - 1)
#define PG_INT64_MAX	INT64CONST(0x7FFFFFFFFFFFFFFF)
#define PG_UINT64_MAX	UINT64CONST(0xFFFFFFFFFFFFFFFF)

#ifndef likely
#define likely(x)	__builtin_expect((x) != 0, 1)
#define unlikely(x) __builtin_expect((x) != 0, 0)
#endif

#define lengthof(array) (sizeof (array) / sizeof ((array)[0]))

#define pg_attribute_unused() __attribute__((unused))
#define pg_attribute_noreturn() __attribute__((noreturn))
#define pg_attribute_always_inline __attribute__((always_inline)) inline
#define pg_attribute_printf(f,a) __attribute__((format(printf, f, a)))
#define pg_nodiscard __attribute__((warn_unused_result))
#define pg_noinline __attribute__((noinline))
#define pg_restrict __restrict
#define FLEXIBLE_ARRAY_MEMBER	/* empty */
#define PGDLLIMPORT
#define INT64_FORMAT "%" PRId64
#include <inttypes.h>

/* Assert: compiled out, matching a production (non-cassert) PG build */
#ifndef Assert
#define Assert(condition)	((void) 0)
#endif
#define AssertMacro(condition)	((void) true)
#define StaticAssertStmt(condition, errmessage) ((void) 0)
#define StaticAssertDecl(condition, errmessage) \
	_Static_assert(condition, errmessage)

/* alignment macros — verbatim shapes from c.h */
#define TYPEALIGN(ALIGNVAL,LEN)  \
	(((uintptr_t) (LEN) + ((ALIGNVAL) - 1)) & ~((uintptr_t) ((ALIGNVAL) - 1)))
#define SHORTALIGN(LEN)			TYPEALIGN(2, (LEN))
#define INTALIGN(LEN)			TYPEALIGN(4, (LEN))
#define LONGALIGN(LEN)			TYPEALIGN(8, (LEN))
#define MAXIMUM_ALIGNOF 8
#define MAXALIGN(LEN)			TYPEALIGN(MAXIMUM_ALIGNOF, (LEN))

#define Min(x, y)		((x) < (y) ? (x) : (y))
#define Max(x, y)		((x) > (y) ? (x) : (y))
#define Abs(x)			((x) >= 0 ? (x) : -(x))

#define MemSet(start, val, len) memset(start, val, len)

/* c.h verbatim shapes */
#define IS_HIGHBIT_SET(ch)		((unsigned char) (ch) & 0x80)
#define HIGHBIT					(0x80)

/* gettext: not wired in the oracle */
#define _(x) (x)

#define HAVE__BUILTIN_OP_OVERFLOW 1

/* c.h verbatim shapes */
#define i32abs(i) abs(i)
#define i64abs(i) llabs(i)

/* ---------------- Datum layer (postgres.h verbatim shapes) ---------------- */

typedef uintptr_t Datum;
#define SIZEOF_DATUM 8

static inline Datum
PointerGetDatum(const void *X)
{
	return (Datum) X;
}
static inline void *
DatumGetPointer(Datum X)
{
	return (void *) X;
}
static inline char *
DatumGetCString(Datum X)
{
	return (char *) X;
}
static inline Datum
CStringGetDatum(const char *X)
{
	return PointerGetDatum(X);
}
static inline bool
DatumGetBool(Datum X)
{
	return (X != 0);
}
static inline Datum
BoolGetDatum(bool X)
{
	return (Datum) (X ? 1 : 0);
}
static inline int32
DatumGetInt32(Datum X)
{
	return (int32) X;
}
static inline Datum
Int32GetDatum(int32 X)
{
	return (Datum) X;
}
static inline Datum
ObjectIdGetDatum(Oid X)
{
	return (Datum) X;
}

/* ---------------- varlena layer (varatt.h subset, verbatim shapes) -------- */

typedef struct varlena
{
	char		vl_len_[4];
	char		vl_dat[FLEXIBLE_ARRAY_MEMBER];
} varlena;

typedef struct varlena bytea;
typedef struct varlena text;

#define VARHDRSZ		((int32) sizeof(int32))

typedef union
{
	struct
	{
		uint32		va_header;
		char		va_data[FLEXIBLE_ARRAY_MEMBER];
	}			va_4byte;
} varattrib_4b_shim;

#ifdef WORDS_BIGENDIAN
#error "jsonpath_diff oracle assumes little-endian (both lab platforms are)"
#endif

#define VARSIZE(PTR) \
	((((varattrib_4b_shim *) (PTR))->va_4byte.va_header >> 2) & 0x3FFFFFFF)
#define SET_VARSIZE(PTR, len) \
	(((varattrib_4b_shim *) (PTR))->va_4byte.va_header = (((uint32) (len)) << 2))
#define VARDATA(PTR) (((varattrib_4b_shim *) (PTR))->va_4byte.va_data)
#define VARSIZE_ANY(PTR) VARSIZE(PTR)
#define VARSIZE_ANY_EXHDR(PTR) (VARSIZE(PTR) - VARHDRSZ)
#define VARDATA_ANY(PTR) VARDATA(PTR)

/* the oracle only ever sees plain inline 4B-header images the driver built,
 * so detoast is the identity (documented shim; never logic) */
#define PG_DETOAST_DATUM(datum) ((struct varlena *) DatumGetPointer(datum))
#define PG_DETOAST_DATUM_COPY(datum) PG_DETOAST_DATUM(datum)

/* ---------------- memory: TLS arena palloc (pg_jsonpath_env.c) ------------ */

extern void *pg_jsonpath_palloc(Size size);
extern void *pg_jsonpath_palloc0(Size size);
extern void *pg_jsonpath_repalloc(void *ptr, Size size);
extern void pg_jsonpath_pfree(void *ptr);
extern char *pg_jsonpath_pstrdup(const char *in);
extern char *pnstrdup(const char *in, Size len);

#define palloc(sz) pg_jsonpath_palloc(sz)
#define palloc0(sz) pg_jsonpath_palloc0(sz)
#define repalloc(p, sz) pg_jsonpath_repalloc((p), (sz))
#define pfree(p) pg_jsonpath_pfree(p)
#define pstrdup(s) pg_jsonpath_pstrdup(s)

/* regex engine (regcustom.h) uses the no-OOM allocation forms */
#define MCXT_ALLOC_NO_OOM 0x04
extern void *pg_jsonpath_palloc_extended(Size size, int flags);
extern void *pg_jsonpath_repalloc_extended(void *ptr, Size size, int flags);
#define palloc_extended(sz, flags) pg_jsonpath_palloc_extended((sz), (flags))
#define repalloc_extended(p, sz, flags) pg_jsonpath_repalloc_extended((p), (sz), (flags))

extern char *psprintf(const char *fmt, ...) pg_attribute_printf(1, 2);
extern size_t pvsnprintf(char *buf, size_t len, const char *fmt, va_list args);

#define MaxAllocSize	((Size) 0x3fffffff) /* 1 gigabyte - 1 */
#define AllocSizeIsValid(size)	((Size) (size) <= MaxAllocSize)

/* ---------------- error machinery (capture model, see header) ------------- */

/* real sqlstate encoding — utils/elog.h VERBATIM */
#define PGSIXBIT(ch)	(((ch) - '0') & 0x3F)
#define PGUNSIXBIT(val) (((val) & 0x3F) + '0')
#define MAKE_SQLSTATE(ch1,ch2,ch3,ch4,ch5)	\
	(PGSIXBIT(ch1) + (PGSIXBIT(ch2) << 6) + (PGSIXBIT(ch3) << 12) + \
	 (PGSIXBIT(ch4) << 18) + (PGSIXBIT(ch5) << 24))

/* errcodes used by the vendored TUs — values VERBATIM from errcodes.h */
#define ERRCODE_SUCCESSFUL_COMPLETION MAKE_SQLSTATE('0','0','0','0','0')
#define ERRCODE_SYNTAX_ERROR MAKE_SQLSTATE('4','2','6','0','1')
#define ERRCODE_INVALID_TEXT_REPRESENTATION MAKE_SQLSTATE('2','2','P','0','2')
#define ERRCODE_INVALID_BINARY_REPRESENTATION MAKE_SQLSTATE('2','2','P','0','3')
#define ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE MAKE_SQLSTATE('2','2','0','0','3')
#define ERRCODE_INVALID_PARAMETER_VALUE MAKE_SQLSTATE('2','2','0','2','3')
#define ERRCODE_INVALID_REGULAR_EXPRESSION MAKE_SQLSTATE('2','2','0','1','B')
#define ERRCODE_INVALID_ESCAPE_SEQUENCE MAKE_SQLSTATE('2','2','0','2','5')
#define ERRCODE_UNTRANSLATABLE_CHARACTER MAKE_SQLSTATE('2','2','P','0','5')
#define ERRCODE_CHARACTER_NOT_IN_REPERTOIRE MAKE_SQLSTATE('2','2','0','2','1')
#define ERRCODE_FEATURE_NOT_SUPPORTED MAKE_SQLSTATE('0','A','0','0','0')
#define ERRCODE_PROGRAM_LIMIT_EXCEEDED MAKE_SQLSTATE('5','4','0','0','0')
#define ERRCODE_STATEMENT_TOO_COMPLEX MAKE_SQLSTATE('5','4','0','0','1')
#define ERRCODE_PROTOCOL_VIOLATION MAKE_SQLSTATE('0','8','P','0','1')
#define ERRCODE_INTERNAL_ERROR MAKE_SQLSTATE('X','X','0','0','0')
#define ERRCODE_DATETIME_VALUE_OUT_OF_RANGE MAKE_SQLSTATE('2','2','0','0','8')
#define ERRCODE_INDETERMINATE_COLLATION MAKE_SQLSTATE('4','2','P','2','2')

/* elevels (elog.h values) */
#define DEBUG3		12
#define DEBUG1		14
#define NOTICE		18
#define WARNING		19
#define ERROR		21

typedef struct Node Node;		/* full def in nodes/nodes.h */

/* capture channel state (defined in pg_jsonpath_env.c) */
extern _Thread_local int pg_jsonpath_errcode;
extern _Thread_local char pg_jsonpath_errmsg_buf[1024];
extern _Thread_local char pg_jsonpath_errdetail_buf[1024];
extern _Thread_local sigjmp_buf pg_jsonpath_error_jmp;

extern int	errcode(int sqlerrcode);
extern int	errmsg(const char *fmt, ...) pg_attribute_printf(1, 2);
extern int	errmsg_internal(const char *fmt, ...) pg_attribute_printf(1, 2);
extern int	errdetail(const char *fmt, ...) pg_attribute_printf(1, 2);
extern int	errdetail_internal(const char *fmt, ...) pg_attribute_printf(1, 2);
extern int	errhint(const char *fmt, ...) pg_attribute_printf(1, 2);
extern void pg_jsonpath_ereport_finish(int elevel) pg_attribute_noreturn();
extern void pg_jsonpath_errsave_finish(Node *escontext);
extern void pg_jsonpath_elog(int elevel, const char *fmt, ...)
			pg_attribute_printf(2, 3);

/*
 * ereport: evaluate the auxiliary calls (they populate the TLS capture
 * channel), then dispatch on elevel — ERROR longjmps out of the vendored
 * code to the pg_diff_* entry's sigsetjmp. Levels < ERROR are ignored
 * (the vendored TUs only use them for debug output).
 */
#define ereport(elevel, ...) \
	do { \
		pg_jsonpath_errcode = ERRCODE_INTERNAL_ERROR; \
		(void) (__VA_ARGS__); \
		if ((elevel) >= ERROR) \
			pg_jsonpath_ereport_finish(elevel); \
		else \
			pg_jsonpath_errcode = 0; \
	} while (0)

#define elog(elevel, ...) pg_jsonpath_elog((elevel), __VA_ARGS__)

/*
 * errsave/ereturn — the real protocol (miscnodes.h ErrorSaveContext):
 * against a live escontext the error is recorded softly and control
 * continues (ereturn then returns its dummy value); with escontext == NULL
 * this escalates to ereport(ERROR).
 */
#define errsave(context, ...) \
	do { \
		pg_jsonpath_errcode = ERRCODE_INTERNAL_ERROR; \
		(void) (__VA_ARGS__); \
		pg_jsonpath_errsave_finish((Node *) (context)); \
	} while (0)

#define ereturn(context, dummy_value, ...)	\
	do { \
		errsave(context, __VA_ARGS__); \
		return dummy_value; \
	} while (0)

/* ---------------- misc environment ---------------- */

/* port/pg_bswap.h essentials (network byte order helpers) */
#define pg_bswap16(x) __builtin_bswap16(x)
#define pg_bswap32(x) __builtin_bswap32(x)
#define pg_bswap64(x) __builtin_bswap64(x)
#ifdef WORDS_BIGENDIAN
#define pg_hton16(x) (x)
#define pg_hton32(x) (x)
#define pg_hton64(x) (x)
#define pg_ntoh16(x) (x)
#define pg_ntoh32(x) (x)
#define pg_ntoh64(x) (x)
#else
#define pg_hton16(x) pg_bswap16(x)
#define pg_hton32(x) pg_bswap32(x)
#define pg_hton64(x) pg_bswap64(x)
#define pg_ntoh16(x) pg_bswap16(x)
#define pg_ntoh32(x) pg_bswap32(x)
#define pg_ntoh64(x) pg_bswap64(x)
#endif

/* port.h subset: ASCII-only case-insensitive compare (src/port/pgstrcasecmp.c
 * vendored verbatim in pg_support_min.c) */
extern int	pg_strcasecmp(const char *s1, const char *s2);
extern int	pg_strncasecmp(const char *s1, const char *s2, size_t n);
extern unsigned char pg_tolower(unsigned char ch);
extern unsigned char pg_toupper(unsigned char ch);
extern unsigned char pg_ascii_toupper(unsigned char ch);
extern unsigned char pg_ascii_tolower(unsigned char ch);

/* valgrind client requests: off */
#define VALGRIND_CHECK_MEM_IS_DEFINED(addr, len) ((void) 0)
#define VALGRIND_MAKE_MEM_DEFINED(addr, len) ((void) 0)
#define VALGRIND_MAKE_MEM_NOACCESS(addr, len) ((void) 0)
#define VALGRIND_MAKE_MEM_UNDEFINED(addr, len) ((void) 0)


/* --- additions for the jsonpathexec_diff oracle family (shim, NOT PG code):
 * Datum conversion inlines VERBATIM shapes from postgres.h @ 18.3 --- */
static inline uint32
DatumGetUInt32(Datum X)
{
	return (uint32) X;
}

static inline Datum
UInt32GetDatum(uint32 X)
{
	return (Datum) X;
}

static inline uint64
DatumGetUInt64(Datum X)
{
	return (uint64) X;
}

static inline Datum
UInt64GetDatum(uint64 X)
{
	return (Datum) X;
}

static inline int64
DatumGetInt64(Datum X)
{
	return (int64) X;
}

static inline Datum
Int64GetDatum(int64 X)
{
	return (Datum) X;
}

static inline char
DatumGetChar(Datum X)
{
	return (char) X;
}

static inline Datum
CharGetDatum(char X)
{
	return (Datum) X;
}

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

/* pg_rotate_left32 VERBATIM from port/pg_bitutils.h @ 18.3 */
static inline uint32
pg_rotate_left32(uint32 word, int n)
{
	return (word << n) | (word >> (32 - n));
}


/* --- additions for the jsonpathexec_diff oracle family (shim; sqlstates are
 * the real MAKE_SQLSTATE encodings from errcodes.h @ 18.3) --- */
#define ERRCODE_DUPLICATE_JSON_OBJECT_KEY_VALUE MAKE_SQLSTATE('2','2','0','3','0')
#define ERRCODE_DIVISION_BY_ZERO MAKE_SQLSTATE('2','2','0','1','2')
#define ERRCODE_UNDEFINED_OBJECT MAKE_SQLSTATE('4','2','7','0','4')
#define ERRCODE_SINGLETON_SQL_JSON_ITEM_REQUIRED MAKE_SQLSTATE('2','2','0','3','8')
#define ERRCODE_NON_NUMERIC_SQL_JSON_ITEM MAKE_SQLSTATE('2','2','0','3','6')
#define ERRCODE_INVALID_ARGUMENT_FOR_SQL_JSON_DATETIME_FUNCTION MAKE_SQLSTATE('2','2','0','3','1')
#define ERRCODE_INVALID_SQL_JSON_SUBSCRIPT MAKE_SQLSTATE('2','2','0','3','3')
#define ERRCODE_SQL_JSON_ARRAY_NOT_FOUND MAKE_SQLSTATE('2','2','0','3','9')
#define ERRCODE_SQL_JSON_MEMBER_NOT_FOUND MAKE_SQLSTATE('2','2','0','3','A')
#define ERRCODE_SQL_JSON_NUMBER_NOT_FOUND MAKE_SQLSTATE('2','2','0','3','B')
#define ERRCODE_SQL_JSON_OBJECT_NOT_FOUND MAKE_SQLSTATE('2','2','0','3','C')
#define ERRCODE_SQL_JSON_SCALAR_REQUIRED MAKE_SQLSTATE('2','2','0','3','F')
#define ERRCODE_SQL_JSON_ITEM_CANNOT_BE_CAST_TO_TARGET_TYPE MAKE_SQLSTATE('2','2','0','3','G')
#define ERRCODE_DATETIME_FIELD_OVERFLOW MAKE_SQLSTATE('2','2','0','0','8')
#define ERRCODE_INVALID_DATETIME_FORMAT MAKE_SQLSTATE('2','2','0','0','7')
#define ERRCODE_MORE_THAN_ONE_SQL_JSON_ITEM MAKE_SQLSTATE('2','2','0','3','4')
#define ERRCODE_NO_SQL_JSON_ITEM MAKE_SQLSTATE('2','2','0','3','5')
#define ERRCODE_INVALID_JSON_TEXT MAKE_SQLSTATE('2','2','0','3','2')

/* qsort_arg (vendored verbatim pg_qsort_arg.c) */
typedef int (*qsort_arg_comparator) (const void *a, const void *b, void *arg);
extern void qsort_arg(void *base, size_t nel, size_t elsize,
					  qsort_arg_comparator cmp, void *arg);


/* MemoryContext model (shim): the oracle family runs on the TLS pointer
 * arena in pg_jsonpath_env.c; contexts are opaque tokens, switches are
 * recorded but allocation always goes to the arena (per-entry reset). The
 * JsonTable machinery that creates/resets private contexts is UNREACHABLE
 * (executor carve) and its context entry points are loud abort stubs. */
typedef struct MemoryContextData *MemoryContext;
extern MemoryContext CurrentMemoryContext;
extern MemoryContext TopMemoryContext;
extern MemoryContext MemoryContextSwitchTo(MemoryContext context);
extern MemoryContext AllocSetContextCreate(MemoryContext parent,
										   const char *name, int flags);
extern void MemoryContextResetOnly(MemoryContext context);
extern void MemoryContextDelete(MemoryContext context);
#define ALLOCSET_DEFAULT_SIZES 0
#define ALLOCSET_SMALL_SIZES 0
extern void MemoryContextSetIdentifier(MemoryContext context, const char *id);
extern void MemoryContextSetParent(MemoryContext context, MemoryContext new_parent);


/* asserts compiled out (production build model) */
#define PG_USED_FOR_ASSERTS_ONLY pg_attribute_unused()
#ifndef pg_attribute_unused
#define pg_attribute_unused() __attribute__((unused))
#endif

static inline text *
DatumGetTextP(Datum X)
{
	return (text *) PG_DETOAST_DATUM(X);
}


static inline float4
DatumGetFloat4(Datum X)
{
	union { int32 value; float4 retval; } myunion;
	myunion.value = (int32) X;
	return myunion.retval;
}

/* unconstify VERBATIM from c.h @ 18.3 */
#define unconstify(underlying_type, expr) \
	(StaticAssertExpr(__builtin_types_compatible_p(__typeof(expr), const underlying_type), \
					  "wrong cast"), \
	 (underlying_type) (expr))
#ifndef StaticAssertExpr
#define StaticAssertExpr(condition, errmessage) \
	((void) ({ _Static_assert(condition, errmessage); }))
#endif

/* no TOAST in this harness: all varlenas are plain 4B-header images */
static inline struct varlena *
pg_detoast_datum_packed(struct varlena *datum)
{
	return datum;
}
#endif							/* PG_JSONPATH_DIFF_SHIM_POSTGRES_H */
