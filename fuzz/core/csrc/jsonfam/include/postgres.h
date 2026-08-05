/*
 * jsonfam shim postgres.h — minimal backend environment for compiling the
 * VERBATIM vendored PostgreSQL 18.3 files under fuzz/core/csrc/jsonfam/
 * (jsonapi.c, stringinfo.c) plus the pg_json_io.c oracle TU.
 *
 * Provenance of everything vendored here: PostgreSQL 18.3 (Stamp-18.3,
 * upstream sha 62d6c7d3df6287f1bd83199c1a746e50d31571a0), verified against
 * ../pgrust-fabled/vendor/postgres-src.
 *
 * SHIMS (plumbing only, never logic):
 *   - palloc/palloc0/repalloc/pfree/pstrdup -> the TLS arena in
 *     pg_json_io.c (models memory-context reset; see the LSan incident
 *     class, proofs/p1-lanej @ 7306d300196).
 *   - elog/ereport(ERROR) -> record errcode in pg_diff_errcode and longjmp
 *     out through pg_jsonfam_jmp (defined in pg_json_io.c). Message text is
 *     out of scope for the differential; err* argument calls are swallowed.
 *   - check_stack_depth() -> pg_jsonfam_stack_guard(): counts nesting and
 *     fires ERRCODE_STATEMENT_TOO_COMPLEX at a huge pinned depth, mirroring
 *     the backend guard's role. The fuzz driver caps input length well below
 *     both sides' limits, so the guard is never load-bearing in the diff.
 *   - SYMBOL PREFIX: exported names of the vendored TUs are #define-renamed
 *     with a pg_jsonfam_ prefix so this oracle can never collide with other
 *     lanes' vendored copies (e.g. the jsonb lane's) in the single fuzz-core
 *     archive. Pure rename; bodies untouched.
 */
#ifndef PG_JSONFAM_POSTGRES_H
#define PG_JSONFAM_POSTGRES_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <stdio.h>
#include <setjmp.h>
#include <limits.h>
#include <ctype.h>
#include <errno.h>

/* ---- c.h basics ---- */
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
typedef uint32 bits32;

#define IS_HIGHBIT_SET(ch)	((unsigned char)(ch) & 0x80)

#define PG_INT32_MAX INT32_MAX
#define PG_INT32_MIN INT32_MIN
#define PG_UINT16_MAX UINT16_MAX

#define lengthof(array) (sizeof (array) / sizeof ((array)[0]))
#define Min(x, y) ((x) < (y) ? (x) : (y))
#define Max(x, y) ((x) > (y) ? (x) : (y))

#define pg_attribute_unused()
#define pg_attribute_noreturn()
#define pg_attribute_printf(f, a)
#define pg_attribute_always_inline inline
#define pg_noreturn _Noreturn
#define pg_nodiscard
#define pg_restrict __restrict
#define PGDLLIMPORT
#define INLINE_IF_POSSIBLE static inline
#define unlikely(x) __builtin_expect((x) != 0, 0)
#define likely(x) __builtin_expect((x) != 0, 1)
#define UnusedArg(arg) ((void) (arg))

#define StaticAssertDecl(condition, errmessage) \
	_Static_assert(condition, errmessage)
#define StaticAssertStmt(condition, errmessage) \
	do { _Static_assert(condition, errmessage); } while(0)

/* Release-backend parity: Assert compiles out (no USE_ASSERT_CHECKING). */
#define Assert(x) ((void) 0)

/* gettext no-op */
#define _(x) (x)

/* MaxAllocSize from memutils.h (used by stringinfo.c enlarge) */
#define MaxAllocSize ((Size) 0x3fffffff)	/* 1 gigabyte - 1 */

/* ---- palloc family -> TLS arena (pg_json_io.c) ---- */
extern void *pg_jsonfam_palloc(Size n);
extern void *pg_jsonfam_palloc0(Size n);
extern void *pg_jsonfam_repalloc(void *p, Size n);
extern void pg_jsonfam_pfree(void *p);
extern char *pg_jsonfam_pstrdup(const char *s);
extern char *pg_jsonfam_psprintf(const char *fmt, ...);
#define psprintf pg_jsonfam_psprintf
extern size_t pg_jsonfam_pvsnprintf(char *buf, size_t len, const char *fmt, va_list args);
#define pvsnprintf pg_jsonfam_pvsnprintf
#define palloc pg_jsonfam_palloc
#define palloc0 pg_jsonfam_palloc0
#define repalloc pg_jsonfam_repalloc
#define pfree pg_jsonfam_pfree
#define pstrdup pg_jsonfam_pstrdup

/* ---- elog/ereport -> errcode record + longjmp (pg_json_io.c) ---- */
extern _Thread_local jmp_buf pg_jsonfam_jmp;
extern _Thread_local int pg_jsonfam_errcode;	/* PG errcode of the fired ereport */
extern pg_noreturn void pg_jsonfam_error_fire(int code);

/* MAKE_SQLSTATE from elog.h, verbatim */
#define PGSIXBIT(ch)	(((ch) - '0') & 0x3F)
#define MAKE_SQLSTATE(ch1,ch2,ch3,ch4,ch5)	\
	(PGSIXBIT(ch1) + (PGSIXBIT(ch2) << 6) + (PGSIXBIT(ch3) << 12) + \
	 (PGSIXBIT(ch4) << 18) + (PGSIXBIT(ch5) << 24))

/* errcodes this family raises (verbatim values from utils/errcodes.h) */
#define ERRCODE_INVALID_TEXT_REPRESENTATION MAKE_SQLSTATE('2','2','P','0','2')
#define ERRCODE_UNTRANSLATABLE_CHARACTER MAKE_SQLSTATE('2','2','P','0','5')
#define ERRCODE_INVALID_PARAMETER_VALUE MAKE_SQLSTATE('2','2','0','2','3')
#define ERRCODE_ARRAY_SUBSCRIPT_ERROR MAKE_SQLSTATE('2','2','0','2','E')
#define ERRCODE_NULL_VALUE_NOT_ALLOWED MAKE_SQLSTATE('2','2','0','0','4')
#define ERRCODE_DUPLICATE_JSON_OBJECT_KEY_VALUE MAKE_SQLSTATE('2','2','0','3','0')
#define ERRCODE_STATEMENT_TOO_COMPLEX MAKE_SQLSTATE('5','4','0','0','1')
#define ERRCODE_PROGRAM_LIMIT_EXCEEDED MAKE_SQLSTATE('5','4','0','0','0')
#define ERRCODE_OUT_OF_MEMORY MAKE_SQLSTATE('5','3','2','0','0')

/*
 * ereport shim: evaluates errcode(...) via the helpers below (the last
 * errcode(..) call before the fire wins), swallows errmsg/errdetail/etc.,
 * then longjmps. ERROR and above never return, matching the backend.
 */
#define ERROR 21
#define ereport(elevel, ...) \
	do { pg_jsonfam_pending_errcode = ERRCODE_INVALID_TEXT_REPRESENTATION; \
		 __VA_ARGS__; \
		 pg_jsonfam_error_fire(pg_jsonfam_pending_errcode); } while (0)
#define elog(elevel, ...) \
	do { pg_jsonfam_error_fire(ERRCODE_INVALID_TEXT_REPRESENTATION); } while (0)

extern _Thread_local int pg_jsonfam_pending_errcode;
static inline int errcode(int code) { pg_jsonfam_pending_errcode = code; return 0; }
static inline int errmsg(const char *fmt, ...) { (void) fmt; return 0; }
static inline int errdetail(const char *fmt, ...) { (void) fmt; return 0; }
static inline int errdetail_internal(const char *fmt, ...) { (void) fmt; return 0; }
static inline int errhint(const char *fmt, ...) { (void) fmt; return 0; }
static inline int errcontext_msg(const char *fmt, ...) { (void) fmt; return 0; }
#define errcontext errcontext_msg

/* ---- symbol prefix for the vendored TUs (pure rename shim) ---- */
#define makeJsonLexContextCstringLen pg_jsonfam_makeJsonLexContextCstringLen
#define makeJsonLexContextIncremental pg_jsonfam_makeJsonLexContextIncremental
#define setJsonLexContextOwnsTokens pg_jsonfam_setJsonLexContextOwnsTokens
#define freeJsonLexContext pg_jsonfam_freeJsonLexContext
#define pg_parse_json pg_jsonfam_pg_parse_json
#define pg_parse_json_incremental pg_jsonfam_pg_parse_json_incremental
#define json_count_array_elements pg_jsonfam_json_count_array_elements
#define json_lex pg_jsonfam_json_lex
#define json_errdetail pg_jsonfam_json_errdetail
#define IsValidJsonNumber pg_jsonfam_IsValidJsonNumber
#define nullSemAction pg_jsonfam_nullSemAction
/* stringinfo.c exports */
#define makeStringInfo pg_jsonfam_makeStringInfo
#define initStringInfo pg_jsonfam_initStringInfo
#define initStringInfoWithSize pg_jsonfam_initStringInfoWithSize
#define resetStringInfo pg_jsonfam_resetStringInfo
#define appendStringInfo pg_jsonfam_appendStringInfo
#define appendStringInfoVA pg_jsonfam_appendStringInfoVA
#define appendStringInfoString pg_jsonfam_appendStringInfoString
#define appendStringInfoChar pg_jsonfam_appendStringInfoChar
#define appendStringInfoSpaces pg_jsonfam_appendStringInfoSpaces
#define appendBinaryStringInfo pg_jsonfam_appendBinaryStringInfo
#define appendBinaryStringInfoNT pg_jsonfam_appendBinaryStringInfoNT
#define destroyStringInfo pg_jsonfam_destroyStringInfo
#define enlargeStringInfo pg_jsonfam_enlargeStringInfo

#endif							/* PG_JSONFAM_POSTGRES_H */
