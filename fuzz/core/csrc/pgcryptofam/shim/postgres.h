/*
 * SHIM postgres.h for the pgcryptofam_diff oracle TUs (lane p1-pgcryptofam).
 * NOT PostgreSQL code — plumbing only, never logic. Every vendored body under
 * ../vendor/ is verbatim PostgreSQL 18.3 (upstream 62d6c7d3df6287f1bd83199c1a746e50d31571a0):
 *   contrib/pgcrypto/{px-crypt,crypt-md5,crypt-des,crypt-blowfish,crypt-gensalt,
 *   crypt-sha,pgp-armor,px}.c, src/common/{stringinfo,psprintf,string,md5,
 *   md5_common,sha1,sha2,cryptohash,base64}.c, src/port/{snprintf,pgstrcasecmp,
 *   strlcpy}.c.
 *
 * Error model: ereport/elog route through a setjmp channel that records
 * (sqlstate, elevel, message text):
 *   - elevel >= ERROR  -> record + longjmp back to the armed pg_diff_* entry
 *     (models PG's error longjmp; sqlstate defaults to ERRCODE_INTERNAL_ERROR
 *     exactly as elog.c errstart does for elevel >= ERROR).
 *   - NOTICE/WARNING   -> RECORDED AND RETURNED in the status struct, then
 *     control continues (crypt-sha.c's rounds-clamp NOTICE is part of the
 *     compared observable behavior).
 *   - < NOTICE (DEBUG1) -> suppressed without evaluating the aux args, the
 *     same as a production server at default log_min_messages.
 *
 * palloc/pfree -> per-exec bump arena reset wholesale at the top of every
 * pg_diff_pgcryptofam_* entry (the csrc/contribb precedent): crypt-sha.c
 * frees its two PX_MD contexts and two StringInfos on the success path only,
 * so an error-path longjmp leaks by construction — the arena makes that
 * structurally harmless (no LSan suppressions needed).
 *
 * The pgcryptofam objects are compiled -funsigned-char: plain char signedness
 * is implementation-defined and PG inherits the platform default; the oracle
 * of record is the fleet's Linux/aarch64 build where char is UNSIGNED.
 * Without the pin a macOS (signed-char) local build diverges on salt/password
 * bytes >= 0x80 (e.g. crypt-des ascii_to_bin, crypt-blowfish BF_atoi64).
 */
#ifndef PGCRYPTOFAM_SHIM_POSTGRES_H
#define PGCRYPTOFAM_SHIM_POSTGRES_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <limits.h>
#include <errno.h>

/* ---- c.h type layer (verbatim-equivalent fixed-width typedefs, LP64) ---- */

typedef int8_t int8;
typedef int16_t int16;
typedef int32_t int32;
typedef int64_t int64;
typedef uint8_t uint8;
typedef uint16_t uint16;
typedef uint32_t uint32;
typedef uint64_t uint64;
typedef size_t Size;

#define UINT64CONST(x) UINT64_C(x)
#define INT64CONST(x) INT64_C(x)

/* verbatim c.h values */
#define HIGHBIT					(0x80)
#define IS_HIGHBIT_SET(ch)		((unsigned char)(ch) & HIGHBIT)

#define Min(x, y) ((x) < (y) ? (x) : (y))
#define Max(x, y) ((x) > (y) ? (x) : (y))
#define lengthof(array) (sizeof (array) / sizeof ((array)[0]))
/* Assert compiled out — matches a production (NDEBUG) PostgreSQL build */
#define Assert(condition) ((void) 0)
#define AssertMacro(condition) ((void) true)
#define StaticAssertDecl(condition, errmessage) \
	_Static_assert(condition, errmessage)
#define StaticAssertStmt(condition, errmessage) \
	do { _Static_assert(condition, errmessage); } while (0)
#define FLEXIBLE_ARRAY_MEMBER	/* empty */
#define pg_restrict __restrict
#define unlikely(x) (x)
#define likely(x) (x)
#define _(x) (x)
#define gettext_noop(x) (x)
#define pg_attribute_unused()
#define pg_attribute_printf(f, a) __attribute__((format(printf, f, a)))
#define pg_noreturn _Noreturn
#define pg_nodiscard
#define PGDLLIMPORT
#define PG_USED_FOR_ASSERTS_ONLY pg_attribute_unused()

/* zeroization strength is process hygiene, never a compared output plane */
#ifndef explicit_bzero
#define explicit_bzero(p, n) memset((p), 0, (n))
#endif

/*
 * strlcpy: the vendored src/port/strlcpy.c body must not collide with the
 * libc strlcpy that macOS (and glibc >= 2.38) declare in <string.h>.
 * Renamed here AFTER <string.h> is included (a -D on the command line loses
 * to Apple's _FORTIFY re-#define — the csrc/portfam precedent).
 */
#undef strlcpy
#define strlcpy pgcryptofam_strlcpy
extern Size pgcryptofam_strlcpy(char *dst, const char *src, Size siz);

/*
 * printf replacement layer, port.h parity: every PostgreSQL build compiles
 * with USE_REPL_SNPRINTF, so the backend's snprintf/vsnprintf ARE
 * pg_snprintf/pg_vsnprintf (vendored verbatim src/port/snprintf.c).
 * pvsnprintf's vsnprintf call and crypt-gensalt.c's pg_snprintf call bind the
 * same engine the ratified oracle runs.
 */
extern int	pg_vsnprintf(char *str, size_t count, const char *fmt, va_list args);
extern int	pg_snprintf(char *str, size_t count, const char *fmt,...) pg_attribute_printf(3, 4);
extern int	pg_vsprintf(char *str, const char *fmt, va_list args);
extern int	pg_sprintf(char *str, const char *fmt,...) pg_attribute_printf(2, 3);
extern int	pg_vfprintf(FILE *stream, const char *fmt, va_list args);
extern int	pg_fprintf(FILE *stream, const char *fmt,...) pg_attribute_printf(2, 3);
extern int	pg_vprintf(const char *fmt, va_list args);
extern int	pg_printf(const char *fmt,...) pg_attribute_printf(1, 2);
#undef vsnprintf				/* Apple fortify macros in secure/_stdio.h */
#undef snprintf
#undef vsprintf
#undef sprintf
#define vsnprintf pg_vsnprintf
#define snprintf pg_snprintf
#define vsprintf pg_vsprintf
#define sprintf pg_sprintf

/*
 * strerror_r, port.h parity: snprintf.c's %m arm assigns the result to
 * const char * (GNU semantics); PG routes this through pg_strerror_r.
 * SHIM (env): %m is unreachable from every format string in this cone —
 * the helper exists only so the verbatim TU compiles on POSIX strerror_r
 * platforms (macOS).
 */
#define PG_STRERROR_R_BUFLEN 256	/* verbatim port.h value */
#undef strerror_r
#define strerror_r pgcryptofam_strerror_r
extern char *pgcryptofam_strerror_r(int errnum, char *buf, size_t buflen);

/* ---- per-exec arena palloc (see header comment) ---- */

extern void *pgcryptofam_palloc(Size size);
extern void *pgcryptofam_palloc0(Size size);
extern void *pgcryptofam_palloc_extended(Size size, int flags);
extern void *pgcryptofam_repalloc(void *ptr, Size size);
extern void pgcryptofam_pfree(void *ptr);
extern char *pgcryptofam_pstrdup(const char *s);

#define palloc pgcryptofam_palloc
#define palloc0 pgcryptofam_palloc0
#define palloc_extended pgcryptofam_palloc_extended
#define repalloc pgcryptofam_repalloc
#define pfree pgcryptofam_pfree
#define pstrdup pgcryptofam_pstrdup

/* ---- elevels: verbatim values from src/include/utils/elog.h ---- */

#define DEBUG5		10
#define DEBUG4		11
#define DEBUG3		12
#define DEBUG2		13
#define DEBUG1		14
#define LOG			15
#define LOG_SERVER_ONLY 16
#define INFO		17
#define NOTICE		18
#define WARNING		19
#define WARNING_CLIENT_ONLY 20
#define ERROR		21
#define FATAL		22
#define PANIC		23

/* ---- SQLSTATEs: verbatim MAKE_SQLSTATE encoding from elog.h; the code
 * values are transcribed from src/backend/utils/errcodes.txt (errcodes.h is
 * a generated file not present in the vendored tree) ---- */

#define PGSIXBIT(ch)	(((ch) - '0') & 0x3F)
#define MAKE_SQLSTATE(ch1,ch2,ch3,ch4,ch5)	\
	(PGSIXBIT(ch1) + (PGSIXBIT(ch2) << 6) + (PGSIXBIT(ch3) << 12) + \
	 (PGSIXBIT(ch4) << 18) + (PGSIXBIT(ch5) << 24))

#define ERRCODE_SUCCESSFUL_COMPLETION MAKE_SQLSTATE('0','0','0','0','0')
#define ERRCODE_WARNING MAKE_SQLSTATE('0','1','0','0','0')
#define ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE MAKE_SQLSTATE('2','2','0','0','3')
#define ERRCODE_INVALID_PARAMETER_VALUE MAKE_SQLSTATE('2','2','0','2','3')
#define ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION MAKE_SQLSTATE('3','9','0','0','0')
#define ERRCODE_SYNTAX_ERROR MAKE_SQLSTATE('4','2','6','0','1')
#define ERRCODE_PROGRAM_LIMIT_EXCEEDED MAKE_SQLSTATE('5','4','0','0','0')
#define ERRCODE_INTERNAL_ERROR MAKE_SQLSTATE('X','X','0','0','0')
#define ERRCODE_NAME_TOO_LONG MAKE_SQLSTATE('4','2','6','2','2')

/* verbatim src/include/pg_config_manual.h value (identifier truncation
 * length used by the vendored scansup.c) */
#define NAMEDATALEN 64

/*
 * Encoding environment for the vendored scansup.c downcase path. The
 * campaign pins a SINGLE-BYTE database encoding (SQL_ASCII), so
 * pg_database_encoding_max_length() is 1 and pg_mbcliplen() reduces to
 * mbutils.c's `cliplen` — see pgcryptofam_shim.c for the verbatim body
 * and the ENVIRONMENT MOCK note.
 */
extern int	pg_database_encoding_max_length(void);
extern int	pg_mbcliplen(const char *mbstr, int len, int limit);

/* ---- ereport channel (see header comment; impl in pgcryptofam_shim.c) ---- */

extern bool pgcryptofam_errstart(int elevel);
extern pg_noreturn void pgcryptofam_errfinish_error(int elevel);
extern void pgcryptofam_errfinish(int elevel);
extern int	pgcryptofam_errcode(int sqlerrcode);
extern int	pgcryptofam_errmsg(const char *fmt,...) pg_attribute_printf(1, 2);
extern int	pgcryptofam_errdetail(const char *fmt,...) pg_attribute_printf(1, 2);
extern int	pgcryptofam_errhint(const char *fmt,...) pg_attribute_printf(1, 2);

#define errcode(sqlerrcode) pgcryptofam_errcode(sqlerrcode)
#define errmsg(...) pgcryptofam_errmsg(__VA_ARGS__)
#define errmsg_internal(...) pgcryptofam_errmsg(__VA_ARGS__)
#define errdetail(...) pgcryptofam_errdetail(__VA_ARGS__)
#define errdetail_internal(...) pgcryptofam_errdetail(__VA_ARGS__)
#define errhint(...) pgcryptofam_errhint(__VA_ARGS__)

/*
 * ereport/elog: the aux args are evaluated only when errstart says the level
 * is reported (mirrors elog.h's errstart-guarded expansion); errfinish
 * longjmps for elevel >= ERROR and records-and-returns for NOTICE/WARNING.
 */
#define ereport(elevel, ...) \
	do { \
		if (pgcryptofam_errstart(elevel)) \
		{ \
			__VA_ARGS__; \
			pgcryptofam_errfinish(elevel); \
		} \
		if (__builtin_constant_p(elevel) && (elevel) >= ERROR) \
			__builtin_unreachable(); \
	} while (0)

#define elog(elevel, ...) \
	do { \
		if (pgcryptofam_errstart(elevel)) \
		{ \
			pgcryptofam_errmsg(__VA_ARGS__); \
			pgcryptofam_errfinish(elevel); \
		} \
		if (__builtin_constant_p(elevel) && (elevel) >= ERROR) \
			__builtin_unreachable(); \
	} while (0)

/* port.h parity: pgstrcasecmp.c's exports, declared for every consumer */
extern int	pg_strcasecmp(const char *s1, const char *s2);
extern int	pg_strncasecmp(const char *s1, const char *s2, size_t n);
extern unsigned char pg_toupper(unsigned char ch);
extern unsigned char pg_tolower(unsigned char ch);
extern unsigned char pg_ascii_toupper(unsigned char ch);
extern unsigned char pg_ascii_tolower(unsigned char ch);

/* port.h parity: psprintf.c's exports */
extern char *psprintf(const char *fmt,...) pg_attribute_printf(1, 2);
extern size_t pvsnprintf(char *buf, size_t len, const char *fmt, va_list args) pg_attribute_printf(3, 0);

/*
 * postgres.h -> utils/elog.h includes lib/stringinfo.h in the real 18.3
 * header topology, which is how crypt-sha.c sees the StringInfo layer
 * without including it directly; mirror that here (the header itself is
 * the verbatim src/include/lib/stringinfo.h under vendor/include).
 */
#include "lib/stringinfo.h"

/*
 * pg_strong_random (src/port/pg_strong_random.c in a real build): satisfied
 * from the caller-injected entropy buffer so px_gen_salt's deterministic
 * output planes are reproducible. SHIM (env): the randomness SOURCE is
 * environment, its consumers are all verbatim.
 */
extern bool pg_strong_random(void *buf, size_t len);

#endif							/* PGCRYPTOFAM_SHIM_POSTGRES_H */
