/*
 * gucfile/postgres.h — self-contained shim environment for the verbatim
 * PostgreSQL 18.3 guc-file.l scanner (guc_file_diff target, lane p1-wavef).
 *
 * PLUMBING ONLY, never logic. Vendored verbatim pieces are marked; every
 * other definition is environment mocking (allocator arena, ereport
 * capture, level/errcode constants copied verbatim from elog.h /
 * errcodes.txt values, ConfigVariable copied verbatim from utils/guc.h).
 *
 * SYMBOL HYGIENE (hard rule 2026-08-01): every extern this family's TUs
 * define or reference is renamed with the gucf_ family prefix below; the
 * flex scanner's own exports already carry the GUC_yy prefix
 * (%option prefix="GUC_yy" in the verbatim .l). nm census required before
 * push.
 */
#ifndef GUCFILE_SHIM_POSTGRES_H
#define GUCFILE_SHIM_POSTGRES_H

/* Pull in every system header the flex skeleton will include later, so the
 * malloc/realloc/free arena macros below cannot rewrite system-header
 * declarations (header guards make the later includes no-ops). */
#include <stddef.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <setjmp.h>
#include <errno.h>
#include <limits.h>
#include <inttypes.h>
#include <ctype.h>
#include <unistd.h>

/* ---------- family symbol prefix renames (link isolation) ---------- */
#define ProcessConfigFile gucf_ProcessConfigFile
#define ProcessConfigFileInternal gucf_ProcessConfigFileInternal
#define ParseConfigFile gucf_ParseConfigFile
#define ParseConfigFp gucf_ParseConfigFp
#define ParseConfigDirectory gucf_ParseConfigDirectory
#define record_config_file_error gucf_record_config_file_error
#define FreeConfigVariables gucf_FreeConfigVariables
#define DeescapeQuotedString gucf_DeescapeQuotedString
#define guc_name_compare gucf_guc_name_compare
#define AbsoluteConfigLocation gucf_AbsoluteConfigLocation
#define AllocateFile gucf_AllocateFile
#define FreeFile gucf_FreeFile
#define GetConfFilesInDir gucf_GetConfFilesInDir
#define psprintf gucf_psprintf
#define IsUnderPostmaster gucf_IsUnderPostmaster
#define AllocSetContextCreate gucf_AllocSetContextCreate
#define MemoryContextSwitchTo gucf_MemoryContextSwitchTo
#define MemoryContextDelete gucf_MemoryContextDelete
#define CurrentMemoryContext gucf_CurrentMemoryContext
#define unpack_sql_state gucf_unpack_sql_state

/* ---------- c.h-level typedefs (LP64 parity) ---------- */
typedef size_t Size;
typedef void *MemoryContext;

#define Assert(x) ((void) 0)	/* release-build parity */

/* ---------- GucContext (utils/guc.h, values in enum order) ---------- */
typedef enum
{
	PGC_INTERNAL,
	PGC_POSTMASTER,
	PGC_SIGHUP,
	PGC_SU_BACKEND,
	PGC_BACKEND,
	PGC_SUSET,
	PGC_USERSET,
} GucContext;

/* ---------- ConfigVariable — VERBATIM utils/guc.h lines 139-149 ---------- */
typedef struct ConfigVariable
{
	char	   *name;
	char	   *value;
	char	   *errmsg;
	char	   *filename;
	int			sourceline;
	bool		ignore;
	bool		applied;
	struct ConfigVariable *next;
} ConfigVariable;

/* prototypes the scanner's code section gets from utils/guc.h upstream */
extern void ProcessConfigFile(GucContext context);
extern bool ParseConfigFile(const char *config_file, bool strict,
							const char *calling_file, int calling_lineno,
							int depth, int elevel,
							struct ConfigVariable **head_p,
							struct ConfigVariable **tail_p);
extern bool ParseConfigFp(FILE *fp, const char *config_file, int depth,
						  int elevel, struct ConfigVariable **head_p,
						  struct ConfigVariable **tail_p);
extern bool ParseConfigDirectory(const char *includedir,
								 const char *calling_file, int calling_lineno,
								 int depth, int elevel,
								 struct ConfigVariable **head_p,
								 struct ConfigVariable **tail_p);
extern void record_config_file_error(const char *errmsg,
									 const char *config_file, int lineno,
									 struct ConfigVariable **head_p,
									 struct ConfigVariable **tail_p);
extern void FreeConfigVariables(struct ConfigVariable *list);
extern char *DeescapeQuotedString(const char *s);

/* ---------- error levels — VERBATIM utils/elog.h values ---------- */
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
#define WARNING_CLIENT_ONLY	20
#define ERROR		21
#define FATAL		22
#define PANIC		23

/* ---------- sqlstate packing — VERBATIM utils/elog.h lines 67-72 ---------- */
#define PGSIXBIT(ch)	(((ch) - '0') & 0x3F)
#define PGUNSIXBIT(val) (((val) & 0x3F) + '0')
#define MAKE_SQLSTATE(ch1,ch2,ch3,ch4,ch5)	\
	(PGSIXBIT(ch1) + (PGSIXBIT(ch2) << 6) + (PGSIXBIT(ch3) << 12) + \
	 (PGSIXBIT(ch4) << 18) + (PGSIXBIT(ch5) << 24))

/* errcodes used by guc-file.l — sqlstates verbatim from errcodes.txt */
#define ERRCODE_INVALID_PARAMETER_VALUE MAKE_SQLSTATE('2','2','0','2','3')
#define ERRCODE_SYNTAX_ERROR MAKE_SQLSTATE('4','2','6','0','1')
#define ERRCODE_PROGRAM_LIMIT_EXCEEDED MAKE_SQLSTATE('5','4','0','0','0')
#define ERRCODE_INTERNAL_ERROR MAKE_SQLSTATE('X','X','0','0','0')

/* ---------- ereport/elog capture shim (environment mock) ----------
 * ereport(elevel, (errcode(..), errmsg(..))) expands to a comma expression:
 * begin -> aux calls record into the pending slot -> finish records the
 * report and siglongjmps to the driver when elevel >= ERROR (models PG's
 * error longjmp). elog carries ERRCODE_INTERNAL_ERROR by default, exactly
 * as PG's elog does for >= ERROR.
 */
extern void gucf_ereport_begin(int elevel);
extern int	gucf_errcode(int sqlerrcode);
extern int	gucf_errmsg(const char *fmt, ...) __attribute__((format(printf, 1, 2)));
extern void gucf_ereport_finish(void);

#define ereport(elevel, rest) \
	(gucf_ereport_begin(elevel), rest, gucf_ereport_finish())
#define errcode(c) gucf_errcode(c)
#define errmsg(...) gucf_errmsg(__VA_ARGS__)
#define elog(elevel, ...) \
	(gucf_ereport_begin(elevel), gucf_errmsg(__VA_ARGS__), gucf_ereport_finish())
extern int	gucf_errcode_for_file_access(void);
#define errcode_for_file_access() gucf_errcode_for_file_access()

/* ---------- allocator arena (environment mock) ----------
 * All allocations in the scanner TU — palloc/pstrdup/pfree AND the flex
 * skeleton's own malloc/realloc/free inside GUC_yyalloc/yyrealloc/yyfree —
 * are routed into a driver-owned arena that pg_gucf_run() resets per
 * iteration, so the sigsetjmp escape from ereport(ERROR) leaks nothing.
 * The driver TU (GUCF_DRIVER_TU) keeps real malloc for its own machinery.
 */
extern void *gucf_arena_malloc(size_t size);
extern void *gucf_arena_realloc(void *ptr, size_t size);
extern void gucf_arena_free(void *ptr);
extern char *gucf_arena_strdup(const char *s);

#define palloc(sz) gucf_arena_malloc(sz)
#define pstrdup(s) gucf_arena_strdup(s)
#define pfree(p) gucf_arena_free(p)

#ifndef GUCF_DRIVER_TU
#define malloc(sz) gucf_arena_malloc(sz)
#define realloc(p, sz) gucf_arena_realloc(p, sz)
#define free(p) gucf_arena_free(p)
#endif

/* ---------- declarations for OUT-of-carve dependencies ----------
 * These are file-IO / process-state functions excluded by the census carve
 * (include-directive-free input domain). The driver TU defines them as
 * loud abort() stubs: any reach is a driver domain-filter breach, never a
 * silent fabricated result.
 */
extern char *AbsoluteConfigLocation(const char *location, const char *calling_file);
extern FILE *AllocateFile(const char *name, const char *mode);
extern int	FreeFile(FILE *file);
extern char **GetConfFilesInDir(const char *includedir, const char *calling_file,
								int elevel, int *num_filenames, char **err_msg);
extern char *psprintf(const char *fmt, ...);
extern bool IsUnderPostmaster;
extern MemoryContext CurrentMemoryContext;
extern MemoryContext AllocSetContextCreate(MemoryContext parent, const char *name,
										   Size minContextSize, Size initBlockSize,
										   Size maxBlockSize);
extern MemoryContext MemoryContextSwitchTo(MemoryContext context);
extern void MemoryContextDelete(MemoryContext context);
#define ALLOCSET_DEFAULT_SIZES 0, 0, 0
extern bool ProcessConfigFileInternal(GucContext context, bool applySettings, int elevel);

/* guc_internal.h counterpart: vendored verbatim in pg_guc_file_io.c */
extern int	guc_name_compare(const char *namea, const char *nameb);

#endif							/* GUCFILE_SHIM_POSTGRES_H */
