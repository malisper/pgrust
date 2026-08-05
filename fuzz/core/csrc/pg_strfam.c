/*
 * Vendored PostgreSQL C: src/common string-family helpers — differential-fuzz
 * oracle for the p1-lanec campaign batch (common/string, common/archive,
 * common/percentrepl, common/relpath, common/wait_error).
 *
 * Provenance (all bodies VERBATIM unless a shim is listed below), from the
 * repo's vendored ground-truth checkout ../pgrust-fabled/vendor/postgres-src
 * @ 62d6c7d3df6287f1bd83199c1a746e50d31571a0 (REL_18 "Stamp 18.3"):
 *   - src/common/string.c: strtoint, pg_clean_ascii — verbatim (backend
 *     palloc_extended arm shimmed to malloc; alloc-failure arm untaken).
 *   - src/common/percentrepl.c: replace_percent_placeholders — verbatim
 *     backend arm (ereport shim below records errcode 3 and longjmps).
 *   - src/common/archive.c: BuildRestoreCommand — verbatim.
 *   - src/port/path.c make_native_path: Windows-only backslash rewrite; on
 *     POSIX its body is empty — shimmed to a no-op, exactly its POSIX build.
 *   - src/common/relpath.c: forkNames, GetDatabasePath, GetRelationPath —
 *     verbatim (RelPathStr/constants inlined from src/include/common/
 *     relpath.h; TABLESPACE_VERSION_DIRECTORY pinned to "PG_18_202506291",
 *     the same value crates/_support/types/types_storage exports).
 *   - src/common/wait_error.c: wait_result_to_str, wait_result_is_signal,
 *     wait_result_is_any_signal, wait_result_to_exit_code — verbatim
 *     (%m arm via the pg_snprintf-%m shim below; _() identity: message
 *     translation is out of scope).
 *   - src/port/pgstrsignal.c: pg_strsignal — verbatim (HAVE_STRSIGNAL arm;
 *     every supported host has strsignal(3)).
 *
 * Shims (plumbing only, never logic):
 *   - ereport(ERROR, errcode(X), ...) -> record X in pg_diff_errcode
 *     (3 = ERRCODE_INVALID_PARAMETER_VALUE, 22023 — the only errcode this
 *     family raises) and longjmp out through pg_strfam_jmp. errmsg/errdetail
 *     arguments are unevaluated (comparator checks the errcode plane only).
 *   - StringInfo (initStringInfo/appendStringInfoChar/appendStringInfoString)
 *     -> compact malloc-append buffer. Buffer growth is lib/stringinfo.c
 *     plumbing, not percentrepl.c logic; append semantics are identical.
 *   - palloc/palloc_extended/pstrdup/psprintf/pfree -> malloc family.
 *   - snprintf "%m" (pg's port/snprintf.c feature, absent from BSD libc) ->
 *     strerror(errno) via pg_strfam_snprintf; all other formats forward to
 *     the platform vsnprintf, which is what pg's snprintf emits for them.
 *
 * NOTE the strtoint/strtoul oracles' parse core is the platform
 * strtol/strtoul, exactly as in real PostgreSQL (which defers to libc);
 * on this host that is macOS libc. Ground-truthing vs glibc is the
 * postgres:18.3 Docker replay step.
 */

#include "postgres.h"

#include <ctype.h>
#include <errno.h>
#include <setjmp.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <sys/wait.h>

/* ---- shared error plane (defined in pg_float_io.c) ---- */
extern _Thread_local int pg_diff_errcode;
int pg_diff_errcode_get(void);

/* ---- non-local exit for ereport(ERROR) call sites in this family ---- */
_Thread_local jmp_buf pg_strfam_jmp;

#define ERRCODE_INVALID_PARAMETER_VALUE 3

#define ereport(level, ...) \
	do { pg_strfam_ereport_fire(); } while (0)
/* errcode()/errmsg()/errdetail() appear only inside ereport(...) argument
 * lists, which the macro above leaves unevaluated — but the preprocessor
 * still needs the ereport invocation to parse, so the whole argument list
 * is swallowed by the variadic macro. The errcode is recorded here: this
 * family's only ereport errcode is ERRCODE_INVALID_PARAMETER_VALUE. */
static void
pg_strfam_ereport_fire(void)
{
	pg_diff_errcode = ERRCODE_INVALID_PARAMETER_VALUE;
	longjmp(pg_strfam_jmp, 1);
}

#define _(x) (x)
#define pg_restrict restrict

/* ---- allocator shims ---- */
#define palloc_extended(sz, flags) malloc(sz)

/* Last pstrdup result: BuildRestoreCommand's nativePath is pstrdup'd before
 * replace_percent_placeholders can longjmp past its pfree — real PG reclaims
 * it via memory-context reset; the error exit frees it here (fleet LSan
 * caught the 1-byte leak at 146 execs). */
static _Thread_local char *pg_strfam_last_strdup;

static char *
pstrdup(const char *s)
{
	char	   *r = strdup(s);

	if (!r)
		abort();
	pg_strfam_last_strdup = r;
	return r;
}
#define pfree(p) free(p)

static char *
psprintf(const char *fmt, ...)
{
	char		buf[4096];
	va_list		ap;
	va_start(ap, fmt);
	vsnprintf(buf, sizeof(buf), fmt, ap);
	va_end(ap);
	return pstrdup(buf);
}

/* ---- pg snprintf %m shim (see header) ---- */
static int
pg_strfam_snprintf(char *str, size_t count, const char *fmt, ...)
{
	va_list		ap;
	int			r;

	if (strcmp(fmt, "%m") == 0)
	{
		/* pg's port/snprintf.c expands %m as strerror(errno) */
		r = (int) strlen(strerror(errno));
		strncpy(str, strerror(errno), count - 1);
		str[count - 1] = '\0';
		return r;
	}
	va_start(ap, fmt);
	r = vsnprintf(str, count, fmt, ap);
	va_end(ap);
	return r;
}
#define snprintf pg_strfam_snprintf

/* ---- StringInfo shim (plumbing; see header) ----
 * pg_strfam_pending tracks the one live buffer so the ereport/longjmp exit
 * can free it (real PG reclaims via memory-context reset; without this the
 * error arm leaks per-exec and the campaign OOMs — seen at 6.7M execs). */
typedef struct StringInfoData
{
	char	   *data;
	int			len;
	int			maxlen;
} StringInfoData;
typedef StringInfoData *StringInfo;

static _Thread_local char *pg_strfam_pending;

static void
pg_strfam_free_pending(void)
{
	free(pg_strfam_pending);
	pg_strfam_pending = NULL;
}

static void
initStringInfo(StringInfo str)
{
	str->maxlen = 1024;
	str->data = malloc(str->maxlen);
	if (!str->data)
		abort();
	str->len = 0;
	str->data[0] = '\0';
	pg_strfam_pending = str->data;
}

static void
appendBinaryStringInfo(StringInfo str, const char *data, int datalen)
{
	if (str->len + datalen + 1 > str->maxlen)
	{
		while (str->len + datalen + 1 > str->maxlen)
			str->maxlen *= 2;
		str->data = realloc(str->data, str->maxlen);
		if (!str->data)
			abort();
		pg_strfam_pending = str->data;
	}
	memcpy(str->data + str->len, data, datalen);
	str->len += datalen;
	str->data[str->len] = '\0';
}

static void
appendStringInfoChar(StringInfo str, char ch)
{
	appendBinaryStringInfo(str, &ch, 1);
}

static void
appendStringInfoString(StringInfo str, const char *s)
{
	appendBinaryStringInfo(str, s, (int) strlen(s));
}

/* ================================================================
 * src/common/string.c — VERBATIM
 * ================================================================ */

/*
 * strtoint --- just like strtol, but returns int not long
 */
int
strtoint(const char *pg_restrict str, char **pg_restrict endptr, int base)
{
	long		val;

	val = strtol(str, endptr, base);
	if (val != (int) val)
		errno = ERANGE;
	return (int) val;
}

/*
 * pg_clean_ascii -- Replace any non-ASCII chars with a "\xXX" string
 */
char *
pg_clean_ascii(const char *str, int alloc_flags)
{
	size_t		dstlen;
	char	   *dst;
	const char *p;
	size_t		i = 0;

	/* Worst case, each byte can become four bytes, plus a null terminator. */
	dstlen = strlen(str) * 4 + 1;

	dst = palloc_extended(dstlen, alloc_flags);

	if (!dst)
		return NULL;

	for (p = str; *p != '\0'; p++)
	{

		/* Only allow clean ASCII chars in the string */
		if (*p < 32 || *p > 126)
		{
			Assert(i < (dstlen - 3));
			snprintf(&dst[i], dstlen - i, "\\x%02x", (unsigned char) *p);
			i += 4;
		}
		else
		{
			Assert(i < dstlen);
			dst[i] = *p;
			i++;
		}
	}

	Assert(i < dstlen);
	dst[i] = '\0';
	return dst;
}

/* ================================================================
 * src/common/percentrepl.c — VERBATIM (backend arm)
 * ================================================================ */

char *
replace_percent_placeholders(const char *instr, const char *param_name, const char *letters,...)
{
	StringInfoData result;

	initStringInfo(&result);

	for (const char *sp = instr; *sp; sp++)
	{
		if (*sp == '%')
		{
			if (sp[1] == '%')
			{
				/* Convert %% to a single % */
				sp++;
				appendStringInfoChar(&result, *sp);
			}
			else if (sp[1] == '\0')
			{
				/* Incomplete escape sequence, expected a character afterward */
				ereport(ERROR,
						errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("invalid value for parameter \"%s\": \"%s\"", param_name, instr),
						errdetail("String ends unexpectedly after escape character \"%%\"."));
			}
			else
			{
				/* Look up placeholder character */
				bool		found = false;
				va_list		ap;

				sp++;

				va_start(ap, letters);
				for (const char *lp = letters; *lp; lp++)
				{
					char	   *val = va_arg(ap, char *);

					if (*sp == *lp)
					{
						if (val)
						{
							appendStringInfoString(&result, val);
							found = true;
						}
						/* If val is NULL, we will report an error. */
						break;
					}
				}
				va_end(ap);
				if (!found)
				{
					/* Unknown placeholder */
					ereport(ERROR,
							errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							errmsg("invalid value for parameter \"%s\": \"%s\"", param_name, instr),
							errdetail("String contains unexpected placeholder \"%%%c\".", *sp));
				}
			}
		}
		else
		{
			appendStringInfoChar(&result, *sp);
		}
	}

	return result.data;
}

/* ================================================================
 * src/common/archive.c — VERBATIM
 * (make_native_path: POSIX build is a no-op — see header)
 * ================================================================ */

static void
make_native_path(char *filename)
{
	/* #ifdef WIN32 backslash rewrite; empty on POSIX */
}

char *
BuildRestoreCommand(const char *restoreCommand,
					const char *xlogpath,
					const char *xlogfname,
					const char *lastRestartPointFname)
{
	char	   *nativePath = NULL;
	char	   *result;

	if (xlogpath)
	{
		nativePath = pstrdup(xlogpath);
		make_native_path(nativePath);
	}

	result = replace_percent_placeholders(restoreCommand, "restore_command", "frp",
										  xlogfname, lastRestartPointFname, nativePath);

	if (nativePath)
		pfree(nativePath);

	return result;
}

/* ================================================================
 * src/common/relpath.c — VERBATIM
 * (constants inlined from src/include/common/relpath.h @ same ref)
 * ================================================================ */

typedef uint32 Oid;
typedef Oid RelFileNumber;
typedef enum ForkNumber
{
	InvalidForkNumber = -1,
	MAIN_FORKNUM = 0,
	FSM_FORKNUM,
	VISIBILITYMAP_FORKNUM,
	INIT_FORKNUM,
} ForkNumber;
#define MAX_FORKNUM		INIT_FORKNUM
#define PG_TBLSPC_DIR "pg_tblspc"
#define TABLESPACE_VERSION_DIRECTORY "PG_18_202506291"
#define GLOBALTABLESPACE_OID 1664
#define DEFAULTTABLESPACE_OID 1663
#define INVALID_PROC_NUMBER (-1)
/* REL_PATH_STR_MAXLEN per relpath.h: generous fixed bound */
#define REL_PATH_STR_MAXLEN 128
typedef struct RelPathStr
{
	char		str[REL_PATH_STR_MAXLEN + 1];
} RelPathStr;

const char *const forkNames[] = {
	[MAIN_FORKNUM] = "main",
	[FSM_FORKNUM] = "fsm",
	[VISIBILITYMAP_FORKNUM] = "vm",
	[INIT_FORKNUM] = "init",
};

char *
GetDatabasePath(Oid dbOid, Oid spcOid)
{
	if (spcOid == GLOBALTABLESPACE_OID)
	{
		/* Shared system relations live in {datadir}/global */
		Assert(dbOid == 0);
		return pstrdup("global");
	}
	else if (spcOid == DEFAULTTABLESPACE_OID)
	{
		/* The default tablespace is {datadir}/base */
		return psprintf("base/%u", dbOid);
	}
	else
	{
		/* All other tablespaces are accessed via symlinks */
		return psprintf("%s/%u/%s/%u",
						PG_TBLSPC_DIR, spcOid,
						TABLESPACE_VERSION_DIRECTORY, dbOid);
	}
}

RelPathStr
GetRelationPath(Oid dbOid, Oid spcOid, RelFileNumber relNumber,
				int procNumber, ForkNumber forkNumber)
{
	RelPathStr	rp;

	if (spcOid == GLOBALTABLESPACE_OID)
	{
		/* Shared system relations live in {datadir}/global */
		Assert(dbOid == 0);
		Assert(procNumber == INVALID_PROC_NUMBER);
		if (forkNumber != MAIN_FORKNUM)
			sprintf(rp.str, "global/%u_%s",
					relNumber, forkNames[forkNumber]);
		else
			sprintf(rp.str, "global/%u",
					relNumber);
	}
	else if (spcOid == DEFAULTTABLESPACE_OID)
	{
		/* The default tablespace is {datadir}/base */
		if (procNumber == INVALID_PROC_NUMBER)
		{
			if (forkNumber != MAIN_FORKNUM)
			{
				sprintf(rp.str, "base/%u/%u_%s",
						dbOid, relNumber,
						forkNames[forkNumber]);
			}
			else
				sprintf(rp.str, "base/%u/%u",
						dbOid, relNumber);
		}
		else
		{
			if (forkNumber != MAIN_FORKNUM)
				sprintf(rp.str, "base/%u/t%d_%u_%s",
						dbOid, procNumber, relNumber,
						forkNames[forkNumber]);
			else
				sprintf(rp.str, "base/%u/t%d_%u",
						dbOid, procNumber, relNumber);
		}
	}
	else
	{
		/* All other tablespaces are accessed via symlinks */
		if (procNumber == INVALID_PROC_NUMBER)
		{
			if (forkNumber != MAIN_FORKNUM)
				sprintf(rp.str, "%s/%u/%s/%u/%u_%s",
						PG_TBLSPC_DIR, spcOid,
						TABLESPACE_VERSION_DIRECTORY,
						dbOid, relNumber,
						forkNames[forkNumber]);
			else
				sprintf(rp.str, "%s/%u/%s/%u/%u",
						PG_TBLSPC_DIR, spcOid,
						TABLESPACE_VERSION_DIRECTORY,
						dbOid, relNumber);
		}
		else
		{
			if (forkNumber != MAIN_FORKNUM)
				sprintf(rp.str, "%s/%u/%s/%u/t%d_%u_%s",
						PG_TBLSPC_DIR, spcOid,
						TABLESPACE_VERSION_DIRECTORY,
						dbOid, procNumber, relNumber,
						forkNames[forkNumber]);
			else
				sprintf(rp.str, "%s/%u/%s/%u/t%d_%u",
						PG_TBLSPC_DIR, spcOid,
						TABLESPACE_VERSION_DIRECTORY,
						dbOid, procNumber, relNumber);
		}
	}

	Assert(strnlen(rp.str, REL_PATH_STR_MAXLEN + 1) <= REL_PATH_STR_MAXLEN);

	return rp;
}

/* ================================================================
 * src/port/pgstrsignal.c — VERBATIM (HAVE_STRSIGNAL arm)
 * ================================================================ */

const char *
pg_strsignal(int signum)
{
	const char *result;

	/*
	 * If we have strsignal(3), use that --- but check its result for NULL.
	 */
	result = strsignal(signum);
	if (result == NULL)
		result = "unrecognized signal";

	return result;
}

/* ================================================================
 * src/common/wait_error.c — VERBATIM (POSIX arms)
 * ================================================================ */

char *
wait_result_to_str(int exitstatus)
{
	char		str[512];

	/*
	 * To simplify using this after pclose() and system(), handle status -1
	 * first.  In that case, there is no wait result but some error indicated
	 * by errno.
	 */
	if (exitstatus == -1)
	{
		snprintf(str, sizeof(str), "%m");
	}
	else if (WIFEXITED(exitstatus))
	{
		/*
		 * Give more specific error message for some common exit codes that
		 * have a special meaning in shells.
		 */
		switch (WEXITSTATUS(exitstatus))
		{
			case 126:
				snprintf(str, sizeof(str), _("command not executable"));
				break;

			case 127:
				snprintf(str, sizeof(str), _("command not found"));
				break;

			default:
				snprintf(str, sizeof(str),
						 _("child process exited with exit code %d"),
						 WEXITSTATUS(exitstatus));
		}
	}
	else if (WIFSIGNALED(exitstatus))
	{
		snprintf(str, sizeof(str),
				 _("child process was terminated by signal %d: %s"),
				 WTERMSIG(exitstatus), pg_strsignal(WTERMSIG(exitstatus)));
	}
	else
		snprintf(str, sizeof(str),
				 _("child process exited with unrecognized status %d"),
				 exitstatus);

	return pstrdup(str);
}

bool
wait_result_is_signal(int exit_status, int signum)
{
	if (WIFSIGNALED(exit_status) && WTERMSIG(exit_status) == signum)
		return true;
	if (WIFEXITED(exit_status) && WEXITSTATUS(exit_status) == 128 + signum)
		return true;
	return false;
}

bool
wait_result_is_any_signal(int exit_status, bool include_command_not_found)
{
	if (WIFSIGNALED(exit_status))
		return true;
	if (WIFEXITED(exit_status) &&
		WEXITSTATUS(exit_status) > (include_command_not_found ? 125 : 128))
		return true;
	return false;
}

int
wait_result_to_exit_code(int exit_status)
{
	if (exit_status == -1)
		return -1;				/* failure of pclose() or system() */
	if (WIFEXITED(exit_status))
		return WEXITSTATUS(exit_status);
	if (WIFSIGNALED(exit_status))
		return 128 + WTERMSIG(exit_status);
	/* On many systems, this is unreachable */
	return -1;
}

/* ================================================================
 * pg_diff_* entry wrappers (shim: FFI surface for the Rust drivers)
 * ================================================================ */

/*
 * strtoint through the callers' universal reject test
 * (endptr == str || *endptr != '\0' || errno != 0). Returns 1 = accepted
 * (with *out set), 0 = C would have rejected.
 */
int
pg_diff_strtoint10_strict(const char *s, int32_t *out)
{
	char	   *end;
	int			val;

	errno = 0;
	val = strtoint(s, &end, 10);
	if (end == s || *end != '\0' || errno != 0)
		return 0;
	*out = val;
	return 1;
}

/* strtoul(s, &end, 0): value + consumed + ERANGE observables */
uint64_t
pg_diff_strtoul_base0(const char *s, size_t *consumed, int *range_err)
{
	char	   *end;
	unsigned long val;

	errno = 0;
	val = strtoul(s, &end, 0);
	*consumed = (size_t) (end - s);
	*range_err = (errno == ERANGE);
	return (uint64_t) val;
}

/* caller frees */
char *
pg_diff_clean_ascii(const char *s)
{
	return pg_clean_ascii(s, 0);
}

/*
 * replace_percent_placeholders with the archive-style 3-slot spec.
 * Returns 0 + *out on success (caller frees); errcode (3) on ereport.
 */
int
pg_diff_percentrepl(const char *instr, const char *param_name,
					const char *letters, const char *v0, const char *v1,
					const char *v2, char **out)
{
	pg_diff_errcode = 0;
	if (setjmp(pg_strfam_jmp) != 0)
	{
		pg_strfam_free_pending();
		return pg_diff_errcode;
	}
	*out = replace_percent_placeholders(instr, param_name, letters, v0, v1, v2);
	pg_strfam_pending = NULL;	/* ownership to caller */
	return 0;
}

/* BuildRestoreCommand; same contract as pg_diff_percentrepl */
int
pg_diff_build_restore_command(const char *cmd, const char *xlogpath,
							  const char *xlogfname, const char *restartname,
							  char **out)
{
	pg_diff_errcode = 0;
	pg_strfam_last_strdup = NULL;
	if (setjmp(pg_strfam_jmp) != 0)
	{
		pg_strfam_free_pending();
		free(pg_strfam_last_strdup);	/* nativePath (see pstrdup shim) */
		pg_strfam_last_strdup = NULL;
		return pg_diff_errcode;
	}
	*out = BuildRestoreCommand(cmd, xlogpath, xlogfname, restartname);
	pg_strfam_pending = NULL;	/* ownership to caller */
	pg_strfam_last_strdup = NULL;	/* body already pfree'd nativePath */
	return 0;
}

/* caller frees */
char *
pg_diff_get_database_path(uint32_t dbOid, uint32_t spcOid)
{
	return GetDatabasePath(dbOid, spcOid);
}

/* out must hold REL_PATH_STR_MAXLEN + 1 bytes */
void
pg_diff_get_relation_path(uint32_t dbOid, uint32_t spcOid, uint32_t relNumber,
						  int procNumber, int forkNumber, char *out)
{
	RelPathStr	rp = GetRelationPath(dbOid, spcOid, relNumber, procNumber,
									 (ForkNumber) forkNumber);

	memcpy(out, rp.str, REL_PATH_STR_MAXLEN + 1);
}

/* caller frees */
char *
pg_diff_wait_result_to_str(int exitstatus, int errno_pin)
{
	errno = errno_pin;
	return wait_result_to_str(exitstatus);
}

int
pg_diff_wait_result_is_signal(int exit_status, int signum)
{
	return wait_result_is_signal(exit_status, signum) ? 1 : 0;
}

int
pg_diff_wait_result_is_any_signal(int exit_status, int include_cnf)
{
	return wait_result_is_any_signal(exit_status, include_cnf != 0) ? 1 : 0;
}

int
pg_diff_wait_result_to_exit_code(int exit_status)
{
	return wait_result_to_exit_code(exit_status);
}

const char *
pg_diff_pg_strsignal(int signum)
{
	return pg_strsignal(signum);
}

/* host W* macro observables (dual-exec the Rust libc wrappers against) */
int
pg_diff_wifexited(int status)
{
	return WIFEXITED(status) ? 1 : 0;
}

int
pg_diff_wexitstatus(int status)
{
	return WEXITSTATUS(status);
}

int
pg_diff_wifsignaled(int status)
{
	return WIFSIGNALED(status) ? 1 : 0;
}

int
pg_diff_wtermsig(int status)
{
	return WTERMSIG(status);
}

/* C-locale isspace observable (fuzz binary never calls setlocale) */
int
pg_diff_isspace(int c)
{
	return isspace((unsigned char) c) ? 1 : 0;
}
