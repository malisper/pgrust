/*
 * pg_guc_file_io.c: vendored PostgreSQL C oracle for the guc_file_diff
 * differential fuzz target (100%-coverage campaign, lane p1-wavef).
 * Crate under test: crates/backend/utils/misc/guc_file (postgresql.conf
 * parser, phase-1 carve: ParseConfigFp over in-memory bytes, RESTRICTED
 * to include-directive-free inputs).
 *
 * Provenance (all PG bodies VERBATIM from the vendor tree at
 * ~/dev/pgrust-fabled/vendor/postgres-src, Stamp-18.3, upstream sha
 * 62d6c7d3df6287f1bd83199c1a746e50d31571a0):
 *   - csrc/gucfile/guc-file.l: byte-for-byte copy of
 *     src/backend/utils/misc/guc-file.l (whole-file verbatim vendoring —
 *     ParseConfigFp, ParseConfigFile, ParseConfigDirectory,
 *     DeescapeQuotedString, record_config_file_error, FreeConfigVariables,
 *     GUC_flex_fatal and the flex token rules all ride along).
 *   - csrc/gucfile/guc-file.c: generated from that copy by
 *     `flex -o guc-file.c guc-file.l` (Apple flex 2.6.4). PG itself ships
 *     no generated scanner; flex is the build tool of record upstream, so
 *     the .l file is the verbatim artifact and the .c is its committed
 *     build product (regenerate with any flex >= 2.6).
 *   - guc_name_compare: VERBATIM src/backend/utils/misc/guc.c (Stamp-18.3),
 *     pasted below.
 *   - ereport levels / sqlstates / ConfigVariable / CONF_FILE_* constants:
 *     verbatim values in csrc/gucfile/postgres.h (provenance notes there).
 *
 * Shims (plumbing only, never logic — see csrc/gucfile/postgres.h):
 *   - ereport/elog -> capture (elevel, packed sqlstate, formatted message)
 *     and siglongjmp to pg_gucf_run when elevel >= ERROR (PG's error
 *     longjmp). SqlState packing is bit-identical to pgrust's
 *     types_error::make_sqlstate, so the errcode plane is an i32 compare.
 *   - palloc/pstrdup/pfree and the flex skeleton's malloc/realloc/free ->
 *     per-iteration pointer arena reset at every pg_gucf_run entry
 *     (leak-free across the longjmp escape; the scaffold's TLS-arena
 *     precedent, sized for whole-file parses).
 *   - AbsoluteConfigLocation / AllocateFile / FreeFile / GetConfFilesInDir
 *     / psprintf / ProcessConfigFileInternal / memory-context calls:
 *     abort() stubs. All are reachable only through include-directive
 *     processing or ProcessConfigFile, which the census carve excludes
 *     (the Rust driver filters any input containing case-insensitive
 *     "include" before either side runs). A stub firing means the domain
 *     filter is broken — crash loudly, never fabricate a result.
 *
 * Driver entries (SECTION E, pg_gucf_ prefix) are fuzz plumbing, NOT
 * Postgres code:
 *   pg_gucf_run(buf, len, elevel) parses buf as a config file named
 *   "conf" at depth CONF_FILE_START_DEPTH, returns 0 (returned) or
 *   1 (ereport >= ERROR longjmp), and leaves the ConfigVariable list plus
 *   the captured report channel for the accessor functions.
 *   len == 0: ParseConfigFp's token loop exits on the immediate EOF with
 *   OK=true and an empty list; fmemopen(size 0) is not portable
 *   (EINVAL on macOS), so the empty result is synthesized to that
 *   inspection-verified fixed point and cross-checked by the "\n"-only
 *   and comment-only seeds which take the real path.
 */

#define _GNU_SOURCE 1  /* vasprintf (glibc); a no-op on macOS libc */
#define GUCF_DRIVER_TU 1
#include "postgres.h"
#include "utils/conffiles.h"

/* the scanner TU's export (renamed by the prefix defines in postgres.h) */
extern bool ParseConfigFp(FILE *fp, const char *config_file, int depth,
						  int elevel, ConfigVariable **head_p,
						  ConfigVariable **tail_p);

/* =====================================================================
 * SECTION A: verbatim PG bodies
 * ===================================================================== */

/*
 * the bare comparison function for guc names
 *
 * VERBATIM src/backend/utils/misc/guc.c (Stamp-18.3), guc_name_compare.
 */
int
guc_name_compare(const char *namea, const char *nameb)
{
	/*
	 * The temptation to use strcasecmp() here must be resisted, because the
	 * hash mapping has to remain stable across setlocale() calls. So, build
	 * our own with a simple ASCII-only downcasing.
	 */
	while (*namea && *nameb)
	{
		char		cha = *namea++;
		char		chb = *nameb++;

		if (cha >= 'A' && cha <= 'Z')
			cha += 'a' - 'A';
		if (chb >= 'A' && chb <= 'Z')
			chb += 'a' - 'A';
		if (cha != chb)
			return cha - chb;
	}
	if (*namea)
		return 1;				/* a is longer */
	if (*nameb)
		return -1;				/* b is longer */
	return 0;
}

/* =====================================================================
 * SECTION B: arena allocator (environment mock)
 * ===================================================================== */

#define GUCF_ARENA_MAX 65536

static _Thread_local void *gucf_arena[GUCF_ARENA_MAX];
static _Thread_local size_t gucf_arena_n;

static void
gucf_arena_reset(void)
{
	for (size_t i = 0; i < gucf_arena_n; i++)
		if (gucf_arena[i])
			free(gucf_arena[i]);
	gucf_arena_n = 0;
}

void *
gucf_arena_malloc(size_t size)
{
	void	   *p;

	if (gucf_arena_n >= GUCF_ARENA_MAX)
		abort();				/* arena census overflow — enlarge, never drop */
	p = malloc(size ? size : 1);
	if (!p)
		abort();
	gucf_arena[gucf_arena_n++] = p;
	return p;
}

void *
gucf_arena_realloc(void *ptr, size_t size)
{
	if (ptr == NULL)
		return gucf_arena_malloc(size);
	for (size_t i = gucf_arena_n; i-- > 0;)
	{
		if (gucf_arena[i] == ptr)
		{
			void	   *p = realloc(ptr, size ? size : 1);

			if (!p)
				abort();
			gucf_arena[i] = p;
			return p;
		}
	}
	abort();					/* realloc of a pointer the arena never saw */
}

void
gucf_arena_free(void *ptr)
{
	if (ptr == NULL)
		return;
	for (size_t i = gucf_arena_n; i-- > 0;)
	{
		if (gucf_arena[i] == ptr)
		{
			free(ptr);
			gucf_arena[i] = NULL;
			return;
		}
	}
	abort();					/* free of a pointer the arena never saw */
}

char *
gucf_arena_strdup(const char *s)
{
	size_t		len = strlen(s) + 1;
	char	   *p = gucf_arena_malloc(len);

	memcpy(p, s, len);
	return p;
}

/* =====================================================================
 * SECTION C: ereport capture (environment mock)
 * ===================================================================== */

static _Thread_local sigjmp_buf gucf_driver_jmp;
static _Thread_local int gucf_driver_jmp_armed;

static _Thread_local int gucf_pending_elevel;
static _Thread_local int gucf_pending_code;
/* Dynamic, never truncating: PG formats errmsg into an expanding StringInfo,
 * so a fixed capture buffer would clip long tokens and manufacture a false
 * message divergence (the fleet found exactly that at 3.2M execs). */
static _Thread_local char *gucf_pending_msg;

static _Thread_local int gucf_thrown;		/* 1 after an elevel >= ERROR report */
static _Thread_local int gucf_thrown_elevel;
static _Thread_local int gucf_thrown_code;
static _Thread_local char *gucf_thrown_msg;

/* count + last of the sub-ERROR reports (PG's log-only channel) */
static _Thread_local int gucf_logged_count;
static _Thread_local int gucf_logged_last_elevel;
static _Thread_local int gucf_logged_last_code;

void
gucf_ereport_begin(int elevel)
{
	gucf_pending_elevel = elevel;
	gucf_pending_code = ERRCODE_INTERNAL_ERROR; /* elog default; errcode() overrides */
	if (gucf_pending_msg)
	{
		free(gucf_pending_msg);
		gucf_pending_msg = NULL;
	}
}

int
gucf_errcode(int sqlerrcode)
{
	gucf_pending_code = sqlerrcode;
	return 0;
}

int
gucf_errmsg(const char *fmt, ...)
{
	va_list		ap;

	va_start(ap, fmt);
	if (vasprintf(&gucf_pending_msg, fmt, ap) < 0)
		abort();
	va_end(ap);
	return 0;
}

int
gucf_errcode_for_file_access(void)
{
	abort();					/* OUT-of-carve (AllocateFile failure path) */
}

void
gucf_ereport_finish(void)
{
	if (gucf_pending_elevel >= ERROR)
	{
		gucf_thrown = 1;
		gucf_thrown_elevel = gucf_pending_elevel;
		gucf_thrown_code = gucf_pending_code;
		free(gucf_thrown_msg);
		gucf_thrown_msg = gucf_pending_msg ? strdup(gucf_pending_msg) : strdup("");
		if (!gucf_thrown_msg)
			abort();
		if (!gucf_driver_jmp_armed)
			abort();
		siglongjmp(gucf_driver_jmp, 1);
	}
	gucf_logged_count++;
	gucf_logged_last_elevel = gucf_pending_elevel;
	gucf_logged_last_code = gucf_pending_code;
}

/* =====================================================================
 * SECTION D: OUT-of-carve abort stubs (see file header)
 * ===================================================================== */

bool		IsUnderPostmaster = false;
MemoryContext CurrentMemoryContext = NULL;

char *
AbsoluteConfigLocation(const char *location, const char *calling_file)
{
	abort();
}

FILE *
AllocateFile(const char *name, const char *mode)
{
	abort();
}

int
FreeFile(FILE *file)
{
	abort();
}

char **
GetConfFilesInDir(const char *includedir, const char *calling_file,
				  int elevel, int *num_filenames, char **err_msg)
{
	abort();
}

char *
psprintf(const char *fmt, ...)
{
	abort();
}

MemoryContext
AllocSetContextCreate(MemoryContext parent, const char *name,
					  Size minContextSize, Size initBlockSize,
					  Size maxBlockSize)
{
	abort();
}

MemoryContext
MemoryContextSwitchTo(MemoryContext context)
{
	abort();
}

void
MemoryContextDelete(MemoryContext context)
{
	abort();
}

bool
ProcessConfigFileInternal(GucContext context, bool applySettings, int elevel)
{
	abort();
}

/* =====================================================================
 * SECTION E: driver entry + accessors (fuzz plumbing, pg_gucf_ prefix)
 * ===================================================================== */

static _Thread_local ConfigVariable *gucf_head;
static _Thread_local int gucf_ok;
static _Thread_local int gucf_returned;

/* returns 0 = ParseConfigFp returned, 1 = error thrown (longjmp) */
int
pg_gucf_run(const unsigned char *buf, size_t len, int elevel)
{
	ConfigVariable *head = NULL;
	ConfigVariable *tail = NULL;
	FILE	   *fp;

	gucf_arena_reset();
	gucf_head = NULL;
	gucf_ok = 0;
	gucf_returned = 0;
	gucf_thrown = 0;
	gucf_thrown_elevel = 0;
	gucf_thrown_code = 0;
	free(gucf_thrown_msg);
	gucf_thrown_msg = NULL;
	gucf_logged_count = 0;
	gucf_logged_last_elevel = 0;
	gucf_logged_last_code = 0;

	if (len == 0)
	{
		/* see file header: yylex() returns 0 at once; OK=true, empty list */
		gucf_returned = 1;
		gucf_ok = 1;
		return 0;
	}

	fp = fmemopen((void *) buf, len, "r");
	if (!fp)
		abort();

	if (sigsetjmp(gucf_driver_jmp, 0) == 0)
	{
		gucf_driver_jmp_armed = 1;
		gucf_ok = ParseConfigFp(fp, "conf", CONF_FILE_START_DEPTH, elevel,
								&head, &tail) ? 1 : 0;
		gucf_returned = 1;
	}
	gucf_driver_jmp_armed = 0;
	fclose(fp);
	gucf_head = head;
	return gucf_returned ? 0 : 1;
}

int
pg_gucf_ok(void)
{
	return gucf_ok;
}

size_t
pg_gucf_item_count(void)
{
	size_t		n = 0;

	for (ConfigVariable *it = gucf_head; it; it = it->next)
		n++;
	return n;
}

static ConfigVariable *
gucf_item(size_t i)
{
	ConfigVariable *it = gucf_head;

	while (i-- > 0 && it)
		it = it->next;
	if (!it)
		abort();
	return it;
}

const char *
pg_gucf_item_name(size_t i)
{
	return gucf_item(i)->name;
}

const char *
pg_gucf_item_value(size_t i)
{
	return gucf_item(i)->value;
}

const char *
pg_gucf_item_errmsg(size_t i)
{
	return gucf_item(i)->errmsg;
}

const char *
pg_gucf_item_filename(size_t i)
{
	return gucf_item(i)->filename;
}

int
pg_gucf_item_sourceline(size_t i)
{
	return gucf_item(i)->sourceline;
}

int
pg_gucf_item_ignore(size_t i)
{
	return gucf_item(i)->ignore ? 1 : 0;
}

int
pg_gucf_item_applied(size_t i)
{
	return gucf_item(i)->applied ? 1 : 0;
}

int
pg_gucf_thrown_get_code(void)
{
	return gucf_thrown ? gucf_thrown_code : 0;
}

int
pg_gucf_thrown_get_elevel(void)
{
	return gucf_thrown ? gucf_thrown_elevel : 0;
}

const char *
pg_gucf_thrown_get_msg(void)
{
	return gucf_thrown_msg ? gucf_thrown_msg : "";
}

int
pg_gucf_logged_get_count(void)
{
	return gucf_logged_count;
}

int
pg_gucf_logged_get_last_code(void)
{
	return gucf_logged_last_code;
}

int
pg_gucf_logged_get_last_elevel(void)
{
	return gucf_logged_last_elevel;
}

/*
 * DeescapeQuotedString driven directly (sibling arm). `buf`/`len` are the raw
 * token bytes INCLUDING both quotes; C reads them as a NUL-terminated string,
 * so the copy below terminates them exactly as yytext is terminated.
 */
const char *
pg_gucf_deescape(const unsigned char *buf, size_t len)
{
	char	   *tok;

	gucf_arena_reset();
	tok = gucf_arena_malloc(len + 1);
	memcpy(tok, buf, len);
	tok[len] = '\0';
	return DeescapeQuotedString(tok);
}
