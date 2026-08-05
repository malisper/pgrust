/*
 * pg_jsonpath_env.c — environment shims + fuzz-facing driver entries for the
 * jsonpath_diff oracle family. NOT PostgreSQL code (plumbing only, never
 * logic); the vendored computation lives in jsonpath.c, jsonpath_gram.c,
 * jsonpath_scan.c, regex/regcomp.c (+siblings), pg_numeric_min.c,
 * pg_formatting_min.c, pg_stringinfo.c, pg_support_min.c — all VERBATIM
 * from postgres-src @ 62d6c7d3df6287f1bd83199c1a746e50d31571a0 (18.3).
 *
 * Shims implemented here (each declared in include/postgres.h or the shim
 * headers):
 *   - TLS growable pointer arena behind palloc/palloc0/repalloc/pfree
 *     (models PG's per-query memory-context reset; every pg_diff_* entry
 *     resets it first so error longjmps cannot leak — the 2026-07-31 LSan
 *     incident class). palloc_extended(NO_OOM) for the regex engine.
 *   - ereport/errsave capture: TLS errcode (real MAKE_SQLSTATE value) +
 *     message/detail buffers; ERROR -> siglongjmp to the entry's setjmp;
 *     errsave against a live ErrorSaveContext sets error_occurred and
 *     returns (the real soft-error protocol).
 *   - psprintf/pvsnprintf over the arena (src/common/psprintf.c behavior,
 *     thin reimplementation — plumbing).
 *   - pg_newlocale_from_collation: pinned default collation with
 *     ctype_is_c = true (mirrors the Rust harness's
 *     set_default_locale_c_for_tests C_LOCALE pin; documented in
 *     include/utils/pg_locale.h).
 *   - exprType: reads the type oid off the driver's PgDiffVarExpr nodes
 *     (mirrors the shipped Rust vars model &[(name, Oid)]; documented in
 *     include/nodes/nodeFuncs.h).
 *
 * Driver entries (C ABI, called from fuzz/core/src/jsonpath_diff.rs):
 *   pg_diff_jsonpath_in / _out / _recv / _send / pg_diff_jsp_is_mutable.
 * Return 0 = ok, 1 = hard error, 2 = soft error; result pointers reference
 * the TLS arena and stay valid until the next pg_diff_* call on the thread.
 * All entries route through the VERBATIM fmgr wrappers (jsonpath_in etc.)
 * so the vendored fmgr bodies execute too.
 */

#include "postgres.h"

#include "fmgr.h"
#include "lib/stringinfo.h"
#include "nodes/miscnodes.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pg_list.h"
#include "nodes/value.h"
#include "utils/jsonpath.h"
#include "utils/pg_locale.h"

/* ---------------- TLS pointer arena ---------------- */

static _Thread_local void **pg_jsonpath_arena;
static _Thread_local size_t pg_jsonpath_arena_n;
static _Thread_local size_t pg_jsonpath_arena_cap;

static void
pg_jsonpath_arena_reset(void)
{
	size_t		i;

	for (i = 0; i < pg_jsonpath_arena_n; i++)
		free(pg_jsonpath_arena[i]);
	pg_jsonpath_arena_n = 0;
}

static void
pg_jsonpath_arena_track(void *p)
{
	if (pg_jsonpath_arena_n == pg_jsonpath_arena_cap)
	{
		size_t		newcap = pg_jsonpath_arena_cap ? pg_jsonpath_arena_cap * 2 : 256;
		void	  **na = realloc(pg_jsonpath_arena, newcap * sizeof(void *));

		if (na == NULL)
			abort();
		pg_jsonpath_arena = na;
		pg_jsonpath_arena_cap = newcap;
	}
	pg_jsonpath_arena[pg_jsonpath_arena_n++] = p;
}

/* exported for the jsonpathexec_diff entries (pg_jsonpath_exec_env.c) */
void
pg_jsonpath_arena_reset_public(void)
{
	pg_jsonpath_arena_reset();
}

void *
pg_jsonpath_palloc(Size size)
{
	void	   *p = malloc(size ? size : 1);

	if (p == NULL)
		abort();
	pg_jsonpath_arena_track(p);
	return p;
}

void *
pg_jsonpath_palloc0(Size size)
{
	void	   *p = calloc(1, size ? size : 1);

	if (p == NULL)
		abort();
	pg_jsonpath_arena_track(p);
	return p;
}

void *
pg_jsonpath_repalloc(void *ptr, Size size)
{
	size_t		i;

	if (ptr == NULL)
		return pg_jsonpath_palloc(size);
	for (i = pg_jsonpath_arena_n; i-- > 0;)
	{
		if (pg_jsonpath_arena[i] == ptr)
		{
			void	   *p = realloc(ptr, size ? size : 1);

			if (p == NULL)
				abort();
			pg_jsonpath_arena[i] = p;
			return p;
		}
	}
	/* repalloc of a pointer the arena never issued is a shim bug */
	abort();
}

void
pg_jsonpath_pfree(void *ptr)
{
	size_t		i;

	for (i = pg_jsonpath_arena_n; i-- > 0;)
	{
		if (pg_jsonpath_arena[i] == ptr)
		{
			free(ptr);
			pg_jsonpath_arena[i] = pg_jsonpath_arena[--pg_jsonpath_arena_n];
			return;
		}
	}
	/* pfree of a pointer the arena never issued is a shim bug */
	abort();
}

char *
pg_jsonpath_pstrdup(const char *in)
{
	size_t		n = strlen(in) + 1;
	char	   *p = pg_jsonpath_palloc(n);

	memcpy(p, in, n);
	return p;
}

/* palloc_extended(NO_OOM) for the regex engine (regcustom.h MALLOC) */
void *
pg_jsonpath_palloc_extended(Size size, int flags)
{
	void	   *p = malloc(size ? size : 1);

	if (p == NULL)
		return NULL;
	pg_jsonpath_arena_track(p);
	return p;
}

void *
pg_jsonpath_repalloc_extended(void *ptr, Size size, int flags)
{
	size_t		i;

	if (ptr == NULL)
		return pg_jsonpath_palloc_extended(size, flags);
	for (i = pg_jsonpath_arena_n; i-- > 0;)
	{
		if (pg_jsonpath_arena[i] == ptr)
		{
			void	   *p = realloc(ptr, size ? size : 1);

			if (p == NULL)
				return NULL;
			pg_jsonpath_arena[i] = p;
			return p;
		}
	}
	abort();
}

/* ---------------- psprintf / pvsnprintf (arena) ---------------- */

size_t
pvsnprintf(char *buf, size_t len, const char *fmt, va_list args)
{
	int			n = vsnprintf(buf, len, fmt, args);

	if (n < 0)
		abort();
	return (size_t) n;
}

char *
psprintf(const char *fmt, ...)
{
	va_list		args;
	int			n;
	char	   *buf;

	va_start(args, fmt);
	n = vsnprintf(NULL, 0, fmt, args);
	va_end(args);
	if (n < 0)
		abort();
	buf = pg_jsonpath_palloc((size_t) n + 1);
	va_start(args, fmt);
	vsnprintf(buf, (size_t) n + 1, fmt, args);
	va_end(args);
	return buf;
}

/* ---------------- error capture machinery ---------------- */

_Thread_local int pg_jsonpath_errcode;
_Thread_local char pg_jsonpath_errmsg_buf[1024];
_Thread_local char pg_jsonpath_errdetail_buf[1024];
_Thread_local sigjmp_buf pg_jsonpath_error_jmp;

int
errcode(int sqlerrcode)
{
	pg_jsonpath_errcode = sqlerrcode;
	return 0;
}

int
errmsg(const char *fmt, ...)
{
	va_list		args;

	va_start(args, fmt);
	vsnprintf(pg_jsonpath_errmsg_buf, sizeof(pg_jsonpath_errmsg_buf), fmt, args);
	va_end(args);
	return 0;
}

int
errmsg_internal(const char *fmt, ...)
{
	va_list		args;

	va_start(args, fmt);
	vsnprintf(pg_jsonpath_errmsg_buf, sizeof(pg_jsonpath_errmsg_buf), fmt, args);
	va_end(args);
	return 0;
}

int
errdetail(const char *fmt, ...)
{
	va_list		args;

	va_start(args, fmt);
	vsnprintf(pg_jsonpath_errdetail_buf, sizeof(pg_jsonpath_errdetail_buf), fmt, args);
	va_end(args);
	return 0;
}

int
errdetail_internal(const char *fmt, ...)
{
	va_list		args;

	va_start(args, fmt);
	vsnprintf(pg_jsonpath_errdetail_buf, sizeof(pg_jsonpath_errdetail_buf), fmt, args);
	va_end(args);
	return 0;
}

int
errhint(const char *fmt, ...)
{
	return 0;
}

void
pg_jsonpath_ereport_finish(int elevel)
{
	siglongjmp(pg_jsonpath_error_jmp, 1);
}

void
pg_jsonpath_errsave_finish(Node *escontext)
{
	if (escontext != NULL && IsA(escontext, ErrorSaveContext))
	{
		((ErrorSaveContext *) escontext)->error_occurred = true;
		return;
	}
	siglongjmp(pg_jsonpath_error_jmp, 1);
}

void
pg_jsonpath_elog(int elevel, const char *fmt, ...)
{
	va_list		args;

	va_start(args, fmt);
	vsnprintf(pg_jsonpath_errmsg_buf, sizeof(pg_jsonpath_errmsg_buf), fmt, args);
	va_end(args);
	if (elevel >= ERROR)
	{
		pg_jsonpath_errcode = ERRCODE_INTERNAL_ERROR;
		siglongjmp(pg_jsonpath_error_jmp, 1);
	}
}

/* ---------------- pinned locale + exprType models ---------------- */

static struct pg_locale_struct pg_jsonpath_default_locale = {
	.provider = COLLPROVIDER_LIBC,
	.deterministic = true,
	.collate_is_c = true,
	.ctype_is_c = true,
	.is_default = true,
};

pg_locale_t
pg_newlocale_from_collation(Oid collid)
{
	/* pinned model: only DEFAULT_COLLATION_OID reaches here (jsonpath's
	 * pg_regcomp call site passes it literally); C_COLLATION short-circuits
	 * inside regc_pg_locale.c before calling. */
	return &pg_jsonpath_default_locale;
}

Oid
exprType(const Node *expr)
{
	return ((const PgDiffVarExpr *) expr)->typeoid;
}

/* ---------------- driver entries ---------------- */

extern Datum jsonpath_in(FunctionCallInfo fcinfo);
extern Datum jsonpath_out(FunctionCallInfo fcinfo);
extern Datum jsonpath_recv(FunctionCallInfo fcinfo);
extern Datum jsonpath_send(FunctionCallInfo fcinfo);

const char *
pg_diff_jsonpath_last_msg(void)
{
	return pg_jsonpath_errmsg_buf;
}

const char *
pg_diff_jsonpath_last_detail(void)
{
	return pg_jsonpath_errdetail_buf;
}

static void
pg_diff_entry_reset(void)
{
	pg_jsonpath_arena_reset();
	pg_jsonpath_errcode = 0;
	pg_jsonpath_errmsg_buf[0] = '\0';
	pg_jsonpath_errdetail_buf[0] = '\0';
}

/*
 * jsonpath_in over the verbatim fmgr wrapper.
 * soft != 0: pass a live ErrorSaveContext as fcinfo->context.
 * Returns 0 ok / 1 hard error / 2 soft error; *sqlstate_out = captured
 * errcode (MAKE_SQLSTATE-encoded) on error.
 */
int
pg_diff_jsonpath_in(const char *str, size_t len, int soft,
					const unsigned char **image_out, size_t *image_len,
					int *sqlstate_out)
{
	ErrorSaveContext escontext = {.type = T_ErrorSaveContext};
	char	   *cstr;
	Datum		d;

	pg_diff_entry_reset();
	if (sigsetjmp(pg_jsonpath_error_jmp, 0) != 0)
	{
		*sqlstate_out = pg_jsonpath_errcode;
		return 1;
	}
	cstr = pg_jsonpath_palloc(len + 1);
	memcpy(cstr, str, len);
	cstr[len] = '\0';

	{
		LOCAL_FCINFO(fcinfo, 1);
		memset(fcinfo, 0, SizeForFunctionCallInfo(1));
		fcinfo->nargs = 1;
		fcinfo->context = soft ? (Node *) &escontext : NULL;
		fcinfo->args[0].value = CStringGetDatum(cstr);
		d = jsonpath_in(fcinfo);
		if (soft && escontext.error_occurred)
		{
			*sqlstate_out = pg_jsonpath_errcode;
			return 2;
		}
		if (fcinfo->isnull)
			abort();			/* non-soft NULL return: shim bug */
	}

	*image_out = (const unsigned char *) DatumGetPointer(d);
	*image_len = VARSIZE(DatumGetPointer(d));
	return 0;
}

/*
 * jsonpath_out over the verbatim fmgr wrapper; image must be a full 4B-header
 * varlena image (the driver feeds back exactly what pg_diff_jsonpath_in /
 * _recv produced, or the Rust side's image for cross-checks).
 */
int
pg_diff_jsonpath_out(const unsigned char *image, size_t image_len,
					 const char **text_out, size_t *text_len,
					 int *sqlstate_out)
{
	unsigned char *copy;
	Datum		d;

	pg_diff_entry_reset();
	if (sigsetjmp(pg_jsonpath_error_jmp, 0) != 0)
	{
		*sqlstate_out = pg_jsonpath_errcode;
		return 1;
	}
	copy = pg_jsonpath_palloc(image_len);
	memcpy(copy, image, image_len);

	{
		LOCAL_FCINFO(fcinfo, 1);
		memset(fcinfo, 0, SizeForFunctionCallInfo(1));
		fcinfo->nargs = 1;
		fcinfo->args[0].value = PointerGetDatum(copy);
		d = jsonpath_out(fcinfo);
	}

	*text_out = DatumGetCString(d);
	*text_len = strlen(DatumGetCString(d));
	return 0;
}

/*
 * jsonpath_recv over the verbatim fmgr wrapper; wire = the bytea payload
 * (version byte + text) exactly as a client would send it.
 */
int
pg_diff_jsonpath_recv(const unsigned char *wire, size_t wire_len,
					  const unsigned char **image_out, size_t *image_len,
					  int *sqlstate_out)
{
	StringInfoData buf;
	Datum		d;

	pg_diff_entry_reset();
	if (sigsetjmp(pg_jsonpath_error_jmp, 0) != 0)
	{
		*sqlstate_out = pg_jsonpath_errcode;
		return 1;
	}

	initStringInfo(&buf);
	appendBinaryStringInfo(&buf, (const char *) wire, (int) wire_len);
	buf.cursor = 0;

	{
		LOCAL_FCINFO(fcinfo, 1);
		memset(fcinfo, 0, SizeForFunctionCallInfo(1));
		fcinfo->nargs = 1;
		fcinfo->args[0].value = PointerGetDatum(&buf);
		d = jsonpath_recv(fcinfo);
	}

	*image_out = (const unsigned char *) DatumGetPointer(d);
	*image_len = VARSIZE(DatumGetPointer(d));
	return 0;
}

/*
 * jsonpath_send over the verbatim fmgr wrapper; returns the bytea payload
 * (headerless).
 */
int
pg_diff_jsonpath_send(const unsigned char *image, size_t image_len,
					  const unsigned char **wire_out, size_t *wire_len,
					  int *sqlstate_out)
{
	unsigned char *copy;
	Datum		d;
	bytea	   *b;

	pg_diff_entry_reset();
	if (sigsetjmp(pg_jsonpath_error_jmp, 0) != 0)
	{
		*sqlstate_out = pg_jsonpath_errcode;
		return 1;
	}
	copy = pg_jsonpath_palloc(image_len);
	memcpy(copy, image, image_len);

	{
		LOCAL_FCINFO(fcinfo, 1);
		memset(fcinfo, 0, SizeForFunctionCallInfo(1));
		fcinfo->nargs = 1;
		fcinfo->args[0].value = PointerGetDatum(copy);
		d = jsonpath_send(fcinfo);
	}

	b = (bytea *) DatumGetPointer(d);
	*wire_out = (const unsigned char *) VARDATA(b);
	*wire_len = VARSIZE(b) - VARHDRSZ;
	return 0;
}

/*
 * jspIsMutable over a parsed image, with the PASSING-variables model:
 * nvars parallel arrays of NUL-terminated names and type oids (mirrors the
 * shipped Rust signature jsp_is_mutable(image, vars: &[(&[u8], Oid)])).
 */
int
pg_diff_jsp_is_mutable(const unsigned char *image, size_t image_len,
					   int nvars, const char *const *varnames,
					   const unsigned int *vartypes,
					   int *mutable_out, int *sqlstate_out)
{
	unsigned char *copy;
	List	   *names = NIL;
	List	   *exprs = NIL;
	int			i;
	bool		result;

	pg_diff_entry_reset();
	if (sigsetjmp(pg_jsonpath_error_jmp, 0) != 0)
	{
		*sqlstate_out = pg_jsonpath_errcode;
		return 1;
	}
	copy = pg_jsonpath_palloc(image_len);
	memcpy(copy, image, image_len);

	for (i = 0; i < nvars; i++)
	{
		PgDiffVarExpr *ve = pg_jsonpath_palloc0(sizeof(PgDiffVarExpr));

		ve->typeoid = vartypes[i];
		names = lappend(names, makeString(pg_jsonpath_pstrdup(varnames[i])));
		exprs = lappend(exprs, ve);
	}

	result = jspIsMutable((JsonPath *) copy, names, exprs);
	*mutable_out = result ? 1 : 0;
	return 0;
}
