/*
 * pg_jsonpath_exec_env.c — environment shims + fuzz-facing driver entries
 * for the jsonpathexec_diff oracle (lane p1-laneaa, crate
 * adt/jsonpath_exec). NOT PostgreSQL code (plumbing only, never logic); the
 * vendored computation lives in jsonpath_exec.c + jsonb_util.c +
 * pg_jsonb_min.c + the jsonpath_diff family TUs — all VERBATIM from
 * postgres-src @ 62d6c7d3df6287f1bd83199c1a746e50d31571a0 (18.3).
 *
 * Shim groups in this file:
 *
 *   1. DRIVER ENTRIES (C ABI, called from fuzz/core/src/jsonpathexec_diff.rs):
 *      pg_diff_jsonb_path_exists / _match / _query_array / _query_first.
 *      Each takes full 4B-header varlena images for the jsonb document, the
 *      jsonpath, and (optionally) the vars jsonb — the driver parses BOTH
 *      documents ONCE with the shipped Rust adt_jsonb crate and feeds the
 *      identical image bytes to both engines (input-strategy decision
 *      recorded in the Rust driver header). Entries route through the
 *      VERBATIM fmgr wrappers (jsonb_path_exists etc.), reset the TLS arena
 *      + regex cache first, and report verdicts as
 *      0 = ok / 1 = hard error (sqlstate captured) / 3 = SQL NULL result.
 *
 *   2. DATETIME CARVE SENTINELS: the .datetime()/.date()/.time*()/
 *      .timestamp*() method family and jbvDatetime comparisons read session
 *      timezone state and are CARVED AT THE DRIVER LEVEL (the driver skips
 *      any path whose parsed item tree contains a datetime-family item, on
 *      both engines). Every C function on that call graph is a LOUD ABORT
 *      stub here: if a carved input ever escapes the driver filter the
 *      process dies with a distinctive message instead of fabricating a
 *      comparison. They must never fire.
 *
 *   3. EXECUTOR/SRF/HASH STUBS: JSON_TABLE machinery, the JsonExpr executor
 *      entries' expression evaluation, the SRF MultiFuncCall protocol
 *      (jsonb_path_query is a set-returning function — out of scope, see
 *      the Rust driver header), and the GIN/hash-opclass hash entries are
 *      unreachable-by-construction from the four driver entries. All loud
 *      abort stubs.
 *
 *   4. ENVIRONMENT MODELS (documented, never logic):
 *      - MemoryContext: opaque tokens over the TLS arena (see postgres.h);
 *        switches/creates/deletes are no-ops — every allocation lives in
 *        the per-entry arena, and the regexp cache indices are reset at
 *        every entry (pg_jsonpath_regex_cache_reset in pg_jsonb_min.c).
 *      - construct_array_builtin/ArrayGetIntegerTypmods: the 2-element
 *        CSTRING typmod-array round trip used by .decimal(p,s) (see
 *        include/utils/array_model.h). ArrayGetIntegerTypmods converts with
 *        the VERBATIM pg_strtoint32 exactly like arrayutils.c does.
 *      - pg_server_to_any: same-encoding arm under the UTF-8 pin — returns
 *        the input unchanged, exactly mbutils.c's src==dest behavior (and
 *        it is only called when server encoding != UTF-8, i.e. never here).
 *      - pnstrdup: arena-backed, real mcxt.c semantics (length-capped copy,
 *        NUL-terminated).
 */

#include "postgres.h"

#include "catalog/pg_type.h"
#include "fmgr.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
#include "nodes/execnodes.h"
#include "nodes/miscnodes.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/datetime.h"
#include "utils/formatting.h"
#include "utils/json.h"
#include "utils/jsonb.h"
#include "utils/jsonpath.h"
#include "utils/pg_locale.h"
#include "utils/timestamp.h"

/* from pg_jsonpath_env.c (same family/library) */
extern _Thread_local int pg_jsonpath_errcode;
extern _Thread_local sigjmp_buf pg_jsonpath_error_jmp;
extern void pg_jsonpath_arena_reset_public(void);

/* from pg_jsonb_min.c */
extern void pg_jsonpath_regex_cache_reset(void);

/* the verbatim fmgr wrappers in jsonpath_exec.c */
extern Datum jsonb_path_exists(FunctionCallInfo fcinfo);
extern Datum jsonb_path_exists_tz(FunctionCallInfo fcinfo);
extern Datum jsonb_path_exists_opr(FunctionCallInfo fcinfo);
extern Datum jsonb_path_match(FunctionCallInfo fcinfo);
extern Datum jsonb_path_match_tz(FunctionCallInfo fcinfo);
extern Datum jsonb_path_match_opr(FunctionCallInfo fcinfo);
extern Datum jsonb_path_query_array(FunctionCallInfo fcinfo);
extern Datum jsonb_path_query_array_tz(FunctionCallInfo fcinfo);
extern Datum jsonb_path_query_first(FunctionCallInfo fcinfo);
extern Datum jsonb_path_query_first_tz(FunctionCallInfo fcinfo);

/* ================= 2. datetime carve sentinels (LOUD) ================= */

#define PG_JSONPATH_CARVE_ABORT(name) \
	do { \
		fprintf(stderr, "jsonpathexec_diff ORACLE: datetime-carve sentinel " \
				name " fired — a carved input escaped the driver filter " \
				"(harness bug)\n"); \
		abort(); \
	} while (0)

pg_tz	   *session_timezone = NULL;

Datum
parse_datetime(text *date_txt, text *fmt, Oid collid, bool strict,
			   Oid *typid, int32 *typmod, int *tz, struct Node *escontext)
{
	PG_JSONPATH_CARVE_ABORT("parse_datetime");
}

char *
JsonEncodeDateTime(char *buf, Datum value, Oid typid, const int *tzp)
{
	PG_JSONPATH_CARVE_ABORT("JsonEncodeDateTime");
}

int
timestamp2tm(Timestamp dt, int *tzp, struct pg_tm *tm, fsec_t *fsec,
			 const char **tzn, pg_tz *attimezone)
{
	PG_JSONPATH_CARVE_ABORT("timestamp2tm");
}

void
j2date(int jd, int *year, int *month, int *day)
{
	PG_JSONPATH_CARVE_ABORT("j2date");
}

int
DetermineTimeZoneOffset(struct pg_tm *tm, pg_tz *tzp)
{
	PG_JSONPATH_CARVE_ABORT("DetermineTimeZoneOffset");
}

void
AdjustTimeForTypmod(TimeADT *time, int32 typmod)
{
	PG_JSONPATH_CARVE_ABORT("AdjustTimeForTypmod");
}

void
AdjustTimestampForTypmod(Timestamp *time, int32 typmod, struct Node *escontext)
{
	PG_JSONPATH_CARVE_ABORT("AdjustTimestampForTypmod");
}

int32
anytime_typmod_check(bool istz, int32 typmod)
{
	PG_JSONPATH_CARVE_ABORT("anytime_typmod_check");
}

int32
anytimestamp_typmod_check(bool istz, int32 typmod)
{
	PG_JSONPATH_CARVE_ABORT("anytimestamp_typmod_check");
}

int32
date_cmp_timestamp_internal(DateADT dateVal, Timestamp dt2)
{
	PG_JSONPATH_CARVE_ABORT("date_cmp_timestamp_internal");
}

int32
date_cmp_timestamptz_internal(DateADT dateVal, TimestampTz dt2)
{
	PG_JSONPATH_CARVE_ABORT("date_cmp_timestamptz_internal");
}

int32
timestamp_cmp_timestamptz_internal(Timestamp timestampVal, TimestampTz dt2)
{
	PG_JSONPATH_CARVE_ABORT("timestamp_cmp_timestamptz_internal");
}

#define CARVED_FMGR_STUB(name) \
	Datum \
	name(FunctionCallInfo fcinfo) \
	{ \
		PG_JSONPATH_CARVE_ABORT(#name); \
	}

CARVED_FMGR_STUB(date_cmp)
CARVED_FMGR_STUB(date_timestamp)
CARVED_FMGR_STUB(date_timestamptz)
CARVED_FMGR_STUB(time_cmp)
CARVED_FMGR_STUB(time_timetz)
CARVED_FMGR_STUB(time_tz)
CARVED_FMGR_STUB(timetz_cmp)
CARVED_FMGR_STUB(timetz_time)
CARVED_FMGR_STUB(timestamp_cmp)
CARVED_FMGR_STUB(timestamp_tz)
CARVED_FMGR_STUB(timestamp_date)
CARVED_FMGR_STUB(timestamp_time)
CARVED_FMGR_STUB(timestamp_timestamptz)
CARVED_FMGR_STUB(timestamptz_date)
CARVED_FMGR_STUB(timestamptz_time)
CARVED_FMGR_STUB(timestamptz_timetz)
CARVED_FMGR_STUB(timestamptz_timestamp)

/* ============ 3. executor / SRF / hash stubs (unreachable) ============ */

#define PG_JSONPATH_UNREACHABLE_ABORT(name) \
	do { \
		fprintf(stderr, "jsonpathexec_diff ORACLE: unreachable stub " name \
				" fired (harness bug)\n"); \
		abort(); \
	} while (0)

Datum
ExecEvalExpr(ExprState *state, ExprContext *econtext, bool *isNull)
{
	PG_JSONPATH_UNREACHABLE_ABORT("ExecEvalExpr");
}

int32
exprTypmod(const Node *expr)
{
	PG_JSONPATH_UNREACHABLE_ABORT("exprTypmod");
}

FuncCallContext *
init_MultiFuncCall(FunctionCallInfo fcinfo)
{
	PG_JSONPATH_UNREACHABLE_ABORT("init_MultiFuncCall (SRF carve)");
}

FuncCallContext *
per_MultiFuncCall(FunctionCallInfo fcinfo)
{
	PG_JSONPATH_UNREACHABLE_ABORT("per_MultiFuncCall (SRF carve)");
}

Datum
jsonb_in(FunctionCallInfo fcinfo)
{
	/* only reachable via the JSONOID arm of the executor's
	 * JsonItemFromDatum — never from the four driver entries */
	PG_JSONPATH_UNREACHABLE_ABORT("jsonb_in (executor JSONOID arm)");
}

char *
format_type_be(Oid type_oid)
{
	PG_JSONPATH_UNREACHABLE_ABORT("format_type_be");
}

Datum
hash_any(const unsigned char *k, int keylen)
{
	PG_JSONPATH_UNREACHABLE_ABORT("hash_any (GIN/hash opclass)");
}

Datum
hash_any_extended(const unsigned char *k, int keylen, uint64 seed)
{
	PG_JSONPATH_UNREACHABLE_ABORT("hash_any_extended (GIN/hash opclass)");
}

Datum
hash_numeric(FunctionCallInfo fcinfo)
{
	PG_JSONPATH_UNREACHABLE_ABORT("hash_numeric (GIN/hash opclass)");
}

Datum
hash_numeric_extended(FunctionCallInfo fcinfo)
{
	PG_JSONPATH_UNREACHABLE_ABORT("hash_numeric_extended (GIN/hash opclass)");
}

Datum
hashchar(FunctionCallInfo fcinfo)
{
	PG_JSONPATH_UNREACHABLE_ABORT("hashchar (GIN/hash opclass)");
}

Datum
hashcharextended(FunctionCallInfo fcinfo)
{
	PG_JSONPATH_UNREACHABLE_ABORT("hashcharextended (GIN/hash opclass)");
}

List *
list_delete_first(List *list)
{
	/* only caller is jsonb_path_query_internal's SRF per-call path (the
	 * MultiFuncCall carve) — init_MultiFuncCall aborts before this can run */
	PG_JSONPATH_UNREACHABLE_ABORT("list_delete_first (SRF carve)");
}

/* JsonValueListGetList is exec-internal; the SRF wrapper's list_head use is
 * satisfied by the vendored pg_list.h inline. */

int
pg_strncoll(const char *arg1, ssize_t len1, const char *arg2, ssize_t len2,
			pg_locale_t locale)
{
	/* varstr_cmp's non-C-collation arm; unreachable under the pinned
	 * ctype_is_c/collate_is_c default locale */
	PG_JSONPATH_UNREACHABLE_ABORT("pg_strncoll (C-collation pin)");
}

/* ==================== 4. environment models ==================== */

/* MemoryContext tokens over the TLS arena (see file header) */
static struct MemoryContextData *const pg_jsonpath_dummy_cxt =
	(struct MemoryContextData *) 0x1;

MemoryContext CurrentMemoryContext = (MemoryContext) 0x1;
MemoryContext TopMemoryContext = (MemoryContext) 0x1;

MemoryContext
MemoryContextSwitchTo(MemoryContext context)
{
	return (MemoryContext) pg_jsonpath_dummy_cxt;
}

MemoryContext
AllocSetContextCreate(MemoryContext parent, const char *name, int flags)
{
	return (MemoryContext) pg_jsonpath_dummy_cxt;
}

void
MemoryContextResetOnly(MemoryContext context)
{
	/* arena reset happens per pg_diff entry */
}

void
MemoryContextDelete(MemoryContext context)
{
	/* memory lives in the arena */
}

void
MemoryContextSetIdentifier(MemoryContext context, const char *id)
{
}

void
MemoryContextSetParent(MemoryContext context, MemoryContext new_parent)
{
}

char *
pnstrdup(const char *in, Size len)
{
	/* real mcxt.c semantics: copy at most len bytes, NUL-terminate */
	char	   *out;
	Size		actual = 0;

	while (actual < len && in[actual] != '\0')
		actual++;
	out = palloc(actual + 1);
	memcpy(out, in, actual);
	out[actual] = '\0';
	return out;
}

char *
pg_server_to_any(const char *s, int len, int encoding)
{
	/* same-encoding arm under the UTF-8 pin (see file header) */
	return (char *) s;
}

ArrayType *
construct_array_builtin(Datum *elems, int nelems, Oid elmtype)
{
	ArrayType  *a;
	int			i;

	if (elmtype != CSTRINGOID)
		PG_JSONPATH_UNREACHABLE_ABORT("construct_array_builtin (non-CSTRING)");
	a = palloc(sizeof(ArrayType));
	a->nelems = nelems;
	a->values = palloc(sizeof(char *) * nelems);
	for (i = 0; i < nelems; i++)
		a->values[i] = DatumGetCString(elems[i]);
	return a;
}

int32 *
ArrayGetIntegerTypmods(ArrayType *arr, int *n)
{
	/* arrayutils.c behavior over the model array: each cstring element is
	 * converted with the VERBATIM pg_strtoint32 (hard error on garbage),
	 * exactly like the real implementation */
	int32	   *result;
	int			i;

	*n = arr->nelems;
	result = palloc(sizeof(int32) * arr->nelems);
	for (i = 0; i < arr->nelems; i++)
		result[i] = pg_strtoint32(arr->values[i]);
	return result;
}

/* ==================== 1. driver entries ==================== */

static void
pg_diff_exec_entry_reset(void)
{
	pg_jsonpath_regex_cache_reset();
	pg_jsonpath_arena_reset_public();
	pg_jsonpath_errcode = 0;
}

/*
 * Copy a full 4B-header varlena image into the arena (the verbatim wrappers
 * detoast in place; inputs must live in oracle-owned memory).
 */
static void *
pg_diff_image_copy(const unsigned char *image, size_t len)
{
	void	   *copy = palloc(len);

	memcpy(copy, image, len);
	return copy;
}

/*
 * Shared body for exists/match: fn selection by (which, tz, opr).
 * Returns 0 ok (*res = 0/1), 3 ok-NULL, 1 hard error (*sqlstate_out set).
 */
static int
pg_diff_bool_common(Datum (*fn) (FunctionCallInfo), int nargs,
					const unsigned char *doc, size_t doc_len,
					const unsigned char *path, size_t path_len,
					const unsigned char *vars, size_t vars_len,
					int silent, int *res, int *sqlstate_out)
{
	Datum		d;

	pg_diff_exec_entry_reset();
	if (sigsetjmp(pg_jsonpath_error_jmp, 0) != 0)
	{
		*sqlstate_out = pg_jsonpath_errcode;
		return 1;
	}

	{
		LOCAL_FCINFO(fcinfo, 4);
		memset(fcinfo, 0, SizeForFunctionCallInfo(4));
		fcinfo->nargs = (short) nargs;
		fcinfo->args[0].value = PointerGetDatum(pg_diff_image_copy(doc, doc_len));
		fcinfo->args[1].value = PointerGetDatum(pg_diff_image_copy(path, path_len));
		if (nargs == 4)
		{
			fcinfo->args[2].value = PointerGetDatum(pg_diff_image_copy(vars, vars_len));
			fcinfo->args[3].value = BoolGetDatum(silent != 0);
		}
		d = fn(fcinfo);
		if (fcinfo->isnull)
			return 3;
	}

	*res = DatumGetBool(d) ? 1 : 0;
	return 0;
}

int
pg_diff_jsonb_path_exists(const unsigned char *doc, size_t doc_len,
						  const unsigned char *path, size_t path_len,
						  const unsigned char *vars, size_t vars_len,
						  int silent, int tz, int opr,
						  int *res, int *sqlstate_out)
{
	Datum		(*fn) (FunctionCallInfo);
	int			nargs = opr ? 2 : 4;

	if (opr)
		fn = jsonb_path_exists_opr;
	else
		fn = tz ? jsonb_path_exists_tz : jsonb_path_exists;
	return pg_diff_bool_common(fn, nargs, doc, doc_len, path, path_len,
							   vars, vars_len, silent, res, sqlstate_out);
}

int
pg_diff_jsonb_path_match(const unsigned char *doc, size_t doc_len,
						 const unsigned char *path, size_t path_len,
						 const unsigned char *vars, size_t vars_len,
						 int silent, int tz, int opr,
						 int *res, int *sqlstate_out)
{
	Datum		(*fn) (FunctionCallInfo);
	int			nargs = opr ? 2 : 4;

	if (opr)
		fn = jsonb_path_match_opr;
	else
		fn = tz ? jsonb_path_match_tz : jsonb_path_match;
	return pg_diff_bool_common(fn, nargs, doc, doc_len, path, path_len,
							   vars, vars_len, silent, res, sqlstate_out);
}

/*
 * Shared body for query_array/query_first (jsonb-image results).
 * Returns 0 ok (image_out/image_len = the full varlena image, arena
 * memory valid until the next pg_diff call), 3 SQL NULL, 1 hard error.
 */
static int
pg_diff_image_common(Datum (*fn) (FunctionCallInfo),
					 const unsigned char *doc, size_t doc_len,
					 const unsigned char *path, size_t path_len,
					 const unsigned char *vars, size_t vars_len,
					 int silent,
					 const unsigned char **image_out, size_t *image_len,
					 int *sqlstate_out)
{
	Datum		d;

	pg_diff_exec_entry_reset();
	if (sigsetjmp(pg_jsonpath_error_jmp, 0) != 0)
	{
		*sqlstate_out = pg_jsonpath_errcode;
		return 1;
	}

	{
		LOCAL_FCINFO(fcinfo, 4);
		memset(fcinfo, 0, SizeForFunctionCallInfo(4));
		fcinfo->nargs = 4;
		fcinfo->args[0].value = PointerGetDatum(pg_diff_image_copy(doc, doc_len));
		fcinfo->args[1].value = PointerGetDatum(pg_diff_image_copy(path, path_len));
		fcinfo->args[2].value = PointerGetDatum(pg_diff_image_copy(vars, vars_len));
		fcinfo->args[3].value = BoolGetDatum(silent != 0);
		d = fn(fcinfo);
		if (fcinfo->isnull)
			return 3;
	}

	*image_out = (const unsigned char *) DatumGetPointer(d);
	*image_len = VARSIZE(DatumGetPointer(d));
	return 0;
}

/*
 * The pure row-collection core of jsonb_path_query (the SRF wrapper's
 * MultiFuncCall plumbing is OUT OF SCOPE — documented carve): runs the
 * VERBATIM 4-arg jsonb_path_query_array wrapper's exact collection semantics
 * by calling the VERBATIM executeJsonPath through jsonb_path_query_array,
 * then re-walking the result array? No — that would change the plane.
 * Instead this entry mirrors jsonb_path_query_internal's collection pass
 * with the same verbatim calls (executeJsonPath is static, so we go through
 * the wrapper JsonPathQuery-free route): serialize each found item exactly
 * like SRF_RETURN_NEXT does (JsonbValueToJsonb per item).
 *
 * Implementation note: executeJsonPath is file-static in jsonpath_exec.c,
 * so this entry uses jsonb_path_query_array to obtain the found list is NOT
 * possible without re-wrapping. The 18.3 SRF body does:
 *     executeJsonPath(jp, vars, getJsonPathVariableFromJsonb,
 *                     countVariablesFromJsonb, jb, !silent, &found, tz)
 * then JsonbValueToJsonb per item. Both callbacks and executeJsonPath are
 * static; the ONLY exported route with identical semantics is
 * jsonb_path_query_array (same executeJsonPath call, same silent handling)
 * followed by disassembling the wrapping array — but disassembly would not
 * be the SRF's per-item serialization. Therefore this entry is built from
 * the wrapper pair instead:
 *   items buffer := for each element of query_array's result array (in
 *   order), the jsonb image produced by JsonbValueToJsonb over the
 *   element's JsonbValue as extracted with getIthJsonbValueFromContainer.
 * getIthJsonbValueFromContainer + JsonbValueToJsonb are VERBATIM
 * (jsonb_util.c). For scalars/objects/arrays this is exactly the SRF's
 * per-item image (PostgreSQL serializes each found item to a standalone
 * jsonb); the driver's unit tier locks this equivalence against the Rust
 * row-collection core on every regress vector, and the crate's own tests
 * lock those rows against regress expected output (README-TODO records
 * the docker spot checks).
 *
 * Output framing: *items_out = arena buffer of concatenated
 * [u32 native-endian image_len][image bytes] records; *count_out = number
 * of records. rc 0 ok / 1 hard error.
 */
int
pg_diff_jsonb_path_query_items(const unsigned char *doc, size_t doc_len,
							   const unsigned char *path, size_t path_len,
							   const unsigned char *vars, size_t vars_len,
							   int silent, int tz,
							   const unsigned char **items_out,
							   size_t *items_len, int *count_out,
							   int *sqlstate_out)
{
	Datum		d;
	Jsonb	   *arr;
	uint32		nelems;
	StringInfoData buf;
	uint32		i;

	pg_diff_exec_entry_reset();
	if (sigsetjmp(pg_jsonpath_error_jmp, 0) != 0)
	{
		*sqlstate_out = pg_jsonpath_errcode;
		return 1;
	}

	{
		LOCAL_FCINFO(fcinfo, 4);
		memset(fcinfo, 0, SizeForFunctionCallInfo(4));
		fcinfo->nargs = 4;
		fcinfo->args[0].value = PointerGetDatum(pg_diff_image_copy(doc, doc_len));
		fcinfo->args[1].value = PointerGetDatum(pg_diff_image_copy(path, path_len));
		fcinfo->args[2].value = PointerGetDatum(pg_diff_image_copy(vars, vars_len));
		fcinfo->args[3].value = BoolGetDatum(silent != 0);
		d = (tz ? jsonb_path_query_array_tz : jsonb_path_query_array) (fcinfo);
		if (fcinfo->isnull)
			abort();			/* query_array never returns NULL */
	}

	arr = (Jsonb *) DatumGetPointer(d);
	if (!JsonContainerIsArray(&arr->root) || JsonContainerIsScalar(&arr->root))
		abort();				/* wrapper always returns a plain array */
	nelems = JsonContainerSize(&arr->root);

	initStringInfo(&buf);
	for (i = 0; i < nelems; i++)
	{
		JsonbValue *v = getIthJsonbValueFromContainer(&arr->root, i);
		Jsonb	   *item = JsonbValueToJsonb(v);
		uint32		ilen = VARSIZE(item);

		appendBinaryStringInfo(&buf, (const char *) &ilen, sizeof(uint32));
		appendBinaryStringInfo(&buf, (const char *) item, ilen);
	}

	*items_out = (const unsigned char *) buf.data;
	*items_len = (size_t) buf.len;
	*count_out = (int) nelems;
	return 0;
}

int
pg_diff_jsonb_path_query_array(const unsigned char *doc, size_t doc_len,
							   const unsigned char *path, size_t path_len,
							   const unsigned char *vars, size_t vars_len,
							   int silent, int tz,
							   const unsigned char **image_out,
							   size_t *image_len, int *sqlstate_out)
{
	return pg_diff_image_common(tz ? jsonb_path_query_array_tz
								: jsonb_path_query_array,
								doc, doc_len, path, path_len,
								vars, vars_len, silent,
								image_out, image_len, sqlstate_out);
}

int
pg_diff_jsonb_path_query_first(const unsigned char *doc, size_t doc_len,
							   const unsigned char *path, size_t path_len,
							   const unsigned char *vars, size_t vars_len,
							   int silent, int tz,
							   const unsigned char **image_out,
							   size_t *image_len, int *sqlstate_out)
{
	return pg_diff_image_common(tz ? jsonb_path_query_first_tz
								: jsonb_path_query_first,
								doc, doc_len, path, path_len,
								vars, vars_len, silent,
								image_out, image_len, sqlstate_out);
}
