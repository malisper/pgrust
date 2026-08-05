/*
 * pg_jsonbops.c: vendored PostgreSQL C oracle EXTENSION for the
 * jsonbops_diff differential fuzz target (100%-coverage campaign; crate
 * crates/backend/utils/adt/jsonb) — the two-doc ops/mutate/getfield family
 * on top of the jsonbio_diff family TU (csrc/pg_jsonbio_io.c). Same shim
 * environment (csrc/jsonbfam/shim), same symbol isolation (jbfam_ prefix),
 * same error-class contract.
 *
 * Provenance: PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df), via
 * ../pgrust-fabled/vendor/postgres-src. Vendored text pulled in here:
 *   jsonbfam/arrayfuncs_c.inc    arrayfuncs.c deconstruct_array(+builtin),
 *                                array_contains_nulls; arrayutils.c
 *                                ArrayGetNItems(+Safe)
 *   jsonbfam/string_c.inc        src/common/string.c strtoint
 *   jsonbfam/jsonb_object_c.inc  jsonb.c jsonb_object + jsonb_object_two_arg
 *   jsonbfam/jsonfuncs_ops_c.inc jsonfuncs.c getfield/extract_path/mutate
 *                                family (incl. setPath*, IteratorConcat)
 * Compiled as separate family TUs (registered in build.rs):
 *   jsonbfam/jsonb_op.c          whole file (exists/contains/cmp/hash ops)
 *   jsonbfam/hashfn.c            src/common/hashfn.c whole file (with the
 *                                REAL common/hashfn.h replacing the abort
 *                                stub decls the io lane used)
 * Also vendored into the io TU (shared statics live there):
 *   jsonbfam/numeric_cmp_c.inc   numeric.c numeric_cmp/eq, cmp_* statics,
 *                                hash_numeric(+extended)
 *   jsonbfam/varlena_cmp_c.inc   varlena.c varstr_cmp + check_collation_set
 *                                over the shim pg_locale.h C-collation pin
 *
 * ENVIRONMENT PINS added by this TU (documented non-computation):
 *   - database collation C (shim/utils/pg_locale.h): every collation this
 *     family sees is DEFAULT_COLLATION_OID from compareJsonbScalarValue;
 *     Rust mirror = pg_locale_seams::varstr_cmp_locale := varstrfastcmp_c.
 *   - text[] arguments arrive as complete flat ArrayType images built by
 *     the Rust driver (both sides read the SAME bytes; PG_GETARG_ARRAYTYPE_P
 *     detoast is a passthrough for 4B-header images).
 *
 * Error-class contract (shim/postgres.h): as jsonbio_diff plus
 *   10 = 2202E array subscript error, 11 = 22004 null value not allowed.
 *   100+class = doc-a parse failed inside an entry; 200+class = doc-b /
 *   newval parse failed. The Rust driver only calls after ITS OWN parses
 *   succeeded, so 100+/200+ returns assert as divergences. -1 = SQL NULL.
 */

#include <assert.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <setjmp.h>

#include "postgres.h"			/* jsonbfam/shim/postgres.h */
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "utils/jsonb.h"
#include "utils/jsonfuncs.h"
#include "utils/fmgrprotos.h"
#include "catalog/pg_type.h"
#include "common/int.h"
#include "utils/array.h"
#include "access/tupmacs.h"
#include "common/hashfn.h"

/* shared driver plumbing exported by pg_jsonbio_io.c */
extern void pg_jsonbfam_arena_reset(void);

/* ==================== vendored PostgreSQL text ==================== */

#include "jsonbfam/string_c.inc"
#include "jsonbfam/arrayfuncs_c.inc"
#include "jsonbfam/jsonb_object_c.inc"
#include "jsonbfam/jsonfuncs_ops_c.inc"

/* ========== fuzz-facing driver entries (NOT Postgres code) ========== */

#define JBOPS_ENTER() \
	do { \
		pg_jsonbfam_arena_reset(); \
		pg_diff_errcode = 0; \
		if (setjmp(pg_jsonbfam_jmp) != 0) \
			return pg_diff_errcode ? pg_diff_errcode : ERRCODE_INTERNAL_ERROR; \
	} while (0)

static Datum
jbops_call(PGFunction fn, int nargs, const Datum *args, bool *isnull)
{
	LOCAL_FCINFO(fcinfo, 4);
	Datum		d;
	int			i;

	InitFunctionCallInfoData(*fcinfo, NULL, nargs, InvalidOid, NULL, NULL);
	for (i = 0; i < nargs; i++)
	{
		fcinfo->args[i].value = args[i];
		fcinfo->args[i].isnull = false;
	}
	d = fn(fcinfo);
	*isnull = fcinfo->isnull;
	return d;
}

/* parse one doc; on failure longjmp is intercepted and base+class returned
 * through *rc (0 on success). */
static Datum
jbops_parse(const char *str, int base, int *rc)
{
	jmp_buf		saved;
	Datum		jb = (Datum) 0;
	bool		isnull;

	memcpy(&saved, &pg_jsonbfam_jmp, sizeof(jmp_buf));
	if (setjmp(pg_jsonbfam_jmp) != 0)
	{
		memcpy(&pg_jsonbfam_jmp, &saved, sizeof(jmp_buf));
		*rc = base + (pg_diff_errcode ? pg_diff_errcode : ERRCODE_INTERNAL_ERROR);
		return (Datum) 0;
	}
	jb = jbops_call(jsonb_in, 1, (Datum[]) {CStringGetDatum(str)}, &isnull);
	memcpy(&pg_jsonbfam_jmp, &saved, sizeof(jmp_buf));
	*rc = 0;
	return jb;
}

static int
jbops_copyout(const void *src, int len, unsigned char *buf, int cap,
			  int *outlen)
{
	if (len > cap)
	{
		*outlen = len;
		return -2;
	}
	memcpy(buf, src, len);
	*outlen = len;
	return 0;
}

/* jsonb result -> payload bytes (root container, header stripped) */
static int
jbops_out_jsonb(Datum d, unsigned char *buf, int cap, int *outlen)
{
	struct varlena *v = (struct varlena *) DatumGetPointer(d);

	return jbops_copyout(VARDATA(v), VARSIZE(v) - VARHDRSZ, buf, cap, outlen);
}

/* text result -> payload bytes */
static int
jbops_out_text(Datum d, unsigned char *buf, int cap, int *outlen)
{
	return jbops_copyout(VARDATA_ANY(DatumGetPointer(d)),
						 VARSIZE_ANY_EXHDR(DatumGetPointer(d)),
						 buf, cap, outlen);
}

static text *
jbops_text(const unsigned char *p, int len)
{
	return cstring_to_text_with_len((const char *) p, len);
}

/* cmp + eq in one entry: out[0..3] = int32 cmp (LE by memcpy), out[4] = eq */
int
pg_diff_jbops_cmp(const char *a, const char *b, int32_t *cmp_out,
				  uint8_t *eq_out)
{
	Datum		da,
				db,
				d;
	bool		isnull;
	int			rc;

	JBOPS_ENTER();
	da = jbops_parse(a, 100, &rc);
	if (rc)
		return rc;
	db = jbops_parse(b, 200, &rc);
	if (rc)
		return rc;
	d = jbops_call(jsonb_cmp, 2, (Datum[]) {da, db}, &isnull);
	*cmp_out = DatumGetInt32(d);
	d = jbops_call(jsonb_eq, 2, (Datum[]) {da, db}, &isnull);
	*eq_out = DatumGetBool(d) ? 1 : 0;
	return 0;
}

/* which: 0 contains(a,b), 1 contained(a,b) */
int
pg_diff_jbops_contains(int which, const char *a, const char *b, uint8_t *out)
{
	Datum		da,
				db,
				d;
	bool		isnull;
	int			rc;

	JBOPS_ENTER();
	da = jbops_parse(a, 100, &rc);
	if (rc)
		return rc;
	db = jbops_parse(b, 200, &rc);
	if (rc)
		return rc;
	d = jbops_call(which ? jsonb_contained : jsonb_contains, 2,
				   (Datum[]) {da, db}, &isnull);
	*out = DatumGetBool(d) ? 1 : 0;
	return 0;
}

int
pg_diff_jbops_exists(const char *a, const unsigned char *key, int keylen,
					 uint8_t *out)
{
	Datum		da,
				d;
	bool		isnull;
	int			rc;

	JBOPS_ENTER();
	da = jbops_parse(a, 100, &rc);
	if (rc)
		return rc;
	d = jbops_call(jsonb_exists, 2,
				   (Datum[]) {da, PointerGetDatum(jbops_text(key, keylen))},
				   &isnull);
	*out = DatumGetBool(d) ? 1 : 0;
	return 0;
}

/* all: 0 = exists_any, 1 = exists_all; arr = flat text[] image */
int
pg_diff_jbops_exists_arr(int all, const char *a, const unsigned char *arr,
						 uint8_t *out)
{
	Datum		da,
				d;
	bool		isnull;
	int			rc;

	JBOPS_ENTER();
	da = jbops_parse(a, 100, &rc);
	if (rc)
		return rc;
	d = jbops_call(all ? jsonb_exists_all : jsonb_exists_any, 2,
				   (Datum[]) {da, PointerGetDatum(arr)}, &isnull);
	*out = DatumGetBool(d) ? 1 : 0;
	return 0;
}

/* ext: 0 = jsonb_hash (int32 result, sign-extended), 1 = jsonb_hash_extended */
int
pg_diff_jbops_hash(int ext, const char *a, int64_t seed, int64_t *out)
{
	Datum		da,
				d;
	bool		isnull;
	int			rc;

	JBOPS_ENTER();
	da = jbops_parse(a, 100, &rc);
	if (rc)
		return rc;
	if (ext)
	{
		d = jbops_call(jsonb_hash_extended, 2,
					   (Datum[]) {da, Int64GetDatum(seed)}, &isnull);
		*out = DatumGetInt64(d);
	}
	else
	{
		d = jbops_call(jsonb_hash, 1, (Datum[]) {da}, &isnull);
		*out = (int64_t) DatumGetInt32(d);
	}
	return 0;
}

/* which: 0 object_field (jsonb out), 1 object_field_text (text out),
 * 2 array_element (jsonb out), 3 array_element_text (text out). -1 = NULL. */
int
pg_diff_jbops_getfield(int which, const char *a, const unsigned char *key,
					   int keylen, int32_t idx,
					   unsigned char *out, int outcap, int *outlen)
{
	static const PGFunction fns[] = {
		jsonb_object_field, jsonb_object_field_text,
		jsonb_array_element, jsonb_array_element_text,
	};
	Datum		da,
				arg1,
				d;
	bool		isnull;
	int			rc;

	JBOPS_ENTER();
	da = jbops_parse(a, 100, &rc);
	if (rc)
		return rc;
	arg1 = (which < 2) ? PointerGetDatum(jbops_text(key, keylen))
		: Int32GetDatum(idx);
	d = jbops_call(fns[which], 2, (Datum[]) {da, arg1}, &isnull);
	if (isnull)
		return -1;
	return (which % 2 == 0) ? jbops_out_jsonb(d, out, outcap, outlen)
		: jbops_out_text(d, out, outcap, outlen);
}

/* which: 0 extract_path (jsonb), 1 extract_path_text (text),
 * 2 delete_path (jsonb), 3 set (jsonb; flag = create_missing),
 * 4 insert (jsonb; flag = insert_after). newval used by 3/4. -1 = NULL. */
int
pg_diff_jbops_path(int which, const char *a, const unsigned char *arr,
				   const char *newval, int flag,
				   unsigned char *out, int outcap, int *outlen)
{
	Datum		da,
				dn = (Datum) 0,
				d;
	bool		isnull;
	int			rc;

	JBOPS_ENTER();
	da = jbops_parse(a, 100, &rc);
	if (rc)
		return rc;
	if (which == 3 || which == 4)
	{
		dn = jbops_parse(newval, 200, &rc);
		if (rc)
			return rc;
	}
	switch (which)
	{
		case 0:
			d = jbops_call(jsonb_extract_path, 2,
						   (Datum[]) {da, PointerGetDatum(arr)}, &isnull);
			break;
		case 1:
			d = jbops_call(jsonb_extract_path_text, 2,
						   (Datum[]) {da, PointerGetDatum(arr)}, &isnull);
			break;
		case 2:
			d = jbops_call(jsonb_delete_path, 2,
						   (Datum[]) {da, PointerGetDatum(arr)}, &isnull);
			break;
		case 3:
			d = jbops_call(jsonb_set, 4,
						   (Datum[]) {da, PointerGetDatum(arr), dn,
									  BoolGetDatum(flag != 0)}, &isnull);
			break;
		default:
			d = jbops_call(jsonb_insert, 4,
						   (Datum[]) {da, PointerGetDatum(arr), dn,
									  BoolGetDatum(flag != 0)}, &isnull);
			break;
	}
	if (isnull)
		return -1;
	return (which == 1) ? jbops_out_text(d, out, outcap, outlen)
		: jbops_out_jsonb(d, out, outcap, outlen);
}

/* which: 0 delete key, 1 delete idx, 2 delete text[] */
int
pg_diff_jbops_delete(int which, const char *a, const unsigned char *key,
					 int keylen, int32_t idx, const unsigned char *arr,
					 unsigned char *out, int outcap, int *outlen)
{
	Datum		da,
				arg1,
				d;
	PGFunction	fn;
	bool		isnull;
	int			rc;

	JBOPS_ENTER();
	da = jbops_parse(a, 100, &rc);
	if (rc)
		return rc;
	switch (which)
	{
		case 0:
			fn = jsonb_delete;
			arg1 = PointerGetDatum(jbops_text(key, keylen));
			break;
		case 1:
			fn = jsonb_delete_idx;
			arg1 = Int32GetDatum(idx);
			break;
		default:
			fn = jsonb_delete_array;
			arg1 = PointerGetDatum(arr);
			break;
	}
	d = jbops_call(fn, 2, (Datum[]) {da, arg1}, &isnull);
	if (isnull)
		return -1;
	return jbops_out_jsonb(d, out, outcap, outlen);
}

int
pg_diff_jbops_concat(const char *a, const char *b,
					 unsigned char *out, int outcap, int *outlen)
{
	Datum		da,
				db,
				d;
	bool		isnull;
	int			rc;

	JBOPS_ENTER();
	da = jbops_parse(a, 100, &rc);
	if (rc)
		return rc;
	db = jbops_parse(b, 200, &rc);
	if (rc)
		return rc;
	d = jbops_call(jsonb_concat, 2, (Datum[]) {da, db}, &isnull);
	if (isnull)
		return -1;
	return jbops_out_jsonb(d, out, outcap, outlen);
}

/* two: 0 = jsonb_object(text[]), 1 = jsonb_object(text[], text[]) */
int
pg_diff_jbops_object(int two, const unsigned char *arr1,
					 const unsigned char *arr2,
					 unsigned char *out, int outcap, int *outlen)
{
	Datum		d;
	bool		isnull;

	JBOPS_ENTER();
	if (two)
		d = jbops_call(jsonb_object_two_arg, 2,
					   (Datum[]) {PointerGetDatum(arr1),
								  PointerGetDatum(arr2)}, &isnull);
	else
		d = jbops_call(jsonb_object, 1,
					   (Datum[]) {PointerGetDatum(arr1)}, &isnull);
	if (isnull)
		return -1;
	return jbops_out_jsonb(d, out, outcap, outlen);
}
