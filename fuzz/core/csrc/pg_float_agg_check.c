/*
 * Vendored PostgreSQL C: check_float8_array (float.c) — differential-fuzz
 * oracle for the float8[] transition-array shape check and the shipped
 * write_float8_transarray image writer (p1-lanead, 2026-07-31).
 *
 * Provenance (bodies VERBATIM unless a shim is listed below):
 *   - src/backend/utils/adt/float.c @ postgres-src
 *     62d6c7d3df6287f1bd83199c1a746e50d31571a0 (REL_18, stamp 18.3):
 *     check_float8_array — verbatim.
 *   - ArrayType struct head + ARR_* accessor macros: the same shim set the
 *     proofs oracle uses (proofs/float-agg/c/pg_float_agg.c) — the exact
 *     src/include/utils/array.h definitions for the fields the body reads.
 *
 * Shims (plumbing only, never logic):
 *   - elog(ERROR, ...) -> record class 7 in this TU's thread-local +
 *     longjmp (same non-returning control flow as PG's error longjmp).
 *   - Datum surface: the driver passes the raw varlena image pointer; in
 *     PG the varlena is always at least VARSIZE bytes — the driver only
 *     feeds images of >= expected size (shorter images are asserted on
 *     the Rust side alone; see diff.rs arm docs).
 */

#include "postgres.h"

#include <setjmp.h>
#include <stdint.h>
#include <string.h>

typedef int32_t int32;
typedef uint32_t Oid;

static _Thread_local int pg_diff_aggchk_err;
static _Thread_local jmp_buf pg_diff_aggchk_jmp;

#define elog(level, ...) \
	do { pg_diff_aggchk_err = 7; longjmp(pg_diff_aggchk_jmp, 1); } while (0)

/* ---- ArrayType shims (src/include/utils/array.h field layout) ---- */

#define FLOAT8OID 701

typedef struct ArrayType
{
	int32		vl_len_;
	int			ndim;
	int32		dataoffset;
	Oid			elemtype;
} ArrayType;

#define TYPEALIGN(ALIGNVAL,LEN) \
	(((uintptr_t) (LEN) + ((ALIGNVAL) - 1)) & ~((uintptr_t) ((ALIGNVAL) - 1)))
#define MAXALIGN(LEN) TYPEALIGN(8, (LEN))

#define ARR_NDIM(a) ((a)->ndim)
#define ARR_HASNULL(a) ((a)->dataoffset != 0)
#define ARR_ELEMTYPE(a) ((a)->elemtype)
#define ARR_DIMS(a) ((int *) (((char *) (a)) + sizeof(ArrayType)))
#define ARR_OVERHEAD_NONULLS(ndims) \
	MAXALIGN(sizeof(ArrayType) + 2 * sizeof(int) * (ndims))
#define ARR_DATA_OFFSET(a) \
	(ARR_HASNULL(a) ? (a)->dataoffset : ARR_OVERHEAD_NONULLS(ARR_NDIM(a)))
#define ARR_DATA_PTR(a) (((char *) (a)) + ARR_DATA_OFFSET(a))

/* ---- float.c check_float8_array, body VERBATIM ---- */

static float8 *
check_float8_array(ArrayType *transarray, const char *caller, int n)
{
	/*
	 * We expect the input to be an N-element float array; verify that. We
	 * don't need to use deconstruct_array() since the array data is just
	 * going to look like a C array of N float8 values.
	 */
	if (ARR_NDIM(transarray) != 1 ||
		ARR_DIMS(transarray)[0] != n ||
		ARR_HASNULL(transarray) ||
		ARR_ELEMTYPE(transarray) != FLOAT8OID)
		elog(ERROR, "%s: expected %d-element float8 array", caller, n);
	return (float8 *) ARR_DATA_PTR(transarray);
}

/* ---- fuzz-facing entry point (driver, NOT Postgres code) ---- */

/*
 * Run the vendored check on an image of at least 24 + 8n bytes.  Returns 0
 * and writes n doubles to out on success; 7 if the vendored body raised.
 */
int
pg_diff_check_float8_array(const char *image, int n, double *out)
{
	ArrayType  *a;
	float8	   *vals;
	int			i;

	/* align the image as the executor's palloc'd varlena would be */
	static _Thread_local char abuf[128];

	memcpy(abuf, image, 24 + 8 * (size_t) n);
	a = (ArrayType *) abuf;
	pg_diff_aggchk_err = 0;
	if (setjmp(pg_diff_aggchk_jmp))
		return pg_diff_aggchk_err;
	vals = check_float8_array(a, "fuzz", n);
	for (i = 0; i < n; i++)
		out[i] = vals[i];
	return 0;
}
