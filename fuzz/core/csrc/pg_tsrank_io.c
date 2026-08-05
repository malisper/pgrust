/*
 * pg_tsrank_io.c: driver entries + array-helper vendoring for the tsrank_diff
 * differential fuzz target (100%-coverage campaign; crate
 * crates/backend/utils/adt/tsrank).
 *
 * THE VENDORED ORACLE IS NOT IN THIS FILE: upstream tsrank.c lives
 * byte-identical (shasum 194490cc2f66e899814c7d2c70ed04cd9271b0b8, verified
 * against ../pgrust-fabled/vendor/postgres-src @
 * 62d6c7d3df6287f1bd83199c1a746e50d31571a0, PostgreSQL 18.3 Stamp-18.3) in
 * csrc/tsvec/tsrank.c, compiled against the same shim web as the
 * tsvector_core_diff oracle (csrc/tsvec/postgres.h — palloc arena,
 * ereport/longjmp, pg_qsort; see pg_tsvector_core_io.c header). No carves:
 * tsrank.c is pure math over TSVector/TSQuery images + TS_execute
 * (tsvector_op.c, already vendored).
 *
 * THIS FILE contains:
 *   - SECTION 1 (VERBATIM): ArrayGetNItems + ArrayGetNItemsSafe
 *     (src/backend/utils/adt/arrayutils.c) and array_contains_nulls
 *     (src/backend/utils/adt/arrayfuncs.c), the three array helpers
 *     tsrank.c's getWeights calls. ArrayType layout + ARR_* macros are
 *     verbatim in include/utils/array.h. The float4[] weights argument is a
 *     REAL array varlena image built by the Rust driver and handed to BOTH
 *     sides byte-identically (the Rust counterpart arg_weights reads the
 *     same image), so array handling here is the genuine upstream layout
 *     code, not the element-list plumbing shim the tsvector ops use.
 *   - SECTION 2: the pg_diff_ts_rank fuzz-facing driver entry, reusing the
 *     tsvec oracle's shared error/arena machinery (pg_tsvec_jmp,
 *     pg_tsvec_prep, pg_tsvec_mkvarlena — pg_tsvector_core_io.c).
 *
 * Errcode classes: csrc/tsvec/postgres.h (8 = 2202E array_subscript_error,
 * raised by getWeights; 3 = 22004; 5 = 22023; 2 = 54000).
 */

#include "tsvec/postgres.h"

#include <assert.h>
#include <setjmp.h>
#include <string.h>

#include "utils/array.h"
#include "utils/fmgrprotos.h"
#include "tsearch/ts_type.h"
#include "varatt.h"

/* Shared TLS errcode channel (defined in csrc/pg_float_io.c). */
extern _Thread_local int pg_diff_errcode;

/* shared tsvec oracle machinery (pg_tsvector_core_io.c) */
extern _Thread_local jmp_buf pg_tsvec_jmp;
extern void pg_tsvec_prep(void);
extern struct varlena *pg_tsvec_mkvarlena(const unsigned char *payload, int len);

/* the vendored SQL-callable wrappers (csrc/tsvec/tsrank.c) */
extern Datum ts_rank_wttf(FunctionCallInfo fcinfo);
extern Datum ts_rank_wtt(FunctionCallInfo fcinfo);
extern Datum ts_rank_ttf(FunctionCallInfo fcinfo);
extern Datum ts_rank_tt(FunctionCallInfo fcinfo);
extern Datum ts_rankcd_wttf(FunctionCallInfo fcinfo);
extern Datum ts_rankcd_wtt(FunctionCallInfo fcinfo);
extern Datum ts_rankcd_ttf(FunctionCallInfo fcinfo);
extern Datum ts_rankcd_tt(FunctionCallInfo fcinfo);

/* ==================== SECTION 1: array helpers (VERBATIM) ================ */

/*
 * VERBATIM from src/backend/utils/adt/arrayutils.c @ 62d6c7d3df (lines
 * 56-102), including comments.
 */
int
ArrayGetNItems(int ndim, const int *dims)
{
	return ArrayGetNItemsSafe(ndim, dims, NULL);
}

/*
 * This entry point can return the error into an ErrorSaveContext
 * instead of throwing an exception.  -1 is returned after an error.
 */
int
ArrayGetNItemsSafe(int ndim, const int *dims, struct Node *escontext)
{
	int32		ret;
	int			i;

	if (ndim <= 0)
		return 0;
	ret = 1;
	for (i = 0; i < ndim; i++)
	{
		int64		prod;

		/* A negative dimension implies that UB-LB overflowed ... */
		if (dims[i] < 0)
			ereturn(escontext, -1,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("array size exceeds the maximum allowed (%d)",
							(int) MaxArraySize)));

		prod = (int64) ret * (int64) dims[i];

		ret = (int32) prod;
		if ((int64) ret != prod)
			ereturn(escontext, -1,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("array size exceeds the maximum allowed (%d)",
							(int) MaxArraySize)));
	}
	Assert(ret >= 0);
	if ((Size) ret > MaxArraySize)
		ereturn(escontext, -1,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("array size exceeds the maximum allowed (%d)",
						(int) MaxArraySize)));
	return (int) ret;
}

/*
 * VERBATIM from src/backend/utils/adt/arrayfuncs.c @ 62d6c7d3df
 * (array_contains_nulls, lines 3772-3807), including comments.
 */
bool
array_contains_nulls(ArrayType *array)
{
	int			nelems;
	bits8	   *bitmap;
	int			bitmask;

	/* Easy answer if there's no null bitmap */
	if (!ARR_HASNULL(array))
		return false;

	nelems = ArrayGetNItems(ARR_NDIM(array), ARR_DIMS(array));

	bitmap = ARR_NULLBITMAP(array);

	/* check whole bytes of the bitmap byte-at-a-time */
	while (nelems >= 8)
	{
		if (*bitmap != 0xFF)
			return true;
		bitmap++;
		nelems -= 8;
	}

	/* check last partial byte */
	bitmask = 1;
	while (nelems > 0)
	{
		if ((*bitmap & bitmask) == 0)
			return true;
		bitmask <<= 1;
		nelems--;
	}

	return false;
}

/* ========== SECTION 2: fuzz-facing driver entry (NOT Postgres code) ====== */

/*
 * pg_diff_ts_rank: dispatch one of the eight SQL wrappers.
 *   variant 0..3 = ts_rank_{wttf,wtt,ttf,tt}; 4..7 = ts_rankcd_{...}.
 *   wpayload/wplen: float4[] array varlena PAYLOAD (after vl_len_) for the
 *     w* variants (ignored otherwise).
 *   vimg/vlen, qimg/qlen: tsvector / tsquery varlena payloads.
 *   method: int32 4th arg for the *ttf variants (ignored otherwise).
 * Returns 0 = ok (*res_bits = IEEE bits of the float4 result), 1 = the C
 * side threw (class in pg_diff_errcode).
 */
int
pg_diff_ts_rank(int variant,
				const unsigned char *wpayload, int wplen,
				const unsigned char *vimg, int vlen,
				const unsigned char *qimg, int qlen,
				int32 method, uint32 *res_bits)
{
	FunctionCallInfoBaseData fcinfo;
	Datum		d;
	float4		f;
	int			a = 0;

	pg_tsvec_prep();
	if (setjmp(pg_tsvec_jmp) != 0)
		return 1;

	memset(&fcinfo, 0, sizeof(fcinfo));
	if (variant % 4 <= 1)		/* w* forms: weights array first */
		fcinfo.args[a++].value =
			PointerGetDatum(pg_tsvec_mkvarlena(wpayload, wplen));
	fcinfo.args[a++].value = PointerGetDatum(pg_tsvec_mkvarlena(vimg, vlen));
	fcinfo.args[a++].value = PointerGetDatum(pg_tsvec_mkvarlena(qimg, qlen));
	if (variant % 4 == 0 || variant % 4 == 2)	/* *ttf forms: method */
		fcinfo.args[a++].value = Int32GetDatum(method);
	fcinfo.nargs = (short) a;

	switch (variant & 7)
	{
		case 0:
			d = ts_rank_wttf(&fcinfo);
			break;
		case 1:
			d = ts_rank_wtt(&fcinfo);
			break;
		case 2:
			d = ts_rank_ttf(&fcinfo);
			break;
		case 3:
			d = ts_rank_tt(&fcinfo);
			break;
		case 4:
			d = ts_rankcd_wttf(&fcinfo);
			break;
		case 5:
			d = ts_rankcd_wtt(&fcinfo);
			break;
		case 6:
			d = ts_rankcd_ttf(&fcinfo);
			break;
		default:
			d = ts_rankcd_tt(&fcinfo);
			break;
	}

	f = DatumGetFloat4(d);
	memcpy(res_bits, &f, sizeof(uint32));
	return 0;
}
