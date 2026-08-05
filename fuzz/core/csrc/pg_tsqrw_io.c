/*
 * pg_tsqrw_io.c: vendored PostgreSQL C oracle for the tsqrw_diff
 * differential fuzz target (100%-coverage campaign; crate
 * crates/backend/utils/adt/tsquery_rewrite). p1-lanef vendored the C
 * (csrc/tsqrw/tsquery_rewrite.c + tsquery_util.c + upstream
 * ts_type.h/ts_utils.h, all byte-verified against
 * ../pgrust-fabled/vendor/postgres-src @
 * 62d6c7d3df6287f1bd83199c1a746e50d31571a0, PostgreSQL 18.3); p1-laneaf
 * completed the compile environment. The scaffold #error gates were
 * removed together with the verbatim vendor compiling as its own
 * translation units (see fuzz/README-TODO-tsqrw_diff.md, Lane-F oracle
 * plan).
 *
 * Shim environment: SHARED with the tsq family (csrc/tsq/shim/ — see
 * csrc/tsq/shim/postgres.h for the full inventory). tsquery_rewrite.c's
 * SPI-dependent half (tsquery_rewrite_query, oid 3685) compiles against
 * LINK-ONLY aborting SPI stubs and is never called: that function is the
 * lane's documented NAMED CARVE (SPI = executor state; the same boundary
 * the C file's #include "executor/spi.h" marks).
 *
 * IMAGE PLANE (lanef's plan, kept): tsquery values cross as varlena
 * images with the 4-byte header ZEROED (the Rust convention); inputs get
 * a well-formed header stamped on the arena copy, outputs are re-zeroed.
 * The C PARSER is deliberately NOT part of this target — images are
 * built Rust-side, so the text-search configuration/GUC cache stays out
 * of scope.
 *
 * Entry return protocol: 0 = ok; 1 = hard error (ereport captured via
 * longjmp; class in pg_diff_errcode).
 */

#include "postgres.h"
#include "varatt.h"
#include "fmgr.h"
#include "utils/fmgrprotos.h"
#include "tsearch/ts_utils.h"

#include <string.h>
#include "pg_oracle_guard.h"	/* oracle-serialization holder check */

/* Shared TLS errcode channel (defined in csrc/pg_float_io.c). */
extern _Thread_local int pg_diff_errcode;

/* arena copy of a zero-header image, stamping a valid 4B varlena header */
static TSQuery
tsqrw_from_image(const unsigned char *img, int len)
{
	TSQuery		q = (TSQuery) palloc(len);

	memcpy(q, img, len);
	SET_VARSIZE(q, len);
	return q;
}

/*
 * tsquery_rewrite core (oid 3684): ts_rewrite(query, ex, subs).
 * Output image written with the 4-byte header zeroed (see header).
 */
int
pg_diff_tsquery_rewrite(const unsigned char *img_query, int len_query,
						const unsigned char *img_ex, int len_ex,
						const unsigned char *img_subs, int len_subs,
						unsigned char *out, int out_cap, int *out_len)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	FunctionCallInfoBaseData fcdata;
	struct varlena *res;
	int			sz;

	pg_tsq_arena_reset();
	pg_diff_errcode = 0;
	*out_len = 0;
	if (setjmp(pg_tsq_error_jmp))
		return 1;

	memset(&fcdata, 0, sizeof(fcdata));
	fcdata.nargs = 3;
	fcdata.args[0].value = PointerGetDatum(tsqrw_from_image(img_query, len_query));
	fcdata.args[1].value = PointerGetDatum(tsqrw_from_image(img_ex, len_ex));
	fcdata.args[2].value = PointerGetDatum(tsqrw_from_image(img_subs, len_subs));
	res = (struct varlena *) DatumGetPointer(tsquery_rewrite(&fcdata));

	sz = (int) VARSIZE(res);
	if (sz > out_cap)
		abort();				/* driver caps inputs; loud, never silent */
	memcpy(out, res, sz);
	memset(out, 0, 4);
	*out_len = sz;
	return 0;
}
