/*
 * pg_tsquery_core_io.c: vendored PostgreSQL C oracle for the
 * tsquery_core_diff differential fuzz target (100%-coverage campaign;
 * crate crates/backend/utils/adt/tsquery_core).
 *
 * VENDOR LAYOUT (replaces the scaffold's paste-in-place sections; every
 * scaffold #error gate was removed together with its verbatim vendor):
 * the upstream files compile as their own translation units under
 * csrc/tsq/, byte-for-byte copies from postgres-src
 * @ 62d6c7d3df6287f1bd83199c1a746e50d31571a0 (PostgreSQL 18.3, Stamp-18.3,
 * verified against ../pgrust-fabled/vendor/postgres-src):
 *   - csrc/tsq/tsquery.c           (tsqueryin/parse_tsquery/gettoken_query_*,
 *                                   pushval_asis, tsqueryout/infix,
 *                                   tsquerysend, tsqueryrecv, tsquerytree)
 *   - csrc/tsq/tsquery_op.c        (tsquery_and/or/phrase[_distance]/not,
 *                                   CompareTSQ + cmp family, tsq_mcontains)
 *   - csrc/tsq/tsquery_cleanup.c   (clean_NOT, cleanup_tsquery_stopwords)
 *   - csrc/tsq/tsvector_parser.c   (gettoken_tsvector operand tokenizer)
 *   - csrc/tsq/ts_locale_excerpt.c (t_is* excerpt; carve documented there)
 *   - csrc/tsqrw/tsquery_util.c    (QTNode machinery; vendored by p1-lanef,
 *                                   compiled ONCE for both this oracle and
 *                                   pg_tsqrw_io.c)
 * Shim environment (plumbing only, inventory + justifications in
 * csrc/tsq/shim/postgres.h and siblings): bump-arena palloc, ereport ->
 * pg_diff_errcode class + longjmp, errsave/ereturn soft-error protocol
 * with a real ErrorSaveContext, NOTICE counter, List shim, pqformat wire
 * shims, UTF-8-pinned pg_mblen family, mbstowcs-backed char2wchar,
 * no-op check_stack_depth (driver caps input length — documented seam).
 *
 * IMAGE PLANE CONVENTION (all entries below): tsquery/text/bytea values
 * cross this boundary as raw byte images. INPUT images arrive as
 * (payload-with-zeroed-4-byte-header, total_len) — matching the Rust
 * side's zeroed vl_len_ (copy_image in adt/tsquery_core/src/builtins.rs);
 * each entry copies them into the arena and stamps a well-formed 4B
 * varlena header before handing them to the vendored C (which reads
 * VARSIZE in CompareTSQ). OUTPUT images are written back with the 4-byte
 * header ZEROED again, so the Rust driver compares full buffers
 * byte-for-byte in the Rust convention.
 *
 * Errcode classes: see csrc/tsq/shim/postgres.h (PG_DIFF_ERR_*).
 * Entry return protocol: 0 = ok; 1 = hard error (longjmp caught);
 * 2 = soft error recorded in the ErrorSaveContext (soft mode only).
 * pg_diff_errcode carries the class in both error cases.
 */

#include "postgres.h"
#include "nodes/nodes.h"
#include "nodes/miscnodes.h"
#include "varatt.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "utils/fmgrprotos.h"
#include "tsearch/ts_utils.h"

#include <string.h>
#include "pg_oracle_guard.h"	/* oracle-serialization holder check */

/* shared TLS errcode channel (defined in csrc/pg_float_io.c) */
extern _Thread_local int pg_diff_errcode;

/* ---- local helpers (driver plumbing, not Postgres code) ---- */

/* arena copy of a zero-header image, stamping a valid 4B varlena header */
static TSQuery
tsq_from_image(const unsigned char *img, int len)
{
	TSQuery		q = (TSQuery) palloc(len);

	memcpy(q, img, len);
	SET_VARSIZE(q, len);
	return q;
}

/* copy a varlena out with the 4-byte header zeroed; abort on cap overflow */
static int
image_out(const struct varlena *v, unsigned char *out, int out_cap)
{
	int			sz = (int) VARSIZE(v);

	if (sz > out_cap)
		abort();				/* driver caps inputs; loud, never silent */
	memcpy(out, v, sz);
	memset(out, 0, 4);
	return sz;
}

/*
 * pushval_asis is static in tsquery.c; this is the same one-line body
 * (driver plumbing) so parse_tsquery can be driven with the web/plain
 * tokenizer flags that tsqueryin does not expose.
 */
static void
pg_tsq_pushval_asis(Datum opaque, TSQueryParserState state, char *strval,
					int lenval, int16 weight, bool prefix)
{
	(void) opaque;
	pushValue(state, strval, lenval, weight, prefix);
}

/* ==================== driver entries ==================== */

/*
 * tsqueryin core (oid 3612) + the P_TSQ_PLAIN / P_TSQ_WEB tokenizers the
 * to_tsquery family routes through the same parse_tsquery entry.
 * flags: 0 = standard, 1 = P_TSQ_PLAIN, 2 = P_TSQ_WEB (ts_utils.h values).
 * soft_mode != 0 wires a real ErrorSaveContext (tsqueryin's
 * fcinfo->context path). out_notices reports the NOTICE count
 * (empty-query / stopword-cleanup notices — parse_tsquery "noisy" arm
 * fires only when escontext is NULL, exactly as upstream).
 */
int
pg_diff_tsquery_in(const char *input, int flags, int soft_mode,
				   unsigned char *out, int out_cap, int *out_len,
				   int *out_notices)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	ErrorSaveContext escontext = {T_ErrorSaveContext};
	TSQuery		q;

	pg_tsq_arena_reset();
	pg_diff_errcode = 0;
	*out_len = 0;
	*out_notices = 0;
	if (setjmp(pg_tsq_error_jmp))
		return 1;

	q = parse_tsquery((char *) input, pg_tsq_pushval_asis,
					  (Datum) 0, flags,
					  soft_mode ? (Node *) &escontext : NULL);

	*out_notices = pg_tsq_notice_count;
	if (soft_mode && escontext.error_occurred)
		return 2;
	*out_len = image_out((struct varlena *) q, out, out_cap);
	return 0;
}

/* tsqueryout core (oid 3613): image -> infix text (NUL-terminated) */
int
pg_diff_tsquery_out(const unsigned char *img, int len,
					char *out, int out_cap, int *out_len)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	FunctionCallInfoBaseData fcdata;
	char	   *res;
	int			n;

	pg_tsq_arena_reset();
	pg_diff_errcode = 0;
	*out_len = 0;
	if (setjmp(pg_tsq_error_jmp))
		return 1;

	memset(&fcdata, 0, sizeof(fcdata));
	fcdata.nargs = 1;
	fcdata.args[0].value = PointerGetDatum(tsq_from_image(img, len));
	res = DatumGetCString(tsqueryout(&fcdata));
	n = (int) strlen(res);
	if (n + 1 > out_cap)
		abort();
	memcpy(out, res, n + 1);
	*out_len = n;
	return 0;
}

/* tsquerysend core (oid 3640): image -> wire bytes (bytea payload) */
int
pg_diff_tsquery_send(const unsigned char *img, int len,
					 unsigned char *out, int out_cap, int *out_len)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	FunctionCallInfoBaseData fcdata;
	bytea	   *res;
	int			n;

	pg_tsq_arena_reset();
	pg_diff_errcode = 0;
	*out_len = 0;
	if (setjmp(pg_tsq_error_jmp))
		return 1;

	memset(&fcdata, 0, sizeof(fcdata));
	fcdata.nargs = 1;
	fcdata.args[0].value = PointerGetDatum(tsq_from_image(img, len));
	res = (bytea *) DatumGetPointer(tsquerysend(&fcdata));
	n = (int) VARSIZE(res) - VARHDRSZ;
	if (n > out_cap)
		abort();
	memcpy(out, VARDATA(res), n);
	*out_len = n;
	return 0;
}

/*
 * tsqueryrecv core (oid 3641): wire bytes -> image. out_consumed reports
 * the message-cursor position so the driver can model the protocol
 * layer's trailing-junk check (pq_getmsgend lives in the CALLER upstream,
 * not in tsqueryrecv).
 */
int
pg_diff_tsquery_recv(const unsigned char *wire, int wire_len,
					 unsigned char *out, int out_cap, int *out_len,
					 int *out_consumed)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	FunctionCallInfoBaseData fcdata;
	StringInfoData msg;
	TSQuery		q;

	pg_tsq_arena_reset();
	pg_diff_errcode = 0;
	*out_len = 0;
	*out_consumed = 0;
	if (setjmp(pg_tsq_error_jmp))
		return 1;

	msg.data = (char *) wire;
	msg.len = wire_len;
	msg.maxlen = wire_len;
	msg.cursor = 0;

	memset(&fcdata, 0, sizeof(fcdata));
	fcdata.nargs = 1;
	fcdata.args[0].value = PointerGetDatum(&msg);
	q = (TSQuery) DatumGetPointer(tsqueryrecv(&fcdata));
	*out_consumed = msg.cursor;
	*out_len = image_out((struct varlena *) q, out, out_cap);
	return 0;
}

/*
 * tsquery_and/or/phrase/phrase_distance/not cores (oids 3669/3670/5003/
 * 5004/3671). op: 0 = AND, 1 = OR, 2 = PHRASE (distance arg used),
 * 3 = NOT (img_b/len_b ignored). PHRASE goes through
 * tsquery_phrase_distance so the distance-range ereport (22023) is on
 * the plane; the driver reproduces tsquery_phrase(a,b) as op 2 with
 * distance 1, exactly what the upstream wrapper does.
 */
int
pg_diff_tsquery_binop(int op, const unsigned char *img_a, int len_a,
					  const unsigned char *img_b, int len_b,
					  int distance,
					  unsigned char *out, int out_cap, int *out_len)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	FunctionCallInfoBaseData fcdata;
	Datum		res;

	pg_tsq_arena_reset();
	pg_diff_errcode = 0;
	*out_len = 0;
	if (setjmp(pg_tsq_error_jmp))
		return 1;

	memset(&fcdata, 0, sizeof(fcdata));
	fcdata.args[0].value = PointerGetDatum(tsq_from_image(img_a, len_a));
	switch (op)
	{
		case 0:
		case 1:
			fcdata.nargs = 2;
			fcdata.args[1].value = PointerGetDatum(tsq_from_image(img_b, len_b));
			res = (op == 0) ? tsquery_and(&fcdata) : tsquery_or(&fcdata);
			break;
		case 2:
			fcdata.nargs = 3;
			fcdata.args[1].value = PointerGetDatum(tsq_from_image(img_b, len_b));
			fcdata.args[2].value = Int32GetDatum(distance);
			res = tsquery_phrase_distance(&fcdata);
			break;
		case 3:
			fcdata.nargs = 1;
			res = tsquery_not(&fcdata);
			break;
		default:
			abort();
	}
	*out_len = image_out((struct varlena *) DatumGetPointer(res), out, out_cap);
	return 0;
}

/*
 * tsquery_cmp core (oid 3668; the six boolean comparisons 3662-3667 are
 * pure sign tests over this value on both sides).
 */
int
pg_diff_tsquery_cmp(const unsigned char *img_a, int len_a,
					const unsigned char *img_b, int len_b,
					int *out_cmp)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	FunctionCallInfoBaseData fcdata;

	pg_tsq_arena_reset();
	pg_diff_errcode = 0;
	*out_cmp = 0;
	if (setjmp(pg_tsq_error_jmp))
		return 1;

	memset(&fcdata, 0, sizeof(fcdata));
	fcdata.nargs = 2;
	fcdata.args[0].value = PointerGetDatum(tsq_from_image(img_a, len_a));
	fcdata.args[1].value = PointerGetDatum(tsq_from_image(img_b, len_b));
	*out_cmp = DatumGetInt32(tsquery_cmp(&fcdata));
	return 0;
}

/* tsq_mcontains core (oids 3691/3692; mcontained = swapped args upstream) */
int
pg_diff_tsq_mcontains(const unsigned char *img_q, int len_q,
					  const unsigned char *img_ex, int len_ex,
					  int *out_bool)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	FunctionCallInfoBaseData fcdata;

	pg_tsq_arena_reset();
	pg_diff_errcode = 0;
	*out_bool = 0;
	if (setjmp(pg_tsq_error_jmp))
		return 1;

	memset(&fcdata, 0, sizeof(fcdata));
	fcdata.nargs = 2;
	fcdata.args[0].value = PointerGetDatum(tsq_from_image(img_q, len_q));
	fcdata.args[1].value = PointerGetDatum(tsq_from_image(img_ex, len_ex));
	*out_bool = DatumGetBool(tsq_mcontains(&fcdata)) ? 1 : 0;
	return 0;
}

/* tsquery_numnode core (oid 3672) */
int
pg_diff_tsquery_numnode(const unsigned char *img, int len, int *out_n)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	FunctionCallInfoBaseData fcdata;

	pg_tsq_arena_reset();
	pg_diff_errcode = 0;
	*out_n = 0;
	if (setjmp(pg_tsq_error_jmp))
		return 1;

	memset(&fcdata, 0, sizeof(fcdata));
	fcdata.nargs = 1;
	fcdata.args[0].value = PointerGetDatum(tsq_from_image(img, len));
	*out_n = DatumGetInt32(tsquery_numnode(&fcdata));
	return 0;
}

/* tsquerytree core (oid 3673): image -> text payload (no NUL) */
int
pg_diff_tsquerytree(const unsigned char *img, int len,
					char *out, int out_cap, int *out_len)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	FunctionCallInfoBaseData fcdata;
	text	   *res;
	int			n;

	pg_tsq_arena_reset();
	pg_diff_errcode = 0;
	*out_len = 0;
	if (setjmp(pg_tsq_error_jmp))
		return 1;

	memset(&fcdata, 0, sizeof(fcdata));
	fcdata.nargs = 1;
	fcdata.args[0].value = PointerGetDatum(tsq_from_image(img, len));
	res = (text *) DatumGetPointer(tsquerytree(&fcdata));
	n = (int) VARSIZE(res) - VARHDRSZ;
	if (n > out_cap)
		abort();
	memcpy(out, VARDATA(res), n);
	*out_len = n;
	return 0;
}

/*
 * cleanup_tsquery_stopwords direct-image entry (tsquery_cleanup.c:387).
 * Upstream reaches the QI_VALSTOP+OP_PHRASE folding arms only through the
 * dictionary-morph pushval (to_tsquery/websearch_to_tsquery), which is the
 * excluded(engine) to_tsany crate's plumbing; the FUNCTION itself is pure
 * over a parse-internal image containing QI_VALSTOP entries. The driver
 * generates well-formed polish trees (valid opers, bounded depth) — both
 * sides treat the image as the trusted parse-internal representation, same
 * as upstream parse_tsquery does before calling it. noisy=false (the noisy
 * NOTICE arm is compared through the parse arms).
 */
int
pg_diff_tsquery_cleanup(const unsigned char *img, int len,
						unsigned char *out, int out_cap, int *out_len)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	TSQuery		res;

	pg_tsq_arena_reset();
	pg_diff_errcode = 0;
	*out_len = 0;
	if (setjmp(pg_tsq_error_jmp))
		return 1;

	res = cleanup_tsquery_stopwords(tsq_from_image(img, len), false);
	*out_len = image_out((struct varlena *) res, out, out_cap);
	return 0;
}

/* driver-controlled environment knob (see shim/utils/pg_locale.h) */
extern void pg_tsq_set_database_ctype_is_c(bool v);

int
pg_diff_tsq_set_ctype_is_c(int v)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	pg_tsq_set_database_ctype_is_c(v != 0);
	return 0;
}
