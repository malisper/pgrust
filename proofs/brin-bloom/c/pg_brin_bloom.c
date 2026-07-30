/*
 * Vendored from postgres REL_18_STABLE (fetched 2026-07-30 via
 * raw.githubusercontent.com/postgres/postgres/REL_18_STABLE):
 *   src/backend/access/brin/brin_bloom.c —
 *       brin_bloom_summary_in, brin_bloom_summary_recv (bodies verbatim),
 *       brin_bloom_summary_send (whole body verbatim
 *           `return byteasend(fcinfo);`)
 *   src/backend/utils/adt/varlena.c — byteasend: detoast the bytea arg and
 *       return it unchanged ("just copy the input" per its comment) — the
 *       binary wire image IS the detoasted payload.
 *
 * SHIMS (plumbing only; mirrors proofs/brin-minmax c/pg_brin_multi_dist.c
 * summary section — see that header):
 *  - elog.h PGSIXBIT/MAKE_SQLSTATE encoders verbatim, so the recorded
 *    errcode int is bit-comparable against pgrust's SqlState(i32);
 *  - ereport(ERROR, ...) -> records the errcode in pg_bloom_errcode and
 *    returns 1 at the exact program point (C aborts via longjmp there);
 *    errmsg -> no-op (message text never crosses the seam);
 *  - summary_send: PG_FUNCTION_ARGS -> plain (payload ptr, len) params
 *    (PG_GETARG detoast prologue = caller-provided detoasted payload; the
 *    Rust wrapper's own detoast stays in-theorem);
 *    PG_RETURN_BYTEA_P -> payload bytes to a caller buffer (allocation
 *    strategy out of proof); int returns (void/Unit FFI trap).
 */

#include "../../support/c/pg_proof_shim.h"

/* ==================== summary_in / summary_recv ==================== */

/* elog.h encoders, verbatim */
#define PGSIXBIT(ch)	(((ch) - '0') & 0x3F)
#define MAKE_SQLSTATE(ch1,ch2,ch3,ch4,ch5)	\
	(PGSIXBIT(ch1) + (PGSIXBIT(ch2) << 6) + (PGSIXBIT(ch3) << 12) + \
	 (PGSIXBIT(ch4) << 18) + (PGSIXBIT(ch5) << 24))
#define ERRCODE_FEATURE_NOT_SUPPORTED MAKE_SQLSTATE('0','A','0','0','0')

/* ereport rewire (header comment): record errcode, return 1 at the exact
 * program point. Message text never crosses the seam. */
static int32 pg_bloom_errcode;
#define errcode(c) ((void) (pg_bloom_errcode = (c)))
#define errmsg(...) ((void) 0)
#define ereport(elevel, rest) do { rest; return 1; } while (0)
#define PG_RETURN_VOID() return 0

int32
pg_bloom_errcode_get(void)
{
	return pg_bloom_errcode;
}

/* brin_bloom_summary_in, body verbatim */
int
pg_bloom_summary_in(void)
{
	pg_bloom_errcode = 0;

	/*
	 * brin_bloom_summary stores the data in binary form and parsing text
	 * input is not needed, so disallow this.
	 */
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("cannot accept a value of type %s", "pg_brin_bloom_summary")));

	PG_RETURN_VOID();			/* keep compiler quiet */
}

/* brin_bloom_summary_recv, body verbatim */
int
pg_bloom_summary_recv(void)
{
	pg_bloom_errcode = 0;

	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("cannot accept a value of type %s", "pg_brin_bloom_summary")));

	PG_RETURN_VOID();			/* keep compiler quiet */
}

/* ==================== summary_send ==================== */

/* brin_bloom_summary_send == byteasend == identity copy of the detoasted
 * payload (see header). */
int
pg_bloom_summary_send(const unsigned char *d, int len, unsigned char *out)
{
	int			i;

	for (i = 0; i < len; i++)
		out[i] = d[i];
	return 0;
}

/* ==================== negative control (NOT postgres code) ==================== */

/* control shim: flips the last payload byte — the harness comparing this
 * against the shipped fc_summary_send MUST FAIL with a decodable
 * counterexample. */
int
pg_bloom_summary_send_wrong(const unsigned char *d, int len, unsigned char *out)
{
	int			i;

	for (i = 0; i < len; i++)
		out[i] = d[i];
	if (len > 0)
		out[len - 1] = (unsigned char) ~d[len - 1];
	return 0;
}
