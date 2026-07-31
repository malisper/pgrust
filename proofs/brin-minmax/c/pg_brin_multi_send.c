/*
 * Vendored from postgres REL_18_STABLE (fetched 2026-07-30 via
 * raw.githubusercontent.com/postgres/postgres/REL_18_STABLE):
 *   src/backend/access/brin/brin_minmax_multi.c —
 *       brin_minmax_multi_summary_send, whose entire body is verbatim
 *           `return byteasend(fcinfo);`
 *   src/backend/utils/adt/varlena.c — byteasend: detoast the bytea arg and
 *       return it unchanged ("just copy the input" per its comment) — the
 *       binary wire image IS the detoasted payload.
 *
 * SEPARATE c-lib from pg_brin_multi_dist.c / pg_brin_minmax.c (mbconv law:
 * whole-family linking fakes solver walls). The send harnesses link only
 * this file.
 *
 * SHIMS (plumbing only, per family rules):
 *  - fmgr unwrapping: PG_FUNCTION_ARGS -> plain (payload ptr, len)
 *    parameters (the PG_GETARG_BYTEA_P_COPY detoast prologue is the
 *    caller-provided detoasted payload; the Rust wrapper performs the same
 *    detoast via arg_varlena_packed, which stays in-theorem);
 *  - PG_RETURN_BYTEA_P(vlena) -> the returned image's PAYLOAD bytes are
 *    written to a caller buffer (out); header reconstruction is asserted
 *    on the Rust side via PackedVarlena::data()/len parity. Allocation
 *    (palloc of the copy) -> caller buffer; strategy out of proof.
 *  - int return (void/Unit FFI trap).
 */

#include "../../support/c/pg_proof_shim.h"

/* brin_minmax_multi_summary_send == byteasend == identity copy of the
 * detoasted payload (see header). */
int
pg_mm_summary_send(const unsigned char *d, int len, unsigned char *out)
{
	int			i;

	for (i = 0; i < len; i++)
		out[i] = d[i];
	return 0;
}

/* ==================== negative control (NOT postgres code) ==================== */

/* control shim: drops the last payload byte (replaces it with the
 * complement) — the harness comparing this against the shipped
 * fc_summary_send MUST FAIL with a decodable counterexample. */
int
pg_mm_summary_send_wrong(const unsigned char *d, int len, unsigned char *out)
{
	int			i;

	for (i = 0; i < len; i++)
		out[i] = d[i];
	if (len > 0)
		out[len - 1] = (unsigned char) ~d[len - 1];
	return 0;
}
