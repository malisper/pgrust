/*
 * Vendored PostgreSQL C for Kani dual-execution proofs: "char" wire rows
 * charrecv (pg_proc oid 2434) and charsend (2435).
 *
 * Provenance (REL_18_STABLE, fetched 2026-07-30):
 *   src/backend/utils/adt/char.c    (charrecv, charsend bodies)
 *   src/backend/libpq/pqformat.c    (pq_getmsgbyte; pq_begintypsend,
 *                                    pq_sendbyte, pq_endtypsend semantics)
 *
 * SHIMS (plumbing only, never logic; the proofs/pg_lsn wave-5 wire
 * conventions — see proofs/pg_lsn/c/pg_pg_lsn.c):
 *   R1. StringInfo -> (data, len, cursor) triple; pq_getmsgbyte's
 *       ereport(ERROR, errcode(ERRCODE_PROTOCOL_VIOLATION),
 *       errmsg("no data left in message")) -> status sentinel 4
 *       (sqlstate 08P01 + level asserted Rust-side).  The cursor-bounds
 *       test (msg->cursor >= msg->len) and the byte read/advance are
 *       verbatim.
 *   R2. fmgr unwrapped: PG_RETURN_CHAR truncates int -> char; modeled by
 *       the explicit (char) cast exactly where C's fmgr macro does it
 *       (csrc/char_shim.c precedent; int-widening of the result is
 *       char-signedness platform-split, harnesses compare 8-bit values —
 *       see ADJUDICATION-CHAR-SIGNEDNESS.md).
 *   S1. charsend: pq_begintypsend + pq_sendbyte(arg1) + pq_endtypsend ->
 *       caller-provided out buffer; SET_VARSIZE = 4-byte little-endian
 *       varlena header (total_len << 2), payload after it (the
 *       proofs/uuid pg_uuid_send shim pattern).  Returns the total image
 *       length (5).
 *
 * Postgres compiles with -fwrapv; CBMC's two's-complement default matches.
 */

#include <stdint.h>

/* pqformat.c pq_getmsgbyte under shim R1: bounds test + read verbatim. */
static int
pgc_pq_getmsgbyte(const unsigned char *data, int32_t len, int32_t *cursor,
				  int *out)
{
	if (*cursor >= len)
		return 4;				/* shim R1: ereport(ERRCODE_PROTOCOL_VIOLATION,
								 * "no data left in message") */
	*out = (int) (unsigned char) data[(*cursor)++];
	return 0;
}

/* char.c charrecv: PG_RETURN_CHAR(pq_getmsgbyte(buf)); shims R1/R2.
 * Returns 0 with *out = the received char (widened int), or 4. */
int
pgc_charrecv(const unsigned char *data, int32_t len, int32_t *cursor,
			 int *out)
{
	int			st;
	int			b;

	st = pgc_pq_getmsgbyte(data, len, cursor, &b);
	if (st != 0)
		return st;
	*out = (int) (char) b;		/* shim R2: PG_RETURN_CHAR truncation */
	return 0;
}

/* char.c charsend: pq_begintypsend + pq_sendbyte(arg1) + pq_endtypsend
 * -> 5-byte image (4B LE header + 1 payload byte); shims R2/S1. */
int32_t
pgc_charsend(int ch_i, unsigned char *out /* [5] */ )
{
	char		arg1 = (char) ch_i;	/* shim R2: PG_GETARG_CHAR */
	uint32_t	hdr = (uint32_t) 5 << 2;	/* shim S1 */

	out[4] = (unsigned char) arg1;	/* pq_sendbyte(buf, arg1) */
	out[0] = (unsigned char) (hdr & 0xFF);
	out[1] = (unsigned char) ((hdr >> 8) & 0xFF);
	out[2] = (unsigned char) ((hdr >> 16) & 0xFF);
	out[3] = (unsigned char) ((hdr >> 24) & 0xFF);
	return 5;
}
