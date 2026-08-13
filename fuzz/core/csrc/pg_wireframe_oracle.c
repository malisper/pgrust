/*
 * Vendored PostgreSQL C: frontend message-length FRAMING arithmetic —
 * differential-fuzz oracle for the wire_length_diff target.
 *
 * This is the narrowest self-contained slice of the FE/BE wire framing: the
 * big-endian length-word read + the `-4` length arithmetic + the length-vs-
 * bound check. It is the ST3 memory-safety surface (the INT32_MIN
 * `i32::from_be_bytes(len) - 4` wrapping class). It deliberately does NOT
 * vendor the surrounding SSL/GSS/cancel state machine, the pq_getbytes recv
 * buffering, MyProcPort, or StringInfo growth — those are the pieces EDGE2
 * documented as "too entangled". Only the length-framing arithmetic (the part
 * an attacker's length word actually drives) is extracted, with a byte-order
 * shim so the oracle is host-independent.
 *
 * Provenance (bodies VERBATIM apart from the numbered shims), from the repo's
 * vendored ground-truth checkout
 * ../pgrust-reference/vendor/postgres-src @ 62d6c7d "Stamp 18.3." (PostgreSQL
 * 18.3 exactly — the campaign oracle pin):
 *
 *   - src/backend/tcop/backend_startup.c  ProcessStartupPacket, lines 532-542:
 *         len = pg_ntoh32(len);
 *         len -= 4;
 *         if (len < (int32) sizeof(ProtocolVersion) ||
 *             len > MAX_STARTUP_PACKET_LENGTH)
 *             ereport(COMMERROR, ... "invalid length of startup packet");
 *     sizeof(ProtocolVersion) == 4 (src/include/libpq/pqcomm.h:99, uint32);
 *     MAX_STARTUP_PACKET_LENGTH == 10000 (pqcomm.h:118).
 *     NOTE the subtract happens BEFORE the range check — an INT_MIN length
 *     word wraps on `len -= 4` (two's-complement under a normal cc build,
 *     matching a compiled backend) and the wrapped huge value is then
 *     rejected by the upper bound. Reproduced faithfully below.
 *
 *   - src/backend/libpq/pqcomm.c  pq_getmessage, lines 1221-1231:
 *         len = pg_ntoh32(len);
 *         if (len < 4 || len > maxlen)
 *             ereport(COMMERROR, ... "invalid message length");
 *         len -= 4;                 // discount length itself
 *     Here the range check happens BEFORE the subtract, so `len -= 4` can
 *     never underflow (len >= 4 on that path). Reproduced faithfully below.
 *
 * SHIMS (plumbing only, never logic; numbered, each marked at its site):
 *   S1. Byte-order read. C reads the 4 wire bytes directly into `len`'s
 *       storage (network order) then applies pg_ntoh32. The equivalent
 *       host-independent value is the big-endian interpretation of the four
 *       bytes — computed explicitly here so the oracle needs no PG headers
 *       and no host-endianness assumption. Identical to Rust's
 *       i32::from_be_bytes([b0,b1,b2,b3]).
 *   S2. ereport(COMMERROR, ...) -> return the reject code. The FE/BE framing
 *       paths log a COMMERROR and return EOF/STATUS_ERROR (no longjmp); the
 *       accept-or-reject verdict is the only compared plane. Message text is
 *       out of scope.
 */

#include <stdint.h>

/* Verdict codes (the single compared plane). */
#define WF_ACCEPT 0
#define WF_REJECT 1

/* S2 constants, verbatim from pqcomm.h. */
#define WF_SIZEOF_PROTOCOL_VERSION 4       /* sizeof(uint32 ProtocolVersion) */
#define WF_MAX_STARTUP_PACKET_LENGTH 10000 /* MAX_STARTUP_PACKET_LENGTH */

/* S1: pg_ntoh32(memcpy(&len, buf, 4)) rendered host-independently. */
static inline int32_t wf_read_be32(const uint8_t b[4])
{
	uint32_t v = ((uint32_t) b[0] << 24) | ((uint32_t) b[1] << 16) |
				 ((uint32_t) b[2] << 8) | ((uint32_t) b[3]);
	return (int32_t) v;
}

/*
 * ProcessStartupPacket length framing (backend_startup.c:532-542, verbatim).
 * Returns WF_ACCEPT and writes the body length (packet minus the 4-byte length
 * word) to *out_body, or WF_REJECT.
 */
int
pg_wireframe_startup_len(const uint8_t len_bytes[4], int32_t *out_body)
{
	int32_t		len;

	len = wf_read_be32(len_bytes);	/* len = pg_ntoh32(len); */
	len -= 4;					/* len -= 4;  (subtract BEFORE the check) */

	if (len < (int32_t) WF_SIZEOF_PROTOCOL_VERSION ||
		len > WF_MAX_STARTUP_PACKET_LENGTH)
	{
		/* S2: ereport(COMMERROR, ... "invalid length of startup packet"); */
		return WF_REJECT;
	}

	*out_body = len;
	return WF_ACCEPT;
}

/*
 * pq_getmessage outer length framing (pqcomm.c:1221-1231, verbatim).
 * `maxlen` is the caller's cap (PqRecvBuffer-independent here). Returns
 * WF_ACCEPT and writes the body length to *out_body, or WF_REJECT.
 */
int
pg_wireframe_getmessage_len(const uint8_t len_bytes[4], int32_t maxlen,
							int32_t *out_body)
{
	int32_t		len;

	len = wf_read_be32(len_bytes);	/* len = pg_ntoh32(len); */

	if (len < 4 || len > maxlen)
	{
		/* S2: ereport(COMMERROR, ... "invalid message length"); */
		return WF_REJECT;
	}

	len -= 4;					/* len -= 4;  (subtract AFTER the check) */

	*out_body = len;
	return WF_ACCEPT;
}
