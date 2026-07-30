/*
 * Vendored from postgres REL_18_STABLE src/backend/utils/adt/network.c
 * (fetched 2026-07-30): network_recv, network_send, addressOK.
 * Proof lane proofs/network-2026-07-30 (recv/send core-level rows
 * inet_recv 2496 / inet_send 2497 / cidr_recv 2498 / cidr_send 2499).
 *
 * Bodies verbatim except the documented shims:
 *  - typedef/macro boilerplate (pgc_inet, ip_* macros) matches this
 *    family's net_arith.c / net_ops.c convention.
 *  - StringInfo -> pgc_msgbuf {data,len,cursor}: the recv message is a
 *    caller-provided byte buffer; pq_getmsgbyte (pqformat.c REL_18_STABLE:
 *    "if (msg->cursor >= msg->len) ereport(ERROR, 08P01 no data left in
 *    message); return (unsigned char) msg->data[msg->cursor++];") becomes
 *    pgc_getmsgbyte with the family's err-flag convention (-5) instead of
 *    the ereport longjmp. After each read the body checks the flag and
 *    returns; C's ereport is also an immediate exit, so control flow is
 *    identical.
 *  - ereport(ERROR, ...) -> return of a negative sentinel; message text is
 *    out of proof (harness asserts sqlstate-class parity):
 *      -1 = invalid address family        (22P03)
 *      -2 = invalid bits                  (22P03)
 *      -3 = invalid length                (22P03)
 *      -4 = invalid external cidr value   (22P03, bits right of mask)
 *      -5 = no data left in message       (08P01, from pgc_getmsgbyte)
 *  - palloc0 -> caller-provided zeroed dst (family precedent).
 *  - SET_INET_VARSIZE dropped on recv (the varlena header is the Rust
 *    wrapper tier's job; the recv core-level claim is the value struct).
 *  - network_send: pq_begintypsend/pq_sendbyte/pq_endtypsend -> fixed
 *    caller buffer. pq_begintypsend reserves VARHDRSZ (4) zero bytes;
 *    pq_endtypsend stamps SET_VARSIZE(result, buf->len) — reproduced as
 *    the little-endian 4B-U header word ((uint32)len << 2), byte-for-byte
 *    what SET_VARSIZE_4B stores on the LE target platforms (and what the
 *    shipped datum::set_varsize_4b writes). Returns total image length.
 *  - Assert -> no-op (addressOK's Assert(bits <= maxbits): every caller
 *    here validates bits first, matching C).
 */

typedef struct {
	unsigned char family;
	unsigned char bits;
	unsigned char addr[16];
} pgc_inet;

#define PGSQL_AF_INET	2
#define PGSQL_AF_INET6	3

#define ip_family(inetptr)	((inetptr)->family)
#define ip_bits(inetptr)	((inetptr)->bits)
#define ip_addr(inetptr)	((inetptr)->addr)
#define ip_maxbits(inetptr) \
	(ip_family(inetptr) == PGSQL_AF_INET ? 32 : 128)
#define ip_addrsize(inetptr) \
	(ip_family(inetptr) == PGSQL_AF_INET ? 4 : 16)

typedef pgc_inet inet;
typedef int bool;
#define true 1
#define false 0
#define Assert(x) ((void) 0)

/* StringInfo read-side model (see header) */
typedef struct {
	const unsigned char *data;
	int			len;
	int			cursor;
} pgc_msgbuf;

/* pq_getmsgbyte, ereport -> *err = 1 (sentinel -5 at the caller) */
static int
pgc_getmsgbyte(pgc_msgbuf *msg, int *err)
{
	if (msg->cursor >= msg->len)
	{
		*err = 1;
		return 0;
	}
	return (unsigned char) msg->data[msg->cursor++];
}

/* addressOK: verbatim */
static bool
pgc_addressOK(unsigned char *a, int bits, int family)
{
	int			byte;
	int			nbits;
	int			maxbits;
	int			maxbytes;
	unsigned char mask;

	if (family == PGSQL_AF_INET)
	{
		maxbits = 32;
		maxbytes = 4;
	}
	else
	{
		maxbits = 128;
		maxbytes = 16;
	}
	Assert(bits <= maxbits);

	if (bits == maxbits)
		return true;

	byte = bits / 8;

	nbits = bits % 8;
	mask = 0xff;
	if (bits != 0)
		mask >>= nbits;

	while (byte < maxbytes)
	{
		if ((a[byte] & mask) != 0)
			return false;
		mask = 0xff;
		byte++;
	}

	return true;
}

/*
 * network_recv: body verbatim modulo the header's shims. Returns 0 on
 * success (dst filled, *cursor_out = final message cursor) or a negative
 * sentinel. dst must be zeroed by the caller (palloc0).
 */
int
pg_network_recv(const unsigned char *data, int len, int is_cidr,
				pgc_inet *dst, int *cursor_out)
{
	pgc_msgbuf	mbuf;
	pgc_msgbuf *buf = &mbuf;
	inet	   *addr = dst;
	char	   *addrptr;
	int			bits;
	int			nb,
				i;
	int			err = 0;

	mbuf.data = data;
	mbuf.len = len;
	mbuf.cursor = 0;

	ip_family(addr) = pgc_getmsgbyte(buf, &err);
	if (err)
		return -5;
	if (ip_family(addr) != PGSQL_AF_INET &&
		ip_family(addr) != PGSQL_AF_INET6)
		return -1;				/* ereport 22P03 invalid address family */
	bits = pgc_getmsgbyte(buf, &err);
	if (err)
		return -5;
	if (bits < 0 || bits > ip_maxbits(addr))
		return -2;				/* ereport 22P03 invalid bits */
	ip_bits(addr) = bits;
	i = pgc_getmsgbyte(buf, &err);	/* ignore is_cidr */
	if (err)
		return -5;
	nb = pgc_getmsgbyte(buf, &err);
	if (err)
		return -5;
	if (nb != ip_addrsize(addr))
		return -3;				/* ereport 22P03 invalid length */

	addrptr = (char *) ip_addr(addr);
	for (i = 0; i < nb; i++)
	{
		addrptr[i] = pgc_getmsgbyte(buf, &err);
		if (err)
			return -5;
	}

	/*
	 * Error check: CIDR values must not have any bits set beyond the masklen.
	 */
	if (is_cidr)
	{
		if (!pgc_addressOK(ip_addr(addr), bits, ip_family(addr)))
			return -4;			/* ereport 22P03, bits right of mask */
	}

	*cursor_out = mbuf.cursor;
	return 0;
}

/*
 * network_send: body verbatim modulo the fixed-buffer StringInfo shims
 * (see header). out must hold 4 + 4 + 16 bytes; returns the total image
 * length (varlena header included).
 */
int
pg_network_send(const pgc_inet *addr, int is_cidr, unsigned char *out)
{
	int			buflen = 0;
	char	   *addrptr;
	int			nb,
				i;

	/* pq_begintypsend: reserve 4 zero bytes for the bytea length word */
	for (i = 0; i < 4; i++)
		out[buflen++] = 0;
	out[buflen++] = ip_family(addr);
	out[buflen++] = ip_bits(addr);
	out[buflen++] = is_cidr;
	nb = ip_addrsize(addr);
	out[buflen++] = nb;
	addrptr = (char *) ip_addr(addr);
	for (i = 0; i < nb; i++)
		out[buflen++] = addrptr[i];

	/* pq_endtypsend: SET_VARSIZE(result, buf->len), LE 4B-U header word */
	{
		unsigned int header = ((unsigned int) buflen) << 2;

		out[0] = (unsigned char) (header & 0xff);
		out[1] = (unsigned char) ((header >> 8) & 0xff);
		out[2] = (unsigned char) ((header >> 16) & 0xff);
		out[3] = (unsigned char) ((header >> 24) & 0xff);
	}
	return buflen;
}
