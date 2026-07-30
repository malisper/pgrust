/*
 * Vendored from postgres REL_18_STABLE src/backend/utils/adt/network.c
 * (fetched 2026-07-28): network_cmp_internal, network_lt/le/eq/ge/gt/ne/cmp,
 * network_smaller/larger, network_sub/subeq/sup/supeq/overlap,
 * network_masklen, network_family, network_broadcast, network_netmask,
 * network_hostmask, inet_same_family, inet_merge,
 * cidr_set_masklen_internal. Bodies verbatim.
 * (bitncmp/bitncommon were checked identical between master and
 * REL_18_STABLE; the existing net_shim.c vendoring stands for both.)
 *
 * SHIMS (plumbing only, never logic):
 *  - inet varlena (varattrib + inet_struct) -> plain pgc_inet value struct
 *    {family, bits, addr[16]}; ip_family/ip_bits/ip_addr/ip_maxbits/
 *    ip_addrsize macros redefined over it, definitions otherwise as in
 *    utils/inet.h.
 *  - PG_FUNCTION_ARGS / PG_GETARG_INET_PP / PG_RETURN_* unwrapped to plain
 *    C signatures around the extracted bodies.
 *  - palloc0(sizeof(inet)) -> caller passes a ZEROED pgc_inet *dst (the
 *    harness zeroes it; palloc0's all-zero guarantee is part of the C
 *    contract for the untouched tail bytes).
 *  - SET_INET_VARSIZE(dst) dropped (varlena-header housekeeping; the header
 *    is not part of the compared value).
 *  - network_smaller/larger return the winning INPUT pointer in C
 *    (PG_RETURN_INET_P(a1|a2)); shimmed to the winning arg INDEX (0/1).
 *  - inet_merge's ereport(ERROR, ...) on family mismatch -> return -1
 *    sentinel (message plumbing leaves the proof; verdict stays).
 *  - bitncmp/bitncommon resolve to pgc_bitncmp/pgc_bitncommon from
 *    net_shim.c (same translation unit set), which pin libc memcmp to the
 *    first-differing-byte-difference convention (see net_shim.c header).
 *  - Assert() compiled out (NDEBUG / release-build semantics): the one
 *    Assert in the vendored set — cidr_set_masklen_internal's
 *    Assert(bits <= ip_maxbits(dst)) inside the bits > 0 branch — is
 *    dropped here, exactly as a production postgres build compiles it out.
 *    Callers in this file only pass bitncommon results (<= maxbits), so
 *    the guarded precondition holds. (Drop was previously undocumented;
 *    recorded per provenance audit, proofs/PROVENANCE-AUDIT.md 2026-07-28.)
 *  - Min macro inlined from c.h.
 *  - the Datum-returning builders (broadcast/netmask/hostmask) return a
 *    dummy int 0 instead of void: Kani lowers Rust () as `struct Unit`,
 *    which goto-cc rejects against C void (prove-target skill shim).
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

#define Min(x, y)		((x) < (y) ? (x) : (y))

extern int pgc_bitncmp(const unsigned char *l, const unsigned char *r, int n);
extern int pgc_bitncommon(const unsigned char *l, const unsigned char *r, int n);
#define bitncmp pgc_bitncmp
#define bitncommon pgc_bitncommon

void *memcpy(void *dst, const void *src, unsigned long n);

typedef pgc_inet inet;

/*
 * Copy src and set mask length to 'bits' (which must be valid for the family)
 */
static inet *
pg_cidr_set_masklen_internal(const inet *src, int bits, inet *dst)
{
	ip_family(dst) = ip_family(src);
	ip_bits(dst) = bits;

	if (bits > 0)
	{
		/* Clone appropriate bytes of the address, leaving the rest 0 */
		memcpy(ip_addr(dst), ip_addr(src), (bits + 7) / 8);

		/* Clear any unwanted bits in the last partial byte */
		if (bits % 8)
			ip_addr(dst)[bits / 8] &= ~(0xFF >> (bits % 8));
	}

	return dst;
}

int
pg_network_cmp_internal(inet *a1, inet *a2)
{
	if (ip_family(a1) == ip_family(a2))
	{
		int			order;

		order = bitncmp(ip_addr(a1), ip_addr(a2),
						Min(ip_bits(a1), ip_bits(a2)));
		if (order != 0)
			return order;
		order = ((int) ip_bits(a1)) - ((int) ip_bits(a2));
		if (order != 0)
			return order;
		return bitncmp(ip_addr(a1), ip_addr(a2), ip_maxbits(a1));
	}

	return ip_family(a1) - ip_family(a2);
}

#define network_cmp_internal pg_network_cmp_internal

int
pg_network_lt(inet *a1, inet *a2)
{
	return network_cmp_internal(a1, a2) < 0;
}

int
pg_network_le(inet *a1, inet *a2)
{
	return network_cmp_internal(a1, a2) <= 0;
}

int
pg_network_eq(inet *a1, inet *a2)
{
	return network_cmp_internal(a1, a2) == 0;
}

int
pg_network_ge(inet *a1, inet *a2)
{
	return network_cmp_internal(a1, a2) >= 0;
}

int
pg_network_gt(inet *a1, inet *a2)
{
	return network_cmp_internal(a1, a2) > 0;
}

int
pg_network_ne(inet *a1, inet *a2)
{
	return network_cmp_internal(a1, a2) != 0;
}

int
pg_network_cmp(inet *a1, inet *a2)
{
	return network_cmp_internal(a1, a2);
}

/* winning arg index: 0 = a1, 1 = a2 (PG_RETURN_INET_P shim, see header) */
int
pg_network_smaller(inet *a1, inet *a2)
{
	if (network_cmp_internal(a1, a2) < 0)
		return 0;
	else
		return 1;
}

int
pg_network_larger(inet *a1, inet *a2)
{
	if (network_cmp_internal(a1, a2) > 0)
		return 0;
	else
		return 1;
}

int
pg_network_sub(inet *a1, inet *a2)
{
	if (ip_family(a1) == ip_family(a2))
	{
		return (ip_bits(a1) > ip_bits(a2) &&
				bitncmp(ip_addr(a1), ip_addr(a2), ip_bits(a2)) == 0);
	}

	return 0;
}

int
pg_network_subeq(inet *a1, inet *a2)
{
	if (ip_family(a1) == ip_family(a2))
	{
		return (ip_bits(a1) >= ip_bits(a2) &&
				bitncmp(ip_addr(a1), ip_addr(a2), ip_bits(a2)) == 0);
	}

	return 0;
}

int
pg_network_sup(inet *a1, inet *a2)
{
	if (ip_family(a1) == ip_family(a2))
	{
		return (ip_bits(a1) < ip_bits(a2) &&
				bitncmp(ip_addr(a1), ip_addr(a2), ip_bits(a1)) == 0);
	}

	return 0;
}

int
pg_network_supeq(inet *a1, inet *a2)
{
	if (ip_family(a1) == ip_family(a2))
	{
		return (ip_bits(a1) <= ip_bits(a2) &&
				bitncmp(ip_addr(a1), ip_addr(a2), ip_bits(a1)) == 0);
	}

	return 0;
}

int
pg_network_overlap(inet *a1, inet *a2)
{
	if (ip_family(a1) == ip_family(a2))
	{
		return (bitncmp(ip_addr(a1), ip_addr(a2),
						Min(ip_bits(a1), ip_bits(a2))) == 0);
	}

	return 0;
}

int
pg_network_masklen(inet *ip)
{
	return ip_bits(ip);
}

int
pg_network_family(inet *ip)
{
	switch (ip_family(ip))
	{
		case PGSQL_AF_INET:
			return 4;
		case PGSQL_AF_INET6:
			return 6;
		default:
			return 0;
	}
}

/* dst: caller-zeroed (palloc0 shim) */
int
pg_network_broadcast(inet *ip, inet *dst)
{
	int			byte;
	int			bits;
	int			maxbytes;
	unsigned char mask;
	unsigned char *a,
			   *b;

	maxbytes = ip_addrsize(ip);
	bits = ip_bits(ip);
	a = ip_addr(ip);
	b = ip_addr(dst);

	for (byte = 0; byte < maxbytes; byte++)
	{
		if (bits >= 8)
		{
			mask = 0x00;
			bits -= 8;
		}
		else if (bits == 0)
			mask = 0xff;
		else
		{
			mask = 0xff >> bits;
			bits = 0;
		}

		b[byte] = a[byte] | mask;
	}

	ip_family(dst) = ip_family(ip);
	ip_bits(dst) = ip_bits(ip);
	return 0;
}

/* dst: caller-zeroed (palloc0 shim) */
int
pg_network_netmask(inet *ip, inet *dst)
{
	int			byte;
	int			bits;
	unsigned char mask;
	unsigned char *b;

	bits = ip_bits(ip);
	b = ip_addr(dst);

	byte = 0;

	while (bits)
	{
		if (bits >= 8)
		{
			mask = 0xff;
			bits -= 8;
		}
		else
		{
			mask = 0xff << (8 - bits);
			bits = 0;
		}

		b[byte] = mask;
		byte++;
	}

	ip_family(dst) = ip_family(ip);
	ip_bits(dst) = ip_maxbits(ip);
	return 0;
}

/* dst: caller-zeroed (palloc0 shim) */
int
pg_network_hostmask(inet *ip, inet *dst)
{
	int			byte;
	int			bits;
	int			maxbytes;
	unsigned char mask;
	unsigned char *b;

	maxbytes = ip_addrsize(ip);
	bits = ip_maxbits(ip) - ip_bits(ip);
	b = ip_addr(dst);

	byte = maxbytes - 1;

	while (bits)
	{
		if (bits >= 8)
		{
			mask = 0xff;
			bits -= 8;
		}
		else
		{
			mask = 0xff >> (8 - bits);
			bits = 0;
		}

		b[byte] = mask;
		byte--;
	}

	ip_family(dst) = ip_family(ip);
	ip_bits(dst) = ip_maxbits(ip);
	return 0;
}

int
pg_inet_same_family(inet *a1, inet *a2)
{
	return ip_family(a1) == ip_family(a2);
}

/*
 * dst: caller-zeroed (palloc0 shim). Returns 0 on success, -1 for the
 * family-mismatch ereport (shim).
 */
int
pg_inet_merge(inet *a1, inet *a2, inet *dst)
{
	int			commonbits;

	if (ip_family(a1) != ip_family(a2))
		return -1;				/* ereport(ERROR, ...) shim */

	commonbits = bitncommon(ip_addr(a1), ip_addr(a2),
							Min(ip_bits(a1), ip_bits(a2)));

	pg_cidr_set_masklen_internal(a1, commonbits, dst);
	return 0;
}

/*
 * network_network (oid 683) + inetnot/inetand/inetor (oids 2627/2628/2629),
 * vendored from REL_18_STABLE src/backend/utils/adt/network.c (fetched
 * 2026-07-28). Same shims as the builders above: fmgr wrappers -> plain
 * signatures over the flat inet struct, palloc0(dst) -> caller-zeroed dst,
 * SET_INET_VARSIZE dropped (flat struct), inetand/inetor's
 * ereport(ERROR, ERRCODE_INVALID_PARAMETER_VALUE, "cannot AND/OR inet
 * values of different sizes") -> return -1 sentinel. Loop bodies verbatim.
 */

#ifndef Max
#define Max(x, y) ((x) > (y) ? (x) : (y))
#endif

int
pg_network_network(inet *ip, inet *dst)
{
	int			byte;
	int			bits;
	unsigned char mask;
	unsigned char *a,
			   *b;

	bits = ip_bits(ip);
	a = ip_addr(ip);
	b = ip_addr(dst);

	byte = 0;

	while (bits)
	{
		if (bits >= 8)
		{
			mask = 0xff;
			bits -= 8;
		}
		else
		{
			mask = 0xff << (8 - bits);
			bits = 0;
		}

		b[byte] = a[byte] & mask;
		byte++;
	}

	ip_family(dst) = ip_family(ip);
	ip_bits(dst) = ip_bits(ip);
	return 0;
}

int
pg_inetnot(inet *ip, inet *dst)
{
	{
		int			nb = ip_addrsize(ip);
		unsigned char *pip = ip_addr(ip);
		unsigned char *pdst = ip_addr(dst);

		while (--nb >= 0)
			pdst[nb] = ~pip[nb];
	}
	ip_bits(dst) = ip_bits(ip);

	ip_family(dst) = ip_family(ip);
	return 0;
}

int
pg_inetand(inet *ip, inet *ip2, inet *dst)
{
	if (ip_family(ip) != ip_family(ip2))
		return -1;				/* ereport(ERROR, invalid_parameter_value) */
	else
	{
		int			nb = ip_addrsize(ip);
		unsigned char *pip = ip_addr(ip);
		unsigned char *pip2 = ip_addr(ip2);
		unsigned char *pdst = ip_addr(dst);

		while (--nb >= 0)
			pdst[nb] = pip[nb] & pip2[nb];
	}
	ip_bits(dst) = Max(ip_bits(ip), ip_bits(ip2));

	ip_family(dst) = ip_family(ip);
	return 0;
}

int
pg_inetor(inet *ip, inet *ip2, inet *dst)
{
	if (ip_family(ip) != ip_family(ip2))
		return -1;				/* ereport(ERROR, invalid_parameter_value) */
	else
	{
		int			nb = ip_addrsize(ip);
		unsigned char *pip = ip_addr(ip);
		unsigned char *pip2 = ip_addr(ip2);
		unsigned char *pdst = ip_addr(dst);

		while (--nb >= 0)
			pdst[nb] = pip[nb] | pip2[nb];
	}
	ip_bits(dst) = Max(ip_bits(ip), ip_bits(ip2));

	ip_family(dst) = ip_family(ip);
	return 0;
}
