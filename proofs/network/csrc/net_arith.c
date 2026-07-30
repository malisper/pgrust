/*
 * Vendored from postgres REL_18_STABLE src/backend/utils/adt/network.c
 * (fetched 2026-07-28): internal_inetpl, inetpl, inetmi_int8, inetmi,
 * inet_to_cidr, inet_set_masklen, cidr_set_masklen, and (for the
 * set_masklen rows) a second static copy of cidr_set_masklen_internal.
 * Bodies verbatim.
 *
 * SHIMS (plumbing only, never logic — same conventions as net_ops.c):
 *  - inet varlena -> plain pgc_inet value struct {family, bits, addr[16]};
 *    ip_family/ip_bits/ip_addr/ip_maxbits/ip_addrsize macros redefined over
 *    it, definitions otherwise as in utils/inet.h.
 *  - PG_FUNCTION_ARGS / PG_GETARG_INET_PP / PG_GETARG_INT32 / PG_GETARG_INT64
 *    / PG_RETURN_* unwrapped to plain C signatures around the extracted
 *    bodies; value results go to a caller-provided *dst, scalar results to
 *    an out-param.
 *  - palloc0(sizeof(inet)) -> caller passes a ZEROED pgc_inet *dst (palloc0's
 *    all-zero guarantee is part of the C contract for the untouched tail
 *    bytes).
 *  - inet_set_masklen's `palloc(VARSIZE_ANY(src)) + memcpy` clone copies the
 *    src VALUE (family + addr over ip_addrsize bytes) into a zeroed dst:
 *    a v4 inet varlena image physically contains only 4 address bytes, so
 *    bytes past ip_addrsize are not part of the C value; the harness
 *    compares them against the Rust side's zeroed tail (both zero).
 *  - SET_INET_VARSIZE(dst) dropped (varlena-header housekeeping; the header
 *    is not part of the compared value).
 *  - ereport(ERROR, ...) -> int return sentinel; message plumbing leaves the
 *    proof, the errcode VERDICT stays:
 *        0  = no error
 *       -1  = ERRCODE_INVALID_PARAMETER_VALUE  (invalid mask length /
 *             cannot subtract inet values of different sizes)
 *       -2  = ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE (result is out of range)
 *       -3  = elog(ERROR, "invalid inet bit length: %d") in inet_to_cidr
 *             (internal error; unreachable under the datatype invariant)
 *  - inetmi_int8's `-addend` on int64: postgres compiles with -fwrapv, so
 *    negation of INT64_MIN wraps to INT64_MIN; CBMC's default
 *    two's-complement wrap models exactly that (prove-target skill note) —
 *    body kept verbatim, matching the shipped Rust wrapping_neg().
 *  - Assert() compiled out (NDEBUG / release-build semantics), as in
 *    net_ops.c: cidr_set_masklen_internal's Assert(bits <= ip_maxbits(dst))
 *    is dropped exactly as a production build compiles it out. Callers here
 *    (inet_to_cidr, cidr_set_masklen) pass bits validated <= maxbits first.
 *  - hashinet/hashinetextended are NOT vendored as hashes: C hashes
 *    VARDATA_ANY(addr) = the {family, bits, addr[0..addrsize]} byte prefix
 *    with hash_any (proved ≡ hashfn::hash_bytes[_extended], proofs/hash).
 *    pg_hashinet_view below reproduces exactly the byte string and length
 *    C feeds to hash_any ("XXX this assumes there are no pad bytes in the
 *    data structure" — pgc_inet has none, like inet_struct); the harness
 *    asserts the shipped Rust hashinet_bytes[_extended] equals the shipped
 *    hash over this C-assembled view, composing with the proved hash rows.
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
typedef long long int64;
typedef unsigned long long uint64;

void *memcpy(void *dst, const void *src, unsigned long n);

/*
 * Copy src and set mask length to 'bits' (which must be valid for the family)
 * [verbatim cidr_set_masklen_internal body; palloc0 -> caller-zeroed dst;
 *  Assert dropped per header; SET_INET_VARSIZE dropped]
 */
static inet *
pg_cidr_set_masklen_internal2(const inet *src, int bits, inet *dst)
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

/* inet_to_cidr(PG_FUNCTION_ARGS): src unwrapped, result to *dst. */
int
pg_inet_to_cidr(const inet *src, inet *dst)
{
	int			bits;

	bits = ip_bits(src);

	/* safety check */
	if ((bits < 0) || (bits > ip_maxbits(src)))
		return -3;				/* elog(ERROR, "invalid inet bit length") */

	pg_cidr_set_masklen_internal2(src, bits, dst);
	return 0;
}

/* inet_set_masklen(PG_FUNCTION_ARGS): (src, bits) unwrapped, result *dst. */
int
pg_inet_set_masklen(const inet *src, int bits, inet *dst)
{
	if (bits == -1)
		bits = ip_maxbits(src);

	if ((bits < 0) || (bits > ip_maxbits(src)))
		return -1;				/* ereport ERRCODE_INVALID_PARAMETER_VALUE */

	/* clone the original data */
	memcpy(ip_addr(dst), ip_addr(src), ip_addrsize(src));
	ip_family(dst) = ip_family(src);

	ip_bits(dst) = bits;

	return 0;
}

/* cidr_set_masklen(PG_FUNCTION_ARGS): (src, bits) unwrapped, result *dst. */
int
pg_cidr_set_masklen(const inet *src, int bits, inet *dst)
{
	if (bits == -1)
		bits = ip_maxbits(src);

	if ((bits < 0) || (bits > ip_maxbits(src)))
		return -1;				/* ereport ERRCODE_INVALID_PARAMETER_VALUE */

	pg_cidr_set_masklen_internal2(src, bits, dst);
	return 0;
}

/*
 * internal_inetpl [verbatim body; palloc0 -> caller-zeroed dst; ereport ->
 * -2 sentinel; SET_INET_VARSIZE dropped]
 */
static int
pg_internal_inetpl(inet *ip, int64 addend, inet *dst)
{
	{
		int			nb = ip_addrsize(ip);
		unsigned char *pip = ip_addr(ip);
		unsigned char *pdst = ip_addr(dst);
		int			carry = 0;

		while (--nb >= 0)
		{
			carry = pip[nb] + (int) (addend & 0xFF) + carry;
			pdst[nb] = (unsigned char) (carry & 0xFF);
			carry >>= 8;

			/*
			 * We have to be careful about right-shifting addend because
			 * right-shift isn't portable for negative values, while simply
			 * dividing by 256 doesn't work (the standard rounding is in the
			 * wrong direction, besides which there may be machines out there
			 * that round the wrong way).  So, explicitly clear the low-order
			 * byte to remove any doubt about the correct result of the
			 * division, and then divide rather than shift.
			 */
			addend &= ~((int64) 0xFF);
			addend /= 0x100;
		}

		/*
		 * At this point we should have addend and carry both zero if original
		 * addend was >= 0, or addend -1 and carry 1 if original addend was <
		 * 0.  Anything else means overflow.
		 */
		if (!((addend == 0 && carry == 0) ||
			  (addend == -1 && carry == 1)))
			return -2;			/* ereport ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE */
	}

	ip_bits(dst) = ip_bits(ip);
	ip_family(dst) = ip_family(ip);

	return 0;
}

/* inetpl(PG_FUNCTION_ARGS): (ip, addend) unwrapped, result *dst. */
int
pg_inetpl(inet *ip, int64 addend, inet *dst)
{
	return pg_internal_inetpl(ip, addend, dst);
}

/* inetmi_int8(PG_FUNCTION_ARGS): (ip, addend) unwrapped, result *dst. */
int
pg_inetmi_int8(inet *ip, int64 addend, inet *dst)
{
	return pg_internal_inetpl(ip, -addend, dst);
}

/* inetmi(PG_FUNCTION_ARGS): (ip, ip2) unwrapped, i64 result to *out. */
int
pg_inetmi(inet *ip, inet *ip2, int64 *out)
{
	int64		res = 0;

	if (ip_family(ip) != ip_family(ip2))
		return -1;				/* ereport ERRCODE_INVALID_PARAMETER_VALUE */
	else
	{
		/*
		 * We form the difference using the traditional complement, increment,
		 * and add rule, with the increment part being handled by starting the
		 * carry off at 1.  If you don't think integer arithmetic is done in
		 * two's complement, too bad.
		 */
		int			nb = ip_addrsize(ip);
		int			byte = 0;
		unsigned char *pip = ip_addr(ip);
		unsigned char *pip2 = ip_addr(ip2);
		int			carry = 1;

		while (--nb >= 0)
		{
			int			lobyte;

			carry = pip[nb] + (~pip2[nb] & 0xFF) + carry;
			lobyte = carry & 0xFF;
			if (byte < sizeof(int64))
			{
				res |= ((int64) lobyte) << (byte * 8);
			}
			else
			{
				/*
				 * Input wider than int64: check for overflow.  All bytes to
				 * the left of what will fit should be 0 or 0xFF, depending on
				 * sign of the now-complete result.
				 */
				if ((res < 0) ? (lobyte != 0xFF) : (lobyte != 0))
					return -2;	/* ereport ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE */
			}
			carry >>= 8;
			byte++;
		}

		/*
		 * If input is narrower than int64, overflow is not possible, but we
		 * have to do proper sign extension.
		 */
		if (carry == 0 && byte < sizeof(int64))
			res |= ((uint64) (int64) -1) << (byte * 8);
	}

	*out = res;
	return 0;
}

/*
 * hashinet/hashinetextended byte-view (see header): writes the exact byte
 * string C feeds hash_any — VARDATA_ANY(addr) for addrsize + 2 bytes, i.e.
 * {family, bits, addr[0..addrsize]} — and returns its length.
 */
int
pg_hashinet_view(const inet *addr, unsigned char *out)
{
	int			addrsize = ip_addrsize(addr);
	int			i;

	out[0] = ip_family(addr);
	out[1] = ip_bits(addr);
	for (i = 0; i < addrsize; i++)
		out[2 + i] = ip_addr(addr)[i];
	return addrsize + 2;
}
