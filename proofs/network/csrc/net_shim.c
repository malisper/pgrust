/*
 * Vendored from postgres master src/backend/utils/adt/network.c
 * (bitncmp, bitncommon; fetched 2026-07-28). REL_18_STABLE conformance:
 * bitncmp/bitncommon checked identical between master and REL_18_STABLE
 * (provenance audit, proofs/PROVENANCE-AUDIT.md, 2026-07-28).
 *
 * SHIMS (everything else is verbatim):
 *  - IS_HIGHBIT_SET macro inlined from c.h.
 *  - memcmp replaced by pgc_memcmp, a first-differing-byte-difference loop.
 *    ISO C only guarantees the SIGN of memcmp's result; the Rust port pins
 *    the byte-difference convention (network/src/lib.rs:564 comment: the int
 *    is wire-visible via network_cmp). This proof therefore checks the
 *    byte-difference reading of C; sign-equivalence is implied. Any libc
 *    whose memcmp returns a different magnitude would differ from BOTH
 *    implementations here in the same way.
 *  - names pgc_-prefixed.
 */

#define IS_HIGHBIT_SET(ch) ((unsigned char)(ch) & 0x80)

static int
pgc_memcmp(const unsigned char *l, const unsigned char *r, int n)
{
	int			i;

	for (i = 0; i < n; i++)
		if (l[i] != r[i])
			return (int) l[i] - (int) r[i];
	return 0;
}

int
pgc_bitncmp(const unsigned char *l, const unsigned char *r, int n)
{
	unsigned int lb,
				rb;
	int			x,
				b;

	b = n / 8;
	x = pgc_memcmp(l, r, b);
	if (x || (n % 8) == 0)
		return x;

	lb = l[b];
	rb = r[b];
	for (b = n % 8; b > 0; b--)
	{
		if (IS_HIGHBIT_SET(lb) != IS_HIGHBIT_SET(rb))
		{
			if (IS_HIGHBIT_SET(lb))
				return 1;
			return -1;
		}
		lb <<= 1;
		rb <<= 1;
	}
	return 0;
}

int
pgc_bitncommon(const unsigned char *l, const unsigned char *r, int n)
{
	int			byte,
				nbits;

	/* number of bits to examine in last byte */
	nbits = n % 8;

	/* check whole bytes */
	for (byte = 0; byte < n / 8; byte++)
	{
		if (l[byte] != r[byte])
		{
			/* at least one bit in the last byte is not common */
			nbits = 7;
			break;
		}
	}

	/* check bits in last partial byte */
	if (nbits != 0)
	{
		/* calculate diff of first non-matching bytes */
		unsigned int diff = l[byte] ^ r[byte];

		/* compare the bits from the most to the least */
		while ((diff >> (8 - nbits)) != 0)
			nbits--;
	}

	return (8 * byte) + nbits;
}
