/*
 * Vendored PostgreSQL C for the numeric compare-shaped sibling rows:
 * numeric_smaller / numeric_larger, numeric_abs / numeric_uminus,
 * hash_numeric / hash_numeric_extended, in_range_numeric_numeric.
 *
 * Provenance:
 *   - src/backend/utils/adt/numeric.c, branch REL_18_STABLE, fetched
 *     2026-07-28 (same ref as ../c/pg_numeric_cmp.c): numeric_smaller,
 *     numeric_larger, numeric_abs, numeric_uminus, hash_numeric,
 *     hash_numeric_extended, in_range_numeric_numeric, and the finite-arm
 *     arithmetic support they need (NumericVar, init_var_from_num,
 *     alloc_var, free_var, zero_var, strip_var, cmp_abs, cmp_abs_common,
 *     cmp_var, cmp_var_common, add_abs, sub_abs, add_var, sub_var) —
 *     bodies VERBATIM except the shims below.
 *   - src/common/hashfn.c, branch REL_18_STABLE, fetched 2026-07-28:
 *     mix()/final()/rot/UINT32_ALIGN_MASK and the full bodies of
 *     hash_bytes / hash_bytes_extended (pg_ prefix; bodies unmodified) —
 *     byte-for-byte the same vendoring as proofs/hash-rows/c/pg_hash_rows.c
 *     (where those kernels are already PROVED against the shipped Rust
 *     hashfn crate); duplicated here so the family crate stays
 *     self-contained.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * SHIMS (plumbing only, never logic; each marked at its site):
 *  1. PACKED-HEADER ACCESS SHIM (same convention as pg_numeric_cmp.c):
 *     functions that only CONSUME a Numeric through the accessor macros
 *     (NUMERIC_SIGN/WEIGHT/DSCALE/DIGITS/NDIGITS and the IS_* predicates)
 *     take the explicit tuple those macros produce; the *_S predicate
 *     macros below rewrite the REL_18 predicates on the explicit sign
 *     value. Consequence: the C side's packed-header decode is OUT of the
 *     theorem for those functions; the Rust side's decode (fed real packed
 *     images built per the on-disk spec) is IN.
 *  2. HEADER-WORD SHIM (numeric_abs / numeric_uminus): these two WRITE the
 *     packed header. Every macro they use (NUMERIC_IS_SHORT/IS_SPECIAL/
 *     IS_NAN, NUMERIC_SIGN, NUMERIC_DSCALE) reads only choice.n_header /
 *     choice.n_long.n_sign_dscale — the SAME first uint16 of the payload —
 *     so here the HDRN_* macros below transcribe the REL_18 macros
 *     verbatim onto an explicit uint16 header parameter, and the C
 *     header-bit decode IS in the theorem. duplicate_numeric() (palloc +
 *     memcpy of the whole image, then the header word is overwritten) is
 *     shimmed to "result header := input header"; the harness asserts the
 *     non-header payload bytes are unchanged on the Rust side, which is
 *     duplicate_numeric's only other effect. NUMERIC_NDIGITS reads VARSIZE
 *     and is passed explicitly (uminus only).
 *  3. FMGR SHIM: PG_FUNCTION_ARGS, PG_GETARG_x, PG_RETURN_x and
 *     PG_FREE_IF_COPY unwrapped to plain C signatures; detoasting out of
 *     scope
 *     (pre-detoasted caller contract, bytea-cmp precedent).
 *     - numeric_smaller/numeric_larger return the WINNING INPUT POINTER;
 *       shimmed to return the winning arg INDEX (0/1) — datetime-cmp
 *       timetz_larger/smaller precedent.
 *     - hash_numeric returns PG_RETURN_UINT32 / PG_RETURN_DATUM(uint32
 *       datum ^ int weight). Datum is typedef'd uint64 as on the target;
 *       the return value is truncated to uint32 exactly as the SQL int4
 *       consumer contract (DatumGetUInt32) does. hash_numeric_extended
 *       returns the uint64 datum unchanged.
 *     - in_range_numeric_numeric: ereport(ERROR ...) -> set *err = 1 and
 *       return 0 (PROOF_EREPORT_FLAG convention); message text out of
 *       proof.
 *  4. ALLOCATOR SHIM: palloc-backed digitbuf_alloc -> bump pointer into a
 *     static NumericDigit arena, reset at pg_in_range_numeric_numeric
 *     entry (the only vendored entry point that allocates); digitbuf_free
 *     -> no-op. An arena overflow would surface as a CBMC out-of-bounds
 *     violation, so the shim cannot silently mask capacity bugs.
 *  5. Types: NumericDigit = int16 -> int16_t; Assert -> no-op; Max/Min
 *     spelled locally; hash_any/hash_any_extended (hashfn.h wrappers over
 *     hash_bytes) transcribed as direct pg_hash_bytes* calls.
 *
 * Postgres compiles with -fwrapv; CBMC's default two's-complement wrap
 * matches, so signed arithmetic is vendored as-is.
 */

#include <stdint.h>
#include <stddef.h>
#include <string.h>
#include <stdbool.h>

typedef int16_t int16;
typedef int32_t int32;
typedef int64_t int64;
typedef uint8_t uint8;
typedef uint16_t uint16;
typedef uint32_t uint32;
typedef uint64_t uint64;
typedef size_t Size;
typedef uint64 Datum;
typedef int16_t NumericDigit;

#define Assert(x) ((void) 0)
#define Max(x, y) ((x) > (y) ? (x) : (y))
#define Min(x, y) ((x) < (y) ? (x) : (y))

/* numeric.c: interpretation of high bits (verbatim values) */
#define NUMERIC_SIGN_MASK	0xC000
#define NUMERIC_POS			0x0000
#define NUMERIC_NEG			0x4000
#define NUMERIC_SHORT		0x8000
#define NUMERIC_SPECIAL		0xC000
#define NUMERIC_EXT_SIGN_MASK	0xF000
#define NUMERIC_NAN				0xC000
#define NUMERIC_PINF			0xD000
#define NUMERIC_NINF			0xF000
#define NUMERIC_INF_SIGN_MASK	0x2000
#define NUMERIC_SHORT_SIGN_MASK			0x2000
#define NUMERIC_SHORT_DSCALE_MASK		0x1F80
#define NUMERIC_SHORT_DSCALE_SHIFT		7
#define NUMERIC_SHORT_WEIGHT_SIGN_MASK	0x0040
#define NUMERIC_SHORT_WEIGHT_MASK		0x003F
#define NUMERIC_DSCALE_MASK			0x3FFF

#define NBASE		10000
#define DEC_DIGITS	4

/* SHIM 1 predicates on an explicit sign value (see pg_numeric_cmp.c) */
#define NUMERIC_IS_SPECIAL_S(sign) (((sign) & NUMERIC_SIGN_MASK) == NUMERIC_SPECIAL)
#define NUMERIC_IS_NAN_S(sign) ((sign) == NUMERIC_NAN)
#define NUMERIC_IS_PINF_S(sign) ((sign) == NUMERIC_PINF)
#define NUMERIC_IS_NINF_S(sign) ((sign) == NUMERIC_NINF)

/*
 * SHIM 2 macros: the REL_18 header macros transcribed verbatim onto an
 * explicit uint16 header word h (== choice.n_short.n_header ==
 * choice.n_long.n_sign_dscale, the first payload uint16):
 *   NUMERIC_FLAGBITS(n)        ((n)->choice.n_header & NUMERIC_SIGN_MASK)
 *   NUMERIC_IS_SHORT(n)        (NUMERIC_FLAGBITS(n) == NUMERIC_SHORT)
 *   NUMERIC_IS_SPECIAL(n)      (NUMERIC_FLAGBITS(n) == NUMERIC_SPECIAL)
 *   NUMERIC_IS_NAN(n)          ((n)->choice.n_header == NUMERIC_NAN)
 *   NUMERIC_HEADER_IS_SHORT(n) (((n)->choice.n_header & 0x8000) != 0)
 *   NUMERIC_SIGN(n), NUMERIC_DSCALE(n): see numeric.c (transcribed below)
 */
#define HDRN_FLAGBITS(h)	((h) & NUMERIC_SIGN_MASK)
#define HDRN_IS_SHORT(h)	(HDRN_FLAGBITS(h) == NUMERIC_SHORT)
#define HDRN_IS_SPECIAL(h)	(HDRN_FLAGBITS(h) == NUMERIC_SPECIAL)
#define HDRN_IS_NAN(h)		((h) == NUMERIC_NAN)
#define HDRN_EXT_FLAGBITS(h) ((h) & NUMERIC_EXT_SIGN_MASK)
#define HDRN_HEADER_IS_SHORT(h) (((h) & 0x8000) != 0)
#define HDRN_SIGN(h) \
	(HDRN_IS_SHORT(h) ? \
		(((h) & NUMERIC_SHORT_SIGN_MASK) ? \
		 NUMERIC_NEG : NUMERIC_POS) : \
		(HDRN_IS_SPECIAL(h) ? \
		 HDRN_EXT_FLAGBITS(h) : HDRN_FLAGBITS(h)))
#define HDRN_DSCALE(h)	(HDRN_HEADER_IS_SHORT(h) ? \
	((h) & NUMERIC_SHORT_DSCALE_MASK) \
		>> NUMERIC_SHORT_DSCALE_SHIFT \
	: ((h) & NUMERIC_DSCALE_MASK))

/* port/pg_bitutils.h (verbatim body) */
static inline uint32
pg_rotate_left32(uint32 word, int n)
{
	return (word << n) | (word >> (32 - n));
}

/* ============ src/common/hashfn.c (verbatim, pg_ prefix) ============ */

/* Get a bit mask of the bits set in non-uint32 aligned addresses */
#define UINT32_ALIGN_MASK (sizeof(uint32) - 1)

#define rot(x,k) pg_rotate_left32(x, k)

/*----------
 * mix -- mix 3 32-bit values reversibly.
 *
 * This is reversible, so any information in (a,b,c) before mix() is
 * still in (a,b,c) after mix().
 *
 * If four pairs of (a,b,c) inputs are run through mix(), or through
 * mix() in reverse, there are at least 32 bits of the output that
 * are sometimes the same for one pair and different for another pair.
 * This was tested for:
 * * pairs that differed by one bit, by two bits, in any combination
 *	 of top bits of (a,b,c), or in any combination of bottom bits of
 *	 (a,b,c).
 * * "differ" is defined as +, -, ^, or ~^.  For + and -, I transformed
 *	 the output delta to a Gray code (a^(a>>1)) so a string of 1's (as
 *	 is commonly produced by subtraction) look like a single 1-bit
 *	 difference.
 * * the base values were pseudorandom, all zero but one bit set, or
 *	 all zero plus a counter that starts at zero.
 *
 * This does not achieve avalanche.  There are input bits of (a,b,c)
 * that fail to affect some output bits of (a,b,c), especially of a.  The
 * most thoroughly mixed value is c, but it doesn't really even achieve
 * avalanche in c.
 *
 * This allows some parallelism.  Read-after-writes are good at doubling
 * the number of bits affected, so the goal of mixing pulls in the opposite
 * direction from the goal of parallelism.  I did what I could.  Rotates
 * seem to cost as much as shifts on every machine I could lay my hands on,
 * and rotates are much kinder to the top and bottom bits, so I used rotates.
 *----------
 */
#define mix(a,b,c) \
{ \
  a -= c;  a ^= rot(c, 4);	c += b; \
  b -= a;  b ^= rot(a, 6);	a += c; \
  c -= b;  c ^= rot(b, 8);	b += a; \
  a -= c;  a ^= rot(c,16);	c += b; \
  b -= a;  b ^= rot(a,19);	a += c; \
  c -= b;  c ^= rot(b, 4);	b += a; \
}

/*----------
 * final -- final mixing of 3 32-bit values (a,b,c) into c
 *
 * Pairs of (a,b,c) values differing in only a few bits will usually
 * produce values of c that look totally different.  This was tested for
 * * pairs that differed by one bit, by two bits, in any combination
 *	 of top bits of (a,b,c), or in any combination of bottom bits of
 *	 (a,b,c).
 * * "differ" is defined as +, -, ^, or ~^.  For + and -, I transformed
 *	 the output delta to a Gray code (a^(a>>1)) so a string of 1's (as
 *	 is commonly produced by subtraction) look like a single 1-bit
 *	 difference.
 * * the base values were pseudorandom, all zero but one bit set, or
 *	 all zero plus a counter that starts at zero.
 *
 * The use of separate functions for mix() and final() allow for a
 * substantial performance increase since final() does not need to
 * do well in reverse, but is does need to affect all output bits.
 * mix(), on the other hand, does not need to affect all output
 * bits (affecting 32 bits is enough).  The original hash function had
 * a single mixing operation that had to satisfy both sets of requirements
 * and was slower as a result.
 *----------
 */
#define final(a,b,c) \
{ \
  c ^= b; c -= rot(b,14); \
  a ^= c; a -= rot(c,11); \
  b ^= a; b -= rot(a,25); \
  c ^= b; c -= rot(b,16); \
  a ^= c; a -= rot(c, 4); \
  b ^= a; b -= rot(a,14); \
  c ^= b; c -= rot(b,24); \
}


/*
 * pg_hash_bytes() -- hash a variable-length key into a 32-bit value
 *		k		: the key (the unaligned variable-length array of bytes)
 *		len		: the length of the key, counting by bytes
 *
 * Returns a uint32 value.  Every bit of the key affects every bit of
 * the return value.  Every 1-bit and 2-bit delta achieves avalanche.
 * About 6*len+35 instructions. The best hash table sizes are powers
 * of 2.  There is no need to do mod a prime (mod is sooo slow!).
 * If you need less than 32 bits, use a bitmask.
 *
 * This procedure must never throw elog(ERROR); the ResourceOwner code
 * relies on this not to fail.
 *
 * Note: we could easily change this function to return a 64-bit hash value
 * by using the final values of both b and c.  b is perhaps a little less
 * well mixed than c, however.
 */
uint32
pg_hash_bytes(const unsigned char *k, int keylen)
{
	uint32		a,
				b,
				c,
				len;

	/* Set up the internal state */
	len = keylen;
	a = b = c = 0x9e3779b9 + len + 3923095;

	/* If the source pointer is word-aligned, we use word-wide fetches */
	if (((uintptr_t) k & UINT32_ALIGN_MASK) == 0)
	{
		/* Code path for aligned source data */
		const uint32 *ka = (const uint32 *) k;

		/* handle most of the key */
		while (len >= 12)
		{
			a += ka[0];
			b += ka[1];
			c += ka[2];
			mix(a, b, c);
			ka += 3;
			len -= 12;
		}

		/* handle the last 11 bytes */
		k = (const unsigned char *) ka;
#ifdef WORDS_BIGENDIAN
		switch (len)
		{
			case 11:
				c += ((uint32) k[10] << 8);
				/* fall through */
			case 10:
				c += ((uint32) k[9] << 16);
				/* fall through */
			case 9:
				c += ((uint32) k[8] << 24);
				/* fall through */
			case 8:
				/* the lowest byte of c is reserved for the length */
				b += ka[1];
				a += ka[0];
				break;
			case 7:
				b += ((uint32) k[6] << 8);
				/* fall through */
			case 6:
				b += ((uint32) k[5] << 16);
				/* fall through */
			case 5:
				b += ((uint32) k[4] << 24);
				/* fall through */
			case 4:
				a += ka[0];
				break;
			case 3:
				a += ((uint32) k[2] << 8);
				/* fall through */
			case 2:
				a += ((uint32) k[1] << 16);
				/* fall through */
			case 1:
				a += ((uint32) k[0] << 24);
				/* case 0: nothing left to add */
		}
#else							/* !WORDS_BIGENDIAN */
		switch (len)
		{
			case 11:
				c += ((uint32) k[10] << 24);
				/* fall through */
			case 10:
				c += ((uint32) k[9] << 16);
				/* fall through */
			case 9:
				c += ((uint32) k[8] << 8);
				/* fall through */
			case 8:
				/* the lowest byte of c is reserved for the length */
				b += ka[1];
				a += ka[0];
				break;
			case 7:
				b += ((uint32) k[6] << 16);
				/* fall through */
			case 6:
				b += ((uint32) k[5] << 8);
				/* fall through */
			case 5:
				b += k[4];
				/* fall through */
			case 4:
				a += ka[0];
				break;
			case 3:
				a += ((uint32) k[2] << 16);
				/* fall through */
			case 2:
				a += ((uint32) k[1] << 8);
				/* fall through */
			case 1:
				a += k[0];
				/* case 0: nothing left to add */
		}
#endif							/* WORDS_BIGENDIAN */
	}
	else
	{
		/* Code path for non-aligned source data */

		/* handle most of the key */
		while (len >= 12)
		{
#ifdef WORDS_BIGENDIAN
			a += (k[3] + ((uint32) k[2] << 8) + ((uint32) k[1] << 16) + ((uint32) k[0] << 24));
			b += (k[7] + ((uint32) k[6] << 8) + ((uint32) k[5] << 16) + ((uint32) k[4] << 24));
			c += (k[11] + ((uint32) k[10] << 8) + ((uint32) k[9] << 16) + ((uint32) k[8] << 24));
#else							/* !WORDS_BIGENDIAN */
			a += (k[0] + ((uint32) k[1] << 8) + ((uint32) k[2] << 16) + ((uint32) k[3] << 24));
			b += (k[4] + ((uint32) k[5] << 8) + ((uint32) k[6] << 16) + ((uint32) k[7] << 24));
			c += (k[8] + ((uint32) k[9] << 8) + ((uint32) k[10] << 16) + ((uint32) k[11] << 24));
#endif							/* WORDS_BIGENDIAN */
			mix(a, b, c);
			k += 12;
			len -= 12;
		}

		/* handle the last 11 bytes */
#ifdef WORDS_BIGENDIAN
		switch (len)
		{
			case 11:
				c += ((uint32) k[10] << 8);
				/* fall through */
			case 10:
				c += ((uint32) k[9] << 16);
				/* fall through */
			case 9:
				c += ((uint32) k[8] << 24);
				/* fall through */
			case 8:
				/* the lowest byte of c is reserved for the length */
				b += k[7];
				/* fall through */
			case 7:
				b += ((uint32) k[6] << 8);
				/* fall through */
			case 6:
				b += ((uint32) k[5] << 16);
				/* fall through */
			case 5:
				b += ((uint32) k[4] << 24);
				/* fall through */
			case 4:
				a += k[3];
				/* fall through */
			case 3:
				a += ((uint32) k[2] << 8);
				/* fall through */
			case 2:
				a += ((uint32) k[1] << 16);
				/* fall through */
			case 1:
				a += ((uint32) k[0] << 24);
				/* case 0: nothing left to add */
		}
#else							/* !WORDS_BIGENDIAN */
		switch (len)
		{
			case 11:
				c += ((uint32) k[10] << 24);
				/* fall through */
			case 10:
				c += ((uint32) k[9] << 16);
				/* fall through */
			case 9:
				c += ((uint32) k[8] << 8);
				/* fall through */
			case 8:
				/* the lowest byte of c is reserved for the length */
				b += ((uint32) k[7] << 24);
				/* fall through */
			case 7:
				b += ((uint32) k[6] << 16);
				/* fall through */
			case 6:
				b += ((uint32) k[5] << 8);
				/* fall through */
			case 5:
				b += k[4];
				/* fall through */
			case 4:
				a += ((uint32) k[3] << 24);
				/* fall through */
			case 3:
				a += ((uint32) k[2] << 16);
				/* fall through */
			case 2:
				a += ((uint32) k[1] << 8);
				/* fall through */
			case 1:
				a += k[0];
				/* case 0: nothing left to add */
		}
#endif							/* WORDS_BIGENDIAN */
	}

	final(a, b, c);

	/* report the result */
	return c;
}

/*
 * pg_hash_bytes_extended() -- hash into a 64-bit value, using an optional seed
 *		k		: the key (the unaligned variable-length array of bytes)
 *		len		: the length of the key, counting by bytes
 *		seed	: a 64-bit seed (0 means no seed)
 *
 * Returns a uint64 value.  Otherwise similar to pg_hash_bytes.
 */
uint64
pg_hash_bytes_extended(const unsigned char *k, int keylen, uint64 seed)
{
	uint32		a,
				b,
				c,
				len;

	/* Set up the internal state */
	len = keylen;
	a = b = c = 0x9e3779b9 + len + 3923095;

	/* If the seed is non-zero, use it to perturb the internal state. */
	if (seed != 0)
	{
		/*
		 * In essence, the seed is treated as part of the data being hashed,
		 * but for simplicity, we pretend that it's padded with four bytes of
		 * zeroes so that the seed constitutes a 12-byte chunk.
		 */
		a += (uint32) (seed >> 32);
		b += (uint32) seed;
		mix(a, b, c);
	}

	/* If the source pointer is word-aligned, we use word-wide fetches */
	if (((uintptr_t) k & UINT32_ALIGN_MASK) == 0)
	{
		/* Code path for aligned source data */
		const uint32 *ka = (const uint32 *) k;

		/* handle most of the key */
		while (len >= 12)
		{
			a += ka[0];
			b += ka[1];
			c += ka[2];
			mix(a, b, c);
			ka += 3;
			len -= 12;
		}

		/* handle the last 11 bytes */
		k = (const unsigned char *) ka;
#ifdef WORDS_BIGENDIAN
		switch (len)
		{
			case 11:
				c += ((uint32) k[10] << 8);
				/* fall through */
			case 10:
				c += ((uint32) k[9] << 16);
				/* fall through */
			case 9:
				c += ((uint32) k[8] << 24);
				/* fall through */
			case 8:
				/* the lowest byte of c is reserved for the length */
				b += ka[1];
				a += ka[0];
				break;
			case 7:
				b += ((uint32) k[6] << 8);
				/* fall through */
			case 6:
				b += ((uint32) k[5] << 16);
				/* fall through */
			case 5:
				b += ((uint32) k[4] << 24);
				/* fall through */
			case 4:
				a += ka[0];
				break;
			case 3:
				a += ((uint32) k[2] << 8);
				/* fall through */
			case 2:
				a += ((uint32) k[1] << 16);
				/* fall through */
			case 1:
				a += ((uint32) k[0] << 24);
				/* case 0: nothing left to add */
		}
#else							/* !WORDS_BIGENDIAN */
		switch (len)
		{
			case 11:
				c += ((uint32) k[10] << 24);
				/* fall through */
			case 10:
				c += ((uint32) k[9] << 16);
				/* fall through */
			case 9:
				c += ((uint32) k[8] << 8);
				/* fall through */
			case 8:
				/* the lowest byte of c is reserved for the length */
				b += ka[1];
				a += ka[0];
				break;
			case 7:
				b += ((uint32) k[6] << 16);
				/* fall through */
			case 6:
				b += ((uint32) k[5] << 8);
				/* fall through */
			case 5:
				b += k[4];
				/* fall through */
			case 4:
				a += ka[0];
				break;
			case 3:
				a += ((uint32) k[2] << 16);
				/* fall through */
			case 2:
				a += ((uint32) k[1] << 8);
				/* fall through */
			case 1:
				a += k[0];
				/* case 0: nothing left to add */
		}
#endif							/* WORDS_BIGENDIAN */
	}
	else
	{
		/* Code path for non-aligned source data */

		/* handle most of the key */
		while (len >= 12)
		{
#ifdef WORDS_BIGENDIAN
			a += (k[3] + ((uint32) k[2] << 8) + ((uint32) k[1] << 16) + ((uint32) k[0] << 24));
			b += (k[7] + ((uint32) k[6] << 8) + ((uint32) k[5] << 16) + ((uint32) k[4] << 24));
			c += (k[11] + ((uint32) k[10] << 8) + ((uint32) k[9] << 16) + ((uint32) k[8] << 24));
#else							/* !WORDS_BIGENDIAN */
			a += (k[0] + ((uint32) k[1] << 8) + ((uint32) k[2] << 16) + ((uint32) k[3] << 24));
			b += (k[4] + ((uint32) k[5] << 8) + ((uint32) k[6] << 16) + ((uint32) k[7] << 24));
			c += (k[8] + ((uint32) k[9] << 8) + ((uint32) k[10] << 16) + ((uint32) k[11] << 24));
#endif							/* WORDS_BIGENDIAN */
			mix(a, b, c);
			k += 12;
			len -= 12;
		}

		/* handle the last 11 bytes */
#ifdef WORDS_BIGENDIAN
		switch (len)
		{
			case 11:
				c += ((uint32) k[10] << 8);
				/* fall through */
			case 10:
				c += ((uint32) k[9] << 16);
				/* fall through */
			case 9:
				c += ((uint32) k[8] << 24);
				/* fall through */
			case 8:
				/* the lowest byte of c is reserved for the length */
				b += k[7];
				/* fall through */
			case 7:
				b += ((uint32) k[6] << 8);
				/* fall through */
			case 6:
				b += ((uint32) k[5] << 16);
				/* fall through */
			case 5:
				b += ((uint32) k[4] << 24);
				/* fall through */
			case 4:
				a += k[3];
				/* fall through */
			case 3:
				a += ((uint32) k[2] << 8);
				/* fall through */
			case 2:
				a += ((uint32) k[1] << 16);
				/* fall through */
			case 1:
				a += ((uint32) k[0] << 24);
				/* case 0: nothing left to add */
		}
#else							/* !WORDS_BIGENDIAN */
		switch (len)
		{
			case 11:
				c += ((uint32) k[10] << 24);
				/* fall through */
			case 10:
				c += ((uint32) k[9] << 16);
				/* fall through */
			case 9:
				c += ((uint32) k[8] << 8);
				/* fall through */
			case 8:
				/* the lowest byte of c is reserved for the length */
				b += ((uint32) k[7] << 24);
				/* fall through */
			case 7:
				b += ((uint32) k[6] << 16);
				/* fall through */
			case 6:
				b += ((uint32) k[5] << 8);
				/* fall through */
			case 5:
				b += k[4];
				/* fall through */
			case 4:
				a += ((uint32) k[3] << 24);
				/* fall through */
			case 3:
				a += ((uint32) k[2] << 16);
				/* fall through */
			case 2:
				a += ((uint32) k[1] << 8);
				/* fall through */
			case 1:
				a += k[0];
				/* case 0: nothing left to add */
		}
#endif							/* WORDS_BIGENDIAN */
	}

	final(a, b, c);

	/* report the result */
	return ((uint64) b << 32) | c;
}

/* ============ src/backend/utils/adt/numeric.c ============ */

/*
 * hash_numeric() — body VERBATIM from REL_18_STABLE numeric.c under SHIMS
 * 1/3/5: the Numeric arg is the explicit (sign, weight, digits, ndigits)
 * tuple; hash_any -> pg_hash_bytes; PG_RETURN_UINT32(x) -> return x;
 * the final PG_RETURN_DATUM(result) is truncated to uint32 per the SQL
 * int4 consumer contract (DatumGetUInt32).
 */
uint32
pg_hash_numeric(int key_sign, int key_weight,
				const NumericDigit *key_digits, int key_ndigits)
{
	Datum		digit_hash;
	Datum		result;
	int			weight;
	int			start_offset;
	int			end_offset;
	int			i;
	int			hash_len;
	const NumericDigit *digits;

	/* If it's NaN or infinity, don't try to hash the rest of the fields */
	if (NUMERIC_IS_SPECIAL_S(key_sign))
		return (uint32) 0;

	weight = key_weight;
	start_offset = 0;
	end_offset = 0;

	/*
	 * Omit any leading or trailing zeros from the input to the hash. The
	 * numeric implementation *should* guarantee that leading and trailing
	 * zeros are suppressed, but we're paranoid. Note that we measure the
	 * starting and ending offsets in units of NumericDigits, not bytes.
	 */
	digits = key_digits;
	for (i = 0; i < key_ndigits; i++)
	{
		if (digits[i] != (NumericDigit) 0)
			break;

		start_offset++;

		/*
		 * The weight is effectively the # of digits before the decimal
		 * point, so decrement it for each leading zero we skip.
		 */
		weight--;
	}

	/*
	 * If there are no non-zero digits, then the value of the number is zero,
	 * regardless of any other fields.
	 */
	if (key_ndigits == start_offset)
		return (uint32) -1;

	for (i = key_ndigits - 1; i >= 0; i--)
	{
		if (digits[i] != (NumericDigit) 0)
			break;

		end_offset++;
	}

	/* If we get here, there should be at least one non-zero digit */
	Assert(start_offset + end_offset < key_ndigits);

	/*
	 * Note that we don't hash on the Numeric's scale, since two numerics can
	 * compare equal but have different scales. We also don't hash on the
	 * sign, although we could: since a sign difference implies inequality,
	 * this shouldn't affect correctness.
	 */
	hash_len = key_ndigits - start_offset - end_offset;
	digit_hash = (Datum) pg_hash_bytes((const unsigned char *) (key_digits + start_offset),
									   hash_len * sizeof(NumericDigit));

	/* Mix in the weight, via XOR */
	result = digit_hash ^ weight;

	return (uint32) result;
}

/*
 * hash_numeric_extended() — body VERBATIM under the same shims;
 * PG_GETARG_INT64 seed -> explicit uint64 param, PG_RETURN_UINT64 /
 * PG_RETURN_DATUM -> raw uint64 return (UInt64GetDatum/DatumGetUInt64 are
 * identities on this target).
 */
uint64
pg_hash_numeric_extended(int key_sign, int key_weight,
						 const NumericDigit *key_digits, int key_ndigits,
						 uint64 seed)
{
	Datum		digit_hash;
	Datum		result;
	int			weight;
	int			start_offset;
	int			end_offset;
	int			i;
	int			hash_len;
	const NumericDigit *digits;

	/* If it's NaN or infinity, don't try to hash the rest of the fields */
	if (NUMERIC_IS_SPECIAL_S(key_sign))
		return seed;

	weight = key_weight;
	start_offset = 0;
	end_offset = 0;

	digits = key_digits;
	for (i = 0; i < key_ndigits; i++)
	{
		if (digits[i] != (NumericDigit) 0)
			break;

		start_offset++;

		weight--;
	}

	if (key_ndigits == start_offset)
		return seed - 1;

	for (i = key_ndigits - 1; i >= 0; i--)
	{
		if (digits[i] != (NumericDigit) 0)
			break;

		end_offset++;
	}

	Assert(start_offset + end_offset < key_ndigits);

	hash_len = key_ndigits - start_offset - end_offset;
	digit_hash = (Datum) pg_hash_bytes_extended((const unsigned char *) (key_digits
																		 + start_offset),
												hash_len * sizeof(NumericDigit),
												seed);

	result = digit_hash ^ weight;

	return (uint64) result;
}

/*
 * numeric_abs() — body VERBATIM under SHIM 2: operates on the explicit
 * uint16 header word; duplicate_numeric -> res_header = header; the
 * n_header / n_sign_dscale stores (the same first uint16) -> res_header.
 */
uint16
pg_numeric_abs_hdr(uint16 num_header)
{
	uint16		res_header;

	/*
	 * Do it the easy way directly on the packed format
	 */
	res_header = num_header;	/* duplicate_numeric */

	if (HDRN_IS_SHORT(num_header))
		res_header =
			num_header & ~NUMERIC_SHORT_SIGN_MASK;
	else if (HDRN_IS_SPECIAL(num_header))
	{
		/* This changes -Inf to Inf, and doesn't affect NaN */
		res_header =
			num_header & ~NUMERIC_INF_SIGN_MASK;
	}
	else
		res_header = NUMERIC_POS | HDRN_DSCALE(num_header);

	return res_header;
}

/*
 * numeric_uminus() — body VERBATIM under SHIM 2; NUMERIC_NDIGITS (a
 * VARSIZE read) is the explicit ndigits param.
 */
uint16
pg_numeric_uminus_hdr(uint16 num_header, int num_ndigits)
{
	uint16		res_header;

	/*
	 * Do it the easy way directly on the packed format
	 */
	res_header = num_header;	/* duplicate_numeric */

	if (HDRN_IS_SPECIAL(num_header))
	{
		/* Flip the sign, if it's Inf or -Inf */
		if (!HDRN_IS_NAN(num_header))
			res_header =
				num_header ^ NUMERIC_INF_SIGN_MASK;
	}

	/*
	 * The packed format is known to be totally zero digit trimmed always. So
	 * once we've eliminated specials, we can identify a zero by the fact that
	 * there are no digits at all. Do nothing to a zero.
	 */
	else if (num_ndigits != 0)
	{
		/* Else, flip the sign */
		if (HDRN_IS_SHORT(num_header))
			res_header =
				num_header ^ NUMERIC_SHORT_SIGN_MASK;
		else if (HDRN_SIGN(num_header) == NUMERIC_POS)
			res_header =
				NUMERIC_NEG | HDRN_DSCALE(num_header);
		else
			res_header =
				NUMERIC_POS | HDRN_DSCALE(num_header);
	}

	return res_header;
}

/*
 * numeric_smaller() / numeric_larger() — bodies VERBATIM from
 * REL_18_STABLE numeric.c under SHIMS 1/3: each Numeric is the explicit
 * spec tuple; PG_RETURN_NUMERIC(numN) -> return the winning arg INDEX
 * (0 for num1, 1 for num2), datetime-cmp timetz precedent.
 * cmp_numerics is the already-vendored pg_cmp_numerics from
 * pg_numeric_cmp.c (compiled into the same goto-program).
 */
extern int pg_cmp_numerics(int sign1, int weight1, const NumericDigit *digits1, int ndigits1,
						   int sign2, int weight2, const NumericDigit *digits2, int ndigits2);

int
pg_numeric_smaller(int sign1, int weight1, const NumericDigit *digits1, int ndigits1,
				   int sign2, int weight2, const NumericDigit *digits2, int ndigits2)
{
	/*
	 * Use cmp_numerics so that this will agree with the comparison operators,
	 * particularly as regards comparisons involving NaN.
	 */
	if (pg_cmp_numerics(sign1, weight1, digits1, ndigits1,
						sign2, weight2, digits2, ndigits2) < 0)
		return 0;
	else
		return 1;
}

int
pg_numeric_larger(int sign1, int weight1, const NumericDigit *digits1, int ndigits1,
				  int sign2, int weight2, const NumericDigit *digits2, int ndigits2)
{
	/*
	 * Use cmp_numerics so that this will agree with the comparison operators,
	 * particularly as regards comparisons involving NaN.
	 */
	if (pg_cmp_numerics(sign1, weight1, digits1, ndigits1,
						sign2, weight2, digits2, ndigits2) > 0)
		return 0;
	else
		return 1;
}

/* ======== NumericVar machinery for in_range (bodies verbatim) ======== */

/* numeric.c NumericVar (verbatim struct) */
typedef struct NumericVar
{
	int			ndigits;		/* # of digits in digits[] - can be 0! */
	int			weight;			/* weight of first digit */
	int			sign;			/* NUMERIC_POS, _NEG, _NAN, _PINF, or _NINF */
	int			dscale;			/* display scale */
	NumericDigit *buf;			/* start of palloc'd space for digits[] */
	NumericDigit *digits;		/* base-NBASE digits */
} NumericVar;

/* SHIM 4: palloc-backed digit buffers -> static bump arena */
#define PG_DIGIT_ARENA_CAP 64
static NumericDigit pg_digit_arena[PG_DIGIT_ARENA_CAP];
static int	pg_digit_arena_used = 0;

static NumericDigit *
pg_digitbuf_alloc_shim(int ndigits)
{
	NumericDigit *p = pg_digit_arena + pg_digit_arena_used;

	/* an overflow would be caught by CBMC as an out-of-bounds access */
	pg_digit_arena_used += ndigits;
	return p;
}

#define digitbuf_alloc(ndigits)  pg_digitbuf_alloc_shim(ndigits)
#define digitbuf_free(buf)	((void) 0)

#define init_var(v)		memset(v, 0, sizeof(NumericVar))

/*
 * init_var_from_num() — the packed-header reads (NUMERIC_NDIGITS/WEIGHT/
 * SIGN/DSCALE/DIGITS) are the explicit tuple per SHIM 1.
 */
static void
pg_init_var_from_parts(NumericVar *dest, int sign, int weight, int dscale,
					   const NumericDigit *digits, int ndigits)
{
	dest->ndigits = ndigits;
	dest->weight = weight;
	dest->sign = sign;
	dest->dscale = dscale;
	dest->digits = (NumericDigit *) digits;
	dest->buf = NULL;			/* digits array is not palloc'd */
}

/* free_var() — body verbatim (digitbuf_free is the SHIM-4 no-op) */
static void
free_var(NumericVar *var)
{
	digitbuf_free(var->buf);
	var->buf = NULL;
	var->digits = NULL;
	var->sign = NUMERIC_NAN;
}

/* zero_var() — body verbatim */
static void
zero_var(NumericVar *var)
{
	digitbuf_free(var->buf);
	var->buf = NULL;
	var->digits = NULL;
	var->ndigits = 0;
	var->weight = 0;			/* by convention; doesn't really matter */
	var->sign = NUMERIC_POS;	/* anything but NAN... */
}

/* strip_var() — body verbatim */
static void
strip_var(NumericVar *var)
{
	NumericDigit *digits = var->digits;
	int			ndigits = var->ndigits;

	/* Strip leading zeroes */
	while (ndigits > 0 && *digits == 0)
	{
		digits++;
		var->weight--;
		ndigits--;
	}

	/* Strip trailing zeroes */
	while (ndigits > 0 && digits[ndigits - 1] == 0)
		ndigits--;

	/* If it's zero, normalize the sign and weight */
	if (ndigits == 0)
	{
		var->sign = NUMERIC_POS;
		var->weight = 0;
	}

	var->digits = digits;
	var->ndigits = ndigits;
}

/* cmp_abs_common() — body verbatim */
static int
cmp_abs_common(const NumericDigit *var1digits, int var1ndigits, int var1weight,
			   const NumericDigit *var2digits, int var2ndigits, int var2weight)
{
	int			i1 = 0;
	int			i2 = 0;

	/* Check any digits before the first common digit */

	while (var1weight > var2weight && i1 < var1ndigits)
	{
		if (var1digits[i1++] != 0)
			return 1;
		var1weight--;
	}
	while (var2weight > var1weight && i2 < var2ndigits)
	{
		if (var2digits[i2++] != 0)
			return -1;
		var2weight--;
	}

	/* At this point, either w1 == w2 or we've run out of digits */

	if (var1weight == var2weight)
	{
		while (i1 < var1ndigits && i2 < var2ndigits)
		{
			int			stat = var1digits[i1++] - var2digits[i2++];

			if (stat)
			{
				if (stat > 0)
					return 1;
				return -1;
			}
		}
	}

	/*
	 * At this point, we've run out of digits on one side or the other; so any
	 * remaining nonzero digits imply that side is larger
	 */
	while (i1 < var1ndigits)
	{
		if (var1digits[i1++] != 0)
			return 1;
	}
	while (i2 < var2ndigits)
	{
		if (var2digits[i2++] != 0)
			return -1;
	}

	return 0;
}

/* cmp_abs() — body verbatim */
static int
cmp_abs(const NumericVar *var1, const NumericVar *var2)
{
	return cmp_abs_common(var1->digits, var1->ndigits, var1->weight,
						  var2->digits, var2->ndigits, var2->weight);
}

/* cmp_var_common() — body verbatim */
static int
cmp_var_common(const NumericDigit *var1digits, int var1ndigits,
			   int var1weight, int var1sign,
			   const NumericDigit *var2digits, int var2ndigits,
			   int var2weight, int var2sign)
{
	if (var1ndigits == 0)
	{
		if (var2ndigits == 0)
			return 0;
		if (var2sign == NUMERIC_NEG)
			return 1;
		return -1;
	}
	if (var2ndigits == 0)
	{
		if (var1sign == NUMERIC_POS)
			return 1;
		return -1;
	}

	if (var1sign == NUMERIC_POS)
	{
		if (var2sign == NUMERIC_NEG)
			return 1;
		return cmp_abs_common(var1digits, var1ndigits, var1weight,
							  var2digits, var2ndigits, var2weight);
	}

	if (var2sign == NUMERIC_POS)
		return -1;

	return cmp_abs_common(var2digits, var2ndigits, var2weight,
						  var1digits, var1ndigits, var1weight);
}

/* cmp_var() — body verbatim */
static int
cmp_var(const NumericVar *var1, const NumericVar *var2)
{
	return cmp_var_common(var1->digits, var1->ndigits,
						  var1->weight, var1->sign,
						  var2->digits, var2->ndigits,
						  var2->weight, var2->sign);
}

/* add_abs() — body verbatim */
static void
add_abs(const NumericVar *var1, const NumericVar *var2, NumericVar *result)
{
	NumericDigit *res_buf;
	NumericDigit *res_digits;
	int			res_ndigits;
	int			res_weight;
	int			res_rscale,
				rscale1,
				rscale2;
	int			res_dscale;
	int			i,
				i1,
				i2;
	int			carry = 0;

	/* copy these values into local vars for speed in inner loop */
	int			var1ndigits = var1->ndigits;
	int			var2ndigits = var2->ndigits;
	NumericDigit *var1digits = var1->digits;
	NumericDigit *var2digits = var2->digits;

	res_weight = Max(var1->weight, var2->weight) + 1;

	res_dscale = Max(var1->dscale, var2->dscale);

	/* Note: here we are figuring rscale in base-NBASE digits */
	rscale1 = var1->ndigits - var1->weight - 1;
	rscale2 = var2->ndigits - var2->weight - 1;
	res_rscale = Max(rscale1, rscale2);

	res_ndigits = res_rscale + res_weight + 1;
	if (res_ndigits <= 0)
		res_ndigits = 1;

	res_buf = digitbuf_alloc(res_ndigits + 1);
	res_buf[0] = 0;				/* spare digit for later rounding */
	res_digits = res_buf + 1;

	i1 = res_rscale + var1->weight + 1;
	i2 = res_rscale + var2->weight + 1;
	for (i = res_ndigits - 1; i >= 0; i--)
	{
		i1--;
		i2--;
		if (i1 >= 0 && i1 < var1ndigits)
			carry += var1digits[i1];
		if (i2 >= 0 && i2 < var2ndigits)
			carry += var2digits[i2];

		if (carry >= NBASE)
		{
			res_digits[i] = carry - NBASE;
			carry = 1;
		}
		else
		{
			res_digits[i] = carry;
			carry = 0;
		}
	}

	Assert(carry == 0);			/* else we failed to allow for carry out */

	digitbuf_free(result->buf);
	result->ndigits = res_ndigits;
	result->buf = res_buf;
	result->digits = res_digits;
	result->weight = res_weight;
	result->dscale = res_dscale;

	/* Remove leading/trailing zeroes */
	strip_var(result);
}

/* sub_abs() — body verbatim */
static void
sub_abs(const NumericVar *var1, const NumericVar *var2, NumericVar *result)
{
	NumericDigit *res_buf;
	NumericDigit *res_digits;
	int			res_ndigits;
	int			res_weight;
	int			res_rscale,
				rscale1,
				rscale2;
	int			res_dscale;
	int			i,
				i1,
				i2;
	int			borrow = 0;

	/* copy these values into local vars for speed in inner loop */
	int			var1ndigits = var1->ndigits;
	int			var2ndigits = var2->ndigits;
	NumericDigit *var1digits = var1->digits;
	NumericDigit *var2digits = var2->digits;

	res_weight = var1->weight;

	res_dscale = Max(var1->dscale, var2->dscale);

	/* Note: here we are figuring rscale in base-NBASE digits */
	rscale1 = var1->ndigits - var1->weight - 1;
	rscale2 = var2->ndigits - var2->weight - 1;
	res_rscale = Max(rscale1, rscale2);

	res_ndigits = res_rscale + res_weight + 1;
	if (res_ndigits <= 0)
		res_ndigits = 1;

	res_buf = digitbuf_alloc(res_ndigits + 1);
	res_buf[0] = 0;				/* spare digit for later rounding */
	res_digits = res_buf + 1;

	i1 = res_rscale + var1->weight + 1;
	i2 = res_rscale + var2->weight + 1;
	for (i = res_ndigits - 1; i >= 0; i--)
	{
		i1--;
		i2--;
		if (i1 >= 0 && i1 < var1ndigits)
			borrow += var1digits[i1];
		if (i2 >= 0 && i2 < var2ndigits)
			borrow -= var2digits[i2];

		if (borrow < 0)
		{
			res_digits[i] = borrow + NBASE;
			borrow = -1;
		}
		else
		{
			res_digits[i] = borrow;
			borrow = 0;
		}
	}

	Assert(borrow == 0);		/* else caller gave us var1 < var2 */

	digitbuf_free(result->buf);
	result->ndigits = res_ndigits;
	result->buf = res_buf;
	result->digits = res_digits;
	result->weight = res_weight;
	result->dscale = res_dscale;

	/* Remove leading/trailing zeroes */
	strip_var(result);
}

/* add_var() — body verbatim */
static void
add_var(const NumericVar *var1, const NumericVar *var2, NumericVar *result)
{
	/*
	 * Decide on the signs of the two variables what to do
	 */
	if (var1->sign == NUMERIC_POS)
	{
		if (var2->sign == NUMERIC_POS)
		{
			/*
			 * Both are positive result = +(ABS(var1) + ABS(var2))
			 */
			add_abs(var1, var2, result);
			result->sign = NUMERIC_POS;
		}
		else
		{
			/*
			 * var1 is positive, var2 is negative Must compare absolute values
			 */
			switch (cmp_abs(var1, var2))
			{
				case 0:
					/* ----------
					 * ABS(var1) == ABS(var2)
					 * result = ZERO
					 * ----------
					 */
					zero_var(result);
					result->dscale = Max(var1->dscale, var2->dscale);
					break;

				case 1:
					/* ----------
					 * ABS(var1) > ABS(var2)
					 * result = +(ABS(var1) - ABS(var2))
					 * ----------
					 */
					sub_abs(var1, var2, result);
					result->sign = NUMERIC_POS;
					break;

				case -1:
					/* ----------
					 * ABS(var1) < ABS(var2)
					 * result = -(ABS(var2) - ABS(var1))
					 * ----------
					 */
					sub_abs(var2, var1, result);
					result->sign = NUMERIC_NEG;
					break;
			}
		}
	}
	else
	{
		if (var2->sign == NUMERIC_POS)
		{
			/* ----------
			 * var1 is negative, var2 is positive
			 * Must compare absolute values
			 * ----------
			 */
			switch (cmp_abs(var1, var2))
			{
				case 0:
					/* ----------
					 * ABS(var1) == ABS(var2)
					 * result = ZERO
					 * ----------
					 */
					zero_var(result);
					result->dscale = Max(var1->dscale, var2->dscale);
					break;

				case 1:
					/* ----------
					 * ABS(var1) > ABS(var2)
					 * result = -(ABS(var1) - ABS(var2))
					 * ----------
					 */
					sub_abs(var1, var2, result);
					result->sign = NUMERIC_NEG;
					break;

				case -1:
					/* ----------
					 * ABS(var1) < ABS(var2)
					 * result = +(ABS(var2) - ABS(var1))
					 * ----------
					 */
					sub_abs(var2, var1, result);
					result->sign = NUMERIC_POS;
					break;
			}
		}
		else
		{
			/* ----------
			 * Both are negative
			 * result = -(ABS(var1) + ABS(var2))
			 * ----------
			 */
			add_abs(var1, var2, result);
			result->sign = NUMERIC_NEG;
		}
	}
}

/* sub_var() — body verbatim */
static void
sub_var(const NumericVar *var1, const NumericVar *var2, NumericVar *result)
{
	/*
	 * Decide on the signs of the two variables what to do
	 */
	if (var1->sign == NUMERIC_POS)
	{
		if (var2->sign == NUMERIC_NEG)
		{
			/* ----------
			 * var1 is positive, var2 is negative
			 * result = +(ABS(var1) + ABS(var2))
			 * ----------
			 */
			add_abs(var1, var2, result);
			result->sign = NUMERIC_POS;
		}
		else
		{
			/* ----------
			 * Both are positive
			 * Must compare absolute values
			 * ----------
			 */
			switch (cmp_abs(var1, var2))
			{
				case 0:
					/* ----------
					 * ABS(var1) == ABS(var2)
					 * result = ZERO
					 * ----------
					 */
					zero_var(result);
					result->dscale = Max(var1->dscale, var2->dscale);
					break;

				case 1:
					/* ----------
					 * ABS(var1) > ABS(var2)
					 * result = +(ABS(var1) - ABS(var2))
					 * ----------
					 */
					sub_abs(var1, var2, result);
					result->sign = NUMERIC_POS;
					break;

				case -1:
					/* ----------
					 * ABS(var1) < ABS(var2)
					 * result = -(ABS(var2) - ABS(var1))
					 * ----------
					 */
					sub_abs(var2, var1, result);
					result->sign = NUMERIC_NEG;
					break;
			}
		}
	}
	else
	{
		if (var2->sign == NUMERIC_NEG)
		{
			/* ----------
			 * Both are negative
			 * Must compare absolute values
			 * ----------
			 */
			switch (cmp_abs(var1, var2))
			{
				case 0:
					/* ----------
					 * ABS(var1) == ABS(var2)
					 * result = ZERO
					 * ----------
					 */
					zero_var(result);
					result->dscale = Max(var1->dscale, var2->dscale);
					break;

				case 1:
					/* ----------
					 * ABS(var1) > ABS(var2)
					 * result = -(ABS(var1) - ABS(var2))
					 * ----------
					 */
					sub_abs(var1, var2, result);
					result->sign = NUMERIC_NEG;
					break;

				case -1:
					/* ----------
					 * ABS(var1) < ABS(var2)
					 * result = +(ABS(var2) - ABS(var1))
					 * ----------
					 */
					sub_abs(var2, var1, result);
					result->sign = NUMERIC_POS;
					break;
			}
		}
		else
		{
			/* ----------
			 * var1 is negative, var2 is positive
			 * result = -(ABS(var2) + ABS(var1))
			 * ----------
			 */
			add_abs(var1, var2, result);
			result->sign = NUMERIC_NEG;
		}
	}
}

/*
 * in_range_numeric_numeric() — body VERBATIM from REL_18_STABLE numeric.c
 * under SHIMS 1/3/4: each Numeric arg is the explicit spec tuple
 * (sign, weight, dscale, digits, ndigits); the NUMERIC_IS_* predicates and
 * NUMERIC_SIGN read the explicit sign; init_var_from_num ->
 * pg_init_var_from_parts on the same tuple; ereport(ERROR ...) ->
 * *err = 1, return 0 (message text out of proof); PG_FREE_IF_COPY /
 * PG_RETURN_BOOL unwrapped.
 */
int
pg_in_range_numeric_numeric(int val_sign, int val_weight, int val_dscale,
							const NumericDigit *val_digits, int val_ndigits,
							int base_sign, int base_weight, int base_dscale,
							const NumericDigit *base_digits, int base_ndigits,
							int offset_sign, int offset_weight, int offset_dscale,
							const NumericDigit *offset_digits, int offset_ndigits,
							int sub, int less, int *err)
{
	bool		result;

	*err = 0;
	pg_digit_arena_used = 0;	/* SHIM 4 arena reset */

	/*
	 * Reject negative (including -Inf) or NaN offset.  Negative is per spec,
	 * and NaN is because appropriate semantics for that seem non-obvious.
	 */
	if (NUMERIC_IS_NAN_S(offset_sign) ||
		NUMERIC_IS_NINF_S(offset_sign) ||
		offset_sign == NUMERIC_NEG)
	{
		*err = 1;				/* ereport(ERROR, ERRCODE_INVALID_PRECEDING_
								 * OR_FOLLOWING_SIZE) */
		return 0;
	}

	/*
	 * Deal with cases where val and/or base is NaN, following the rule that
	 * NaN sorts after non-NaN (cf cmp_numerics).  The offset cannot affect
	 * the conclusion.
	 */
	if (NUMERIC_IS_NAN_S(val_sign))
	{
		if (NUMERIC_IS_NAN_S(base_sign))
			result = true;		/* NAN = NAN */
		else
			result = !less;		/* NAN > non-NAN */
	}
	else if (NUMERIC_IS_NAN_S(base_sign))
	{
		result = less;			/* non-NAN < NAN */
	}

	/*
	 * Deal with infinite offset (necessarily +Inf, at this point).
	 */
	else if (NUMERIC_IS_SPECIAL_S(offset_sign))
	{
		Assert(NUMERIC_IS_PINF_S(offset_sign));
		if (sub ? NUMERIC_IS_PINF_S(base_sign) : NUMERIC_IS_NINF_S(base_sign))
		{
			/*
			 * base +/- offset would produce NaN, so return true for any val
			 * (see in_range_float8_float8() for reasoning).
			 */
			result = true;
		}
		else if (sub)
		{
			/* base - offset must be -inf */
			if (less)
				result = NUMERIC_IS_NINF_S(val_sign);	/* only -inf is <= sum */
			else
				result = true;	/* any val is >= sum */
		}
		else
		{
			/* base + offset must be +inf */
			if (less)
				result = true;	/* any val is <= sum */
			else
				result = NUMERIC_IS_PINF_S(val_sign);	/* only +inf is >= sum */
		}
	}

	/*
	 * Deal with cases where val and/or base is infinite.  The offset, being
	 * now known finite, cannot affect the conclusion.
	 */
	else if (NUMERIC_IS_SPECIAL_S(val_sign))
	{
		if (NUMERIC_IS_PINF_S(val_sign))
		{
			if (NUMERIC_IS_PINF_S(base_sign))
				result = true;	/* PINF = PINF */
			else
				result = !less; /* PINF > any other non-NAN */
		}
		else					/* val must be NINF */
		{
			if (NUMERIC_IS_NINF_S(base_sign))
				result = true;	/* NINF = NINF */
			else
				result = less;	/* NINF < anything else */
		}
	}
	else if (NUMERIC_IS_SPECIAL_S(base_sign))
	{
		if (NUMERIC_IS_NINF_S(base_sign))
			result = !less;		/* normal > NINF */
		else
			result = less;		/* normal < PINF */
	}
	else
	{
		/*
		 * Otherwise go ahead and compute base +/- offset.  While it's
		 * possible for this to overflow the numeric format, it's unlikely
		 * enough that we don't take measures to prevent it.
		 */
		NumericVar	valv;
		NumericVar	basev;
		NumericVar	offsetv;
		NumericVar	sum;

		pg_init_var_from_parts(&valv, val_sign, val_weight, val_dscale,
							   val_digits, val_ndigits);
		pg_init_var_from_parts(&basev, base_sign, base_weight, base_dscale,
							   base_digits, base_ndigits);
		pg_init_var_from_parts(&offsetv, offset_sign, offset_weight, offset_dscale,
							   offset_digits, offset_ndigits);
		init_var(&sum);

		if (sub)
			sub_var(&basev, &offsetv, &sum);
		else
			add_var(&basev, &offsetv, &sum);

		if (less)
			result = (cmp_var(&valv, &sum) <= 0);
		else
			result = (cmp_var(&valv, &sum) >= 0);

		free_var(&sum);
	}

	return (int) result;
}
