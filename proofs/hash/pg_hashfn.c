/*
 * Vendored VERBATIM from PostgreSQL master:
 *   https://raw.githubusercontent.com/postgres/postgres/master/src/common/hashfn.c
 *   (fetched 2026-07-28; Portions Copyright (c) 1996-2026, PostgreSQL Global
 *   Development Group; Portions Copyright (c) 1994, Regents of the University
 *   of California)
 * REL_18_STABLE conformance: zero code drift vs REL_18_STABLE (provenance
 * audit, proofs/PROVENANCE-AUDIT.md, 2026-07-28).
 * Contains ONLY: the mix()/final()/rot macros and the function bodies of
 * hash_bytes, hash_bytes_extended, hash_bytes_uint32,
 * hash_bytes_uint32_extended -- bodies unmodified, functions renamed with a
 * pg_ prefix to avoid clashing with the Rust port under verification.
 * Preamble below replaces postgres.h/pg_bitutils.h with the minimal
 * definitions those bodies need (uint32/uint64/pg_rotate_left32/
 * pg_fallthrough), matching the C definitions exactly.
 */
#include <stdint.h>

typedef uint32_t uint32;
typedef uint64_t uint64;

/* port/pg_bitutils.h */
static inline uint32
pg_rotate_left32(uint32 word, int n)
{
	return (word << n) | (word >> (32 - n));
}

/* c.h: pg_fallthrough */
#define pg_fallthrough ((void) 0)

/* Get a bit mask of the bits set in non-uint32 aligned addresses */
#define UINT32_ALIGN_MASK (sizeof(uint32) - 1)

#define rot(x,k) pg_rotate_left32(x, k)

#define mix(a,b,c) \
{ \
  a -= c;  a ^= rot(c, 4);	c += b; \
  b -= a;  b ^= rot(a, 6);	a += c; \
  c -= b;  c ^= rot(b, 8);	b += a; \
  a -= c;  a ^= rot(c,16);	c += b; \
  b -= a;  b ^= rot(a,19);	a += c; \
  c -= b;  c ^= rot(b, 4);	b += a; \
}


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
				pg_fallthrough;
			case 10:
				c += ((uint32) k[9] << 16);
				pg_fallthrough;
			case 9:
				c += ((uint32) k[8] << 24);
				pg_fallthrough;
			case 8:
				/* the lowest byte of c is reserved for the length */
				b += ka[1];
				a += ka[0];
				break;
			case 7:
				b += ((uint32) k[6] << 8);
				pg_fallthrough;
			case 6:
				b += ((uint32) k[5] << 16);
				pg_fallthrough;
			case 5:
				b += ((uint32) k[4] << 24);
				pg_fallthrough;
			case 4:
				a += ka[0];
				break;
			case 3:
				a += ((uint32) k[2] << 8);
				pg_fallthrough;
			case 2:
				a += ((uint32) k[1] << 16);
				pg_fallthrough;
			case 1:
				a += ((uint32) k[0] << 24);
				/* case 0: nothing left to add */
		}
#else							/* !WORDS_BIGENDIAN */
		switch (len)
		{
			case 11:
				c += ((uint32) k[10] << 24);
				pg_fallthrough;
			case 10:
				c += ((uint32) k[9] << 16);
				pg_fallthrough;
			case 9:
				c += ((uint32) k[8] << 8);
				pg_fallthrough;
			case 8:
				/* the lowest byte of c is reserved for the length */
				b += ka[1];
				a += ka[0];
				break;
			case 7:
				b += ((uint32) k[6] << 16);
				pg_fallthrough;
			case 6:
				b += ((uint32) k[5] << 8);
				pg_fallthrough;
			case 5:
				b += k[4];
				pg_fallthrough;
			case 4:
				a += ka[0];
				break;
			case 3:
				a += ((uint32) k[2] << 16);
				pg_fallthrough;
			case 2:
				a += ((uint32) k[1] << 8);
				pg_fallthrough;
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
				pg_fallthrough;
			case 10:
				c += ((uint32) k[9] << 16);
				pg_fallthrough;
			case 9:
				c += ((uint32) k[8] << 24);
				pg_fallthrough;
			case 8:
				/* the lowest byte of c is reserved for the length */
				b += k[7];
				pg_fallthrough;
			case 7:
				b += ((uint32) k[6] << 8);
				pg_fallthrough;
			case 6:
				b += ((uint32) k[5] << 16);
				pg_fallthrough;
			case 5:
				b += ((uint32) k[4] << 24);
				pg_fallthrough;
			case 4:
				a += k[3];
				pg_fallthrough;
			case 3:
				a += ((uint32) k[2] << 8);
				pg_fallthrough;
			case 2:
				a += ((uint32) k[1] << 16);
				pg_fallthrough;
			case 1:
				a += ((uint32) k[0] << 24);
				/* case 0: nothing left to add */
		}
#else							/* !WORDS_BIGENDIAN */
		switch (len)
		{
			case 11:
				c += ((uint32) k[10] << 24);
				pg_fallthrough;
			case 10:
				c += ((uint32) k[9] << 16);
				pg_fallthrough;
			case 9:
				c += ((uint32) k[8] << 8);
				pg_fallthrough;
			case 8:
				/* the lowest byte of c is reserved for the length */
				b += ((uint32) k[7] << 24);
				pg_fallthrough;
			case 7:
				b += ((uint32) k[6] << 16);
				pg_fallthrough;
			case 6:
				b += ((uint32) k[5] << 8);
				pg_fallthrough;
			case 5:
				b += k[4];
				pg_fallthrough;
			case 4:
				a += ((uint32) k[3] << 24);
				pg_fallthrough;
			case 3:
				a += ((uint32) k[2] << 16);
				pg_fallthrough;
			case 2:
				a += ((uint32) k[1] << 8);
				pg_fallthrough;
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
				pg_fallthrough;
			case 10:
				c += ((uint32) k[9] << 16);
				pg_fallthrough;
			case 9:
				c += ((uint32) k[8] << 24);
				pg_fallthrough;
			case 8:
				/* the lowest byte of c is reserved for the length */
				b += ka[1];
				a += ka[0];
				break;
			case 7:
				b += ((uint32) k[6] << 8);
				pg_fallthrough;
			case 6:
				b += ((uint32) k[5] << 16);
				pg_fallthrough;
			case 5:
				b += ((uint32) k[4] << 24);
				pg_fallthrough;
			case 4:
				a += ka[0];
				break;
			case 3:
				a += ((uint32) k[2] << 8);
				pg_fallthrough;
			case 2:
				a += ((uint32) k[1] << 16);
				pg_fallthrough;
			case 1:
				a += ((uint32) k[0] << 24);
				/* case 0: nothing left to add */
		}
#else							/* !WORDS_BIGENDIAN */
		switch (len)
		{
			case 11:
				c += ((uint32) k[10] << 24);
				pg_fallthrough;
			case 10:
				c += ((uint32) k[9] << 16);
				pg_fallthrough;
			case 9:
				c += ((uint32) k[8] << 8);
				pg_fallthrough;
			case 8:
				/* the lowest byte of c is reserved for the length */
				b += ka[1];
				a += ka[0];
				break;
			case 7:
				b += ((uint32) k[6] << 16);
				pg_fallthrough;
			case 6:
				b += ((uint32) k[5] << 8);
				pg_fallthrough;
			case 5:
				b += k[4];
				pg_fallthrough;
			case 4:
				a += ka[0];
				break;
			case 3:
				a += ((uint32) k[2] << 16);
				pg_fallthrough;
			case 2:
				a += ((uint32) k[1] << 8);
				pg_fallthrough;
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
				pg_fallthrough;
			case 10:
				c += ((uint32) k[9] << 16);
				pg_fallthrough;
			case 9:
				c += ((uint32) k[8] << 24);
				pg_fallthrough;
			case 8:
				/* the lowest byte of c is reserved for the length */
				b += k[7];
				pg_fallthrough;
			case 7:
				b += ((uint32) k[6] << 8);
				pg_fallthrough;
			case 6:
				b += ((uint32) k[5] << 16);
				pg_fallthrough;
			case 5:
				b += ((uint32) k[4] << 24);
				pg_fallthrough;
			case 4:
				a += k[3];
				pg_fallthrough;
			case 3:
				a += ((uint32) k[2] << 8);
				pg_fallthrough;
			case 2:
				a += ((uint32) k[1] << 16);
				pg_fallthrough;
			case 1:
				a += ((uint32) k[0] << 24);
				/* case 0: nothing left to add */
		}
#else							/* !WORDS_BIGENDIAN */
		switch (len)
		{
			case 11:
				c += ((uint32) k[10] << 24);
				pg_fallthrough;
			case 10:
				c += ((uint32) k[9] << 16);
				pg_fallthrough;
			case 9:
				c += ((uint32) k[8] << 8);
				pg_fallthrough;
			case 8:
				/* the lowest byte of c is reserved for the length */
				b += ((uint32) k[7] << 24);
				pg_fallthrough;
			case 7:
				b += ((uint32) k[6] << 16);
				pg_fallthrough;
			case 6:
				b += ((uint32) k[5] << 8);
				pg_fallthrough;
			case 5:
				b += k[4];
				pg_fallthrough;
			case 4:
				a += ((uint32) k[3] << 24);
				pg_fallthrough;
			case 3:
				a += ((uint32) k[2] << 16);
				pg_fallthrough;
			case 2:
				a += ((uint32) k[1] << 8);
				pg_fallthrough;
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

uint32
pg_hash_bytes_uint32(uint32 k)
{
	uint32		a,
				b,
				c;

	a = b = c = 0x9e3779b9 + (uint32) sizeof(uint32) + 3923095;
	a += k;

	final(a, b, c);

	/* report the result */
	return c;
}

uint64
pg_hash_bytes_uint32_extended(uint32 k, uint64 seed)
{
	uint32		a,
				b,
				c;

	a = b = c = 0x9e3779b9 + (uint32) sizeof(uint32) + 3923095;

	if (seed != 0)
	{
		a += (uint32) (seed >> 32);
		b += (uint32) seed;
		mix(a, b, c);
	}

	a += k;

	final(a, b, c);

	/* report the result */
	return ((uint64) b << 32) | c;
}
