/*
 * Vendored VERBATIM from PostgreSQL REL_18_STABLE (fetched 2026-07-28):
 *   https://raw.githubusercontent.com/postgres/postgres/REL_18_STABLE/src/common/hashfn.c
 *     -- mix()/final()/rot/UINT32_ALIGN_MASK macros and the full bodies of
 *        hash_bytes, hash_bytes_extended, hash_bytes_uint32,
 *        hash_bytes_uint32_extended (renamed with a pg_ prefix; bodies
 *        unmodified).
 *   https://raw.githubusercontent.com/postgres/postgres/REL_18_STABLE/src/backend/access/hash/hashfunc.c
 *     -- hashchar/hashint2/hashint4/hashint8/hashoid/hashenum/hashfloat4/
 *        hashfloat8/hashoidvector/hashname/hashtext (+ all *extended)
 *   https://raw.githubusercontent.com/postgres/postgres/REL_18_STABLE/src/backend/utils/adt/varchar.c
 *     -- hashbpchar/hashbpcharextended + bpchartruelen (bcTruelen core)
 *   https://raw.githubusercontent.com/postgres/postgres/REL_18_STABLE/src/backend/utils/adt/mac.c
 *   https://raw.githubusercontent.com/postgres/postgres/REL_18_STABLE/src/backend/utils/adt/mac8.c
 *     -- hashmacaddr/hashmacaddr8 (+ extended)
 *   https://raw.githubusercontent.com/postgres/postgres/REL_18_STABLE/src/backend/utils/adt/bool.c
 *     -- hashbool/hashboolextended
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * SHIMS (plumbing only, never logic; each marked at its site):
 *  1. postgres.h / c.h / pg_bitutils.h replaced by the minimal typedef
 *     preamble below; pg_rotate_left32 is the verbatim pg_bitutils.h body.
 *  2. fmgr PG_FUNCTION_ARGS unwrapping -> plain C signatures around the
 *     verbatim bodies; PG_RETURN_x, UInt32GetDatum, UInt64GetDatum -> raw
 *     uint32/uint64 return values (Datum packing stays in the tested tier;
 *     the Rust harness compares Datum::as_u32()/as_u64()).
 *  3. hash_any/hash_any_extended/hash_uint32/hash_uint32_extended are the
 *     hashfn.h static-inline wrappers, transcribed with the raw-integer
 *     return shim of (2).
 *  4. get_float8_nan() (utils/float.h) -> return (float8) NAN; verbatim body.
 *  5. hashtext/hashbpchar: the collation checks (!collid error) and the
 *     pg_newlocale_from_collation()->deterministic dispatch are OUTSIDE the
 *     provable core (locale seam). Vendored here is the DETERMINISTIC arm
 *     over pre-detoasted (data,len) -- the post-PG_GETARG_*_PP caller
 *     contract, bytea-cmp precedent. The Rust harness pins fncollation to
 *     C_COLLATION_OID (950), which both sides classify deterministic; the
 *     nondeterministic pg_strnxfrm arm and the collid==0 error arm leave the
 *     proof (error-arm verdict parity is a separate Rust-side harness).
 *  6. hashname: NameStr + strlen -> bounded NUL scan pg_name_strlen (NameData
 *     contract: 64-byte block, NUL-terminated within it).
 *  7. hashoidvector: check_valid_oidvector (oid.c ereport gate) is a
 *     precondition here (harness constructs only valid oidvectors: ndim==1,
 *     dataoffset==0, elemtype==OIDOID); body takes (values,dim1) directly --
 *     the layout the Rust wrapper's oidvector_bytes reads.
 *  8. hashmacaddr/hashmacaddr8: PG_GETARG_MACADDR_P pointer -> const uint8*;
 *     sizeof(macaddr)==6 / sizeof(macaddr8)==8 (all-char structs, no
 *     padding) spelled as literal 6/8.
 *
 * Postgres compiles with -fwrapv; CBMC's default two's-complement wrap
 * matches, so signed arithmetic is vendored as-is.
 */
#include <stdint.h>
#include <stddef.h>
#include <math.h>
#include <string.h>

typedef int16_t int16;
typedef int32_t int32;
typedef int64_t int64;
typedef uint8_t uint8;
typedef uint16_t uint16;
typedef uint32_t uint32;
typedef uint64_t uint64;
typedef uint32 Oid;
typedef size_t Size;
typedef float float4;
typedef double float8;

/* port/pg_bitutils.h (verbatim body) */
static inline uint32
pg_rotate_left32(uint32 word, int n)
{
	return (word << n) | (word >> (32 - n));
}

/* ================== src/common/hashfn.c (verbatim, pg_ prefix) ============ */


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

/*
 * pg_hash_bytes_uint32() -- hash a 32-bit value to a 32-bit value
 *
 * This has the same result as
 *		pg_hash_bytes(&k, sizeof(uint32))
 * but is faster and doesn't force the caller to store k into memory.
 */
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

/*
 * pg_hash_bytes_uint32_extended() -- hash 32-bit value to 64-bit value, with seed
 *
 * Like pg_hash_bytes_uint32, this is a convenience function.
 */
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

/* ============ hashfn.h static-inline wrappers (shim 3: raw returns) ======= */

static inline uint32
hash_any(const unsigned char *k, int keylen)
{
	return pg_hash_bytes(k, keylen);
}

static inline uint64
hash_any_extended(const unsigned char *k, int keylen, uint64 seed)
{
	return pg_hash_bytes_extended(k, keylen, seed);
}

static inline uint32
hash_uint32(uint32 k)
{
	return pg_hash_bytes_uint32(k);
}

static inline uint64
hash_uint32_extended(uint32 k, uint64 seed)
{
	return pg_hash_bytes_uint32_extended(k, seed);
}

/*
 * utils/float.h get_float8_nan (shim 4). Upstream body is `return (float8)
 * NAN;` -- but CBMC lowers the NAN macro to __builtin_nanf's 0.0f/0.0f,
 * which trips CBMC's NaN-on-division check and carries an unspecified
 * payload. Spelled instead as the exact IEEE-754 bit pattern gcc/clang
 * produce for (float8) NAN on every platform PG supports (positive quiet
 * NaN, 0x7FF8000000000000) -- the very "standard float8 NaN" the hashfloat*
 * comments demand.
 */
static inline float8
get_float8_nan(void)
{
	uint64		bits = UINT64_C(0x7FF8000000000000);
	float8		result;

	memcpy(&result, &bits, sizeof(result));
	return result;
}

/* ========== access/hash/hashfunc.c rows (shim 2: plain signatures) ======== */

/* Note: this is used for both "char" and boolean datatypes */
uint32
pg_hashchar(char c)
{
	return hash_uint32((int32) c);
}

uint64
pg_hashcharextended(char c, int64 seed)
{
	return hash_uint32_extended((int32) c, seed);
}

uint32
pg_hashint2(int16 v)
{
	return hash_uint32((int32) v);
}

uint64
pg_hashint2extended(int16 v, int64 seed)
{
	return hash_uint32_extended((int32) v, seed);
}

uint32
pg_hashint4(int32 v)
{
	return hash_uint32(v);
}

uint64
pg_hashint4extended(int32 v, int64 seed)
{
	return hash_uint32_extended(v, seed);
}

uint32
pg_hashint8(int64 val)
{
	/*
	 * The idea here is to produce a hash value compatible with the values
	 * produced by hashint4 and hashint2 for logically equal inputs; this is
	 * necessary to support cross-type hash joins across these input types.
	 * Since all three types are signed, we can xor the high half of the int8
	 * value if the sign is positive, or the complement of the high half when
	 * the sign is negative.
	 */
	uint32		lohalf = (uint32) val;
	uint32		hihalf = (uint32) (val >> 32);

	lohalf ^= (val >= 0) ? hihalf : ~hihalf;

	return hash_uint32(lohalf);
}

uint64
pg_hashint8extended(int64 val, int64 seed)
{
	/* Same approach as hashint8 */
	uint32		lohalf = (uint32) val;
	uint32		hihalf = (uint32) (val >> 32);

	lohalf ^= (val >= 0) ? hihalf : ~hihalf;

	return hash_uint32_extended(lohalf, seed);
}

/* hashoid; also the verbatim body of hashenum (identical text) */
uint32
pg_hashoid(Oid o)
{
	return hash_uint32((uint32) o);
}

uint64
pg_hashoidextended(Oid o, int64 seed)
{
	return hash_uint32_extended((uint32) o, seed);
}

/* utils/adt/bool.c */
uint32
pg_hashbool(_Bool b)
{
	return hash_uint32((int32) b);
}

uint64
pg_hashboolextended(_Bool b, int64 seed)
{
	return hash_uint32_extended((int32) b, seed);
}

uint32
pg_hashfloat4(float4 key)
{
	float8		key8;

	/*
	 * On IEEE-float machines, minus zero and zero have different bit patterns
	 * but should compare as equal.  We must ensure that they have the same
	 * hash value, which is most reliably done this way:
	 */
	if (key == (float4) 0)
		return 0;				/* PG_RETURN_UINT32(0) */

	/*
	 * To support cross-type hashing of float8 and float4, we want to return
	 * the same hash value hashfloat8 would produce for an equal float8 value.
	 * So, widen the value to float8 and hash that.
	 */
	key8 = key;

	/*
	 * Similarly, NaNs can have different bit patterns but they should all
	 * compare as equal.  For backwards-compatibility reasons we force them to
	 * have the hash value of a standard float8 NaN.
	 */
	if (isnan(key8))
		key8 = get_float8_nan();

	return hash_any((unsigned char *) &key8, sizeof(key8));
}

uint64
pg_hashfloat4extended(float4 key, int64 seed_in)
{
	uint64		seed = seed_in;
	float8		key8;

	/* Same approach as hashfloat4 */
	if (key == (float4) 0)
		return seed;			/* PG_RETURN_UINT64(seed) */
	key8 = key;
	if (isnan(key8))
		key8 = get_float8_nan();

	return hash_any_extended((unsigned char *) &key8, sizeof(key8), seed);
}

uint32
pg_hashfloat8(float8 key)
{
	/*
	 * On IEEE-float machines, minus zero and zero have different bit patterns
	 * but should compare as equal.  We must ensure that they have the same
	 * hash value, which is most reliably done this way:
	 */
	if (key == (float8) 0)
		return 0;				/* PG_RETURN_UINT32(0) */

	/*
	 * Similarly, NaNs can have different bit patterns but they should all
	 * compare as equal.  For backwards-compatibility reasons we force them to
	 * have the hash value of a standard NaN.
	 */
	if (isnan(key))
		key = get_float8_nan();

	return hash_any((unsigned char *) &key, sizeof(key));
}

uint64
pg_hashfloat8extended(float8 key, int64 seed_in)
{
	uint64		seed = seed_in;

	/* Same approach as hashfloat8 */
	if (key == (float8) 0)
		return seed;			/* PG_RETURN_UINT64(seed) */
	if (isnan(key))
		key = get_float8_nan();

	return hash_any_extended((unsigned char *) &key, sizeof(key), seed);
}

/*
 * hashoidvector (shim 7): check_valid_oidvector is a harness precondition;
 * (values, dim1) is the validated oidvector's payload.
 */
uint32
pg_hashoidvector(const Oid *values, int32 dim1)
{
	return hash_any((unsigned char *) values, dim1 * sizeof(Oid));
}

uint64
pg_hashoidvectorextended(const Oid *values, int32 dim1, int64 seed)
{
	return hash_any_extended((unsigned char *) values,
							 dim1 * sizeof(Oid),
							 seed);
}

/* hashname (shim 6): strlen over a NUL-terminated 64-byte NameData block */
static int
pg_name_strlen(const char *key)
{
	int			len = 0;

	while (len < 64 && key[len] != '\0')
		len++;
	return len;
}

uint32
pg_hashname(const char *key)
{
	return hash_any((unsigned char *) key, pg_name_strlen(key));
}

uint64
pg_hashnameextended(const char *key, int64 seed)
{
	return hash_any_extended((unsigned char *) key, pg_name_strlen(key),
							 seed);
}

/* hashtext deterministic arm (shim 5) over (VARDATA_ANY, VARSIZE_ANY_EXHDR) */
uint32
pg_hashtext_det(const char *keydata, int keylen)
{
	return hash_any((unsigned char *) keydata, keylen);
}

uint64
pg_hashtextextended_det(const char *keydata, int keylen, int64 seed)
{
	return hash_any_extended((unsigned char *) keydata, keylen, seed);
}

/* utils/adt/varchar.c bpchartruelen -- verbatim body (bcTruelen core) */
static int
pg_bpchartruelen(const char *s, int len)
{
	int			i;

	/*
	 * Note that we rely on the assumption that ' ' is a singleton unit on
	 * every supported multibyte server encoding.
	 */
	for (i = len - 1; i >= 0; i--)
	{
		if (s[i] != ' ')
			break;
	}
	return i + 1;
}

/* hashbpchar deterministic arm (shim 5): bcTruelen trim IN-THEOREM */
uint32
pg_hashbpchar_det(const char *keydata, int len)
{
	int			keylen = pg_bpchartruelen(keydata, len);

	return hash_any((unsigned char *) keydata, keylen);
}

uint64
pg_hashbpcharextended_det(const char *keydata, int len, int64 seed)
{
	int			keylen = pg_bpchartruelen(keydata, len);

	return hash_any_extended((unsigned char *) keydata, keylen, seed);
}

/* utils/adt/mac.c (shim 8): key = 6-byte macaddr block */
uint32
pg_hashmacaddr(const uint8 *key)
{
	return hash_any((unsigned char *) key, 6);
}

uint64
pg_hashmacaddrextended(const uint8 *key, int64 seed)
{
	return hash_any_extended((unsigned char *) key, 6, seed);
}

/* utils/adt/mac8.c (shim 8): key = 8-byte macaddr8 block */
uint32
pg_hashmacaddr8(const uint8 *key)
{
	return hash_any((unsigned char *) key, 8);
}

uint64
pg_hashmacaddr8extended(const uint8 *key, int64 seed)
{
	return hash_any_extended((unsigned char *) key, 8, seed);
}

/* ==================================================================== */
/* WAVE 5 (2026-07-28): the remaining scalar hash pg_proc rows.          */
/* Provenance (fetched 2026-07-28, REL_18_STABLE):                       */
/*   src/backend/utils/adt/tid.c   (hashtid/hashtidextended bodies —     */
/*      hash_any over sizeof(BlockIdData) + sizeof(OffsetNumber) = 6     */
/*      raw ItemPointerData bytes, deliberately NOT sizeof(struct))      */
/*   src/backend/utils/adt/xid.c   (hashxid/hashxidextended = hash_uint32 */
/*      of the xid; hashxid8/hashxid8extended = the hashint8 high-word   */
/*      fold over the xid8's u64; hashcid/hashcidextended = hash_uint32) */
/*   src/backend/utils/adt/pg_lsn.c (pg_lsn_hash/_extended: "return      */
/*      hashint8(fcinfo)" — the verbatim hashint8 fold over the LSN)     */
/* Shims: same conventions as the rows above (fmgr unwrap -> plain args; */
/* hashtid's ItemPointer arg = the three u16 fields, assembled into the  */
/* same 6-byte block the on-tuple image carries — shim 6/8 pattern).     */
/* ==================================================================== */

/* tid.c hashtid: hash the 6 component-field bytes of ItemPointerData */
uint32
pg_hashtid(const uint8 *key /* 6-byte tid block */ )
{
	return hash_any((unsigned char *) key, 6);
}

uint64
pg_hashtidextended(const uint8 *key, int64 seed)
{
	return hash_any_extended((unsigned char *) key, 6, seed);
}

/* xid.c hashxid: return hash_uint32(PG_GETARG_TRANSACTIONID(0)) */
uint32
pg_hashxid(uint32 xid)
{
	return hash_uint32(xid);
}

uint64
pg_hashxidextended(uint32 xid, int64 seed)
{
	return hash_uint32_extended(xid, seed);
}

/* xid.c hashcid: return hash_uint32(PG_GETARG_COMMANDID(0)) */
uint32
pg_hashcid(uint32 cid)
{
	return hash_uint32(cid);
}

uint64
pg_hashcidextended(uint32 cid, int64 seed)
{
	return hash_uint32_extended(cid, seed);
}

/* xid.c hashxid8: hashint8's sign-aware high-word fold over the u64 */
uint32
pg_hashxid8(uint64 x)
{
	int64		val = (int64) x;
	uint32		lohalf = (uint32) val;
	uint32		hihalf = (uint32) (val >> 32);

	lohalf ^= (val >= 0) ? hihalf : ~hihalf;

	return hash_uint32(lohalf);
}

uint64
pg_hashxid8extended(uint64 x, int64 seed)
{
	int64		val = (int64) x;
	uint32		lohalf = (uint32) val;
	uint32		hihalf = (uint32) (val >> 32);

	lohalf ^= (val >= 0) ? hihalf : ~hihalf;

	return hash_uint32_extended(lohalf, seed);
}

/* pg_lsn.c pg_lsn_hash: return hashint8(fcinfo) — same fold */
uint32
pg_pg_lsn_hash(uint64 lsn)
{
	return pg_hashxid8(lsn);
}

uint64
pg_pg_lsn_hash_extended(uint64 lsn, int64 seed)
{
	return pg_hashxid8extended(lsn, seed);
}
