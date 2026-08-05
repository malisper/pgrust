/*
 * REDUCED port/pg_bitutils.h — for the libfam_diff differential-fuzz oracle.
 *
 * NOT the full PostgreSQL header. Every function/table body below is
 * VERBATIM from postgres-src @ 62d6c7d3df6287f1bd83199c1a746e50d31571a0
 * (Stamp-18.3), reduced to exactly what csrc/libfam/vendor/{hyperloglog,
 * bloomfilter}.c consume:
 *
 *   - pg_leftmost_one_pos32: src/include/port/pg_bitutils.h lines 34..65,
 *     byte-for-byte. HAVE__BUILTIN_CLZ is defined below (the arm every
 *     supported gcc/clang target compiles).
 *   - pg_number_of_ones: src/port/pg_bitutils.c lines 81..104, byte-for-byte.
 *     LINKAGE NOTE: declared static here (upstream: extern const) so this
 *     single-TU vendoring cannot collide with other oracle TUs; bytes of the
 *     table are untouched.
 *   - pg_popcount64_slow: src/port/pg_bitutils.c lines 374..400, byte-for-byte
 *     (HAVE__BUILTIN_POPCOUNT + SIZEOF_LONG==8 arm, defined below).
 *   - pg_popcount_slow: src/port/pg_bitutils.c lines 402..447, byte-for-byte.
 *   - pg_popcount_optimized: src/port/pg_bitutils.c lines 520..528 (the
 *     !TRY_POPCNT_X86_64 && !POPCNT_AARCH64 portable arm), byte-for-byte
 *     except LINKAGE (static, same note as above). ARM NOTE: real aarch64
 *     PostgreSQL builds route pg_popcount() to the Neon implementation;
 *     bit-population count is input-deterministic and arm-invariant (all
 *     arms compute the same mathematical function), so the portable arm is
 *     the oracle here. pg_popcount32_slow / masked variants are not consumed
 *     by these files and are omitted.
 *   - pg_popcount (static inline dispatcher): src/include/port/pg_bitutils.h
 *     lines 353..385, byte-for-byte.
 *
 * Config macros below mirror pg_config.h on every supported LP64
 * gcc/clang target (plumbing, never logic).
 */
#ifndef PG_BITUTILS_H
#define PG_BITUTILS_H

#define HAVE__BUILTIN_CLZ 1
#define HAVE__BUILTIN_POPCOUNT 1
#define SIZEOF_LONG 8
#define SIZEOF_VOID_P 8


/*
 * pg_leftmost_one_pos32
 *		Returns the position of the most significant set bit in "word",
 *		measured from the least significant bit.  word must not be 0.
 */
static inline int
pg_leftmost_one_pos32(uint32 word)
{
#ifdef HAVE__BUILTIN_CLZ
	Assert(word != 0);

	return 31 - __builtin_clz(word);
#elif defined(_MSC_VER)
	unsigned long result;
	bool		non_zero;

	Assert(word != 0);

	non_zero = _BitScanReverse(&result, word);
	return (int) result;
#else
	int			shift = 32 - 8;

	Assert(word != 0);

	while ((word >> shift) == 0)
		shift -= 8;

	return shift + pg_leftmost_one_pos[(word >> shift) & 255];
#endif							/* HAVE__BUILTIN_CLZ */
}

/* ---- src/port/pg_bitutils.c lines 81..104 (VERBATIM body; static linkage, see header) ---- */
static
/*
 * Array giving the number of 1-bits in each possible byte value.
 *
 * Note: we export this for use by functions in which explicit use
 * of the popcount functions seems unlikely to be a win.
 */
const uint8 pg_number_of_ones[256] = {
	0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4,
	1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
	1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
	1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
	3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
	4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8
};

/* ---- src/port/pg_bitutils.c lines 374..400 (VERBATIM) ---- */
/*
 * pg_popcount64_slow
 *		Return the number of 1 bits set in word
 */
static inline int
pg_popcount64_slow(uint64 word)
{
#ifdef HAVE__BUILTIN_POPCOUNT
#if SIZEOF_LONG == 8
	return __builtin_popcountl(word);
#elif SIZEOF_LONG_LONG == 8
	return __builtin_popcountll(word);
#else
#error "cannot find integer of the same size as uint64_t"
#endif
#else							/* !HAVE__BUILTIN_POPCOUNT */
	int			result = 0;

	while (word != 0)
	{
		result += pg_number_of_ones[word & 255];
		word >>= 8;
	}

	return result;
#endif							/* HAVE__BUILTIN_POPCOUNT */
}

/* ---- src/port/pg_bitutils.c lines 402..447 (VERBATIM) ---- */
/*
 * pg_popcount_slow
 *		Returns the number of 1-bits in buf
 */
static uint64
pg_popcount_slow(const char *buf, int bytes)
{
	uint64		popcnt = 0;

#if SIZEOF_VOID_P >= 8
	/* Process in 64-bit chunks if the buffer is aligned. */
	if (buf == (const char *) TYPEALIGN(8, buf))
	{
		const uint64 *words = (const uint64 *) buf;

		while (bytes >= 8)
		{
			popcnt += pg_popcount64_slow(*words++);
			bytes -= 8;
		}

		buf = (const char *) words;
	}
#else
	/* Process in 32-bit chunks if the buffer is aligned. */
	if (buf == (const char *) TYPEALIGN(4, buf))
	{
		const uint32 *words = (const uint32 *) buf;

		while (bytes >= 4)
		{
			popcnt += pg_popcount32_slow(*words++);
			bytes -= 4;
		}

		buf = (const char *) words;
	}
#endif

	/* Process any remaining bytes */
	while (bytes--)
		popcnt += pg_number_of_ones[(unsigned char) *buf++];

	return popcnt;
}

/* ---- src/port/pg_bitutils.c lines 520..528 (VERBATIM body; static linkage, see header) ---- */
static
/*
 * pg_popcount_optimized
 *		Returns the number of 1-bits in buf
 */
uint64
pg_popcount_optimized(const char *buf, int bytes)
{
	return pg_popcount_slow(buf, bytes);
}

/* ---- src/include/port/pg_bitutils.h lines 353..385 (VERBATIM) ---- */
/*
 * Returns the number of 1-bits in buf.
 *
 * If there aren't many bytes to process, the function call overhead of the
 * optimized versions isn't worth taking, so we inline a loop that consults
 * pg_number_of_ones in that case.  If there are many bytes to process, we
 * accept the function call overhead because the optimized versions are likely
 * to be faster.
 */
static inline uint64
pg_popcount(const char *buf, int bytes)
{
	/*
	 * We set the threshold to the point at which we'll first use special
	 * instructions in the optimized version.
	 */
#if SIZEOF_VOID_P >= 8
	int			threshold = 8;
#else
	int			threshold = 4;
#endif

	if (bytes < threshold)
	{
		uint64		popcnt = 0;

		while (bytes--)
			popcnt += pg_number_of_ones[(unsigned char) *buf++];
		return popcnt;
	}

	return pg_popcount_optimized(buf, bytes);
}

#endif							/* PG_BITUTILS_H */
