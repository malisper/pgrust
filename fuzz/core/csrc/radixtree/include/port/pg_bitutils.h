/*
 * REDUCED port/pg_bitutils.h — verbatim pieces only (radixtree_diff oracle).
 *
 * lib/radixtree.h consumes pg_leftmost_one_pos64, pg_rightmost_one_pos32,
 * pg_rightmost_one_pos64, pg_nextpower2_32 (all static inline in the
 * upstream header) and — via nodes/bitmapset.h's bmw_popcount —
 * pg_popcount64. Every body below is BYTE-FOR-BYTE the upstream
 * implementation @ 62d6c7d3df6287f1bd83199c1a746e50d31571a0 (Stamp-18.3)
 * on the config both oracle platforms (macOS arm64 dev, Linux aarch64
 * fleet, both LP64 clang/gcc) actually take:
 *   - HAVE__BUILTIN_CLZ / HAVE__BUILTIN_CTZ / HAVE__BUILTIN_POPCOUNT are
 *     all defined by configure for gcc/clang; SIZEOF_LONG == 8 on LP64.
 *     The taken #if arm of each upstream body is reproduced verbatim; the
 *     dead MSVC/fallback arms are not copied (they reference lookup tables
 *     this oracle never links).
 *   - pg_popcount64: upstream aarch64 links the extern function in
 *     src/port/pg_bitutils.c whose entire body (line 344..347 non-x86 arm)
 *     is `return pg_popcount64_slow(word);`, and pg_popcount64_slow's taken
 *     HAVE__BUILTIN_POPCOUNT/SIZEOF_LONG==8 arm is
 *     `return __builtin_popcountl(word);` (pg_bitutils.c lines 356..362).
 *     That one-line verbatim chain is inlined here as a static inline (same
 *     cc build, no PG build system to route the extern).
 *   - pg_leftmost_one_pos32 is consumed by pg_nextpower2_32's body.
 */
#ifndef PG_BITUTILS_H
#define PG_BITUTILS_H

/* verbatim taken-arm of src/include/port/pg_bitutils.h pg_leftmost_one_pos32 */
static inline int
pg_leftmost_one_pos32(uint32 word)
{
	Assert(word != 0);

	return 31 - __builtin_clz(word);
}

/* verbatim taken-arm of src/include/port/pg_bitutils.h pg_leftmost_one_pos64 */
static inline int
pg_leftmost_one_pos64(uint64 word)
{
	Assert(word != 0);

	return 63 - __builtin_clzl(word);
}

/* verbatim taken-arm of src/include/port/pg_bitutils.h pg_rightmost_one_pos32 */
static inline int
pg_rightmost_one_pos32(uint32 word)
{
	Assert(word != 0);

	return __builtin_ctz(word);
}

/* verbatim taken-arm of src/include/port/pg_bitutils.h pg_rightmost_one_pos64 */
static inline int
pg_rightmost_one_pos64(uint64 word)
{
	Assert(word != 0);

	return __builtin_ctzl(word);
}

/* verbatim: src/include/port/pg_bitutils.h pg_nextpower2_32 (whole body) */
static inline uint32
pg_nextpower2_32(uint32 num)
{
	Assert(num > 0 && num <= PG_UINT32_MAX / 2 + 1);

	/*
	 * A power 2 number has only 1 bit set.  Subtracting 1 from such a number
	 * will turn on all previous bits resulting in no common bits being set
	 * between num and num-1.
	 */
	if ((num & (num - 1)) == 0)
		return num;				/* already power 2 */

	return ((uint32) 1) << (pg_leftmost_one_pos32(num) + 1);
}

/* verbatim taken-arm chain: src/port/pg_bitutils.c pg_popcount64 ->
 * pg_popcount64_slow (see file header) */
static inline int
pg_popcount64(uint64 word)
{
	return __builtin_popcountl(word);
}

#endif							/* PG_BITUTILS_H */
