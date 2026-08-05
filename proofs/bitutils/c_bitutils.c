/*
 * Vendored PORTABLE implementations from PostgreSQL master:
 *   src/include/port/pg_bitutils.h  (static inline word-level functions)
 *   src/port/pg_bitutils.c          (lookup tables + buffer popcount)
 *
 * CONFIG CHOICE (documented): we take the FALLBACK branches of the
 * HAVE__BUILTIN_CLZ / HAVE__BUILTIN_CTZ / _MSC_VER ifdefs -- i.e. the
 * portable table/loop-based semantics.  Those are the reference
 * semantics PostgreSQL defines for all platforms; the builtin branches
 * are just faster encodings of the same function.
 *
 * PROVENANCE NOTE (audit 2026-07-28, proofs/PROVENANCE-AUDIT.md): the
 * pg_popcount32/64 bodies below are MASTER's branchless bithack;
 * REL_18_STABLE's fallback (pg_popcount32/64_slow) instead uses
 * __builtin_popcount / a pg_number_of_ones byte-walk. Same function
 * computed — value-equivalent, and the proof target (Rust intrinsics ≡
 * popcount) is unaffected; the vendored text is master-only. Re-vendor
 * the REL_18 _slow bodies at next code touch, or keep with this note.
 * All other functions in this file match REL_18_STABLE. The uncompiled
 * pg_bitutils_upstream.{c,h} reference copies are master files with
 * substantial non-vendored drift (SVE paths added, etc.).
 *
 * Buffer popcount: SIZEOF_VOID_P is assumed 8 (all pgrust targets are
 * 64-bit), so the aligned-word fast loop in pg_popcount_portable is
 * compiled in, exactly as on a 64-bit C build.
 *
 * Assert() is compiled out (NDEBUG semantics, matching a production
 * postgres build); harnesses fence the same preconditions with
 * kani::assume.
 *
 * Function bodies below are verbatim from upstream except:
 *   - "static inline" dropped so goto-cc exports the symbols
 *   - c.h replaced by the minimal typedef/macro block below
 */

typedef unsigned char uint8;
typedef unsigned int uint32;
typedef unsigned long long uint64;
typedef unsigned long uintptr_t_pg;

#define UINT64CONST(x) (x##ULL)
#define PG_UINT32_MAX (0xFFFFFFFFU)
#define PG_UINT64_MAX UINT64CONST(0xFFFFFFFFFFFFFFFF)
#define Assert(x) ((void) 0)
/* verbatim shape from c.h */
#define TYPEALIGN(ALIGNVAL,LEN)  \
	(((uintptr_t_pg) (LEN) + ((ALIGNVAL) - 1)) & ~((uintptr_t_pg) ((ALIGNVAL) - 1)))

/* ---- tables: verbatim from src/port/pg_bitutils.c ---- */

const uint8 pg_leftmost_one_pos[256] = {
	0, 0, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3,
	4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4,
	5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5,
	5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5,
	6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
	6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
	6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
	6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
	7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
	7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
	7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
	7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
	7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
	7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
	7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
	7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7
};

const uint8 pg_rightmost_one_pos[256] = {
	0, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	6, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	7, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	6, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0
};

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

/* ---- word-level functions: verbatim fallback branches from
 * src/include/port/pg_bitutils.h ---- */

int
pg_leftmost_one_pos32(uint32 word)
{
	int			shift = 32 - 8;

	Assert(word != 0);

	while ((word >> shift) == 0)
		shift -= 8;

	return shift + pg_leftmost_one_pos[(word >> shift) & 255];
}

int
pg_leftmost_one_pos64(uint64 word)
{
	int			shift = 64 - 8;

	Assert(word != 0);

	while ((word >> shift) == 0)
		shift -= 8;

	return shift + pg_leftmost_one_pos[(word >> shift) & 255];
}

int
pg_rightmost_one_pos32(uint32 word)
{
	int			result = 0;

	Assert(word != 0);

	while ((word & 255) == 0)
	{
		word >>= 8;
		result += 8;
	}
	result += pg_rightmost_one_pos[word & 255];
	return result;
}

int
pg_rightmost_one_pos64(uint64 word)
{
	int			result = 0;

	Assert(word != 0);

	while ((word & 255) == 0)
	{
		word >>= 8;
		result += 8;
	}
	result += pg_rightmost_one_pos[word & 255];
	return result;
}

uint32
pg_nextpower2_32(uint32 num)
{
	Assert(num > 0 && num <= PG_UINT32_MAX / 2 + 1);

	if ((num & (num - 1)) == 0)
		return num;				/* already power 2 */

	return ((uint32) 1) << (pg_leftmost_one_pos32(num) + 1);
}

uint64
pg_nextpower2_64(uint64 num)
{
	Assert(num > 0 && num <= PG_UINT64_MAX / 2 + 1);

	if ((num & (num - 1)) == 0)
		return num;				/* already power 2 */

	return ((uint64) 1) << (pg_leftmost_one_pos64(num) + 1);
}

uint32
pg_prevpower2_32(uint32 num)
{
	return ((uint32) 1) << pg_leftmost_one_pos32(num);
}

uint64
pg_prevpower2_64(uint64 num)
{
	return ((uint64) 1) << pg_leftmost_one_pos64(num);
}

uint32
pg_ceil_log2_32(uint32 num)
{
	if (num < 2)
		return 0;
	else
		return pg_leftmost_one_pos32(num - 1) + 1;
}

uint64
pg_ceil_log2_64(uint64 num)
{
	if (num < 2)
		return 0;
	else
		return pg_leftmost_one_pos64(num - 1) + 1;
}

int
pg_popcount32(uint32 word)
{
	word -= (word >> 1) & 0x55555555;
	word = (word & 0x33333333) + ((word >> 2) & 0x33333333);
	return (((word + (word >> 4)) & 0xf0f0f0f) * 0x1010101) >> 24;
}

int
pg_popcount64(uint64 word)
{
	word -= (word >> 1) & UINT64CONST(0x5555555555555555);
	word = (word & UINT64CONST(0x3333333333333333)) +
		((word >> 2) & UINT64CONST(0x3333333333333333));
	word = (word + (word >> 4)) & UINT64CONST(0xf0f0f0f0f0f0f0f);
	return (word * UINT64CONST(0x101010101010101)) >> 56;
}

uint32
pg_rotate_right32(uint32 word, int n)
{
	return (word >> n) | (word << (32 - n));
}

uint32
pg_rotate_left32(uint32 word, int n)
{
	return (word << n) | (word >> (32 - n));
}

/* ---- buffer popcount: verbatim from src/port/pg_bitutils.c
 * (SIZEOF_VOID_P >= 8 branch compiled in) ---- */

uint64
pg_popcount_portable(const char *buf, int bytes)
{
	uint64		popcnt = 0;

	/* Process in 64-bit chunks if the buffer is aligned. */
	if (buf == (const char *) TYPEALIGN(8, buf))
	{
		const uint64 *words = (const uint64 *) buf;

		while (bytes >= 8)
		{
			popcnt += pg_popcount64(*words++);
			bytes -= 8;
		}

		buf = (const char *) words;
	}

	/* Process any remaining bytes */
	while (bytes--)
		popcnt += pg_number_of_ones[(unsigned char) *buf++];

	return popcnt;
}

uint64
pg_popcount_masked_portable(const char *buf, int bytes, uint8 mask)
{
	uint64		popcnt = 0;

	/* Process in 64-bit chunks if the buffer is aligned */
	uint64		maskv = ~UINT64CONST(0) / 0xFF * mask;

	if (buf == (const char *) TYPEALIGN(8, buf))
	{
		const uint64 *words = (const uint64 *) buf;

		while (bytes >= 8)
		{
			popcnt += pg_popcount64(*words++ & maskv);
			bytes -= 8;
		}

		buf = (const char *) words;
	}

	/* Process any remaining bytes */
	while (bytes--)
		popcnt += pg_number_of_ones[(unsigned char) *buf++ & mask];

	return popcnt;
}

/* pg_popcount / pg_popcount_masked static-inline dispatchers from the
 * header, with pg_popcount_optimized == the portable version (the
 * !HAVE_X86_64_POPCNTQ && !USE_NEON configuration of pg_bitutils.c). */

uint64
pg_popcount_c(const char *buf, int bytes)
{
	if (bytes < 8)
	{
		uint64		popcnt = 0;

		while (bytes--)
			popcnt += pg_number_of_ones[(unsigned char) *buf++];
		return popcnt;
	}

	return pg_popcount_portable(buf, bytes);
}

uint64
pg_popcount_masked_c(const char *buf, int bytes, uint8 mask)
{
	if (bytes < 8)
	{
		uint64		popcnt = 0;

		while (bytes--)
			popcnt += pg_number_of_ones[(unsigned char) *buf++ & mask];
		return popcnt;
	}

	return pg_popcount_masked_portable(buf, bytes, mask);
}
