/*
 * Vendored PORTABLE pg_nextpower2_32 for the proofs/arrayuser harness:
 * PostgreSQL 18.3 (Stamp-18.3, upstream 62d6c7d3df),
 * src/include/port/pg_bitutils.h — verbatim FALLBACK (table-walk) branch,
 * same config choice as proofs/bitutils/c_bitutils.c (the builtin-clz
 * branches are faster encodings of the same function).
 *
 * Target: the LOCAL transcription adt array_userfuncs carries at
 * crates/backend/utils/adt/array_userfuncs/src/lib.rs (pg_nextpower2_32),
 * duplicated there from pg_bitutils.h; the shipped crates/port/pg_bitutils
 * copy is separately proved by proofs/bitutils.
 *
 * Shims: "static inline" dropped for goto-cc symbol export; c.h replaced
 * by the minimal typedef block; Assert compiled out (NDEBUG semantics),
 * precondition fenced with kani::assume in the harness.
 */

typedef unsigned char uint8;
typedef unsigned int uint32;

#define PG_UINT32_MAX (0xFFFFFFFFU)
#define Assert(x) ((void) 0)

/* table: verbatim from src/port/pg_bitutils.c */
static const uint8 pg_leftmost_one_pos[256] = {
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

/* verbatim pg_bitutils.h fallback branch ("static inline" dropped) */
int
pg_leftmost_one_pos32(uint32 word)
{
	int			shift = 32 - 8;

	Assert(word != 0);

	while ((word >> shift) == 0)
		shift -= 8;

	return shift + pg_leftmost_one_pos[(word >> shift)];
}

uint32
pg_nextpower2_32(uint32 num)
{
	Assert(num > 0 && num <= PG_UINT32_MAX / 2 + 1);

	if ((num & (num - 1)) == 0)
		return num;				/* already power 2 */

	return ((uint32) 1) << (pg_leftmost_one_pos32(num) + 1);
}
