/* SHIM header for the jsonpathexec_diff oracle - NOT PostgreSQL code.
 * pg_nextpower2_32 VERBATIM from src/include/port/pg_bitutils.h @ 18.3
 * (with pg_leftmost_one_pos32 on the __builtin_clz arm). */
#ifndef PG_BITUTILS_H
#define PG_BITUTILS_H
#include "postgres.h"

static inline int
pg_leftmost_one_pos32(uint32 word)
{
	Assert(word != 0);
	return 31 - __builtin_clz(word);
}

/* ---- pg_bitutils.h:189-201 VERBATIM (pg_nextpower2_32) ---- */
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

	return ((uint32) 1) << (32 - __builtin_clz(num));
}
#endif
