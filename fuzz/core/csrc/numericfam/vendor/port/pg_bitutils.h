/* SHIM port/pg_bitutils.h — plumbing only, for the vendored hashfn.c.
 * pg_rotate_left32 is value-exact vs src/include/port/pg_bitutils.h. */
#ifndef NFZ_PG_BITUTILS_H
#define NFZ_PG_BITUTILS_H
static inline uint32
pg_rotate_left32(uint32 word, int n)
{
	return (word << n) | (word >> (32 - n));
}
static inline uint32
pg_rotate_right32(uint32 word, int n)
{
	return (word >> n) | (word << (32 - n));
}
#endif
