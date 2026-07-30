/*
 * Vendored from postgres REL_15_STABLE src/backend/utils/adt/encode.c
 * (hextbl, hexlookup, hex_encode, get_hex, hex_decode; fetched 2026-07-28).
 * REL_15_STABLE chosen per charter (older, simpler hex lane); note its
 * get_hex/hex_decode already carry the (cp,end) range form.
 *
 * SHIMS (everything else is verbatim):
 *  - ereport(ERROR, ...) has no C model here; get_hex's error becomes a -1
 *    sentinel return (the message's pg_mblen_range call is dropped with it
 *    — message text is not part of the equivalence claim). hex_decode is
 *    restructured minimally to propagate the sentinel: -1 = invalid digit,
 *    -2 = odd number of digits, >= 0 = decoded byte count. Where C's
 *    control flow aborts via longjmp, the shim aborts via early return at
 *    the exact same program points.
 *  - uint64/int8 typedefs inlined (no postgres headers).
 *  - names pgc_-prefixed.
 */

typedef unsigned long long pgc_uint64;
typedef signed char pgc_int8;

static const char pgc_hextbl[] = "0123456789abcdef";

static const pgc_int8 pgc_hexlookup[128] = {
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	0, 1, 2, 3, 4, 5, 6, 7, 8, 9, -1, -1, -1, -1, -1, -1,
	-1, 10, 11, 12, 13, 14, 15, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, 10, 11, 12, 13, 14, 15, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
};

pgc_uint64
pgc_hex_encode(const char *src, pgc_uint64 len, char *dst)
{
	const char *end = src + len;

	while (src < end)
	{
		*dst++ = pgc_hextbl[(*src >> 4) & 0xF];
		*dst++ = pgc_hextbl[*src & 0xF];
		src++;
	}
	return (pgc_uint64) len * 2;
}

/* SHIM: ereport(ERROR, invalid hexadecimal digit) -> return -1 */
static int
pgc_get_hex(const char *cp, const char *end)
{
	unsigned char c = (unsigned char) *cp;
	int			res = -1;

	if (c < 127)
		res = pgc_hexlookup[c];

	if (res < 0)
		return -1;				/* SHIM: was ereport(ERROR, ...) */

	return res;
}

/* returns decoded length, or -1 invalid digit / -2 odd number of digits */
long long
pgc_hex_decode(const char *src, pgc_uint64 len, char *dst)
{
	const char *s,
			   *srcend;
	int			v1,
				v2;
	char	   *p;

	srcend = src + len;
	s = src;
	p = dst;
	while (s < srcend)
	{
		if (*s == ' ' || *s == '\n' || *s == '\t' || *s == '\r')
		{
			s++;
			continue;
		}
		v1 = pgc_get_hex(s, srcend);	/* SHIM: << 4 moved below the
										 * sentinel check */
		if (v1 < 0)
			return -1;
		s++;
		if (s >= srcend)
			return -2;			/* SHIM: was ereport(ERROR, odd number) */

		v2 = pgc_get_hex(s, srcend);
		if (v2 < 0)
			return -1;
		s++;
		*p++ = (char) ((v1 << 4) | v2);
	}

	return (long long) (p - dst);
}
