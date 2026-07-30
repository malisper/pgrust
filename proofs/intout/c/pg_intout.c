/*
 * Vendored PostgreSQL C for the integer-output family, verbatim from:
 *   src/backend/utils/adt/numutils.c   (postgres/postgres master,
 *                                       fetched 2026-07-28 via
 *                                       raw.githubusercontent.com)
 *     DIGIT_TABLE, decimalLength32, decimalLength64,
 *     pg_ultoa_n, pg_ltoa, pg_ulltoa_n, pg_lltoa, pg_itoa
 *   src/include/port/pg_bitutils.h + src/port/pg_bitutils.c (same ref):
 *     pg_leftmost_one_pos32, pg_leftmost_one_pos64, pg_leftmost_one_pos[]
 * REL_18_STABLE conformance: zero code drift vs REL_18_STABLE (provenance
 * audit, proofs/PROVENANCE-AUDIT.md, 2026-07-28).
 *
 * SHIMS (plumbing only, never logic):
 *   - c.h replaced by the minimal typedef/macro block below
 *     (int16/int32/int64/uint32/uint64, UINT64CONST, Assert -> no-op,
 *      matching a production NDEBUG build; harnesses fence the same
 *      preconditions with kani::assume where relevant).
 *   - "static inline" dropped so goto-cc exports the symbols.
 *   - CONFIG CHOICE (same as proofs/bitutils): pg_leftmost_one_pos32/64
 *     use the PORTABLE fallback branch of the HAVE__BUILTIN_CLZ ifdef
 *     (table walk).  That branch is PostgreSQL's reference semantics for
 *     all platforms; the builtin branch is a faster encoding of the same
 *     function (already proven equivalent to the Rust intrinsics in
 *     proofs/bitutils).  NOTE: the table-walk loop's exit depends on the
 *     word != 0 precondition -- every harness reaching it needs
 *     #[kani::unwind(N)] (see prove-target skill traps).
 *   - No renaming needed: the functions already carry the pg_ prefix and
 *     the Rust symbols are mangled, so there is no link collision.
 *
 * Everything else is byte-for-byte upstream.  PostgreSQL compiles with
 * -fwrapv; CBMC's two's-complement default matches (pg_ltoa/pg_lltoa
 * negation of INT_MIN relies on unsigned wrap, which is defined anyway).
 */

#include <string.h>			/* memcpy: CBMC has a builtin model */

typedef signed short int16;
typedef signed int int32;
typedef signed long long int64;
typedef unsigned char uint8;
typedef unsigned int uint32;
typedef unsigned long long uint64;

#define UINT64CONST(x) (x##ULL)
#define Assert(x) ((void) 0)

/* ---- from src/port/pg_bitutils.c: verbatim table ---- */

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

/* ---- from src/include/port/pg_bitutils.h: portable fallback branch ---- */

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

/* ---- from src/backend/utils/adt/numutils.c: verbatim ---- */

/*
 * A table of all two-digit numbers. This is used to speed up decimal digit
 * generation by copying pairs of digits into the final output.
 */
static const char DIGIT_TABLE[200] =
"00" "01" "02" "03" "04" "05" "06" "07" "08" "09"
"10" "11" "12" "13" "14" "15" "16" "17" "18" "19"
"20" "21" "22" "23" "24" "25" "26" "27" "28" "29"
"30" "31" "32" "33" "34" "35" "36" "37" "38" "39"
"40" "41" "42" "43" "44" "45" "46" "47" "48" "49"
"50" "51" "52" "53" "54" "55" "56" "57" "58" "59"
"60" "61" "62" "63" "64" "65" "66" "67" "68" "69"
"70" "71" "72" "73" "74" "75" "76" "77" "78" "79"
"80" "81" "82" "83" "84" "85" "86" "87" "88" "89"
"90" "91" "92" "93" "94" "95" "96" "97" "98" "99";

/*
 * Adapted from http://graphics.stanford.edu/~seander/bithacks.html#IntegerLog10
 */
int
decimalLength32(const uint32 v)
{
	int			t;
	static const uint32 PowersOfTen[] = {
		1, 10, 100,
		1000, 10000, 100000,
		1000000, 10000000, 100000000,
		1000000000
	};

	/*
	 * Compute base-10 logarithm by dividing the base-2 logarithm by a
	 * good-enough approximation of the base-2 logarithm of 10
	 */
	t = (pg_leftmost_one_pos32(v) + 1) * 1233 / 4096;
	return t + (v >= PowersOfTen[t]);
}

int
decimalLength64(const uint64 v)
{
	int			t;
	static const uint64 PowersOfTen[] = {
		UINT64CONST(1), UINT64CONST(10),
		UINT64CONST(100), UINT64CONST(1000),
		UINT64CONST(10000), UINT64CONST(100000),
		UINT64CONST(1000000), UINT64CONST(10000000),
		UINT64CONST(100000000), UINT64CONST(1000000000),
		UINT64CONST(10000000000), UINT64CONST(100000000000),
		UINT64CONST(1000000000000), UINT64CONST(10000000000000),
		UINT64CONST(100000000000000), UINT64CONST(1000000000000000),
		UINT64CONST(10000000000000000), UINT64CONST(100000000000000000),
		UINT64CONST(1000000000000000000), UINT64CONST(10000000000000000000)
	};

	/*
	 * Compute base-10 logarithm by dividing the base-2 logarithm by a
	 * good-enough approximation of the base-2 logarithm of 10
	 */
	t = (pg_leftmost_one_pos64(v) + 1) * 1233 / 4096;
	return t + (v >= PowersOfTen[t]);
}

/*
 * pg_ultoa_n: converts an unsigned 32-bit integer to its string representation,
 * not NUL-terminated, and returns the length of that string representation
 *
 * Caller must ensure that 'a' points to enough memory to hold the result (at
 * least 10 bytes)
 */
int
pg_ultoa_n(uint32 value, char *a)
{
	int			olength,
				i = 0;

	/* Degenerate case */
	if (value == 0)
	{
		*a = '0';
		return 1;
	}

	olength = decimalLength32(value);

	/* Compute the result string. */
	while (value >= 10000)
	{
		const uint32 c = value - 10000 * (value / 10000);
		const uint32 c0 = (c % 100) << 1;
		const uint32 c1 = (c / 100) << 1;

		char	   *pos = a + olength - i;

		value /= 10000;

		memcpy(pos - 2, DIGIT_TABLE + c0, 2);
		memcpy(pos - 4, DIGIT_TABLE + c1, 2);
		i += 4;
	}
	if (value >= 100)
	{
		const uint32 c = (value % 100) << 1;

		char	   *pos = a + olength - i;

		value /= 100;

		memcpy(pos - 2, DIGIT_TABLE + c, 2);
		i += 2;
	}
	if (value >= 10)
	{
		const uint32 c = value << 1;

		char	   *pos = a + olength - i;

		memcpy(pos - 2, DIGIT_TABLE + c, 2);
	}
	else
	{
		*a = (char) ('0' + value);
	}

	return olength;
}

/*
 * pg_ltoa: converts a signed 32-bit integer to its string representation and
 * returns strlen(a).
 *
 * It is the caller's responsibility to ensure that a is at least 12 bytes long,
 * which is enough room to hold a minus sign, a maximally long int32, and the
 * above terminating NUL.
 */
int
pg_ltoa(int32 value, char *a)
{
	uint32		uvalue = (uint32) value;
	int			len = 0;

	if (value < 0)
	{
		uvalue = (uint32) 0 - uvalue;
		a[len++] = '-';
	}
	len += pg_ultoa_n(uvalue, a + len);
	a[len] = '\0';
	return len;
}

/*
 * Get the decimal representation, not NUL-terminated, and return the length of
 * same.  Caller must ensure that a points to at least MAXINT8LEN bytes.
 */
int
pg_ulltoa_n(uint64 value, char *a)
{
	int			olength,
				i = 0;
	uint32		value2;

	/* Degenerate case */
	if (value == 0)
	{
		*a = '0';
		return 1;
	}

	olength = decimalLength64(value);

	/* Compute the result string. */
	while (value >= 100000000)
	{
		const uint64 q = value / 100000000;
		uint32		value3 = (uint32) (value - 100000000 * q);

		const uint32 c = value3 % 10000;
		const uint32 d = value3 / 10000;
		const uint32 c0 = (c % 100) << 1;
		const uint32 c1 = (c / 100) << 1;
		const uint32 d0 = (d % 100) << 1;
		const uint32 d1 = (d / 100) << 1;

		char	   *pos = a + olength - i;

		value = q;

		memcpy(pos - 2, DIGIT_TABLE + c0, 2);
		memcpy(pos - 4, DIGIT_TABLE + c1, 2);
		memcpy(pos - 6, DIGIT_TABLE + d0, 2);
		memcpy(pos - 8, DIGIT_TABLE + d1, 2);
		i += 8;
	}

	/* Switch to 32-bit for speed */
	value2 = (uint32) value;

	if (value2 >= 10000)
	{
		const uint32 c = value2 - 10000 * (value2 / 10000);
		const uint32 c0 = (c % 100) << 1;
		const uint32 c1 = (c / 100) << 1;

		char	   *pos = a + olength - i;

		value2 /= 10000;

		memcpy(pos - 2, DIGIT_TABLE + c0, 2);
		memcpy(pos - 4, DIGIT_TABLE + c1, 2);
		i += 4;
	}
	if (value2 >= 100)
	{
		const uint32 c = (value2 % 100) << 1;
		char	   *pos = a + olength - i;

		value2 /= 100;

		memcpy(pos - 2, DIGIT_TABLE + c, 2);
		i += 2;
	}
	if (value2 >= 10)
	{
		const uint32 c = value2 << 1;
		char	   *pos = a + olength - i;

		memcpy(pos - 2, DIGIT_TABLE + c, 2);
	}
	else
		*a = (char) ('0' + value2);

	return olength;
}

/*
 * pg_lltoa: converts a signed 64-bit integer to its string representation and
 * returns strlen(a).
 *
 * Caller must ensure that 'a' points to enough memory to hold the result
 * (at least MAXINT8LEN + 1 bytes, counting a leading sign and trailing NUL).
 */
int
pg_lltoa(int64 value, char *a)
{
	uint64		uvalue = value;
	int			len = 0;

	if (value < 0)
	{
		uvalue = (uint64) 0 - uvalue;
		a[len++] = '-';
	}

	len += pg_ulltoa_n(uvalue, a + len);
	a[len] = '\0';
	return len;
}

/*
 * pg_itoa: converts a signed 16-bit integer to its string representation
 * and returns strlen(a).
 *
 * Caller must ensure that 'a' points to enough memory to hold the result
 * (at least 7 bytes, counting a leading sign and trailing NUL).
 *
 * It doesn't seem worth implementing this separately.
 */
int
pg_itoa(int16 i, char *a)
{
	return pg_ltoa((int32) i, a);
}
