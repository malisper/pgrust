/*
 * Vendored verbatim from PostgreSQL master src/common/wchar.c
 * (pg_utf_mblen + pg_utf8_islegal), plus int-returning shims for the
 * Kani c-ffi bridge (avoids bool/void ABI warts).
 * REL_18_STABLE conformance: the vendored kernels are REL_18-derived
 * despite the "master" wording — zero code drift vs REL_18_STABLE for
 * these functions (provenance audit, proofs/PROVENANCE-AUDIT.md,
 * 2026-07-28; note PG19/master removed MULE elsewhere in wchar.c).
 */
#include <stdbool.h>

#define pg_fallthrough /* FALLTHROUGH */

int
pg_utf_mblen(const unsigned char *s)
{
	int			len;

	if ((*s & 0x80) == 0)
		len = 1;
	else if ((*s & 0xe0) == 0xc0)
		len = 2;
	else if ((*s & 0xf0) == 0xe0)
		len = 3;
	else if ((*s & 0xf8) == 0xf0)
		len = 4;
#ifdef NOT_USED
	else if ((*s & 0xfc) == 0xf8)
		len = 5;
	else if ((*s & 0xfe) == 0xfc)
		len = 6;
#endif
	else
		len = 1;
	return len;
}

bool
pg_utf8_islegal(const unsigned char *source, int length)
{
	unsigned char a;

	switch (length)
	{
		default:
			/* reject lengths 5 and 6 for now */
			return false;
		case 4:
			a = source[3];
			if (a < 0x80 || a > 0xBF)
				return false;
			pg_fallthrough;
		case 3:
			a = source[2];
			if (a < 0x80 || a > 0xBF)
				return false;
			pg_fallthrough;
		case 2:
			a = source[1];
			switch (*source)
			{
				case 0xE0:
					if (a < 0xA0 || a > 0xBF)
						return false;
					break;
				case 0xED:
					if (a < 0x80 || a > 0x9F)
						return false;
					break;
				case 0xF0:
					if (a < 0x90 || a > 0xBF)
						return false;
					break;
				case 0xF4:
					if (a < 0x80 || a > 0x8F)
						return false;
					break;
				default:
					if (a < 0x80 || a > 0xBF)
						return false;
					break;
			}
			pg_fallthrough;
		case 1:
			a = *source;
			if (a >= 0x80 && a < 0xC2)
				return false;
			if (a > 0xF4)
				return false;
			break;
	}
	return true;
}

/* --- int-returning shims for the Kani FFI bridge --- */

int
c_pg_utf8_islegal(const unsigned char *source, int length)
{
	return pg_utf8_islegal(source, length) ? 1 : 0;
}

int
c_pg_utf_mblen(const unsigned char *s)
{
	return pg_utf_mblen(s);
}
