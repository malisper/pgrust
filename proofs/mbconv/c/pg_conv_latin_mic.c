/*
 * pg_conv_latin_mic.c — vendored from latin_and_mic/latin_and_mic.c
 * (fetched 2026-07-28 from https://raw.githubusercontent.com/postgres/postgres/REL_18_STABLE/src/backend/utils/mb/conversion_procs). Same shim conventions as pg_conv_cyrillic_mic.c.
 */
#include "pg_mbconv.h"

int
pg_latin1_to_mic(const unsigned char *src, unsigned char *dest, int len, bool noError)
{
	int			converted;

	pg_mbconv_err = 0;
	converted = latin2mic(src, dest, len, LC_ISO8859_1, PG_LATIN1, noError);
	return converted;
}

int
pg_mic_to_latin1(const unsigned char *src, unsigned char *dest, int len, bool noError)
{
	int			converted;

	pg_mbconv_err = 0;
	converted = mic2latin(src, dest, len, LC_ISO8859_1, PG_LATIN1, noError);
	return converted;
}

int
pg_latin3_to_mic(const unsigned char *src, unsigned char *dest, int len, bool noError)
{
	int			converted;

	pg_mbconv_err = 0;
	converted = latin2mic(src, dest, len, LC_ISO8859_3, PG_LATIN3, noError);
	return converted;
}

int
pg_mic_to_latin3(const unsigned char *src, unsigned char *dest, int len, bool noError)
{
	int			converted;

	pg_mbconv_err = 0;
	converted = mic2latin(src, dest, len, LC_ISO8859_3, PG_LATIN3, noError);
	return converted;
}

int
pg_latin4_to_mic(const unsigned char *src, unsigned char *dest, int len, bool noError)
{
	int			converted;

	pg_mbconv_err = 0;
	converted = latin2mic(src, dest, len, LC_ISO8859_4, PG_LATIN4, noError);
	return converted;
}

int
pg_mic_to_latin4(const unsigned char *src, unsigned char *dest, int len, bool noError)
{
	int			converted;

	pg_mbconv_err = 0;
	converted = mic2latin(src, dest, len, LC_ISO8859_4, PG_LATIN4, noError);
	return converted;
}

