/*
 * pg_conv_utf8_win.c — vendored from utf8_and_win/utf8_and_win.c + the 22
 * win*.map files (fetched 2026-07-28 from https://raw.githubusercontent.com/
 * postgres/postgres/REL_18_STABLE/src/backend/utils/mb; maps included
 * VERBATIM from orig/maps/). The two procs take the encoding from fcinfo
 * arg 0/1, so the shim signature carries an explicit `encoding` parameter;
 * the maps[] array and the dispatch loop bodies are VERBATIM.
 * CHECK_ENCODING_CONVERSION_ARGS dropped (covered by
 * eq_check_encoding_conversion_args); error paths per PROOF_EREPORT_FLAG.
 */
#include "pg_mbconv.h"

#include "orig/maps/win866_to_utf8.map"
#include "orig/maps/win874_to_utf8.map"
#include "orig/maps/win1250_to_utf8.map"
#include "orig/maps/win1251_to_utf8.map"
#include "orig/maps/win1252_to_utf8.map"
#include "orig/maps/win1253_to_utf8.map"
#include "orig/maps/win1254_to_utf8.map"
#include "orig/maps/win1255_to_utf8.map"
#include "orig/maps/win1256_to_utf8.map"
#include "orig/maps/win1257_to_utf8.map"
#include "orig/maps/win1258_to_utf8.map"
#include "orig/maps/utf8_to_win866.map"
#include "orig/maps/utf8_to_win874.map"
#include "orig/maps/utf8_to_win1250.map"
#include "orig/maps/utf8_to_win1251.map"
#include "orig/maps/utf8_to_win1252.map"
#include "orig/maps/utf8_to_win1253.map"
#include "orig/maps/utf8_to_win1254.map"
#include "orig/maps/utf8_to_win1255.map"
#include "orig/maps/utf8_to_win1256.map"
#include "orig/maps/utf8_to_win1257.map"
#include "orig/maps/utf8_to_win1258.map"

typedef struct
{
	pg_enc		encoding;
	const pg_mb_radix_tree *map1;	/* to UTF8 map name */
	const pg_mb_radix_tree *map2;	/* from UTF8 map name */
} pg_conv_map;

static const pg_conv_map maps[] = {
	{PG_WIN866, &win866_to_unicode_tree, &win866_from_unicode_tree},
	{PG_WIN874, &win874_to_unicode_tree, &win874_from_unicode_tree},
	{PG_WIN1250, &win1250_to_unicode_tree, &win1250_from_unicode_tree},
	{PG_WIN1251, &win1251_to_unicode_tree, &win1251_from_unicode_tree},
	{PG_WIN1252, &win1252_to_unicode_tree, &win1252_from_unicode_tree},
	{PG_WIN1253, &win1253_to_unicode_tree, &win1253_from_unicode_tree},
	{PG_WIN1254, &win1254_to_unicode_tree, &win1254_from_unicode_tree},
	{PG_WIN1255, &win1255_to_unicode_tree, &win1255_from_unicode_tree},
	{PG_WIN1256, &win1256_to_unicode_tree, &win1256_from_unicode_tree},
	{PG_WIN1257, &win1257_to_unicode_tree, &win1257_from_unicode_tree},
	{PG_WIN1258, &win1258_to_unicode_tree, &win1258_from_unicode_tree},
};

int
pg_win_to_utf8(int encoding, const unsigned char *src, unsigned char *dest, int len, bool noError)
{
	int			i;

	pg_mbconv_err = 0;

	for (i = 0; i < lengthof(maps); i++)
	{
		if (encoding == maps[i].encoding)
		{
			int			converted;

			converted = LocalToUtf(src, len, dest,
								   maps[i].map1,
								   NULL, 0,
								   NULL,
								   encoding,
								   noError);
			return converted;
		}
	}

	ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg("unexpected encoding ID %d for WIN character sets",
					encoding)));

	return 0;
}

int
pg_utf8_to_win(int encoding, const unsigned char *src, unsigned char *dest, int len, bool noError)
{
	int			i;

	pg_mbconv_err = 0;

	for (i = 0; i < lengthof(maps); i++)
	{
		if (encoding == maps[i].encoding)
		{
			int			converted;

			converted = UtfToLocal(src, len, dest,
								   maps[i].map2,
								   NULL, 0,
								   NULL,
								   encoding,
								   noError);
			return converted;
		}
	}

	ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg("unexpected encoding ID %d for WIN character sets",
					encoding)));

	return 0;
}
