/*
 * pg_conv_utf8_iso8859.c — vendored from utf8_and_iso8859/utf8_and_iso8859.c
 * + the 26 iso8859*.map files (fetched 2026-07-28 from
 * https://raw.githubusercontent.com/postgres/postgres/REL_18_STABLE/src/
 * backend/utils/mb; maps included VERBATIM from orig/maps/). Same shim
 * conventions as pg_conv_utf8_win.c (explicit `encoding` parameter; maps[]
 * array and dispatch loops VERBATIM; CHECK dropped; PROOF_EREPORT_FLAG).
 */
#include "pg_mbconv.h"

#include "orig/maps/iso8859_2_to_utf8.map"
#include "orig/maps/iso8859_3_to_utf8.map"
#include "orig/maps/iso8859_4_to_utf8.map"
#include "orig/maps/iso8859_5_to_utf8.map"
#include "orig/maps/iso8859_6_to_utf8.map"
#include "orig/maps/iso8859_7_to_utf8.map"
#include "orig/maps/iso8859_8_to_utf8.map"
#include "orig/maps/iso8859_9_to_utf8.map"
#include "orig/maps/iso8859_10_to_utf8.map"
#include "orig/maps/iso8859_13_to_utf8.map"
#include "orig/maps/iso8859_14_to_utf8.map"
#include "orig/maps/iso8859_15_to_utf8.map"
#include "orig/maps/iso8859_16_to_utf8.map"
#include "orig/maps/utf8_to_iso8859_2.map"
#include "orig/maps/utf8_to_iso8859_3.map"
#include "orig/maps/utf8_to_iso8859_4.map"
#include "orig/maps/utf8_to_iso8859_5.map"
#include "orig/maps/utf8_to_iso8859_6.map"
#include "orig/maps/utf8_to_iso8859_7.map"
#include "orig/maps/utf8_to_iso8859_8.map"
#include "orig/maps/utf8_to_iso8859_9.map"
#include "orig/maps/utf8_to_iso8859_10.map"
#include "orig/maps/utf8_to_iso8859_13.map"
#include "orig/maps/utf8_to_iso8859_14.map"
#include "orig/maps/utf8_to_iso8859_15.map"
#include "orig/maps/utf8_to_iso8859_16.map"

typedef struct
{
	pg_enc		encoding;
	const pg_mb_radix_tree *map1;	/* to UTF8 map name */
	const pg_mb_radix_tree *map2;	/* from UTF8 map name */
} pg_conv_map;

static const pg_conv_map maps[] = {
	{PG_LATIN2, &iso8859_2_to_unicode_tree,
	&iso8859_2_from_unicode_tree},	/* ISO-8859-2 Latin 2 */
	{PG_LATIN3, &iso8859_3_to_unicode_tree,
	&iso8859_3_from_unicode_tree},	/* ISO-8859-3 Latin 3 */
	{PG_LATIN4, &iso8859_4_to_unicode_tree,
	&iso8859_4_from_unicode_tree},	/* ISO-8859-4 Latin 4 */
	{PG_LATIN5, &iso8859_9_to_unicode_tree,
	&iso8859_9_from_unicode_tree},	/* ISO-8859-9 Latin 5 */
	{PG_LATIN6, &iso8859_10_to_unicode_tree,
	&iso8859_10_from_unicode_tree}, /* ISO-8859-10 Latin 6 */
	{PG_LATIN7, &iso8859_13_to_unicode_tree,
	&iso8859_13_from_unicode_tree}, /* ISO-8859-13 Latin 7 */
	{PG_LATIN8, &iso8859_14_to_unicode_tree,
	&iso8859_14_from_unicode_tree}, /* ISO-8859-14 Latin 8 */
	{PG_LATIN9, &iso8859_15_to_unicode_tree,
	&iso8859_15_from_unicode_tree}, /* ISO-8859-15 Latin 9 */
	{PG_LATIN10, &iso8859_16_to_unicode_tree,
	&iso8859_16_from_unicode_tree}, /* ISO-8859-16 Latin 10 */
	{PG_ISO_8859_5, &iso8859_5_to_unicode_tree,
	&iso8859_5_from_unicode_tree},	/* ISO-8859-5 */
	{PG_ISO_8859_6, &iso8859_6_to_unicode_tree,
	&iso8859_6_from_unicode_tree},	/* ISO-8859-6 */
	{PG_ISO_8859_7, &iso8859_7_to_unicode_tree,
	&iso8859_7_from_unicode_tree},	/* ISO-8859-7 */
	{PG_ISO_8859_8, &iso8859_8_to_unicode_tree,
	&iso8859_8_from_unicode_tree},	/* ISO-8859-8 */
};


int
pg_iso8859_to_utf8(int encoding, const unsigned char *src, unsigned char *dest, int len, bool noError)
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
			 errmsg("unexpected encoding ID %d for ISO 8859 character sets",
					encoding)));

	return 0;
}

int
pg_utf8_to_iso8859(int encoding, const unsigned char *src, unsigned char *dest, int len, bool noError)
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
			 errmsg("unexpected encoding ID %d for ISO 8859 character sets",
					encoding)));

	return 0;
}
