/*
 * pg_conv_utf8_euc2004.c — vendored from utf8_and_euc2004.c + map files
 * (fetched 2026-07-28 from https://raw.githubusercontent.com/postgres/postgres/REL_18_STABLE/src/backend/utils/mb: conversion_procs + Unicode maps, map files included VERBATIM from orig/maps/). Shims per gen_shim.awk conventions (see pg_conv_cyrillic_mic.c header); error paths per PROOF_EREPORT_FLAG (pg_mbconv.h).
 */
#include "pg_mbconv.h"

#include "orig/maps/euc_jis_2004_to_utf8.map"
#include "orig/maps/utf8_to_euc_jis_2004.map"

int
pg_euc_jis_2004_to_utf8(const unsigned char *src, unsigned char *dest, int len, bool noError)
{
	int			converted;

	pg_mbconv_err = 0;
	converted = LocalToUtf(src, len, dest,
						   &euc_jis_2004_to_unicode_tree,
						   LUmapEUC_JIS_2004_combined, lengthof(LUmapEUC_JIS_2004_combined),
						   NULL,
						   PG_EUC_JIS_2004,
						   noError);
	return converted;
}

int
pg_utf8_to_euc_jis_2004(const unsigned char *src, unsigned char *dest, int len, bool noError)
{
	int			converted;

	pg_mbconv_err = 0;
	converted = UtfToLocal(src, len, dest,
						   &euc_jis_2004_from_unicode_tree,
						   ULmapEUC_JIS_2004_combined, lengthof(ULmapEUC_JIS_2004_combined),
						   NULL,
						   PG_EUC_JIS_2004,
						   noError);
	return converted;
}

