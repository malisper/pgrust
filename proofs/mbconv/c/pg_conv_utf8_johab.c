/*
 * pg_conv_utf8_johab.c — vendored from utf8_and_johab.c + map files
 * (fetched 2026-07-28 from https://raw.githubusercontent.com/postgres/postgres/REL_18_STABLE/src/backend/utils/mb: conversion_procs + Unicode maps, map files included VERBATIM from orig/maps/). Shims per gen_shim.awk conventions (see pg_conv_cyrillic_mic.c header); error paths per PROOF_EREPORT_FLAG (pg_mbconv.h).
 */
#include "pg_mbconv.h"

#include "orig/maps/johab_to_utf8.map"
#include "orig/maps/utf8_to_johab.map"

int
pg_johab_to_utf8(const unsigned char *src, unsigned char *dest, int len, bool noError)
{
	int			converted;

	pg_mbconv_err = 0;
	converted = LocalToUtf(src, len, dest,
						   &johab_to_unicode_tree,
						   NULL, 0,
						   NULL,
						   PG_JOHAB,
						   noError);
	return converted;
}

int
pg_utf8_to_johab(const unsigned char *src, unsigned char *dest, int len, bool noError)
{
	int			converted;

	pg_mbconv_err = 0;
	converted = UtfToLocal(src, len, dest,
						   &johab_from_unicode_tree,
						   NULL, 0,
						   NULL,
						   PG_JOHAB,
						   noError);
	return converted;
}

