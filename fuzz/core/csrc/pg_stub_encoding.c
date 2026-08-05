/*
 * stub:encoding — C-oracle side of the shared encoding static-table pin
 * (fuzz/core/src/stub_encoding.rs is the Rust side).
 *
 * Provenance (postgres-src 62d6c7d3df6287f1bd83199c1a746e50d31571a0,
 * REL_18 "Stamp 18.3", ../pgrust-fabled/vendor/postgres-src):
 *   - pg_enc enum: src/include/mb/pg_wchar.h — VERBATIM.
 *   - pg_enc2name_tbl: src/common/encnames.c — VERBATIM (the non-WIN32
 *     DEF_ENC2NAME arm: { #name, PG_##name }; codepage column is a WIN32
 *     compile arm, not present on any campaign platform).
 *   - maxmblen column: src/common/wchar.c pg_wchar_table — the last field
 *     of each initializer row, extracted mechanically (the full table
 *     carries function pointers whose bodies live in the wchar Rust crate;
 *     only the scalar column is state). Extraction:
 *       python3 -c "regex over 'const pg_wchar_tbl pg_wchar_table' rows"
 *     yields 42 values, transcribed below in enum order.
 *
 * The Rust side compares, per encoding 0..41: this table's name against
 *   mbutils::pg_encoding_to_char, this maxmblen against
 *   wchar::pg_encoding_max_length, and the server-encoding boundary
 *   (PG_ENCODING_BE_LAST) — pinning the static tables identically on both
 *   sides. A transcription defect on EITHER side is a caught divergence
 *   (the Rust tables are the shipped crates, not copies of this file).
 */
#include "pg_oracle_guard.h"	/* oracle-serialization holder check */

typedef enum pg_enc
{
	PG_SQL_ASCII = 0,			/* SQL/ASCII */
	PG_EUC_JP,					/* EUC for Japanese */
	PG_EUC_CN,					/* EUC for Chinese */
	PG_EUC_KR,					/* EUC for Korean */
	PG_EUC_TW,					/* EUC for Taiwan */
	PG_EUC_JIS_2004,			/* EUC-JIS-2004 */
	PG_UTF8,					/* Unicode UTF8 */
	PG_MULE_INTERNAL,			/* Mule internal code */
	PG_LATIN1,					/* ISO-8859-1 Latin 1 */
	PG_LATIN2,					/* ISO-8859-2 Latin 2 */
	PG_LATIN3,					/* ISO-8859-3 Latin 3 */
	PG_LATIN4,					/* ISO-8859-4 Latin 4 */
	PG_LATIN5,					/* ISO-8859-9 Latin 5 */
	PG_LATIN6,					/* ISO-8859-10 Latin6 */
	PG_LATIN7,					/* ISO-8859-13 Latin7 */
	PG_LATIN8,					/* ISO-8859-14 Latin8 */
	PG_LATIN9,					/* ISO-8859-15 Latin9 */
	PG_LATIN10,					/* ISO-8859-16 Latin10 */
	PG_WIN1256,					/* windows-1256 */
	PG_WIN1258,					/* Windows-1258 */
	PG_WIN866,					/* (MS-DOS CP866) */
	PG_WIN874,					/* windows-874 */
	PG_KOI8R,					/* KOI8-R */
	PG_WIN1251,					/* windows-1251 */
	PG_WIN1252,					/* windows-1252 */
	PG_ISO_8859_5,				/* ISO-8859-5 */
	PG_ISO_8859_6,				/* ISO-8859-6 */
	PG_ISO_8859_7,				/* ISO-8859-7 */
	PG_ISO_8859_8,				/* ISO-8859-8 */
	PG_WIN1250,					/* windows-1250 */
	PG_WIN1253,					/* windows-1253 */
	PG_WIN1254,					/* windows-1254 */
	PG_WIN1255,					/* windows-1255 */
	PG_WIN1257,					/* windows-1257 */
	PG_KOI8U,					/* KOI8-U */

	PG_SJIS,					/* Shift JIS (Windows-932) */
	PG_BIG5,					/* Big5 (Windows-950) */
	PG_GBK,						/* GBK (Windows-936) */
	PG_UHC,						/* UHC (Windows-949) */
	PG_GB18030,					/* GB18030 */
	PG_JOHAB,					/* EUC for Korean JOHAB */
	PG_SHIFT_JIS_2004,			/* Shift-JIS-2004 */
	_PG_LAST_ENCODING_			/* mark only */

} pg_enc;

#define PG_ENCODING_BE_LAST PG_KOI8U

typedef struct pg_enc2name
{
	const char *name;
	pg_enc		encoding;
} pg_enc2name;

#define DEF_ENC2NAME(name, codepage) { #name, PG_##name }

static const pg_enc2name pg_enc2name_tbl[] =
{
	[PG_SQL_ASCII] = DEF_ENC2NAME(SQL_ASCII, 0),
	[PG_EUC_JP] = DEF_ENC2NAME(EUC_JP, 20932),
	[PG_EUC_CN] = DEF_ENC2NAME(EUC_CN, 20936),
	[PG_EUC_KR] = DEF_ENC2NAME(EUC_KR, 51949),
	[PG_EUC_TW] = DEF_ENC2NAME(EUC_TW, 0),
	[PG_EUC_JIS_2004] = DEF_ENC2NAME(EUC_JIS_2004, 20932),
	[PG_UTF8] = DEF_ENC2NAME(UTF8, 65001),
	[PG_MULE_INTERNAL] = DEF_ENC2NAME(MULE_INTERNAL, 0),
	[PG_LATIN1] = DEF_ENC2NAME(LATIN1, 28591),
	[PG_LATIN2] = DEF_ENC2NAME(LATIN2, 28592),
	[PG_LATIN3] = DEF_ENC2NAME(LATIN3, 28593),
	[PG_LATIN4] = DEF_ENC2NAME(LATIN4, 28594),
	[PG_LATIN5] = DEF_ENC2NAME(LATIN5, 28599),
	[PG_LATIN6] = DEF_ENC2NAME(LATIN6, 0),
	[PG_LATIN7] = DEF_ENC2NAME(LATIN7, 0),
	[PG_LATIN8] = DEF_ENC2NAME(LATIN8, 0),
	[PG_LATIN9] = DEF_ENC2NAME(LATIN9, 28605),
	[PG_LATIN10] = DEF_ENC2NAME(LATIN10, 0),
	[PG_WIN1256] = DEF_ENC2NAME(WIN1256, 1256),
	[PG_WIN1258] = DEF_ENC2NAME(WIN1258, 1258),
	[PG_WIN866] = DEF_ENC2NAME(WIN866, 866),
	[PG_WIN874] = DEF_ENC2NAME(WIN874, 874),
	[PG_KOI8R] = DEF_ENC2NAME(KOI8R, 20866),
	[PG_WIN1251] = DEF_ENC2NAME(WIN1251, 1251),
	[PG_WIN1252] = DEF_ENC2NAME(WIN1252, 1252),
	[PG_ISO_8859_5] = DEF_ENC2NAME(ISO_8859_5, 28595),
	[PG_ISO_8859_6] = DEF_ENC2NAME(ISO_8859_6, 28596),
	[PG_ISO_8859_7] = DEF_ENC2NAME(ISO_8859_7, 28597),
	[PG_ISO_8859_8] = DEF_ENC2NAME(ISO_8859_8, 28598),
	[PG_WIN1250] = DEF_ENC2NAME(WIN1250, 1250),
	[PG_WIN1253] = DEF_ENC2NAME(WIN1253, 1253),
	[PG_WIN1254] = DEF_ENC2NAME(WIN1254, 1254),
	[PG_WIN1255] = DEF_ENC2NAME(WIN1255, 1255),
	[PG_WIN1257] = DEF_ENC2NAME(WIN1257, 1257),
	[PG_KOI8U] = DEF_ENC2NAME(KOI8U, 21866),
	[PG_SJIS] = DEF_ENC2NAME(SJIS, 932),
	[PG_BIG5] = DEF_ENC2NAME(BIG5, 950),
	[PG_GBK] = DEF_ENC2NAME(GBK, 936),
	[PG_UHC] = DEF_ENC2NAME(UHC, 949),
	[PG_GB18030] = DEF_ENC2NAME(GB18030, 54936),
	[PG_JOHAB] = DEF_ENC2NAME(JOHAB, 0),
	[PG_SHIFT_JIS_2004] = DEF_ENC2NAME(SHIFT_JIS_2004, 932),
};

/* maxmblen column of src/common/wchar.c pg_wchar_table (enum order) */
static const int pg_stub_maxmblen_tbl[_PG_LAST_ENCODING_] = {
	1, 3, 3, 3, 4, 3, 4, 4, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 4, 3, 2,
};

/* ------------------------------------------------------------------ */
/* accessors for the Rust comparator (out-of-range -> NULL / -1: the   */
/* Rust side asserts these arms too, so the clamp itself is compared)  */
/* ------------------------------------------------------------------ */

int
pg_stub_enc_count(void)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	return (int) _PG_LAST_ENCODING_;
}

int
pg_stub_enc_be_last(void)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	return (int) PG_ENCODING_BE_LAST;
}

const char *
pg_stub_enc_name(int enc)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	if (enc < 0 || enc >= (int) _PG_LAST_ENCODING_)
		return 0;
	return pg_enc2name_tbl[enc].name;
}

int
pg_stub_enc_enum_value(int enc)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	if (enc < 0 || enc >= (int) _PG_LAST_ENCODING_)
		return -1;
	return (int) pg_enc2name_tbl[enc].encoding;
}

int
pg_stub_enc_maxmblen(int enc)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	if (enc < 0 || enc >= (int) _PG_LAST_ENCODING_)
		return -1;
	return pg_stub_maxmblen_tbl[enc];
}
