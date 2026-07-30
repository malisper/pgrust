/*
 * pg_mbconv.h — shared declarations for the mbconv proof family.
 *
 * PROVENANCE: all macro values, the pg_enc enum, and the map/struct
 * typedefs below are copied VERBATIM from PostgreSQL
 * src/include/mb/pg_wchar.h @ REL_18_STABLE (fetched 2026-07-28).
 *
 * Shims in this header (plumbing only, never logic):
 *   - PROOF ereport rewires per the suite's PROOF_EREPORT_FLAG convention
 *     (see ../../support/c/pg_proof_shim.h): the vendored conversion
 *     engines abort via longjmp in C; here the report_* / ereport calls
 *     are macro-replaced at the exact program point with
 *     "set pg_mbconv_err = <class>; return -1;". Error message TEXT never
 *     crosses this seam — only the verdict class does:
 *        1 = report_invalid_encoding    (C errcode 22021)
 *        2 = report_untranslatable_char (C errcode 22P05)
 *        3 = ereport invalid-encoding-number (22023; unreachable with the
 *            pinned valid encodings the harnesses pass)
 *        9 = elog "unsupported character length" (defensive; unreachable —
 *            control continues as in C where the macro cannot return)
 *   - bool comes from <stdbool.h> via pg_proof_shim.h.
 */
#ifndef PG_MBCONV_H
#define PG_MBCONV_H

#include "../../support/c/pg_proof_shim.h"

/* ---- pg_wchar.h verbatim: encoding ids ---- */
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
#define PG_VALID_ENCODING(_enc) \
		((_enc) >= 0 && (_enc) < _PG_LAST_ENCODING_)

/* ---- pg_wchar.h verbatim: byte-class macros ---- */
#define SS2 0x8e				/* single shift 2 (JIS0201) */
#define SS3 0x8f				/* single shift 3 (JIS0212) */
#define ISSJISHEAD(c) (((c) >= 0x81 && (c) <= 0x9f) || ((c) >= 0xe0 && (c) <= 0xfc))
#define ISSJISTAIL(c) (((c) >= 0x40 && (c) <= 0x7e) || ((c) >= 0x80 && (c) <= 0xfc))
#define HIGHBIT					(0x80)
#define IS_HIGHBIT_SET(ch)		((unsigned char)(ch) & HIGHBIT)

#define LC_ISO8859_1		0x81	/* ISO8859 Latin 1 */
#define LC_ISO8859_2		0x82	/* ISO8859 Latin 2 */
#define LC_ISO8859_3		0x83	/* ISO8859 Latin 3 */
#define LC_ISO8859_4		0x84	/* ISO8859 Latin 4 */
#define LC_JISX0201K		0x89	/* Japanese 1 byte kana */
#define LC_JISX0201R		0x8a	/* Japanese 1 byte Roman */
#define LC_KOI8_R			0x8b	/* Cyrillic KOI8-R */
#define LC_ISO8859_5		0x8c	/* ISO8859 Cyrillic */
#define LC_GB2312_80		0x91	/* Chinese */
#define LC_JISX0208			0x92	/* Japanese Kanji (JIS X 0208) */
#define LC_KS5601			0x93	/* Korean */
#define LC_JISX0212			0x94	/* Japanese Kanji (JIS X 0212) */
#define LC_CNS11643_1		0x95	/* CNS 11643-1992 Plane 1 */
#define LC_CNS11643_2		0x96	/* CNS 11643-1992 Plane 2 */

#define LCPRV1_A		0x9a
#define LCPRV1_B		0x9b
#define IS_LC1(c)	((unsigned char)(c) >= 0x81 && (unsigned char)(c) <= 0x8d)
#define IS_LC2(c)	((unsigned char)(c) >= 0x90 && (unsigned char)(c) <= 0x99)
#define IS_LCPRV1(c)	((unsigned char)(c) == LCPRV1_A || (unsigned char)(c) == LCPRV1_B)
#define LCPRV2_A		0x9c
#define LCPRV2_B		0x9d
#define IS_LCPRV2(c)	((unsigned char)(c) == LCPRV2_A || (unsigned char)(c) == LCPRV2_B)

#define LC_CNS11643_3		0xf6
#define LC_CNS11643_4		0xf7
#define LC_CNS11643_5		0xf8
#define LC_CNS11643_6		0xf9
#define LC_CNS11643_7		0xfa

/* wchar.c verbatim */
#define NONUTF8_INVALID_BYTE0 (0x8d)
#define NONUTF8_INVALID_BYTE1 (' ')
#define IS_EUC_RANGE_VALID(c)	((c) >= 0xa1 && (c) <= 0xfe)

/* ---- pg_wchar.h verbatim: radix tree / combined map types ---- */
typedef struct
{
	const uint16 *chars16;
	const uint32 *chars32;

	uint32		b1root;
	uint8		b1_lower;
	uint8		b1_upper;

	uint32		b2root;
	uint8		b2_1_lower;
	uint8		b2_1_upper;
	uint8		b2_2_lower;
	uint8		b2_2_upper;

	uint32		b3root;
	uint8		b3_1_lower;
	uint8		b3_1_upper;
	uint8		b3_2_lower;
	uint8		b3_2_upper;
	uint8		b3_3_lower;
	uint8		b3_3_upper;

	uint32		b4root;
	uint8		b4_1_lower;
	uint8		b4_1_upper;
	uint8		b4_2_lower;
	uint8		b4_2_upper;
	uint8		b4_3_lower;
	uint8		b4_3_upper;
	uint8		b4_4_lower;
	uint8		b4_4_upper;
} pg_mb_radix_tree;

typedef struct
{
	uint32		utf1;			/* UTF-8 code 1 */
	uint32		utf2;			/* UTF-8 code 2 */
	uint32		code;			/* local code */
} pg_utf_to_local_combined;

typedef struct
{
	uint32		code;			/* local code */
	uint32		utf1;			/* UTF-8 code 1 */
	uint32		utf2;			/* UTF-8 code 2 */
} pg_local_to_utf_combined;

typedef uint32 (*utf_local_conversion_func) (uint32 code);

/* ---- PROOF ereport rewires (see header comment) ---- */
extern int	pg_mbconv_err;

#define report_invalid_encoding(enc, mbstr, len) \
	do { pg_mbconv_err = 1; return -1; } while (0)
#define report_untranslatable_char(senc, denc, mbstr, len) \
	do { pg_mbconv_err = 2; return -1; } while (0)
#define ereport(level, rest) \
	do { pg_mbconv_err = 3; return -1; } while (0)
#define elog(level, ...) \
	do { pg_mbconv_err = 9; } while (0)

/* ---- vendored wchar.c kernels (pg_mbconv_common.c) ---- */
int			pg_utf_mblen(const unsigned char *s);
bool		pg_utf8_islegal(const unsigned char *source, int length);
int			pg_mule_mblen(const unsigned char *s);
int			pg_encoding_verifymbchar(int encoding, const char *mbstr, int len);

/* ---- vendored conv.c engines (pg_mbconv_common.c) ---- */
int			local2local(const unsigned char *l, unsigned char *p, int len,
						int src_encoding, int dest_encoding,
						const unsigned char *tab, bool noError);
int			latin2mic(const unsigned char *l, unsigned char *p, int len,
					  int lc, int encoding, bool noError);
int			mic2latin(const unsigned char *mic, unsigned char *p, int len,
					  int lc, int encoding, bool noError);
int			latin2mic_with_table(const unsigned char *l, unsigned char *p,
								 int len, int lc, int encoding,
								 const unsigned char *tab, bool noError);
int			mic2latin_with_table(const unsigned char *mic, unsigned char *p,
								 int len, int lc, int encoding,
								 const unsigned char *tab, bool noError);
int			UtfToLocal(const unsigned char *utf, int len, unsigned char *iso,
					   const pg_mb_radix_tree *map,
					   const pg_utf_to_local_combined *cmap, int cmapsize,
					   utf_local_conversion_func conv_func,
					   int encoding, bool noError);
int			LocalToUtf(const unsigned char *iso, int len, unsigned char *utf,
					   const pg_mb_radix_tree *map,
					   const pg_local_to_utf_combined *cmap, int cmapsize,
					   utf_local_conversion_func conv_func,
					   int encoding, bool noError);

/* bsearch model (pg_mbconv_common.c): CBMC has no libc, see there */
void	   *bsearch(const void *key, const void *base, size_t nmemb,
					size_t size, int (*compar) (const void *, const void *));

#endif							/* PG_MBCONV_H */
