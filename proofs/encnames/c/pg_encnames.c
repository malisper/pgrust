/*
 * Vendored PostgreSQL C for the encoding-name lookup proofs
 * (PG_char_to_encoding oid 1264 / PG_encoding_to_char oid 1597).
 *
 * Provenance (all fetched 2026-07-28 from postgres/postgres REL_18_STABLE):
 *  - src/common/encnames.c: pg_encname struct + pg_encname_tbl (81 rows,
 *    lines ~32-299), DEF_ENC2NAME + pg_enc2name_tbl (lines ~301-355),
 *    clean_encoding_name / pg_char_to_encoding / pg_encoding_to_char
 *    (lines ~521-605) — tables and function bodies VERBATIM.
 *  - src/include/mb/pg_wchar.h: pg_enc enum (lines 240-289),
 *    PG_ENCODING_BE_LAST + PG_VALID_* macros (lines 291-307), pg_enc2name
 *    struct (lines 355-362; WIN32 codepage field compiled out, exactly as
 *    on any non-WIN32 build).
 *
 * SHIMS (everything else verbatim; this list is exhaustive):
 *  - shared typedefs/Assert via pg_proof_shim.h (c.h subset).
 *  - NAMEDATALEN 64 per src/include/pg_config_manual.h (default build).
 *  - isalnum((unsigned char) *p) -> pg_proof_isalnum((unsigned char) *p):
 *    Kani/CBMC has no libc ctype model.  Postgres's own comment on the
 *    table ("search routines use isalnum() chars only") is C-locale; the
 *    replacement below is the C-locale ASCII isalnum, total over unsigned
 *    char.  NOTE: glibc isalnum in a non-C locale could classify high
 *    bytes as alnum — PostgreSQL runs this in the server locale.  The
 *    proof models the C locale; the runner lane's ground-truthing on real
 *    glibc PG (see src/lib.rs, wrapper-plane harness) covers the locale
 *    question empirically before anything is recorded.
 *  - strlen/strcmp from <string.h>: CBMC built-in models.
 *  - linkage: pg_char_to_encoding / pg_encoding_to_char are extern in C
 *    already (vendored as-is, no rename needed — the pg_ prefix is
 *    upstream's); clean_encoding_name and pg_encname_tbl stay static,
 *    exactly as in encnames.c.
 *  - pg_encoding_to_char_frame (bottom): HARNESS PLUMBING, not vendored —
 *    projects the returned const char* into a 64-byte zero-padded
 *    NameData-style frame with namestrcpy truncation semantics
 *    (min(strlen, 63) bytes + zero fill) so the harness can byte-compare
 *    against the shipped Rust NameData::namestrcpy result.
 */

#include "../../support/c/pg_proof_shim.h"
#include <string.h>

#define NAMEDATALEN 64			/* pg_config_manual.h default */

/* C-locale isalnum replacement (see shim notes above) */
static inline int
pg_proof_isalnum(int c)
{
	return (c >= '0' && c <= '9') ||
		(c >= 'a' && c <= 'z') ||
		(c >= 'A' && c <= 'Z');
}

/* ---- src/include/mb/pg_wchar.h (verbatim) ---- */

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
	/* PG_ENCODING_BE_LAST points to the above entry */

	/* followings are for client encoding only */
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

/*
 * Please use these tests before access to pg_enc2name_tbl[]
 * or to other places...
 */
#define PG_VALID_BE_ENCODING(_enc) \
		((_enc) >= 0 && (_enc) <= PG_ENCODING_BE_LAST)

#define PG_ENCODING_IS_CLIENT_ONLY(_enc) \
		((_enc) > PG_ENCODING_BE_LAST && (_enc) < _PG_LAST_ENCODING_)

#define PG_VALID_ENCODING(_enc) \
		((_enc) >= 0 && (_enc) < _PG_LAST_ENCODING_)

/* On FE are possible all encodings */
#define PG_VALID_FE_ENCODING(_enc)	PG_VALID_ENCODING(_enc)

typedef struct pg_enc2name
{
	const char *name;
	pg_enc		encoding;
#ifdef WIN32
	unsigned	codepage;		/* codepage for WIN32 */
#endif
} pg_enc2name;

/* ---- src/common/encnames.c (verbatim) ---- */

typedef struct pg_encname
{
	const char *name;
	pg_enc		encoding;
} pg_encname;

static const pg_encname pg_encname_tbl[] =
{
	{
		"abc", PG_WIN1258
	},							/* alias for WIN1258 */
	{
		"alt", PG_WIN866
	},							/* IBM866 */
	{
		"big5", PG_BIG5
	},							/* Big5; Chinese for Taiwan multibyte set */
	{
		"euccn", PG_EUC_CN
	},							/* EUC-CN; Extended Unix Code for simplified
								 * Chinese */
	{
		"eucjis2004", PG_EUC_JIS_2004
	},							/* EUC-JIS-2004; Extended UNIX Code fixed
								 * Width for Japanese, standard JIS X 0213 */
	{
		"eucjp", PG_EUC_JP
	},							/* EUC-JP; Extended UNIX Code fixed Width for
								 * Japanese, standard OSF */
	{
		"euckr", PG_EUC_KR
	},							/* EUC-KR; Extended Unix Code for Korean
								 * precomposed (Wansung) encoding, standard KS
								 * X 1001 */
	{
		"euctw", PG_EUC_TW
	},							/* EUC-TW; Extended Unix Code for
								 *
								 * traditional Chinese */
	{
		"gb18030", PG_GB18030
	},							/* GB18030;GB18030 */
	{
		"gbk", PG_GBK
	},							/* GBK; Chinese Windows CodePage 936
								 * simplified Chinese */
	{
		"iso88591", PG_LATIN1
	},							/* ISO-8859-1; RFC1345,KXS2 */
	{
		"iso885910", PG_LATIN6
	},							/* ISO-8859-10; RFC1345,KXS2 */
	{
		"iso885913", PG_LATIN7
	},							/* ISO-8859-13; RFC1345,KXS2 */
	{
		"iso885914", PG_LATIN8
	},							/* ISO-8859-14; RFC1345,KXS2 */
	{
		"iso885915", PG_LATIN9
	},							/* ISO-8859-15; RFC1345,KXS2 */
	{
		"iso885916", PG_LATIN10
	},							/* ISO-8859-16; RFC1345,KXS2 */
	{
		"iso88592", PG_LATIN2
	},							/* ISO-8859-2; RFC1345,KXS2 */
	{
		"iso88593", PG_LATIN3
	},							/* ISO-8859-3; RFC1345,KXS2 */
	{
		"iso88594", PG_LATIN4
	},							/* ISO-8859-4; RFC1345,KXS2 */
	{
		"iso88595", PG_ISO_8859_5
	},							/* ISO-8859-5; RFC1345,KXS2 */
	{
		"iso88596", PG_ISO_8859_6
	},							/* ISO-8859-6; RFC1345,KXS2 */
	{
		"iso88597", PG_ISO_8859_7
	},							/* ISO-8859-7; RFC1345,KXS2 */
	{
		"iso88598", PG_ISO_8859_8
	},							/* ISO-8859-8; RFC1345,KXS2 */
	{
		"iso88599", PG_LATIN5
	},							/* ISO-8859-9; RFC1345,KXS2 */
	{
		"johab", PG_JOHAB
	},							/* JOHAB; Korean combining (Johab) encoding,
								 * standard KS X 1001 annex 3 */
	{
		"koi8", PG_KOI8R
	},							/* _dirty_ alias for KOI8-R (backward
								 * compatibility) */
	{
		"koi8r", PG_KOI8R
	},							/* KOI8-R; RFC1489 */
	{
		"koi8u", PG_KOI8U
	},							/* KOI8-U; RFC2319 */
	{
		"latin1", PG_LATIN1
	},							/* alias for ISO-8859-1 */
	{
		"latin10", PG_LATIN10
	},							/* alias for ISO-8859-16 */
	{
		"latin2", PG_LATIN2
	},							/* alias for ISO-8859-2 */
	{
		"latin3", PG_LATIN3
	},							/* alias for ISO-8859-3 */
	{
		"latin4", PG_LATIN4
	},							/* alias for ISO-8859-4 */
	{
		"latin5", PG_LATIN5
	},							/* alias for ISO-8859-9 */
	{
		"latin6", PG_LATIN6
	},							/* alias for ISO-8859-10 */
	{
		"latin7", PG_LATIN7
	},							/* alias for ISO-8859-13 */
	{
		"latin8", PG_LATIN8
	},							/* alias for ISO-8859-14 */
	{
		"latin9", PG_LATIN9
	},							/* alias for ISO-8859-15 */
	{
		"mskanji", PG_SJIS
	},							/* alias for Shift_JIS */
	{
		"muleinternal", PG_MULE_INTERNAL
	},
	{
		"shiftjis", PG_SJIS
	},							/* Shift_JIS; JIS X 0202-1991 */

	{
		"shiftjis2004", PG_SHIFT_JIS_2004
	},							/* SHIFT-JIS-2004; Shift JIS for Japanese,
								 * standard JIS X 0213 */
	{
		"sjis", PG_SJIS
	},							/* alias for Shift_JIS */
	{
		"sqlascii", PG_SQL_ASCII
	},
	{
		"tcvn", PG_WIN1258
	},							/* alias for WIN1258 */
	{
		"tcvn5712", PG_WIN1258
	},							/* alias for WIN1258 */
	{
		"uhc", PG_UHC
	},							/* UHC; Unified Hangul Code, Microsoft Windows
								 * CodePage 949; superset of EUC-KR covering
								 * all 11,172 precomposed Hangul syllables */
	{
		"unicode", PG_UTF8
	},							/* alias for UTF8 */
	{
		"utf8", PG_UTF8
	},							/* alias for UTF8 */
	{
		"vscii", PG_WIN1258
	},							/* alias for WIN1258 */
	{
		"win", PG_WIN1251
	},							/* _dirty_ alias for windows-1251 (backward
								 * compatibility) */
	{
		"win1250", PG_WIN1250
	},							/* alias for Windows-1250 */
	{
		"win1251", PG_WIN1251
	},							/* alias for Windows-1251 */
	{
		"win1252", PG_WIN1252
	},							/* alias for Windows-1252 */
	{
		"win1253", PG_WIN1253
	},							/* alias for Windows-1253 */
	{
		"win1254", PG_WIN1254
	},							/* alias for Windows-1254 */
	{
		"win1255", PG_WIN1255
	},							/* alias for Windows-1255 */
	{
		"win1256", PG_WIN1256
	},							/* alias for Windows-1256 */
	{
		"win1257", PG_WIN1257
	},							/* alias for Windows-1257 */
	{
		"win1258", PG_WIN1258
	},							/* alias for Windows-1258 */
	{
		"win866", PG_WIN866
	},							/* IBM866 */
	{
		"win874", PG_WIN874
	},							/* alias for Windows-874 */
	{
		"win932", PG_SJIS
	},							/* alias for Shift_JIS */
	{
		"win936", PG_GBK
	},							/* alias for GBK */
	{
		"win949", PG_UHC
	},							/* alias for UHC */
	{
		"win950", PG_BIG5
	},							/* alias for BIG5 */
	{
		"windows1250", PG_WIN1250
	},							/* Windows-1251; Microsoft */
	{
		"windows1251", PG_WIN1251
	},							/* Windows-1251; Microsoft */
	{
		"windows1252", PG_WIN1252
	},							/* Windows-1252; Microsoft */
	{
		"windows1253", PG_WIN1253
	},							/* Windows-1253; Microsoft */
	{
		"windows1254", PG_WIN1254
	},							/* Windows-1254; Microsoft */
	{
		"windows1255", PG_WIN1255
	},							/* Windows-1255; Microsoft */
	{
		"windows1256", PG_WIN1256
	},							/* Windows-1256; Microsoft */
	{
		"windows1257", PG_WIN1257
	},							/* Windows-1257; Microsoft */
	{
		"windows1258", PG_WIN1258
	},							/* Windows-1258; Microsoft */
	{
		"windows866", PG_WIN866
	},							/* IBM866 */
	{
		"windows874", PG_WIN874
	},							/* Windows-874; Microsoft */
	{
		"windows932", PG_SJIS
	},							/* alias for Shift_JIS */
	{
		"windows936", PG_GBK
	},							/* alias for GBK */
	{
		"windows949", PG_UHC
	},							/* alias for UHC */
	{
		"windows950", PG_BIG5
	}							/* alias for BIG5 */
};

/* ----------
 * These are "official" encoding names.
 * ----------
 */
#ifndef WIN32
#define DEF_ENC2NAME(name, codepage) { #name, PG_##name }
#else
#define DEF_ENC2NAME(name, codepage) { #name, PG_##name, codepage }
#endif

const pg_enc2name pg_enc2name_tbl[] =
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

/*
 * Remove irrelevant chars from encoding name, store at *newkey
 *
 * (Caller's responsibility to provide a large enough buffer)
 */
static char *
clean_encoding_name(const char *key, char *newkey)
{
	const char *p;
	char	   *np;

	for (p = key, np = newkey; *p != '\0'; p++)
	{
		if (pg_proof_isalnum((unsigned char) *p))
		{
			if (*p >= 'A' && *p <= 'Z')
				*np++ = *p + 'a' - 'A';
			else
				*np++ = *p;
		}
	}
	*np = '\0';
	return newkey;
}

/*
 * Search encoding by encoding name
 *
 * Returns encoding ID, or -1 if not recognized
 */
int
pg_char_to_encoding(const char *name)
{
	unsigned int nel = lengthof(pg_encname_tbl);
	const pg_encname *base = pg_encname_tbl,
			   *last = base + nel - 1,
			   *position;
	int			result;
	char		buff[NAMEDATALEN],
			   *key;

	if (name == NULL || *name == '\0')
		return -1;

	if (strlen(name) >= NAMEDATALEN)
		return -1;				/* it's certainly not in the table */

	key = clean_encoding_name(name, buff);

	while (last >= base)
	{
		position = base + ((last - base) >> 1);
		result = key[0] - position->name[0];

		if (result == 0)
		{
			result = strcmp(key, position->name);
			if (result == 0)
				return position->encoding;
		}
		if (result < 0)
			last = position - 1;
		else
			base = position + 1;
	}
	return -1;
}

const char *
pg_encoding_to_char(int encoding)
{
	if (PG_VALID_ENCODING(encoding))
	{
		const pg_enc2name *p = &pg_enc2name_tbl[encoding];

		Assert(encoding == p->encoding);
		return p->name;
	}
	return "";
}

/* ---- harness plumbing (NOT vendored postgres code) ---- */

/*
 * Project pg_encoding_to_char's result into a 64-byte zero-padded frame
 * with namestrcpy truncation semantics (namestrcpy: zero-fill NAMEDATALEN,
 * then copy min(strlen(src), NAMEDATALEN-1) bytes).  Lets the harness
 * byte-compare the whole NameData image against the shipped Rust
 * types_tuple::NameData::namestrcpy output.
 */
int								/* int (not void): Kani lowers Rust () as
								 * struct Unit, which goto-cc rejects */
pg_encoding_to_char_frame(int encoding, unsigned char *out)
{
	const char *name = pg_encoding_to_char(encoding);
	int			i;

	for (i = 0; i < NAMEDATALEN; i++)
		out[i] = 0;
	for (i = 0; i < NAMEDATALEN - 1 && name[i] != '\0'; i++)
		out[i] = (unsigned char) name[i];
	return 0;
}
