/*
 * pg_name_io.c: vendored PostgreSQL 18.3 C oracle for the adt/name
 * differential fuzz target (decoder_fuzz::name_diff).
 *
 * Provenance (all bodies verbatim modulo the documented shims):
 *   - src/backend/utils/adt/name.c      PostgreSQL 18.3, upstream sha
 *     62d6c7d3df (namein, nameout, namerecv core, namesend core, namecmp,
 *     nameeq/ne/lt/le/gt/ge, btnamecmp, namestrcpy, namestrcmp,
 *     nameconcatoid)
 *   - src/backend/utils/adt/varlena.c   PostgreSQL 18.3 (varstr_cmp
 *     collate_is_c branch; nameeqtext/namenetext/btnametextcmp,
 *     texteqname/textnename/bttextnamecmp and the CmpCall-derived
 *     lt/le/ge/gt wrappers; text_name clip core)
 *   - src/backend/utils/mb/mbutils.c    PostgreSQL 18.3
 *     (pg_encoding_mbcliplen, cliplen)
 *   - src/common/wchar.c                PostgreSQL 18.3 (pg_utf_mblen)
 * Source of record: pgrust-fabled/vendor/postgres-src (PACKAGE_VERSION
 * '18.3'), matching proofs/name-ascii/c/pg_name_ascii.c provenance.
 *
 * Shims (plumbing only, never logic):
 *   1. PG_FUNCTION_ARGS unwrapping -> plain C signatures over
 *      (name[NAMEDATALEN]) blocks and (ptr,len) text payloads;
 *      PG_RETURN_BOOL -> int; palloc0 -> caller buffer zeroed here.
 *   2. strncmp/memcmp -> pg_ref_strncmp/pg_ref_memcmp: classic
 *      unsigned-char byte loops returning the RAW byte difference at the
 *      first mismatch (glibc convention; the raw magnitude is SQL-visible
 *      through btnamecmp/btnametextcmp, and the shipped Rust core
 *      documents/implements the same convention). Same shim precedent as
 *      proofs/name-ascii/c/pg_name_ascii.c shim 2 -- the host libc's
 *      memcmp magnitude is implementation-defined, glibc's (= real PG on
 *      the reference platform) is the byte difference.
 *   3. DATABASE ENCODING FIXED = UTF8: pg_mbcliplen(s,len,limit) ->
 *      pg_encoding_mbcliplen(PG_UTF8, ...) with the mblen_fn table lookup
 *      resolved to pg_utf_mblen and pg_encoding_max_length(PG_UTF8) = 4.
 *      This matches what SHIPPED pgrust does: namein delegates to the
 *      pg_mbcliplen seam, production installs mbutils::pg_mbcliplen which
 *      dispatches on the database encoding; the fuzz driver pins the
 *      database encoding to UTF8 (PostgreSQL's default) on both sides, so
 *      the differential exercises the real multibyte char-boundary clip.
 *      (Under SQL_ASCII real PG takes the max_length==1 cliplen arm =
 *      min(len,limit) for NUL-free input; that arm is pinned by
 *      proofs/name-ascii instead.)
 *   4. namerecv: pq_getmsgtext -> pg_client_to_server -> pg_any_to_server.
 *      With client encoding SQL_ASCII (the driver leaves pgrust's default)
 *      C performs NO conversion but MUST STILL VALIDATE the bytes against
 *      the database encoding (mbutils.c pg_any_to_server: "No conversion
 *      is needed, but we must still validate the data"), raising
 *      ERRCODE_CHARACTER_NOT_IN_REPERTOIRE (22021) on invalid UTF8 --
 *      vendored verbatim below (pg_verify_mbstr_len UTF8 walk +
 *      pg_utf8_verifychar + pg_utf8_islegal from src/common/wchar.c).
 *      Empty payloads skip validation (pg_any_to_server len <= 0 early
 *      return). The ereports become errcode classes via the _Thread_local
 *      capture below (message text out of scope): 1 = 42622 name-too-long,
 *      2 = 22021 invalid byte sequence.
 *   5. namesend: pq_begintypsend/pq_sendtext/pq_endtypsend framing owned
 *      by pqformat on the Rust side; the C VALUE core is the payload =
 *      strlen(NameStr(*s)) bytes (no conversion, shim 4). Exposed via
 *      pg_diff_nameout's prefix.
 *   6. snprintf(suffix, 20, "_%u", oid) in nameconcatoid kept as the real
 *      libc snprintf ("_%u" of a u32 is locale/platform-invariant).
 */

#include <stdio.h>
#include <string.h>

typedef unsigned int Oid;

#define NAMEDATALEN 64
#define C_COLLATION_OID 950		/* catalog/pg_collation.h */
#define PG_UTF8 6				/* mb/pg_wchar.h */

#ifndef Min
#define Min(x, y) ((x) < (y) ? (x) : (y))
#endif

/* ---- error capture (comparator plane 2/3: verdict + errcode class) ---- */

#define C_NAME_ERR_NONE 0
#define C_NAME_ERR_NAME_TOO_LONG 1		/* 42622 */
#define C_NAME_ERR_NOT_IN_REPERTOIRE 2	/* 22021 invalid byte sequence */

static _Thread_local int pg_name_errcode = C_NAME_ERR_NONE;

int
pg_diff_name_errcode_get(void)
{
	return pg_name_errcode;
}

/* ---- shim 2: raw-difference byte-loop strncmp/memcmp ---- */

static int
pg_ref_strncmp(const unsigned char *s1, const unsigned char *s2, unsigned long n)
{
	unsigned long i;

	for (i = 0; i < n; i++)
	{
		if (s1[i] != s2[i])
			return (int) s1[i] - (int) s2[i];
		if (s1[i] == 0)
			return 0;
	}
	return 0;
}

static int
pg_ref_memcmp(const unsigned char *s1, const unsigned char *s2, unsigned long n)
{
	unsigned long i;

	for (i = 0; i < n; i++)
	{
		if (s1[i] != s2[i])
			return (int) s1[i] - (int) s2[i];
	}
	return 0;
}

/* ---- src/common/wchar.c: pg_utf_mblen (verbatim) ---- */

static int
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
	else
		len = 1;
	return len;
}

/* ---- src/common/wchar.c: pg_utf8_islegal (verbatim) ---- */

static int
pg_utf8_islegal(const unsigned char *source, int length)
{
	unsigned char a;

	switch (length)
	{
		default:
			/* reject lengths 5 and 6 for now */
			return 0;
		case 4:
			a = source[3];
			if (a < 0x80 || a > 0xBF)
				return 0;
			/* FALL THRU */
		case 3:
			a = source[2];
			if (a < 0x80 || a > 0xBF)
				return 0;
			/* FALL THRU */
		case 2:
			a = source[1];
			switch (*source)
			{
				case 0xE0:
					if (a < 0xA0 || a > 0xBF)
						return 0;
					break;
				case 0xED:
					if (a < 0x80 || a > 0x9F)
						return 0;
					break;
				case 0xF0:
					if (a < 0x90 || a > 0xBF)
						return 0;
					break;
				case 0xF4:
					if (a < 0x80 || a > 0x8F)
						return 0;
					break;
				default:
					if (a < 0x80 || a > 0xBF)
						return 0;
					break;
			}
			/* FALL THRU */
		case 1:
			a = *source;
			if (a >= 0x80 && a < 0xC2)
				return 0;
			if (a > 0xF4)
				return 0;
			break;
	}
	return 1;
}

/* ---- src/common/wchar.c: pg_utf8_verifychar (verbatim) ---- */

static int
pg_utf8_verifychar(const unsigned char *s, int len)
{
	int			l;

	if ((*s & 0x80) == 0)
	{
		if (*s == '\0')
			return -1;
		return 1;
	}
	else if ((*s & 0xe0) == 0xc0)
		l = 2;
	else if ((*s & 0xf0) == 0xe0)
		l = 3;
	else if ((*s & 0xf8) == 0xf0)
		l = 4;
	else
		l = 1;

	if (l > len)
		return -1;

	if (!pg_utf8_islegal(s, l))
		return -1;

	return l;
}

/*
 * mbutils.c pg_verify_mbstr_len multibyte walk (verbatim modulo shim 3:
 * encoding fixed UTF8, mbverifychar resolved to pg_utf8_verifychar;
 * report_invalid_encoding -> errcode class 2). noError=false posture:
 * returns 0 on valid, -1 with the errcode set on invalid.
 */
static int
pg_verify_mbstr_utf8(const unsigned char *mbstr, int len)
{
	while (len > 0)
	{
		int			l;

		/* fast path for ASCII-subset characters */
		if (!(*mbstr & 0x80))
		{
			if (*mbstr != '\0')
			{
				mbstr++;
				len--;
				continue;
			}
			pg_name_errcode = C_NAME_ERR_NOT_IN_REPERTOIRE;
			return -1;
		}

		l = pg_utf8_verifychar(mbstr, len);

		if (l < 0)
		{
			pg_name_errcode = C_NAME_ERR_NOT_IN_REPERTOIRE;
			return -1;
		}

		mbstr += l;
		len -= l;
	}
	return 0;
}

/* shim 3: encoding fixed UTF8 */
static int
pg_encoding_max_length(int encoding)
{
	(void) encoding;			/* PG_UTF8 only */
	return 4;
}

/* ---- src/backend/utils/mb/mbutils.c: cliplen (verbatim) ---- */

static int
cliplen(const char *str, int len, int limit)
{
	int			l = 0;

	len = Min(len, limit);
	while (l < len && str[l])
		l++;
	return l;
}

/*
 * mbutils.c: pg_encoding_mbcliplen (verbatim modulo shim 3: mblen_fn table
 * lookup resolved to pg_utf_mblen).
 */
static int
pg_encoding_mbcliplen(int encoding, const char *mbstr,
					  int len, int limit)
{
	int			clen = 0;
	int			l;

	/* optimization for single byte encoding */
	if (pg_encoding_max_length(encoding) == 1)
		return cliplen(mbstr, len, limit);

	while (len > 0 && *mbstr)
	{
		l = pg_utf_mblen((const unsigned char *) mbstr);
		if ((clen + l) > limit)
			break;
		clen += l;
		if (clen == limit)
			break;
		len -= l;
		mbstr += l;
	}
	return clen;
}

static int
pg_mbcliplen(const char *mbstr, int len, int limit)
{
	return pg_encoding_mbcliplen(PG_UTF8, mbstr, len, limit);
}

/* ======================= name.c ======================= */

/*
 * namein (verbatim modulo shim 1). Returns the copied length (diagnostic).
 * s is NUL-terminated (cstring contract).
 */
int
pg_diff_namein(const char *s, unsigned char *result /* [NAMEDATALEN] */ )
{
	int			len;

	pg_name_errcode = C_NAME_ERR_NONE;
	len = (int) strlen(s);

	/* Truncate oversize input */
	if (len >= NAMEDATALEN)
		len = pg_mbcliplen(s, len, NAMEDATALEN - 1);

	/* We use palloc0 here to ensure result is zero-padded */
	memset(result, 0, NAMEDATALEN);
	memcpy(result, s, len);

	return len;
}

/*
 * nameout: pstrdup(NameStr(*s)) -> strlen-prefix copy + NUL into a caller
 * buffer (>= NAMEDATALEN + 1). Returns strlen. Also serves as the namesend
 * payload core (shim 5).
 */
int
pg_diff_nameout(const unsigned char *name, unsigned char *out)
{
	int			len = (int) strlen((const char *) name);

	memcpy(out, name, len);
	out[len] = 0;
	return len;
}

/*
 * namerecv core (verbatim value logic; shims 1/4). payload/nbytes = the
 * message bytes after pq_getmsgtext's identity pass. Returns nbytes on
 * success, -1 with errcode class 1 on the too-long ereport.
 */
int
pg_diff_namerecv(const unsigned char *payload, int nbytes,
				 unsigned char *result /* [NAMEDATALEN] */ )
{
	pg_name_errcode = C_NAME_ERR_NONE;
	/*
	 * pq_getmsgtext -> pg_client_to_server -> pg_any_to_server (shim 4):
	 * client SQL_ASCII, database UTF8 => no conversion but mandatory
	 * validation; empty input skips it (len <= 0 early return).
	 */
	if (nbytes > 0 && pg_verify_mbstr_utf8(payload, nbytes) < 0)
		return -1;
	if (nbytes >= NAMEDATALEN)
	{
		pg_name_errcode = C_NAME_ERR_NAME_TOO_LONG;
		return -1;
	}
	memset(result, 0, NAMEDATALEN);
	memcpy(result, payload, nbytes);
	return nbytes;
}

/*
 * namecmp (verbatim modulo shim 2). C COLLATION ONLY: the varstr_cmp
 * locale path is out of the differential's scope (the crate's carve of
 * record); the driver never passes another collid.
 */
static int
namecmp_c(const unsigned char *arg1, const unsigned char *arg2, Oid collid)
{
	/* Fast path for common case used in system catalogs */
	if (collid == C_COLLATION_OID)
		return pg_ref_strncmp(arg1, arg2, NAMEDATALEN);

	/* Else rely on the varstr infrastructure -- out of scope, poison */
	return -2147483647;
}

int
pg_diff_nameeq(const unsigned char *arg1, const unsigned char *arg2)
{
	return namecmp_c(arg1, arg2, C_COLLATION_OID) == 0;
}

int
pg_diff_namene(const unsigned char *arg1, const unsigned char *arg2)
{
	return namecmp_c(arg1, arg2, C_COLLATION_OID) != 0;
}

int
pg_diff_namelt(const unsigned char *arg1, const unsigned char *arg2)
{
	return namecmp_c(arg1, arg2, C_COLLATION_OID) < 0;
}

int
pg_diff_namele(const unsigned char *arg1, const unsigned char *arg2)
{
	return namecmp_c(arg1, arg2, C_COLLATION_OID) <= 0;
}

int
pg_diff_namegt(const unsigned char *arg1, const unsigned char *arg2)
{
	return namecmp_c(arg1, arg2, C_COLLATION_OID) > 0;
}

int
pg_diff_namege(const unsigned char *arg1, const unsigned char *arg2)
{
	return namecmp_c(arg1, arg2, C_COLLATION_OID) >= 0;
}

int
pg_diff_btnamecmp(const unsigned char *arg1, const unsigned char *arg2)
{
	return namecmp_c(arg1, arg2, C_COLLATION_OID);
}

/* namestrcpy (verbatim: strncpy + forced terminator) */
void
pg_diff_namestrcpy(unsigned char *name /* [NAMEDATALEN] */ , const char *str)
{
	/* NB: We need to zero-pad the destination. */
	strncpy((char *) name, str, NAMEDATALEN);
	name[NAMEDATALEN - 1] = '\0';
}

/* namestrcmp (verbatim modulo shim 2); NULL args via flags */
int
pg_diff_namestrcmp(const unsigned char *name, const char *str)
{
	if (!name && !str)
		return 0;
	if (!name)
		return -1;				/* NULL < anything */
	if (!str)
		return 1;				/* NULL < anything */
	return pg_ref_strncmp(name, (const unsigned char *) str, NAMEDATALEN);
}

/* nameconcatoid (verbatim modulo shims 1/6) */
int
pg_diff_nameconcatoid(const unsigned char *nam, unsigned int oid,
					  unsigned char *result /* [NAMEDATALEN] */ )
{
	char		suffix[20];
	int			suflen;
	int			namlen;

	suflen = snprintf(suffix, sizeof(suffix), "_%u", oid);
	namlen = (int) strlen((const char *) nam);

	/* Truncate oversize input by truncating name part, not suffix */
	if (namlen + suflen >= NAMEDATALEN)
		namlen = pg_mbcliplen((const char *) nam, namlen,
							  NAMEDATALEN - 1 - suflen);

	/* We use palloc0 here to ensure result is zero-padded */
	memset(result, 0, NAMEDATALEN);
	memcpy(result, nam, namlen);
	memcpy(result + namlen, suffix, suflen);

	return namlen + suflen;
}

/* ======================= varlena.c ======================= */

/*
 * varstr_cmp, collate_is_c branch (verbatim modulo shim 2). C collation
 * only -- pg_newlocale_from_collation(C_COLLATION_OID)->collate_is_c is
 * true by catalog definition.
 */
static int
varstr_cmp_c(const unsigned char *arg1, int len1,
			 const unsigned char *arg2, int len2)
{
	int			result;

	result = pg_ref_memcmp(arg1, arg2, Min(len1, len2));
	if ((result == 0) && (len1 != len2))
		result = (len1 < len2) ? -1 : 1;
	return result;
}

/* btnametextcmp (verbatim modulo shim 1; C collation) */
int
pg_diff_btnametextcmp(const unsigned char *arg1,
					  const unsigned char *arg2, int len2)
{
	return varstr_cmp_c(arg1, (int) strlen((const char *) arg1), arg2, len2);
}

/* bttextnamecmp (verbatim modulo shim 1; C collation) */
int
pg_diff_bttextnamecmp(const unsigned char *arg1, int len1,
					  const unsigned char *arg2)
{
	return varstr_cmp_c(arg1, len1, arg2, (int) strlen((const char *) arg2));
}

/* nameeqtext, C-collation arm (verbatim modulo shim 1/2) */
int
pg_diff_nameeqtext(const unsigned char *arg1,
				   const unsigned char *arg2, int len2)
{
	int			len1 = (int) strlen((const char *) arg1);

	return (len1 == len2 && pg_ref_memcmp(arg1, arg2, len1) == 0);
}

int
pg_diff_namenetext(const unsigned char *arg1,
				   const unsigned char *arg2, int len2)
{
	return !pg_diff_nameeqtext(arg1, arg2, len2);
}

/* namelttext/nameletext/namegetext/namegttext: CmpCall(btnametextcmp) */
int
pg_diff_namelttext(const unsigned char *a1, const unsigned char *a2, int l2)
{
	return pg_diff_btnametextcmp(a1, a2, l2) < 0;
}

int
pg_diff_nameletext(const unsigned char *a1, const unsigned char *a2, int l2)
{
	return pg_diff_btnametextcmp(a1, a2, l2) <= 0;
}

int
pg_diff_namegetext(const unsigned char *a1, const unsigned char *a2, int l2)
{
	return pg_diff_btnametextcmp(a1, a2, l2) >= 0;
}

int
pg_diff_namegttext(const unsigned char *a1, const unsigned char *a2, int l2)
{
	return pg_diff_btnametextcmp(a1, a2, l2) > 0;
}

/* texteqname, C-collation arm (verbatim modulo shim 1/2) */
int
pg_diff_texteqname(const unsigned char *arg1, int len1,
				   const unsigned char *arg2)
{
	int			len2 = (int) strlen((const char *) arg2);

	return (len1 == len2 && pg_ref_memcmp(arg1, arg2, len1) == 0);
}

int
pg_diff_textnename(const unsigned char *arg1, int len1,
				   const unsigned char *arg2)
{
	return !pg_diff_texteqname(arg1, len1, arg2);
}

/* textltname/textlename/textgename/textgtname: CmpCall(bttextnamecmp) */
int
pg_diff_textltname(const unsigned char *a1, int l1, const unsigned char *a2)
{
	return pg_diff_bttextnamecmp(a1, l1, a2) < 0;
}

int
pg_diff_textlename(const unsigned char *a1, int l1, const unsigned char *a2)
{
	return pg_diff_bttextnamecmp(a1, l1, a2) <= 0;
}

int
pg_diff_textgename(const unsigned char *a1, int l1, const unsigned char *a2)
{
	return pg_diff_bttextnamecmp(a1, l1, a2) >= 0;
}

int
pg_diff_textgtname(const unsigned char *a1, int l1, const unsigned char *a2)
{
	return pg_diff_bttextnamecmp(a1, l1, a2) > 0;
}

/*
 * text_name clip core (varlena.c; oids 407/1400): namein's truncation over
 * an explicit-length text payload -- NO strlen walk, embedded NULs are
 * data. Returns the copied length.
 */
int
pg_diff_text_name(const unsigned char *s, int len,
				  unsigned char *result /* [NAMEDATALEN] */ )
{
	pg_name_errcode = C_NAME_ERR_NONE;

	/* Truncate oversize input */
	if (len >= NAMEDATALEN)
		len = pg_mbcliplen((const char *) s, len, NAMEDATALEN - 1);

	/* We use palloc0 here to ensure result is zero-padded */
	memset(result, 0, NAMEDATALEN);
	memcpy(result, s, len);

	return len;
}
