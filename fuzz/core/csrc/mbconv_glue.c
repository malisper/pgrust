/*
 * mbconv_glue.c — native accessors for the mbconv oracle's thread-local
 * error-class flag (p1-lanez). pg_mbconv_err is __thread under
 * PG_MBCONV_TLS (see proofs/mbconv/c/pg_mbconv.h); stable Rust cannot bind
 * an extern TLS static, so the fuzz/exhaustive drivers reset and read it
 * through these two calls. Plumbing only — no logic.
 */
#include "pg_mbconv.h"
#include <string.h>
#include <stdio.h>

int
pg_mbconv_err_get(void)
{
	return pg_mbconv_err;
}

void
pg_mbconv_err_reset(void)
{
	pg_mbconv_err = 0;
}

/*
 * appendStringInfoStringQuoted — verbatim PostgreSQL 18.3
 * src/backend/utils/mb/stringinfo_mb.c logic over a caller-supplied flat
 * buffer (shims, plumbing only: appendStringInfoCharMacro -> *p++,
 * appendBinaryStringInfoNT -> memcpy, appendStringInfo("%s...'") ->
 * sprintf, pnstrdup -> local copy). pg_mbcliplen comes from pg_name_io.c's
 * vendored UTF8-pinned pg_encoding_mbcliplen (same convention as the Rust
 * side's thread-pinned UTF8 database encoding in the driver).
 * Caller guarantees out has room (2*strlen(s) + 8). Returns strlen(out).
 */
/*
 * mbutils.c pg_encoding_mbcliplen loop, verbatim modulo the mblen dispatch
 * shim (pg_wchar_table[...].mblen -> the vendored pg_utf_mblen in
 * pg_mbconv_common.c — this TU is compiled with the same rename defines, so
 * the token resolves to the mbconv family's own copy). DATABASE ENCODING
 * FIXED = UTF8, mirroring the Rust driver's thread pin (name_diff
 * convention).
 */
static int
pg_mbcliplen(const char *mbstr, int len, int limit)
{
	int			clen = 0;
	int			l;

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

int
pg_diff_append_quoted(const char *s, int maxlen, char *out)
{
	char	   *p = out;
	char		copybuf[4096];
	char	   *copy = NULL;
	const char *chunk_search_start,
			   *chunk_copy_start,
			   *chunk_end;
	int			slen;
	bool		ellipsis;

	slen = strlen(s);
	if (maxlen >= 0 && maxlen < slen)
	{
		int			finallen = pg_mbcliplen(s, slen, maxlen);

		memcpy(copybuf, s, finallen);
		copybuf[finallen] = '\0';
		copy = copybuf;
		chunk_search_start = copy;
		chunk_copy_start = copy;

		ellipsis = true;
	}
	else
	{
		chunk_search_start = s;
		chunk_copy_start = s;

		ellipsis = false;
	}

	*p++ = '\'';

	while ((chunk_end = strchr(chunk_search_start, '\'')) != NULL)
	{
		/* copy including the found delimiting ' */
		memcpy(p, chunk_copy_start, chunk_end - chunk_copy_start + 1);
		p += chunk_end - chunk_copy_start + 1;

		/* in order to double it, include this ' into the next chunk as well */
		chunk_copy_start = chunk_end;
		chunk_search_start = chunk_end + 1;
	}

	/* copy the last chunk and terminate */
	if (ellipsis)
		p += sprintf(p, "%s...'", chunk_copy_start);
	else
		p += sprintf(p, "%s'", chunk_copy_start);

	(void) copy;
	*p = '\0';
	return (int) (p - out);
}
