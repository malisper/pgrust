/*
 * Vendored PostgreSQL C: ascii.c conversion cores — differential-fuzz oracle.
 *
 * Provenance (bodies VERBATIM unless a shim is listed below):
 *   - src/backend/utils/adt/ascii.c @ postgres-src
 *     62d6c7d3df6287f1bd83199c1a746e50d31571a0 (REL_18, the repo's vendored
 *     ground-truth checkout ../pgrust-fabled/vendor/postgres-src):
 *     pg_to_ascii, ascii_safe_strlcpy — verbatim.
 *   - pg_enc enum: verbatim via shim/mb/pg_wchar.h.
 *
 * Shims (plumbing only, never logic):
 *   - ereport(ERROR, (errcode(X), errmsg(...))) -> record X in
 *     pg_hashenc_errcode; the following `return` statement in the verbatim
 *     body then exits as C's longjmp would. The comparator checks the
 *     errcode class, not message text.
 *   - errcode symbols -> small ints (see pg_hashenc_glue.c for the map).
 *   - The fmgr-level to_ascii_* wrappers are NOT vendored (fcinfo/varlena
 *     plumbing); their check logic (PG_VALID_ENCODING, pg_char_to_encoding)
 *     is compared at the glue level.
 */

#include "postgres.h"

#include "mb/pg_wchar.h"

extern _Thread_local int pg_hashenc_errcode;

#define ERRCODE_FEATURE_NOT_SUPPORTED 1
#define errcode(c) (pg_hashenc_errcode = (c))
#define errmsg(...) 0
#define ereport(level, stuff) do { (void) (stuff); } while (0)
#define ERROR 21

/* ----------
 * to_ascii
 * ----------
 */
void
pg_to_ascii(unsigned char *src, unsigned char *src_end, unsigned char *dest, int enc)
{
	unsigned char *x;
	const unsigned char *ascii;
	int			range;

	/*
	 * relevant start for an encoding
	 */
#define RANGE_128	128
#define RANGE_160	160

	if (enc == PG_LATIN1)
	{
		/*
		 * ISO-8859-1 <range: 160 -- 255>
		 */
		ascii = (const unsigned char *) "  cL Y  \"Ca  -R     'u .,      ?AAAAAAACEEEEIIII NOOOOOxOUUUUYTBaaaaaaaceeeeiiii nooooo/ouuuuyty";
		range = RANGE_160;
	}
	else if (enc == PG_LATIN2)
	{
		/*
		 * ISO-8859-2 <range: 160 -- 255>
		 */
		ascii = (const unsigned char *) " A L LS \"SSTZ-ZZ a,l'ls ,sstz\"zzRAAAALCCCEEEEIIDDNNOOOOxRUUUUYTBraaaalccceeeeiiddnnoooo/ruuuuyt.";
		range = RANGE_160;
	}
	else if (enc == PG_LATIN9)
	{
		/*
		 * ISO-8859-15 <range: 160 -- 255>
		 */
		ascii = (const unsigned char *) "  cL YS sCa  -R     Zu .z   EeY?AAAAAAACEEEEIIII NOOOOOxOUUUUYTBaaaaaaaceeeeiiii nooooo/ouuuuyty";
		range = RANGE_160;
	}
	else if (enc == PG_WIN1250)
	{
		/*
		 * Window CP1250 <range: 128 -- 255>
		 */
		ascii = (const unsigned char *) "  ' \"    %S<STZZ `'\"\".--  s>stzz   L A  \"CS  -RZ  ,l'u .,as L\"lzRAAAALCCCEEEEIIDDNNOOOOxRUUUUYTBraaaalccceeeeiiddnnoooo/ruuuuyt ";
		range = RANGE_128;
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("encoding conversion from %s to ASCII not supported",
						pg_encoding_to_char(enc))));
		return;					/* keep compiler quiet */
	}

	/*
	 * Encode
	 */
	for (x = src; x < src_end; x++)
	{
		if (*x < 128)
			*dest++ = *x;
		else if (*x < range)
			*dest++ = ' ';		/* bogus 128 to 'range' */
		else
			*dest++ = ascii[*x - range];
	}
}

/* ----------
 * Copy a string in an arbitrary backend-safe encoding, converting it to a
 * valid ASCII string by replacing non-ASCII bytes with '?'.  Otherwise the
 * behavior is identical to strlcpy(), except that we don't bother with a
 * return value.
 *
 * This must not trigger ereport(ERROR), as it is called in postmaster.
 * ----------
 */
void
ascii_safe_strlcpy(char *dest, const char *src, size_t destsiz)
{
	if (destsiz == 0)			/* corner case: no room for trailing nul */
		return;

	while (--destsiz > 0)
	{
		/* use unsigned char here to avoid compiler warning */
		unsigned char ch = *src++;

		if (ch == '\0')
			break;
		/* Keep printable ASCII characters */
		if (32 <= ch && ch <= 127)
			*dest = ch;
		/* White-space is also OK */
		else if (ch == '\n' || ch == '\r' || ch == '\t')
			*dest = ch;
		/* Everything else is replaced with '?' */
		else
			*dest = '?';
		dest++;
	}

	*dest = '\0';
}
