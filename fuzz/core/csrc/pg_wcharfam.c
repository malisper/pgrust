/*
 * pg_wcharfam.c — differential-fuzz oracle for the p1-laneah batch:
 * common/wchar (per-encoding mblen/dsplen/verifier tables + UTF-8
 * machinery) and mb/mbutils (pure verifier/length/clip/increment
 * wrappers + encnames lookups).
 *
 * Sources, ALL VERBATIM from the vendored PostgreSQL 18.3 tree
 * (pgrust-fabled/vendor/postgres-src @ 62d6c7d "Stamp 18.3"):
 *   - wcharfam/wchar.c            = src/common/wchar.c        (whole file)
 *   - wcharfam/encnames.c         = src/common/encnames.c     (whole file)
 *   - wcharfam/mb/pg_wchar.h      = src/include/mb/pg_wchar.h (whole file)
 *   - wcharfam/utils/ascii.h      = src/include/utils/ascii.h (whole file)
 *   - wcharfam/port/simd.h        = src/include/port/simd.h   (whole file)
 *   - wcharfam/common/unicode_*.h = generated tables          (whole files)
 *   - the mbutils extracts below  = src/backend/utils/mb/mbutils.c,
 *     line-exact regions noted at each paste site.
 *
 * Shims (plumbing only, never logic):
 *   - wcharfam/c.h: fixed-width typedefs + no-op Assert (same shim the
 *     wchar c_parity vector generator compiles against).
 *   - Every extern symbol wchar.c/encnames.c define is macro-renamed to
 *     wfam_* so this TU can coexist with sibling oracles (pg_name_io.c
 *     also vendors pg_utf_mblen). Bodies are untouched; the renames are
 *     the same #define-before-include device pg_wchar_kernels.c uses.
 *   - ereport(ERROR)/elog(ERROR) capture: errcode recorded, longjmp back
 *     to the wfam_x_* entry shim (verdict + sqlstate planes; message
 *     text out of scope per the campaign comparator contract).
 *   - DatabaseEncoding session cell: verbatim static + wfam_x_set_db_enc
 *     setter mirroring SetDatabaseEncoding's assignment (environment
 *     mocking, never computation).
 *   - VALGRIND_CHECK_MEM_IS_DEFINED -> no-op (memdebug.h does the same
 *     without USE_VALGRIND).
 */

#include <setjmp.h>
#include <stdio.h>
#include <string.h>

/* ---- extern-symbol renames (see header) ---- */
#define pg_encoding_dsplen              wfam_pg_encoding_dsplen
#define pg_encoding_max_length          wfam_pg_encoding_max_length
#define pg_encoding_mblen               wfam_pg_encoding_mblen
#define pg_encoding_mblen_bounded       wfam_pg_encoding_mblen_bounded
#define pg_encoding_mblen_or_incomplete wfam_pg_encoding_mblen_or_incomplete
#define pg_encoding_set_invalid         wfam_pg_encoding_set_invalid
#define pg_encoding_verifymbchar        wfam_pg_encoding_verifymbchar
#define pg_encoding_verifymbstr         wfam_pg_encoding_verifymbstr
#define pg_mule_mblen                   wfam_pg_mule_mblen
#define pg_utf_mblen                    wfam_pg_utf_mblen
#define pg_utf8_islegal                 wfam_pg_utf8_islegal
#define pg_wchar_table                  wfam_pg_wchar_table
#define pg_char_to_encoding             wfam_pg_char_to_encoding
#define pg_encoding_to_char             wfam_pg_encoding_to_char
#define pg_valid_client_encoding        wfam_pg_valid_client_encoding
#define pg_valid_server_encoding        wfam_pg_valid_server_encoding
#define pg_valid_server_encoding_id     wfam_pg_valid_server_encoding_id
#define is_encoding_supported_by_icu    wfam_is_encoding_supported_by_icu
#define get_encoding_name_for_icu       wfam_get_encoding_name_for_icu
#define pg_enc2name_tbl                 wfam_pg_enc2name_tbl
#define pg_enc2gettext_tbl              wfam_pg_enc2gettext_tbl

#include "wchar.c"
#include "encnames.c"
#include "pg_oracle_guard.h"	/* oracle-serialization holder check */

/* ---- error-capture shims (see header) ---- */
static _Thread_local jmp_buf wfam_env;
static _Thread_local int wfam_errcode_val;

/* verbatim MAKE_SQLSTATE encoding from src/include/utils/elog.h */
#define PGSIXBIT(ch)	(((ch) - '0') & 0x3f)
#define PGUNSIXBIT(val) (((val) & 0x3f) + '0')
#define MAKE_SQLSTATE(ch1,ch2,ch3,ch4,ch5)	\
	(PGSIXBIT(ch1) + (PGSIXBIT(ch2) << 6) + (PGSIXBIT(ch3) << 12) + \
	 (PGSIXBIT(ch4) << 18) + (PGSIXBIT(ch5) << 24))
#define ERRCODE_CHARACTER_NOT_IN_REPERTOIRE MAKE_SQLSTATE('2','2','0','2','1')
#define ERRCODE_UNTRANSLATABLE_CHARACTER    MAKE_SQLSTATE('2','2','P','0','5')
#define ERRCODE_INTERNAL_SHIM               MAKE_SQLSTATE('X','X','0','0','0')

#define errcode(sqlerrcode) (wfam_errcode_val = (sqlerrcode), 0)
#define errmsg(...) 0
#define errdetail(...) 0
#define ereport(elevel, ...) \
	do { (void) (__VA_ARGS__); longjmp(wfam_env, 1); } while (0)
#define elog(elevel, ...) \
	do { wfam_errcode_val = ERRCODE_INTERNAL_SHIM; longjmp(wfam_env, 1); } while (0)
#define VALGRIND_CHECK_MEM_IS_DEFINED(a, b) ((void) 0)

/* mbutils.c line 83 (verbatim body; _Thread_local added as SHIM: the C
 * backend is one process per session, the harness is one thread per
 * session — the Rust side's DATABASE_ENCODING is a thread_local Cell,
 * and parallel test threads must not race one process-global cell) */
static _Thread_local const pg_enc2name *DatabaseEncoding = &pg_enc2name_tbl[PG_SQL_ASCII];

/* ---- second rename block: mbutils.c extern symbols (same device) ---- */
#define pg_mb2wchar                     wfam_pg_mb2wchar
#define pg_mb2wchar_with_len            wfam_pg_mb2wchar_with_len
#define pg_encoding_mb2wchar_with_len   wfam_pg_encoding_mb2wchar_with_len
#define pg_wchar2mb                     wfam_pg_wchar2mb
#define pg_wchar2mb_with_len            wfam_pg_wchar2mb_with_len
#define pg_encoding_wchar2mb_with_len   wfam_pg_encoding_wchar2mb_with_len
#define pg_mblen_cstr                   wfam_pg_mblen_cstr
#define pg_mblen_range                  wfam_pg_mblen_range
#define pg_mblen_with_len               wfam_pg_mblen_with_len
#define pg_mblen_unbounded              wfam_pg_mblen_unbounded
#define pg_mblen                        wfam_pg_mblen
#define pg_dsplen                       wfam_pg_dsplen
#define pg_mbstrlen                     wfam_pg_mbstrlen
#define pg_mbstrlen_with_len            wfam_pg_mbstrlen_with_len
#define pg_mbcliplen                    wfam_pg_mbcliplen
#define pg_encoding_mbcliplen           wfam_pg_encoding_mbcliplen
#define pg_mbcharcliplen                wfam_pg_mbcharcliplen
#define GetDatabaseEncoding             wfam_GetDatabaseEncoding
#define pg_database_encoding_character_incrementer wfam_pg_database_encoding_character_incrementer
#define pg_database_encoding_max_length wfam_pg_database_encoding_max_length
#define pg_verifymbstr                  wfam_pg_verifymbstr
#define pg_verify_mbstr                 wfam_pg_verify_mbstr
#define pg_verify_mbstr_len             wfam_pg_verify_mbstr_len
#define report_invalid_encoding         wfam_report_invalid_encoding
#define report_untranslatable_char      wfam_report_untranslatable_char
#define check_encoding_conversion_args  wfam_check_encoding_conversion_args

/* forward declarations (mbutils.c has an equivalent block at its top;
 * the renames above apply to these too) */
static void report_invalid_encoding_db(const char *mbstr, int mblen, int len);
static void report_invalid_encoding_int(int encoding, const char *mbstr, int mblen, int len);
static int	cliplen(const char *str, int len, int limit);
extern int	pg_database_encoding_max_length(void);
extern bool pg_verify_mbstr(int encoding, const char *mbstr, int len, bool noError);
extern void report_invalid_encoding(int encoding, const char *mbstr, int len);
extern int	pg_encoding_mbcliplen(int encoding, const char *mbstr, int len, int limit);
#define pg_utf8_increment               wfam_pg_utf8_increment
#define pg_eucjp_increment              wfam_pg_eucjp_increment




/* ---- VERBATIM mbutils.c lines 984-1028 ---- */


/* convert a multibyte string to a wchar */
int
pg_mb2wchar(const char *from, pg_wchar *to)
{
	return pg_wchar_table[DatabaseEncoding->encoding].mb2wchar_with_len((const unsigned char *) from, to, strlen(from));
}

/* convert a multibyte string to a wchar with a limited length */
int
pg_mb2wchar_with_len(const char *from, pg_wchar *to, int len)
{
	return pg_wchar_table[DatabaseEncoding->encoding].mb2wchar_with_len((const unsigned char *) from, to, len);
}

/* same, with any encoding */
int
pg_encoding_mb2wchar_with_len(int encoding,
							  const char *from, pg_wchar *to, int len)
{
	return pg_wchar_table[encoding].mb2wchar_with_len((const unsigned char *) from, to, len);
}

/* convert a wchar string to a multibyte */
int
pg_wchar2mb(const pg_wchar *from, char *to)
{
	return pg_wchar_table[DatabaseEncoding->encoding].wchar2mb_with_len(from, (unsigned char *) to, pg_wchar_strlen(from));
}

/* convert a wchar string to a multibyte with a limited length */
int
pg_wchar2mb_with_len(const pg_wchar *from, char *to, int len)
{
	return pg_wchar_table[DatabaseEncoding->encoding].wchar2mb_with_len(from, (unsigned char *) to, len);
}

/* same, with any encoding */
int
pg_encoding_wchar2mb_with_len(int encoding,
							  const pg_wchar *from, char *to, int len)
{
	return pg_wchar_table[encoding].wchar2mb_with_len(from, (unsigned char *) to, len);
}

/* ---- VERBATIM mbutils.c lines 1029-1284 ---- */

/*
 * Returns the byte length of a multibyte character sequence in a
 * null-terminated string.  Raises an illegal byte sequence error if the
 * sequence would hit a null terminator.
 *
 * The caller is expected to have checked for a terminator at *mbstr == 0
 * before calling, but some callers want 1 in that case, so this function
 * continues that tradition.
 *
 * This must only be used for strings that have a null-terminator to enable
 * bounds detection.
 */
int
pg_mblen_cstr(const char *mbstr)
{
	int			length = pg_wchar_table[DatabaseEncoding->encoding].mblen((const unsigned char *) mbstr);

	/*
	 * The .mblen functions return 1 when given a pointer to a terminator.
	 * Some callers depend on that, so we tolerate it for now.  Well-behaved
	 * callers check the leading byte for a terminator *before* calling.
	 */
	for (int i = 1; i < length; ++i)
		if (unlikely(mbstr[i] == 0))
			report_invalid_encoding_db(mbstr, length, i);

	/*
	 * String should be NUL-terminated, but checking that would make typical
	 * callers O(N^2), tripling Valgrind check-world time.  Unless
	 * VALGRIND_EXPENSIVE, check 1 byte after each actual character.  (If we
	 * found a character, not a terminator, the next byte must be a terminator
	 * or the start of the next character.)  If the caller iterates the whole
	 * string, the last call will diagnose a missing terminator.
	 */
	if (mbstr[0] != '\0')
	{
#ifdef VALGRIND_EXPENSIVE
		VALGRIND_CHECK_MEM_IS_DEFINED(mbstr, strlen(mbstr));
#else
		VALGRIND_CHECK_MEM_IS_DEFINED(mbstr + length, 1);
#endif
	}

	return length;
}

/*
 * Returns the byte length of a multibyte character sequence bounded by a range
 * [mbstr, end) of at least one byte in size.  Raises an illegal byte sequence
 * error if the sequence would exceed the range.
 */
int
pg_mblen_range(const char *mbstr, const char *end)
{
	int			length = pg_wchar_table[DatabaseEncoding->encoding].mblen((const unsigned char *) mbstr);

	Assert(end > mbstr);

	if (unlikely(mbstr + length > end))
		report_invalid_encoding_db(mbstr, length, end - mbstr);

#ifdef VALGRIND_EXPENSIVE
	VALGRIND_CHECK_MEM_IS_DEFINED(mbstr, end - mbstr);
#else
	VALGRIND_CHECK_MEM_IS_DEFINED(mbstr, length);
#endif

	return length;
}

/*
 * Returns the byte length of a multibyte character sequence bounded by a range
 * extending for 'limit' bytes, which must be at least one.  Raises an illegal
 * byte sequence error if the sequence would exceed the range.
 */
int
pg_mblen_with_len(const char *mbstr, int limit)
{
	int			length = pg_wchar_table[DatabaseEncoding->encoding].mblen((const unsigned char *) mbstr);

	Assert(limit >= 1);

	if (unlikely(length > limit))
		report_invalid_encoding_db(mbstr, length, limit);

#ifdef VALGRIND_EXPENSIVE
	VALGRIND_CHECK_MEM_IS_DEFINED(mbstr, limit);
#else
	VALGRIND_CHECK_MEM_IS_DEFINED(mbstr, length);
#endif

	return length;
}


/*
 * Returns the length of a multibyte character sequence, without any
 * validation of bounds.
 *
 * PLEASE NOTE:  This function can only be used safely if the caller has
 * already verified the input string, since otherwise there is a risk of
 * overrunning the buffer if the string is invalid.  A prior call to a
 * pg_mbstrlen* function suffices.
 */
int
pg_mblen_unbounded(const char *mbstr)
{
	int			length = pg_wchar_table[DatabaseEncoding->encoding].mblen((const unsigned char *) mbstr);

	VALGRIND_CHECK_MEM_IS_DEFINED(mbstr, length);

	return length;
}

/*
 * Historical name for pg_mblen_unbounded().  Should not be used and will be
 * removed in a later version.
 */
int
pg_mblen(const char *mbstr)
{
	return pg_mblen_unbounded(mbstr);
}

/* returns the display length of a multibyte character */
int
pg_dsplen(const char *mbstr)
{
	return pg_wchar_table[DatabaseEncoding->encoding].dsplen((const unsigned char *) mbstr);
}

/* returns the length (counted in wchars) of a multibyte string */
int
pg_mbstrlen(const char *mbstr)
{
	int			len = 0;

	/* optimization for single byte encoding */
	if (pg_database_encoding_max_length() == 1)
		return strlen(mbstr);

	while (*mbstr)
	{
		mbstr += pg_mblen_cstr(mbstr);
		len++;
	}
	return len;
}

/* returns the length (counted in wchars) of a multibyte string
 * (stops at the first of "limit" or a NUL)
 */
int
pg_mbstrlen_with_len(const char *mbstr, int limit)
{
	int			len = 0;

	/* optimization for single byte encoding */
	if (pg_database_encoding_max_length() == 1)
		return limit;

	while (limit > 0 && *mbstr)
	{
		int			l = pg_mblen_with_len(mbstr, limit);

		limit -= l;
		mbstr += l;
		len++;
	}
	return len;
}

/*
 * returns the byte length of a multibyte string
 * (not necessarily NULL terminated)
 * that is no longer than limit.
 * this function does not break multibyte character boundary.
 */
int
pg_mbcliplen(const char *mbstr, int len, int limit)
{
	return pg_encoding_mbcliplen(DatabaseEncoding->encoding, mbstr,
								 len, limit);
}

/*
 * pg_mbcliplen with specified encoding; string must be valid in encoding
 */
int
pg_encoding_mbcliplen(int encoding, const char *mbstr,
					  int len, int limit)
{
	mblen_converter mblen_fn;
	int			clen = 0;
	int			l;

	/* optimization for single byte encoding */
	if (pg_encoding_max_length(encoding) == 1)
		return cliplen(mbstr, len, limit);

	mblen_fn = pg_wchar_table[encoding].mblen;

	while (len > 0 && *mbstr)
	{
		l = (*mblen_fn) ((const unsigned char *) mbstr);
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

/*
 * Similar to pg_mbcliplen except the limit parameter specifies the
 * character length, not the byte length.
 */
int
pg_mbcharcliplen(const char *mbstr, int len, int limit)
{
	int			clen = 0;
	int			nch = 0;
	int			l;

	/* optimization for single byte encoding */
	if (pg_database_encoding_max_length() == 1)
		return cliplen(mbstr, len, limit);

	while (len > 0 && *mbstr)
	{
		l = pg_mblen_with_len(mbstr, len);
		nch++;
		if (nch > limit)
			break;
		clen += l;
		len -= l;
		mbstr += l;
	}
	return clen;
}

/* mbcliplen for any single-byte encoding */
static int
cliplen(const char *str, int len, int limit)
{
	int			l = 0;

	len = Min(len, limit);
	while (l < len && str[l])
		l++;
	return l;
}

/* ---- VERBATIM mbutils.c lines 1381-1390 ---- */
/*
 * The database encoding, also called the server encoding, represents the
 * encoding of data stored in text-like data types.  Affected types include
 * cstring, text, varchar, name, xml, and json.
 */
int
GetDatabaseEncoding(void)
{
	return DatabaseEncoding->encoding;
}

/* ---- VERBATIM mbutils.c lines 1439-1675 ---- */

/*
 * Generic character incrementer function.
 *
 * Not knowing anything about the properties of the encoding in use, we just
 * keep incrementing the last byte until we get a validly-encoded result,
 * or we run out of values to try.  We don't bother to try incrementing
 * higher-order bytes, so there's no growth in runtime for wider characters.
 * (If we did try to do that, we'd need to consider the likelihood that 255
 * is not a valid final byte in the encoding.)
 */
static bool
pg_generic_charinc(unsigned char *charptr, int len)
{
	unsigned char *lastbyte = charptr + len - 1;
	mbchar_verifier mbverify;

	/* We can just invoke the character verifier directly. */
	mbverify = pg_wchar_table[GetDatabaseEncoding()].mbverifychar;

	while (*lastbyte < (unsigned char) 255)
	{
		(*lastbyte)++;
		if ((*mbverify) (charptr, len) == len)
			return true;
	}

	return false;
}

/*
 * UTF-8 character incrementer function.
 *
 * For a one-byte character less than 0x7F, we just increment the byte.
 *
 * For a multibyte character, every byte but the first must fall between 0x80
 * and 0xBF; and the first byte must be between 0xC0 and 0xF4.  We increment
 * the last byte that's not already at its maximum value.  If we can't find a
 * byte that's less than the maximum allowable value, we simply fail.  We also
 * need some special-case logic to skip regions used for surrogate pair
 * handling, as those should not occur in valid UTF-8.
 *
 * Note that we don't reset lower-order bytes back to their minimums, since
 * we can't afford to make an exhaustive search (see make_greater_string).
 */
static bool
pg_utf8_increment(unsigned char *charptr, int length)
{
	unsigned char a;
	unsigned char limit;

	switch (length)
	{
		default:
			/* reject lengths 5 and 6 for now */
			return false;
		case 4:
			a = charptr[3];
			if (a < 0xBF)
			{
				charptr[3]++;
				break;
			}
			/* FALL THRU */
		case 3:
			a = charptr[2];
			if (a < 0xBF)
			{
				charptr[2]++;
				break;
			}
			/* FALL THRU */
		case 2:
			a = charptr[1];
			switch (*charptr)
			{
				case 0xED:
					limit = 0x9F;
					break;
				case 0xF4:
					limit = 0x8F;
					break;
				default:
					limit = 0xBF;
					break;
			}
			if (a < limit)
			{
				charptr[1]++;
				break;
			}
			/* FALL THRU */
		case 1:
			a = *charptr;
			if (a == 0x7F || a == 0xDF || a == 0xEF || a == 0xF4)
				return false;
			charptr[0]++;
			break;
	}

	return true;
}

/*
 * EUC-JP character incrementer function.
 *
 * If the sequence starts with SS2 (0x8e), it must be a two-byte sequence
 * representing JIS X 0201 characters with the second byte ranging between
 * 0xa1 and 0xdf.  We just increment the last byte if it's less than 0xdf,
 * and otherwise rewrite the whole sequence to 0xa1 0xa1.
 *
 * If the sequence starts with SS3 (0x8f), it must be a three-byte sequence
 * in which the last two bytes range between 0xa1 and 0xfe.  The last byte
 * is incremented if possible, otherwise the second-to-last byte.
 *
 * If the sequence starts with a value other than the above and its MSB
 * is set, it must be a two-byte sequence representing JIS X 0208 characters
 * with both bytes ranging between 0xa1 and 0xfe.  The last byte is
 * incremented if possible, otherwise the second-to-last byte.
 *
 * Otherwise, the sequence is a single-byte ASCII character. It is
 * incremented up to 0x7f.
 */
static bool
pg_eucjp_increment(unsigned char *charptr, int length)
{
	unsigned char c1,
				c2;
	int			i;

	c1 = *charptr;

	switch (c1)
	{
		case SS2:				/* JIS X 0201 */
			if (length != 2)
				return false;

			c2 = charptr[1];

			if (c2 >= 0xdf)
				charptr[0] = charptr[1] = 0xa1;
			else if (c2 < 0xa1)
				charptr[1] = 0xa1;
			else
				charptr[1]++;
			break;

		case SS3:				/* JIS X 0212 */
			if (length != 3)
				return false;

			for (i = 2; i > 0; i--)
			{
				c2 = charptr[i];
				if (c2 < 0xa1)
				{
					charptr[i] = 0xa1;
					return true;
				}
				else if (c2 < 0xfe)
				{
					charptr[i]++;
					return true;
				}
			}

			/* Out of 3-byte code region */
			return false;

		default:
			if (IS_HIGHBIT_SET(c1)) /* JIS X 0208? */
			{
				if (length != 2)
					return false;

				for (i = 1; i >= 0; i--)
				{
					c2 = charptr[i];
					if (c2 < 0xa1)
					{
						charptr[i] = 0xa1;
						return true;
					}
					else if (c2 < 0xfe)
					{
						charptr[i]++;
						return true;
					}
				}

				/* Out of 2 byte code region */
				return false;
			}
			else
			{					/* ASCII, single byte */
				if (c1 > 0x7e)
					return false;
				(*charptr)++;
			}
			break;
	}

	return true;
}

/*
 * get the character incrementer for the encoding for the current database
 */
mbcharacter_incrementer
pg_database_encoding_character_incrementer(void)
{
	/*
	 * Eventually it might be best to add a field to pg_wchar_table[], but for
	 * now we just use a switch.
	 */
	switch (GetDatabaseEncoding())
	{
		case PG_UTF8:
			return pg_utf8_increment;

		case PG_EUC_JP:
			return pg_eucjp_increment;

		default:
			return pg_generic_charinc;
	}
}

/*
 * fetch maximum length of the encoding for the current database
 */
int
pg_database_encoding_max_length(void)
{
	return pg_wchar_table[GetDatabaseEncoding()].maxmblen;
}

/* ---- VERBATIM mbutils.c lines 1676-1902 ---- */

/*
 * Verify mbstr to make sure that it is validly encoded in the current
 * database encoding.  Otherwise same as pg_verify_mbstr().
 */
bool
pg_verifymbstr(const char *mbstr, int len, bool noError)
{
	return pg_verify_mbstr(GetDatabaseEncoding(), mbstr, len, noError);
}

/*
 * Verify mbstr to make sure that it is validly encoded in the specified
 * encoding.
 */
bool
pg_verify_mbstr(int encoding, const char *mbstr, int len, bool noError)
{
	int			oklen;

	Assert(PG_VALID_ENCODING(encoding));

	oklen = pg_wchar_table[encoding].mbverifystr((const unsigned char *) mbstr, len);
	if (oklen != len)
	{
		if (noError)
			return false;
		report_invalid_encoding(encoding, mbstr + oklen, len - oklen);
	}
	return true;
}

/*
 * Verify mbstr to make sure that it is validly encoded in the specified
 * encoding.
 *
 * mbstr is not necessarily zero terminated; length of mbstr is
 * specified by len.
 *
 * If OK, return length of string in the encoding.
 * If a problem is found, return -1 when noError is
 * true; when noError is false, ereport() a descriptive message.
 *
 * Note: We cannot use the faster encoding-specific mbverifystr() function
 * here, because we need to count the number of characters in the string.
 */
int
pg_verify_mbstr_len(int encoding, const char *mbstr, int len, bool noError)
{
	mbchar_verifier mbverifychar;
	int			mb_len;

	Assert(PG_VALID_ENCODING(encoding));

	/*
	 * In single-byte encodings, we need only reject nulls (\0).
	 */
	if (pg_encoding_max_length(encoding) <= 1)
	{
		const char *nullpos = memchr(mbstr, 0, len);

		if (nullpos == NULL)
			return len;
		if (noError)
			return -1;
		report_invalid_encoding(encoding, nullpos, 1);
	}

	/* fetch function pointer just once */
	mbverifychar = pg_wchar_table[encoding].mbverifychar;

	mb_len = 0;

	while (len > 0)
	{
		int			l;

		/* fast path for ASCII-subset characters */
		if (!IS_HIGHBIT_SET(*mbstr))
		{
			if (*mbstr != '\0')
			{
				mb_len++;
				mbstr++;
				len--;
				continue;
			}
			if (noError)
				return -1;
			report_invalid_encoding(encoding, mbstr, len);
		}

		l = (*mbverifychar) ((const unsigned char *) mbstr, len);

		if (l < 0)
		{
			if (noError)
				return -1;
			report_invalid_encoding(encoding, mbstr, len);
		}

		mbstr += l;
		len -= l;
		mb_len++;
	}
	return mb_len;
}

/*
 * check_encoding_conversion_args: check arguments of a conversion function
 *
 * "expected" arguments can be either an encoding ID or -1 to indicate that
 * the caller will check whether it accepts the ID.
 *
 * Note: the errors here are not really user-facing, so elog instead of
 * ereport seems sufficient.  Also, we trust that the "expected" encoding
 * arguments are valid encoding IDs, but we don't trust the actuals.
 */
void
check_encoding_conversion_args(int src_encoding,
							   int dest_encoding,
							   int len,
							   int expected_src_encoding,
							   int expected_dest_encoding)
{
	if (!PG_VALID_ENCODING(src_encoding))
		elog(ERROR, "invalid source encoding ID: %d", src_encoding);
	if (src_encoding != expected_src_encoding && expected_src_encoding >= 0)
		elog(ERROR, "expected source encoding \"%s\", but got \"%s\"",
			 pg_enc2name_tbl[expected_src_encoding].name,
			 pg_enc2name_tbl[src_encoding].name);
	if (!PG_VALID_ENCODING(dest_encoding))
		elog(ERROR, "invalid destination encoding ID: %d", dest_encoding);
	if (dest_encoding != expected_dest_encoding && expected_dest_encoding >= 0)
		elog(ERROR, "expected destination encoding \"%s\", but got \"%s\"",
			 pg_enc2name_tbl[expected_dest_encoding].name,
			 pg_enc2name_tbl[dest_encoding].name);
	if (len < 0)
		elog(ERROR, "encoding conversion length must not be negative");
}

/*
 * report_invalid_encoding: complain about invalid multibyte character
 *
 * note: len is remaining length of string, not length of character;
 * len must be greater than zero (or we'd neglect initializing "buf").
 */
void
report_invalid_encoding(int encoding, const char *mbstr, int len)
{
	int			l = pg_encoding_mblen_or_incomplete(encoding, mbstr, len);

	report_invalid_encoding_int(encoding, mbstr, l, len);
}

static void
report_invalid_encoding_int(int encoding, const char *mbstr, int mblen, int len)
{
	char		buf[8 * 5 + 1];
	char	   *p = buf;
	int			j,
				jlimit;

	jlimit = Min(mblen, len);
	jlimit = Min(jlimit, 8);	/* prevent buffer overrun */

	for (j = 0; j < jlimit; j++)
	{
		p += sprintf(p, "0x%02x", (unsigned char) mbstr[j]);
		if (j < jlimit - 1)
			p += sprintf(p, " ");
	}

	ereport(ERROR,
			(errcode(ERRCODE_CHARACTER_NOT_IN_REPERTOIRE),
			 errmsg("invalid byte sequence for encoding \"%s\": %s",
					pg_enc2name_tbl[encoding].name,
					buf)));
}

static void
report_invalid_encoding_db(const char *mbstr, int mblen, int len)
{
	report_invalid_encoding_int(GetDatabaseEncoding(), mbstr, mblen, len);
}

/*
 * report_untranslatable_char: complain about untranslatable character
 *
 * note: len is remaining length of string, not length of character;
 * len must be greater than zero (or we'd neglect initializing "buf").
 */
void
report_untranslatable_char(int src_encoding, int dest_encoding,
						   const char *mbstr, int len)
{
	int			l;
	char		buf[8 * 5 + 1];
	char	   *p = buf;
	int			j,
				jlimit;

	/*
	 * We probably could use plain pg_encoding_mblen(), because
	 * gb18030_to_utf8() verifies before it converts.  All conversions should.
	 * For src_encoding!=GB18030, len>0 meets pg_encoding_mblen() needs.  Even
	 * so, be defensive, since a buggy conversion might pass invalid data.
	 * This is not a performance-critical path.
	 */
	l = pg_encoding_mblen_or_incomplete(src_encoding, mbstr, len);
	jlimit = Min(l, len);
	jlimit = Min(jlimit, 8);	/* prevent buffer overrun */

	for (j = 0; j < jlimit; j++)
	{
		p += sprintf(p, "0x%02x", (unsigned char) mbstr[j]);
		if (j < jlimit - 1)
			p += sprintf(p, " ");
	}

	ereport(ERROR,
			(errcode(ERRCODE_UNTRANSLATABLE_CHARACTER),
			 errmsg("character with byte sequence %s in encoding \"%s\" has no equivalent in encoding \"%s\"",
					buf,
					pg_enc2name_tbl[src_encoding].name,
					pg_enc2name_tbl[dest_encoding].name)));
}

/* ================================================================== */
/* Exported entry shims (wfam_x_*): setjmp error capture + session-cell */
/* setter. Plumbing only; each calls exactly one verbatim body above.  */
/* ================================================================== */

void
wfam_x_set_db_encoding(int encoding)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	/* SetDatabaseEncoding's assignment, minus the elog gate (the driver
	 * only feeds PG_VALID_BE_ENCODING values). */
	DatabaseEncoding = &pg_enc2name_tbl[encoding];
}

void
wfam_x_sqlstate(char out[6])
{
	PG_ORACLE_GUARD_CHECK(__func__);
	int			val = wfam_errcode_val;

	for (int i = 0; i < 5; i++)
	{
		out[i] = (char) PGUNSIXBIT(val);
		val >>= 6;
	}
	out[5] = 0;
}

/* err out-param: 0 = returned, 1 = ereport/elog(ERROR) fired */
#define WFAM_TRY(err) \
	wfam_errcode_val = 0; \
	if (setjmp(wfam_env) != 0) { *(err) = 1; return -2; } \
	*(err) = 0

int
wfam_x_verify_mbstr(int encoding, const char *mbstr, int len, int noError, int *err)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	WFAM_TRY(err);
	return (int) pg_verify_mbstr(encoding, mbstr, len, (bool) noError);
}

int
wfam_x_verify_mbstr_len(int encoding, const char *mbstr, int len, int noError, int *err)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	WFAM_TRY(err);
	return pg_verify_mbstr_len(encoding, mbstr, len, (bool) noError);
}

int
wfam_x_verifymbstr_db(const char *mbstr, int len, int noError, int *err)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	WFAM_TRY(err);
	return (int) pg_verifymbstr(mbstr, len, (bool) noError);
}

int
wfam_x_encoding_verifymbstr(int encoding, const char *mbstr, int len)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	return pg_encoding_verifymbstr(encoding, mbstr, len);
}

int
wfam_x_encoding_verifymbchar(int encoding, const char *mbstr, int len)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	return pg_encoding_verifymbchar(encoding, mbstr, len);
}

int
wfam_x_encoding_mblen(int encoding, const char *mbstr)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	return pg_encoding_mblen(encoding, mbstr);
}

int
wfam_x_encoding_mblen_bounded(int encoding, const char *mbstr)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	return pg_encoding_mblen_bounded(encoding, mbstr);
}

int
wfam_x_encoding_mblen_or_incomplete(int encoding, const char *mbstr, size_t remaining)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	return pg_encoding_mblen_or_incomplete(encoding, mbstr, remaining);
}

int
wfam_x_encoding_dsplen(int encoding, const char *mbstr)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	return pg_encoding_dsplen(encoding, mbstr);
}

int
wfam_x_encoding_max_length(int encoding)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	return pg_encoding_max_length(encoding);
}

void
wfam_x_encoding_set_invalid(int encoding, char *dst)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	pg_encoding_set_invalid(encoding, dst);
}

int
wfam_x_utf8_islegal(const unsigned char *source, int length)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	return (int) pg_utf8_islegal(source, length);
}

int
wfam_x_utf_mblen(const unsigned char *s)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	return pg_utf_mblen(s);
}

unsigned int
wfam_x_utf8_to_unicode(const unsigned char *c)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	return (unsigned int) utf8_to_unicode(c);
}

void
wfam_x_unicode_to_utf8(unsigned int c, unsigned char *utf8string)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	(void) unicode_to_utf8((pg_wchar) c, utf8string);
}

int
wfam_x_unicode_utf8len(unsigned int c)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	return unicode_utf8len((pg_wchar) c);
}

int
wfam_x_is_valid_unicode_codepoint(unsigned int c)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	return (int) is_valid_unicode_codepoint((pg_wchar) c);
}

int
wfam_x_is_utf16_surrogate_first(unsigned int c)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	return (int) is_utf16_surrogate_first((pg_wchar) c);
}

int
wfam_x_is_utf16_surrogate_second(unsigned int c)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	return (int) is_utf16_surrogate_second((pg_wchar) c);
}

unsigned int
wfam_x_surrogate_pair_to_codepoint(unsigned int first, unsigned int second)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	return (unsigned int) surrogate_pair_to_codepoint((pg_wchar) first, (pg_wchar) second);
}

int
wfam_x_mb2wchar_with_len(int encoding, const char *from, unsigned int *to, int len)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	return pg_encoding_mb2wchar_with_len(encoding, from, (pg_wchar *) to, len);
}

int
wfam_x_wchar2mb_with_len(int encoding, const unsigned int *from, char *to, int len)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	return pg_encoding_wchar2mb_with_len(encoding, (const pg_wchar *) from, to, len);
}

int
wfam_x_mblen_db(const char *mbstr)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	return pg_mblen(mbstr);
}

int
wfam_x_dsplen_db(const char *mbstr)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	return pg_dsplen(mbstr);
}

int
wfam_x_mblen_cstr_db(const char *mbstr, int *err)
{
	WFAM_TRY(err);
	return pg_mblen_cstr(mbstr);
}

int
wfam_x_mblen_range_db(const char *mbstr, const char *end, int *err)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	WFAM_TRY(err);
	return pg_mblen_range(mbstr, end);
}

int
wfam_x_mblen_with_len_db(const char *mbstr, int limit, int *err)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	WFAM_TRY(err);
	return pg_mblen_with_len(mbstr, limit);
}

int
wfam_x_mbstrlen_db(const char *mbstr, int *err)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	WFAM_TRY(err);
	return pg_mbstrlen(mbstr);
}

int
wfam_x_mbstrlen_with_len_db(const char *mbstr, int limit, int *err)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	WFAM_TRY(err);
	return pg_mbstrlen_with_len(mbstr, limit);
}

int
wfam_x_mbcliplen_db(const char *mbstr, int len, int limit)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	return pg_mbcliplen(mbstr, len, limit);
}

int
wfam_x_encoding_mbcliplen(int encoding, const char *mbstr, int len, int limit)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	return pg_encoding_mbcliplen(encoding, mbstr, len, limit);
}

int
wfam_x_mbcharcliplen_db(const char *mbstr, int len, int limit, int *err)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	WFAM_TRY(err);
	return pg_mbcharcliplen(mbstr, len, limit);
}

int
wfam_x_database_encoding_max_length_db(void)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	return pg_database_encoding_max_length();
}

int
wfam_x_utf8_increment(unsigned char *charptr, int length)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	return (int) pg_utf8_increment(charptr, length);
}

int
wfam_x_eucjp_increment(unsigned char *charptr, int length)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	return (int) pg_eucjp_increment(charptr, length);
}

int
wfam_x_generic_charinc_db(unsigned char *charptr, int len)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	return (int) pg_generic_charinc(charptr, len);
}

/* which incrementer pg_database_encoding_character_incrementer resolves
 * to for the session encoding: 0 = generic, 1 = utf8, 2 = eucjp */
int
wfam_x_charinc_selector_db(void)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	mbcharacter_incrementer f = pg_database_encoding_character_incrementer();

	if (f == pg_utf8_increment)
		return 1;
	if (f == pg_eucjp_increment)
		return 2;
	return 0;
}

int
wfam_x_check_encoding_conversion_args(int src_encoding, int dest_encoding, int len,
									  int expected_src_encoding, int expected_dest_encoding,
									  int *err)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	WFAM_TRY(err);
	check_encoding_conversion_args(src_encoding, dest_encoding, len,
								   expected_src_encoding, expected_dest_encoding);
	return 0;
}

int
wfam_x_report_untranslatable_char(int src_encoding, int dest_encoding,
								  const char *mbstr, int len, int *err)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	WFAM_TRY(err);
	report_untranslatable_char(src_encoding, dest_encoding, mbstr, len);
	return 0;
}

int
wfam_x_char_to_encoding(const char *name)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	return pg_char_to_encoding(name);
}

const char *
wfam_x_encoding_to_char(int encoding)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	return pg_encoding_to_char(encoding);
}

int
wfam_x_valid_client_encoding(const char *name)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	return pg_valid_client_encoding(name);
}

int
wfam_x_valid_server_encoding(const char *name)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	return pg_valid_server_encoding(name);
}

/* pg_wchar_strlen: both p1-mb-miscfam and p1-mb-portfam independently fixed
 * the missing-definition link break; the merge keeps ONE definition — the
 * verbatim TU csrc/wcharfam/wstrncmp.c (registered in build.rs) — because a
 * second in-file copy here is a duplicate symbol under Linux ld (the exact
 * failure class that broke the fleet fuzz build at 2c0bf108f008). */
