/* VERBATIM from src/backend/utils/mb/mbutils.c @ 62d6c7d3df (18.3):
 * pg_unicode_to_server_noerror (comment-boundary exact).
 * Utf8ToServerConvProc is NULL in this oracle (no conversion procs);
 * the UTF8 server-encoding arm is the live path (encoding pin). */
/*
 * Convert a single Unicode code point into a string in the server encoding.
 *
 * Same as pg_unicode_to_server(), except that we don't throw errors,
 * but simply return false on conversion failure.
 */
bool
pg_unicode_to_server_noerror(pg_wchar c, unsigned char *s)
{
	unsigned char c_as_utf8[MAX_MULTIBYTE_CHAR_LEN + 1];
	int			c_as_utf8_len;
	int			converted_len;
	int			server_encoding;

	/* Fail if invalid Unicode code point */
	if (!is_valid_unicode_codepoint(c))
		return false;

	/* Otherwise, if it's in ASCII range, conversion is trivial */
	if (c <= 0x7F)
	{
		s[0] = (unsigned char) c;
		s[1] = '\0';
		return true;
	}

	/* If the server encoding is UTF-8, we just need to reformat the code */
	server_encoding = GetDatabaseEncoding();
	if (server_encoding == PG_UTF8)
	{
		unicode_to_utf8(c, s);
		s[pg_utf_mblen(s)] = '\0';
		return true;
	}

	/* For all other cases, we must have a conversion function available */
	if (Utf8ToServerConvProc == NULL)
		return false;

	/* Construct UTF-8 source string */
	unicode_to_utf8(c, c_as_utf8);
	c_as_utf8_len = pg_utf_mblen(c_as_utf8);
	c_as_utf8[c_as_utf8_len] = '\0';

	/* Convert, but without throwing error if we can't */
	converted_len = DatumGetInt32(FunctionCall6(Utf8ToServerConvProc,
												Int32GetDatum(PG_UTF8),
												Int32GetDatum(server_encoding),
												CStringGetDatum((char *) c_as_utf8),
												CStringGetDatum((char *) s),
												Int32GetDatum(c_as_utf8_len),
												BoolGetDatum(true)));

	/* Conversion was successful iff it consumed the whole input */
	return (converted_len == c_as_utf8_len);
}
