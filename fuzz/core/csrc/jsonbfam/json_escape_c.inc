/* VERBATIM from src/backend/utils/adt/json.c @ 62d6c7d3df (18.3):
 * escape_json_char / escape_json / escape_json_with_len (1556-1726). */

/*
 * escape_json_char
 *		Inline helper function for escape_json* functions
 */
static pg_attribute_always_inline void
escape_json_char(StringInfo buf, char c)
{
	switch (c)
	{
		case '\b':
			appendStringInfoString(buf, "\\b");
			break;
		case '\f':
			appendStringInfoString(buf, "\\f");
			break;
		case '\n':
			appendStringInfoString(buf, "\\n");
			break;
		case '\r':
			appendStringInfoString(buf, "\\r");
			break;
		case '\t':
			appendStringInfoString(buf, "\\t");
			break;
		case '"':
			appendStringInfoString(buf, "\\\"");
			break;
		case '\\':
			appendStringInfoString(buf, "\\\\");
			break;
		default:
			if ((unsigned char) c < ' ')
				appendStringInfo(buf, "\\u%04x", (int) c);
			else
				appendStringInfoCharMacro(buf, c);
			break;
	}
}

/*
 * escape_json
 *		Produce a JSON string literal, properly escaping the NUL-terminated
 *		cstring.
 */
void
escape_json(StringInfo buf, const char *str)
{
	appendStringInfoCharMacro(buf, '"');

	for (; *str != '\0'; str++)
		escape_json_char(buf, *str);

	appendStringInfoCharMacro(buf, '"');
}

/*
 * Define the number of bytes that escape_json_with_len will look ahead in the
 * input string before flushing the input string to the destination buffer.
 * Looking ahead too far could result in cachelines being evicted that will
 * need to be reloaded in order to perform the appendBinaryStringInfo call.
 * Smaller values will result in a larger number of calls to
 * appendBinaryStringInfo and introduce additional function call overhead.
 * Values larger than the size of L1d cache will likely result in worse
 * performance.
 */
#define ESCAPE_JSON_FLUSH_AFTER 512

/*
 * escape_json_with_len
 *		Produce a JSON string literal, properly escaping the possibly not
 *		NUL-terminated characters in 'str'.  'len' defines the number of bytes
 *		from 'str' to process.
 */
void
escape_json_with_len(StringInfo buf, const char *str, int len)
{
	int			vlen;

	Assert(len >= 0);

	/*
	 * Since we know the minimum length we'll need to append, let's just
	 * enlarge the buffer now rather than incrementally making more space when
	 * we run out.  Add two extra bytes for the enclosing quotes.
	 */
	enlargeStringInfo(buf, len + 2);

	/*
	 * Figure out how many bytes to process using SIMD.  Round 'len' down to
	 * the previous multiple of sizeof(Vector8), assuming that's a power-of-2.
	 */
	vlen = len & (int) (~(sizeof(Vector8) - 1));

	appendStringInfoCharMacro(buf, '"');

	for (int i = 0, copypos = 0;;)
	{
		/*
		 * To speed this up, try searching sizeof(Vector8) bytes at once for
		 * special characters that we need to escape.  When we find one, we
		 * fall out of the Vector8 loop and copy the portion we've vector
		 * searched and then we process sizeof(Vector8) bytes one byte at a
		 * time.  Once done, come back and try doing vector searching again.
		 * We'll also process any remaining bytes at the tail end of the
		 * string byte-by-byte.  This optimization assumes that most chunks of
		 * sizeof(Vector8) bytes won't contain any special characters.
		 */
		for (; i < vlen; i += sizeof(Vector8))
		{
			Vector8		chunk;

			vector8_load(&chunk, (const uint8 *) &str[i]);

			/*
			 * Break on anything less than ' ' or if we find a '"' or '\\'.
			 * Those need special handling.  That's done in the per-byte loop.
			 */
			if (vector8_has_le(chunk, (unsigned char) 0x1F) ||
				vector8_has(chunk, (unsigned char) '"') ||
				vector8_has(chunk, (unsigned char) '\\'))
				break;

#ifdef ESCAPE_JSON_FLUSH_AFTER

			/*
			 * Flush what's been checked so far out to the destination buffer
			 * every so often to avoid having to re-read cachelines when
			 * escaping large strings.
			 */
			if (i - copypos >= ESCAPE_JSON_FLUSH_AFTER)
			{
				appendBinaryStringInfo(buf, &str[copypos], i - copypos);
				copypos = i;
			}
#endif
		}

		/*
		 * Write to the destination up to the point that we've vector searched
		 * so far.  Do this only when switching into per-byte mode rather than
		 * once every sizeof(Vector8) bytes.
		 */
		if (copypos < i)
		{
			appendBinaryStringInfo(buf, &str[copypos], i - copypos);
			copypos = i;
		}

		/*
		 * Per-byte loop for Vector8s containing special chars and for
		 * processing the tail of the string.
		 */
		for (int b = 0; b < sizeof(Vector8); b++)
		{
			/* check if we've finished */
			if (i == len)
				goto done;

			Assert(i < len);

			escape_json_char(buf, str[i++]);
		}

		copypos = i;
		/* We're not done yet.  Try the vector search again. */
	}

done:
	appendStringInfoCharMacro(buf, '"');
}
