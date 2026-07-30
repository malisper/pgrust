/*
 * pg_conv_utf8_iso8859_1.c — vendored from
 * utf8_and_iso8859_1/utf8_and_iso8859_1.c (fetched 2026-07-28 from
 * https://raw.githubusercontent.com/postgres/postgres/REL_18_STABLE/src/
 * backend/utils/mb/conversion_procs). These two procs inline their
 * conversion loops, so the WHOLE proc bodies are vendored verbatim; the
 * only shims are the fmgr unwrap (PG_GETARG_* -> scalar params,
 * PG_RETURN_INT32 -> return), the dropped
 * CHECK_ENCODING_CONVERSION_ARGS (covered by the
 * eq_check_encoding_conversion_args harness), and the
 * PROOF_EREPORT_FLAG error rewire (pg_mbconv.h).
 */
#include "pg_mbconv.h"

int
pg_iso8859_1_to_utf8(const unsigned char *src0, unsigned char *dest, int len, bool noError)
{
	const unsigned char *src = src0;
	const unsigned char *start = src;
	unsigned short c;

	pg_mbconv_err = 0;

	while (len > 0)
	{
		c = *src;
		if (c == 0)
		{
			if (noError)
				break;
			report_invalid_encoding(PG_LATIN1, (const char *) src, len);
		}
		if (!IS_HIGHBIT_SET(c))
			*dest++ = c;
		else
		{
			*dest++ = (c >> 6) | 0xc0;
			*dest++ = (c & 0x003f) | HIGHBIT;
		}
		src++;
		len--;
	}
	*dest = '\0';

	return src - start;
}

int
pg_utf8_to_iso8859_1(const unsigned char *src0, unsigned char *dest, int len, bool noError)
{
	const unsigned char *src = src0;
	const unsigned char *start = src;
	unsigned short c,
				c1;

	pg_mbconv_err = 0;

	while (len > 0)
	{
		c = *src;
		if (c == 0)
		{
			if (noError)
				break;
			report_invalid_encoding(PG_UTF8, (const char *) src, len);
		}
		/* fast path for ASCII-subset characters */
		if (!IS_HIGHBIT_SET(c))
		{
			*dest++ = c;
			src++;
			len--;
		}
		else
		{
			int			l = pg_utf_mblen(src);

			if (l > len || !pg_utf8_islegal(src, l))
			{
				if (noError)
					break;
				report_invalid_encoding(PG_UTF8, (const char *) src, len);
			}
			if (l != 2)
			{
				if (noError)
					break;
				report_untranslatable_char(PG_UTF8, PG_LATIN1,
										   (const char *) src, len);
			}
			c1 = src[1] & 0x3f;
			c = ((c & 0x1f) << 6) | c1;
			if (c >= 0x80 && c <= 0xff)
			{
				*dest++ = (unsigned char) c;
				src += 2;
				len -= 2;
			}
			else
			{
				if (noError)
					break;
				report_untranslatable_char(PG_UTF8, PG_LATIN1,
										   (const char *) src, len);
			}
		}
	}
	*dest = '\0';

	return src - start;
}
