/*
 * pg_conv_euc_kr_mic.c — vendored from euc_kr_and_mic.c
 * (fetched 2026-07-28 from https://raw.githubusercontent.com/postgres/postgres/REL_18_STABLE/src/backend/utils/mb/conversion_procs). Helper functions VERBATIM (static dropped by extract_fn.awk); shims per gen_shim.awk conventions (see pg_conv_cyrillic_mic.c header); error paths per PROOF_EREPORT_FLAG (pg_mbconv.h).
 */
#include "pg_mbconv.h"

int			euc_kr2mic(const unsigned char *a, unsigned char *b, int c, bool d);
int			mic2euc_kr(const unsigned char *a, unsigned char *b, int c, bool d);

int
euc_kr2mic(const unsigned char *euc, unsigned char *p, int len, bool noError)
{
	const unsigned char *start = euc;
	int			c1;
	int			l;

	while (len > 0)
	{
		c1 = *euc;
		if (IS_HIGHBIT_SET(c1))
		{
			l = pg_encoding_verifymbchar(PG_EUC_KR, (const char *) euc, len);
			if (l != 2)
			{
				if (noError)
					break;
				report_invalid_encoding(PG_EUC_KR,
										(const char *) euc, len);
			}
			*p++ = LC_KS5601;
			*p++ = c1;
			*p++ = euc[1];
			euc += 2;
			len -= 2;
		}
		else
		{						/* should be ASCII */
			if (c1 == 0)
			{
				if (noError)
					break;
				report_invalid_encoding(PG_EUC_KR,
										(const char *) euc, len);
			}
			*p++ = c1;
			euc++;
			len--;
		}
	}
	*p = '\0';

	return euc - start;
}

int
mic2euc_kr(const unsigned char *mic, unsigned char *p, int len, bool noError)
{
	const unsigned char *start = mic;
	int			c1;
	int			l;

	while (len > 0)
	{
		c1 = *mic;
		if (!IS_HIGHBIT_SET(c1))
		{
			/* ASCII */
			if (c1 == 0)
			{
				if (noError)
					break;
				report_invalid_encoding(PG_MULE_INTERNAL,
										(const char *) mic, len);
			}
			*p++ = c1;
			mic++;
			len--;
			continue;
		}
		l = pg_encoding_verifymbchar(PG_MULE_INTERNAL, (const char *) mic, len);
		if (l < 0)
		{
			if (noError)
				break;
			report_invalid_encoding(PG_MULE_INTERNAL,
									(const char *) mic, len);
		}
		if (c1 == LC_KS5601)
		{
			*p++ = mic[1];
			*p++ = mic[2];
		}
		else
		{
			if (noError)
				break;
			report_untranslatable_char(PG_MULE_INTERNAL, PG_EUC_KR,
									   (const char *) mic, len);
		}
		mic += l;
		len -= l;
	}
	*p = '\0';

	return mic - start;
}

int
pg_euc_kr_to_mic(const unsigned char *src, unsigned char *dest, int len, bool noError)
{
	int			converted;

	pg_mbconv_err = 0;
	converted = euc_kr2mic(src, dest, len, noError);
	return converted;
}

int
pg_mic_to_euc_kr(const unsigned char *src, unsigned char *dest, int len, bool noError)
{
	int			converted;

	pg_mbconv_err = 0;
	converted = mic2euc_kr(src, dest, len, noError);
	return converted;
}

