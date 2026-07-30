/*
 * Vendored from postgres master src/backend/utils/adt/bool.c
 * (parse_bool_with_len, fetched 2026-07-28) and REL_17_STABLE
 * src/port/pgstrcasecmp.c (pg_strncasecmp, its only callee).
 * REL_18_STABLE conformance: parse_bool_with_len has zero code drift vs
 * REL_18_STABLE bool.c, and the REL_17 pg_strncasecmp text is identical
 * to REL_18_STABLE's (verified against REL_18_STABLE sources 2026-07-28;
 * provenance audit, proofs/PROVENANCE-AUDIT.md).
 *
 * SHIMS (everything else is verbatim):
 *  - bool -> int (goto-cc bool-ABI wart): true/false spelled 1/0, the
 *    bool *result out-param becomes int *result.
 *  - pg_strncasecmp's high-bit locale branch
 *        else if (IS_HIGHBIT_SET(ch1) && isupper(ch1)) ch1 = tolower(ch1);
 *    is DROPPED: postgres runs this comparison under the C/POSIX locale
 *    assumption for these ASCII literals, where isupper() is false for all
 *    bytes >= 0x80, making the branch a no-op. (Avoids depending on CBMC's
 *    ctype model.) The Rust port makes the same C-locale assumption
 *    (to_ascii_lowercase). This shim is the ONE semantic judgment call.
 *  - names pgc_-prefixed.
 */

typedef unsigned long size_t_pgc;

static int
pgc_strncasecmp(const char *s1, const char *s2, size_t_pgc n)
{
	while (n-- > 0)
	{
		unsigned char ch1 = (unsigned char) *s1++;
		unsigned char ch2 = (unsigned char) *s2++;

		if (ch1 != ch2)
		{
			if (ch1 >= 'A' && ch1 <= 'Z')
				ch1 += 'a' - 'A';
			/* SHIM: high-bit locale fold dropped (C locale: no-op) */

			if (ch2 >= 'A' && ch2 <= 'Z')
				ch2 += 'a' - 'A';
			/* SHIM: high-bit locale fold dropped (C locale: no-op) */

			if (ch1 != ch2)
				return (int) ch1 - (int) ch2;
		}
		if (ch1 == 0)
			break;
	}
	return 0;
}

static int
pgc_parse_bool_with_len(const char *value, size_t_pgc len, int *result)
{
	/* Check the most-used possibilities first. */
	switch (*value)
	{
		case 't':
		case 'T':
			if (pgc_strncasecmp(value, "true", len) == 0)
			{
				if (result)
					*result = 1;
				return 1;
			}
			break;
		case 'f':
		case 'F':
			if (pgc_strncasecmp(value, "false", len) == 0)
			{
				if (result)
					*result = 0;
				return 1;
			}
			break;
		case 'y':
		case 'Y':
			if (pgc_strncasecmp(value, "yes", len) == 0)
			{
				if (result)
					*result = 1;
				return 1;
			}
			break;
		case 'n':
		case 'N':
			if (pgc_strncasecmp(value, "no", len) == 0)
			{
				if (result)
					*result = 0;
				return 1;
			}
			break;
		case 'o':
		case 'O':
			/* 'o' is not unique enough */
			if (pgc_strncasecmp(value, "on", (len > 2 ? len : 2)) == 0)
			{
				if (result)
					*result = 1;
				return 1;
			}
			else if (pgc_strncasecmp(value, "off", (len > 2 ? len : 2)) == 0)
			{
				if (result)
					*result = 0;
				return 1;
			}
			break;
		case '1':
			if (len == 1)
			{
				if (result)
					*result = 1;
				return 1;
			}
			break;
		case '0':
			if (len == 1)
			{
				if (result)
					*result = 0;
				return 1;
			}
			break;
		default:
			break;
	}

	if (result)
		*result = 0;			/* suppress compiler warning */
	return 0;
}

/* verdict encoding: -1 reject, 0 accepted-false, 1 accepted-true */
int
pgc_parse_bool_verdict(const char *value, unsigned long len)
{
	int			result;

	if (pgc_parse_bool_with_len(value, len, &result))
		return result;
	return -1;
}
