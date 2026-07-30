/*
 * Vendored VERBATIM from postgres src/port/pgstrcasecmp.c
 * (pg_ascii_toupper / pg_ascii_tolower). Ref: REL_18_STABLE — both
 * functions exist BYTE-IDENTICAL in REL_18_STABLE (verified 2026-07-28,
 * provenance audit, proofs/PROVENANCE-AUDIT.md); originally fetched from
 * REL_17_STABLE, same text. The crate is REL_18-conformant with no
 * REL_17 dependence. (An earlier header claim that these were "removed
 * from master; REL_17 source of record" was wrong and is retracted.)
 *
 * SHIMS:
 *  - renamed with pgc_ prefix (avoid any link ambiguity with the Rust crate)
 *  - int-returning/int-taking wrappers (goto-cc rejects unsigned-char ABI
 *    mismatches less gracefully; also the () / void wart workaround pattern)
 * Core logic is byte-for-byte identical.
 */

static unsigned char
pgc_ascii_toupper(unsigned char ch)
{
	if (ch >= 'a' && ch <= 'z')
		ch += 'A' - 'a';
	return ch;
}

static unsigned char
pgc_ascii_tolower(unsigned char ch)
{
	if (ch >= 'A' && ch <= 'Z')
		ch += 'a' - 'A';
	return ch;
}

int
pgc_ascii_toupper_i(int ch)
{
	return (int) pgc_ascii_toupper((unsigned char) ch);
}

int
pgc_ascii_tolower_i(int ch)
{
	return (int) pgc_ascii_tolower((unsigned char) ch);
}
