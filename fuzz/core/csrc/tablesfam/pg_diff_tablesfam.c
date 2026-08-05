/*
 * pg_diff shims over the VERBATIM vendored PostgreSQL 18.3 table-lookup
 * family (kwlookup.c + keywords.c + unicode_category.c @
 * 62d6c7d3df6287f1bd83199c1a746e50d31571a0, REL_18; kwlist_d.h is the crate's
 * committed gen_keywordlist.pl output for the same ref — byte-identical
 * tables to a real 18.3 build). NOT PostgreSQL code — plumbing only.
 */

#include "postgres_fe.h"

#include "common/keywords.h"
#include "common/unicode_category.h"

int
pg_diff_scan_keyword_lookup(const char *str)
{
	return ScanKeywordLookup(str, &ScanKeywords);
}

/*
 * Keyword text for index n.
 *
 * HARNESS PLUMBING, NOT VENDORED BEHAVIOR: verbatim GetScanKeyword
 * (src/include/common/kwlookup.h:38-42) is `kw_string + kw_offsets[n]` with NO
 * range check — an out-of-range n is C UB. The guard below exists only so the
 * driver cannot read out of bounds; NO PARITY CLAIM is made against its NULL
 * return (see the carve in fuzz/core/src/tablesfam.rs). Only in-range calls
 * are oracle calls.
 */
const char *
pg_diff_get_scan_keyword(int n)
{
	if (n < 0 || n >= ScanKeywords.num_keywords)
		return 0;
	return GetScanKeyword(n, &ScanKeywords);
}

int
pg_diff_keyword_category(int n)
{
	if (n < 0 || n >= ScanKeywords.num_keywords)
		return -1;
	return ScanKeywordCategories[n];
}

int
pg_diff_keyword_bare_label(int n)
{
	if (n < 0 || n >= ScanKeywords.num_keywords)
		return -1;
	return ScanKeywordBareLabel[n] ? 1 : 0;
}

int
pg_diff_num_keywords(void)
{
	return ScanKeywords.num_keywords;
}

int
pg_diff_max_kw_len(void)
{
	return ScanKeywords.max_kw_len;
}

int
pg_diff_unicode_category(unsigned int code)
{
	return (int) unicode_category(code);
}

/*
 * All 18 predicate surfaces packed into one bitmask so a single call
 * compares every plane for a codepoint. Bit order matches the Rust side.
 */
unsigned int
pg_diff_unicode_props(unsigned int code, int posix)
{
	unsigned int m = 0;

	m |= pg_u_prop_alphabetic(code) ? 1u << 0 : 0;
	m |= pg_u_prop_lowercase(code) ? 1u << 1 : 0;
	m |= pg_u_prop_uppercase(code) ? 1u << 2 : 0;
	m |= pg_u_prop_cased(code) ? 1u << 3 : 0;
	m |= pg_u_prop_case_ignorable(code) ? 1u << 4 : 0;
	m |= pg_u_prop_white_space(code) ? 1u << 5 : 0;
	m |= pg_u_prop_hex_digit(code) ? 1u << 6 : 0;
	m |= pg_u_prop_join_control(code) ? 1u << 7 : 0;
	m |= pg_u_isdigit(code, (bool) posix) ? 1u << 8 : 0;
	m |= pg_u_isalpha(code) ? 1u << 9 : 0;
	m |= pg_u_isalnum(code, (bool) posix) ? 1u << 10 : 0;
	m |= pg_u_isupper(code) ? 1u << 11 : 0;
	m |= pg_u_islower(code) ? 1u << 12 : 0;
	m |= pg_u_isblank(code) ? 1u << 13 : 0;
	m |= pg_u_isgraph(code) ? 1u << 14 : 0;
	m |= pg_u_isprint(code) ? 1u << 15 : 0;
	m |= pg_u_ispunct(code, (bool) posix) ? 1u << 16 : 0;
	m |= pg_u_isspace(code) ? 1u << 17 : 0;
	return m;
}
