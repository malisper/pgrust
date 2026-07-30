/*
 * Vendored verbatim from PostgreSQL REL_18_3 src/common/unicode_category.c
 * (byte-identical to pgrust-fabled/vendor/postgres-src @ 62d6c7d3df62
 * "Stamp 18.3", copied 2026-07-28): range_search, unicode_category, the
 * pg_u_prop_* family and the pg_u_is* compatibility predicates that the
 * pgrust `unicode_category` crate ports. unicode_category_string/abbrev
 * (string tables) and pg_u_isword/iscntrl/isxdigit (not ported) omitted.
 *
 * Tables: c/unicode_category_table.h is a BYTE-IDENTICAL copy (cmp-verified,
 * 236394 bytes) of both crates/common/unicode_category/unicode_category_table.h
 * (the file the Rust build.rs transcribes) and the REL_18_3 original.
 *
 * Shims (plumbing only, never logic):
 *   - common/unicode_category.h replaced by c/common/unicode_category.h:
 *     verbatim pg_unicode_category enum + uint8/uint32/pg_wchar typedefs,
 *     lengthof, Assert -> no-op (NDEBUG production build).
 *   - postgres.h include dropped.
 *   - c_* int-returning wrappers for the Kani FFI bridge (bool ABI wart).
 */

#include "common/unicode_category.h"
#include "unicode_category_table.h"

/*
 * Create bitmasks from pg_unicode_category values for efficient comparison of
 * multiple categories. For instance, PG_U_MN_MASK is a bitmask representing
 * the general category Mn; and PG_U_M_MASK represents general categories Mn,
 * Me, and Mc.
 *
 * The number of Unicode General Categories should never grow, so a 32-bit
 * mask is fine.
 */
#define PG_U_CATEGORY_MASK(X) ((uint32)(1 << (X)))

#define PG_U_LU_MASK PG_U_CATEGORY_MASK(PG_U_UPPERCASE_LETTER)
#define PG_U_LL_MASK PG_U_CATEGORY_MASK(PG_U_LOWERCASE_LETTER)
#define PG_U_LT_MASK PG_U_CATEGORY_MASK(PG_U_TITLECASE_LETTER)
#define PG_U_LC_MASK (PG_U_LU_MASK|PG_U_LL_MASK|PG_U_LT_MASK)
#define PG_U_LM_MASK PG_U_CATEGORY_MASK(PG_U_MODIFIER_LETTER)
#define PG_U_LO_MASK PG_U_CATEGORY_MASK(PG_U_OTHER_LETTER)
#define PG_U_L_MASK (PG_U_LU_MASK|PG_U_LL_MASK|PG_U_LT_MASK|PG_U_LM_MASK|\
					 PG_U_LO_MASK)
#define PG_U_MN_MASK PG_U_CATEGORY_MASK(PG_U_NONSPACING_MARK)
#define PG_U_ME_MASK PG_U_CATEGORY_MASK(PG_U_ENCLOSING_MARK)
#define PG_U_MC_MASK PG_U_CATEGORY_MASK(PG_U_SPACING_MARK)
#define PG_U_M_MASK (PG_U_MN_MASK|PG_U_MC_MASK|PG_U_ME_MASK)
#define PG_U_ND_MASK PG_U_CATEGORY_MASK(PG_U_DECIMAL_NUMBER)
#define PG_U_NL_MASK PG_U_CATEGORY_MASK(PG_U_LETTER_NUMBER)
#define PG_U_NO_MASK PG_U_CATEGORY_MASK(PG_U_OTHER_NUMBER)
#define PG_U_N_MASK (PG_U_ND_MASK|PG_U_NL_MASK|PG_U_NO_MASK)
#define PG_U_PC_MASK PG_U_CATEGORY_MASK(PG_U_CONNECTOR_PUNCTUATION)
#define PG_U_PD_MASK PG_U_CATEGORY_MASK(PG_U_DASH_PUNCTUATION)
#define PG_U_PS_MASK PG_U_CATEGORY_MASK(PG_U_OPEN_PUNCTUATION)
#define PG_U_PE_MASK PG_U_CATEGORY_MASK(PG_U_CLOSE_PUNCTUATION)
#define PG_U_PI_MASK PG_U_CATEGORY_MASK(PG_U_INITIAL_PUNCTUATION)
#define PG_U_PF_MASK PG_U_CATEGORY_MASK(PG_U_FINAL_PUNCTUATION)
#define PG_U_PO_MASK PG_U_CATEGORY_MASK(PG_U_OTHER_PUNCTUATION)
#define PG_U_P_MASK (PG_U_PC_MASK|PG_U_PD_MASK|PG_U_PS_MASK|PG_U_PE_MASK|\
					 PG_U_PI_MASK|PG_U_PF_MASK|PG_U_PO_MASK)
#define PG_U_SM_MASK PG_U_CATEGORY_MASK(PG_U_MATH_SYMBOL)
#define PG_U_SC_MASK PG_U_CATEGORY_MASK(PG_U_CURRENCY_SYMBOL)
#define PG_U_SK_MASK PG_U_CATEGORY_MASK(PG_U_MODIFIER_SYMBOL)
#define PG_U_SO_MASK PG_U_CATEGORY_MASK(PG_U_OTHER_SYMBOL)
#define PG_U_S_MASK (PG_U_SM_MASK|PG_U_SC_MASK|PG_U_SK_MASK|PG_U_SO_MASK)
#define PG_U_ZS_MASK PG_U_CATEGORY_MASK(PG_U_SPACE_SEPARATOR)
#define PG_U_ZL_MASK PG_U_CATEGORY_MASK(PG_U_LINE_SEPARATOR)
#define PG_U_ZP_MASK PG_U_CATEGORY_MASK(PG_U_PARAGRAPH_SEPARATOR)
#define PG_U_Z_MASK (PG_U_ZS_MASK|PG_U_ZL_MASK|PG_U_ZP_MASK)
#define PG_U_CC_MASK PG_U_CATEGORY_MASK(PG_U_CONTROL)
#define PG_U_CF_MASK PG_U_CATEGORY_MASK(PG_U_FORMAT)
#define PG_U_CS_MASK PG_U_CATEGORY_MASK(PG_U_SURROGATE)
#define PG_U_CO_MASK PG_U_CATEGORY_MASK(PG_U_PRIVATE_USE)
#define PG_U_CN_MASK PG_U_CATEGORY_MASK(PG_U_UNASSIGNED)
#define PG_U_C_MASK (PG_U_CC_MASK|PG_U_CF_MASK|PG_U_CS_MASK|PG_U_CO_MASK|\
					 PG_U_CN_MASK)

#define PG_U_CHARACTER_TAB	0x09

static bool range_search(const pg_unicode_range *tbl, size_t size,
						 pg_wchar code);

/*
 * Unicode general category for the given codepoint.
 */
pg_unicode_category
unicode_category(pg_wchar code)
{
	int			min = 0;
	int			mid;
	int			max = lengthof(unicode_categories) - 1;

	Assert(code <= 0x10ffff);

	if (code < 0x80)
		return unicode_opt_ascii[code].category;

	while (max >= min)
	{
		mid = (min + max) / 2;
		if (code > unicode_categories[mid].last)
			min = mid + 1;
		else if (code < unicode_categories[mid].first)
			max = mid - 1;
		else
			return unicode_categories[mid].category;
	}

	return PG_U_UNASSIGNED;
}

bool
pg_u_prop_alphabetic(pg_wchar code)
{
	if (code < 0x80)
		return unicode_opt_ascii[code].properties & PG_U_PROP_ALPHABETIC;

	return range_search(unicode_alphabetic,
						lengthof(unicode_alphabetic),
						code);
}

bool
pg_u_prop_lowercase(pg_wchar code)
{
	if (code < 0x80)
		return unicode_opt_ascii[code].properties & PG_U_PROP_LOWERCASE;

	return range_search(unicode_lowercase,
						lengthof(unicode_lowercase),
						code);
}

bool
pg_u_prop_uppercase(pg_wchar code)
{
	if (code < 0x80)
		return unicode_opt_ascii[code].properties & PG_U_PROP_UPPERCASE;

	return range_search(unicode_uppercase,
						lengthof(unicode_uppercase),
						code);
}

bool
pg_u_prop_cased(pg_wchar code)
{
	uint32		category_mask;

	if (code < 0x80)
		return unicode_opt_ascii[code].properties & PG_U_PROP_CASED;

	category_mask = PG_U_CATEGORY_MASK(unicode_category(code));

	return category_mask & PG_U_LT_MASK ||
		pg_u_prop_lowercase(code) ||
		pg_u_prop_uppercase(code);
}

bool
pg_u_prop_case_ignorable(pg_wchar code)
{
	if (code < 0x80)
		return unicode_opt_ascii[code].properties & PG_U_PROP_CASE_IGNORABLE;

	return range_search(unicode_case_ignorable,
						lengthof(unicode_case_ignorable),
						code);
}

bool
pg_u_prop_white_space(pg_wchar code)
{
	if (code < 0x80)
		return unicode_opt_ascii[code].properties & PG_U_PROP_WHITE_SPACE;

	return range_search(unicode_white_space,
						lengthof(unicode_white_space),
						code);
}

bool
pg_u_prop_hex_digit(pg_wchar code)
{
	if (code < 0x80)
		return unicode_opt_ascii[code].properties & PG_U_PROP_HEX_DIGIT;

	return range_search(unicode_hex_digit,
						lengthof(unicode_hex_digit),
						code);
}

bool
pg_u_prop_join_control(pg_wchar code)
{
	if (code < 0x80)
		return unicode_opt_ascii[code].properties & PG_U_PROP_JOIN_CONTROL;

	return range_search(unicode_join_control,
						lengthof(unicode_join_control),
						code);
}

/*
 * The following functions implement the Compatibility Properties described
 * at: http://www.unicode.org/reports/tr18/#Compatibility_Properties
 *
 * If 'posix' is true, implements the "POSIX Compatible" variant, otherwise
 * the "Standard" variant.
 */

bool
pg_u_isdigit(pg_wchar code, bool posix)
{
	if (posix)
		return ('0' <= code && code <= '9');
	else
		return unicode_category(code) == PG_U_DECIMAL_NUMBER;
}

bool
pg_u_isalpha(pg_wchar code)
{
	return pg_u_prop_alphabetic(code);
}

bool
pg_u_isalnum(pg_wchar code, bool posix)
{
	return pg_u_isalpha(code) || pg_u_isdigit(code, posix);
}

bool
pg_u_isupper(pg_wchar code)
{
	return pg_u_prop_uppercase(code);
}

bool
pg_u_islower(pg_wchar code)
{
	return pg_u_prop_lowercase(code);
}

bool
pg_u_isblank(pg_wchar code)
{
	return code == PG_U_CHARACTER_TAB ||
		unicode_category(code) == PG_U_SPACE_SEPARATOR;
}

bool
pg_u_isgraph(pg_wchar code)
{
	uint32		category_mask = PG_U_CATEGORY_MASK(unicode_category(code));

	if (category_mask & (PG_U_CC_MASK | PG_U_CS_MASK | PG_U_CN_MASK) ||
		pg_u_isspace(code))
		return false;
	return true;
}

bool
pg_u_isprint(pg_wchar code)
{
	pg_unicode_category category = unicode_category(code);

	if (category == PG_U_CONTROL)
		return false;

	return pg_u_isgraph(code) || pg_u_isblank(code);
}

bool
pg_u_ispunct(pg_wchar code, bool posix)
{
	uint32		category_mask;

	if (posix)
	{
		if (pg_u_isalpha(code))
			return false;

		category_mask = PG_U_CATEGORY_MASK(unicode_category(code));
		return category_mask & (PG_U_P_MASK | PG_U_S_MASK);
	}
	else
	{
		category_mask = PG_U_CATEGORY_MASK(unicode_category(code));

		return category_mask & PG_U_P_MASK;
	}
}

bool
pg_u_isspace(pg_wchar code)
{
	return pg_u_prop_white_space(code);
}

/*
 * Binary search to test if given codepoint exists in one of the ranges in the
 * given table.
 */
static bool
range_search(const pg_unicode_range *tbl, size_t size, pg_wchar code)
{
	int			min = 0;
	int			mid;
	int			max = size - 1;

	Assert(code <= 0x10ffff);

	while (max >= min)
	{
		mid = (min + max) / 2;
		if (code > tbl[mid].last)
			min = mid + 1;
		else if (code < tbl[mid].first)
			max = mid - 1;
		else
			return true;
	}

	return false;
}

/* --- int-returning shims for the Kani FFI bridge --- */

int c_unicode_category(pg_wchar code) { return (int) unicode_category(code); }
int c_pg_u_prop_alphabetic(pg_wchar code) { return pg_u_prop_alphabetic(code) ? 1 : 0; }
int c_pg_u_prop_lowercase(pg_wchar code) { return pg_u_prop_lowercase(code) ? 1 : 0; }
int c_pg_u_prop_uppercase(pg_wchar code) { return pg_u_prop_uppercase(code) ? 1 : 0; }
int c_pg_u_prop_cased(pg_wchar code) { return pg_u_prop_cased(code) ? 1 : 0; }
int c_pg_u_prop_case_ignorable(pg_wchar code) { return pg_u_prop_case_ignorable(code) ? 1 : 0; }
int c_pg_u_prop_white_space(pg_wchar code) { return pg_u_prop_white_space(code) ? 1 : 0; }
int c_pg_u_prop_hex_digit(pg_wchar code) { return pg_u_prop_hex_digit(code) ? 1 : 0; }
int c_pg_u_prop_join_control(pg_wchar code) { return pg_u_prop_join_control(code) ? 1 : 0; }
int c_pg_u_isdigit(pg_wchar code, int posix) { return pg_u_isdigit(code, posix != 0) ? 1 : 0; }
int c_pg_u_isalpha(pg_wchar code) { return pg_u_isalpha(code) ? 1 : 0; }
int c_pg_u_isalnum(pg_wchar code, int posix) { return pg_u_isalnum(code, posix != 0) ? 1 : 0; }
int c_pg_u_isupper(pg_wchar code) { return pg_u_isupper(code) ? 1 : 0; }
int c_pg_u_islower(pg_wchar code) { return pg_u_islower(code) ? 1 : 0; }
int c_pg_u_isblank(pg_wchar code) { return pg_u_isblank(code) ? 1 : 0; }
int c_pg_u_isgraph(pg_wchar code) { return pg_u_isgraph(code) ? 1 : 0; }
int c_pg_u_isprint(pg_wchar code) { return pg_u_isprint(code) ? 1 : 0; }
int c_pg_u_ispunct(pg_wchar code, int posix) { return pg_u_ispunct(code, posix != 0) ? 1 : 0; }
int c_pg_u_isspace(pg_wchar code) { return pg_u_isspace(code) ? 1 : 0; }
