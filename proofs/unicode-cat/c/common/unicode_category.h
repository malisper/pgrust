/*
 * SHIM header standing in for src/include/common/unicode_category.h so the
 * verbatim unicode_category_table.h (which does `#include
 * "common/unicode_category.h"`) and unicode_category.c compile standalone.
 *
 * Contents: the pg_unicode_category enum copied VERBATIM from PostgreSQL
 * REL_18_3 src/include/common/unicode_category.h, plus plumbing typedefs
 * that postgres.h/c.h would normally provide (uint8/uint32, pg_wchar,
 * lengthof, Assert as no-op — matching an NDEBUG production build, bool via
 * stdbool). No logic.
 */
#ifndef PROOF_UNICODE_CATEGORY_H
#define PROOF_UNICODE_CATEGORY_H

#include <stdbool.h>
#include <stddef.h>

typedef unsigned char uint8;
typedef unsigned int uint32;
typedef unsigned int pg_wchar;

#define lengthof(array) (sizeof (array) / sizeof ((array)[0]))
#define Assert(condition) ((void) 0)	/* NDEBUG production build */

typedef enum pg_unicode_category
{
	PG_U_UNASSIGNED = 0,		/* Cn */
	PG_U_UPPERCASE_LETTER = 1,	/* Lu */
	PG_U_LOWERCASE_LETTER = 2,	/* Ll */
	PG_U_TITLECASE_LETTER = 3,	/* Lt */
	PG_U_MODIFIER_LETTER = 4,	/* Lm */
	PG_U_OTHER_LETTER = 5,		/* Lo */
	PG_U_NONSPACING_MARK = 6,	/* Mn */
	PG_U_ENCLOSING_MARK = 7,	/* Me */
	PG_U_SPACING_MARK = 8,		/* Mc */
	PG_U_DECIMAL_NUMBER = 9,	/* Nd */
	PG_U_LETTER_NUMBER = 10,	/* Nl */
	PG_U_OTHER_NUMBER = 11,		/* No */
	PG_U_SPACE_SEPARATOR = 12,	/* Zs */
	PG_U_LINE_SEPARATOR = 13,	/* Zl */
	PG_U_PARAGRAPH_SEPARATOR = 14,	/* Zp */
	PG_U_CONTROL = 15,			/* Cc */
	PG_U_FORMAT = 16,			/* Cf */
	PG_U_PRIVATE_USE = 17,		/* Co */
	PG_U_SURROGATE = 18,		/* Cs */
	PG_U_DASH_PUNCTUATION = 19, /* Pd */
	PG_U_OPEN_PUNCTUATION = 20, /* Ps */
	PG_U_CLOSE_PUNCTUATION = 21,	/* Pe */
	PG_U_CONNECTOR_PUNCTUATION = 22,	/* Pc */
	PG_U_OTHER_PUNCTUATION = 23,	/* Po */
	PG_U_MATH_SYMBOL = 24,		/* Sm */
	PG_U_CURRENCY_SYMBOL = 25,	/* Sc */
	PG_U_MODIFIER_SYMBOL = 26,	/* Sk */
	PG_U_OTHER_SYMBOL = 27,		/* So */
	PG_U_INITIAL_PUNCTUATION = 28,	/* Pi */
	PG_U_FINAL_PUNCTUATION = 29 /* Pf */
} pg_unicode_category;

#endif							/* PROOF_UNICODE_CATEGORY_H */
