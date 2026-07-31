/*
 * SHIM utils/pg_locale.h for the jsonpath_diff oracle — NOT PostgreSQL code.
 *
 * COLLATION PIN (documented model): the harness pins the database default
 * locale to the C locale on BOTH sides — the Rust driver calls
 * pg_locale::set_default_locale_c_for_tests() (ctype_is_c C_LOCALE), so
 * regex_core's pg_set_regex_collation(DEFAULT_COLLATION_OID) lands on its
 * C strategy; the shim pg_newlocale_from_collation below returns the
 * matching ctype_is_c entry so the VERBATIM regc_pg_locale.c lands on
 * PG_REGEX_STRATEGY_C. Struct shape VERBATIM subset of
 * src/include/utils/pg_locale.h @ 18.3 (ICU arm compiled out, no USE_ICU).
 */
#ifndef _PG_LOCALE_
#define _PG_LOCALE_

#include "postgres.h"
#include <locale.h>
#ifdef __APPLE__
#include <xlocale.h>
#endif

/* catalog/pg_collation.h VERBATIM provider tags */
#define COLLPROVIDER_DEFAULT	'd'
#define COLLPROVIDER_BUILTIN	'b'
#define COLLPROVIDER_ICU		'i'
#define COLLPROVIDER_LIBC		'c'

struct pg_locale_struct;
typedef struct pg_locale_struct *pg_locale_t;

struct pg_locale_struct
{
	char		provider;
	bool		deterministic;
	bool		collate_is_c;
	bool		ctype_is_c;
	bool		is_default;

	const struct collate_methods *collate;	/* NULL if collate_is_c */

	union
	{
		struct
		{
			const char *locale;
			bool		casemap_full;
		}			builtin;
		locale_t	lt;
	}			info;
};

extern pg_locale_t pg_newlocale_from_collation(Oid collid);

#endif							/* _PG_LOCALE_ */
