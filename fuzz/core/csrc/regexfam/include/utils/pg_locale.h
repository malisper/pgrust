/*
 * SHIM HEADER (regexp_diff oracle, p1-laneag) — NOT vendored PostgreSQL.
 *
 * Minimal pg_locale_t for regc_pg_locale.c.  With collation pinned to
 * C_COLLATION_OID the vendored pg_set_regex_collation takes the strategy-C
 * branch and never dereferences a pg_locale_t (locale stays 0), so the
 * struct only needs the fields the compiler sees.  Field shapes follow
 * src/include/utils/pg_locale.h @ 18.3; pg_newlocale_from_collation is an
 * abort() stub in the glue file (loud if ever reached).
 */
#ifndef PG_REGEXFAM_PG_LOCALE_H
#define PG_REGEXFAM_PG_LOCALE_H

#include <locale.h>
#ifdef __APPLE__
#include <xlocale.h>
#endif

#include "postgres.h"

#define COLLPROVIDER_DEFAULT	'd'
#define COLLPROVIDER_BUILTIN	'b'
#define COLLPROVIDER_ICU		'i'
#define COLLPROVIDER_LIBC		'c'

struct pg_locale_struct
{
	char		provider;
	bool		deterministic;
	bool		collate_is_c;
	bool		ctype_is_c;
	bool		is_default;
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

typedef struct pg_locale_struct *pg_locale_t;

/* symbol isolation, as in mb/pg_wchar.h */
#define pg_newlocale_from_collation pg_regexfam_newlocale_from_collation

extern pg_locale_t pg_newlocale_from_collation(Oid collid);

#endif							/* PG_REGEXFAM_PG_LOCALE_H */
