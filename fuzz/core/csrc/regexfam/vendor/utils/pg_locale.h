/* Non-C locale strategies are never selected in this bench (C collation
 * only, matching the port's C-locale-only surface); their code must merely
 * compile, so the catalog lookup aborts. */
#ifndef CREF_REGEX_PG_LOCALE_H
#define CREF_REGEX_PG_LOCALE_H

#include <locale.h>
#if defined(__APPLE__)
#include <xlocale.h>
#endif

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

static inline pg_locale_t pg_newlocale_from_collation(Oid collid) { abort(); }

#endif
