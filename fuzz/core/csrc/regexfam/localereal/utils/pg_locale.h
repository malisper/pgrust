/* Locale-probe variant of vendor/utils/pg_locale.h (p1-lanew): same struct,
 * but pg_newlocale_from_collation is REAL for the two synthetic builtin
 * collation oids the exhaustive pg_wc_* sweeps drive (61001 = "C.UTF-8"
 * posix casemap, 61002 = "PG_UNICODE_FAST" full casemap) — mirroring what
 * pg_locale.c's create_pg_locale_builtin builds for those catalog rows.
 * Defined in pg_regexfam_locale.c; aborts on any other oid. */
#ifndef LPROBE_PG_LOCALE_H
#define LPROBE_PG_LOCALE_H

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

extern pg_locale_t pg_newlocale_from_collation(Oid collid);

#endif
