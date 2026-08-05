/* shim: locale surface; the fmt lanes run collid=InvalidOid/C paths only,
 * everything else links to aborting stubs */
#ifndef FMTV_PG_LOCALE_H
#define FMTV_PG_LOCALE_H
#include <sys/types.h>
#include <locale.h>
extern struct lconv *PGLC_localeconv(void);
struct pg_locale_struct
{
	bool		deterministic;
	bool		collate_is_c;
	bool		ctype_is_c;
	bool		is_default;
};
typedef struct pg_locale_struct *pg_locale_t;
extern pg_locale_t pg_newlocale_from_collation(Oid collid);
extern size_t pg_strlower(char *dst, size_t dstsize, const char *src,
						  ssize_t srclen, pg_locale_t locale);
extern size_t pg_strupper(char *dst, size_t dstsize, const char *src,
						  ssize_t srclen, pg_locale_t locale);
extern size_t pg_strtitle(char *dst, size_t dstsize, const char *src,
						  ssize_t srclen, pg_locale_t locale);
extern size_t pg_strfold(char *dst, size_t dstsize, const char *src,
						 ssize_t srclen, pg_locale_t locale);
extern void cache_locale_time(void);
/* localized-name caches (TM arms; stubs keep them NULL) */
extern char *localized_abbrev_days[];
extern char *localized_full_days[];
extern char *localized_abbrev_months[];
extern char *localized_full_months[];
#endif
