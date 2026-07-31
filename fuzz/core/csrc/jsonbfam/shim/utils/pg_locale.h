/* SHIM utils/pg_locale.h — collation ENVIRONMENT pin for the vendored
 * varstr_cmp (varlena_cmp_c.inc). The family's only collation source is
 * compareJsonbScalarValue's DEFAULT_COLLATION_OID; the pinned database
 * collation is C (mirrored on the Rust side: pg_locale_seams::
 * varstr_cmp_locale installed as varstrfastcmp_c). pg_locale_struct is a
 * field-subset of the real struct (collate_is_c + deterministic are the
 * only fields varstr_cmp's compiled arms read); pg_strncoll is the
 * unreachable non-C arm — abort-loud. */
#ifndef PG_JSONBFAM_SHIM_PG_LOCALE_H
#define PG_JSONBFAM_SHIM_PG_LOCALE_H

#include "postgres.h"

typedef struct pg_locale_struct
{
	bool		deterministic;
	bool		collate_is_c;
	bool		ctype_is_c;
} pg_locale_struct;
typedef struct pg_locale_struct *pg_locale_t;

static pg_locale_struct pg_jsonbfam_c_locale = {true, true, true};

static inline pg_locale_t
pg_newlocale_from_collation(Oid collid)
{
	(void) collid;				/* database collation pin: C */
	return &pg_jsonbfam_c_locale;
}

static inline int
pg_strncoll(const char *arg1, size_t len1, const char *arg2, size_t len2,
			pg_locale_t locale)
{
	(void) arg1; (void) len1; (void) arg2; (void) len2; (void) locale;
	abort();					/* unreachable: collate_is_c pin */
}

#endif
