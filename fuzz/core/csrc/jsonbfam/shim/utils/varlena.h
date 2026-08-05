/* SHIM utils/varlena.h — varstr_cmp decl only; compare arms are the
 * ops-target's charter (abort-stub in driver TU for the io target). */
#ifndef PG_JSONBFAM_SHIM_VARLENA_H
#define PG_JSONBFAM_SHIM_VARLENA_H
#include "postgres.h"
extern int varstr_cmp(const char *arg1, int len1, const char *arg2, int len2, Oid collid);
#endif
