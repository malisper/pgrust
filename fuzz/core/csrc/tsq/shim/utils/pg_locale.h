/*
 * SHIM utils/pg_locale.h — NOT PostgreSQL code. (tsq oracle family)
 *
 * The two symbols the vendored ts_locale.c excerpt reads:
 *   - database_ctype_is_c: a driver-settable global (default false, i.e. a
 *     real ICU/libc UTF-8 database) mirroring the Rust session seam
 *     ::pg_locale::database_ctype_is_c. The DRIVER must keep both sides in
 *     agreement per iteration.
 *   - char2wchar -> mbstowcs (pg_locale.c's default-locale arm): the Rust
 *     side (ts_locale/src/public.rs classify()) calls the same libc
 *     mbstowcs in the same process — parity by construction. Implemented
 *     in pg_tsq_shim.c.
 */
#ifndef PG_DIFFFUZZ_TSQ_SHIM_PG_LOCALE_H
#define PG_DIFFFUZZ_TSQ_SHIM_PG_LOCALE_H

#include <wchar.h>
#include "postgres.h"

typedef struct pg_locale_struct *pg_locale_t;

extern bool database_ctype_is_c;

extern size_t char2wchar(wchar_t *to, size_t tolen,
						 const char *from, size_t fromlen, pg_locale_t locale);

#endif
