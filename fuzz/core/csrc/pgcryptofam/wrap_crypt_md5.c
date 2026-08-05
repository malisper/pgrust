/*
 * wrap_crypt_md5.c — whole-TU verbatim inclusion of
 * contrib/pgcrypto/crypt-md5.c (PostgreSQL 18.3, upstream 62d6c7d3df)
 * plus an exhaustive-diff exporter for its file-static _crypt_to64. The
 * vendored file is compiled ONLY through this TU.
 */
#include "vendor/crypt-md5.c"
#include "../pg_oracle_guard.h"	/* oracle-serialization holder check */

/* exporter: file-static _crypt_to64 (n itoa64 chars of v, low-first) */
void
pg_diff_pgcryptofam_to64(char *s, unsigned long v, int n)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	_crypt_to64(s, v, n);
}
