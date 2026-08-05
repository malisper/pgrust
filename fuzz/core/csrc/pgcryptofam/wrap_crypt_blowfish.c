/*
 * wrap_crypt_blowfish.c — whole-TU verbatim inclusion of
 * contrib/pgcrypto/crypt-blowfish.c (PostgreSQL 18.3, upstream
 * 62d6c7d3df) plus exhaustive-diff exporters for its file-static
 * BF_encode/BF_decode radix-64 codecs. The vendored file is compiled ONLY
 * through this TU.
 */
#include "vendor/crypt-blowfish.c"
#include "../pg_oracle_guard.h"	/* oracle-serialization holder check */

/* exporter: file-static BF_encode (bcrypt radix-64 encode, size bytes) */
void
pg_diff_pgcryptofam_bf_encode(char *dst, const unsigned int *src, int size)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	BF_encode(dst, src, size);
}

/* exporter: file-static BF_decode; returns -1 on any non-alphabet char */
int
pg_diff_pgcryptofam_bf_decode(unsigned int *dst, const char *src, int size)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	return BF_decode(dst, src, size);
}
