/*
 * wrap_crypt_des.c — whole-TU verbatim inclusion of
 * contrib/pgcrypto/crypt-des.c (PostgreSQL 18.3, upstream 62d6c7d3df; the
 * numericfam pg_numeric_oracle.c precedent) plus an exhaustive-diff
 * exporter for its file-static ascii_to_bin. The vendored file is compiled
 * ONLY through this TU.
 */
#include "vendor/crypt-des.c"
#include "../pg_oracle_guard.h"	/* oracle-serialization holder check */

/* exporter: file-static ascii_to_bin (setting-char -> 6-bit value) */
int
pg_diff_pgcryptofam_ascii_to_bin(char ch)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	return ascii_to_bin(ch);
}
