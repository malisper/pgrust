/*
 * wrap_crypt_gensalt.c — whole-TU verbatim inclusion of
 * contrib/pgcrypto/crypt-gensalt.c (PostgreSQL 18.3, upstream
 * 62d6c7d3df) plus an exhaustive-diff exporter for the xdes iteration-
 * count encoding. The vendored file is compiled ONLY through this TU.
 */
#include "vendor/crypt-gensalt.c"
#include "../pg_oracle_guard.h"	/* oracle-serialization holder check */

/*
 * exporter: the 4-char xdes count encoding — the count-dependent slice of
 * _crypt_gensalt_extended_rn's output (crypt-gensalt.c lines 62-65),
 * indexing the SAME file-static _crypt_itoa64 table the vendored body
 * uses. Harness exporter, not a fabricated oracle body: it exists so the
 * driver can exhaustively diff the count encoding without driving entropy
 * through the full generator.
 */
void
pg_diff_pgcryptofam_xdes_count_encode(unsigned long count, char *out)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	out[0] = _crypt_itoa64[count & 0x3f];
	out[1] = _crypt_itoa64[(count >> 6) & 0x3f];
	out[2] = _crypt_itoa64[(count >> 12) & 0x3f];
	out[3] = _crypt_itoa64[(count >> 18) & 0x3f];
}
