/*
 * mbconv_glue.c — native accessors for the mbconv oracle's thread-local
 * error-class flag (p1-lanez). pg_mbconv_err is __thread under
 * PG_MBCONV_TLS (see proofs/mbconv/c/pg_mbconv.h); stable Rust cannot bind
 * an extern TLS static, so the fuzz/exhaustive drivers reset and read it
 * through these two calls. Plumbing only — no logic.
 */
#include "pg_mbconv.h"

int
pg_mbconv_err_get(void)
{
	return pg_mbconv_err;
}

void
pg_mbconv_err_reset(void)
{
	pg_mbconv_err = 0;
}
