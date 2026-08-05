/*
 * pg_conv_check.c — vendored check_encoding_conversion_args from
 * src/backend/utils/mb/mbutils.c @ REL_18_STABLE (fetched 2026-07-28).
 * Body VERBATIM. Shim: this TU redefines elog to "flag 9 + return;"
 * (the function returns void, so the early return mirrors C's noreturn
 * elog(ERROR) control flow at the exact program point; the message args —
 * including the pg_enc2name_tbl name lookups — are swallowed unevaluated,
 * message text is out of proof per the PROOF_EREPORT_FLAG convention).
 */
#include "pg_mbconv.h"

#undef elog
#define elog(level, ...) do { pg_mbconv_err = 9; return; } while (0)

void
check_encoding_conversion_args(int src_encoding,
							   int dest_encoding,
							   int len,
							   int expected_src_encoding,
							   int expected_dest_encoding)
{
	if (!PG_VALID_ENCODING(src_encoding))
		elog(ERROR, "invalid source encoding ID: %d", src_encoding);
	if (src_encoding != expected_src_encoding && expected_src_encoding >= 0)
		elog(ERROR, "expected source encoding \"%s\", but got \"%s\"",
			 pg_enc2name_tbl[expected_src_encoding].name,
			 pg_enc2name_tbl[src_encoding].name);
	if (!PG_VALID_ENCODING(dest_encoding))
		elog(ERROR, "invalid destination encoding ID: %d", dest_encoding);
	if (dest_encoding != expected_dest_encoding && expected_dest_encoding >= 0)
		elog(ERROR, "expected destination encoding \"%s\", but got \"%s\"",
			 pg_enc2name_tbl[expected_dest_encoding].name,
			 pg_enc2name_tbl[dest_encoding].name);
	if (len < 0)
		elog(ERROR, "encoding conversion length must not be negative");
}

/* harness entry: resets the flag, then runs the verbatim check */
int
pg_check_encoding_conversion_args(int src_encoding, int dest_encoding, int len,
								  int expected_src_encoding, int expected_dest_encoding)
{
	pg_mbconv_err = 0;
	check_encoding_conversion_args(src_encoding, dest_encoding, len,
								   expected_src_encoding, expected_dest_encoding);
	return pg_mbconv_err;
}
