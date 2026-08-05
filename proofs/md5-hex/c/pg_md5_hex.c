/*
 * Vendored PostgreSQL C: md5_common.c bytesToHex — Kani dual-execution
 * oracle for pg_md5::bytes_to_hex.
 *
 * Provenance: src/common/md5_common.c @ postgres-src
 * 62d6c7d3df6287f1bd83199c1a746e50d31571a0 (REL_18 = PostgreSQL 18.3, the
 * repo's vendored ground-truth checkout ../pgrust-fabled/vendor/postgres-src),
 * copied 2026-07-30. Body VERBATIM.
 *
 * Shims (plumbing only, never logic):
 *   - body kept verbatim as the file-local static; exported through the
 *     int-returning wrapper pg_bytesToHex below (Kani lowers Rust () as
 *     `struct Unit`, which goto-cc rejects against C void — the standard
 *     int-shim from the prove-target conventions).
 *   - uint8 typedef'd locally (the file's only postgres.h dependency here).
 */

typedef unsigned char uint8;

static void
bytesToHex(uint8 b[16], char *s)
{
	static const char *hex = "0123456789abcdef";
	int			q,
				w;

	for (q = 0, w = 0; q < 16; q++)
	{
		s[w++] = hex[(b[q] >> 4) & 0x0F];
		s[w++] = hex[b[q] & 0x0F];
	}
	s[w] = '\0';
}

/* SHIM: linkable int-returning wrapper (see header). */
int
pg_bytesToHex(uint8 b[16], char *s)
{
	bytesToHex(b, s);
	return 0;
}
