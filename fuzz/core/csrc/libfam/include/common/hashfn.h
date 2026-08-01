/*
 * REDUCED common/hashfn.h — for the libfam_diff differential-fuzz oracle.
 *
 * NOT the full PostgreSQL header. VERBATIM pieces from
 * src/include/common/hashfn.h @ 62d6c7d3df6287f1bd83199c1a746e50d31571a0
 * (Stamp-18.3), reduced to what csrc/libfam/vendor/bloomfilter.c consumes:
 *   - hash_bytes_extended extern declaration (lines 24..25). The DEFINITION
 *     is the VERBATIM vendored body in csrc/pg_hashfn_io.c (same cc build,
 *     same oracle pin) — one definition, shared across targets.
 *   - hash_any_extended static inline (lines 36..40), byte-for-byte.
 * Datum / UInt64GetDatum come from the TU shim section (LP64 parity).
 */
#ifndef HASHFN_H
#define HASHFN_H

extern uint64 hash_bytes_extended(const unsigned char *k,
								  int keylen, uint64 seed);

static inline Datum
hash_any_extended(const unsigned char *k, int keylen, uint64 seed)
{
	return UInt64GetDatum(hash_bytes_extended(k, keylen, seed));
}

#endif							/* HASHFN_H */
