/*
 * pg_hashfn_io.c: vendored PostgreSQL C oracle for the hashfn_diff differential
 * fuzz target (100%-coverage campaign; crate crates/common/hashfn).
 *
 * GENERATED SKELETON (fuzz/scaffold.py) — NOT yet a valid oracle. Every
 * TODO(scaffold) paste site below must be filled with VERBATIM upstream C,
 * and every #error compile gate removed WITH its paste, before the
 * .file("csrc/pg_hashfn_io.c") line in core/build.rs is uncommented. A
 * half-filled shim can therefore never silently build or link.
 *
 * Provenance (fill in as you paste; follow csrc/pg_uuid_io.c):
 *   - Vendor sections 1..N byte-for-byte from src/backend/utils/adt/hashfn.c / src/backend/utils/adt/hashfn.h
 *     @ postgres-src 62d6c7d3df6287f1bd83199c1a746e50d31571a0
 *     (PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df); re-verify against the repo's vendored ground-truth
 *     checkout ../pgrust-reference/vendor/postgres-src before pasting).
 *   - Functions to vendor: hash_bytes, hash_bytes_extended, hash_bytes_uint32, hash_bytes_uint32_extended, string_hash, tag_hash, uint32_hash, hash_combine, hash_combine64, murmurhash32, murmurhash64, rotate_high_and_low_32bits.
 *   - Bodies VERBATIM except documented shims; shims are PLUMBING ONLY
 *     (isxdigit/strtoul C-locale shims, ereturn -> int sentinel, fmgr
 *     PG_FUNCTION_ARGS unwrapped to plain C signatures, palloc'd results ->
 *     caller buffers, wire triples for recv/send), NEVER logic. List every
 *     shim in this header when you paste.
 *
 * Errcode capture follows csrc/pg_float_io.c: the shared _Thread_local
 * pg_diff_errcode (defined there) records the errcode class; map each
 * errcode this crate's C raises to a small class constant below.
 */

#include <string.h>
#include <stdint.h>

/* Shared TLS errcode channel (defined in csrc/pg_float_io.c). */
extern _Thread_local int pg_diff_errcode;

/* TODO(scaffold): one class constant per distinct errcode the vendored C
 * raises, e.g.:
 *   #define PG_DIFF_ERR_INVALID_TEXT 1   (22P02)
 */

/* ==================== SECTION 1: hashfn.c (VERBATIM) ==================== */

/*
 * TODO(scaffold): paste here, byte-for-byte from
 * src/backend/utils/adt/hashfn.c @ 62d6c7d3df6287f1bd83199c1a746e50d31571a0,
 * the bodies backing: hash_bytes, hash_bytes_extended, hash_bytes_uint32, hash_bytes_uint32_extended, string_hash, tag_hash, uint32_hash
 * (rename with a pg_ prefix; unwrap fmgr wrappers; document every shim in
 * the file header above). Remove the #error line together with the paste.
 */
#error "SCAFFOLD-TODO(hashfn_diff): verbatim C from hashfn.c not pasted yet"

/* ==================== SECTION 2: hashfn.h (VERBATIM) ==================== */

/*
 * TODO(scaffold): paste here, byte-for-byte from
 * src/backend/utils/adt/hashfn.h @ 62d6c7d3df6287f1bd83199c1a746e50d31571a0,
 * the bodies backing: hash_combine, hash_combine64, murmurhash32, murmurhash64, rotate_high_and_low_32bits
 * (rename with a pg_ prefix; unwrap fmgr wrappers; document every shim in
 * the file header above). Remove the #error line together with the paste.
 */
#error "SCAFFOLD-TODO(hashfn_diff): verbatim C from hashfn.h not pasted yet"

/* ========== SECTION 3: fuzz-facing driver entries (NOT Postgres code) ===== */

/*
 * One thin pg_diff_* wrapper per fuzz arm: reset pg_diff_errcode = 0 on
 * entry, call the vendored function, return an int status (0 = ok, nonzero
 * = error class) and write results through caller-provided buffers. Shape
 * them after csrc/pg_uuid_io.c section 4, e.g.:
 *
 *   int pg_diff_uuid_in(const char *source, unsigned char *out)
 *   {
 *       pg_uuid_t u;
 *       pg_diff_errcode = 0;
 *       if (pg_string_to_uuid(source, &u) != 0)
 *       {
 *           pg_diff_errcode = PG_DIFF_ERR_INVALID_TEXT;
 *           return 1;
 *       }
 *       memcpy(out, u.data, UUID_LEN);
 *       return 0;
 *   }
 */
/*
 * TODO(scaffold): int pg_diff_hash_bytes(...)   [oid 0, hashfn.c]
 */
#error "SCAFFOLD-TODO(hashfn_diff): pg_diff_hash_bytes driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_hash_bytes_extended(...)   [oid 0, hashfn.c]
 */
#error "SCAFFOLD-TODO(hashfn_diff): pg_diff_hash_bytes_extended driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_hash_bytes_uint32(...)   [oid 0, hashfn.c]
 */
#error "SCAFFOLD-TODO(hashfn_diff): pg_diff_hash_bytes_uint32 driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_hash_bytes_uint32_extended(...)   [oid 0, hashfn.c]
 */
#error "SCAFFOLD-TODO(hashfn_diff): pg_diff_hash_bytes_uint32_extended driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_string_hash(...)   [oid 0, hashfn.c]
 */
#error "SCAFFOLD-TODO(hashfn_diff): pg_diff_string_hash driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_tag_hash(...)   [oid 0, hashfn.c]
 */
#error "SCAFFOLD-TODO(hashfn_diff): pg_diff_tag_hash driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_uint32_hash(...)   [oid 0, hashfn.c]
 */
#error "SCAFFOLD-TODO(hashfn_diff): pg_diff_uint32_hash driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_hash_combine(...)   [oid 0, hashfn.h]
 */
#error "SCAFFOLD-TODO(hashfn_diff): pg_diff_hash_combine driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_hash_combine64(...)   [oid 0, hashfn.h]
 */
#error "SCAFFOLD-TODO(hashfn_diff): pg_diff_hash_combine64 driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_murmurhash32(...)   [oid 0, hashfn.h]
 */
#error "SCAFFOLD-TODO(hashfn_diff): pg_diff_murmurhash32 driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_murmurhash64(...)   [oid 0, hashfn.h]
 */
#error "SCAFFOLD-TODO(hashfn_diff): pg_diff_murmurhash64 driver entry not written yet"
/*
 * TODO(scaffold): int pg_diff_rotate_high_and_low_32bits(...)   [oid 0, hashfn.h]
 */
#error "SCAFFOLD-TODO(hashfn_diff): pg_diff_rotate_high_and_low_32bits driver entry not written yet"
