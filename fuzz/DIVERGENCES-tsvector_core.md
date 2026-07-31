# DIVERGENCES — tsvector_core_diff (lane p1-laneae)

Status: found during harness bring-up smoke tests, 2026-07-31. NOT yet
ground-truthed against `postgres:18.3` Docker (coordinator to adjudicate;
vendored-oracle evidence only, per the ground-truth law).

## KNOWN-DIVERGENCE-1: tsvectorrecv needSort path — datum IMAGE layout skew

Input (wire bytes, arm 1): two lexemes out of order —
`00000002 "bb\0" 0000 "aa\0" 0000`
(hex: `00 00 00 02 62 62 00 00 00 61 61 00 00 00`)

- C (tsvector.c tsvectorrecv, upstream 447-553): when entries arrive
  unsorted (`needSort`), sorts ONLY the WordEntry array in place with
  `qsort_arg(ARRPTR(vec), ..., compareentry, STRPTR(vec))`; the lexeme
  string storage keeps WIRE order. Result payload:
  `[2][entry(aa)->pos=2][entry(bb)->pos=0]["bbaa"]`.
- Rust (io.rs tsvector_recv_core, "Rare wire case: rebuild via sort on a
  decoded view"): rebuilds the whole image with storage in sorted order:
  `[2][entry(aa)->pos=0][entry(bb)->pos=2]["aabb"]`.

Decoded CONTENT is identical (same sorted entries, lexemes, positions);
the stored DATUM BYTES differ. Anything hashing or memcmp-ing the datum
(binary COPY round-trip byte identity, datum-image equality paths) sees
different bytes than C would produce.

Also latent in the same path: C's qsort_arg is UNSTABLE while Rust's
`sort_by` is STABLE — a wire message with DUPLICATE lexemes may order the
duplicate entries differently, which would be a SEMANTIC divergence (the
harness's semantic plane panics on it; none observed yet).

Harness handling: strict image compare first; on mismatch a decoded-content
comparison (`tsvec_semantic_eq`) must pass, so only *semantic* skew aborts
the fuzzer. Triage class: pgrust-bug candidate (representation parity), low
severity; fix would be to mirror C (sort entries in place, keep storage
order) in tsvector_recv_core.
