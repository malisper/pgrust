# Adjudication package: "char" signedness cluster (6 fleet rows)

Decode lane, 2026-07-29. Fleet context: 6 proved-on-macOS rows failed
identically on BOTH replicated Linux-aarch64 full-suite runs @3ade6dd7
(char/eq_i4tochar, charin_equiv, charout_equiv_and_roundtrip,
eq_text_char; hash-rows proofs::eq_hashchar/_extended).

## Mechanism

C `char` is SIGNED on macOS-aarch64 and Linux/x86-64, UNSIGNED on
Linux-aarch64 (AAPCS64). Everywhere Postgres widens a bare `char` to int,
the widening is platform-split for high-bit bytes.

## Verdict: the cluster SPLITS

### 1. Four char-family rows = HARNESS ARTIFACT (fixed)

charin/charout-roundtrip/i4tochar/text_char failed only at the harness
shim's `(int)(char)x` return widening — a seam real Postgres never
exposes: PG_RETURN_CHAR goes into a Datum and every consumer recovers it
via DatumGetChar (8-bit truncation). Ground truth (2026-07-29, docker
postgres:18 Linux-aarch64 vs macOS psql 18.4, same SQL):

    ('\200'::"char")::int      -> -128 | -128   (identical: chartoi4 is
    ('\377'::"char")::int      ->   -1 |   -1    explicitly (int8) in C)
    ('\200'::"char")::text     -> \200 | \200   (charout identical)
    '\377'::"char" < '\001'    ->    f |    f   (comparisons unsigned, identical)

Fix applied (proofs/char/src/lib.rs): the four theorems now assert 8-bit
datum-value parity (`as u8` both sides) — the honest platform-portable
claim. All 4 re-proved locally 0.35-0.98s. On macOS the new claim is
implied by the old one; on Linux-aarch64 it is the correct claim.

### 2. hashchar / hashcharextended = REAL PLATFORM SPLIT IN C POSTGRES
(tidin class — adjudication pending)

C hashfunc.c: `hash_uint32((int32) key)` with key a bare `char`.
Ground truth on real Postgres 18.4, identical SQL:

    hashchar('\200'::"char")          Linux-aarch64: 1807103465
                                      macOS (signed): 1361043915
    hashchar('\377'::"char")          Linux-aarch64: -1811739487
                                      macOS (signed):  385747274
    hashchar('A'::"char")             identical (-201530951) — no high bit
    hashcharextended('\200',1)        split the same way

C Postgres itself hashes "char" differently on signed-char vs
unsigned-char platforms. pgrust ships the SIGN-EXTENDING arm
(`as_char() as i32` in adt_int/adt_char fc_hashchar) — it matches C PG on
macOS/Linux-x86-64 and DIVERGES from C PG on Linux-aarch64 (the
deployment platform) for the 128 high-bit chars.

User-visible surface: hash indexes (self-consistent either way) and hash
PARTITION ROUTING — a hash-partitioned table written by C PG on Graviton
and read by pgrust routes high-bit "char" keys to different partitions.
(PG makes no cross-platform hash portability promise; this is a
C-PG-on-same-platform compat question.)

Options:
  (a) keep signed model (status quo; matches x86-64 C PG; document);
  (b) switch to unsigned model (matches Linux-aarch64 C PG = deployment
      platform; 1-line change in fc_hashchar/fc_hashcharextended x2 crates);
  (c) make it platform-conditional like C (worst of both — pgrust binaries
      would disagree with each other).
NOTE: bool hashing routes through the same
C function but only produces 0/1 — unaffected. jsonb ops.rs hashes bools
via the same model — unaffected.

Harness state (proofs/hash-rows/src/lib.rs, pinned-model pattern):
  - eq_hashchar/eq_hashcharextended: fenced to v >= 0 (portable plane) —
    green on any suite host (0.2s/3.9s local);
  - model_hashchar_signed_full/_extended: full-i8 pin of the SHIPPED
    (sign-extending) behavior against the vendored hash core
    (pg_hashint4(v as i32)) — 0.2s/5.4s local. These pin what pgrust DOES,
    not what is ratified; re-point them if the ruling picks (b).
