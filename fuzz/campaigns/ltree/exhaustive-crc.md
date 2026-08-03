# a0 EXHAUSTIVE-DIFF — ltree_crc32_sz (contrib/ltree crc.rs vs verbatim 18.3 crc32.c)

Domain: ALL 1-byte inputs (256) + ALL 2-byte inputs (65536), each under BOTH
fold paths (ltree LOWER_NODE crc vs lquery raw crc; the driver's C-ctype pin
holds on both sides) = 132,096 dual-execution compares, value plane bit-exact.

Driver: `fuzz/core/src/ltree_diff.rs::tests::crc_exhaustive_1_2_bytes`
(runs on every `cargo test -p decoder_fuzz`; CI-replayed).

Run of record: 2026-08-01, host = aarch64 macOS laptop (release), wall < 1s
(0.01s reported by the harness; the domain is trivially small — the point is
TOTALITY, not scale). Result: PASS, 0 divergences.

Ledger strength: tested(exhaustive: full 1-2-byte domain x both fold paths,
value plane). The >=3-byte domain rides the differential campaign floors.
