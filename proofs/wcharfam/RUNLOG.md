# wcharfam a0 EXHAUSTIVE-DIFF run log (p1-laneah)

Driver: fuzz/core/tests/wcharfam_exhaustive.rs (dual-exec vs verbatim
vendored 18.3 C oracle csrc/pg_wcharfam.c). Raw output:
xtier-runlog-raw.txt.

Host: Apple M4 Pro, macOS 15.5, 8 test threads, release profile.
Date: 2026-07-31. Tree: b9102d0ed49af91a5ccc91ef170cc889f614dd47.

## Quick tier (runs on every cargo test) — all full domains
- q_mblen_dsplen_verifychar_2byte_full: 42 encodings x full 2^16 (+ len-1)
  x {mblen, mblen_bounded, mblen_or_incomplete, dsplen, verifychar}
- q_verifystr_2byte_full: 42 encodings x full 0..2-byte domain
- q_increments_len123_full: pg_utf8_increment + pg_eucjp_increment, full
  1..=3-byte image domains (2^8+2^16+2^24 each)
- q_generic_charinc_full_2byte: 35 BE encodings x full 1..=2-byte images
- q_encnames_table_full: all 81 table names x case/decoration/high-byte
  variants + NAMEDATALEN boundary
- q_encoding_to_char_and_max_length_band: banded i32 (-1000..1000 + i32
  extremes) + all 42 max_length + set_invalid rows
- q_check_args_grid, q_untranslatable_grid, q_surrogate_pair_full_contract
  (full 2^20 in-contract surrogate grid), q_dbwalk_grid
Result: 10 passed, 2.27s wall.

## x tier — full 2^24 / 2^32 domains, 8 passed, 182.45s wall, RC=0
- x_verifychar_3byte_full: 14 distinct kernels x full 2^24
- x_verifychar_4byte_full: kernels {EUC_TW, UTF8, MULE, GB18030} x full 2^32
- x_mblen_4byte_full: same 4 kernels x full 2^32
- x_utf8_dsplen_4byte_full: full 2^32 (utf8_to_unicode + mbbisearch +
  ucs_wcwidth chain, both tables)
- x_codepoint_full_u32: full 2^32 codepoints (unicode_to_utf8 whole-image,
  unicode_utf8len, is_valid_unicode_codepoint, surrogate predicates)
- x_utf8_bytes_full: full 2^32 4-byte images (utf8_to_unicode, pg_utf_mblen,
  pg_utf8_islegal lengths 1..=4)
- x_utf8_increment_4byte_full: full 2^32 whole-image + verdict
- x_verifystr_3byte_full: 14 kernels x full 2^24 streams

Total enumerated compares: > 5.5 * 10^10 (x tier), all EQUAL.

## Divergences found by this rig (both fixed in-lane, C-parity)
1. mbutils::pg_mbstrlen walked over a NUL embedded inside an invalid
   multibyte character; C pg_mblen_cstr raises 22021. Fixed to C semantics.
2. mbutils::pg_utf8_increment len-1 arm: C charptr[0]++ wraps 0xFF->0x00
   (unsigned); Rust += 1 panicked under overflow checks. wrapping_add now.
