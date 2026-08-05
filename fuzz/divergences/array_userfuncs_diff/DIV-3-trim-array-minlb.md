# DIV-3 — trim_array checked-arithmetic panic at lb == i32::MIN (FIXED)

- Found by: fleet campaign pgrust-fuzz-campaign-1785510949-0ed6-78397
  (sha c35058fdd9, crashed-early at 295,882 of 10M execs).
- Site: crates/backend/utils/adt/array_userfuncs/src/lib.rs
  trim_array_internal, `upper[0] = lbs[0] + array_length - n - 1`.
- Class: same as DIV-1 — C computes this bare under -fwrapv; a valid stored
  array with lower bound i32::MIN wraps in C, panicked in overflow-checked
  Rust builds (debug/test/fuzz); release wrapped identically to C.
- Ground truth (docker postgres:18.3, 2026-07-31):
    select trim_array('[-2147483648:-2147483648]={7}'::int[], 1)  ->  {7}
    select trim_array('[-2147483648:-2147483647]={7,8}'::int[], 1) -> {7}
  The first row is C's own wrap quirk: upper wraps MIN-1 -> i32::MAX, the
  slice [MIN, MAX] covers the whole array, so trimming ALL elements of a
  lb=i32::MIN array returns the array unchanged instead of '{}'. The fix
  reproduces this C behavior exactly (upstream quirk, match-not-fix).
- Fix: wrapping_sub pair on the upper-bound computation (lib.rs, comment
  cites this record). Fleet repro banked as
  fuzz/corpus/array_userfuncs_diff/div3-trim-minlb (replays clean).
