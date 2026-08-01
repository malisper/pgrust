# libfam_diff exhaustive sweep (decision-cascade a0): hll add full u32 domain
# host: Michaels-MacBook-Pro.local (laptop, low load); date: 2026-07-31; sha: 766357dfc74796199b3f4cfc808815065e287fef
# cmd: cargo test --release --lib hll_add_full_domain -- --ignored --nocapture
hll_add_full_domain bwidth=10 : 4294967296 adds, 8.72625825s
hll_add_full_domain bwidth=5 : 4294967296 adds, 51.9762745s
# verdict: PASS (register-file compare every 2^16 adds, estimate bits every 2^24; 2 x 4294967296 = 8589934592 adds total)
