// Differential target: common/wchar + mb/mbutils pure kernels, shipped Rust
// vs vendored PostgreSQL 18.3 C. A crash artifact = a divergence reproducer.
#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    decoder_fuzz::wcharfam_diff(data);
});
