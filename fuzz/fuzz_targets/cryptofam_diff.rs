#![no_main]
//! Differential: p1-lanef crypto/hash family batch (common/md5, common/sha1,
//! common/hmac, common/scram_common, adt/cryptohashfuncs) shipped Rust vs
//! vendored PostgreSQL 18.3 C — see decoder_fuzz::cryptofam::cryptofam_diff.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    decoder_fuzz::cryptofam_diff(data);
});
