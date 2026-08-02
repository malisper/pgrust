#![no_main]
//! Differential: contrib/pgcrypto's crypt()/gen_salt()/armor family, shipped
//! Rust vs vendored PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) C —
//! see decoder_fuzz::pgcryptofam_diff (lane p1-pgcryptofam).
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    decoder_fuzz::pgcryptofam_diff(data);
});
