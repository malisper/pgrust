#![no_main]
//! Differential: crates/backend/tsearch/spell (ispell/hunspell dictionary
//! loader + normalizer) shipped Rust vs vendored PostgreSQL 18.3 C
//! (Stamp-18.3, upstream sha 62d6c7d3df) — see decoder_fuzz::spellfam_diff
//! (lane p1-spell).
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    decoder_fuzz::spellfam_diff(data);
});
