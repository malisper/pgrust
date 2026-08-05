#![no_main]
//! Differential: cmdtag/pg_class/earthdistance/pg_rusage/xlogstats/stringinfo
//! shipped Rust vs vendored PostgreSQL 18.3 (Stamp-18.3, upstream sha
//! 62d6c7d3df) C — see decoder_fuzz::miscfam_diff (lane p1-mb-miscfam).
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    decoder_fuzz::miscfam_diff(data);
});
