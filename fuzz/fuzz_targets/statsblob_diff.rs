#![no_main]
//! Differential: extended-statistics on-disk bytea deserializers
//! (statext_{ndistinct,dependencies,mcv}_deserialize) shipped Rust vs
//! vendored PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) C —
//! see decoder_fuzz::statext_diff. Attacker surface: PG18 stats restore.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    decoder_fuzz::statext_diff(data);
});
