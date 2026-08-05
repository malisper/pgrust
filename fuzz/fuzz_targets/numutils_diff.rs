#![no_main]
//! Differential: adt/numutils (p1-laneaj) — pg_strtoint16/32/64(_safe),
//! uint32in_subr/uint64in_subr, and the pg_ultoa_n/pg_ulltoa_n/pg_ltoa/
//! pg_lltoa/pg_itoa/pg_ultostr(_zeropad) emit family, shipped Rust vs
//! vendored PostgreSQL 18.3 C — see decoder_fuzz::numutils_diff.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    decoder_fuzz::numutils_diff(data);
});
