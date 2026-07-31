#![no_main]
//! Differential: unary float8 math family (dacos..dtanh, 28 fns) shipped
//! Rust vs vendored PostgreSQL C — see decoder_fuzz::diff::float_math_diff.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    decoder_fuzz::float_math_diff(data);
});
