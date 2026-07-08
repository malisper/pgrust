#![no_main]
//! Wire protocol message-parse fuzz target — see decoder_fuzz::wire_pqformat.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    decoder_fuzz::wire_pqformat(data);
});
