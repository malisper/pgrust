#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    pgrust::backend::tcop::postgres::fuzz_startup_packet(data);
});
