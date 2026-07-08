#![no_main]
//! WAL record decode fuzz target — see decoder_fuzz::wal_record.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    decoder_fuzz::wal_record(data);
});
