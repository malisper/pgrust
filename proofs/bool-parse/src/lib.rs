//! Kani C≡Rust equivalence: parse_bool_with_len.
//! Rust side: shipped crates/backend/utils/adt/bool. C side: vendored
//! bool.c parse_bool_with_len + pg_strncasecmp (csrc/bool_shim.c).
//!
//! Domain: symbolic byte strings, len 0..=8. C's callers guarantee that
//! value[len] (one past the logical length) is either the cstring NUL or a
//! trimmed-away isspace() byte — boolin trims before calling. The 'o' case
//! reads that byte (len.max(2)), so the harness constrains buf[len] to that
//! caller-invariant set; Rust models the same read as 0, and the shipped
//! comment at bool/src/lib.rs:22-25 claims space behaves exactly like NUL
//! there. This proof adjudicates that claim.

#[cfg(kani)]
mod proofs {
    extern "C" {
        fn pgc_parse_bool_verdict(value: *const u8, len: u64) -> i32;
    }

    #[kani::proof]
    #[kani::unwind(12)]
    fn parse_bool_equiv() {
        const MAX: usize = 8;
        let len: usize = kani::any();
        kani::assume(len <= MAX);

        let mut buf: [u8; MAX + 1] = kani::any();
        // caller invariant: the byte one past the logical value is the
        // terminating NUL or a trimmed isspace() byte
        let term = buf[len];
        kani::assume(
            term == 0
                || term == b' '
                || term == b'\t'
                || term == b'\n'
                || term == 0x0b
                || term == 0x0c
                || term == b'\r',
        );

        let rust = adt_bool::parse_bool_with_len(&buf[..len]);
        let rust_verdict = match rust {
            None => -1,
            Some(false) => 0,
            Some(true) => 1,
        };

        let c_verdict = unsafe { pgc_parse_bool_verdict(buf.as_ptr(), len as u64) };
        assert_eq!(rust_verdict, c_verdict);
    }
}
