//! Native differential grounding the [S4] libc strtou64 MODEL
//! (c/pg_xid8snap.c pg_proof_strtou64) against the REAL libc strtoull.
//!
//! The Kani proof eq_strtou64_len6 establishes xid8funcs::strtou64 ≡
//! pg_proof_strtou64 (the model). This test establishes
//! xid8funcs::strtou64 ≡ real libc strtoull(s, &e, 10) over millions of
//! adversarial inputs, transitively grounding the model. errno/ERANGE is
//! out of scope (the code under proof never consults it) — only the
//! returned value (saturated on overflow) and the end pointer are
//! compared, matching the proof's claim.
//!
//! PLATFORM: runs against the host libc (macOS/BSD on the authoring
//! laptop). Per the ground-truth law, replay on Linux/glibc before
//! treating any mismatch as reportable; strtoull base-10 value/endptr
//! behavior is C-standard-pinned so variance is not expected (the known
//! strtoul platform variance is errno-on-no-conversion, out of scope).

use std::os::raw::{c_char, c_int};

extern "C" {
    fn strtoull(s: *const c_char, endp: *mut *mut c_char, base: c_int) -> u64;
}

fn libc_strtou64(bytes_nul: &[u8]) -> (u64, usize) {
    let mut endp: *mut c_char = std::ptr::null_mut();
    let start = bytes_nul.as_ptr() as *const c_char;
    let v = unsafe { strtoull(start, &mut endp, 10) };
    (v, unsafe { endp.offset_from(start) } as usize)
}

/// xorshift64* — deterministic, seedable, no deps
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545F4914F6CDD1D)
    }
}

const ALPHABET: &[u8] = b"0123456789999999   \t+-:,xabZ\x7f."; // digit-heavy

#[test]
fn native_differential_vs_libc_strtoull() {
    let mut rng = Rng(0x9E3779B97F4A7C15);
    let mut checks: u64 = 0;

    // structured probes: saturation boundary, huge digit runs, signs
    let fixed: &[&[u8]] = &[
        b"",
        b" ",
        b"+",
        b"-",
        b"18446744073709551615",
        b"18446744073709551616",
        b"99999999999999999999999999",
        b"-1",
        b"-18446744073709551615",
        b"  +42x",
        b"\t\n\x0b\x0c\r 7",
        b"0000000000000000000000000009",
    ];
    for f in fixed {
        let mut buf = f.to_vec();
        buf.push(0);
        let (cv, ce) = libc_strtou64(&buf);
        let (rv, re) = xid8funcs::strtou64(f);
        assert_eq!((cv, ce), (rv, re), "fixed input {:?}", String::from_utf8_lossy(f));
        checks += 1;
    }

    // randomized: lengths 0..=14 over a digit-heavy alphabet, NUL-free
    for _ in 0..4_000_000u64 {
        let len = (rng.next() % 15) as usize;
        let mut s = Vec::with_capacity(len + 1);
        for _ in 0..len {
            s.push(ALPHABET[(rng.next() % ALPHABET.len() as u64) as usize]);
        }
        let slice = s.clone();
        s.push(0);
        let (cv, ce) = libc_strtou64(&s);
        let (rv, re) = xid8funcs::strtou64(&slice);
        assert_eq!(
            (cv, ce),
            (rv, re),
            "input {:?}",
            String::from_utf8_lossy(&slice)
        );
        checks += 1;
    }
    eprintln!("native strtou64 differential: {checks} checks, 0 diffs");
}
