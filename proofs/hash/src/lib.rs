//! Kani C-vs-Rust equivalence harnesses for pgrust hashfn against verbatim
//! PostgreSQL master src/common/hashfn.c (vendored as pg_hashfn.c, pg_ prefix).
//! Run: ~/.cargo/bin/cargo-kani kani -Z c-ffi --c-lib pg_hashfn.c --harness <h>
#![cfg(kani)]

extern "C" {
    fn pg_hash_bytes(k: *const u8, keylen: i32) -> u32;
    fn pg_hash_bytes_extended(k: *const u8, keylen: i32, seed: u64) -> u64;
    fn pg_hash_bytes_uint32(k: u32) -> u32;
    fn pg_hash_bytes_uint32_extended(k: u32, seed: u64) -> u64;
}

fn check_fixed<const N: usize>() {
    let buf: [u8; N] = kani::any();
    let c = unsafe { pg_hash_bytes(buf.as_ptr(), N as i32) };
    assert_eq!(hashfn::hash_bytes(&buf), c);
}

#[kani::proof]
#[kani::unwind(2)]
fn hash_bytes_len0() {
    let buf: [u8; 1] = kani::any();
    let c = unsafe { pg_hash_bytes(buf.as_ptr(), 0) };
    assert_eq!(hashfn::hash_bytes(&buf[..0]), c);
}

#[kani::proof]
#[kani::unwind(2)]
fn hash_bytes_len4() {
    check_fixed::<4>();
}

#[kani::proof]
#[kani::unwind(2)]
fn hash_bytes_len8() {
    check_fixed::<8>();
}

#[kani::proof]
#[kani::unwind(3)]
fn hash_bytes_len12() {
    check_fixed::<12>();
}

#[kani::proof]
#[kani::unwind(3)]
fn hash_bytes_len16() {
    check_fixed::<16>();
}

#[kani::proof]
#[kani::unwind(4)]
fn hash_bytes_len24() {
    check_fixed::<24>();
}

/// Symbolic length 0..=16 over a symbolic 16-byte buffer.
#[kani::proof]
#[kani::unwind(4)]
fn hash_bytes_symlen16() {
    let buf: [u8; 16] = kani::any();
    let len: usize = kani::any();
    kani::assume(len <= 16);
    let c = unsafe { pg_hash_bytes(buf.as_ptr(), len as i32) };
    assert_eq!(hashfn::hash_bytes(&buf[..len]), c);
}

/// Symbolic length 0..=32 (stretch: two mix-loop iterations reachable).
#[kani::proof]
#[kani::unwind(5)]
fn hash_bytes_symlen32() {
    let buf: [u8; 32] = kani::any();
    let len: usize = kani::any();
    kani::assume(len <= 32);
    let c = unsafe { pg_hash_bytes(buf.as_ptr(), len as i32) };
    assert_eq!(hashfn::hash_bytes(&buf[..len]), c);
}

/// Deliberately misaligned start to force C's byte-at-a-time path.
#[kani::proof]
#[kani::unwind(4)]
fn hash_bytes_len13_unaligned() {
    let buf: [u8; 14] = kani::any();
    let k = &buf[1..14]; // 13 bytes at odd offset
    let c = unsafe { pg_hash_bytes(k.as_ptr(), 13) };
    assert_eq!(hashfn::hash_bytes(k), c);
}

#[kani::proof]
#[kani::unwind(3)]
fn hash_bytes_extended_len8_seeded() {
    let buf: [u8; 8] = kani::any();
    let seed: u64 = kani::any();
    let c = unsafe { pg_hash_bytes_extended(buf.as_ptr(), 8, seed) };
    assert_eq!(hashfn::hash_bytes_extended(&buf, seed), c);
}

#[kani::proof]
fn hash_bytes_uint32_all() {
    let k: u32 = kani::any();
    let c = unsafe { pg_hash_bytes_uint32(k) };
    assert_eq!(hashfn::hash_bytes_uint32(k), c);
}

#[kani::proof]
fn hash_bytes_uint32_extended_all() {
    let k: u32 = kani::any();
    let seed: u64 = kani::any();
    let c = unsafe { pg_hash_bytes_uint32_extended(k, seed) };
    assert_eq!(hashfn::hash_bytes_uint32_extended(k, seed), c);
}

/// Stretch: symbolic length 0..=48 (up to four mix-loop iterations).
#[kani::proof]
#[kani::unwind(6)]
fn hash_bytes_symlen48() {
    let buf: [u8; 48] = kani::any();
    let len: usize = kani::any();
    kani::assume(len <= 48);
    let c = unsafe { pg_hash_bytes(buf.as_ptr(), len as i32) };
    assert_eq!(hashfn::hash_bytes(&buf[..len]), c);
}

/// NEGATIVE CONTROL: deliberately mismatched lengths (Rust hashes 4 bytes,
/// C hashes 3). Must FAIL with a counterexample -- proves the rig detects
/// divergence rather than passing vacuously.
#[kani::proof]
#[kani::unwind(2)]
fn control_expected_divergence() {
    let buf: [u8; 4] = kani::any();
    let c = unsafe { pg_hash_bytes(buf.as_ptr(), 3) };
    assert_eq!(hashfn::hash_bytes(&buf), c);
}
