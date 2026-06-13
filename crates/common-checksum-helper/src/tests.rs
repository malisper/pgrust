//! Unit tests for `common-checksum-helper`.
//!
//! CRC-32C is exercised against published known-answer vectors. The SHA-2 path
//! is exercised through a deterministic mock installed on the cryptohash
//! owner's seam (no real cryptohash crate exists yet), modelling the real
//! `pg_cryptohash_ctx *` lifetime.

extern crate std;

use super::*;
use std::boxed::Box;

// ---------------------------------------------------------------------------
// parse / name round-trips
// ---------------------------------------------------------------------------

#[test]
fn parses_checksum_type_case_insensitively() {
    assert_eq!(pg_checksum_parse_type(b"none\0"), Some(CHECKSUM_TYPE_NONE));
    assert_eq!(pg_checksum_parse_type(b"NONE\0"), Some(CHECKSUM_TYPE_NONE));
    assert_eq!(
        pg_checksum_parse_type(b"crc32c\0"),
        Some(CHECKSUM_TYPE_CRC32C)
    );
    assert_eq!(
        pg_checksum_parse_type(b"CRC32C\0"),
        Some(CHECKSUM_TYPE_CRC32C)
    );
    assert_eq!(
        pg_checksum_parse_type(b"sha224\0"),
        Some(CHECKSUM_TYPE_SHA224)
    );
    assert_eq!(
        pg_checksum_parse_type(b"Sha256\0"),
        Some(CHECKSUM_TYPE_SHA256)
    );
    assert_eq!(
        pg_checksum_parse_type(b"SHA384\0"),
        Some(CHECKSUM_TYPE_SHA384)
    );
    assert_eq!(
        pg_checksum_parse_type(b"sha512\0"),
        Some(CHECKSUM_TYPE_SHA512)
    );
}

#[test]
fn rejects_unknown_checksum_type() {
    assert_eq!(pg_checksum_parse_type(b"unknown\0"), None);
    assert_eq!(pg_checksum_parse_type(b"\0"), None);
    assert_eq!(pg_checksum_parse_type(b"sha\0"), None);
    assert_eq!(pg_checksum_parse_type(b"md5\0"), None);
}

#[test]
fn checksum_type_names_match_postgres() {
    assert_eq!(pg_checksum_type_name(CHECKSUM_TYPE_NONE), "NONE");
    assert_eq!(pg_checksum_type_name(CHECKSUM_TYPE_CRC32C), "CRC32C");
    assert_eq!(pg_checksum_type_name(CHECKSUM_TYPE_SHA224), "SHA224");
    assert_eq!(pg_checksum_type_name(CHECKSUM_TYPE_SHA256), "SHA256");
    assert_eq!(pg_checksum_type_name(CHECKSUM_TYPE_SHA384), "SHA384");
    assert_eq!(pg_checksum_type_name(CHECKSUM_TYPE_SHA512), "SHA512");
}

#[test]
fn enum_discriminants_match_c_order() {
    assert_eq!(CHECKSUM_TYPE_NONE as u32, 0);
    assert_eq!(CHECKSUM_TYPE_CRC32C as u32, 1);
    assert_eq!(CHECKSUM_TYPE_SHA224 as u32, 2);
    assert_eq!(CHECKSUM_TYPE_SHA256 as u32, 3);
    assert_eq!(CHECKSUM_TYPE_SHA384 as u32, 4);
    assert_eq!(CHECKSUM_TYPE_SHA512 as u32, 5);
}

/// init → update → final, returning the digest bytes.
fn digest(ty: pg_checksum_type, input: &[u8]) -> Result<std::vec::Vec<u8>, ChecksumError> {
    let mut ctx = pg_checksum_init(ty)?;
    pg_checksum_update(&mut ctx, input)?;
    let mut out = [0u8; PG_CHECKSUM_MAX_LENGTH];
    let n = pg_checksum_final(&mut ctx, &mut out)?;
    Ok(out[..n].to_vec())
}

// ---------------------------------------------------------------------------
// NONE
// ---------------------------------------------------------------------------

#[test]
fn none_produces_zero_length_digest() {
    let mut ctx = pg_checksum_init(CHECKSUM_TYPE_NONE).unwrap();
    assert_eq!(ctx.checksum_type(), CHECKSUM_TYPE_NONE);
    pg_checksum_update(&mut ctx, b"anything").unwrap();
    let mut out = [0u8; PG_CHECKSUM_MAX_LENGTH];
    assert_eq!(pg_checksum_final(&mut ctx, &mut out).unwrap(), 0);

    assert_eq!(digest(CHECKSUM_TYPE_NONE, b"abc").unwrap(), []);
}

// ---------------------------------------------------------------------------
// CRC-32C known-answer vectors
// ---------------------------------------------------------------------------

/// The standard CRC-32C check value: the ASCII string "123456789" yields
/// 0xE3069283 (the canonical Castagnoli check constant).
#[test]
fn crc32c_standard_check_value() {
    let d = digest(CHECKSUM_TYPE_CRC32C, b"123456789").unwrap();
    assert_eq!(d.len(), 4);
    let value = u32::from_ne_bytes([d[0], d[1], d[2], d[3]]);
    assert_eq!(value, 0xE306_9283);
}

#[test]
fn crc32c_empty_input_is_zero() {
    let d = digest(CHECKSUM_TYPE_CRC32C, b"").unwrap();
    let value = u32::from_ne_bytes([d[0], d[1], d[2], d[3]]);
    assert_eq!(value, 0x0000_0000);
}

#[test]
fn crc32c_incremental_matches_one_shot() {
    let data = b"The quick brown fox jumps over the lazy dog";
    let one_shot = digest(CHECKSUM_TYPE_CRC32C, data).unwrap();

    let mut ctx = pg_checksum_init(CHECKSUM_TYPE_CRC32C).unwrap();
    pg_checksum_update(&mut ctx, &data[..10]).unwrap();
    pg_checksum_update(&mut ctx, &data[10..]).unwrap();
    let mut out = [0u8; PG_CHECKSUM_MAX_LENGTH];
    let n = pg_checksum_final(&mut ctx, &mut out).unwrap();
    assert_eq!(&out[..n], &one_shot[..]);
}

#[test]
fn crc32c_final_rejects_undersized_buffer() {
    let mut ctx = pg_checksum_init(CHECKSUM_TYPE_CRC32C).unwrap();
    pg_checksum_update(&mut ctx, b"x").unwrap();
    let mut tiny = [0u8; 2];
    assert_eq!(
        pg_checksum_final(&mut ctx, &mut tiny),
        Err(ChecksumError::OutputTooSmall {
            needed: 4,
            actual: 2
        })
    );
}

// ---------------------------------------------------------------------------
// SHA-2 via the cryptohash owner's seam (deterministic mock)
// ---------------------------------------------------------------------------

// A trivial mock provider modelling the real pg_cryptohash_ctx lifetime: create
// boxes a small state, the calls operate on it, and free drops the box. It
// "hashes" by remembering the requested type and running byte count, returning
// a fixed-length digest with the count in byte 0. Enough to verify dispatch,
// pointer lifetime, and length logic without real crypto.

use core::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard, Once};

static MOCK_FREED: AtomicUsize = AtomicUsize::new(0);

// The cryptohash mock is installed on a process-global seam, and `MOCK_FREED`
// is a process-global static. Every test that installs/observes that mock must
// run serially with respect to one another: otherwise one test's
// `pg_cryptohash_free` (incrementing `MOCK_FREED`) races another test's
// `store(0)`/`load()` of the same counter, producing spurious failures only
// under `cargo test`'s default parallel execution. Each such test acquires
// this lock for its full body (install -> use -> observe -> teardown).
static TEST_MUTEX: Mutex<()> = Mutex::new(());

/// Serialize a test that touches the global cryptohash mock seam / `MOCK_FREED`.
/// The returned guard must be held for the whole test body. A poisoned lock
/// (another such test panicked) is recovered rather than re-panicking, so one
/// genuine failure does not cascade into spurious failures of the siblings.
fn lock_global_mock() -> MutexGuard<'static, ()> {
    TEST_MUTEX.lock().unwrap_or_else(|e| e.into_inner())
}

struct MockCtx {
    bytes: usize,
}

fn mock_create(type_: pg_cryptohash_type) -> *mut pg_cryptohash_ctx {
    // Only the four SHA-2 variants are ever requested by checksum_helper.
    assert!(matches!(
        type_,
        pg_cryptohash_type::PG_SHA224
            | pg_cryptohash_type::PG_SHA256
            | pg_cryptohash_type::PG_SHA384
            | pg_cryptohash_type::PG_SHA512
    ));
    Box::into_raw(Box::new(MockCtx { bytes: 0 })) as *mut pg_cryptohash_ctx
}

fn mock_init(_ctx: *mut pg_cryptohash_ctx) -> i32 {
    0
}

fn mock_update(ctx: *mut pg_cryptohash_ctx, data: *const u8, len: usize) -> i32 {
    let m = unsafe { &mut *(ctx as *mut MockCtx) };
    let _ = data;
    m.bytes += len;
    0
}

fn mock_final(ctx: *mut pg_cryptohash_ctx, dest: *mut u8, len: usize) -> i32 {
    let m = unsafe { &*(ctx as *mut MockCtx) };
    let out = unsafe { core::slice::from_raw_parts_mut(dest, len) };
    for b in out.iter_mut() {
        *b = 0;
    }
    if !out.is_empty() {
        out[0] = m.bytes as u8;
    }
    0
}

fn mock_free(ctx: *mut pg_cryptohash_ctx) {
    drop(unsafe { Box::from_raw(ctx as *mut MockCtx) });
    MOCK_FREED.fetch_add(1, Ordering::SeqCst);
}

static INSTALL: Once = Once::new();

fn install_mock() {
    INSTALL.call_once(|| {
        common_cryptohash_seams::pg_cryptohash_create::set(mock_create);
        common_cryptohash_seams::pg_cryptohash_init::set(mock_init);
        common_cryptohash_seams::pg_cryptohash_update::set(mock_update);
        common_cryptohash_seams::pg_cryptohash_final::set(mock_final);
        common_cryptohash_seams::pg_cryptohash_free::set(mock_free);
    });
}

#[test]
fn sha2_dispatch_lengths_and_pointer_lifetime() {
    let _guard = lock_global_mock();
    install_mock();

    for (ty, expected_len) in [
        (CHECKSUM_TYPE_SHA224, 28usize),
        (CHECKSUM_TYPE_SHA256, 32),
        (CHECKSUM_TYPE_SHA384, 48),
        (CHECKSUM_TYPE_SHA512, 64),
    ] {
        MOCK_FREED.store(0, Ordering::SeqCst);
        let mut ctx = pg_checksum_init(ty).unwrap();
        assert_eq!(ctx.checksum_type(), ty);
        pg_checksum_update(&mut ctx, b"hello").unwrap();
        pg_checksum_update(&mut ctx, b"!").unwrap();
        let mut out = [0u8; PG_CHECKSUM_MAX_LENGTH];
        let n = pg_checksum_final(&mut ctx, &mut out).unwrap();
        assert_eq!(n, expected_len, "digest length for {ty:?}");
        // mock final encodes the running byte count in byte 0
        assert_eq!(out[0], 6, "byte count threaded through update");
        // final must have issued exactly one free (matching C pg_cryptohash_free)
        assert_eq!(MOCK_FREED.load(Ordering::SeqCst), 1);
    }
}

#[test]
fn sha2_final_rejects_undersized_buffer() {
    let _guard = lock_global_mock();
    install_mock();
    let mut ctx = pg_checksum_init(CHECKSUM_TYPE_SHA256).unwrap();
    pg_checksum_update(&mut ctx, b"x").unwrap();
    let mut tiny = [0u8; 8];
    assert_eq!(
        pg_checksum_final(&mut ctx, &mut tiny),
        Err(ChecksumError::OutputTooSmall {
            needed: 32,
            actual: 8
        })
    );
    // Buffer-size check happens before the cryptohash context is finalized/freed,
    // matching C; the undersized context is left intact.
    common_cryptohash_seams::pg_cryptohash_free::call(match ctx.raw_context {
        RawContext::Sha2(p) => p,
        _ => unreachable!(),
    });
}
