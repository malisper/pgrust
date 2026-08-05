//! Target: cryptofam_diff — the p1-lanef crypto/hash family batch
//! (common/md5, common/sha1, common/hmac, common/scram_common,
//! adt/cryptohashfuncs) shipped Rust vs vendored PostgreSQL 18.3 C
//! (csrc/cryptofam/, verbatim @ 62d6c7d3df, FRONTEND arms) in-process.
//!
//! Comparison planes (harness contract): value bytes (digests, hex text,
//! encrypted-password strings, SCRAM secrets, CRC integers) + error-verdict.
//! No errcode plane exists in this family: every vendored error arm is
//! OOM/EVP-engine failure (frontend `false`/-1/NULL), which the shipped Rust
//! surfaces are infallible against by construction — the comparator treats a
//! C-side failure report as a fatal harness error, never a divergence.
//! Any mismatch panics, so a libFuzzer crash artifact is a divergence
//! reproducer.
//!
//! Domain carves (documented, ratified non-surfaces):
//!   - C `pg_md5_encrypt` and `scram_SaltedPassword`/`scram_build_secret`
//!     take NUL-terminated passwords (strlen-measured): every password
//!     input is truncated at the first NUL before BOTH sides. In real
//!     PostgreSQL the password always arrives as a C string, so NUL-free
//!     is the reachable domain; salts and messages stay binary-safe.
//!   - `scram_salted_password` iteration counts are capped small in-fuzz
//!     (1..=8) — the loop body is iteration-index-independent XOR/HMAC
//!     chaining, and the shipped default (4096) is exercised by the crate
//!     unit suite; an uncapped count would turn every exec into seconds
//!     of PBKDF2 and starve coverage search.
//!   - Both incremental engines (Rust and C) are driven with the SAME
//!     fuzz-chosen update chunking, and the Rust incremental result is
//!     additionally checked against the Rust one-shot, so buffering-arm
//!     equivalence is part of every exec.

use std::ffi::CStr;
use std::os::raw::{c_char, c_int, c_longlong};
use std::sync::Once;

use ::datum::Datum;
use types_fmgr::LocalFcinfo;

extern "C" {
    fn pg_diff_md5_hash(data: *const u8, len: usize, hexsum33: *mut c_char) -> c_int;
    fn pg_diff_md5_binary(data: *const u8, len: usize, out16: *mut u8) -> c_int;
    fn pg_diff_md5_encrypt(
        passwd: *const c_char,
        salt: *const u8,
        salt_len: usize,
        buf36: *mut c_char,
    ) -> c_int;
    fn pg_diff_sha(
        which: c_int,
        data: *const u8,
        len: usize,
        chunk_lens: *const usize,
        nchunks: c_int,
        out: *mut u8,
        outlen: *mut usize,
    ) -> c_int;
    fn pg_diff_hmac(
        which: c_int,
        key: *const u8,
        keylen: usize,
        msg: *const u8,
        msglen: usize,
        chunk_lens: *const usize,
        nchunks: c_int,
        out: *mut u8,
        outlen: *mut usize,
    ) -> c_int;
    fn pg_diff_scram_salted_password(
        password: *const c_char,
        salt: *const u8,
        saltlen: c_int,
        iterations: c_int,
        out32: *mut u8,
    ) -> c_int;
    fn pg_diff_scram_h(input: *const u8, out32: *mut u8) -> c_int;
    fn pg_diff_scram_client_key(salted_password: *const u8, out32: *mut u8) -> c_int;
    fn pg_diff_scram_server_key(salted_password: *const u8, out32: *mut u8) -> c_int;
    fn pg_diff_scram_build_secret(
        salt: *const u8,
        saltlen: c_int,
        iterations: c_int,
        password: *const c_char,
    ) -> *mut c_char;
    fn pg_diff_crc32_bytea(data: *const u8, len: usize) -> c_longlong;
    fn pg_diff_crc32c_bytea(data: *const u8, len: usize) -> c_longlong;
    fn free(p: *mut std::ffi::c_void);
}

/// NUL-truncated prefix (the C-string password domain; see header) plus a
/// NUL-terminated copy for the C side.
fn password_domain(bytes: &[u8]) -> (&[u8], Vec<u8>) {
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    let mut c = bytes[..end].to_vec();
    c.push(0);
    (&bytes[..end], c)
}

/// Deterministic update-chunk schedule from one fuzz byte: chunk sizes walk
/// 1..=97 so block boundaries (64/128) are crossed at every phase.
fn chunk_schedule(seed: u8, len: usize) -> Vec<usize> {
    let mut chunks = Vec::new();
    let mut left = len;
    let mut sz = (seed as usize % 97) + 1;
    while left > 0 {
        let take = sz.min(left);
        chunks.push(take);
        left -= take;
        sz = (sz * 31 + 7) % 97 + 1;
    }
    if chunks.is_empty() {
        chunks.push(0); // one empty update: exercises the zero-len arm
    }
    chunks
}

/// Call a strict 1-arg bytea/text fmgr builtin with `input` as a 4B-header
/// varlena and return the result varlena's data bytes.
fn call_bytea_fn(
    f: fn(
        Option<&mut types_fmgr::FmgrInfo>,
        &mut types_fmgr::FunctionCallInfoBaseData,
    ) -> types_error::PgResult<Datum>,
    input: &[u8],
) -> Vec<u8> {
    let mut v = Vec::with_capacity(4 + input.len());
    v.extend_from_slice(&::datum::varlena::set_varsize_4b(4 + input.len()));
    v.extend_from_slice(input);
    let ctx = mcx::MemoryContext::new("cryptofam_diff");
    let mut fcinfo = LocalFcinfo::<1>::new(0);
    // SAFETY: ctx outlives the call and the result read below.
    unsafe { fcinfo.set_result_mcx(ctx.mcx()) };
    fcinfo.set_arg(0, Datum::from_usize(v.as_ptr() as usize));
    let d = f(None, &mut fcinfo).expect("crypto builtins are infallible on non-null input");
    // SAFETY: the wrappers return an uncompressed 4B-header varlena
    // allocated in ctx, alive until ctx drops at end of scope.
    let r = unsafe { ::datum::varlena::VarlenaRef::from_ptr(d.as_usize() as *const u8) };
    r.data().to_vec()
}

fn call_int_fn(
    f: fn(
        Option<&mut types_fmgr::FmgrInfo>,
        &mut types_fmgr::FunctionCallInfoBaseData,
    ) -> types_error::PgResult<Datum>,
    input: &[u8],
) -> i64 {
    let mut v = Vec::with_capacity(4 + input.len());
    v.extend_from_slice(&::datum::varlena::set_varsize_4b(4 + input.len()));
    v.extend_from_slice(input);
    let ctx = mcx::MemoryContext::new("cryptofam_diff");
    let mut fcinfo = LocalFcinfo::<1>::new(0);
    // SAFETY: ctx outlives the call.
    unsafe { fcinfo.set_result_mcx(ctx.mcx()) };
    fcinfo.set_arg(0, Datum::from_usize(v.as_ptr() as usize));
    f(None, &mut fcinfo)
        .expect("crc builtins are infallible on non-null input")
        .as_i64()
}

fn init_seams() {
    static ONCE: Once = Once::new();
    // Tolerate hashenc (p1-lanee) installing the identical no-op CFI seam
    // first — both lanes' oracles run in one test binary since the p1 union.
    ONCE.call_once(|| {
        let _ = std::panic::catch_unwind(|| {
            postgres_seams::check_for_interrupts::set(|| Ok(()));
        });
    });
}

fn diff_md5(payload: &[u8]) {
    let (split, msg) = match payload.split_first() {
        Some((s, m)) => (*s, m),
        None => (0, &[][..]),
    };

    // Rust one-shot binary + hex.
    let r_bin = pg_md5::pg_md5_binary(msg);
    let r_hex = pg_md5::pg_md5_hash(msg);

    // Rust incremental with the fuzz-chosen schedule must match one-shot.
    let mut inc = pg_md5::Md5::new();
    let mut off = 0;
    for c in chunk_schedule(split, msg.len()) {
        inc.update(&msg[off..off + c]);
        off += c;
    }
    assert_eq!(inc.finish(), r_bin, "md5 incremental != one-shot");

    // C oracle.
    let mut c_bin = [0u8; 16];
    let mut c_hex = [0i8; 33];
    // SAFETY: msg live for the call; out buffers sized per md5.h contract.
    let rc1 = unsafe { pg_diff_md5_binary(msg.as_ptr(), msg.len(), c_bin.as_mut_ptr()) };
    let rc2 = unsafe { pg_diff_md5_hash(msg.as_ptr(), msg.len(), c_hex.as_mut_ptr().cast()) };
    assert_eq!((rc1, rc2), (0, 0), "C md5 engine failure (harness error)");
    assert_eq!(r_bin, c_bin, "pg_md5_binary diverges");
    let c_hex = unsafe { CStr::from_ptr(c_hex.as_ptr().cast()) }.to_bytes();
    assert_eq!(&r_hex[..], c_hex, "pg_md5_hash diverges");

    // fmgr wrappers (md5_text 2311 / md5_bytea 2321 share md5_common).
    assert_eq!(
        call_bytea_fn(cryptohashfuncs::fc_md5_text, msg),
        r_hex.to_vec(),
        "fc_md5_text diverges from pg_md5_hash"
    );
    assert_eq!(
        call_bytea_fn(cryptohashfuncs::fc_md5_bytea, msg),
        r_hex.to_vec(),
        "fc_md5_bytea diverges from pg_md5_hash"
    );
}

fn diff_md5_encrypt(payload: &[u8]) {
    let (n, rest) = match payload.split_first() {
        Some((n, r)) => (*n as usize, r),
        None => return,
    };
    let n = n.min(rest.len());
    let (passwd_raw, salt) = rest.split_at(n);
    let (passwd, passwd_c) = password_domain(passwd_raw);

    let r = pg_md5::pg_md5_encrypt(passwd, salt);
    let mut c = [0i8; 36];
    // SAFETY: NUL-terminated passwd_c; salt live; 36-byte out per md5.h.
    let rc = unsafe {
        pg_diff_md5_encrypt(
            passwd_c.as_ptr().cast(),
            salt.as_ptr(),
            salt.len(),
            c.as_mut_ptr().cast(),
        )
    };
    assert_eq!(rc, 0, "C pg_md5_encrypt failure (harness error)");
    let c = unsafe { CStr::from_ptr(c.as_ptr().cast()) }.to_bytes();
    assert_eq!(&r[..], c, "pg_md5_encrypt diverges");
}

fn diff_sha1(payload: &[u8]) {
    let (split, msg) = match payload.split_first() {
        Some((s, m)) => (*s, m),
        None => (0, &[][..]),
    };

    let r = pg_sha1::sha1(msg);

    let chunks = chunk_schedule(split, msg.len());
    let mut inc = pg_sha1::Sha1::init();
    let mut off = 0;
    for &c in &chunks {
        inc.update(&msg[off..off + c]);
        off += c;
    }
    assert_eq!(inc.finish(), r, "sha1 incremental != one-shot");

    let mut c_out = [0u8; 20];
    let mut c_len = 0usize;
    // SAFETY: chunks sum to msg.len(); out sized for SHA1_DIGEST_LENGTH.
    let rc = unsafe {
        pg_diff_sha(
            0,
            msg.as_ptr(),
            msg.len(),
            chunks.as_ptr(),
            chunks.len() as c_int,
            c_out.as_mut_ptr(),
            &mut c_len,
        )
    };
    assert_eq!((rc, c_len), (0, 20), "C sha1 engine failure (harness error)");
    assert_eq!(r, c_out, "sha1 diverges");
}

fn diff_sha2_wrappers(payload: &[u8]) {
    let (sel, msg) = match payload.split_first() {
        Some((s, m)) => (*s, m),
        None => (0, &[][..]),
    };
    let which = (sel & 3) as usize;
    const FNS: [(
        fn(
            Option<&mut types_fmgr::FmgrInfo>,
            &mut types_fmgr::FunctionCallInfoBaseData,
        ) -> types_error::PgResult<Datum>,
        usize,
    ); 4] = [
        (cryptohashfuncs::fc_sha224_bytea, 28),
        (cryptohashfuncs::fc_sha256_bytea, 32),
        (cryptohashfuncs::fc_sha384_bytea, 48),
        (cryptohashfuncs::fc_sha512_bytea, 64),
    ];
    let (f, dlen) = FNS[which];
    let r = call_bytea_fn(f, msg);

    let mut c_out = [0u8; 64];
    let mut c_len = 0usize;
    let chunks = [msg.len()];
    // SAFETY: single chunk covering msg; out sized for the widest digest.
    let rc = unsafe {
        pg_diff_sha(
            which as c_int + 1,
            msg.as_ptr(),
            msg.len(),
            chunks.as_ptr(),
            1,
            c_out.as_mut_ptr(),
            &mut c_len,
        )
    };
    assert_eq!((rc, c_len), (0, dlen), "C sha2 engine failure (harness error)");
    assert_eq!(r, c_out[..dlen].to_vec(), "sha{}_bytea diverges", [224, 256, 384, 512][which]);
}

fn diff_hmac(payload: &[u8]) {
    let mut it = payload.iter();
    let (sel, klen, split) = match (it.next(), it.next(), it.next()) {
        (Some(a), Some(b), Some(c)) => (*a, *b as usize, *c),
        _ => return,
    };
    let rest = &payload[3..];
    let klen = klen.min(rest.len());
    let (key, msg) = rest.split_at(klen);
    let which = (sel & 3) as usize;

    let chunks = chunk_schedule(split, msg.len());

    fn rust_hmac<H: pg_hmac::HmacHash>(key: &[u8], msg: &[u8], chunks: &[usize]) -> Vec<u8> {
        let mut ctx = pg_hmac::PgHmacCtx::<H>::init(key);
        let mut off = 0;
        for &c in chunks {
            ctx.update(&msg[off..off + c]);
            off += c;
        }
        ctx.finalize().as_ref().to_vec()
    }
    let r = match which {
        0 => rust_hmac::<pg_hmac::Sha224>(key, msg, &chunks),
        1 => rust_hmac::<pg_hmac::Sha256>(key, msg, &chunks),
        2 => rust_hmac::<pg_hmac::Sha384>(key, msg, &chunks),
        _ => rust_hmac::<pg_hmac::Sha512>(key, msg, &chunks),
    };
    if which == 1 {
        assert_eq!(
            r,
            pg_hmac::hmac_sha256(key, msg).to_vec(),
            "hmac_sha256 one-shot != incremental"
        );
    }

    let mut c_out = [0u8; 64];
    let mut c_len = 0usize;
    // SAFETY: chunks sum to msg.len(); out sized for the widest digest.
    let rc = unsafe {
        pg_diff_hmac(
            which as c_int,
            key.as_ptr(),
            key.len(),
            msg.as_ptr(),
            msg.len(),
            chunks.as_ptr(),
            chunks.len() as c_int,
            c_out.as_mut_ptr(),
            &mut c_len,
        )
    };
    assert_eq!((rc, c_len), (0, r.len()), "C hmac engine failure (harness error)");
    assert_eq!(r, c_out[..r.len()].to_vec(), "hmac diverges (width {which})");
}

fn diff_scram(payload: &[u8]) {
    init_seams();
    let mut it = payload.iter();
    let (i_sel, slen) = match (it.next(), it.next()) {
        (Some(a), Some(b)) => (*a, *b as usize),
        _ => return,
    };
    let rest = &payload[2..];
    let iterations = (i_sel % 8) as i32 + 1; // capped: see header carve
    let slen = slen.min(rest.len());
    let (salt, passwd_raw) = rest.split_at(slen);
    let (passwd, passwd_c) = password_domain(passwd_raw);

    let r_sp = scram_common::scram_salted_password(passwd, salt, iterations)
        .expect("interrupt seam returns Ok");
    let mut c_sp = [0u8; 32];
    // SAFETY: NUL-terminated password; salt live; out is 32 (SHA-256 key len).
    let rc = unsafe {
        pg_diff_scram_salted_password(
            passwd_c.as_ptr().cast(),
            salt.as_ptr(),
            salt.len() as c_int,
            iterations,
            c_sp.as_mut_ptr(),
        )
    };
    assert_eq!(rc, 0, "C scram_SaltedPassword failure (harness error)");
    assert_eq!(r_sp, c_sp, "scram_salted_password diverges");

    let r_ck = scram_common::scram_client_key(&r_sp);
    let r_sk = scram_common::scram_server_key(&r_sp);
    let r_h = scram_common::scram_h(&r_ck);
    let (mut c_ck, mut c_sk, mut c_h) = ([0u8; 32], [0u8; 32], [0u8; 32]);
    // SAFETY: 32-byte ins/outs per scram-common.h.
    unsafe {
        assert_eq!(pg_diff_scram_client_key(c_sp.as_ptr(), c_ck.as_mut_ptr()), 0);
        assert_eq!(pg_diff_scram_server_key(c_sp.as_ptr(), c_sk.as_mut_ptr()), 0);
        assert_eq!(pg_diff_scram_h(c_ck.as_ptr(), c_h.as_mut_ptr()), 0);
    }
    assert_eq!(r_ck, c_ck, "scram_client_key diverges");
    assert_eq!(r_sk, c_sk, "scram_server_key diverges");
    assert_eq!(r_h, c_h, "scram_h diverges");

    // Full secret string (exercises pg_b64 + formatting on both sides).
    let ctx = mcx::MemoryContext::new("cryptofam_diff");
    let r_secret = scram_common::scram_build_secret(ctx.mcx(), salt, iterations, passwd)
        .expect("infallible: allocator mock never fails");
    let c_secret = unsafe {
        pg_diff_scram_build_secret(
            salt.as_ptr(),
            salt.len() as c_int,
            iterations,
            passwd_c.as_ptr().cast(),
        )
    };
    assert!(!c_secret.is_null(), "C scram_build_secret failure (harness error)");
    let c_str = unsafe { CStr::from_ptr(c_secret) }.to_bytes().to_vec();
    unsafe { free(c_secret.cast()) };
    assert_eq!(r_secret.as_bytes(), &c_str[..], "scram_build_secret diverges");
}

fn diff_crc(payload: &[u8]) {
    let r32 = call_int_fn(cryptohashfuncs::fc_crc32_bytea, payload);
    let r32c = call_int_fn(cryptohashfuncs::fc_crc32c_bytea, payload);
    // SAFETY: payload live for both calls.
    let c32 = unsafe { pg_diff_crc32_bytea(payload.as_ptr(), payload.len()) };
    let c32c = unsafe { pg_diff_crc32c_bytea(payload.as_ptr(), payload.len()) };
    assert_eq!(r32, c32 as i64, "crc32_bytea diverges");
    assert_eq!(r32c, c32c as i64, "crc32c_bytea diverges");
}

/// Entry point: data[0] selects the family member, the rest is its payload.
pub fn cryptofam_diff(data: &[u8]) {
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
    let (op, payload) = match data.split_first() {
        Some((op, p)) => (*op, p),
        None => return,
    };
    match op % 7 {
        0 => diff_md5(payload),
        1 => diff_md5_encrypt(payload),
        2 => diff_sha1(payload),
        3 => diff_sha2_wrappers(payload),
        4 => diff_hmac(payload),
        5 => diff_scram(payload),
        _ => diff_crc(payload),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_ops_smoke() {
        let _serial = crate::c_oracle_serial();
        for op in 0u8..7 {
            let mut v = vec![op, 3, 5];
            v.extend_from_slice(b"the quick brown fox jumps over the lazy dog");
            cryptofam_diff(&v);
        }
    }

    #[test]
    fn empty_and_boundary_lengths() {
        let _serial = crate::c_oracle_serial();
        for op in 0u8..7 {
            cryptofam_diff(&[op]);
            for n in [55usize, 56, 63, 64, 65, 127, 128, 129, 200] {
                let mut v = vec![op, 7, 2];
                v.extend(std::iter::repeat(0xA5u8).take(n));
                cryptofam_diff(&v);
            }
        }
    }

    // Shipped-default iteration count: the fuzz cap (<=8) never reaches it,
    // so pin it here once against the C oracle (spot check, slow-ish).
    #[test]
    fn scram_default_iterations_spot() {
        let _serial = crate::c_oracle_serial();
        init_seams();
        let passwd = b"pencil";
        let salt: &[u8] = &[0x41, 0x25, 0xc2, 0x47, 0xe4, 0x3a, 0xb1, 0xe9, 0x3c, 0x6d, 0xff, 0x76];
        let iters = scram_common::SCRAM_SHA_256_DEFAULT_ITERATIONS;
        let r = scram_common::scram_salted_password(passwd, salt, iters).unwrap();
        let mut c = [0u8; 32];
        let mut pc = passwd.to_vec();
        pc.push(0);
        let rc = unsafe {
            pg_diff_scram_salted_password(
                pc.as_ptr().cast(),
                salt.as_ptr(),
                salt.len() as c_int,
                iters,
                c.as_mut_ptr(),
            )
        };
        assert_eq!(rc, 0);
        assert_eq!(r, c);
    }

    // CI regression rail: replay the banked corpus (fleet 12M-exec campaign
    // survivors + seeds) through the full comparator on every test run.
    #[test]
    fn cryptofam_corpus_replay() {
        let _serial = crate::c_oracle_serial();
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/cryptofam_diff");
        let mut n = 0;
        if let Ok(rd) = std::fs::read_dir(dir) {
            for e in rd.flatten() {
                if e.path().is_file() {
                    cryptofam_diff(&std::fs::read(e.path()).unwrap());
                    n += 1;
                }
            }
        }
        assert!(n >= 60, "corpus bank missing or truncated ({n} units)");
    }

    #[test]
    fn password_nul_truncation_carve_is_symmetric() {
        let _serial = crate::c_oracle_serial();
        // Interior NUL: both sides must see "ab".
        let mut v = vec![1u8, 5]; // op=1 md5_encrypt, passwd_len=5
        v.extend_from_slice(b"ab\0cd");
        v.extend_from_slice(b"somesalt");
        cryptofam_diff(&v);
        let r = pg_md5::pg_md5_encrypt(b"ab", b"somesalt");
        let r2 = pg_md5::pg_md5_encrypt(b"ab\0cd", b"somesalt");
        assert_ne!(r, r2, "sanity: NUL truncation is semantically visible");
    }
}
