//! hashenc_diff: encoding + crypto/hash family differential driver
//! (p1-lanee campaign batch: common/base64, common/md5, common/sha1,
//! common/hmac, common/scram_common, adt/adt_ascii, adt/cryptohashfuncs).
//!
//! Shipped Rust vs VERBATIM vendored PostgreSQL 18.3 C (csrc/hashenc/,
//! provenance in csrc/hashenc/shim/c.h), both in-process. Comparison
//! planes: exact value bytes/bits + error-verdict + errcode class. Any
//! mismatch panics -> libFuzzer crash artifact = divergence reproducer.
//!
//! Input layout: [sel][payload...]; sel % 16 picks the family, so one
//! corpus explores all siblings (the float_in_diff selector pattern).
//!
//! Documented non-surfaces (target-header carves):
//! - message text (never compared; C errmsg is compiled out in the shim).
//! - encoding-NAME lookup (fc_to_ascii_encname): pg_char_to_encoding is
//!   mbutils/encnames territory (not in this batch); the wrapper is driven
//!   through names resolved by the shipped Rust lookup and the CONVERSION
//!   result is compared at the resolved encoding. The name->id table
//!   itself is owned by the encnames proof family.
//! - to_ascii_default's GetDatabaseEncoding() is session state: checked as
//!   self-consistency against fc_to_ascii_enc at the same encoding.
//! - scram passwords are NUL-free (C scram_SaltedPassword takes a cstring;
//!   the server never passes embedded NULs — SASLprep rejects them).

use datum::Datum;
use mcx::MemoryContext;
use types_error::{PgError, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_UNDEFINED_OBJECT};
use types_fmgr::{FmgrInfo, FunctionCallInfoBaseData as Fcinfo, LocalFcinfo};

extern "C" {
    fn pg_hashenc_b64_encode(src: *const u8, len: i32, dst: *mut u8, dstlen: i32) -> i32;
    fn pg_hashenc_b64_decode(src: *const u8, len: i32, dst: *mut u8, dstlen: i32) -> i32;
    fn pg_hashenc_b64_enc_len(srclen: i32) -> i32;
    fn pg_hashenc_b64_dec_len(srclen: i32) -> i32;
    fn pg_hashenc_md5_hash(buff: *const u8, len: usize, hexsum33: *mut u8) -> i32;
    fn pg_hashenc_md5_binary(buff: *const u8, len: usize, out16: *mut u8) -> i32;
    fn pg_hashenc_md5_encrypt(passwd: *const u8, salt: *const u8, salt_len: usize, buf36: *mut u8) -> i32;
    fn pg_hashenc_digest(ty: i32, data: *const u8, len: usize, dest: *mut u8, destlen: usize) -> i32;
    fn pg_hashenc_digest_split(ty: i32, data: *const u8, len: usize, split: usize, dest: *mut u8, destlen: usize) -> i32;
    fn pg_hashenc_hmac(ty: i32, key: *const u8, keylen: usize, data: *const u8, datalen: usize, dest: *mut u8, destlen: usize) -> i32;
    fn pg_hashenc_scram_salted_password(password: *const core::ffi::c_char, salt: *const u8, saltlen: i32, iterations: i32, out32: *mut u8) -> i32;
    fn pg_hashenc_scram_h(input: *const u8, out32: *mut u8) -> i32;
    fn pg_hashenc_scram_client_key(salted: *const u8, out32: *mut u8) -> i32;
    fn pg_hashenc_scram_server_key(salted: *const u8, out32: *mut u8) -> i32;
    fn pg_hashenc_scram_build_secret(salt: *const u8, saltlen: i32, iterations: i32, password: *const core::ffi::c_char) -> *mut core::ffi::c_char;
    fn pg_hashenc_free(p: *mut core::ffi::c_void);
    fn pg_hashenc_to_ascii(src: *const u8, len: usize, dest: *mut u8, enc: i32) -> i32;
    fn pg_hashenc_valid_encoding(enc: i32) -> i32;
    fn pg_hashenc_ascii_safe_strlcpy(dest: *mut u8, src: *const u8, destsiz: usize);
    fn pg_hashenc_crc32_bytea(data: *const u8, len: usize) -> i64;
    fn pg_hashenc_crc32c_bytea(data: *const u8, len: usize) -> i64;
}

/* pg_cryptohash_type (vendored cryptohash.h, verbatim values) */
const C_MD5: i32 = 0;
const C_SHA1: i32 = 1;
const C_SHA224: i32 = 2;
const C_SHA256: i32 = 3;
const C_SHA384: i32 = 4;
const C_SHA512: i32 = 5;

/// Read a 4-byte-header (uncompressed, untoasted) varlena result datum.
/// Everything these wrappers return is exactly that shape.
unsafe fn varlena_bytes<'a>(d: Datum) -> &'a [u8] {
    let p = d.as_usize() as *const u8;
    let hdr = u32::from_le_bytes(*(p as *const [u8; 4]));
    let total = (hdr >> 2) as usize;
    core::slice::from_raw_parts(p.add(4), total - 4)
}

/// One-varlena-arg strict fc_* call (md5/sha/crc/to_ascii wrappers).
fn call1(
    fc: fn(Option<&mut FmgrInfo>, &mut Fcinfo) -> Result<Datum, Box<PgError>>,
    mcx_holder: &MemoryContext,
    image: &[u8],
) -> Result<Datum, Box<PgError>> {
    let mut f = LocalFcinfo::<1>::new(0);
    // SAFETY: mcx_holder outlives the call.
    unsafe { f.set_result_mcx(mcx_holder.mcx()) };
    f.set_arg(0, Datum::from_usize(image.as_ptr() as usize));
    fc(None, &mut f)
}

/// Build a 4-byte-header varlena image around `data`.
fn image(data: &[u8]) -> Vec<u8> {
    let total = data.len() + 4;
    let mut v = Vec::with_capacity(total);
    v.extend_from_slice(&(((total as u32) << 2)).to_le_bytes());
    v.extend_from_slice(data);
    v
}

fn b64_family(payload: &[u8]) {
    let len = payload.len() as i32;

    // Length helpers: same closed form both sides.
    assert_eq!(
        pg_b64::pg_b64_enc_len(len),
        unsafe { pg_hashenc_b64_enc_len(len) },
        "pg_b64_enc_len DIVERGENCE len={len}"
    );
    assert_eq!(
        pg_b64::pg_b64_dec_len(len),
        unsafe { pg_hashenc_b64_dec_len(len) },
        "pg_b64_dec_len DIVERGENCE len={len}"
    );

    // Encode, exact-size dst.
    let cap = pg_b64::pg_b64_enc_len(len);
    let mut r_dst = vec![0u8; cap as usize];
    let mut c_dst = vec![0u8; cap as usize];
    let rn = pg_b64::pg_b64_encode(payload, len, &mut r_dst, cap);
    let cn = unsafe { pg_hashenc_b64_encode(payload.as_ptr(), len, c_dst.as_mut_ptr(), cap) };
    assert!(
        rn == cn && r_dst == c_dst,
        "pg_b64_encode DIVERGENCE len={len}: rust rc={rn} c rc={cn}"
    );

    // Encode, short dst (error path zeroes dst on both sides).
    if cap > 0 {
        let short = cap - 1;
        let mut r_dst = vec![0xAAu8; short as usize];
        let mut c_dst = vec![0xAAu8; short as usize];
        let rn = pg_b64::pg_b64_encode(payload, len, &mut r_dst, short);
        let cn = unsafe { pg_hashenc_b64_encode(payload.as_ptr(), len, c_dst.as_mut_ptr(), short) };
        assert!(
            rn == cn && r_dst == c_dst,
            "pg_b64_encode short-dst DIVERGENCE len={len}: rust rc={rn} c rc={cn}"
        );
    }

    // Decode the raw payload as base64 text (fuzzer explores invalid forms).
    let dcap = pg_b64::pg_b64_dec_len(len);
    let mut r_dst = vec![0x55u8; dcap.max(0) as usize];
    let mut c_dst = vec![0x55u8; dcap.max(0) as usize];
    let rn = pg_b64::pg_b64_decode(payload, len, &mut r_dst, dcap);
    let cn = unsafe { pg_hashenc_b64_decode(payload.as_ptr(), len, c_dst.as_mut_ptr(), dcap) };
    assert!(
        rn == cn && r_dst == c_dst,
        "pg_b64_decode DIVERGENCE input={payload:?}: rust rc={rn} c rc={cn}"
    );

    // Decode, short dst: the three per-byte overflow arms error + zero dst
    // on both sides (reachable only when dstlen < dec_len).
    if rn > 0 {
        let short = rn - 1;
        let mut r_dst = vec![0xAAu8; short as usize];
        let mut c_dst = vec![0xAAu8; short as usize];
        let rs = pg_b64::pg_b64_decode(payload, len, &mut r_dst, short);
        let cs = unsafe { pg_hashenc_b64_decode(payload.as_ptr(), len, c_dst.as_mut_ptr(), short) };
        assert!(
            rs == cs && rs == -1 && r_dst == c_dst,
            "pg_b64_decode short-dst DIVERGENCE input={payload:?}: rust rc={rs} c rc={cs}"
        );
    }
}

fn md5_family(payload: &[u8]) {
    // Kernel plane.
    let r_hex = pg_md5::pg_md5_hash(payload);
    let mut c_hex = [0u8; 33];
    let rc = unsafe { pg_hashenc_md5_hash(payload.as_ptr(), payload.len(), c_hex.as_mut_ptr()) };
    assert!(rc == 0, "C pg_md5_hash failed (OOM-only arm)");
    assert_eq!(&r_hex[..], &c_hex[..32], "pg_md5_hash DIVERGENCE len={}", payload.len());

    let r_bin = pg_md5::pg_md5_binary(payload);
    let mut c_bin = [0u8; 16];
    let rc = unsafe { pg_hashenc_md5_binary(payload.as_ptr(), payload.len(), c_bin.as_mut_ptr()) };
    assert!(rc == 0 && r_bin == c_bin, "pg_md5_binary DIVERGENCE len={}", payload.len());

    // cryptohash.c MD5 dispatch arm (same verbatim md5.c engine underneath).
    let mut c_dis = [0u8; 16];
    let rc = unsafe { pg_hashenc_digest(C_MD5, payload.as_ptr(), payload.len(), c_dis.as_mut_ptr(), 16) };
    assert!(rc == 0 && c_dis == c_bin, "cryptohash md5 dispatch self-DIVERGENCE");

    // SQL wrapper plane (md5_text / md5_bytea share md5_common).
    let ctx = MemoryContext::new("hashenc");
    let img = image(payload);
    let d = call1(cryptohashfuncs::fc_md5_text, &ctx, &img).expect("md5_text is infallible");
    // SAFETY: result is a fresh 4B-header varlena in ctx.
    let out = unsafe { varlena_bytes(d) };
    assert_eq!(out, &c_hex[..32], "fc_md5_text DIVERGENCE len={}", payload.len());
    let d = call1(cryptohashfuncs::fc_md5_bytea, &ctx, &img).expect("md5_bytea is infallible");
    let out = unsafe { varlena_bytes(d) };
    assert_eq!(out, &c_hex[..32], "fc_md5_bytea DIVERGENCE len={}", payload.len());
}

fn md5_encrypt_family(payload: &[u8]) {
    let Some((&cut, rest)) = payload.split_first() else { return };
    let cut = (cut as usize).min(rest.len());
    let (passwd, salt) = rest.split_at(cut);
    if passwd.contains(&0) {
        return; // C API takes a cstring password
    }
    let r = pg_md5::pg_md5_encrypt(passwd, salt);
    let mut c = [0u8; 36];
    let mut pz = passwd.to_vec();
    pz.push(0);
    let rc = unsafe {
        pg_hashenc_md5_encrypt(pz.as_ptr(), salt.as_ptr(), salt.len(), c.as_mut_ptr())
    };
    assert!(rc == 0 && r[..] == c[..35], "pg_md5_encrypt DIVERGENCE passwd={passwd:?} salt={salt:?}");
}

fn sha_family(sel: u8, payload: &[u8]) {
    // (ctype, rust oneshot, digest len, fc wrapper or None)
    type Fc = fn(Option<&mut FmgrInfo>, &mut Fcinfo) -> Result<Datum, Box<PgError>>;
    let (ctype, r_digest, fc): (i32, Vec<u8>, Option<Fc>) = match sel % 5 {
        0 => (C_SHA1, pg_sha1::sha1(payload).to_vec(), None),
        1 => (C_SHA224, pg_sha2::sha224(payload).to_vec(), Some(cryptohashfuncs::fc_sha224_bytea as Fc)),
        2 => (C_SHA256, pg_sha2::sha256(payload).to_vec(), Some(cryptohashfuncs::fc_sha256_bytea as Fc)),
        3 => (C_SHA384, pg_sha2::sha384(payload).to_vec(), Some(cryptohashfuncs::fc_sha384_bytea as Fc)),
        _ => (C_SHA512, pg_sha2::sha512(payload).to_vec(), Some(cryptohashfuncs::fc_sha512_bytea as Fc)),
    };
    let n = r_digest.len();
    let mut c_digest = vec![0u8; n];
    let rc = unsafe { pg_hashenc_digest(ctype, payload.as_ptr(), payload.len(), c_digest.as_mut_ptr(), n) };
    assert!(rc == 0 && r_digest == c_digest, "sha type={ctype} DIVERGENCE len={}", payload.len());

    // Split-update plane (incremental buffering arms both sides).
    if !payload.is_empty() {
        let split = (payload[0] as usize * 131) % (payload.len() + 1);
        let mut c_split = vec![0u8; n];
        let rc = unsafe {
            pg_hashenc_digest_split(ctype, payload.as_ptr(), payload.len(), split, c_split.as_mut_ptr(), n)
        };
        assert!(rc == 0 && c_split == c_digest, "C split-update self-DIVERGENCE type={ctype}");
        let r_split: Vec<u8> = match ctype {
            C_SHA1 => {
                let mut c = pg_sha1::Sha1::init();
                c.update(&payload[..split]);
                c.update(&payload[split..]);
                c.finish().to_vec()
            }
            C_SHA224 => {
                let mut c = pg_sha2::PgSha256Ctx::init_sha224();
                c.update(&payload[..split]);
                c.update(&payload[split..]);
                c.final_sha224().to_vec()
            }
            C_SHA256 => {
                let mut c = pg_sha2::PgSha256Ctx::init_sha256();
                c.update(&payload[..split]);
                c.update(&payload[split..]);
                c.final_sha256().to_vec()
            }
            C_SHA384 => {
                let mut c = pg_sha2::PgSha512Ctx::init_sha384();
                c.update(&payload[..split]);
                c.update(&payload[split..]);
                c.final_sha384().to_vec()
            }
            _ => {
                let mut c = pg_sha2::PgSha512Ctx::init_sha512();
                c.update(&payload[..split]);
                c.update(&payload[split..]);
                c.final_sha512().to_vec()
            }
        };
        assert_eq!(r_split, r_digest, "rust split-update self-DIVERGENCE type={ctype}");
    }

    // SQL wrapper plane: full bytea image compare (catches any NUL-byte
    // truncation in the digest->varlena copy).
    if let Some(fc) = fc {
        let ctx = MemoryContext::new("hashenc");
        let img = image(payload);
        let d = call1(fc, &ctx, &img).expect("sha wrappers are infallible");
        let out = unsafe { varlena_bytes(d) };
        assert_eq!(out, &c_digest[..], "fc_sha wrapper DIVERGENCE type={ctype} len={}", payload.len());
    }
}

fn hmac_family(payload: &[u8]) {
    let Some((&w, rest)) = payload.split_first() else { return };
    let Some((&cut, rest)) = rest.split_first() else { return };
    let cut = (cut as usize).min(rest.len());
    let (key, msg) = rest.split_at(cut);

    let (ctype, r): (i32, Vec<u8>) = match w % 4 {
        0 => {
            let mut c = pg_hmac::PgHmacCtx::<pg_hmac::Sha224>::init(key);
            c.update(msg);
            (C_SHA224, c.finalize().to_vec())
        }
        1 => {
            let mut c = pg_hmac::PgHmacCtx::<pg_hmac::Sha256>::init(key);
            c.update(msg);
            (C_SHA256, c.finalize().to_vec())
        }
        2 => {
            let mut c = pg_hmac::PgHmacCtx::<pg_hmac::Sha384>::init(key);
            c.update(msg);
            (C_SHA384, c.finalize().to_vec())
        }
        _ => {
            let mut c = pg_hmac::PgHmacCtx::<pg_hmac::Sha512>::init(key);
            c.update(msg);
            (C_SHA512, c.finalize().to_vec())
        }
    };
    let n = r.len();
    let mut c_out = vec![0u8; n];
    let rc = unsafe {
        pg_hashenc_hmac(ctype, key.as_ptr(), key.len(), msg.as_ptr(), msg.len(), c_out.as_mut_ptr(), n)
    };
    assert!(rc == 0 && r == c_out, "hmac type={ctype} DIVERGENCE keylen={} msglen={}", key.len(), msg.len());

    // hmac_sha256 convenience entry (the SCRAM building block).
    if ctype == C_SHA256 {
        assert_eq!(pg_hmac::hmac_sha256(key, msg).to_vec(), c_out, "hmac_sha256 DIVERGENCE");
    }
}

/// scram_salted_password is interruptible in the backend; in-harness the
/// CHECK_FOR_INTERRUPTS seam is a sanctioned no-op (never Err).
fn install_cfi() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    // Tolerate cryptofam (p1-lanef) installing the identical no-op CFI seam
    // first — both lanes' oracles run in one test binary since the p1 union.
    ONCE.call_once(|| {
        let _ = std::panic::catch_unwind(|| {
            postgres_seams::check_for_interrupts::set(|| Ok(()));
        });
    });
}

fn scram_family(payload: &[u8]) {
    install_cfi();
    let Some((&itb, rest)) = payload.split_first() else { return };
    let Some((&cut, rest)) = rest.split_first() else { return };
    let iterations = 1 + (itb as i32 % 32); // fenced: C loop is iteration-count-shaped
    let cut = (cut as usize).min(rest.len());
    let (passwd, salt) = rest.split_at(cut);
    if passwd.contains(&0) {
        return; // carve: C API takes a cstring password (see header)
    }
    let mut pz = passwd.to_vec();
    pz.push(0);

    let r_salted = scram_common::scram_salted_password(passwd, salt, iterations)
        .expect("no interrupts in-harness");
    let mut c_salted = [0u8; 32];
    let rc = unsafe {
        pg_hashenc_scram_salted_password(pz.as_ptr().cast(), salt.as_ptr(), salt.len() as i32, iterations, c_salted.as_mut_ptr())
    };
    assert!(rc == 0 && r_salted == c_salted, "scram_salted_password DIVERGENCE iter={iterations}");

    // Derived keys.
    let mut c_out = [0u8; 32];
    let rc = unsafe { pg_hashenc_scram_h(c_salted.as_ptr(), c_out.as_mut_ptr()) };
    assert!(rc == 0 && scram_common::scram_h(&r_salted) == c_out, "scram_h DIVERGENCE");
    let rc = unsafe { pg_hashenc_scram_client_key(c_salted.as_ptr(), c_out.as_mut_ptr()) };
    assert!(rc == 0 && scram_common::scram_client_key(&r_salted) == c_out, "scram_ClientKey DIVERGENCE");
    let rc = unsafe { pg_hashenc_scram_server_key(c_salted.as_ptr(), c_out.as_mut_ptr()) };
    assert!(rc == 0 && scram_common::scram_server_key(&r_salted) == c_out, "scram_ServerKey DIVERGENCE");

    // Full secret string (base64 + format plane).
    let ctx = MemoryContext::new("hashenc");
    let r_secret = scram_common::scram_build_secret(ctx.mcx(), salt, iterations, passwd)
        .expect("no interrupts in-harness");
    let c_secret = unsafe { pg_hashenc_scram_build_secret(salt.as_ptr(), salt.len() as i32, iterations, pz.as_ptr().cast()) };
    assert!(!c_secret.is_null(), "C scram_build_secret failed (OOM-only arm)");
    // SAFETY: C returns a NUL-terminated malloc'd string.
    let c_str = unsafe { core::ffi::CStr::from_ptr(c_secret.cast()) };
    assert_eq!(r_secret.as_bytes(), c_str.to_bytes(), "scram_build_secret DIVERGENCE iter={iterations}");
    unsafe { pg_hashenc_free(c_secret.cast()) };
}

fn to_ascii_family(payload: &[u8]) {
    let Some((&encb, text)) = payload.split_first() else { return };
    let enc = (encb as i32) % (wchar::_PG_LAST_ENCODING_ + 6) - 3; // probe out-of-range too

    // Kernel plane: pg_to_ascii vs verbatim C (same enc int both sides).
    let mut c_out = vec![0u8; text.len()];
    let c_rc = unsafe { pg_hashenc_to_ascii(text.as_ptr(), text.len(), c_out.as_mut_ptr(), enc) };
    let mut r_out = vec![0u8; text.len()];
    match adt_ascii::pg_to_ascii(text, &mut r_out, enc) {
        Ok(()) => {
            assert!(c_rc == 0, "pg_to_ascii DIVERGENCE enc={enc}: C errored ({c_rc}), Rust Ok");
            assert_eq!(r_out, c_out, "pg_to_ascii DIVERGENCE enc={enc} text={text:?}");
        }
        Err(e) => {
            assert!(
                c_rc == 1 && e.sqlstate == ERRCODE_FEATURE_NOT_SUPPORTED,
                "pg_to_ascii error-plane DIVERGENCE enc={enc}: C rc={c_rc}, Rust sqlstate={:?}",
                e.sqlstate
            );
        }
    }

    // Wrapper plane: fc_to_ascii_enc — PG_VALID_ENCODING gate (UNDEFINED_OBJECT)
    // ahead of the conversion (FEATURE_NOT_SUPPORTED), exactly C's order.
    if text.contains(&0) {
        return; // text datum payloads are NUL-free through the SQL surface
    }
    let ctx = MemoryContext::new("hashenc");
    let img = image(text);
    let mut f = LocalFcinfo::<2>::new(0);
    // SAFETY: ctx outlives the call.
    unsafe { f.set_result_mcx(ctx.mcx()) };
    f.set_arg(0, Datum::from_usize(img.as_ptr() as usize));
    f.set_arg(1, Datum::from_i32(enc));
    match adt_ascii::fc_to_ascii_enc(None, &mut f) {
        Ok(d) => {
            let out = unsafe { varlena_bytes(d) };
            assert!(c_rc == 0 && unsafe { pg_hashenc_valid_encoding(enc) } == 1);
            assert_eq!(out, &c_out[..], "fc_to_ascii_enc DIVERGENCE enc={enc}");
        }
        Err(e) => {
            let c_valid = unsafe { pg_hashenc_valid_encoding(enc) } == 1;
            if !c_valid {
                assert!(e.sqlstate == ERRCODE_UNDEFINED_OBJECT, "fc_to_ascii_enc gate DIVERGENCE enc={enc}");
            } else {
                assert!(c_rc == 1 && e.sqlstate == ERRCODE_FEATURE_NOT_SUPPORTED, "fc_to_ascii_enc DIVERGENCE enc={enc}");
            }
        }
    }

    // fc_to_ascii_encname: drive through names the shipped lookup resolves
    // (see header carve — the name table belongs to encnames).
    let name: &[u8] = match encb % 6 {
        0 => b"LATIN1",
        1 => b"LATIN2",
        2 => b"LATIN9",
        3 => b"WIN1250",
        4 => b"UTF8",
        _ => b"NOT_A_REAL_ENCODING",
    };
    let mut namebuf = [0u8; 64];
    namebuf[..name.len()].copy_from_slice(name);
    let mut f = LocalFcinfo::<2>::new(0);
    // SAFETY: ctx outlives the call.
    unsafe { f.set_result_mcx(ctx.mcx()) };
    f.set_arg(0, Datum::from_usize(img.as_ptr() as usize));
    f.set_arg(1, Datum::from_usize(namebuf.as_ptr() as usize));
    let resolved = mbutils::pg_char_to_encoding(core::str::from_utf8(name).unwrap());
    match adt_ascii::fc_to_ascii_encname(None, &mut f) {
        Ok(d) => {
            let out = unsafe { varlena_bytes(d) };
            let mut c_out = vec![0u8; text.len()];
            let c_rc = unsafe { pg_hashenc_to_ascii(text.as_ptr(), text.len(), c_out.as_mut_ptr(), resolved) };
            assert!(c_rc == 0, "fc_to_ascii_encname verdict DIVERGENCE name={name:?}");
            assert_eq!(out, &c_out[..], "fc_to_ascii_encname DIVERGENCE name={name:?}");
        }
        Err(e) => {
            if resolved < 0 {
                assert!(e.sqlstate == ERRCODE_UNDEFINED_OBJECT, "fc_to_ascii_encname gate DIVERGENCE");
            } else {
                let mut c_out = vec![0u8; text.len()];
                let c_rc = unsafe { pg_hashenc_to_ascii(text.as_ptr(), text.len(), c_out.as_mut_ptr(), resolved) };
                assert!(c_rc == 1 && e.sqlstate == ERRCODE_FEATURE_NOT_SUPPORTED, "fc_to_ascii_encname DIVERGENCE name={name:?}");
            }
        }
    }
}

/// fc_to_ascii_default: GetDatabaseEncoding() is session state (default
/// PG_UTF8 in-harness -> C's FEATURE_NOT_SUPPORTED arm); checked as
/// self-consistency against the enc-parameterized oracle at the same
/// resolved encoding (see module-header carve).
fn to_ascii_default_family(payload: &[u8]) {
    if payload.contains(&0) {
        return;
    }
    let ctx = MemoryContext::new("hashenc");
    let img = image(payload);
    let enc = mbutils::GetDatabaseEncoding();
    let mut c_out = vec![0u8; payload.len()];
    let c_rc = unsafe { pg_hashenc_to_ascii(payload.as_ptr(), payload.len(), c_out.as_mut_ptr(), enc) };
    match call1(adt_ascii::fc_to_ascii_default, &ctx, &img) {
        Ok(d) => {
            let out = unsafe { varlena_bytes(d) };
            assert!(c_rc == 0, "fc_to_ascii_default verdict DIVERGENCE enc={enc}");
            assert_eq!(out, &c_out[..], "fc_to_ascii_default DIVERGENCE enc={enc}");
        }
        Err(e) => {
            assert!(
                c_rc == 1 && e.sqlstate == ERRCODE_FEATURE_NOT_SUPPORTED,
                "fc_to_ascii_default error-plane DIVERGENCE enc={enc}"
            );
        }
    }
}

fn strlcpy_family(payload: &[u8]) {
    let Some((&szb, src)) = payload.split_first() else { return };
    let destsiz = (szb as usize) % 40;
    let mut r_dest = vec![0xAAu8; destsiz.max(1)];
    let mut c_dest = vec![0xAAu8; destsiz.max(1)];
    // Rust API: dest slice length IS destsiz (0 => empty slice).
    let mut src_z = src.to_vec();
    src_z.push(0); // C reads until NUL or destsiz-1 bytes; give it a terminator
    let r_len = destsiz.min(r_dest.len());
    adt_ascii::ascii_safe_strlcpy(&mut r_dest[..r_len], &src_z);
    unsafe { pg_hashenc_ascii_safe_strlcpy(c_dest.as_mut_ptr(), src_z.as_ptr(), destsiz) };
    if destsiz == 0 {
        return; // both sides no-op; buffers were never touched
    }
    assert_eq!(
        &r_dest[..destsiz],
        &c_dest[..destsiz],
        "ascii_safe_strlcpy DIVERGENCE destsiz={destsiz} src={src:?}"
    );
}

fn crc_family(payload: &[u8]) {
    let ctx = MemoryContext::new("hashenc");
    let img = image(payload);
    let d = call1(cryptohashfuncs::fc_crc32_bytea, &ctx, &img).expect("crc32 is infallible");
    let c = unsafe { pg_hashenc_crc32_bytea(payload.as_ptr(), payload.len()) };
    assert_eq!(d.as_i64(), c, "crc32_bytea DIVERGENCE len={}", payload.len());
    let d = call1(cryptohashfuncs::fc_crc32c_bytea, &ctx, &img).expect("crc32c is infallible");
    let c = unsafe { pg_hashenc_crc32c_bytea(payload.as_ptr(), payload.len()) };
    assert_eq!(d.as_i64(), c, "crc32c_bytea DIVERGENCE len={}", payload.len());
}

pub fn hashenc_diff(data: &[u8]) {
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
    let Some((&sel, payload)) = data.split_first() else { return };
    if payload.len() > 4096 {
        return; // keep exec rate up; every arm is length-generic
    }
    match sel % 16 {
        0 | 1 => b64_family(payload),
        2 => md5_family(payload),
        3 => md5_encrypt_family(payload),
        4..=8 => sha_family(sel, payload),
        9 | 10 => hmac_family(payload),
        11 => scram_family(payload),
        12 => to_ascii_family(payload),
        13 => {
            to_ascii_family(payload);
            to_ascii_default_family(payload);
        }
        14 => strlcpy_family(payload),
        _ => crc_family(payload),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// CI replay rail: every committed corpus unit replays clean through the
    /// differential on stable (the banked corpus is the regression suite —
    /// any C/Rust divergence or harness panic fails this test per-commit).
    #[test]
    fn hashenc_corpus_replay() {
        let _serial = crate::c_oracle_serial();
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/hashenc_diff");
        let mut n = 0usize;
        for entry in std::fs::read_dir(dir).expect("committed corpus present") {
            let p = entry.unwrap().path();
            if p.is_file() {
                hashenc_diff(&std::fs::read(&p).unwrap());
                n += 1;
            }
        }
        assert!(n > 500, "corpus unexpectedly small: {n} units");
    }

    /// Deterministic seed sweep: every family, assorted shapes. A failure
    /// here is a real C/Rust divergence (or harness defect) on stable.
    #[test]
    fn hashenc_seeds() {
        let _serial = crate::c_oracle_serial();
        hashenc_diff(b"");
        for sel in 0u8..=48 {
            hashenc_diff(&[sel]);
            hashenc_diff(&[sel, 0]);
            hashenc_diff(&[sel, 3, b'a', b'b', b'c', 0xE9, 0xFF, b'=', b'=']);
            hashenc_diff(&[sel, 200, 1, 2, 3]);
            let mut long = vec![sel, 65];
            long.extend((0..300u32).map(|i| (i % 251) as u8));
            hashenc_diff(&long);
        }
        // b64 canonical vectors through the decode arm.
        hashenc_diff(b"\x01Zm9vYmFy");
        hashenc_diff(b"\x01Zm=9");
        hashenc_diff(b"\x01Z g==");
        // hmac over-block key (key-shrink arm): cut byte > blocklen.
        let mut v = vec![9u8, 2, 200];
        v.extend(vec![0xAAu8; 220]);
        hashenc_diff(&v);
        // scram RFC 5802-ish shape.
        let mut v = vec![11u8, 8, 6];
        v.extend(b"pencil");
        v.extend(b"saltsalt");
        hashenc_diff(&v);
        // to_ascii all four supported encodings + invalid codes.
        for encb in 0u8..=60 {
            let mut v = vec![12u8, encb];
            v.extend((120u8..=255).step_by(3));
            hashenc_diff(&v);
        }
        // crc
        hashenc_diff(b"\x0fhello world");
    }
}
