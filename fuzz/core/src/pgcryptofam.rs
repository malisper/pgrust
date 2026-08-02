//! pgcryptofam — FFI surface of the C differential-fuzz ORACLE for
//! contrib/pgcrypto's crypt()/gen_salt()/armor family (lane
//! p1-pgcryptofam).
//!
//! This module holds ONLY the `extern "C"` declarations for the
//! `pg_diff_pgcryptofam_*` entries (csrc/pgcryptofam/, verbatim
//! PostgreSQL 18.3 @ 62d6c7d3df) and thin safe wrappers. NO comparison
//! logic lives here — the differential driver is a separate step.
//!
//! Oracle observable planes surfaced per call via [`PgcryptofamStatus`]:
//!   - value bytes written to the caller buffer (return >= 0),
//!   - ERROR-VERDICT (`ok` == 0 => the C side raised ereport(>=ERROR)),
//!   - ERRCODE (`sqlstate`, PG MAKE_SQLSTATE-encoded int),
//!   - NOTICE plane (`notice_count` / `notice_sqlstate` /
//!     `elevel_of_last_notice` / `notice_text`): crypt-sha.c's
//!     rounds-clamp NOTICE is compared behavior, not log noise,
//!   - message text (`msg`) for triage only.
//!
//! COST BOUNDING: callers MUST consult [`cost_probe`] before running
//! `crypt` on fuzz-chosen settings — bf cost 31 is 2^31 blowfish key
//! schedules and sha rounds go to 999,999,999; the C side executes
//! whatever it is handed (CHECK_FOR_INTERRUPTS is a no-op in the
//! harness).

#![allow(clippy::too_many_arguments)]

use std::os::raw::{c_char, c_int, c_ulong};

pub const PGCRYPTOFAM_MSG_CAP: usize = 512;

/// Mirrors `PgcryptofamStatus` in csrc/pgcryptofam/pgcryptofam_shim.h.
#[repr(C)]
pub struct PgcryptofamStatus {
    pub ok: i32,
    pub sqlstate: i32,
    pub error_elevel: i32,
    pub notice_count: i32,
    pub notice_sqlstate: i32,
    pub elevel_of_last_notice: i32,
    pub notice_text: [u8; PGCRYPTOFAM_MSG_CAP],
    pub msg: [u8; PGCRYPTOFAM_MSG_CAP],
}

impl Default for PgcryptofamStatus {
    fn default() -> Self {
        PgcryptofamStatus {
            ok: 0,
            sqlstate: 0,
            error_elevel: 0,
            notice_count: 0,
            notice_sqlstate: 0,
            elevel_of_last_notice: 0,
            notice_text: [0; PGCRYPTOFAM_MSG_CAP],
            msg: [0; PGCRYPTOFAM_MSG_CAP],
        }
    }
}

impl std::fmt::Debug for PgcryptofamStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgcryptofamStatus")
            .field("ok", &self.ok)
            .field("sqlstate", &self.sqlstate)
            .field("error_elevel", &self.error_elevel)
            .field("notice_count", &self.notice_count)
            .field("notice_sqlstate", &self.notice_sqlstate)
            .field("elevel_of_last_notice", &self.elevel_of_last_notice)
            .field("notice_text", &self.notice_str())
            .field("msg", &self.msg_str())
            .finish()
    }
}

impl PgcryptofamStatus {
    fn cstr(bytes: &[u8]) -> &str {
        let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
        std::str::from_utf8(&bytes[..end]).unwrap_or("<non-utf8 oracle message>")
    }

    /// errmsg text of the raised error (triage plane).
    pub fn msg_str(&self) -> &str {
        Self::cstr(&self.msg)
    }

    /// errmsg text of the last NOTICE/WARNING (compared plane for
    /// crypt-sha's rounds clamp).
    pub fn notice_str(&self) -> &str {
        Self::cstr(&self.notice_text)
    }
}

/// `out_kind` values reported by [`cost_probe`] (must match the
/// PGCRYPTOFAM_KIND_* defines in pg_diff_pgcryptofam.c).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(i32)]
pub enum PgcryptofamKind {
    Des = 0,
    Xdes = 1,
    Md5 = 2,
    Bf = 3,
    Sha256 = 4,
    Sha512 = 5,
    /// "$2$" prefix: px_crypt's dispatch row with a NULL crypt fn
    /// (`crypt()` raises "crypt(3) returned NULL").
    None = 6,
}

extern "C" {
    fn pg_diff_pgcryptofam_crypt(
        pw: *const u8,
        pwlen: usize,
        setting: *const u8,
        settinglen: usize,
        out: *mut u8,
        outcap: usize,
        st: *mut PgcryptofamStatus,
    ) -> i64;
    fn pg_diff_pgcryptofam_gen_salt(
        algo: *const u8,
        algolen: usize,
        rounds: i32,
        entropy: *const u8,
        entropylen: usize,
        out: *mut u8,
        outcap: usize,
        st: *mut PgcryptofamStatus,
    ) -> i64;
    fn pg_diff_pgcryptofam_armor(
        data: *const u8,
        datalen: usize,
        keys: *const *const u8,
        keylens: *const usize,
        values: *const *const u8,
        vallens: *const usize,
        nheaders: i32,
        out: *mut u8,
        outcap: usize,
        st: *mut PgcryptofamStatus,
    ) -> i64;
    fn pg_diff_pgcryptofam_dearmor(
        text: *const u8,
        textlen: usize,
        out: *mut u8,
        outcap: usize,
        st: *mut PgcryptofamStatus,
    ) -> i64;
    fn pg_diff_pgcryptofam_armor_headers(
        text: *const u8,
        textlen: usize,
        out: *mut u8,
        outcap: usize,
        nheaders: *mut i32,
        st: *mut PgcryptofamStatus,
    ) -> i64;
    fn pg_diff_pgcryptofam_cost_probe(
        setting: *const u8,
        settinglen: usize,
        out_kind: *mut i32,
        out_cost: *mut i64,
    ) -> i32;
    fn pg_diff_pgcryptofam_to64(s: *mut c_char, v: c_ulong, n: c_int);
    fn pg_diff_pgcryptofam_ascii_to_bin(ch: c_char) -> c_int;
    fn pg_diff_pgcryptofam_bf_encode(dst: *mut c_char, src: *const u32, size: c_int);
    fn pg_diff_pgcryptofam_bf_decode(dst: *mut u32, src: *const c_char, size: c_int) -> c_int;
    fn pg_diff_pgcryptofam_xdes_count_encode(count: c_ulong, out: *mut c_char);
}

/// Result of one oracle call: `Ok(n)` = n value bytes written into the
/// caller buffer; `Err(status)` = the C side raised (ERROR-VERDICT plane;
/// inspect `sqlstate`). A C-side `-2` (output capacity too small) is a
/// harness sizing bug and panics.
pub type PgcryptofamResult = Result<usize, Box<PgcryptofamStatus>>;

fn finish(ret: i64, st: Box<PgcryptofamStatus>, what: &str) -> PgcryptofamResult {
    match ret {
        n if n >= 0 => {
            debug_assert_eq!(st.ok, 1);
            Ok(n as usize)
        }
        -1 => {
            debug_assert_eq!(st.ok, 0);
            Err(st)
        }
        -2 => panic!("pgcryptofam oracle: {what}: output buffer too small (harness sizing bug)"),
        other => panic!("pgcryptofam oracle: {what}: unexpected return {other}"),
    }
}

/// C `crypt(password, setting)` — the SQL crypt() surface (px_crypt over
/// the verbatim engines; NULL becomes the wrapper's 39000 ereport).
/// CALLER MUST cost-bound `setting` via [`cost_probe`] first.
pub fn c_crypt(pw: &[u8], setting: &[u8], out: &mut [u8]) -> PgcryptofamResult {
    let mut st: Box<PgcryptofamStatus> = Box::default();
    let ret = unsafe {
        pg_diff_pgcryptofam_crypt(
            pw.as_ptr(),
            pw.len(),
            setting.as_ptr(),
            setting.len(),
            out.as_mut_ptr(),
            out.len(),
            &mut *st,
        )
    };
    finish(ret, st, "crypt")
}

/// C `gen_salt(algo[, rounds])` with injectable entropy (`rounds == 0`
/// models the 1-argument form). The C generator draws from `entropy`
/// front-to-back; when it runs dry px_gen_salt reports PXE_NO_RANDOM.
/// NOTE (driver contract): C and Rust consume DIFFERENT NUMBERS of random
/// bytes for the same algorithm, so even identical entropy buffers do not
/// align the streams — compare error verdict + sqlstate + length +
/// deterministic prefix + alphabet membership only.
pub fn c_gen_salt(algo: &[u8], rounds: i32, entropy: &[u8], out: &mut [u8]) -> PgcryptofamResult {
    let mut st: Box<PgcryptofamStatus> = Box::default();
    let ret = unsafe {
        pg_diff_pgcryptofam_gen_salt(
            algo.as_ptr(),
            algo.len(),
            rounds,
            entropy.as_ptr(),
            entropy.len(),
            out.as_mut_ptr(),
            out.len(),
            &mut *st,
        )
    };
    finish(ret, st, "gen_salt")
}

/// C `armor(data, keys, values)` -> pgp_armor_encode. Header key/value
/// SQL-array validation (parse_key_value_arrays) is the driver's plane.
pub fn c_armor(
    data: &[u8],
    headers: &[(&[u8], &[u8])],
    out: &mut [u8],
) -> PgcryptofamResult {
    let keys: Vec<*const u8> = headers.iter().map(|(k, _)| k.as_ptr()).collect();
    let keylens: Vec<usize> = headers.iter().map(|(k, _)| k.len()).collect();
    let vals: Vec<*const u8> = headers.iter().map(|(_, v)| v.as_ptr()).collect();
    let vallens: Vec<usize> = headers.iter().map(|(_, v)| v.len()).collect();
    let mut st: Box<PgcryptofamStatus> = Box::default();
    let ret = unsafe {
        pg_diff_pgcryptofam_armor(
            data.as_ptr(),
            data.len(),
            keys.as_ptr(),
            keylens.as_ptr(),
            vals.as_ptr(),
            vallens.as_ptr(),
            headers.len() as i32,
            out.as_mut_ptr(),
            out.len(),
            &mut *st,
        )
    };
    finish(ret, st, "armor")
}

/// C `dearmor(text)` -> pgp_armor_decode (+ the pg_dearmor wrapper's
/// px_THROW_ERROR translation).
pub fn c_dearmor(text: &[u8], out: &mut [u8]) -> PgcryptofamResult {
    let mut st: Box<PgcryptofamStatus> = Box::default();
    let ret = unsafe {
        pg_diff_pgcryptofam_dearmor(
            text.as_ptr(),
            text.len(),
            out.as_mut_ptr(),
            out.len(),
            &mut *st,
        )
    };
    finish(ret, st, "dearmor")
}

/// C `pgp_armor_headers(text)` -> pgp_extract_armor_headers. On success
/// returns the (key, value) pairs decoded from the oracle's
/// key\0value\0... framing.
pub fn c_armor_headers(text: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Box<PgcryptofamStatus>> {
    // worst case every input byte lands in a header copy, plus NULs
    let mut out = vec![0u8; text.len() * 2 + 64];
    let mut nheaders: i32 = 0;
    let mut st: Box<PgcryptofamStatus> = Box::default();
    let ret = unsafe {
        pg_diff_pgcryptofam_armor_headers(
            text.as_ptr(),
            text.len(),
            out.as_mut_ptr(),
            out.len(),
            &mut nheaders,
            &mut *st,
        )
    };
    let used = finish(ret, st, "armor_headers")?;
    let mut pairs = Vec::with_capacity(nheaders as usize);
    let mut rest = &out[..used];
    for _ in 0..nheaders {
        let kend = rest.iter().position(|&b| b == 0).expect("oracle framing");
        let key = rest[..kend].to_vec();
        rest = &rest[kend + 1..];
        let vend = rest.iter().position(|&b| b == 0).expect("oracle framing");
        let val = rest[..vend].to_vec();
        rest = &rest[vend + 1..];
        pairs.push((key, val));
    }
    Ok(pairs)
}

/// HARNESS FACILITY: parse `setting` the way the vendored preambles do and
/// report (algorithm, main-loop iteration count) WITHOUT running any crypt
/// work. `cost == 0` means the engine errors (or returns NULL) before its
/// expensive loop. Drivers refuse settings above their cost threshold.
pub fn cost_probe(setting: &[u8]) -> (PgcryptofamKind, i64) {
    let mut kind: i32 = 0;
    let mut cost: i64 = 0;
    let rc = unsafe {
        pg_diff_pgcryptofam_cost_probe(setting.as_ptr(), setting.len(), &mut kind, &mut cost)
    };
    assert_eq!(rc, 0, "cost_probe cannot fail");
    let kind = match kind {
        0 => PgcryptofamKind::Des,
        1 => PgcryptofamKind::Xdes,
        2 => PgcryptofamKind::Md5,
        3 => PgcryptofamKind::Bf,
        4 => PgcryptofamKind::Sha256,
        5 => PgcryptofamKind::Sha512,
        6 => PgcryptofamKind::None,
        other => panic!("cost_probe: unknown kind {other}"),
    };
    (kind, cost)
}

/// Exhaustive-diff helper: crypt-md5.c's file-static `_crypt_to64` —
/// writes `n` itoa64 chars of `v`, low bits first.
pub fn c_to64(v: u64, n: usize) -> Vec<u8> {
    assert!(n <= 11, "to64 emits at most ceil(64/6) chars");
    let mut buf = vec![0i8; n];
    unsafe { pg_diff_pgcryptofam_to64(buf.as_mut_ptr(), v as c_ulong, n as c_int) };
    buf.into_iter().map(|b| b as u8).collect()
}

/// Exhaustive-diff helper: crypt-des.c's file-static `ascii_to_bin`.
pub fn c_ascii_to_bin(ch: u8) -> i32 {
    unsafe { pg_diff_pgcryptofam_ascii_to_bin(ch as c_char) }
}

/// Exhaustive-diff helper: crypt-blowfish.c's file-static `BF_encode`
/// (bcrypt radix-64 encode of `src` bytes; `size` counts BYTES).
pub fn c_bf_encode(src: &[u8], size: usize) -> Vec<u8> {
    assert!(size <= src.len() && size <= 24, "bcrypt encodes <= 24 bytes");
    // BF_encode reads size bytes from a BF_word*-typed buffer
    let mut words = [0u32; 6];
    let bytes: &mut [u8; 24] = unsafe { &mut *(words.as_mut_ptr() as *mut [u8; 24]) };
    bytes[..size].copy_from_slice(&src[..size]);
    let outlen = (size * 4).div_ceil(3);
    let mut out = vec![0i8; outlen + 4];
    unsafe { pg_diff_pgcryptofam_bf_encode(out.as_mut_ptr(), words.as_ptr(), size as c_int) };
    out.truncate(outlen);
    out.into_iter().map(|b| b as u8).collect()
}

/// Exhaustive-diff helper: crypt-blowfish.c's file-static `BF_decode`.
/// Returns `Some(size decoded bytes)` or `None` when the C body reports
/// a non-alphabet char (-1). `size` counts OUTPUT bytes.
pub fn c_bf_decode(src: &[u8], size: usize) -> Option<Vec<u8>> {
    assert!(size <= 24, "bcrypt decodes <= 24 bytes (16-byte salt in tree)");
    // BF_decode consumes ceil(size*4/3) input chars; caller supplies them
    assert!(src.len() >= (size * 4).div_ceil(3));
    let mut words = [0u32; 6];
    let rc = unsafe {
        pg_diff_pgcryptofam_bf_decode(
            words.as_mut_ptr(),
            src.as_ptr() as *const c_char,
            size as c_int,
        )
    };
    if rc != 0 {
        return None;
    }
    let bytes: &[u8; 24] = unsafe { &*(words.as_ptr() as *const [u8; 24]) };
    Some(bytes[..size].to_vec())
}

/// Exhaustive-diff helper: the 4-char xdes iteration-count encoding slice
/// of `_crypt_gensalt_extended_rn` (crypt-gensalt.c's `_crypt_itoa64`).
pub fn c_xdes_count_encode(count: u32) -> [u8; 4] {
    let mut out = [0i8; 4];
    unsafe { pg_diff_pgcryptofam_xdes_count_encode(count as c_ulong, out.as_mut_ptr()) };
    out.map(|b| b as u8)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Non-vacuity anchors captured from live postgres:18.3 by a prior
    /// lane; if the oracle disagrees, the ORACLE is wrong.
    #[test]
    fn smoke_crypt_anchors() {
        let mut out = [0u8; 256];
        let n = c_crypt(b"foox", b"$1$", &mut out).expect("md5 crypt");
        assert_eq!(&out[..n], b"$1$$yS/p28w2q0fzatmSApPDM0".as_slice());

        let n = c_crypt(b"foox", b"$7$abc", &mut out).expect("des catch-all");
        assert_eq!(&out[..n], b"$7i/EW2zc2O3M".as_slice());
    }

    #[test]
    fn smoke_shacrypt_notice_plane() {
        let mut out = [0u8; 256];
        // SHA-crypt.txt reference vector: rounds=10 clamps to 1000 with a
        // NOTICE (22003) — the notice is a compared plane.
        let setting = b"$6$rounds=10$roundstoolow";
        let pw = b"the minimum number is still observed";
        let n = c_crypt(pw, setting, &mut out).expect("shacrypt");
        assert_eq!(
            &out[..n],
            b"$6$rounds=1000$roundstoolow$kUMsbe306n21p9R.FRkW3IGn.S9NPN0x50YhH1xhLsPuWGsUSklZt58jaTfF4ZEQpyUNGc0dqbpBYYBaHHrsX.".as_slice()
        );
    }

    #[test]
    fn smoke_error_verdict_plane() {
        let mut out = [0u8; 256];
        let err = c_crypt(b"x", b"$2a$xx$", &mut out).unwrap_err();
        assert_eq!(err.ok, 0);
        // 22023 invalid_parameter_value, MAKE_SQLSTATE-encoded
        assert_eq!(err.sqlstate, 2 + (2 << 6) + (2 << 18) + (3 << 24));
    }

    #[test]
    fn smoke_cost_probe_bounds() {
        assert_eq!(cost_probe(b"$1$ab"), (PgcryptofamKind::Md5, 1000));
        assert_eq!(
            cost_probe(b"$2a$31$abcdefghijklmnopqrstuv"),
            (PgcryptofamKind::Bf, 1i64 << 31)
        );
        assert_eq!(
            cost_probe(b"$6$rounds=999999999$x"),
            (PgcryptofamKind::Sha512, 999_999_999)
        );
        assert_eq!(cost_probe(b"_J9..abcd"), (PgcryptofamKind::Xdes, 725));
        assert_eq!(cost_probe(b"ab"), (PgcryptofamKind::Des, 25));
        assert_eq!(cost_probe(b"$2$x"), (PgcryptofamKind::None, 0));
    }

    #[test]
    fn smoke_armor_roundtrip() {
        let mut armored = [0u8; 4096];
        let data = b"hello pgcrypto";
        let headers: &[(&[u8], &[u8])] = &[(b"Version", b"1.0"), (b"Comment", b"hi")];
        let n = c_armor(data, headers, &mut armored).expect("armor");
        let mut plain = [0u8; 4096];
        let m = c_dearmor(&armored[..n], &mut plain).expect("dearmor");
        assert_eq!(&plain[..m], data.as_slice());
        let pairs = c_armor_headers(&armored[..n]).expect("headers");
        assert_eq!(pairs.len(), 2);
        assert_eq!(pairs[0], (b"Version".to_vec(), b"1.0".to_vec()));
        assert_eq!(pairs[1], (b"Comment".to_vec(), b"hi".to_vec()));
    }

    #[test]
    fn smoke_exhaustive_helpers() {
        assert_eq!(c_xdes_count_encode(725), *b"J9..");
        assert_eq!(c_ascii_to_bin(b'.'), 0);
        assert_eq!(c_ascii_to_bin(b'z'), 63);
        assert_eq!(c_to64(725, 4), b"J9..".to_vec());
        let enc = c_bf_encode(&[0u8; 16], 16);
        assert_eq!(enc.len(), 22);
        let dec = c_bf_decode(&enc, 16).expect("alphabet");
        assert_eq!(dec, vec![0u8; 16]);
        assert_eq!(c_bf_decode(b"!!!!!!!!!!!!!!!!!!!!!!", 16), None);
    }
}
