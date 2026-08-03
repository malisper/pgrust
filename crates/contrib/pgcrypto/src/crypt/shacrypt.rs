//! SHA-256 / SHA-512 crypt (`$5$` / `$6$`) — a NATIVE port of PostgreSQL
//! 18.3's `px_crypt_shacrypt` (contrib/pgcrypto/crypt-sha.c), not a shim.
//!
//! Lane p1-pgcrypto proved the previous 47-line shim over an external crate
//! CANNOT be repaired by pre-validation: crypt-sha.c skips a leading `$` in
//! the salt while building the CLEANED salt (`decoded_salt`), but then feeds
//! digest B (step 6) and digest DS (step 18) from the RAW pointer
//! (`dec_salt_binary`), so C's answer for `$5$$abc` differs from C's own
//! answer for `$5$abc` (verified live, twice, on stock 18.3). That raw-vs-
//! cleaned pointer asymmetry IS the contract and is reproduced structurally
//! here (`salt_raw` vs `salt_clean`).
//!
//! The rounds option is parsed with C's exact `strtoint` semantics
//! (src/common/string.c: `strtol`, truncate long->int, errno IGNORED), so
//! out-of-range values wrap and clamp exactly as C does — e.g. C turns
//! `rounds=2147483648` into `-2147483648`, notices, and runs 1000 rounds.
//! Parsing into a wider type and clamping to the local max turned a 13-byte
//! setting string into a 999,999,999-round DoS (lane p1-pgcrypto, D12).
//!
//! Error identities (message AND SQLSTATE) are what stock 18.3 raises,
//! captured by execution 2026-08-01 (twice, independent containers):
//!   - 22023 "invalid character in salt string: \"<mb char>\""
//!   - 42601 "could not parse salt options"
//!   - XX000 "bogus magic byte found in salt string"           (elog)
//!   - XX000 "invalid rounds option specified in salt string"  (elog)
//!   - NOTICE 22003 on rounds clamp, C's exact text
//!
//! C operates on NUL-terminated strings; Rust `&str` can embed NUL (not
//! reachable from SQL `text`). We reproduce C's strlen/strstr semantics by
//! truncating the setting at the first NUL byte up front.

use super::CryptError;
use types_error::PgError;

const ROUNDS_MIN: i32 = 1000; // PX_SHACRYPT_ROUNDS_MIN
const ROUNDS_MAX: i32 = 999_999_999; // PX_SHACRYPT_ROUNDS_MAX
const ROUNDS_DEFAULT: u32 = 5000; // PX_SHACRYPT_ROUNDS_DEFAULT
const SALT_MAX_LEN: usize = 16; // PX_SHACRYPT_SALT_MAX_LEN

// crypt-sha.c's rounds-clamp diagnostics: a non-throwing client NOTICE
// (18.3 raises it with SQLSTATE 22003, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE).
fn notice(msg: &str) {
    let _ = elog::ereport(types_error::NOTICE)
        .errcode(types_error::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
        .errmsg(msg.to_string())
        .finish(types_error::ErrorLocation { filename: None, lineno: 0, funcname: None });
}

// ereport(ERROR, errcode(ERRCODE_INVALID_PARAMETER_VALUE), ...) — 22023.
fn err_22023(msg: String) -> CryptError {
    CryptError::Pg(
        PgError::error(msg)
            .with_sqlstate(types_error::ERRCODE_INVALID_PARAMETER_VALUE)
            .into(),
    )
}

// elog(ERROR, ...) — no errcode, so XX000 (verified live on 18.3).
fn err_elog(msg: &str) -> CryptError {
    CryptError::Pg(
        PgError::error(msg.to_string())
            .with_sqlstate(types_error::ERRCODE_INTERNAL_ERROR)
            .into(),
    )
}

/// C `strtol(str, &endp, 10)` over bytes: skip isspace, optional sign,
/// base-10 digits saturating to `long` (i64) on overflow. Returns
/// (value, end index); on no conversion the end index is 0 (C sets
/// `*endptr = str`, the pre-whitespace start).
fn strtol10(s: &[u8]) -> (i64, usize) {
    let mut i = 0usize;
    while i < s.len() && matches!(s[i], b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r') {
        i += 1;
    }
    let mut neg = false;
    if i < s.len() && (s[i] == b'+' || s[i] == b'-') {
        neg = s[i] == b'-';
        i += 1;
    }
    let mut val: i64 = 0;
    let mut saturated = false;
    let mut any = false;
    while i < s.len() && s[i].is_ascii_digit() {
        any = true;
        let d = (s[i] - b'0') as i64;
        if !saturated {
            // Accumulate negated (like C impls) so i64::MIN is reachable.
            match val.checked_mul(10).and_then(|v| v.checked_sub(d)) {
                Some(v) => val = v,
                None => saturated = true,
            }
        }
        i += 1;
    }
    if !any {
        return (0, 0);
    }
    if saturated || (!neg && val == i64::MIN) {
        return (if neg { i64::MIN } else { i64::MAX }, i);
    }
    (if neg { val } else { -val }, i)
}

// _crypt_itoa64 (crypt-sha.c:62) — same table as crypt.rs ITOA64.
const ITOA64: &[u8; 64] = super::ITOA64;

/// `pg_mblen`-alike for the error message's `%.*s`: byte length of the
/// (assumed UTF-8) character starting at `s[0]`, clamped to the slice.
fn mb_char(s: &[u8]) -> String {
    let n = match s[0] {
        b if b < 0x80 => 1,
        b if b >= 0xf0 => 4,
        b if b >= 0xe0 => 3,
        b if b >= 0xc0 => 2,
        _ => 1,
    }
    .min(s.len());
    String::from_utf8_lossy(&s[..n]).into_owned()
}

fn contains(hay: &[u8], needle: &[u8]) -> bool {
    hay.windows(needle.len()).any(|w| w == needle)
}

/// crypt-sha.c:207-231's two clamp NOTICEs, returning (text, clamped value)
/// or None when `srounds` is already in range.
///
/// The value C prints is `%d` of the POST-strtoint, PRE-clamp `int` — the
/// TRUNCATED SIGNED value. That is why `rounds=2147483648` notices
/// `rounds=-2147483648`, `rounds=4294967296` notices `rounds=0`, and
/// `rounds=99999999999999999999` notices `rounds=-1`: all three then fail
/// `< MIN` and run 1000 rounds. Formatting the clamped value instead, or a
/// wider type, would silently turn a 13-byte setting string into a
/// 999,999,999-round burn (lane p1-pgcrypto, D12).
fn clamp_rounds(srounds: i32) -> Option<(String, i32)> {
    if srounds > ROUNDS_MAX {
        Some((
            format!(
                "rounds={srounds} exceeds maximum supported value ({ROUNDS_MAX}), using {ROUNDS_MAX} instead"
            ),
            ROUNDS_MAX,
        ))
    } else if srounds < ROUNDS_MIN {
        Some((
            format!(
                "rounds={srounds} is below supported value ({ROUNDS_MIN}), using {ROUNDS_MIN} instead"
            ),
            ROUNDS_MIN,
        ))
    } else {
        None
    }
}

/// pwhash-lineage hash64 encoder, audited byte-exact against C's
/// b64_from_24bit emission order via the transpose tables below.
fn hash64_encode(bs: &[u8]) -> String {
    let ngroups = bs.len().div_ceil(3);
    let mut out = String::with_capacity(ngroups * 4);
    for g in 0..ngroups {
        let mut g_idx = g * 3;
        let mut enc = 0u32;
        for _ in 0..3 {
            let b = (if g_idx < bs.len() { bs[g_idx] } else { 0 }) as u32;
            enc >>= 8;
            enc |= b << 16;
            g_idx += 1;
        }
        for _ in 0..4 {
            out.push(ITOA64[(enc & 0x3f) as usize] as char);
            enc >>= 6;
        }
    }
    match bs.len() % 3 {
        1 => {
            out.pop();
            out.pop();
        }
        2 => {
            out.pop();
        }
        _ => (),
    }
    out
}

// pg_sha2's two context types behind one face for the shared crypt loop.
trait ShaCtx: Sized {
    const DSIZE: usize;
    fn init() -> Self;
    fn update(&mut self, data: &[u8]);
    fn finish(self) -> Vec<u8>;
}

struct Ctx256(pg_sha2::PgSha256Ctx);
impl ShaCtx for Ctx256 {
    const DSIZE: usize = 32;
    fn init() -> Self {
        Ctx256(pg_sha2::PgSha256Ctx::init_sha256())
    }
    fn update(&mut self, data: &[u8]) {
        self.0.update(data);
    }
    fn finish(self) -> Vec<u8> {
        self.0.final_sha256().to_vec()
    }
}

struct Ctx512(pg_sha2::PgSha512Ctx);
impl ShaCtx for Ctx512 {
    const DSIZE: usize = 64;
    fn init() -> Self {
        Ctx512(pg_sha2::PgSha512Ctx::init_sha512())
    }
    fn update(&mut self, data: &[u8]) {
        self.0.update(data);
    }
    fn finish(self) -> Vec<u8> {
        self.0.final_sha512().to_vec()
    }
}

const SHA256_TRANSPOSE: &[u8] = b"\x14\x0a\x00\x0b\x01\x15\x02\x16\x0c\x17\x0d\x03\x0e\x04\x18\x05\
      \x19\x0f\x1a\x10\x06\x11\x07\x1b\x08\x1c\x12\x1d\x13\x09\x1e\x1f";
const SHA512_TRANSPOSE: &[u8] = b"\x2a\x15\x00\x01\x2b\x16\x17\x02\x2c\x2d\x18\x03\x04\x2e\x19\x1a\
      \x05\x2f\x30\x1b\x06\x07\x31\x1c\x1d\x08\x32\x33\x1e\x09\x0a\x34\
      \x1f\x20\x0b\x35\x36\x21\x0c\x0d\x37\x22\x23\x0e\x38\x39\x24\x0f\
      \x10\x3a\x25\x26\x11\x3b\x3c\x27\x12\x13\x3d\x28\x29\x14\x3e\x3f";

/// crypt-sha.c steps 1-21 (Drepper's algorithm), CHECK_FOR_INTERRUPTS per
/// round exactly where C's step-21 loop has it.
///
/// THE QUIRK OF RECORD (D14): `salt_clean` is C's `decoded_salt->data`
/// (leading `$`s skipped); `salt_raw` is C's `dec_salt_binary` — the raw
/// post-options bytes, same LENGTH as the cleaned salt but possibly
/// different CONTENT. C feeds digest A (step 3) and byte sequence S's
/// length from the cleaned salt, but digest B (step 6) and digest DS
/// (step 18) from the raw pointer. Both slices here have identical length
/// (`salt_len`); only their bytes may differ.
fn sha2_crypt<C: ShaCtx>(
    pass: &[u8],
    salt_clean: &[u8],
    salt_raw: &[u8],
    rounds: u32,
    trn_table: &[u8],
) -> Result<Vec<u8>, CryptError> {
    debug_assert_eq!(salt_clean.len(), salt_raw.len());
    let dsize = C::DSIZE;
    let salt_len = salt_clean.len();

    // Steps 4-8: digest B = H(pw || RAW salt || pw).
    let mut dgst_b = C::init();
    dgst_b.update(pass);
    dgst_b.update(salt_raw);
    dgst_b.update(pass);
    let mut hash_b = dgst_b.finish();

    // Steps 1-3: digest A starts with pw || CLEANED salt.
    let mut dgst_a = C::init();
    dgst_a.update(pass);
    dgst_a.update(salt_clean);

    // Steps 9-10.
    let plen = pass.len();
    let mut p = plen;
    while p > 0 {
        dgst_a.update(&hash_b[..p.min(dsize)]);
        if p < dsize {
            break;
        }
        p -= dsize;
    }

    // Step 11.
    p = plen;
    while p > 0 {
        if p & 1 == 0 {
            dgst_a.update(pass);
        } else {
            dgst_a.update(&hash_b[..dsize]);
        }
        p >>= 1;
    }

    // Step 12.
    let mut hash_a = dgst_a.finish();

    // Steps 13-16: byte sequence P.
    let mut dgst_b = C::init();
    for _ in 0..plen {
        dgst_b.update(pass);
    }
    hash_b = dgst_b.finish();
    let mut seq_p = Vec::<u8>::with_capacity(plen.div_ceil(dsize.max(1)) * dsize);
    p = plen;
    while p > 0 {
        seq_p.extend(&hash_b[..p.min(dsize)]);
        if p < dsize {
            break;
        }
        p -= dsize;
    }

    // Steps 17-20: byte sequence S — digest DS over the RAW salt,
    // 16 + A[0] times (crypt-sha.c:455), then the first salt_len bytes.
    let mut dgst_b = C::init();
    for _ in 0..SALT_MAX_LEN + (hash_a[0] as usize) {
        dgst_b.update(salt_raw);
    }
    hash_b = dgst_b.finish();
    let mut seq_s = Vec::<u8>::with_capacity(SALT_MAX_LEN);
    seq_s.extend(&hash_b[..salt_len]);

    // Step 21.
    for r in 0..rounds {
        // C runs CHECK_FOR_INTERRUPTS() at the top of every round so large
        // "rounds" stay cancellable (crypt-sha.c:498). A raised cancel/die
        // propagates out as the error.
        postgres_seams::check_for_interrupts::call().map_err(CryptError::Pg)?;

        let mut dgst_a = C::init();
        if r % 2 == 1 {
            dgst_a.update(&seq_p[..]);
        } else {
            dgst_a.update(&hash_a[..dsize]);
        }
        if r % 3 > 0 {
            dgst_a.update(&seq_s[..]);
        }
        if r % 7 > 0 {
            dgst_a.update(&seq_p[..]);
        }
        if r % 2 == 1 {
            dgst_a.update(&hash_a[..dsize]);
        } else {
            dgst_a.update(&seq_p[..]);
        }
        hash_a = dgst_a.finish();
    }

    let mut out = hash_b;
    for (i, &ti) in trn_table.iter().enumerate() {
        out[i] = hash_a[ti as usize];
    }
    out.truncate(trn_table.len());
    Ok(out)
}

/// Native `px_crypt_shacrypt` (crypt-sha.c:68). The dispatcher only routes
/// `$5$`/`$6$`-prefixed settings here, but the C entry checks are ported for
/// direct callers.
pub fn crypt_sha(pw: &[u8], setting: &[u8]) -> Result<String, CryptError> {
    let full = setting;
    // C sees a NUL-terminated string: strlen/strstr stop at the first NUL.
    // SQL `text` cannot carry NUL; this matters only for direct Rust callers.
    let s = &full[..full.iter().position(|&b| b == 0).unwrap_or(full.len())];

    // crypt-sha.c:137 — strlen(salt) < 3.
    if s.len() < 3 {
        return Err(err_22023("invalid salt".to_string()));
    }
    // crypt-sha.c:146 — magic byte enclosure.
    if s[0] != b'$' || s[2] != b'$' {
        return Err(CryptError::Pg(
            PgError::error("invalid format of salt".to_string())
                .with_sqlstate(types_error::ERRCODE_INVALID_PARAMETER_VALUE)
                .with_hint("magic byte format for shacrypt is either \"$5$\" or \"$6$\"")
                .into(),
        ));
    }
    // crypt-sha.c:158-167 magic match; :273 unknown identifier (elog).
    // C parses a rounds option before raising this, but with salt[0]=='$'
    // the "rounds=" strncmp can never match first — erroring here is C-exact.
    let is_512 = match s[1] {
        b'5' => false,
        b'6' => true,
        c => {
            return Err(err_elog(&format!(
                "unknown crypt identifier \"{}\"",
                // C prints the raw byte via %c.
                c as char
            )))
        }
    };

    // Rounds option (crypt-sha.c:184-230).
    let mut rest: &[u8] = &s[3..];
    let mut rounds: u32 = ROUNDS_DEFAULT;
    let mut rounds_custom = false;
    if rest.starts_with(b"rounds=") {
        let num = &rest[b"rounds=".len()..];
        let (lval, end) = strtol10(num);
        // C strtoint: truncate long -> int; overflow/errno ignored.
        let mut srounds = lval as i32;
        if num.get(end) != Some(&b'$') {
            return Err(CryptError::Pg(
                PgError::error("could not parse salt options".to_string())
                    .with_sqlstate(types_error::ERRCODE_SYNTAX_ERROR)
                    .into(),
            ));
        }
        rest = &num[end + 1..];
        if let Some((msg, clamped)) = clamp_rounds(srounds) {
            notice(&msg);
            srounds = clamped;
        }
        rounds = srounds as u32;
        rounds_custom = true;
    }

    // Salt scan (crypt-sha.c:293-347): walk at most SALT_MAX_LEN bytes.
    // C re-runs whole-remaining-string strstr guards for "$5$"/"$6$"/
    // "rounds=" on EVERY iteration — observably identical to running them
    // once per entered loop body, and like C they never run if the loop
    // body never executes (empty remainder ⇒ empty salt is legal, D13).
    let mut decoded: Vec<u8> = Vec::with_capacity(SALT_MAX_LEN);
    let mut i = 0usize;
    while i < rest.len() && i < SALT_MAX_LEN {
        if contains(rest, b"$5$") || contains(rest, b"$6$") {
            return Err(err_elog("bogus magic byte found in salt string"));
        }
        if contains(rest, b"rounds=") {
            return Err(err_elog("invalid rounds option specified in salt string"));
        }
        let c = rest[i];
        if c != b'$' {
            if ITOA64.contains(&c) {
                decoded.push(c);
            } else {
                return Err(err_22023(format!(
                    "invalid character in salt string: \"{}\"",
                    mb_char(&rest[i..])
                )));
            }
        } else if !decoded.is_empty() {
            // '$' after at least one absorbed byte terminates the salt;
            // anything after is an (ignored) attached password hash.
            break;
        }
        // A '$' with nothing absorbed yet is SKIPPED in the cleaned salt —
        // but the raw pointer below still sees it (D14).
        i += 1;
    }
    let salt_len = decoded.len();
    // C's dec_salt_binary: digest B and DS read salt_len bytes from the RAW
    // post-options string, NOT the cleaned salt (crypt-sha.c:377,456).
    let salt_raw = &rest[..salt_len];

    let raw = if is_512 {
        sha2_crypt::<Ctx512>(pw, &decoded, salt_raw, rounds, SHA512_TRANSPOSE)?
    } else {
        sha2_crypt::<Ctx256>(pw, &decoded, salt_raw, rounds, SHA256_TRANSPOSE)?
    };
    let magic = if is_512 { "$6$" } else { "$5$" };
    let encoded = hash64_encode(&raw);
    // The result string carries the CLEANED salt (C appends decoded_salt).
    let salt_str = core::str::from_utf8(&decoded).expect("itoa64 subset is ASCII");
    Ok(if rounds_custom {
        format!("{magic}rounds={rounds}${salt_str}${encoded}")
    } else {
        format!("{magic}{salt_str}${encoded}")
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// D12's load-bearing detail, previously witnessed NOWHERE: the clamp
    /// NOTICE prints the TRUNCATED SIGNED int32, not the clamped value and
    /// not a wider type. Texts captured from live 18.3 (p1-pgcrypto /
    /// p1-shaport, 2026-08-01); the truncations are C `strtoint`
    /// (src/common/string.c:50 — strtol, cast to int, errno ignored).
    #[test]
    fn clamp_notice_prints_the_truncated_signed_value() {
        // (rounds spelling, strtoint result, notice text)
        for (spelling, want_srounds) in [
            ("2147483648", -2147483648i32),
            ("4294967296", 0),
            ("99999999999999999999", -1),
            ("-1", -1),
            ("-5", -5),
            ("0", 0),
            ("", 0),
            ("999", 999),
        ] {
            let (lval, _) = strtol10(spelling.as_bytes());
            let srounds = lval as i32;
            assert_eq!(srounds, want_srounds, "strtoint({spelling:?})");
            let (msg, clamped) = clamp_rounds(srounds).expect("below MIN => a NOTICE");
            assert_eq!(
                msg,
                format!("rounds={want_srounds} is below supported value (1000), using 1000 instead")
            );
            assert_eq!(clamped, ROUNDS_MIN);
        }
        // Above MAX is only reachable in (MAX, i32::MAX].
        let (msg, clamped) = clamp_rounds(1_000_000_000).expect("above MAX => a NOTICE");
        assert_eq!(
            msg,
            "rounds=1000000000 exceeds maximum supported value (999999999), using 999999999 instead"
        );
        assert_eq!(clamped, ROUNDS_MAX);
        // In range: no NOTICE at all.
        for r in [ROUNDS_MIN, 5000, ROUNDS_MAX] {
            assert!(clamp_rounds(r).is_none(), "rounds={r} is in range");
        }
    }

    /// strtol leniency is part of the contract (D17): leading whitespace is
    /// skipped, a '+'/'-' sign is accepted, an empty digit run converts
    /// nothing and reports the PRE-whitespace start as the end pointer (C
    /// sets `*endptr = str`), and overflow saturates at LONG_MAX/LONG_MIN
    /// before the cast to int.
    #[test]
    fn strtol10_matches_c() {
        assert_eq!(strtol10(b"5000$x"), (5000, 4));
        assert_eq!(strtol10(b" 5000$x"), (5000, 5));
        assert_eq!(strtol10(b"+5000$x"), (5000, 5));
        assert_eq!(strtol10(b"-5000$x"), (-5000, 5));
        assert_eq!(strtol10(b"0005000$x"), (5000, 7));
        assert_eq!(strtol10(b"\t\n\x0b\x0c\r7$"), (7, 6));
        // No conversion: value 0, end index 0 (NOT past the whitespace).
        assert_eq!(strtol10(b"$abc"), (0, 0));
        assert_eq!(strtol10(b"   $abc"), (0, 0));
        assert_eq!(strtol10(b"abc"), (0, 0));
        assert_eq!(strtol10(b"+$"), (0, 0));
        // Saturation, then the int32 truncation C applies on top.
        assert_eq!(strtol10(b"99999999999999999999$").0, i64::MAX);
        assert_eq!(strtol10(b"-99999999999999999999$").0, i64::MIN);
        assert_eq!(strtol10(b"9223372036854775807$").0, i64::MAX);
        assert_eq!(strtol10(b"-9223372036854775808$").0, i64::MIN);
    }
}
