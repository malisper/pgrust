//! crypt() / gen_salt() password hashing (px-crypt.c router). md5-crypt
//! (`$1$`) is inline over the in-repo pg_md5; des/xdes (crypt-des.c), bcrypt
//! (`$2a$`, crypt-blowfish.c), and sha-crypt (`$5$`/`$6$`, crypt-sha.c) live in
//! submodules. None of these touch OpenSSL (pgcrypto's own C), so live-C
//! byte-identity holds on the CI cluster.
#![allow(deprecated)]

mod bcrypt;
mod cryptdes;
mod desc;
mod shacrypt;

use pg_md5::Md5;
use pg_strong_random::pg_strong_random;

const MD5_SIZE: usize = 16;
const ITOA64: &[u8; 64] = b"./0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

pub enum CryptError {
    Unsupported(&'static str),
    Message(String),
    // A raised interrupt (query cancel / die) surfaced by CHECK_FOR_INTERRUPTS
    // inside the bcrypt/sha-crypt cost loops, or a C-parity ereport with its
    // own SQLSTATE; rethrown untouched.
    Pg(Box<types_error::PgError>),
}

impl From<String> for CryptError {
    fn from(m: String) -> Self {
        CryptError::Message(m)
    }
}

fn random_salt_chars(n: usize) -> Result<Vec<u8>, String> {
    let mut raw = vec![0u8; n];
    if !pg_strong_random(&mut raw) {
        return Err("Failed to generate random number".to_string());
    }
    Ok(raw.iter().map(|&b| ITOA64[(b & 0x3f) as usize]).collect())
}

pub fn gen_salt(salt_type: &str, rounds: i32) -> Result<String, CryptError> {
    let lower = salt_type.to_ascii_lowercase();
    Ok(match lower.as_str() {
        "des" => String::from_utf8_lossy(&random_salt_chars(2)?).into_owned(),
        "md5" => format!("$1${}", String::from_utf8_lossy(&random_salt_chars(8)?)),
        "xdes" => {
            let n = if rounds == 0 { 7250 } else { rounds };
            let count = (n as u32) | 1;
            let mut enc = [0u8; 4];
            let mut c = count;
            for b in enc.iter_mut() {
                *b = ITOA64[(c & 0x3f) as usize];
                c >>= 6;
            }
            format!(
                "_{}{}",
                String::from_utf8_lossy(&enc),
                String::from_utf8_lossy(&random_salt_chars(4)?)
            )
        }
        "bf" => {
            let r = if rounds == 0 { 6 } else { rounds };
            if !(4..=31).contains(&r) {
                return Err("gen_salt: Incorrect number of rounds".to_string().into());
            }
            let mut raw = [0u8; 16];
            if !pg_strong_random(&mut raw) {
                return Err("Failed to generate random number".to_string().into());
            }
            format!("$2a${r:02}${}", bcrypt::encode_salt64(&raw))
        }
        "sha256crypt" | "sha512crypt" => {
            let r = if rounds == 0 { 5000 } else { rounds };
            if !(1000..=999_999_999).contains(&r) {
                return Err("gen_salt: Incorrect number of rounds".to_string().into());
            }
            let salt = random_salt_chars(16)?;
            let magic = if lower == "sha256crypt" { '5' } else { '6' };
            format!("${magic}$rounds={r}${}", String::from_utf8_lossy(&salt))
        }
        _ => return Err("gen_salt: Unknown salt algorithm".to_string().into()),
    })
}

pub fn crypt(password: &str, salt: &str) -> Result<String, CryptError> {
    let s = salt.as_bytes();
    if s.starts_with(b"$1$") {
        crypt_md5(password.as_bytes(), s).map_err(CryptError::Message)
    } else if s.starts_with(b"$5$") || s.starts_with(b"$6$") {
        shacrypt::crypt_sha(password, salt)
    } else if s.starts_with(b"$2a$") || s.starts_with(b"$2x$") || s.starts_with(b"$2b$") {
        bcrypt::crypt_bf(password.as_bytes(), s)
    } else if s.first() == Some(&b'_') {
        desc::crypt_xdes(password.as_bytes(), s).map_err(CryptError::Message)
    } else {
        desc::crypt_des(password.as_bytes(), s).map_err(CryptError::Message)
    }
}

fn to64(out: &mut Vec<u8>, mut v: u32, n: usize) {
    for _ in 0..n {
        out.push(ITOA64[(v & 0x3f) as usize]);
        v >>= 6;
    }
}

fn crypt_md5(pw: &[u8], salt: &[u8]) -> Result<String, String> {
    const MAGIC: &[u8] = b"$1$";
    let after = &salt[MAGIC.len()..];
    let mut sl = 0usize;
    while sl < after.len() && sl < 8 && after[sl] != b'$' {
        sl += 1;
    }
    let salt_bytes = &after[..sl];

    let mut alt_ctx = Md5::new();
    alt_ctx.update(pw);
    alt_ctx.update(salt_bytes);
    alt_ctx.update(pw);
    let alt = alt_ctx.finish();

    let mut ctx = Md5::new();
    ctx.update(pw);
    ctx.update(MAGIC);
    ctx.update(salt_bytes);
    let mut pl = pw.len();
    while pl > 0 {
        let take = pl.min(MD5_SIZE);
        ctx.update(&alt[..take]);
        pl -= take;
    }
    let mut i = pw.len();
    while i != 0 {
        if i & 1 != 0 {
            ctx.update(&[0u8]);
        } else {
            ctx.update(&pw[..1]);
        }
        i >>= 1;
    }
    let mut digest = ctx.finish();

    for r in 0..1000usize {
        let mut c = Md5::new();
        if r & 1 != 0 {
            c.update(pw);
        } else {
            c.update(&digest);
        }
        if r % 3 != 0 {
            c.update(salt_bytes);
        }
        if r % 7 != 0 {
            c.update(pw);
        }
        if r & 1 != 0 {
            c.update(&digest);
        } else {
            c.update(pw);
        }
        digest = c.finish();
    }

    let d = &digest;
    let mut enc = Vec::with_capacity(22);
    to64(&mut enc, ((d[0] as u32) << 16) | ((d[6] as u32) << 8) | (d[12] as u32), 4);
    to64(&mut enc, ((d[1] as u32) << 16) | ((d[7] as u32) << 8) | (d[13] as u32), 4);
    to64(&mut enc, ((d[2] as u32) << 16) | ((d[8] as u32) << 8) | (d[14] as u32), 4);
    to64(&mut enc, ((d[3] as u32) << 16) | ((d[9] as u32) << 8) | (d[15] as u32), 4);
    to64(&mut enc, ((d[4] as u32) << 16) | ((d[10] as u32) << 8) | (d[5] as u32), 4);
    to64(&mut enc, d[11] as u32, 2);

    Ok(format!(
        "$1${}${}",
        String::from_utf8_lossy(salt_bytes),
        String::from_utf8_lossy(&enc)
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::cell::Cell;
    use std::sync::Once;

    // CHECK_FOR_INTERRUPTS test double: a per-thread budget of Ok() calls,
    // then a query-cancel error — plus a call counter so tests can assert
    // exactly how many rounds actually ran. Seams are set-once per process,
    // so the installed fn reads thread-locals the tests configure.
    thread_local! {
        static CFI_BUDGET: Cell<u64> = const { Cell::new(u64::MAX) };
        static CFI_CALLS: Cell<u64> = const { Cell::new(0) };
    }
    static CFI_INIT: Once = Once::new();

    fn test_cfi() -> types_error::PgResult<()> {
        CFI_CALLS.with(|c| c.set(c.get() + 1));
        CFI_BUDGET.with(|b| {
            let v = b.get();
            if v == 0 {
                Err(types_error::PgError::error("canceling statement due to user request")
                    .with_sqlstate(types_error::ERRCODE_QUERY_CANCELED)
                    .into())
            } else {
                b.set(v - 1);
                Ok(())
            }
        })
    }

    fn arm_cfi(budget: u64) {
        CFI_INIT.call_once(|| postgres_seams::check_for_interrupts::set(test_cfi));
        CFI_BUDGET.with(|b| b.set(budget));
        CFI_CALLS.with(|c| c.set(0));
    }

    fn cfi_calls() -> u64 {
        CFI_CALLS.with(|c| c.get())
    }

    fn ok(r: Result<String, CryptError>) -> String {
        match r {
            Ok(s) => s,
            Err(CryptError::Message(m)) => panic!("crypt errored: {m}"),
            Err(CryptError::Unsupported(m)) => panic!("crypt unsupported: {m}"),
            Err(CryptError::Pg(e)) => panic!("crypt raised: {}", e.message),
        }
    }

    // Oracle constants EXECUTED against stock PostgreSQL 18.3 (pg-stock183,
    // 2026-08-01): crypt('pw', '$5$rounds=<N>$abcdefgh').
    const C_ORACLE_CLAMPED_MIN: &str =
        "$5$rounds=1000$abcdefgh$ggW8Ynp0m1BgJIXoc8pRCAiEP.uCk5kdN89CuGUnta.";
    const C_ORACLE_7000: &str =
        "$5$rounds=7000$abcdefgh$wl4DQWovoaIdxuO70qEzKqNLALNFLomxAyP1MAId67D";

    // D12: C's strtoint truncates strtol's long to int and IGNORES errno
    // (src/common/string.c), so out-of-range rounds wrap negative, fail the
    // < MIN check, and clamp to 1000 — never a 999,999,999-round run.
    #[test]
    fn shacrypt_rounds_out_of_range_clamps_like_c() {
        arm_cfi(u64::MAX);
        for r in [
            "2147483648",           // wraps to -2147483648 (C NOTICE prints it)
            "4294967296",           // wraps to 0
            "99999999999999999999", // strtol saturates LONG_MAX, truncates to -1
            "-5",
            "",  // strtol converts nothing, endp at '$': srounds = 0
            "0",
            "999",
        ] {
            arm_cfi(u64::MAX);
            let h = ok(crypt("pw", &format!("$5$rounds={r}$abcdefgh")));
            assert_eq!(h, C_ORACLE_CLAMPED_MIN, "rounds={r}");
            // The rounds ACTUALLY RUN equal C's clamped 1000 (one
            // CHECK_FOR_INTERRUPTS per round).
            assert_eq!(cfi_calls(), 1000, "rounds={r}");
        }
    }

    #[test]
    fn shacrypt_rounds_strtol_sign_and_space_like_c() {
        for r in ["7000", "+7000", " 7000"] {
            arm_cfi(u64::MAX);
            let h = ok(crypt("pw", &format!("$5$rounds={r}$abcdefgh")));
            assert_eq!(h, C_ORACLE_7000, "rounds={r}");
            assert_eq!(cfi_calls(), 7000, "rounds={r}");
        }
    }

    // C: 42601 "could not parse salt options" when the char after the
    // number is not '$' (executed against 18.3: rounds=abc, rounds=7000abc).
    #[test]
    fn shacrypt_rounds_garbage_errors_like_c() {
        arm_cfi(u64::MAX);
        for r in ["abc", "7000abc", "-$x"] {
            match crypt("pw", &format!("$5$rounds={r}$abcdefgh")) {
                Err(CryptError::Pg(e)) => {
                    assert_eq!(e.message, "could not parse salt options", "rounds={r}");
                    assert_eq!(e.sqlstate, types_error::ERRCODE_SYNTAX_ERROR, "rounds={r}");
                }
                other => panic!(
                    "rounds={r}: expected 42601 error, got {:?}",
                    other.map_err(|_| "err")
                ),
            }
        }
    }

    // D19: the sha-crypt rounds loop honors CHECK_FOR_INTERRUPTS — a raised
    // cancel aborts the loop instead of grinding out the remaining rounds.
    // Uses the max-clamp path so the DoS shape itself is the witness, with
    // a budget that keeps the test bounded.
    #[test]
    fn shacrypt_loop_is_cancellable() {
        arm_cfi(5);
        match crypt("pw", "$6$rounds=1000000000$abcdefgh") {
            Err(CryptError::Pg(e)) => {
                assert_eq!(e.sqlstate, types_error::ERRCODE_QUERY_CANCELED);
            }
            Ok(_) => panic!("999,999,999-round crypt completed: interrupts not honored"),
            Err(_) => panic!("unexpected error kind"),
        }
        assert_eq!(cfi_calls(), 6); // 5 Ok rounds + the cancelling call
    }

    // D19: same for the bcrypt 2^cost loop.
    #[test]
    fn bcrypt_loop_is_cancellable() {
        arm_cfi(5);
        match crypt("pw", "$2a$14$......................") {
            Err(CryptError::Pg(e)) => {
                assert_eq!(e.sqlstate, types_error::ERRCODE_QUERY_CANCELED);
            }
            Ok(_) => panic!("2^14-round bcrypt completed: interrupts not honored"),
            Err(_) => panic!("unexpected error kind"),
        }
        assert_eq!(cfi_calls(), 6);
    }

    #[test]
    fn md5_crypt_shape_and_roundtrip() {
        arm_cfi(u64::MAX);
        let h = crypt("foox", "$1$Szzz0yzz").map_err(|_| ()).unwrap();
        assert!(h.starts_with("$1$Szzz0yzz$"));
        assert_eq!(h.len(), "$1$Szzz0yzz$".len() + 22);
        assert_eq!(crypt("foox", &h).map_err(|_| ()).unwrap(), h);
    }

    // Traditional/xdes DES known vectors (crypt-des.c), incl. adversarial salt.
    #[test]
    fn des_known_vectors() {
        assert_eq!(crypt("foob", "rl").map_err(|_| ()).unwrap(), "rlK6kmJqyMjZM");
        assert_eq!(
            crypt("password", "_/!!!!!!!").map_err(|_| ()).unwrap(),
            "_/!!!!!!!zqM49hRzxko"
        );
    }

    // bcrypt $2a$ roundtrip: crypt(pw, hash) reproduces the hash.
    #[test]
    fn bcrypt_roundtrip() {
        arm_cfi(u64::MAX);
        let setting = "$2a$06$......................";
        let h = crypt("foox", setting).map_err(|_| ()).unwrap();
        assert!(h.starts_with("$2a$06$"));
        assert_eq!(crypt("foox", &h).map_err(|_| ()).unwrap(), h);
    }

    // sha-crypt $5$/$6$ roundtrip.
    #[test]
    fn shacrypt_roundtrip() {
        arm_cfi(u64::MAX);
        let h5 = crypt("foox", "$5$Szzz0yzz").map_err(|_| ()).unwrap();
        assert!(h5.starts_with("$5$Szzz0yzz$"));
        assert_eq!(crypt("foox", &h5).map_err(|_| ()).unwrap(), h5);
        let h6 = crypt("foox", "$6$Szzz0yzz").map_err(|_| ()).unwrap();
        assert!(h6.starts_with("$6$Szzz0yzz$"));
        assert_eq!(crypt("foox", &h6).map_err(|_| ()).unwrap(), h6);
    }
}
