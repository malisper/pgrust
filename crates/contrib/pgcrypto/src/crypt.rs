//! crypt() / gen_salt() password hashing (px-crypt.c router). md5-crypt
//! (`$1$`) is inline over the in-repo pg_md5; des/xdes (crypt-des.c), bcrypt
//! (`$2a$`, crypt-blowfish.c), and sha-crypt (`$5$`/`$6$`, crypt-sha.c) live in
//! submodules. None of these touch OpenSSL (pgcrypto's own C), so live-C
//! byte-identity holds on the fleet.
#![allow(deprecated)]

mod bcrypt;
mod cryptdes;
mod desc;
#[cfg(test)]
mod divergence_witness;
mod shacrypt;

use pg_md5::Md5;
use pg_strong_random::pg_strong_random;

const MD5_SIZE: usize = 16;
const ITOA64: &[u8; 64] = b"./0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

#[derive(Debug)]
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

// px-crypt.h round constants.
const PX_XDES_ROUNDS: i32 = 29 * 25; // 725
const PX_BF_ROUNDS: i32 = 6;
const PX_SHACRYPT_SALT_MAX_LEN: usize = 16;
const PX_SHACRYPT_ROUNDS_DEFAULT: i32 = 5000;
const PX_SHACRYPT_ROUNDS_MIN: i32 = 1000;
const PX_SHACRYPT_ROUNDS_MAX: i32 = 999_999_999;

// px_strerror (px.c:53-55) for the codes px_gen_salt can return; pgcrypto.c
// renders them as errmsg("gen_salt: %s", ...).
const PXE_UNKNOWN_SALT_ALGO: &str = "gen_salt: Unknown salt algorithm";
const PXE_BAD_SALT_ROUNDS: &str = "gen_salt: Incorrect number of rounds";
const PXE_NO_RANDOM: &str = "gen_salt: Failed to generate strong random bits";

/// `_crypt_itoa64[value & 0x3f]` over the low 24 bits of a little-endian
/// 3-byte group — crypt-gensalt.c's shared "pack 3 random bytes into 4 salt
/// chars" step.
fn itoa64_triplet(input: &[u8], out: &mut String) {
    let value = (input[0] as u32) | ((input[1] as u32) << 8) | ((input[2] as u32) << 16);
    for shift in [0, 6, 12, 18] {
        out.push(ITOA64[((value >> shift) & 0x3f) as usize] as char);
    }
}

// crypt-gensalt.c generators. `count` is C's `unsigned long` parameter, which
// px_gen_salt fills from an `int` — so a negative `rounds` arrives as a huge
// value, which is exactly how des/md5 (whose gen_list rows have def_rounds 0
// and are therefore NOT range-checked) come to reject it.
//
// Every generator also guards `output_size`; px_gen_salt always passes
// PX_MAX_SALT_LEN (128), which clears all of them, so those guards have no
// runtime form here.
type GenFn = fn(count: u64, input: &[u8]) -> Option<String>;

/// `_crypt_gensalt_traditional_rn` (crypt-gensalt.c:25).
fn gensalt_traditional(count: u64, input: &[u8]) -> Option<String> {
    if input.len() < 2 || (count != 0 && count != 25) {
        return None;
    }
    Some(
        [ITOA64[(input[0] & 0x3f) as usize], ITOA64[(input[1] & 0x3f) as usize]]
            .iter()
            .map(|&b| b as char)
            .collect(),
    )
}

/// `_crypt_gensalt_extended_rn` (crypt-gensalt.c:42). An EVEN iteration count
/// makes weak DES keys easy to spot in the hash, so upstream REFUSES it rather
/// than repairing it.
fn gensalt_extended(mut count: u64, input: &[u8]) -> Option<String> {
    if input.len() < 3 || (count != 0 && (count > 0xffffff || count & 1 == 0)) {
        return None;
    }
    if count == 0 {
        count = 725;
    }
    let mut out = String::from('_');
    for shift in [0, 6, 12, 18] {
        out.push(ITOA64[((count >> shift) & 0x3f) as usize] as char);
    }
    itoa64_triplet(input, &mut out);
    Some(out)
}

/// `_crypt_gensalt_md5_rn` (crypt-gensalt.c:78). The second triplet is emitted
/// only when the caller supplied >= 6 input bytes; the md5 gen_list row asks
/// for 6, so both are always written.
fn gensalt_md5(count: u64, input: &[u8]) -> Option<String> {
    if input.len() < 3 || (count != 0 && count != 1000) {
        return None;
    }
    let mut out = String::from("$1$");
    itoa64_triplet(input, &mut out);
    if input.len() >= 6 {
        itoa64_triplet(&input[3..], &mut out);
    }
    Some(out)
}

/// `_crypt_gensalt_blowfish_rn` (crypt-gensalt.c:158).
fn gensalt_blowfish(mut count: u64, input: &[u8]) -> Option<String> {
    if input.len() < 16 || (count != 0 && !(4..=31).contains(&count)) {
        return None;
    }
    if count == 0 {
        count = 5;
    }
    let raw: [u8; 16] = input[..16].try_into().ok()?;
    Some(format!(
        "$2a${}{}${}",
        (b'0' + (count / 10) as u8) as char,
        (b'0' + (count % 10) as u8) as char,
        bcrypt::encode_salt64(&raw)
    ))
}

/// `_crypt_gensalt_sha` (crypt-gensalt.c:224), shared by the sha256/sha512
/// generators, which differ only in the magic byte they pre-write.
fn gensalt_sha(magic: char, count: u64, input: &[u8]) -> Option<String> {
    if input.len() != PX_SHACRYPT_SALT_MAX_LEN {
        return None;
    }
    let mut out = format!("${magic}$rounds={count}$");
    for &b in input {
        out.push(ITOA64[(b & 0x3f) as usize] as char);
    }
    Some(out)
}

fn gensalt_sha256(count: u64, input: &[u8]) -> Option<String> {
    gensalt_sha('5', count, input)
}

fn gensalt_sha512(count: u64, input: &[u8]) -> Option<String> {
    gensalt_sha('6', count, input)
}

struct Generator {
    name: &'static str,
    gen: GenFn,
    input_len: usize,
    def_rounds: i32,
    min_rounds: i32,
    max_rounds: i32,
}

/// `gen_list` (px-crypt.c:137-153). `def_rounds == 0` means px_gen_salt does
/// NOT range-check at all for that row — the generator's own `count` guard is
/// the only check (des accepts 0 or 25, md5 accepts 0 or 1000).
static GEN_LIST: &[Generator] = &[
    Generator { name: "des", gen: gensalt_traditional, input_len: 2, def_rounds: 0, min_rounds: 0, max_rounds: 0 },
    Generator { name: "md5", gen: gensalt_md5, input_len: 6, def_rounds: 0, min_rounds: 0, max_rounds: 0 },
    Generator { name: "xdes", gen: gensalt_extended, input_len: 3, def_rounds: PX_XDES_ROUNDS, min_rounds: 1, max_rounds: 0xFFFFFF },
    Generator { name: "bf", gen: gensalt_blowfish, input_len: 16, def_rounds: PX_BF_ROUNDS, min_rounds: 4, max_rounds: 31 },
    Generator {
        name: "sha256crypt",
        gen: gensalt_sha256,
        input_len: PX_SHACRYPT_SALT_MAX_LEN,
        def_rounds: PX_SHACRYPT_ROUNDS_DEFAULT,
        min_rounds: PX_SHACRYPT_ROUNDS_MIN,
        max_rounds: PX_SHACRYPT_ROUNDS_MAX,
    },
    Generator {
        name: "sha512crypt",
        gen: gensalt_sha512,
        input_len: PX_SHACRYPT_SALT_MAX_LEN,
        def_rounds: PX_SHACRYPT_ROUNDS_DEFAULT,
        min_rounds: PX_SHACRYPT_ROUNDS_MIN,
        max_rounds: PX_SHACRYPT_ROUNDS_MAX,
    },
];

/// `px_gen_salt` (px-crypt.c:155).
pub fn gen_salt(salt_type: &str, mut rounds: i32) -> Result<String, CryptError> {
    // C matches with pg_strcasecmp.
    let Some(g) = GEN_LIST.iter().find(|g| g.name.eq_ignore_ascii_case(salt_type)) else {
        return Err(PXE_UNKNOWN_SALT_ALGO.to_string().into());
    };

    if g.def_rounds != 0 {
        if rounds == 0 {
            rounds = g.def_rounds;
        }
        if rounds < g.min_rounds || rounds > g.max_rounds {
            return Err(PXE_BAD_SALT_ROUNDS.to_string().into());
        }
    }

    let mut rbuf = vec![0u8; g.input_len];
    if !pg_strong_random(&mut rbuf) {
        return Err(PXE_NO_RANDOM.to_string().into());
    }

    // C widens the `int` rounds to the generator's `unsigned long` parameter.
    (g.gen)(rounds as i64 as u64, &rbuf).ok_or_else(|| PXE_BAD_SALT_ROUNDS.to_string().into())
}

// px-crypt.c:37-78 run_crypt_des / run_crypt_md5 / run_crypt_bf /
// run_crypt_sha — the four handlers px_crypt_list points at.
//
// D21: the whole crypt cone carries RAW BYTES end to end, as C does.
// C's px_crypt copies setting-prefix bytes VERBATIM into the result
// (crypt-des.c: `output[0..2] = setting[0..2]` / xdes `strlcpy(output,
// setting, 10)`; crypt-md5.c re-emits the raw salt run), so the result is
// not guaranteed UTF-8 even when the setting is (a multibyte char truncated
// at the copy boundary). Password and salt likewise enter as bytes —
// `text` in a non-UTF-8 database can carry arbitrary bytes, and laundering
// them through `from_utf8_lossy` collapsed distinct passwords onto U+FFFD.
// sha-crypt and bcrypt outputs are structurally ASCII (their salts are
// ITOA64/BF64-validated before echo), so crypt_sha's `String` return is
// exact and converts losslessly.
fn run_crypt_des(psw: &[u8], salt: &[u8]) -> Result<Vec<u8>, CryptError> {
    desc::run_crypt_des(psw, salt)
}
fn run_crypt_md5(psw: &[u8], salt: &[u8]) -> Result<Vec<u8>, CryptError> {
    crypt_md5(psw, salt).map_err(CryptError::Message)
}
fn run_crypt_bf(psw: &[u8], salt: &[u8]) -> Result<Vec<u8>, CryptError> {
    bcrypt::crypt_bf(psw, salt)
}
fn run_crypt_sha(psw: &[u8], salt: &[u8]) -> Result<Vec<u8>, CryptError> {
    shacrypt::crypt_sha(psw, salt).map(String::into_bytes)
}

type CryptFn = fn(&[u8], &[u8]) -> Result<Vec<u8>, CryptError>;

/// `px_crypt_list` (px-crypt.c:88-99), in C's exact order. Two rows carry
/// behavior that is easy to lose by hand-writing the dispatch as an if-chain:
///
///   - `{"$2$", 3, NULL}` matches BEFORE the DES catch-all and has a NULL
///     handler, so `px_crypt` returns NULL and pgcrypto.c:234 raises
///     "crypt(3) returned NULL" (SQLSTATE 39000).
///   - there is deliberately no `"$2b$"` row: `$2b$…` fails `strncmp` against
///     `$2a$`/`$2x$` (byte 2) and against `$2$` (byte 2, 'b' vs '$'), so it
///     reaches `{"", 0, run_crypt_des}` and is traditional-DES-hashed with the
///     2-char salt `$2`.
static PX_CRYPT_LIST: &[(&[u8], Option<CryptFn>)] = &[
    (b"$2a$", Some(run_crypt_bf)),
    (b"$2x$", Some(run_crypt_bf)),
    (b"$2$", None),
    (b"$1$", Some(run_crypt_md5)),
    (b"$5$", Some(run_crypt_sha)),
    (b"$6$", Some(run_crypt_sha)),
    (b"_", Some(run_crypt_des)),
    (b"", Some(run_crypt_des)),
];

/// `px_crypt` (px-crypt.c:101). C walks the table until either the zero-length
/// catch-all id or a `strncmp` hit, then returns NULL when the matched row has
/// no handler. D21: password, salt, and result are raw bytes end to end — see
/// the handler-block comment above.
pub fn crypt(password: &[u8], salt: &[u8]) -> Result<Vec<u8>, CryptError> {
    let (_, handler) = PX_CRYPT_LIST
        .iter()
        .find(|(id, _)| id.is_empty() || salt.starts_with(id))
        .expect("px_crypt_list ends with the zero-length catch-all row");
    match handler {
        None => Err(CryptError::Message("crypt(3) returned NULL".to_string())),
        Some(f) => f(password, salt),
    }
}

fn to64(out: &mut Vec<u8>, mut v: u32, n: usize) {
    for _ in 0..n {
        out.push(ITOA64[(v & 0x3f) as usize]);
        v >>= 6;
    }
}

fn crypt_md5(pw: &[u8], salt: &[u8]) -> Result<Vec<u8>, String> {
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

    // C emits MAGIC + the RAW salt bytes + '$' + itoa64 hash; the salt run is
    // echoed verbatim (crypt-md5.c), so the result stays bytes.
    let mut out = Vec::with_capacity(MAGIC.len() + salt_bytes.len() + 1 + enc.len());
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(salt_bytes);
    out.push(b'$');
    out.extend_from_slice(&enc);
    Ok(out)
}

// CHECK_FOR_INTERRUPTS test double, shared by the crypt/bcrypt/shacrypt/des
// test modules: a per-thread budget of Ok() calls, then a query-cancel error —
// plus a call counter so tests can assert exactly how many rounds actually
// ran. Seams are set-once per process, so the installed fn reads
// thread-locals the tests configure. NB: the seam has no default leg and
// PANICS if uninstalled — a crypt test passing WITHOUT arm_cfi() is itself
// proof its path carries no interrupt check.
#[cfg(test)]
pub(crate) mod cfi_test_support {
    use core::cell::Cell;
    use std::sync::Once;

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

    pub(crate) fn arm_cfi(budget: u64) {
        CFI_INIT.call_once(|| postgres_seams::check_for_interrupts::set(test_cfi));
        CFI_BUDGET.with(|b| b.set(budget));
        CFI_CALLS.with(|c| c.set(0));
    }

    pub(crate) fn cfi_calls() -> u64 {
        CFI_CALLS.with(|c| c.get())
    }
}

#[cfg(test)]
mod tests {
    use super::cfi_test_support::{arm_cfi, cfi_calls};
    use super::*;

    fn ok(r: Result<Vec<u8>, CryptError>) -> String {
        match r {
            // Every vector in these tests is ASCII; non-UTF-8 outputs are the
            // divergence_witness module's domain.
            Ok(s) => String::from_utf8(s).expect("test vectors are ASCII"),
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
        for r in [
            "2147483648",           // wraps to -2147483648 (C NOTICE prints it)
            "4294967296",           // wraps to 0
            "99999999999999999999", // strtol saturates LONG_MAX, truncates to -1
            "-5",
            "",  // strtol converts nothing, endp at '$': srounds = 0
            "0",
            "999",
        ] {
            // Finite CFI budget keeps this witness BOUNDED even if the clamp
            // regresses to a huge round count: a broken clamp trips the
            // budget on round 2001 and errors fast instead of grinding.
            arm_cfi(2000);
            let h = ok(crypt(b"pw", format!("$5$rounds={r}$abcdefgh").as_bytes()));
            assert_eq!(h, C_ORACLE_CLAMPED_MIN, "rounds={r}");
            // The rounds ACTUALLY RUN equal C's clamped 1000 (one
            // CHECK_FOR_INTERRUPTS per round).
            assert_eq!(cfi_calls(), 1000, "rounds={r}");
        }
    }

    #[test]
    fn shacrypt_rounds_strtol_sign_and_space_like_c() {
        for r in ["7000", "+7000", " 7000"] {
            arm_cfi(8000); // bounded witness, same rationale as above
            let h = ok(crypt(b"pw", format!("$5$rounds={r}$abcdefgh").as_bytes()));
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
            match crypt(b"pw", format!("$5$rounds={r}$abcdefgh").as_bytes()) {
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
    // 200k rounds keeps the witness bounded even with interrupts absent
    // (the planted-defect run must fail FAST, not wedge the binary).
    #[test]
    fn shacrypt_loop_is_cancellable() {
        arm_cfi(5);
        match crypt(b"pw", b"$6$rounds=200000$abcdefgh") {
            Err(CryptError::Pg(e)) => {
                assert_eq!(e.sqlstate, types_error::ERRCODE_QUERY_CANCELED);
            }
            Ok(_) => panic!("200,000-round crypt completed: interrupts not honored"),
            Err(_) => panic!("unexpected error kind"),
        }
        assert_eq!(cfi_calls(), 6); // 5 Ok rounds + the cancelling call
    }

    // D19: same for the bcrypt 2^cost loop.
    #[test]
    fn bcrypt_loop_is_cancellable() {
        arm_cfi(5);
        match crypt(b"pw", b"$2a$14$......................") {
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
        let h = ok(crypt(b"foox", b"$1$Szzz0yzz"));
        assert!(h.starts_with("$1$Szzz0yzz$"));
        assert_eq!(h.len(), "$1$Szzz0yzz$".len() + 22);
        assert_eq!(ok(crypt(b"foox", h.as_bytes())), h);
    }

    // Traditional/xdes DES known vectors (crypt-des.c), incl. adversarial salt.
    #[test]
    fn des_known_vectors() {
        assert_eq!(ok(crypt(b"foob", b"rl")), "rlK6kmJqyMjZM");
        assert_eq!(ok(crypt(b"password", b"_/!!!!!!!")), "_/!!!!!!!zqM49hRzxko");
    }

    // bcrypt $2a$ roundtrip: crypt(pw, hash) reproduces the hash.
    #[test]
    fn bcrypt_roundtrip() {
        arm_cfi(u64::MAX);
        let setting = b"$2a$06$......................";
        let h = ok(crypt(b"foox", setting));
        assert!(h.starts_with("$2a$06$"));
        assert_eq!(ok(crypt(b"foox", h.as_bytes())), h);
    }

    // sha-crypt $5$/$6$ roundtrip.
    #[test]
    fn shacrypt_roundtrip() {
        arm_cfi(u64::MAX);
        let h5 = ok(crypt(b"foox", b"$5$Szzz0yzz"));
        assert!(h5.starts_with("$5$Szzz0yzz$"));
        assert_eq!(ok(crypt(b"foox", h5.as_bytes())), h5);
        let h6 = ok(crypt(b"foox", b"$6$Szzz0yzz"));
        assert!(h6.starts_with("$6$Szzz0yzz$"));
        assert_eq!(ok(crypt(b"foox", h6.as_bytes())), h6);
    }
}
