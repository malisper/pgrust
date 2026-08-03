//! Ground-truthed C-vs-Rust divergence witnesses for the crypt()/gen_salt()
//! salt-parser and dispatch surface (campaign lanes p1-pgcrypto, p1-shaport).
//!
//! EVERY expectation in this file was captured from a live **PostgreSQL 18.3**
//! server (docker `postgres:18.3`, pgcrypto 1.4) — never from a hand-read of
//! the C or from a vendored model. The oracle pin is the campaign's
//! (LIT-REVIEW §6/§8.4): the C source of record is
//! `contrib/pgcrypto/{px-crypt,crypt-gensalt,crypt-des,crypt-sha}.c` at
//! upstream `62d6c7d3df`, and the *behavior* of record is what that build
//! actually printed, quoted verbatim in each row below.
//!
//! Two capture waves:
//!   - p1-pgcrypto 2026-08-01 (`pg-stock183`, re-verified on an independent
//!     container before banking);
//!   - p1-shaport 2026-08-01 (`pg-stock183` capture 1, independent fresh
//!     container `pg-shaport-b` capture 2 — the two agreed exactly, including
//!     every SQLSTATE, NOTICE text, and the `$5$$abc` quirk hash family).
//!
//! A test named `div_*` documents a defect row: while the defect was open it
//! was `#[ignore]`d, and un-ignoring it is the fix gate. Lane p1-shaport's
//! native crypt-sha.c port retired D6, D7, D13, D14, D16, D17, D18; lane
//! p1-pgcryptofam-fixes retired D1, D2 (px_crypt_list ported as a table),
//! D3, D4, D5 (gen_list ported as a table) and D11 ($2x$ sign-extension
//! bug-compat). EVERY `div_*` row in this file is now ACTIVE. A test named
//! `par_*` asserts a parity that already held and must not regress.

use super::{crypt, gen_salt, CryptError};

fn crypt_ok(pw: &str, salt: &str) -> Result<String, String> {
    crypt(pw.as_bytes(), salt.as_bytes())
        .map(|v| String::from_utf8(v).expect("this witness row's hash is ASCII"))
        .map_err(|e| match e {
        CryptError::Unsupported(w) => format!("unsupported:{w}"),
        CryptError::Message(m) => m,
        CryptError::Pg(e) => e.message.clone(),
    })
}

/// Like `crypt_ok` but returns (message, sqlstate) for the error rows whose
/// SQLSTATE was captured from 18.3 (`\set VERBOSITY verbose`).
fn crypt_err_state(pw: &str, salt: &str) -> (String, types_error::SqlState) {
    match crypt(pw.as_bytes(), salt.as_bytes()) {
        Ok(h) => panic!("expected an error, got hash {h:?}"),
        Err(CryptError::Pg(e)) => (e.message.clone(), e.sqlstate),
        Err(CryptError::Message(m)) => panic!("plain message (no sqlstate): {m:?}"),
        Err(CryptError::Unsupported(w)) => panic!("unsupported: {w}"),
    }
}

fn gen_salt_ok(ty: &str, rounds: i32) -> Result<String, String> {
    gen_salt(ty, rounds).map_err(|e| match e {
        CryptError::Unsupported(w) => format!("unsupported:{w}"),
        CryptError::Message(m) => m,
        CryptError::Pg(e) => e.message.clone(),
    })
}

fn arm() {
    super::cfi_test_support::arm_cfi(u64::MAX);
}

// ---------------------------------------------------------------------------
// D1 — crypt(pw, "$2$...") : C ERRORS, pgrust silently hashes with DES.
//
// C: px-crypt.c:92 `{"$2$", 3, NULL}` matches BEFORE the DES catch-all, so
// px_crypt returns NULL and pgcrypto.c raises
//     ERROR:  crypt(3) returned NULL      (SQLSTATE 39000,
//     ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION)
// Captured 18.3:
//     SELECT crypt('foox','$2$06$......................')
//       => ERROR:  crypt(3) returned NULL
//     SELECT crypt('foox','$2$')
//       => ERROR:  crypt(3) returned NULL    (39000, re-captured x2 2026-08-01)
// RETIRED (lane p1-pgcryptofam-fixes): crypt() now walks a port of the
// px_crypt_list TABLE, so the NULL-handler row is reachable.
// ---------------------------------------------------------------------------
#[test]
fn div_d1_dollar2_must_error() {
    for setting in ["$2$06$......................", "$2$"] {
        let got = crypt_ok("foox", setting);
        assert_eq!(
            got,
            Err("crypt(3) returned NULL".to_string()),
            "setting {setting:?}: C 18.3 raises 'crypt(3) returned NULL'"
        );
    }
}

// ---------------------------------------------------------------------------
// D2 — crypt(pw, "$2b$...") : C runs TRADITIONAL DES, pgrust runs bcrypt.
//
// C: px_crypt_list has NO "$2b$" row. strncmp fails against "$2a$"/"$2x$"
// (byte 2) and against "$2$" (byte 2: 'b' vs '$'), so the `{"", 0,
// run_crypt_des}` catch-all wins and DES hashes with the 2-char salt "$2".
// Captured 18.3:
//     SELECT crypt('foox','$2b$06$......................')
//       => $2A2eTeOR8FRk
// RETIRED (lane p1-pgcryptofam-fixes): the ported table has no $2b$ row and
// crypt-blowfish.c's setting[2] check accepts only 'a'/'x'.
// ---------------------------------------------------------------------------
#[test]
fn div_d2_dollar2b_is_des_in_c() {
    let got = crypt_ok("foox", "$2b$06$......................");
    assert_eq!(
        got,
        Ok("$2A2eTeOR8FRk".to_string()),
        "C 18.3 falls through to run_crypt_des for $2b$"
    );
}

// ---------------------------------------------------------------------------
// D3 — gen_salt('xdes') default rounds: C = PX_XDES_ROUNDS = 29*25 = 725,
//      pgrust hardcodes 7250 (decimal-shifted by one place).
// Captured 18.3:
//     SELECT gen_salt('xdes')    => _J9..D/q8      (count chars "J9..")
//     SELECT gen_salt('xdes', 0) => _J9..hrzq      (count chars "J9..")
// RETIRED (lane p1-pgcryptofam-fixes): gen_list ported as a table, so the
// default comes from PX_XDES_ROUNDS (px-crypt.h:43) instead of a literal.
// ---------------------------------------------------------------------------
#[test]
fn div_d3_xdes_default_rounds_is_725() {
    const ITOA64: &[u8; 64] =
        b"./0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    fn decode_count(salt: &str) -> u32 {
        let b = salt.as_bytes();
        let idx = |c: u8| ITOA64.iter().position(|&x| x == c).unwrap() as u32;
        idx(b[1]) | (idx(b[2]) << 6) | (idx(b[3]) << 12) | (idx(b[4]) << 18)
    }
    for rounds in [0, 0] {
        let s = gen_salt_ok("xdes", rounds).expect("xdes gen_salt must succeed");
        assert!(s.starts_with('_'), "xdes salt must start with '_': {s:?}");
        assert_eq!(
            decode_count(&s),
            725,
            "C 18.3 gen_salt('xdes') encodes count=725 (chars \"J9..\"); got salt {s:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// D4 — gen_salt('xdes', <even>) : C ERRORS, pgrust silently forces it odd.
// Captured 18.3:
//     SELECT gen_salt('xdes', 100) => ERROR:  gen_salt: Incorrect number of rounds
// RETIRED (lane p1-pgcryptofam-fixes): _crypt_gensalt_extended_rn's own
// `!(count & 1)` guard (crypt-gensalt.c:50) is ported — it REFUSES an even
// count where pgrust used to OR it odd.
// ---------------------------------------------------------------------------
#[test]
fn div_d4_xdes_even_rounds_must_error() {
    for rounds in [2, 100, 1000, 16_777_214] {
        assert_eq!(
            gen_salt_ok("xdes", rounds),
            Err("gen_salt: Incorrect number of rounds".to_string()),
            "C 18.3 rejects even xdes rounds={rounds}"
        );
    }
}

// ---------------------------------------------------------------------------
// D5 — gen_salt('xdes', <out of range>) : C range-checks [1, 0xFFFFFF],
//      pgrust does not.
// Captured 18.3:
//     SELECT gen_salt('xdes', -5)       => ERROR:  gen_salt: Incorrect number of rounds
//     SELECT gen_salt('xdes', 16777216) => ERROR:  gen_salt: Incorrect number of rounds
//     SELECT gen_salt('xdes', 16777215) => _zzzz/9Vu   (accepted; odd, == max)
// RETIRED (lane p1-pgcryptofam-fixes): px_gen_salt's `rounds < min_rounds ||
// rounds > max_rounds` check (px-crypt.c:176) is ported and driven by the
// gen_list row's [1, 0xFFFFFF].
// ---------------------------------------------------------------------------
#[test]
fn div_d5_xdes_rounds_range_checked() {
    for rounds in [-5, -1, i32::MIN, 16_777_216, i32::MAX] {
        assert_eq!(
            gen_salt_ok("xdes", rounds),
            Err("gen_salt: Incorrect number of rounds".to_string()),
            "C 18.3 rejects xdes rounds={rounds} (valid range is 1..=0xFFFFFF)"
        );
    }
    let s = gen_salt_ok("xdes", 16_777_215).expect("xdes rounds=0xFFFFFF is valid in C");
    assert!(s.starts_with("_zzzz"), "C 18.3 encodes 0xFFFFFF as \"zzzz\": {s:?}");
}

// ---------------------------------------------------------------------------
// D6 — crypt(pw, "$5$rounds=<empty|0|overflow>$salt") : C NOTICEs and CLAMPS
//      to PX_SHACRYPT_ROUNDS_MIN. RETIRED by the native crypt-sha.c port
//      (strtoint wrap semantics + clamp-with-NOTICE).
//
// Captured 18.3 (p1-shaport Q13/Q14/Q15/Q19, x2 2026-08-01; all SUCCEED):
//   $5$rounds=$abc   => NOTICE 22003 "rounds=0 is below supported value
//                       (1000), using 1000 instead" then the hash below
//   $5$rounds=0$abc  => same NOTICE, same hash
//   $5$rounds=999999999999999999999$abc
//                    => NOTICE "rounds=-1 is below ..." (wrapping accumulator
//                       printed signed), same hash
//   $5$rounds=1000$abc => same hash, no NOTICE
// ---------------------------------------------------------------------------
#[test]
fn div_d6_shacrypt_rounds_clamped_not_rejected() {
    arm();
    const EXPECT: &str = "$5$rounds=1000$abc$kTpZz2KFSVwRekqnKRuPp35Q8tDNHtBQqRufqP7PH06";
    for setting in [
        "$5$rounds=$abc",
        "$5$rounds=0$abc",
        "$5$rounds=999999999999999999999$abc",
        "$5$rounds=1000$abc",
    ] {
        assert_eq!(
            crypt_ok("foox", setting),
            Ok(EXPECT.to_string()),
            "C 18.3 clamps rounds to 1000 and succeeds for {setting:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// D7 — crypt(pw, "$5$<salt with an out-of-alphabet char>") : C ERRORS.
//      RETIRED by the native port (itoa64 charset walk + strstr guards +
//      strtoint terminator check), with C's exact messages AND SQLSTATEs.
//
// Captured 18.3 (p1-shaport Q24-Q27/Q34, x2 2026-08-01):
//   $5$ab*cd             => ERROR 22023: invalid character in salt string: "*"
//   $5$rounds=1000$ab*cd => ERROR 22023: invalid character in salt string: "*"
//   $6$ab*cd             => ERROR 22023: invalid character in salt string: "*"
//   $5$abrounds=xx       => ERROR XX000: invalid rounds option specified in salt string
//   $5$rounds=1000abc    => ERROR 42601: could not parse salt options
// ---------------------------------------------------------------------------
#[test]
fn div_d7_shacrypt_salt_charset_validated() {
    arm();
    for (setting, want_msg, want_state) in [
        (
            "$5$ab*cd",
            "invalid character in salt string: \"*\"",
            types_error::ERRCODE_INVALID_PARAMETER_VALUE,
        ),
        (
            "$5$rounds=1000$ab*cd",
            "invalid character in salt string: \"*\"",
            types_error::ERRCODE_INVALID_PARAMETER_VALUE,
        ),
        (
            "$6$ab*cd",
            "invalid character in salt string: \"*\"",
            types_error::ERRCODE_INVALID_PARAMETER_VALUE,
        ),
        (
            "$5$abrounds=xx",
            "invalid rounds option specified in salt string",
            types_error::ERRCODE_INTERNAL_ERROR,
        ),
        (
            "$5$rounds=1000abc",
            "could not parse salt options",
            types_error::ERRCODE_SYNTAX_ERROR,
        ),
    ] {
        let (msg, state) = crypt_err_state("foox", setting);
        assert_eq!(msg, want_msg, "message for {setting:?}");
        assert_eq!(state, want_state, "sqlstate for {setting:?}");
    }
}

// ---------------------------------------------------------------------------
// D13 — bare "$5$" / "$6$" : C succeeds with an EMPTY salt. RETIRED by the
//       native port (the salt loop simply never runs, crypt-sha.c:293).
//
// Captured 18.3 (p1-shaport Q07/Q08/Q09, x2 2026-08-01):
//   $5$   => $5$$BKJW5ey1MB7BqbSq882..E29Y/k3WN80Ch8NBXT7rl/
//   $6$   => $6$$B/s2.164gZeDFJLKTK1nt.…KwsE/
//   $5$$  => same hash as $5$ (leading '$' skipped, raw prefix empty)
// ---------------------------------------------------------------------------
#[test]
fn div_d13_shacrypt_empty_salt_accepted() {
    arm();
    const H5: &str = "$5$$BKJW5ey1MB7BqbSq882..E29Y/k3WN80Ch8NBXT7rl/";
    assert_eq!(crypt_ok("foox", "$5$"), Ok(H5.to_string()));
    assert_eq!(
        crypt_ok("foox", "$6$"),
        Ok("$6$$B/s2.164gZeDFJLKTK1nt.obcBB20D9S3ENA1XG7fDIIh.SHlnxRqDiHf5QOU.HWJFmm6qUAjL80ofIAEKwsE/".to_string())
    );
    // Salt "$": skipped in the cleaned salt, salt_len stays 0, so the raw
    // prefix is also empty — identical to the bare-magic hash.
    assert_eq!(crypt_ok("foox", "$5$$"), Ok(H5.to_string()));
}

// ---------------------------------------------------------------------------
// D14 — leading '$' in a sha-crypt salt: digest B and DS read the RAW salt
//       pointer while digest A and the result string use the CLEANED salt
//       (crypt-sha.c:333-346 vs :377,:456). RETIRED by the native port,
//       which reproduces the raw-vs-cleaned asymmetry structurally.
//
// Proof the quirk is live: C's answer for "$5$$abc" is NOT C's own answer
// for "$5$abc". Captured 18.3 (p1-shaport Q01-Q05/Q23, x2 2026-08-01):
//   $5$$abc   => $5$abc$dn624JDMiLH3tHBXng298PICrdnAUhoOFU1QWRKP3G1
//   $5$abc    => $5$abc$MhVm6lOm5NxccMg.jT6tB06vflslWYXNfwgh/APZHyB
//   $6$$abc   => $6$abc$XCMcKN9cKmnviz32iYEUVo6caY2XHnBZCddXgjM/Hplil…
//   $6$abc    => $6$abc$LJzMxPoOSUgNgMCLfvIxpZdFSkQLzoP5e/7LkrFgT5Pj…
//   $5$$$$abc => $5$abc$VaBQXB6gld6Cg9t7B..ZZiRppFWajvT8gu3nH/9NXK3
//                (raw prefix "$$$" — THREE distinct hashes for the same
//                 cleaned salt "abc")
//   $5$rounds=1001$$abc
//             => $5$rounds=1001$abc$akPdJbm2VVWMGDqkzH6cfizkpFdXy85tZZ7Sn1mV9Y3
// ---------------------------------------------------------------------------
#[test]
fn div_d14_shacrypt_leading_dollar_salt() {
    arm();
    assert_eq!(
        crypt_ok("foox", "$5$$abc"),
        Ok("$5$abc$dn624JDMiLH3tHBXng298PICrdnAUhoOFU1QWRKP3G1".to_string()),
        "C 18.3 hashes a leading-'$' salt differently from the same salt without it"
    );
    assert_eq!(
        crypt_ok("foox", "$5$abc"),
        Ok("$5$abc$MhVm6lOm5NxccMg.jT6tB06vflslWYXNfwgh/APZHyB".to_string())
    );
    assert_eq!(
        crypt_ok("foox", "$6$$abc"),
        Ok("$6$abc$XCMcKN9cKmnviz32iYEUVo6caY2XHnBZCddXgjM/HplilBxvw5yuMp3QlZE3i3jh1adAZVkBrlj0Hg6AE.cp91".to_string())
    );
    assert_eq!(
        crypt_ok("foox", "$6$abc"),
        Ok("$6$abc$LJzMxPoOSUgNgMCLfvIxpZdFSkQLzoP5e/7LkrFgT5PjUwgMjboE34LCHAKnpivBTkLevqYrcKzgCDPC7/yAL.".to_string())
    );
    assert_eq!(
        crypt_ok("foox", "$5$$$$abc"),
        Ok("$5$abc$VaBQXB6gld6Cg9t7B..ZZiRppFWajvT8gu3nH/9NXK3".to_string()),
        "raw prefix \"$$$\": a third distinct hash for cleaned salt \"abc\""
    );
    assert_eq!(
        crypt_ok("foox", "$5$rounds=1001$$abc"),
        Ok("$5$rounds=1001$abc$akPdJbm2VVWMGDqkzH6cfizkpFdXy85tZZ7Sn1mV9Y3".to_string()),
        "the quirk composes with a custom rounds option"
    );
}

// ---------------------------------------------------------------------------
// D16 — C's whole-string strstr guards (crypt-sha.c:307-321). RETIRED by the
//       native port: the guards scan the ENTIRE remaining string (past the
//       salt terminator), and run only when the salt loop body executes.
//
// Captured 18.3 (p1-shaport Q28/Q29, x2 2026-08-01):
//   $5$abc$rounds=1000 => ERROR XX000: invalid rounds option specified in salt string
//   $5$abc$$5$         => ERROR XX000: bogus magic byte found in salt string
// ---------------------------------------------------------------------------
#[test]
fn div_d16_shacrypt_wholestring_guards() {
    arm();
    let (msg, state) = crypt_err_state("foox", "$5$abc$rounds=1000");
    assert_eq!(msg, "invalid rounds option specified in salt string");
    assert_eq!(state, types_error::ERRCODE_INTERNAL_ERROR);
    let (msg, state) = crypt_err_state("foox", "$5$abc$$5$");
    assert_eq!(msg, "bogus magic byte found in salt string");
    assert_eq!(state, types_error::ERRCODE_INTERNAL_ERROR);
}

// ---------------------------------------------------------------------------
// D17 — strtol leniency: C accepts rounds spellings with leading whitespace
//       and a '+'/'-' sign. RETIRED by the native port's strtol10.
//
// Captured 18.3 (p1-shaport Q16/Q17/Q18, x2 2026-08-01):
//   $5$rounds= 5000$Szzz0yzz => $5$rounds=5000$Szzz0yzz$7hI0rUWk…  (no notice)
//   $5$rounds=+5000$Szzz0yzz => same hash
//   $5$rounds=-1$Szzz0yzz    => NOTICE rounds=-1 … using 1000, then success
// ---------------------------------------------------------------------------
#[test]
fn div_d17_shacrypt_strtol_leniency() {
    arm();
    const SIGNED_5000: &str = "$5$rounds=5000$Szzz0yzz$7hI0rUWkO2QdBkzamh.vP.MIPlbZiwSvu2smhSi6064";
    for setting in ["$5$rounds= 5000$Szzz0yzz", "$5$rounds=+5000$Szzz0yzz"] {
        assert_eq!(
            crypt_ok("foox", setting),
            Ok(SIGNED_5000.to_string()),
            "C 18.3 accepts {setting:?} via strtol and uses rounds=5000"
        );
    }
    assert_eq!(
        crypt_ok("foox", "$5$rounds=-1$Szzz0yzz"),
        Ok("$5$rounds=1000$Szzz0yzz$Ue.UW67KXANe1OZKZU7Erdp88npDuGus0kz4Si9vueA".to_string()),
        "C 18.3 clamps a negative rounds to 1000 and succeeds"
    );
}

// ---------------------------------------------------------------------------
// D18 — the old pwhash path computed `b as u32 - 0x20` BEFORE its range
//       check (enc_dec.rs:38): PANIC in debug/test/fuzz profiles, wrapped
//       nonsense + wrong error in release. RETIRED: the native port
//       validates salt bytes by itoa64 table membership — no arithmetic at
//       all — and raises C's exact error, so EVERY build profile (including
//       release fuzz builds, task #72) is panic-free here.
//
// Captured 18.3 (p1-shaport Q30/Q31, x2 2026-08-01):
//   $5$ab<LF>cd  => ERROR 22023: invalid character in salt string: "<LF>"
//   $5$ab<é>cd   => ERROR 22023: invalid character in salt string: "é"
//                   (pg_mblen: ONE multibyte char in the message)
// ---------------------------------------------------------------------------
#[test]
fn div_d18_shacrypt_sub_space_salt_byte() {
    arm();
    let (msg, state) = crypt_err_state("foox", "$5$ab\ncd");
    assert_eq!(msg, "invalid character in salt string: \"\n\"");
    assert_eq!(state, types_error::ERRCODE_INVALID_PARAMETER_VALUE);
    // Sub-0x20 and DEL-range bytes must error cleanly in every profile.
    for b in ["\u{1}", "\u{1f}", "\u{7f}"] {
        let (msg, _) = crypt_err_state("foox", &format!("$5$ab{b}cd"));
        assert_eq!(msg, format!("invalid character in salt string: \"{b}\""));
    }
    // Multibyte: C prints pg_mblen bytes — one whole UTF-8 char.
    let (msg, state) = crypt_err_state("foox", "$5$ab\u{e9}cd");
    assert_eq!(msg, "invalid character in salt string: \"\u{e9}\"");
    assert_eq!(state, types_error::ERRCODE_INVALID_PARAMETER_VALUE);
}

// ---------------------------------------------------------------------------
// D11 — "$2x$" sign-extension bug-compat. RETIRED (lane p1-pgcryptofam-fixes)
// by porting BF_set_key with C's fourth parameter (crypt-blowfish.c:549-577,
// selected at :643 by setting[2] == 'x').
//
// U&'\00e9abc' is the 5-byte UTF-8 password "\xc3\xa9abc": both leading bytes
// are >= 0x80, so the bug is live in the first expanded word.
// ---------------------------------------------------------------------------
#[test]
fn div_d11_bcrypt_2x_sign_extension() {
    arm();
    let got = crypt_ok("\u{e9}abc", "$2x$06$......................");
    assert_eq!(
        got,
        Ok("$2x$06$......................V57Ks8to0WewzBScSB7UsaowRVkZyEq".to_string()),
        "C 18.3 sign-extends high-bit password bytes under the $2x$ bug-compat flag"
    );
}

/// STRUCTURAL fence for the D11 fix (no oracle constant of its own — the
/// oracle row is div_d11 above). crypt-blowfish.c:566 differs from :568 only
/// for bytes >= 0x80, so: an all-ASCII password must hash IDENTICALLY under
/// `$2x$` and `$2a$`, and a high-bit password must NOT. The second half is
/// what fails if the bug flag is wired but ignored; the first half is what
/// fails if the bug is applied unconditionally (which would corrupt `$2a$`).
#[test]
fn par_bcrypt_sign_extension_is_2x_only() {
    arm();
    let ascii_2x = crypt_ok("foox", "$2x$06$......................");
    let ascii_2a = crypt_ok("foox", "$2a$06$......................");
    assert_eq!(
        ascii_2x.as_deref().map(|s| &s[4..]),
        ascii_2a.as_deref().map(|s| &s[4..]),
        "an all-ASCII password takes the same path under both minor versions"
    );
    let high_2a = crypt_ok("\u{e9}abc", "$2a$06$......................");
    let high_2x = crypt_ok("\u{e9}abc", "$2x$06$......................");
    assert_ne!(
        high_2a.as_deref().map(|s| &s[4..]),
        high_2x.as_deref().map(|s| &s[4..]),
        "a high-bit password MUST diverge between $2a$ and $2x$"
    );
}

// NOTE ON D12 (rounds >= 2^31) — fixed by the strtoint-semantics parse (lane
// p1-cryptofix) and covered by shacrypt_rounds_out_of_range_clamps_like_c in
// crypt.rs, which also counts the rounds ACTUALLY RUN via the CFI seam.

/// Salt truncation at PX_SHACRYPT_SALT_MAX_LEN = 16 IS parity, and bytes at
/// index >= 16 are NOT charset-validated (the loop never reaches them).
/// Captured 18.3 (Q10/Q11/Q12, x2 2026-08-01):
///   $5$aaaaaaaaaaaaaaaaaaaaaaaaaaaa (28 a's)
///     => $5$aaaaaaaaaaaaaaaa$B11QZDRhoWyM20Hwh7.Zl49rcSWmzw7UJhWuikpQAJ9
///   $5$abcdefghijklmnopqrstuvwx$zz
///     => $5$abcdefghijklmnop$JAyZH0bk8ZKHDs.vwAmgvGdfpibPWWZ4w8WAGOn5Hw.
///   $5$aaaaaaaaaaaaaaaa* ('*' at byte 16: ignored, same hash as 16 a's)
///     => $5$aaaaaaaaaaaaaaaa$B11QZDRhoWyM20Hwh7.Zl49rcSWmzw7UJhWuikpQAJ9
#[test]
fn par_shacrypt_salt_truncation() {
    arm();
    const H16A: &str = "$5$aaaaaaaaaaaaaaaa$B11QZDRhoWyM20Hwh7.Zl49rcSWmzw7UJhWuikpQAJ9";
    assert_eq!(crypt_ok("foox", "$5$aaaaaaaaaaaaaaaaaaaaaaaaaaaa"), Ok(H16A.to_string()));
    assert_eq!(
        crypt_ok("foox", "$5$abcdefghijklmnopqrstuvwx$zz"),
        Ok("$5$abcdefghijklmnop$JAyZH0bk8ZKHDs.vwAmgvGdfpibPWWZ4w8WAGOn5Hw.".to_string())
    );
    assert_eq!(
        crypt_ok("foox", "$5$aaaaaaaaaaaaaaaa*"),
        Ok(H16A.to_string()),
        "a byte past the 16-byte cap is ignored, not validated (C parity)"
    );
}

/// Mid-salt '$' terminates after >=1 absorbed byte; the remainder (a would-be
/// attached hash) is ignored. Captured 18.3 (Q06, x2 2026-08-01):
///   $5$a$b => $5$a$gi1XEPlQ5zDdZGkLnvfZe433Gvv.CxgA2VH6SDE4zl0
#[test]
fn par_shacrypt_salt_terminator() {
    arm();
    assert_eq!(
        crypt_ok("foox", "$5$a$b"),
        Ok("$5$a$gi1XEPlQ5zDdZGkLnvfZe433Gvv.CxgA2VH6SDE4zl0".to_string())
    );
}

/// $6$ with a custom in-range rounds value (no clamp, no notice).
/// Captured 18.3 (Q22, x2 2026-08-01).
#[test]
fn par_shacrypt_sha512_custom_rounds() {
    arm();
    assert_eq!(
        crypt_ok("foox", "$6$rounds=1234$xyz"),
        Ok("$6$rounds=1234$xyz$sRV9A3N0x9Be37PfT2zfmsPYVO0BvXQJczTlkguIvVZNKxET9NtnOHrPUMOqTCNpHf19V/aEI0R9Rpm3lgGm0.".to_string())
    );
}

/// Rounds spellings that agree on both sides — a fence so the D17 fix does
/// not overshoot. Captured 18.3 (Q20/Q21, x2 2026-08-01): leading zeros are
/// not significant.
#[test]
fn par_shacrypt_rounds_agreed_spellings() {
    arm();
    const CANON: &str = "$5$rounds=5000$Szzz0yzz$7hI0rUWkO2QdBkzamh.vP.MIPlbZiwSvu2smhSi6064";
    assert_eq!(crypt_ok("foox", "$5$rounds=5000$Szzz0yzz"), Ok(CANON.to_string()));
    assert_eq!(
        crypt_ok("foox", "$5$rounds=0005000$Szzz0yzz"),
        Ok(CANON.to_string()),
        "leading zeros in the rounds value are not significant on either side"
    );
}

/// An unknown `$N$` magic is NOT special-cased by C: it falls through
/// px_crypt_list to the DES catch-all and hashes with the 2-char salt "$7".
///   SELECT crypt('foox','$7$abc') => $7i/EW2zc2O3M
/// An md5 setting with an empty salt is accepted:
///   SELECT crypt('foox','$1$')    => $1$$yS/p28w2q0fzatmSApPDM0
/// A setting too short for any DES salt errors "invalid salt" (22023,
/// re-captured x2 2026-08-01 for ''):
///   SELECT crypt('foox','')  => ERROR:  invalid salt
///   SELECT crypt('foox','x') => ERROR:  invalid salt
///   SELECT crypt('foox','_') => ERROR:  invalid salt
#[test]
fn par_des_fallthrough_and_short_salt() {
    arm();
    assert_eq!(crypt_ok("foox", "$7$abc"), Ok("$7i/EW2zc2O3M".to_string()));
    assert_eq!(crypt_ok("foox", "$1$"), Ok("$1$$yS/p28w2q0fzatmSApPDM0".to_string()));
    for bad in ["", "x", "_"] {
        assert_eq!(
            crypt_ok("foox", bad),
            Err("invalid salt".to_string()),
            "C 18.3 raises 'invalid salt' for {bad:?}"
        );
    }
}

/// gen_salt round validation that ALREADY matches C: bf is bounded [4,31] and
/// sha256crypt/sha512crypt are bounded [1000, 999999999] on BOTH sides, and an
/// unknown algorithm name is rejected.
#[test]
fn par_gen_salt_bounds_and_unknown_algo() {
    for rounds in [3, 32, -1] {
        assert_eq!(
            gen_salt_ok("bf", rounds),
            Err("gen_salt: Incorrect number of rounds".to_string()),
            "bf rounds={rounds} is out of C's [4,31]"
        );
    }
    for ty in ["sha256crypt", "sha512crypt"] {
        for rounds in [999, 1_000_000_000] {
            assert_eq!(
                gen_salt_ok(ty, rounds),
                Err("gen_salt: Incorrect number of rounds".to_string()),
                "{ty} rounds={rounds} is out of C's [1000, 999999999]"
            );
        }
    }
    assert_eq!(
        gen_salt_ok("nosuchalgo", 0),
        Err("gen_salt: Unknown salt algorithm".to_string())
    );
    // C matches the algorithm name with pg_strcasecmp — case-insensitive.
    for ty in ["DES", "Md5", "BF", "XDES"] {
        assert!(
            gen_salt_ok(ty, 0).is_ok(),
            "C 18.3 matches gen_salt type case-insensitively (pg_strcasecmp): {ty}"
        );
    }
}

/// D20 (found while porting gen_list; NOT in the p1-pgcrypto dossier and NOT
/// yet re-captured from a live server — the lane's 18.3 container was
/// unavailable, so this row's provenance is the C SOURCE, not an execution).
///
/// The `des` and `md5` gen_list rows have `def_rounds == 0`, so px_gen_salt
/// skips its range check entirely (px-crypt.c:171) and hands `rounds` straight
/// to the generator, whose own guard is `count && count != 25`
/// (crypt-gensalt.c:29) resp. `count && count != 1000` (:83). A NULL return
/// becomes PXE_BAD_SALT_ROUNDS. So C accepts only 0 or the algorithm's fixed
/// count, and rejects everything else — including negatives, which arrive as
/// huge `unsigned long` values. pgrust previously ignored `rounds` for both.
#[test]
fn div_d20_des_md5_gen_salt_rounds_are_checked() {
    for (ty, ok_count) in [("des", 25), ("md5", 1000)] {
        assert!(gen_salt_ok(ty, 0).is_ok(), "{ty}: rounds=0 takes the default path");
        assert!(
            gen_salt_ok(ty, ok_count).is_ok(),
            "{ty}: the algorithm's own fixed count is accepted"
        );
        for bad in [1, 5, -5, i32::MIN, i32::MAX] {
            assert_eq!(
                gen_salt_ok(ty, bad),
                Err("gen_salt: Incorrect number of rounds".to_string()),
                "{ty} rounds={bad} fails the generator's count guard"
            );
        }
    }
}

/// gen_salt output SHAPE, which is entropy-independent and therefore the only
/// value-plane assertion available without an entropy seam: prefix, total
/// length, and that every random character is drawn from the algorithm's
/// alphabet. Lengths are what C's generators emit (crypt-gensalt.c).
#[test]
fn par_gen_salt_shapes() {
    const ITOA64: &[u8] = b"./0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    const BF64: &[u8] = b"./ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

    let des = gen_salt_ok("des", 0).unwrap();
    assert_eq!(des.len(), 2, "traditional salt is 2 chars");
    assert!(des.bytes().all(|b| ITOA64.contains(&b)));

    let md5 = gen_salt_ok("md5", 0).unwrap();
    assert_eq!(md5.len(), 11, "$1$ + 8 chars");
    assert!(md5.starts_with("$1$"));
    assert!(md5[3..].bytes().all(|b| ITOA64.contains(&b)));

    let xdes = gen_salt_ok("xdes", 0).unwrap();
    assert_eq!(xdes.len(), 9, "_ + 4 count chars + 4 salt chars");
    assert!(xdes.starts_with('_'));
    assert!(xdes[1..].bytes().all(|b| ITOA64.contains(&b)));

    let bf = gen_salt_ok("bf", 0).unwrap();
    assert_eq!(bf.len(), 29, "$2a$NN$ + 22 chars");
    assert!(bf.starts_with("$2a$06$"), "PX_BF_ROUNDS=6 default: {bf:?}");
    assert!(bf[7..].bytes().all(|b| BF64.contains(&b)));

    for (ty, magic) in [("sha256crypt", '5'), ("sha512crypt", '6')] {
        let s = gen_salt_ok(ty, 0).unwrap();
        let want_prefix = format!("${magic}$rounds=5000$");
        assert!(
            s.starts_with(&want_prefix),
            "C always emits the rounds= option and defaults to PX_SHACRYPT_ROUNDS_DEFAULT=5000: {s:?}"
        );
        assert_eq!(
            s.len(),
            want_prefix.len() + 16,
            "salt is PX_SHACRYPT_SALT_MAX_LEN=16 chars: {s:?}"
        );
        assert!(s[want_prefix.len()..].bytes().all(|b| ITOA64.contains(&b)));
    }
}

// ---------------------------------------------------------------------------
// D21 — crypt() output is RAW BYTES: C copies setting-prefix bytes VERBATIM
// into the result (crypt-des.c traditional `output[0..2] = setting[0..2]`,
// xdes `strlcpy(output, setting, 10)`, crypt-md5.c raw salt re-emission), so
// a setting whose copied prefix truncates a multibyte character yields a
// NON-UTF-8 hash even in a UTF-8 database (PG does not encoding-validate a
// function's text result). The pre-fix pgrust laundered those bytes through
// `String::from_utf8_lossy`, substituting U+FFFD — a different hash that,
// unlike C's, does not even round-trip through its own verification.
//
// EXECUTED against stock PostgreSQL 18.3 (pg-stock183, 2026-08-02), byte
// values read via ::bytea:
//   crypt('password', E'\u{20AC}A')          -> \xe282555a6f49796a2f48792f63
//   crypt('password', E'$1$aaaaaaa\u{20AC}') -> \x24312461616161616161e224
//                                                 4e5a72746e754c3351363165
//                                                 436e75436f56762e552e
//   crypt('password', E'_J9..j2z\u{20AC}')   -> \x5f4a392e2e6a327a
//                                                 e26f6a595965614871456c55
// and all three round-trip: crypt(pw, hash) = hash returned true live.
// ---------------------------------------------------------------------------

#[test]
fn div_d21_des_verbatim_salt_echo_and_roundtrip() {
    arm();
    // '€' = E2 82 AC; traditional DES copies setting[0..2] = E2 82 verbatim.
    let setting = "€A".as_bytes();
    let h = crypt(b"password", setting).expect("C hashes this");
    assert_eq!(
        h,
        b"\xe2\x82\x55\x5a\x6f\x49\x79\x6a\x2f\x48\x79\x2f\x63".to_vec(),
        "C 18.3 result bytes (captured live 2026-08-02)"
    );
    // C's crypt(pw, hash) == hash held live; the lossy port broke it.
    assert_eq!(crypt(b"password", &h).expect("re-crypt"), h);
}

#[test]
fn div_d21_md5_verbatim_salt_echo_and_roundtrip() {
    arm();
    // 8-byte salt cap slices the '€' after its first byte: salt run ends E2.
    let setting = "$1$aaaaaaa€".as_bytes();
    let h = crypt(b"password", setting).expect("C hashes this");
    assert_eq!(
        h,
        b"\x24\x31\x24\x61\x61\x61\x61\x61\x61\x61\xe2\x24\x4e\x5a\x72\x74\
          \x6e\x75\x4c\x33\x51\x36\x31\x65\x43\x6e\x75\x43\x6f\x56\x76\x2e\
          \x55\x2e"
            .to_vec(),
        "C 18.3 result bytes (captured live 2026-08-02)"
    );
    assert_eq!(crypt(b"password", &h).expect("re-crypt"), h);
}

#[test]
fn div_d21_xdes_verbatim_setting_echo_and_roundtrip() {
    arm();
    // strlcpy(output, setting, 10) copies 9 bytes: "_J9..j2z" + lone E2.
    let setting = "_J9..j2z€".as_bytes();
    let h = crypt(b"password", setting).expect("C hashes this");
    assert_eq!(
        h,
        b"\x5f\x4a\x39\x2e\x2e\x6a\x32\x7a\xe2\x6f\x6a\x59\x59\x65\x61\x48\
          \x71\x45\x6c\x55"
            .to_vec(),
        "C 18.3 result bytes (captured live 2026-08-02)"
    );
    assert_eq!(crypt(b"password", &h).expect("re-crypt"), h);
}

// D21's input plane: pw/salt enter as raw bytes. Distinct non-UTF-8 password
// byte strings must hash DISTINCTLY (the lossy port collapsed both onto
// U+FFFD — a port-introduced password collision in non-UTF-8 databases).
// Stock 18.3 cannot execute this from SQL in a UTF-8 database (invalid bytes
// are rejected at input), so the expectation here is INEQUALITY +
// determinism, not a captured C value.
#[test]
fn div_d21_non_utf8_passwords_do_not_collide() {
    arm();
    let h_e9 = crypt(b"\xe9", b"$1$saltsalt").expect("md5-crypt of raw byte");
    let h_e8 = crypt(b"\xe8", b"$1$saltsalt").expect("md5-crypt of raw byte");
    assert_ne!(h_e9, h_e8, "distinct password bytes must not collide");
    // And neither equals the hash of the literal replacement character the
    // lossy port substituted.
    let h_fffd = crypt("\u{FFFD}".as_bytes(), b"$1$saltsalt").unwrap();
    assert_ne!(h_e9, h_fffd);
    assert_ne!(h_e8, h_fffd);
}
