use super::*;
use mcx::MemoryContext;

const RFC7677_SECRET: &str = "SCRAM-SHA-256$4096:W22ZaJ0SNY7soEsUEjb6gQ==$\
WG5d8oPm3OtcPnkdi4Uo7BkeZkBFzpcXkuLmtbsT4qY=:wfPLwcE6nTWhTAmQ7tl2KeoiWGPlZqQxSrmfPwDl2dU=";
const MD5_SECRET: &str = "md553f48b7c4b76a86ce72276c5755f217d";

fn install_cfi() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| postgres_seams::check_for_interrupts::set(|| Ok(())));
}

#[test]
fn classifies_password_types() {
    assert_eq!(get_password_type(MD5_SECRET), PasswordType::Md5);
    assert_eq!(get_password_type(RFC7677_SECRET), PasswordType::ScramSha256);
    assert_eq!(get_password_type("md5short"), PasswordType::Plaintext);
    assert_eq!(get_password_type("md5X3f48b7c4b76a86ce72276c5755f217d"), PasswordType::Plaintext);
    assert_eq!(get_password_type("hunter2"), PasswordType::Plaintext);
    assert_eq!(get_password_type("SCRAM-SHA-256$bogus"), PasswordType::Plaintext);
}

#[test]
fn encrypt_password_md5_matches_c_layout() {
    let cx = MemoryContext::new("crypt-test");
    let out = encrypt_password(cx.mcx(), PasswordType::Md5, "postgres", "secret").unwrap();
    assert_eq!(out.as_str(), MD5_SECRET);
}

#[test]
fn encrypt_password_passes_through_encrypted_forms() {
    let cx = MemoryContext::new("crypt-test");
    let out = encrypt_password(cx.mcx(), PasswordType::ScramSha256, "r", MD5_SECRET).unwrap();
    assert_eq!(out.as_str(), MD5_SECRET);
    let out = encrypt_password(cx.mcx(), PasswordType::Md5, "r", RFC7677_SECRET).unwrap();
    assert_eq!(out.as_str(), RFC7677_SECRET);
}

#[test]
fn encrypt_password_scram_fixed_salt_byte_parity() {
    install_cfi();
    // Deterministic salt via the test hook; expected verifier generated from
    // C's algorithm (PBKDF2-HMAC-SHA256 + Client/Server Key + H).
    std::env::set_var("PGRUST_SCRAM_FIXED_SALT_B64", "AAECAwQFBgcICQoLDA0ODw==");
    let cx = MemoryContext::new("crypt-test");
    let out = encrypt_password(cx.mcx(), PasswordType::ScramSha256, "scramuser", "secret").unwrap();
    std::env::remove_var("PGRUST_SCRAM_FIXED_SALT_B64");
    assert_eq!(
        out.as_str(),
        "SCRAM-SHA-256$4096:AAECAwQFBgcICQoLDA0ODw==$\
         THoPhoTAuqyoQsK4dUHncUzgfD8fdmhsgKZhWVqNP5U=:\
         7YiHMMi2OcXGRogub03Ek06JRZ9bkhTOdCzHa5iPLiQ="
    );
}

#[test]
fn md5_crypt_verify_challenge() {
    let salt = [0x01u8, 0x02, 0x03, 0x04];
    let expected = pg_md5::pg_md5_encrypt(&MD5_SECRET.as_bytes()[3..], &salt);
    let mut logdetail = None;
    let ok = md5_crypt_verify(
        "postgres",
        MD5_SECRET,
        core::str::from_utf8(&expected).unwrap(),
        &salt,
        &mut logdetail,
    )
    .unwrap();
    assert_eq!(ok, STATUS_OK);
    assert!(logdetail.is_none());

    let bad = md5_crypt_verify("postgres", MD5_SECRET, "md5ffffffff", &salt, &mut logdetail).unwrap();
    assert_eq!(bad, STATUS_ERROR);
    assert_eq!(logdetail.as_deref(), Some("Password does not match for user \"postgres\"."));

    let mut logdetail = None;
    let wrong_kind =
        md5_crypt_verify("postgres", RFC7677_SECRET, "x", &salt, &mut logdetail).unwrap();
    assert_eq!(wrong_kind, STATUS_ERROR);
    assert_eq!(
        logdetail.as_deref(),
        Some("User \"postgres\" has a password that cannot be used with MD5 authentication.")
    );
}

#[test]
fn plain_crypt_verify_all_arms() {
    install_cfi();
    let cx = MemoryContext::new("crypt-test");
    let mut logdetail = None;

    assert_eq!(
        plain_crypt_verify(cx.mcx(), "u", RFC7677_SECRET, "pencil", &mut logdetail).unwrap(),
        STATUS_OK
    );
    assert_eq!(
        plain_crypt_verify(cx.mcx(), "u", RFC7677_SECRET, "wrong", &mut logdetail).unwrap(),
        STATUS_ERROR
    );

    assert_eq!(
        plain_crypt_verify(cx.mcx(), "postgres", MD5_SECRET, "secret", &mut logdetail).unwrap(),
        STATUS_OK
    );
    assert_eq!(
        plain_crypt_verify(cx.mcx(), "postgres", MD5_SECRET, "wrong", &mut logdetail).unwrap(),
        STATUS_ERROR
    );

    let mut logdetail = None;
    assert_eq!(
        plain_crypt_verify(cx.mcx(), "u", "plainstored", "plainstored", &mut logdetail).unwrap(),
        STATUS_ERROR
    );
    assert_eq!(
        logdetail.as_deref(),
        Some("Password of user \"u\" is in unrecognized format.")
    );
}

fn install_role_fixtures() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        timestamp_seams::get_current_timestamp::set(|| 1_000_000);
        syscache_seams::lookup_authid_rolpassword::set(|mcx, rolname| {
            let (pass, vuntil) = match rolname {
                "alice" => (Some(RFC7677_SECRET), None),
                "nopass" => (None, None),
                "expired" => (Some(RFC7677_SECRET), Some(999_999i64)),
                "future" => (Some(RFC7677_SECRET), Some(1_000_001i64)),
                _ => return Ok(None),
            };
            let rolpassword = match pass {
                Some(p) => Some(mcx::PgString::from_str_in(p, mcx)?),
                None => None,
            };
            Ok(Some(syscache_seams::AuthIdPasswordShape {
                rolpassword,
                rolvaliduntil: vuntil,
            }))
        });
    });
}

#[test]
fn get_role_password_arms() {
    install_role_fixtures();

    let mut ld = None;
    assert_eq!(
        get_role_password("alice", &mut ld).unwrap().as_deref(),
        Some(RFC7677_SECRET)
    );
    assert!(ld.is_none());

    let mut ld = None;
    assert!(get_role_password("ghost", &mut ld).unwrap().is_none());
    assert_eq!(ld.as_deref(), Some("Role \"ghost\" does not exist."));

    let mut ld = None;
    assert!(get_role_password("nopass", &mut ld).unwrap().is_none());
    assert_eq!(ld.as_deref(), Some("User \"nopass\" has no password assigned."));

    let mut ld = None;
    assert!(get_role_password("expired", &mut ld).unwrap().is_none());
    assert_eq!(ld.as_deref(), Some("User \"expired\" has an expired password."));

    let mut ld = None;
    assert!(get_role_password("future", &mut ld).unwrap().is_some());
    assert!(ld.is_none());
}
