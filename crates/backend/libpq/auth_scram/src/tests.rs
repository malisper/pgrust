use super::*;
use mcx::MemoryContext;

const RFC7677_SECRET: &str = "SCRAM-SHA-256$4096:W22ZaJ0SNY7soEsUEjb6gQ==$\
WG5d8oPm3OtcPnkdi4Uo7BkeZkBFzpcXkuLmtbsT4qY=:wfPLwcE6nTWhTAmQ7tl2KeoiWGPlZqQxSrmfPwDl2dU=";

fn install_cfi() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| postgres_seams::check_for_interrupts::set(|| Ok(())));
}

#[test]
fn parse_valid_secret() {
    let p = parse_scram_secret(RFC7677_SECRET).unwrap();
    assert_eq!(p.iterations, 4096);
    assert_eq!(p.key_length, 32);
    assert_eq!(p.salt, "W22ZaJ0SNY7soEsUEjb6gQ==");
    assert_eq!(p.stored_key[0], 0x58);
    assert_eq!(p.server_key[0], 0xc1);
}

#[test]
fn parse_rejects_malformed() {
    for bad in [
        "",
        "md5abc",
        "SCRAM-SHA-256",
        "SCRAM-SHA-256$4096",
        "SCRAM-SHA-256$4096:salt",
        "SCRAM-SHA-256$4096:salt$stored",
        "SCRAM-SHA-1$4096:c2FsdA==$c3RvcmVk:c2VydmVy",
        "SCRAM-SHA-256$40x96:c2FsdA==$c3RvcmVk:c2VydmVy",
        "SCRAM-SHA-256$4096:!!!$c3RvcmVk:c2VydmVy",
        "SCRAM-SHA-256$4096:c2FsdA==$c2hvcnQ=:c2VydmVy",
    ] {
        assert!(parse_scram_secret(bad).is_none(), "{bad}");
    }
}

// strtol semantics: empty iterations converts nothing and yields 0; C accepts.
#[test]
fn parse_strtol_edges() {
    let stored = "WG5d8oPm3OtcPnkdi4Uo7BkeZkBFzpcXkuLmtbsT4qY=";
    let server = "wfPLwcE6nTWhTAmQ7tl2KeoiWGPlZqQxSrmfPwDl2dU=";
    let empty = format!("SCRAM-SHA-256$:c2FsdA==${stored}:{server}");
    assert_eq!(parse_scram_secret(&empty).unwrap().iterations, 0);
    let neg = format!("SCRAM-SHA-256$-1:c2FsdA==${stored}:{server}");
    assert_eq!(parse_scram_secret(&neg).unwrap().iterations, -1);
    let ws = format!("SCRAM-SHA-256$ 42:c2FsdA==${stored}:{server}");
    assert_eq!(parse_scram_secret(&ws).unwrap().iterations, 42);
}

#[test]
fn verify_plain_password_matches_and_rejects() {
    install_cfi();
    let cx = MemoryContext::new("scram-verify-test");
    assert!(scram_verify_plain_password(cx.mcx(), "user", "pencil", RFC7677_SECRET).unwrap());
    assert!(!scram_verify_plain_password(cx.mcx(), "user", "pencil2", RFC7677_SECRET).unwrap());
}

#[test]
fn build_secret_round_trips_through_verify() {
    install_cfi();
    let cx = MemoryContext::new("scram-build-test");
    let secret = pg_be_scram_build_secret(cx.mcx(), "s3kret").unwrap();
    assert!(secret.as_str().starts_with("SCRAM-SHA-256$4096:"));
    assert!(scram_verify_plain_password(cx.mcx(), "u", "s3kret", secret.as_str()).unwrap());
    assert!(!scram_verify_plain_password(cx.mcx(), "u", "other", secret.as_str()).unwrap());
}
