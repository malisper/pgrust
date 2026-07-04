use super::*;
use mcx::MemoryContext;

fn install_cfi() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| postgres_seams::check_for_interrupts::set(|| Ok(())));
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

// Published PBKDF2-HMAC-SHA256 vectors (password/salt, 1/2/4096 iterations).
#[test]
fn pbkdf2_reference_vectors() {
    install_cfi();
    let cases = [
        (1, "120fb6cffcf8b32c43e7225256c4f837a86548c92ccc35480805987cb70be17b"),
        (2, "ae4d0c95af6b46d32d0adff928f06dd02a303f8ef3c251dfd6e2d85a95474c43"),
        (4096, "c5e478d59288c841aa530db6845c4c8d962893a001ce4e11a4963873aa98134a"),
    ];
    for (iterations, expected) in cases {
        let sp = scram_salted_password(b"password", b"salt", iterations).unwrap();
        assert_eq!(hex(&sp), expected, "iterations={iterations}");
    }
}

// RFC 7677 test vector: pencil / base64(W22ZaJ0SNY7soEsUEjb6gQ==) / 4096.
#[test]
fn rfc7677_key_derivation() {
    install_cfi();
    let salt = b64decode("W22ZaJ0SNY7soEsUEjb6gQ==");
    let sp = scram_salted_password(b"pencil", &salt, 4096).unwrap();
    assert_eq!(
        hex(&sp),
        "c4a49510323ab4f952cac1fa99441939e78ea74d6be81ddf7096e87513dc615d"
    );
    let stored_key = scram_h(&scram_client_key(&sp));
    let server_key = scram_server_key(&sp);
    assert_eq!(b64encode(&stored_key), "WG5d8oPm3OtcPnkdi4Uo7BkeZkBFzpcXkuLmtbsT4qY=");
    assert_eq!(b64encode(&server_key), "wfPLwcE6nTWhTAmQ7tl2KeoiWGPlZqQxSrmfPwDl2dU=");
}

#[test]
fn build_secret_rfc7677_verifier() {
    install_cfi();
    let cx = MemoryContext::new("scram-test");
    let salt = b64decode("W22ZaJ0SNY7soEsUEjb6gQ==");
    let secret = scram_build_secret(cx.mcx(), &salt, 4096, b"pencil").unwrap();
    assert_eq!(
        secret.as_str(),
        "SCRAM-SHA-256$4096:W22ZaJ0SNY7soEsUEjb6gQ==$\
         WG5d8oPm3OtcPnkdi4Uo7BkeZkBFzpcXkuLmtbsT4qY=:\
         wfPLwcE6nTWhTAmQ7tl2KeoiWGPlZqQxSrmfPwDl2dU="
    );
}

#[test]
fn build_secret_fixed_salt_fixture() {
    install_cfi();
    let cx = MemoryContext::new("scram-test");
    let salt: Vec<u8> = (0..16u8).collect();
    let secret = scram_build_secret(cx.mcx(), &salt, 4096, b"secret").unwrap();
    assert_eq!(
        secret.as_str(),
        "SCRAM-SHA-256$4096:AAECAwQFBgcICQoLDA0ODw==$\
         THoPhoTAuqyoQsK4dUHncUzgfD8fdmhsgKZhWVqNP5U=:\
         7YiHMMi2OcXGRogub03Ek06JRZ9bkhTOdCzHa5iPLiQ="
    );
}

#[test]
fn one_iteration_is_first_hmac_only() {
    install_cfi();
    let sp = scram_salted_password(b"pw", b"abcd", 1).unwrap();
    let mut ctx = PgHmacCtx::<Sha256>::init(b"pw");
    ctx.update(b"abcd");
    ctx.update(&1u32.to_be_bytes());
    assert_eq!(sp, ctx.finalize());
}

fn b64decode(s: &str) -> Vec<u8> {
    let cap = pg_b64::pg_b64_dec_len(s.len() as i32);
    let mut dst = vec![0u8; cap as usize];
    let n = pg_b64::pg_b64_decode(s.as_bytes(), s.len() as i32, &mut dst, cap);
    assert!(n >= 0);
    dst.truncate(n as usize);
    dst
}

fn b64encode(b: &[u8]) -> String {
    let cap = pg_b64_enc_len(b.len() as i32);
    let mut dst = vec![0u8; cap as usize];
    let n = pg_b64_encode(b, b.len() as i32, &mut dst, cap);
    assert!(n >= 0);
    dst.truncate(n as usize);
    String::from_utf8(dst).unwrap()
}
