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

use auth_sasl::{
    SaslMech, PG_SASL_EXCHANGE_CONTINUE, PG_SASL_EXCHANGE_FAILURE, PG_SASL_EXCHANGE_SUCCESS,
};
use ip::SockAddr;
use pg_b64::pg_b64_dec_len;
use pg_hmac::{PgHmacCtx, Sha256};
use scram_common::{scram_client_key, scram_h, scram_salted_password, scram_server_key};
use types_error::make_sqlstate;
use types_startup::{ClientSocket, Port};

fn test_port(user: &str, ssl_in_use: bool) -> Port {
    let mut port = Port::new(&ClientSocket {
        sock: -1,
        raddr: SockAddr::zeroed(),
    });
    port.user_name = Some(user.to_string());
    port.ssl_in_use = ssl_in_use;
    port
}

fn b64enc(src: &[u8]) -> String {
    let cap = pg_b64_enc_len(src.len() as i32);
    let mut dst = vec![0u8; cap as usize];
    let n = pg_b64_encode(src, src.len() as i32, &mut dst, cap);
    assert!(n >= 0);
    dst.truncate(n as usize);
    String::from_utf8(dst).unwrap()
}

fn b64d(src: &str) -> Vec<u8> {
    let cap = pg_b64_dec_len(src.len() as i32);
    let mut dst = vec![0u8; cap as usize];
    let n = pg_b64_decode(src.as_bytes(), src.len() as i32, &mut dst, cap);
    assert!(n >= 0);
    dst.truncate(n as usize);
    dst
}

struct ClientMath {
    client_first_bare: String,
    server_first: String,
    salted_password: [u8; 32],
}

impl ClientMath {
    fn final_message(&self, password_ok: bool, cbind_b64: &str) -> String {
        let full_nonce = {
            let mut it = self.server_first.split(',');
            it.next().unwrap().strip_prefix("r=").unwrap().to_string()
        };
        let without_proof = format!("c={cbind_b64},r={full_nonce}");
        let auth_message = format!(
            "{},{},{}",
            self.client_first_bare, self.server_first, without_proof
        );
        let salted = self.salted_password;
        let client_key = scram_client_key(&salted);
        let stored_key = scram_h(&client_key);
        let mut ctx = PgHmacCtx::<Sha256>::init(&stored_key);
        ctx.update(auth_message.as_bytes());
        let signature = ctx.finalize();
        let mut proof = [0u8; 32];
        for i in 0..32 {
            proof[i] = client_key[i] ^ signature[i];
        }
        if !password_ok {
            proof[0] ^= 0xff;
        }
        format!("{without_proof},p={}", b64enc(&proof))
    }

    fn expect_server_signature(&self, final_without_proof: &str) -> String {
        let auth_message = format!(
            "{},{},{}",
            self.client_first_bare, self.server_first, final_without_proof
        );
        let server_key = scram_server_key(&self.salted_password);
        let mut ctx = PgHmacCtx::<Sha256>::init(&server_key);
        ctx.update(auth_message.as_bytes());
        format!("v={}", b64enc(&ctx.finalize()))
    }
}

fn drive_first(
    state: &mut ScramState,
    port: &mut Port,
    password: &str,
    gs2: &str,
) -> (ClientMath, String) {
    let client_first = format!("{gs2}n=,r=clientnonceclientnonce");
    let (r, out) = ScramMech
        .exchange(state, port, Some(client_first.as_bytes()), &mut None)
        .unwrap();
    assert_eq!(r, PG_SASL_EXCHANGE_CONTINUE);
    let server_first = String::from_utf8(out.unwrap()).unwrap();
    let mut parts = server_first.split(',');
    let _nonce = parts.next().unwrap();
    let salt = b64d(parts.next().unwrap().strip_prefix("s=").unwrap());
    let iterations: i32 = parts.next().unwrap().strip_prefix("i=").unwrap().parse().unwrap();
    let bare = client_first[gs2.len()..].to_string();
    let math = ClientMath {
        client_first_bare: bare,
        server_first: server_first.clone(),
        salted_password: scram_salted_password(password.as_bytes(), &salt, iterations).unwrap(),
    };
    (math, server_first)
}

#[test]
fn exchange_success_full_flow() {
    install_cfi();
    let mut port = test_port("user", false);
    let mut state = test_scram_init(&port, b"SCRAM-SHA-256", Some(RFC7677_SECRET)).unwrap();
    assert!(!state.doomed());

    let (math, _sf) = drive_first(&mut state, &mut port, "pencil", "n,,");
    let final_msg = math.final_message(true, "biws");
    let mut logdetail = None;
    let (r, out) = ScramMech
        .exchange(&mut state, &mut port, Some(final_msg.as_bytes()), &mut logdetail)
        .unwrap();
    assert_eq!(r, PG_SASL_EXCHANGE_SUCCESS);
    let without_proof = final_msg.rsplit_once(",p=").unwrap().0;
    assert_eq!(
        String::from_utf8(out.unwrap()).unwrap(),
        math.expect_server_signature(without_proof)
    );
    assert!(port.has_scram_keys);
    assert!(logdetail.is_none());
}

#[test]
fn exchange_wrong_password_fails_without_output() {
    install_cfi();
    let mut port = test_port("user", false);
    let mut state = test_scram_init(&port, b"SCRAM-SHA-256", Some(RFC7677_SECRET)).unwrap();
    let (math, _sf) = drive_first(&mut state, &mut port, "pencil", "n,,");
    let final_msg = math.final_message(false, "biws");
    let mut logdetail = None;
    let (r, out) = ScramMech
        .exchange(&mut state, &mut port, Some(final_msg.as_bytes()), &mut logdetail)
        .unwrap();
    assert_eq!(r, PG_SASL_EXCHANGE_FAILURE);
    assert!(out.is_none());
    assert!(logdetail.is_none());
    assert!(!port.has_scram_keys);
}

// An MD5 secret cannot do SCRAM: mock (doomed) exchange, correct password
// still fails, logdetail names the real cause.
#[test]
fn md5_secret_runs_doomed_mock_exchange() {
    install_cfi();
    transam_xlog::control_file_mark_read_for_tests();
    let mut port = test_port("alice", false);
    let mut state = test_scram_init(
        &port,
        b"SCRAM-SHA-256",
        Some("md5b4e2418ce2af2f5ffcbbb257a9f55d21"),
    )
    .unwrap();
    assert!(state.doomed());
    assert_eq!(state.iterations(), 4096);

    let (math, sf) = drive_first(&mut state, &mut port, "correct-password", "n,,");
    assert!(sf.ends_with(",i=4096"));
    assert_eq!(state.salt().len(), 24); // 16-byte mock salt, base64

    let final_msg = math.final_message(true, "biws");
    let mut logdetail = None;
    let (r, out) = ScramMech
        .exchange(&mut state, &mut port, Some(final_msg.as_bytes()), &mut logdetail)
        .unwrap();
    assert_eq!(r, PG_SASL_EXCHANGE_FAILURE);
    assert!(out.is_none());
    assert_eq!(
        logdetail.as_deref(),
        Some("User \"alice\" does not have a valid SCRAM secret.")
    );
}

// Nonexistent user: mock exchange, no logdetail (the caller asked for it).
#[test]
fn missing_secret_runs_doomed_mock_exchange() {
    install_cfi();
    transam_xlog::control_file_mark_read_for_tests();
    let mut port = test_port("ghost", false);
    let mut state = test_scram_init(&port, b"SCRAM-SHA-256", None).unwrap();
    assert!(state.doomed());

    let (math, _sf) = drive_first(&mut state, &mut port, "whatever", "n,,");
    let final_msg = math.final_message(true, "biws");
    let mut logdetail = None;
    let (r, _out) = ScramMech
        .exchange(&mut state, &mut port, Some(final_msg.as_bytes()), &mut logdetail)
        .unwrap();
    assert_eq!(r, PG_SASL_EXCHANGE_FAILURE);
    assert!(logdetail.is_none());
}

// RFC 7677 test vector, exact bytes.
#[test]
fn rfc7677_exact_vector() {
    install_cfi();
    let port = test_port("user", false);
    let mut state = test_scram_init(&port, b"SCRAM-SHA-256", Some(RFC7677_SECRET)).unwrap();
    {
        let (bare, server_first, final_wp, proof, _stored) = state.test_fields();
        *bare = b"n=user,r=rOprNGfwEbeRWgbNEkqO".to_vec();
        *server_first =
            b"r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096"
                .to_vec();
        *final_wp =
            b"c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0".to_vec();
        let d = {
            let cap = pg_b64_dec_len(44);
            let mut dst = vec![0u8; cap as usize];
            let src = b"dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=";
            let n = pg_b64_decode(src, src.len() as i32, &mut dst, cap);
            dst.truncate(n as usize);
            dst
        };
        proof[..32].copy_from_slice(&d);
    }
    assert!(test_verify_client_proof(&mut state));
    assert_eq!(
        test_build_server_final_message(&state),
        b"v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=".to_vec()
    );
}

#[test]
fn init_rejects_invalid_mechanism() {
    let port = test_port("user", false);
    let err = test_scram_init(&port, b"SCRAM-SHA-256-PLUS", Some(RFC7677_SECRET)).unwrap_err();
    assert_eq!(err.sqlstate(), make_sqlstate(*b"08P01"));
    assert_eq!(
        err.message(),
        "client selected an invalid SASL authentication mechanism"
    );
}

#[test]
fn get_mechanisms_advertises_plus_only_under_ssl() {
    let cx = MemoryContext::new("mechs");
    let mut buf = stringinfo::StringInfo::new_in(cx.mcx()).unwrap();
    ScramMech
        .get_mechanisms(&test_port("u", false), &mut buf)
        .unwrap();
    assert_eq!(buf.as_bytes(), b"SCRAM-SHA-256\0");

    let mut buf = stringinfo::StringInfo::new_in(cx.mcx()).unwrap();
    ScramMech
        .get_mechanisms(&test_port("u", true), &mut buf)
        .unwrap();
    assert_eq!(buf.as_bytes(), b"SCRAM-SHA-256-PLUS\0SCRAM-SHA-256\0");
}

fn first_message_err(gs2_msg: &str, ssl: bool, mech: &[u8]) -> Box<types_error::PgError> {
    install_cfi();
    let mut port = test_port("user", ssl);
    let mut state = test_scram_init(&port, mech, Some(RFC7677_SECRET)).unwrap();
    ScramMech
        .exchange(&mut state, &mut port, Some(gs2_msg.as_bytes()), &mut None)
        .unwrap_err()
}

#[test]
fn first_message_error_arms() {
    let err = first_message_err("", false, b"SCRAM-SHA-256");
    assert_eq!(err.detail(), Some("The message is empty."));

    let err = first_message_err("n,,n=,r=abc\0def", false, b"SCRAM-SHA-256");
    assert_eq!(
        err.detail(),
        Some("Message length does not match input length.")
    );

    // Downgrade attack arm: client says "y" but the server is SSL-capable.
    let err = first_message_err("y,,n=,r=abcdef", true, b"SCRAM-SHA-256");
    assert_eq!(err.sqlstate(), make_sqlstate(*b"28000"));
    assert_eq!(err.message(), "SCRAM channel binding negotiation error");

    // Channel binding data without the PLUS mechanism.
    let err = first_message_err("p=tls-server-end-point,,n=,r=abcdef", false, b"SCRAM-SHA-256");
    assert_eq!(err.sqlstate(), make_sqlstate(*b"08P01"));
    assert_eq!(err.detail(), Some("The client selected SCRAM-SHA-256 without channel binding, but the SCRAM message includes channel binding data."));

    let err = first_message_err("x,,n=,r=abcdef", false, b"SCRAM-SHA-256");
    assert_eq!(err.detail(), Some("Unexpected channel-binding flag \"'x'\"."));

    let err = first_message_err("n,a=admin,n=,r=abcdef", false, b"SCRAM-SHA-256");
    assert_eq!(err.sqlstate(), make_sqlstate(*b"0A000"));
    assert_eq!(
        err.message(),
        "client uses authorization identity, but it is not supported"
    );

    let err = first_message_err("n,,m=ext,n=,r=abcdef", false, b"SCRAM-SHA-256");
    assert_eq!(err.sqlstate(), make_sqlstate(*b"0A000"));
    assert_eq!(err.message(), "client requires an unsupported SCRAM extension");

    let err = first_message_err("n,,n=,r=abc\x01def", false, b"SCRAM-SHA-256");
    assert_eq!(err.message(), "non-printable characters in SCRAM nonce");

    let err = first_message_err("n,,x=,r=abcdef", false, b"SCRAM-SHA-256");
    assert_eq!(
        err.detail(),
        Some("Expected attribute \"n\" but found \"'x'\".")
    );

    let err = first_message_err("nx", false, b"SCRAM-SHA-256");
    assert_eq!(
        err.detail(),
        Some("Comma expected, but found character \"'x'\".")
    );
}

fn final_message_err(final_msg: &str) -> Box<types_error::PgError> {
    install_cfi();
    let mut port = test_port("user", false);
    let mut state = test_scram_init(&port, b"SCRAM-SHA-256", Some(RFC7677_SECRET)).unwrap();
    let (_math, _sf) = drive_first(&mut state, &mut port, "pencil", "n,,");
    ScramMech
        .exchange(&mut state, &mut port, Some(final_msg.as_bytes()), &mut None)
        .unwrap_err()
}

#[test]
fn final_message_error_arms() {
    let err = final_message_err("c=eSws,r=x,p=eA==");
    assert_eq!(
        err.message(),
        "unexpected SCRAM channel-binding attribute in client-final-message"
    );

    let err =
        final_message_err("c=biws,r=wrongnonce,p=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=");
    assert_eq!(err.message(), "invalid SCRAM response");
    assert_eq!(err.detail(), Some("Nonce does not match."));
}

#[test]
fn final_message_malformed_proof_and_garbage() {
    install_cfi();
    let mut port = test_port("user", false);
    let mut state = test_scram_init(&port, b"SCRAM-SHA-256", Some(RFC7677_SECRET)).unwrap();
    let (math, _sf) = drive_first(&mut state, &mut port, "pencil", "n,,");
    let good = math.final_message(true, "biws");
    let without_proof = good.rsplit_once(",p=").unwrap().0;

    let err = ScramMech
        .exchange(
            &mut state,
            &mut port,
            Some(format!("{without_proof},p=c2hvcnQ=").as_bytes()),
            &mut None,
        )
        .unwrap_err();
    assert_eq!(err.detail(), Some("Malformed proof in client-final-message."));

    let mut port = test_port("user", false);
    let mut state = test_scram_init(&port, b"SCRAM-SHA-256", Some(RFC7677_SECRET)).unwrap();
    let (math, _sf) = drive_first(&mut state, &mut port, "pencil", "n,,");
    let good = math.final_message(true, "biws");
    let err = ScramMech
        .exchange(
            &mut state,
            &mut port,
            Some(format!("{good},x=trailing").as_bytes()),
            &mut None,
        )
        .unwrap_err();
    assert_eq!(
        err.detail(),
        Some("Garbage found at the end of client-final-message.")
    );
}

fn install_cert_hash() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        be_secure_seams::be_tls_get_certificate_hash::set(|| Ok(b"fake-cert-hash-32-bytes....".to_vec()));
    });
}

#[test]
fn channel_binding_success_with_injected_cert_hash() {
    install_cfi();
    install_cert_hash();
    let mut port = test_port("user", true);
    let mut state =
        test_scram_init(&port, b"SCRAM-SHA-256-PLUS", Some(RFC7677_SECRET)).unwrap();
    let (math, _sf) = drive_first(&mut state, &mut port, "pencil", "p=tls-server-end-point,,");

    let mut cbind_input = b"p=tls-server-end-point,,".to_vec();
    cbind_input.extend_from_slice(b"fake-cert-hash-32-bytes....");
    let final_msg = math.final_message(true, &b64enc(&cbind_input));
    let (r, out) = ScramMech
        .exchange(&mut state, &mut port, Some(final_msg.as_bytes()), &mut None)
        .unwrap();
    assert_eq!(r, PG_SASL_EXCHANGE_SUCCESS);
    assert!(out.is_some());
}

#[test]
fn channel_binding_mismatch_is_28000() {
    install_cfi();
    install_cert_hash();
    let mut port = test_port("user", true);
    let mut state =
        test_scram_init(&port, b"SCRAM-SHA-256-PLUS", Some(RFC7677_SECRET)).unwrap();
    let (math, _sf) = drive_first(&mut state, &mut port, "pencil", "p=tls-server-end-point,,");

    let mut cbind_input = b"p=tls-server-end-point,,".to_vec();
    cbind_input.extend_from_slice(b"WRONG-cert-hash-32-bytes...");
    let final_msg = math.final_message(true, &b64enc(&cbind_input));
    let err = ScramMech
        .exchange(&mut state, &mut port, Some(final_msg.as_bytes()), &mut None)
        .unwrap_err();
    assert_eq!(err.sqlstate(), make_sqlstate(*b"28000"));
    assert_eq!(err.message(), "SCRAM channel binding check failed");
}

#[test]
fn plus_selected_first_message_without_binding_data() {
    install_cfi();
    let mut port = test_port("user", true);
    let mut state =
        test_scram_init(&port, b"SCRAM-SHA-256-PLUS", Some(RFC7677_SECRET)).unwrap();
    let err = ScramMech
        .exchange(
            &mut state,
            &mut port,
            Some(b"n,,n=,r=abcdef".as_slice()),
            &mut None,
        )
        .unwrap_err();
    assert_eq!(err.detail(), Some("The client selected SCRAM-SHA-256-PLUS, but the SCRAM message does not include channel binding data."));
}

#[test]
fn unsupported_channel_binding_type() {
    install_cfi();
    let mut port = test_port("user", true);
    let mut state =
        test_scram_init(&port, b"SCRAM-SHA-256-PLUS", Some(RFC7677_SECRET)).unwrap();
    let err = ScramMech
        .exchange(
            &mut state,
            &mut port,
            Some(b"p=tls-unique,,n=,r=abcdef".as_slice()),
            &mut None,
        )
        .unwrap_err();
    assert_eq!(
        err.message(),
        "unsupported SCRAM channel-binding type \"tls-unique\""
    );
}

#[test]
fn no_initial_response_gets_empty_challenge() {
    install_cfi();
    let mut port = test_port("user", false);
    let mut state = test_scram_init(&port, b"SCRAM-SHA-256", Some(RFC7677_SECRET)).unwrap();
    let (r, out) = ScramMech
        .exchange(&mut state, &mut port, None, &mut None)
        .unwrap();
    assert_eq!(r, PG_SASL_EXCHANGE_CONTINUE);
    assert_eq!(out, Some(Vec::new()));
}
