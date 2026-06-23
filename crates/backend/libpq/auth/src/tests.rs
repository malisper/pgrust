//! Tests for the pure, side-effect-free logic of `auth.c`.

use crate::*;

#[test]
fn protocol_constants_match_protocol_h() {
    assert_eq!(AUTH_REQ_OK, 0);
    assert_eq!(AUTH_REQ_PASSWORD, 3);
    assert_eq!(AUTH_REQ_MD5, 5);
    assert_eq!(AUTH_REQ_SASL, 10);
    assert_eq!(AUTH_REQ_SASL_CONT, 11);
    assert_eq!(AUTH_REQ_SASL_FIN, 12);
    assert_eq!(PqMsg_AuthenticationRequest, b'R');
    assert_eq!(PqMsg_PasswordMessage, b'p');
}

#[test]
fn status_codes() {
    assert_eq!(STATUS_OK, 0);
    assert_eq!(STATUS_ERROR, -1);
    assert_eq!(STATUS_EOF, -2);
}
