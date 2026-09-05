use super::*;

fn detail(e: &PgError) -> String {
    e.detail().map(|s| s.to_string()).unwrap_or_default()
}

#[test]
fn plaintext_too_short_quotes_the_parameter() {
    let e = check_plaintext("regress_pwc_short", "weak", 8).expect_err("too short");
    assert_eq!(e.message(), "password is too short");
    assert_eq!(e.sqlstate(), ERRCODE_INVALID_PARAMETER_VALUE);
    assert_eq!(
        detail(&e),
        "password must be at least \"passwordcheck.min_password_length\" (8) bytes long"
    );
    // The configured length is what the DETAIL reports (passwordcheck.c:82).
    let e = check_plaintext("u", "abcdefgh1", 12).expect_err("too short at 12");
    assert_eq!(
        detail(&e),
        "password must be at least \"passwordcheck.min_password_length\" (12) bytes long"
    );
    // An empty password reaches the hook and is too short (user.c:398 runs
    // before the empty-password clearing at user.c:419).
    assert_eq!(check_plaintext("u", "", 8).expect_err("empty").message(), "password is too short");
}

#[test]
fn plaintext_user_name_and_letter_mix_arms_in_c_order() {
    // strstr(password, username) is checked before the letter mix.
    let e = check_plaintext("regress_pwc_name", "xregress_pwc_name1", 8).expect_err("contains");
    assert_eq!(e.message(), "password must not contain user name");
    assert_eq!(e.sqlstate(), ERRCODE_INVALID_PARAMETER_VALUE);
    for pw in ["abcdefgh", "12345678", "!@#$%^&*", "\u{e9}\u{e9}\u{e9}\u{e9}\u{e9}"] {
        let e = check_plaintext("u", pw, 8).expect_err(pw);
        assert_eq!(e.message(), "password must contain both letters and nonletters", "{pw}");
    }
    // Non-ASCII bytes are non-letters (isalpha on the unsigned byte), so a
    // letter plus accented characters passes.
    assert!(check_plaintext("u", "abc\u{e9}\u{e9}\u{e9}", 8).is_ok());
    assert!(check_plaintext("u", "abcdefg1", 8).is_ok());
    assert!(check_plaintext("u", "good1234", 8).is_ok());
}
