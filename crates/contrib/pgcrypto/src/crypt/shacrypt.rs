//! SHA-256 / SHA-512 crypt (`$5$` / `$6$`, `crypt-sha.c`), driven through the
//! `pwhash` crate's `sha256_crypt` / `sha512_crypt` (byte-identical to the
//! glibc/pgcrypto sha-crypt). Handles the optional `rounds=NNNN$` prefix and the
//! round-count bounds (`PX_SHACRYPT_ROUNDS_{MIN,MAX}` = 1000 / 999999999).

const ROUNDS_MIN: u64 = 1000;
const ROUNDS_MAX: u64 = 999_999_999;

/// `ereport(NOTICE, errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg(...))` —
/// a non-throwing client notice (crypt-sha.c's rounds-clamp diagnostics).
fn notice(msg: &str) {
    let _ = ::utils_error::ereport(::types_error::NOTICE)
        .errcode(::types_error::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
        .errmsg(msg.to_string())
        .finish(::types_error::ErrorLocation {
            filename: None,
            lineno: 0,
            funcname: None,
        });
}

/// `crypt_sha(key, setting)` — `setting` is `$5$[rounds=NNNN$]salt` (`$6$` for
/// SHA-512). A `rounds=` value of 0 / an unparsable encoding yields C's
/// `crypt(3) returned NULL`.
pub fn crypt_sha(pw: &str, setting: &str) -> Result<String, String> {
    let bytes = setting.as_bytes();
    let is_512 = bytes.starts_with(b"$6$");

    // Validate an explicit rounds= value: C clamps to [MIN, MAX] (with a NOTICE)
    // but rejects a literal 0 (`crypt(3) returned NULL`).
    let after = &setting[3..];
    if let Some(rest) = after.strip_prefix("rounds=") {
        let count_str: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
        // The count must be followed by '$'.
        if after.as_bytes().get(count_str.len() + "rounds=".len()) != Some(&b'$')
            || count_str.is_empty()
        {
            return Err("crypt(3) returned NULL".to_string());
        }
        let count: u64 = count_str.parse().unwrap_or(0);
        if count == 0 {
            return Err("crypt(3) returned NULL".to_string());
        }
        // C emits a NOTICE and clamps; `pwhash` clamps to the same value, so the
        // resulting hash matches — we only need to mirror the NOTICE.
        if count > ROUNDS_MAX {
            notice(&format!(
                "rounds={count} exceeds maximum supported value ({ROUNDS_MAX}), using {ROUNDS_MAX} instead"
            ));
        } else if count < ROUNDS_MIN {
            notice(&format!(
                "rounds={count} is below supported value ({ROUNDS_MIN}), using {ROUNDS_MIN} instead"
            ));
        }
    }

    let res = if is_512 {
        ::pwhash::sha512_crypt::hash_with(setting, pw)
    } else {
        ::pwhash::sha256_crypt::hash_with(setting, pw)
    };
    res.map_err(|_| "crypt(3) returned NULL".to_string())
}
