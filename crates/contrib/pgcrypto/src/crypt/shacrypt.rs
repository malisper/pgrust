//! SHA-256 / SHA-512 crypt (`$5$` / `$6$`, `crypt-sha.c`) via the `pwhash`
//! crate (byte-identical to glibc/pgcrypto sha-crypt). Handles the optional
//! `rounds=NNNN$` prefix and clamps to PX_SHACRYPT_ROUNDS_{MIN,MAX}.

const ROUNDS_MIN: u64 = 1000;
const ROUNDS_MAX: u64 = 999_999_999;

// crypt-sha.c's rounds-clamp diagnostics: a non-throwing client NOTICE.
fn notice(msg: &str) {
    let _ = elog::ereport(types_error::NOTICE)
        .errcode(types_error::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
        .errmsg(msg.to_string())
        .finish(types_error::ErrorLocation { filename: None, lineno: 0, funcname: None });
}

pub fn crypt_sha(pw: &str, setting: &str) -> Result<String, String> {
    let is_512 = setting.as_bytes().starts_with(b"$6$");
    let after = &setting[3..];
    if let Some(rest) = after.strip_prefix("rounds=") {
        let count_str: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
        if after.as_bytes().get(count_str.len() + "rounds=".len()) != Some(&b'$')
            || count_str.is_empty()
        {
            return Err("crypt(3) returned NULL".to_string());
        }
        let count: u64 = count_str.parse().unwrap_or(0);
        if count == 0 {
            return Err("crypt(3) returned NULL".to_string());
        }
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
        pwhash::sha512_crypt::hash_with(setting, pw)
    } else {
        pwhash::sha256_crypt::hash_with(setting, pw)
    };
    res.map_err(|_| "crypt(3) returned NULL".to_string())
}
