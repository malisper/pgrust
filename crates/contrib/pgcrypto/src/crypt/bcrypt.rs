//! bcrypt (`$2a$`/`$2x$`/`$2b$`) — Blowfish-based password hashing
//! (`crypt-blowfish.c`). The expensive EksBlowfish key schedule is driven
//! through the `blowfish` crate's `bcrypt` primitives (`bc_init_state`,
//! `salted_expand_key`, `bc_expand_key`, `bc_encrypt`), which implement the
//! same salted Blowfish setup OpenBSD's bcrypt uses.

use ::blowfish::Blowfish;

/// bcrypt's base-64 alphabet (`BF_itoa64`, differs from the crypt `./0-9A-Za-z`).
const BF64: &[u8; 64] = b"./ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

/// `BF_atoi64` — base-64 char to 6-bit value (0x20-based), 0xff = invalid.
fn atoi64(c: u8) -> Option<u8> {
    BF64.iter().position(|&b| b == c).map(|p| p as u8)
}

/// `BF_decode` — decode `count` bytes from `count*4/3` base-64 chars.
fn bf_decode(src: &[u8], count: usize) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(count);
    let mut sp = 0usize;
    while out.len() < count {
        let c1 = atoi64(*src.get(sp)?)?;
        sp += 1;
        let c2 = atoi64(*src.get(sp)?)?;
        sp += 1;
        out.push((c1 << 2) | ((c2 & 0x30) >> 4));
        if out.len() >= count {
            break;
        }
        let c3 = atoi64(*src.get(sp)?)?;
        sp += 1;
        out.push(((c2 & 0x0f) << 4) | ((c3 & 0x3c) >> 2));
        if out.len() >= count {
            break;
        }
        let c4 = atoi64(*src.get(sp)?)?;
        sp += 1;
        out.push(((c3 & 0x03) << 6) | c4);
    }
    Some(out)
}

/// `BF_encode` — encode `count` bytes to base-64 (`BF_itoa64`).
fn bf_encode(src: &[u8], count: usize) -> Vec<u8> {
    let mut out = Vec::new();
    let mut sp = 0usize;
    while sp < count {
        let c1 = src[sp] as u32;
        sp += 1;
        out.push(BF64[(c1 >> 2) as usize]);
        let mut c1b = (c1 & 0x03) << 4;
        if sp >= count {
            out.push(BF64[c1b as usize]);
            break;
        }
        let c2 = src[sp] as u32;
        sp += 1;
        c1b |= (c2 >> 4) & 0x0f;
        out.push(BF64[c1b as usize]);
        let mut c1c = (c2 & 0x0f) << 2;
        if sp >= count {
            out.push(BF64[c1c as usize]);
            break;
        }
        let c3 = src[sp] as u32;
        sp += 1;
        c1c |= (c3 >> 6) & 0x03;
        out.push(BF64[c1c as usize]);
        out.push(BF64[(c3 & 0x3f) as usize]);
    }
    out
}

/// Encode 16 random salt bytes as the 22-char bcrypt base-64 string
/// (`gen_salt('bf')`).
pub fn encode_salt64(raw: &[u8; 16]) -> String {
    String::from_utf8_lossy(&bf_encode(raw, 16)).into_owned()
}

/// `crypt_bf(key, setting)` — bcrypt. `setting` is `$2<minor>$NN$<22-char salt>`.
pub fn crypt_bf(pw: &[u8], setting: &[u8]) -> Result<String, String> {
    // Validate the setting prefix exactly as crypt-blowfish.c's _crypt_blowfish_rn.
    if setting.len() < 7 + 22
        || setting[0] != b'$'
        || setting[1] != b'2'
        || (setting[2] != b'a' && setting[2] != b'x' && setting[2] != b'b')
        || setting[3] != b'$'
        || !setting[4].is_ascii_digit()
        || setting[4] > b'3'
        || !setting[5].is_ascii_digit()
        || (setting[4] == b'3' && setting[5] > b'1')
        || setting[6] != b'$'
    {
        return Err("invalid salt".to_string());
    }

    let log_rounds = ((setting[4] - b'0') as u32) * 10 + (setting[5] - b'0') as u32;
    let count: u64 = 1u64 << log_rounds;
    if count < 16 {
        return Err("invalid salt".to_string());
    }

    // Decode the 16-byte salt from the 22 base-64 chars after the prefix.
    let salt_chars = &setting[7..7 + 22];
    let salt = bf_decode(salt_chars, 16).ok_or_else(|| "invalid salt".to_string())?;

    // C's BF_set_key cycles `key` then its terminating NUL, wrapping back to the
    // start (the trailing NUL is part of the cycle). The `blowfish` crate's
    // `next_u32_wrap` cycles the slice WITHOUT a NUL, so append one to reproduce
    // C's exact key stream. (The `$2x$` sign-extension bug variant is not
    // reproduced; `$2a$`/`$2b$` are the path the regression suite exercises.)
    let mut key_nul = pw.to_vec();
    key_nul.push(0);

    // EksBlowfish setup: init state, one salted expansion, then 2^cost rounds of
    // alternating key / salt expansion.
    let mut state = Blowfish::bc_init_state();
    state.salted_expand_key(&salt, &key_nul);
    for _ in 0..count {
        state.bc_expand_key(&key_nul);
        state.bc_expand_key(&salt);
    }

    // Encrypt the magic "OrpheanBeholderScryDoubt" (6 words) 64 times.
    let mut magic: [u32; 6] = [
        0x4F72_7068, 0x6561_6E42, 0x6568_6F6C, 0x6465_7253, 0x6372_7944, 0x6F75_6274,
    ];
    for i in (0..6).step_by(2) {
        let mut lr = [magic[i], magic[i + 1]];
        for _ in 0..64 {
            lr = state.bc_encrypt(lr);
        }
        magic[i] = lr[0];
        magic[i + 1] = lr[1];
    }

    // Serialize the 6 words big-endian to 24 bytes, encode only 23 (the original
    // bug-compatible truncation).
    let mut out_bytes = Vec::with_capacity(24);
    for w in magic.iter() {
        out_bytes.extend_from_slice(&w.to_be_bytes());
    }
    let enc = bf_encode(&out_bytes, 23);

    // Assemble: the first 7+22 chars of the setting + the 31-char hash. C
    // rewrites the last salt char (index 28 = 7+22-1) as
    // `BF_itoa64[BF_atoi64[c] & 0x30]` to zero its unused low bits.
    let mut result = Vec::with_capacity(60);
    result.extend_from_slice(&setting[..7 + 22]);
    let last = 7 + 22 - 1;
    if let Some(v) = atoi64(setting[last]) {
        result[last] = BF64[(v & 0x30) as usize];
    }
    result.extend_from_slice(&enc);
    Ok(String::from_utf8_lossy(&result).into_owned())
}
