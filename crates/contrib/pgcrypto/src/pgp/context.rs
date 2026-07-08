//! `PGP_Context` — the encrypt/decrypt options struct + `parse_args`
//! (pgp-pgsql.c's set_arg/parse_args + the NOTICE-emitting expect-* checks).

use super::consts::*;

#[derive(Clone)]
pub struct PgpContext {
    pub cipher_algo: i32,
    pub s2k_cipher_algo: i32,
    pub s2k_mode: i32,
    pub s2k_count: i32,
    pub s2k_digest_algo: i32,
    pub compress_algo: i32,
    pub compress_level: i32,
    pub disable_mdc: i32,
    pub use_sess_key: i32,
    pub convert_crlf: i32,
    pub unicode_mode: i32,
    pub text_mode: i32,
    /// `debug=1` — emit the `dbg:` NOTICE trace.
    pub debug: i32,
    /// Accumulated `dbg:` NOTICE lines (emitted by the SQL wrapper).
    pub debug_notices: Vec<String>,
    /// Set when a text-mode decrypt meets a binary ('b') literal packet → the
    /// "Not text data" error after the pipeline completes.
    pub unexpected_binary: bool,
    /// Deferred "mdcbuf_finish: bad MDC pkt hdr" debug line (emitted after the
    /// literal-data parse to match C's filter ordering).
    pub pending_bad_mdc: bool,

    // expect-* debug fields (decrypt side); -1 = unset.
    pub expect: bool,
    pub exp_cipher_algo: i32,
    pub exp_s2k_mode: i32,
    pub exp_s2k_count: i32,
    pub exp_s2k_cipher_algo: i32,
    pub exp_s2k_digest_algo: i32,
    pub exp_compress_algo: i32,
    pub exp_use_sess_key: i32,
    pub exp_disable_mdc: i32,
    pub exp_unicode_mode: i32,
}

impl Default for PgpContext {
    fn default() -> PgpContext {
        PgpContext {
            cipher_algo: PGP_SYM_AES_128,
            s2k_cipher_algo: -1,
            s2k_mode: PGP_S2K_ISALTED,
            s2k_count: -1,
            s2k_digest_algo: PGP_DIGEST_SHA1,
            compress_algo: PGP_COMPR_NONE,
            compress_level: 6,
            disable_mdc: 0,
            use_sess_key: 0,
            convert_crlf: 0,
            unicode_mode: 0,
            text_mode: 0,
            debug: 0,
            debug_notices: Vec::new(),
            unexpected_binary: false,
            pending_bad_mdc: false,
            expect: false,
            exp_cipher_algo: -1,
            exp_s2k_mode: -1,
            exp_s2k_count: -1,
            exp_s2k_cipher_algo: -1,
            exp_s2k_digest_algo: -1,
            exp_compress_algo: -1,
            exp_use_sess_key: -1,
            exp_disable_mdc: -1,
            exp_unicode_mode: -1,
        }
    }
}

impl PgpContext {
    /// `px_debug` — record a `dbg:` NOTICE line when `debug=1`.
    pub fn dbg(&mut self, msg: &str) {
        if self.debug != 0 {
            self.debug_notices.push(format!("dbg: {msg}"));
        }
    }

    /// `parse_args` — the comma-separated `key=val` option string.
    pub fn parse_args(&mut self, args: &[u8]) -> Result<(), String> {
        // downcase
        let lower: Vec<u8> = args
            .iter()
            .map(|&c| {
                if c.is_ascii_uppercase() {
                    c + 32
                } else {
                    c
                }
            })
            .collect();
        let s = String::from_utf8_lossy(&lower);
        // split into key=val pairs on ',', words trimmed of whitespace.
        for pair in s.split(',') {
            let pair = pair.trim();
            if pair.is_empty() {
                continue;
            }
            let (key, val) = pair.split_once('=').ok_or("pgp_decrypt error")?;
            let key = key.trim();
            let val = val.trim();
            if key.is_empty() || val.is_empty() {
                return Err("pgp error".to_string());
            }
            self.set_arg(key, val)?;
        }
        Ok(())
    }

    fn set_arg(&mut self, key: &str, val: &str) -> Result<(), String> {
        let atoi = |v: &str| v.parse::<i32>().unwrap_or(0);
        match key {
            "cipher-algo" => {
                self.cipher_algo = cipher_code(val).ok_or(UNSUPPORTED_CIPHER.to_string())?;
            }
            "disable-mdc" => self.disable_mdc = atoi(val),
            "sess-key" => self.use_sess_key = atoi(val),
            "s2k-mode" => {
                let m = atoi(val);
                if m != 0 && m != 1 && m != 3 {
                    return Err("Unsupported S2K mode".to_string());
                }
                self.s2k_mode = m;
            }
            "s2k-count" => {
                let c = atoi(val);
                if !(1024..=65011712).contains(&c) {
                    return Err("argument error".to_string());
                }
                self.s2k_count = c;
            }
            "s2k-digest-algo" => {
                self.s2k_digest_algo = digest_code(val).ok_or(UNSUPPORTED_HASH.to_string())?;
            }
            "s2k-cipher-algo" => {
                self.s2k_cipher_algo = cipher_code(val).ok_or(UNSUPPORTED_CIPHER.to_string())?;
            }
            "compress-algo" => self.compress_algo = atoi(val),
            "compress-level" => self.compress_level = atoi(val),
            "convert-crlf" => self.convert_crlf = atoi(val),
            "unicode-mode" => self.unicode_mode = atoi(val),
            "debug" => self.debug = atoi(val),
            "expect-cipher-algo" => {
                self.expect = true;
                self.exp_cipher_algo = cipher_code(val).unwrap_or(-1);
            }
            "expect-disable-mdc" => {
                self.expect = true;
                self.exp_disable_mdc = atoi(val);
            }
            "expect-sess-key" => {
                self.expect = true;
                self.exp_use_sess_key = atoi(val);
            }
            "expect-s2k-mode" => {
                self.expect = true;
                self.exp_s2k_mode = atoi(val);
            }
            "expect-s2k-count" => {
                self.expect = true;
                self.exp_s2k_count = atoi(val);
            }
            "expect-s2k-digest-algo" => {
                self.expect = true;
                self.exp_s2k_digest_algo = digest_code(val).unwrap_or(-1);
            }
            "expect-s2k-cipher-algo" => {
                self.expect = true;
                self.exp_s2k_cipher_algo = cipher_code(val).unwrap_or(-1);
            }
            "expect-compress-algo" => {
                self.expect = true;
                self.exp_compress_algo = atoi(val);
            }
            "expect-unicode-mode" => {
                self.expect = true;
                self.exp_unicode_mode = atoi(val);
            }
            _ => return Err("argument error".to_string()),
        }
        Ok(())
    }
}
