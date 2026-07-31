
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
    pub debug: i32,
    pub debug_notices: Vec<String>,
    pub unexpected_binary: bool,
    pub pending_bad_mdc: bool,

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
    pub fn dbg(&mut self, msg: &str) {
        if self.debug != 0 {
            self.debug_notices.push(format!("dbg: {msg}"));
        }
    }

    /// pgp-pgsql.c parse_args + getword, ported byte-for-byte.
    ///
    /// getword's whitespace set is exactly {' ', '\t', '\n'} -- narrower
    /// than C-locale isspace(): '\r', VT (0x0b), FF (0x0c), and any
    /// non-ASCII bytes are WORD DATA.  Ground truth (PostgreSQL 18.3):
    ///   pgp_sym_encrypt('x','k', E'cipher-algo=aes256\r') -> ERROR
    ///     (Unsupported cipher algorithm: the '\r' stays in the value)
    ///   pgp_sym_encrypt('x','k', 'debug=1=2')             -> ERROR
    ///   pgp_sym_encrypt('x','k', 'convert-crlf=1,,debug=1') -> ERROR
    ///     (both "Illegal argument to function")
    ///   pgp_sym_decrypt(..., E'convert-crlf=1\x0c')       -> OK
    ///     (FF is word data; atoi("1\x0c") == 1)
    pub fn parse_args(&mut self, args: &[u8]) -> Result<(), String> {
        const ARGUMENT_ERROR: &str = "Illegal argument to function"; // PXE_ARGUMENT_ERROR

        fn is_ws(c: u8) -> bool {
            c == b' ' || c == b'\t' || c == b'\n'
        }
        // getword: skip {sp,\t,\n}; '=' and ',' are one-byte words; other
        // words run to the next {sp,\t,\n,'=',','}; skip trailing ws.
        fn getword(b: &[u8], p: &mut usize) -> (usize, usize) {
            while *p < b.len() && is_ws(b[*p]) {
                *p += 1;
            }
            let start = *p;
            if *p < b.len() && (b[*p] == b'=' || b[*p] == b',') {
                *p += 1;
            } else {
                while *p < b.len()
                    && !is_ws(b[*p])
                    && b[*p] != b'='
                    && b[*p] != b','
                {
                    *p += 1;
                }
            }
            let end = *p;
            while *p < b.len() && is_ws(b[*p]) {
                *p += 1;
            }
            (start, end)
        }

        // downcase_convert; the C string ends at the first NUL.
        let mut lower: Vec<u8> = args
            .iter()
            .map(|&c| if c.is_ascii_uppercase() { c + 32 } else { c })
            .collect();
        if let Some(n) = lower.iter().position(|&c| c == 0) {
            lower.truncate(n);
        }
        let b = &lower[..];

        let mut i = 0usize;
        while i < b.len() {
            let (ks, ke) = getword(b, &mut i);
            // C: if (*p++ != '=') break;   (PXE_ARGUMENT_ERROR)
            if i >= b.len() || b[i] != b'=' {
                return Err(ARGUMENT_ERROR.to_string());
            }
            i += 1;
            let (vs, ve) = getword(b, &mut i);
            // C: *p must be NUL or ','.
            if i < b.len() {
                if b[i] != b',' {
                    return Err(ARGUMENT_ERROR.to_string());
                }
                i += 1;
            }
            // C: if (*key == 0 || *val == 0 || val_len == 0) break;
            if ks == ke || vs == ve {
                return Err(ARGUMENT_ERROR.to_string());
            }
            let key = String::from_utf8_lossy(&b[ks..ke]).into_owned();
            let val = String::from_utf8_lossy(&b[vs..ve]).into_owned();
            self.set_arg(&key, &val)?;
        }
        Ok(())
    }

    fn set_arg(&mut self, key: &str, val: &str) -> Result<(), String> {
        // C atoi: skip C-locale isspace, optional sign, leading digits;
        // trailing junk ignored ("1\x0c" -> 1, where parse() would fail).
        let atoi = |v: &str| {
            let b = v.as_bytes();
            let mut i = 0;
            while i < b.len() && pg_string::isspace_c_locale(b[i]) {
                i += 1;
            }
            let neg = match b.get(i) {
                Some(b'-') => {
                    i += 1;
                    true
                }
                Some(b'+') => {
                    i += 1;
                    false
                }
                _ => false,
            };
            let mut acc: i64 = 0;
            while i < b.len() && b[i].is_ascii_digit() {
                acc = (acc * 10 + i64::from(b[i] - b'0')).min(i64::from(i32::MAX) + 1);
                i += 1;
            }
            let v = if neg { -acc } else { acc };
            v.clamp(i64::from(i32::MIN), i64::from(i32::MAX)) as i32
        };
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
                    return Err("Illegal argument to function".to_string());
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
            _ => return Err("Illegal argument to function".to_string()),
        }
        Ok(())
    }
}

#[cfg(test)]
mod parse_args_tests {
    use super::PgpContext;

    /// Ground truth (PostgreSQL 18.3, contrib/pgcrypto):
    ///   pgp_sym_encrypt('x','k', E'cipher-algo=aes256\r')   -> ERROR
    ///     "Unsupported cipher algorithm" ('\r' is word data in getword)
    ///   pgp_sym_encrypt('x','k', E'\rcipher-algo=aes256')   -> ERROR
    ///     "Illegal argument to function"
    ///   pgp_sym_encrypt('x','k', 'debug=1=2')                -> ERROR
    ///   pgp_sym_encrypt('x','k', 'convert-crlf=1,,debug=1')  -> ERROR
    ///   pgp_sym_decrypt(..., E'convert-crlf=1\x0c')          -> OK
    #[test]
    fn getword_whitespace_is_sp_tab_nl_only() {
        let mut c = PgpContext::default();
        // CR is word data: value "aes256\r" is not a cipher name.
        assert_eq!(
            c.parse_args(b"cipher-algo=aes256\r").unwrap_err(),
            "Unsupported cipher algorithm"
        );
        // CR in the key makes it unrecognized -> PXE_ARGUMENT_ERROR text.
        assert_eq!(
            c.parse_args(b"\rcipher-algo=aes256").unwrap_err(),
            "Illegal argument to function"
        );
        // FF is word data but atoi("1\x0c") == 1, so this succeeds.
        let mut c = PgpContext::default();
        c.parse_args(b"convert-crlf=1\x0c").unwrap();
        assert_eq!(c.convert_crlf, 1);
        // sp/tab/nl ARE skipped.
        let mut c = PgpContext::default();
        c.parse_args(b" \t\nconvert-crlf \t= \n1 , debug=1").unwrap();
        assert_eq!((c.convert_crlf, c.debug), (1, 1));
    }

    #[test]
    fn parse_args_structure_matches_c() {
        let mut c = PgpContext::default();
        assert!(c.parse_args(b"debug=1=2").is_err());
        assert!(c.parse_args(b"convert-crlf=1,,debug=1").is_err());
        assert!(c.parse_args(b"cipheralgo").is_err());
        assert!(c.parse_args(b"debug=").is_err());
        assert!(c.parse_args(b"=1").is_err());
        // Trailing comma at end of string is accepted (C loop exits on NUL).
        let mut c = PgpContext::default();
        c.parse_args(b"debug=1,").unwrap();
        assert_eq!(c.debug, 1);
        // But a trailing comma followed by whitespace is an error in C.
        assert!(c.parse_args(b"debug=1, ").is_err());
        // Empty option string is fine.
        PgpContext::default().parse_args(b"").unwrap();
        // Uppercase is downcased.
        let mut c = PgpContext::default();
        c.parse_args(b"DEBUG=1").unwrap();
        assert_eq!(c.debug, 1);
    }
}
