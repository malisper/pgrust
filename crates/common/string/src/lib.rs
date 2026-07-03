// MCXT_ALLOC_NO_OOM would return None in C; Rust allocation aborts instead.
pub fn pg_clean_ascii(s: &str, _alloc_flags: i32) -> Option<String> {
    let mut dst = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        if !(32..=126).contains(&b) {
            dst.push_str(&format!("\\x{b:02x}"));
        } else {
            dst.push(b as char);
        }
    }
    Some(dst)
}

pub fn init_seams() {
    string_seams::pg_clean_ascii::set(pg_clean_ascii);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn passthrough_clean_ascii() {
        assert_eq!(pg_clean_ascii("psql", 0).unwrap(), "psql");
        assert_eq!(pg_clean_ascii("", 0).unwrap(), "");
    }

    #[test]
    fn hex_escapes_non_printables() {
        assert_eq!(pg_clean_ascii("a\x1fb", 0).unwrap(), "a\\x1fb");
        assert_eq!(pg_clean_ascii("\x7f", 0).unwrap(), "\\x7f");
        assert_eq!(pg_clean_ascii("caf\u{e9}", 0).unwrap(), "caf\\xc3\\xa9");
        assert_eq!(pg_clean_ascii("\t\n", 0).unwrap(), "\\x09\\x0a");
    }
}
