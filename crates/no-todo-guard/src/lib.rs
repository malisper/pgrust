//! Durable guard against `todo!()` / `unimplemented!()` re-entering the tree.
//!
//! This crate exposes a single function, [`scan_workspace`], that walks every
//! `crates/*/src/**.rs` file and reports each *real* `todo!(` or
//! `unimplemented!(` macro invocation. "Real" means it is not inside a line
//! comment (`//`, `//!`, `///`), a block comment (`/* ... */`), or a string /
//! char literal (including raw strings).
//!
//! The actual gate lives in `tests/no_todo.rs` so it runs under
//! `cargo test --workspace`.

use std::fs;
use std::path::{Path, PathBuf};

/// A single offending macro invocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hit {
    pub file: PathBuf,
    pub line: usize,
    pub macro_name: &'static str,
}

impl std::fmt::Display for Hit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}: {}!", self.file.display(), self.line, self.macro_name)
    }
}

/// Locate the workspace root (the directory containing the top-level Cargo.toml
/// with `[workspace]`). We start from this crate's manifest dir and walk up.
pub fn workspace_root() -> PathBuf {
    let mut dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    loop {
        let candidate = dir.join("Cargo.toml");
        if candidate.is_file() {
            if let Ok(contents) = fs::read_to_string(&candidate) {
                if contents.contains("[workspace]") {
                    return dir;
                }
            }
        }
        if !dir.pop() {
            // Fell off the top; default to two levels up from the manifest dir
            // (crates/no-todo-guard -> repo root).
            return PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .and_then(|p| p.parent())
                .map(|p| p.to_path_buf())
                .unwrap_or_else(|| PathBuf::from("."));
        }
    }
}

/// Scan the whole workspace and return every real offending invocation.
pub fn scan_workspace() -> Vec<Hit> {
    let root = workspace_root();
    let crates_dir = root.join("crates");
    let mut hits = Vec::new();
    let mut rs_files = Vec::new();
    if let Ok(entries) = fs::read_dir(&crates_dir) {
        for entry in entries.flatten() {
            let src = entry.path().join("src");
            if src.is_dir() {
                collect_rs(&src, &mut rs_files);
            }
        }
    }
    rs_files.sort();
    for file in rs_files {
        if let Ok(contents) = fs::read_to_string(&file) {
            scan_text(&contents, &file, &mut hits);
        }
    }
    hits
}

fn collect_rs(dir: &Path, out: &mut Vec<PathBuf>) {
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                collect_rs(&path, out);
            } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
                out.push(path);
            }
        }
    }
}

/// Scan a single file's text. Tracks comment/string/char state across the whole
/// file (so multi-line block comments and multi-line raw strings are handled)
/// and records the 1-based line on which each real macro token starts.
fn scan_text(text: &str, file: &Path, hits: &mut Vec<Hit>) {
    let bytes = text.as_bytes();
    let n = bytes.len();
    // 1-based line numbers, computed lazily from byte offsets.
    let line_at = |offset: usize| -> usize { bytes[..offset].iter().filter(|&&b| b == b'\n').count() + 1 };

    let mut i = 0usize;
    while i < n {
        let c = bytes[i];

        // Line comment: // ... (covers //, ///, //!)
        if c == b'/' && i + 1 < n && bytes[i + 1] == b'/' {
            while i < n && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }

        // Block comment: /* ... */ (Rust block comments nest)
        if c == b'/' && i + 1 < n && bytes[i + 1] == b'*' {
            let mut depth = 1usize;
            i += 2;
            while i < n && depth > 0 {
                if bytes[i] == b'/' && i + 1 < n && bytes[i + 1] == b'*' {
                    depth += 1;
                    i += 2;
                } else if bytes[i] == b'*' && i + 1 < n && bytes[i + 1] == b'/' {
                    depth -= 1;
                    i += 2;
                } else {
                    i += 1;
                }
            }
            continue;
        }

        // Raw string: r"...", r#"..."#, br"...", etc. Detect an optional b
        // prefix, then r, then #*, then ".
        if let Some(after) = try_raw_string_start(bytes, i) {
            let (hashes, content_start) = after;
            // Find closing "###... matching hashes count.
            i = skip_raw_string(bytes, content_start, hashes);
            continue;
        }

        // Normal / byte string: "..." or b"..."
        if c == b'"' || (c == b'b' && i + 1 < n && bytes[i + 1] == b'"') {
            let start = if c == b'b' { i + 1 } else { i };
            i = skip_string(bytes, start + 1);
            continue;
        }

        // Char / byte-char literal: '.' or b'.' — but beware lifetimes ('a).
        if c == b'\'' || (c == b'b' && i + 1 < n && bytes[i + 1] == b'\'') {
            let quote = if c == b'b' { i + 1 } else { i };
            if let Some(end) = try_char_literal(bytes, quote) {
                i = end;
                continue;
            }
            // Not a char literal (likely a lifetime); just advance one.
            i += 1;
            continue;
        }

        // Macro detection. Match identifier `todo` or `unimplemented` that is
        // (a) at a token boundary (prev char not an ident char), and
        // (b) followed (after optional whitespace) by `!` then `(`.
        if c == b't' || c == b'u' {
            for name in ["todo", "unimplemented"] {
                let nb = name.as_bytes();
                if matches_ident(bytes, i, nb) {
                    let mut j = i + nb.len();
                    // After the identifier we require `!` then `(` (allowing
                    // whitespace before `!` is not valid Rust, so don't).
                    if j < n && bytes[j] == b'!' {
                        j += 1;
                        // Allow whitespace between ! and (.
                        while j < n && bytes[j].is_ascii_whitespace() {
                            j += 1;
                        }
                        if j < n && (bytes[j] == b'(' || bytes[j] == b'[' || bytes[j] == b'{') {
                            hits.push(Hit {
                                file: file.to_path_buf(),
                                line: line_at(i),
                                macro_name: if name == "todo" { "todo" } else { "unimplemented" },
                            });
                            i += nb.len();
                            break;
                        }
                    }
                }
            }
        }

        i += 1;
    }
}

/// Does `bytes[i..]` start with `ident` at a token boundary (prev byte, if any,
/// is not an identifier char)?
fn matches_ident(bytes: &[u8], i: usize, ident: &[u8]) -> bool {
    if i + ident.len() > bytes.len() {
        return false;
    }
    if &bytes[i..i + ident.len()] != ident {
        return false;
    }
    // Preceding char must not be an identifier char.
    if i > 0 && is_ident_byte(bytes[i - 1]) {
        return false;
    }
    // Following char (the `!`) is checked by the caller; here just ensure the
    // char right after the ident is not another ident byte (so `todoX` fails).
    let after = i + ident.len();
    if after < bytes.len() && is_ident_byte(bytes[after]) {
        return false;
    }
    true
}

fn is_ident_byte(b: u8) -> bool {
    b == b'_' || b.is_ascii_alphanumeric()
}

/// If a raw string starts at `i`, return (hash_count, content_start_after_quote).
fn try_raw_string_start(bytes: &[u8], i: usize) -> Option<(usize, usize)> {
    let n = bytes.len();
    let mut j = i;
    // Optional b prefix for raw byte strings (br"...").
    if bytes[j] == b'b' && j + 1 < n {
        j += 1;
    }
    if j >= n || bytes[j] != b'r' {
        return None;
    }
    // Ensure this `r` is the start of a token (prev char not ident), so `for` /
    // `var` don't trip it. The b-prefix case already implies a boundary check
    // below since we look at the char before `i`.
    if i > 0 && is_ident_byte(bytes[i - 1]) {
        return None;
    }
    j += 1; // past 'r'
    let mut hashes = 0usize;
    while j < n && bytes[j] == b'#' {
        hashes += 1;
        j += 1;
    }
    if j < n && bytes[j] == b'"' {
        Some((hashes, j + 1))
    } else {
        None
    }
}

/// Skip a raw string body. `start` points just past the opening quote.
fn skip_raw_string(bytes: &[u8], start: usize, hashes: usize) -> usize {
    let n = bytes.len();
    let mut i = start;
    while i < n {
        if bytes[i] == b'"' {
            // Check for `hashes` trailing '#'.
            let mut k = i + 1;
            let mut count = 0;
            while k < n && count < hashes && bytes[k] == b'#' {
                count += 1;
                k += 1;
            }
            if count == hashes {
                return k;
            }
        }
        i += 1;
    }
    n
}

/// Skip a normal/byte string body. `start` points just past the opening quote.
fn skip_string(bytes: &[u8], start: usize) -> usize {
    let n = bytes.len();
    let mut i = start;
    while i < n {
        match bytes[i] {
            b'\\' => i += 2, // escape: skip next char
            b'"' => return i + 1,
            _ => i += 1,
        }
    }
    n
}

/// If `quote` points at a `'` that opens a char/byte-char literal, return the
/// index just past the closing `'`. Returns None for lifetimes like `'a`.
fn try_char_literal(bytes: &[u8], quote: usize) -> Option<usize> {
    let n = bytes.len();
    let mut i = quote + 1;
    if i >= n {
        return None;
    }
    if bytes[i] == b'\\' {
        // Escaped char: '\n', '\\', '\'', '\u{..}', etc. Scan to closing quote.
        i += 1;
        // Handle \u{...}
        if i < n && bytes[i] == b'u' {
            while i < n && bytes[i] != b'\'' {
                i += 1;
            }
            if i < n && bytes[i] == b'\'' {
                return Some(i + 1);
            }
            return None;
        }
        i += 1; // the escaped char
        if i < n && bytes[i] == b'\'' {
            return Some(i + 1);
        }
        return None;
    }
    // Single (non-escaped) char then closing quote => char literal.
    if i + 1 < n && bytes[i + 1] == b'\'' {
        return Some(i + 2);
    }
    // Otherwise it's a lifetime (e.g. 'a, 'static) — not a literal.
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    fn hits_for(src: &str) -> Vec<Hit> {
        let mut hits = Vec::new();
        scan_text(src, Path::new("x.rs"), &mut hits);
        hits
    }

    #[test]
    fn detects_real_todo() {
        let h = hits_for("fn f() { todo!() }\n");
        assert_eq!(h.len(), 1);
        assert_eq!(h[0].macro_name, "todo");
        assert_eq!(h[0].line, 1);
    }

    #[test]
    fn detects_real_unimplemented_with_args() {
        let h = hits_for("fn f() {\n    unimplemented!(\"x\")\n}\n");
        assert_eq!(h.len(), 1);
        assert_eq!(h[0].macro_name, "unimplemented");
        assert_eq!(h[0].line, 2);
    }

    #[test]
    fn ignores_line_comment() {
        assert!(hits_for("// todo!() here\n").is_empty());
        assert!(hits_for("//! unimplemented!() doc\n").is_empty());
        assert!(hits_for("/// todo!()\n").is_empty());
        assert!(hits_for("let x = 1; // unimplemented!(...)\n").is_empty());
    }

    #[test]
    fn ignores_block_comment() {
        assert!(hits_for("/* todo!() */\n").is_empty());
        assert!(hits_for("/* multi\n todo!()\n line */\n").is_empty());
        assert!(hits_for("/* outer /* todo!() */ still */\n").is_empty());
    }

    #[test]
    fn ignores_string_literal() {
        assert!(hits_for("let s = \"todo!()\";\n").is_empty());
        assert!(hits_for("let s = \"call unimplemented!(x)\";\n").is_empty());
        assert!(hits_for("let s = \"esc \\\" todo!()\";\n").is_empty());
    }

    #[test]
    fn ignores_raw_string() {
        assert!(hits_for("let s = r\"todo!()\";\n").is_empty());
        assert!(hits_for("let s = r#\"todo!() and \"quote\"#;\n").is_empty());
        assert!(hits_for("let s = br#\"unimplemented!()\"#;\n").is_empty());
    }

    #[test]
    fn ignores_substring_idents() {
        assert!(hits_for("fn mytodo() {}\nlet todone = 1;\n").is_empty());
        assert!(hits_for("xunimplemented!()\n").is_empty());
    }

    #[test]
    fn lifetime_does_not_break_scan() {
        let h = hits_for("fn f<'a>(x: &'a str) { todo!() }\n");
        assert_eq!(h.len(), 1);
    }

    #[test]
    fn char_literal_with_quote_then_todo() {
        let h = hits_for("let c = '\\'';\n todo!();\n");
        assert_eq!(h.len(), 1);
        assert_eq!(h[0].line, 2);
    }

    #[test]
    fn macro_in_string_after_real_one() {
        // First a real one, then one inside a string: only the real one counts.
        let h = hits_for("todo!();\nlet s = \"todo!()\";\n");
        assert_eq!(h.len(), 1);
        assert_eq!(h[0].line, 1);
    }
}
