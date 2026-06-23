//! Build script for `backend-parser-scan`.
//!
//! Generates two pieces of authoritative, bison-derived data:
//!
//!   * `tokens.rs` — the grammar token-code constants (e.g. `IDENT = 258`),
//!     transcribed verbatim from `gram_tokens.txt`.  That file is the
//!     `enum yytokentype` extracted from the bison-generated `gram.h` for
//!     PostgreSQL 18.3's `gram.y`, so the numbers match exactly what gram.y
//!     will use (single-char tokens keep their ASCII value; named tokens are
//!     numbered from 258 in declaration order, exactly as `scanner.h`
//!     promises: "IDENT = 258 and so on").
//!
//!   * `keyword_tokens.rs` — `ScanKeywordTokens[]`, the `const uint16` array
//!     scan.l exports.  It maps the zero-based keyword index returned by
//!     `ScanKeywordLookup` (i.e. the ASCII-sorted `kwlist.h` order) to the
//!     bison token number of that keyword.  Built from `kwlist.h` + the token
//!     table, mirroring scan.l's `PG_KEYWORD(kwname, value, ...) value,`.

use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::fs;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn Error>> {
    let manifest_dir = PathBuf::from(
        env::var_os("CARGO_MANIFEST_DIR")
            .ok_or("main: CARGO_MANIFEST_DIR not set in build environment")?,
    );
    let out_dir = PathBuf::from(
        env::var_os("OUT_DIR").ok_or("main: OUT_DIR not set in build environment")?,
    );

    let tokens_path = manifest_dir.join("gram_tokens.txt");
    let kwlist_path = manifest_dir.join("kwlist.h");
    println!("cargo:rerun-if-changed={}", tokens_path.display());
    println!("cargo:rerun-if-changed={}", kwlist_path.display());

    let token_src = fs::read_to_string(&tokens_path)
        .map_err(|e| format!("main: could not read {}: {e}", tokens_path.display()))?;
    let mut token_value: HashMap<String, i32> = HashMap::new();
    let mut token_consts = String::new();
    token_consts.push_str("// Generated from gram_tokens.txt (bison gram.h enum yytokentype).\n");
    for line in token_src.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        // Each line is "NAME = NUMBER".
        let (name, value) = line
            .split_once('=')
            .ok_or_else(|| format!("main: unexpected token line: {line}"))?;
        let name = name.trim();
        let value: i32 = value
            .trim()
            .parse()
            .map_err(|e| format!("main: bad token number in line {line:?}: {e}"))?;
        token_value.insert(name.to_string(), value);
        token_consts.push_str(&format!("pub const {name}: i32 = {value};\n"));
    }
    fs::write(out_dir.join("tokens.rs"), token_consts)
        .map_err(|e| format!("main: could not write tokens.rs: {e}"))?;

    // ScanKeywordTokens[]: keyword index -> bison token value.
    let kwlist = fs::read_to_string(&kwlist_path)
        .map_err(|e| format!("main: could not read {}: {e}", kwlist_path.display()))?;
    let mut entries: Vec<u16> = Vec::new();
    for line in kwlist.lines() {
        let line = line.trim();
        let Some(body) = line.strip_prefix("PG_KEYWORD(") else {
            continue;
        };
        let body = &body[..body
            .find(')')
            .ok_or_else(|| format!("main: unexpected PG_KEYWORD shape: {line}"))?];
        let fields: Vec<&str> = body.split(',').map(str::trim).collect();
        assert_eq!(fields.len(), 4, "unexpected PG_KEYWORD shape: {line}");
        let token_name = fields[1];
        let value = *token_value
            .get(token_name)
            .ok_or_else(|| format!("main: keyword token {token_name} not found in token table"))?;
        entries.push(
            u16::try_from(value)
                .map_err(|e| format!("main: token value {value} for {token_name} out of u16 range: {e}"))?,
        );
    }

    let mut kw = String::new();
    kw.push_str(&format!(
        "// Generated from kwlist.h. Maps ScanKeywordLookup index -> bison token.\n\
         pub static SCAN_KEYWORD_TOKENS: [u16; {}] = [",
        entries.len()
    ));
    for (idx, value) in entries.iter().enumerate() {
        if idx > 0 {
            kw.push_str(", ");
        }
        kw.push_str(&value.to_string());
    }
    kw.push_str("];\n");
    fs::write(out_dir.join("keyword_tokens.rs"), kw)
        .map_err(|e| format!("main: could not write keyword_tokens.rs: {e}"))?;
    Ok(())
}
