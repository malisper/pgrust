use std::env;
use std::fs;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manifest_dir = PathBuf::from(
        env::var_os("CARGO_MANIFEST_DIR").ok_or("main: CARGO_MANIFEST_DIR not set")?,
    );
    let kwlist_path = manifest_dir.join("kwlist.h");
    println!("cargo:rerun-if-changed={}", kwlist_path.display());

    let kwlist = fs::read_to_string(&kwlist_path)
        .map_err(|e| format!("main: cannot read {}: {e}", kwlist_path.display()))?;
    let entries = parse_keywords(&kwlist);

    let mut kw_string = Vec::new();
    let mut offsets = Vec::with_capacity(entries.len());
    let mut max_len = 0;

    for entry in &entries {
        offsets.push(kw_string.len() as u16);
        kw_string.extend_from_slice(entry.name.as_bytes());
        kw_string.push(0);
        max_len = max_len.max(entry.name.len());
    }

    let mut output = String::new();
    output.push_str(&format!(
        "pub const SCANKEYWORDS_NUM_KEYWORDS: usize = {};\n",
        entries.len()
    ));
    output.push_str(&format!(
        "pub const SCANKEYWORDS_MAX_KW_LEN: usize = {};\n",
        max_len
    ));
    output.push_str("pub static SCAN_KEYWORDS_KW_STRING: [u8; ");
    output.push_str(&kw_string.len().to_string());
    output.push_str("] = [");
    for (idx, byte) in kw_string.iter().enumerate() {
        if idx > 0 {
            output.push_str(", ");
        }
        output.push_str(&byte.to_string());
    }
    output.push_str("];\n");

    output.push_str("pub static SCAN_KEYWORDS_KW_OFFSETS: [u16; ");
    output.push_str(&offsets.len().to_string());
    output.push_str("] = [");
    for (idx, offset) in offsets.iter().enumerate() {
        if idx > 0 {
            output.push_str(", ");
        }
        output.push_str(&offset.to_string());
    }
    output.push_str("];\n");

    output.push_str("pub static SCAN_KEYWORD_TEXT: [&str; ");
    output.push_str(&entries.len().to_string());
    output.push_str("] = [");
    for (idx, entry) in entries.iter().enumerate() {
        if idx > 0 {
            output.push_str(", ");
        }
        output.push('"');
        output.push_str(entry.name);
        output.push('"');
    }
    output.push_str("];\n");

    output.push_str("pub static SCAN_KEYWORD_CATEGORIES: [pg_ffi_fgram::KeywordCategory; ");
    output.push_str(&entries.len().to_string());
    output.push_str("] = [");
    for (idx, entry) in entries.iter().enumerate() {
        if idx > 0 {
            output.push_str(", ");
        }
        output.push_str(entry.category.rust_variant());
    }
    output.push_str("];\n");

    output.push_str("pub static SCAN_KEYWORD_BARE_LABEL: [bool; ");
    output.push_str(&entries.len().to_string());
    output.push_str("] = [");
    for (idx, entry) in entries.iter().enumerate() {
        if idx > 0 {
            output.push_str(", ");
        }
        output.push_str(if entry.bare_label { "true" } else { "false" });
    }
    output.push_str("];\n");

    let out_path =
        PathBuf::from(env::var_os("OUT_DIR").ok_or("main: OUT_DIR not set")?).join("keywords.rs");
    fs::write(&out_path, output)
        .map_err(|e| format!("main: cannot write {}: {e}", out_path.display()))?;
    Ok(())
}

fn parse_keywords(input: &str) -> Vec<Entry<'_>> {
    input
        .lines()
        .filter_map(|line| {
            let line = line.trim();
            let body = line.strip_prefix("PG_KEYWORD(")?;
            let body = &body[..body
                .find(')')
                .unwrap_or_else(|| panic!("unexpected PG_KEYWORD shape: {line}"))];
            let fields: Vec<_> = body.split(',').map(str::trim).collect();
            assert_eq!(fields.len(), 4, "unexpected PG_KEYWORD shape: {line}");

            Some(Entry {
                name: fields[0].trim_matches('"'),
                category: KeywordCategoryToken::parse(fields[2]),
                bare_label: match fields[3] {
                    "BARE_LABEL" => true,
                    "AS_LABEL" => false,
                    other => panic!("unknown bare-label token {other}"),
                },
            })
        })
        .collect()
}

struct Entry<'a> {
    name: &'a str,
    category: KeywordCategoryToken,
    bare_label: bool,
}

enum KeywordCategoryToken {
    Unreserved,
    ColumnName,
    TypeOrFunctionName,
    Reserved,
}

impl KeywordCategoryToken {
    fn parse(token: &str) -> Self {
        match token {
            "UNRESERVED_KEYWORD" => Self::Unreserved,
            "COL_NAME_KEYWORD" => Self::ColumnName,
            "TYPE_FUNC_NAME_KEYWORD" => Self::TypeOrFunctionName,
            "RESERVED_KEYWORD" => Self::Reserved,
            other => panic!("unknown keyword category {other}"),
        }
    }

    fn rust_variant(&self) -> &'static str {
        match self {
            Self::Unreserved => "pg_ffi_fgram::KeywordCategory::Unreserved",
            Self::ColumnName => "pg_ffi_fgram::KeywordCategory::ColumnName",
            Self::TypeOrFunctionName => "pg_ffi_fgram::KeywordCategory::TypeOrFunctionName",
            Self::Reserved => "pg_ffi_fgram::KeywordCategory::Reserved",
        }
    }
}
