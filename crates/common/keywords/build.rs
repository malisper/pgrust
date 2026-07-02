use std::env;
use std::fmt::Write as _;
use std::fs;
use std::path::PathBuf;

// Transcribes the generated kwlist_d.h (gen_keywordlist.pl output for 18.3)
// verbatim: kw_string, kw_offsets, the perfect-hash h[] table and its
// multipliers/modulus, plus categories/bare-label flags from kwlist.h.
fn main() {
    let dir = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let kwlist_d = fs::read_to_string(dir.join("kwlist_d.h")).expect("read kwlist_d.h");
    let kwlist = fs::read_to_string(dir.join("kwlist.h")).expect("read kwlist.h");
    println!("cargo:rerun-if-changed=kwlist_d.h");
    println!("cargo:rerun-if-changed=kwlist.h");

    let keywords = parse_kw_string(&kwlist_d);
    let offsets = parse_i64_array(&kwlist_d, "ScanKeywords_kw_offsets[] =");
    let h_table = parse_i64_array(&kwlist_d, "static const int16 h[");

    let num_keywords: usize = capture(&kwlist_d, "#define SCANKEYWORDS_NUM_KEYWORDS ")
        .trim()
        .parse()
        .expect("num_keywords");
    let mult_a: u32 = capture(&kwlist_d, "a = a * ").parse().expect("mult_a");
    let mult_b: u32 = capture(&kwlist_d, "b = b * ").parse().expect("mult_b");
    let hash_mod: u32 = capture(&kwlist_d, "return h[a % ").parse().expect("hash_mod");

    let entries = parse_kwlist(&kwlist);
    assert_eq!(keywords.len(), num_keywords, "kw_string vs NUM_KEYWORDS");
    assert_eq!(offsets.len(), num_keywords, "kw_offsets vs NUM_KEYWORDS");
    assert_eq!(entries.len(), num_keywords, "kwlist.h vs NUM_KEYWORDS");
    assert_eq!(h_table.len(), hash_mod as usize, "h[] vs modulus");
    for (kw, e) in keywords.iter().zip(&entries) {
        assert_eq!(kw, &e.name, "kwlist_d.h / kwlist.h keyword order");
    }
    let max_kw_len = keywords.iter().map(|k| k.len()).max().unwrap();

    let mut kw_string: Vec<u8> = Vec::new();
    for (kw, &off) in keywords.iter().zip(&offsets) {
        assert_eq!(kw_string.len(), off as usize, "offset mismatch for {kw}");
        kw_string.extend_from_slice(kw.as_bytes());
        kw_string.push(0);
    }
    // Pad so the compare loop can never index past the buffer.
    kw_string.extend(std::iter::repeat(0).take(max_kw_len + 1));

    let mut out = String::new();
    let _ = writeln!(out, "pub const SCANKEYWORDS_NUM_KEYWORDS: usize = {num_keywords};");
    let _ = writeln!(out, "pub const SCANKEYWORDS_MAX_KW_LEN: usize = {max_kw_len};");
    let _ = writeln!(out, "pub const KW_HASH_MULT_A: u32 = {mult_a};");
    let _ = writeln!(out, "pub const KW_HASH_MULT_B: u32 = {mult_b};");
    let _ = writeln!(out, "pub const KW_HASH_MOD: u32 = {hash_mod};");
    let _ = writeln!(out, "pub static KW_STRING: [u8; {}] = {:?};", kw_string.len(), kw_string);
    let offs: Vec<u16> = offsets.iter().map(|&o| o as u16).collect();
    let _ = writeln!(out, "pub static KW_OFFSETS: [u16; {}] = {:?};", offs.len(), offs);
    let h16: Vec<i16> = h_table.iter().map(|&v| v as i16).collect();
    let _ = writeln!(out, "pub static KW_HASH_H: [i16; {}] = {:?};", h16.len(), h16);
    let _ = writeln!(out, "pub static KEYWORD_TEXT: [&str; {num_keywords}] = {keywords:?};");
    let cats: Vec<u8> = entries.iter().map(|e| e.category).collect();
    let _ = writeln!(
        out,
        "pub static KEYWORD_CATEGORIES: [crate::KeywordCategory; {num_keywords}] = unsafe {{ core::mem::transmute::<[u8; {num_keywords}], [crate::KeywordCategory; {num_keywords}]>({cats:?}) }};"
    );
    let bare: Vec<bool> = entries.iter().map(|e| e.bare_label).collect();
    let _ = writeln!(out, "pub static KEYWORD_BARE_LABEL: [bool; {num_keywords}] = {bare:?};");

    let out_path = PathBuf::from(env::var_os("OUT_DIR").expect("OUT_DIR")).join("keywords.rs");
    fs::write(out_path, out).expect("write keywords.rs");
}

fn capture<'a>(src: &'a str, prefix: &str) -> &'a str {
    let start = src.find(prefix).unwrap_or_else(|| panic!("missing {prefix:?}")) + prefix.len();
    let rest = &src[start..];
    let end = rest
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(rest.len());
    &rest[..end]
}

fn parse_kw_string(src: &str) -> Vec<String> {
    let start = src.find("ScanKeywords_kw_string[] =").expect("kw_string");
    let end = start + src[start..].find(';').expect("kw_string end");
    let mut out = Vec::new();
    for frag in src[start..end].split('"').skip(1).step_by(2) {
        for kw in frag.split("\\0") {
            if !kw.is_empty() {
                assert!(kw.bytes().all(|b| b.is_ascii_lowercase() || b == b'_'), "{kw}");
                out.push(kw.to_string());
            }
        }
    }
    out
}

fn parse_i64_array(src: &str, marker: &str) -> Vec<i64> {
    let start = src.find(marker).unwrap_or_else(|| panic!("missing {marker:?}"));
    let start = start + src[start..].find('{').expect("array open");
    let end = start + src[start..].find('}').expect("array close");
    src[start + 1..end]
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| s.parse().unwrap_or_else(|e| panic!("bad number {s:?}: {e}")))
        .collect()
}

struct Entry {
    name: String,
    category: u8,
    bare_label: bool,
}

fn parse_kwlist(src: &str) -> Vec<Entry> {
    src.lines()
        .filter_map(|line| {
            let body = line.trim().strip_prefix("PG_KEYWORD(")?;
            let body = &body[..body.find(')').expect("PG_KEYWORD close")];
            let fields: Vec<&str> = body.split(',').map(str::trim).collect();
            assert_eq!(fields.len(), 4, "PG_KEYWORD shape: {line}");
            // Category values: src/include/common/keywords.h.
            let category = match fields[2] {
                "UNRESERVED_KEYWORD" => 0,
                "COL_NAME_KEYWORD" => 1,
                "TYPE_FUNC_NAME_KEYWORD" => 2,
                "RESERVED_KEYWORD" => 3,
                other => panic!("unknown category {other}"),
            };
            let bare_label = match fields[3] {
                "BARE_LABEL" => true,
                "AS_LABEL" => false,
                other => panic!("unknown label kind {other}"),
            };
            Some(Entry {
                name: fields[0].trim_matches('"').to_string(),
                category,
                bare_label,
            })
        })
        .collect()
}
