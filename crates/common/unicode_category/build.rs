use std::env;
use std::fmt::Write as _;
use std::fs;
use std::path::PathBuf;

// Transcribes the generated unicode_category_table.h (PG 18.3,
// generate-unicode_category_table.pl output) verbatim into Rust statics.
fn main() {
    let dir = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let src = fs::read_to_string(dir.join("unicode_category_table.h")).expect("read table header");
    println!("cargo:rerun-if-changed=unicode_category_table.h");

    let mut out = String::new();

    let ascii = parse_opt_ascii(&src);
    assert_eq!(ascii.len(), 128, "unicode_opt_ascii");
    let _ = writeln!(
        out,
        "pub static UNICODE_OPT_ASCII: [(u8, u8); 128] = {ascii:?};"
    );

    let cats = parse_category_ranges(&src, "unicode_categories[3368] =");
    assert_eq!(cats.len(), 3368, "unicode_categories");
    let _ = writeln!(
        out,
        "pub static UNICODE_CATEGORIES: [(u32, u32, u8); {}] = {cats:?};",
        cats.len()
    );

    for (name, array, count) in [
        ("UNICODE_ALPHABETIC", "unicode_alphabetic[1179] =", 1179),
        ("UNICODE_LOWERCASE", "unicode_lowercase[690] =", 690),
        ("UNICODE_UPPERCASE", "unicode_uppercase[656] =", 656),
        (
            "UNICODE_CASE_IGNORABLE",
            "unicode_case_ignorable[506] =",
            506,
        ),
        ("UNICODE_WHITE_SPACE", "unicode_white_space[11] =", 11),
        ("UNICODE_HEX_DIGIT", "unicode_hex_digit[6] =", 6),
        ("UNICODE_JOIN_CONTROL", "unicode_join_control[1] =", 1),
    ] {
        let ranges = parse_ranges(&src, array);
        assert_eq!(ranges.len(), count, "{array}");
        let _ = writeln!(
            out,
            "pub static {name}: [(u32, u32); {count}] = {ranges:?};"
        );
    }

    let out_dir = PathBuf::from(env::var_os("OUT_DIR").expect("OUT_DIR"));
    fs::write(out_dir.join("tables.rs"), out).expect("write tables.rs");
}

fn section<'a>(src: &'a str, marker: &str) -> &'a str {
    let start = src.find(marker).unwrap_or_else(|| panic!("marker {marker}"));
    let rest = &src[start..];
    let end = rest.find("\n};").expect("section end");
    &rest[..end]
}

const CATEGORIES: [&str; 30] = [
    "PG_U_UNASSIGNED",
    "PG_U_UPPERCASE_LETTER",
    "PG_U_LOWERCASE_LETTER",
    "PG_U_TITLECASE_LETTER",
    "PG_U_MODIFIER_LETTER",
    "PG_U_OTHER_LETTER",
    "PG_U_NONSPACING_MARK",
    "PG_U_ENCLOSING_MARK",
    "PG_U_SPACING_MARK",
    "PG_U_DECIMAL_NUMBER",
    "PG_U_LETTER_NUMBER",
    "PG_U_OTHER_NUMBER",
    "PG_U_SPACE_SEPARATOR",
    "PG_U_LINE_SEPARATOR",
    "PG_U_PARAGRAPH_SEPARATOR",
    "PG_U_CONTROL",
    "PG_U_FORMAT",
    "PG_U_PRIVATE_USE",
    "PG_U_SURROGATE",
    "PG_U_DASH_PUNCTUATION",
    "PG_U_OPEN_PUNCTUATION",
    "PG_U_CLOSE_PUNCTUATION",
    "PG_U_CONNECTOR_PUNCTUATION",
    "PG_U_OTHER_PUNCTUATION",
    "PG_U_MATH_SYMBOL",
    "PG_U_CURRENCY_SYMBOL",
    "PG_U_MODIFIER_SYMBOL",
    "PG_U_OTHER_SYMBOL",
    "PG_U_INITIAL_PUNCTUATION",
    "PG_U_FINAL_PUNCTUATION",
];

const PROPS: [&str; 8] = [
    "PG_U_PROP_ALPHABETIC",
    "PG_U_PROP_LOWERCASE",
    "PG_U_PROP_UPPERCASE",
    "PG_U_PROP_CASED",
    "PG_U_PROP_CASE_IGNORABLE",
    "PG_U_PROP_WHITE_SPACE",
    "PG_U_PROP_JOIN_CONTROL",
    "PG_U_PROP_HEX_DIGIT",
];

fn category_value(name: &str) -> u8 {
    CATEGORIES
        .iter()
        .position(|&c| c == name)
        .unwrap_or_else(|| panic!("category {name}")) as u8
}

fn parse_opt_ascii(src: &str) -> Vec<(u8, u8)> {
    let sec = section(src, "unicode_opt_ascii[128] =");
    let mut cats = Vec::new();
    let mut props = Vec::new();
    for line in sec.lines() {
        let line = line.trim();
        if let Some(v) = line.strip_prefix(".category = ") {
            cats.push(category_value(v.trim_end_matches(',')));
        } else if let Some(v) = line.strip_prefix(".properties = ") {
            let v = v.trim_end_matches(',').trim();
            let mut bits = 0u8;
            if v != "0" {
                for term in v.split('|') {
                    let term = term.trim();
                    let bit = PROPS
                        .iter()
                        .position(|&p| p == term)
                        .unwrap_or_else(|| panic!("property {term}"));
                    bits |= 1 << bit;
                }
            }
            props.push(bits);
        }
    }
    assert_eq!(cats.len(), props.len(), "category/properties pairing");
    cats.into_iter().zip(props).collect()
}

fn parse_category_ranges(src: &str, marker: &str) -> Vec<(u32, u32, u8)> {
    let sec = section(src, marker);
    let mut out = Vec::new();
    for line in sec.lines() {
        let line = line.trim();
        if let Some(body) = line.strip_prefix('{').and_then(|l| l.strip_suffix("},")) {
            let mut it = body.split(',').map(str::trim);
            let first = parse_hex(it.next().expect("first"));
            let last = parse_hex(it.next().expect("last"));
            let cat = category_value(it.next().expect("category"));
            out.push((first, last, cat));
        }
    }
    out
}

fn parse_ranges(src: &str, marker: &str) -> Vec<(u32, u32)> {
    let sec = section(src, marker);
    let mut out = Vec::new();
    for line in sec.lines() {
        let line = line.trim();
        if let Some(body) = line.strip_prefix('{').and_then(|l| l.strip_suffix("},")) {
            let mut it = body.split(',').map(str::trim);
            let first = parse_hex(it.next().expect("first"));
            let last = parse_hex(it.next().expect("last"));
            out.push((first, last));
        }
    }
    out
}

fn parse_hex(s: &str) -> u32 {
    let s = s.trim_start_matches("0x");
    u32::from_str_radix(s, 16).unwrap_or_else(|_| panic!("hex {s}"))
}
