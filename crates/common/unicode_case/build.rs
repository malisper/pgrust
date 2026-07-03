use std::env;
use std::fmt::Write as _;
use std::fs;
use std::path::PathBuf;

// Transcribes the generated unicode_case_table.h (PG 18.3,
// generate-unicode_case_table.pl output) verbatim into Rust statics,
// including the case_index() dispatch ranges recovered from its body.
fn main() {
    let dir = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let src = fs::read_to_string(dir.join("unicode_case_table.h")).expect("read table header");
    println!("cargo:rerun-if-changed=unicode_case_table.h");

    let mut out = String::new();

    let special = parse_special_case(&src);
    assert_eq!(special.len(), 106, "special_case");
    let _ = writeln!(
        out,
        "pub static SPECIAL_CASE: [(i16, [[u32; 3]; 4]); 106] = {special:?};"
    );

    for (name, marker) in [
        ("CASE_MAP_LOWER", "case_map_lower[1704] ="),
        ("CASE_MAP_TITLE", "case_map_title[1704] ="),
        ("CASE_MAP_UPPER", "case_map_upper[1704] ="),
        ("CASE_MAP_FOLD", "case_map_fold[1704] ="),
    ] {
        let vals = parse_scalar_array(&src, marker);
        assert_eq!(vals.len(), 1704, "{marker}");
        let _ = writeln!(out, "pub static {name}: [u32; 1704] = {vals:?};");
    }

    let map_special = parse_scalar_array(&src, "case_map_special[1704] =");
    assert_eq!(map_special.len(), 1704, "case_map_special");
    assert!(map_special.iter().all(|&v| v < 106));
    let special_u8: Vec<u8> = map_special.iter().map(|&v| v as u8).collect();
    let _ = writeln!(
        out,
        "pub static CASE_MAP_SPECIAL: [u8; 1704] = {special_u8:?};"
    );

    let case_map = parse_scalar_array(&src, "case_map[4778] =");
    assert_eq!(case_map.len(), 4778, "case_map");
    assert!(case_map.iter().all(|&v| v < 1704));
    let map_u16: Vec<u16> = case_map.iter().map(|&v| v as u16).collect();
    let _ = writeln!(out, "pub static CASE_MAP: [u16; 4778] = {map_u16:?};");

    let ranges = parse_case_index_ranges(&src, case_map.len());
    let _ = writeln!(
        out,
        "pub static CASE_INDEX_RANGES: [(u32, u32, u16); {}] = {ranges:?};",
        ranges.len()
    );

    let out_dir = PathBuf::from(env::var_os("OUT_DIR").expect("OUT_DIR"));
    fs::write(out_dir.join("tables.rs"), out).expect("write tables.rs");
}

fn section<'a>(src: &'a str, marker: &str) -> &'a str {
    let start = src.find(marker).unwrap_or_else(|| panic!("marker {marker}"));
    let rest = &src[start..];
    let end = rest.find("\n};").expect("section end");
    &rest[..end]
}

fn parse_int(s: &str) -> u32 {
    let s = s.trim();
    if let Some(hex) = s.strip_prefix("0x") {
        u32::from_str_radix(hex, 16).unwrap_or_else(|_| panic!("hex {s}"))
    } else {
        s.parse().unwrap_or_else(|_| panic!("int {s}"))
    }
}

fn parse_scalar_array(src: &str, marker: &str) -> Vec<u32> {
    let sec = section(src, marker);
    let mut out = Vec::new();
    for line in sec.lines().skip(2) {
        let line = line.trim();
        let val = match line.split(',').next() {
            Some(v) if !v.is_empty() && !v.starts_with('/') && !v.starts_with('{') => v,
            _ => continue,
        };
        if val.chars().next().is_some_and(|c| c.is_ascii_digit()) {
            out.push(parse_int(val));
        }
    }
    out
}

fn parse_special_case(src: &str) -> Vec<(i16, [[u32; 3]; 4])> {
    let sec = section(src, "special_case[106] =");
    let kinds = ["[CaseLower] = ", "[CaseTitle] = ", "[CaseUpper] = ", "[CaseFold] = "];
    let mut out = Vec::new();
    for line in sec.lines() {
        let line = line.trim();
        let Some(body) = line.strip_prefix('{') else {
            continue;
        };
        if body.trim().is_empty() {
            continue;
        }
        let conditions: i16 = if body.starts_with("PG_U_FINAL_SIGMA") {
            1
        } else if body.starts_with('0') {
            0
        } else {
            panic!("conditions in {line}");
        };
        let mut map = [[0u32; 3]; 4];
        if body.contains("[CaseLower]") {
            for (k, label) in kinds.iter().enumerate() {
                let pos = body.find(label).unwrap_or_else(|| panic!("{label} in {line}"));
                let triple = &body[pos + label.len()..];
                let triple = triple
                    .strip_prefix('{')
                    .and_then(|t| t.split('}').next())
                    .unwrap_or_else(|| panic!("triple in {line}"));
                for (i, v) in triple.split(',').enumerate() {
                    map[k][i] = parse_int(v);
                }
            }
        } else {
            // Positional zero row: {0, {{0,0,0},{0,0,0},{0,0,0}}}
            assert!(
                body.replace(['{', '}', ',', ' '], "").chars().all(|c| c == '0'),
                "unexpected positional entry {line}"
            );
        }
        out.push((conditions, map));
    }
    out
}

fn parse_case_index_ranges(src: &str, case_map_len: usize) -> Vec<(u32, u32, u16)> {
    let start = src.find("case_index(pg_wchar cp)").expect("case_index body");
    let body = &src[start..];

    // Fast-path bound: "if (cp < 0xNNNN)\n\t{\n\t\treturn case_map[cp];"
    let fast_end = body
        .split("return case_map[cp];")
        .next()
        .and_then(|head| head.rfind("if (cp < 0x").map(|p| &head[p + 11..]))
        .and_then(|s| s.split(')').next())
        .map(|s| u32::from_str_radix(s.trim(), 16).expect("fast bound"))
        .expect("fast path");

    let mut pairs: Vec<(u32, u32)> = Vec::new();
    for piece in body.split("case_map[cp - 0x").skip(1) {
        let (start_hex, rest) = piece.split_once(' ').expect("range start");
        let offset = rest
            .strip_prefix("+ ")
            .and_then(|r| r.split(']').next())
            .expect("range offset");
        pairs.push((
            u32::from_str_radix(start_hex, 16).expect("range start hex"),
            offset.trim().parse().expect("range offset int"),
        ));
    }
    assert!(pairs.windows(2).all(|w| w[0].1 < w[1].1), "offsets monotonic");
    assert_eq!(pairs.first().map(|p| p.1), Some(fast_end), "fast-path length");

    let mut out = Vec::new();
    for (i, &(start, offset)) in pairs.iter().enumerate() {
        let next = pairs.get(i + 1).map_or(case_map_len as u32, |p| p.1);
        let len = next - offset;
        out.push((start, start + len, offset as u16));
    }
    out.insert(0, (0, fast_end, 0));
    out
}
