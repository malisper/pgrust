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

    // Emit the minimal perfect hash, mirroring PostgreSQL's
    // src/tools/PerfectHash.pm (used by gen_keywordlist.pl to build
    // ScanKeywordList.hash).  This reproduces the O(1) case-insensitive
    // perfect-hash function the C uses, replacing the O(n) text scan.
    let keys: Vec<&str> = entries.iter().map(|e| e.name).collect();
    let perfect = generate_perfect_hash(&keys);
    output.push_str(&perfect);

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

// --- Minimal perfect hash generation ----------------------------------------
//
// Faithful port of PostgreSQL's `src/tools/PerfectHash.pm`, the build-time
// tool that emits `ScanKeywordList.hash`.  It constructs a minimal perfect
// hash for the keyword set using the Czech/Havas/Majewski algorithm.  The
// emitted constants drive the O(1) runtime lookup in `lib.rs`.
//
// PostgreSQL always generates the keyword hash with `case_fold => 1`, so this
// port hard-codes case-folding (OR'ing 0x20 into each byte at hash time).

/// Compute one of the two component hashes (multiply-and-add in u32).
/// `case_fold` always applies for keyword hashing.
fn calc_hash(key: &str, mult: u32, seed: u32) -> u32 {
    let mut result = seed;
    for &byte in key.as_bytes() {
        let cn = (byte | 0x20) as u32;
        result = result.wrapping_mul(mult).wrapping_add(cn);
    }
    result
}

/// Result of a successful mapping-table construction.
struct HashTable {
    /// Mapping table values (may be negative; widest int the perl picks is
    /// int32 — we store i64 here and let the emitter pick a Rust type).
    hashtab: Vec<i64>,
    nverts: u32,
}

/// Attempt to build the mapping table for the given hash parameters.
/// Returns `None` on failure (caller retries with other parameters).
fn construct_hash_table(
    keys: &[&str],
    hash_mult1: u32,
    hash_mult2: u32,
    hash_seed1: u32,
    hash_seed2: u32,
) -> Option<HashTable> {
    let nedges = keys.len();
    let mut nverts: u32 = 2 * nedges as u32 + 1;

    // Avoid table sizes that are multiples of either multiplier.
    while nverts % hash_mult1 == 0 || nverts % hash_mult2 == 0 {
        nverts += 1;
    }
    let nverts_usize = nverts as usize;

    // Edges: (left vertex, right vertex) per key.
    let mut edges: Vec<(usize, usize)> = Vec::with_capacity(nedges);
    for &kw in keys {
        let hash1 = (calc_hash(kw, hash_mult1, hash_seed1) % nverts) as usize;
        let hash2 = (calc_hash(kw, hash_mult2, hash_seed2) % nverts) as usize;
        if hash1 == hash2 {
            return None;
        }
        edges.push((hash1, hash2));
    }

    // Adjacency: for each vertex, the set of incident edge indices.
    let mut vert_edges: Vec<std::collections::BTreeSet<usize>> =
        vec![std::collections::BTreeSet::new(); nverts_usize];
    for (e, &(l, r)) in edges.iter().enumerate() {
        vert_edges[l].insert(e);
        vert_edges[r].insert(e);
    }

    // Peel degree-1 vertices, recording removal order (front-insertion so the
    // final list is in reverse order of removal, matching the perl).
    let mut output_order: std::collections::VecDeque<usize> = std::collections::VecDeque::new();
    for startv in 0..nverts_usize {
        let mut v = startv;
        while vert_edges[v].len() == 1 {
            let e = *vert_edges[v].iter().next().unwrap();
            vert_edges[v].remove(&e);

            let (l, r) = edges[e];
            let v2 = if l == v { r } else { l };
            vert_edges[v2].remove(&e);

            output_order.push_front(e);
            v = v2;
        }
    }

    // Graph must be fully peeled (acyclic) to succeed.
    if output_order.len() != nedges {
        return None;
    }

    // Assign hash-table values so that hashtab[l] + hashtab[r] == edge index.
    let mut hashtab: Vec<i64> = vec![0; nverts_usize];
    let mut visited: Vec<bool> = vec![false; nverts_usize];
    for &e in &output_order {
        let (l, r) = edges[e];
        if !visited[l] {
            hashtab[l] = e as i64 - hashtab[r];
        } else {
            assert!(!visited[r], "oops, doubly used hashtab entry");
            hashtab[r] = e as i64 - hashtab[l];
        }
        visited[l] = true;
        visited[r] = true;
    }

    // Detect value range to choose an unused-entry flag (mirrors the perl's
    // int8/int16/int32 selection; we keep i64 storage but compute the flag).
    let mut hmin: i64 = nedges as i64;
    let mut hmax: i64 = 0;
    for &v in &hashtab {
        if v < hmin {
            hmin = v;
        }
        if v > hmax {
            hmax = v;
        }
    }
    let unused_flag: i64 = if hmin >= -0x7F && hmax <= 0x7F && hmin + 0x7F >= nedges as i64 {
        0x7F
    } else if hmin >= -0x7FFF && hmax <= 0x7FFF && hmin + 0x7FFF >= nedges as i64 {
        0x7FFF
    } else if hmin >= -0x7FFF_FFFF && hmax <= 0x7FFF_FFFF && hmin + 0x3FFF_FFFF >= nedges as i64 {
        0x3FFF_FFFF
    } else {
        panic!("hash table values too wide");
    };

    for (i, v) in hashtab.iter_mut().enumerate() {
        if !visited[i] {
            *v = unused_flag;
        }
    }

    Some(HashTable { hashtab, nverts })
}

/// Generate the Rust source for the perfect-hash constants/function, mirroring
/// `PerfectHash::generate_hash_function`.
fn generate_perfect_hash(keys: &[&str]) -> String {
    // Same parameter search order as the perl tool, for reproducible results.
    let hash_mult1: u32 = 257;
    let mut found: Option<(u32, u32, u32, u32, HashTable)> = None;
    'search: for hash_seed1 in 0u32..10 {
        for hash_seed2 in 0u32..10 {
            for &hash_mult2 in &[17u32, 31, 127, 8191] {
                if let Some(table) =
                    construct_hash_table(keys, hash_mult1, hash_mult2, hash_seed1, hash_seed2)
                {
                    found = Some((hash_mult2, hash_seed1, hash_seed2, hash_mult1, table));
                    break 'search;
                }
            }
        }
    }
    let (hash_mult2, hash_seed1, hash_seed2, hash_mult1, table) =
        found.expect("failed to generate perfect hash");

    let nhash = table.nverts as usize;
    let mut out = String::new();
    out.push_str(&format!(
        "pub const SCAN_KEYWORD_HASH_MULT1: u32 = {hash_mult1};\n"
    ));
    out.push_str(&format!(
        "pub const SCAN_KEYWORD_HASH_MULT2: u32 = {hash_mult2};\n"
    ));
    out.push_str(&format!(
        "pub const SCAN_KEYWORD_HASH_SEED1: u32 = {hash_seed1};\n"
    ));
    out.push_str(&format!(
        "pub const SCAN_KEYWORD_HASH_SEED2: u32 = {hash_seed2};\n"
    ));
    out.push_str(&format!("pub const SCAN_KEYWORD_HASH_NHASH: u32 = {nhash};\n"));
    out.push_str(&format!("pub static SCAN_KEYWORD_HASH_TABLE: [i32; {nhash}] = ["));
    for (i, v) in table.hashtab.iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        out.push_str(&v.to_string());
    }
    out.push_str("];\n");
    out
}
