use std::collections::VecDeque;
use std::env;
use std::error::Error;
use std::fmt::Write as _;
use std::fs;
use std::path::PathBuf;

// Generates from vendored artifacts:
//   tokens.rs          gram token codes (gram_tokens.txt = bison gram.h enum)
//   keyword_tokens.rs  ScanKeywordTokens[] (kwlist.h x token codes)
//   dfa.rs             flex -CF yy_transition/yy_start_state_list from scan.c
fn main() -> Result<(), Box<dyn Error>> {
    let dir = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").ok_or("no CARGO_MANIFEST_DIR")?);
    let out = PathBuf::from(env::var_os("OUT_DIR").ok_or("no OUT_DIR")?);
    for f in ["gram_tokens.txt", "kwlist.h", "scan.c"] {
        println!("cargo:rerun-if-changed={}", dir.join(f).display());
    }

    let token_src = fs::read_to_string(dir.join("gram_tokens.txt"))?;
    let mut token_value = std::collections::HashMap::new();
    let mut tokens_rs = String::new();
    for line in token_src.lines().map(str::trim).filter(|l| !l.is_empty()) {
        let (name, value) = line.split_once('=').ok_or_else(|| format!("bad line {line}"))?;
        let (name, value): (&str, i32) = (name.trim(), value.trim().parse()?);
        token_value.insert(name.to_string(), value);
        let _ = writeln!(tokens_rs, "pub const {name}: i32 = {value};");
    }
    fs::write(out.join("tokens.rs"), tokens_rs)?;

    let kwlist = fs::read_to_string(dir.join("kwlist.h"))?;
    let mut kw_tokens: Vec<u16> = Vec::new();
    for line in kwlist.lines() {
        let Some(body) = line.trim().strip_prefix("PG_KEYWORD(") else { continue };
        let body = &body[..body.find(')').ok_or("PG_KEYWORD shape")?];
        let fields: Vec<&str> = body.split(',').map(str::trim).collect();
        let value = *token_value
            .get(fields[1])
            .ok_or_else(|| format!("token {} not in gram_tokens.txt", fields[1]))?;
        kw_tokens.push(u16::try_from(value)?);
    }
    fs::write(
        out.join("keyword_tokens.rs"),
        format!(
            "pub static SCAN_KEYWORD_TOKENS: [u16; {}] = {:?};\n",
            kw_tokens.len(),
            kw_tokens
        ),
    )?;

    let scan_c = fs::read_to_string(dir.join("scan.c"))?;
    let trans = parse_transitions(&scan_c)?;
    let starts = parse_start_states(&scan_c)?;
    let num_rules: i32 = capture(&scan_c, "#define YY_NUM_RULES ")?.parse()?;
    let eob: i32 = capture(&scan_c, "#define YY_END_OF_BUFFER ")?.parse()?;
    assert_eq!(eob, num_rules + 1);
    // Start-condition numbering the Rust State enum hardcodes.
    for (name, val) in [
        ("INITIAL", 0), ("xb", 1), ("xc", 2), ("xd", 3), ("xh", 4), ("xq", 5),
        ("xqs", 6), ("xe", 7), ("xdolq", 8), ("xui", 9), ("xus", 10), ("xeu", 11),
    ] {
        let got: i32 = capture(&scan_c, &format!("#define {name} "))?.parse()?;
        assert_eq!(got, val, "start condition {name}");
    }
    assert_eq!(starts.len(), 25);

    validate(&trans, &starts, eob);

    let mut dfa = String::new();
    let _ = writeln!(dfa, "pub const YY_NUM_RULES: i32 = {num_rules};");
    let _ = writeln!(dfa, "pub const YY_END_OF_BUFFER: i32 = {eob};");
    let _ = writeln!(dfa, "pub static YY_START_STATE: [u32; 25] = {starts:?};");
    let _ = writeln!(dfa, "pub static YY_TRANSITION: [YyTransInfo; {}] = [", trans.len());
    for chunk in trans.chunks(16) {
        dfa.push_str("    ");
        for &(v, n) in chunk {
            let _ = write!(dfa, "t({v},{n}),");
        }
        dfa.push('\n');
    }
    dfa.push_str("];\n");
    fs::write(out.join("dfa.rs"), dfa)?;
    Ok(())
}

fn capture<'a>(src: &'a str, prefix: &str) -> Result<&'a str, String> {
    let start = src.find(prefix).ok_or_else(|| format!("missing {prefix:?}"))? + prefix.len();
    let rest = src[start..].trim_start();
    let end = rest.find(|c: char| !c.is_ascii_digit()).unwrap_or(rest.len());
    Ok(&rest[..end])
}

fn parse_transitions(src: &str) -> Result<Vec<(i32, i32)>, Box<dyn Error>> {
    let marker = "static const struct yy_trans_info yy_transition[";
    let start = src.find(marker).ok_or("missing yy_transition")?;
    let count: usize = capture(&src[start..], "yy_transition[")?.parse()?;
    let body_start = start + src[start..].find('{').ok_or("open")?;
    let body_end = body_start + src[body_start..].find("};").ok_or("close")?;
    let mut trans = Vec::with_capacity(count);
    let mut it = src[body_start..body_end]
        .split(|c: char| !(c.is_ascii_digit() || c == '-'))
        .filter(|s| !s.is_empty() && *s != "-");
    while let (Some(v), Some(n)) = (it.next(), it.next()) {
        trans.push((v.parse()?, n.parse()?));
    }
    assert!(trans.len() <= count, "yy_transition entry count");
    // C zero-fills to [count]; the end-of-buffer state at the table tail then
    // needs one extra jam row so state+0..=256 stays in bounds (flex's own
    // array is one entry short of that for byte 255).
    trans.resize(count + 257, (0, 0));
    Ok(trans)
}

fn parse_start_states(src: &str) -> Result<Vec<u32>, Box<dyn Error>> {
    let marker = "yy_start_state_list[";
    let start = src.find(marker).ok_or("missing start list")?;
    let end = start + src[start..].find("};").ok_or("close")?;
    Ok(src[start..end]
        .match_indices("&yy_transition[")
        .map(|(i, m)| {
            let rest = &src[start + i + m.len()..end];
            rest[..rest.find(']').unwrap()].parse().unwrap()
        })
        .collect())
}

// Proves every state reachable from any start condition keeps state-1 and
// state+0..=256 in bounds, so the walk's unchecked indexing is sound.
fn validate(trans: &[(i32, i32)], starts: &[u32], eob: i32) {
    let len = trans.len() as i64;
    let mut seen = vec![false; trans.len()];
    let mut queue: VecDeque<i64> = starts.iter().map(|&s| s as i64).collect();
    let mut reachable = 0u32;
    while let Some(s) = queue.pop_front() {
        if seen[s as usize] {
            continue;
        }
        seen[s as usize] = true;
        reachable += 1;
        assert!(s >= 1 && s + 256 < len, "state {s} out of bounds");
        let act = trans[(s - 1) as usize].1;
        assert!((0..=eob + 12 + 1).contains(&act), "state {s} bad action {act}");
        for c in 0..=256i64 {
            let (verify, nxt) = trans[(s + c) as usize];
            if verify as i64 == c {
                queue.push_back(s + nxt as i64);
            }
        }
    }
    assert!(reachable > 200, "suspiciously few reachable states: {reachable}");
}
