use std::env;
use std::error::Error;
use std::fmt::Write as _;
use std::fs;
use std::path::PathBuf;

// Mechanically extracts the bison 2.3 LALR tables from the vendored gram.c
// (byte copy of the built 18.3 checkout's generated parser) into OUT_DIR:
//   tables.rs  yytranslate/yypact/yydefact/yypgoto/yydefgoto/yytable/yycheck/
//              yyr1/yyr2 + the #define'd automaton constants
//   names.rs   yytname/yyrline/HAS_ACTION (cold: panic messages for
//              unimplemented rule actions)
fn main() -> Result<(), Box<dyn Error>> {
    let dir = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").ok_or("no CARGO_MANIFEST_DIR")?);
    let out = PathBuf::from(env::var_os("OUT_DIR").ok_or("no OUT_DIR")?);
    println!("cargo:rerun-if-changed={}", dir.join("gram.c").display());
    let src = fs::read_to_string(dir.join("gram.c"))?;

    let yyfinal: i64 = define(&src, "YYFINAL")?;
    let yylast: i64 = define(&src, "YYLAST")?;
    let yyntokens: i64 = define(&src, "YYNTOKENS")?;
    let yynnts: i64 = define(&src, "YYNNTS")?;
    let yynrules: i64 = define(&src, "YYNRULES")?;
    let yynstates: i64 = define(&src, "YYNSTATES")?;
    let yymaxutok: i64 = define(&src, "YYMAXUTOK")?;
    let yypact_ninf: i64 = define(&src, "YYPACT_NINF")?;
    let yytable_ninf: i64 = define(&src, "YYTABLE_NINF")?;

    let yytranslate = ints(&src, "yytranslate")?;
    let yypact = ints(&src, "yypact")?;
    let yydefact = ints(&src, "yydefact")?;
    let yypgoto = ints(&src, "yypgoto")?;
    let yydefgoto = ints(&src, "yydefgoto")?;
    let yytable = ints(&src, "yytable")?;
    let yycheck = ints(&src, "yycheck")?;
    let yyr1 = ints(&src, "yyr1")?;
    let yyr2 = ints(&src, "yyr2")?;
    let yyrline = ints(&src, "yyrline")?;
    let yytname = strings(&src, "yytname")?;

    assert_eq!(yytranslate.len() as i64, yymaxutok + 1);
    assert_eq!(yypact.len() as i64, yynstates);
    assert_eq!(yydefact.len() as i64, yynstates);
    assert_eq!(yypgoto.len() as i64, yynnts);
    assert_eq!(yydefgoto.len() as i64, yynnts);
    assert_eq!(yytable.len(), yycheck.len());
    assert!(yytable.len() as i64 > yylast);
    assert_eq!(yyr1.len() as i64, yynrules + 1);
    assert_eq!(yyr2.len() as i64, yynrules + 1);
    assert_eq!(yyrline.len() as i64, yynrules + 1);
    assert_eq!(yytname.len() as i64, yyntokens + yynnts);
    for (i, &v) in yytranslate.iter().enumerate() {
        assert!((0..yyntokens).contains(&v), "yytranslate[{i}]={v}");
    }
    for (i, &r) in yydefact.iter().enumerate() {
        assert!((0..=yynrules).contains(&r), "yydefact[{i}]={r}");
    }
    for (i, &s) in yyr1.iter().enumerate().skip(1) {
        assert!((yyntokens..yyntokens + yynnts).contains(&s), "yyr1[{i}]={s}");
    }
    // Every shift/goto target in yytable is a valid state; reduce entries are
    // -rule (rule 0 slots are error, never read as reductions). This licenses
    // indexing yypact/yydefact by any state the walk produces.
    for (i, &v) in yytable.iter().enumerate() {
        assert!(v < yynstates && -v <= yynrules, "yytable[{i}]={v}");
    }
    // -1 marks nonterminals whose gotos always resolve through yypgoto/yytable.
    for &g in yydefgoto.iter() {
        assert!((-1..yynstates).contains(&g), "yydefgoto={g}");
    }

    let case_rules = case_labels(&src)?;
    assert!(case_rules.len() > 2000, "action switch parse: {}", case_rules.len());
    let mut has_action = vec![false; (yynrules + 1) as usize];
    for r in case_rules {
        has_action[r as usize] = true;
    }

    let mut t = String::new();
    let _ = writeln!(t, "pub const YYFINAL: i32 = {yyfinal};");
    let _ = writeln!(t, "pub const YYLAST: i32 = {yylast};");
    let _ = writeln!(t, "pub const YYNTOKENS: i32 = {yyntokens};");
    let _ = writeln!(t, "pub const YYNRULES: usize = {yynrules};");
    let _ = writeln!(t, "pub const YYNSTATES: i32 = {yynstates};");
    let _ = writeln!(t, "pub const YYMAXUTOK: i32 = {yymaxutok};");
    let _ = writeln!(t, "pub const YYPACT_NINF: i32 = {yypact_ninf};");
    let _ = writeln!(t, "pub const YYTABLE_NINF: i32 = {yytable_ninf};");
    emit(&mut t, "YYTRANSLATE", "u16", &yytranslate);
    emit(&mut t, "YYPACT", "i32", &yypact);
    emit(&mut t, "YYDEFACT", "u16", &yydefact);
    emit(&mut t, "YYPGOTO", "i16", &yypgoto);
    emit(&mut t, "YYDEFGOTO", "i16", &yydefgoto);
    emit(&mut t, "YYTABLE", "i16", &yytable);
    emit(&mut t, "YYCHECK", "i16", &yycheck);
    emit(&mut t, "YYR1", "u16", &yyr1);
    emit(&mut t, "YYR2", "u8", &yyr2);
    fs::write(out.join("tables.rs"), t)?;

    let mut n = String::new();
    emit(&mut n, "YYRLINE", "u16", &yyrline);
    emit(&mut n, "HAS_ACTION", "bool", &has_action.iter().map(|&b| b as i64).collect::<Vec<_>>());
    let _ = writeln!(n, "pub static YYTNAME: [&str; {}] = [", yytname.len());
    for name in &yytname {
        let _ = writeln!(n, "    {name:?},");
    }
    n.push_str("];\n");
    fs::write(out.join("names.rs"), n)?;
    Ok(())
}

fn define(src: &str, name: &str) -> Result<i64, Box<dyn Error>> {
    let prefix = format!("#define {name} ");
    let start = src.find(&prefix).ok_or_else(|| format!("missing {prefix}"))? + prefix.len();
    let rest = src[start..].trim_start();
    let end = rest
        .find(|c: char| !(c.is_ascii_digit() || c == '-'))
        .unwrap_or(rest.len());
    Ok(rest[..end].parse()?)
}

fn body<'a>(src: &'a str, name: &str) -> Result<&'a str, Box<dyn Error>> {
    let marker = format!(" {name}[] =");
    let start = src.find(&marker).ok_or_else(|| format!("missing {name}"))?;
    let open = start + src[start..].find('{').ok_or("open brace")?;
    let close = open + src[open..].find("};").ok_or("close brace")?;
    Ok(&src[open + 1..close])
}

fn ints(src: &str, name: &str) -> Result<Vec<i64>, Box<dyn Error>> {
    body(src, name)?
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| Ok(s.parse()?))
        .collect()
}

fn strings(src: &str, name: &str) -> Result<Vec<String>, Box<dyn Error>> {
    let b = body(src, name)?;
    let mut out = Vec::new();
    let mut chars = b.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '"' {
            continue;
        }
        let mut s = String::new();
        loop {
            match chars.next().ok_or("unterminated yytname string")? {
                '"' => break,
                '\\' => s.push(chars.next().ok_or("dangling escape")?),
                c => s.push(c),
            }
        }
        out.push(s);
    }
    Ok(out)
}

// Rule numbers with semantic actions: the `case N:` labels of yyparse's
// reduction switch (2-space indentation is the bison 2.3 rendering; yydestruct
// and yy_symbol_value_print switches use different label shapes).
fn case_labels(src: &str) -> Result<Vec<i64>, Box<dyn Error>> {
    let start = src.find("  switch (yyn)").ok_or("missing action switch")?;
    let end = start + src[start..].find("\n/* Line ").ok_or("missing switch end")?;
    let mut out = Vec::new();
    let mut lines = src[start..end].lines().peekable();
    while let Some(line) = lines.next() {
        // Actions contain their own C switches (numeric or not); a rule label
        // is followed by the action's `#line NNN "gram.y"` marker.
        if let Some(rest) = line.trim_start().strip_prefix("case ") {
            if let Some(num) = rest.strip_suffix(':') {
                if let (Ok(n), Some(next)) = (num.trim().parse(), lines.peek()) {
                    if next.starts_with("#line ") {
                        out.push(n);
                    }
                }
            }
        }
    }
    Ok(out)
}

fn emit(dst: &mut String, name: &str, ty: &str, vals: &[i64]) {
    let _ = write!(dst, "pub static {name}: [{ty}; {}] = [", vals.len());
    for (i, v) in vals.iter().enumerate() {
        if i % 16 == 0 {
            dst.push_str("\n    ");
        }
        if ty == "bool" {
            let _ = write!(dst, "{},", *v != 0);
        } else {
            let _ = write!(dst, "{v},");
        }
    }
    dst.push_str("\n];\n");
}
