//! Differential test vs live PostgreSQL 18.3 (psql -h /tmp -p 5432 -d
//! postgres). Results compare hex-encoded so embedded newlines and multibyte
//! bytes survive the psql line protocol. Skips if PG is unreachable.

use std::io::Write;
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};

use mcx::MemoryContext;

static QAI: AtomicBool = AtomicBool::new(false);

fn init_guc() {
    guc_tables::vars::quote_all_identifiers.install(guc_tables::GucVarAccessors {
        get: || QAI.load(Ordering::Relaxed),
        set: |v| QAI.store(v, Ordering::Relaxed),
    });
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn sql_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

fn psql(sql: &str) -> Option<Vec<(usize, String)>> {
    let path = std::env::temp_dir().join(format!("pgrust_quote_diff_{}.sql", std::process::id()));
    std::fs::File::create(&path)
        .ok()?
        .write_all(sql.as_bytes())
        .ok()?;
    let out = Command::new("psql")
        .args(["-h", "/tmp", "-p", "5432", "-d", "postgres", "-tAF", "\t", "-f"])
        .arg(&path)
        .output()
        .ok()?;
    if !out.status.success() {
        eprintln!("psql failed: {}", String::from_utf8_lossy(&out.stderr));
        return None;
    }
    let mut rows = Vec::new();
    for line in String::from_utf8_lossy(&out.stdout).lines() {
        let mut it = line.splitn(2, '\t');
        if let (Some(id), Some(r)) = (it.next(), it.next()) {
            if let Ok(id) = id.parse::<usize>() {
                rows.push((id, r.to_string()));
            }
        }
    }
    Some(rows)
}

fn pg_batch(func: &str, inputs: &[&str], set_guc: Option<&str>) -> Option<Vec<String>> {
    let mut sql = String::from("SET standard_conforming_strings = on;\n");
    if let Some(setting) = set_guc {
        sql.push_str(setting);
        sql.push('\n');
    }
    sql.push_str(&format!(
        "SELECT v.id, encode(convert_to({func}(v.s), 'UTF8'), 'hex') FROM (VALUES\n"
    ));
    for (i, s) in inputs.iter().enumerate() {
        if i > 0 {
            sql.push_str(",\n");
        }
        sql.push_str(&format!("({}, {})", i, sql_quote(s)));
    }
    sql.push_str("\n) AS v(id, s) ORDER BY v.id;\n");
    let rows = psql(&sql)?;
    let mut results = vec![String::new(); inputs.len()];
    for (id, r) in rows {
        if id < results.len() {
            results[id] = r;
        }
    }
    Some(results)
}

const IDENTS: &[&str] = &[
    "", "a", "abc", "_abc", "a1", "a_b_c", "_", "x0123456789", "1abc", "Abc", "ABC", "aBc",
    "a b", "a-b", "a.b", "a\"b", "\"", "\"\"", "a\"\"b", "tab\there", "new\nline",
    "select", "SELECT", "Select", "table", "from", "where", "order", "group", "limit",
    "user", "all", "and", "not", "null", "true", "false", "with", "window", "cast",
    "between", "authorization", "binary", "boolean", "cross", "left", "like", "ilike",
    "integer", "varchar", "concat", "abort", "zone", "day", "insert", "update", "delete",
    "é", "Émile", "日本語", "naïve", "識別子", "a日b",
];

const LITERALS: &[&str] = &[
    "", "abc", " ", "  spaced  ", "it's", "'", "''", "O'Reilly", "'leading", "trailing'",
    "\\", "\\\\", "a\\b", "C:\\path\\file", "mix ' and \\ both", "'\\", "\\'",
    "E", "E'x'", "line1\nline2", "tab\tsep", "100%", "under_score", "NULL",
    "é", "日本語", "naïve résumé", "日'本\\語",
];

fn rust_quote_ident(inputs: &[&str]) -> Vec<String> {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    inputs
        .iter()
        .map(|s| hex(adt_quote::quote_ident(mcx, s.as_bytes()).unwrap().data()))
        .collect()
}

fn rust_quote_literal(inputs: &[&str]) -> Vec<String> {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    inputs
        .iter()
        .map(|s| hex(adt_quote::quote_literal(mcx, s.as_bytes()).unwrap().data()))
        .collect()
}

fn compare(label: &str, inputs: &[&str], pg: &[String], rust: &[String]) -> (usize, Vec<String>) {
    let mut mismatches = Vec::new();
    let mut compared = 0;
    for (i, s) in inputs.iter().enumerate() {
        if pg[i].is_empty() {
            continue;
        }
        compared += 1;
        if pg[i] != rust[i] {
            mismatches.push(format!("  {label} input={s:?} pg={} pgrust={}", pg[i], rust[i]));
        }
    }
    (compared, mismatches)
}

#[test]
fn differential_vs_live_pg() {
    init_guc();

    let Some(pg_ident) = pg_batch("quote_ident", IDENTS, None) else {
        eprintln!("SKIP: no reachable PostgreSQL at /tmp:5432");
        let _ = rust_quote_ident(IDENTS);
        let _ = rust_quote_literal(LITERALS);
        return;
    };
    let mut total = 0;
    let mut bad = Vec::new();

    let (n, m) = compare("quote_ident", IDENTS, &pg_ident, &rust_quote_ident(IDENTS));
    total += n;
    bad.extend(m);

    if let Some(pg_all) = pg_batch("quote_ident", IDENTS, Some("SET quote_all_identifiers = on;")) {
        QAI.store(true, Ordering::Relaxed);
        let rust = rust_quote_ident(IDENTS);
        QAI.store(false, Ordering::Relaxed);
        let (n, m) = compare("quote_ident[all]", IDENTS, &pg_all, &rust);
        total += n;
        bad.extend(m);
    }

    for func in ["quote_literal", "quote_nullable"] {
        if let Some(pg_lit) = pg_batch(func, LITERALS, None) {
            let (n, m) = compare(func, LITERALS, &pg_lit, &rust_quote_literal(LITERALS));
            total += n;
            bad.extend(m);
        }
    }

    if let Some(rows) = psql(
        "SELECT 0, encode(convert_to(quote_nullable(NULL::text), 'UTF8'), 'hex');\n",
    ) {
        total += 1;
        let expect = hex(b"NULL");
        if rows.first().map(|(_, r)| r.as_str()) != Some(expect.as_str()) {
            bad.push(format!("  quote_nullable(NULL) pg={rows:?} pgrust={expect}"));
        }
    }

    eprintln!("differential: {total} cases compared, {} mismatches", bad.len());
    assert!(bad.is_empty(), "{} mismatches:\n{}", bad.len(), bad.join("\n"));
}
