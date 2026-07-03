//! Differential test vs live PostgreSQL 18.3: LIKE / NOT LIKE / ILIKE /
//! bytea LIKE under COLLATE "C" plus like_escape, comparing results and
//! SQLSTATEs. Skips silently if no PG is reachable.

use std::io::Write;
use std::process::Command;

use adt_like::{bytealike, like_escape_into, texticlike, textlike, textnlike, IcScratch};
use types_core::catalog::C_COLLATION_OID;
use types_error::{unpack_sqlstate, PgError};
use wchar::PG_UTF8;

const STRINGS: &[&str] = &[
    "", "a", "b", "abc", "ABC", "aXc", "hello", "HELLO", "Hello World", "50%", "100%", "a_c",
    "a\\c", "\\", "%", "_", "héllo", "HÉLLO", "ÉÉ", "éé", "日本語", "日本", "abé", "abcdef",
    "aa", "aXbXc", "indio",
];

const PATTERNS: &[&str] = &[
    "", "%", "%%", "_", "__", "___", "abc", "ABC", "a%", "%c", "%b%", "%ell%", "a_c", "h_llo",
    "h__lo", "_____", "50\\%", "a\\_c", "a\\\\c", "\\%", "\\_", "\\\\", "\\", "%\\", "ab%\\",
    "abc\\", "a%c_e_", "h%x", "%_%", "a__", "é", "é%", "%é", "_é_", "É%", "héllo", "HÉLLO",
    "日%", "%語", "_本_", "___語", "%日本語%", "h_LLO", "%ELL%",
];

const ESCAPES: &[(&str, &str)] = &[
    ("50#%", "#"),
    ("a#_c", "#"),
    ("a\\c", "#"),
    ("##", "#"),
    ("#", "#"),
    ("#\\", "#"),
    ("#%#_#", "#"),
    ("%#", "#"),
    ("\\#", "#"),
    ("abc", "\\"),
    ("a\\c", "\\"),
    ("a\\c", ""),
    ("abc", ""),
    ("", ""),
    ("", "#"),
    ("é%", "é"),
    ("%é", "é"),
    ("abc", "é"),
    ("éé", "é"),
    ("日#%", "#"),
    ("50%%", "%"),
    ("a_b", "_"),
    ("abc", "xy"),
    ("abc", "éx"),
    ("abc", "xé"),
    ("abc", "##"),
];

fn err_tag(e: &PgError) -> String {
    format!("E{}", String::from_utf8(unpack_sqlstate(e.sqlstate()).to_vec()).unwrap())
}

fn sql_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

fn psql(sql: &str, fname: &str, nrows: usize) -> Option<Vec<String>> {
    let path = std::env::temp_dir().join(fname);
    std::fs::File::create(&path).ok()?.write_all(sql.as_bytes()).ok()?;
    let out = Command::new("psql")
        .args(["-h", "/tmp", "-p", "5432", "-d", "postgres", "-X"])
        .args(["-tAF", "\t", "-v", "ON_ERROR_STOP=0", "-f"])
        .arg(&path)
        .output()
        .ok()?;
    if !out.status.success() {
        eprintln!("psql failed: {}", String::from_utf8_lossy(&out.stderr));
        return None;
    }
    let mut results = vec![String::new(); nrows];
    for line in String::from_utf8_lossy(&out.stdout).lines() {
        let mut it = line.splitn(2, '\t');
        if let (Some(id), Some(r)) = (it.next(), it.next()) {
            if let Ok(id) = id.parse::<usize>() {
                if id < nrows {
                    results[id] = r.to_string();
                }
            }
        }
    }
    Some(results)
}

#[test]
fn differential_like_vs_live_pg() {
    mbutils::SetDatabaseEncoding(PG_UTF8).unwrap();
    let mut corpus: Vec<(&str, &str, u8)> = Vec::new();
    for &p in PATTERNS {
        for &s in STRINGS {
            for op in 0..4u8 {
                corpus.push((s, p, op));
            }
        }
    }

    let mut sql = String::from(
        "SET standard_conforming_strings = on;\n\
         CREATE OR REPLACE FUNCTION pg_temp.like_try(s text, p text, op int) \
         RETURNS text LANGUAGE plpgsql AS $fn$\n\
         BEGIN\n\
           IF op = 0 THEN RETURN CASE WHEN (s COLLATE \"C\") LIKE p THEN 't' ELSE 'f' END;\n\
           ELSIF op = 1 THEN RETURN CASE WHEN (s COLLATE \"C\") NOT LIKE p THEN 't' ELSE 'f' END;\n\
           ELSIF op = 2 THEN RETURN CASE WHEN (s COLLATE \"C\") ILIKE p THEN 't' ELSE 'f' END;\n\
           ELSE RETURN CASE WHEN convert_to(s, 'UTF8') LIKE convert_to(p, 'UTF8') THEN 't' ELSE 'f' END;\n\
           END IF;\n\
         EXCEPTION WHEN others THEN RETURN 'E' || SQLSTATE;\n\
         END $fn$;\n\
         SELECT v.id, pg_temp.like_try(v.s, v.p, v.op) FROM (VALUES\n",
    );
    for (i, (s, p, op)) in corpus.iter().enumerate() {
        if i > 0 {
            sql.push_str(",\n");
        }
        sql.push_str(&format!("({}, {}, {}, {})", i, sql_quote(s), sql_quote(p), op));
    }
    sql.push_str("\n) AS v(id, s, p, op) ORDER BY v.id;\n");

    let pg = match psql(&sql, "pgrust_like_diff.sql", corpus.len()) {
        Some(v) => v,
        None => {
            eprintln!("SKIP: no reachable PostgreSQL (psql -h /tmp -p 5432 -d postgres)");
            return;
        }
    };

    let ctx = mcx::MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut scratch = IcScratch::default();
    let mut mismatches = Vec::new();
    let mut compared = 0usize;
    for (i, &(s, p, op)) in corpus.iter().enumerate() {
        let expect = &pg[i];
        if expect.is_empty() {
            continue;
        }
        compared += 1;
        let r = match op {
            0 => textlike(s.as_bytes(), p.as_bytes(), C_COLLATION_OID),
            1 => textnlike(s.as_bytes(), p.as_bytes(), C_COLLATION_OID),
            2 => texticlike(mcx, s.as_bytes(), p.as_bytes(), C_COLLATION_OID, &mut scratch),
            _ => bytealike(s.as_bytes(), p.as_bytes()),
        };
        let got = match r {
            Ok(true) => "t".to_string(),
            Ok(false) => "f".to_string(),
            Err(e) => err_tag(&e),
        };
        if &got != expect {
            mismatches.push(format!("  s={s:?} p={p:?} op={op}  pg={expect}  pgrust={got}"));
        }
    }
    eprintln!("like differential: {compared} pairs compared, {} mismatches", mismatches.len());
    assert!(
        mismatches.is_empty(),
        "{} / {} mismatches vs live PG:\n{}",
        mismatches.len(),
        compared,
        mismatches.iter().take(60).cloned().collect::<Vec<_>>().join("\n")
    );
}

#[test]
fn differential_like_escape_vs_live_pg() {
    mbutils::SetDatabaseEncoding(PG_UTF8).unwrap();
    let mut sql = String::from(
        "SET standard_conforming_strings = on;\n\
         CREATE OR REPLACE FUNCTION pg_temp.esc_try(p text, e text) \
         RETURNS text LANGUAGE plpgsql AS $fn$\n\
         BEGIN RETURN 'V' || like_escape(p, e);\n\
         EXCEPTION WHEN others THEN RETURN 'E' || SQLSTATE;\n\
         END $fn$;\n\
         SELECT v.id, pg_temp.esc_try(v.p, v.e) FROM (VALUES\n",
    );
    for (i, (p, e)) in ESCAPES.iter().enumerate() {
        if i > 0 {
            sql.push_str(",\n");
        }
        sql.push_str(&format!("({}, {}, {})", i, sql_quote(p), sql_quote(e)));
    }
    sql.push_str("\n) AS v(id, p, e) ORDER BY v.id;\n");

    let pg = match psql(&sql, "pgrust_like_escape_diff.sql", ESCAPES.len()) {
        Some(v) => v,
        None => {
            eprintln!("SKIP: no reachable PostgreSQL (psql -h /tmp -p 5432 -d postgres)");
            return;
        }
    };

    let mut mismatches = Vec::new();
    let mut compared = 0usize;
    for (i, &(p, e)) in ESCAPES.iter().enumerate() {
        let expect = &pg[i];
        if expect.is_empty() {
            continue;
        }
        compared += 1;
        let mut out = Vec::new();
        let got = match like_escape_into(p.as_bytes(), e.as_bytes(), &mut out) {
            Ok(()) => format!("V{}", String::from_utf8(out).unwrap()),
            Err(err) => err_tag(&err),
        };
        if &got != expect {
            mismatches.push(format!("  p={p:?} e={e:?}  pg={expect}  pgrust={got}"));
        }
    }
    eprintln!(
        "like_escape differential: {compared} pairs compared, {} mismatches",
        mismatches.len()
    );
    assert!(
        mismatches.is_empty(),
        "like_escape mismatches vs live PG:\n{}",
        mismatches.join("\n")
    );
}
