//! Differential test: text_substring / textpos / split_part / string_agg vs
//! live PostgreSQL 18.3 (PGHOST=/tmp port 5432). Values compare byte-exact;
//! errors compare SQLSTATE + primary message via plpgsql SQLSTATE/SQLERRM.
//! Skips silently if PG is unreachable.
//! Not miri-runnable: shells out to psql.
#![cfg(not(miri))]

use std::io::Write;
use std::process::Command;

use datum::Datum;
use mcx::MemoryContext;
use types_core::C_COLLATION_OID;
use types_error::unpack_sqlstate;
use types_fmgr::{AggStateNode, LocalFcinfo};
use varlena::{split_part, text_position, text_substring};

const OK: &str = "OK:";
const ERR: &str = "ERR:";

fn set_utf8() {
    mbutils::SetDatabaseEncoding(wchar::PG_UTF8).unwrap();
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        mbutils::init_seams();
        detoast::init_seams();
    });
}

fn render<T: std::fmt::Display>(r: Result<T, Box<types_error::PgError>>) -> String {
    match r {
        Ok(v) => format!("{OK}{v}"),
        Err(e) => format!(
            "{ERR}{}:{}",
            std::str::from_utf8(&unpack_sqlstate(e.sqlstate())).unwrap(),
            e.message
        ),
    }
}

fn sql_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

fn psql(sql: &str) -> Option<Vec<String>> {
    static SEQ: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let path = std::env::temp_dir().join(format!(
        "pgrust_varlena_diff_{}_{}.sql",
        std::process::id(),
        SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    std::fs::File::create(&path).ok()?.write_all(sql.as_bytes()).ok()?;
    let out = Command::new("psql")
        .env("PGHOST", "/tmp")
        .env("PGCLIENTENCODING", "UTF8")
        .args(["-p", "5432", "-d", "postgres", "-tAF", "\t", "-f"])
        .arg(&path)
        .output()
        .ok()?;
    let _ = std::fs::remove_file(&path);
    if !out.status.success() {
        eprintln!("psql failed: {}", String::from_utf8_lossy(&out.stderr));
        return None;
    }
    let mut rows = Vec::new();
    for line in String::from_utf8(out.stdout).ok()?.lines() {
        let mut it = line.splitn(2, '\t');
        if let (Some(id), Some(r)) = (it.next(), it.next()) {
            if let Ok(id) = id.parse::<usize>() {
                while rows.len() <= id {
                    rows.push(String::new());
                }
                rows[id] = r.to_string();
            }
        }
    }
    Some(rows)
}

const STRINGS: &[&str] = &[
    "",
    "a",
    "hello",
    "hello world",
    "abcabcabc",
    "é",
    "café",
    "日本語",
    "日本語abc日本語",
    "a😀b😀c",
    "😀",
    "xxxxxxxxxx",
    "The quick brown fox jumps over the lazy dog",
    "ααββγγ",
    "mixed日本ascii語text",
];

#[test]
fn substr_differential() {
    set_utf8();
    let starts: &[i32] = &[i32::MIN, -2147483647, -100, -2, -1, 0, 1, 2, 3, 5, 100, 2147483646, i32::MAX];
    let lens: &[i32] = &[i32::MIN, -100, -1, 0, 1, 2, 3, 100, 2147483646, i32::MAX];
    let mut corpus: Vec<(&str, i32, Option<i32>)> = Vec::new();
    for &s in STRINGS {
        for &st in starts {
            corpus.push((s, st, None));
            for &l in lens {
                corpus.push((s, st, Some(l)));
            }
        }
    }

    let mut sql = String::from(
        "SET client_min_messages = warning;\n\
         CREATE OR REPLACE FUNCTION pg_temp.try_substr(s text, st int, l int) \
         RETURNS text LANGUAGE plpgsql AS $fn$\n\
         BEGIN\n\
           IF l IS NULL THEN RETURN 'OK:' || substr(s, st); END IF;\n\
           RETURN 'OK:' || substr(s, st, l);\n\
         EXCEPTION WHEN others THEN RETURN 'ERR:' || SQLSTATE || ':' || SQLERRM;\n\
         END $fn$;\n\
         SELECT v.id, pg_temp.try_substr(v.s, v.st, v.l) FROM (VALUES\n",
    );
    for (i, (s, st, l)) in corpus.iter().enumerate() {
        if i > 0 {
            sql.push_str(",\n");
        }
        let l = l.map_or("NULL::int".to_string(), |l| format!("{l}"));
        sql.push_str(&format!("({i}, {}, {st}, {l})", sql_quote(s)));
    }
    sql.push_str("\n) AS v(id, s, st, l) ORDER BY v.id;\n");

    let Some(pg) = psql(&sql) else {
        eprintln!("SKIP: no reachable PostgreSQL");
        return;
    };

    let ctx = MemoryContext::new("diff");
    let mcx = ctx.mcx();
    let mut checked = 0;
    let mut mismatches = Vec::new();
    for (i, (s, st, l)) in corpus.iter().enumerate() {
        let mut img = vec![0u8; 4];
        img.extend_from_slice(s.as_bytes());
        let hdr = datum::varlena::set_varsize_4b(img.len());
        img[..4].copy_from_slice(&hdr);
        let got = render(
            text_substring(mcx, &img, *st, l.unwrap_or(-1), l.is_none())
                .map(|v| String::from_utf8(v.data().to_vec()).unwrap()),
        );
        checked += 1;
        if got != pg[i] {
            mismatches.push(format!("  substr({s:?},{st},{l:?})  pg={}  pgrust={got}", pg[i]));
        }
    }
    eprintln!("substr differential: {checked} cases, {} mismatches", mismatches.len());
    assert!(mismatches.is_empty(), "\n{}", mismatches.join("\n"));
}

#[test]
fn position_differential() {
    set_utf8();
    let needles: &[&str] = &[
        "", "a", "b", "z", "ab", "bc", "abc", "abcabc", "hello", "world", "xx", "xxx",
        "é", "本", "語", "日本語", "😀", "😀c", "quick brown", "notpresent",
        "longer-than-any-haystack-string-here-really", "ascii語",
    ];
    let mut corpus: Vec<(&str, &str)> = Vec::new();
    for &h in STRINGS {
        for &n in needles {
            corpus.push((h, n));
        }
    }

    let mut sql = String::from("SELECT v.id, 'OK:' || position(v.n in v.h) FROM (VALUES\n");
    for (i, (h, n)) in corpus.iter().enumerate() {
        if i > 0 {
            sql.push_str(",\n");
        }
        sql.push_str(&format!("({i}, {}, {})", sql_quote(h), sql_quote(n)));
    }
    sql.push_str("\n) AS v(id, h, n) ORDER BY v.id;\n");

    let Some(pg) = psql(&sql) else {
        eprintln!("SKIP: no reachable PostgreSQL");
        return;
    };

    let mut mismatches = Vec::new();
    for (i, (h, n)) in corpus.iter().enumerate() {
        let got = render(text_position(h.as_bytes(), n.as_bytes(), C_COLLATION_OID));
        if got != pg[i] {
            mismatches.push(format!("  position({n:?} in {h:?})  pg={}  pgrust={got}", pg[i]));
        }
    }
    eprintln!("position differential: {} cases, {} mismatches", corpus.len(), mismatches.len());
    assert!(mismatches.is_empty(), "\n{}", mismatches.join("\n"));
}

#[test]
fn split_part_differential() {
    set_utf8();
    let inputs: &[&str] = &[
        "",
        "abc",
        "abc~@~def~@~ghi",
        "a,b,c,d,e",
        ",leading",
        "trailing,",
        ",,",
        "a,,b",
        "日、本、語",
        "no-sep-here",
        "😀|a|😀",
        "xxxx",
    ];
    let seps: &[&str] = &["", ",", "~@~", "、", "|", "x", "xx", "notfound"];
    let fields: &[i32] = &[i32::MIN, -100, -5, -3, -2, -1, 0, 1, 2, 3, 5, 100, i32::MAX];
    let mut corpus: Vec<(&str, &str, i32)> = Vec::new();
    for &s in inputs {
        for &sep in seps {
            for &f in fields {
                corpus.push((s, sep, f));
            }
        }
    }

    let mut sql = String::from(
        "SET client_min_messages = warning;\n\
         CREATE OR REPLACE FUNCTION pg_temp.try_split(s text, sep text, f int) \
         RETURNS text LANGUAGE plpgsql AS $fn$\n\
         BEGIN\n\
           RETURN 'OK:' || split_part(s, sep, f);\n\
         EXCEPTION WHEN others THEN RETURN 'ERR:' || SQLSTATE || ':' || SQLERRM;\n\
         END $fn$;\n\
         SELECT v.id, pg_temp.try_split(v.s, v.sep, v.f) FROM (VALUES\n",
    );
    for (i, (s, sep, f)) in corpus.iter().enumerate() {
        if i > 0 {
            sql.push_str(",\n");
        }
        sql.push_str(&format!("({i}, {}, {}, {f})", sql_quote(s), sql_quote(sep)));
    }
    sql.push_str("\n) AS v(id, s, sep, f) ORDER BY v.id;\n");

    let Some(pg) = psql(&sql) else {
        eprintln!("SKIP: no reachable PostgreSQL");
        return;
    };

    let ctx = MemoryContext::new("diff");
    let mcx = ctx.mcx();
    let mut mismatches = Vec::new();
    for (i, (s, sep, f)) in corpus.iter().enumerate() {
        let got = render(
            split_part(mcx, s.as_bytes(), sep.as_bytes(), *f, C_COLLATION_OID)
                .map(|v| String::from_utf8(v.data().to_vec()).unwrap()),
        );
        if got != pg[i] {
            mismatches.push(format!(
                "  split_part({s:?},{sep:?},{f})  pg={}  pgrust={got}",
                pg[i]
            ));
        }
    }
    eprintln!("split_part differential: {} cases, {} mismatches", corpus.len(), mismatches.len());
    assert!(mismatches.is_empty(), "\n{}", mismatches.join("\n"));
}

fn text_image(s: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(4 + s.len());
    v.extend_from_slice(&datum::varlena::set_varsize_4b(4 + s.len()));
    v.extend_from_slice(s);
    v
}

fn rust_string_agg(rows: &[Option<&str>], delim: Option<&str>) -> Option<String> {
    let agg_ctx = MemoryContext::new_bump("aggcontext");
    let mut node = AggStateNode::new(agg_ctx);
    let result_ctx = MemoryContext::new_bump("per-tuple");
    let delim_img = delim.map(|d| text_image(d.as_bytes()));
    let mut state = Datum::null();
    let mut state_null = true;
    for row in rows {
        let mut fcinfo = LocalFcinfo::<3>::new(0);
        fcinfo.context = node.fm_node_ptr();
        if !state_null {
            fcinfo.set_arg(0, state);
        }
        let img = row.map(|v| text_image(v.as_bytes()));
        if let Some(img) = &img {
            fcinfo.set_arg(1, Datum::from_usize(img.as_ptr() as usize));
        }
        if let Some(d) = &delim_img {
            fcinfo.set_arg(2, Datum::from_usize(d.as_ptr() as usize));
        }
        state = varlena::builtins::fc_string_agg_transfn(None, &mut fcinfo).unwrap();
        state_null = fcinfo.isnull;
    }
    let mut fcinfo = LocalFcinfo::<1>::new(0);
    fcinfo.context = node.fm_node_ptr();
    // SAFETY: result_ctx outlives the call below.
    unsafe { fcinfo.set_result_mcx(result_ctx.mcx()) };
    if !state_null {
        fcinfo.set_arg(0, state);
    }
    let d = varlena::builtins::fc_string_agg_finalfn(None, &mut fcinfo).unwrap();
    if fcinfo.isnull {
        return None;
    }
    // SAFETY: the finalfn result is a live 4B-header varlena in result_ctx.
    let bytes =
        unsafe { datum::VarlenaRef::from_ptr(d.as_usize() as *const u8) }.data().to_vec();
    Some(String::from_utf8(bytes).unwrap())
}

#[test]
fn string_agg_differential() {
    set_utf8();
    let corpus: Vec<(Vec<Option<&str>>, Option<&str>)> = vec![
        (vec![Some("a"), Some("b"), Some("c")], Some(",")),
        (vec![Some("a"), None, Some("c")], Some("+")),
        (vec![None, None], Some(",")),
        (vec![], Some(",")),
        (vec![Some("solo")], Some("~@~")),
        (vec![Some(""), Some(""), Some("")], Some(",")),
        (vec![Some("日本"), Some("語"), Some("😀")], Some("、")),
        (vec![Some("x"), Some("y")], None),
        (vec![None, Some("first-non-null"), Some("z")], Some("|")),
        (vec![Some("a'b"), Some("c''d")], Some("'")),
    ];

    let mut sql = String::from("SELECT v.id, CASE WHEN r IS NULL THEN '<null>' ELSE 'OK:' || r END FROM (\n");
    let mut parts = Vec::new();
    for (i, (rows, delim)) in corpus.iter().enumerate() {
        let elems: Vec<String> = rows
            .iter()
            .map(|r| r.map_or("NULL::text".to_string(), sql_quote))
            .collect();
        let arr = if elems.is_empty() { "ARRAY[]::text[]".to_string() } else { format!("ARRAY[{}]", elems.join(",")) };
        let d = delim.map_or("NULL::text".to_string(), sql_quote);
        parts.push(format!(
            "SELECT {i} AS id, (SELECT string_agg(x, {d} ORDER BY ord) FROM unnest({arr}) WITH ORDINALITY AS t(x, ord)) AS r"
        ));
    }
    sql.push_str(&parts.join("\nUNION ALL\n"));
    sql.push_str("\n) v ORDER BY v.id;\n");

    let Some(pg) = psql(&sql) else {
        eprintln!("SKIP: no reachable PostgreSQL");
        return;
    };

    let mut mismatches = Vec::new();
    for (i, (rows, delim)) in corpus.iter().enumerate() {
        let got = match rust_string_agg(rows, *delim) {
            None => "<null>".to_string(),
            Some(s) => format!("OK:{s}"),
        };
        if got != pg[i] {
            mismatches.push(format!("  string_agg case {i}  pg={}  pgrust={got}", pg[i]));
        }
    }
    eprintln!("string_agg differential: {} cases, {} mismatches", corpus.len(), mismatches.len());
    assert!(mismatches.is_empty(), "\n{}", mismatches.join("\n"));
}
