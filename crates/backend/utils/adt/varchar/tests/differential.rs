//! Differential test: adt_varchar value cores vs live PostgreSQL 18.3
//! (psql -h /tmp -p 5432 -d postgres). Each case pairs a SQL expression with
//! the crate-side evaluation; both normalize to "V:<text>" or
//! "ERR:<sqlstate>:<message>". Skips silently if no PG is reachable.

use std::io::Write;
use std::process::Command;

use mcx::MemoryContext;
use types_core::C_COLLATION_OID;
use types_error::{unpack_sqlstate, PgError, PgResult};

const C: u32 = C_COLLATION_OID;

fn norm<T: ToString>(r: PgResult<T>) -> String {
    match r {
        Ok(v) => format!("V:{}", v.to_string()),
        Err(e) => err_str(&e),
    }
}

fn err_str(e: &PgError) -> String {
    let ss = unpack_sqlstate(e.sqlstate());
    format!(
        "ERR:{}:{}",
        core::str::from_utf8(&ss).unwrap(),
        e.message()
    )
}

fn text(r: PgResult<Option<datum::Varlena<'_>>>, source: &str) -> String {
    match r {
        Ok(Some(v)) => format!("V:{}", String::from_utf8_lossy(v.data())),
        Ok(None) => format!("V:{source}"),
        Err(e) => err_str(&e),
    }
}

fn tm(n: i32) -> i32 {
    n + 4
}

fn cases() -> Vec<(String, Box<dyn Fn(mcx::Mcx<'_>) -> String>)> {
    let mut v: Vec<(String, Box<dyn Fn(mcx::Mcx<'_>) -> String>)> = Vec::new();
    macro_rules! case {
        ($sql:expr, $f:expr) => {
            v.push(($sql.to_string(), Box::new($f)))
        };
    }

    let bpin: &[(&str, i32)] = &[
        ("abc", 5),
        ("", 3),
        ("abc   ", 3),
        ("abcd", 3),
        ("abc d", 3),
        ("abc", -5),
        ("éé", 3),
        ("ééé", 2),
        ("ééé", 4),
        ("ééé   ", 3),
    ];
    for &(s, n) in bpin {
        let typmod = if n < 0 { -1 } else { tm(n) };
        case!(
            format!("SELECT format('%s', bpcharin('{s}'::cstring, 0, {typmod}))"),
            move |mcx| text(adt_varchar::bpchar_input(mcx, s.as_bytes(), typmod, None), s)
        );
        case!(
            format!("SELECT format('%s', varcharin('{s}'::cstring, 0, {typmod}))"),
            move |mcx| text(adt_varchar::varchar_input(mcx, s.as_bytes(), typmod, None), s)
        );
    }

    let casts: &[(&str, i32, bool)] = &[
        ("abc", 5, false),
        ("abc", 3, false),
        ("abcd", 3, true),
        ("abcd", 3, false),
        ("abc   ", 3, false),
        ("ab", -1, false),
        ("ééé", 2, true),
        ("ééé", 2, false),
        ("éé", 4, false),
    ];
    for &(s, n, explicit) in casts {
        let typmod = if n < 0 { -1 } else { tm(n) };
        case!(
            format!("SELECT format('%s', \"bpchar\"('{s}'::bpchar, {typmod}, {explicit}))"),
            move |mcx| text(adt_varchar::bpchar(mcx, s.as_bytes(), typmod, explicit), s)
        );
        case!(
            format!("SELECT format('%s', \"varchar\"('{s}'::varchar, {typmod}, {explicit}))"),
            move |mcx| text(adt_varchar::varchar(mcx, s.as_bytes(), typmod, explicit), s)
        );
    }

    let eqs: &[(&str, &str)] = &[
        ("abc   ", "abc"),
        ("abc", "abc  "),
        ("", "   "),
        ("abc", "abd"),
        (" abc", "abc"),
        ("a bc", "abc"),
        ("éé ", "éé"),
    ];
    for &(a, b) in eqs {
        case!(
            format!("SELECT ('{a}'::bpchar = '{b}'::bpchar COLLATE \"C\")::text"),
            move |_| norm(adt_varchar::bpchareq(a.as_bytes(), b.as_bytes(), C))
        );
        case!(
            format!("SELECT ('{a}'::bpchar < '{b}'::bpchar COLLATE \"C\")::text"),
            move |_| norm(adt_varchar::bpcharlt(a.as_bytes(), b.as_bytes(), C))
        );
        // C returns raw memcmp values from the cmp entry points; compare signs.
        case!(
            format!("SELECT sign(bpcharcmp('{a}'::bpchar, '{b}'::bpchar COLLATE \"C\"))::text"),
            move |_| norm(adt_varchar::bpcharcmp(a.as_bytes(), b.as_bytes(), C).map(|c| c.signum()))
        );
        case!(
            format!("SELECT sign(btbpchar_pattern_cmp('{a}'::bpchar, '{b}'::bpchar))::text"),
            move |_| format!(
                "V:{}",
                adt_varchar::btbpchar_pattern_cmp(a.as_bytes(), b.as_bytes()).signum()
            )
        );
        case!(
            format!("SELECT hashbpchar('{a}'::bpchar COLLATE \"C\")::text"),
            move |_| norm(adt_varchar::hashbpchar(a.as_bytes(), C).map(|h| h as i32))
        );
        case!(
            format!("SELECT hashbpcharextended('{a}'::bpchar COLLATE \"C\", 42)::text"),
            move |_| norm(
                adt_varchar::hashbpcharextended(a.as_bytes(), C, 42).map(|h| h as i64)
            )
        );
    }

    for s in ["abc  ", "", "     ", "éé "] {
        case!(
            format!("SELECT length('{s}'::bpchar)::text"),
            move |_| norm(adt_varchar::bpcharlen(s.as_bytes()))
        );
        case!(
            format!("SELECT octet_length('{s}'::bpchar)::text"),
            move |_| format!("V:{}", adt_varchar::bpcharoctetlen(s.as_bytes()))
        );
    }

    for t in ["5", "1", "0", "-3", "10485760", "10485761"] {
        case!(
            format!("SELECT bpchartypmodin('{{{t}}}'::cstring[])::text"),
            move |mcx| norm(adt_varchar::bpchartypmodin(mcx, &cstring_array_1d(&[t.as_bytes()])))
        );
        case!(
            format!("SELECT varchartypmodin('{{{t}}}'::cstring[])::text"),
            move |mcx| norm(adt_varchar::varchartypmodin(mcx, &cstring_array_1d(&[t.as_bytes()])))
        );
    }
    for t in [9i32, 5, 4, 0, -1] {
        case!(
            format!("SELECT bpchartypmodout({t})::text"),
            move |_| {
                let mut buf = [0u8; 16];
                let n = adt_varchar::anychar_typmodout(t, &mut buf);
                format!("V:{}", core::str::from_utf8(&buf[..n]).unwrap())
            }
        );
    }

    for s in ["abc   ", "abc", "a"] {
        case!(
            format!("SELECT ('{s}'::bpchar)::name::text"),
            move |_| {
                let n = adt_varchar::bpchar_name(s.as_bytes());
                let end = n.iter().position(|&b| b == 0).unwrap_or(n.len());
                format!("V:{}", core::str::from_utf8(&n[..end]).unwrap())
            }
        );
        case!(
            format!("SELECT format('%s', ('{s}'::name)::bpchar)"),
            move |mcx| {
                let mut nd = [0u8; 64];
                nd[..s.len()].copy_from_slice(s.as_bytes());
                norm(
                    adt_varchar::name_bpchar(mcx, &nd)
                        .map(|v| String::from_utf8_lossy(v.data()).into_owned()),
                )
            }
        );
    }
    case!("SELECT format('%s', ('x'::\"char\")::bpchar)", move |mcx| norm(
        adt_varchar::char_bpchar(mcx, b'x' as i8)
            .map(|v| String::from_utf8_lossy(v.data()).into_owned())
    ));

    v
}

fn cstring_array_1d(elems: &[&[u8]]) -> Vec<u8> {
    let mut v = vec![0u8; 4];
    v.extend_from_slice(&1i32.to_ne_bytes());
    v.extend_from_slice(&0i32.to_ne_bytes());
    v.extend_from_slice(&(types_core::CSTRINGOID as u32).to_ne_bytes());
    v.extend_from_slice(&(elems.len() as i32).to_ne_bytes());
    v.extend_from_slice(&1i32.to_ne_bytes());
    while v.len() % 8 != 0 {
        v.push(0);
    }
    for e in elems {
        v.extend_from_slice(e);
        v.push(0);
    }
    let total = (v.len() as u32) << 2;
    v[..4].copy_from_slice(&total.to_ne_bytes());
    v
}

fn pg_batch(sqls: &[String]) -> Option<Vec<String>> {
    let mut sql = String::new();
    sql.push_str(
        "SET client_encoding = 'UTF8';\n\
         CREATE OR REPLACE FUNCTION pg_temp.try(q text) RETURNS text LANGUAGE plpgsql AS $fn$\n\
         DECLARE r text;\n\
         BEGIN\n\
           EXECUTE q INTO r;\n\
           RETURN 'V:' || COALESCE(r, '<null>');\n\
         EXCEPTION WHEN others THEN RETURN 'ERR:' || SQLSTATE || ':' || SQLERRM;\n\
         END $fn$;\n",
    );
    sql.push_str("SELECT v.id::text || E'\\t' || pg_temp.try(v.q) FROM (VALUES\n");
    for (i, q) in sqls.iter().enumerate() {
        if i > 0 {
            sql.push_str(",\n");
        }
        sql.push_str(&format!("({}, '{}')", i, q.replace('\'', "''")));
    }
    sql.push_str("\n) AS v(id, q) ORDER BY v.id;\n");

    let dir = std::env::temp_dir();
    // pid-unique: a fixed name strands another user's file in sticky /tmp
    // when an oracle-less run precedes the gated one (quote precedent).
    let path = dir.join(format!("pgrust_varchar_diff_{}.sql", std::process::id()));
    std::fs::File::create(&path).ok()?.write_all(sql.as_bytes()).ok()?;

    let out = Command::new("psql")
        .args(["-h", "/tmp", "-p", "5432", "-d", "postgres"])
        .args(["-tA", "-v", "ON_ERROR_STOP=1", "-f"])
        .arg(&path)
        .output()
        .ok()?;
    if !out.status.success() {
        eprintln!("psql failed: {}", String::from_utf8_lossy(&out.stderr));
        return None;
    }
    let mut results = vec![String::new(); sqls.len()];
    for line in String::from_utf8_lossy(&out.stdout).lines() {
        let mut it = line.splitn(2, '\t');
        if let (Some(id), Some(r)) = (it.next(), it.next()) {
            if let Ok(id) = id.parse::<usize>() {
                if id < results.len() {
                    results[id] = r.to_string();
                }
            }
        }
    }
    Some(results)
}

#[test]
fn differential_vs_live_pg() {
    mbutils::SetDatabaseEncoding(wchar::PG_UTF8).unwrap();
    let cases = cases();
    let sqls: Vec<String> = cases.iter().map(|(s, _)| s.clone()).collect();
    let pg = match pg_batch(&sqls) {
        Some(v) => v,
        None => {
            eprintln!("SKIP: no reachable PostgreSQL (psql -h /tmp -p 5432 -d postgres).");
            let ctx = MemoryContext::new("t");
            for (_, f) in &cases {
                let _ = f(ctx.mcx());
            }
            return;
        }
    };

    let ctx = MemoryContext::new("t");
    let mut mismatches = Vec::new();
    let mut compared = 0usize;
    for (i, (sql, f)) in cases.iter().enumerate() {
        let expect = &pg[i];
        if expect.is_empty() {
            continue;
        }
        compared += 1;
        let got = f(ctx.mcx());
        if &got != expect {
            mismatches.push(format!("  {sql}\n    pg={expect:?}\n    pgrust={got:?}"));
        }
    }
    assert!(compared > 0, "no PG results parsed");
    assert!(
        mismatches.is_empty(),
        "{} of {} mismatched:\n{}",
        mismatches.len(),
        compared,
        mismatches.join("\n")
    );
    eprintln!("differential: {compared} cases matched live PG");
}
