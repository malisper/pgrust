//! Differential test vs live PostgreSQL 18.3 (host /tmp, port 5432, db
//! postgres). Every corpus case is evaluated on both sides as
//! `V:<hex-of-utf8-bytes>` or `E:<sqlstate>` and compared byte-exact.
//! Skips silently if no PG is reachable.

use std::io::Write;
use std::process::Command;

use adt_oracle_compat as oc;
use datum::Varlena;
use mcx::{Mcx, MemoryContext};
use types_core::C_COLLATION_OID;
use types_error::{unpack_sqlstate, PgResult};

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn side(r: PgResult<Varlena<'_>>) -> String {
    match r {
        Ok(v) => format!("V:{}", hex(v.data())),
        Err(e) => format!(
            "E:{}",
            std::str::from_utf8(&unpack_sqlstate(e.sqlstate())).unwrap().to_string()
        ),
    }
}

fn side_i32(r: PgResult<i32>) -> String {
    match r {
        Ok(v) => format!("V:{}", hex(v.to_string().as_bytes())),
        Err(e) => format!(
            "E:{}",
            std::str::from_utf8(&unpack_sqlstate(e.sqlstate())).unwrap().to_string()
        ),
    }
}

fn q(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

fn corpus(mcx: Mcx<'_>) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = Vec::new();
    let strings = [
        "",
        "abc",
        "héllo",
        "日本語",
        "🐘",
        "a🐘é日",
        "MiXeD 123 don't",
        "hello THE world 3rd time",
        "  padded  ",
        "xyxHIxyx",
    ];
    let ns: [i32; 9] = [i32::MIN, -5, -1, 0, 1, 2, 5, 100, i32::MAX];

    for s in strings {
        for n in ns {
            out.push((
                format!("left({}, ({n})::int)", q(s)),
                side(oc::text_left(mcx, s.as_bytes(), n)),
            ));
            out.push((
                format!("right({}, ({n})::int)", q(s)),
                side(oc::text_right(mcx, s.as_bytes(), n)),
            ));
        }
        out.push((format!("reverse({})", q(s)), side(oc::text_reverse(mcx, s.as_bytes()))));
        for f in ["lower", "upper", "initcap", "casefold"] {
            let local = match f {
                "lower" => oc::lower(mcx, s.as_bytes(), C_COLLATION_OID),
                "upper" => oc::upper(mcx, s.as_bytes(), C_COLLATION_OID),
                "initcap" => oc::initcap(mcx, s.as_bytes(), C_COLLATION_OID),
                _ => oc::casefold(mcx, s.as_bytes(), C_COLLATION_OID),
            };
            out.push((format!("{f}({} COLLATE \"C\")", q(s)), side(local)));
        }
        for set in ["", " ", "xy", "é", "🐘x"] {
            out.push((
                format!("btrim({}, {})", q(s), q(set)),
                side(oc::btrim(mcx, s.as_bytes(), set.as_bytes())),
            ));
            out.push((
                format!("ltrim({}, {})", q(s), q(set)),
                side(oc::ltrim(mcx, s.as_bytes(), set.as_bytes())),
            ));
            out.push((
                format!("rtrim({}, {})", q(s), q(set)),
                side(oc::rtrim(mcx, s.as_bytes(), set.as_bytes())),
            ));
        }
        out.push((format!("btrim({})", q(s)), side(oc::btrim1(mcx, s.as_bytes()))));
        out.push((format!("ltrim({})", q(s)), side(oc::ltrim1(mcx, s.as_bytes()))));
        out.push((format!("rtrim({})", q(s)), side(oc::rtrim1(mcx, s.as_bytes()))));
        out.push((format!("ascii({})", q(s)), side_i32(oc::ascii(s.as_bytes()))));
    }

    for (s, n, fill) in [
        ("hi", 5, "xy"),
        ("hi", 5, ""),
        ("hi", -3, "xy"),
        ("hi", 0, "xy"),
        ("hello", 3, "xy"),
        ("", 3, "ab"),
        ("héllo", 7, "àb"),
        ("é", 3, "ü"),
        ("日本語", 5, "🐘"),
        ("x", i32::MAX, "y"),
        ("x", i32::MAX, ""),
        ("🐘", i32::MIN, "y"),
    ] {
        out.push((
            format!("lpad({}, ({n})::int, {})", q(s), q(fill)),
            side(oc::lpad(mcx, s.as_bytes(), n, fill.as_bytes())),
        ));
        out.push((
            format!("rpad({}, ({n})::int, {})", q(s), q(fill)),
            side(oc::rpad(mcx, s.as_bytes(), n, fill.as_bytes())),
        ));
    }

    for c in [
        -1, 0, 1, 65, 127, 128, 233, 8364, 55295, 55296, 57343, 57344, 65535, 1114111, 1114112,
        i32::MAX,
    ] {
        out.push((format!("chr(({c})::int)"), side(oc::chr(mcx, c))));
    }

    for (s, from, to) in [
        ("12345", "143", "ax"),
        ("abc", "", ""),
        ("abc", "", "xyz"),
        ("abc", "abc", ""),
        ("héllo", "é", "e"),
        ("a日b", "日", "🐘x"),
        ("aéb", "xé", "y"),
        ("🐘🐘", "🐘", "é"),
        ("", "a", "b"),
    ] {
        out.push((
            format!("translate({}, {}, {})", q(s), q(from), q(to)),
            side(oc::translate(mcx, s.as_bytes(), from.as_bytes(), to.as_bytes())),
        ));
    }

    for (s, n) in [("Pg", 4), ("Pg", 0), ("Pg", -2), ("é", 3), ("", 5), ("Pg", i32::MAX)] {
        out.push((
            format!("repeat({}, ({n})::int)", q(s)),
            side(oc::repeat(mcx, s.as_bytes(), n)),
        ));
    }

    out
}

fn pg_batch(exprs: &[String]) -> Option<Vec<String>> {
    let mut sql = String::from(
        "SET standard_conforming_strings = on;\n\
         CREATE OR REPLACE FUNCTION pg_temp.try(e text) RETURNS text \
         LANGUAGE plpgsql AS $fn$\n\
         DECLARE r text;\n\
         BEGIN\n\
           EXECUTE 'SELECT encode(convert_to((' || e || ')::text, ''UTF8''), ''hex'')' INTO r;\n\
           RETURN 'V:' || r;\n\
         EXCEPTION WHEN others THEN RETURN 'E:' || SQLSTATE;\n\
         END $fn$;\n\
         SELECT v.id, pg_temp.try(v.e) FROM (VALUES\n",
    );
    for (i, e) in exprs.iter().enumerate() {
        if i > 0 {
            sql.push_str(",\n");
        }
        sql.push_str(&format!("({}, {})", i, q(e)));
    }
    sql.push_str("\n) AS v(id, e) ORDER BY v.id;\n");

    let path = std::env::temp_dir().join("pgrust_oracle_compat_diff.sql");
    std::fs::File::create(&path).ok()?.write_all(sql.as_bytes()).ok()?;

    let out = Command::new("psql")
        .args(["-h", "/tmp", "-p", "5432", "-d", "postgres", "-tAF", "\t", "-f"])
        .arg(&path)
        .output()
        .ok()?;
    if !out.status.success() {
        eprintln!("psql failed: {}", String::from_utf8_lossy(&out.stderr));
        return None;
    }
    let mut results = vec![String::new(); exprs.len()];
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
    let ctx = MemoryContext::new("diff");
    let corpus = corpus(ctx.mcx());
    let exprs: Vec<String> = corpus.iter().map(|(e, _)| e.clone()).collect();
    let pg = match pg_batch(&exprs) {
        Some(v) => v,
        None => {
            eprintln!("SKIP: no reachable PostgreSQL at /tmp:5432.");
            return;
        }
    };

    let mut mismatches = Vec::new();
    let mut compared = 0usize;
    for (i, (expr, local)) in corpus.iter().enumerate() {
        let expect = &pg[i];
        if expect.is_empty() {
            continue;
        }
        compared += 1;
        if local != expect {
            mismatches.push(format!("  {expr}  pg={expect}  pgrust={local}"));
        }
    }
    eprintln!("differential: {compared} cases compared, {} mismatches", mismatches.len());
    assert!(
        mismatches.is_empty(),
        "{} / {} mismatches vs live PG:\n{}",
        mismatches.len(),
        compared,
        mismatches.iter().take(60).cloned().collect::<Vec<_>>().join("\n")
    );
}
