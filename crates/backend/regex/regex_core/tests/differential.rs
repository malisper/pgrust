//! Differential test: pgrust regex engine vs live PostgreSQL 18.3.
//!
//! For each (string, pattern, icase) triple we run the engine's
//! compile+exec (`seam_pg_regcomp`/`seam_pg_regexec`) under the C collation
//! and compare match/no-match/error against `psql` evaluating the same
//! `~`/`~*` under `COLLATE "C"`. Skips silently if no PG is reachable.

use std::io::Write;
use std::process::Command;

use regex::{RegMatch, RegcompResult, RegexecResult};
use regex_core::regex_consts::{REG_ADVANCED, REG_ICASE};
use regex_core::regex_export_free_error::{seam_pg_regcomp, seam_pg_regexec};
use types_core::C_COLLATION_OID;

fn to_w(s: &str) -> Vec<u32> {
    s.chars().map(|c| c as u32).collect()
}

fn pgrust_eval(s: &str, p: &str, icase: bool) -> &'static str {
    let cflags = REG_ADVANCED | if icase { REG_ICASE } else { 0 };
    let pw = to_w(p);
    match seam_pg_regcomp(&pw, cflags, C_COLLATION_OID) {
        Ok(RegcompResult::Compiled(re)) => {
            let dw = to_w(s);
            match seam_pg_regexec(&re, &dw, 0, &mut []) {
                Ok(RegexecResult::Matched) => "t",
                Ok(RegexecResult::NoMatch) => "f",
                _ => "ERR",
            }
        }
        _ => "ERR",
    }
}

/// pgrust capture extraction: returns the group-1.. substrings, or None if no
/// match / compile error. Mirrors `regexp_match`, which reports groups 1..n
/// (or the whole match as one element when the pattern has no groups).
fn pgrust_captures(s: &str, p: &str) -> Option<Vec<Option<String>>> {
    let pw = to_w(p);
    let re = match seam_pg_regcomp(&pw, REG_ADVANCED, C_COLLATION_OID) {
        Ok(RegcompResult::Compiled(re)) => re,
        _ => return None,
    };
    let nsub = re.re_nsub;
    let dchars: Vec<char> = s.chars().collect();
    let dw: Vec<u32> = dchars.iter().map(|&c| c as u32).collect();
    let mut pmatch = vec![RegMatch::UNSET; nsub + 1];
    match seam_pg_regexec(&re, &dw, 0, &mut pmatch) {
        Ok(RegexecResult::Matched) => {}
        _ => return None,
    }
    let group = |m: RegMatch| -> Option<String> {
        if m.rm_so < 0 || m.rm_eo < 0 {
            None
        } else {
            Some(dchars[m.rm_so as usize..m.rm_eo as usize].iter().collect())
        }
    };
    if nsub == 0 {
        Some(vec![group(pmatch[0])])
    } else {
        Some((1..=nsub).map(|i| group(pmatch[i])).collect())
    }
}

/// Feature-covering corpus of (string, pattern, icase).
fn corpus() -> Vec<(String, String, bool)> {
    // Patterns spanning the claimed feature set.
    let patterns: &[&str] = &[
        // literals / dot / concatenation
        "a", "abc", "a.c", "...", "", "a\\.c",
        // anchors
        "^abc", "abc$", "^abc$", "^$", "^a", "c$", "\\Aabc", "abc\\Z",
        // char classes
        "[abc]", "[^abc]", "[a-z]", "[A-Z]", "[0-9]", "[a-zA-Z0-9]", "[]a]", "[a-]", "[-a]",
        "[^0-9]", "[[:alpha:]]", "[[:digit:]]", "[[:alnum:]]", "[[:space:]]", "[[:upper:]]",
        "[[:lower:]]", "[[:punct:]]", "[[:xdigit:]]", "[[:alpha:][:digit:]]",
        // escapes / shorthand classes
        "\\d", "\\D", "\\w", "\\W", "\\s", "\\S", "\\d+", "\\w+", "\\s*", "[\\d]", "[\\w.]",
        // quantifiers greedy
        "a*", "a+", "a?", "a{2}", "a{2,}", "a{2,4}", "ab*c", "a.*b", ".*", ".+",
        "colou?r", "(ab)+", "(ab)*", "a{0}", "[0-9]{3}",
        // quantifiers non-greedy
        "a*?", "a+?", "a??", "a{2,4}?", ".*?b", "<.*?>", "<.+?>",
        // alternation / groups
        "a|b", "abc|def", "(a|b)c", "(foo|bar|baz)", "^(yes|no)$", "gr(a|e)y",
        "(a|b|c)+", "cat|dog|bird",
        // backreferences
        "(a)\\1", "(ab)\\1", "(.)\\1", "(\\w)\\1", "(a)(b)\\2\\1", "^(.)(.)\\2\\1$",
        // word boundaries (Spencer AREs)
        "\\yword\\y", "\\mstart", "end\\M", "\\ba", // \b is backspace in ARE bracket only; bare \b = word bdy? actually \b is backspace
        // nested / complex
        "(a(b(c)))", "((x))", "a(b|c)*d", "([a-z]+)@([a-z]+)", "\\d{4}-\\d{2}-\\d{2}",
        "[A-Za-z_][A-Za-z0-9_]*", "(\\d+)\\.(\\d+)", "^\\s*$", "\\s+$",
        // case-relevant
        "ABC", "[a-c]", "hello", "WORLD",
        // error / edge patterns (should error on both sides)
        "[", "(", ")", "a{", "*", "+abc", "[z-a]", "\\", "a{2,1}", "[[:bogus:]]",
        "(?P<n>a)", "**", "(unclosed", "a\\", "[a-\\d]",
    ];
    // Strings to test each pattern against.
    let strings: &[&str] = &[
        "", "a", "abc", "ABC", "aXc", "a.c", "hello world", "Hello World",
        "12345", "2024-01-15", "  ", "\t \n", "foobar", "foo", "bar", "baz",
        "aa", "aaa", "abab", "grey", "gray", "color", "colour", "user@host",
        "The quick brown fox", "!@#$%", "x", "xx", "aXbXc", "<a><b>",
        "word here", "start of line", "the end", "3.14", "007", "_id42",
        "yes", "no", "maybe", "CamelCase", "snake_case", "UPPER", "lower",
    ];
    let mut out = Vec::new();
    for &p in patterns {
        for &s in strings {
            out.push((s.to_string(), p.to_string(), false));
        }
    }
    // A case-insensitive sweep on the ASCII-relevant subset.
    let ic_pats: &[&str] = &[
        "abc", "ABC", "[a-z]", "[A-Z]", "hello", "WORLD", "a|B", "(foo|BAR)",
        "^camelcase$", "upper", "[[:alpha:]]+", "xx?", "gr(a|e)y",
    ];
    let ic_strs: &[&str] = &[
        "abc", "ABC", "AbC", "hello", "HELLO", "Hello", "world", "WORLD",
        "foo", "BAR", "CamelCase", "UPPER", "lower", "grey", "GRAY", "xx", "",
    ];
    for &p in ic_pats {
        for &s in ic_strs {
            out.push((s.to_string(), p.to_string(), true));
        }
    }
    out
}

fn sql_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

fn pg_batch(corpus: &[(String, String, bool)]) -> Option<Vec<String>> {
    let mut sql = String::new();
    sql.push_str(
        "SET standard_conforming_strings = on;\n\
         CREATE OR REPLACE FUNCTION pg_temp.reg_try(s text, p text, ic bool) \
         RETURNS text LANGUAGE plpgsql AS $fn$\n\
         BEGIN\n\
           IF ic THEN RETURN (CASE WHEN (s COLLATE \"C\") ~* p THEN 't' ELSE 'f' END);\n\
           ELSE RETURN (CASE WHEN (s COLLATE \"C\") ~ p THEN 't' ELSE 'f' END);\n\
           END IF;\n\
         EXCEPTION WHEN others THEN RETURN 'ERR';\n\
         END $fn$;\n",
    );
    sql.push_str("SELECT v.id, pg_temp.reg_try(v.s, v.p, v.ic) FROM (VALUES\n");
    for (i, (s, p, ic)) in corpus.iter().enumerate() {
        if i > 0 {
            sql.push_str(",\n");
        }
        sql.push_str(&format!(
            "({}, {}, {}, {})",
            i,
            sql_quote(s),
            sql_quote(p),
            if *ic { "true" } else { "false" }
        ));
    }
    sql.push_str("\n) AS v(id, s, p, ic) ORDER BY v.id;\n");

    let dir = std::env::temp_dir();
    let path = dir.join("pgrust_regex_diff.sql");
    std::fs::File::create(&path).ok()?.write_all(sql.as_bytes()).ok()?;

    let out = Command::new("psql")
        .args(["-tAF", "\t", "-v", "ON_ERROR_STOP=0", "-f"])
        .arg(&path)
        .output()
        .ok()?;
    if !out.status.success() {
        eprintln!("psql failed: {}", String::from_utf8_lossy(&out.stderr));
        return None;
    }
    let mut results = vec![String::new(); corpus.len()];
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
    let corpus = corpus();
    let pg = match pg_batch(&corpus) {
        Some(v) => v,
        None => {
            eprintln!("SKIP: no reachable PostgreSQL (psql). Ran engine-only smoke instead.");
            // Still exercise the engine so the test isn't a no-op.
            for (s, p, ic) in &corpus {
                let _ = pgrust_eval(s, p, *ic);
            }
            return;
        }
    };

    let mut mismatches = Vec::new();
    let mut compared = 0usize;
    for (i, (s, p, ic)) in corpus.iter().enumerate() {
        let expect = &pg[i];
        if expect.is_empty() {
            continue; // PG produced no row (shouldn't happen); skip
        }
        compared += 1;
        let got = pgrust_eval(s, p, *ic);
        // Treat both-ERR as agreement (error parity at the boolean level).
        if got != expect.as_str() {
            mismatches.push(format!(
                "  s={s:?} p={p:?} icase={ic}  pg={expect}  pgrust={got}"
            ));
        }
    }

    eprintln!(
        "differential: {} pairs compared, {} mismatches",
        compared,
        mismatches.len()
    );
    if !mismatches.is_empty() {
        let shown: Vec<_> = mismatches.iter().take(60).cloned().collect();
        panic!(
            "{} / {} mismatches vs live PG:\n{}",
            mismatches.len(),
            compared,
            shown.join("\n")
        );
    }
}

/// Capture-group corpus: (string, pattern) with subexpressions.
fn capture_corpus() -> Vec<(String, String)> {
    let cases: &[(&str, &str)] = &[
        ("foobarbaz", "bar"),
        ("foobarbaz", "(bar)(baz)"),
        ("2024-01-15", "(\\d+)-(\\d+)-(\\d+)"),
        ("user@host.com", "([a-z]+)@([a-z.]+)"),
        ("abcabc", "(abc)\\1"),
        ("hello world", "(\\w+)\\s+(\\w+)"),
        ("aXbXc", "(.)X(.)X(.)"),
        ("nomatch", "(\\d+)"),
        ("key=value", "(\\w+)=(\\w+)"),
        ("  padded  ", "^(\\s*)(\\S*)(\\s*)$"),
        ("3.14159", "(\\d+)\\.(\\d+)"),
        ("abc", "(a)(b)(c)"),
        ("aaa", "(a+)"),
        ("optional", "(x)?(optional)"),
        ("greedy", "(.*)(e)"),
        ("nongreedy", "(.*?)(e)"),
        ("nested", "((n)(e))(sted)"),
        ("CamelCase", "([A-Z][a-z]+)([A-Z][a-z]+)"),
        ("a1b2c3", "([a-z])(\\d)"),
        ("", "(a)?"),
    ];
    cases.iter().map(|(s, p)| (s.to_string(), p.to_string())).collect()
}

#[test]
fn differential_captures_vs_live_pg() {
    let corpus = capture_corpus();
    // Build one query returning each capture array as a text via array_to_string
    // with a sentinel for NULL elements; NULL whole result => no match.
    let mut sql = String::from("SET standard_conforming_strings = on;\n");
    sql.push_str("SELECT v.id, CASE WHEN m IS NULL THEN '<none>' ELSE \
        array_to_string(array(SELECT COALESCE(x, '<null>') FROM unnest(m) x), chr(31)) END \
        FROM (SELECT v.id, regexp_match(v.s COLLATE \"C\", v.p) AS m FROM (VALUES\n");
    for (i, (s, p)) in corpus.iter().enumerate() {
        if i > 0 {
            sql.push_str(",\n");
        }
        sql.push_str(&format!("({}, {}, {})", i, sql_quote(s), sql_quote(p)));
    }
    sql.push_str("\n) AS v(id, s, p)) v ORDER BY v.id;\n");

    let path = std::env::temp_dir().join("pgrust_regex_capture.sql");
    if std::fs::File::create(&path)
        .and_then(|mut f| f.write_all(sql.as_bytes()))
        .is_err()
    {
        eprintln!("SKIP captures: temp write failed");
        return;
    }
    let out = match Command::new("psql")
        .args(["-tAF", "\t", "-f"])
        .arg(&path)
        .output()
    {
        Ok(o) if o.status.success() => o,
        _ => {
            eprintln!("SKIP captures: psql unavailable");
            return;
        }
    };
    let mut pg = vec![String::new(); corpus.len()];
    for line in String::from_utf8_lossy(&out.stdout).lines() {
        let mut it = line.splitn(2, '\t');
        if let (Some(id), Some(r)) = (it.next(), it.next()) {
            if let Ok(id) = id.parse::<usize>() {
                if id < pg.len() {
                    pg[id] = r.to_string();
                }
            }
        }
    }

    let mut mismatches = Vec::new();
    for (i, (s, p)) in corpus.iter().enumerate() {
        let expect = &pg[i];
        let got = match pgrust_captures(s, p) {
            None => "<none>".to_string(),
            Some(groups) => groups
                .iter()
                .map(|g| g.clone().unwrap_or_else(|| "<null>".to_string()))
                .collect::<Vec<_>>()
                .join("\u{1f}"),
        };
        if &got != expect {
            mismatches.push(format!("  s={s:?} p={p:?}  pg={expect:?}  pgrust={got:?}"));
        }
    }
    eprintln!(
        "capture differential: {} patterns, {} mismatches",
        corpus.len(),
        mismatches.len()
    );
    assert!(
        mismatches.is_empty(),
        "capture mismatches vs live PG:\n{}",
        mismatches.join("\n")
    );
}
