//! The \d family, \l, \dn, \df, \conninfo: catalog queries cribbed from
//! psql's describe.c (PostgreSQL 18) so titles, headers, row content and
//! footers match real psql. The GATE for this port diffs rendered output
//! against PGDG psql 18 byte-for-byte; keep any query change aligned with
//! describe.c.

use crate::print::{self, Table};
use crate::proto::{ExecStatus, QueryResult};
use crate::{result_to_table, PsqlState};

/// PSQLexec-alike: run one catalog query over the simple protocol; the
/// caller renders. Errors come back as the psql-shaped message text.
fn run(st: &mut PsqlState, sql: &str) -> Result<QueryResult, String> {
    let Some(conn) = st.conn.as_mut() else {
        return Err("You are currently not connected to a database.".into());
    };
    conn.send_query(sql).map_err(|e| e)?;
    let mut last: Option<QueryResult> = None;
    let mut err: Option<String> = None;
    loop {
        match conn.get_result() {
            Err(e) => return Err(e),
            Ok(None) => break,
            Ok(Some(r)) => {
                if r.status == ExecStatus::Error {
                    let msg = match &r.diag {
                        Some(f) => crate::errmsg::build_message(f, Some(sql), true),
                        None => format!("{}\n", r.conn_err),
                    };
                    err = Some(msg);
                } else {
                    last = Some(r);
                }
            }
        }
    }
    if let Some(e) = err {
        return Err(e.trim_end().to_string());
    }
    last.ok_or_else(|| "no result".to_string())
}

fn print_result(st: &PsqlState, r: &QueryResult, title: &str) {
    let mut t = result_to_table(r);
    if !title.is_empty() {
        t.title = Some(title.to_string());
    }
    let mut out = std::io::stdout();
    let _ = print::print_table(&t, &st.popt, &mut out);
    let _ = std::io::Write::flush(&mut out);
}

fn cell(r: &QueryResult, i: usize, j: usize) -> String {
    r.rows[i][j].as_ref().map(|b| String::from_utf8_lossy(b).into_owned()).unwrap_or_default()
}

fn cell_opt(r: &QueryResult, i: usize, j: usize) -> Option<String> {
    r.rows[i][j].as_ref().map(|b| String::from_utf8_lossy(b).into_owned())
}

// ----------------------------------------------------- name pattern -> SQL

/// Port of processSQLNamePattern (fe_utils/string_utils.c): shell-style
/// pattern -> anchored regex clauses. Returns WHERE-fragment lines (without
/// leading WHERE/AND).
pub struct PatternClauses {
    pub clauses: Vec<String>,
}

pub fn process_pattern(
    pattern: Option<&str>,
    schemavar: &str,
    namevar: &str,
    visibilityrule: Option<&str>,
) -> PatternClauses {
    let mut clauses = Vec::new();
    let Some(pattern) = pattern else {
        if let Some(v) = visibilityrule {
            clauses.push(v.to_string());
        }
        return PatternClauses { clauses };
    };

    // Split on dots outside double quotes; keep the last two parts as
    // schema.name (db part validated-and-ignored by psql; we ignore too).
    let mut parts: Vec<String> = Vec::new();
    let mut cur = String::new();
    let mut in_quotes = false;
    let chars: Vec<char> = pattern.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c == '"' {
            if in_quotes && i + 1 < chars.len() && chars[i + 1] == '"' {
                cur.push('"');
                cur.push('"');
                i += 2;
                continue;
            }
            in_quotes = !in_quotes;
            cur.push('"');
        } else if c == '.' && !in_quotes {
            parts.push(std::mem::take(&mut cur));
            i += 1;
            continue;
        } else {
            cur.push(c);
        }
        i += 1;
    }
    parts.push(cur);
    let (schema_pat, name_pat) = if parts.len() >= 2 {
        (Some(parts[parts.len() - 2].clone()), parts[parts.len() - 1].clone())
    } else {
        (None, parts.pop().unwrap())
    };

    let name_re = pattern_part_to_regex(&name_pat);
    if !name_re.is_empty() {
        clauses.push(format!(
            "{namevar} OPERATOR(pg_catalog.~) '^({name_re})$' COLLATE pg_catalog.default"
        ));
    }
    match schema_pat {
        Some(sp) => {
            let re = pattern_part_to_regex(&sp);
            if !re.is_empty() {
                clauses.push(format!(
                    "{schemavar} OPERATOR(pg_catalog.~) '^({re})$' COLLATE pg_catalog.default"
                ));
            }
        }
        None => {
            if let Some(v) = visibilityrule {
                clauses.push(v.to_string());
            }
        }
    }
    PatternClauses { clauses }
}

fn pattern_part_to_regex(part: &str) -> String {
    let mut re = String::new();
    let chars: Vec<char> = part.chars().collect();
    let mut in_quotes = false;
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c == '"' {
            if in_quotes && i + 1 < chars.len() && chars[i + 1] == '"' {
                re.push('"');
                i += 2;
                continue;
            }
            in_quotes = !in_quotes;
            i += 1;
            continue;
        }
        if !in_quotes && c == '*' {
            re.push_str(".*");
        } else if !in_quotes && c == '?' {
            re.push('.');
        } else {
            let c = if in_quotes { c } else { lower_char(c) };
            if "|*+?()[]{}.^$\\".contains(c) {
                re.push('\\');
            }
            // SQL-literal escaping of the regex happens at embed time.
            re.push(c);
        }
        i += 1;
    }
    // Escape single quotes for direct embedding in a SQL literal.
    re.replace('\'', "''")
}

fn lower_char(c: char) -> char {
    // downcase_identifier: ASCII lowercase (locale-independent here).
    if c.is_ascii_uppercase() {
        c.to_ascii_lowercase()
    } else {
        c
    }
}

fn where_clause(clauses: &[String], extra_first: &str) -> String {
    let mut all: Vec<&str> = Vec::new();
    if !extra_first.is_empty() {
        all.push(extra_first);
    }
    for c in clauses {
        all.push(c);
    }
    if all.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", all.join("\n  AND "))
    }
}

// ------------------------------------------------------------------ \l

pub fn list_databases(st: &mut PsqlState, pattern: Option<&str>, plus: bool) -> Result<(), String> {
    let pat = process_pattern(pattern, "", "d.datname", None);
    let mut sql = String::from(
        "SELECT\n  d.datname as \"Name\",\n  pg_catalog.pg_get_userbyid(d.datdba) as \"Owner\",\n  pg_catalog.pg_encoding_to_char(d.encoding) as \"Encoding\",\n  CASE d.datlocprovider WHEN 'b' THEN 'builtin' WHEN 'c' THEN 'libc' WHEN 'i' THEN 'icu' END AS \"Locale Provider\",\n  d.datcollate as \"Collate\",\n  d.datctype as \"Ctype\",\n  d.datlocale as \"Locale\",\n  d.daticurules as \"ICU Rules\",\n  CASE WHEN pg_catalog.array_length(d.datacl, 1) = 0 THEN '(none)' ELSE pg_catalog.array_to_string(d.datacl, E'\\n') END AS \"Access privileges\"",
    );
    if plus {
        sql.push_str(
            ",\n  CASE WHEN pg_catalog.has_database_privilege(d.datname, 'CONNECT')\n       THEN pg_catalog.pg_size_pretty(pg_catalog.pg_database_size(d.datname))\n       ELSE 'No Access'\n  END as \"Size\",\n  t.spcname as \"Tablespace\",\n  pg_catalog.shobj_description(d.oid, 'pg_database') as \"Description\"",
        );
    }
    sql.push_str("\nFROM pg_catalog.pg_database d");
    if plus {
        sql.push_str("\n  JOIN pg_catalog.pg_tablespace t on d.dattablespace = t.oid");
    }
    if !pat.clauses.is_empty() {
        sql.push('\n');
        sql.push_str(&where_clause(&pat.clauses, ""));
    }
    sql.push_str("\nORDER BY 1;");
    let r = run(st, &sql)?;
    print_result(st, &r, "List of databases");
    Ok(())
}

// ------------------------------------------------------------------ \dn

pub fn list_schemas(st: &mut PsqlState, pattern: Option<&str>, plus: bool) -> Result<(), String> {
    let pat = process_pattern(pattern, "", "n.nspname", None);
    let mut sql = String::from(
        "SELECT n.nspname AS \"Name\",\n  pg_catalog.pg_get_userbyid(n.nspowner) AS \"Owner\"",
    );
    if plus {
        sql.push_str(
            ",\n  pg_catalog.array_to_string(n.nspacl, E'\\n') AS \"Access privileges\",\n  pg_catalog.obj_description(n.oid, 'pg_namespace') AS \"Description\"",
        );
    }
    sql.push_str("\nFROM pg_catalog.pg_namespace n\n");
    let base = if pattern.is_some() {
        String::new()
    } else {
        "n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'".to_string()
    };
    sql.push_str(&where_clause(&pat.clauses, &base));
    sql.push_str("\nORDER BY 1;");
    let r = run(st, &sql)?;
    print_result(st, &r, "List of schemas");
    Ok(())
}

// --------------------------------------------------------- \dt \di \dv \ds

/// kinds: subset of "tivs" from the command letters.
pub fn list_relations(
    st: &mut PsqlState,
    kinds: &str,
    pattern: Option<&str>,
    plus: bool,
) -> Result<(), String> {
    let mut relkinds: Vec<&str> = Vec::new();
    let mut showed: Vec<&str> = Vec::new();
    if kinds.is_empty() || kinds.contains('t') {
        relkinds.extend(["'r'", "'p'"]);
        showed.push("tables");
    }
    if kinds.is_empty() || kinds.contains('v') {
        relkinds.extend(["'v'"]);
        showed.push("views");
    }
    if kinds.is_empty() || kinds.contains('m') {
        relkinds.extend(["'m'"]);
    }
    if kinds.is_empty() || kinds.contains('s') {
        relkinds.extend(["'S'"]);
        showed.push("sequences");
    }
    if kinds.contains('i') {
        relkinds.extend(["'i'", "'I'"]);
        showed.push("indexes");
    }
    if kinds.is_empty() {
        relkinds.extend(["'f'"]);
    }

    let with_table_col = kinds.contains('i') || kinds.contains('s');
    let mut sql = String::from(
        "SELECT n.nspname as \"Schema\",\n  c.relname as \"Name\",\n  CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 't' THEN 'TOAST table' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' WHEN 'I' THEN 'partitioned index' END as \"Type\",\n  pg_catalog.pg_get_userbyid(c.relowner) as \"Owner\"",
    );
    if kinds.contains('i') {
        sql.push_str(",\n  c2.relname as \"Table\"");
    }
    if plus {
        sql.push_str(
            ",\n  CASE c.relpersistence WHEN 'p' THEN 'permanent' WHEN 't' THEN 'temporary' WHEN 'u' THEN 'unlogged' END as \"Persistence\",\n  am.amname as \"Access method\",\n  pg_catalog.pg_size_pretty(pg_catalog.pg_table_size(c.oid)) as \"Size\",\n  pg_catalog.obj_description(c.oid, 'pg_class') as \"Description\"",
        );
    }
    sql.push_str("\nFROM pg_catalog.pg_class c\n     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace");
    if plus {
        sql.push_str("\n     LEFT JOIN pg_catalog.pg_am am ON am.oid = c.relam");
    }
    if kinds.contains('i') {
        sql.push_str(
            "\n     LEFT JOIN pg_catalog.pg_index i ON i.indexrelid = c.oid\n     LEFT JOIN pg_catalog.pg_class c2 ON i.indrelid = c2.oid",
        );
    }
    let _ = with_table_col;

    let mut base = format!("c.relkind IN ({})", relkinds.join(","));
    if pattern.is_none() {
        base.push_str(
            "\n      AND n.nspname <> 'pg_catalog'\n      AND n.nspname !~ '^pg_toast'\n      AND n.nspname <> 'information_schema'",
        );
    }
    let pat = process_pattern(
        pattern,
        "n.nspname",
        "c.relname",
        Some("pg_catalog.pg_table_is_visible(c.oid)"),
    );
    sql.push('\n');
    sql.push_str(&where_clause(&pat.clauses, &base));
    sql.push_str("\nORDER BY 1,2;");

    let r = run(st, &sql)?;
    // Titles and not-found wording are per-kind (listTables in describe.c).
    let word = match kinds {
        "t" => "tables",
        "i" => "indexes",
        "v" => "views",
        "s" => "sequences",
        _ => "relations",
    };
    if r.rows.is_empty() {
        match pattern {
            Some(p) => {
                if word == "relations" {
                    eprintln!("Did not find any relations named \"{p}\".");
                } else {
                    eprintln!("Did not find any {word} named \"{p}\".");
                }
            }
            None => eprintln!("Did not find any {word}."),
        }
        let _ = showed;
        return Ok(());
    }
    print_result(st, &r, &format!("List of {word}"));
    Ok(())
}

// ------------------------------------------------------------------ \df

pub fn list_functions(st: &mut PsqlState, pattern: Option<&str>, plus: bool) -> Result<(), String> {
    let mut sql = String::from(
        "SELECT n.nspname as \"Schema\",\n  p.proname as \"Name\",\n  pg_catalog.pg_get_function_result(p.oid) as \"Result data type\",\n  pg_catalog.pg_get_function_arguments(p.oid) as \"Argument data types\",\n CASE p.prokind\n  WHEN 'a' THEN 'agg'\n  WHEN 'w' THEN 'window'\n  WHEN 'p' THEN 'proc'\n  ELSE 'func'\n END as \"Type\"",
    );
    if plus {
        sql.push_str(
            ",\n CASE\n  WHEN p.provolatile = 'i' THEN 'immutable'\n  WHEN p.provolatile = 's' THEN 'stable'\n  WHEN p.provolatile = 'v' THEN 'volatile'\n END as \"Volatility\",\n CASE\n  WHEN p.proparallel = 'r' THEN 'restricted'\n  WHEN p.proparallel = 's' THEN 'safe'\n  WHEN p.proparallel = 'u' THEN 'unsafe'\n END as \"Parallel\",\n pg_catalog.pg_get_userbyid(p.proowner) as \"Owner\",\n CASE WHEN prosecdef THEN 'definer' ELSE 'invoker' END AS \"Security\",\n pg_catalog.array_to_string(p.proacl, E'\\n') AS \"Access privileges\",\n l.lanname as \"Language\",\n CASE WHEN l.lanname IN ('internal', 'c') THEN p.prosrc END as \"Internal name\",\n pg_catalog.obj_description(p.oid, 'pg_proc') as \"Description\"",
        );
    }
    sql.push_str("\nFROM pg_catalog.pg_proc p\n     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace");
    if plus {
        sql.push_str("\n     LEFT JOIN pg_catalog.pg_language l ON l.oid = p.prolang");
    }
    let mut base = String::new();
    if pattern.is_none() {
        base.push_str(
            "pg_catalog.pg_function_is_visible(p.oid)\n      AND n.nspname <> 'pg_catalog'\n      AND n.nspname <> 'information_schema'",
        );
    }
    let pat = process_pattern(
        pattern,
        "n.nspname",
        "p.proname",
        if pattern.is_some() { Some("pg_catalog.pg_function_is_visible(p.oid)") } else { None },
    );
    sql.push('\n');
    sql.push_str(&where_clause(&pat.clauses, &base));
    sql.push_str("\nORDER BY 1, 2, 4;");
    let r = run(st, &sql)?;
    print_result(st, &r, "List of functions");
    Ok(())
}

// ------------------------------------------------------------------ \d

pub fn d_command(st: &mut PsqlState, pattern: Option<&str>, plus: bool) -> Result<(), String> {
    let Some(pattern) = pattern else {
        // \d with no argument = \dtvmsE ("List of relations").
        return list_relations(st, "", None, plus);
    };
    let pat = process_pattern(
        Some(pattern),
        "n.nspname",
        "c.relname",
        Some("pg_catalog.pg_table_is_visible(c.oid)"),
    );
    let sql = format!(
        "SELECT c.oid,\n  n.nspname,\n  c.relname\nFROM pg_catalog.pg_class c\n     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace\n{}\nORDER BY 2, 3;",
        where_clause(&pat.clauses, "")
    );
    let r = run(st, &sql)?;
    if r.rows.is_empty() {
        eprintln!("Did not find any relation named \"{pattern}\".");
        return Ok(());
    }
    for i in 0..r.rows.len() {
        let oid = cell(&r, i, 0);
        let nsp = cell(&r, i, 1);
        let name = cell(&r, i, 2);
        describe_one(st, &oid, &nsp, &name, plus)?;
    }
    Ok(())
}

fn describe_one(
    st: &mut PsqlState,
    oid: &str,
    nsp: &str,
    name: &str,
    plus: bool,
) -> Result<(), String> {
    // Basic relation info.
    let info = run(
        st,
        &format!(
            "SELECT c.relkind, c.relpersistence, c.relhasindex, c.relhasrules, c.relhastriggers, c.relrowsecurity, am.amname\nFROM pg_catalog.pg_class c\n LEFT JOIN pg_catalog.pg_am am ON am.oid = c.relam\nWHERE c.oid = '{oid}';"
        ),
    )?;
    if info.rows.is_empty() {
        return Ok(());
    }
    let relkind = cell(&info, 0, 0);

    match relkind.as_str() {
        "S" => return describe_sequence(st, oid, nsp, name),
        "i" | "I" => return describe_index(st, oid, nsp, name, &relkind),
        _ => {}
    }

    // Column list.
    let cols = run(
        st,
        &format!(
            "SELECT a.attname,\n  pg_catalog.format_type(a.atttypid, a.atttypmod),\n  (SELECT pg_catalog.pg_get_expr(d.adbin, d.adrelid, true)\n   FROM pg_catalog.pg_attrdef d\n   WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef),\n  a.attnotnull,\n  (SELECT c.collname FROM pg_catalog.pg_collation c, pg_catalog.pg_type t\n   WHERE c.oid = a.attcollation AND t.oid = a.atttypid AND a.attcollation <> t.typcollation) AS attcollation,\n  a.attidentity,\n  a.attgenerated\nFROM pg_catalog.pg_attribute a\nWHERE a.attrelid = '{oid}' AND a.attnum > 0 AND NOT a.attisdropped\nORDER BY a.attnum;"
        ),
    )?;

    let title_kind = match relkind.as_str() {
        "r" => "Table",
        "v" => "View",
        "m" => "Materialized view",
        "f" => "Foreign table",
        "p" => "Partitioned table",
        "t" => "TOAST table",
        _ => "Table",
    };
    let title = format!("{title_kind} \"{nsp}.{name}\"");

    let mut cells: Vec<Vec<Option<String>>> = Vec::new();
    for i in 0..cols.rows.len() {
        let colname = cell(&cols, i, 0);
        let typ = cell(&cols, i, 1);
        let default = cell_opt(&cols, i, 2);
        let notnull = cell(&cols, i, 3) == "t";
        let collation = cell_opt(&cols, i, 4);
        let identity = cell(&cols, i, 5);
        let generated = cell(&cols, i, 6);
        let default_disp = if generated == "s" {
            Some(format!("generated always as ({}) stored", default.clone().unwrap_or_default()))
        } else if generated == "v" {
            Some(format!("generated always as ({})", default.clone().unwrap_or_default()))
        } else if identity == "a" {
            Some("generated always as identity".to_string())
        } else if identity == "d" {
            Some("generated by default as identity".to_string())
        } else {
            default
        };
        cells.push(vec![
            Some(colname),
            Some(typ),
            collation,
            Some(if notnull { "not null".into() } else { String::new() }),
            default_disp,
        ]);
    }

    let mut footers: Vec<String> = Vec::new();

    if matches!(relkind.as_str(), "r" | "p" | "m") {
        // Indexes.
        let idx = run(
            st,
            &format!(
                "SELECT c2.relname, i.indisprimary, i.indisunique, i.indisclustered, i.indisvalid,\n  pg_catalog.pg_get_indexdef(i.indexrelid, 0, true),\n  pg_catalog.pg_get_constraintdef(con.oid, true),\n  contype, condeferrable, condeferred, i.indisreplident, c2.reltablespace, con.conperiod\nFROM pg_catalog.pg_class c, pg_catalog.pg_class c2, pg_catalog.pg_index i\n  LEFT JOIN pg_catalog.pg_constraint con ON (conrelid = i.indrelid AND conindid = i.indexrelid AND contype IN ('p','u','x'))\nWHERE c.oid = '{oid}' AND c.oid = i.indrelid AND i.indexrelid = c2.oid\nORDER BY i.indisprimary DESC, c2.relname;"
            ),
        )?;
        if !idx.rows.is_empty() {
            footers.push("Indexes:".to_string());
            for i in 0..idx.rows.len() {
                let iname = cell(&idx, i, 0);
                let isprimary = cell(&idx, i, 1) == "t";
                let isunique = cell(&idx, i, 2) == "t";
                let indexdef = cell(&idx, i, 5);
                let contype = cell(&idx, i, 7);
                let mut line = format!("    \"{iname}\"");
                if isprimary {
                    line.push_str(" PRIMARY KEY,");
                } else if isunique {
                    if contype == "u" {
                        line.push_str(" UNIQUE CONSTRAINT,");
                    } else {
                        line.push_str(" UNIQUE,");
                    }
                }
                // Strip "CREATE [UNIQUE] INDEX name ON table USING " prefix.
                let tail = indexdef.split_once(" USING ").map(|x| x.1).unwrap_or(&indexdef);
                line.push(' ');
                line.push_str(tail);
                footers.push(line);
            }
        }
        // Check constraints.
        let checks = run(
            st,
            &format!(
                "SELECT r.conname, pg_catalog.pg_get_constraintdef(r.oid, true)\nFROM pg_catalog.pg_constraint r\nWHERE r.conrelid = '{oid}' AND r.contype = 'c'\nORDER BY 1;"
            ),
        )?;
        if !checks.rows.is_empty() {
            footers.push("Check constraints:".to_string());
            for i in 0..checks.rows.len() {
                footers.push(format!("    \"{}\" {}", cell(&checks, i, 0), cell(&checks, i, 1)));
            }
        }
        // Foreign keys.
        let fks = run(
            st,
            &format!(
                "SELECT conname, pg_catalog.pg_get_constraintdef(oid, true)\nFROM pg_catalog.pg_constraint\nWHERE conrelid = '{oid}' AND contype = 'f'\nORDER BY 1;"
            ),
        )?;
        if !fks.rows.is_empty() {
            footers.push("Foreign-key constraints:".to_string());
            for i in 0..fks.rows.len() {
                footers.push(format!("    \"{}\" {}", cell(&fks, i, 0), cell(&fks, i, 1)));
            }
        }
        // Referenced by.
        let refs = run(
            st,
            &format!(
                "SELECT conname, conrelid::pg_catalog.regclass AS ontable,\n       pg_catalog.pg_get_constraintdef(oid, true)\nFROM pg_catalog.pg_constraint\nWHERE confrelid = '{oid}' AND contype = 'f'\nORDER BY conname;"
            ),
        )?;
        if !refs.rows.is_empty() {
            footers.push("Referenced by:".to_string());
            for i in 0..refs.rows.len() {
                footers.push(format!(
                    "    TABLE \"{}\" CONSTRAINT \"{}\" {}",
                    cell(&refs, i, 1),
                    cell(&refs, i, 0),
                    cell(&refs, i, 2)
                ));
            }
        }
    }

    if relkind == "v" && plus {
        let def = run(
            st,
            &format!("SELECT pg_catalog.pg_get_viewdef('{oid}'::pg_catalog.oid, true);"),
        )?;
        if !def.rows.is_empty() {
            footers.push("View definition:".to_string());
            for l in cell(&def, 0, 0).lines() {
                footers.push(format!(" {l}"));
            }
        }
    }

    let t = Table {
        title: Some(title),
        headers: vec![
            "Column".into(),
            "Type".into(),
            "Collation".into(),
            "Nullable".into(),
            "Default".into(),
        ],
        aligns: vec!['l', 'l', 'l', 'l', 'l'],
        cells,
        footers: if footers.is_empty() { Some(Vec::new()) } else { Some(footers) },
    };
    let mut out = std::io::stdout();
    let _ = print::print_table(&t, &st.popt, &mut out);
    let _ = std::io::Write::flush(&mut out);
    Ok(())
}

fn describe_sequence(st: &mut PsqlState, oid: &str, nsp: &str, name: &str) -> Result<(), String> {
    let r = run(
        st,
        &format!(
            "SELECT pg_catalog.format_type(seqtypid, NULL) AS \"Type\",\n       seqstart AS \"Start\",\n       seqmin AS \"Minimum\",\n       seqmax AS \"Maximum\",\n       seqincrement AS \"Increment\",\n       CASE WHEN seqcycle THEN 'yes' ELSE 'no' END AS \"Cycles?\",\n       seqcache AS \"Cache\"\nFROM pg_catalog.pg_sequence\nWHERE seqrelid = '{oid}';"
        ),
    )?;
    let mut t = result_to_table(&r);
    t.title = Some(format!("Sequence \"{nsp}.{name}\""));
    // Owned-by footer.
    let owned = run(
        st,
        &format!(
            "SELECT pg_catalog.quote_ident(nspname) || '.' ||\n       pg_catalog.quote_ident(relname) || '.' ||\n       pg_catalog.quote_ident(attname),\n       d.deptype\nFROM pg_catalog.pg_class c\nINNER JOIN pg_catalog.pg_depend d ON c.oid=d.refobjid\nINNER JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace\nINNER JOIN pg_catalog.pg_attribute a ON (a.attrelid=c.oid AND a.attnum=d.refobjsubid)\nWHERE d.classid='pg_catalog.pg_class'::pg_catalog.regclass\n  AND d.refclassid='pg_catalog.pg_class'::pg_catalog.regclass\n  AND d.objid='{oid}'\n  AND d.deptype IN ('a', 'i')"
        ),
    )?;
    let mut footers = Vec::new();
    if !owned.rows.is_empty() {
        footers.push(format!("Owned by: {}", cell(&owned, 0, 0)));
    }
    t.footers = Some(footers);
    let mut out = std::io::stdout();
    let _ = print::print_table(&t, &st.popt, &mut out);
    let _ = std::io::Write::flush(&mut out);
    Ok(())
}

fn describe_index(
    st: &mut PsqlState,
    oid: &str,
    nsp: &str,
    name: &str,
    relkind: &str,
) -> Result<(), String> {
    let meta = run(
        st,
        &format!(
            "SELECT i.indisunique, i.indisprimary, i.indisclustered, i.indisvalid, am.amname,\n  c2.relname,\n  n2.nspname,\n  i.indnkeyatts\nFROM pg_catalog.pg_index i, pg_catalog.pg_class c, pg_catalog.pg_class c2, pg_catalog.pg_am am, pg_catalog.pg_namespace n2\nWHERE i.indexrelid = c.oid AND c.oid = '{oid}' AND c.relam = am.oid\n  AND i.indrelid = c2.oid AND c2.relnamespace = n2.oid;"
        ),
    )?;
    if meta.rows.is_empty() {
        return Ok(());
    }
    let isunique = cell(&meta, 0, 0) == "t";
    let isprimary = cell(&meta, 0, 1) == "t";
    let amname = cell(&meta, 0, 4);
    let tabname = cell(&meta, 0, 5);
    let tabnsp = cell(&meta, 0, 6);
    let nkey: usize = cell(&meta, 0, 7).parse().unwrap_or(0);

    let cols = run(
        st,
        &format!(
            "SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod), a.attnum,\n  pg_catalog.pg_get_indexdef(a.attrelid, a.attnum, TRUE) AS indexdef\nFROM pg_catalog.pg_attribute a\nWHERE a.attrelid = '{oid}' AND a.attnum > 0 AND NOT a.attisdropped\nORDER BY a.attnum;"
        ),
    )?;
    let mut cells = Vec::new();
    for i in 0..cols.rows.len() {
        let attnum: usize = cell(&cols, i, 2).parse().unwrap_or(0);
        cells.push(vec![
            Some(cell(&cols, i, 0)),
            Some(cell(&cols, i, 1)),
            Some(if attnum <= nkey { "yes".into() } else { "no".into() }),
            Some(cell(&cols, i, 3)),
        ]);
    }
    let kindword = if relkind == "I" { "Partitioned index" } else { "Index" };
    let mut qual = String::new();
    if isprimary {
        qual.push_str("primary key, ");
    } else if isunique {
        qual.push_str("unique, ");
    }
    let footer = format!("{qual}{amname}, for table \"{tabnsp}.{tabname}\"");
    let t = Table {
        title: Some(format!("{kindword} \"{nsp}.{name}\"")),
        headers: vec!["Column".into(), "Type".into(), "Key?".into(), "Definition".into()],
        aligns: vec!['l', 'l', 'l', 'l'],
        cells,
        footers: Some(vec![footer]),
    };
    let mut out = std::io::stdout();
    let _ = print::print_table(&t, &st.popt, &mut out);
    let _ = std::io::Write::flush(&mut out);
    Ok(())
}

// ------------------------------------------------------------- \conninfo

pub fn conninfo(st: &mut PsqlState) {
    // PG18's tabular \conninfo (exec_command_conninfo, command.c).
    let Some(conn) = st.conn.as_ref() else {
        println!("You are currently not connected to a database.");
        return;
    };
    let p = &st.cparams;
    let mut rows: Vec<(String, String)> = Vec::new();
    rows.push(("Database".into(), p.dbname.clone()));
    rows.push(("Client User".into(), p.user.clone()));
    if p.host.starts_with('/') {
        rows.push(("Socket Directory".into(), p.host.clone()));
    } else {
        rows.push(("Host".into(), p.host.clone()));
    }
    rows.push(("Server Port".into(), p.port.clone()));
    rows.push(("Options".into(), String::new()));
    rows.push(("Protocol Version".into(), "3.0".into()));
    rows.push(("Password Used".into(), if conn.used_password { "true" } else { "false" }.into()));
    rows.push(("GSSAPI Authenticated".into(), "false".into()));
    rows.push(("Backend PID".into(), conn.be_pid.to_string()));
    rows.push(("SSL Connection".into(), "false".into()));
    rows.push((
        "Superuser".into(),
        conn.parameter_status("is_superuser").unwrap_or("off").to_string(),
    ));
    rows.push((
        "Hot Standby".into(),
        conn.parameter_status("in_hot_standby").unwrap_or("off").to_string(),
    ));
    let t = Table {
        title: Some("Connection Information".into()),
        headers: vec!["Parameter".into(), "Value".into()],
        aligns: vec!['l', 'l'],
        cells: rows.into_iter().map(|(a, b)| vec![Some(a), Some(b)]).collect(),
        footers: None,
    };
    let mut out = std::io::stdout();
    let _ = print::print_table(&t, &st.popt, &mut out);
    let _ = std::io::Write::flush(&mut out);
}
